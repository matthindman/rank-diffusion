#!/usr/bin/env python3
"""Build comments-focused Reddit panels from completed monthly comment aggregates."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from phase2_common import ensure_layout, file_manifest_row, sha256_file, upsert_manifest


SCRIPT_NAME = "scripts/data_wrangling/build_reddit_comment_panels.py"
PANEL_COLS = [
    "date",
    "endpoint_id",
    "metric_value",
    "submission_karma",
    "comment_karma",
    "submission_count",
    "comment_count",
]
SUM_COLS = ["metric_value", "submission_karma", "comment_karma", "submission_count", "comment_count"]


def month_range(start: str, end: str) -> list[str]:
    return [str(m) for m in pd.period_range(start, end, freq="M")]


def month_from_name(path: Path) -> str | None:
    m = re.search(r"_(\d{4}-\d{2})\.parquet$", path.name)
    return m.group(1) if m else None


def map_months(paths: list[Path]) -> dict[str, Path]:
    out = {}
    for path in paths:
        month = month_from_name(path)
        if month:
            out[month] = path
    return out


def week_start(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s)
    return d - pd.to_timedelta(d.dt.weekday, unit="D")


def load_comment_month(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=["subreddit", "date", "score_sum", "count"])
    df = df.rename(
        columns={
            "subreddit": "endpoint_id",
            "score_sum": "comment_karma",
            "count": "comment_count",
        }
    )
    df["endpoint_id"] = df["endpoint_id"].astype(str)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()
    df["comment_karma"] = pd.to_numeric(df["comment_karma"], errors="coerce").fillna(0).astype("int64")
    df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce").fillna(0).astype("int64")
    df = df.groupby(["endpoint_id", "date"], as_index=False, sort=False)[["comment_karma", "comment_count"]].sum()
    df["metric_value"] = df["comment_karma"].clip(lower=0).astype("int64")
    df["submission_karma"] = 0
    df["submission_count"] = 0
    df = df[PANEL_COLS].sort_values(["date", "endpoint_id"]).reset_index(drop=True)
    if df.duplicated(["endpoint_id", "date"]).any():
        raise AssertionError(f"duplicate (endpoint_id, date) in {path}")
    if (df["metric_value"] < 0).any():
        raise AssertionError(f"negative metric_value in {path}")
    return df


def append_parquet(writer: pq.ParquetWriter | None, path: Path, df: pd.DataFrame) -> pq.ParquetWriter:
    table = pa.Table.from_pandas(df, preserve_index=False)
    if writer is None:
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = pq.ParquetWriter(path, table.schema, compression="snappy")
    writer.write_table(table)
    return writer


def finalize_weekly(parts: list[pd.DataFrame]) -> pd.DataFrame:
    weekly = pd.concat(parts, ignore_index=True)
    weekly = weekly.groupby(["endpoint_id", "date"], as_index=False, sort=False)[SUM_COLS].sum()
    weekly = weekly[["endpoint_id", "date", *SUM_COLS]].sort_values(["date", "endpoint_id"]).reset_index(drop=True)
    if weekly.duplicated(["endpoint_id", "date"]).any():
        raise AssertionError("duplicate (endpoint_id, date) in weekly comments panel")
    if (weekly["metric_value"] < 0).any():
        raise AssertionError("negative metric_value in weekly comments panel")
    if not (pd.to_datetime(weekly["date"]).dt.weekday == 0).all():
        raise AssertionError("non-Monday date in weekly comments panel")
    return weekly


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ssd-root", type=Path, default=Path("/Volumes/T9/rank-diffusion-data"))
    parser.add_argument("--start", default="2018-12")
    parser.add_argument("--end", default="2021-06")
    parser.add_argument("--output-stem", default=None)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ensure_layout(args.ssd_root)
    monthly_dir = args.ssd_root / "aggregates" / "reddit" / "monthly" / "comments"
    comments_by_month = map_months(sorted(monthly_dir.glob("comments_*.parquet")))
    months = month_range(args.start, args.end)
    missing = [m for m in months if m not in comments_by_month]
    available = [m for m in months if m in comments_by_month]

    print(f"Months requested: {len(months)}")
    print(f"Comment months available: {len(available)}")
    if available:
        print(f"Available range: {available[0]}..{available[-1]}")
    if missing:
        print(f"Missing comment months: {', '.join(missing)}")
        if args.require_complete:
            raise SystemExit("missing required Reddit comment monthly aggregates")
    if args.dry_run:
        return
    if not available:
        raise SystemExit("no Reddit comment aggregates available")

    stem = args.output_stem or f"reddit_comments_{args.start}_{args.end}"
    derived_dir = args.ssd_root / "derived"
    manifest_dir = args.ssd_root / "manifest"
    daily_path = derived_dir / f"{stem}_daily.parquet"
    weekly_path = derived_dir / f"{stem}_weekly.parquet"
    coverage_path = manifest_dir / f"{stem}_coverage.csv"
    daily_tmp = daily_path.with_suffix(".parquet.tmp")
    weekly_tmp = weekly_path.with_suffix(".parquet.tmp")
    for path in [daily_tmp, weekly_tmp]:
        if path.exists():
            path.unlink()

    source_paths: list[str] = []
    source_bytes: list[int] = []
    source_sha256: list[str] = []
    coverage_rows = []
    weekly_parts = []
    daily_rows = 0
    writer: pq.ParquetWriter | None = None

    for i, month in enumerate(months, 1):
        path = comments_by_month.get(month)
        if path is None:
            coverage_rows.append({"month": month, "status": "missing", "source_path": "", "rows": "", "bytes": ""})
            continue
        print(f"[{i}/{len(months)}] building comments {month}", flush=True)
        daily = load_comment_month(path)
        daily_rows += len(daily)
        writer = append_parquet(writer, daily_tmp, daily)
        w = daily.copy()
        w["date"] = week_start(w["date"])
        weekly_parts.append(w.groupby(["endpoint_id", "date"], as_index=False, sort=False)[SUM_COLS].sum())
        source_paths.append(str(path))
        source_bytes.append(path.stat().st_size)
        source_sha256.append(sha256_file(path))
        coverage_rows.append(
            {"month": month, "status": "ok", "source_path": str(path), "rows": len(daily), "bytes": path.stat().st_size}
        )
    if writer is not None:
        writer.close()

    weekly = finalize_weekly(weekly_parts)
    pq.write_table(pa.Table.from_pandas(weekly, preserve_index=False), weekly_tmp, compression="snappy")
    os.replace(daily_tmp, daily_path)
    os.replace(weekly_tmp, weekly_path)
    pd.DataFrame(coverage_rows).to_csv(coverage_path, index=False)

    # Exact sum check from written daily file.
    daily_check = pd.read_parquet(daily_path)
    daily_min = daily_check["date"].min()
    daily_max = daily_check["date"].max()
    daily_check["date"] = week_start(daily_check["date"])
    daily_week = daily_check.groupby(["endpoint_id", "date"], as_index=False, sort=False)[SUM_COLS].sum()
    compare = weekly.merge(daily_week, on=["endpoint_id", "date"], how="outer", suffixes=("_weekly", "_daily"), indicator=True)
    if (compare["_merge"] != "both").any():
        raise AssertionError("weekly and daily-week key sets differ")
    for col in SUM_COLS:
        if not (compare[f"{col}_weekly"].to_numpy() == compare[f"{col}_daily"].to_numpy()).all():
            raise AssertionError(f"weekly {col} != sum of daily {col}")

    params = json.dumps(
        {
            "ssd_root": str(args.ssd_root),
            "start": args.start,
            "end": args.end,
            "output_stem": stem,
            "metric_value": "daily comment_karma clipped at zero, then Monday-summed",
        },
        sort_keys=True,
    )
    manifest_rows = [
        file_manifest_row(
            daily_path,
            role="derived/reddit/comments_daily",
            sha256=sha256_file(daily_path),
            source_paths=source_paths,
            source_bytes=";".join(str(x) for x in source_bytes),
            source_sha256=";".join(source_sha256),
            script=SCRIPT_NAME,
            parameters=params,
            status="ok",
        ),
        file_manifest_row(
            weekly_path,
            role="derived/reddit/comments_weekly",
            sha256=sha256_file(weekly_path),
            source_paths=source_paths,
            source_bytes=";".join(str(x) for x in source_bytes),
            source_sha256=";".join(source_sha256),
            script=SCRIPT_NAME,
            parameters=params,
            status="ok",
        ),
        file_manifest_row(
            coverage_path,
            role="manifest/reddit/comments_coverage",
            sha256=sha256_file(coverage_path),
            source_paths=source_paths,
            source_bytes=";".join(str(x) for x in source_bytes),
            source_sha256=";".join(source_sha256),
            script=SCRIPT_NAME,
            parameters=params,
            status="ok",
        ),
    ]
    upsert_manifest(args.ssd_root / "manifest" / "MANIFEST.csv", manifest_rows)

    print(f"Wrote daily: {daily_path} rows={daily_rows:,} bytes={daily_path.stat().st_size:,}")
    print(f"Wrote weekly: {weekly_path} rows={len(weekly):,} bytes={weekly_path.stat().st_size:,}")
    print(f"Wrote coverage: {coverage_path}")
    print(f"Date range daily: {pd.Timestamp(daily_min).date()} to {pd.Timestamp(daily_max).date()}")
    print(f"Date range weekly: {weekly['date'].min().date()} to {weekly['date'].max().date()}")


if __name__ == "__main__":
    main()
