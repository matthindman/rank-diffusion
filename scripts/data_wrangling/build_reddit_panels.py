#!/usr/bin/env python3
"""Build model-ready Reddit daily/weekly panels from SSD monthly aggregates."""

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


SCRIPT_NAME = "scripts/data_wrangling/build_reddit_panels.py"
PANEL_COLS = [
    "date",
    "endpoint_id",
    "metric_value",
    "submission_karma",
    "comment_karma",
    "submission_count",
    "comment_count",
]


def month_range(start: str, end: str) -> list[str]:
    months = pd.period_range(start, end, freq="M")
    return [str(m) for m in months]


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


def load_side(path: Path | None, kind: str) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=["endpoint_id", "date", f"{kind}_karma", f"{kind}_count"])
    df = pd.read_parquet(path, columns=["subreddit", "date", "score_sum", "count"])
    df = df.rename(
        columns={
            "subreddit": "endpoint_id",
            "score_sum": f"{kind}_karma",
            "count": f"{kind}_count",
        }
    )
    df["endpoint_id"] = df["endpoint_id"].astype(str)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()
    df[f"{kind}_karma"] = pd.to_numeric(df[f"{kind}_karma"], errors="coerce").fillna(0).astype("int64")
    df[f"{kind}_count"] = pd.to_numeric(df[f"{kind}_count"], errors="coerce").fillna(0).astype("int64")
    return (
        df.groupby(["endpoint_id", "date"], as_index=False, sort=False)[[f"{kind}_karma", f"{kind}_count"]]
        .sum()
    )


def build_month(sub_path: Path | None, com_path: Path | None) -> pd.DataFrame:
    subs = load_side(sub_path, "submission")
    comments = load_side(com_path, "comment")
    if subs.empty:
        out = comments.copy()
        out["submission_karma"] = 0
        out["submission_count"] = 0
    elif comments.empty:
        out = subs.copy()
        out["comment_karma"] = 0
        out["comment_count"] = 0
    else:
        out = subs.merge(comments, on=["endpoint_id", "date"], how="outer")
    for col in ["submission_karma", "comment_karma", "submission_count", "comment_count"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")
    out["metric_value"] = out["submission_karma"].clip(lower=0).astype("int64")
    out = out[PANEL_COLS].sort_values(["date", "endpoint_id"]).reset_index(drop=True)
    if out.duplicated(["endpoint_id", "date"]).any():
        raise AssertionError("duplicate (endpoint_id, date) in monthly Reddit output")
    if (out["metric_value"] < 0).any():
        raise AssertionError("negative metric_value in monthly Reddit output")
    return out


def append_parquet(writer: pq.ParquetWriter | None, path: Path, df: pd.DataFrame) -> pq.ParquetWriter:
    table = pa.Table.from_pandas(df, preserve_index=False)
    if writer is None:
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = pq.ParquetWriter(path, table.schema, compression="snappy")
    writer.write_table(table)
    return writer


def finalize_weekly(parts: list[pd.DataFrame]) -> pd.DataFrame:
    weekly = pd.concat(parts, ignore_index=True)
    weekly = weekly.groupby(["endpoint_id", "date"], as_index=False, sort=False)[
        ["metric_value", "submission_karma", "comment_karma", "submission_count", "comment_count"]
    ].sum()
    weekly = weekly[["endpoint_id", "date", "metric_value", "submission_karma", "comment_karma", "submission_count", "comment_count"]]
    weekly = weekly.sort_values(["date", "endpoint_id"]).reset_index(drop=True)
    if weekly.duplicated(["endpoint_id", "date"]).any():
        raise AssertionError("duplicate (endpoint_id, date) in Reddit weekly output")
    if (weekly["metric_value"] < 0).any():
        raise AssertionError("negative metric_value in Reddit weekly output")
    if not (pd.to_datetime(weekly["date"]).dt.weekday == 0).all():
        raise AssertionError("non-Monday date in Reddit weekly output")
    return weekly


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ssd-root", type=Path, default=Path("/Volumes/T9/rank-diffusion-data"))
    parser.add_argument("--start", default="2018-12")
    parser.add_argument("--end", default="2022-12")
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ensure_layout(args.ssd_root)
    monthly = args.ssd_root / "aggregates" / "reddit" / "monthly"
    sub_by_month = map_months(sorted((monthly / "submissions").glob("submissions_*.parquet")))
    com_by_month = map_months(sorted((monthly / "comments").glob("comments_*.parquet")))
    months = month_range(args.start, args.end)
    missing = [m for m in months if m not in sub_by_month and m not in com_by_month]
    print(f"Months requested: {len(months)}")
    print(f"Submission months available: {len([m for m in months if m in sub_by_month])}")
    print(f"Comment months available: {len([m for m in months if m in com_by_month])}")
    if missing:
        print(f"Missing months with neither type: {', '.join(missing)}")
        if args.require_complete:
            raise SystemExit("missing required Reddit monthly aggregates")
    if args.dry_run:
        return

    derived_dir = args.ssd_root / "derived"
    daily_path = derived_dir / "reddit_daily_long.parquet"
    weekly_path = derived_dir / "reddit_weekly_long.parquet"
    daily_tmp = daily_path.with_suffix(".parquet.tmp")
    weekly_tmp = weekly_path.with_suffix(".parquet.tmp")
    for path in [daily_tmp, weekly_tmp]:
        if path.exists():
            path.unlink()

    writer: pq.ParquetWriter | None = None
    weekly_parts = []
    source_paths: list[str] = []
    daily_rows = 0
    for i, month in enumerate(months, 1):
        sub_path = sub_by_month.get(month)
        com_path = com_by_month.get(month)
        if sub_path is None and com_path is None:
            continue
        if sub_path is not None:
            source_paths.append(str(sub_path))
        if com_path is not None:
            source_paths.append(str(com_path))
        print(f"[{i}/{len(months)}] building {month}", flush=True)
        daily = build_month(sub_path, com_path)
        daily_rows += len(daily)
        writer = append_parquet(writer, daily_tmp, daily)
        w = daily.copy()
        w["date"] = week_start(w["date"])
        weekly_parts.append(
            w.groupby(["endpoint_id", "date"], as_index=False, sort=False)[
                ["metric_value", "submission_karma", "comment_karma", "submission_count", "comment_count"]
            ].sum()
        )
    if writer is not None:
        writer.close()
    if not weekly_parts:
        raise SystemExit("no Reddit monthly aggregates available to build panels")

    weekly = finalize_weekly(weekly_parts)
    pq.write_table(pa.Table.from_pandas(weekly, preserve_index=False), weekly_tmp, compression="snappy")
    os.replace(daily_tmp, daily_path)
    os.replace(weekly_tmp, weekly_path)

    # Exact sum check from the just-written daily file.
    daily_check = pd.read_parquet(daily_path)
    daily_min = daily_check["date"].min()
    daily_max = daily_check["date"].max()
    daily_check["date"] = week_start(daily_check["date"])
    chk = daily_check.groupby(["endpoint_id", "date"], as_index=False)["metric_value"].sum()
    merged = chk.merge(
        weekly[["endpoint_id", "date", "metric_value"]].rename(columns={"metric_value": "weekly_metric"}),
        on=["endpoint_id", "date"],
        how="outer",
    )
    if len(merged) != len(weekly) or not (merged["metric_value"].fillna(-1).to_numpy() == merged["weekly_metric"].fillna(-2).to_numpy()).all():
        raise AssertionError("Reddit weekly metric_value != sum of daily metric_value")

    params = json.dumps({"ssd_root": str(args.ssd_root), "start": args.start, "end": args.end}, sort_keys=True)
    rows = [
        file_manifest_row(
            daily_path,
            role="derived/reddit/daily",
            sha256=sha256_file(daily_path),
            source_paths=source_paths,
            source_bytes="",
            source_sha256="",
            script=SCRIPT_NAME,
            parameters=params,
            status="ok",
        ),
        file_manifest_row(
            weekly_path,
            role="derived/reddit/weekly",
            sha256=sha256_file(weekly_path),
            source_paths=source_paths,
            source_bytes="",
            source_sha256="",
            script=SCRIPT_NAME,
            parameters=params,
            status="ok",
        ),
    ]
    upsert_manifest(args.ssd_root / "manifest" / "MANIFEST.csv", rows)
    print(f"Wrote daily: {daily_path} rows={daily_rows:,}")
    print(f"Wrote weekly: {weekly_path} rows={len(weekly):,}")
    print(f"Date range daily: {daily_min.date()} to {daily_max.date()}")
    print(f"Date range weekly: {weekly['date'].min().date()} to {weekly['date'].max().date()}")


if __name__ == "__main__":
    main()
