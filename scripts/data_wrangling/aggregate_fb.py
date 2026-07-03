#!/usr/bin/env python3
"""Build Facebook daily/weekly panels from copied CrowdTangle raw files."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from phase2_common import ensure_layout, file_manifest_row, sha256_file, upsert_manifest, utc_now


SCRIPT_NAME = "scripts/data_wrangling/aggregate_fb.py"

BACKFILL_COMPONENTS = {
    "like_count": "statistics.actual.likeCount",
    "share_count": "statistics.actual.shareCount",
    "comment_count": "statistics.actual.commentCount",
    "love_count": "statistics.actual.loveCount",
    "wow_count": "statistics.actual.wowCount",
    "haha_count": "statistics.actual.hahaCount",
    "sad_count": "statistics.actual.sadCount",
    "angry_count": "statistics.actual.angryCount",
    "thankful_count": "statistics.actual.thankfulCount",
    "care_count": "statistics.actual.careCount",
}
FULL_FB_COMPONENTS = {
    "like_count": "likes",
    "share_count": "shares",
    "comment_count": "comments",
    "love_count": "loves",
    "wow_count": "wows",
    "haha_count": "hahas",
    "sad_count": "sads",
    "angry_count": "angrys",
    "care_count": "cares",
}
COMPONENT_COLS = list(BACKFILL_COMPONENTS.keys())
DAILY_COLS = [
    "date",
    "account_id",
    "account_platform_id",
    "account_name",
    "metric_value",
    *COMPONENT_COLS,
    "post_count",
    "interactions_max",
    "source_kind",
]


def parse_int_series(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        s = s.astype(str).str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype("int64")


def normalize_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.tz_localize(None).dt.normalize()


def backfill_metadata(raw_root: Path) -> pd.DataFrame:
    rows = []
    for p in sorted((raw_root / "crowdtangle_backfill").glob("*_test.parquet")):
        m = re.match(r"(\d{4}-\d{2}-\d{2})--(\d{4}-\d{2}-\d{2})_test\.parquet$", p.name)
        if not m:
            continue
        pf = pq.ParquetFile(p)
        rows.append(
            {
                "date": pd.Timestamp(m.group(1)),
                "end": pd.Timestamp(m.group(2)),
                "path": str(p),
                "rows": pf.metadata.num_rows,
                "bytes": p.stat().st_size,
            }
        )
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def full_fb_date_counts(path: Path) -> pd.Series:
    counts: defaultdict[pd.Timestamp, int] = defaultdict(int)
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(columns=["post_created_date"], batch_size=2_000_000):
        s = batch.column(0).to_pandas()
        d = normalize_date(s)
        vc = d.value_counts()
        for date, n in vc.items():
            if pd.notna(date):
                counts[pd.Timestamp(date)] += int(n)
    return pd.Series(counts, dtype="int64").sort_index()


def build_day_sources(raw_root: Path, out_path: Path) -> pd.DataFrame:
    meta = backfill_metadata(raw_root)
    if meta.empty:
        raise SystemExit("No copied backfill parquet files found")
    full_fb = raw_root / "crowdtangle" / "full_fb.parquet"
    full_counts = full_fb_date_counts(full_fb) if full_fb.exists() else pd.Series(dtype="int64")
    all_days = pd.date_range(meta["date"].min(), meta["date"].max(), freq="D")
    by_day = meta.set_index("date")
    rows = []
    for day in all_days:
        source_kind = ""
        source_path = ""
        source_rows = 0
        status = "missing"
        reason = "no backfill file"
        if day in by_day.index:
            rec = by_day.loc[day]
            rows_count = int(rec["rows"])
            if rows_count > 0:
                source_kind = "backfill"
                source_path = rec["path"]
                source_rows = rows_count
                status = "complete"
                reason = "backfill nonzero rows"
            else:
                status = "missing"
                reason = "zero-row backfill file treated as collection failure"
        if status != "complete" and day.year == 2023 and day in full_counts.index:
            source_kind = "full_fb"
            source_path = str(full_fb)
            source_rows = int(full_counts.loc[day])
            status = "complete"
            reason = "patched from full_fb.parquet"
        rows.append(
            {
                "date": day,
                "status": status,
                "source_kind": source_kind,
                "source_path": source_path,
                "source_rows": source_rows,
                "reason": reason,
            }
        )
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    return df


def aggregate_one_backfill(path: Path) -> pd.DataFrame:
    cols = [
        "platformId",
        "date",
        "account.id",
        "account.platformId",
        "account.name",
        *BACKFILL_COMPONENTS.values(),
    ]
    df = pd.read_parquet(path, columns=cols)
    out = pd.DataFrame(
        {
            "post_id": df["platformId"].astype(str),
            "date": normalize_date(df["date"]),
            "account_id": df["account.id"].astype(str),
            "account_platform_id": df["account.platformId"].astype(str),
            "account_name": df["account.name"].astype(str),
        }
    )
    for out_col, src_col in BACKFILL_COMPONENTS.items():
        out[out_col] = parse_int_series(df[src_col])
    out["metric_value"] = out[COMPONENT_COLS].sum(axis=1)
    out = out.dropna(subset=["date"])
    out = out.sort_values("metric_value", ascending=False).drop_duplicates("post_id", keep="first")
    return group_daily(out, "backfill")


def group_daily(posts: pd.DataFrame, source_kind: str) -> pd.DataFrame:
    gcols = ["date", "account_id", "account_platform_id", "account_name"]
    agg = {col: "sum" for col in ["metric_value", *COMPONENT_COLS]}
    agg["post_id"] = "count"
    agg["metric_value_for_max"] = "max"
    posts = posts.rename(columns={"metric_value": "metric_value_for_max"}).assign(
        metric_value=posts["metric_value"]
    )
    agg["metric_value"] = "sum"
    grouped = posts.groupby(gcols, as_index=False, sort=False).agg(agg)
    grouped = grouped.rename(columns={"post_id": "post_count", "metric_value_for_max": "interactions_max"})
    grouped["source_kind"] = source_kind
    return grouped[DAILY_COLS]


def aggregate_full_fb_patch(full_fb_path: Path, patch_dates: set[pd.Timestamp]) -> pd.DataFrame:
    if not patch_dates:
        return pd.DataFrame(columns=DAILY_COLS)
    date_values = sorted(pd.Timestamp(d).date() for d in patch_dates)
    dataset = ds.dataset(full_fb_path, format="parquet")
    columns = [
        "facebook_id",
        "page_name",
        "user_name",
        "post_created_date",
        "url",
        *FULL_FB_COMPONENTS.values(),
    ]
    filt = pc.field("post_created_date").isin(pa.array(date_values, type=pa.date32()))
    frames = []
    for batch in dataset.to_batches(columns=columns, filter=filt, batch_size=500_000):
        df = batch.to_pandas()
        if df.empty:
            continue
        out = pd.DataFrame(
            {
                "post_id": df["url"].astype(str),
                "date": normalize_date(df["post_created_date"]),
                "account_id": df["facebook_id"].astype(str),
                "account_platform_id": df["user_name"].astype(str),
                "account_name": df["page_name"].astype(str),
            }
        )
        for out_col, src_col in FULL_FB_COMPONENTS.items():
            out[out_col] = parse_int_series(df[src_col])
        out["thankful_count"] = 0
        out["metric_value"] = out[COMPONENT_COLS].sum(axis=1)
        out = out[out["date"].isin(patch_dates)]
        out = out.sort_values("metric_value", ascending=False).drop_duplicates("post_id", keep="first")
        frames.append(group_daily(out, "full_fb"))
    if not frames:
        return pd.DataFrame(columns=DAILY_COLS)
    return pd.concat(frames, ignore_index=True)


def week_start(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s)
    return d - pd.to_timedelta(d.dt.weekday, unit="D")


def add_endpoint(df: pd.DataFrame, candidate: str) -> pd.DataFrame:
    c = {
        "account.id": "account_id",
        "account.platformId": "account_platform_id",
        "account.name": "account_name",
    }[candidate]
    out = df.copy()
    out["endpoint_id"] = out[c].astype(str)
    out = out[out["endpoint_id"].notna() & (out["endpoint_id"] != "") & (out["endpoint_id"] != "nan")]
    return out


def complete_weeks(day_sources: pd.DataFrame) -> set[pd.Timestamp]:
    complete = day_sources[day_sources["status"].eq("complete")].copy()
    complete["week"] = week_start(complete["date"])
    counts = complete.groupby("week")["date"].nunique()
    return set(counts[counts == 7].index)


def weekly_from_daily(daily: pd.DataFrame, candidate: str, weeks: set[pd.Timestamp]) -> pd.DataFrame:
    d = add_endpoint(daily, candidate)
    d["week"] = week_start(d["date"])
    d = d[d["week"].isin(weeks)].copy()
    agg = {col: "sum" for col in ["metric_value", *COMPONENT_COLS, "post_count"]}
    agg["interactions_max"] = "max"
    w = d.groupby(["endpoint_id", "week"], as_index=False, sort=False).agg(agg)
    return w.rename(columns={"week": "date"})


def final_daily_from_aggregate(daily_agg: pd.DataFrame, candidate: str) -> pd.DataFrame:
    d = add_endpoint(daily_agg, candidate)
    agg = {col: "sum" for col in ["metric_value", *COMPONENT_COLS, "post_count"]}
    agg["interactions_max"] = "max"
    agg["source_kind"] = lambda x: "|".join(sorted(set(map(str, x))))
    daily = d.groupby(["endpoint_id", "date"], as_index=False, sort=False).agg(agg)
    first_cols = ["date", "endpoint_id", "metric_value"]
    return daily[first_cols + [c for c in daily.columns if c not in first_cols]]


def validation_bakeoff(daily: pd.DataFrame, day_sources: pd.DataFrame, trusted_path: Path, out_dir: Path) -> pd.DataFrame:
    weeks = complete_weeks(day_sources)
    trusted = pd.read_parquet(trusted_path, columns=["date", "endpoint_id", "metric_value"])
    trusted["date"] = pd.to_datetime(trusted["date"]).dt.normalize()
    trusted = trusted[trusted["date"] >= pd.Timestamp("2020-11-02")].copy()
    details = []
    summaries = []
    for candidate in ["account.id", "account.platformId", "account.name"]:
        weekly = weekly_from_daily(daily, candidate, weeks)
        wkset = sorted(set(weekly["date"]).intersection(set(trusted["date"])))
        for wk in wkset:
            a = trusted[trusted["date"].eq(wk)][["endpoint_id", "metric_value"]].rename(
                columns={"metric_value": "trusted_metric"}
            )
            b = weekly[weekly["date"].eq(wk)][["endpoint_id", "metric_value"]].rename(
                columns={"metric_value": "rebuilt_metric"}
            )
            joined = a.merge(b, on="endpoint_id", how="inner")
            corr = joined["trusted_metric"].corr(joined["rebuilt_metric"]) if len(joined) >= 3 else np.nan
            details.append(
                {
                    "candidate": candidate,
                    "week": wk,
                    "trusted_entities": len(a),
                    "rebuilt_entities": len(b),
                    "joined_entities": len(joined),
                    "trusted_join_rate": len(joined) / max(len(a), 1),
                    "rebuilt_join_rate": len(joined) / max(len(b), 1),
                    "metric_correlation": corr,
                }
            )
        det = pd.DataFrame([r for r in details if r["candidate"] == candidate])
        summaries.append(
            {
                "candidate": candidate,
                "weeks": len(det),
                "mean_trusted_join_rate": det["trusted_join_rate"].mean() if not det.empty else np.nan,
                "min_trusted_join_rate": det["trusted_join_rate"].min() if not det.empty else np.nan,
                "mean_metric_correlation": det["metric_correlation"].mean() if not det.empty else np.nan,
                "min_metric_correlation": det["metric_correlation"].min() if not det.empty else np.nan,
            }
        )
    detail_df = pd.DataFrame(details)
    summary_df = pd.DataFrame(summaries).sort_values(
        ["mean_metric_correlation", "mean_trusted_join_rate"], ascending=False
    )
    detail_df.to_csv(out_dir / "fb_validation_id_bakeoff_by_week.csv", index=False)
    summary_df.to_csv(out_dir / "fb_validation_id_bakeoff_summary.csv", index=False)
    return summary_df


def assert_daily_weekly_exact(daily: pd.DataFrame, weekly: pd.DataFrame, day_sources: pd.DataFrame) -> None:
    weeks = complete_weeks(day_sources)
    d = daily.copy()
    d["week"] = week_start(d["date"])
    d = d[d["week"].isin(weeks)]
    chk = d.groupby(["endpoint_id", "week"], as_index=False)["metric_value"].sum()
    chk = chk.rename(columns={"week": "date", "metric_value": "daily_sum"})
    w = weekly[["endpoint_id", "date", "metric_value"]].rename(columns={"metric_value": "weekly_value"})
    merged = chk.merge(w, on=["endpoint_id", "date"], how="outer")
    if len(merged) != len(w) or not (merged["daily_sum"].fillna(-1).to_numpy() == merged["weekly_value"].fillna(-2).to_numpy()).all():
        bad = merged[merged["daily_sum"].fillna(-1) != merged["weekly_value"].fillna(-2)].head()
        raise AssertionError(f"weekly != daily sums; sample {bad.to_dict(orient='records')}")


def write_derived_from_aggregate(
    daily_agg: pd.DataFrame,
    day_sources: pd.DataFrame,
    winner: str,
    derived_dir: Path,
    manifest_dir: Path,
    manifest: Path,
    ssd_root: Path,
) -> tuple[Path, Path, int, int]:
    daily = final_daily_from_aggregate(daily_agg, winner)
    if daily.duplicated(["endpoint_id", "date"]).any():
        raise AssertionError("duplicate (endpoint_id, date) in fb_daily")
    if (daily["metric_value"] < 0).any():
        raise AssertionError("negative metric_value in fb_daily")
    daily_path = derived_dir / "fb_daily.parquet"
    daily.to_parquet(daily_path, index=False)

    weeks = complete_weeks(day_sources)
    weekly = weekly_from_daily(daily_agg, winner, weeks)
    if weekly.duplicated(["endpoint_id", "date"]).any():
        raise AssertionError("duplicate (endpoint_id, date) in fb_weekly")
    if (weekly["metric_value"] < 0).any():
        raise AssertionError("negative metric_value in fb_weekly")
    weekly_path = derived_dir / "fb_weekly_rebuilt.parquet"
    weekly.to_parquet(weekly_path, index=False)
    assert_daily_weekly_exact(daily, weekly, day_sources)

    clean_beyond = sorted(w for w in weeks if w > pd.Timestamp("2022-06-27"))
    coverage = pd.DataFrame(
        {
            "week": sorted(weeks),
            "complete": True,
            "beyond_2022_06_27": [w > pd.Timestamp("2022-06-27") for w in sorted(weeks)],
        }
    )
    coverage_path = manifest_dir / "fb_complete_week_coverage.csv"
    coverage.to_csv(coverage_path, index=False)

    source_paths = sorted(set(day_sources.loc[day_sources["status"].eq("complete"), "source_path"]))
    params = json.dumps({"ssd_root": str(ssd_root), "endpoint_winner": winner}, sort_keys=True)
    rows = [
        file_manifest_row(
            coverage_path,
            role="manifest/facebook/week_coverage",
            sha256=sha256_file(coverage_path),
            source_paths="",
            source_bytes="",
            source_sha256="",
            script=SCRIPT_NAME,
            parameters=params,
            status="ok",
        ),
        file_manifest_row(
            daily_path,
            role="derived/facebook/daily",
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
            role="derived/facebook/weekly",
            sha256=sha256_file(weekly_path),
            source_paths=source_paths,
            source_bytes="",
            source_sha256="",
            script=SCRIPT_NAME,
            parameters=params,
            status="ok",
            notes=f"complete weeks beyond 2022-06-27: {len(clean_beyond)}",
        ),
    ]
    upsert_manifest(manifest, rows)
    return daily_path, weekly_path, len(daily), len(weekly)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ssd-root", default="/Volumes/T9/rank-diffusion-data")
    ap.add_argument("--trusted-weekly", default="data/raw/fb_ranked_weekly_cutdown.parquet")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--rebuild-derived-only", action="store_true")
    ap.add_argument("--endpoint-winner", default="")
    args = ap.parse_args()

    ssd_root = Path(args.ssd_root)
    raw_root = ssd_root / "raw_small" / "facebook"
    manifest_dir = ssd_root / "manifest"
    manifest = manifest_dir / "MANIFEST.csv"
    aggregates_dir = ssd_root / "aggregates" / "facebook"
    derived_dir = ssd_root / "derived"

    day_sources_path = manifest_dir / "fb_day_sources.csv"
    completeness_path = manifest_dir / "fb_completeness.csv"
    daily_agg_path = aggregates_dir / "fb_daily_aggregates.parquet"
    summary_path = manifest_dir / "fb_validation_id_bakeoff_summary.csv"

    if args.rebuild_derived_only:
        ensure_layout(ssd_root)
        day_sources = pd.read_csv(day_sources_path, parse_dates=["date"])
        daily_agg = pd.read_parquet(daily_agg_path)
        winner = args.endpoint_winner
        if not winner:
            summary = pd.read_csv(summary_path).sort_values(
                ["mean_metric_correlation", "mean_trusted_join_rate"], ascending=False
            )
            winner = str(summary.iloc[0]["candidate"])
        daily_path, weekly_path, daily_rows, weekly_rows = write_derived_from_aggregate(
            daily_agg, day_sources, winner, derived_dir, manifest_dir, manifest, ssd_root
        )
        print(f"Rebuilt daily: {daily_path} rows={daily_rows:,}")
        print(f"Rebuilt weekly: {weekly_path} rows={weekly_rows:,}")
        return

    print("Building FB day source table")
    if args.dry_run:
        print(f"DRY RUN: would read copied FB raw files from {raw_root}")
        return
    ensure_layout(ssd_root)
    day_sources = build_day_sources(raw_root, day_sources_path)
    day_sources.to_csv(completeness_path, index=False)
    complete = day_sources[day_sources["status"].eq("complete")]
    print(f"Complete days: {len(complete)} / {len(day_sources)}")
    print(day_sources["source_kind"].value_counts(dropna=False).to_string())

    frames = []
    patch_dates = set(pd.to_datetime(day_sources.loc[day_sources["source_kind"].eq("full_fb"), "date"]))
    selected_backfill = day_sources[day_sources["source_kind"].eq("backfill")]
    for i, row in enumerate(selected_backfill.itertuples(index=False), 1):
        if i % 50 == 1 or i == len(selected_backfill):
            print(f"Aggregating backfill day {i}/{len(selected_backfill)}: {row.date.date()}", flush=True)
        frames.append(aggregate_one_backfill(Path(row.source_path)))
    if patch_dates:
        print(f"Aggregating {len(patch_dates)} full_fb patch days")
        frames.append(aggregate_full_fb_patch(raw_root / "crowdtangle" / "full_fb.parquet", patch_dates))
    daily_agg = pd.concat(frames, ignore_index=True)
    daily_agg = daily_agg.groupby(
        ["date", "account_id", "account_platform_id", "account_name", "source_kind"],
        as_index=False,
        sort=False,
    ).agg(
        {
            "metric_value": "sum",
            **{c: "sum" for c in COMPONENT_COLS},
            "post_count": "sum",
            "interactions_max": "max",
        }
    )
    daily_agg.to_parquet(daily_agg_path, index=False)
    print(f"Wrote aggregate: {daily_agg_path} rows={len(daily_agg):,}")

    summary = validation_bakeoff(daily_agg, day_sources, Path(args.trusted_weekly), manifest_dir)
    print("ID bake-off summary:")
    print(summary.to_string(index=False))
    winner = str(summary.iloc[0]["candidate"])
    print(f"Winning endpoint candidate: {winner}")

    daily_path, weekly_path, daily_rows, weekly_rows = write_derived_from_aggregate(
        daily_agg, day_sources, winner, derived_dir, manifest_dir, manifest, ssd_root
    )
    clean_beyond = sorted(w for w in complete_weeks(day_sources) if w > pd.Timestamp("2022-06-27"))

    source_paths = sorted(set(day_sources.loc[day_sources["status"].eq("complete"), "source_path"]))
    produced = [
        (day_sources_path, "manifest/facebook/day_sources"),
        (completeness_path, "manifest/facebook/completeness"),
        (manifest_dir / "fb_validation_id_bakeoff_by_week.csv", "manifest/facebook/validation"),
        (manifest_dir / "fb_validation_id_bakeoff_summary.csv", "manifest/facebook/validation"),
        (daily_agg_path, "aggregate/facebook/daily"),
    ]
    rows = []
    params = json.dumps({"ssd_root": str(ssd_root), "endpoint_winner": winner}, sort_keys=True)
    for path, role in produced:
        rows.append(
            file_manifest_row(
                path,
                role=role,
                sha256=sha256_file(path),
                source_paths=source_paths if role.startswith(("aggregate", "derived")) else "",
                source_bytes="",
                source_sha256="",
                script=SCRIPT_NAME,
                parameters=params,
                status="ok",
                notes=f"complete weeks beyond 2022-06-27: {len(clean_beyond)}" if role.endswith("weekly") else "",
            )
        )
    upsert_manifest(manifest, rows)
    print(f"Wrote daily: {daily_path} rows={daily_rows:,}")
    print(f"Wrote weekly: {weekly_path} rows={weekly_rows:,}")
    print(f"Complete weekly weeks beyond 2022-06-27: {len(clean_beyond)}")


if __name__ == "__main__":
    main()
