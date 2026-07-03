#!/usr/bin/env python3
"""Validate Facebook derived panels produced on the SSD."""

from __future__ import annotations

import argparse
import gc
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


COMPONENT_COLS = [
    "like_count",
    "share_count",
    "comment_count",
    "love_count",
    "wow_count",
    "haha_count",
    "sad_count",
    "angry_count",
    "thankful_count",
    "care_count",
]
SUM_COLS = ["metric_value", *COMPONENT_COLS, "post_count"]


def week_start(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s)
    return d - pd.to_timedelta(d.dt.weekday, unit="D")


def parquet_info(path: Path) -> dict[str, object]:
    pf = pq.ParquetFile(path)
    return {
        "path": str(path),
        "rows": pf.metadata.num_rows,
        "bytes": path.stat().st_size,
        "columns": pf.schema_arrow.names,
    }


def basic_checks(df: pd.DataFrame, label: str) -> dict[str, object]:
    date = pd.to_datetime(df["date"])
    per_period = df.groupby(date)["endpoint_id"].nunique()
    return {
        f"{label}_rows": int(len(df)),
        f"{label}_date_min": str(date.min().date()),
        f"{label}_date_max": str(date.max().date()),
        f"{label}_periods": int(date.nunique()),
        f"{label}_mean_entities_per_period": float(per_period.mean()),
        f"{label}_duplicate_keys": int(df.duplicated(["endpoint_id", "date"]).sum()),
        f"{label}_negative_metric_rows": int((df["metric_value"] < 0).sum()),
        f"{label}_date_tz": str(getattr(date.dt, "tz", None)),
    }


def exact_weekly_sum_check(
    daily: pd.DataFrame, weekly: pd.DataFrame, complete_weeks: set[pd.Timestamp]
) -> dict[str, object]:
    d = daily[["endpoint_id", "date", *SUM_COLS]].copy()
    d["date"] = week_start(d["date"])
    d = d[d["date"].isin(complete_weeks)].copy()
    daily_week = d.groupby(["endpoint_id", "date"], as_index=False, sort=False)[SUM_COLS].sum()
    compare = weekly[["endpoint_id", "date", *SUM_COLS]].merge(
        daily_week,
        on=["endpoint_id", "date"],
        how="outer",
        suffixes=("_weekly", "_daily"),
        indicator=True,
    )
    result: dict[str, object] = {
        "weekly_sum_compare_rows": int(len(compare)),
        "weekly_sum_left_only": int((compare["_merge"] == "left_only").sum()),
        "weekly_sum_right_only": int((compare["_merge"] == "right_only").sum()),
    }
    both = compare["_merge"] == "both"
    for col in SUM_COLS:
        w = compare.loc[both, f"{col}_weekly"].to_numpy()
        dsum = compare.loc[both, f"{col}_daily"].to_numpy()
        result[f"weekly_sum_{col}_mismatches"] = int(np.count_nonzero(w != dsum))
    return result


def top_entities(df: pd.DataFrame, date: pd.Timestamp, n: int = 5) -> list[dict[str, object]]:
    sample = df[pd.to_datetime(df["date"]).eq(date)].nlargest(n, "metric_value")
    return [
        {
            "endpoint_id": str(row.endpoint_id),
            "metric_value": int(row.metric_value),
        }
        for row in sample.itertuples(index=False)
    ]


def smoke_load(path: Path, repo_root: Path) -> dict[str, object]:
    sys.path.insert(0, str(repo_root / "llm_fitting"))
    import minimal_rankdiff as mrd  # noqa: PLC0415

    df = mrd.load_panel(
        {
            "path": str(path),
            "id_col": "endpoint_id",
            "ts_col": "date",
            "metric_col": "metric_value",
            "max_rank": None,
        }
    )
    sample_period = int(df["period"].median())
    sample = df[df["period"].eq(sample_period)].nsmallest(5, "rank")
    out = {
        "path": str(path),
        "rows": int(len(df)),
        "periods": int(df["period"].nunique()),
        "mean_entities_per_period": float(df.groupby("period")["entity_id"].nunique().mean()),
        "sample_period": sample_period,
        "sample_top5": [
            {"entity_id": str(row.entity_id), "metric": float(row.metric)}
            for row in sample.itertuples(index=False)
        ],
    }
    del df
    gc.collect()
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ssd-root", type=Path, default=Path("/Volumes/T9/rank-diffusion-data"))
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--skip-smoke", action="store_true")
    args = parser.parse_args()

    daily_path = args.ssd_root / "derived" / "fb_daily.parquet"
    weekly_path = args.ssd_root / "derived" / "fb_weekly_rebuilt.parquet"
    completeness_path = args.ssd_root / "manifest" / "fb_completeness.csv"
    coverage_path = args.ssd_root / "manifest" / "fb_complete_week_coverage.csv"
    bakeoff_path = args.ssd_root / "manifest" / "fb_validation_id_bakeoff_summary.csv"

    daily = pd.read_parquet(daily_path)
    weekly = pd.read_parquet(weekly_path)
    completeness = pd.read_csv(completeness_path, parse_dates=["date"])
    coverage = pd.read_csv(coverage_path, parse_dates=["week"])
    bakeoff = pd.read_csv(bakeoff_path)
    complete_col = "is_complete" if "is_complete" in coverage.columns else "complete"
    complete_week_set = set(coverage.loc[coverage[complete_col].astype(bool), "week"])

    summary: dict[str, object] = {
        "daily_file": parquet_info(daily_path),
        "weekly_file": parquet_info(weekly_path),
        **basic_checks(daily, "daily"),
        **basic_checks(weekly, "weekly"),
        "weekly_dates_all_monday": bool((pd.to_datetime(weekly["date"]).dt.weekday == 0).all()),
    }
    summary.update(exact_weekly_sum_check(daily, weekly, complete_week_set))

    sample_week = pd.Timestamp("2021-01-04")
    if sample_week not in set(pd.to_datetime(weekly["date"])):
        sample_week = pd.to_datetime(weekly["date"]).sort_values().iloc[len(weekly) // 2]
    summary["sample_week"] = str(sample_week.date())
    summary["sample_week_top5"] = top_entities(weekly, sample_week)

    summary["complete_days"] = int(completeness["status"].eq("complete").sum())
    summary["total_days"] = int(len(completeness))
    summary["complete_weeks"] = int(coverage[complete_col].sum())
    summary["total_weeks"] = int(len(coverage))
    summary["complete_weeks_beyond_2022_06_27"] = int(
        coverage[coverage["week"].gt(pd.Timestamp("2022-06-27"))][complete_col].sum()
    )
    summary["id_bakeoff"] = bakeoff.to_dict(orient="records")

    if not args.skip_smoke:
        summary["smoke_daily"] = smoke_load(daily_path, args.repo_root)
        summary["smoke_weekly"] = smoke_load(weekly_path, args.repo_root)

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
