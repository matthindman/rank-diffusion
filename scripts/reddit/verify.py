#!/usr/bin/env python3
"""
Verify the merged Reddit daily parquet for completeness and sanity.

Checks date coverage, subreddit counts, top subreddits, metric distributions,
and confirms compatibility with the rankdiff pipeline.

Usage:
    python verify.py
    python verify.py --input data/reddit/reddit_daily.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def check_date_coverage(df: pd.DataFrame) -> None:
    """Check for gaps in date coverage."""
    dates = pd.to_datetime(df["date"]).dt.date
    all_days = pd.date_range(dates.min(), dates.max(), freq="D").date
    present_days = set(dates.unique())
    missing = sorted(set(all_days) - present_days)

    print(f"\nDate coverage:")
    print(f"  Range: {dates.min()} to {dates.max()}")
    print(f"  Expected days: {len(all_days):,}")
    print(f"  Present days: {len(present_days):,}")
    print(f"  Missing days: {len(missing)}")
    if missing and len(missing) <= 20:
        for d in missing:
            print(f"    {d}")
    elif missing:
        print(f"    First 10: {missing[:10]}")
        print(f"    Last 10: {missing[-10:]}")


def check_subreddit_counts(df: pd.DataFrame) -> None:
    """Show subreddit counts over time."""
    monthly = df.copy()
    monthly["month"] = pd.to_datetime(monthly["date"]).dt.to_period("M")
    subs_per_month = monthly.groupby("month")["endpoint_id"].nunique()

    print(f"\nSubreddits per month:")
    for month, count in subs_per_month.items():
        print(f"  {month}: {count:,}")


def check_top_subreddits(df: pd.DataFrame) -> None:
    """Verify well-known large subreddits are present and highly ranked."""
    expected_top = [
        "AskReddit", "funny", "pics", "gaming", "worldnews",
        "videos", "todayilearned", "movies", "news", "science",
    ]

    total_metric = df.groupby("endpoint_id")["metric_value"].sum().sort_values(ascending=False)

    print(f"\nTop 20 subreddits by total metric_value:")
    for i, (sub, val) in enumerate(total_metric.head(20).items(), 1):
        marker = " *" if sub in expected_top else ""
        print(f"  {i:>3}. r/{sub}: {val:,.0f}{marker}")

    print(f"\nExpected large subreddits check:")
    for sub in expected_top:
        if sub in total_metric.index:
            rank = (total_metric.index.get_loc(sub)) + 1
            print(f"  r/{sub}: rank {rank:,} (metric_value={total_metric[sub]:,.0f})")
        else:
            print(f"  r/{sub}: NOT FOUND")


def check_metric_distribution(df: pd.DataFrame) -> None:
    """Show distribution statistics for metric_value."""
    mv = df["metric_value"]

    print(f"\nmetric_value distribution:")
    print(f"  count: {len(mv):,}")
    print(f"  mean:  {mv.mean():,.1f}")
    print(f"  std:   {mv.std():,.1f}")
    print(f"  min:   {mv.min():,.0f}")
    print(f"  25%:   {mv.quantile(0.25):,.0f}")
    print(f"  50%:   {mv.median():,.0f}")
    print(f"  75%:   {mv.quantile(0.75):,.0f}")
    print(f"  90%:   {mv.quantile(0.90):,.0f}")
    print(f"  99%:   {mv.quantile(0.99):,.0f}")
    print(f"  max:   {mv.max():,.0f}")

    zeros = (mv == 0).sum()
    print(f"  zeros: {zeros:,} ({zeros/len(mv)*100:.1f}%)")

    # Per-metric breakdown
    for col in ["submission_karma", "comment_karma", "submission_count", "comment_count"]:
        if col in df.columns:
            print(f"\n  {col}: mean={df[col].mean():,.1f}, median={df[col].median():,.0f}, max={df[col].max():,.0f}")


def check_cadence(df: pd.DataFrame) -> None:
    """Verify the pipeline would detect daily cadence."""
    ts = pd.to_datetime(df["date"])
    ordered = ts.drop_duplicates().sort_values()
    diffs = ordered.diff().dropna().dt.days
    median_gap = diffs.median()

    print(f"\nCadence check:")
    print(f"  Median gap between timestamps: {median_gap:.1f} days")
    print(f"  infer_cadence would return: {'daily' if median_gap <= 2 else 'weekly'}")


def check_rows_per_day(df: pd.DataFrame) -> None:
    """Show rows (subreddits) per day statistics."""
    rpd = df.groupby("date").size()

    print(f"\nRows per day:")
    print(f"  mean:   {rpd.mean():,.0f}")
    print(f"  median: {rpd.median():,.0f}")
    print(f"  min:    {rpd.min():,} (on {rpd.idxmin()})")
    print(f"  max:    {rpd.max():,} (on {rpd.idxmax()})")


def main():
    parser = argparse.ArgumentParser(description="Verify Reddit daily parquet.")
    parser.add_argument(
        "--input",
        default=None,
        help="Path to reddit_daily.parquet",
    )
    args = parser.parse_args()

    if args.input:
        path = Path(args.input)
    else:
        repo_root = Path(__file__).resolve().parent.parent.parent
        path = repo_root / "data" / "reddit" / "reddit_daily.parquet"

    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading: {path}")
    df = pd.read_parquet(path)
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Dtypes:\n{df.dtypes.to_string()}")

    check_date_coverage(df)
    check_rows_per_day(df)
    check_subreddit_counts(df)
    check_top_subreddits(df)
    check_metric_distribution(df)
    check_cadence(df)

    print(f"\n{'='*50}")
    print("Verification complete.")


if __name__ == "__main__":
    main()
