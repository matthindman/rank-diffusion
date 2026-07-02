#!/usr/bin/env python3
"""
Merge monthly subreddit aggregate parquets into a single reddit_daily.parquet.

Reads all submissions_*.parquet and comments_*.parquet from the monthly
directory, joins them on (subreddit, date), computes derived metrics, applies
activity filters, and writes the final output for the rank-diffusion pipeline.

Usage:
    python merge.py
    python merge.py --monthly-dir data/reddit/monthly/ --output data/reddit/reddit_daily.parquet
    python merge.py --min-daily-activity 10 --min-active-days 30
    python merge.py --metric submission_karma
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


VALID_METRICS = [
    "total_karma",
    "submission_karma",
    "comment_karma",
    "total_activity",
    "submission_count",
    "comment_count",
]


def load_monthly_parquets(monthly_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load and concatenate all monthly parquets, separated by type."""
    sub_files = sorted(monthly_dir.glob("submissions_*.parquet"))
    com_files = sorted(monthly_dir.glob("comments_*.parquet"))

    if not sub_files and not com_files:
        print(f"Error: no parquet files found in {monthly_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(sub_files)} submission files, {len(com_files)} comment files")

    submissions = pd.DataFrame()
    if sub_files:
        submissions = pd.concat(
            [pd.read_parquet(f) for f in sub_files], ignore_index=True
        )
        # Aggregate in case a subreddit-day appears in multiple monthly files
        # (e.g., UTC boundary effects)
        submissions = (
            submissions.groupby(["subreddit", "date"], as_index=False)
            .agg({"score_sum": "sum", "count": "sum", "nsfw_count": "sum"})
        )

    comments = pd.DataFrame()
    if com_files:
        comments = pd.concat(
            [pd.read_parquet(f) for f in com_files], ignore_index=True
        )
        agg_cols = {"score_sum": "sum", "count": "sum"}
        if "nsfw_count" in comments.columns:
            agg_cols["nsfw_count"] = "sum"
        comments = (
            comments.groupby(["subreddit", "date"], as_index=False)
            .agg(agg_cols)
        )

    return submissions, comments


def merge_and_compute(
    submissions: pd.DataFrame,
    comments: pd.DataFrame,
) -> pd.DataFrame:
    """Join submissions and comments, compute all metric columns."""
    # Rename before join to avoid conflicts
    sub_rename = {"score_sum": "submission_karma", "count": "submission_count"}
    if "nsfw_count" in submissions.columns:
        sub_rename["nsfw_count"] = "submission_nsfw_count"
    sub_renamed = submissions.rename(columns=sub_rename)

    com_rename = {"score_sum": "comment_karma", "count": "comment_count"}
    if "nsfw_count" in comments.columns:
        com_rename["nsfw_count"] = "comment_nsfw_count"
    com_renamed = comments.rename(columns=com_rename)

    if sub_renamed.empty:
        merged = com_renamed.copy()
        merged["submission_karma"] = 0
        merged["submission_count"] = 0
    elif com_renamed.empty:
        merged = sub_renamed.copy()
        merged["comment_karma"] = 0
        merged["comment_count"] = 0
    else:
        merged = pd.merge(
            sub_renamed,
            com_renamed,
            on=["subreddit", "date"],
            how="outer",
        )

    # Fill NaN from outer join with 0
    fill_cols = ["submission_karma", "submission_count", "comment_karma", "comment_count"]
    for nsfw_col in ["submission_nsfw_count", "comment_nsfw_count"]:
        if nsfw_col in merged.columns:
            fill_cols.append(nsfw_col)
    for col in fill_cols:
        merged[col] = merged[col].fillna(0).astype("int64")

    # Compute derived metrics
    merged["total_karma"] = merged["submission_karma"] + merged["comment_karma"]
    merged["total_activity"] = merged["submission_count"] + merged["comment_count"]

    return merged


def apply_filters(
    df: pd.DataFrame,
    min_daily_activity: int,
    min_active_days: int,
    max_nsfw_frac: float,
) -> pd.DataFrame:
    """Apply activity filters to remove low-activity and NSFW subreddits."""
    n_before = len(df)
    subs_before = df["subreddit"].nunique()

    # Filter 0: remove NSFW subreddits based on over_18 fraction
    nsfw_cols = [c for c in df.columns if c.endswith("_nsfw_count")]
    if nsfw_cols:
        total_nsfw = df.groupby("subreddit")[nsfw_cols].sum().sum(axis=1)
        activity_cols = [c.replace("_nsfw_count", "_count") for c in nsfw_cols]
        total_posts = df.groupby("subreddit")[activity_cols].sum().sum(axis=1)
        nsfw_frac = total_nsfw / total_posts.clip(lower=1)
        nsfw_subs = nsfw_frac[nsfw_frac > max_nsfw_frac].index
        sfw_subs = nsfw_frac[nsfw_frac <= max_nsfw_frac].index
        df = df[df["subreddit"].isin(sfw_subs)].copy()
        subs_after_nsfw = df["subreddit"].nunique()
        print(
            f"  After NSFW filter (>{max_nsfw_frac:.0%}): "
            f"{subs_after_nsfw:,} subreddits kept, "
            f"{len(nsfw_subs):,} NSFW subreddits removed"
        )
        subs_before = subs_after_nsfw
        n_before = len(df)

    # Filter 1: minimum daily activity (submissions + comments)
    if min_daily_activity > 0:
        df = df[df["total_activity"] >= min_daily_activity].copy()
        print(
            f"  After min_daily_activity >= {min_daily_activity}: "
            f"{len(df):,} rows ({n_before - len(df):,} dropped)"
        )

    # Filter 2: minimum active days per subreddit
    if min_active_days > 0:
        days_per_sub = df.groupby("subreddit")["date"].nunique()
        keep_subs = days_per_sub[days_per_sub >= min_active_days].index
        df = df[df["subreddit"].isin(keep_subs)].copy()
        subs_after = df["subreddit"].nunique()
        print(
            f"  After min_active_days >= {min_active_days}: "
            f"{subs_after:,} subreddits ({subs_before - subs_after:,} dropped)"
        )

    return df


def finalize(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Set metric_value column and rename to pipeline schema."""
    # Compute the chosen metric_value, floored at 0
    df["metric_value"] = df[metric].clip(lower=0)

    # Rename to match the rankdiff pipeline expected columns
    out = df.rename(columns={"subreddit": "endpoint_id"})
    out = out.sort_values(["date", "endpoint_id"]).reset_index(drop=True)

    # Select columns: pipeline columns first, then extra metrics
    cols = [
        "date",
        "endpoint_id",
        "metric_value",
        "submission_karma",
        "comment_karma",
        "submission_count",
        "comment_count",
    ]
    return out[cols]


def main():
    parser = argparse.ArgumentParser(
        description="Merge monthly Reddit aggregates into final daily parquet."
    )
    parser.add_argument(
        "--monthly-dir",
        default=None,
        help="Directory containing monthly parquet files",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output parquet path",
    )
    parser.add_argument(
        "--metric",
        default="total_karma",
        choices=VALID_METRICS,
        help="Which metric to use as metric_value (default: total_karma)",
    )
    parser.add_argument(
        "--min-daily-activity",
        type=int,
        default=10,
        help="Minimum submissions + comments per subreddit-day (default: 10)",
    )
    parser.add_argument(
        "--min-active-days",
        type=int,
        default=30,
        help="Minimum days a subreddit must appear (default: 30)",
    )
    parser.add_argument(
        "--max-nsfw-frac",
        type=float,
        default=0.10,
        help="Remove subreddits where over_18 fraction exceeds this (default: 0.10)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent.parent
    monthly_dir = Path(args.monthly_dir) if args.monthly_dir else repo_root / "data" / "reddit" / "monthly"
    output_path = Path(args.output) if args.output else repo_root / "data" / "reddit" / "reddit_daily.parquet"

    print(f"Monthly dir: {monthly_dir}")
    print(f"Output: {output_path}")
    print(f"Metric: {args.metric}")
    print()

    # Load
    print("Loading monthly parquets...")
    submissions, comments = load_monthly_parquets(monthly_dir)
    print(f"  Submissions: {len(submissions):,} rows")
    print(f"  Comments: {len(comments):,} rows")

    # Merge
    print("\nMerging submissions and comments...")
    merged = merge_and_compute(submissions, comments)
    print(f"  Merged: {len(merged):,} rows, {merged['subreddit'].nunique():,} subreddits")
    print(f"  Date range: {merged['date'].min()} to {merged['date'].max()}")

    # Filter
    print("\nApplying filters...")
    filtered = apply_filters(merged, args.min_daily_activity, args.min_active_days, args.max_nsfw_frac)
    print(f"  Final: {len(filtered):,} rows, {filtered['subreddit'].nunique():,} subreddits")

    # Finalize
    out = finalize(filtered, args.metric)

    # Write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)
    print(f"\nWrote: {output_path} ({output_path.stat().st_size / 1e6:.1f} MB)")

    # Summary
    print(f"\nFinal dataset summary:")
    print(f"  Shape: {out.shape}")
    print(f"  Date range: {out['date'].min()} to {out['date'].max()}")
    print(f"  Unique subreddits: {out['endpoint_id'].nunique():,}")
    print(f"  Unique days: {out['date'].nunique():,}")
    print(f"  metric_value (mean): {out['metric_value'].mean():,.0f}")
    print(f"  metric_value (median): {out['metric_value'].median():,.0f}")
    top = out.groupby("endpoint_id")["metric_value"].sum().nlargest(10)
    print(f"\n  Top 10 subreddits by total {args.metric}:")
    for sub, val in top.items():
        print(f"    r/{sub}: {val:,.0f}")


if __name__ == "__main__":
    main()
