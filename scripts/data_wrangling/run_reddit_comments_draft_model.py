#!/usr/bin/env python3
"""Run the draft rank-diffusion model on the shorter Reddit comments panel."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--weekly-path",
        type=Path,
        default=Path("/Volumes/T9/rank-diffusion-data/derived/reddit_comments_2018-12_2021-06_weekly.parquet"),
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--top-k", type=int, default=2500)
    parser.add_argument("--buffer-mult", type=int, default=4)
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--md-lags", type=int, default=6)
    parser.add_argument("--min-knot-entities", type=int, default=8)
    parser.add_argument("--temperament", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--t-tails", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    sys.path.insert(0, str(args.repo_root / "llm_fitting"))
    import minimal_rankdiff as mrd  # noqa: PLC0415

    platform = "reddit_comments_short"
    mrd.PLATFORMS[platform] = {
        "path": str(args.weekly_path),
        "id_col": "endpoint_id",
        "ts_col": "date",
        "metric_col": "metric_value",
        "max_rank": None,
    }
    # Measured on reddit_comments_2018-12_2021-06_weekly.parquet:
    # top-1000 ~= 81%, top-2500 ~= 91%, top-5000 ~= 96%.
    mrd.COVERAGE_K[platform] = {80: 1000, 90: 2500, 95: 5000}
    mrd.run_platform(
        platform,
        reps=args.reps,
        top_k_u=args.top_k,
        buffer_mult=args.buffer_mult,
        temper=args.temperament,
        min_knot_n=args.min_knot_entities,
        md_lags=args.md_lags,
        t_tails=args.t_tails,
    )


if __name__ == "__main__":
    main()
