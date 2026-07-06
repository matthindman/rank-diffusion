#!/usr/bin/env python3
"""Build IG high-measurability panels for the censoring-rescue runs
(pre-registered: ig_censoring_prereg.md Amendment 1).

For each metric variant (totals Y; per-post Y/M):
  1. weeks 1..52 (drop partial week 0);
  2. absence-penalized permanent rank over the FULL account population
     (absent weeks at floor N_t + 1) — the program's standard membership rule;
  3. keep the top 60,000 accounts by permanent rank (contains K + B = 50k
     with margin; same pre-cut pattern as ig_weekly_ranked_top50k, declared);
  4. write date/user_name/metric_value/n_posts parquet.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

FULL = "llm_fitting/ig_weekly_ranked.parquet"
KEEP = 60_000


def build(df: pd.DataFrame, metric: np.ndarray, out_path: str, keep: int = KEEP) -> None:
    d = df.copy()
    d["mv"] = metric
    d = d[d["mv"] > 0]
    T = d["t"].nunique()
    # within-week rank, 1 = largest
    d["r"] = d.groupby("t")["mv"].rank(ascending=False, method="first")
    floor = d.groupby("t")["r"].max() + 1.0          # N_t + 1 per week
    tot_floor = floor.sum()
    d["r_minus_floor"] = d["r"] - d["t"].map(floor)
    agg = d.groupby("user_name")["r_minus_floor"].sum()
    perm = (agg + tot_floor) / T                      # absence-penalized permanent rank
    keep_ids = perm.nsmallest(keep).index
    out = d[d["user_name"].isin(keep_ids)][["date", "user_name", "mv", "n_posts"]]
    out = out.rename(columns={"mv": "metric_value"}).sort_values(["date", "user_name"])
    out.to_parquet(out_path, index=False)
    pres = out.groupby("user_name")["date"].size()
    print(f"{out_path}: {len(out):,} rows, {out['user_name'].nunique():,} accounts, "
          f"T={T}; kept-account presence mean={pres.mean()/T:.3f} "
          f"median={pres.median()/T:.3f}")


def main() -> None:
    df = pq.read_table(FULL, columns=["date", "user_name", "metric_value", "n_posts"]).to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    df["t"] = df["date"].map({w: k for k, w in enumerate(weeks)})
    df = df[df["t"] > 0]                              # drop partial week 0
    print(f"full panel weeks 1..52: {len(df):,} rows, "
          f"{df['user_name'].nunique():,} accounts")

    y = df["metric_value"].to_numpy(float)
    m = df["n_posts"].to_numpy(float)
    build(df, y, "llm_fitting/ig_hm_totals.parquet")
    build(df, np.where(m > 0, y / np.maximum(m, 1), 0.0), "llm_fitting/ig_hm_perpost.parquet")


if __name__ == "__main__":
    main()
