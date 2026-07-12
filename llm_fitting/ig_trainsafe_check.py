#!/usr/bin/env python3
"""Containment check for the IG train-safe pre-cut (2026-07-11, external
review: the keep=60k pre-cut used FULL-window permanent rank -- future
leakage into the OOS gate's train-only membership; omitted share of the
train-selected 40k universe was 15.3% (T0=13) .. 1.1% (T0=39)).

For each gate origin T0, compute the TRAIN-ONLY (weeks < T0) absence-
penalized permanent-rank top-(K+B... = buffer B = 4K = 40k) selection from
the FULL account panel, and report the share NOT contained in a given
pre-cut.  A pre-cut with omitted share 0.0000 at every origin is a VERIFIED
SUPERSET of every train-only universe: the gate's per-split
restrict_universe(member_window=T0) then selects identically from the
pre-cut and from the full panel -- train-safe by construction.

Usage:
  python llm_fitting/ig_trainsafe_check.py llm_fitting/ig_hm_totals_k200.parquet
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

FULL = "llm_fitting/ig_weekly_ranked.parquet"
B_UNIVERSE = 40_000          # registered: K=10,000, buffer_mult=4
ORIGINS = (13, 20, 26, 32, 39)   # gate origins at T=52, test_len=13, n_splits=5


def perm_rank_ids(d: pd.DataFrame, top_n: int) -> set:
    """Absence-penalized permanent rank (absent weeks at floor N_t + 1) over
    the periods present in d; top_n best ids."""
    T = d["t"].nunique()
    r = d.groupby("t")["mv"].rank(ascending=False, method="first")
    d = d.assign(r=r)
    floor = d.groupby("t")["r"].max() + 1.0
    tot_floor = floor.sum()
    d["rmf"] = d["r"] - d["t"].map(floor)
    agg = d.groupby("user_name")["rmf"].sum()
    perm = (agg + tot_floor) / T
    return set(perm.nsmallest(top_n).index)


def main() -> None:
    precut_path = sys.argv[1]
    pre_ids = set(pd.read_parquet(precut_path, columns=["user_name"])
                  ["user_name"].unique())
    print(f"pre-cut {precut_path}: {len(pre_ids):,} accounts")

    df = pq.read_table(FULL, columns=["date", "user_name", "metric_value"]).to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    df["t"] = df["date"].map({w: k for k, w in enumerate(weeks)})
    df = df[(df["t"] > 0) & (df["metric_value"] > 0)]
    df["t"] -= 1                              # panel periods 0..51, as the gate sees them
    df = df.rename(columns={"metric_value": "mv"})

    worst = 0.0
    for T0 in ORIGINS:
        train_ids = perm_rank_ids(df[df["t"] < T0], B_UNIVERSE)
        omitted = len(train_ids - pre_ids) / len(train_ids)
        worst = max(worst, omitted)
        print(f"  T0={T0:>3}: train-only top-{B_UNIVERSE:,} omitted from pre-cut: "
              f"{omitted:.4%}")
    print(f"VERDICT: {'TRAIN-SAFE (verified superset)' if worst == 0 else 'NOT contained -- increase --keep'}"
          f"  (worst omitted share {worst:.4%})")


if __name__ == "__main__":
    main()
