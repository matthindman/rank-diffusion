#!/usr/bin/env python3
"""EXACT per-origin train-only IG membership (2026-07-11, review round 3).

Round-2's union pre-cut fixed candidate EXCLUSION but not selection
EQUIVALENCE: `restrict_universe` re-ranks within the loaded panel, so
selecting 40k from the 67,524-account pre-cut yields a DIFFERENT member set
than full-population train-only selection (measured overlap only 79-83% --
the invalid logical step is documented in ig_trainsafe_check.py's original
docstring; that check verified candidate inclusion, not membership
equality).

This builder computes, for each gate origin T0, the exact FULL-POPULATION
train-only member set under the program's own rule -- absence-penalized
permanent rank over periods < T0, replicating restrict_universe's selection
block verbatim (rank = metric desc with entity_id tiebreak per week; absent
weeks at floor N_t + 1; mergesort id tiebreak) -- and writes them to
`ig_trainsafe_members.parquet` (columns: T0, entity_id).  The gate consumes
them via `rankdiff_kalman --member-ids-file`, which passes them to
`restrict_universe(member_ids=...)`: fixed membership, within-universe
reranking only.  Data availability is guaranteed by the union pre-cut
(`ig_hm_totals_ts.parquet`), whose candidate containment IS the property the
round-2 check verified.

Also prints, per origin, the overlap between exact membership and what
pre-cut-internal selection would have chosen (documents the round-2 error's
magnitude).

Usage:
  python llm_fitting/ig_trainsafe_members.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

FULL = "llm_fitting/ig_weekly_ranked.parquet"
PRECUT = "llm_fitting/ig_hm_totals_ts.parquet"
OUT = "llm_fitting/ig_trainsafe_members.parquet"
B_UNIVERSE = 40_000          # registered: K=10,000, buffer_mult=4
ORIGINS = (13, 20, 26, 32, 39)   # gate origins at T=52, test_len=13, n_splits=5


def canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Weeks 1..52 -> periods 0..51; metric>0; unique per-week ranks by
    (metric desc, entity_id asc) -- mirrors minimal_rankdiff._rank_within."""
    df = df.rename(columns={"user_name": "entity_id", "metric_value": "metric"})
    df["entity_id"] = df["entity_id"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    df["period"] = df["date"].map({w: k for k, w in enumerate(weeks)})
    df = df[(df["period"] > 0) & (df["metric"] > 0)].copy()
    df["period"] -= 1
    df = df.sort_values(["period", "metric", "entity_id"],
                        ascending=[True, False, True])
    df["rank"] = df.groupby("period").cumcount() + 1
    df["N"] = df.groupby("period")["entity_id"].transform("size")
    return df.reset_index(drop=True)


def select_members(win: pd.DataFrame, B: int) -> set:
    """restrict_universe's selection block, verbatim semantics."""
    n_periods = win["period"].nunique()
    floors = win.drop_duplicates("period").set_index("period")["N"] + 1.0
    g = win.groupby("entity_id")
    sum_rank = g["rank"].sum()
    sum_floor_present = (win["N"] + 1.0).groupby(win["entity_id"]).sum()
    perm_rank = (sum_rank + (floors.sum() - sum_floor_present)) / n_periods
    perm_rank = perm_rank.sort_index().sort_values(kind="mergesort")
    return set(perm_rank.index[:B])


def main() -> None:
    raw_full = pq.read_table(
        FULL, columns=["date", "user_name", "metric_value", "n_posts"]).to_pandas()
    full = canonical(raw_full[["date", "user_name", "metric_value"]].copy())
    pre = canonical(pd.read_parquet(
        PRECUT, columns=["date", "user_name", "metric_value"]))
    pre_ids = set(pre["entity_id"].unique())

    exact_sets = {}
    for T0 in ORIGINS:
        exact_sets[T0] = select_members(full[full["period"] < T0], B_UNIVERSE)
        internal = select_members(pre[pre["period"] < T0], B_UNIVERSE)
        overlap = len(exact_sets[T0] & internal) / B_UNIVERSE
        missing = len(exact_sets[T0] - pre_ids)
        print(f"  T0={T0:>3}: exact-vs-precut-internal overlap {overlap:.2%}"
              f"   exact ids missing from pre-cut data: {missing}")

    # Self-heal the union panel: the round-2 union was built with the
    # checker's tie-breaking (pandas appearance order), not the exact rule's
    # (entity_id tiebreak), so tie-boundary ids can be absent.  Append the
    # missing ids' rows from the full panel so the pre-cut is a data superset
    # under the EXACT rule, deterministically.
    need = set().union(*exact_sets.values()) - pre_ids
    if need:
        print(f"  appending {len(need)} exact-rule ids to {PRECUT}")
        raw_full["date"] = pd.to_datetime(raw_full["date"])
        weeks = np.sort(raw_full["date"].unique())
        t = raw_full["date"].map({w: k for k, w in enumerate(weeks)})
        add = raw_full[(t > 0) & (raw_full["metric_value"] > 0)
                       & raw_full["user_name"].astype(str).isin(need)]
        cur = pd.read_parquet(PRECUT)
        out_panel = (pd.concat([cur, add[cur.columns]], ignore_index=True)
                     .drop_duplicates(["date", "user_name"])
                     .sort_values(["date", "user_name"]))
        out_panel.to_parquet(PRECUT, index=False)
        print(f"  {PRECUT}: now {out_panel['user_name'].nunique():,} accounts")

    out = pd.concat([pd.DataFrame({"T0": T0, "entity_id": sorted(s)})
                     for T0, s in exact_sets.items()], ignore_index=True)
    out.to_parquet(OUT, index=False)
    print(f"wrote {OUT}: {len(out):,} rows "
          f"({len(ORIGINS)} origins x {B_UNIVERSE:,} exact train-only members)")


if __name__ == "__main__":
    main()
