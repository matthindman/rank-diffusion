#!/usr/bin/env python3
"""P5 membership robustness (2026-07-03): on a LONG census panel, how sensitive
are the universe membership set and the headline metrics to the membership
window?

Owner caveat (MODEL_STATUS 2g): absence-penalized membership is suspect on long
panels -- entities that legitimately rose or died mid-panel are penalized for
weeks before birth / after death, so full-panel membership drifts toward
"always-existed" entities.  Fine at T=30; at T=136 (reddit comments) it must be
MEASURED.  This script measures; it does not redesign.

Probes (reddit_comments, owner scale K=12,500, B=50k):
  * membership sets from four windows: FULL (0..T), FIRST-HALF (0..T/2),
    SECOND-HALF (T/2..T), TRAILING-60 (T-60..T)
  * pairwise member-set overlap (share of B; B identical across variants)
  * headline-metric movement: the estimand-faithful in-sample card
    (temper + pool>=8 + md6 + t-tails, reps=3 -- declared: fewer reps than the
    canonical 5, this is a sensitivity probe, not a headline fit) under each
    membership, all scored on the SAME diagnostics.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

K, BUF, REPS = 12500, 4, 3


def members(uni) -> set:
    return set(uni["entity_id"].unique())


def card(df, uni, tag: str) -> None:
    score_k = uni.attrs["score_k"]
    T = int(uni["period"].max()) + 1
    top_k = max(10, int(round(0.01 * score_k)))
    ev, er, et, ers = mrd.empirical_structures(uni, top_k, topid_k=score_k)
    emp = mrd.diagnostics(ev, er, et, ers, top_k, score_k=score_k)
    p = mrd.estimate(uni, temper=True, min_knot_n=8, md_lags=6, t_tails=True)
    sims = [mrd.diagnostics(*mrd._sim_struct(mrd.simulate(p, T, seed=s, kappa=None,
                                                          top_record=score_k)),
                            top_k, score_k=score_k)
            for s in range(REPS)]
    sim = {k: np.nanmean([s[k] for s in sims]) for k in emp if not k.startswith("_")}
    npass, ntot, churn = mrd._score(emp, sim, top_k)
    keys = ["RACF1", "VR4", "dRank1", "dRank4", "coll1", "outfluxK", "return4K"]
    detail = "  ".join(f"{k}:{emp.get(k, np.nan):.3f}/{sim.get(k, np.nan):.3f}"
                       for k in keys)
    print(f"  {tag:<12} score {npass}/{ntot}  churn {churn:.3f}  s={p.temper_s:.3f}"
          f"  | emp/sim {detail}")


if __name__ == "__main__":
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments"])
    T = int(df["period"].max()) + 1
    windows = {
        "full": (0, T),
        "first-half": (0, T // 2),
        "second-half": (T // 2, T),
        "trailing-60": (T - 60, T),
    }
    unis = {tag: mrd.restrict_universe(df, K, buffer_mult=BUF, member_span=span)
            for tag, span in windows.items()}
    sets = {tag: members(u) for tag, u in unis.items()}
    B = BUF * K

    print(f"reddit_comments T={T}, K={K}, B={B}")
    print("\nmember-set overlap (share of B):")
    tags = list(windows)
    print("            " + "".join(f"{t:>13}" for t in tags))
    for a in tags:
        row = "".join(f"{len(sets[a] & sets[b]) / B:>13.3f}" for b in tags)
        print(f"  {a:<10}{row}")

    print(f"\nheadline-metric movement (temper+pool+md6+t, reps={REPS}, "
          f"scored on the same estimand):")
    for tag, uni in unis.items():
        card(df, uni, tag)
