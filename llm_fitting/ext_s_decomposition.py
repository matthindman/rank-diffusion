#!/usr/bin/env python3
"""EXPLORATORY (post-confirmation-report; MODEL_STATUS §2z-q line in
protocol §5 governs): decompose the E1 s failure (0.8319 vs band
[0.64, 0.74]) into era vs membership-composition vs window-length effects.

Design (2026-07-12 adjudicated plan, Phase 2a):
  four cells   = {train window [0,136), extension window [136,213)}
                 x {train-selected membership, extension-selected membership}
  controls     = E1-estimand replication (ext window, FULL-window membership;
                 must reproduce 0.8319), frozen-reference replication
                 (train/train; expect ~0.6922), and matched-77-week
                 sub-windows of the FROZEN span ([0,77), [59,136), own
                 membership) to bound segment-length bias.
Readout: s = estimate_temperament(min_changes=12), the E1 estimand.
Membership selection via restrict_universe(member_span=...); cross-cell
application via member_ids (fixed membership, weekly re-ranking only —
the §2z-f machinery). All slicing BEFORE estimation; periods re-indexed.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

K, BUF = 12_500, 4
T_TRAIN, T_FULL = 136, 213


def s_of(df, lo, hi, member_ids=None, member_span=None, label=""):
    u = mrd.restrict_universe(df, K, buffer_mult=BUF,
                              member_ids=member_ids, member_span=member_span)
    seg = u[(u["period"] >= lo) & (u["period"] < hi)].copy()
    seg["period"] -= lo
    s = mrd.estimate_temperament(seg, min_changes=12)["s"]
    print(f"  {label:<44} s = {s:.4f}", flush=True)
    return s


def ids_of(df, span):
    u = mrd.restrict_universe(df, K, buffer_mult=BUF, member_span=span)
    return set(u["entity_id"].unique())


def main():
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    print(f"extended panel loaded: T={df['ts'].nunique()} weeks")
    mem_tr = ids_of(df, (0, T_TRAIN))
    mem_ex = ids_of(df, (T_TRAIN, T_FULL))
    ov = len(mem_tr & mem_ex) / len(mem_tr | mem_ex)
    print(f"membership: train-selected B={len(mem_tr):,}  "
          f"ext-selected B={len(mem_ex):,}  Jaccard overlap {ov:.3f}")

    print("replication controls:")
    s_of(df, T_TRAIN, T_FULL, member_span=(0, T_FULL),
         label="ext window, FULL-window mem (E1 estimand)")
    print("four cells (era x membership):")
    sA = s_of(df, 0, T_TRAIN, member_ids=mem_tr, label="A train win, train mem (ref ~0.692)")
    sB = s_of(df, T_TRAIN, T_FULL, member_ids=mem_ex, label="B ext win,  ext mem")
    sC = s_of(df, 0, T_TRAIN, member_ids=mem_ex, label="C train win, ext mem")
    sD = s_of(df, T_TRAIN, T_FULL, member_ids=mem_tr, label="D ext win,  train mem")
    print("window-length controls (frozen span, own membership, 77 weeks):")
    sW1 = s_of(df, 0, 77, member_span=(0, 77), label="W1 frozen [0,77)")
    sW2 = s_of(df, 59, T_TRAIN, member_span=(59, T_TRAIN), label="W2 frozen [59,136)")

    era = ((sB - sA) + (sD - sC)) / 2.0
    comp = ((sC - sA) + (sB - sD)) / 2.0
    print(f"\ndecomposition (additive, mean of the two one-factor contrasts):")
    print(f"  era effect (window):        {era:+.4f}")
    print(f"  composition effect (mem):   {comp:+.4f}")
    print(f"  window-length bias bound:   frozen 77-wk windows give "
          f"{sW1:.4f}/{sW2:.4f} vs full-train {sA:.4f} "
          f"(if ~equal, segment length does not inflate s)")


if __name__ == "__main__":
    main()
