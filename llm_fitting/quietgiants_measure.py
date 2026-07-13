#!/usr/bin/env python3
"""Follow-up measurements to the anatomy session (2z-x list, all three).
Measurement only. Declared predictions, written before running:

  Q1 (quiet-giants test): the empirical head is populated by relatively
     LOW-amplitude entities — top-10 / 11-40 relative realized vol
     (vs the 301-1000 reference bin) is LOWER empirically than in the sim,
     and within-top-100 Spearman(vol_i, perm_rank_i) is POSITIVE
     empirically (quieter toward #1) vs ~0 in the sim (v_i drawn
     rank-independent; only occupancy selection can induce correlation).
  Q2 (contender-stratum tails, now POWERED via direct head panels): given
     the A2 inversion at mid ranks, if emp >= sim here too, tail-shape
     surgery is dead at ALL ranks and the allocation/rank-amplitude story
     stands alone. (A2's original direction is already refuted; this
     closes the last unpowered stratum.)
  Q3 (fast=0 by band + returns, extension, 3 seeds): if fast=0 normalizes
     the SHELL but not the CORE excess, a second core mechanism remains;
     if it normalizes both, the fast layer carries the whole boundary
     story.

Construction (identical emp/sim, declared): head panels = all entities
with permanent rank <= 300 (emp: direct pivot from the universe panel;
sim: tracked-sample columns with perm rank <= 300, pooled across 10
seeds). Changes net of the HEAD-PANEL cross-sectional mean change (same
rule both sides); per-entity SD for vol; standardization by own SD for
tails; one-sided ratios; n >= 500 floor.
"""
from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

LONG = dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True,
            md_vr_long=True, stat_factor=True, two_scale=True,
            mix_hetero=True)
BINS = ((1, 10), (11, 40), (41, 100), (101, 300))
REF = (301, 1000)


def head_stats(vals: np.ndarray, perm: np.ndarray, tag: str):
    """vals (T, n) value panel; perm (n,) permanent ranks. Prints the vol
    profile (bin median SD / reference median SD), within-top-100 Spearman,
    and one-sided standardized q99/q90 for strata 2-20 and 21-100."""
    dv = np.diff(vals, axis=0)
    u = dv - np.nanmean(dv, axis=1, keepdims=True)
    sd = np.nanstd(u, axis=0)
    ok = np.isfinite(sd) & (sd > 0) & (np.isfinite(u).sum(axis=0) >= 12)
    u, sd, perm = u[:, ok], sd[ok], perm[ok]
    ref = (perm >= REF[0]) & (perm <= REF[1])
    ref_med = np.median(sd[ref]) if ref.sum() >= 20 else np.nan
    prof = []
    for lo, hi in BINS:
        m = (perm >= lo) & (perm <= hi)
        prof.append(f"{lo}-{hi}: {np.median(sd[m]) / ref_med:.3f} (n={m.sum()})"
                    if m.sum() >= 5 and np.isfinite(ref_med) else f"{lo}-{hi}: n/a")
    print(f"  {tag} vol profile (bin median SD / ref {REF[0]}-{REF[1]}): "
          + "  ".join(prof))
    top = perm <= 100
    if top.sum() >= 20:
        from scipy.stats import spearmanr
        rho = spearmanr(sd[top], perm[top]).statistic
        print(f"  {tag} within-top-100 Spearman(vol, perm rank) = {rho:+.3f} "
              f"(positive = quieter toward #1)  n={top.sum()}")
    z = u / sd
    for lo, hi, name in ((2, 20, "contender2-20"), (21, 100, "21-100")):
        m = (perm >= lo) & (perm <= hi)
        x = z[:, m].ravel()
        x = x[np.isfinite(x)]
        line = f"  {tag} tails {name} (n={len(x)}):"
        for side, arr in (("pos", x[x > 0]), ("neg", -x[x < 0])):
            if len(arr) >= 500:
                q90, q99 = np.percentile(arr, [90, 99])
                line += f"  {side} q99/q90 {q99 / q90:.3f}"
        print(line, flush=True)


def dev(plat, top_k):
    df = mrd.load_panel(mrd.PLATFORMS[plat])
    df = mrd.restrict_universe(df, top_k, buffer_mult=4)
    sk = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    print(f"== {plat} T={T} ==")
    pr = df.groupby("entity_id")["rank"].mean().sort_values()
    head_ids = pr.index[pr <= 1000]
    sub = df[df["entity_id"].isin(head_ids)]
    piv = sub.pivot_table(index="period", columns="entity_id", values="X").reindex(range(T))
    perm_e = pr[piv.columns].to_numpy()
    head_stats(piv.to_numpy(dtype=float), perm_e, "emp")
    p = mrd.estimate(df, **LONG)
    vs, ps = [], []
    for s in range(10):
        sim = mrd.simulate(p, T, seed=s, top_record=sk)
        tv = np.asarray(sim["tvals"], dtype=float)
        tr = np.asarray(sim["tranks"], dtype=float)
        pm = np.nanmean(np.where(tr > 0, tr, np.nan), axis=0)
        keep = np.isfinite(pm) & (pm <= 1000)
        vs.append(tv[:, keep])
        ps.append(pm[keep])
    head_stats(np.concatenate(vs, axis=1), np.concatenate(ps),
               "sim (10 seeds pooled)")


def ext_bands():
    print("== Q3 (EXPLORATORY, extension): fast=0 vs baseline, full boundary "
          "decomposition, 3 seeds ==")
    import ext_boundary_flux as ebf
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    df = mrd.restrict_universe(df, 12500, buffer_mult=4)
    sk = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    p = mrd.estimate(df, **LONG)
    z = np.zeros_like(np.asarray(p.sigma_trans))
    for tag, p2 in (("baseline", p), ("fast=0", replace(p, sigma_trans=z))):
        for s in range(3):
            d = ebf.flux_decomp(mrd.simulate(p2, T, seed=s, top_record=sk)["top_ids"], K=sk)
            ebf.show(f"{tag} s{s}", d)
    print("  (emp: outflux 0.0861  ret4 0.395  perm 0.157  "
          "core/mid/shell 0.0011/0.0406/0.0444)")


if __name__ == "__main__":
    what = sys.argv[1]
    if what == "fb":
        dev("facebook_a", 3500)
    elif what == "comments":
        dev("reddit_comments", 12500)
    elif what == "ext":
        ext_bands()
