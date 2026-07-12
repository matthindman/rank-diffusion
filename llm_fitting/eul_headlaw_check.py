#!/usr/bin/env python3
"""Eulerian-constraint validation, head-law readout (A2 adoption pipeline,
step 1 of the registered gates): S(1)/S(10) within recorded top-M on the
DEVELOPMENT panels, flag-off vs --eul-level, 10 seeds each arm, plus the
head-knot partition readout (kappa, sigma_perm, implied stationary level).

DEV PANELS ONLY (facebook_a / reddit_comments / reddit) — the extension is
not touched here; per A2, extension results on the fix are exploratory and
independent confirmation needs data this program has not used.

Both arms are (re)measured under the CURRENT NNLS defaults: the recorded
§2z-b sim-S(1) baselines predate the A4 NNLS re-freeze and are not assumed.

Usage: python3 llm_fitting/eul_headlaw_check.py facebook_a
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import community_metrics as cm  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

SEEDS = range(10)
STACKS = {
    "facebook_a": (3500, dict(temper=True, min_knot_n=8, md_lags=6,
                              t_tails=True, md_vr_long=True, stat_factor=True,
                              two_scale=True, mix_hetero=True)),
    "reddit_comments": (12500, dict(temper=True, min_knot_n=8, md_lags=6,
                                    t_tails=True, md_vr_long=True,
                                    stat_factor=True, two_scale=True,
                                    mix_hetero=True)),
    "reddit": (5000, dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True)),
}


def implied_head_level(p, n=4):
    a = 1.0 - np.asarray(p.kappa_z[:n])
    W = np.asarray(p.sigma_perm[:n]) ** 2 / np.clip(1 - a ** 2, 1e-9, None)
    V = np.asarray(p.sigma_trans[:n]) ** 2 / np.clip(
        1 - np.asarray(p.phi[:n]) ** 2, 1e-9, None)
    tot = W + V + np.asarray(p.sigma_obs[:n]) ** 2
    if p.sigma_trans2 is not None:
        tot += np.asarray(p.sigma_trans2[:n]) ** 2 / np.clip(
            1 - np.asarray(p.phi2[:n]) ** 2, 1e-9, None)
    return float(np.mean(tot))


def arm(df, T, score_k, kw, eul, ers):
    p = mrd.estimate(df, eul_level=eul, **kw)
    tag = "eul-level ON " if eul else "flag OFF     "
    print(f"  [{tag}] kappa head4 {np.array2string(np.asarray(p.kappa_z[:4]), precision=4)}  "
          f"sigma_perm head4 {np.array2string(np.asarray(p.sigma_perm[:4]), precision=4)}  "
          f"implied head stationary level {implied_head_level(p):.4f}", flush=True)
    s1s, s10s, offa = [], [], []
    for s in SEEDS:
        rs = mrd._sim_struct(mrd.simulate(p, T, seed=s, top_record=score_k))[3]
        s1s.append(cm.top_share(rs, 1))
        s10s.append(cm.top_share(rs, 10))
        offa.append(cm.head_offset(ers, rs, level_adjust=True))
    print(f"  [{tag}] sim S(1) {np.mean(s1s):.4f} ± {np.std(s1s):.4f}   "
          f"S(10) {np.mean(s10s):.4f} ± {np.std(s10s):.4f}   "
          f"offset(adj) {np.mean(offa):+.4f} ± {np.std(offa):.4f}   "
          f"(10 seeds, ddof=0)", flush=True)
    return p


def main():
    plat = sys.argv[1]
    top_k, kw = STACKS[plat]
    df = mrd.load_panel(mrd.PLATFORMS[plat])
    df = mrd.restrict_universe(df, top_k, buffer_mult=4)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    _, _, _, ers = mrd.empirical_structures(df, 10, topid_k=score_k)
    M = min(ers.shape[1], 2000)
    print(f"== {plat}  T={T}  score_k={score_k}  shares within recorded "
          f"top-{M} ==")
    print(f"  empirical S(1) {cm.top_share(ers, 1):.4f}   "
          f"S(10) {cm.top_share(ers, 10):.4f}", flush=True)
    arm(df, T, score_k, kw, False, ers)
    arm(df, T, score_k, kw, True, ers)


if __name__ == "__main__":
    main()
