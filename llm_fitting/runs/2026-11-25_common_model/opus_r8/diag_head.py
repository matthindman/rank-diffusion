#!/usr/bin/env python3
"""D3 (Round 8): why does R6 S2 over-move the Reddit top-100 IN SAMPLE?

TRAINING-WINDOW-ONLY diagnostic. For a training window [0, T0) the model is fitted on [0, T0) and
then simulated forward from its filtered state at week T0 - W (W = 52, a lookback inside the
training window), so the simulated weeks [T0 - W, T0) can be compared with the OBSERVED training
weeks [T0 - W, T0). No test-window data are read.

For the cohort of the top-200 at the first compared week it reports, data vs model:
  - median |d rank| over 1, 4, 13 weeks (entities with rank <= 100 at t), the gate's statistic,
  - median |d Xc| over the same horizons for the same entities (Xc = X - top-K median),
  - median local ladder gap (Xc spacing per rank) among ranks 1..100,
and repeats the simulation with single components switched off (jumps, lasting core, entity amplitude
heterogeneity) to attribute the excess.
Writes only into opus_r8/.
"""
from __future__ import annotations

import json
import math
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr  # noqa: E402

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")


def load(pn):
    d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
    return d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])


def cohort_stats(Xw, Rw, K, hs=(1, 4, 13), cap=100):
    """Gate-style statistics on a window (T x N) of observed or simulated X/R."""
    T = Xw.shape[0]
    med = np.array([np.nanmedian(Xw[t, np.isfinite(Rw[t]) & (Rw[t] <= K)]) for t in range(T)])
    Xc = Xw - med[:, None]
    coh = np.flatnonzero(np.isfinite(Rw[0]) & (Rw[0] <= 200))
    R, Xcc = Rw[:, coh], Xc[:, coh]
    out = {}
    for h in hs:
        r0, rh = R[:-h], R[h:]
        m = np.isfinite(r0) & np.isfinite(rh) & (r0 <= cap)
        out[f"dR{h}"] = float(np.median(np.abs(rh[m] - r0[m])))
        dx = np.abs(Xcc[h:] - Xcc[:-h])[m]
        out[f"dX{h}"] = float(np.median(dx[np.isfinite(dx)]))
    gaps = []
    for t in range(T):
        ok = np.flatnonzero(np.isfinite(Rw[t]) & (Rw[t] <= 101))
        if ok.size > 10:
            v = np.sort(Xc[t, ok])[::-1]
            gaps.append(np.median(-np.diff(v)))
    out["gap"] = float(np.median(gaps))
    return out


def sim_from_filter(p, filt, T_sim, reps, seed0=0, unit_v=False):
    N = filt["L_hat"].size
    c11 = np.sqrt(np.maximum(filt["P11"], 1e-10))
    c21 = filt["P12"] / c11
    c22 = np.sqrt(np.maximum(filt["P22"] - c21 * c21, 1e-10))
    res = []
    for s in range(reps):
        rng = np.random.default_rng(seed0 + s)
        z1, z2 = rng.standard_normal(N), rng.standard_normal(N)
        L_i = filt["L_hat"] + c11 * z1
        xi_i = filt["xi_hat"] + c21 * z1 + c22 * z2
        if p.s_vol > 0.02 and not unit_v:
            lv = np.log(filt["v_hat"]) - 0.5 * filt["post_s_v"] ** 2 + filt["post_s_v"] * np.clip(rng.standard_normal(N), -2, 2)
            v_i = np.exp(np.clip(lv, -2.5 * p.s_vol, 2.5 * p.s_vol))
        else:
            v_i = np.ones(N)
        st = cr.CommonState(p, L_i, rng, xi0=xi_i, v0=v_i)
        Xs = np.empty((T_sim, N))
        Rs = np.full((T_sim, N), np.nan)
        for t in range(T_sim):
            x = st.step(apply_absence=True)
            alive = st.ids == np.arange(N)
            x = np.where(alive, x, np.nan)
            Xs[t] = x
            ok = np.flatnonzero(np.isfinite(x))
            o = ok[np.argsort(-x[ok], kind="stable")]
            Rs[t, o] = np.arange(1, o.size + 1)
        res.append(cohort_stats(Xs, Rs, p.K))
    return {k: float(np.mean([r[k] for r in res])) for k in res[0]}


def main():
    out = {}
    jobs = [("reddit_submissions_long", 106), ("reddit_submissions_long", 159), ("reddit_comments_ext", 106), ("facebook_a", 54)]
    for pn, T0 in jobs:
        X, R, fl, K = load(pn)
        W = 52 if T0 >= 104 else 21
        sb = None
        if pn == "facebook_a":
            js = json.loads((CACHE / "facebook_a_spec_b.json").read_text())
            sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
        Xtr, Rtr, ftr = X[:T0], R[:T0], fl[:T0]
        p = cr.estimate_common(Xtr, Rtr, K=K, floors=ftr, spec_b=sb, refine_drift=True, asymmetry=False)
        t_start = T0 - W
        filt = cr.filter_endpoints(Xtr[:t_start], Rtr[:t_start], p, floors_tr=ftr[:t_start])
        emp = cohort_stats(Xtr[t_start:T0], Rtr[t_start:T0], K)
        rows = {"data_train_window": emp}
        variants = {
            "model_full": (p, False),
            "no_jumps": (replace(p, jump_lam=0.0, jump_c=0.0), False),
            "no_lasting_no_jumps": (replace(p, jump_lam=0.0, jump_c=0.0, sigma_L0=1e-6), False),
            "unit_v": (p, True),
        }
        for nm, (pv, uv) in variants.items():
            rows[nm] = sim_from_filter(pv, filt, W, reps=6, seed0=500, unit_v=uv)
        print(f"\n===== {pn} T0={T0} window=[{t_start},{T0}) jump_lam={p.jump_lam} jump_c={p.jump_c:.3f} sL0={p.sigma_L0:.4f} beta={p.beta:+.3f} s_vol={p.s_vol:.2f}")
        for nm, r in rows.items():
            print(f"  {nm:22s} " + " ".join(f"{k}={v:.3g}" for k, v in r.items()))
        out[f"{pn}@{T0}"] = rows
    (HERE / "diag_head.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
