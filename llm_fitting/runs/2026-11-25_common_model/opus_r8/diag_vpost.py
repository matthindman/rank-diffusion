#!/usr/bin/env python3
"""D4 (Round 8): does the R6 volatility posterior reproduce each head entity's own median |dX_1|?

TRAINING-WINDOW-ONLY. Fit S2 on [0, T0); filter on [0, Tf) with Tf = T0 - 52 (or T0 - 21 on FB).
For the top-200 cohort at Tf - 1 (by observed rank) it compares, entity by entity:
  m_in   = observed median |dX_1| on [0, Tf)      (the posterior's own input)
  m_post = model median |dX_1| implied at v_hat    (MC through the simulator's increment law)
  m_out  = observed median |dX_1| on [Tf, T0)      (later training weeks)
  m_sim  = simulated median |dX_1| from the filtered state over [Tf, T0)
Prints log-ratio summaries, the share of entities at the v grid edge, and the split of the model's
1-week variance into lasting vs passing at the head. Writes only into opus_r8/.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr  # noqa: E402

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")


def demeaned_dx(Xw, Rw, K):
    med = np.array([np.nanmedian(Xw[t, np.isfinite(Rw[t]) & (Rw[t] <= K)]) for t in range(Xw.shape[0])])
    d = np.diff(Xw - med[:, None], axis=0)
    return d - np.nanmean(d, axis=1, keepdims=True)


def model_median(p, z, sL, v, n=40000, seed=9):
    rng = np.random.default_rng(seed)
    pool = p.innov_pool
    dr = lambda: pool[rng.integers(0, pool.size, n)]
    xi = rng.standard_normal(n)
    for _ in range(60):
        xi = p.phi * xi + math.sqrt(1 - p.phi ** 2) * dr()
    xid = (p.phi - 1) * xi + math.sqrt(1 - p.phi ** 2) * dr()
    od = dr() - dr()
    jd = (rng.random(n) < p.jump_lam) * rng.standard_normal(n) if p.jump_lam > 0 else 0.0
    out = []
    for zi, sli, vi in zip(z, sL, v):
        smp = sli * dr() + cr._jump_sd_of(p, np.array([zi]))[0] * jd + math.sqrt(vi) * (
            math.sqrt(cr.v_xi_of(p, np.array([zi]))[0]) * xid + cr.sigma_obs_of(p, np.array([zi]))[0] * od)
        out.append(np.median(np.abs(smp)))
    return np.array(out)


def main():
    jobs = [("reddit_submissions_long", 106, 52), ("reddit_comments_ext", 106, 52), ("facebook_a", 54, 21)]
    res = {}
    for pn, T0, W in jobs:
        d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
        X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
        sb = None
        if pn == "facebook_a":
            js = json.loads((CACHE / "facebook_a_spec_b.json").read_text())
            sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
        p = cr.estimate_common(X[:T0], R[:T0], K=K, floors=fl[:T0], spec_b=sb, refine_drift=False, asymmetry=False)
        Tf = T0 - W
        filt = cr.filter_endpoints(X[:Tf], R[:Tf], p, floors_tr=fl[:Tf])
        coh = np.flatnonzero(np.isfinite(R[Tf - 1]) & (R[Tf - 1] <= 200))
        dx_in = demeaned_dx(X[:Tf], R[:Tf], K)[:, coh]
        dx_out = demeaned_dx(X[Tf:T0], R[Tf:T0], K)[:, coh]
        m_in = np.nanmedian(np.abs(dx_in), axis=0)
        m_out = np.nanmedian(np.abs(dx_out), axis=0)
        pen = np.where(np.isfinite(R[:Tf]), R[:Tf], fl[:Tf, None])
        N = X.shape[1]
        pr = np.empty(N)
        pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
        z = np.log(np.clip((pr[coh] - 0.5) / N, cr.Z_CLIP, 1))
        mx = np.nanmean(X[:Tf, coh], axis=0)
        sL = cr.sigma_L_of(p, mx)
        v = filt["v_hat"][coh]
        m_post = model_median(p, z, sL, v)
        m_floor = model_median(p, z, sL, np.full(v.size, math.exp(-2.5 * p.s_vol)))
        ok = np.isfinite(m_in) & np.isfinite(m_out) & (m_in > 0) & (m_out > 0)
        lr_post = np.log(m_post[ok] / m_in[ok])
        lr_drift = np.log(m_out[ok] / m_in[ok])
        at_edge = float(np.mean(m_floor[ok] > m_in[ok]))
        las_share = float(np.median(sL[ok] ** 2 / (sL[ok] ** 2 + v[ok] * (2 * cr.sigma_obs_of(p, z[ok]) ** 2 + 2 * (1 - p.phi) * cr.v_xi_of(p, z[ok])))))
        r = dict(n=int(ok.sum()), med_m_in=float(np.median(m_in[ok])), med_m_out=float(np.median(m_out[ok])), med_m_post=float(np.median(m_post[ok])),
                 med_logratio_post_vs_in=float(np.median(lr_post)), mean_logratio_post_vs_in=float(np.mean(lr_post)),
                 med_logratio_out_vs_in=float(np.median(lr_drift)), share_lasting_floor_exceeds_measured=at_edge,
                 median_lasting_share_of_1wk_var=las_share, v_hat_median=float(np.median(v[ok])), s_vol=p.s_vol)
        res[pn] = r
        print(f"\n===== {pn} T0={T0} Tf={Tf} cohort={ok.sum()}")
        for k, val in r.items():
            print(f"  {k:40s} {val:.4f}" if isinstance(val, float) else f"  {k:40s} {val}")
    (HERE / "diag_vpost.json").write_text(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
