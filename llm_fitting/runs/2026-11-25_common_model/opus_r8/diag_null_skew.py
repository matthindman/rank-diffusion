#!/usr/bin/env python3
"""D6 (Round 8): symmetric-null test of the S3 surge identification.

Simulate full panels (T = observed T, same N, K) from the fitted SYMMETRIC R6 S2 model (and from the
fitted R6 S3 model), starting from the deconvolved first-block ladder exactly as Gold Goal 1 does,
then apply to the simulated panel the SAME data-side estimators used on real data:
  - tail-asymmetry profile A_h(q) of pooled top-K (permanent rank, presence >= 0.5) increments,
  - the R6 S3 surge estimator (_estimate_stage3_surge) -> (lam, mu, rho).
If a symmetric world produces positive A / active surges, the observed skew is partly a selection /
censoring artifact of the measurement and the surge estimator is biased. Writes only into opus_r8/.
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
from diag_asym_model import A_profile, emp_profile, CACHE, PANELS  # noqa: E402


def surge_on(X, R, fl, K):
    T, N = X.shape
    pen = np.where(np.isfinite(R), R, fl[:, None])
    pr = np.empty(N)
    pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
    pres = np.isfinite(R).mean(axis=0)
    Xc = X - cr._top_k_center(X, R, K)
    edges = cr._band_edges(K, N)
    nb = len(edges) - 1
    band_all = np.clip(np.searchsorted(edges, pr, side="right") - 1, 0, nb - 1)
    band_of = np.where(pres >= 0.5, band_all, -1)
    H = int(min(52, max(6, T // 2)))
    D = cr._structure_function(Xc, band_of, nb, H)
    with np.errstate(all="ignore"):
        D_med = np.nanmedian(D[:-1], axis=0)
    lam, mu, rho, dg = cr._estimate_stage3_surge(Xc, pr, pres, K, D_med)
    return lam, mu, rho


def main():
    res = {}
    for pn, has_sb in PANELS.items():
        d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
        X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
        sb = None
        if has_sb:
            js = json.loads((CACHE / f"{pn}_spec_b.json").read_text())
            sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
        T = X.shape[0]
        hs = [h for h in (4, 13, 26, 52) if h <= T // 2]
        e = emp_profile(X, R, fl, K, hs)
        row = {"data": {h: e[h].tolist() for h in hs}, "data_surge": surge_on(X, R, fl, K)}
        block = {"facebook_a": 26, "instagram_hm": 13}.get(pn, 52)
        m_all = np.nanmean(X, axis=0)
        with np.errstate(all="ignore"):
            m0 = np.nanmean(X[:block], axis=0)
        m0 = np.where(np.isfinite(m0), m0, np.where(np.isfinite(m_all), m_all, float(np.nanmin(m_all))))
        for arm, asym in (("S2", False), ("S3", True)):
            p = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=True, asymmetry=asym)
            L0 = cr.deconvolve_starting_ladder(p, m0, block=block, iters=5)
            dz, dgv, _ = cr._refine_drift_smooth(p, L0)
            p = cr.replace(p, drift_z=dz, drift_g=dgv)
            profs, surges = [], []
            for sd in range(2):
                Xs, Rs, _ = cr.simulate_common_panel(p, L0, T_sim=T, seed=900 + sd, apply_absence=True)
                fls = np.sum(np.isfinite(Rs), axis=1).astype(float) + 1.0
                profs.append(emp_profile(Xs, Rs, fls, K, hs))
                surges.append(surge_on(Xs, Rs, fls, K))
            row[arm] = {h: np.mean([pp[h] for pp in profs], axis=0).tolist() for h in hs}
            row[arm + "_surge_on_sim"] = surges
            row[arm + "_fitted_surge"] = [p.surge_lam, p.surge_mu, p.surge_rho]
        res[pn] = row
        print(f"\n===== {pn}")
        print(f"  surge estimator on DATA: lam={row['data_surge'][0]:.4f} mu={row['data_surge'][1]:.2f} rho={row['data_surge'][2]:.4f}")
        for arm in ("S2", "S3"):
            print(f"  surge estimator on {arm}-simulated panels: " + "; ".join(f"lam={s[0]:.4f} mu={s[1]:.2f} rho={s[2]:.4f}" for s in row[arm + '_surge_on_sim']))
        print("   h   source        A(1)    A(2)    A(3)    A(4)    A(6)")
        for h in hs:
            for nm in ("data", "S2", "S3"):
                print(f"  {h:2d}  {nm + ('-sim panel' if nm != 'data' else ''):12s} " + " ".join(f"{a:+.4f}" for a in row[nm][h]))
        (HERE / "diag_null_skew.json").write_text(json.dumps(res, indent=1, default=float))


if __name__ == "__main__":
    main()
