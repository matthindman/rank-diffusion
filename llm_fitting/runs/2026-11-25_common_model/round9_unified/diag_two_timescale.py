#!/usr/bin/env python3
"""5-second diagnostic: Does a 2-timescale passing process (phi_fast + phi_slow, +1 or +2 scalars)
simultaneously eliminate lag-1 overprediction (+6..+13%) AND preserve lag-52 accuracy (rH = 1.00)
on all 4 DEVELOPMENT panels, while zeroing out cleanly on single-AR(1) planted worlds?"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
from scipy.optimize import nnls

ROOT = Path(__file__).resolve().parents[4]
BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(BASE))

import common_rankdiff as cr
import round3_gold_benchmark as rgb


def fit_two_mode(D, hs, nb, spec_b_pinned, so_fl, phi_f_grid=(0.35,), w_mode="rel"):
    """Fit D_b(h) = 2*so2_b + 2*V_xi_b * [wf*(1-phi_f^h) + (1-wf)*(1-phi_s^h)] + sL2_b * h
    with pooled (phi_s, phi_f, wf) -> +1 scalar (if phi_f fixed) or +2 scalars (if phi_f profiled)!"""
    wf_grid = np.linspace(0.0, 0.85, 18)
    best = None
    for phi_f in phi_f_grid:
        cf = 2.0 * (1.0 - phi_f ** hs)
        for phi_s in cr.PHI_GRID:
            if phi_s <= phi_f + 0.10:
                continue
            cs = 2.0 * (1.0 - phi_s ** hs)
            for wf in wf_grid:
                col_xi = wf * cf + (1.0 - wf) * cs
                col_L = hs
                sse = 0.0
                so2_b = np.full(nb, np.nan)
                vxi_b = np.full(nb, np.nan)
                sL2_b = np.full(nb, np.nan)
                for b in range(nb):
                    y = D[b]
                    ok = np.isfinite(y)
                    if ok.sum() < 4:
                        continue
                    y_ok = y[ok]
                    if w_mode == "rel":
                        w = 1.0 / np.maximum(y_ok, 1e-4)
                    elif w_mode == "rel_sqrt_h":
                        w = 1.0 / (np.maximum(y_ok, 1e-4) * np.sqrt(hs[ok]))
                    else:
                        w = np.ones_like(y_ok)
                    fl = (so_fl[b] ** 2) if spec_b_pinned else 0.0
                    y_adj = np.maximum(y_ok - 2.0 * fl, 0.0)
                    A = np.column_stack([2.0 * np.ones(ok.sum()), col_xi[ok], col_L[ok]]) * w[:, None]
                    coef, res = nnls(A, y_adj * w)
                    so2_b[b] = fl + coef[0]
                    vxi_b[b] = coef[1]
                    sL2_b[b] = coef[2]
                    denom = max(float(np.sum((y_ok * w) ** 2)), 1e-12)
                    if b < nb - 1:
                        sse += (res ** 2) / denom
                if best is None or sse < best[0]:
                    best = (sse, float(phi_s), float(phi_f), float(wf), so2_b, vxi_b, sL2_b)
    return best


def main():
    print("=== REAL DEVELOPMENT PANELS: 1-AR(1) vs 2-TIMESCALE PASSING (w_f, phi_f=0.35 or grid) ===")
    for plat in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
        data, sb = rgb.load_cached(plat)
        X, R, K = data["X"].astype(np.float64), data["R"].astype(np.float64), int(data["K"])
        p2v = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=False, scale_moment="variance", refine_drift=False)
        D = np.array(p2v.diag["D"], float)
        H = p2v.diag["H"]
        hs = np.arange(1, H + 1, dtype=float)
        nb = D.shape[0]
        band_z = np.array(p2v.diag["band_z"], float)
        so_fl = np.exp(np.clip(p2v.a_obs_floor + p2v.b_obs_floor * (band_z - p2v.z_K), -6.0, 3.0)) if p2v.spec_b_pinned else None
        check_h = [h for h in (1, 2, 4, 8, 13, 26, 39, 52) if h <= H]
        print(f"\n--- {plat} (H={H}) ---")
        # 1-AR(1) uniform baseline
        _, phi1, so1, vx1, sL1 = cr._fit_structure_profiles(D, hs, nb, p2v.spec_b_pinned, so_fl)
        errs1 = []
        for h in check_h:
            m = 2*so1[:7] + 2*vx1[:7]*(1 - phi1**h) + sL1[:7]*h
            errs1.append(f"h{h:2d}:{np.nanmedian((m - D[:7, h-1])/D[:7, h-1]):+.1%}")
        print(f"  1-AR1 (uni): phi={phi1:.2f}             sL2={np.nanmedian(sL1[:7])*1e3:5.2f}e-3 so2={np.nanmedian(so1[:7]):.3f} | " + " ".join(errs1))

        for lbl, pf_g, wm in [
            ("2-AR1 (+1,pf=.35,rel)", (0.35,), "rel"),
            ("2-AR1 (+1,pf=.35,rsh)", (0.35,), "rel_sqrt_h"),
            ("2-AR1 (+2,grid,rsh)  ", (0.20, 0.35, 0.50), "rel_sqrt_h"),
        ]:
            sse, ps, pf, wf, so2, vx2, sL2 = fit_two_mode(D, hs, nb, p2v.spec_b_pinned, so_fl, pf_g, wm)
            errs = []
            for h in check_h:
                m = 2*so2[:7] + 2*vx2[:7]*(wf*(1 - pf**h) + (1 - wf)*(1 - ps**h)) + sL2[:7]*h
                errs.append(f"h{h:2d}:{np.nanmedian((m - D[:7, h-1])/D[:7, h-1]):+.1%}")
            print(f"  {lbl}: ps={ps:.2f} pf={pf:.2f} wf={wf:.2f} sL2={np.nanmedian(sL2[:7])*1e3:5.2f}e-3 so2={np.nanmedian(so2[:7]):.3f} | " + " ".join(errs))

    print("\n=== NULL CHECK: SINGLE-AR(1) PLANTED PANEL (true wf = 0.0, phi = 0.78) ===")
    rng = np.random.default_rng(12345)
    T, N, K = 160, 4000, 1000
    L = 10.0 - 0.8 * np.log(np.arange(1, N + 1))
    xi = math.sqrt(0.10) * rng.standard_normal(N)
    X = np.empty((T, N)); R = np.empty((T, N))
    for t in range(T):
        L = L + 0.08 * rng.standard_normal(N)
        xi = 0.78 * xi + math.sqrt(0.10 * (1 - 0.78**2)) * rng.standard_normal(N)
        X[t] = L + xi + 0.15 * rng.standard_normal(N)
        o = np.argsort(-X[t]); R[t, o] = np.arange(1, N + 1)
    rperm = np.mean(R, axis=0)
    pr_ord = np.empty(N); pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
    edges = cr._band_edges(K, N); nb = len(edges) - 1
    band_of = np.clip(np.searchsorted(edges, pr_ord, side="right") - 1, 0, nb - 1)
    Xc = X - cr._top_k_center(X, R, K)
    D = cr._structure_function(Xc, band_of, nb, 52)
    hs = np.arange(1, 53, dtype=float)
    sse, ps, pf, wf, so2, vx2, sL2 = fit_two_mode(D, hs, nb, False, None, (0.35,), "rel_sqrt_h")
    print(f"  Null plant (true phi=0.78, wf=0.00): ps={ps:.2f} pf={pf:.2f} wf={wf:.2f} sL2={np.nanmedian(sL2[:7])*1e3:5.2f}e-3 (true 6.40) so2={np.nanmedian(so2[:7]):.3f} (true 0.023)")


if __name__ == "__main__":
    main()
