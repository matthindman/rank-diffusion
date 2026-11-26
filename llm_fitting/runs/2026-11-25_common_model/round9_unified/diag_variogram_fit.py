#!/usr/bin/env python3
"""Inspect variogram fit across lags h=1..52 under uniform vs relative lag weighting,
and check rank-scaled surge (b_surge from k3) + rank-scaled drift (b_d) variance budgets."""
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


def fit_weighted_phi(D, hs, nb, spec_b_pinned, so_band_fixed, weight_mode="uniform"):
    best = None
    for phi in cr.PHI_GRID:
        col_xi = 2.0 * (1.0 - phi ** hs)
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
            if weight_mode == "uniform":
                w = np.ones_like(y_ok)
            elif weight_mode == "rel":
                w = 1.0 / np.maximum(y_ok, 1e-4)
            elif weight_mode == "inv_sqrt_h":
                w = 1.0 / np.sqrt(hs[ok])
            elif weight_mode == "rel_sqrt_h":
                w = 1.0 / (np.maximum(y_ok, 1e-4) * np.sqrt(hs[ok]))
            fl = (so_band_fixed[b] ** 2) if spec_b_pinned else 0.0
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
            best = (sse, float(phi), so2_b, vxi_b, sL2_b)
    return best


def main():
    for plat in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
        data, sb = rgb.load_cached(plat)
        X, R, K = data["X"].astype(np.float64), data["R"].astype(np.float64), int(data["K"])
        p2v = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=False, scale_moment="variance", refine_drift=False)
        D = np.array(p2v.diag["D"], float)
        H = p2v.diag["H"]
        hs = np.arange(1, H + 1, dtype=float)
        nb = D.shape[0]
        band_z = np.array(p2v.diag["band_z"], float)
        z_K = p2v.z_K
        so_fl = None
        if p2v.spec_b_pinned:
            so_fl = np.exp(np.clip(p2v.a_obs_floor + p2v.b_obs_floor * (band_z - z_K), -6.0, 3.0))

        print(f"\n==================== {plat} (H={H}) ====================")
        check_h = [h for h in (1, 2, 4, 8, 13, 26, 39, 52) if h <= H]
        for mode in ("uniform", "inv_sqrt_h", "rel", "rel_sqrt_h"):
            sse, phi, so2, vxi, sL2 = fit_weighted_phi(D, hs, nb, p2v.spec_b_pinned, so_fl, mode)
            # compute median relative error across inside-K bands at each check_h
            rel_at_h = []
            for h in check_h:
                m_h = 2.0 * so2[:7] + 2.0 * vxi[:7] * (1.0 - phi ** h) + sL2[:7] * h
                e_h = (m_h - D[:7, h - 1]) / np.maximum(D[:7, h - 1], 1e-6)
                rel_at_h.append(f"h{h:2d}:{np.nanmedian(e_h):+.1%}")
            n0 = int(np.sum(sL2[:7] <= 1e-7))
            print(f"  {mode:11s}: phi={phi:.2f} sL2_med={np.nanmedian(sL2[:7])*1e3:5.2f}e-3 (0s={n0}) "
                  f"vxi_med={np.nanmedian(vxi[:7]):.3f} so2_med={np.nanmedian(so2[:7]):.3f} | " + " ".join(rel_at_h))

        # Check band-by-band short-lag fit at h=1,2,4 for uniform vs rel
        _, phi_u, so2_u, vxi_u, sL2_u = fit_weighted_phi(D, hs, nb, p2v.spec_b_pinned, so_fl, "uniform")
        _, phi_r, so2_r, vxi_r, sL2_r = fit_weighted_phi(D, hs, nb, p2v.spec_b_pinned, so_fl, "rel")
        # Also check empirical lag-1 / lag-4 / lag-13 / lag-H ratios across bands
        print(f"  Empirical D_med(h) at {check_h}: " +
              " ".join(f"h{h}={np.nanmedian(D[:7, h-1]):.3f}" for h in check_h))


if __name__ == "__main__":
    main()
