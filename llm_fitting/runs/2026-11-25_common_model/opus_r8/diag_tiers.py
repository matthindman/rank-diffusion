#!/usr/bin/env python3
"""D7 (Round 8): rank allocation of the S3 components + a band-demeaned surge estimator.

(a) Tiered persistent-drift moment: the S3 gapped window-change covariance Cov(x, y) computed
    separately inside the permanent-rank tiers (K/16, K/4, K), each net of the passing AR(1) and
    surge covariances evaluated at that tier, divided by G_d -> sigma_d^2 per tier. R6 S3 pools the
    tiers and applies one sigma_d to every rank.
(b) The R6 surge cumulant estimator with h-increments demeaned WITHIN permanent-rank bands (the
    variogram's convention) instead of across all selected entities, on the data and on panels
    simulated from the fitted symmetric S2 model (the D6 null). Rank-varying drift mixed with
    rank-varying variance gives a pooled marginal skew with no per-entity skew at all.
Reads only the DEVELOPMENT caches; writes only into opus_r8/.
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
from diag_asym_model import CACHE, PANELS  # noqa: E402


def prep(X, R, fl, K):
    T, N = X.shape
    pen = np.where(np.isfinite(R), R, fl[:, None])
    pr = np.empty(N)
    pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
    pres = np.isfinite(R).mean(axis=0)
    Xc = X - cr._top_k_center(X, R, K)
    return Xc, pr, pres


def band_demeaned_k35(Xc, pr, pres, K):
    """kappa_3, kappa_5 per h as in _estimate_stage3_surge, but demeaning within rank bands."""
    N = Xc.shape[1]
    sel = (pr <= K) & (pres >= 0.5)
    edges = cr._band_edges(K, N)
    band = np.clip(np.searchsorted(edges, pr, side="right") - 1, 0, len(edges) - 2)[sel]
    T = Xc.shape[0]
    hs = np.array([h for h in (4, 8, 13, 20, 26, 39, 52) if h <= min(52, T // 2)], float)
    k3s, k5s = np.zeros(hs.size), np.zeros(hs.size)
    Xs = Xc[:, sel]
    for i, h in enumerate(hs.astype(int)):
        dx = Xs[h:] - Xs[:-h]
        for b in np.unique(band):
            m = band == b
            dx[:, m] = dx[:, m] - np.nanmean(dx[:, m], axis=1, keepdims=True)
        v = dx[np.isfinite(dx)]
        sd = float(np.std(v))
        vw = np.clip(v, -7 * sd, 7 * sd)
        vc = vw - vw.mean()
        v2, v3 = float(np.mean(vc ** 2)), float(np.mean(vc ** 3))
        k3s[i], k5s[i] = v3, float(np.mean(vc ** 5)) - 10 * v2 * v3
    return hs, k3s, k5s


def surge_from_k(hs, k3s, k5s):
    """The R6 closed-form surge solution applied to given cumulant profiles (no budget guard)."""
    best = None
    for hl in cr.SURGE_HL_GRID:
        rg = 0.5 ** (1 / hl)
        f3 = 3 * rg ** hs * (1 - rg ** hs)
        C3 = max(float(f3 @ k3s / (f3 @ f3)), 0.0)
        err = float(np.sum((k3s - C3 * f3) ** 2))
        if best is None or err < best[0]:
            best = (err, hl, rg, C3)
    _, hl, rho, C3 = best
    f5 = 5 * rho ** hs * (1 - rho ** hs) * (1 - 2 * rho ** hs + 2 * rho ** (2 * hs))
    C5 = float(f5 @ k5s / (f5 @ f5))
    if C3 < 0.025 or C5 <= 0.01 or float(np.sum(k3s)) <= 0.02:
        return dict(active=False, C3=C3, C5=C5, sum_k3=float(np.sum(k3s)))
    ratio = C5 * (1 - rho ** 5) / max(C3 * (1 - rho ** 3), 1e-12)
    mu = float(np.clip(math.sqrt(max(ratio * cr.CAP_M3 / cr.CAP_M5, 0)), 0.15, 2.5))
    lam = float(np.clip(C3 * (1 - rho ** 3) / mu ** 3 / cr.CAP_M3, 0, 0.15))
    return dict(active=True, hl=float(hl), mu=mu, lam=lam, C3=C3, C5=C5, sum_k3=float(np.sum(k3s)))


def tiered_drift(Xc, pr, pres, K, p):
    T = Xc.shape[0]
    if T < 160:
        return None
    W, H, g = 13, 52, 13
    rho_d = 0.5 ** (1 / cr.DRIFT_HL_FIXED)
    sel = (pr <= K) & (pres >= 0.5)
    lab = np.where(pr <= K / 16, 0, np.where(pr <= K / 4, 1, 2))
    N = Xc.shape[1]
    out = {}
    for j, nm in enumerate(("head_K16", "mid_K4", "outer_K")):
        m = sel & (lab == j)
        Xs = Xc[:, m]
        covs = []
        for o in range(0, T - (2 * H + g) + 1, W):
            with np.errstate(all="ignore"):
                a0 = np.nanmean(Xs[o:o + W], axis=0); a1 = np.nanmean(Xs[o + H - W:o + H], axis=0)
                b0 = np.nanmean(Xs[o + H + g:o + H + g + W], axis=0); b1 = np.nanmean(Xs[o + 2 * H + g - W:o + 2 * H + g], axis=0)
            ok = np.isfinite(a0) & np.isfinite(a1) & np.isfinite(b0) & np.isfinite(b1)
            if ok.sum() < 20:
                continue
            x, y = (a1 - a0)[ok], (b1 - b0)[ok]
            x, y = x - x.mean(), y - y.mean()
            sx, sy = x.std(), y.std()
            covs.append(float(np.mean(np.clip(x, -6 * sx, 6 * sx) * np.clip(y, -6 * sy, 6 * sy))))
        z = np.log(np.clip((pr[m] - 0.5) / N, cr.Z_CLIP, 1))
        vxi = float(np.mean(cr.v_xi_of(p, z)))
        c_xi = vxi * cr._G_ar1(p.phi, W, H, g)
        Vs = cr._surge_variance(p.surge_lam, p.surge_mu, p.surge_rho)
        ssc2 = float(np.mean(cr._surge_scale_of(p.b_xi, z, p.z_K) ** 2))
        c_s = Vs * ssc2 * cr._G_ar1(p.surge_rho, W, H, g) if Vs > 0 else 0.0
        Gd = cr._G_drift(rho_d, W, H, g)
        cov = float(np.mean(covs))
        sd2 = max((cov - c_xi - c_s) / Gd, 0.0)
        lev = float(np.nanmean(np.nanmean(Xc[:, m], axis=0)))
        out[nm] = dict(n=int(m.sum()), cov_emp=cov, c_xi=c_xi, c_s=c_s, sigma_d=math.sqrt(sd2),
                       sigma_L_at_tier=float(np.mean(cr.sigma_L_of(p, np.nanmean(Xc[:, m], axis=0) + p.L_K - np.nanmedian(Xc[:, m] * 0)))))
    return out


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
        Xc, pr, pres = prep(X, R, fl, K)
        p3 = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=True)
        row = {"S3_full_fit": dict(lam=p3.surge_lam, mu=p3.surge_mu, rho=p3.surge_rho, sigma_d=math.sqrt(p3.decline_var))}
        row["tiered_drift"] = tiered_drift(Xc, pr, pres, K, p3)
        hs, k3, k5 = band_demeaned_k35(Xc, pr, pres, K)
        row["band_demeaned_surge_data"] = surge_from_k(hs, k3, k5)
        # symmetric null: one S2-simulated panel (same construction as D6)
        p2 = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=True, asymmetry=False)
        block = {"facebook_a": 26, "instagram_hm": 13}.get(pn, 52)
        m_all = np.nanmean(X, axis=0)
        with np.errstate(all="ignore"):
            m0 = np.nanmean(X[:block], axis=0)
        m0 = np.where(np.isfinite(m0), m0, np.where(np.isfinite(m_all), m_all, float(np.nanmin(m_all))))
        L0 = cr.deconvolve_starting_ladder(p2, m0, block=block, iters=5)
        dz, dgv, _ = cr._refine_drift_smooth(p2, L0)
        p2 = cr.replace(p2, drift_z=dz, drift_g=dgv)
        nulls = []
        for sd in range(2):
            Xs, Rs, _ = cr.simulate_common_panel(p2, L0, T_sim=T, seed=900 + sd, apply_absence=True)
            fls = np.sum(np.isfinite(Rs), axis=1).astype(float) + 1.0
            a, b, c = prep(Xs, Rs, fls, K)
            nulls.append(surge_from_k(*band_demeaned_k35(a, b, c, K)))
        row["band_demeaned_surge_S2null"] = nulls
        res[pn] = row
        print(f"\n===== {pn}")
        print("  R6 S3 full fit:", {k: round(v, 4) for k, v in row["S3_full_fit"].items()})
        print("  band-demeaned surge on DATA:", {k: (round(v, 4) if isinstance(v, float) else v) for k, v in row["band_demeaned_surge_data"].items()})
        for nn in nulls:
            print("  band-demeaned surge on S2 null:", {k: (round(v, 4) if isinstance(v, float) else v) for k, v in nn.items()})
        if row["tiered_drift"]:
            for k, v in row["tiered_drift"].items():
                print(f"  drift tier {k:9s} n={v['n']:5d} cov={v['cov_emp']:+.4f} c_xi={v['c_xi']:+.4f} c_s={v['c_s']:+.4f} -> sigma_d={v['sigma_d']:.4f}")
        (HERE / "diag_tiers.json").write_text(json.dumps(res, indent=1, default=float))


if __name__ == "__main__":
    main()
