#!/usr/bin/env python3
"""Round 9 Pre-Check (pure estimation & planted-recovery diagnostics; zero gate scoring):
  1. Planted recovery of (phi, sigma_L0, V_xi, sigma_obs) under lag weightings:
     uniform vs inv_sqrt_h vs rel (1/D) vs rel_sqrt_h (1/(D*sqrt(h))).
  2. Effect of lag weighting on (phi, sL2_var, sL2_rob, gap_share) across all 4 DEVELOPMENT panels.
  3. Bandwise k3(h) surge slope b_surge + Opus 3-tier SE-gated drift b_d -> check D11 zero-band counts
     across all 4 full panels and all 20 gate training windows!
"""
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
import rankdiff_kalman as rk
import round3_gold_benchmark as rgb


def fit_profiles_weighted(D, hs, nb, spec_b_pinned, so_band_fixed, D_subtract=None, phi_grid=None, w_mode="uniform"):
    best = None
    for phi in (cr.PHI_GRID if phi_grid is None else np.asarray(phi_grid, dtype=float)):
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
            h_ok = hs[ok]
            if w_mode == "uniform":
                w = np.ones_like(y_ok)
            elif w_mode == "inv_sqrt_h":
                w = 1.0 / np.sqrt(h_ok)
            elif w_mode == "rel":
                w = 1.0 / np.maximum(y_ok, 1e-4)
            elif w_mode == "rel_sqrt_h":
                w = 1.0 / (np.maximum(y_ok, 1e-4) * np.sqrt(h_ok))
            else:
                raise ValueError(w_mode)
            if D_subtract is not None:
                sub_b = D_subtract[b, ok] if D_subtract.ndim == 2 else D_subtract[ok]
                sub_ok = np.minimum(sub_b, 0.85 * y_ok)
                y_net = np.maximum(y_ok - sub_ok, 0.0)
            else:
                y_net = y_ok
            fl = (so_band_fixed[b] ** 2) if spec_b_pinned else 0.0
            y_adj = np.maximum(y_net - 2.0 * fl, 0.0)
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


def check1_planted_recovery():
    print("=" * 84)
    print("CHECK 1: PLANTED RECOVERY OF (phi, sigma_L0, V_xi, sigma_obs) BY LAG WEIGHTING")
    print("=" * 84)
    # Generate 4 realistic planted synthetic panels (2-component + obs noise, T=160, N=4000, K=1000)
    plants = [
        ("fast_phi65", dict(phi=0.65, sL=0.08, vxi=0.14, so=0.18, s_vol=0.70, jump_lam=0.0, jump_c=0.0)),
        ("med_phi78",  dict(phi=0.78, sL=0.09, vxi=0.10, so=0.15, s_vol=0.70, jump_lam=0.0, jump_c=0.0)),
        ("slow_phi89", dict(phi=0.89, sL=0.07, vxi=0.15, so=0.18, s_vol=0.70, jump_lam=0.0, jump_c=0.0)),
        ("jump_phi78", dict(phi=0.78, sL=0.06, vxi=0.06, so=0.14, s_vol=0.70, jump_lam=0.02, jump_c=0.18)),
        ("short_T52",  dict(phi=0.78, sL=0.10, vxi=0.20, so=0.55, s_vol=0.75, jump_lam=0.0, jump_c=0.0, T=52)),
    ]
    for name, cfg in plants:
        T = cfg.get("T", 160)
        N, K = 4000, 1000
        rng = np.random.default_rng(12345)
        L0 = 10.0 - 0.8 * np.log(np.arange(1, N + 1))
        # Use exact simulator-like synthetic panel (homogeneous rank for clean oracle check)
        z_v = np.clip(rng.standard_normal(N), -2.5, 2.5)
        v = np.exp(cfg["s_vol"] * z_v); v /= np.mean(v)
        sqv = np.sqrt(v)
        L = L0.copy()
        xi = math.sqrt(cfg["vxi"]) * sqv * rng.standard_normal(N)
        c_xi = math.sqrt(1.0 - cfg["phi"] ** 2)
        X = np.empty((T, N))
        R = np.empty((T, N))
        for t in range(T):
            L = L + cfg["sL"] * rng.standard_normal(N)
            if cfg["jump_lam"] > 0:
                hit = rng.random(N) < cfg["jump_lam"]
                L[hit] += math.sqrt(cfg["jump_c"]) * rng.standard_normal(int(hit.sum()))
            xi = cfg["phi"] * xi + math.sqrt(cfg["vxi"]) * c_xi * sqv * rng.standard_normal(N)
            X[t] = L + xi + cfg["so"] * sqv * rng.standard_normal(N)
            ord_t = np.argsort(-X[t])
            R[t, ord_t] = np.arange(1, N + 1)
        sL2_true = cfg["sL"] ** 2 + cfg["jump_lam"] * cfg["jump_c"]
        # Build D exactly as estimate_common does
        floors = np.full(T, N + 1.0)
        rperm = np.mean(R, axis=0)
        pr_ord = np.empty(N); pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
        edges = cr._band_edges(K, N); nb = len(edges) - 1
        band_of = np.clip(np.searchsorted(edges, pr_ord, side="right") - 1, 0, nb - 1)
        Xc = X - cr._top_k_center(X, R, K)
        H = min(52, max(6, T // 2))
        D = cr._structure_function(Xc, band_of, nb, H)
        hs = np.arange(1, H + 1, dtype=float)
        print(f"\n  Plant [{name}] TRUE: phi={cfg['phi']:.2f}  sL2_tot={sL2_true*1e3:5.2f}e-3  vxi={cfg['vxi']:.3f}  so2={cfg['so']**2:.3f}")
        for wm in ("uniform", "inv_sqrt_h", "rel", "rel_sqrt_h"):
            _, phi, so2, vxi, sL2 = fit_profiles_weighted(D, hs, nb, False, None, w_mode=wm)
            # Weighted average across in-K bands
            sL2_m = float(np.nanmedian(sL2[:7]))
            vxi_m = float(np.nanmedian(vxi[:7]))
            so2_m = float(np.nanmedian(so2[:7]))
            err_tot = abs(phi - cfg["phi"]) + abs(sL2_m - sL2_true) / sL2_true + abs(vxi_m - cfg["vxi"]) / cfg["vxi"] + abs(so2_m - cfg["so"]**2) / (cfg["so"]**2)
            print(f"    {wm:11s}: phi={phi:.2f}  sL2={sL2_m*1e3:5.2f}e-3 ({(sL2_m/sL2_true-1):+5.1%})  "
                  f"vxi={vxi_m:.3f} ({(vxi_m/cfg['vxi']-1):+5.1%})  so2={so2_m:.3f} ({(so2_m/cfg['so']**2-1):+5.1%})  sum_rel_err={err_tot:.3f}")


def estimate_b_surge(Xc, band_of, band_z, z_K, band_n, H):
    """Estimate rank slope b_surge of surge amplitude from bandwise kappa_3(Delta_h X) across h in (8, 13, 26)."""
    hs_k = [h for h in (8, 13, 20, 26) if h <= H]
    if not hs_k:
        hs_k = [min(4, H)]
    k3_band = np.full(7, np.nan)
    for b in range(7):
        idx = np.flatnonzero(band_of == b)
        if idx.size < 20:
            continue
        vals = []
        for h in hs_k:
            dx = Xc[h:, idx] - Xc[:-h, idx]
            dx = dx - np.nanmean(dx, axis=1, keepdims=True)
            v = dx[np.isfinite(dx)]
            if v.size < 100:
                continue
            sd = float(np.std(v))
            if sd < 1e-6:
                continue
            vw = np.clip(v, -7.0 * sd, 7.0 * sd)
            vc = vw - float(np.mean(vw))
            vals.append(float(np.mean(vc ** 3)))
        if vals:
            k3_band[b] = float(np.mean(vals))
    ok = np.isfinite(k3_band) & (k3_band > 1e-4)
    if ok.sum() < 3:
        return 0.0, k3_band
    dz = band_z[:7] - z_K
    w = np.minimum(band_n[:7], 100.0)
    _, slope_k3 = cr._wls_line(dz[ok], np.log(k3_band[ok]), w[ok])
    b_surge = float(np.clip(slope_k3 / 3.0, 0.0, 0.50))
    return b_surge, k3_band


def check2_real_panels_and_d11():
    print("\n" + "=" * 84)
    print("CHECK 2: b_surge & D11 ZERO-BAND AUDIT UNDER rel_sqrt_h + RANK-SCALED SURGES & DRIFT")
    print("=" * 84)
    # Import Opus R8's clean 3-tier SE-gated drift estimator to test joint effect!
    sys.path.insert(0, str(BASE / "opus_r8"))
    import common_rankdiff_opus_r8 as cr8

    for plat in ("reddit_comments_ext", "reddit_submissions_long"):
        data, sb = rgb.load_cached(plat)
        X, R, K = data["X"].astype(np.float64), data["R"].astype(np.float64), int(data["K"])
        T, N = X.shape
        origins, _ = rk._gate_windows(T, 5)
        print(f"\n=== {plat} (T={T}, K={K}) ===")
        for label, T_end in [("FULL", T)] + [(f"T0={int(o)}", int(o)) for o in origins]:
            Xt, Rt = X[:T_end], R[:T_end]
            Tt = Xt.shape[0]
            floors = np.sum(np.isfinite(Rt), axis=1).astype(float) + 1.0
            pen = np.where(np.isfinite(Rt), Rt, floors[:, None])
            rperm = np.mean(pen, axis=0)
            pres = np.isfinite(Rt).mean(axis=0)
            pr_ord = np.empty(N, dtype=float)
            pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
            edges = cr._band_edges(K, N); nb = len(edges) - 1
            band_all = np.clip(np.searchsorted(edges, pr_ord, side="right") - 1, 0, nb - 1)
            band_of = np.where(pres >= 0.5, band_all, -1)
            band_z = np.log(np.clip((np.sqrt(np.maximum(edges[:-1], 1.0) * edges[1:]) - 0.5) / N, cr.Z_CLIP, 1.0))
            band_n = np.array([float(np.sum(band_of == b)) for b in range(nb)])
            z_K = math.log(max((K - 0.5) / N, cr.Z_CLIP))
            Xc = Xt - cr._top_k_center(Xt, Rt, K)
            H = int(min(52, max(6, Tt // 2)))
            D = cr._structure_function(Xc, band_of, nb, H)
            hs = np.arange(1, H + 1, dtype=float)
            so_fl = None
            if sb is not None:
                z_sb, so_sb = sb
                ok_sb = np.isfinite(z_sb) & np.isfinite(so_sb) & (so_sb > 1e-5)
                af, bf = cr._wls_line(z_sb[ok_sb] - z_K, np.log(so_sb[ok_sb]), np.ones(ok_sb.sum()))
                so_fl = np.exp(np.clip(af + bf * (band_z - z_K), -6.0, 3.0))

            # Compare uniform S2 vs rel_sqrt_h S2
            _, phi_u, _, vxi_u, sL2_u = fit_profiles_weighted(D, hs, nb, sb is not None, so_fl, w_mode="uniform")
            _, phi_w, _, vxi_w, sL2_w = fit_profiles_weighted(D, hs, nb, sb is not None, so_fl, w_mode="rel_sqrt_h")

            # Estimate surge & rank-scaled b_surge
            with np.errstate(all="ignore"):
                D_med = np.nanmedian(D[:7], axis=0)
            s_lam, s_mu, s_rho, _ = cr._estimate_stage3_surge(Xc, pr_ord, pres, K, D_med)
            Vs = cr._surge_variance(s_lam, s_mu, s_rho)
            b_srg, _ = estimate_b_surge(Xc, band_of, band_z, z_K, band_n, H)

            # Estimate Opus R8 rank-scaled SE-gated drift
            in_k = np.arange(nb) < nb - 1
            dz_b = band_z - z_K
            w_oct = np.minimum(band_n, 100.0)
            ok_xi = in_k & np.isfinite(vxi_w) & (vxi_w > 1e-7)
            a_xi, b_xi = cr._wls_line(dz_b[ok_xi], 0.5 * np.log(vxi_w[ok_xi]), w_oct[ok_xi])
            sel_k = (pr_ord <= K) & (pres >= 0.5)
            z_sel = np.log(np.clip((pr_ord[sel_k] - 0.5) / N, cr.Z_CLIP, 1.0))
            Vxi_mean = float(np.mean(np.exp(2.0 * np.clip(a_xi + b_xi * (z_sel - z_K), -6.0, 3.0))))
            vxi_fn = lambda z_arr, _a=a_xi, _b=b_xi, _zk=z_K: np.exp(2.0 * np.clip(_a + _b * (z_arr - _zk), -6.0, 3.0))
            ssc2_fn = lambda z_arr, _bs=b_srg, _zk=z_K: cr._surge_scale_of(_bs, z_arr, _zk) ** 2
            d_var, d_rho, b_d, dr_diag = cr8._estimate_stage3_drift(
                Xc, pr_ord, pres, K, phi_w, Vxi_mean, Vs, s_rho,
                vxi_fn=vxi_fn, ssc2_fn=ssc2_fn, z_K=z_K, rank_law=True,
            )

            # Build rank-scaled D_sub with b_srg and b_d
            D_sub = np.zeros((nb, H))
            if Vs > 0 and s_rho > 0:
                sc_b = cr._surge_scale_of(b_srg, band_z, z_K)
                D_sub += (sc_b[:, None] ** 2) * (2.0 * Vs * (1.0 - s_rho ** hs))[None, :]
            if d_var > 0 and d_rho > 0:
                d_sc = np.exp(b_d * np.clip(band_z - z_K, -8.0, 0.0))
                D_sub += (d_var * (d_sc[:, None] ** 2)) * cr._f_vel(hs, d_rho)[None, :]

            _, phi_s3_new, _, _, sL2_s3_new = fit_profiles_weighted(D, hs, nb, sb is not None, so_fl, D_subtract=D_sub, w_mode="rel_sqrt_h")
            # Compare to R6 S3 zero count
            p3_r6 = cr.estimate_common(Xt, Rt, K, spec_b=sb, asymmetry=True, scale_moment="meta", refine_drift=False)
            n0_r6 = int(np.sum(np.array(p3_r6.diag["sL2_band"][:7]) <= 1e-7))
            n0_new = int(np.sum(sL2_s3_new[:7] <= 1e-7))

            # Fit beta on new sL2_s3_new
            band_level = np.array([float(np.nanmean(Xt[:, band_of == b])) if np.any(band_of == b) else np.nan for b in range(nb)])
            L_K = p3_r6.L_K
            ok_L = in_k & np.isfinite(sL2_s3_new) & (sL2_s3_new > 1e-7) & np.isfinite(band_level)
            if ok_L.sum() >= 3:
                c0, c1 = cr._wls_line(band_level[ok_L] - L_K, np.log(sL2_s3_new[ok_L]), w_oct[ok_L])
                sL0_new, beta_new = math.exp(0.5 * c0), float(np.clip(-0.5 * c1, -0.25, 0.50))
            else:
                sL0_new, beta_new = math.sqrt(float(np.nanmedian(sL2_s3_new[:7][sL2_s3_new[:7] > 0]))) if np.any(sL2_s3_new[:7] > 0) else 0.01, 0.0

            print(f"  {label:6s} | phi: u={phi_u:.2f} w={phi_w:.2f} s3w={phi_s3_new:.2f} | "
                  f"b_srg={b_srg:+.2f} b_d={b_d:+.2f} sd={math.sqrt(d_var):.4f} | "
                  f"R6_S3: 0s={n0_r6}/7 beta={p3_r6.beta:+.3f} sL0={p3_r6.sigma_L0:.4f} | "
                  f"NEW_S3: 0s={n0_new}/7 beta={beta_new:+.3f} sL0={sL0_new:.4f}")


if __name__ == "__main__":
    check2_real_panels_and_d11()
