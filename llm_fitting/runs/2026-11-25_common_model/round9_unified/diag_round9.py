#!/usr/bin/env python3
"""Round 9 Read-Only Diagnostics on DEVELOPMENT scratch caches only.

Diagnoses:
  1. Exact Round 6 S2 & S3 per-origin Gate v1 errors and moment breakdown.
  2. D11: Bandwise NNLS zero-clamping vs. Joint Parametric Variogram Fit on all 4 panels + 20 gate windows.
  3. Surge cumulants (k3, k5, hl_s) & S3 boundary overshoot mechanism across all 4 panels.
  4. Rank-dependent lasting gap (var - IQRv) and jump activation profile across all 4 panels + 20 gate windows.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
from scipy.optimize import least_squares

ROOT = Path(__file__).resolve().parents[4]
BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(BASE))

import common_rankdiff as cr
import round3_frozen_gate as rfg
import round3_gold_benchmark as rgb


def load_panel(plat: str):
    data, sb = rgb.load_cached(plat)
    X = data["X"].astype(np.float64)
    R = data["R"].astype(np.float64)
    K = int(data["K"])
    return X, R, K, sb


def inspect_r6_gate():
    print("=" * 80)
    print("1. ROUND 6 PER-ORIGIN GATE v1 BREAKDOWN")
    print("=" * 80)
    g_s2 = json.loads((BASE / "round6_meta/final_bench/candidate_frozen_gate_results.json").read_text())
    g_s3 = json.loads((BASE / "astra_v3/candidate_frozen_gate_results.json").read_text())
    for plat in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
        print(f"\n--- {plat} ---")
        for lbl, d, k in [("R6_S2", g_s2, "s2"), ("R6_S3", g_s3, "s3")]:
            pd = d[plat]
            print(f"  {lbl}: mean={pd[k+'_rel']:.3f} ± {pd[k+'_sd']:.3f} | cov={pd[k+'_cov']*100:.0f}% | wins={pd[k+'_wins_base']}/5 | base={pd['base_rel']:.3f}")
            for sp in pd["splits"]:
                m_str = ""
                if f"{k}_moments" in sp:
                    ms = sp[f"{k}_moments"]
                    em = sp.get("emp_moments", {})
                    parts = [f"{mk}:{ms[mk]:.2f}/{em.get(mk, float('nan')):.2f}" for mk in ms]
                    m_str = " | " + ", ".join(parts[:6])
                print(f"    T0={sp['T0']:3d}: {k}={sp[k+'_rel']:.3f} (base={sp['base_rel']:.3f}, ci={sp['in_ci_'+k]}){m_str}")


def inspect_full_panel_fits():
    print("\n" + "=" * 80)
    print("2. FULL-PANEL DIAGNOSTICS: S2/S3 BANDS, SURGES, DRIFT, JUMPS, BOUNDARY")
    print("=" * 80)
    for plat, cfg in rfg.PANELS.items():
        X, R, K, sb = load_panel(plat)
        p2 = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=False, scale_moment="meta", refine_drift=False)
        p3 = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=True, scale_moment="meta", refine_drift=False)
        print(f"\n=== {plat} (T={X.shape[0]}, N={X.shape[1]}, K={K}) ===")
        sL_var = np.array(p2.diag["sL2_band_var"][:7])
        sL_s2 = np.array(p2.diag["sL2_band"][:7])
        sL_s3 = np.array(p3.diag["sL2_band"][:7])
        print(f"  S2: phi={p2.phi:.2f} sL0={p2.sigma_L0:.4f} beta={p2.beta:+.3f} a_xi={p2.a_xi:.3f} b_xi={p2.b_xi:+.3f} s_vol={p2.s_vol:.3f}")
        print(f"      jumps: active={p2.jump_lam>0} lam={p2.jump_lam:.3f} jump_c={p2.jump_c:.4f} gap_share={p2.diag['jumps']['gap_share']:.3f}")
        print(f"      sL2_var (x1e3): {np.round(sL_var*1e3, 3).tolist()}")
        print(f"      sL2_s2  (x1e3): {np.round(sL_s2*1e3, 3).tolist()}")
        print(f"  S3: phi={p3.phi:.2f} sL0={p3.sigma_L0:.4f} beta={p3.beta:+.3f} a_xi={p3.a_xi:.3f} b_xi={p3.b_xi:+.3f}")
        s3d = p3.diag.get("stage3", {})
        sg = s3d.get("surge", {})
        dr = s3d.get("drift", {})
        print(f"      surge: active={sg.get('active')} hl={sg.get('hl_s')} lam={p3.surge_lam:.4f} mu={p3.surge_mu:.3f} Vs={sg.get('V_s',0):.4f} budget_hit={sg.get('budget_hit')} mean={cr._surge_mean(p3.surge_lam, p3.surge_mu, p3.surge_rho):.3f}")
        if sg.get("k3s"):
            print(f"      hs={sg.get('hs')} k3s={np.round(sg['k3s'], 3).tolist()} k5s={np.round(sg.get('k5s', []), 2).tolist()}")
        print(f"      drift: active={dr.get('active')} sig_d={dr.get('sigma_d',0):.5f} net_cov={dr.get('net_cov',0):.5f}")
        print(f"      sL2_s3  (x1e3): {np.round(sL_s3*1e3, 3).tolist()}  (n_zero={int(np.sum(sL_s3 <= 1e-7))}/7)")


def test_joint_parametric_variogram():
    print("\n" + "=" * 80)
    print("3. JOINT PARAMETRIC VARIOGRAM FIT vs. BANDWISE NNLS (D11 AUDIT)")
    print("=" * 80)
    for plat, cfg in rfg.PANELS.items():
        X, R, K, sb = load_panel(plat)
        T, N = X.shape
        test_len = max(13, T // 4)
        origins = np.linspace(test_len, T - test_len, 5).round().astype(int)
        print(f"\n=== {plat} ===")
        for label, T_end in [("FULL", T)] + [(f"T0={int(o)}", int(o)) for o in origins]:
            Xt, Rt = X[:T_end], R[:T_end]
            p3 = cr.estimate_common(Xt, Rt, K, spec_b=sb, asymmetry=True, scale_moment="meta", refine_drift=False)
            p2v = cr.estimate_common(Xt, Rt, K, spec_b=sb, asymmetry=False, scale_moment="variance", refine_drift=False)
            D = np.array(p3.diag["D"], float)  # (nb, H)
            H = p3.diag["H"]
            hs = np.arange(1, H + 1, dtype=float)
            nb = D.shape[0]
            in_k = np.arange(nb) < nb - 1
            band_z = np.array(p3.diag["band_z"], float)
            band_level = np.array(p3.diag["band_level"], float)
            z_K = p3.z_K
            L_K = p3.L_K
            dz = band_z[:7] - z_K
            du = band_level[:7] - L_K

            # Compute Stage 3 subtraction exactly as R6 does
            Vs = cr._surge_variance(p3.surge_lam, p3.surge_mu, p3.surge_rho)
            D_sub = np.zeros((7, H))
            if Vs > 0 and p3.surge_rho > 0:
                s_sc = cr._surge_scale_of(p3.b_xi, band_z[:7], z_K)
                D_sub += (s_sc[:, None] ** 2) * (2.0 * Vs * (1.0 - p3.surge_rho ** hs))[None, :]
            if p3.decline_var > 0 and p3.decline_rho > 0:
                D_sub += (p3.decline_var * cr._f_vel(hs, p3.decline_rho))[None, :]

            # Spec-B floor
            fl2 = np.zeros(7)
            if p3.spec_b_pinned:
                fl2 = np.exp(2.0 * np.clip(p3.a_obs_floor + p3.b_obs_floor * dz, -8.0, 3.0))

            # Fit joint 6-parameter smooth variogram (sL0, beta, a_xi, b_xi, a_obs, b_obs) at fixed phi=p3.phi
            # normalizing each band by its RMS D_b so all 7 inside-K bands carry equal relative weight
            Y = D[:7]
            ok = np.isfinite(Y)
            b_rms = np.array([math.sqrt(max(float(np.nanmean(Y[b] ** 2)), 1e-12)) for b in range(7)])

            def resid(theta):
                log_sL0, beta, a_xi, b_xi, a_obs, b_obs = theta
                sL2 = np.exp(2.0 * (log_sL0 - beta * du))
                vxi = np.exp(2.0 * (a_xi + b_xi * dz))
                so2 = fl2 + np.exp(2.0 * (a_obs + b_obs * dz))
                M = (2.0 * so2[:, None]
                     + 2.0 * vxi[:, None] * (1.0 - p3.phi ** hs)[None, :]
                     + sL2[:, None] * hs[None, :]
                     + D_sub)
                r = (M - Y) / b_rms[:, None]
                return r[ok]

            x0 = np.array([
                math.log(max(p2v.sigma_L0, 1e-3)), np.clip(p2v.beta, -0.2, 0.45),
                p2v.a_xi, p2v.b_xi, p2v.a_obs, p2v.b_obs,
            ])
            lb = np.array([-8.0, -0.25, -6.0, -1.5, -8.0, -1.5])
            ub = np.array([1.0, 0.50, 2.0, 1.5, 2.0, 1.5])
            res = least_squares(resid, np.clip(x0, lb + 1e-4, ub - 1e-4), bounds=(lb, ub))
            ls0, b_j, axi_j, bxi_j, aob_j, bob_j = res.x
            sL0_j = math.exp(ls0)

            sL2_s3 = np.array(p3.diag["sL2_band"][:7])
            n_zero = int(np.sum(sL2_s3 <= 1e-7))
            print(f"  {label:6s} | S2var: sL0={p2v.sigma_L0:.4f} beta={p2v.beta:+.3f} | "
                  f"R6_S3: sL0={p3.sigma_L0:.4f} beta={p3.beta:+.3f} (0s={n_zero}/7) | "
                  f"JOINT_S3: sL0={sL0_j:.4f} beta={b_j:+.3f} (cost={res.cost:.4f})")


def inspect_s3_boundary_mechanism():
    print("\n" + "=" * 80)
    print("4. WHY DOES S3 OVERSHOOT BOUNDARY RATIO (1.13-1.17)?")
    print("=" * 80)
    for plat in ("facebook_a", "reddit_comments_ext"):
        X, R, K, sb = load_panel(plat)
        p2 = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=False, scale_moment="meta", refine_drift=True)
        p3 = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=True, scale_moment="meta", refine_drift=True)
        L0_2 = cr.deconvolve_starting_ladder(p2, p2.ladder_obs, block=min(26, max(4, X.shape[0]//3)))
        L0_3 = cr.deconvolve_starting_ladder(p3, p3.ladder_obs, block=min(26, max(4, X.shape[0]//3)))
        # Simulate 3 seeds and track rank-K median X and rank-K latent L across time
        for lbl, p, L0 in [("S2", p2, L0_2), ("S3", p3, L0_3)]:
            rng = np.random.default_rng(7000)
            st = cr.CommonState(p, L0, rng)
            T = X.shape[0]
            l_top1, l_K, x_top1, x_K, s_K, x_b0, x_b1 = [], [], [], [], [], [], []
            for t in range(T):
                x = st.step(apply_absence=True)
                ls = np.sort(st.L)[::-1]
                xf = np.sort(x[np.isfinite(x)])[::-1]
                l_top1.append(ls[0] - ls[K - 1])
                l_K.append(ls[K - 1])
                x_top1.append(xf[0] - xf[min(K, xf.size) - 1])
                x_K.append(xf[min(K, xf.size) - 1])
                if st.surge is not None:
                    s_K.append(float(np.mean(st.surge)))
            b = min(26, T // 3)
            # Check X(K) - X(K/4) or X(K) drift over time!
            print(f"  {plat} {lbl}: L_K start={np.mean(l_K[:b]):.3f} end={np.mean(l_K[-b:]):.3f} (d={np.mean(l_K[-b:])-np.mean(l_K[:b]):+.3f}) | "
                  f"X_K start={np.mean(x_K[:b]):.3f} end={np.mean(x_K[-b:]):.3f} (d={np.mean(x_K[-b:])-np.mean(x_K[:b]):+.3f}) | "
                  f"L_span(1-K) start={np.mean(l_top1[:b]):.3f} end={np.mean(l_top1[-b:]):.3f} | "
                  f"X_span(1-K) start={np.mean(x_top1[:b]):.3f} end={np.mean(x_top1[-b:]):.3f}"
                  + (f" | mean_surge start={np.mean(s_K[:b]):.3f} end={np.mean(s_K[-b:]):.3f} th={cr._surge_mean(p.surge_lam, p.surge_mu, p.surge_rho):.3f}" if s_K else ""))


if __name__ == "__main__":
    test_joint_parametric_variogram()
