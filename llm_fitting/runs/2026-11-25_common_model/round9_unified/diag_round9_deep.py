#!/usr/bin/env python3
"""Deep Round 9 diagnostics (DEVELOPMENT caches only):
  A. Moment-by-moment Gate v1 error decomposition for Round 6 S2 & S3.
  B. Exact band-by-band variance budget on all 4 panels: D_emp(h) vs D_pass + D_obs + D_surge + D_drift.
  C. Band-by-band empirical k3(h) / surge rank profile & surge activation across all 4 platforms.
  D. Check whether entity volatility v_i scales lasting/jump variance vs passing variance (empirical).
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[4]
BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(BASE))

import common_rankdiff as cr
import round3_gold_benchmark as rgb


def part_a_gate_moments():
    print("=" * 80)
    print("A. ROUND 6 EXACT GATE v1 MOMENT-BY-MOMENT ERRORS")
    print("=" * 80)
    g_s2 = json.loads((BASE / "round6_meta/final_bench/candidate_frozen_gate_results.json").read_text())
    g_s3 = json.loads((BASE / "astra_v3/candidate_frozen_gate_results.json").read_text())
    for plat in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
        print(f"\n--- {plat} ---")
        sample_sp = g_s2[plat]["splits"][0]
        print("  split keys:", list(sample_sp.keys()))
        for lbl, d, ak in [("R6_S2", g_s2, "s2"), ("R6_S3", g_s3, "s3")]:
            print(f"  [{lbl}] mean={d[plat][ak+'_rel']:.3f}")
            for sp in d[plat]["splits"]:
                # print all scalar keys except raw dicts
                extra = {k: round(v, 3) if isinstance(v, float) else v
                         for k, v in sp.items() if k in ("T0", f"{ak}_rel", "base_rel", f"w1_{ak}", f"w4_{ak}", f"w13_{ak}", f"r1_{ak}", f"c1_{ak}")}
                # Check if per-moment errors exist
                errs = sp.get(f"{ak}_errs") or sp.get(f"errs_{ak}") or sp.get("moments")
                print(f"    T0={sp['T0']:3d} rel={sp[ak+'_rel']:.3f} (base={sp['base_rel']:.3f}) | " +
                      ", ".join(f"{k}={v}" for k, v in extra.items() if k not in ("T0", f"{ak}_rel", "base_rel")))


def part_b_band_budget_and_surge_profile():
    print("\n" + "=" * 80)
    print("B & C. BAND-BY-BAND VARIANCE BUDGET & SURGE k3 RANK PROFILE (FULL PANELS)")
    print("=" * 80)
    for plat in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
        data, sb = rgb.load_cached(plat)
        X, R, K = data["X"].astype(np.float64), data["R"].astype(np.float64), int(data["K"])
        T, N = X.shape
        floors = np.sum(np.isfinite(R), axis=1).astype(float) + 1.0
        pen = np.where(np.isfinite(R), R, floors[:, None])
        rperm = np.mean(pen, axis=0)
        pres = np.isfinite(R).mean(axis=0)
        pr_ord = np.empty(N, dtype=float)
        pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
        edges = cr._band_edges(K, N)
        nb = len(edges) - 1
        band_all = np.clip(np.searchsorted(edges, pr_ord, side="right") - 1, 0, nb - 1)
        band_of = np.where(pres >= 0.5, band_all, -1)
        band_z = np.log(np.clip((np.sqrt(np.maximum(edges[:-1], 1.0) * edges[1:]) - 0.5) / N, cr.Z_CLIP, 1.0))
        z_K = math.log(max((K - 0.5) / N, cr.Z_CLIP))
        Xc = X - cr._top_k_center(X, R, K)

        p2v = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=False, scale_moment="variance", refine_drift=False)
        p3 = cr.estimate_common(X, R, K, spec_b=sb, asymmetry=True, scale_moment="meta", refine_drift=False)
        D = np.array(p3.diag["D"], float)
        H = p3.diag["H"]
        hs = np.arange(1, H + 1, dtype=float)
        Vs = cr._surge_variance(p3.surge_lam, p3.surge_mu, p3.surge_rho)
        s_sc = cr._surge_scale_of(p3.b_xi, band_z[:7], z_K)

        print(f"\n=== {plat} (T={T}, H={H}, phi_S2={p2v.phi:.2f}, phi_S3={p3.phi:.2f}, sig_d={math.sqrt(p3.decline_var):.5f}, Vs={Vs:.4f}, hl_s={p3.diag['stage3']['surge'].get('hl_s')}) ===")
        # Check at h = H (longest lag) and h = 13: D_emp vs D_pass_S2 + D_obs_S2 + sL2_var*h vs D_surge + D_drift
        h_long = H
        fvel_long = float(cr._f_vel(np.array([h_long]), p3.decline_rho)[0]) if p3.decline_var > 0 else 0.0
        D_drift_long = p3.decline_var * fvel_long
        D_surge_long = (s_sc ** 2) * (2.0 * Vs * (1.0 - p3.surge_rho ** h_long)) if Vs > 0 else np.zeros(7)
        sL2_v = np.array(p2v.diag["sL2_band"][:7])
        vxi_v = np.array(p2v.diag["vxi_band"][:7])
        so2_v = np.array(p2v.diag["so2_band"][:7])
        print(f"  At h={h_long}: D_drift_flat={D_drift_long:.4f}")
        print(f"  {'band':>4s} {'n':>5s} {'dz':>6s} | {'D_emp(H)':>8s} {'sL2v*H':>8s} {'D_srg(H)':>8s} {'D_drf(H)':>8s} {'rem/sL2vH':>9s} | {'k3(13)':>7s} {'k3(H/2)':>8s} {'skew(13)':>8s}")
        for b in range(7):
            idx = np.flatnonzero(band_of == b)
            # compute k3(13) and k3(H//2) in band b
            k3_13, sk_13, k3_hm = np.nan, np.nan, np.nan
            for h_k, tag in [(min(13, H), "13"), (max(4, H // 2), "hm")]:
                if idx.size >= 20 and h_k < T:
                    dx = Xc[h_k:, idx] - Xc[:-h_k, idx]
                    dx = dx - np.nanmean(dx, axis=1, keepdims=True)
                    v = dx[np.isfinite(dx)]
                    if v.size >= 100:
                        sd = float(np.std(v))
                        if sd > 1e-6:
                            vw = np.clip(v, -7.0 * sd, 7.0 * sd)
                            vc = vw - float(np.mean(vw))
                            v2 = float(np.mean(vc ** 2))
                            v3 = float(np.mean(vc ** 3))
                            if tag == "13":
                                k3_13, sk_13 = v3, v3 / (v2 ** 1.5)
                            else:
                                k3_hm = v3
            rem = sL2_v[b] * h_long - D_surge_long[b] - D_drift_long
            frac = rem / max(sL2_v[b] * h_long, 1e-9)
            print(f"  {b:4d} {idx.size:5d} {band_z[b]-z_K:+6.2f} | {D[b, h_long-1]:8.4f} {sL2_v[b]*h_long:8.4f} {D_surge_long[b]:8.4f} {D_drift_long:8.4f} {frac:+9.2f} | {k3_13:+7.3f} {k3_hm:+8.3f} {sk_13:+8.3f}")

        # Also check empirical multi-lag drift decay (Cov(x, y) at gaps g=0, 4, 13, 26, 39) on Reddit
        if T >= 180:
            sel = (pr_ord <= K) & (pres >= 0.5)
            pr_sub = pr_ord[sel]
            lab_sub = np.where(pr_sub <= (K / 16.0), 0, np.where(pr_sub <= (K / 4.0), 1, 2))
            Xsub = Xc[:, sel]
            print("  Multi-gap empirical Cov(x,y) by tier (W=13, H=39, gaps g=0,13,26,39,52):")
            for g in (0, 13, 26, 39, 52):
                W, Hw = 13, 39
                tier_c = [[], [], []]
                for o in range(0, T - (2 * Hw + g) + 1, 13):
                    with np.errstate(all="ignore"):
                        a0 = np.nanmean(Xsub[o:o+W], axis=0)
                        a1 = np.nanmean(Xsub[o+Hw-W:o+Hw], axis=0)
                        b0 = np.nanmean(Xsub[o+Hw+g:o+Hw+g+W], axis=0)
                        b1 = np.nanmean(Xsub[o+2*Hw+g-W:o+2*Hw+g], axis=0)
                    ok = np.isfinite(a0) & np.isfinite(a1) & np.isfinite(b0) & np.isfinite(b1)
                    x, y = a1 - a0, b1 - b0
                    for j in (0, 1, 2):
                        sj = ok & (lab_sub == j)
                        if sj.sum() >= 30:
                            xd = x[sj] - np.mean(x[sj])
                            yd = y[sj] - np.mean(y[sj])
                            tier_c[j].append(float(np.mean(xd * yd)))
                # Compare decay ratio to hl_d = 13, 26, 52, 104, inf
                vals = [np.mean(tc) if tc else np.nan for tc in tier_c]
                print(f"    g={g:2d}: K/16={vals[0]:+.4f}  K/4={vals[1]:+.4f}  K={vals[2]:+.4f}  (G_drift(hl=52)={cr._G_drift(0.5**(1/52), W, Hw, g):.0f}, hl=26={cr._G_drift(0.5**(1/26), W, Hw, g):.0f}, hl=13={cr._G_drift(0.5**(1/13), W, Hw, g):.0f})")


if __name__ == "__main__":
    part_a_gate_moments()
    part_b_band_budget_and_surge_profile()
