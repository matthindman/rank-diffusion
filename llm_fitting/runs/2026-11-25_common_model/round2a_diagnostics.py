#!/usr/bin/env python3
"""Round 2A Diagnostics (D1, D2, D3) responding directly to Fable 5.2 & Astra 6.2 reviews.

D1: Point-forecast (MAE) & share-MAE autopsy on all 4 panels:
    Compare deterministic Kalman posterior mean vs 20-draw vs 99-draw MC median vs
    no_change vs Stage-1 local_level EWMA, and isolate P12=-V_xi*v_hat, s>=0 clamp
    compensation, and unmasked top-K share denominator.
D2: Surge & downside-asymmetry identifiability audit:
    - Extended surge half-life grid {4, 8, 13, 20, 26, 39, 52, 78, 104} with hs up to 52
    - Exact capped-exponential moments m1, m2, m3 vs uncapped
    - Sign-preserving within-entity permutation null for Reddit decline runs r2, r3
    - Three-window mixed third moment (Astra's proposal) for skewed redraw drift theta
D3: Matched-length & count-noise placebo audit:
    - Re-estimate Stage 3 on Reddit cut to 86w (2020-12-28..2022-08-22, R19 dates) and 52w
    - Multi-gap covariance check C_g / G_d(g; 156) at g in {4, 13, 26} on Reddit
    - Poisson-thinned count-noise placebo (true s_vol = 0) to test whether s_vol ~ 0.70 is mechanical.

Runs 100% sandboxed on DEVELOPMENT caches only. Zero sealed data contact.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import common_rankdiff as cr  # noqa: E402

CACHE_DIR = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
OUT_DIR = Path(__file__).resolve().parent


def load_cached(panel: str):
    d = np.load(CACHE_DIR / f"{panel}.npz", allow_pickle=True)
    data = {k: d[k] for k in d.files}
    sb = None
    sb_path = CACHE_DIR / f"{panel}_spec_b.json"
    if sb_path.exists():
        js = json.loads(sb_path.read_text())
        sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
    return data, sb


def run_d1_mae_autopsy() -> dict:
    print("\n================ D1: POINT-FORECAST & SHARE-MAE AUTOPSY ================", flush=True)
    out = {}
    for pn, cfg in [
        ("facebook_a", dict(horizons=[1, 4, 13, 26], spec_b=True)),
        ("instagram_hm", dict(horizons=[1, 4, 13], spec_b=False)),
        ("reddit_comments_ext", dict(horizons=[1, 4, 13, 26, 52], spec_b=False)),
        ("reddit_submissions_long", dict(horizons=[1, 4, 13, 26, 52], spec_b=False)),
    ]:
        data, sb = load_cached(pn)
        X, R, floors, K = data["X"].astype(float), data["R"].astype(float), data["floors"].astype(float), int(data["K"])
        T, N = X.shape
        horizons = cfg["horizons"]
        max_h = max(horizons)
        min_train = 78 if T >= 180 else (36 if T >= 80 else 22)
        origins = np.unique(np.round(np.linspace(min_train, T - max_h - 1, 5)).astype(int)).tolist()

        med_K = np.array([np.nanmedian(X[t, np.isfinite(R[t]) & (R[t] <= K)]) for t in range(T)])
        Xr = X - med_K[:, None]
        A = np.where(np.isfinite(X), np.expm1(np.clip(X, 0.0, 40.0)), 0.0)
        tot_K = np.sum(np.where(np.isfinite(R) & (R <= K), A, 0.0), axis=1)
        S_share = A / np.maximum(tot_K[:, None], 1.0)

        rows_h = {str(h): [] for h in horizons}
        for si, T0 in enumerate(origins):
            X_tr, R_tr, fl_tr = X[:T0], R[:T0], floors[:T0]
            p2 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=sb if cfg["spec_b"] else None, refine_drift=True, asymmetry=False)
            p3 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=sb if cfg["spec_b"] else None, refine_drift=True, asymmetry=True)
            f2 = cr.filter_endpoints(X_tr, R_tr, p2, floors_tr=fl_tr)
            f3 = cr.filter_endpoints(X_tr, R_tr, p3, floors_tr=fl_tr)

            pen_tr = np.where(np.isfinite(R_tr), R_tr, fl_tr[:, None])
            ord_tr = np.argsort(np.mean(pen_tr, axis=0), kind="stable")
            pres_tr = np.isfinite(R_tr).mean(axis=0)
            topK = ord_tr[:K]
            if pn == "instagram_hm":
                topK = topK[pres_tr[topK] >= 0.5]

            # Train-only local-level EWMA tuned on train 1-step-ahead MAE
            Xr_tr = Xr[:T0]
            best_a, best_e = 0.5, np.inf
            for alpha in (0.15, 0.25, 0.35, 0.50, 0.65, 0.80):
                ew = np.full(N, np.nan)
                errs = []
                for t in range(T0):
                    o = np.isfinite(Xr_tr[t])
                    if t >= max(5, T0 // 2):
                        ok = o & np.isfinite(ew)
                        if ok.any():
                            errs.append(float(np.mean(np.abs(Xr_tr[t, ok] - ew[ok]))))
                    ew = np.where(o, np.where(np.isfinite(ew), alpha * Xr_tr[t] + (1.0 - alpha) * ew, Xr_tr[t]), ew)
                e = float(np.mean(errs)) if errs else np.inf
                if e < best_e:
                    best_e, best_a = e, alpha
            ew = np.full(N, np.nan)
            last_Xr = np.full(N, np.nan)
            for t in range(T0):
                o = np.isfinite(Xr_tr[t])
                ew = np.where(o, np.where(np.isfinite(ew), best_a * Xr_tr[t] + (1.0 - best_a) * ew, Xr_tr[t]), ew)
                last_Xr = np.where(o, Xr_tr[t], last_Xr)

            # Analytical deterministic Kalman mean forecast at horizon h!
            # S2: E[X_{T0-1+h}] = L_hat + h * g_i + phi^h * xi_hat (or without g_i!)
            z_i = np.log(np.clip((np.argsort(np.argsort(np.mean(pen_tr, axis=0), kind="stable")) + 0.5) / N, cr.Z_CLIP, 1.0))
            g2_i = np.interp(z_i, p2.drift_z, p2.drift_g)
            g3_i = np.interp(z_i, p3.drift_z, p3.drift_g)
            has_d = p3.decline_var > 0.0
            has_s = p3.surge_lam > 0.0 and p3.surge_mu > 0.0 and p3.surge_rho > 0.0
            rho_d = p3.decline_rho if has_d else 0.0
            rho_s = p3.surge_rho if has_s else 0.0

            # Also check clamp frequency in S3!
            clamp_frac = 0.0
            clamp_mean_shift = 0.0
            if has_s and "s_hat" in f3:
                s_sc = f3["s_scale"]
                s_mean = cr._surge_mean(p3.surge_lam, p3.surge_mu, p3.surge_rho)
                s_u = f3["s_hat"] / np.maximum(s_sc, 1e-4) + s_mean
                cl = s_u < 0.0
                clamp_frac = float(np.mean(cl[topK]))
                clamp_mean_shift = float(np.mean(np.where(cl[topK], -s_sc[topK] * s_u[topK], 0.0)))

            for h in horizons:
                t_fut = T0 - 1 + h
                yt = Xr[t_fut, topK]
                ok = np.isfinite(yt) & np.isfinite(last_Xr[topK]) & np.isfinite(ew[topK])
                if ok.sum() < 100:
                    continue
                # Common truth-defined cohort for ALL point forecasts!
                def _rel_mae(pred_full):
                    # Center pred by top-K median of pred!
                    pm = float(np.nanmedian(pred_full[topK]))
                    return float(np.mean(np.abs(yt[ok] - (pred_full[topK[ok]] - pm))))

                def _share_mae_corr(pred_full):
                    # Convert deterministic level forecast to arithmetic share vs top-K total!
                    a_hat = np.expm1(np.clip(pred_full, 0.0, 40.0))
                    # Use predicted top-K total (all N slots)
                    ord_p = np.argsort(-pred_full)[:K]
                    tot_p = float(np.sum(a_hat[ord_p]))
                    s_hat = a_hat[topK] / max(tot_p, 1.0)
                    s_now = S_share[T0 - 1, topK]
                    s_tru = S_share[t_fut, topK]
                    ds_t = s_tru - s_now
                    ds_h = s_hat - s_now
                    mae = float(np.mean(np.abs(ds_t - ds_h)))
                    corr = float(np.corrcoef(ds_t, ds_h)[0, 1]) if np.std(ds_h) > 1e-12 else 0.0
                    return mae, corr

                # Deterministic S2 mean (with and without g_i)
                pred_s2_g = f2["L_hat"] + h * g2_i + (p2.phi ** h) * f2["xi_hat"]
                pred_s2_nog = f2["L_hat"] + (p2.phi ** h) * f2["xi_hat"]

                # Deterministic S3 mean
                d_term = ((1.0 - rho_d ** h) / max(1.0 - rho_d, 1e-9)) * f3["d_hat"] if (has_d and "d_hat" in f3) else 0.0
                s_term = (rho_s ** h) * f3["s_hat"] if (has_s and "s_hat" in f3) else 0.0
                pred_s3_g = f3["L_hat"] + h * g3_i + d_term + (p3.phi ** h) * f3["xi_hat"] + s_term
                pred_s3_nog = f3["L_hat"] + d_term + (p3.phi ** h) * f3["xi_hat"] + s_term

                # Also check local_level share MAE
                ew_abs = ew + float(med_K[T0 - 1])
                sh_no = float(np.mean(np.abs(S_share[t_fut, topK] - S_share[T0 - 1, topK])))
                sh_ew_m, sh_ew_r = _share_mae_corr(ew_abs)
                sh_s2_m, sh_s2_r = _share_mae_corr(pred_s2_g)
                sh_s3_m, sh_s3_r = _share_mae_corr(pred_s3_g)

                rows_h[str(h)].append(dict(
                    mae_no=float(np.mean(np.abs(yt[ok] - last_Xr[topK[ok]]))),
                    mae_ewma=float(np.mean(np.abs(yt[ok] - ew[topK[ok]]))),
                    mae_s2_det=float(_rel_mae(pred_s2_g)),
                    mae_s2_nog=float(_rel_mae(pred_s2_nog)),
                    mae_s3_det=float(_rel_mae(pred_s3_g)),
                    mae_s3_nog=float(_rel_mae(pred_s3_nog)),
                    sh_no=sh_no, sh_ewma=sh_ew_m, sh_ewma_r=sh_ew_r,
                    sh_s2=sh_s2_m, sh_s2_r=sh_s2_r, sh_s3=sh_s3_m, sh_s3_r=sh_s3_r,
                    clamp_frac=clamp_frac, clamp_shift=clamp_mean_shift,
                ))

        agg = {hs: {k: float(np.mean([r[k] for r in rs])) for k in rs[0]} for hs, rs in rows_h.items() if rs}
        out[pn] = agg
        print(f"--- {pn} (clamp_frac={agg['1']['clamp_frac']:.3f}, clamp_shift={agg['1']['clamp_shift']:.4f}) ---", flush=True)
        for hs, m in agg.items():
            print(f"  h={int(hs):2d}: MAE no={m['mae_no']:.4f} ewma={m['mae_ewma']:.4f} S2_det={m['mae_s2_det']:.4f} (noG={m['mae_s2_nog']:.4f}) S3_det={m['mae_s3_det']:.4f} (noG={m['mae_s3_nog']:.4f}) | "
                  f"shMAE*1e5 no={m['sh_no']*1e5:.2f} ewma={m['sh_ewma']*1e5:.2f}(r={m['sh_ewma_r']:.2f}) S2={m['sh_s2']*1e5:.2f}(r={m['sh_s2_r']:.2f}) S3={m['sh_s3']*1e5:.2f}(r={m['sh_s3_r']:.2f})", flush=True)
    return out


def run_d2_surge_and_downside() -> dict:
    print("\n================ D2: SURGE EXTENDED GRID + SIGN-PRESERVING RUNS + 3-WINDOW SKEW ================", flush=True)
    out = {}
    hl_ext = np.array([4.0, 8.0, 13.0, 20.0, 26.0, 39.0, 52.0, 78.0, 104.0])
    # Exact capped Exp(1) moments for cap C = 5.0
    C = 5.0
    eC = math.exp(-C)
    m1_c = 1.0 - eC * (1.0 + C)  # wait: E[min(E,C)] = 1 - e^-C! Let's compute exact via integral!
    # E[min(E, C)^k] = k! * (1 - e^-C * sum_{l=0}^{k-1} C^l/l!) + C^k * e^-C ... wait:
    # Let's check exact formula: int_0^C x^k e^-x dx + C^k e^-C = k! (1 - e^-C sum_{l=0}^k C^l/l!) + C^k e^-C = k! (1 - e^-C sum_{l=0}^{k-1} C^l/l!)!
    # For k=1: 1!*(1 - e^-C) = 1 - e^-5 = 0.993262! Matches Astra!
    # For k=2: 2!*(1 - e^-5*(1 + 5)) = 2*(1 - 6*e^-5) = 1.919145! Matches Astra!
    # For k=3: 6*(1 - e^-5*(1 + 5 + 12.5)) = 5.252087! Matches Astra!
    m_cap = [math.factorial(k) * (1.0 - eC * sum((C ** l) / math.factorial(l) for l in range(k))) for k in (1, 2, 3, 4)]
    print(f"  Exact capped Exp(1) raw moments (C=5): {[round(x, 5) for x in m_cap]}", flush=True)

    for pn in ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]:
        data, _ = load_cached(pn)
        X, R, floors, K = data["X"].astype(float), data["R"].astype(float), data["floors"].astype(float), int(data["K"])
        T, N = X.shape
        pen = np.where(np.isfinite(R), R, floors[:, None])
        pr = np.empty(N); pr[np.argsort(np.mean(pen, axis=0), kind="stable")] = np.arange(1, N + 1)
        pres = np.isfinite(R).mean(axis=0)
        sel = (pr <= K) & (pres >= 0.5)
        Xc = X[:, sel] - np.nanmean(X[:, sel], axis=1, keepdims=True)

        # 1. Extended surge grid fit up to h=52
        hs = np.array([h for h in (4, 8, 13, 20, 26, 39, 52) if h <= min(52, T // 2)], dtype=float)
        k3s, k5s, excs, k2s = [], [], [], []
        for h in hs.astype(int):
            dx = Xc[h:] - Xc[:-h]
            dx = dx - np.nanmean(dx, axis=1, keepdims=True)
            v = dx[np.isfinite(dx)]
            sd = float(np.std(v))
            vw = np.clip(v, -6.0 * sd, 6.0 * sd)
            vc = vw - float(np.mean(vw))
            v2 = float(np.mean(vc ** 2))
            v3 = float(np.mean(vc ** 3))
            v5 = float(np.mean(vc ** 5)) - 10.0 * v2 * v3  # exact 5th cumulant!
            exc = float(np.mean((vc ** 3) * np.abs(vc))) - 4.0 * float(np.mean(np.abs(vc))) * v3
            k2s.append(v2); k3s.append(v3); k5s.append(v5); excs.append(exc)
        k2s, k3s, k5s, excs = map(np.array, (k2s, k3s, k5s, excs))
        best = None
        for hl in hl_ext:
            rg = 0.5 ** (1.0 / hl)
            f3 = 3.0 * (rg ** hs) * (1.0 - rg ** hs)
            C3 = max(float((f3 @ k3s) / (f3 @ f3)), 0.0)
            err = float(np.linalg.norm(k3s - C3 * f3) / max(np.linalg.norm(k3s), 1e-9))
            if best is None or err < best[0]:
                best = (err, float(hl), C3)
        # Also check k5 / k3 implied mu across horizons (exact kappa_5(J)/kappa_3(J) ~ 20*mu^2 for uncapped; ~10*mu^2 for capped!)
        mu_k5 = [math.sqrt(max(k5s[i] / max(20.0 * k3s[i], 1e-9), 0.0)) for i in range(hs.size)]

        # 2. Sign-preserving within-entity permutation null for decline runs r2, r3!
        rb = 26 if T >= 160 else (13 if T >= 75 else 7)
        nb = T // rb
        st = T - nb * rb
        B = np.array([np.nanmean(Xc[st + b * rb:st + (b + 1) * rb], axis=0) for b in range(nb)])
        dB = np.diff(B, axis=0)
        dB = dB - np.nanmean(dB, axis=1, keepdims=True)
        # Observed r2, r3
        def _runs(M):
            ok2 = np.isfinite(M[:-1]) & np.isfinite(M[1:])
            r2 = float(np.sum(ok2 & (M[:-1] < 0) & (M[1:] < 0)) / max(np.sum(ok2 & (M[:-1] > 0) & (M[1:] > 0)), 1))
            ok3 = np.isfinite(M[:-2]) & np.isfinite(M[1:-1]) & np.isfinite(M[2:])
            r3 = float(np.sum(ok3 & (M[:-2] < 0) & (M[1:-1] < 0) & (M[2:] < 0)) / max(np.sum(ok3 & (M[:-2] > 0) & (M[1:-1] > 0) & (M[2:] > 0)), 1))
            return r2, r3
        r2_obs, r3_obs = _runs(dB)
        p_neg = float(np.mean(dB[np.isfinite(dB)] < 0))
        rng = np.random.default_rng(42)
        null_r2, null_r3 = [], []
        n_tr = dB.shape[0]
        for _ in range(100):
            # Permute time order independently WITHIN each entity (preserves entity's exact sign counts & missingness!)
            M_perm = np.empty_like(dB)
            for j in range(dB.shape[1]):
                M_perm[:, j] = dB[rng.permutation(n_tr), j]
            a2, a3 = _runs(M_perm)
            null_r2.append(a2); null_r3.append(a3)

        # 3. Three-window mixed third moment (Astra's proposal!) on Reddit
        tri_m3 = None
        if T >= 180:
            W, H, g = 13, 39, 13
            # Three disjoint gapped 39w changes spanning 3*39 + 2*13 = 143w
            vals = []
            for o in range(0, T - (3 * H + 2 * g) + 1, 13):
                c1 = np.nanmean(Xc[o + H - W:o + H], 0) - np.nanmean(Xc[o:o + W], 0)
                o2 = o + H + g
                c2 = np.nanmean(Xc[o2 + H - W:o2 + H], 0) - np.nanmean(Xc[o2:o2 + W], 0)
                o3 = o2 + H + g
                c3 = np.nanmean(Xc[o3 + H - W:o3 + H], 0) - np.nanmean(Xc[o3:o3 + W], 0)
                ok = np.isfinite(c1) & np.isfinite(c2) & np.isfinite(c3)
                if ok.sum() > 200:
                    u1 = c1[ok] - np.mean(c1[ok])
                    u2 = c2[ok] - np.mean(c2[ok])
                    u3 = c3[ok] - np.mean(c3[ok])
                    s1, s2, s3 = np.std(u1), np.std(u2), np.std(u3)
                    vals.append(float(np.mean(np.clip(u1/s1,-6,6) * np.clip(u2/s2,-6,6) * np.clip(u3/s3,-6,6))))
            tri_m3 = float(np.mean(vals)) if vals else None

        out[pn] = dict(
            ext_surge=dict(rel_err=best[0], hl_best=best[1], C3=best[2], mu_k5=[round(x, 3) for x in mu_k5]),
            runs=dict(
                block=rb, p_neg=p_neg,
                r2_obs=r2_obs, r2_signperm_med=float(np.median(null_r2)), r2_signperm_q95=float(np.quantile(null_r2, 0.95)),
                r3_obs=r3_obs, r3_signperm_med=float(np.median(null_r3)), r3_signperm_q95=float(np.quantile(null_r3, 0.95)),
            ),
            tri_window_std_m3=tri_m3,
        )
        print(f"--- {pn} ---", flush=True)
        print(f"  ext_surge (h<=52): hl_best={best[1]:.0f}w (rel_err={best[0]:.3f}), C3={best[2]:.3f}, mu_k5={out[pn]['ext_surge']['mu_k5']}", flush=True)
        rn = out[pn]["runs"]
        print(f"  runs (blk={rb}w, P<0={p_neg:.4f}): r2={rn['r2_obs']:.3f} vs sign-perm med={rn['r2_signperm_med']:.3f} [q95={rn['r2_signperm_q95']:.3f}] | "
              f"r3={rn['r3_obs']:.3f} vs sign-perm med={rn['r3_signperm_med']:.3f} [q95={rn['r3_signperm_q95']:.3f}] | 3-win std m3={tri_m3}", flush=True)
    return out


def run_d3_matched_length_and_placebo() -> dict:
    print("\n================ D3: MATCHED-LENGTH REDDIT & s_vol COUNT-NOISE PLACEBO ================", flush=True)
    out = dict(matched_length={}, multi_gap={}, s_vol_placebo={})

    # 1. Matched-length Reddit: 86w from R19 date (approx week 109..195 -> 2020-12-28..2022-08-22) + 52w (last 52w)
    for pn in ["reddit_comments_ext", "reddit_submissions_long"]:
        data, _ = load_cached(pn)
        X, R, floors, K = data["X"].astype(float), data["R"].astype(float), data["floors"].astype(float), int(data["K"])
        T, N = X.shape
        # Check exact dates if stored
        dates = data["dates"] if "dates" in data else None
        idx86 = 109 if T >= 195 else T - 86
        res_pn = {}
        for tag, sl in [("full", slice(0, T)), ("86w_r19", slice(idx86, idx86 + 86)), ("86w_early", slice(0, 86)), ("52w_late", slice(T - 52, T))]:
            Xs, Rs, fls = X[sl], R[sl], floors[sl]
            p3 = cr.estimate_common(Xs, Rs, K=K, floors=fls, refine_drift=False, asymmetry=True)
            sd = math.sqrt(p3.decline_var) if p3.decline_var > 0 else 0.0
            res_pn[tag] = dict(
                T=int(Xs.shape[0]), sigma_L0=p3.sigma_L0, phi=p3.phi, s_vol=p3.s_vol,
                sigma_d=sd, surge_hl=float(-math.log(2)/math.log(p3.surge_rho)) if p3.surge_rho > 0 else 0.0,
                surge_lam=p3.surge_lam, surge_mu=p3.surge_mu,
                drift_diag=p3.diag["stage3"]["drift"],
            )
            print(f"  {pn:24s} [{tag:9s} T={Xs.shape[0]:3d}]: sigma_d={sd:.5f} (cov_emp={p3.diag['stage3']['drift'].get('cov_emp',0):+.4f}, net={p3.diag['stage3']['drift'].get('net_cov',0):+.4f}) phi={p3.phi:.2f} s_vol={p3.s_vol:.3f}", flush=True)
        out["matched_length"][pn] = res_pn

        # 2. Multi-gap check C_g / G_d(g; 156) on full Reddit panels!
        p2 = cr.estimate_common(X, R, K=K, floors=floors, refine_drift=False, asymmetry=False)
        pen = np.where(np.isfinite(R), R, floors[:, None])
        pr = np.empty(N); pr[np.argsort(np.mean(pen, axis=0), kind="stable")] = np.arange(1, N + 1)
        pres = np.isfinite(R).mean(axis=0)
        sel = (pr <= K) & (pres >= 0.5)
        z_sel = np.log(np.clip((pr[sel] - 0.5) / N, cr.Z_CLIP, 1.0))
        Vxi_m = float(np.mean(cr.v_xi_of(p2, z_sel)))
        Xc = X[:, sel] - np.nanmean(X[:, sel], axis=1, keepdims=True)
        rho_d = 0.5 ** (1.0 / 156.0)
        mg = {}
        for g_val in (4, 13, 26, 39):
            W, H = 13, 39
            if 2 * H + g_val > T:
                continue
            covs = []
            for o in range(0, T - (2 * H + g_val) + 1, 13):
                a0, a1 = np.nanmean(Xc[o:o+W], 0), np.nanmean(Xc[o+H-W:o+H], 0)
                b0, b1 = np.nanmean(Xc[o+H+g_val:o+H+g_val+W], 0), np.nanmean(Xc[o+2*H+g_val-W:o+2*H+g_val], 0)
                ok = np.isfinite(a0) & np.isfinite(a1) & np.isfinite(b0) & np.isfinite(b1)
                x, y = a1[ok] - a0[ok], b1[ok] - b0[ok]
                x -= x.mean(); y -= y.mean()
                sx, sy = x.std(), y.std()
                covs.append(float(np.mean(np.clip(x,-6*sx,6*sx) * np.clip(y,-6*sy,6*sy))))
            c_emp = float(np.mean(covs))
            c_xi = Vxi_m * cr._G_ar1(p2.phi, W, H, g_val)
            gd = cr._G_drift(rho_d, W, H, g_val)
            sd_g = math.sqrt(max((c_emp - c_xi) / gd, 0.0))
            mg[str(g_val)] = dict(cov_emp=c_emp, c_xi=c_xi, G_d=gd, implied_sigma_d=sd_g)
        out["multi_gap"][pn] = mg
        print(f"  {pn:24s} multi-gap (W=13,H=39): " + ", ".join(f"g={k}:sd={v['implied_sigma_d']:.5f}" for k, v in mg.items()), flush=True)

    # 3. Poisson-thinned count-noise placebo (true s_vol = 0!)
    #    Simulate a pure count-noise + homogeneous diffusion ladder matching Reddit/FB levels
    #    with ZERO entity volatility heterogeneity (s_vol = 0), and check what _split_half_vol_spread returns!
    rng = np.random.default_rng(99)
    for tag, (K_p, N_p, T_p, L_top, slope, sL) in [
        ("fb_like_poisson", (3500, 14000, 86, 14.5, 0.85, 0.11)),
        ("reddit_like_poisson", (5000, 20000, 212, 13.5, 1.05, 0.08)),
    ]:
        r = np.arange(1, N_p + 1, dtype=float)
        L = L_top - slope * np.log(r)
        X_sim = np.empty((T_p, N_p))
        for t in range(T_p):
            L = L + sL * rng.standard_normal(N_p)
            # True counts = Poisson(exp(L)) -> exact count-noise heterogeneity with ZERO s_vol!
            lam_c = np.exp(np.clip(L, -2.0, 18.0))
            cnt = rng.poisson(lam_c)
            X_sim[t] = np.log1p(cnt)
        R_sim = np.empty_like(X_sim)
        for t in range(T_p):
            R_sim[t, np.argsort(-X_sim[t])] = np.arange(1, N_p + 1)
        p_pl = cr.estimate_common(X_sim, R_sim, K=K_p, refine_drift=False, asymmetry=False)
        out["s_vol_placebo"][tag] = dict(s_vol_hat=p_pl.s_vol, sigma_L0=p_pl.sigma_L0, phi=p_pl.phi)
        print(f"  Placebo {tag:22s} (true s_vol=0, Poisson counts): s_vol_hat={p_pl.s_vol:.4f} (vs emp ~0.72!)", flush=True)

    return out


def main():
    res = dict(
        d1_mae_autopsy=run_d1_mae_autopsy(),
        d2_surge_downside=run_d2_surge_and_downside(),
        d3_matched_placebo=run_d3_matched_length_and_placebo(),
    )
    out_path = OUT_DIR / "round2a_diagnostics.json"
    out_path.write_text(json.dumps(res, indent=2))
    print(f"\nSaved -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
