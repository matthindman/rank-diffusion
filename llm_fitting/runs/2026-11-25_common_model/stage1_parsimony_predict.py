#!/usr/bin/env python3
"""Stage 1E & 1F diagnostics:
  1E. Parsimony & Defect Audit across 8 permanent-rank bands on all four DEVELOPMENT ladders:
      - Band profiles of gamma0, gamma1, gamma2, structure function D_b(h)
      - Linear-in-log-rank (2-parameter) R^2 for volatility and absence profiles
      - Standardized weekly residual tail asymmetry & boundedness
      - Intermittency vs. permanent exit rates
      - Entity volatility persistence (split-half correlation of log sigma_i)
  1F. Calendar-aligned Individual Endpoint Predictability Ceiling (Goal 2):
      - Rolling origins (5 splits per panel, train-only universe, strict calendar alignment T0 -> T0 + h)
      - Targets: relative log-level CRPS/MAE, coarsened log-rank CRPS, audience-share shift MAE/corr,
        top-K and top-K/16 retention Brier, move-size |dX| correlation/R^2
      - Predictors: no_change, local_level (EWMA/Kalman), local_level + sym_trend,
        local_level + asym_trend (separate decline vs. gain), local_level + items_channel,
        rank_bin empirical transition kernel; band-only vs. empirical-Bayes entity volatility v_i.

Runs entirely from the lean scratch cache (DEVELOPMENT data only).
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
from scipy.stats import norm, spearmanr

CACHE_DIR = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
OUT_DIR = Path(__file__).resolve().parent

PANELS = ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]


def load_cached(panel: str) -> dict:
    d = np.load(CACHE_DIR / f"{panel}.npz", allow_pickle=True)
    return {k: d[k] for k in d.files}


def band_edges(K: int, M: int) -> np.ndarray:
    e = np.array([0, K / 64, K / 32, K / 16, K / 8, K / 4, K / 2, K, max(M, K + 1)], dtype=float)
    return np.maximum.accumulate(e)


def linear_r2(x: np.ndarray, y: np.ndarray, w: np.ndarray | None = None) -> tuple[float, float, float]:
    ok = np.isfinite(x) & np.isfinite(y)
    if ok.sum() < 3:
        return np.nan, np.nan, np.nan
    xo, yo = x[ok], y[ok]
    wo = np.ones_like(xo) if w is None else np.sqrt(np.maximum(w[ok], 1.0))
    A = np.column_stack([np.ones_like(xo), xo]) * wo[:, None]
    coef = np.linalg.lstsq(A, yo * wo, rcond=None)[0]
    yhat = coef[0] + coef[1] * xo
    ybar = np.average(yo, weights=wo ** 2)
    ss_res = float(np.sum((wo ** 2) * (yo - yhat) ** 2))
    ss_tot = float(np.sum((wo ** 2) * (yo - ybar) ** 2))
    r2 = 1.0 - ss_res / max(ss_tot, 1e-12)
    return float(r2), float(coef[0]), float(coef[1])


def parsimony_audit(pn: str, d: dict) -> dict:
    X = d["X"].astype(np.float64)
    R = d["R"].astype(np.float64)
    rperm = d["rperm"].astype(np.float64)
    pres = d["pres"].astype(np.float64)
    K = int(d["K"])
    T, M = X.shape

    # Common-factor demeaned weekly X
    Xc = X - np.nanmean(X, axis=1, keepdims=True)
    dX = np.diff(Xc, axis=0)  # (T-1, M)

    # Permanent rank index 1..M
    pr_idx = np.empty(M, dtype=float)
    pr_idx[np.argsort(rperm, kind="stable")] = np.arange(1, M + 1)
    edges = band_edges(K, M)
    nb = len(edges) - 1
    band_of = np.clip(np.searchsorted(edges, pr_idx, side="right") - 1, 0, nb - 1)
    band_z = np.log(np.clip((np.sqrt(np.maximum(edges[:-1], 1.0) * edges[1:]) - 0.5) / M, 1e-6, 1.0))

    horizons = [h for h in (1, 2, 4, 8, 13, 26, 52) if h < T // 2]
    bands = []
    for b in range(nb):
        sel = (band_of == b) & (pres >= 0.5)
        n_b = int(sel.sum())
        if n_b < 10:
            bands.append(dict(band=b, n=n_b))
            continue
        dx_b = dX[:, sel]
        g0 = float(np.nanmean(dx_b * dx_b))
        g1 = float(np.nanmean(dx_b[1:] * dx_b[:-1]))
        g2 = float(np.nanmean(dx_b[2:] * dx_b[:-2]))
        # Structure function D_b(h)
        Dh = {}
        for h in horizons:
            dh = Xc[h:, sel] - Xc[:-h, sel]
            Dh[str(h)] = float(np.nanmean(dh * dh))
        # Rough lasting slope from last two horizons
        h_a, h_b = horizons[-2], horizons[-1]
        sL2 = max((Dh[str(h_b)] - Dh[str(h_a)]) / (h_b - h_a), 1e-8)
        # Absence and return rate within band (using ALL entities in band, not pres>=0.5)
        sel_all = band_of == b
        R_b = R[:, sel_all]
        abs_rate = float(np.mean(~np.isfinite(R_b)))
        # Among absence spells starting inside [1, T-14], fraction returning within 4 and 13 weeks
        obs_b = np.isfinite(R_b)
        starts = obs_b[:-1] & (~obs_b[1:])  # became absent at t+1
        ret4_num = ret13_num = starts_n = 0
        for t in range(min(T - 14, starts.shape[0])):
            who = np.flatnonzero(starts[t])
            if who.size:
                starts_n += who.size
                ret4_num += int(np.sum(np.any(obs_b[t + 2:t + 6, who], axis=0)))
                ret13_num += int(np.sum(np.any(obs_b[t + 2:t + 15, who], axis=0)))
        bands.append(dict(
            band=b, z=float(band_z[b]), n=n_b, mean_X=float(np.nanmean(X[:, sel])),
            g0=g0, g1=g1, g2=g2, lag1_corr=float(g1 / max(g0, 1e-12)),
            Dh=Dh, sL2_rough=float(sL2),
            abs_rate=abs_rate,
            return_within_4w=float(ret4_num / max(starts_n, 1)) if starts_n else None,
            return_within_13w=float(ret13_num / max(starts_n, 1)) if starts_n else None,
            n_absence_starts=int(starts_n),
        ))

    # Linear-in-z R^2 across the 7 inside-K bands
    in_b = [b for b in bands[:nb - 1] if "g0" in b]
    zs = np.array([b["z"] for b in in_b])
    ns = np.array([b["n"] for b in in_b], float)
    r2_g0, _, slope_g0 = linear_r2(zs, np.log([b["g0"] for b in in_b]), ns)
    r2_sL, _, slope_sL = linear_r2(zs, np.log([b["sL2_rough"] for b in in_b]), ns)
    r2_g1c, _, slope_g1c = linear_r2(zs, np.array([b["lag1_corr"] for b in in_b]), ns)
    # Also linear in mean_X (size law)
    xs_lev = np.array([b["mean_X"] for b in in_b])
    r2_sL_lev, _, slope_sL_lev = linear_r2(xs_lev, np.log([b["sL2_rough"] for b in in_b]), ns)

    # Tail anatomy & entity volatility persistence (top-K entities with pres >= 0.5)
    inK = (pr_idx <= K) & (pres >= 0.5)
    dxK = dX[:, inK]
    # Split-half entity volatility correlation
    half = dxK.shape[0] // 2
    sd1 = np.nanstd(dxK[:half], axis=0)
    sd2 = np.nanstd(dxK[half:], axis=0)
    ok_v = np.isfinite(sd1) & np.isfinite(sd2) & (sd1 > 1e-4) & (sd2 > 1e-4)
    vol_split_corr = float(np.corrcoef(np.log(sd1[ok_v]), np.log(sd2[ok_v]))[0, 1])
    vol_split_spear = float(spearmanr(sd1[ok_v], sd2[ok_v]).correlation)
    # Within-band log-sd spread (proxy for temperament s)
    sd_all = np.nanstd(dxK, axis=0)
    ok_s = np.isfinite(sd_all) & (sd_all > 1e-4)
    log_sd = np.log(sd_all[ok_s])
    band_inK = band_of[inK][ok_s]
    resid_log_sd = log_sd.copy()
    for b in range(nb):
        m = band_inK == b
        if m.sum() > 5:
            resid_log_sd[m] -= np.mean(resid_log_sd[m])
    # Note: var(log(sigma_i^2)) = 4 * var(log(sigma_i)), minus sampling variance ~ 2/T
    s_raw_var = float(np.var(2.0 * resid_log_sd))
    s_est = float(math.sqrt(max(s_raw_var - 2.0 / max(T - 1, 1), 0.0)))

    # Standardized weekly increments (by entity SD) -> tail quantiles & positive vs negative symmetry
    u = dxK[:, ok_s] / sd_all[ok_s][None, :]
    uf = u[np.isfinite(u)]
    raw_f = dxK[np.isfinite(dxK)]
    qs = np.quantile(uf, [0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999])
    rqs = np.quantile(raw_f, [0.001, 0.01, 0.99, 0.999])
    tails = dict(
        std_q001=float(qs[0]), std_q01=float(qs[1]), std_q05=float(qs[2]),
        std_q95=float(qs[4]), std_q99=float(qs[5]), std_q999=float(qs[6]),
        std_kurt=float(np.mean(uf ** 4) - 3.0),
        std_skew=float(np.mean(uf ** 3)),
        raw_q001=float(rqs[0]), raw_q01=float(rqs[1]), raw_q99=float(rqs[2]), raw_q999=float(rqs[3]),
        raw_max=float(np.max(raw_f)), raw_min=float(np.min(raw_f)),
        max_X=float(np.nanmax(X)),
    )

    return dict(
        T=T, M=M, K=K, bands=bands,
        profile_r2=dict(
            log_g0_vs_z=r2_g0, slope_g0_z=slope_g0,
            log_sL2_vs_z=r2_sL, slope_sL2_z=slope_sL,
            lag1_corr_vs_z=r2_g1c,
            log_sL2_vs_level=r2_sL_lev, beta_rough=float(-0.5 * slope_sL_lev),
        ),
        entity_vol=dict(split_half_pearson=vol_split_corr, split_half_spearman=vol_split_spear, s_proxy=s_est),
        tails=tails,
    )


def gaussian_crps(y: np.ndarray, mu: np.ndarray, sigma: np.ndarray) -> np.ndarray:
    """Closed-form exact CRPS for N(mu, sigma^2)."""
    s = np.maximum(sigma, 1e-6)
    z = (y - mu) / s
    return s * (z * (2.0 * norm.cdf(z) - 1.0) + 2.0 * norm.pdf(z) - 1.0 / math.sqrt(math.pi))


def fair_ensemble_crps(y: np.ndarray, ens: np.ndarray) -> np.ndarray:
    """Fair CRPS for ensemble samples ens (N_entities, M_members) vs. truth y (N_entities,)."""
    # E|X - y| - 0.5 * E|X - X'| using sorted order stats (O(M log M), exact!)
    M = ens.shape[1]
    term1 = np.mean(np.abs(ens - y[:, None]), axis=1)
    es = np.sort(ens, axis=1)
    # sum_{i<j} (es_j - es_i) = sum_k (2k - M + 1) es_k
    w = (2.0 * np.arange(M) - M + 1.0)
    term2 = np.sum(es * w[None, :], axis=1) / (M * (M - 1))
    return term1 - term2


def predictability_audit(pn: str, d: dict) -> dict:
    """Stage 1F: Calendar-aligned rolling-origin individual endpoint predictability ceiling."""
    X = d["X"].astype(np.float64)
    R = d["R"].astype(np.float64)
    C = d["C"].astype(np.float64)
    floors = d["floors"].astype(np.float64)
    K = int(d["K"])
    T, M = X.shape

    # Common-factor removed relative log-activity: relative to top-K median each week
    med_K = np.array([np.nanmedian(X[t, np.isfinite(R[t]) & (R[t] <= K)]) for t in range(T)])
    Xr = X - med_K[:, None]
    # Item-count and per-item channels (demeaned per week)
    valid_c = np.isfinite(X) & np.isfinite(C) & (C >= 0)
    N_mat = np.where(valid_c, np.log1p(np.maximum(C, 0.0)), np.nan)
    E_mat = np.where(valid_c, X - N_mat, np.nan)
    E_rel = E_mat - np.nanmedian(E_mat, axis=1, keepdims=True)

    # Arithmetic shares within top-K each week (expm1(X) / sum_topK expm1(X))
    A = np.where(np.isfinite(X), np.expm1(np.clip(X, 0.0, 40.0)), 0.0)
    in_topK_t = np.isfinite(R) & (R <= K)
    tot_K = np.sum(np.where(in_topK_t, A, 0.0), axis=1)
    S_share = A / np.maximum(tot_K[:, None], 1.0)

    # Choose 5 rolling origins spaced cleanly in the back half of the panel
    if T >= 180:
        horizons = [1, 4, 13, 26, 52]
        min_train = 78
        max_h = 52
    elif T >= 80:
        horizons = [1, 4, 13, 26]
        min_train = 36
        max_h = 26
    else:
        horizons = [1, 4, 13]
        min_train = 22
        max_h = 13
    origins = np.unique(np.round(np.linspace(min_train, T - max_h - 1, 5)).astype(int)).tolist()

    # Per-horizon accumulators
    metrics = {str(h): [] for h in horizons}

    for T0 in origins:
        # Train-only permanent rank & top-K membership (zero lookahead!)
        R_tr = R[:T0]
        pen_tr = np.where(np.isfinite(R_tr), R_tr, floors[:T0, None])
        rperm_tr = np.mean(pen_tr, axis=0)
        pres_tr = np.isfinite(R_tr).mean(axis=0)
        order_tr = np.argsort(rperm_tr, kind="stable")
        topK_idx = order_tr[:K]
        if pn == "instagram_hm":
            topK_idx = topK_idx[pres_tr[topK_idx] >= 0.5]

        # Extract train arrays for topK_idx
        Xtr = Xr[:T0, topK_idx]
        Etr = E_rel[:T0, topK_idx]

        # 1. Last observed state at T0 - 1 (or carry-forward last finite value in train)
        last_X = np.full(topK_idx.size, np.nan)
        for t in range(T0):
            f = np.isfinite(Xtr[t])
            last_X[f] = Xtr[t, f]

        # 2. Optimal 1-state EWMA / steady-state local level (alpha tuned strictly inside train!)
        # Grid search alpha on last 13 weeks of train (one-step-ahead / multi-step MAE)
        best_alpha = 0.3
        best_err = np.inf
        val_start = max(10, T0 - 13)
        for alpha in (0.10, 0.20, 0.30, 0.45, 0.60, 0.80):
            lv = Xtr[0].copy()
            # Initialize missing with first available later
            errs = []
            for t in range(1, T0):
                obs = np.isfinite(Xtr[t])
                if t >= val_start:
                    ok = obs & np.isfinite(lv)
                    if ok.any():
                        errs.append(np.mean(np.abs(Xtr[t, ok] - lv[ok])))
                first = obs & ~np.isfinite(lv)
                lv[first] = Xtr[t, first]
                upd = obs & np.isfinite(lv) & ~first
                lv[upd] = (1.0 - alpha) * lv[upd] + alpha * Xtr[t, upd]
            if errs and np.mean(errs) < best_err:
                best_err = float(np.mean(errs))
                best_alpha = alpha

        # Compute full EWMA trajectory up to T0-1 with best_alpha
        lv = np.full(topK_idx.size, np.nan)
        lv_hist = np.full_like(Xtr, np.nan)
        for t in range(T0):
            obs = np.isfinite(Xtr[t])
            first = obs & ~np.isfinite(lv)
            lv[first] = Xtr[t, first]
            upd = obs & np.isfinite(lv) & ~first
            lv[upd] = (1.0 - best_alpha) * lv[upd] + best_alpha * Xtr[t, upd]
            lv_hist[t] = lv

        # 3. Empirical-Bayes entity volatility v_i from train weekly differences (shrunk toward band prior)
        dXtr = np.diff(Xtr, axis=0)
        n_obs_d = np.sum(np.isfinite(dXtr), axis=0)
        var_i = np.nanvar(dXtr, axis=0)
        band_prior_var = float(np.nanmedian(var_i[n_obs_d >= 10]))
        prior_df = 8.0
        eb_var_i = (n_obs_d * np.nan_to_num(var_i, nan=band_prior_var) + prior_df * band_prior_var) / (n_obs_d + prior_df)
        eb_sd_ratio = np.sqrt(eb_var_i / max(band_prior_var, 1e-12))

        # 4. Trailing trend (recent window minus earlier window inside train, e.g. half-year or quarter)
        tw = min(26, max(8, T0 // 3))
        w_tr = min(8, max(4, tw // 3))
        tr_recent = np.nanmean(Xtr[T0 - w_tr:T0], axis=0)
        tr_earlier = np.nanmean(Xtr[T0 - tw - w_tr:T0 - tw], axis=0)
        trend_i = np.nan_to_num(tr_recent - tr_earlier, nan=0.0)
        trend_neg = np.minimum(trend_i, 0.0)
        trend_pos = np.maximum(trend_i, 0.0)
        # Trailing per-item excess E_rel (recent 4w minus train mean -> measures recent viral spike vs. baseline!)
        e_recent = np.nanmean(Etr[max(0, T0 - 4):T0], axis=0)
        e_base = np.nanmean(Etr[:max(1, T0 - 4)], axis=0)
        e_spike = np.nan_to_num(e_recent - e_base, nan=0.0)

        # Fit simple ridge/OLS weights strictly on an internal train split (T_in = T0 - h) for each h!
        for h in horizons:
            target_t = T0 - 1 + h  # STRICT calendar alignment: T0-1 is last train week, +h is h weeks later!
            if target_t >= T:
                continue
            y_true = Xr[target_t, topK_idx]
            ok_eval = np.isfinite(y_true) & np.isfinite(last_X) & np.isfinite(lv)
            if ok_eval.sum() < 200:
                continue

            # Empirical train residual scale at horizon h (for band-only vs. EB-vol CRPS)
            # Use internal train origin T_in = T0 - h
            T_in = T0 - h
            if T_in >= max(16, tw + w_tr + 2):
                y_in = Xr[T0 - 1, topK_idx]
                lv_in = lv_hist[T_in - 1]
                ok_in = np.isfinite(y_in) & np.isfinite(lv_in)
                res_in = y_in[ok_in] - lv_in[ok_in]
                band_sd_h = float(np.std(res_in))
                # Calibrate EB-vol scaling on internal split: optimal exponent gamma in sd_i = band_sd * ratio^gamma
                best_g, best_c = 0.0, np.inf
                r_in = eb_sd_ratio[ok_in]
                for g_exp in (0.0, 0.25, 0.5, 0.75, 1.0):
                    sc = np.mean(gaussian_crps(y_in[ok_in], lv_in[ok_in], band_sd_h * (r_in ** g_exp)))
                    if sc < best_c:
                        best_c, best_g = float(sc), g_exp

                # Internal trend & item-spike features at T_in - 1
                tr_r_in = np.nanmean(Xtr[T_in - w_tr:T_in], axis=0)
                tr_e_in = np.nanmean(Xtr[T_in - tw - w_tr:T_in - tw], axis=0)
                tr_in = np.nan_to_num(tr_r_in - tr_e_in, nan=0.0)
                e_r_in = np.nanmean(Etr[max(0, T_in - 4):T_in], axis=0)
                e_b_in = np.nanmean(Etr[:max(1, T_in - 4)], axis=0)
                es_in = np.nan_to_num(e_r_in - e_b_in, nan=0.0)

                # Fit symmetric trend, asymmetric trend, and item-spike OLS with tiny ridge (strictly inside train!)
                def fit_lin(feats_in):
                    A_f = np.column_stack([np.ones(ok_in.sum())] + [f[ok_in] for f in feats_in])
                    reg = np.diag([0.0] + [10.0] * len(feats_in))
                    return np.linalg.solve(A_f.T @ A_f + reg, A_f.T @ res_in)

                c_sym = fit_lin([tr_in])
                c_asym = fit_lin([np.minimum(tr_in, 0.0), np.maximum(tr_in, 0.0)])
                c_items = fit_lin([es_in])
                c_both = fit_lin([np.minimum(tr_in, 0.0), np.maximum(tr_in, 0.0), es_in])
            else:
                band_sd_h = float(np.nanstd(Xr[h:T0, topK_idx] - Xr[:T0 - h, topK_idx]))
                best_g = 0.5
                c_sym = np.zeros(2)
                c_asym = np.zeros(3)
                c_items = np.zeros(2)
                c_both = np.zeros(4)

            # Predictions at T0 - 1 -> T0 - 1 + h
            p_no = last_X[ok_eval]
            p_lv = lv[ok_eval]
            p_sym = p_lv + c_sym[0] + c_sym[1] * trend_i[ok_eval]
            p_asym = p_lv + c_asym[0] + c_asym[1] * trend_neg[ok_eval] + c_asym[2] * trend_pos[ok_eval]
            p_items = p_lv + c_items[0] + c_items[1] * e_spike[ok_eval]
            p_both = p_lv + c_both[0] + c_both[1] * trend_neg[ok_eval] + c_both[2] * trend_pos[ok_eval] + c_both[3] * e_spike[ok_eval]
            yt = y_true[ok_eval]

            # Scale predictions
            sd_const = np.full_like(yt, band_sd_h)
            sd_eb = band_sd_h * (eb_sd_ratio[ok_eval] ** best_g)

            # Move magnitude predictability: Corr(|y_true - lv|, eb_sd_ratio)
            abs_err_lv = np.abs(yt - p_lv)
            vol_corr = float(spearmanr(abs_err_lv, eb_sd_ratio[ok_eval]).correlation)

            # Implied audience-share shifts (for top-K entities observed at both T0-1 and target_t)
            # Softmax of exp(X_hat) over evaluated top-K entities vs. actual shares
            s_now = S_share[T0 - 1, topK_idx[ok_eval]]
            s_true = S_share[target_t, topK_idx[ok_eval]]
            ds_true = s_true - s_now
            def pred_share_shift(x_hat):
                # Convert predicted relative log-activity back to share within top-K
                a_hat = np.exp(np.clip(x_hat - np.max(x_hat), -30.0, 0.0))
                s_hat = a_hat / np.sum(a_hat)
                # Normalize scale to match sum(s_true) ~ 1
                return s_hat * np.sum(s_true) - s_now

            ds_no = np.zeros_like(ds_true)
            ds_lv = pred_share_shift(p_lv)
            ds_both = pred_share_shift(p_both)

            # Coarsened log-rank CRPS (comparing point/Gaussian-implied rank samples against rank_bin kernel)
            r_true = R[target_t, topK_idx]
            logr_true = np.log(np.where(np.isfinite(r_true) & (r_true <= K), r_true, K + 1.0))
            r_now = R[T0 - 1, topK_idx]
            logr_now = np.log(np.where(np.isfinite(r_now) & (r_now <= K), r_now, K + 1.0))
            # Rank-bin empirical kernel from train window (20 log-rank bins)
            rng_rb = np.random.default_rng(1000 + T0 * 100 + h)
            n_ens = 20
            bin_edges = np.linspace(0.0, np.log(K + 1.0) + 1e-6, 21)
            R_tr_sub = R[:T0 - h, topK_idx]
            R_tr_fut = R[h:T0, topK_idx]
            lr_tr_cur = np.log(np.where(np.isfinite(R_tr_sub) & (R_tr_sub <= K), R_tr_sub, K + 1.0))
            lr_tr_fut = np.log(np.where(np.isfinite(R_tr_fut) & (R_tr_fut <= K), R_tr_fut, K + 1.0))
            rb_ens = np.empty((topK_idx.size, n_ens))
            cur_bin = np.clip(np.searchsorted(bin_edges, logr_now, side="right") - 1, 0, 19)
            tr_bin = np.clip(np.searchsorted(bin_edges, lr_tr_cur, side="right") - 1, 0, 19)
            for b in range(20):
                who = np.flatnonzero(cur_bin == b)
                if not who.size:
                    continue
                pool = lr_tr_fut[tr_bin == b]
                if pool.size < 20:
                    rb_ens[who] = logr_now[who, None]
                else:
                    rb_ens[who] = rng_rb.choice(pool, size=(who.size, n_ens), replace=True)
            crps_logr_no = float(np.mean(np.abs(logr_true - logr_now)))
            crps_logr_rb = float(np.mean(fair_ensemble_crps(logr_true, rb_ens)))

            # Quick local-level Gaussian ensemble in level space -> re-rank -> coarsened log-rank CRPS
            # (gives us a clean reference for how much separating level + EB vol improves log-rank CRPS!)
            lv_all = np.where(np.isfinite(lv), lv, np.nanmin(lv) - 1.0)
            sd_eb_all = band_sd_h * (eb_sd_ratio ** best_g)
            ens_X = lv_all[:, None] + sd_eb_all[:, None] * rng_rb.standard_normal((topK_idx.size, n_ens))
            ens_lr = np.empty_like(ens_X)
            for m in range(n_ens):
                ord_m = np.argsort(-ens_X[:, m])
                rk_m = np.empty(topK_idx.size, dtype=float)
                rk_m[ord_m] = np.arange(1, topK_idx.size + 1)
                ens_lr[:, m] = np.log(np.minimum(rk_m, K + 1.0))
            crps_logr_lv_eb = float(np.mean(fair_ensemble_crps(logr_true, ens_lr)))

            # Top-K/16 retention Brier score
            k_head = K // 16
            y_head = (np.isfinite(r_true) & (r_true <= k_head)).astype(float)
            p_head_no = (np.isfinite(r_now) & (r_now <= k_head)).astype(float)
            p_head_rb = np.mean(rb_ens <= np.log(k_head + 0.5), axis=1)
            p_head_lv = np.mean(ens_lr <= np.log(k_head + 0.5), axis=1)
            # Restrict Brier to entities in top K/4 at origin (where the event is non-trivial)
            near_head = np.isfinite(r_now) & (r_now <= K // 4)
            brier_no = float(np.mean((y_head[near_head] - p_head_no[near_head]) ** 2))
            brier_rb = float(np.mean((y_head[near_head] - p_head_rb[near_head]) ** 2))
            brier_lv = float(np.mean((y_head[near_head] - p_head_lv[near_head]) ** 2))

            metrics[str(h)].append(dict(
                T0=T0, alpha=best_alpha, eb_gamma=best_g, n_eval=int(ok_eval.sum()),
                mae_no=float(np.mean(np.abs(yt - p_no))),
                mae_lv=float(np.mean(np.abs(yt - p_lv))),
                mae_sym=float(np.mean(np.abs(yt - p_sym))),
                mae_asym=float(np.mean(np.abs(yt - p_asym))),
                mae_items=float(np.mean(np.abs(yt - p_items))),
                mae_both=float(np.mean(np.abs(yt - p_both))),
                crps_no_const=float(np.mean(gaussian_crps(yt, p_no, sd_const))),
                crps_lv_const=float(np.mean(gaussian_crps(yt, p_lv, sd_const))),
                crps_lv_eb=float(np.mean(gaussian_crps(yt, p_lv, sd_eb))),
                crps_asym_eb=float(np.mean(gaussian_crps(yt, p_asym, sd_eb))),
                crps_both_eb=float(np.mean(gaussian_crps(yt, p_both, sd_eb))),
                vol_spearman=vol_corr,
                share_mae_no=float(np.mean(np.abs(ds_true - ds_no))),
                share_mae_lv=float(np.mean(np.abs(ds_true - ds_lv))),
                share_mae_both=float(np.mean(np.abs(ds_true - ds_both))),
                share_corr_lv=float(np.corrcoef(ds_true, ds_lv)[0, 1]),
                share_corr_both=float(np.corrcoef(ds_true, ds_both)[0, 1]),
                crps_logr_no=crps_logr_no,
                crps_logr_rb=crps_logr_rb,
                crps_logr_lv_eb=crps_logr_lv_eb,
                brier_head_no=brier_no,
                brier_head_rb=brier_rb,
                brier_head_lv=brier_lv,
                coef_asym=c_asym.tolist(),
                coef_items=c_items.tolist(),
            ))

    # Aggregate over origins
    summary = {}
    for h_s, rows in metrics.items():
        if not rows:
            continue
        keys = [k for k in rows[0] if k not in ("T0", "coef_asym", "coef_items")]
        summary[h_s] = {k: float(np.mean([r[k] for r in rows])) for k in keys}
        summary[h_s]["n_origins"] = len(rows)
        summary[h_s]["mean_coef_asym"] = np.mean([r["coef_asym"] for r in rows], axis=0).tolist()
        summary[h_s]["mean_coef_items"] = np.mean([r["coef_items"] for r in rows], axis=0).tolist()
    return dict(origins=origins, by_horizon=summary)


def main():
    out = {}
    for pn in PANELS:
        print(f"\n==================== {pn} ====================", flush=True)
        d = load_cached(pn)
        pa = parsimony_audit(pn, d)
        pr = predictability_audit(pn, d)
        out[pn] = dict(parsimony=pa, predictability=pr)

        r2 = pa["profile_r2"]
        ev = pa["entity_vol"]
        tl = pa["tails"]
        print(f"  1E Profile R^2: log_g0~z={r2['log_g0_vs_z']:.3f}, log_sL2~z={r2['log_sL2_vs_z']:.3f}, "
              f"log_sL2~L={r2['log_sL2_vs_level']:.3f} (rough beta={r2['beta_rough']:+.3f}), lag1~z={r2['lag1_corr_vs_z']:.3f}", flush=True)
        print(f"  1E Entity vol: split-half r={ev['split_half_pearson']:.3f} (spear={ev['split_half_spearman']:.3f}), s_proxy={ev['s_proxy']:.3f}", flush=True)
        print(f"  1E Tails: std [q01={tl['std_q01']:.2f}, q99={tl['std_q99']:.2f}, q999={tl['std_q999']:.2f}], "
              f"kurt={tl['std_kurt']:.2f}, skew={tl['std_skew']:+.2f}, raw_max={tl['raw_max']:.2f}, max_X={tl['max_X']:.2f}", flush=True)
        print(f"  1E Absence return (within 4w/13w, head vs body): "
              f"band0={pa['bands'][0].get('return_within_4w')}/{pa['bands'][0].get('return_within_13w')}, "
              f"band5={pa['bands'][5].get('return_within_4w')}/{pa['bands'][5].get('return_within_13w')}, "
              f"band6={pa['bands'][6].get('return_within_4w')}/{pa['bands'][6].get('return_within_13w')}", flush=True)

        print("  1F Calendar-aligned endpoint predictability (mean over 5 origins):", flush=True)
        for h_s, s in pr["by_horizon"].items():
            print(f"    h={int(h_s):2d}w | MAE: no={s['mae_no']:.3f} lv={s['mae_lv']:.3f} sym={s['mae_sym']:.3f} "
                  f"asym={s['mae_asym']:.3f} itm={s['mae_items']:.3f} both={s['mae_both']:.3f} | "
                  f"CRPS(X): const={s['crps_lv_const']:.3f} eb={s['crps_lv_eb']:.3f} both_eb={s['crps_both_eb']:.3f} "
                  f"(vol_r={s['vol_spearman']:.3f}) | "
                  f"logR CRPS: no={s['crps_logr_no']:.3f} rb={s['crps_logr_rb']:.3f} lv_eb={s['crps_logr_lv_eb']:.3f} | "
                  f"share_r={s['share_corr_lv']:.3f}->{s['share_corr_both']:.3f}", flush=True)

    out_path = OUT_DIR / "stage1_parsimony_predict_results.json"
    out_path.write_text(json.dumps(out, indent=2))
    print(f"\nSaved {out_path}", flush=True)


if __name__ == "__main__":
    main()
