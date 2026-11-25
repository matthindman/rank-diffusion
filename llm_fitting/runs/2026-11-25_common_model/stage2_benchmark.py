#!/usr/bin/env python3
"""Stage 2 Full Benchmark of the 12-scalar Common Rank-Diffusion Model across all
four DEVELOPMENT ladders (facebook_a, instagram_hm, reddit_comments_ext,
reddit_submissions_long) on all three goals:

  Goal 1 (Aggregate Structure, 20 seeds):
    - Turnover at top-10, K/64, K/16, K/4, K/2
    - Rank-1 and rank-10 weekly occupant change
    - First-block & last-block ladder span at K/64, K/16, K/4 above K
    - Arithmetic head share (mean, median, 5-95% band, max simulated X -> zero blowups!)
    - Crossing ratio (measured / predicted c = sigma^2 / 2S)
    - Established-departure rate & crossing fraction (cohort permanent rank <= K/2)
    - Gapped persistence & asymmetry D (expected to fail in Stage 2 -> Stage 3 baseline)

  Goal 2 (Individual Endpoint Movement & Share Shifts, 5 rolling origins, strictly
  calendar-aligned h in {1, 4, 13, 26, 52}, 20 ensemble members):
    - Relative log-level CRPS & MAE
    - Coarsened log-rank fair CRPS (vs no_change, rank_bin, common_no_eb ablation)
    - Arithmetic audience-share shift MAE & correlation (vs no_change)
    - Top-K/16 and top-K retention Brier score (vs no_change, rank_bin)
    - 90% predictive interval coverage (calibration)
    - Pooled Gate v1 displacement relative error vs. historical-mobility baseline

  Goal 3 (Parsimony):
    - 10 scalars (Spec-B pinned FB) / 12 scalars (IG, Reddit) vs ~500 in Model A / RDM.

Runs strictly inside the sandbox from the lean scratch cache (DEVELOPMENT data only).
"""
from __future__ import annotations

import gc
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

PANELS = {
    "facebook_a": dict(block=26, pres=0.9, W=4, H=26, gap=4, step=4, horizons=[1, 4, 13, 26], spec_b=True),
    "instagram_hm": dict(block=13, pres=0.75, W=4, H=16, gap=4, step=4, horizons=[1, 4, 13], spec_b=False),
    "reddit_comments_ext": dict(block=52, pres=0.9, W=13, H=52, gap=13, step=13, horizons=[1, 4, 13, 26, 52], spec_b=False),
    "reddit_submissions_long": dict(block=52, pres=0.9, W=13, H=52, gap=13, step=13, horizons=[1, 4, 13, 26, 52], spec_b=False),
}


def load_cached(panel: str) -> tuple[dict, tuple[np.ndarray, np.ndarray] | None]:
    d = np.load(CACHE_DIR / f"{panel}.npz", allow_pickle=True)
    data = {k: d[k] for k in d.files}
    sb = None
    sb_path = CACHE_DIR / f"{panel}_spec_b.json"
    if sb_path.exists():
        js = json.loads(sb_path.read_text())
        sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
    return data, sb


def wmean(X: np.ndarray, lo: int, hi: int, pres: float) -> np.ndarray:
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(axis=0) >= pres
    with np.errstate(all="ignore"):
        m = np.nanmean(seg, axis=0)
    return np.where(ok, m, np.nan)


def _order_desc(v: np.ndarray) -> np.ndarray:
    f = np.flatnonzero(np.isfinite(v))
    return f[np.argsort(-v[f], kind="stable")]


def fair_ensemble_crps(y: np.ndarray, ens: np.ndarray) -> np.ndarray:
    M = ens.shape[1]
    term1 = np.mean(np.abs(ens - y[:, None]), axis=1)
    es = np.sort(ens, axis=1)
    w = (2.0 * np.arange(M) - M + 1.0)
    term2 = np.sum(es * w[None, :], axis=1) / (M * (M - 1))
    return term1 - term2


def structure_stats(X: np.ndarray, R: np.ndarray, K: int, cfg: dict, ids: np.ndarray | None = None) -> dict:
    """Identical aggregate-structure & dynamics evaluator for BOTH empirical and simulated panels."""
    T, N = X.shape
    block = cfg["block"]
    pres = cfg["pres"]
    nb = T // block
    start = T - nb * block
    M_blk = np.array([wmean(X, start + b * block, start + (b + 1) * block, pres) for b in range(nb)])
    tops = [_order_desc(M_blk[b]) for b in range(nb)]

    # 1. Turnover across scales
    ks_turn = [10, K // 64, K // 16, K // 4, K // 2]
    turnover = {}
    for k in ks_turn:
        vals = [1.0 - np.intersect1d(tops[b][:k], tops[b + 1][:k]).size / k
                for b in range(nb - 1) if tops[b].size >= k and tops[b + 1].size >= k]
        turnover[str(k)] = float(np.mean(vals)) if vals else np.nan

    # 2. Rank-1 & Rank-10 weekly occupant change
    occ = {}
    for r_target in (1, 10):
        o = np.full(T, -1, dtype=np.int64)
        for t in range(T):
            w = np.flatnonzero(R[t] == r_target)
            if w.size:
                o[t] = ids[t, w[0]] if ids is not None else w[0]
        ok = (o[1:] >= 0) & (o[:-1] >= 0)
        occ[str(r_target)] = float(np.mean(o[1:][ok] != o[:-1][ok])) if ok.any() else np.nan

    # 3. Ladder span above rank K (first & last block)
    lad_first, lad_last = {}, {}
    for k in (1, K // 64, K // 16, K // 4):
        if tops[0].size >= K and tops[-1].size >= K:
            lad_first[str(k)] = float(M_blk[0, tops[0][k - 1]] - M_blk[0, tops[0][K - 1]])
            lad_last[str(k)] = float(M_blk[-1, tops[-1][k - 1]] - M_blk[-1, tops[-1][K - 1]])
    lad_change = {k: lad_last[k] - lad_first[k] for k in lad_first}

    # 4. Arithmetic head share (top K/64 of top K in last block) + max X check
    k64 = K // 64
    lo, hi = start + (nb - 1) * block, start + nb * block
    A_blk = np.where(np.isfinite(X[lo:hi]), np.expm1(np.clip(X[lo:hi], 0.0, 45.0)), 0.0)
    if tops[-1].size >= K:
        head_share = float(A_blk[:, tops[-1][:k64]].sum() / max(A_blk[:, tops[-1][:K]].sum(), 1.0))
    else:
        head_share = np.nan
    max_X = float(np.nanmax(X))

    # 5. Crossing ratio (measured vs predicted c = sigma_L^2 / (2 S)) over K/16..K
    #    Uses half-block (or 13w/26w) persistent set matching mts_boundary_law logic cleanly
    b_cr = 26 if T >= 160 else (13 if T >= 70 else 7)
    nb_c = T // b_cr
    st_c = T - nb_c * b_cr
    Mc = np.array([wmean(X, st_c + b * b_cr, st_c + (b + 1) * b_cr, min(pres, 0.75)) for b in range(nb_c)])
    # Compare blocks spaced by 2*b_cr (1 year on Reddit, 26w on FB, 14w on IG)
    sp_c = 2 if nb_c >= 4 else 1
    ratios = []
    for a in range(nb_c - sp_c):
        La, Lb = Mc[a], Mc[a + sp_c]
        pers = np.isfinite(La) & np.isfinite(Lb)
        if pers.sum() < K:
            continue
        ia = np.flatnonzero(pers)
        oa = ia[np.argsort(-La[ia], kind="stable")]
        ob = ia[np.argsort(-Lb[ia], kind="stable")]
        dL = Lb[oa] - La[oa]
        dL = dL - np.mean(dL[:K])
        for k in (K // 16, K // 8, K // 4, K // 2, K):
            if k >= oa.size:
                continue
            # Measured crossing gain per member of top k: mean(Lb[top_k_b]) - mean(Lb[top_k_a])
            c_obs = float(np.mean(Lb[ob[:k]]) - np.mean(Lb[oa[:k]]))
            # Local slope S_k around rank k
            lo_r, hi_r = max(1, int(k / 1.4)), min(oa.size, int(k * 1.4))
            S_k = float(-np.polyfit(np.log(np.arange(lo_r, hi_r + 1)), La[oa[lo_r - 1:hi_r]], 1)[0])
            # Local lasting variance near rank k (from variance of dL in [lo_r, hi_r])
            sig2_dt = float(np.var(dL[lo_r - 1:hi_r]))
            c_pred = sig2_dt / max(2.0 * S_k, 1e-6)
            if c_obs > 0 and c_pred > 0:
                ratios.append(c_obs / c_pred)
    boundary_ratio = float(np.exp(np.mean(np.log(ratios)))) if ratios else np.nan

    # 6. Established-departure rate & crossing share (train-half permanent rank <= K/2, scored on back half!)
    #    Matches §2z-ac / §2z-au: weekly exits from top-K among cohort with first-half perm-rank <= K/2
    mid = T // 2
    floors = np.sum(np.isfinite(R), axis=1).astype(float) + 1.0
    pen1 = np.where(np.isfinite(R[:mid]), R[:mid], floors[:mid, None])
    rperm1 = np.mean(pen1, axis=0)
    ord1 = np.empty(N, dtype=float)
    ord1[np.argsort(rperm1, kind="stable")] = np.arange(1, N + 1)
    est_coh = ord1 <= (K // 2)
    # Track identities on back half [mid..T-1]
    R2 = R[mid:, est_coh]
    if ids is not None:
        id_ref = ids[mid - 1, est_coh]
        alive2 = ids[mid:, est_coh] == id_ref[None, :]
        inK2 = alive2 & np.isfinite(R2) & (R2 <= K)
        still_obs_below = alive2 & np.isfinite(R2) & (R2 > K)
        # An exit event: in top-K at t, NOT in top-K at t+1
        ex_ev = inK2[:-1] & (~inK2[1:])
        at_risk = inK2[:-1]
        dep_rate = float(np.sum(ex_ev) / max(np.sum(at_risk), 1))
        # Crossing = still observed below K (or later observed alive again!)
        ever_after = np.zeros_like(inK2)
        seen = np.zeros(inK2.shape[1], dtype=bool)
        for tt in range(inK2.shape[0] - 1, -1, -1):
            seen = seen | (alive2[tt] & np.isfinite(R2[tt]))
            ever_after[tt] = seen
        crossing_ev = ex_ev & (still_obs_below[1:] | ever_after[1:])
        dep_cross_frac = float(np.sum(crossing_ev) / max(np.sum(ex_ev), 1)) if np.any(ex_ev) else np.nan
    else:
        inK2 = np.isfinite(R2) & (R2 <= K)
        still_obs_below = np.isfinite(R2) & (R2 > K)
        ex_ev = inK2[:-1] & (~inK2[1:])
        at_risk = inK2[:-1]
        dep_rate = float(np.sum(ex_ev) / max(np.sum(at_risk), 1))
        ever_after = np.zeros_like(inK2)
        seen = np.zeros(inK2.shape[1], dtype=bool)
        for tt in range(inK2.shape[0] - 1, -1, -1):
            seen = seen | np.isfinite(R2[tt])
            ever_after[tt] = seen
        crossing_ev = ex_ev & (still_obs_below[1:] | ever_after[1:])
        dep_cross_frac = float(np.sum(crossing_ev) / max(np.sum(ex_ev), 1)) if np.any(ex_ev) else np.nan

    # 7. Gapped persistence & asymmetry D (identical to Stage 1)
    W, H, gap, step = cfg["W"], cfg["H"], cfg["gap"], cfg["step"]
    pen_all = np.where(np.isfinite(R), R, floors[:, None])
    pr_all = np.mean(pen_all, axis=0)
    o_pr = np.argsort(pr_all, kind="stable")
    lab = np.full(N, -1, dtype=np.int32)
    lab[o_pr[:K]] = 2
    lab[o_pr[:K // 4]] = 1
    lab[o_pr[:K // 16]] = 0
    rhos, Ds = [], []
    a = 0
    while a + 2 * (H + W) + gap <= T:
        x = wmean(X, a + H, a + H + W, pres) - wmean(X, a, a + W, pres)
        b0 = a + H + W + gap
        y = wmean(X, b0 + H, b0 + H + W, pres) - wmean(X, b0, b0 + W, pres)
        ok = (lab >= 0) & np.isfinite(x) & np.isfinite(y)
        if ids is not None:
            ok = ok & (ids[a] == ids[b0 + H + W - 1])
        xd, yd = np.full_like(x, np.nan), np.full_like(y, np.nan)
        for j in range(3):
            s = ok & (lab == j)
            if s.sum() > 30:
                xd[s] = x[s] - np.mean(x[s])
                yd[s] = y[s] - np.mean(y[s])
        ok2 = np.isfinite(xd) & np.isfinite(yd)
        if ok2.sum() >= 100:
            xo, yo = xd[ok2], yd[ok2]
            rhos.append(float(spearmanr(xo, yo).correlation))
            ql, qh = np.quantile(xo, [0.1, 0.9])
            lo_m, hi_m = xo <= ql, xo >= qh
            Ds.append(float(np.mean(yo[lo_m]) / np.mean(xo[lo_m]) - np.mean(yo[hi_m]) / np.mean(xo[hi_m])))
        a += step

    return dict(
        turnover=turnover,
        occ_change=occ,
        lad_first=lad_first,
        lad_change=lad_change,
        head_share=head_share,
        max_X=max_X,
        boundary_ratio=boundary_ratio,
        dep_rate=dep_rate,
        dep_cross_frac=dep_cross_frac,
        gapped_rho=float(np.mean(rhos)) if rhos else np.nan,
        D=float(np.mean(Ds)) if Ds else np.nan,
    )


def _gate_moments_from_ranks(R_mat: np.ndarray, cohort_cols: np.ndarray, horizons: list[int], K: int) -> dict:
    """Compute Gate v1 pooled displacement moments (dRank1, 4, 13, RACF1, coll1, 5, 20)
    from a weekly rank trajectory matrix R_mat (T_test x N)."""
    T_te = R_mat.shape[0]
    cr = R_mat[:, cohort_cols]
    out = {}
    for h in horizons:
        if h < T_te:
            d = np.abs(cr[h:] - cr[:-h])
            f = d[np.isfinite(d)]
            out[f"dRank{h}"] = float(np.median(f)) if f.size else np.nan
    # RACF1: lag-1 Spearman rank autocorrelation within cohort (matches rankdiff_kalman.py!)
    if T_te >= 2:
        a, b = cr[:-1], cr[1:]
        ok = np.isfinite(a) & np.isfinite(b)
        if ok.sum() > 50:
            out["RACF1"] = float(spearmanr(a[ok], b[ok]).correlation)
        else:
            out["RACF1"] = np.nan
    # Collisions: fraction of weeks where rank c occupant changes
    for c in (1, 5, 20):
        occ = np.full(T_te, -1, dtype=np.int64)
        for t in range(T_te):
            w = np.flatnonzero(R_mat[t] == c)
            if w.size:
                occ[t] = w[0]
        ok = (occ[1:] >= 0) & (occ[:-1] >= 0)
        out[f"coll{c}"] = float(np.mean(occ[1:][ok] != occ[:-1][ok])) if ok.any() else np.nan
    return out


def oos_endpoint_and_gate(pn: str, X: np.ndarray, R: np.ndarray, floors: np.ndarray, K: int,
                          cfg: dict, spec_b: tuple[np.ndarray, np.ndarray] | None, n_ens: int = 20) -> dict:
    """Goal 2: 5 rolling origins, strictly train-only estimation & filtering, strictly calendar-aligned."""
    T, N = X.shape
    horizons = cfg["horizons"]
    max_h = max(horizons)
    min_train = 78 if T >= 180 else (36 if T >= 80 else 22)
    origins = np.unique(np.round(np.linspace(min_train, T - max_h - 1, 5)).astype(int)).tolist()

    # Common weekly median of top-K for relative level evaluation
    med_K = np.array([np.nanmedian(X[t, np.isfinite(R[t]) & (R[t] <= K)]) for t in range(T)])
    Xr = X - med_K[:, None]
    # Arithmetic shares within top-K
    A = np.where(np.isfinite(X), np.expm1(np.clip(X, 0.0, 40.0)), 0.0)
    tot_K = np.sum(np.where(np.isfinite(R) & (R <= K), A, 0.0), axis=1)
    S_share = A / np.maximum(tot_K[:, None], 1.0)

    by_h = {str(h): [] for h in horizons}
    gate_splits = []

    for split_i, T0 in enumerate(origins):
        X_tr = X[:T0]
        R_tr = R[:T0]
        fl_tr = floors[:T0]

        # Estimate Common model strictly on train window [0..T0-1]!
        p_tr = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=spec_b, refine_drift=True)
        filt = cr.filter_endpoints(X_tr, R_tr, p_tr, floors_tr=fl_tr)

        # Train-only permanent rank -> top-K scored cohort
        pen_tr = np.where(np.isfinite(R_tr), R_tr, fl_tr[:, None])
        rperm_tr = np.mean(pen_tr, axis=0)
        pres_tr = np.isfinite(R_tr).mean(axis=0)
        ord_tr = np.argsort(rperm_tr, kind="stable")
        topK = ord_tr[:K]
        if pn == "instagram_hm":
            topK = topK[pres_tr[topK] >= 0.5]

        # Calendar-aligned ensemble forecasts: full (with EB v_i) vs ablation (no EB v_i)
        fc_full = cr.forecast_endpoints(filt, p_tr, horizons=horizons, n_ens=n_ens, seed=100 * split_i,
                                        apply_absence=True, use_eb_vol=True)
        fc_noeb = cr.forecast_endpoints(filt, p_tr, horizons=horizons, n_ens=n_ens, seed=100 * split_i,
                                        apply_absence=True, use_eb_vol=False)

        # Last observed X and R at T0 - 1
        last_Xr = np.full(N, np.nan)
        for t in range(T0):
            f = np.isfinite(Xr[t])
            last_Xr[f] = Xr[t, f]
        r_now = R[T0 - 1]
        logr_now = np.log(np.where(np.isfinite(r_now) & (r_now <= K), r_now, K + 1.0))

        for h in horizons:
            t_fut = T0 - 1 + h
            if t_fut >= T:
                continue

            # Empirical truth at t_fut
            yt_X = Xr[t_fut, topK]
            rt = R[t_fut, topK]
            logr_true = np.log(np.where(np.isfinite(rt) & (rt <= K), rt, K + 1.0))

            # Rank-bin empirical transition baseline from train window (20 log-rank bins)
            rng_rb = np.random.default_rng(5000 + split_i * 100 + h)
            bin_edges = np.linspace(0.0, np.log(K + 1.0) + 1e-6, 21)
            lr_tr_cur = np.log(np.where(np.isfinite(R_tr[:-h, topK]) & (R_tr[:-h, topK] <= K), R_tr[:-h, topK], K + 1.0))
            lr_tr_fut = np.log(np.where(np.isfinite(R_tr[h:, topK]) & (R_tr[h:, topK] <= K), R_tr[h:, topK], K + 1.0))
            rb_ens = np.empty((topK.size, n_ens))
            cbin = np.clip(np.searchsorted(bin_edges, logr_now[topK], side="right") - 1, 0, 19)
            tbin = np.clip(np.searchsorted(bin_edges, lr_tr_cur, side="right") - 1, 0, 19)
            for b in range(20):
                who = np.flatnonzero(cbin == b)
                if not who.size:
                    continue
                pool = lr_tr_fut[tbin == b]
                rb_ens[who] = logr_now[topK[who], None] if pool.size < 20 else rng_rb.choice(pool, size=(who.size, n_ens), replace=True)

            # Extract Common model ensembles (centered per member by member's top-K median!)
            def score_model(fc_pair):
                Xe_all, Re_all = fc_pair
                # Member-wise top-K median centering for relative level Xr
                Xe_rel = np.empty((topK.size, n_ens))
                S_ens = np.empty((topK.size, n_ens))
                for m in range(n_ens):
                    xm = Xe_all[:, m]
                    rm = Re_all[:, m]
                    inKm = np.isfinite(rm) & (rm <= K) & np.isfinite(xm)
                    med_m = float(np.nanmedian(xm[inKm])) if inKm.any() else float(np.nanmedian(xm))
                    Xe_rel[:, m] = xm[topK] - med_m
                    am = np.where(np.isfinite(xm), np.expm1(np.clip(xm, 0.0, 40.0)), 0.0)
                    tot_m = float(np.sum(am[inKm])) if inKm.any() else max(float(np.sum(am)), 1.0)
                    S_ens[:, m] = am[topK] / max(tot_m, 1.0)
                Re_top = Re_all[topK]
                lre = np.log(np.where(np.isfinite(Re_top) & (Re_top <= K), Re_top, K + 1.0))

                # Level CRPS & MAE & 90% coverage (on entities observed at t_fut and finite across ensemble)
                ok_x = np.isfinite(yt_X) & np.all(np.isfinite(Xe_rel), axis=1)
                if ok_x.sum() < 100:
                    # Fill occasional absent ensemble draws with entity median for level scoring
                    med_e = np.nanmedian(Xe_rel, axis=1, keepdims=True)
                    Xe_fill = np.where(np.isfinite(Xe_rel), Xe_rel, med_e)
                    ok_x = np.isfinite(yt_X) & np.all(np.isfinite(Xe_fill), axis=1)
                else:
                    Xe_fill = Xe_rel
                crps_X = float(np.mean(fair_ensemble_crps(yt_X[ok_x], Xe_fill[ok_x])))
                mae_X = float(np.mean(np.abs(yt_X[ok_x] - np.median(Xe_fill[ok_x], axis=1))))
                q05 = np.quantile(Xe_fill[ok_x], 0.05, axis=1)
                q95 = np.quantile(Xe_fill[ok_x], 0.95, axis=1)
                cov90 = float(np.mean((yt_X[ok_x] >= q05) & (yt_X[ok_x] <= q95)))

                # Coarsened log-rank CRPS (ALL topK entities — absence = K+1!)
                crps_lr = float(np.mean(fair_ensemble_crps(logr_true, lre)))

                # Arithmetic share shift MAE & correlation
                s_now = S_share[T0 - 1, topK]
                s_true = S_share[t_fut, topK]
                ds_true = s_true - s_now
                ds_hat = np.median(S_ens, axis=1) - s_now
                share_mae = float(np.mean(np.abs(ds_true - ds_hat)))
                share_corr = float(np.corrcoef(ds_true, ds_hat)[0, 1]) if np.std(ds_hat) > 1e-12 else 0.0

                # Top-K/16 and top-K retention Brier score
                k_hd = K // 16
                near_hd = np.isfinite(r_now[topK]) & (r_now[topK] <= K // 4)
                y_hd = (np.isfinite(rt) & (rt <= k_hd)).astype(float)
                p_hd = np.mean(lre <= np.log(k_hd + 0.5), axis=1)
                brier_hd = float(np.mean((y_hd[near_hd] - p_hd[near_hd]) ** 2))

                y_inK = (np.isfinite(rt) & (rt <= K)).astype(float)
                p_inK = np.mean(lre <= np.log(K + 0.5), axis=1)
                brier_K = float(np.mean((y_inK - p_inK) ** 2))

                return dict(crps_X=crps_X, mae_X=mae_X, cov90=cov90, crps_lr=crps_lr,
                            share_mae=share_mae, share_corr=share_corr,
                            brier_head=brier_hd, brier_K=brier_K)

            m_full = score_model(fc_full[h])
            m_noeb = score_model(fc_noeb[h])

            # Baselines
            ok_no = np.isfinite(yt_X) & np.isfinite(last_Xr[topK])
            mae_no = float(np.mean(np.abs(yt_X[ok_no] - last_Xr[topK[ok_no]])))
            crps_lr_no = float(np.mean(np.abs(logr_true - logr_now[topK])))
            crps_lr_rb = float(np.mean(fair_ensemble_crps(logr_true, rb_ens)))
            s_now = S_share[T0 - 1, topK]
            s_true = S_share[t_fut, topK]
            share_mae_no = float(np.mean(np.abs(s_true - s_now)))
            k_hd = K // 16
            near_hd = np.isfinite(r_now[topK]) & (r_now[topK] <= K // 4)
            y_hd = (np.isfinite(rt) & (rt <= k_hd)).astype(float)
            brier_hd_no = float(np.mean((y_hd[near_hd] - (r_now[topK[near_hd]] <= k_hd).astype(float)) ** 2))
            brier_hd_rb = float(np.mean((y_hd[near_hd] - np.mean(rb_ens[near_hd] <= np.log(k_hd + 0.5), axis=1)) ** 2))
            y_inK = (np.isfinite(rt) & (rt <= K)).astype(float)
            brier_K_rb = float(np.mean((y_inK - np.mean(rb_ens <= np.log(K + 0.5), axis=1)) ** 2))

            by_h[str(h)].append(dict(
                T0=T0,
                mae_no=mae_no, mae_full=m_full["mae_X"], mae_noeb=m_noeb["mae_X"],
                crps_X_full=m_full["crps_X"], crps_X_noeb=m_noeb["crps_X"], cov90_full=m_full["cov90"],
                crps_lr_no=crps_lr_no, crps_lr_rb=crps_lr_rb,
                crps_lr_full=m_full["crps_lr"], crps_lr_noeb=m_noeb["crps_lr"],
                share_mae_no=share_mae_no, share_mae_full=m_full["share_mae"],
                share_corr_full=m_full["share_corr"],
                brier_hd_no=brier_hd_no, brier_hd_rb=brier_hd_rb, brier_hd_full=m_full["brier_head"],
                brier_K_rb=brier_K_rb, brier_K_full=m_full["brier_K"],
            ))

        # Also evaluate Gate v1 pooled displacement moments on the 13-week test window [T0..T0+test_len-1]
        test_len = min(26 if T >= 120 else 15, T - T0)
        g_hor = [h for h in (1, 4, 13) if h < test_len]
        # Cohort = top 100 at T0 - 1 (matching Gate v1's top-100 cap)
        coh100 = np.flatnonzero(np.isfinite(R[T0 - 1]) & (R[T0 - 1] <= 100))
        if coh100.size >= 50 and g_hor:
            em = _gate_moments_from_ranks(R[T0:T0 + test_len], coh100, g_hor, K)
            # Historical-mobility baseline from last `test_len` weeks of train
            coh_tr = np.flatnonzero(np.isfinite(R[max(0, T0 - test_len - 1)]) & (R[max(0, T0 - test_len - 1)] <= 100))
            bm = _gate_moments_from_ranks(R[max(0, T0 - test_len):T0], coh_tr, g_hor, K)
            # Simulate `test_len` weeks continuously for 20 seeds
            sm_list = []
            # Fast single-pass continuous simulation for Gate v1
            c11 = np.sqrt(np.maximum(filt["P11"], 1e-10))
            c21 = filt["P12"] / c11
            c22 = np.sqrt(np.maximum(filt["P22"] - c21 * c21, 1e-10))
            for sd in range(n_ens):
                rng = np.random.default_rng(9000 + split_i * 100 + sd)
                z1, z2 = rng.standard_normal(N), rng.standard_normal(N)
                L_i = filt["L_hat"] + c11 * z1
                xi_i = filt["xi_hat"] + c21 * z1 + c22 * z2
                lv = np.log(filt["v_hat"]) + filt["post_s_v"] * np.clip(rng.standard_normal(N), -2.0, 2.0) if p_tr.s_vol > 0.02 else np.zeros(N)
                v_i = np.exp(np.clip(lv, -2.5 * p_tr.s_vol, 2.5 * p_tr.s_vol)) if p_tr.s_vol > 0.02 else np.ones(N)
                st = cr.CommonState(p_tr, L_i, rng, xi0=xi_i, v0=v_i)
                Rs_te = np.full((test_len, N), np.nan)
                for tt in range(test_len):
                    xt = st.step(apply_absence=True)
                    ok = np.flatnonzero(np.isfinite(xt))
                    if ok.size:
                        oo = ok[np.argsort(-xt[ok], kind="stable")]
                        rk = np.full(N, np.nan)
                        rk[oo] = np.arange(1, oo.size + 1, dtype=float)
                        alive = st.ids == np.arange(N, dtype=np.int64)
                        Rs_te[tt] = np.where(alive, rk, np.nan)
                sm_list.append(_gate_moments_from_ranks(Rs_te, coh100, g_hor, K))
            sm = {k: float(np.nanmean([d[k] for d in sm_list])) for k in em}
            kk = [k for k in em if np.isfinite(em[k]) and np.isfinite(sm[k]) and abs(em[k]) >= 0.02]
            me = float(np.mean([abs(sm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk]))
            be = float(np.mean([abs(bm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk if np.isfinite(bm.get(k, np.nan))]))
            gate_splits.append(dict(T0=T0, model_rel=me, base_rel=be, em=em, sm=sm, bm=bm))

    # Aggregate over origins
    agg_h = {}
    for h_s, rows in by_h.items():
        if not rows:
            continue
        keys = [k for k in rows[0] if k != "T0"]
        agg_h[h_s] = {k: float(np.mean([r[k] for r in rows])) for k in keys}
        agg_h[h_s]["n_origins"] = len(rows)

    gate_agg = dict(
        model_rel=float(np.mean([g["model_rel"] for g in gate_splits])),
        model_rel_sd=float(np.std([g["model_rel"] for g in gate_splits])),
        base_rel=float(np.mean([g["base_rel"] for g in gate_splits])),
        base_rel_sd=float(np.std([g["base_rel"] for g in gate_splits])),
        wins=int(sum(g["model_rel"] < g["base_rel"] for g in gate_splits)),
        n_splits=len(gate_splits),
    ) if gate_splits else {}

    return dict(origins=origins, by_horizon=agg_h, gate_v1=gate_agg)


def _band_agg(dicts: list[dict], key: str) -> dict:
    v = np.array([d[key] for d in dicts if np.isfinite(d[key])], float)
    if not v.size:
        return dict(mean=np.nan, median=np.nan, q05=np.nan, q95=np.nan)
    return dict(mean=float(np.mean(v)), median=float(np.median(v)),
                q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)))


def run_panel(pn: str, cfg: dict, n_seeds: int = 20) -> dict:
    print(f"\n==================== STAGE 2: {pn} ====================", flush=True)
    data, sb = load_cached(pn)
    X = data["X"].astype(np.float64)
    R = data["R"].astype(np.float64)
    floors = data["floors"].astype(np.float64)
    K = int(data["K"])
    T, N = X.shape

    # 1. Fit 12-scalar Common model on full panel (for Goal 1 structure validation)
    p = cr.estimate_common(X, R, K=K, floors=floors, spec_b=sb if cfg["spec_b"] else None, refine_drift=True)
    # Principled deconvolved starting ladder (0 free params, replaces c = 0.96!)
    nb = T // cfg["block"]
    start = T - nb * cfg["block"]
    m_blk0 = wmean(X, start, start + cfg["block"], min(cfg["pres"], 0.75))
    m_all = np.nanmean(X, axis=0)
    fill_v = float(np.nanmin(m_all))
    m_blk0 = np.where(np.isfinite(m_blk0), m_blk0, np.where(np.isfinite(m_all), m_all, fill_v))
    L0_dec = cr.deconvolve_starting_ladder(p, m_blk0, block=cfg["block"], iters=5)
    # Re-refine balancing drift on the deconvolved ladder so start and stationary drift agree
    dz_d, dg_d, _ = cr._refine_drift_smooth(p, L0_dec)
    p = cr.replace(p, drift_z=dz_d, drift_g=dg_d)

    print(f"  Params ({p.n_free_scalars()} scalars, spec_b={p.spec_b_pinned}): "
          f"sigL0={p.sigma_L0:.4f}, beta={p.beta:+.3f}, phi={p.phi:.2f}, "
          f"xi=({p.a_xi:+.2f},{p.b_xi:+.2f}), obs=({p.a_obs:+.2f},{p.b_obs:+.2f}), "
          f"s_vol={p.s_vol:.3f}, gamma_L={p.gamma_L:.2f}, abs=({p.a_abs:+.2f},{p.b_abs:+.2f}), exit={p.lam_exit:.4f}",
          flush=True)

    # 2. Goal 1: Data vs 20 simulated worlds
    emp_st = structure_stats(X, R, K, cfg)
    sim_sts = []
    for sd in range(n_seeds):
        Xs, Rs, ids = cr.simulate_common_panel(p, L0_dec, T_sim=T, seed=sd, apply_absence=True, burn=0)
        sim_sts.append(structure_stats(Xs, Rs, K, cfg, ids=ids))
        del Xs, Rs, ids
    gc.collect()

    # Summarize Goal 1
    g1_sim = dict(
        turnover={k: _band_agg([s["turnover"] for s in sim_sts], k) for k in emp_st["turnover"]},
        occ_change={k: _band_agg([s["occ_change"] for s in sim_sts], k) for k in emp_st["occ_change"]},
        lad_first={k: _band_agg([s["lad_first"] for s in sim_sts], k) for k in emp_st["lad_first"]},
        lad_change={k: _band_agg([s["lad_change"] for s in sim_sts], k) for k in emp_st["lad_change"]},
        head_share=_band_agg(sim_sts, "head_share"),
        max_X=_band_agg(sim_sts, "max_X"),
        boundary_ratio=_band_agg(sim_sts, "boundary_ratio"),
        dep_rate=_band_agg(sim_sts, "dep_rate"),
        dep_cross_frac=_band_agg(sim_sts, "dep_cross_frac"),
        gapped_rho=_band_agg(sim_sts, "gapped_rho"),
        D=_band_agg(sim_sts, "D"),
    )

    k64, k2 = str(K // 64), str(K // 2)
    print(f"  Goal 1 (Data vs 20-seed Stage 2):", flush=True)
    print(f"    Turn top10 : data={emp_st['turnover']['10']:.3f} | sim={g1_sim['turnover']['10']['mean']:.3f} [{g1_sim['turnover']['10']['q05']:.3f},{g1_sim['turnover']['10']['q95']:.3f}]", flush=True)
    print(f"    Turn K/64  : data={emp_st['turnover'][k64]:.3f} | sim={g1_sim['turnover'][k64]['mean']:.3f} [{g1_sim['turnover'][k64]['q05']:.3f},{g1_sim['turnover'][k64]['q95']:.3f}]", flush=True)
    print(f"    Turn K/2   : data={emp_st['turnover'][k2]:.3f} | sim={g1_sim['turnover'][k2]['mean']:.3f} [{g1_sim['turnover'][k2]['q05']:.3f},{g1_sim['turnover'][k2]['q95']:.3f}]", flush=True)
    print(f"    Rank-1 chg : data={emp_st['occ_change']['1']:.3f} | sim={g1_sim['occ_change']['1']['mean']:.3f} [{g1_sim['occ_change']['1']['q05']:.3f},{g1_sim['occ_change']['1']['q95']:.3f}]", flush=True)
    print(f"    Lad K/64   : data={emp_st['lad_first'][k64]:.2f} (d={emp_st['lad_change'][k64]:+.2f}) | sim={g1_sim['lad_first'][k64]['mean']:.2f} (d={g1_sim['lad_change'][k64]['mean']:+.2f})", flush=True)
    print(f"    Head share : data={emp_st['head_share']:.3f} | sim mean={g1_sim['head_share']['mean']:.3f}, med={g1_sim['head_share']['median']:.3f} [{g1_sim['head_share']['q05']:.3f},{g1_sim['head_share']['q95']:.3f}] | max_X={g1_sim['max_X']['q95']:.2f} (data {emp_st['max_X']:.2f})", flush=True)
    print(f"    Boundary   : data={emp_st['boundary_ratio']:.3f} | sim={g1_sim['boundary_ratio']['mean']:.3f} [{g1_sim['boundary_ratio']['q05']:.3f},{g1_sim['boundary_ratio']['q95']:.3f}]", flush=True)
    print(f"    Departures : data rate={emp_st['dep_rate']:.4f} (cross={emp_st['dep_cross_frac']:.3f}) | sim rate={g1_sim['dep_rate']['mean']:.4f} (cross={g1_sim['dep_cross_frac']['mean']:.3f})", flush=True)
    print(f"    Persist/D  : data rho={emp_st['gapped_rho']:+.3f}, D={emp_st['D']:+.3f} | sim rho={g1_sim['gapped_rho']['mean']:+.3f}, D={g1_sim['D']['mean']:+.3f}", flush=True)

    # 3. Goal 2: Rolling-origin calendar-aligned endpoint forecasts + Gate v1
    g2 = oos_endpoint_and_gate(pn, X, R, floors, K, cfg, sb if cfg["spec_b"] else None, n_ens=n_seeds)
    print(f"  Goal 2 (5 rolling origins, calendar-aligned, {n_seeds} members):", flush=True)
    for h_s, s in g2["by_horizon"].items():
        print(f"    h={int(h_s):2d}w | logR CRPS: no={s['crps_lr_no']:.4f} rb={s['crps_lr_rb']:.4f} noEB={s['crps_lr_noeb']:.4f} FULL={s['crps_lr_full']:.4f} | "
              f"X CRPS: noEB={s['crps_X_noeb']:.4f} FULL={s['crps_X_full']:.4f} (cov90={s['cov90_full']:.2f}) | "
              f"share_r={s['share_corr_full']:.3f} | Brier(hd): no={s['brier_hd_no']:.4f} rb={s['brier_hd_rb']:.4f} FULL={s['brier_hd_full']:.4f}", flush=True)
    if g2["gate_v1"]:
        gv = g2["gate_v1"]
        print(f"    Gate v1 pooled: FULL={gv['model_rel']:.3f}±{gv['model_rel_sd']:.3f} vs hist_mob={gv['base_rel']:.3f}±{gv['base_rel_sd']:.3f} (wins {gv['wins']}/{gv['n_splits']})", flush=True)

    params_dict = {k: getattr(p, k) for k in (
        "sigma_L0", "beta", "phi", "a_xi", "b_xi", "a_obs", "b_obs",
        "s_vol", "gamma_L", "a_abs", "b_abs", "lam_exit", "K", "N", "L_K", "z_K", "spec_b_pinned",
    )}
    params_dict["n_free_scalars"] = p.n_free_scalars()
    return dict(params=params_dict, goal1_data=emp_st, goal1_sim=g1_sim, goal2=g2)


def main():
    import warnings
    warnings.filterwarnings("ignore", message="Mean of empty slice")
    args = sys.argv[1:]
    tag = "v2"
    if args and args[0].startswith("--tag="):
        tag = args[0].split("=", 1)[1]
        args = args[1:]
    targets = args if args else list(PANELS.keys())
    out = {}
    for pn in targets:
        out[pn] = run_panel(pn, PANELS[pn], n_seeds=20)
        gc.collect()
    out_path = OUT_DIR / f"stage2_{tag}_benchmark_results.json"
    out_path.write_text(json.dumps(out, indent=2))
    print(f"\nSaved {out_path}", flush=True)


if __name__ == "__main__":
    main()
