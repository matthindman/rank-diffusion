#!/usr/bin/env python3
"""Stage 3 Head-to-Head Benchmark: Stage 2 (11 scalars, asymmetry=False) vs
Stage 3 (11–15 scalars, asymmetry=True: fading positive surges + persistent entity drift)
on COMMON RANDOM NUMBERS across all four DEVELOPMENT ladders:
  - facebook_a (Era A, 86w, K=3500, Spec-B lower bound)
  - instagram_hm (2023 apparatus, 52w, K=10000)
  - reddit_comments_ext (213w, K=12500)
  - reddit_submissions_long (212w, K=5000)

Evaluates all three goals simultaneously:
  Goal 1 (Aggregate Structure & Asymmetry, 20 paired seeds):
    turnover, rank-1/10 change, ladder span & drift, arithmetic head share (mean/median/5-95%, max_X),
    crossing ratio, established-departure rate & crossing fraction,
    gapped persistence rho, D = p_fall - p_rise, p_fall, p_rise, m21, m12, leverage, decline runs r2/r3.
  Goal 2 (Calendar-Aligned Endpoint Forecasts & Gate v1, 5 rolling origins, 20 members):
    X CRPS & MAE, cov90, coarsened logR CRPS (vs no_change & rank_bin),
    audience share shift MAE & corr, top-K/16 & top-K Brier, Gate v1 pooled relative error.
  Goal 3 (Parsimony):
    Exact scalar count per platform (11 for Stage 2; 14–15 for Stage 3 when active).

Runs 100% sandboxed from the lean DEVELOPMENT scratch cache. Zero sealed data contact.
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
    "facebook_a": dict(block=26, pres=0.9, W=4, H=26, gap=4, step=4, run_blk=13, horizons=[1, 4, 13, 26], spec_b=True),
    "instagram_hm": dict(block=13, pres=0.75, W=4, H=16, gap=4, step=4, run_blk=7, horizons=[1, 4, 13], spec_b=False),
    "reddit_comments_ext": dict(block=52, pres=0.9, W=13, H=52, gap=13, step=13, run_blk=26, horizons=[1, 4, 13, 26, 52], spec_b=False),
    "reddit_submissions_long": dict(block=52, pres=0.9, W=13, H=52, gap=13, step=13, run_blk=26, horizons=[1, 4, 13, 26, 52], spec_b=False),
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
    """Identical Goal 1 structure + asymmetry evaluator for empirical & simulated panels."""
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

    # 4. Arithmetic head share (top K/64 of top K in last block) + max X
    k64 = K // 64
    lo, hi = start + (nb - 1) * block, start + nb * block
    A_blk = np.where(np.isfinite(X[lo:hi]), np.expm1(np.clip(X[lo:hi], 0.0, 45.0)), 0.0)
    if tops[-1].size >= K:
        head_share = float(A_blk[:, tops[-1][:k64]].sum() / max(A_blk[:, tops[-1][:K]].sum(), 1.0))
    else:
        head_share = np.nan
    max_X = float(np.nanmax(X))

    # 5. Crossing ratio (measured vs predicted c = sigma_L^2 / (2 S))
    b_cr = 26 if T >= 160 else (13 if T >= 70 else 7)
    nb_c = T // b_cr
    st_c = T - nb_c * b_cr
    Mc = np.array([wmean(X, st_c + b * b_cr, st_c + (b + 1) * b_cr, min(pres, 0.75)) for b in range(nb_c)])
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
            c_obs = float(np.mean(Lb[ob[:k]]) - np.mean(Lb[oa[:k]]))
            lo_r, hi_r = max(1, int(k / 1.4)), min(oa.size, int(k * 1.4))
            S_k = float(-np.polyfit(np.log(np.arange(lo_r, hi_r + 1)), La[oa[lo_r - 1:hi_r]], 1)[0])
            sig2_dt = float(np.var(dL[lo_r - 1:hi_r]))
            c_pred = sig2_dt / max(2.0 * S_k, 1e-6)
            if c_obs > 0 and c_pred > 0:
                ratios.append(c_obs / c_pred)
    boundary_ratio = float(np.exp(np.mean(np.log(ratios)))) if ratios else np.nan

    # 6. Established-departure rate & crossing fraction
    mid = T // 2
    floors = np.sum(np.isfinite(R), axis=1).astype(float) + 1.0
    pen1 = np.where(np.isfinite(R[:mid]), R[:mid], floors[:mid, None])
    rperm1 = np.mean(pen1, axis=0)
    ord1 = np.empty(N, dtype=float)
    ord1[np.argsort(rperm1, kind="stable")] = np.arange(1, N + 1)
    est_coh = ord1 <= (K // 2)
    R2 = R[mid:, est_coh]
    if ids is not None:
        id_ref = ids[mid - 1, est_coh]
        alive2 = ids[mid:, est_coh] == id_ref[None, :]
        inK2 = alive2 & np.isfinite(R2) & (R2 <= K)
        still_obs_below = alive2 & np.isfinite(R2) & (R2 > K)
        ex_ev = inK2[:-1] & (~inK2[1:])
        at_risk = inK2[:-1]
        dep_rate = float(np.sum(ex_ev) / max(np.sum(at_risk), 1))
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

    # 7. Gapped persistence, asymmetry D, p_fall, p_rise, third moments m21/m12, leverage
    W, H, gap, step = cfg["W"], cfg["H"], cfg["gap"], cfg["step"]
    pen_all = np.where(np.isfinite(R), R, floors[:, None])
    pr_all = np.mean(pen_all, axis=0)
    o_pr = np.argsort(pr_all, kind="stable")
    lab = np.full(N, -1, dtype=np.int32)
    lab[o_pr[:K]] = 2
    lab[o_pr[:K // 4]] = 1
    lab[o_pr[:K // 16]] = 0

    rhos, Ds, pfs, prs, m21s, m12s, levs = [], [], [], [], [], [], []
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
            pf = float(np.mean(yo[lo_m]) / np.mean(xo[lo_m]))
            pr_v = float(np.mean(yo[hi_m]) / np.mean(xo[hi_m]))
            pfs.append(pf)
            prs.append(pr_v)
            Ds.append(pf - pr_v)
            sx, sy = float(np.std(xo)), float(np.std(yo))
            if sx > 1e-8 and sy > 1e-8:
                xw = np.clip(xo / sx, -6.0, 6.0)
                yw = np.clip(yo / sy, -6.0, 6.0)
                m21s.append(float(np.mean(xw * xw * yw)))
                m12s.append(float(np.mean(xw * yw * yw)))
                sd_lo = float(np.std(yo[lo_m]))
                sd_hi = float(np.std(yo[hi_m]))
                if sd_hi > 1e-8:
                    levs.append(sd_lo / sd_hi)
        a += step

    # 8. Consecutive decline-vs-gain run ratios (r2, r3) on non-overlapping blocks
    rb_len = cfg["run_blk"]
    nb_r = T // rb_len
    r2_ratio, r3_ratio = np.nan, np.nan
    if nb_r >= 4:
        st_r = T - nb_r * rb_len
        sel_k = lab >= 0
        Br = np.array([wmean(X[:, sel_k], st_r + b * rb_len, st_r + (b + 1) * rb_len, min(pres, 0.75)) for b in range(nb_r)])
        dB = np.diff(Br, axis=0)
        dB = dB - np.nanmean(dB, axis=1, keepdims=True)
        # 2-block runs
        ok2r = np.isfinite(dB[:-1]) & np.isfinite(dB[1:])
        n_dec2 = float(np.sum(ok2r & (dB[:-1] < 0) & (dB[1:] < 0)))
        n_inc2 = float(np.sum(ok2r & (dB[:-1] > 0) & (dB[1:] > 0)))
        r2_ratio = n_dec2 / max(n_inc2, 1.0)
        if dB.shape[0] >= 3:
            ok3r = np.isfinite(dB[:-2]) & np.isfinite(dB[1:-1]) & np.isfinite(dB[2:])
            n_dec3 = float(np.sum(ok3r & (dB[:-2] < 0) & (dB[1:-1] < 0) & (dB[2:] < 0)))
            n_inc3 = float(np.sum(ok3r & (dB[:-2] > 0) & (dB[1:-1] > 0) & (dB[2:] > 0)))
            r3_ratio = n_dec3 / max(n_inc3, 1.0)

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
        p_fall=float(np.mean(pfs)) if pfs else np.nan,
        p_rise=float(np.mean(prs)) if prs else np.nan,
        m21=float(np.mean(m21s)) if m21s else np.nan,
        m12=float(np.mean(m12s)) if m12s else np.nan,
        lev=float(np.mean(levs)) if levs else np.nan,
        r2=float(r2_ratio),
        r3=float(r3_ratio),
    )


def _gate_moments_from_ranks(R_mat: np.ndarray, cohort_cols: np.ndarray, horizons: list[int], K: int) -> dict:
    T_te = R_mat.shape[0]
    cr_m = R_mat[:, cohort_cols]
    out = {}
    for h in horizons:
        if h < T_te:
            d = np.abs(cr_m[h:] - cr_m[:-h])
            f = d[np.isfinite(d)]
            out[f"dRank{h}"] = float(np.median(f)) if f.size else np.nan
    if T_te >= 2:
        a, b = cr_m[:-1], cr_m[1:]
        ok = np.isfinite(a) & np.isfinite(b)
        out["RACF1"] = float(spearmanr(a[ok], b[ok]).correlation) if ok.sum() > 50 else np.nan
    for c in (1, 5, 20):
        occ = np.full(T_te, -1, dtype=np.int64)
        for t in range(T_te):
            w = np.flatnonzero(R_mat[t] == c)
            if w.size:
                occ[t] = w[0]
        ok = (occ[1:] >= 0) & (occ[:-1] >= 0)
        out[f"coll{c}"] = float(np.mean(occ[1:][ok] != occ[:-1][ok])) if ok.any() else np.nan
    return out


def _sim_gate_v1(filt: dict, p_tr: cr.CommonParams, test_len: int, coh100: np.ndarray,
                 g_hor: list[int], K: int, n_ens: int, split_i: int) -> dict:
    """Continuous Gate v1 simulation using exact posterior sampling (2-state or 4-state)."""
    N = filt["L_hat"].size
    use_4 = "P4" in filt
    if use_4:
        chol4 = np.linalg.cholesky(filt["P4"] + 1e-10 * np.eye(4)[None, :, :])
        has_d = p_tr.decline_var > 0.0
        has_s = (p_tr.surge_lam > 0.0) and (p_tr.surge_mu > 0.0) and (p_tr.surge_rho > 0.0)
        sd_d = math.sqrt(p_tr.decline_var) if has_d else 0.0
        s_mean = cr._surge_mean(p_tr.surge_lam, p_tr.surge_mu, p_tr.surge_rho) if has_s else 0.0
    else:
        c11 = np.sqrt(np.maximum(filt["P11"], 1e-10))
        c21 = filt["P12"] / c11
        c22 = np.sqrt(np.maximum(filt["P22"] - c21 * c21, 1e-10))

    sm_list = []
    for sd in range(n_ens):
        rng = np.random.default_rng(9000 + split_i * 100 + sd)
        if use_4:
            draw = np.einsum("nij,nj->ni", chol4, rng.standard_normal((N, 4)))
            L_i = filt["L_hat"] + draw[:, 0]
            xi_i = filt["xi_hat"] + draw[:, 1]
            d_i = np.clip(filt["d_hat"] + draw[:, 2], -2.8 * sd_d, 2.8 * sd_d) if has_d else None
            if has_s:
                s_sc = filt.get("s_scale", np.ones(N))
                s_obs_raw = filt["s_hat"] + draw[:, 3]
                s_unit_raw = s_obs_raw / np.maximum(s_sc, 1e-4) + s_mean
                s_i = np.maximum(s_unit_raw, 0.0)
                L_i = L_i - s_sc * (s_i - s_unit_raw)
            else:
                s_i = None
        else:
            z1, z2 = rng.standard_normal(N), rng.standard_normal(N)
            L_i = filt["L_hat"] + c11 * z1
            xi_i = filt["xi_hat"] + c21 * z1 + c22 * z2
            d_i, s_i = None, None
        if p_tr.s_vol > 0.02:
            lv = np.log(filt["v_hat"]) - 0.5 * (filt["post_s_v"] ** 2) + filt["post_s_v"] * np.clip(rng.standard_normal(N), -2.0, 2.0)
            v_i = np.exp(np.clip(lv, -2.5 * p_tr.s_vol, 2.5 * p_tr.s_vol))
        else:
            v_i = np.ones(N)
        st = cr.CommonState(p_tr, L_i, rng, xi0=xi_i, v0=v_i, d0=d_i, s0=s_i)
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
    return {k: float(np.nanmean([d[k] for d in sm_list])) for k in sm_list[0]}


def oos_head_to_head(pn: str, X: np.ndarray, R: np.ndarray, floors: np.ndarray, K: int,
                     cfg: dict, spec_b: tuple[np.ndarray, np.ndarray] | None, n_ens: int = 20) -> dict:
    """Paired Goal 2 evaluation: Stage 2 (asymmetry=False) vs Stage 3 (asymmetry=True) + baselines."""
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

    by_h = {str(h): [] for h in horizons}
    gate_s2, gate_s3 = [], []

    for split_i, T0 in enumerate(origins):
        X_tr, R_tr, fl_tr = X[:T0], R[:T0], floors[:T0]

        p_s2 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=spec_b, refine_drift=True, asymmetry=False)
        p_s3 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=spec_b, refine_drift=True, asymmetry=True)
        filt_s2 = cr.filter_endpoints(X_tr, R_tr, p_s2, floors_tr=fl_tr)
        filt_s3 = cr.filter_endpoints(X_tr, R_tr, p_s3, floors_tr=fl_tr)

        pen_tr = np.where(np.isfinite(R_tr), R_tr, fl_tr[:, None])
        rperm_tr = np.mean(pen_tr, axis=0)
        pres_tr = np.isfinite(R_tr).mean(axis=0)
        ord_tr = np.argsort(rperm_tr, kind="stable")
        topK = ord_tr[:K]
        if pn == "instagram_hm":
            topK = topK[pres_tr[topK] >= 0.5]

        fc_s2 = cr.forecast_endpoints(filt_s2, p_s2, horizons=horizons, n_ens=n_ens, seed=100 * split_i, apply_absence=True, use_eb_vol=True)
        fc_s3 = cr.forecast_endpoints(filt_s3, p_s3, horizons=horizons, n_ens=n_ens, seed=100 * split_i, apply_absence=True, use_eb_vol=True)

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
            yt_X = Xr[t_fut, topK]
            rt = R[t_fut, topK]
            logr_true = np.log(np.where(np.isfinite(rt) & (rt <= K), rt, K + 1.0))

            # Rank-bin baseline
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

            def score_model(fc_pair):
                Xe_all, Re_all = fc_pair
                Xe_rel = np.empty((topK.size, n_ens))
                S_ens = np.empty((topK.size, n_ens))
                for m in range(n_ens):
                    xm, rm = Xe_all[:, m], Re_all[:, m]
                    inKm = np.isfinite(rm) & (rm <= K) & np.isfinite(xm)
                    med_m = float(np.nanmedian(xm[inKm])) if inKm.any() else float(np.nanmedian(xm))
                    Xe_rel[:, m] = xm[topK] - med_m
                    am = np.where(np.isfinite(xm), np.expm1(np.clip(xm, 0.0, 40.0)), 0.0)
                    tot_m = float(np.sum(am[inKm])) if inKm.any() else max(float(np.sum(am)), 1.0)
                    S_ens[:, m] = am[topK] / max(tot_m, 1.0)
                Re_top = Re_all[topK]
                lre = np.log(np.where(np.isfinite(Re_top) & (Re_top <= K), Re_top, K + 1.0))

                ok_x = np.isfinite(yt_X) & np.all(np.isfinite(Xe_rel), axis=1)
                if ok_x.sum() < 100:
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
                crps_lr = float(np.mean(fair_ensemble_crps(logr_true, lre)))

                s_now, s_true = S_share[T0 - 1, topK], S_share[t_fut, topK]
                ds_true, ds_hat = s_true - s_now, np.median(S_ens, axis=1) - s_now
                share_mae = float(np.mean(np.abs(ds_true - ds_hat)))
                share_corr = float(np.corrcoef(ds_true, ds_hat)[0, 1]) if np.std(ds_hat) > 1e-12 else 0.0

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

            m2 = score_model(fc_s2[h])
            m3 = score_model(fc_s3[h])

            ok_no = np.isfinite(yt_X) & np.isfinite(last_Xr[topK])
            mae_no = float(np.mean(np.abs(yt_X[ok_no] - last_Xr[topK[ok_no]])))
            crps_lr_no = float(np.mean(np.abs(logr_true - logr_now[topK])))
            crps_lr_rb = float(np.mean(fair_ensemble_crps(logr_true, rb_ens)))
            s_now, s_true = S_share[T0 - 1, topK], S_share[t_fut, topK]
            share_mae_no = float(np.mean(np.abs(s_true - s_now)))
            k_hd = K // 16
            near_hd = np.isfinite(r_now[topK]) & (r_now[topK] <= K // 4)
            y_hd = (np.isfinite(rt) & (rt <= k_hd)).astype(float)
            brier_hd_rb = float(np.mean((y_hd[near_hd] - np.mean(rb_ens[near_hd] <= np.log(k_hd + 0.5), axis=1)) ** 2))

            by_h[str(h)].append(dict(
                T0=T0, mae_no=mae_no, crps_lr_no=crps_lr_no, crps_lr_rb=crps_lr_rb,
                share_mae_no=share_mae_no, brier_hd_rb=brier_hd_rb,
                mae_s2=m2["mae_X"], crps_X_s2=m2["crps_X"], cov90_s2=m2["cov90"], crps_lr_s2=m2["crps_lr"],
                share_mae_s2=m2["share_mae"], share_corr_s2=m2["share_corr"], brier_hd_s2=m2["brier_head"], brier_K_s2=m2["brier_K"],
                mae_s3=m3["mae_X"], crps_X_s3=m3["crps_X"], cov90_s3=m3["cov90"], crps_lr_s3=m3["crps_lr"],
                share_mae_s3=m3["share_mae"], share_corr_s3=m3["share_corr"], brier_hd_s3=m3["brier_head"], brier_K_s3=m3["brier_K"],
            ))

        # Gate v1 pooled displacement evaluation (paired seeds!)
        test_len = min(26 if T >= 120 else 15, T - T0)
        g_hor = [h for h in (1, 4, 13) if h < test_len]
        coh100 = np.flatnonzero(np.isfinite(R[T0 - 1]) & (R[T0 - 1] <= 100))
        if coh100.size >= 50 and g_hor:
            em = _gate_moments_from_ranks(R[T0:T0 + test_len], coh100, g_hor, K)
            coh_tr = np.flatnonzero(np.isfinite(R[max(0, T0 - test_len - 1)]) & (R[max(0, T0 - test_len - 1)] <= 100))
            bm = _gate_moments_from_ranks(R[max(0, T0 - test_len):T0], coh_tr, g_hor, K)
            sm2 = _sim_gate_v1(filt_s2, p_s2, test_len, coh100, g_hor, K, n_ens, split_i)
            sm3 = _sim_gate_v1(filt_s3, p_s3, test_len, coh100, g_hor, K, n_ens, split_i)
            kk = [k for k in em if np.isfinite(em[k]) and np.isfinite(sm2[k]) and np.isfinite(sm3[k]) and abs(em[k]) >= 0.02]
            be = float(np.mean([abs(bm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk if np.isfinite(bm.get(k, np.nan))]))
            e2 = float(np.mean([abs(sm2[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk]))
            e3 = float(np.mean([abs(sm3[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk]))
            gate_s2.append(dict(T0=T0, model_rel=e2, base_rel=be, em=em, sm=sm2))
            gate_s3.append(dict(T0=T0, model_rel=e3, base_rel=be, em=em, sm=sm3))

    agg_h = {}
    for h_s, rows in by_h.items():
        if rows:
            keys = [k for k in rows[0] if k != "T0"]
            agg_h[h_s] = {k: float(np.mean([r[k] for r in rows])) for k in keys}
            agg_h[h_s]["n_origins"] = len(rows)

    def _agg_g(gs):
        if not gs:
            return {}
        return dict(
            model_rel=float(np.mean([g["model_rel"] for g in gs])),
            model_rel_sd=float(np.std([g["model_rel"] for g in gs])),
            base_rel=float(np.mean([g["base_rel"] for g in gs])),
            base_rel_sd=float(np.std([g["base_rel"] for g in gs])),
            wins=int(sum(g["model_rel"] < g["base_rel"] for g in gs)),
            n_splits=len(gs),
        )

    return dict(origins=origins, by_horizon=agg_h, gate_v1_s2=_agg_g(gate_s2), gate_v1_s3=_agg_g(gate_s3))


def _band_agg(dicts: list[dict], key: str) -> dict:
    v = np.array([d[key] for d in dicts if np.isfinite(d[key])], float)
    if not v.size:
        return dict(mean=np.nan, median=np.nan, q05=np.nan, q95=np.nan)
    return dict(mean=float(np.mean(v)), median=float(np.median(v)),
                q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)))


def _fit_and_sim_g1(X: np.ndarray, R: np.ndarray, floors: np.ndarray, K: int, cfg: dict,
                    sb: tuple[np.ndarray, np.ndarray] | None, asymmetry: bool, n_seeds: int) -> tuple[cr.CommonParams, dict]:
    T = X.shape[0]
    p = cr.estimate_common(X, R, K=K, floors=floors, spec_b=sb if cfg["spec_b"] else None,
                           refine_drift=True, asymmetry=asymmetry)
    nb = T // cfg["block"]
    start = T - nb * cfg["block"]
    m_blk0 = wmean(X, start, start + cfg["block"], min(cfg["pres"], 0.75))
    m_all = np.nanmean(X, axis=0)
    fill_v = float(np.nanmin(m_all))
    m_blk0 = np.where(np.isfinite(m_blk0), m_blk0, np.where(np.isfinite(m_all), m_all, fill_v))
    L0_dec = cr.deconvolve_starting_ladder(p, m_blk0, block=cfg["block"], iters=5)
    dz_d, dg_d, _ = cr._refine_drift_smooth(p, L0_dec)
    p = cr.replace(p, drift_z=dz_d, drift_g=dg_d)

    sim_stats = []
    for sd in range(n_seeds):
        Xs, Rs, ids_s = cr.simulate_common_panel(p, L0_dec, T_sim=T, seed=300 + sd, apply_absence=True)
        sim_stats.append(structure_stats(Xs, Rs, K, cfg, ids=ids_s))

    scalar_keys = ["head_share", "max_X", "boundary_ratio", "dep_rate", "dep_cross_frac",
                   "gapped_rho", "D", "p_fall", "p_rise", "m21", "m12", "lev", "r2", "r3"]
    agg = {k: _band_agg(sim_stats, k) for k in scalar_keys}
    agg["turnover"] = {
        k: _band_agg([dict(v=s["turnover"][k]) for s in sim_stats], "v")
        for k in sim_stats[0]["turnover"]
    }
    agg["occ_change"] = {
        k: _band_agg([dict(v=s["occ_change"][k]) for s in sim_stats], "v")
        for k in sim_stats[0]["occ_change"]
    }
    agg["lad_first"] = {
        k: _band_agg([dict(v=s["lad_first"][k]) for s in sim_stats], "v")
        for k in sim_stats[0]["lad_first"]
    }
    agg["lad_change"] = {
        k: _band_agg([dict(v=s["lad_change"][k]) for s in sim_stats], "v")
        for k in sim_stats[0]["lad_change"]
    }
    return p, agg


def _param_dict(p: cr.CommonParams) -> dict:
    return dict(
        n_scalars=p.n_free_scalars(), spec_b_pinned=p.spec_b_pinned,
        sigma_L0=p.sigma_L0, beta=p.beta, phi=p.phi,
        a_xi=p.a_xi, b_xi=p.b_xi, a_obs=p.a_obs, b_obs=p.b_obs,
        s_vol=p.s_vol, gamma_L=p.gamma_L, a_abs=p.a_abs, b_abs=p.b_abs, lam_exit=p.lam_exit,
        surge_lam=p.surge_lam, surge_mu=p.surge_mu, surge_rho=p.surge_rho,
        surge_hl=float(-math.log(2.0) / math.log(p.surge_rho)) if p.surge_rho > 0 else 0.0,
        surge_Vs=cr._surge_variance(p.surge_lam, p.surge_mu, p.surge_rho),
        sigma_d=math.sqrt(p.decline_var) if p.decline_var > 0 else 0.0,
        decline_rho=p.decline_rho,
    )


def run_panel(pn: str, cfg: dict, n_seeds: int = 20) -> dict:
    print(f"\n==================== STAGE 3 HEAD-TO-HEAD: {pn} ====================", flush=True)
    data, sb = load_cached(pn)
    X = data["X"].astype(np.float64)
    R = data["R"].astype(np.float64)
    floors = data["floors"].astype(np.float64)
    K = int(data["K"])
    T, N = X.shape

    emp = structure_stats(X, R, K, cfg)
    p2, g1_s2 = _fit_and_sim_g1(X, R, floors, K, cfg, sb, asymmetry=False, n_seeds=n_seeds)
    p3, g1_s3 = _fit_and_sim_g1(X, R, floors, K, cfg, sb, asymmetry=True, n_seeds=n_seeds)

    pd2, pd3 = _param_dict(p2), _param_dict(p3)
    print(f"  S2 ({pd2['n_scalars']} sc): sL0={pd2['sigma_L0']:.4f} beta={pd2['beta']:+.3f} phi={pd2['phi']:.2f} "
          f"xi=({pd2['a_xi']:+.2f},{pd2['b_xi']:+.2f}) obs=({pd2['a_obs']:+.2f},{pd2['b_obs']:+.2f}) s={pd2['s_vol']:.3f}", flush=True)
    print(f"  S3 ({pd3['n_scalars']} sc): sL0={pd3['sigma_L0']:.4f} beta={pd3['beta']:+.3f} phi={pd3['phi']:.2f} "
          f"xi=({pd3['a_xi']:+.2f},{pd3['b_xi']:+.2f}) obs=({pd3['a_obs']:+.2f},{pd3['b_obs']:+.2f}) s={pd3['s_vol']:.3f} | "
          f"surge(hl={pd3['surge_hl']:.0f},lam={pd3['surge_lam']:.4f},mu={pd3['surge_mu']:.2f},Vs={pd3['surge_Vs']:.3f}) "
          f"sigma_d={pd3['sigma_d']:.5f}", flush=True)

    print("  --- Goal 1: Emp | S2 med [q05, q95] | S3 med [q05, q95] ---", flush=True)
    for lbl, ev, s2v, s3v in [
        ("Turn top10", emp["turnover"]["10"], g1_s2["turnover"]["10"], g1_s3["turnover"]["10"]),
        ("Turn K/4", emp["turnover"][str(K // 4)], g1_s2["turnover"][str(K // 4)], g1_s3["turnover"][str(K // 4)]),
        ("Rank-1 chg", emp["occ_change"]["1"], g1_s2["occ_change"]["1"], g1_s3["occ_change"]["1"]),
        ("Head share", emp["head_share"], g1_s2["head_share"], g1_s3["head_share"]),
        ("Max X", emp["max_X"], g1_s2["max_X"], g1_s3["max_X"]),
        ("Boundary", emp["boundary_ratio"], g1_s2["boundary_ratio"], g1_s3["boundary_ratio"]),
        ("Dep rate", emp["dep_rate"], g1_s2["dep_rate"], g1_s3["dep_rate"]),
        ("Dep cross%", emp["dep_cross_frac"], g1_s2["dep_cross_frac"], g1_s3["dep_cross_frac"]),
        ("Gapped rho", emp["gapped_rho"], g1_s2["gapped_rho"], g1_s3["gapped_rho"]),
        ("Asym D", emp["D"], g1_s2["D"], g1_s3["D"]),
        ("  p_fall", emp["p_fall"], g1_s2["p_fall"], g1_s3["p_fall"]),
        ("  p_rise", emp["p_rise"], g1_s2["p_rise"], g1_s3["p_rise"]),
        ("  m21", emp["m21"], g1_s2["m21"], g1_s3["m21"]),
        ("  m12", emp["m12"], g1_s2["m12"], g1_s3["m12"]),
        ("  lev", emp["lev"], g1_s2["lev"], g1_s3["lev"]),
        ("  runs r2", emp["r2"], g1_s2["r2"], g1_s3["r2"]),
        ("  runs r3", emp["r3"], g1_s2["r3"], g1_s3["r3"]),
    ]:
        print(f"    {lbl:11s}: emp={ev:+.4f} | S2={s2v['median']:+.4f} [{s2v['q05']:+.3f},{s2v['q95']:+.3f}] | "
              f"S3={s3v['median']:+.4f} [{s3v['q05']:+.3f},{s3v['q95']:+.3f}]", flush=True)

    # Goal 2 head-to-head
    g2 = oos_head_to_head(pn, X, R, floors, K, cfg, sb if cfg["spec_b"] else None, n_ens=20)
    print("  --- Goal 2 (5 origins, calendar-aligned): S2 vs S3 vs baselines ---", flush=True)
    for h_s, m in g2["by_horizon"].items():
        print(f"    h={int(h_s):2d}: logR CRPS no={m['crps_lr_no']:.4f} rb={m['crps_lr_rb']:.4f} S2={m['crps_lr_s2']:.4f} S3={m['crps_lr_s3']:.4f} | "
              f"X CRPS S2={m['crps_X_s2']:.4f} S3={m['crps_X_s3']:.4f} cov90(S2/S3)={m['cov90_s2']:.2f}/{m['cov90_s3']:.2f} | "
              f"sh_r(S2/S3)={m['share_corr_s2']:.2f}/{m['share_corr_s3']:.2f}", flush=True)
    g2_2, g2_3 = g2["gate_v1_s2"], g2["gate_v1_s3"]
    if g2_2:
        print(f"    Gate v1: base={g2_2['base_rel']:.3f} | S2={g2_2['model_rel']:.3f} ({g2_2['wins']}/{g2_2['n_splits']}) | "
              f"S3={g2_3['model_rel']:.3f} ({g2_3['wins']}/{g2_3['n_splits']})", flush=True)

    res = dict(
        panel=pn, T=T, N=N, K=K,
        params_s2=pd2, params_s3=pd3,
        emp_structure=emp, g1_s2=g1_s2, g1_s3=g1_s3, g2=g2,
    )
    del X, R, floors, data
    gc.collect()
    return res


def main():
    panels = sys.argv[1:] if len(sys.argv) > 1 else list(PANELS.keys())
    out_path = OUT_DIR / "stage3_benchmark_results.json"
    all_res = json.loads(out_path.read_text()) if out_path.exists() and len(sys.argv) > 1 else {}
    for pn in panels:
        all_res[pn] = run_panel(pn, PANELS[pn], n_seeds=20)
        out_path.write_text(json.dumps(all_res, indent=2))
    print(f"\nSaved -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
