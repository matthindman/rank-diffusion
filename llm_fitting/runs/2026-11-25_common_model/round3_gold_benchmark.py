#!/usr/bin/env python3
"""Round 3 Gold Benchmark: Repaired Goal 1 + Goal 2 + True Out-of-Period Structure Test.

Implements every methodological repair identified by the independent Fable 5.2 and Astra 6.2
reviews and the Round 2A diagnostics:
  1. Goal 1 (20 paired seeds, all 4 DEVELOPMENT panels):
     - Canonical departure decomposition (§2z-ac):
         (i) immediate crossing (r_t <= K, K < r_{t+1} < inf, same ID),
        (ii) absent at t+1 returning within 13 weeks (same ID),
       (iii) terminal / >13w departure,
       plus legacy combined crossing-or-ever-return for reference.
     - Block-by-block trajectories of arithmetic head share and ladder spans (ranks 1, K/64, K/16, K/4).
     - Full 10-decile gapped persistence curve E[y | x decile].
     - Data-side entity-clustered bootstrap 90% CIs (200 replicates) alongside 20-seed model bands.
  2. Goal 2 (5 rolling origins x n_ens=39 continuous members):
     - Exact distribution-free 90% coverage via order statistics [x_(2), x_(38)] (expected 36/40 = 0.9000)
       with an internal Gaussian oracle self-check, plus randomized PIT (mean, SD, KS).
     - Single truth-defined common cohort across ALL models (no_change, local_level EWMA,
       rank_bin, zero_share_shift, Common-S2, Common-S3) — zero NaN-to-median collapse!
     - Unmasked top-K medians and totals (including reborn competitors) before masking departed IDs.
     - Both ensemble-median AND analytical Kalman-mean X MAE (centered at med_K[T0-1]).
     - Audience share-shift MAE & correlation reported separately for ALL top-K and HEAD top-K/16.
     - Fair Brier score: (p - y)^2 - p(1-p)/(M-1).
     - Full per-origin paired differences saved for paired win-counts and SEs.
  3. True Out-of-Period Structure Test (2019-20 -> 2021-22, 20 paired seeds, zero 2021-22 info):
     - Fit S2 and S3 on weeks 0..103 (2019-20), filter to end of 2020, roll forward 2021-22,
       and evaluate out-of-period aggregate structure on reddit_comments_ext & reddit_submissions_long.

Runs 100% sandboxed from the lean DEVELOPMENT scratch cache. Zero sealed data contact.
"""
from __future__ import annotations

import gc
import json
import math
import sys
import time
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


def fair_brier(y: np.ndarray, p_hat: np.ndarray, M: int) -> float:
    b = (p_hat - y) ** 2 - (p_hat * (1.0 - p_hat)) / max(M - 1, 1)
    return float(np.mean(b))


def exact_cov90_and_pit(y: np.ndarray, ens: np.ndarray, rng: np.random.Generator) -> dict:
    """For M=39 exchangeable continuous draws, [x_(2), x_(38)] (0-based idx 1 and 37) has
    exact distribution-free coverage 36/40 = 0.9000! Also computes randomized PIT stats."""
    M = ens.shape[1]
    assert M == 39, f"Expected M=39 for exact [x_(2), x_(38)] 90% coverage, got {M}"
    es = np.sort(ens, axis=1)
    lo = es[:, 1]
    hi = es[:, 37]
    cov90 = float(np.mean((y >= lo) & (y <= hi)))
    lt = np.sum(es < y[:, None], axis=1)
    eq = np.sum(es == y[:, None], axis=1)
    u = (lt + rng.random(y.size) * (eq + 1.0)) / (M + 1.0)
    us = np.sort(u)
    grid = (np.arange(1, u.size + 1) - 0.5) / max(u.size, 1)
    ks = float(np.max(np.abs(us - grid))) if u.size else np.nan
    return dict(cov90=cov90, pit_mean=float(np.mean(u)), pit_sd=float(np.std(u)), pit_ks=ks)


def run_gaussian_oracle_check() -> dict:
    rng = np.random.default_rng(20261125)
    y = rng.standard_normal(40000)
    ens = rng.standard_normal((40000, 39))
    res = exact_cov90_and_pit(y, ens, rng)
    assert abs(res["cov90"] - 0.90) < 0.01, f"Oracle cov90 failed: {res}"
    assert abs(res["pit_sd"] - 1.0 / math.sqrt(12.0)) < 0.01, f"Oracle PIT SD failed: {res}"
    print(f"[Oracle M=39 check] cov90={res['cov90']:.4f} (target 0.9000), "
          f"pit_mean={res['pit_mean']:.4f} (0.5000), pit_sd={res['pit_sd']:.4f} (0.2887), pit_ks={res['pit_ks']:.4f}",
          flush=True)
    return res


# --------------------------------------------------------------------------- #
# Goal 1: Upgraded Structure + Canonical Departures + Deciles + Cluster CIs
# --------------------------------------------------------------------------- #
def _gapped_windows_arrays(X: np.ndarray, R: np.ndarray, K: int, cfg: dict, ids: np.ndarray | None = None):
    """Collect per-window (entity_idx, xd, yd) arrays for asymmetry metrics & entity-cluster boot."""
    T, N = X.shape
    W, H, gap, step, pres = cfg["W"], cfg["H"], cfg["gap"], cfg["step"], cfg["pres"]
    floors = np.sum(np.isfinite(R), axis=1).astype(float) + 1.0
    pen_all = np.where(np.isfinite(R), R, floors[:, None])
    pr_all = np.mean(pen_all, axis=0)
    o_pr = np.argsort(pr_all, kind="stable")
    lab = np.full(N, -1, dtype=np.int32)
    lab[o_pr[:K]] = 2
    lab[o_pr[:K // 4]] = 1
    lab[o_pr[:K // 16]] = 0

    wins = []
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
            idx = np.flatnonzero(ok2)
            wins.append((idx, xd[idx], yd[idx]))
        a += step
    return wins, lab


def _asym_from_windows(wins: list[tuple[np.ndarray, np.ndarray, np.ndarray]],
                       weights_by_ent: np.ndarray | None = None) -> dict:
    rhos, Ds, pfs, prs, m21s, m12s, levs = [], [], [], [], [], [], []
    dec_y_list, dec_x_list = [], []
    for idx, xo, yo in wins:
        if weights_by_ent is not None:
            w = weights_by_ent[idx]
            keep = w > 0
            if np.sum(w[keep]) < 100:
                continue
            xo = np.repeat(xo[keep], w[keep])
            yo = np.repeat(yo[keep], w[keep])
        rhos.append(float(spearmanr(xo, yo).correlation))
        qs = np.quantile(xo, np.linspace(0.0, 1.0, 11))
        ql, qh = qs[1], qs[9]
        lo_m, hi_m = xo <= ql, xo >= qh
        pf = float(np.mean(yo[lo_m]) / np.mean(xo[lo_m]))
        pr_v = float(np.mean(yo[hi_m]) / np.mean(xo[hi_m]))
        pfs.append(pf)
        prs.append(pr_v)
        Ds.append(pf - pr_v)

        # 10-decile curve
        dx_d, dy_d = np.full(10, np.nan), np.full(10, np.nan)
        for d_i in range(10):
            if d_i == 0:
                m_d = xo <= qs[1]
            elif d_i == 9:
                m_d = xo >= qs[9]
            else:
                m_d = (xo > qs[d_i]) & (xo <= qs[d_i + 1])
            if np.any(m_d):
                dx_d[d_i] = float(np.mean(xo[m_d]))
                dy_d[d_i] = float(np.mean(yo[m_d]))
        dec_x_list.append(dx_d)
        dec_y_list.append(dy_d)

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

    return dict(
        gapped_rho=float(np.mean(rhos)) if rhos else np.nan,
        D=float(np.mean(Ds)) if Ds else np.nan,
        p_fall=float(np.mean(pfs)) if pfs else np.nan,
        p_rise=float(np.mean(prs)) if prs else np.nan,
        m21=float(np.mean(m21s)) if m21s else np.nan,
        m12=float(np.mean(m12s)) if m12s else np.nan,
        lev=float(np.mean(levs)) if levs else np.nan,
        dec_x=np.nanmean(dec_x_list, axis=0).tolist() if dec_x_list else [np.nan] * 10,
        dec_y=np.nanmean(dec_y_list, axis=0).tolist() if dec_y_list else [np.nan] * 10,
    )


def entity_cluster_ci(X: np.ndarray, R: np.ndarray, K: int, cfg: dict, n_boot: int = 200, seed: int = 77) -> dict:
    """Entity-clustered bootstrap 90% CIs on empirical gapped asymmetry metrics."""
    wins, _ = _gapped_windows_arrays(X, R, K, cfg, ids=None)
    if not wins:
        return {}
    N = X.shape[1]
    rng = np.random.default_rng(seed)
    active = np.unique(np.concatenate([w[0] for w in wins]))
    n_act = active.size
    keys = ["gapped_rho", "D", "p_fall", "p_rise", "m21", "m12", "lev"]
    boot_vals = {k: [] for k in keys}
    for _ in range(n_boot):
        w_ent = np.zeros(N, dtype=np.int32)
        draw = active[rng.integers(0, n_act, size=n_act)]
        np.add.at(w_ent, draw, 1)
        res = _asym_from_windows(wins, weights_by_ent=w_ent)
        for k in keys:
            if np.isfinite(res[k]):
                boot_vals[k].append(res[k])
    out = {}
    for k in keys:
        v = np.array(boot_vals[k], float)
        out[k] = dict(q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)), sd=float(np.std(v))) if v.size else {}
    return out


def structure_stats(X: np.ndarray, R: np.ndarray, K: int, cfg: dict, ids: np.ndarray | None = None) -> dict:
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

    # 3. Block-by-block ladder spans & arithmetic head shares
    lad_blocks = {str(k): [] for k in (1, K // 64, K // 16, K // 4)}
    head_share_blocks = []
    k64 = K // 64
    for b in range(nb):
        lo, hi = start + b * block, start + (b + 1) * block
        A_blk = np.where(np.isfinite(X[lo:hi]), np.expm1(np.clip(X[lo:hi], 0.0, 45.0)), 0.0)
        if tops[b].size >= K:
            hs_b = float(A_blk[:, tops[b][:k64]].sum() / max(A_blk[:, tops[b][:K]].sum(), 1.0))
            for k in (1, K // 64, K // 16, K // 4):
                lad_blocks[str(k)].append(float(M_blk[b, tops[b][k - 1]] - M_blk[b, tops[b][K - 1]]))
        else:
            hs_b = np.nan
            for k in (1, K // 64, K // 16, K // 4):
                lad_blocks[str(k)].append(np.nan)
        head_share_blocks.append(hs_b)

    lad_first = {k: v[0] for k, v in lad_blocks.items() if v}
    lad_last = {k: v[-1] for k, v in lad_blocks.items() if v}
    lad_change = {k: lad_last[k] - lad_first[k] for k in lad_first}
    head_share = head_share_blocks[-1] if head_share_blocks else np.nan
    max_X = float(np.nanmax(X))

    # 4. Crossing ratio (boundary_ratio)
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

    # 5. Canonical departure decomposition (§2z-ac) for established cohort (ord1 <= K//2)
    mid = T // 2
    floors = np.sum(np.isfinite(R), axis=1).astype(float) + 1.0
    pen1 = np.where(np.isfinite(R[:mid]), R[:mid], floors[:mid, None])
    rperm1 = np.mean(pen1, axis=0)
    ord1 = np.empty(N, dtype=float)
    ord1[np.argsort(rperm1, kind="stable")] = np.arange(1, N + 1)
    est_coh = ord1 <= (K // 2)
    R2 = R[mid:, est_coh]
    T2, M2 = R2.shape
    if ids is not None:
        id_ref = ids[mid - 1, est_coh]
        alive2 = ids[mid:, est_coh] == id_ref[None, :]
    else:
        alive2 = np.ones((T2, M2), dtype=bool)
    obs_alive = alive2 & np.isfinite(R2)
    inK2 = obs_alive & (R2 <= K)
    still_obs_below = obs_alive & (R2 > K)
    ex_ev = inK2[:-1] & (~inK2[1:])
    at_risk = inK2[:-1]
    n_ex = int(np.sum(ex_ev))
    dep_rate = float(n_ex / max(int(np.sum(at_risk)), 1))

    # Immediate crossing at t+1 (canonical §2z-ac)
    imm_cross = ex_ev & still_obs_below[1:]
    # Absent at t+1 (not observed at all at t+1), returns within next 13 weeks (t+2 .. t+14)
    abs_at_next = ex_ev & (~obs_alive[1:])
    ret13 = np.zeros_like(ex_ev)
    ever_after = np.zeros_like(ex_ev)
    for tt in range(T2 - 1):
        hi_w = min(T2, tt + 15)
        if tt + 2 < hi_w:
            ret13[tt] = np.any(obs_alive[tt + 2:hi_w], axis=0)
        if tt + 1 < T2:
            ever_after[tt] = np.any(obs_alive[tt + 1:], axis=0)
    dep_imm_cross_frac = float(np.sum(imm_cross) / max(n_ex, 1)) if n_ex else np.nan
    dep_return13_frac = float(np.sum(abs_at_next & ret13) / max(n_ex, 1)) if n_ex else np.nan
    dep_terminal_frac = float(np.sum(abs_at_next & (~ret13)) / max(n_ex, 1)) if n_ex else np.nan
    dep_legacy_cross_or_ret = float(np.sum(ex_ev & (still_obs_below[1:] | ever_after)) / max(n_ex, 1)) if n_ex else np.nan

    # 6. Gapped asymmetry + 10-decile curve
    wins, lab = _gapped_windows_arrays(X, R, K, cfg, ids=ids)
    asym = _asym_from_windows(wins)

    # 7. Consecutive decline-vs-gain run ratios
    rb_len = cfg["run_blk"]
    nb_r = T // rb_len
    r2_ratio, r3_ratio = np.nan, np.nan
    if nb_r >= 4:
        st_r = T - nb_r * rb_len
        sel_k = lab >= 0
        Br = np.array([wmean(X[:, sel_k], st_r + b * rb_len, st_r + (b + 1) * rb_len, min(pres, 0.75)) for b in range(nb_r)])
        dB = np.diff(Br, axis=0)
        dB = dB - np.nanmean(dB, axis=1, keepdims=True)
        ok2r = np.isfinite(dB[:-1]) & np.isfinite(dB[1:])
        r2_ratio = float(np.sum(ok2r & (dB[:-1] < 0) & (dB[1:] < 0)) / max(np.sum(ok2r & (dB[:-1] > 0) & (dB[1:] > 0)), 1))
        if dB.shape[0] >= 3:
            ok3r = np.isfinite(dB[:-2]) & np.isfinite(dB[1:-1]) & np.isfinite(dB[2:])
            r3_ratio = float(np.sum(ok3r & (dB[:-2] < 0) & (dB[1:-1] < 0) & (dB[2:] < 0)) / max(np.sum(ok3r & (dB[:-2] > 0) & (dB[1:-1] > 0) & (dB[2:] > 0)), 1))

    return dict(
        turnover=turnover,
        occ_change=occ,
        lad_first=lad_first,
        lad_last=lad_last,
        lad_change=lad_change,
        lad_blocks=lad_blocks,
        head_share=head_share,
        head_share_blocks=head_share_blocks,
        max_X=max_X,
        boundary_ratio=boundary_ratio,
        dep_rate=dep_rate,
        dep_imm_cross_frac=dep_imm_cross_frac,
        dep_return13_frac=dep_return13_frac,
        dep_terminal_frac=dep_terminal_frac,
        dep_legacy_cross_or_ret=dep_legacy_cross_or_ret,
        r2=float(r2_ratio),
        r3=float(r3_ratio),
        **asym,
    )


def _band_agg(dicts: list[dict], key: str) -> dict:
    v = np.array([d[key] for d in dicts if np.isfinite(d[key])], float)
    if not v.size:
        return dict(mean=np.nan, median=np.nan, q05=np.nan, q95=np.nan)
    return dict(mean=float(np.mean(v)), median=float(np.median(v)),
                q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)))


def _list_band_agg(list_of_lists: list[list[float]]) -> list[dict]:
    arr = np.array(list_of_lists, dtype=float)
    out = []
    for j in range(arr.shape[1]):
        v = arr[:, j]
        v = v[np.isfinite(v)]
        if not v.size:
            out.append(dict(mean=np.nan, median=np.nan, q05=np.nan, q95=np.nan))
        else:
            out.append(dict(mean=float(np.mean(v)), median=float(np.median(v)),
                            q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95))))
    return out


def _agg_structure_sims(sim_stats: list[dict]) -> dict:
    scalar_keys = [
        "head_share", "max_X", "boundary_ratio",
        "dep_rate", "dep_imm_cross_frac", "dep_return13_frac", "dep_terminal_frac", "dep_legacy_cross_or_ret",
        "gapped_rho", "D", "p_fall", "p_rise", "m21", "m12", "lev", "r2", "r3",
    ]
    agg = {k: _band_agg(sim_stats, k) for k in scalar_keys}
    for dkey in ("turnover", "occ_change", "lad_first", "lad_last", "lad_change"):
        agg[dkey] = {
            k: _band_agg([dict(v=s[dkey][k]) for s in sim_stats], "v")
            for k in sim_stats[0][dkey]
        }
    agg["head_share_blocks"] = _list_band_agg([s["head_share_blocks"] for s in sim_stats])
    agg["lad_blocks"] = {k: _list_band_agg([s["lad_blocks"][k] for s in sim_stats]) for k in sim_stats[0]["lad_blocks"]}
    agg["dec_x"] = _list_band_agg([s["dec_x"] for s in sim_stats])
    agg["dec_y"] = _list_band_agg([s["dec_y"] for s in sim_stats])
    return agg


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
    return p, _agg_structure_sims(sim_stats)


# --------------------------------------------------------------------------- #
# Goal 2: Exact Single-Pass Forecaster + Common Cohort + M=39 Exact Coverage
# --------------------------------------------------------------------------- #
def _forecast_exact_single_pass(
    filt: dict,
    p: cr.CommonParams,
    horizons: list[int],
    n_ens: int,
    seed: int,
) -> dict[int, dict]:
    """Single-pass calendar-aligned forecast returning BOTH:
      - Observed (absence+exit masked) R, S, med_K, tot_K (with med_K/tot_K including reborn competitors!)
      - Conditional-on-present X_cond_rel (x_unmasked - med_K_obs), zero NaN-to-median collapse!
        For the rare (<0.5%) draw where an original top-K entity drifted below K and triggered lam_exit,
        runs a clean conditional-on-surviving (lam_exit=0) draw for that slot so all M=39 draws are
        genuine continuous draws from P(X_{t+h} - med_K(t+h) | observed at t+h).
    """
    H_max = max(horizons)
    hor_set = set(horizons)
    L_hat, xi_hat = filt["L_hat"], filt["xi_hat"]
    P11, P12, P22 = filt["P11"], filt["P12"], filt["P22"]
    v_hat, post_s_v = filt["v_hat"], filt["post_s_v"]
    N = L_hat.size

    use_4 = "P4" in filt
    if use_4:
        chol4 = np.linalg.cholesky(filt["P4"] + 1e-10 * np.eye(4)[None, :, :])
        d_hat, s_hat = filt["d_hat"], filt["s_hat"]
        s_sc = filt.get("s_scale", np.ones(N))
        has_d = p.decline_var > 0.0
        has_s = (p.surge_lam > 0.0) and (p.surge_mu > 0.0) and (p.surge_rho > 0.0)
        sd_d = math.sqrt(p.decline_var) if has_d else 0.0
        s_mean = cr._surge_mean(p.surge_lam, p.surge_mu, p.surge_rho) if has_s else 0.0
    else:
        c11 = np.sqrt(np.maximum(P11, 1e-10))
        c21 = P12 / c11
        c22 = np.sqrt(np.maximum(P22 - c21 * c21, 1e-10))

    out_X_cond = {h: np.empty((N, n_ens), dtype=np.float64) for h in horizons}
    out_R = {h: np.full((N, n_ens), np.nan, dtype=np.float64) for h in horizons}
    out_S = {h: np.zeros((N, n_ens), dtype=np.float64) for h in horizons}
    out_exited = {h: np.zeros((N, n_ens), dtype=bool) for h in horizons}
    p_noexit = cr.replace(p, lam_exit=0.0)

    def _draw_init(rng: np.random.Generator):
        if use_4:
            draw = np.einsum("nij,nj->ni", chol4, rng.standard_normal((N, 4)))
            L_i = L_hat + draw[:, 0]
            xi_i = xi_hat + draw[:, 1]
            d_i = np.clip(d_hat + draw[:, 2], -2.8 * sd_d, 2.8 * sd_d) if has_d else None
            if has_s:
                s_unit_raw = (s_hat + draw[:, 3]) / np.maximum(s_sc, 1e-4) + s_mean
                s_i = np.maximum(s_unit_raw, 0.0)
            else:
                s_i = None
        else:
            z1, z2 = rng.standard_normal(N), rng.standard_normal(N)
            L_i = L_hat + c11 * z1
            xi_i = xi_hat + c21 * z1 + c22 * z2
            d_i, s_i = None, None
        if p.s_vol > 0.02:
            lv = np.log(v_hat) - 0.5 * (post_s_v ** 2) + post_s_v * np.clip(rng.standard_normal(N), -2.0, 2.0)
            v_i = np.exp(np.clip(lv, -2.5 * p.s_vol, 2.5 * p.s_vol))
        else:
            v_i = np.ones(N)
        return L_i, xi_i, v_i, d_i, s_i

    orig_ids = np.arange(N, dtype=np.int64)
    for m in range(n_ens):
        rng = np.random.default_rng(seed + m)
        L_i, xi_i, v_i, d_i, s_i = _draw_init(rng)
        st = cr.CommonState(p, L_i, rng, xi0=xi_i, v0=v_i, d0=d_i, s0=s_i)
        # Parallel surviving-only clone ONLY used if an original slot triggers lam_exit
        rng_surv = np.random.default_rng(seed + 100000 + m)
        L_s, xi_s, v_s, d_s, s_s = _draw_init(rng_surv)
        st_surv = cr.CommonState(p_noexit, L_s, rng_surv, xi0=xi_s, v0=v_s, d0=d_s, s0=s_s)

        for step_h in range(1, H_max + 1):
            x_unm = st.step(apply_absence=False)
            x_surv = st_surv.step(apply_absence=False)
            # Apply exact logistic absence mask independently of observation noise
            if p.a_abs > -7.5:
                _, zp = st._ranks_and_z()
                absent = rng.random(N) < cr.p_abs_of(p, zp)
                x_obs = np.where(absent, np.nan, x_unm)
            else:
                x_obs = x_unm

            if step_h in hor_set:
                alive = st.ids == orig_ids
                ok = np.flatnonzero(np.isfinite(x_obs))
                if ok.size:
                    ord_ok = ok[np.argsort(-x_obs[ok], kind="stable")]
                    rk = np.full(N, np.nan)
                    rk[ord_ok] = np.arange(1, ord_ok.size + 1, dtype=float)
                    out_R[step_h][:, m] = np.where(alive, rk, np.nan)
                    top_k_idx = ord_ok[:min(p.K, ord_ok.size)]
                    med_m = float(np.median(x_obs[top_k_idx]))
                    am_all = np.where(np.isfinite(x_obs), np.expm1(np.clip(x_obs, 0.0, 40.0)), 0.0)
                    tot_m = max(float(np.sum(am_all[top_k_idx])), 1.0)
                    out_S[step_h][:, m] = np.where(alive, am_all / tot_m, 0.0)
                else:
                    med_m = float(np.nanmedian(x_unm))
                # Conditional-on-present X relative to observed top-K median
                x_cond = np.where(alive, x_unm, x_surv) - med_m
                out_X_cond[step_h][:, m] = x_cond
                out_exited[step_h][:, m] = ~alive

    return {h: dict(X_cond=out_X_cond[h], R=out_R[h], S=out_S[h], exited=out_exited[h]) for h in horizons}


def _analytical_kalman_point(filt: dict, p: cr.CommonParams, h: int, med_last: float) -> np.ndarray:
    """Analytical conditional mean E[X_{T0-1+h} | F_{T0-1}] - med_K[T0-1]."""
    pred = filt["L_hat"] + (p.phi ** h) * filt["xi_hat"]
    if "d_hat" in filt and p.decline_var > 0.0:
        rho_d = float(p.decline_rho)
        geom = h if abs(1.0 - rho_d) < 1e-9 else (1.0 - rho_d ** h) / (1.0 - rho_d)
        pred = pred + geom * filt["d_hat"]
    if "s_hat" in filt and p.surge_lam > 0.0 and p.surge_rho > 0.0:
        pred = pred + (p.surge_rho ** h) * filt["s_hat"]
    return pred - med_last


def _tune_local_level_ewma(Xr_tr: np.ndarray, topK: np.ndarray) -> tuple[float, np.ndarray]:
    """Train-only tuned EWMA local-level baseline on centered Xr_tr."""
    T0, N = Xr_tr.shape
    eval_start = max(4, T0 // 2)
    best_a, best_mae = 0.35, 1e9
    for a in np.linspace(0.10, 0.90, 17):
        st = np.full(N, np.nan)
        errs = []
        for t in range(T0):
            obs = np.isfinite(Xr_tr[t])
            if t >= eval_start:
                ok = obs[topK] & np.isfinite(st[topK])
                if ok.any():
                    errs.append(float(np.mean(np.abs(Xr_tr[t, topK[ok]] - st[topK[ok]]))))
            st = np.where(obs, np.where(np.isfinite(st), a * Xr_tr[t] + (1.0 - a) * st, Xr_tr[t]), st)
        if errs:
            m = float(np.mean(errs))
            if m < best_mae:
                best_mae, best_a = m, float(a)
    # Final filtered state at T0 - 1
    st = np.full(N, np.nan)
    for t in range(T0):
        obs = np.isfinite(Xr_tr[t])
        st = np.where(obs, np.where(np.isfinite(st), best_a * Xr_tr[t] + (1.0 - best_a) * st, Xr_tr[t]), st)
    return best_a, st


def oos_goal2_gold(pn: str, X: np.ndarray, R: np.ndarray, floors: np.ndarray, K: int,
                   cfg: dict, spec_b: tuple[np.ndarray, np.ndarray] | None, n_ens: int = 39) -> dict:
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
    pit_rng = np.random.default_rng(12345)

    for split_i, T0 in enumerate(origins):
        X_tr, R_tr, fl_tr = X[:T0], R[:T0], floors[:T0]
        p_s2 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=spec_b if cfg["spec_b"] else None,
                                  refine_drift=True, asymmetry=False)
        p_s3 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=spec_b if cfg["spec_b"] else None,
                                  refine_drift=True, asymmetry=True)
        filt_s2 = cr.filter_endpoints(X_tr, R_tr, p_s2, floors_tr=fl_tr)
        filt_s3 = cr.filter_endpoints(X_tr, R_tr, p_s3, floors_tr=fl_tr)

        pen_tr = np.where(np.isfinite(R_tr), R_tr, fl_tr[:, None])
        rperm_tr = np.mean(pen_tr, axis=0)
        pres_tr = np.isfinite(R_tr).mean(axis=0)
        ord_tr = np.argsort(rperm_tr, kind="stable")
        topK = ord_tr[:K]
        if pn == "instagram_hm":
            topK = topK[pres_tr[topK] >= 0.5]

        # Baselines: last observed Xr and train-tuned EWMA local-level
        last_Xr = np.full(N, np.nan)
        for t in range(T0):
            f = np.isfinite(Xr[t])
            last_Xr[f] = Xr[t, f]
        alpha_ll, ewma_Xr = _tune_local_level_ewma(Xr[:T0], topK)
        # Share implied by local-level EWMA
        ewma_X_abs = ewma_Xr + med_K[T0 - 1]
        ewma_A = np.where(np.isfinite(ewma_X_abs), np.expm1(np.clip(ewma_X_abs, 0.0, 40.0)), 0.0)
        ord_ewma = np.argsort(-ewma_A)
        ewma_totK = max(float(np.sum(ewma_A[ord_ewma[:K]])), 1.0)
        ewma_S = ewma_A / ewma_totK

        fc_s2 = _forecast_exact_single_pass(filt_s2, p_s2, horizons, n_ens=n_ens, seed=2000 * split_i)
        fc_s3 = _forecast_exact_single_pass(filt_s3, p_s3, horizons, n_ens=n_ens, seed=2000 * split_i)

        r_now = R[T0 - 1]
        logr_now = np.log(np.where(np.isfinite(r_now) & (r_now <= K), r_now, K + 1.0))
        k_hd = K // 16
        head_mask = np.isfinite(r_now[topK]) & (r_now[topK] <= k_hd)
        near_hd = np.isfinite(r_now[topK]) & (r_now[topK] <= K // 4)

        for h in horizons:
            t_fut = T0 - 1 + h
            if t_fut >= T:
                continue
            yt_X = Xr[t_fut, topK]
            rt = R[t_fut, topK]
            logr_true = np.log(np.where(np.isfinite(rt) & (rt <= K), rt, K + 1.0))

            # Single truth-defined common cohort for X metrics (IDENTICAL across ALL models!)
            ok_x = np.isfinite(yt_X) & np.isfinite(last_Xr[topK]) & np.isfinite(ewma_Xr[topK])

            # Rank-bin empirical transition baseline (M=39)
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

            s_now = S_share[T0 - 1, topK]
            s_true = S_share[t_fut, topK]
            ds_true = s_true - s_now

            def score_one(fc_h: dict, filt_m: dict, p_m: cr.CommonParams) -> dict:
                Xc_top = fc_h["X_cond"][topK]
                Re_top = fc_h["R"][topK]
                Se_top = fc_h["S"][topK]
                exit_frac = float(np.mean(fc_h["exited"][topK[ok_x]]))

                lre = np.log(np.where(np.isfinite(Re_top) & (Re_top <= K), Re_top, K + 1.0))
                crps_X = float(np.mean(fair_ensemble_crps(yt_X[ok_x], Xc_top[ok_x])))
                mae_ens = float(np.mean(np.abs(yt_X[ok_x] - np.median(Xc_top[ok_x], axis=1))))
                kal_pt = _analytical_kalman_point(filt_m, p_m, h, med_K[T0 - 1])[topK]
                mae_kal = float(np.mean(np.abs(yt_X[ok_x] - kal_pt[ok_x])))
                cal = exact_cov90_and_pit(yt_X[ok_x], Xc_top[ok_x], pit_rng)
                crps_lr = float(np.mean(fair_ensemble_crps(logr_true, lre)))

                # Audience share-shift (all top-K AND head top-K/16 separately!)
                ds_hat_med = np.median(Se_top, axis=1) - s_now
                ds_hat_mean = np.mean(Se_top, axis=1) - s_now
                sh_mae_all = float(np.mean(np.abs(ds_true - ds_hat_med)))
                sh_corr_all = float(np.corrcoef(ds_true, ds_hat_med)[0, 1]) if np.std(ds_hat_med) > 1e-12 else 0.0
                sh_mae_hd = float(np.mean(np.abs(ds_true[head_mask] - ds_hat_med[head_mask]))) if head_mask.any() else np.nan
                sh_mae_hd_mn = float(np.mean(np.abs(ds_true[head_mask] - ds_hat_mean[head_mask]))) if head_mask.any() else np.nan
                sh_corr_hd = (float(np.corrcoef(ds_true[head_mask], ds_hat_med[head_mask])[0, 1])
                              if head_mask.sum() > 5 and np.std(ds_hat_med[head_mask]) > 1e-12 else 0.0)

                # Fair Brier
                y_hd = (np.isfinite(rt) & (rt <= k_hd)).astype(float)
                p_hd = np.mean(lre <= np.log(k_hd + 0.5), axis=1)
                brier_hd = fair_brier(y_hd[near_hd], p_hd[near_hd], n_ens)
                y_inK = (np.isfinite(rt) & (rt <= K)).astype(float)
                p_inK = np.mean(lre <= np.log(K + 0.5), axis=1)
                brier_K = fair_brier(y_inK, p_inK, n_ens)

                return dict(
                    crps_X=crps_X, mae_ens=mae_ens, mae_kal=mae_kal,
                    cov90=cal["cov90"], pit_mean=cal["pit_mean"], pit_sd=cal["pit_sd"], pit_ks=cal["pit_ks"],
                    crps_lr=crps_lr,
                    share_mae_all=sh_mae_all, share_corr_all=sh_corr_all,
                    share_mae_hd=sh_mae_hd, share_mae_hd_mean=sh_mae_hd_mn, share_corr_hd=sh_corr_hd,
                    brier_hd=brier_hd, brier_K=brier_K, exit_frac=exit_frac,
                )

            m2 = score_one(fc_s2[h], filt_s2, p_s2)
            m3 = score_one(fc_s3[h], filt_s3, p_s3)

            # Baselines on EXACT same ok_x, head_mask, near_hd, topK
            mae_no = float(np.mean(np.abs(yt_X[ok_x] - last_Xr[topK[ok_x]])))
            mae_ll = float(np.mean(np.abs(yt_X[ok_x] - ewma_Xr[topK[ok_x]])))
            crps_lr_no = float(np.mean(np.abs(logr_true - logr_now[topK])))
            crps_lr_rb = float(np.mean(fair_ensemble_crps(logr_true, rb_ens)))
            sh_mae_no_all = float(np.mean(np.abs(ds_true)))
            sh_mae_no_hd = float(np.mean(np.abs(ds_true[head_mask]))) if head_mask.any() else np.nan
            ds_ll = ewma_S[topK] - s_now
            sh_mae_ll_all = float(np.mean(np.abs(ds_true - ds_ll)))
            sh_mae_ll_hd = float(np.mean(np.abs(ds_true[head_mask] - ds_ll[head_mask]))) if head_mask.any() else np.nan
            sh_corr_ll_hd = (float(np.corrcoef(ds_true[head_mask], ds_ll[head_mask])[0, 1])
                             if head_mask.sum() > 5 and np.std(ds_ll[head_mask]) > 1e-12 else 0.0)
            y_hd = (np.isfinite(rt) & (rt <= k_hd)).astype(float)
            p_hd_rb = np.mean(rb_ens <= np.log(k_hd + 0.5), axis=1)
            brier_hd_rb = fair_brier(y_hd[near_hd], p_hd_rb[near_hd], n_ens)
            y_inK = (np.isfinite(rt) & (rt <= K)).astype(float)
            p_inK_rb = np.mean(rb_ens <= np.log(K + 0.5), axis=1)
            brier_K_rb = fair_brier(y_inK, p_inK_rb, n_ens)

            row = dict(
                T0=int(T0), n_ok_x=int(ok_x.sum()), n_topK=int(topK.size), n_head=int(head_mask.sum()),
                alpha_ll=alpha_ll,
                mae_no=mae_no, mae_ll=mae_ll,
                crps_lr_no=crps_lr_no, crps_lr_rb=crps_lr_rb,
                share_mae_no_all=sh_mae_no_all, share_mae_no_hd=sh_mae_no_hd,
                share_mae_ll_all=sh_mae_ll_all, share_mae_ll_hd=sh_mae_ll_hd, share_corr_ll_hd=sh_corr_ll_hd,
                brier_hd_rb=brier_hd_rb, brier_K_rb=brier_K_rb,
            )
            for k, v in m2.items():
                row[f"{k}_s2"] = v
            for k, v in m3.items():
                row[f"{k}_s3"] = v
            by_h[str(h)].append(row)

    agg_h = {}
    for h_s, rows in by_h.items():
        if not rows:
            continue
        keys = [k for k in rows[0] if k != "T0"]
        d_agg = {k: float(np.mean([r[k] for r in rows])) for k in keys}
        d_agg["n_origins"] = len(rows)
        # Paired origin-level comparisons
        for name, a_k, b_k in [
            ("s3_vs_s2_crps_X", "crps_X_s3", "crps_X_s2"),
            ("s3_vs_s2_crps_lr", "crps_lr_s3", "crps_lr_s2"),
            ("s2_vs_ll_mae_kal", "mae_kal_s2", "mae_ll"),
            ("s3_vs_ll_mae_kal", "mae_kal_s3", "mae_ll"),
            ("s2_vs_rb_crps_lr", "crps_lr_s2", "crps_lr_rb"),
            ("s3_vs_rb_crps_lr", "crps_lr_s3", "crps_lr_rb"),
            ("s2_vs_no_share_hd", "share_mae_hd_s2", "share_mae_no_hd"),
            ("s3_vs_no_share_hd", "share_mae_hd_s3", "share_mae_no_hd"),
            ("s2_vs_ll_share_hd", "share_mae_hd_s2", "share_mae_ll_hd"),
            ("s3_vs_ll_share_hd", "share_mae_hd_s3", "share_mae_ll_hd"),
        ]:
            diffs = np.array([r[a_k] - r[b_k] for r in rows], float)
            d_agg[f"diff_{name}_mean"] = float(np.mean(diffs))
            d_agg[f"diff_{name}_sd"] = float(np.std(diffs))
            d_agg[f"wins_{name}"] = int(np.sum(diffs < 0.0))
        d_agg["by_origin"] = rows
        agg_h[h_s] = d_agg
    return dict(origins=origins, n_ens=n_ens, by_horizon=agg_h)


# --------------------------------------------------------------------------- #
# True Out-of-Period Structure Test: 2019-20 (weeks 0..103) -> 2021-22
# --------------------------------------------------------------------------- #
def out_of_period_structure_test(pn: str, X: np.ndarray, R: np.ndarray, floors: np.ndarray, K: int,
                                 cfg: dict, n_seeds: int = 20) -> dict:
    """Fit strictly on first 104 weeks (2019-20), filter to end of 2020, roll forward
    through 2021-22 (zero 2021-22 info!), and compare simulated 2021-22 structure against
    observed 2021-22 and observed 2019-20."""
    T, N = X.shape
    T_tr = 104
    T_te = T - T_tr
    cfg_oop = dict(cfg)
    cfg_oop["block"] = 52
    cfg_oop["H"] = 26
    cfg_oop["W"] = 8
    cfg_oop["gap"] = 8
    cfg_oop["step"] = 8
    cfg_oop["run_blk"] = 26

    emp_2019_20 = structure_stats(X[:T_tr], R[:T_tr], K, cfg_oop)
    emp_2021_22 = structure_stats(X[T_tr:], R[T_tr:], K, cfg_oop)

    X_tr, R_tr, fl_tr = X[:T_tr], R[:T_tr], floors[:T_tr]
    out = dict(T_tr=T_tr, T_te=T_te, emp_2019_20=emp_2019_20, emp_2021_22=emp_2021_22)

    for label, asym in [("S2", False), ("S3", True)]:
        p = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=None, refine_drift=True, asymmetry=asym)
        filt = cr.filter_endpoints(X_tr, R_tr, p, floors_tr=fl_tr)
        use_4 = "P4" in filt
        if use_4:
            chol4 = np.linalg.cholesky(filt["P4"] + 1e-10 * np.eye(4)[None, :, :])
            has_d = p.decline_var > 0.0
            has_s = (p.surge_lam > 0.0) and (p.surge_mu > 0.0) and (p.surge_rho > 0.0)
            sd_d = math.sqrt(p.decline_var) if has_d else 0.0
            s_mean = cr._surge_mean(p.surge_lam, p.surge_mu, p.surge_rho) if has_s else 0.0
            s_sc = filt.get("s_scale", np.ones(N))
        else:
            c11 = np.sqrt(np.maximum(filt["P11"], 1e-10))
            c21 = filt["P12"] / c11
            c22 = np.sqrt(np.maximum(filt["P22"] - c21 * c21, 1e-10))

        sim_stats = []
        for sd in range(n_seeds):
            rng = np.random.default_rng(7000 + sd)
            if use_4:
                draw = np.einsum("nij,nj->ni", chol4, rng.standard_normal((N, 4)))
                L_i = filt["L_hat"] + draw[:, 0]
                xi_i = filt["xi_hat"] + draw[:, 1]
                d_i = np.clip(filt["d_hat"] + draw[:, 2], -2.8 * sd_d, 2.8 * sd_d) if has_d else None
                if has_s:
                    s_unit_raw = (filt["s_hat"] + draw[:, 3]) / np.maximum(s_sc, 1e-4) + s_mean
                    s_i = np.maximum(s_unit_raw, 0.0)
                else:
                    s_i = None
            else:
                z1, z2 = rng.standard_normal(N), rng.standard_normal(N)
                L_i = filt["L_hat"] + c11 * z1
                xi_i = filt["xi_hat"] + c21 * z1 + c22 * z2
                d_i, s_i = None, None
            if p.s_vol > 0.02:
                lv = np.log(filt["v_hat"]) - 0.5 * (filt["post_s_v"] ** 2) + filt["post_s_v"] * np.clip(rng.standard_normal(N), -2.0, 2.0)
                v_i = np.exp(np.clip(lv, -2.5 * p.s_vol, 2.5 * p.s_vol))
            else:
                v_i = np.ones(N)
            st = cr.CommonState(p, L_i, rng, xi0=xi_i, v0=v_i, d0=d_i, s0=s_i)
            Xs = np.empty((T_te, N), dtype=np.float64)
            Rs = np.full((T_te, N), np.nan, dtype=np.float64)
            Ids = np.empty((T_te, N), dtype=np.int64)
            for tt in range(T_te):
                xt = st.step(apply_absence=True)
                Xs[tt] = xt
                Ids[tt] = st.ids
                ok = np.flatnonzero(np.isfinite(xt))
                if ok.size:
                    oo = ok[np.argsort(-xt[ok], kind="stable")]
                    Rs[tt, oo] = np.arange(1, oo.size + 1, dtype=float)
            sim_stats.append(structure_stats(Xs, Rs, K, cfg_oop, ids=Ids))
        out[label] = dict(params=_param_dict(p), sim=_agg_structure_sims(sim_stats))
    return out


def _param_dict(p: cr.CommonParams) -> dict:
    return dict(
        n_scalars=p.n_free_scalars(), spec_b_pinned=p.spec_b_pinned,
        sigma_L0=p.sigma_L0, beta=p.beta, phi=p.phi,
        a_xi=p.a_xi, b_xi=p.b_xi, a_obs=p.a_obs, b_obs=p.b_obs,
        a_obs_floor=p.a_obs_floor, b_obs_floor=p.b_obs_floor,
        s_vol=p.s_vol, gamma_L=p.gamma_L, a_abs=p.a_abs, b_abs=p.b_abs, lam_exit=p.lam_exit,
        surge_lam=p.surge_lam, surge_mu=p.surge_mu, surge_rho=p.surge_rho,
        surge_hl=float(-math.log(2.0) / math.log(p.surge_rho)) if p.surge_rho > 0 else 0.0,
        surge_Vs=cr._surge_variance(p.surge_lam, p.surge_mu, p.surge_rho),
        sigma_d=math.sqrt(p.decline_var) if p.decline_var > 0 else 0.0,
        decline_rho=p.decline_rho,
    )


def run_panel(pn: str, cfg: dict, n_seeds: int = 20, n_ens: int = 39) -> dict:
    t0 = time.time()
    print(f"\n==================== ROUND 3 GOLD BENCHMARK: {pn} ====================", flush=True)
    data, sb = load_cached(pn)
    X = data["X"].astype(np.float64)
    R = data["R"].astype(np.float64)
    floors = data["floors"].astype(np.float64)
    K = int(data["K"])

    emp = structure_stats(X, R, K, cfg)
    emp_ci = entity_cluster_ci(X, R, K, cfg, n_boot=200)
    p2, g1_s2 = _fit_and_sim_g1(X, R, floors, K, cfg, sb, asymmetry=False, n_seeds=n_seeds)
    p3, g1_s3 = _fit_and_sim_g1(X, R, floors, K, cfg, sb, asymmetry=True, n_seeds=n_seeds)
    pd2, pd3 = _param_dict(p2), _param_dict(p3)

    print(f"  S2 ({pd2['n_scalars']} sc): sL0={pd2['sigma_L0']:.4f} beta={pd2['beta']:+.3f} phi={pd2['phi']:.2f} "
          f"xi=({pd2['a_xi']:+.2f},{pd2['b_xi']:+.2f}) obs=({pd2['a_obs']:+.2f},{pd2['b_obs']:+.2f}) s={pd2['s_vol']:.3f}", flush=True)
    print(f"  S3 ({pd3['n_scalars']} sc): sL0={pd3['sigma_L0']:.4f} beta={pd3['beta']:+.3f} phi={pd3['phi']:.2f} "
          f"xi=({pd3['a_xi']:+.2f},{pd3['b_xi']:+.2f}) obs=({pd3['a_obs']:+.2f},{pd3['b_obs']:+.2f}) s={pd3['s_vol']:.3f} | "
          f"surge(hl={pd3['surge_hl']:.0f},lam={pd3['surge_lam']:.5f},mu={pd3['surge_mu']:.2f},Vs={pd3['surge_Vs']:.3f}) "
          f"sigma_d={pd3['sigma_d']:.5f} (hl_d=52)", flush=True)

    print("  --- Goal 1: Emp (cluster 90% CI) | S2 med [q05,q95] | S3 med [q05,q95] ---", flush=True)
    for lbl, ev, s2v, s3v, ci_k in [
        ("Turn top10", emp["turnover"]["10"], g1_s2["turnover"]["10"], g1_s3["turnover"]["10"], None),
        ("Turn K/4", emp["turnover"][str(K // 4)], g1_s2["turnover"][str(K // 4)], g1_s3["turnover"][str(K // 4)], None),
        ("Rank-1 chg", emp["occ_change"]["1"], g1_s2["occ_change"]["1"], g1_s3["occ_change"]["1"], None),
        ("Head share", emp["head_share"], g1_s2["head_share"], g1_s3["head_share"], None),
        ("Max X", emp["max_X"], g1_s2["max_X"], g1_s3["max_X"], None),
        ("Boundary", emp["boundary_ratio"], g1_s2["boundary_ratio"], g1_s3["boundary_ratio"], None),
        ("Dep rate", emp["dep_rate"], g1_s2["dep_rate"], g1_s3["dep_rate"], None),
        ("Dep imm%", emp["dep_imm_cross_frac"], g1_s2["dep_imm_cross_frac"], g1_s3["dep_imm_cross_frac"], None),
        ("Dep ret13%", emp["dep_return13_frac"], g1_s2["dep_return13_frac"], g1_s3["dep_return13_frac"], None),
        ("Dep term%", emp["dep_terminal_frac"], g1_s2["dep_terminal_frac"], g1_s3["dep_terminal_frac"], None),
        ("Gapped rho", emp["gapped_rho"], g1_s2["gapped_rho"], g1_s3["gapped_rho"], "gapped_rho"),
        ("Asym D", emp["D"], g1_s2["D"], g1_s3["D"], "D"),
        ("  p_fall", emp["p_fall"], g1_s2["p_fall"], g1_s3["p_fall"], "p_fall"),
        ("  p_rise", emp["p_rise"], g1_s2["p_rise"], g1_s3["p_rise"], "p_rise"),
        ("  m21", emp["m21"], g1_s2["m21"], g1_s3["m21"], "m21"),
        ("  m12", emp["m12"], g1_s2["m12"], g1_s3["m12"], "m12"),
        ("  lev", emp["lev"], g1_s2["lev"], g1_s3["lev"], "lev"),
        ("  runs r2", emp["r2"], g1_s2["r2"], g1_s3["r2"], None),
        ("  runs r3", emp["r3"], g1_s2["r3"], g1_s3["r3"], None),
    ]:
        ci_str = ""
        if ci_k and ci_k in emp_ci and emp_ci[ci_k]:
            ci_str = f" [{emp_ci[ci_k]['q05']:+.3f},{emp_ci[ci_k]['q95']:+.3f}]"
        print(f"    {lbl:11s}: emp={ev:+.4f}{ci_str:16s} | S2={s2v['median']:+.4f} [{s2v['q05']:+.3f},{s2v['q95']:+.3f}] | "
              f"S3={s3v['median']:+.4f} [{s3v['q05']:+.3f},{s3v['q95']:+.3f}]", flush=True)

    print("  --- Head share by block ---", flush=True)
    for b_i, hv in enumerate(emp["head_share_blocks"]):
        b2 = g1_s2["head_share_blocks"][b_i]
        b3 = g1_s3["head_share_blocks"][b_i]
        print(f"    blk {b_i}: emp={hv:.4f} | S2={b2['median']:.4f} [{b2['q05']:.3f},{b2['q95']:.3f}] | "
              f"S3={b3['median']:.4f} [{b3['q05']:.3f},{b3['q95']:.3f}]", flush=True)

    g2 = oos_goal2_gold(pn, X, R, floors, K, cfg, sb, n_ens=n_ens)
    print(f"  --- Goal 2 (M={n_ens}, exact [x_(2),x_(38)] cov90, common cohort) ---", flush=True)
    for h_s, r in g2["by_horizon"].items():
        print(f"    h={int(h_s):2d}w | X_CRPS S2={r['crps_X_s2']:.4f} S3={r['crps_X_s3']:.4f} (S3 wins {r['wins_s3_vs_s2_crps_X']}/5) | "
              f"MAE no={r['mae_no']:.4f} ll={r['mae_ll']:.4f} S2_kal={r['mae_kal_s2']:.4f} S3_kal={r['mae_kal_s3']:.4f} "
              f"S2_ens={r['mae_ens_s2']:.4f} S3_ens={r['mae_ens_s3']:.4f}", flush=True)
        print(f"          | cov90 S2={r['cov90_s2']:.3f}(pitSD={r['pit_sd_s2']:.3f}) S3={r['cov90_s3']:.3f}(pitSD={r['pit_sd_s3']:.3f}) | "
              f"logR no={r['crps_lr_no']:.4f} rb={r['crps_lr_rb']:.4f} S2={r['crps_lr_s2']:.4f} S3={r['crps_lr_s3']:.4f}", flush=True)
        print(f"          | shareHD_MAE no={r['share_mae_no_hd']:.6f} ll={r['share_mae_ll_hd']:.6f} "
              f"S2={r['share_mae_hd_s2']:.6f}(r={r['share_corr_hd_s2']:+.3f}) S3={r['share_mae_hd_s3']:.6f}(r={r['share_corr_hd_s3']:+.3f}) | "
              f"fBrierHD rb={r['brier_hd_rb']:.4f} S2={r['brier_hd_s2']:.4f} S3={r['brier_hd_s3']:.4f}", flush=True)

    oop = None
    if pn in ("reddit_comments_ext", "reddit_submissions_long"):
        print(f"  --- Out-of-Period Structure Test (2019-20 -> 2021-22, 20 seeds) ---", flush=True)
        oop = out_of_period_structure_test(pn, X, R, floors, K, cfg, n_seeds=n_seeds)
        e19, e21 = oop["emp_2019_20"], oop["emp_2021_22"]
        s2o, s3o = oop["S2"]["sim"], oop["S3"]["sim"]
        for lbl, v19, v21, b2, b3 in [
            ("Head share", e19["head_share"], e21["head_share"], s2o["head_share"], s3o["head_share"]),
            ("Turn top10", e19["turnover"]["10"], e21["turnover"]["10"], s2o["turnover"]["10"], s3o["turnover"]["10"]),
            ("Turn K/4", e19["turnover"][str(K // 4)], e21["turnover"][str(K // 4)], s2o["turnover"][str(K // 4)], s3o["turnover"][str(K // 4)]),
            ("Lad gap r1", e19["lad_last"]["1"], e21["lad_last"]["1"], s2o["lad_last"]["1"], s3o["lad_last"]["1"]),
            ("Lad K/64", e19["lad_last"][str(K // 64)], e21["lad_last"][str(K // 64)], s2o["lad_last"][str(K // 64)], s3o["lad_last"][str(K // 64)]),
            ("Boundary", e19["boundary_ratio"], e21["boundary_ratio"], s2o["boundary_ratio"], s3o["boundary_ratio"]),
            ("Dep rate", e19["dep_rate"], e21["dep_rate"], s2o["dep_rate"], s3o["dep_rate"]),
            ("Dep imm%", e19["dep_imm_cross_frac"], e21["dep_imm_cross_frac"], s2o["dep_imm_cross_frac"], s3o["dep_imm_cross_frac"]),
            ("Gapped rho", e19["gapped_rho"], e21["gapped_rho"], s2o["gapped_rho"], s3o["gapped_rho"]),
            ("Asym D", e19["D"], e21["D"], s2o["D"], s3o["D"]),
        ]:
            print(f"    OOP {lbl:10s}: 19-20={v19:+.4f} | 21-22={v21:+.4f} | "
                  f"S2={b2['median']:+.4f} [{b2['q05']:+.3f},{b2['q95']:+.3f}] | "
                  f"S3={b3['median']:+.4f} [{b3['q05']:+.3f},{b3['q95']:+.3f}]", flush=True)

    print(f"  [Done {pn} in {time.time() - t0:.1f}s]", flush=True)
    return dict(
        panel=pn, K=K, T=int(X.shape[0]), N=int(X.shape[1]),
        params_s2=pd2, params_s3=pd3,
        emp=emp, emp_cluster_ci=emp_ci,
        g1_s2=g1_s2, g1_s3=g1_s3,
        goal2=g2, oop_2019_20_to_2021_22=oop,
    )


def main():
    oracle = run_gaussian_oracle_check()
    all_res = {"oracle_check": oracle, "panels": {}}
    for pn, cfg in PANELS.items():
        all_res["panels"][pn] = run_panel(pn, cfg, n_seeds=20, n_ens=39)
        gc.collect()
    out_path = OUT_DIR / "round3_gold_benchmark_results.json"
    out_path.write_text(json.dumps(all_res, indent=2))
    print(f"\nSaved -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
