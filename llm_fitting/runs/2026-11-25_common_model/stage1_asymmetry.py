#!/usr/bin/env python3
"""Stage 1A–1D diagnostics:
  1A. Planted synthetic verification + empirical cross third moments (E[x^2 y], E[x y^2]) & leverage ratio
  1B. Consecutive multi-block decline vs. gain run-length asymmetry
  1C. Honest uncertainty: origin-by-origin & 50-cluster co-movement bootstrap vs. entity bootstrap
  1D. Two-channel physical decomposition: Item Supply log(1+items) vs. Engagement-per-Item (X - N)

Runs entirely from the lean scratch cache (DEVELOPMENT data only).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

CACHE_DIR = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
OUT_DIR = Path(__file__).resolve().parent

PANEL_CFG = {
    "facebook_a": dict(W=4, H=26, gap=4, step=4, pres_min=0.9, block_runs=(13,)),
    "instagram_hm": dict(W=4, H=16, gap=4, step=4, pres_min=0.75, block_runs=(7,)),
    "reddit_comments_ext": dict(W=13, H=52, gap=13, step=13, pres_min=0.9, block_runs=(26, 13)),
    "reddit_submissions_long": dict(W=13, H=52, gap=13, step=13, pres_min=0.9, block_runs=(26, 13)),
}


def load_cached(panel: str) -> dict:
    d = np.load(CACHE_DIR / f"{panel}.npz", allow_pickle=True)
    return {k: d[k] for k in d.files}


def strata_of(rperm: np.ndarray, K: int, pres: np.ndarray | None = None, min_pres: float = 0.0) -> np.ndarray:
    """3 permanent-rank strata inside top-K: 0=head (<=K/16), 1=mid (<=K/2), 2=body (<=K), -1=outside."""
    lab = np.full(rperm.size, -1, dtype=np.int32)
    # rperm is already sorted or we rank by rperm
    ord_r = np.empty_like(rperm)
    ord_r[np.argsort(rperm, kind="stable")] = np.arange(1, rperm.size + 1)
    lab[ord_r <= K] = 2
    lab[ord_r <= K // 4] = 1
    lab[ord_r <= K // 16] = 0
    if pres is not None and min_pres > 0:
        lab[pres < min_pres] = -1
    return lab


def wmean(X: np.ndarray, lo: int, hi: int, pres_min: float = 0.9) -> np.ndarray:
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(axis=0) >= pres_min
    with np.errstate(all="ignore"):
        m = np.nanmean(seg, axis=0)
    return np.where(ok, m, np.nan)


def extract_pairs(X: np.ndarray, lab: np.ndarray, W: int, H: int, gap: int, step: int, pres_min: float) -> list[dict]:
    T = X.shape[0]
    pairs = []
    a = 0
    while a + 2 * (H + W) + gap <= T:
        x = wmean(X, a + H, a + H + W, pres_min) - wmean(X, a, a + W, pres_min)
        b0 = a + H + W + gap
        y = wmean(X, b0 + H, b0 + H + W, pres_min) - wmean(X, b0, b0 + W, pres_min)
        ok = (lab >= 0) & np.isfinite(x) & np.isfinite(y)
        xd = np.full_like(x, np.nan)
        yd = np.full_like(y, np.nan)
        for j in range(3):
            s = ok & (lab == j)
            if s.sum() > 30:
                xd[s] = x[s] - np.mean(x[s])
                yd[s] = y[s] - np.mean(y[s])
        ok2 = np.isfinite(xd) & np.isfinite(yd)
        if ok2.sum() >= 100:
            pairs.append(dict(origin=a, xd=xd, yd=yd, ok=ok2))
        a += step
    return pairs


def pair_metrics(xd: np.ndarray, yd: np.ndarray, ok: np.ndarray) -> dict:
    x, y = xd[ok], yd[ok]
    ql, qh = np.quantile(x, [0.1, 0.9])
    lo, hi = x <= ql, x >= qh
    p_fall = float(np.mean(y[lo]) / np.mean(x[lo]))
    p_rise = float(np.mean(y[hi]) / np.mean(x[hi]))
    D = p_fall - p_rise
    rho = float(spearmanr(x, y).correlation)
    sx, sy = float(np.std(x)), float(np.std(y))
    zx, zy = x / max(sx, 1e-12), y / max(sy, 1e-12)
    mu21 = float(np.mean((zx ** 2) * zy))
    mu12 = float(np.mean(zx * (zy ** 2)))
    skew_x = float(np.mean(zx ** 3))
    skew_y = float(np.mean(zy ** 3))
    lev_ratio = float(np.std(y[lo]) / max(np.std(y[hi]), 1e-12))
    # Also trimmed third moments (winsorized at +/-5 sd) to guard against 1-entity outlier dominance
    zxw = np.clip(zx, -5.0, 5.0)
    zyw = np.clip(zy, -5.0, 5.0)
    mu21_w = float(np.mean((zxw ** 2) * zyw))
    mu12_w = float(np.mean(zxw * (zyw ** 2)))
    return dict(
        n=int(x.size), rho=rho, D=D, p_fall=p_fall, p_rise=p_rise,
        mu21=mu21, mu12=mu12, mu21_w=mu21_w, mu12_w=mu12_w,
        skew_x=skew_x, skew_y=skew_y, lev_ratio=lev_ratio,
    )


def summarize_pairs(pairs: list[dict], cols: np.ndarray | None = None) -> dict:
    per = []
    for p in pairs:
        xd, yd, ok = p["xd"], p["yd"], p["ok"]
        if cols is not None:
            xd, yd, ok = xd[cols], yd[cols], ok[cols]
        if ok.sum() < 100:
            continue
        m = pair_metrics(xd, yd, ok)
        m["origin"] = p["origin"]
        per.append(m)
    if not per:
        return {}
    keys = ["rho", "D", "p_fall", "p_rise", "mu21", "mu12", "mu21_w", "mu12_w", "skew_x", "skew_y", "lev_ratio"]
    agg = {k: float(np.mean([r[k] for r in per])) for k in keys}
    agg["n_origins"] = len(per)
    agg["mean_n"] = float(np.mean([r["n"] for r in per]))
    agg["by_origin"] = per
    return agg


def planted_verification() -> dict:
    """Verify P1.1: cross third moments cleanly separate homogeneous, skewed persistent drift,
    fading positive surges, and combined worlds."""
    rng = np.random.default_rng(2026)
    T, N = 212, 6000
    W, H, gap, step = 13, 52, 13, 13
    lab = np.zeros(N, dtype=np.int32)
    lab[:N // 16] = 0
    lab[N // 16:N // 2] = 1
    lab[N // 2:] = 2

    def sim_base():
        # Homogeneous walk + AR(1) passing swing + obs noise
        L = np.cumsum(0.05 * rng.standard_normal((T, N)), axis=0)
        xi = np.zeros((T, N))
        for t in range(1, T):
            xi[t] = 0.6 * xi[t - 1] + 0.15 * rng.standard_normal(N)
        eps = 0.2 * rng.standard_normal((T, N))
        return L + xi + eps

    # World 1: Homogeneous Gaussian
    X1 = sim_base()

    # World 2: Negatively skewed persistent growth (20% decliners at -0.015/wk, mean-zero)
    X2 = sim_base()
    is_dec = rng.random(N) < 0.20
    g = np.where(is_dec, -0.015, 0.015 * 0.20 / 0.80)
    X2 += g[None, :] * np.arange(T)[:, None]

    # World 3: Positively skewed fading surges (lam=0.008/wk, mu=1.2, hl=26 wks)
    X3 = sim_base()
    rho_s = 0.5 ** (1.0 / 26.0)
    s = np.zeros(N)
    S = np.empty((T, N))
    for t in range(-104, T):
        s = rho_s * s + (rng.random(N) < 0.008) * 1.2 * rng.standard_exponential(N)
        if t >= 0:
            S[t] = s
    X3 += S

    # World 4: Combined (decliners + fading positive surges)
    X4 = X2 + S

    out = {}
    for name, Xw in [
        ("homogeneous", X1),
        ("skewed_decline", X2),
        ("fading_surge", X3),
        ("combined", X4),
    ]:
        ps = extract_pairs(Xw, lab, W, H, gap, step, pres_min=0.9)
        s_agg = summarize_pairs(ps)
        s_agg.pop("by_origin", None)
        out[name] = s_agg
    return out


def build_comovement_clusters(X: np.ndarray, lab: np.ndarray, n_clusters: int = 50, seed: int = 42) -> np.ndarray:
    """Cluster top-K entities into co-movement clusters using truncated SVD + k-means on
    weekly demeaned changes (captures shared topic/platform/ban-wave shocks)."""
    idx = np.flatnonzero(lab >= 0)
    dX = np.diff(X[:, idx], axis=0)
    dX = dX - np.nanmean(dX, axis=1, keepdims=True)
    # Standardize each entity's weekly change; fill NaN with 0
    sd = np.nanstd(dX, axis=0, keepdims=True)
    sd = np.where(np.isfinite(sd) & (sd > 1e-6), sd, 1.0)
    Z = np.nan_to_num(dX / sd, nan=0.0)
    Z = np.clip(Z, -5.0, 5.0)
    # Truncated SVD via covariance of T x T (super fast since T <= 213!)
    C_t = (Z @ Z.T) / max(Z.shape[1], 1)
    eigvals, eigvecs = np.linalg.eigh(C_t)
    k_svd = min(15, Z.shape[0] - 1)
    U = eigvecs[:, -k_svd:]  # (T-1, k_svd)
    # Entity loadings in top k_svd temporal modes: (M_in, k_svd)
    F = Z.T @ U
    norms = np.linalg.norm(F, axis=1, keepdims=True)
    F = F / np.maximum(norms, 1e-8)

    # Lightweight k-means (25 iterations, deterministic seed)
    rng = np.random.default_rng(seed)
    c_idx = rng.choice(F.shape[0], size=min(n_clusters, F.shape[0]), replace=False)
    centers = F[c_idx].copy()
    assign = np.zeros(F.shape[0], dtype=np.int32)
    for _ in range(25):
        # Compute squared distances in chunks to stay tiny in RAM
        d2 = np.sum(F ** 2, axis=1, keepdims=True) + np.sum(centers ** 2, axis=1)[None, :] - 2.0 * (F @ centers.T)
        new_assign = np.argmin(d2, axis=1).astype(np.int32)
        if np.array_equal(new_assign, assign):
            break
        assign = new_assign
        for c in range(centers.shape[0]):
            m = assign == c
            if m.any():
                centers[c] = F[m].mean(axis=0)
    clusters = np.full(X.shape[1], -1, dtype=np.int32)
    clusters[idx] = assign
    return clusters


def bootstrap_uncertainty(pairs: list[dict], lab: np.ndarray, clusters: np.ndarray, n_boot: int = 200, seed: int = 17) -> dict:
    rng = np.random.default_rng(seed)
    ent_stats = {k: [] for k in ("D", "rho", "p_fall", "p_rise", "mu21_w", "mu12_w")}
    cls_stats = {k: [] for k in ent_stats}

    # Stratified entity indices
    s_idx = [np.flatnonzero(lab == j) for j in range(3)]
    # Cluster groups
    c_ids = np.unique(clusters[clusters >= 0])
    c_members = [np.flatnonzero(clusters == c) for c in c_ids]

    for _ in range(n_boot):
        # Entity bootstrap (within strata)
        cols_e = np.concatenate([rng.choice(ix, size=ix.size, replace=True) for ix in s_idx if ix.size > 0])
        me = summarize_pairs(pairs, cols=cols_e)
        if me:
            for k in ent_stats:
                ent_stats[k].append(me[k])
        # Cluster bootstrap (resample whole co-movement clusters)
        picked = rng.choice(len(c_members), size=len(c_members), replace=True)
        cols_c = np.concatenate([c_members[i] for i in picked])
        mc = summarize_pairs(pairs, cols=cols_c)
        if mc:
            for k in cls_stats:
                cls_stats[k].append(mc[k])

    def qband(vals):
        v = np.asarray(vals, float)
        return dict(q05=float(np.quantile(v, 0.05)), q50=float(np.quantile(v, 0.50)),
                    q95=float(np.quantile(v, 0.95)), sd=float(np.std(v, ddof=1)))

    out = {"entity": {}, "cluster50": {}, "width_ratio": {}}
    for k in ent_stats:
        eb = qband(ent_stats[k])
        cb = qband(cls_stats[k])
        out["entity"][k] = eb
        out["cluster50"][k] = cb
        ew = eb["q95"] - eb["q05"]
        cw = cb["q95"] - cb["q05"]
        out["width_ratio"][k] = float(cw / max(ew, 1e-12))
    return out


def run_length_asymmetry(X: np.ndarray, lab: np.ndarray, block: int, pres_min: float = 0.75, n_perm: int = 200, seed: int = 99) -> dict:
    """Count consecutive decline vs. gain runs of length 2, 3, 4 across non-overlapping blocks."""
    T = X.shape[0]
    nb = T // block
    if nb < 3:
        return {}
    start = T - nb * block
    M = np.empty((nb, X.shape[1]), dtype=float)
    for b in range(nb):
        M[b] = wmean(X, start + b * block, start + (b + 1) * block, pres_min)
    d = np.diff(M, axis=0)  # (nb-1, entities)
    # Stratum demean each block transition
    inK = lab >= 0
    for b in range(d.shape[0]):
        for j in range(3):
            s = (lab == j) & np.isfinite(d[b])
            if s.sum() > 30:
                d[b, s] -= np.mean(d[b, s])
    # Restrict to entities observed in all nb blocks
    valid = inK & np.all(np.isfinite(d), axis=0)
    dv = d[:, valid]
    if dv.shape[1] < 100:
        return {}
    n_tr = dv.shape[0]
    sgn = np.sign(dv)  # -1 or +1

    def count_runs(S):
        res = {}
        for m in (2, 3, 4):
            if m > S.shape[0]:
                continue
            dec_cnt = 0
            gain_cnt = 0
            tot_windows = (S.shape[0] - m + 1) * S.shape[1]
            for t in range(S.shape[0] - m + 1):
                sub = S[t:t + m]
                dec_cnt += int(np.sum(np.all(sub < 0, axis=0)))
                gain_cnt += int(np.sum(np.all(sub > 0, axis=0)))
            res[str(m)] = dict(
                dec_rate=float(dec_cnt / tot_windows),
                gain_rate=float(gain_cnt / tot_windows),
                ratio=float(dec_cnt / max(gain_cnt, 1)),
            )
        return res

    obs = count_runs(sgn)
    # Sign-symmetric null: randomly flip sign of each entity's entire trajectory or each transition
    # (time-permutation within entity tests serial dependence; sign-flip tests symmetry!)
    rng = np.random.default_rng(seed)
    perm_ratios = {m: [] for m in obs}
    for _ in range(n_perm):
        # Random sign flip per transition (independent across entities, symmetric null)
        flips = rng.choice([-1.0, 1.0], size=sgn.shape)
        r_p = count_runs(sgn * flips)
        for m in obs:
            perm_ratios[m].append(r_p[m]["ratio"])
    for m in obs:
        pr = np.asarray(perm_ratios[m])
        obs[m]["null_q05"] = float(np.quantile(pr, 0.05))
        obs[m]["null_q95"] = float(np.quantile(pr, 0.95))
    return dict(block=block, n_blocks=nb, n_transitions=n_tr, n_entities=int(dv.shape[1]), runs=obs)


def lasting_var_slope(X: np.ndarray, lab: np.ndarray, h1: int, h2: int) -> float:
    """Quick structure-function slope estimate of lasting weekly variance: (D(h2) - D(h1)) / (h2 - h1)."""
    inK = lab >= 0
    Xsub = X[:, inK]
    vals = {}
    for h in (h1, h2):
        d = Xsub[h:] - Xsub[:-h]
        d = d - np.nanmean(d, axis=1, keepdims=True)
        vals[h] = float(np.nanmean(d * d))
    return float((vals[h2] - vals[h1]) / (h2 - h1))


def two_channel_decomposition(X: np.ndarray, C: np.ndarray, lab: np.ndarray, cfg: dict) -> dict:
    """Decompose X = N + E where N = log1p(items) and E = X - N (for weeks where X finite & C >= 0)."""
    valid_c = np.isfinite(X) & np.isfinite(C) & (C >= 0)
    N_mat = np.where(valid_c, np.log1p(np.maximum(C, 0.0)), np.nan)
    E_mat = np.where(valid_c, X - N_mat, np.nan)

    W, H, gap, step, pres_min = cfg["W"], cfg["H"], cfg["gap"], cfg["step"], cfg["pres_min"]
    h1, h2 = (min(13, H // 2), H)
    out = {}
    pairs_map = {}
    for ch_name, mat in [("total_X", X), ("items_N", N_mat), ("per_item_E", E_mat)]:
        ps = extract_pairs(mat, lab, W, H, gap, step, pres_min)
        pairs_map[ch_name] = ps
        s = summarize_pairs(ps)
        s.pop("by_origin", None)
        s["lasting_var_wk"] = lasting_var_slope(mat, lab, h1, h2)
        out[ch_name] = s

    # Cross-channel persistence: Corr(dN_1, dE_2), Corr(dE_1, dN_2), and contemporaneous Corr(dN_1, dE_1)
    pN = {p["origin"]: p for p in pairs_map["items_N"]}
    pE = {p["origin"]: p for p in pairs_map["per_item_E"]}
    common_o = sorted(set(pN) & set(pE))
    cross = dict(N1_N2=[], E1_E2=[], N1_E2=[], E1_N2=[], N1_E1_contemp=[])
    for o in common_o:
        ok = pN[o]["ok"] & pE[o]["ok"]
        if ok.sum() < 100:
            continue
        n1, n2 = pN[o]["xd"][ok], pN[o]["yd"][ok]
        e1, e2 = pE[o]["xd"][ok], pE[o]["yd"][ok]
        cross["N1_N2"].append(float(spearmanr(n1, n2).correlation))
        cross["E1_E2"].append(float(spearmanr(e1, e2).correlation))
        cross["N1_E2"].append(float(spearmanr(n1, e2).correlation))
        cross["E1_N2"].append(float(spearmanr(e1, n2).correlation))
        cross["N1_E1_contemp"].append(float(spearmanr(n1, e1).correlation))
    out["cross_channel_spearman"] = {k: float(np.mean(v)) if v else None for k, v in cross.items()}
    return out


def main():
    print("=== 1A. Planted Synthetic Verification of Third Moments ===", flush=True)
    planted = planted_verification()
    for name, s in planted.items():
        print(f"  {name:16s}: rho={s['rho']:+.3f}  D={s['D']:+.3f} (pf={s['p_fall']:+.3f}, pr={s['p_rise']:+.3f})  "
              f"mu21_w={s['mu21_w']:+.3f}  mu12_w={s['mu12_w']:+.3f}  lev={s['lev_ratio']:.3f}", flush=True)

    results = dict(planted=planted, panels={})
    for pn, cfg in PANEL_CFG.items():
        print(f"\n=== Panel: {pn} ===", flush=True)
        d = load_cached(pn)
        X = d["X"].astype(np.float64)
        C = d["C"].astype(np.float64)
        rperm = d["rperm"].astype(np.float64)
        pres = d["pres"].astype(np.float64)
        K = int(d["K"])
        min_p = 0.5 if pn == "instagram_hm" else 0.0
        lab = strata_of(rperm, K, pres=pres, min_pres=min_p)

        pairs = extract_pairs(X, lab, cfg["W"], cfg["H"], cfg["gap"], cfg["step"], cfg["pres_min"])
        summary = summarize_pairs(pairs)
        clusters = build_comovement_clusters(X, lab, n_clusters=50)
        unc = bootstrap_uncertainty(pairs, lab, clusters, n_boot=200)
        runs = {str(b): run_length_asymmetry(X, lab, b, pres_min=min(cfg["pres_min"], 0.75)) for b in cfg["block_runs"]}
        two_ch = two_channel_decomposition(X, C, lab, cfg)

        # Print concise diagnostics
        print(f"  Total X: n_origins={summary['n_origins']}, mean_n={summary['mean_n']:.0f}, "
              f"rho={summary['rho']:+.3f}, D={summary['D']:+.3f} (pf={summary['p_fall']:+.3f}, pr={summary['p_rise']:+.3f})", flush=True)
        print(f"           mu21={summary['mu21']:+.3f} (w={summary['mu21_w']:+.3f}), "
              f"mu12={summary['mu12']:+.3f} (w={summary['mu12_w']:+.3f}), "
              f"skew_x={summary['skew_x']:+.3f}, lev={summary['lev_ratio']:.3f}", flush=True)
        print(f"  D 90% CI: entity [{unc['entity']['D']['q05']:+.3f}, {unc['entity']['D']['q95']:+.3f}] | "
              f"cluster50 [{unc['cluster50']['D']['q05']:+.3f}, {unc['cluster50']['D']['q95']:+.3f}] "
              f"(width x{unc['width_ratio']['D']:.2f})", flush=True)
        print(f"  rho 90% CI: entity [{unc['entity']['rho']['q05']:+.3f}, {unc['entity']['rho']['q95']:+.3f}] | "
              f"cluster50 [{unc['cluster50']['rho']['q05']:+.3f}, {unc['cluster50']['rho']['q95']:+.3f}] "
              f"(width x{unc['width_ratio']['rho']:.2f})", flush=True)
        print("  Origins:", ", ".join(f"w{r['origin']}:D={r['D']:+.3f}/rho={r['rho']:+.3f}" for r in summary["by_origin"]), flush=True)
        for bk, rinfo in runs.items():
            if rinfo:
                rstr = ", ".join(f"m={m}: dec/gain={v['ratio']:.2f} (null [{v['null_q05']:.2f},{v['null_q95']:.2f}])"
                                 for m, v in rinfo["runs"].items())
                print(f"  Runs (block={bk}w, n={rinfo['n_entities']}): {rstr}", flush=True)
        print("  Two-channel decomposition:", flush=True)
        for ch in ("total_X", "items_N", "per_item_E"):
            c = two_ch[ch]
            if c:
                print(f"    {ch:11s}: sigL2/wk={c['lasting_var_wk']:.5f}  rho={c['rho']:+.3f}  "
                      f"D={c['D']:+.3f} (pf={c['p_fall']:+.3f}, pr={c['p_rise']:+.3f})  "
                      f"mu21_w={c['mu21_w']:+.3f}  mu12_w={c['mu12_w']:+.3f}  lev={c['lev_ratio']:.3f}", flush=True)
        print(f"    Cross Spearman: {two_ch['cross_channel_spearman']}", flush=True)

        results["panels"][pn] = dict(
            summary=summary,
            uncertainty=unc,
            runs=runs,
            two_channel=two_ch,
        )

    out_path = OUT_DIR / "stage1_asymmetry_results.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nSaved {out_path}", flush=True)


if __name__ == "__main__":
    main()
