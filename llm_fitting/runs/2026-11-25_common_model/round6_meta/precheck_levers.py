"""Fast pre-check of Round 6 levers on the 4 DEVELOPMENT panels (zero sealed contact)."""
from __future__ import annotations

import json, math, sys
from pathlib import Path
import numpy as np

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "llm_fitting"))
import common_rankdiff as cr

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")


def load_panel(name: str):
    d = np.load(CACHE / f"{name}.npz")
    spec_b = None
    if name == "facebook_a":
        with open(CACHE / "facebook_a_spec_b.json") as f:
            sb = json.load(f)
        spec_b = (np.asarray(sb["z"], float), np.asarray(sb["sigma_obs"], float))
    return d["X"], d["R"], int(d["K"]), spec_b


def perm_bands(R, K):
    T, N = R.shape
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
    z_K = float(math.log(max((K - 0.5) / N, cr.Z_CLIP)))
    return pr_ord, pres, band_of, nb, band_z, z_K


def block_mu21(Xc, band_of, nb, in_k_ent, B):
    """Block-mean cross third moments mu21, mu12 on consecutive B-week blocks (3 blocks => 2 changes)."""
    T = Xc.shape[0]
    if T < 3 * B:
        return np.nan, np.nan
    m21s, m12s = [], []
    step = max(B // 2, 3)
    for t0 in range(0, T - 3 * B + 1, step):
        with np.errstate(all="ignore"):
            b0 = np.nanmean(Xc[t0:t0 + B], axis=0)
            b1 = np.nanmean(Xc[t0 + B:t0 + 2 * B], axis=0)
            b2 = np.nanmean(Xc[t0 + 2 * B:t0 + 3 * B], axis=0)
        d1 = b1 - b0
        d2 = b2 - b1
        # demean within band
        dm1 = np.full_like(d1, np.nan)
        dm2 = np.full_like(d2, np.nan)
        for b in range(nb - 1):
            sel = (band_of == b) & in_k_ent
            ok = sel & np.isfinite(d1) & np.isfinite(d2)
            if ok.sum() >= 10:
                dm1[ok] = d1[ok] - np.mean(d1[ok])
                dm2[ok] = d2[ok] - np.mean(d2[ok])
        ok = np.isfinite(dm1) & np.isfinite(dm2)
        if ok.sum() >= 50:
            x1, x2 = dm1[ok], dm2[ok]
            s1, s2 = float(np.std(x1)), float(np.std(x2))
            if s1 > 1e-6 and s2 > 1e-6:
                m21s.append(float(np.mean((x1 ** 2) * x2)) / (s1 * s1 * s2))
                m12s.append(float(np.mean(x1 * (x2 ** 2))) / (s1 * s2 * s2))
    return (float(np.mean(m21s)) if m21s else np.nan), (float(np.mean(m12s)) if m12s else np.nan)


for name in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
    X, R, K, spec_b = load_panel(name)
    T, N = X.shape
    pr_ord, pres, band_of, nb, band_z, z_K = perm_bands(R, K)
    in_k_ent = (pr_ord <= K) & (pres >= 0.5)
    Xc = X - cr._top_k_center(X, R, K)

    print(f"\n=== {name} (T={T}, K={K}) ===")
    for B in (7, 13, 26):
        m21, m12 = block_mu21(Xc, band_of, nb, in_k_ent, B)
        if np.isfinite(m21):
            print(f"  Block B={B:2d}w: mu21={m21:+.4f}, mu12={m12:+.4f}")

    # Fit b_J from lasting-variance gap (band-level)
    pr = cr.estimate_common(X, R, K, spec_b=spec_b, refine_drift=False, asymmetry=False, scale_moment="robust")
    gap = np.array(pr.diag["jumps"]["gap_band"][:-1], float)
    sLv = np.array(pr.diag["sL2_band_var"][:-1], float)
    bz = band_z[:-1]
    dz = bz - z_K
    bn = np.array([int(np.sum((band_of == b) & in_k_ent)) for b in range(nb - 1)], float)
    w = np.minimum(bn, 100.0)
    ok_g = np.isfinite(gap) & (gap > 1e-5) & np.isfinite(sLv) & (sLv > 1e-5)
    if ok_g.sum() >= 2:
        aJ, bJ = cr._wls_line(dz[ok_g], 0.5 * np.log(gap[ok_g]), w[ok_g])
        # Also include near-zero head bands softly via log(max(gap, 1e-4))
        ok_all = np.isfinite(gap) & np.isfinite(sLv) & (sLv > 1e-5)
        aJ_all, bJ_all = cr._wls_line(dz[ok_all], 0.5 * np.log(np.maximum(gap[ok_all], 1e-5)), w[ok_all])
        print(f"  Jump rank law (pos only): a_J={aJ:+.3f}, b_J={bJ:+.3f} (b_xi={pr.b_xi:+.3f}) | (all w/ floor 1e-5): a_J={aJ_all:+.3f}, b_J={bJ_all:+.3f}")

    # Fit b_surge from band-level k3(h=13) (or h=8..13 avg)
    k3_b = np.full(nb - 1, np.nan)
    hs_k3 = [h for h in (4, 8, 13) if h < T // 2]
    for b in range(nb - 1):
        idx = np.flatnonzero((band_of == b) & in_k_ent)
        if idx.size < 15:
            continue
        vals = []
        for h in hs_k3:
            sub = Xc[h:, idx] - Xc[:-h, idx]
            with np.errstate(all="ignore"):
                sub = sub - np.nanmean(sub, axis=1, keepdims=True)
            vf = sub[np.isfinite(sub)]
            if vf.size > 50:
                vals.append(float(np.mean(vf ** 3)))
        if vals:
            k3_b[b] = float(np.mean(vals))
    ok_k3 = np.isfinite(k3_b) & (k3_b > 1e-4)
    if ok_k3.sum() >= 2:
        aS, bS = cr._wls_line(dz[ok_k3], (1.0 / 3.0) * np.log(k3_b[ok_k3]), w[ok_k3])
        print(f"  Surge k3(4..13) rank law: a_S={aS:+.3f}, b_surge={bS:+.3f} (b_xi={pr.b_xi:+.3f}), k3_b={[round(x,4) if np.isfinite(x) else None for x in k3_b]}")
    else:
        print(f"  Surge k3(4..13) too few positive bands: {[round(x,4) if np.isfinite(x) else None for x in k3_b]}")
