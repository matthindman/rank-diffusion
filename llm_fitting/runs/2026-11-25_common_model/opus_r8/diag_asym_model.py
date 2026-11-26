#!/usr/bin/env python3
"""D5 (Round 8): does the R6 S3 surge law reproduce the data's ROBUST tail asymmetry?

Full-panel R6 S3 fit per DEVELOPMENT panel. The model's pooled top-K h-increment law (bandwise core
from the fit, lognormal entity amplitude, passing AR(1), observation noise, persistent drift and the
fitted capped-exponential surges with their inherited rank scale) is simulated by Monte Carlo with
the band mixture of the data, and its tail-asymmetry profile
    A_h(q) = P(dX - med > q s) - P(dX - med < -q s),  s = IQR / 1.349,
is compared with the empirical profile from diag_skew.py's estimator. A symmetric law gives 0.
Writes only into opus_r8/.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr  # noqa: E402

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
PANELS = {"facebook_a": True, "instagram_hm": False, "reddit_comments_ext": False, "reddit_submissions_long": False}
QS = np.array([1.0, 2.0, 3.0, 4.0, 6.0])


def A_profile(v):
    med = np.median(v)
    q25, q75 = np.quantile(v, [0.25, 0.75])
    s = (q75 - q25) / 1.349
    d = v - med
    return np.array([np.mean(d > q * s) - np.mean(d < -q * s) for q in QS])


def emp_profile(X, R, fl, K, hs):
    T, N = X.shape
    pen = np.where(np.isfinite(R), R, fl[:, None])
    pr = np.empty(N)
    pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
    pres = np.isfinite(R).mean(axis=0)
    Xc = X - cr._top_k_center(X, R, K)
    sel = (pr <= K) & (pres >= 0.5)
    out = {}
    for h in hs:
        dx = Xc[h:, sel] - Xc[:-h, sel]
        dx = dx - np.nanmean(dx, axis=1, keepdims=True)
        out[h] = A_profile(dx[np.isfinite(dx)])
    return out


def model_profile(p, hs, n_tot=60000, seed=11):
    """Monte Carlo of the fitted model's pooled top-K h-increment law (band mixture by band counts)."""
    dg = p.diag
    bz = np.array(dg["band_z"])[:-1]
    bl = np.array(dg["band_level"])[:-1]
    edges = cr._band_edges(p.K, p.N)
    cnt = np.diff(np.minimum(edges, p.K))[:-1]
    w = cnt / cnt.sum()
    rng = np.random.default_rng(seed)
    pool = p.innov_pool
    dr = lambda *s: pool[rng.integers(0, pool.size, size=s)]
    H = max(hs)
    out = {h: [] for h in hs}
    for b in range(bz.size):
        n = max(2000, int(round(n_tot * w[b])))
        z = np.full(n, bz[b])
        sL = cr.sigma_L_of(p, np.full(n, bl[b]))[0]
        sx = math.sqrt(cr.v_xi_of(p, z)[0])
        so = cr.sigma_obs_of(p, z)[0]
        if p.s_vol > 0:
            zv = np.clip(rng.standard_normal(n), -2.5, 2.5)
            v = np.exp(p.s_vol * zv)
            v /= v.mean()
        else:
            v = np.ones(n)
        sq = np.sqrt(v)
        L = np.cumsum(np.vstack([np.zeros(n), sL * dr(H, n)]), axis=0)
        if p.jump_lam > 0:
            L = L + np.cumsum(np.vstack([np.zeros(n), (rng.random((H, n)) < p.jump_lam) * cr._jump_sd_of(p, z)[0] * rng.standard_normal((H, n))]), axis=0)
        xi = np.empty((H + 1, n))
        xi[0] = sx * rng.standard_normal(n)
        c = math.sqrt(max(1 - p.phi ** 2, 1e-6))
        for t in range(H):
            xi[t + 1] = p.phi * xi[t] + sx * c * dr(n)
        eps = so * dr(H + 1, n)
        Xp = L + sq[None, :] * (xi + eps)
        if p.decline_var > 0:
            sd = math.sqrt(p.decline_var)
            d = np.clip(sd * rng.standard_normal(n), -2.5 * sd, 2.5 * sd)
            acc = np.zeros(n)
            dpath = np.zeros((H + 1, n))
            isd = math.sqrt(p.decline_var * max(1 - p.decline_rho ** 2, 0))
            for t in range(H):
                acc = acc + d
                dpath[t + 1] = acc
                d = p.decline_rho * d + isd * rng.standard_normal(n)
            Xp = Xp + dpath
        if p.surge_lam > 0:
            s = np.zeros(n)
            for _ in range(int(math.ceil(math.log(0.001) / math.log(p.surge_rho)))):
                s = p.surge_rho * s + (rng.random(n) < p.surge_lam) * p.surge_mu * np.minimum(rng.standard_exponential(n), cr.SURGE_EXP_CAP)
            sp = np.empty((H + 1, n))
            sp[0] = s
            for t in range(H):
                s = p.surge_rho * s + (rng.random(n) < p.surge_lam) * p.surge_mu * np.minimum(rng.standard_exponential(n), cr.SURGE_EXP_CAP)
                sp[t + 1] = s
            Xp = Xp + cr._surge_scale_of(p.b_xi, z, p.z_K)[0] * sp
        for h in hs:
            out[h].append(Xp[h] - Xp[0])
    return {h: A_profile(np.concatenate(out[h])) for h in hs}


def main():
    res = {}
    for pn, has_sb in PANELS.items():
        d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
        X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
        sb = None
        if has_sb:
            js = json.loads((CACHE / f"{pn}_spec_b.json").read_text())
            sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
        T = X.shape[0]
        hs = [h for h in (4, 13, 26, 52) if h <= T // 2]
        p3 = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=True)
        p2 = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=False)
        e = emp_profile(X, R, fl, K, hs)
        m3 = model_profile(p3, hs)
        m2 = model_profile(p2, hs)
        print(f"\n===== {pn}: S3 surge lam={p3.surge_lam:.4f} mu={p3.surge_mu:.2f} rho={p3.surge_rho:.4f} sigma_d={math.sqrt(p3.decline_var):.4f}")
        print("   h   source    A(1)    A(2)    A(3)    A(4)    A(6)")
        for h in hs:
            for nm, src in (("data", e), ("R6 S2", m2), ("R6 S3", m3)):
                print(f"  {h:2d}  {nm:7s} " + " ".join(f"{a:+.4f}" for a in src[h]))
        res[pn] = {"data": {h: e[h].tolist() for h in hs}, "s2": {h: m2[h].tolist() for h in hs}, "s3": {h: m3[h].tolist() for h in hs},
                   "s3_surge": [p3.surge_lam, p3.surge_mu, p3.surge_rho]}
    (HERE / "diag_asym_model.json").write_text(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
