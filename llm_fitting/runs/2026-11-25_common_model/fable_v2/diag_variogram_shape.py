#!/usr/bin/env python3
"""D1: variogram SHAPE diagnostic.

For each DEVELOPMENT panel, compare the within-band demeaned h-week increments of top-K entities
(the exact objects the structure-function estimator uses) between the empirical panel and a
simulated S2 world fitted to it, on three scale measures:
  - mean-square variance (what the NNLS variogram fit matches),
  - robust IQR^2/1.349^2 and MAD^2/0.6745^2 (what the gate's MEDIAN displacement responds to),
  - excess kurtosis and the 3-robust-sd tail fraction (shape).
Both sides pass through one code path (band_setup -> inc_stats). Zero model code changed.
"""
from __future__ import annotations

import json
import math
import sys
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_fable as cr  # noqa: E402

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
PANELS = {"facebook_a": True, "instagram_hm": False, "reddit_comments_ext": False, "reddit_submissions_long": False}
HS = [1, 2, 4, 8, 13, 26, 52]


def load(pn):
    d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
    X = d["X"].astype(np.float64)
    R = d["R"].astype(np.float64)
    fl = d["floors"].astype(np.float64)
    K = int(d["K"])
    sb = None
    pth = CACHE / f"{pn}_spec_b.json"
    if pth.exists():
        js = json.loads(pth.read_text())
        sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
    return X, R, fl, K, sb


def band_setup(X, R, floors, K):
    T, N = X.shape
    pen = np.where(np.isfinite(R), R, floors[:, None])
    rperm = pen.mean(axis=0)
    pres = np.isfinite(R).mean(axis=0)
    pr_ord = np.empty(N)
    pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
    edges = cr._band_edges(K, N)
    nb = len(edges) - 1
    band_all = np.clip(np.searchsorted(edges, pr_ord, side="right") - 1, 0, nb - 1)
    band_of = np.where(pres >= 0.5, band_all, -1)
    Xc = X - cr._top_k_center(X, R, K)
    return Xc, band_of, nb, pr_ord, pres


def inc_stats(Xc, band_of, nb, hs):
    T = Xc.shape[0]
    out = {}
    for b in list(range(nb - 1)) + ["pool"]:
        if b == "pool":
            idx = np.flatnonzero((band_of >= 0) & (band_of < nb - 1))
        else:
            idx = np.flatnonzero(band_of == b)
        if idx.size < 5:
            continue
        Xb = Xc[:, idx]
        bb = band_of[idx]
        for h in hs:
            if h >= T:
                continue
            d = Xb[h:] - Xb[:-h]
            # within-band, within-t demeaning (same as _structure_function), done per band even in pool
            dm_parts = []
            df = 0.0
            for b2 in np.unique(bb):
                sel = bb == b2
                dd = d[:, sel]
                fin = np.isfinite(dd)
                cnt = fin.sum(axis=1)
                ok_t = cnt >= 5
                dd = dd[ok_t]
                with np.errstate(all="ignore"):
                    mu = np.nanmean(dd, axis=1, keepdims=True)
                v = (dd - mu)[np.isfinite(dd)]
                dm_parts.append(v)
                df += float(np.sum(cnt[ok_t] - 1.0))
            dm = np.concatenate(dm_parts) if dm_parts else np.array([])
            n = dm.size
            if n < 50:
                continue
            var = float(np.sum(dm * dm) / max(df, 1.0))
            q25, q50, q75 = np.quantile(dm, [0.25, 0.5, 0.75])
            iqr_var = float(((q75 - q25) / 1.349) ** 2)
            mad_var = float((np.median(np.abs(dm - q50)) / 0.6745) ** 2)
            sd = math.sqrt(var)
            exk = float(np.mean(dm ** 4)) / (var * var) - 3.0
            dw = np.clip(dm, -7 * sd, 7 * sd)
            vw = float(np.mean(dw * dw))
            exk_w = float(np.mean(dw ** 4)) / (vw * vw) - 3.0
            rsd = math.sqrt(iqr_var)
            out[f"{b}|{h}"] = dict(
                n=int(n), var=var, iqr_var=iqr_var, mad_var=mad_var, ratio_iqr=var / iqr_var,
                exk=exk, exk_w7=exk_w, tail3=float(np.mean(np.abs(dm) > 3.0 * rsd)),
                med_abs=float(np.median(np.abs(dm))),
            )
    return out


def model_D(p, band_level, band_z, hs):
    out = {}
    for b in range(len(band_z) - 1):
        sL2 = float(cr.sigma_L_of(p, np.array([band_level[b]]))[0] ** 2) if np.isfinite(band_level[b]) else np.nan
        vxi = float(cr.v_xi_of(p, np.array([band_z[b]]))[0])
        so2 = float(cr.sigma_obs_of(p, np.array([band_z[b]]))[0] ** 2)
        for h in hs:
            out[f"{b}|{h}"] = 2 * so2 + 2 * vxi * (1 - p.phi ** h) + sL2 * h
    return out


def main(panels):
    res = {}
    for pn in panels:
        t0 = time.time()
        X, R, fl, K, sb = load(pn)
        T, N = X.shape
        p = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb if PANELS[pn] else None, refine_drift=True, asymmetry=False)
        Xc, band_of, nb, pr_ord, pres = band_setup(X, R, fl, K)
        emp = inc_stats(Xc, band_of, nb, HS)
        band_level = np.array(p.diag["band_level"])
        band_z = np.array(p.diag["band_z"])
        mod = model_D(p, band_level, band_z, HS)
        # Simulated S2 world (ladder_obs start, 26w burn, absence on), same code path
        Xs, Rs, _ = cr.simulate_common_panel(p, p.ladder_obs, T_sim=T, seed=300, apply_absence=True, burn=26)
        fls = np.sum(np.isfinite(Rs), axis=1).astype(float) + 1.0
        Xcs, band_s, nbs, _, _ = band_setup(Xs, Rs, fls, K)
        sim = inc_stats(Xcs, band_s, nbs, HS)
        # pool kurtosis
        pool = p.innov_pool
        pk = float(np.mean(pool ** 4)) - 3.0
        print(f"\n===== {pn}: T={T} N={N} K={K} | S2 sL0={p.sigma_L0:.4f} beta={p.beta:+.3f} phi={p.phi:.2f} s_vol={p.s_vol:.3f} | innov_pool exkurt={pk:.2f}  [{time.time()-t0:.0f}s]")
        print(f"  {'band':>5s} {'h':>3s} | {'empVar':>8s} {'empIQRv':>8s} {'ratio':>6s} {'exk':>6s} {'tail3':>6s} | {'simVar':>8s} {'simIQRv':>8s} {'ratio':>6s} {'exk':>6s} {'tail3':>6s} | {'modelD':>8s} | {'emp/sim IQRv':>12s} {'emp/sim medabs':>14s}")
        for b in [0, 1, 2, "pool"]:
            for h in HS:
                k = f"{b}|{h}"
                if k not in emp or k not in sim:
                    continue
                e, s = emp[k], sim[k]
                md = mod.get(k, np.nan) if b != "pool" else np.nan
                print(f"  {str(b):>5s} {h:3d} | {e['var']:8.4f} {e['iqr_var']:8.4f} {e['ratio_iqr']:6.2f} {e['exk']:6.1f} {e['tail3']:6.3f} | "
                      f"{s['var']:8.4f} {s['iqr_var']:8.4f} {s['ratio_iqr']:6.2f} {s['exk']:6.1f} {s['tail3']:6.3f} | {md:8.4f} | "
                      f"{e['iqr_var']/s['iqr_var']:12.3f} {e['med_abs']/s['med_abs']:14.3f}")
        res[pn] = dict(T=T, N=N, K=K, params=dict(sigma_L0=p.sigma_L0, beta=p.beta, phi=p.phi, s_vol=p.s_vol, a_xi=p.a_xi, b_xi=p.b_xi, a_obs=p.a_obs, b_obs=p.b_obs),
                       pool_exkurt=pk, emp=emp, sim=sim, model=mod,
                       band_level=band_level.tolist(), band_z=band_z.tolist())
        del X, R, Xc, Xs, Rs, Xcs
    (HERE / "diag_variogram_shape.json").write_text(json.dumps(res, indent=1, default=float))


if __name__ == "__main__":
    main(sys.argv[1:] or list(PANELS))
