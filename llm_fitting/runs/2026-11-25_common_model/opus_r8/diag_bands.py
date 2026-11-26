#!/usr/bin/env python3
"""D1 (Round 8): band-level variance accounting on full DEVELOPMENT panels under the Round 6 S2 fit.

For every permanent-rank band b inside K, compares
  - the bandwise mean-square fit (sL2_var, vxi_var, so2_var) and the robust core sL2_rob,
  - the smooth laws the simulator actually uses at that band (sigma_L(L_b)^2 + lam_J * Var(J | z_b),
    V_xi(z_b), sigma_obs(z_b)^2),
  - the measured mean-square variogram D_b(h) against the variogram implied by the smooth laws,
    at h = 1, 4, 13, 26, 52.
Also reports the measured mean h-week change of top-K-median-centred X by CURRENT rank band
(a data moment only; no model comparison) for the drift question.
Reads only the four DEVELOPMENT caches. Writes only into opus_r8/.
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
import common_rankdiff_opus_r8 as cr  # noqa: E402

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
PANELS = {"facebook_a": True, "instagram_hm": False, "reddit_comments_ext": False, "reddit_submissions_long": False}


def load(pn):
    d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
    X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
    sb = None
    if PANELS[pn]:
        js = json.loads((CACHE / f"{pn}_spec_b.json").read_text())
        sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
    return X, R, fl, K, sb


def main():
    out = {}
    for pn in (sys.argv[1:] or PANELS):
        t0 = time.time()
        X, R, fl, K, sb = load(pn)
        T, N = X.shape
        p = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=False)
        dg = p.diag
        bz = np.array(dg["band_z"])
        bl = np.array(dg["band_level"])
        nb = bz.size
        sL2_law = cr.sigma_L_of(p, bl) ** 2
        jv = p.jump_lam * cr._jump_sd_of(p, bz) ** 2
        vxi_law = cr.v_xi_of(p, bz)
        so2_law = cr.sigma_obs_of(p, bz) ** 2
        D = np.array(dg["D"])
        hs = np.arange(1, D.shape[1] + 1, dtype=float)
        rows = []
        print(f"\n===== {pn} T={T} N={N} K={K} n_sc={p.n_free_scalars()} phi={p.phi:.2f} sL0={p.sigma_L0:.4f} beta={p.beta:+.3f} "
              f"b_xi={p.b_xi:+.3f} jump_lam={p.jump_lam} jump_c={p.jump_c:.4f} ({time.time()-t0:.0f}s)")
        print(" b   z-zK   lev-LK | sL2_var  sL2_rob  sL2_used(core+jump) | vxi_var vxi_law | so2_var so2_law | D(13) meas/law  D(52) meas/law")
        for b in range(nb - 1):
            used = float(np.clip(sL2_law[b], 0, None) + jv[b])
            Dlaw = 2 * so2_law[b] + 2 * vxi_law[b] * (1 - p.phi ** hs) + used * hs
            r = dict(b=b, dz=float(bz[b] - p.z_K), dlev=float(bl[b] - p.L_K),
                     sL2_var=dg["sL2_band_var"][b], sL2_rob=dg["sL2_band"][b], sL2_law=float(sL2_law[b]), jump_var=float(jv[b]),
                     vxi_var=dg["vxi_band_var"][b], vxi_law=float(vxi_law[b]), so2_var=dg["so2_band_var"][b], so2_law=float(so2_law[b]),
                     D_meas={int(h): float(D[b, int(h) - 1]) for h in (1, 4, 13, 26, 52) if h <= D.shape[1]},
                     D_law={int(h): float(Dlaw[int(h) - 1]) for h in (1, 4, 13, 26, 52) if h <= D.shape[1]})
            rows.append(r)
            d13 = (r["D_meas"].get(13, np.nan), r["D_law"].get(13, np.nan))
            d52 = (r["D_meas"].get(52, np.nan), r["D_law"].get(52, np.nan))
            print(f" {b}  {r['dz']:+6.2f} {r['dlev']:+6.2f} | {r['sL2_var']:.5f} {r['sL2_rob']:.5f} {used:.5f}({sL2_law[b]:.5f}+{jv[b]:.5f}) | "
                  f"{r['vxi_var']:.4f} {vxi_law[b]:.4f} | {r['so2_var']:.4f} {so2_law[b]:.4f} | {d13[0]:.4f}/{d13[1]:.4f}  {d52[0]:.4f}/{d52[1]:.4f}")

        # Data moment: mean h-week change of top-K-median-centred X by CURRENT rank band at t (survivors only)
        medK = np.array([np.nanmedian(X[t, np.isfinite(R[t]) & (R[t] <= K)]) for t in range(T)])
        Xc = X - medK[:, None]
        edges = np.array([0, K / 64, K / 16, K / 4, K / 2, K])
        mv = {}
        for h in (1, 4, 13, 26, 52):
            if h >= T - 4:
                continue
            res = []
            for b in range(edges.size - 1):
                acc = []
                for t in range(0, T - h):
                    sel = np.isfinite(R[t]) & (R[t] > edges[b]) & (R[t] <= edges[b + 1]) & np.isfinite(Xc[t + h])
                    if sel.sum() >= 3:
                        acc.append(float(np.mean(Xc[t + h, sel] - Xc[t, sel])))
                res.append(float(np.mean(acc)) if acc else float("nan"))
            mv[h] = res
        print(" mean dXc by current rank band (K/64, K/16, K/4, K/2, K):")
        for h, res in mv.items():
            print(f"   h={h:2d}: " + " ".join(f"{v:+.4f}" for v in res) + "   per-week: " + " ".join(f"{v/h:+.5f}" for v in res))
        out[pn] = dict(rows=rows, move_by_rank=mv, phi=p.phi, n_sc=p.n_free_scalars(), jumps=dg.get("jumps"))
    (HERE / "diag_bands.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
