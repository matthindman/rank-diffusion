#!/usr/bin/env python3
"""D4: is the pooled kappa_5 / kappa_3 surge-size estimator dominated by a handful of entities?

Replicates the exact cumulant construction of `_estimate_stage3_surge` (clip at 7 sd, additive k3, k5)
on the top-K present entities of each full panel, then recomputes k3(h), k5(h) and the implied
(hl, lam, mu) under three poolings:
  (1) pooled raw increments (the Round 3/4 estimator),
  (2) pooled after removing the 0.5% of entities with the largest |increment| contribution to k5,
  (3) per-stratum (3 permanent-rank strata x 3 training-volatility tertiles) cumulants, averaged
      with equal weights (kills the 10 Cov(var, k3) pooling inflation of kappa_5),
  (4) per-entity standardized increments (divided by each entity's own sd_i), pooled.
No model code changed; estimator-only diagnostic.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_fable as cr  # noqa: E402

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
PANELS = ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]


def cums(v: np.ndarray):
    sd = float(np.std(v))
    vw = np.clip(v, -7.0 * sd, 7.0 * sd)
    vc = vw - float(np.mean(vw))
    v2 = float(np.mean(vc ** 2))
    v3 = float(np.mean(vc ** 3))
    v5 = float(np.mean(vc ** 5)) - 10.0 * v2 * v3
    return v2, v3, v5


def invert(hs, k3s, k5s):
    best = None
    for hl_g in cr.SURGE_HL_GRID:
        rg = float(0.5 ** (1.0 / hl_g))
        f3 = 3.0 * (rg ** hs) * (1.0 - rg ** hs)
        C3 = max(float((f3 @ k3s) / max(float(f3 @ f3), 1e-12)), 0.0)
        err = float(np.sum((k3s - C3 * f3) ** 2))
        if best is None or err < best[0]:
            best = (err, float(hl_g), rg, C3)
    _, hl, rho, C3 = best
    f5 = 5.0 * (rho ** hs) * (1.0 - rho ** hs) * (1.0 - 2.0 * (rho ** hs) + 2.0 * (rho ** (2.0 * hs)))
    C5 = float((f5 @ k5s) / max(float(f5 @ f5), 1e-12))
    if C3 <= 0 or C5 <= 0:
        return dict(hl=hl, C3=C3, C5=C5, mu=np.nan, lam=np.nan)
    ratio_J = (C5 * (1.0 - rho ** 5)) / max(C3 * (1.0 - rho ** 3), 1e-12)
    mu_raw = math.sqrt(max(ratio_J * (cr.CAP_M3 / cr.CAP_M5), 0.0))
    mu = float(np.clip(mu_raw, 0.15, 2.50))
    rhs = C3 * (1.0 - rho ** 3) / max(mu ** 3, 1e-9)
    lam = float(np.clip(rhs / cr.CAP_M3, 0.0, 0.15))
    for _ in range(4):
        f = lam * cr.CAP_M3 - 3.0 * (lam ** 2) * cr.CAP_M1 * cr.CAP_M2 + 2.0 * (lam ** 3) * (cr.CAP_M1 ** 3) - rhs
        fp = cr.CAP_M3 - 6.0 * lam * cr.CAP_M1 * cr.CAP_M2 + 6.0 * (lam ** 2) * (cr.CAP_M1 ** 3)
        lam = float(np.clip(lam - f / max(fp, 1e-6), 0.0, 0.15))
    Vs = cr._surge_variance(lam, mu, rho)
    return dict(hl=hl, C3=C3, C5=C5, mu_raw=mu_raw, mu=mu, lam=lam, Vs=Vs, max_jump_rankK=5.0 * mu)


def main(panels):
    res = {}
    for pn in panels:
        d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
        X = d["X"].astype(np.float64)
        R = d["R"].astype(np.float64)
        fl = d["floors"].astype(np.float64)
        K = int(d["K"])
        T, N = X.shape
        pen = np.where(np.isfinite(R), R, fl[:, None])
        rperm = pen.mean(axis=0)
        pres = np.isfinite(R).mean(axis=0)
        pr_ord = np.empty(N)
        pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
        Xc = X - cr._top_k_center(X, R, K)
        sel = (pr_ord <= K) & (pres >= 0.5)
        Xsub = Xc[:, sel]
        pr_sub = pr_ord[sel]
        hs = np.array([h for h in (4, 8, 13, 20, 26, 39, 52) if h <= min(52, T // 2)], dtype=float)
        # entity sd of 1-week increments (training-volatility tertiles)
        d1 = np.diff(Xsub, axis=0)
        d1 = d1 - np.nanmean(d1, axis=1, keepdims=True)
        sd_i = np.nanstd(d1, axis=0)
        sd_i = np.where(np.isfinite(sd_i) & (sd_i > 1e-4), sd_i, np.nanmedian(sd_i))
        rank_lab = np.where(pr_sub <= K / 16, 0, np.where(pr_sub <= K / 4, 1, 2))
        vol_lab = np.digitize(sd_i, np.quantile(sd_i, [1 / 3, 2 / 3]))

        k3 = {m: np.zeros(hs.size) for m in ("pooled", "drop0.5%", "strata", "ent_std")}
        k5 = {m: np.zeros(hs.size) for m in k3}
        share_top = []
        for i, h in enumerate(hs.astype(int)):
            dx = Xsub[h:] - Xsub[:-h]
            dx = dx - np.nanmean(dx, axis=1, keepdims=True)
            fin = np.isfinite(dx)
            v = dx[fin]
            ent = np.broadcast_to(np.arange(Xsub.shape[1])[None, :], dx.shape)[fin]
            # (1) pooled
            _, k3["pooled"][i], k5["pooled"][i] = cums(v)
            # k5 contribution share of top 0.5% |v|
            sd = float(np.std(v))
            vw = np.clip(v, -7 * sd, 7 * sd)
            vc = vw - vw.mean()
            thr = np.quantile(np.abs(vc), 0.995)
            share_top.append(float(np.sum(vc[np.abs(vc) >= thr] ** 5) / max(np.sum(vc ** 5), 1e-12)))
            # (2) drop the 0.5% of entities with largest 5th-power contribution
            c5_ent = np.bincount(ent, weights=np.abs(vc) ** 5, minlength=Xsub.shape[1])
            bad = c5_ent >= np.quantile(c5_ent, 0.995)
            keep = ~bad[ent]
            _, k3["drop0.5%"][i], k5["drop0.5%"][i] = cums(v[keep])
            # (3) strata-averaged
            k3s, k5s = [], []
            for rl in (0, 1, 2):
                for vl in (0, 1, 2):
                    m = (rank_lab[ent] == rl) & (vol_lab[ent] == vl)
                    if m.sum() >= 500:
                        _, a3, a5 = cums(v[m])
                        k3s.append(a3)
                        k5s.append(a5)
            k3["strata"][i] = float(np.mean(k3s)) if k3s else 0.0
            k5["strata"][i] = float(np.mean(k5s)) if k5s else 0.0
            # (4) entity-standardized (then rescaled by median sd so units are comparable)
            u = v / sd_i[ent]
            s_ref = float(np.median(sd_i))
            _, b3, b5 = cums(u)
            k3["ent_std"][i] = b3 * s_ref ** 3
            k5["ent_std"][i] = b5 * s_ref ** 5
        print(f"\n===== {pn} (K={K}, top-K present n={int(sel.sum())}) | share of kappa_5 from top 0.5% |dx|: " + " ".join(f"{s:.2f}" for s in share_top))
        out = {}
        for m in k3:
            inv = invert(hs, k3[m], k5[m])
            out[m] = dict(k3=k3[m].tolist(), k5=k5[m].tolist(), **inv)
            print(f"  {m:9s}: k3(h)=" + " ".join(f"{x:+.4f}" for x in k3[m]) + f" | hl={inv['hl']:.0f} C3={inv['C3']:.4f} C5={inv['C5']:.4f} "
                  f"mu_raw={inv.get('mu_raw', np.nan):.3f} mu={inv['mu']:.3f} lam={inv['lam']:.5f} Vs={inv.get('Vs', np.nan):.4f} maxjump@K={inv.get('max_jump_rankK', np.nan):.2f}")
        res[pn] = dict(hs=hs.tolist(), share_top5=share_top, **out)
    (HERE / "diag_surge_cumulants.json").write_text(json.dumps(res, indent=1, default=float))


if __name__ == "__main__":
    main(sys.argv[1:] or PANELS)
