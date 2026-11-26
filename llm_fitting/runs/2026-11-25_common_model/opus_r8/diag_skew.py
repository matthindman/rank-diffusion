#!/usr/bin/env python3
"""D2 (Round 8): where does the odd-moment (skewness) signal behind the S3 surge live?

Uses exactly the S3 surge estimator's preprocessing (top-K-median-centred X, permanent-rank <= K,
presence >= 0.5, per-week demeaned h-increments, 7-SD winsorization) and reports per panel and h:
  - kappa_3 and kappa_5 of the winsorized increments and the share of each carried by the top 0.5%
    of |increment| values,
  - Bowley quantile skewness at alpha = 0.25, 0.10, 0.05, 0.01,
  - the tail-asymmetry profile A(q) = P(dX > q s) - P(dX < -q s), s = IQR/1.349, q = 0.5, 1, 2, 3, 4, 6
    (exactly zero in expectation for ANY symmetric law, bounded influence),
  - the same at h = 13 split by permanent-rank tier (head K/16, middle K/16..K/2, outer K/2..K).
Reads only the four DEVELOPMENT caches; writes only into opus_r8/.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
PANELS = ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long")
QS = (0.5, 1.0, 2.0, 3.0, 4.0, 6.0)


def prep(pn):
    d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
    X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
    T, N = X.shape
    pen = np.where(np.isfinite(R), R, fl[:, None])
    pr = np.empty(N)
    pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
    pres = np.isfinite(R).mean(axis=0)
    med = np.array([np.nanmedian(X[t, np.isfinite(R[t]) & (R[t] <= K)]) for t in range(T)])
    Xc = X - med[:, None]
    return Xc, pr, pres, K


def stats(v):
    sd = float(np.std(v))
    vw = np.clip(v, -7 * sd, 7 * sd)
    vc = vw - vw.mean()
    v2, v3 = float(np.mean(vc ** 2)), float(np.mean(vc ** 3))
    k5 = float(np.mean(vc ** 5)) - 10 * v2 * v3
    cut = np.quantile(np.abs(vc), 0.995)
    top = np.abs(vc) > cut
    sh3 = float(np.sum(vc[top] ** 3) / np.sum(vc ** 3)) if abs(np.sum(vc ** 3)) > 0 else np.nan
    sh5 = float(np.sum(vc[top] ** 5) / np.sum(vc ** 5)) if abs(np.sum(vc ** 5)) > 0 else np.nan
    q = np.quantile(v, [0.01, 0.05, 0.10, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99])
    med = q[4]
    bow = {a: float((q[8 - i] + q[i] - 2 * med) / max(q[8 - i] - q[i], 1e-12)) for i, a in enumerate((0.01, 0.05, 0.10, 0.25))}
    s = (q[5] - q[3]) / 1.349
    vm = v - med
    A = {qq: float(np.mean(vm > qq * s) - np.mean(vm < -qq * s)) for qq in QS}
    return dict(sd=sd, rob_sd=float(s), k3=v3, k5=k5, k3_std=v3 / v2 ** 1.5, top05_share_k3=sh3, top05_share_k5=sh5, bowley=bow, A=A, n=int(v.size))


def main():
    out = {}
    for pn in (sys.argv[1:] or PANELS):
        Xc, pr, pres, K = prep(pn)
        T = Xc.shape[0]
        sel = (pr <= K) & (pres >= 0.5)
        tier = np.where(pr <= K / 16, 0, np.where(pr <= K / 2, 1, 2))
        res = {}
        print(f"\n===== {pn} T={T} K={K} n_sel={int(sel.sum())}")
        for h in (1, 4, 13, 26, 52):
            if h > T // 2:
                continue
            dx = Xc[h:, sel] - Xc[:-h, sel]
            dx = dx - np.nanmean(dx, axis=1, keepdims=True)
            st = stats(dx[np.isfinite(dx)])
            res[h] = {"all": st}
            print(f" h={h:2d} sd={st['sd']:.3f} rob={st['rob_sd']:.3f} k3={st['k3']:+.5f} (std {st['k3_std']:+.2f}, top0.5% share {st['top05_share_k3']:.2f}) "
                  f"k5 top0.5% share {st['top05_share_k5']:.2f} | Bowley .25/.10/.05/.01: " + " ".join(f"{st['bowley'][a]:+.3f}" for a in (0.25, 0.10, 0.05, 0.01))
                  + " | A(q): " + " ".join(f"{q}:{st['A'][q]:+.4f}" for q in QS))
            if h == 13 or (h == 4 and T < 60):
                for j, nm in enumerate(("head", "middle", "outer")):
                    m = tier[sel] == j
                    v = dx[:, m]
                    stj = stats(v[np.isfinite(v)])
                    res[h][nm] = stj
                    print(f"     {nm:6s} sd={stj['sd']:.3f} rob={stj['rob_sd']:.3f} k3std={stj['k3_std']:+.2f} Bowley .10/.01: {stj['bowley'][0.10]:+.3f} {stj['bowley'][0.01]:+.3f} | A(q): "
                          + " ".join(f"{q}:{stj['A'][q]:+.4f}" for q in QS))
        out[pn] = res
    (HERE / "diag_skew.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
