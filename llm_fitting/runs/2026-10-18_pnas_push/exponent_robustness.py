"""A7 of PLAN.md: the headline size-variance exponent, made robust (same strata and blocks as
runs/2026-10-18_altitude/size_variance.py).

A7-1 leave-one-stratum-out beta, both years; A7-2 strata to rank 4K with absences counted as a fall to the
universe floor (the block mean of the level at rank 4K+1); A7-3 Facebook lasting beta (13-week blocks: two-step
minus one-step variance); A7-5 one-year retention of the top 10 and top 25; A7-6 fold change of the sd.
(A7-4, beta from item counts, is the portfolio test's items exponent, already recorded in 2z-dc.)

    python llm_fitting/runs/2026-10-18_pnas_push/exponent_robustness.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402


def beta(sizes, sds):
    sizes, sds = np.asarray(sizes), np.asarray(sds)
    ok = np.isfinite(sizes) & np.isfinite(sds) & (sds > 0)
    return float(-np.polyfit(sizes[ok], np.log(sds[ok]), 1)[0])


def order(L):
    o = np.flatnonzero(np.isfinite(L))
    return o[np.argsort(-L[o], kind="mergesort")]


out = {}
for panel in ("reddit_submissions_long", "reddit_comments_ext", "facebook_a"):
    cfg = m1.PANELS[panel]
    K = cfg["K"]
    reddit = panel.startswith("reddit")
    block, pairs = ((52, [(1, 2), (2, 3)]) if reddit else (13, [(1, 5)]))
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    del df
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
    pr, _ = ba.perm_rank(Rk, start, start + block, floors)
    order_pr = np.argsort(pr, kind="stable")
    edges = [K // 64 * 2 ** i for i in range(7)]
    strata = [order_pr[lo:hi] for lo, hi in zip([0] + edges[:-1], edges)]
    res = dict(K=K, block=block, loo={}, fold={}, extended={}, retention={})
    for a, b in pairs:
        sz, sd = [], []
        for s in strata:
            m = s[np.isfinite(M[a][s]) & np.isfinite(M[b][s])]
            sz.append(np.mean(M[a][m]))
            sd.append(np.std(M[b][m] - M[a][m], ddof=1))
        full = beta(sz, sd)
        loo = [beta(np.delete(sz, i), np.delete(sd, i)) for i in range(len(sz))]
        res["loo"][f"{a}->{b}"] = dict(full=full, loo=loo, sd=sd, size=sz)
        res["fold"][f"{a}->{b}"] = dict(end_to_end=float(sd[-1] / sd[0]), max_over_min=float(max(sd) / min(sd)))
        # A7-2: strata to 4K, absences at the next block counted at the universe floor
        ext_edges = [K // 64 * 2 ** i for i in range(9)]
        ext = [order_pr[lo:hi] for lo, hi in zip([0] + ext_edges[:-1], ext_edges)]
        fl = np.nanmean(outside[start + b * block:start + (b + 1) * block])
        sz2, sd2, nabs = [], [], []
        for s in ext:
            m = s[np.isfinite(M[a][s])]
            yb = np.where(np.isfinite(M[b][m]), M[b][m], fl)
            nabs.append(float(np.mean(~np.isfinite(M[b][m]))))
            sz2.append(np.mean(M[a][m]))
            sd2.append(np.std(yb - M[a][m], ddof=1))
        res["extended"][f"{a}->{b}"] = dict(beta=beta(sz2, sd2), sd=sd2, size=sz2, absent=nabs, floor=float(fl))
        print(f"{panel} {a}->{b}: beta {full:+.3f}; leave-one-out {np.round(loo, 3).tolist()}; fold end-to-end "
              f"{sd[-1] / sd[0]:.2f}, max/min {max(sd) / min(sd):.2f}; to 4K with absences at the floor {res['extended'][f'{a}->{b}']['beta']:+.3f} "
              f"(absent share by stratum {np.round(nabs, 3).tolist()})", flush=True)
    if not reddit:
        # A7-3: lasting beta on Facebook from 13-week blocks: var(M[a+2]-M[a]) - var(M[a+1]-M[a]), averaged over a
        sz, sd = [], []
        for s in strata:
            vs, ss = [], []
            for a in range(1, nb - 2):
                m = s[np.isfinite(M[a][s]) & np.isfinite(M[a + 1][s]) & np.isfinite(M[a + 2][s])]
                vs.append(np.var(M[a + 2][m] - M[a][m], ddof=1) - np.var(M[a + 1][m] - M[a][m], ddof=1))
                ss.append(np.mean(M[a][m]))
            v = float(np.mean(vs))
            sz.append(float(np.mean(ss)))
            sd.append(np.sqrt(v) if v > 0 else np.nan)
        res["lasting_fb"] = dict(beta=beta(sz, sd), sd=sd, size=sz)
        print(f"{panel}: lasting beta (13-week blocks, two-step minus one-step) {res['lasting_fb']['beta']:+.3f}; lasting sd by stratum {np.round(sd, 3).tolist()}", flush=True)
    # A7-5: retention of the top 10 and top 25 over one year (Reddit: consecutive 52-week blocks; Facebook: 26-week blocks)
    B2 = M if reddit else ba.block_means(X, 26, start=T - (T // 26) * 26, min_frac=m1.PRES)
    for k in (10, 25):
        rr = []
        for a in range(B2.shape[0] - 1):
            oa, ob = order(B2[a])[:k], order(B2[a + 1])[:k]
            rr.append(float(np.intersect1d(oa, ob).size / k))
        res["retention"][k] = rr
    print(f"{panel}: retention of the top 10 by pair {res['retention'][10]}; top 25 {res['retention'][25]}", flush=True)
    out[panel] = res
Path(__file__).with_name("exponent_robustness.json").write_text(json.dumps(out, indent=1, default=float))
