"""R13c-d of PLAN.md: one- and two-year retention of top sets (lock-in), and quantiles of the one-year change by
stratum of permanent rank (the 'funnel' check).  Usable on a data panel or on a simulated frame (measures()).

    python llm_fitting/runs/2026-10-18_pnas_push/lockin_funnel.py <reddit_submissions_long|reddit_comments_ext>
"""
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(HERE))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402

PRES = 0.9


def _order(L):
    o = np.flatnonzero(np.isfinite(L))
    return o[np.argsort(-L[o], kind="mergesort")]


def measures(df, K):
    X, Rk, floors, _, _ = m1.matrices(df, K)
    T = X.shape[0]
    nb = T // 52
    start = T - nb * 52
    M = ba.block_means(X, 52, start=start, min_frac=PRES)
    ks = [10, 25] + [K // 2 ** j for j in (6, 5, 4, 3, 2, 1)]
    ret = {}
    for k in ks:
        for h in (1, 2):
            vals = []
            for a in range(nb - h):
                oa, ob = _order(M[a]), _order(M[a + h])
                vals.append(np.intersect1d(oa[:k], ob[:k]).size / k)
            ret.setdefault(int(k), {})[h] = dict(mean=float(np.mean(vals)), lo=float(np.min(vals)), hi=float(np.max(vals)), n=len(vals))
    pr, _ = ba.perm_rank(Rk, start, start + 52, floors)
    op = np.argsort(pr, kind="stable")
    edges = [K // 64 * 2 ** i for i in range(7)]
    strata = []
    for lo, hi in zip([0] + edges[:-1], edges):
        s = op[lo:hi]
        m = s[np.isfinite(M[1][s]) & np.isfinite(M[2][s])]
        d = M[2][m] - M[1][m]
        ad = np.abs(d - np.median(d))
        strata.append(dict(lo=int(lo), hi=int(hi), n=int(m.size), size=float(np.mean(M[1][m])), sd=float(np.std(d, ddof=1)),
                           p50=float(np.percentile(ad, 50)), p90=float(np.percentile(ad, 90)), p95=float(np.percentile(ad, 95)),
                           max=float(ad.max())))
    return dict(retention=ret, strata=strata)


if __name__ == "__main__":
    import minimal_rankdiff as mrd  # noqa: E402
    pn = sys.argv[1]
    K = m1.PANELS[pn]["K"]
    df = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[pn]["platform"]])
    res = measures(df, K)
    res["panel"], res["K"] = pn, K
    (HERE / f"lockin_funnel__{pn}.json").write_text(json.dumps(res, indent=1))
    for k, v in res["retention"].items():
        print(f"{pn} k={k}: replaced in 1 yr {1 - v[1]['mean']:.3f}, in 2 yr {1 - v[2]['mean']:.3f}, ratio {(1 - v[2]['mean']) / max(1 - v[1]['mean'], 1e-9):.2f}", flush=True)
    for s in res["strata"]:
        print(f"{pn} stratum {s['lo']}-{s['hi']} (n {s['n']}): sd {s['sd']:.3f}  p50 {s['p50']:.3f}  p90 {s['p90']:.3f}  p95 {s['p95']:.3f}  max {s['max']:.3f}", flush=True)
