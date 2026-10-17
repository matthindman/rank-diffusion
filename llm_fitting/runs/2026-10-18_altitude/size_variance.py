"""Z1-Z3 of DESIGN_depth.md: the size-variance exponent as the firm literature measures it.

    python llm_fitting/runs/2026-10-18_altitude/size_variance.py <panel>
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

panel = sys.argv[1]
cfg = m1.PANELS[panel]
K = cfg["K"]
block, pairs, lasting = ((52, [(1, 2), (2, 3)], True) if panel.startswith("reddit") else (13, [(1, 5)], False))
df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
X, Rk, floors, outside, ids = m1.matrices(df, K)
del df
T = X.shape[0]
nb = T // block
start = T - nb * block
M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
pr, pres = ba.perm_rank(Rk, start, start + block, floors)
order = np.argsort(pr, kind="stable")
edges = [K // 64 * 2 ** i for i in range(7)]           # K/64, K/32, ..., K
strata = [order[lo:hi] for lo, hi in zip([0] + edges[:-1], edges)]
rng = np.random.default_rng(0)
B = 400


def beta(sizes, sds):
    ok = np.isfinite(sizes) & np.isfinite(sds) & (sds > 0)
    return float(-np.polyfit(sizes[ok], np.log(sds[ok]), 1)[0])


def stat(fun):
    pt = fun(lambda m: m)
    draws = [fun(lambda m: m[rng.integers(0, m.size, m.size)]) for _ in range(B)]
    return dict(beta=pt, q=[float(np.nanquantile(draws, 0.05)), float(np.nanquantile(draws, 0.95))])


out = dict(panel=panel, K=K, block=block, strata=[[int(lo), int(hi)] for lo, hi in zip([0] + edges[:-1], edges)], raw={}, lasting=None)
for a, b in pairs:
    def raw(pick, a=a, b=b):
        sizes, sds = [], []
        for s in strata:
            m = s[np.isfinite(M[a][s]) & np.isfinite(M[b][s])]
            m = pick(m)
            sizes.append(np.mean(M[a][m]))
            sds.append(np.std(M[b][m] - M[a][m], ddof=1))
        return beta(np.array(sizes), np.array(sds))
    r = stat(raw)
    out["raw"][f"{a}->{b}"] = r
    print(f"{panel}: raw one-year growth, blocks {a}->{b}: beta {r['beta']:+.3f} (5-95% {r['q'][0]:+.3f} to {r['q'][1]:+.3f})", flush=True)
if lasting:
    def last(pick):
        sizes, sds = [], []
        for s in strata:
            m = s[np.isfinite(M[1][s]) & np.isfinite(M[2][s]) & np.isfinite(M[3][s])]
            m = pick(m)
            v = np.var(M[3][m] - M[1][m], ddof=1) - np.var(M[2][m] - M[1][m], ddof=1)
            sizes.append(np.mean(M[1][m]))
            sds.append(np.sqrt(v) if v > 0 else np.nan)
        return beta(np.array(sizes), np.array(sds))
    r = stat(last)
    out["lasting"] = r
    print(f"{panel}: lasting (two-year minus one-year variance): beta {r['beta']:+.3f} (5-95% {r['q'][0]:+.3f} to {r['q'][1]:+.3f})", flush=True)
for s, (lo, hi) in zip(strata, out["strata"]):
    a, b = pairs[0]
    m = s[np.isfinite(M[a][s]) & np.isfinite(M[b][s])]
    print(f"   ranks {lo + 1:>6}-{hi:<6} n {m.size:>5}  size {np.mean(M[a][m]):6.2f}  sd of one-year change {np.std(M[b][m] - M[a][m], ddof=1):.3f}")
Path(__file__).with_name(f"size_variance__{panel}.json").write_text(json.dumps(out, indent=1))
