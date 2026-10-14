"""The paper's opening facts, descriptive (DEVELOPMENT data; no names): the ladder's shape by year
against how much the names on it move.  Per panel:
  - the ladder of block levels (log level minus the level at rank K) against log rank, per block;
  - turnover: the share of the top k of one block still in the top k one, two, three blocks later;
  - the distribution of one-block changes of log rank for the top K's members;
  - the weekly log-rank paths of a random sample of 150 endpoints in the first block's top K.
Blocks: 52 weeks on Reddit, 26 on Facebook Era A.

    python llm_fitting/runs/2026-10-15_puzzle/puzzle_data.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

panel = sys.argv[1]
cfg = m1.PANELS[panel]
df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
K, block = cfg["K"], cfg["block"]
X, Rk, floors, outside, ids = m1.matrices(df, K)
T = X.shape[0]
nb = T // block
start = T - nb * block
M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).str[:10].tolist()
grid = np.unique(np.round(np.logspace(0, np.log10(2 * K), 60)).astype(int))
out = dict(panel=panel, K=K, block=block, first_week=[dates[start + b * block] for b in range(nb)], ladders=[], turnover={}, rank_change={}, paths={})
for b in range(nb):
    lad = np.sort(M[b][np.isfinite(M[b])])[::-1]
    g = grid[grid <= lad.size]
    out["ladders"].append(dict(ranks=g.tolist(), level=(lad[g - 1] - lad[K - 1]).tolist()))
for k in (K // 64, K // 8, K):
    rows = []
    for lag in range(1, nb):
        sh = []
        for b in range(nb - lag):
            def top(bb):
                v = M[bb].copy()
                v[~np.isfinite(v)] = -np.inf
                return set(np.argsort(-v)[:k])
            sh.append(len(top(b) & top(b + lag)) / k)
        rows.append(float(np.mean(sh)))
    out["turnover"][str(k)] = rows
# one-block change of log rank among the top K at the start
for b in range(nb - 1):
    v0, v1 = M[b].copy(), M[b + 1].copy()
    r0 = np.full(v0.size, np.nan); o = np.argsort(-np.where(np.isfinite(v0), v0, -np.inf)); r0[o] = np.arange(1, o.size + 1)
    r1 = np.full(v1.size, np.nan); o = np.argsort(-np.where(np.isfinite(v1), v1, -np.inf)); r1[o] = np.arange(1, o.size + 1)
    m = (r0 <= K) & np.isfinite(v0) & np.isfinite(v1)
    d = np.log(r1[m] / r0[m])
    out["rank_change"][out["first_week"][b]] = dict(median_abs=float(np.median(np.abs(d))), q90_abs=float(np.quantile(np.abs(d), 0.9)),
                                                    share_moving_2x=float(np.mean(np.abs(d) > np.log(2))), n=int(m.sum()))
# weekly log-rank paths of 150 random members of the first block's top K (rank among everything observed that week)
rng = np.random.default_rng(1)
first = np.flatnonzero(np.isfinite(M[0]))
first = first[np.argsort(-M[0][first])][:K]
pick = rng.choice(first, size=min(150, first.size), replace=False)
paths = []
for j in pick:
    r = Rk[:, j].astype(float)
    r[r <= 0] = np.nan
    paths.append([None if not np.isfinite(x) else float(np.log10(x)) for x in r[::2]])
out["paths"] = dict(weeks_step=2, first_week=dates[0], log10_rank=paths)
Path(__file__).with_name(f"puzzle__{panel}.json").write_text(json.dumps(out))
print(panel, "turnover", {k: [round(x, 2) for x in v] for k, v in out["turnover"].items()})
print(panel, "rank change", {k: (round(v["median_abs"], 2), round(v["share_moving_2x"], 2)) for k, v in out["rank_change"].items()})
