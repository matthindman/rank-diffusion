"""POST HOC (explaining the shortfall of 2z-cu): do communities next to each other on the ladder move
together?  Crossings at a boundary need neighbours in level to move relative to each other; if they
co-move, crossings fall short of sigma^2/2S, which takes the cross-sectional variance as if moves were
independent.  For the persisting set of blocks 1 and 5 (26-week blocks, 104 weeks apart), within the
band of block-1 ranks (k/2, 2k], members sorted by their block-1 level: the change over the 104 weeks,
with its local trend on block-1 level removed (a running median over 51 neighbours), and the
correlation of that change between members m places apart (m = 1, 2, 5, 10, 25) and between random
pairs in the band.  Implied crossing factor ~ 1 - rho(1).  No names are read.

    python llm_fitting/runs/2026-10-15_neighbour_comovement/neighbours.py <panel>
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
K = cfg["K"]
X, Rk, floors, outside, ids = m1.matrices(df, K)
T = X.shape[0]
nb = T // 26
start = T - nb * 26
M = ba.block_means(X, 26, start=start, min_frac=m1.PRES)
a, b = 1, 5
P = np.flatnonzero(np.isfinite(M[a]) & np.isfinite(M[b]))
order = P[np.argsort(-M[a][P])]
rng = np.random.default_rng(0)


def running_median(y, w=51):
    h = w // 2
    return np.array([np.median(y[max(0, i - h):i + h + 1]) for i in range(y.size)])


out = {}
for k in (K // 64, K // 32, K // 16, K // 8, K // 4, K // 2):
    band = order[max(0, k // 2):min(order.size, 2 * k)]
    d = M[b][band] - M[a][band]
    d = d - running_median(d)
    res = {}
    for m in (1, 2, 5, 10, 25):
        res[f"rho_{m}"] = float(np.corrcoef(d[:-m], d[m:])[0, 1])
    i = rng.integers(0, d.size, 20000)
    j = rng.integers(0, d.size, 20000)
    keep = i != j
    res["rho_random"] = float(np.corrcoef(d[i[keep]], d[j[keep]])[0, 1])
    res["n"] = int(band.size)
    out[str(k)] = res
    print(f"{panel} k={k}: n={band.size} " + " ".join(f"{key}={v:+.3f}" for key, v in res.items() if key != "n"), flush=True)
Path(__file__).with_name(f"neighbours__{panel}.json").write_text(json.dumps(out, indent=1))
