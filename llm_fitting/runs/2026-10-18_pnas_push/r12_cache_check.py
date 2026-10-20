"""R12, memory: check that the lean universe cache (r12_cache.py) is exact.  Recomputes the data auxiliary
statistics from the cached frame and compares them with r12_moments__<panel>.json, which the original path
(mrd.load_panel + mrd.restrict_universe) produced.  Writes nothing.

    R12_CACHE=<dir> python llm_fitting/runs/2026-10-18_pnas_push/r12_cache_check.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import r12_hetgrowth as h  # noqa: E402

pn = sys.argv[1]
df, K = h.load(pn)
X, R, floors, _, _ = h.m1.matrices(df, K)
a = h.aux(X, R, floors, K, h.GAPS_TEST)
saved = json.loads((HERE / f"r12_moments__{pn}.json").read_text())["aux"]
new = json.loads(json.dumps(a, default=float))
pairs = []
for g in saved["gapped"]:
    pairs += list(zip(saved["gapped"][g]["cov"], new["gapped"][g]["cov"])) + [(saved["gapped"][g]["rho"], new["gapped"][g]["rho"])]
pairs += list(zip(saved["sf"], new["sf"]))
pairs += [(x, y) for s_, n_ in zip(saved["wk"], new["wk"]) for x, y in zip(s_, n_)]
pairs += [(saved["asym"][k], new["asym"][k]) for k in ("fallers", "risers", "diff")]
d = np.array([abs(x - y) for x, y in pairs])
print(f"{pn}: {len(pairs)} statistics; identical {int((d == 0).sum())}; max abs difference {d.max():.3e}", flush=True)
sys.exit(0 if d.max() == 0 else 1)
