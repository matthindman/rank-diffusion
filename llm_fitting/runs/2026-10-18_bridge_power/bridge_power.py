"""Power for the redesigned bridge protocol's boundary test (planted worlds only; no data).

The bridge gives one start: a cohort block, then blocks a, a+2, a+4 of 26 weeks (156 weeks in all). In the
test-suite world (N 25,600, K 1,600, S 1.5, lasting variance 0.02 a week), with one start: the pooled ratio
R~/c, the geometric mean over K/8 ... K, over 20 seeds of a plain walk, of rare jumps, and of the rival that
the current band [0.75, 1.33] admits (fixed volatility types still sorting, 2z-cq).  Gives the margin a single
start supports, and how often the rival falls inside it.

    python llm_fitting/runs/2026-10-18_bridge_power/bridge_power.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting" / "runs" / "2026-10-15_boundary_robustness"))
import planted_robustness as pr  # noqa: E402

pr.WEEKS = 156
KS = (200, 400, 800, 1600)
out = {}
for kind in ("plain", "jumps", "volatility_types"):
    pooled = []
    for seed in range(100, 120):
        res = pr.bd.analyse(pr.long_frame(pr.world(kind, seed)), pr.K, 26, boot=5)
        rat = {r["k"]: r["ratio"] for r in res["pooled"]}
        pooled.append(float(np.exp(np.mean([np.log(rat[k]) for k in KS]))))
    p = np.array(pooled)
    out[kind] = dict(pooled=pooled, mean=float(p.mean()), sd=float(p.std(ddof=1)), q05=float(np.quantile(p, 0.05)), q95=float(np.quantile(p, 0.95)))
    print(f"{kind:17s} pooled K/8..K over 20 seeds: mean {p.mean():.3f}, sd {p.std(ddof=1):.3f}, 5-95% {np.quantile(p, 0.05):.3f}-{np.quantile(p, 0.95):.3f}", flush=True)
kinds = list(out)
margins = {}
for lo, hi in ((0.75, 1.33), (0.85, 1.18), (0.88, 1.14)):
    margins[f"[{lo}, {hi}]"] = {k: float(np.mean([(lo <= v <= hi) for v in out[k]["pooled"]])) for k in kinds}
    print(f"margin [{lo}, {hi}]: share inside — " + ", ".join(f"{k} {margins[f'[{lo}, {hi}]'][k]:.2f}" for k in kinds), flush=True)
out["margins"] = margins
Path(__file__).with_name("bridge_power.json").write_text(json.dumps(out, indent=1))
