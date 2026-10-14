"""Planted-world robustness of the boundary test of 2z-cm (synthetic only; no data).
Exponential ladders (N = 25,600, K = 1,600, S = 1.5, lasting variance 0.02 a week), 8 blocks of
26 weeks, three seeds each.  Each world departs from the plain random walk in one way; the module's
own estimates (variance route, slope, crossings) give the ratio R~/c that the test scores.

    python llm_fitting/runs/2026-10-15_boundary_robustness/planted_robustness.py
"""
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(ROOT / "tests"))
import mts_boundary_law as bd  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

N, K, S, SIG2, WEEKS = 25600, 1600, 1.5, 0.02, 208


def world(kind, seed):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    b = np.zeros(N)
    sd = np.full(N, np.sqrt(SIG2))
    if kind == "drift_types":            # persistent named drifts: 156 * delta^2 = 0.3 sigma^2
        b = rng.normal(0, np.sqrt(0.3 * SIG2 / 156), N)
    if kind == "volatility_types":       # fixed volatility types, log-normal, mean variance SIG2
        v = rng.lognormal(0, 0.8, N)
        sd = np.sqrt(SIG2 * v / v.mean())
    out = np.empty((WEEKS, N))
    for t in range(WEEKS):
        if kind == "fat_tails":          # Student-t steps, 3 degrees of freedom, variance SIG2
            step = np.sqrt(SIG2 / 3.0) * rng.standard_t(3, N)
        elif kind == "jumps":            # rare large jumps carry half the variance
            step = rng.normal(0, np.sqrt(SIG2 / 2), N) + (rng.random(N) < 0.01) * rng.normal(0, np.sqrt(SIG2 / 2 / 0.01), N)
        else:
            step = rng.normal(0, 1, N) * sd
        x = x + b + step
        out[t] = x
    return out


if __name__ == "__main__":
    for kind in ("plain", "fat_tails", "jumps", "drift_types", "volatility_types"):
        acc = {}
        for seed in (21, 22, 23):
            res = bd.analyse(long_frame(world(kind, seed)), K, 26, boot=5)
            for r in res["pooled"]:
                acc.setdefault(r["k"], []).append(r["ratio"])
        print(kind.ljust(17), "  ".join(f"k={k}: {np.mean(v):.2f}±{np.std(v):.2f}" for k, v in acc.items()), flush=True)
