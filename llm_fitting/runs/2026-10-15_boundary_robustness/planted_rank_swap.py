"""Planted worlds (synthetic only): is "crossings = sigma^2 / 2S" automatic?  Two rivals to diffusion of
levels, each with the same exponential ladder (N = 25,600, K = 1,600, S = 1.5):
  - rank swaps: each week a name moves to a random rank with probability p (displacement), otherwise
    keeps its rank; levels are the ladder's level at the rank (as in rank-only models of Iñiguez et al.);
  - local swaps: each week adjacent pairs exchange ranks with probability 1/2 (a rank random walk).
The test of 2z-cm is run on each; plain diffusion of levels is the reference (planted_robustness.py).

    python llm_fitting/runs/2026-10-15_boundary_robustness/planted_rank_swap.py
"""
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(ROOT / "tests"))
import mts_boundary_law as bd  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

N, K, S, WEEKS = 25600, 1600, 1.5, 208
LADDER = S * np.log(N / np.arange(1, N + 1))


def world(kind, seed):
    rng = np.random.default_rng(seed)
    rank_of = rng.permutation(N)                     # rank (0-based) held by each name
    out = np.empty((WEEKS, N))
    for t in range(WEEKS):
        if kind == "rank_swaps":
            movers = np.flatnonzero(rng.random(N) < 0.01)
            if movers.size > 1:
                rank_of[movers] = rng.permutation(rank_of[movers])   # movers take each other's ranks, at random
        else:                                                         # local swaps of neighbours
            order = np.argsort(rank_of)
            start = t % 2
            a = order[start:-1:2]
            b = order[start + 1::2][:a.size]
            sw = rng.random(a.size) < 0.5
            ra, rb = rank_of[a[sw]].copy(), rank_of[b[sw]].copy()
            rank_of[a[sw]], rank_of[b[sw]] = rb, ra
        out[t] = LADDER[rank_of] + rng.normal(0, 0.05, N)
    return out


if __name__ == "__main__":
    for kind in ("rank_swaps", "local_swaps"):
        acc = {}
        for seed in (51, 52):
            res = bd.analyse(long_frame(world(kind, seed)), K, 26, boot=5)
            for r in res["pooled"]:
                acc.setdefault(r["k"], []).append((r["ratio"], r["sigma2"], r["R_lasting"]))
        print(kind.ljust(12), "  ".join(f"k={k}: ratio {np.mean([a for a, _, _ in v]):.2f}" for k, v in acc.items()), flush=True)
