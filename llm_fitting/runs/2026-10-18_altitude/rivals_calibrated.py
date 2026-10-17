"""V1-V3 of DESIGN_depth.md: rank-model rivals calibrated to the data's turnover, scored by the identical
boundary estimators (planted worlds only; no data).

    python llm_fitting/runs/2026-10-18_altitude/rivals_calibrated.py
"""
import json
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
TARGET, KT = 0.80, K // 4
KS = (200, 400, 800, 1600)


def step(kind, rank_of, rng, par):
    if kind == "displacement":
        movers = np.flatnonzero(rng.random(N) < par)
        if movers.size > 1:
            rank_of[movers] = rng.permutation(rank_of[movers])
    elif kind == "logrank":
        prop = np.log1p(rank_of) + rng.normal(0, par, N)
        rank_of[np.argsort(prop, kind="stable")] = np.arange(N)
    else:
        order = np.argsort(rank_of)
        start = rng.integers(0, 2)
        a, b = order[start:-1:2], order[start + 1::2]
        a = a[:b.size]
        sw = rng.random(a.size) < 0.5
        ra, rb = rank_of[a[sw]].copy(), rank_of[b[sw]].copy()
        rank_of[a[sw]], rank_of[b[sw]] = rb, ra
    return rank_of


def retention(kind, par, seed=0):
    rng = np.random.default_rng(seed)
    rank_of = rng.permutation(N)
    for _ in range(26):                                # burn-in
        rank_of = step(kind, rank_of, rng, par)
    top = rank_of < KT
    for _ in range(52):
        rank_of = step(kind, rank_of, rng, par)
    return float((rank_of[top] < KT).mean())


def calibrate(kind, lo, hi):
    for _ in range(18):
        mid = np.sqrt(lo * hi)
        if retention(kind, mid) > TARGET:
            lo = mid
        else:
            hi = mid
    return float(np.sqrt(lo * hi))


def world(kind, par, seed):
    rng = np.random.default_rng(seed)
    rank_of = rng.permutation(N)
    for _ in range(26):
        rank_of = step(kind, rank_of, rng, par)
    out = np.empty((WEEKS, N))
    for t in range(WEEKS):
        rank_of = step(kind, rank_of, rng, par)
        out[t] = LADDER[rank_of] + rng.normal(0, 0.05, N)
    return out


out = {}
pars = {"displacement": calibrate("displacement", 1e-4, 0.05), "logrank": calibrate("logrank", 1e-3, 1.0), "local_swaps": None}
for kind, par in pars.items():
    ret = retention(kind, par, seed=9)
    ratios, pooled = {}, []
    for seed in (61, 62, 63):
        res = bd.analyse(long_frame(world(kind, par, seed)), K, 26, boot=5)
        rat = {r["k"]: r["ratio"] for r in res["pooled"]}
        for k, v in rat.items():
            ratios.setdefault(int(k), []).append(float(v))
        pooled.append(float(np.exp(np.mean([np.log(rat[k]) for k in KS]))))
    out[kind] = dict(parameter=par, retention_topK4_1yr=ret, ratio_by_k={k: float(np.nanmean(v)) for k, v in ratios.items()}, pooled_K8_K=pooled)
    print(f"{kind:12s} parameter {par if par is None else round(par, 5)}; top-K/4 one-year retention {ret:.3f}; "
          f"ratio by k: " + ", ".join(f"{k}: {np.nanmean(v):.2f}" for k, v in sorted(ratios.items())) +
          f"; pooled K/8..K {np.mean(pooled):.3f} ({min(pooled):.3f}-{max(pooled):.3f})", flush=True)
Path(__file__).with_name("rivals_calibrated.json").write_text(json.dumps(out, indent=1))
