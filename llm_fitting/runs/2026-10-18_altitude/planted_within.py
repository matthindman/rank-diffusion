"""Method check before any data (PROMPT.md, 'Depth'): does the within term of Gomez's accounting, measured on
arithmetic activity shares, recover the sign of incumbents' own drift when movement has large passing swings?

Worlds: N 25,600 on a Zipf-like ladder (S 1.0, the slope at the data's heads), K 1,600, lasting variance 0.02 a
week, 208 weeks, with passing swings (OU, 10-week half-life, variance 0.3, as tests/test_mts_boundary_law.py
'swings').  Drift by CURRENT rank, graded inside the top K: g(r) = g0 log(K/r)/log(K) for r <= K, 0 below,
with g0 = +0.004 a week ('advantage', rich get richer), 0 ('neutral'), -0.004 ('decay').  Activity =
exp(level + swing); shares are of the whole population each block.  (First design, S 1.5 with one common drift
for the top K: the normalisation cancels a common drift of a set that holds most of the activity, so every
world read about -0.01 a year; replaced, output kept in planted_within_output_v1.txt.)

Two estimators of the within term of the top k, on 26-week blocks, per year (log rate x 2):
  naive  : top k selected at block b, its share growth b -> b+1 (Gomez's one-step accounting);
  lagged : top k selected at block b, its share growth b+1 -> b+2 (selected one block before the window).
Expected if the lagged estimator works: neutral ~ 0, advantage > 0, decay < 0, while naive is pulled down.

    python llm_fitting/runs/2026-10-18_altitude/planted_within.py
"""
import json
from pathlib import Path

import numpy as np

N, K, S, SIG2, WEEKS, BLOCK = 25600, 1600, 1.0, 0.02, 208, 26
KAP, VSW = 0.07, 0.3


def world(g, seed):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    z = rng.normal(0, np.sqrt(VSW), N)
    act = np.empty((WEEKS, N))
    for t in range(WEEKS):
        rank = np.empty(N)
        rank[np.argsort(-x, kind="stable")] = np.arange(1, N + 1)
        x = x + rng.normal(0, np.sqrt(SIG2), N) + g * np.clip(np.log(K / rank) / np.log(K), 0, None)
        z = z * np.exp(-KAP) + rng.normal(0, np.sqrt(VSW * (1 - np.exp(-2 * KAP))), N)
        act[t] = np.exp(x + z)
    return act


def within_rates(act, k):
    nb = WEEKS // BLOCK
    W = np.array([act[b * BLOCK:(b + 1) * BLOCK].sum(0) for b in range(nb)])
    W = W / W.sum(1, keepdims=True)
    naive, lagged = [], []
    for b in range(nb - 2):
        T = np.argsort(-W[b])[:k]
        naive.append(np.log(W[b + 1, T].sum() / W[b, T].sum()) * 52 / BLOCK)
        lagged.append(np.log(W[b + 2, T].sum() / W[b + 1, T].sum()) * 52 / BLOCK)
    return float(np.mean(naive)), float(np.mean(lagged))


out = {}
for name, g in (("advantage", 0.004), ("neutral", 0.0), ("decay", -0.004)):
    res = {}
    for k in (100, 400):
        nv, lg = zip(*[within_rates(world(g, s), k) for s in (1, 2, 3)])
        res[k] = dict(naive=list(nv), lagged=list(lg))
        print(f"{name:9s} top {k:3d}: within per year — naive {np.mean(nv):+.3f} ({min(nv):+.3f} to {max(nv):+.3f}); "
              f"lagged {np.mean(lg):+.3f} ({min(lg):+.3f} to {max(lg):+.3f}); planted drift at rank k {52 * g * np.log(K / k) / np.log(K):+.3f}", flush=True)
    out[name] = res
Path(__file__).with_name("planted_within.json").write_text(json.dumps(out, indent=1))
