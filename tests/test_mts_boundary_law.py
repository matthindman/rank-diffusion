"""Known-answer tests for the boundary theory by rank (mts_boundary_law).
Planted random walks on an exponential ladder (slope S, lasting variance SIG2): the lasting
crossing gain at every rank is SIG2 / (2 S); passing swings and block averaging add one-time
constants of opposite sign that a difference of two spacings removes.  Tolerances come from
three seeds of the same worlds (2026-10-14): seed-to-seed spread of the crossing ratio is
about 5% at k >= 400 and 20% at k <= 100."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mts_boundary_law as bd  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

N, K, S, SIG2, BLOCK, NB = 25600, 1600, 1.5, 0.02, 26, 8
C = SIG2 / (2 * S)
BODY = (200, 400, 800, 1600)


def world(kind, seed=11):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    z = np.zeros(N)
    kap, v = 0.07, 0.3                               # passing swings: 10-week half-life
    if kind == "swings":
        z = rng.normal(0, np.sqrt(v), N)
    out = np.empty((BLOCK * NB, N))
    for t in range(BLOCK * NB):
        step = rng.normal(0, np.sqrt(SIG2), N)
        if kind == "pressed_head":                   # whoever is in the top K/8 this week loses 2C
            thr = np.partition(x, N - K // 8)[N - K // 8]
            step = step - 2 * C * (x >= thr)
        x = x + step
        if kind == "swings":
            z = z * np.exp(-kap) + rng.normal(0, np.sqrt(v * (1 - np.exp(-2 * kap))), N)
        out[t] = x + z
    return out


_CACHE = {}


def pooled(kind):
    if kind not in _CACHE:
        res = bd.analyse(long_frame(world(kind)), K, BLOCK, boot=10)
        _CACHE[kind] = {r["k"]: r for r in res["pooled"]}, res
    return _CACHE[kind]


def theory(r):
    return SIG2 / (2 * r["S"])


def test_the_split_is_exact_and_a_reordering_only_gains():
    _, res = pooled("stationary")
    for s in res["starts"]:
        for rows in s["single"].values():
            for r in rows:
                assert r["R"] >= -1e-12
                assert abs((r["R"] - r["E"]) - r["obs"]) < 1e-12


def test_stationary_walks_cross_at_sigma2_over_2S_at_every_rank():
    p, _ = pooled("stationary")
    for k in BODY:
        assert 0.8 < p[k]["R_lasting"] / theory(p[k]) < 1.25, (k, p[k]["R_lasting"] / theory(p[k]))
        assert abs(p[k]["E_lasting"] - p[k]["L_lasting"]) < 0.2 * C
        assert p[k]["E_one"] - p[k]["L_one"] < -0.05 * C       # block averaging: the current top carries momentum


def test_passing_swings_add_a_one_time_fall_back_that_the_difference_removes():
    p, _ = pooled("swings")
    q, _ = pooled("stationary")
    shift = np.mean([(p[k]["E_one"] - p[k]["L_one"]) - (q[k]["E_one"] - q[k]["L_one"]) for k in BODY])
    assert shift > 0.1 * C, shift                                 # whoever is on top through a swing falls back
    for k in BODY:
        assert 0.8 < p[k]["R_lasting"] / theory(p[k]) < 1.25, (k, p[k]["R_lasting"] / theory(p[k]))
        assert abs(p[k]["E_lasting"] - p[k]["L_lasting"]) < 0.2 * C


def test_a_pressed_head_sinks_and_crosses_at_the_theory_rate_below_the_press():
    p, _ = pooled("pressed_head")
    for k in (400, 800, 1600):
        assert 0.75 < p[k]["R_lasting"] / theory(p[k]) < 1.33, (k, p[k]["R_lasting"] / theory(p[k]))
    for k in (25, 50, 100):
        assert p[k]["obs_one"] < -0.3 * C
