"""Known-answer tests for the seasonal forecasts of half-year shape changes (mts_seasonal).
Planted walks whose top K/8 gains in one half of every year and loses in the other, the
halves switching at the middle of the 26-week blocks (so that a change from one block's
middle to the next lies within one season, as April-September does on Reddit): the same
season's slides forecast each half-year's change; last half-year's slides get the sign wrong."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mts_seasonal as se  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

N, K, S, SIG2, BLOCK, NB = 12800, 800, 1.5, 0.01, 26, 8
C = SIG2 / (2 * S)


def world(kind, seed=5):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    out = np.empty((BLOCK * NB, N))
    for t in range(BLOCK * NB):
        step = rng.normal(0, np.sqrt(SIG2), N)
        thr = np.partition(x, N - K // 8)[N - K // 8]
        if kind == "seasonal":                       # the top K/8 gains 3C in odd half-years, loses 3C in even
            step = step + (3 * C if ((t + BLOCK // 2) // BLOCK) % 2 else -3 * C) * (x >= thr)
        elif kind == "persistent":                   # the top K/8 loses 3C throughout
            step = step - 3 * C * (x >= thr)
        x = x + step
        out[t] = x
    return out


_C = {}


def rows(kind):
    if kind not in _C:
        res = se.analyse(long_frame(world(kind)), K)
        _C[kind] = [x for t in res["targets"] for x in t["rows"] if K // 32 <= x["k"] <= K // 8]
    return _C[kind]


def test_a_seasonal_head_is_forecast_by_the_same_season_and_not_by_the_last_half_year():
    r = rows("seasonal")
    # one small world, 12 cells: the ordering is the check (2026-10-15: F2 3.55, F0 4.02, F1 6.92 per 1,000 weeks)
    assert se.rms(r, "F2") < se.rms(r, "F0")
    assert se.rms(r, "F2") < 0.6 * se.rms(r, "F1")
    assert se.rms(r, "E2") < 0.7 * se.rms(r, "E1")


def test_a_persistent_head_is_forecast_by_either_lag():
    r = rows("persistent")
    assert se.rms(r, "F1") < 0.7 * se.rms(r, "F0") and se.rms(r, "F2") < 0.7 * se.rms(r, "F0")
