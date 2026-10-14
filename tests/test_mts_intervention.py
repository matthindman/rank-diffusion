"""Known-answer tests for the rolling-origin intervention forecasts (mts_intervention).
Planted walks; from week 104 whoever is in the top K/8 loses 3C a week.  At origins whose last
quarter is after the change, the law with last quarter's slides forecasts the next year's change of
the head's shape; the trend (last year's change, which straddles the change) and no change do not."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mts_intervention as iv  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

N, K, S, SIG2, WEEKS, START = 25600, 1600, 1.5, 0.01, 208, 104
C = SIG2 / (2 * S)


def world(seed=7):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    out = np.empty((WEEKS, N))
    for t in range(WEEKS):
        step = rng.normal(0, np.sqrt(SIG2), N)
        if t >= START:
            thr = np.partition(x, N - K // 8)[N - K // 8]
            step = step - 3 * C * (x >= thr)
        x = x + step
        out[t] = x
    return out


_C = {}


def res():
    if not _C:
        _C["r"] = iv.analyse(long_frame(world()), K)
    return _C["r"]


def test_after_a_change_in_drift_the_last_quarter_beats_the_trend():
    r = res()
    # ranks K/16 .. K/4: at K/64 .. K/32 the boundary strata hold 12-50 members and a quarter's slide is noise
    post = [x for o in r["origins"] if o["origin_block"] - 1 >= START // iv.Q for x in o["rows"] if K // 16 <= x["k"] <= K // 4]
    assert post, "no post-change origins"
    assert iv.rms(post, "LQ") < 0.7 * iv.rms(post, "T"), (iv.rms(post, "LQ"), iv.rms(post, "T"))
    assert iv.rms(post, "LQ") < 0.6 * iv.rms(post, "N"), (iv.rms(post, "LQ"), iv.rms(post, "N"))
