"""Known-answer tests for the detection of a change in drift by rank (mts_drift_detect)."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mts_drift_detect as dd  # noqa: E402
import mts_step1 as m1  # noqa: E402
from test_mts_balance_law import K, N, S, SIG2, long_frame  # noqa: E402

T, T_EVENT = 104, 60


def world(delta, seed=0):
    """Weekly random walks; from week T_EVENT the endpoints then ranked in the top K/16 drift down by delta."""
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    out = np.empty((T, N))
    hit = None
    for t in range(T):
        if t == T_EVENT:
            hit = x >= np.sort(x)[::-1][K // 16 - 1]
        step = rng.normal(0, np.sqrt(SIG2), N)
        if hit is not None:
            step = step - delta * hit
        x = x + step
        out[t] = x
    df = long_frame(out)
    df["ts"] = (np.datetime64("2020-01-06") + 7 * df["period"].to_numpy()).astype("datetime64[D]").astype(str)
    return df


def state(df):
    X, R, floors, outside, ids = m1.matrices(df, K)
    dates = df.groupby("period")["ts"].first().reindex(range(X.shape[0])).astype(str).tolist()
    return dict(X=X, R=R, floors=floors, Mw=dd.short_means(X), st=dd.strata(K), dates=dates, rng=np.random.default_rng(1))


def test_a_planted_change_at_the_head_is_found_within_weeks_and_none_is_found_without_one():
    event = str(np.datetime64("2020-01-06") + 7 * T_EVENT)
    # this small world's head is about 80 endpoints and its 4-week detectable change about 0.03 a week
    hit = dd.event_study(state(world(0.08)), K, event)
    assert hit["detection"]["4"]["head_detected_weeks_after"] is not None
    assert hit["detection"]["4"]["head_detected_weeks_after"] <= 6
    near = dd.event_study(state(world(0.03)), K, event)          # at the detectable size: found, but later
    assert near["detection"]["8"]["head_detected_weeks_after"] is not None
    calm = dd.event_study(state(world(0.0)), K, event)
    assert all(v["head_detected_weeks_after"] is None for v in calm["detection"].values())


def test_the_detectable_change_shrinks_with_window_length_and_stratum_size():
    out, _ = dd.analyse(world(0.0), K)
    mde = out["mde"]
    assert mde["26"]["head"] < mde["4"]["head"]
    assert mde["13"]["rel"][-1] < mde["13"]["rel"][1]             # the bottom stratum is big, the top ones small
