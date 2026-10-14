"""Known-answer tests for the balance by rank out of equilibrium (mts_balance_law).
Planted random walks: stationary, with the head pressed down, and with the head too steep."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402

N, K, S, SIG2, BLOCK, NB = 12800, 800, 1.5, 0.02, 26, 4
C = SIG2 / (2 * S)


def long_frame(X):
    R = np.zeros(X.shape, dtype=np.int32)
    for t in range(X.shape[0]):
        o = ba._order(X[t])
        R[t, o] = np.arange(1, o.size + 1)
    per, ent = np.nonzero(np.isfinite(X))
    df = pd.DataFrame(dict(entity_id=[f"e{j:05d}" for j in ent], period=per, X=X[per, ent], rank=R[per, ent]))
    df["N"] = df.groupby("period")["entity_id"].transform("size")
    return df


def world(kind, seed=0):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    if kind == "steep_head":                      # the top K/4 start 1.6 times too steep
        cut = x[K // 4 - 1]
        x = np.where(x > cut, cut + 1.6 * (x - cut), x)
    out = np.empty((BLOCK * NB, N))
    for t in range(BLOCK * NB):
        step = rng.normal(0, np.sqrt(SIG2), N)
        if kind == "pressed_head":                # whoever is in the top K/8 this week loses 2C
            thr = np.partition(x, N - K // 8)[N - K // 8]
            step = step - 2 * C * (x >= thr)
        x = x + step
        out[t] = x
    return out


def rows(kind, pair):
    res = bl.analyse(long_frame(world(kind)), K, BLOCK, boot=40)
    p = [q for q in res["pairs"] if q["primary"] and q["start"] == pair][0]
    return {r["k"]: r for r in p["rows"]}


def test_stationary_walks_predict_and_show_no_change():
    r = rows("stationary", 2)
    for k in (100, 200, 400):
        assert abs(r[k]["pred"]) < 0.6 * C and abs(r[k]["obs_P"]) < 0.5 * C
    assert abs(r[K]["c"] - r[K]["sbar"]) < 0.3 * C            # at K the law is slide = diffusion


def test_a_pressed_head_sinks_at_the_predicted_rate():
    r = rows("pressed_head", 2)
    below = [r[k] for k in (25, 50, 100, 200, 400)]
    for x in below[:4]:
        assert x["obs_P"] < -0.3 * C and x["pred"] < -0.3 * C
        assert abs(x["pred"] - x["obs_P"]) < 0.7 * C
    sm = bl.summary(below)
    assert 0.6 < sm["slope"] < 1.4 and sm["rmse"] < 0.5 * sm["rmse_zero"]
    lag = bl.summary(below, "pred_lag")
    assert lag["rmse"] < 0.6 * lag["rmse_zero"]                # last year's slides still predict


def test_a_head_that_is_too_steep_is_predicted_to_sink():
    r = rows("steep_head", 1)
    assert r[100]["c"] < 0.85 * C                              # too steep: reordering returns less than the slide
    assert r[100]["pred"] < 0 and r[50]["pred"] < 0
    assert np.mean([r[k]["obs_P"] for k in (25, 50, 100)]) < 0
