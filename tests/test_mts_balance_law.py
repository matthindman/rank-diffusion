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


def test_cutoffs_sit_on_the_strata_edges_even_when_those_are_not_whole_numbers():
    """K / 128 is fractional for the real panels (97.66 for K = 12,500).  Every cut-off must
    find the stratum that ends there and the one that begins there."""
    for KK in (12500, 5000, 800):
        st, ks = bl.strata(KK), bl.cutoffs(KK)
        assert [k for _, k in ks][-1] == KK and len(ks) == 7
        for j, k in ks:
            assert abs(st[j][1] - k) <= 0.5 and st[j + 1][0] == st[j][1]
    assert bl._at_boundary([1.0, 3.0, 5.0], [1, 1, 1], 1) == 4.0


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


def test_passing_swings_and_births_do_not_break_it():
    """The pressed head again, with AR(1) swings on every endpoint and a newcomer a week
    landing near the K-th level.  Entry is taken out exactly; the rest still follows the law."""
    X = world("pressed_head")
    rng = np.random.default_rng(100)
    T = X.shape[0]
    e = np.zeros((T, N))
    e[0] = rng.normal(0, np.sqrt(0.05), N)
    for i in range(1, T):
        e[i] = 0.8 * e[i - 1] + rng.normal(0, np.sqrt(0.05 * (1 - 0.8 ** 2)), N)
    X = X + e
    cols = []
    for w in range(BLOCK, T - 4):
        srt = np.sort(X[w])[::-1]
        path = np.full(T, np.nan)
        path[w:] = rng.uniform(srt[2 * K - 1], srt[K - 1] + 0.5) + np.cumsum(rng.normal(0, np.sqrt(SIG2), T - w))
        cols.append(path)
    res = bl.analyse(long_frame(np.column_stack([X] + cols)), K, BLOCK, boot=40)
    p = [q for q in res["pairs"] if q["primary"] and q["start"] == 2][0]
    below = [r for r in p["rows"] if 25 <= r["k"] < K]
    sm = bl.summary(below)
    assert 0.5 < sm["slope"] < 1.6 and sm["rmse"] < 0.6 * sm["rmse_zero"] and sm["sign_agree"] == sm["n"]
    assert all(r["entry"] <= 1e-12 for r in below)             # newcomers near K lift the centre, not the head


def test_half_year_starts_with_pairs_a_year_apart():
    """The same pressed head read with blocks half as long and pairs two blocks apart (the
    design of 2z-cg): the law still tracks the sinking, pair by pair, and lagged pairs exist."""
    res = bl.analyse(long_frame(world("pressed_head")), K, BLOCK // 2, boot=40, step=2)
    assert res["variance_blocks"] == [1, 3, 5] and len(res["pairs"]) == 5
    rows = [r for p in res["pairs"] for r in p["rows"] if 25 <= r["k"] < K]
    sm = bl.summary(rows)
    assert 0.5 < sm["slope"] < 1.6 and sm["rmse"] < 0.6 * sm["rmse_zero"]
    assert sum("pred_lag" in p["rows"][0] for p in res["pairs"]) == 3
