"""Known-answer tests for the balance accounting (movement-to-structure, step 1).

Every panel here is synthetic.  The generators are small on purpose: the
recovery study on planted rivals at real panel sizes is Codex's task, with its
own simulator.
"""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import balance_accounting as ba  # noqa: E402


def weekly_ranks(X):
    R = np.zeros(X.shape, dtype=np.int32)
    for t in range(X.shape[0]):
        o = ba._order(X[t])
        R[t, o] = np.arange(1, o.size + 1)
    return R


def atlas(n=1500, burn=3000, T=300, g=0.01, a=0.01, bottom=0.1, seed=0):
    """Drift by position only: -g for everyone above the bottom tenth, the
    compensating push for the bottom tenth.  Stationary slope a / (2 g)."""
    rng = np.random.default_rng(seed)
    x = np.sort(rng.normal(0, 1, n))[::-1].copy()
    cut = int((1 - bottom) * n)
    up = g * (1 - bottom) / bottom
    out = np.empty((T, n))
    for t in range(burn + T):
        rank = np.empty(n, dtype=int)
        rank[np.argsort(-x, kind="mergesort")] = np.arange(n)
        x = x + np.where(rank < cut, -g, up) + np.sqrt(a) * rng.standard_normal(n)
        if t >= burn:
            out[t - burn] = x
    return out


def lasting_plus_noise(kind, n=6000, T=208, seed=0, sd_step=0.03, kappa=0.02, sd_home=0.6,
                       phi=0.5, sd_noise=0.3):
    """Weekly levels: a wide spread of fixed starting levels, a lasting part
    (random walk, or reversion to the endpoint's own home), and AR(1) noise."""
    rng = np.random.default_rng(seed)
    base = rng.normal(0, 2.0, n)
    if kind == "rw":
        last = np.cumsum(sd_step * rng.standard_normal((T, n)), axis=0)
    else:
        last = np.empty((T, n))
        d = sd_home * rng.standard_normal(n)
        s = sd_home * np.sqrt(1 - np.exp(-2 * kappa))
        for t in range(T):
            d = d * np.exp(-kappa) + s * rng.standard_normal(n)
            last[t] = d
    e = np.empty((T, n))
    z = sd_noise * rng.standard_normal(n)
    for t in range(T):
        z = phi * z + sd_noise * np.sqrt(1 - phi * phi) * rng.standard_normal(n)
        e[t] = z
    return base[None, :] + last + e


def test_identity_is_exact_with_entry_and_exit():
    rng = np.random.default_rng(1)
    n = 800
    L0, L1 = rng.normal(0, 1, n), rng.normal(0, 1, n)
    L0[rng.random(n) < 0.15] = np.nan
    L1[rng.random(n) < 0.15] = np.nan
    acc = ba.topk_accounting(L0, L1, [10, 50, 200, 400], k_centre=400)
    for c in acc:
        assert abs(c["resid"]) < 1e-9 * c["k"]
        assert c["n_climb"] + c["n_enter"] == c["n_fall"] + c["n_exit"]
        assert c["R"] >= 0
    assert abs(acc[-1]["shape"]) < 1e-9          # the centre's own k: no shape change by definition
    assert any(c["n_enter"] > 0 for c in acc) and any(c["n_exit"] > 0 for c in acc)


def test_identity_holds_when_nothing_moves():
    L = np.linspace(5, 0, 300)
    c = ba.topk_accounting(L, L.copy(), [30, 100], k_centre=200)[0]
    assert c["G"] == 0 and c["R"] == 0 and c["F"] == 0 and c["shape"] == 0


def test_position_drift_stationary_balance():
    """Drift by position, stationary: occupant loss = reordering gain = g, the
    small-step prediction k v / (2 S) agrees, and the shape does not move."""
    X = atlas()
    res = ba.accounting_series(X, 1, [150, 400, 900], k_centre=1200)
    g, r, rd = res["g"].mean(axis=0), res["r"].mean(axis=0), res["r_diff"].mean(axis=0)
    assert np.all(np.abs(res["resid"]) < 1e-9)
    assert np.allclose(g, 0.01, rtol=0.15), g
    assert np.allclose(r, 0.01, rtol=0.15), r
    assert np.allclose(rd, 0.01, rtol=0.25), rd
    assert np.all(np.abs(res["shape"].mean(axis=0)) < 0.15 * r)
    assert np.all(res["f"] == 0)
    S = ba.local_slope(ba.ladder(X[-1]), 200, 800)
    assert 0.4 < S < 0.6                          # a / (2 g) = 0.5


def test_random_walk_shows_no_restoring_drift_from_earlier_cohorts():
    """No restoring force: cohorts fixed on the first block slide together
    against the positional centre, with no difference between bins and no
    slope on either earlier level.  On weekly positions the occupants' loss
    per week is several times larger at one week than at 26, from noise."""
    X = lasting_plus_noise("rw")
    K = 1000
    R = weekly_ranks(X)
    M = ba.block_means(X, 52)
    c = ba.centre_series(M, K)
    pr, pres = ba.perm_rank(R, 0, 52)
    labels, which = ba.cohort_bins(pr, K, pres)
    rows = ba.cohort_drift(M, c, which, len(labels), 1, 2)
    inside = [r for r in rows[:-1] if r["n"] >= 40]
    w = np.array([r["n"] for r in inside], dtype=float)
    common = float((w * np.array([r["mean"] for r in inside])).sum() / w.sum())
    assert common < 0                             # the centre outgrows its incumbents
    for r in inside:
        half = (r["q95"] - r["q05"]) / 2
        assert abs(r["mean"] - common) < 3.5 * half / ba.Z90 + 0.01, (r, common)
    M4 = ba.block_means(X, 26)
    c4 = ba.centre_series(M4, K)
    pr4, pres4 = ba.perm_rank(R, 0, 52)
    _, which4 = ba.cohort_bins(pr4, K, pres4)
    reg = ba.carrier_regression(M4, c4, b_pos=3, b_start=4, b_end=6, mask=which4 < which4.max(), b_home=1)
    for nm in ("pos", "home"):
        assert reg[nm + "_q05"] - 0.02 < 0 < reg[nm + "_q95"] + 0.02, reg
    wk1 = ba.accounting_series(X, 1, [K // 4], K, origins=np.arange(0, 150, 10))["g"].mean()
    wk26 = ba.accounting_series(X, 26, [K // 4], K, origins=np.arange(0, 150, 10))["g"].mean()
    assert wk1 > 3 * wk26 > 0


def test_return_to_own_level_is_detected():
    X = lasting_plus_noise("home", seed=3)
    K = 1000
    R = weekly_ranks(X)
    M = ba.block_means(X, 26)
    c = ba.centre_series(M, K)
    pr, pres = ba.perm_rank(R, 0, 52)
    _, which = ba.cohort_bins(pr, K, pres)
    reg = ba.carrier_regression(M, c, b_pos=3, b_start=4, b_end=6, mask=which < which.max(), b_home=1)
    assert reg["pos_q95"] < 0 and reg["home_q05"] > 0, reg


def test_predictable_loss_covers_the_same_survivors_with_a_joint_interval():
    X = lasting_plus_noise("rw", n=3000, seed=5)
    K = 600
    R = weekly_ranks(X)
    M = ba.block_means(X, 52)
    M[2, ba._order(M[1])[5]] = np.nan              # one occupant of the top k leaves the record
    c = ba.centre_series(M, K)
    pr, pres = ba.perm_rank(R, 0, 52)
    labels, which = ba.cohort_bins(pr, K, pres)
    rows, draws = ba.cohort_drift(M, c, which, len(labels), 1, 2, return_draws=True)
    ks = [K // 8, K // 2, K]
    pl = ba.predictable_loss(M, c, which, rows, 1, 2, ks, draws=draws, outside_bin=len(labels) - 1)
    acc = ba.topk_accounting(M[1], M[2], ks, K)
    for p, a in zip(pl, acc):
        assert p["n_cohort_survivors"] + p["n_outside"] + p["n_exit"] == p["k"]
        assert sum(p["bin_counts"]) == p["n_cohort_survivors"]
        assert p["n_exit"] == a["n_exit"] == 1
        assert p["G_pred_q05"] <= p["G_pred"] <= p["G_pred_q95"]
    occ = ba.occupancy(which, len(labels), M[2], ks)
    assert len(occ) == len(labels) and all(len(o) == len(ks) for o in occ)
    assert occ[0][-1] > 0.9 and occ[-1][0] < 0.05


def test_block_means_presence_rule():
    X = np.arange(40, dtype=float).reshape(8, 5)
    X[0:3, 0] = np.nan                            # present 1 of 4 in block 0
    M = ba.block_means(X, 4, min_frac=0.7)
    assert np.isnan(M[0, 0]) and np.isfinite(M[1, 0]) and M.shape == (2, 5)
    with pytest.raises(ValueError):
        ba.topk_accounting(X[0], X[1], [10], 3)


def long_frame(X):
    import pandas as pd
    R = weekly_ranks(X)
    per, ent = np.nonzero(np.isfinite(X))
    df = pd.DataFrame(dict(entity_id=[f"e{j:05d}" for j in ent], period=per, X=X[per, ent], rank=R[per, ent]))
    df["N"] = df.groupby("period")["entity_id"].transform("size")
    return df


def test_runner_end_to_end_on_a_synthetic_long_panel():
    """mts_step1.analyse on a long panel: the identity closes in every cell,
    the structure is JSON-ready, every prediction gets one of the four
    statuses, and the registered-panel entry point refuses to start before
    the protocol is reviewed."""
    import json

    import mts_step1 as m1

    X = lasting_plus_noise("rw", n=3000, T=160, seed=7)
    X[:, -50:][:40] = np.nan                       # 50 endpoints enter late
    res = m1.analyse(long_frame(X), K=300, block=40, horizons=(1, 13), boundaries=(1.0,), boot=50)
    json.dumps(res, default=float)
    cell = res["boundaries"]["1.0"]
    assert res["blocks"]["n"] == 4 and res["n_tracked"] <= X.shape[1]
    assert min(res["blocks"]["certified_top"]) >= 300
    for w in cell["weekly"].values():
        assert w["max_abs_resid"] < 1e-9
    pairs = cell["blocks"]["pairs"]
    assert len(pairs) == 4                          # select 0: (1,2), (1,3), (2,3); select 1: (2,3)
    assert sum(p["role"].startswith("primary") for p in pairs) == 1
    assert sum(p["role"].startswith("replicate") for p in pairs) == 1
    for p in pairs:
        for a in p["accounting"]:
            assert abs(a["resid"]) < 1e-9 * a["k"]
    assert any("home" in p.get("carriers", {}) for p in pairs)
    sc = m1.score(res, "reddit")
    sc.pop("_slide_no_gap")
    assert set(sc) == {"A1", "A2", "A2b", "A3", "A4", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B9", "B10", "screen"}
    assert all(v["status"] in ("PASS", "FAIL", "INCONCLUSIVE", m1.NA) for v in sc.values())
    assert sc["A1"]["status"] == "PASS"
    assert sc["B4"]["status"] != "FAIL"            # a random walk: no association with the earlier level
    json.dumps(sc, default=float)
    with pytest.raises(SystemExit):
        m1.run("facebook_a", reviewed=False)


# ---- Codex's review R4: each counterexample kept as a test --------------------------------------
def test_reordering_gain_is_not_additive_over_subintervals():
    path = np.array([[1.0, -1.0], [-1.0, 1.0], [1.0, -1.0]])
    gain = lambda a, b: ba.topk_accounting(a, b, [1], 2)[0]["R"]
    assert gain(path[0], path[2]) == 0.0
    assert gain(path[0], path[1]) + gain(path[1], path[2]) == 4.0


def test_carrier_signs_do_not_identify_a_mechanism():
    """Position-only OU in 52-week blocks (exact covariances): the slope on the
    still earlier block is positive with no personal home anywhere.  A random
    walk plus a slowly decaying component gives the same pattern."""
    kappa, block = 0.01, 52
    t = np.arange(block)
    cov = np.array([np.exp(-kappa * np.abs(t[:, None] - (t[None, :] + j * block))).mean() for j in range(4)])
    beta = np.linalg.solve([[cov[0], cov[1]], [cov[1], cov[0]]], [cov[2] - cov[1], cov[3] - cov[2]])
    assert beta[0] < -0.3 and beta[1] > 0.05
    X = lasting_plus_noise("rw", n=6000, T=208, seed=1000, sd_step=0.005, phi=0.98, sd_noise=0.6)
    K = 1000
    M = ba.block_means(X, 52)
    c = ba.centre_series(M, K)
    pr, pres = ba.perm_rank(weekly_ranks(X), 0, 52)
    labels, which = ba.cohort_bins(pr, K, pres)
    reg = ba.carrier_regression(M, c, 1, 2, 3, which < len(labels) - 1, b_home=0)
    assert reg["pos_q95"] < 0 and reg["home_q05"] > 0
    # and the cohort chosen on block 0 falls from block 1 to 2 with no drift in the lasting level
    rows = ba.cohort_drift(M, np.zeros(4), which, len(labels), 1, 2)
    inside = [r for r in rows[:-1] if "mean" in r]
    assert sum(r["n"] * r["mean"] for r in inside) / sum(r["n"] for r in inside) < -0.02


def test_cohort_drift_is_survivor_conditional_and_says_so():
    M = np.array([np.zeros(100), np.zeros(100), np.r_[np.ones(50), np.full(50, np.nan)]])
    row = ba.cohort_drift(M, np.zeros(3), np.zeros(100, dtype=int), 1, 1, 2)[0]
    assert row["mean"] == 1.0 and row["attrition"] == 0.5 and row["n_start"] == 100 and row["n"] == 50


def test_predictable_loss_weights_survivors_only():
    M = np.array([[10.0, 9.0, 8.0, 7.0], [9.0, np.nan, 7.0, 6.0]])
    c = ba.centre_series(M, 3)
    change = -1.0 - (c[1] - c[0])
    acc = ba.topk_accounting(M[0], M[1], [2], 3)[0]
    pred = ba.predictable_loss(M, c, np.zeros(4, dtype=int), [dict(mean=change)], 0, 1, [2])[0]
    assert pred["n_exit"] == acc["n_exit"] == 1 and pred["n_cohort_survivors"] == 1
    assert pred["G_pred"] == pytest.approx(acc["G"]) and pred["G_cohort"] == pytest.approx(acc["G"])


def test_interval_of_a_weighted_sum_is_joint_not_a_sum_of_brackets():
    rng = np.random.default_rng(0)
    draws = rng.standard_normal((40000, 2))
    M = np.array([[2.0, 1.0], [2.0, 1.0]])
    rows = [dict(mean=0.0), dict(mean=0.0)]
    p = ba.predictable_loss(M, np.zeros(2), np.array([0, 1]), rows, 0, 1, [2], draws=draws)[0]
    half = (p["G_pred_q95"] - p["G_pred_q05"]) / 2
    assert half == pytest.approx(ba.Z90 * np.sqrt(2), rel=0.03)      # 2.33, not 3.29


def test_uncertified_tracking_is_not_adjudicated():
    """480 endpoints rotate through the weekly top 48 and are otherwise zero;
    one more is always 9 and so is never tracked with K = 12, though its block
    mean is the largest.  Nothing that depends on the block ladder is scored."""
    import mts_step1 as m1

    K, block, T = 12, 10, 30
    X = np.zeros((T, 481))
    for t in range(T):
        X[t, (t % 10) * 48:(t % 10 + 1) * 48] = 10 + np.arange(48) * 0.001
    X[:, -1] = 9
    res = m1.analyse(long_frame(X), K, block, horizons=(), boundaries=(1.0,), boot=20)
    assert res["blocks"]["certified_top"] == [0, 0, 0]
    pairs = res["boundaries"]["1.0"]["blocks"]["pairs"]
    assert pairs and all("not_adjudicated" in p and "accounting" not in p for p in pairs)
    sc = m1.score(res, "reddit")
    assert sc["screen"]["status"] == m1.NA and sc["B1"]["status"] == m1.NA


def test_block_means_shorten_the_reordering_gain_and_not_the_slide():
    """Found in a self-review of step 1 (MODEL_STATUS 2z-bw).  Random walks on an
    exponential ladder: the level at every rank rises at sigma^2 / (2S) per week
    and the shape does not move.  On 52-week block means one year apart the slide
    of a cohort fixed earlier recovers that rate; the reordering gain and the
    occupants' loss come out near two thirds of it, because the difference of two
    L-week means D weeks apart has variance sigma^2 (D - L/3), not sigma^2 D."""
    rng = np.random.default_rng(0)
    N, K, S, sig2, L, nb = 20000, 2000, 1.5, 0.009, 52, 4
    X = S * np.log(N / np.arange(1, N + 1)) + np.cumsum(rng.normal(0, np.sqrt(sig2), (L * nb, N)), axis=0)
    M = X.reshape(nb, L, N).mean(axis=1)
    c = sig2 / (2 * S)
    centre = [np.sort(M[b])[::-1][:K].mean() for b in range(nb)]
    cohort = np.argsort(-M[0])[:K]
    slide = -((M[3, cohort] - M[2, cohort]).mean() - (centre[3] - centre[2])) / L
    rows = ba.topk_accounting(M[2], M[3], [K // 8, K // 4, K // 2, K], K)
    r = np.median([x["R"] / (x["k"] * L) for x in rows])
    g = np.median([x["G"] / (x["k"] * L) for x in rows])
    assert slide / c == pytest.approx(1.0, abs=0.15)
    assert r / c == pytest.approx(2 / 3, abs=0.1)
    assert g / c == pytest.approx(2 / 3, abs=0.2)
    assert r / slide < 0.8
