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


def test_predictable_loss_and_occupancy_shapes():
    X = lasting_plus_noise("rw", n=3000, seed=5)
    K = 600
    R = weekly_ranks(X)
    M = ba.block_means(X, 52)
    c = ba.centre_series(M, K)
    pr, pres = ba.perm_rank(R, 0, 52)
    labels, which = ba.cohort_bins(pr, K, pres)
    rows = ba.cohort_drift(M, c, which, len(labels), 1, 2)
    ks = [K // 8, K // 2, K]
    pl = ba.predictable_loss(M, c, which, rows, 1, ks)
    acc = ba.topk_accounting(M[1], M[2], ks, K)
    for p, a in zip(pl, acc):
        assert sum(p["bin_counts"]) == p["k"]
        assert p["G_pred_lo"] <= p["G_pred"] <= p["G_pred_hi"]
        assert np.isfinite(a["R"])
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


def test_runner_end_to_end_on_a_synthetic_long_panel():
    """mts_step1.analyse on a long panel: the identity closes in every cell,
    the structure is JSON-ready, and the registered-panel entry point refuses
    to start before the protocol is reviewed."""
    import json

    import pandas as pd

    import mts_step1 as m1

    X = lasting_plus_noise("rw", n=3000, T=160, seed=7)
    X[:, -50:][:40] = np.nan                       # 50 endpoints enter late
    T, n = X.shape
    R = weekly_ranks(X)
    per, ent = np.nonzero(np.isfinite(X))
    df = pd.DataFrame(dict(entity_id=[f"e{j:05d}" for j in ent], period=per, X=X[per, ent], rank=R[per, ent]))
    df["N"] = df.groupby("period")["entity_id"].transform("size")
    res = m1.analyse(df, K=300, block=40, horizons=(1, 13), boundaries=(1.0,), boot=50)
    json.dumps(res, default=float)
    cell = res["boundaries"]["1.0"]
    assert res["blocks"]["n"] == 4 and res["n_tracked"] <= n
    for w in cell["weekly"].values():
        assert w["max_abs_resid"] < 1e-9
    pairs = cell["blocks"]["pairs"]
    assert len(pairs) == 4                          # select 0: (1,2), (1,3), (2,3); select 1: (2,3)
    for p in pairs:
        for a in p["accounting"]:
            assert abs(a["resid"]) < 1e-9 * a["k"]
    assert any("home" in p.get("carriers", {}) for p in pairs)
    with pytest.raises(SystemExit):
        m1.run("facebook_a", reviewed=False)
