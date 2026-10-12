"""Known-answer tests for the residual-anatomy statistics (work plan A1, A2)."""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import exit_audit  # noqa: E402
import residual_anatomy as ra  # noqa: E402


def panel(n=600, T=160, kind="rw", seed=0, scales=None, df=None):
    """Synthetic (R, X): levels spread widely so ranks are stable; shocks by kind."""
    rng = np.random.default_rng(seed)
    level = np.sort(rng.normal(0, 3, n))[::-1][None, :]
    sc = np.ones(n) if scales is None else scales
    def draw(size):
        if df is None:
            return rng.standard_normal(size)
        return rng.standard_t(df, size) / np.sqrt(df / (df - 2.0))
    if kind == "rw":
        X = level + np.cumsum(0.05 * sc * draw((T, n)), axis=0)
    elif kind == "noise":
        X = level + 0.05 * sc * draw((T, n))
    elif kind == "mix":       # Gaussian random walk + heavy-tailed white noise
        X = level + np.cumsum(0.03 * rng.standard_normal((T, n)), axis=0) \
            + 0.08 * rng.standard_t(3, (T, n)) / np.sqrt(3.0)
    R = (np.argsort(np.argsort(-X, axis=1), axis=1) + 1).astype(np.int32)
    return R, X


def rows(table, **kw):
    return [r for r in table if all(r[k] == v for k, v in kw.items())]


def test_from_sim_splits_a_reborn_slot_into_two_identities():
    T = 6
    sim = dict(tranks=np.array([[1, 2]] * T), tvals=np.arange(2 * T, dtype=float).reshape(T, 2),
               tids=np.array([[10, 20], [10, 20], [10, 21], [10, 21], [10, 21], [10, 21]]))
    R, X = ra.from_sim(sim)
    assert R.shape == (T, 3)
    assert list((R > 0).sum(axis=0)) == [6, 2, 4]
    assert np.isnan(X[2:, 1]).all() and np.isnan(X[:2, 2]).all()


def test_retention_benchmarks_random_walk_and_white_noise():
    K = 500
    rw = ra.retention(*panel(kind="rw", seed=1), K)
    wn = ra.retention(*panel(kind="noise", seed=2), K)
    # small moves are a noisy reference at long lags (a ratio of small sums),
    # so they are checked at one week only
    for cls, lags in (("big_up", (1, 4, 13)), ("big_down", (1, 4, 13)), ("small_up", (1,))):
        for k in lags:
            a = rows(rw, group="middle", cls=cls, k=k)[0]["retention"]
            b = rows(wn, group="middle", cls=cls, k=k)[0]["retention"]
            assert a == pytest.approx(1.0, abs=0.12), (cls, k, a)
            assert b == pytest.approx(0.5, abs=0.08), (cls, k, b)


def test_tail_ratio_is_gaussian_for_a_gaussian_walk_and_fat_for_t_shocks():
    K = 500
    g = ra.tails_by_horizon(*panel(kind="rw", seed=3), K, horizons=(1, 4, 13))
    t = ra.tails_by_horizon(*panel(kind="rw", seed=3, df=3), K, horizons=(1, 4, 13))
    for h in (1, 4, 13):
        assert rows(g, group="middle", h=h, mode="group")[0]["r9990"] == pytest.approx(1.566, abs=0.12)
    assert rows(t, group="middle", h=1, mode="group")[0]["r9990"] > 1.9
    assert rows(t, group="middle", h=1, mode="group")[0]["p3"] > 0.008


def test_passing_fat_tails_fade_with_horizon():
    K = 500
    m = ra.tails_by_horizon(*panel(kind="mix", seed=4, T=220), K, horizons=(1, 13, 52))
    r1 = rows(m, group="middle", h=1, mode="group")[0]["r9990"]
    r52 = rows(m, group="middle", h=52, mode="group")[0]["r9990"]
    assert r1 > 1.9 and r52 < r1 - 0.25


def test_amplitude_spread_shows_in_group_scaling_and_not_in_entity_scaling():
    K, n = 500, 600
    scales = np.exp(np.random.default_rng(5).normal(0, 0.8, n))
    t = ra.tails_by_horizon(*panel(kind="rw", seed=6, scales=scales, n=n), K, horizons=(1,))
    plain = ra.tails_by_horizon(*panel(kind="rw", seed=6, n=n), K, horizons=(1,))
    grp = rows(t, group="middle", h=1, mode="group")[0]["r9990"]
    ent = rows(t, group="middle", h=1, mode="entity")[0]["r9990"]
    ent0 = rows(plain, group="middle", h=1, mode="entity")[0]["r9990"]
    assert grp > 1.9                      # a scale mixture looks fat-tailed
    assert ent == pytest.approx(ent0, abs=0.08)   # the endpoint's own scale cancels


def test_departure_classes_on_constructed_histories():
    K, T, t0 = 100, 80, 40
    R = np.zeros((T, 5), dtype=np.int32)
    R[:, 0] = 10                                   # never leaves
    R[:, 1] = 20; R[50:54, 1] = 150                # excursion: out 4 weeks, back
    R[:, 2] = 30; R[55:, 2] = 300                  # lasting: leaves and stays out
    R[:, 3] = 40; R[45:60, 3] = 90; R[60:, 3] = 130    # slides to the cut, then out
    R[:, 4] = 25; R[75:, 4] = 0                    # dies 5 weeks before the end
    ev, exposure = ra.departures(R, K, t0, floor=1001)
    by = {e["cls"]: e for e in ev}
    assert sorted(e["cls"] for e in ev) == ["censored", "excursion", "lasting", "slide"]
    assert by["excursion"]["back"] == 5 and by["excursion"]["kind"] == "crossing"
    assert by["lasting"]["back"] == np.inf and by["lasting"]["followed"] >= 13
    assert by["slide"]["slid"] and by["slide"]["margin"] == pytest.approx(0.10)
    assert by["censored"]["kind"] == "absence" and by["censored"]["followed"] == 5
    s = ra.departure_summary(ev, exposure)
    assert s["n"] == 4 and s["rate"] == pytest.approx(4 / exposure)
    assert s["rate_slide"] + s["rate_excursion"] + s["rate_lasting"] + s["rate_censored"] \
        == pytest.approx(s["rate"])


def test_departure_rate_equals_the_p7_estimand():
    """Same cohort, events and exposure as exit_audit.score_matrix."""
    rng = np.random.default_rng(7)
    K, T, t0, n = 300, 120, 70, 1500
    X = np.sort(rng.normal(0, 2, n))[::-1][None, :] + np.cumsum(rng.normal(0, 0.12, (T, n)), axis=0)
    R = (np.argsort(np.argsort(-X, axis=1), axis=1) + 1).astype(np.int32)
    R[rng.random((T, n)) < 0.02] = 0               # some absent weeks
    ref = exit_audit.score_matrix(R, t0, K, K // 2, 0.7, is_sim=False)
    ev, exposure = ra.departures(R, K, t0, floor=exit_audit.N_FLOOR)
    s = ra.departure_summary(ev, exposure)
    assert exposure == ref["risk"] and s["n"] == ref["crossings"] + ref["absences"]
    assert s["rate"] == pytest.approx(ref["rate"])
    assert s["rate_crossing"] * exposure == pytest.approx(ref["crossings"])


def test_long_dispersion_grows_for_a_walk_and_levels_off_for_noise():
    K = 500
    rw = ra.long_dispersion(*panel(kind="rw", seed=8, T=280), K, horizons=(13, 52, 104))
    wn = ra.long_dispersion(*panel(kind="noise", seed=9, T=280), K, horizons=(13, 52, 104))
    v = {r["h"]: r["var"] for r in rows(rw, group="middle")}
    w = {r["h"]: r["var"] for r in rows(wn, group="middle")}
    assert v[104] / v[13] == pytest.approx(8.0, rel=0.25)
    assert w[104] / w[13] == pytest.approx(1.0, rel=0.15)


# --------------------------------------------------------------------------- #
# A7 follow-up statistics
# --------------------------------------------------------------------------- #
def test_anchored_tail_ratio_does_not_depend_on_wandering():
    """A Gaussian walk scores the Gaussian value (1.57) at every horizon under
    the anchored scale, and well above it at long horizons under the entity
    scale; fat one-week shocks show at one week."""
    R, X = panel(n=3000, T=136, kind="rw")
    t = {(r["h"], r["mode"]): r["r9990"]
         for r in rows(ra.tails_by_horizon(R, X, 3000), group="middle")}
    assert abs(t[(26, "anchor")] - 1.57) < 0.05
    assert abs(t[(1, "anchor")] - t[(1, "entity")]) < 0.03
    assert t[(26, "entity")] > t[(26, "anchor")] + 0.10
    R, X = panel(n=3000, T=136, kind="rw", df=3)
    t = {(r["h"], r["mode"]): r["r9990"]
         for r in rows(ra.tails_by_horizon(R, X, 3000), group="middle")}
    assert t[(1, "anchor")] > 2.2


def test_momentum_benchmarks():
    n, T = 3000, 136
    rng = np.random.default_rng(5)

    def skip_adj(R, X):
        m = {r["pair"]: r["slope"] for r in rows(ra.momentum(R, X, n, block=26), group="middle")}
        return m["skip"], m["adjacent"]

    R, X = panel(n=n, T=T, kind="rw")
    skip, adj = skip_adj(R, X)
    assert abs(skip) < 0.05 and abs(adj - 0.25) < 0.05
    R, X = panel(n=n, T=T, kind="noise")
    skip, adj = skip_adj(R, X)
    assert abs(skip) < 0.07 and abs(adj + 0.5) < 0.06
    R, X = panel(n=n, T=T, kind="rw")                    # endpoint-specific trends
    X = X + rng.normal(0, 0.01, n) * np.arange(T)[:, None]
    skip, _ = skip_adj(R, X)
    assert skip > 0.4
    level = np.sort(rng.normal(0, 3, n))[::-1]           # a level that reverts
    ou, x = np.zeros((T, n)), rng.normal(0, 0.5, n)
    for t in range(T):
        x = 0.97 * x + rng.normal(0, 0.5 * np.sqrt(0.06), n)
        ou[t] = x
    skip, _ = skip_adj(R, level + ou)
    assert skip < -0.15


def test_volatility_persistence_fixed_and_switched():
    n, T = 3000, 136
    rng = np.random.default_rng(6)
    level = np.sort(rng.normal(0, 3, n))[::-1]
    a1, a2 = np.exp(rng.normal(0, 0.5, n)), np.exp(rng.normal(0, 0.5, n))
    sh = rng.normal(0, 0.05, (T, n))
    fixed = level + np.cumsum(sh * a1, axis=0)
    sw = sh.copy()
    sw[: T // 2] *= a1
    sw[T // 2:] *= a2
    switched = level + np.cumsum(sw, axis=0)
    R = (np.argsort(np.argsort(-fixed, axis=1), axis=1) + 1).astype(np.int32)
    v = rows(ra.volatility_persistence(R, fixed, n), group="middle")[0]
    assert abs(v["ratio"] - 1.0) < 0.03 and v["corr_interleaved"] > 0.8
    v = rows(ra.volatility_persistence(R, switched, n), group="middle")[0]
    assert v["corr_interleaved"] > 0.7 and v["corr_halves"] < 0.15


def test_cohort_fate_counts_endpoints_not_events():
    """100 endpoints at fixed ranks, K = 50, train weeks 0-39: the cohort is
    ranks 1-25.  Five of them sit at rank 45 through the scoring window
    (relocated, never out); two step to rank 60 at week 42 and stay (one exit
    each, lasting); one flickers across the cut every other week."""
    T, n, K, t0 = 80, 100, 50, 40
    R = np.tile(np.arange(1, n + 1), (T, 1)).astype(np.int32)
    R[t0:, 0:5] = 45
    R[42:, 5:7] = 60
    R[t0:, 7] = np.where(np.arange(t0, T) % 2 == 0, 45, 55)
    f = ra.cohort_fate(R, K, t0)
    assert f["n"] == 25
    assert f["share_relocated"] == pytest.approx(8 / 25)       # 5 at 45, 2 at 60, 1 flickering
    assert f["share_relocated_out"] == pytest.approx(2 / 25)   # mean rank beyond K: the two at 60 (about 57)
    assert f["share_ever_exit"] == pytest.approx(3 / 25)
    assert f["share_gone"] == 0.0
    ev, _ = ra.departures(R, K, t0)
    assert f["exits_per_exiting"] == pytest.approx(len(ev) / 3)
    assert len(ev) > 15                                         # the flickerer dominates the event count
    assert f["first_lasting"] == pytest.approx(2 / 3)


def test_momentum_tells_an_annual_cycle_from_a_trend():
    """A random walk plus an endpoint-specific annual cycle: quarter changes
    half a year apart are opposed, a year apart they repeat; whole-year
    blocks see only the walk."""
    n, T = 3000, 212
    rng = np.random.default_rng(8)
    R, X = panel(n=n, T=T, kind="rw")
    amp, phase = rng.normal(0, 0.3, n), rng.uniform(0, 2 * np.pi, n)
    X = X + amp * np.sin(2 * np.pi * np.arange(T)[:, None] / 52.0 + phase)
    q = {r["pair"]: r["slope"] for r in rows(ra.momentum(R, X, n, block=13, gaps=(2, 4)),
                                             group="middle")}
    assert q["gap2"] < -0.2 and q["gap4"] > 0.2
    y = {r["pair"]: r["slope"] for r in rows(ra.momentum(R, X, n, block=52), group="middle")}
    assert abs(y["skip"]) < 0.06 and abs(y["adjacent"] - 0.25) < 0.08


def test_amplitude_coherence_one_amplitude_against_two():
    """One amplitude for the walk and the noise gives 1.  A separate amplitude
    for the walk gives the correlation between the two population scales,
    which is known here (about 0.70 at 13 weeks)."""
    n, T, sw, sn, h = 3000, 212, 0.05, 0.079, 13
    rng = np.random.default_rng(9)
    level = np.sort(rng.normal(0, 4, n))[::-1]
    a, b = np.exp(rng.normal(0, 0.5, n)), np.exp(rng.normal(0, 0.5, n))
    walk, noise = rng.normal(0, sw, (T, n)), rng.normal(0, sn, (T, n))
    one = level + np.cumsum(walk * a, axis=0) + noise * a
    two = level + np.cumsum(walk * b, axis=0) + noise * a
    R = (np.argsort(np.argsort(-one, axis=1), axis=1) + 1).astype(np.int32)
    truth = np.corrcoef(0.5 * np.log(sw**2 * b**2 + 2 * sn**2 * a**2),
                        0.5 * np.log(h * sw**2 * b**2 + 2 * sn**2 * a**2))[0, 1]
    c1 = rows(ra.amplitude_coherence(R, one, n, h_long=h), group="middle")[0]["coherence"]
    c2 = rows(ra.amplitude_coherence(R, two, n, h_long=h), group="middle")[0]["coherence"]
    assert abs(c1 - 1.0) < 0.05
    assert abs(c2 - truth) < 0.08 and c2 < 0.85


def test_far_tail_separates_gaussian_from_t_shocks_beyond_the_99th_percentile():
    R, X = panel(n=2000, T=160, kind="rw")
    g = rows(ra.far_tail(R, X, 2000), group="middle")[0]
    R, X = panel(n=2000, T=160, kind="rw", df=4.3)
    t = rows(ra.far_tail(R, X, 2000), group="middle")[0]
    assert g["beyond6"] < 1e-5 and g["z_max_up"] < 7.5        # 127,000 Gaussian draws: about 4.5 to 6
    assert t["beyond6"] > 5e-4 and t["z_max_up"] > 12
    assert t["raw_max_up"] > 2 * g["raw_max_up"]

