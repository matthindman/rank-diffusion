"""Known-answer tests for the variogram estimator of lasting variance (mts_variogram).
Planted random walks with passing swings (half-life 10 weeks) and weekly noise: lags a year
apart recover the lasting variance in strata of a few hundred members (precision is set by the
few independent year-long changes each member has, so smaller strata are noisy); a variance
that doubles after week 104 shows in the era ratio of large strata."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mts_variogram as vg  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

N, K, S, SIG2, WEEKS = 12800, 800, 1.5, 0.01, 208


def world(double=False, seed=3):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    kap, v = 0.07, 0.3
    z = rng.normal(0, np.sqrt(v), N)
    out = np.empty((WEEKS, N))
    for t in range(WEEKS):
        s2 = SIG2 * (2.0 if (double and t >= 104) else 1.0)
        x = x + rng.normal(0, np.sqrt(s2), N)
        z = z * np.exp(-kap) + rng.normal(0, np.sqrt(v * (1 - np.exp(-2 * kap))), N)
        out[t] = x + z + rng.normal(0, 0.3, N)          # and weekly noise
    return out


_C = {}


def res(double):
    if double not in _C:
        _C[double] = vg.analyse(long_frame(world(double)), K, boot=20)
    return _C[double]


def test_rolling_means_need_most_of_the_window():
    X = np.array([[1.0], [np.nan], [3.0], [5.0], [np.nan], [np.nan]])
    m = vg.rolling_means(X, w=4, min_frac=0.75)
    assert np.isclose(m[0, 0], 3.0) and np.isnan(m[1, 0]) and np.isnan(m[3, 0])


def test_lags_a_year_apart_recover_the_lasting_variance_despite_swings_and_noise():
    r = res(False)
    for v in r["pooled"]["strata"]:
        if v["n"] >= 300:
            assert abs(v["sigma2"] / SIG2 - 1) < 0.15, v


def test_a_doubled_variance_shows_in_the_era_ratio():
    r = res(True)
    for e in r["era_ratio"]:
        if e["hi"] >= 800:
            assert 1.4 < e["ratio"] < 2.5, e
