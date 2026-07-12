"""cond_home option on the conditional cohort sim (review finding 4):
'state' (committed default) must be byte-identical to the pre-option
behavior; 'trainmean' must actually move the OU anchor."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402


def _panel(n=60, T=25, seed=0):
    rng = np.random.default_rng(seed)
    base = np.sort(rng.normal(8.0, 1.5, n))[::-1]
    rows = []
    for t in range(T):
        # drifting entities: half trend up, half down, so train-mean != end level
        drift = np.where(np.arange(n) % 2 == 0, 0.03, -0.03) * t
        x = base + drift + rng.normal(0, 0.15, n)
        r = (-x).argsort().argsort() + 1
        for i in range(n):
            rows.append(dict(entity_id=f"e{i}", period=t, metric=float(np.expm1(x[i])),
                             X=x[i], rank=int(r[i]), N=n,
                             z=np.log((r[i] - 0.5) / n)))
    return pd.DataFrame(rows)


class TestCondHome(unittest.TestCase):
    def setUp(self):
        self.df = _panel()
        self.p = mrd.estimate(self.df)

    def test_state_mode_deterministic_and_default(self):
        a1, t1 = rk.sim_cohort_conditional(self.p, self.df, 6, 0.3, seed=0)
        a2, t2 = rk.sim_cohort_conditional(self.p, self.df, 6, 0.3, seed=0,
                                           cond_home="state")
        np.testing.assert_array_equal(np.nan_to_num(a1), np.nan_to_num(a2))
        np.testing.assert_array_equal(t1, t2)

    def test_trainmean_moves_the_anchor(self):
        a_state, _ = rk.sim_cohort_conditional(self.p, self.df, 6, 0.5, seed=0)
        a_mean, _ = rk.sim_cohort_conditional(self.p, self.df, 6, 0.5, seed=0,
                                              cond_home="trainmean")
        # same rng stream, same initial state; only the OU anchor differs --
        # with drifting entities and strong kappa the trajectories must diverge
        self.assertFalse(np.allclose(np.nan_to_num(a_state), np.nan_to_num(a_mean)))


if __name__ == "__main__":
    unittest.main()
