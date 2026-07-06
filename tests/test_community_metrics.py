"""Tests for community_metrics (2026-07-06 metrics audit): community-canonical
ranking observables (C(r), flux/turnover, rank diversity), ladder distances,
rolling-origin card variants, transition matrices, and the proper-score
primitives (ensemble CRPS / PIT / predictive quantile coverage) wired opt-in
into the OOS gate.  All synthetic -- no data files."""
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import community_metrics as cm  # noqa: E402


def _crps_brute(ens, y):
    ens = np.asarray(ens, dtype=float)
    n = ens.size
    t1 = np.mean(np.abs(ens - y))
    t2 = 0.5 * np.mean(np.abs(ens[:, None] - ens[None, :]))
    return t1 - t2


class TestCRPS(unittest.TestCase):
    def test_matches_brute_force(self):
        rng = np.random.default_rng(0)
        ens = rng.normal(size=137)
        obs = rng.normal(size=11)
        fast = cm.crps_sample(ens, obs)
        for j, y in enumerate(obs):
            self.assertAlmostEqual(fast[j], _crps_brute(ens, y), places=10)

    def test_degenerate_ensemble_is_abs_error(self):
        # CRPS of a point forecast reduces to absolute error
        self.assertAlmostEqual(cm.crps_sample(np.array([3.0]), np.array([7.0]))[0],
                               4.0, places=12)

    def test_propriety_direction(self):
        # forecasting with the true sample beats forecasting with a shifted one
        rng = np.random.default_rng(1)
        truth = rng.normal(size=4000)
        obs = rng.normal(size=2000)
        good = cm.crps_mean(truth, obs)
        bad = cm.crps_mean(truth + 2.0, obs)
        self.assertLess(good, bad)


class TestPITCoverage(unittest.TestCase):
    def test_pit_midrank(self):
        ens = np.array([1.0, 2.0, 3.0, 4.0])
        # y=2 -> (1 below + 0.5*1 tie)/4
        self.assertAlmostEqual(cm.pit_values(ens, np.array([2.0]))[0], 0.375)
        self.assertAlmostEqual(cm.pit_values(ens, np.array([0.0]))[0], 0.0)
        self.assertAlmostEqual(cm.pit_values(ens, np.array([9.0]))[0], 1.0)

    def test_coverage_calibrated_when_same_law(self):
        rng = np.random.default_rng(2)
        ens = rng.normal(size=20000)
        obs = rng.normal(size=20000)
        cov = cm.quantile_coverage(ens, obs, qs=(0.1, 0.5, 0.9))
        for q, v in cov.items():
            self.assertAlmostEqual(v, q, delta=0.02)


class TestRankChangeFluxDiversity(unittest.TestCase):
    def _alternating_topids(self, T=10, K=4):
        # rank 1 alternates ids 100/101; ranks 2..K constant ids 0..K-2
        ti = np.zeros((T, K), dtype=np.int64)
        for t in range(T):
            ti[t, 0] = 100 + (t % 2)
            ti[t, 1:] = np.arange(K - 1)
        return ti

    def test_rank_change_curve(self):
        ti = self._alternating_topids()
        grid, C = cm.rank_change_curve(ti, np.array([1, 2, 4]))
        self.assertAlmostEqual(C[0], 1.0)   # alternates every week
        self.assertAlmostEqual(C[1], 0.0)   # constant occupant
        self.assertAlmostEqual(C[2], 0.0)

    def test_flux_zero_for_closed_list(self):
        T, K = 8, 5
        ti = np.tile(np.arange(K, dtype=np.int64), (T, 1))
        ft = cm.flux_turnover(ti, K)
        self.assertAlmostEqual(ft["F"], 0.0)
        self.assertAlmostEqual(ft["o_dot"], 0.0)
        self.assertTrue(np.allclose(ft["o"], 1.0))

    def test_flux_one_for_full_replacement(self):
        T, K = 6, 3
        ti = np.arange(T * K, dtype=np.int64).reshape(T, K)  # all-new each week
        ft = cm.flux_turnover(ti, K)
        self.assertAlmostEqual(ft["F"], 1.0)
        self.assertAlmostEqual(ft["o_dot"], 1.0)  # (T - 1) / (T - 1)

    def test_rank_diversity(self):
        ti = self._alternating_topids(T=10, K=4)
        d = cm.rank_diversity(ti)
        self.assertAlmostEqual(d[0], 2 / 10)   # two distinct ids at rank 1
        self.assertAlmostEqual(d[1], 1 / 10)   # one id at rank 2


class TestRollingVariants(unittest.TestCase):
    def test_rolling_pers_constant_set(self):
        T, K = 20, 10
        ti = np.tile(np.arange(K, dtype=np.int64), (T, 1))
        out = cm.rolling_pers(ti, top_k=5, horizons=(1, 4))
        for h in (1, 4):
            m, s = out[f"Pers{h}"]
            self.assertAlmostEqual(m, 5.0)
            self.assertAlmostEqual(s, 0.0)

    def test_rolling_r2_perfect_persistence(self):
        rng = np.random.default_rng(3)
        base = rng.normal(size=30)
        V = np.tile(base, (12, 1))          # constant cross-section over time
        out = cm.rolling_r2(V, horizons=(1, 4))
        for h in (1, 4):
            m, s = out[f"R2_{h}"]
            self.assertAlmostEqual(m, 1.0, places=10)
            self.assertAlmostEqual(s, 0.0, places=10)


class TestTransitionMatrix(unittest.TestCase):
    def test_identity_dynamics(self):
        T, K = 12, 60
        ti = np.tile(np.arange(K, dtype=np.int64), (T, 1))
        M, mass, labels = cm.transition_matrix(ti, h=1, K=K)
        nb = len(labels) - 1                 # bands excl. 'out'
        self.assertTrue(np.allclose(M[:, :nb], np.eye(nb)))
        self.assertTrue(np.allclose(M[:, nb], 0.0))
        self.assertAlmostEqual(cm.transition_distance(M, M, mass), 0.0)

    def test_exit_goes_to_out(self):
        # single band (K < smallest edge); all occupants replaced each week
        T, K = 5, 6
        ti = np.arange(T * K, dtype=np.int64).reshape(T, K)
        M, mass, labels = cm.transition_matrix(ti, h=1, K=K)
        self.assertEqual(len(labels), 2)     # one band + out
        self.assertAlmostEqual(M[0, -1], 1.0)

    def test_distance_weighted_tv(self):
        Ma = np.array([[1.0, 0.0], [0.0, 1.0]])
        Mb = np.array([[0.0, 1.0], [0.0, 1.0]])
        mass = np.array([3.0, 1.0])
        # row 0: TV=1, weight .75; row 1: TV=0, weight .25
        self.assertAlmostEqual(cm.transition_distance(Ma, Mb, mass), 0.75)


class TestLadder(unittest.TestCase):
    def test_identical_curves_zero(self):
        rng = np.random.default_rng(4)
        rs = np.sort(rng.normal(5, 1, size=(9, 300)), axis=1)[:, ::-1]
        self.assertAlmostEqual(cm.ladder_rmse(rs, rs.copy()), 0.0)
        d, *_ = cm.share_distance(rs, rs.copy())
        self.assertAlmostEqual(d, 0.0)

    def test_constant_offset_recovered(self):
        rng = np.random.default_rng(5)
        rs = np.sort(rng.normal(5, 1, size=(9, 300)), axis=1)[:, ::-1]
        self.assertAlmostEqual(cm.ladder_rmse(rs, rs + 0.3), 0.3, places=10)

    def test_ladder_drift(self):
        rng = np.random.default_rng(6)
        base = np.sort(rng.normal(5, 1, size=300))[::-1]
        rs = np.tile(base, (12, 1))
        self.assertAlmostEqual(cm.ladder_drift(rs), 0.0)      # stationary
        trend = np.tile(base, (12, 1)) + 0.05 * np.arange(12)[:, None]
        # first-third mean t={0,1,2,3}->1.5, last-third t={8..11}->9.5: gap 8*0.05
        self.assertAlmostEqual(cm.ladder_drift(trend), 0.4, places=10)

    def test_top_share(self):
        # two entities, activity expm1(X): shares exactly computable
        rs = np.log1p(np.array([[3.0, 1.0], [3.0, 1.0]]))
        self.assertAlmostEqual(cm.top_share(rs, 1), 0.75)
        self.assertAlmostEqual(cm.top_share(rs, 2), 1.0)


if __name__ == "__main__":
    unittest.main()
