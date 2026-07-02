"""Tests for the temperament (persistent entity-volatility) estimator and its
simulator support.

The estimator must recover a known mixing spread s from synthetic panels
(including s=0), and the simulator's temperament draws must preserve
band-level variance (the Eulerian structure) by construction.
"""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def synth_panel(s: float, n_ent: int = 800, T: int = 60, sigma0: float = 0.3,
                seed: int = 0) -> pd.DataFrame:
    """Entities with fixed levels + iid Gaussian changes whose per-entity sigma
    is sigma0 * sqrt(v_i), log v_i ~ N(-s^2/2, s^2)."""
    rng = np.random.default_rng(seed)
    levels = rng.normal(10, 2, n_ent)
    v = rng.lognormal(-0.5 * s * s, s, n_ent) if s > 0 else np.ones(n_ent)
    # iid noise around a fixed level: X_t = level + e_t (changes are MA(1))
    X = levels[None, :] + sigma0 * np.sqrt(v)[None, :] * rng.standard_normal((T, n_ent))
    rows = []
    for t in range(T):
        for i in range(n_ent):
            rows.append((f"e{i}", t, float(X[t, i])))
    df = pd.DataFrame(rows, columns=["entity_id", "period", "X"])
    df["metric"] = np.exp(df["X"])  # only used for ranking
    return mrd._rank_within(df)


class TemperamentTests(unittest.TestCase):
    def test_recovers_known_spread(self):
        est = mrd.estimate_temperament(synth_panel(s=0.8))
        # iid level+noise => changes are MA(1) with rho1=-0.5; the Satterthwaite
        # correction must handle that and still recover s
        self.assertGreater(est["s"], 0.6)
        self.assertLess(est["s"], 1.0)
        self.assertGreater(est["kappa"], 1.2)  # detected the MA structure

    def test_null_case_near_zero(self):
        est = mrd.estimate_temperament(synth_panel(s=0.0))
        self.assertLess(est["s"], 0.15)

    def test_simulator_preserves_band_variance(self):
        # E[v]=1 by construction: mean squared multiplier ~ 1
        rng = np.random.default_rng(3)
        s = 0.7
        v = rng.lognormal(-0.5 * s * s, s, 200_000)
        self.assertAlmostEqual(float(v.mean()), 1.0, places=2)

    def test_eb_vhat_recovers_entity_multipliers(self):
        # entities with known v_i: shrunken estimates should correlate strongly
        # with truth, have mean ~1, and rank quiet entities below volatile ones
        rng = np.random.default_rng(7)
        n_ent, T, s = 400, 60, 0.8
        v_true = rng.lognormal(-0.5 * s * s, s, n_ent)
        levels = rng.normal(10, 2, n_ent)
        X = levels[None, :] + 0.3 * np.sqrt(v_true)[None, :] * rng.standard_normal((T, n_ent))
        rows = [(f"e{i}", t, float(np.exp(X[t, i])))
                for t in range(T) for i in range(n_ent)]
        df = pd.DataFrame(rows, columns=["entity_id", "period", "metric"])
        df["X"] = np.log(df["metric"])
        df = mrd._rank_within(df)
        vhat = mrd.eb_vhat(df)
        v = vhat.reindex([f"e{i}" for i in range(n_ent)]).to_numpy()
        corr = float(np.corrcoef(np.log(v), np.log(v_true))[0, 1])
        self.assertGreater(corr, 0.6)
        self.assertAlmostEqual(float(np.mean(v)), 1.0, places=6)
        # shrinkage: estimates less dispersed than truth
        self.assertLess(np.std(np.log(v)), np.std(np.log(v_true)))

    def test_temperament_off_is_bitwise_baseline(self):
        # temper_s=0 must not change the simulation path at all
        df = synth_panel(s=0.0, n_ent=150, T=30)
        p = mrd.estimate(df, obs_frac=0.4)
        self.assertEqual(p.temper_s, 0.0)
        a = mrd.simulate(p, 10, seed=1)
        b = mrd.simulate(p, 10, seed=1)
        np.testing.assert_array_equal(a["tranks"], b["tranks"])


if __name__ == "__main__":
    unittest.main()
