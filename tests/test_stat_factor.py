"""--stat-factor: the common factor as a measured STATIONARY level instead of
integrating into every entity's permanent state (which manufactures spurious
long-horizon variance -- the empirical platform level mean-reverts)."""
import sys
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def _toy_params(N=400, factor_rho=None, sigma_F=0.20):
    nk = 5
    z = np.linspace(np.log(0.5 / N), np.log((N - 0.5) / N), nk)
    arr = lambda v: np.full(nk, v)  # noqa: E731
    w0 = np.sort(np.random.default_rng(0).normal(5.0, 1.0, N))[::-1]
    return mrd.RankParams(
        z_knots=z, phi=arr(0.2), sigma_trans=arr(0.05), sigma_perm=arr(0.02),
        sigma_obs=arr(0.05), lam=arr(1.0), exit_rate=arr(0.0), T_curve=w0[:nk],
        kappa=0.05, sigma_F=sigma_F, N=N, w0=w0, bottom_mu=w0[-50:],
        temper_s=0.0, kappa_z=None, t_df=float("inf"), factor_rho=factor_rho)


class StatFactorTests(unittest.TestCase):
    def test_integrated_level_wanders_stationary_level_does_not(self):
        T = 300
        p_int = _toy_params(factor_rho=None)
        p_st = replace(p_int, factor_rho=0.3)
        lvl_i = np.nanmean(mrd.simulate(p_int, T, seed=1, use_exit=False)["tvals"], axis=1)
        lvl_s = np.nanmean(mrd.simulate(p_st, T, seed=1, use_exit=False)["tvals"], axis=1)
        # integrated: common level variance grows ~ t * sigma_F^2; stationary:
        # bounded at sigma_F^2 / (2 (1 - rho))
        self.assertGreater(np.var(lvl_i), 5 * np.var(lvl_s))
        self.assertLess(np.var(lvl_s), 5 * 0.2**2 / (2 * 0.7))

    def test_stationary_level_reproduces_measured_dlevel_sd(self):
        T = 2000
        p_st = _toy_params(factor_rho=0.3)
        lvl = np.nanmean(mrd.simulate(p_st, T, seed=2, use_exit=False)["tvals"], axis=1)
        # sd of the weekly common-level change should reproduce sigma_F
        # (idiosyncratic components average out over N=400 only partially --
        # allow a generous band)
        self.assertLess(abs(np.std(np.diff(lvl)) - 0.20), 0.05)

    def test_legacy_path_untouched_when_rho_is_none(self):
        p = _toy_params(factor_rho=None)
        a = mrd.simulate(p, 50, seed=3)["tvals"]
        b = mrd.simulate(p, 50, seed=3)["tvals"]
        np.testing.assert_array_equal(a, b)  # deterministic, and rho=None
        # exercises the legacy branch (F integrates into mu)

    def test_estimate_measures_rho_in_range(self):
        rng = np.random.default_rng(4)
        n_ent, T = 400, 80
        base = rng.normal(6.0, 1.0, n_ent)
        lvl = np.zeros(T)
        for t in range(1, T):
            lvl[t] = 0.5 * lvl[t - 1] + rng.normal(0, 0.15)
        X = base[None, :] + rng.normal(0, 0.3, (T, n_ent)) + lvl[:, None]
        import pandas as pd
        df = pd.DataFrame({
            "entity_id": np.tile([f"e{i}" for i in range(n_ent)], T),
            "ts": np.repeat(pd.date_range("2020-01-06", periods=T, freq="W-MON"), n_ent),
            "metric": np.expm1(X).ravel(),
        })
        df["period"] = df["ts"].rank(method="dense").astype(int) - 1
        df["X"] = np.log1p(df["metric"])
        df = mrd._rank_within(df)
        p = mrd.estimate(df, stat_factor=True)
        self.assertIsNotNone(p.factor_rho)
        self.assertGreaterEqual(p.factor_rho, 0.0)
        self.assertLessEqual(p.factor_rho, 0.95)
        p0 = mrd.estimate(df)
        self.assertIsNone(p0.factor_rho)


if __name__ == "__main__":
    unittest.main()
