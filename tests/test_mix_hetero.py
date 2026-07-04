"""--mix-hetero: per-entity permanent-share heterogeneity, sigma_perm,i =
sigma_perm(z) * v_i^(b/2) / norm, with b identified from the s(h) horizon
moment (nested: b=0 = movement-only temperament)."""
import sys
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def _hetero_panel(b, n_ent=3000, T=130, s=0.7, seed=11):
    """Panel where sigma_perm,i scales as v^(b/2) and the fast components as
    sqrt(v) -- the structure --mix-hetero models."""
    rng = np.random.default_rng(seed)
    v = rng.lognormal(-0.5 * s * s, s, n_ent)
    sp = 0.10 * v ** (b / 2.0)
    st_ = 0.10 * np.sqrt(v)
    se = 0.10 * np.sqrt(v)
    a, phi = 0.95, 0.2
    h = rng.normal(0, sp / np.sqrt(1 - a**2))
    xi = rng.normal(0, st_ / np.sqrt(1 - phi**2))
    base = rng.normal(8.0, 1.5, n_ent)
    X = np.empty((T, n_ent))
    for t in range(T):
        h = a * h + rng.normal(0, sp)
        xi = phi * xi + rng.normal(0, st_)
        X[t] = base + h + xi + rng.normal(0, se)
    df = pd.DataFrame({
        "entity_id": np.tile([f"e{i}" for i in range(n_ent)], T),
        "ts": np.repeat(pd.date_range("2019-01-07", periods=T, freq="W-MON"), n_ent),
        "metric": np.expm1(X).ravel().clip(0),
    })
    df["period"] = df["ts"].rank(method="dense").astype(int) - 1
    df["X"] = np.log1p(df["metric"])
    return mrd._rank_within(df)


class MixHeteroTests(unittest.TestCase):
    def test_b_recovery_full_vs_none(self):
        df1 = _hetero_panel(b=1.0)
        s1 = mrd.estimate_temperament(df1)["s"]
        b1 = mrd.estimate_mix_b(df1, s1)
        df0 = _hetero_panel(b=0.0)
        s0 = mrd.estimate_temperament(df0)["s"]
        b0 = mrd.estimate_mix_b(df0, s0)
        self.assertGreater(b1, 0.8)   # full structure B recovered
        self.assertLess(b0, 0.55)     # movement-only clearly separated

    def test_mix_b_fix_imposes_the_restriction(self):
        df = _hetero_panel(b=1.0, n_ent=800, T=60)
        p = mrd.estimate(df, temper=True, mix_hetero=True, mix_b_fix=1.0)
        self.assertEqual(p.mix_b, 1.0)
        # at b=1 the lognormal renormalization is exactly 1: w = v (factorized law)
        sqv = np.sqrt(np.random.default_rng(1).lognormal(-0.32, 0.8, 1000))
        np.testing.assert_allclose(mrd._sqw(sqv, 1.0, 0.8), sqv, rtol=1e-12)

    def test_sqw_normalization(self):
        rng = np.random.default_rng(0)
        s, b = 0.9, 0.8
        sqv = np.sqrt(rng.lognormal(-0.5 * s * s, s, 400_000))
        w = mrd._sqw(sqv, b, s) ** 2
        self.assertLess(abs(w.mean() - 1.0), 0.02)   # E[w] = 1 (Eulerian preserved)
        self.assertLess(np.median(w), 1.0)           # median entity below the mean

    def test_b_zero_is_exact_legacy(self):
        nk, N = 5, 300
        z = np.linspace(np.log(0.5 / N), np.log((N - 0.5) / N), nk)
        arr = lambda v: np.full(nk, v)  # noqa: E731
        w0 = np.sort(np.random.default_rng(0).normal(5.0, 1.0, N))[::-1]
        p0 = mrd.RankParams(
            z_knots=z, phi=arr(0.2), sigma_trans=arr(0.05), sigma_perm=arr(0.03),
            sigma_obs=arr(0.05), lam=arr(1.0), exit_rate=arr(0.005), T_curve=w0[:nk],
            kappa=0.05, sigma_F=0.1, N=N, w0=w0, bottom_mu=w0[-50:],
            temper_s=0.5, kappa_z=None, t_df=float("inf"))
        a = mrd.simulate(p0, 40, seed=9)["tvals"]
        b_ = mrd.simulate(replace(p0, mix_b=0.0), 40, seed=9)["tvals"]
        np.testing.assert_array_equal(a, b_)
        c = mrd.simulate(replace(p0, mix_b=1.0), 40, seed=9)["tvals"]
        self.assertFalse(np.array_equal(a, c))


if __name__ == "__main__":
    unittest.main()
