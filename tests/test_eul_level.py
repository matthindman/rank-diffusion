"""--eul-level: Eulerian stationarity moment appended to the MD objective
(the A2 candidate fix, activated by the MODEL_STATUS §2z-q E5 trigger).
Zero new components; opt-in; defaults byte-identical (flag off never touches
the level machinery)."""
import inspect
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402

TRUE = dict(kappa=0.04, s_eta=0.10, phi=0.10, s_nu=0.20, s_e=0.15)
T_WIN = 120.0


def theoretical_gamma(kappa, s_eta, phi, s_nu, s_e, L=6):
    a = 1.0 - kappa
    W = s_eta**2 / (1 - a**2)
    V = s_nu**2 / (1 - phi**2)
    A, B = W * (1 - a) ** 2, V * (1 - phi) ** 2
    g = [2 * W * (1 - a) + 2 * V * (1 - phi) + 2 * s_e**2, -A - B - s_e**2]
    for k in range(2, L + 1):
        g.append(-A * a ** (k - 1) - B * phi ** (k - 1))
    return np.array(g)


def theoretical_level(kappa, s_eta, phi, s_nu, s_e, T=T_WIN):
    a = 1.0 - kappa
    W = s_eta**2 / (1 - a**2)
    V = s_nu**2 / (1 - phi**2)
    return (W * mrd._samplevar_shrink(a, T) + V * mrd._samplevar_shrink(phi, T)
            + s_e**2 * (1.0 - 1.0 / T))


def implied_level(kap, s_eta, phi, s_nu, s_e, T=T_WIN):
    return theoretical_level(kap, s_eta, phi, s_nu, s_e, T)


class EulLevelTests(unittest.TestCase):
    def test_shrink_factor_properties(self):
        # iid: sample var (ddof=0) shrinks by exactly (1 - 1/T)
        self.assertAlmostEqual(mrd._samplevar_shrink(0.0, 100), 1 - 1 / 100, places=12)
        # monotone decreasing in persistence; slow mixing -> strong shrink
        cs = [0.0, 0.5, 0.9, 0.99, 0.999]
        vals = [mrd._samplevar_shrink(c, 100) for c in cs]
        self.assertTrue(all(a > b for a, b in zip(vals, vals[1:])))
        self.assertLess(vals[-1], 0.15)

    def test_exact_recovery_with_level_moment(self):
        gk = theoretical_gamma(**TRUE)
        lev = theoretical_level(**TRUE)
        kap, s_eta, phi, s_nu, s_e = mrd._md_partition(
            gk, lev_mom=lev, lev_T=T_WIN)
        self.assertAlmostEqual(kap, TRUE["kappa"], places=6)
        self.assertAlmostEqual(phi, TRUE["phi"], places=6)
        self.assertAlmostEqual(s_eta, TRUE["s_eta"], places=3)
        self.assertAlmostEqual(s_nu, TRUE["s_nu"], places=3)
        self.assertAlmostEqual(s_e, TRUE["s_e"], places=3)
        # composes with the Spec-B pin
        kap_p, s_eta_p, phi_p, s_nu_p, _ = mrd._md_partition(
            gk, s_e_fix=TRUE["s_e"], lev_mom=lev, lev_T=T_WIN)
        self.assertAlmostEqual(kap_p, TRUE["kappa"], places=6)
        self.assertAlmostEqual(s_eta_p, TRUE["s_eta"], places=3)

    def test_level_row_is_live_and_directional(self):
        # same change moments, level target 3x the truth: the fitted
        # stationary level must move UP relative to the truth-consistent fit
        gk = theoretical_gamma(**TRUE)
        lev = theoretical_level(**TRUE)
        fit_true = mrd._md_partition(gk, lev_mom=lev, lev_T=T_WIN)
        fit_high = mrd._md_partition(gk, lev_mom=3.0 * lev, lev_T=T_WIN)
        self.assertGreater(implied_level(*fit_high),
                           implied_level(*fit_true) * 1.05)

    def test_two_scale_exact_recovery_with_level_moment(self):
        true = dict(kappa=0.02, s_eta=0.05, p1=0.20, s1=0.10, p2=0.90,
                    s2=0.06, s_e=0.12)
        a = 1 - true["kappa"]
        comps = [(a, true["s_eta"]), (true["p1"], true["s1"]),
                 (true["p2"], true["s2"])]
        Vs = [(c, sd**2 / (1 - c**2)) for c, sd in comps]
        gk = [2 * sum(V * (1 - c) for c, V in Vs) + 2 * true["s_e"]**2,
              -sum(V * (1 - c)**2 for c, V in Vs) - true["s_e"]**2]
        for k in range(2, 7):
            gk.append(-sum(V * (1 - c)**2 * c**(k - 1) for c, V in Vs))
        dm = np.array([2 * sum(V * (1 - c**h) for c, V in Vs)
                       + 2 * true["s_e"]**2 for h in mrd.VR_MOM_H])
        lev = (sum(V * mrd._samplevar_shrink(c, T_WIN) for c, V in Vs)
               + true["s_e"]**2 * (1 - 1 / T_WIN))
        kap, s_eta, p1, s1, p2, s2, s_e = mrd._md_partition2(
            np.array(gk), d_mom=dm, lev_mom=lev, lev_T=T_WIN)
        self.assertAlmostEqual(kap, true["kappa"], places=6)
        self.assertAlmostEqual(p2, true["p2"], places=6)
        self.assertAlmostEqual(s_e, true["s_e"], places=3)

    def test_estimate_flag_gating_and_default(self):
        self.assertIs(inspect.signature(mrd.estimate)
                      .parameters["eul_level"].default, False)
        self.assertIs(inspect.signature(mrd._md_partition)
                      .parameters["lev_mom"].default, None)
        import pandas as pd
        df = pd.DataFrame({"entity_id": ["a", "a"], "period": [0, 1],
                           "X": [1.0, 1.1], "rank": [1, 1], "z": [-3.0, -3.0]})
        with self.assertRaises(ValueError):
            mrd.estimate(df, eul_level=True)          # requires md_lags

    def test_estimate_e2e_smoke_and_flag_off_untouched(self):
        # synthetic OU+AR+noise panel through the FULL estimate() path
        rng = np.random.default_rng(3)
        n_ent, T = 400, 80
        a, s_eta, phi, s_nu, s_e = 0.96, 0.08, 0.2, 0.15, 0.10
        h = rng.normal(0, s_eta / np.sqrt(1 - a**2), n_ent)
        xi = rng.normal(0, s_nu / np.sqrt(1 - phi**2), n_ent)
        mu = np.sort(rng.normal(5, 1.5, n_ent))[::-1]
        rows = []
        for t in range(T):
            h = a * h + rng.normal(0, s_eta, n_ent)
            xi = phi * xi + rng.normal(0, s_nu, n_ent)
            X = mu + h + xi + rng.normal(0, s_e, n_ent)
            order = np.argsort(-X)
            rk = np.empty(n_ent, dtype=int)
            rk[order] = np.arange(1, n_ent + 1)
            for i in range(n_ent):
                rows.append((f"e{i:04d}", t, X[i], rk[i]))
        import pandas as pd
        df = pd.DataFrame(rows, columns=["entity_id", "period", "X", "rank"])
        df["z"] = np.log(np.clip((df["rank"] - 0.5) / n_ent, 1e-9, 1.0))
        p_off1 = mrd.estimate(df, md_lags=6)
        p_off2 = mrd.estimate(df, md_lags=6)
        np.testing.assert_array_equal(p_off1.kappa_z, p_off2.kappa_z)  # deterministic
        p_on = mrd.estimate(df, md_lags=6, eul_level=True)
        self.assertTrue(np.all(np.isfinite(p_on.kappa_z)))
        self.assertTrue(np.all(np.isfinite(p_on.sigma_perm)))
        # the moment is truth-consistent here, so estimates stay in range
        self.assertLess(abs(float(np.median(p_on.kappa_z)) - (1 - a)), 0.06)


if __name__ == "__main__":
    unittest.main()
