"""Minimum-distance covariance-structure estimator (_md_partition): recovery of
known OU-home + AR(1)-transitory + noise parameters from exact and simulated
change autocovariances."""
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def theoretical_gamma(kappa, s_eta, phi, s_nu, s_e, L=6):
    a = 1.0 - kappa
    W = s_eta**2 / (1 - a**2)
    V = s_nu**2 / (1 - phi**2)
    A, B = W * (1 - a) ** 2, V * (1 - phi) ** 2
    g = [2 * W * (1 - a) + 2 * V * (1 - phi) + 2 * s_e**2]
    g.append(-A - B - s_e**2)
    for k in range(2, L + 1):
        g.append(-A * a ** (k - 1) - B * phi ** (k - 1))
    return np.array(g)


class MDPartitionTests(unittest.TestCase):
    def test_exact_recovery_from_theoretical_moments(self):
        # true values chosen ON the search grids so recovery should be exact
        true = dict(kappa=0.04, s_eta=0.10, phi=0.10, s_nu=0.20, s_e=0.15)
        gk = theoretical_gamma(**true)
        kap, s_eta, phi, s_nu, s_e = mrd._md_partition(gk)
        self.assertAlmostEqual(kap, true["kappa"], places=6)
        self.assertAlmostEqual(phi, true["phi"], places=6)
        self.assertAlmostEqual(s_eta, true["s_eta"], places=3)
        self.assertAlmostEqual(s_nu, true["s_nu"], places=3)
        self.assertAlmostEqual(s_e, true["s_e"], places=3)

    def test_recovery_from_simulated_panel(self):
        rng = np.random.default_rng(0)
        n_ent, T = 3000, 120
        kappa, s_eta, phi, s_nu, s_e = 0.07, 0.08, 0.20, 0.18, 0.12
        a = 1 - kappa
        h = rng.normal(0, s_eta / np.sqrt(1 - a**2), n_ent)
        xi = rng.normal(0, s_nu / np.sqrt(1 - phi**2), n_ent)
        X = np.empty((T, n_ent))
        for t in range(T):
            h = a * h + rng.normal(0, s_eta, n_ent)
            xi = phi * xi + rng.normal(0, s_nu, n_ent)
            X[t] = h + xi + rng.normal(0, s_e, n_ent)
        dX = np.diff(X, axis=0)
        gk = np.array([np.mean(dX[:-k or None] * dX[k:] if k else dX * dX)
                       for k in range(7)])
        kap_hat, s_eta_hat, phi_hat, s_nu_hat, s_e_hat = mrd._md_partition(gk)
        self.assertLess(abs(kap_hat - kappa), 0.04)
        self.assertLess(abs(phi_hat - phi), 0.15)
        self.assertLess(abs(s_e_hat - s_e), 0.05)
        self.assertLess(abs(s_nu_hat - s_nu), 0.06)

    def test_rw_home_gives_near_zero_kappa(self):
        # pure random walk home: gamma tail ~ 0 -> fitted kappa at grid minimum
        gk = theoretical_gamma(kappa=0.005, s_eta=0.1, phi=0.1, s_nu=0.2, s_e=0.1)
        kap, *_ = mrd._md_partition(gk)
        self.assertLessEqual(kap, 0.011)


if __name__ == "__main__":
    unittest.main()
