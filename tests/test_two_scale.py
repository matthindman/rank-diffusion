"""--two-scale: second (medium-timescale) transitory component, identified from
the D(h) moments (nested: sigma2=0 recovers the current model)."""
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def theory_moments(kappa, s_eta, p1, s1, p2, s2, s_e, L=6):
    a = 1 - kappa
    comps = [(a, s_eta), (p1, s1), (p2, s2)]
    Vs = [(c, sd**2 / (1 - c**2)) for c, sd in comps]
    g = [2 * sum(V * (1 - c) for c, V in Vs) + 2 * s_e**2]
    g.append(-sum(V * (1 - c)**2 for c, V in Vs) - s_e**2)
    for k in range(2, L + 1):
        g.append(-sum(V * (1 - c)**2 * c**(k - 1) for c, V in Vs))
    d = [2 * sum(V * (1 - c**h) for c, V in Vs) + 2 * s_e**2 for h in mrd.VR_MOM_H]
    return np.array(g), np.array(d)


class TwoScaleTests(unittest.TestCase):
    TRUE = dict(kappa=0.02, s_eta=0.05, p1=0.20, s1=0.10, p2=0.90, s2=0.06, s_e=0.12)

    def test_exact_recovery(self):
        gk, dm = theory_moments(**self.TRUE)
        kap, s_eta, p1, s1, p2, s2, s_e = mrd._md_partition2(gk, d_mom=dm)
        self.assertAlmostEqual(kap, self.TRUE["kappa"], places=6)
        self.assertAlmostEqual(p2, self.TRUE["p2"], places=6)
        self.assertAlmostEqual(s2, self.TRUE["s2"], places=3)
        self.assertAlmostEqual(s_e, self.TRUE["s_e"], places=3)
        # composes with the spec-B pin
        kap_p, _, _, _, p2_p, s2_p, _ = mrd._md_partition2(
            gk, s_e_fix=self.TRUE["s_e"], d_mom=dm)
        self.assertAlmostEqual(kap_p, self.TRUE["kappa"], places=6)
        self.assertAlmostEqual(p2_p, self.TRUE["p2"], places=6)

    def test_recovery_from_simulated_panel(self):
        rng = np.random.default_rng(7)
        n_ent, T = 5000, 140
        tr = self.TRUE
        a = 1 - tr["kappa"]
        h = rng.normal(0, tr["s_eta"] / np.sqrt(1 - a**2), n_ent)
        x1 = rng.normal(0, tr["s1"] / np.sqrt(1 - tr["p1"]**2), n_ent)
        x2 = rng.normal(0, tr["s2"] / np.sqrt(1 - tr["p2"]**2), n_ent)
        X = np.empty((T, n_ent))
        for t in range(T):
            h = a * h + rng.normal(0, tr["s_eta"], n_ent)
            x1 = tr["p1"] * x1 + rng.normal(0, tr["s1"], n_ent)
            x2 = tr["p2"] * x2 + rng.normal(0, tr["s2"], n_ent)
            X[t] = h + x1 + x2 + rng.normal(0, tr["s_e"], n_ent)
        dX = np.diff(X, axis=0)
        gk = np.array([np.mean(dX[:-k or None] * dX[k:] if k else dX * dX)
                       for k in range(7)])
        dm = np.array([np.var(X[hh:] - X[:-hh]) for hh in mrd.VR_MOM_H])
        kap, _, _, _, p2, s2, s_e = mrd._md_partition2(gk, d_mom=dm)
        self.assertLess(abs(kap - tr["kappa"]), 0.03)
        self.assertLess(abs(p2 - tr["p2"]), 0.11)   # grid neighbor tolerance
        self.assertLess(abs(s_e - tr["s_e"]), 0.04)
        self.assertGreater(s2, 0.02)                 # medium component detected

    def test_long_horizon_moments_exact_recovery(self):
        """--md-vr-long: D(26)/D(52) rows separate a slow directional component
        (kappa=0.01) from medium wander -- exact recovery through the 2-scale
        partition with the long moment set."""
        true = dict(kappa=0.01, s_eta=0.04, p1=0.20, s1=0.10, p2=0.90, s2=0.06, s_e=0.12)
        a = 1 - true["kappa"]
        comps = [(a, true["s_eta"]), (true["p1"], true["s1"]), (true["p2"], true["s2"])]
        Vs = [(c, sd**2 / (1 - c**2)) for c, sd in comps]
        gk = [2 * sum(V * (1 - c) for c, V in Vs) + 2 * true["s_e"]**2,
              -sum(V * (1 - c)**2 for c, V in Vs) - true["s_e"]**2]
        for k in range(2, 7):
            gk.append(-sum(V * (1 - c)**2 * c**(k - 1) for c, V in Vs))
        hs = mrd.VR_MOM_H_LONG
        dm = np.array([2 * sum(V * (1 - c**h) for c, V in Vs) + 2 * true["s_e"]**2
                       for h in hs])
        kap, s_eta, p1, s1, p2, s2, s_e = mrd._md_partition2(
            np.array(gk), d_mom=dm, d_h=hs)
        self.assertAlmostEqual(kap, true["kappa"], places=6)
        self.assertAlmostEqual(p2, true["p2"], places=6)
        self.assertAlmostEqual(s_e, true["s_e"], places=3)

    def test_zero_sigma2_is_inert_in_simulate(self):
        """A zeros sigma_trans2 must not consume rng draws -- output identical
        to phi2/sigma_trans2 = None (nesting + legacy stream preservation)."""
        from dataclasses import replace
        nk, N = 5, 300
        z = np.linspace(np.log(0.5 / N), np.log((N - 0.5) / N), nk)
        arr = lambda v: np.full(nk, v)  # noqa: E731
        w0 = np.sort(np.random.default_rng(0).normal(5.0, 1.0, N))[::-1]
        p0 = mrd.RankParams(
            z_knots=z, phi=arr(0.2), sigma_trans=arr(0.05), sigma_perm=arr(0.02),
            sigma_obs=arr(0.05), lam=arr(1.0), exit_rate=arr(0.005), T_curve=w0[:nk],
            kappa=0.05, sigma_F=0.1, N=N, w0=w0, bottom_mu=w0[-50:],
            temper_s=0.3, kappa_z=None, t_df=float("inf"))
        p_zero = replace(p0, phi2=arr(0.9), sigma_trans2=arr(0.0))
        a = mrd.simulate(p0, 40, seed=5)["tvals"]
        b = mrd.simulate(p_zero, 40, seed=5)["tvals"]
        np.testing.assert_array_equal(a, b)
        # and an ACTIVE medium component changes the dynamics
        p_two = replace(p0, phi2=arr(0.9), sigma_trans2=arr(0.06))
        c = mrd.simulate(p_two, 40, seed=5)["tvals"]
        self.assertFalse(np.array_equal(a, c))


if __name__ == "__main__":
    unittest.main()
