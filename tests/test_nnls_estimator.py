"""NNLS option for the MD moment solves (2026-07-11 external-review finding 2).

The legacy convention is unconstrained OLS -> clip negatives -> score the
CLIPPED SSE (which also drives the (a, phi) grid choice).  That is not true
NNLS.  These tests lock: (a) the _solve_nonneg contract (NNLS never worse than
clipped OLS; identical when the unconstrained optimum is feasible), (b) exact
recovery of _md_partition under nnls=True, (c) the default path is unchanged
(legacy guard for the estimator layer)."""
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def _gamma_true(a, phi, W, V, s_e2, L):
    """Change autocovariances of OU(a) + AR1(phi) + iid noise."""
    A, B = W * (1 - a) ** 2, V * (1 - phi) ** 2
    g = [2 * W * (1 - a) + 2 * V * (1 - phi) + 2 * s_e2,
         -A - B - s_e2]
    g += [-A * a ** (k - 1) - B * phi ** (k - 1) for k in range(2, L + 1)]
    return np.array(g)


class TestSolveNonneg(unittest.TestCase):
    def test_identical_when_unconstrained_feasible(self):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(10, 3))
        c_true = np.array([1.0, 2.0, 0.5])
        y = X @ c_true
        c_leg, sse_leg = mrd._solve_nonneg(X, y, nnls=False)
        c_nn, sse_nn = mrd._solve_nonneg(X, y, nnls=True)
        np.testing.assert_allclose(c_leg, c_true, atol=1e-8)
        np.testing.assert_allclose(c_nn, c_true, atol=1e-8)
        self.assertAlmostEqual(sse_leg, 0.0, places=12)
        self.assertAlmostEqual(sse_nn, 0.0, places=12)

    def test_nnls_never_worse_than_clipped(self):
        rng = np.random.default_rng(1)
        strictly_better = 0
        for _ in range(50):
            X = rng.normal(size=(8, 3))
            y = rng.normal(size=8)
            _, sse_leg = mrd._solve_nonneg(X, y, nnls=False)
            c_nn, sse_nn = mrd._solve_nonneg(X, y, nnls=True)
            self.assertTrue(np.all(c_nn >= -1e-12))
            self.assertLessEqual(sse_nn, sse_leg + 1e-10)
            if sse_nn < sse_leg - 1e-8:
                strictly_better += 1
        # the clip must actually be suboptimal in a nontrivial share of draws,
        # otherwise this test proves nothing
        self.assertGreater(strictly_better, 5)

    def test_known_suboptimal_clip(self):
        # OLS solves exactly with a negative coef; clipping leaves a large
        # residual that a re-fit of the surviving coefficient removes.
        X = np.array([[1.0, 0.0], [1.0, 1.0]])
        y = np.array([1.0, -0.5])
        c_leg, sse_leg = mrd._solve_nonneg(X, y, nnls=False)
        c_nn, sse_nn = mrd._solve_nonneg(X, y, nnls=True)
        np.testing.assert_allclose(c_leg, [1.0, 0.0], atol=1e-10)
        self.assertLess(sse_nn, sse_leg - 0.5)


class TestMDPartitionNNLS(unittest.TestCase):
    def test_exact_recovery(self):
        # same construction as the committed recovery test, solved with NNLS
        a, phi, W, V, s_e2 = 0.96, 0.35, 0.30, 0.10, 0.04
        gk = _gamma_true(a, phi, W, V, s_e2, L=6)
        kap, s_eta, phi_h, s_nu, s_e = mrd._md_partition(gk, nnls=True)
        self.assertAlmostEqual(kap, 1 - a, places=6)
        self.assertAlmostEqual(phi_h, phi, places=6)
        self.assertAlmostEqual(s_e, np.sqrt(s_e2), places=6)
        self.assertAlmostEqual(s_eta, np.sqrt(W * (1 - a ** 2)), places=6)

    def test_default_unchanged_on_clean_moments(self):
        # on exactly-generated moments the constraint is slack: legacy == NNLS
        a, phi, W, V, s_e2 = 0.93, 0.20, 0.25, 0.15, 0.02
        gk = _gamma_true(a, phi, W, V, s_e2, L=6)
        out_leg = mrd._md_partition(gk, nnls=False)
        out_nn = mrd._md_partition(gk, nnls=True)
        np.testing.assert_allclose(out_leg, out_nn, atol=1e-8)


def _diverging_pert():
    """Perturbation (deterministic: default_rng(0), second N(0, 0.02) draw of
    length 7) that makes the clip bind at the optimal grid point: legacy and
    NNLS provably diverge on gk = _gamma_true(0.96,0.35,0.30,0.10,0.04) + pert
    (found by search 2026-07-11; reconstructed exactly, not hard-coded, so no
    rounding can silently defuse the discrimination assert below)."""
    rng = np.random.default_rng(0)
    rng.normal(0, 0.02, 7)          # trial 0 (non-diverging), discarded
    return rng.normal(0, 0.02, 7)   # trial 1


class TestNNLSDefaultLock(unittest.TestCase):
    """Regression lock for the 2026-07-11 Option-A re-freeze (2z-g/A4):
    the DEFAULT solve is exact NNLS.  Locks both the API defaults and the
    default behavior on a case where the two solvers provably differ, so a
    silent default reversion cannot pass the suite."""

    def test_api_defaults_are_nnls(self):
        import inspect
        import rankdiff_kalman as rk
        for fn, name in ((mrd._md_partition, "nnls"),
                         (mrd._md_partition2, "nnls"),
                         (mrd.estimate, "nnls"),
                         (mrd.run_platform, "nnls"),
                         (rk._estimate_fast, "nnls"),
                         (rk.oos_movement, "nnls")):
            default = inspect.signature(fn).parameters[name].default
            self.assertIs(default, True,
                          f"{fn.__module__}.{fn.__name__} default nnls != True")

    def test_default_behavior_is_nnls_where_solvers_differ(self):
        gk = _gamma_true(0.96, 0.35, 0.30, 0.10, 0.04, L=6) + _diverging_pert()
        out_default = np.array(mrd._md_partition(gk))
        out_nnls = np.array(mrd._md_partition(gk, nnls=True))
        out_legacy = np.array(mrd._md_partition(gk, nnls=False))
        # the case must actually discriminate, or this test proves nothing
        self.assertFalse(np.allclose(out_nnls, out_legacy, atol=1e-6))
        np.testing.assert_allclose(out_default, out_nnls, atol=1e-12)

    def test_cli_flags_mutually_exclusive(self):
        import subprocess
        for mod in ("minimal_rankdiff.py", "rankdiff_kalman.py"):
            r = subprocess.run(
                [sys.executable, f"llm_fitting/{mod}", "facebook",
                 "--nnls", "--legacy-clip"],
                capture_output=True, text=True,
                cwd=Path(__file__).resolve().parents[1])
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("not allowed with", r.stderr)


if __name__ == "__main__":
    unittest.main()
