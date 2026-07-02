import math
import os
import unittest

os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

from scipy.integrate import quad

from llm_fitting.tail_estimation_analysis import (
    _log_alpha_poly,
    predict_deg1,
    predict_deg2,
    tail_mass_deg1,
    tail_mass_deg2,
    tail_mass_deg3,
    tail_mass_pl,
)


class TailEstimationAnalysisTests(unittest.TestCase):
    def test_log_alpha_poly_matches_documented_series(self):
        self.assertAlmostEqual(_log_alpha_poly(1.0, [1.0, 2.0, 3.0]), 2.5)
        self.assertAlmostEqual(
            _log_alpha_poly(2.0, [0.4, 0.6]),
            0.4 * 2.0 + 0.5 * 0.6 * (2.0 ** 2),
        )

    def test_predict_deg2_matches_direct_integral_left_of_boundary(self):
        y0 = 13997.0
        r0 = 5000.0
        alpha0 = 1.985932062282675
        eta0 = 0.7771256764824139
        eta1 = 0.37992841117134146
        r = 4000.0
        u = math.log(r / r0)

        def alpha_of(s):
            return alpha0 * math.exp(_log_alpha_poly(s, [eta0, eta1]))

        direct, _ = quad(alpha_of, 0.0, u, limit=200)
        expected = y0 * math.exp(-direct)
        observed = predict_deg2([r], y0, r0, alpha0, eta0, eta1)[0]

        self.assertAlmostEqual(observed, expected, delta=expected * 1e-9)

    def test_tail_mass_deg1_stays_near_power_law_for_tiny_curvature(self):
        alpha0 = 1.01
        eta0 = 1e-6
        power_law = tail_mass_pl(1.0, 1.0, alpha0)
        observed = tail_mass_deg1(1.0, 1.0, alpha0, eta0)

        self.assertAlmostEqual(observed / power_law, 0.9901906591114003, places=6)
        self.assertGreater(observed, power_law * 0.98)

    def test_tail_masses_remain_monotone_with_positive_coefficients(self):
        y0 = 13997.0
        r0 = 5000.0
        alpha0 = 1.985932062282675
        eta0 = 0.7771256764824139
        eta1 = 0.37992841117134146
        eta2 = 0.0

        pl = tail_mass_pl(y0, r0, alpha0)
        d1 = tail_mass_deg1(y0, r0, alpha0, eta0)
        d2 = tail_mass_deg2(y0, r0, alpha0, eta0, eta1)
        d3 = tail_mass_deg3(y0, r0, alpha0, eta0, eta1, eta2)

        self.assertGreaterEqual(pl, d1)
        self.assertGreaterEqual(d1, d2)
        self.assertGreaterEqual(d2, d3)

    def test_predict_deg1_is_continuous_at_small_eta(self):
        r = [4500.0]
        y0 = 10.0
        r0 = 5000.0
        alpha0 = 1.7
        eta0 = 1e-10

        observed = predict_deg1(r, y0, r0, alpha0, eta0)[0]
        expected = y0 * math.exp(-alpha0 * math.log(r[0] / r0))

        self.assertAlmostEqual(observed, expected, delta=expected * 1e-10)


if __name__ == "__main__":
    unittest.main()
