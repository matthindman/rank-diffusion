"""Ladder explainer (llm_fitting/ladder_explainer.py): the restricted
minimum-distance partitions recover what they claim to fit, leave the
committed estimator untouched, and the index arithmetic matches DESIGN.md."""
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import ladder_explainer as le  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402


def gamma(kappa, s_eta, phi, s_nu, s_e, L=6):
    """Change autocovariances of OU home + AR(1) + noise; kappa = 0 is the
    random-walk limit (gamma_0 gets s_eta^2, later lags nothing)."""
    V = s_nu**2 / (1 - phi**2) if s_nu else 0.0
    B = V * (1 - phi) ** 2
    if kappa == 0:
        g = [s_eta**2 + 2 * V * (1 - phi) + 2 * s_e**2, -B - s_e**2]
        g += [-B * phi ** (k - 1) for k in range(2, L + 1)]
        return np.array(g)
    a = 1.0 - kappa
    W = s_eta**2 / (1 - a**2)
    A = W * (1 - a) ** 2
    g = [2 * W * (1 - a) + 2 * V * (1 - phi) + 2 * s_e**2, -A - B - s_e**2]
    g += [-A * a ** (k - 1) - B * phi ** (k - 1) for k in range(2, L + 1)]
    return np.array(g)


class RestrictedPartitionTests(unittest.TestCase):
    KW = dict(s_e_fix=None, d_mom=None, d_h=(), nnls=True, lev_mom=None, lev_T=None)

    def test_rw_books_all_variance_as_permanent(self):
        gk = gamma(0, 0.2, 0.0, 0.0, 0.1)
        kap, s_eta, phi, s_nu, s_e = le.part_rw(gk, **self.KW)
        self.assertEqual((kap, phi, s_nu, s_e), (0.0, 0.0, 0.0, 0.0))
        self.assertAlmostEqual(s_eta**2, gk[0], places=12)

    def test_rw_noise_recovers_local_level_model(self):
        gk = gamma(0, 0.12, 0.0, 0.0, 0.2)
        kap, s_eta, phi, s_nu, s_e = le.part_rw_noise(gk, **self.KW)
        self.assertEqual((kap, phi, s_nu), (0.0, 0.0, 0.0))
        self.assertAlmostEqual(s_eta, 0.12, places=3)
        self.assertAlmostEqual(s_e, 0.2, places=3)

    def test_ou_noise_recovers_ou_plus_white(self):
        gk = gamma(0.07, 0.10, 0.0, 0.0, 0.15)
        kap, s_eta, phi, s_nu, s_e = le.part_ou_noise(gk, **self.KW)
        self.assertAlmostEqual(kap, 0.07, places=6)
        self.assertEqual((phi, s_nu), (0.0, 0.0))
        self.assertAlmostEqual(s_eta, 0.10, places=3)
        self.assertAlmostEqual(s_e, 0.15, places=3)

    def test_context_restores_estimator_and_grids(self):
        a0, p0, f0 = mrd.A_GRID.copy(), mrd.PHI_GRID.copy(), mrd._md_partition
        for name in le.PARTITIONS:
            with le.partition(name):
                if name != "full":
                    mrd._md_partition(gamma(0.04, 0.1, 0.1, 0.2, 0.15), **self.KW)
            self.assertIs(mrd._md_partition, f0)
            np.testing.assert_array_equal(mrd.A_GRID, a0)
            np.testing.assert_array_equal(mrd.PHI_GRID, p0)
        # and the committed fit is unchanged afterwards
        out = mrd._md_partition(gamma(0.04, 0.10, 0.10, 0.20, 0.15))
        self.assertAlmostEqual(out[0], 0.04, places=6)
        self.assertAlmostEqual(out[2], 0.10, places=6)

    def test_context_restores_on_error(self):
        f0 = mrd._md_partition
        with self.assertRaises(RuntimeError):
            with le.partition("rw"):
                raise RuntimeError("boom")
        self.assertIs(mrd._md_partition, f0)


class DesignTests(unittest.TestCase):
    def test_top_rungs_are_the_frozen_stacks(self):
        s = le.spec_for("facebook_a", "struct", "S4")
        self.assertTrue(all(s[k] for k in ("temper", "t_tails", "md_vr_long",
                                           "stat_factor", "two_scale", "mix_hetero")))
        self.assertEqual(s["partition"], "full")
        g = le.spec_for("facebook_a", "gate", "G2")
        self.assertTrue(g["spec_b"] and g["conditional"] == "state")
        self.assertFalse(g["mix_hetero"] or g["md_vr_long"] or g["stat_factor"])
        g = le.spec_for("reddit_comments", "gate", "G2")
        self.assertTrue(g["mix_hetero"] and not g["spec_b"])
        g = le.spec_for("reddit", "gate", "G1")
        self.assertTrue(g["temper"] and g["t_tails"] and g["conditional"] == "state")
        s = le.spec_for("instagram", "struct", "S2")
        self.assertTrue(s["stat_factor"] and not s["md_vr_long"] and not s["two_scale"])

    def test_floor_has_no_optional_feature(self):
        s = le.spec_for("reddit", "gate", "R0")
        self.assertEqual(s["partition"], "rw")
        self.assertFalse(any(s[k] for k in ("temper", "t_tails", "mix_hetero", "spec_b")))
        self.assertIsNone(s["conditional"])
        self.assertEqual(le.param_count(s), dict(profiles=1, scalars=0))
        self.assertEqual(le.param_count(le.spec_for("facebook_a", "struct", "S4")),
                         dict(profiles=7, scalars=4))

    def test_macro_index_units(self):
        emp = dict(S1=0.02, S10=0.1, S100=0.4, VR2=0.8, VR4=0.6, VR8=0.4, VR13=0.3,
                   ACF1=-0.3, ACF2=-0.05, rR2_1=0.9, rR2_4=0.8, rR2_13=0.7,
                   RACF1=0.9, RACF4=0.8, RACF13=0.6, rPers1=30, rPers4=28,
                   rPers13=25, outfluxK=0.05)
        idx, blocks = le.macro_index(emp, dict(emp), top_k=35)
        self.assertEqual(idx, 0.0)
        sim = dict(emp, S1=0.024)                       # 20% high = 1 tolerance
        idx, blocks = le.macro_index(emp, sim, top_k=35)
        self.assertAlmostEqual(blocks["ladder"], 1 / 3, places=9)
        self.assertAlmostEqual(idx, 1 / 12, places=9)
        sim = dict(emp, RACF1=0.82, rPers1=35.0)        # 0.08 abs; 5 of tol 5
        _, blocks = le.macro_index(emp, sim, top_k=35)
        self.assertAlmostEqual(blocks["order"], 1 / 6, places=9)
        self.assertAlmostEqual(blocks["topset"], 1 / 4, places=9)
        self.assertEqual(le.stat_error("S1", "ratio", 0.02, 0.0, 35), le.ERR_CAP)
        self.assertEqual(le.stat_error("S1", "ratio", 0.02, 20.0, 35), le.ERR_CAP)

    def test_weights(self):
        present = list(le.PANELS)
        w = le.panel_weights("primary", present)
        self.assertAlmostEqual(sum(w.values()), 1.0)
        self.assertAlmostEqual(w["facebook_a"], 1 / 3)
        self.assertAlmostEqual(w["reddit_comments"], 1 / 3)
        self.assertAlmostEqual(w["reddit"], 1 / 6)
        self.assertAlmostEqual(w["reddit_submissions_long"], 1 / 6)
        self.assertNotIn("instagram", w)
        w = le.panel_weights("A_one_vote_per_platform", present)
        self.assertAlmostEqual(w["facebook_a"], 0.5)
        self.assertAlmostEqual(w["reddit_comments"], 0.25)
        self.assertAlmostEqual(w["reddit"], 0.125)
        w = le.panel_weights("B_one_vote_per_panel", present)
        self.assertEqual(len(w), 4)
        w = le.panel_weights("D_instagram_as_fourth_system", present)
        self.assertAlmostEqual(w["instagram"], 0.25)
        # a missing panel hands its share to the rest of its system
        w = le.panel_weights("primary", [p for p in present
                                         if p != "reddit_submissions_long"])
        self.assertAlmostEqual(w["reddit"], 1 / 3)

    def test_split_errors_match_the_gate_formula(self):
        em = dict(dRank1=10.0, dRank4=20.0, dRank13=30.0, RACF1=0.9, coll1=0.01,
                  coll5=0.5, coll20=0.8)
        sm = dict(dRank1=12.0, dRank4=20.0, dRank13=24.0, RACF1=0.9, coll1=0.5,
                  coll5=0.5, coll20=0.8)
        bm = dict(em, dRank1=11.0)
        me, be = le.split_errors(em, bm, sm, [1, 4, 13])
        # coll1 is below MOM_FLOOR and is left out: six moments remain
        self.assertAlmostEqual(me, (0.2 + 0.2) / 6, places=12)
        self.assertAlmostEqual(be, 0.1 / 6, places=12)


if __name__ == "__main__":
    unittest.main()
