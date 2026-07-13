"""simulate(track_ids=True): identity-safe tracked output (2z-ab, sixth
review — tranks follows fixed SLOTS whose identity changes silently at
rebirth; tids makes death observable). Defaults byte-identical."""
import sys
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
from exit_audit import idsafe_cohort_events  # noqa: E402


def _params(exit_rate=0.005):
    nk, N = 5, 300
    z = np.linspace(np.log(0.5 / N), np.log((N - 0.5) / N), nk)
    arr = lambda v: np.full(nk, v)  # noqa: E731
    w0 = np.sort(np.random.default_rng(0).normal(5.0, 1.0, N))[::-1]
    return mrd.RankParams(
        z_knots=z, phi=arr(0.2), sigma_trans=arr(0.05), sigma_perm=arr(0.02),
        sigma_obs=arr(0.05), lam=arr(1.0), exit_rate=arr(exit_rate),
        T_curve=w0[:nk], kappa=0.05, sigma_F=0.1, N=N, w0=w0,
        bottom_mu=w0[-50:], temper_s=0.3, kappa_z=None, t_df=float("inf"))


class TrackIdsTests(unittest.TestCase):
    def test_flag_off_byte_identical_and_gated(self):
        p = _params()
        a = mrd.simulate(p, 30, seed=4)
        b = mrd.simulate(p, 30, seed=4, track_ids=True)
        np.testing.assert_array_equal(a["tvals"], b["tvals"])   # no rng use
        np.testing.assert_array_equal(a["tranks"], b["tranks"])
        self.assertNotIn("tids", a)
        self.assertIn("tids", b)

    def test_slot_property_documented(self):
        # with a high exit rate, slots MUST change identity while their
        # ranks stay positive -- the representational fact the sixth
        # review caught (tranks<=0 can never identify death)
        p = _params(exit_rate=0.05)
        s = mrd.simulate(p, 60, seed=1, track_ids=True)
        self.assertTrue((s["tranks"] > 0).all())
        changes = (np.diff(s["tids"], axis=0) != 0).sum()
        self.assertGreater(changes, 0)

    def test_idsafe_scorer_counts_death_exactly_once(self):
        # constructed histories: slot 0 = established id, dies at t=6 and
        # the slot is inherited by a new id (which even re-enters top-K);
        # slot 1 = established id that CROSSES below K at t=5 and returns.
        T, K = 10, 100
        tids = np.zeros((T, 2), dtype=np.int64)
        tranks = np.zeros((T, 2), dtype=np.int32)
        tids[:, 0] = 7
        tids[6:, 0] = 99                       # rebirth: new identity
        tranks[:, 0] = 50
        tranks[6:, 0] = 40                     # new id ranks high again
        tids[:, 1] = 8
        tranks[:, 1] = 60
        tranks[5, 1] = 150                     # one-week crossing
        ev = idsafe_cohort_events(tranks, tids, t0=3, K=K, cut=10_000,
                                  ref_t=2)
        self.assertEqual(ev["deaths"], 1)      # id 7 dead exactly once
        self.assertEqual(ev["crossings"], 1)   # id 8: one in-K -> out event
        # risk-weeks: slot0 alive+inK t=3..5 (3), slot1 t=3..8 minus the
        # out week it isn't inK (t=5 out at 6? in at 5? rank150>K at t=5)
        self.assertGreater(ev["risk_weeks"], 0)


if __name__ == "__main__":
    unittest.main()
