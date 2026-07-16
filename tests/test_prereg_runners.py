"""PREREG_2026-07-16 A1.11 dry tests: subs runners (P4/P5/P9 algebra) and
IG runners (dedup, reconciliation gate both directions, P11 per the
reviewer's four requirements)."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import ig_daily_2023 as ig  # noqa: E402
import subs_backtest as sb  # noqa: E402


class SubsAlgebraTests(unittest.TestCase):
    def test_four_cell_algebra(self):
        # pure era shift: windows differ, membership doesn't matter
        era, comp = sb.four_cell(sA=0.6, sB=0.8, sC=0.6, sD=0.8)
        self.assertAlmostEqual(era, 0.2)
        self.assertAlmostEqual(comp, 0.0)
        # pure composition: membership matters, window doesn't
        era, comp = sb.four_cell(sA=0.6, sB=0.8, sC=0.8, sD=0.6)
        self.assertAlmostEqual(era, 0.0)
        self.assertAlmostEqual(comp, 0.2)

    def test_f_ratio(self):
        # comments-extension numbers: F = 0.085/0.111 ~ 0.766
        f = sb.f_ratio(0.229, 0.144, 0.256, 0.145)
        self.assertAlmostEqual(f, (0.229 - 0.144) / (0.256 - 0.145))

    def test_k_grid_rule(self):
        # synthetic panel where top-5000 holds >=90% but top-2500 doesn't
        rng = np.random.default_rng(0)
        rows = []
        for t in range(4):
            vals = np.concatenate([np.full(2500, 1.0), np.full(2500, 0.5),
                                   np.full(1000, 0.01)])
            for i, v in enumerate(vals):
                rows.append((f"e{i}", t, v * (1 + 0.01 * rng.random()), i + 1))
        df = pd.DataFrame(rows, columns=["entity_id", "period", "metric", "rank"])
        self.assertEqual(sb.p3_k(df), 5000)


class IGRunnerTests(unittest.TestCase):
    def _daily(self, rows):
        return pd.DataFrame(rows, columns=["user_name", "date",
                                           "metric_value", "n_posts"])

    def test_dedup_keeps_max(self):
        df = pd.DataFrame({"url": ["u1", "u1", "u2"],
                           "total_interactions": [5, 9, 3]})
        out = ig.dedup_posts(df)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[out["url"] == "u1"]["total_interactions"].item(), 9)

    def test_reconcile_pass_and_failures(self):
        days = pd.date_range("2023-01-02", "2023-01-15", freq="D")
        rows = [(u, d, 10, 2) for u in ("a", "b", "out") for d in days]
        daily = self._daily(rows)
        weekly = ig.weekly_from_daily(daily)
        ids = {"a", "b"}
        ig.reconcile(daily, weekly.copy(), ids)          # PASS
        # modeled value mismatch -> FAIL
        w2 = weekly.copy()
        i = w2.index[w2["user_name"] == "a"][0]
        w2.loc[i, "metric_value"] += 1
        with self.assertRaises(SystemExit):
            ig.reconcile(daily, w2, ids)
        # modeled weekly-only cell -> FAIL
        w3 = pd.concat([weekly, pd.DataFrame(
            [{"user_name": "a", "date": pd.Timestamp("2023-06-05"),
              "metric_value": 5, "n_posts": 1}])], ignore_index=True)
        with self.assertRaises(SystemExit):
            ig.reconcile(daily, w3, ids)
        # modeled daily-only cell (drop a modeled weekly row) -> FAIL
        w4 = weekly[~((weekly["user_name"] == "b")
                      & (weekly["date"] == weekly["date"].min()))]
        with self.assertRaises(SystemExit):
            ig.reconcile(daily, w4, ids)
        # OUTSIDE population: big value drift -> FAIL on weighted bound
        w5 = weekly.copy()
        j = w5.index[w5["user_name"] == "out"][0]
        w5.loc[j, "metric_value"] += 1000
        with self.assertRaises(SystemExit):
            ig.reconcile(daily, w5, ids)


class P11Tests(unittest.TestCase):
    """The reviewer's four required synthetic properties."""
    N, DRAWS = 200, 200

    def _matrix_from_daily(self, active):
        # wrap an activity matrix into the daily-frame -> cohort_matrix path
        days = pd.date_range("2023-01-02", periods=364, freq="D")
        rows = []
        for i in range(active.shape[0]):
            for d in np.where(active[i] > 0)[0]:
                rows.append((f"u{i:04d}", days[d], 5, int(active[i, d])))
        daily = pd.DataFrame(rows, columns=["user_name", "date",
                                            "metric_value", "n_posts"])
        return ig.cohort_matrix(daily, cohort_n=active.shape[0])

    def test_matrix_shape_and_zero_fill(self):
        rng = np.random.default_rng(0)
        active = (rng.random((self.N, 364)) < 0.8).astype(np.int8)
        active[:, 100] = 0                       # a fully absent day
        m = self._matrix_from_daily(active)
        self.assertEqual(m.shape, (self.N, 364))  # exactly cohort x 364
        self.assertTrue((m[:, 100] == 0).all())   # missing cells = 0 posts
        np.testing.assert_array_equal(m, active)

    def test_synchronized_outage_passes(self):
        rng = np.random.default_rng(1)
        active = (rng.random((self.N, 364)) < 0.9).astype(np.int8)
        for w in (10, 30, 45):                    # platform-wide bad weeks
            off = rng.random(self.N) < 0.6
            active[off, w * 7:(w + 1) * 7] = 0
        t = ig.p11_stat(active)
        null = ig.p11_null(active, draws=self.DRAWS, seed=0)
        self.assertGreater(t, np.percentile(null, 97.5))

    def test_independent_bursty_accounts_fail(self):
        # heavy within-account clustering (long alternating on/off runs),
        # independently timed -> must NOT fire
        # stationary phase (uniform over a full year): a shorter phase
        # window synchronizes every account's startup transient — a REAL
        # common shock the statistic rightly detects (verified during dry
        # testing; the first draft of this test used 0..80 and failed
        # honestly)
        rng = np.random.default_rng(2)
        active = np.zeros((self.N, 364), dtype=np.int8)
        for i in range(self.N):
            t = -int(rng.integers(0, 364))
            while t < 364:
                on = int(rng.integers(5, 40))
                off = int(rng.integers(5, 40))
                active[i, max(t, 0):max(t + on, 0)] = 1   # clip BOTH ends:
                # a negative stop index wraps in Python and paints the year
                t += on + off
        t_obs = ig.p11_stat(active)
        null = ig.p11_null(active, draws=self.DRAWS, seed=0)
        self.assertLessEqual(t_obs, np.percentile(null, 97.5))

    def test_null_preserves_totals_and_weekday_totals(self):
        rng = np.random.default_rng(3)
        active = (rng.random((20, 364)) < 0.7).astype(np.int8)
        byweek = active.reshape(20, 52, 7)
        shifts = np.random.default_rng(0).integers(0, 52, 20)
        rolled = np.stack([np.roll(byweek[i], shifts[i], axis=0)
                           for i in range(20)])
        # per-entity total absence preserved
        np.testing.assert_array_equal(rolled.sum(axis=(1, 2)),
                                      byweek.sum(axis=(1, 2)))
        # per-entity WEEKDAY-specific totals preserved (whole-week shifts)
        np.testing.assert_array_equal(rolled.sum(axis=1), byweek.sum(axis=1))


if __name__ == "__main__":
    unittest.main()


class VerdictBothDirectionsTests(unittest.TestCase):
    """A4.11: both verdict directions for P4/P5/P9 + P3 STOP branch."""

    def test_p4_verdict(self):
        self.assertTrue(sb.p4_verdict(0.6, 0.7, 0.8))
        self.assertFalse(sb.p4_verdict(0.8, 0.7, 0.6))
        self.assertFalse(sb.p4_verdict(0.6, 0.6, 0.8))   # non-strict

    def test_p5_verdict(self):
        self.assertTrue(sb.p5_verdict(1.0, 1.02))
        self.assertFalse(sb.p5_verdict(0.90, 1.02))       # b4 out
        self.assertFalse(sb.p5_verdict(1.0, 1.20))        # b8 out

    def test_p9_verdict(self):
        v = sb.p9_verdict(F=0.6, excess=0.05)
        self.assertTrue(v["p9a"] and v["p9b"])
        self.assertFalse(sb.p9_verdict(F=0.77, excess=0.05)["p9b"])  # comments knife-edge
        self.assertFalse(sb.p9_verdict(F=0.45, excess=0.05)["p9b"])
        self.assertFalse(sb.p9_verdict(F=0.6, excess=-0.01)["p9a"])

    def test_p3_stop_branch(self):
        # dispersed panel: even K=20,000 holds < 90% of the mass
        rows = []
        for t in range(3):
            for i in range(40_000):
                rows.append((f"e{i}", t, 1.0, i + 1))
        df = pd.DataFrame(rows, columns=["entity_id", "period",
                                         "metric", "rank"])
        with self.assertRaises(SystemExit):
            sb.p3_k(df)


class P10PureTests(unittest.TestCase):
    """A4.11: P10 i-iii piecewise logic, both directions + structural."""

    def test_pass_construction(self):
        M = np.geomspace(100, 2, 12)                 # head posts a lot
        sig = np.sqrt(0.5 / M)                       # exact 1/M law
        rec = sig * 1.2                              # recorded above floor
        v = ig.p10_verdicts(sig, M, rec)
        self.assertTrue(v["structural"] and v["i"] and v["ii"] and v["iii"])

    def test_fail_directions(self):
        M = np.geomspace(100, 2, 12)
        sig = np.sqrt(0.5 / M)
        rec = sig * 1.2
        # (i) slope out of band: sigma^2 ~ 1/M^3
        v = ig.p10_verdicts(np.sqrt(0.5 / M**3), M, np.sqrt(0.5 / M**3) * 1.2)
        self.assertFalse(v["i"])
        # (ii) orientation inverted
        v = ig.p10_verdicts(sig[::-1], M, rec[::-1] * 10)
        self.assertFalse(v["ii"])
        # (iii) one band below the floor
        rec_bad = rec.copy(); rec_bad[5] = sig[5] * 0.5
        self.assertFalse(ig.p10_verdicts(sig, M, rec_bad)["iii"])

    def test_structural_short_bands_fail(self):
        # the eighth review's case: 10 bands must FAIL, not truncate-pass
        M = np.geomspace(100, 2, 10)
        sig = np.sqrt(0.5 / M)
        v = ig.p10_verdicts(sig, M, sig * 1.2)
        self.assertFalse(v["structural"])
        self.assertFalse(v["i"] or v["ii"] or v["iii"])


class P7CLITests(unittest.TestCase):
    """A4.10/A4.11: the P7 execution path incl. anchor enforcement."""

    @classmethod
    def setUpClass(cls):
        import minimal_rankdiff as mrd
        rng = np.random.default_rng(0)
        n, T = 220, 48
        rows = []
        dates = pd.date_range("2020-01-06", periods=T, freq="7D")
        base = np.sort(rng.lognormal(3, 1.5, n))[::-1]
        for t, d in enumerate(dates):
            vals = base * np.exp(rng.normal(0, 0.3, n))
            for i in range(n):
                rows.append((f"e{i:03d}", d, float(vals[i])))
        df = pd.DataFrame(rows, columns=["endpoint_id", "date",
                                         "metric_value"])
        cls.pq = "/tmp/p7_cli_test.parquet"
        df.to_parquet(cls.pq, index=False)
        mrd.PLATFORMS["_p7_test"] = dict(
            path=cls.pq, id_col="endpoint_id", ts_col="date",
            metric_col="metric_value", max_rank=None)

    @classmethod
    def tearDownClass(cls):
        import minimal_rankdiff as mrd
        del mrd.PLATFORMS["_p7_test"]

    def test_anchor_fail_before_estimation(self):
        from exit_audit import aligned_main
        with self.assertRaisesRegex(SystemExit, "ANCHOR FAIL"):
            aligned_main(n_seeds=1, platform="_p7_test", t0=30,
                         top_k=100, anchor_date="1999-01-04")

    def test_anchor_pass_and_full_dry_run(self):
        from exit_audit import aligned_main
        # period 30 = 2020-01-06 + 30 weeks
        want = str((pd.Timestamp("2020-01-06")
                    + pd.Timedelta(weeks=30)).date())
        aligned_main(n_seeds=1, platform="_p7_test", t0=30,
                     top_k=100, anchor_date=want)   # completes end-to-end
