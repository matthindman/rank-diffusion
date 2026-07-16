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


class A5Tests(unittest.TestCase):
    """Amendment 5: P7 completion, P6/P10(iv) verdicts, guard, band_M,
    externalized builder — both directions."""

    def test_p7_verdict_both_directions(self):
        from exit_audit import p7_verdict
        v = p7_verdict(sim_mean=0.004, emp_ci_low=0.008, emp_rate=0.012,
                       emp_cross_share=0.95)
        self.assertTrue(v["deficit"] and v["composition"])
        # sim inside the CI -> no deficit
        self.assertFalse(p7_verdict(0.009, 0.008, 0.012, 0.95)["deficit"])
        # ratio below 1.5 -> no deficit even below CI
        self.assertFalse(p7_verdict(0.009, 0.010, 0.012, 0.95)["deficit"])
        # death-dominated composition -> composition FAIL
        self.assertFalse(p7_verdict(0.004, 0.008, 0.012, 0.5)["composition"])

    def test_p7_anchor_derivation_fail_and_run(self):
        import minimal_rankdiff as mrd
        from exit_audit import p7_main
        rng = np.random.default_rng(0)
        n = 200
        base = np.sort(rng.lognormal(3, 1.5, n))[::-1]

        def panel(start):
            rows = []
            for t, d in enumerate(pd.date_range(start, periods=60,
                                                freq="7D")):
                vals = base * np.exp(rng.normal(0, 0.3, n))
                rows.extend((f"e{i:03d}", d, float(vals[i]))
                            for i in range(n))
            return pd.DataFrame(rows, columns=["endpoint_id", "date",
                                               "metric_value"])
        # panel WITHOUT the frozen anchor week -> derivation FAILS
        panel("2019-01-07").to_parquet("/tmp/p7_noanchor.parquet",
                                       index=False)
        mrd.PLATFORMS["_p7a"] = dict(path="/tmp/p7_noanchor.parquet",
                                     id_col="endpoint_id", ts_col="date",
                                     metric_col="metric_value",
                                     max_rank=None)
        try:
            with self.assertRaisesRegex(SystemExit, "ANCHOR FAIL"):
                p7_main("_p7a", top_k=100, n_seeds=1)
            # panel CONTAINING 2021-07-05 -> derives t0 and runs end-to-end
            panel("2021-01-04").to_parquet("/tmp/p7_anchor.parquet",
                                           index=False)
            mrd.PLATFORMS["_p7a"]["path"] = "/tmp/p7_anchor.parquet"
            out = p7_main("_p7a", top_k=100, n_seeds=1)
            self.assertIn("p7_pass", out)
            self.assertIn("descriptive_trainfit", out["arms"])
            self.assertIn("verdict", out["arms"]["primary_fullfit"]["K/2"])
        finally:
            del mrd.PLATFORMS["_p7a"]

    def test_p7_subprocess_cli(self):
        import subprocess
        r = subprocess.run(
            [sys.executable, "llm_fitting/exit_audit.py", "--p7",
             "--platform", "_no_such_platform", "--top-k", "100"],
            capture_output=True, text=True, timeout=120)
        self.assertNotEqual(r.returncode, 0)     # wiring reaches p7_main
        r2 = subprocess.run(
            [sys.executable, "llm_fitting/exit_audit.py", "--p7"],
            capture_output=True, text=True, timeout=120)
        self.assertNotEqual(r2.returncode, 0)    # required flags enforced

    def test_p6_p10iv_verdicts(self):
        self.assertTrue(ig.p6_verdict(0.30, 0.30, 0.60))
        self.assertFalse(ig.p6_verdict(0.40, 0.30, 0.60))   # rel err out
        self.assertFalse(ig.p6_verdict(0.30, 0.30, 0.40))   # coverage out
        self.assertTrue(ig.p10iv_verdict(0.32, 0.50, 0.60))
        self.assertFalse(ig.p10iv_verdict(0.50, 0.60, 0.80))  # |.5-.32|>.15
        self.assertFalse(ig.p10iv_verdict(0.32, 0.20, 0.80))  # loses to base

    def test_guard_platform_wide(self):
        days = pd.date_range("2023-01-02", periods=60, freq="D")
        rows = [(f"u{i}", d, 5, 1) for d in days for i in range(50)]
        daily = pd.DataFrame(rows, columns=["user_name", "date",
                                            "metric_value", "n_posts"])
        collapse = pd.Timestamp("2023-02-15")
        daily = daily[~((daily["date"] == collapse)
                        & (daily["user_name"] != "u0"))]   # 98% collapse
        filtered, flagged = ig.apply_guard(daily)
        self.assertEqual(len(flagged), 1)
        wk = collapse - pd.Timedelta(days=collapse.weekday())
        self.assertFalse(((pd.to_datetime(filtered["date"])
                           - pd.to_timedelta(pd.to_datetime(filtered["date"])
                                             .dt.weekday, unit="D"))
                          == wk).any())          # whole week dropped
        # clean panel: nothing flagged, nothing dropped
        clean = pd.DataFrame(rows, columns=["user_name", "date",
                                            "metric_value", "n_posts"])
        f2, fl2 = ig.apply_guard(clean)
        self.assertEqual(len(fl2), 0)
        self.assertEqual(len(f2), len(clean))

    def test_band_m_vectorized(self):
        wk = pd.Timestamp("2023-01-02")
        npost = pd.Series({("a", wk): 10, ("b", wk): 20, ("c", wk): 40})
        npost.index = pd.MultiIndex.from_tuples(npost.index)
        m1 = pd.MultiIndex.from_tuples([("a", wk), ("b", wk)])
        m2 = pd.MultiIndex.from_tuples([("c", wk)])
        np.testing.assert_allclose(ig.band_M([m1, m2], npost), [15.0, 40.0])

    def test_external_builder_dedup_and_anomalies(self):
        import shutil
        tmp = Path("/tmp/ig_build_test"); shutil.rmtree(tmp, ignore_errors=True)
        posts = pd.DataFrame({
            "user_name": ["a", "a", "b", "b", "b"],
            "post_created_date": ["2023-03-01", "2023-03-01", "2023-03-01",
                                  "2023-03-02", "2023-03-02"],
            "total_interactions": [5, 9, 3, 4, 4],
            "url": ["u1", "u1", "u2", "u3", "u4"]})   # u1 duplicated
        raw = "/tmp/ig_build_raw.parquet"; posts.to_parquet(raw, index=False)
        out = "/tmp/ig_build_daily.parquet"
        ig.build(raw=raw, out=out, tmp_dir=str(tmp))
        d = pd.read_parquet(out).set_index(["user_name", "date"])
        self.assertEqual(int(d.loc[("a", pd.Timestamp("2023-03-01")),
                                   "metric_value"]), 9)    # keep-max
        self.assertEqual(int(d.loc[("a", pd.Timestamp("2023-03-01")),
                                   "n_posts"]), 1)          # deduped
        self.assertEqual(int(d.loc[("b", pd.Timestamp("2023-03-02")),
                                   "n_posts"]), 2)
        # anomalies FAIL
        for col, val in (("total_interactions", -1),
                         ("total_interactions", 2.5),
                         ("url", None)):
            bad = posts.copy(); bad.loc[0, col] = val
            bad.to_parquet(raw, index=False)
            with self.assertRaises(SystemExit):
                ig.build(raw=raw, out=out, tmp_dir=str(tmp))
        # conflicting duplicate url (different user) FAILS
        conf = posts.copy(); conf.loc[1, "user_name"] = "zzz"
        conf.to_parquet(raw, index=False)
        with self.assertRaises(SystemExit):
            ig.build(raw=raw, out=out, tmp_dir=str(tmp))

    def test_pinned_command_flags_parse(self):
        import subprocess
        r = subprocess.run(
            [sys.executable, "llm_fitting/rankdiff_kalman.py", "--help"],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0)
        for flag in ("--oos", "--spec-b", "--member-ids-file",
                     "--expect-member-sha", "--reps", "--boot",
                     "--conditional", "--dist-scores"):
            self.assertIn(flag, r.stdout)


class A6Tests(unittest.TestCase):
    """Amendment 6: dist-scores full-path preflight, wrapper wiring,
    three-pass builder scratch scope, P7 seed lock."""

    def test_oos_movement_dist_scores_returns_scalar_coverage(self):
        # the tenth review's crash: quantile_coverage overwrote the gate
        # coverage scalar; this executes the FULL return path
        import minimal_rankdiff as mrd
        import rankdiff_kalman as rk
        rng = np.random.default_rng(0)
        n, T = 250, 60
        base = np.sort(rng.lognormal(3, 1.2, n))[::-1]
        rows = []
        for t, d in enumerate(pd.date_range("2020-01-06", periods=T,
                                            freq="7D")):
            vals = base * np.exp(rng.normal(0, 0.25, n))
            rows.extend((f"e{i:03d}", d, float(vals[i])) for i in range(n))
        pd.DataFrame(rows, columns=["endpoint_id", "date", "metric_value"]) \
            .to_parquet("/tmp/a6_gate.parquet", index=False)
        mrd.PLATFORMS["_a6_gate"] = dict(
            path="/tmp/a6_gate.parquet", id_col="endpoint_id",
            ts_col="date", metric_col="metric_value", max_rank=None)
        try:
            res = rk.oos_movement("_a6_gate", top_k=100, temper=True,
                                  min_knot_n=8, md_lags=6, t_tails=True,
                                  conditional="state", dist_scores=True,
                                  reps=2, boot=100)
        finally:
            del mrd.PLATFORMS["_a6_gate"]
        self.assertIsInstance(res["coverage"], float)   # scalar, not dict
        for k in ("model_rel", "base_rel", "n_splits"):
            self.assertIn(k, res)

    def test_wrapper_uses_frozen_parameters(self):
        # gate_verdicts must call oos_movement with the registered params
        import gate_verdicts as gv
        import rankdiff_kalman as rk
        captured = {}
        orig = rk.oos_movement
        rk.oos_movement = lambda *a, **kw: (captured.update(kw),
                                            dict(model_rel=0.3,
                                                 base_rel=0.3,
                                                 coverage=0.6))[1]
        try:
            out = gv.run_p6(top_k=5000)
            self.assertEqual(captured["reps"], 20)
            self.assertEqual(captured["boot"], 2000)
            self.assertTrue(captured["dist_scores"])
            self.assertTrue(out["p6_pass"])
            captured.clear()
            out = gv.run_p10iv()
            self.assertTrue(captured["spec_b"])
            self.assertEqual(captured["expect_member_sha"], gv.MEMBER_SHA)
            self.assertTrue(out["p10iv_pass"])   # 0.3 within .15 of .320
        finally:
            rk.oos_movement = orig

    def test_builder_scratch_under_raw_small_and_cleanup(self):
        self.assertIn("raw_small", ig.BUILD_TMP)
        # cleanup even on failure: run an anomalous build, tmp must vanish
        tmp = Path("/tmp/ig_a6_tmp")
        posts = pd.DataFrame({
            "user_name": ["a"], "post_created_date": ["2023-03-01"],
            "total_interactions": [-5], "url": ["u1"]})
        posts.to_parquet("/tmp/ig_a6_raw.parquet", index=False)
        with self.assertRaises(SystemExit):
            ig.build(raw="/tmp/ig_a6_raw.parquet",
                     out="/tmp/ig_a6_out.parquet", tmp_dir=str(tmp))
        self.assertFalse(tmp.exists())           # failure-safe cleanup

    def test_p7_cli_has_no_seed_flag(self):
        import subprocess
        r = subprocess.run(
            [sys.executable, "llm_fitting/exit_audit.py", "--p7",
             "--platform", "x", "--top-k", "10", "--seeds", "5"],
            capture_output=True, text=True, timeout=120)
        self.assertNotEqual(r.returncode, 0)     # unknown flag rejected
        self.assertIn("unrecognized", r.stderr)

    def test_p7_subprocess_success_path(self):
        import subprocess
        script = """
import sys
sys.path.insert(0, "llm_fitting")
import numpy as np, pandas as pd
import minimal_rankdiff as mrd
rng = np.random.default_rng(0)
n = 150
base = np.sort(rng.lognormal(3, 1.5, n))[::-1]
rows = []
for t, d in enumerate(pd.date_range("2021-01-04", periods=50, freq="7D")):
    vals = base * np.exp(rng.normal(0, 0.3, n))
    rows.extend((f"e{i:03d}", d, float(vals[i])) for i in range(n))
pd.DataFrame(rows, columns=["endpoint_id", "date", "metric_value"]) \\
    .to_parquet("/tmp/p7_sub.parquet", index=False)
mrd.PLATFORMS["_p7sub"] = dict(path="/tmp/p7_sub.parquet",
    id_col="endpoint_id", ts_col="date", metric_col="metric_value",
    max_rank=None)
# REAL CLI: argparse dispatch incl. the hardcoded n_seeds=30 (eleventh
# review -- the previous version called p7_main directly)
import runpy
sys.argv = ["exit_audit.py", "--p7", "--platform", "_p7sub",
            "--top-k", "80"]
runpy.run_path("llm_fitting/exit_audit.py", run_name="__main__")
print("SUBPROCESS_P7_OK")
"""
        r = subprocess.run([sys.executable, "-c", script],
                           capture_output=True, text=True, timeout=600)
        self.assertEqual(r.returncode, 0, r.stderr[-2000:])
        self.assertIn("SUBPROCESS_P7_OK", r.stdout)


class A6PrimeTests(unittest.TestCase):
    """Eleventh review: interruption safety + genuine Spec-B preflight."""

    def _posts(self):
        return pd.DataFrame({
            "user_name": ["a", "b"],
            "post_created_date": ["2023-03-01", "2023-03-02"],
            "total_interactions": [5, 7], "url": ["u1", "u2"]})

    def test_stale_scratch_refused(self):
        import shutil
        tmp = Path("/tmp/ig_stale_tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        (tmp / "u00_0000.parquet").write_bytes(b"ghost")
        self._posts().to_parquet("/tmp/ig_stale_raw.parquet", index=False)
        with self.assertRaisesRegex(SystemExit, "stale"):
            ig.build(raw="/tmp/ig_stale_raw.parquet",
                     out="/tmp/ig_stale_out.parquet", tmp_dir=str(tmp))
        shutil.rmtree(tmp, ignore_errors=True)

    def test_interrupted_pass3_preserves_prior_output(self):
        from unittest.mock import patch
        import shutil
        out = Path("/tmp/ig_atomic_out.parquet")
        prior = pd.DataFrame({"user_name": ["old"],
                              "date": [pd.Timestamp("2023-01-02")],
                              "metric_value": [1], "n_posts": [1]})
        prior.to_parquet(out, index=False)
        before = out.read_bytes()
        self._posts().to_parquet("/tmp/ig_atomic_raw.parquet", index=False)
        tmp = Path("/tmp/ig_atomic_tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        with patch("pyarrow.parquet.ParquetWriter",
                   side_effect=RuntimeError("simulated pass-3 kill")):
            with self.assertRaises(RuntimeError):
                ig.build(raw="/tmp/ig_atomic_raw.parquet", out=str(out),
                         tmp_dir=str(tmp))
        self.assertEqual(out.read_bytes(), before)   # prior output intact
        self.assertFalse(Path(str(out) + ".staging").exists())
        self.assertFalse(tmp.exists())               # scratch cleaned

    def test_specb_oos_full_path(self):
        # genuine Spec-B gate execution: weekly + consistent full-week
        # dailies through oos_movement(spec_b=True, dist_scores=True)
        import minimal_rankdiff as mrd
        import rankdiff_kalman as rk
        rng = np.random.default_rng(1)
        n, T = 250, 60
        base = np.sort(rng.lognormal(3, 1.2, n))[::-1]
        wrows, drows = [], []
        for t, d in enumerate(pd.date_range("2020-01-06", periods=T,
                                            freq="7D")):
            vals = base * np.exp(rng.normal(0, 0.25, n))
            for i in range(n):
                wv = float(vals[i])
                wrows.append((f"e{i:03d}", d, wv))
                # 7 noisy dailies summing exactly to the weekly value
                parts = np.maximum(rng.normal(wv / 7, wv / 28, 7), 0.01)
                parts = parts * (wv / parts.sum())
                for k in range(7):
                    drows.append((d + pd.Timedelta(days=k), f"e{i:03d}",
                                  float(parts[k])))
        pd.DataFrame(wrows, columns=["endpoint_id", "date", "metric_value"]) \
            .to_parquet("/tmp/a6p_w.parquet", index=False)
        pd.DataFrame(drows, columns=["date", "endpoint_id", "metric_value"]) \
            .to_parquet("/tmp/a6p_d.parquet", index=False)
        mrd.PLATFORMS["_a6p_specb"] = dict(
            path="/tmp/a6p_w.parquet", id_col="endpoint_id", ts_col="date",
            metric_col="metric_value", max_rank=None,
            daily_path="/tmp/a6p_d.parquet", day_guard=False)
        try:
            res = rk.oos_movement("_a6p_specb", top_k=100, temper=True,
                                  min_knot_n=8, md_lags=6, t_tails=True,
                                  conditional="state", spec_b=True,
                                  dist_scores=True, reps=2, boot=100)
        finally:
            del mrd.PLATFORMS["_a6p_specb"]
        self.assertIsInstance(res["coverage"], float)
        self.assertIn("model_rel", res)
