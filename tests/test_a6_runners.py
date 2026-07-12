"""A6 execution paths (CONFIRMATION_PROTOCOL §11): intake gate, E4 transport
runner, E5 readouts, E1 helpers.  All synthetic -- no data files, no
extension contact."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import check_extension_panel as cep  # noqa: E402
import community_metrics as cm  # noqa: E402
import e1_transport as e1  # noqa: E402
import e4_kappa_transport as e4  # noqa: E402


def _weekly(dates_ids_vals):
    return pd.DataFrame(dates_ids_vals,
                        columns=["endpoint_id", "date", "metric_value"])


def _panel(dates, ids, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    for d in dates:
        for i in ids:
            rows.append((i, d, float(rng.integers(1, 100))))
    return _weekly(rows)


def _fixture(tmp, n_ent=20, collapse_july=False):
    """Full synthetic intake fixture: frozen weekly+daily, extension daily
    (via values), extended weekly BUILT BY THE ASSEMBLER, coverage log.
    collapse_july: drop 90% of entities on July 1-7 (aggregation-consistent
    -- the round-7 day-guard false pass)."""
    import shutil
    import build_extension_weekly as bew
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    ids = [f"e{i:02d}" for i in range(n_ent)]
    frozen_weeks = pd.date_range("2021-05-31", cep.FROZEN_LAST_WEEK, freq="7D")
    frozen = _panel(frozen_weeks, ids, seed=1)
    fro_p = str(tmp / "frozen_weekly.parquet")
    frozen.to_parquet(fro_p, index=False)
    fro_days = pd.date_range("2021-05-21", "2021-06-30", freq="D")
    frozen_daily = _panel(fro_days, ids, seed=2)
    frod_p = str(tmp / "frozen_daily.parquet")
    frozen_daily.to_parquet(frod_p, index=False)
    days = pd.date_range(cep.EXT_FIRST_DAY, cep.EXT_LAST_DAY, freq="D")
    daily = _panel(days, ids, seed=3)
    if collapse_july:
        july = (daily["date"] >= "2021-07-01") & (daily["date"] <= "2021-07-07")
        keep = daily["endpoint_id"].isin(ids[:2])           # 10% of entities
        daily = daily[~july | keep]
    day_p = str(tmp / "ext_daily.parquet")
    daily.to_parquet(day_p, index=False)
    out_p = str(tmp / "ext_weekly.parquet")
    bew.assemble(fro_p, day_p, out_p)                       # weekly = sum(daily)
    log = pd.DataFrame({"month": cep.MONTHS.astype(str), "status": "ok",
                        "source_path": "x", "rows": 1000, "bytes": 5000})
    log_p = str(tmp / "coverage.csv")
    log.to_csv(log_p, index=False)
    months = cep.MONTHS.astype(str)
    plog = pd.DataFrame({
        "record_type": (["comments"] * len(months)) + ["submissions"],
        "month": list(months) + [months[0]],
        "status": "ok", "lines": 10_000, "errors": 0,
        "output_bytes": 99_999,
        "finished_at_utc": "2026-07-01T00:00:00Z"})
    plog_p = str(tmp / "processing_log.csv")
    plog.to_csv(plog_p, index=False)
    return dict(ext=out_p, fro=fro_p, day=day_p, frod=frod_p, log=log_p,
                plog=plog_p, tmp=tmp, daily=daily, frozen=frozen)


class TestIntakeGate(unittest.TestCase):
    """FAIL-CLOSED intake gate (A7 + A8): every reproduced false pass from
    rounds 6 and 7 must FAIL."""

    def setUp(self):
        self.f = _fixture(Path("/tmp/a8_intake_test"))

    def _rewrite(self, df, name):
        p = str(self.f["tmp"] / name)
        df.to_parquet(p, index=False)
        return p

    def test_good_extension_passes(self):
        cep.check(self.f["ext"], self.f["fro"], self.f["day"],
                  self.f["frod"], self.f["log"], self.f["plog"])

    def test_boundary_leak_detected(self):
        bad = pd.read_parquet(self.f["ext"])
        m = pd.to_datetime(bad["date"]) == pd.Timestamp(cep.FROZEN_LAST_WEEK)
        bad.loc[m, "metric_value"] += 1.0    # July 1-4 folded in
        with self.assertRaises(SystemExit):
            cep.check(self._rewrite(bad, "bad1.parquet"), self.f["fro"],
                      self.f["day"], self.f["frod"], self.f["log"], self.f["plog"])

    def test_missing_frozen_column_fails(self):
        bad = pd.read_parquet(self.f["ext"]).drop(columns=["metric_value"])
        bad["other"] = 1.0
        with self.assertRaises(SystemExit):
            cep.check(self._rewrite(bad, "bad2.parquet"), self.f["fro"],
                      self.f["day"], self.f["frod"], self.f["log"], self.f["plog"])

    def test_missing_extension_day_fails(self):
        d2 = self.f["daily"][self.f["daily"]["date"] != pd.Timestamp("2022-03-03")]
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"],
                      self._rewrite(d2, "day2.parquet"), self.f["frod"],
                      self.f["log"], self.f["plog"])

    def test_daily_only_cell_fails(self):
        # round-7 false pass #1: an extra daily entity/week must not be
        # silently dropped by reindexing
        extra = pd.DataFrame({"endpoint_id": ["ghost"] * 3,
                              "date": pd.date_range("2022-05-02", periods=3),
                              "metric_value": [5.0, 6.0, 7.0]})
        d2 = pd.concat([self.f["daily"], extra], ignore_index=True)
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"],
                      self._rewrite(d2, "day3.parquet"), self.f["frod"],
                      self.f["log"], self.f["plog"])

    def test_first_week_collapse_flagged(self):
        # round-7 false pass #2: July 1-7 must be judged against FROZEN
        # history (aggregation-consistent collapse: weekly rebuilt from the
        # collapsed dailies, so only the day guard can catch it)
        f2 = _fixture(Path("/tmp/a8_intake_collapse"), collapse_july=True)
        with self.assertRaises(SystemExit):
            cep.check(f2["ext"], f2["fro"], f2["day"], f2["frod"], f2["log"], f2["plog"])

    def test_coverage_log_defects_fail(self):
        # round-7 false pass #3 family: duplicate month, missing month,
        # empty month -- a directory glob saw none of these
        log = pd.read_csv(self.f["log"], dtype={"month": str})
        cases = [pd.concat([log, log.tail(1)], ignore_index=True),      # dup
                 log[log["month"] != "2022-05"],                        # missing
                 log.assign(rows=np.where(log["month"] == "2021-09",
                                          0, log["rows"]))]             # empty
        for j, bad in enumerate(cases):
            p = str(self.f["tmp"] / f"log{j}.csv")
            bad.to_csv(p, index=False)
            with self.assertRaises(SystemExit):
                cep.check(self.f["ext"], self.f["fro"], self.f["day"],
                          self.f["frod"], p, self.f["plog"])

    def test_parse_errors_fail(self):
        # round-8 reproduced false pass (A9): status="ok" with errors>0 --
        # the reviewer's exact errors=123 case must FAIL
        plog = pd.read_csv(self.f["plog"], dtype={"month": str})
        plog.loc[plog["month"] == "2022-02", "errors"] = 123
        p = str(self.f["tmp"] / "plog_err.csv")
        plog.to_csv(p, index=False)
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"], self.f["day"],
                      self.f["frod"], self.f["log"], p)

    def test_parse_errors_latest_record_governs(self):
        # re-runs append: an old errors>0 record superseded by a newer
        # clean run PASSES; the reverse FAILS (declared latest-record rule)
        plog = pd.read_csv(self.f["plog"], dtype={"month": str})
        old_bad = pd.DataFrame({"record_type": ["comments"],
                                "month": ["2022-02"], "status": ["ok"],
                                "lines": [10], "errors": [7],
                                "output_bytes": [1],
                                "finished_at_utc": ["2026-06-01T00:00:00Z"]})
        ok_after_retry = pd.concat([old_bad, plog], ignore_index=True)
        p1 = str(self.f["tmp"] / "plog_retry.csv")
        ok_after_retry.to_csv(p1, index=False)
        cep.check(self.f["ext"], self.f["fro"], self.f["day"],
                  self.f["frod"], self.f["log"], p1)          # PASSES
        new_bad = old_bad.assign(finished_at_utc="2026-08-01T00:00:00Z")
        bad_after_ok = pd.concat([plog, new_bad], ignore_index=True)
        p2 = str(self.f["tmp"] / "plog_newbad.csv")
        bad_after_ok.to_csv(p2, index=False)
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"], self.f["day"],
                      self.f["frod"], self.f["log"], p2)

    def test_processing_log_missing_month_fails(self):
        plog = pd.read_csv(self.f["plog"], dtype={"month": str})
        plog = plog[~((plog["month"] == "2022-07")
                      & (plog["record_type"] == "comments"))]
        p = str(self.f["tmp"] / "plog_missing.csv")
        plog.to_csv(p, index=False)
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"], self.f["day"],
                      self.f["frod"], self.f["log"], p)

    def test_daily_ending_dec25_fails(self):
        d2 = self.f["daily"][self.f["daily"]["date"] <= pd.Timestamp("2022-12-25")]
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"],
                      self._rewrite(d2, "day4.parquet"), self.f["frod"],
                      self.f["log"], self.f["plog"])

    def test_metric_missing_from_daily_fails(self):
        d2 = self.f["daily"].rename(columns={"metric_value": "renamed"})
        d2["renamed"] = d2["renamed"]
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"],
                      self._rewrite(d2, "day5.parquet"), self.f["frod"],
                      self.f["log"], self.f["plog"])

    def test_partial_final_week_detected(self):
        bad = pd.concat([pd.read_parquet(self.f["ext"]),
                         _panel([pd.Timestamp("2022-12-26")], ["e00"], seed=9)],
                        ignore_index=True)
        with self.assertRaises(SystemExit):
            cep.check(self._rewrite(bad, "bad3.parquet"), self.f["fro"],
                      self.f["day"], self.f["frod"], self.f["log"], self.f["plog"])

    def test_weekly_daily_value_mismatch_fails(self):
        d2 = self.f["daily"].copy()
        # perturb an IN-WINDOW day (boundary days like Dec 31 are correctly
        # outside the weekly aggregation and would not -- and should not --
        # trip this check)
        idx = d2.index[d2["date"] == pd.Timestamp("2022-05-03")][0]
        d2.loc[idx, "metric_value"] += 5.0
        with self.assertRaises(SystemExit):
            cep.check(self.f["ext"], self.f["fro"],
                      self._rewrite(d2, "day6.parquet"), self.f["frod"],
                      self.f["log"], self.f["plog"])


class TestAssembler(unittest.TestCase):
    def test_prefix_untouched_and_boundary_days_reported(self):
        f = _fixture(Path("/tmp/a8_assembler_test"))
        out = pd.read_parquet(f["ext"])
        frozen = f["frozen"]
        pre = out[pd.to_datetime(out["date"]) <= pd.Timestamp(cep.FROZEN_LAST_WEEK)]
        self.assertEqual(len(pre), len(frozen))               # prefix intact
        self.assertEqual(float(pre["metric_value"].sum()),
                         float(frozen["metric_value"].sum()))  # no fold-in
        b = pd.read_parquet(f["ext"].replace(".parquet", "_boundary_days.parquet"))
        bdates = set(pd.to_datetime(b["date"]).dt.date.astype(str))
        for d in ("2021-07-01", "2021-07-04", "2022-12-26", "2022-12-31"):
            self.assertIn(d, bdates)


def _ou_panel(a_i, T, seed):
    """Per-entity OU with entity-specific reversion a_i (kappa_i signal)."""
    rng = np.random.default_rng(seed)
    n = a_i.size
    x = rng.normal(0, 1, n)
    out = np.empty((T, n))
    for t in range(T):
        x = a_i * x + rng.normal(0, 0.3, n)
        out[t] = x
    return out


class TestE4Runner(unittest.TestCase):
    def test_persistent_kappa_transports(self):
        rng = np.random.default_rng(0)
        n = 900
        a_i = rng.uniform(0.55, 0.999, n)          # strong, persistent kappa_i
        X_tr = _ou_panel(a_i, 136, seed=1)
        X_ext = _ou_panel(a_i, 77, seed=2)          # same entities, new draws
        out = e4.e4_stats(X_tr, X_ext, boot=100)
        self.assertGreater(out["rho_hat"], 0.2)
        self.assertGreaterEqual(out["spearman"], 0.20)
        self.assertGreaterEqual(out["concentration"], 1.3)
        self.assertTrue(out["passes"])

    def test_shuffled_extension_fails(self):
        rng = np.random.default_rng(0)
        n = 900
        a_i = rng.uniform(0.55, 0.999, n)
        X_tr = _ou_panel(a_i, 136, seed=1)
        X_ext = _ou_panel(a_i[rng.permutation(n)], 77, seed=2)  # signal broken
        out = e4.e4_stats(X_tr, X_ext, boot=50)
        self.assertLess(abs(out["spearman"]), 0.20)
        self.assertFalse(out["passes"])


class TestE5Readouts(unittest.TestCase):
    def test_head_offset_recovers_shift_and_level_adjust_kills_it(self):
        rng = np.random.default_rng(4)
        base = np.sort(rng.normal(8, 1.5, 1000))[::-1]
        rs_e = np.tile(base, (10, 1))
        rs_s = rs_e + 0.3                              # pure level shift
        self.assertAlmostEqual(cm.head_offset(rs_e, rs_s), 0.3, places=10)
        self.assertAlmostEqual(cm.head_offset(rs_e, rs_s, level_adjust=True),
                               0.0, places=10)
        # a head-only distortion survives level adjustment
        rs_h = rs_e.copy()
        rs_h[:, :600] += 0.2
        adj = cm.head_offset(rs_e, rs_h, level_adjust=True)
        self.assertGreater(adj, 0.05)


class TestE1Helpers(unittest.TestCase):
    def test_kappa_orientation_rule(self):
        inc = np.linspace(0.01, 0.10, 54)              # recorded orientation
        self.assertTrue(e1.kappa_orientation_ok(e1.kappa_bands12(inc)))
        dec = inc[::-1]                                # head most mobile: fail
        self.assertFalse(e1.kappa_orientation_ok(e1.kappa_bands12(dec)))
        flat = np.full(54, 0.05)                       # head not strictly lowest
        self.assertFalse(e1.kappa_orientation_ok(e1.kappa_bands12(flat)))
        # the frozen reference's own shape (head << mid ~ deep, mid>deep
        # within noise) MUST pass -- the dry run that corrected A6.2
        ref_like = np.concatenate([np.full(18, 0.0050), np.full(18, 0.0198),
                                   np.full(18, 0.0191)])
        self.assertTrue(e1.kappa_orientation_ok(e1.kappa_bands12(ref_like)))

    def test_kappa_bands12_pooling(self):
        b = e1.kappa_bands12(np.arange(24, dtype=float))
        self.assertEqual(len(b), 12)
        self.assertAlmostEqual(b[0], 0.5)              # mean of (0, 1)
        self.assertAlmostEqual(b[-1], 22.5)

    def test_daily_path_fail_closed_and_propagated(self):
        # round-6 P0: E1 must use the SELECTED platform's daily panel
        import minimal_rankdiff as mrd
        with self.assertRaises(SystemExit):
            e1.daily_path_for("facebook")              # no daily_path entry
        mrd.PLATFORMS["_e1_test"] = dict(daily_path="DAILY_SENTINEL")
        try:
            self.assertEqual(e1.daily_path_for("_e1_test"), "DAILY_SENTINEL")
        finally:
            del mrd.PLATFORMS["_e1_test"]
        # and _quantities REQUIRES the path (no hardcoded fallback)
        import inspect
        sig = inspect.signature(e1._quantities)
        self.assertIs(sig.parameters["daily_path"].default,
                      inspect.Parameter.empty)


class TestE5Trigger(unittest.TestCase):
    def test_registered_algebra(self):
        import e5_headlaw as e5
        # overshoot 0.05 with seed SD 0.01 -> fired
        t = e5.e5_trigger(0.10, 0.15 + 0.01 * np.random.default_rng(0).normal(size=20))
        self.assertTrue(t["fired"])
        # overshoot within 2 SD -> not fired
        t2 = e5.e5_trigger(0.10, 0.11 + 0.02 * np.random.default_rng(1).normal(size=20))
        self.assertFalse(t2["fired"])
        # undershoot never fires (direction matters)
        t3 = e5.e5_trigger(0.10, np.full(20, 0.05))
        self.assertFalse(t3["fired"])

    def test_sd_convention_frozen_ddof0(self):
        # A8: POPULATION SD (ddof=0), the registered-baseline convention --
        # exact threshold on a fixed vector
        import e5_headlaw as e5
        sims = np.array([0.10, 0.12, 0.14, 0.16])   # mean .13, pop SD sqrt(.0005)
        t = e5.e5_trigger(0.05, sims)
        self.assertAlmostEqual(t["threshold"], 2 * np.std(sims, ddof=0), places=15)
        self.assertAlmostEqual(t["threshold"], 2 * 0.022360679774997897, places=12)
        self.assertNotAlmostEqual(t["threshold"], 2 * np.std(sims, ddof=1), places=6)

    def test_seeds_frozen_at_20(self):
        import e5_headlaw as e5
        self.assertEqual(list(e5.SEEDS), list(range(20)))


class TestWeekBlockCI(unittest.TestCase):
    def test_covers_truth_on_synthetic(self):
        import rankdiff_kalman as rk
        rng = np.random.default_rng(0)
        base = np.arange(1, 121, dtype=float)
        R = np.tile(base, (30, 1)) + rng.normal(0, 5, (30, 120))
        lo, hi = rk._boot_ci_weekblock(R, h=1, B=200)
        d = np.abs(R[1:] - R[:-1])[:, base <= 100]
        med = np.median(d)
        self.assertTrue(lo <= med <= hi)
        self.assertGreater(hi, lo)


if __name__ == "__main__":
    unittest.main()
