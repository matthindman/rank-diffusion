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


class TestIntakeGate(unittest.TestCase):
    """FAIL-CLOSED intake gate (A7): the round-6 reproduced false passes —
    missing frozen column, missing extension day, omitted daily panel —
    must all FAIL now."""

    def setUp(self):
        import shutil
        self.tmp = Path("/tmp/a6_intake_test")
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir()
        frozen_weeks = pd.date_range("2021-05-31", cep.FROZEN_LAST_WEEK, freq="7D")
        ext_weeks = pd.date_range(cep.EXT_FIRST_WEEK, cep.EXT_LAST_WEEK, freq="7D")
        self.frozen = _panel(frozen_weeks, ["a", "b", "c"], seed=1)
        self.good = pd.concat([self.frozen,
                               _panel(ext_weeks, ["a", "b", "c"], seed=2)],
                              ignore_index=True)
        self.fro_p = str(self.tmp / "frozen.parquet")
        self.frozen.to_parquet(self.fro_p, index=False)
        # daily panel consistent with the good weekly extension: put each
        # week's total on its Monday, plus zero-filler on every other
        # calendar day so coverage is complete
        ext = self.good[self.good["date"] > pd.Timestamp(cep.FROZEN_LAST_WEEK)]
        daily = ext.copy()
        fill_days = pd.date_range(cep.EXT_FIRST_DAY, cep.EXT_LAST_DAY, freq="D")
        filler = pd.DataFrame({"endpoint_id": "filler",
                               "date": fill_days,
                               "metric_value": 1.0})
        wk_fill = filler.copy()
        wk_fill["wk"] = (wk_fill["date"]
                         - pd.to_timedelta(wk_fill["date"].dt.dayofweek, unit="D"))
        wk_fill = wk_fill[(wk_fill["wk"] >= pd.Timestamp(cep.EXT_FIRST_WEEK))
                          & (wk_fill["wk"] <= pd.Timestamp(cep.EXT_LAST_WEEK))]
        wk_fill = (wk_fill.groupby(["endpoint_id", "wk"], as_index=False)
                   ["metric_value"].sum().rename(columns={"wk": "date"}))
        self.good = pd.concat([self.good, wk_fill], ignore_index=True)
        self.daily = pd.concat([daily, filler], ignore_index=True)
        self.day_p = str(self.tmp / "daily.parquet")
        self.daily.to_parquet(self.day_p, index=False)
        raw = self.tmp / "raw"
        raw.mkdir()
        for m in cep.MONTHS.astype(str):
            (raw / f"RC_{m}.zst").touch()
        self.raw = str(raw)

    def _write(self, df, name="ext.parquet"):
        p = str(self.tmp / name)
        df.to_parquet(p, index=False)
        return p

    def test_good_extension_passes(self):
        cep.check(self._write(self.good), self.fro_p, self.day_p, self.raw)

    def test_boundary_leak_detected(self):
        bad = self.good.copy()
        m = bad["date"] == pd.Timestamp(cep.FROZEN_LAST_WEEK)
        bad.loc[m, "metric_value"] += 1.0    # July 1-4 folded in
        with self.assertRaises(SystemExit):
            cep.check(self._write(bad), self.fro_p, self.day_p, self.raw)

    def test_missing_frozen_column_fails(self):
        # round-6 false pass #1: schema must be EQUAL, not intersected
        bad = self.good.drop(columns=["metric_value"]).assign(other=1.0)
        with self.assertRaises(SystemExit):
            cep.check(self._write(bad), self.fro_p, self.day_p, self.raw)

    def test_missing_extension_day_fails(self):
        # round-6 false pass #2: a whole missing day must fail coverage
        d2 = self.daily[self.daily["date"] != pd.Timestamp("2022-03-03")]
        p2 = str(self.tmp / "daily2.parquet")
        d2.to_parquet(p2, index=False)
        with self.assertRaises(SystemExit):
            cep.check(self._write(self.good), self.fro_p, p2, self.raw)

    def test_missing_raw_month_fails(self):
        (Path(self.raw) / "RC_2022-05.zst").unlink()
        with self.assertRaises(SystemExit):
            cep.check(self._write(self.good), self.fro_p, self.day_p, self.raw)

    def test_partial_final_week_detected(self):
        bad = pd.concat([self.good,
                         _panel([pd.Timestamp("2022-12-26")], ["a"], seed=3)],
                        ignore_index=True)
        with self.assertRaises(SystemExit):
            cep.check(self._write(bad), self.fro_p, self.day_p, self.raw)

    def test_duplicate_keys_detected(self):
        bad = pd.concat([self.good, self.good.tail(1)], ignore_index=True)
        with self.assertRaises(SystemExit):
            cep.check(self._write(bad), self.fro_p, self.day_p, self.raw)

    def test_weekly_daily_mismatch_fails(self):
        d2 = self.daily.copy()
        d2.loc[d2.index[-1], "metric_value"] += 5.0
        p2 = str(self.tmp / "daily3.parquet")
        d2.to_parquet(p2, index=False)
        with self.assertRaises(SystemExit):
            cep.check(self._write(self.good), self.fro_p, p2, self.raw)


class TestAssembler(unittest.TestCase):
    def test_roundtrip_passes_gate_and_prefix_is_untouched(self):
        import shutil
        import build_extension_weekly as bew
        tmp = Path("/tmp/a7_assembler_test")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir()
        frozen_weeks = pd.date_range("2021-05-31", cep.FROZEN_LAST_WEEK, freq="7D")
        frozen = _panel(frozen_weeks, ["a", "b"], seed=1)
        fro_p = str(tmp / "frozen.parquet")
        frozen.to_parquet(fro_p, index=False)
        # dailies: include July 1-4 boundary days that MUST NOT be folded
        days = pd.date_range("2021-07-01", cep.EXT_LAST_DAY, freq="D")
        daily = _panel(days, ["a", "b"], seed=2)
        day_p = str(tmp / "daily.parquet")
        daily.to_parquet(day_p, index=False)
        out_p = str(tmp / "ext.parquet")
        bew.assemble(fro_p, day_p, out_p)
        cep.check(out_p, fro_p, day_p, str(self._raw(tmp)))   # gate PASSes
        out = pd.read_parquet(out_p)
        pre = out[out["date"] <= pd.Timestamp(cep.FROZEN_LAST_WEEK)]
        self.assertEqual(len(pre), len(frozen))               # prefix intact
        self.assertEqual(
            float(pre["metric_value"].sum()),
            float(frozen["metric_value"].sum()))              # no fold-in
        # boundary days went to the side parquet
        b = pd.read_parquet(out_p.replace(".parquet", "_boundary_days.parquet"))
        self.assertEqual(set(pd.to_datetime(b["date"]).dt.date.astype(str)) &
                         {"2021-07-01", "2021-07-04"},
                         {"2021-07-01", "2021-07-04"})

    @staticmethod
    def _raw(tmp):
        raw = tmp / "raw"
        raw.mkdir(exist_ok=True)
        for m in cep.MONTHS.astype(str):
            (raw / f"RC_{m}.zst").touch()
        return raw


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
