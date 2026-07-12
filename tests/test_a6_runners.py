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
    def setUp(self):
        self.tmp = Path("/tmp/a6_intake_test")
        self.tmp.mkdir(exist_ok=True)
        frozen_weeks = pd.date_range("2021-05-31", cep.FROZEN_LAST_WEEK, freq="7D")
        ext_weeks = pd.date_range(cep.EXT_FIRST_WEEK, cep.EXT_LAST_WEEK, freq="7D")
        self.frozen = _panel(frozen_weeks, ["a", "b", "c"], seed=1)
        self.good = pd.concat([self.frozen,
                               _panel(ext_weeks, ["a", "b", "c"], seed=2)],
                              ignore_index=True)
        self.fro_p = str(self.tmp / "frozen.parquet")
        self.frozen.to_parquet(self.fro_p, index=False)

    def _write(self, df):
        p = str(self.tmp / "ext.parquet")
        df.to_parquet(p, index=False)
        return p

    def test_good_extension_passes(self):
        cep.check(self._write(self.good), self.fro_p)   # no raise

    def test_boundary_leak_detected(self):
        # the A6.1 signature: the frozen partial-week row's VALUE changes
        bad = self.good.copy()
        m = bad["date"] == pd.Timestamp(cep.FROZEN_LAST_WEEK)
        bad.loc[m, "metric_value"] += 1.0    # July 1-4 folded in
        with self.assertRaises(SystemExit):
            cep.check(self._write(bad), self.fro_p)

    def test_partial_final_week_detected(self):
        bad = pd.concat([self.good,
                         _panel([pd.Timestamp("2022-12-26")], ["a"], seed=3)],
                        ignore_index=True)
        with self.assertRaises(SystemExit):
            cep.check(self._write(bad), self.fro_p)

    def test_duplicate_keys_detected(self):
        bad = pd.concat([self.good, self.good.tail(1)], ignore_index=True)
        with self.assertRaises(SystemExit):
            cep.check(self._write(bad), self.fro_p)


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


if __name__ == "__main__":
    unittest.main()
