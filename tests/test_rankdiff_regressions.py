import json
import tempfile
import unittest
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

from archive.src.rankdiff.diagnostics import compute_sim_diagnostics
from archive.src.rankdiff.diagnostics import score_diagnostics
from archive.src.rankdiff.initializers import _fit_centered_t
from archive.src.rankdiff.io import load_fit_result
from archive.src.rankdiff.preprocess import build_data_bundle
from archive.src.rankdiff.simulator import _split_entry_counts, simulate_one
from archive.src.rankdiff.types import Config, EstimatedParams, ThresholdModel


class RankdiffRegressionTests(unittest.TestCase):
    def test_load_fit_result_restores_integer_empirical_keys(self):
        payload = {
            "config": {"data_path": "input.csv", "n_jobs": 1},
            "data_summary": {
                "platform": "demo",
                "cadence": "daily",
                "n_periods": 3,
                "n_entities": 2,
                "mean_n": 2.0,
                "max_n": 2,
                "threshold_by_period": [1.0, 1.0, 1.0],
                "max_missing_value_by_period": [1.0, 1.0, 1.0],
                "empirical": {
                    "top_k": 10,
                    "vr_emp": {"2": 1.2},
                    "acf_emp": {"1": 0.1},
                    "racf_emp": {"1": 0.2},
                    "pers_emp": {"1": 5},
                    "xr2_emp": {"13": 0.8},
                },
            },
            "initial": {
                "sigma_obs": 0.1,
                "sigma_het": 0.2,
                "alpha_arch": 0.1,
                "t_df_global": 6.0,
                "jump_prob": 0.01,
                "jump_scale": 5.0,
                "alpha_kappa": 0.5,
                "kappa_base_raw": 0.01,
                "z_knots": [0.0],
                "sigma_eta_anchor": [0.1],
                "phi_anchor": [0.5],
                "sigma_nu_anchor": [0.1],
                "t_df_anchor": [6.0],
                "threshold": {
                    "threshold_by_period": [1.0, 1.0, 1.0],
                    "max_missing_value_by_period": [1.0, 1.0, 1.0],
                },
                "top_k": 10,
                "metadata": {},
            },
            "params": {
                "sigma_obs": 0.1,
                "sigma_het": 0.2,
                "alpha_arch": 0.1,
                "t_df_global": 6.0,
                "jump_prob": 0.01,
                "jump_scale": 5.0,
                "alpha_kappa": 0.5,
                "kappa_base_raw": 0.01,
                "kappa_stab_factor": 1.0,
                "z_knots": [0.0],
                "sigma_eta_curve": [0.1],
                "phi_curve": [0.5],
                "sigma_nu_curve": [0.1],
                "kappa_curve": [0.01],
                "t_df_curve": [6.0],
                "threshold": {
                    "threshold_by_period": [1.0, 1.0, 1.0],
                    "max_missing_value_by_period": [1.0, 1.0, 1.0],
                },
                "top_k": 10,
                "n_full": 2,
                "w0_sorted": [1.0, 0.5],
                "burnin_periods": 10,
                "metadata": {},
                "exit_p_base": 0.0,
                "exit_alpha": 0.3,
                "exit_transient_rate": 0.07,
                "entry_burst_frac": 0.008,
                "t_df_curve_precal": [6.0],
            },
            "diagnostics": {},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "fit_result.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            loaded = load_fit_result(path)

        self.assertIn(13, loaded.data.empirical["xr2_emp"])
        self.assertEqual(loaded.data.empirical["xr2_emp"][13], 0.8)
        self.assertTrue(all(isinstance(k, int) for k in loaded.data.empirical["vr_emp"]))

    def test_fit_centered_t_uses_zero_location_mle(self):
        x = np.array([-0.4, -0.2, 0.1, 0.4, 8.0], dtype=float)

        df_fit, scale_fit = _fit_centered_t(x, (3.0, 200.0))
        df_expected, _, scale_expected = sp_stats.t.fit(x, floc=0.0)
        _, loc_free, _ = sp_stats.t.fit(x)

        self.assertAlmostEqual(df_fit, df_expected)
        self.assertAlmostEqual(scale_fit, scale_expected)
        self.assertNotAlmostEqual(loc_free, 0.0, places=2)

    def test_split_entry_counts_respects_zero_and_full_burst_rates(self):
        rng = np.random.default_rng(123)

        self.assertEqual(_split_entry_counts(rng, 7, 0.0), (0, 7))
        self.assertEqual(_split_entry_counts(rng, 7, 1.0), (7, 0))

    def test_simulate_one_zeroes_ranks_for_replaced_tracked_entities(self):
        cfg = Config(
            data_path="input.csv",
            random_seed=123,
            track_entity_count=3,
            simulate_periods=3,
            use_obs_noise=False,
            exit_enabled=True,
            exit_alpha=0.0,
        )
        threshold = ThresholdModel(
            threshold_by_period=np.zeros(3, dtype=float),
            max_missing_value_by_period=np.zeros(3, dtype=float),
            effectively_exact_above_threshold=True,
        )
        params = EstimatedParams(
            sigma_obs=0.0,
            sigma_het=0.0,
            alpha_arch=0.0,
            t_df_global=10.0,
            jump_prob=0.0,
            jump_scale=1.0,
            alpha_kappa=0.0,
            kappa_base_raw=0.0,
            kappa_stab_factor=1.0,
            z_knots=np.array([-10.0, 0.0], dtype=float),
            sigma_eta_curve=np.zeros(2, dtype=float),
            phi_curve=np.zeros(2, dtype=float),
            sigma_nu_curve=np.zeros(2, dtype=float),
            kappa_curve=np.zeros(2, dtype=float),
            t_df_curve=np.full(2, 10.0, dtype=float),
            threshold=threshold,
            top_k=1,
            n_full=3,
            w0_sorted=np.array([3.0, 2.0, 1.0], dtype=float),
            burnin_periods=0,
            metadata={},
            exit_p_base=1.0,
            exit_alpha=0.0,
            exit_transient_rate=1.0,
            entry_burst_frac=0.0,
            t_df_curve_precal=None,
        )

        sim = simulate_one(123, params, 3, cfg)

        self.assertTrue(np.all(np.isnan(sim["tracked_values"][1])))
        self.assertTrue(np.all(sim["tracked_ranks"][1] == 0))

    def test_simulate_one_handles_full_burst_when_all_entities_exit(self):
        cfg = Config(
            data_path="input.csv",
            random_seed=123,
            track_entity_count=3,
            simulate_periods=3,
            use_obs_noise=False,
            exit_enabled=True,
            exit_alpha=0.0,
        )
        threshold = ThresholdModel(
            threshold_by_period=np.zeros(3, dtype=float),
            max_missing_value_by_period=np.zeros(3, dtype=float),
            effectively_exact_above_threshold=True,
        )
        params = EstimatedParams(
            sigma_obs=0.0,
            sigma_het=0.0,
            alpha_arch=0.0,
            t_df_global=10.0,
            jump_prob=0.0,
            jump_scale=1.0,
            alpha_kappa=0.0,
            kappa_base_raw=0.0,
            kappa_stab_factor=1.0,
            z_knots=np.array([-10.0, 0.0], dtype=float),
            sigma_eta_curve=np.zeros(2, dtype=float),
            phi_curve=np.zeros(2, dtype=float),
            sigma_nu_curve=np.zeros(2, dtype=float),
            kappa_curve=np.zeros(2, dtype=float),
            t_df_curve=np.full(2, 10.0, dtype=float),
            threshold=threshold,
            top_k=1,
            n_full=3,
            w0_sorted=np.array([3.0, 2.0, 1.0], dtype=float),
            burnin_periods=0,
            metadata={},
            exit_p_base=1.0,
            exit_alpha=0.0,
            exit_transient_rate=1.0,
            entry_burst_frac=1.0,
            t_df_curve_precal=None,
        )

        sim = simulate_one(123, params, 3, cfg)

        self.assertEqual(sim["tracked_values"].shape, (3, 3))
        self.assertTrue(np.isfinite(sim["observed_counts"]).all())

    def test_compute_sim_diagnostics_handles_short_sparse_series_without_warnings(self):
        cfg = Config(data_path="input.csv")
        sim = {
            "tracked_values": np.array(
                [
                    [1.0, 2.0],
                    [np.nan, 3.0],
                    [np.nan, 4.0],
                ],
                dtype=float,
            ),
            "tracked_ranks": np.array(
                [
                    [1, 2],
                    [0, 2],
                    [0, 2],
                ],
                dtype=int,
            ),
            "top_ids": np.array([[0], [1], [1]], dtype=int),
            "period0_sorted_values": np.array([1.0, 0.5], dtype=float),
            "xsec_var": np.array([1.0, 1.0, 1.0], dtype=float),
            "observed_counts": np.array([2, 1, 1], dtype=int),
        }

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("error", RuntimeWarning)
            diag = compute_sim_diagnostics(sim, cfg)

        self.assertEqual(caught, [])
        self.assertIn("mean_observed_n", diag)

    def test_score_diagnostics_handles_zero_empirical_vr(self):
        cfg = Config(
            data_path="input.csv",
            vr_lags=(2,),
            acf_lags=(),
            racf_lags=(),
            pers_horizons=(),
            r2_horizons=(),
        )
        emp = {
            "top_k": 10,
            "vr_emp": {2: 0.0},
            "acf_emp": {},
            "racf_emp": {},
            "pers_emp": {},
            "xr2_emp": {},
        }

        score = score_diagnostics(emp, [{"vr2": 1.0}], cfg)

        self.assertFalse(score["tests"]["VR(2)"])

    def test_build_data_bundle_respects_max_dense_entities_cap(self):
        rows = []
        for day in pd.date_range("2024-01-01", periods=3, freq="D"):
            for idx in range(5):
                rows.append(
                    {
                        "endpoint_id": f"e{idx}",
                        "date": day,
                        "metric_value": float(100 - idx),
                    }
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "panel.parquet"
            pd.DataFrame(rows).to_parquet(path)
            bundle = build_data_bundle(
                Config(
                    data_path=path,
                    track_entity_count=5,
                    max_dense_entities=2,
                    min_presence_frac=1.0,
                )
            )

        self.assertEqual(bundle.tracked_entity_ids.size, 2)

    def test_empirical_mean_rank_ids_align_with_sorted_mean_rank(self):
        # Regression: mean_rank is sorted ascending while rank_wide.columns /
        # tracked_balanced_ids stay in unsorted tracked order.  Anchor/band
        # selection must go through mean_rank_ids so each rank band gets the
        # entities that actually belong to it (not a positionally-scrambled set).
        rng = np.random.default_rng(0)
        rows = []
        n_entities = 40
        bases = np.linspace(1000.0, 100.0, n_entities)  # distinct, stable rank levels
        for day in pd.date_range("2024-01-01", periods=12, freq="D"):
            for idx in range(n_entities):
                rows.append(
                    {
                        "endpoint_id": f"e{idx:02d}",
                        "date": day,
                        "metric_value": float(bases[idx] + rng.normal(0, 1.0)),
                    }
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "panel.parquet"
            pd.DataFrame(rows).to_parquet(path)
            bundle = build_data_bundle(Config(data_path=path, min_presence_frac=1.0))

        emp = bundle.empirical
        mean_rank = np.asarray(emp["mean_rank"], dtype=float)
        mean_rank_ids = np.asarray(emp["mean_rank_ids"], dtype=str)
        rank_wide = emp["rank_wide"]

        # mean_rank is sorted ascending and the same length as the id array.
        self.assertTrue(np.all(np.diff(mean_rank) >= 0))
        self.assertEqual(mean_rank.size, mean_rank_ids.size)

        # mean_rank_ids[i] is the entity whose own mean rank equals mean_rank[i].
        col_mean_by_id = rank_wide[list(mean_rank_ids)].mean(axis=0).to_numpy(dtype=float)
        self.assertTrue(np.allclose(col_mean_by_id, mean_rank))

        # Masking by a rank band selects only entities truly inside that band.
        lo, hi = 5, 20
        mask = (mean_rank >= lo) & (mean_rank <= hi)
        if mask.any():
            selected = rank_wide[list(mean_rank_ids[mask])].mean(axis=0).to_numpy(dtype=float)
            self.assertTrue(np.all((selected >= lo) & (selected <= hi)))

    def test_process_zst_file_supports_pipeline_local_archives(self):
        import zstandard
        from archive.scripts.reddit.process_month import process_zst_file

        records = [
            b'{"subreddit":"RankDiff","score":3,"created_utc":1704067200,"over_18":false}\n',
            b'{"subreddit":"RankDiff","score":4,"created_utc":1704067200,"over_18":true}\n',
        ]
        compressed = zstandard.ZstdCompressor().compress(b"".join(records))

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "RS_2024-01.zst"
            path.write_bytes(compressed)
            aggregates, lines_processed, errors = process_zst_file(path, "submissions")

        self.assertEqual(lines_processed, 2)
        self.assertEqual(errors, 0)
        self.assertEqual(aggregates[("RankDiff", "2024-01-01")]["score_sum"], 7)


if __name__ == "__main__":
    unittest.main()
