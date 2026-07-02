from rankdiff import Config, run_pipeline
from rankdiff.ablation import run_ablation
from rankdiff.sensitivity import run_sensitivity
from rankdiff.io import (
    save_fit_result,
    save_ablation_results,
    save_sensitivity_results,
)


def test_save_outputs_smoke(tmp_path):
    cfg = Config(
        data_path="data/toy_rank_data.parquet",
        id_col="entity_id",
        timestamp_col="timestamp",
        metric_col="metric_value",
        rank_col=None,
        dev_mode=True,
        track_entity_count=50,
        max_duplicate_entity_period_rate=0.05,
        min_anchor_bins=1,
        max_anchor_bins=3,
        min_anchor_bin_size=5,
        acf_sample_size=25,
        n_optim_restarts=5,
        mc_reps=1,
        mc_reps_dev=1,
        kurtosis_cal_reps=1,
    )

    result = run_pipeline(cfg)
    abl = run_ablation(result.params, result.data, cfg)
    sens = run_sensitivity(result.params, result.data, cfg)

    save_fit_result(result, tmp_path)
    save_ablation_results(abl, tmp_path)
    save_sensitivity_results(sens, result.params, tmp_path)

    assert (tmp_path / "fit_result.json").exists()
    assert (tmp_path / "curves.csv").exists()
    assert (tmp_path / "ablation.json").exists()
    assert (tmp_path / "ablation_summary.txt").exists()
    assert (tmp_path / "sensitivity.json").exists()
    assert (tmp_path / "sensitivity_summary.txt").exists()