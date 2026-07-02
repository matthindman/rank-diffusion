from rankdiff import Config, run_pipeline
from rankdiff.sensitivity import run_sensitivity


def test_sensitivity_smoke():
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
    sens = run_sensitivity(result.params, result.data, cfg)

    assert len(sens) > 0
    first_key = next(iter(sens))
    first_delta = next(iter(sens[first_key]))
    entry = sens[first_key][first_delta]

    assert "value" in entry
    assert "mc_means" in entry
    assert "pass_fail" in entry
    assert "n_pass" in entry
    assert "n_total" in entry