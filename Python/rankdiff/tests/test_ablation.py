from rankdiff import Config, run_pipeline
from rankdiff.ablation import run_ablation


def test_ablation_smoke():
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

    assert len(abl) > 0
    assert "level" in abl[0]
    assert "mc_means" in abl[0]
    assert "pass_fail" in abl[0]
    assert "n_pass" in abl[0]
    assert "n_total" in abl[0]