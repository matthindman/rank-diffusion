from pathlib import Path

from rankdiff import Config, run_pipeline
from rankdiff.ablation import run_ablation
from rankdiff.sensitivity import run_sensitivity
from rankdiff.diagnostics import score_diagnostics
from rankdiff.plotting import plot_core_diagnostics, plot_ablation, plot_sensitivity
from rankdiff.simulator import simulate_many


def test_plotting_smoke(tmp_path):
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

    sims = simulate_many(result.params, result.data, cfg)
    sim_diags = [sim["diagnostics"] for sim in sims]
    score = score_diagnostics(result.data.empirical, sim_diags, cfg)

    abl = run_ablation(result.params, result.data, cfg)
    sens = run_sensitivity(result.params, result.data, cfg)

    p1 = plot_core_diagnostics(result.data, score, tmp_path, "toy")
    p2 = plot_ablation(abl, tmp_path, "toy")
    p3 = plot_sensitivity(sens, cfg.sensitivity_deltas, tmp_path, "toy")

    assert Path(p1).exists()
    assert Path(p2).exists()
    assert Path(p3).exists()