import numpy as np

from rankdiff import Config
from rankdiff.preprocess import build_data_bundle
from rankdiff.initializers import estimate_initial_params
from rankdiff.fit import (
    fit_parameter_curves,
    estimate_alpha_kappa,
    calibrate_kappa_stab,
)
from rankdiff import run_pipeline

# end to end
def test_end_to_end_pipeline():
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

    bundle = build_data_bundle(cfg)
    init = estimate_initial_params(bundle, cfg)
    params = fit_parameter_curves(bundle, init, cfg)
    params = estimate_alpha_kappa(params, bundle, cfg)
    params = calibrate_kappa_stab(params, bundle, cfg)

    assert bundle.n_periods > 0
    assert bundle.n_entities > 0
    assert len(bundle.panel) > 0

    assert np.isfinite(init.sigma_obs)
    assert np.isfinite(init.sigma_het)
    assert np.isfinite(init.alpha_arch)
    assert np.isfinite(init.t_df_global)

    assert np.isfinite(params.sigma_obs)
    assert np.isfinite(params.sigma_het)
    assert np.isfinite(params.alpha_arch)
    assert np.isfinite(params.t_df_global)
    assert np.isfinite(params.jump_prob)

    assert params.top_k > 0
    assert params.burnin_periods > 0
    assert len(params.kappa_curve) > 0

    assert "acf_emp" in bundle.empirical
    assert "racf_emp" in bundle.empirical
    assert "pers_emp" in bundle.empirical
    assert "xr2_emp" in bundle.empirical
    assert "zipf_slope" in bundle.empirical

# single run // public facing version
def test_public_run_pipeline():
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

    assert result.data.n_periods > 0
    assert result.data.n_entities > 0
    assert result.params.top_k > 0
    assert "acf_emp" in result.diagnostics