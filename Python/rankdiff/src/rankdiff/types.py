from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping

import numpy as np
import pandas as pd

Cadence = Literal["auto", "daily", "weekly"]
ThresholdMode = Literal["observed_min", "provided", "timevarying"]
UniverseMode = Literal["full", "topk_buffered"]


@dataclass(frozen=True)
class Config:
    """
    Main configuration object for rankdiff usage

    Defines input data locations, column mappings, preprocessing choices,
    model controls, calibration settings, and output options
    """
    
    data_path: str | Path
    id_col: str = "endpoint_id"
    timestamp_col: str = "date"
    metric_col: str = "metric_value"
    rank_col: str | None = "rank"
    cadence: Cadence = "auto"
    threshold_mode: ThresholdMode = "observed_min"
    activity_threshold: float | None = None
    platform: str = "auto"
    universe_mode: UniverseMode = "full"
    top_k_focus: int | None = None
    buffer_k: int = 0
    fit_start: str | None = None
    fit_end: str | None = None
    simulate_periods: int | None = None
    burnin_periods: int | None = None
    calibration_periods: int | None = None
    calibration_track_entity_count: int | None = None
    mc_reps: int = 25
    mc_reps_dev: int = 5
    n_jobs: int = 1
    dev_mode: bool = False
    random_seed: int = 42
    track_entity_count: int = 5000
    max_dense_entities: int = 50000
    max_duplicate_entity_period_rate: float = 0.001
    acf_sample_size: int = 2000
    top_k_pct: float = 0.01
    min_top_k: int = 10
    min_anchor_bins: int = 6
    max_anchor_bins: int = 12
    min_anchor_bin_size: int = 250
    z_rank_clip: float = 1e-6
    sigma_obs_bounds: tuple[float, float] = (0.01, 0.50)
    tdf_bounds: tuple[float, float] = (3.0, 200.0)
    alpha_arch_bounds: tuple[float, float] = (0.01, 0.50)
    arch_clip: tuple[float, float] = (0.1, 10.0)
    z_sq_clip: float = 4.0
    jump_prob_floor: float = 0.005
    alpha_kappa_default: float = 0.5
    alpha_kappa_grid: tuple[float, ...] = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)
    kappa_stab_grid: tuple[float, ...] = (0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5, 2.0)
    n_optim_restarts: int = 200
    use_obs_noise: bool = True
    exit_enabled: bool = True
    exit_alpha: float = 0.3
    exit_incumbent_rate: float | None = None
    exit_transient_rate: float = 0.07
    entry_burst_frac: float = 0.008
    kurtosis_cal_reps: int = 5
    kurtosis_overshoot: float = 1.5
    kurtosis_min_signal_frac: float = 0.30
    vr_lags: tuple[int, ...] = (2, 4, 8, 13)
    acf_lags: tuple[int, ...] = (1, 2)
    racf_lags: tuple[int, ...] = (1, 4, 13)
    pers_horizons: tuple[int, ...] = (1, 4, 13)
    r2_horizons: tuple[int, ...] = (1, 4, 13)
    vr_threshold: float = 0.20
    acf_threshold: float = 0.08
    racf_threshold: float = 0.08
    pers_threshold_pct: float = 0.15
    pers_threshold_min: int = 3
    r2_threshold: float = 0.08
    zipf_fit_fraction: float = 0.40
    sensitivity_deltas: tuple[float, ...] = (-0.20, -0.10, 0.0, 0.10, 0.20)
    max_rank_filter: int | None = None
    min_presence_frac: float = 1.0
    max_noise_frac: float = 0.50
    min_perm_frac: float = 0.10
    output_dir: str | Path | None = None
    skip_plots: bool = False

    @property
    def resolved_mc_reps(self) -> int:
        return self.mc_reps_dev if self.dev_mode else self.mc_reps


@dataclass
class ThresholdModel:
    threshold_by_period: np.ndarray
    max_missing_value_by_period: np.ndarray
    effectively_exact_above_threshold: bool = True


@dataclass
class DataBundle:
    panel: pd.DataFrame
    platform: str
    cadence: str
    dates: pd.Index
    n_periods: int
    n_entities: int
    mean_n: float
    max_n: int
    balanced_ids: np.ndarray
    tracked_entity_ids: np.ndarray
    threshold: ThresholdModel
    empirical: Mapping[str, Any]


@dataclass
class InitialParams:
    sigma_obs: float
    sigma_het: float
    alpha_arch: float
    t_df_global: float
    jump_prob: float
    jump_scale: float
    alpha_kappa: float
    kappa_base_raw: float
    z_knots: np.ndarray
    sigma_eta_anchor: np.ndarray
    phi_anchor: np.ndarray
    sigma_nu_anchor: np.ndarray
    t_df_anchor: np.ndarray
    threshold: ThresholdModel
    top_k: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EstimatedParams:
    sigma_obs: float
    sigma_het: float
    alpha_arch: float
    t_df_global: float
    jump_prob: float
    jump_scale: float
    alpha_kappa: float
    kappa_base_raw: float
    kappa_stab_factor: float
    z_knots: np.ndarray
    sigma_eta_curve: np.ndarray
    phi_curve: np.ndarray
    sigma_nu_curve: np.ndarray
    kappa_curve: np.ndarray
    t_df_curve: np.ndarray
    threshold: ThresholdModel
    top_k: int
    n_full: int
    w0_sorted: np.ndarray
    burnin_periods: int
    metadata: dict[str, Any] = field(default_factory=dict)
    exit_p_base: float = 0.0
    exit_alpha: float = 0.3
    exit_transient_rate: float = 0.07
    entry_burst_frac: float = 0.008
    t_df_curve_precal: np.ndarray | None = None


@dataclass(frozen=True)
class SimFeatures:
    burn_in: bool = True
    kappa: bool = True
    rank_dep_kappa: bool = True
    kappa_stab: bool = True
    heavy_tails: bool = True
    arch: bool = True
    obs_noise: bool = True
    exit_entry: bool = True
    calibrated_tdf: bool = True


@dataclass
class FitResult:
    config: Config
    data: DataBundle
    initial: InitialParams
    params: EstimatedParams
    diagnostics: Mapping[str, Any]
