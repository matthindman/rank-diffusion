from __future__ import annotations

from dataclasses import replace

import numpy as np
from scipy import stats as sp_stats

from .types import Config, DataBundle, EstimatedParams, InitialParams


def _estimate_exit_params(bundle: DataBundle, cfg: Config) -> tuple[float, float, float, float]:
    emp = bundle.empirical
    exit_rate = float(emp.get("mean_exit_rate", 0.0))
    alpha = cfg.exit_alpha
    if cfg.exit_incumbent_rate is not None:
        inc_rate = cfg.exit_incumbent_rate
    elif exit_rate > 0:
        inc_rate = exit_rate
    else:
        inc_rate = 0.004
    p_base = inc_rate * (alpha + 1.0)
    trans_rate = cfg.exit_transient_rate
    return p_base, alpha, trans_rate, cfg.entry_burst_frac


def fit_parameter_curves(bundle: DataBundle, init: InitialParams, cfg: Config) -> EstimatedParams:
    z_knots = np.asarray(init.z_knots, dtype=float)
    order = np.argsort(z_knots)
    z_knots = z_knots[order]

    sigma_eta_curve = np.asarray(init.sigma_eta_anchor, dtype=float)[order]
    phi_curve = np.asarray(init.phi_anchor, dtype=float)[order]
    sigma_nu_curve = np.asarray(init.sigma_nu_anchor, dtype=float)[order]
    t_df_curve = np.asarray(init.t_df_anchor, dtype=float)[order]

    alpha_kappa = float(init.alpha_kappa)
    kappa_curve = init.kappa_base_raw * np.exp(alpha_kappa * z_knots)

    burnin_periods = resolve_burnin(init.kappa_base_raw, alpha_kappa, cfg)
    w0_sorted = np.asarray(bundle.empirical["w0_sorted"], dtype=float)

    exit_p_base, exit_alpha, exit_trans, burst_frac = _estimate_exit_params(bundle, cfg)

    return EstimatedParams(
        sigma_obs=init.sigma_obs,
        sigma_het=init.sigma_het,
        alpha_arch=init.alpha_arch,
        t_df_global=init.t_df_global,
        jump_prob=init.jump_prob,
        jump_scale=init.jump_scale,
        alpha_kappa=alpha_kappa,
        kappa_base_raw=init.kappa_base_raw,
        kappa_stab_factor=1.0,
        z_knots=z_knots,
        sigma_eta_curve=sigma_eta_curve,
        phi_curve=phi_curve,
        sigma_nu_curve=sigma_nu_curve,
        kappa_curve=kappa_curve,
        t_df_curve=t_df_curve,
        threshold=init.threshold,
        top_k=init.top_k,
        n_full=max(int(round(bundle.mean_n)), len(w0_sorted)),
        w0_sorted=w0_sorted,
        burnin_periods=burnin_periods,
        metadata={
            "initial_metadata": init.metadata,
            "window_turnover_n": bundle.empirical.get("window_turnover_n"),
            "window_turnover_rate": bundle.empirical.get("window_turnover_rate"),
            "window_turnover_count": bundle.empirical.get("window_turnover_count"),
        },
        exit_p_base=exit_p_base,
        exit_alpha=exit_alpha,
        exit_transient_rate=exit_trans,
        entry_burst_frac=burst_frac,
        t_df_curve_precal=t_df_curve.copy(),
    )


def resolve_burnin(kappa_base: float, alpha_kappa: float, cfg: Config) -> int:
    if cfg.burnin_periods is not None:
        return int(cfg.burnin_periods)

    kappa_median = max(kappa_base * 0.5**alpha_kappa, 1e-8)
    half_life = np.log(2.0) / kappa_median
    return int(max(50, round(3.0 * half_life)))


def _calibration_cfg(bundle: DataBundle, cfg: Config) -> Config:
    horizon_candidates = [
        *(cfg.vr_lags or ()),
        *(cfg.acf_lags or ()),
        *(cfg.racf_lags or ()),
        *(cfg.pers_horizons or ()),
        *(cfg.r2_horizons or ()),
    ]
    min_periods = max(horizon_candidates, default=1) + 2
    auto_periods = max(min_periods, 18 if cfg.dev_mode else 26)
    calibration_periods = cfg.calibration_periods or auto_periods
    simulate_cap = cfg.simulate_periods or bundle.n_periods
    resolved_periods = int(min(bundle.n_periods, simulate_cap, calibration_periods))

    calibration_track_count = cfg.calibration_track_entity_count
    if calibration_track_count is None:
        calibration_track_count = min(cfg.track_entity_count, 1500 if cfg.dev_mode else 3000)

    burnin_periods = cfg.burnin_periods
    if burnin_periods is None and cfg.dev_mode:
        burnin_periods = 10

    return replace(
        cfg,
        simulate_periods=resolved_periods,
        burnin_periods=burnin_periods,
        track_entity_count=int(calibration_track_count),
        n_jobs=1,
        mc_reps=1,
    )


def _candidate_score(diag: dict[str, float], emp: dict[str, object], cfg: Config) -> float:
    score = abs(diag["xsec_var_drift"] - 1.0) / 0.2
    if "xr2_13" in diag and 13 in emp["xr2_emp"]:
        score += abs(diag["xr2_13"] - emp["xr2_emp"][13]) / max(cfg.r2_threshold, 1e-6)
    if "pers13" in diag and 13 in emp["pers_emp"]:
        pers_tol = max(cfg.pers_threshold_min, int(round(cfg.pers_threshold_pct * emp["top_k"])))
        score += abs(diag["pers13"] - emp["pers_emp"][13]) / max(pers_tol, 1)
    return score


def _evaluate_alpha_candidate(alpha: float, params: EstimatedParams, n_periods: int, cfg: Config) -> tuple[float, EstimatedParams, dict[str, float]]:
    from .simulator import simulate_one

    curve = params.kappa_base_raw * np.exp(alpha * params.z_knots)
    candidate = replace(params, alpha_kappa=float(alpha), kappa_curve=curve)
    sim = simulate_one(cfg.random_seed, candidate, n_periods, cfg)
    return float(alpha), candidate, sim["diagnostics"]


def _evaluate_kappa_factor(
    factor: float,
    params: EstimatedParams,
    n_periods: int,
    cfg: Config,
) -> tuple[float, EstimatedParams, dict[str, float]]:
    from .simulator import simulate_one

    curve = params.kappa_base_raw * factor * np.exp(params.alpha_kappa * params.z_knots)
    candidate = replace(params, kappa_curve=curve, kappa_stab_factor=float(factor))
    sim = simulate_one(cfg.random_seed + 101, candidate, n_periods, cfg)
    return float(factor), candidate, sim["diagnostics"]


def estimate_alpha_kappa(params: EstimatedParams, bundle: DataBundle, cfg: Config) -> EstimatedParams:
    emp = bundle.empirical
    cal_cfg = _calibration_cfg(bundle, cfg)
    best = params
    best_score = float("inf")

    results = [_evaluate_alpha_candidate(alpha, params, bundle.n_periods, cal_cfg) for alpha in cfg.alpha_kappa_grid]

    for _, candidate, diag in results:
        score = _candidate_score(diag, emp, cfg)
        if score < best_score:
            best = candidate
            best_score = score
    return best


def calibrate_kappa_stab(params: EstimatedParams, bundle: DataBundle, cfg: Config) -> EstimatedParams:
    emp = bundle.empirical
    cal_cfg = _calibration_cfg(bundle, cfg)
    best = params
    best_score = float("inf")

    results = [_evaluate_kappa_factor(factor, params, bundle.n_periods, cal_cfg) for factor in cfg.kappa_stab_grid]

    for _, candidate, diag in results:
        score = _candidate_score(diag, emp, cfg)
        if score < best_score:
            best = candidate
            best_score = score
    return best


def _compute_sim_band_kurtosis(
    sim: dict[str, object],
    z_knots: np.ndarray,
    n_full: int,
    z_rank_clip: float = 1e-6,
) -> np.ndarray:
    tracked_values = np.asarray(sim["tracked_values"], dtype=float)
    tracked_ranks = np.asarray(sim["tracked_ranks"], dtype=float)

    observed_all = np.all(np.isfinite(tracked_values), axis=0)
    if observed_all.sum() < 20:
        return np.full(len(z_knots), np.nan)

    values = tracked_values[:, observed_all]
    ranks = tracked_ranks[:, observed_all]
    changes = np.diff(values, axis=0)

    mean_rank = np.nanmean(ranks, axis=0)
    z_mean = np.log(np.clip((mean_rank - 0.5) / n_full, z_rank_clip, 1.0))

    band_kurt = np.full(len(z_knots), np.nan)
    assignments = np.argmin(np.abs(z_mean[:, None] - z_knots[None, :]), axis=1)

    for i in range(len(z_knots)):
        mask = assignments == i
        if mask.sum() < 5:
            continue
        band_ch = changes[:, mask].ravel()
        band_ch = band_ch[np.isfinite(band_ch)]
        if band_ch.size > 20:
            band_kurt[i] = float(sp_stats.kurtosis(band_ch, fisher=True))
    return band_kurt


def calibrate_kurtosis(params: EstimatedParams, bundle: DataBundle, cfg: Config) -> EstimatedParams:
    from .simulator import simulate_one

    init_meta = params.metadata.get("initial_metadata", {})
    emp_kurt_target = np.asarray(init_meta.get("anchor_kurtosis", []), dtype=float)
    signal_frac = np.asarray(init_meta.get("anchor_signal_frac", []), dtype=float)

    if emp_kurt_target.size != params.z_knots.size:
        return params

    cal_cfg = _calibration_cfg(bundle, cfg)
    n_cal = cfg.kurtosis_cal_reps
    cal_seeds = [cfg.random_seed + 200 + i for i in range(n_cal)]

    all_sim_kurt = []
    for seed in cal_seeds:
        sim = simulate_one(seed, params, bundle.n_periods, cal_cfg)
        bk = _compute_sim_band_kurtosis(sim, params.z_knots, params.n_full, cfg.z_rank_clip)
        all_sim_kurt.append(bk)

    sim_kurt_median = np.nanmedian(np.array(all_sim_kurt), axis=0)
    t_df_new = params.t_df_curve.copy()
    overshoot = cfg.kurtosis_overshoot

    for i in range(len(params.z_knots)):
        emp_k = emp_kurt_target[i]
        sim_k = sim_kurt_median[i]
        sf = signal_frac[i] if i < signal_frac.size else 1.0

        if sf < cfg.kurtosis_min_signal_frac:
            continue
        if not (np.isfinite(emp_k) and np.isfinite(sim_k)):
            continue
        if emp_k <= 0.5 or sim_k <= 0.5:
            continue
        if abs(sim_k - emp_k) / emp_k <= 0.10:
            continue

        old_df = t_df_new[i]
        if old_df > 4.5:
            old_t_kurt = 6.0 / (old_df - 4.0)
        else:
            old_t_kurt = 6.0 / max(old_df - 4.0, 0.3)

        ratio = emp_k / sim_k
        target_t_kurt = old_t_kurt * (ratio ** overshoot)
        new_df = 4.0 + 6.0 / max(target_t_kurt, 1e-6)
        t_df_new[i] = float(np.clip(new_df, cfg.tdf_bounds[0], cfg.tdf_bounds[1]))

    return replace(params, t_df_curve=t_df_new, t_df_curve_precal=params.t_df_curve.copy())
