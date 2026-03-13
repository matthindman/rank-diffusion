from __future__ import annotations

import numpy as np

from .diagnostics import compute_sim_diagnostics
from .types import Config, EstimatedParams, SimFeatures


def _sample_student_t_vector(rng: np.random.Generator, df_vec: np.ndarray) -> np.ndarray:
    z = rng.standard_normal(df_vec.size)
    chi = rng.chisquare(np.clip(df_vec, 1.0, None))
    return z / np.sqrt(chi / np.clip(df_vec, 1.0, None))


def _interpolate_curve(z: np.ndarray, z_knots: np.ndarray, values: np.ndarray) -> np.ndarray:
    return np.interp(z, z_knots, values, left=values[0], right=values[-1])


def _extend_initial_state(w0_sorted: np.ndarray, n_full: int) -> np.ndarray:
    if w0_sorted.size >= n_full:
        return w0_sorted[:n_full].astype(np.float64, copy=True)

    if w0_sorted.size < 20:
        tail = np.full(n_full - w0_sorted.size, w0_sorted[-1] if w0_sorted.size else 0.0)
        return np.concatenate([w0_sorted, tail]).astype(np.float64, copy=False)

    tail_n = min(2000, w0_sorted.size)
    tail_ranks = np.arange(w0_sorted.size - tail_n + 1, w0_sorted.size + 1, dtype=float)
    slope, intercept = np.polyfit(np.log(tail_ranks), w0_sorted[-tail_n:], 1)
    ext_ranks = np.arange(w0_sorted.size + 1, n_full + 1, dtype=float)
    ext_vals = intercept + slope * np.log(ext_ranks)
    return np.concatenate([w0_sorted, ext_vals]).astype(np.float64, copy=False)


def _tracked_indices(n_full: int, track_count: int, top_k: int, seed: int) -> np.ndarray:
    if n_full <= track_count:
        return np.arange(n_full, dtype=np.int32)

    rng = np.random.default_rng(seed)
    sampled = rng.choice(n_full, size=track_count, replace=False)
    return np.sort(sampled).astype(np.int32, copy=False)


def _resolve_simulation_size(params: EstimatedParams, cfg: Config, n_global: int) -> int:
    if cfg.universe_mode != "topk_buffered":
        return n_global
    if cfg.top_k_focus is None:
        raise ValueError("top_k_focus must be set when universe_mode='topk_buffered'.")
    if cfg.buffer_k <= 0:
        raise ValueError("buffer_k must be positive when universe_mode='topk_buffered'.")
    if cfg.top_k_focus < params.top_k:
        raise ValueError(
            f"top_k_focus ({cfg.top_k_focus}) must be at least as large as diagnostic top_k ({params.top_k})."
        )
    return int(min(n_global, cfg.top_k_focus + cfg.buffer_k))


def _prepare_boundary_refresh(
    params: EstimatedParams,
    cfg: Config,
    w0_full: np.ndarray,
    n_sim: int,
    n_global: int,
) -> tuple[np.ndarray, float, int]:
    if cfg.universe_mode != "topk_buffered" or n_sim >= n_global:
        return np.array([], dtype=np.float64), 0.0, 0

    boundary_hi = min(n_global, n_sim + max(cfg.buffer_k, 1))
    boundary_values = np.asarray(w0_full[n_sim:boundary_hi], dtype=np.float64)
    if boundary_values.size == 0:
        fallback = w0_full[min(max(n_sim - 1, 0), w0_full.size - 1)]
        boundary_values = np.array([fallback], dtype=np.float64)

    boundary_scale = float(np.std(boundary_values))
    turnover_rate = params.metadata.get("window_turnover_rate")
    if turnover_rate is None or not np.isfinite(turnover_rate):
        replace_n = 0
    else:
        replace_n = int(np.ceil(float(turnover_rate) * n_sim))
    replace_n = int(min(max(replace_n, 0), cfg.buffer_k))
    return boundary_values, boundary_scale, replace_n


def _resolve_effective_params(
    params: EstimatedParams,
    cfg: Config,
    features: SimFeatures | None,
) -> tuple[EstimatedParams, bool, bool, bool]:
    """Apply feature flags to produce effective simulation parameters."""
    if features is None:
        return params, cfg.exit_enabled, cfg.use_obs_noise, True

    from dataclasses import replace

    p = params
    use_exit = features.exit_entry and cfg.exit_enabled
    use_obs = features.obs_noise and cfg.use_obs_noise
    use_heavy = features.heavy_tails

    burnin = p.burnin_periods if features.burn_in else 0

    if not features.kappa:
        kappa_curve = np.zeros_like(p.kappa_curve)
    elif not features.rank_dep_kappa:
        kappa_uniform = p.kappa_base_raw * np.mean(
            np.exp(p.alpha_kappa * p.z_knots)
        )
        kappa_curve = np.full_like(p.kappa_curve, kappa_uniform)
    elif not features.kappa_stab:
        kappa_curve = p.kappa_base_raw * np.exp(p.alpha_kappa * p.z_knots)
    else:
        kappa_curve = p.kappa_curve

    if not features.arch:
        alpha_arch = 0.0
    else:
        alpha_arch = p.alpha_arch

    if not features.heavy_tails:
        t_df_curve = np.full_like(p.t_df_curve, 200.0)
    elif not features.calibrated_tdf and p.t_df_curve_precal is not None:
        t_df_curve = p.t_df_curve_precal.copy()
    else:
        t_df_curve = p.t_df_curve

    p = replace(
        p,
        burnin_periods=burnin,
        kappa_curve=kappa_curve,
        alpha_arch=alpha_arch,
        t_df_curve=t_df_curve,
    )
    return p, use_exit, use_obs, use_heavy


def simulate_one(
    seed: int,
    params: EstimatedParams,
    n_periods: int,
    cfg: Config,
    features: SimFeatures | None = None,
) -> dict[str, object]:
    params, use_exit, use_obs, use_heavy = _resolve_effective_params(params, cfg, features)

    rng = np.random.default_rng(seed)
    n_global = int(params.n_full)
    n_sim = _resolve_simulation_size(params, cfg, n_global)
    t_record = int(cfg.simulate_periods or n_periods)
    t_record = min(t_record, n_periods)
    t_total = params.burnin_periods + t_record

    w0_full = _extend_initial_state(params.w0_sorted, n_global)
    tau = w0_full[:n_sim].astype(np.float64, copy=True)
    c_state = np.zeros(n_sim, dtype=np.float64)
    last_z_sq = np.ones(n_sim, dtype=np.float64)
    entity_ids = np.arange(n_sim, dtype=np.int64)
    next_entity_id = int(n_sim)
    mean_reversion_target = float(np.mean(w0_full))

    het_lo = float(np.exp(-3.0 * params.sigma_het))
    het_hi = float(np.exp(3.0 * params.sigma_het))
    het_multiplier = np.clip(np.exp(rng.normal(0.0, params.sigma_het, n_sim)), het_lo, het_hi)

    ep_type = np.zeros(n_sim, dtype=np.int32)

    tracked = _tracked_indices(n_sim, min(cfg.track_entity_count, n_sim), params.top_k, seed)
    tracked_mask = np.zeros(n_sim, dtype=bool)
    tracked_mask[tracked] = True
    tracked_values = np.full((t_record, tracked.size), np.nan, dtype=np.float32)
    tracked_ranks = np.zeros((t_record, tracked.size), dtype=np.int32)
    top_ids = np.full((t_record, params.top_k), -1, dtype=np.int64)
    observed_counts = np.zeros(t_record, dtype=np.int32)
    xsec_var = np.zeros(t_record, dtype=np.float64)
    period0_sorted_values = np.array([], dtype=np.float32)

    thresholds = params.threshold.threshold_by_period[:t_record]
    top_store_n = max(1000, params.top_k * 50)
    boundary_values, boundary_scale, replace_n = _prepare_boundary_refresh(params, cfg, w0_full, n_sim, n_global)

    exit_p_base = params.exit_p_base
    exit_alpha = params.exit_alpha
    exit_trans = params.exit_transient_rate
    burst_frac = params.entry_burst_frac

    for t_abs in range(t_total):
        x_true = tau + c_state
        order = np.argsort(-x_true)
        latent_rank = np.empty(n_sim, dtype=np.int32)
        latent_rank[order] = np.arange(1, n_sim + 1, dtype=np.int32)

        z_rank = np.log(np.clip((latent_rank.astype(np.float64) - 0.5) / n_global, cfg.z_rank_clip, 1.0))
        sigma_eta = _interpolate_curve(z_rank, params.z_knots, params.sigma_eta_curve) * het_multiplier
        phi = _interpolate_curve(z_rank, params.z_knots, params.phi_curve)
        sigma_nu = _interpolate_curve(z_rank, params.z_knots, params.sigma_nu_curve) * het_multiplier
        kappa = _interpolate_curve(z_rank, params.z_knots, params.kappa_curve)
        df_vec = np.clip(_interpolate_curve(z_rank, params.z_knots, params.t_df_curve), 3.0, None)

        jump_mask = rng.random(n_sim) < params.jump_prob
        eta = rng.normal(0.0, sigma_eta)
        if jump_mask.any():
            eta[jump_mask] = rng.normal(0.0, sigma_eta[jump_mask] * params.jump_scale)

        arch_var = (1.0 - params.alpha_arch) + params.alpha_arch * last_z_sq
        arch_scale = np.sqrt(np.clip(arch_var, cfg.arch_clip[0], cfg.arch_clip[1]))

        if use_heavy:
            t_raw = _sample_student_t_vector(rng, df_vec)
            t_var_factor = np.sqrt(np.clip((df_vec - 2.0) / df_vec, 0.5 / df_vec, None))
            nu = sigma_nu * arch_scale * t_var_factor * t_raw
        else:
            nu = sigma_nu * arch_scale * rng.standard_normal(n_sim)

        c_state = phi * c_state + nu
        last_z_sq = np.clip(np.square(nu / np.maximum(sigma_nu, 1e-8)), 0.0, cfg.z_sq_clip)

        current_mean = mean_reversion_target if cfg.universe_mode == "topk_buffered" else float(np.mean(tau))
        tau = tau + eta - kappa * (tau - current_mean)

        t_rec = t_abs - params.burnin_periods
        if t_rec < 0:
            continue

        x_true = tau + c_state
        obs = x_true + rng.normal(0.0, params.sigma_obs, n_sim) if use_obs else x_true
        threshold_log = np.log1p(float(thresholds[t_rec]))
        observed_mask = obs >= threshold_log

        observed_order = order[observed_mask[order]]
        observed_count = int(observed_order.size)
        observed_counts[t_rec] = observed_count
        xsec_var[t_rec] = float(np.var(tau))

        if observed_count > 0:
            tracked_rank = np.zeros(tracked.size, dtype=np.int32)
            tracked_observed = observed_mask[tracked]
            if tracked_observed.any():
                observed_latent_ranks = np.flatnonzero(observed_mask[order]) + 1
                tracked_rank[tracked_observed] = np.searchsorted(
                    observed_latent_ranks,
                    latent_rank[tracked][tracked_observed],
                    side="left",
                ) + 1
            tracked_ranks[t_rec] = tracked_rank

            tracked_obs = obs[tracked]
            tracked_obs[tracked_rank == 0] = np.nan
            tracked_values[t_rec] = tracked_obs.astype(np.float32)

            top_n = min(params.top_k, observed_count)
            top_ids[t_rec, :top_n] = entity_ids[observed_order[:top_n]]

            if t_rec == 0:
                keep_n = min(top_store_n, observed_count)
                period0_sorted_values = obs[observed_order[:keep_n]].astype(np.float32)

        # --- Entry/exit process ---
        if use_exit and exit_p_base > 0 and t_abs < t_total - 1:
            nr = latent_rank.astype(np.float64) / n_sim
            p_exit = np.where(
                ep_type == 0,
                exit_p_base * np.power(nr, exit_alpha),
                exit_trans,
            )
            exit_mask = rng.random(n_sim) < p_exit
            n_ex = int(exit_mask.sum())

            if n_ex > 0:
                exi = np.where(exit_mask)[0]
                n_burst = max(1, int(n_ex * burst_frac))
                n_norm = n_ex - n_burst

                surviving = ~exit_mask
                bq = float(np.percentile(tau[surviving], 10))
                bstd = float(np.std(tau[tau < np.median(tau)]) * 0.4)
                bstd = max(bstd, 1e-6)
                new_tau = rng.normal(bq, bstd, n_norm)

                if n_burst > 0:
                    buq = float(np.percentile(tau[surviving], 90))
                    bust = float(np.std(tau) * 0.25)
                    bust = max(bust, 1e-6)
                    burst_tau = rng.normal(buq, bust, n_burst)
                    new_tau = np.concatenate([new_tau, burst_tau])

                tau[exi] = new_tau
                if use_heavy:
                    from scipy import stats as sp_stats
                    c_state[exi] = sp_stats.t.rvs(
                        df=params.t_df_global, size=n_ex,
                        random_state=rng.integers(0, 2**31),
                    ) * 0.3
                else:
                    c_state[exi] = rng.normal(0, 0.3, n_ex)
                het_multiplier[exi] = np.clip(
                    np.exp(rng.normal(0.0, params.sigma_het, n_ex)),
                    het_lo, het_hi,
                )
                last_z_sq[exi] = 1.0
                ep_type[exi] = 1
                entity_ids[exi] = np.arange(next_entity_id, next_entity_id + n_ex, dtype=np.int64)
                next_entity_id += n_ex

        # --- Boundary refresh (topk_buffered mode) ---
        if replace_n > 0 and t_abs < t_total - 1:
            refresh_candidates = order[::-1]
            refresh_slots = refresh_candidates[~tracked_mask[refresh_candidates]][:replace_n]
            if refresh_slots.size > 0:
                draws = boundary_values[rng.integers(0, boundary_values.size, size=refresh_slots.size)]
                if boundary_scale > 0.0:
                    draws = draws + rng.normal(0.0, boundary_scale, size=refresh_slots.size)
                tau[refresh_slots] = draws
                c_state[refresh_slots] = 0.0
                last_z_sq[refresh_slots] = 1.0
                het_multiplier[refresh_slots] = np.clip(
                    np.exp(rng.normal(0.0, params.sigma_het, refresh_slots.size)),
                    het_lo,
                    het_hi,
                )
                entity_ids[refresh_slots] = np.arange(next_entity_id, next_entity_id + refresh_slots.size, dtype=np.int64)
                next_entity_id += int(refresh_slots.size)

    sim = {
        "tracked_ids": entity_ids[tracked].copy(),
        "tracked_values": tracked_values,
        "tracked_ranks": tracked_ranks,
        "top_ids": top_ids,
        "observed_counts": observed_counts,
        "xsec_var": xsec_var,
        "period0_sorted_values": period0_sorted_values,
    }
    sim["diagnostics"] = compute_sim_diagnostics(sim, cfg)
    return sim


def simulate_many(
    params: EstimatedParams,
    bundle,
    cfg: Config,
    features: SimFeatures | None = None,
) -> list[dict[str, object]]:
    n_rep = cfg.resolved_mc_reps
    seeds = [cfg.random_seed + i * 7919 for i in range(n_rep)]
    n_periods = bundle.n_periods
    return [simulate_one(seed, params, n_periods, cfg, features=features) for seed in seeds]
