# ---- Fit: curve fitting, calibration, and kurtosis tuning ----
# Translates Python fit.py: fit_parameter_curves, resolve_burnin,
# estimate_alpha_kappa, calibrate_kappa_stab, calibrate_kurtosis, and
# supporting internal helpers.

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Update an estimated-params list in place
#'
#' Creates a shallow copy of the parameter list with selected fields replaced.
#' Preserves the \code{rankdiff_params} class attribute if present.
#'
#' @param params A list (typically of class \code{rankdiff_params}).
#' @param ... Named values to insert or replace.
#' @return A modified copy of \code{params}.
#' @keywords internal
.update_params <- function(params, ...) {
  updates <- list(...)
  for (nm in names(updates)) params[[nm]] <- updates[[nm]]
  params
}

#' Estimate exit / entry parameters from empirical data and config
#'
#' Derives the base exit probability, power-law exponent, transient exit rate,
#' and entry burst fraction used by the simulation kernel.
#'
#' @param bundle A \code{rankdiff_bundle} list.
#' @param cfg A \code{rankdiff_config}.
#' @return A named list with elements \code{p_base}, \code{alpha},
#'   \code{trans_rate}, and \code{burst_frac}.
#' @keywords internal
.estimate_exit_params <- function(bundle, cfg) {
  emp <- bundle$empirical
  exit_rate <- as.double(emp[["mean_exit_rate"]] %||% 0.0)
  alpha <- cfg$exit_alpha
  if (!is.null(cfg$exit_incumbent_rate)) {
    inc_rate <- cfg$exit_incumbent_rate
  } else if (exit_rate > 0) {
    inc_rate <- exit_rate
  } else {
    inc_rate <- 0.004
  }
  p_base <- inc_rate * (alpha + 1.0)
  trans_rate <- cfg$exit_transient_rate
  list(
    p_base     = p_base,
    alpha      = alpha,
    trans_rate = trans_rate,
    burst_frac = cfg$entry_burst_frac
  )
}

#' Build a lightweight calibration config
#'
#' Creates a modified config with shorter simulation horizons, fewer tracked
#' entities, and a single MC replication -- suitable for the fast grid searches
#' in \code{estimate_alpha_kappa} and \code{calibrate_kappa_stab}.
#'
#' @param bundle A \code{rankdiff_bundle} list.
#' @param cfg A \code{rankdiff_config}.
#' @return A \code{rankdiff_config} with reduced settings.
#' @keywords internal
.calibration_cfg <- function(bundle, cfg) {
  horizon_candidates <- c(cfg$vr_lags, cfg$acf_lags, cfg$racf_lags,
                          cfg$pers_horizons, cfg$r2_horizons)
  min_periods <- max(horizon_candidates, 1L) + 2L
  auto_periods <- max(min_periods, if (isTRUE(cfg$dev_mode)) 18L else 26L)
  # Use at least half the production period so that exit/entry-induced
  # variance drift is visible during kappa_stab calibration.
  half_production <- bundle$n_periods %/% 2L
  auto_periods <- max(auto_periods, half_production)
  calibration_periods <- cfg$calibration_periods %||% auto_periods
  simulate_cap <- cfg$simulate_periods %||% bundle$n_periods
  resolved_periods <- as.integer(min(bundle$n_periods, simulate_cap,
                                     calibration_periods))

  calibration_track_count <- cfg$calibration_track_entity_count
  if (is.null(calibration_track_count)) {
    calibration_track_count <- min(
      cfg$track_entity_count,
      if (isTRUE(cfg$dev_mode)) 1500L else 3000L
    )
  }

  burnin_periods <- cfg$burnin_periods
  if (is.null(burnin_periods) && isTRUE(cfg$dev_mode)) {
    burnin_periods <- 10L
  }

  update_config(
    cfg,
    simulate_periods   = resolved_periods,
    burnin_periods     = burnin_periods,
    track_entity_count = as.integer(calibration_track_count),
    n_jobs             = 1L,
    mc_reps            = 1L
  )
}

#' Composite score for alpha_kappa / kappa_stab candidate selection
#'
#' Evaluates a candidate parameter set by comparing simulated diagnostics
#' against empirical targets.  The score is a weighted sum of normalised
#' absolute deviations for cross-sectional variance drift, R-squared,
#' persistence, and rank ACF.
#'
#' @param diag A diagnostics list from \code{compute_sim_diagnostics}.
#' @param emp A named list of empirical targets (from \code{bundle$empirical}).
#' @param cfg A \code{rankdiff_config}.
#' @return Numeric scalar score (lower is better).
#' @keywords internal
.candidate_score <- function(diag, emp, cfg) {
  score <- abs(diag[["xsec_var_drift"]] - 1.0) / 0.2

  # R-squared at horizon 13
  xr2_13 <- diag[["xr2_13"]]
  xr2_emp_13 <- emp[["xr2_emp"]][["13"]]
  if (!is.null(xr2_13) && !is.null(xr2_emp_13) &&
      is.finite(xr2_13) && is.finite(xr2_emp_13)) {
    score <- score + abs(xr2_13 - xr2_emp_13) / max(cfg$r2_threshold, 1e-6)
  }

  # Persistence at horizon 13
  pers13 <- diag[["pers13"]]
  pers_emp_13 <- emp[["pers_emp"]][["13"]]
  if (!is.null(pers13) && !is.null(pers_emp_13) &&
      is.finite(pers13) && is.finite(pers_emp_13)) {
    pers_tol <- max(cfg$pers_threshold_min,
                    as.integer(round(cfg$pers_threshold_pct * emp[["top_k"]])))
    score <- score + abs(pers13 - pers_emp_13) / max(pers_tol, 1L)
  }

  # Rank ACF at lag 1
  racf1 <- diag[["racf1"]]
  racf_emp_1 <- emp[["racf_emp"]][["1"]]
  if (!is.null(racf1) && !is.null(racf_emp_1) &&
      is.finite(racf1) && is.finite(racf_emp_1)) {
    score <- score + abs(racf1 - racf_emp_1) / max(cfg$racf_threshold, 1e-6)
  }

  score
}

#' Compute per-band excess kurtosis from a single simulation
#'
#' Assigns each tracked entity to its nearest z-knot band based on mean
#' log-rank, then computes the excess kurtosis of pooled log-changes within
#' each band.
#'
#' @param sim A simulation result list (from \code{simulate_one}).
#' @param z_knots Numeric vector of sorted z-knot locations.
#' @param n_full Integer simulation universe size.
#' @param z_rank_clip Numeric clipping floor for log-rank coordinate.
#' @return Numeric vector of per-band excess kurtosis (length =
#'   \code{length(z_knots)}).  Bands with fewer than 20 observations return
#'   \code{NA}.
#' @keywords internal
.compute_sim_band_kurtosis <- function(sim, z_knots, n_full, z_rank_clip = 1e-6) {
  tracked_values <- as.matrix(sim[["tracked_values"]])
  tracked_ranks  <- as.matrix(sim[["tracked_ranks"]])

  # Restrict to fully-observed entities
  observed_all <- apply(tracked_values, 2, function(col) all(is.finite(col)))
  if (sum(observed_all) < 20L) {
    return(rep(NA_real_, length(z_knots)))
  }

  values <- tracked_values[, observed_all, drop = FALSE]
  ranks  <- tracked_ranks[, observed_all, drop = FALSE]
  changes <- diff(values)

  mean_rank <- colMeans(ranks, na.rm = TRUE)
  z_mean <- log(pmax(pmin((mean_rank - 0.5) / n_full, 1.0), z_rank_clip))

  band_kurt <- rep(NA_real_, length(z_knots))

  # Assign each entity to its nearest z-knot
  # assignments[j] = index into z_knots for entity j
  assignments <- vapply(z_mean, function(z) {
    which.min(abs(z - z_knots))
  }, integer(1))

  for (i in seq_along(z_knots)) {
    mask <- assignments == i
    if (sum(mask) < 5L) next
    band_ch <- as.vector(changes[, mask, drop = FALSE])
    band_ch <- band_ch[is.finite(band_ch)]
    if (length(band_ch) > 20L) {
      band_kurt[i] <- excess_kurtosis(band_ch)
    }
  }
  band_kurt
}

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

#' Resolve burn-in period count
#'
#' If \code{cfg$burnin_periods} is set, returns that value directly.
#' Otherwise computes an automatic burn-in as
#' \code{max(50, round(3 * half_life))} where the half-life is derived from
#' the median mean-reversion rate.
#'
#' @param kappa_base Numeric scalar: base mean-reversion rate.
#' @param alpha_kappa Numeric scalar: rank-dependence exponent for kappa.
#' @param cfg A \code{rankdiff_config}.
#' @return Integer number of burn-in periods.
#' @export
resolve_burnin <- function(kappa_base, alpha_kappa, cfg) {
  if (!is.null(cfg$burnin_periods)) {
    return(as.integer(cfg$burnin_periods))
  }
  kappa_median <- max(kappa_base * 0.5^alpha_kappa, 1e-8)
  half_life <- log(2.0) / kappa_median
  as.integer(max(50L, round(3.0 * half_life)))
}

#' Fit parameter curves from initial estimates
#'
#' Takes the moment-based initial parameter estimates and constructs the full
#' estimated parameter set used by the simulation kernel.  This includes
#' sorting anchor knots, building interpolation curves, computing burn-in
#' length, and estimating exit/entry parameters.
#'
#' @param bundle A \code{rankdiff_bundle} list produced by
#'   \code{\link{build_data_bundle}}.
#' @param init A \code{rankdiff_initial} list from
#'   \code{\link{estimate_initial_params}}.
#' @param cfg A \code{rankdiff_config}.
#' @return A list of class \code{rankdiff_params} containing all fields
#'   required by \code{\link{simulate_one}}.
#' @export
fit_parameter_curves <- function(bundle, init, cfg) {
  # Sort anchor knots and reorder per-band curves accordingly
  z_knots <- as.double(init$z_knots)
  ord <- order(z_knots)
  z_knots <- z_knots[ord]

  sigma_eta_curve <- as.double(init$sigma_eta_anchor)[ord]
  phi_curve       <- as.double(init$phi_anchor)[ord]
  sigma_nu_curve  <- as.double(init$sigma_nu_anchor)[ord]
  t_df_curve      <- as.double(init$t_df_anchor)[ord]

  alpha_kappa <- as.double(init$alpha_kappa)
  kappa_curve <- init$kappa_base_raw * exp(alpha_kappa * z_knots)

  burnin_periods <- resolve_burnin(init$kappa_base_raw, alpha_kappa, cfg)

  w0_sorted <- as.double(bundle$empirical[["w0_sorted"]])

  exit_info <- .estimate_exit_params(bundle, cfg)

  params <- list(
    sigma_obs           = init$sigma_obs,
    sigma_het           = init$sigma_het,
    alpha_arch          = init$alpha_arch,
    t_df_global         = init$t_df_global,
    jump_prob           = init$jump_prob,
    jump_scale          = init$jump_scale,
    alpha_kappa         = alpha_kappa,
    kappa_base_raw      = init$kappa_base_raw,
    kappa_stab_factor   = 1.0,
    z_knots             = z_knots,
    sigma_eta_curve     = sigma_eta_curve,
    phi_curve           = phi_curve,
    sigma_nu_curve      = sigma_nu_curve,
    kappa_curve         = kappa_curve,
    t_df_curve          = t_df_curve,
    threshold           = init$threshold,
    top_k               = init$top_k,
    n_full              = max(as.integer(round(bundle$mean_n)),
                              length(w0_sorted)),
    w0_sorted           = w0_sorted,
    burnin_periods      = burnin_periods,
    metadata            = list(
      initial_metadata      = init$metadata,
      window_turnover_n     = bundle$empirical[["window_turnover_n"]],
      window_turnover_rate  = bundle$empirical[["window_turnover_rate"]],
      window_turnover_count = bundle$empirical[["window_turnover_count"]]
    ),
    exit_p_base         = exit_info$p_base,
    exit_alpha          = exit_info$alpha,
    exit_transient_rate = exit_info$trans_rate,
    entry_burst_frac    = exit_info$burst_frac,
    t_df_curve_precal   = t_df_curve  # copy before kurtosis calibration
  )
  class(params) <- "rankdiff_params"
  params
}

#' Estimate the alpha_kappa exponent via grid search
#'
#' Iterates over \code{cfg$alpha_kappa_grid}, running a fast calibration
#' simulation for each candidate and selecting the value that minimises the
#' composite candidate score (cross-sectional variance drift, R-squared,
#' persistence, and rank ACF).
#'
#' @param params An estimated parameter list (class \code{rankdiff_params}).
#' @param bundle A \code{rankdiff_bundle} list.
#' @param cfg A \code{rankdiff_config}.
#' @return An updated parameter list with the best \code{alpha_kappa} and
#'   corresponding \code{kappa_curve}.
#' @export
estimate_alpha_kappa <- function(params, bundle, cfg) {
  emp <- bundle$empirical
  cal_cfg <- .calibration_cfg(bundle, cfg)
  best <- params
  best_score <- Inf

  for (alpha in cfg$alpha_kappa_grid) {
    curve <- params$kappa_base_raw * exp(alpha * params$z_knots)
    candidate <- .update_params(params,
                                alpha_kappa = as.double(alpha),
                                kappa_curve = curve)
    sim <- simulate_one(cfg$random_seed, candidate, bundle$n_periods, cal_cfg)
    score <- .candidate_score(sim[["diagnostics"]], emp, cfg)
    if (score < best_score) {
      best <- candidate
      best_score <- score
    }
  }
  best
}

#' Calibrate the kappa stabilisation factor via grid search
#'
#' Iterates over \code{cfg$kappa_stab_grid}, running a fast calibration
#' simulation for each scaling factor and selecting the value that minimises
#' the composite candidate score.
#'
#' @param params An estimated parameter list (class \code{rankdiff_params}).
#' @param bundle A \code{rankdiff_bundle} list.
#' @param cfg A \code{rankdiff_config}.
#' @return An updated parameter list with the best \code{kappa_stab_factor}
#'   and corresponding \code{kappa_curve}.
#' @export
calibrate_kappa_stab <- function(params, bundle, cfg) {
  emp <- bundle$empirical
  cal_cfg <- .calibration_cfg(bundle, cfg)
  best <- params
  best_score <- Inf

  for (factor in cfg$kappa_stab_grid) {
    curve <- params$kappa_base_raw * factor *
             exp(params$alpha_kappa * params$z_knots)
    candidate <- .update_params(params,
                                kappa_curve       = curve,
                                kappa_stab_factor = as.double(factor))
    sim <- simulate_one(cfg$random_seed + 101L, candidate,
                        bundle$n_periods, cal_cfg)
    score <- .candidate_score(sim[["diagnostics"]], emp, cfg)
    if (score < best_score) {
      best <- candidate
      best_score <- score
    }
  }
  best
}

#' Calibrate t-distribution degrees-of-freedom curves via kurtosis matching
#'
#' Runs \code{cfg$kurtosis_cal_reps} calibration simulations, computes
#' per-band excess kurtosis, compares to the empirical anchor kurtosis, and
#' adjusts the \code{t_df_curve} where the relative error exceeds 10%.
#' The adjustment uses an overshoot exponent
#' (\code{cfg$kurtosis_overshoot}) to accelerate convergence when the
#' procedure is iterated.
#'
#' @param params An estimated parameter list (class \code{rankdiff_params}).
#' @param bundle A \code{rankdiff_bundle} list.
#' @param cfg A \code{rankdiff_config}.
#' @return An updated parameter list with recalibrated \code{t_df_curve}
#'   and preserved \code{t_df_curve_precal} (the pre-calibration curve).
#' @export
calibrate_kurtosis <- function(params, bundle, cfg) {
  init_meta <- params$metadata[["initial_metadata"]]
  if (is.null(init_meta)) init_meta <- list()

  emp_kurt_target <- as.double(init_meta[["anchor_kurtosis"]] %||% numeric(0))
  signal_frac     <- as.double(init_meta[["anchor_signal_frac"]] %||% numeric(0))

  if (length(emp_kurt_target) != length(params$z_knots)) {
    return(params)
  }

  cal_cfg <- .calibration_cfg(bundle, cfg)
  n_cal <- cfg$kurtosis_cal_reps
  cal_seeds <- cfg$random_seed + 200L + seq_len(n_cal) - 1L

  # Collect per-band kurtosis across calibration replications
  all_sim_kurt <- vector("list", n_cal)
  for (idx in seq_len(n_cal)) {
    sim <- simulate_one(cal_seeds[idx], params, bundle$n_periods, cal_cfg)
    all_sim_kurt[[idx]] <- .compute_sim_band_kurtosis(
      sim, params$z_knots, params$n_full, cfg$z_rank_clip
    )
  }

  # Median across replications (column-wise)
  kurt_mat <- do.call(rbind, all_sim_kurt)
  sim_kurt_median <- apply(kurt_mat, 2, function(x) median(x, na.rm = TRUE))

  t_df_new <- params$t_df_curve
  overshoot <- cfg$kurtosis_overshoot

  for (i in seq_along(params$z_knots)) {
    emp_k <- emp_kurt_target[i]
    sim_k <- sim_kurt_median[i]
    sf <- if (i <= length(signal_frac)) signal_frac[i] else 1.0

    # Skip bands with low signal fraction
    if (sf < cfg$kurtosis_min_signal_frac) next
    # Skip non-finite values
    if (!is.finite(emp_k) || !is.finite(sim_k)) next
    # Skip very low kurtosis
    if (emp_k <= 0.5 || sim_k <= 0.5) next
    # Skip if already within 10% tolerance
    if (abs(sim_k - emp_k) / emp_k <= 0.10) next

    old_df <- t_df_new[i]
    if (old_df > 4.5) {
      old_t_kurt <- 6.0 / (old_df - 4.0)
    } else {
      old_t_kurt <- 6.0 / max(old_df - 4.0, 0.3)
    }

    ratio <- emp_k / sim_k
    target_t_kurt <- old_t_kurt * (ratio^overshoot)
    new_df <- 4.0 + 6.0 / max(target_t_kurt, 1e-6)
    t_df_new[i] <- min(max(new_df, cfg$tdf_bounds[1]), cfg$tdf_bounds[2])
  }

  .update_params(params,
                 t_df_curve        = t_df_new,
                 t_df_curve_precal = params$t_df_curve)
}
