# ---- Configuration and type constructors ----
# Translates Python types.py: Config, DataBundle, InitialParams, EstimatedParams,
# SimFeatures, FitResult, ThresholdModel.
# Follows the existing R project pattern of config-as-list (see R/config.R in
# the parent project).

#' Create a rankdiff configuration
#'
#' Constructs a configuration list controlling all aspects of the rank-diffusion
#' fitting pipeline: data I/O, estimation, simulation, diagnostics, and output.
#'
#' @param data_path Path to a parquet file containing the input panel.
#' @param id_col Column name for entity identifiers.
#' @param timestamp_col Column name for timestamps.
#' @param metric_col Column name for the metric (e.g. impressions, views).
#' @param rank_col Column name for pre-computed ranks (or NULL).
#' @param cadence One of "auto", "daily", "weekly".
#' @param threshold_mode One of "observed_min", "provided", "timevarying".
#' @param activity_threshold Fixed threshold when threshold_mode = "provided".
#' @param platform Platform label; "auto" infers from filename.
#' @param universe_mode "full" or "topk_buffered".
#' @param top_k_focus Number of top entities to focus on (topk_buffered mode).
#' @param buffer_k Buffer size beyond top_k_focus.
#' @param fit_start Optional start date for fitting window (character or Date).
#' @param fit_end Optional end date for fitting window.
#' @param simulate_periods Number of periods to simulate (NULL = use data length).
#' @param burnin_periods Number of burn-in periods (NULL = auto).
#' @param calibration_periods Periods used in calibration sims (NULL = auto).
#' @param calibration_track_entity_count Tracked entities in calibration (NULL = auto).
#' @param mc_reps Number of Monte Carlo replications.
#' @param mc_reps_dev MC reps in dev mode.
#' @param n_jobs Requested parallel worker count.
#' @param dev_mode If TRUE, use faster/smaller settings.
#' @param random_seed RNG seed for reproducibility.
#' @param track_entity_count Number of entities to track for diagnostics.
#' @param max_dense_entities Cap on dense tracking.
#' @param max_duplicate_entity_period_rate Max allowed duplicate rate.
#' @param acf_sample_size Number of entities sampled for ACF computation.
#' @param top_k_pct Fraction of mean_n used for top_k.
#' @param min_top_k Minimum top_k.
#' @param min_anchor_bins Minimum number of anchor bins.
#' @param max_anchor_bins Maximum number of anchor bins.
#' @param min_anchor_bin_size Minimum entities per anchor bin.
#' @param z_rank_clip Clipping epsilon for log-rank coordinate.
#' @param sigma_obs_bounds Lower and upper bounds for sigma_obs.
#' @param tdf_bounds Lower and upper bounds for t degrees of freedom.
#' @param alpha_arch_bounds Bounds for ARCH coefficient.
#' @param arch_clip Clipping bounds for ARCH variance multiplier.
#' @param z_sq_clip Clipping bound for squared innovations.
#' @param jump_prob_floor Minimum jump probability.
#' @param alpha_kappa_default Default alpha_kappa.
#' @param alpha_kappa_grid Grid of alpha_kappa candidates.
#' @param kappa_stab_grid Grid of kappa stabilisation factor candidates.
#' @param n_optim_restarts Number of optimisation restarts for band fitting.
#' @param use_obs_noise Whether to include observation noise.
#' @param exit_enabled Whether to enable entry/exit process.
#' @param exit_alpha Power-law exponent for rank-dependent exit.
#' @param exit_incumbent_rate Override for incumbent exit rate (NULL = data-driven).
#' @param exit_transient_rate Exit rate for recent entrants.
#' @param entry_burst_frac Fraction of entrants that burst to top.
#' @param kurtosis_cal_reps Number of reps for kurtosis calibration.
#' @param kurtosis_overshoot Overshoot factor for kurtosis calibration.
#' @param kurtosis_min_signal_frac Minimum signal fraction for calibration.
#' @param vr_lags Variance ratio lags.
#' @param acf_lags Autocorrelation lags.
#' @param racf_lags Rank autocorrelation lags.
#' @param pers_horizons Persistence horizons.
#' @param r2_horizons Cross-sectional R-squared horizons.
#' @param vr_threshold Pass/fail threshold for variance ratios.
#' @param acf_threshold Pass/fail threshold for ACF.
#' @param racf_threshold Pass/fail threshold for rank ACF.
#' @param pers_threshold_pct Persistence tolerance as fraction of top_k.
#' @param pers_threshold_min Minimum persistence tolerance.
#' @param r2_threshold Pass/fail threshold for R-squared.
#' @param zipf_fit_fraction Fraction of period-0 entities for Zipf fit.
#' @param sensitivity_deltas Perturbation deltas for sensitivity analysis.
#' @param max_rank_filter Keep only entities with rank <= this value (NULL = no filter).
#' @param min_presence_frac Minimum fraction of periods an entity must appear in.
#' @param max_noise_frac Maximum fraction of change variance from obs noise.
#' @param min_perm_frac Minimum permanent component fraction.
#' @param output_dir Output directory for results (NULL = auto).
#' @param skip_plots If TRUE, skip diagnostic plots.
#' @return A list of class \code{rankdiff_config}.
#' @export
create_config <- function(
    data_path,
    id_col               = "endpoint_id",
    timestamp_col         = "date",
    metric_col            = "metric_value",
    rank_col              = "rank",
    cadence               = "auto",
    threshold_mode        = "observed_min",
    activity_threshold    = NULL,
    platform              = "auto",
    universe_mode         = "full",
    top_k_focus           = NULL,
    buffer_k              = 0L,
    fit_start             = NULL,
    fit_end               = NULL,
    simulate_periods      = NULL,
    burnin_periods        = NULL,
    calibration_periods   = NULL,
    calibration_track_entity_count = NULL,
    mc_reps               = 25L,
    mc_reps_dev           = 5L,
    n_jobs                = 1L,
    dev_mode              = FALSE,
    random_seed           = 42L,
    track_entity_count    = 5000L,
    max_dense_entities    = 50000L,
    max_duplicate_entity_period_rate = 0.001,
    acf_sample_size       = 2000L,
    top_k_pct             = 0.01,
    min_top_k             = 10L,
    min_anchor_bins       = 6L,
    max_anchor_bins       = 12L,
    min_anchor_bin_size   = 250L,
    z_rank_clip           = 1e-6,
    sigma_obs_bounds      = c(0.01, 0.50),
    tdf_bounds            = c(3.0, 200.0),
    alpha_arch_bounds     = c(0.01, 0.50),
    arch_clip             = c(0.1, 10.0),
    z_sq_clip             = 4.0,
    jump_prob_floor       = 0.005,
    alpha_kappa_default   = 0.5,
    alpha_kappa_grid      = c(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8),
    kappa_stab_grid       = c(0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5, 2.0),
    n_optim_restarts      = 200L,
    use_obs_noise         = TRUE,
    exit_enabled          = TRUE,
    exit_alpha            = 0.3,
    exit_incumbent_rate   = NULL,
    exit_transient_rate   = 0.07,
    entry_burst_frac      = 0.008,
    kurtosis_cal_reps     = 5L,
    kurtosis_overshoot    = 1.5,
    kurtosis_min_signal_frac = 0.30,
    vr_lags               = c(2L, 4L, 8L, 13L),
    acf_lags              = c(1L, 2L),
    racf_lags             = c(1L, 4L, 13L),
    pers_horizons         = c(1L, 4L, 13L),
    r2_horizons           = c(1L, 4L, 13L),
    vr_threshold          = 0.20,
    acf_threshold         = 0.08,
    racf_threshold        = 0.08,
    pers_threshold_pct    = 0.15,
    pers_threshold_min    = 3L,
    r2_threshold          = 0.08,
    zipf_fit_fraction     = 0.40,
    sensitivity_deltas    = c(-0.20, -0.10, 0.0, 0.10, 0.20),
    max_rank_filter       = NULL,
    min_presence_frac     = 1.0,
    max_noise_frac        = 0.50,
    min_perm_frac         = 0.10,
    output_dir            = NULL,
    skip_plots            = FALSE
) {
  cfg <- list(
    data_path              = as.character(data_path),
    id_col                 = id_col,
    timestamp_col          = timestamp_col,
    metric_col             = metric_col,
    rank_col               = rank_col,
    cadence                = match.arg(cadence, c("auto", "daily", "weekly")),
    threshold_mode         = match.arg(threshold_mode, c("observed_min", "provided", "timevarying")),
    activity_threshold     = activity_threshold,
    platform               = platform,
    universe_mode          = match.arg(universe_mode, c("full", "topk_buffered")),
    top_k_focus            = top_k_focus,
    buffer_k               = as.integer(buffer_k),
    fit_start              = fit_start,
    fit_end                = fit_end,
    simulate_periods       = simulate_periods,
    burnin_periods         = burnin_periods,
    calibration_periods    = calibration_periods,
    calibration_track_entity_count = calibration_track_entity_count,
    mc_reps                = as.integer(mc_reps),
    mc_reps_dev            = as.integer(mc_reps_dev),
    n_jobs                 = as.integer(n_jobs),
    dev_mode               = isTRUE(dev_mode),
    random_seed            = as.integer(random_seed),
    track_entity_count     = as.integer(track_entity_count),
    max_dense_entities     = as.integer(max_dense_entities),
    max_duplicate_entity_period_rate = max_duplicate_entity_period_rate,
    acf_sample_size        = as.integer(acf_sample_size),
    top_k_pct              = top_k_pct,
    min_top_k              = as.integer(min_top_k),
    min_anchor_bins        = as.integer(min_anchor_bins),
    max_anchor_bins        = as.integer(max_anchor_bins),
    min_anchor_bin_size    = as.integer(min_anchor_bin_size),
    z_rank_clip            = z_rank_clip,
    sigma_obs_bounds       = sigma_obs_bounds,
    tdf_bounds             = tdf_bounds,
    alpha_arch_bounds      = alpha_arch_bounds,
    arch_clip              = arch_clip,
    z_sq_clip              = z_sq_clip,
    jump_prob_floor        = jump_prob_floor,
    alpha_kappa_default    = alpha_kappa_default,
    alpha_kappa_grid       = alpha_kappa_grid,
    kappa_stab_grid        = kappa_stab_grid,
    n_optim_restarts       = as.integer(n_optim_restarts),
    use_obs_noise          = isTRUE(use_obs_noise),
    exit_enabled           = isTRUE(exit_enabled),
    exit_alpha             = exit_alpha,
    exit_incumbent_rate    = exit_incumbent_rate,
    exit_transient_rate    = exit_transient_rate,
    entry_burst_frac       = entry_burst_frac,
    kurtosis_cal_reps      = as.integer(kurtosis_cal_reps),
    kurtosis_overshoot     = kurtosis_overshoot,
    kurtosis_min_signal_frac = kurtosis_min_signal_frac,
    vr_lags                = as.integer(vr_lags),
    acf_lags               = as.integer(acf_lags),
    racf_lags              = as.integer(racf_lags),
    pers_horizons          = as.integer(pers_horizons),
    r2_horizons            = as.integer(r2_horizons),
    vr_threshold           = vr_threshold,
    acf_threshold          = acf_threshold,
    racf_threshold         = racf_threshold,
    pers_threshold_pct     = pers_threshold_pct,
    pers_threshold_min     = as.integer(pers_threshold_min),
    r2_threshold           = r2_threshold,
    zipf_fit_fraction      = zipf_fit_fraction,
    sensitivity_deltas     = sensitivity_deltas,
    max_rank_filter        = max_rank_filter,
    min_presence_frac      = min_presence_frac,
    max_noise_frac         = max_noise_frac,
    min_perm_frac          = min_perm_frac,
    output_dir             = output_dir,
    skip_plots             = isTRUE(skip_plots)
  )
  class(cfg) <- "rankdiff_config"
  cfg
}

#' Get resolved MC reps (respects dev_mode)
#' @param cfg A \code{rankdiff_config}.
#' @return Integer number of MC reps.
#' @export
resolved_mc_reps <- function(cfg) {
  if (isTRUE(cfg$dev_mode)) cfg$mc_reps_dev else cfg$mc_reps
}

#' Modify a config, returning a new copy
#' @param cfg A \code{rankdiff_config}.
#' @param ... Named fields to override.
#' @return A new \code{rankdiff_config} with the overrides applied.
#' @keywords internal
update_config <- function(cfg, ...) {
  updates <- list(...)
  for (nm in names(updates)) {
    cfg[[nm]] <- updates[[nm]]
  }
  class(cfg) <- "rankdiff_config"
  cfg
}

#' @export
print.rankdiff_config <- function(x, ...) {
  cat("rankdiff_config\n")
  cat("  data_path:", x$data_path, "\n")
  cat("  platform:", x$platform, "\n")
  cat("  cadence:", x$cadence, "\n")
  cat("  mc_reps:", x$mc_reps, " (dev:", x$mc_reps_dev, ")\n")
  cat("  dev_mode:", x$dev_mode, "\n")
  invisible(x)
}

# ---- Threshold model ----

#' Create a threshold model
#' @param threshold_by_period Numeric vector of per-period thresholds.
#' @param max_missing_value_by_period Numeric vector of max-missing values.
#' @param effectively_exact_above_threshold Logical.
#' @return A list of class \code{rankdiff_threshold}.
#' @keywords internal
new_threshold_model <- function(threshold_by_period,
                                max_missing_value_by_period,
                                effectively_exact_above_threshold = TRUE) {
  structure(
    list(
      threshold_by_period = as.numeric(threshold_by_period),
      max_missing_value_by_period = as.numeric(max_missing_value_by_period),
      effectively_exact_above_threshold = effectively_exact_above_threshold
    ),
    class = "rankdiff_threshold"
  )
}

# ---- SimFeatures (ablation control) ----

#' Create simulation feature flags for ablation control
#'
#' @param burn_in Enable burn-in period.
#' @param kappa Enable mean reversion.
#' @param rank_dep_kappa Enable rank-dependent kappa.
#' @param kappa_stab Enable kappa stabilisation factor.
#' @param heavy_tails Enable heavy tails (student-t + jumps).
#' @param arch Enable ARCH(1) volatility clustering.
#' @param obs_noise Enable observation noise.
#' @param exit_entry Enable entry/exit process.
#' @param calibrated_tdf Use calibrated t_df curve.
#' @return A list of class \code{rankdiff_features}.
#' @export
sim_features <- function(burn_in       = TRUE,
                         kappa         = TRUE,
                         rank_dep_kappa = TRUE,
                         kappa_stab    = TRUE,
                         heavy_tails   = TRUE,
                         arch          = TRUE,
                         obs_noise     = TRUE,
                         exit_entry    = TRUE,
                         calibrated_tdf = TRUE) {
  structure(
    list(
      burn_in        = isTRUE(burn_in),
      kappa          = isTRUE(kappa),
      rank_dep_kappa = isTRUE(rank_dep_kappa),
      kappa_stab     = isTRUE(kappa_stab),
      heavy_tails    = isTRUE(heavy_tails),
      arch           = isTRUE(arch),
      obs_noise      = isTRUE(obs_noise),
      exit_entry     = isTRUE(exit_entry),
      calibrated_tdf = isTRUE(calibrated_tdf)
    ),
    class = "rankdiff_features"
  )
}

# ---- Excess kurtosis (avoids extra dependency) ----

#' Fisher excess kurtosis
#' @param x Numeric vector.
#' @return Bias-corrected Fisher excess kurtosis.
#' @keywords internal
excess_kurtosis <- function(x) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n < 4) return(NA_real_)
  centered <- x - mean(x)
  m2 <- mean(centered^2)
  if (m2 < 1e-12) return(NA_real_)
  g2 <- mean(centered^4) / (m2^2) - 3.0
  ((n - 1.0) / ((n - 2.0) * (n - 3.0))) * ((n + 1.0) * g2 + 6.0)
}
