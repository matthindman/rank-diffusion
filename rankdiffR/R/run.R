# ---- Main entry point: full pipeline orchestration ----
# Translates Python model_v43.py main() to R for the rankdiffR package.

#' Fit a rank-diffusion model end-to-end
#'
#' Orchestrates the complete fitting pipeline:
#' \enumerate{
#'   \item Build data bundle (loading, preprocessing, empirical targets).
#'   \item Estimate initial parameters (moment-based PT decomposition).
#'   \item Fit parameter curves (smooth rank-dependent curves from anchors).
#'   \item Calibrate alpha_kappa (rank-dependent mean reversion exponent).
#'   \item Calibrate kappa_stab (mean-reversion stabilisation factor).
#'   \item Calibrate kurtosis (t-df curve adjustment).
#'   \item Run Monte Carlo validation simulation.
#'   \item Score diagnostics against empirical targets.
#'   \item Save fit result to disk.
#'   \item Optionally: plot diagnostics, run ablation study, run sensitivity
#'     analysis.
#' }
#'
#' @param cfg A \code{rankdiff_config} created by \code{\link{create_config}}.
#' @return A list of class \code{rankdiff_result} with elements:
#'   \describe{
#'     \item{config}{The \code{rankdiff_config} used.}
#'     \item{data}{The \code{rankdiff_bundle} (panel, empirical targets, etc.).}
#'     \item{initial}{Initial parameter estimates (\code{rankdiff_initial}).}
#'     \item{params}{Final estimated parameters (list).}
#'     \item{diagnostics}{Score list from \code{score_diagnostics}.}
#'     \item{ablation}{Ablation results (if run), or \code{NULL}.}
#'     \item{sensitivity}{Sensitivity results (if run), or \code{NULL}.}
#'   }
#' @export
rankdiff_fit <- function(cfg) {

  # ===========================================================================
  # Step 1: Build data bundle
  # ===========================================================================
  message(strrep("=", 70))
  message("BUILDING DATA BUNDLE")
  message(strrep("=", 70))
  bundle <- build_data_bundle(cfg)
  message("  Platform: ", bundle$platform)
  message("  Periods: ", bundle$n_periods, ", Entities: ", bundle$n_entities)
  message("  Mean N: ", round(bundle$mean_n), ", Balanced: ", length(bundle$balanced_ids))
  message("  Exit rate: ", format(bundle$empirical$mean_exit_rate %||% 0, digits = 4))

  # ===========================================================================
  # Step 2: Estimate initial parameters
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("ESTIMATING INITIAL PARAMETERS")
  message(strrep("=", 70))
  initial <- estimate_initial_params(bundle, cfg)
  message("  sigma_obs: ", format(initial$sigma_obs, digits = 4))
  message("  sigma_het: ", format(initial$sigma_het, digits = 4))
  message("  t_df_global: ", format(initial$t_df_global, digits = 2, nsmall = 2))
  message("  kappa_base_raw: ", format(initial$kappa_base_raw, digits = 6))
  message("  jump_prob: ", format(initial$jump_prob, digits = 4))
  message("  alpha_arch: ", format(initial$alpha_arch, digits = 4))

  # ===========================================================================
  # Step 3: Fit parameter curves
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("FITTING PARAMETER CURVES")
  message(strrep("=", 70))
  params <- fit_parameter_curves(bundle, initial, cfg)
  message("  z_knots: ", paste(round(params$z_knots, 3), collapse = ", "))
  message("  sigma_eta_curve: ", paste(round(params$sigma_eta_curve, 4), collapse = ", "))
  message("  kappa_curve: ", paste(round(params$kappa_curve, 6), collapse = ", "))
  message("  exit_p_base: ", format(params$exit_p_base, digits = 6))

  # ===========================================================================
  # Step 4: Calibrate alpha_kappa
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("CALIBRATING alpha_kappa")
  message(strrep("=", 70))
  params <- estimate_alpha_kappa(params, bundle, cfg)
  message("  alpha_kappa: ", format(params$alpha_kappa, digits = 3))

  # ===========================================================================
  # Step 5: Calibrate kappa_stab
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("CALIBRATING kappa_stab")
  message(strrep("=", 70))
  params <- calibrate_kappa_stab(params, bundle, cfg)
  message("  kappa_stab_factor: ", format(params$kappa_stab_factor, digits = 3))

  # ===========================================================================
  # Step 6: Calibrate kurtosis
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("CALIBRATING KURTOSIS")
  message(strrep("=", 70))
  params <- calibrate_kurtosis(params, bundle, cfg)
  message("  t_df_curve (post-cal): ",
          paste(round(params$t_df_curve, 2), collapse = ", "))

  # ===========================================================================
  # Step 7: Main MC simulation
  # ===========================================================================
  mc_reps <- resolved_mc_reps(cfg)
  message("\n", strrep("=", 70))
  message("RUNNING MC SIMULATION (", mc_reps, " reps)")
  message(strrep("=", 70))
  sims <- simulate_many(params, bundle, cfg)

  # ===========================================================================
  # Step 8: Score diagnostics
  # ===========================================================================
  sim_diags <- lapply(sims, function(sim) sim$diagnostics)
  score <- score_diagnostics(bundle$empirical, sim_diags, cfg)

  # ===========================================================================
  # Step 9: Save fit result
  # ===========================================================================
  result <- list(
    config      = cfg,
    data        = bundle,
    initial     = initial,
    params      = params,
    diagnostics = score,
    ablation    = NULL,
    sensitivity = NULL
  )
  class(result) <- "rankdiff_result"

  artifact_dir <- save_fit_result(
    result,
    out_dir = cfg$output_dir %||% file.path("output", "rankdiff", bundle$platform)
  )
  message("\n  Score: ", score$n_pass, "/", score$n_total)
  message("  Artifacts: ", artifact_dir)

  # ===========================================================================
  # Step 10: Diagnostic plots
  # ===========================================================================
  if (!isTRUE(cfg$skip_plots)) {
    tryCatch({
      plot_core_diagnostics(bundle, score, artifact_dir, bundle$platform)
    }, error = function(e) {
      message("  Warning: diagnostic plot failed: ", conditionMessage(e))
    })
  }

  # ===========================================================================
  # Scorecard
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("DIAGNOSTIC SCORECARD")
  message(strrep("=", 70))
  tests <- score$tests
  mc_stats <- score$mc_stats
  for (name in names(tests)) {
    mark <- if (isTRUE(tests[[name]])) "PASS" else "FAIL"
    # Derive mc_stats key from test name
    key <- tolower(name)
    key <- gsub("[()]", "", key)
    key <- gsub("r2", "xr2_", key)
    sim_val <- if (!is.null(mc_stats[[key]])) mc_stats[[key]]$mean else NA_real_
    if (is.finite(sim_val)) {
      message(sprintf("  [%4s] %-10s  sim=%.3f", mark, name, sim_val))
    } else {
      message(sprintf("  [%4s] %-10s", mark, name))
    }
  }
  message(sprintf("\n  Total: %d/%d", score$n_pass, score$n_total))

  # ===========================================================================
  # Ablation study
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("ABLATION STUDY")
  message(strrep("=", 70))
  tryCatch({
    abl_results <- run_ablation(params, bundle, cfg)
    message(format_ablation_summary(abl_results))
    result$ablation <- abl_results

    if (!isTRUE(cfg$skip_plots)) {
      tryCatch({
        plot_ablation(abl_results, artifact_dir, bundle$platform)
      }, error = function(e) {
        message("  Warning: ablation plot failed: ", conditionMessage(e))
      })
    }
  }, error = function(e) {
    message("  Warning: ablation study failed: ", conditionMessage(e))
  })

  # ===========================================================================
  # Sensitivity analysis
  # ===========================================================================
  message("\n", strrep("=", 70))
  message("SENSITIVITY ANALYSIS")
  message(strrep("=", 70))
  tryCatch({
    sens_results <- run_sensitivity(params, bundle, cfg)
    message(format_sensitivity_summary(sens_results, params, cfg$sensitivity_deltas))
    result$sensitivity <- sens_results

    if (!isTRUE(cfg$skip_plots)) {
      tryCatch({
        plot_sensitivity(sens_results, cfg$sensitivity_deltas,
                         artifact_dir, bundle$platform)
      }, error = function(e) {
        message("  Warning: sensitivity plot failed: ", conditionMessage(e))
      })
    }
  }, error = function(e) {
    message("  Warning: sensitivity analysis failed: ", conditionMessage(e))
  })

  # ===========================================================================
  # Summary
  # ===========================================================================
  summary_info <- list(
    platform          = bundle$platform,
    cadence           = bundle$cadence,
    n_pass            = score$n_pass,
    n_total           = score$n_total,
    artifact_dir      = artifact_dir,
    top_k             = bundle$empirical$top_k,
    mean_n            = bundle$mean_n,
    kappa_stab_factor = params$kappa_stab_factor,
    alpha_kappa       = params$alpha_kappa,
    exit_p_base       = params$exit_p_base
  )
  message("\n", jsonlite::toJSON(summary_info, auto_unbox = TRUE, pretty = TRUE))

  # Re-assign class after modifications

  class(result) <- "rankdiff_result"
  invisible(result)
}

#' Print method for rankdiff_result
#'
#' Displays a concise summary of the fit result including the diagnostic score,
#' platform, and key parameters.
#'
#' @param x A \code{rankdiff_result} object.
#' @param ... Additional arguments (ignored).
#' @return \code{x}, invisibly.
#' @export
print.rankdiff_result <- function(x, ...) {
  score <- x$diagnostics
  cat("rankdiff_result\n")
  cat("  Platform:     ", x$data$platform %||% "unknown", "\n")
  cat("  Cadence:      ", x$data$cadence %||% "unknown", "\n")
  cat("  Score:        ", score$n_pass, "/", score$n_total, "\n")
  cat("  Periods:      ", x$data$n_periods, "\n")
  cat("  Entities:     ", x$data$n_entities, "\n")
  cat("  Mean N:       ", round(x$data$mean_n), "\n")

  if (!is.null(x$params)) {
    cat("  sigma_obs:    ", format(x$params$sigma_obs, digits = 4), "\n")
    cat("  alpha_kappa:  ", format(x$params$alpha_kappa, digits = 3), "\n")
    cat("  kappa_stab:   ", format(x$params$kappa_stab_factor, digits = 3), "\n")
  }

  # Test details
  tests <- score$tests
  if (length(tests) > 0L) {
    cat("\n  Diagnostics:\n")
    for (name in names(tests)) {
      mark <- if (isTRUE(tests[[name]])) "PASS" else "FAIL"
      cat(sprintf("    [%4s] %s\n", mark, name))
    }
  }

  invisible(x)
}
