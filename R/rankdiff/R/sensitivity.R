# ---- Sensitivity analysis: parameter perturbation study ----
# Translates Python sensitivity.py: SENSITIVITY_PARAMS, run_sensitivity,
# format_sensitivity_summary, and _perturb_params.

#' Default sensitivity parameters
#'
#' A list of two-element character vectors, each giving
#' \code{c(display_name, attribute_name)} for a parameter to perturb.
#'
#' @format A list of length-2 character vectors.
#' @keywords internal
SENSITIVITY_PARAMS <- list(
  c("sigma_obs",   "sigma_obs"),
  c("sigma_het",   "sigma_het"),
  c("kappa_base",  "kappa_base_raw"),
  c("alpha_kappa", "alpha_kappa"),
  c("alpha_arch",  "alpha_arch"),
  c("t_df_global", "t_df_global")
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Perturb a single parameter by a multiplicative delta
#'
#' Scales the specified parameter by \code{(1 + delta)}.  For
#' \code{kappa_base_raw} and \code{alpha_kappa}, also updates the derived
#' \code{kappa_curve}.
#'
#' @param params Estimated parameters list.
#' @param attr Character name of the parameter field to perturb.
#' @param delta Numeric perturbation fraction (e.g. \code{0.10} for +10\%).
#' @return A modified copy of \code{params}.
#' @keywords internal
#' @noRd
.perturb_params <- function(params, attr, delta) {
  base_val <- params[[attr]]
  new_val  <- base_val * (1.0 + delta)

  # Make a shallow copy
  p <- params
  # Ensure list (not reference) semantics
  p <- as.list(p)
  class(p) <- class(params)

  if (attr == "kappa_base_raw") {
    scale_factor <- new_val / max(base_val, 1e-12)
    p$kappa_base_raw <- new_val
    p$kappa_curve <- params$kappa_curve * scale_factor
  } else if (attr == "alpha_kappa") {
    p$alpha_kappa <- new_val
    p$kappa_curve <- params$kappa_base_raw * params$kappa_stab_factor *
                     exp(new_val * params$z_knots)
  } else {
    p[[attr]] <- new_val
  }

  p
}

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

#' Run sensitivity analysis across parameter perturbations
#'
#' For each parameter and each delta perturbation, creates a perturbed copy of
#' the estimated parameters, runs the Monte Carlo simulation, and computes
#' pass/fail diagnostics.
#'
#' @param params Estimated parameters list.
#' @param bundle A \code{rankdiff_bundle} produced by
#'   \code{\link{build_data_bundle}}.
#' @param cfg A \code{rankdiff_config}.
#' @param param_list Optional list of \code{c(display_name, attr_name)} vectors
#'   (defaults to \code{SENSITIVITY_PARAMS}).
#' @param deltas Optional numeric vector of perturbation fractions (defaults to
#'   \code{cfg$sensitivity_deltas}).
#' @return A named list (keyed by display name) of named lists (keyed by delta),
#'   each containing:
#'   \describe{
#'     \item{value}{Perturbed parameter value.}
#'     \item{mc_means}{Named list of MC mean diagnostics.}
#'     \item{pass_fail}{Named logical list.}
#'     \item{n_pass}{Integer.}
#'     \item{n_total}{Integer.}
#'   }
#' @export
run_sensitivity <- function(params, bundle, cfg,
                            param_list = NULL, deltas = NULL) {
  if (is.null(param_list)) param_list <- SENSITIVITY_PARAMS
  if (is.null(deltas))     deltas     <- cfg$sensitivity_deltas

  emp      <- bundle$empirical
  emp_vals <- .get_emp_values(emp)
  top_k    <- as.integer(emp$top_k %||% 100L)

  all_results <- list()

  for (pair in param_list) {
    pname    <- pair[1]
    attr     <- pair[2]
    base_val <- params[[attr]]

    delta_results <- list()

    for (delta in deltas) {
      perturbed <- .perturb_params(params, attr, delta)
      sims      <- simulate_many(perturbed, bundle, cfg)
      sim_diags <- lapply(sims, function(sim) sim$diagnostics)

      # MC means
      mc_means <- list()
      for (key in DIAG_KEYS) {
        vals <- vapply(sim_diags, function(d) {
          v <- d[[key]]
          if (is.null(v)) NA_real_ else as.numeric(v)
        }, numeric(1))
        vals <- vals[is.finite(vals)]
        mc_means[[key]] <- if (length(vals) > 0L) mean(vals) else NA_real_
      }

      # Pass/fail
      pass_fail <- list()
      for (key in DIAG_KEYS) {
        ev <- emp_vals[[key]]
        if (is.null(ev)) ev <- NA_real_
        sv <- mc_means[[key]]
        if (is.null(sv)) sv <- NA_real_
        pass_fail[[key]] <- .diag_passes(key, sv, ev, cfg, top_k = top_k)
      }
      n_pass <- sum(unlist(pass_fail))

      delta_results[[as.character(delta)]] <- list(
        value     = base_val * (1.0 + delta),
        mc_means  = mc_means,
        pass_fail = pass_fail,
        n_pass    = as.integer(n_pass),
        n_total   = length(DIAG_KEYS)
      )
    }

    all_results[[pname]] <- delta_results
  }

  all_results
}

#' Format sensitivity analysis results as a text summary
#'
#' Produces a human-readable text table showing the score at each perturbation
#' level for each parameter, plus a list of diagnostics failing at the extreme
#' perturbations.
#'
#' @param results A nested list as returned by \code{\link{run_sensitivity}}.
#' @param params Estimated parameters list (used for display of base values).
#' @param deltas Numeric vector of perturbation fractions.
#' @return A single character string containing the formatted summary.
#' @export
format_sensitivity_summary <- function(results, params,
                                       deltas = c(-0.20, -0.10, 0.0, 0.10, 0.20)) {
  lines <- character(0)

  # Header
  hdr <- sprintf("%-14s", "Parameter")
  for (d in deltas) {
    hdr <- paste0(hdr, sprintf(" %+5.0f%%", d * 100))
  }
  lines <- c(lines, hdr)
  lines <- c(lines, strrep("-", nchar(hdr)))

  # Rows
  for (pname in names(results)) {
    row <- sprintf("%-14s", pname)
    for (d in deltas) {
      entry <- results[[pname]][[as.character(d)]]
      if (is.null(entry)) {
        row <- paste0(row, "   -- ")
      } else {
        sc     <- entry$n_pass
        marker <- if (sc < entry$n_total) "*" else " "
        row <- paste0(row, sprintf("  %2d%s ", sc, marker))
      }
    }
    lines <- c(lines, row)
  }

  # Failing diagnostics at extreme perturbations
  lines <- c(lines, "")
  lines <- c(lines, "Diagnostics failing at +/-20% perturbation:")
  for (pname in names(results)) {
    fails_m <- character(0)
    fails_p <- character(0)

    entry_m <- results[[pname]][[as.character(-0.20)]]
    entry_p <- results[[pname]][[as.character(0.20)]]

    if (!is.null(entry_m)) {
      for (j in seq_along(DIAG_KEYS)) {
        if (!isTRUE(entry_m$pass_fail[[DIAG_KEYS[j]]])) {
          fails_m <- c(fails_m, DIAG_NAMES[j])
        }
      }
    }
    if (!is.null(entry_p)) {
      for (j in seq_along(DIAG_KEYS)) {
        if (!isTRUE(entry_p$pass_fail[[DIAG_KEYS[j]]])) {
          fails_p <- c(fails_p, DIAG_NAMES[j])
        }
      }
    }

    all_fails <- sort(unique(c(fails_m, fails_p)))
    fail_str <- if (length(all_fails) > 0L) {
      paste(all_fails, collapse = ", ")
    } else {
      "(robust to +/-20%)"
    }
    lines <- c(lines, sprintf("  %-14s: %s", pname, fail_str))
  }

  paste(lines, collapse = "\n")
}
