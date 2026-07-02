# ---- Ablation study: incremental feature evaluation ----
# Translates Python ablation.py: ABLATION_LEVELS, run_ablation,
# format_ablation_summary, and supporting helpers.

#' Ablation levels for incremental feature evaluation
#'
#' Returns the default list of 8 ablation levels, each adding one model
#' component on top of the previous level.
#'
#' @return A list of named lists with \code{name}, \code{short}, and
#'   \code{features}.
#' @keywords internal
.default_ablation_levels <- function() {
  list(
    list(
      name  = "1. Base (PT+Gauss)",
      short = "Base",
      features = sim_features(
        burn_in = FALSE, kappa = FALSE, rank_dep_kappa = FALSE,
        kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
        obs_noise = TRUE, exit_entry = FALSE, calibrated_tdf = FALSE
      )
    ),
    list(
      name  = "2. +Burn-in",
      short = "+Burn-in",
      features = sim_features(
        burn_in = TRUE, kappa = FALSE, rank_dep_kappa = FALSE,
        kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
        obs_noise = TRUE, exit_entry = FALSE, calibrated_tdf = FALSE
      )
    ),
    list(
      name  = "3. +kappa (global)",
      short = "+kappa",
      features = sim_features(
        burn_in = TRUE, kappa = TRUE, rank_dep_kappa = FALSE,
        kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
        obs_noise = TRUE, exit_entry = FALSE, calibrated_tdf = FALSE
      )
    ),
    list(
      name  = "4. +kappa(r)",
      short = "+kappa(r)",
      features = sim_features(
        burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
        kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
        obs_noise = TRUE, exit_entry = FALSE, calibrated_tdf = FALSE
      )
    ),
    list(
      name  = "5. +Heavy tails",
      short = "+Tails",
      features = sim_features(
        burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
        kappa_stab = FALSE, heavy_tails = TRUE, arch = FALSE,
        obs_noise = TRUE, exit_entry = FALSE, calibrated_tdf = FALSE
      )
    ),
    list(
      name  = "6. +ARCH(1)",
      short = "+ARCH",
      features = sim_features(
        burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
        kappa_stab = FALSE, heavy_tails = TRUE, arch = TRUE,
        obs_noise = TRUE, exit_entry = FALSE, calibrated_tdf = FALSE
      )
    ),
    list(
      name  = "7. +Calibrated t_df",
      short = "+Cal-tdf",
      features = sim_features(
        burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
        kappa_stab = FALSE, heavy_tails = TRUE, arch = TRUE,
        obs_noise = TRUE, exit_entry = FALSE, calibrated_tdf = TRUE
      )
    ),
    list(
      name  = "8. +kappa-stab (full)",
      short = "Full",
      features = sim_features(
        burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
        kappa_stab = TRUE, heavy_tails = TRUE, arch = TRUE,
        obs_noise = TRUE, exit_entry = TRUE, calibrated_tdf = TRUE
      )
    )
  )
}

#' Diagnostic display names (length 15)
#' @keywords internal
DIAG_NAMES <- c(
  "VR(2)", "VR(4)", "VR(8)", "VR(13)",
  "ACF(1)", "ACF(2)",
  "RACF(1)", "RACF(4)", "RACF(13)",
  "Pers(1)", "Pers(4)", "Pers(13)",
  "R2(1)", "R2(4)", "R2(13)"
)

#' Diagnostic keys matching DIAG_NAMES (length 15)
#' @keywords internal
DIAG_KEYS <- c(
  "vr2", "vr4", "vr8", "vr13",
  "acf1", "acf2",
  "racf1", "racf4", "racf13",
  "pers1", "pers4", "pers13",
  "xr2_1", "xr2_4", "xr2_13"
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Check whether a single diagnostic passes its threshold
#'
#' @param key Character diagnostic key (e.g. \code{"vr4"}, \code{"racf1"}).
#' @param sim_val Numeric simulated value (MC mean).
#' @param emp_val Numeric empirical value.
#' @param cfg A \code{rankdiff_config}.
#' @param top_k Integer top-k size for persistence tolerance.
#' @return Logical scalar: \code{TRUE} if the diagnostic passes.
#' @keywords internal
.diag_passes <- function(key, sim_val, emp_val, cfg, top_k = 100L) {
  if (!is.finite(sim_val)) return(FALSE)

  if (startsWith(key, "vr")) {
    return(abs(sim_val - emp_val) / max(abs(emp_val), 1e-6) < cfg$vr_threshold)
  } else if (startsWith(key, "acf")) {
    return(abs(sim_val - emp_val) < cfg$acf_threshold)
  } else if (startsWith(key, "racf")) {
    return(abs(sim_val - emp_val) < cfg$racf_threshold)
  } else if (startsWith(key, "pers")) {
    pers_tol <- max(cfg$pers_threshold_min,
                    as.integer(round(cfg$pers_threshold_pct * top_k)))
    return(abs(sim_val - emp_val) <= pers_tol)
  } else if (startsWith(key, "xr2")) {
    return(abs(sim_val - emp_val) < cfg$r2_threshold)
  } else {
    return(abs(sim_val - emp_val) < cfg$acf_threshold)
  }
}

#' Extract empirical values keyed by DIAG_KEYS
#'
#' @param emp Named list of empirical targets (as returned by
#'   \code{compute_empirical_targets}).
#' @return Named numeric vector with one entry per DIAG_KEY.
#' @keywords internal
.get_emp_values <- function(emp) {
  vals <- numeric(0)

  for (k in c(2L, 4L, 8L, 13L)) {
    key <- paste0("vr", k)
    v <- emp$vr_emp[[as.character(k)]]
    vals[[key]] <- if (is.null(v)) NA_real_ else as.numeric(v)
  }
  for (k in c(1L, 2L)) {
    key <- paste0("acf", k)
    v <- emp$acf_emp[[as.character(k)]]
    vals[[key]] <- if (is.null(v)) NA_real_ else as.numeric(v)
  }
  for (k in c(1L, 4L, 13L)) {
    key <- paste0("racf", k)
    v <- emp$racf_emp[[as.character(k)]]
    vals[[key]] <- if (is.null(v)) NA_real_ else as.numeric(v)
  }
  for (k in c(1L, 4L, 13L)) {
    key <- paste0("pers", k)
    v <- emp$pers_emp[[as.character(k)]]
    vals[[key]] <- if (is.null(v)) NA_real_ else as.numeric(v)
  }
  for (k in c(1L, 4L, 13L)) {
    key <- paste0("xr2_", k)
    v <- emp$xr2_emp[[as.character(k)]]
    vals[[key]] <- if (is.null(v)) NA_real_ else as.numeric(v)
  }
  vals
}

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

#' Run an ablation study across incremental model levels
#'
#' For each ablation level, runs the Monte Carlo simulation with the
#' corresponding feature flags, computes MC means for each diagnostic, and
#' determines pass/fail status.
#'
#' @param params Estimated parameters list (class \code{rankdiff_params} or
#'   plain list).
#' @param bundle A \code{rankdiff_bundle} produced by
#'   \code{\link{build_data_bundle}}.
#' @param cfg A \code{rankdiff_config}.
#' @param levels Optional list of ablation levels (defaults to
#'   \code{ABLATION_LEVELS}).
#' @return A list of result lists, one per level.  Each result contains:
#'   \describe{
#'     \item{level}{The ablation level definition.}
#'     \item{mc_means}{Named list of MC mean diagnostics.}
#'     \item{pass_fail}{Named logical list of pass/fail per diagnostic.}
#'     \item{n_pass}{Integer count of passing diagnostics.}
#'     \item{n_total}{Integer total diagnostics.}
#'   }
#' @export
run_ablation <- function(params, bundle, cfg, levels = NULL) {
  if (is.null(levels)) levels <- .default_ablation_levels()

  emp      <- bundle$empirical
  emp_vals <- .get_emp_values(emp)
  top_k    <- as.integer(emp$top_k %||% 100L)
  results  <- list()

  for (lvl in levels) {
    features  <- lvl$features
    sims      <- simulate_many(params, bundle, cfg, features = features)
    sim_diags <- lapply(sims, function(sim) sim$diagnostics)

    # Compute MC means for each diagnostic key
    mc_means <- list()
    for (key in DIAG_KEYS) {
      vals <- vapply(sim_diags, function(d) {
        v <- d[[key]]
        if (is.null(v)) NA_real_ else as.numeric(v)
      }, numeric(1))
      vals <- vals[is.finite(vals)]
      mc_means[[key]] <- if (length(vals) > 0L) mean(vals) else NA_real_
    }

    # Extra diagnostics (kurtosis, xsec_var_drift)
    for (extra in c("kurtosis", "xsec_var_drift")) {
      vals <- vapply(sim_diags, function(d) {
        v <- d[[extra]]
        if (is.null(v)) NA_real_ else as.numeric(v)
      }, numeric(1))
      vals <- vals[is.finite(vals)]
      mc_means[[extra]] <- if (length(vals) > 0L) mean(vals) else NA_real_
    }

    # Pass/fail for each diagnostic
    pass_fail <- list()
    for (key in DIAG_KEYS) {
      ev <- emp_vals[[key]]
      if (is.null(ev)) ev <- NA_real_
      sv <- mc_means[[key]]
      if (is.null(sv)) sv <- NA_real_
      pass_fail[[key]] <- .diag_passes(key, sv, ev, cfg, top_k = top_k)
    }

    n_pass <- sum(unlist(pass_fail))

    results[[length(results) + 1L]] <- list(
      level     = lvl,
      mc_means  = mc_means,
      pass_fail = pass_fail,
      n_pass    = as.integer(n_pass),
      n_total   = length(DIAG_KEYS)
    )
  }

  results
}

#' Format ablation results as a text summary table
#'
#' Produces a human-readable text table showing pass/fail status for each
#' diagnostic at each ablation level, plus a feature contribution summary.
#'
#' @param results A list of ablation results as returned by
#'   \code{\link{run_ablation}}.
#' @return A single character string containing the formatted table.
#' @export
format_ablation_summary <- function(results) {
  lines <- character(0)

  # Header
  hdr <- sprintf("%-24s %5s", "Level", "Score")
  for (dn in DIAG_NAMES) {
    hdr <- paste0(hdr, sprintf(" %7s", dn))
  }
  hdr <- paste0(hdr, sprintf(" %6s %6s", "Kurt", "VarDr"))
  lines <- c(lines, hdr)
  lines <- c(lines, strrep("-", nchar(hdr)))

  # Rows

  for (res in results) {
    mc <- res$mc_means
    pf <- res$pass_fail
    row <- sprintf("%-24s %2d/%2d", res$level$short, res$n_pass, res$n_total)
    for (key in DIAG_KEYS) {
      mark <- if (isTRUE(pf[[key]])) "  Y" else " *N"
      row <- paste0(row, sprintf(" %7s", mark))
    }
    kurt  <- mc[["kurtosis"]]     %||% NA_real_
    drift <- mc[["xsec_var_drift"]] %||% NA_real_
    row <- paste0(row, sprintf(" %6.1f %6.2f", kurt, drift))
    lines <- c(lines, row)
  }

  # Feature contribution
  lines <- c(lines, "")
  lines <- c(lines, "Feature Contribution:")
  if (length(results) > 1L) {
    for (i in seq(2L, length(results))) {
      prev_pf <- results[[i - 1L]]$pass_fail
      curr_pf <- results[[i]]$pass_fail
      newly_passing <- character(0)
      newly_failing <- character(0)
      for (j in seq_along(DIAG_KEYS)) {
        k <- DIAG_KEYS[j]
        if (isTRUE(curr_pf[[k]]) && !isTRUE(prev_pf[[k]])) {
          newly_passing <- c(newly_passing, DIAG_NAMES[j])
        }
        if (!isTRUE(curr_pf[[k]]) && isTRUE(prev_pf[[k]])) {
          newly_failing <- c(newly_failing, DIAG_NAMES[j])
        }
      }
      delta <- results[[i]]$n_pass - results[[i - 1L]]$n_pass
      fixed_str <- if (length(newly_passing) > 0L) {
        paste(newly_passing, collapse = ", ")
      } else {
        "(none)"
      }
      broke_str <- if (length(newly_failing) > 0L) {
        paste(newly_failing, collapse = ", ")
      } else {
        "(none)"
      }
      lines <- c(lines, sprintf("  %s", results[[i]]$level$name))
      lines <- c(lines, sprintf("    Fixed: %s  |  Broke: %s  |  Delta: %+d",
                                 fixed_str, broke_str, delta))
    }
  }

  paste(lines, collapse = "\n")
}
