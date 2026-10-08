# ---- Diagnostics: empirical targets, simulation diagnostics, scoring ----
# Translates Python diagnostics.py to R for the rankdiffR package.

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Safe lag-k autocorrelation
#'
#' Computes the Pearson correlation between \code{x[1:(n-lag)]} and
#' \code{x[(lag+1):n]}.
#'
#' @param x Numeric vector.
#' @param lag Integer lag.
#' @return Scalar correlation or \code{NA_real_} if the series is too short or
#'   has near-zero variance.
#' @keywords internal
.safe_autocorr <- function(x, lag) {
  n <- length(x)
  if (n <= lag + 5L) return(NA_real_)
  a <- x[1:(n - lag)]
  b <- x[(lag + 1):n]
  if (sd(a, na.rm = TRUE) < 1e-12 || sd(b, na.rm = TRUE) < 1e-12) {
    return(NA_real_)
  }
  cor(a, b, use = "complete.obs")
}

#' Pivot panel to wide matrix
#'
#' Subsets \code{panel} to rows whose \code{entity_id} is in \code{ids},
#' then pivots to a matrix with rows = period_index, columns = entity_id.
#'
#' @param panel Data frame with columns \code{entity_id}, \code{period_index},
#'   and \code{value_col}.
#' @param ids Character vector of entity IDs to include.
#' @param value_col Character name of the value column to pivot.
#' @return A numeric matrix (periods x entities), or a 0-row, 0-column matrix
#'   if \code{ids} is empty.
#' @keywords internal
.make_wide <- function(panel, ids, value_col) {
  if (length(ids) == 0L) return(matrix(numeric(0), nrow = 0, ncol = 0))
  subset_df <- panel[panel$entity_id %in% ids, , drop = FALSE]
  if (nrow(subset_df) == 0L) return(matrix(numeric(0), nrow = 0, ncol = 0))

  # Build a matrix: rows = sorted unique period_index, cols = ids (in order)

  period_idx <- sort(unique(subset_df$period_index))
  n_periods <- length(period_idx)
  n_ids <- length(ids)
  mat <- matrix(NA_real_, nrow = n_periods, ncol = n_ids)
  colnames(mat) <- ids
  rownames(mat) <- as.character(period_idx)

  # Map period_index to row position
  period_map <- setNames(seq_along(period_idx), as.character(period_idx))
  # Map entity_id to column position

  id_map <- setNames(seq_along(ids), ids)

  rows <- period_map[as.character(subset_df$period_index)]
  cols <- id_map[as.character(subset_df$entity_id)]
  keep <- !is.na(rows) & !is.na(cols)
  mat[cbind(rows[keep], cols[keep])] <- subset_df[[value_col]][keep]
  mat
}

# ---------------------------------------------------------------------------
# compute_empirical_targets
# ---------------------------------------------------------------------------

#' Compute empirical diagnostic targets from panel data
#'
#' Calculates variance ratios, autocorrelations (log-metric and rank),
#' persistence, cross-sectional R-squared, Zipf slope, kurtosis, exit rates,
#' and other summary statistics from the observed panel. These targets are
#' compared against Monte Carlo simulations in \code{\link{score_diagnostics}}.
#'
#' @param panel Data frame with columns \code{entity_id}, \code{period_index},
#'   \code{metric_value}, \code{rank}, and \code{local_slope}.
#' @param balanced_ids Character vector of entity IDs observed in every period.
#' @param tracked_ids Character vector of entity IDs selected for tracking.
#' @param threshold A \code{rankdiff_threshold} object (see
#'   \code{\link{new_threshold_model}}).
#' @param cfg A \code{rankdiff_config} object (see \code{\link{create_config}}).
#' @return A named list of empirical targets, including:
#' \describe{
#'   \item{counts_by_period}{Integer vector of entity counts per period.}
#'   \item{top_k}{Integer: number of top entities used for persistence tests.}
#'   \item{tracked_balanced_ids}{Character vector: tracked IDs that are also balanced.}
#'   \item{metric_wide}{Numeric matrix of metric values (periods x entities).}
#'   \item{rank_wide}{Numeric matrix of ranks (periods x entities).}
#'   \item{log_metric}{Numeric matrix: log1p of metric_wide.}
#'   \item{log_changes}{Numeric matrix: first differences of log_metric.}
#'   \item{var_1}{Numeric vector: per-entity variance of log_changes.}
#'   \item{vr_emp}{Named list of empirical variance ratios.}
#'   \item{acf_emp}{Named list of empirical log-metric ACFs.}
#'   \item{racf_emp}{Named list of empirical rank ACFs.}
#'   \item{pers_emp}{Named list of empirical persistence counts.}
#'   \item{xr2_emp}{Named list of empirical cross-sectional R-squared.}
#'   \item{zipf_slope}{Numeric: Zipf slope from period 0.}
#'   \item{emp_kurt}{Numeric: excess kurtosis of all log-changes.}
#'   \item{emp_mean_var}{Numeric: mean of per-entity variances.}
#'   \item{emp_median_var}{Numeric: median of per-entity variances.}
#'   \item{xsec_var_emp}{Numeric: mean cross-sectional variance of log-metric.}
#'   \item{w0_sorted}{Numeric vector: sorted (descending) log1p metric at period 0.}
#'   \item{mean_rank}{Numeric vector: per-entity mean rank (sorted).}
#'   \item{z_rank_mean}{Numeric vector: log-rank coordinate of mean_rank.}
#'   \item{local_slope_mean}{Numeric vector: per-entity mean local_slope.}
#'   \item{threshold_by_period}{Numeric vector from threshold model.}
#'   \item{window_turnover_n}{Integer or NULL: window size for turnover.}
#'   \item{window_turnover_rate}{Numeric: mean turnover rate.}
#'   \item{window_turnover_count}{Numeric: mean turnover count.}
#'   \item{mean_exit_count}{Numeric: mean per-period exit count.}
#'   \item{mean_exit_rate}{Numeric: mean per-period exit rate.}
#' }
#' @export
compute_empirical_targets <- function(panel, balanced_ids, tracked_ids,
                                      threshold, cfg) {
  # Restrict tracked IDs to those that are also balanced

  balanced_set <- unique(balanced_ids)
  tracked_balanced <- tracked_ids[tracked_ids %in% balanced_set]

  metric_wide <- .make_wide(panel, tracked_balanced, "metric_value")
  rank_wide   <- .make_wide(panel, tracked_balanced, "rank")

  n_periods <- nrow(metric_wide)
  n_entities <- ncol(metric_wide)

  # Log-metric and first differences
  if (n_periods > 0 && n_entities > 0) {
    log_metric  <- log1p(metric_wide)
    log_changes <- diff(log_metric)                 # (n_periods-1) x n_entities
    var_1       <- apply(log_changes, 2, var, na.rm = TRUE)
  } else {
    log_metric  <- matrix(numeric(0), nrow = 0, ncol = 0)
    log_changes <- matrix(numeric(0), nrow = 0, ncol = 0)
    var_1       <- numeric(0)
  }


  # --- Variance ratios ---
  vr_emp <- list()
  if (n_periods > 0 && n_entities > 0 && length(var_1) > 0) {
    for (k in cfg$vr_lags) {
      if (k < n_periods) {
        # k-period differences of log_metric
        diff_k <- log_metric[(k + 1):n_periods, , drop = FALSE] -
                  log_metric[1:(n_periods - k), , drop = FALSE]
        numer <- apply(diff_k, 2, var, na.rm = TRUE)
        ratios <- numer / (k * var_1)
        ratios <- ratios[is.finite(ratios)]
        vr_emp[[as.character(k)]] <- if (length(ratios) > 0L) median(ratios) else NA_real_
      }
    }
  }

  # --- ACF of log-changes ---
  acf_emp <- list()
  n_sample <- if (ncol(log_changes) > 0) min(cfg$acf_sample_size, ncol(log_changes)) else 0L
  for (lag in cfg$acf_lags) {
    vals <- vapply(seq_len(n_sample), function(i) {
      col <- log_changes[, i]
      col <- col[!is.na(col)]
      .safe_autocorr(col, lag)
    }, numeric(1))
    vals <- vals[is.finite(vals)]
    acf_emp[[as.character(lag)]] <- if (length(vals) > 0) median(vals) else 0.0
  }

  # --- RACF (rank autocorrelation) ---
  racf_emp <- list()
  n_rank_sample <- if (ncol(rank_wide) > 0) min(cfg$acf_sample_size, ncol(rank_wide)) else 0L
  for (lag in cfg$racf_lags) {
    vals <- vapply(seq_len(n_rank_sample), function(i) {
      col <- rank_wide[, i]
      col <- col[!is.na(col)]
      .safe_autocorr(col, lag)
    }, numeric(1))
    vals <- vals[is.finite(vals)]
    racf_emp[[as.character(lag)]] <- if (length(vals) > 0) median(vals) else 0.0
  }

  # --- Entity counts per period ---
  counts_by_period <- tapply(panel$entity_id, panel$period_index, function(x) length(unique(x)))
  counts_by_period <- counts_by_period[order(as.integer(names(counts_by_period)))]
  n_total_periods <- length(counts_by_period)
  mean_n <- mean(counts_by_period)
  top_k <- max(cfg$min_top_k, as.integer(round(cfg$top_k_pct * mean_n)))

  # --- Window turnover (topk_buffered mode) ---
  window_turnover_n     <- NULL
  window_turnover_rate  <- NA_real_
  window_turnover_count <- NA_real_

  if (cfg$universe_mode == "topk_buffered" &&
      !is.null(cfg$top_k_focus) && cfg$buffer_k > 0L) {
    window_turnover_n <- as.integer(min(round(mean_n),
                                        cfg$top_k_focus + cfg$buffer_k))
    if (window_turnover_n > 0L) {
      top_window <- panel[panel$rank <= window_turnover_n,
                          c("period_index", "entity_id"), drop = FALSE]
      tw_split <- split(as.character(top_window$entity_id),
                        top_window$period_index)
      period_sets <- lapply(tw_split, function(x) unique(x))

      turnover_counts <- numeric(0)
      turnover_rates  <- numeric(0)
      for (pidx in seq_len(n_total_periods - 1L) - 1L) {
        ids_now  <- period_sets[[as.character(pidx)]]
        ids_next <- period_sets[[as.character(pidx + 1L)]]
        if (is.null(ids_now) || is.null(ids_next)) next
        base <- min(window_turnover_n, length(ids_now), length(ids_next))
        if (base <= 0L) next
        overlap  <- length(intersect(ids_now, ids_next))
        turnover <- max(base - overlap, 0L)
        turnover_counts <- c(turnover_counts, turnover)
        turnover_rates  <- c(turnover_rates, turnover / base)
      }
      if (length(turnover_rates) > 0) {
        window_turnover_rate  <- mean(turnover_rates)
        window_turnover_count <- mean(turnover_counts)
      }
    }
  }

  # --- Exit counts ---
  period_entity_sets <- vector("list", n_total_periods)
  for (pidx in seq_len(n_total_periods) - 1L) {
    period_entity_sets[[pidx + 1L]] <-
      unique(panel$entity_id[panel$period_index == pidx])
  }
  exit_counts <- integer(0)
  for (pidx in seq_len(n_total_periods - 1L)) {
    ids_now  <- period_entity_sets[[pidx]]
    ids_next <- period_entity_sets[[pidx + 1L]]
    exit_counts <- c(exit_counts, length(setdiff(ids_now, ids_next)))
  }
  mean_exit_count <- if (length(exit_counts) > 0) mean(exit_counts) else 0.0
  mean_exit_rate  <- mean_exit_count / max(mean_n, 1.0)

  # --- Persistence & R-squared ---
  pers_emp <- list()
  xr2_emp  <- list()
  if (nrow(panel) > 0) {
    period0 <- panel[panel$period_index == 0, , drop = FALSE]
    period0 <- period0[order(period0$rank), , drop = FALSE]
    t0_ids  <- unique(period0$entity_id[period0$rank <= top_k])

    for (k in cfg$pers_horizons) {
      if (k >= n_total_periods) next
      tk <- panel[panel$period_index == k, , drop = FALSE]
      tk <- tk[order(tk$rank), , drop = FALSE]
      tk_ids <- unique(tk$entity_id[tk$rank <= top_k])
      pers_emp[[as.character(k)]] <- length(intersect(t0_ids, tk_ids))
    }

    for (k in cfg$r2_horizons) {
      if (k >= nrow(log_metric)) next
      start_vals <- log_metric[1, ]
      end_vals   <- log_metric[k + 1, ]           # +1 because R is 1-indexed
      valid <- is.finite(start_vals) & is.finite(end_vals)
      if (sum(valid) > 5) {
        r <- cor(start_vals[valid], end_vals[valid])
        xr2_emp[[as.character(k)]] <- r^2
      } else {
        xr2_emp[[as.character(k)]] <- NA_real_
      }
    }
  }

  # --- Zipf slope ---
  period0 <- panel[panel$period_index == 0, , drop = FALSE]
  period0 <- period0[order(period0$rank), , drop = FALSE]
  zipf_n <- max(10L, as.integer(round(cfg$zipf_fit_fraction * nrow(period0))))
  zipf_subset <- period0[seq_len(min(zipf_n, nrow(period0))), , drop = FALSE]
  zipf_mask <- zipf_subset$metric_value > 0
  if (sum(zipf_mask, na.rm = TRUE) > 5) {
    zipf_slope <- coef(lm(log(metric_value) ~ log(rank),
                          data = zipf_subset[zipf_mask, , drop = FALSE]))[2]
    zipf_slope <- unname(zipf_slope)
  } else {
    zipf_slope <- NA_real_
  }

  # --- Kurtosis, variances ---
  if (length(log_changes) > 0) {
    all_changes <- as.vector(log_changes)
    all_changes <- all_changes[is.finite(all_changes)]
  } else {
    all_changes <- numeric(0)
  }
  emp_kurt       <- if (length(all_changes) > 20) excess_kurtosis(all_changes) else NA_real_
  emp_mean_var   <- if (length(var_1) > 0 && any(is.finite(var_1))) mean(var_1, na.rm = TRUE) else NA_real_
  emp_median_var <- if (length(var_1) > 0 && any(is.finite(var_1))) median(var_1, na.rm = TRUE) else NA_real_

  # --- Cross-sectional variance of log-metric (mean across periods) ---
  panel_log <- log1p(panel$metric_value)
  xsec_vars <- tapply(panel_log, panel$period_index, var, na.rm = TRUE)
  xsec_var_emp <- mean(xsec_vars, na.rm = TRUE)

  # --- Mean rank and z_rank ---
  if (ncol(rank_wide) > 0) {
    mean_rank_vec <- colMeans(rank_wide, na.rm = TRUE)
    ord <- order(mean_rank_vec)
    mean_rank_vec <- mean_rank_vec[ord]
    z_rank_mean <- log(pmax(pmin((mean_rank_vec - 0.5) / mean_n, 1.0),
                            cfg$z_rank_clip))
  } else {
    mean_rank_vec <- numeric(0)
    z_rank_mean   <- numeric(0)
  }

  # --- Local-slope mean per entity (matched to mean_rank order) ---
  if (length(mean_rank_vec) > 0) {
    tb_panel <- panel[panel$entity_id %in% tracked_balanced, , drop = FALSE]
    ls_means <- tapply(tb_panel$local_slope, tb_panel$entity_id, mean,
                       na.rm = TRUE)
    # Reindex to the same order as mean_rank_vec
    local_slope_mean <- as.numeric(ls_means[names(mean_rank_vec)])
  } else {
    local_slope_mean <- numeric(0)
  }

  # --- w0_sorted: sorted (descending) log1p(metric_value) at period 0 ---
  w0_sorted <- sort(log1p(period0$metric_value), decreasing = TRUE)

  # --- Return ---
  list(
    counts_by_period      = as.integer(counts_by_period),
    top_k                 = top_k,
    tracked_balanced_ids  = tracked_balanced,
    metric_wide           = metric_wide,
    rank_wide             = rank_wide,
    log_metric            = log_metric,
    log_changes           = log_changes,
    var_1                 = var_1,
    vr_emp                = vr_emp,
    acf_emp               = acf_emp,
    racf_emp              = racf_emp,
    pers_emp              = pers_emp,
    xr2_emp               = xr2_emp,
    zipf_slope            = zipf_slope,
    emp_kurt              = emp_kurt,
    emp_mean_var          = emp_mean_var,
    emp_median_var        = emp_median_var,
    xsec_var_emp          = xsec_var_emp,
    w0_sorted             = w0_sorted,
    mean_rank             = mean_rank_vec,
    z_rank_mean           = z_rank_mean,
    local_slope_mean      = local_slope_mean,
    threshold_by_period   = threshold$threshold_by_period,
    window_turnover_n     = window_turnover_n,
    window_turnover_rate  = window_turnover_rate,
    window_turnover_count = window_turnover_count,
    mean_exit_count       = mean_exit_count,
    mean_exit_rate        = mean_exit_rate
  )
}

# ---------------------------------------------------------------------------
# compute_sim_diagnostics
# ---------------------------------------------------------------------------

#' Compute diagnostics from a single simulation run
#'
#' Extracts variance ratios, ACFs, rank ACFs, persistence, cross-sectional
#' R-squared, kurtosis, Zipf slope, and cross-sectional variance diagnostics
#' from the output of \code{\link{simulate_one}}.
#'
#' @param sim A named list as returned by \code{\link{simulate_one}}, with
#'   elements \code{tracked_values} (matrix: T x n_tracked),
#'   \code{tracked_ranks} (matrix: T x n_tracked), \code{top_ids} (matrix:
#'   T x top_k), \code{period0_sorted_values} (numeric vector),
#'   \code{xsec_var} (numeric vector, length T), and
#'   \code{observed_counts} (numeric vector, length T).
#' @param cfg A \code{rankdiff_config}.
#' @return A named list of scalar diagnostics (named like \code{"vr2"},
#'   \code{"acf1"}, \code{"racf1"}, \code{"pers1"}, \code{"xr2_1"},
#'   \code{"kurtosis"}, \code{"zipf_slope"}, \code{"xsec_var_start"}, etc.).
#' @export
compute_sim_diagnostics <- function(sim, cfg) {
  values <- as.matrix(sim$tracked_values)
  ranks  <- as.matrix(sim$tracked_ranks)

  # Restrict to fully-observed entities if enough are available
  observed_all <- apply(values, 2, function(col) all(is.finite(col)))
  if (sum(observed_all) >= 10L) {
    values <- values[, observed_all, drop = FALSE]
    ranks  <- ranks[, observed_all, drop = FALSE]
  }

  n_t <- nrow(values)
  n_e <- ncol(values)

  changes <- diff(values)                           # (n_t - 1) x n_e
  var_1   <- apply(changes, 2, var, na.rm = TRUE)   # per-entity variance

  diag <- list()

  # --- Variance ratios ---
  for (k in cfg$vr_lags) {
    if (k < n_t) {
      diff_k <- values[(k + 1):n_t, , drop = FALSE] -
                values[1:(n_t - k), , drop = FALSE]
      numer <- apply(diff_k, 2, var, na.rm = TRUE)
      valid <- is.finite(var_1) & (var_1 > 1e-12) & is.finite(numer)
      if (any(valid)) {
        diag[[paste0("vr", k)]] <- median(numer[valid] / (k * var_1[valid]),
                                           na.rm = TRUE)
      }
    }
  }

  # --- ACF of log-changes ---
  n_acf <- min(n_e, cfg$acf_sample_size)
  for (lag in cfg$acf_lags) {
    vals <- vapply(seq_len(n_acf), function(i) {
      col <- changes[, i]
      col <- col[is.finite(col)]
      .safe_autocorr(col, lag)
    }, numeric(1))
    vals <- vals[is.finite(vals)]
    diag[[paste0("acf", lag)]] <- if (length(vals) > 0) median(vals) else NA_real_
  }

  # --- RACF ---
  n_racf <- min(n_e, cfg$acf_sample_size)
  for (lag in cfg$racf_lags) {
    vals <- vapply(seq_len(n_racf), function(i) {
      col <- ranks[, i]
      col <- col[col > 0]
      .safe_autocorr(col, lag)
    }, numeric(1))
    vals <- vals[is.finite(vals)]
    diag[[paste0("racf", lag)]] <- if (length(vals) > 0) median(vals) else NA_real_
  }

  # --- Persistence ---
  top_ids <- as.matrix(sim$top_ids)
  for (k in cfg$pers_horizons) {
    if (k < nrow(top_ids)) {
      t0_set <- setdiff(top_ids[1, ], -1L)
      tk_set <- setdiff(top_ids[k + 1, ], -1L)     # +1: R is 1-indexed
      diag[[paste0("pers", k)]] <- length(intersect(t0_set, tk_set))
    }
  }

  # --- Cross-sectional R-squared ---
  for (k in cfg$r2_horizons) {
    if (k < n_t) {
      start_vals <- values[1, ]
      end_vals   <- values[k + 1, ]                 # +1: R is 1-indexed
      valid <- is.finite(start_vals) & is.finite(end_vals)
      if (sum(valid) > 5) {
        r <- cor(start_vals[valid], end_vals[valid])
        diag[[paste0("xr2_", k)]] <- r^2
      } else {
        diag[[paste0("xr2_", k)]] <- NA_real_
      }
    }
  }

  # --- Kurtosis ---
  flat_changes <- as.vector(changes)
  flat_changes <- flat_changes[is.finite(flat_changes)]
  diag[["kurtosis"]] <- if (length(flat_changes) > 20) {
    excess_kurtosis(flat_changes)
  } else {
    NA_real_
  }

  # --- Zipf slope ---
  first_sorted <- as.numeric(sim$period0_sorted_values)
  zipf_n <- max(10L, as.integer(round(cfg$zipf_fit_fraction * length(first_sorted))))
  zipf_vals <- first_sorted[seq_len(min(zipf_n, length(first_sorted)))]
  if (length(zipf_vals) > 5) {
    log_ranks <- log(seq_along(zipf_vals))
    fit <- lm(zipf_vals ~ log_ranks)
    diag[["zipf_slope"]] <- unname(coef(fit)[2])
  } else {
    diag[["zipf_slope"]] <- NA_real_
  }

  # --- Cross-sectional variance ---
  xsec_var <- as.numeric(sim$xsec_var)
  diag[["xsec_var_start"]] <- xsec_var[1]
  diag[["xsec_var_end"]]   <- xsec_var[length(xsec_var)]
  diag[["xsec_var_drift"]] <- xsec_var[length(xsec_var)] /
                               max(xsec_var[1], 1e-8)

  # --- Mean observed N ---
  diag[["mean_observed_n"]] <- mean(as.numeric(sim$observed_counts))

  diag
}

# ---------------------------------------------------------------------------
# score_diagnostics
# ---------------------------------------------------------------------------

#' Score simulation diagnostics against empirical targets
#'
#' Aggregates Monte Carlo replicate diagnostics, computes mean/std/lo/hi
#' statistics for each diagnostic key, and applies per-diagnostic pass/fail
#' tests.
#'
#' @param emp Named list of empirical targets as returned by
#'   \code{\link{compute_empirical_targets}}.
#' @param sim_diags A list of diagnostic lists, each as returned by
#'   \code{\link{compute_sim_diagnostics}}.
#' @param cfg A \code{rankdiff_config}.
#' @return A named list with elements:
#' \describe{
#'   \item{mc_stats}{Named list of lists, one per diagnostic key, each with
#'     elements \code{mean}, \code{std}, \code{lo}, \code{hi}.}
#'   \item{tests}{Named logical list of pass/fail results.}
#'   \item{n_pass}{Integer: number of passing tests.}
#'   \item{n_total}{Integer: total number of tests applied.}
#'   \item{pers_tolerance}{Integer: persistence tolerance used.}
#' }
#' @export
score_diagnostics <- function(emp, sim_diags, cfg) {
  # Collect all keys across MC replicates
  all_keys <- sort(unique(unlist(lapply(sim_diags, names))))

  mc_stats <- list()
  for (key in all_keys) {
    vals <- vapply(sim_diags, function(d) {
      v <- d[[key]]
      if (is.null(v)) NA_real_ else as.numeric(v)
    }, numeric(1))
    vals <- vals[is.finite(vals)]
    if (length(vals) == 0L) next
    mc_stats[[key]] <- list(
      mean = mean(vals),
      std  = sd(vals),
      lo   = as.numeric(quantile(vals, 0.025)),
      hi   = as.numeric(quantile(vals, 0.975))
    )
  }

  tests <- list()

  # Variance ratio tests
  for (lag in cfg$vr_lags) {
    key <- paste0("vr", lag)
    lag_ch <- as.character(lag)
    if (!is.null(mc_stats[[key]]) && !is.null(emp$vr_emp[[lag_ch]])) {
      emp_val <- emp$vr_emp[[lag_ch]]
      if (is.finite(emp_val) && abs(emp_val) > 0) {
        tests[[paste0("VR(", lag, ")")]] <-
          abs(mc_stats[[key]]$mean - emp_val) / abs(emp_val) < cfg$vr_threshold
      }
    }
  }

  # ACF tests
  for (lag in cfg$acf_lags) {
    key <- paste0("acf", lag)
    lag_ch <- as.character(lag)
    if (!is.null(mc_stats[[key]]) && !is.null(emp$acf_emp[[lag_ch]])) {
      tests[[paste0("ACF(", lag, ")")]] <-
        abs(mc_stats[[key]]$mean - emp$acf_emp[[lag_ch]]) < cfg$acf_threshold
    }
  }

  # RACF tests
  for (lag in cfg$racf_lags) {
    key <- paste0("racf", lag)
    lag_ch <- as.character(lag)
    if (!is.null(mc_stats[[key]]) && !is.null(emp$racf_emp[[lag_ch]])) {
      tests[[paste0("RACF(", lag, ")")]] <-
        abs(mc_stats[[key]]$mean - emp$racf_emp[[lag_ch]]) < cfg$racf_threshold
    }
  }

  # Persistence tests
  pers_tol <- max(cfg$pers_threshold_min,
                  as.integer(round(cfg$pers_threshold_pct * emp$top_k)))
  for (horizon in cfg$pers_horizons) {
    key <- paste0("pers", horizon)
    h_ch <- as.character(horizon)
    if (!is.null(mc_stats[[key]]) && !is.null(emp$pers_emp[[h_ch]])) {
      tests[[paste0("Pers(", horizon, ")")]] <-
        abs(mc_stats[[key]]$mean - emp$pers_emp[[h_ch]]) <= pers_tol
    }
  }

  # R-squared tests
  for (horizon in cfg$r2_horizons) {
    key <- paste0("xr2_", horizon)
    h_ch <- as.character(horizon)
    if (!is.null(mc_stats[[key]]) && !is.null(emp$xr2_emp[[h_ch]])) {
      emp_val <- emp$xr2_emp[[h_ch]]
      if (is.finite(emp_val)) {
        tests[[paste0("R2(", horizon, ")")]] <-
          abs(mc_stats[[key]]$mean - emp_val) < cfg$r2_threshold
      }
    }
  }

  n_pass  <- sum(unlist(tests))
  n_total <- length(tests)

  list(
    mc_stats       = mc_stats,
    tests          = tests,
    n_pass         = as.integer(n_pass),
    n_total        = as.integer(n_total),
    pers_tolerance = pers_tol
  )
}
