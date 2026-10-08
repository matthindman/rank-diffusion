# ---- Initializers: moment-based parameter estimation ----
# Translates Python initializers.py: build_anchor_bins, _fit_band_params,
# estimate_initial_params, and supporting helpers.

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Adaptive bin count from mean panel width
#'
#' Computes the number of log-spaced anchor bins as a function of the average
#' number of entities per period, clipped to \code{[cfg$min_anchor_bins,
#' cfg$max_anchor_bins]}.
#'
#' @param mean_n Mean number of entities per period (numeric scalar).
#' @param cfg A \code{rankdiff_config}.
#' @return Integer bin count.
#' @keywords internal
.adaptive_bin_count <- function(mean_n, cfg) {

  raw <- round(log2(max(mean_n, 512)) - 5)
  as.integer(min(max(raw, cfg$min_anchor_bins), cfg$max_anchor_bins))
}

#' Model-implied variance ratio at lag k
#'
#' Computes the variance ratio VR(k) for a permanent-transitory model with
#' AR(1) transitory component and observation noise.
#'
#' @param k Integer lag.
#' @param se2 Permanent innovation variance (sigma_eta^2).
#' @param phi AR(1) coefficient for the transitory component.
#' @param sn2 Transitory innovation variance (sigma_nu^2).
#' @param sobs2 Observation noise variance (sigma_obs^2).
#' @return Numeric scalar variance ratio.
#' @keywords internal
.model_vr <- function(k, se2, phi, sn2, sobs2) {
  sc2 <- if (abs(phi) < 0.999) sn2 / (1.0 - phi^2) else sn2 * 1000.0
  vd <- se2 + 2.0 * sc2 * (1.0 - phi) + 2.0 * sobs2
  if (vd <= 0) return(1.0)
  vk <- k * se2 + 2.0 * sc2 * (1.0 - phi^k) + 2.0 * sobs2
  vk / (k * vd)
}

#' Model-implied first-order autocorrelation
#'
#' Computes the ACF(1) of log-changes for a permanent-transitory model with
#' AR(1) transitory component and observation noise.
#'
#' @param se2 Permanent innovation variance.
#' @param phi AR(1) coefficient.
#' @param sn2 Transitory innovation variance.
#' @param sobs2 Observation noise variance.
#' @return Numeric scalar ACF(1).
#' @keywords internal
.model_acf1 <- function(se2, phi, sn2, sobs2) {
  sc2 <- if (abs(phi) < 0.999) sn2 / (1.0 - phi^2) else sn2 * 1000.0
  vd <- se2 + 2.0 * sc2 * (1.0 - phi) + 2.0 * sobs2
  if (vd <= 0) return(0.0)
  (-sc2 * (1.0 - phi)^2 - sobs2) / vd
}

#' Fit band PT parameters via Nelder-Mead
#'
#' Estimates per-band permanent innovation variance (\code{sigma_eta^2}), AR(1)
#' coefficient (\code{phi}), and transitory innovation variance
#' (\code{sigma_nu^2}) by matching empirical total variance, VR(4), ACF(1), and
#' optionally VR(13).
#'
#' @param emp_var Empirical total variance of log-changes for this band.
#' @param emp_vr4 Empirical variance ratio at lag 4.
#' @param emp_acf1 Empirical ACF(1) of log-changes.
#' @param emp_vr13 Empirical variance ratio at lag 13 (may be NaN).
#' @param sobs2 Observation noise variance.
#' @param cfg A \code{rankdiff_config}.
#' @return Numeric vector of length 3: \code{c(sigma_eta^2, phi, sigma_nu^2)}.
#' @keywords internal
.fit_band_params <- function(emp_var, emp_vr4, emp_acf1, emp_vr13, sobs2, cfg) {

  objective <- function(p) {
    se2 <- exp(p[1])
    phi <- 0.95 / (1.0 + exp(-p[2]))
    sn2 <- exp(p[3])
    sc2 <- if (abs(phi) < 0.999) sn2 / (1.0 - phi^2) else sn2 * 1000.0
    mvar <- se2 + 2.0 * sc2 * (1.0 - phi) + 2.0 * sobs2
    loss <- 10.0 * (log(mvar) - log(emp_var))^2
    loss <- loss + 5.0 * (.model_vr(4L, se2, phi, sn2, sobs2) - emp_vr4)^2
    loss <- loss + 3.0 * (.model_acf1(se2, phi, sn2, sobs2) - emp_acf1)^2
    if (is.finite(emp_vr13)) {
      loss <- loss + 2.0 * (.model_vr(13L, se2, phi, sn2, sobs2) - emp_vr13)^2
    }
    loss
  }

  n_restarts <- if (cfg$dev_mode) min(cfg$n_optim_restarts, 50L) else cfg$n_optim_restarts

  best_val <- Inf
  best_par <- NULL

  .with_local_seed(cfg$random_seed, {
    for (i in seq_len(n_restarts)) {
      x0 <- c(runif(1, -9, -1), runif(1, -2, 2), runif(1, -4, 1))
      result <- tryCatch(
        optim(
          x0,
          objective,
          method = "Nelder-Mead",
          control = list(maxit = 5000L, reltol = 1e-10)
        ),
        error = function(e) NULL
      )
      if (!is.null(result) && result$value < best_val) {
        best_val <- result$value
        best_par <- result$par
      }
    }
  })

  if (is.null(best_par)) {
    stop("Band parameter optimisation failed after ", n_restarts, " restarts.")
  }

  se2 <- exp(best_par[1])
  phi <- 0.95 / (1.0 + exp(-best_par[2]))
  sn2 <- exp(best_par[3])
  c(se2 = se2, phi = phi, sn2 = sn2)
}

#' Fit a centred Student-t distribution via profile MLE
#'
#' Given approximately centered residuals \code{x}, profiles over the scale
#' parameter to estimate the degrees of freedom and scale of a zero-location
#' Student-t distribution.
#'
#' @param x Numeric vector of standardised observations.
#' @param bounds Length-2 numeric vector giving the search interval for df.
#' @return A named list with components \code{df} and \code{scale}.
#' @keywords internal
.fit_t_distribution <- function(x, bounds = c(3, 200)) {
  x <- x[is.finite(x)]
  if (length(x) < 2L) {
    return(list(df = mean(bounds), scale = 1.0))
  }

  scale_upper <- max(sd(x), sqrt(mean(x^2)), 1e-6) * 10.0
  scale_upper <- max(scale_upper, 1.0)

  profile_scale <- function(df) {
    optimize(
      function(scale) {
        if (!is.finite(scale) || scale <= 0) {
          return(Inf)
        }
        -sum(dt(x / scale, df = df, log = TRUE) - log(scale))
      },
      interval = c(1e-6, scale_upper)
    )
  }

  df_opt <- optimize(
    function(df) profile_scale(df)$objective,
    interval = bounds
  )
  scale_opt <- profile_scale(df_opt$minimum)
  list(df = df_opt$minimum, scale = scale_opt$minimum)
}

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

#' Build log-spaced anchor bins from empirical moments
#'
#' Partitions tracked balanced entities into log-spaced rank bands and computes
#' per-band summary statistics (total variance, variance ratios, ACF(1),
#' kurtosis, local slope) used as fitting targets for the PT decomposition.
#'
#' @param bundle A \code{rankdiff_bundle} list (see \code{build_data_bundle}).
#' @param cfg A \code{rankdiff_config}.
#' @return A data frame with one row per retained anchor bin and columns:
#'   \code{rank_lo}, \code{rank_hi}, \code{rank_mid}, \code{z_center},
#'   \code{n_entities}, \code{total_var}, \code{vr4}, \code{vr13},
#'   \code{acf1}, \code{local_slope}, \code{kurtosis}.
#' @export
build_anchor_bins <- function(bundle, cfg) {
  emp <- bundle$empirical
  mean_rank <- as.numeric(emp$mean_rank)
  if (length(mean_rank) == 0L) {
    stop("No balanced tracked entities available for anchor construction.")
  }

  n_bins <- .adaptive_bin_count(bundle$mean_n, cfg)
  edges <- exp(seq(log(1.0), log(bundle$mean_n + 1.0), length.out = n_bins + 1L)) - 1.0
  rank_lo <- pmax(1L, as.integer(floor(edges[-length(edges)])))
  rank_hi <- pmax(rank_lo, as.integer(ceiling(edges[-1L])))

  # Empirical wide-format objects (matrices or data frames)
  log_metric  <- emp$log_metric   # matrix: rows = periods, cols = entities
  log_changes <- emp$log_changes  # matrix: rows = periods, cols = entities
  rank_wide   <- emp$rank_wide    # matrix: rows = periods, cols = entities
  local_slope_mean <- as.numeric(emp$local_slope_mean)

  # Column names from rank_wide (entity IDs)
  col_names <- colnames(rank_wide)

  rows <- list()
  for (b in seq_along(rank_lo)) {
    lo <- rank_lo[b]
    hi <- rank_hi[b]
    mask <- (mean_rank >= lo) & (mean_rank <= hi)
    n_ent <- sum(mask)
    if (n_ent < cfg$min_anchor_bin_size) next

    cols_idx <- which(mask)
    cols <- col_names[cols_idx]

    # Extract sub-matrices for this band
    ch <- log_changes[, cols_idx, drop = FALSE]
    lv <- log_metric[, cols_idx, drop = FALSE]

    # Total variance: median of per-entity variance of log-changes
    entity_vars <- apply(ch, 2, function(x) var(x[is.finite(x)], na.rm = TRUE))
    total_var <- median(entity_vars, na.rm = TRUE)

    # Variance ratio at lag 4
    n_rows <- nrow(lv)
    if (n_rows > 4L) {
      diff4 <- lv[(4L + 1L):n_rows, , drop = FALSE] - lv[1L:(n_rows - 4L), , drop = FALSE]
      var_diff4 <- apply(diff4, 2, function(x) var(x[is.finite(x)], na.rm = TRUE))
      vr4_vals <- var_diff4 / (4.0 * entity_vars)
      vr4 <- median(vr4_vals, na.rm = TRUE)
    } else {
      vr4 <- NaN
    }

    # Variance ratio at lag 13
    if (n_rows > 13L) {
      diff13 <- lv[(13L + 1L):n_rows, , drop = FALSE] - lv[1L:(n_rows - 13L), , drop = FALSE]
      var_diff13 <- apply(diff13, 2, function(x) var(x[is.finite(x)], na.rm = TRUE))
      vr13_vals <- var_diff13 / (13.0 * entity_vars)
      vr13 <- median(vr13_vals, na.rm = TRUE)
    } else {
      vr13 <- NaN
    }

    # ACF(1): sample-based per-entity lag-1 autocorrelation
    n_acf_sample <- min(cfg$acf_sample_size, length(cols_idx))
    acf1_vals <- numeric(0)
    for (j in seq_len(n_acf_sample)) {
      arr <- ch[, j]
      arr <- arr[is.finite(arr)]
      if (length(arr) > 6L && sd(arr[-length(arr)]) > 1e-12 && sd(arr[-1L]) > 1e-12) {
        acf1_vals <- c(acf1_vals, cor(arr[-length(arr)], arr[-1L]))
      }
    }
    band_acf1 <- if (length(acf1_vals) > 0L) median(acf1_vals, na.rm = TRUE) else 0.0

    # Kurtosis of pooled band log-changes
    band_changes <- as.numeric(ch)
    band_changes <- band_changes[is.finite(band_changes)]
    band_kurt <- if (length(band_changes) > 20L) excess_kurtosis(band_changes) else NaN

    # z-center: median of log(rank_frac) within band
    rank_frac <- pmax((mean_rank[mask] - 0.5) / bundle$mean_n, cfg$z_rank_clip)
    z_center <- median(log(pmin(rank_frac, 1.0)))

    # local slope
    local_slope_val <- if (length(local_slope_mean) > 0L) {
      median(local_slope_mean[mask], na.rm = TRUE)
    } else {
      NaN
    }

    rows[[length(rows) + 1L]] <- data.frame(
      rank_lo     = as.numeric(lo),
      rank_hi     = as.numeric(hi),
      rank_mid    = sqrt(lo * hi),
      z_center    = z_center,
      n_entities  = as.numeric(n_ent),
      total_var   = total_var,
      vr4         = vr4,
      vr13        = vr13,
      acf1        = band_acf1,
      local_slope = local_slope_val,
      kurtosis    = band_kurt,
      stringsAsFactors = FALSE
    )
  }

  if (length(rows) == 0L) {
    stop("Unable to construct anchor bins with enough entities.")
  }
  do.call(rbind, rows)
}

#' Estimate initial parameters from empirical moments
#'
#' Implements the six-stage initialisation pipeline:
#' \enumerate{
#'   \item \strong{sigma_obs} from ACF inversion (lag-2, lag-3 autocorrelations).
#'   \item \strong{sigma_het} from mean/median variance ratio.
#'   \item \strong{t_df_global} and per-band \strong{t_df_anchor} from
#'     standardised residual t-fits.
#'   \item \strong{jump_prob / jump_scale} from tail analysis.
#'   \item \strong{alpha_arch} from squared-residual ACF.
#'   \item \strong{PT decomposition} per anchor band via Nelder-Mead, and
#'     \strong{kappa_base_raw} from cross-sectional variance of initial values.
#' }
#'
#' @param bundle A \code{rankdiff_bundle} list produced by
#'   \code{build_data_bundle}.
#' @param cfg A \code{rankdiff_config}.
#' @return A list of class \code{rankdiff_initial} containing scalar
#'   estimates (\code{sigma_obs}, \code{sigma_het}, \code{alpha_arch},
#'   \code{t_df_global}, \code{jump_prob}, \code{jump_scale},
#'   \code{alpha_kappa}, \code{kappa_base_raw}), vector estimates
#'   (\code{z_knots}, \code{sigma_eta_anchor}, \code{phi_anchor},
#'   \code{sigma_nu_anchor}, \code{t_df_anchor}), a \code{threshold} object,
#'   \code{top_k}, and a \code{metadata} list.
#' @export
estimate_initial_params <- function(bundle, cfg) {
  emp <- bundle$empirical
  anchors <- build_anchor_bins(bundle, cfg)

  log_changes <- emp$log_changes   # matrix: rows = periods, cols = entities
  var_1       <- as.numeric(emp$var_1)
  emp_median_var <- as.numeric(emp$emp_median_var)
  emp_mean_var   <- as.numeric(emp$emp_mean_var)

  col_names   <- colnames(log_changes)
  n_cols      <- ncol(log_changes)
  sample_ncol <- min(cfg$acf_sample_size, n_cols)
  sample_idx  <- seq_len(sample_ncol)

  # ===========================================================================
  # Stage 1: sigma_obs from ACF inversion
  # ===========================================================================
  acf3_lag2 <- numeric(0)
  acf3_lag3 <- numeric(0)
  for (j in sample_idx) {
    arr <- log_changes[, j]
    arr <- arr[is.finite(arr)]
    if (length(arr) > 8L) {
      n_a <- length(arr)
      lag2 <- if (sd(arr[1:(n_a - 2L)]) > 1e-12 && sd(arr[3:n_a]) > 1e-12) {
        cor(arr[1:(n_a - 2L)], arr[3:n_a])
      } else {
        NA_real_
      }
      lag3 <- if (sd(arr[1:(n_a - 3L)]) > 1e-12 && sd(arr[4:n_a]) > 1e-12) {
        cor(arr[1:(n_a - 3L)], arr[4:n_a])
      } else {
        NA_real_
      }
      if (is.finite(lag2) && is.finite(lag3)) {
        acf3_lag2 <- c(acf3_lag2, lag2)
        acf3_lag3 <- c(acf3_lag3, lag3)
      }
    }
  }

  if (length(acf3_lag2) > 0L) {
    acf2_ref <- median(acf3_lag2, na.rm = TRUE)
    acf3_ref <- median(acf3_lag3, na.rm = TRUE)
  } else {
    acf2_ref <- emp$acf_emp[["2"]] %||% 0.0
    acf3_ref <- 0.5 * acf2_ref
  }

  phi_agg <- if (abs(acf2_ref) > 1e-3) acf3_ref / acf2_ref else 0.5

  gamma1 <- (emp$acf_emp[["1"]] %||% 0.0) * emp_median_var
  gamma2 <- (emp$acf_emp[["2"]] %||% 0.0) * emp_median_var

  sigma2_obs_est <- -gamma1 + gamma2 / phi_agg

  # Adaptive upper bound: obs noise cannot absorb more than max_noise_frac of
  # change variance (Stock & Watson 1998, Kamber, Morley & Wong 2018).
  sigma2_obs_upper <- cfg$sigma_obs_bounds[2]^2
  if (cfg$max_noise_frac < 1.0 && emp_median_var > 0) {
    sigma2_obs_adaptive <- cfg$max_noise_frac * emp_median_var / 2.0
    sigma2_obs_upper <- min(sigma2_obs_upper, sigma2_obs_adaptive)
  }
  sigma2_obs <- min(max(sigma2_obs_est, cfg$sigma_obs_bounds[1]^2), sigma2_obs_upper)
  sigma_obs  <- sqrt(sigma2_obs)
  sobs2      <- sigma_obs^2

  # ===========================================================================
  # Stage 2: sigma_het from mean/median var ratio
  # ===========================================================================
  var_ratio <- emp_mean_var / max(emp_median_var, 1e-12)
  sigma_het <- sqrt(max(log(max(var_ratio, 1.0)) / 2.0, 0.0))

  # ===========================================================================
  # Stage 3: t_df_global from standardised residuals
  # ===========================================================================
  standardized <- list()
  for (j in sample_idx) {
    arr <- log_changes[, j]
    arr <- arr[is.finite(arr)]
    if (length(arr) > 10L) {
      mu <- mean(arr)
      s  <- sd(arr)
      if (s > 1e-8) {
        standardized[[length(standardized) + 1L]] <- (arr - mu) / s
      }
    }
  }
  z_within <- if (length(standardized) > 0L) unlist(standardized) else c(0.0, 0.0)

  t_fit <- .fit_t_distribution(z_within, bounds = cfg$tdf_bounds)
  t_df_global <- min(max(t_fit$df, cfg$tdf_bounds[1]), cfg$tdf_bounds[2])
  scale_fit <- t_fit$scale

  obs_noise_var <- 2.0 * sobs2

  # ===========================================================================
  # Stage 3b / 5: per-band t_df and PT decomposition
  # ===========================================================================
  tracked_balanced_ids <- as.character(emp$tracked_balanced_ids)
  id_to_pos <- setNames(seq_along(tracked_balanced_ids), tracked_balanced_ids)

  t_df_anchor      <- numeric(nrow(anchors))
  sigma_eta_anchor <- numeric(nrow(anchors))
  phi_anchor       <- numeric(nrow(anchors))
  sigma_nu_anchor  <- numeric(nrow(anchors))

  for (r in seq_len(nrow(anchors))) {
    row <- anchors[r, ]
    mask <- (emp$mean_rank >= row$rank_lo) & (emp$mean_rank <= row$rank_hi)
    cols <- tracked_balanced_ids[mask]
    cols <- cols[cols %in% names(id_to_pos)]

    if (length(cols) == 0L) {
      t_df_anchor[r]      <- t_df_global
      sigma_eta_anchor[r] <- NaN
      phi_anchor[r]       <- NaN
      sigma_nu_anchor[r]  <- NaN
      next
    }

    # Per-band standardised residuals for t_df
    band_std <- list()
    for (col in cols) {
      col_idx <- id_to_pos[[col]]
      arr <- log_changes[, col_idx]
      arr <- arr[is.finite(arr)]
      if (length(arr) > 10L) {
        mu <- mean(arr)
        s  <- sd(arr)
        if (s > 1e-8) {
          band_std[[length(band_std) + 1L]] <- (arr - mu) / s
        }
      }
    }
    if (length(band_std) > 0L) {
      z_band  <- unlist(band_std)
      df_band <- .fit_t_distribution(z_band, bounds = cfg$tdf_bounds)$df
      df_band <- min(max(df_band, cfg$tdf_bounds[1]), cfg$tdf_bounds[2])
    } else {
      df_band <- t_df_global
    }

    # Signal fraction adjustment: inflate t_df when signal is small
    signal_frac <- max(0.05, 1.0 - obs_noise_var / max(row$total_var, obs_noise_var + 1e-8))
    if (signal_frac < 0.30) {
      df_band <- min(max(df_band / signal_frac, cfg$tdf_bounds[1]), cfg$tdf_bounds[2])
    }
    t_df_anchor[r] <- df_band

    # PT decomposition via Nelder-Mead
    band_fit <- .fit_band_params(row$total_var, row$vr4, row$acf1, row$vr13, sobs2, cfg)
    se2 <- band_fit[["se2"]]
    phi <- band_fit[["phi"]]
    sn2 <- band_fit[["sn2"]]

    # Enforce minimum permanent fraction (Harvey 1989, Stock & Watson 1998)
    signal_var <- max(row$total_var - obs_noise_var, 1e-8)
    se2_floor  <- cfg$min_perm_frac * signal_var
    if (se2 < se2_floor) se2 <- se2_floor

    sigma_eta_anchor[r] <- sqrt(se2)
    phi_anchor[r]       <- phi
    sigma_nu_anchor[r]  <- sqrt(sn2)
  }

  # ===========================================================================
  # Stage 4: jump_prob / jump_scale from tail analysis
  # ===========================================================================
  threshold <- 4.0
  expected_tail <- 2.0 * pt(threshold / max(scale_fit, 1e-8), df = t_df_global, lower.tail = FALSE)
  actual_tail   <- mean(abs(z_within) > threshold * scale_fit)
  jump_prob     <- max(cfg$jump_prob_floor, actual_tail - expected_tail)

  extreme_mask <- abs(z_within) > threshold * scale_fit
  n_extreme    <- sum(extreme_mask)
  n_normal     <- sum(!extreme_mask)
  if (n_extreme > 10L && n_normal > 10L) {
    jump_scale <- sd(z_within[extreme_mask]) / max(sd(z_within[!extreme_mask]), 1e-8)
  } else {
    jump_scale <- 5.0
  }

  # ===========================================================================
  # Stage 4.5: alpha_arch from squared-residual ACF
  # ===========================================================================
  z_sq_acfs <- numeric(0)
  for (j in sample_idx) {
    arr <- log_changes[, j]
    arr <- arr[is.finite(arr)]
    if (length(arr) > 15L) {
      mu <- mean(arr)
      s  <- sd(arr)
      if (s > 1e-8) {
        z_sq <- ((arr - mu) / s)^2
        centered <- z_sq - mean(z_sq)
        denom <- var(z_sq)
        if (denom > 1e-12) {
          n_c <- length(centered)
          acf_val <- sum(centered[-n_c] * centered[-1L]) / ((n_c - 1L) * denom)
          z_sq_acfs <- c(z_sq_acfs, acf_val)
        }
      }
    }
  }
  alpha_arch_raw <- if (length(z_sq_acfs) > 0L) median(z_sq_acfs, na.rm = TRUE) else 0.1
  alpha_arch <- min(max(alpha_arch_raw, cfg$alpha_arch_bounds[1]), cfg$alpha_arch_bounds[2])

  # ===========================================================================
  # Stage 6: kappa_base_raw from cross-sectional variance
  # ===========================================================================
  e_h2 <- exp(2.0 * sigma_het^2)
  jump_var_factor <- (1.0 - jump_prob) + jump_prob * jump_scale^2
  mean_eta2 <- e_h2 * mean(sigma_eta_anchor^2, na.rm = TRUE) * jump_var_factor

  w0_sorted <- as.numeric(emp$w0_sorted)
  init_mean  <- mean(w0_sorted)
  init_dev2  <- (w0_sorted - init_mean)^2
  init_ranks <- seq_along(w0_sorted)
  rank_weight <- (init_ranks / max(length(w0_sorted), 1L))^cfg$alpha_kappa_default
  weighted_dev2 <- mean(rank_weight * init_dev2)
  kappa_base_raw <- max(mean_eta2 / max(2.0 * weighted_dev2, 1e-8), 1e-6)

  # ===========================================================================
  # Anchor signal fractions (metadata)
  # ===========================================================================
  anchor_signal_frac <- vapply(seq_len(nrow(anchors)), function(r) {
    max(0.05, 1.0 - obs_noise_var / max(anchors$total_var[r], obs_noise_var + 1e-8))
  }, numeric(1))

  # ===========================================================================
  # Return
  # ===========================================================================
  result <- list(
    sigma_obs        = sigma_obs,
    sigma_het        = sigma_het,
    alpha_arch       = alpha_arch,
    t_df_global      = t_df_global,
    jump_prob        = jump_prob,
    jump_scale       = jump_scale,
    alpha_kappa      = cfg$alpha_kappa_default,
    kappa_base_raw   = kappa_base_raw,
    z_knots          = as.numeric(anchors$z_center),
    sigma_eta_anchor = sigma_eta_anchor,
    phi_anchor       = phi_anchor,
    sigma_nu_anchor  = sigma_nu_anchor,
    t_df_anchor      = t_df_anchor,
    threshold        = bundle$threshold,
    top_k            = as.integer(emp$top_k),
    metadata         = list(
      phi_agg             = phi_agg,
      sigma2_obs_est      = sigma2_obs_est,
      anchor_table        = anchors,
      anchor_kurtosis     = as.numeric(anchors$kurtosis),
      anchor_signal_frac  = anchor_signal_frac
    )
  )
  class(result) <- "rankdiff_initial"
  result
}

#' @export
print.rankdiff_initial <- function(x, ...) {
  cat("rankdiff_initial\n")
  cat("  sigma_obs:    ", format(x$sigma_obs, digits = 4), "\n")
  cat("  sigma_het:    ", format(x$sigma_het, digits = 4), "\n")
  cat("  alpha_arch:   ", format(x$alpha_arch, digits = 4), "\n")
  cat("  t_df_global:  ", format(x$t_df_global, digits = 2), "\n")
  cat("  jump_prob:    ", format(x$jump_prob, digits = 4), "\n")
  cat("  jump_scale:   ", format(x$jump_scale, digits = 2), "\n")
  cat("  alpha_kappa:  ", format(x$alpha_kappa, digits = 2), "\n")
  cat("  kappa_base:   ", format(x$kappa_base_raw, digits = 6), "\n")
  cat("  anchor bins:  ", length(x$z_knots), "\n")
  cat("  top_k:        ", x$top_k, "\n")
  invisible(x)
}
