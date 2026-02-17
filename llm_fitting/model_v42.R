#!/usr/bin/env Rscript

# Permanent-Transitory Rank Diffusion Model v4.2
# R port of llm_fitting/model_v42.py with idiomatic R structure.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(tibble)
  library(ggplot2)
})

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

print_rule <- function(title = NULL, width = 70L) {
  cat(strrep("=", width), "\n", sep = "")
  if (!is.null(title)) {
    cat(title, "\n", sep = "")
    cat(strrep("=", width), "\n", sep = "")
  }
}

safe_median <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0L) return(NA_real_)
  stats::median(x)
}

safe_sd <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 2L) return(NA_real_)
  stats::sd(x)
}

excess_kurtosis <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 4L) return(NA_real_)
  m <- mean(x)
  s <- stats::sd(x)
  if (!is.finite(s) || s <= 0) return(NA_real_)
  mean(((x - m) / s)^4) - 3
}

skewness_stat <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 3L) return(NA_real_)
  m <- mean(x)
  s <- stats::sd(x)
  if (!is.finite(s) || s <= 0) return(NA_real_)
  mean(((x - m) / s)^3)
}

jb_test <- function(x) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n < 10L) {
    return(list(statistic = NA_real_, p_value = NA_real_, skew = NA_real_, kurt = NA_real_))
  }
  sk <- skewness_stat(x)
  ku <- excess_kurtosis(x)
  stat <- (n / 6) * (sk^2 + 0.25 * ku^2)
  p_val <- 1 - stats::pchisq(stat, df = 2)
  list(statistic = stat, p_value = p_val, skew = sk, kurt = ku)
}

lag_autocor <- function(x, lag_k) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n <= lag_k + 5L) return(NA_real_)
  x1 <- x[seq_len(n - lag_k)]
  x2 <- x[(lag_k + 1L):n]
  if (!is.finite(stats::sd(x1)) || !is.finite(stats::sd(x2))) return(NA_real_)
  if (stats::sd(x1) <= 0 || stats::sd(x2) <= 0) return(NA_real_)
  stats::cor(x1, x2)
}

matrix_diff_lag <- function(mat, lag_k) {
  if (nrow(mat) <= lag_k) {
    return(matrix(numeric(0), nrow = 0L, ncol = ncol(mat)))
  }
  mat[(lag_k + 1L):nrow(mat), , drop = FALSE] - mat[seq_len(nrow(mat) - lag_k), , drop = FALSE]
}

median_vr <- function(level_mat, var1_vec, k) {
  d_k <- matrix_diff_lag(level_mat, k)
  if (nrow(d_k) == 0L) return(NA_real_)
  var_k <- apply(d_k, 2, stats::var, na.rm = TRUE)
  ratios <- var_k / (k * pmax(var1_vec, 1e-12))
  safe_median(ratios)
}

fit_student_t <- function(x, df_start = 8, loc_start = NULL, scale_start = NULL) {
  x <- x[is.finite(x)]
  if (length(x) < 50L) {
    return(list(df = max(3, df_start), loc = mean(x), scale = stats::sd(x)))
  }

  if (is.null(loc_start)) loc_start <- stats::median(x)
  if (is.null(scale_start)) scale_start <- max(stats::mad(x), stats::sd(x), 1e-3)

  nll <- function(par) {
    df <- 2 + exp(par[1])
    loc <- par[2]
    scale <- exp(par[3])
    z <- (x - loc) / scale
    -sum(stats::dt(z, df = df, log = TRUE) - log(scale))
  }

  init <- c(log(max(df_start - 2, 1e-3)), loc_start, log(max(scale_start, 1e-3)))

  fit <- tryCatch(
    stats::optim(
      par = init,
      fn = nll,
      method = "Nelder-Mead",
      control = list(maxit = 8000, reltol = 1e-12)
    ),
    error = function(e) NULL
  )

  if (is.null(fit) || fit$convergence != 0L || !is.finite(fit$value)) {
    return(list(df = max(3, df_start), loc = mean(x), scale = max(stats::sd(x), 1e-3)))
  }

  list(
    df = max(3, 2 + exp(fit$par[1])),
    loc = fit$par[2],
    scale = max(exp(fit$par[3]), 1e-6)
  )
}

band_key <- function(lo, hi) paste0(lo, "_", hi)

clip_num <- function(x, lo, hi) pmin(pmax(x, lo), hi)

hill_estimator <- function(x, k) {
  x <- abs(x[is.finite(x)])
  x <- sort(x, decreasing = TRUE)
  if (k < 2L || k >= length(x)) return(list(alpha = NA_real_, se = NA_real_))
  log_ratios <- log(x[seq_len(k)]) - log(x[k + 1L])
  alpha_hat <- k / sum(log_ratios)
  se_hat <- alpha_hat / sqrt(k)
  list(alpha = alpha_hat, se = se_hat)
}

compute_transition_matrix <- function(rank_matrix, n_quintiles, horizon) {
  t0 <- rank_matrix[1, ]
  tk <- rank_matrix[min(horizon + 1L, nrow(rank_matrix)), ]

  n_total <- length(t0)
  q_size <- n_total / n_quintiles
  q0 <- pmin(pmax(floor((t0 - 1) / q_size) + 1L, 1L), n_quintiles)
  qk <- pmin(pmax(floor((tk - 1) / q_size) + 1L, 1L), n_quintiles)

  trans <- matrix(0, nrow = n_quintiles, ncol = n_quintiles)
  for (i in seq_len(n_total)) {
    trans[q0[i], qk[i]] <- trans[q0[i], qk[i]] + 1
  }

  rs <- rowSums(trans)
  rs[rs == 0] <- 1
  trans / rs
}

mc_summary <- function(values) {
  values <- values[is.finite(values)]
  if (length(values) == 0L) {
    return(list(mean = NA_real_, std = NA_real_, lo = NA_real_, hi = NA_real_, median = NA_real_))
  }
  list(
    mean = mean(values),
    std = stats::sd(values),
    lo = as.numeric(stats::quantile(values, 0.025, na.rm = TRUE)),
    hi = as.numeric(stats::quantile(values, 0.975, na.rm = TRUE)),
    median = stats::median(values)
  )
}

as_row_tibble <- function(x) {
  tibble::as_tibble_row(x)
}

get_script_dir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- "--file="
  hit <- grep(file_arg, args)
  if (length(hit) > 0L) {
    return(normalizePath(dirname(sub(file_arg, "", args[hit[1]])), mustWork = TRUE))
  }
  normalizePath(getwd(), mustWork = TRUE)
}

# -----------------------------------------------------------------------------
# Main pipeline
# -----------------------------------------------------------------------------

main <- function() {
  t_start <- Sys.time()

  base_dir <- get_script_dir()
  data_path <- file.path(base_dir, "..", "data", "raw", "fb_ranked_weekly_cutdown.parquet")

  print_rule("LOADING DATA")

  df <- arrow::read_parquet(data_path) %>%
    as_tibble() %>%
    mutate(date = as.Date(date))

  dates <- sort(unique(df$date))
  n_weeks <- length(dates)

  ep_counts <- df %>% distinct(endpoint_id, date) %>% count(endpoint_id, name = "n_obs_weeks")
  all_weeks_eps <- ep_counts %>%
    filter(n_obs_weeks == n_weeks) %>%
    pull(endpoint_id) %>%
    sort()
  n_balanced <- length(all_weeks_eps)

  weekly_eps <- split(df$endpoint_id, df$date)
  weekly_eps <- lapply(weekly_eps, unique)
  weekly_counts <- lengths(weekly_eps)
  mean_n <- mean(weekly_counts)

  exits_list <- purrr::map_int(2:length(dates), function(i) {
    length(setdiff(weekly_eps[[as.character(dates[i - 1L])]], weekly_eps[[as.character(dates[i])]]))
  })
  mean_exits <- mean(exits_list)

  cat(sprintf("  N_balanced=%d, mean_N=%.0f, exits=%.0f/wk\n", n_balanced, mean_n, mean_exits))

  metric_wide <- df %>%
    filter(endpoint_id %in% all_weeks_eps) %>%
    select(date, endpoint_id, metric_value) %>%
    pivot_wider(names_from = endpoint_id, values_from = metric_value) %>%
    arrange(date)

  endpoint_cols <- names(metric_wide)[-1]

  rank_wide <- df %>%
    filter(endpoint_id %in% all_weeks_eps) %>%
    select(date, endpoint_id, rank) %>%
    pivot_wider(names_from = endpoint_id, values_from = rank) %>%
    arrange(date)

  rank_wide <- rank_wide %>% select(date, all_of(endpoint_cols))

  metric_mat <- as.matrix(metric_wide[, -1, drop = FALSE])
  rank_mat <- as.matrix(rank_wide[, -1, drop = FALSE])

  log_metric <- log1p(metric_mat)
  log_changes <- matrix_diff_lag(log_metric, 1L)

  var_1 <- apply(log_changes, 2, stats::var, na.rm = TRUE)
  vr_emp <- list()
  for (k in c(2L, 3L, 4L, 6L, 8L, 13L, 17L, 26L, 39L, 52L)) {
    if (k < n_weeks) {
      vr_emp[[as.character(k)]] <- median_vr(log_metric, var_1, k)
    }
  }

  sample_n <- min(2000L, ncol(log_changes))
  sample_idx <- seq_len(sample_n)
  sample_eps <- endpoint_cols[sample_idx]

  acf_emp <- list()
  for (lag_k in c(1L, 2L, 3L, 4L, 8L)) {
    cors <- vapply(sample_idx, function(j) lag_autocor(log_changes[, j], lag_k), numeric(1))
    acf_emp[[as.character(lag_k)]] <- safe_median(cors)
  }

  racf_emp <- list()
  for (lag_k in c(1L, 4L, 13L, 26L, 52L)) {
    cors <- vapply(sample_idx, function(j) lag_autocor(rank_mat[, j], lag_k), numeric(1))
    racf_emp[[as.character(lag_k)]] <- safe_median(cors)
  }

  pers_emp <- list()
  xr2_emp <- list()
  for (k in c(1L, 4L, 13L, 26L, 52L)) {
    if (k < n_weeks) {
      t0_ids <- df %>% filter(date == dates[1], rank <= 100) %>% pull(endpoint_id)
      tk_ids <- df %>% filter(date == dates[k + 1L], rank <= 100) %>% pull(endpoint_id)
      pers_emp[[as.character(k)]] <- length(intersect(t0_ids, tk_ids))

      t0v <- log_metric[1, ]
      tkv <- log_metric[k + 1L, ]
      ok <- is.finite(t0v) & is.finite(tkv)
      xr2_emp[[as.character(k)]] <- stats::cor(t0v[ok], tkv[ok])^2
    }
  }

  s0 <- df %>% filter(date == dates[1]) %>% arrange(rank)
  s0t <- s0 %>% filter(rank >= 1, rank <= 5000, metric_value > 0)
  zipf_fit <- stats::lm(log(metric_value) ~ log(rank), data = s0t)
  zipf_slope <- unname(stats::coef(zipf_fit)[2])

  all_ch_emp <- as.vector(log_changes)
  all_ch_emp <- all_ch_emp[is.finite(all_ch_emp)]

  emp_kurt <- excess_kurtosis(all_ch_emp)
  emp_mean_var <- mean(var_1, na.rm = TRUE)
  emp_median_var <- stats::median(var_1, na.rm = TRUE)
  xsec_var_emp <- mean(apply(log_metric, 1, stats::var, na.rm = TRUE), na.rm = TRUE)

  w0_all <- df %>% filter(date == dates[1])
  xsec_var_full <- stats::var(log1p(w0_all$metric_value[w0_all$metric_value > 0]), na.rm = TRUE)

  cat(sprintf("  Change var: median=%.4f, mean=%.4f\n", emp_median_var, emp_mean_var))
  cat(sprintf("  Cross-sec var: bp=%.2f, full_w0=%.2f\n", xsec_var_emp, xsec_var_full))

  bands <- tibble(
    lo = c(1L, 101L, 501L, 2001L, 5001L),
    hi = c(100L, 500L, 2000L, 5000L, 12000L)
  )

  avg_rank <- colMeans(rank_mat, na.rm = TRUE)
  band_stats <- bands %>% mutate(n = 0L, var = NA_real_, vr4 = NA_real_, vr13 = NA_real_, acf1 = NA_real_)

  for (i in seq_len(nrow(band_stats))) {
    lo <- band_stats$lo[i]
    hi <- band_stats$hi[i]
    idx <- which(avg_rank >= lo & avg_rank <= hi)
    if (length(idx) == 0L) next

    bc <- log_changes[, idx, drop = FALSE]
    bm <- log_metric[, idx, drop = FALSE]

    total_var <- safe_median(apply(bc, 2, stats::var, na.rm = TRUE))
    vr4 <- if (nrow(bm) > 4L) {
      v4 <- apply(matrix_diff_lag(bm, 4L), 2, stats::var, na.rm = TRUE)
      v1 <- apply(bc, 2, stats::var, na.rm = TRUE)
      safe_median(v4 / (4 * pmax(v1, 1e-12)))
    } else {
      NA_real_
    }

    vr13 <- if (nrow(bm) > 13L) {
      v13 <- apply(matrix_diff_lag(bm, 13L), 2, stats::var, na.rm = TRUE)
      v1 <- apply(bc, 2, stats::var, na.rm = TRUE)
      safe_median(v13 / (13 * pmax(v1, 1e-12)))
    } else {
      NA_real_
    }

    acf_pool <- vapply(seq_len(min(500L, ncol(bc))), function(j) lag_autocor(bc[, j], 1L), numeric(1))

    band_stats$n[i] <- length(idx)
    band_stats$var[i] <- total_var
    band_stats$vr4[i] <- vr4
    band_stats$vr13[i] <- vr13
    band_stats$acf1[i] <- safe_median(acf_pool)
  }

  cat("\n  Targets:\n")
  cat(sprintf("    VR(4)=%.4f, VR(13)=%.4f, ACF(1)=%.4f\n", vr_emp[["4"]], vr_emp[["13"]], acf_emp[["1"]]))
  cat(sprintf("    RACF(1)=%.4f, R2(1)=%.4f, R2(13)=%.4f\n", racf_emp[["1"]], xr2_emp[["1"]], xr2_emp[["13"]]))
  cat(sprintf("    Kurt=%.1f, Top-100 pers(1)=%d\n", emp_kurt, pers_emp[["1"]]))

  # ---------------------------------------------------------------------------
  # Stage 1
  # ---------------------------------------------------------------------------
  print_rule("STAGE 1: ESTIMATE sigma_obs FROM ACF STRUCTURE")

  if (abs(acf_emp[["2"]]) > 0.001) {
    phi_agg <- acf_emp[["3"]] / acf_emp[["2"]]
  } else {
    phi_agg <- 0.5
    cat(sprintf("  WARNING: acf_emp[2]=%.6f near zero; using phi_agg=0.5 fallback\n", acf_emp[["2"]]))
  }

  gamma1 <- acf_emp[["1"]] * emp_median_var
  gamma2 <- acf_emp[["2"]] * emp_median_var

  sigma2_obs_est <- -gamma1 + gamma2 / phi_agg
  clip_lo <- 0.01^2
  clip_hi <- 0.50^2
  sigma2_obs_clipped <- clip_num(sigma2_obs_est, clip_lo, clip_hi)
  sigma_obs <- sqrt(sigma2_obs_clipped)
  sobs2 <- sigma_obs^2

  cat(sprintf("  sigma2_obs_est = %.6f (unclipped)\n", sigma2_obs_est))
  if (sigma2_obs_est < clip_lo) {
    cat(sprintf("  CLIPPING: below %.4f; using sigma_obs = %.4f\n", clip_lo, sigma_obs))
  } else if (sigma2_obs_est > clip_hi) {
    cat(sprintf("  CLIPPING: above %.4f; using sigma_obs = %.4f\n", clip_hi, sigma_obs))
  } else {
    cat(sprintf("  sigma_obs = %.4f (no clipping)\n", sigma_obs))
  }

  cat("  phi_agg sensitivity:\n")
  for (phi_alt in c(0.3, 0.5, 0.7)) {
    s2_alt <- -gamma1 + gamma2 / phi_alt
    s_alt <- sqrt(clip_num(s2_alt, clip_lo, clip_hi))
    tag <- if (abs(phi_alt - phi_agg) < 1e-6) " <-- used" else ""
    cat(sprintf("    phi_agg=%.1f: sigma2_obs=%.6f, sigma_obs=%.4f%s\n", phi_alt, s2_alt, s_alt, tag))
  }

  # ---------------------------------------------------------------------------
  # Stage 2
  # ---------------------------------------------------------------------------
  print_rule("STAGE 2: ESTIMATE sigma_het")

  var_ratio <- emp_mean_var / emp_median_var
  sigma_het <- sqrt(log(var_ratio) / 2)
  e_h2 <- exp(2 * sigma_het^2)

  cat(sprintf("  sigma_het = %.4f, E[h^2] = %.4f\n", sigma_het, e_h2))

  # ---------------------------------------------------------------------------
  # Stage 3
  # ---------------------------------------------------------------------------
  print_rule("STAGE 3: ESTIMATE BAND-LEVEL t_df")

  standardized_residuals <- vector("list", length = sample_n)
  keep_i <- 0L
  for (j in sample_idx) {
    ch <- log_changes[, j]
    ch <- ch[is.finite(ch)]
    if (length(ch) > 10L) {
      mu_ep <- mean(ch)
      std_ep <- stats::sd(ch)
      if (is.finite(std_ep) && std_ep > 1e-6) {
        keep_i <- keep_i + 1L
        standardized_residuals[[keep_i]] <- (ch - mu_ep) / std_ep
      }
    }
  }
  standardized_residuals <- standardized_residuals[seq_len(keep_i)]

  z_within <- unlist(standardized_residuals, use.names = FALSE)
  t_fit <- fit_student_t(z_within)
  df_fit <- t_fit$df
  loc_fit <- t_fit$loc
  scale_fit <- t_fit$scale
  t_df_global <- max(3, df_fit)

  cat(sprintf("  Global MLE: df=%.2f -> t_df_global=%.2f\n", df_fit, t_df_global))

  obs_noise_var <- 2 * sobs2

  band_tdf <- list()
  band_tdf_raw <- list()

  endpoint_to_col <- setNames(seq_along(endpoint_cols), endpoint_cols)
  sample_set <- unique(sample_eps)

  for (i in seq_len(nrow(bands))) {
    lo <- bands$lo[i]
    hi <- bands$hi[i]
    key <- band_key(lo, hi)

    beps <- endpoint_cols[avg_rank >= lo & avg_rank <= hi]
    beps_sample <- intersect(beps, sample_set)

    band_std_resid <- list()
    b_count <- 0L
    for (ep in beps_sample) {
      j <- endpoint_to_col[[ep]]
      ch <- log_changes[, j]
      ch <- ch[is.finite(ch)]
      if (length(ch) > 10L) {
        mu_ep <- mean(ch)
        std_ep <- stats::sd(ch)
        if (is.finite(std_ep) && std_ep > 1e-6) {
          b_count <- b_count + 1L
          band_std_resid[[b_count]] <- (ch - mu_ep) / std_ep
        }
      }
    }

    if (b_count > 5L) {
      z_band <- unlist(band_std_resid, use.names = FALSE)
      fit_band <- fit_student_t(z_band, df_start = t_df_global)
      df_band <- max(3, fit_band$df)
    } else {
      df_band <- t_df_global
    }

    band_tdf_raw[[key]] <- df_band

    total_var <- band_stats %>% filter(lo == !!lo, hi == !!hi) %>% pull(var)
    signal_frac <- max(0.05, 1 - obs_noise_var / total_var)

    if (signal_frac < 0.30) {
      df_corrected <- min(df_band / signal_frac, 200)
    } else {
      df_corrected <- df_band
    }

    band_tdf[[key]] <- df_corrected

    cat(sprintf("  Band %5d-%5d: MLE_df=%.2f signal_frac=%.2f -> t_df=%.2f (n_ep=%d)\n",
                lo, hi, df_band, signal_frac, df_corrected, length(beps_sample)))
  }

  tdf_arr <- bands %>%
    mutate(key = band_key(lo, hi)) %>%
    pull(key) %>%
    map_dbl(~ band_tdf[[.x]])

  # Keep global for jump estimation and diagnostics compatibility
  t_df <- t_df_global

  # ---------------------------------------------------------------------------
  # Stage 4
  # ---------------------------------------------------------------------------
  print_rule("STAGE 4: JUMP PARAMETERS")

  threshold <- 4
  expected_tail <- 2 * stats::pt(threshold, df = t_df, lower.tail = FALSE)
  actual_tail <- mean(abs(z_within - loc_fit) > threshold * scale_fit)
  jump_prob <- max(0.005, actual_tail - expected_tail)

  extreme_mask <- abs(z_within) > threshold * scale_fit
  jump_scale <- if (sum(extreme_mask) > 10L) {
    stats::sd(z_within[extreme_mask]) / stats::sd(z_within[!extreme_mask])
  } else {
    5
  }

  cat(sprintf("  jump_prob = %.4f, jump_scale = %.2f\n", jump_prob, jump_scale))

  # ---------------------------------------------------------------------------
  # Stage 4.5
  # ---------------------------------------------------------------------------
  print_rule("STAGE 4.5: ARCH COEFFICIENT")

  z_sq_acfs <- c()
  for (j in sample_idx) {
    ch <- log_changes[, j]
    ch <- ch[is.finite(ch)]
    if (length(ch) > 15L) {
      mu_ep <- mean(ch)
      std_ep <- stats::sd(ch)
      if (is.finite(std_ep) && std_ep > 1e-6) {
        z_ep <- (ch - mu_ep) / std_ep
        z_sq <- z_ep^2
        z_sq_dm <- z_sq - mean(z_sq)
        var_z_sq <- stats::var(z_sq)
        if (is.finite(var_z_sq) && var_z_sq > 1e-10) {
          acf_sq1 <- sum(z_sq_dm[-length(z_sq_dm)] * z_sq_dm[-1]) / ((length(z_sq_dm) - 1) * var_z_sq)
          if (is.finite(acf_sq1)) z_sq_acfs <- c(z_sq_acfs, acf_sq1)
        }
      }
    }
  }

  alpha_arch_raw <- safe_median(z_sq_acfs)
  alpha_arch <- clip_num(alpha_arch_raw, 0.01, 0.50)

  cat(sprintf("  Raw median ACF(z^2,1) = %.4f\n", alpha_arch_raw))
  cat(sprintf("  alpha_arch = %.4f\n", alpha_arch))
  cat(sprintf("  Interpretation: after a 2sd shock, next-period transitory sd scales by %.3fx\n",
              sqrt((1 - alpha_arch) + alpha_arch * 4)))

  # ---------------------------------------------------------------------------
  # Stage 5
  # ---------------------------------------------------------------------------
  print_rule("STAGE 5: BAND-LEVEL STRUCTURAL ESTIMATION")

  model_vr_fn <- function(k, se2, phi, sn2, sobs2_local = 0) {
    sc2 <- if (abs(phi) < 0.999) sn2 / (1 - phi^2) else sn2 * 1000
    vd <- se2 + 2 * sc2 * (1 - phi) + 2 * sobs2_local
    if (vd <= 0) return(1)
    vk <- k * se2 + 2 * sc2 * (1 - phi^k) + 2 * sobs2_local
    vk / (k * vd)
  }

  model_acf1_fn <- function(se2, phi, sn2, sobs2_local = 0) {
    sc2 <- if (abs(phi) < 0.999) sn2 / (1 - phi^2) else sn2 * 1000
    vd <- se2 + 2 * sc2 * (1 - phi) + 2 * sobs2_local
    if (vd <= 0) return(0)
    (-sc2 * (1 - phi)^2 - sobs2_local) / vd
  }

  fit_params <- function(emp_var, emp_vr4, emp_acf1, emp_vr13 = NA_real_, sobs2_local = 0) {
    objective <- function(par) {
      se2 <- exp(par[1])
      phi <- 0.95 / (1 + exp(-par[2]))
      sn2 <- exp(par[3])

      sc2 <- if (abs(phi) < 0.999) sn2 / (1 - phi^2) else sn2 * 1000
      mvar <- se2 + 2 * sc2 * (1 - phi) + 2 * sobs2_local
      if (!is.finite(mvar) || mvar <= 0 || !is.finite(emp_var) || emp_var <= 0) {
        return(1e12)
      }

      loss <- 10 * (log(mvar) - log(emp_var))^2
      loss <- loss + 5 * (model_vr_fn(4, se2, phi, sn2, sobs2_local) - emp_vr4)^2
      loss <- loss + 3 * (model_acf1_fn(se2, phi, sn2, sobs2_local) - emp_acf1)^2

      if (is.finite(emp_vr13)) {
        loss <- loss + 2 * (model_vr_fn(13, se2, phi, sn2, sobs2_local) - emp_vr13)^2
      }

      if (!is.finite(loss)) 1e12 else loss
    }

    best <- NULL
    for (r in seq_len(200L)) {
      x0 <- c(stats::runif(1, -9, -1), stats::runif(1, -2, 2), stats::runif(1, -4, 1))
      fit <- tryCatch(
        stats::optim(
          par = x0,
          fn = objective,
          method = "Nelder-Mead",
          control = list(maxit = 15000, reltol = 1e-12)
        ),
        error = function(e) NULL
      )

      if (!is.null(fit) && is.finite(fit$value)) {
        if (is.null(best) || fit$value < best$value) best <- fit
      }
    }

    if (is.null(best)) {
      stop("Optimizer failed: no valid solution in 200 restarts")
    }

    list(
      se2 = exp(best$par[1]),
      phi = 0.95 / (1 + exp(-best$par[2])),
      sn2 = exp(best$par[3])
    )
  }

  set.seed(123)
  band_params <- bands %>%
    mutate(se = NA_real_, phi = NA_real_, sn = NA_real_, se2 = NA_real_, sn2 = NA_real_, pf = NA_real_)

  for (i in seq_len(nrow(band_params))) {
    st <- band_stats[i, ]
    fitted <- fit_params(st$var, st$vr4, st$acf1, st$vr13, sobs2)
    sc2 <- fitted$sn2 / (1 - fitted$phi^2)
    mvar_s <- fitted$se2 + 2 * sc2 * (1 - fitted$phi)
    mvar_t <- mvar_s + 2 * sobs2

    band_params$se[i] <- sqrt(fitted$se2)
    band_params$phi[i] <- fitted$phi
    band_params$sn[i] <- sqrt(fitted$sn2)
    band_params$se2[i] <- fitted$se2
    band_params$sn2[i] <- fitted$sn2
    band_params$pf[i] <- fitted$se2 / mvar_t

    cat(sprintf("  %5d-%5d: se=%.4f phi=%.4f sn=%.4f perm=%.1f%%\n",
                st$lo, st$hi,
                sqrt(fitted$se2), fitted$phi, sqrt(fitted$sn2),
                100 * fitted$se2 / mvar_t))
  }

  bc_arr <- sqrt(band_params$lo * band_params$hi)
  ses_arr <- band_params$se
  phs_arr <- band_params$phi
  sns_arr <- band_params$sn

  get_p <- function(ranks) {
    lr <- log(clip_num(as.numeric(ranks), 1, max(bc_arr) * 2))
    xl <- log(bc_arr)
    list(
      se = stats::approx(xl, ses_arr, xout = lr, rule = 2)$y,
      phi = stats::approx(xl, phs_arr, xout = lr, rule = 2)$y,
      sn = stats::approx(xl, sns_arr, xout = lr, rule = 2)$y
    )
  }

  # ---------------------------------------------------------------------------
  # Stage 6
  # ---------------------------------------------------------------------------
  print_rule("STAGE 6: RANK-DEPENDENT KAPPA CALIBRATION")

  n_full <- as.integer(round(mean_n))
  alpha_kappa <- 0.5

  total_n <- sum(band_stats$n)
  mean_se2 <- sum(band_params$se2 * band_stats$n) / total_n
  jump_var_factor <- (1 - jump_prob + jump_prob * jump_scale^2)
  mean_eta2 <- e_h2 * mean_se2 * jump_var_factor

  w0_data <- df %>% filter(date == dates[1]) %>% arrange(rank)
  w0_log <- log1p(w0_data$metric_value)
  w0_sorted <- sort(w0_log, decreasing = TRUE)
  n_w0 <- length(w0_sorted)

  if (n_w0 < n_full) {
    tail_n <- min(2000L, n_w0)
    tail_idx <- seq.int(n_w0 - tail_n + 1L, n_w0)
    fit_tail <- stats::lm(w0_sorted[tail_idx] ~ log(tail_idx))
    ic <- unname(stats::coef(fit_tail)[1])
    sl <- unname(stats::coef(fit_tail)[2])
    er <- seq.int(n_w0 + 1L, n_full)
    w0_sorted <- c(w0_sorted, ic + sl * log(er))
  } else {
    w0_sorted <- w0_sorted[seq_len(n_full)]
  }

  init_mean <- mean(w0_sorted)
  init_dev2 <- (w0_sorted - init_mean)^2
  init_ranks <- seq_len(n_full)
  rank_weight <- (init_ranks / n_full)^alpha_kappa

  weighted_dev2 <- mean(rank_weight * init_dev2)
  kappa_base_raw <- max(mean_eta2 / (2 * weighted_dev2), 0.001)

  kappa_stab_factor <- 1.20
  kappa_base <- kappa_base_raw * kappa_stab_factor

  cat(sprintf("  alpha = %.2f\n", alpha_kappa))
  cat(sprintf("  kappa_base_raw = %.6f (analytical)\n", kappa_base_raw))
  cat(sprintf("  kappa_stab_factor = %.2f\n", kappa_stab_factor))
  cat(sprintf("  kappa_base = %.6f (stabilized)\n", kappa_base))

  for (r_check in c(1L, 100L, 1000L, 5000L, n_full)) {
    k_r <- kappa_base * (r_check / n_full)^alpha_kappa
    hl <- if (k_r > 0) log(2) / k_r else Inf
    cat(sprintf("    Rank %5d: kappa=%.6f (HL=%.0f wk)\n", r_check, k_r, hl))
  }

  kappa_global <- kappa_base * mean((seq_len(n_full) / n_full)^alpha_kappa)

  print_rule("PARAMETER SUMMARY (v4.2)")
  cat(sprintf("  sigma_obs   = %.4f\n", sigma_obs))
  cat(sprintf("  sigma_het   = %.4f\n", sigma_het))
  cat(sprintf("  t_df        = %.2f\n", t_df))
  for (i in seq_len(nrow(bands))) {
    key <- band_key(bands$lo[i], bands$hi[i])
    cat(sprintf("    Band %5d-%5d: t_df = %.2f\n", bands$lo[i], bands$hi[i], band_tdf[[key]]))
  }
  cat(sprintf("  kappa_base  = %.6f (raw=%.6f x stab=%.2f)\n", kappa_base, kappa_base_raw, kappa_stab_factor))
  cat(sprintf("  kappa_global= %.6f\n", kappa_global))
  cat(sprintf("  T_BURNIN    = 50 weeks\n"))
  cat(sprintf("  jump_prob   = %.4f\n", jump_prob))
  cat(sprintf("  jump_scale  = %.2f\n", jump_scale))
  cat(sprintf("  alpha_arch  = %.4f\n", alpha_arch))

  inc_alpha <- 0.3
  p_exit_incumbent <- 0.0040
  inc_p_base <- p_exit_incumbent * (inc_alpha + 1)
  trans_p_exit <- 0.07

  t_sim <- n_weeks
  t_burnin_default <- 50L
  t_total_default <- t_burnin_default + t_sim
  n_rep <- as.integer(Sys.getenv("V42_N_REP", "25"))
  if (!is.finite(n_rep) || n_rep < 1L) n_rep <- 25L

  n_cal <- as.integer(Sys.getenv("V42_N_CAL", "5"))
  if (!is.finite(n_cal) || n_cal < 1L) n_cal <- 5L

  skip_plots <- tolower(Sys.getenv("V42_SKIP_PLOTS", "false")) %in% c("1", "true", "yes", "y")

  cat("\n")
  print_rule(sprintf("SIMULATION v4.2 - %d MC REPS", n_rep))
  cat(sprintf("  N=%d, T_record=%d, T_burnin=%d, T_total=%d\n", n_full, t_sim, t_burnin_default, t_total_default))

  # pre-calibration defaults (updated after calibration step)
  band_tdf_precal <- band_tdf
  tdf_arr_precal <- tdf_arr

  run_sim <- function(seed, config = list(), return_extra = FALSE, return_band_kurtosis = FALSE) {
    set.seed(seed)

    use_burnin <- if (!is.null(config$burn_in)) isTRUE(config$burn_in) else TRUE
    use_kappa <- if (!is.null(config$kappa)) isTRUE(config$kappa) else TRUE
    use_rank_dep_kappa <- if (!is.null(config$rank_dep_kappa)) isTRUE(config$rank_dep_kappa) else TRUE
    use_kappa_stab <- if (!is.null(config$kappa_stab)) isTRUE(config$kappa_stab) else TRUE
    use_heavy_tails <- if (!is.null(config$heavy_tails)) isTRUE(config$heavy_tails) else TRUE
    use_arch <- if (!is.null(config$arch)) isTRUE(config$arch) else TRUE
    use_rank_dep_tdf <- if (!is.null(config$rank_dep_tdf)) isTRUE(config$rank_dep_tdf) else TRUE
    use_calibrated_tdf <- if (!is.null(config$calibrated_tdf)) isTRUE(config$calibrated_tdf) else TRUE

    s_obs <- if (!is.null(config$sigma_obs)) config$sigma_obs else sigma_obs
    s_het <- if (!is.null(config$sigma_het)) config$sigma_het else sigma_het
    a_arch <- if (!is.null(config$alpha_arch)) config$alpha_arch else alpha_arch
    a_kap <- if (!is.null(config$alpha_kappa)) config$alpha_kappa else alpha_kappa
    tdf_g <- if (!is.null(config$t_df_global)) config$t_df_global else t_df_global

    if (!is.null(config$kappa_base)) {
      kb <- config$kappa_base
    } else if (use_kappa) {
      kb <- if (use_kappa_stab) kappa_base else kappa_base_raw
    } else {
      kb <- 0
    }
    if (!use_kappa) kb <- 0

    kappa_uniform <- if (kb > 0) kb * mean((seq_len(n_full) / n_full)^a_kap) else 0

    tdf_scale <- if (tdf_g != t_df_global) tdf_g / t_df_global else 1

    get_tdf_local <- function(ranks) {
      lr <- log(clip_num(as.numeric(ranks), 1, max(bc_arr) * 2))
      xl <- log(bc_arr)
      base_arr <- if (use_calibrated_tdf) tdf_arr else tdf_arr_precal
      base <- stats::approx(xl, base_arr, xout = lr, rule = 2)$y
      if (tdf_scale != 1) {
        pmax(3.5, base * tdf_scale)
      } else {
        base
      }
    }

    t_burnin <- if (use_burnin) t_burnin_default else 0L
    t_total <- t_burnin + t_sim

    tau <- w0_sorted
    c_state <- rep(0, n_full)
    het_multiplier <- clip_num(exp(stats::rnorm(n_full, 0, s_het)), 0.15, 8)
    ep_type <- rep(0L, n_full)
    endpoint_id <- as.numeric(0:(n_full - 1L))
    next_id <- n_full
    last_z_sq <- rep(1, n_full)

    sim_ly <- matrix(0, nrow = t_sim, ncol = n_full)
    sim_rk <- matrix(0L, nrow = t_sim, ncol = n_full)
    sim_ids <- matrix(0, nrow = t_sim, ncol = n_full)
    sim_ly_true <- if (return_extra) matrix(0, nrow = t_sim, ncol = n_full) else NULL

    obs_noise <- stats::rnorm(n_full, 0, s_obs)
    y0_obs <- tau + c_state + obs_noise
    ord <- order(y0_obs, decreasing = TRUE)
    ranks <- integer(n_full)
    ranks[ord] <- seq_len(n_full)

    total_exits <- 0L
    xsec_vars <- c(stats::var(tau))

    if (t_burnin == 0L) {
      sim_ly[1, ] <- y0_obs
      sim_rk[1, ] <- ranks
      sim_ids[1, ] <- endpoint_id
      if (return_extra) sim_ly_true[1, ] <- tau + c_state
    }

    if (t_total >= 2L) {
      for (t_abs0 in seq_len(t_total - 1L)) {
        cr <- ranks
        pvals <- get_p(cr)
        se <- pvals$se
        phi_v <- pvals$phi
        sn <- pvals$sn

        se_het <- se * het_multiplier
        sn_het <- sn * het_multiplier

        # Permanent innovation
        if (use_heavy_tails) {
          is_jump <- stats::runif(n_full) < jump_prob
          eta <- stats::rnorm(n_full, 0, se_het)
          if (any(is_jump)) {
            eta[is_jump] <- stats::rnorm(sum(is_jump), 0, se_het[is_jump] * jump_scale)
          }
        } else {
          eta <- stats::rnorm(n_full, 0, se_het)
        }

        # ARCH scaling
        if (use_arch) {
          arch_var <- (1 - a_arch) + a_arch * last_z_sq
          arch_scale <- sqrt(clip_num(arch_var, 0.1, 10))
        } else {
          arch_scale <- rep(1, n_full)
        }

        # Transitory innovation
        if (use_heavy_tails) {
          if (use_rank_dep_tdf) {
            df_vec <- get_tdf_local(cr)
            t_raw <- stats::rt(n_full, df = df_vec)
            t_var_factor <- sqrt(pmax(df_vec - 2, 0.5) / df_vec)
          } else {
            t_raw <- stats::rt(n_full, df = tdf_g)
            t_var_factor <- sqrt(max(tdf_g - 2, 0.5) / tdf_g)
          }
          nu <- sn_het * t_var_factor * arch_scale * t_raw
        } else {
          nu <- sn_het * arch_scale * stats::rnorm(n_full)
        }

        c_state <- phi_v * c_state + nu
        if (use_arch) {
          last_z_sq <- clip_num(nu^2 / (sn_het^2 + 1e-10), 0, 4)
        }

        # Mean reversion
        current_mean <- mean(tau)
        if (kb > 0) {
          if (use_rank_dep_kappa) {
            kappa_r <- kb * (cr / n_full)^a_kap
          } else {
            kappa_r <- kappa_uniform
          }
          tau <- tau + eta - kappa_r * (tau - current_mean)
        } else {
          tau <- tau + eta
        }

        xsec_vars <- c(xsec_vars, stats::var(tau))

        t_rec0 <- t_abs0 - t_burnin

        # Exit / entry only during recording period
        if (t_rec0 >= 0L) {
          nr <- cr / n_full
          p_exit <- ifelse(ep_type == 0L, inc_p_base * (nr^inc_alpha), trans_p_exit)
          exit_mask <- stats::runif(n_full) < p_exit
          n_ex <- sum(exit_mask)
          total_exits <- total_exits + n_ex

          if (n_ex > 0L) {
            exi <- which(exit_mask)
            n_burst <- max(1L, as.integer(floor(n_ex * 0.008)))
            n_norm <- n_ex - n_burst

            tau_pool <- tau[!exit_mask]
            bq <- as.numeric(stats::quantile(tau_pool, 0.10, na.rm = TRUE))

            lower_half <- tau[tau < stats::median(tau)]
            lower_sd <- safe_sd(lower_half)
            if (!is.finite(lower_sd) || lower_sd <= 0) lower_sd <- safe_sd(tau)
            bstd <- max(lower_sd * 0.4, 1e-6)

            new_tau <- if (n_norm > 0L) stats::rnorm(n_norm, bq, bstd) else numeric(0)
            if (n_burst > 0L) {
              buq <- as.numeric(stats::quantile(tau_pool, 0.90, na.rm = TRUE))
              bust <- max(safe_sd(tau) * 0.25, 1e-6)
              new_tau <- c(new_tau, stats::rnorm(n_burst, buq, bust))
            }

            tau[exi] <- new_tau

            if (use_heavy_tails) {
              c_state[exi] <- stats::rt(n_ex, df = tdf_g) * 0.3
            } else {
              c_state[exi] <- stats::rnorm(n_ex, 0, 0.3)
            }

            het_multiplier[exi] <- clip_num(exp(stats::rnorm(n_ex, 0, s_het)), 0.15, 8)
            last_z_sq[exi] <- 1
            ep_type[exi] <- 1L
            endpoint_id[exi] <- seq.int(next_id, next_id + n_ex - 1L)
            next_id <- next_id + n_ex
          }
        }

        log_y_true <- tau + c_state
        obs_noise <- stats::rnorm(n_full, 0, s_obs)
        log_y_obs <- log_y_true + obs_noise

        ord <- order(log_y_obs, decreasing = TRUE)
        ranks <- integer(n_full)
        ranks[ord] <- seq_len(n_full)

        if (t_rec0 == 0L) {
          sim_ly[1, ] <- log_y_obs
          sim_rk[1, ] <- ranks
          sim_ids[1, ] <- endpoint_id
          if (return_extra) sim_ly_true[1, ] <- log_y_true
        } else if (t_rec0 > 0L && t_rec0 < t_sim) {
          idx <- t_rec0 + 1L
          sim_ly[idx, ] <- log_y_obs
          sim_rk[idx, ] <- ranks
          sim_ids[idx, ] <- endpoint_id
          if (return_extra) sim_ly_true[idx, ] <- log_y_true
        }
      }
    }

    # Utility: build balanced panel matrices by survivor IDs.
    build_balanced <- function(values_mat, rank_mat_local, id_mat) {
      survivor_ids <- Reduce(intersect, lapply(seq_len(t_sim), function(tt) id_mat[tt, ]))
      survivor_ids <- sort(survivor_ids)
      n_bp <- length(survivor_ids)
      if (n_bp == 0L) {
        return(list(ids = numeric(0), values = matrix(numeric(0), 0, 0), ranks = matrix(integer(0), 0, 0)))
      }

      bp_values <- matrix(NA_real_, nrow = t_sim, ncol = n_bp)
      bp_ranks <- matrix(NA_integer_, nrow = t_sim, ncol = n_bp)

      for (tt in seq_len(t_sim)) {
        idx <- match(survivor_ids, id_mat[tt, ])
        bp_values[tt, ] <- values_mat[tt, idx]
        bp_ranks[tt, ] <- rank_mat_local[tt, idx]
      }

      list(ids = survivor_ids, values = bp_values, ranks = bp_ranks)
    }

    if (return_band_kurtosis) {
      bp <- build_balanced(sim_ly, sim_rk, sim_ids)
      bp_ly_bk <- bp$values
      bp_rk_bk <- bp$ranks

      if (ncol(bp_ly_bk) == 0L || nrow(bp_ly_bk) < 2L) {
        out <- setNames(rep(NA_real_, nrow(bands)), bands %>% mutate(key = band_key(lo, hi)) %>% pull(key))
        return(out)
      }

      bp_avg_rk_bk <- colMeans(bp_rk_bk, na.rm = TRUE)
      sim_ch_bk <- matrix_diff_lag(bp_ly_bk, 1L)

      out <- numeric(nrow(bands))
      names(out) <- bands %>% mutate(key = band_key(lo, hi)) %>% pull(key)

      for (i in seq_len(nrow(bands))) {
        lo <- bands$lo[i]
        hi <- bands$hi[i]
        key <- band_key(lo, hi)

        bm <- which(bp_avg_rk_bk >= lo & bp_avg_rk_bk <= hi)
        if (length(bm) > 5L) {
          bch <- as.vector(sim_ch_bk[, bm, drop = FALSE])
          bch <- bch[is.finite(bch)]
          out[key] <- if (length(bch) > 20L) excess_kurtosis(bch) else NA_real_
        } else {
          out[key] <- NA_real_
        }
      }
      return(out)
    }

    bp <- build_balanced(sim_ly, sim_rk, sim_ids)
    bp_ly <- bp$values
    bp_rk <- bp$ranks
    n_bp <- ncol(bp_ly)

    if (n_bp == 0L) {
      return(list(diag = list(), extra = NULL))
    }

    bp_ly_true_arr <- NULL
    if (return_extra) {
      bp_true <- build_balanced(sim_ly_true, sim_rk, sim_ids)
      bp_ly_true_arr <- bp_true$values
    }

    sim_ch <- matrix_diff_lag(bp_ly, 1L)
    sim_v1 <- apply(sim_ch, 2, stats::var, na.rm = TRUE)

    diag <- list()

    for (k in c(2L, 4L, 8L, 13L)) {
      if (k < t_sim) {
        diag[[paste0("vr", k)]] <- median_vr(bp_ly, sim_v1, k)
      }
    }

    for (lag_k in c(1L, 2L)) {
      cors <- vapply(seq_len(min(1000L, n_bp)), function(j) lag_autocor(sim_ch[, j], lag_k), numeric(1))
      diag[[paste0("acf", lag_k)]] <- safe_median(cors)
    }

    for (lag_k in c(1L, 4L, 13L)) {
      cors <- vapply(seq_len(min(1000L, n_bp)), function(j) lag_autocor(bp_rk[, j], lag_k), numeric(1))
      diag[[paste0("racf", lag_k)]] <- safe_median(cors)
    }

    for (k in c(1L, 4L, 13L, 26L, 52L)) {
      if (k < t_sim) {
        t0_ids <- sim_ids[1, sim_rk[1, ] <= 100]
        tk_ids <- sim_ids[k + 1L, sim_rk[k + 1L, ] <= 100]
        diag[[paste0("pers", k)]] <- length(intersect(t0_ids, tk_ids))
      }
    }

    for (k in c(1L, 4L, 13L, 26L, 52L)) {
      if (k < nrow(bp_ly)) {
        diag[[paste0("xr2_", k)]] <- stats::cor(bp_ly[1, ], bp_ly[k + 1L, ])^2
      }
    }

    sim_ch_flat <- as.vector(sim_ch)
    sim_ch_flat <- sim_ch_flat[is.finite(sim_ch_flat)]

    diag[["kurtosis"]] <- excess_kurtosis(sim_ch_flat)

    ks_n <- min(50000L, length(all_ch_emp), length(sim_ch_flat))
    ks_emp <- sample(all_ch_emp, ks_n, replace = FALSE)
    ks_sim <- sample(sim_ch_flat, ks_n, replace = FALSE)
    diag[["ks"]] <- as.numeric(stats::ks.test(ks_emp, ks_sim)$statistic)

    sim_y0 <- exp(sim_ly[1, ])
    ss <- sort(sim_y0, decreasing = TRUE)
    rr <- seq_len(n_full)
    mask <- rr <= 5000 & ss > 0
    zipf_sim <- stats::lm(log(ss[mask]) ~ log(rr[mask]))
    diag[["zipf_slope"]] <- unname(stats::coef(zipf_sim)[2])

    diag[["survivor_pct"]] <- n_bp / n_full * 100
    diag[["xsec_var_start"]] <- xsec_vars[t_burnin + 1L]
    diag[["xsec_var_end"]] <- tail(xsec_vars, 1)
    diag[["avg_exits"]] <- total_exits / t_sim
    diag[["xsec_var_drift"]] <- tail(xsec_vars, 1) / max(xsec_vars[t_burnin + 1L], 0.01)

    bp_avg_rk_rep <- colMeans(bp_rk, na.rm = TRUE)
    for (i in seq_len(nrow(bands))) {
      lo <- bands$lo[i]
      hi <- bands$hi[i]
      key <- paste0("kurt_", lo, "_", hi)

      bm <- which(bp_avg_rk_rep >= lo & bp_avg_rk_rep <= hi)
      if (length(bm) > 5L) {
        band_ch <- as.vector(sim_ch[, bm, drop = FALSE])
        band_ch <- band_ch[is.finite(band_ch)]
        diag[[key]] <- if (length(band_ch) > 20L) excess_kurtosis(band_ch) else NA_real_
      } else {
        diag[[key]] <- NA_real_
      }
    }

    extra <- NULL
    if (return_extra) {
      extra <- list(
        sim_ly = sim_ly,
        sim_rk = sim_rk,
        bp_ly = bp_ly,
        bp_ly_true = bp_ly_true_arr,
        bp_rk = bp_rk,
        sim_ch = sim_ch,
        sim_v1 = sim_v1,
        sim_ch_flat = sim_ch_flat,
        xsec_vars = xsec_vars,
        n_bp = n_bp,
        bp_avg_rk_global = bp_avg_rk_rep,
        sim_rk_full = sim_rk,
        sim_ly_full = sim_ly,
        sim_ids_full = sim_ids
      )
    }

    list(diag = diag, extra = extra)
  }

  # ---------------------------------------------------------------------------
  # Two-pass kurtosis calibration
  # ---------------------------------------------------------------------------
  print_rule("KURTOSIS CALIBRATION PASS (v4.2)")

  cal_seeds <- seq.int(200L, 200L + n_cal - 1L)
  cat(sprintf("  Running %d calibration replications...\n", n_cal))

  emp_band_kurt_target <- list()
  for (i in seq_len(nrow(bands))) {
    lo <- bands$lo[i]
    hi <- bands$hi[i]
    key <- band_key(lo, hi)

    beps <- endpoint_cols[avg_rank >= lo & avg_rank <= hi]
    idx <- endpoint_to_col[beps]
    emp_band_ch <- as.vector(log_changes[, idx, drop = FALSE])
    emp_band_ch <- emp_band_ch[is.finite(emp_band_ch)]
    emp_band_kurt_target[[key]] <- excess_kurtosis(emp_band_ch)

    cat(sprintf("  Emp target %5d-%5d: kurt = %.2f\n", lo, hi, emp_band_kurt_target[[key]]))
  }

  cat("\n  Running calibration sims...\n")
  cal_kurts_all <- setNames(vector("list", nrow(bands)), bands %>% mutate(key = band_key(lo, hi)) %>% pull(key))

  for (ci in seq_along(cal_seeds)) {
    seed_i <- cal_seeds[ci]
    t_cal <- Sys.time()
    bk <- run_sim(seed_i, return_band_kurtosis = TRUE)
    for (nm in names(cal_kurts_all)) {
      if (is.finite(bk[[nm]])) {
        cal_kurts_all[[nm]] <- c(cal_kurts_all[[nm]], bk[[nm]])
      }
    }
    cat(sprintf("    Cal %d/%d: %.1fs\n", ci, n_cal, as.numeric(difftime(Sys.time(), t_cal, units = "secs"))))
  }

  overshoot <- 1.5
  protected_bands <- c(band_key(1, 100), band_key(101, 500))

  cat(sprintf("\n  Calibration results and t_df adjustment (overshoot=%.1fx):\n", overshoot))
  cat(sprintf("  Protected bands (no adjustment): %s\n", paste(protected_bands, collapse = ", ")))

  band_tdf_calibrated <- list()

  for (i in seq_len(nrow(bands))) {
    lo <- bands$lo[i]
    hi <- bands$hi[i]
    key <- band_key(lo, hi)

    old_df <- band_tdf[[key]]
    cal_vals <- cal_kurts_all[[key]]
    emp_k <- emp_band_kurt_target[[key]]

    if (key %in% protected_bands) {
      new_df <- old_df
      reason <- "protected"
    } else if (length(cal_vals) >= 2L) {
      sim_k <- stats::median(cal_vals)
      if (sim_k > 0.5 && emp_k > 0.5 && abs(sim_k - emp_k) / emp_k > 0.10) {
        old_t_kurt <- if (old_df > 4.5) 6 / (old_df - 4) else 6 / max(old_df - 4, 0.3)
        ratio <- emp_k / sim_k
        target_t_kurt <- old_t_kurt * (ratio^overshoot)
        new_df <- 4 + 6 / target_t_kurt
        new_df <- clip_num(new_df, 4.2, 200)
        reason <- sprintf("adjusted (ratio=%.2f)", ratio)
      } else {
        new_df <- old_df
        reason <- "within 10%"
      }
    } else {
      new_df <- old_df
      reason <- "insufficient data"
    }

    band_tdf_calibrated[[key]] <- new_df
    sim_k_str <- if (length(cal_vals) >= 2L) sprintf("%.2f", stats::median(cal_vals)) else "N/A"
    cat(sprintf("    %5d-%5d: emp=%.2f sim_cal=%s t_df: %.2f -> %.2f [%s]\n",
                lo, hi, emp_k, sim_k_str, old_df, new_df, reason))
  }

  # Save pre-calibration values for ablation levels without calibrated t_df.
  band_tdf_precal <- band_tdf
  tdf_arr_precal <- bands %>%
    mutate(key = band_key(lo, hi)) %>%
    pull(key) %>%
    map_dbl(~ band_tdf_precal[[.x]])

  # Activate calibrated t_df for main run.
  band_tdf <- band_tdf_calibrated
  tdf_arr <- bands %>%
    mutate(key = band_key(lo, hi)) %>%
    pull(key) %>%
    map_dbl(~ band_tdf[[.x]])

  cat("\n  Calibrated t_df values now active for main MC run.\n")

  # ---------------------------------------------------------------------------
  # Main MC runs
  # ---------------------------------------------------------------------------
  cat(sprintf("\nRunning %d replications...\n", n_rep))

  all_diags <- vector("list", n_rep)
  rep_extra <- NULL
  seeds <- c(42L, seq.int(100L, 100L + n_rep - 2L))

  for (i in seq_along(seeds)) {
    seed_i <- seeds[i]
    t_rep <- Sys.time()
    out <- run_sim(seed_i, return_extra = (seed_i == 42L))
    all_diags[[i]] <- out$diag
    if (!is.null(out$extra)) rep_extra <- out$extra

    if (i == 1L || i %% 5L == 0L) {
      cat(sprintf("  Rep %d/%d (seed=%d): %.1fs\n", i, n_rep, seed_i,
                  as.numeric(difftime(Sys.time(), t_rep, units = "secs"))))
    }
  }

  cat(sprintf("  Total MC time: %.0fs\n", as.numeric(difftime(Sys.time(), t_start, units = "secs"))))

  diag_df <- bind_rows(lapply(all_diags, as_row_tibble))

  mc_stats <- lapply(names(diag_df), function(nm) mc_summary(diag_df[[nm]]))
  names(mc_stats) <- names(diag_df)

  mc_get <- function(key, field = "mean") {
    if (!key %in% names(mc_stats)) return(NA_real_)
    mc_stats[[key]][[field]]
  }

  # ---------------------------------------------------------------------------
  # Validation
  # ---------------------------------------------------------------------------
  print_rule(sprintf("VALIDATION (mean +/- 95%% CI, %d replications)", n_rep))

  cat("\n--- Variance Ratios ---\n")
  for (k in c(2L, 4L, 8L, 13L)) {
    key <- paste0("vr", k)
    s <- mc_stats[[key]]
    err <- abs(s$mean - vr_emp[[as.character(k)]]) / vr_emp[[as.character(k)]] * 100
    ok <- if (err < 20) "Y" else "N"
    cat(sprintf("  VR(%2d): emp=%.4f sim=%.4f [%.4f, %.4f] err=%.1f%% [%s]\n",
                k, vr_emp[[as.character(k)]], s$mean, s$lo, s$hi, err, ok))
  }

  cat("\n--- ACF of changes ---\n")
  for (lag_k in c(1L, 2L)) {
    key <- paste0("acf", lag_k)
    s <- mc_stats[[key]]
    err <- abs(s$mean - acf_emp[[as.character(lag_k)]])
    ok <- if (err < 0.08) "Y" else "N"
    cat(sprintf("  ACF(%d): emp=%.4f sim=%.4f [%.4f, %.4f] err=%.4f [%s]\n",
                lag_k, acf_emp[[as.character(lag_k)]], s$mean, s$lo, s$hi, err, ok))
  }

  cat("\n--- Rank ACF ---\n")
  for (lag_k in c(1L, 4L, 13L)) {
    key <- paste0("racf", lag_k)
    s <- mc_stats[[key]]
    err <- abs(s$mean - racf_emp[[as.character(lag_k)]])
    ok <- if (err < 0.08) "Y" else "N"
    cat(sprintf("  RACF(%2d): emp=%.4f sim=%.4f [%.4f, %.4f] err=%.4f [%s]\n",
                lag_k, racf_emp[[as.character(lag_k)]], s$mean, s$lo, s$hi, err, ok))
  }

  cat("\n--- Top-100 Persistence ---\n")
  for (k in c(1L, 4L, 13L, 26L, 52L)) {
    key <- paste0("pers", k)
    if (!key %in% names(mc_stats)) next
    s <- mc_stats[[key]]
    d <- s$mean - pers_emp[[as.character(k)]]
    ok <- if (abs(d) < 10) "Y" else "N"
    cat(sprintf("  k=%2d: emp=%d sim=%.1f [%.0f, %.0f] diff=%+.1f [%s]\n",
                k, pers_emp[[as.character(k)]], s$mean, s$lo, s$hi, d, ok))
  }

  cat("\n--- Cross-Sectional R-squared ---\n")
  for (k in c(1L, 4L, 13L, 26L, 52L)) {
    key <- paste0("xr2_", k)
    if (!key %in% names(mc_stats)) next
    s <- mc_stats[[key]]
    err <- abs(s$mean - xr2_emp[[as.character(k)]])
    ok <- if (err < 0.08) "Y" else "N"
    cat(sprintf("  R2(%2d): emp=%.4f sim=%.4f [%.4f, %.4f] err=%.4f [%s]\n",
                k, xr2_emp[[as.character(k)]], s$mean, s$lo, s$hi, err, ok))
  }

  cat("\n--- Additional ---\n")
  add_table <- tribble(
    ~key, ~label, ~emp,
    "kurtosis", "Kurtosis", emp_kurt,
    "ks", "KS stat", NA_real_,
    "zipf_slope", "Zipf slope", zipf_slope,
    "survivor_pct", "Survivors %", n_balanced / mean_n * 100
  )

  for (i in seq_len(nrow(add_table))) {
    key <- add_table$key[i]
    s <- mc_stats[[key]]
    emp_val <- add_table$emp[i]
    if (is.finite(emp_val)) {
      cat(sprintf("  %s: emp=%.2f sim=%.2f [%.2f, %.2f]\n", add_table$label[i], emp_val, s$mean, s$lo, s$hi))
    } else {
      cat(sprintf("  %s: sim=%.3f [%.3f, %.3f]\n", add_table$label[i], s$mean, s$lo, s$hi))
    }
  }

  s_start <- mc_stats[["xsec_var_start"]]
  s_end <- mc_stats[["xsec_var_end"]]
  cat(sprintf("  Cross-sec var: start=%.2f end=%.2f (emp=%.2f)\n", s_start$mean, s_end$mean, xsec_var_full))

  tests <- c(
    "VR(2)" = abs(mc_get("vr2") - vr_emp[["2"]]) / vr_emp[["2"]] < 0.20,
    "VR(4)" = abs(mc_get("vr4") - vr_emp[["4"]]) / vr_emp[["4"]] < 0.20,
    "VR(8)" = abs(mc_get("vr8") - vr_emp[["8"]]) / vr_emp[["8"]] < 0.20,
    "VR(13)" = abs(mc_get("vr13") - vr_emp[["13"]]) / vr_emp[["13"]] < 0.20,
    "ACF(1)" = abs(mc_get("acf1") - acf_emp[["1"]]) < 0.08,
    "ACF(2)" = abs(mc_get("acf2") - acf_emp[["2"]]) < 0.08,
    "RACF(1)" = abs(mc_get("racf1") - racf_emp[["1"]]) < 0.08,
    "RACF(4)" = abs(mc_get("racf4") - racf_emp[["4"]]) < 0.08,
    "RACF(13)" = abs(mc_get("racf13") - racf_emp[["13"]]) < 0.08,
    "Pers(1)" = abs(mc_get("pers1") - pers_emp[["1"]]) < 10,
    "Pers(4)" = abs(mc_get("pers4") - pers_emp[["4"]]) < 10,
    "Pers(13)" = abs(mc_get("pers13") - pers_emp[["13"]]) < 10,
    "R2(1)" = abs(mc_get("xr2_1") - xr2_emp[["1"]]) < 0.08,
    "R2(4)" = abs(mc_get("xr2_4") - xr2_emp[["4"]]) < 0.08,
    "R2(13)" = abs(mc_get("xr2_13") - xr2_emp[["13"]]) < 0.08
  )

  n_pass <- sum(tests, na.rm = TRUE)

  print_rule(sprintf("SUMMARY v4.2 (%d replications)", n_rep))
  cat(sprintf("\n  Diagnostics: %d/%d\n", n_pass, length(tests)))
  for (nm in names(tests)) {
    cat(sprintf("    %s: %s\n", nm, if (isTRUE(tests[[nm]])) "PASS" else "FAIL"))
  }

  # ---------------------------------------------------------------------------
  # MC uncertainty reporting
  # ---------------------------------------------------------------------------
  print_rule(sprintf("MC UNCERTAINTY ANALYSIS (%d replications)", n_rep))

  mc_diag_info <- tribble(
    ~dname, ~dkey, ~emp, ~mode, ~thresh,
    "VR(2)", "vr2", vr_emp[["2"]], "rel", 0.20,
    "VR(4)", "vr4", vr_emp[["4"]], "rel", 0.20,
    "VR(8)", "vr8", vr_emp[["8"]], "rel", 0.20,
    "VR(13)", "vr13", vr_emp[["13"]], "rel", 0.20,
    "ACF(1)", "acf1", acf_emp[["1"]], "abs", 0.08,
    "ACF(2)", "acf2", acf_emp[["2"]], "abs", 0.08,
    "RACF(1)", "racf1", racf_emp[["1"]], "abs", 0.08,
    "RACF(4)", "racf4", racf_emp[["4"]], "abs", 0.08,
    "RACF(13)", "racf13", racf_emp[["13"]], "abs", 0.08,
    "Pers(1)", "pers1", pers_emp[["1"]], "abs", 10,
    "Pers(4)", "pers4", pers_emp[["4"]], "abs", 10,
    "Pers(13)", "pers13", pers_emp[["13"]], "abs", 10,
    "R2(1)", "xr2_1", xr2_emp[["1"]], "abs", 0.08,
    "R2(4)", "xr2_4", xr2_emp[["4"]], "abs", 0.08,
    "R2(13)", "xr2_13", xr2_emp[["13"]], "abs", 0.08
  )

  cat(sprintf("\n  %-12s %8s %8s %8s %8s %6s %8s\n", "Diagnostic", "MC Mean", "MC SE", "Distance", "Thresh", "d/SE", "Label"))
  cat("  ", strrep("-", 70), "\n", sep = "")

  for (i in seq_len(nrow(mc_diag_info))) {
    dkey <- mc_diag_info$dkey[i]
    vals <- diag_df[[dkey]]
    vals <- vals[is.finite(vals)]

    mc_mean <- mean(vals)
    mc_se <- stats::sd(vals) / sqrt(length(vals))

    if (mc_diag_info$mode[i] == "rel") {
      distance <- mc_diag_info$thresh[i] * mc_diag_info$emp[i] - abs(mc_mean - mc_diag_info$emp[i])
    } else {
      distance <- mc_diag_info$thresh[i] - abs(mc_mean - mc_diag_info$emp[i])
    }

    d_over_se <- if (is.finite(mc_se) && mc_se > 1e-10) distance / mc_se else Inf
    label <- if (d_over_se > 3) {
      "solid"
    } else if (d_over_se > 1.5) {
      "marginal"
    } else {
      "FRAGILE"
    }

    cat(sprintf("  %-12s %8.4f %8.4f %8.4f %8.4f %6.1f %8s\n",
                mc_diag_info$dname[i], mc_mean, mc_se, distance,
                mc_diag_info$thresh[i], d_over_se, label))
  }

  # ---------------------------------------------------------------------------
  # Publication diagnostics
  # ---------------------------------------------------------------------------
  print_rule("PUBLICATION DIAGNOSTICS (v4.2)")

  if (!is.null(rep_extra)) {
    sim_ly <- rep_extra$sim_ly
    sim_rk <- rep_extra$sim_rk
    bp_ly <- rep_extra$bp_ly
    bp_ly_true <- rep_extra$bp_ly_true
    bp_rk <- rep_extra$bp_rk
    sim_ch <- rep_extra$sim_ch
    sim_v1 <- rep_extra$sim_v1
    sim_ch_flat <- rep_extra$sim_ch_flat
    xsec_vars <- rep_extra$xsec_vars
    n_bp <- rep_extra$n_bp
    bp_avg_rk_global <- rep_extra$bp_avg_rk_global
    sim_rk_full <- rep_extra$sim_rk_full
    sim_ly_full <- rep_extra$sim_ly_full
    sim_ids_full <- rep_extra$sim_ids_full

    set.seed(12345)

    cat("\n--- Formal Statistical Tests ---\n")

    # Two-sample KS as deterministic publication test fallback in base R.
    sim_subsample <- sample(sim_ch_flat, min(5000L, length(sim_ch_flat)), replace = FALSE)
    emp_subsample <- sample(all_ch_emp, min(5000L, length(all_ch_emp)), replace = FALSE)
    ks_pub <- stats::ks.test(emp_subsample, sim_subsample)

    cat(sprintf("  KS two-sample: stat=%.3f, p=%.4f\n",
                as.numeric(ks_pub$statistic), ks_pub$p.value))

    sim_std_resid <- c()
    for (j in seq_len(min(500L, n_bp))) {
      ch <- sim_ch[, j]
      ch <- ch[is.finite(ch)]
      if (length(ch) > 10L) {
        mu <- mean(ch)
        s <- stats::sd(ch)
        if (is.finite(s) && s > 1e-6) {
          sim_std_resid <- c(sim_std_resid, (ch - mu) / s)
        }
      }
    }

    jb_sim <- jb_test(sim_std_resid)
    cat(sprintf("\n  Jarque-Bera (sim residuals): stat=%.1f, p=%.2e\n", jb_sim$statistic, jb_sim$p_value))
    cat(sprintf("    Skew=%.3f, Excess kurtosis=%.2f\n", jb_sim$skew, jb_sim$kurt))

    jb_emp <- jb_test(z_within)
    cat(sprintf("  Jarque-Bera (emp residuals): stat=%.1f, p=%.2e\n", jb_emp$statistic, jb_emp$p_value))
    cat(sprintf("    Skew=%.3f, Excess kurtosis=%.2f\n", jb_emp$skew, jb_emp$kurt))

    cat("\n  Ljung-Box test (residual serial correlation):\n")
    for (lag_test in c(5L, 10L, 20L)) {
      n_reject <- 0L
      n_tested <- 0L

      for (j in seq_len(min(200L, n_bp))) {
        ch <- sim_ch[, j]
        ch <- ch[is.finite(ch)]
        if (length(ch) > lag_test + 5L) {
          pval <- tryCatch(stats::Box.test(ch, lag = lag_test, type = "Ljung-Box")$p.value,
                           error = function(e) NA_real_)
          if (is.finite(pval)) {
            n_tested <- n_tested + 1L
            if (pval < 0.05) n_reject <- n_reject + 1L
          }
        }
      }

      if (n_tested > 0L) {
        cat(sprintf("    Lag %2d: %d/%d (%.1f%%) reject at alpha=0.05\n",
                    lag_test, n_reject, n_tested, 100 * n_reject / n_tested))
      }
    }

    cat("\n  Hill Tail Index Estimator:\n")
    for (lab in c("Empirical", "Simulated")) {
      data_vec <- if (lab == "Empirical") all_ch_emp else sim_ch_flat
      k_opt <- as.integer(floor(sqrt(length(data_vec))))
      h <- hill_estimator(data_vec, k_opt)
      ci_lo <- h$alpha - 1.96 * h$se
      ci_hi <- h$alpha + 1.96 * h$se
      cat(sprintf("    %s: alpha_hat=%.3f +/- %.3f [%.3f, %.3f] (k=%d)\n",
                  lab, h$alpha, 1.96 * h$se, ci_lo, ci_hi, k_opt))
    }

    cat("\n  Rank Transition Matrix & Shorrocks Mobility Index:\n")
    n_quintiles <- 5L
    emp_rk_arr <- rank_mat

    for (horizon in c(1L, 4L, 13L)) {
      sim_trans <- compute_transition_matrix(bp_rk, n_quintiles, horizon)
      emp_trans <- compute_transition_matrix(emp_rk_arr, n_quintiles, horizon)
      sh_sim <- (n_quintiles - sum(diag(sim_trans))) / (n_quintiles - 1)
      sh_emp <- (n_quintiles - sum(diag(emp_trans))) / (n_quintiles - 1)
      cat(sprintf("    %2d-week: Shorrocks M = %.4f (sim) vs %.4f (emp)\n", horizon, sh_sim, sh_emp))
    }

    sim_trans_13 <- compute_transition_matrix(bp_rk, n_quintiles, 13L)
    emp_trans_13 <- compute_transition_matrix(emp_rk_arr, n_quintiles, 13L)

    cat("\n  Half-Life of Rank Persistence by Stratum:\n")
    strata <- tribble(
      ~rk_lo, ~rk_hi, ~label,
      1L, 100L, "Top 100",
      101L, 500L, "101-500",
      501L, 2000L, "501-2K",
      2001L, 5000L, "2K-5K",
      5001L, n_full, "5K+"
    )

    for (i in seq_len(nrow(strata))) {
      rk_lo <- strata$rk_lo[i]
      rk_hi <- strata$rk_hi[i]
      slabel <- strata$label[i]

      t0_ids <- sim_ids_full[1, sim_rk_full[1, ] >= rk_lo & sim_rk_full[1, ] <= rk_hi]
      n_in <- length(t0_ids)
      if (n_in == 0L) next

      half_life <- NA_real_
      for (k in 1:(t_sim - 1L)) {
        tk_ids <- sim_ids_full[k + 1L, sim_rk_full[k + 1L, ] >= rk_lo & sim_rk_full[k + 1L, ] <= rk_hi]
        frac <- length(intersect(t0_ids, tk_ids)) / n_in

        if (frac < 0.5) {
          prev_k <- k - 1L
          if (prev_k >= 0L) {
            tk_prev <- sim_ids_full[prev_k + 1L, sim_rk_full[prev_k + 1L, ] >= rk_lo & sim_rk_full[prev_k + 1L, ] <= rk_hi]
            frac_prev <- length(intersect(t0_ids, tk_prev)) / n_in
            if (frac_prev > frac) {
              half_life <- prev_k + (frac_prev - 0.5) / (frac_prev - frac)
            } else {
              half_life <- k
            }
          } else {
            half_life <- k
          }
          break
        }
      }

      half_life_str <- if (is.finite(half_life)) sprintf("%.1f wk", half_life) else sprintf(">%d wk", t_sim)

      # empirical (balanced panel: index-based is safe)
      t0_emp_idx <- which(emp_rk_arr[1, ] >= rk_lo & emp_rk_arr[1, ] <= rk_hi)
      n_in_emp <- length(t0_emp_idx)

      half_life_emp <- NA_real_
      if (n_in_emp > 0L) {
        for (k in 1:(nrow(emp_rk_arr) - 1L)) {
          tk_emp <- which(emp_rk_arr[k + 1L, ] >= rk_lo & emp_rk_arr[k + 1L, ] <= rk_hi)
          frac_emp <- length(intersect(t0_emp_idx, tk_emp)) / n_in_emp
          if (frac_emp < 0.5) {
            prev_k <- k - 1L
            if (prev_k >= 0L) {
              tk_prev_emp <- which(emp_rk_arr[prev_k + 1L, ] >= rk_lo & emp_rk_arr[prev_k + 1L, ] <= rk_hi)
              frac_prev_emp <- length(intersect(t0_emp_idx, tk_prev_emp)) / n_in_emp
              if (frac_prev_emp > frac_emp) {
                half_life_emp <- prev_k + (frac_prev_emp - 0.5) / (frac_prev_emp - frac_emp)
              } else {
                half_life_emp <- k
              }
            } else {
              half_life_emp <- k
            }
            break
          }
        }
      }

      hl_emp_str <- if (is.finite(half_life_emp)) sprintf("%.1f wk", half_life_emp) else sprintf(">%d wk", nrow(emp_rk_arr))

      cat(sprintf("    %10s: sim=%10s  emp=%10s\n", slabel, half_life_str, hl_emp_str))
    }

    cat("\n  Kurtosis by Rank Band (25-rep MC mean +/- 95%% CI):\n")
    for (i in seq_len(nrow(bands))) {
      lo <- bands$lo[i]
      hi <- bands$hi[i]
      key <- band_key(lo, hi)

      beps <- endpoint_cols[avg_rank >= lo & avg_rank <= hi]
      idx <- endpoint_to_col[beps]
      emp_band_ch <- as.vector(log_changes[, idx, drop = FALSE])
      emp_band_ch <- emp_band_ch[is.finite(emp_band_ch)]
      emp_band_kurt <- if (length(emp_band_ch) > 20L) excess_kurtosis(emp_band_ch) else NA_real_

      bm <- which(bp_avg_rk_global >= lo & bp_avg_rk_global <= hi)
      sim_band_kurt <- NA_real_
      if (length(bm) > 5L) {
        sim_band_ch <- as.vector(sim_ch[, bm, drop = FALSE])
        sim_band_ch <- sim_band_ch[is.finite(sim_band_ch)]
        if (length(sim_band_ch) > 20L) sim_band_kurt <- excess_kurtosis(sim_band_ch)
      }

      mc_key <- paste0("kurt_", lo, "_", hi)
      if (mc_key %in% names(mc_stats)) {
        s <- mc_stats[[mc_key]]
        cat(sprintf("    %5d-%5d: emp=%6.2f sim_mean=%6.2f [%.2f, %.2f] (rep42=%.2f)\n",
                    lo, hi, emp_band_kurt, s$mean, s$lo, s$hi, sim_band_kurt))
      }
    }

    cat("\n  Volatility Clustering (ACF of |changes| and changes^2):\n")
    for (lag_k in c(1L, 2L, 4L, 8L)) {
      emp_abs <- vapply(sample_idx[seq_len(min(length(sample_idx), 500L))],
                        function(j) lag_autocor(abs(log_changes[, j]), lag_k), numeric(1))
      emp_sq <- vapply(sample_idx[seq_len(min(length(sample_idx), 500L))],
                       function(j) lag_autocor(log_changes[, j]^2, lag_k), numeric(1))

      sim_abs <- vapply(seq_len(min(500L, n_bp)), function(j) lag_autocor(abs(sim_ch[, j]), lag_k), numeric(1))
      sim_sq <- vapply(seq_len(min(500L, n_bp)), function(j) lag_autocor(sim_ch[, j]^2, lag_k), numeric(1))

      cat(sprintf("    Lag %d: |dy| ACF emp=%.4f sim=%.4f  dy^2 ACF emp=%.4f sim=%.4f\n",
                  lag_k, safe_median(emp_abs), safe_median(sim_abs), safe_median(emp_sq), safe_median(sim_sq)))
    }

    # -----------------------------------------------------------------------
    # Plot set 1: Core diagnostics
    # -----------------------------------------------------------------------
    if (!skip_plots) {
      cat("\n\nGenerating plots...\n")

    png(file.path(base_dir, "v42_diagnostics.png"), width = 2200, height = 2800, res = 130)
    old_par <- par(no.readonly = TRUE)
    on.exit(par(old_par), add = TRUE)

    par(mfrow = c(5, 3), mar = c(3.5, 3.5, 2.5, 1), mgp = c(2, 0.7, 0))

    # VR
    vr_ks <- sort(as.integer(names(vr_emp)))
    vr_ks <- vr_ks[vr_ks <= 52]
    plot(vr_ks, unlist(vr_emp[as.character(vr_ks)]), type = "b", pch = 16, col = "black",
         xlab = "Horizon", ylab = "VR", main = "Variance Ratio")
    sim_vr <- vapply(vr_ks, function(k) {
      if (k >= nrow(bp_ly)) return(NA_real_)
      d_k <- matrix_diff_lag(bp_ly, k)
      v_k <- apply(d_k, 2, stats::var, na.rm = TRUE)
      safe_median(v_k / (k * pmax(sim_v1, 1e-12)))
    }, numeric(1))
    lines(vr_ks, sim_vr, type = "b", pch = 17, lty = 2, col = "red")
    legend("topright", legend = c("Emp", "Sim"), col = c("black", "red"), pch = c(16, 17), bty = "n", cex = 0.8)

    # ACF
    acf_lags <- c(1, 2, 3, 4)
    emp_acf_vals <- unlist(acf_emp[as.character(acf_lags)])
    sim_acf_vals <- vapply(acf_lags, function(l) {
      vals <- vapply(seq_len(min(500L, n_bp)), function(j) lag_autocor(sim_ch[, j], l), numeric(1))
      safe_median(vals)
    }, numeric(1))
    barplot(rbind(emp_acf_vals, sim_acf_vals), beside = TRUE, col = c("gray20", "firebrick"),
            names.arg = acf_lags, main = "ACF of Changes", xlab = "Lag")
    abline(h = 0, lwd = 0.7)

    # RACF
    racf_lags <- c(1, 4, 13, 26)
    emp_racf_vals <- unlist(racf_emp[as.character(racf_lags)])
    sim_racf_vals <- vapply(racf_lags, function(l) {
      vals <- vapply(seq_len(min(1000L, n_bp)), function(j) lag_autocor(bp_rk[, j], l), numeric(1))
      safe_median(vals)
    }, numeric(1))
    barplot(rbind(emp_racf_vals, sim_racf_vals), beside = TRUE, col = c("gray20", "firebrick"),
            names.arg = racf_lags, main = "Rank ACF", xlab = "Lag")
    abline(h = 0, lwd = 0.7)

    # R2
    r2_k <- c(1, 4, 13, 26, 52)
    emp_r2_vals <- unlist(xr2_emp[as.character(r2_k)])
    sim_r2_vals <- vapply(r2_k, function(k) {
      if (k + 1L > nrow(bp_ly)) return(NA_real_)
      stats::cor(bp_ly[1, ], bp_ly[k + 1L, ])^2
    }, numeric(1))
    plot(r2_k, emp_r2_vals, type = "b", pch = 16, col = "black", ylim = range(c(emp_r2_vals, sim_r2_vals), na.rm = TRUE),
         main = "Cross-Sectional R2", xlab = "Horizon", ylab = "R2")
    lines(r2_k, sim_r2_vals, type = "b", pch = 17, lty = 2, col = "red")

    # Persistence
    pers_k <- c(1, 4, 13, 26, 52)
    emp_pers <- unlist(pers_emp[as.character(pers_k)])
    sim_pers <- vapply(pers_k, function(k) {
      if (k + 1L > nrow(sim_rk_full)) return(NA_real_)
      t0 <- sim_ids_full[1, sim_rk_full[1, ] <= 100]
      tk <- sim_ids_full[k + 1L, sim_rk_full[k + 1L, ] <= 100]
      length(intersect(t0, tk))
    }, numeric(1))
    plot(pers_k, emp_pers, type = "b", pch = 16, col = "black", ylim = range(c(emp_pers, sim_pers), na.rm = TRUE),
         main = "Top-100 Persistence", xlab = "Horizon", ylab = "Count")
    lines(pers_k, sim_pers, type = "b", pch = 17, lty = 2, col = "red")

    # Distribution
    hist(clip_num(all_ch_emp, -3, 3), breaks = 100, freq = FALSE, col = rgb(0, 0, 0, 0.35), border = NA,
         main = sprintf("Changes (kurt=%.1f / %.1f)", mc_stats$kurtosis$mean, emp_kurt),
         xlab = "d log(y)")
    hist(clip_num(sim_ch_flat, -3, 3), breaks = 100, freq = FALSE, add = TRUE,
         col = rgb(1, 0, 0, 0.35), border = NA)

    # Cross-sectional variance
    emp_xsec_ts <- apply(log_metric, 1, stats::var, na.rm = TRUE)
    sim_bp_xsec <- apply(bp_ly, 1, stats::var, na.rm = TRUE)
    plot(seq_len(t_sim), xsec_vars[(t_burnin_default + 1L):(t_burnin_default + t_sim)], type = "l", lwd = 2,
         col = "red", main = "Cross-Sectional Variance", xlab = "Week", ylab = "Variance")
    lines(seq_len(t_sim), emp_xsec_ts, col = "black", lwd = 2)
    lines(seq_len(t_sim), sim_bp_xsec, col = "red", lty = 2, lwd = 2)
    legend("topright", legend = c("Sim tau", "Emp BP", "Sim BP"),
           col = c("red", "black", "red"), lty = c(1, 1, 2), bty = "n", cex = 0.8)

    # Band variance
    bc_mid <- sqrt(bands$lo * bands$hi)
    ev <- band_stats$var
    sim_band_var <- vapply(seq_len(nrow(bands)), function(i) {
      bm <- which(bp_avg_rk_global >= bands$lo[i] & bp_avg_rk_global <= bands$hi[i])
      if (length(bm) <= 5L) return(NA_real_)
      safe_median(apply(sim_ch[, bm, drop = FALSE], 2, stats::var, na.rm = TRUE))
    }, numeric(1))
    plot(bc_mid, ev, type = "b", pch = 16, col = "black", log = "x",
         main = "Band Variance", xlab = "Rank (geometric midpoint)", ylab = "Variance")
    lines(bc_mid, sim_band_var, type = "b", pch = 17, lty = 2, col = "red")

    # Band VR4
    ev4 <- band_stats$vr4
    sim_band_vr4 <- vapply(seq_len(nrow(bands)), function(i) {
      bm <- which(bp_avg_rk_global >= bands$lo[i] & bp_avg_rk_global <= bands$hi[i])
      if (length(bm) <= 5L) return(NA_real_)
      bdf <- bp_ly[, bm, drop = FALSE]
      bch <- sim_ch[, bm, drop = FALSE]
      v4 <- apply(matrix_diff_lag(bdf, 4L), 2, stats::var, na.rm = TRUE)
      v1 <- apply(bch, 2, stats::var, na.rm = TRUE)
      safe_median(v4 / (4 * pmax(v1, 1e-12)))
    }, numeric(1))
    plot(bc_mid, ev4, type = "b", pch = 16, col = "black", log = "x",
         main = "Band VR(4)", xlab = "Rank (geometric midpoint)", ylab = "VR(4)")
    lines(bc_mid, sim_band_vr4, type = "b", pch = 17, lty = 2, col = "red")

    # Trajectories
    idx_traj <- c(1L, max(1L, floor(n_bp / 2)), n_bp)
    for (idx in idx_traj) {
      plot(seq_len(t_sim), bp_rk[, idx], type = "l", col = "red", lwd = 1.2,
           main = sprintf("Rank trajectory: endpoint %d", idx), xlab = "Week", ylab = "Rank")
      ylim <- par("usr")[3:4]
      par(new = TRUE)
      plot(seq_len(t_sim), rev(bp_rk[, idx]), type = "n", axes = FALSE, xlab = "", ylab = "", ylim = ylim)
      par(new = FALSE)
    }

    # MC histograms
    hist(diag_df$pers13, breaks = 15, col = "steelblue", border = "white",
         main = "Pers(13) MC dist", xlab = "Value")
    abline(v = pers_emp[["13"]], col = "black", lty = 2, lwd = 2)
    abline(v = mean(diag_df$pers13, na.rm = TRUE), col = "red", lwd = 2)

    hist(diag_df$kurtosis, breaks = 15, col = "steelblue", border = "white",
         main = "Kurtosis MC dist", xlab = "Value")
    abline(v = emp_kurt, col = "black", lty = 2, lwd = 2)
    abline(v = mean(diag_df$kurtosis, na.rm = TRUE), col = "red", lwd = 2)

    hist(diag_df$racf1, breaks = 15, col = "steelblue", border = "white",
         main = "RACF(1) MC dist", xlab = "Value")
    abline(v = racf_emp[["1"]], col = "black", lty = 2, lwd = 2)
    abline(v = mean(diag_df$racf1, na.rm = TRUE), col = "red", lwd = 2)

    dev.off()
    cat("Saved v42_diagnostics.png\n")

    # -----------------------------------------------------------------------
    # Plot set 2: Publication diagnostics
    # -----------------------------------------------------------------------
    png(file.path(base_dir, "v42_pub_diagnostics.png"), width = 2400, height = 3200, res = 130)
    old_par2 <- par(no.readonly = TRUE)
    on.exit(par(old_par2), add = TRUE)
    par(mfrow = c(5, 3), mar = c(3.5, 3.5, 2.5, 1), mgp = c(2, 0.7, 0))

    # QQ simulated residuals vs fitted t
    n_qq <- min(10000L, length(sim_std_resid))
    qq_sample <- sort(sample(sim_std_resid, n_qq, replace = FALSE))
    p_grid <- seq(0.001, 0.999, length.out = n_qq)
    theo <- stats::qt(p_grid, df = t_df) * scale_fit + loc_fit
    plot(theo, qq_sample, pch = 16, cex = 0.3, col = rgb(0.2, 0.4, 0.8, 0.4),
         xlab = sprintf("t(df=%.1f) quantiles", t_df), ylab = "Sim std residuals",
         main = "QQ: Sim Residuals")
    abline(0, 1, col = "red", lwd = 1.5)

    # Zipf plot
    emp_w0 <- df %>% filter(date == dates[1]) %>% arrange(rank)
    emp_mask <- emp_w0$metric_value > 0 & emp_w0$rank <= 10000
    plot(log10(emp_w0$rank[emp_mask]), log10(emp_w0$metric_value[emp_mask]),
         pch = 16, cex = 0.3, col = rgb(0, 0, 0, 0.3),
         xlab = "log10(Rank)", ylab = "log10(Value)", main = "Zipf Rank-Size")
    sim_y0_vals <- exp(sim_ly[1, ])
    sim_sorted <- sort(sim_y0_vals, decreasing = TRUE)
    sim_rank <- seq_along(sim_sorted)
    sim_mask <- sim_sorted > 0 & sim_rank <= 10000
    points(log10(sim_rank[sim_mask]), log10(sim_sorted[sim_mask]), pch = 16, cex = 0.3, col = rgb(1, 0, 0, 0.3))

    # Innovation density (log scale)
    bins <- seq(-5, 5, length.out = 200)
    emp_hist <- hist(clip_num(all_ch_emp, -5, 5), breaks = bins, plot = FALSE)
    sim_hist <- hist(clip_num(sim_ch_flat, -5, 5), breaks = bins, plot = FALSE)
    centers <- emp_hist$mids
    emp_d <- pmax(emp_hist$density, 1e-6)
    sim_d <- pmax(sim_hist$density, 1e-6)
    plot(centers, emp_d, type = "l", log = "y", col = "black", lwd = 2,
         xlab = "d log(y)", ylab = "Density (log)", main = "Innovation Density")
    lines(centers, sim_d, col = "red", lwd = 2)

    # Transition heatmaps
    image(t(apply(sim_trans_13[nrow(sim_trans_13):1, ], 2, rev)), axes = FALSE,
          col = heat.colors(50), main = "Sim 13w Transition")
    axis(1, at = seq(0, 1, length.out = n_quintiles), labels = paste0("Q", 1:5))
    axis(2, at = seq(0, 1, length.out = n_quintiles), labels = paste0("Q", 5:1))

    image(t(apply(emp_trans_13[nrow(emp_trans_13):1, ], 2, rev)), axes = FALSE,
          col = heat.colors(50), main = "Emp 13w Transition")
    axis(1, at = seq(0, 1, length.out = n_quintiles), labels = paste0("Q", 1:5))
    axis(2, at = seq(0, 1, length.out = n_quintiles), labels = paste0("Q", 5:1))

    # CCDF
    emp_abs <- sort(abs(all_ch_emp), decreasing = TRUE)
    sim_abs <- sort(abs(sim_ch_flat), decreasing = TRUE)
    emp_ccdf <- seq_along(emp_abs) / length(emp_abs)
    sim_ccdf <- seq_along(sim_abs) / length(sim_abs)
    n_plot <- min(5000L, length(emp_abs), length(sim_abs))
    emp_idx <- round(seq(1, length(emp_abs), length.out = n_plot))
    sim_idx <- round(seq(1, length(sim_abs), length.out = n_plot))
    plot(emp_abs[emp_idx], emp_ccdf[emp_idx], log = "xy", type = "l", col = "black", lwd = 2,
         xlab = "|d log(y)|", ylab = "P(|d log(y)| > x)", main = "CCDF")
    lines(sim_abs[sim_idx], sim_ccdf[sim_idx], col = "red", lwd = 2)

    # Hill plot
    ks_range <- unique(round(exp(seq(log(10), log(length(all_ch_emp) / 5), length.out = 80))))
    emp_h <- vapply(ks_range, function(k) hill_estimator(all_ch_emp, k)$alpha, numeric(1))
    sim_h <- vapply(ks_range, function(k) hill_estimator(sim_ch_flat, k)$alpha, numeric(1))
    plot(ks_range, emp_h, log = "x", type = "l", col = "black", lwd = 2,
         xlab = "k", ylab = "alpha_hat", main = "Hill Plot")
    lines(ks_range, sim_h, col = "red", lwd = 2)
    abline(h = t_df, col = "blue", lty = 2)

    # Volatility bars
    vol_lags <- c(1, 2, 4, 8)
    emp_abs_acf <- vapply(vol_lags, function(l) {
      vals <- vapply(sample_idx[seq_len(min(500L, sample_n))], function(j) lag_autocor(abs(log_changes[, j]), l), numeric(1))
      safe_median(vals)
    }, numeric(1))
    sim_abs_acf <- vapply(vol_lags, function(l) {
      vals <- vapply(seq_len(min(500L, n_bp)), function(j) lag_autocor(abs(sim_ch[, j]), l), numeric(1))
      safe_median(vals)
    }, numeric(1))
    emp_sq_acf <- vapply(vol_lags, function(l) {
      vals <- vapply(sample_idx[seq_len(min(500L, sample_n))], function(j) lag_autocor(log_changes[, j]^2, l), numeric(1))
      safe_median(vals)
    }, numeric(1))
    sim_sq_acf <- vapply(vol_lags, function(l) {
      vals <- vapply(seq_len(min(500L, n_bp)), function(j) lag_autocor(sim_ch[, j]^2, l), numeric(1))
      safe_median(vals)
    }, numeric(1))
    barplot(rbind(emp_abs_acf, sim_abs_acf, emp_sq_acf, sim_sq_acf), beside = TRUE,
            col = c("black", "red", "gray50", "salmon"), names.arg = vol_lags,
            main = "Volatility Clustering", xlab = "Lag")
    abline(h = 0, lwd = 0.7)

    # Kurtosis by band
    emp_k_b <- numeric(nrow(bands))
    sim_k_b <- numeric(nrow(bands))
    for (i in seq_len(nrow(bands))) {
      lo <- bands$lo[i]
      hi <- bands$hi[i]
      idx_emp <- endpoint_to_col[endpoint_cols[avg_rank >= lo & avg_rank <= hi]]
      emp_band <- as.vector(log_changes[, idx_emp, drop = FALSE])
      emp_band <- emp_band[is.finite(emp_band)]
      emp_k_b[i] <- if (length(emp_band) > 20L) excess_kurtosis(emp_band) else NA_real_

      idx_sim <- which(bp_avg_rk_global >= lo & bp_avg_rk_global <= hi)
      sim_band <- as.vector(sim_ch[, idx_sim, drop = FALSE])
      sim_band <- sim_band[is.finite(sim_band)]
      sim_k_b[i] <- if (length(sim_band) > 20L) excess_kurtosis(sim_band) else NA_real_
    }
    mids <- seq_len(nrow(bands))
    barplot(rbind(emp_k_b, sim_k_b), beside = TRUE, col = c("black", "red"),
            names.arg = paste0(bands$lo, "-", bands$hi), las = 2,
            main = "Kurtosis by Rank Band", ylab = "Excess kurtosis")

    # CDC
    cdc_times <- c(1L, max(1L, floor(t_sim / 4)), max(1L, floor(t_sim / 2)), max(1L, floor(3 * t_sim / 4)), t_sim)
    cdc_cols <- c("navy", "steelblue", "green4", "orange", "red")
    plot(NA, xlim = c(0, 50), ylim = c(0, 100), xlab = "Rank percentile", ylab = "Cumulative share (%)", main = "Capital Distribution Curves")
    for (i in seq_along(cdc_times)) {
      ti <- cdc_times[i]
      vals <- exp(sim_ly_full[ti, ])
      srt <- sort(vals, decreasing = TRUE)
      cum_share <- cumsum(srt) / sum(srt)
      rank_pct <- seq_along(srt) / length(srt) * 100
      lines(rank_pct, 100 * cum_share, col = cdc_cols[i], lwd = 1.5)
    }
    emp_vals <- df %>% filter(date == dates[1]) %>% arrange(rank) %>% pull(metric_value)
    emp_vals <- emp_vals[emp_vals > 0]
    emp_sorted <- sort(emp_vals, decreasing = TRUE)
    emp_cum <- cumsum(emp_sorted) / sum(emp_sorted)
    emp_pct <- seq_along(emp_sorted) / length(emp_sorted) * 100
    lines(emp_pct, 100 * emp_cum, col = "black", lty = 2, lwd = 2)

    # Survival curves
    top_ks <- c(50, 100, 200, 500)
    plot(NA, xlim = c(1, t_sim), ylim = c(0, 100), xlab = "Weeks", ylab = "Survival (%)",
         main = "Top-K Persistence Survival")
    cols <- c("navy", "steelblue", "green4", "orange")
    for (i in seq_along(top_ks)) {
      k_top <- top_ks[i]
      t0_ids <- sim_ids_full[1, sim_rk_full[1, ] <= k_top]
      n_init <- length(t0_ids)
      surv <- numeric(t_sim)
      for (wk in seq_len(t_sim)) {
        tw_ids <- sim_ids_full[wk, sim_rk_full[wk, ] <= k_top]
        surv[wk] <- length(intersect(t0_ids, tw_ids)) / n_init * 100
      }
      lines(seq_len(t_sim), surv, col = cols[i], lwd = 1.5)

      t0_emp <- which(emp_rk_arr[1, ] <= k_top)
      n_emp <- length(t0_emp)
      surv_emp <- numeric(nrow(emp_rk_arr))
      for (wk in seq_len(nrow(emp_rk_arr))) {
        tw_emp <- which(emp_rk_arr[wk, ] <= k_top)
        surv_emp[wk] <- length(intersect(t0_emp, tw_emp)) / n_emp * 100
      }
      lines(seq_len(nrow(emp_rk_arr)), surv_emp, col = cols[i], lty = 2, lwd = 1.2)
    }
    abline(h = 50, lty = 3)

    # QQ empirical residuals
    n_qq_emp <- min(10000L, length(z_within))
    qq_emp <- sort(sample(z_within, n_qq_emp, replace = FALSE))
    theo_emp <- stats::qt(seq(0.001, 0.999, length.out = n_qq_emp), df = t_df) * scale_fit + loc_fit
    plot(theo_emp, qq_emp, pch = 16, cex = 0.3, col = rgb(0, 0, 0, 0.4),
         xlab = sprintf("t(df=%.1f) quantiles", t_df), ylab = "Emp std residuals", main = "QQ: Emp Residuals")
    abline(0, 1, col = "red", lwd = 1.5)

    # Rank-rank
    plot(NA, xlim = c(1, n_full), ylim = c(1, n_full), xlab = "Initial rank", ylab = "Mean rank at k",
         main = "Rank-Rank Regression")
    cols_rr <- c("navy", "green4", "orange", "red")
    ks_rr <- c(1L, 4L, 13L, 26L)
    for (ii in seq_along(ks_rr)) {
      k <- ks_rr[ii]
      if (k + 1L > nrow(bp_rk)) next
      r0 <- bp_rk[1, ]
      rk <- bp_rk[k + 1L, ]
      n_bins <- 50
      edges <- seq(1, n_full + 1, length.out = n_bins + 1L)
      x_mean <- numeric(n_bins)
      y_mean <- numeric(n_bins)
      for (b in seq_len(n_bins)) {
        mask <- r0 >= edges[b] & r0 < edges[b + 1L]
        x_mean[b] <- mean(r0[mask])
        y_mean[b] <- mean(rk[mask])
      }
      lines(x_mean, y_mean, col = cols_rr[ii], lwd = 1.5)
    }
    abline(0, 1, lty = 2)

    # Cross-sectional density snapshots
    hist(sim_ly_full[1, ], breaks = 80, freq = FALSE, col = rgb(0, 0, 0.6, 0.3), border = NA,
         main = "Cross-sectional Density", xlab = "log(1+value)")
    hist(sim_ly_full[max(1L, floor(t_sim / 2)), ], breaks = 80, freq = FALSE,
         col = rgb(0, 0.6, 0, 0.3), border = NA, add = TRUE)
    hist(sim_ly_full[t_sim, ], breaks = 80, freq = FALSE,
         col = rgb(1, 0, 0, 0.3), border = NA, add = TRUE)
    hist(log_metric[1, ], breaks = 80, freq = FALSE, add = TRUE, border = "black", col = NA, lwd = 2)

    # Shorrocks vs horizon
    horizons <- c(1, 2, 4, 8, 13, 26, 52)
    sh_sim <- vapply(horizons, function(h) {
      if (h + 1L > nrow(bp_rk)) return(NA_real_)
      st <- compute_transition_matrix(bp_rk, n_quintiles, h)
      (n_quintiles - sum(diag(st))) / (n_quintiles - 1)
    }, numeric(1))
    sh_emp <- vapply(horizons, function(h) {
      if (h + 1L > nrow(emp_rk_arr)) return(NA_real_)
      st <- compute_transition_matrix(emp_rk_arr, n_quintiles, h)
      (n_quintiles - sum(diag(st))) / (n_quintiles - 1)
    }, numeric(1))
    plot(horizons, sh_emp, type = "b", pch = 16, col = "black", ylim = range(c(sh_emp, sh_sim), na.rm = TRUE),
         xlab = "Horizon (weeks)", ylab = "Shorrocks M", main = "Shorrocks Mobility")
    lines(horizons, sh_sim, type = "b", pch = 17, lty = 2, col = "red")

      dev.off()
      cat("Saved v42_pub_diagnostics.png\n")
    } else {
      cat("\nSkipping plot generation (V42_SKIP_PLOTS=true).\n")
    }
  }

  t_phase1 <- Sys.time()
  print_rule("PHASE 1 COMPLETE - Core simulation + publication diagnostics")
  cat(sprintf("  Calibration: %d/%d diagnostics pass\n", n_pass, length(tests)))
  cat(sprintf("  Elapsed: %.0fs\n", as.numeric(difftime(t_phase1, t_start, units = "secs"))))

  # ---------------------------------------------------------------------------
  # Phase 2: Ablation study
  # ---------------------------------------------------------------------------
  print_rule("PHASE 2: ABLATION STUDY (v4.2)")

  ablation_levels <- list(
    list(name = "1. Base (PT+Gauss)", short = "Base",
         config = list(burn_in = FALSE, kappa = FALSE, rank_dep_kappa = FALSE,
                       kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
                       rank_dep_tdf = FALSE, calibrated_tdf = FALSE)),
    list(name = "2. +Burn-in", short = "+Burn-in",
         config = list(burn_in = TRUE, kappa = FALSE, rank_dep_kappa = FALSE,
                       kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
                       rank_dep_tdf = FALSE, calibrated_tdf = FALSE)),
    list(name = "3. +kappa (global)", short = "+kappa",
         config = list(burn_in = TRUE, kappa = TRUE, rank_dep_kappa = FALSE,
                       kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
                       rank_dep_tdf = FALSE, calibrated_tdf = FALSE)),
    list(name = "4. +kappa(r)", short = "+kappa(r)",
         config = list(burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
                       kappa_stab = FALSE, heavy_tails = FALSE, arch = FALSE,
                       rank_dep_tdf = FALSE, calibrated_tdf = FALSE)),
    list(name = "5. +Heavy tails", short = "+Tails",
         config = list(burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
                       kappa_stab = FALSE, heavy_tails = TRUE, arch = FALSE,
                       rank_dep_tdf = FALSE, calibrated_tdf = FALSE)),
    list(name = "6. +ARCH(1)", short = "+ARCH",
         config = list(burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
                       kappa_stab = FALSE, heavy_tails = TRUE, arch = TRUE,
                       rank_dep_tdf = FALSE, calibrated_tdf = FALSE)),
    list(name = "7. +Rank-dep t_df", short = "+Rank-tdf",
         config = list(burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
                       kappa_stab = FALSE, heavy_tails = TRUE, arch = TRUE,
                       rank_dep_tdf = TRUE, calibrated_tdf = TRUE)),
    list(name = "8. +kappa-stab (v3.9)", short = "Full v3.9",
         config = list(burn_in = TRUE, kappa = TRUE, rank_dep_kappa = TRUE,
                       kappa_stab = TRUE, heavy_tails = TRUE, arch = TRUE,
                       rank_dep_tdf = TRUE, calibrated_tdf = TRUE))
  )

  abl_diag_names <- c(
    "VR(2)", "VR(4)", "VR(8)", "VR(13)",
    "ACF(1)", "ACF(2)",
    "RACF(1)", "RACF(4)", "RACF(13)",
    "Pers(1)", "Pers(4)", "Pers(13)",
    "R2(1)", "R2(4)", "R2(13)"
  )

  abl_diag_keys <- c(
    "vr2", "vr4", "vr8", "vr13",
    "acf1", "acf2",
    "racf1", "racf4", "racf13",
    "pers1", "pers4", "pers13",
    "xr2_1", "xr2_4", "xr2_13"
  )

  abl_emp_vals <- c(
    vr_emp[["2"]], vr_emp[["4"]], vr_emp[["8"]], vr_emp[["13"]],
    acf_emp[["1"]], acf_emp[["2"]],
    racf_emp[["1"]], racf_emp[["4"]], racf_emp[["13"]],
    pers_emp[["1"]], pers_emp[["4"]], pers_emp[["13"]],
    xr2_emp[["1"]], xr2_emp[["4"]], xr2_emp[["13"]]
  )

  abl_passes <- function(key, sim_val, emp_val) {
    if (startsWith(key, "vr")) {
      abs(sim_val - emp_val) / emp_val < 0.20
    } else if (startsWith(key, "pers")) {
      abs(sim_val - emp_val) < 10
    } else {
      abs(sim_val - emp_val) < 0.08
    }
  }

  abl_seeds <- c(42L, seq.int(100L, 100L + n_rep - 2L))
  all_abl_results <- vector("list", length(ablation_levels))

  for (lvl_idx in seq_along(ablation_levels)) {
    level <- ablation_levels[[lvl_idx]]
    cat(sprintf("\n  Level %d: %s\n", lvl_idx, level$name))

    lvl_diags <- vector("list", n_rep)
    for (ri in seq_along(abl_seeds)) {
      t0_abl <- Sys.time()
      d <- run_sim(abl_seeds[ri], config = level$config)$diag
      lvl_diags[[ri]] <- d
      if (ri == 1L || ri == n_rep) {
        cat(sprintf("    Rep %d/%d: %.1fs\n", ri, n_rep, as.numeric(difftime(Sys.time(), t0_abl, units = "secs"))))
      }
    }

    lvl_df <- bind_rows(lapply(lvl_diags, as_row_tibble))

    mc_abl <- list()
    for (key in abl_diag_keys) {
      vals <- lvl_df[[key]]
      vals <- vals[is.finite(vals)]
      mc_abl[[key]] <- if (length(vals) > 0L) mean(vals) else NA_real_
    }

    mc_abl$kurtosis <- mean(lvl_df$kurtosis, na.rm = TRUE)
    mc_abl$xsec_var_drift <- mean(lvl_df$xsec_var_drift, na.rm = TRUE)

    pass_fail <- list()
    for (i in seq_along(abl_diag_keys)) {
      key <- abl_diag_keys[i]
      pass_fail[[key]] <- if (is.finite(mc_abl[[key]])) abl_passes(key, mc_abl[[key]], abl_emp_vals[i]) else FALSE
    }

    mc_abl$n_pass <- sum(unlist(pass_fail))
    mc_abl$pass_fail <- pass_fail

    all_abl_results[[lvl_idx]] <- list(level = level, mc = mc_abl)

    score <- mc_abl$n_pass
    fails <- abl_diag_names[!unlist(pass_fail)]
    fail_str <- if (length(fails) == 0L) "(none)" else paste(fails, collapse = ", ")

    cat(sprintf("    Score: %d/15  |  Fails: %s\n", score, fail_str))
    cat(sprintf("    Kurtosis: %.1f  Var drift: %.2f\n", mc_abl$kurtosis, mc_abl$xsec_var_drift))
  }

  print_rule("ABLATION SUMMARY TABLE")

  header <- sprintf("%-24s %5s", "Level", "Score")
  for (nm in abl_diag_names) header <- paste0(header, sprintf(" %7s", nm))
  header <- paste0(header, sprintf(" %6s %6s", "Kurt", "VarDr"))
  cat(header, "\n")
  cat(strrep("-", nchar(header)), "\n")

  for (res in all_abl_results) {
    mc_abl <- res$mc
    pf <- mc_abl$pass_fail
    row <- sprintf("%-24s %2d/15", res$level$short, mc_abl$n_pass)
    for (key in abl_diag_keys) {
      row <- paste0(row, sprintf(" %7s", if (isTRUE(pf[[key]])) "Y" else "N"))
    }
    row <- paste0(row, sprintf(" %6.1f %6.2f", mc_abl$kurtosis, mc_abl$xsec_var_drift))
    cat(row, "\n")
  }

  print_rule("FEATURE CONTRIBUTION")
  for (i in 2:length(all_abl_results)) {
    prev_pf <- all_abl_results[[i - 1L]]$mc$pass_fail
    curr_pf <- all_abl_results[[i]]$mc$pass_fail

    newly_passing <- abl_diag_names[vapply(seq_along(abl_diag_keys), function(j) {
      key <- abl_diag_keys[j]
      isTRUE(curr_pf[[key]]) && !isTRUE(prev_pf[[key]])
    }, logical(1))]

    newly_failing <- abl_diag_names[vapply(seq_along(abl_diag_keys), function(j) {
      key <- abl_diag_keys[j]
      !isTRUE(curr_pf[[key]]) && isTRUE(prev_pf[[key]])
    }, logical(1))]

    fixed_str <- if (length(newly_passing)) paste(newly_passing, collapse = ", ") else "(none)"
    broke_str <- if (length(newly_failing)) paste(newly_failing, collapse = ", ") else "(none)"
    delta <- all_abl_results[[i]]$mc$n_pass - all_abl_results[[i - 1L]]$mc$n_pass

    cat(sprintf("  %s\n", all_abl_results[[i]]$level$name))
    cat(sprintf("    Fixed: %s  |  Broke: %s  |  Delta: %+d\n", fixed_str, broke_str, delta))
  }

  if (!skip_plots) {
    cat("\nGenerating ablation figure...\n")
    png(file.path(base_dir, "v42_ablation.png"), width = 1800, height = 800, res = 200)

    layout(matrix(c(1, 2), nrow = 1), widths = c(3, 1))

    n_lvls <- length(all_abl_results)
    n_diags <- length(abl_diag_names)
    hm <- matrix(0, nrow = n_lvls, ncol = n_diags)
    for (i in seq_len(n_lvls)) {
      for (j in seq_len(n_diags)) {
        hm[i, j] <- if (isTRUE(all_abl_results[[i]]$mc$pass_fail[[abl_diag_keys[j]]])) 1 else 0
      }
    }

    image(t(hm[n_lvls:1, , drop = FALSE]), axes = FALSE,
          col = c("#d32f2f", "#4caf50"), main = "Ablation: Pass/Fail by Model Level")
    axis(1, at = seq(0, 1, length.out = n_diags), labels = abl_diag_names, las = 2, cex.axis = 0.7)
    axis(2, at = seq(0, 1, length.out = n_lvls), labels = rev(vapply(all_abl_results, function(x) x$level$short, character(1))), las = 2, cex.axis = 0.8)

    scores_abl <- vapply(all_abl_results, function(x) x$mc$n_pass, numeric(1))
    score_cols <- ifelse(scores_abl == 15, "#4caf50", ifelse(scores_abl >= 12, "#ff9800", "#d32f2f"))
    barplot(scores_abl, horiz = TRUE, col = score_cols, border = NA,
            main = "Score", xlim = c(0, 16), names.arg = rep("", n_lvls))
    abline(v = 15, col = "darkgreen", lty = 2)

    dev.off()
    cat("  Saved: v42_ablation.png\n")
  }

  # ---------------------------------------------------------------------------
  # Phase 3: Sensitivity
  # ---------------------------------------------------------------------------
  print_rule("PHASE 3: PARAMETER SENSITIVITY ANALYSIS (v4.2)")

  sens_seeds <- c(42L, seq.int(300L, 300L + n_rep - 2L))

  sens_params <- tribble(
    ~name, ~var, ~base,
    "sigma_obs", "sigma_obs", sigma_obs,
    "sigma_het", "sigma_het", sigma_het,
    "kappa_base", "kappa_base", kappa_base,
    "alpha_kappa", "alpha_kappa", alpha_kappa,
    "alpha_arch", "alpha_arch", alpha_arch,
    "t_df_global", "t_df_global", t_df_global
  )

  perturbations <- c(-0.20, -0.10, 0.0, 0.10, 0.20)
  all_sens_results <- list()

  for (i in seq_len(nrow(sens_params))) {
    pname <- sens_params$name[i]
    pvar <- sens_params$var[i]
    pbase <- sens_params$base[i]

    cat(sprintf("\n  Parameter: %s (baseline = %.4f)\n", pname, pbase))
    all_sens_results[[pname]] <- list()

    for (delta in perturbations) {
      pval <- pbase * (1 + delta)
      overrides <- list()
      overrides[[pvar]] <- pval

      diags_list <- vector("list", length(sens_seeds))
      for (j in seq_along(sens_seeds)) {
        diags_list[[j]] <- run_sim(sens_seeds[j], config = overrides)$diag
      }

      ddf <- bind_rows(lapply(diags_list, as_row_tibble))

      mc_s <- list()
      for (key in abl_diag_keys) {
        vals <- ddf[[key]]
        vals <- vals[is.finite(vals)]
        mc_s[[key]] <- if (length(vals) > 0L) mean(vals) else NA_real_
      }

      pass_fail <- list()
      for (k in seq_along(abl_diag_keys)) {
        key <- abl_diag_keys[k]
        pass_fail[[key]] <- if (is.finite(mc_s[[key]])) abl_passes(key, mc_s[[key]], abl_emp_vals[k]) else FALSE
      }

      mc_s$n_pass <- sum(unlist(pass_fail))
      mc_s$pass_fail <- pass_fail
      all_sens_results[[pname]][[as.character(delta)]] <- mc_s

      fails <- abl_diag_names[!unlist(pass_fail)]
      tag <- if (delta == 0) " (BASELINE)" else ""
      cat(sprintf("    %5s: val=%.4f  score=%d/15  fails=%s%s\n",
                  sprintf("%+.0f%%", 100 * delta), pval, mc_s$n_pass,
                  if (length(fails)) paste(fails, collapse = ", ") else "(none)", tag))
    }
  }

  print_rule("PARAMETER SENSITIVITY SUMMARY")
  cat("\nScore (/15) at each perturbation level:\n\n")

  hdr_s <- sprintf("%-14s", "Parameter")
  for (d in perturbations) hdr_s <- paste0(hdr_s, sprintf(" %5s", sprintf("%+.0f%%", d * 100)))
  cat(hdr_s, "\n")
  cat(strrep("-", nchar(hdr_s)), "\n")

  for (i in seq_len(nrow(sens_params))) {
    pname <- sens_params$name[i]
    row <- sprintf("%-14s", pname)
    for (d in perturbations) {
      sc <- all_sens_results[[pname]][[as.character(d)]]$n_pass
      marker <- if (sc < 15) "*" else " "
      row <- paste0(row, sprintf("  %2d%s ", sc, marker))
    }
    cat(row, "\n")
  }

  cat("\n\nDiagnostic sensitivity (fails at +/-20% perturbation):\n\n")
  for (i in seq_len(nrow(sens_params))) {
    pname <- sens_params$name[i]
    fails_m20 <- abl_diag_names[!unlist(all_sens_results[[pname]][[as.character(-0.20)]]$pass_fail)]
    fails_p20 <- abl_diag_names[!unlist(all_sens_results[[pname]][[as.character(0.20)]]$pass_fail)]
    all_fails <- sort(unique(c(fails_m20, fails_p20)))
    cat(sprintf("  %-14s: %s\n", pname, if (length(all_fails)) paste(all_fails, collapse = ", ") else "(robust to +/-20%)"))
  }

  cat("\n\nIdentification structure (parameter families affecting diagnostics):\n\n")
  families <- list(
    VR = c("vr2", "vr4", "vr8", "vr13"),
    ACF = c("acf1", "acf2"),
    RACF = c("racf1", "racf4", "racf13"),
    Pers = c("pers1", "pers4", "pers13"),
    R2 = c("xr2_1", "xr2_4", "xr2_13")
  )

  for (fname in names(families)) {
    fkeys <- families[[fname]]
    affecting <- c()

    for (i in seq_len(nrow(sens_params))) {
      pname <- sens_params$name[i]
      baseline_vals <- vapply(fkeys, function(k) all_sens_results[[pname]][[as.character(0)]][[k]], numeric(1))

      changed <- FALSE
      for (d in c(-0.20, 0.20)) {
        pert_vals <- vapply(fkeys, function(k) all_sens_results[[pname]][[as.character(d)]][[k]], numeric(1))
        shift <- abs(pert_vals - baseline_vals)
        shift <- shift[is.finite(shift)]
        if (length(shift) > 0L && max(shift) > 0.02) {
          changed <- TRUE
          break
        }
      }

      if (changed) affecting <- c(affecting, pname)
    }

    cat(sprintf("  %-6s: %s\n", fname, if (length(affecting)) paste(unique(affecting), collapse = ", ") else "(insensitive)"))
  }

  if (!skip_plots) {
    cat("\nGenerating sensitivity figure...\n")

    png(file.path(base_dir, "v42_sensitivity.png"), width = 1600, height = 1000, res = 200)
    old_par3 <- par(no.readonly = TRUE)
    on.exit(par(old_par3), add = TRUE)
    par(mfrow = c(2, 3), mar = c(4, 4, 3, 1), mgp = c(2.2, 0.7, 0))

    for (i in seq_len(nrow(sens_params))) {
      pname <- sens_params$name[i]
      pbase <- sens_params$base[i]
      scores <- vapply(perturbations, function(d) all_sens_results[[pname]][[as.character(d)]]$n_pass, numeric(1))
      cols <- ifelse(scores == 15, "#4caf50", ifelse(scores >= 12, "#ff9800", "#d32f2f"))
      bp <- barplot(scores, col = cols, ylim = c(0, 16), names.arg = sprintf("%+.0f%%", perturbations * 100),
                    main = sprintf("%s (baseline=%.3f)", pname, pbase), ylab = "Score (/15)")
      abline(h = 15, col = "darkgreen", lty = 2)
      text(bp, scores + 0.3, labels = scores, cex = 0.8)
    }

    mtext("Parameter Sensitivity: Score vs +/-10% and +/-20% perturbation", outer = TRUE, line = -1)
    dev.off()
    cat("  Saved: v42_sensitivity.png\n")
  }

  # ---------------------------------------------------------------------------
  # Final summary
  # ---------------------------------------------------------------------------
  elapsed_total <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))

  print_rule("v4.2 COMPLETE")
  cat(sprintf("  Phase 1 - Core simulation: %d/%d diagnostics pass\n", n_pass, length(tests)))
  cat(sprintf("  Phase 2 - Ablation: %d levels evaluated\n", length(ablation_levels)))
  abl_scores <- vapply(all_abl_results, function(x) x$mc$n_pass, numeric(1))
  cat(sprintf("    Scores: %s\n", paste(abl_scores, collapse = " -> ")))

  cat(sprintf("  Phase 3 - Sensitivity: %d params x %d perturbations\n", nrow(sens_params), length(perturbations)))

  robust_params <- sens_params$name[vapply(seq_len(nrow(sens_params)), function(i) {
    pname <- sens_params$name[i]
    all(vapply(perturbations, function(d) all_sens_results[[pname]][[as.character(d)]]$n_pass >= 14, logical(1)))
  }, logical(1))]

  fragile_params <- sens_params$name[vapply(seq_len(nrow(sens_params)), function(i) {
    pname <- sens_params$name[i]
    any(vapply(perturbations, function(d) all_sens_results[[pname]][[as.character(d)]]$n_pass < 12, logical(1)))
  }, logical(1))]

  cat(sprintf("    Robust to +/-20%%: %s\n", if (length(robust_params)) paste(robust_params, collapse = ", ") else "(none)"))
  cat(sprintf("    Fragile at +/-20%%: %s\n", if (length(fragile_params)) paste(fragile_params, collapse = ", ") else "(none)"))
  cat(sprintf("  Total elapsed: %.0fs\n", elapsed_total))
  cat("Done.\n")

  invisible(list(
    tests = tests,
    mc_stats = mc_stats,
    ablation = all_abl_results,
    sensitivity = all_sens_results
  ))
}

if (identical(environment(), globalenv())) {
  tryCatch(
    main(),
    error = function(e) {
      message("model_v42.R failed: ", conditionMessage(e))
      quit(status = 1)
    }
  )
}
