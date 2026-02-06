# Jump-model model zoo + diagnostics + fitting utilities

has_pkg <- function(pkg) requireNamespace(pkg, quietly = TRUE)

clamp_range <- function(x, lo, hi) pmin(pmax(x, lo), hi)

resolve_curve_vec <- function(curve, value_cols = NULL) {
  if (is.null(curve)) return(NULL)
  if (is.numeric(curve)) return(as.numeric(curve))
  if (is.list(curve) && !is.data.frame(curve)) {
    if (!is.null(curve$mean_vec)) return(as.numeric(curve$mean_vec))
    if (!is.null(curve$sd_vec)) return(as.numeric(curve$sd_vec))
  }
  if (is.data.frame(curve)) {
    if (is.null(value_cols)) {
      stop("value_cols required when curve is data.frame")
    }
    for (col in value_cols) {
      if (col %in% names(curve)) {
        vec <- curve[[col]]
        if (is.numeric(vec)) return(as.numeric(vec))
      }
    }
  }
  NULL
}

build_rank_slot_increments <- function(rank_panel, K_max = NULL) {
  df <- rank_panel
  if (!is.null(K_max)) df <- df %>% dplyr::filter(rank <= K_max)
  df %>% dplyr::select(week, rank, dlogw)
}

build_identity_increments <- function(endpoint_weekly, horizon = 1L) {
  h <- as.integer(horizon)
  endpoint_weekly %>%
    dplyr::arrange(endpoint_id, week) %>%
    dplyr::group_by(endpoint_id) %>%
    dplyr::mutate(
      log_w = log(pmax(share_global, 1e-18)),
      dlogw = dplyr::lead(log_w, h) - log_w,
      rank_t = rank,
      week_tp = dplyr::lead(week, h)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(is.finite(dlogw)) %>%
    dplyr::transmute(
      week = week,
      endpoint_id = endpoint_id,
      rank = rank_t,
      dlogw = dlogw
    )
}

standardize_increments <- function(increments_df, mu_curve, sd_curve, rank_col = "rank", bucket_def = NULL) {
  mu_vec <- resolve_curve_vec(mu_curve, c("mean_dlogw_s", "mean_dlogw"))
  sd_vec <- resolve_curve_vec(sd_curve, c("sd_dlogw_s", "sd_dlogw"))
  if (is.null(mu_vec) || is.null(sd_vec)) {
    stop("standardize_increments: unable to resolve mu_curve/sd_curve vectors")
  }

  df <- increments_df %>% dplyr::mutate(rank__ = .data[[rank_col]])
  max_rank <- max(df$rank__, na.rm = TRUE)
  if (length(mu_vec) < max_rank) {
    mu_vec <- c(mu_vec, rep(0, max_rank - length(mu_vec)))
  }
  if (length(sd_vec) < max_rank) {
    sd_vec <- c(sd_vec, rep(1, max_rank - length(sd_vec)))
  }

  mu_hat <- mu_vec[df$rank__]
  sd_hat <- pmax(sd_vec[df$rank__], 1e-8)

  df <- df %>%
    dplyr::mutate(
      mu_hat = mu_hat,
      sd_hat = sd_hat,
      dlogw_std = (dlogw - mu_hat) / sd_hat
    )

  if (!is.null(bucket_def)) {
    df <- df %>% dplyr::mutate(bucket = assign_bucket(rank__, bucket_def))
  }

  df %>% dplyr::rename(rank = rank__)
}

skewness_basic <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 3) return(NA_real_)
  m <- mean(x)
  s <- sd(x)
  if (!is.finite(s) || s <= 0) return(NA_real_)
  mean(((x - m) / s)^3)
}

kurtosis_basic <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 4) return(NA_real_)
  m <- mean(x)
  s <- sd(x)
  if (!is.finite(s) || s <= 0) return(NA_real_)
  mean(((x - m) / s)^4) - 3
}

robust_skew_medcouple <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 5) return(NA_real_)
  if (has_pkg("robustbase")) {
    return(as.numeric(robustbase::mc(x)))
  }
  q <- stats::quantile(x, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
  denom <- (q[3] - q[1])
  if (!is.finite(denom) || denom <= 0) return(NA_real_)
  (q[3] + q[1] - 2 * q[2]) / denom
}

robust_kurt_moors <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 10) return(NA_real_)
  qs <- stats::quantile(x, probs = c(0.125, 0.25, 0.375, 0.625, 0.75, 0.875), na.rm = TRUE)
  denom <- qs[5] - qs[2]
  if (!is.finite(denom) || denom <= 0) return(NA_real_)
  ((qs[6] - qs[4]) + (qs[3] - qs[1])) / denom
}

hill_alpha <- function(x, k = NULL) {
  x <- abs(x[is.finite(x)])
  if (length(x) < 20) return(NA_real_)
  x <- sort(x, decreasing = TRUE)
  if (is.null(k)) k <- max(10, floor(0.05 * length(x)))
  k <- min(k, length(x) - 1L)
  if (k <= 1) return(NA_real_)
  xk <- x[seq_len(k)]
  xk1 <- x[k + 1L]
  if (!is.finite(xk1) || xk1 <= 0) return(NA_real_)
  1 / mean(log(xk) - log(xk1))
}

.tail_stats_single <- function(z, thresholds, probs) {
  z <- z[is.finite(z)]
  out <- list(n = length(z))
  if (length(z) == 0) return(tibble::as_tibble(out))

  for (thr in thresholds) {
    thr_chr <- gsub("\\.", "_", as.character(thr))
    out[[paste0("p_z_gt_", thr_chr)]] <- mean(z > thr, na.rm = TRUE)
    out[[paste0("p_z_lt_-", thr_chr)]] <- mean(z < -thr, na.rm = TRUE)
    out[[paste0("p_abs_gt_", thr_chr)]] <- mean(abs(z) > thr, na.rm = TRUE)
  }

  qs <- stats::quantile(z, probs = probs, na.rm = TRUE, names = FALSE)
  for (i in seq_along(probs)) {
    p <- probs[i]
    p_chr <- gsub("\\.", "_", format(p, scientific = FALSE))
    out[[paste0("q_", p_chr)]] <- qs[i]
  }

  if (all(c(0.001, 0.999) %in% probs)) {
    q001 <- qs[which(probs == 0.001)]
    q999 <- qs[which(probs == 0.999)]
    out[["R_999_001"]] <- ifelse(abs(q001) > 0, abs(q999) / abs(q001), NA_real_)
    out[["Delta_tail"]] <- q999 + q001
  }

  out[["skewness"]] <- skewness_basic(z)
  out[["excess_kurtosis"]] <- kurtosis_basic(z)
  out[["skew_medcouple"]] <- robust_skew_medcouple(z)
  out[["kurt_moors"]] <- robust_kurt_moors(z)
  out[["hill_alpha"]] <- hill_alpha(z)
  tibble::as_tibble(out)
}

tail_skew_diagnostics <- function(z_df, thresholds = c(3, 5), probs = c(0.001, 0.01, 0.99, 0.999)) {
  df <- z_df
  if (!"z" %in% names(df)) {
    stop("tail_skew_diagnostics: z_df must include column 'z'")
  }
  grp_cols <- intersect(c("bucket", "rank_bin"), names(df))
  if (length(grp_cols) == 0) {
    return(.tail_stats_single(df$z, thresholds, probs))
  }

  df %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(grp_cols))) %>%
    dplyr::group_modify(~ .tail_stats_single(.x$z, thresholds, probs)) %>%
    dplyr::ungroup()
}

jump_clustering_diagnostics <- function(z_df, thresholds = c(3, 4, 5)) {
  if (!"week" %in% names(z_df)) {
    stop("jump_clustering_diagnostics: z_df must include 'week'")
  }
  if (!"bucket" %in% names(z_df)) {
    z_df <- z_df %>% dplyr::mutate(bucket = "all")
  }

  res <- purrr::map_dfr(thresholds, function(thr) {
    z_df %>%
      dplyr::group_by(bucket, week) %>%
      dplyr::summarise(p_extreme = mean(abs(z) > thr, na.rm = TRUE),
                       mean_abs = mean(abs(z), na.rm = TRUE), .groups = "drop") %>%
      dplyr::group_by(bucket) %>%
      dplyr::group_modify(~{
        s_ext <- .x$p_extreme
        s_abs <- .x$mean_abs
        acf_ext <- stats::acf(s_ext, plot = FALSE, lag.max = 1)$acf
        acf_abs <- stats::acf(s_abs, plot = FALSE, lag.max = 1)$acf
        acf1_ext <- ifelse(length(acf_ext) >= 2, acf_ext[2], NA_real_)
        acf1_abs <- ifelse(length(acf_abs) >= 2, acf_abs[2], NA_real_)
        p_ext <- tryCatch(stats::Box.test(s_ext, lag = 1, type = "Ljung-Box")$p.value,
                          error = function(e) NA_real_)
        p_abs <- tryCatch(stats::Box.test(s_abs, lag = 1, type = "Ljung-Box")$p.value,
                          error = function(e) NA_real_)
        tibble::tibble(
          threshold = thr,
          acf1_extreme = acf1_ext,
          acf1_abs = acf1_abs,
          pval_extreme = p_ext,
          pval_abs = p_abs,
          cluster_flag = is.finite(acf1_ext) && acf1_ext > 0.1 && is.finite(p_ext) && p_ext < 0.05
        )
      }) %>%
      dplyr::ungroup()
  })

  res
}

qqplot_data <- function(z, dist = c("norm", "t"), params = list()) {
  dist <- match.arg(dist)
  z <- z[is.finite(z)]
  n <- length(z)
  if (n < 2) return(tibble::tibble(sample_q = numeric(0), theo_q = numeric(0)))
  p <- (seq_len(n) - 0.5) / n
  sample_q <- sort(z)
  if (dist == "norm") {
    theo_q <- stats::qnorm(p)
  } else {
    df <- params$df %||% 6
    scale <- params$scale %||% 1
    theo_q <- stats::qt(p, df = df) * scale
  }
  tibble::tibble(sample_q = sample_q, theo_q = theo_q)
}

qqplot_suite <- function(z, t_df = 6) {
  list(
    normal = qqplot_data(z, dist = "norm"),
    student_t = qqplot_data(z, dist = "t", params = list(df = t_df))
  )
}

# --- distribution helpers ---

r_double_exp_asym <- function(n, pi_pos, eta_pos, eta_neg) {
  if (n <= 0) return(numeric(0))
  u <- runif(n)
  is_pos <- u < pi_pos
  out <- numeric(n)
  if (any(is_pos)) out[is_pos] <- rexp(sum(is_pos), rate = eta_pos)
  if (any(!is_pos)) out[!is_pos] <- -rexp(sum(!is_pos), rate = eta_neg)
  out
}

loglik_student_t <- function(z, df, scale) {
  z <- z[is.finite(z)]
  if (length(z) == 0) return(0)
  sum(stats::dt(z / scale, df = df, log = TRUE) - log(scale))
}

loglik_gaussian_mixture <- function(z, p, mu1, sd1, mu2, sd2) {
  dens <- (1 - p) * stats::dnorm(z, mean = mu1, sd = sd1) + p * stats::dnorm(z, mean = mu2, sd = sd2)
  sum(log(pmax(dens, 1e-12)))
}

loglik_merton <- function(z, p, mu_j, sd_j) {
  dens <- (1 - p) * stats::dnorm(z, mean = 0, sd = 1) + p * stats::dnorm(z, mean = mu_j, sd = sd_j)
  sum(log(pmax(dens, 1e-12)))
}

loglik_kou <- function(z, p, pi_pos, eta_pos, eta_neg) {
  dens_jump <- ifelse(
    z >= 0,
    pi_pos * eta_pos * exp(-eta_pos * z),
    (1 - pi_pos) * eta_neg * exp(-eta_neg * (-z))
  )
  dens <- (1 - p) * stats::dnorm(z, mean = 0, sd = 1) + p * dens_jump
  sum(log(pmax(dens, 1e-12)))
}

# --- EM fitters ---

fit_student_t_bucket <- function(z, df_min = 2.2) {
  z <- z[is.finite(z)]
  if (length(z) < 10) return(list(converged = FALSE, df = NA_real_, scale = NA_real_, loglik = NA_real_))
  nll <- function(par) {
    df <- df_min + exp(par[1])
    scale <- exp(par[2])
    -loglik_student_t(z, df, scale)
  }
  init <- c(log(6 - df_min), log(sd(z)))
  fit <- tryCatch(
    optim(init, nll, method = "L-BFGS-B", control = list(maxit = 200)),
    error = function(e) NULL
  )
  if (is.null(fit) || fit$convergence != 0) {
    return(list(converged = FALSE, df = NA_real_, scale = NA_real_, loglik = NA_real_))
  }
  df <- df_min + exp(fit$par[1])
  scale <- exp(fit$par[2])
  ll <- -fit$value
  list(converged = TRUE, df = df, scale = scale, loglik = ll)
}

fit_student_t <- function(z_df, mode = "bucket", df_min = 2.2) {
  if (!"bucket" %in% names(z_df)) stop("fit_student_t: z_df must have bucket")
  buckets <- split(z_df$z, z_df$bucket)
  fits <- lapply(buckets, fit_student_t_bucket, df_min = df_min)
  converged <- all(vapply(fits, function(x) isTRUE(x$converged), logical(1)))
  params_df <- tibble::tibble(
    bucket = names(fits),
    df = vapply(fits, function(x) x$df, numeric(1)),
    scale = vapply(fits, function(x) x$scale, numeric(1)),
    loglik = vapply(fits, function(x) x$loglik, numeric(1))
  )
  ll <- sum(params_df$loglik, na.rm = TRUE)
  n <- sum(vapply(buckets, length, integer(1)))
  k <- 2L * nrow(params_df)
  aic <- 2 * k - 2 * ll
  bic <- log(max(1, n)) * k - 2 * ll
  list(
    type = "student_t",
    mode = mode,
    converged = converged,
    params = params_df,
    loglik = ll,
    aic = aic,
    bic = bic
  )
}

fit_skew_t_bucket <- function(z) {
  if (!has_pkg("sn")) return(NULL)
  z <- z[is.finite(z)]
  if (length(z) < 20) return(NULL)
  fit <- tryCatch(sn::selm(z ~ 1, family = "ST"), error = function(e) NULL)
  if (is.null(fit)) return(NULL)
  dp <- tryCatch(sn::coef(fit, "DP"), error = function(e) NULL)
  if (is.null(dp)) return(NULL)
  ll <- tryCatch(as.numeric(stats::logLik(fit)), error = function(e) NA_real_)
  list(converged = TRUE, dp = dp, loglik = ll)
}

fit_skew_t <- function(z_df, mode = "bucket") {
  if (!has_pkg("sn")) {
    message("Package 'sn' not installed; skipping skew-t fits.")
    return(NULL)
  }
  if (!"bucket" %in% names(z_df)) stop("fit_skew_t: z_df must have bucket")
  buckets <- split(z_df$z, z_df$bucket)
  fits <- lapply(buckets, fit_skew_t_bucket)
  if (all(vapply(fits, is.null, logical(1)))) return(NULL)
  converged <- all(vapply(fits, function(x) !is.null(x) && isTRUE(x$converged), logical(1)))
  params_df <- tibble::tibble(
    bucket = names(fits),
    dp = I(lapply(fits, function(x) if (is.null(x)) NA else x$dp)),
    loglik = vapply(fits, function(x) if (is.null(x)) NA_real_ else x$loglik, numeric(1))
  )
  ll <- sum(params_df$loglik, na.rm = TRUE)
  n <- sum(vapply(buckets, length, integer(1)))
  k <- 4L * nrow(params_df) # xi, omega, alpha, nu
  aic <- 2 * k - 2 * ll
  bic <- log(max(1, n)) * k - 2 * ll
  list(
    type = "skew_t",
    mode = mode,
    converged = converged,
    params = params_df,
    loglik = ll,
    aic = aic,
    bic = bic
  )
}

fit_ghyp_bucket <- function(z, variant = "GH") {
  if (!has_pkg("ghyp")) return(NULL)
  z <- z[is.finite(z)]
  if (length(z) < 30) return(NULL)
  fit <- tryCatch(ghyp::fit.ghypuv(z), error = function(e) NULL)
  if (is.null(fit)) return(NULL)
  ll <- tryCatch(as.numeric(stats::logLik(fit)), error = function(e) NA_real_)
  list(converged = TRUE, obj = fit, loglik = ll, variant = variant)
}

fit_ghyp <- function(z_df, mode = "bucket") {
  if (!has_pkg("ghyp")) {
    message("Package 'ghyp' not installed; skipping GH fits.")
    return(NULL)
  }
  if (!"bucket" %in% names(z_df)) stop("fit_ghyp: z_df must have bucket")
  buckets <- split(z_df$z, z_df$bucket)
  fits <- lapply(buckets, fit_ghyp_bucket)
  if (all(vapply(fits, is.null, logical(1)))) return(NULL)
  converged <- all(vapply(fits, function(x) !is.null(x) && isTRUE(x$converged), logical(1)))
  params_df <- tibble::tibble(
    bucket = names(fits),
    obj = I(lapply(fits, function(x) if (is.null(x)) NULL else x$obj)),
    loglik = vapply(fits, function(x) if (is.null(x)) NA_real_ else x$loglik, numeric(1))
  )
  ll <- sum(params_df$loglik, na.rm = TRUE)
  n <- sum(vapply(buckets, length, integer(1)))
  k <- 5L * nrow(params_df) # rough parameter count
  aic <- 2 * k - 2 * ll
  bic <- log(max(1, n)) * k - 2 * ll
  list(
    type = "ghyp",
    mode = mode,
    converged = converged,
    params = params_df,
    loglik = ll,
    aic = aic,
    bic = bic
  )
}

fit_gaussian_mixture_bucket <- function(z, p_max = 0.25, max_iter = 500, tol = 1e-6) {
  z <- z[is.finite(z)]
  if (length(z) < 20) {
    return(list(converged = FALSE, p = NA_real_, mu1 = NA_real_, sd1 = NA_real_, mu2 = NA_real_, sd2 = NA_real_, loglik = NA_real_))
  }

  p <- min(mean(abs(z) > 3, na.rm = TRUE), p_max)
  if (!is.finite(p) || p <= 0) p <- min(0.05, p_max)
  mu1 <- mean(z, na.rm = TRUE)
  mu2 <- mean(z[abs(z) > 2], na.rm = TRUE)
  if (!is.finite(mu2)) mu2 <- mu1
  sd1 <- sd(z, na.rm = TRUE)
  sd2 <- sd(z[abs(z) > 2], na.rm = TRUE)
  if (!is.finite(sd2) || sd2 <= 0) sd2 <- max(sd1 * 2, 1e-3)

  ll_prev <- -Inf
  for (iter in seq_len(max_iter)) {
    dens1 <- stats::dnorm(z, mean = mu1, sd = sd1)
    dens2 <- stats::dnorm(z, mean = mu2, sd = sd2)
    denom <- (1 - p) * dens1 + p * dens2
    r <- p * dens2 / pmax(denom, 1e-12)

    p <- clamp_range(mean(r, na.rm = TRUE), 1e-6, p_max)
    w1 <- 1 - r
    w2 <- r
    mu1 <- sum(w1 * z) / sum(w1)
    mu2 <- sum(w2 * z) / sum(w2)
    sd1 <- sqrt(sum(w1 * (z - mu1)^2) / sum(w1))
    sd2 <- sqrt(sum(w2 * (z - mu2)^2) / sum(w2))
    sd1 <- max(sd1, 1e-6)
    sd2 <- max(sd2, 1e-6)

    if (sd2 < sd1) {
      tmp <- mu1; mu1 <- mu2; mu2 <- tmp
      tmp <- sd1; sd1 <- sd2; sd2 <- tmp
      p <- 1 - p
      r <- 1 - r
    }

    ll <- loglik_gaussian_mixture(z, p, mu1, sd1, mu2, sd2)
    if (is.finite(ll) && abs(ll - ll_prev) < tol) {
      return(list(converged = TRUE, p = p, mu1 = mu1, sd1 = sd1, mu2 = mu2, sd2 = sd2, loglik = ll, iter = iter))
    }
    ll_prev <- ll
  }

  list(converged = FALSE, p = p, mu1 = mu1, sd1 = sd1, mu2 = mu2, sd2 = sd2, loglik = ll_prev, iter = max_iter)
}

fit_mixture_gaussian <- function(z_df, mode = "bucket", p_max = 0.25) {
  if (!"bucket" %in% names(z_df)) stop("fit_mixture_gaussian: z_df must have bucket")
  buckets <- split(z_df$z, z_df$bucket)
  fits <- lapply(buckets, fit_gaussian_mixture_bucket, p_max = p_max)
  converged <- all(vapply(fits, function(x) isTRUE(x$converged), logical(1)))
  params_df <- tibble::tibble(
    bucket = names(fits),
    p = vapply(fits, function(x) x$p, numeric(1)),
    mu1 = vapply(fits, function(x) x$mu1, numeric(1)),
    sd1 = vapply(fits, function(x) x$sd1, numeric(1)),
    mu2 = vapply(fits, function(x) x$mu2, numeric(1)),
    sd2 = vapply(fits, function(x) x$sd2, numeric(1)),
    loglik = vapply(fits, function(x) x$loglik, numeric(1))
  )
  ll <- sum(params_df$loglik, na.rm = TRUE)
  n <- sum(vapply(buckets, length, integer(1)))
  k <- 5L * nrow(params_df)
  aic <- 2 * k - 2 * ll
  bic <- log(max(1, n)) * k - 2 * ll
  list(
    type = "mixture_gaussian",
    mode = mode,
    converged = converged,
    params = params_df,
    loglik = ll,
    aic = aic,
    bic = bic
  )
}

fit_jump_merton_bucket <- function(z, p_max = 0.25, max_iter = 500, tol = 1e-6) {
  z <- z[is.finite(z)]
  if (length(z) < 20) {
    return(list(converged = FALSE, p = NA_real_, mu_j = NA_real_, sd_j = NA_real_, loglik = NA_real_))
  }
  u <- 3
  p0 <- min(mean(abs(z) > u, na.rm = TRUE), p_max)
  if (!is.finite(p0) || p0 <= 0) p0 <- min(0.02, p_max)
  mu_j <- mean(z[abs(z) > u], na.rm = TRUE)
  if (!is.finite(mu_j)) mu_j <- 0
  sd_j <- sd(z[abs(z) > u], na.rm = TRUE)
  if (!is.finite(sd_j) || sd_j <= 0) sd_j <- 1
  p <- p0

  ll_prev <- -Inf
  for (iter in seq_len(max_iter)) {
    dens_jump <- stats::dnorm(z, mean = mu_j, sd = sd_j)
    dens_diff <- stats::dnorm(z, mean = 0, sd = 1)
    denom <- (1 - p) * dens_diff + p * dens_jump
    r <- p * dens_jump / pmax(denom, 1e-12)

    p <- clamp_range(mean(r, na.rm = TRUE), 1e-6, p_max)
    mu_j <- sum(r * z) / sum(r)
    sd_j <- sqrt(sum(r * (z - mu_j)^2) / sum(r))
    sd_j <- max(sd_j, 1e-6)

    ll <- loglik_merton(z, p, mu_j, sd_j)
    if (is.finite(ll) && abs(ll - ll_prev) < tol) {
      return(list(converged = TRUE, p = p, mu_j = mu_j, sd_j = sd_j, loglik = ll, iter = iter))
    }
    ll_prev <- ll
  }

  list(converged = FALSE, p = p, mu_j = mu_j, sd_j = sd_j, loglik = ll_prev, iter = max_iter)
}

fit_jump_merton <- function(z_df, mode = "bucket", p_max = 0.25) {
  if (!"bucket" %in% names(z_df)) stop("fit_jump_merton: z_df must have bucket")
  buckets <- split(z_df$z, z_df$bucket)
  fits <- lapply(buckets, fit_jump_merton_bucket, p_max = p_max)
  converged <- all(vapply(fits, function(x) isTRUE(x$converged), logical(1)))
  params_df <- tibble::tibble(
    bucket = names(fits),
    p = vapply(fits, function(x) x$p, numeric(1)),
    mu_j = vapply(fits, function(x) x$mu_j, numeric(1)),
    sd_j = vapply(fits, function(x) x$sd_j, numeric(1)),
    loglik = vapply(fits, function(x) x$loglik, numeric(1))
  )
  ll <- sum(params_df$loglik, na.rm = TRUE)
  n <- sum(vapply(buckets, length, integer(1)))
  k <- 3L * nrow(params_df)
  aic <- 2 * k - 2 * ll
  bic <- log(max(1, n)) * k - 2 * ll
  list(
    type = "jump_merton",
    mode = mode,
    converged = converged,
    params = params_df,
    loglik = ll,
    aic = aic,
    bic = bic
  )
}

fit_jump_kou_bucket <- function(z, p_max = 0.25, max_iter = 500, tol = 1e-6) {
  z <- z[is.finite(z)]
  if (length(z) < 20) {
    return(list(converged = FALSE, p = NA_real_, pi_pos = NA_real_, eta_pos = NA_real_, eta_neg = NA_real_, loglik = NA_real_))
  }
  u <- 3
  p0 <- min(mean(abs(z) > u, na.rm = TRUE), p_max)
  if (!is.finite(p0) || p0 <= 0) p0 <- min(0.02, p_max)

  n_pos <- sum(z > u, na.rm = TRUE)
  n_neg <- sum(z < -u, na.rm = TRUE)
  pi_pos <- ifelse(n_pos + n_neg > 0, n_pos / (n_pos + n_neg), 0.5)

  mplus <- mean(z[z > u] - u, na.rm = TRUE)
  mminus <- mean((-z[z < -u]) - u, na.rm = TRUE)
  eta_pos <- 1 / max(mplus, 1e-3)
  eta_neg <- 1 / max(mminus, 1e-3)

  p <- p0
  ll_prev <- -Inf
  for (iter in seq_len(max_iter)) {
    dens_jump <- ifelse(
      z >= 0,
      pi_pos * eta_pos * exp(-eta_pos * z),
      (1 - pi_pos) * eta_neg * exp(-eta_neg * (-z))
    )
    dens_diff <- stats::dnorm(z, mean = 0, sd = 1)
    denom <- (1 - p) * dens_diff + p * dens_jump
    r <- p * dens_jump / pmax(denom, 1e-12)

    p <- clamp_range(mean(r, na.rm = TRUE), 1e-6, p_max)
    w_pos <- r * (z > 0)
    w_neg <- r * (z < 0)
    w_pos_sum <- sum(w_pos)
    w_neg_sum <- sum(w_neg)
    w_sum <- w_pos_sum + w_neg_sum
    pi_pos <- ifelse(w_sum > 0, w_pos_sum / w_sum, pi_pos)

    if (w_pos_sum > 0) {
      eta_pos <- w_pos_sum / sum(w_pos * z)
    }
    if (w_neg_sum > 0) {
      eta_neg <- w_neg_sum / sum(w_neg * (-z))
    }
    eta_pos <- max(eta_pos, 1e-6)
    eta_neg <- max(eta_neg, 1e-6)

    ll <- loglik_kou(z, p, pi_pos, eta_pos, eta_neg)
    if (is.finite(ll) && abs(ll - ll_prev) < tol) {
      return(list(converged = TRUE, p = p, pi_pos = pi_pos, eta_pos = eta_pos, eta_neg = eta_neg, loglik = ll, iter = iter))
    }
    ll_prev <- ll
  }

  list(converged = FALSE, p = p, pi_pos = pi_pos, eta_pos = eta_pos, eta_neg = eta_neg, loglik = ll_prev, iter = max_iter)
}

fit_jump_kou_smooth <- function(z_df, K_max = NULL, u = 3, p_max = 0.25) {
  if (!has_pkg("mgcv")) {
    message("Package 'mgcv' not installed; falling back to bucket Kou fit.")
    return(NULL)
  }
  if (is.null(K_max)) K_max <- max(z_df$rank, na.rm = TRUE)
  df <- z_df %>%
    dplyr::filter(rank <= K_max) %>%
    dplyr::mutate(log_rank = log(rank), jump = abs(z) > u, pos_jump = z > u, neg_jump = z < -u)

  # p_jump smooth
  gam_p <- tryCatch(mgcv::gam(jump ~ s(log_rank, k = 10), family = binomial(), data = df), error = function(e) NULL)
  if (is.null(gam_p)) return(NULL)
  log_rank_new <- log(seq_len(K_max))
  p_hat <- mgcv::predict.gam(gam_p, newdata = data.frame(log_rank = log_rank_new), type = "response")
  p_hat <- clamp_range(p_hat, 1e-6, p_max)

  # pi_pos smooth on jump subset
  df_jump <- df %>% dplyr::filter(jump)
  if (nrow(df_jump) > 0) {
    gam_pi <- tryCatch(mgcv::gam(pos_jump ~ s(log_rank, k = 10), family = binomial(), data = df_jump), error = function(e) NULL)
    pi_hat <- if (is.null(gam_pi)) rep(0.5, K_max) else mgcv::predict.gam(gam_pi, newdata = data.frame(log_rank = log_rank_new), type = "response")
  } else {
    pi_hat <- rep(0.5, K_max)
  }
  pi_hat <- clamp_range(pi_hat, 1e-3, 1 - 1e-3)

  # eta_pos smooth from exceedances
  df_pos <- df %>% dplyr::filter(z > u) %>% dplyr::mutate(excess = z - u)
  if (nrow(df_pos) > 5) {
    gam_pos <- tryCatch(mgcv::gam(log(excess) ~ s(log_rank, k = 10), data = df_pos), error = function(e) NULL)
    mean_excess_pos <- if (is.null(gam_pos)) rep(mean(df_pos$excess, na.rm = TRUE), K_max) else exp(mgcv::predict.gam(gam_pos, newdata = data.frame(log_rank = log_rank_new)))
  } else {
    mean_excess_pos <- rep(mean(df_pos$excess, na.rm = TRUE), K_max)
  }
  eta_pos <- 1 / pmax(mean_excess_pos, 1e-3)

  # eta_neg smooth from negative exceedances
  df_neg <- df %>% dplyr::filter(z < -u) %>% dplyr::mutate(excess = -z - u)
  if (nrow(df_neg) > 5) {
    gam_neg <- tryCatch(mgcv::gam(log(excess) ~ s(log_rank, k = 10), data = df_neg), error = function(e) NULL)
    mean_excess_neg <- if (is.null(gam_neg)) rep(mean(df_neg$excess, na.rm = TRUE), K_max) else exp(mgcv::predict.gam(gam_neg, newdata = data.frame(log_rank = log_rank_new)))
  } else {
    mean_excess_neg <- rep(mean(df_neg$excess, na.rm = TRUE), K_max)
  }
  eta_neg <- 1 / pmax(mean_excess_neg, 1e-3)

  list(
    type = "jump_kou",
    mode = "smooth",
    converged = TRUE,
    params = list(p = p_hat, pi_pos = pi_hat, eta_pos = eta_pos, eta_neg = eta_neg),
    loglik = NA_real_,
    aic = NA_real_,
    bic = NA_real_
  )
}

fit_jump_kou_asym <- function(z_df, mode = "bucket", p_max = 0.25) {
  if (mode == "smooth") {
    smooth_fit <- fit_jump_kou_smooth(z_df, K_max = max(z_df$rank, na.rm = TRUE), u = 3, p_max = p_max)
    if (!is.null(smooth_fit)) return(smooth_fit)
  }
  if (!"bucket" %in% names(z_df)) stop("fit_jump_kou_asym: z_df must have bucket")
  buckets <- split(z_df$z, z_df$bucket)
  fits <- lapply(buckets, fit_jump_kou_bucket, p_max = p_max)
  converged <- all(vapply(fits, function(x) isTRUE(x$converged), logical(1)))
  params_df <- tibble::tibble(
    bucket = names(fits),
    p = vapply(fits, function(x) x$p, numeric(1)),
    pi_pos = vapply(fits, function(x) x$pi_pos, numeric(1)),
    eta_pos = vapply(fits, function(x) x$eta_pos, numeric(1)),
    eta_neg = vapply(fits, function(x) x$eta_neg, numeric(1)),
    loglik = vapply(fits, function(x) x$loglik, numeric(1))
  )
  ll <- sum(params_df$loglik, na.rm = TRUE)
  n <- sum(vapply(buckets, length, integer(1)))
  k <- 4L * nrow(params_df)
  aic <- 2 * k - 2 * ll
  bic <- log(max(1, n)) * k - 2 * ll
  list(
    type = "jump_kou",
    mode = mode,
    converged = converged,
    params = params_df,
    loglik = ll,
    aic = aic,
    bic = bic
  )
}

# --- factor models ---

build_z_matrix <- function(z_df, K_pca) {
  wide <- z_df %>%
    dplyr::filter(rank <= K_pca) %>%
    dplyr::select(week, rank, z) %>%
    tidyr::pivot_wider(names_from = rank, values_from = z)
  wide <- wide %>% tidyr::drop_na()
  if (nrow(wide) < 2) return(list(X = NULL, weeks = NULL))
  X <- wide %>% dplyr::select(-week) %>% as.matrix()
  list(X = X, weeks = wide$week)
}

compute_pca_factor <- function(z_df, K_pca, K_max, smoothing_h = 0L) {
  mat <- build_z_matrix(z_df, K_pca)
  if (is.null(mat$X)) return(NULL)
  pca_fit <- stats::prcomp(mat$X, center = TRUE, scale. = TRUE)
  var_explained <- (pca_fit$sdev^2) / sum(pca_fit$sdev^2)
  beta_raw <- pca_fit$rotation[, 1]
  beta_vec <- rep(0, K_max)
  beta_vec[seq_len(K_pca)] <- beta_raw
  if (smoothing_h > 0) beta_vec <- moving_average_rank(beta_vec, h = smoothing_h)
  beta_scale <- sqrt(mean(beta_vec[seq_len(K_pca)]^2, na.rm = TRUE))
  if (is.finite(beta_scale) && beta_scale > 0) beta_vec <- beta_vec / beta_scale

  F_t <- pca_fit$x[, 1]
  tibble::tibble(week = mat$weeks, F_t = F_t, stringsAsFactors = FALSE) %>%
    list(beta_k = beta_vec, F = ., var_explained = var_explained)
}

project_factor_scores <- function(z_df, beta_k) {
  if (is.null(beta_k)) return(NULL)
  beta_k <- as.numeric(beta_k)
  df <- z_df %>% dplyr::filter(rank <= length(beta_k))
  df %>%
    dplyr::group_by(week) %>%
    dplyr::summarise(
      F_t = sum(beta_k[rank] * z, na.rm = TRUE) / sum(beta_k[rank]^2, na.rm = TRUE),
      .groups = "drop"
    )
}

fit_factor_t <- function(z_df, K_pca, K_max, smoothing_h = 0L, df_min = 2.2, bucket_def = NULL) {
  pca <- compute_pca_factor(z_df, K_pca, K_max, smoothing_h)
  if (is.null(pca)) return(NULL)
  F_df <- pca$F
  F_fit <- fit_student_t_bucket(F_df$F_t, df_min = df_min)

  df <- z_df %>%
    dplyr::inner_join(F_df, by = "week") %>%
    dplyr::mutate(eps = z - pca$beta_k[rank] * F_t)

  if (!is.null(bucket_def)) {
    df <- df %>% dplyr::mutate(bucket = assign_bucket(rank, bucket_def))
  }

  eps_fit <- fit_student_t(df, mode = "bucket", df_min = df_min)

  nF <- length(F_df$F_t)
  llF <- F_fit$loglik %||% NA_real_
  aicF <- ifelse(is.finite(llF), 2 * 2 - 2 * llF, NA_real_)
  bicF <- ifelse(is.finite(llF), log(max(1, nF)) * 2 - 2 * llF, NA_real_)

  list(
    type = "factor_t",
    converged = isTRUE(F_fit$converged) && isTRUE(eps_fit$converged),
    beta_k = pca$beta_k,
    factor_fit = F_fit,
    idio_fit = eps_fit,
    loglik = (F_fit$loglik %||% 0) + (eps_fit$loglik %||% 0),
    aic = aicF + (eps_fit$aic %||% 0),
    bic = bicF + (eps_fit$bic %||% 0)
  )
}

fit_factor_kou <- function(z_df, K_pca, K_max, smoothing_h = 0L, p_max = 0.25, bucket_def = NULL) {
  pca <- compute_pca_factor(z_df, K_pca, K_max, smoothing_h)
  if (is.null(pca)) return(NULL)
  F_df <- pca$F
  F_fit <- fit_jump_kou_bucket(F_df$F_t, p_max = p_max)

  df <- z_df %>%
    dplyr::inner_join(F_df, by = "week") %>%
    dplyr::mutate(eps = z - pca$beta_k[rank] * F_t)

  if (!is.null(bucket_def)) {
    df <- df %>% dplyr::mutate(bucket = assign_bucket(rank, bucket_def))
  }

  eps_fit <- fit_jump_kou_asym(df %>% dplyr::rename(z = eps), mode = "bucket", p_max = p_max)

  nF <- length(F_df$F_t)
  llF <- F_fit$loglik %||% NA_real_
  aicF <- ifelse(is.finite(llF), 2 * 4 - 2 * llF, NA_real_)
  bicF <- ifelse(is.finite(llF), log(max(1, nF)) * 4 - 2 * llF, NA_real_)

  list(
    type = "factor_kou",
    converged = isTRUE(F_fit$converged) && isTRUE(eps_fit$converged),
    beta_k = pca$beta_k,
    factor_fit = F_fit,
    idio_fit = eps_fit,
    loglik = (F_fit$loglik %||% 0) + (eps_fit$loglik %||% 0),
    aic = aicF + (eps_fit$aic %||% 0),
    bic = bicF + (eps_fit$bic %||% 0)
  )
}

fit_factor_kou_tv <- function(z_df, K_pca, K_max, smoothing_h = 0L, p_max = 0.25, bucket_def = NULL, u = 3) {
  pca <- compute_pca_factor(z_df, K_pca, K_max, smoothing_h)
  if (is.null(pca)) return(NULL)
  F_df <- pca$F
  F_fit <- fit_jump_kou_bucket(F_df$F_t, p_max = p_max)

  # simple self-exciting intensity on factor jump indicator
  I_jump <- abs(F_df$F_t) > u
  if (length(I_jump) > 5) {
    I_prev <- I_jump[-length(I_jump)]
    I_now <- I_jump[-1]
    glm_fit <- tryCatch(stats::glm(I_now ~ I_prev, family = binomial()), error = function(e) NULL)
    if (!is.null(glm_fit)) {
      coefs <- stats::coef(glm_fit)
      p0 <- stats::plogis(coefs[1])
      alpha <- coefs[2]
    } else {
      p0 <- mean(I_jump)
      alpha <- 0
    }
  } else {
    p0 <- mean(I_jump)
    alpha <- 0
  }

  df <- z_df %>%
    dplyr::inner_join(F_df, by = "week") %>%
    dplyr::mutate(eps = z - pca$beta_k[rank] * F_t)

  if (!is.null(bucket_def)) {
    df <- df %>% dplyr::mutate(bucket = assign_bucket(rank, bucket_def))
  }

  eps_fit <- fit_jump_kou_asym(df %>% dplyr::rename(z = eps), mode = "bucket", p_max = p_max)

  list(
    type = "factor_kou_tv",
    converged = isTRUE(F_fit$converged) && isTRUE(eps_fit$converged),
    beta_k = pca$beta_k,
    factor_fit = F_fit,
    factor_tv = list(p0 = p0, alpha = alpha, threshold = u),
    idio_fit = eps_fit,
    loglik = (F_fit$loglik %||% 0) + (eps_fit$loglik %||% 0)
  )
}

# Simple two-state regime switching on factor series
fit_factor_regime <- function(z_df, K_pca, K_max, smoothing_h = 0L, bucket_def = NULL, max_iter = 200, tol = 1e-6, df_min = 2.2) {
  pca <- compute_pca_factor(z_df, K_pca, K_max, smoothing_h)
  if (is.null(pca)) return(NULL)
  F <- pca$F$F_t
  F <- F[is.finite(F)]
  if (length(F) < 20) return(NULL)

  # init
  p11 <- 0.9
  p22 <- 0.9
  sigma1 <- sd(F) * 0.5
  sigma2 <- sd(F) * 1.5

  ll_prev <- -Inf
  n <- length(F)
  for (iter in seq_len(max_iter)) {
    # emission densities
    d1 <- stats::dnorm(F, mean = 0, sd = sigma1)
    d2 <- stats::dnorm(F, mean = 0, sd = sigma2)

    # forward-backward (scaled)
    alpha <- matrix(0, n, 2)
    scale <- numeric(n)
    pi0 <- c(0.5, 0.5)
    alpha[1, ] <- pi0 * c(d1[1], d2[1])
    scale[1] <- sum(alpha[1, ])
    alpha[1, ] <- alpha[1, ] / scale[1]

    for (t in 2:n) {
      alpha[t, 1] <- d1[t] * (alpha[t - 1, 1] * p11 + alpha[t - 1, 2] * (1 - p22))
      alpha[t, 2] <- d2[t] * (alpha[t - 1, 1] * (1 - p11) + alpha[t - 1, 2] * p22)
      scale[t] <- sum(alpha[t, ])
      alpha[t, ] <- alpha[t, ] / scale[t]
    }

    beta <- matrix(0, n, 2)
    beta[n, ] <- c(1, 1)
    for (t in (n - 1):1) {
      beta[t, 1] <- (p11 * d1[t + 1] * beta[t + 1, 1] + (1 - p11) * d2[t + 1] * beta[t + 1, 2]) / scale[t + 1]
      beta[t, 2] <- ((1 - p22) * d1[t + 1] * beta[t + 1, 1] + p22 * d2[t + 1] * beta[t + 1, 2]) / scale[t + 1]
    }

    gamma <- alpha * beta
    gamma <- gamma / rowSums(gamma)

    xi11 <- sum(gamma[-n, 1] * (p11 * d1[-1] * beta[-1, 1]) / scale[-1])
    xi22 <- sum(gamma[-n, 2] * (p22 * d2[-1] * beta[-1, 2]) / scale[-1])
    gamma1 <- sum(gamma[-n, 1])
    gamma2 <- sum(gamma[-n, 2])

    p11 <- clamp_range(xi11 / max(gamma1, 1e-6), 0.5, 0.999)
    p22 <- clamp_range(xi22 / max(gamma2, 1e-6), 0.5, 0.999)

    sigma1 <- sqrt(sum(gamma[, 1] * F^2) / sum(gamma[, 1]))
    sigma2 <- sqrt(sum(gamma[, 2] * F^2) / sum(gamma[, 2]))
    if (sigma1 > sigma2) {
      tmp <- sigma1; sigma1 <- sigma2; sigma2 <- tmp
    }

    ll <- sum(log(pmax(scale, 1e-12)))
    if (is.finite(ll) && abs(ll - ll_prev) < tol) {
      break
    }
    ll_prev <- ll
  }
  converged <- is.finite(ll_prev)

  # fit idiosyncratic t on residuals
  F_df <- pca$F
  df_eps <- z_df %>%
    dplyr::inner_join(F_df, by = "week") %>%
    dplyr::mutate(eps = z - pca$beta_k[rank] * F_t)
  if (!is.null(bucket_def)) {
    df_eps <- df_eps %>% dplyr::mutate(bucket = assign_bucket(rank, bucket_def))
  }
  eps_fit <- fit_student_t(df_eps %>% dplyr::rename(z = eps), mode = "bucket", df_min = df_min)

  list(
    type = "factor_regime",
    converged = converged && isTRUE(eps_fit$converged),
    beta_k = pca$beta_k,
    factor_params = list(p11 = p11, p22 = p22, sigma1 = sigma1, sigma2 = sigma2),
    idio_fit = eps_fit,
    loglik = ll_prev + (eps_fit$loglik %||% 0)
  )
}

# --- model scoring and comparison ---

build_emp_targets <- function(endpoint_weekly, rank_panel, sm_params, K_cut, horizons_durable, bucket_def, K_xi,
                              tail_thresholds = c(3, 5), tail_probs = c(0.001, 0.01, 0.99, 0.999)) {
  emp_cdc <- compute_emp_cdc(endpoint_weekly, K_cut)
  emp_durable <- compute_emp_endpoint_change(
    df = endpoint_weekly %>% dplyr::filter(rank <= K_cut),
    horizons = horizons_durable,
    bucket_def = bucket_def
  )

  xi_weekly <- endpoint_weekly %>%
    dplyr::group_by(week) %>%
    dplyr::arrange(rank, .by_group = TRUE) %>%
    dplyr::mutate(
      w = pmax(share_global, 1e-18),
      w_next = dplyr::lead(w),
      xi = log(w / w_next),
      k = rank
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(is.finite(xi), is.finite(k))

  emp_xi <- xi_weekly %>%
    dplyr::filter(k <= (K_xi - 1L)) %>%
    dplyr::group_by(k) %>%
    dplyr::summarise(rho_k = median(xi, na.rm = TRUE), .groups = "drop")

  rank_inc <- build_rank_slot_increments(rank_panel, K_cut)
  z_rank <- standardize_increments(rank_inc, sm_params$mean_dlogw_s, sm_params$sd_dlogw_s, bucket_def = bucket_def)
  z_rank <- z_rank %>% dplyr::mutate(z = dlogw_std)
  tail_emp <- tail_skew_diagnostics(z_rank, thresholds = tail_thresholds, probs = tail_probs)

  list(
    cdc = emp_cdc,
    durable = emp_durable,
    xi = emp_xi,
    tail = tail_emp
  )
}

prepare_model_for_sim <- function(fit, cfg, moment_curves) {
  if (is.null(fit)) return(NULL)
  model <- list(type = fit$type, mu = cfg$sim_mu, sigma = cfg$sim_sigma)

  if (fit$type == "student_t") {
    if (is.data.frame(fit$params)) {
      model$df <- setNames(fit$params$df, fit$params$bucket)
      model$scale <- setNames(fit$params$scale, fit$params$bucket)
    } else {
      model$df <- fit$params$df %||% fit$params
      model$scale <- fit$params$scale %||% 1
    }
  }
  if (fit$type == "skew_t") {
    model$skew_params <- fit$params
  }
  if (fit$type == "ghyp") {
    model$ghyp_params <- fit$params
  }
  if (fit$type == "mixture_gaussian") {
    if (is.data.frame(fit$params)) {
      model$p <- setNames(fit$params$p, fit$params$bucket)
      model$mu1 <- setNames(fit$params$mu1, fit$params$bucket)
      model$sd1 <- setNames(fit$params$sd1, fit$params$bucket)
      model$mu2 <- setNames(fit$params$mu2, fit$params$bucket)
      model$sd2 <- setNames(fit$params$sd2, fit$params$bucket)
    } else {
      model$p <- fit$params$p
      model$mu1 <- fit$params$mu1
      model$sd1 <- fit$params$sd1
      model$mu2 <- fit$params$mu2
      model$sd2 <- fit$params$sd2
    }
  }
  if (fit$type == "jump_merton") {
    if (is.data.frame(fit$params)) {
      model$p <- setNames(fit$params$p, fit$params$bucket)
      model$mu_j <- setNames(fit$params$mu_j, fit$params$bucket)
      model$sd_j <- setNames(fit$params$sd_j, fit$params$bucket)
    } else {
      model$p <- fit$params$p
      model$mu_j <- fit$params$mu_j
      model$sd_j <- fit$params$sd_j
    }
  }
  if (fit$type == "jump_kou") {
    if (is.data.frame(fit$params)) {
      model$p <- setNames(fit$params$p, fit$params$bucket)
      model$pi_pos <- setNames(fit$params$pi_pos, fit$params$bucket)
      model$eta_pos <- setNames(fit$params$eta_pos, fit$params$bucket)
      model$eta_neg <- setNames(fit$params$eta_neg, fit$params$bucket)
    } else {
      model$p <- fit$params$p
      model$pi_pos <- fit$params$pi_pos
      model$eta_pos <- fit$params$eta_pos
      model$eta_neg <- fit$params$eta_neg
    }
  }
  if (fit$type == "factor_t") {
    model$beta_k <- fit$beta_k
    model$factor_df <- fit$factor_fit$df
    model$factor_scale <- fit$factor_fit$scale
    if (is.data.frame(fit$idio_fit$params)) {
      model$idio_df <- setNames(fit$idio_fit$params$df, fit$idio_fit$params$bucket)
      model$idio_scale <- setNames(fit$idio_fit$params$scale, fit$idio_fit$params$bucket)
    } else {
      model$idio_df <- fit$idio_fit$params$df
      model$idio_scale <- fit$idio_fit$params$scale
    }
  }
  if (fit$type == "factor_kou") {
    model$beta_k <- fit$beta_k
    model$factor_params <- fit$factor_fit
    model$idio_params <- fit$idio_fit
  }
  if (fit$type == "factor_kou_tv") {
    model$beta_k <- fit$beta_k
    model$factor_params <- fit$factor_fit
    model$factor_tv <- fit$factor_tv
    model$idio_params <- fit$idio_fit
  }
  if (fit$type == "factor_regime") {
    model$beta_k <- fit$beta_k
    model$factor_params <- fit$factor_params
    model$idio_params <- fit$idio_fit
  }
  model
}

loglik_for_model <- function(fit, z_df, bucket_def = NULL) {
  if (is.null(fit)) return(NA_real_)
  if (!"bucket" %in% names(z_df) && !is.null(bucket_def)) {
    z_df <- z_df %>% dplyr::mutate(bucket = assign_bucket(rank, bucket_def))
  }

  if (fit$type == "skew_t") {
    if (!has_pkg("sn")) return(NA_real_)
    params <- fit$params
    split_df <- split(z_df, z_df$bucket)
    return(sum(purrr::map_dbl(names(split_df), function(b) {
      z <- split_df[[b]]$z
      row <- params[params$bucket == b, ]
      if (nrow(row) == 0) return(NA_real_)
      dp <- row$dp[[1]]
      if (is.null(dp)) return(NA_real_)
      sum(sn::dst(z, dp = dp, log = TRUE))
    }), na.rm = TRUE))
  }

  if (fit$type == "ghyp") {
    if (!has_pkg("ghyp")) return(NA_real_)
    params <- fit$params
    split_df <- split(z_df, z_df$bucket)
    return(sum(purrr::map_dbl(names(split_df), function(b) {
      z <- split_df[[b]]$z
      row <- params[params$bucket == b, ]
      if (nrow(row) == 0) return(NA_real_)
      obj <- row$obj[[1]]
      if (is.null(obj)) return(NA_real_)
      sum(ghyp::dghyp(z, object = obj, log = TRUE))
    }), na.rm = TRUE))
  }

  if (fit$type == "student_t") {
    params <- fit$params
    split_df <- split(z_df, z_df$bucket)
    return(sum(purrr::map_dbl(names(split_df), function(b) {
      z <- split_df[[b]]$z
      row <- params[params$bucket == b, ]
      if (nrow(row) == 0) return(NA_real_)
      loglik_student_t(z, row$df[1], row$scale[1])
    }), na.rm = TRUE))
  }

  if (fit$type == "mixture_gaussian") {
    params <- fit$params
    if (!is.data.frame(params)) return(NA_real_)
    split_df <- split(z_df, z_df$bucket)
    return(sum(purrr::map_dbl(names(split_df), function(b) {
      z <- split_df[[b]]$z
      row <- params[params$bucket == b, ]
      if (nrow(row) == 0) return(NA_real_)
      loglik_gaussian_mixture(z, row$p[1], row$mu1[1], row$sd1[1], row$mu2[1], row$sd2[1])
    }), na.rm = TRUE))
  }

  if (fit$type == "jump_merton") {
    params <- fit$params
    if (!is.data.frame(params)) return(NA_real_)
    split_df <- split(z_df, z_df$bucket)
    return(sum(purrr::map_dbl(names(split_df), function(b) {
      z <- split_df[[b]]$z
      row <- params[params$bucket == b, ]
      if (nrow(row) == 0) return(NA_real_)
      loglik_merton(z, row$p[1], row$mu_j[1], row$sd_j[1])
    }), na.rm = TRUE))
  }

  if (fit$type == "jump_kou") {
    params <- fit$params
    if (!is.data.frame(params)) return(NA_real_)
    split_df <- split(z_df, z_df$bucket)
    return(sum(purrr::map_dbl(names(split_df), function(b) {
      z <- split_df[[b]]$z
      row <- params[params$bucket == b, ]
      if (nrow(row) == 0) return(NA_real_)
      loglik_kou(z, row$p[1], row$pi_pos[1], row$eta_pos[1], row$eta_neg[1])
    }), na.rm = TRUE))
  }

  if (fit$type %in% c("factor_t", "factor_kou", "factor_kou_tv", "factor_regime")) {
    return(fit$loglik %||% NA_real_)
  }

  NA_real_
}

calc_tail_mismatch <- function(emp_tail, sim_tail) {
  if (is.null(emp_tail) || is.null(sim_tail)) return(NA_real_)
  key_cols <- intersect(names(emp_tail), names(sim_tail))
  metrics <- setdiff(key_cols, c("bucket", "rank_bin", "n"))
  if (length(metrics) == 0) return(NA_real_)
  emp_long <- emp_tail %>%
    dplyr::select(dplyr::any_of(c("bucket", "rank_bin")), dplyr::all_of(metrics)) %>%
    tidyr::pivot_longer(-dplyr::any_of(c("bucket", "rank_bin")), names_to = "metric", values_to = "emp")
  sim_long <- sim_tail %>%
    dplyr::select(dplyr::any_of(c("bucket", "rank_bin")), dplyr::all_of(metrics)) %>%
    tidyr::pivot_longer(-dplyr::any_of(c("bucket", "rank_bin")), names_to = "metric", values_to = "sim")
  cmp <- emp_long %>%
    dplyr::left_join(sim_long, by = c("bucket", "rank_bin", "metric"))
  mean((cmp$emp - cmp$sim)^2, na.rm = TRUE)
}

calc_skew_mismatch <- function(emp_tail, sim_tail) {
  if (is.null(emp_tail) || is.null(sim_tail)) return(NA_real_)
  key_cols <- intersect(names(emp_tail), names(sim_tail))
  metrics <- setdiff(key_cols, c("bucket", "rank_bin", "n"))
  skew_metrics <- metrics[grepl("skew|Delta_tail|R_999_001", metrics)]
  if (length(skew_metrics) == 0) return(NA_real_)
  emp_long <- emp_tail %>%
    dplyr::select(dplyr::any_of(c("bucket", "rank_bin")), dplyr::all_of(skew_metrics)) %>%
    tidyr::pivot_longer(-dplyr::any_of(c("bucket", "rank_bin")), names_to = "metric", values_to = "emp")
  sim_long <- sim_tail %>%
    dplyr::select(dplyr::any_of(c("bucket", "rank_bin")), dplyr::all_of(skew_metrics)) %>%
    tidyr::pivot_longer(-dplyr::any_of(c("bucket", "rank_bin")), names_to = "metric", values_to = "sim")
  cmp <- emp_long %>%
    dplyr::left_join(sim_long, by = c("bucket", "rank_bin", "metric"))
  mean((cmp$emp - cmp$sim)^2, na.rm = TRUE)
}

simulate_from_fit <- function(fit, cfg, moment_curves, w0_ext, entrant_sampler, bucket_def, K_cut, K_max, K_xi) {
  model_sim <- prepare_model_for_sim(fit, cfg, moment_curves)
  simulate_rank_paths(
    w0 = w0_ext,
    K_cut = K_cut,
    K_max = K_max,
    T = cfg$sim_T_weeks,
    n_paths = cfg$model_zoo_sim_paths,
    mu = cfg$sim_mu,
    sigma = cfg$sim_sigma,
    entry_frac = cfg$sim_entry_frac,
    mean_vec = moment_curves$mean_vec,
    sd_vec = moment_curves$sd_vec,
    horizons = cfg$horizons_durable,
    K_xi = K_xi,
    model = model_sim,
    moment_curves = moment_curves,
    cache = list(beta_k = fit$beta_k, state = new.env(parent = emptyenv())),
    entrant_sampler = entrant_sampler,
    bucket_def = bucket_def
  )
}

eval_from_sim <- function(sim_out, targets_emp, bucket_def) {
  sim_cdc <- sim_out$snapshots %>%
    dplyr::group_by(rank) %>%
    dplyr::summarise(w_bar = mean(share, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(rank)
  list(
    rmse_cdc = cdc_rmse(targets_emp$cdc, sim_cdc),
    rmse_durable = durable_rmse(targets_emp$durable, sim_out$growth, bucket_def),
    rmse_xi = xi_rmse(targets_emp$xi, sim_out$xi)
  )
}

build_model_registry <- function(cfg, bucket_def, pca_K, K_max) {
  registry <- list(
    A1_student_t = list(
      name = "A1 Student-t",
      family = "A",
      fit_fn = function(z) fit_student_t(z, mode = "bucket", df_min = cfg$model_zoo_df_min),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    ),
    B3_gaussian_mix = list(
      name = "B3 Gaussian Mixture",
      family = "B",
      fit_fn = function(z) fit_mixture_gaussian(z, mode = "bucket", p_max = cfg$model_zoo_jump_p_max),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    ),
    B1_merton = list(
      name = "B1 Merton",
      family = "B",
      fit_fn = function(z) fit_jump_merton(z, mode = "bucket", p_max = cfg$model_zoo_jump_p_max),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    ),
    B2_kou = list(
      name = "B2 Kou (asymmetric)",
      family = "B",
      fit_fn = function(z) fit_jump_kou_asym(z, mode = cfg$model_zoo_param_mode, p_max = cfg$model_zoo_jump_p_max),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    ),
    C1_factor_t = list(
      name = "C1 Factor + t",
      family = "C",
      fit_fn = function(z) fit_factor_t(z, pca_K, K_max, smoothing_h = cfg$smoothing_h, df_min = cfg$model_zoo_df_min, bucket_def = bucket_def),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    ),
    C2_factor_kou = list(
      name = "C2 Factor + Kou",
      family = "C",
      fit_fn = function(z) fit_factor_kou(z, pca_K, K_max, smoothing_h = cfg$smoothing_h, p_max = cfg$model_zoo_jump_p_max, bucket_def = bucket_def),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    )
  )

  if (has_pkg("sn")) {
    registry$A2_skew_t <- list(
      name = "A2 Skew-t",
      family = "A",
      fit_fn = function(z) fit_skew_t(z, mode = "bucket"),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    )
  }
  if (has_pkg("ghyp")) {
    registry$A3_ghyp <- list(
      name = "A3 GH",
      family = "A",
      fit_fn = function(z) fit_ghyp(z, mode = "bucket"),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    )
  }
  if (isTRUE(cfg$enable_jump_clustering_models)) {
    registry$C3_factor_kou_tv <- list(
      name = "C3 Factor + Kou (TV intensity)",
      family = "C",
      fit_fn = function(z) fit_factor_kou_tv(z, pca_K, K_max, smoothing_h = cfg$smoothing_h, p_max = cfg$model_zoo_jump_p_max, bucket_def = bucket_def),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    )
    registry$D2_factor_regime <- list(
      name = "D2 Factor Regime",
      family = "D",
      fit_fn = function(z) fit_factor_regime(z, pca_K, K_max, smoothing_h = cfg$smoothing_h, bucket_def = bucket_def, df_min = cfg$model_zoo_df_min),
      simulate_fn = simulate_from_fit,
      eval_fn = eval_from_sim
    )
  }

  registry
}

run_model_comparison <- function(
  cfg,
  endpoint_weekly,
  rank_panel,
  sm_params,
  w0_ext,
  entrant_sampler,
  bucket_def,
  K_cut,
  K_max,
  K_xi,
  pca_K
) {
  if (!isTRUE(cfg$run_jump_model_zoo)) {
    message("run_jump_model_zoo is FALSE; skipping model comparison")
    return(NULL)
  }

  moment_curves <- list(
    mean_vec = sm_params$mean_dlogw_s,
    sd_vec = sm_params$sd_dlogw_s,
    bucket_def = bucket_def
  )

  # Build standardized increments (rank-slot)
  rank_inc <- build_rank_slot_increments(rank_panel, K_cut)
  z_rank <- standardize_increments(rank_inc, sm_params$mean_dlogw_s, sm_params$sd_dlogw_s, bucket_def = bucket_def)
  z_rank <- z_rank %>% dplyr::mutate(z = dlogw_std)

  # Train/test split by week
  weeks <- sort(unique(z_rank$week))
  n_train <- max(2, floor(length(weeks) * cfg$model_zoo_train_frac))
  train_weeks <- weeks[seq_len(n_train)]
  test_weeks <- weeks[(n_train + 1):length(weeks)]
  z_train <- z_rank %>% dplyr::filter(week %in% train_weeks)
  z_test <- z_rank %>% dplyr::filter(week %in% test_weeks)

  # Empirical targets (full + test)
  targets_emp <- build_emp_targets(
    endpoint_weekly, rank_panel, sm_params, K_cut, cfg$horizons_durable, bucket_def, K_xi,
    tail_thresholds = cfg$model_zoo_tail_thresholds,
    tail_probs = cfg$model_zoo_tail_probs
  )
  targets_emp_test <- build_emp_targets(
    endpoint_weekly %>% dplyr::filter(week %in% test_weeks),
    rank_panel %>% dplyr::filter(week %in% test_weeks),
    sm_params,
    K_cut,
    cfg$horizons_durable,
    bucket_def,
    K_xi,
    tail_thresholds = cfg$model_zoo_tail_thresholds,
    tail_probs = cfg$model_zoo_tail_probs
  )

  models <- build_model_registry(cfg, bucket_def, pca_K, K_max)

  results <- list()

  for (model_id in names(models)) {
    model_meta <- models[[model_id]]
    fit <- tryCatch(model_meta$fit_fn(z_train), error = function(e) {
      message("Model ", model_id, " fit failed: ", conditionMessage(e))
      NULL
    })

    if (is.null(fit)) {
      results[[model_id]] <- list(id = model_id, converged = FALSE)
      next
    }

    fit$id <- model_id

    model_sim <- prepare_model_for_sim(fit, cfg, moment_curves)

    sim_out <- tryCatch({
      simulate_rank_paths(
        w0 = w0_ext,
        K_cut = K_cut,
        K_max = K_max,
        T = cfg$sim_T_weeks,
        n_paths = cfg$model_zoo_sim_paths,
        mu = cfg$sim_mu,
        sigma = cfg$sim_sigma,
        entry_frac = cfg$sim_entry_frac,
        mean_vec = moment_curves$mean_vec,
        sd_vec = moment_curves$sd_vec,
        horizons = cfg$horizons_durable,
        K_xi = K_xi,
        model = model_sim,
        moment_curves = moment_curves,
        cache = list(beta_k = fit$beta_k, state = new.env(parent = emptyenv())),
        entrant_sampler = entrant_sampler,
        bucket_def = bucket_def
      )
    }, error = function(e) NULL)

    if (is.null(sim_out)) {
      results[[model_id]] <- list(id = model_id, converged = FALSE)
      next
    }

    sim_cdc <- sim_out$snapshots %>%
      dplyr::group_by(rank) %>%
      dplyr::summarise(w_bar = mean(share, na.rm = TRUE), .groups = "drop") %>%
      dplyr::arrange(rank)

    rmse_cdc <- cdc_rmse(targets_emp$cdc, sim_cdc)
    rmse_durable <- durable_rmse(targets_emp$durable, sim_out$growth, bucket_def)
    rmse_xi <- xi_rmse(targets_emp$xi, sim_out$xi)

    rmse_cdc_test <- if (nrow(targets_emp_test$cdc) > 0) cdc_rmse(targets_emp_test$cdc, sim_cdc) else NA_real_
    rmse_durable_test <- if (nrow(targets_emp_test$durable) > 0) durable_rmse(targets_emp_test$durable, sim_out$growth, bucket_def) else NA_real_
    rmse_xi_test <- if (nrow(targets_emp_test$xi) > 0) xi_rmse(targets_emp_test$xi, sim_out$xi) else NA_real_

    # tail diagnostics from simulated increments
    sim_inc_tail <- sample_increments_for_tail(
      model = model_sim,
      moment_curves = moment_curves,
      cache = list(beta_k = fit$beta_k, state = new.env(parent = emptyenv())),
      K_cut = K_cut,
      n_steps = cfg$model_zoo_tail_steps,
      n_paths = cfg$model_zoo_tail_paths,
      ranks_per_bucket = cfg$model_zoo_tail_ranks_per_bucket
    )

    sim_inc_z <- standardize_increments(sim_inc_tail, sm_params$mean_dlogw_s, sm_params$sd_dlogw_s, bucket_def = bucket_def)
    sim_inc_z <- sim_inc_z %>% dplyr::mutate(z = dlogw_std)
    tail_sim <- tail_skew_diagnostics(sim_inc_z, thresholds = cfg$model_zoo_tail_thresholds, probs = cfg$model_zoo_tail_probs)

    tail_mismatch <- calc_tail_mismatch(targets_emp$tail, tail_sim)
    skew_mismatch <- calc_skew_mismatch(targets_emp$tail, tail_sim)

    ll_train <- loglik_for_model(fit, z_train, bucket_def)
    ll_test <- if (nrow(z_test) > 0) loglik_for_model(fit, z_test, bucket_def) else NA_real_

    score <- cfg$model_zoo_weight_cdc * rmse_cdc +
      cfg$model_zoo_weight_durable * rmse_durable +
      cfg$model_zoo_weight_xi * rmse_xi +
      cfg$model_zoo_weight_tail * tail_mismatch +
      cfg$model_zoo_weight_skew * skew_mismatch
    if (!isTRUE(fit$converged)) score <- Inf

    results[[model_id]] <- list(
      id = model_id,
      name = model_meta$name,
      family = model_meta$family,
      type = fit$type,
      converged = isTRUE(fit$converged),
      aic = fit$aic,
      bic = fit$bic,
      loglik = fit$loglik,
      loglik_train = ll_train,
      loglik_test = ll_test,
      rmse_cdc = rmse_cdc,
      rmse_durable = rmse_durable,
      rmse_xi = rmse_xi,
      rmse_cdc_test = rmse_cdc_test,
      rmse_durable_test = rmse_durable_test,
      rmse_xi_test = rmse_xi_test,
      tail_mismatch = tail_mismatch,
      skew_mismatch = skew_mismatch,
      score = score,
      fit_fn = model_meta$fit_fn,
      fit = fit,
      tail_sim = tail_sim
    )
  }

  table <- purrr::map_dfr(results, function(x) {
    tibble::tibble(
      model = x$id,
      name = x$name,
      family = x$family,
      type = x$type,
      converged = x$converged,
      aic = x$aic,
      bic = x$bic,
      loglik = x$loglik,
      loglik_train = x$loglik_train,
      loglik_test = x$loglik_test,
      rmse_cdc = x$rmse_cdc,
      rmse_durable = x$rmse_durable,
      rmse_xi = x$rmse_xi,
      rmse_cdc_test = x$rmse_cdc_test,
      rmse_durable_test = x$rmse_durable_test,
      rmse_xi_test = x$rmse_xi_test,
      tail_mismatch = x$tail_mismatch,
      skew_mismatch = x$skew_mismatch,
      score = x$score
    )
  })

  table <- table %>%
    dplyr::mutate(score = ifelse(is.na(score), Inf, score)) %>%
    dplyr::arrange(score)
  best_id <- table$model[which.min(table$score)]
  best_fit <- results[[best_id]]$fit

  list(
    model_table = table,
    best_model = best_fit,
    results = results,
    targets_emp = targets_emp,
    z_rank = z_rank,
    z_train = z_train,
    z_test = z_test
  )
}

block_sample_weeks <- function(weeks, block_length = 4L) {
  weeks <- sort(unique(weeks))
  n <- length(weeks)
  if (n == 0) return(weeks)
  L <- max(1L, as.integer(block_length))
  n_blocks <- ceiling(n / L)
  starts <- sample.int(n, size = n_blocks, replace = TRUE)
  idx <- unlist(lapply(starts, function(s) s:min(n, s + L - 1L)))
  if (length(idx) > n) idx <- idx[seq_len(n)]
  weeks[idx]
}

flatten_fit_params <- function(fit) {
  if (is.null(fit)) return(NULL)
  out <- c()
  if (fit$type == "student_t") {
    params <- fit$params
    if (is.data.frame(params)) {
      for (i in seq_len(nrow(params))) {
        b <- params$bucket[i]
        out[paste0("df_", b)] <- params$df[i]
        out[paste0("scale_", b)] <- params$scale[i]
      }
    }
  }
  if (fit$type == "jump_merton") {
    params <- fit$params
    if (is.data.frame(params)) {
      for (i in seq_len(nrow(params))) {
        b <- params$bucket[i]
        out[paste0("p_", b)] <- params$p[i]
        out[paste0("mu_j_", b)] <- params$mu_j[i]
        out[paste0("sd_j_", b)] <- params$sd_j[i]
      }
    }
  }
  if (fit$type == "jump_kou") {
    params <- fit$params
    if (is.data.frame(params)) {
      for (i in seq_len(nrow(params))) {
        b <- params$bucket[i]
        out[paste0("p_", b)] <- params$p[i]
        out[paste0("pi_pos_", b)] <- params$pi_pos[i]
        out[paste0("eta_pos_", b)] <- params$eta_pos[i]
        out[paste0("eta_neg_", b)] <- params$eta_neg[i]
      }
    }
  }
  if (fit$type == "mixture_gaussian") {
    params <- fit$params
    if (is.data.frame(params)) {
      for (i in seq_len(nrow(params))) {
        b <- params$bucket[i]
        out[paste0("p_", b)] <- params$p[i]
        out[paste0("mu1_", b)] <- params$mu1[i]
        out[paste0("sd1_", b)] <- params$sd1[i]
        out[paste0("mu2_", b)] <- params$mu2[i]
        out[paste0("sd2_", b)] <- params$sd2[i]
      }
    }
  }
  if (fit$type %in% c("factor_kou", "factor_kou_tv")) {
    fp <- fit$factor_fit
    if (!is.null(fp)) {
      out["factor_p"] <- fp$p
      out["factor_pi_pos"] <- fp$pi_pos
      out["factor_eta_pos"] <- fp$eta_pos
      out["factor_eta_neg"] <- fp$eta_neg
    }
    if (!is.null(fit$factor_tv)) {
      out["factor_p0"] <- fit$factor_tv$p0
      out["factor_alpha"] <- fit$factor_tv$alpha
    }
  }
  if (length(out) == 0) return(NULL)
  out
}

bootstrap_model_params <- function(fit_fn, z_df, B = 100L, block_length = 4L) {
  weeks <- sort(unique(z_df$week))
  res <- vector("list", B)
  for (b in seq_len(B)) {
    w_samp <- block_sample_weeks(weeks, block_length = block_length)
    z_b <- z_df %>% dplyr::filter(week %in% w_samp)
    fit_b <- tryCatch(fit_fn(z_b), error = function(e) NULL)
    res[[b]] <- flatten_fit_params(fit_b)
  }
  mat <- do.call(rbind, res)
  if (is.null(mat)) return(NULL)
  mat <- as.data.frame(mat)
  tibble::tibble(
    param = names(mat),
    q05 = apply(mat, 2, function(x) stats::quantile(x, 0.05, na.rm = TRUE)),
    q50 = apply(mat, 2, function(x) stats::quantile(x, 0.50, na.rm = TRUE)),
    q95 = apply(mat, 2, function(x) stats::quantile(x, 0.95, na.rm = TRUE))
  )
}
