build_pool_by_rank <- function(rank_panel, K_max, value_col = "dlogw") {
  pools <- vector("list", K_max)
  if (!value_col %in% names(rank_panel)) {
    stop("value_col not in rank_panel: ", value_col)
  }
  rp <- dplyr::filter(rank_panel, rank <= K_max)
  split_vals <- split(rp[[value_col]], rp$rank)
  for (r in seq_len(K_max)) {
    v <- split_vals[[as.character(r)]]
    if (is.null(v)) v <- numeric(0)
    pools[[r]] <- v
  }
  pools
}

resolve_rank_bandwidth <- function(rank_bandwidth, r) {
  h <- if (is.function(rank_bandwidth)) {
    rank_bandwidth(r)
  } else if (length(rank_bandwidth) > 1L) {
    rank_bandwidth[pmin(r, length(rank_bandwidth))]
  } else {
    rank_bandwidth
  }
  h <- as.integer(round(h))
  if (!is.finite(h) || h < 0L) h <- 0L
  h
}

kernel_weights <- function(dist, h, kernel) {
  if (length(dist) == 0) return(numeric(0))
  if (h <= 0L) return(rep(1, length(dist)))
  if (kernel == "uniform") return(rep(1, length(dist)))
  if (kernel == "triangular") return(pmax(0, 1 - dist / (h + 1)))
  if (kernel == "exp") {
    tau <- max(1e-6, h / 2)
    return(exp(-dist / tau))
  }
  rep(1, length(dist))
}

build_rank_neighborhoods <- function(K_max, rank_bandwidth, kernel = "uniform") {
  neighbors <- vector("list", K_max)
  probs <- vector("list", K_max)
  for (r in seq_len(K_max)) {
    h <- resolve_rank_bandwidth(rank_bandwidth, r)
    lo <- max(1L, r - h)
    hi <- min(K_max, r + h)
    nbrs <- lo:hi
    dist <- abs(nbrs - r)
    w <- kernel_weights(dist, h, kernel)
    if (length(w) == 0 || sum(w) <= 0) w <- rep(1, length(nbrs))
    w <- w / sum(w)
    neighbors[[r]] <- nbrs
    probs[[r]] <- w
  }
  list(neighbors = neighbors, probs = probs)
}

stationary_bootstrap_indices <- function(n, L, size) {
  if (n <= 0) stop("n must be positive")
  if (L <= 1) return(sample.int(n, size = size, replace = TRUE))
  p <- 1 / L
  idx <- integer(size)
  idx[1] <- sample.int(n, size = 1)
  for (i in 2:size) {
    if (runif(1) < p) {
      idx[i] <- sample.int(n, size = 1)
    } else {
      idx[i] <- idx[i - 1L] + 1L
      if (idx[i] > n) idx[i] <- 1L
    }
  }
  idx
}

gpd_fit_mle <- function(excess) {
  excess <- excess[is.finite(excess) & excess >= 0]
  if (length(excess) < 5) return(NULL)

  nll <- function(par) {
    xi <- par[1]
    beta <- par[2]
    if (!is.finite(beta) || beta <= 0) return(Inf)
    if (any(1 + xi * excess / beta <= 0)) return(Inf)
    if (abs(xi) < 1e-6) {
      return(length(excess) * log(beta) + sum(excess) / beta)
    }
    length(excess) * log(beta) + (1 / xi + 1) * sum(log(1 + xi * excess / beta))
  }

  init <- c(0.1, max(mean(excess), 1e-6))
  fit <- tryCatch(
    optim(
      init,
      nll,
      method = "L-BFGS-B",
      lower = c(-0.5, 1e-6),
      upper = c(5, Inf)
    ),
    error = function(e) NULL
  )

  if (is.null(fit) || fit$convergence != 0 || any(!is.finite(fit$par))) return(NULL)
  list(xi = fit$par[1], beta = fit$par[2])
}

rgpd <- function(n, xi, beta) {
  if (n <= 0) return(numeric(0))
  u <- runif(n)
  if (abs(xi) < 1e-6) return(rexp(n, rate = 1 / beta))
  beta / xi * (u^(-xi) - 1)
}

build_tail_splice_fits <- function(residual_pools, bucket_assigner, tail_q, tail_fit_min_n) {
  if (is.null(bucket_assigner)) return(NULL)
  bucket_by_rank <- bucket_assigner(seq_along(residual_pools))
  bucket_levels <- unique(bucket_by_rank[!is.na(bucket_by_rank)])
  if (length(bucket_levels) == 0) return(NULL)

  fits <- list()
  for (bucket in bucket_levels) {
    ranks <- which(bucket_by_rank == bucket)
    resids <- unlist(residual_pools[ranks], use.names = FALSE)
    resids <- resids[is.finite(resids)]
    if (length(resids) == 0) next

    abs_resids <- abs(resids)
    u <- as.numeric(stats::quantile(abs_resids, tail_q, na.rm = TRUE))
    exceed <- abs_resids[abs_resids > u] - u
    if (length(exceed) < tail_fit_min_n) next

    fit <- gpd_fit_mle(exceed)
    if (is.null(fit)) next

    fits[[as.character(bucket)]] <- list(
      u = u,
      p_tail = mean(abs_resids > u, na.rm = TRUE),
      sign_prob = mean(resids[abs_resids > u] >= 0, na.rm = TRUE),
      xi = fit$xi,
      beta = fit$beta
    )
  }

  list(fits = fits, bucket_by_rank = bucket_by_rank)
}

sample_from_pool <- function(pool) {
  if (length(pool) == 0) return(0)
  pool[sample.int(length(pool), 1L)]
}

sample_from_pool_tail_splice <- function(pool, tail_fit) {
  if (length(pool) == 0) return(0)
  if (is.null(tail_fit)) return(sample_from_pool(pool))

  u <- tail_fit$u
  body <- pool[abs(pool) <= u]
  if (length(body) == 0) body <- pool

  if (runif(1) < tail_fit$p_tail) {
    sign <- ifelse(runif(1) < tail_fit$sign_prob, 1, -1)
    return(sign * (u + rgpd(1, tail_fit$xi, tail_fit$beta)))
  }

  body[sample.int(length(body), 1L)]
}

build_sampler_iid_rank <- function(rank_panel, cfg) {
  pool_by_rank <- build_pool_by_rank(rank_panel, cfg$K_max, "dlogw")

  sample_one_week <- function(mu = 0, sigma = 1) {
    inc <- numeric(cfg$K_max)
    for (r in seq_len(cfg$K_max)) {
      inc[r] <- sample_from_pool(pool_by_rank[[r]])
    }
    mu + sigma * inc
  }

  list(
    method = "iid_rank",
    K_max = cfg$K_max,
    sample_one_week = sample_one_week,
    debug_info = list()
  )
}

build_sampler_local_rank <- function(rank_panel, cfg) {
  pool_by_rank <- build_pool_by_rank(rank_panel, cfg$K_max, "dlogw")
  neighborhoods <- build_rank_neighborhoods(cfg$K_max, cfg$rank_bandwidth, cfg$kernel)

  sample_one_week <- function(mu = 0, sigma = 1) {
    inc <- numeric(cfg$K_max)
    for (r in seq_len(cfg$K_max)) {
      nbrs <- neighborhoods$neighbors[[r]]
      probs <- neighborhoods$probs[[r]]
      r_prime <- if (length(nbrs) == 1L) nbrs else sample(nbrs, size = 1L, prob = probs)
      inc[r] <- sample_from_pool(pool_by_rank[[r_prime]])
    }
    mu + sigma * inc
  }

  list(
    method = "local_rank",
    K_max = cfg$K_max,
    sample_one_week = sample_one_week,
    debug_info = list(neighborhoods = neighborhoods)
  )
}

build_sampler_week_vector <- function(rank_panel, cfg) {
  max_rank_by_week <- rank_panel %>%
    dplyr::group_by(week) %>%
    dplyr::summarise(max_rank = max(rank), .groups = "drop")

  K_use <- min(cfg$K_max, max(max_rank_by_week$max_rank, na.rm = TRUE))
  if (K_use < cfg$K_max) {
    stop(
      "week_vector bootstrap requires cfg$K_max <= max observed rank. ",
      "Got K_max=", cfg$K_max, " but max observed rank is ", K_use, "."
    )
  }

  rank_wide <- rank_panel %>%
    dplyr::filter(rank <= K_use) %>%
    dplyr::select(week, rank, dlogw) %>%
    tidyr::pivot_wider(names_from = rank, values_from = dlogw) %>%
    dplyr::arrange(week)

  X <- as.matrix(rank_wide %>% dplyr::select(-week))
  complete <- complete.cases(X)
  X_complete <- X[complete, , drop = FALSE]

  if (nrow(X_complete) < 2) {
    stop("Not enough complete weeks for week_vector bootstrap.")
  }

  sample_path <- function(T_sim, mu = 0, sigma = 1) {
    idx <- if (isTRUE(cfg$block_bootstrap)) {
      stationary_bootstrap_indices(nrow(X_complete), cfg$block_length, T_sim)
    } else {
      sample.int(nrow(X_complete), T_sim, replace = TRUE)
    }

    mat <- X_complete[idx, , drop = FALSE]
    mu + sigma * mat
  }

  list(
    method = "week_vector",
    K_max = cfg$K_max,
    sample_path = sample_path,
    debug_info = list(K_use = K_use)
  )
}

build_sampler_factor_local <- function(rank_panel, cfg, bucket_assigner = NULL) {
  K_max <- cfg$K_max
  K_pca <- min(cfg$K_pca, K_max)

  rp <- dplyr::filter(rank_panel, rank <= K_max)

  rank_wide <- rp %>%
    dplyr::filter(rank <= K_pca) %>%
    dplyr::select(week, rank, dlogw) %>%
    tidyr::pivot_wider(names_from = rank, values_from = dlogw) %>%
    dplyr::arrange(week)

  X <- as.matrix(rank_wide %>% dplyr::select(-week))
  complete <- complete.cases(X)
  X_complete <- X[complete, , drop = FALSE]

  if (nrow(X_complete) < 2) {
    stop("Not enough complete weeks for factor PCA.")
  }

  pca_fit <- stats::prcomp(X_complete, center = TRUE, scale. = TRUE)
  J <- min(cfg$n_factors, ncol(pca_fit$x))
  if (J < 1) stop("bootstrap_n_factors must be >= 1.")

  F_emp <- pca_fit$x[, seq_len(J), drop = FALSE]
  factor_weeks <- rank_wide$week[complete]

  rp_factor <- dplyr::filter(rp, week %in% factor_weeks)
  split_by_rank <- split(rp_factor, rp_factor$rank)

  a_vec <- numeric(K_max)
  b_mat <- matrix(0, nrow = K_max, ncol = J)
  residual_pools <- vector("list", K_max)

  for (r in seq_len(K_max)) {
    df_r <- split_by_rank[[as.character(r)]]
    v_all <- dplyr::filter(rp, rank == r) %>% dplyr::pull(dlogw)

    fallback <- function() {
      if (length(v_all) == 0) {
        a_vec[r] <<- 0
        residual_pools[[r]] <<- numeric(0)
      } else {
        a_vec[r] <<- mean(v_all, na.rm = TRUE)
        residual_pools[[r]] <<- v_all - a_vec[r]
      }
    }

    if (is.null(df_r) || nrow(df_r) < (J + 2L)) {
      fallback()
      next
    }

    idx <- match(df_r$week, factor_weeks)
    valid <- !is.na(idx)
    if (sum(valid) < (J + 2L)) {
      fallback()
      next
    }

    y <- df_r$dlogw[valid]
    Xr <- cbind(1, F_emp[idx[valid], , drop = FALSE])
    fit <- stats::lm.fit(x = Xr, y = y)
    coef <- fit$coefficients

    if (any(!is.finite(coef))) {
      fallback()
      next
    }

    a_vec[r] <- coef[1]
    b_mat[r, ] <- coef[-1]
    residual_pools[[r]] <- as.numeric(y - Xr %*% coef)
  }

  neighborhoods <- build_rank_neighborhoods(K_max, cfg$rank_bandwidth, cfg$kernel)
  tail_fits <- NULL
  if (isTRUE(cfg$tailsplice) && !is.null(bucket_assigner)) {
    tail_fits <- build_tail_splice_fits(
      residual_pools,
      bucket_assigner,
      cfg$tail_q,
      cfg$tail_fit_min_n
    )
  }

  sample_residuals <- function() {
    e <- numeric(K_max)
    for (r in seq_len(K_max)) {
      nbrs <- neighborhoods$neighbors[[r]]
      probs <- neighborhoods$probs[[r]]
      r_prime <- if (length(nbrs) == 1L) nbrs else sample(nbrs, size = 1L, prob = probs)
      pool <- residual_pools[[r_prime]]
      if (length(pool) == 0) {
        e[r] <- 0
        next
      }

      if (!is.null(tail_fits) && length(tail_fits$fits) > 0) {
        bucket <- tail_fits$bucket_by_rank[r_prime]
        fit <- tail_fits$fits[[as.character(bucket)]]
        e[r] <- sample_from_pool_tail_splice(pool, fit)
      } else {
        e[r] <- sample_from_pool(pool)
      }
    }
    e
  }

  sample_path <- function(T_sim, mu = 0, sigma = 1) {
    F_boot <- if (isTRUE(cfg$block_bootstrap)) {
      idx <- stationary_bootstrap_indices(nrow(F_emp), cfg$block_length, T_sim)
      F_emp[idx, , drop = FALSE]
    } else {
      F_emp[sample.int(nrow(F_emp), T_sim, replace = TRUE), , drop = FALSE]
    }

    out <- matrix(0, nrow = T_sim, ncol = K_max)
    for (t in seq_len(T_sim)) {
      e <- sample_residuals()
      inc_raw <- a_vec + as.vector(b_mat %*% F_boot[t, ]) + e
      out[t, ] <- mu + sigma * inc_raw
    }
    out
  }

  list(
    method = if (isTRUE(cfg$tailsplice)) "factor_local_tailsplice" else "factor_local",
    K_max = K_max,
    sample_path = sample_path,
    debug_info = list(
      a = a_vec,
      b = b_mat,
      F_emp = F_emp,
      factor_weeks = factor_weeks
    )
  )
}

build_increment_sampler <- function(rank_panel, cfg, bucket_assigner = NULL) {
  method <- cfg$bootstrap_method
  if (method == "factor_local_tailsplice") {
    cfg$tailsplice <- TRUE
    method <- "factor_local"
  }

  if (method == "iid_rank") {
    return(build_sampler_iid_rank(rank_panel, cfg))
  }
  if (method == "local_rank") {
    return(build_sampler_local_rank(rank_panel, cfg))
  }
  if (method == "factor_local") {
    return(build_sampler_factor_local(rank_panel, cfg, bucket_assigner))
  }
  if (method == "week_vector") {
    return(build_sampler_week_vector(rank_panel, cfg))
  }

  stop("Unknown bootstrap method: ", method)
}
