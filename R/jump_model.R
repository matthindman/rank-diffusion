clamp01 <- function(x) pmin(pmax(x, 0), 1)

rstd_t <- function(n, df) {
  if (df <= 2) stop("df must be > 2 for unit-variance t")
  rt(n, df) * sqrt((df - 2) / df)
}

rstd_t_vec <- function(df_vec, df_default = 6) {
  df_vec <- as.numeric(df_vec)
  df_vec[!is.finite(df_vec)] <- df_default
  df_vec <- pmax(df_vec, 2.01)
  out <- numeric(length(df_vec))
  for (d in unique(df_vec)) {
    idx <- which(df_vec == d)
    out[idx] <- rstd_t(length(idx), d)
  }
  out
}

rlaplace <- function(n, scale) {
  scale <- as.numeric(scale)
  if (!is.finite(scale) || scale <= 0) stop("scale must be > 0")
  sign <- sample(c(-1, 1), size = n, replace = TRUE)
  sign * rexp(n, rate = 1 / scale)
}

r_double_exp_asym <- function(n, pi_pos, eta_pos, eta_neg) {
  if (n <= 0) return(numeric(0))
  if (length(pi_pos) == 1L) pi_pos <- rep(pi_pos, n)
  if (length(eta_pos) == 1L) eta_pos <- rep(eta_pos, n)
  if (length(eta_neg) == 1L) eta_neg <- rep(eta_neg, n)
  u <- runif(n)
  is_pos <- u < pi_pos
  out <- numeric(n)
  if (any(is_pos)) out[is_pos] <- rexp(sum(is_pos), rate = eta_pos[is_pos])
  if (any(!is_pos)) out[!is_pos] <- -rexp(sum(!is_pos), rate = eta_neg[!is_pos])
  out
}

resolve_bucket_param <- function(param, K_max, bucket_def = NULL, default = NULL) {
  if (is.null(param)) return(rep(default, K_max))
  if (length(param) == 1L) return(rep(param, K_max))
  if (length(param) == K_max) return(param)
  if (is.null(bucket_def) || is.null(bucket_def$breaks)) return(rep(default, K_max))

  if (!is.null(names(param))) {
    buckets <- assign_bucket(seq_len(K_max), bucket_def)
    out <- rep(default, K_max)
    for (b in levels(buckets)) {
      if (b %in% names(param)) out[buckets == b] <- param[[b]]
    }
    return(out)
  }
  if (!is.null(bucket_def$labels) && length(param) == length(bucket_def$labels)) {
    buckets <- assign_bucket(seq_len(K_max), bucket_def)
    out <- rep(default, K_max)
    for (i in seq_along(bucket_def$labels)) {
      out[buckets == bucket_def$labels[i]] <- param[i]
    }
    return(out)
  }
  rep(default, K_max)
}

resolve_beta_k <- function(model, cache, K_max) {
  if (!is.null(model$beta_k)) {
    if (length(model$beta_k) >= K_max) return(model$beta_k[seq_len(K_max)])
    return(c(model$beta_k, rep(0, K_max - length(model$beta_k))))
  }
  if (!is.null(cache$beta_k) && length(cache$beta_k) == K_max) return(cache$beta_k)
  rep(0, K_max)
}

resolve_sys_loading <- function(model, beta_k, sd_vec) {
  if (is.character(model$sys_loading)) {
    if (model$sys_loading == "beta") return(beta_k)
    if (model$sys_loading == "sigma") {
      s <- sd_vec / mean(sd_vec)
      return(s)
    }
  }
  if (is.numeric(model$sys_loading) && length(model$sys_loading) == length(sd_vec)) {
    return(model$sys_loading)
  }
  sd_vec / mean(sd_vec)
}

draw_jump_sizes <- function(n, spec) {
  if (n <= 0) return(numeric(0))
  dist <- spec$dist %||% "laplace"
  if (dist == "laplace") {
    scale <- spec$scale %||% 0.1
    return(rlaplace(n, scale = scale))
  }
  if (dist == "t") {
    df <- spec$df %||% 5
    scale <- spec$scale %||% 0.1
    return(rstd_t(n, df = df) * scale)
  }
  stop("Unknown jump dist: ", dist)
}

draw_increments <- function(t, w_t, moment_curves, model, cache) {
  type <- model$type %||% "gaussian"
  mean_vec <- moment_curves$mean_vec
  sd_vec <- moment_curves$sd_vec
  K_max <- length(mean_vec)

  mu <- model$mu %||% 0
  sigma <- model$sigma %||% 1

  if (type == "gaussian") {
    return(rnorm(K_max, mean = mu + mean_vec, sd = sigma * sd_vec))
  }

  if (type %in% c("t", "student_t")) {
    df_vec <- resolve_bucket_param(
      param = model$df %||% 6,
      K_max = K_max,
      bucket_def = moment_curves$bucket_def,
      default = model$df %||% 6
    )
    scale_vec <- resolve_bucket_param(
      param = model$scale %||% 1,
      K_max = K_max,
      bucket_def = moment_curves$bucket_def,
      default = 1
    )
    z <- rstd_t_vec(df_vec) * scale_vec
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "skew_t") {
    if (!requireNamespace("sn", quietly = TRUE)) {
      stop("sn package required for skew_t")
    }
    buckets <- assign_bucket(seq_len(K_max), moment_curves$bucket_def)
    z <- numeric(K_max)
    params <- model$skew_params
    for (b in levels(buckets)) {
      idx <- which(buckets == b)
      if (length(idx) == 0) next
      dp <- NULL
      if (is.data.frame(params)) {
        row <- params[params$bucket == b, ]
        if (nrow(row) > 0) dp <- row$dp[[1]]
      } else if (is.list(params)) {
        dp <- params[[b]]
      }
      if (is.null(dp)) dp <- list(xi = 0, omega = 1, alpha = 0, nu = 5)
      z[idx] <- sn::rst(length(idx), dp = dp)
    }
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "ghyp") {
    if (!requireNamespace("ghyp", quietly = TRUE)) {
      stop("ghyp package required for ghyp")
    }
    buckets <- assign_bucket(seq_len(K_max), moment_curves$bucket_def)
    z <- numeric(K_max)
    params <- model$ghyp_params
    for (b in levels(buckets)) {
      idx <- which(buckets == b)
      if (length(idx) == 0) next
      obj <- NULL
      if (is.data.frame(params)) {
        row <- params[params$bucket == b, ]
        if (nrow(row) > 0) obj <- row$obj[[1]]
      } else if (is.list(params)) {
        obj <- params[[b]]
      }
      if (is.null(obj)) {
        z[idx] <- rnorm(length(idx))
      } else {
        z[idx] <- ghyp::rghyp(length(idx), object = obj)
      }
    }
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "mixture_gaussian") {
    p_vec <- resolve_bucket_param(model$p %||% 0.05, K_max, moment_curves$bucket_def, default = 0.05)
    mu1_vec <- resolve_bucket_param(model$mu1 %||% 0, K_max, moment_curves$bucket_def, default = 0)
    sd1_vec <- resolve_bucket_param(model$sd1 %||% 1, K_max, moment_curves$bucket_def, default = 1)
    mu2_vec <- resolve_bucket_param(model$mu2 %||% 0, K_max, moment_curves$bucket_def, default = 0)
    sd2_vec <- resolve_bucket_param(model$sd2 %||% 2, K_max, moment_curves$bucket_def, default = 2)
    p_vec <- clamp01(p_vec)
    I <- rbinom(K_max, 1, p_vec)
    z <- rnorm(K_max, mean = mu1_vec, sd = sd1_vec)
    if (any(I == 1)) {
      idx <- which(I == 1)
      z[idx] <- rnorm(length(idx), mean = mu2_vec[idx], sd = sd2_vec[idx])
    }
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "jump_merton") {
    p_vec <- resolve_bucket_param(model$p %||% 0.02, K_max, moment_curves$bucket_def, default = 0.02)
    mu_j <- resolve_bucket_param(model$mu_j %||% 0, K_max, moment_curves$bucket_def, default = 0)
    sd_j <- resolve_bucket_param(model$sd_j %||% 1, K_max, moment_curves$bucket_def, default = 1)
    p_vec <- clamp01(p_vec)
    I <- rbinom(K_max, 1, p_vec)
    z <- rnorm(K_max)
    if (any(I == 1)) {
      idx <- which(I == 1)
      z[idx] <- rnorm(length(idx), mean = mu_j[idx], sd = sd_j[idx])
    }
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "jump_kou") {
    p_vec <- resolve_bucket_param(model$p %||% 0.02, K_max, moment_curves$bucket_def, default = 0.02)
    pi_pos <- resolve_bucket_param(model$pi_pos %||% 0.5, K_max, moment_curves$bucket_def, default = 0.5)
    eta_pos <- resolve_bucket_param(model$eta_pos %||% 1, K_max, moment_curves$bucket_def, default = 1)
    eta_neg <- resolve_bucket_param(model$eta_neg %||% 1, K_max, moment_curves$bucket_def, default = 1)
    p_vec <- clamp01(p_vec)
    pi_pos <- clamp01(pi_pos)
    I <- rbinom(K_max, 1, p_vec)
    z <- rnorm(K_max)
    if (any(I == 1)) {
      idx <- which(I == 1)
      z[idx] <- r_double_exp_asym(length(idx), pi_pos[idx], eta_pos[idx], eta_neg[idx])
    }
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "factor_t") {
    beta_k <- resolve_beta_k(model, cache, K_max)
    phi_F <- model$factor_phi %||% model$phi_F %||% 0
    state <- cache$state
    if (is.null(state$F_prev)) state$F_prev <- 0
    if (abs(phi_F) >= 1) phi_F <- sign(phi_F) * 0.99
    df_F <- model$factor_df %||% 6
    scale_F <- model$factor_scale %||% 1
    eta <- rstd_t(1, df = df_F) * scale_F
    F_t <- phi_F * state$F_prev + sqrt(1 - phi_F^2) * eta
    state$F_prev <- F_t

    df_eps <- resolve_bucket_param(model$idio_df %||% 6, K_max, moment_curves$bucket_def, default = 6)
    sc_eps <- resolve_bucket_param(model$idio_scale %||% 1, K_max, moment_curves$bucket_def, default = 1)
    eps <- rstd_t_vec(df_eps) * sc_eps
    z <- beta_k * F_t + eps
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "factor_kou") {
    beta_k <- resolve_beta_k(model, cache, K_max)
    state <- cache$state
    if (is.null(state$F_prev)) state$F_prev <- 0
    phi_F <- model$factor_phi %||% model$phi_F %||% 0
    if (abs(phi_F) >= 1) phi_F <- sign(phi_F) * 0.99

    fp <- model$factor_params %||% list(p = 0.02, pi_pos = 0.5, eta_pos = 1, eta_neg = 1)
    pF <- fp$p %||% 0.02
    piF <- fp$pi_pos %||% 0.5
    etaFpos <- fp$eta_pos %||% 1
    etaFneg <- fp$eta_neg %||% 1
    piF <- clamp01(piF)
    pF <- clamp01(pF)
    F_shock <- if (runif(1) < pF) r_double_exp_asym(1, piF, etaFpos, etaFneg) else rnorm(1)
    F_t <- phi_F * state$F_prev + sqrt(1 - phi_F^2) * F_shock
    state$F_prev <- F_t

    idio <- model$idio_params %||% list()
    idio_params <- idio$params %||% idio
    if (is.data.frame(idio_params)) {
      idio_params <- list(
        p = setNames(idio_params$p, idio_params$bucket),
        pi_pos = setNames(idio_params$pi_pos, idio_params$bucket),
        eta_pos = setNames(idio_params$eta_pos, idio_params$bucket),
        eta_neg = setNames(idio_params$eta_neg, idio_params$bucket)
      )
    }
    p_idio <- resolve_bucket_param(idio_params$p %||% 0.01, K_max, moment_curves$bucket_def, default = 0.01)
    pi_idio <- resolve_bucket_param(idio_params$pi_pos %||% 0.5, K_max, moment_curves$bucket_def, default = 0.5)
    eta_pos <- resolve_bucket_param(idio_params$eta_pos %||% 1, K_max, moment_curves$bucket_def, default = 1)
    eta_neg <- resolve_bucket_param(idio_params$eta_neg %||% 1, K_max, moment_curves$bucket_def, default = 1)

    p_idio <- clamp01(p_idio)
    pi_idio <- clamp01(pi_idio)
    I <- rbinom(K_max, 1, p_idio)
    eps <- rnorm(K_max)
    if (any(I == 1)) {
      idx <- which(I == 1)
      eps[idx] <- r_double_exp_asym(length(idx), pi_idio[idx], eta_pos[idx], eta_neg[idx])
    }
    z <- beta_k * F_t + eps
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "factor_kou_tv") {
    beta_k <- resolve_beta_k(model, cache, K_max)
    state <- cache$state
    if (is.null(state$F_prev)) state$F_prev <- 0
    if (is.null(state$F_jump_prev)) state$F_jump_prev <- 0
    phi_F <- model$factor_phi %||% model$phi_F %||% 0
    if (abs(phi_F) >= 1) phi_F <- sign(phi_F) * 0.99

    fp <- model$factor_params %||% list(p = 0.02, pi_pos = 0.5, eta_pos = 1, eta_neg = 1)
    tv <- model$factor_tv %||% list(p0 = 0.02, alpha = 0, threshold = 3)
    p0 <- tv$p0 %||% 0.02
    alpha <- tv$alpha %||% 0
    logit_p0 <- qlogis(pmin(pmax(p0, 1e-6), 1 - 1e-6))
    pF <- plogis(logit_p0 + alpha * state$F_jump_prev)
    pF <- clamp01(pF)

    piF <- clamp01(fp$pi_pos %||% 0.5)
    etaFpos <- fp$eta_pos %||% 1
    etaFneg <- fp$eta_neg %||% 1

    I_jump <- rbinom(1, 1, pF)
    state$F_jump_prev <- I_jump
    F_shock <- if (I_jump == 1) r_double_exp_asym(1, piF, etaFpos, etaFneg) else rnorm(1)
    F_t <- phi_F * state$F_prev + sqrt(1 - phi_F^2) * F_shock
    state$F_prev <- F_t

    idio <- model$idio_params %||% list()
    idio_params <- idio$params %||% idio
    if (is.data.frame(idio_params)) {
      idio_params <- list(
        p = setNames(idio_params$p, idio_params$bucket),
        pi_pos = setNames(idio_params$pi_pos, idio_params$bucket),
        eta_pos = setNames(idio_params$eta_pos, idio_params$bucket),
        eta_neg = setNames(idio_params$eta_neg, idio_params$bucket)
      )
    }
    p_idio <- resolve_bucket_param(idio_params$p %||% 0.01, K_max, moment_curves$bucket_def, default = 0.01)
    pi_idio <- resolve_bucket_param(idio_params$pi_pos %||% 0.5, K_max, moment_curves$bucket_def, default = 0.5)
    eta_pos <- resolve_bucket_param(idio_params$eta_pos %||% 1, K_max, moment_curves$bucket_def, default = 1)
    eta_neg <- resolve_bucket_param(idio_params$eta_neg %||% 1, K_max, moment_curves$bucket_def, default = 1)
    p_idio <- clamp01(p_idio)
    pi_idio <- clamp01(pi_idio)
    I <- rbinom(K_max, 1, p_idio)
    eps <- rnorm(K_max)
    if (any(I == 1)) {
      idx <- which(I == 1)
      eps[idx] <- r_double_exp_asym(length(idx), pi_idio[idx], eta_pos[idx], eta_neg[idx])
    }
    z <- beta_k * F_t + eps
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "factor_regime") {
    beta_k <- resolve_beta_k(model, cache, K_max)
    state <- cache$state
    if (is.null(state$regime_state)) state$regime_state <- 1L
    fp <- model$factor_params %||% list(p11 = 0.9, p22 = 0.9, sigma1 = 1, sigma2 = 2)
    p11 <- fp$p11 %||% 0.9
    p22 <- fp$p22 %||% 0.9
    if (state$regime_state == 1L) {
      if (runif(1) > p11) state$regime_state <- 2L
    } else {
      if (runif(1) > p22) state$regime_state <- 1L
    }
    sigma_F <- if (state$regime_state == 1L) fp$sigma1 else fp$sigma2
    F_t <- rnorm(1, mean = 0, sd = sigma_F)

    idio <- model$idio_params %||% list()
    idio_params <- idio$params %||% idio
    if (is.data.frame(idio_params)) {
      idio_params <- list(
        df = setNames(idio_params$df, idio_params$bucket),
        scale = setNames(idio_params$scale, idio_params$bucket)
      )
    }
    df_eps <- resolve_bucket_param(idio_params$df %||% 6, K_max, moment_curves$bucket_def, default = 6)
    sc_eps <- resolve_bucket_param(idio_params$scale %||% 1, K_max, moment_curves$bucket_def, default = 1)
    eps <- rstd_t_vec(df_eps) * sc_eps
    z <- beta_k * F_t + eps
    return(mu + mean_vec + sigma * sd_vec * z)
  }

  if (type == "jump_factor") {
    df_F <- model$df_F %||% 6
    phi_F <- model$phi_F %||% 0

    state <- cache$state
    if (is.null(state$F_prev)) state$F_prev <- 0

    if (abs(phi_F) >= 1) phi_F <- sign(phi_F) * 0.99
    eta <- rstd_t(1, df = df_F)
    F_t <- phi_F * state$F_prev + sqrt(1 - phi_F^2) * eta
    state$F_prev <- F_t

    beta_k <- resolve_beta_k(model, cache, K_max)

    df_eps_vec <- resolve_bucket_param(
      param = model$df_eps %||% 8,
      K_max = K_max,
      bucket_def = moment_curves$bucket_def,
      default = model$df_eps %||% 8
    )
    df_eps_vec <- pmax(df_eps_vec, 2.01)
    eps <- rstd_t_vec(df_eps_vec)

    p_sys <- clamp01(model$p_sys %||% 0)
    I_sys <- rbinom(1, 1, p_sys)
    J_sys <- rep(0, K_max)
    if (I_sys == 1) {
      S_sys <- draw_jump_sizes(1, spec = model$jump_sys %||% list(
        dist = model$jump_dist %||% "laplace",
        scale = model$jump_scale_sys %||% model$jump_scale %||% 0.1,
        df = model$jump_df_sys %||% model$jump_df
      ))
      a_k <- resolve_sys_loading(model, beta_k, sd_vec)
      J_sys <- a_k * S_sys
    }

    p_idio_vec <- resolve_bucket_param(
      param = model$p_idio %||% 0,
      K_max = K_max,
      bucket_def = moment_curves$bucket_def,
      default = 0
    )
    p_idio_vec <- clamp01(p_idio_vec)
    I_idio <- rbinom(K_max, 1, p_idio_vec)
    J_idio <- rep(0, K_max)
    if (any(I_idio == 1)) {
      S_idio <- draw_jump_sizes(sum(I_idio == 1), spec = model$jump_idio %||% list(
        dist = model$jump_dist %||% "laplace",
        scale = model$jump_scale_idio %||% model$jump_scale %||% 0.05,
        df = model$jump_df_idio %||% model$jump_df
      ))
      J_idio[I_idio == 1] <- S_idio
    }

    return(mu + mean_vec + beta_k * F_t + sigma * sd_vec * eps + J_sys + J_idio)
  }

  stop("Unknown model type: ", type)
}

sample_increments_for_tail <- function(
  model, moment_curves, cache,
  K_cut,
  n_steps = 200L,
  n_paths = 30L,
  ranks_per_bucket = 200L
) {
  K_max <- length(moment_curves$mean_vec)
  ranks <- seq_len(min(K_cut, K_max))

  bucket_def <- moment_curves$bucket_def %||%
    (if (exists("bucket_def")) bucket_def else NULL)
  if (is.null(bucket_def)) stop("bucket_def is required for tail sampling")

  buckets <- assign_bucket(ranks, bucket_def)
  keep <- !is.na(buckets)
  ranks <- ranks[keep]
  buckets <- buckets[keep]

  if (is.null(cache)) cache <- list()
  if (is.null(cache$state)) cache$state <- new.env(parent = emptyenv())

  use_mean_reversion <- isTRUE(model$mean_reversion)
  if (use_mean_reversion) {
    kappa_vec <- moment_curves$kappa_vec
    if (is.null(kappa_vec)) {
      kappa_vec <- resolve_bucket_param(
        param = model$kappa %||% 0,
        K_max = K_max,
        bucket_def = bucket_def,
        default = 0
      )
    }
  }

  res <- vector("list", n_paths * n_steps)
  idx <- 1

  for (p in seq_len(n_paths)) {
    cache$state$F_prev <- 0
    cache$state$regime_state <- 1L
    cache$state$F_jump_prev <- 0
    if (use_mean_reversion) y <- rep(0, K_max)

    for (t in seq_len(n_steps)) {
      dlogw_innov <- draw_increments(
        t = t,
        w_t = rep(1 / K_max, K_max),
        moment_curves = moment_curves,
        model = model,
        cache = cache
      )

      if (use_mean_reversion) {
        dlogw <- dlogw_innov - kappa_vec * y
        y <- (1 - kappa_vec) * y + dlogw_innov
      } else {
        dlogw <- dlogw_innov
      }

      dlogw <- dlogw[ranks]

      df <- tibble(rank = ranks, dlogw = dlogw, bucket = buckets)
  if (!is.null(ranks_per_bucket) && is.finite(ranks_per_bucket)) {
    n_take <- as.integer(ranks_per_bucket)
    if (!is.na(n_take) && n_take > 0L) {
      df <- df %>%
        group_by(bucket) %>%
        group_modify(~{
          n_avail <- nrow(.x)
          if (n_avail == 0L) return(.x)
          n_keep <- min(n_take, n_avail)
          .x[sample.int(n_avail, n_keep), , drop = FALSE]
        }) %>%
        ungroup()
    }
  }
      res[[idx]] <- df
      idx <- idx + 1
    }
  }

  bind_rows(res)
}
