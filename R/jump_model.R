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
    df <- model$df %||% 6
    z <- rstd_t(K_max, df = df)
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
