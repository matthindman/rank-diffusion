`%||%` <- function(x, y) if (is.null(x)) y else x

coerce_numeric <- function(x) {
  if (is.null(x)) return(NA_real_)
  if (is.numeric(x)) return(as.numeric(x))
  if (is.character(x)) {
    return(suppressWarnings(readr::parse_number(x)))
  }
  suppressWarnings(as.numeric(x))
}

coerce_numeric_vec <- function(x) {
  if (is.null(x)) return(numeric(0))
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  if (is.numeric(x)) return(as.numeric(x))
  if (is.character(x)) return(readr::parse_number(x))
  suppressWarnings(as.numeric(x))
}

coerce_character_vec <- function(x) {
  if (is.null(x)) return(character(0))
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  as.character(x)
}

coerce_date <- function(x) {
  if (is.null(x)) return(as.Date(NA))
  if (inherits(x, "Date")) return(x)
  if (inherits(x, c("POSIXct", "POSIXt"))) return(as.Date(x))
  suppressWarnings(as.Date(x))
}

config_from_params <- function(params) {
  cfg <- within(list(), {
    mode <- params$mode %||% "dev"
    output_type <- params$output_type %||% "full"
    use_cache <- if (is.null(params$use_cache)) TRUE else isTRUE(params$use_cache)
    run_heavy <- isTRUE(params$run_heavy)
    cache_version <- params$cache_version %||% "v1"

    fb_weekly_parquet <- params$fb_weekly_parquet %||%
      here::here("data", "raw", "fb_ranked_weekly.parquet")

    seed <- as.integer(coerce_numeric(params$seed %||% 1823L))

    K_cut_target <- as.integer(coerce_numeric(params$K_cut_target %||% 12000L))
    K_tail_buffer <- as.integer(coerce_numeric(params$K_tail_buffer %||% 2000L))
    min_week_ranks_keep <- as.integer(coerce_numeric(params$min_week_ranks_keep %||% 12000L))

    horizons_growth <- as.integer(coerce_numeric_vec(params$horizons_growth %||% c(1L, 7L, 28L)))
    horizons_durable <- as.integer(coerce_numeric_vec(params$horizons_durable %||% c(4L, 8L)))
    horizons_micro <- as.integer(coerce_numeric_vec(params$horizons_micro %||% c(1L, 4L, 12L)))

    bucket_breaks <- coerce_numeric_vec(params$bucket_breaks %||% c(0, 25, 250, Inf))
    bucket_labels <- coerce_character_vec(params$bucket_labels %||% c("large", "midsize", "small"))

    smoothing_h <- as.integer(coerce_numeric(params$smoothing_h %||% 5L))

    sim_T_weeks <- as.integer(coerce_numeric(params$sim_T_weeks %||% 52L))
    sim_n_paths <- as.integer(coerce_numeric(params$sim_n_paths %||% 200L))
    sim_mu <- coerce_numeric(params$sim_mu %||% 0)
    sim_sigma <- coerce_numeric(params$sim_sigma %||% 0.16)
    sim_entry_frac <- coerce_numeric(params$sim_entry_frac %||% 0.1)
    sim_K_xi <- as.integer(coerce_numeric(params$sim_K_xi %||% 500L))

    turnover_K_list <- as.integer(coerce_numeric_vec(params$turnover_K_list %||% c(25L, 250L, 1000L)))
    turnover_horizons <- as.integer(coerce_numeric_vec(params$turnover_horizons %||% c(1L, 4L, 12L)))

    pca_K <- as.integer(coerce_numeric(params$pca_K %||% 500L))

    run_grid_search <- isTRUE(params$run_grid_search)
    run_bootstrap_model <- isTRUE(params$run_bootstrap_model)
    run_jump_model <- isTRUE(params$run_jump_model)
    run_jump_model_zoo <- isTRUE(params$run_jump_model_zoo)

    bootstrap_B <- as.integer(coerce_numeric(params$bootstrap_B %||% 200L))
    bootstrap_k_min <- as.integer(coerce_numeric(params$bootstrap_k_min %||% 10L))
    bootstrap_period_unit <- as.character(params$bootstrap_period_unit %||% "quarter")
    bootstrap_method <- as.character(params$bootstrap_method %||% "iid_rank")
    bootstrap_rank_bandwidth <- params$bootstrap_rank_bandwidth %||% 25L
    bootstrap_kernel <- as.character(params$bootstrap_kernel %||% "uniform")
    bootstrap_block_bootstrap <- isTRUE(params$bootstrap_block_bootstrap)
    bootstrap_block_length <- as.integer(coerce_numeric(params$bootstrap_block_length %||% 4L))
    bootstrap_n_factors <- as.integer(coerce_numeric(params$bootstrap_n_factors %||% 1L))
    bootstrap_K_pca <- as.integer(coerce_numeric(params$bootstrap_K_pca %||% pca_K))
    bootstrap_tailsplice <- isTRUE(params$bootstrap_tailsplice)
    bootstrap_tail_q <- coerce_numeric(params$bootstrap_tail_q %||% 0.99)
    bootstrap_tail_fit_min_n <- as.integer(coerce_numeric(params$bootstrap_tail_fit_min_n %||% 30L))
    bootstrap_diag_paths <- as.integer(coerce_numeric(params$bootstrap_diag_paths %||% 0L))

    if (!is.function(bootstrap_rank_bandwidth)) {
      bootstrap_rank_bandwidth <- coerce_numeric(bootstrap_rank_bandwidth)
    }
    if (is.na(bootstrap_rank_bandwidth) && !is.function(bootstrap_rank_bandwidth)) {
      bootstrap_rank_bandwidth <- 25L
    }
    if (is.na(bootstrap_block_length)) bootstrap_block_length <- 4L
    if (is.na(bootstrap_n_factors) || bootstrap_n_factors < 1L) bootstrap_n_factors <- 1L
    if (is.na(bootstrap_K_pca) || bootstrap_K_pca < 2L) bootstrap_K_pca <- pca_K
    if (is.na(bootstrap_tail_q)) bootstrap_tail_q <- 0.99
    if (is.na(bootstrap_tail_fit_min_n)) bootstrap_tail_fit_min_n <- 30L
    if (is.na(bootstrap_diag_paths) || bootstrap_diag_paths < 0L) bootstrap_diag_paths <- 0L
    bootstrap_tailsplice <- isTRUE(bootstrap_tailsplice) || identical(bootstrap_method, "factor_local_tailsplice")

    jump_model_type <- as.character(params$jump_model_type %||% "jump_factor")
    jump_dist <- as.character(params$jump_dist %||% "laplace")
    jump_sys_loading <- as.character(params$jump_sys_loading %||% "beta")
    jump_df_F <- coerce_numeric(params$jump_df_F %||% 5)
    jump_phi_F <- coerce_numeric(params$jump_phi_F %||% 0)
    jump_df_eps_large <- coerce_numeric(params$jump_df_eps_large %||% 6)
    jump_df_eps_midsize <- coerce_numeric(params$jump_df_eps_midsize %||% 8)
    jump_df_eps_small <- coerce_numeric(params$jump_df_eps_small %||% 10)
    jump_p_sys <- coerce_numeric(params$jump_p_sys %||% 0.02)
    jump_p_idio_large <- coerce_numeric(params$jump_p_idio_large %||% 0.01)
    jump_p_idio_midsize <- coerce_numeric(params$jump_p_idio_midsize %||% 0.005)
    jump_p_idio_small <- coerce_numeric(params$jump_p_idio_small %||% 0.002)
    jump_scale_sys <- coerce_numeric(params$jump_scale_sys %||% 0.35)
    jump_scale_idio <- coerce_numeric(params$jump_scale_idio %||% 0.20)
    jump_scale <- coerce_numeric(params$jump_scale %||% 0.10)
    jump_df <- coerce_numeric(params$jump_df %||% 5)
    jump_mean_reversion <- isTRUE(params$jump_mean_reversion)
    jump_kappa_large <- coerce_numeric(params$jump_kappa_large %||% 0.04)
    jump_kappa_midsize <- coerce_numeric(params$jump_kappa_midsize %||% 0.03)
    jump_kappa_small <- coerce_numeric(params$jump_kappa_small %||% 0.02)

    model_zoo_param_mode <- as.character(params$model_zoo_param_mode %||% "bucket")
    model_zoo_fit_end_week <- coerce_date(params$model_zoo_fit_end_week %||% "2022-07-01")
    model_zoo_train_frac <- coerce_numeric(params$model_zoo_train_frac %||% 0.7)
    model_zoo_df_min <- coerce_numeric(params$model_zoo_df_min %||% 2.2)
    model_zoo_jump_p_max <- coerce_numeric(params$model_zoo_jump_p_max %||% 0.25)
    model_zoo_sim_paths <- as.integer(coerce_numeric(params$model_zoo_sim_paths %||% 120L))
    model_zoo_tail_steps <- as.integer(coerce_numeric(params$model_zoo_tail_steps %||% 200L))
    model_zoo_tail_paths <- as.integer(coerce_numeric(params$model_zoo_tail_paths %||% 30L))
    model_zoo_tail_ranks_per_bucket <- as.integer(coerce_numeric(params$model_zoo_tail_ranks_per_bucket %||% 200L))
    model_zoo_tail_thresholds <- coerce_numeric_vec(params$model_zoo_tail_thresholds %||% c(3, 5))
    model_zoo_tail_probs <- coerce_numeric_vec(params$model_zoo_tail_probs %||% c(0.001, 0.01, 0.99, 0.999))
    model_zoo_weight_cdc <- coerce_numeric(params$model_zoo_weight_cdc %||% 2.0)
    model_zoo_weight_durable <- coerce_numeric(params$model_zoo_weight_durable %||% 1.0)
    model_zoo_weight_xi <- coerce_numeric(params$model_zoo_weight_xi %||% 2.0)
    model_zoo_weight_tail <- coerce_numeric(params$model_zoo_weight_tail %||% 1.5)
    model_zoo_weight_skew <- coerce_numeric(params$model_zoo_weight_skew %||% 1.0)
    model_zoo_weight_micro <- coerce_numeric(params$model_zoo_weight_micro %||% 0.5)
    enable_jump_clustering_models <- isTRUE(params$enable_jump_clustering_models)
    model_zoo_bootstrap_B <- as.integer(coerce_numeric(params$model_zoo_bootstrap_B %||% 100L))
    model_zoo_bootstrap_block_length <- as.integer(coerce_numeric(params$model_zoo_bootstrap_block_length %||% 4L))
    c2_smooth_enabled <- if (is.null(params$c2_smooth_enabled)) TRUE else isTRUE(params$c2_smooth_enabled)
    c2_smooth_x_mode <- as.character(params$c2_smooth_x_mode %||% "log_rank")
    c2_smooth_quadratic <- isTRUE(params$c2_smooth_quadratic)
    c2_smooth_max_iter <- as.integer(coerce_numeric(params$c2_smooth_max_iter %||% 60L))
    c2_smooth_subsample_frac <- coerce_numeric(params$c2_smooth_subsample_frac %||% 0.2)
    c2_smooth_refine_max_iter <- as.integer(coerce_numeric(params$c2_smooth_refine_max_iter %||% 20L))
    c2_smooth_ridge_lambda <- coerce_numeric(params$c2_smooth_ridge_lambda %||% 1e-3)

    bucket_def <- list(
      breaks = bucket_breaks,
      labels = bucket_labels
    )

    cache_dir <- here::here("cache")
    meta_dir  <- here::here("cache", "_meta")
    dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
    dir.create(meta_dir, showWarnings = FALSE, recursive = TRUE)
  })

  if (is.na(cfg$K_cut_target) || is.na(cfg$K_tail_buffer) || is.na(cfg$min_week_ranks_keep) || is.na(cfg$smoothing_h)) {
    stop(
      "K_cut_target, K_tail_buffer, min_week_ranks_keep, and smoothing_h must be numeric. Got: ",
      "K_cut_target=", params$K_cut_target, ", ",
      "K_tail_buffer=", params$K_tail_buffer, ", ",
      "min_week_ranks_keep=", params$min_week_ranks_keep, ", ",
      "smoothing_h=", params$smoothing_h
    )
  }

  if (cfg$K_cut_target < 1) {
    stop("K_cut_target must be >= 1. Got: ", params$K_cut_target)
  }

  if (cfg$K_tail_buffer < 0) {
    stop("K_tail_buffer must be >= 0. Got: ", params$K_tail_buffer)
  }

  if (cfg$min_week_ranks_keep < 0) {
    stop("min_week_ranks_keep must be >= 0. Got: ", params$min_week_ranks_keep)
  }

  if (cfg$smoothing_h < 0) {
    stop("smoothing_h must be >= 0. Got: ", params$smoothing_h)
  }

  if (is.na(cfg$bootstrap_B) || cfg$bootstrap_B < 1) {
    stop("bootstrap_B must be a positive integer. Got: ", params$bootstrap_B)
  }

  if (is.na(cfg$bootstrap_k_min) || cfg$bootstrap_k_min < 1) {
    stop("bootstrap_k_min must be a positive integer. Got: ", params$bootstrap_k_min)
  }

  valid_bootstrap_methods <- c(
    "iid_rank", "local_rank", "factor_local", "week_vector", "factor_local_tailsplice"
  )
  if (!cfg$bootstrap_method %in% valid_bootstrap_methods) {
    stop(
      "bootstrap_method must be one of: ", paste(valid_bootstrap_methods, collapse = ", "),
      ". Got: ", cfg$bootstrap_method
    )
  }

  valid_bootstrap_kernels <- c("uniform", "triangular", "exp")
  if (!cfg$bootstrap_kernel %in% valid_bootstrap_kernels) {
    stop(
      "bootstrap_kernel must be one of: ", paste(valid_bootstrap_kernels, collapse = ", "),
      ". Got: ", cfg$bootstrap_kernel
    )
  }

  if (!is.function(cfg$bootstrap_rank_bandwidth)) {
    if (is.na(cfg$bootstrap_rank_bandwidth) || cfg$bootstrap_rank_bandwidth < 0) {
      stop("bootstrap_rank_bandwidth must be >= 0. Got: ", params$bootstrap_rank_bandwidth)
    }
  }

  if (isTRUE(cfg$bootstrap_block_bootstrap) &&
      (is.na(cfg$bootstrap_block_length) || cfg$bootstrap_block_length < 1)) {
    stop(
      "bootstrap_block_length must be a positive integer when block bootstrap is on. Got: ",
      params$bootstrap_block_length
    )
  }

  if (is.na(cfg$bootstrap_n_factors) || cfg$bootstrap_n_factors < 1) {
    stop("bootstrap_n_factors must be >= 1. Got: ", params$bootstrap_n_factors)
  }

  if (is.na(cfg$bootstrap_K_pca) || cfg$bootstrap_K_pca < 2) {
    stop("bootstrap_K_pca must be >= 2. Got: ", params$bootstrap_K_pca)
  }

  if (is.na(cfg$bootstrap_tail_q) || cfg$bootstrap_tail_q <= 0.5 || cfg$bootstrap_tail_q >= 1) {
    stop("bootstrap_tail_q must be in (0.5, 1). Got: ", params$bootstrap_tail_q)
  }

  if (is.na(cfg$bootstrap_tail_fit_min_n) || cfg$bootstrap_tail_fit_min_n < 10) {
    stop("bootstrap_tail_fit_min_n must be >= 10. Got: ", params$bootstrap_tail_fit_min_n)
  }

  if (is.na(cfg$bootstrap_diag_paths) || cfg$bootstrap_diag_paths < 0) {
    stop("bootstrap_diag_paths must be >= 0. Got: ", params$bootstrap_diag_paths)
  }

  if (is.na(cfg$sim_T_weeks) || cfg$sim_T_weeks < 1) {
    stop("sim_T_weeks must be a positive integer. Got: ", params$sim_T_weeks)
  }

  if (is.na(cfg$sim_n_paths) || cfg$sim_n_paths < 1) {
    stop("sim_n_paths must be a positive integer. Got: ", params$sim_n_paths)
  }

  if (is.na(cfg$sim_mu) || is.na(cfg$sim_sigma) || is.na(cfg$sim_entry_frac)) {
    stop(
      "sim_mu, sim_sigma, and sim_entry_frac must be numeric. Got: ",
      "sim_mu=", params$sim_mu, ", ",
      "sim_sigma=", params$sim_sigma, ", ",
      "sim_entry_frac=", params$sim_entry_frac
    )
  }

  if (cfg$sim_entry_frac < 0 || cfg$sim_entry_frac > 1) {
    stop("sim_entry_frac must be in [0, 1]. Got: ", params$sim_entry_frac)
  }

  if (is.na(cfg$sim_K_xi) || cfg$sim_K_xi < 2) {
    stop("sim_K_xi must be an integer >= 2. Got: ", params$sim_K_xi)
  }

  valid_param_modes <- c("bucket", "smooth")
  if (!cfg$model_zoo_param_mode %in% valid_param_modes) {
    stop(
      "model_zoo_param_mode must be one of: ",
      paste(valid_param_modes, collapse = ", "),
      ". Got: ", cfg$model_zoo_param_mode
    )
  }

  if (is.na(cfg$model_zoo_train_frac) || cfg$model_zoo_train_frac <= 0.5 || cfg$model_zoo_train_frac >= 0.95) {
    stop("model_zoo_train_frac must be in (0.5, 0.95). Got: ", params$model_zoo_train_frac)
  }
  if (is.na(cfg$model_zoo_fit_end_week)) {
    stop("model_zoo_fit_end_week must be a valid date (YYYY-MM-DD). Got: ", params$model_zoo_fit_end_week)
  }

  if (is.na(cfg$model_zoo_df_min) || cfg$model_zoo_df_min <= 2) {
    stop("model_zoo_df_min must be > 2 for finite variance. Got: ", params$model_zoo_df_min)
  }

  if (is.na(cfg$model_zoo_jump_p_max) || cfg$model_zoo_jump_p_max <= 0 || cfg$model_zoo_jump_p_max >= 1) {
    stop("model_zoo_jump_p_max must be in (0,1). Got: ", params$model_zoo_jump_p_max)
  }

  if (is.na(cfg$model_zoo_sim_paths) || cfg$model_zoo_sim_paths < 1) {
    stop("model_zoo_sim_paths must be >= 1. Got: ", params$model_zoo_sim_paths)
  }
  if (is.na(cfg$model_zoo_tail_steps) || cfg$model_zoo_tail_steps < 1) {
    stop("model_zoo_tail_steps must be >= 1. Got: ", params$model_zoo_tail_steps)
  }
  if (is.na(cfg$model_zoo_tail_paths) || cfg$model_zoo_tail_paths < 1) {
    stop("model_zoo_tail_paths must be >= 1. Got: ", params$model_zoo_tail_paths)
  }
  if (is.na(cfg$model_zoo_tail_ranks_per_bucket) || cfg$model_zoo_tail_ranks_per_bucket < 1) {
    stop("model_zoo_tail_ranks_per_bucket must be >= 1. Got: ", params$model_zoo_tail_ranks_per_bucket)
  }

  if (length(cfg$model_zoo_tail_thresholds) == 0 || any(cfg$model_zoo_tail_thresholds <= 0)) {
    stop("model_zoo_tail_thresholds must be positive.")
  }

  if (length(cfg$model_zoo_tail_probs) == 0 || any(cfg$model_zoo_tail_probs <= 0) || any(cfg$model_zoo_tail_probs >= 1)) {
    stop("model_zoo_tail_probs must be in (0,1).")
  }

  if (is.na(cfg$model_zoo_bootstrap_B) || cfg$model_zoo_bootstrap_B < 0) {
    stop("model_zoo_bootstrap_B must be >= 0. Got: ", params$model_zoo_bootstrap_B)
  }
  if (is.na(cfg$model_zoo_bootstrap_block_length) || cfg$model_zoo_bootstrap_block_length < 1) {
    stop("model_zoo_bootstrap_block_length must be >= 1. Got: ", params$model_zoo_bootstrap_block_length)
  }

  valid_c2_x_mode <- c("log_rank", "log_sigma_hat")
  if (!cfg$c2_smooth_x_mode %in% valid_c2_x_mode) {
    stop(
      "c2_smooth_x_mode must be one of: ",
      paste(valid_c2_x_mode, collapse = ", "),
      ". Got: ", cfg$c2_smooth_x_mode
    )
  }
  if (is.na(cfg$c2_smooth_max_iter) || cfg$c2_smooth_max_iter < 1) {
    stop("c2_smooth_max_iter must be >= 1. Got: ", params$c2_smooth_max_iter)
  }
  if (is.na(cfg$c2_smooth_subsample_frac) || cfg$c2_smooth_subsample_frac <= 0 || cfg$c2_smooth_subsample_frac > 1) {
    stop("c2_smooth_subsample_frac must be in (0,1]. Got: ", params$c2_smooth_subsample_frac)
  }
  if (is.na(cfg$c2_smooth_refine_max_iter) || cfg$c2_smooth_refine_max_iter < 0) {
    stop("c2_smooth_refine_max_iter must be >= 0. Got: ", params$c2_smooth_refine_max_iter)
  }
  if (is.na(cfg$c2_smooth_ridge_lambda) || cfg$c2_smooth_ridge_lambda < 0) {
    stop("c2_smooth_ridge_lambda must be >= 0. Got: ", params$c2_smooth_ridge_lambda)
  }

  cfg$horizons_growth <- cfg$horizons_growth[!is.na(cfg$horizons_growth) & cfg$horizons_growth >= 1L]
  cfg$horizons_durable <- cfg$horizons_durable[!is.na(cfg$horizons_durable) & cfg$horizons_durable >= 1L]
  cfg$horizons_micro <- cfg$horizons_micro[!is.na(cfg$horizons_micro) & cfg$horizons_micro >= 1L]
  cfg$turnover_K_list <- cfg$turnover_K_list[!is.na(cfg$turnover_K_list) & cfg$turnover_K_list >= 1L]
  cfg$turnover_horizons <- cfg$turnover_horizons[!is.na(cfg$turnover_horizons) & cfg$turnover_horizons >= 1L]

  if (length(cfg$horizons_growth) == 0 || length(cfg$horizons_durable) == 0 || length(cfg$horizons_micro) == 0) {
    stop("horizons_growth, horizons_durable, and horizons_micro must include at least one integer >= 1.")
  }

  if (length(cfg$turnover_K_list) == 0 || length(cfg$turnover_horizons) == 0) {
    stop("turnover_K_list and turnover_horizons must include at least one integer >= 1.")
  }

  if (length(cfg$bucket_breaks) < 2 || length(cfg$bucket_labels) != (length(cfg$bucket_breaks) - 1L)) {
    stop("bucket_labels must have length = length(bucket_breaks) - 1.")
  }

  max_h_durable <- max(cfg$horizons_durable)
  if (cfg$sim_T_weeks < max_h_durable) {
    message(
      "sim_T_weeks (", cfg$sim_T_weeks, ") < max(horizons_durable) (", max_h_durable,
      "); using sim_T_weeks=", max_h_durable, "."
    )
    cfg$sim_T_weeks <- max_h_durable
  }

  set.seed(cfg$seed)
  cfg
}
