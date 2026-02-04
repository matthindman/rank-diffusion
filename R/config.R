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

    bootstrap_B <- as.integer(coerce_numeric(params$bootstrap_B %||% 200L))
    bootstrap_k_min <- as.integer(coerce_numeric(params$bootstrap_k_min %||% 10L))
    bootstrap_period_unit <- as.character(params$bootstrap_period_unit %||% "quarter")

    bucket_def <- list(
      breaks = bucket_breaks,
      labels = bucket_labels
    )

    cache_dir <- here::here("cache")
    meta_dir  <- here::here("cache", "_meta")
    dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
    dir.create(meta_dir, showWarnings = FALSE, recursive = TRUE)
  })

  if (is.na(cfg$K_cut_target) || is.na(cfg$K_tail_buffer) || is.na(cfg$smoothing_h)) {
    stop(
      "K_cut_target, K_tail_buffer, and smoothing_h must be numeric. Got: ",
      "K_cut_target=", params$K_cut_target, ", ",
      "K_tail_buffer=", params$K_tail_buffer, ", ",
      "smoothing_h=", params$smoothing_h
    )
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
