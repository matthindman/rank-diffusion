compute_emp_cdc <- function(endpoint_weekly, K_cut) {
  endpoint_weekly %>%
    dplyr::filter(rank <= K_cut) %>%
    dplyr::group_by(rank) %>%
    dplyr::summarise(w_bar = mean(share_topK, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(rank)
}

compute_cdc_by_period <- function(endpoint_weekly, K_cut, period_unit = "quarter") {
  endpoint_weekly %>%
    dplyr::filter(rank <= K_cut) %>%
    dplyr::mutate(period = lubridate::floor_date(week, period_unit)) %>%
    dplyr::group_by(period, rank) %>%
    dplyr::summarise(w_bar = mean(share_topK, na.rm = TRUE), .groups = "drop")
}

compute_gauss_params_raw <- function(rank_panel, K_max) {
  rank_panel %>%
    dplyr::filter(rank <= K_max) %>%
    dplyr::group_by(rank) %>%
    dplyr::summarise(
      mean_dlogw = mean(dlogw, na.rm = TRUE),
      sd_dlogw = sd(dlogw, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::right_join(tibble::tibble(rank = 1:K_max), by = "rank") %>%
    dplyr::arrange(rank) %>%
    dplyr::mutate(
      mean_dlogw = tidyr::replace_na(mean_dlogw, 0),
      sd_dlogw = tidyr::replace_na(sd_dlogw, 0),
      sd_dlogw = dplyr::if_else(sd_dlogw <= 0 | !is.finite(sd_dlogw), 1e-6, sd_dlogw)
    )
}

build_gaussian_params_smoothed <- function(gauss_params_raw, K_max, h) {
  m <- gauss_params_raw$mean_dlogw
  s <- gauss_params_raw$sd_dlogw
  tibble::tibble(
    rank = 1:K_max,
    mean_dlogw_s = moving_average_rank(m, h = h),
    sd_dlogw_s = pmax(moving_average_rank(s, h = h), 1e-6)
  )
}

compute_emp_endpoint_change <- function(df, horizons, bucket_def) {
  df2 <- df %>%
    dplyr::group_by(rank) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(log_w = log(pmax(share_topK, 1e-12)))
  purrr::map_dfr(horizons, function(h) {
    df2 %>%
      dplyr::mutate(d = dplyr::lead(log_w, h) - log_w) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(
        horizon = as.integer(h),
        bucket = assign_bucket(rank, bucket_def)
      ) %>%
      dplyr::filter(!is.na(d), !is.na(bucket)) %>%
      dplyr::group_by(bucket, horizon) %>%
      dplyr::summarise(
        med_abs_emp = median(abs(d), na.rm = TRUE),
        .groups = "drop"
      )
  })
}

bootstrap_sigma_period <- function(
  xi_df_period,
  S_by_k,
  B = 200,
  k_min = 10L,
  K_xi_est = 2000L
) {
  weeks <- unique(xi_df_period$week)
  boot_stats <- numeric(B)

  for (b in seq_len(B)) {
    w_samp <- sample(weeks, size = length(weeks), replace = TRUE)

    rho_b <- xi_df_period %>%
      dplyr::filter(week %in% w_samp, k <= (K_xi_est - 1L)) %>%
      dplyr::group_by(k) %>%
      dplyr::summarise(rho_k = median(xi, na.rm = TRUE), .groups = "drop")

    sig_b <- rho_b %>%
      dplyr::inner_join(S_by_k %>% dplyr::select(k, S_k), by = "k") %>%
      dplyr::mutate(
        sigma2_hat = -2 * S_k * rho_k,
        sigma_hat = dplyr::if_else(sigma2_hat > 0, sqrt(sigma2_hat), NA_real_)
      ) %>%
      dplyr::filter(k >= k_min, k <= (K_xi_est - 1L))

    boot_stats[b] <- median(sig_b$sigma_hat, na.rm = TRUE)
  }

  tibble::tibble(
    sigma_hat_med = median(boot_stats, na.rm = TRUE),
    ci_lo = quantile(boot_stats, 0.025, na.rm = TRUE),
    ci_hi = quantile(boot_stats, 0.975, na.rm = TRUE)
  )
}

topK_overlap <- function(df, K, horizons = c(1L, 4L, 12L)) {
  K <- as.integer(K)

  df_topK <- df %>%
    dplyr::filter(rank <= K) %>%
    dplyr::select(week, endpoint_id) %>%
    dplyr::distinct()

  week_vec <- sort(unique(df_topK$week))

  purrr::map_dfr(as.integer(horizons), function(h) {
    base_weeks <- week_vec[week_vec + lubridate::weeks(h) <= max(week_vec)]

    purrr::map_dfr(base_weeks, function(w) {
      w_tp <- w + lubridate::weeks(h)

      a <- df_topK %>% dplyr::filter(week == w) %>% dplyr::pull(endpoint_id)
      b <- df_topK %>% dplyr::filter(week == w_tp) %>% dplyr::pull(endpoint_id)

      if (length(a) < K || length(b) < K) {
        return(tibble::tibble(K = K, horizon = h, week = w, overlap_frac = NA_real_))
      }

      tibble::tibble(
        K = K,
        horizon = h,
        week = w,
        overlap_frac = length(intersect(a, b)) / K
      )
    })
  })
}

emp_micro_for_k <- function(endpoint_weekly, k, horizons = c(1L, 4L, 12L)) {
  base <- endpoint_weekly %>%
    dplyr::filter(rank == k) %>%
    dplyr::transmute(
      week_t = week,
      endpoint_id,
      share_t = share_topK
    )

  fut <- endpoint_weekly %>%
    dplyr::transmute(
      week_tp = week,
      endpoint_id,
      rank_tp = rank,
      share_tp = share_topK
    )

  purrr::map_dfr(horizons, function(h) {
    base %>%
      dplyr::mutate(week_tp = week_t + lubridate::weeks(h), horizon = as.integer(h)) %>%
      dplyr::left_join(fut, by = c("week_tp", "endpoint_id")) %>%
      dplyr::mutate(
        present_tp = !is.na(share_tp),
        rank_change = dplyr::if_else(present_tp, rank_tp - k, NA_integer_)
      )
  })
}

emp_rankslot_micro_for_k <- function(endpoint_weekly, k, horizons = c(1L, 4L, 12L)) {
  base <- endpoint_weekly %>%
    dplyr::filter(rank == k) %>%
    dplyr::transmute(week_t = week, share_t = share_topK)

  fut <- endpoint_weekly %>%
    dplyr::filter(rank == k) %>%
    dplyr::transmute(week_tp = week, share_tp = share_topK)

  purrr::map_dfr(horizons, function(h) {
    base %>%
      dplyr::mutate(week_tp = week_t + lubridate::weeks(as.integer(h)), horizon = as.integer(h)) %>%
      dplyr::left_join(fut, by = "week_tp") %>%
      dplyr::filter(!is.na(share_tp))
  })
}

sim_micro_for_k <- function(sim_snapshots, k, horizons = c(1L, 4L, 12L)) {
  base <- sim_snapshots %>%
    dplyr::filter(rank == k) %>%
    dplyr::transmute(path, t, share_t = share)

  fut <- sim_snapshots %>%
    dplyr::filter(rank == k) %>%
    dplyr::transmute(path, t_tp = t, share_tp = share)

  purrr::map_dfr(horizons, function(h) {
    base %>%
      dplyr::mutate(t_tp = t + as.integer(h), horizon = as.integer(h)) %>%
      dplyr::left_join(fut, by = c("path", "t_tp")) %>%
      dplyr::filter(!is.na(share_tp))
  })
}

cdc_rmse <- function(emp, sim) {
  df <- emp %>%
    dplyr::inner_join(sim, by = "rank", suffix = c("_emp", "_sim"))
  sqrt(mean((log(df$w_bar_emp) - log(df$w_bar_sim))^2, na.rm = TRUE))
}

durable_rmse <- function(emp_targets, sim_growth, bucket_def) {
  sim_targets <- sim_growth %>%
    dplyr::mutate(bucket = assign_bucket(rank, bucket_def)) %>%
    dplyr::filter(!is.na(bucket)) %>%
    dplyr::group_by(bucket, horizon) %>%
    dplyr::summarise(med_abs_sim = median(abs(log_growth), na.rm = TRUE), .groups = "drop")

  emp_targets %>%
    dplyr::inner_join(sim_targets, by = c("bucket", "horizon")) %>%
    dplyr::summarise(rmse = sqrt(mean((med_abs_emp - med_abs_sim)^2, na.rm = TRUE))) %>%
    dplyr::pull(rmse)
}

xi_rmse <- function(emp_rho_by_k, sim_xi_df) {
  sim_rho <- sim_xi_df %>%
    dplyr::group_by(k) %>%
    dplyr::summarise(rho_k = median(xi, na.rm = TRUE), .groups = "drop")
  df <- emp_rho_by_k %>%
    dplyr::inner_join(sim_rho, by = "k", suffix = c("_emp", "_sim"))
  sqrt(mean((df$rho_k_emp - df$rho_k_sim)^2, na.rm = TRUE))
}

load_emp_cdc <- function(endpoint_weekly, K_cut, cfg = CFG, force = FALSE) {
  endpoint_fp <- read_cache_fingerprint("endpoint_weekly", cfg)

  cache_or_compute(
    "emp_cdc",
    compute_fn = function() compute_emp_cdc(endpoint_weekly, K_cut),
    deps = list(
      cache_version = cfg$cache_version,
      K_cut = K_cut,
      upstream = list(endpoint_weekly_fp = endpoint_fp),
      code = deps_code_mtime(c(here::here("R", "metrics.R")))
    ),
    force = force,
    cfg = cfg
  )
}

load_emp_targets <- function(endpoint_weekly, K_cut, horizons_durable, bucket_def, cfg = CFG, force = FALSE) {
  endpoint_fp <- read_cache_fingerprint("endpoint_weekly", cfg)

  cache_or_compute(
    "emp_durable_targets",
    compute_fn = function() compute_emp_endpoint_change(
      df = endpoint_weekly %>% dplyr::filter(rank <= K_cut),
      horizons = horizons_durable,
      bucket_def = bucket_def
    ),
    deps = list(
      cache_version = cfg$cache_version,
      K_cut = K_cut,
      horizons_durable = horizons_durable,
      bucket_def = bucket_def,
      upstream = list(endpoint_weekly_fp = endpoint_fp),
      code = deps_code_mtime(c(here::here("R", "metrics.R"), here::here("R", "utils.R")))
    ),
    force = force,
    cfg = cfg
  )
}

load_gauss_params_raw <- function(rank_panel, K_max, cfg = CFG, force = FALSE) {
  rank_fp <- read_cache_fingerprint("rank_panel", cfg)

  cache_or_compute(
    "gauss_params_raw",
    compute_fn = function() compute_gauss_params_raw(rank_panel, K_max),
    deps = list(
      cache_version = cfg$cache_version,
      K_max = K_max,
      upstream = list(rank_panel_fp = rank_fp),
      code = deps_code_mtime(c(here::here("R", "metrics.R")))
    ),
    force = force,
    cfg = cfg
  )
}

load_gauss_params_smoothed <- function(gauss_params_raw, K_max, smoothing_h, cfg = CFG, force = FALSE) {
  gauss_fp <- read_cache_fingerprint("gauss_params_raw", cfg)

  cache_or_compute(
    "gauss_params_smoothed",
    compute_fn = function() build_gaussian_params_smoothed(gauss_params_raw, K_max, h = smoothing_h),
    deps = list(
      cache_version = cfg$cache_version,
      K_max = K_max,
      smoothing_h = smoothing_h,
      upstream = list(gauss_params_raw_fp = gauss_fp),
      code = deps_code_mtime(c(here::here("R", "metrics.R"), here::here("R", "utils.R")))
    ),
    force = force,
    cfg = cfg
  )
}
