build_endpoint_weekly <- function(raw_path, K_cut_target, K_tail_buffer, sim_K_xi) {
  raw <- arrow::read_parquet(raw_path)

  endpoint_weekly <- raw %>%
    dplyr::transmute(
      week = as.Date(date),
      endpoint_id,
      metric_value,
      rank = rank
    ) %>%
    dplyr::group_by(week) %>%
    dplyr::arrange(dplyr::desc(metric_value), .by_group = TRUE) %>%
    dplyr::mutate(
      rank = dplyr::row_number(),
      share_global = metric_value / sum(metric_value, na.rm = TRUE)
    ) %>%
    dplyr::ungroup()

  max_rank_by_week <- endpoint_weekly %>%
    dplyr::group_by(week) %>%
    dplyr::summarise(max_rank = max(rank), .groups = "drop")

  max_rank_seen <- max(max_rank_by_week$max_rank, na.rm = TRUE)

  K_cut <- min(as.integer(K_cut_target), as.integer(max_rank_seen))
  K_max <- min(as.integer(K_cut + K_tail_buffer), as.integer(max_rank_seen))

  if (sim_K_xi > K_cut) {
    message("sim_K_xi (", sim_K_xi, ") > K_cut (", K_cut, "); using sim_K_xi=", K_cut, ".")
    sim_K_xi <- K_cut
  }

  endpoint_weekly <- endpoint_weekly %>%
    dplyr::group_by(week) %>%
    dplyr::mutate(
      share_topK = dplyr::if_else(
        rank <= K_cut,
        share_global / sum(share_global[rank <= K_cut], na.rm = TRUE),
        NA_real_
      )
    ) %>%
    dplyr::ungroup()

  list(
    endpoint_weekly = endpoint_weekly,
    max_rank_by_week = max_rank_by_week,
    max_rank_seen = max_rank_seen,
    K_cut = K_cut,
    K_max = K_max,
    sim_K_xi = sim_K_xi
  )
}

build_rank_panel <- function(endpoint_weekly) {
  endpoint_weekly %>%
    dplyr::group_by(rank) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(
      share_global = pmax(share_global, 1e-12),
      log_w = log(share_global),
      dlogw = log_w - dplyr::lag(log_w)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(is.finite(dlogw))
}

build_entrant_pool <- function(endpoint_weekly, K_cut, K_tail_buffer, max_rank_seen) {
  endpoint_weekly %>%
    dplyr::filter(rank > K_cut, rank <= min(K_cut + K_tail_buffer, max_rank_seen)) %>%
    dplyr::transmute(share = share_global) %>%
    dplyr::filter(is.finite(share), share > 0)
}

build_w0_ext <- function(endpoint_weekly, K_max, entrant_sampler) {
  mean_share <- endpoint_weekly %>%
    dplyr::group_by(rank) %>%
    dplyr::summarise(w_bar = mean(share_global, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(rank)

  w0_raw <- mean_share$w_bar[seq_len(min(K_max, nrow(mean_share)))]
  w0_raw <- w0_raw / sum(w0_raw)

  if (length(w0_raw) < K_max) {
    w0_ext <- c(w0_raw, entrant_sampler(K_max - length(w0_raw)))
    w0_ext <- w0_ext / sum(w0_ext)
  } else {
    w0_ext <- w0_raw[1:K_max]
    w0_ext <- w0_ext / sum(w0_ext)
  }

  w0_ext
}

load_endpoint_weekly <- function(cfg = CFG, force = FALSE) {
  raw_path <- resolve_weekly_parquet_path(cfg$fb_weekly_parquet)

  cache_or_compute(
    "endpoint_weekly",
    compute_fn = function() build_endpoint_weekly(
      raw_path = raw_path,
      K_cut_target = cfg$K_cut_target,
      K_tail_buffer = cfg$K_tail_buffer,
      sim_K_xi = cfg$sim_K_xi
    ),
    deps = list(
      cache_version = cfg$cache_version,
      K_cut_target = cfg$K_cut_target,
      K_tail_buffer = cfg$K_tail_buffer,
      sim_K_xi = cfg$sim_K_xi,
      raw_mtime = deps_file_mtime(raw_path),
      code = deps_code_mtime(c(here::here("R", "data_prep.R"), here::here("R", "utils.R")))
    ),
    force = force,
    cfg = cfg
  )
}

load_rank_panel <- function(endpoint_weekly, cfg = CFG, force = FALSE) {
  endpoint_fp <- read_cache_fingerprint("endpoint_weekly", cfg)

  cache_or_compute(
    "rank_panel",
    compute_fn = function() build_rank_panel(endpoint_weekly),
    deps = list(
      cache_version = cfg$cache_version,
      upstream = list(endpoint_weekly_fp = endpoint_fp),
      code = deps_code_mtime(c(here::here("R", "data_prep.R")))
    ),
    force = force,
    cfg = cfg
  )
}
