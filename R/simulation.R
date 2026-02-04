make_entrant_sampler <- function(entrant_pool) {
  stopifnot(nrow(entrant_pool) > 0)
  function(n) {
    sample(entrant_pool$share, size = n, replace = TRUE)
  }
}

simulate_rank_paths <- function(
  w0, K_cut, K_max, T = 52, n_paths = 200,
  mu = 0, sigma = 1, entry_frac = 0.10,
  mean_vec, sd_vec,
  horizons = c(4L, 8L),
  K_xi = NULL,
  entrant_sampler = NULL
) {
  stopifnot(length(mean_vec) == K_max, length(sd_vec) == K_max)

  if (is.null(entrant_sampler)) {
    entrant_sampler <- get("entrant_sampler", inherits = TRUE)
  }

  top_n_sorted <- function(w, n) w[order(w, decreasing = TRUE)][seq_len(n)]

  sample_increments_gaussian <- function(mu = 0, sigma = 1) {
    rnorm(K_max, mean = mu + mean_vec, sd = sigma * sd_vec)
  }

  horizons <- as.integer(horizons)
  stopifnot(all(horizons >= 1L), all(horizons <= T))

  if (!is.null(K_xi)) {
    K_xi <- as.integer(K_xi)
    stopifnot(K_xi >= 2L, K_xi <= K_cut)
  }

  snapshots <- vector("list", n_paths)
  growth_collect <- setNames(vector("list", length(horizons)), paste0("h", horizons))
  xi_collect <- vector("list", n_paths)

  for (p in seq_len(n_paths)) {
    if (length(w0) < K_max) {
      w <- c(w0, entrant_sampler(K_max - length(w0)))
      w <- w / sum(w)
    } else {
      w <- w0[1:K_max]
      w <- w / sum(w)
    }

    w_baseK <- top_n_sorted(w, K_cut)
    w_baseK <- w_baseK / sum(w_baseK)

    snaps_p <- vector("list", T)
    xi_p <- vector("list", T)

    for (t in seq_len(T)) {
      dlogw <- sample_increments_gaussian(mu = mu, sigma = sigma)
      w <- w * exp(dlogw)
      w <- pmax(w, 1e-18)
      w <- w / sum(w)

      w_top <- top_n_sorted(w, K_cut)
      n_tail <- K_max - K_cut

      if (n_tail > 0) {
        n_add <- max(1L, floor(n_tail * entry_frac))
        w <- c(w_top, entrant_sampler(n_add), entrant_sampler(n_tail - n_add))
      } else {
        w <- w_top
      }

      w <- w / sum(w)

      wK <- top_n_sorted(w, K_cut)
      wK <- wK / sum(wK)

      snaps_p[[t]] <- tibble::tibble(
        path = p,
        t = t,
        rank = seq_len(K_cut),
        share = wK
      )

      if (!is.null(K_xi)) {
        w_top_now <- top_n_sorted(w, K_cut) %>% pmax(1e-18)
        xi_vals <- log(w_top_now[seq_len(K_xi - 1L)] / w_top_now[seq(2L, K_xi)])
        xi_p[[t]] <- tibble::tibble(
          path = p,
          t = t,
          k = seq_len(K_xi - 1L),
          xi = xi_vals
        )
      }

      if (t %in% horizons) {
        lg <- log(wK / w_baseK)
        lbl <- paste0("h", t)
        growth_collect[[lbl]] <- rbind(
          growth_collect[[lbl]],
          cbind(
            path = p,
            rank = seq_len(K_cut),
            horizon = t,
            log_growth = lg
          )
        )
      }
    }

    snapshots[[p]] <- dplyr::bind_rows(snaps_p)
    if (!is.null(K_xi)) xi_collect[[p]] <- dplyr::bind_rows(xi_p)
  }

  growth_df <- dplyr::bind_rows(lapply(growth_collect, tibble::as_tibble))

  out <- list(
    snapshots = dplyr::bind_rows(snapshots),
    growth = growth_df
  )

  if (!is.null(K_xi)) out$xi <- dplyr::bind_rows(xi_collect)
  out
}

sample_increments_bootstrap <- function(increment_pools, mu = 0, sigma = 1, K_max = NULL) {
  if (is.null(K_max)) K_max <- nrow(increment_pools)
  inc <- purrr::map_dbl(seq_len(K_max), function(r) {
    v <- increment_pools$pool[[r]]
    if (length(v) == 0) return(0)
    sample(v, size = 1, replace = TRUE)
  })
  mu + sigma * inc
}

simulate_rank_paths_bootstrap <- function(
  w0, K_cut, K_max, T = 52, n_paths = 200,
  mu = 0, sigma = 1, entry_frac = 0.10,
  horizons = c(4L, 8L),
  K_xi = NULL,
  increment_pools,
  entrant_sampler = NULL
) {
  if (is.null(entrant_sampler)) {
    entrant_sampler <- get("entrant_sampler", inherits = TRUE)
  }

  top_n_sorted <- function(w, n) w[order(w, decreasing = TRUE)][seq_len(n)]

  horizons <- as.integer(horizons)
  if (!is.null(K_xi)) {
    K_xi <- as.integer(K_xi)
    stopifnot(K_xi >= 2L, K_xi <= K_cut)
  }

  snapshots <- vector("list", n_paths)
  growth_collect <- setNames(vector("list", length(horizons)), paste0("h", horizons))
  xi_collect <- vector("list", n_paths)

  for (p in seq_len(n_paths)) {
    if (length(w0) < K_max) {
      w <- c(w0, entrant_sampler(K_max - length(w0)))
      w <- w / sum(w)
    } else {
      w <- w0[1:K_max]
      w <- w / sum(w)
    }

    w_baseK <- top_n_sorted(w, K_cut)
    w_baseK <- w_baseK / sum(w_baseK)

    snaps_p <- vector("list", T)
    xi_p <- vector("list", T)

    for (t in seq_len(T)) {
      dlogw <- sample_increments_bootstrap(increment_pools, mu = mu, sigma = sigma, K_max = K_max)
      w <- w * exp(dlogw)
      w <- pmax(w, 1e-18)
      w <- w / sum(w)

      w_top <- top_n_sorted(w, K_cut)
      n_tail <- K_max - K_cut

      if (n_tail > 0) {
        n_add <- max(1L, floor(n_tail * entry_frac))
        w <- c(w_top, entrant_sampler(n_add), entrant_sampler(n_tail - n_add))
      } else {
        w <- w_top
      }

      w <- w / sum(w)

      wK <- top_n_sorted(w, K_cut)
      wK <- wK / sum(wK)

      snaps_p[[t]] <- tibble::tibble(
        path = p,
        t = t,
        rank = seq_len(K_cut),
        share = wK
      )

      if (!is.null(K_xi)) {
        w_top_now <- top_n_sorted(w, K_cut) %>% pmax(1e-18)
        xi_vals <- log(w_top_now[seq_len(K_xi - 1L)] / w_top_now[seq(2L, K_xi)])
        xi_p[[t]] <- tibble::tibble(
          path = p,
          t = t,
          k = seq_len(K_xi - 1L),
          xi = xi_vals
        )
      }

      if (t %in% horizons) {
        lg <- log(wK / w_baseK)
        lbl <- paste0("h", t)
        growth_collect[[lbl]] <- rbind(
          growth_collect[[lbl]],
          cbind(
            path = p,
            rank = seq_len(K_cut),
            horizon = t,
            log_growth = lg
          )
        )
      }
    }

    snapshots[[p]] <- dplyr::bind_rows(snaps_p)
    if (!is.null(K_xi)) xi_collect[[p]] <- dplyr::bind_rows(xi_p)
  }

  growth_df <- dplyr::bind_rows(lapply(growth_collect, tibble::as_tibble))

  out <- list(
    snapshots = dplyr::bind_rows(snapshots),
    growth = growth_df
  )

  if (!is.null(K_xi)) out$xi <- dplyr::bind_rows(xi_collect)
  out
}

calibrate_sigma <- function(
  sigma,
  w0_ext,
  mean_vec_s,
  sd_vec_s,
  K_cut = K_cut,
  K_max = K_max,
  sim_T_weeks = sim_T_weeks,
  sim_n_paths = sim_n_paths,
  sim_mu = sim_mu,
  sim_entry_frac = sim_entry_frac,
  horizons_durable = horizons_durable,
  emp_targets = emp_targets,
  emp_cdc = emp_cdc,
  bucket_def = bucket_def
) {
  sim_res <- simulate_rank_paths(
    w0 = w0_ext,
    K_cut = K_cut,
    K_max = K_max,
    T = sim_T_weeks,
    n_paths = sim_n_paths,
    mu = sim_mu,
    sigma = sigma,
    entry_frac = sim_entry_frac,
    mean_vec = mean_vec_s,
    sd_vec = sd_vec_s,
    horizons = horizons_durable,
    K_xi = NULL
  )

  sim_cdc_i <- sim_res$snapshots %>%
    dplyr::group_by(rank) %>%
    dplyr::summarise(w_bar = mean(share, na.rm = TRUE), .groups = "drop")

  rmse_dur <- durable_rmse(emp_targets, sim_res$growth, bucket_def)
  rmse_cdc <- cdc_rmse(emp_cdc, sim_cdc_i)

  tibble::tibble(
    sigma = sigma,
    rmse_dur = rmse_dur,
    rmse_cdc = rmse_cdc,
    score = rmse_dur + 0.25 * rmse_cdc
  )
}

sigma_by_horizon <- function(
  sigma,
  w0_ext,
  mean_vec_s,
  sd_vec_s,
  K_cut = K_cut,
  K_max = K_max,
  sim_T_weeks = sim_T_weeks,
  sim_n_paths = sim_n_paths,
  sim_mu = sim_mu,
  sim_entry_frac = sim_entry_frac,
  horizons_durable = horizons_durable,
  emp_targets = emp_targets,
  bucket_def = bucket_def
) {
  sim_res <- simulate_rank_paths(
    w0 = w0_ext,
    K_cut = K_cut,
    K_max = K_max,
    T = sim_T_weeks,
    n_paths = sim_n_paths,
    mu = sim_mu,
    sigma = sigma,
    entry_frac = sim_entry_frac,
    mean_vec = mean_vec_s,
    sd_vec = sd_vec_s,
    horizons = horizons_durable,
    K_xi = NULL
  )

  sim_targets <- sim_res$growth %>%
    dplyr::mutate(bucket = assign_bucket(rank, bucket_def)) %>%
    dplyr::filter(!is.na(bucket)) %>%
    dplyr::group_by(bucket, horizon) %>%
    dplyr::summarise(med_abs_sim = median(abs(log_growth), na.rm = TRUE), .groups = "drop")

  emp_targets %>%
    dplyr::inner_join(sim_targets, by = c("bucket", "horizon")) %>%
    dplyr::group_by(horizon) %>%
    dplyr::summarise(
      sigma = sigma,
      rmse_h = sqrt(mean((med_abs_emp - med_abs_sim)^2, na.rm = TRUE)),
      .groups = "drop"
    )
}

load_sim_baseline <- function(
  w0_ext,
  mean_vec_s,
  sd_vec_s,
  K_cut,
  K_max,
  sim_K_xi,
  cfg = CFG,
  force = FALSE,
  entrant_sampler = NULL
) {
  if (is.null(entrant_sampler)) {
    entrant_sampler <- get("entrant_sampler", inherits = TRUE)
  }

  deps <- list(
    cache_version = cfg$cache_version,
    seed = cfg$seed,
    K_cut = K_cut,
    K_max = K_max,
    sim_T_weeks = cfg$sim_T_weeks,
    sim_n_paths = cfg$sim_n_paths,
    sim_mu = cfg$sim_mu,
    sim_sigma = cfg$sim_sigma,
    sim_entry_frac = cfg$sim_entry_frac,
    horizons_durable = cfg$horizons_durable,
    sim_K_xi = sim_K_xi,
    w0_hash = digest::digest(w0_ext, algo = "xxhash64"),
    gauss_params_smoothed_fp = read_cache_fingerprint("gauss_params_smoothed", cfg),
    code = deps_code_mtime(c(here::here("R", "simulation.R"), here::here("R", "data_prep.R")))
  )

  cache_or_compute(
    "sim_baseline",
    compute_fn = function() withr::with_seed(cfg$seed, {
      simulate_rank_paths(
        w0 = w0_ext,
        K_cut = K_cut,
        K_max = K_max,
        T = cfg$sim_T_weeks,
        n_paths = cfg$sim_n_paths,
        mu = cfg$sim_mu,
        sigma = cfg$sim_sigma,
        entry_frac = cfg$sim_entry_frac,
        mean_vec = mean_vec_s,
        sd_vec = sd_vec_s,
        horizons = cfg$horizons_durable,
        K_xi = sim_K_xi,
        entrant_sampler = entrant_sampler
      )
    }),
    deps = deps,
    force = force,
    cfg = cfg
  )
}
