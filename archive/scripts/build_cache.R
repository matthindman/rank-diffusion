args <- commandArgs(trailingOnly = TRUE)
force <- "--force" %in% args
what <- NULL

if ("--what" %in% args) {
  idx <- match("--what", args)
  if (!is.na(idx) && length(args) >= idx + 1) {
    what <- strsplit(args[idx + 1], ",", fixed = TRUE)[[1]]
    what <- trimws(what)
  }
}

source(here::here("R", "init.R"))

all_targets <- c(
  "endpoint_weekly",
  "rank_panel",
  "emp_cdc",
  "emp_durable_targets",
  "gauss_params_raw",
  "gauss_params_smoothed",
  "sim_baseline"
)

to_build <- if (is.null(what)) all_targets else intersect(all_targets, what)
if (length(to_build) == 0) {
  stop("No matching targets. Valid: ", paste(all_targets, collapse = ", "))
}

need_endpoint <- any(to_build %in% all_targets)

if (need_endpoint) {
  endpoint_bundle <- load_endpoint_weekly(cfg = CFG, force = force && "endpoint_weekly" %in% to_build)
  endpoint_weekly <- endpoint_bundle$endpoint_weekly
  max_rank_by_week <- endpoint_bundle$max_rank_by_week
  max_rank_seen <- endpoint_bundle$max_rank_seen
  K_cut <- endpoint_bundle$K_cut
  K_max <- endpoint_bundle$K_max
  sim_K_xi <- endpoint_bundle$sim_K_xi
}

if ("rank_panel" %in% to_build) {
  rank_panel <- load_rank_panel(endpoint_weekly, cfg = CFG, force = force)
}

if ("emp_cdc" %in% to_build) {
  emp_cdc <- load_emp_cdc(endpoint_weekly, K_cut, cfg = CFG, force = force)
}

if ("emp_durable_targets" %in% to_build) {
  emp_targets <- load_emp_targets(endpoint_weekly, K_cut, CFG$horizons_durable, CFG$bucket_def, cfg = CFG, force = force)
}

if ("gauss_params_raw" %in% to_build) {
  if (!exists("rank_panel")) {
    rank_panel <- load_rank_panel(endpoint_weekly, cfg = CFG, force = FALSE)
  }
  gauss_params_raw <- load_gauss_params_raw(rank_panel, K_max, cfg = CFG, force = force)
}

if ("gauss_params_smoothed" %in% to_build) {
  if (!exists("gauss_params_raw")) {
    if (!exists("rank_panel")) {
      rank_panel <- load_rank_panel(endpoint_weekly, cfg = CFG, force = FALSE)
    }
    gauss_params_raw <- load_gauss_params_raw(rank_panel, K_max, cfg = CFG, force = FALSE)
  }
  gauss_params_smoothed <- load_gauss_params_smoothed(gauss_params_raw, K_max, CFG$smoothing_h, cfg = CFG, force = force)
}

if ("sim_baseline" %in% to_build) {
  if (!exists("gauss_params_smoothed")) {
    if (!exists("gauss_params_raw")) {
      if (!exists("rank_panel")) {
        rank_panel <- load_rank_panel(endpoint_weekly, cfg = CFG, force = FALSE)
      }
      gauss_params_raw <- load_gauss_params_raw(rank_panel, K_max, cfg = CFG, force = FALSE)
    }
    gauss_params_smoothed <- load_gauss_params_smoothed(gauss_params_raw, K_max, CFG$smoothing_h, cfg = CFG, force = FALSE)
  }

  entrant_pool <- build_entrant_pool(endpoint_weekly, K_cut, CFG$K_tail_buffer, max_rank_seen)
  entrant_sampler <- make_entrant_sampler(entrant_pool)
  w0_ext <- build_w0_ext(endpoint_weekly, K_max, entrant_sampler)

  deps <- list(
    cache_version = CFG$cache_version,
    seed = CFG$seed,
    K_cut = K_cut,
    K_max = K_max,
    sim_T_weeks = CFG$sim_T_weeks,
    sim_n_paths = CFG$sim_n_paths,
    sim_mu = CFG$sim_mu,
    sim_sigma = CFG$sim_sigma,
    sim_entry_frac = CFG$sim_entry_frac,
    horizons_durable = CFG$horizons_durable,
    sim_K_xi = sim_K_xi,
    w0_hash = digest::digest(w0_ext, algo = "xxhash64"),
    gauss_params_smoothed_fp = read_cache_fingerprint("gauss_params_smoothed", CFG),
    code = deps_code_mtime(c(here::here("R", "simulation.R"), here::here("R", "data_prep.R")))
  )

  cache_or_compute(
    "sim_baseline",
    compute_fn = function() withr::with_seed(CFG$seed, {
      simulate_rank_paths(
        w0 = w0_ext,
        K_cut = K_cut,
        K_max = K_max,
        T = CFG$sim_T_weeks,
        n_paths = CFG$sim_n_paths,
        mu = CFG$sim_mu,
        sigma = CFG$sim_sigma,
        entry_frac = CFG$sim_entry_frac,
        mean_vec = gauss_params_smoothed$mean_dlogw_s,
        sd_vec = gauss_params_smoothed$sd_dlogw_s,
        horizons = CFG$horizons_durable,
        K_xi = sim_K_xi,
        entrant_sampler = entrant_sampler
      )
    }),
    deps = deps,
    force = force,
    cfg = CFG
  )
}

message("Done.")
