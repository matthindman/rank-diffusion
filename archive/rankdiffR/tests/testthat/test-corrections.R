test_that("excess_kurtosis uses the bias-corrected Fisher definition", {
  x <- c(0, 1, 2, 4, 8, 16)
  n <- length(x)
  centered <- x - mean(x)
  g2 <- mean(centered^4) / mean(centered^2)^2 - 3
  expected <- ((n - 1) / ((n - 2) * (n - 3))) * ((n + 1) * g2 + 6)

  expect_equal(rankdiffR:::excess_kurtosis(x), expected)
})

test_that("numeric_gradient matches NumPy on a nonuniform grid", {
  x <- c(0.0, 0.7, 1.8, 3.0)
  y <- c(0.0, 1.0, 1.5, 3.0)
  expected <- c(1.42857143, 1.04978355, 0.83498024, 1.25)

  expect_equal(rankdiffR:::.numeric_gradient(y, x), expected, tolerance = 1e-8)
})

test_that("select_tracked_ids does not require an existing RNG state", {
  if (exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)) {
    rm(".Random.seed", envir = .GlobalEnv)
  }

  ids <- rankdiffR:::.select_tracked_ids(data.frame(), letters[1:5], 2L, 42L)

  expect_length(ids, 2L)
  expect_false(exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE))
})

test_that("save_fit_result and load_fit_result preserve config and param classes", {
  cfg <- create_config(
    data_path = "test.parquet",
    output_dir = tempdir(),
    n_jobs = 4L
  )
  threshold <- rankdiffR:::new_threshold_model(c(1, 2), c(1, 2))

  initial <- structure(
    list(
      sigma_obs = 0.1,
      sigma_het = 0.2,
      alpha_arch = 0.1,
      t_df_global = 8,
      jump_prob = 0.01,
      jump_scale = 5,
      alpha_kappa = 0.5,
      kappa_base_raw = 0.02,
      z_knots = c(-3, -2),
      sigma_eta_anchor = c(0.1, 0.2),
      phi_anchor = c(0.7, 0.8),
      sigma_nu_anchor = c(0.2, 0.3),
      t_df_anchor = c(8, 10),
      threshold = threshold,
      top_k = 10L,
      metadata = list()
    ),
    class = "rankdiff_initial"
  )

  params <- structure(
    list(
      sigma_obs = 0.1,
      sigma_het = 0.2,
      alpha_arch = 0.1,
      t_df_global = 8,
      jump_prob = 0.01,
      jump_scale = 5,
      alpha_kappa = 0.5,
      kappa_base_raw = 0.02,
      kappa_stab_factor = 1,
      z_knots = c(-3, -2),
      sigma_eta_curve = c(0.1, 0.2),
      phi_curve = c(0.7, 0.8),
      sigma_nu_curve = c(0.2, 0.3),
      kappa_curve = c(0.01, 0.02),
      t_df_curve = c(8, 10),
      threshold = threshold,
      top_k = 10L,
      n_full = 100L,
      w0_sorted = c(3, 2, 1),
      burnin_periods = 50L,
      metadata = list(),
      exit_p_base = 0.01,
      exit_alpha = 0.3,
      exit_transient_rate = 0.07,
      entry_burst_frac = 0.008,
      t_df_curve_precal = c(8, 10)
    ),
    class = "rankdiff_params"
  )

  data <- structure(
    list(
      panel = data.frame(),
      platform = "test",
      cadence = "weekly",
      dates = as.Date(character()),
      n_periods = 2L,
      n_entities = 2L,
      mean_n = 2,
      max_n = 2L,
      balanced_ids = character(0),
      tracked_ids = character(0),
      tracked_entity_ids = character(0),
      threshold = threshold,
      empirical = list(
        counts_by_period = c(2L, 2L),
        top_k = 10L,
        vr_emp = list("2" = 1.0),
        acf_emp = list("1" = 0.1),
        racf_emp = list("1" = 0.1),
        pers_emp = list("1" = 9L),
        xr2_emp = list("1" = 0.8),
        zipf_slope = -1.1,
        emp_kurt = 0.5,
        emp_mean_var = 1.0,
        emp_median_var = 1.0,
        xsec_var_emp = 1.0,
        window_turnover_n = NULL,
        window_turnover_rate = NA_real_,
        window_turnover_count = NA_real_
      )
    ),
    class = "rankdiff_bundle"
  )

  result <- structure(
    list(
      config = cfg,
      data = data,
      initial = initial,
      params = params,
      diagnostics = list(
        mc_stats = list(),
        tests = list(),
        n_pass = 0L,
        n_total = 0L
      )
    ),
    class = "rankdiff_result"
  )

  path <- file.path(tempdir(), paste0("rankdiffR-test-", Sys.getpid()))
  unlink(path, recursive = TRUE, force = TRUE)
  on.exit(unlink(path, recursive = TRUE, force = TRUE), add = TRUE)

  save_fit_result(result, path)
  loaded <- load_fit_result(path)

  expect_equal(loaded$config$n_jobs, 4L)
  expect_s3_class(loaded$params, "rankdiff_params")
  expect_equal(loaded$data$tracked_entity_ids, character(0))
})

test_that("tracked ID selection respects max_dense_entities cap", {
  cfg <- create_config(
    data_path = "test.parquet",
    track_entity_count = 5L,
    max_dense_entities = 2L
  )

  track_count <- min(cfg$track_entity_count, cfg$max_dense_entities)
  ids <- rankdiffR:::.select_tracked_ids(data.frame(), letters[1:5], track_count, 42L)

  expect_length(ids, 2L)
})

test_that("split_entry_counts respects zero and full burst rates", {
  expect_equal(rankdiffR:::.split_entry_counts(7L, 0.0), c(burst = 0L, normal = 7L))
  expect_equal(rankdiffR:::.split_entry_counts(7L, 1.0), c(burst = 7L, normal = 0L))
})

test_that("simulate_one zeroes ranks for replaced tracked entities", {
  cfg <- create_config(
    data_path = "input.parquet",
    random_seed = 123L,
    track_entity_count = 3L,
    simulate_periods = 3L,
    use_obs_noise = FALSE,
    exit_enabled = TRUE,
    exit_alpha = 0.0
  )
  threshold <- rankdiffR:::new_threshold_model(c(0, 0, 0), c(0, 0, 0))
  params <- structure(
    list(
      sigma_obs = 0.0,
      sigma_het = 0.0,
      alpha_arch = 0.0,
      t_df_global = 10.0,
      jump_prob = 0.0,
      jump_scale = 1.0,
      alpha_kappa = 0.0,
      kappa_base_raw = 0.0,
      kappa_stab_factor = 1.0,
      z_knots = c(-10.0, 0.0),
      sigma_eta_curve = c(0.0, 0.0),
      phi_curve = c(0.0, 0.0),
      sigma_nu_curve = c(0.0, 0.0),
      kappa_curve = c(0.0, 0.0),
      t_df_curve = c(10.0, 10.0),
      threshold = threshold,
      top_k = 1L,
      n_full = 3L,
      w0_sorted = c(3.0, 2.0, 1.0),
      burnin_periods = 0L,
      metadata = list(),
      exit_p_base = 1.0,
      exit_alpha = 0.0,
      exit_transient_rate = 1.0,
      entry_burst_frac = 0.0,
      t_df_curve_precal = NULL
    ),
    class = "rankdiff_params"
  )

  sim <- simulate_one(123L, params, 3L, cfg)

  expect_true(all(is.na(sim$tracked_values[2, ])))
  expect_true(all(sim$tracked_ranks[2, ] == 0L))

  params$entry_burst_frac <- 1.0
  sim_full_burst <- simulate_one(123L, params, 3L, cfg)
  expect_true(all(is.finite(sim_full_burst$observed_counts)))
})
