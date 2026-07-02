# Confirm the packaged toy parquet file is present and accessible
test_that("toy parquet path resolves from inst/extdata", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")

  expect_true(nzchar(toy_path))
  expect_true(file.exists(toy_path))
})

# Confirm the main pipeline runs end to end on the toy data and returns the expected object structure
test_that("main pipeline runs end to end on toy data", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")

  cfg <- create_config(
    data_path = toy_path,
    id_col = "entity_id",
    timestamp_col = "timestamp",
    metric_col = "metric_value",
    rank_col = NULL,
    dev_mode = TRUE,
    track_entity_count = 50L,
    max_duplicate_entity_period_rate = 0.05,
    min_anchor_bins = 1L,
    max_anchor_bins = 3L,
    min_anchor_bin_size = 5L,
    acf_sample_size = 25L,
    n_optim_restarts = 5L,
    mc_reps = 1L,
    mc_reps_dev = 1L,
    kurtosis_cal_reps = 1L
  )

  result <- rankdiff_fit(cfg)

  expect_s3_class(result, "rankdiff_result")
  expect_s3_class(result$config, "rankdiff_config")
  expect_s3_class(result$data, "rankdiff_bundle")
  expect_s3_class(result$initial, "rankdiff_initial")
  expect_s3_class(result$params, "rankdiff_params")
})

# Confirm the returned data bundle contains nonempty, coherent core fields
test_that("pipeline returns a valid populated data bundle", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")

  cfg <- create_config(
    data_path = toy_path,
    id_col = "entity_id",
    timestamp_col = "timestamp",
    metric_col = "metric_value",
    rank_col = NULL,
    dev_mode = TRUE,
    track_entity_count = 50L,
    max_duplicate_entity_period_rate = 0.05,
    min_anchor_bins = 1L,
    max_anchor_bins = 3L,
    min_anchor_bin_size = 5L,
    acf_sample_size = 25L,
    n_optim_restarts = 5L,
    mc_reps = 1L,
    mc_reps_dev = 1L,
    kurtosis_cal_reps = 1L
  )

  result <- rankdiff_fit(cfg)
  bundle <- result$data

  expect_true(is.data.frame(bundle$panel))
  expect_gt(nrow(bundle$panel), 0)
  expect_gt(bundle$n_periods, 0)
  expect_gt(bundle$n_entities, 0)
  expect_gt(bundle$mean_n, 0)
  expect_gte(bundle$max_n, 1)

  expect_true("entity_id" %in% names(bundle$panel))
  expect_true("timestamp" %in% names(bundle$panel))
  expect_true("metric_value" %in% names(bundle$panel))
  expect_true("rank" %in% names(bundle$panel))
  expect_true("period_index" %in% names(bundle$panel))
})

# Confirm the fitted initial and final parameter objects contain finite core quantities
test_that("pipeline returns finite core parameter estimates", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")

  cfg <- create_config(
    data_path = toy_path,
    id_col = "entity_id",
    timestamp_col = "timestamp",
    metric_col = "metric_value",
    rank_col = NULL,
    dev_mode = TRUE,
    track_entity_count = 50L,
    max_duplicate_entity_period_rate = 0.05,
    min_anchor_bins = 1L,
    max_anchor_bins = 3L,
    min_anchor_bin_size = 5L,
    acf_sample_size = 25L,
    n_optim_restarts = 5L,
    mc_reps = 1L,
    mc_reps_dev = 1L,
    kurtosis_cal_reps = 1L
  )

  result <- rankdiff_fit(cfg)

  expect_true(is.finite(result$initial$sigma_obs))
  expect_true(is.finite(result$initial$sigma_het))
  expect_true(is.finite(result$initial$alpha_arch))
  expect_true(is.finite(result$initial$t_df_global))

  expect_true(is.finite(result$params$sigma_obs))
  expect_true(is.finite(result$params$sigma_het))
  expect_true(is.finite(result$params$alpha_arch))
  expect_true(is.finite(result$params$t_df_global))
  expect_true(is.finite(result$params$jump_prob))
  expect_gt(result$params$top_k, 0)
  expect_gt(result$params$burnin_periods, 0)
  expect_length(result$params$kappa_curve, length(result$params$z_knots))
})

# Confirm the empirical diagnostics are present and contain the expected named components
test_that("pipeline returns expected empirical diagnostics", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")

  cfg <- create_config(
    data_path = toy_path,
    id_col = "entity_id",
    timestamp_col = "timestamp",
    metric_col = "metric_value",
    rank_col = NULL,
    dev_mode = TRUE,
    track_entity_count = 50L,
    max_duplicate_entity_period_rate = 0.05,
    min_anchor_bins = 1L,
    max_anchor_bins = 3L,
    min_anchor_bin_size = 5L,
    acf_sample_size = 25L,
    n_optim_restarts = 5L,
    mc_reps = 1L,
    mc_reps_dev = 1L,
    kurtosis_cal_reps = 1L
  )

  result <- rankdiff_fit(cfg)
  emp <- result$data$empirical

  expect_true(is.list(emp))
  expect_true("acf_emp" %in% names(emp))
  expect_true("racf_emp" %in% names(emp))
  expect_true("pers_emp" %in% names(emp))
  expect_true("xr2_emp" %in% names(emp))
  expect_true("vr_emp" %in% names(emp))
  expect_true("zipf_slope" %in% names(emp))
  expect_true("top_k" %in% names(emp))
  expect_gt(emp$top_k, 0)
})

# Confirm simulation-based scoring diagnostics exist in the returned result
test_that("pipeline returns scored diagnostics summary", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")

  cfg <- create_config(
    data_path = toy_path,
    id_col = "entity_id",
    timestamp_col = "timestamp",
    metric_col = "metric_value",
    rank_col = NULL,
    dev_mode = TRUE,
    track_entity_count = 50L,
    max_duplicate_entity_period_rate = 0.05,
    min_anchor_bins = 1L,
    max_anchor_bins = 3L,
    min_anchor_bin_size = 5L,
    acf_sample_size = 25L,
    n_optim_restarts = 5L,
    mc_reps = 1L,
    mc_reps_dev = 1L,
    kurtosis_cal_reps = 1L
  )

  result <- rankdiff_fit(cfg)
  diag <- result$diagnostics

  expect_true(is.list(diag))
  expect_true("mc_stats" %in% names(diag))
  expect_true("tests" %in% names(diag))
  expect_true("n_pass" %in% names(diag))
  expect_true("n_total" %in% names(diag))
  expect_gte(diag$n_total, 0)
  expect_gte(diag$n_pass, 0)
})

# Confirm the processed panel has unique entity-period rows after canonicalization
test_that("processed panel has unique entity-period combinations", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")

  cfg <- create_config(
    data_path = toy_path,
    id_col = "entity_id",
    timestamp_col = "timestamp",
    metric_col = "metric_value",
    rank_col = NULL,
    dev_mode = TRUE,
    track_entity_count = 50L,
    max_duplicate_entity_period_rate = 0.05,
    min_anchor_bins = 1L,
    max_anchor_bins = 3L,
    min_anchor_bin_size = 5L,
    acf_sample_size = 25L,
    n_optim_restarts = 5L,
    mc_reps = 1L,
    mc_reps_dev = 1L,
    kurtosis_cal_reps = 1L
  )

  result <- rankdiff_fit(cfg)
  panel <- result$data$panel

  key_n <- nrow(unique(panel[, c("entity_id", "period_index")]))
  expect_equal(key_n, nrow(panel))
})

# Confirm saving artifacts works when an explicit temp output directory is provided
test_that("pipeline can save outputs to a temporary directory", {
  toy_path <- system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")
  out_dir <- file.path(tempdir(), paste0("rankdiff-pipeline-", Sys.getpid()))
  unlink(out_dir, recursive = TRUE, force = TRUE)
  on.exit(unlink(out_dir, recursive = TRUE, force = TRUE), add = TRUE)

  cfg <- create_config(
    data_path = toy_path,
    id_col = "entity_id",
    timestamp_col = "timestamp",
    metric_col = "metric_value",
    rank_col = NULL,
    output_dir = out_dir,
    dev_mode = TRUE,
    track_entity_count = 50L,
    max_duplicate_entity_period_rate = 0.05,
    min_anchor_bins = 1L,
    max_anchor_bins = 3L,
    min_anchor_bin_size = 5L,
    acf_sample_size = 25L,
    n_optim_restarts = 5L,
    mc_reps = 1L,
    mc_reps_dev = 1L,
    kurtosis_cal_reps = 1L
  )

  result <- rankdiff_fit(cfg)

  expect_s3_class(result, "rankdiff_result")
  expect_true(dir.exists(out_dir))
})