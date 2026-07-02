test_that("create_config returns valid config with defaults", {
  cfg <- create_config(data_path = "test.parquet")
  expect_s3_class(cfg, "rankdiff_config")
  expect_equal(cfg$data_path, "test.parquet")
  expect_equal(cfg$mc_reps, 25L)
  expect_equal(cfg$n_jobs, 1L)
  expect_equal(cfg$random_seed, 42L)
  expect_equal(cfg$id_col, "endpoint_id")
})

test_that("create_config resolved_mc_reps respects dev_mode", {
  cfg_prod <- create_config(data_path = "test.parquet", dev_mode = FALSE)
  cfg_dev  <- create_config(data_path = "test.parquet", dev_mode = TRUE)
  expect_equal(resolved_mc_reps(cfg_prod), 25L)
  expect_equal(resolved_mc_reps(cfg_dev), 5L)
})

test_that("sim_features creates valid feature set", {
  f <- sim_features()
  expect_s3_class(f, "rankdiff_features")
  expect_true(f$burn_in)
  expect_true(f$heavy_tails)
})

test_that("infer_cadence and add_period_index are exported", {
  expect_true(is.function(infer_cadence))
  expect_true(is.function(add_period_index))
})
