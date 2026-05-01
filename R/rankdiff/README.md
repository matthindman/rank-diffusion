# rankdiff

`rankdiff` is an R package for preprocessing ranked panel data, fitting a rank-diffusion style model, and returning fitted parameters plus diagnostics.

## What it expects

Input data must be a **`parquet`** file with one row per entity-time observation.

At minimum, your data must contain:

- an entity ID column
- a timestamp column
- a numeric metric column used for ranking

You can map your column names through `create_config()`.

Here’s an example of the input structure:

```
| entity_id | timestamp  | metric_value |
| --------- | ---------- | ------------ |
| A         | 2024-01-07 | 1200         |
| B         | 2024-01-07 | 950          |
| A         | 2024-01-14 | 1250         |
| B         | 2024-01-14 | 900          |
```


## Minimal usage

1. Open a fresh R session and install locally:

```r
setwd("~/path/to/src/rankdiff")

devtools::load_all()
```

2. Run:

```r
library(rankdiff)

cfg <- create_config(
  data_path = system.file("extdata", "toy_rank_data.parquet", package = "rankdiff"),
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

print(result$params)
names(result$diagnostics)
```

Or the latest stable relase from CRAN: 

```r
install.packages("rankdiff")

library(rankdiff)
```

## Saving results

By default, `rankdiff_fit()` writes outputs if `output_dir` is specified in the `config`.

To explicitly control saving:

```r
cfg <- create_config(
  data_path = "...",
  output_dir = "outputs"
)

result <- rankdiff_fit(cfg)
```

This will create files such as:

- `fit_result.json`
- `curves.csv`
- diagnostic plots (if enabled)

Additional outputs, depending on settings, may include:

- ablation summaries
- sensitivity summaries

## Extended analysis (ablation, sensitivity, plotting)

In addition to the core pipeline, `rankdiff` supports deeper model evaluation, e.g.:

- Ablation analysis: understand which model components improve diagnostics
- Sensitivity analysis: assess robustness to parameter perturbations
- Plotting utilities: generate diagnostic, ablation, and sensitivity visualizations

For example:

```r
library(rankdiff)

cfg <- create_config(...)
result <- rankdiff_fit(cfg)

abl <- run_ablation(result$params, result$data, cfg)
sens <- run_sensitivity(result$params, result$data, cfg)

plot_ablation(abl, "outputs", "run")
plot_sensitivity(sens, cfg$sensitivity_deltas, "outputs", "run")
plot_core_diagnostics(result$data, result$diagnostics, "outputs", "run")
```

## Quick Start

The package includes a synthetic toy parquet dataset located at:

```
system.file("extdata", "toy_rank_data.parquet", package = "rankdiff")
```

Users can run the full workflow on this dataset as shown above.

## Public API

The main public entry points are:

- `create_config()`
- `rankdiff_fit()`

Additional functions:

- `run_ablation()`
- `run_sensitivity()`
- plotting utilities (`plot_*`)
- save/load helpers (`save_fit_result()`, `load_fit_result()`)

Lower-level functions are also available for advanced usage within the package source.

In brief, the minimal required scope for effective use is:

- parquet input
- config-driven column mapping
- pipeline stages: preprocessing, initialization, fitting, calibration, diagnostics
- extended evaluation: ablation, sensitivity, visualization

*Note: the package is under active development, with frequent changes expected.*