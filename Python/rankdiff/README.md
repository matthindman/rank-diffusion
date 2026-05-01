# rankdiff

`rankdiff` is a Python package for preprocessing ranked panel data, fitting a rank-diffusion style model, and returning fitted parameters plus diagnostics.

## What it expects

Input data must be a **`parquet`** file with one row per entity-time observation.

At minimum, your data must contain:

- an entity ID column
- a timestamp column
- a numeric metric column used for ranking

You can easily map your column names through `Config`.

Here's an example of the input structure:

```
| entity_id | timestamp  | metric_value |
| --------- | ---------- | ------------ |
| A         | 2024-01-07 | 1200         |
| B         | 2024-01-07 | 950          |
| A         | 2024-01-14 | 1250         |
| B         | 2024-01-14 | 900          |
```

## Minimal usage

1. Get setup in a fresh terminal:

```bash
cd ~/path/to/src/rankdiff
pip install -e .
python
```

2. Run:

```python
from rankdiff import Config, run_pipeline

cfg = Config(
    data_path="data/toy_rank_data.parquet",
    id_col="entity_id",
    timestamp_col="timestamp",
    metric_col="metric_value",
    rank_col=None,
    dev_mode=True,
    track_entity_count=50,
    max_duplicate_entity_period_rate=0.05,
    min_anchor_bins=1,
    max_anchor_bins=3,
    min_anchor_bin_size=5,
    acf_sample_size=25,
    n_optim_restarts=5,
    mc_reps=1,
    mc_reps_dev=1,
    kurtosis_cal_reps=1,
)

result = run_pipeline(cfg)

# print main parameter results in a clean way
print("=== PARAMETER RESULTS ===")
print(f"sigma_obs:      {result.params.sigma_obs}")
print(f"sigma_het:      {result.params.sigma_het}")
print(f"alpha_arch:     {result.params.alpha_arch}")
print(f"t_df_global:    {result.params.t_df_global}")
print(f"jump_prob:      {result.params.jump_prob}")
print(f"top_k:          {result.params.top_k}")
print(f"burnin_periods: {result.params.burnin_periods}")
```

## Saving results

By default, `run_pipeline()` returns results in memory and does not write to disk. To save outputs, users should run:

```python
from rankdiff.io import save_fit_result

save_fit_result(result, "outputs")
```

This will create:

- `fit_result.json`
- `curves.csv`

Additional helpers are available for saving:

- ablation results
- sensitivity results

## Extended analysis (ablation, sensitivity, plotting)

In addition to the core pipeline, `rankdiff` also supports deeper model evaluation:

- **Ablation analysis**: understand which model components improve diagnostics
- **Sensitivity analysis**: assess robustness to parameter perturbations
- **Plotting utilities**: generate diagnostic, ablation, and sensitivity visualizations

For example:

```python
from rankdiff import Config, run_pipeline
from rankdiff.ablation import run_ablation
from rankdiff.sensitivity import run_sensitivity
from rankdiff.diagnostics import score_diagnostics
from rankdiff.plotting import plot_core_diagnostics, plot_ablation, plot_sensitivity
from rankdiff.simulator import simulate_many

cfg = Config(...)

result = run_pipeline(cfg)

# simulate and score
sims = simulate_many(result.params, result.data, cfg)
sim_diags = [sim["diagnostics"] for sim in sims]
score = score_diagnostics(result.data.empirical, sim_diags, cfg)

# run analyses
abl = run_ablation(result.params, result.data, cfg)
sens = run_sensitivity(result.params, result.data, cfg)

# plot results
plot_core_diagnostics(result.data, score, "outputs", "run")
plot_ablation(abl, "outputs", "run")
plot_sensitivity(sens, cfg.sensitivity_deltas, "outputs", "run")
```

## Quick Start

Of note, as used above, the package comes pre-loaded with a synthetic toy parquet dataset (which users can follow re: data structure, format, etc), located: `data/toy_rank_data.parquet`. Using this sample data, users can run the example workflow end to end via:

```bash
python examples/quickstart.py
```

which returns:

```python
=== PARAMETER RESULTS ===
sigma_obs:      0.045883707826526676
sigma_het:      0.190775387634835
alpha_arch:     0.11466806279986036
t_df_global:    200.0
jump_prob:      0.005
top_k:          10
burnin_periods: 3623

=== DIAGNOSTIC SUMMARY ===
emp_median_var: 0.008421258575640262
emp_mean_var: 0.00905710731963054
emp_kurt: 0.1016969039241129
zipf_slope: -0.8829688285030384
mean_exit_rate: 0.0
```

## Public API

The main public entry points are:

- `Config`
- `run_pipeline`

Additional modules:

- `rankdiff.ablation`
- `rankdiff.sensitivity`
- `rankdiff.plotting`

Lower level functions are also available for advanced usage. Explore more deeply in `src/rankdiff`. But in brief, the minimal required scope for effective use of the package is:

- parquet input only
- `config`-driven column mapping
- pipeline stages: preprocessing, initialization, fitting, calibration, diagnostics
- extended evaluation: ablation, sensitivity, visualization

*Note: the package is under active development, with frequent changes expected.*
