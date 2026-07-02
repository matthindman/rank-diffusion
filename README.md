# rank-diffusion

This repository contains the full development and production implementation of a rank diffusion modeling framework for analyzing the dynamics of social media platform panel data over time. Included:

- A production Python package (`/Python/rankdiff`)
- A production R package (`/R/rankdiff`)
- The active research line (`/llm_fitting` + `/tests`) — see **Current research line** below
- Archived research code, experiments, and earlier pipelines (`/archive`)

## Overview

The rank diffusion framework is designed to model how entities/endpoints/profiles/etc move through ranked online platforms over time, e.g.:

- Social media engagement rankings
- Economic or firm/org performance rankings
- Content popularity dynamics
- Or any panel data where entities are repeatedly ranked over time

At a high level, the model:

- Ingests panel data (`entity × time`)
- Constructs rank trajectories
- Estimates a stochastic diffusion process governing rank changes assuming roughly Brownian movement
- Calibrates model parameters
- Evaluates fit using diagnostic statistics

## Current research line (2026-07)

Platform rankings show **Eulerian stability with Lagrangian churn** — the rank-size
curve and per-rank share are stationary while the identities occupying fixed ranks
turn over. The active research prototypes model this with: an OU "home" level
(reversion estimated from the change-autocovariance tail), measurement noise
independently identified from the daily-within-week noise floor (Reddit),
persistent entity-level volatility heterogeneity ("temperament", one
moment-identified scalar, s ≈ 0.9 on both platforms), and Gabaix rebirth at the
bottom of a pre-registered top-coverage universe (Reddit K=5,000 ≈ 90% of weekly
karma; FB K=3,500).

Headline (Reddit): in-sample 14/15 with rank displacement exact at 1/4/13-week
horizons; rolling-origin OOS movement gate at par with the persistence baseline
with 100% bootstrap-CI coverage; σ_obs identified (Spec-A weekly covariances and
Spec-B daily replication agree within ~25%). Facebook: OOS at par (0.158 ± 0.027
vs 0.146 ± 0.022).

**Canonical status record: [`llm_fitting/MODEL_STATUS.md`](llm_fitting/MODEL_STATUS.md)**
(model spec, locked-in results, corrected pitfalls, reproduction commands).

```sh
python -m pytest tests/ -q                        # regression suite

# in-sample scorecard (Reddit, full stack)
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --spec-b

# OOS movement gate (the acceptance criterion)
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --spec-b
python llm_fitting/rankdiff_kalman.py facebook --oos --temperament \
    --min-knot-entities 8 --md-lags 6

# sigma_obs identification report (Spec-A vs Spec-B)
python llm_fitting/spec_b_sigma_obs.py 5000
```

Data (not committed): FB `data/raw/fb_ranked_weekly_cutdown.parquet` (**always the
cutdown file** — the full file is corrupt after ~88 weeks); Reddit
`data/reddit/reddit_weekly.parquet` + `reddit_daily.parquet` (Spec-B); Instagram
top-50k parquet is a **negative control only** (query-censored collection).

## Repository Structure

```
rank-diffusion/
├── Python/rankdiff/     # Production Python pkg
├── R/rankdiff/          # Production R pkg
├── llm_fitting/         # Active research prototypes + LLM modeling workflows
├── tests/               # Regression suite for the research prototypes
├── archive/             # Historical code, scripts, and prior pipelines
├── .gitignore
├── README.md
```

#### What goes where

1. Start here (most users):

- `Python/rankdiff`
- `R/rankdiff`

2. Active research line (current results):
`llm_fitting` (+ `tests`) — see [`llm_fitting/MODEL_STATUS.md`](llm_fitting/MODEL_STATUS.md)

3. Old code and dev history:
`archive` (*not actively maintained, for reference only*)

## Quick Start

#### Python

Basic usage:

```bash
cd Python/rankdiff
pip install -e .
python examples/quickstart.py
```

Core API:

```python
from rankdiff import Config, run_pipeline

cfg = Config(...)
result = run_pipeline(cfg)
```

#### R

Basic usage:

```R
setwd("R/rankdiff")
devtools::load_all()

library(rankdiff)
result <- rankdiff_fit(cfg)
```

#### Data Requirements

Both implementations expect panel data in parquet format with:

- `entity_id`
- `timestamp`
- `metric_value` (used for ranking)

e.g.:

```
| entity_id | timestamp  | metric_value |
|-----------|------------|--------------|
| A         | 2024-01-07 | 1200         |
| B         | 2024-01-07 | 950          |
```

Column names are configurable via the respective config objects. See language-specific `README` files for more on this. 

## Core Pipeline

Across both implementations, the workflow is conceptually identical:

1. Preprocessing
  - Canonicalize panel structure
  - Infer cadence and construct periods

2. Initialization
  - Build anchor bins across rank space
  - Estimate initial parameters

3. Model Fitting
  - Estimate diffusion parameters
  - Fit heterogeneity and noise components

4. Calibration
  - Match empirical moments (variance, ACF, etc.)
  - Stabilize parameters

5. Diagnostics

6. Simulation

#### Extended Analysis

Both packages support deeper evaluation tools, e.g.:

- Ablation analysis: Identify which model components drive performance
- Sensitivity analysis: Assess robustness to parameter perturbations
- Visualization:
    - Diagnostic plots
    - Simulation comparisons
