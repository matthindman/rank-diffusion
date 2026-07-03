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
horizons; the conditional out-of-sample forecast (real filtered initial state +
per-entity temperament) **beats the persistence baseline** — rel err 0.118 ± 0.061
vs 0.168 ± 0.004, winning 4 of 5 rolling splits with 100% bootstrap-CI coverage.
Facebook (2026-07-03, Era-A slice of the rebuilt CrowdTangle panel, instrument-era
disciplined — MODEL_STATUS §2g/§2h): the unconditional model now **beats persistence
on all 5 rolling splits** — 0.114 ± 0.046 vs 0.144 ± 0.030 — with calibrated
σ_obs scale = 1.0 on 4/5 splits (self-consistent observation model), and σ_obs is
now Spec-B-identified from FB dailies at the universe head (0.21→0.37 "of tracked
activity"). Cross-metric check on Reddit COMMENT karma (T=136 census): parameters
transport (κ shape, floor shape, t_df) but s = 0.69 (metric-dependent) and the
long panel exposes a structural 4–13-week reversion deficit (VR block) that σ_obs
identification does not fix — the sharpest open modeling item (§2h).

**Canonical status record: [`llm_fitting/MODEL_STATUS.md`](llm_fitting/MODEL_STATUS.md)**
(model spec, locked-in results, corrected pitfalls, reproduction commands).

```sh
python -m pytest tests/ -q                        # regression suite

# in-sample scorecard (Reddit, full stack)
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --spec-b

# OOS movement gate (the acceptance criterion); --conditional state|vhat
# forecasts from the real filtered end-of-train state (+ per-entity temperament)
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state
python llm_fitting/rankdiff_kalman.py facebook --oos --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state

# sigma_obs identification report (Spec-A vs Spec-B)
python llm_fitting/spec_b_sigma_obs.py 5000
```

Data (not committed): FB `data/raw/fb_ranked_weekly_cutdown.parquet` (**always the
cutdown file** — the full file is corrupt after ~88 weeks); Reddit
`data/reddit/reddit_weekly.parquet` + `reddit_daily.parquet` (Spec-B); Instagram
top-50k parquet is a **negative control only** (query-censored collection).
New (2026-07, on the SSD via `data/ssd` symlink; see DATA_PHASE2_REPORT.md):
`derived/fb_daily.parquet` + `fb_weekly_rebuilt.parquet` (158 clean weeks incl. 72
beyond the old corruption point — enables Spec-B on FB) and
`derived/reddit_comments_2018-12_2021-06_{daily,weekly}.parquet` (136 weeks,
comment karma). **Censoring caveat**: Reddit/Pushshift is a platform census, so
its coverage shares are platform-wide; CrowdTangle (FB/IG) tracked only large
pages — its coverage shares describe the *tracked* universe only
(MODEL_STATUS §2g).

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
