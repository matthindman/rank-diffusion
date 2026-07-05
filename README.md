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
moment-identified scalar per metric — s ≈ 0.89–0.94 for FB interactions and
Reddit submission karma, 0.69 for Reddit comment karma), and Gabaix rebirth at
the bottom of a pre-registered top-coverage universe (Reddit K=5,000 ≈ 90% of
weekly karma; FB K=3,500).

Headline (2026-07-03, MODEL_STATUS §2i–§2m): the long-standing 4–13-week
over-persistence was decomposed into four measured layers — unidentified home
reversion (fixed: long-horizon variance moments, `--md-vr`), an integrated common
factor the data contradicts (fixed: measured stationary level, `--stat-factor`), a
missing medium timescale (fixed on FB: `--two-scale`), and permanent-share
heterogeneity (`--mix-hetero`: σ_perm scales with the SAME entity amplitude as the
transitory — b = s(h*)/s(1) measured at 1.02–1.08, i.e. **structure B holds**).
Results: **Facebook Era A 15/15 in-sample** (first legitimate perfect goal-1 card,
"of tracked activity") with OOS **beating persistence on all 5 splits** (0.114 ±
0.046 vs 0.144 ± 0.030, spec-A calibrated); **Reddit comments** (T=136 census)
conditional gate at par with persistence with **100% bootstrap-CI coverage**
(0.159 ± 0.070 vs 0.160 ± 0.068, last-split held-out displacement exact at all
horizons); Reddit submissions unchanged (conditional **beats persistence 4/5**,
0.118 vs 0.168). Every parameter is moment-identified, instrument-identified, or
train-only-calibrated and declared (the OOS gate's σ_obs scale); none is tuned to
an evaluation score. σ_obs is independently identified at the universe head and
bracketed below it. Remaining: estimation-vs-scorecard population asymmetry
(comments), FB head collisions, and system breadth (§2m).

**Canonical status record: [`llm_fitting/MODEL_STATUS.md`](llm_fitting/MODEL_STATUS.md)**
(model spec, locked-in results, corrected pitfalls, reproduction commands).

```sh
python -m pytest tests/ -q                        # regression suite (41 tests)

# FB Era A working spec — 15/15 in-sample (goal-1)
python llm_fitting/minimal_rankdiff.py facebook_a --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor --two-scale --mix-hetero

# Reddit comments working spec (census, T=136)
python llm_fitting/minimal_rankdiff.py reddit_comments --top-k 12500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor --mix-hetero

# OOS movement gates (the acceptance criterion)
python llm_fitting/rankdiff_kalman.py facebook_a --oos --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails                     # 0.114 vs 0.144, 5/5
python llm_fitting/rankdiff_kalman.py reddit_comments --oos --top-k 12500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --mix-hetero --conditional state
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state # 0.118 vs 0.168

# sigma_obs identification (Spec-A vs Spec-B); platform arg optional
python llm_fitting/spec_b_sigma_obs.py 5000
python llm_fitting/spec_b_sigma_obs.py 3500 facebook_a
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
