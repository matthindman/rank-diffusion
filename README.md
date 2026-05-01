# rank-diffusion

This repository contains the full development and production implementation of a rank diffusion modeling framework for analyzing the dynamics of social media platform panel data over time. Included:

- A production Python package (`/Python/rankdiff`)
- A production R package (`/R/rankdiff`)
- Archived research code, experiments, and earlier pipelines

## Overview

The rank diffusion framework is designed to model how entities/endpoints/profiles/etc move through ranked online platforms over time. Examples include:

- Social media engagement rankings
- Economic or firm-level performance rankings
- Content popularity dynamics
- Any panel dataset where entities are repeatedly ranked over time

At a high level, the model:

- Ingests panel data (entity × time)
- Constructs rank trajectories
- Estimates a stochastic diffusion process governing rank changes, assuming roughly Brownian movement
- Calibrates model parameters
- Evaluates fit using diagnostic statistics

## Repository Structure

```
rank-diffusion/
├── Python/rankdiff/     # Production Python pkg
├── R/rankdiff/          # Production R pkg
├── llm_fitting/         # CLaude/LLM modeling and older experimental workflows
├── archive/             # Historical code, scripts, and prior pipelines
├── .gitignore
├── README.md
```

#### What goes where

1. Start here (most users):

- `Python/rankdiff`
- `R/rankdiff`

2. Experimental / research workflows:
`llm_fitting`

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
