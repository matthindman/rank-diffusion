# Modularization Plan (Phase 1): Child Documents + Explicit Cached Artifacts
## Implementation Specification for Codex

### Introduction and context
You are implementing a refactor of an existing large, monolithic R Markdown analysis (“master Rmd”), which has become too slow to run end-to-end. The project must support two workflows:

1) **Fast iteration** while exploratory work continues: developers should be able to run a single section quickly without re-running expensive data prep and simulation steps.

2) **Single-document assembly for review**: the complete analysis must still be renderable into a single PDF for robustness/peer review, with reproducible results.

Critical constraint: **minimize initial switching cost**. Therefore, Phase 1 uses the **R Markdown child documents pattern** (no targets/drake yet). To avoid silent staleness, we will implement **explicit intermediate artifact caching** with robust invalidation (fingerprints based on raw data mtimes, code mtimes, key parameters, and a cache_version). This keeps the workflow familiar while providing substantial runtime wins.

This plan is written as an implementation spec. Follow it literally unless a concrete detail must be adapted to match existing object names/paths.

---

## 0) End state after Phase 1
You will deliver:

- `report/master_report.Rmd` that produces a single PDF by stitching together child documents in `analysis/`.
- Ability to render any single analysis module quickly via `scripts/render_section.R` (HTML by default).
- Heavy intermediate objects cached as `cache/*.rds` with metadata in `cache/_meta/*.json`, rebuilt only when inputs change.
- Reusable logic moved out of `.Rmd` files into `R/*.R` (functions only), plus a centralized config.
- Reproducibility bundle: `renv.lock`, stable paths via `here`, deterministic seeds, provenance footer, and a `README.md`.

---

## 1) Repository layout (create exactly this)
project_root/
  FB_Rank_Diffusion.Rproj
  renv.lock
  .Rprofile
  .gitignore

  R/
    init.R                # loads pkgs; sources all R/*.R; builds CFG from params
    config.R              # params + derived constants + validation + seed
    cache.R               # cache_or_compute() + fingerprints + helpers
    utils.R
    data_prep.R
    metrics.R
    simulation.R
    plotting.R

  data/
    raw/                  # input parquet(s); immutable
    derived/              # optional, if you want non-cache derived outputs

  cache/                  # cached artifacts (NOT committed)
    _meta/                # metadata sidecars

  analysis/               # child Rmd sections (NO YAML)
    00_setup.Rmd
    01_data_qc.Rmd
    02_cdc.Rmd
    03_durable_change.Rmd
    04_model_variants.Rmd
    05_sigma_calibration.Rmd
    06_micro_validation.Rmd
    90_supplement.Rmd

  report/
    master_report.Rmd
    section_wrapper.Rmd   # renders a single child with shared setup

  scripts/
    build_cache.R         # builds/refreshes heavy artifacts
    render_section.R      # render one analysis child quickly (wrapper)
    smoke_test.R          # minimal clean-run test

  output/
    report/               # PDFs, HTMLs
    figures/
    tables/

  README.md

.gitignore must exclude:
  cache/
  output/
  data/raw/* (if raw data cannot be committed)

---

## 2) Configuration: one source of truth

### 2.1 master_report.Rmd YAML (mother document)
report/master_report.Rmd contains YAML params and includes children. Required params:

- mode: "dev" or "full"
- output_type: "full" | "paper" | "supplement"
- use_cache: TRUE/FALSE
- run_heavy: TRUE/FALSE
- cache_version: string to invalidate caches globally (e.g., "v1")
- seed, K_cut, K_tail_buffer, horizons_durable, etc. (mirror existing)

Example YAML:
---
title: "Facebook Weekly Rank Diffusion: Master Analysis"
output:
  pdf_document:
    toc: true
    toc_depth: 3
params:
  mode: "dev"
  output_type: "full"
  use_cache: true
  run_heavy: false
  cache_version: "v1"
  seed: 1823
  K_cut: 12000
  K_tail_buffer: 2000
  horizons_durable: [4, 8]
---

### 2.2 R/config.R
Implement config_from_params(params) that:
- reads params (with defaults for scripts)
- coerces/validates types
- creates derived constants (bucket_def, etc.)
- creates cache directories
- sets seed deterministically (once)

Skeleton:
```r
# R/config.R
`%||%` <- function(x, y) if (is.null(x)) y else x

config_from_params <- function(params) {
  cfg <- within(list(), {
    mode <- params$mode %||% "dev"
    output_type <- params$output_type %||% "full"
    use_cache <- isTRUE(params$use_cache)
    run_heavy <- isTRUE(params$run_heavy)
    cache_version <- params$cache_version %||% "v1"

    seed <- as.integer(params$seed %||% 1823L)
    K_cut <- as.integer(params$K_cut %||% 12000L)
    K_tail_buffer <- as.integer(params$K_tail_buffer %||% 2000L)
    horizons_durable <- as.integer(params$horizons_durable %||% c(4L, 8L))

    bucket_def <- list(
      breaks = c(0, 25, 250, Inf),
      labels = c("large", "midsize", "small")
    )

    cache_dir <- here::here("cache")
    meta_dir  <- here::here("cache", "_meta")
    dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
    dir.create(meta_dir,  showWarnings = FALSE, recursive = TRUE)
  })

  set.seed(cfg$seed)
  cfg
}
```

---

## 3) Shared code: move logic out of Rmds

### 3.1 R/init.R (loaded by everything)

Responsibilities:

* load packages (suppress startup messages)
* source all R/*.R in stable order
* build CFG from params (or defaults if absent)
* expose CFG and functions in the global environment

Skeleton:

```r
# R/init.R
suppressPackageStartupMessages({
  library(here)
  library(tidyverse)
  library(arrow)
  library(digest)
  library(jsonlite)
  library(withr)
})

source(here("R", "config.R"))
source(here("R", "cache.R"))
source(here("R", "utils.R"))
source(here("R", "data_prep.R"))
source(here("R", "metrics.R"))
source(here("R", "simulation.R"))
source(here("R", "plotting.R"))

if (!exists("params")) params <- list()
CFG <- config_from_params(params)
```

### 3.2 Child Rmd rule (strict)

Each analysis/*.Rmd must:

* have NO YAML
* have NO library() calls
* assume R/init.R has already been sourced (so CFG exists)
* load inputs via cache_or_compute() or readRDS() of cached artifacts
* only produce tables/plots/diagnostics

---

## 4) Caching: explicit artifacts with safe invalidation

### 4.1 R/cache.R API (implement exactly)

Required functions:

* cache_path(name), meta_path(name)
* deps_code_mtime(paths), deps_file_mtime(path)
* fingerprint(deps)
* cache_or_compute(name, compute_fn, deps, force = FALSE)

Skeleton:

```r
# R/cache.R
cache_path <- function(name, cfg = CFG) here::here("cache", paste0(name, ".rds"))
meta_path  <- function(name, cfg = CFG) here::here("cache", "_meta", paste0(name, ".json"))

deps_code_mtime <- function(paths) {
  info <- file.info(paths)
  setNames(as.character(info$mtime), paths)
}

deps_file_mtime <- function(path) {
  as.character(file.info(path)$mtime)
}

fingerprint <- function(deps) digest::digest(deps, algo = "xxhash64")

cache_or_compute <- function(name, compute_fn, deps, force = FALSE, cfg = CFG) {
  stopifnot(is.function(compute_fn))
  fp <- fingerprint(deps)

  rds <- cache_path(name, cfg)
  meta <- meta_path(name, cfg)

  if (cfg$use_cache && file.exists(rds) && file.exists(meta) && !force) {
    m <- jsonlite::read_json(meta, simplifyVector = TRUE)
    if (!is.null(m$fingerprint) && identical(m$fingerprint, fp)) {
      message("[cache hit] ", name)
      return(readRDS(rds))
    }
    message("[cache stale] ", name)
  } else if (cfg$use_cache && file.exists(rds) && !force) {
    message("[cache missing meta] ", name)
  }

  message("[compute] ", name)
  obj <- compute_fn()
  saveRDS(obj, rds)
  jsonlite::write_json(
    list(
      name = name,
      fingerprint = fp,
      created_at = as.character(Sys.time()),
      deps = deps
    ),
    meta,
    pretty = TRUE, auto_unbox = TRUE
  )
  obj
}
```

### 4.2 Dependency spec rule

Every cached artifact must include deps with:

* CFG$cache_version
* key CFG params used to compute the artifact
* raw input file mtime(s)
* relevant R/*.R code file mtimes
* (for downstream artifacts) upstream artifact fingerprint(s)

Example:

```r
raw_path <- here::here("data","raw","fb_ranked_weekly.parquet")

endpoint_weekly <- cache_or_compute(
  "endpoint_weekly",
  compute_fn = function() build_endpoint_weekly(raw_path, K_cut = CFG$K_cut),
  deps = list(
    cache_version = CFG$cache_version,
    K_cut = CFG$K_cut,
    raw_mtime = deps_file_mtime(raw_path),
    code = deps_code_mtime(c(here("R","data_prep.R"), here("R","utils.R")))
  )
)

endpoint_fp <- jsonlite::read_json(meta_path("endpoint_weekly"))$fingerprint

emp_cdc <- cache_or_compute(
  "emp_cdc",
  compute_fn = function() compute_emp_cdc(endpoint_weekly, K_cut = CFG$K_cut),
  deps = list(
    cache_version = CFG$cache_version,
    K_cut = CFG$K_cut,
    upstream = list(endpoint_weekly_fp = endpoint_fp),
    code = deps_code_mtime(c(here("R","metrics.R")))
  )
)
```

---

## 5) Heavy lifting: scriptable cache builds

### 5.1 scripts/build_cache.R

Responsibilities:

* define params (or read from a small YAML)
* source R/init.R
* build/refresh heavy artifacts
* support args: --force and --what (subset list)

Artifacts to standardize (adapt names to existing objects):

* endpoint_weekly
* emp_cdc
* emp_durable_targets
* gauss_params_raw
* gauss_params_smoothed
* sim_baseline (heavy)
* micro_empirical_panels (if heavy)
* micro_sim_panels (if heavy)

For stochastic compute_fn, wrap with withr::with_seed(CFG$seed, {...}).

---

## 6) Reporting layer: master + children

### 6.1 report/master_report.Rmd structure

Setup chunk:

````r
```{r setup, include=FALSE}
knitr::opts_chunk$set(
  echo = TRUE,
  message = FALSE,
  warning = FALSE,
  fig.width = 9,
  fig.height = 5
)
source(here::here("R", "init.R"))
````

```

Child includes (gate heavy sections by params):
```r
# Setup
```{r child = here::here("analysis", "00_setup.Rmd")}
````

# Data Quality

```{r child = here::here("analysis", "01_data_qc.Rmd")}
```

# CDC

```{r child = here::here("analysis", "02_cdc.Rmd")}
```

# Durable Change

```{r child = here::here("analysis", "03_durable_change.Rmd")}
```

# Model Variants

```{r child = here::here("analysis", "04_model_variants.Rmd")}
```

# Sigma Calibration (heavy; default off in dev)

```{r child = if (params$run_heavy) here::here("analysis", "05_sigma_calibration.Rmd")}
```

# Micro Validation

```{r child = here::here("analysis", "06_micro_validation.Rmd")}
```

# Supplement

```{r child = if (params$output_type %in% c("supplement","full"))
  here::here("analysis", "90_supplement.Rmd")}
```

```

At end of master, include provenance:
- git commit hash (if available)
- sessionInfo()

---

## 7) Single-section dev workflow (fast iteration)

### 7.1 report/section_wrapper.Rmd
Purpose: render one child with shared setup.

YAML:
---
title: "Section Render"
output: html_document
params:
  child: null
  use_cache: true
  run_heavy: false
  cache_version: "v1"
  seed: 1823
  K_cut: 12000
  K_tail_buffer: 2000
  horizons_durable: [4, 8]
---

Body:
```r
```{r setup, include=FALSE}
source(here::here("R", "init.R"))
stopifnot(!is.null(params$child))
````

```{r child = here::here(params$child)}
```

```

### 7.2 scripts/render_section.R
- positional arg = child path, e.g. analysis/02_cdc.Rmd
- optional flags to override params (at least use_cache, run_heavy, cache_version)
- call rmarkdown::render("report/section_wrapper.Rmd", params = list(child = <path>, ...), output_dir = "output/report")

---

## 8) Paper vs supplement assembly
Phase 1: one master with params$output_type controlling inclusion.
Later: optionally split into two masters:
- report/paper_report.Rmd
- report/supplement_report.Rmd

---

## 9) Reproducibility requirements (must implement)
1) renv:
- renv::init() once
- renv::snapshot() after packages installed
- commit renv.lock
- README instructs reviewers: renv::restore()

2) deterministic randomness:
- set seed only in config_from_params()
- use withr::with_seed() for stochastic cached artifacts

3) project-relative paths:
- here::here() everywhere
- no absolute paths

4) provenance footer:
- Sys.time, git hash (if available), R version, sessionInfo()

---

## 10) Phased implementation checklist

Phase A (scaffold, no behavior change):
- create directories and placeholder files
- move helper functions into R/*.R
- add R/init.R and R/config.R

Phase B (split monolith into children):
- split into analysis/*.Rmd in original order
- create report/master_report.Rmd stitching them
- verify master renders and matches baseline output

Phase C (cache top bottlenecks):
- implement R/cache.R
- wrap the heaviest repeated steps with cache_or_compute()
- add scripts/build_cache.R

Phase D (single-section runner):
- add report/section_wrapper.Rmd and scripts/render_section.R
- verify fast section render and no unnecessary recomputation

Phase E (smoke test):
- scripts/smoke_test.R renders one light section and the master in dev mode

---

## 11) “Do not do” rules (prevent regressions)
- Child docs must not call library() or redefine core functions.
- Do not use knitr chunk cache as the primary speed mechanism.
- Do not read raw parquet in multiple children; raw IO should be cached once.
- Do not let bucket definitions / K_cut vary across modules; always use CFG.
- Do not trigger heavy recomputation from within knitting (no pipeline runs inside report setup).

---

## 12) Planned upgrade path (Phase 2, optional)
After Phase 1 stabilizes, you may replace scripts/build_cache.R + manual caching with targets.
Do not change the reporting layer; only swap the artifact-building backend.
If adopting targets, do NOT run tar_make() inside knitting; build pipeline first, then render.

