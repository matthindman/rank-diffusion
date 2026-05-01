# Rank Diffusion Modular Report

This repo is modularized using R Markdown child documents with explicit cached artifacts.

## Quick Start

1. Restore packages:

```r
renv::restore()
```

2. Build or refresh caches (optional but recommended):

```sh
Rscript scripts/build_cache.R
```

3. Render the full report (PDF):

```r
rmarkdown::render("report/master_report.Rmd")
```

## Fast Section Rendering

Render a single section (HTML) for quick iteration:

```sh
Rscript scripts/render_section.R analysis/02_cdc.Rmd --use_cache=TRUE
```

## Project Layout

- `report/master_report.Rmd` stitches child docs into a single PDF.
- `analysis/*.Rmd` are child sections (no YAML, no `library()` calls).
- `R/` contains reusable functions, config, and caching helpers.
- `cache/` stores `.rds` artifacts + metadata in `cache/_meta/` (not committed).
- `output/` stores rendered reports and figures (not committed).

## Reproducibility

- Parameters live in the master report YAML; `R/config.R` validates and normalizes.
- Caches are invalidated via fingerprints based on inputs, code mtimes, and `cache_version`.
- A provenance footer with timestamp, git hash, and `sessionInfo()` is appended to the master report.
