#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

if (!requireNamespace("rmarkdown", quietly = TRUE)) {
  stop("Package 'rmarkdown' is required. Install it with install.packages('rmarkdown')")
}

files <- if (length(args) > 0) {
  args
} else {
  fs <- list.files(".", pattern = "^atlas_cdc_estimation.*\\.Rmd$", full.names = TRUE)
  if (length(fs) == 0) stop("No atlas_cdc_estimation*.Rmd files found in repo root")
  fs
}

status <- TRUE
for (f in files) {
  cat(sprintf("[render] Rendering %s...\n", f))
  ok <- tryCatch({
    rmarkdown::render(f, quiet = TRUE)
    TRUE
  }, error = function(e) {
    message(sprintf("[render] FAILED: %s", conditionMessage(e)))
    FALSE
  })
  status <- status && ok
}

if (!status) quit(status = 1L) else cat("[render] Completed successfully.\n")

