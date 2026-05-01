args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript scripts/render_section.R analysis/02_cdc.Rmd [--use_cache=TRUE|FALSE] [--run_heavy=TRUE|FALSE] [--cache_version=...]" )
}

child <- args[1]
params <- list(child = child)

if (length(args) > 1) {
  for (arg in args[-1]) {
    if (!startsWith(arg, "--")) next
    kv <- strsplit(sub("^--", "", arg), "=", fixed = TRUE)[[1]]
    key <- kv[1]
    val <- if (length(kv) > 1) kv[2] else ""

    if (key %in% c("use_cache", "run_heavy")) {
      params[[key]] <- tolower(val) %in% c("true", "t", "1", "yes")
    } else if (key %in% c("cache_version", "seed", "K_cut_target", "K_tail_buffer")) {
      params[[key]] <- val
    }
  }
}

rmarkdown::render(
  input = here::here("report", "section_wrapper.Rmd"),
  params = params,
  output_dir = here::here("output", "report")
)
