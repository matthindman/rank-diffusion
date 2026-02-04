source(here::here("R", "init.R"))

rmarkdown::render(
  input = here::here("report", "section_wrapper.Rmd"),
  params = list(child = "analysis/01_data_qc.Rmd"),
  output_dir = here::here("output", "report")
)

rmarkdown::render(
  input = here::here("report", "master_report.Rmd"),
  params = list(mode = "dev", run_heavy = FALSE, use_cache = TRUE),
  output_dir = here::here("output", "report")
)
