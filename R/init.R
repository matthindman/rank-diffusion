suppressPackageStartupMessages({
  library(here)
  library(tidyverse)
  library(arrow)
  library(lubridate)
  library(scales)
  library(broom)
  library(digest)
  library(jsonlite)
  library(withr)
})

source(here("R", "config.R"))
source(here("R", "cache.R"))
source(here("R", "utils.R"))
source(here("R", "bootstrap_increment_sampler.R"))
source(here("R", "jump_model.R"))
source(here("R", "data_prep.R"))
source(here("R", "metrics.R"))
source(here("R", "simulation.R"))
source(here("R", "plotting.R"))

if (!exists("params", inherits = FALSE)) {
  params <- tryCatch(knitr::opts_knit$get("params"), error = function(e) NULL)
}
if (is.null(params)) params <- list()
CFG <- config_from_params(params)
