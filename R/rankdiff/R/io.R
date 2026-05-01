# ---- I/O: saving and loading fit results ----
# Translates Python io.py: save_fit_result, load_fit_result, _to_jsonable.

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Recursively convert R objects to JSON-safe types
#'
#' Handles matrices, data frames, numeric vectors, and nested lists.
#' Arrays become lists, data frames become column-oriented lists,
#' and scalar numerics are unboxed.
#'
#' @param value Any R object.
#' @return A JSON-serialisable object (list, scalar, or character).
#' @keywords internal
.to_jsonable <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (is.data.frame(value)) {
    return(lapply(value, function(col) {
      if (is.numeric(col)) as.list(col) else as.list(as.character(col))
    }))
  }
  if (is.matrix(value)) {
    return(as.list(as.numeric(value)))
  }
  if (is.list(value) && !is.null(names(value))) {
    return(lapply(setNames(names(value), names(value)), function(nm) {
      .to_jsonable(value[[nm]])
    }))
  }
  if (is.list(value)) {
    return(lapply(value, .to_jsonable))
  }
  if (is.numeric(value) && length(value) > 1L) {
    return(as.list(as.numeric(value)))
  }
  if (is.integer(value) && length(value) > 1L) {
    return(as.list(as.integer(value)))
  }
  if (is.logical(value) && length(value) == 1L) {
    return(value)
  }
  if (is.numeric(value) && length(value) == 1L) {
    return(value)
  }
  if (is.character(value)) {
    return(value)
  }
  # Fallback
  as.character(value)
}

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

#' Save a fit result to disk
#'
#' Writes two files to the output directory:
#' \describe{
#'   \item{curves.csv}{Parameter curves table (z_knots, sigma_eta, phi,
#'     sigma_nu, kappa, t_df).}
#'   \item{fit_result.json}{Complete serialisation of config, data summary,
#'     initial parameters, estimated parameters, and diagnostics.}
#' }
#'
#' @param result A list with elements \code{config}, \code{data},
#'   \code{initial}, \code{params}, and \code{diagnostics}.
#' @param out_dir Character path to the output directory.  If \code{NULL},
#'   falls back to \code{result$config$output_dir}, then \code{"output/rankdiff"}.
#' @return The output directory path (invisibly).
#' @export
save_fit_result <- function(result, out_dir = NULL) {
  target <- out_dir %||% result$config$output_dir %||% file.path("output", "rankdiff")
  dir.create(target, showWarnings = FALSE, recursive = TRUE)

  params <- result$params

  # ---- curves.csv ----
  curves_df <- data.frame(
    z_knots   = as.numeric(params$z_knots),
    sigma_eta = as.numeric(params$sigma_eta_curve),
    phi       = as.numeric(params$phi_curve),
    sigma_nu  = as.numeric(params$sigma_nu_curve),
    kappa     = as.numeric(params$kappa_curve),
    t_df      = as.numeric(params$t_df_curve)
  )
  utils::write.csv(curves_df, file.path(target, "curves.csv"), row.names = FALSE)

  # ---- fit_result.json ----
  empirical <- result$data$empirical
  empirical_summary <- list(
    counts_by_period      = empirical$counts_by_period,
    top_k                 = empirical$top_k,
    vr_emp                = empirical$vr_emp,
    acf_emp               = empirical$acf_emp,
    racf_emp              = empirical$racf_emp,
    pers_emp              = empirical$pers_emp,
    xr2_emp               = empirical$xr2_emp,
    zipf_slope            = empirical$zipf_slope,
    emp_kurt              = empirical$emp_kurt,
    emp_mean_var          = empirical$emp_mean_var,
    emp_median_var        = empirical$emp_median_var,
    xsec_var_emp          = empirical$xsec_var_emp,
    window_turnover_n     = empirical$window_turnover_n,
    window_turnover_rate  = empirical$window_turnover_rate,
    window_turnover_count = empirical$window_turnover_count
  )

  # Build parameter payloads
  initial_payload <- .to_jsonable(as.list(result$initial))
  params_payload  <- .to_jsonable(as.list(result$params))

  # Config payload
  cfg <- result$config
  cfg_payload <- .to_jsonable(as.list(cfg))

  # Threshold
  threshold <- result$data$threshold
  threshold_payload <- list(
    threshold_by_period         = as.list(as.numeric(threshold$threshold_by_period)),
    max_missing_value_by_period = as.list(as.numeric(threshold$max_missing_value_by_period))
  )

  payload <- list(
    config = cfg_payload,
    data_summary = list(
      platform                    = result$data$platform,
      cadence                     = result$data$cadence,
      n_periods                   = result$data$n_periods,
      n_entities                  = result$data$n_entities,
      mean_n                      = result$data$mean_n,
      max_n                       = result$data$max_n,
      threshold_by_period         = threshold_payload$threshold_by_period,
      max_missing_value_by_period = threshold_payload$max_missing_value_by_period,
      empirical                   = .to_jsonable(empirical_summary)
    ),
    initial     = initial_payload,
    params      = params_payload,
    diagnostics = .to_jsonable(result$diagnostics)
  )

  json_str <- jsonlite::toJSON(payload, auto_unbox = TRUE, pretty = TRUE,
                                null = "null", na = "null", digits = NA)
  writeLines(json_str, file.path(target, "fit_result.json"))

  message("  Saved fit result to: ", target)
  invisible(target)
}

# ---------------------------------------------------------------------------
# Array field sets for reconstruction
# ---------------------------------------------------------------------------

.INITIAL_ARRAY_FIELDS <- c(
  "z_knots", "sigma_eta_anchor", "phi_anchor", "sigma_nu_anchor", "t_df_anchor"
)

.PARAMS_ARRAY_FIELDS <- c(
  "z_knots", "sigma_eta_curve", "phi_curve", "sigma_nu_curve",
  "kappa_curve", "t_df_curve", "t_df_curve_precal", "w0_sorted"
)

.THRESHOLD_ARRAY_FIELDS <- c(
  "threshold_by_period", "max_missing_value_by_period"
)

#' Reconstruct arrays from JSON-decoded lists
#'
#' Converts named fields from lists back to numeric vectors.
#'
#' @param d A named list (JSON-decoded).
#' @param array_fields Character vector of field names that should be numeric
#'   vectors.
#' @return The reconstructed list.
#' @keywords internal
.reconstruct_arrays <- function(d, array_fields) {
  out <- list()
  for (k in names(d)) {
    v <- d[[k]]
    if (k %in% array_fields && is.list(v)) {
      out[[k]] <- as.numeric(unlist(v))
    } else if (k %in% array_fields && is.null(v)) {
      out[[k]] <- NULL
    } else if (k == "threshold" && is.list(v)) {
      reconstructed <- .reconstruct_arrays(v, .THRESHOLD_ARRAY_FIELDS)
      out[[k]] <- new_threshold_model(
        threshold_by_period         = reconstructed$threshold_by_period,
        max_missing_value_by_period = reconstructed$max_missing_value_by_period
      )
    } else if (k == "metadata" && is.list(v)) {
      out[[k]] <- v
    } else {
      out[[k]] <- v
    }
  }
  out
}

#' Load a fit result from disk
#'
#' Reads \code{fit_result.json} from the specified path (directory or file) and
#' reconstructs the config, data summary, initial parameters, estimated
#' parameters, and diagnostics as R lists with appropriate classes.
#'
#' @param path Character path to either the directory containing
#'   \code{fit_result.json} or the JSON file itself.
#' @return A list of class \code{rankdiff_result} with elements \code{config},
#'   \code{data}, \code{initial}, \code{params}, and \code{diagnostics}.
#' @export
load_fit_result <- function(path) {
  root <- path
  if (dir.exists(root)) {
    json_path <- file.path(root, "fit_result.json")
  } else {
    json_path <- root
    root <- dirname(root)
  }

  if (!file.exists(json_path)) {
    stop("fit_result.json not found at: ", json_path)
  }

  payload <- jsonlite::fromJSON(json_path, simplifyVector = FALSE)

  # ---- Config ----
  cfg_args <- payload$config
  cfg <- do.call(create_config, cfg_args)

  # ---- Threshold ----
  threshold <- new_threshold_model(
    threshold_by_period         = as.numeric(unlist(payload$data_summary$threshold_by_period)),
    max_missing_value_by_period = as.numeric(unlist(payload$data_summary$max_missing_value_by_period))
  )

  # ---- Data (stub -- panel is not serialised) ----
  data <- list(
    panel        = data.frame(),
    platform     = payload$data_summary$platform,
    cadence      = payload$data_summary$cadence,
    dates        = character(0),
    n_periods    = as.integer(payload$data_summary$n_periods),
    n_entities   = as.integer(payload$data_summary$n_entities),
    mean_n       = as.numeric(payload$data_summary$mean_n),
    max_n        = as.integer(payload$data_summary$max_n),
    balanced_ids = character(0),
    tracked_ids  = character(0),
    tracked_entity_ids = character(0),
    threshold    = threshold,
    empirical    = payload$data_summary$empirical
  )
  class(data) <- "rankdiff_bundle"

  # ---- Initial params ----
  initial <- .reconstruct_arrays(payload$initial, .INITIAL_ARRAY_FIELDS)
  class(initial) <- "rankdiff_initial"

  # ---- Estimated params ----
  params <- .reconstruct_arrays(payload$params, .PARAMS_ARRAY_FIELDS)
  class(params) <- "rankdiff_params"

  # ---- Diagnostics ----
  diagnostics <- payload$diagnostics

  result <- list(
    config      = cfg,
    data        = data,
    initial     = initial,
    params      = params,
    diagnostics = diagnostics
  )
  class(result) <- "rankdiff_result"
  result
}
