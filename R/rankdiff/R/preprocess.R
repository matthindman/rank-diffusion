# ---- Preprocessing: threshold estimation, feature engineering, bundle ----
# Translates Python preprocess.py: estimate_threshold_model, compute_log_rank_coord,
# compute_local_slope, _infer_platform_name, _select_tracked_ids, build_data_bundle.

#' Estimate the threshold model for entry/exit
#'
#' Computes per-period threshold values based on the minimum observed metric
#' value within each period, or uses a fixed user-supplied threshold.
#'
#' @param panel A canonicalized panel tibble with \code{period_index} and
#'   \code{metric_value} columns.
#' @param cfg A \code{rankdiff_config}.
#' @return A \code{rankdiff_threshold} object (see \code{\link{new_threshold_model}}).
#' @keywords internal
estimate_threshold_model <- function(panel, cfg) {
  per_period <- panel %>%
    dplyr::group_by(.data$period_index) %>%
    dplyr::summarise(min_mv = min(.data$metric_value), .groups = "drop") %>%
    dplyr::arrange(.data$period_index)

  if (cfg$threshold_mode == "provided") {
    if (is.null(cfg$activity_threshold)) {
      stop("activity_threshold must be set when threshold_mode='provided'")
    }
    threshold_by_period <- rep(as.numeric(cfg$activity_threshold),
                               nrow(per_period))
  } else {
    threshold_by_period <- per_period$min_mv
  }

  max_missing_value <- threshold_by_period

  new_threshold_model(
    threshold_by_period          = threshold_by_period,
    max_missing_value_by_period  = max_missing_value
  )
}

#' Compute log-rank coordinate (z_rank)
#'
#' Normalizes rank to \code{(rank - 0.5) / n_t}, clips to
#' \code{[eps, 1]}, and returns the natural log.
#'
#' @param rank Integer vector of ranks.
#' @param n_t Numeric vector of period sizes (same length as \code{rank}).
#' @param eps Numeric clipping floor (default \code{1e-6}).
#' @return Numeric vector of log-rank coordinates.
#' @keywords internal
compute_log_rank_coord <- function(rank, n_t, eps = 1e-6) {
  normalized <- (as.numeric(rank) - 0.5) / as.numeric(n_t)
  log(pmax(pmin(normalized, 1.0), eps))
}

#' Compute local slope of the log-metric vs log-rank curve
#'
#' Within each period, computes the numerical gradient of
#' \code{log1p(metric_value)} with respect to \code{log(rank)} using finite
#' differences. Periods with fewer than 3 entities receive a slope of zero.
#'
#' @param panel A canonicalized panel tibble with \code{period_index},
#'   \code{rank}, and \code{metric_value} columns.
#' @return The input tibble augmented with a \code{local_slope} column.
#' @keywords internal
compute_local_slope <- function(panel) {
  log_rank   <- log(pmax(as.numeric(panel$rank), 1.0))
  log_metric <- log1p(pmax(as.numeric(panel$metric_value), 0.0))
  local_slope <- numeric(nrow(panel))

  period_indices <- split(seq_len(nrow(panel)), panel$period_index)

  for (idx in period_indices) {
    x <- log_rank[idx]
    y <- log_metric[idx]
    n <- length(x)

    if (n >= 3L) {
      slope <- .numeric_gradient(y, x)
      # Replace non-finite values with linear fallback
      if (any(!is.finite(slope))) {
        x_range <- x[n] - x[1L]
        fallback <- (y[n] - y[1L]) / max(x_range, 1e-8)
        slope[!is.finite(slope)] <- fallback
      }
    } else {
      slope <- rep(0.0, n)
    }
    local_slope[idx] <- slope
  }

  panel$local_slope <- local_slope
  panel
}

#' Compute numerical gradient (internal helper)
#'
#' Replicates \code{numpy.gradient(y, x)} for a 1-D array: second-order
#' accurate finite differences on a nonuniform grid in the interior and
#' first-order one-sided differences at the boundaries.
#'
#' @param y Numeric vector of function values.
#' @param x Numeric vector of coordinates (same length as \code{y}).
#' @return Numeric vector of estimated derivatives, same length as \code{y}.
#' @keywords internal
.numeric_gradient <- function(y, x) {

  n <- length(y)
  grad <- numeric(n)

  if (n == 1L) {
    return(0.0)
  }
  if (n == 2L) {
    d <- (y[2L] - y[1L]) / max(x[2L] - x[1L], 1e-12)
    return(c(d, d))
  }

  # Left boundary: forward difference
  grad[1L] <- (y[2L] - y[1L]) / max(x[2L] - x[1L], 1e-12)

  # Interior points: second-order accurate formula for irregular spacing.
  for (i in 2L:(n - 1L)) {
    hs <- x[i] - x[i - 1L]
    hd <- x[i + 1L] - x[i]
    denom <- max(hs * hd * (hs + hd), 1e-12)
    grad[i] <- (
      (-hd^2) * y[i - 1L] +
      (hd^2 - hs^2) * y[i] +
      hs^2 * y[i + 1L]
    ) / denom
  }

  # Right boundary: backward difference
  grad[n] <- (y[n] - y[n - 1L]) / max(x[n] - x[n - 1L], 1e-12)

  grad
}

#' Infer platform name from the data file path
#'
#' If \code{cfg$platform} is not \code{"auto"}, returns it directly.
#' Otherwise inspects the file stem for known platform substrings.
#'
#' @param cfg A \code{rankdiff_config}.
#' @return A character string identifying the platform.
#' @keywords internal
.infer_platform_name <- function(cfg) {
  if (cfg$platform != "auto") {
    return(cfg$platform)
  }

  stem <- tolower(tools::file_path_sans_ext(basename(cfg$data_path)))

  if (grepl("instagram", stem) || grepl("^ig_", stem)) {
    return("instagram")
  }
  if (grepl("facebook", stem) || grepl("^fb_", stem)) {
    return("facebook")
  }
  stem
}

#' Select a random subset of entity IDs to track
#'
#' If fewer than \code{track_entity_count} balanced IDs are available, all are
#' returned. Otherwise a reproducible random sample is drawn.
#'
#' @param panel Not used directly (kept for interface parity).
#' @param balanced_ids Character vector of entity IDs that meet presence
#'   requirements.
#' @param track_entity_count Integer maximum number of tracked entities.
#' @param seed Integer RNG seed.
#' @return A character vector of tracked entity IDs.
#' @keywords internal
.select_tracked_ids <- function(panel, balanced_ids, track_entity_count, seed) {
  if (length(balanced_ids) == 0L) {
    return(character(0L))
  }
  if (length(balanced_ids) <= track_entity_count) {
    return(balanced_ids)
  }
  .with_local_seed(seed, sample(balanced_ids, size = track_entity_count, replace = FALSE))
}

#' Build the full data bundle from a configuration
#'
#' Orchestrates the complete preprocessing pipeline: loading, canonicalization,
#' cadence detection, period indexing, optional rank filtering, local slope
#' computation, balanced-panel identification, threshold estimation, and
#' empirical target computation. Returns a list of class
#' \code{rankdiff_bundle} containing all data and metadata needed for
#' downstream fitting.
#'
#' @param cfg A \code{rankdiff_config} created by \code{\link{create_config}}.
#' @return A list of class \code{rankdiff_bundle} with elements:
#'   \describe{
#'     \item{panel}{Preprocessed tibble with canonical columns plus
#'       \code{period_start}, \code{period_index}, \code{local_slope}, and
#'       \code{z_rank}.}
#'     \item{platform}{Character string identifying the platform.}
#'     \item{cadence}{\code{"daily"} or \code{"weekly"}.}
#'     \item{dates}{Date vector of unique period start dates.}
#'     \item{n_periods}{Integer number of unique periods.}
#'     \item{n_entities}{Integer number of unique entities.}
#'     \item{mean_n}{Mean entities per period.}
#'     \item{max_n}{Maximum entities in any period.}
#'     \item{balanced_ids}{Character vector of entity IDs meeting the
#'       minimum-presence requirement.}
#'     \item{tracked_ids}{Character vector of sampled entity IDs for
#'       diagnostics.}
#'     \item{threshold}{A \code{rankdiff_threshold} object.}
#'     \item{empirical}{Empirical targets list from
#'       \code{compute_empirical_targets}.}
#'   }
#' @export
build_data_bundle <- function(cfg) {
  raw   <- load_panel(cfg)
  panel <- canonicalize_panel(raw, cfg)

  cadence <- infer_cadence(panel$timestamp, cfg$cadence)
  panel   <- add_period_index(panel, cadence)

  # --- Optional rank filter ---
  if (!is.null(cfg$max_rank_filter)) {
    panel <- panel %>%
      dplyr::filter(.data$rank <= cfg$max_rank_filter)
  }

  # --- Local slope ---
  panel <- compute_local_slope(panel)

  # --- Per-period entity counts ---
  counts <- panel %>%
    dplyr::group_by(.data$period_index) %>%
    dplyr::summarise(n_ent = dplyr::n_distinct(.data$entity_id),
                     .groups = "drop") %>%
    dplyr::arrange(.data$period_index)

  dates <- panel %>%
    dplyr::group_by(.data$period_index) %>%
    dplyr::summarise(period_start = dplyr::first(.data$period_start),
                     .groups = "drop") %>%
    dplyr::arrange(.data$period_index) %>%
    dplyr::pull(.data$period_start)

  n_periods  <- nrow(counts)
  n_entities <- dplyr::n_distinct(panel$entity_id)
  mean_n     <- mean(counts$n_ent)
  max_n      <- max(counts$n_ent)

  # --- Balanced IDs (entities present in >= min_presence_frac of periods) ---
  ep_counts <- panel %>%
    dplyr::group_by(.data$entity_id) %>%
    dplyr::summarise(n_per = dplyr::n_distinct(.data$period_index),
                     .groups = "drop")

  min_periods <- max(1L, as.integer(ceiling(cfg$min_presence_frac * n_periods)))
  balanced_ids <- ep_counts %>%
    dplyr::filter(.data$n_per >= min_periods) %>%
    dplyr::pull(.data$entity_id) %>%
    as.character()

  # --- Tracked IDs ---
  track_count <- min(cfg$track_entity_count,
                     max(cfg$max_dense_entities, cfg$track_entity_count))
  tracked_ids <- .select_tracked_ids(panel, balanced_ids, track_count,
                                     cfg$random_seed)

  # --- Log-rank coordinate (z_rank) ---
  n_t <- panel %>%
    dplyr::group_by(.data$period_index) %>%
    dplyr::mutate(.n_t = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::pull(.data$.n_t)

  panel$z_rank <- compute_log_rank_coord(panel$rank, n_t, eps = cfg$z_rank_clip)

  # --- Threshold model ---
  threshold <- estimate_threshold_model(panel, cfg)

  # --- Platform ---
  platform <- .infer_platform_name(cfg)

  # --- Empirical targets (defined in diagnostics.R) ---
  empirical <- compute_empirical_targets(panel, balanced_ids, tracked_ids,
                                         threshold, cfg)

  structure(
    list(
      panel        = tibble::as_tibble(panel),
      platform     = platform,
      cadence      = cadence,
      dates        = dates,
      n_periods    = as.integer(n_periods),
      n_entities   = as.integer(n_entities),
      mean_n       = mean_n,
      max_n        = as.integer(max_n),
      balanced_ids = balanced_ids,
      tracked_ids  = tracked_ids,
      tracked_entity_ids = tracked_ids,
      threshold    = threshold,
      empirical    = empirical
    ),
    class = "rankdiff_bundle"
  )
}
