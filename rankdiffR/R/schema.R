# ---- Schema: data loading and canonicalization ----
# Translates Python schema.py: load_panel, canonicalize_panel, infer_cadence,
# add_period_index.

#' Load a panel dataset from a parquet file
#'
#' Reads the parquet file specified in the configuration and returns it as a
#' tibble.
#'
#' @param cfg A \code{rankdiff_config} created by \code{\link{create_config}}.
#' @return A tibble containing the raw panel data.
#' @export
load_panel <- function(cfg) {
  data_path <- cfg$data_path
  if (!file.exists(data_path)) {
    stop("Data file not found: ", data_path)
  }
  ext <- tolower(tools::file_ext(data_path))
  if (ext != "parquet") {
    stop("Only parquet inputs are currently supported, got: .", ext)
  }
  arrow::read_parquet(data_path)
}

#' Canonicalize a panel to a standard schema
#'
#' Renames columns to canonical names (\code{entity_id}, \code{timestamp},
#' \code{metric_value}), coerces types, drops invalid rows, handles duplicates,
#' applies date filters, and computes dense ranks within each timestamp.
#'
#' @param df A data frame (typically from \code{\link{load_panel}}).
#' @param cfg A \code{rankdiff_config}.
#' @return A tibble with canonical columns: \code{entity_id} (character),
#'   \code{timestamp} (Date), \code{metric_value} (numeric), and \code{rank}
#'   (integer, descending by metric_value within each timestamp).
#' @export
canonicalize_panel <- function(df, cfg) {
  # --- Validate required columns exist ---
  required <- c(cfg$id_col, cfg$timestamp_col, cfg$metric_col)
  missing <- setdiff(required, colnames(df))
  if (length(missing) > 0L) {
    stop("Missing required columns: ", paste(missing, collapse = ", "))
  }

  # --- Build rename map ---
  rename_map <- c(
    "entity_id"    = cfg$id_col,
    "timestamp"    = cfg$timestamp_col,
    "metric_value" = cfg$metric_col
  )
  if (!is.null(cfg$rank_col) && cfg$rank_col %in% colnames(df)) {
    rename_map <- c(rename_map, "rank" = cfg$rank_col)
  }

  panel <- df %>%
    dplyr::rename(dplyr::all_of(rename_map)) %>%
    dplyr::mutate(
      entity_id    = as.character(.data$entity_id),
      timestamp    = as.Date(.data$timestamp),
      metric_value = suppressWarnings(as.numeric(.data$metric_value))
    ) %>%
    dplyr::filter(
      !is.na(.data$entity_id),
      !is.na(.data$timestamp),
      !is.na(.data$metric_value),
      .data$metric_value >= 0
    )

  # --- Date filters ---
  if (!is.null(cfg$fit_start)) {
    panel <- panel %>%
      dplyr::filter(.data$timestamp >= as.Date(cfg$fit_start))
  }
  if (!is.null(cfg$fit_end)) {
    panel <- panel %>%
      dplyr::filter(.data$timestamp <= as.Date(cfg$fit_end))
  }

  if (nrow(panel) == 0L) {
    stop("No rows left after canonicalization and date filtering.")
  }

  # --- Handle duplicates ---
  dup_df <- panel %>%
    dplyr::group_by(.data$timestamp, .data$entity_id) %>%
    dplyr::mutate(.n_dup = dplyr::n()) %>%
    dplyr::ungroup()

  dup_rows <- sum(dup_df$.n_dup > 1L)

  if (dup_rows > 0L) {
    dup_rate <- dup_rows / max(nrow(panel), 1L)
    if (dup_rate > cfg$max_duplicate_entity_period_rate) {
      stop("Duplicate entity-period rows exceed the allowed rate")
    }
    # Keep max metric_value per (timestamp, entity_id)
    panel <- panel %>%
      dplyr::group_by(.data$timestamp, .data$entity_id) %>%
      dplyr::summarise(metric_value = max(.data$metric_value), .groups = "drop") %>%
      dplyr::arrange(.data$timestamp, dplyr::desc(.data$metric_value), .data$entity_id)
  } else {
    panel <- panel %>%
      dplyr::arrange(.data$timestamp, dplyr::desc(.data$metric_value), .data$entity_id)
  }

  # --- Compute rank within each timestamp ---
  panel <- panel %>%
    dplyr::group_by(.data$timestamp) %>%
    dplyr::mutate(
      rank = as.integer(dplyr::row_number(dplyr::desc(.data$metric_value)))
    ) %>%
    dplyr::ungroup()

  tibble::as_tibble(panel)
}

#' Infer cadence from timestamps
#'
#' If \code{requested} is \code{"auto"}, examines the median gap between
#' consecutive sorted unique timestamps to decide between \code{"daily"} and
#' \code{"weekly"}.
#'
#' @param ts A vector of Date or POSIXct timestamps.
#' @param requested One of \code{"auto"}, \code{"daily"}, \code{"weekly"}.
#' @return A character string: \code{"daily"} or \code{"weekly"}.
#' @export
infer_cadence <- function(ts, requested) {
  if (requested != "auto") {
    return(requested)
  }
  ordered <- sort(unique(as.Date(ts[!is.na(ts)])))
  if (length(ordered) < 3L) {
    return("weekly")
  }
  day_diffs <- as.numeric(diff(ordered), units = "days")
  median_gap <- median(day_diffs)
  if (median_gap <= 2) {
    return("daily")
  }
  "weekly"
}

#' Add period index columns to a panel
#'
#' Computes \code{period_start} (the floored date for each row's cadence) and
#' \code{period_index} (a zero-based integer index over sorted unique periods).
#'
#' @param df A tibble with a \code{timestamp} column (Date).
#' @param cadence One of \code{"daily"} or \code{"weekly"}.
#' @return The input tibble augmented with \code{period_start} (Date) and
#'   \code{period_index} (integer) columns.
#' @export
add_period_index <- function(df, cadence) {
  panel <- df

  if (cadence == "daily") {
    panel$period_start <- as.Date(panel$timestamp)
  } else if (cadence == "weekly") {
    # Floor to start of ISO week (Monday), matching pandas W convention
    panel$period_start <- as.Date(
      cut.Date(panel$timestamp, breaks = "week")
    )
  } else {
    stop("Unsupported cadence: ", cadence)
  }

  unique_periods <- sort(unique(panel$period_start))
  period_map <- setNames(seq_along(unique_periods) - 1L, as.character(unique_periods))
  panel$period_index <- as.integer(period_map[as.character(panel$period_start)])

  tibble::as_tibble(panel)
}
