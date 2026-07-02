resolve_weekly_parquet_path <- function(primary = NULL, candidates = NULL) {
  if (!is.null(primary) && file.exists(primary)) return(primary)

  if (is.null(candidates)) {
    candidates <- c(
      primary,
      here::here("data", "raw", "fb_ranked_weekly.parquet"),
      here::here("fb_ranked_weekly.parquet"),
      here::here("Facebook", "fb_ranked_weekly.parquet")
    )
  }

  candidates <- candidates[!is.na(candidates)]
  hit <- candidates[file.exists(candidates)][1]

  if (is.na(hit) || is.null(hit)) {
    stop(
      "Weekly parquet not found. Tried: ",
      paste(candidates, collapse = ", ")
    )
  }

  hit
}

rank_bucket_simple <- function(r) {
  dplyr::case_when(
    r <= 25 ~ "large",
    r <= 250 ~ "midsize",
    TRUE ~ "small"
  )
}

moving_average_rank <- function(x, h) {
  stopifnot(h >= 0L)
  n <- length(x)
  if (h == 0L) return(x)
  cs <- c(0, cumsum(dplyr::if_else(is.na(x), 0, x)))
  out <- numeric(n)
  for (r in seq_len(n)) {
    lo <- max(1L, r - h)
    hi <- min(n, r + h)
    out[r] <- (cs[hi + 1L] - cs[lo]) / (hi - lo + 1L)
  }
  out
}

assign_bucket <- function(rank, bucket_def) {
  cut(
    rank,
    breaks = bucket_def$breaks,
    labels = bucket_def$labels,
    include.lowest = TRUE,
    right = TRUE
  )
}

make_rank_bin <- function(rank, K, n_bins = 10L) {
  dplyr::case_when(
    is.na(rank) ~ "out",
    rank > K ~ "out",
    TRUE ~ paste0("D", pmin(n_bins, pmax(1L, ceiling(n_bins * rank / K))))
  )
}

filter_bad_weeks <- function(endpoint_weekly, min_week_ranks_keep = 12000L, verbose = TRUE) {
  if (is.null(min_week_ranks_keep) || length(min_week_ranks_keep) == 0L) {
    min_week_ranks_keep <- 12000L
  }
  min_week_ranks_keep <- as.integer(min_week_ranks_keep)

  week_counts <- endpoint_weekly %>%
    dplyr::count(week, name = "n_ranks") %>%
    dplyr::arrange(week)

  if (is.na(min_week_ranks_keep) || min_week_ranks_keep <= 0L) {
    bad_weeks <- week_counts[0, , drop = FALSE]
    return(list(
      endpoint_weekly = endpoint_weekly,
      bad_weeks = bad_weeks,
      week_counts = week_counts
    ))
  }

  bad_weeks <- week_counts %>%
    dplyr::filter(n_ranks < min_week_ranks_keep)

  if (nrow(bad_weeks) > 0 && isTRUE(verbose)) {
    message(sprintf(
      "[weekly filter] dropping %d weeks with < %d ranks (min=%d)",
      nrow(bad_weeks),
      min_week_ranks_keep,
      min(bad_weeks$n_ranks, na.rm = TRUE)
    ))
  }

  if (nrow(bad_weeks) > 0) {
    endpoint_weekly <- endpoint_weekly %>%
      dplyr::filter(!week %in% bad_weeks$week)
  }

  list(
    endpoint_weekly = endpoint_weekly,
    bad_weeks = bad_weeks,
    week_counts = week_counts
  )
}
