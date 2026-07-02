# ---- Plotting: diagnostic visualizations using ggplot2 ----
# Translates Python plotting.py to R/ggplot2 for the rankdiffR package.

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

#' Plot core diagnostic comparison (empirical vs simulated)
#'
#' Creates a 2x2 panel of diagnostic plots: Variance Ratio, Rank ACF,
#' Cross-Sectional R-squared, and Top-k Persistence.  Each panel compares
#' empirical targets against Monte Carlo simulation means.
#'
#' @param bundle A \code{rankdiff_bundle} produced by
#'   \code{\link{build_data_bundle}}.
#' @param score A score list as returned by \code{\link{score_diagnostics}},
#'   containing \code{mc_stats}, \code{n_pass}, and \code{n_total}.
#' @param out_dir Character path to the output directory (created if needed).
#' @param prefix Character prefix for the output filename.
#' @return The file path of the saved PNG (invisibly).
#' @export
plot_core_diagnostics <- function(bundle, score, out_dir, prefix) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    warning("ggplot2 is required for plotting; skipping.")
    return(invisible(NULL))
  }

  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  emp <- bundle$empirical
  mc  <- score$mc_stats

  # ---- Helper to safely extract mc mean ----
  mc_mean <- function(key) {
    s <- mc[[key]]
    if (is.null(s)) NA_real_ else as.numeric(s$mean)
  }

  # ---- 1. Variance Ratio ----
  vr_lags <- sort(as.integer(names(emp$vr_emp)))
  vr_df <- data.frame(
    lag     = rep(vr_lags, 2),
    value   = c(
      vapply(vr_lags, function(k) as.numeric(emp$vr_emp[[as.character(k)]]), numeric(1)),
      vapply(vr_lags, function(k) mc_mean(paste0("vr", k)), numeric(1))
    ),
    source  = rep(c("Empirical", "Simulated"), each = length(vr_lags)),
    stringsAsFactors = FALSE
  )
  p_vr <- ggplot2::ggplot(vr_df, ggplot2::aes(x = .data$lag, y = .data$value,
                                                colour = .data$source,
                                                linetype = .data$source)) +
    ggplot2::geom_line(linewidth = 0.8) +
    ggplot2::geom_point(size = 2.5) +
    ggplot2::scale_colour_manual(values = c(Empirical = "black", Simulated = "red")) +
    ggplot2::scale_linetype_manual(values = c(Empirical = "solid", Simulated = "dashed")) +
    ggplot2::labs(title = "Variance Ratio", x = "Lag", y = "VR") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.title = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank())

  # ---- 2. Rank ACF ----
  racf_lags <- sort(as.integer(names(emp$racf_emp)))
  n_racf <- length(racf_lags)
  racf_df <- data.frame(
    x      = c(seq_len(n_racf) - 0.15, seq_len(n_racf) + 0.15),
    value  = c(
      vapply(racf_lags, function(k) as.numeric(emp$racf_emp[[as.character(k)]]), numeric(1)),
      vapply(racf_lags, function(k) mc_mean(paste0("racf", k)), numeric(1))
    ),
    source = rep(c("Empirical", "Simulated"), each = n_racf),
    label  = rep(as.character(racf_lags), 2),
    stringsAsFactors = FALSE
  )
  p_racf <- ggplot2::ggplot(racf_df, ggplot2::aes(x = .data$x, y = .data$value,
                                                    fill = .data$source)) +
    ggplot2::geom_col(width = 0.28, position = "identity") +
    ggplot2::scale_fill_manual(values = c(Empirical = "grey30", Simulated = "red")) +
    ggplot2::scale_x_continuous(breaks = seq_len(n_racf),
                                labels = as.character(racf_lags)) +
    ggplot2::labs(title = "Rank ACF", x = "Lag", y = "RACF") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.title = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank())

  # ---- 3. Cross-Sectional R-squared ----
  r2_horizons <- sort(as.integer(names(emp$xr2_emp)))
  r2_df <- data.frame(
    horizon = rep(r2_horizons, 2),
    value   = c(
      vapply(r2_horizons, function(k) as.numeric(emp$xr2_emp[[as.character(k)]]), numeric(1)),
      vapply(r2_horizons, function(k) mc_mean(paste0("xr2_", k)), numeric(1))
    ),
    source  = rep(c("Empirical", "Simulated"), each = length(r2_horizons)),
    stringsAsFactors = FALSE
  )
  p_r2 <- ggplot2::ggplot(r2_df, ggplot2::aes(x = .data$horizon, y = .data$value,
                                                colour = .data$source,
                                                linetype = .data$source)) +
    ggplot2::geom_line(linewidth = 0.8) +
    ggplot2::geom_point(size = 2.5) +
    ggplot2::scale_colour_manual(values = c(Empirical = "black", Simulated = "red")) +
    ggplot2::scale_linetype_manual(values = c(Empirical = "solid", Simulated = "dashed")) +
    ggplot2::labs(title = "Cross-Sectional R2", x = "Horizon", y = "R-squared") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.title = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank())

  # ---- 4. Persistence ----
  pers_horizons <- sort(as.integer(names(emp$pers_emp)))
  pers_df <- data.frame(
    horizon = rep(pers_horizons, 2),
    value   = c(
      vapply(pers_horizons, function(k) as.numeric(emp$pers_emp[[as.character(k)]]), numeric(1)),
      vapply(pers_horizons, function(k) mc_mean(paste0("pers", k)), numeric(1))
    ),
    source  = rep(c("Empirical", "Simulated"), each = length(pers_horizons)),
    stringsAsFactors = FALSE
  )
  top_k_label <- emp$top_k %||% ""
  p_pers <- ggplot2::ggplot(pers_df, ggplot2::aes(x = .data$horizon, y = .data$value,
                                                    colour = .data$source,
                                                    linetype = .data$source)) +
    ggplot2::geom_line(linewidth = 0.8) +
    ggplot2::geom_point(size = 2.5) +
    ggplot2::scale_colour_manual(values = c(Empirical = "black", Simulated = "red")) +
    ggplot2::scale_linetype_manual(values = c(Empirical = "solid", Simulated = "dashed")) +
    ggplot2::labs(title = paste0("Top-", top_k_label, " Persistence"),
                  x = "Horizon", y = "Count") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.title = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank())

  # ---- Combine with patchwork or manual arrangement ----
  # Use gridExtra if available, otherwise save individually
  title_text <- paste0(
    tools::toTitleCase(bundle$platform),
    " RankDiff | ", score$n_pass, "/", score$n_total
  )

  file_path <- file.path(out_dir, paste0(prefix, "_diagnostics.png"))

  if (requireNamespace("patchwork", quietly = TRUE)) {
    combined <- (p_vr + p_racf) / (p_r2 + p_pers) +
      patchwork::plot_annotation(title = title_text)
    ggplot2::ggsave(file_path, combined, width = 12, height = 9, dpi = 160)
  } else {
    # Fallback: use gridExtra
    grDevices::png(file_path, width = 12, height = 9, units = "in", res = 160)
    if (requireNamespace("gridExtra", quietly = TRUE)) {
      gridExtra::grid.arrange(p_vr, p_racf, p_r2, p_pers,
                              ncol = 2, top = title_text)
    } else {
      # Last resort: just save the VR plot
      print(p_vr + ggplot2::labs(subtitle = title_text))
    }
    grDevices::dev.off()
  }

  message("  Saved diagnostics plot: ", file_path)
  invisible(file_path)
}

#' Plot ablation study results
#'
#' Creates a two-panel figure: (1) a heatmap of pass/fail status for each
#' diagnostic at each ablation level, and (2) a horizontal bar chart of
#' overall scores.
#'
#' @param results A list of ablation results as returned by
#'   \code{\link{run_ablation}}.
#' @param out_dir Character path to the output directory.
#' @param prefix Character prefix for the output filename.
#' @return The file path of the saved PNG (invisibly).
#' @export
plot_ablation <- function(results, out_dir, prefix) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    warning("ggplot2 is required for plotting; skipping.")
    return(invisible(NULL))
  }

  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  n_lvls  <- length(results)
  n_diags <- length(DIAG_NAMES)

  # Build heatmap data frame
  hm_rows <- list()
  for (i in seq_len(n_lvls)) {
    for (j in seq_len(n_diags)) {
      hm_rows[[length(hm_rows) + 1L]] <- data.frame(
        level    = results[[i]]$level$short,
        diag     = DIAG_NAMES[j],
        pass     = isTRUE(results[[i]]$pass_fail[[DIAG_KEYS[j]]]),
        level_i  = i,
        diag_j   = j,
        stringsAsFactors = FALSE
      )
    }
  }
  hm_df <- do.call(rbind, hm_rows)
  hm_df$level <- factor(hm_df$level,
                         levels = rev(vapply(results, function(r) r$level$short, character(1))))
  hm_df$diag  <- factor(hm_df$diag, levels = DIAG_NAMES)
  hm_df$label <- ifelse(hm_df$pass, "Y", "N")

  p_hm <- ggplot2::ggplot(hm_df, ggplot2::aes(x = .data$diag, y = .data$level,
                                                fill = .data$pass)) +
    ggplot2::geom_tile(colour = "white", linewidth = 0.5) +
    ggplot2::geom_text(ggplot2::aes(label = .data$label),
                       colour = "white", fontface = "bold", size = 3) +
    ggplot2::scale_fill_manual(values = c("TRUE" = "#4caf50", "FALSE" = "#d32f2f"),
                               guide = "none") +
    ggplot2::labs(title = "Ablation: Diagnostic Pass/Fail",
                  x = "Diagnostic", y = "Model Level") +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, size = 8),
      axis.text.y = ggplot2::element_text(size = 9),
      panel.grid   = ggplot2::element_blank()
    )

  # Score bar chart
  score_df <- data.frame(
    level = factor(
      vapply(results, function(r) r$level$short, character(1)),
      levels = rev(vapply(results, function(r) r$level$short, character(1)))
    ),
    score = vapply(results, function(r) r$n_pass, integer(1)),
    stringsAsFactors = FALSE
  )
  max_score <- if (length(results) > 0L) results[[1]]$n_total else 15L

  score_df$colour <- ifelse(
    score_df$score == max_score, "#4caf50",
    ifelse(score_df$score >= max_score - 3L, "#ff9800", "#d32f2f")
  )

  p_bar <- ggplot2::ggplot(score_df, ggplot2::aes(x = .data$score, y = .data$level)) +
    ggplot2::geom_col(fill = score_df$colour, width = 0.7) +
    ggplot2::geom_text(ggplot2::aes(label = .data$score),
                       hjust = -0.3, size = 3.5) +
    ggplot2::geom_vline(xintercept = max_score, linetype = "dashed",
                        colour = "green", alpha = 0.5) +
    ggplot2::xlim(0, max_score + 1) +
    ggplot2::labs(title = "Score", x = paste0("Passing (/", max_score, ")"), y = NULL) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      axis.text.y  = ggplot2::element_blank(),
      panel.grid.minor = ggplot2::element_blank()
    )

  file_path <- file.path(out_dir, paste0(prefix, "_ablation.png"))

  if (requireNamespace("patchwork", quietly = TRUE)) {
    combined <- p_hm + p_bar + patchwork::plot_layout(widths = c(3, 1))
    ggplot2::ggsave(file_path, combined, width = 16, height = 7, dpi = 200)
  } else {
    grDevices::png(file_path, width = 16, height = 7, units = "in", res = 200)
    if (requireNamespace("gridExtra", quietly = TRUE)) {
      gridExtra::grid.arrange(p_hm, p_bar, ncol = 2, widths = c(3, 1))
    } else {
      print(p_hm)
    }
    grDevices::dev.off()
  }

  message("  Saved ablation plot: ", file_path)
  invisible(file_path)
}

#' Plot sensitivity analysis results
#'
#' Creates per-parameter bar charts showing the diagnostic score at each
#' perturbation level.
#'
#' @param results A nested list as returned by \code{\link{run_sensitivity}}.
#' @param deltas Numeric vector of perturbation fractions.
#' @param out_dir Character path to the output directory.
#' @param prefix Character prefix for the output filename.
#' @return The file path of the saved PNG (invisibly).
#' @export
plot_sensitivity <- function(results, deltas, out_dir, prefix) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    warning("ggplot2 is required for plotting; skipping.")
    return(invisible(NULL))
  }

  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  param_names <- names(results)
  n_params    <- length(param_names)

  # Build combined data frame
  rows <- list()
  for (pname in param_names) {
    for (d in deltas) {
      entry <- results[[pname]][[as.character(d)]]
      if (is.null(entry)) next
      base_val <- if (d == 0.0) entry$value else NA_real_
      rows[[length(rows) + 1L]] <- data.frame(
        param     = pname,
        delta     = d,
        delta_pct = sprintf("%+.0f%%", d * 100),
        score     = entry$n_pass,
        max_score = entry$n_total,
        stringsAsFactors = FALSE
      )
    }
  }
  plot_df <- do.call(rbind, rows)
  plot_df$delta_pct <- factor(plot_df$delta_pct,
                              levels = sprintf("%+.0f%%", deltas * 100))

  max_score <- if (nrow(plot_df) > 0L) plot_df$max_score[1] else 15L

  plot_df$colour <- ifelse(
    plot_df$score == max_score, "#4caf50",
    ifelse(plot_df$score >= max_score - 3L, "#ff9800", "#d32f2f")
  )

  # Build individual facet base values for subtitles
  base_vals <- vapply(param_names, function(pname) {
    entry <- results[[pname]][[as.character(0.0)]]
    if (!is.null(entry)) sprintf("base=%.4f", entry$value) else ""
  }, character(1))
  plot_df$facet_label <- paste0(plot_df$param, " (",
                                base_vals[plot_df$param], ")")
  plot_df$facet_label <- factor(plot_df$facet_label,
                                levels = unique(plot_df$facet_label))

  p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = .data$delta_pct,
                                              y = .data$score)) +
    ggplot2::geom_col(fill = plot_df$colour, width = 0.7) +
    ggplot2::geom_text(ggplot2::aes(label = .data$score),
                       vjust = -0.5, size = 3) +
    ggplot2::geom_hline(yintercept = max_score, linetype = "dashed",
                        colour = "green", alpha = 0.5) +
    ggplot2::ylim(0, max_score + 1) +
    ggplot2::facet_wrap(~ .data$facet_label, scales = "fixed",
                        ncol = min(3L, n_params)) +
    ggplot2::labs(
      title = "Parameter Sensitivity: Score vs Perturbation",
      x     = "Perturbation",
      y     = paste0("Score (/", max_score, ")")
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      strip.text       = ggplot2::element_text(size = 9),
      panel.grid.minor = ggplot2::element_blank()
    )

  ncols <- min(3L, n_params)
  nrows <- ceiling(n_params / ncols)

  file_path <- file.path(out_dir, paste0(prefix, "_sensitivity.png"))
  ggplot2::ggsave(file_path, p,
                  width  = 5.5 * ncols,
                  height = 5 * nrows,
                  dpi    = 200)

  message("  Saved sensitivity plot: ", file_path)
  invisible(file_path)
}
