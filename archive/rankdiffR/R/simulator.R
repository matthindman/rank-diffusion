# ---- Simulation kernel ----
# Translates Python simulator.py: simulate_one, simulate_many, and all
# supporting helpers.  This is the core Monte Carlo engine for the
# permanent-transitory rank diffusion model.

# ---- Internal helpers ----

#' Sample from a Student-t distribution with element-wise degrees of freedom
#'
#' Generates t-distributed random variates using the ratio-of-normals
#' representation: Z / sqrt(chi2(df) / df).  Degrees of freedom are clipped
#' to a minimum of 1.
#'
#' @param df_vec Numeric vector of degrees of freedom (one per draw).
#' @return Numeric vector of t-distributed samples (same length as
#'   \code{df_vec}).
#' @keywords internal
.sample_student_t_vector <- function(df_vec) {
  n <- length(df_vec)
  z <- rnorm(n)
  df_safe <- pmax(df_vec, 1.0)
  chi <- rchisq(n, df = df_safe)
  z / sqrt(chi / df_safe)
}

#' Piecewise-linear interpolation with constant extrapolation
#'
#' Wraps \code{\link[stats]{approx}} with \code{rule = 2} (constant
#' extrapolation beyond the knot range), replicating
#' \code{numpy.interp(z, z_knots, values)}.
#'
#' @param z Numeric vector of query points.
#' @param z_knots Numeric vector of knot locations (sorted ascending).
#' @param values Numeric vector of knot values (same length as
#'   \code{z_knots}).
#' @return Numeric vector of interpolated values (same length as \code{z}).
#' @keywords internal
.interpolate_curve <- function(z, z_knots, values) {
  approx(z_knots, values, xout = z, rule = 2)$y
}

#' Extend initial state vector to size n_full
#'
#' If \code{w0_sorted} is shorter than \code{n_full}, extends it by
#' log-linear extrapolation of the tail (or constant padding when the input
#' is very short).
#'
#' @param w0_sorted Numeric vector of sorted initial log-metric values
#'   (descending).
#' @param n_full Integer target length.
#' @return Numeric vector of length \code{n_full}.
#' @keywords internal
.extend_initial_state <- function(w0_sorted, n_full) {
  n0 <- length(w0_sorted)
  if (n0 >= n_full) {
    return(as.double(w0_sorted[seq_len(n_full)]))
  }

  if (n0 < 20L) {
    tail_val <- if (n0 > 0L) w0_sorted[n0] else 0.0
    tail <- rep(tail_val, n_full - n0)
    return(as.double(c(w0_sorted, tail)))
  }

  tail_n <- min(2000L, n0)
  tail_ranks <- seq.int(n0 - tail_n + 1L, n0)
  fit <- lm(w0_sorted[seq.int(n0 - tail_n + 1L, n0)] ~ log(tail_ranks))
  intercept <- coef(fit)[1L]
  slope <- coef(fit)[2L]
  ext_ranks <- seq.int(n0 + 1L, n_full)
  ext_vals <- intercept + slope * log(ext_ranks)
  as.double(c(w0_sorted, ext_vals))
}

#' Select tracked entity indices
#'
#' If the simulation size is small enough, tracks all entities; otherwise
#' draws a reproducible random sample.
#'
#' @param n_full Integer simulation size.
#' @param track_count Integer maximum number of tracked positions.
#' @param top_k Integer number of top entities (unused but kept for API
#'   parity).
#' @param seed Integer RNG seed.
#' @return Integer vector of 1-based tracked indices.
#' @keywords internal
.tracked_indices <- function(n_full, track_count, top_k, seed) {
  if (n_full <= track_count) {
    return(seq_len(n_full))
  }
  .with_local_seed(seed, sort(sample.int(n_full, size = track_count, replace = FALSE)))
}

#' Resolve simulation size
#'
#' In \code{"full"} universe mode the simulation runs over \code{n_global}
#' entities; in \code{"topk_buffered"} mode it is capped at
#' \code{top_k_focus + buffer_k}.
#'
#' @param params Estimated parameters list.
#' @param cfg A \code{rankdiff_config}.
#' @param n_global Integer full universe size.
#' @return Integer simulation size.
#' @keywords internal
.resolve_simulation_size <- function(params, cfg, n_global) {

  if (cfg$universe_mode != "topk_buffered") {
    return(n_global)
  }
  if (is.null(cfg$top_k_focus)) {
    stop("top_k_focus must be set when universe_mode='topk_buffered'.")
  }
  if (cfg$buffer_k <= 0L) {
    stop("buffer_k must be positive when universe_mode='topk_buffered'.")
  }
  if (cfg$top_k_focus < params$top_k) {
    stop(sprintf(
      "top_k_focus (%d) must be at least as large as diagnostic top_k (%d).",
      cfg$top_k_focus, params$top_k
    ))
  }
  as.integer(min(n_global, cfg$top_k_focus + cfg$buffer_k))
}

#' Prepare boundary refresh values for topk_buffered mode
#'
#' Computes the pool of boundary values and the per-period replacement count
#' used to refresh the bottom of the simulation buffer.
#'
#' @param params Estimated parameters list.
#' @param cfg A \code{rankdiff_config}.
#' @param w0_full Numeric vector of extended initial state.
#' @param n_sim Integer simulation size.
#' @param n_global Integer full universe size.
#' @return A list with elements \code{boundary_values} (numeric vector),
#'   \code{boundary_scale} (numeric scalar), and \code{replace_n} (integer).
#' @keywords internal
.prepare_boundary_refresh <- function(params, cfg, w0_full, n_sim, n_global) {
  if (cfg$universe_mode != "topk_buffered" || n_sim >= n_global) {
    return(list(
      boundary_values = numeric(0),
      boundary_scale  = 0.0,
      replace_n       = 0L
    ))
  }

  boundary_hi <- min(n_global, n_sim + max(cfg$buffer_k, 1L))
  if (n_sim < boundary_hi) {
    boundary_values <- as.double(w0_full[seq.int(n_sim + 1L, boundary_hi)])
  } else {
    boundary_values <- numeric(0)
  }
  if (length(boundary_values) == 0L) {
    fallback_idx <- min(max(n_sim, 1L), length(w0_full))
    boundary_values <- w0_full[fallback_idx]
  }

  boundary_scale <- sd(boundary_values)
  if (is.na(boundary_scale)) boundary_scale <- 0.0

  turnover_rate <- params$metadata[["window_turnover_rate"]]
  if (is.null(turnover_rate) || !is.finite(turnover_rate)) {
    replace_n <- 0L
  } else {
    replace_n <- as.integer(ceiling(as.double(turnover_rate) * n_sim))
  }
  replace_n <- as.integer(min(max(replace_n, 0L), cfg$buffer_k))

  list(
    boundary_values = boundary_values,
    boundary_scale  = boundary_scale,
    replace_n       = replace_n
  )
}

#' Split exiting entities into burst and normal entrants
#'
#' @param n_exits Integer number of exiting entities.
#' @param burst_frac Probability an entrant is a burst entrant.
#' @return Integer vector with \code{burst} and \code{normal} counts.
#' @keywords internal
.split_entry_counts <- function(n_exits, burst_frac) {
  burst_prob <- min(max(as.double(burst_frac), 0.0), 1.0)
  n_burst <- as.integer(rbinom(1L, as.integer(n_exits), burst_prob))
  c(burst = n_burst, normal = as.integer(n_exits) - n_burst)
}

#' Resolve effective simulation parameters given feature flags
#'
#' Modifies parameters for ablation studies by disabling or overriding
#' specific model components based on the feature flag settings.
#'
#' @param params Estimated parameters list.
#' @param cfg A \code{rankdiff_config}.
#' @param features A \code{rankdiff_features} object, or NULL for defaults.
#' @return A list with elements \code{params} (modified parameter list),
#'   \code{use_exit} (logical), \code{use_obs} (logical), \code{use_heavy}
#'   (logical).
#' @keywords internal
.resolve_effective_params <- function(params, cfg, features) {
  if (is.null(features)) {
    return(list(
      params   = params,
      use_exit = isTRUE(cfg$exit_enabled),
      use_obs  = isTRUE(cfg$use_obs_noise),
      use_heavy = TRUE
    ))
  }

  p <- params
  use_exit  <- isTRUE(features$exit_entry) && isTRUE(cfg$exit_enabled)
  use_obs   <- isTRUE(features$obs_noise) && isTRUE(cfg$use_obs_noise)
  use_heavy <- isTRUE(features$heavy_tails)

  # Burn-in
  burnin <- if (isTRUE(features$burn_in)) p$burnin_periods else 0L


  # Kappa features
  if (!isTRUE(features$kappa)) {
    kappa_base_raw  <- 0.0
    kappa_stab_factor <- p$kappa_stab_factor
    alpha_kappa     <- p$alpha_kappa
  } else if (!isTRUE(features$rank_dep_kappa)) {
    kappa_base_raw <- p$kappa_base_raw * p$kappa_stab_factor *
      mean(exp(p$alpha_kappa * p$z_knots))
    kappa_stab_factor <- 1.0
    alpha_kappa <- 0.0
  } else if (!isTRUE(features$kappa_stab)) {
    kappa_base_raw  <- p$kappa_base_raw
    kappa_stab_factor <- 1.0
    alpha_kappa     <- p$alpha_kappa
  } else {
    kappa_base_raw  <- p$kappa_base_raw
    kappa_stab_factor <- p$kappa_stab_factor
    alpha_kappa     <- p$alpha_kappa
  }

  # ARCH
  alpha_arch <- if (isTRUE(features$arch)) p$alpha_arch else 0.0

  # Heavy tails / t-df curve
  if (!isTRUE(features$heavy_tails)) {
    t_df_curve <- rep(200.0, length(p$t_df_curve))
  } else if (!isTRUE(features$calibrated_tdf) && !is.null(p$t_df_curve_precal)) {
    t_df_curve <- as.double(p$t_df_curve_precal)
  } else {
    t_df_curve <- p$t_df_curve
  }

  p$burnin_periods   <- as.integer(burnin)
  p$kappa_base_raw   <- kappa_base_raw
  p$kappa_stab_factor <- kappa_stab_factor
  p$alpha_kappa      <- alpha_kappa
  p$alpha_arch       <- alpha_arch
  p$t_df_curve       <- t_df_curve

  list(
    params   = p,
    use_exit = use_exit,
    use_obs  = use_obs,
    use_heavy = use_heavy
  )
}

# ---- Public simulation functions ----

#' Run a single Monte Carlo simulation
#'
#' Simulates one realization of the permanent-transitory rank diffusion
#' process.  The simulation maintains permanent (\code{tau}) and transitory
#' (\code{c_state}) components for each entity, with rank-dependent
#' volatility, mean reversion, ARCH effects, heavy tails, observation
#' noise, and stochastic entry/exit.
#'
#' @param seed Integer RNG seed for this realization.
#' @param params Estimated parameters list (class \code{rankdiff_params} or
#'   plain list).  Must contain at minimum: \code{n_full}, \code{w0_sorted},
#'   \code{burnin_periods}, \code{z_knots}, \code{sigma_eta_curve},
#'   \code{phi_curve}, \code{sigma_nu_curve}, \code{t_df_curve},
#'   \code{kappa_base_raw}, \code{kappa_stab_factor}, \code{alpha_kappa},
#'   \code{sigma_het}, \code{sigma_obs}, \code{alpha_arch}, \code{jump_prob},
#'   \code{jump_scale}, \code{top_k}, \code{threshold}, and entry/exit
#'   parameters (\code{exit_p_base}, \code{exit_alpha},
#'   \code{exit_transient_rate}, \code{entry_burst_frac}, \code{t_df_global}).
#' @param n_periods Integer number of recording periods.
#' @param cfg A \code{rankdiff_config}.
#' @param features Optional \code{rankdiff_features} for ablation control
#'   (NULL uses all features).
#' @return A list with elements:
#'   \describe{
#'     \item{tracked_ids}{Integer vector of entity IDs at tracked positions.}
#'     \item{tracked_values}{Matrix (t_record x n_tracked) of observed values.}
#'     \item{tracked_ranks}{Integer matrix (t_record x n_tracked) of observed
#'       ranks.}
#'     \item{top_ids}{Integer matrix (t_record x top_k) of top entity IDs.}
#'     \item{observed_counts}{Integer vector of per-period observed counts.}
#'     \item{xsec_var}{Numeric vector of per-period cross-sectional variance
#'       of tau.}
#'     \item{period0_sorted_values}{Numeric vector of sorted observed values
#'       at period 0.}
#'     \item{diagnostics}{Diagnostics list from
#'       \code{compute_sim_diagnostics}.}
#'   }
#' @export
simulate_one <- function(seed, params, n_periods, cfg, features = NULL) {

  # ---- Resolve feature flags ----
  resolved <- .resolve_effective_params(params, cfg, features)
  params    <- resolved$params
  use_exit  <- resolved$use_exit
  use_obs   <- resolved$use_obs
  use_heavy <- resolved$use_heavy

  had_seed <- exists(".Random.seed", envir = globalenv(), inherits = FALSE)
  if (had_seed) {
    old_seed <- get(".Random.seed", envir = globalenv(), inherits = FALSE)
  }
  on.exit({
    if (had_seed) {
      assign(".Random.seed", old_seed, envir = globalenv())
    } else if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
      rm(".Random.seed", envir = globalenv())
    }
  }, add = TRUE)
  set.seed(as.integer(seed)[1L])

  # ---- Dimensions ----
  n_global <- as.integer(params$n_full)
  n_sim    <- .resolve_simulation_size(params, cfg, n_global)
  t_record <- as.integer(cfg$simulate_periods %||% n_periods)
  t_record <- min(t_record, n_periods)
  t_total  <- as.integer(params$burnin_periods) + t_record

  # ---- Initial state ----
  w0_full <- .extend_initial_state(params$w0_sorted, n_global)
  tau     <- as.double(w0_full[seq_len(n_sim)])
  c_state <- rep(0.0, n_sim)
  last_z_sq <- rep(1.0, n_sim)
  entity_ids <- seq_len(n_sim)
  next_entity_id <- n_sim + 1L
  mean_reversion_target <- mean(w0_full)

  # ---- Heterogeneity multipliers ----
  het_lo <- exp(-3.0 * params$sigma_het)
  het_hi <- exp(3.0 * params$sigma_het)
  het_multiplier <- pmin(pmax(
    exp(rnorm(n_sim, 0.0, params$sigma_het)),
    het_lo
  ), het_hi)

  # ---- Entity type (0 = incumbent, 1 = entrant) ----
  ep_type <- integer(n_sim)

  # ---- Tracked entity bookkeeping ----
  tracked <- .tracked_indices(
    n_sim,
    min(cfg$track_entity_count, n_sim),
    params$top_k,
    seed
  )
  n_tracked <- length(tracked)
  tracked_mask <- rep(FALSE, n_sim)
  tracked_mask[tracked] <- TRUE

  tracked_values <- matrix(NA_real_, nrow = t_record, ncol = n_tracked)
  tracked_ranks  <- matrix(0L, nrow = t_record, ncol = n_tracked)
  top_ids        <- matrix(-1L, nrow = t_record, ncol = params$top_k)
  observed_counts <- integer(t_record)
  xsec_var       <- numeric(t_record)
  period0_sorted_values <- numeric(0)

  tracked_initial_ids <- NULL
  tracked_alive <- rep(TRUE, n_tracked)

  # ---- Thresholds ----
  thresholds <- params$threshold$threshold_by_period[seq_len(t_record)]
  top_store_n <- max(1000L, params$top_k * 50L)

  # ---- Boundary refresh (topk_buffered mode) ----
  bnd <- .prepare_boundary_refresh(params, cfg, w0_full, n_sim, n_global)
  boundary_values <- bnd$boundary_values
  boundary_scale  <- bnd$boundary_scale
  replace_n       <- bnd$replace_n

  # ---- Exit/entry parameters ----
  exit_p_base <- params$exit_p_base %||% 0.0
  exit_alpha  <- params$exit_alpha %||% 0.3
  exit_trans  <- params$exit_transient_rate %||% 0.07
  burst_frac  <- params$entry_burst_frac %||% 0.008

  # ---- Main simulation loop ----
  for (t_abs in seq_len(t_total)) {
    # -- Latent values and ranking --
    x_true <- tau + c_state
    latent_rank <- rank(-x_true, ties.method = "first")

    # -- Rank-dependent parameters --
    z_rank <- log(pmax(pmin((latent_rank - 0.5) / n_global, 1.0), cfg$z_rank_clip))
    sigma_eta <- .interpolate_curve(z_rank, params$z_knots, params$sigma_eta_curve) * het_multiplier
    phi       <- .interpolate_curve(z_rank, params$z_knots, params$phi_curve)
    sigma_nu  <- .interpolate_curve(z_rank, params$z_knots, params$sigma_nu_curve) * het_multiplier
    kappa     <- params$kappa_base_raw * params$kappa_stab_factor * exp(params$alpha_kappa * z_rank)
    df_vec    <- pmax(.interpolate_curve(z_rank, params$z_knots, params$t_df_curve), 3.0)

    # -- Jump component --
    jump_mask <- runif(n_sim) < params$jump_prob
    eta <- rnorm(n_sim, 0.0, sigma_eta)
    if (any(jump_mask)) {
      n_jump <- sum(jump_mask)
      eta[jump_mask] <- rnorm(n_jump, 0.0, sigma_eta[jump_mask] * params$jump_scale)
    }

    # -- ARCH volatility clustering --
    arch_var   <- (1.0 - params$alpha_arch) + params$alpha_arch * last_z_sq
    arch_scale <- sqrt(pmin(pmax(arch_var, cfg$arch_clip[1L]), cfg$arch_clip[2L]))

    # -- Transitory innovation --
    if (use_heavy) {
      t_raw <- .sample_student_t_vector(df_vec)
      t_var_factor <- sqrt(pmax((df_vec - 2.0) / df_vec, 0.5 / df_vec))
      nu <- sigma_nu * arch_scale * t_var_factor * t_raw
    } else {
      nu <- sigma_nu * arch_scale * rnorm(n_sim)
    }

    # -- Update transitory component --
    c_state <- phi * c_state + nu
    last_z_sq <- pmin(pmax((nu / pmax(sigma_nu, 1e-8))^2, 0.0), cfg$z_sq_clip)

    # -- Update permanent component with mean reversion --
    current_mean <- if (cfg$universe_mode == "topk_buffered") {
      mean_reversion_target
    } else {
      mean(tau)
    }
    tau <- tau + eta - kappa * (tau - current_mean)

    # -- Recording period index (1-based in storage, 0-based logic) --
    t_rec <- t_abs - as.integer(params$burnin_periods)
    if (t_rec < 1L) next  # burn-in period

    # -- Track entity identity for continuity detection --
    if (is.null(tracked_initial_ids)) {
      tracked_initial_ids <- entity_ids[tracked]
    } else {
      tracked_alive <- tracked_alive & (entity_ids[tracked] == tracked_initial_ids)
    }

    # -- Observation with optional noise --
    x_true <- tau + c_state
    if (use_obs) {
      obs <- x_true + rnorm(n_sim, 0.0, params$sigma_obs)
    } else {
      obs <- x_true
    }
    threshold_log <- log1p(thresholds[t_rec])
    observed_mask <- obs >= threshold_log

    # -- Rank by observed values --
    obs_rank <- rank(-obs, ties.method = "first")

    # -- Identify observed entities --
    obs_order <- order(-obs)
    observed_in_order <- obs_order[observed_mask[obs_order]]
    observed_count <- length(observed_in_order)
    observed_counts[t_rec] <- observed_count
    xsec_var[t_rec] <- var(tau)

    if (observed_count > 0L) {
      # Compute observed-only ranks for tracked entities
      tracked_rank <- integer(n_tracked)
      tracked_observed <- observed_mask[tracked]

      if (any(tracked_observed)) {
        # Replicate Python:
        #   observed_obs_ranks = flatnonzero(observed_mask[obs_order]) + 1
        #   tracked_rank = searchsorted(observed_obs_ranks,
        #                               obs_rank[tracked][tracked_observed],
        #                               side="left") + 1
        # observed_positions: 1-based positions in obs_order that are observed
        observed_positions <- which(observed_mask[obs_order])
        tracked_obs_ranks_vals <- obs_rank[tracked[tracked_observed]]
        # findInterval with shifted breakpoints replicates searchsorted(side="left") + 1
        tracked_rank[tracked_observed] <- findInterval(
          tracked_obs_ranks_vals,
          observed_positions - 0.5
        )
      }

      # Record tracked values, masking unobserved and dead entities
      tracked_obs <- obs[tracked]
      tracked_obs[tracked_rank == 0L] <- NA_real_
      tracked_obs[!tracked_alive] <- NA_real_
      tracked_rank[!tracked_alive] <- 0L
      tracked_ranks[t_rec, ] <- tracked_rank
      tracked_values[t_rec, ] <- tracked_obs

      # Record top entity IDs
      top_n <- min(params$top_k, observed_count)
      top_ids[t_rec, seq_len(top_n)] <- entity_ids[observed_in_order[seq_len(top_n)]]

      # Record period-0 sorted values
      if (t_rec == 1L) {
        keep_n <- min(top_store_n, observed_count)
        period0_sorted_values <- obs[observed_in_order[seq_len(keep_n)]]
      }
    }

    # ---- Entry/exit process ----
    if (use_exit && exit_p_base > 0 && t_abs < t_total) {
      nr <- latent_rank / n_sim
      p_exit <- ifelse(
        ep_type == 0L,
        exit_p_base * nr^exit_alpha,
        exit_trans
      )
      exit_mask <- runif(n_sim) < p_exit
      n_ex <- sum(exit_mask)

      if (n_ex > 0L) {
        exi <- which(exit_mask)
        entry_counts <- .split_entry_counts(n_ex, burst_frac)
        n_burst <- entry_counts[["burst"]]
        n_norm  <- entry_counts[["normal"]]

        # Variance-neutral entry: normal entries replace near departing
        # entities' values to preserve cross-sectional distribution
        departing_vals <- sample(tau[exi])  # shuffle
        lower_tail <- tau[tau < median(tau)]
        bstd <- if (length(lower_tail) > 1L) sd(lower_tail) * 0.4 else 0.0
        bstd <- max(if (is.finite(bstd)) bstd else 0.0, 1e-6)

        if (n_norm > 0L) {
          new_tau <- departing_vals[seq_len(n_norm)] + rnorm(n_norm, 0, bstd)
        } else {
          new_tau <- numeric(0)
        }

        if (n_burst > 0L) {
          surviving <- !exit_mask
          burst_source <- if (any(surviving)) tau[surviving] else tau
          buq <- quantile(burst_source, 0.90, names = FALSE)
          bust <- sd(tau) * 0.25
          bust <- max(if (is.finite(bust)) bust else 0.0, 1e-6)
          burst_tau <- rnorm(n_burst, buq, bust)
          new_tau <- c(new_tau, burst_tau)
        }

        tau[exi] <- new_tau

        # Reset transitory component for new entrants
        if (use_heavy) {
          t_df_g <- params$t_df_global %||% 5.0
          c_state[exi] <- rt(n_ex, df = t_df_g) * 0.3
        } else {
          c_state[exi] <- rnorm(n_ex, 0, 0.3)
        }

        # Reset heterogeneity multipliers
        het_multiplier[exi] <- pmin(pmax(
          exp(rnorm(n_ex, 0.0, params$sigma_het)),
          het_lo
        ), het_hi)

        last_z_sq[exi] <- 1.0
        ep_type[exi] <- 1L

        # Assign new entity IDs
        entity_ids[exi] <- seq.int(next_entity_id, length.out = n_ex)
        next_entity_id <- next_entity_id + n_ex
      }
    }

    # ---- Boundary refresh (topk_buffered mode) ----
    if (replace_n > 0L && t_abs < t_total) {
      # Refresh the worst-ranked entities in the current latent state.
      refresh_order <- order(-(tau + c_state))
      refresh_candidates <- rev(refresh_order)
      not_tracked <- !tracked_mask[refresh_candidates]
      refresh_slots <- refresh_candidates[not_tracked]
      if (length(refresh_slots) > replace_n) {
        refresh_slots <- refresh_slots[seq_len(replace_n)]
      }

      n_refresh <- length(refresh_slots)
      if (n_refresh > 0L) {
        draw_idx <- sample.int(length(boundary_values), size = n_refresh, replace = TRUE)
        draws <- boundary_values[draw_idx]
        if (boundary_scale > 0.0) {
          draws <- draws + rnorm(n_refresh, 0.0, boundary_scale)
        }
        tau[refresh_slots] <- draws
        c_state[refresh_slots] <- 0.0
        last_z_sq[refresh_slots] <- 1.0
        het_multiplier[refresh_slots] <- pmin(pmax(
          exp(rnorm(n_refresh, 0.0, params$sigma_het)),
          het_lo
        ), het_hi)
        entity_ids[refresh_slots] <- seq.int(next_entity_id, length.out = n_refresh)
        next_entity_id <- next_entity_id + n_refresh
      }
    }
  }

  # ---- Assemble output ----
  sim <- list(
    tracked_ids           = entity_ids[tracked],
    tracked_values        = tracked_values,
    tracked_ranks         = tracked_ranks,
    top_ids               = top_ids,
    observed_counts       = observed_counts,
    xsec_var              = xsec_var,
    period0_sorted_values = period0_sorted_values
  )
  sim$diagnostics <- compute_sim_diagnostics(sim, cfg)
  sim
}

#' Run multiple Monte Carlo simulations
#'
#' Runs \code{\link{simulate_one}} across multiple seeds derived from the
#' configuration's \code{random_seed}.  The number of replications is
#' determined by \code{\link{resolved_mc_reps}}.
#'
#' @param params Estimated parameters list.
#' @param bundle A \code{rankdiff_bundle} produced by
#'   \code{\link{build_data_bundle}}.
#' @param cfg A \code{rankdiff_config}.
#' @param features Optional \code{rankdiff_features} for ablation control.
#' @return A list of simulation result lists (one per replication), each as
#'   returned by \code{\link{simulate_one}}.
#' @export
simulate_many <- function(params, bundle, cfg, features = NULL) {
  n_rep <- resolved_mc_reps(cfg)
  seeds <- cfg$random_seed + (seq_len(n_rep) - 1L) * 7919L
  n_periods <- bundle$n_periods

  lapply(seeds, function(s) {
    simulate_one(s, params, n_periods, cfg, features = features)
  })
}
