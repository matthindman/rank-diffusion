# C2 Factor-Kou with Smooth Rank-Dependent Parameters: Implementation Specification

**Project:** Facebook Weekly Rank Diffusion — Model Zoo Extension
**Date:** February 6, 2026
**Version:** 2.0 (supersedes bucketed C2 spec v1.0)
**Target implementer:** Codex / Claude Code / developer unfamiliar with the codebase
**Language:** R (tidyverse style, consistent with existing codebase)

---

## 0. Executive Summary

This spec describes the implementation of a two-stage estimation procedure for a Factor + asymmetric Kou jump-diffusion model of rank-slot dynamics on Facebook. The model decomposes weekly rank-slot increments into a common factor shock and an idiosyncratic Kou jump-diffusion component, with all idiosyncratic parameters varying smoothly as functions of log-rank rather than being estimated in crude buckets.

The implementation has five phases:

1. **Phase 1 (prerequisite):** Fix the existing C1 Factor-t model so its idiosyncratic distribution is estimated on factor-residuals rather than raw increments.
2. **Phase 2 (prerequisite):** Diagnose why all existing models produce zero simulated tail mass.
3. **Phase 3 (core):** Implement the two-stage C2 estimator with smooth rank-dependent parameters.
4. **Phase 4:** Simulation integration and tail calibration.
5. **Phase 5:** Diagnostics, bootstrap, and reporting.

Estimated total effort: 15–22 hours.

---

## 1. Context: What the Current Reports Show

This section summarizes the empirical evidence from the master analysis report and model zoo report that motivates the design. **Read this section carefully before writing any code** — the design decisions below are driven by specific empirical findings.

### 1.1 The Data

The dataset consists of 176 weekly snapshots of Facebook endpoint (page/account) engagement metrics, spanning October 2020 to March 2024. Each week contains roughly 13,000–14,700 endpoints ranked by a metric (likely total interactions). The analysis works with the top K_cut = 12,000 endpoints.

Core objects already computed in the existing pipeline:

| Object | Description | Location |
|--------|-------------|----------|
| `endpoint_weekly` | Raw panel: week, endpoint_id, metric_value, rank, share_global, share_topK | Loaded in setup |
| `rank_panel` | Rank-slot series: for each rank k, the sequence of shares of whoever occupies rank k each week. Columns: week, endpoint_id, metric_value, rank, share_global, share_topK, log_w, dlogw | Built from endpoint_weekly |
| `sm_params` | Smoothed per-rank moments: rank, mean_dlogw_s, sd_dlogw_s | LOESS/moving-average on rank_panel |
| `gauss_params_raw` | Raw per-rank moments: rank, mean_dlogw, sd_dlogw | Direct computation on rank_panel |
| `rank_inc_std` | Standardized rank-slot increments: z_{k,t} = (dlogw - mu_hat(k)) / sigma_hat(k). Columns: week, rank, bucket, dlogw, dlogw_std | Built by build_rank_slot_increments() + standardize_increments() |
| `pca_cache` | PCA results: beta_k (loadings, length K_max, smoothed, normalized), pc1_var_explained, pc1 (time series) | Computed in 04_model_variants.Rmd |
| `bucket_def` | Bucket definitions mapping ranks to {large, midsize, small} | Config |
| `emp_cdc` | Empirical capital distribution curve: rank, w_bar | Mean share by rank |
| `emp_targets` | Durable change targets: bucket, horizon, med_abs_emp | Median absolute log-share change |
| `rho_by_k_med` | Xi gap structure: k, rho_k (median log-gap between consecutive ranks) | From xi_weekly |
| `w0_ext` | Initial share vector for simulation (length K_max) | Built from endpoint_weekly |
| `entrant_sampler` | Function that generates new entrant shares | Built from entrant_pool |

### 1.2 The Existing Model Zoo

Six models were estimated. The comparison table:

| Model | Type | AIC | Converged | Notes |
|-------|------|-----|-----------|-------|
| A1 Student-t | Per-bucket Student-t | 1.50M | Yes | Best distributional fit (tied with C1) |
| B1 Merton | Per-bucket Merton jump-diffusion | 3.29M | Yes | Won composite score (good simulation targets, poor distribution) |
| B2 Kou | Per-bucket asymmetric Kou | 3.30M | Yes | Poor overall |
| B3 Gaussian Mixture | Per-bucket 2-component Gaussian mix | 1.57M | Yes | Good core fit, misses extreme tails |
| C1 Factor-t | Factor (Student-t) + idiosyncratic (Student-t) | 1.50M | Yes | **Bug: idiosyncratic fitted to raw data, not residuals** |
| C2 Factor-Kou | Factor + idiosyncratic Kou | — | **No** | Joint EM failed to converge |

### 1.3 Key Empirical Findings

#### Finding 1: The C1 implementation has a critical bug.

The C1 idiosyncratic parameters are *identical* to A1's standalone Student-t parameters:

```
         A1 Student-t    C1 Factor-t (idiosyncratic)
df_large       5.61              5.61
df_midsize     4.14              4.14
df_small       2.20              2.20
scale_large    0.824             0.824
scale_midsize  0.712             0.712
scale_small    0.216             0.216
```

This means C1 is fitting the idiosyncratic distribution to the RAW standardized increments, not to the residuals after factor removal. The factor (df=3.29, scale=9.44, loglik=-486) is estimated independently and added to the total likelihood, but it doesn't feed back into the idiosyncratic estimation. **Fixing this is a prerequisite for C2.**

#### Finding 2: Every model produces zero simulated tail mass.

The tail metrics comparison shows that ALL five converged models produce p_abs_gt_3 = 0 and p_abs_gt_5 = 0 for the simulated large-bucket rank-slot increments. The empirical targets are 0.013 and 0.0007 respectively. This is a simulation framework problem, not a distributional model problem. Possible causes:

- Smoothed moment vectors compress increment magnitudes.
- Rank re-sorting after applying increments mechanically compresses the rank-slot distribution.
- Entry/exit censors extreme observations.

**This must be diagnosed before C2 simulation results can be trusted.**

#### Finding 3: The B1 Merton "winning" is misleading.

B1 has AIC = 3.29M (dramatically worse than A1/C1's 1.50M) but won the composite score because its occasional large jumps improve CDC/durable-change simulation targets. Its parameters are wildly unstable under bootstrap: p_large ranges from 0.002 to 0.044 (25x), and mu_j_midsize spans -5.28 to +3.31 (sign unidentified).

#### Finding 4: The Student-t is a poor model for the small bucket.

The QQ plot of small-bucket standardized increments against a fitted Student-t (df=2.20) shows the theoretical distribution generating a range of ±500 while the data span only ±15. The df=2.20 estimate is being pulled down by a handful of extremes, producing massive overfit in the tails and underfit in the core. The Kou model (Gaussian base + occasional jumps) is structurally better for this bucket.

#### Finding 5: Jump clustering is significant for midsize and small buckets.

ACF1 of extreme indicators (|z| > 3) at the 3-sigma threshold: large = 0.05 (not significant), midsize = 0.46 (highly significant), small = 0.54 (highly significant). This means extreme observations tend to occur in consecutive weeks, suggesting either common shock episodes or self-exciting dynamics.

#### Finding 6: The CDC, durable change, and Xi gap structure are well-captured by the Gaussian baseline.

The simple Gaussian simulation with smoothed rank-dependent moments achieves RMSE scores of 0.096 (CDC), 0.056 (durable), 0.008 (Xi). These are the targets to match or beat. The standalone jump_factor model catastrophically failed these targets (RMSE 1.77, 1.64, 0.012) by overshooting tail mass.

#### Finding 7: Rank-slot tails are much lighter than identity-based tails.

For the large bucket: rank-slot p_abs_gt_3 = 0.013, identity-based p_abs_gt_3 = 0.343. The rank-slot view "averages out" individual-level volatility because the rank slot is re-occupied by a different endpoint each week. **All C2 estimation must target the rank-slot distribution, not the identity-based distribution.**

### 1.4 Why Buckets Are Wrong

The current framework uses three buckets (large, midsize, small) to discretize the rank axis for innovation distribution estimation. This is problematic because:

1. The bucket boundaries are arbitrary, creating artificial discontinuities.
2. It forces 5 parameters × 3 buckets = 15 free parameters when 8–12 smooth parameters would suffice.
3. It prevents the paper from characterizing *how* the innovation distribution varies with rank — only that it differs across three arbitrary groups.
4. The existing smoothed moment functions (mu_hat(k), sigma_hat(k)) already demonstrate that rank-dependent dynamics are smooth, not discontinuous.

The correct approach is to parameterize all innovation distribution parameters as smooth functions of log-rank.

---

## 2. Mathematical Framework

### 2.1 Full Model

The rank-slot increment for rank k at week t:

$$\Delta \log w_{k,t} = \hat{\mu}(k) + \hat{\sigma}(k) \cdot z_{k,t}$$

where mu_hat(k) and sigma_hat(k) are the smoothed per-rank drift and volatility (already estimated in `sm_params`), and:

$$z_{k,t} = \beta_k F_t + \varepsilon_{k,t}$$

with:

- beta_k: factor loading for rank k (from PCA, stored in `pca_cache$beta_k`)
- F_t: common factor, distributed Student-t(nu_F, s_F)
- eps_{k,t}: idiosyncratic component, following a Kou jump-diffusion with rank-dependent parameters

### 2.2 Factor Distribution

$$F_t \sim t_{\nu_F}(0, s_F)$$

Two free parameters: (nu_F, s_F). Starting values from C1: nu_F = 3.29, s_F = 9.44.

### 2.3 Idiosyncratic Kou Component (Smooth Parameters)

The idiosyncratic residual at rank k:

$$\varepsilon_{k,t} = \sigma_\varepsilon(k) \cdot W_t + J_{k,t}$$

where W_t ~ N(0,1) and J_{k,t} is a Kou jump:

- Jump occurs with probability p(k)
- Jump size: with probability pi_pos(k), draw Y ~ Exp(eta_pos(k)); otherwise Y ~ -Exp(eta_neg(k))

### 2.4 Smooth Parameter Functions

All parameters are smooth functions of log-rank via link functions:

| Parameter | Link | Functional form | Free params |
|-----------|------|----------------|-------------|
| p(k) | logit | logit(p) = a0 + a1 * log(k) | 2 |
| sigma_eps(k) | log | log(sigma_eps) = b0 + b1 * log(k) | 2 |
| pi_pos(k) | logit | logit(pi_pos) = c0 + c1 * log(k) | 2 |
| eta_pos(k) | log | log(eta_pos) = d0 + d1 * log(k) | 2 |
| eta_neg(k) | log | log(eta_neg) = e0 + e1 * log(k) | 2 |

**Total: 10 free parameters** for the idiosyncratic component (vs 15 for the bucketed version).

If the linear specification is too restrictive, add quadratic terms:

$$\text{logit}(p(k)) = a_0 + a_1 \log k + a_2 (\log k)^2$$

This adds 5 more parameters for a total of 15 — same count as bucketed but with smooth interpolation and no arbitrary boundaries.

### 2.5 Alternative: Condition on sigma_hat(k)

Instead of log(k), parameterize on log(sigma_hat(k)):

$$\text{logit}(p(k)) = a_0 + a_1 \log \hat{\sigma}(k)$$

This is more theoretically motivated (tail behavior scales with local volatility) and is invariant to changes in K_cut or bucket definitions. **Implement both and compare.**

---

## 3. File Layout

All new code goes in the existing R/ package directory:

```
R/
  model_c2_twostage.R      # NEW — Stages 1-4 + smooth parameter estimation
  model_c2_smooth.R         # NEW — smooth parameter link functions and joint likelihood
  model_c2_simulate.R       # NEW — increment generator for simulation framework
  model_zoo.R               # MODIFY — add C2 entry point
  kou_em.R                  # EXISTING — reuse for bucketed validation
  factor_fit.R              # EXISTING — reuse for Stage 1
analysis/
  04_model_variants.Rmd     # MODIFY — add C2 call + diagnostics
  05_model_zoo.Rmd          # MODIFY — add C2 to comparison
```

---

## 4. Phase 1: Fix the C1 Residual Pipeline

**Priority: Must do first. Estimated effort: 1–2 hours.**

### 4.1 Problem

The current C1 code estimates the factor distribution on the extracted factor series F_hat, then fits the idiosyncratic Student-t to the RAW standardized increments z_{k,t} instead of the residuals eps_{k,t} = z_{k,t} - beta_k * F_hat_t. This means the factor contributes a likelihood bonus but doesn't change the idiosyncratic model.

### 4.2 Fix

Locate the C1 estimation code (likely in `model_zoo.R` or a dedicated `factor_fit.R`). Find where the idiosyncratic Student-t is fitted. Change the input from `z_rank` (raw standardized increments) to residuals after factor removal.

Pseudocode for the fix:

```r
# CURRENT (broken):
# 1. Extract F_hat from cross-section of z_{k,t}
# 2. Fit Student-t to F_hat -> factor_fit
# 3. Fit Student-t to z_{k,t} by bucket -> idio_fit   <-- BUG: should use residuals

# FIXED:
# 1. Extract F_hat from cross-section of z_{k,t}
# 2. Fit Student-t to F_hat -> factor_fit
# 3. Compute eps_{k,t} = z_{k,t} - beta_k * F_hat_t
# 4. Fit Student-t to eps_{k,t} by bucket -> idio_fit  <-- CORRECT
```

### 4.3 Validation

After fixing, re-run C1 and check:

1. **Idiosyncratic df values should INCREASE relative to A1.** The factor absorbs some tail mass, so residuals should have lighter tails (higher df). If df_large goes from 5.61 to ~7–10 and df_small from 2.20 to ~3–4, the fix is working correctly.
2. **Idiosyncratic scale values should DECREASE.** The factor absorbs variance, so residual variance should be lower.
3. **If parameters remain identical to A1**, the factor loadings are effectively zero for all ranks used in estimation, or the factor extraction is returning near-zero values. Investigate.

### 4.4 Output

A corrected C1 that serves as the "C1.5" benchmark for C2.

---

## 5. Phase 2: Diagnose Simulation Tail Suppression

**Priority: Must do before Phase 4. Can be done in parallel with Phases 1 and 3. Estimated effort: 1–2 hours.**

### 5.1 Problem

Every model in the zoo produces p_abs_gt_3 = 0 in simulation for the large bucket, despite models like A1 Student-t explicitly having heavy tails with df=5.61. This means the simulation framework itself is suppressing extreme observations.

### 5.2 Diagnostic Tests

Run these tests on the existing Gaussian baseline simulation:

**Test A: Moment smoothing compression.** Generate 10,000 draws from N(mean_vec_s[k], sd_vec_s[k]) for a single rank k in the large bucket. Compute the fraction exceeding ±3 in standardized terms. If this fraction is > 0 (it should be ~0.27% for Gaussian), the moments aren't the problem. If it's 0, the smoothed sd_vec_s is compressing the scale.

```r
# For a representative large-bucket rank, e.g. k = 5:
k <- 5
z_sim <- rnorm(10000, mean_vec_s[k], sd_vec_s[k])
z_std <- (z_sim - mean_vec_s[k]) / sd_vec_s[k]
mean(abs(z_std) > 3)  # should be ~0.0027 for Gaussian
```

**Test B: Rank re-sorting compression.** After one simulation step, compute rank-slot increments BEFORE and AFTER the re-sorting step. Compare tail fractions. If re-sorting compresses tails, the rank-slot simulation framework fundamentally cannot match identity-based tail targets (which may be acceptable — see Finding 7).

**Test C: Entry/exit censoring.** Track endpoints that exit the top-K during simulation. Compute what their rank-slot increment would have been. If exits are systematically associated with large negative increments, the entry/exit mechanism is censoring the left tail.

### 5.3 Expected Outcome

Most likely, a combination of B and C explains the tail suppression. The correct response is NOT to artificially inject tail mass, but to:

1. Accept that rank-slot simulation will have lighter tails than the raw distributional model.
2. Calibrate the model parameters so that the SIMULATED rank-slot distribution (after re-sorting and entry/exit) matches the EMPIRICAL rank-slot tail metrics.
3. Report this calibration gap explicitly in the paper.

---

## 6. Phase 3: Two-Stage C2 with Smooth Parameters

**Priority: Core implementation. Estimated effort: 10–14 hours.**

### 6.1 Stage 1: Factor Extraction and Distribution Estimation

#### Purpose

Extract F_hat(t) from the cross-section of standardized increments and fit a Student-t distribution.

#### Input

- `rank_inc_std`: tibble with columns (week, rank, bucket, dlogw_std). Already computed.
- `pca_cache$beta_k`: numeric vector of PCA loadings, length K_max, smoothed and normalized.
- `K_pca`: integer, number of ranks used in PCA (from config, typically 200).

#### Algorithm

**Step 1a: Pivot to wide format and extract factor.**

```r
# Pivot standardized increments to weeks x ranks
z_wide <- rank_inc_std %>%
  filter(rank <= K_pca) %>%
  select(week, rank, dlogw_std) %>%
  pivot_wider(names_from = rank, values_from = dlogw_std)

# Drop weeks with any missing ranks
z_mat <- z_wide %>% drop_na() %>% arrange(week)
weeks_used <- z_mat$week
X <- z_mat %>% select(-week) %>% as.matrix()

# Extract factor: OLS projection onto loading vector
beta <- pca_cache$beta_k[1:K_pca]
beta_norm_sq <- sum(beta^2)
F_hat <- as.numeric(X %*% beta) / beta_norm_sq

F_hat_df <- tibble(week = weeks_used, F_hat = F_hat)
```

**Step 1b: Fit Student-t to F_hat.**

```r
# Use MASS::fitdistr or custom MLE
# Parameterize as location-scale Student-t: f(x; nu, s) = t_nu(x / s) / s
# Constraint: nu > 2 (finite variance)
# Starting values: nu = 3.3, s = 9.4 (from C1 factor estimate)

fit_factor_t <- function(F_vec, nu_init = 3.3, scale_init = 9.4) {
  neg_loglik <- function(par) {
    nu <- exp(par[1]) + 2  # ensures nu > 2
    s <- exp(par[2])       # ensures s > 0
    -sum(dt(F_vec / s, df = nu, log = TRUE) - log(s))
  }
  opt <- optim(
    par = c(log(nu_init - 2), log(scale_init)),
    fn = neg_loglik,
    method = "L-BFGS-B",
    hessian = TRUE
  )
  nu <- exp(opt$par[1]) + 2
  s <- exp(opt$par[2])
  list(
    df = nu, scale = s,
    loglik = -opt$value,
    converged = opt$convergence == 0,
    hessian = opt$hessian,
    n = length(F_vec)
  )
}
```

#### Function Signature

```r
#' Stage 1: Extract factor and fit its distribution
#'
#' @param z_long     Tibble with columns (week, rank, dlogw_std).
#' @param beta_k     Numeric vector of PCA loadings (length >= K_pca).
#' @param K_pca      Integer, number of ranks to use.
#' @param df_init    Starting value for Student-t df (default 3.3).
#' @param scale_init Starting value for Student-t scale (default 9.4).
#'
#' @return List:
#'   - F_hat: tibble (week, F_hat)
#'   - factor_fit: list (df, scale, loglik, converged, hessian, n)
#'   - beta_used: numeric vector of loadings actually used
#'   - diagnostics: list (n_weeks_used, n_weeks_dropped, beta_norm_sq,
#'                        F_hat_mean, F_hat_sd, F_hat_skew, F_hat_kurt)
stage1_extract_factor <- function(z_long, beta_k, K_pca,
                                   df_init = 3.3, scale_init = 9.4)
```

#### Validation

1. F_hat should have mean ≈ 0. If |mean(F_hat)| > 0.1, something is wrong with the centering.
2. QQ-plot of F_hat against fitted Student-t should track the 45-degree line.
3. n_weeks_dropped should be < 10% of total weeks. Log warning if higher.
4. Correlation of F_hat with pca_cache$pc1 should be very high (> 0.95) — they're estimating the same thing differently.

---

### 6.2 Stage 2: Residual Computation

#### Purpose

Remove the factor component to isolate idiosyncratic residuals.

#### Algorithm

```r
# For each (k, t): eps_{k,t} = z_{k,t} - beta_k * F_hat_t
residuals <- z_long %>%
  inner_join(F_hat_df, by = "week") %>%
  mutate(
    beta = beta_k[rank],  # look up loading for this rank
    eps_hat = dlogw_std - beta * F_hat
  )
```

For ranks k > K_pca, beta_k is approximately zero, so eps_hat ≈ dlogw_std. This is correct.

#### Function Signature

```r
#' Stage 2: Compute idiosyncratic residuals
#'
#' @param z_long   Tibble (week, rank, bucket, dlogw_std).
#' @param F_hat    Tibble (week, F_hat) from Stage 1.
#' @param beta_k   Numeric vector of loadings (length >= max(z_long$rank)).
#'
#' @return Tibble (week, rank, bucket, dlogw_std, F_hat, beta, eps_hat)
stage2_compute_residuals <- function(z_long, F_hat, beta_k)
```

#### Validation

Run these diagnostics and store results for the report:

1. **Factor-residual orthogonality.** For a grid of ranks (e.g., 1, 5, 10, 25, 50, 100, 500, 1000, 5000):

```r
cor_check <- residuals %>%
  group_by(rank) %>%
  summarise(cor_F_eps = cor(F_hat, eps_hat, use = "complete.obs"))
```

All values should be ≈ 0. If any exceed |0.1|, the loading vector needs adjustment.

2. **Tail reduction.** Run `tail_skew_diagnostics()` on the residuals (using buckets for comparison purposes) and compare to the raw-increment diagnostics:

```r
tail_residuals <- tail_skew_diagnostics(residuals, ...)
# Compare: tail_residuals$p_abs_gt_3 should be < tail_rank$p_abs_gt_3
# Compare: tail_residuals$excess_kurtosis should be < tail_rank$excess_kurtosis
```

If tails DON'T decrease, the factor isn't absorbing common shocks and the factor structure isn't adding value.

3. **Variance reduction.** Compute var(eps_hat) / var(dlogw_std) by rank. This ratio should be < 1 for ranks where beta_k is non-negligible, and ≈ 1 for ranks beyond K_pca.

4. **Jump clustering reduction.** Re-run `jump_clustering_diagnostics()` on residuals. If ACF1 of extreme indicators decreases (especially for midsize and small), the factor is absorbing common shock episodes.

---

### 6.3 Stage 3: Smooth Kou Estimation on Residuals

This is the most complex stage and the core novelty.

#### 6.3.1 Kou Density (Closed Form)

The density of a single residual eps under the Kou model with parameters theta = (p, sigma_eps, pi_pos, eta_pos, eta_neg):

```
f(eps | theta) = (1 - p) * phi(eps; 0, sigma_eps)
               + p * [pi_pos  * f_pos_conv(eps; sigma_eps, eta_pos)
                     + (1 - pi_pos) * f_neg_conv(eps; sigma_eps, eta_neg)]
```

where phi is the Gaussian density and the convolution terms (Gaussian + Exponential) have closed forms:

```
f_pos_conv(eps; sigma, eta) = eta * exp(eta * sigma^2 / 2 - eta * eps)
                             * Phi(eps / sigma - eta * sigma)

f_neg_conv(eps; sigma, eta) = eta * exp(eta * sigma^2 / 2 + eta * eps)
                             * Phi(-eps / sigma - eta * sigma)
```

where Phi is the standard normal CDF.

**Implementation note:** These convolutions involve exp() * Phi() products that can overflow/underflow. Use log-sum-exp and the log-normal-CDF (pnorm with log.p=TRUE) for numerical stability:

```r
log_f_pos_conv <- function(eps, sigma, eta) {
  log(eta) + eta * sigma^2 / 2 - eta * eps + pnorm(eps / sigma - eta * sigma, log.p = TRUE)
}

log_f_neg_conv <- function(eps, sigma, eta) {
  log(eta) + eta * sigma^2 / 2 + eta * eps + pnorm(-eps / sigma - eta * sigma, log.p = TRUE)
}

log_kou_density <- function(eps, p, sigma_eps, pi_pos, eta_pos, eta_neg) {
  log_gauss <- dnorm(eps, 0, sigma_eps, log = TRUE)
  log_jump_pos <- log_f_pos_conv(eps, sigma_eps, eta_pos)
  log_jump_neg <- log_f_neg_conv(eps, sigma_eps, eta_neg)

  log_jump <- log(pi_pos) + log_jump_pos  # initialize
  log_jump <- matrixStats::logSumExp(cbind(
    log(pi_pos) + log_jump_pos,
    log(1 - pi_pos) + log_jump_neg
  ))  # vectorize this properly

  matrixStats::logSumExp(cbind(
    log(1 - p) + log_gauss,
    log(p) + log_jump
  ))  # per-observation log-density
}
```

**Critical: Vectorize over observations, not parameters.** The density must be evaluated for ~2 million observations per likelihood call. Use vectorized R operations throughout; avoid any per-observation loops.

#### 6.3.2 Smooth Parameter Functions

Define the mapping from the coefficient vector theta_smooth to per-rank parameters:

```r
#' Evaluate smooth Kou parameters at given ranks
#'
#' @param ranks    Integer vector of ranks.
#' @param theta    Named numeric vector of smooth coefficients.
#' @param x_func   Function mapping rank -> covariate (default: log).
#'
#' @return Tibble (rank, p, sigma_eps, pi_pos, eta_pos, eta_neg)
smooth_kou_params <- function(ranks, theta, x_func = log) {
  x <- x_func(ranks)

  # Linear specification: param = link_inv(intercept + slope * x)
  p        <- plogis(theta["a0"] + theta["a1"] * x)
  sigma_eps <- exp(theta["b0"] + theta["b1"] * x)
  pi_pos   <- plogis(theta["c0"] + theta["c1"] * x)
  eta_pos  <- exp(theta["d0"] + theta["d1"] * x)
  eta_neg  <- exp(theta["e0"] + theta["e1"] * x)

  tibble(rank = ranks, p = p, sigma_eps = sigma_eps,
         pi_pos = pi_pos, eta_pos = eta_pos, eta_neg = eta_neg)
}
```

For the quadratic specification, add theta["a2"] * x^2, etc.

#### 6.3.3 Joint Log-Likelihood

```r
#' Smooth Kou log-likelihood across all ranks
#'
#' @param theta   Named numeric vector of smooth coefficients (10 for linear, 15 for quadratic).
#' @param eps_df  Tibble (rank, eps_hat) — the residuals.
#' @param x_func  Function mapping rank -> covariate.
#'
#' @return Scalar negative log-likelihood (for minimization).
neg_loglik_smooth_kou <- function(theta, eps_df, x_func = log) {
  # Get per-rank parameters
  unique_ranks <- sort(unique(eps_df$rank))
  par_df <- smooth_kou_params(unique_ranks, theta, x_func)

  # Join parameters to observations
  eps_with_par <- eps_df %>%
    inner_join(par_df, by = "rank")

  # Compute per-observation log-density
  ll <- with(eps_with_par,
    log_kou_density(eps_hat, p, sigma_eps, pi_pos, eta_pos, eta_neg)
  )

  -sum(ll, na.rm = TRUE)
}
```

**Performance note:** With ~2M observations, each likelihood evaluation takes meaningful time. Precompute the rank -> covariate mapping and parameter lookup outside the inner loop. Consider grouping observations by rank and computing the density in batches. If optimization is too slow, subsample observations (e.g., every 4th week) for the main optimization and evaluate the full likelihood only at the final estimate.

#### 6.3.4 Starting Values

Derive starting values from the existing B2 bucket estimates by inverting the link functions:

```r
# B2 estimates (from model_zoo_report):
# large  (representative rank ~5):   p=0.25,  pi_pos=0.427, eta_pos=1.21, eta_neg=1.19
# midsize (representative rank ~100): p=0.119, pi_pos=0.809, eta_pos=1.58, eta_neg=0.414
# small  (representative rank ~5000): p=0.027, pi_pos=0.391, eta_pos=0.159, eta_neg=0.259

# For the linear-in-log-rank model, use the three bucket midpoints
# to solve for intercept and slope:
# logit(p) = a0 + a1 * log(k)
# At k=5:    logit(0.25)  = -1.10 = a0 + a1 * 1.61
# At k=5000: logit(0.027) = -3.58 = a0 + a1 * 8.52
# => a1 = (-3.58 - (-1.10)) / (8.52 - 1.61) = -0.359
# => a0 = -1.10 - (-0.359) * 1.61 = -0.52

init_theta <- c(
  a0 = -0.52, a1 = -0.36,    # p(k): decreasing with rank
  b0 = -0.15, b1 = -0.05,    # sigma_eps(k): roughly constant or slightly decreasing
  c0 =  0.0,  c1 = -0.05,    # pi_pos(k): roughly 0.5, slight decrease
  d0 =  0.3,  d1 = -0.10,    # eta_pos(k): moderate, decreasing
  e0 =  0.1,  e1 = -0.05     # eta_neg(k): slightly lower than eta_pos (heavier left tail)
)
```

These starting values should be adjusted after the C1 fix (Phase 1) and residual computation (Stage 2). The residuals will have different tail structure than the raw data.

#### 6.3.5 Optimization

```r
#' Stage 3: Fit smooth Kou model to residuals
#'
#' @param eps_long   Tibble (week, rank, eps_hat).
#' @param init_theta Named numeric starting values (default: derived from B2).
#' @param x_func     Covariate function (default: log; alternative: function(k) log(sigma_hat(k))).
#' @param quadratic  Logical, include quadratic terms (default FALSE).
#' @param max_iter   L-BFGS-B iterations (default 500).
#' @param subsample_frac  Fraction of weeks to use (default 1.0; reduce for speed).
#'
#' @return List:
#'   - theta: named numeric vector of optimal coefficients
#'   - loglik: scalar log-likelihood at optimum
#'   - converged: logical
#'   - hessian: matrix (for SEs)
#'   - n_params: integer
#'   - n_obs: integer
#'   - aic: scalar
#'   - bic: scalar
#'   - param_curves: tibble (rank, p, sigma_eps, pi_pos, eta_pos, eta_neg) evaluated at all ranks
#'   - diagnostics: list (optimization trace, gradient norm, etc.)
stage3_fit_smooth_kou <- function(eps_long, init_theta = NULL,
                                   x_func = log, quadratic = FALSE,
                                   max_iter = 500L, subsample_frac = 1.0)
```

Use `optim()` with method = "L-BFGS-B". The parameter space is unconstrained (all parameters are in link-function space where the domain is all of R), so box bounds are not strictly needed, but add wide bounds (±20) to prevent numerical overflow in the link inverses.

If convergence is difficult, try:

1. Reduce subsample_frac to 0.25 for a fast initial estimate, then refine with full data.
2. Use `nlminb()` instead of `optim()` — it's often more robust for moderate-dimensional problems.
3. Add a small ridge penalty (L2 regularization on the slope coefficients a1, b1, ..., e1) to prevent extreme extrapolation at the boundaries of the rank range.

#### 6.3.6 Bucketed Validation

After fitting the smooth model, evaluate the smooth parameter curves at the representative ranks for each bucket and compare to the B2 bucket estimates. The smooth estimates should pass through the neighborhood of the bucket estimates but won't match exactly (which is fine — the smooth model pools information across the rank boundary).

```r
# Validation: compare smooth to bucketed at representative ranks
bucket_validation <- tibble(
  bucket = c("large", "midsize", "small"),
  rep_rank = c(5, 100, 5000)
) %>%
  mutate(smooth_params = map(rep_rank, ~ smooth_kou_params(.x, theta_opt, x_func))) %>%
  unnest(smooth_params)
# Compare to B2 params
```

Also fit the Kou model **independently per bucket** (reusing the existing B2 EM code on residuals instead of raw data) and compare. The smooth and bucketed estimates should be qualitatively consistent.

---

### 6.4 Stage 4: Assembly

#### Purpose

Package the two-stage estimates into a model object compatible with the simulation framework and comparison table.

```r
#' Stage 4: Assemble C2 fit object
#'
#' @param stage1  Output of stage1_extract_factor().
#' @param stage2  Output of stage2_compute_residuals().
#' @param stage3  Output of stage3_fit_smooth_kou().
#' @param x_func  Covariate function used in Stage 3.
#'
#' @return List (fit object for simulation and comparison):
#'   - type: "factor_kou_smooth_twostage"
#'   - converged: logical (TRUE iff both Stage 1 and Stage 3 converged)
#'   - factor_fit: list (df, scale, loglik, F_hat, beta_k)
#'   - idio_fit: list (theta, loglik, param_curves, n_params)
#'   - loglik: combined log-likelihood
#'   - aic: combined AIC
#'   - bic: combined BIC
#'   - n_params: total parameter count (2 factor + 10 or 15 idiosyncratic)
#'   - x_func: the covariate function (needed for simulation)
#'   - estimation_method: "two_stage"
#'   - diagnostics: list (from stages 1, 2, 3)
stage4_assemble <- function(stage1, stage2, stage3, x_func = log)
```

Combined likelihood:

```
loglik_total = stage1$factor_fit$loglik + stage3$loglik
n_params_total = 2 + stage3$n_params  # (2 factor + 10 or 15 idiosyncratic)
AIC = -2 * loglik_total + 2 * n_params_total
BIC = -2 * loglik_total + n_params_total * log(stage3$n_obs + stage1$factor_fit$n)
```

**Caveat for the paper:** The two-stage log-likelihood is an approximation. Note this in the model table footnote.

---

## 7. Phase 4: Simulation Integration

### 7.1 Increment Generator

The simulation framework calls a function each timestep to generate increments for all K_max ranks. The C2 generator:

```r
#' Generate one-step increments under C2 Factor-Kou (smooth)
#'
#' @param K_max     Total number of ranks in simulation.
#' @param mean_vec  Smoothed per-rank drift, length K_max.
#' @param sd_vec    Smoothed per-rank volatility, length K_max.
#' @param model     C2 fit object from stage4_assemble().
#'
#' @return Numeric vector of length K_max: raw (unstandardized) dlogw increments.
generate_c2_increments <- function(K_max, mean_vec, sd_vec, model) {
  beta <- model$factor_fit$beta_k  # length K_max, zero-padded

  # 1. Draw common factor
  F_t <- rt(1, df = model$factor_fit$df) * model$factor_fit$scale

  # 2. Evaluate smooth Kou parameters at all ranks
  par <- smooth_kou_params(1:K_max, model$idio_fit$theta, model$x_func)

  # 3. Draw idiosyncratic components (vectorized)
  # Gaussian base
  eps <- rnorm(K_max, 0, par$sigma_eps)

  # Kou jumps
  has_jump <- rbinom(K_max, 1, par$p)
  n_jumps <- sum(has_jump)
  if (n_jumps > 0) {
    jump_positive <- rbinom(n_jumps, 1, par$pi_pos[has_jump == 1])
    jump_sizes <- ifelse(
      jump_positive,
      rexp(n_jumps, par$eta_pos[has_jump == 1]),
      -rexp(n_jumps, par$eta_neg[has_jump == 1])
    )
    eps[has_jump == 1] <- eps[has_jump == 1] + jump_sizes
  }

  # 4. Combine and unstandardize
  z <- beta * F_t + eps
  dlogw <- mean_vec + sd_vec * z
  return(dlogw)
}
```

### 7.2 Registration in Model Zoo

Add C2 to the `run_model_comparison()` function or the dispatcher in `05_model_zoo.Rmd`:

```r
# Wire into model zoo
if ("C2_factor_kou_smooth" %in% models_to_run) {
  c2_result <- tryCatch({
    s1 <- stage1_extract_factor(rank_inc_std, pca_cache$beta_k, K_pca)
    s2 <- stage2_compute_residuals(rank_inc_std, s1$F_hat, pca_cache$beta_k)
    s3 <- stage3_fit_smooth_kou(s2, x_func = log)
    s4 <- stage4_assemble(s1, s2, s3, x_func = log)

    # Also fit with sigma_hat covariate for comparison
    x_func_sigma <- function(k) log(sm_params$sd_dlogw_s[pmin(k, length(sm_params$sd_dlogw_s))])
    s3_sigma <- stage3_fit_smooth_kou(s2, x_func = x_func_sigma)

    # Simulation
    sim_c2 <- simulate_rank_paths(
      w0 = w0_ext, K_cut = K_cut, K_max = K_max,
      T = CFG$sim_T_weeks, n_paths = CFG$sim_n_paths,
      # ... standard params ...
      model = s4$fit,
      increment_fn = generate_c2_increments  # may need adaptation to existing interface
    )

    # Tail metrics on simulated increments
    tail_sim <- compute_sim_tail_metrics(sim_c2, sm_params, bucket_def)

    # Scores
    score <- tibble(
      model = "C2_factor_kou_smooth",
      rmse_cdc = cdc_rmse(emp_cdc, sim_c2$cdc),
      rmse_durable = durable_rmse(emp_targets, sim_c2$growth, bucket_def),
      rmse_xi = xi_rmse(rho_by_k_med, sim_c2$xi)
    )

    list(fit = s4, sim = sim_c2, score = score, tail_sim = tail_sim)
  }, error = function(e) {
    warning("C2 smooth failed: ", conditionMessage(e))
    list(fit = list(type = "factor_kou_smooth_twostage", converged = FALSE))
  })

  results[["C2_factor_kou_smooth"]] <- c2_result
}
```

### 7.3 Tail Calibration

If Phase 2 reveals that the simulation framework structurally suppresses tails (most likely due to rank re-sorting), implement a calibration step:

```r
# After simulation, compute the empirical-to-simulated tail ratio
# and report it. Do NOT artificially inject tail mass.
tail_calibration <- tibble(
  bucket = c("large", "midsize", "small"),
  emp_p3 = tail_rank$p_abs_gt_3,
  sim_p3 = tail_sim$p_abs_gt_3,
  ratio = emp_p3 / pmax(sim_p3, 1e-6)
)
```

---

## 8. Phase 5: Diagnostics and Bootstrap

### 8.1 Report Sections

Add these diagnostic blocks to the Rmd:

#### Factor Quality

```r
# 1. F_hat time series plot
# 2. QQ: F_hat vs fitted Student-t
# 3. Correlation: F_hat vs pca_cache$pc1
# 4. Loading recovery: cor(F_hat, z_{k,t}) by rank vs beta_k
```

#### Residual Quality

```r
# 1. Tail diagnostics table: raw z vs residual eps, by bucket
#    Columns: bucket, metric, raw_value, residual_value, pct_reduction
# 2. Factor-residual correlation by rank (should be flat near zero)
# 3. Variance ratio var(eps) / var(z) by rank
# 4. Jump clustering: raw vs residual ACF1 at 3-sigma threshold
```

#### Smooth Parameter Curves

```r
# KEY FIGURE FOR THE PAPER:
# Plot all 5 Kou parameters as smooth curves of log-rank,
# with bucketed B2 estimates overlaid as validation points.

param_curves <- smooth_kou_params(1:K_cut, theta_opt, log)

p1 <- ggplot(param_curves, aes(rank, p)) +
  geom_line() +
  scale_x_log10() +
  labs(title = "Jump probability p(k)", x = "Rank (log)", y = "p")

p2 <- ggplot(param_curves, aes(rank, eta_neg)) +
  geom_line(aes(color = "eta_neg")) +
  geom_line(aes(y = eta_pos, color = "eta_pos")) +
  scale_x_log10() +
  labs(title = "Jump decay rates", x = "Rank (log)", y = "eta")

# ... etc for sigma_eps, pi_pos
```

Also produce: if using the sigma_hat covariate, plot parameters vs sigma_hat(k) instead of k.

#### Model Comparison

```r
# 1. Updated model table with C2 row
# 2. CDC overlay: Empirical vs Gaussian baseline vs C1.5 vs C2
# 3. Tail metrics scatter: all models including C2
# 4. QQ grid: add C2 column to the 5-model × 3-bucket grid
```

### 8.2 Bootstrap

Block bootstrap for the two-stage estimator. Resample weeks in contiguous blocks of length L (default 8 weeks).

```r
#' Block bootstrap for C2 two-stage parameters
#'
#' @param z_long       Standardized increments.
#' @param beta_k       PCA loadings.
#' @param K_pca        Number of PCA ranks.
#' @param x_func       Covariate function.
#' @param B            Number of resamples (default 500).
#' @param block_length Block length in weeks (default 8).
#' @param seed         Random seed.
#'
#' @return Tibble (param, q05, q50, q95) for all coefficients.
bootstrap_c2_smooth <- function(z_long, beta_k, K_pca, x_func = log,
                                 B = 500L, block_length = 8L, seed = 42L) {
  # For each resample:
  #   1. Block-resample weeks
  #   2. Re-run stage1 (extract factor on resampled cross-sections)
  #   3. Re-run stage2 (residuals)
  #   4. Re-run stage3 (smooth Kou on residuals)
  #   5. Store theta vector
  # Return quantile summary
}
```

Compare CI widths to B1 Merton (which showed 25× range on p_large and unidentified signs on mu_j). If C2 CIs are substantially tighter, this demonstrates that the factor decomposition resolves the identification problem that plagued the standalone jump models.

---

## 9. Configuration

Add to `config.yml`:

```yaml
c2_smooth:
  enabled: true
  K_pca: 200
  factor_df_init: 3.3
  factor_scale_init: 9.4
  x_func: "log_rank"           # "log_rank" or "log_sigma_hat"
  quadratic: false              # add quadratic terms to smooth functions
  optim_method: "L-BFGS-B"
  optim_max_iter: 500
  subsample_frac: 1.0          # reduce for faster iteration
  ridge_lambda: 0.0            # L2 penalty on slope coefficients (0 = none)
  bootstrap_B: 500
  bootstrap_block_length: 8
  # Comparison: also run bucketed version for validation
  also_run_bucketed: true
  kou_bucket_max_iter: 500
  kou_bucket_tol: 1.0e-6
```

---

## 10. Success Criteria

### Must-have

1. **Convergence.** Both Stage 1 and Stage 3 converge. The fit$converged flag is TRUE.
2. **CDC fit.** rmse_cdc < 0.14 (within 50% of Gaussian baseline's 0.096).
3. **Durable change fit.** rmse_durable < 0.084 (within 50% of baseline's 0.056).
4. **Xi fit.** rmse_xi < 0.012 (comparable to baseline's 0.008).
5. **Smooth parameter curves are monotonic or slowly varying.** Wild oscillations indicate overfitting or a covariate mismatch.

### Should-have

6. **AIC competitive with A1/C1.** Below 1.6M (A1/C1 = 1.50M; B3 = 1.57M).
7. **Tail metrics improved.** Simulated p_abs_gt_3 > 0 for at least one bucket (breaking the universal-zero problem). If Phase 2 reveals this is a simulation framework constraint, document the calibration ratio.
8. **Bootstrap CIs tighter than B1 Merton.** Especially for jump probability parameters.
9. **Negative skew captured.** eta_neg(k) < eta_pos(k) for large-rank range, consistent with Delta_tail = -1.01.
10. **C1.5 checkpoint passes.** Fixed C1 (Phase 1) shows idiosyncratic df > A1's df, confirming the factor does useful work.

### Nice-to-have

11. **Sigma-hat covariate outperforms log-rank.** Would strengthen the theoretical interpretation.
12. **Quadratic terms not needed.** Linear specification suffices (parsimony).
13. **Residual jump clustering reduced.** Factor absorbs common shock episodes, reducing ACF1 in residual extreme indicators.

---

## 11. Implementation Sequence

| Step | Phase | Action | Hours | Dependency |
|------|-------|--------|-------|------------|
| 1 | 1 | Fix C1 residual pipeline in existing code | 1–2 | None |
| 2 | 1 | Re-run C1, validate idiosyncratic params differ from A1 | 0.5 | Step 1 |
| 3 | 2 | Run simulation tail suppression diagnostics (Tests A/B/C) | 1–2 | None (parallel) |
| 4 | 3 | Create `model_c2_smooth.R`: link functions, smooth_kou_params, log_kou_density | 2–3 | None |
| 5 | 3 | Create `model_c2_twostage.R`: stage1_extract_factor | 1–2 | Step 4 |
| 6 | 3 | Add stage2_compute_residuals + validation diagnostics | 1 | Step 5 |
| 7 | 3 | Add stage3_fit_smooth_kou (joint likelihood + optim) | 3–4 | Steps 4, 6 |
| 8 | 3 | Add stage4_assemble | 0.5 | Step 7 |
| 9 | 3 | Run smooth estimation, debug convergence, validate vs buckets | 2–3 | Steps 1–8 |
| 10 | 4 | Create `model_c2_simulate.R`: generate_c2_increments | 1 | Step 8 |
| 11 | 4 | Wire into model zoo, run simulation, compute scores | 1–2 | Steps 9, 10 |
| 12 | 5 | Add diagnostic blocks to Rmd | 1–2 | Step 11 |
| 13 | 5 | Add bootstrap function | 1–2 | Step 9 |
| 14 | 5 | Full re-render and comparison | 0.5 | All |

**Total: 17–25 hours.**

Critical path: Steps 1 → 5 → 6 → 7 → 9 → 10 → 11 (11–17 hours).

---

## 12. Risk Mitigation

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Stage 3 smooth optimization doesn't converge | Medium | Start with subsample_frac = 0.25; use nlminb instead of optim; add ridge penalty on slopes; fall back to bucketed Kou on residuals |
| Smooth parameter curves oscillate wildly | Medium | Increase ridge penalty; reduce to linear-only (no quadratic); verify starting values are reasonable by checking bucketed estimates on residuals first |
| Residuals show no tail reduction after factor removal | Low-Medium | Indicates factor loadings are too small or PCA captured noise rather than signal. Try: increase K_pca; use varimax rotation; fit factor by maximum likelihood instead of PCA projection |
| Simulation still produces zero tail mass even with C2 | High | This is likely a simulation framework issue (Phase 2). Document the calibration gap; present distributional fit and simulation fit as separate lines of evidence in the paper |
| L-BFGS-B is too slow with 2M observations | Medium | Subsample to 500K observations for optimization (every 4th week); evaluate full likelihood only at optimum; precompute rank→parameter mapping |
| C2 AIC is worse than A1/C1 despite better tail capture | Low | The two-stage approximation penalizes the likelihood. Report both AIC and simulation-based scores; argue simulation targets are more policy-relevant |
| Bootstrap is too slow (500 × full pipeline) | High | Reduce B to 200; subsample within each bootstrap replicate; parallelize with `furrr::future_map()` |

---

## 13. Dependencies and Packages

Existing packages used in the codebase (do not add new dependencies unless essential):

- `tidyverse` (dplyr, tidyr, ggplot2, purrr, stringr, readr, tibble, forcats, lubridate)
- `scales`, `broom`, `arrow`, `withr`, `jsonlite`, `digest`
- `here` (project paths)
- `MASS` (fitdistr, for Stage 1 validation)
- `robustbase` (robust moments, existing)

Potentially needed (check if already loaded):

- `matrixStats` (logSumExp for numerically stable log-density computation)
- `numDeriv` (numerical Hessian if optim's hessian is unreliable)

If `matrixStats` is not available, implement logSumExp manually:

```r
log_sum_exp <- function(x) {
  m <- max(x, na.rm = TRUE)
  if (is.infinite(m)) return(m)
  m + log(sum(exp(x - m), na.rm = TRUE))
}
```

---

## Appendix A: Notation Reference

| Symbol | Meaning |
|--------|---------|
| k | Rank (1 = highest engagement) |
| t | Week index |
| w_{k,t} | Share of total engagement held by whoever occupies rank k at week t |
| dlogw_{k,t} | One-week log-change in rank-slot share: log(w_{k,t+1}) - log(w_{k,t}) |
| mu_hat(k) | Smoothed per-rank drift (mean of dlogw) |
| sigma_hat(k) | Smoothed per-rank volatility (SD of dlogw) |
| z_{k,t} | Standardized increment: (dlogw - mu_hat) / sigma_hat |
| beta_k | PCA factor loading for rank k |
| F_t | Common factor realization at week t |
| eps_{k,t} | Idiosyncratic residual: z_{k,t} - beta_k * F_t |
| p(k) | Jump probability at rank k |
| pi_pos(k) | Probability of positive jump (given jump occurs) |
| eta_pos(k) | Positive jump decay rate (larger = smaller jumps) |
| eta_neg(k) | Negative jump decay rate (smaller = larger negative jumps) |
| sigma_eps(k) | Diffusive volatility of idiosyncratic component |
| K_cut | Top-K cutoff for analysis (12,000) |
| K_max | Extended rank count for simulation (includes buffer beyond K_cut) |
| K_pca | Number of ranks used in PCA factor extraction (typically 200) |
| CDC | Capital Distribution Curve: mean share by rank |
| Xi_k | Log-gap between consecutive ranks: log(w_k / w_{k+1}) |
| rho_k | Median Xi_k across weeks |

## Appendix B: Key File Locations in Existing Codebase

These are inferred from the report code. Verify actual paths before coding.

```
R/
  helpers.R                    # rank_bucket_simple(), assign_bucket(), etc.
  smoothing.R                  # moving_average_rank(), LOESS wrappers
  simulation.R                 # simulate_rank_paths(), build_entrant_pool(), etc.
  model_zoo.R                  # run_model_comparison(), model dispatcher
  kou_em.R                     # B2 Kou EM estimation
  factor_fit.R                 # C1 factor distribution estimation
  tail_diagnostics.R           # tail_skew_diagnostics(), jump_clustering_diagnostics()
  metrics.R                    # cdc_rmse(), durable_rmse(), xi_rmse()
  micro.R                      # emp_micro_for_k(), sim_micro_for_k()
  cache.R                      # cache_or_compute(), fingerprinting

analysis/
  00_setup.Rmd                 # Loads data, defines parameters
  01_data_quality.Rmd          # Coverage checks
  02_cdc.Rmd                   # Capital distribution curve
  03_durable_change.Rmd        # Durable change targets
  04_model_variants.Rmd        # Moments, Xi gaps, PCA, simulation, model zoo
  05_model_zoo.Rmd             # Dedicated model zoo report (new)
  master_report.Rmd            # Knits everything together

config.yml                     # CFG parameters
```
