# C2 Factor + Smooth Innovations: Implementation Specification

**Project:** Facebook Weekly Rank Diffusion — Model Zoo Extension
**Date:** February 6, 2026
**Version:** 3.0 (supersedes v2.0; incorporates clean-data analysis)
**Target implementer:** Codex / Claude Code / developer unfamiliar with the codebase
**Language:** R (tidyverse style, consistent with existing codebase)

---

## 0. Executive Summary

This spec describes the implementation of a two-stage estimation procedure for modeling rank-slot dynamics on Facebook. The model decomposes weekly rank-slot increments into a Gaussian common factor and an idiosyncratic heavy-tailed component, with all idiosyncratic parameters varying smoothly as functions of log-rank.

Two co-equal model specifications are implemented:

- **C1.5 Factor + Smooth Student-t:** Gaussian factor + idiosyncratic Student-t with rank-dependent df(k) and scale(k). This is the expected workhorse model.
- **C2 Factor + Smooth Kou:** Gaussian factor + idiosyncratic Kou jump-diffusion with rank-dependent parameters. This tests whether a discrete jump mechanism adds value beyond continuous heavy tails.

Both share the same two-stage architecture (factor extraction → residual computation → idiosyncratic fit) and smooth parameterization. The C1.5 is simpler (4 free idiosyncratic parameters) and should be implemented first; C2 (10 free idiosyncratic parameters) extends it with jump structure.

The implementation has four phases:

1. **Phase 1 (prerequisite):** Fix the existing C1 Factor-t so its idiosyncratic distribution is estimated on factor-residuals, not raw increments.
2. **Phase 2 (core):** Implement the two-stage estimator with smooth rank-dependent parameters — C1.5 first, then C2.
3. **Phase 3:** Simulation integration and comparison.
4. **Phase 4:** Diagnostics, bootstrap, and reporting.

Estimated total effort: 12–18 hours.

---

## 1. Context: Data, Findings, and Design Rationale

**Read this section carefully before writing any code.** Every design decision below is motivated by specific empirical findings from two rounds of analysis.

### 1.1 The Data and Its Limitations

The dataset consists of weekly snapshots of Facebook endpoint (page/account) engagement metrics collected via CrowdTangle. The full archive spans October 2020 to March 2024 (~176 weeks), but **the analysis uses a clean subset of approximately 80 weeks** due to a major data quality problem:

**CrowdTangle suffered a massive technical breakdown in summer 2022.** During this period, observed ranks dropped from 12,000+ to as few as 4,000 on some days. This was a measurement instrument failure, not a reflection of actual platform dynamics — real content continued to be published and engaged with, but the data pipeline stopped recording it. The breakdown persisted for several months and was followed by a gradual recovery during which observed rank counts slowly decayed.

**The model zoo is built on the cleanest data window only.** This decision is critical for interpreting the results. An earlier analysis that included the contaminated period produced dramatically different findings:

| Finding | Contaminated data | Clean data | Explanation |
|---------|------------------|-----------|-------------|
| Common factor df | 3.3 (very heavy-tailed) | 36.3 (essentially Gaussian) | The outage *was* the fat-tailed factor shock |
| Jump clustering (ACF1 at 3σ) | 0.46–0.54, p < 1e-9 | 0.05–0.10, not significant | Multi-month breakdown created temporal clustering |
| Midsize Student-t df | 4.14 (heavy tails) | 15.7 (near Gaussian) | Artificial extremes from measurement failure |
| Small Student-t df | 2.20 (pathological) | 3.54 (moderate) | Same mechanism |
| B2 Kou convergence | Converged | Failed | Lighter tails give less signal for jump identification |
| B3 Gaussian Mixture convergence | Converged | Failed | Same |

Every "dramatic" finding from the contaminated analysis — fat-tailed common factor, jump clustering, extreme kurtosis — was an artifact of the instrument failure. The clean data tells a much more coherent story about normal platform dynamics.

### 1.2 Core Data Objects

These are already computed in the existing pipeline:

| Object | Description |
|--------|-------------|
| `endpoint_weekly` | Raw panel: week, endpoint_id, metric_value, rank, share_global, share_topK |
| `rank_panel` | Rank-slot series: for each rank k, the share of whoever occupies rank k each week. Columns: week, endpoint_id, metric_value, rank, share_global, share_topK, log_w, dlogw |
| `sm_params` | Smoothed per-rank moments: rank, mean_dlogw_s, sd_dlogw_s |
| `rank_inc_std` | Standardized rank-slot increments: z_{k,t} = (dlogw - mu_hat(k)) / sigma_hat(k). Columns: week, rank, bucket, dlogw, dlogw_std |
| `pca_cache` | PCA results: beta_k (loadings, length K_max, smoothed, normalized), pc1_var_explained, pc1 (time series) |
| `bucket_def` | Bucket definitions mapping ranks to {large, midsize, small} |
| `w0_ext` | Initial share vector for simulation (length K_max) |
| `entrant_sampler` | Function that generates new entrant shares |

### 1.3 The Existing Model Zoo (Clean Data)

Six models were estimated on the clean data window:

| Model | Type | AIC | Converged | Key finding |
|-------|------|-----|-----------|-------------|
| A1 Student-t | Per-bucket Student-t | 3.26M | Yes | df: 8.78 / 15.7 / 3.54 |
| B1 Merton | Per-bucket Merton jump | 3.50M | Yes | Won composite score; parameters unidentified |
| B2 Kou | Per-bucket asymmetric Kou | 3.43M | **No** | p_large at bound (0.25); EM unstable |
| B3 Gaussian Mixture | Per-bucket 2-component mix | 3.25M | **No** | Best AIC despite non-convergence |
| C1 Factor-t | Factor + idiosyncratic Student-t | 3.26M | Yes | **Bug: idiosyncratic = raw, not residuals** |
| C2 Factor-Kou | Factor + idiosyncratic Kou | — | **No** | Joint EM failed entirely |

### 1.4 Key Empirical Findings (Clean Data)

#### Finding 1: The common factor is Gaussian.

The C1 factor estimate has df = 36.3 and scale = 14.0. A Student-t with df > 30 is indistinguishable from Gaussian. Platform-wide shocks under normal operation are moderate and symmetric — extreme rank movements are idiosyncratic, not systemic. This is a substantive finding: viral events and sudden ranking changes are endpoint-specific, not platform-level cascades.

#### Finding 2: Innovation tail weight varies smoothly with rank.

The A1 Student-t estimates show a clear gradient:

- Large (ranks 1–25): df = 8.78 — moderate tails, driven by a few large negative shocks at top ranks
- Midsize (ranks 26–250): df = 15.7 — near-Gaussian, very stable dynamics
- Small (ranks 251–12,000): df = 3.54 — heavier tails, reflecting entry/exit dynamics and volatility at lower ranks

This gradient is smooth, not discontinuous. The bucket boundaries are arbitrary. A smooth function df(k) that declines from ~16 in the mid-range to ~4 at the extremes captures the empirical structure far better than three point estimates.

#### Finding 3: The C1 implementation has a critical bug.

The C1 idiosyncratic parameters are identical to A1's standalone Student-t:

```
C1 idio:  df = 8.78, 15.7, 3.54;  scale = 0.871, 0.882, 0.577
A1:       df = 8.78, 15.7, 3.54;  scale = 0.871, 0.882, 0.577
```

The factor is estimated independently (loglik = −473.8) but doesn't feed back into the idiosyncratic estimation. The idiosyncratic model is fitted to raw z_{k,t} instead of residuals eps_{k,t} = z_{k,t} − beta_k * F_hat_t. **Fixing this is a prerequisite.**

#### Finding 4: The C1 factor scale is too large.

With scale = 14.0 and RMS-normalized loadings, a 1σ factor shock translates to ~14 units in standardized-increment space. The simulated C1 produces excess kurtosis of ~200 for the large bucket (empirical: 1.54), meaning the factor generates occasional enormous common shocks that dominate higher moments. This is likely an artifact of the factor being estimated in isolation from the idiosyncratic component — once C1 is fixed and the factor is properly calibrated against residual variance, the scale should decrease.

#### Finding 5: Jump models are poorly identified on clean data.

B2 Kou failed to converge. Its p_large sits at the 0.25 constraint bound, meaning the "jump" component wants to become a second continuous component rather than a rare-event mechanism. The B1 Merton "wins" composite score but its bootstrap shows:

- p_large: 0.002–0.054 (27× range)
- mu_j_midsize: −2.73 to +3.31 (sign unidentified)
- p_midsize: 0.002–0.179 (90× range)

These parameters are not learning anything stable about jump structure. The moderate tails in the clean data are better described by continuous heavy-tailed distributions (Student-t) than by discrete jump mechanisms (Merton/Kou).

#### Finding 6: No temporal clustering of extremes.

Jump clustering diagnostics show no significant ACF at any threshold for any bucket (all p > 0.05). Self-exciting and regime-switching extensions are not justified. Idiosyncratic extremes are temporally independent under normal platform operation.

#### Finding 7: All models produce zero simulated tail mass.

Every converged model produces p_abs_gt_3 = 0 and p_abs_gt_5 = 0 in simulation for the large bucket (empirical targets: 0.0096 and 0.0002). The exception is C1, which overshoots (excess kurtosis ~200, driven by the uncalibrated factor scale). This is a simulation framework issue, not a distributional model issue. Likely causes: rank re-sorting after increment application compresses the rank-slot distribution; entry/exit censors extreme negative observations.

#### Finding 8: Rank-slot tails are much lighter than identity-based tails.

| Bucket | Rank-slot p_abs_gt_3 | Identity-based p_abs_gt_3 |
|--------|---------------------|--------------------------|
| large | 0.010 | 0.337 |
| midsize | 0.006 | 0.691 |
| small | 0.017 | 0.620 |

The rank-slot view "averages out" individual volatility. All estimation must target the rank-slot distribution.

### 1.5 Why Buckets Are Wrong

The three buckets (large, midsize, small) are arbitrary discretizations of a smooth rank axis. The A1 Student-t estimates show df varying from 8.78 to 15.7 to 3.54 — a pattern that is obviously non-monotonic with rank and demands smooth modeling. The buckets:

1. Create artificial discontinuities at arbitrary boundaries
2. Prevent characterizing *how* the innovation distribution varies with rank
3. Force parameter counts proportional to number of buckets (5 × 3 = 15 for Kou)
4. Are inconsistent with the existing smooth functions for drift mu_hat(k) and volatility sigma_hat(k)

The correct approach: parameterize all innovation parameters as smooth functions of log-rank or log-sigma_hat(k).

---

## 2. Mathematical Framework

### 2.1 Full Model

The rank-slot increment for rank k at week t:

$$\Delta \log w_{k,t} = \hat{\mu}(k) + \hat{\sigma}(k) \cdot z_{k,t}$$

where mu_hat(k) and sigma_hat(k) are already estimated in `sm_params`, and:

$$z_{k,t} = \beta_k F_t + \varepsilon_{k,t}$$

with:
- beta_k: PCA factor loading for rank k (from `pca_cache$beta_k`)
- F_t: common factor (Gaussian)
- eps_{k,t}: idiosyncratic component with rank-dependent distribution

### 2.2 Factor Distribution

$$F_t \sim N(0, s_F^2)$$

One free parameter: the scale s_F. Starting value: s_F ≈ 14 from C1, but this will change substantially once the factor is properly calibrated.

**Optional Student-t extension:** Also fit F_t ~ t_{nu_F}(0, s_F) and compare via AIC. The clean data strongly suggests nu_F is large (>30), so the Gaussian should win. Implement both and let the data decide.

### 2.3 Idiosyncratic Component: C1.5 (Smooth Student-t)

$$\varepsilon_{k,t} \sim t_{\nu(k)}(0, s(k))$$

with smooth rank-dependent parameters:

| Parameter | Link | Functional form | Free params |
|-----------|------|----------------|-------------|
| nu(k) | log(nu − 2) | log(nu(k) − 2) = a0 + a1 * log(k) | 2 |
| s(k) | log | log(s(k)) = b0 + b1 * log(k) | 2 |

**Total: 4 free idiosyncratic parameters** (+ 1–2 factor parameters = 5–6 total).

The nu − 2 transform ensures nu > 2 (finite variance). The current bucket estimates provide validation targets:

- At k ≈ 5 (large): nu ≈ 8.78, so log(8.78 − 2) = log(6.78) = 1.91
- At k ≈ 100 (midsize): nu ≈ 15.7, so log(15.7 − 2) = log(13.7) = 2.62
- At k ≈ 5000 (small): nu ≈ 3.54, so log(3.54 − 2) = log(1.54) = 0.43

Note: this is **non-monotonic** in log-rank — df peaks in the midsize range and declines toward both extremes. A linear-in-log-rank specification won't capture this. Options:

**(a) Quadratic specification:**

$$\log(\nu(k) - 2) = a_0 + a_1 \log k + a_2 (\log k)^2$$

This adds 2 parameters (6 total idiosyncratic) and can capture the hump.

**(b) Condition on sigma_hat(k) instead of k:**

$$\log(\nu(k) - 2) = a_0 + a_1 \log \hat{\sigma}(k)$$

This may produce a monotonic relationship (higher local volatility → heavier tails), which would work with the linear specification. The rationale: tail behavior scales with the level of volatility, not with rank per se.

**Implement both (a) and (b) and compare.** Start with (b) as the theoretically motivated default.

### 2.4 Idiosyncratic Component: C2 (Smooth Kou)

$$\varepsilon_{k,t} = \sigma_\varepsilon(k) W_t + J_{k,t}$$

where W_t ~ N(0,1) and J_{k,t} is a Kou jump:
- Jump with probability p(k)
- Jump size: with prob pi_pos(k), Y ~ Exp(eta_pos(k)); else Y ~ −Exp(eta_neg(k))

Smooth parameter functions (linear in covariate x = log(k) or log(sigma_hat(k))):

| Parameter | Link | Free params |
|-----------|------|-------------|
| p(k) | logit | 2 |
| sigma_eps(k) | log | 2 |
| pi_pos(k) | logit | 2 |
| eta_pos(k) | log | 2 |
| eta_neg(k) | log | 2 |

**Total: 10 free idiosyncratic parameters** (+ 1–2 factor = 11–12 total).

**Important caveat:** Given that B2 Kou failed to converge on this data with 4 free parameters per bucket, the 10-parameter smooth Kou may also struggle. The C1.5 smooth Student-t is the safer bet and should be implemented first. C2 is an incremental test: does the discrete jump mechanism add anything beyond the continuous Student-t tails?

---

## 3. File Layout

```
R/
  model_c2_twostage.R      # NEW — Stages 1–4 (factor extraction, residuals, assembly)
  model_c2_smooth.R         # NEW — Smooth parameter functions, likelihoods, optimization
  model_c2_simulate.R       # NEW — Increment generator for simulation
  model_zoo.R               # MODIFY — Add C1.5 and C2 entry points
  kou_em.R                  # EXISTING — Reuse for bucketed validation
  factor_fit.R              # EXISTING — Reuse/fix for Stage 1
  tail_diagnostics.R        # EXISTING — Reuse for validation
analysis/
  04_model_variants.Rmd     # MODIFY — Add C1.5/C2 calls + diagnostics
  05_model_zoo.Rmd          # MODIFY — Add to comparison
```

---

## 4. Phase 1: Fix the C1 Residual Pipeline

**Priority: Must do first. Estimated effort: 1–2 hours.**

### 4.1 Problem

C1 estimates the factor distribution on F_hat, then fits the idiosyncratic Student-t to the RAW standardized increments z_{k,t} instead of residuals eps_{k,t} = z_{k,t} − beta_k * F_hat_t.

### 4.2 Fix

Locate the C1 estimation code (likely in `model_zoo.R` or `factor_fit.R`). Find where the idiosyncratic Student-t is fitted. Change the input from raw increments to residuals.

```r
# CURRENT (broken):
# 1. Extract F_hat from cross-section of z_{k,t}
# 2. Fit Student-t to F_hat → factor_fit
# 3. Fit Student-t to z_{k,t} by bucket → idio_fit   ← BUG

# FIXED:
# 1. Extract F_hat from cross-section of z_{k,t}
# 2. Fit Student-t to F_hat → factor_fit
# 3. Compute eps_{k,t} = z_{k,t} − beta_k * F_hat_t
# 4. Fit Student-t to eps_{k,t} by bucket → idio_fit  ← CORRECT
```

### 4.3 Validation

After fixing, re-run C1 and check:

1. **Idiosyncratic df values should change relative to A1.** The direction depends on how much variance the factor absorbs. If the factor captures common tail events, residual df should increase (lighter tails). If the factor mostly captures level shifts, df may stay similar but scale should decrease.
2. **Idiosyncratic scale values should decrease.** The factor absorbs variance, so var(eps) < var(z).
3. **The factor scale should decrease** once the estimation is aware that the factor and idiosyncratic components need to jointly explain the data. The current scale = 14.0 likely reflects the factor trying to explain total variance including the idiosyncratic component.
4. **If parameters remain identical to A1**, the factor loadings are effectively zero or the factor extraction is producing near-zero values. Check: `summary(pca_cache$beta_k[1:K_pca])` and `summary(stage1$F_hat$F_hat)`.

### 4.4 Output

A corrected C1 that serves as the baseline for the smooth extensions. The corrected bucket-level estimates become validation targets for the smooth parameter curves.

---

## 5. Phase 2: Two-Stage Estimation with Smooth Parameters

### 5.1 Stage 1: Factor Extraction and Distribution

#### Purpose

Extract F_hat(t) from the cross-section of standardized increments and fit a Gaussian (and optionally Student-t) distribution.

#### Algorithm

**Step 1a: Pivot and extract factor.**

```r
# Pivot standardized increments to weeks × ranks
z_wide <- rank_inc_std %>%
  filter(rank <= K_pca) %>%
  select(week, rank, dlogw_std) %>%
  pivot_wider(names_from = rank, values_from = dlogw_std)

# Drop weeks with any missing ranks
z_mat <- z_wide %>% drop_na() %>% arrange(week)
weeks_used <- z_mat$week
X <- z_mat %>% select(-week) %>% as.matrix()

# OLS projection onto loading vector
beta <- pca_cache$beta_k[1:K_pca]
beta_norm_sq <- sum(beta^2)
F_hat <- as.numeric(X %*% beta) / beta_norm_sq

F_hat_df <- tibble(week = weeks_used, F_hat = F_hat)
```

**Step 1b: Fit factor distribution.**

```r
# Gaussian (1 parameter: scale)
gauss_fit <- list(
  type = "gaussian",
  scale = sd(F_hat),
  loglik = sum(dnorm(F_hat, 0, sd(F_hat), log = TRUE)),
  df = Inf,
  n = length(F_hat)
)

# Student-t (2 parameters: df, scale)
fit_factor_t <- function(F_vec, df_init = 30, scale_init = NULL) {
  if (is.null(scale_init)) scale_init <- sd(F_vec)
  neg_loglik <- function(par) {
    nu <- exp(par[1]) + 2   # ensures nu > 2
    s <- exp(par[2])        # ensures s > 0
    -sum(dt(F_vec / s, df = nu, log = TRUE) - log(s))
  }
  opt <- optim(
    par = c(log(df_init - 2), log(scale_init)),
    fn = neg_loglik, method = "L-BFGS-B", hessian = TRUE
  )
  nu <- exp(opt$par[1]) + 2
  s <- exp(opt$par[2])
  list(type = "student_t", df = nu, scale = s,
       loglik = -opt$value, converged = opt$convergence == 0,
       hessian = opt$hessian, n = length(F_vec))
}

t_fit <- fit_factor_t(F_hat)

# Select by AIC (Gaussian has 1 param, Student-t has 2)
aic_gauss <- -2 * gauss_fit$loglik + 2
aic_t <- -2 * t_fit$loglik + 4
factor_fit <- if (aic_gauss <= aic_t) gauss_fit else t_fit
```

#### Function Signature

```r
#' Stage 1: Extract factor and fit distribution
#'
#' @param z_long     Tibble (week, rank, dlogw_std).
#' @param beta_k     Numeric vector of PCA loadings (length >= K_pca).
#' @param K_pca      Integer, number of ranks to use.
#' @param df_init    Starting value for Student-t df (default 30).
#'
#' @return List:
#'   - F_hat: tibble (week, F_hat)
#'   - factor_fit: list (type, df, scale, loglik, converged, n)
#'                 type is "gaussian" or "student_t"
#'   - aic_comparison: tibble (model, aic) for Gaussian vs Student-t
#'   - beta_used: numeric vector of loadings used
#'   - diagnostics: list (n_weeks_used, n_weeks_dropped, F_hat_mean,
#'                        F_hat_sd, F_hat_skew, F_hat_kurt)
stage1_extract_factor <- function(z_long, beta_k, K_pca, df_init = 30)
```

#### Validation

1. F_hat should have mean ≈ 0 and sd close to the fitted scale.
2. QQ-plot of F_hat vs fitted distribution should track the 45-degree line.
3. n_weeks_dropped should be < 10% of total weeks. On the clean data (~80 weeks), losing more than 8 is concerning.
4. Shapiro-Wilk test on F_hat: if p > 0.05, the Gaussian factor is justified.

---

### 5.2 Stage 2: Residual Computation

#### Algorithm

```r
# For each (k, t): eps_{k,t} = z_{k,t} − beta_k * F_hat_t
residuals <- z_long %>%
  inner_join(F_hat_df, by = "week") %>%
  mutate(
    beta = beta_k[rank],
    eps_hat = dlogw_std - beta * F_hat
  )
```

For ranks k > K_pca, beta_k ≈ 0, so eps_hat ≈ dlogw_std. This is correct — the factor has negligible loading on distant ranks.

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

Run these diagnostics and store for the report:

1. **Factor-residual orthogonality.** For a grid of ranks (1, 5, 10, 25, 50, 100, 500, 1000, 5000, 10000): cor(F_hat, eps_hat) should be ≈ 0. Plot as function of rank.

2. **Variance reduction.** var(eps_hat) / var(dlogw_std) by rank. Should be < 1 where beta_k is non-negligible.

3. **Tail diagnostics.** Run `tail_skew_diagnostics()` on residuals by bucket and compare to raw:

```r
tail_residuals <- tail_skew_diagnostics(residuals %>% rename(dlogw_std = eps_hat), ...)
# Compare tail_residuals vs tail_rank
```

4. **Residual distribution by rank.** QQ plots of eps_hat against normal for a few representative ranks. These are the data that Stage 3 will model.

---

### 5.3 Stage 3A: Smooth Student-t (C1.5)

**Implement this first.** It is simpler, more likely to converge, and captures the clean data's empirical structure.

#### 5.3.1 Log-Likelihood

The Student-t density for residual eps with df = nu and scale = s:

```r
log_student_t <- function(eps, nu, s) {
  dt(eps / s, df = nu, log = TRUE) - log(s)
}
```

This is numerically stable out of the box (R's `dt` handles all edge cases).

#### 5.3.2 Smooth Parameter Functions

```r
#' Evaluate smooth Student-t parameters at given ranks
#'
#' @param ranks  Integer vector.
#' @param theta  Named numeric vector of smooth coefficients.
#' @param x_func Function mapping rank → covariate (default: log).
#' @param quadratic Logical, include quadratic term (default FALSE).
#'
#' @return Tibble (rank, nu, scale_idio)
smooth_t_params <- function(ranks, theta, x_func = log, quadratic = FALSE) {
  x <- x_func(ranks)

  if (quadratic) {
    log_nu_minus_2 <- theta["a0"] + theta["a1"] * x + theta["a2"] * x^2
    log_s          <- theta["b0"] + theta["b1"] * x + theta["b2"] * x^2
  } else {
    log_nu_minus_2 <- theta["a0"] + theta["a1"] * x
    log_s          <- theta["b0"] + theta["b1"] * x
  }

  tibble(
    rank = ranks,
    nu = exp(log_nu_minus_2) + 2,
    scale_idio = exp(log_s)
  )
}
```

#### 5.3.3 Joint Log-Likelihood

```r
#' Smooth Student-t negative log-likelihood
#'
#' @param theta   Named numeric vector of coefficients.
#' @param eps_df  Tibble (rank, eps_hat).
#' @param x_func  Covariate function.
#' @param quadratic Include quadratic terms.
#'
#' @return Scalar negative log-likelihood.
neg_loglik_smooth_t <- function(theta, eps_df, x_func = log, quadratic = FALSE) {
  # Precompute per-rank parameters
  unique_ranks <- sort(unique(eps_df$rank))
  par_df <- smooth_t_params(unique_ranks, theta, x_func, quadratic)

  # Join to observations (use data.table or keyed join for speed)
  eps_with_par <- eps_df %>% inner_join(par_df, by = "rank")

  # Per-observation log-density
  ll <- with(eps_with_par, dt(eps_hat / scale_idio, df = nu, log = TRUE) - log(scale_idio))

  -sum(ll, na.rm = TRUE)
}
```

**Performance note:** With the clean data (~80 weeks × up to 12,000 ranks = ~960K observations), each likelihood evaluation should be fast. If using the full K_cut, precompute the rank → parameter lookup as a named vector for O(1) access.

#### 5.3.4 Starting Values

Derive from the corrected C1 bucket estimates (after Phase 1 fix). For the initial implementation before Phase 1, use the A1 estimates as a proxy:

```r
# A1 bucket estimates (clean data):
# large  (k ≈ 5):    df = 8.78,  scale = 0.871
# midsize (k ≈ 100):  df = 15.7,  scale = 0.882
# small  (k ≈ 5000): df = 3.54,  scale = 0.577

# For covariate x = log(k):
# log(df - 2) at three points:
# k=5:    log(6.78) = 1.91,  x = 1.61
# k=100:  log(13.7) = 2.62,  x = 4.61
# k=5000: log(1.54) = 0.43,  x = 8.52

# Non-monotonic → need quadratic:
# Solve: a0 + a1*x + a2*x^2 through (1.61, 1.91), (4.61, 2.62), (8.52, 0.43)

# For covariate x = log(sigma_hat(k)):
# Need to evaluate sigma_hat at representative ranks.
# If sigma_hat is monotonically related to df, linear may suffice.

# Conservative starting values for quadratic-in-log-rank:
init_theta_t_quad <- c(
  a0 = 0.5, a1 = 0.8, a2 = -0.12,   # df(k): peaks in midrange, declines at extremes
  b0 = -0.1, b1 = 0.0, b2 = -0.005  # scale(k): roughly constant, slight decrease
)

# For linear-in-log-sigma:
init_theta_t_sigma <- c(
  a0 = 2.0, a1 = -0.5,    # higher volatility → lower df (heavier tails)
  b0 = -0.1, b1 = -0.05   # scale decreases with volatility
)
```

#### 5.3.5 Optimization

```r
#' Stage 3A: Fit smooth Student-t to residuals
#'
#' @param eps_long    Tibble (week, rank, eps_hat).
#' @param init_theta  Named numeric starting values (or NULL for defaults).
#' @param x_func      Covariate function (default: log; alt: function(k) log(sigma_hat(k))).
#' @param quadratic   Include quadratic terms (default FALSE for log-sigma, TRUE for log-rank).
#' @param max_iter    L-BFGS-B max iterations (default 500).
#'
#' @return List:
#'   - theta: named numeric vector of optimal coefficients
#'   - loglik: scalar log-likelihood at optimum
#'   - converged: logical
#'   - hessian: matrix
#'   - n_params: integer (4 for linear, 6 for quadratic)
#'   - n_obs: integer
#'   - aic, bic: scalar
#'   - param_curves: tibble (rank, nu, scale_idio) evaluated at 1:K_cut
#'   - diagnostics: list (optimization trace, gradient norm)
stage3a_fit_smooth_t <- function(eps_long, init_theta = NULL,
                                  x_func = log, quadratic = FALSE,
                                  max_iter = 500L)
```

Use `optim()` with method = "L-BFGS-B" or `nlminb()`. The parameter space is unconstrained (link-function space). Add wide bounds (±20) to prevent overflow.

If convergence is difficult:
1. Try `nlminb()` (often more robust than L-BFGS-B for moderate dimensions)
2. Provide multiple starting values from a coarse grid
3. Add a small ridge penalty on slope coefficients

#### 5.3.6 Validation

1. **Parameter curves pass through bucket estimates.** Plot nu(k) and s(k) as smooth curves with the three A1 bucket estimates overlaid as points.
2. **QQ residual plots.** For representative ranks, QQ-plot eps_hat against the fitted rank-specific Student-t. Should be tighter than the raw-data QQ plots.
3. **Compare linear-in-log-sigma vs quadratic-in-log-rank** by AIC.
4. **Verify monotonicity or smooth hump shape** of nu(k). Wild oscillations → overfitting.

---

### 5.4 Stage 3B: Smooth Kou (C2)

**Implement only after C1.5 is working.** This tests whether discrete jumps add value beyond continuous heavy tails.

#### 5.4.1 Kou Density (Closed Form)

The density of eps under Kou parameters (p, sigma_eps, pi_pos, eta_pos, eta_neg):

```
f(eps | theta) = (1 − p) * phi(eps; 0, sigma_eps)
               + p * [pi_pos  * f_pos_conv(eps; sigma_eps, eta_pos)
                     + (1 − pi_pos) * f_neg_conv(eps; sigma_eps, eta_neg)]
```

The Gaussian-exponential convolution:

```r
# Numerically stable log-density computation
log_f_pos_conv <- function(eps, sigma, eta) {
  log(eta) + eta * sigma^2 / 2 - eta * eps +
    pnorm(eps / sigma - eta * sigma, log.p = TRUE)
}

log_f_neg_conv <- function(eps, sigma, eta) {
  log(eta) + eta * sigma^2 / 2 + eta * eps +
    pnorm(-eps / sigma - eta * sigma, log.p = TRUE)
}
```

Full log-density (vectorized over observations):

```r
log_kou_density <- function(eps, p, sigma_eps, pi_pos, eta_pos, eta_neg) {
  # Three components in log space
  log_c1 <- log(1 - p) + dnorm(eps, 0, sigma_eps, log = TRUE)
  log_c2 <- log(p) + log(pi_pos) + log_f_pos_conv(eps, sigma_eps, eta_pos)
  log_c3 <- log(p) + log(1 - pi_pos) + log_f_neg_conv(eps, sigma_eps, eta_neg)

  # Log-sum-exp across the three components (per observation)
  max_log <- pmax(log_c1, log_c2, log_c3)
  max_log + log(exp(log_c1 - max_log) + exp(log_c2 - max_log) + exp(log_c3 - max_log))
}
```

**Critical: Vectorize over observations.** Use only vectorized R operations. No per-observation loops.

#### 5.4.2 Smooth Parameter Functions

```r
smooth_kou_params <- function(ranks, theta, x_func = log) {
  x <- x_func(ranks)
  tibble(
    rank = ranks,
    p         = plogis(theta["a0"] + theta["a1"] * x),
    sigma_eps = exp(theta["b0"] + theta["b1"] * x),
    pi_pos    = plogis(theta["c0"] + theta["c1"] * x),
    eta_pos   = exp(theta["d0"] + theta["d1"] * x),
    eta_neg   = exp(theta["e0"] + theta["e1"] * x)
  )
}
```

#### 5.4.3 Starting Values

The B2 estimates are unreliable (non-converged, p at bound). Derive starting values from the C1.5 results instead:

**Strategy:** Start from the C1.5 smooth Student-t fit and convert:
- Set p(k) small everywhere (logit(0.05) = −2.94) with zero slope
- Set sigma_eps(k) from the C1.5 scale curve
- Set pi_pos = 0.5 (symmetric jumps initially)
- Set eta_pos = eta_neg = 1.0 (moderate jump sizes)

```r
# Derive from C1.5 results:
init_theta_kou <- c(
  a0 = -3.0, a1 = 0.0,     # p(k) ≈ 0.05 everywhere initially
  b0 = c15_theta["b0"], b1 = c15_theta["b1"],  # from C1.5 scale
  c0 = 0.0,  c1 = 0.0,     # pi_pos ≈ 0.5
  d0 = 0.0,  d1 = 0.0,     # eta_pos ≈ 1.0
  e0 = 0.0,  e1 = 0.0      # eta_neg ≈ 1.0
)
```

#### 5.4.4 Function Signature

```r
#' Stage 3B: Fit smooth Kou to residuals
#'
#' @param eps_long     Tibble (week, rank, eps_hat).
#' @param init_theta   Named numeric starting values (or NULL to derive from C1.5).
#' @param c15_fit      Optional C1.5 fit object (to derive starting values).
#' @param x_func       Covariate function.
#' @param max_iter     L-BFGS-B max iterations (default 500).
#' @param subsample_frac Fraction of observations for speed (default 1.0).
#'
#' @return List:
#'   - theta: optimal coefficients (10 parameters)
#'   - loglik, converged, hessian, n_params, n_obs, aic, bic
#'   - param_curves: tibble (rank, p, sigma_eps, pi_pos, eta_pos, eta_neg)
#'   - improvement_over_c15: AIC_kou − AIC_t (negative = Kou wins)
stage3b_fit_smooth_kou <- function(eps_long, init_theta = NULL,
                                    c15_fit = NULL, x_func = log,
                                    max_iter = 500L, subsample_frac = 1.0)
```

#### 5.4.5 Key Decision: Is C2 Worth It?

After fitting both C1.5 and C2, compute:

```r
delta_aic <- c2_fit$aic - c15_fit$aic   # negative means C2 is better
delta_bic <- c2_fit$bic - c15_fit$bic
```

If delta_aic > 0 (C1.5 wins), the Kou jump structure is not justified on this data. Report C1.5 as the lead model and note that discrete jumps don't improve over continuous heavy tails.

Given the clean data's moderate tails and B2's convergence failure, **expect C1.5 to win.** The C2 implementation is for completeness and to make the claim empirically rather than assumptively.

---

### 5.5 Stage 4: Assembly

Package the two-stage estimates into a model object compatible with the simulation framework.

```r
#' Stage 4: Assemble fit object
#'
#' @param stage1    Output of stage1_extract_factor().
#' @param stage2    Output of stage2_compute_residuals().
#' @param stage3    Output of stage3a_fit_smooth_t() or stage3b_fit_smooth_kou().
#' @param x_func    Covariate function used in Stage 3.
#' @param model_name "C1.5_smooth_t" or "C2_smooth_kou"
#'
#' @return Fit object:
#'   - type: character
#'   - converged: logical
#'   - factor_fit: list (type, df, scale, loglik, F_hat, beta_k)
#'   - idio_fit: list (theta, loglik, param_curves, n_params)
#'   - loglik: combined
#'   - aic, bic: combined
#'   - n_params: total
#'   - x_func: covariate function
#'   - estimation_method: "two_stage"
stage4_assemble <- function(stage1, stage2, stage3, x_func, model_name)
```

Combined AIC:
```
loglik_total = stage1$factor_fit$loglik + stage3$loglik
n_params_total = n_factor_params + stage3$n_params
AIC = −2 * loglik_total + 2 * n_params_total
BIC = −2 * loglik_total + n_params_total * log(n_total_obs)
```

**Caveat:** Two-stage log-likelihood is approximate (stages not jointly optimized). Note in model table footnote.

---

## 6. Phase 3: Simulation Integration

### 6.1 Increment Generator

```r
#' Generate one-step increments under C1.5 or C2 (smooth)
#'
#' @param K_max     Total number of ranks.
#' @param mean_vec  Smoothed per-rank drift (length K_max).
#' @param sd_vec    Smoothed per-rank volatility (length K_max).
#' @param model     Fit object from stage4_assemble().
#'
#' @return Numeric vector of length K_max: raw dlogw increments.
generate_smooth_increments <- function(K_max, mean_vec, sd_vec, model) {
  beta <- model$factor_fit$beta_k   # length K_max, zero-padded

  # 1. Draw common factor
  if (model$factor_fit$type == "gaussian") {
    F_t <- rnorm(1, 0, model$factor_fit$scale)
  } else {
    F_t <- rt(1, df = model$factor_fit$df) * model$factor_fit$scale
  }

  # 2. Evaluate smooth parameters at all ranks
  par <- model$param_fn(1:K_max)   # precomputed or evaluated on the fly

  # 3. Draw idiosyncratic component
  if (model$type == "smooth_t") {
    # Student-t draws with rank-dependent df and scale
    eps <- rt(K_max, df = par$nu) * par$scale_idio

  } else if (model$type == "smooth_kou") {
    # Gaussian base
    eps <- rnorm(K_max, 0, par$sigma_eps)
    # Kou jumps
    has_jump <- rbinom(K_max, 1, par$p)
    n_j <- sum(has_jump)
    if (n_j > 0) {
      idx <- which(has_jump == 1)
      pos <- rbinom(n_j, 1, par$pi_pos[idx])
      jump_sizes <- ifelse(
        pos,
        rexp(n_j, par$eta_pos[idx]),
        -rexp(n_j, par$eta_neg[idx])
      )
      eps[idx] <- eps[idx] + jump_sizes
    }
  }

  # 4. Combine and unstandardize
  z <- beta * F_t + eps
  dlogw <- mean_vec + sd_vec * z
  return(dlogw)
}
```

### 6.2 Registration in Model Zoo

```r
# Wire C1.5 and C2 into model zoo
for (model_id in c("C1.5_smooth_t", "C2_smooth_kou")) {
  result <- tryCatch({
    # Stages 1–2 (shared)
    s1 <- stage1_extract_factor(rank_inc_std, pca_cache$beta_k, K_pca)
    s2 <- stage2_compute_residuals(rank_inc_std, s1$F_hat, pca_cache$beta_k)

    # Stage 3 (model-specific)
    if (model_id == "C1.5_smooth_t") {
      s3 <- stage3a_fit_smooth_t(s2, x_func = x_func_chosen)
    } else {
      s3 <- stage3b_fit_smooth_kou(s2, c15_fit = c15_result$fit,
                                    x_func = x_func_chosen)
    }

    # Stage 4
    s4 <- stage4_assemble(s1, s2, s3, x_func_chosen, model_id)

    # Simulation
    sim <- simulate_rank_paths(
      w0 = w0_ext, K_cut = K_cut, K_max = K_max,
      T = CFG$sim_T_weeks, n_paths = CFG$sim_n_paths,
      model = s4,
      increment_fn = generate_smooth_increments
    )

    # Score
    list(
      fit = s4,
      sim = sim,
      score = compute_model_scores(sim, emp_cdc, emp_targets, rho_by_k_med),
      tail_sim = compute_sim_tail_metrics(sim, sm_params, bucket_def)
    )
  }, error = function(e) {
    warning(model_id, " failed: ", conditionMessage(e))
    list(fit = list(type = model_id, converged = FALSE))
  })
  results[[model_id]] <- result
}
```

### 6.3 Simulation Tail Suppression

**All existing models produce zero simulated tail mass.** Before trusting C1.5/C2 simulation tail metrics, diagnose the cause:

**Test A: Raw vs smoothed moments.**
```r
k <- 5
z_sim <- rnorm(10000, mean_vec_s[k], sd_vec_s[k])
z_std <- (z_sim - mean_vec_s[k]) / sd_vec_s[k]
mean(abs(z_std) > 3)  # should be ~0.0027 for Gaussian
```

**Test B: Pre- vs post-resorting tails.** After one simulation step, compute rank-slot increments before and after re-ranking. If re-ranking compresses tails, the rank-slot simulation framework structurally can't match empirical tail targets.

**Test C: Entry/exit censoring.** Track what fraction of |z| > 3 events correspond to endpoints that exit the top-K.

**Expected outcome:** Rank re-sorting is the primary mechanism. Document the calibration gap. Present distributional fit (AIC, QQ) and simulation fit (CDC, durable, Xi) as complementary but separate lines of evidence.

---

## 7. Phase 4: Diagnostics, Bootstrap, and Reporting

### 7.1 Report Sections

#### Factor Quality
```r
# 1. F_hat time series (should look like moderate Gaussian noise)
# 2. QQ: F_hat vs Gaussian and vs fitted Student-t
# 3. Shapiro-Wilk test result
# 4. AIC comparison: Gaussian vs Student-t factor
```

#### Residual Quality
```r
# 1. Tail diagnostics table: raw z vs residual eps, by bucket
# 2. Factor-residual correlation by rank (flat near zero)
# 3. Variance ratio var(eps) / var(z) by rank
# 4. QQ of residuals at representative ranks vs normal reference
```

#### Smooth Parameter Curves (KEY PAPER FIGURE)
```r
# Plot nu(k) and scale(k) as smooth curves over log-rank
# Overlay the three corrected-C1 bucket estimates as validation points
# If using log-sigma covariate: plot nu vs sigma_hat(k) instead

param_curves <- smooth_t_params(1:K_cut, theta_opt, x_func_chosen, quadratic)

ggplot(param_curves, aes(rank, nu)) +
  geom_line(color = "steelblue", linewidth = 0.8) +
  geom_point(data = bucket_validation, aes(rep_rank, nu_bucket),
             color = "red", size = 3) +
  scale_x_log10() +
  labs(title = "Innovation degrees of freedom nu(k)",
       subtitle = "Smooth curve with bucket estimates overlaid",
       x = "Rank (log scale)", y = "Student-t df") +
  theme_bw()
```

If C2 is also fitted, plot all 5 Kou parameter curves similarly.

#### Covariate Comparison
```r
# Table: AIC for log-rank (linear), log-rank (quadratic), log-sigma (linear)
# The theoretically motivated covariate should win or tie
```

#### Model Comparison
```r
# 1. Updated model table with C1.5 and C2 rows
# 2. CDC overlay: Empirical vs Gaussian baseline vs C1.5 vs C2
# 3. QQ grid: add C1.5 (and optionally C2) to the model × bucket display
# 4. Tail metrics scatter: all models
# 5. AIC comparison: A1 (bucketed) vs C1.5 (smooth) — the smooth version
#    should have equal or better AIC with fewer effective parameters
```

### 7.2 Bootstrap

Block bootstrap resampling weeks in contiguous blocks of length L.

```r
#' Block bootstrap for smooth two-stage parameters
#'
#' @param z_long       Standardized increments.
#' @param beta_k       PCA loadings.
#' @param K_pca        Number of PCA ranks.
#' @param x_func       Covariate function.
#' @param model_type   "smooth_t" or "smooth_kou"
#' @param B            Number of resamples (default 200).
#' @param block_length Block length in weeks (default 8).
#' @param seed         Random seed.
#'
#' @return Tibble (param, q05, q50, q95).
bootstrap_smooth <- function(z_long, beta_k, K_pca, x_func = log,
                              model_type = "smooth_t",
                              B = 200L, block_length = 8L, seed = 42L) {
  set.seed(seed)
  all_weeks <- sort(unique(z_long$week))
  T_total <- length(all_weeks)
  n_blocks <- ceiling(T_total / block_length)

  boot_results <- vector("list", B)
  for (b in seq_len(B)) {
    # Block resample
    block_starts <- sample(seq_len(T_total - block_length + 1), n_blocks, replace = TRUE)
    boot_weeks <- unlist(lapply(block_starts, function(s) all_weeks[s:(s + block_length - 1)]))
    boot_weeks <- boot_weeks[1:T_total]

    # Subset data
    z_boot <- z_long %>% filter(week %in% boot_weeks)

    # Re-run pipeline
    s1 <- stage1_extract_factor(z_boot, beta_k, K_pca)
    s2 <- stage2_compute_residuals(z_boot, s1$F_hat, beta_k)
    s3 <- if (model_type == "smooth_t") {
      stage3a_fit_smooth_t(s2, x_func = x_func)
    } else {
      stage3b_fit_smooth_kou(s2, x_func = x_func)
    }

    boot_results[[b]] <- c(factor_scale = s1$factor_fit$scale, s3$theta)
  }

  # Summarize
  boot_mat <- do.call(rbind, boot_results)
  tibble(
    param = colnames(boot_mat),
    q05 = apply(boot_mat, 2, quantile, 0.05, na.rm = TRUE),
    q50 = apply(boot_mat, 2, quantile, 0.50, na.rm = TRUE),
    q95 = apply(boot_mat, 2, quantile, 0.95, na.rm = TRUE)
  )
}
```

**Key comparison:** Are the C1.5 bootstrap CIs tighter than B1 Merton's? Merton showed 27–90× range on p and unidentified signs on mu_j. The smooth Student-t has 4–6 well-identified parameters (intercepts and slopes with clear interpretations). CIs should be far tighter, confirming the model is learning stable structure.

**Performance:** With ~80 clean weeks and B = 200 resamples, each running Stages 1–3, this will take time. Use `furrr::future_map()` for parallelism. Reduce B to 100 if needed.

---

## 8. Configuration

Add to `config.yml`:

```yaml
c_smooth:
  enabled: true
  K_pca: 200
  factor_df_init: 30              # high because we expect Gaussian
  x_func: "log_sigma_hat"         # "log_rank" or "log_sigma_hat"
  quadratic_log_rank: true         # add quadratic term if x_func = log_rank
  optim_method: "L-BFGS-B"        # or "nlminb"
  optim_max_iter: 500

  # C1.5 Smooth Student-t
  run_smooth_t: true

  # C2 Smooth Kou (only if C1.5 succeeds)
  run_smooth_kou: true
  kou_subsample_frac: 1.0

  # Validation: also run bucketed versions on residuals
  run_bucketed_validation: true

  # Bootstrap
  bootstrap_B: 200
  bootstrap_block_length: 8
```

---

## 9. Success Criteria

### Must-have

1. **Phase 1 complete.** Corrected C1 shows idiosyncratic parameters different from A1.
2. **C1.5 converges.** The smooth Student-t optimization converges and produces a valid fit object.
3. **Smooth curves are physically sensible.** nu(k) is everywhere > 2, scale(k) is positive, no wild oscillations.
4. **CDC fit.** rmse_cdc comparable to Gaussian baseline (within 50%).
5. **Durable change fit.** rmse_durable comparable to Gaussian baseline (within 50%).
6. **Xi fit.** rmse_xi comparable to Gaussian baseline.

### Should-have

7. **C1.5 AIC ≤ A1 AIC.** The smooth model should match or beat the bucketed model — it has fewer parameters and pools information across the rank boundary.
8. **Smooth curves pass through bucket validation points.** nu(k) evaluated at representative bucket ranks should be in the neighborhood of the corrected-C1 bucket estimates.
9. **Factor is Gaussian.** AIC comparison selects Gaussian over Student-t for the factor.
10. **Bootstrap CIs are tight.** Substantially tighter than B1 Merton's 27–90× ranges.
11. **C2 adds little over C1.5.** delta_AIC(C2 − C1.5) ≥ 0 (C1.5 wins or ties), confirming that discrete jumps don't improve over continuous heavy tails on clean data.

### Nice-to-have

12. **Log-sigma covariate outperforms log-rank.** Stronger theoretical interpretation.
13. **Simulated tail mass > 0.** At least one of C1.5/C2 produces nonzero p_abs_gt_3 in simulation.
14. **Smooth curves reveal new structure.** The nu(k) curve shows features not visible in three-bucket discretization (e.g., a local minimum, an inflection point, a plateau).

---

## 10. Implementation Sequence

| Step | Phase | Action | Hours | Dependency |
|------|-------|--------|-------|------------|
| 1 | 1 | Fix C1 residual pipeline | 1–2 | None |
| 2 | 1 | Re-run C1, validate corrected params differ from A1 | 0.5 | Step 1 |
| 3 | 2 | Create `model_c2_smooth.R`: smooth_t_params, neg_loglik_smooth_t | 1–2 | None |
| 4 | 2 | Create `model_c2_twostage.R`: stage1_extract_factor | 1–2 | Step 3 |
| 5 | 2 | Add stage2_compute_residuals + validation diagnostics | 1 | Step 4 |
| 6 | 2 | Add stage3a_fit_smooth_t (C1.5) + optimization | 2–3 | Steps 3, 5 |
| 7 | 2 | Run C1.5, validate against bucket estimates, iterate | 1–2 | Steps 1, 6 |
| 8 | 2 | Add stage3b_fit_smooth_kou (C2) — only if C1.5 works | 2–3 | Step 6 |
| 9 | 2 | Add stage4_assemble for both C1.5 and C2 | 0.5 | Steps 6–8 |
| 10 | 3 | Create `model_c2_simulate.R`: generate_smooth_increments | 1 | Step 9 |
| 11 | 3 | Wire into model zoo, run simulation, compute scores | 1–2 | Step 10 |
| 12 | 3 | Simulation tail suppression diagnostics (Tests A/B/C) | 1 | Step 11 |
| 13 | 4 | Add diagnostic + report blocks to Rmd | 1–2 | Step 11 |
| 14 | 4 | Add bootstrap function | 1–2 | Steps 6–8 |
| 15 | 4 | Full re-render and comparison | 0.5 | All |

**Total: 14–22 hours.**

Critical path: Steps 1 → 4 → 5 → 6 → 7 → 10 → 11 (8–13 hours to first simulation results).

**If C1.5 is sufficient (expected), skip Step 8 and save 2–3 hours.**

---

## 11. Risk Mitigation

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| C1.5 smooth optimization doesn't converge | Low | Student-t likelihood is smooth and well-behaved; try nlminb, multiple starting values, ridge penalty |
| Non-monotonic nu(k) requires quadratic but quadratic overfits | Medium | Compare linear-in-log-sigma vs quadratic-in-log-rank via AIC; penalize quadratic coefficients if needed |
| Factor extraction drops too many weeks | Medium (short clean window) | With only ~80 weeks and K_pca = 200 ranks needing complete data, missing ranks could be common. Lower K_pca to 100 or impute missing ranks with CDC-mean before projection |
| C2 Kou doesn't converge (as B2 didn't) | High | Expected outcome. Report C1.5 as lead model and note that Kou was tested and found not to improve |
| Simulation still produces zero tail mass | High | Document calibration gap; present distributional and simulation evidence separately; note that rank re-sorting mechanically compresses rank-slot tails |
| The 80-week clean window is too short for stable estimation | Medium | Bootstrap CIs will be wider than with full data; this is honest. Emphasize in paper that results are for normal platform operation and note the data quality constraint |
| Smooth curves look implausible at rank extremes (k = 1 or k = 12,000) | Medium | The smooth function extrapolates; add a plot showing the data density by rank to contextualize where the curve is well-supported vs extrapolated. Constrain extrapolation by clamping parameters at rank-range boundaries |

---

## 12. How This Fits the Paper

The paper's contribution on the modeling side is:

1. **The capital distribution curve and its dynamics can be captured by a Gaussian simulation with smoothed rank-dependent moments.** This was already established. The CDC, durable change, and Xi gap structure are insensitive to the innovation distribution — they're driven by the first two moments.

2. **The innovation distribution varies smoothly with rank, not discontinuously.** The smooth nu(k) curve is a new empirical contribution that the bucket analysis cannot make. It characterizes a departure from standard SPT (which predicts rank-independent innovations) and links tail behavior to position in the ranking.

3. **Under normal platform operation, the common factor is Gaussian.** Extreme rank movements are idiosyncratic, not systemic. This is a substantive finding for platform governance.

4. **Discrete jump processes (Merton, Kou) are not identified on clean data.** The moderate tails are better described by continuous heavy-tailed distributions. The jump models' apparent success on contaminated data was an artifact of a measurement failure.

5. **The CrowdTangle breakdown masqueraded as fat-tailed common factor dynamics.** This is a cautionary tale about data quality in platform research and should be discussed explicitly.

The smooth parameter curves become the paper's signature figure for this section — analogous to how the CDC curve is the signature figure for the distributional structure.

---

## Appendix A: Notation Reference

| Symbol | Meaning |
|--------|---------|
| k | Rank (1 = highest engagement) |
| t | Week index |
| w_{k,t} | Share held by whoever occupies rank k at week t |
| dlogw_{k,t} | Log-change in rank-slot share |
| mu_hat(k) | Smoothed per-rank drift |
| sigma_hat(k) | Smoothed per-rank volatility |
| z_{k,t} | Standardized increment: (dlogw − mu_hat) / sigma_hat |
| beta_k | PCA factor loading for rank k |
| F_t | Common factor realization |
| eps_{k,t} | Idiosyncratic residual: z − beta_k * F_t |
| nu(k) | Rank-dependent Student-t degrees of freedom |
| s(k) | Rank-dependent Student-t scale |
| p(k) | Rank-dependent Kou jump probability |
| pi_pos(k) | Prob of positive jump given jump occurs |
| eta_pos(k), eta_neg(k) | Kou exponential decay rates |
| K_cut | Top-K cutoff (12,000) |
| K_max | Extended rank count for simulation |
| K_pca | Ranks used in PCA (typically 200) |
| CDC | Capital Distribution Curve: mean share by rank |

## Appendix B: Inferred File Locations

```
R/
  helpers.R                    # rank_bucket_simple(), assign_bucket()
  smoothing.R                  # moving_average_rank(), LOESS wrappers
  simulation.R                 # simulate_rank_paths(), build_entrant_pool()
  model_zoo.R                  # run_model_comparison(), model dispatcher
  kou_em.R                     # B2 Kou EM estimation
  factor_fit.R                 # C1 factor distribution estimation
  tail_diagnostics.R           # tail_skew_diagnostics(), jump_clustering_diagnostics()
  metrics.R                    # cdc_rmse(), durable_rmse(), xi_rmse()

analysis/
  00_setup.Rmd                 # Loads data, defines parameters
  04_model_variants.Rmd        # Moments, Xi gaps, PCA, simulation, model zoo
  05_model_zoo.Rmd             # Dedicated model zoo report

config.yml                     # CFG parameters
```

## Appendix C: Clean Data Summary Statistics

From the current (clean) model zoo report:

**Tail diagnostics (rank-slot, standardized):**

| Bucket | n | p_abs_gt_3 | p_abs_gt_5 | Delta_tail | excess_kurt | hill_alpha |
|--------|---|------------|------------|------------|-------------|------------|
| large | 4,175 | 0.0096 | 0.0002 | −0.50 | 1.54 | 4.11 |
| midsize | 37,600 | 0.0061 | 0.0 | — | — | — |
| small | 1,960,000 | 0.0168 | 0.0031 | — | — | — |

**A1 Student-t estimates:**

| Bucket | df | scale | loglik |
|--------|-----|-------|--------|
| large | 8.78 | 0.871 | −4,052 |
| midsize | 15.7 | 0.882 | −35,452 |
| small | 3.54 | 0.577 | −1,590,144 |

**C1 Factor estimate:**

| Parameter | Value |
|-----------|-------|
| df | 36.3 |
| scale | 14.0 |
| loglik | −473.8 |

**B1 Merton bootstrap (for comparison to C1.5):**

| Parameter | 5th pctile | 95th pctile | Range ratio |
|-----------|-----------|-------------|-------------|
| p_large | 0.002 | 0.054 | 27× |
| p_midsize | 0.002 | 0.179 | 90× |
| mu_j_midsize | −2.73 | 3.31 | sign unidentified |
