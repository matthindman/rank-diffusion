# Codex spec: bootstrap-style simulator to match heavy tails + micro movement (Facebook rank diffusion)

## 0) Purpose

Implement a bootstrap-based increment generator (and optional factor + tail enhancements) that can replace/augment the current Gaussian rank-slot increment model.

Key goals:

1. **Match heavy tails** in weekly rank-slot increments (`dlogw`) observed in the Facebook weekly panel.
2. **Match micro-level movement** diagnostics used in the current workstream (rank-conditioned future shares, across horizons).
3. Respect a core constraint: **only ~176 weeks** exist, so **per-rank** resampling has limited tail support. Borrow strength across **nearby ranks** (“rank neighborhoods”) while keeping rank-dependence.
4. Keep the implementation **modular**: do not assume existing function/variable names remain unchanged. Provide clear interfaces so refactoring won’t break the substantive logic.

This spec is intended for an R codebase that already:
- Loads the weekly parquet, constructs `endpoint_weekly`, `rank_panel` with `dlogw`, and runs simulators + fit metrics (CDC, durable-change, Xi) and micro movement checks.
- Already contains a *simple* bootstrap simulator (per-rank iid resampling). This spec generalizes it.

---

## 1) Data & objects assumed (interfaces, not names)

### 1.1 Input weekly panel (long)
A data frame with at least:

- `week` (Date) — weekly timestamp
- `endpoint_id` (chr)
- `rank` (int) — within-week rank (1 = largest)
- `share_global` (dbl) — endpoint share of platform (or a `metric_value` that can be normalized to share)

Optional:
- `share_topK` (dbl) — share renormalized within top-K (if already computed)

### 1.2 Rank-slot increments table (long)
A data frame with:

- `week` (Date) — week index for the increment (convention: increment from `week-1` to `week`)
- `rank` (int)
- `dlogw` (dbl) — `log(share_rank(t)) - log(share_rank(t-1))`, computed **within rank-slot** time series.

This is the object the bootstrap resamples.

### 1.3 Parameters (configuration)
A single config list passed through modules (YAML/JSON/`params` list):

- `K_cut` (int): top-K of interest (e.g., 12000)
- `K_max` (int): simulation universe length (e.g., K_cut + buffer)
- `T_sim` (int): number of simulated weeks per path (e.g., 52)
- `n_paths` (int): number of simulated paths
- `entry_frac` (dbl): entry/exit fraction for tail refresh (existing logic)
- `mu_global` (dbl): global drift shift (keep existing)
- `sigma_global` (dbl): global volatility scale (keep existing)

Bootstrap-specific:
- `bootstrap_method` (str): one of  
  - `"iid_rank"` (existing baseline: per-rank iid pool)  
  - `"local_rank"` (recommended baseline improvement)  
  - `"factor_local"` (recommended for peer-review)  
  - `"week_vector"` (alternative)
  - `"factor_local_tailsplice"` (optional)
- `rank_bandwidth` (int): neighborhood half-width in rank units (e.g., 10, 25, 50). Can also support a function of rank; see §4.2.
- `kernel` (str): `"uniform" | "triangular" | "exp"` (weights inside neighborhood)
- `block_bootstrap` (bool): apply time-block resampling to common factors and/or residuals
- `block_length` (int): expected block length in weeks (e.g., 4 or 8)
- `n_factors` (int): number of common shock factors (e.g., 1–3)
- `K_pca` (int): ranks used to estimate common factors (top K_pca ranks; must be mostly complete)
- `tailsplice` (bool): if TRUE, use semi-parametric tail splicing for residuals
- `tail_q` (dbl): tail threshold quantile (e.g., 0.99)
- `tail_fit_min_n` (int): minimum tail exceedances required to fit GPD; otherwise fallback to empirical tail

Diagnostics/tuning:
- `bucket_def` (function or list): mapping from rank -> bucket label (e.g., large/midsize/small)
- `horizons_micro` (int vector): e.g., c(1,4,12)
- `horizons_durable` (int vector): e.g., c(4,8)
- `seed` (int)

---

## 2) Outputs

### 2.1 Increment sampler object
Return a list-like object with:

- `method` (chr)
- `K_max` (int)
- `sample_one_week(mu, sigma)` -> numeric vector length `K_max`  
  - Returns a `dlogw` vector aligned with the **current rank slots** (1..K_max) for use inside the simulator.

Optional:
- `debug_info`: objects for reproducibility (factor loadings, residual pools, etc.)

### 2.2 Simulation output (existing shape)
The simulator should continue returning:

- `snapshots`: tibble with `path, t, rank, share` for ranks 1..K_cut at each simulated week
- `growth`: horizon-based log growth outcomes (existing)
- Optional `xi`: if Xi diagnostics are enabled (existing)

---

## 3) Core design choices to implement

### 3.1 Baseline improvement: **local-rank pooled bootstrap** (`bootstrap_method = "local_rank"`)
Problem: per-rank pools have ~175 observations (weeks), which is too small to represent heavy tails at a single rank reliably.

Solution:
- For each rank slot `r`, define neighborhood `N(r) = {r' : |r'-r| <= h}`.
- Sample increments from the pooled set `{ dlogw(week, r') : r' in N(r) }`.
- Optional kernel weights by distance.

Properties:
- Preserves **rank dependence** (local smoothness).
- Increases effective sample size per rank by factor `(2h+1)`.
- Still simple and defensible.

### 3.2 Peer-review recommended: **factor + local residual bootstrap** (`bootstrap_method = "factor_local"`)
Motivation: evidence of common shocks across ranks (PCA of rank-slot increments is already computed in the master analysis). A method that preserves common shocks will be easier to defend.

Model:
- Let `dlogw(t,r) = a_r + sum_{j=1..J} b_{rj} f_j(t) + e(t,r)`
  - `f_j(t)` are common factors over time (estimated from top `K_pca`)
  - `a_r` is rank drift intercept
  - `b_{rj}` are rank loadings
  - `e(t,r)` are idiosyncratic residuals (heavy-tailed)

Bootstrap generation:
- Resample factor time series `f_j(t)` using **block bootstrap** (optional but recommended).
- Resample residuals `e(t,r)` using **local-rank pooled bootstrap** (as in 3.1).
- Recompose: `dlogw*(t,r) = a_r + Σ b_{rj} f*_j(t) + e*(t,r)`
- Apply global `mu_global` and `sigma_global` as: `mu_global + sigma_global * dlogw*` (or keep `a_r` outside scaling; document the choice).

### 3.3 Optional: tail splicing (`bootstrap_method = "factor_local_tailsplice"` or `tailsplice=TRUE`)
If the empirical sample under-represents extreme tails for the specific ranks of interest:
- For each bucket (or rank neighborhood), fit a GPD to residual exceedances beyond quantile `tail_q`.
- Sample body from empirical residuals; sample tail from fitted GPD; combine with continuity at threshold.

---

## 4) Implementation steps (modules + function-level specs)

### 4.1 Module: `bootstrap_increment_sampler.R`

#### Function: `build_increment_sampler(rank_panel, cfg, bucket_assigner = NULL)`
**Inputs**
- `rank_panel`: long df with `week, rank, dlogw`
- `cfg`: config list (see §1.3)
- `bucket_assigner` (optional): function mapping rank -> bucket label (for tail splicing)

**Output**
- increment sampler object (see §2.1)

**Behavior**
- Switch on `cfg$bootstrap_method` and call corresponding builder:
  - `build_sampler_iid_rank()`
  - `build_sampler_local_rank()`
  - `build_sampler_factor_local()`
  - `build_sampler_week_vector()`

---

### 4.2 Builder: `build_sampler_local_rank(rank_panel, cfg)`

**Precompute**
1. Filter to `rank <= cfg$K_max`.
2. For each `rank`, store its increment vector: `pool_by_rank[[r]]`.
3. For each `rank r`, define neighborhood ranks:
   - If `cfg$rank_bandwidth` is scalar: `N(r) = max(1,r-h) : min(K_max,r+h)`
   - If `cfg$rank_bandwidth` is function: `h = cfg$rank_bandwidth(r)` then as above.
4. Precompute sampling weights for `N(r)` given `cfg$kernel`:
   - uniform: all equal
   - triangular: weight proportional to `1 - |r-r'|/(h+1)`
   - exp: weight proportional to `exp(-|r-r'|/tau)` with `tau = h/2` unless specified

**Sampling**
Provide function:
`sample_one_week(mu, sigma)`:
- For each rank slot `r`:
  1. draw rank index `r'` from `N(r)` with weights
  2. draw `x` from `pool_by_rank[[r']]` with replacement (if empty, use 0)
  3. set `inc[r] = x`
- Return `mu + sigma * inc`

**Performance notes**
- Avoid `purrr::map_dbl` inside the inner loop if possible (use `for` + preallocated numeric vector).
- Precompute neighborhood rank vectors and cumulative weight CDFs to speed up sampling.

---

### 4.3 Builder: `build_sampler_factor_local(rank_panel, cfg)`

#### Step A: build week × rank matrix for factor estimation
1. Choose `K_pca = min(cfg$K_pca, cfg$K_max)`
2. Create wide matrix `X` with rows = weeks, cols = ranks 1..K_pca, values = `dlogw`.
3. Keep weeks with complete data for ranks 1..K_pca:
   - `X_complete = X[complete.cases(X), ]`
   - Store corresponding week indices.

#### Step B: estimate factors
1. Standardize columns if desired (document choice). Recommended:
   - center each rank series; optionally scale by sd to prevent high-volatility ranks dominating PCA.
2. Run PCA:
   - `pca = prcomp(X_complete, center = TRUE, scale. = TRUE)`
3. Set `J = cfg$n_factors`.
4. Extract factor scores: `F = pca$x[, 1:J]` (T_complete × J)

#### Step C: project factors back to all weeks
Because simulation needs factor draws for `T_sim` weeks, you only need the **empirical factor series** to bootstrap from.
- If you want factor values for all weeks (not only complete-case weeks), compute them by:
  - using the PCA rotation/loadings and whatever subset is available, or
  - restrict to complete-case weeks only and bootstrap within that set.
Keep it simple: **use complete-case weeks** (top ranks usually complete).

Store:
- `F_emp`: matrix (n_weeks_factor × J)

#### Step D: fit per-rank loadings and intercepts
For each rank `r = 1..K_max`:
1. Build df of observed `dlogw(t,r)` where `t` is in the factor-week set.
2. Fit OLS regression:
   - `dlogw(t,r) = a_r + Σ b_{rj} f_j(t) + residual`
3. Store `a_r` and `b_r` (length J).
4. Collect residuals `e(t,r)` in a residual pool for this rank.

Notes:
- If a rank has insufficient observations in the factor-week set, fallback:
  - set `b_r = 0` and `a_r = mean(dlogw)`; residuals = demeaned increments.
  - Or interpolate loadings from nearby ranks.

#### Step E: build local pooling of residuals
Same as §4.2, but pools are residuals `e(t,r)` rather than raw `dlogw`.

#### Step F: bootstrap the factor time series (optional block bootstrap)
If `cfg$block_bootstrap` is TRUE:
- Implement **stationary bootstrap** (Politis–Romano):
  - sample blocks with random geometric length with mean `L = cfg$block_length`
  - concatenate until length `T_sim`
- Otherwise sample `F_emp` rows iid with replacement.

Return `F_boot` of dimension `T_sim × J`.

#### Step G: define sampler
`sample_path_increments(mu, sigma, T_sim)`:
- Draw a bootstrapped factor path `F_boot` once per simulated path (or once globally; document choice).
  - Recommended: **once per path** to preserve within-path dependence.
- For each simulated week `t=1..T_sim`:
  - sample residuals locally for each rank `r` (iid across ranks) -> `e_star[r]`
  - compute `inc_raw[r] = a_r + dot(b_r, F_boot[t,]) + e_star[r]`
  - set `dlogw_star[t, ] = mu + sigma * inc_raw`
Return `dlogw_star` (T_sim × K_max)

Then the simulator for each week `t` uses row `t`.

**Simulator integration option**
- Either:
  1) increment sampler exposes `sample_one_week()` and internally maintains factor path state, or
  2) sampler exposes `sample_path(T_sim)` returning a full matrix used in the simulator loop.

Prefer (2) for clarity and speed.

---

### 4.4 Builder: `build_sampler_week_vector(rank_panel, cfg)` (alternative)
Goal: preserve cross-rank dependence nonparametrically by resampling entire weekly increment profiles.

**Precompute**
1. Build matrix `D` with rows = weeks, cols = ranks 1..K_use where `K_use` is chosen such that missingness is low.
2. For ranks with missing values in some weeks:
   - Option A (simple): restrict to `K_use = min(max_rank_by_week)` so no missingness.
   - Option B: impute missing by local-rank median (requires rank smoothing).

**Sampling**
- Sample week rows (iid or blocks) to produce a sequence of length `T_sim`.
- Return those row vectors as the `dlogw` applied each simulated week.

---

## 5) Simulator integration

### 5.1 Make the simulator accept an increment generator
Refactor the simulator so it does NOT assume Gaussian increments.

Recommended interface:

`simulate_rank_paths_generic(w0, cfg, increment_generator, entrant_sampler, metrics_fns)`

Where:
- `increment_generator` is an object with either:
  - `sample_path(T_sim, mu, sigma)` returning matrix `T_sim × K_max`, or
  - `sample_one_week(t, mu, sigma)` returning vector length `K_max`

The simulator loop then:
1. Obtain `dlogw` for that week
2. Update shares: `w <- w * exp(dlogw)`
3. Floor: `w <- pmax(w, eps)`
4. Renormalize: `w <- w / sum(w)`
5. Apply entry/exit (existing logic)
6. Store snapshots; compute growth horizons; compute Xi if requested

No changes to durable-change or CDC code should be required.

---

## 6) Diagnostics & acceptance criteria (must implement)

### 6.1 Heavy tail diagnostics (reproduce master analysis logic)
Implement a function:

`tail_diagnostics(rank_panel, cfg, bucket_assigner) -> tibble`

For each bucket:
- Standardize: `z = (dlogw - mean_bucket)/sd_bucket`
- Compute:
  - `p_gt_3sd = mean(|z|>=3)`
  - `p_gt_5sd = mean(|z|>=5)`
  - `q_0.001`, `q_0.999`
- Output table and QQ plots.

For simulated data:
- Create simulated rank_panel-like increments from simulation output:
  - For each path and week, compute rank-slot `dlogw` if available.
  - Or, if increments are directly generated, store them.

Acceptance targets:
- Match empirical `p_gt_5sd` and extreme quantiles by bucket within tolerance bands you define (e.g., ±20% relative).

### 6.2 Micro movement diagnostics
Reuse existing micro functions if present. If not, implement:

- `emp_micro_for_k(endpoint_weekly, k, horizons)`
- `sim_micro_for_k(sim_snapshots, k, horizons)`
- Optional: `emp_rankslot_micro_for_k(endpoint_weekly, k, horizons)` for apples-to-apples vs rank-slot simulation.

Acceptance targets:
- For representative ranks (e.g., k in {10, 20, ..., 200}):
  - match median future share and 10–90% band at horizons (1,4,12) as closely as feasible.
- Track improvements relative to Gaussian baseline.

### 6.3 Macro fit metrics (already exist)
Continue reporting:
- CDC RMSE
- Durable-change RMSE
- Xi RMSE (if enabled)

Ensure bootstrap variants are scored on the same scoreboard.

---

## 7) Tuning plan (automatable)

Implement a grid or simple search over:
- `rank_bandwidth` (e.g., 5, 10, 25, 50)
- `block_length` (e.g., 2, 4, 8) when using factor_local + block bootstrap
- `n_factors` (e.g., 1, 2, 3)

Objective:
- weighted score combining:
  - tail diagnostic mismatch (bucket-level)
  - micro movement mismatch (RMSE across k-grid)
  - plus existing macro mismatch metrics (CDC, durable)

Do not overfit:
- Optionally reserve a holdout set of weeks for evaluation (e.g., last 26 weeks).

---

## 8) Reproducibility and reporting requirements

- All samplers must respect `cfg$seed`.
- Each run should save:
  - config used
  - method name
  - tuning parameters selected
  - summary tables for tail diagnostics, micro movement, macro fit

Produce a single “model comparison” tibble with one row per method and all metrics.

---

## 9) Minimal unit tests (must include)

1. Dimension checks:
   - sampler returns length `K_max`
   - simulator outputs have expected columns and no NA shares in top-K
2. Invariants:
   - shares are positive and sum to 1 (within tolerance) each simulated week
   - ranks are sorted descending in snapshots
3. Stability:
   - with fixed seed, sampler outputs are deterministic
4. Smoke test:
   - run `n_paths=2`, `T_sim=5` quickly for CI

---

## 10) Implementation notes for Codex

- Use base R loops inside simulation for speed; avoid dplyr inside tight loops.
- Precompute pools as lists of numeric vectors.
- For kernel-weighted neighborhood sampling:
  - precompute per-rank neighborhood index vector and `prob` vector.
- For stationary bootstrap:
  - implement a simple function `stationary_bootstrap_indices(n, L)`.

Suggested file layout:
- `R/increment_samplers.R` (all builders)
- `R/simulator.R` (generic simulator)
- `R/diagnostics_tail.R`
- `R/diagnostics_micro.R`
- `R/fit_metrics.R` (existing)
- `tests/testthat/test_samplers.R`

