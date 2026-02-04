# Spec for heavy-tail / jump / hybrid models for weekly rank dynamics

## 0) Purpose

Implement and evaluate alternatives to Gaussian rank-based diffusion for weekly rank/attention dynamics in the Facebook page panel, with specific emphasis on:

- Capturing **heavy tails** in rank-slot increments (`dlogw`) and/or endpoint-level growth.
- Capturing **common shocks** (cross-rank dependence).
- Optionally capturing **identity-based micro movement** (tracking the same endpoint forward from rank *k* at time *t*).

This spec is designed to be robust to ongoing refactors: it defines **interfaces**, **contracts**, and **model options**, not exact object names or file layouts.

---

## 1) Inputs / required data products

### 1.1 Required base table (weekly endpoint panel)

A data frame (or Arrow table) with at least:

- `week` (Date or integer index, weekly)
- `endpoint_id` (string/int id)
- `metric_value` (nonnegative numeric)
- `rank` (1 = largest metric_value in week; recomputed within week)

Derived columns to compute (do not assume they already exist):

- `share_global = metric_value / sum(metric_value)` within each `week`.
- Choose `K_cut` (top-K universe for analysis) and compute:
  - `share_topK = share_global / sum(share_global[rank<=K_cut])` within each `week` for `rank<=K_cut`.

### 1.2 Rank-slot panel

From the endpoint panel, construct a rank-slot time series panel:

- For each `week` and each `rank` in `1..K_max` (where `K_max >= K_cut` to support entry/exit), define:
  - `w(rank, week)` = the share (global or topK, but be consistent with the simulator output).
  - `log_w = log(pmax(w, eps))`
  - `dlogw(rank, week)` = `log_w(week) - log_w(week-1)` for that rank-slot series.

### 1.3 Buckets / groups

Keep the existing bucket definition (e.g., `large`, `midsize`, `small`) but implement as a reusable function:

- `assign_bucket(rank) -> factor/character`

The modeling should support bucket-level parameterization where needed.

---

## 2) Empirical diagnostics to run before fitting models

Create a diagnostics module that produces the following from the rank-slot panel (`dlogw`) and (optionally) endpoint identity panel.

### 2.1 Heavy-tail diagnostics (rank-slot)

For each bucket:

1) Standardize `dlogw` within bucket:
- `z = (dlogw - mean_bucket) / sd_bucket`

2) Compute tail summaries:
- `p(|z| >= 3)`
- `p(|z| >= 5)`
- extreme quantiles (e.g., `q0.001`, `q0.999`)
- optionally skewness / kurtosis

3) Visuals:
- QQ plot vs Gaussian
- histogram of `z` on log-y scale

### 2.2 Cross-rank dependence / common shocks

- PCA on a wide matrix of `dlogw` for top `K_pca` ranks (complete weeks only).
- Store:
  - variance explained by PC1..PC10
  - time series of PC1 scores (common-shock proxy)
  - rank loadings for PC1 (to parameterize factor loadings)

Also compute “extreme co-movement”:

- For each week, count # of ranks with `|z|>u` (u=3,4,5) and compare to a binomial expectation under independence.

### 2.3 Serial dependence

Per bucket and/or per rank:

- ACF of `dlogw`
- Regression `dlogw(t+1) ~ dlogw(t)` summarized by bucket (slope + CI)
- Optional: `dlogw(t+1) ~ dlogw(t) + PC1(t)`

### 2.4 Micro object definitions (identity vs rank-slot)

Implement two parallel micro objects:

**(A) Identity-based micro object (as in v12)**
- For rank `k` and horizon `h`:
  - Identify endpoint at rank `k` at time `t`, then track *same* endpoint to `t+h`.
  - Summaries: retention, median, p10/p90, etc.

**(B) Rank-slot comparator**
- For rank `k` and horizon `h`:
  - Use share at rank `k` at time `t+h` (no identity tracking).
  - Summaries: median, p10/p90, etc.

This is required so simulation can be validated on the correct target (rank-slot vs identity-based).

---

## 3) Model family: menu of heavy-tail / jump / hybrid approaches

All models below should plug into a common simulation engine via an **innovation generator**.

### 3.1 Common simulation interface

Define a function signature (names flexible):

```r
simulate_paths(
  w0,              # numeric vector length K_max, sums to 1
  T,               # number of weekly steps
  n_paths,
  K_cut, K_max,
  entry_frac,      # fraction of mass replaced by entrants per step (or other rule)
  entrant_sampler, # function(n)-> numeric shares (positive)
  model,           # list defining innovation model & parameters
  moment_curves,   # rank-smoothed mean/sd curves and any other rank curves
  seed = NULL,
  store = list(snapshots = TRUE, xi = FALSE, diagnostics = TRUE)
) -> list(snapshots=..., growth=..., xi=..., diagnostics=...)
```

Contract:
- Output `snapshots` has columns: `path`, `t`, `rank`, `share` (and optionally `id` if identity-tracking is enabled).
- Each snapshot at a fixed `(path,t)` must sum to 1 over ranks `1..K_cut` (topK space) and also be valid on `1..K_max` as used internally.

### 3.2 Innovation generator contract

Implement:

```r
draw_increments(
  t,
  w_t,               # current shares (length K_max)
  moment_curves,     # includes rank-dependent mean/sd curves
  model,             # type + parameters
  cache              # optional precomputed objects (e.g., PCA loadings)
) -> numeric vector dlogw length K_max
```

This is the only part that differs across models.

---

## 4) Recommended primary model (peer-review friendly)

### 4.1 Model: factor + t innovations + explicit jump component (discrete-time jump model)

For each week `t` and rank `k`:

\[
\Delta \log w_{k,t}
= \mu_k
+ \beta_k F_t
+ \sigma_k \varepsilon_{k,t}
+ J^{sys}_{k,t}
+ J^{idio}_{k,t}.
\]

Where:

- `mu_k` = smoothed per-rank mean curve (existing)
- `sigma_k` = smoothed per-rank sd curve (existing)
- `F_t` = common factor shock (heavy-tailed)
- `beta_k` = factor loading by rank (from PCA loadings, smoothed)
- `eps_{k,t}` = idiosyncratic heavy-tailed innovation (Student-t or skew-t)
- `J_sys` = systemic jump (rare, heavy-tailed) applied to many ranks
- `J_idio` = idiosyncratic jumps (rarer, possibly rank-dependent)

Implementation details:

#### 4.1.1 Common factor `F_t`

Start with:
- `F_t ~ scaled Student-t(df_F)` with mean 0, var 1
- optionally add AR(1): `F_t = phi_F * F_{t-1} + eta_t` (eta_t t-distributed)

#### 4.1.2 Idiosyncratic innovations

- `eps_{k,t} ~ Student-t(df_eps)` (optionally bucket-specific `df_eps[bucket(k)]`)

#### 4.1.3 Jump indicators

Two Bernoulli processes:

- Systemic: `I_sys,t ~ Bernoulli(p_sys)`  
- Idiosyncratic: `I_idio,k,t ~ Bernoulli(p_idio(bucket(k)) or smooth(p_idio(k)))`

Jump sizes (choose one of the following families; keep modular):
- **Double exponential** (Kou-style): sign ~ Bernoulli(0.5) or estimated; magnitude ~ Exp(rate)
- **Student-t** jump size: `t(df_J)` scaled
- **Generalized Pareto** (EVT-tail) for magnitudes above threshold

Apply:
- If `I_sys,t=1`, add `J_sys_k,t = a_k * S_sys,t` where `S_sys,t` is the systemic jump size and `a_k` is a rank loading (e.g., proportional to `sigma_k` or PC1 loading).
- If `I_idio,k,t=1`, add `J_idio_k,t = S_idio,k,t`.

#### 4.1.4 Ensuring stationarity / CDC consistency

Two options; implement at least one:

**Option A (recommended): mean-reversion in deviations from baseline CDC**
- Precompute baseline `log_wbar_k`.
- Simulate `y_k,t = log w_k,t - log_wbar_k`
- Update `y_k,t+1 = (1 - kappa_k) * y_k,t + innovations`
- Convert back: `log w_k,t+1 = log_wbar_k + y_k,t+1`

`kappa_k` can be bucket-specific or smooth-by-rank, and can be calibrated using the martingale regression slope diagnostics.

**Option B: drift adjustment**
- Adjust `mu_k` each step so that `E[sum_k w_k]` remains normalized and mean CDC matches.

---

## 5) Alternatives (also acceptable; implement as additional `model$type`)

### 5.1 Pure heavy-tailed innovations (minimal change)

- Same as existing simulator but replace `rnorm()` with `rt(df)` and scale to match `sigma_k`.

Pros: simplest and often enough to match `p(|z|>5)`.  
Cons: no explicit “jump” mechanism; may under-fit cross-rank co-jumps.

### 5.2 Two-component mixture (diffusion + jump regime)

For each `(k,t)`:

- With prob `(1-p_jump_k)`: `eps ~ N(0,1)` (or t with high df)
- With prob `p_jump_k`: `eps ~ N(0, s_jump^2)` (or t with low df)

This is a discrete-time analogue of jump diffusion; easy to estimate via EM (or match tail targets).

### 5.3 Markov-switching (regime persistence)

A latent regime `S_t in {calm, turbulent}` evolves via a 2-state Markov chain.

- Calm regime: smaller volatility, maybe Gaussian/t
- Turbulent regime: large volatility and/or high jump intensity

Useful if you see clustering in weekly extremes.

### 5.4 EVT hybrid (bootstrap bulk + GPD tail)

- For `|z| <= u`: resample empirical standardized residuals (bootstrap)
- For `|z| > u`: sample exceedances from fitted GPD (POT) separately for left/right tails

This is a strong option when you want tail realism without strong parametric assumptions.

### 5.5 Rank-based interacting particle system with jumps (identity-aware “second-order” option)

If identity-based micro movement is a *primary* target, implement an identity-tracking simulator:

- Simulate `N = K_max` particles with fixed ids.
- Update each particle’s log-share using rank-dependent terms (and optional id-dependent terms):
  - `alpha_id` (name effect) + `beta_rank` (rank effect)
  - volatility `sigma_rank`
  - Lévy / jump innovations as above
- Re-normalize and re-rank each step.

This is the discrete-time analogue of “second-order” / “hybrid Atlas” models.

---

## 6) Estimation / calibration strategy (simulation-based)

Likelihood-based inference is generally hard with ranking + entry/exit. Use simulation-based fitting.

### 6.1 Targets (objective terms)

Reuse existing fit metrics and add tail/common-shock terms.

**Macro targets**
- CDC RMSE (log-scale recommended)
- durable change RMSE (median abs Δlog share at horizons 4 and 8 by bucket)
- Xi RMSE (if using Xi calibration)

**Tail targets (new)**
- bucket-wise `p(|z|>3)` and `p(|z|>5)`
- extreme quantiles `q0.001`, `q0.999`

**Dependence targets (new)**
- PC1 variance explained (or fraction explained by first few PCs)
- distribution of weekly exceedance counts

**Micro targets (optional but recommended)**
- identity-based median and p10/p90 for selected ranks and horizons
- retention by rank and horizon

### 6.2 Parameterization to avoid overfit

Keep the parameter count small:

- Use bucket-level parameters for df and jump rates (`df_eps[b]`, `p_idio[b]`).
- Use a small number of global parameters (`p_sys`, `df_F`, `phi_F`, jump scale).
- Loadings `beta_k` derived from PCA then smoothed, not free per rank.

### 6.3 Optimization approach

- Use a black-box optimizer (Nelder–Mead, CMA-ES, Bayesian optimization) over a small parameter set.
- Each objective evaluation runs `simulate_paths()` with moderate `n_paths`.
- Add a “fast mode” for calibration (smaller K_max, fewer paths) and a “full mode” for final reporting.

### 6.4 Uncertainty: block bootstrap

Because weeks are dependent and common shocks exist:

- Bootstrap in **blocks of contiguous weeks** (moving block bootstrap) for empirical target estimation.
- For parameter uncertainty: refit on each bootstrap replicate.
- Report CIs for tail metrics and for calibrated parameters.

---

## 7) Validation and reporting outputs

### 7.1 Standard report tables

For each model:

- Macro RMSE scoreboard (CDC, durable, Xi)
- Tail summary table by bucket: empirical vs simulated
- Dependence summary: PC variance explained, exceedance-count distribution
- Micro summary for k-grid: RMSE, retention

### 7.2 Plots

- CDC empirical vs simulated
- QQ plots by bucket (empirical vs simulation)
- Time series of PC1 (empirical) vs simulated factor proxy
- Micro movement plots (identity vs rank-slot vs simulation) for selected k

### 7.3 Reproducibility

- Every run must be controlled by a single `config` list and a single RNG seed.
- Save config, fitted params, and output summaries as JSON/CSV alongside figures.

---

## 8) Engineering requirements (for modular refactor)

### 8.1 Files / modules (suggested)

- `R/diagnostics_tail.R`
- `R/diagnostics_dependence.R`
- `R/model_innovations.R` (all `draw_increments_*` implementations)
- `R/sim_engine.R`
- `R/calibration.R`
- `R/evaluation.R`
- `R/reporting.R`

### 8.2 Unit tests (minimum)

- `draw_increments()` returns finite numeric vector length K_max.
- `simulate_paths()` returns valid probability vectors (positive, sums to 1 in topK).
- Tail metrics computed on simulated increments reproduce analytical expectations in toy cases (e.g., pure t).
- Identity-tracking simulator preserves ids and produces consistent ranks.

---

## 9) Acceptance criteria (definition of “done”)

A model is considered acceptable if, on a held-out period:

1) CDC RMSE and durable RMSE are not worse than the current Gaussian baseline by more than a small tolerance.
2) Tail targets: `p(|z|>5)` and `q0.999/q0.001` match empirical within bootstrap CIs.
3) Dependence: PC1 variance explained and weekly exceedance clustering are closer to empirical than baseline.
4) If identity micro is in scope: rank-conditioned identity forward-share medians and bands are materially closer than baseline (measured by RMSE over (k,h) grid).

---

## 10) Notes for Codex implementation

- Keep all current empirical-prep functions, but **abstract** the innovation draw step behind `draw_increments()`.
- Prefer pure-R implementations first (rt, rpois, rexp), then optionally add packages:
  - `ghyp` / `GeneralizedHyperbolic` for GH / NIG
  - `VarianceGamma` for VG
  - `ismev` / `evd` / `extRemes` for GPD fitting
- Store intermediate objects (PCA loadings, smoothed curves) in a `cache` list passed into `draw_increments()` to avoid recomputing.

