# Spec: principled jump & heavy-tail model testing, calibration, and selection (for FB weekly rank dynamics)

**Purpose.** Extend the existing Rmarkdown report so it can **systematically test multiple jump / heavy‑tail model families**, **estimate parameters in a principled way**, and **select a preferred model** (with robustness checks) that addresses the heavy tails and **rank-dependent asymmetry** (top ranks biased toward downward jumps; lower ranks biased toward upward jumps).

This spec is written for **Codex** to implement. It is designed to be robust to ongoing refactors: it names *interfaces* and *outputs*, not exact file paths or object names.

---

## 0) Why this is needed (tie-in to the current report)

### Empirical stylized facts to preserve
The revised report already documents two key facts that drive the modeling requirements:

1) **Rank-/size-dependent asymmetry in movements.**  
Endpoint-level forward growth (identity-based) shows strong downward drift for “large/midsize” ranks and near-zero drift for small ranks at short horizons (e.g., 1-week medians: large −0.126; midsize −0.147; small −0.0019). This is consistent with “top ranks biased downward.”  
(Keep this as a motivating empirical object and add explicit *skew/jump* diagnostics below.)

2) **Very heavy-tailed rank-slot increments** (and asymmetry in extreme quantiles).  
The report’s tail diagnostics show standardized increments with very extreme empirical quantiles; importantly the left vs right tails differ by bucket (e.g., large bucket has much more negative q0.001 than positive q0.999; small bucket has a larger positive q0.999 than negative q0.001), consistent with *downward-biased extremes in the top ranks and upward-biased extremes in the lower ranks*.

### Current jump model: good direction, but needs principled estimation + broader testing
The report includes a first “jump_factor” model configuration with:
- a **systematic heavy-tail factor** (df_F, phi_F),
- **idiosyncratic heavy tails** (df_eps by bucket),
- **jump probabilities** p_sys and p_idio (bucketed),
- **jump size distribution** currently set to *symmetric Laplace*, and
- **mean reversion** (kappa by bucket).

The tail-comparison table indicates partial improvement but remaining mismatches (e.g., too-light tails for large/midsize at the 0.1% left tail, and too-heavy tails for small). This is exactly the situation where “hand-tuned” parameters need to be replaced with a repeatable estimation/calibration + model comparison protocol.

---

## 1) Add a formal “model zoo” section to the report

### 1.1 Model families to include (minimum complete set)
Codex should implement at least these families **because they are standard, interpretable, and cover the most common peer‑reviewed choices**:

**A. Heavy-tailed diffusion innovations (no explicit jumps)**  
These are the simplest upgrade from Gaussian and often the first reviewer question.
- A1. Student-t innovations (possibly bucket-specific df)
- A2. **Skew-t** innovations (bucket- or rank-dependent skew)
- A3. **Generalized Hyperbolic (GH)** family innovations (includes NIG, VG; flexible skew + tails)

**B. Discrete-time jump mixtures (explicit jumps)**  
Treat weekly increments as a mixture: “diffusion” + “jump” component.
- B1. **Merton-style**: Gaussian diffusion + Bernoulli jump with Normal jump size
- B2. **Kou-style**: Gaussian diffusion + Bernoulli jump with **double-exponential** (asymmetric) jump size  
  (Key for rank-dependent downward vs upward jump bias.)
- B3. **Two-component Gaussian mixture** (diffusion vs jump variance) as a pragmatic baseline (EM‑fit)

**C. Factor + idiosyncratic structure (systemic vs idio), with heavy tails/jumps**  
These are motivated by the report’s “common shock” analysis and are typically more realistic than purely independent ranks.
- C1. Factor model with heavy-tailed factor + heavy-tailed idio (t / GH)
- C2. Factor model with **systemic jumps + idiosyncratic jumps** (asymmetric Kou jumps)
- C3. Factor model with **time-varying jump intensity** (optional advanced; see §1.2)

**D. Optional advanced models (implement if diagnostics justify)**  
Only include if the data show meaningful clustering of extremes (see §2.4).
- D1. **Hawkes/self-exciting jump intensity** (systemic and/or idiosyncratic)
- D2. Regime-switching mixture (calm vs turbulent) (HMM)

### 1.2 How to justify this set to reviewers (add to narrative)
In the report text, add a short “why these models” paragraph:
- Ranking dynamics in complex systems often mix **local diffusion-like movement** with **long jumps / Lévy-like regimes** and separate **replacement** mechanisms; this motivates explicit jump components rather than pure diffusion.  
- Finance/econometrics literature treats heavy tails with either (i) heavy-tailed innovations (t, GH) or (ii) explicit jump mixtures, often augmented with time-varying or self-exciting intensities; and asymmetry (good vs bad jumps) is standard in many applications.

(Insert citations in the paper; Codex should add BibTeX entries. See §8.)

---

## 2) Add a new diagnostics block: *tail shape + skew + jump clustering* (by bucket and by rank)

### 2.1 Standardized increment object(s)
Codex should compute diagnostics on BOTH:
- **Rank-slot increments**: Δlog w_{k,t} where “k” is the rank slot (the object the simulation targets).
- **Identity-based increments**: Δlog share for the same endpoint i across weeks, conditional on its rank bucket at time t (secondary, but important for interpretation).

Store both in a single long table with columns:
- `week`, `k` (rank slot) OR `endpoint_id`,
- `bucket` (large/midsize/small or whatever bucket function exists),
- `dlogw` (increment),
- `dlogw_std = (dlogw - mu_hat(k)) / sd_hat(k)` where mu_hat, sd_hat come from the smoothed moment curves already used.

### 2.2 Tail-probability diagnostics (must be directional)
The existing tail diagnostics are mostly symmetric (|z|>c). Keep them, but **add directional versions**:

For each bucket (and optionally by rank quantile bins), compute:
- `p_z_gt_3`, `p_z_lt_-3`, `p_abs_gt_3`
- `p_z_gt_5`, `p_z_lt_-5`, `p_abs_gt_5`
- quantiles: `q_0.001`, `q_0.01`, `q_0.99`, `q_0.999`
- tail-imbalance ratios:
  - `R_999_001 = abs(q_0.999) / abs(q_0.001)`  
  - `Delta_tail = q_0.999 + q_0.001` (sign indicates net skew)

These directional metrics are the *formal target* for “top ranks biased downward vs lower ranks biased upward.”

### 2.3 Distributional shape diagnostics
Add the following, per bucket:
- skewness and excess kurtosis (robust versions also: medcouple skewness; robust kurtosis)
- QQ plots against:
  - Normal
  - Student-t (with fitted df)
  - GH (fitted)
- Hill estimator / tail-index estimate for |z| (optional but helpful for deciding between exponential-tailed vs power-tailed models)

### 2.4 Jump clustering diagnostics (to decide whether Hawkes/regime-switching is warranted)
On weekly data you may or may not see clustering, but test it explicitly:

For each bucket:
- Define extreme indicator `I_t = 1(|z|>c)` for c in {3,4,5}.
- Compute:
  - autocorrelation of `I_t` and of `|z|` (per rank, then averaged), and/or
  - a runs test / Ljung–Box on `I_t`.
- If there is clear positive dependence (clustered extremes), enable optional models D1/D2.

Codex should gate advanced models behind a config flag: `CFG$enable_jump_clustering_models`.

---

## 3) Parameter estimation: “principled” procedures to add

### 3.1 General rule: estimate conditional one-step increment distributions first
For each model family, perform estimation primarily on the **one-step standardized increments** (rank-slot object), because:
- it directly targets the heavy tails problem, and
- it does not depend on the multi-step simulation loop (avoids circularity).

Then use simulation-based calibration only for the few global parameters that need it (e.g., entry/exit rate, overall sigma scaling).

### 3.2 Rank dependence: bucketed vs smooth (implement both)
Codex should implement two parameterization modes:

**Mode 1 (default / simplest): bucket-specific parameters**  
Estimate separate parameters for {large, midsize, small}. This is easy to explain and robust.

**Mode 2 (optional): smooth-by-rank parameters**  
For parameters that should vary gradually with rank (e.g., jump intensity, skew), estimate:
- `logit(p_jump(k)) = s_p(log(k))` (spline)
- `log(scale_pos(k)) = s_+(log(k))`, `log(scale_neg(k)) = s_-(log(k))`
- `logit(pi_pos(k)) = s_pi(log(k))` (probability jump is positive)

Use penalized splines / mgcv GAM with shrinkage to avoid overfitting.
Codex should implement both modes and show that bucketed ≈ smooth, or else justify smooth.

### 3.3 Estimation methods by model family

#### A) Student-t and skew-t innovations (A1/A2)
- Fit by maximum likelihood per bucket or using a rank-smooth paramization.
- Record `df`, `skew` (if skew-t), `scale` (should be close to 1 on standardized z if the sd curve is correct).
- Produce AIC/BIC and diagnostic plots.

#### B) GH innovations (A3)
- Use the **ghyp** package to fit GH / NIG / VG variants by MLE.
- Use model selection inside GH family (AIC, LR tests) to choose between NIG, VG, skew-t, full GH where appropriate.
- Store fitted parameters and their standard errors (ghyp provides methods for this).

#### C) Jump mixtures (B1–B3)
Treat weekly increments as a mixture:
- With prob (1−p), diffusion component
- With prob p, jump component

**B3 (two-Gaussian mixture)**
- Fit 2-component Gaussian mixture by EM per bucket (e.g., `mclust` or custom EM).
- Constrain component 1 to have smaller variance (diffusion), component 2 larger (jump).
- Use BIC to validate that 2 components outperform 1.

**B1 (Merton)**
- Diffusion: Normal(0,1) on standardized scale.
- Jump size: Normal(μ_J, σ_J).
- Weekly jump indicator: Bernoulli(p).
- Estimate (p, μ_J, σ_J) by EM or direct MLE.  
  (EM is easiest: latent jump indicator.)

**B2 (Kou / asymmetric double-exponential jumps)**
- Jump size distribution:
  - With prob π, J>0 with exponential rate η_+
  - With prob (1−π), J<0 with exponential rate η_-  
- Estimate (p, π, η_+, η_-) by MLE (closed forms exist for exponential rates conditional on membership) inside EM.

**Critical requirement for this project:** allow π and/or (η_+,η_-) to vary by bucket (or smoothly by rank) so the model can reproduce “top ranks biased downward; lower ranks biased upward.”

#### D) Factor + idiosyncratic structure (C1–C3)
Use the report’s existing PCA-based evidence as the default factor extraction:
- Let β_k be the (smoothed) PC1 loading by rank slot.
- Let F_t be the PC1 score time series (standardized).

Then fit on residuals:
- z_{k,t} = β_k F_t + ε_{k,t}
- Fit heavy tails/jumps on F_t and ε_{k,t} separately.

For systemic jumps:
- allow F_t to have a jump mixture (p_sys, jump size params).
For idiosyncratic jumps:
- allow ε_{k,t} to have bucket-/rank-dependent jump mixture (p_idio(k), …).

**If implementing C3 (time-varying intensity):**
- model p_sys(t) and/or p_idio(t) as a function of past jump indicators (Hawkes-like), or as state-dependent (e.g., depends on volatility proxy).
- Keep it minimal: 1–2 parameters (baseline + excitation) to avoid overfitting.


### 3.4 Parameter constraints, initialization, and convergence (implementation requirements)

To keep estimation stable and defensible, Codex must enforce the following constraints:

**Common constraints**
- Jump probability parameters: `0 < p < p_max` where default `p_max = 0.25` (weekly jumps more frequent than this should be treated as “diffusion regime shift” or mis-specified scaling).
- Student-t degrees of freedom: `df > 2.2` (finite variance) unless explicitly exploring infinite-variance models.
- GH parameters: enforce the parameter domain required by `ghyp` (Codex should rely on package checks and stop with a readable error if convergence fails).
- Mixture component ordering: enforce `sd_component1 < sd_component2` to avoid label switching.

**Initialization best practices (Codex must implement)**
- Start values for jump probability `p`:
  - `p0 = mean(|z| > 3)` (bucket-specific) as a crude upper bound.
  - or `p0 = mean(|z| > 4)` if the diffusion component is already heavy-tailed.
- Start values for asymmetric sign probability `pi_pos`:
  - `pi0 = mean(z > 0 & |z| > 3) / mean(|z| > 3)`  (bucket-specific).
- Start values for Kou rates (η_+, η_-):
  - compute mean exceedance sizes over a threshold u (default u=3):  
    `mplus = mean(z - u | z>u)`, `mminus = mean((-z) - u | z<-u)`  
    then initialize `η_+ = 1 / max(mplus, 1e-3)` and `η_- = 1 / max(mminus, 1e-3)`.
- Start values for Merton jump Normal params:
  - `μ_J0 = mean(z | |z| > u)`, `σ_J0 = sd(z | |z| > u)`.

**Convergence**
- EM iterations: stop when relative log-likelihood improvement < 1e-6 or max iterations (e.g., 500).
- Keep and report:
  - final log-likelihood
  - number of iterations
  - whether convergence was achieved
- If a model fails to converge for a bucket, the comparison table must flag it as “nonconverged” and exclude it from selection unless it is the only model in its family.

### 3.5 Explicit EM update formulas for B1/B2/B3 (Codex guidance)

Codex should implement EM in a transparent way (even if using a package) so the report can briefly describe it.

**B3: two-Gaussian mixture on z**
- E-step: compute responsibilities r_i for high-variance component.
- M-step: update mixing weight p, component means (typically near 0), and component variances, enforcing var2 > var1 (swap labels if needed).

**B1: Merton jump mixture**
Treat observed z_i as:
- diffusion: N(0,1) on standardized scale (optionally allow a small scale parameter if needed)
- jump: N(μ_J, σ_J)

EM:
- E-step: r_i = p * φ(z_i; μ_J, σ_J) / [(1-p)*φ(z_i;0,1) + p*φ(z_i; μ_J, σ_J)]
- M-step:
  - p = mean(r_i)
  - μ_J = sum(r_i z_i)/sum(r_i)
  - σ_J^2 = sum(r_i (z_i-μ_J)^2)/sum(r_i)

**B2: Kou asymmetric double-exponential jump mixture**
Jump density:
- f_J(z) = pi * η_+ * exp(-η_+ z) for z>0
         + (1-pi) * η_- * exp(-η_- (-z)) for z<0

EM (conceptually):
- latent indicators: jump vs diffusion; and within jump, positive vs negative branch.
- E-step: compute jump responsibility r_i as in B1 but using f_J(z_i) for jump density.
- M-step:
  - p = mean(r_i)
  - pi = sum(r_i * 1[z_i>0]) / sum(r_i)
  - η_+ = sum(r_i * 1[z_i>0]) / sum(r_i * 1[z_i>0] * z_i)
  - η_- = sum(r_i * 1[z_i<0]) / sum(r_i * 1[z_i<0] * (-z_i))

(Ensure safeguards for empty positive/negative sets; in that case fix pi to empirical sign rate and refit.)

### 3.6 Standard errors / uncertainty for fitted distribution parameters
Codex should output uncertainty using at least one of:
- asymptotic SEs (if the fitting routine provides them, e.g., GHyp methods), and/or
- bootstrap SEs (preferred and consistent across families):
  - resample weeks in blocks (see §6)
  - refit distribution parameters
  - report 50% and 90% intervals

---


---

## 4) Calibration to platform-level targets (simulation-based) — unify across models

After fitting each candidate model to one-step increments, run the existing simulation pipeline and compute the existing macro targets (CDC, durable change, Xi). Add new tail/skew targets.

### 4.1 Unified “targets” object
Codex should create a single list/data structure `targets_emp` containing:
- CDC curve (by rank)
- durable change table (by bucket × horizon)
- Xi curve (median/quantiles by k)
- micro-movement object(s) (rank-slot and/or identity-based comparator)
- tail & skew diagnostics (per bucket, and optionally by rank bins)

### 4.2 Unified “fit metrics” object
For each model, compute a metrics list:
- `rmse_cdc`
- `rmse_durable`
- `rmse_xi`
- `rmse_micro` (if included)
- tail metrics mismatch:  
  - `L2_tail = sum_b sum_m w_m (emp_m - sim_m)^2` where m runs over p_z_gt_3, p_z_lt_-3, …, q_0.001, q_0.999, etc.
- skew mismatch similarly (using Delta_tail or R_999_001)

### 4.3 Objective function and model ranking
Define a transparent scoring function:
- `Score(model) = Σ_j W_j * metric_j`
where W_j are pre-specified weights.

**Default weights (suggested):**
- Put high weight on CDC + Xi (core structural objects)
- Medium on durable change
- Medium-high on tail/skew (since this is the active failure mode)
- Low-medium on micro identity-based until the entry/exit mismatch is resolved

Codex should:
- allow weights to be set in config
- report sensitivity to weights (robustness)

---

## 5) Model selection protocol (what “principled choice” means in the report)

Codex should implement and report a 3-layer selection process:

### Layer 1: Distributional fit on one-step increments (primary statistical fit)
For each bucket (or rank-bin):
- Compute log-likelihood (or pseudo-LL) of the standardized increments under the fitted distribution.
- Report AIC/BIC per model family.

**Selection rule:** Prefer models that materially improve AIC/BIC over Gaussian and that fix the tail/skew diagnostics.

### Layer 2: Out-of-sample validation (time-split)
To avoid overfitting and match best practice in recent applied work:
- Split weeks into train/test (e.g., train 70%, test 30% by time; or rolling origin).
- Fit parameters on train.
- Evaluate tail/skew metrics and CDC/durable/Xi simulation fit on test.

**Selection rule:** The preferred model must not collapse out-of-sample.

### Layer 3: Simulation-based goodness-of-fit (posterior predictive style)
For each model:
- simulate many paths
- compute distribution bands for each target object
- overlay empirical targets and check if empirical lies in, say, 50% and 90% envelopes.

**Selection rule:** Pick the simplest model that passes these checks.

---

## 6) Parameter uncertainty and robustness (bootstrap “best practice” block)

Codex should add a dedicated robustness section with at least:

### 6.1 Block bootstrap over time
Because observations are temporally dependent, bootstrap by blocks:
- sample contiguous blocks of weeks (block length e.g., 4–8 weeks; provide sensitivity)
- refit each candidate model and re-run reduced simulations (fewer paths)
- report parameter intervals + stability of model ranking

### 6.2 Sensitivity to bucket definitions and K_cut
Repeat key fits under:
- alternative bucket cutoffs (e.g., {10,100} instead of {25,250})
- alternative K_cut (if feasible)
- alternative smoothing half-width h for moment curves

---

## 7) Concrete implementation instructions for Codex (code-level)

### 7.1 Add a model registry
Implement a `model_registry` (named list) where each model entry has:
- `name`
- `family` (A/B/C/D)
- `fit_fn(data, cfg) -> fitted_model`
- `simulate_fn(fitted_model, sim_cfg, moment_curves, ...) -> sim_out`
- `eval_fn(sim_out, targets_emp, cfg) -> metrics`

This is key for modularity during refactors.

### 7.2 Implement standardized data builders
Create functions (or modules) that return:
- `build_rank_slot_increments(endpoint_weekly, K_max, ...)`
- `build_identity_increments(endpoint_weekly, horizons, ...)`
- `standardize_increments(increments_df, mu_curve, sd_curve)`

### 7.3 Implement diagnostics functions
Add:
- `tail_skew_diagnostics(z, thresholds=c(3,5), probs=c(0.001,0.01,0.99,0.999))`
- `jump_clustering_diagnostics(z, thresholds=c(3,4,5))`
- `qqplot_suite(z, fitted_models=list(...))`

### 7.4 Implement fitters
Minimum required fitters:
- `fit_student_t(z_by_bucket, mode="bucket"|"smooth")`
- `fit_skew_t(z_by_bucket, ...)`
- `fit_ghyp(z_by_bucket, variants=c("NIG","VG","GH"))`
- `fit_mixture_gaussian(z_by_bucket, n_components=2)`
- `fit_jump_merton(z_by_bucket)`
- `fit_jump_kou_asym(z_by_bucket)`
- `fit_factor_jump_model(z_matrix, beta_k, ...)` (build on existing jump_factor)

### 7.5 Integrate into existing simulation
In the existing simulator (currently supports baseline and `type="jump_factor"`):
- Generalize `simulate_rank_paths()` (or equivalent) to accept `fitted_model` from the registry.
- Ensure all models output the same snapshot schema (rank, t, share, etc.) so downstream evaluation code is unchanged.

### 7.6 Add a “model comparison” driver
Add `run_model_comparison(cfg)` that:
1) Builds/loads empirical targets.
2) Fits each model on train data.
3) Simulates each model.
4) Evaluates all metrics.
5) Produces:
   - a ranked table of models
   - key diagnostic plots per model
   - “best model” object saved for later sections

### 7.7 Update the report narrative and figures
Add new sections (in order):
1) **Tail & skew diagnostics (empirical)**
2) **Candidate model families**
3) **Estimation protocol**
4) **Model comparison results**
5) **Preferred model and interpretation**
6) **Robustness: bootstrap + sensitivity**

Each section should be reproducible and controlled by config flags to manage runtime.

---

## 8) References to add to the report (BibTeX entries)
Codex should add BibTeX entries and cite them in the Methods section to justify:
- diffusion + jumps in ranking dynamics,
- multiple jump-testing/diagnostic approaches,
- time-varying / self-exciting jump intensities,
- asymmetric jumps (good vs bad),
- GH distributions for skew heavy tails,
- GH fitting and model selection tooling in R.

(Implementation note: add a `references.bib` and enable `bibliography:` in YAML.)

---

## 9) Default recommendation for the “lead” model (most defensible to reviewers)
After implementing the full protocol above, the default “lead” model to present is:

**C2: Factor + idiosyncratic asymmetric jump mixture (Kou-style), with bucketed parameters and optional smooth-by-rank extension**, because it:
- explicitly matches the empirical heavy tails,
- can reproduce **directional tail asymmetry** by bucket/rank,
- respects the report’s existing common-shock structure (β_k factor loadings),
- remains interpretable and not over-parameterized.

As a robustness companion, include:
- GH innovations (A3) and
- a 2-component Gaussian mixture (B3)
to show that conclusions are not dependent on a single parametric choice.

---

## 10) Deliverables checklist (what Codex must output)
- [ ] New diagnostics tables/plots (tail/skew, clustering), by bucket and (optional) rank bins.
- [ ] Fitted parameter tables for each model (with SEs where available).
- [ ] Model comparison table with AIC/BIC + simulation target RMSEs + tail/skew mismatch.
- [ ] Out-of-sample comparison results (time split).
- [ ] Bootstrap robustness summary (parameter intervals + model ranking stability).
- [ ] A single `best_model` object used for the final simulation figures and narrative.
