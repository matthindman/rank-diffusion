# Rank Diffusion Model v4.3: Research Notes & Key Findings

## Table of Contents
1. [The Core Problem: IG Estimation Breakdown](#1-the-core-problem-ig-estimation-breakdown)
2. [Academic Literature on Signal Extraction](#2-academic-literature-on-signal-extraction)
3. [Concentration & Top-K Modeling](#3-concentration--top-k-modeling)
4. [Balanced Panel Design with High Turnover](#4-balanced-panel-design-with-high-turnover)
5. [Implementation: What Was Done](#5-implementation-what-was-done)
6. [Current Results & Remaining Failures](#6-current-results--remaining-failures)
7. [Key Sources](#7-key-sources)

---

## 1. The Core Problem: IG Estimation Breakdown

### Data characteristics

| Metric | FB | IG (full) | IG (top 20k) |
|---|---|---|---|
| Entities/week | 13,586 | 463,028 | 20,000 |
| Total unique | 15,685 | 2,312,875 | 201,721 |
| Balanced (ALL weeks) | ~7,889 | 8,916 (0.4%) | 605 |
| Balanced (50%+ weeks) | — | — | 8,023 |
| Weekly exit rate | 2.9% | 44.5% | 57% |
| ACF(1) of changes | -0.355 | -0.432 | -0.375 |
| emp_median_var | 0.924 | 1.082 | 0.602 |

### The failure chain (IG at full scale)

1. **ACF(1) = -0.432** (extremely negative)
2. Method-of-moments sigma_obs estimator: `sigma2_obs = -gamma1 + gamma2/phi_agg = 0.2726`
3. Clipped to upper bound: sigma2_obs = 0.25 (sigma_obs = 0.50)
4. Noise fraction of change variance: `2 * 0.25 / 0.7415 = 67.4%`
5. Band fitter finds sigma_eta^2 ~ 0 (no room for permanent component)
6. kappa_base_raw floors at 1e-6 (no mean reversion)
7. **Model degenerates**: pure transitory AR(1) with observation noise, no permanent drift

This is the classic **pile-up problem** from the structural time series literature.

### The IG data rank column

The `rank` column in `ig_weekly_ranked.parquet` is a **dense/tied rank** of `metric_value`, NOT a unique 1..N rank. For a week with 464k entities but only ~23k unique metric values, rank goes 1 to ~23k with massive ties. The `canonicalize_panel` function in `schema.py` recomputes unique ranks using `rank(method='first', ascending=False)`.

### IG concentration (weekly, by strict metric_value ranking)

| Tier | Volume Share | Min metric_value |
|---|---|---|
| Top 100 | ~20.6% | ~1,020,820 |
| Top 1,000 | ~45% | ~139,628 |
| Top 5,000 | ~66% | ~29,361 |
| Top 10,000 | ~74.6% | ~14,117 |
| Top 20,000 | ~82.9% | ~6,395 |
| Top 30,000 | ~87.3% | ~3,841 |
| Top 50,000 | ~92% | ~1,875 |
| Top 100,000 | ~96.8% | ~583 |

Below ~rank 20k, entities contribute <17% of total volume and are dominated by entry/exit churn, not rank dynamics.

---

## 2. Academic Literature on Signal Extraction

### The pile-up problem

**Stock & Watson (1998), "Median Unbiased Estimation of Coefficient Variance in a Time-Varying Parameter Model" (JASA, 93, 349-358)**
- In the local level model, MLE of the permanent variance has a large probability mass at exactly zero
- This is NOT a small-sample artifact — Shephard (1993) showed MLE for MA(1) coefficient piles up at -1
- Solution: asymptotically median-unbiased estimator by inverting the Nyblom (1989) test statistic
- **Practical implication**: When your MoM estimator produces sigma_obs so large that sigma_eta → 0, this is expected behavior. The permanent component needs explicit protection.

**Harvey (1989), "Forecasting, Structural Time Series Models and the Kalman Filter" (Cambridge UP)**
- Defines signal-to-noise ratio q = sigma_eta^2 / sigma_obs^2
- When MLE gives q=0, the reduced form is noninvertible MA(1)
- Recommends: (1) examine concentrated likelihood near boundary, (2) parameterize via q ratio, (3) **fix measurement noise from external info** if available
- Harvey & Peters (1990) document that MLE frequently hits the q=0 boundary

**Auger-Methe et al. (2016), "State-Space Models' Dirty Little Secrets" (Scientific Reports)**
- "Fixing the standard deviation of the measurement error helped reduce estimation problems dramatically"
- Bayesian approach with inverse-gamma prior on sigma_eta^2 avoids pile-up mechanically

### Variance ratio decomposition

**Cochrane (1988), "How Big Is the Random Walk in GNP?" (JPE, 96, 893-920)**
- Model-free permanent component identification via variance ratios
- `VR(k) = Var(Delta^k x) / (k * Var(Delta x))`
- As k → infinity: VR(k) → sigma_eta^2 / Var(Delta x) = permanent fraction
- At finite k, obs noise contributes 2*sigma_obs^2 / (k*Var(Delta x)) — diluted at long horizons
- **Key insight**: long-horizon VR provides model-free bound on permanent component, uncontaminated by obs noise

### The adaptive noise fraction approach

**Kamber, Morley & Wong (2018), "Intuitive and Reliable Estimates of the Output Gap from a Beveridge-Nelson Filter" (Review of Economics and Statistics, 100(3), 550-566)**
- Most directly relevant precedent for our approach
- Define delta = sigma_perm^2 / sigma_obs^2 (signal-to-noise ratio)
- Standard unconstrained estimation produces implausible delta values
- Their solution: **impose a constraint on delta** (equivalently, cap the noise fraction)
- For US GDP: delta ~ 0.25 (permanent = 25% of forecast error variance)
- They develop an automatic selection procedure for delta

**Translation to our model**:
```
sigma2_obs_max = max_noise_frac * emp_median_var / 2.0
```
The `/2` accounts for obs noise contributing `2 * sigma2_obs` to change variance.
- `max_noise_frac = 0.50` means at most 50% of change variance from observation noise
- This is **more conservative** (tighter) than Kamber et al.'s typical calibration (~60% noise)
- For FB: sigma2_obs ≈ 0.017, adaptive cap ≈ 0.23 — cap never binds
- For IG: sigma2_obs_est = 0.27, adaptive cap = 0.15 — cap binds, brings sigma_obs from 0.50 to 0.39

### The minimum permanent fraction

Even with the noise cap, the band fitter can still assign all *signal* variance to the transitory AR(1), leaving sigma_eta = 0. The solution is a permanent fraction floor:

```python
signal_var = max(total_var - 2 * sigma2_obs, 1e-8)
se2 = max(se2_fitted, min_perm_frac * signal_var)
```

With `min_perm_frac = 0.10`:
- At least 10% of signal variance must be permanent (random walk)
- For IG: signal_var ≈ 0.30 → se2_min ≈ 0.03 → sigma_eta_min ≈ 0.17
- For FB: se2_fitted >> se2_min, so floor never binds

### The VR decomposition math

For the permanent-transitory model with observation noise:
```
Var(Delta^k x) = k * sigma_eta^2 + 2*sigma_c^2*(1-phi^k) + 2*sigma_obs^2
```
where sigma_c^2 = sigma_nu^2 / (1-phi^2).

Variance ratio:
```
VR(k) = [k*A + B*(1-phi^k) + D] / [k*(A + B*(1-phi) + D)]
```
where A = sigma_eta^2, B = 2*sigma_c^2, D = 2*sigma_obs^2.

As k → infinity: VR(k) → A / (A + B*(1-phi) + D) = permanent fraction of change variance.

ACF structure:
```
ACF(1) = (-sigma_c^2*(1-phi)^2 - sigma_obs^2) / Var(Delta x)
```
When ACF(1) is very negative, sigma_obs^2 must be large (given the model). The MoM estimator captures this correctly. The question is whether the model is appropriate — and for IG with extreme turnover, the answer is "only for the stable top tier."

---

## 3. Concentration & Top-K Modeling

### Gabaix (1999), "Zipf's Law for Cities"
- Zipf's law emerges from Gibrat's law + reflecting barrier at minimum size
- The barrier operates at the *bottom*, not the top
- For top-K modeling: the barrier is invisible to top entities
- **Mean reversion (kappa) maintains the Zipf shape**; entry/exit handles the boundary

### Luttmer (2007), "Selection, Growth, and the Size Distribution of Firms" (QJE)
- Explicit entry/exit model producing Zipf distribution
- Calibrated using *upper-tail moments only* (Zipf exponent, entry/exit rates, relative entrant size)
- Does NOT require a balanced panel
- **Key validation**: you can calibrate a model with heavy entry/exit using top-K moments alone

### Gabaix & Ibragimov (2011), rank-1/2 correction
- For OLS estimation of Pareto exponent from rank data: use `ln(Rank - 1/2)` not `ln(Rank)`
- Standard error: `(2/n)^{1/2} * zeta`, not the OLS SE
- With K=10,000 entities: SE ≈ 0.014 * zeta (very precise)

### Clauset, Shalizi & Newman (2009), "Power-Law Distributions in Empirical Data"
- Gold standard for choosing `x_min` (the lower cutoff of power-law behavior)
- Method: for each candidate x_min, fit by MLE, compute KS distance, minimize
- Power-law most reliable in upper tail; below x_min, may be log-normal or exponential
- **Choosing K**: should correspond roughly to the x_min where power-law fits well

### Atlas model (Banner, Fernholz & Karatzas 2005)
- Mathematically rigorous rank-based diffusion
- Only the lowest-ranked particle gets positive drift
- Conservation condition: sum of local times = gamma * N * t
- Zipf's law iff conservation + completeness conditions satisfied
- **Our kappa term is the finite-N approximation of the Atlas drift**

### Practical choice: K = 20,000 for IG

Rationale:
- 82.9% of volume captured
- 8,023 entities at 50% presence (workable balanced panel)
- ~17.5% weekly entry rate at top-20k level (vs 45% overall)
- Comparable to FB scale (~14k entities/week)
- Min metric_value ≈ 6,400 (real accounts with substantial engagement)
- Same code works for both platforms without modification

Balanced panel sizes at different rank cutoffs:

| Cutoff | Ever present | 50%+ weeks | 80%+ weeks | ALL weeks |
|---|---|---|---|---|
| Top 10k | 109,904 | 4,076 | 1,510 | 310 |
| Top 20k | 201,719 | 8,508 | 3,050 | 605 |
| Top 30k | 285,947 | 13,368 | 4,660 | 896 |

### Reddit / FB top-coverage universe (added 2026-07-02)

Concentration of weekly activity (raw submission karma / interactions), measured
across all weeks of each panel; K values are the pre-registered `COVERAGE_K` map
in `minimal_rankdiff.py` (chosen from these statistics alone, before any fit):

| Platform | N/week | top-K for 80% | top-K for 90% | top-K for 95% |
|---|---|---|---|---|
| Reddit (T=30) | ~200,565 | 2,500 (80.0%) | 5,000 (89.2%) | 10,000 (95.6%) |
| Facebook (T=88) | ~14,363 | 1,800 | 3,500 | 5,500 |

Weekly k80/k90 for Reddit are tightly stable (k80 range 2,235–2,837; k90 range
4,906–5,842 over 30 weeks).

**Tail degeneracy (why the tail cannot be modeled with a Gaussian observation
model):** 60% of the Reddit panel has weekly karma <= 5 and 39% has karma <= 1.
Rank-level tie structure at a representative mid-sample week:

| full-panel rank | karma | entities tied at that value |
|---|---|---|
| 2,500 | 18,603 | 1 |
| 5,000 | 7,285 | 1 |
| 10,000 | 2,136 | 6 |
| 20,000 | 397 | 17 |
| 50,000 | 25 | 620 |
| 100,000 | 3 | 10,640 |

Below ~rank 20k, weekly "rank movement" is increasingly tie-breaking noise on a
discrete, zero-inflated count — not attention dynamics. FB never had this
problem: the CrowdTangle panel is already top-truncated at source (~14.4k
pages/week), so an uncapped Reddit panel was a hidden cross-platform asymmetry.

**Boundary behavior at K in {2,500, 5,000, 10,000} (nearly identical across K):**
- ~10%/week of the top-K crosses below the boundary; 99.93% of droppers remain
  OBSERVED in the full panel (true disappearance 0.07%/wk) — no censoring.
- Droppers land shallow: median 1.15 K, p90 1.7 K, p99 ~4 K. Entrant origins are
  symmetric (median 1.16 K); only 0.4–0.7% of top-K entrants come from unobserved.
- 33–38% of droppers are back inside the top-K within 4 weeks (permeable,
  reflecting boundary — not absorbing).
- A CLOSED membership set of B = 4K entities (best permanent rank on a 20-week
  train window) covers 97–98% of the weekly top-K in the 10 held-out weeks.

**Censoring asymmetry (owner note, 2026-07-03):** coverage shares are only platform-wide
where the underlying collection is a census. Reddit/Pushshift is complete — its top-K shares
are true platform shares. CrowdTangle (FB, IG) tracked only pages above inclusion thresholds:
"top-K = X% of interactions in the data" overstates platform coverage, possibly badly. The
top-coverage rule still defines a clean estimand (the head of the tracked universe), but
coverage percentages must never be compared across platforms as if commensurable, and paper
text must say "of tracked activity" for FB/IG.

**CrowdTangle instrument eras (2026-07-03):** the FB collection is additionally
NON-STATIONARY as an instrument: a fixed ~14.5k-page panel (enrollment frozen from the start
— new ids ≈ 0/week), a mid-series collection collapse (2022-07..09, daily counts to ~600), a
2023 stretch that mixes two collection universes (backfill + full-export patch days) with
wildly unstable intensity, and a slow terminal decline into the 2024 shutdown (reported
mechanism: pages crossing the inclusion threshold were no longer added once FB had decided
to kill the product). Standard treatment for instrument/collection change applies: segment
by collection-health metadata (pages/day, pages/week, enrollment rate) with pre-registered
breakpoints, run primary inference on the healthy era, use later healthy segments as
replication only, and never let any estimation or evaluation window straddle a broken era.
Measured era table + directives: MODEL_STATUS section 2g addendum.

**Design adopted (`restrict_universe`):** closed Lagrangian universe of the B=4K
entities with the best ABSENCE-PENALIZED permanent rank (absent weeks count at
the observation floor N_t+1 — averaging observed weeks only re-admits Eulerian
selection: 1–2-week spikers entered the universe, pooled alone in the deepest
knot and inflated its exit rate to 0.60/wk vs the true 0.0007/wk). All member
observations retained (below-K excursions included, uncensored); weekly ranks
recomputed within the universe; Gabaix rebirth operates at the buffer bottom;
goal-1 diagnostics score only entities with time-mean rank <= K; boundary
out-flux and 4-week return are new scorecard rows (predictions, not assumptions).

---

## 4. Balanced Panel Design with High Turnover

### The problem with requiring 100% presence

With 45-57% weekly turnover, requiring presence in ALL 53 weeks gives:
- Full IG: 8,916 / 2.3M = 0.4% → extreme survivorship bias
- Top-20k IG: 605 / 201k = 0.3% → still terrible
- These are the most atypical, ultra-stable entities

### Academic guidance

**Cabral & Mata (2003)**: Compare distributions at two time points: (a) all entities at t=0, (b) survivors at t=1, (c) t=0 values of survivors. Gap between (a) and (c) = selection effect.

**Clementi & Palazzo (2013), "Entry, Exit, Firm Dynamics, and Aggregate Fluctuations"**: Compute diagnostics on balanced panel extracted from simulated data **using the same conditioning rules** as empirical data. **Consistency between empirical and simulated diagnostics is what matters**, not which panel you use.

**Haltiwanger, Jarmin & Miranda (2010)**: Use unbalanced panels with explicit modeling of the selection process. Track entry/exit rates directly.

### The min_presence_frac approach

Instead of ALL periods, require presence in ≥ X% of periods:
- `min_presence_frac = 0.50` → 8,023 balanced entities for top-20k IG
- `min_presence_frac = 0.80` → 3,050 balanced entities
- `min_presence_frac = 1.00` → 605 balanced entities (original)

**Trade-off**: Lower thresholds give more entities but introduce NaN gaps in time series. The ACF/VR code handles NaN via `.dropna()`, which can create non-consecutive period pairs in autocorrelation calculation. For 50% presence, ~10-15% of ACF pairs may span non-consecutive periods. This slightly weakens negative autocorrelation — actually helpful for the IG sigma_obs problem.

### Recommended tiered diagnostic strategy

| Diagnostic | Compute On | Rationale |
|---|---|---|
| Zipf exponent | Cross-sectional top-K each period | Standard tail estimation |
| ACF(1), RACF(1) | Entities in consecutive period pairs | Max data, no survival bias |
| VR (multi-period) | Entities present in full window [t, t+h] | Only condition for the horizon |
| Persistence (long) | "Core" entities >= 80% presence | Compromise |
| Cross-sectional var | Top-K each period | No panel needed |

---

## 5. Implementation: What Was Done

### Config additions (types.py)

| Parameter | Default | Purpose |
|---|---|---|
| `max_rank_filter` | None | Pre-filter to top K per period |
| `min_presence_frac` | 1.0 | Balanced panel presence threshold |
| `max_noise_frac` | 0.50 | Adaptive sigma_obs cap |
| `min_perm_frac` | 0.10 | Minimum permanent fraction of signal |

### Preprocessing (preprocess.py)

The rank filter is applied AFTER `canonicalize_panel` (which creates unique 1..N ranks) and period indexing:
```python
if cfg.max_rank_filter is not None:
    panel = panel[panel["rank"] <= cfg.max_rank_filter].reset_index(drop=True)
```

For large files (>1M rows), use a pre-cut parquet file (top 50k) to avoid slow canonicalization of the full 24.5M row IG file. The rank filter then reduces 50k → 20k per period.

Balanced panel:
```python
min_periods = max(1, int(np.ceil(cfg.min_presence_frac * n_periods)))
balanced_ids = ep_counts[ep_counts >= min_periods].index.to_numpy(dtype=str)
```

### Estimation (initializers.py)

Adaptive sigma_obs cap:
```python
sigma2_obs_upper = cfg.sigma_obs_bounds[1] ** 2  # existing fixed upper bound
if cfg.max_noise_frac < 1.0 and emp_median_var > 0:
    sigma2_obs_adaptive = cfg.max_noise_frac * emp_median_var / 2.0
    sigma2_obs_upper = min(sigma2_obs_upper, sigma2_obs_adaptive)
sigma2_obs = float(np.clip(sigma2_obs_est, lower, sigma2_obs_upper))
```

Permanent fraction floor (per anchor bin):
```python
signal_var = max(row.total_var - obs_noise_var, 1e-8)
se2_floor = cfg.min_perm_frac * signal_var
if se2 < se2_floor:
    se2 = se2_floor
```

### CLI (model_v43.py)

```bash
# IG with top-20k filter
python3 -u model_v43.py ig_weekly_ranked_top50k.parquet \
    --id-col user_name \
    --max-rank-filter 20000 \
    --min-presence-frac 0.50 \
    --dev-mode --skip-plots

# FB (no filter needed)
python3 -u model_v43.py ../data/raw/fb_ranked_weekly.parquet \
    --dev-mode --skip-plots
```

**Important**: Use `-u` flag for unbuffered stdout when running with output redirection, otherwise output appears blank until the program exits.

### Data files

- `data/raw/fb_ranked_weekly.parquet` — FB full data (15k entities/week)
- `data/raw/ig_ranked_weekly_cutdown.parquet` — IG top 50k cut-down (user's file, uses `account` as ID, has 2.4% duplicate rate — needs `user_name` column or higher dup threshold)
- `llm_fitting/ig_weekly_ranked_top50k.parquet` — IG top 50k cut-down (generated, has `user_name`, not committed)
- `llm_fitting/ig_weekly_ranked.parquet` — IG full data (24.5M rows, 463k/week)

The IG `account` column has ~2.4% duplicate rate per week. `user_name` has <0.1% and is the correct unique ID.

---

## 6. Current Results & Remaining Failures

### Scorecard

| Platform | Score | Config |
|---|---|---|
| FB | **11/15** | Default (no filter) |
| IG | **9/15** | `--max-rank-filter 20000 --min-presence-frac 0.50` |

### IG detailed results

| Diagnostic | Empirical | Simulated | Status | Notes |
|---|---|---|---|---|
| VR(2) | 0.542 | ~0.54 | PASS | |
| VR(4) | 0.280 | 0.264 | PASS | |
| VR(8) | 0.147 | 0.141 | PASS | |
| VR(13) | 0.092 | 0.087 | PASS | |
| ACF(1) | -0.375 | -0.394 | PASS | |
| ACF(2) | -0.042 | -0.105 | PASS | |
| RACF(1) | 0.167 | **0.777** | FAIL | Sim 4.7x too high |
| RACF(4) | 0.075 | **0.447** | FAIL | Sim 6x too high |
| RACF(13) | -0.017 | 0.036 | PASS | |
| Pers(1) | 53 | **101** | FAIL | Sim ~2x too high |
| Pers(4) | 53 | 61 | PASS | |
| Pers(13) | 49 | 22 | PASS | |
| R2(1) | 0.407 | **0.322** | FAIL | Sim too low |
| R2(4) | 0.369 | **0.289** | FAIL | Sim too low |
| R2(13) | 0.340 | **0.175** | FAIL | Sim too low |

### FB detailed results

| Diagnostic | Status | Notes |
|---|---|---|
| VR(2,4,8,13) | All PASS | |
| ACF(1,2) | All PASS | |
| RACF(1) | FAIL | sim=0.955, too high |
| RACF(4) | FAIL | sim=0.854, too high |
| RACF(13) | FAIL | sim=0.715, too high |
| Pers(1,4,13) | All PASS | |
| R2(1) | PASS | sim=0.940 |
| R2(4) | PASS | sim=0.849 |
| R2(13) | FAIL | sim=0.714, slightly too low |

### Pattern analysis of remaining failures

**RACF too high on both platforms** is the systematic issue. The simulated entities maintain rank order too well. This means the simulation doesn't shuffle ranks enough. Possible causes:
1. **Entry/exit distribution too orderly**: New entrants placed too close to their replacement's position, preserving rank structure
2. **Observation noise doesn't affect rankings enough**: In real data, measurement noise shuffles observed ranks; if sim noise is lower than empirical noise, ranks are artificially stable
3. **Transitory component (AR(1)) too persistent**: If phi is too high, entities don't shuffle positions enough

**R2 too low on IG** means cross-sectional level correlation is weak — entity values at time 0 don't predict values at time t well enough. This is the flip side of RACF: too much rank stability but not enough value stability. This suggests the issue is in how **value** noise vs **rank** noise interact.

### Sensitivity findings

IG sensitivity analysis shows `sigma_obs` at -20% gives 11/15 (the best score), suggesting the sigma_obs estimate (0.39) is still slightly too high. Lowering it further would help R2 (by reducing noise in values) but might worsen ACF (by making changes less negatively autocorrelated).

The `kappa_base` at -10% or -20% also scores well (11/15), suggesting mean reversion might be overestimated.

### Next steps for improvement

1. **RACF calibration**: Add RACF as an explicit calibration target (not just a diagnostic). The kappa_stab and alpha_kappa calibration currently uses VR drift, R2, and persistence — adding RACF would directly address the main failure.

2. **Entry distribution diversification**: The current entry process draws new entities from the bottom quantile. This may be too conservative. Drawing from a wider distribution (or matching the empirical distribution of new entrants) would increase rank shuffling.

3. **Observation noise in rankings**: The diagnostic computes RACF on ranks after thresholding and observation noise. If the simulation's observation noise doesn't translate to enough rank shuffling, RACF will be too high. Consider whether the obs noise magnitude is correctly scaled.

4. **Lower max_noise_frac**: The sensitivity analysis suggests trying `max_noise_frac=0.40` (40% instead of 50% noise cap).

5. **Publication diagnostics**: The v4.2 had Anderson-Darling, Hill estimator, transition matrices, Shorrocks mobility, volatility clustering, half-life of persistence by stratum, Ljung-Box. These would help diagnose the remaining issues.

---

## 6b. Conditional Forecasting (added 2026-07-02)

### The problem with the unconditional gate

The OOS movement gate compares the model against a persistence baseline (predict that the
test window churns like the train window). That comparison was structurally handicapped:
persistence implicitly uses entity-level information (the train window's actual displacement
distribution reflects the actual entities and their actual gaps), while the unconditional
model simulated a *synthetic* universe — initialized from sorted period-0 values, burned in
for 40 steps, cohort chosen inside the simulation. The model was predicting the movement of
a statistically similar population, not of the population at hand. Matching persistence under
that handicap was already meaningful; beating it required conditioning.

### What conditioning means here (all inputs train-only)

Two nested levels, implemented in `rankdiff_kalman.py` and toggled by
`--conditional {state,vhat}` on the `--oos` gate:

**Level 1 — real initial state (`state`).** Simulate the actual member universe forward from
each member's *filtered* level at the end of the train window:

- Filter: steady-state scalar Kalman filter per entity on its observed weekly series, with
  band-interpolated parameters — state noise Q = σ_perm²(z̄ᵢ), measurement noise
  R = σ_obs²(z̄ᵢ) + Var(ξᵢ) (the short-lived transitory component is *folded into
  measurement noise* for filtering purposes; its state is initialized at its stationary
  mean of 0). Missing weeks propagate the state without an update (P grows by Q).
  The random-walk-level approximation ignores the small OU reversion (κ ≤ ~0.05 over
  ≤ 17-week windows) — declared, second-order.
- No burn-in. Simulation step 0 anchors at the last train week; the tracked cohort is
  formed at step 1 by observed rank — exactly mirroring the empirical cohort definition
  (top-200 by observed rank at the first test week).
- This hands the simulator the *real gap structure* of the ladder at forecast time, rather
  than a stationary-distribution draw.

**Level 2 — per-entity temperament (`vhat`).** Instead of drawing each entity's volatility
multiplier from the mixing distribution, assign each real entity its own empirical-Bayes
posterior mean (`minimal_rankdiff.eb_vhat`):

    log v̂_i = s² / (s² + ψ'(ν_i/2)) · ê_i

where ê_i is the entity's band-demeaned, bias-corrected log change-variance from the train
window, ψ'(ν_i/2) is its χ² sampling noise (Satterthwaite effective df), and s is the
platform temperament spread (§ MODEL_STATUS 2c). Entities with fewer than 8 observed
changes get the prior (v̂ = 1); the v̂ are renormalized to mean 1 so band-level variance —
and hence the Eulerian structure — is preserved. This is textbook variance moderation
(Smyth 2004) applied as a *forecasting input* rather than an estimation device. Reborn
entities in the simulation draw fresh multipliers from the mixing distribution.

Everything is computed from train data only; the σ_obs scale calibration protocol is
unchanged (train moment vector, as before).

### Results (rolling-origin, 5 splits; persistence baselines: Reddit 0.168 ± 0.004, FB 0.146 ± 0.022)

| spec | Reddit rel err | coverage | FB rel err | coverage |
|---|---|---|---|---|
| unconditional | 0.171 ± 0.017 | 100% | 0.158 ± 0.027 | 60% |
| conditional: state | **0.118 ± 0.061** | 100% | 0.152 ± 0.043 | 40% |
| conditional: state+v̂ | 0.148 ± 0.059 | 100% (Wass. 1.3–2.0, best) | 0.161 ± 0.035 | 40% |

**Reddit conditional-state beats persistence on 4 of 5 splits** (0.041 vs 0.167; 0.071 vs
0.171; 0.128 vs 0.162; 0.131 vs 0.168; the miss is the shortest train window) with 100%
bootstrap-CI coverage — the first specification to clear the gate's full bar.

### Attribution and honest caveats

- **The real initial state is the main lever**, not v̂. The gap structure at forecast time
  carries most of the conditional information. v̂ produces the tightest *distributional*
  match recorded (Wasserstein 1.3–2.0) but is slightly behind state-only on the
  moment-vector error.
- Under the Spec-B specification (σ_trans = 0), temperament only scales observation noise,
  so v̂'s conditioning power is structurally limited there (0.220 ± 0.050).
- At the shortest train origin (11 changes), `estimate_temperament`'s min_changes = 12
  silently disables temperament (s = 0), making the two conditional variants identical on
  that split. Pending robustness fix for short windows.
- FB stays at par (its persistence baseline is stronger, and its noise split remains
  gate-calibrated rather than Spec-B-identified); CI coverage dips 60→40% even as
  late-split held-out distributions become essentially exact (dRank1 13/64 vs emp 14/65).
  The identified next lever for FB is the noise identification, not further conditioning.

---

## 6c. The VR Over-Persistence Arc (2026-07-03): layered diagnosis, measured fixes, and what it taught the model

Full numbers: MODEL_STATUS §2i–§2l. This section records the METHOD — how one
persistent error pattern (simulated 4–13-week variance ratios far above empirical,
on every platform, surviving σ_obs identification) was decomposed into four layers,
each measured before it was coded, two of which were artifacts and two of which
were real structure.

**The decomposition (comments VR13, empirical 0.136):**

| layer | sim VR13 | nature | fix | identification moment |
|---|---|---|---|---|
| baseline (md6) | 0.355 | artifact: κ unidentified | `--md-vr` | D(h) = Var(h-wk change), h ∈ {2,4,8,13} — SSE(a) flat under γ₀..γ₆ alone, sharply V-shaped with D(h) |
| + κ identified | 0.284 | artifact: integrated common factor | `--stat-factor` | measured platform-level path: level VR13 = 0.121, ΔL white → stationary AR(1) level, ρ_L per platform |
| + stationary factor | 0.232 | structure: missing medium timescale (FB-binding) | `--two-scale` | D(h) curvature vs near-zero γ tail (B₂ = V₂(1−φ₂)² tiny) — slow-home grid for label separation |
| + mix heterogeneity | 0.218 | structure: permanent-share heterogeneity | `--mix-hetero` | s(h) horizon moment: b = s(h*)/s(1); FLAT s(h) ⇒ b ≈ 1 |
| residual | ~0.08 | measurement/scoring | (open) | estimation-vs-scorecard population asymmetry: the 70%-presence complete-column scoring filter selects empirically quiet entities; the sim has no missingness process |

**Methodological lessons worth keeping:**
1. *Long-horizon variance moments identify slow reversion where short-lag
   autocovariances cannot* (Cochrane 1988; Poterba & Summers 1988; Fama & French
   1988). The OU tail is spread thinly over many lags (each γ_k ≈ noise) while the
   variance of long differences aggregates it. Corollary: the moment set needs
   T ≫ h·k — md-vr degrades short-panel fits (subs 14→13, FB OOS regression), so it
   is a LONG-PANEL tool, gated by the OOS acceptance criterion.
2. *Pooled vs median statistics diagnose heterogeneity*: the estimator fits pooled
   moments; the scorecard reports the median entity. When pooled fits and the median
   misses, the gap is a mixing distribution or a population-selection asymmetry —
   not dynamics. Both appeared here (mix b; the open scoring-filter item).
3. *Simulator assumptions are moment claims*: integrating the common factor into μ
   asserts a random-walk platform level; measurement said mean-reverting (level
   VR13 = 0.121). Every "plumbing" choice in a generative model is testable.
4. *Each fix pre-registered predictions and was validated against all three goals*
   (aggregate structure, OOS movement, parsimony), with the OOS gate as the
   binding acceptance criterion — it rejected md-vr for FB gates (2i) and accepted
   mix-hetero (comments conditional 0.159 vs 0.160, 100% coverage).

**The b ≈ 1 finding (structure B measured).** s(h) — temperament dispersion from
non-overlapping h-week changes — is flat: comments 0.692→0.747 (h=1→13), FB Era A
0.890→0.912 (h=1→8) ⇒ b = 1.08 / 1.02. With b = 1 the model FACTORIZES: every
entity follows the same rank-conditional stochastic process up to ONE entity-specific
amplitude v_i (lognormal, spread s). This is the attention-dynamics analogue of
Sinatra et al.'s (2016) Q-model of scientific careers (random-impact rule × one
person-specific multiplier) — a law-like, publishable statement, and a PARSIMONY
opportunity: testing the b = 1 restriction would remove a parameter.

### Publication positioning (2026-07-03 assessment)

**Closest published relatives.** (i) Iñiguez, Pineda, Gershenson & Barabási,
"Dynamics of ranking" (Nat. Comms 2022): ~30 ranking lists, universal
openness/churn regularities, a 2-parameter displacement+replacement model —
qualitative stylized-fact reproduction, no measurement-error model, no
out-of-sample forecasting. (ii) Blumm et al. (PRL 2012) ranking dynamics;
(iii) attention-dynamics classics: Wu & Huberman (PNAS 2007) collective attention
decay, Lorenz-Spreen et al. (Nat. Comms 2019) accelerating attention; (iv) the
size-distribution tradition (Gabaix 1999; Luttmer 2007; Axtell, Science 2001);
(v) Sinatra et al. (Science 2016) Q-model — the entity-amplitude precedent.

**Where we are stronger than the field standard:** (a) identification discipline —
σ_obs from an independent daily-replication instrument, κ from long-horizon
variances, b from the horizon-dispersion moment: nothing in the ranking-dynamics
literature separates measurement noise from dynamics at all; (b) out-of-sample
validation against a persistence baseline with distributional coverage — rare
anywhere in this literature; (c) instrument forensics (CrowdTangle era
segmentation, census-vs-censored coverage language) — reviewers who care about
data quality will notice; (d) quantitative moment matching (FB Era A 15/15)
rather than stylized-fact reproduction.

**Where we are weaker:** (a) breadth — 2 platforms / 3 metrics vs their ~30
systems; universality is THE currency at PNAS/Nat Comms; (b) apparent complexity —
per-knot curves read as "a big state-space fit" to a physics referee; the counter
is that the MECHANISM list is short (slow home + reversion, fast/medium
transitory, measurement noise, one entity amplitude, Gabaix rebirth) and the
curves are nonparametric rank-dependence, but the b=1 factorization is the framing
that makes this legible; (c) no figures yet — this literature communicates through
data collapse (displacement distributions across horizons/platforms on one curve;
v̂_i distributions collapsing onto one lognormal across metrics; rank-size
stationarity vs occupant churn).

**Steering recommendation:** spend the next complexity budget on BREADTH and
ELEGANCE, not dynamics: (1) test the b=1 restriction (parsimony win + the paper's
law); (2) population-matched scoring (closes the last honest residual with zero
new model parameters); (3) 2–4 cheap additional ranked systems through the
unchanged pipeline (Wikipedia pageviews, YouTube, GitHub stars, app charts) — each
is one PLATFORMS entry; (4) the figure set above. The paper's spine: *one
rank-conditional law, one entity amplitude, identified measurement model,
validated out of sample — across platforms, metrics, and instrument eras.*

### Publication positioning update (2026-07-05, post-hardening; supersedes the spine above)

Two external review rounds adjudicated (MODEL_STATUS §2q, §2x; both saved in
`reviews/`). Revised external verdict: "credible, deeply validated
model-discovery paper; remaining PNAS risks are confirmation and breadth, not
internal statistical mechanics." Venue read: PNAS credible AFTER the registered
confirmation extension + two breadth systems; Nature Communications possibly
close already if framed as an identification-and-validation advance;
computational-social-science flagship strong now pending confirmation.

Steering items (1) and (2) above are now DONE and adjudicated: b = 1 is not
rejected under time-window uncertainty (block-bootstrap CIs contain 1 on both
platforms; conservative bound b ∈ [0.94, 1.11]) — b = 1 is the MAIN model,
measured b the refinement; population-matched scoring is measured DEAD
(census 99% present, censoring moves nothing). The comments VR residual is
decomposed: ~half is the scored-VR functional's non-Gaussianity discount
(spectrum-preserving surrogates), the model's t-tails supply only ~24% of that
discount, and the honest remaining dynamics target is ~+0.03–0.04.

**The revised spine (binding):** *digital attention rankings obey an
approximately factorized rank-diffusion law — a shared rank-conditional
stochastic process with independently bounded measurement noise and boundary
rebirth, multiplied to first order by one persistent endpoint amplitude —
predicting held-out rank movement and reproducing the main churn structure
across Facebook tracked activity and Reddit census attention, with residual
deviations localized to low-frequency structure and bounded reversion
heterogeneity.* Claim discipline: structure vs movement stacks main-text with
a target column; FB movement leads with the identified Spec-B + conditional
spec; "15/15" descriptive + Q as residual localization; "excess low-frequency
structure" replaces lifecycle-arc lead language; dominant one-amplitude
factorization, never "all heterogeneity is amplitude" (κ_i log-SD ≈ 0.30,
bounded, pre-registered as extension diagnostic E4).

---

## 7. Key Sources

### Signal extraction & pile-up problem
- Stock & Watson (1998) — Median Unbiased Estimation, JASA. [link](https://www.tandfonline.com/doi/abs/10.1080/01621459.1998.10474116)
- Harvey (1989) — Forecasting, Structural Time Series Models, Kalman Filter. Cambridge UP. [link](https://www.cambridge.org/core/books/forecasting-structural-time-series-models-and-the-kalman-filter/CE5E112570A56960601760E786A5E631)
- Harvey & Peters (1990) — Estimation Procedures for Structural Time Series. J. Forecasting. [link](https://onlinelibrary.wiley.com/doi/abs/10.1002/for.3980090203)
- Kamber, Morley & Wong (2018) — BN Filter with signal-to-noise constraint. REStat. [link](https://direct.mit.edu/rest/article/100/3/550/58466/Intuitive-and-Reliable-Estimates-of-the-Output-Gap)
- Auger-Methe et al. (2016) — State-Space Models' Dirty Little Secrets. Scientific Reports. [link](https://www.nature.com/articles/srep26677)
- Shephard (1993) — ML estimator pile-up. Econometric Theory. [link](https://www.cambridge.org/core/journals/econometric-theory/article/abs/distribution-of-the-ml-estimator-of-an-ma1-and-a-local-level-model/041A2D1674D5AD12451F9E534A050125)
- Perron & Wada (2009) — Pile-Up Problem in Trend-Cycle Decomposition. [link](https://mpra.ub.uni-muenchen.de/51118/)
- Cochrane (1988) — How Big Is the Random Walk in GNP? JPE. [link](https://www.journals.uchicago.edu/doi/abs/10.1086/261569)

### Zipf's law & rank dynamics
- Gabaix (1999) — Zipf's Law for Cities. [link](https://www.jstor.org/stable/2586883)
- Gabaix & Ibragimov (2011) — Rank-1/2 estimation. [link](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=881759)
- Gabaix & Landier (2008) — CEO Pay and Firm Size. [link](https://www.nber.org/system/files/working_papers/w12365/w12365.pdf)
- Luttmer (2007) — Selection, Growth, Size Distribution of Firms. QJE. [link](http://users.econ.umn.edu/~luttmer/research/fd/Luttmer_QJE.pdf)
- Axtell (2001) — Zipf Distribution of U.S. Firm Sizes. Science. [link](https://pubmed.ncbi.nlm.nih.gov/11546870/)
- Saichev, Malevergne & Sornette (2010) — Theory of Zipf's Law and Beyond. Springer. [link](https://link.springer.com/book/10.1007/978-3-642-02946-2)
- Clauset, Shalizi & Newman (2009) — Power-Law Distributions in Empirical Data. [link](https://arxiv.org/abs/0706.1062)

### Atlas model & rank-based diffusion
- Banner, Fernholz & Karatzas (2005) — Atlas Models of Equity Markets. [link](https://projecteuclid.org/euclid.aoap/1133965764)
- Ichiba, Papathanakos, Banner, Karatzas, Fernholz (2011) — Hybrid Atlas Models. [link](https://arxiv.org/abs/0909.0065)

### Panel data with selection & entry/exit
- Cabral & Mata (2003) — Evolution of Firm Size Distribution. AER. [link](https://www.aeaweb.org/articles?id=10.1257/000282803769206205)
- Clementi & Palazzo (2013) — Entry, Exit, Firm Dynamics. [link](https://www.nber.org/system/files/working_papers/w19217/w19217.pdf)
- Klosin (2024) — Dynamic Biases of Static Panel Data Estimators. [link](https://klosins.github.io/Klosin_JMP.pdf)
- Ijiri & Simon (1977) — Skew Distributions and Business Firms. North-Holland.

### Long-horizon variance / mean-reversion identification (added 2026-07-03, §6c)
- Poterba & Summers (1988) — Mean Reversion in Stock Prices. JFE. [link](https://economics.mit.edu/sites/default/files/publications/1-s2.0-0304405X88900219-main.pdf)
- Fama & French (1988) — Permanent and Temporary Components of Stock Prices. JPE. [link](https://www.journals.uchicago.edu/doi/10.1086/261535)
- (Cochrane 1988 above is the variance-of-long-differences foundation.)

### Ranking / attention dynamics venue landscape (added 2026-07-03, §6c)
- Iñiguez, Pineda, Gershenson & Barabási (2022) — Dynamics of Ranking. Nature Communications. [link](https://www.nature.com/articles/s41467-022-29256-x)
- Blumm et al. (2012) — Dynamics of Ranking Processes in Complex Systems. PRL. [link](https://journals.aps.org/prl/abstract/10.1103/PhysRevLett.109.128701)
- Wu & Huberman (2007) — Novelty and Collective Attention. PNAS. [link](https://www.pnas.org/doi/10.1073/pnas.0704916104)
- Lorenz-Spreen et al. (2019) — Accelerating Dynamics of Collective Attention. Nature Communications. [link](https://www.nature.com/articles/s41467-019-09311-w)
- Sinatra et al. (2016) — Quantifying the Evolution of Individual Scientific Impact (the Q-model; entity-amplitude precedent for b=1). Science. [link](https://www.science.org/doi/10.1126/science.aaf5239)

### Other
- Gabaix (2016) — Power Laws in Economics: An Introduction. JEP. [link](https://www.aeaweb.org/articles?id=10.1257%2Fjep.30.1.185)
- Morley, Nelson & Zivot (2003) — Why BN and UC Decompositions Differ. REStat. [link](https://ideas.repec.org/a/tpr/restat/v85y2003i2p235-243.html)
- Cameron (2022) — Zipf's Law across Social Media. [link](https://repec.its.waikato.ac.nz/wai/econwp/2207.pdf)
- Durbin & Koopman (2012) — Time Series Analysis by State Space Methods. Oxford. [link](https://academic.oup.com/book/16563)

---

*Last updated: 2026-07-03 (§6c + venue landscape). Originally generated during the v4.3 development session (2026-03-12).*
