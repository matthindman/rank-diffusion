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

### Other
- Gabaix (2016) — Power Laws in Economics: An Introduction. JEP. [link](https://www.aeaweb.org/articles?id=10.1257%2Fjep.30.1.185)
- Morley, Nelson & Zivot (2003) — Why BN and UC Decompositions Differ. REStat. [link](https://ideas.repec.org/a/tpr/restat/v85y2003i2p235-243.html)
- Cameron (2022) — Zipf's Law across Social Media. [link](https://repec.its.waikato.ac.nz/wai/econwp/2207.pdf)
- Durbin & Koopman (2012) — Time Series Analysis by State Space Methods. Oxford. [link](https://academic.oup.com/book/16563)

---

*Last updated: 2026-03-12. Generated during v4.3 development session.*
