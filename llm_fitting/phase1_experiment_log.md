# Phase 1 Experiment Log: Kappa Calibration Rebalancing

## Baseline (pre-changes, model_v43 with default scoring)

### Reddit (215 days, top 5K, dev-mode)
- **Score: 7/15**
- alpha_kappa: 0.300, kappa_stab_factor: 1.200
- VR: 4/4, ACF: 2/2, RACF: 0/3, Pers: 0/3, R²: 1/3
- RACF(1): sim=0.026 vs emp=0.212 (gap=0.186)
- Pers(1): sim=31.4 vs emp=41
- R²(1): sim=0.680 vs emp=0.777

### Facebook (176 weeks, full data, dev-mode) — prior known result
- **Score: 15/15** (full MC), 13/15 (dev-mode, varies by seed)
- alpha_kappa: 0.100, kappa_stab_factor: 0.800

### Instagram (53 weeks, top 20K, dev-mode) — prior known result
- **Score: 11/15**
- Pers(1): FAIL (sim 116.6 vs emp 53)
- R²(1,4,13): all FAIL (sim 0.59-0.63 vs emp 0.34-0.41)

---

## Experiment 1: Rebalanced Scoring Function

**Changes to `_candidate_score()` in fit.py:**
- Drift penalty: relaxed from /0.2 to /0.4
- RACF weight: doubled from 1x to 2x

**Changes to `types.py`:**
- kappa_stab_grid: expanded from (0.8,...,2.0) to (0.5,...,2.0)

### Result: Reddit
- **Score: 7/15** (unchanged)
- alpha_kappa: 0.300 (unchanged), kappa_stab_factor: 1.200 (unchanged)
- All diagnostics identical — rebalancing did not change parameter selection
- **Conclusion**: RACF failure is structural, not a calibration sensitivity issue.
  The model produces RACF ≈ 0.03 regardless of kappa parameters.

### Result: Facebook (REGRESSION)
- **Score: 13/15** (was 15/15 previously)
- alpha_kappa: 0.800 (was 0.100), kappa_stab_factor: 1.500 (was 0.800)
- RACF(1): sim=0.830 vs emp=0.477 — FAIL (too high, overshoot)
- RACF(4): sim=0.627 vs emp=0.277 — FAIL
- **Conclusion**: Doubled RACF weight caused calibration to overshoot on FB.
  The single-rep calibration sim is too noisy to benefit from reweighting.

### Decision: REVERT scoring changes. Calibration rebalancing cannot fix the
structural RACF problem and introduces regression risk on platforms that were
already working.

---

## Key Finding

The RACF, Pers, and R² failures on Reddit (and partially on IG) are **not
addressable through kappa calibration**. The calibration grid search selects
the same parameters regardless of scoring weights because:

1. RACF is not meaningfully controlled by kappa — it's determined by
   observation noise, gap structure, and transitory dynamics
2. The 1-rep calibration sim has too much variance to reliably distinguish
   grid points under reweighting
3. The structural problem is that the model lacks position-dependent rank
   noise and common engagement factors

**Recommendation**: Skip remaining Phase 1 experiments (finer bins alone won't
help) and proceed directly to Phase 2 (common factor) and Phase 3
(position-dependent rank noise), which address the root causes identified in
the diagnosis.

---

## Phase 2: Common Engagement Factor

### Implementation

Added a common engagement factor F(t) to the simulation:
- **Estimation** (diagnostics.py): F(t) = cross-sectional mean of log-changes per
  period. Estimate φ_F (AR(1) persistence), σ_F (volatility), and per-entity
  loadings β_i via regression. Summarize β_i by rank band into cf_loading_curve.
- **Types** (types.py): Added cf_sigma, cf_phi, cf_loading_curve to EstimatedParams.
- **Simulator** (simulator.py): F(t) evolves as AR(1); each entity gets
  β(rank) × F(t) added to x_true before ranking.
- **No σ_eta reduction**: Tried scaling down permanent variance by cf_r2 — this
  degraded FB by over-reducing idiosyncratic signal. Reverted.

### Empirical Common Factor Estimates

| Platform | cf_sigma | cf_phi | cf_r2_median | Loading range |
|----------|----------|--------|-------------|---------------|
| Reddit   | 0.048    | -0.22  | 1.1%        | [0.90, 0.98]  |
| Facebook | 0.430    | -0.04  | 31.7%       | [0.97, 1.00]  |

**Key finding**: Reddit's common factor is tiny (1.1% of variance). FB's is
large (31.7%) but has near-uniform loadings → moves all entities equally →
no rank effect, just excess level variance.

### Results (dev-mode, 5 MC reps)

**Variant: Full loadings, no σ_eta reduction**

| Platform | Score | vs Baseline | Key changes |
|----------|-------|-------------|-------------|
| Reddit   | 10/15 | +3 (was 7)  | Pers(1) 31→34, R²(1) 0.68→0.73 |
| Facebook | 12/15 | -3 (was 15) | VR(8) FAIL, RACF(1,4) FAIL |

**Variant: σ_eta reduced by cf_r2**

| Platform | Score | Key changes |
|----------|-------|-------------|
| Reddit   | 11/15 | Lucky calibration seed (alpha_kappa=0.5 vs 0.3) |
| Facebook | 12/15 | Same regression |

**Variant: De-meaned loadings (heterogeneous only)**

| Platform | Score | Key changes |
|----------|-------|-------------|
| Reddit   | 10/15 | Factor effectively disabled (spread ≈ 0.08) |
| Facebook | 13/15 | Factor effectively disabled |

### Analysis

1. **Reddit**: The common factor is negligible (1.1% R²). The 7→10 improvement
   is real but comes from the expanded kappa_stab grid (0.5-0.7 now available)
   interacting with the factor's small perturbation to the variance landscape.
   The RACF gap remains enormous: sim 0.03 vs emp 0.21.

2. **Facebook**: The common factor hurts. σ_F=0.43 adds large common variance
   with uniform loadings → inflates VR without helping ranks. FB's empirical
   common factor is essentially a platform-wide noise term that the model
   already captures via σ_obs.

3. **Core issue**: The common factor helps Pers and R² modestly (+3 points on
   Reddit) but does NOT fix RACF. The rank autocorrelation failure requires a
   mechanism that directly operates in rank space — the common factor operates
   in level space and translates poorly to rank improvements when loadings
   are near-uniform.

### Decision

Keep the common factor infrastructure (it's architecturally clean and may help
more when loadings are heterogeneous on other platforms or date ranges). The
current code is committed. Proceed to Phase 3 (position-dependent rank noise)
which directly targets the RACF gap.

### Remaining changes in codebase
- types.py: expanded kappa_stab_grid to (0.5,...,2.0), added cf_* fields
- diagnostics.py: computes cf_phi, cf_sigma, cf_r2_median, cf_loading_by_z
- fit.py: estimates cf_loading_curve from empirical data, passes to EstimatedParams
- simulator.py: F(t) AR(1) state with rank-dependent loadings added to x_true
- model_v43.py: prints cf diagnostic info

---

## Phase 3: Position-Dependent Rank Noise

### Experiment 1: Gap-Scaled Observation Noise

**Idea**: Scale sigma_obs inversely with local metric gap. Top entities (large
gaps) get less noise → more stable ranks. Middle entities (small gaps) get
more noise.

**Implementation**: After sorting x_true, compute gap between adjacent entities.
Scale sigma_obs by (median_gap / local_gap), clipped to [0.2, 5.0].

**Result**: Both platforms collapsed to 7/15. The scaling INCREASED noise for
middle entities (gap_scale up to 5x), destroying VR, ACF, and all rank
diagnostics. Middle entities got tripled observation noise → even MORE rank
shuffling.

**Verdict**: REVERTED. Gap-scaled noise amplifies the problem, not fixes it.

### Experiment 2: sigma_obs Sweep (max_noise_frac)

**Idea**: Reduce sigma_obs by capping the noise fraction, check RACF response.

| max_noise_frac | sigma_obs | RACF(1) | Score |
|----------------|-----------|---------|-------|
| 0.50 (default) | 0.2495    | 0.026   | 7-10/15 |
| 0.30           | 0.1933    | 0.066   | 9/15 |
| 0.20           | 0.1578    | 0.076   | 9/15 |
| 0.10           | 0.1116    | 0.098   | 10/15 |
| 0.05           | 0.0789    | 0.091   | 10/15 |

**Key finding**: Even reducing sigma_obs by 70% (from 0.25 to 0.08), RACF
only reaches 0.09 — still far from emp 0.21. RACF is NOT sensitive to
sigma_obs because sigma_obs is not the bottleneck.

### Gap Structure Analysis

Computed empirical gaps between adjacent ranked entities:

| Rank | Empirical gap | sigma_obs | Ratio |
|------|-------------|-----------|-------|
| 1-2  | 0.265       | 0.250     | 1.1x  |
| 5-6  | 0.043       | 0.250     | 0.2x  |
| 10-11| 0.025       | 0.250     | 0.1x  |
| 50-51| 0.016       | 0.250     | 0.06x |
| 100  | 0.000       | 0.250     | ~0x   |

sigma_obs dwarfs ALL gaps below rank 2. But the simulated gap structure
(after burn-in) is comparable to empirical — median gaps within 20%.

**Root cause confirmed**: The RACF failure is not about observation noise
magnitude or gap structure. It's about the DYNAMICS: the idiosyncratic
random walk (sigma_eta ≈ 0.11 per period) creates ~16 positions of rank
movement per period, while sigma_obs adds ~36 positions of noise. Even
without obs noise, the signal-driven rank changes are too volatile.

### Why This Approach Failed

The position-dependent noise idea from the Wright-Fisher literature
(Iñiguez et al. 2022) assumes that rank dynamics are PRIMARILY driven
by rank-space noise. In our model, rank dynamics are DERIVED from
level-space dynamics. The level → rank mapping is highly nonlinear:
- sigma_obs/gap ≈ 30x at typical rank positions
- The rank ordering is almost entirely determined by the permanent
  component tau, not by obs noise
- Reducing obs noise doesn't help because the permanent shocks
  (sigma_eta) ALSO create large rank movements

The fundamental issue: our permanent-transitory model generates
entities that evolve as INDEPENDENT random walks with mean reversion.
In the empirical data, entities that are near each other in rank
co-move (platform algorithm, competition, network effects), which
preserves relative ordering. Our model misses this LOCAL correlation.

### Recommendation for Next Steps

The RACF gap (sim 0.03-0.09 vs emp 0.21) requires a mechanism that
creates LOCAL rank persistence — not just common factor (which is global)
or observation noise (which is too small to matter).

Candidates:
1. **Local interaction model**: Entities near each other in rank
   experience correlated shocks (borrowing from the SPT collision
   literature — Itkin & Larsson 2024)
2. **Rank-dependent permanent volatility**: sigma_eta much lower for
   top entities, creating a "stable core" at the top
3. **Alternative rank computation**: Use a smoothed/blended ranking
   (e.g., exponential moving average of metrics across periods)
   rather than fresh ranking each period

---

## Phase 4: Collision-Rate Calibration

### Implementation

Added collision-rate diagnostic to both empirical and simulated outputs:
- For key rank positions (1, 2, 5, 10, 20, 50), compute the fraction of
  periods where the entity holding that rank changes.
- Added to diagnostics.py: empirical collision_emp dict, simulated coll{k}
- Added to model_v43.py: collision rate comparison table in scorecard output
- Extended alpha_kappa_grid to include negative values (-0.3, -0.2, -0.1)

### Key Empirical Finding: Collision Rate Profile

Reddit daily collision rates (entity turnover at each rank):

| Rank | Empirical | Description |
|------|-----------|-------------|
| 1    | 0.696     | Top entity changes 70% of days |
| 2    | 0.804     | 80% turnover |
| 5    | 0.902     | 90% turnover |
| 10   | 0.925     | 93% turnover |
| 20   | 0.958     | 96% turnover |
| 50   | 0.991     | 99% turnover |

### Collision Profile: Model vs Data

| alpha_kappa | Score | Coll(1) gap | Coll(10) gap | Pers(1) | R²(1) |
|-------------|-------|-------------|-------------|---------|-------|
| -0.2        | 6/15  | +0.24       | +0.07       | 13.2    | 0.45  |
| -0.1        | 5/15  | +0.14       | +0.07       | 22.0    | 0.60  |
| **0.0**     | 6/15  | **+0.05**   | +0.05       | 27.6    | 0.64  |
| **0.3**     | 10/15 | **-0.57**   | +0.03       | 34.4    | 0.73  |

### CRITICAL FINDING: Structural Incompatibility

The collision data reveals a fundamental tension in the model:
- **Matching collision rates** at the top requires alpha_kappa ≈ 0 (flat kappa)
  → gives Coll(1) gap of only +0.05
- **Matching Pers/R²** requires alpha_kappa > 0 (protective kappa at top)
  → gives Pers(1) = 34 close to emp 41, but Coll(1) gap = -0.57

These are structurally incompatible because the SAME kappa controls both.

**The empirical data shows something the model cannot produce**: top entities
have BOTH high collision rates (70% daily turnover) AND high persistence
(34/50 remain after 13 days). This is only possible if many entities at the
top are CLOSE in metric value and shuffle positions among themselves while
all remaining in the top group.

This is a "turbulent top cluster" — a group of ~50 entities that are all
large enough to dominate, swapping positions freely but rarely falling out
of the top group. The current PT model cannot produce this because its
rank-dependent kappa either:
- Protects the top (high Pers, low Coll) with positive alpha_kappa
- Exposes the top (low Pers, high Coll) with zero/negative alpha_kappa

### Implications for Model Architecture

To match both Pers AND Coll, the model needs:
1. **Clustered metric values at the top**: A mechanism that compresses the
   metric gap between top entities (so they swap easily) while maintaining
   a gap between the top cluster and everything else (so they persist as a
   group).
2. **Correlated shocks within rank clusters**: Entities near each other in
   rank share correlated noise, preserving within-cluster ordering on
   average while allowing frequent swaps.
3. **Alternatively, multi-period ranking**: If rank is computed from a
   smoothed metric (e.g., 7-day average), the top group persists while
   day-to-day rankings within the group are volatile.

### Changes to codebase
- diagnostics.py: collision_emp and coll{k} diagnostics added
- model_v43.py: collision rate comparison table in output
- types.py: alpha_kappa_grid extended with negative values (-0.3, -0.2, -0.1)
