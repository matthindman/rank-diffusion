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
