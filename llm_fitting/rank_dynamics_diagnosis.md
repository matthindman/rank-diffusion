# Research Report: Rank Dynamics Diagnostic Failures

## 1. Problem Statement

The permanent-transitory (PT) decomposition model captures **variance ratios and level autocorrelation** well across all three platforms (Facebook, Instagram, Reddit). But three diagnostic families systematically fail — all relating to how metric-level dynamics translate to rank dynamics:

| Diagnostic | FB (weekly) | IG (weekly) | Reddit (daily) |
|---|---|---|---|
| VR (2,4,8,13) | 4/4 | 4/4 | 4/4 |
| ACF (1,2) | 2/2 | 2/2 | 2/2 |
| **RACF (1,4,13)** | 3/3 | 3/3 | **0/3** |
| **Pers (1,4,13)** | 3/3 | 1/3 | **0/3** |
| **R² (1,4,13)** | 3/3 | 0/3 | **1/3** |

The failures all involve the **rank transformation**: computing ranks from simulated levels and comparing the rank-domain properties to data. The model matches levels but not ranks.

## 2. The Core Tension

The empirical Reddit data shows a striking combination:

- **RACF ≈ 0.21 at lag 1**: Ranks are moderately autocorrelated — entities mostly keep their positions day-to-day
- **Simulated RACF ≈ 0.03**: The model shuffles ranks almost randomly each period
- **Empirical Pers(13) = 34/50**: 68% of top-50 subreddits remain in top-50 after 13 days
- **Simulated Pers(13) = 15/50**: Only 30% survive — ranks are far too volatile
- **Empirical R²(1) = 0.78**: Cross-sectional levels are highly persistent
- **Simulated R²(1) = 0.68**: Model underpredicts level persistence

The model produces **too much rank turbulence** (low RACF, low Pers) while simultaneously **too little level persistence** (low R²). This is not a simple calibration issue — it's a structural modeling gap.

## 3. Differential Diagnosis

### 3.1 Missing Mechanism: Position-Dependent Rank Noise

**Hypothesis**: The nonlinear mapping from metrics to ranks creates heterogeneous noise amplification that the model ignores.

**Evidence**: Iñiguez et al. (2022, *Nature Communications*) show that rank diffusion follows a Wright-Fisher equation with variance proportional to r(1-r)/N, where r is normalized rank. This means:
- **Top ranks**: Large metric gaps between adjacent entities → a shock of size σ barely changes rank
- **Middle ranks**: Small gaps → the same σ creates many rank swaps
- **Bottom ranks**: Entry/exit creates discontinuous jumps

Our model applies observation noise uniformly in log-metric space (σ_obs), but the rank effect of this noise varies enormously by position. At the top (where Pers and RACF are measured), the noise creates far less rank disruption than the model assumes when it ranks all entities simultaneously.

**Likelihood**: HIGH. This is a fundamental property of the rank transformation that the model does not account for.

### 3.2 Missing Mechanism: Common Factor with Heterogeneous Loadings

**Hypothesis**: Platform-wide engagement shocks move all entities together, preserving relative ordering (boosting RACF and R²), but our model treats all shocks as idiosyncratic.

**Evidence**: Herskovic et al. (2016, *JFE*) show that even after removing systematic factors, firms' idiosyncratic volatilities share a common factor explaining 35% of variation. Gabaix (2011, *Econometrica*) shows that in fat-tailed distributions, idiosyncratic shocks to the largest entities create aggregate fluctuations. Reddit's r/all algorithm creates explicit cross-sectional dependence (Aridor et al., 2020; Reddit r/popular audit, 2025).

A common factor that lifts or depresses all entities simultaneously would:
- **Increase RACF**: Relative ordering preserved when all move together
- **Increase R²**: Cross-sectional correlation persists if entities move in parallel
- **Not affect VR or ACF**: These measure entity-level dynamics, which are unchanged

**Likelihood**: HIGH for Reddit and IG. FB may have weaker common factors due to more heterogeneous content types.

### 3.3 Missing Mechanism: Size-Dependent Volatility (Stanley Scaling)

**Hypothesis**: Larger entities have systematically lower growth-rate volatility, creating rank stability at the top that the model's rank-independent σ_eta curve misses.

**Evidence**: Stanley et al. (1996, *PNAS*) and Amaral et al. (1997) show σ(S) ∝ S^{-β} with β ≈ 0.2 for firms. If top subreddits (r/pics, r/politics) have inherently lower volatility than mid-tier subreddits, the rank ordering at the top is more stable than a model with rank-independent permanent shocks would predict.

Our model has rank-dependent σ_eta via the z_knots curve, but:
- Only 2-3 knots (bins) are used
- The curve is estimated from variance ratios, not directly from rank-volatility profiles
- The curve may not capture the steep decline at the very top

**Likelihood**: MODERATE. The existing rank-dependent curves partially address this, but may need finer resolution or direct calibration to rank-volatility data.

### 3.4 Kappa Calibration Targeting the Wrong Diagnostics

**Hypothesis**: The calibration grid search optimizes a composite objective (variance drift + R²(13) + Pers(13) + RACF(1)), but the optimal parameters for this composite systematically sacrifice short-horizon rank dynamics.

**Evidence**: The ablation study shows that the kappa_stab stage **hurts** on Reddit (net -2 diagnostics) and the final full model scores lower than intermediate stages. Specifically:
- Pre-kappa-stab: 9/15 with RACF, Pers partially passing
- Post-kappa-stab: 7/15 with VR(8,13) gained but RACF, Pers, R² lost

The calibration scoring function weights variance drift (|drift - 1.0| / 0.2) heavily, pushing kappa_stab upward, which over-stabilizes cross-sectional variance at the expense of rank dynamics.

**Likelihood**: HIGH. This is a calibration design issue, not a missing mechanism.

### 3.5 Entry/Exit Model Too Symmetric

**Hypothesis**: The entry/exit process replaces departing entities with entities drawn from the departing distribution + noise, which preserves the rank structure too well (or not enough, depending on direction).

**Evidence**: Luttmer (2007) shows that in firm dynamics, entry creates a stream of small entities at the bottom — not replacements at the position of the exiting entity. Our model's "departing_vals shuffled + noise" approach preserves the cross-sectional distribution by construction, which may be unrealistic.

For Reddit specifically: exit_p_base = 0.26 (26% daily churn out of top-5K) is enormous. If exiting entities are replaced by entities drawn from the bottom, this should create massive rank disruption at the bottom while leaving the top relatively stable. But if the replacement draws from the departing distribution, it mixes top and bottom positions.

**Likelihood**: MODERATE. The entry/exit mechanics affect the rank-dynamics fit but are probably secondary to the position-dependent noise and common factor issues.

### 3.6 Transitory Component Persistence Mismatch

**Hypothesis**: The AR(1) transitory component with a single φ parameter may not capture the empirical autocorrelation structure at multiple lags, especially when ACF(2) is positive (as in Reddit: ACF(2) = +0.02).

**Evidence**: Reddit's positive ACF(2) is unusual and suggests the autocorrelation function is not monotonically decaying. A single AR(1) component produces ACF that decays geometrically: ACF(k) ∝ φ^k. The positive ACF(2) could indicate an ARMA(1,1) or AR(2) structure, or a mixture of two transitory components with different persistence.

**Likelihood**: LOW-MODERATE. This is a refinement rather than a root cause. The ACF diagnostics already pass.

## 4. Prioritized Portfolio of Fixes

### Fix A: Add a Common Engagement Factor [HIGH PRIORITY]

**What**: Add a single time-varying common factor F(t) that shifts all entities' log-metrics simultaneously:

```
x_i(t) = τ_i(t) + c_i(t) + β_i · F(t)
```

where F(t) ~ AR(1) with persistence ρ_F, and β_i are entity-specific loadings (proportional to rank or estimated from data).

**Why**: A common factor preserves relative ordering (boosting RACF) and cross-sectional correlation (boosting R²) without affecting entity-level variance ratios or ACF. This directly addresses the three failing diagnostics.

**Implementation**:
1. Estimate F(t) from cross-sectional mean of log-changes each period
2. Estimate β_i from the regression of entity changes on the common factor
3. Add F(t) as an additional state variable in the simulator
4. β_i can be rank-dependent (larger entities load more on the common factor)

**Calibration target**: Cross-sectional R² at lag 1 and RACF at lag 1.

**Risk**: May not improve Pers if the factor is too transitory. Need to calibrate ρ_F carefully.

### Fix B: Rank-Space Observation Noise (Wright-Fisher) [HIGH PRIORITY]

**What**: Replace uniform log-metric observation noise with position-dependent rank noise. After computing latent ranks, add rank jitter proportional to r(1-r)/N:

```
observed_rank_i = latent_rank_i + ε_i
ε_i ~ N(0, σ_rank · sqrt(r_i(1-r_i)/N))
```

Alternatively, implement this in metric space by making σ_obs depend on the local gap between adjacent entities: wider gaps → less effective rank noise.

**Why**: The Iñiguez et al. (2022) framework shows that rank diffusion is inherently position-dependent. Top ranks are insulated from small metric perturbations because the gap to the next entity is large. Middle ranks are sensitive because gaps are small. This heterogeneity is the primary missing mechanism.

**Implementation**:
1. After computing x_true = tau + c_state and ordering, compute the gap between adjacent ordered values
2. Scale observation noise inversely with the local gap: σ_obs_effective_i = σ_obs / gap_i
3. Or directly: after ranking, apply Wright-Fisher rank diffusion noise

**Risk**: Changes the observation model fundamentally. Needs careful integration with the existing ACF-based σ_obs estimator.

### Fix C: Collision-Rate Calibration (Itkin-Larsson) [HIGH PRIORITY]

**What**: Directly measure the empirical rate of rank swaps between adjacent entities at each rank level, and calibrate the model to match these swap rates rather than aggregate RACF.

**Why**: Itkin & Larsson (2024) show that calibrating to rank-level collision rates (the discrete analogue of local times in the Atlas model) produces much better rank dynamics fits than calibrating to aggregate statistics. This is a more targeted approach than matching RACF, which averages over all rank positions.

**Implementation**:
1. Compute empirical collision matrix: C(k) = fraction of periods where entities at rank k and k+1 swap
2. Compute the same in simulation
3. Add collision-rate matching to the calibration objective
4. Optionally: use collision rates to directly infer rank-dependent volatility

**Risk**: Requires more computation per MC rep. May need the simulation to track all rank pairs, not just tracked entities.

### Fix D: Recalibrate Kappa Objective Function [MEDIUM PRIORITY]

**What**: Modify the calibration scoring in `fit.py` to weight RACF and Pers more heavily relative to variance drift:

```python
# Current: heavily penalizes variance drift
score = |drift - 1.0| / 0.2 + |R²| / threshold + |Pers| / tol + |RACF| / threshold

# Proposed: balance rank-domain diagnostics
score = |drift - 1.0| / 0.4 + |R²| / threshold + |Pers| / tol + 2.0 * |RACF| / threshold
```

Also: expand the kappa_stab grid to include values < 0.8 (currently floors at 0.8), since Reddit may need weaker mean reversion.

**Why**: The ablation clearly shows kappa_stab hurts rank diagnostics. The current objective over-weights stationarity at the expense of rank fit.

**Risk**: May produce non-stationary variance paths if drift penalty is too weak. Need to monitor xsec_var_drift.

### Fix E: Size-Dependent Volatility via Finer Rank Bins [MEDIUM PRIORITY]

**What**: Increase the number of anchor bins from 2-3 to 6-10 for Reddit (the existing min_anchor_bins=6 config exists but may not activate for small balanced panels). Alternatively, parametrize σ_eta directly as a power law of rank: σ_eta(r) = σ_0 · r^{-β_vol}.

**Why**: The Stanley scaling relationship (σ ∝ S^{-0.2}) implies that top entities have 20-30% lower volatility than median entities. With only 2 z_knots, the current curve can't capture this gradient, especially at the very top where Pers and RACF are measured.

**Implementation**: Add `--min-anchor-bins 8` to the CLI invocation. Or modify `fit_parameter_curves()` to use a parametric form instead of piecewise-linear interpolation.

### Fix F: Lévy Walk Component for Rank Jumps [LOW PRIORITY]

**What**: Add a rank-space displacement component: with probability τ_levy per period, an entity relocates to a random rank drawn from a heavy-tailed distribution.

**Why**: Iñiguez et al. (2022) decompose rank dynamics into diffusion (local), displacement (Lévy walk), and replacement (entry/exit). Our model has diffusion and replacement but no displacement. Adding displacement could help match the empirical RACF structure, particularly at middle ranks where occasional large rank jumps occur.

**Risk**: Adds another parameter to calibrate. May conflict with the level-based dynamics (a rank jump implies a level jump, which must be consistent with the PT decomposition).

### Fix G: Two-Speed Transitory Component [LOW PRIORITY]

**What**: Replace the single AR(1) transitory component with two components:
- Fast transitory: φ_fast ≈ 0.3, high σ_ν → creates day-to-day noise
- Slow transitory: φ_slow ≈ 0.9, lower σ_ν → creates week-to-week mean reversion

**Why**: Reddit's positive ACF(2) and the mismatch between short and long-horizon diagnostics suggest the autocorrelation structure is not well-captured by a single AR(1). A mixture could better match the empirical ACF at multiple lags while allowing more short-horizon rank disruption.

**Risk**: Doubles transitory parameters, complicating calibration. Only pursue if simpler fixes don't resolve the ACF(2) anomaly.

## 5. Proposed Implementation Plan

### Phase 1: Quick Calibration Wins (1-2 days)

1. **Recalibrate kappa objective** (Fix D): Reduce drift penalty weight, double RACF weight, expand grid. Re-run on all three platforms. This is a config/scoring change only — no new mechanics.

2. **Finer rank bins** (Fix E): Run Reddit with `--min-anchor-bins 8`. Check if the σ_eta curve shows meaningful variation across the top quintile.

### Phase 2: Common Factor (3-5 days)

3. **Estimate empirical common factor**: Compute the cross-sectional mean of log-changes for each day. Regress each entity's changes on this mean to get loadings β_i. Report the fraction of total variance explained by the common factor.

4. **Add common factor to simulator**: New parameter F(t) with persistence ρ_F, entity loadings β_i (rank-dependent). Calibrate ρ_F and the loading profile.

5. **Re-run diagnostics on all three platforms**.

### Phase 3: Position-Dependent Rank Noise (3-5 days)

6. **Compute empirical gap structure**: For each period, compute the log-metric gap between adjacent ranked entities. Plot gap vs. rank to characterize the position-dependent noise sensitivity.

7. **Implement gap-dependent σ_obs**: Make observation noise inversely proportional to local gap, or implement direct Wright-Fisher rank noise.

8. **Re-calibrate σ_obs estimation** to account for position-dependent noise.

### Phase 4: Collision-Rate Calibration (2-3 days)

9. **Compute empirical collision rates**: For each rank pair (k, k+1), count the fraction of periods where rank k and k+1 swap occupants.

10. **Add collision-rate diagnostic**: Compute the same in simulation. Add as a calibration target.

11. **Iterate calibration** with collision rates in the objective.

### Phase 5: Validation (2-3 days)

12. **Full MC runs** (25 reps) on all three platforms with the improved model.
13. **Ablation study** to quantify each fix's contribution.
14. **Sensitivity analysis** to verify robustness.

## 6. Key References

- **Iñiguez, Pineda et al. (2022)**, "Dynamics of ranking," *Nature Communications* 13:1646 — Wright-Fisher rank diffusion, Lévy walks, replacement dynamics
- **Itkin & Larsson (2024)**, "Calibrated rank volatility stabilized models," arXiv:2403.04674 — collision-rate calibration for rank-based equity models
- **Fernholz, Ichiba & Karatzas (2013)**, "A second-order stock market model," *Annals of Finance* 9:439-454 — name + rank dependent parameters for persistence
- **Herskovic et al. (2016)**, "The common factor in idiosyncratic volatility," *JFE* 119:249-283 — common volatility factors
- **Gabaix (2011)**, "The granular origins of aggregate fluctuations," *Econometrica* 79:733-772 — idiosyncratic shocks creating aggregate effects
- **Stanley et al. (1996)**, "Scaling behaviour in the growth of companies," *Nature* 379:804-806 — size-dependent volatility
- **Ghoshal & Barabási (2011)**, "Ranking stability and super-stable nodes," *Nature Communications* 2:394 — super-stable nodes in power-law networks
- **Banner, Fernholz & Karatzas (2005)**, "Atlas models of equity markets," *Annals of Applied Probability* 15:2296-2330 — rank-based SDEs, local time
- **Stock & Watson (1998)**, "Median unbiased estimation of coefficient variance in a time-varying parameter model," *JASA* 93:349-358 — pile-up problem
- **Luttmer (2007)**, "Selection, growth, and the size distribution of firms," *QJE* 122:1103-1144 — entry/exit dynamics
