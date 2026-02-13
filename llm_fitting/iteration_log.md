# Rank Diffusion Model — Iteration Log

## v2.6 Baseline Assessment
**Date:** 2026-02-12
**Score:** 9/15 diagnostics pass
**Elapsed:** 12s

### Parameters
| Param | Value |
|-------|-------|
| σ_obs | 0.35 |
| σ_het | 0.42 |
| κ (global mean rev) | 0.012 (half-life 58wk) |
| perm_boost | 1.3 |
| t_df | 4.5 |
| jump_prob | 0.03 |
| jump_scale | 5.0 |

### Results Summary

| Diagnostic | Empirical | Simulated | Error | Pass |
|-----------|-----------|-----------|-------|------|
| VR(2) | 0.6017 | 0.6165 | 2.5% | Y |
| VR(4) | 0.3349 | 0.3671 | 9.6% | Y |
| VR(8) | 0.1889 | 0.2193 | 16.1% | Y |
| VR(13) | 0.1236 | 0.1538 | 24.4% | **N** |
| ACF(1) | -0.3988 | -0.4096 | 0.011 | Y |
| ACF(2) | -0.0553 | -0.0259 | 0.029 | Y |
| RACF(1) | 0.4567 | 0.6753 | 0.219 | **N** |
| RACF(4) | 0.2551 | 0.5177 | 0.263 | **N** |
| RACF(13) | 0.0622 | 0.2501 | 0.188 | **N** |
| Pers(1) | 76 | 73 | -3 | Y |
| Pers(4) | 64 | 72 | +8 | Y |
| Pers(13) | 64 | 59 | -5 | Y |
| R²(1) | 0.7899 | 0.8720 | 0.082 | **N** |
| R²(4) | 0.7262 | 0.8113 | 0.085 | **N** |
| R²(13) | 0.6678 | 0.6957 | 0.028 | Y |

Additional: Kurtosis emp=7.0, sim=3.7. KS=0.045. Pers(26)=38 vs 59, Pers(52)=4 vs 43.

### Root Cause Analysis

**Primary failure: σ_obs is too large, starving top-band structural dynamics.**

The observation noise σ_obs=0.35 contributes 2×0.35²=0.245 to the change variance. For bands 1–100 and 101–500, the empirical change variance is ~0.10–0.17, which is *less* than the observation noise floor. The estimation correctly sets σ_η=0 and σ_ν=0 for these bands — there's no room for structural variance.

**Consequence chain:**
1. Top-band endpoints have zero permanent and zero transitory innovation
2. Their true levels (τ+c) are essentially frozen (aside from tiny κ drift)
3. All observed variation is observation noise → temporary rank shuffles that immediately revert
4. → RACF much too high (ranks appear far too stable)
5. → R² too high (underlying values barely change)
6. → Kurtosis too low (everything is Gaussian obs noise, no structural heavy tails)

**Secondary failure: κ mean-reversion to global mean destroys long-horizon persistence.**

With κ=0.012, ~46% of deviation from global mean is absorbed in 52 weeks. Top endpoints (far above global mean) experience persistent downward drag → Pers(26)=38 vs 59, Pers(52)=4 vs 43.

**Tertiary: VR too high at long horizons** — σ_obs inflates VR at all finite horizons by adding a constant to both numerator and denominator of the ratio.

### Diagnosis → Required Changes for v2.7

1. **Reduce σ_obs significantly** (0.35 → ~0.20): Frees variance budget for structural components in all bands. 2×0.20²=0.08 << 0.17, so all bands now have room for (σ_η, φ, σ_ν).
2. **Reduce κ** (0.012 → ~0.004): Preserve long-horizon top-100 persistence. Half-life 58wk → 173wk.
3. **Reduce t_df** (4.5 → 3.5): Heavier tails to increase kurtosis (3.7 → target 7.0).
4. **Increase jump_prob** (0.03 → 0.04): Further kurtosis boost.
5. **Adjust perm_boost**: With non-zero estimated σ_η for top bands, need to calibrate carefully. Start with 1.5.

---

## v2.7 — Principled Estimation
**Date:** 2026-02-12
**Score:** 8/15 diagnostics pass (down from 9, but structurally much better)
**Elapsed:** 26s

### Key Change
Replaced all hand-tuned parameters with data-driven estimates:
- σ_obs=0.2309 (from ACF lag structure: φ_agg=ρ(3)/ρ(2), σ²_obs=-γ(1)+γ(2)/φ_agg)
- σ_het=0.4276 (from mean/median variance ratio: sqrt(log(1.44)/2))
- t_df=3.00 (MLE fit of t-distribution to changes, floored at 3.0)
- κ=0.004623 (from variance stationarity: E[h²]·mean(σ²_η)/(2·Var_xsec), HL=150wk)
- perm_boost=1.0 (eliminated)
- jump_prob=0.005, jump_scale=5.75 (from tail excess beyond fitted t)

### Results Summary

| Diagnostic | Empirical | Simulated | Error | Pass | vs v2.6 |
|-----------|-----------|-----------|-------|------|---------|
| VR(2) | 0.6017 | 0.6111 | 1.6% | Y | = |
| VR(4) | 0.3349 | 0.3401 | 1.6% | Y | ↑ |
| VR(8) | 0.1889 | 0.1905 | 0.9% | Y | ↑ |
| VR(13) | 0.1236 | 0.1285 | 3.9% | **Y** | ↑↑ (was FAIL) |
| ACF(1) | -0.3988 | -0.3702 | 0.029 | Y | = |
| ACF(2) | -0.0553 | -0.0485 | 0.007 | Y | = |
| RACF(1) | 0.4567 | 0.5545 | 0.098 | **N** | ↑ (err 0.22→0.10) |
| RACF(4) | 0.2551 | 0.3497 | 0.095 | **N** | ↑↑ (err 0.26→0.09) |
| RACF(13) | 0.0622 | 0.1600 | 0.098 | **N** | ↑ (err 0.19→0.10) |
| Pers(1) | 76 | 80 | +4 | Y | = |
| Pers(4) | 64 | 69 | +5 | Y | = |
| Pers(13) | 64 | 52 | -12 | **N** | ↓ |
| R²(1) | 0.7899 | 0.8925 | 0.103 | **N** | ↓ |
| R²(4) | 0.7262 | 0.8488 | 0.123 | **N** | ↓ |
| R²(13) | 0.6678 | 0.7554 | 0.088 | **N** | ↓ |

Additional: Kurtosis emp=7.0, sim=**48.8** (broken). KS=0.018 (improved). Pers(52)=30 vs 43. Cross-sec var growing: 3.23→4.54.

### What Improved
- **VR: All horizons now pass** (was 3/6 failing long-horizon). The principled σ_obs gave the estimator room to find proper structural parameters for ALL bands (previously bands 1-500 had zero structural variance).
- **RACF errors halved** (avg err 0.22→0.10). Non-zero permanent innovations for top bands create actual rank dynamics.
- **Band-level estimation now sensible**: All bands have non-zero σ_η, σ_ν, and reasonable φ.
- **KS statistic improved** (0.045→0.018).

### Root Cause Analysis of Remaining Failures

**1. Kurtosis=48.8 — t_df=3.0 is catastrophically wrong.**
MLE on the AGGREGATE change distribution found df=2.66 (floored to 3.0). But this conflates:
- Heavy tails from the t-innovation (what we want)
- Heavy tails from between-endpoint heterogeneity (σ_het=0.43 → different endpoints have different variances)
- Heavy tails from band structure (top vs bottom bands have very different variance)
The MLE on aggregate data DOUBLE-COUNTS heterogeneity — both through t_df and through σ_het. The actual within-endpoint innovation t_df should be much higher (~4-5).
**Fix**: Estimate t_df from WITHIN-ENDPOINT standardized residuals, which remove heterogeneity effects.

**2. R² too high — true levels too persistent.**
R²_true(1)=0.9199 vs empirical 0.7899. The permanent innovations create only 8% displacement per week; the data shows 21%. With perm_boost=1.0, the moment-matched σ_η is too small.
**Root cause**: The model initializes c=0 for all endpoints, but the empirical t=0 data ALREADY contains transitory components. This means the initial cross-sectional variance includes Var(τ)+Var(c)+Var(ε), but the model attributes it all to τ. Then c grows from zero while the "true" τ barely changes, making R²(1) too high.
**Fix**: Initialize c from its stationary distribution: c_i,0 ~ N(0, σ²_c(r_i)·h²_i), and set τ_i,0 = obs_i - c_i,0. This properly decomposes the initial observation.

**3. RACF still too high (same root cause as R²).**
Insufficient cross-sectional displacement → ranks too stable. Fixing the c initialization will help (more true-level variance at t=0 vs t=1). Additionally, may need a data-derived perm_boost to close the gap.

**4. Cross-sec variance growing (3.23→4.54).**
κ=0.0046 is the stationarity-derived value, but it assumes permanent innovations are the only source of variance growth. In reality, the transitory component adds cross-sectional variance as it spins up from c=0. Fixing c initialization should stabilize this.

### Diagnosis → Required Changes for v2.8

1. **t_df from within-endpoint residuals**: Standardize each endpoint's changes by its own mean/std, pool, then MLE fit. Removes heterogeneity double-counting.
2. **Initialize c from stationary distribution**: c_i,0 ~ N(0, σ²_c(r_i)·h²_i), τ_i,0 = obs_i - c_i,0.
3. **Estimate perm_boost from R² gap**: Back out the required σ²_η from the observed R²(1) decay.
4. **Re-derive κ**: After perm_boost and c-initialization changes, recompute κ for stationarity.

---

## v2.8 — Within-Endpoint t_df + c Initialization + perm_boost
**Date:** 2026-02-12
**Score:** 7/15 diagnostics pass (regression — perm_boost/κ coupling catastrophic)
**Elapsed:** 29s

### Key Changes
Three targeted fixes from v2.7's root cause analysis:

1. **t_df from within-endpoint standardized residuals**: For each of 2000 endpoints, computed z_i,t = (Δy_i,t - mean_i)/std_i, pooled 174,000 residuals, then MLE fit. Result: df=4.97 (vs aggregate MLE df=2.66). The aggregate estimate was poisoned by between-endpoint variance heterogeneity — different endpoints have different σ², which looks like heavy tails when pooled. Within-endpoint standardization removes this confound.

2. **c initialized from stationary distribution**: Drew c_i,0 ~ N(0, σ²_c(r_i)·h²_i), set τ_i,0 = obs_i - c_i,0. Intent: decompose the initial observation into permanent and transitory components so the simulation starts with proper R² behavior.

3. **perm_boost from R² gap**: Used analytical formula: pb² = [cov²/(R²_target × var_y0) - var_y0] / σ²_η_eff_base. Result: pb=2.12. Then re-derived κ for stationarity: κ = pb²×σ²_η_base/(2×Var_xsec) = 0.0196 (HL=35wk).

### Parameters
| Param | Value | Source |
|-------|-------|--------|
| σ_obs | 0.2309 | ACF lag structure (same as v2.7) |
| σ_het | 0.4276 | mean/median variance ratio (same) |
| t_df | 4.97 | Within-endpoint standardized residuals MLE |
| κ | 0.01961 (HL=35wk) | Variance stationarity with perm_boost |
| perm_boost | 2.12 | R² gap analytical formula |
| jump_prob | 0.0057 | Tail excess vs fitted t (recomputed with z_within) |
| jump_scale | 4.11 | Extreme-change magnitude ratio |

### Results Summary

| Diagnostic | Empirical | Simulated | Error | Pass | vs v2.7 |
|-----------|-----------|-----------|-------|------|---------|
| VR(2) | 0.6017 | 0.6586 | 9.5% | Y | ↓ |
| VR(4) | 0.3349 | 0.4146 | 23.8% | **N** | ↓↓ (was PASS) |
| VR(8) | 0.1889 | 0.2712 | 43.6% | **N** | ↓↓ (was PASS) |
| VR(13) | 0.1236 | 0.2039 | 64.9% | **N** | ↓↓ (was PASS) |
| ACF(1) | -0.3988 | -0.3290 | 0.070 | Y | ↓ |
| ACF(2) | -0.0553 | -0.0564 | 0.001 | Y | = |
| RACF(1) | 0.4567 | 0.7577 | 0.301 | **N** | ↓↓ |
| RACF(4) | 0.2551 | 0.5892 | 0.334 | **N** | ↓↓ |
| RACF(13) | 0.0622 | 0.2778 | 0.216 | **N** | ↓ |
| Pers(1) | 76 | 72 | -4 | Y | = |
| Pers(4) | 64 | 57 | -7 | Y | = |
| Pers(13) | 64 | 29 | -35 | **N** | ↓↓ |
| R²(1) | 0.7899 | 0.8155 | 0.026 | **Y** | ↑↑ (was FAIL) |
| R²(4) | 0.7262 | 0.7076 | 0.019 | **Y** | ↑↑ (was FAIL) |
| R²(13) | 0.6678 | 0.5158 | 0.152 | **N** | ↓ |

Additional: Kurtosis emp=7.0, sim=5.2 (huge improvement from 48.8). KS=0.052. Pers(26)=6, Pers(52)=1. Cross-sec var growing: 3.52→3.86.

### What Improved
- **t_df fixed**: Within-endpoint MLE gives 4.97 (not 3.0). Kurtosis drops from 48.8 to 5.2 — the double-counting hypothesis was correct.
- **R²(1) and R²(4) now pass**: R²(1)=0.816 vs emp 0.790 (err 0.026). The perm_boost creates enough permanent displacement to match short-horizon R² decay.

### Root Cause of Regressions

**perm_boost=2.12 coupled with κ=0.020 (HL=35wk) is catastrophic.**

The stationarity constraint forces κ = pb²×σ²_η_base/(2×Var_xsec). With pb=2.12, κ scales by pb²=4.5, going from 0.004 to 0.020. At HL=35 weeks:

1. **(1-κ)^13 = 0.78** → permanent correlations decay 22% in 13 weeks. This makes VR appear higher at long horizons (permanent shocks partially revert, looking transitory).
2. **Top endpoints pulled strongly toward mean**: κ×deviation = 0.020×6 = 0.12/week → 1.56 over 13 weeks. This utterly destroys top-100 persistence (29 vs 64).
3. **RACF inflated**: The strong mean-reversion keeps endpoints near their equilibrium positions, making ranks too stable. Paradoxically, the short HL=35wk means positions shuffle on a 1-year timescale, not week-to-week.

**The R² gap formula also overestimated perm_boost** because:
- The c initialization (τ₀ = obs - c₀ with independent c₀) inflated Var(τ₀) from 3.2 to 3.5
- Cov(y₀,y₁) ≈ (1-κ)×Var(obs) regardless of c initialization (the cross-terms cancel)
- So the c initialization barely affects R², and the full R² gap was attributed to perm_boost

### Diagnosis → Required Changes for v2.9

The fundamental problem: **you can't close the R² gap by scaling up permanent innovations** because the stationarity constraint forces a proportional increase in κ, which destroys everything else. Need a different mechanism for R² correction:

1. **Burn-in instead of c initialization**: Run the simulation for extra weeks before recording. The transitory component c naturally reaches its stationary distribution, providing proper R² behavior without decomposition artifacts.
2. **Separate κ from perm_boost**: Don't scale κ with pb².

---

## v2.9 — Burn-in + Rank-Dependent κ
**Date:** 2026-02-12
**Score:** 11/15 diagnostics pass (RACF breakthrough!)
**Elapsed:** 29s

### Key Changes
1. **50-week burn-in** replaces c initialization: Simulate T_total=138 weeks (50 burn-in + 88 recording). During burn-in, τ evolves (permanent innovations + mean-reversion) and c builds from zero. By week 50, c has reached its stationary distribution for all bands:
   - Band 5001-12000 (φ=0.17): 99.99% of stationary at week 50
   - Band 1-100 (φ=0.95, HL=13.5wk): ~92% of stationary at week 50

2. **Rank-dependent mean-reversion**: τ_i -= κ×(τ_i - μ(r_i)) where μ(r) is the sorted initial data (Zipf target). Preserves distribution shape rather than compressing toward global mean.

3. **perm_boost=1.0**: Removed. Burn-in should handle R² correction.

4. **κ at base value** (0.004, HL=159wk): Not scaled by perm_boost.

### Parameters
| Param | Value | Source |
|-------|-------|--------|
| σ_obs | 0.2309 | ACF lag structure |
| σ_het | 0.4276 | mean/median variance ratio |
| t_df | 4.97 | Within-endpoint MLE |
| κ | 0.004348 (HL=159wk) | Variance stationarity (base, no pb scaling) |
| perm_boost | 1.0 | Removed |
| jump_prob | 0.0057 | Tail excess |
| jump_scale | 4.11 | Extreme-change ratio |
| T_burnin | 50 weeks | |

### Results Summary

| Diagnostic | Empirical | Simulated | Error | Pass | vs v2.7 |
|-----------|-----------|-----------|-------|------|---------|
| VR(2) | 0.6017 | 0.6101 | 1.4% | Y | = |
| VR(4) | 0.3349 | 0.3371 | 0.6% | Y | = |
| VR(8) | 0.1889 | 0.1872 | 0.9% | Y | = |
| VR(13) | 0.1236 | 0.1255 | 1.6% | Y | = |
| ACF(1) | -0.3988 | -0.3616 | 0.037 | Y | = |
| ACF(2) | -0.0553 | -0.0640 | 0.009 | Y | = |
| RACF(1) | 0.4567 | 0.5316 | 0.075 | **Y** | ↑↑ (was FAIL!) |
| RACF(4) | 0.2551 | 0.2959 | 0.041 | **Y** | ↑↑ (was FAIL!) |
| RACF(13) | 0.0622 | 0.1262 | 0.064 | **Y** | ↑↑ (was FAIL!) |
| Pers(1) | 76 | 75 | -1 | Y | = |
| Pers(4) | 64 | 60 | -4 | Y | = |
| Pers(13) | 64 | 53 | -11 | **N** | ≈ |
| R²(1) | 0.7899 | 0.8918 | 0.102 | **N** | = |
| R²(4) | 0.7262 | 0.8540 | 0.128 | **N** | = |
| R²(13) | 0.6678 | 0.8075 | 0.140 | **N** | ↓ |

Additional: Kurtosis emp=7.0, sim=4.9. KS=0.030. Survivors=59.3% (target 71%, too low). Cross-sec var exploding: 3.23→12.19. Pers(26)=50 vs 59, Pers(52)=45 vs 43.

### RACF Breakthrough
**All three RACF diagnostics pass for the first time in any version.** The burn-in equilibrates c before recording begins, so the transitory component provides proper rank volatility from week 1:
- RACF(1): 0.53 vs emp 0.46 (err 0.075, barely passes at <0.08 threshold)
- RACF(4): 0.30 vs emp 0.26 (err 0.041)
- RACF(13): 0.13 vs emp 0.06 (err 0.064)

### Remaining Failures

**1. R² still too high (0.89 vs 0.79):** The burn-in does NOT fix R². Analysis shows Cov(y₀,y₁) ≈ (1-κ)×Var(obs) regardless of whether c is at zero or at stationary (the c contribution to covariance is the same either way once c is drawn independently of obs). The R² gap is a fundamental property of the model's σ²_η being too small.

**2. Cross-sectional variance explosion (3.23→12.19):** Rank-dependent κ preserves the distribution shape but doesn't control the spread. The stationarity condition Var(τ) = Var(μ(r)) + σ²_η/(2κ) gives Var(δ)=σ²_η/(2κ)=3.5, so stationary Var(τ)≈6.7. But with Var(μ)≈3.2, that means deviations-from-Zipf add 3.5 to the total variance — nearly doubling it. The simulation heads toward this equilibrium.

**3. Survivors 59%** (target 71%): The burn-in period with active exit/entry creates many transients (type=1, exit rate 7%), which snowball. After 50 weeks, the population is dominated by high-turnover transients.

### Diagnosis → Required Changes for v3.0

1. **Use global-mean κ** (not rank-dependent) for variance stability.
2. **Keep burn-in** for RACF benefit.
3. **Disable exit/entry during burn-in** (burn-in only equilibrates c, not population).

---

## v3.0 — Global-Mean κ + Burn-in (Best Combination)
**Date:** 2026-02-12
**Score:** 14/15 diagnostics pass
**Elapsed:** 29s

### Key Changes
Combined the best elements of v2.7 and v2.9:
1. **Global-mean κ** (from v2.7): Restores variance stability. τ pulled toward population mean, absorbing cross-sectional variance growth.
2. **50-week burn-in** (from v2.9): Keeps the RACF breakthrough.
3. Exit/entry active during burn-in (unfixed — causes excess transients).

### Results Summary

| Diagnostic | Empirical | Simulated | Error | Pass |
|-----------|-----------|-----------|-------|------|
| VR(2) | 0.6017 | 0.6148 | 2.2% | Y |
| VR(4) | 0.3349 | 0.3394 | 1.3% | Y |
| VR(8) | 0.1889 | 0.1876 | 0.7% | Y |
| VR(13) | 0.1236 | 0.1254 | 1.5% | Y |
| ACF(1) | -0.3988 | -0.3572 | 0.042 | Y |
| ACF(2) | -0.0553 | -0.0573 | 0.002 | Y |
| RACF(1) | 0.4567 | 0.5258 | 0.069 | Y |
| RACF(4) | 0.2551 | 0.2984 | 0.043 | Y |
| RACF(13) | 0.0622 | 0.1375 | 0.075 | Y |
| Pers(1) | 76 | 74 | -2 | Y |
| Pers(4) | 64 | 55 | -9 | Y |
| Pers(13) | 64 | 47 | -17 | **N** |
| R²(1) | 0.7899 | 0.8565 | 0.067 | **Y** |
| R²(4) | 0.7262 | 0.8030 | 0.077 | **Y** |
| R²(13) | 0.6678 | 0.7446 | 0.077 | **Y** |

Additional: Kurtosis emp=7.0, sim=5.0. Survivors=59.3% (target 71%, too low due to burn-in transients).

### Major Achievement: R² Now Passes
All R² diagnostics pass for the first time alongside RACF. The combination of burn-in (equilibrated c) + global κ (stable variance) brings R² into the acceptable range. R²(1)=0.857 vs emp 0.790 (err 0.067 < 0.08 threshold).

The mechanism: the global-mean κ prevents cross-sectional variance from growing, which keeps R² from being artificially inflated by widening value gaps.

### Remaining Issue
- **Pers(13)=47 vs 64**: Same structural limitation as before — global-mean κ pulls top endpoints down.
- **Exit rate too high** (269/wk): Burn-in created too many transients.

---

## v3.1 — Disable Exit/Entry During Burn-in (Current Best)
**Date:** 2026-02-12
**Score:** 14/15 diagnostics pass
**Elapsed:** 29s

### Key Change
Disabled exit/entry during the 50-week burn-in period. The burn-in only evolves τ (permanent innovations + mean-reversion) and c (AR(1) transitory). No population dynamics during warm-up. This fixes the transient snowball from v3.0.

### Parameters (all principled — no hand-tuning)
| Param | Value | Source |
|-------|-------|--------|
| σ_obs | 0.2309 | ACF lag structure: φ_agg=ρ(3)/ρ(2), σ²_obs=-γ(1)+γ(2)/φ_agg |
| σ_het | 0.4276 | mean/median variance ratio: sqrt(log(1.44)/2) |
| t_df | 4.97 | Within-endpoint standardized residuals MLE |
| κ | 0.004348 (HL=159wk) | Variance stationarity: E[h²]×mean(σ²_η)×jump_factor/(2×Var_xsec) |
| perm_boost | 1.0 | Not needed with burn-in |
| jump_prob | 0.0057 | Tail excess: observed - expected at 4σ threshold |
| jump_scale | 4.11 | std(extreme)/std(normal) from within-endpoint residuals |
| T_burnin | 50 weeks | 3.7 half-lives for φ=0.95 top band |

Band-level structural parameters (per-band moment matching of variance, VR(4), VR(13), ACF(1)):

| Band | σ_η | φ | σ_ν | Perm% |
|------|-----|---|-----|-------|
| 1-100 | 0.1074 | 0.9500 | 0.1354 | 8.4% |
| 101-500 | 0.0501 | 0.7075 | 0.2329 | 1.5% |
| 501-2000 | 0.1071 | 0.3845 | 0.3124 | 4.4% |
| 2001-5000 | 0.1237 | 0.2990 | 0.4103 | 4.0% |
| 5001-12000 | 0.1417 | 0.1737 | 0.4872 | 3.8% |

### Results Summary

| Diagnostic | Empirical | Simulated | Error | Pass | Trend (v2.6→v3.1) |
|-----------|-----------|-----------|-------|------|-------------------|
| VR(2) | 0.6017 | 0.6099 | 1.4% | Y | Stable (always pass) |
| VR(4) | 0.3349 | 0.3369 | 0.6% | Y | ↑ (0.37→0.34, from σ_obs fix) |
| VR(8) | 0.1889 | 0.1867 | 1.2% | Y | ↑ (0.22→0.19) |
| VR(13) | 0.1236 | 0.1249 | 1.0% | Y | ↑↑ (FAIL→1.0% err) |
| ACF(1) | -0.3988 | -0.3619 | 0.037 | Y | Stable |
| ACF(2) | -0.0553 | -0.0531 | 0.002 | Y | Stable |
| RACF(1) | 0.4567 | 0.5256 | 0.069 | Y | ↑↑↑ (0.22→0.07, from burn-in) |
| RACF(4) | 0.2551 | 0.2987 | 0.044 | Y | ↑↑↑ (0.26→0.04) |
| RACF(13) | 0.0622 | 0.1238 | 0.062 | Y | ↑↑↑ (0.19→0.06) |
| Pers(1) | 76 | 70 | -6 | Y | Stable |
| Pers(4) | 64 | 62 | -2 | Y | Stable |
| Pers(13) | 64 | 45 | -19 | **N** | ↓ (was pass in v2.6 at -5) |
| R²(1) | 0.7899 | 0.8483 | 0.059 | Y | ↑↑ (0.082→0.059) |
| R²(4) | 0.7262 | 0.7924 | 0.066 | Y | ↑↑ (0.085→0.066) |
| R²(13) | 0.6678 | 0.7367 | 0.069 | Y | ↑ (0.028→0.069, different direction) |

Additional diagnostics:
- Kurtosis: emp=7.0, sim=4.9 (improved from 3.7 in v2.6 and 48.8 in v2.7)
- KS statistic: 0.036 (improved from 0.045)
- Zipf slope: emp=-1.11, sim=-1.07
- Survivors: 71.1% (target 71%) ✓
- Cross-sec variance: τ var 3.22→4.76 over 88 recording weeks (growing, but BP stable: 3.78→3.65)

### Evolution Summary: v2.6 (9/15) → v3.1 (14/15)

| Version | Score | Key Innovation | Key Failure |
|---------|-------|---------------|-------------|
| v2.6 | 9/15 | Baseline (hand-tuned) | σ_obs too large, RACF, R² |
| v2.7 | 8/15 | Principled estimation (all params from data) | Kurtosis=48.8, R² still high |
| v2.8 | 7/15 | Within-endpoint t_df, c init, perm_boost | pb=2.12 + κ coupling catastrophic |
| v2.9 | 11/15 | Burn-in (RACF breakthrough!) | R² still high, variance explosion |
| v3.0 | 14/15 | Global κ + burn-in combination | Pers(13), excess transients |
| v3.1 | 14/15 | No exit/entry during burn-in | Pers(13) structural limitation |

### Analysis of the Remaining Failure: Pers(13)

**Why 45 vs 64?** The global-mean reversion systematically pulls top endpoints (τ≈15, mean≈9) downward at κ×deviation = 0.004×6 = 0.024/week. Over 13 weeks, this is ~0.31 cumulative downward shift. Combined with the permanent innovation std of 0.39 (for the top band over 13 weeks), borderline top-100 endpoints have a significant probability of dropping below rank 100.

**Why the data has higher persistence:** Real-world top entities likely benefit from self-reinforcing dynamics (network effects, brand loyalty, economies of scale) that create an asymmetric "moat" around top positions. This is a structural feature that a symmetric stochastic model with global-mean reversion cannot capture.

**Potential model extensions** (not attempted):
1. Rank-dependent κ with variance-preserving rescaling (hybrid approach)
2. Asymmetric dynamics (harder to fall from top than to rise from bottom)
3. Time-varying or state-dependent innovation variances

---

## v3.2 — Rank-Local Mean Reversion + Monte Carlo (REGRESSION)
**Date:** 2026-02-12
**Score:** 8/15 (mean across 25 MC replications) — severe regression
**Elapsed:** 63s (25 replications × 2s each + data loading)

### Key Changes
Three major changes from v3.1:

1. **Rank-local mean reversion**: Replaced global-mean κ with rank-local reversion
   pulling each endpoint toward the Zipf target at its current rank:
   `τ_i -= κ_local × (τ_i - μ(r_i))`. κ_local = 0.037 (HL=19wk for deviations)
   calibrated from Var(δ)=0.4 target.

2. **Monte Carlo replications (N_REP=25)**: First version with uncertainty quantification.
   All diagnostics now reported as mean ± 95% CI across 25 seeds.

3. **Kurtosis tuning**: Reduced t_df from 4.97 to 4.17 (ARCH adjustment), increased
   jump_prob to 0.008, jump_scale to 4.5.

### Results Summary (mean ± 95% CI, 25 reps)

| Diagnostic | Empirical | Sim Mean | 95% CI | Pass |
|-----------|-----------|----------|--------|------|
| VR(4) | 0.3349 | 0.3395 | [0.338, 0.341] | Y |
| VR(13) | 0.1236 | 0.1279 | [0.127, 0.129] | Y |
| ACF(1) | -0.3988 | -0.3616 | [-0.371, -0.355] | Y |
| RACF(1) | 0.4567 | 0.5498 | [0.532, 0.562] | **N** |
| RACF(4) | 0.2551 | 0.3366 | [0.318, 0.348] | **N** |
| RACF(13) | 0.0622 | 0.1521 | [0.138, 0.174] | **N** |
| Pers(13) | 64 | 47.0 | [40, 53] | **N** |
| R²(1) | 0.7899 | 0.8712 | [0.864, 0.875] | **N** |
| R²(4) | 0.7262 | 0.8150 | [0.792, 0.824] | **N** |
| R²(13) | 0.6678 | 0.7525 | [0.739, 0.760] | **N** |

Additional: Kurtosis=11.9 (catastrophic overshoot from t_df=4.17 near df=4 divergence).
Zipf slope: sim=-1.21 vs emp=-1.11. Cross-sec var: 3.61→4.35.

### Root Cause of Failure

**Rank-local reversion was the wrong direction.** Pulling endpoints toward their Zipf
target *stabilizes* them at their "correct" positions, reducing displacement. This made
RACF, R², and Pers worse, not better. The fundamental error: we wanted MORE mobility
for top endpoints, but rank-local reversion gave LESS.

**t_df=4.17 was catastrophic.** The kurtosis of a t-distribution diverges at df=4.
At df=4.17, the raw kurtosis is 6/(4.17-4)=35. Combined with heterogeneity and jumps,
this produced simulated kurtosis of 11.9 (target 7.0) with enormous variance [6.65, 35.82].

**Critical insight from Monte Carlo:** v3.1's 14/15 was partly lucky — with single
replication, several marginal diagnostics (RACF(1)=0.069 err, R²(1)=0.059 err) fell
just below the 0.08 threshold. The "true" v3.1 score under MC averaging may have been
12-13/15.

### Lesson Learned

Rank-local mean reversion is structurally incompatible with the goal of increasing
top-rank mobility. It preserves the rank-size distribution but at the cost of freezing
individual positions. The correct approach is to keep global-mean reversion but
modulate its strength by rank.

---

## v3.3 — Rank-Dependent κ (Power-Law) + Monte Carlo
**Date:** 2026-02-12
**Score:** 14/15 (mean across 25 MC replications)
**Elapsed:** 64s

### Key Change
Return to v3.1's proven global-mean architecture, but add a rank-dependent multiplier:

```
κ(r) = κ_base × (r / N)^α,  pulling toward global mean
```

With α=0.3 and κ_base=0.0063 (calibrated from stationarity):
- Rank 1: κ = 0.0004 (HL=1944wk) — near-zero mean reversion
- Rank 100: κ = 0.0014 (HL=488wk) — very weak
- Rank 7000: κ = 0.0046 (HL=151wk) — moderate
- 13-week cumulative shift for rank 100: 0.088 (vs 0.34 in v3.1)

Kurtosis parameters reverted to v3.1 values (t_df=4.97, jump parameters from MLE).

### Parameters
| Param | Value | Source |
|-------|-------|--------|
| σ_obs | 0.2309 | ACF lag structure |
| σ_het | 0.4276 | mean/median variance ratio |
| t_df | 4.97 | Within-endpoint MLE |
| κ_base | 0.006299 | Stationarity with rank-dep correction |
| α_κ | 0.3 | Power-law exponent for rank-dependent κ |
| jump_prob | 0.0057 | Tail excess |
| jump_scale | 4.11 | Extreme-change ratio |

### Results Summary (mean ± 95% CI, 25 reps)

| Diagnostic | Empirical | Sim Mean | 95% CI | Pass |
|-----------|-----------|----------|--------|------|
| VR(4) | 0.3349 | 0.3355 | [0.334, 0.337] | Y |
| VR(13) | 0.1236 | 0.1242 | [0.124, 0.125] | Y |
| ACF(1) | -0.3988 | -0.3644 | [-0.370, -0.358] | Y |
| RACF(1) | 0.4567 | 0.5177 | [0.509, 0.528] | Y |
| RACF(4) | 0.2551 | 0.2966 | [0.284, 0.319] | Y |
| RACF(13) | 0.0622 | 0.1276 | [0.113, 0.149] | Y |
| Pers(1) | 76 | 75.9 | [70, 81] | Y |
| Pers(4) | 64 | 62.7 | [56, 68] | Y |
| Pers(13) | 64 | 53.3 | [47, 60] | **N** |
| R²(1) | 0.7899 | 0.8519 | [0.847, 0.857] | Y |
| R²(4) | 0.7262 | 0.7932 | [0.788, 0.801] | Y |
| R²(13) | 0.6678 | 0.7310 | [0.716, 0.744] | Y |

Additional: Kurtosis=5.83 [4.37, 9.65]. KS=0.034. Zipf=-1.15. Survivors=71.3%.

### Pers(13) Analysis

Pers(13) = 53.3, diff from emp = -10.7. Just barely fails the threshold of 10.
The rank-dependent κ reduced the 13-week cumulative shift from 0.34 (v3.1) to 0.088,
which improved Pers(13) from 45 to 53 — a gain of 8 units, but not enough.

The remaining gap is from the random-walk component itself (σ_η × √13 ≈ 0.39 for top
band), which causes drift even without mean reversion. Increasing α to further reduce
κ for top ranks should help.

---

## v3.4 — Optimized Rank-Dependent κ (α=0.5) — ALL 15 PASS
**Date:** 2026-02-12
**Score:** 15/15 (mean across 25 MC replications) ✓
**Elapsed:** 63s

### Key Change
Increased α from 0.3 to 0.5, further shielding top ranks from mean reversion:

```
κ(r) = κ_base × (r / N)^0.5,  pulling toward global mean
```

With α=0.5 and κ_base=0.0073 (recalibrated):
- Rank 1: κ = 0.00006 (HL=11335wk) — essentially zero
- Rank 100: κ = 0.00061 (HL=1133wk) — negligible
- Rank 5000: κ = 0.00432 (HL=160wk) — moderate
- 13-week cumulative shift for rank 100: 0.038 (vs 0.088 in v3.3, 0.34 in v3.1)

All other parameters identical to v3.3.

### Parameters
| Param | Value | Source |
|-------|-------|--------|
| σ_obs | 0.2309 | ACF lag structure |
| σ_het | 0.4276 | mean/median variance ratio |
| t_df | 4.97 | Within-endpoint MLE |
| κ_base | 0.007329 | Stationarity with rank-dep correction (α=0.5) |
| α_κ | 0.5 | Power-law exponent |
| jump_prob | 0.0057 | Tail excess |
| jump_scale | 4.11 | Extreme-change ratio |

### Results Summary (mean ± 95% CI, 25 reps)

| Diagnostic | Empirical | Sim Mean | 95% CI | Error | Pass |
|-----------|-----------|----------|--------|-------|------|
| VR(2) | 0.6017 | 0.6095 | [0.607, 0.612] | 1.3% | **Y** |
| VR(4) | 0.3349 | 0.3354 | [0.334, 0.337] | 0.1% | **Y** |
| VR(8) | 0.1889 | 0.1851 | [0.184, 0.186] | 2.0% | **Y** |
| VR(13) | 0.1236 | 0.1240 | [0.124, 0.125] | 0.3% | **Y** |
| ACF(1) | -0.3988 | -0.3646 | [-0.369, -0.359] | 0.034 | **Y** |
| ACF(2) | -0.0553 | -0.0625 | [-0.071, -0.055] | 0.007 | **Y** |
| RACF(1) | 0.4567 | 0.5163 | [0.504, 0.531] | 0.060 | **Y** |
| RACF(4) | 0.2551 | 0.2935 | [0.277, 0.306] | 0.038 | **Y** |
| RACF(13) | 0.0622 | 0.1236 | [0.108, 0.141] | 0.061 | **Y** |
| Pers(1) | 76 | 76.6 | [72, 81] | +0.6 | **Y** |
| Pers(4) | 64 | 63.6 | [55, 70] | -0.4 | **Y** |
| Pers(13) | 64 | 55.0 | [50, 60] | -9.0 | **Y** |
| Pers(26) | 59 | 49.4 | [43, 56] | -9.6 | (Y) |
| Pers(52) | 43 | 43.3 | [31, 49] | +0.3 | (Y) |
| R²(1) | 0.7899 | 0.8533 | [0.849, 0.858] | 0.063 | **Y** |
| R²(4) | 0.7262 | 0.7957 | [0.787, 0.802] | 0.069 | **Y** |
| R²(13) | 0.6678 | 0.7347 | [0.722, 0.744] | 0.067 | **Y** |

Additional: Kurtosis=6.18 [4.51, 12.36] (emp=7.0). KS=0.033. Zipf=-1.18 (emp=-1.11).
Survivors=71.2% (emp=71.4%). Cross-sec var: 3.23→4.51.

### What Made This Work

1. **The α=0.5 power law gives rank 100 near-zero mean reversion** (κ=0.0006,
   force=0.003/week, 13-wk cumulative=0.038). This preserves top-rank positions
   while the bulk of endpoints (ranks 5000+) maintain κ≈0.004-0.007 for variance
   control.

2. **Stationarity calibration automatically compensates**: κ_base increases from
   0.0063 (α=0.3) to 0.0073 (α=0.5) because the weighted deviation product
   E[(r/N)^α × (τ-mean)²] decreases. Bottom-rank endpoints absorb more of the
   variance control burden.

3. **All 14 previously-passing diagnostics remain unchanged**: VR, ACF, RACF, and R²
   diagnostics are dominated by the bulk population, which is barely affected by the
   α change (from 0.3 to 0.5 changes κ for rank 7000 by only 10%).

### Complete Evolution Summary: v2.6 (9/15) → v3.4 (15/15)

| Version | Score | Key Innovation | What It Fixed |
|---------|-------|---------------|---------------|
| v2.6 | 9/15 | Baseline (hand-tuned) | — |
| v2.7 | 8/15 | Principled estimation | VR (all horizons) |
| v2.8 | 7/15 | Within-endpoint t_df | Kurtosis, R²(1,4) |
| v2.9 | 11/15 | 50-week burn-in | RACF (breakthrough) |
| v3.0 | 14/15 | Global κ + burn-in | R² |
| v3.1 | 14/15* | No exit during burn-in | Survivors% |
| v3.2 | 8/15 | Rank-local reversion (FAILED) | — (regression) |
| v3.3 | 14/15 | Rank-dep κ (α=0.3) + MC | Pers(13) partial |
| v3.4 | **15/15** | Rank-dep κ (α=0.5) + MC | **Pers(13) → PASS** |

*v3.1's 14/15 was from a single replication; with 25 MC reps it would likely score 12-13/15.

### Remaining Gaps (not failures, but areas for improvement)

1. **Kurtosis:** sim=6.2 vs emp=7.0 (within acceptable range but could be closer)
2. **Cross-sec variance growth:** 3.23→4.51 over 88 weeks (empirical is ~stable at 3.0)
3. **RACF(13):** sim=0.12 vs emp=0.06 (passing at err=0.06 but not tight)
4. **Zipf slope:** sim=-1.18 vs emp=-1.11 (distribution slightly steeper in simulation)

### Sensitivity of α_κ

The choice α_κ=0.5 is optimized for the current dataset. A sensitivity analysis shows:
- α=0.3: Pers(13)=53.3 (diff=-10.7, FAIL by 0.7)
- α=0.5: Pers(13)=55.0 (diff=-9.0, PASS by 1.0)
- The pass/fail boundary is at α≈0.4 for this dataset.

This suggests the model is at the edge of what symmetric global-mean reversion can
achieve for top-rank persistence. Further improvements would require structural
modifications (asymmetric dynamics, self-reinforcing effects, or rank-dependent
innovation variance).

---

