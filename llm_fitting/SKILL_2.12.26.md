# SKILL: Modeling Rank Diffusion Systems with Mean-Reverting Noise

## Overview

This skill covers the simulation and analysis of ranked dynamical systems where individual particles (endpoints, firms, accounts) have observable activity that determines their rank, and where the observed activity contains a large mean-reverting (transitory) component alongside a smaller persistent (permanent) component. The goal is to build simulations that faithfully reproduce **both** the stable macro-distribution of activity across ranks **and** the empirically observed movement patterns of individual particles.

---

## Core Principles

### DO: Decompose Shocks into Permanent and Transitory

Every simulation of this type **must** model the observed activity as:

```
log(y_it) = τ_it + c_it
```

where:
- `τ_it` = permanent component (random walk or near-random-walk)
- `c_it` = transitory component (fast-decaying AR(1) or similar)

This is the single most important modeling decision. Without this decomposition, you will either over-smooth (by deflating variance) or over-displace (by treating all variance as permanent).

### DO: Use Full Observed Variance, Partitioned Correctly

The total weekly variance of log-activity changes should match the empirical value. The decomposition allocates this variance between permanent and transitory innovations:

```
Var(Δlog y) = σ²_η + (1-φ²)⁻¹ · σ²_ν · (1-φ)² ≈ σ²_η + 2σ²_ν (when φ ≈ 0)
```

### DO NOT: Multiply the Innovation Variance by the Variance Ratio

The Cochrane variance ratio V(k) measures persistence — it tells you *what fraction of the variance is permanent*. It is **not** a multiplier to apply to the innovation variance. If V(4) = 0.33, this means 33% of the 4-week variance is due to permanent shocks. It does NOT mean you should use 0.33 × σ² as your innovation variance.

### DO: Calibrate Against Multiple Targets Simultaneously

A good model matches all of:
1. The rank-size distribution (Zipf curve)
2. The variance ratio profile at multiple horizons (2, 4, 8, 13, 26 weeks)
3. The autocorrelation function of log-activity changes (especially lag 1)
4. Rank-level autocorrelation (level, not changes)
5. Top-N persistence rates
6. Cross-sectional R² of log-activity at various lags

### DO NOT: Calibrate to Only One Diagnostic

Matching just the variance ratio at one horizon, or just the autocorrelation at lag 1, will produce a model that fails other diagnostics. The permanent/transitory split, the AR(1) decay rate, and the rank-dependent volatility structure must be estimated jointly.

---

## Estimation Procedure

### Step 1: Compute Empirical Diagnostics

```python
# Required diagnostics for calibration
# 1. Variance ratio at horizons k = 2, 4, 8, 13, 26
# 2. Lag-1 autocorrelation of Δlog(y)
# 3. Autocorrelation of rank levels at lags 1, 4, 13, 26
# 4. Rank-size (Zipf) slope
# 5. Top-N persistence at lags 1, 4, 13, 26, 52
# 6. Cross-sectional R² of log(y) at lags 1, 4, 13, 26
```

### Step 2: Estimate the Permanent/Transitory Split

**Primary method: Variance ratio + autocorrelation matching.**

From the lag-1 autocorrelation of Δlog(y), estimate the MA(1) parameter:

```
ρ(1) = θ / (1 + θ²)
```

Solve for θ (take the invertible root |θ| < 1). Then:
- Permanent variance fraction ≈ (1+θ)² / (1+θ²)
- This gives a quick initial estimate

**Refined method: State-space estimation.**

Fit an unobserved components model:
```
τ_t = τ_{t-1} + η_t       (permanent, random walk)
c_t = φ c_{t-1} + ν_t       (transitory, AR(1))
y_t = τ_t + c_t             (observation)
```

Estimate (σ²_η, φ, σ²_ν) by maximum likelihood using the Kalman filter.

### Step 3: Estimate Rank-Dependent Parameters

Stratify endpoints into rank bands (e.g., 1–100, 101–500, 501–2000, 2001–5000, 5001+). Within each band:
- Compute the variance ratio profile
- Compute the lag-1 autocorrelation
- Estimate band-specific (σ²_η, φ, σ²_ν)

Fit a smooth function σ²_η(r) and σ²_ν(r) across ranks using log-linear interpolation.

### Step 4: Estimate the Macro Constraint

Compute the empirical mean log-activity at each rank (the Zipf curve target). Fit a function:

```
μ(r) = a - b · log(r)
```

This defines the equilibrium log-activity for each rank.

---

## Simulation Procedure

### Initialization

1. Set initial permanent levels `τ_i,0` to match the empirical log-activity distribution
2. Set initial transitory components `c_i,0 = 0` (or draw from stationary distribution)
3. Rank endpoints by `exp(τ_i,0 + c_i,0)`

### Each Timestep

```python
for each timestep t:
    for each endpoint i:
        r_i = current rank of endpoint i
        
        # 1. Permanent innovation
        η_i = normal(0, σ_η(r_i))
        τ_i += drift(r_i) + η_i
        
        # 2. Transitory update
        ν_i = normal(0, σ_ν(r_i))
        c_i = φ * c_i + ν_i
        
        # 3. Observed activity
        y_i = exp(τ_i + c_i)
    
    # 4. Re-rank
    ranks = argsort(-y)  # descending
    
    # 5. Optional: soft macro constraint
    # Nudge τ toward equilibrium for current rank
    # τ_i += -κ * (τ_i - μ(r_i)) * dt
    # Use VERY small κ to avoid destroying individual dynamics
```

### The Soft Macro Constraint

The macro constraint prevents the rank-size distribution from drifting over long simulations. It should be:
- **Very weak**: κ should be much smaller than the mean-reversion rate of the transitory component
- **Applied only to the permanent component**: The transitory component already mean-reverts naturally
- **Rank-dependent**: The restoring force pulls toward the equilibrium activity for the endpoint's *current rank*

**DO NOT** make the macro constraint strong enough to dominate individual dynamics. If κ is too large, all endpoints simply track their equilibrium rank values and individual mobility vanishes.

---

## Validation

### Required Checks

After simulation, compute the same diagnostics as in Step 1 and compare:

| Diagnostic | Acceptable Match |
|-----------|-----------------|
| Variance ratio V(k) | Within 15% of empirical at each k |
| Lag-1 ACF of Δlog(y) | Within 0.05 of empirical |
| Rank ACF at lag 13 | Within 0.03 of empirical |
| Top-100 persistence (lag 4) | Within 5 percentage points |
| Zipf slope | Within 0.05 of empirical |
| Cross-sectional R² (lag 13) | Within 0.05 of empirical |

### Common Failures and Fixes

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| VR too high (over-persistence) | σ²_η too large relative to σ²_ν | Increase transitory variance, decrease permanent |
| VR too low (over-reversion) | σ²_ν too large OR φ too small | Increase φ or decrease σ²_ν |
| Rank ACF too low | Too much permanent noise | Decrease σ²_η |
| Top-N persistence too low | Too much total volatility | Scale down both σ²_η and σ²_ν |
| Zipf slope drifting | Macro constraint too weak | Increase κ slightly |
| Individual mobility too low | Macro constraint too strong | Decrease κ |
| Heavy-tailed events missing | Gaussian innovations | Add occasional jump component or use t-distribution |

---

## Key Formulas

### Variance Ratio for Permanent + AR(1) Transitory Model

For the model `y_t = τ_t + c_t` with `τ_t ~ RW(σ_η)` and `c_t ~ AR(1)(φ, σ_ν)`:

```
Var(Δy) = σ²_η + 2σ²_c(1 - φ)
where σ²_c = σ²_ν / (1 - φ²)

VR(k) = [k·σ²_η + 2σ²_c(1 - φᵏ)] / [k · (σ²_η + 2σ²_c(1 - φ))]
```

As k → ∞: VR(k) → σ²_η / (σ²_η + 2σ²_c(1-φ))

### Autocorrelation of Changes

```
Cov(Δy_t, Δy_{t+h}) for h ≥ 1:
  = -σ²_c · φ^(h-1) · (1 - φ)²

ρ(1) = -σ²_c(1-φ)² / (σ²_η + 2σ²_c(1-φ))
```

### Rank-Activity Relationship (Zipf)

```
log(y_r) ≈ α - β · log(r)
```

where β ≈ 1.0–1.2 for typical platform data.

---

## DO NOT

- **Do not** treat the system as a pure random walk (ignores mean reversion)
- **Do not** deflate the weekly variance by the variance ratio (destroys short-term dynamics)
- **Do not** use only rank changes (losing information in the metric values)
- **Do not** ignore rank-dependent volatility (top ranks behave differently from bottom)
- **Do not** make the macro constraint dominant (kills individual mobility)
- **Do not** assume Gaussian innovations without checking tails
- **Do not** fit parameters to only one diagnostic
- **Do not** confuse the transitory AR(1) coefficient φ with the autocorrelation of changes ρ(1) — they are related but not the same
- **Do not** skip validation against empirical calibration targets
- **Do not** model the transitory component with a very long half-life — in platform data, most transitory noise decays within 1–3 weeks

---

## Dependencies

- Python 3.8+ with numpy, scipy, pandas
- statsmodels (for state-space estimation, Kalman filter)
- matplotlib (for diagnostic plots)
