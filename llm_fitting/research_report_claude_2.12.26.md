# Modeling Individual Movement in Ranked Dynamical Systems with Mean-Reverting Noise

## A Research Synthesis for the Platform Observatory Project

---

## 1. The Core Problem

We are modeling digital platform ecosystems — Facebook pages, Telegram channels, YouTube accounts — as ranked dynamical systems. Each endpoint (page, channel, group) generates a measurable quantity of activity each period, and we rank them accordingly. Two empirical regularities define the system:

**Macro-structural stability.** The share of total activity accruing to a given *rank* is remarkably stable over time, even as the specific endpoints occupying those ranks churn. The rank-size distribution follows an approximate power law (Zipf's law with exponent ≈ −1.1 in our Facebook data), and the coefficient of variation of the share at a given rank is small (CV < 0.06 for ranks 50–1000).

**Mean-reverting individual noise.** The week-to-week changes in individual endpoints' log-activity are dominated by transitory fluctuations. Variance ratio analysis shows that the k-week variance of log-activity changes grows far more slowly than k times the one-week variance: VR(2) ≈ 0.60, VR(4) ≈ 0.33, VR(13) ≈ 0.12, VR(52) ≈ 0.03. The lag-1 autocorrelation of weekly log-activity changes is approximately −0.40, with near-zero autocorrelation beyond lag 2–3.

The modeling challenge: a standard rank diffusion model driven by the *observed* weekly variance produces far too much long-term endpoint displacement. Deflating the variance to match the estimated "durable" component (roughly 20–25% of naive weekly variance) produces simulations where weekly movements are unrealistically small. We need a model that reconciles large short-term fluctuations with modest long-term displacement.

---

## 2. Theoretical Foundations

### 2.1 The Atlas Model and Rank-Based Interacting Diffusions

The foundational mathematical framework is the Atlas model (Fernholz 2002, Banner, Fernholz & Karatzas 2005). In an Atlas model, N particles on the real line evolve as:

$$dX_i(t) = \sum_{j=1}^{N} \mathbf{1}_{\{X_i(t)=Y_j(t)\}} b_j \, dt + \sum_{j=1}^{N} \mathbf{1}_{\{X_i(t)=Y_j(t)\}} \sigma_j \, dW_i(t)$$

where $Y_1(t) \leq Y_2(t) \leq \cdots \leq Y_N(t)$ is the ranked ordering and $W_1, \ldots, W_N$ are independent Brownian motions. The drift $b_j$ and volatility $\sigma_j$ depend only on rank, not on the identity of the particle occupying that rank.

Key results from this literature:

- **Ergodicity.** Under appropriate conditions on the drift vector (Ichiba, Papathanakos, Banner, Karatzas & Fernholz 2011), the gap process between ranked particles is ergodic and admits a unique stationary distribution. The capital (or activity) distribution curve is stable in the long run.

- **Zipf's law.** Fernholz (2020) shows that the stationary distribution of an Atlas model follows Zipf's law if and only if two conditions — conservation and completeness — are satisfied. This provides a theoretical basis for the power-law rank-size distributions observed in platform data.

- **Propagation of chaos.** As N → ∞, each particle's rank dynamics converge to a nonlinear diffusion described by a porous medium PDE (Jourdain & Reygner 2013). The empirical CDF of particles converges to a deterministic limit, providing the macroscopic description.

- **Hybrid (second-order) models.** Pure first-order Atlas models have each particle spending 1/N of its time at each rank — clearly unrealistic. Hybrid Atlas models (Ichiba et al. 2011, Fernholz, Ichiba & Karatzas 2013) introduce name-based parameters alongside rank-based parameters, allowing heterogeneity in how long different particles spend at different ranks.

### 2.2 The Ornstein-Uhlenbeck Process and Mean Reversion

The Ornstein-Uhlenbeck (OU) process is the canonical model for mean-reverting dynamics:

$$dX_t = \theta(\mu - X_t) \, dt + \sigma \, dW_t$$

where $\theta$ is the speed of mean reversion, $\mu$ is the long-run mean, and $\sigma$ is the volatility. Key properties:

- **Stationary variance:** $\text{Var}(X_\infty) = \sigma^2 / (2\theta)$
- **Autocorrelation function:** $\rho(\tau) = e^{-\theta \tau}$
- **Half-life of mean reversion:** $t_{1/2} = \ln(2) / \theta$

For discretely sampled data at interval $\Delta t$, the OU process maps to an AR(1):

$$X_{t+\Delta t} = \mu(1 - e^{-\theta \Delta t}) + e^{-\theta \Delta t} X_t + \epsilon_t$$

with $\epsilon_t \sim N(0, \frac{\sigma^2}{2\theta}(1 - e^{-2\theta \Delta t}))$.

### 2.3 Permanent-Transitory Decomposition

The Beveridge-Nelson and related decompositions (Campbell & Mankiw 1987, Cochrane 1988) provide the key conceptual framework for our problem. The observed time series is written as the sum of:

$$y_t = \tau_t + c_t$$

where $\tau_t$ is a permanent component (random walk or near-random-walk) and $c_t$ is a stationary transitory component.

**Cochrane's variance ratio** measures the relative importance of the permanent component:

$$V(k) = \frac{\text{Var}(y_t - y_{t-k})}{k \cdot \text{Var}(y_t - y_{t-1})}$$

For a pure random walk, $V(k) = 1$ for all $k$. For a process with mean-reverting components, $V(k) < 1$ and declining. As $k \to \infty$, $V(k)$ converges to $\sigma_\tau^2 / \text{Var}(\Delta y)$, the ratio of permanent innovation variance to total innovation variance.

**Unobserved components models** (Harvey 1989, Watson 1986) formalize this in a state-space framework:

- *State equation (permanent):* $\tau_t = \tau_{t-1} + \eta_t$, $\eta_t \sim N(0, \sigma_\eta^2)$
- *State equation (transitory):* $c_t = \phi c_{t-1} + \nu_t$, $\nu_t \sim N(0, \sigma_\nu^2)$
- *Observation equation:* $y_t = \tau_t + c_t$

The Kalman filter/smoother provides optimal estimates of the unobserved components, and maximum likelihood estimation identifies the parameters.

### 2.4 Measurement Error vs. Transitory Dynamics

A critical distinction exists between two sources of mean-reverting noise:

1. **Measurement error.** The observed activity is a noisy measurement of the "true" underlying engagement level. Aggregation artifacts, sampling variation, and data collection timing create noise that is purely observational and carries no dynamical information.

2. **Transitory real dynamics.** Viral posts, temporary algorithmic boosts, news events, and seasonal patterns create genuine but short-lived spikes or dips in activity. These are real changes that nonetheless revert.

Mathematically, both produce similar variance ratio signatures, but they have very different implications for simulation:

- Measurement error should be modeled as additive observation noise, not as part of the state dynamics.
- Transitory dynamics should be modeled as a fast-decaying state component that generates genuine but temporary displacement.

The distinction matters for how the model handles rank changes: measurement error should not produce "real" rank swaps, while transitory dynamics should produce temporary but genuine rank changes.

---

## 3. Approaches from the Literature

### 3.1 The MA(1) Innovation Model

The simplest model matching our autocorrelation structure treats the log-activity change as an MA(1) process:

$$\Delta \log(y_t) = \mu_t + u_t + \theta u_{t-1}$$

With lag-1 autocorrelation of −0.40, the invertible MA parameter is $\theta \approx -0.5$. This decomposes the innovation into a permanent part $\mu_t$ and a transitory part captured by the negative MA coefficient.

**For simulation**, this means:
- Draw an i.i.d. innovation $u_t$ each period
- The *effective* change is $u_t + \theta u_{t-1}$, so part of this period's shock is offset next period
- The permanent effect of a unit shock is $1 + \theta = 0.5$, meaning only half persists

**Limitations:** This captures only one-period reversion. Our data shows the variance ratio continuing to decline well beyond lag 1, suggesting a richer transitory structure.

### 3.2 The Permanent + AR(1) Transitory Model

A more realistic decomposition models the transitory component as an AR(1) process:

$$\log(y_t) = \tau_t + c_t$$
$$\tau_t = \tau_{t-1} + \eta_t, \quad \eta_t \sim N(0, \sigma_\eta^2)$$
$$c_t = \phi c_{t-1} + \nu_t, \quad \nu_t \sim N(0, \sigma_\nu^2)$$

Observing only $y_t$, the implied change $\Delta y_t = \eta_t + c_t - c_{t-1} = \eta_t + (\phi - 1)c_{t-1} + \nu_t$ generates autocorrelation at all lags. The lag-$k$ autocorrelation of $\Delta y_t$ decays geometrically at rate $\phi$ (plus corrections for the MA component introduced by the permanent shock).

**For simulation**, we evolve two state variables per endpoint: the permanent level $\tau$ and the transitory deviation $c$. The observed activity is their sum. This naturally produces:
- Large week-to-week fluctuations (driven by $\nu_t$)
- Rapid decay of transitory shocks (governed by $\phi$)
- Slow drift of the permanent level (driven by $\eta_t$)

### 3.3 The Rank-Dependent OU Model

Combining the Atlas model structure with OU mean reversion, each endpoint's log-activity evolves as:

$$d \log(y_i) = \theta_{r_i}(\mu_{r_i} - \log(y_i)) \, dt + \sigma_{r_i} \, dW_i + \sigma_\text{perm} \, dB_i$$

where $r_i$ is the current rank of endpoint $i$, and $B_i$ is an independent Brownian motion driving permanent change. The rank-dependent OU component provides mean reversion toward the "typical" log-activity for a given rank, while the Brownian component provides permanent drift.

This elegantly captures:
- Macro stability: the rank-dependent attractor $\mu_r$ defines the stable rank-size distribution
- Transitory fluctuations: the OU component produces mean-reverting deviations
- Individual mobility: the permanent Brownian component drives genuine rank changes

### 3.4 The Kalman Filter / State-Space Approach

Rather than specifying a parametric model for transitory dynamics, the Kalman filter approach treats the problem as one of optimal signal extraction:

- **Observation:** Weekly activity data $y_{i,t}$
- **State:** True underlying engagement $\tau_{i,t}$ plus transitory component $c_{i,t}$
- **Goal:** Estimate the "filtered" state $\hat{\tau}_{i,t}$ that strips out transitory noise

For simulation purposes, the Kalman smoother applied to historical data produces the optimal estimate of the permanent component trajectory. The simulation then evolves the permanent component forward with its estimated variance, and adds fresh transitory noise.

### 3.5 The Two-Scale Diffusion

A physics-inspired approach treats the system as having two timescales:

$$dX_t = dX_t^\text{slow} + dX_t^\text{fast}$$

The slow process has small variance but accumulates over time (the permanent component). The fast process has large variance but is mean-reverting with a short decorrelation time (the transitory component). Homogenization theory provides rigorous results: on timescales much longer than the fast process's decorrelation time, the system behaves like pure diffusion driven by the slow component alone.

**For simulation at the weekly timescale**, the fast component has already partially decorrelated within one timestep. The effective one-step distribution is the convolution of the slow component's increment with the fast component's stationary distribution — producing the characteristic "excess variance that doesn't accumulate" pattern.

---

## 4. Practical Modeling Strategies

### 4.1 Parameter Estimation

**Variance ratio approach.** The variance ratio at horizon $k$ directly estimates the permanent variance fraction:

$$\hat{r}_\text{perm} \approx V(k) \text{ for sufficiently large } k$$

From our data, $V(26) \approx 0.064$ and $V(52) \approx 0.028$. These continue to decline, suggesting that even the "permanent" component may have some very long-term mean reversion — or that the sample is too short to distinguish the permanent level from slow drift.

**For practical simulation**, a permanent fraction of 5–10% of weekly variance (corresponding to VR at horizons of 6–12 months) provides a reasonable working estimate.

**Autocorrelation approach.** The MA(1) coefficient from the lag-1 autocorrelation gives a quick estimate: with $\rho(1) \approx -0.40$, the permanent fraction is $(1 + \theta)^2 / (1 + \theta^2)$, where $\theta \approx -0.5$. This gives approximately $0.25 / 1.25 = 0.20$, matching the project's prior estimate.

**AR(1) decay rate.** Fitting an AR(1) to the detrended rank or log-activity levels gives $\phi \approx 0.45$ (weekly), corresponding to a half-life of about 0.9 weeks. This matches the observation that most correlation is gone after one week.

### 4.2 Simulation Architecture

The recommended simulation architecture has three layers:

**Layer 1: Macro constraint.** The rank-size distribution is fixed as an empirical target. The share of total activity at rank $r$ is set by the observed distribution (approximately Zipf with slope −1.1).

**Layer 2: Permanent dynamics.** Each endpoint has a permanent log-activity level $\tau_i$ that evolves as:

$$\tau_{i,t+1} = \tau_{i,t} + \eta_{i,t}$$

where $\eta_{i,t} \sim N(0, \sigma_\eta^2(r_{i,t}))$. The permanent innovation variance $\sigma_\eta^2$ may depend on rank (higher-ranked endpoints are often more stable).

**Layer 3: Transitory dynamics.** Each endpoint has a transitory component $c_{i,t}$ that evolves as:

$$c_{i,t+1} = \phi c_{i,t} + \nu_{i,t}$$

where $\nu_{i,t} \sim N(0, \sigma_\nu^2(r_{i,t}))$. The observed log-activity is $\log(y_{i,t}) = \tau_{i,t} + c_{i,t}$.

**Re-ranking.** At each timestep, endpoints are re-ranked based on $y_{i,t} = \exp(\tau_{i,t} + c_{i,t})$. The transitory component creates temporary rank swaps that mostly revert, while the permanent component drives durable rank changes.

### 4.3 Calibration Targets

A well-calibrated model should match:

1. **Rank-size distribution:** The share at each rank matches the empirical Zipf curve.
2. **Variance ratio profile:** The variance ratio at horizons 2, 4, 8, 13, 26 weeks matches empirical values.
3. **Autocorrelation function:** Lag-1 autocorrelation of log-activity changes ≈ −0.40, near-zero at lag 3+.
4. **Rank autocorrelation:** The rank level autocorrelation matches empirical decay (≈0.45 at lag 1, ≈0.06 at lag 13).
5. **Top-N persistence:** The fraction of top-100 endpoints remaining in top-100 at lag $k$ matches empirical counts (76% at 1 week, 64% at 4 weeks, 43% at 52 weeks).
6. **Cross-sectional R²:** The cross-sectional correlation of log-activity levels decays at the empirical rate.

### 4.4 Common Pitfalls

1. **Equating measurement noise with transitory dynamics.** Not all mean-reverting variation is observational noise. Some represents genuine but temporary engagement shifts. Treating everything as measurement error produces simulations that are too smooth within each period.

2. **Over-deflating the innovation variance.** Using the long-horizon variance ratio (e.g., VR(52) ≈ 0.03) to deflate weekly shocks produces movements that are far too small. The correct approach is to keep the *total* weekly variance near its observed value, but partition it into permanent and transitory components.

3. **Ignoring rank-dependent heterogeneity.** The volatility structure is not uniform across ranks. Top-ranked endpoints tend to have lower relative volatility (log-scale) than mid- or low-ranked ones. Modeling uniform volatility produces unrealistic churn at the top of the distribution.

4. **Gaussian assumptions in the tails.** The empirical distribution of log-activity changes is heavy-tailed (kurtosis >> 3). Using purely Gaussian innovations understates the frequency of extreme rank jumps.

5. **Neglecting entry and exit.** In platform data, new endpoints enter the ranking and old ones exit. A pure diffusion model with fixed N misses this. Entry/exit can be modeled as birth-death process at the boundaries.

6. **Confusing the Cochrane variance ratio with a variance multiplier.** The variance ratio $V(k)$ measures persistence, not the "right" variance to use. The simulation should use the *full* observed variance but decompose it appropriately, not scale it by $V(k)$.

---

## 5. Recommended Modeling Approach

### 5.1 Model Specification

For the Facebook platform data, we recommend a **permanent-transitory rank diffusion model**:

**State variables per endpoint $i$:**
- $\tau_{i,t}$: permanent log-activity (random walk with drift)
- $c_{i,t}$: transitory deviation (AR(1), fast-decaying)

**Evolution equations:**
$$\tau_{i,t+1} = \tau_{i,t} + \mu(r_{i,t}) + \eta_{i,t}, \quad \eta_{i,t} \sim N(0, \sigma_\eta^2(r_{i,t}))$$
$$c_{i,t+1} = \phi \, c_{i,t} + \nu_{i,t}, \quad \nu_{i,t} \sim N(0, \sigma_\nu^2(r_{i,t}))$$

**Observation:**
$$\log(y_{i,t}) = \tau_{i,t} + c_{i,t}$$

**Ranking:**
$$r_{i,t} = \text{rank of } y_{i,t} \text{ among all endpoints at time } t$$

**Macro constraint (soft):**
A gentle restoring force on the permanent component prevents the system from drifting away from its empirical rank-size distribution. This can be implemented as a rank-dependent drift $\mu(r)$ that is slightly negative for endpoints above their "equilibrium" activity for their rank, and slightly positive for those below.

### 5.2 Estimation Strategy

1. **Estimate the permanent/transitory split** using the variance ratio profile. Target VR(4) ≈ 0.33 and VR(13) ≈ 0.12 simultaneously.

2. **Estimate the AR(1) coefficient** $\phi$ from the autocorrelation decay. Target: lag-1 autocorrelation of changes ≈ −0.40, implying $\phi \approx 0.4$–$0.5$ for the transitory component.

3. **Estimate rank-dependent parameters** by stratifying endpoints into rank bands and computing within-band variance ratios and autocorrelations.

4. **Validate** against the calibration targets in §4.3, iterating parameter choices as needed.

### 5.3 Simulation Algorithm

```
For each time step t:
  1. For each endpoint i:
     a. Draw permanent innovation: η_i ~ N(0, σ²_η(r_i))
     b. Draw transitory innovation: ν_i ~ N(0, σ²_ν(r_i))
     c. Update permanent: τ_i ← τ_i + μ(r_i) + η_i
     d. Update transitory: c_i ← φ · c_i + ν_i
     e. Compute observed: y_i = exp(τ_i + c_i)
  2. Re-rank all endpoints by y_i
  3. (Optional) Apply soft macro constraint to τ_i
```

---

## 6. Key References

- Banner, A.D., Fernholz, R., & Karatzas, I. (2005). Atlas models of equity markets. *Annals of Applied Probability*, 15(4), 2296–2330.
- Campbell, J.Y., & Mankiw, N.G. (1987). Permanent and transitory components in macroeconomic fluctuations. *American Economic Review*, 77, 111–117.
- Cochrane, J.H. (1988). How big is the random walk in GNP? *Journal of Political Economy*, 96(5), 893–920.
- Fernholz, E.R. (2002). *Stochastic Portfolio Theory*. Springer.
- Fernholz, R.T. (2020). Zipf's law for Atlas models. *Journal of Applied Probability*.
- Harvey, A.C. (1989). *Forecasting, Structural Time Series Models and the Kalman Filter*. Cambridge University Press.
- Ichiba, T., Papathanakos, V., Banner, A., Karatzas, I., & Fernholz, R. (2011). Hybrid Atlas models. *Annals of Applied Probability*, 21(2), 609–644.
- Ichiba, T., Pal, S., & Shkolnikov, M. (2013). Convergence rates for rank-based models with applications to portfolio theory. *Probability Theory and Related Fields*, 156, 415–448.
- Jourdain, B., & Reygner, J. (2013). Propagation of chaos for rank-based interacting diffusions. *Stochastics and PDE: Analysis and Computations*, 1(3), 455–506.
- Lo, A.W., & MacKinlay, A.C. (1988). Stock market prices do not follow random walks. *Review of Financial Studies*, 1(1), 41–66.
- Morley, J., Nelson, C., & Zivot, E. (2003). Why are Beveridge-Nelson and unobserved-components decompositions of GDP so different? *Review of Economics and Statistics*, 85(2), 235–243.
- Shkolnikov, M., & Yeung, L.C. (2024). From rank-based models with common noise to pathwise entropy solutions of SPDEs. *arXiv:2406.07286*.
