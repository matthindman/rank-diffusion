# Estimating Unobserved Tail Mass in Rank-Size Distributions

## The problem

You observe the top K entities in some ranked system — the biggest Facebook pages, the largest firms, the most-cited papers — and you want to know: how much total activity is there, including everything below your observation threshold?

Every platform has at least one paper claiming the distribution is "approximately power law." And if you squint at a log-log plot, the line does seem kind of straight, at least near the head. But if you actually compute the derivatives, you see that the slope is monotonically steepening over the entire curve. It's gentle at first, then it balloons the farther down you go. No platform I've looked at is actually power law. They're all concave in log-log.

The standard approach — fit a power law and extrapolate — ignores this curvature and overestimates the tail. Fitting a polynomial directly to the log-log curve is unstable: a cubic can turn upward and give you infinite tail mass (this actually happens about half the time on real data). You need something that respects the concave shape while still giving you a well-behaved finite extrapolation under admissible parameters.

## The idea

Instead of modeling log(activity) as a polynomial in log(rank), model **log(alpha)** — the log of the local Pareto exponent — as a polynomial. This is a small but critical change.

Define alpha(x) = -z'(x), the slope of the log-log curve at each point. For a power law, alpha is constant. For a concave curve, alpha increases into the tail. The model is:

$$\alpha(u) = \alpha_0 \cdot \exp\!\left(\eta_0 u + \tfrac{1}{2}\eta_1 u^2 + \tfrac{1}{6}\eta_2 u^3 + \cdots\right)$$

where $u = \log(r / r_c)$ is the log-rank distance past the censoring boundary.

Truncating at different polynomial degrees gives a family of nested models:

| Degree | Name | What it says about the tail |
|---|---|---|
| 0 | Power law | Slope is constant. Loosest extrapolation; an upper bound if the tail keeps steepening. |
| 1 | Constant $\eta$ | Slope steepens exponentially. Lower estimate within the same family. |
| 2 | Linear $\eta$ | The steepening itself accelerates. Lower still within the same family. |
| 3 | Quadratic $\eta$ | One more derivative of the steepening. Usually close to degree 2. |

The coefficients are:

- **$\alpha_0$**: the local Pareto exponent right at the boundary. Higher means steeper slope, less tail mass.
- **$\eta_0$**: how fast alpha is growing. Positive means the tail is getting steeper.
- **$\eta_1$**: how fast the growth rate itself is increasing. Positive means the steepening accelerates.
- **$\eta_2$**: one more level of acceleration.

## Why this works where polynomials don't

**Guaranteed convergence.** Because alpha controls the slope and we enforce that the polynomial coefficients are non-negative (the tail steepens at least as fast as observed), any model with positive curvature eventually decays super-exponentially. The degree-0 case reduces to the usual power-law condition, so you still need $\alpha_0 > 1$ there. A naive polynomial in log-log has no such guarantee — the leading coefficient can go positive and blow up.

**Nested model family.** Within this parameterization, each additional degree gives a weakly lower estimate: D0 >= D1 >= D2 >= D3, with equality allowed when the added coefficient is estimated as zero. The power law is the loosest extrapolation. Each successive degree incorporates more of the observed curvature. The spread between them tells you how sensitive your answer is to the curvature assumptions.

**Physically motivated.** In stochastic portfolio theory and similar rank-based models, rank-dependent volatility creates a distribution that looks Pareto at the head but steepens continuously into the tail. The smallest entities are systematically undersupplied relative to a pure Pareto — a fact you can confirm directly in fully observed systems like Reddit karma or equity market caps. The log-alpha polynomial captures exactly this structure.

## How the estimation works

**Step 1: Anchor at the data.** Compute $y_0$ from a local-linear smoother on log-views in a narrow window around rank $r_c$. This gives a smoothed boundary level that is close to the observed data at $r_c$ while reducing single-rank noise. The key point is that it avoids using the wide-window regression intercept, which can sit below a concave curve.

**Step 2: Fit the slope and curvature.** A tricube-weighted local quadratic on the log-log data gives you alpha (the slope) and eta (the curvature) at the boundary. Use at least 1,000 ranks in the fitting window.

**Step 3: Estimate the higher derivatives.** Fit the same quadratic at two shifted boundaries ($r_c / 1.28$ and $r_c / 1.64$) to get eta at three points. A second-order backward difference (three-point formula) gives $\eta_1$; the standard second difference gives $\eta_2$. Both are clipped to $\geq 0$ — a modeling choice that preserves nesting and encodes the assumption that steepening does not decelerate in the tail.

**Step 4: Integrate.** For each polynomial degree, compute the tail mass:

$$T_{\text{tail}} = y_0\, r_c \int_0^{\infty} \exp\!\left(u - \int_0^u \alpha(s)\,ds\right) du$$

The power law (degree 0) has a closed form: $y_0 r_c / (\alpha_0 - 1)$. Higher degrees are computed numerically; once the curvature is material the integrand decays super-exponentially, and the implementation extends the integration range adaptively so near-power-law cases are not truncated too early.

## Choosing the boundary

The boundary $r_c$ should be where you trust the data — where censoring is minimal, alpha is comfortably above 1, and the curvature parameter eta is relatively stable. If you plot eta across ranks, look for a region where it's not rapidly changing.

You can (and should) run the estimator at several thresholds. If the estimates are consistent, you're in good shape. If they diverge, the answer is sensitive to where you draw the line, and you should think harder about which threshold best separates real data from censoring artifacts.

## Worked example: Facebook page engagement

CrowdTangle tracked weekly engagement for public Facebook pages. The dataset covers 88 weeks with roughly 11,000–14,000 pages per week. CrowdTangle only included pages with 25K+ followers, so there's effectively zero censoring for the top 5,000 ranks.

For the week of **2021-11-29**, setting the boundary at $r_c = 5{,}000$:

**Fitted parameters:**

| Parameter | Value | What it means |
|---|---|---|
| $\alpha_0$ | 1.99 | Slope at rank 5,000. Comfortably above 1, so the power-law integral converges. |
| $\eta_0$ | 0.78 | Substantial curvature. Under degree 1 alone, by rank ~13,600 ($u=1$) the slope would be about $1.99 \cdot e^{0.78} \approx 4.3$. |
| $\eta_1$ | 0.00 | No curvature acceleration detected at this boundary and resolution. |
| $\eta_2$ | 0.00 | No further acceleration detected at this resolution. |

**Results:**

| Model | Unobserved % | What it means |
|---|---|---|
| Power law (D0) | 10.0% | Constant-slope extrapolation: about 10% of total engagement is unobserved. |
| Constant $\eta$ (D1) | 5.7% | Accounting for curvature cuts the estimate nearly in half. |
| Linear $\eta$ (D2) | 5.7% | No further change (η₁ = η₂ = 0 at this boundary). |
| Quadratic $\eta$ (D3) | 5.7% | Same. |

The top 5,000 pages capture roughly 94–95% of total platform engagement, with a best estimate around 94.3%.

**Consistency across weeks at $r_c = 5{,}000$:**

| Week | $\alpha_0$ | $\eta_0$ | PL | D1 | D2 | D3 |
|---|---|---|---|---|---|---|
| 2020-12-14 | 1.75 | 0.08 | 12.5% | 10.7% | 10.7% | 10.7% |
| 2021-11-29 | 1.99 | 0.78 | 10.0% | 5.7% | 5.7% | 5.7% |
| 2022-02-07 | 1.96 | 0.31 | 9.5% | 6.8% | 5.2% | 4.8% |

The power-law extrapolation is stable at 10–12%. The curvature-corrected estimates range from 5–11% depending on how much curvature the fit detects in each particular week.

**Sensitivity to threshold:**

| $r_c$ | $\alpha_0$ | PL | D1 | D2 | D3 |
|---|---|---|---|---|---|
| 3,000 | 1.52 | 24.8% | 13.6% | 12.4% | 12.2% |
| 5,000 | 1.99 | 10.0% | 5.7% | 5.7% | 5.7% |
| 8,000 | 3.41 | 2.1% | 1.4% | 1.2% | 1.2% |

At $r_c = 8{,}000$ the tail is negligible regardless of model (<2%). At $r_c = 3{,}000$, alpha is close to 1 and the power-law estimate gets wide. The sweet spot is $r_c = 5{,}000$: alpha is around 2, the spread between models is informative, and we're confident the data is clean.

## When to use this

This works for many rank-size distributions where the top is fully observed and the tail may be censored: social media engagement, web traffic, firm revenue, wealth/income, city populations, citation counts. The requirements are at least ~1,000 observed ranks in the fitting window and a concave log-log curve (the slope steepens into the tail). That's the generic case for all of these systems.

It's not appropriate if the tail is heavier than Pareto (convex log-log curve), if you have a known finite maximum rank (use the finite-support variant sketched below rather than the infinite-tail default), or if you have fewer than ~1,000 observations to fit on.

## Relationship to existing methods

Clauset-Shalizi-Newman (2009) and Gabaix-Ibragimov (2011) both assume a pure power law above some threshold. The GPD from extreme value theory is closely related: its heavy-tailed case ($\xi > 0$) gives Pareto-like tail behavior, though the GPD is a model for exceedances rather than rank-size curves directly. All of these share the key limitation of assuming constant $\alpha$ in the tail — the degree-0 special case of the present approach. The contribution here is allowing $\alpha$ to vary, which is necessary for distributions that aren't truly Pareto (which is most of them).

## The bottom line

The family of estimates shows how much the answer moves as you add curvature information. The power law is the loosest extrapolation, the higher-degree models impose progressively stronger tail steepening, and the spread between them tells you how much the curvature matters. When the spread is narrow, the tail estimate is robust to modeling details. When it's wide, the tail shape beyond the boundary is doing real work — and having a principled, well-behaved way to encode that shape is the whole point.

---

## Appendix A: Full Derivation

### Setup and notation

Let $y(r)$ be activity (views, revenue, population, market cap) at rank $r$, with $r=1$ being the largest entity. We observe $y(r)$ for $r = 1, \ldots, r_c$ and want to estimate total activity $T = \sum_{r=1}^{\infty} y(r)$.

Move to log-log coordinates:
$$x = \log r, \qquad z(x) = \log y(e^x)$$

A pure power law (Zipf/Pareto) is linear in these coordinates:
$$z(x) = a - \alpha \cdot x$$

where $\alpha$ is the Pareto exponent. Define the **local Pareto exponent** at log-rank $x$:
$$\alpha(x) = -z'(x)$$

and the **log-slope parameter**:
$$\eta(x) = \frac{d \log \alpha}{dx} = \frac{\alpha'(x)}{\alpha(x)}$$

For a pure power law, $\alpha$ is constant and $\eta = 0$. For a concave curve, $\alpha$ increases with $x$ and $\eta > 0$.

### The polynomial model in log-alpha

We model $\log \alpha$ as a polynomial in $u = x - x_c = \log(r/r_c)$:

$$\log \alpha(u) = \log \alpha_0 + \eta_0 \cdot u + \tfrac{1}{2}\eta_1 \cdot u^2 + \tfrac{1}{6}\eta_2 \cdot u^3 + \cdots$$

or equivalently:

$$\alpha(u) = \alpha_0 \cdot \exp\!\left(\sum_{k=0}^{d} \frac{\eta_k}{(k+1)!} \, u^{k+1}\right)$$

where $d$ is the polynomial degree. Concavity of the log-log curve implies $\eta_0 \geq 0$ (the slope steepens). The constraints $\eta_1, \eta_2 \geq 0$ are additional modeling choices — motivated by empirical observation in fully-observed rank-size systems, but not implied by concavity alone. They preserve the nested ordering and encode the assumption that the tail steepens at least as fast as the observed rate.

The predicted activity at rank $r > r_c$ is:

$$y(r) = y_0 \exp\!\left(-\int_0^{u} \alpha(s)\,ds\right), \qquad u = \log(r/r_c)$$

where $y_0$ is the observed activity at rank $r_c$.

### Tail mass

The total activity is $T = \sum_{r=1}^{\infty} y(r)$. We approximate the unobserved portion with a continuous integral:

$$T \approx \underbrace{\sum_{r=1}^{r_c} y(r)}_{\text{observed exactly}} \;+\; \underbrace{\int_{r_c}^{\infty} y(r)\,dr}_{\text{tail mass (continuous approx.)}}$$

(The exact discrete tail under degree 0 would involve a Hurwitz zeta function. The continuous approximation is standard and accurate when $r_c$ is large.)

Substituting $r = r_c e^u$, $dr = r_c e^u\,du$:

$$T_{\text{tail}} = y_0\, r_c \int_0^{\infty} \exp\!\left(u - \int_0^u \alpha(s)\,ds\right) du$$

**Degree 0 (power law).** $\alpha(s) = \alpha_0$, so $\int_0^u \alpha(s)\,ds = \alpha_0 u$:

$$T_{\text{tail}} = y_0\, r_c \int_0^{\infty} e^{(1-\alpha_0)u}\,du = \frac{y_0\, r_c}{\alpha_0 - 1}, \qquad \alpha_0 > 1$$

**Degree 1 (constant $\eta$).** $\alpha(s) = \alpha_0 e^{\eta_0 s}$, so $\int_0^u \alpha(s)\,ds = \frac{\alpha_0}{\eta_0}(e^{\eta_0 u} - 1)$:

$$T_{\text{tail}} = y_0\, r_c \int_0^{\infty} \exp\!\left(u - \frac{\alpha_0}{\eta_0}(e^{\eta_0 u} - 1)\right) du$$

This can be evaluated via the substitution $t = e^{\eta_0 u}$, which transforms it into an upper incomplete gamma-type integral. In practice we evaluate numerically.

**Degree 2.** The inner integral $\int_0^u e^{\eta_0 s + \frac{1}{2}\eta_1 s^2}\,ds$ can be expressed in terms of $\operatorname{erfi}$ (the imaginary error function), but the outer tail mass integral still has no simple closed form. In practice both are evaluated numerically.

**Degree 3.** Both the inner and outer integrals require numerical integration. In all cases the integrand decays super-exponentially once $\alpha$ grows past $\sim 1$, so convergence is rapid.

### Convergence guarantee

For any polynomial $P(u) = \eta_0 u + \frac{1}{2}\eta_1 u^2 + \cdots$ with non-negative leading coefficient and degree $\geq 1$, $P(u) \to +\infty$ as $u \to \infty$, so $\alpha(u) = \alpha_0 e^{P(u)} \to \infty$. By the standard Laplace-type asymptotic, the inner integral satisfies $\int_0^u \alpha(s)\,ds \sim \alpha_0 e^{P(u)} / P'(u)$ for large $u$. Since $P'(u) = O(u^{d-1})$ grows polynomially while $e^{P(u)}$ grows super-exponentially, the inner integral still dominates the linear $u$ term in the exponent $u - \int_0^u \alpha(s)\,ds$. The tail integrand therefore decays super-exponentially and the integral is finite.

For degree 0 (power law), convergence requires $\alpha_0 > 1$.

### Monotone nesting

If $P_d(u) \leq P_{d+1}(u)$ for all $u \geq 0$ (which holds when $\eta_{d+1} \geq 0$), then $\alpha_{d+1}(u) \geq \alpha_d(u)$, so the inner integral is larger at degree $d+1$, the integrand is smaller, and the tail mass is weakly lower:

$$T_{\text{tail}}^{(d+1)} \leq T_{\text{tail}}^{(d)}$$

## Appendix B: Estimation Details

### Anchoring $y_0$

We compute $y_0$ from a local-linear smoother on $\log y$ vs $\log r$ in a narrow window (default: $\pm 30$ ranks) around $r_c$, using tricube weights. This gives $\hat{z}_0 = \hat{\beta}_0$ at $x_c = \log r_c$, and $y_0 = e^{\hat{z}_0}$.

Anchoring at the observed data rather than the quadratic regression intercept is critical for concave curves. For a concave function, a weighted regression over a wide window tends to pull the fitted value at the boundary below the actual data, causing the extrapolation to start too low. The local smoother avoids this by using only the immediate neighborhood of $r_c$.

### Local quadratic fit for $\alpha_0$ and $\eta_0$

On the $n_{\text{fit}}$ observations immediately below $r_c$, fit:

$$z_i \approx \beta_0 + \beta_1(x_i - x_c) + \tfrac{1}{2}\beta_2(x_i - x_c)^2$$

using tricube weights $w_i = (1 - |d_i / h|^3)^3$ where $d_i = x_i - x_c$ and $h$ is the window width.

Then:
$$\alpha_0 = \max(-\beta_1, \; 0.1), \qquad \eta_0 = \frac{\max(-\beta_2, \; 0)}{\alpha_0}$$

The $\beta_0$ from this fit is *not* used for $y_0$ — that comes from the local smoother above.

### Staggered quadratics for $\eta_1$ and $\eta_2$

Fit the same local quadratic at three boundaries: $x_c$, $x_c - \delta$, and $x_c - 2\delta$ (default $\delta = 0.25$). Each fit yields $\eta$ at that point. Then use the three-point backward finite-difference formulas:

$$\eta_1 = \frac{3\eta(x_c) - 4\eta(x_c - \delta) + \eta(x_c - 2\delta)}{2\delta}$$

$$\eta_2 = \frac{\eta(x_c) - 2\eta(x_c - \delta) + \eta(x_c - 2\delta)}{\delta^2}$$

The three-point formula for $\eta_1$ is second-order accurate (exact for quadratic $\eta$) and avoids the $O(\delta)$ bias of a simple backward difference. Both are clipped to $\geq 0$.

The staggered approach is more robust than fitting a single higher-order polynomial (cubic or quartic), which is numerically unstable on noisy log-log data. Each individual quadratic is well-determined; the trend in $\eta$ comes from comparing three robust estimates.

### Recommended defaults

| Parameter | Default | Rationale |
|---|---|---|
| $n_{\text{fit}}$ | $\max(1000, \lfloor 0.3 \cdot r_c \rfloor)$ | Wide enough to estimate curvature; not so wide it reaches the superstar head |
| $\delta$ | 0.25 log-units ($\approx$ rank ratio 1.28) | Balances resolution with stability |
| Anchor half-width | 30 ranks | Local enough to avoid bias from wide-window regression |
| Non-negativity | $\eta_k \geq 0$ for all $k$ | $\eta_0 \geq 0$ from concavity; $\eta_1, \eta_2 \geq 0$ are modeling choices that preserve nesting |

## Appendix C: Finite-Support Variant

If the platform has a known maximum rank $R_{\max}$, replace the infinite upper limit with $U = \log(R_{\max} / r_c)$:

$$T_{\text{tail}}(R_{\max}) = y_0\, r_c \int_0^{U} \exp\!\left(u - \int_0^u \alpha(s)\,ds\right) du$$

For degree 0 this gives:

$$T_{\text{tail}}(R_{\max}) = \frac{y_0\, r_c}{\alpha_0 - 1}\left(1 - \left(\frac{R_{\max}}{r_c}\right)^{1-\alpha_0}\right), \qquad \alpha_0 \neq 1$$

For higher degrees, a finite-support implementation replaces the infinite upper limit with $U$ in the same numerical integration scheme.

A discrete variant sums individual predicted values:

$$T_{\text{disc}}(R_{\max}) = \sum_{r=1}^{r_c} y(r) + \sum_{r=r_c+1}^{R_{\max}} \hat{y}(r)$$

This is often preferable when $R_{\max}$ is known and moderate.
