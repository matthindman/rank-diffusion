# Variance, named movement, and stationary rank structure

2026-10-12. Independent Codex follow-up to the completed D1 debate. Mathematical analysis and research proposals, not an adopted model or paper claim. Empirical companion: REPORT.md and STATUS_ENTRY.md in this directory. Every external citation below has a reading-status tag; “read” means the specified portion, not necessarily the whole paper.

## The leverage and the identification limit

The owner's proposed connection is productive. A stationary ladder imposes a balance between diffusion, drift, and population flow. It can therefore constrain a movement model much more strongly than separate marginal scorecards. But variance alone does not determine the ladder. Nor does a matching ladder identify a unique individual process. A model becomes explanatory to the extent that its movement coefficients and boundary mechanism are measured independently and then constrain the structure without reusing that structure as a target.

Use X for centered latent log attention, u=P(X>x) for descending population quantile, x(u) for its inverse, and S(u)=-dx/dlog(u)>0 for the local ladder slope. Our actual empirical instrument uses log(1+count); interpreting it as log attention is an additional approximation, especially near zero. A variance rate a is in squared log units/week; S is in log units, and g=a/(2S) is in log units/week, conventionally written /week. None is a variance of ordinal rank increments.

Consider a stationary, continuous, one-dimensional diffusion with effective drift b(x), diffusion variance a(x), density p(x), and zero probability current. With adequate differentiability, integrability, and vanishing a(x)p(x) at the upper boundary,

    J(x) = b(x)p(x) - (a(x)p(x))'/2 = 0,
    a(x)p(x) = -2 ∫_x^∞ b(y)p(y)dy.

Since p(x(u))=u/S(u), define G(u)=-∫_0^u b(x(v))dv. Then

    S(u) = u a(u) / [2 G(u)],
    a(u) = 2 G(u) S(u) / u.                         (1)

This is a direct stationary-balance derivation. In a centered mean-field rank diffusion, it is the descending-rank version of the known stationary quantile formula, subject to the equilibrium and regularity conditions of that theory. Jourdain and Reygner give the quantile integral and convergence result in §2.4/Theorem 2.4. [READ THIS SESSION: definitions, §2.4 and theorem statement; https://arxiv.org/pdf/1312.5660]

If b=-g throughout the upper range, G(u)=gu there and (1) becomes

    S(u)=a(u)/(2g).                                  (2)

Thus a nearly constant a/(2S) is consistent with a nearly constant average upper-tail drift. It is a useful restriction, not proof that the drift is constant, that the system is stationary, or that the observed long-lag variance equals a. Stability still requires compensation below that range or a boundary/renewal mechanism. A negative constant drift on the entire real line has no stationary probability distribution.

Given independently measured G and a, integrate -a(u)/(2G(u)) to recover x(u) up to one additive level. Given S and G, recover a. Given S and a, recover G and, if sufficiently smooth, b(u)=-G'(u). The last is an implied drift, not an independent drift measurement. Derivatives amplify error, making integrated bandwise drift the better initial estimand.

Nonidentification is immediate: for any suitable positive density p and positive a, choose b=(ap)'/(2p) with compatible boundaries. Many variance profiles then have exactly that p. Multiplying both a and b by c>0 changes the speed and leaves the stationary density unchanged. This proves that a static ladder cannot identify the absolute movement clock. It also shows why calling the calibrated ratio a measured restoring force would be circular.

## Why finite Atlas formulas need qualification

For a stable finite first-order system with descending ranks, rank drift g_j relative to the system center, independent name Brownian drivers, and a product-form stationary gap law, put G_k=-∑_{j≤k}g_j>0. The usual gap result is

    E[X_(k)-X_(k+1)] = (a_k+a_(k+1))/(4G_k).

The discrete log-rank slope divides this by log(1+1/k), rather than exactly multiplying by k. Constant or affine rank variances satisfy the familiar skew-symmetry/product-form conditions; arbitrary empirical variance curves do not automatically do so. Hybrid name effects require the joint gap/permutation distribution. [READ THIS SESSION: Ichiba et al., Lemma 3 and Theorem 2, §§5.2–5.3; https://arxiv.org/pdf/0909.0065]

For g_j=-g away from the compensating bottom, the large-k approximation is S_k≈(a_k+a_(k+1))/(4g). Fernholz and Fernholz explicitly treat Atlas/quasi-Atlas laws and their empirical approximations. [READ THIS SESSION: §§2–3, especially (2.9)–(2.18); https://arxiv.org/pdf/1707.04285]

The continuum identity (1) is broader than those finite product laws. However, a continuum result is not an error bound for our finite, named, censored, nonstationary system. The mean of a gap, the gap of a time-median ladder, and the quantile of a pooled marginal distribution are also distinct objects. The mean-versus-median sensitivity in REPORT.md checks only one of these distinctions.

## Named endpoint characteristics are central

Suppose endpoint type θ has drift b_θ and variance a_θ. For a stationary marginal mixture, the relevant coefficients are occupancy-weighted:

    a_eff(x)=E[a_θ(t) | X_θ(t)=x],
    b_eff(x)=E[b_θ(t) | X_θ(t)=x].

They are not generally the variance and drift of endpoints whose permanent rank lies in the corresponding band. If a_i is constant for each name, rank k has effective variance ∑_i π_ik a_i, where π_ik is its rank occupancy probability. Its future occupancy must be explained or separately estimated. Assigning coefficients by permanent rank and assuming that permanent rank is current rank omits this mapping.

The marginal forward equation follows by applying Itô's formula to test functions and conditioning the coefficients on X. It does not require the observed scalar process itself to be Markov. Mimicking results formalize how conditional coefficients can reproduce one-time marginals while leaving path behavior unidentified. [READ THIS SESSION: Brunick and Shreve, Corollary 3.7 and adjacent examples/discussion; https://www.math.cmu.edu/users/shreve/MimickingItoProcess.pdf]

Consequently, even a perfect variance–slope relation would not explain identity persistence, return after displacement, residence times, or pairwise crossings. Nor does name-dependent variance itself imply a restoring home. These require additional movement statistics.

For two names, the instantaneous variance of their log gap is a_i+a_j-2c_ij. A common shock can create large individual movement and little reordering. Our common-factor subtraction removes one estimated shared component, not all local or sector covariance. For stationary relative coordinates, those covariances also affect the effective coefficients after centering. Cross-covariance is not an extra term in an already-correct one-dimensional marginal forward equation; it is needed to construct the correct relative system and its collision behavior.

The project rule to estimate variance by permanent rank should remain. The solution is to estimate named/type coefficients with independent membership information and then transport them through an occupancy model. It is not to regress noisy current-rank increments naively. Separate windows or independent observations must prevent the rank label and measured movement from sharing the same noise.

## Entry, exit, and an observable balance residual

With sources and sinks, stationarity gives J'(x)=s(x), not J=0. Assuming the required upper-boundary limits and integrals exist,

    a(x)p(x) = 2G(u) + 2∫_x^∞ J(y)dy,
    S(u) = u a(u) / [2G(u)+2∫_x(u)^∞ J(y)dy].        (3)

If population size grows, the normalized density equation also includes dilution; if attention jumps, a nonlocal jump term appears. A disappearance from an instrument is not automatically a death, and crossing K is not automatically entry into the population. These distinctions must be measured.

Equation (3) suggests a concrete residual: a(u)u/[2S(u)] minus independently measured G(u). Under the stated model it equals the integrated current. A systematic discrepancy is therefore a target for turnover, omitted covariance/centering, jumps, observation noise, or nonstationarity—not automatically evidence for a personal home. A finite upper boundary with nonzero ap adds its own boundary term.

The empirical equity literature is unusually close to our question. Campbell, Song and Wong distinguish stable open universes from changing fixed cohorts and document volatility/rank patterns. Their observations concern equities, not social endpoints, and are not evidence that the same mechanism operates here. [READ THIS SESSION: §§3.1–3.2 and §§5.1–5.3 of the October 2024 v2 revision (latest PDF too large for the browser; v3 HTML unavailable); https://arxiv.org/html/2409.10859v2]

Recent work by Itkin, Larsson and Zhang connects volatility, neighboring correlation, collision rates, and particle density while correcting entry/exit effects. The local-model slides give density approximately collision rate divided by 2σ²(1−ρ). That is a local-model result, not a universal finite-system theorem. [READ THIS SESSION: 2025 slides, “Particle Density,” “Local Model,” proposition and conclusion; https://www.lse.ac.uk/statistics/assets/Documents/Research-Showcase-Slides/Research-Showcase-2025/David-Itkin.pdf]

A March 2026 seminar abstract reports further bias corrections and stabilization by listings/delistings. This is evidence of an active research direction, not a checked published proof. It makes a blanket assertion that entry/exit stabilization is an untouched problem untenable. [READ THIS SESSION: full primary seminar abstract only; https://orfe.princeton.edu/events/2026/martin-larsson-carnegie-mellon]

## What “lasting variance” measures

For the illustrative decomposition Y_i(t)=μ_i t+√a_i W_i(t)+e_i(t), with independent components and stationary transient noise covariance C(h), the across-name/time increment variance has terms

    D(h)=E[a_i]h + Var(μ_i)h² + 2[C(0)-C(h)].

Seasonality adds its lag-dependent variance and possibly cross-terms. Therefore [D(h₂)-D(h₁)]/(h₂-h₁) cancels a plateau of transient variance, but retains Var(μ_i)(h₁+h₂) and any residual seasonal/transient differences. With mean reversion, even the latent stochastic increment variance need not grow linearly. Matching finite masks improves comparability, but does not establish an instantaneous diffusion coefficient.

Noise-robust integrated-volatility estimation is a mature literature: Zhang, Mykland and Aït-Sahalia combine timescales to remove observation-noise bias. Their high-frequency asymptotics and independent-noise setup cannot simply be transplanted to short weekly panels. [READ THIS SESSION: observation model and §§3.3–3.5; https://swh.princeton.edu/~yacine/twoscales.pdf]

An exact elementary warning helps interpret our decomposition. For unit-variance Brownian motion sampled at m consecutive integer start times, let M_h=m⁻¹∑_{t=0}^{m-1}(W_(t+h)-W_t). Then

    Var(M_h)=m⁻²[m h+2∑_{d=1}^{min(m-1,h-1)}(m-d)(h-d)].

Proof: the covariance of two h-increments is their overlap, (h-|t-s|)_+; sum all pairs. Thus differences between endpoint sample means can explain a large share of long-lag pooled variance even with zero permanent drift. This is a finite-window calculation, not a simulation or a new theorem. Subtracting that observed component mechanically would discard real diffusion. Its benchmark fractions are reported with the data.

## Useful proof and measurement problems

The basic variance–slope identity is known. The opportunity is to establish when the estimators and named endpoint process satisfy its assumptions, and how large errors can be. I would prioritize:

1. **An estimand bridge with an explicit error budget.** Bound the discrepancy between a permanent-rank, noisy finite-lag variance estimate and the occupancy-weighted instantaneous relative variance required by (1). Specify mixing, observation error, missingness, drift heterogeneity, and occupancy assumptions. First prove identification or a counterexample for the proposed observation scheme; then consistency/rates. Weekly trajectories with unrestricted latent drift and noise are not identified merely by collecting more endpoints.
2. **A finite-size, typed population result with renewal.** For finitely many endpoint types, specify drift/variance functions of quantile and type, replacement hazards and replacement distributions independently of the target ladder. Study the coupled stationary equations 0=−(b_θp_θ)'+(a_θp_θ)''/2−λ_θp_θ+ν_θ∫λ_θp_θ, with u(x)=∑_θ∫_x^∞p_θ. Prove existence, uniqueness where true, and convergence/mixing and finite-N bounds on a fixed interior quantile interval. This is a proposed precise research problem; novelty and needed hypotheses have not been established. Common noise may require a conditional/random rather than deterministic stationary measure.
3. **A finite-sampling collision estimator with noise and turnover.** Derive the correction for replacing named increments by reordered increments, then combine it with observation noise and entry/exit corrections. Itkin and Larsson explicitly leave rigorous justification of their finite-frequency volatility-bias heuristic as future research in Appendix B.1. [READ THIS SESSION: Appendix B.1 and abstract/version history; https://arxiv.org/pdf/2403.04674] The arXiv page still shows the 2024 version, and no verified resolution was located; this search does not certify that the question remains unsolved. The new turnover work may overlap it.
4. **A conditional stability/error proposition we can already prove.** If positive estimates satisfy a_hat=a(1+ε_a), G_hat=G(1+ε_G), with |ε_a|≤δ_a and |ε_G|≤δ_G<1, then |S_hat/S−1|≤(δ_a+δ_G)/(1−δ_G). With a shared anchor, the absolute log-ladder error is bounded by this factor times ∫S(v)dlog(v) over the interval. Proof: divide (1+ε_a) by (1+ε_G), subtract one, and integrate. This elementary result is not a novelty claim; it shows that accurate variance cannot compensate for poorly identified cumulative drift, especially near G=0.

These are tractable targets for serious mathematical work; model strength is not a guarantee of a proof. A literature gap must be verified, and a proof must be independently checked. Our first need is an identified empirical target, rather than a general theorem about an inadequately specified process.

## What would change my view

The case strengthens substantially if independently estimated signed drift, covariance, and renewal produce the observed slope and its changes across time, with calibrated uncertainty and named movement also reproduced. A variance shift at stable drift predicts a proportional local slope shift under (2), offering a temporal test instead of another static fit. A measured change in entry/exit predicts a different shift through (3).

I would weaken the diffusion account if noise-managed instantaneous variance fails to track the ladder although long-lag dispersion does; if a/(2S) is explained by cohort selection or trends; if drift/current measured independently contradicts the implied balance; or if jumps dominate structural change. A model that matches the ladder while failing identity persistence and crossing behavior is incomplete even if every aggregate ratio looks attractive.

This establishes at most a structural process explanation conditional on the measured coefficients. It does not by itself explain why social endpoints have those coefficients. Endpoint attributes, exposure opportunities, platform rules, or an external change that shifts movement independently of the ladder would help distinguish causal mechanisms.

For a PNAS-oriented account, the contribution should be a measured process that jointly explains structural stability and named movement, with discriminating evidence against plausible alternatives. The present shape match is a promising empirical constraint and a reason to do that work. It is not already the causal result.
