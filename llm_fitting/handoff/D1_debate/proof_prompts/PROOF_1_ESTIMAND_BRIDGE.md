# Prompt 1 — identify the diffusion coefficient needed by the ladder equation

You are conducting mathematical research in stochastic processes and statistical identification. Resolve the focused problem below as far as possible with complete, checkable arguments. A theorem, a counterexample, and a proved identification boundary are all useful outcomes. Do not assume the proposed rank-diffusion interpretation is correct.

## Scientific target

Determine when noisy, discrete observations of named endpoints identify the effective **instantaneous variance of relative log activity** needed in a stationary rank-size equation. Establish what the current finite-lag instrument measures, what it cannot identify, and a sufficient additional observation design or set of assumptions. Do not solve this by calibrating coefficients to the observed ladder.

## Definitions

Let X_i(t) be a named endpoint's latent log activity in a declared relative coordinate system. Between any population changes, write

    dX_i(t) = beta_i(t) dt + dM_i(t),
    d<M_i,M_j>_t = c_ij(t) dt,    a_i(t) = c_ii(t).

All these coefficients refer to the relative coordinates, after the chosen centering. Start with a fixed population and no jumps. State the filtration, regularity, dependence, and any stationarity assumptions you require. Do not treat a common-factor subtraction estimated from the same panel as known without accounting for its error.

We observe Y_i,n = X_i(n Delta) + e_i,n at fixed Delta > 0 over a finite span T. Begin with full observation and known removal of deterministic seasonality; explicitly label those simplifications. The error process may be serially dependent. Independence from X, an error covariance model, or replicate measurements are additional assumptions, not supplied facts.

An endpoint is assigned to a permanent-rank cohort C using separate calibration information. This is a persistent label, not its current rank. Separate windows do not imply independence when the process has memory. Define precisely whether results condition on calibration information. Missingness and estimated cohort labels are extensions after resolving the baseline.

For a specified common distribution of endpoint/start-time pairs, define

    D_C(h) = Var(Y_i(t+h) - Y_i(t) | i in C),
    A_C(h1,h2) = [D_C(h2)-D_C(h1)]/(h2-h1),

where h1,h2 are positive integer multiples of Delta. This is a population moment; distinguish it from its finite-sample estimator with overlapping increments. State the weighting and centering convention, and the consequences of changing eligible pairs between lags.

For a stationary population marginal with density p, let u(x)=P(X>x), x(u) its descending quantile, and S(u)=-d x(u)/d log u. The structural coefficient is

    a_eff(x) = E[a_I(t) | X_I(t)=x],

where I is a uniformly sampled endpoint. On an interior quantile band B=[u1,u2], also consider the better-conditioned target

    a_B = E[a_I(t) | u(X_I(t)) in B].

Neither target is generally A_C for a similarly numbered permanent-rank cohort. When persistent types have constant variances a_theta, occupancy weights give a_eff(x)=sum_theta P(theta | X=x) a_theta; those weights must be identified too. For within-type state-dependent variances, do not factor a conditional expectation without justification.

Under zero current and suitable upper-boundary conditions, the known stationary equation is

    S(u) = u a_eff(x(u)) / [2G(u)],
    G(u) = -integral_0^u b_eff(x(v)) dv.

This formula is background, not the theorem to rediscover. G must be measured independently if the equation is to predict structure. The mathematical output must not claim identification of a cause merely from this identity.

## Required result

First resolve identification from the law of the discrete observations, not only from two moments. Specify a model class and ask whether distinct admissible latent/observation processes have the same observable panel law and calibration law but different a_B or a_eff. If giving a counterexample, verify every assumption and equality of the observable laws. Matching only D_C(h1) and D_C(h2) proves a weaker moment-identification failure; label it accordingly. An unrestricted-noise counterexample is the opening result, not the entire research contribution.

Then obtain the strongest useful positive result you can in a clearly stated restricted class: an exact identification theorem, a finite-Delta bias/error bound, or an identified interval for a_B. Separate what comes from the data, a noise instrument, and modeling assumptions. Do not assert identification is universally impossible at fixed Delta: restrictive diffusion/observation models may identify their generators. Conversely, do not apply high-frequency consistency to fixed weekly observations. Treat increasing N at fixed Delta,T, increasing T, and Delta tending to zero as different experiments. If a result uses a long-span or high-frequency limit, explain what it does and does not establish for a short weekly panel.

The bridge should expose, as applicable: measurement-error covariance, finite-lag drift and mean reversion, drift–martingale cross terms, uncertainty from overlapping increments and shared shocks, cohort selection, and transport from permanent cohorts to contemporaneous latent occupancy. A result for oracle occupancy is acceptable if it states exactly what additional observations make that oracle replaceable. Replicate noisy observations may identify an error law without resolving every continuous-time or occupancy ambiguity.

Use simple independently derivable cases to audit the claim: constant drift Brownian motion with independent noise, heterogeneous endpoint trends, and stationary Ornstein–Uhlenbeck dynamics. The purpose is to catch false cancellations and hidden limiting assumptions, not to treat these examples as novel mathematics.

End with an explicit measurement implication: which extra observation or restriction would let independently measured movement constrain S, and which uncertainty would remain. If possible, propagate the proved variance error to structural reconstruction conditional on independently supplied G bounded away from zero on the chosen interval. Do not substitute this elementary propagation for the identification result.

## Evidence and output standard

Search current primary literature on low-frequency diffusion identification, measurement error, panel heterogeneity, and conditional projection. The project theory note is a source map, not an authority. For each invoked theorem, supply its exact version, assumptions, and their correspondence to this problem. Tag each citation as READ THIS SESSION (specify portion), KNOWN FROM TRAINING AND NOT RE-READ, or SEARCH SUMMARY ONLY. Only the first category can serve as an externally verified proof dependency; otherwise prove the needed statement or mark the dependency unresolved.

Choose your own proof strategy and pursue it beyond an outline. Do not silently weaken the target; state and justify any restricted theorem separately. Return: precise statement and quantifiers; prior-art relationship; complete proof or explicit counterexample; an assumption-to-observation table; and a list of unresolved claims with the first missing lemma. Distinguish identification, stability, consistency, and finite-sample precision. Use conventional mathematical notation and numbered claims; provide an inspectable proof rather than a transcript of exploratory thought. Do not call a result new or an open problem solved without checking the literature and obtaining independent scrutiny.

This is theoretical work only. Do not read empirical data or run fits, stochastic simulations, model implementations, or gate evaluations. In the project environment, obey its working-copy and write-path restrictions. Do not start additional agents or external jobs from this prompt. If a source is unavailable, continue with what can be proved and list that limitation. At a resource limit, preserve established results and the exact remaining proof obligation; do not invent a conclusion.
