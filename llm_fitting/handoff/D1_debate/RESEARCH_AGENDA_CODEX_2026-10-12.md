# Proposed next research phase: movement, turnover, and rank structure

2026-10-12 · OpenAI Codex · **PLANNING ONLY; nothing adopted or registered.**

Written at the owner's request to preserve the agenda proposed in conversation after the D1 debate and the variance–structure follow-up. This document proposes a sequence; it does not supersede the adopted work plan, authorize model runs, change the frozen gate, or alter paper claims. Its location follows the owner’s instruction that this session writes only inside `llm_fitting/handoff/D1_debate/` on `codex/d1-debate`. Claude can integrate the proposal into the central project documentation and append the accompanying record entry.

## Central question

**Can independently measured endpoint dynamics and population turnover jointly explain the ladder's shape, its stability, and who moves through it?**

The variance–slope relationship should become a constraint on the model, rather than another statistic it is tuned to reproduce. The proposed scientific progression is from conditional movement, to an independently predicted distribution, to identification of the social processes that produce those dynamics. Success at one level does not establish the next.

The three measurement needs are taken as the starting requirements:

- independently measured signed drift and population flow;
- a mapping from persistent named-endpoint characteristics to rank occupancy;
- relative movement and observation noise, including covariance, seasonality, and the distinction between transient and lasting variation.

The next phase should combine these into a single testable account rather than three disconnected measurement projects.

## 1. Test stationary balance with independently measured components

Estimate variance, signed drift, occupancy, and entry/exit on a common population and time scale. Declare the latent quantity, centering convention, observation process, population boundary, and information used to define permanent-rank cohorts. Compare their independently predicted contributions with the observed local slope.

The central output is a rank-by-rank discrepancy: **how much structural stability remains unexplained after accounting for measured movement and turnover?** Examine this discrepancy across calendar windows and nested population boundaries. A discrepancy throughout the distribution suggests different problems from one confined to the head or observation boundary.

The stationary identity and its flow correction are derived, with assumptions and literature tags, in [THEORY.md](variance_structure/THEORY.md). In the zero-current case, the local slope depends on effective variance and cumulative restoring drift. The constant-g relation is a special case; it must not be assumed merely because the empirical ratio is approximately flat.

**Deliverable:** a common estimand specification and a balance-discrepancy exhibit with uncertainty and sensitivity to population boundaries.

**Decision:** if components cannot be separately identified, state which combinations are identified and what additional observations would resolve the ambiguity. Do not turn an implied drift read from the ladder into an independently measured force.

## 2. Validate the new diagnostic against competing mechanisms

Extend the existing recovery work specifically for this balance test. Do not repeat the entire previous recovery program without a new question. Construct rival processes that can resemble one another in variance and ladder shape but differ in their causes: stabilization through renewal, state-dependent drift, or persistent endpoint effects.

Determine whether the proposed measurements distinguish these processes at the actual panel lengths, with the project's observation noise, seasonal behavior, cohort selection, and missingness. Measure false identification as well as successful recovery. Write the competing predictions, precision targets, and stopping rules before execution; process empirical and planted panels through the same diagnostic definitions.

**Deliverable:** an identification and recovery map for the proposed diagnostic, including cases where mechanisms cannot be distinguished.

**Decision:** if the diagnostic cannot separate the rivals, improve the observation design or narrow the claim before adding model complexity. A successful static reconstruction alone cannot select a mechanism. This package proposes future recovery experiments; none is authorized or executed by saving this agenda.

## 3. Predict structure without calibrating to the target ladder

Freeze the measured movement and turnover inputs, then predict an unused window's local slopes and normalized rank-size shape. Declare population selection, rank labels, normalization, and all calibration information. A later slice of development data can provide an exploratory temporal test, but cannot become fresh confirmation evidence.

Begin with **relative structure**—gaps and concentration. Predicting the absolute volume of platform activity requires an additional aggregate-growth model. Do not supply the future ladder's bottom value as an anchor. A relative-shape score may remove a location from each predicted and observed curve separately; it must not feed an observed future level into the prediction.

Use identical rank weighting, horizons, universes, and available information for every comparator. Include carrying the previous shape forward, a power law, and a modest flexible description. Retain the current model as a conditional benchmark and state the information it receives. Correct evaluator discrepancies before comparing candidates; do not combine an evaluator change and a benefiting candidate into one adjudication.

**Deliverable:** a forward structural prediction with a calibration-information ledger, matched comparisons, and uncertainty. Include failures across windows and rank regions.

**Decision:** does independently measured movement constrain unused structure substantially, or does the relationship depend on recalibrating the ladder every time? Do not interpret the two algebraic directions of the variance–slope relation as independent tests.

## 4. Build the smallest supported named-endpoint process

The starting candidate should combine measured diffusion, durable endpoint characteristics, and measured population turnover. Additional restoring forces, jumps, or changing volatility should enter only where a discriminating measurement requires them. A fixed personal home is a hypothesis requiring evidence, not a default consequence of persistent endpoint heterogeneity.

Assess the same process jointly on:

- **Collective outcomes:** slope, concentration, gaps, and structural change.
- **Named outcomes:** displacement, residence times, crossings, departures, and returns.

A process that generates the right hierarchy but gives the wrong identities its positions is incomplete. Good endpoint forecasts conditional on supplied positions likewise do not explain the hierarchy. Distinguish a crossing into the buffer, temporary non-observation, re-entry, and genuine population replacement.

Count the complexity of variance profiles, endpoint effects, boundary rules, and any empirical targets—not only latent components. Compare simpler alternatives after refitting them where fitting is appropriate, and count all explored variants. Keep the frozen implementation as the reference; any implementation and adoption follow the existing project rules and decision rights.

**Deliverable:** a bounded candidate comparison covering structure, named movement, and complexity, with each added mechanism tied to an identifying measurement.

**Decision:** retain the least complex adequately supported process. No new numerical adoption margin is established here, and supplementary mechanism diagnostics do not replace the existing gate.

## 5. Test adjustment and change, not only stationary resemblance

Once inputs are independently identified, ask whether measured changes in volatility, drift, or turnover predict changes in concentration and their timing. Specify prospective directional predictions and comparison windows before examining the structural response. Where inputs move together, report that the individual effects are not separated.

Test whether the proposed process approaches similar structure from substantially different initial distributions, without an empirical target hidden in its parameters. Keeping a fixed distribution of measured homes while perturbing initial states tests conditional attraction, not generation of that home distribution.

Derive or measure adjustment time from the actual process. Do not substitute 1/g. A model might explain an equilibrium yet adjust too slowly to explain observed changes. Alternatively, the data may support a slowly changing hierarchy rather than strict stationarity. These are distinct scientific results, and both stationary and transient predictions should be stated accordingly.

**Deliverable:** structural-response predictions, initial-condition sensitivity, and an adjustment-time analysis with uncertainty and explicit dependence on boundaries and endpoint heterogeneity.

**Decision:** distinguish an equilibrium explanation, a finite-horizon dynamic explanation, and failure of either. Do not infer that the observed ladder is inherited solely from a numerical reciprocal of an estimated drift ratio.

## 6. Identify a social mechanism, then confirm transport

Ask what produces the measured movement: content supply, engagement per item, audience persistence, competition, or platform exposure. Prioritize the supply-versus-engagement decomposition and look for independently documented changes that separate these explanations.

An observational decomposition can locate where movement occurs; it does not alone identify its cause. A credible external change can provide stronger evidence when the design addresses competing changes, anticipatory behavior, and observation artifacts. This is a research target, not an assertion that the existing panels contain a suitable natural experiment.

Only after the model and discriminating predictions are fixed should reserved data test transport. Separate unchanged-parameter transport, narrowly specified recalibration, and transport of the mechanism. State which future evidence stays reserved for later generations. New data contact remains subject to the existing protocol and owner authorization.

**Deliverable:** a mechanism-specific empirical design and, when development warrants it, a frozen transport protocol.

**Decision:** determine whether the contribution supports a distributional process explanation, a named social mechanism, or a more limited conditional account. Model agreement cannot supply missing empirical identification.

## Mathematical work alongside the sequence

The first proof priorities are:

1. The bridge from noisy, finite-window permanent-rank estimates to the effective diffusion and drift coefficients required by stationary balance.
2. The effect of renewal, population boundaries, and persistent endpoint types on that balance and on stability.
3. Error bounds for structural reconstruction and adjustment predictions, including weakly identified cumulative drift.

The basic stationary relationship is already established mathematics under suitable conditions. The needed work is to establish when those conditions and coefficient interpretations apply here, and how large deviations can be. The [theory note](variance_structure/THEORY.md) distinguishes known results, elementary derivations, and proposed problems whose novelty or present open status has not been established. A general new theorem need not precede the decisive empirical tests.

## Immediate next deliverable and stopping point

Write one focused **measurement-and-recovery protocol for steps 1–2**. It should contain the common estimands, calibration-information restrictions, competing mechanisms, falsifiable predictions, uncertainty calculation, recovery design, and outcomes that would stop or redirect the work. Review that concrete protocol before any new execution.

The first decision is whether step 3 is identified and feasible with the available development observations. If not, the result should be an explicit additional-observation requirement or a narrower claim, rather than another flexible model fitted to the same ladder.

The potential PNAS contribution is the joint explanation of persistent structure and changing identities—and, if the evidence permits it, a measured social process explaining changes in that balance. This proposal changes no binding manuscript language.

## Evidence and relationship to the existing program

This proposal develops the signed [D1 position](CODEX_SIGNOFF.md) and the subsequent [independent empirical comparison](variance_structure/REPORT.md). The [A10 review](variance_structure/A10_REVIEW.md) must accompany interpretation of the temporal comparison: the corrected common-grid comparison replaces the original mixed-weighting interpretation, and 1/g has not been established as a relaxation time.

Background read for the recommendation: the two October 8 research agendas, the October 12 adopted work plan, PROGRAM_STATE, MODEL_STATUS through the relevant A9/A10 entries (§2z-bf and §2z-bh), the signed debate consensus on main, the stochastic-modeling skill and evidence standards, and the tagged literature in the theory note. The proposal builds on completed diagnostic and recovery work rather than treating those earlier plans as unexecuted.

**Documentation provenance:** OpenAI Codex; 2026-10-12; starting commit `6c99858aab71e7e8bde5338b6c59c70f72481725`. Documentation only. No new measurements, scored predictions, fits, simulations, gate evaluations, or data contact. No change to the adopted agenda, frozen defaults, protocols, thresholds, or paper claims.
