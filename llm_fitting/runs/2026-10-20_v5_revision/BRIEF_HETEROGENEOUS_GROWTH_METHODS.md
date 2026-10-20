# Brief: standard methods for heterogeneous mean growth (for analysis R12)

The owner has approved a new analysis. It adds a calibrated world in which communities differ in their mean growth to the paper's comparison set. The owner asks that the methodological choices be "as standard as possible in the literature, with multiple references to model papers demonstrating that we're making similar choices to established work."

Your job is to research those standards and recommend a specification. Do not run anything.

## Our setting (read `paper/pnas_draft.tex` Results 4–5 and `paper/pnas_si.tex`, sections on persistence and the simulated worlds)

**Data.** Weekly log activity (karma) of Reddit communities: top 5,000 by submissions over 212 weeks, and top 12,500 by comments over about 213 weeks (2018–22). Activity is ranked by permanent (time-averaged) rank, with absence-penalized membership in a top-K universe.

**The model's lasting core.** Each community has a lasting level H_i(t+1) = H_i(t) + g(r_i(t)) + σ(H_i) η_i(t), where:
- g is a drift by rank (the measured "slide");
- σ(H) = σ0 e^{−β(H−H0)}.

Observed log activity is H plus passing components:
- a fast AR(1) "swing";
- reverting deviations;
- observation noise.

Communities exit at rank-dependent rates and are reborn at the bottom. Mean growth is the same for all communities at a given rank (homogeneous).

**The statistic the homogeneous worlds miss.** It is the rank correlation between changes over windows that share no weeks: the change between two 13-week windows a year apart, against the same change starting 13 or 26 weeks after the first ends.
- Data: +0.13 to +0.20.
- Every homogeneous simulated world: −0.02 to −0.10.
- A synthetic world with a spread of mean growth of 0.4 a year gives +0.17 to +0.19.

The lag profile:

| gap (weeks) | 0 | 26 | 52 | shape |
|---|---|---|---|---|
| submissions | +0.194 | +0.150 | +0.046 | decays |
| comments | +0.135 | +0.136 | +0.108 | flat |

On submissions, the persistence disappears once each community's loading on the platform's total activity is removed. On comments it does not. Prior-year decliners carry 40–60% of the "slide's" excess over random growth.

**Concern G15.** Heterogeneous drift makes the variance of h-week changes grow as σ²h + σ_b²h². This may bias:
- our lasting-variance estimates (a "two-spacing difference" structure function);
- the size exponent β.

## Questions

1. **The standard specifications** for heterogeneous mean growth in each of these traditions, with the key papers and the exact specification each uses:
   - **Income dynamics:** HIP against RIP. For example Lillard & Weiss 1979, Hause 1980, MaCurdy 1982, Abowd & Card 1989, Baker 1997, Haider 2001, Guvenen 2009, Hryshko 2012, Guvenen et al. 2021.
   - **Firm dynamics:** for example Luttmer 2011 and Pugsley, Sedláček & Sterk 2021 (ex-ante heterogeneity in growth profiles), and others you find.
   - **Inequality dynamics:** GLLM 2016 (high- and low-growth types with switching) and Gabaix 2009.
   - **Rank-based models and stochastic portfolio theory:** Fernholz, Ichiba & Karatzas 2013 (second-order model); the hybrid Atlas model of Ichiba et al. 2011.
   - **Ecology:** environmental stochasticity and correlated fitness, for example Kalyuzhny et al. 2015, Mallmin, Traulsen & De Monte 2026 (OU fitness), and Grilli 2020.
   - **Finance:** momentum and factor exposure, as a form of persistence.
2. **The distribution of the heterogeneous component.** Normal, two types, lognormal or something else? What is the standard, and why? Is there a convention for its correlation with initial level (in HIP, the α–β covariance)?
3. **Persistence of the heterogeneity.** Fixed (HIP), AR(1)/OU growth rate, or Markov switching? What is standard, and how do papers choose among them?
4. **Estimation.**
   - The standard estimator of the spread of mean growth: minimum distance on the autocovariance function of growth, equally weighted or optimally weighted (Altonji & Segal 1996), or likelihood or indirect inference.
   - How are transitory MA/AR components and measurement error handled jointly?
   - What can a short panel (four years of weekly data) identify, and what can it not?
   - What are the known power problems?
5. **Selection.** We study a top-K universe whose membership changes. How do the firm and income literatures handle attrition or selection when estimating growth heterogeneity, for example survivor bias in firm growth?
6. **Calibration of a simulated heterogeneous world.** How do published papers calibrate such a world (which moments, which windows), and how do they validate it (recovery from simulated data)?
7. **Recommendation for R12.** The most standard specification for each of three variants:
   - (a) fixed heterogeneity;
   - (b) persistent but decaying heterogeneity;
   - (c) heterogeneous exposure to a common factor.

   For each give:
   - the estimator for its parameters from our panels;
   - the moments it should be calibrated to, using no statistic we then test it on;
   - what it should and should not change: by Fernholz 2017, the stationary shape depends only on rank-specific volatility and reversion;
   - pitfalls, including identification in four years and interaction with the passing components and the top-K selection.

## Standards

- Give a DOI or stable URL for every work.
- Label access: [full text], [abstract only], [secondary] or [memory, unverified].
- For the key specification of each tradition, give the equation and the page or section where it appears.
- Keep checked facts separate from proposals.
- Do not modify repository files.
- Do not open raw data or anything in `llm_fitting/SEALED_PATHS.json`.

## Deliverable

A methods memo of about 3,000–5,000 words with:
- a table of traditions × specification × distribution × persistence × estimator × key references;
- your recommended R12 specification;
- a reference list.
