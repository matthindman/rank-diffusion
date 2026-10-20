_Round-8 review by gpt-6-astra (OpenAI) through the Codex CLI (codex-cli 0.163.0), read-only sandbox, run 2026-10-21 on manuscript v7 (commit 1652645) with `CRITIQUE_PROMPT_R8.md`. Saved verbatim by the orchestrating session (Claude Opus 5.5)._

## 1. Editor’s decision now

**Send out, without enthusiasm.** The paper combines useful rank-based accounting, an informative geometry control, and a potentially important asymmetry in community growth. R16 improves its credibility by withdrawing the overly broad rejection of heterogeneous growth and the claim that durability requires slow adjustment. However, the revised discovery lacks a consistent statistical presentation, while several conclusions about capture, model fit, and response times still exceed their designs. The single change most likely to raise enthusiasm is a central figure establishing **greater persistence after declines than after gains**, with one definition, empirical uncertainty, selection checks, and identically evaluated model comparisons.

## 2. The paper after R16

The paper is scientifically stronger and narratively less settled. R16 has removed two attractive explanations that the evidence did not sustain. That is progress, but the manuscript still devotes most of its opening and visual space to the earlier durability argument.

The defensible contribution now has three parts:

- A measurement framework connects changes in the hierarchy to incumbent movement, crossings, and composition.
- Controlled simulations show that the starting ladder’s geometry changes the turnover profile.
- Reddit exhibits asymmetric persistence that the fitted symmetric specifications miss.

The first is an empirical application of established theory; the second is a useful warning against interpreting turnover without controlling for geometry; the third offers the best prospect of a discovery. None presently identifies the cause of the head’s decline.

The short seller’s case is straightforward: the balance relation is substantially accounting, overall persistence is calibrated, the supposed slow transition has disappeared, and asymmetric dynamics already exist in adjacent literatures. The answer must be a particularly clear and robust empirical result, rather than another poorly fitting specification.

I would propose **“Persistent decline and changing leadership in online attention”**, subject to the owner’s decision. If the existing title remains, define “outlasts” descriptively: it must not imply a demonstrated separation of adjustment times.

The abstract should introduce asymmetric persistence earlier, distinguish it from the historical fall in concentration, and describe the balance as an accounting framework. Its final assertion that fixed differences “make gains persist as much as declines” is inaccurate: the simulated conditional correlations are generally *reversed*, with gains more persistent.

Source references below use [main-text TeX](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex) and [SI TeX](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex) line numbers. The numerical record is [MODEL_STATUS §2z-dp–dq](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md:13589).

## 3. R16’s designs and readings

**A. Reversible decline state.** A centered two-state growth process is a reasonable, standard candidate. Reporting all three occupancy profiles and preserving the failed predictions at \(\pi_D=0.1\) are good practice. The improvement at \(\pi_D=0.25\) supports the narrow statement that this specification can produce the asymmetry’s sign.

It does not identify a four-year decline mechanism. Spread and correlation half-life reach their upper grid limits, and a four-year state-correlation half-life implies approximately **7.7 years of expected residence in decline** at \(\pi_D=0.25\), absent rebirth. That exceeds the observation window substantially. Duration and spread therefore need profiling, not interpretation as measured behavioral quantities.

The 15-moment objective is also not formally calibrated by C’s nine-moment reference distribution. The misfit is large, but “decisively rejected” needs a reference distribution for this procedure. Ten assessment seeds provide useful diagnostics, not strong head-level inference. Most importantly, the manuscript understates this model’s persistence through inconsistent statistic choices, detailed below.

**B. Joint re-estimation.** Jointly estimating persistent and transitory components is the right correction to R12. The substantive finding is secure: freezing the passing components made the earlier rejection too strong. The selected submissions specification improves the objective to **38.8 over 24 moments**, versus the earlier restricted fit’s large discrepancy.

The interpretation “fits on submissions” is not yet established. An objective of 8 on the old nine moments falls within C’s reference range, but that range belongs to another fitting procedure, parameterization, and generating truth. It does not validate the new 24-moment optimum. Comments remain substantially discrepant.

Both optima put the swing multiplier at 0.5 and the head multiplier at 0.25; submissions also put the body spread at its upper limit, 0.8. These are provisional grid optima. Extend or profile the edges, include a jointly refitted zero-heterogeneity comparator, and calibrate the full objective before interpreting parameter magnitudes.

The model also buys persistence at a cost: submissions top-ten turnover falls to **0.18 versus 0.33**, and its boundary ratio rises to **1.22 versus 0.80**. This is partial explanatory success, not overall adequacy.

**C. Recovery and objective calibration.** These are valuable additions. Independent pseudo-data seeds and the disclosed reuse of the saved simulation grid make the exercise interpretable. The enormous separation between R12’s observed objectives and its correct-specification pseudo-data distribution supports rejection of that restricted specification.

The experiment also correctly exposes weak identification between fixed and slowly decaying growth. But the 26-week-swing experiment establishes only that this particular misspecification does not generate spurious fixed heterogeneity; it is not a general safeguard against transitory-component misspecification. Twenty pseudo-panels give a rough upper quantile. Reusing one simulated grid makes the inference conditional on that grid’s Monte Carlo error.

**D. Size-exponent recovery.** Running the actual estimator, including re-estimation of passing components and universe selection, repairs the previous mismatch. I independently recovered the reported means from the saved results: comments **0.1084 versus truth 0.126**, and submissions **0.0628 versus truth 0.045**, with **0.0610 under truth zero**.

The submissions conclusion should be “these two truths are poorly distinguished,” not “size offers no protection.” Comments’ recovery is encouraging under the homogeneous generating model; it does not establish robustness under the heterogeneous dynamics now central to the paper. Nor does this two-truth experiment test discrimination from a firm-like exponent of 0.2. The raw quantile elasticities remain descriptive and should not be equated with latent volatility exponents.

**E. Capture audit.** Boundary/placebo comparisons, item-versus-karma contrasts, and collapse exclusions are worthwhile diagnostics. They argue against a large, abrupt head-relative loss aligned with the July 2021 file boundary. They cannot exclude smooth missingness, delayed deletion, differential score maturation, changing collection processes within files, or errors shared by numerator and denominator.

Two readings are too strong. Summing 14-day before/after contrasts does not partition the total decline, so a positive sum cannot establish that “none” occurred at boundaries. And observing a small within-month pattern does not prove it is capture or establish power against the particular artifact being excluded.

There is also a verified exclusion bug: mid-month placebos inherit the first-of-month eligibility filter instead of being checked themselves. December 15, 2020 and December 15, 2021 cross roster boundaries. Removing those saved contrasts leaves the narrow comparison intact: boundary/placebo mean-absolute-step ratios remain approximately **0.69–1.09**. Correct the implementation and report that sensitivity. The 2K check is uninformative because it retains the entire evaluated cohort.

**F. Durability response.** Paired step/control simulations in the actual curved model are more relevant than the earlier single-slope approximation. Disclosing the registered fit’s failure is essential and correctly done.

The lag-aware replacement is a reasonable exploratory description, not a registered validation of the time constant. It compresses a heterogeneous filter into one lag and estimates relaxation from five annual block averages. Report the response curves and paired uncertainty before emphasizing the fitted ratio.

Membership “e-folding” is itself an exponential-equivalent transformation of one-year retention, although the paper shows that retention is not exponential. Comparing it with a fitted response constant is therefore approximate. The main conclusion should be: **this calibrated homogeneous model provides no evidence for a large separation of the two characteristic times**.

The assertion that crossings offset three fifths of the shock is unsupported by this experiment alone: it observes filtered, reordered shape, not a decomposition of the response into crossings, lag, drift, and entry. Moreover, its last observation averages year five rather than measuring the endpoint after five years.

**G. Combined model.** This is a disclosed, useful post hoc addition test. It is not a joint test of whether the two mechanisms can coexist. Holding B’s optimum fixed while adding another persistent component predictably risks overshooting moments already fitted by B. The result supports “this addition failed at the fixed optimum,” not “the mechanisms do not combine.”

Re-estimate both spreads and passing components together on the existing symmetric moments first. The asymmetry can remain untargeted in that exercise; targeting asymmetry is not necessary merely to free the parameters.

These assessments follow the [registered designs](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/PLAN.md:990), [R16 results](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/RESULTS.md:1367), and [implementation](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/r16_tier3.py).

## 4. Each Results section

**“Reddit’s leading communities turn over while the shape … changes far less.”** The descriptive numbers are now substantially improved. The vulnerable sentence is the comparison itself: percentage membership replacement and percentage change in a mean-log span are different quantities, without a natural common scale. Show both; do not treat their numerical ordering as a dimensionless durability test. The comments head also declined another 0.15 log units in 2020–21, so “held in 2020–22” needs qualification.

**“Measured rank by rank, random growth shows when …”** The accounting and geometry control support a useful benchmark. The first attack will be that locating the change in the measured slide is largely automatic, while the crossing statistic contains composition and its variance estimate can contain heterogeneous growth. The section acknowledges these problems but still occasionally speaks as though the measured terms uniquely identify diffusion.

**“Built from the measured ingredients, a model forms a hierarchy …”** Supported as a conditional model experiment. The vulnerable claim is that forming a hierarchy establishes an explanation of the observed hierarchy: the model underproduces the head and inherits empirical drift and variance profiles. Distinguish this slide-calibrated construction from the separate model whose drift is solved to hold an input ladder.

**“With stable parameters, random growth forecasts neither head’s fall.”** Supported for the named homogeneous forecasting specification. It is not a rejection of random growth generally. The first attack will target the drift chosen to hold the training ladder and the subsequent capture-exclusion language.

**“On Reddit, declines persist and gains do not.”** This is the strongest candidate result and the weakest-supported absolute heading. The matched calibration statistic gives positive riser correlations of approximately **0.069 and 0.042**, and no empirical equivalence test establishes zero. Rename it **“On Reddit, declines are more persistent than gains.”**

## 5. The asymmetry as the discovery

The evidence is suggestive and deserves prominence. Disjoint windows remove the familiar adjacent-block averaging explanation; the sign reversal relative to evaluated symmetric models is informative; and conditional follow-up changes provide a second description beyond correlation.

The robustness package is incomplete in five respects.

First, the paper alternates among pooled-cohort correlations, an origin-averaged statistic using whole-window membership, and stratum-demeaned tail transitions. These are different estimands. The differences materially alter how much persistence the decline model reproduces.

Second, conditioning on the first change’s sign restricts its range. Different conditional correlations can reflect different dispersion, rank composition, nonlinear mean reversion, or mixtures as well as different persistence. Show conditional response curves across initial-change quantiles, within pre-period rank bands, with uncertainty.

Third, whole-window permanent rank uses subsequent outcomes. Applying identical selection to simulations makes comparisons fair conditional on that selection; it does not eliminate selection bias in the substantive interpretation. The requirement of presence in 90% of each measurement window also makes these survivor-selected comparisons.

Fourth, capture and collapse checks principally assess overall persistence and concentration, not the asymmetry itself. Carry those checks through to the actual headline statistic.

Fifth, six overlapping origins are not six independent replications. Thousands of communities do not solve uncertainty from shared calendar shocks. Report origin-specific effects and inference that respects community and calendar dependence.

The broad phenomenon is not new. Guvenen and colleagues explicitly report persistent negative and transitory positive changes among high earners, with the opposite pattern among low earners. The opportunity here is a domain-specific result and its relationship to rank dynamics, not discovery of asymmetric persistence as such. [Guvenen et al. (2021)](https://onlinelibrary.wiley.com/doi/abs/10.3982/ECTA14603)

Online-community research also already documents growth, stabilization, and decline across many communities. TeBlunthuis and colleagues’ comparison is particularly relevant because communities reach those stages at different calendar dates, helping distinguish life cycles from common temporal shocks. [Authors’ account of the 2018 study](https://blog.communitydata.science/revisiting-the-rise-and-decline/)

A convincing contribution would establish the asymmetric conditional response within rank and observable life-cycle strata, demonstrate robustness to calendar episodes and survival rules, and quantify its association with concentration change. The manuscript correctly admits that it has **not shown that the persistently declining communities drove the head’s fall**. Preserve that admission.

## 6. Round-7 points

Using the numbering in [SYNTHESIS_R7](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-20_external_critiques_round7/SYNTHESIS_R7.md):

| Status | Points | Assessment |
|---|---|---|
| Resolved in their original substance | **G2–G5, G8–G12, G14–G16, G18, G20–G21, G29, G31–G32** | The table values, world labels, missing entry term, mathematical wording, calibration descriptions, reversed asymmetry, matched eligibility, and forecasting qualifications are substantially corrected. |
| Half-resolved | **G1, G6–G7, G13, G17, G19, G22–G25, G27–G28, G30, G33** | Paths are corrected but durability prose remains broad; the variance caveat is commented out; new response claims need repair; A1 uses a disclosed substitute; selection and retrospective labels remain inconsistent; joint fitting is provisional; factor exclusion remains overstated; uncertainty is incomplete; turnover ratios still do not uniquely explain the mechanism; ten-year graphics retain mixed horizon cues. |
| Still open | **G26** | The SI still asserts that the extension establishes a minimum, contradicting its own earlier caveat. |

For the Pro addendum:

- **Resolved:** **P2–P5, P9, P11–P12**, concerning broad time-scale wording, explicit numerical corrections, observed versus primitive symmetry, uses, and the definition of homogeneous.
- **Half-resolved:** **P1, P6–P8, P10**. The variance implication is not fully visible in the compiled SI; annual-universe completeness remains unchecked; turnover ratios remain mechanistically ambiguous; capture is only partially audited; statistic differences are disclosed but still cause incorrect comparisons.
- **Open:** **P13**. Results 2 remains overloaded, and the Discussion still contains a response-to-reviewers paragraph.

## 7. Errors

The following require correction independently of the larger interpretive questions.

1. **Decline-state persistence is understated.** Main line 205 and SI line 425 say “a fifth … or less.” From the saved R16 world outputs, the paper’s statistic is **0.075/0.174 = 43%** on submissions and **0.049/0.154 = 32%** on comments. The auxiliary statistic gives **5% and 26%**, so even that definition does not support the universal one-fifth ceiling. Table 1’s 0.08 is consistent with the first definition. The [figure code explicitly reads that definition](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/make_v5_figures.py:320).

2. **A variance multiplier is actually a standard-deviation multiplier.** Main lines 155–160 and SI line 150 describe multipliers 0.7–1.4 as applying to lasting variance. [The parameter code multiplies `sigma0`](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/r12_hetgrowth.py:185), giving variance multipliers **0.49–1.96**.

3. **R16-F3’s reported variation is wrong.** RESULTS line 1597 says less than ±12% under either fit. Saved lag-aware ratios on comments are **1.00, 0.80, 0.69**: relative to the central value, **+25% and −14%**. The registered ±30% criterion still passes; the descriptive sentence does not.

4. **The capture-placebo exclusion is implemented incorrectly.** [Lines 104–110](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/r16_capture.py:104) filter first-of-month dates before constructing mid-month dates. The two contaminated placebos identified above violate PLAN line 1105.

5. **The corrected variance caveat is absent from the PDF.** SI line 162 begins with `%%`, commenting out the entire paragraph explaining how variance inflation changes movement time, balanced slope, and relaxation time.

6. **The grid-minimum contradiction survives.** SI line 438 says the extension establishes the minimum; line 369 correctly states that the comments optimum’s multiplier was omitted and the submissions difference is within Monte Carlo noise.

7. **Table 1 mislabels both targeting and replication.** Its caption says every world has 20 runs, whereas the decline column has 10. Its note calls fallers-minus-risers “the calibration statistic,” although asymmetry is outside the objective. Distinguish “auxiliary statistic computed by the calibration code” from “targeted moment.”

8. **The SI’s universal cohort statement remains false.** Line 29 says cohorts are fixed on disjoint blocks without the exceptions correctly listed in main line 253. The asymmetry code pools eligible communities after whole-window selection; it does not calculate a separate within-stratum correlation.

9. **Platform exposure is not excluded.** Main line 237 says persistence “does not reflect exposure to platform-wide trends.” R15 establishes that noisy estimated loadings can manufacture the apparent removal. When shrinkage collapses loadings toward a common value, the adjustment has little ability to exclude heterogeneous exposure.

The lag-aware fit also needs a deposited executable specification. I found its saved scores and plotting reader, but no fitting routine in the reviewed R16 scripts.

## 8. Writing and structure

Results 2 currently combines accounting, measurement contamination, turnover geometry, size scaling, historical change, and response times. Split it into **balance and observed change**, **geometry and turnover**, and a short **conditional response experiment**. Move the size-exponent detail to the SI.

Give the asymmetry its own main figure. Figure 4C currently displays overall persistence, much of which is fitted, while 4D displays the older five-run prior-decliner exercise. Neither directly displays the revised headline discovery. Replace one with conditional follow-up curves and the other with the matched faller/riser contrast.

The figures are generally legible, but Figure 1D’s labels crowd each other. Figure 2A foregrounds the old approximation with continuous lines while reducing the new result to stars. Figure 3 shades years 0–5 in A and years 5–10 in B; use one horizon convention. Figure 4C wastes substantial vertical space above the observations.

Table 1 is improved at full width but remains dense. Separate targeted moments from untargeted checks and replace “in/above/below” codes with values and intervals for a smaller set of decisive comparisons. In the compiled SI, the joint-fit and provenance tables are extremely small. Avoid shrinking paragraph-length cells with `resizebox`.

Three concrete replacements:

**Main line 199, joint fit:**

> Allowing persistent growth differences and the transitory component to vary together substantially improves the fit. The selected specification reproduces most aggregate persistence, although its optimum lies at the search boundary and its fit across all 24 moments has not been calibrated. It still gives greater persistence after gains than after declines, opposite to the data.

**Main line 189, capture:**

> The daily panels show no unusually large head-relative discontinuity at the July 2021 file boundary. Boundary contrasts are comparable to mid-month contrasts. This weakens an abrupt file-boundary explanation, but does not exclude gradual or community-specific capture changes.

**Main line 155, response times:**

> In the calibrated homogeneous model, an increase in the head’s decline produces a delayed change in observed shape. A post hoc lag-aware fit gives a response time comparable to an exponential-equivalent membership time. This experiment does not support a large separation of time scales, although parameter uncertainty and alternative response specifications remain untested.

Organize the SI by estimand and final method, placing development history and expectation scoring in a compact deviations table. Retain the openly cited 2018 chapter and state the intellectual continuation directly.

## 9. The five most valuable changes before submission

1. **Establish the asymmetry with one primary estimand.** Add empirical uncertainty, within-rank conditional response curves, origin-level reporting, and the same selection/capture sensitivities used elsewhere. Test a practically small gain effect rather than equating non-significance with zero.

2. **Resolve the joint model’s identification and fit.** Profile the grid edges, jointly refit the homogeneous comparator, and calibrate the 24-moment objective. Re-estimate the combined model without initially targeting asymmetry.

3. **Repair the response-time evidence.** Deposit the fitter; lead with paired response curves; distinguish block averages, latent response, and observed response; propagate uncertainty; withdraw the crossings-only “three fifths” attribution unless directly decomposed.

4. **Narrow and repair the capture argument.** Correct the placebo eligibility bug, show dated contrasts with uncertainty, and remove claims that the contrast sum decomposes the decline or that the within-file pattern proves adequate power.

5. **Rebuild the presentation around the revised contribution.** Correct the errors above, replace the persistence figure with an asymmetry figure, shorten Results 2, distinguish the different calibrated models, and make all SI tables readable.

These changes use the existing development data and saved outputs, although several require future authorized computations. The owner must settle the title, submission framing, classification, provenance details, frozen archive/DOI, and any protocol amendments. A raw-archive capture audit requires the relevant drive; genuine confirmation and separation from shared calendar episodes require an adopted protocol and explicitly authorized new data.

This review was read-only: no repository files were changed, no data panels or sealed folders were opened, and no model, simulation, estimation, or test suite was run.