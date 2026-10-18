_Round-3 review by gpt-6-astra (OpenAI) through the Codex CLI 0.163.0 (`codex exec -m gpt-6-astra -s read-only`), 2026-10-18, from `CRITIQUE_PROMPT_R3.md` (committed 69bc82e before the review). Written by the CLI; header added by the orchestrating session. Model output: checked in `SYNTHESIS_R3.md`._

1. **Editor’s decision now: desk reject, with encouragement to resubmit.** The manuscript has an accessible puzzle, substantial measurements, and a potentially valuable method for separating lasting movement from transitory fluctuations. The present submission, however, repeatedly turns accounting relationships and conditional simulations into claims that turnover causally maintains the observed hierarchy. Its model evidence combines different specifications, and the arithmetic-concentration analysis contains a consequential theoretical error. The single change that would most improve its chances is to distinguish clearly what the empirical balance establishes from what one consistently specified generative model actually predicts.

2. **The title and central claim.**

The weakest word is **“sustains.”** “Continual turnover” is supported, and “slowly changing” is defensible if the paper quantifies the comparison of timescales. “Sustains” asserts a mechanism and a counterfactual: without turnover, this hierarchy would disappear or substantially weaken. Neither the accounting decomposition nor the simulations establish that claim for the observed system.

Three quantities currently travel under “churn”: replacement of top-set members, finite-horizon downward movement, and lasting innovation variance. They are related but distinct. Replacement counts do not measure the activity gained through replacement; annual movement includes passing components; and the estimated lasting variance can include heterogeneous persistent drift. The title depends on bridging these distinctions.

The title study provides useful stylistic examples, but selecting highly cited published papers cannot identify which titles increase acceptance or citations. Its assertion that “sustains” is simply a measured result repeats the manuscript’s central inferential problem.

My preferred title now is **“Rapid turnover accompanies a slowly changing hierarchy of online attention.”** If the owner requires a mechanism in the title, **“How turnover can sustain a slowly changing hierarchy of online attention”** honestly frames the model as a demonstration of possibility. Keep creation from equality in the model discussion, explicitly conditional on its drift and replacement rules.

3. **The five Results sections.**

**(1) The puzzle: substantially supported, but the strongest wording needs qualification.**

The contrast between changing membership and persistent distributional shape is real and visually effective. Figure 1 is a good opening. Nevertheless, “its shape moves little” needs a numerical definition: the submissions head’s arithmetic share falls from 50.2% to 34.0%, a substantial proportional change, even while the broader curve remains recognizable.

The reported 27–33% top-ten replacement rates are averages across annual transitions; individual transitions replace 20–40%. Say “on average.” Likewise, “every top set” means the tested cutoffs, not every possible cutoff. The abstract places Facebook within an annual-turnover sentence although its turnover comparison uses half-years.

The first referee attack will be **whether the slow hierarchy is a measured timescale separation or a visual impression**. Report a normalized measure of shape change beside membership turnover, with uncertainty. Also state throughout that arithmetic shares are within the recorded top-\(K\), and that each year’s leading set can contain different communities.

**(2) Scale-free churn and turnover: useful conditional kinematics, overstated as explanation.**

The comments and Facebook size–variance results are reasonably supported; the submissions instability is now disclosed. The firm comparison remains imperfect: annual changes in block-mean weekly log activity are not automatically the same estimand as firms’ annual log growth. “Nearly size-independent” is more accurate than an unqualified “scale-free.”

Equation 1 has a valid derivation under its assumptions. But [the implementation](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/turnover_kinematics.py:34) uses the completed year’s boundary displacement, completed year’s community changes, and observed disappearances. Thus it is a contemporaneous reconstruction, not an out-of-time prediction or a discriminating mechanism test.

Its nontrivial assumption is that changes measured among surviving names initially ranked \(k/2\) through \(k\) represent changes throughout the region capable of crossing. Local log-linearity alone does not establish this independence from initial distance. Moreover, the 48 cells begin at ranks 78, 195, and 54—not at the top ten whose exceptional turnover motivates the section.

The first attack will be **that a post hoc, outcome-conditioned reconstruction is carrying a causal interpretation**. The original linear approximation failed; the replacement’s success is worth reporting, but the failure and post hoc status belong beside Figure 2, not principally in the SI.

**(3) The balance: the empirical centerpiece, but its estimand and uncertainty remain unresolved.**

The explicit acknowledgment that the same-year rate law is accounting is welcome. The independent empirical content is the approximation of the boundary term from movement and slope, not the correlation between an accounting-based prediction and shape change.

“Measured crossings match … at every rank” is too strong. The record contains submissions ratios around 0.61–0.72 at intermediate cutoffs, uncertain head estimates, and an out-of-time pooled ratio of approximately 0.62. A pooled ratio near one cannot validate every cutoff.

More fundamentally, the spacing-difference statistic is \(R_{bc}+C\), with composition contributing approximately 25–35% on Reddit. Calling that term “small” is misleading. The theory note also records that heterogeneous drifts make the variance estimator recover \(\sigma^2+3h\operatorname{Var}(\mu_i)\). Results 5 establishes precisely the sort of persistence that makes this distinction consequential. Agreement between these two contaminated quantities does not identify diffusion innovation variance.

The new bootstrap is an improvement in sharing entity weights, but [its code](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/joint_bootstrap.py:87) resamples three overlapping Reddit starts independently, retains the original slope estimates, and leaves the variance estimator’s temporal support fixed. Facebook has only one start. This is not a calibrated joint time-block bootstrap of the full estimator.

The first attack will be **whether the near-unit ratio validates a physical mechanism or mainly a finite-horizon relationship shared by several processes**. Preserve the result, name its estimand accurately, and calibrate it under persistent-drift and common-factor alternatives.

**(4) The generative model: movement adequacy improves; structural validation remains missing.**

The corrected long-panel means reproduce: submissions 0.183 against A’s 0.188, comments 0.130 against 0.123 (§2z-dg, completed results). The original model’s submissions failure, 0.246, remains visible. These are worthwhile improvements in reporting.

But three different objects support the paragraph:

- Flat-start construction uses the lasting core alone, with measured-slide drift.
- Corrected movement gates use `movinghome_vp`, with the ladder-holding closure.
- Out-of-period structure uses the original, variance-deficient `movinghome`.

The [structure runner](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/structure_oop.py:116) confirms the last distinction. No single specification has demonstrated all three advertised properties.

The flat-start slope comparison also uses drift measured inside the same simulation. Agreement with its own stationary balance is a useful implementation check, not independent recovery of empirical structure. The headline flat-start experiment uses ten seeds, below the project’s twenty-seed requirement for head claims, and Figure 4 omits its uncertainty.

The short seller’s case is straightforward: on comments, removing drift improves the corrected gate from 0.130 to 0.100; on submissions, every model mean loses to historical mobility; and the structural model predicts the submissions head near 0.495 when the observed value is 0.356. Those numbers do not establish the mechanism advertised by “sustains.”

The first attack will be **validation assembled across variants**.

**(5) Persistence and the falling head: an important regularity, with unsupported mechanism language.**

The gapped statistic substantially resolves the block-averaging objection. Positive correlations survive disjoint increment supports, and the matched Facebook-horizon analysis appropriately weakens the platform contrast.

However, [the asymmetry code](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/momentum_decomposition.py:153) subtracts each origin’s median change before dividing observations into “risers” and “fallers.” These are above- and below-median performers, not necessarily communities whose activity increased or declined. The manuscript’s literal decline claim is therefore not established by this calculation.

Item counts also do not count participants. Fewer comments can mean fewer commenters, fewer comments per commenter, changes in bots, moderation, or capture. “Communities that lose their commenters keep losing them” exceeds the available measurement.

The aggregate adjustment is informative but not causal identification. Its factor is the mean log activity of the selected cohort, not platform-total activity. Persistence surviving twenty clusters does not establish that it is community-specific.

There is also residual selection: both momentum runners first restrict membership using full-panel permanent rank, then select an early cohort. Disjoint measurement windows do not remove this preceding use of future information. The magnitude of that effect is unmeasured.

Finally, the arithmetic accounting establishes where the loss appears, not why it occurred. Since displacement is nonnegative by construction, a declining top share necessarily entails a negative within term. Connecting that identity to persistent participation decline requires a matched, head-specific analysis.

4. **The model in the main text.**

The core belongs in the main text, but it needs an accurate state definition and an explicit boundary mechanism. A minimal representation of the implemented moving-home process is

\[
H_{t+1}=H_t+g(r(H_t))+\eta_t,
\qquad
Y_{t+1}=Y_t-\kappa_t(Y_t-H_{t+1})+\epsilon_t,
\qquad
X_t=Y_t+\xi_t+e_t.
\]

Here \(H\) is the wandering home, \(Y-H\) is a reverting deviation, and \(\xi\) is the additional fast swing. The manuscript can combine passing components verbally, but it should not describe them as one independent AR(1): the reverting deviation inherits the home innovation.

The **variance correction is algebraically sound under constant coefficients, stationary deviations, independent innovations, and an inactive floor**. For the code’s update ordering,

\[
\operatorname{Var}(\Delta Y)
=\frac{2q+\kappa\sigma_H^2}{2-\kappa},
\]

so matching A requires \(q=\sigma_p^2-\kappa\sigma_H^2/2\). The intermediate variance of \(H-Y\) differs from the lagged-home derivation in PLAN, but the increment-variance result agrees.

“Variance-preserving” needs those qualifications. Rank-varying coefficients, initialization, the 10% variance floor, and entity amplitude scaling prevent this from being an exact general guarantee. It also preserves one stationary increment moment, not the complete 1–13-week law.

Entry and rebirth cannot be relegated to incidental detail. [They remain active](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/rankdiff_rdm.py:412) in the “slide only” and “churn only” worlds. Those are not interventions removing all turnover or all restoring mechanisms. Consequently, “without churn there would be no hierarchy” and “without the slide it would spread without limit” are not demonstrated for this implementation.

Finally, disclose the inherited flexible profiles and training-score calibration of observation noise. The architecture is compact; its parameterization is not simply three measured scalars.

5. **Round-two points: resolved, partly resolved, and open.**

**Resolved or substantially improved:** the averaging-null objection; explicit submissions-exponent fragility; arithmetic rather than geometric shares in the new structure harness; long-panel gates; deliberate ablations; disclosure of the original variance failure and the subsequent missing-drift bug; and a matched-horizon Facebook comparison.

**Partly resolved:** entrant handling is repaired, but the structure and descriptive analyses still use different population definitions. The arithmetic decomposition exists, but its theoretical comparison is invalid as implemented. Shared entity bootstrap weights improve dependence handling, but temporal uncertainty is not resolved. The corrected model removes systematic tail underdispersion in the new long-panel gates, but calibration remains uneven: comments’ simulated/observed 13-week 90th-percentile ratios range from approximately 0.88 to 1.40. The model now has an out-of-period structure test, but that test concerns the original variance specification.

**Still open:** cross-model recovery; recovery of the hold closure; the structured by-cutoff bias in the boundary estimator; fitted level-process rivals; the earlier harness discrepancy; and a validated link from persistent community-level movement to arithmetic concentration.

Novelty language has also partly regressed. The introduction still emphasizes Gomez’s Forbes application while presenting whole ladders with passing fluctuations as the advance. His Appendix F already treats income flows and distribution-wide decomposition. The contribution must be the particular estimator, its demonstrated performance, and the attention findings—not those broader categories. [Gomez, author manuscript, Appendix F](https://www.matthieugomez.com/files/topshares.pdf)

6. **Errors requiring correction.**

**Arithmetic displacement theory.** [The A8 code](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/gomez_decomposition.py:58) substitutes \(1/S\) into \(\tfrac12(\zeta-1)\nu^2\). Gomez’s general expression is

\[
B=\frac12\frac{f(q)q^2}{p\,\bar a_{\mathrm{top}}}\nu^2.
\]

The reduction to \(\tfrac12(\zeta-1)\nu^2\) uses a Pareto-tail approximation with finite mean, hence \(\zeta>1\). Applying it where \(S>1\) produces invalid negative predictions. This is not evidence that passing swings explain the discrepancy. Use the general density-and-threshold expression, then investigate aggregation and measurement effects. [Gomez, Proposition 2 and stationary-tail discussion](https://www.matthieugomez.com/files/topshares.pdf)

**Equation 1 omits disappearance weighting.** The implemented formula is

\[
f_k=v_k+(1-v_k)\mathbb E[1-e^{-D^+/S_k}\mid\text{survival}],
\]

under the stated distributional assumptions. The parenthetical “plus the small share” does not repair the displayed equation. Saved disappearance fractions reach 3.2%; they are not uniformly negligible relative to turnover.

**A5c is incorrectly scored PASS.** Recomputing the saved comments means gives

\[
0.1304821137-0.0999393660=0.0305427477>0.03.
\]

The declared criterion is narrowly failed, not met “exactly at the margin.” This barely changes the substantive interpretation but must change the recorded verdict.

**Retention accuracy is a mean, not a bound.** The submissions moving-home errors at cutoffs 312 and 625 are approximately 0.066 and 0.077. Comments’ head error is approximately 0.048. “Within 0.02–0.04 at cutoffs” should become “mean absolute error 0.019–0.040 across four cutoffs.”

**Annual rates are described as two-year changes.** The counterfactual output divides total changes by two. The reported 0.03–0.26 range is approximately per year; two-year changes are twice those values. Figure 4’s axis is clearer than the prose.

**Population definitions disagree.** A8 reports submissions shares of 0.498 in 2020 and 0.340 in 2022; A6 reports 0.508 and 0.356. `mts_step1.matrices` tracks anyone ever inside weekly \(4K\); `restrict_universe` selects the best \(4K\) permanent ranks. These differences are methodological, not rounding.

**Several smaller statements are inaccurate.** The sixty rate-law observations are cutoff-by-period cells—five period pairs per ladder—not sixty distinct annual intervals. Displacement spans approximately 0.037–0.078, not 0.05–0.08 everywhere. Two submissions-head cells actually meet A8’s factor-of-two criterion. “Every community loses ground” should describe cohort averages. Figure 5B compares raw and aggregate-adjusted correlations from different origin sets, exaggerating their direct comparability.

**Registration is overstated.** The manuscript says the new-data tests “have therefore registered,” while [protocol v3.1](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/BRIDGE_PROTOCOL_V3_DRAFT_2026-10-18.md:1) remains explicitly “DRAFT, NOT ADOPTED.” Its P7 still specifies the frozen form, not the corrected variant. The SI also retains unfinished gate, structure, and registration sections.

7. **The five most valuable changes before submission, ranked.**

1. **Repair the arithmetic bridge and narrow the causal language.** Correct A8’s theoretical comparator, distinguish exact accounting from mechanism evidence, and remove unsupported claims about commenters, literal decline, and universally sustaining turnover. This addresses the most damaging errors without requiring new data.

2. **Make one model carry the empirical claims.** Keep the core in the main text, but provide a specification table identifying dynamics, drift closure, estimation window, noise treatment, and initial population for every experiment. Evaluate the corrected specification on the existing out-of-period structure task. Present flat-start construction as a separate conditional demonstration unless the same specification supports it.

3. **Make the boundary test’s uncertainty and null meaningful.** Propagate slope estimation, temporal dependence, and shared Reddit shocks. Calibrate the complete estimator under persistent heterogeneous drift, factor exposure, passing fluctuations, and the candidate model itself. Report simultaneous or explicitly pointwise conclusions, including failures by cutoff.

4. **Re-establish the persistence interpretation with matched populations.** Use prefix-only membership, true positive/negative changes alongside median-relative changes, matched origins for factor comparisons, and uncertainty accounting for repeated communities and overlapping origins. Measure the connection to the head’s arithmetic decline directly. Describe item counts as posting or commenting activity until unique-participant evidence exists.

5. **Finish an auditable submission package.** Correct A5c, report all failed expectations beside successes, add the missing SI tables, distinguish seed variability from sampling uncertainty, and put bands on head and flat-start claims. Replace “every rank” with tested cutoffs, quantify hierarchy change, and align each figure with its caption and population.

Owner decisions are needed for the final title and claim language, the model specification to carry forward, and adoption of a corrected bridge protocol before any opening. New data are needed to establish transport beyond the repeatedly examined development period, distinguish persistent dynamics from pandemic-era episodes, and make a comparable annual Facebook claim. Capture-delay and unique-participant evidence would strengthen the substantive interpretation. None of those future steps excuses the mathematical, coding-to-prose, and reporting corrections that can be completed now.