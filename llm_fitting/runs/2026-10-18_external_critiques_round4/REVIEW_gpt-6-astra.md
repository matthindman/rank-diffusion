_Round-4 review by gpt-6-astra (OpenAI) through the Codex CLI codex-cli 0.163.0 (`codex exec -m gpt-6-astra -s read-only`), 2026-10-18, from `CRITIQUE_PROMPT_R4.md` (committed 6590c29 before the review). Written by the CLI; header added by the orchestrating session. Model output: checked in `SYNTHESIS_R4.md`._

**1. Editor’s decision now: send out, without enthusiasm.** The question is appropriate for PNAS, and the combination of weak size scaling, rapid membership replacement, and persistent decline warrants expert review. Version 2 substantially improves the distinction between accounting identities and empirical discoveries, and the corrected model now carries the principal evaluations consistently. Enthusiasm is limited because the causal title and the attribution of falling concentration to persistence exceed what the counterfactuals establish, while several supposedly repaired errors remain in the SI. The single change most likely to raise the odds is a calibrated, replicated comparison showing exactly how diffusion contributes to structural durability, including an explanation of the comments counterfactual’s opposite sign.

I reviewed the TeX sources, all five figures, their generator, the requested records and protocol, and relevant scripts and outputs. I independently recomputed selected summaries from saved JSONs. No repository files were changed, no raw or sealed data were accessed, and no heavy computation was run.

**2. The title and the owner’s question.** The paper answers part of “Where does a durable hierarchy come from when churn is high?” It shows that measured movement and drift can generate a hierarchy, and that boundary crossings contribute positively to maintaining top-set levels. It does not yet explain the observed hierarchy’s full steepness or establish that removing turnover generally makes it less durable.

The weakest expression remains **“holds up.”** Its three supporting arguments establish different things:

- **Crossings return approximately three quarters of the slide:** supported as accounting. From the 60 saved cells, I obtain \(0.003997/0.005518=0.7243\), consistent with §2z-dh.
- **Without churn the hierarchy collapses:** not established by the implemented experiment.
- **A flat start produces a hierarchy:** supported for the reduced lasting core, conditional on empirical volatility and drift; the resulting hierarchy is generally too flat.

The strongest short-seller objection is already in Fig. 4C. At the comments head, shape changes by **−0.0802 log units/year with diffusion and drift**, but only **−0.0379 without diffusion**. The respective 90% simulation bands are **[−0.1024, −0.0548]** and **[−0.0410, −0.0352]**. Removing diffusion therefore makes this hierarchy *more durable over the tested interval*. Submissions support the intended cushioning effect: −0.1601 with diffusion versus −0.2587 without it.

Moreover, “without churn” sets `sigma0=0`; exits and rebirth remain active. See [churn_counterfactual.py:90](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/churn_counterfactual.py:90) and [rankdiff_rdm.py:431](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/rankdiff_rdm.py:431). Removing a crossing term while holding the measured slide fixed differs from removing diffusion, which changes the slide too.

A defensible title retaining the owner’s question is **“How turnover replenishes a durable hierarchy of online attention.”** “Durable” should refer to persistence of the broad rank–size structure, alongside the economically substantial decline in head concentration.

**3. Each Results section.**

**(1) The puzzle.** The descriptive evidence is sufficient: membership changes while a recognizable hierarchy persists. Quantifying mean-log shape change is a substantial improvement over round three.

The first attack will be **“the hierarchy moves far more slowly.”** Replacement fractions, log-shape changes, and arithmetic concentration have different scales; their numerical magnitudes are not directly comparable. The submissions head’s fall from 50.2% to 34.0% is large for an inequality economist, even if the ladder remains visually recognizable. Define structural durability explicitly and retain both measures. The study establishes coexistence of hierarchy and turnover more securely than a general timescale inequality.

**(2) Size gives little protection; Eq. 1; exact-rank change versus replacement.** Weak size–variance scaling is one of the strongest findings. The reconciliation with \(C(R)\) is convincing: exact-rank occupancy and membership of a top set measure different events.

The first attack will be **“activity follows Gibrat’s law.”** A small variance exponent does not establish independence of the entire growth distribution from size. The paper itself requires drift by rank, and the comments exponent’s interval excludes zero. “Growth volatility depends weakly on size” is both accurate and substantial.

The claim that size-blindness *alone* exposes the top also drops a necessary ingredient: the head’s flatter slope. Eq. 1 contains both movement and geometry. Its derivation additionally assumes a locally exponential ladder and displacement sufficiently independent of initial height; it is not an unconditional identity.

The firm comparison needs estimator recovery. A planted exponent of 0.2 becomes **0.114** under the null’s passing noise. That attenuation narrows the empirical distinction, particularly for submissions, whose leave-one-stratum-out range reaches 0.19. The comparison remains interesting, but “measured the same way” needs stronger support.

**(3) The balance and null worlds.** The accounting now leads with an appropriate statement, and the pooled boundary estimate is reported candidly, including submissions’ failure.

The first attack will be **“what it tests is diffusion in level rather than copying”** ([main:136](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:136)). One neutral-copying specification producing 0.58 does not exclude copying as a class. Its sampling, innovation, observation process, and finite-horizon behavior all matter.

The crossing term materially improves accounting: my recomputation gives RMSE **0.003964** for slide alone and **0.001127** for crossing minus slide. However, a ladder-wide constant crossing gain does better still, **0.000581**, as §2z-dh already records. This supports replenishment as a level contribution; it does not establish that the rank-specific variance–slope prediction explains the variation.

The strongest empirical statement is the measured balance’s magnitude and heterogeneity. Present its near-identity component as an estimator and model-consistency check.

**(4) The generative model.** The corrected moving home now carries the gates, out-of-period exercise, and model world. The inherited parameter profiles and variance correction are substantially better disclosed. The previous covert mixing of versions is largely repaired.

The first attack will be **what “generative” explains beyond conditional reproduction**. The flat-start core produces a flatter hierarchy; the full model misses persistent changes and the head’s decline. On submissions, movement error remains 0.183 versus 0.163 for carrying historical statistics forward. The corrected model forecasts the 2022 head share around 0.537 versus 0.356 observed (§2z-dh).

Distinguish one architecture, two drift closures, and a reduced-core intervention. Agreement between the flat-start simulation and its own stationary balance is an internal consistency result, not independent recovery of the observed ladder.

“As well as” also suggests stronger equivalence evidence than similar means across five overlapping origins provide. And “none fitted to the statistics we test it on” needs qualification: the observation scale minimizes training versions of the movement moments ([rankdiff_rdm.py:642](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/rankdiff_rdm.py:642)). This is legitimate training calibration, not held-out leakage, but those moment types are targeted.

**(5) Persistence and the falling head.** The primary repairs are real: prefix membership changes the ordinary gapped statistic little; the split now uses true signs; factor comparisons use matched origins. These resolve important round-three criticisms.

The first attack will be the abstract’s claim that persistence **“cut” the head share from 50% to 34%**. Serial correlation and an exact concentration decomposition do not identify that causal contribution. The decomposition establishes incumbent underperformance; it does not establish why that underperformance occurred.

The post-2020 check strengthens the descriptive finding. Persistence survives both comparisons beginning after the surge and comparisons ending before the capture change (§2z-di). But these are respectively **2–4 and 1–2 overlapping origins**; each subset contains the other potential confounder. The Results sentence “It is not the surge … nor the change” exceeds the Discussion’s correctly qualified conclusion.

The factor interpretation also remains observational. “Platform total” is summed activity in the restricted tracked universe, and removing estimated exposure to that total does not identify persistence’s origin. Counts measure posts and comments, not participant retention; “it stays only as long as its participants do” is unsupported.

Finally, the Gomez selection check is incomplete. Its “permanent” cohort uses the **same starting year** whose activity enters the subsequent change ([gomez_r3.py:53](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/gomez_r3.py:53)). That changes the selection variable but does not eliminate selection on that year’s shocks. Nor does a share-stable year identify regression to the mean: unchanged share mechanically makes the within loss approximately offset displacement.

**4. Automatic against discovered.** The revised distinction is useful, but its universal wording is too strong.

The four null worlds are **single-seed illustrations**. Each is invoked at seed zero; the size-blind and firm-like worlds use specified passing standard deviations of 0.3 and a five-week half-life. The firm-like world also changes drift and the floor. See [null_worlds.py:35](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/null_worlds.py:35). They do not isolate size scaling alone or span plausible calibrated alternatives.

Three corrections matter:

- **Flattening is already produced by the nulls.** Crossing/slide ratios are approximately **0.575 firm-like, 0.910 neutral, and 0.804 model**. Only the selected driftless size-blind world steepens. “Discovered against every null world” is contradicted by the table.
- **Persistence is not excluded by random growth generally.** The SI itself reports a random walk with heterogeneous community drift producing **+0.17 to +0.19**. The evidence rejects specified independent-increment/short-memory worlds, not all random-growth explanations.
- **Size-blindness is empirical; exposed leaders require geometry too.** A differently calibrated size-blind world could change turnover and balance through passing components, initial gaps, or drift.

Conversely, some “automatic” quantities remain informative conditionally. Boundary agreement can reject particular dynamics-plus-measurement specifications. Approximate Eq. 1 agreement checks its local-shape and displacement assumptions. Rank-one stability measures gap size relative to volatility; it is not universal—Facebook’s rank-one occupant changes in **52% of weeks**.

The useful question is therefore which *calibrated alternatives* reproduce the joint empirical pattern, not which statistics are automatic in every conceivable random-growth world.

**5. Round-three points.**

**Resolved in the main text:** Eq. 1’s decomposition status and disappearance term; the \(C(R)\) distinction and ranking literature; moving-home equations; gate cohort and noise-scale disclosure; corrected out-of-period results; 20-seed flat-start bands; true-sign and prefix-membership persistence; matched factor origins; the general Gomez formula.

**Half-resolved:** one-model presentation, because closures and the reduced core still need a compact specification table; “slowly changing,” now quantified but metric-dependent; counterfactual maintenance; null discrimination; persistence versus capture effects; data availability.

**Still open:** calibrated competing level models, cross-model recovery, temporal uncertainty, boundary-estimator bias, and the causal connection from persistence to concentration. The same-year Gomez cohort does not complete the selection repair. Several substantive round-three corrections were never propagated into the SI.

The theorist accepts the accounting, the computational social scientist values the operational distinctions, and the econometrician credits the repairs. The inequality economist still rejects treating structural resemblance as unchanged concentration. The short seller’s strongest case remains the comments intervention, followed by the unsupported persistence-to-concentration attribution.

**6. Errors requiring correction before submission.**

The most consequential new audit finding concerns **expectation scoring**. [PLAN.md:369](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/PLAN.md:369) specifies:

| Expectation | Saved outcome | Required correction |
|---|---:|---|
| Neutral boundary ratio inside [0.85, 1.18] | 0.578 | This component failed. |
| Firm-like estimated β inside [0.12, 0.28] | 0.114 | This component failed. |
| Every null’s persistence within ±0.05 | Three worlds reach −0.06 to −0.11 | The all-world expectation failed. |

[RESULTS.md:375](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/RESULTS.md:375) and §2z-dh mark these PASS by substituting a favorable interpretation for the written criterion. Unexpected null behavior may strengthen a scientific contrast; it does not retrospectively pass the expectation. This deserves immediate correction because failure accounting is a prominently advertised strength.

Other concrete defects:

- **SI:147:** “platform aggregate” contains old cohort-mean residuals. Correct full-membership platform-total values are submissions **0.132/0.020/−0.029**, comments **0.191/0.177/0.141**.
- **SI:149:** old median-split results remain. Correct true-sign riser/faller values are submissions **0.031/0.177**, comments **0.003/0.158**.
- **SI:187:** the withdrawn Pareto Gomez application and negative predictions remain. **SI:63** also omits entry from the stationary formula and leaves the Pareto analogue unqualified. See [SI](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:147).
- **Main:157:** “within 0.03” contradicts the actual comments spread, **0.030543**, correctly scored as a narrow failure in SI:94.
- **Main:201; SI:211:** “committed before each analysis” contradicts the disclosed R3 ordering: the first Gibrat run preceded the plan commit.
- **Fig. 3C:** “Turnover = churn / slope” reinstates the failed linear approximation and omits disappearance.
- **Fig. 2C:** “rank one rarely changes hands” incorrectly includes Facebook.
- **Fig. 4C:** available head simulation bands are omitted, concealing how clearly comments differ between interventions.
- **Figure reproduction:** the default generator calls old `fig2`/`fig3`, producing old filenames; current figures require `fig2v2`/`fig3v2` ([maker:513](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/make_pnas_figures.py:513)).

A particularly relevant missing citation is **Sangiorgio et al., “Followers do not dictate the virality of news outlets on social media.”** It already appears in the repository’s data-sharing notes and studies Gibrat-type Facebook engagement growth. Explain how your annual, permanent-rank variance estimand differs from its size measures and horizons. [Primary text](https://arxiv.org/html/2401.17890v2).

CrowdTangle citation placeholders remain, and Baker/Smyth’s verified names are **Graeme Baker and Caroline Smyth**. [Author record](https://arxiv.org/abs/2608.27156).

**7. Data availability.** The statement is a sensible conservative draft, but **not yet submission-ready or sufficient to certify compliance**.

PNAS requires public access to research materials, permits legal/ethical/logistical exceptions disclosed at submission, and requires editorial access when requested. Supply the actual repository DOI, dataset citations, precise restrictions, and an operational verification route; request the necessary exception rather than assuming reviewer-only access suffices. The AI disclosure also needs specific models/versions. [PNAS policy](https://www.pnas.org/author-center/editorial-and-journal-policies).

For CrowdTangle, withholding exports and identifiable Page-level series while releasing aggregate findings and permitted identifiers aligns with the repository’s documented guidance. However, [DATA_AVAILABILITY_NOTES.md:49](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/DATA_AVAILABILITY_NOTES.md:49) explicitly admits that permission for raw-data transfers to reviewers was not found in CrowdTangle’s own guidance. Another study’s arrangement is precedent, not this project’s authorization. Resolve that before promising transfers.

For Reddit, withholding archives and weekly panels follows the owner’s instruction. Naming communities is scientifically defensible here, but the owner’s ethical judgment does not itself establish contractual permissions. The notes rely on a mirrored 2023 statement and acknowledge that current terms were unchecked. Current official policies impose access and use conditions; they do not alone settle which historical agreement governs these Pushshift archives. Document acquisition dates, provenance, applicable terms, and permissible verification access. [Reddit terms](https://redditinc.com/policies/data-api-terms), [official access guidance](https://support.reddithelp.com/hc/en-us/articles/14945211791892-Developer-Platform-Accessing-Reddit-Data).

The finding is **insufficient documented permission**, not a demonstrated violation.

**8. The five most valuable changes before submission, ranked.**

1. **Resolve the maintenance counterfactual.** Define diffusion, set replacement, and exit/rebirth separately; show the available bands; explain why diffusion increases comments’ flattening while cushioning submissions. Match the title and abstract to the resulting evidence. This is the weakest claim supporting the central argument.

2. **Replace illustrative nulls with calibrated, repeated comparisons.** Use common initial ladders, matched observation and passing components, and paired seeds; vary the mechanism being tested. Include at least 20 seeds for head claims. Test a size-blind world with the data’s passing components and a heterogeneous-drift rival. Report estimator attenuation and whether the *joint* pattern discriminates.

3. **Complete the source-to-manuscript audit.** Correct the expectation verdicts, stale SI, figure labels and generator command, registration language, citations, and model specification table. Generate numerical tables from designated outputs where possible. These are avoidable credibility failures.

4. **Strengthen persistence and its connection to concentration.** Show uncertainty conditional on the observed periods, retain origin-level results, and distinguish factor adjustment from mechanism identification. Use genuinely prior cohorts for the Gomez sensitivity and a simulated noise baseline. Either identify persistence’s contribution to concentration or remove that causal attribution.

5. **Finish positioning and reproducibility.** Address the closest Facebook growth study, state precisely what annual weak variance scaling adds, and complete the DOI, dataset provenance, access restrictions, permissions, and model-version disclosure.

These changes can use existing data, although the calibrated simulations require additional computation beyond this review’s limits.

Owner decisions remain necessary for final claim language, Page-ID publication, data-access arrangements, and protocol adoption. New data are needed for temporal transport of persistence, weak size scaling, and the corrected model. The bridge’s P5–P7 are valuable prospective tests, but the draft is unadopted and contributes no present confirmation.

Before adoption, clarify that **P4 noninferiority does not establish that movement “adds to rank”**, and interpret P6 against the demonstrated attenuation of a planted firm-like exponent. P1’s restricted submissions range must remain prominently disclosed. Passing a near-identity on new data would help establish transport; it would not, by itself, repair the causal maintenance claim.