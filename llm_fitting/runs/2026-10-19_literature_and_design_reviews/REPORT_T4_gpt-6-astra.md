<!-- Provenance: Task 4 (one model or many) report by GPT-6 Astra (gpt-6-astra) (Codex CLI 0.163.0, codex --search exec, read-only sandbox, reasoning effort xhigh), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

## Access check

I read both briefs completely before undertaking the review. I also read:

- `paper/pnas_draft.tex`, `paper/pnas_si.tex`, and `paper/references.bib`; these manuscript sources match the specified v4 commit, `42955ac`.
- The ladder’s `DESIGN.md`, `explainer_text.json`, and the narrative, result structure, and display logic in `explainer.html`.
- The requested R7, R9, and R10 sections of `RESULTS.md`, the full `PLAN.md`, and the model summaries in `null_worlds_r7_summary.json`.
- `PROGRAM_STATE.md`, `GATE_LEDGER.md`, `SYNTHESIS_R5.md`, and relevant canonical entries in `MODEL_STATUS.md`, especially §§2z-ap and 2z-dg–dk.
- Repository instructions and the stochastic-modeling and model-status-authoring skills.

I used local file reading, live web search, and public full-text retrieval. Some publisher pages failed; author and institutional copies supplied most relevant full texts. Chisholm–Pacala and Hoeting et al. are explicitly marked **[abstract only]** below. I did not inspect the rendered manuscript PDFs or figure PNGs, audit all simulation implementations, rerun analyses, open raw or sealed data, or write files.

**Model:** GPT-6 Astra, as identified in the task; exact build unavailable. **Interface:** Codex CLI. **Tools:** local read-only shell, web search, full-text retrieval. **Date:** 19 October 2026.

# Task 4: One model or a structured comparison?

## 1. Recommendation, strongest case for, and strongest case against

**Adopt a limited version: an SI model atlas organized around the paper’s scientific questions, with a short main-text synthesis. Retain the generative model as a central constructive demonstration. Do not promise “all plausible models,” declare an overall winner, or turn this manuscript into an ensemble-forecasting paper.**

### The strongest case for a broader comparison

The strongest argument is already demonstrated by this project. Comparisons have changed the interpretation of its findings.

The controlled geometry experiment showed that elevated turnover near the head does not require size-blind growth: changing the starting ladder while holding dynamics fixed produces the effect under both size-blind and firm-like volatility. The null worlds also separated statistics that arise broadly in diffusion models from persistence that those worlds fail to reproduce. These comparisons improved the scientific answer, rather than merely adding assurances around a preferred specification. Their development and corrections are recorded in `MODEL_STATUS` §§2z-dj–dk.

A systematic presentation could make three contributions.

First, it would distinguish **observed regularities, consequences of mathematical structure, and evidence that discriminates between mechanisms**. A successful boundary identity should carry a different evidential meaning from a successful forecast or a persistence statistic that competing worlds miss.

Second, it would make the authors’ agnosticism operational. Readers could see exactly where simpler models suffice, where additional structure helps, and where every evaluated model fails. This is particularly valuable because the current model does not dominate the movement comparisons.

Third, it would make selection inspectable. A declared set of model families, calibration rules, diagnostics, and exclusions is more informative than scattered comparisons whose inclusion criteria readers must reconstruct.

AI makes implementation and documentation less expensive. That strengthens the practical case for maintaining such an atlas. It does not remove the need for scientific judgment, implementation checks, or protected evaluation data.

### The strongest case against

The most serious objection is **false comprehensiveness**. Model space is effectively unlimited. Ten closely related diffusion variants can create an appearance of breadth while omitting the strongest substantive rival.

That risk is concrete here. The current comparisons are strongest against worlds without persistent differences in mean growth. Yet heterogeneous mean growth already reproduces positive persistence in a small positive-control experiment. A larger collection of homogeneous worlds would not address that alternative.

The owner’s premise also needs qualification: the existing evidence does not establish a distinctive bundle of empirical features that only the current generative model reproduces. Its clearest unresolved empirical feature—persistence—is missed by that model too. Its demonstrated value is an interpretable construction connecting measured ingredients to structural dynamics.

A second objection is that **common outputs can conceal unequal information inputs**. A model given the observed ladder, a model calibrated to maintain that ladder, and a model generating structure from equality face different tasks. Combining their apparent successes in one undifferentiated table would be misleading.

Third, fixed data make extensive searching more consequential. Additional models do not create additional independent historical episodes. The movement ledger documents repeated evaluation on development panels; presenting those evaluations together improves transparency but cannot restore untouched validation.

Finally, the format costs attention. A six- or seven-page article cannot teach several modeling traditions, document their calibration, and preserve the substantive account of online attention. Referees may reasonably challenge weak implementations or an arbitrary model roster. More models can therefore increase the burden of justification.

**The appropriate commitment is bounded comparative explanation:** a small, defensible roster, a common measurement procedure, visible failures, and explicit limits on what each comparison establishes.

## 2. Precedents and what makes them credible

These traditions support different activities. Model intercomparison, many-analyst studies, robustness analysis, and predictive averaging should not be presented as interchangeable.

### Model intercomparison: common experiments matter more than model count

[Eyring et al. (2016), “Overview of the Coupled Model Intercomparison Project Phase 6 experimental design and organization,” *Geoscientific Model Development*](https://doi.org/10.5194/gmd-9-1937-2016) — **[full text]** — describes shared diagnostic and historical experiments, common standards, and additional experiments organized around scientific questions. The design also recognizes resource constraints.

Its institutional uptake is directly observable: [IPCC AR6, Working Group I, Figure 1.22](https://www.ipcc.ch/report/ar6/wg1/figures/chapter-1/figure-1-22/) — **[full text: caption]** — incorporates that architecture. This is evidence that organized intercomparison became useful scientific infrastructure; it is not evidence that every ensemble member or ensemble spread deserves equal evidential weight.

A closer PNAS precedent is [Lotze et al. (2019), “Global ensemble projections reveal trophic amplification of ocean biomass declines with climate change”](https://doi.org/10.1073/pnas.1900194116) — **[full text]**. Different marine ecosystem models produced comparable outputs under coordinated climate and fishing scenarios. The shared question and experimental conditions made the diversity interpretable.

**Application here:** define common experiments—observed-ladder forecasting, equality-start construction, and matched-geometry interventions—before expanding the roster. Keep those experiments separate. The smallest useful borrowing belongs in the opening of SI §9.

### Forecasting ensembles: the strongest direct PNAS template, with a different objective

[Cramer et al. (2022), “Evaluation of individual and ensemble probabilistic forecasts of COVID-19 mortality in the United States,” PNAS](https://doi.org/10.1073/pnas.2113561119) — **[full text]** — evaluates archived forecasts using common targets, horizons, baselines, and probabilistic scoring. The ensemble performed consistently well overall, while performance deteriorated around important changes in epidemic trajectories.

Its credibility comes from forecasts issued before outcomes, explicit eligibility rules, repeated evaluation, and disclosure of consequential failures. The ensemble’s average success did not erase its difficulty at turning points.

**Application here:** borrow the reporting discipline for future forecasts. The failure to predict the submissions head’s decline deserves its own outcome column even if average retention is respectable. Cramer supports a prospective comparison; it does not establish that averaging mechanisms improves explanation.

### Many analysts: variation in judgment is an object of study

[Silberzahn et al. (2018), “Many Analysts, One Data Set,” *Advances in Methods and Practices in Psychological Science*](https://doi.org/10.1177/2515245917747646) — **[full text]** — documents how teams answering a shared question can obtain materially different results through defensible analytic decisions.

[Breznau et al. (2022), “Observing many researchers using the same data and hypothesis reveals a hidden universe of uncertainty,” PNAS](https://doi.org/10.1073/pnas.2203150119) — **[full text]** — extends that point through a large coordinated project. The distribution of analyses, rather than a selected answer, is central evidence.

These are credible because they define the common task and expose variation across actual researchers. A single team asking several language models to generate variants is not an equivalent design: prompts, shared code, and common assumptions can strongly correlate the outputs.

**Application here:** use this literature to justify documenting consequential choices. Do not describe the proposed atlas as a many-analysts study.

### Multiverses and specification curves: enumerate defensible choices

[Steegen et al. (2016), “Increasing Transparency Through a Multiverse Analysis,” *Perspectives on Psychological Science*](https://doi.org/10.1177/1745691616658637) — **[full text]** — shows how alternative processing decisions can expose dependence on an analyst’s choices.

[Simonsohn, Simmons, and Nelson (2020), “Specification curve analysis,” *Nature Human Behaviour*](https://doi.org/10.1038/s41562-020-0912-z) — **[full text]** — requires specifications to be “theoretically justified, statistically valid and non-redundant” (p. 1208). **[verified: [author PDF](https://faculty.wharton.upenn.edu/wp-content/uploads/2016/11/33-Simonsohn-Simmons-Nelson-2020.pdf)]**

The surrounding argument concerns selecting admissible analyses before displaying their results; it does not authorize a combinatorial expansion of every imaginable choice.

Reception includes substantive methodological criticism. [Del Giudice and Gangestad (2021), “A Traveler’s Guide to the Multiverse,” AMPPS](https://doi.org/10.1177/2515245920954925) — **[full text]** — explains how choices can change the measurement, estimand, or precision, making specifications unequal.

**Application here:** counts, karma, different horizons, and different populations can answer different questions. Group them accordingly. Current-rank stratification should not receive equal standing with permanent-rank stratification merely to enlarge a multiverse.

### Power-law alternatives: distinguish adequacy from relative performance

[Clauset, Shalizi, and Newman (2009), “Power-Law Distributions in Empirical Data,” *SIAM Review*](https://doi.org/10.1137/070710111) — **[full text]** — combines assessment of fit with comparison against plausible alternatives.

Their formulation is precise: “Statistical tests can be used to rule out specific hypotheses” (p. 679). **[verified: [full-text PDF](https://www.cs.cornell.edu/courses/cs6241/2019sp/readings/Clauset-2009-power-laws.pdf)]**

The surrounding passage explicitly recognizes that infinitely many alternatives exist and that researchers must decide which are reasonable.

[Broido and Clauset (2019), “Scale-free networks are rare,” *Nature Communications*](https://doi.org/10.1038/s41467-019-08746-5) — **[full text]** — illustrates systematic comparisons against alternatives and graded evidential categories. Reception was not uniform: [Holme’s accompanying perspective, “Rare and everywhere”](https://doi.org/10.1038/s41467-019-09038-8) — **[full text]** — discusses tensions between finite empirical tests and theoretical meanings of scale-free structure.

**Application here:** distinguish a model’s absolute mismatch from its advantage over another model. Borrow that logic, not distributional tests whose independence assumptions do not fit these panels.

### Neutral versus niche: shared patterns may not identify mechanisms

[Chisholm and Pacala (2010), “Niche and neutral models predict asymptotically equivalent species abundance distributions in high-diversity ecological communities,” PNAS](https://doi.org/10.1073/pnas.1009387107) — **[abstract only]** — provides an especially relevant warning: different ecological mechanisms can yield the same limiting abundance pattern.

**Application here:** reproducing a rank–size curve cannot by itself identify the process that generated it. Temporal persistence, transitions, and responses to changes in conditions may discriminate where stationary shape does not. This is a reason to compare carefully chosen dynamic facts, rather than multiply shape-fitting demonstrations.

### Levins: pluralism substantially predates AI

[Levins (1966), “The Strategy of Model Building in Population Biology,” *American Scientist*](https://www.jstor.org/stable/27836590) — **[full text]** — argues for examining conclusions across models with different simplifying assumptions.

“Hence our truth is the intersection of independent lies” (p. 423). **[verified: [full-text PDF](https://www.zoology.ubc.ca/let/pdfs/Levins1966.pdf)]**

The context is robustness to different idealizations while retaining a common biological assumption. The aphorism does not imply that model outputs are statistically independent observations.

**Application here:** replace the historical claim that single-model science is a pre-AI habit with the practical claim that current tools lower the cost of implementing an established comparative strategy. Shared measurement artifacts can survive every model in an atlas.

### Bayesian averaging and stacking: useful predictive tools, not mechanism votes

[Hoeting et al. (1999), “Bayesian model averaging: A tutorial,” *Statistical Science*](https://doi.org/10.1214/ss/1009212519) — **[abstract only]** — treats uncertainty arising from model choice.

[Yao et al. (2018), “Using stacking to average Bayesian predictive distributions,” *Bayesian Analysis*](https://sites.stat.columbia.edu/gelman/research/published/stacking.pdf) — **[full text]** — constructs predictive combinations using proper scoring rules and cross-validation, including settings where none of the candidate models is true.

**Application here:** averaging is optional and premature. A stacking weight would concern predictive usefulness, not the probability that a mechanism explains attention. Any eventual implementation would require temporally appropriate validation and genuinely comparable predictive distributions.

Across these precedents, credibility comes from a shared question, admissible alternatives, explicit information inputs, common evaluation, and complete disclosure. Publication alone does not establish favorable reception; the clearest reception evidence retrieved here is institutional uptake for CMIP and explicit methodological debate around scale-free testing and multiverses.

## 3. Ranked design options

The ranking below concerns value for this revision. A future prospective comparison has greater long-term evidential value than its fourth-place ranking suggests.

| Rank and option | What it would show | Main-text space | Compute and authoring cost | Referee risk and missing requirements |
|---|---|---|---|---|
| **1. (b) SI model atlas** | Which specified mechanisms reproduce which outputs, under stated calibration and initialization | About 80–120 words pointing to SI; replace existing prose | Low compute for organizing existing outputs; moderate authoring. New rivals add substantial work | Lowest risk if incomplete cells are explicit. Needs a model manifest and distinction between diagnostic worlds and fitted competitors |
| **2. (f) A small set of matched contrasts** | The effects of geometry, heterogeneous growth, common factors, and lasting dynamics | One existing panel or short paragraph; details in SI | Moderate compute and careful calibration | High scientific value, but weak rival implementations would undermine it. Needs calibrated heterogeneous-growth and common-factor competitors |
| **3. (a) Main-text facts × models matrix** | A readable summary of the atlas | Roughly a third to half a page; must replace material | Low once atlas exists; moderate editorial work | Binary checkmarks could exaggerate discrimination. Needs quantitative definitions and separate labels for inputs, predictions, failures, and unrun cells |
| **4. (d) Prospective forecasts from eligible models** | Whether conclusions and predictive rankings survive new observations | Brief future-test statement now; substantial protocol outside paper | High authoring and validation cost; bounded simulation cost after roster freezes | Strong eventual evidence. Needs owner-approved protocol, adequate power, common forecasts, and resolution of source-change issues before sealed access |
| **5. (c) Ablation ladder figure** | Contributions of ingredients within a particular model family | About half a page; best in SI | Low for historical display; appreciable for a current-model redesign | Existing ladder concerns an earlier model generation. Requires explicit historical labeling or a new matched ablation |
| **6. (e) Broad analyst-choice multiverse** | Dependence on processing and measurement decisions | SI only, potentially several pages | Potentially large combinatorial cost | Risks mixing different estimands and invalid choices. Needs a restricted admissibility rule and a common target within each branch |

### Why the existing ladder should not become the main comparison figure

The October 12 ladder is valuable, but its scope is specific. It concerns the earlier fixed-home construction and separately follows structure and movement branches. Each rung is refitted. The order therefore affects the apparent contribution of an ingredient.

Its combined index is a presentation device, not the adoption gate. The macro score also does not directly target every property that motivated amplitude and tail additions. Little improvement in that aggregate cannot establish that those ingredients are scientifically unnecessary. The design and `MODEL_STATUS` §2z-ap make these limitations recoverable; the article should not require readers to discover them.

Retain the interactive explainer as a research-history resource. If a current-model ablation is needed, begin with the existing fixed/wandering-home × rank-drift comparison in SI §6 and Figure 4D.

### How to handle the sealed comparison

Register **every eligible frozen candidate**, not every exploratory variant ever attempted. Freeze the roster, calibration windows, observation rules, horizons, exclusions, scoring, and any ensemble rule before access. Report paired differences and uncertainty, not just rankings.

The current proposal is not adopted. `MODEL_STATUS` §2z-dk identifies unresolved issues: the raw-exponent threshold does not discriminate the firm-like world, and a persistence test spans a source switch and blackout. Resolve those in an authorized protocol process. An exploratory winner chosen on the sealed outcomes would require further evidence before being treated as independently validated.

## 4. Choosing models and facts defensibly

### Make selection a documented scientific decision

For the existing atlas, describe selection retrospectively and honestly. The outcomes are already known. Freezing a roster now can govern additional analyses, but cannot make the historical comparison prospective.

A defensible inclusion rule would be:

1. Include models representing substantively different explanations of structure or movement.
2. Include simple forecasting baselines.
3. Include controlled variants needed to isolate a stated mechanism.
4. Include a positive control for every claimed discriminating statistic.
5. Exclude models only for declared reasons such as an incompatible observable, unavailable required input, or inability to implement the stated mechanism.
6. Report all eligible implementations, including failure, numerical problems, and inconclusive results.

The unit of comparison should be **a model specification together with its calibration, observation procedure, initialization, and horizon**. A family name is insufficient.

This matters immediately for the current RDM. The R7 model world uses a drift that holds the training ladder, so the ladder is partly an input. The long-run construction uses measured slides. These belong in separately identified experiments, even if they share code.

### The most important missing competitors

**First: calibrated heterogeneous mean growth and heterogeneous exposure to common shocks.** These directly address the strongest rival readings of persistence.

[Gabaix, Lasry, Lions, and Moll (2016), “The Dynamics of Inequality,” *Econometrica*](https://doi.org/10.3982/ECTA13569) — **[full text]** — develops departures from homogeneous random growth, including high-growth types, to explain transition dynamics. It is already cited in the manuscript. The missing contribution is an operational comparison, not another citation.

The existing heterogeneous-growth toy shows that positive persistence is not unique to a dynamic latent-state explanation. It does not establish that a calibrated heterogeneous-growth model explains the full collection of facts. Compare fixed growth differences, persistent but changing growth, and common-factor exposure using training-only estimation and the same observation pipeline.

**Second: a clearly specified, calibrated SPT/Atlas comparator.** This literature is already represented in the bibliography. The current world named `atlas` is a particular reflected-floor, firm-like specification; its performance cannot stand for the entire Atlas or hybrid rank-based family. Either provide the stronger comparator or narrow the label and claim.

**Third: an accumulation or preferential-attachment comparator, conditionally.** [Simon (1955), “On a Class of Skew Distribution Functions,” *Biometrika*](https://doi.org/10.1093/biomet/42.3-4.425) — **[full text]** — supplies a foundational reinforcement-and-entry mechanism. A useful application here must specify how the latent process generates weekly activity. Comparing cumulative popularity directly with weekly karma would create an avoidable mismatch.

Fitness and aging variants belong in this third tier if the paper makes claims about lifecycle mechanisms. They should not be added merely to complete a list of famous traditions. Reliable age or exposure measures and an explicit link from entity lifecycle to weekly activity are necessary.

### Select facts by their inferential role

| Diagnostic block | What belongs in the atlas | What it can establish |
|---|---|---|
| **Measurement and accounting** | Boundary relation, decomposition residuals, entry/exit accounting | Whether the measurement procedure behaves as expected; often weak mechanism discrimination |
| **Structure and geometry** | Rank–size profile, top-versus-body turnover under matched starting ladders | Whether geometry explains a turnover pattern; whether a model generates or merely inherits structure |
| **Growth process** | Volatility scaling, non-overlapping growth persistence, prior-decliner contrasts | Whether particular homogeneous or heterogeneous mechanisms remain plausible |
| **Predictive performance** | Frozen movement gate, retention, head-share path, tails and exits | Whether a model predicts specified future outcomes with acceptable uncertainty |
| **Long-run construction** | Equality-start formation, observed-start relaxation, ingredient removals | Conditional sufficiency and sensitivity to assumed parameter persistence |

Do not count several algebraically related statistics as independent successes. Likewise, gapped persistence, prior-decliner ordering, and excess-slide attribution are related views of the same underlying phenomenon.

For each cell, show the empirical quantity, simulated estimate and interval, calibration status, and evaluation horizon. Use labels such as **calibration input**, **derived consequence**, **evaluated discrepancy**, and **not evaluated**. Avoid a total number of successes.

Fairness requires common information availability, rather than mechanically identical parameter counts. Report fitted profiles as well as scalar parameters. Keep sampling uncertainty, Monte Carlo variability, and between-model disagreement distinct. The spread of selected models is not automatically a confidence interval for reality.

## 5. Failure as information

Failures inform the reader when each has a stated target, an observed discrepancy, and a consequence for interpretation. They should not be repackaged as victories.

### The flatter hierarchy

The present construction establishes that measured volatility and decline can generate a hierarchy from equality under maintained conditions. It does not reproduce the observed steepness quantitatively: the recorded long-run hierarchy is 8–46% flatter.

That distinction should remain in the main model paragraph, immediately alongside the construction result. Avoid suggesting that reproducing a hierarchy proves the historical origin of the observed one.

There is also an unresolved accounting question. `MODEL_STATUS` §2z-dk lists a check of whether entry/exit effects are counted twice; `PLAN.md` sketches a related drift sensitivity. Until that is resolved, attributing the steepness discrepancy to omitted persistence would be premature.

**Smallest change:** describe the result as conditional construction with a measured quantitative shortfall; keep the mechanism responsible for that shortfall open.

### The missed fall of the head

This is a distinct failure of prediction. The corrected model’s retention performance does not compensate for its failure to anticipate the submissions head’s decline. The corrected out-of-period results are recorded in `MODEL_STATUS` §2z-dh §3 and SI §6.

Preserve both findings together. The model captures aspects of ordinary rank movement while missing a substantial redistribution of attention. That identifies the limits of extrapolating its estimated dynamics.

Persistence is a plausible direction for improvement, but the current evidence does not show that adding it would have forecast the head’s fall. The prior-decliner cohort and the historical head are not identical.

### The null worlds

State exactly what fails and what survives.

- The geometry experiment supports a geometric explanation under both tested volatility structures.
- The homogeneous worlds fail the positive persistence statistic, while a heterogeneous-growth positive control can reproduce it.
- The neutral-copying world’s pooled boundary discrepancy is strongly affected by the universe edge; it does not justify rejecting neutral copying as an entire tradition.
- Raw volatility-exponent recovery is weak enough that a simple threshold can misclassify firm-like dynamics.

These are boundaries on explanations.

The same discipline applies to R10. Its later excess-slide accounting is interesting, but it follows two failed or partly failed written expectations. Those original expectations and the subsequent accounting must appear together. **SI §11 should explicitly add R10 to its failure and post hoc inventory.**

Implementation failures deserve a separate category. Preserve the variance-bookkeeping error in the historical record, but do not treat the buggy version as an equally plausible scientific competitor.

## 6. Nine prioritized robustness checks using existing development material

These are proposals for bounded follow-up work, not analyses performed for this report. Existing results should be reused where sufficient; additional runs should retain frozen defaults and use explicitly identified experimental variants.

| Priority | Check and inference tested | Current status | Result that would change the paper; location |
|---|---|---|---|
| **1** | **Reconcile drift, entry/exit, and normalization.** Does the simulator implement the same balance used to interpret its stationary slope? | The unresolved accounting check is explicitly listed in §2z-dk; a related sensitivity is planned, not reported as completed | A material bookkeeping discrepancy would change the interpretation of Figure 4’s shortfall and possibly its counterfactuals. Resolve algebra first, then test the declared correction; document in SI §6 |
| **2** | **Calibrated heterogeneous-growth and common-factor competitors.** Does persistence require changing latent states, or can stable growth differences/exposures explain it? | A small heterogeneous-growth positive control and factor residualizations exist; full matched competitors do not | Reproducing persistence and excess slide with simple fixed heterogeneity would narrow the mechanism claim. Failure of calibrated rivals across several diagnostics would strengthen the case for additional dynamics. SI §§7 and 9 |
| **3** | **Recovery under heterogeneous growth and common factors.** Can the pipeline distinguish the volatility mechanisms it is asked to distinguish? | Homogeneous planted-parameter recovery exists; raw-exponent compression is already documented. Relevant heterogeneous recovery remains unrun | If distinct planted mechanisms remain indistinguishable, remove sharp size-blind/firm-like classifications and revise proposed confirmation thresholds through the authorized process. SI §§3 and 9 |
| **4** | **Drift estimated from different development periods.** Are long-run structural conclusions stable to the period supplying the measured slide? | Thirty-year runs with alternative-period drift are listed as unrun | Reversal or large instability would make Figure 4 a period-conditioned scenario rather than a stable empirical characterization. Use declared periods, with no selection of the best match. SI §6 |
| **5** | **R10 stability and completion.** Are the excess-slide attribution and prior-decliner ordering stable across seeds, starts, cutoffs, and both Reddit streams? | Existing R10 comparisons use five seeds; the comments model-with-drift comparison is missing. Two written expectations fail at least partly | Unstable or substantially smaller attribution would require softening the 40–60% emphasis. Before strengthening head-related claims, use at least 20 seeds and bands; retain the original failed expectations. SI §§7 and 11 |
| **6** | **Measurement and period triangulation.** Does persistence survive appropriate count, karma, per-item, factor, and collection-era comparisons? | Much is done: counts carry the effect; per-item engagement contributes little; platform adjustment differs by stream; post-surge and pre-capture windows are reported | Dependence on one measurement channel or collection period would narrow the finding. No available development comparison excludes both historical concerns simultaneously. Consolidate existing evidence in SI §§1 and 7; do not imply the unrun capture audit is complete |
| **7** | **Membership, permanent rank, and universe sensitivity with a fixed estimand.** Are conclusions artifacts of inclusion or normalization? | Prefix-only membership, permanent-rank rules, and several cutoff sensitivities already exist | Material changes in signs or model ordering would restrict population claims. Compare fixed absolute ranks separately from fixed fractions of \(K\); algebraic invariance at a fixed rank is not independent robustness evidence. SI §§2, 5, and 7 |
| **8** | **Dependence-aware uncertainty for model differences.** Are conclusions supported by independent temporal information rather than many overlapping windows? | Multi-seed bands, paired geometry simulations, and some joint entity/start resampling exist; a fully calibrated temporal uncertainty analysis is not established | Wide intervals encompassing materially different interpretations would require “inconclusive” cells. Validate temporal resampling on simulated panels of the observed length and distinguish it from seed variability. SI methods accompanying the atlas |
| **9** | **A common forecast audit of movement, head outcomes, tails, and exits.** Is apparent adequacy confined to pooled survivor movement? | Gate and corrected retention/head comparisons exist; the current model’s thin tails are disclosed. A common forecast panel for new rivals is missing | A simpler rival matching structure while improving held-out outcomes would change the model’s comparative status. If all models miss the head, retain that shared failure. Keep the adoption gate unchanged; add supplementary outcomes in SI §6 |

The existing current-model two-by-two comparison already addresses an important ablation question. I would not prioritize a large new factorial ladder above these checks. If the ladder becomes a publication figure, then interaction and ordering checks become necessary for the ingredient-contribution claims made by that figure.

## 7. Minimal revision that captures most of the benefit

The smallest useful version is primarily editorial and can be built from existing outputs.

**1. Reorganize SI §9 into a bounded model atlas.** Replace the present mixture of table and model-specific paragraphs with:

- A compact manifest: mechanism, fitted inputs, initialization, drift closure, observation rule, seeds, and purpose.
- A quantitative diagnostic table covering the existing worlds, with unrun cells explicit.
- A separate block for the current-model ablations and forecasts, cross-referencing SI §6.
- A short statement of the strongest missing competitors.

Keep the five-seed positive controls and R10 analyses visibly separate from the 20-seed headline null-world comparisons. This should require roughly one or two additional SI pages if existing prose is consolidated, rather than duplicated.

**2. Add one sentence near the end of the Introduction.** Proposed wording:

*We use a common set of diagnostics to distinguish patterns shared by competing growth processes from patterns that require additional structure, and report the limitations of each evaluated model.*

This states the comparative strategy without announcing a new genre of paper.

**3. Make a small replacement in Results, “Where the hierarchy comes from.”** Introduce the model as a constructive account under measured conditions. Keep the flatter hierarchy, limited movement advantage, and missed head decline adjacent to its successes. Replace some of the numerical forecast detail with a reference to the SI atlas; the present paragraph is carrying too many numbers.

**4. Preserve Figure 4’s scientific work.** Keep the construction and removal experiments. Retain its current comparison panel unless a compact quantitative matrix can replace it without losing the actual movement evidence. Do not add a sixth main figure. The historical ladder should remain in the SI or linked explainer.

**5. Update SI §11.** Add R9/R10 chronology and R10’s failed expectations, distinguish the subsequent attribution calculation, and link the complete movement ledger. Readers should be able to recover all eligible comparisons without treating every historical debugging run as a model.

**6. Add one short Discussion qualification.** State that persistent growth differences and heterogeneous common-factor exposure remain viable explanations, and that current comparisons do not uniquely identify a mechanism.

The smallest worthwhile new scientific comparison is the calibrated heterogeneous-growth/common-factor pair. It is more valuable than expanding the roster with several additional homogeneous models. A prospective comparison on sealed data is a separate, owner-authorized next stage.

The paper can therefore retain its generative centerpiece while making the scientific question—and the evidence that survives comparison—the organizing principle.

## Search log

**19 October 2026.** Used live web search, publisher pages, PubMed/PMC, and author or institutional PDFs. Main queries covered CMIP6 experimental design; marine ecosystem ensembles; Cramer’s COVID-19 forecast comparison; Silberzahn and Breznau many-analyst studies; multiverse and specification-curve methods and critiques; power-law alternatives and reception; neutral–niche equivalence; Levins; Bayesian averaging and stacking; heterogeneous growth; and Simon’s reinforcement model. Local reading compared the v4 manuscript, requested experiment records, and canonical status entries. No new empirical analysis or sealed-data access was performed.