<!-- Provenance: Task 2 (stochastic-model methods) report by GPT-6 Astra (gpt-6-astra) (Codex CLI 0.163.0, codex --search exec, read-only sandbox, reasoning effort xhigh), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

**Access check.** I read both assignment briefs completely; the main manuscript, SI, and bibliography at authoritative commit `42955ac`; the round-five synthesis; relevant sections of `PROGRAM_STATE.md`; and `MODEL_STATUS.md` §2z-dk. I also read the repository instructions and applicable research and record-authoring skills. I used local file reads, live web search, and public HTML/PDF retrieval. I did not inspect the rendered manuscript PDFs or figure images, so this is not a visual-layout review. Some publisher endpoints were unavailable; author manuscripts or abstracts were used and identified below. For Grilli, relevant full-text sections were available through indexed publisher/PMC content despite direct-access failures. No raw or sealed data were opened, no analyses were run, and no files were written.

**Model/interface/tools/date:** GPT-6 Astra, as identified in the assignment; no more specific build identifier available. Codex CLI; read-only shell, live web search, HTML/PDF text retrieval. **19 October 2026.**

# Stochastic models of rank and size: what this manuscript should learn from the methods tradition

The strongest position for this paper is an **empirically disciplined account of how concentration and mobility coexist**, followed by a test of which temporal behaviors its stochastic description cannot reproduce. The stationary balance supplies the organizing mathematics. The empirical contribution depends on measuring its ingredients, establishing where its approximations work, and showing what additional observations discriminate among explanations.

The closest literature makes three demands. First, distinguish an accounting relation from a model prediction. Second, judge a model jointly on distributional shape and individual dynamics. Third, treat persistent differences between named entities as an identification problem: fixed growth differences, slowly varying growth conditions, and unequal exposure to common shocks can produce similar observations.

The most consequential missing references are **Fernholz, Ichiba, and Karatzas’s second-order market model; Mallmin, Traulsen, and De Monte’s 2026 ecology paper; Itkin and Larsson’s calibrated market model; and Kalyuzhny, Kadmon, and Shnerb’s environmentally varying neutral model**. These change the interpretation or the appropriate comparison, rather than merely extending the bibliography.

# 1. What these models are good for

## Scientific uses and their limits

| Scientific use | Especially useful example | What the model delivers—and what it does not |
|---|---|---|
| Explain a stationary size distribution | Gabaix’s city-size analysis; rank-based market models | Connects distributional shape to growth and a stabilizing boundary or restoring mechanism. A matching tail does not identify a unique underlying process. |
| Decompose aggregate change | Gomez’s wealth-share accounting | Separates changes among incumbents from changes in membership and population composition. This establishes contributions to an accounting quantity, not causes of those contributions. |
| Explain transition speeds | Gabaix, Lasry, Lions, and Moll | Uses dynamics to discriminate between models that can share similar stationary distributions. Particularly relevant when a model produces the hierarchy but misses its historical change. |
| Infer latent components | Guvenen’s heterogeneous income profiles | Uses covariance across time to distinguish components of growth. Identification depends on horizon, observation error, and restrictions on persistent heterogeneity. |
| Detect departures from a null | Cultural-transmission and ecological neutral models | Converts a verbal claim about randomness into a falsifiable process. Rejection concerns the specified null, including its updating and observation rules. |
| Link microscopic variation to aggregate volatility | Riccaboni and colleagues | Connects firm growth fluctuations to the composition of firms from constituent units. The empirical strength comes from observing those constituents, not simply fitting an exponent. |
| Describe stable structure with changing identities | Second-order finance models; Mallmin and colleagues | Separates persistence of the distribution from persistence of named entities. Aggregate stationarity need not imply equal long-run opportunities. |
| Forecast trajectories | Wang, Song, and Barabási | Produces predictions from a specified early observation window and exposes departures such as late revivals. In-sample curve collapse alone is not forecasting evidence. |
| Infer population patterns through incomplete observation | Grilli | Adds an observation model between latent abundance and measured counts. This is especially valuable when absence can reflect sampling rather than disappearance. |
| Compare systems compactly | Iñiguez and colleagues; Mallmin and colleagues | Reduces many measurements to common dynamic coordinates. Comparability requires aligned observables; identical mathematical notation does not make stocks, flows, and ranks interchangeable. |

The linked, access-labelled references for these examples appear in the reading list below.

Three distinctions matter especially here.

**A minimal mechanism is different from a minimal fitted model.** A short stochastic equation can depend on many estimated rank profiles, latent-state parameters, corrections, and initial conditions. The manuscript should report both its small number of conceptual ingredients and the empirical flexibility used to instantiate them. Neither count replaces the other.

**A model intervention is different from an identified platform intervention.** Removing volatility while holding all other fitted ingredients fixed answers a coherent question about the simulator. It does not establish what would happen if a platform altered recommendations, moderation, or incentives: those actions could change several coefficients and the observation process simultaneously.

**A latent persistent component is different from persistent quality.** A lasting advantage might reflect subject matter, audience composition, institutional resources, accumulated visibility, or exposure to shared events. Calling the component quality would add a causal and evaluative interpretation that these panels do not identify.

The current paper is therefore strongest at measurement, decomposition, constrained simulation, and identifying inadequacies of homogeneous growth. Forecasting and causal intervention remain more demanding uses. The documented absence of an improvement in held-out movement should determine how prominently forecasting enters the paper’s contribution statement. The relevant record is [MODEL_STATUS §2z-dk](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md).

## Annotated reading list

The tiers indicate the amount of engagement needed, not prestige. **Tier 1** changes a claim or an essential comparison; **Tier 2** supplies a useful method or presentation example; **Tier 3** supplies background. “Already cited” refers to the bibliography at `42955ac`. Access labels apply to the version actually consulted.

### Tier 1 — must engage

**1. Fernholz, R., Ichiba, T., and Karatzas, I. (2013). “A second-order stock market model.” *Annals of Finance* 9, 439–454. [DOI](https://doi.org/10.1007/s10436-012-0193-2); [author manuscript](https://arxiv.org/pdf/1302.3870). [full text; author manuscript]. Missing.**

This is the closest direct predecessor for distinguishing rank-dependent dynamics from persistent differences between names. Its first-order benchmark reproduces stable market structure while imposing a symmetry of long-run rank occupation; second-order coefficients relax that symmetry. The actionable connection is to the persistence section and SI alternatives. The paper should explain whether its proposed refinement concerns name-specific drift, changing growth states, or both. A reduced heterogeneous-growth comparison is appropriate; estimating the full occupation matrix described in this article would be excessive for these panels.

**2. Mallmin, E., Traulsen, A., and De Monte, S. (2026). “Fluctuating Growth Rates Link Turnover and Unevenness in Species-Rich Communities.” *Ecology Letters* 29, e70333. [DOI and published full text](https://doi.org/10.1111/ele.70333). [full text]. Missing.**

This recent paper directly connects uneven abundance with turnover using temporally correlated growth, competition, and immigration. It develops dimensionless coordinates and examines unequal long-run growth conditions. Its §3.6 is particularly relevant: aggregate abundance patterns can remain similar while named entities acquire persistent rarity or dominance. This is a close conceptual competitor, although it does not test the digital-attention data. Cite it in the Introduction’s connection between stable hierarchy and changing occupants, and in the Discussion’s treatment of persistence.

**3. Itkin, D., and Larsson, M. “Calibrated rank volatility stabilized models for large equity markets.” arXiv:2403.04674; listed as forthcoming in *Finance and Stochastics* on the author’s research page. [Manuscript](https://arxiv.org/pdf/2403.04674); [publication-status source](https://daviditkin.com/research). [full text; preprint]. Missing.**

This is an unusually close empirical model-comparison reference: volatility, collisions, capital-distribution curves, and individual trajectories are assessed together, including an out-of-sample period. Its calibration explicitly exposes a trade-off between fitting collisions and fitting distributional shape. That makes it useful for presenting the manuscript’s structure–movement trade-off without implying that every objective can be satisfied simultaneously. Its tuning procedure is not the project’s independent-moment procedure; emulate the transparent accounting of compromises, not its calibration rule.

**4. Kalyuzhny, M., Kadmon, R., and Shnerb, N. M. (2015). “A neutral theory with environmental stochasticity explains static and dynamic properties of ecological communities.” *Ecology Letters* 18, 572–580. [DOI](https://doi.org/10.1111/ele.12439); [author preprint](https://arxiv.org/pdf/1406.2926). [full text; preprint under an earlier title]. Missing.**

The critical distinction is between demographic neutrality and equal long-run fitness under fluctuating environments. Time-correlated environmental variation can generate episodes of sustained growth and decline without permanent superiority of particular species. This qualifies what positive gapped growth correlation rejects. Add it to the SI null-world discussion and use an analogous colored-growth process as one candidate comparison. The paper’s calibration is not wholly independent of the fluctuation statistics it seeks to reproduce, so it should not be cited as an unqualified template for independent calibration.

**5. Gabaix, X., Lasry, J.-M., Lions, P.-L., and Moll, B. (2016). “The Dynamics of Inequality.” *Econometrica* 84, 2071–2111. [DOI](https://doi.org/10.3982/ECTA13569); [full text](https://xgabaix.scholars.harvard.edu/sites/g/files/omnuum7761/files/xgabaix/files/dynamics_of_inequality.pdf). [full text]. Already cited; deepen engagement.**

The central methodological lesson is to turn a stationary model’s dynamic failure into the scientific question. The paper derives restrictions on transition speeds, confronts them with observed change, and studies parsimonious departures involving scale and growth types. This is the best template for handling the submissions-head decline. The relevant question is which measured temporal feature prevents the baseline from following concentration changes, rather than whether a long simulation eventually produces a recognizable hierarchy.

**6. Gomez, M. (2023). “Decomposing the Growth of Top Wealth Shares.” *Econometrica*. [DOI](https://doi.org/10.3982/ECTA16755); [author manuscript](https://www.matthieugomez.com/files/topshares.pdf). [full text; author manuscript]. Already cited; deepen engagement.**

This is the most important methodological anchor for separating accounting, diffusion approximations, and empirical contributions. It also develops mobility and first-passage implications in §6; those should not be treated as an unexplored conceptual extension. Appendix C.1 explicitly considers heterogeneous growth types and distinguishes their finite-horizon variance contribution from instantaneous diffusion. Engage these parts in the SI’s balance derivation and in the proposed variance-recovery checks. Preserve the distinction between wealth-share arithmetic and the manuscript’s mean-log quantities.

**7. Guvenen, F. (2009). “An empirical investigation of labor income processes.” *Review of Economic Dynamics* 12, 58–79. [DOI](https://doi.org/10.1016/j.red.2008.06.004). [abstract only]. Already cited; deepen engagement.**

The verified abstract addresses the distinction between heterogeneous income profiles and highly persistent shocks. It directly supports treating growth heterogeneity as an identification issue, rather than mentioning it only as a parallel from earnings research. The manuscript should make that distinction explicit around Fig. 5. Because I could not retrieve the original full text in this session, I do not rely on its detailed estimators or numerical findings; those should be checked before importing a specific procedure.

### Tier 2 — should cite or emulate where the comparison is used

**8. Grilli, J. (2020). “Macroecological laws describe variation and diversity in microbial communities.” *Nature Communications* 11, 4743. [DOI](https://doi.org/10.1038/s41467-020-18529-y); [PMC](https://pmc.ncbi.nlm.nih.gov/articles/PMC7506021/). [full text; relevant indexed sections]. Missing.**

The useful architecture combines regularities in abundance fluctuations with an explicit sampling layer and predictions about additional observables. It offers a model for discussing missingness and latent activity. Some heterogeneity enters through the distribution of mean abundances; the paper should not be read as deriving every aspect of the abundance distribution from identical entities.

**9. Descheemaeker, L., and de Buyl, S. (2020). “Stochastic logistic models reproduce experimental time series of microbial communities.” *eLife* 9, e55650. [DOI](https://doi.org/10.7554/eLife.55650); [full text](https://pmc.ncbi.nlm.nih.gov/articles/PMC7410486/). [full text]. Missing.**

A relatively simple stochastic description can reproduce several temporal statistics even when detailed interactions are not modeled explicitly. This supports presenting the attention model as an effective description. It also cautions against identifying a unique microscopic mechanism from successful temporal fits. Relevant location: the Discussion’s account of what the model explains.

**10. Mallmin, E., Traulsen, A., and De Monte, S. (2024). “Chaotic turnover of rare and abundant species in a strongly interacting model community.” *PNAS* 121, e2312822121. [DOI](https://doi.org/10.1073/pnas.2312822121); [author version](https://arxiv.org/pdf/2306.11031). [full text; author version]. Missing.**

Deterministic interactions can generate irregular turnover that admits a useful stochastic description at the level of individual entities. This is a valuable PNAS example of moving between a richer process and a reduced description. Its relevance is interpretive: effective stochastic noise need not be external noise. It provides no evidence that digital communities exhibit ecological chaos.

**11. Evans, T. S., and Giometto, A. (2011). “Turnover Rate of Popularity Charts in Neutral Models.” [Stable preprint](https://arxiv.org/abs/1105.4044). [full text]. Missing.**

Turnover depends on the population update rule and parameter regime; a rule successful for one neutral process need not transfer to another. Cite in the SI specifying neutral copying. Align definitions carefully: counting both entering and departing items differs from the manuscript’s fraction of an initial cohort that leaves.

**12. Acerbi, A., and Bentley, R. A. (2014). “Biases in cultural transmission shape the turnover of popular traits.” *Evolution and Human Behavior* 35, 228–236. [DOI](https://doi.org/10.1016/j.evolhumbehav.2014.02.003); [author PDF](https://acerbialberto.wordpress.com/wp-content/uploads/2014/11/ehb2014.pdf). [full text]. Missing.**

Turnover across list sizes becomes a diagnostic for competing transmission processes. This is closer to the manuscript’s turnover question than a generic appeal to cumulative advantage. Use it to motivate checking a profile of observables. A profile that distinguishes the tested models still need not identify a unique psychological mechanism.

**13. Riccaboni, M., Pammolli, F., Buldyrev, S. V., Ponta, L., and Stanley, H. E. (2008). “The size variance relationship of business firm growth rates.” *PNAS* 105, 19595–19600. [DOI](https://doi.org/10.1073/pnas.0810478105); [full text](https://pmc.ncbi.nlm.nih.gov/articles/PMC2604942/). [full text; relevant indexed sections]. Already cited.**

This is the strongest methodological comparison for size–variance scaling because it connects aggregate growth to observed constituent units. Its lesson is to measure aggregation mechanisms when possible. A similar exponent in attention and firms would be weaker evidence than a verified relationship between participation, constituent variation, and aggregate fluctuations.

**14. Iñiguez, G., Pineda, C., Gershenson, C., and Barabási, A.-L. (2022). “Dynamics of ranking.” *Nature Communications* 13, 1646. [DOI](https://doi.org/10.1038/s41467-022-29256-x); [full text](https://www.barabasi.com/media/I-iguez_et_al-2022-Nature_Communications.pdf). [full text]. Already cited.**

Its strength is reducing many ranking systems to a compact dynamic model and common observables. It is a useful rank-dynamics benchmark, but a rank-only model should not be penalized for lacking an activity-size distribution that it does not claim to predict. Comparisons belong on shared outcomes with matched definitions.

**15. Baker and Smyth (2026). “Traveling Waves in Equity Markets with Rank-Based Entry and Exit.” [Stable preprint](https://arxiv.org/abs/2608.27156). [full text]. Already cited.**

This directly connects rank-based population dynamics, entry and exit, and distributional shape. Distinguish its forward constructions from inverse drift calibration. Also distinguish entry/exit turnover from reranking within an existing population. The smallest useful addition is a row in the SI comparison table identifying those differences, rather than another paragraph of general genealogy.

**16. Wang, D., Song, C., and Barabási, A.-L. (2013). “Quantifying Long-Term Scientific Impact.” *Science* 342, 127–132. [DOI](https://doi.org/10.1126/science.1237825); [author-hosted article](https://www.barabasi.com/media/pub_imports/files/434.pdf). [full text]. Missing.**

Preferential attachment, aging, and fitted fitness generate temporal trajectories and a rescaled representation. The useful presentation lesson is to separate fitting, collapse, later prediction, and acknowledged failure modes. Cite only if scientometric forecasting or temporal collapse is discussed; it need not occupy scarce main-text space.

**17. Luttmer, E. G. J. (2011). “On the Mechanics of Firm Growth.” *Review of Economic Studies*. [DOI](https://doi.org/10.1093/restud/rdq028); [earlier working paper](https://www.mcgill.ca/economics/files/economics/luttmer.pdf). [full text; earlier working-paper version]. Missing.**

Firm ages provide an additional constraint that size distributions alone cannot supply. This suggests using observed time since entry into the tracked attention universe to constrain growth models, while acknowledging that this is not necessarily community age. The published version should be consulted before borrowing detailed quantitative results from the earlier manuscript.

**18. Newberry, M. G., Ahern, C. A., Clark, R., and Plotkin, J. B. (2017). “Detecting evolutionary forces in language change.” *Nature* 551, 223–226. [DOI](https://doi.org/10.1038/nature24455); [earlier preprint](https://arxiv.org/pdf/1608.00938). [full text; earlier preprint]. Missing.**

The useful comparison is methodological: evaluate trajectories against a specified stochastic null, taking the observation process seriously. Visually directional change need not establish selection. This belongs, if anywhere, in an SI discussion of null testing rather than in the main literature paragraph.

### Tier 3 — useful background

**19. Gabaix, X. (1999). “Zipf’s Law for Cities: An Explanation.” *Quarterly Journal of Economics* 114, 739–767. [DOI](https://doi.org/10.1162/003355399556133); [author PDF](https://xgabaix.scholars.harvard.edu/sites/g/files/omnuum7761/files/xgabaix/files/zipfs_law.pdf). [full text; relevant theory]. Already cited.**

Use for the conditional link between random growth, stabilization, and a stationary tail. Proportional growth alone does not guarantee the desired stationary distribution. Its discussion also provides an accessible route into the recurrence-based random-growth tradition.

**20. Simon, H. A. (1955). “On a Class of Skew Distribution Functions.” *Biometrika* 42, 425–440. [DOI](https://doi.org/10.1093/biomet/42.3-4.425); [full text](https://snap.stanford.edu/class/cs224w-readings/Simon55Skewdistribution.pdf). [full text; relevant model sections]. Missing.**

Useful for defining innovation-plus-reinforcement baselines precisely. A cumulative-count process needs an additional aging or observation construction before it becomes a model of weekly activity flows. Add only if that baseline is actually developed or contrasted.

# 2. The anatomy of the strongest articles

The recurring structure is not simply stylized facts, an equation, and a simulation. It is a sequence in which **each step creates a constraint the next step must satisfy**.

## Begin with a joint fact that a familiar explanation cannot accommodate

The strongest motivation combines observations rather than displaying a heavy tail by itself. Gabaix and colleagues begin from a conflict between plausible stationary models and observed transition speed:

> “generate transition dynamics that are too slow relative to those observed in the data.”

—Gabaix et al., p. 2071. **[verified: [full text](https://xgabaix.scholars.harvard.edu/sites/g/files/omnuum7761/files/xgabaix/files/dynamics_of_inequality.pdf)]**

The surrounding argument concerns canonical random-growth models and the rapid change in top inequality. The manuscript should emulate this structure by making the failure to follow the submissions-head decline scientifically central. A stationary balance and a finite-time historical transition are different tests.

## State what the minimal model necessarily implies

Fernholz and colleagues motivate additional complexity by identifying a specific implication of their first-order model:

> “each stock asymptotically spends equal average time at each rank”

—Fernholz, Ichiba, and Karatzas, author manuscript p. 1. **[verified: [full text](https://arxiv.org/pdf/1302.3870)]**

This statement concerns the symmetric first-order setting described there, not every rank diffusion. The lesson is to derive the baseline’s implication for named communities before presenting a richer fit. Which observation requires another component? Positive growth correlation is useful only after specifying which processes could produce it.

## Keep identities, closures, and independent predictions visibly separate

Gomez’s article distinguishes an accounting decomposition from its behavior under random-growth assumptions and from its empirical application. The attention manuscript should use the same separation in Eqs. 2–3 and the generative section. A parameter obtained by requiring a training ladder to remain stationary cannot then provide independent evidence that the same ladder is explained. A measured-drift construction has a different evidentiary role. [Gomez, author manuscript](https://www.matthieugomez.com/files/topshares.pdf)

Independent moments need not come from a different platform, but they must impose a restriction not already enforced by the calibration. A table can make that distinction more economically than several prose qualifications.

## Expose compromises between scientific targets

Itkin and Larsson describe their calibration trade-off directly:

> “a small value of λ provides a good fit to the collisions and poor fit to the capital distribution curve”

—Itkin and Larsson, manuscript p. 16, §4.4. **[verified: [full text](https://arxiv.org/pdf/2403.04674)]**

The manuscript’s analogous targets are hierarchy, endpoint movement, and parsimony. A figure showing all three is more informative than declaring one model successful because it resembles the ladder. Parameter uncertainty and simulation variability should remain distinct.

## Predict something the model was not built to reproduce

Grilli’s observation layer connects latent abundance assumptions to additional measured patterns. Riccaboni and colleagues use constituent information to constrain aggregate fluctuations. These are stronger templates than fitting a size–variance curve and then celebrating the same curve as a prediction. For this manuscript, suitable additional targets include a gap-dependent covariance profile, return after departure from a top set, or concentration changes over a withheld development interval. [Grilli](https://doi.org/10.1038/s41467-020-18529-y); [Riccaboni et al.](https://doi.org/10.1073/pnas.0810478105)

## Use collapse and phase diagrams only after specifying their evidentiary role

A normalization can reveal a compact relationship, but fitted rescaling can also absorb discrepancies. Distinguish an algebraic rearrangement, a fitted empirical collapse, and a prediction using previously estimated parameters. Mallmin’s dimensionless ecological regimes and Iñiguez’s cross-system ranking comparisons are useful templates for organizing variation; neither licenses universal claims from two digital platforms. [Mallmin et al. 2026](https://doi.org/10.1111/ele.70333); [Iñiguez et al.](https://doi.org/10.1038/s41467-022-29256-x)

## Make failure part of the model’s specification

Wang and colleagues introduce their limitations explicitly:

> “The proposed model has obvious limitations:”

—Wang, Song, and Barabási, p. 132. **[verified: [full text](https://www.barabasi.com/media/pub_imports/files/434.pdf)]**

They then discuss late citation revivals that their model does not capture. The equivalent here is to identify the historical change and temporal dependence the fitted process misses. That disclosure helps define the domain of the model; it should appear alongside its achievements.

# 3. Eight ranked improvements to the draft

These are proposed revisions and analyses, not completed work or an adopted agenda. None requires opening sealed Reddit data. The manuscript can improve substantially through the first, third, fourth, and eighth items before undertaking new simulation work.

## 1. Reorganize the contribution around distinct evidentiary claims

**Location:** final Introduction paragraph; transitions into the balance and generative-model sections; first Discussion paragraph.

**Change:** distinguish:

1. measured coexistence of shape stability and membership change;
2. an accounting relation and diffusion-based approximation;
3. simulation consequences of specified ingredients;
4. temporal observations the homogeneous models fail to reproduce.

The smallest change is one short contribution paragraph and consistent labels in the Results. Avoid treating all four as interchangeable evidence for one mechanism.

**Cost:** editorial; likely a net reduction of 100–200 main-text words. **New data:** none.

## 2. Replace the broad homogeneous-versus-persistent contrast with a small identification comparison

**Location:** persistence Results section, Fig. 5, and SI null worlds.

**Change:** compare three economical additions separately: heterogeneous mean growth, temporally correlated growth conditions, and unequal exposure to common factors. Preserve the existing observation pipeline and use training windows for estimation.

A simple diagnostic illustrates the issue. For an idealized latent process with independent Brownian increments and fixed heterogeneous drift,

\[
\operatorname{Var}(\Delta_h H)
=\sigma^2h+\operatorname{Var}(b)h^2.
\]

Disjoint increments also share the heterogeneous-drift component. A colored growth process can resemble this over short windows but loses covariance as the separating gap grows. Weekly aggregation, transitory levels, and selection alter these expressions, so recovery must be checked through the actual estimator.

**Cost:** moderate analysis; one SI figure comparing horizons and gaps, with at most one replacement main-text panel. **New data:** none. Short panels may leave the alternatives unresolved; that is an informative outcome.

## 3. Add a parameter-and-prediction provenance table

**Location:** SI generative specification, referenced in Methods.

**Change:** for each ingredient, identify its estimating moment, training interval, rank convention, whether the observed ladder enters, and which outputs remain unconstrained. Separate measured-slide drift from drift obtained by a stationarity closure.

Include the transitory and observation components, not just the few parameters highlighted in the main text. Also identify which quantities are reused across figures.

**Cost:** approximately one SI page; modest audit of existing specifications. **New data:** none.

This table would clarify both parsimony and evidentiary independence without changing any default or estimator.

## 4. Write an explicit bridge from continuous rank diffusion to the measured panel

**Location:** SI turnover derivation, balance, and observation-model sections.

**Change:** state which relations are exact within a mathematical model, which are short-horizon approximations, and which are finite-window empirical constructions. Explain the distinction between permanent-rank estimation and any instantaneous rank or state dependence inside the simulator. Address correlated innovations, temporal aggregation, composition changes, and measurement noise in the same bridge.

For entry and exit, identify precisely where each contribution enters, so the decomposition and simulator do not silently count the same effect twice.

**Cost:** about half an SI page plus a targeted recovery check where uncertainty remains. **New data:** none. This is clarification and validation, not a request to replace permanent-rank estimation.

## 5. Make the stability timescale a testable, conditional result

**Location:** balance Results, generative Results, and SI sensitivity analysis.

**Change:** align the linearized relaxation calculation and simulation comparison on the same observable and perturbation. Examine sensitivity to the development-era window used to estimate drift. Distinguish local relaxation around a specified state from convergence after starting at equality.

Retain the comments/submissions contrast. The recorded timescale separation is not uniform across the head of both series. [MODEL_STATUS §2z-dk](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md)

**Cost:** one SI figure or compact table; moderate development-data analysis. Replace some long-horizon exposition rather than expanding it. **New data:** none; a four-year record cannot directly validate a multidecade forecast.

## 6. Add one measure of durable access to attention

**Location:** first Results section or SI, with a brief Discussion connection.

**Change:** supplement endpoint overlap with either return after leaving a top set or cumulative attention captured over a fixed interval. The smallest useful addition would distinguish one-time displacement from recurrent occupation by the same limited set of communities.

Do not interpret annual overlap as uninterrupted residence or invert the annual replacement fraction into a lifetime. Use censoring-aware finite-horizon quantities.

**Cost:** modest analysis; one SI panel and one main-text sentence. **New data:** none, provided existing development panels retain identities and the needed activity measures.

## 7. Present model comparisons by task, uncertainty, and complexity

**Location:** Fig. 4 comparison panel or a replacement SI scorecard.

**Change:** show separate outcomes for distributional shape, individual movement, persistence, and historical aggregate change. Identify which were calibration targets. Include the existing simpler worlds and the small comparison set in item 2 without requiring every model to predict quantities outside its scope.

Preserve all relevant origins and the project’s multi-seed convention; head results need uncertainty bands and at least 20 seeds. Show the flatter simulated hierarchy and lack of improved held-out movement beside the successful demonstrations.

**Cost:** mostly reorganization; additional simulations only for explicitly chosen new comparisons. **New data:** none. This does not alter the adoption gate.

## 8. Fund the revisions by shortening the main-text mechanism catalogue

**Location:** Introduction, the size–variance portion of Fig. 2, and generative-model exposition.

**Change:** retain the geometry comparison that directly addresses turnover, while moving weakly identified exponent detail and secondary rank-movement diagnostics to the SI. Replace several literature sentences with two focused connections: rank-based accounting and heterogeneous or correlated growth.

The main narrative can then proceed through observation, balance, generative consequences, and temporal failure.

**Cost:** editorial, with a target saving of roughly three-quarters to one main-text page. **New data:** none. Decide the precise panel layout after visual inspection, which I did not perform.

# 4. If the model works: the most interesting questions it could answer

The rankings below are judgments about scientific value and practical tractability, not probabilities of success. **Rank 1 is highest.** All proposed analyses use existing development data unless otherwise stated.

| Question | Scientific importance | Feasibility | Broad PNAS fit |
|---|---:|---:|---:|
| What determines how quickly attention concentration changes? | 1 | 3 | 1 |
| Does visible turnover conceal durable control of cumulative attention? | 2 | 1 | 2 |
| How much persistence reflects enduring growth differences rather than temporary conditions? | 3 | 4 | 3 |
| Can platforms be compared by the separation between mobility and structural timescales? | 4 | 2 | 4 |
| Who can reach the top, and how long does the journey take? | 5 | 6 | 5 |
| Why does aggregation fail to smooth attention fluctuations more strongly? | 6 | 5 | 6 |
| Which hypothetical changes in drift or volatility would alter concentration? | 7 | 7 | 7 |

## 1. What determines the speed of concentration change?

**Analysis.** Estimate the balance ingredients in several eligible development windows and evaluate subsequent concentration changes. Compare homogeneous growth with separately estimated heterogeneous-growth and common-factor variants. Examine both the sign and speed of change; predicting eventual flattening is weaker than predicting when appreciable flattening occurs.

**Interesting result.** A low-dimensional, independently estimated persistence component substantially changes the response timescale while leaving short-horizon volatility similar. That would connect individual trajectories to aggregate concentration in a way the stationary curve cannot.

**Risks.** A few calendar episodes may dominate. Different drift windows may yield incompatible long-run implications. The model might reproduce a decline after calibration to that decline without predicting it. The useful outcome could therefore be a bound on what homogeneous diffusion can explain, rather than a successful forecast.

The intellectual template is the transition-speed question in [Gabaix et al.](https://doi.org/10.3982/ECTA13569), applied with the manuscript’s own estimands.

## 2. Does visible turnover conceal durable control of cumulative attention?

**Analysis.** Compare endpoint membership, uninterrupted spells, return probabilities, and the distribution of cumulative attention over fixed windows. For example, measure how much cumulative activity accrues to entities that repeatedly leave and return to a top set.

Use the activity quantity appropriate to each dataset; do not silently equate karma, posting volume, and audience reach.

**Interesting result.** A rapidly changing weekly leaderboard might coexist with concentrated cumulative attention among a relatively restricted set of communities. The opposite would also matter: substantial redistribution of cumulative attention despite a stable rank–size curve.

**Risks.** Observation thresholds can turn continuing low activity into apparent absence. Long spells are censored. With a few years of data, finite-window entrenchment can be measured more credibly than asymptotic opportunity. This distinction is precisely why the name-versus-rank separation in [Fernholz et al.](https://doi.org/10.1007/s10436-012-0193-2) matters.

## 3. How much persistence reflects enduring growth differences?

**Analysis.** Estimate covariance across several increment lengths and separating gaps, using partial pooling for community growth differences and training-only estimation of common exposures. Evaluate recovery under synthetic versions of each candidate process before interpreting estimated components.

**Interesting result.** A substantial share of persistence survives common-factor adjustment and is stable across independent development windows, or the apparent persistent component instead decays on an observable timescale.

**Risks.** A growth state lasting longer than the record is empirically difficult to distinguish from a fixed slope. Estimating and subtracting individual trends from the same short histories can remove genuine stochastic persistence. Any surviving difference remains an effective growth advantage, not identified quality.

This question should be framed using both [Guvenen](https://doi.org/10.1016/j.red.2008.06.004) and the environmentally varying alternative in [Kalyuzhny et al.](https://doi.org/10.1111/ele.12439).

## 4. Can mobility and structural stability be compared on a common scale?

**Analysis.** Explore two descriptive coordinates:

\[
\chi_k(h)=\frac{\sigma_k\sqrt h}{S_k},
\qquad
\Lambda_k=\frac{T_{\mathrm{shape},k}}{T_{\mathrm{membership},k}}.
\]

The first is a local diffusive displacement relative to ladder steepness, under the corresponding approximation. The second compares a consistently defined shape-response timescale with an observed membership timescale. Use restricted finite-horizon measures when a residence median cannot be estimated.

**Interesting result.** Differences in turnover largely align after accounting for geometry, while persistence explains systematic departures. Alternatively, the comments and submissions series occupy distinct regimes despite sharing a platform.

**Risks.** Drift, transitory components, and the observation horizon may defeat the diffusion normalization. Estimated timescales may not be comparable. Two platforms cannot establish universality. Also, rearranging the stationary balance into a dimensionless ratio does not provide another independent test of that balance.

## 5. Who can reach the top, and how long does the journey take?

**Analysis.** Start with conditional first-passage probabilities from well-observed ranks within the tracked universe. Separate upward passage, downward passage, return, and disappearance. Compare model predictions with observed finite-horizon passage frequencies.

**Interesting result.** Similar stationary concentration may coexist with sharply different access to the head, or a small persistent growth difference may have a large effect on upward passage while barely changing the aggregate curve.

**Risks.** Rare top-ten events provide little information. The observed universe excludes many potential entrants, so this cannot estimate everyone’s opportunity to become a leader. Crossing a moving rank threshold is not equivalent to crossing a fixed activity threshold. Gomez already develops related first-passage implications; extend that connection explicitly rather than presenting mobility as an entirely new application. [Gomez, §6](https://www.matthieugomez.com/files/topshares.pdf)

## 6. Why does aggregation fail to smooth attention fluctuations more strongly?

**Analysis.** Where existing derived data contain participation counts, examine how changes in aggregate activity relate to the number of contributing units and shared temporal components. Begin with quantities already available rather than requiring new item-level collection.

**Interesting result.** Larger communities might contain more contributors but also more correlated activity, or activity might be dominated by a small effective number of contributors. Either could help explain weak smoothing with size.

**Risks.** Aggregate counts alone do not identify covariance among contributors. Without the necessary constituent observations, several mechanisms remain observationally equivalent. The attainable result may be a decomposition of aggregate participation effects, not a microscopic explanation. [Riccaboni et al.](https://doi.org/10.1073/pnas.0810478105)

## 7. Which hypothetical coefficient changes alter concentration?

**Analysis.** Map the model’s finite-time response to modest changes in diffusion, restoring drift, and persistence, separately and jointly. Show uncertainty and dependence on calibration vintage.

**Interesting result.** Two changes with similar short-run effects on leaderboard churn could have different long-run effects on concentration. This would clarify which observable responses would be useful to monitor in a future intervention study.

**Risks.** There is currently no identified mapping from platform policy to these coefficients. Such simulations are conditional scenarios. Establishing what an actual recommender or moderation change does would require independent intervention information, a credible design, or new data. This is therefore the lowest-priority immediate extension.

# 5. Threats from the literature and how to preempt them

## The stationary balance may be read as a known consequence presented as an explanation

Random-growth and rank-diffusion traditions already connect stationary shape to growth dispersion and restoring forces. The manuscript should identify the inherited mathematical result and concentrate its empirical claims on measurement, approximation accuracy, and departures.

**Preemption:** label the logical status of each equation; separate forward calibration from inverse closure; state which empirical quantities were not imposed. The balance remains useful even when its formal structure is inherited. [Gabaix 1999](https://doi.org/10.1162/003355399556133); [Gomez 2023](https://doi.org/10.3982/ECTA16755)

## Persistent named differences already belong to the rank-model tradition

A referee familiar with second-order market models will recognize heterogeneous drift as an established way to retain aggregate stability while changing named trajectories.

**Preemption:** acknowledge that lineage beside the persistence result. Define what the digital data establish about the needed temporal component. Avoid allowing a rejection of homogeneous growth to stand in for identification of its replacement. [Fernholz et al.](https://doi.org/10.1007/s10436-012-0193-2)

## Positive growth correlation does not uniquely select permanent heterogeneity

Correlated environmental growth and unequal responses to common shocks offer alternatives. The 2026 ecology paper makes the turnover–unevenness connection especially close.

**Preemption:** present persistent growth as an observed property over specified horizons. Compare a small set of alternatives through their gap and horizon dependence. If the record cannot distinguish them, preserve that uncertainty in the Discussion. [Kalyuzhny et al.](https://doi.org/10.1111/ele.12439); [Mallmin et al. 2026](https://doi.org/10.1111/ele.70333)

## Finite-window growth dispersion need not equal diffusion variance

Gomez’s heterogeneous-type extension explicitly distinguishes drift-generated variance, which enters at order \(h^2\), from diffusion variation at order \(h\). This is consequential when persistence is itself the empirical finding.

**Preemption:** test recovery of the manuscript’s volatility estimator under heterogeneous and correlated growth using its actual aggregation and selection rules. Report any resulting limitation on the interpretation of crossing gains. This is a proposed check, not evidence that the present estimator is wrong. [Gomez, Appendix C.1](https://www.matthieugomez.com/files/topshares.pdf)

## A good stationary curve can coexist with inadequate dynamics

The relevant literature evaluates transition speed, collisions, individual trajectories, or additional moments precisely because distributions are insufficient discriminators.

**Preemption:** make the flatter simulated hierarchy, lack of improved held-out movement, and missed submissions decline visible in the same comparison that presents successful simulations. Preserve the two failed written expectations in the persistence analysis. These recorded limitations determine which uses of the model are supported. [Itkin and Larsson](https://arxiv.org/pdf/2403.04674); [MODEL_STATUS §2z-dk](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md)

## Rejection of one copying process does not reject neutrality as a family

Update rules, innovation, correlated environments, and sampling can materially change neutral predictions.

**Preemption:** name the exact process rejected and list its defining assumptions in the SI. Compare matched quantities and horizons. Avoid interpreting failure of that baseline as evidence for a particular social transmission bias. [Evans and Giometto](https://arxiv.org/abs/1105.4044); [Acerbi and Bentley](https://doi.org/10.1016/j.evolhumbehav.2014.02.003)

## Effective stochastic success does not identify the source of fluctuations

An effective stochastic description can summarize variation arising from unmodeled deterministic interactions, changing environments, or sampling.

**Preemption:** retain the model’s descriptive and conditional interpretation. Name the measurements that would be needed to connect its coefficients to specific platform processes. The ecological literature provides a concrete demonstration of why this distinction matters, without establishing the corresponding mechanism online. [Mallmin et al. 2024](https://doi.org/10.1073/pnas.2312822121)

# Search log

**Date:** 19 October 2026.

**Local sources:** the two task briefs; authoritative manuscript, SI, and bibliography at `42955ac`; round-five synthesis; program orientation and the relevant canonical results section; applicable repository instructions and skills.

**Public sources and tools:** general live web search; publisher pages; arXiv; PMC; university and author-hosted manuscripts; author publication lists. This was a targeted methods review, not a systematic database review.

**Main searches:** second-order and hybrid Atlas models; calibrated rank volatility models; dynamics of inequality and growth-type heterogeneity; wealth-share decomposition and first passage; environmental stochasticity and ecological neutrality; stochastic logistic abundance models; chaotic turnover; neutral popularity-chart turnover; cultural transmission and turnover; firm size–variance scaling; citation aging and fitness; stochastic tests of language change.

**Recent-work checks:** verified the published 2026 *Ecology Letters* article, the author-listed status of Itkin and Larsson’s manuscript, and the available Baker–Smyth preprint. Quotations were checked against retrieved full text. Proposed analyses, revised comparisons, and manuscript edits were not executed.