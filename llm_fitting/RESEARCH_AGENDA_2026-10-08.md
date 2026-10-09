# Rank-diffusion: project assessment and next research agenda

2026-10-08 · Planning and literature synthesis, not a registered protocol or an adopted model change.

## Assessment against the three scientific aims

The program has a credible, empirically disciplined model of heterogeneous attention dynamics. It has not yet established the complete conjunction of structural explanation, useful individual prediction, and minimal sufficient complexity. The best next investment is to distinguish deficiencies in estimation and forecasting from genuinely missing dynamics, then simplify the model under those stronger checks.

The evidence below comes from the dated sections of [MODEL_STATUS.md](MODEL_STATUS.md), especially §§2z-g, 2z-q, 2z-ac, and 2z-ae. The latest substantive results are from July 16–17; the July 18 Instagram document is a subsequent design plan. Older agendas and favorable scorecards must not displace these later findings.

| Aim | Established progress | Remaining gap |
|---|---|---|
| Stable rank-size structure | The simulator can maintain much of an observed ladder while identities move. The permanent–transitory architecture provides a plausible separation of slow positions and rapid fluctuations. | Entity homes are seeded from the observed ladder. This is conditional maintenance, not an independent derivation of its shape. Head concentration is not consistently reproduced across instruments and metrics. |
| Individual movement | Several pooled mobility statistics transport; the registered comments extension produced a particularly strong frozen-parameter result. Persistent amplitude heterogeneity and approximate amplitude collapse have substantial support. | The main gate scores pooled movement moments, largely conditional on survival. Proper-score skill is approximately zero in the recorded comparisons. Useful endpoint-specific predictive distributions have not been demonstrated. Established entities leave high ranks too infrequently in simulation. |
| Simplicity | A small set of latent components has survived extensive falsification and independent measurement constraints. | Roughly 500 moment-estimated band values over about 55 knots, empirical homes, and numerous estimation/selection choices remain. A simple diagram of components is not yet a demonstration of a parsimonious fitted model. |

### The current model and what its evidence means

After removing a common period factor, log activity is a slowly reverting entity level, a fast transitory AR component, optional medium-timescale variation on long panels, and observation noise. Persistent entity amplitude heterogeneity supplies much of the variation in volatility. Parameters are estimated by permanent rank, with a separate observation-noise instrument and long-horizon moments constraining the decomposition. There is bottom-boundary exit/rebirth machinery.

The attractive empirical result is that one amplitude can approximately scale variation across horizons: a potentially compressive law, not just another source of flexibility. However, amplitude implementation differs across simulator stacks and flags. The agenda must audit actual scaling of permanent, transitory, and observation components rather than treating the conceptual b=1 law as uniformly implemented.

The evidence is mixed, with several distinct meanings of “pass”:

| Evaluation | Recorded movement result | Interpretation |
|---|---|---|
| Facebook Era A, NNLS primary, five rolling splits (§2z-g) | Model relative error 0.123 ± 0.033; historical-mobility baseline 0.145 ± 0.031; median-in-CI coverage 60% | Modest pooled-moment advantage; wins on four of five splits. |
| Earlier Reddit comments, five splits (§2z-g) | 0.171 ± 0.046 versus 0.165 ± 0.062; coverage 60% | At par within the registered tolerance, not a point-estimate improvement. |
| Earlier short submissions panel, five splits (§2z-g) | 0.164 ± 0.053 versus 0.168 ± 0.004; coverage 80% | Essentially at par. |
| Registered comments extension (§2z-q) | 0.032 versus 0.171, frozen parameters | Strong success on the predeclared 34-week future block. This is the protocol's registered single-block evaluation, not five independent rolling replications. The complete confirmation battery was **MIXED EVIDENCE**, because parameter transport failed. |
| Long submissions panel, five splits (§2z-ae) | 0.194 ± 0.061 versus 0.163; coverage 80% | Passes the +0.05 tolerance, but does not beat the baseline. CRPS skill is approximately zero. |

The ± values in the rolling evaluations summarize variation across splits, not confidence intervals for a generalization advantage. The code's “persistence” comparison here is a train-window historical-mobility distribution, not simply a forecast of unchanged individual rank. “Coverage” in this table means the simulated median lies inside an empirical bootstrap confidence interval. It is not coverage of endpoint predictive intervals. These distinctions materially limit the forecasting claim.

The comments confirmation also underpredicted upper displacement tails: empirical p90 values 27/41/65 versus simulated 25/35/51 at the recorded horizons. The amplitude-spread and reversion-parameter transport checks failed even though the movement gate passed. Subsequent exploration makes that extension development evidence for future work; it cannot become a fresh confirmation set again.

### The most informative unresolved residual

The established-departure deficit survived repeated corrections to population selection, identity tracking, and survivor conditioning:

- Comments (§2z-ac, 30 simulation seeds): established top-K/2 departure rate 0.00800 per week empirically versus 0.00350 in simulation, with simulated seed q10–q90 [0.00283, 0.00410]; at K/4, 0.00172 versus 0.00050 [0.00028, 0.00077].
- Long submissions (§2z-ae): 0.00851 (empirical week-block CI [0.00746, 0.00968]) versus 0.00311 (simulated seed q10–q90 [0.00214, 0.00408]) at K/2, an empirical/simulated ratio of 2.74; at K/4, 0.00302 versus 0.00092. Seed quantiles and empirical confidence intervals summarize different uncertainties.

These are mostly crossings by extant identities, not empirical permanent disappearances. Simulated permanent deaths form a much larger share of its already scarce established departures. “Returnable” must not be silently converted to “observed to return”: finite follow-up and right censoring still matter.

Keep four problems distinct: too few established departures; too many crossings by transient spikers in some current-rank cohorts; temporary absence versus permanent rebirth; and head concentration. Reducing all fast volatility could help the second while worsening the first. Empirical within-entity standardized tails were already found to be fatter in powered strata; “thin the t tails” is not an adequate general diagnosis.

The head-share excess occurred on Facebook and the comments extension, but did not occur on long submissions, where the point-estimate sign reversed. That establishes heterogeneity. It does **not** isolate Facebook measurement as the cause. The stronger causal wording at the end of §2z-ae should be treated as an interpretation requiring a discriminating test, not as a measured identification result.

Other boundaries remain important. The monotone rise in amplitude spread did not replicate on submissions; the functional share of its variance-ratio residual also missed its registered range. Instagram P10/P11 were not adjudicated because the observation unit and reconciliation target were defective. The [July 18 allocation plan](IG_JOINT_POST_ALLOCATION_PLAN_2026-07-18.md) is an unexecuted design, not evidence that collaboration shares have been identified. Wikipedia remains the planned distinct-platform confirmation target, subject to an exposure audit of the existing acquisition pilot.

## What transfers from the x-collection review

I used the October 7 reply-tree design review and x-collection consensus as a methodological starting point. Their strongest transferable lessons are to separate a general model family from a particular custom specification; count architecture search as complexity; refit nested ablations; distinguish observation failure from latent dynamics; and separate frozen-parameter transfer, limited recalibration, and transport of a model architecture.

The important differences change the substantive research agenda:

| Reply-tree setting | Rank-diffusion setting and consequence |
|---|---|
| Branching events, parent selection, and missing tree branches | Persistent entity-by-time panels, globally relative ranks, and observation/entry/exit processes. Hawkes or branching machinery is not a default next mechanism. |
| Event-time likelihoods may be central | Weekly aggregation, measurement noise, latent persistence, and overlapping horizons make covariance identification and filtering central. |
| Many separate cascades can provide replication | Entities share a competitive ranking and calendar shocks. N×T is not a count of independent observations, and more seeds do not create more empirical eras. |
| Cascade-size and temporal kernels | Stationary score/gap distributions and individual displacement must be explained jointly. Earnings panels and rank-based interacting diffusions are closer technical analogies. |

The review's general concerns about overfitting are useful. Its judgments about a specific reply-tree architecture are not findings about this project. Nor does that project's decision to pause a particular experiment bind this agenda.

## Literature synthesis: what changes the plan

The [annotated review](LITERATURE_REVIEW_2026-10-08.md) documents 31 primary papers and methods papers, including access limitations. This was a targeted review across eight connected literatures, not an exhaustive systematic review.

1. **Treat permanent–transitory identification as an earnings-panel problem as well as a diffusion problem.** Abowd–Card and Meghir–Pistaferri connect panel covariances, measurement error, and heterogeneous permanent/transitory variation. Arellano–Blundell–Bonhomme and Guvenen and colleagues motivate measuring persistence by shock size and sign before adding nonlinear dynamics. They do not establish that digital attention follows income dynamics. [Abowd–Card](https://www.nber.org/papers/w1832), [Meghir–Pistaferri](https://web.stanford.edu/~pista/meghir.pdf), [Arellano et al.](https://onlinelibrary.wiley.com/doi/abs/10.3982/ECTA13795).

2. **Validate the inverse problem before interpreting fitted components.** Even simple state-space models can have weakly identified process and measurement variances. A converged optimizer, exact NNLS, or a well-behaved sampler does not resolve that. Recovery experiments and objective profiles are particularly relevant here. [Auger-Méthé et al.](https://arxiv.org/abs/1508.04325), [Raue et al.](https://doi.org/10.1093/bioinformatics/btp358).

3. **Use stock/city rank models as structural comparators with explicit limits.** Gabaix supplies conditions for a stationary size law; Atlas and hybrid Atlas models connect rank-dependent dynamics with stable ordered distributions. Iñiguez supplies a compact displacement/replacement benchmark. None by itself solves this project's censored observation model or endpoint-specific forecasting problem. [Gabaix](https://xgabaix.scholars.harvard.edu/publications/zipfs-law-cities-explanation), [Hybrid Atlas](https://arxiv.org/abs/0909.0065), [Iñiguez et al.](https://www.nature.com/articles/s41467-022-29256-x).

4. **Score the predictive claim actually intended.** Proper scores distinguish useful distributions from matching selected medians; time-series validation must respect what was available at each origin. Finite-ensemble corrections and dependence-aware uncertainty matter. [Gneiting–Raftery](https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf), [Ferro](https://empslocal.ex.ac.uk/people/staff/ferro/Publications/ferro2013.pdf), [Bürkner et al.](https://arxiv.org/abs/1902.06281).

5. **Modern simulation-based inference is an option, not an identification cure.** Synthetic likelihood can accommodate complex summaries, but misspecified simulators can produce misleading inference. A 2025 calibration method requires real observations with known parameter labels, which this project generally lacks for latent homes, reversion, and temperament. Neural inference is therefore not the first investment. [Wood](https://pubmed.ncbi.nlm.nih.gov/20703226/), [Frazier–Drovandi](https://doi.org/10.1080/10618600.2021.1875839), [Wehenkel et al.](https://proceedings.mlr.press/v267/wehenkel25a.html).

## Ordered research program

These are proposed experiments, not permission to alter registered defaults or thresholds. Each work package ends in an artifact and a decision. Use previously exposed panels for development; retain the frozen implementation as a control. The current rolling-origin gate remains the adoption criterion unless the owner explicitly registers a different one. Supplementary endpoint scores can strengthen or limit claims without retroactively changing historical verdicts.

### 1. Recover known truth and audit the existing forecast construction

**Question:** Can the present estimator and forecast path recover and use the components that the present simulator actually generates?

Build a small, registered simulation study following ADEMP: aims, data-generating mechanisms, estimands, methods, and performance measures. Start with a linear, fixed-band limit whose covariance and filter are known. Add, in declared stages, realistic panel lengths, rank reordering, amplitude mixtures, heavy tails, the observation instrument, and missingness/entry. Use a designed grid covering weak-identification boundaries rather than a large undirected sweep.

Measure recovery of identifiable combinations as well as individual parameters; boundary pile-up; interval coverage; forecast calibration; and the frequency of falsely inferring heterogeneity when true s=0. Preserve the actual universe selection, permanent-rank estimation, aggregation, and noise-floor construction. Daily Spec-B is not repeated observation of an identical latent weekly truth: reproduce its assumptions and failure modes in the study.

Three code-level questions deserve priority:

- `estimate_temperament` uses a corrected dispersion of log sample variances with an effective-degrees-of-freedom approximation. Test its recovery under the heavy tails and serial dependence that the simulator uses. Do not assume a measured bias before running this test.
- The unconditional simulator estimates profiles by permanent rank but updates coefficients by the current latent-level ordering. Establish when that mapping preserves the intended moments and when it changes the effective process.
- The conditional forecast uses a scalar level filter, folds transitory variance into its noise term, initializes transitory states at zero, and ordinarily sets the future home to the last filtered level. Compare it with coherent filtering of the **same** slow/fast/medium components and uncertainty over the state at the forecast origin. The alternative train-mean home was already tested; repeating that switch alone is not the proposed experiment.

An augmented linear-Gaussian filter provides a reference in the fixed-coefficient limit. Heavy-tailed shocks and rank interactions require appropriate approximations; a collection of independent exact Kalman filters is not automatically an exact solution to the full interacting model. Start with a small verifiable cohort/reference implementation before attempting expensive full-system particle inference.

**Artifact:** recovery maps, an explicit state/parameter/observation specification for each simulator stack, and a paired comparison of current versus coherent forecast initialization.

**Decision:** failure on own-model synthetic data prioritizes inference repair. Successful recovery with persistent real-data residuals supports investigating missing dynamics. Weak identification calls for pooling, bounds, or reporting combinations—not another free component. These hypotheses require predeclared tolerances and Monte Carlo precision before execution; none is scored in this planning session.

### 2. Identify what creates established departures and returns

**Question:** Which observed transition is absent from the model, and is it dynamics or observation?

First complete the knot-aligned exit-estimand audit already motivated by §§2z-ab/ac. At origins determined from training data, distinguish: still observed but below a rank boundary; absent from the instrument; reobserved after absence; and identity retirement/rebirth in simulation. Use symmetric identity rules, denominators, rank boundaries, and the same observation operator. Resolve the recorded t+h versus t+1+h return convention before comparing curves.

Estimate first-passage incidence, time below the boundary, return-time survival curves, and signed displacement paths. Handle the end of the panel as right censoring. Permanent death generally cannot be identified from “never returned before the dataset ended.” Robust-design capture–recapture offers a useful distinction between detection, temporary absence, and survival, but its identification requires observations and assumptions that may not be available here. Where unavailable, report bounds or sensitivity, not a fitted biological analogy. [Kendall et al.](https://webhome.auburn.edu/~grandjb/wildpop/readings/Robust%20Design/Kendall_et_al_1997.pdf).

Measure persistence after positive and negative shocks of different sizes, by permanent-rank and temperament strata. Event selection itself produces regression-to-the-mean patterns; compare data with simulations processed through exactly the same event-selection rule. Avoid defining shocks using future outcomes. Require adequate independent time episodes, not merely a large number of overlapping entity-weeks.

**Artifact:** one transition taxonomy and a jointly simulated diagnostic panel for departure incidence, spell duration, return, and concentration on comments and submissions, with Facebook as a separately observed regime.

**Decision:** a deficit in observed downward crossings cannot be repaired solely by relabeling missingness. If crossings are correct but absence/return is wrong, repair the observation/boundary model. If persistent, sign- or size-dependent displacement is demonstrated after the recovery audit, permit one targeted dynamical candidate. A selected return rate is not a new analytic moment to append to the existing minimum-distance equations without deriving its selection law; indirect inference with the same selection operator is a possible alternative.

The diagnostic should distinguish shock frequency, shock magnitude, and recovery duration. A model with too few large level changes but correct conditional recovery suggests a different intervention from one with enough shocks that decay too quickly. The former could motivate a tightly constrained rare-displacement component; the latter could motivate persistence depending on shock magnitude. Both would need an independent identifying signature and a refitted simpler comparator. Neither is licensed merely by the aggregate departure ratio, and neither is equivalent to the already unsuccessful global innovation-tail adjustments.

### 3. Make parsimony a measured result

**Question:** How much rank-profile flexibility and latent structure is required for predictive and structural adequacy?

Compare a short, predeclared complexity ladder: coarse/shared profiles; a small parametric or low-rank basis; and penalized smooth profiles, with the current knot specification as the reference. Use b=1 as a candidate constraint supported by its own measurements. Keep optional medium components and heterogeneity layers only where their independent moments and forecasts justify them. Eilers–Marx provides a practical smoothing basis, not evidence that smoothing must improve this application. [Eilers–Marx](https://sites.stat.washington.edu/courses/stat527/s13/readings/EilersMarx_StatSci_1996.pdf).

Refit every nested ablation. Simply setting s, a variance, or a timescale to zero diagnoses an intervention in a fitted model; it does not establish whether a simpler fitted model would suffice. Choose smoothing and shrinkage only inside the training history, with an inner temporal validation scheme where needed. Count estimated homes, effective degrees of freedom, hyperparameters, and specification-search choices alongside nominal coefficients.

Plot complexity against the frozen movement gate, supplementary individual forecast scores, structural diagnostics, and uncertainty. A practically indistinguishable simpler model is a serious scientific success. The practical equivalence margin must be registered before this comparison; this agenda does not invent a new acceptance threshold.

**Artifact:** a refitted ablation table and a complexity–performance frontier, including all failed and tied variants.

**Decision:** graduate only the least complex adequately supported specification. If the data cannot distinguish two latent decompositions, do not select the more elaborate one because its components sound more realistic. Ordinary AIC is not valid on an arbitrary moment-distance score; a variance-at-zero comparison also does not automatically have an ordinary likelihood-ratio reference distribution. [Self–Liang](https://www.stat.cmu.edu/~brian/763-2015/week06/papers/self-liang-1987.pdf).

### 4. Run a bounded, fair model comparison

Use the same origins, training information, observation rules, universes, computational budgets, and forecast outputs. Separate comparators by the scientific claim they can address:

| Comparator | What it tests |
|---|---|
| Unchanged endpoint rank and the existing historical-mobility baseline | Whether individual forecasting beats elementary alternatives; these are distinct baselines. |
| A regularized empirical transition forecast using only past data | Whether mechanistic structure adds predictive value beyond a compact statistical forecast. Conditioning a forecast on current rank is allowed; it does not justify estimating structural diffusion coefficients from current-rank-selected changes. |
| Compressed permanent–transitory model with coherent filtering | Whether the present mechanism can succeed with less flexibility and better use of available state information. |
| Iñiguez-style displacement/replacement model | How much rank turnover needs more than a few parameters. It cannot win or lose a score-size claim it does not generate. |
| A specified random-growth/rank-interaction structural comparator | Whether a simpler process can jointly explain ladder shape and movement, under a comparably explicit observation model. |
| At most one new dynamical mechanism at a time | Whether a residual demonstrated in package 2 warrants additional structure. |

A new mechanism needs a discriminating prediction outside its calibration moments. For example, a measured shock-duration asymmetry could motivate a constrained asymmetric persistence model; it does not authorize an unrestricted regime-switching mixture. Do not revive common volatility, rank-dependent tail surgery, κ_i, or the rejected variance anchor without new evidence that addresses their recorded failures.

### 5. Establish exactly what “structural explanation” means

Separate three claims: finite-horizon maintenance of a supplied empirical ladder; attraction toward a stationary law conditional on a fixed distribution of homes; and generation of that home/size law from a smaller mechanism. The first is worthwhile, but does not imply the third.

Derive invariant distributions or stationary balance conditions in controlled limits of the current model, then measure the full model's departures from those limits. Perturbing initial states while keeping empirical homes fixed tests conditional attraction. Changing the home distribution tests something stronger. Also test entry/rebirth balance, buffer sensitivity, initialization dependence, and mixing time; never infer stationarity merely because a short burn-in produced a plausible figure.

Atlas models are useful because they make the connection between interactions and ordered gaps explicit. The project's permanent-rank estimation and identity homes are different from pure current-rank interactions, so the relevant generator and boundary conditions must be derived rather than borrowed. A universal exact Zipf law is not required: compare plausible size distributions and their temporal stability under dependence-aware uncertainty. [Hybrid Atlas](https://arxiv.org/abs/0909.0065), [Clauset et al.](https://arxiv.org/abs/0706.1062).

**Artifact:** a claim-to-assumption map, solvable limiting cases, and structural stress tests. If a low-dimensional home distribution plus measured dynamics suffices, that is a meaningful compression and explanation. If full ladder genesis requires a separate mechanism, keep that as a clearly scoped theory branch rather than concealing empirical initialization.

### 6. Confirm transport on reserved evidence

After choosing the candidate on development evidence, audit exactly what Wikipedia data and outcomes the acquisition pilot exposed. Freeze a distinct-platform protocol on genuinely unexamined outcomes. Distinguish three arms: unchanged parameters; narrowly specified recalibration using an allowed training segment; and refitting the same architecture. They answer different universality questions.

Use existing data for the inference and identity audits first. Breadth without reliable observation and estimand definitions will not resolve the current deficiencies. Instagram should proceed through its explicit measurement/allocation design and validation route; the raw panel remains a negative control, and modeled conclusions remain scoped to the censoring apparatus. Validation against solo-post pseudo-collaborations does not by itself identify true collaboration allocation shares.

## Validation discipline for the proposed program

Keep the frozen gate and its original labels in reproduced tables. Add supplementary diagnostics with explicit claim labels:

- **Individual distributions:** origin-defined identities, signed displacement and rank distributions at h=1/4/13; CRPS or ranked probability scores; predictive interval calibration and sharpness. Use a joint population simulation to preserve the relative nature of ranks. Pooled CRPS is not an individual forecast score.
- **Presence and transitions:** proper scores for presence, boundary passage, and return within a defined horizon. Do not silently drop missing endpoints or assign every absence an arbitrary worst rank. Report the outcome convention and separate structural and observation uncertainty.
- **Collective structure:** rank-size and concentration trajectories, identity turnover, and joint dependence. Marginally calibrated individual forecasts need not produce a coherent joint ranking; dependence-sensitive diagnostics can supplement marginal scores.
- **Uncertainty:** separate simulation Monte Carlo error, parameter uncertainty, state uncertainty, and sampling variability. Use paired comparisons and time blocks shared across all entities to preserve calendar dependence and ranking competition; block lengths require sensitivity analysis. Five overlapping origins are not five independent experiments. Re-estimate within resamples when estimating uncertainty from fitting.
- **Computation and search:** select replication counts by Monte Carlo precision, retaining the repository minimum for head claims. Announce aggregate weights and protected strata before seeing results. Inner selection stays inside past data; a repeatedly used holdout becomes development data. Record rejected architectures as well as rejected parameter values.

Ferro-style fair ensemble scores require the relevant sampling assumptions. Do not apply a finite-ensemble correction as though pooled dependent observations were independent forecast draws. Likewise, use simulation-based calibration only if a Bayesian posterior procedure is introduced; it checks computation under the assumed model, not the empirical truth of that model. [Ferro](https://empslocal.ex.ac.uk/people/staff/ferro/Publications/ferro2013.pdf), [Talts et al.](https://arxiv.org/abs/1804.06788).

## First execution cycle and use of the two stronger models

Start with a compact cycle: (1) freeze the implementation/exposure/claim inventory; (2) specify the own-model recovery study and exact forecast state equations; (3) complete the knot-aligned identity/return audit; (4) compare coherent filtering and one compressed specification on development origins. Only then choose a new mechanism or commit a fresh confirmation set.

Use the two models for independent derivation and adversarial checking before outcomes are revealed. One writes the estimand, equations, and executable design; the other independently checks identifiability, observation mapping, leakage, and counterexamples. Resolve disagreements in a single written pre-run record, then swap roles on the next package. Model agreement is not empirical evidence, and indefinite cycles of reviewing an unchanged result are not progress. This is a proposed collaboration workflow; no messages or tasks were dispatched to another model in this session.

The first decision should be whether the present model fails because it cannot infer/initialize its own states, or because those states cannot generate observed established departures. The second should be how much of its rank dependence can be removed without losing justified performance. Together these directly advance prediction and simplicity before increasing mechanistic complexity.

Predictions for the first cycle should be written before execution: a coherent filter must recover the analytical reference forecast in the fixed-band Gaussian limit; introducing observation gaps must not turn known surviving synthetic identities into permanent deaths under an observation-aware estimator; and a compressed model must retain the predeclared level of predictive and structural adequacy after refitting. On real data, improved initialization may or may not improve individual scores, and an observation repair alone may or may not eliminate a particular departure residual. Those are open comparisons, with null/negative outcomes retained. Numerical tolerances, power targets, and any new adoption criteria require a separate pre-run specification; this planning document is not that registration.

## Analytical checks worth carrying into the design

These are derivations or code-audit hypotheses, not new measured research results.

**Local identification.** In a stationary fixed-band, no-entry limit with independent components, let Y=H+T+ε, with AR coefficients ρ=1−κ and φ, stationary variances A and B, and white observation variance R. Then C(h)=Aρ^h+Bφ^h+R·1[h=0], and for h≥1, D(h)=2{A(1−ρ^h)+B(1−φ^h)+R}. As φ approaches zero, B and R become hard to separate. For small κh, the slow term identifies Aκ much better than A and κ separately. This clarifies why long time coverage and a valid external noise instrument matter; more cross-sectional entities do not remove every short-panel ambiguity.

**Mixing.** At ρ=0.995, a 40-step burn-in retains about 0.818 of an initial displacement; a home-initialized scalar AR process accumulates only about 0.330 of its stationary variance by then. This illustrative calculation does not establish that the reported simulations are invalid, but a uniform 40-step burn-in cannot itself certify equilibrium across all admissible timescales.

**Stationary balance.** For a one-dimensional diffusion with zero stationary probability current and diffusion coefficient b, drift a=(b²)'/2+(b²/2)(log p)' for stationary density p. Thus a size density alone leaves many drift/noise choices. Entry/exit, interactions, rank local times, and nonzero currents require additional terms and conditions; this scalar identity is not a new estimator for the full model and does not resurrect the rejected empirical variance anchor.

**Log tails versus activity tails.** Unbounded Student-t log shocks have no finite exponential moment, even when log variance and kurtosis exist. Consequently finite log-moment fits do not automatically imply finite expected unnormalized activity. Ranks and normalized shares remain bounded, so this is not a blanket invalidation of those outputs. Audit which moment claims are actually needed, numerical tail sensitivity, and the interaction with amplitude mixtures. Do not infer that arbitrary clipping or thinner shocks would improve the measured departure problem.

## Session provenance

Read-first skills, canonical status/protocol, current estimator/simulator/forecast code, July 18 IG design, and the October 7 cross-project review were inspected. The research regression suite passed with 177 tests and 13 subtests; the Python package suite passed with 8 tests. These are software checks, not new scientific validation. No model fit, new-data analysis, threshold/default change, or package change was performed for this agenda. Existing unrelated working-tree changes were preserved. A dated planning entry was appended to MODEL_STATUS.
