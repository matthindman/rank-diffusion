_Review by gpt-6-astra (OpenAI) through the Codex CLI 0.163.0 (`codex exec -m gpt-6-astra -s read-only`), 2026-10-18, from `CRITIQUE_PROMPT.md` in this folder. Written to this file by the CLI; header added by the orchestrating session. Model output: its claims are checked in `SYNTHESIS.md` and MODEL_STATUS §2z-da, not taken on trust._

**1. Verdict**

I would not send the present framing to PNAS referees with confidence, although the repository contains the ingredients of a consequential paper. Its strongest asset is the combination of longitudinal engagement data, explicit measurement models, and unusually candid recorded failures. The largest gap is that the bridge from measured movement to concentration and incumbency remains less identified than the paper’s language suggests. The single change that would most improve its prospects is one prospectively constructed evaluation in which the same movement model must account for both aggregate redistribution and individual retention. The ambitious paper is a quantitative account of **contestability in online attention: how much movement changes who occupies privileged positions, how much changes the advantages those positions command, and what determines the difference**.

This review combines the requested theoretical, social-scientific, econometric, inequality-economics, editorial, and short-seller perspectives. I inspected code, tests, figures, and saved outputs, including independent recalculations of boundary ratios and lock-in scores. References below default to `llm_fitting/`; record citations refer to `MODEL_STATUS.md`. No raw or sealed data were opened, and I wrote no files.

**2. The ambitious version**

The unreasonable target should be:

> **From an origin’s observed ladder and independently estimated movement distribution, predict both the subsequent distribution of attention and the persistence of its incumbents—without fitting either outcome.**

This asks more than demonstrating a balance identity, fitting movement moments, or forecasting retention separately. It is worth attempting because these are complementary restrictions on the same process. A model can reproduce concentration while moving the wrong people; it can forecast incumbency while preserving an incorrect ladder. Requiring both creates a substantive test.

The headline should concern **mobility and persistent advantage**, with the boundary relation serving as an explanatory component. “Crossings follow variance divided by slope” alone risks becoming an application of established mathematics. “Measured movement explains how inequality persists while its beneficiaries change, and identifies when that explanation fails” offers a durable contribution—if the joint evidence earns it.

Three claims could carry that paper.

**First: positional turnover and redistribution are empirically separable.** Show ordinary activity concentration, turnover, uninterrupted residence, exit–re-entry, and cumulative activity captured by the original incumbents on the same time axis. The existing opening figure shows changing names and similar-looking curves, but the submissions concentration index falls substantially (§2z-cw). The empirical puzzle therefore needs precision: persistent *shape family* coexists with meaningful changes in concentration.

The carrying figure would distinguish replacement by previously established endpoints, renewed activity, and first-observed entrants. Annual terminal membership is insufficient to describe access: an incumbent can leave and return, while many apparent newcomers may never approach the head.

**Second: the distribution of movement connects structure to incumbent persistence.** Crossings and retention interrogate different parts of that distribution. Rare large movements can contribute heavily to variance while most incumbents remain relatively still. That is a promising explanation of the Gaussian retention forecast’s underprediction, but “jumps” are not yet uniquely identified.

Compare a Gaussian process, persistent growth heterogeneity, and an empirical or mixture transition distribution, all estimated from declared training information. Score future shape, arithmetic shares, and endpoint retention together. The decisive figure should have those three outcomes side by side, with the same models, origins, and uncertainty—not separate figures displaying each model’s strongest result.

**Third: engagement redistribution can be localized to production versus engagement per item.** The June 2021 evidence is unusually useful here. The top five’s submission-karma share fell 31%, while their submission-count share showed no corresponding onset (§2z-ca). This localizes the change to engagement per post. Meanwhile, comments’ half-year head gain was broad: 59% of the top 781 gained, and the ten largest gainers supplied only 9% of gains (§2z-cr). An aligned figure could reveal how different forms of activity reorganized within the same platform.

That supports a substantive measurement result. It does not establish a causal algorithm effect.

A and B should belong to one paper **only through an executable bridge**. A supplies the measurement decomposition and a competing generative model; B supplies aggregate restrictions and diagnoses where that model fails. Run B’s accounting on A’s simulated panels, using exactly the empirical pipeline. Then identify which term—movement distribution, cohort drift, reordering, entry, or initialization—produces A’s structural error.

This integration could simplify the manuscript considerably. A’s full parameter machinery belongs in the methods and supporting information unless it earns incremental explanatory power in the joint test. The main text should follow one question, not alternate between two research programs.

**3. The short seller’s case**

**Claim 1: the measured “lasting crossing rate” tests the continuous balance law.**

The repository itself supplies the strongest objection. [balance_accounting.py:31](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/balance_accounting.py:31) explicitly says endpoint reordering is nonadditive and is not accumulated local time. Yet `mts_boundary_law.py:18–24` interprets differences between 52- and 104-week endpoint gains as a lasting crossing rate.

For a fixed population, let \(T_a,T_b\) be the top sets at successive dates and \(G_{bc}(T)\) their mean movement between \(b\) and \(c\). Exactly,

\[
R_{ac}-R_{ab}
=
R_{bc}+G_{bc}(T_b)-G_{bc}(T_a).
\]

The differenced statistic contains the different subsequent movement of the later occupants and the original occupants. That extra term matters precisely when drift depends on rank or changes over time.

Moreover, [mts_boundary_law.py:134](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/mts_boundary_law.py:134) selects a different survivor population at each horizon, then re-ranks it at the starting date. The subtraction can therefore change both the population and the initial top set.

The successful ratios are real: saved JSON reproduces comments’ rank-12,500 ratio of **1.199111**, and submissions’ ratios of **0.717585** and **0.608939** at ranks 312 and 625 (§2z-cm). Their interpretation is the vulnerability.

*Hostile referee:* “You derive an instantaneous local-time law, measure a nonadditive cohort statistic, and treat agreement as validation of the former.”

*Fix or retire:* Derive and measure the finite-horizon remainder; repeat the comparison on a common survivor intersection; report composition separately. Validate under changing drift at the affected boundary. The existing pressed-head test checks the crossing formula below the press (`tests/test_mts_boundary_law.py:83–88`), leaving the difficult region insufficiently tested.

**Claim 2: current movement sustains a submissions-head share of 16%.**

The arithmetic reproduces: **51.146%**, **34.795%**, and **16.064%** for the 2019 index, 2022 index, and latest sustained scenario. But these are shares of exponentiated block-mean log activity, not shares of total karma. Section 2z-cw says so; the abstract and concentration figure do not.

[concentration.py:39](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-15_concentration/concentration.py:39) integrates estimated sustained slopes and then grafts the observed final-year head above K/64 onto the scenario. Calling that graft conservative requires an argument about the unobserved counterfactual head.

The scenario also depends on submissions-head variance estimates whose precision elsewhere prevents adjudicating the boundary theory. There is no propagated concentration band.

*Hostile referee:* “Your most consequential number is neither the quantity readers think it is nor a sufficiently measured destination.”

*Fix or retire:* Present arithmetic activity shares alongside the geometric index; propagate joint uncertainty; vary the retained-head assumption. Describe the result as a frozen-movement scenario. A robust direction of imbalance could survive even if “16%” does not.

**Claim 3: ex ante forecasts establish unexpectedly strong lock-in across four ladders.**

The scores are honestly calculated. Recomputing `runs/2026-10-15_lockin/lockin__*.json` gives Brier improvements over constant turnover of **20.70%, 18.28%, 13.38%, and 19.60%** for comments, submissions, Facebook, and Instagram.

The interpretation outruns them. The comparator gives every member the same probability; the movement forecast knows distance from the cutoff. A logistic using rank and horizon already matches its performance. Brier improvement measures probability accuracy, not exclusively discrimination.

Pooling also obscures the intended target. Approximately **76%** of endpoint weight comes from K/4 and only **5%** from K/64. The aggregate calibration pass, 95/126 cells, comprises 41/45 on comments but only **26/45 on submissions** and **11/18 on Facebook**. At three or more blocks, pooled absolute error is **0.06325 versus 0.04404** for constant turnover; Facebook is **0.1250 versus 0.04994** (§2z-cy).

There is a prospective-construction problem too. `mts_lockin.py:44,184–189` uses `instagram_hm`, whose source was selected by full-year permanent rank. [minimal_rankdiff.py:75](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/minimal_rankdiff.py:75) documents that construction and, immediately afterward, the separate train-safe source and exact-membership requirement introduced after earlier leakage audits (§2z-e/f). The distortion at these shallow cutoffs is unmeasured; strict ex ante status is nevertheless unproven.

*Hostile referee:* “You establish that endpoints farther above a boundary retain membership more often, then interpret Gaussian forecast error as platform lock-in.”

*Fix or retire:* Audit origin-specific universes; add rank/gap baselines and paired uncertainty; show each cutoff separately. Distinguish terminal retention from uninterrupted incumbency. Call the residual “excess retention relative to the Gaussian forecast” until stronger interpretations survive.

**Claim 4: Instagram censoring cannot generate the result and contributes at most 23–41% of movement.**

The proposed bound is invalid without additional assumptions. Approximately,

\[
\Delta\log M=\Delta\log P+\Delta\log q,
\]

where \(M\) is matching-post count, \(P\) posting volume, and \(q\) matching propensity. The variance of their sum does not bound either component: posting and propensity changes can offset.

Likewise, engagement per matching post is not automatically “q-free.” It is engagement conditional on caption matching. Changes in language, topic, or post composition can change that conditional mean.

[ig_censoring_sim.py:22](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/ig_censoring_sim.py:22) gives each account fixed posting volume and generates propensity independently of activity. Its successful recovery validates those planted assumptions, not arbitrary informative selection.

*Hostile referee:* “Your empirical upper bound is not a bound, and the simulation assumes away the dependence that could invalidate it.”

*Fix or retire:* Withdraw the upper-bound claim. Test dependent propensity, posting, engagement, and dropout scenarios. Retain Instagram as evidence about reliably measured **a-matching engagement**, with explicit sensitivity limits.

**Claim 5: B explains A’s failure and their tail discrepancies identify one mechanism.**

The revised `BIG_PICTURE_REVIEW` §1 correctly says that B bears on A’s structural failure but does not explain it. Section 6 still says it explains that failure.

The measured evidence is more specific. Section 2z-av attributes the Reddit head-share errors largely to initialization: endpoint-mean seeding brings simulated shares to **1.00 and 0.97** of observed values. Facebook and Instagram’s excess is substantially carried by passing-shock spike weeks. These are different diagnoses from nonstationary submissions-head relaxation.

A’s under-dispersed movement tails and B’s underpredicted retention concern different distributions, conditioning, and horizons. Section 2z-cz demonstrates a sufficient block-jump mechanism among selected rivals; it does not establish uniqueness.

*Hostile referee:* “The unity of the paper is asserted where the models have not been compared on the same diagnostic.”

*Fix or retire:* Apply the same balance and retention diagnostics to A’s simulations. Keep the proposed connection as a hypothesis until that comparison identifies a common residual.

**4. Fair criticisms of the models themselves**

**Balance law and estimators.** The exact accounting is valuable and secure. The main uncertainty lies in translating its finite-horizon components into diffusion coefficients.

The “lasting variance” estimator in [mts_balance_law.py:71](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/mts_balance_law.py:71) is

\[
\frac{\operatorname{Var}(d_1+d_2)
-\{\operatorname{Var}(d_1)+\operatorname{Var}(d_2)\}/2}{D}.
\]

For heterogeneous persistent drifts, \(X_i(t)=\mu_i t+\sigma W_i(t)\), this becomes \(\sigma^2+3D\operatorname{Var}(\mu_i)\) under the simple independent setup. It does not isolate diffusion variance. The repository deliberately plants 30% drift contamination and obtains ratios near one (§2z-cq). That supports robustness of an empirical relation while weakening its interpretation as identifying diffusion physics.

The least-tested assumptions are transport from earlier cohorts to actual boundary occupants, cancellation of selection constants under changing dynamics, and approximation of boundary density by local slope. Current-rank conditioning is not inherently wrong in rank-based theory: it targets a different coefficient. The paper should establish the relationship between those estimands.

Uncertainty is incomplete: boundary bootstraps resample overlapping starts independently, estimate variance and reordering uncertainty separately, and hold slope fixed. The resulting intervals do not represent joint uncertainty in the full ratio. Passing a point-estimate tolerance band is also not an equivalence test.

**Permanent–transitory model.** A remains useful as a predictive statistical representation. Its latent parameters have weaker mechanistic standing. The independent recovery study reports reversion at bounds in **46–68%** of bands, substantial recovery error, positive amplitude spread when true spread is zero, and inconsistent tail recovery (§2z-bc).

Removing home reversion improves some movement discrepancies while destroying structure (§2z-ay). This is evidence that the home performs incompatible jobs, not evidence that more flexible home profiles will solve them. Parsimony should be assessed against simpler refitted alternatives, with effective freedom counted.

**Lock-in forecaster.** Its weakest assumptions are linear extrapolation of movement moments, deterministic extrapolation of the boundary, Gaussian conditional changes, and geometric extrapolation of presence. One- and two-block moments use different observed-member masks (`mts_lockin.py:82–100`), so their difference can mix attrition with dynamics.

Importantly, the forecast contains no \(\sigma^2/(2S)\) crossing law. Its success is complementary evidence, not validation of B. Empirical-shape improvements are promising development results; changing the distribution halves much of the apparent excess retention (§2z-cz).

**Instagram apparatus.** Its genuine achievement is showing that useful statistics can survive specified thinning and dropout mechanisms. Its least-tested assumptions concern informative missingness and dependence between caption matching and engagement. A stable account-specific offset cancels from that account’s changes but can still rearrange which accounts occupy a rank, especially when volatility differs across accounts.

**5. What we are not seeing**

The most consequential blind spot is **what constitutes opportunity**. Turnover of a ranked set is not enough to establish access. Measure first entry, repeated entry, time spent above the cutoff, cumulative activity captured, and the origin ranks of successful climbers. These would give “lock-in” an interpretable social meaning.

“New to the record” also needs separation from organizational birth. Section 2z-bv’s classification begins with the archive window, and block presence rules can turn intermittent activity into apparent entry. Age, accumulation of audience, and organizational resources could explain lower volatility among large endpoints without volatility responding directly to size.

The activity unit matters. Communities aggregate many posts and contributors. Changing topic mixtures, contributor composition, moderation, posting intensity, or engagement per item can generate apparent community-level persistent shocks. The production/engagement decomposition already available is therefore central, not ancillary.

Dependence remains underexamined. A correlation between long-horizon changes of neighboring ranks does not exhaust shared topical shocks, audience substitution, or calendar clustering. The rejected neighbor-correlation explanation (§2z-cv) should not become a general rejection of dependence.

The bibliography is less deficient than the use made of it:

- [Iñiguez et al.](https://doi.org/10.1038/s41467-022-29256-x) already distinguish large moves, diffusion, and replacement. “Two-speed movement” cannot carry novelty by itself. The repository’s random-permutation rival is also not a fitted reproduction of their full model.
- [Wu and Huberman](https://arxiv.org/abs/0704.1158) study item-level attention decay. Here, the substantive extension is explaining how portfolios of such items produce durable endpoint advantage.
- [Gabaix, Lasry, Lions, and Moll](https://www.nber.org/papers/w21363) make transition speed a separate test from stationary shape. Their lesson directly challenges the extrapolated relaxation times.
- [Itkin and Larsson](https://arxiv.org/abs/2403.04674) jointly examine volatility, collisions, distributional structure, and out-of-sample fit; [Baker and Smyth](https://arxiv.org/abs/2608.27156) derive stability and relaxation from a specified population process. These are substantive comparison standards.
- Networked reinforcement offers a different generative explanation of hierarchy, exemplified by [Kawakatsu et al.](https://pmc.ncbi.nlm.nih.gov/articles/PMC8072324/). It appears in the outline chiefly as a presentation precedent; it should also inform the mechanism discussion.

For platform governance, [Huszár et al.](https://doi.org/10.1073/pnas.2025334119) measure amplification against randomized chronological-feed assignment. This repository offers longitudinal redistribution evidence, but lacks that exposure contrast. Comments are a comparison outcome, not automatically an untreated control.

**6. What the expert panel would require**

The stochastic-process theorist would require a finite-horizon statement matching the implemented statistic, including drift-composition and survivor terms. A continuum derivation plus favorable planted worlds is insufficient.

The econometrician would require prospective sample construction, joint uncertainty, separation of weak identification from parameter change, and transparent treatment of overlapping origins. “Registered” analyses on repeatedly inspected development panels must remain development evidence.

The inequality economist would require the distribution and mobility to be jointly explained, actual activity shares, and transition predictions tested within the observed window. The **2–6-year and 5–22-year** relaxation estimates (§2z-cp) are scalar linearizations with uniform-slope and frozen-slide assumptions; they are not measured adjustment times.

The computational social scientist would require meaningful entry and persistence outcomes, production versus engagement decomposition, and a careful account of what karma and tracked interactions measure.

The editor would require one memorable empirical conclusion supported by one coherent chain of evidence. Four figures could suffice: the substantive puzzle; finite-horizon mechanism and failure map; joint redistribution/retention predictions; and the dated engagement redistribution episode.

Reproducibility needs a submission artifact, not merely a large repository: a pinned environment and code snapshot; a claim-to-output manifest; a clean build from distributable derived inputs; and explicit availability conditions for Reddit and CrowdTangle materials. Synthetic recovery fixtures are valuable but cannot substitute for access to the empirical evidence needed to verify the headline.

**7. Prioritized changes**

“Available now” below means feasible using existing unsealed holdings in subsequent work; none requires new data acquisition. These are recommendations, not adopted changes.

| Priority | What to do, with which data/code | Expected payoff | Effort and risk |
|---|---|---|---|
| 1 | Derive and measure the finite-horizon remainder; use common survivor sets in `mts_boundary_law.py`, `balance_accounting.py`, and existing Reddit panels. | Determines what the central test actually establishes. | Medium; could materially narrow the law. |
| 2 | Audit prospective membership and block-rank completeness in `mts_lockin.py`; use the existing Instagram train-safe machinery appropriately. | Repairs the strongest forecasting claim’s sample construction. | Medium; effect on scores unknown. |
| 3 | Apply B’s accounting, concentration, and retention diagnostics to A’s saved simulations where available; otherwise simulate frozen stacks under the same pipeline. | Provides the missing A–B bridge and localizes structural failure. | Medium–high; may reject integration. |
| 4 | Recompute arithmetic shares alongside the geometric index; propagate uncertainty through `concentration.py` and vary its retained-head assumption. | Makes the inequality result interpretable and defensible. | Medium; likely weakens the 16% headline. |
| 5 | Compare lock-in with rank/gap-only, historical transition, and empirical-shape baselines; report each cutoff and paired origin uncertainty. | Establishes incremental predictive information. | Medium; the simplest baseline may suffice. |
| 6 | Score terminal retention, continuous residence, re-entry, and cumulative incumbent activity using existing identity histories. | Converts turnover into substantive contestability. | Medium; definitions and censoring need care. |
| 7 | Rebuild boundary uncertainty jointly across entities, dates, slopes, variance, and crossings; validate interval behavior in planted worlds. | Makes precision claims credible. | Medium–high; sparse temporal replication remains. |
| 8 | Run calibrated rivals with persistent drift, changing drift, jumps, and informative attrition through the complete estimator; include the submissions deficit. | Separates empirical closure from mechanism identification. | High; observational equivalence may remain. |
| 9 | Use existing daily/count holdings and `2026-10-14_daily_date_check` outputs to align production, engagement per item, and concentration around June 2021, with placebo dates. | Supplies the strongest substantive platform result. | Medium; remains observational. |
| 10 | Replace Instagram’s attribution bound with dependent-censoring sensitivity using `ig_censoring_sim.py` and saved decomposition outputs. | Preserves useful breadth without false identification. | Medium; population-wide claims may remain impossible. |
| 11 | Build the claim-to-output manifest and four-figure submission narrative from pinned outputs. | Removes contradictions and makes independent reproduction practical. | Low–medium; distribution rights need resolution. |

The following require additional owner decisions:

| Priority | What requires the owner | Payoff | Effort and risk |
|---|---|---|---|
| 12 | Resolve the protected conditional-simulator timing issue, score aligned exports, and approve any revised binding claim set. | Makes A’s endpoint forecasting role publication-ready. | Medium; preserve recorded verdicts and report new results separately. |
| 13 | Freeze one joint test before opening the already-held Reddit bridge; use Wikipedia only after its exposure status and suitable temporal coverage are established. | Supplies genuine prospective confirmation beyond development analyses. | High; spends a finite confirmation opportunity. |

New data would be needed for direct causal exposure effects or nonparametric identification of Instagram’s all-post matching propensity. Neither is necessary for the proposed paper if its claims remain appropriately scoped.

**8. Errors and corrections**

Beyond the substantive vulnerabilities above, these are concrete corrections:

- **Wrong estimand in the abstract:** [PAPER_FRAME_DRAFT:36](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/PAPER_FRAME_DRAFT_2026-10-15.md:36) calls the exact decomposition a change in “share.” It is a mean-log-level shape identity. Lines 51–54 similarly omit the concentration index’s qualification.
- **Mislabelled concentration figure:** `runs/2026-10-15_figures/make_figures.py:295–297` says “share of the top K’s activity” and “Where the head is going.” Neither label adequately describes the geometric index and conditional scenario.
- **Mislabelled Instagram variance:** the four-ladder caption and `make_figures.py:132–154` call it lasting variance, while §2z-cx explicitly says the 14–28-week quantity still contains passing movement.
- **Invalid attribution bound:** §2z-cx and `REVIEWER_QUESTIONS` question 4a treat matching-count variance as an upper bound on propensity drift. The covariance counterexample above invalidates that inference.
- **Forecast-date mislabelling:** `mts_lockin.py:293–294` prints the origin block’s first week, although the forecast consumes that whole block. Label the information cutoff and target block explicitly.
- **Known simulator timing discrepancy:** [rankdiff_kalman.py:635](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/rankdiff_kalman.py:635) documents two transitions before the first recorded conditional row. Section 2z-bg correctly withholds calendar-aligned endpoint skill; subsequent paper text must retain that restriction.
- **Internal contradiction:** `BIG_PICTURE_REVIEW` §1 now says B does not explain A’s failure, while §6 still says it does. The adjacent “same fact seen twice” assertion also remains a hypothesis.
- **Numerical wording error:** §2z-cu says the 13-week comments variant has one ratio outside `[0.75,1.33]`; its own table contains **0.72 and 1.41**, hence two.
- **Horizon error:** §2z-cy discusses a growing gap “by 1.5–2.5 years,” largest for Facebook, although Facebook’s evaluated horizon stops at **39 weeks**.
- **Unsupported exclusivity and causality:** `PAPER_FRAME_DRAFT:23–26` says shape changes only when drift changes and that the findings show how platform design reshapes inequality. The equation also contains changing variance, slope, and entry; the event analysis does not identify a causal design effect.

The principal saved headline calculations checked here reproduce. The most serious problems are changes of estimand, incomplete identification, prospective-selection vulnerabilities, and interpretations stronger than the computations establish.