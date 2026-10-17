_Review by a fresh Claude Opus 5.5 session (Claude Code subagent, no prior context), 2026-10-18, from `CRITIQUE_PROMPT.md` in this folder. Read-only; saved verbatim by the orchestrating session. Model output: its claims are checked in `SYNTHESIS.md` and MODEL_STATUS §2z-da, not taken on trust._

# Independent review: what stands between this repository and a PNAS paper

I worked read-only and opened no data or sealed paths. Any number marked "recomputed" comes from the saved JSONs under `llm_fitting/runs/`. The panel's perspectives are labelled where they drive a point. One note on concurrent work: while I was reviewing, commit 5362ac9 corrected the claim in `BIG_PICTURE_REVIEW_2026-10-18.md` that B explains A's failure, and someone left uncommitted edits to `mts_boundary_law.py` and a new `runs/2026-10-18_pooled_ratio/`. I did not review those.

## 1. Verdict

1. As it stands, PNAS would probably not send this out. The outline it would be judged against (`paper/pnas_outline.tex`) is almost entirely `\todo`. Its title says "Eulerian stability", which B's own numbers refute: the share held by the top 78 submissions communities fell from 51% to 35% in three years (§2z-cw).
2. The largest gap is that no load-bearing result is both non-mechanical and out of sample:
   - the boundary theory is a check, in the same window, of something close to an identity;
   - the structure forecasts do no better than trend (C13);
   - the lock-in forecaster loses to a three-parameter logistic on rank.
3. The social-science payoff — that platform design moves concentration — rests on contrasts that were never registered and have no control (June 2021). It also rests on a "share" that is really an index of geometric-mean activity.
4. The paper is fixable, because the data on hand support a sharper and more interesting claim than the one drafted. That claim is about attention inequality out of equilibrium, steered by drift by rank. A "law that holds" is not the claim.
5. The one change that would most raise the odds: turn the boundary theory into a prediction made from an earlier period, scored against fitted rival models, and stake the paper on it. Do it now on the development data, then confirm it once on the sealed 2023–24 Reddit bridge under a re-registered protocol (owner-gated).

## 2. The ambitious version

**Headline the data could carry.** Attention concentration on platforms is not a stable form. It is a moving balance:
- lasting, level-space mobility pulls the head down at a rate σ²/(2S);
- drift by rank pushes it back;
- drift is the quantity platform design sets.

So measured movement says where concentration is going, how slowly it gets there, and what a feed change did to that path. The four claims and the analyses that would carry them:

**Claim 1. Attention moves in level space, at two speeds, and not in rank space.** This is the defensible core of C3/C4 and of the lock-in diagnostics.
- *Analysis.* Fit the rival classes to each ladder's own turnover, then push them through the identical estimators:
  - the Iñiguez et al. (2022) rank-displacement model;
  - Yule–Simon / preferential attachment;
  - a Gabaix–Lasry–Lions–Moll (GLLM) two-type growth model;
  - a block-scale jump-diffusion.
- Measure the boundary ratio, retention curves and the shape split for each (extend `runs/2026-10-15_boundary_robustness/planted_rank_swap.py` and `planted_robustness.py`).
- Add the sharper prediction set out in section 3.1, where the third and fourth cumulants of lasting increments are measured directly. The ratio then becomes a prediction with a correction term, not a test against 1.
- *Figure.* A data collapse: measured over predicted for 4 ladders × 7 ranks × each start, with every rival's band drawn on the same axes.

**Claim 2. Concentration is out of equilibrium and relaxes slowly, slowest at the top.**
- Recast S as an inverse Pareto exponent (ζ = 1/S). Then S* = σ²/(2·net slide) is Gabaix's ζ = 2g/σ² applied locally, and the relaxation times of §2z-cp are GLLM's question of transition speed.
- *Analysis.*
  - Track S*(t) and S(t) by rank at every half-year origin, with bands (from the cells of `mts_balance_law.py run-halfyear`), in arithmetic karma shares.
  - Test whether S moves toward S* at the predicted rate.
  - Compare GLLM's speed formula. Where observed change is faster than random growth allows — the comments head gained 0.175 in one half-year (§2z-cr) against τ ≈ 10 years — show which GLLM remedy the data pick: type heterogeneity (your two-speed jumps) or scale-dependent drift (your shifts of drift by rank). That connects online attention to the central puzzle of inequality dynamics.

**Claim 3. A dated feed change moved drift by rank and mobility, and the law turns that into a long-run concentration effect.**
- *Analysis.* An event study on the daily panels:
  - placebo dates in late June 2019, 2020 and 2022;
  - controls: Reddit comments, and submissions ranks 1,250–5,000;
  - a check against the archive's capture-delay change on 1 July 2021;
  - then the counterfactual S* with pre-change drift.
- *Figure.* Event-study coefficients for the head's drift and crossings, with the counterfactual path of concentration.

**Claim 4. An inequality impact assessment for algorithm changes.**
- §2z-ci shows a change in drift by rank is detectable within weeks in large strata. Combined with the law and the relaxation time, that is a method for auditing, from public ranked data, what a recommender change does to concentration.
- This is the citation hook for platform governance (DSA Art. 34 systemic-risk assessment, Art. 40 vetted-researcher data access).

**How A and B relate.**
- A is the individual process and measurement model. It carries the program's only registered held-out success (E2, 0.032 against 0.171, §2z-q).
- B is the aggregation law. One paper, B-led, with A in Methods and the SI as the validated micro-model.
- The integration is not yet a result:
  - A's stationary head is more concentrated than observed, while B's sustained head is less (BIG_PICTURE §1, corrected). So the stationary home ladder is the closure that has to go.
  - A's registered failure of s to transport (0.65→0.83) and B's 16–55% rise in lasting variance after June 2021 (§2z-co) start at the same date. Test whether they are the same fact.

**A target that seems unreasonable.** "From a year of public ranked data, we forecast a platform's attention concentration years ahead and attribute its change to a dated design decision." The solvable version:
- forecast the direction and a speed bound for the submissions head across 2023–24;
- register a deceleration test (the law) against linear trend;
- measure the June 2021 counterfactual within the development data.

## 3. The short seller's case

### 3.1 "The boundary theory is a falsifiable law of the dynamics, the result that separates the law from accounting" (C3, §2z-cm)

**Evidence against.**
- *Theorist.* On a locally exponential ladder with increments that do not depend on position, the whole ladder shifts by S·log E[e^{Δ/S}]. So R̃ = S·κ(1/S) = σ²/(2S)·[1 + κ₃/(3σ²S) + κ₄/(12σ²S²) + …].
- That is the occupation-density (local-time) formula, and it holds at every instant. "Out of equilibrium" adds no difficulty: the derivation in `BALANCE_LAW_THEORY_NOTE_2026-10-14.md` §2 never uses stationarity.
- The ratio can leave 1 only through four routes: increments that depend on position, cumulants at scale S, a mismatch between the cohort and the boundary's occupants, or a misread density. That is exactly what the planted worlds show:
  - Student-t tails, jumps and drift types all give 0.85–1.25;
  - rank-exchange processes fail;
  - fixed volatility types still sorting into the head give 1.20–1.30 in the body (§2z-cq).
- The registered band [0.75, 1.33] contains that rival's body values. At the two cut-offs with tight intervals, comments gives 1.11 (1.04–1.18) at K/2 and 1.20 (1.14–1.26) at K. Those intervals exclude 1. The sorting world gives 1.29 ± 0.01 and 1.20 ± 0.06 at the same relative ranks; the plain walk gives 1.02 at K.
- At the top 98 (the K = 6,250 run, recomputed) the ratio is 1.82 (1.17–2.65), close to the sorting world's head value (2.41 ± 0.56 at k = 100).
- The choice of estimator moves a single rank's ratio by more than the gap between the rivals:
  - k = 195 is 1.22 (26-week blocks), 0.99 (variogram) and 0.72 (13-week blocks);
  - k = 781 is 1.08, 0.87 and 1.41 (1.05–2.28).
- The registered pooled test takes σ² from blocks 1–5 and crossings from blocks 3–7, while lasting variance rose after mid-2021. The time-aligned ratios at the comments head are 1.01, 0.90 and 0.86, and k = 781 in start 2 is 0.71 (0.57–0.90) (`by_start_post_hoc_output.txt`).

**How a hostile referee would put it.** "The authors check the occupation-density identity with plug-in estimates, call it a law, and set the pass band wide enough to admit the one non-trivial alternative."

**Fix or retire.** Recast it as a test that discriminates between model classes: level diffusion against rank exchange. Specifically:
- predict crossings from σ², S and the cumulants of an earlier period;
- set an equivalence margin justified by power against fitted rivals;
- report one envelope across estimators.

Retire "law of the dynamics" and the "out-of-equilibrium" selling point.

### 3.2 "Rank-size curves keep their form" (the puzzle; the outline's title; the binding §6 framing)

**Evidence against.**
- Submissions top 78: 51.1% to 34.8%. Comments top 195: 63.2% to 51.9% (§2z-cw).
- The sustained head slope on submissions is 0.25–0.46 against 0.84–0.98 observed (§2z-cp).
- The comments head changes by −0.20 to +0.175 log units a half-year (§2z-cr).
- The left panel of `fig_puzzle.png` shows the submissions head curves moving visibly.
- The record itself concludes that "no era is stationary" (BIG_PICTURE §2).

**Referee.** "The motivating puzzle is contradicted by the paper's own Figures 4 and 6."

**Fix.** Retire "stable form" and "Eulerian stability". The real puzzle is the gap in speed: names move about 0.2 log-rank a year while the shape moves slowly and occasionally fast. That is GLLM's question, and the record has the ingredients to answer it. Batty's (2006) rank clocks and Duranton (2007) are the precedents.

### 3.3 "Ex ante lock-in forecasts discriminate who stays as well as a fitted model; incumbents are stickier than movement implies" (C11, §2z-cy, Reviewer Q3a)

**Evidence against** (recomputed from `runs/2026-10-15_lockin/lockin__*.json`).
- The logistic on log rank and log horizon beats the movement forecast on Brier score on every ladder: 0.1244 against 0.1252, 0.1309 against 0.1357, 0.1587 against 0.1607, 0.1615 against 0.1621.
- On aggregate retention error it is roughly twice as good: MAE 0.022/0.035, 0.031/0.069, 0.028/0.073, 0.018/0.030.
- Share of cells within ±0.07 (T2):

| forecaster | share of 126 cells |
|---|---|
| movement forecast (registered) | 75.4% |
| empirical-shape variant | 84.1% |
| constant turnover | 88.1% |
| logistic | 92.9% |

- At h ≥ 3 the MAE is 0.063 for the movement model, 0.044 for constant turnover and 0.034 for the logistic.
- Being "outcome-free" is no information advantage: the training retention outcomes are known at the origin.
- The "stickiness" halves under the empirical shape of changes (§2z-cz), so it is mostly leptokurtosis seen through a Gaussian forecaster. It is not lock-in in the sense of cumulative advantage.

**Referee.** "Rank alone forecasts retention better than the physics. What does the model add?"

**Fix.**
- Compare against the logistic, not constant turnover.
- Register the empirical-shape/jump forecaster.
- Claim only what the model uniquely offers: a forecast of retention under changed mobility. Test it by forecasting post-June-2021 retention from post-change movement, against a logistic fitted before the change.
- Drop "the opposite of the platforms' claim" (§2z-cy) unless that claim is sourced and tested.

### 3.4 "After the 2021 Home-feed change the drift of the very top fell within days and its mobility tripled" (draft abstract; C10)

**Evidence against.**
- Onset is 23–24 June, before the announced 28 June (§2z-ca).
- "Tripled" means K/64 crossings went from 2.35 (0.80–3.79) to 7.82 (3.75–13.69). That rests on 78 communities in two adjacent windows whose intervals touch, and was unscored and unregistered (§2z-cn).
- In the same windows the comments head's crossings rose from 3.90 to 5.46, and lasting variance rose 1.16–1.39× on comments (§2z-co), with no feed story.
- From 1 July 2021 the archive's capture delay changes from about 20 to about 360 days (§2z-bz). That puts every "after" quantity under a different measurement regime. §2z-ca cleared the level step of this, not the rise in variance.
- The intervention forecasts failed (J1, J2).

**Referee.** "No control, a measurement change at the same moment, and an unregistered contrast on 78 units."

**Fix.** The event study of section 2, Claim 3: placebo years, comments and lower ranks as difference-in-differences controls, a check on score age at capture. Register the June 2023 blackout as a second event in the sealed protocol.

### 3.5 "Concentration fell from 51% to 35% and current movement sustains 16%" (C8, abstract, `fig_concentration.png`)

**Evidence against.**
- §2z-cw says the quantity is an index of exp(mean log activity), "not the share of total karma". Yet the abstract and the figure's y-axis say "share … activity".
- A geometric mean is pulled down by volatility within the block. That volatility rose at the head after June 2021 (implied variance at K/64 went 0.0036 → 0.0147, §2z-cn), so part of the fall may be Jensen's inequality.
- S* comes from drift by rank that turns every half-year on comments (§2z-cg). Consecutive years give a sustained top-195 share of 44% and 53%.
- σ² is measured once per panel (§2z-cv item 3), so the two "sustained" ladders differ only in slides.
- "Falling entry steepens inequality" rests on two yearly pairs, one of them containing 2020 (§2z-ck).

**Referee.** "A counterfactual built on a quantity the authors show is unstable, in a unit they mislabel."

**Fix.** Arithmetic karma shares; bands on S* from variation by start; S*(t) at every half-year; an in-sample test of the predicted relaxation before any destination claim.

## 4. Fair criticisms of the models

**The balance law and its estimators.**
- C2 (the exact split) is an identity. C6 (the same-year rate law: slope 0.88, r 0.90) is largely the correlation of the members' slide with itself, as §2z-cg concedes; it should not be a main figure.
- The weight-carrying assumptions:
  - increments do not depend on position within about √V of the boundary;
  - the cohort's stratum variance equals the occupants' variance;
  - n = k/S;
  - "lasting" means a random walk.
- The last is not identified against slow reversion over a 4-year panel (§2z-bm proves non-identification in the general Itô class). A pure OU process with a half-life over a year also gives a positive "variance route" and growing R(D). So B cannot tell A's world (homes) from its own (slides).
- The stratum variances are noisy and not monotone. On submissions, adjacent strata read 0.0033 (n = 145) and 0.0141 (n = 279). The "persistent shortfall at 300–600" may be that one stratum, since `_at_boundary` averages only two strata.
- Both sides are conditioned on survivors: the persisting set P, with attrition of 5–7% a year in the bottom bins (§2z-cc).
- The bootstraps resample communities independently. They ignore common shocks and the fact that there are 3 overlapping starts (Reddit) or 1 (Facebook, Instagram). They also pair independent draws of R and σ² from the same endpoints (`mts_boundary_law.py` lines 119 and 138–142).

**The permanent–transitory model (A).**
- The heaviest assumptions are a stationary home ladder with Gabaix rebirth, separable rank × amplitude structure (b = 1), and roughly Gaussian shocks.
- The least tested is stationarity, which B contradicts. On the record:
  - amplitude spread drifted (s 0.65→0.84);
  - κ failed to transport;
  - the head is 1.4–2.9× too concentrated (§2z-q E5, §2z-av);
  - upper tails are under-dispersed;
  - proper scores are only at par with persistence.
- A's validated success is medians of the displacement distribution; it does not forecast individual endpoints.
- A is a hybrid Atlas model (Ichiba et al. 2011) and should be positioned as one.

**The lock-in forecaster.**
- Assumptions: Gaussian shape, variance linear in horizon, homogeneity within a band, and strata of current rank (`mts_lockin.py` lines 5, 79–81).
- Current-rank strata defensibly depart from the AGENTS.md rule "never current rank", but the departure is not declared as one.
- Least tested is horizon extrapolation, and it fails (T3). The model contributes nothing a logistic on rank does not, except a capacity for counterfactuals that is untested.

**The Instagram censoring treatment.**
- The weight-carrying assumption is that q_i (the "a"-match share) is persistent and independent of level and dynamics.
- In reality q is chiefly language and caption style: a standalone "a" is common in English, Spanish and Portuguese and absent in German, Japanese or Arabic. 2023 was also the year of Reels, which brought shorter captions and higher engagement, so q drift would be negatively correlated with engagement change.
- The planted world (`ig_censoring_sim.py`) has homogeneous σ², an exactly exponential ladder, and q independent of everything. Its validation therefore shows only that censoring does not bias a near-identity.
- Both sides are displacement variances at 14–28 weeks (§2z-cx), so on Instagram the test is closest to the identity. The scope should be "a-captioning, mostly English and Romance-language accounts".

## 5. What we are not seeing

**Blind spots.**
- The same-window, same-moment design of every B test.
- No test can fail for reasons other than estimator noise, except rank exchange.
- The development panels were read many times before "registered" runs: §2z-cd admits the run "was not blind".
- There are forking paths across K, block length, estimator and cut-off range: 13-week blocks, K = 6,250, and choosing K/16…K for Facebook and Instagram.

**Alternative explanations.**
- The archive's capture-delay regime from July 2021, for the rise in variance, A's s failure and the June 2021 mobility contrast.
- The 2020 pandemic growth year, for "falling entry".
- Name churn in Facebook page ids (MODEL_STATUS §2g), which shows up as exit plus entry.
- CrowdTangle's frozen inclusion lists (§2g), which censor risers.
- The June 2020 Reddit ban wave and quarantines, which arrive as exits.
- Vote fuzzing, and removals/deletions captured differently at a 360-day delay.

**Literatures missed** (not in the theory note, the novelty search or the outline).

| field | works |
|---|---|
| rank-dynamics precedents | Batty 2006 "Rank clocks" (Nature); Duranton 2007 (AER: stable Zipf, churning city ranks) |
| size–variance scaling | Stanley et al. 1996 (Nature); Riccaboni et al. 2008 (PNAS) — comparable to your variance falling with rank |
| entry and the Pareto tail | Jones & Kim 2018 (entry/creative destruction flattens the Pareto tail; your §2z-ck); Aoki & Nirei 2017; Luttmer 2011 |
| earnings dynamics | Guvenen, Karahan, Ozkan & Song 2021 (Econometrica, kurtosis: the "two-speed" fact); Arellano, Blundell & Bonhomme 2017; MaCurdy 1982 and Abowd & Card 1989 (your "differences of spacings" is their permanent–transitory device, which weakens novelty claim b) |
| rank-based diffusion theory | Kolli & Shkolnikov 2018 (finite-N fluctuations: a proper null for R̃); Karatzas & Sarantsev 2016 (entry/exit) |
| online attention and platforms | Salganik, Dodds & Watts 2006; van de Rijt et al. 2014 (PNAS, success-breeds-success); Lorenz-Spreen et al. 2019; Weng et al. 2012; Stoddard 2015 (Reddit popularity); Huszár et al. 2022 (PNAS) and Guess et al. 2023 (algorithmic amplification) |
| data quality | Gaffney & Matias 2018 (Pushshift missing data) |

**Data features ignored.**
- Turnover is nearly invariant to scale: 0.79/0.82/0.84 a year at k = 195, 1,562 and 12,500 (§2z-cu), which is a rank-diversity regularity in its own right.
- The size–variance exponent.
- Instagram's post-level dailies, which could measure q directly (§2z-cx Limits).
- Comments and submissions as a paired within-platform control.

**Mechanisms that explain the same patterns.**
- Any level-based multiplicative process passes the boundary test, including cumulative advantage, copying and Hawkes-type bursts. The test therefore says nothing about the mechanism of interest (rich-get-richer).
- Slow reversion to a home also fits "lasting" variance.
- Volatility types still sorting fit the comments ratios at K and K/2.

## 6. What experts would require of a PNAS-class paper

- **Theory.** A statement of which results are identities, near-identities and predictions. The cumulant-corrected boundary formula. A finite-N null distribution for R̃. The precise map to Fernholz's gap identity, GLLM's speeds and Baker & Smyth.
- **Out of sample.** At least one registered success on held-out time: the bridge, Wikipedia, or within-development out-of-time tests labelled as such.
- **Controls.** Fitted rival models scored by the identical estimators, and the logistic baseline throughout.
- **Causal design** for any platform claim: event study, placebos, control series, measurement-regime checks.
- **Robustness in one SI table.** Every variant of K, block length, estimator, presence rule and universe. Arithmetic against geometric shares. Leave-out of flagged weeks.
- **Inference.** Bootstrap over time blocks, adjustment for common factors, a multiple-testing ledger across the 26 scored items.
- **Figures.** A data collapse across platforms. Event-study coefficients. S*(t) against S(t) with bands. The lock-in figure with the logistic shown.
- **Reproducibility and availability.**
  - Deposit anonymized derived weekly panels.
  - State the Pushshift / Arctic Shift provenance and their access status after 2023.
  - Give a CrowdTangle replacement statement (Meta Content Library).
  - Provide one script that rebuilds every figure from deposited panels.

## 7. Prioritised change list

**Now, with development data.**

1. **Out-of-time boundary prediction with an equivalence margin justified by power.**
   - What: σ², S and cumulants from start a; crossings from a + 2; one envelope across estimators.
   - Data and code: `mts_boundary_law.py`, `by_start_post_hoc.py`; comments, submissions, `facebook_a`.
   - Payoff: turns the near-identity into a prediction.
   - Effort: days. Risk: the head may fail, since c rose 3.9→6.4 across starts.
2. **Fitted rival classes run through the identical estimators.**
   - What: Iñiguez rank-displacement, Yule–Simon, GLLM two-type, block-jump diffusion.
   - Code: `planted_rank_swap.py`, `planted_robustness.py`.
   - Payoff: "level, not rank, two-speed" becomes a model-selection result.
   - Effort: 1–2 weeks. Risk: low.
3. **Lock-in reset.**
   - What: the logistic as the baseline; register the empirical-shape/jump forecaster; a counterfactual retention test across June 2021; remove the platform-claim language.
   - Code: `mts_lockin.py`.
   - Effort: days. Risk: the physics may still lose; then report it as a diagnostic.
4. **June 2021 event study.**
   - What: daily panels (T9, DEVELOPMENT), placebo years, comments and lower ranks as difference-in-differences controls, a capture-age check.
   - Payoff: the paper's social-science core.
   - Effort: 1–2 weeks. Risk: part of the effect may be measurement.
5. **Arithmetic shares and the speed test.**
   - What: arithmetic karma shares with bands; S*(t) at every half-year; observed against predicted relaxation; GLLM comparison.
   - Code: `runs/2026-10-15_concentration/`, `mts_balance_law.py run-halfyear`.
   - Effort: days.
6. **A↔B integration.**
   - What: B's decomposition on A's simulated panels (`minimal_rankdiff.py`); an opt-in time-varying drift by rank; test whether the s trend equals the post-2021 rise in variance.
   - Payoff: one paper.
   - Effort: weeks. Risk: medium.
7. **Inference overhaul.** Bootstrap over time blocks, adjustment for common factors, a ledger of multiplicity and forking paths.
8. **Size–variance exponent and a turnover data collapse across ladders.** Low effort; links to the firm and city literatures.
9. **Fix claim language, figures and the errors in section 8.** Retitle `fig_four_ladders`; add the logistic to `fig_lockin`; move C6 to the SI.

**Needs the owner, sealed data or new data.**

10. **Owner:** one paper; amend the binding §6 paragraph; retire the "Eulerian stability" title.
11. **Owner + sealed: the Reddit 2023–24 bridge** under a re-registered protocol (§2z-cp revised). Its primary tests should be ones that discriminate:
    - out-of-time boundary ratios at a tight margin;
    - the empirical-shape lock-in forecast against the logistic;
    - deceleration of the submissions head against linear trend;
    - the June 2023 blackout with comments as control.
    - Payoff: the single largest. Risk: genuine, which is the point.
12. **Owner + sealed: Wikipedia pageviews** as a fifth ladder — views rather than karma, over a long span. Answers "karma isn't attention" and "short spans".
13. **Owner: names for event analyses** (ban waves, quarantines, NSFW and default status), under the privacy rules.
14. **Data-availability package:** deposited derived panels and the provenance statements of section 6.

## 8. Errors

1. **`fig_concentration.png` y-axis, the abstract (`PAPER_FRAME_DRAFT_2026-10-15.md` lines 52–53), and C8.** They say "share … activity". §2z-cw defines an index of geometric-mean activity, not karma.
2. **C11 (`EVIDENCE_PACKET`), the §2z-cy headline, `PAPER_FRAME` lines 112–114, `REVIEWER_QUESTIONS` Q3a.** "At par with / as well as a logistic fitted to outcomes" is wrong. Recomputed, the logistic is better on every ladder on Brier and about twice as good on aggregate error. T2 is 75.4% against the logistic's 92.9% and constant turnover's 88.1%.
3. **`BIG_PICTURE_REVIEW_2026-10-18.md` lines 171–172** still say "B explains A's main failure", contradicting the corrected §1 (lines 44–57).
4. **`BIG_PICTURE` lines 94–95.** "Every test was designed with predictions committed before the data were read" is false:
   - §2z-cd says it was "not blind";
   - C2, C8, C9, the by-start r = 0.94, the June 2021 mobility contrast and the lock-in repair are all post hoc.
5. **`REVIEWER_QUESTIONS` line 173 and §2z-cu §4.** "9 of 17 held, one partly not adjudicated (§2z-cu)": the not-adjudicated item lies outside that 17. The packet's count is 15 of 26.
6. **The packet's scoreboard** counts I3 and J3 (predicted non-adjudication or unforecastability) as "held". Excluding them, 13 of the 24 remaining predictions held.
7. **Head-variance precision.** "±30% (§2z-co)" (`EVIDENCE_PACKET` lines 78–80, `BIG_PICTURE` lines 83–84) disagrees with "±40%" in §2z-cm (MODEL_STATUS line 9820). §2z-co gives no ±30% figure.
8. **`PAPER_FRAME` lines 44–46.** "At every rank from the top 200 … over time and with two estimators" is contradicted by:
   - 0.72 at k = 195 and 1.41 at k = 781 with 13-week blocks (§2z-cu);
   - 0.71 at k = 781 in start 2;
   - 1.82 at the top 98.
9. **The significance statement (`PAPER_FRAME` line 23) and the title of `fig_four_ladders.png`** ("One law on four ladders") omit submissions' failures: B1, V3 and R2 all FAIL.
10. **C3's ranges** give Facebook as 0.77–1.17 and Instagram as 0.94–1.12 under a K/64…K label. They leave out Facebook's 0.71 at K/64 and Instagram's 2.49 at K/64 and 1.24 at K/32.
11. **"K: the ratio does not depend on K"** (§2z-cu R1/R2, `REVIEWER_QUESTIONS` Q7) is an algebraic invariance of the estimator — crossings and strata at rank k do not involve K — not a robustness result.
12. **`PAPER_FRAME` lines 48–49.** "Fell within days" mis-orders cause and effect, since onset (23–24 June) precedes the announced date. "Tripled" is unregistered, with intervals that touch (§2z-ca, §2z-cn).
13. **`fig_puzzle.png` title and the outline's title / §6 framing** say "keeps its form" / "stationary", contradicted by §2z-cw and by the figure's own submissions panel.
14. **`mts_lockin.py`** stratifies by current rank, against AGENTS.md, without declaring the deviation in `runs/2026-10-15_lockin/DESIGN.md`.
15. **`REVIEWER_QUESTIONS` Q2.** Citing the submissions head's 1.6–1.9 as failing "where it should" is circular, because the failure is attributed to noise.
16. **The registered pooled ratios are misaligned in time.** σ² comes from blocks 1–5 and crossings from blocks 3–7. This inflates the comments head (1.22 registered against 0.86–1.01 aligned) and should be disclosed beside every pooled number.
