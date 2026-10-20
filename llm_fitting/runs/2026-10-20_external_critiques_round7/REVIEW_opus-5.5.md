_Round-7 review by a fresh Claude Opus 5.5 (Anthropic), run as a read-only sub-agent of the orchestrating Claude Code session, 2026-10-20, from `CRITIQUE_PROMPT_R7.md` (committed 9d78e65 before the review; manuscript v5 at 61b6bbe). Returned as the agent's final message (hand-back) and extracted verbatim from its transcript by the orchestrating session. Model output: checked in `SYNTHESIS_R7.md`._

Claude Opus 5.5 (a fresh instance), 2026-10-20. I worked read-only from `llm_fitting/runs/2026-10-20_external_critiques_round7/CRITIQUE_PROMPT_R7.md`. I wrote no files, ran no model and opened no panel. Every recomputation below comes from saved JSON or text outputs, printed to stdout.

# Referee report, round 7 (manuscript v5, 61b6bbe)

## 1. Editor's decision now

**Send out, without enthusiasm.**

1. The question is of broad interest, and the work is unusually careful and candid about registration.
2. The positive core is that durability is what homogeneous random growth implies. The paper says this is known in principle, so the novelty is measurement on platform data.
3. The displayed evidence for that core is thin. The observed shape path is never plotted against the prediction. The saved ladders show both heads flattening by about 0.65 log units over 2019–22 (§7, E1).
4. The two discoveries are left unexplained. The submissions head's fall is a shift in its slide, and the persistence is asymmetric. The analysis meant to explain the persistence (R12) is a rejected calibrated model.
5. The abstract and Discussion end on negatives. An editor reads "measured a known property; standard explanations of the departures fail" and sends it out, but not eagerly.

**Single change that most raises the odds.** Make durability a displayed prediction. Plot the observed 2019–22 shape at every cut-off, on both ladders, against the rate-law and model forecasts (Fig. 3B and the SI). Then state the claim the comparison supports. That turns "random growth implies" into "random growth predicted, and here is where it failed."

## 2. The frame and the title

**GLLM is the right reference, but the fit is not exact.**
- GLLM's fast-transition problem is about speed of response. After a parameter change, the homogeneous model's cross-section converges too slowly. Heterogeneity (type switching) or scale dependence speeds the response.
- The paper's model already has rank-dependent drift, g(r), which is scale dependence in GLLM's sense. Given the measured 2020–22 slide, the rate law reproduces the submissions head's fall: −0.23 and −0.31 a year predicted, against −0.29 and −0.21 observed (`2026-10-14_balance_law/cells/law__reddit_submissions_long.json`, K/64). The SI also gives a relaxation time of 3–4 years there.
- So at the submissions head there is no GLLM speed failure. There is a shock: the slide quadrupled, from Gomez within-terms of −0.06 to −0.24 and −0.26.
- In GLLM, heterogeneity governs how fast a shock propagates, not whether a shock happens. Asking heterogeneity calibrated on 2019–20 to forecast the 2021–22 fall (R12-3, Fig. 4A) asks it to do something GLLM never claimed.
- Recommended framing: durability is GLLM's slow transition, measured. The head's fall is a drift shift that rank-based growth transmits quickly. Persistence is a separate discovery.
- R12 tested income-dynamics forms (HIP, AR, factor), not GLLM's own mechanism, which is type switching. "In income dynamics fast changes in concentration likewise require growth that differs between individuals" (l.178; also l.95) should say which form GLLM used, and that scale dependence is GLLM's other channel.

**Stochastic portfolio theory and rank-based diffusions.**
- The Atlas slope S* = σ²/2g is correctly attributed (Fernholz & Fernholz 2020, eq. 2.17).
- The crossing gain is SPT's local-time term at the rank-k/k+1 boundary. Say "local time" once, so a theorist recognizes it.
- The durability factor is τ/movement = 2 ln(K/k)(S*/S)². Two things in it come from the analyst, not the platform: ln(K/k), the number of e-folds between the top set and the edge of the universe, and S*/S = c/(s̄−e). This belongs in the main text, not only SI l.118.
- Cite the rank-based convergence-rate literature: Ichiba, Pal & Shkolnikov (2013, PTRF), and for the infinite Atlas model, where no spectral gap exists, work by Dembo and coauthors. Please verify these references. Then the reflecting-barrier λ reads explicitly as an approximation.
- Pal & Pitman 2008 is in `references.bib` but cited nowhere.

**Gabaix.** Correctly cited for the barrier (l.95).

**Gomez.**
- The decomposition is used well.
- The record (§2z-c…, MODEL_STATUS l.~11430) notes that at a Zipf-like head, displacement supports the arithmetic share about ten times less than the mean-log index (0.05σ² against 0.55σ²).
- That is why "the shape is durable" (mean log) and "concentration fell fast" (arithmetic share) can both hold. The paper uses one estimand for durability and the other for the departure without saying why. A referee will say it picked the measure to fit each claim. One paragraph fixes this.

**Baker & Smyth (2026).** Mapping their turnover to e_k (l.135) is right. "In attention data both terms matter" is fair, given e = −0.08 a year at the comments head (Fig. 1A).

**Platform research.** Mostly well positioned (Boik, Veselovsky, TeBlunthuis). Several directly relevant entries are in `references.bib` but cited in neither document:
- Gleeson et al. 2014, a PNAS generative model of online popularity, which the editor will expect;
- Ratkiewicz et al. 2010;
- Blumm et al. 2012 on rank stability;
- Luttmer 2011, the firm-growth analogue of GLLM's problem and the natural citation for persistent decline;
- van de Rijt et al. 2014;
- Houssard et al. 2023 (a stable Twitch top);
- Kairam et al. 2012.

**Novelty claim.** "Not widely used in their field" (l.226) is about right. Add one explicit sentence: the separation of time scales is standard (GLLM; Gabaix 2009), and the contribution is to measure it rank by rank.

**Title.** "When" is earned only conditionally (§3). The weakest word is **"attention"**: the paper itself says these are "measures of engaged activity, not of views" (l.105). Use "online activity", or keep "attention" and defend it in the first paragraph. "Outlasts" is earned only relative to turnover.

## 3. Each Results section

**R1, turnover against a slowly changing shape.**
- Turnover is solid.
- The shape claim is the one a referee will attack first. "At no cut-off and in no year did it move by more than a third of a log unit" (l.110) fails on the ladders plotted in Fig. 1B (E1).
- Over two years, "1.3–1.7 times … because many moves are passing swings that reverse" misattributes the cause. Pure diffusion gives √2 = 1.41 with no reversals, because E[D⁺] ∝ σ√t (SI Eq. S2). Drift and exits push the ratio toward 2. R13-1's rationale is therefore wrong, even though the expectation passed.

**R2, the balance and the time scales.**
- The weakest link is the durability factor. It depends on the lasting variance, which has three estimators that disagree by about 2× at the comments head:
  - three-block: σ² ≈ 0.0068–0.008 a week for ranks 1–781 (`law__reddit_comments_ext.json`, the "variance" strata);
  - the R12 two-spacing statistic: 0.0039 (`r12_moments__reddit_comments_ext.json`, sf[0]);
  - the model's structure function: recovers σ0 24% high, and β 0.069 for 0.126.
- With S* = σ²/2(s̄−e), the ratio τ/movement scales as σ⁴. At the two-spacing value, the comments head's factor falls from about 3.9 to about 1.3.
- A second route has the same effect. The measured crossing statistic includes a composition term C, +25–35% on Reddit, which the paper attributes to heterogeneity (l.138). Under the homogeneous reading the paper invokes, the relevant c is the lasting part. That gives c/(s̄−e) ≈ 0.45 and a factor of about 1.7 at K/64.
- Report C in the simulated worlds. If C ≈ 0 there, the data's lasting crossings are about 0.7 of σ²/2S, not 1.07.

**R3, the generative model.**
- "A year ahead the model forecasts membership well" (l.166) is contradicted on comments: MAE 0.022 against 0.013 for carry-forward. On submissions it is beaten by the older fixed-home model (0.024, SI l.197).
- The head-flattening result is solid and honestly reported.

**R4, the submissions head.**
- The out-of-period miss and the posting split are solid, and the posting split is undersold.
- The claim to attack: "The rate law with the measured 2020–22 slide accounts for the fall" (l.178). Eq. 2 with the measured slide is nearly an accounting identity. The SI calls it "an accounting".
- The comments head also fell below its band (0.505 against 0.547–0.670). The heading and Significance imply it did not.

**R5, persistence.**
- The asymmetry is strong: fallers +0.16–0.19, risers 0.00–0.03.
- Two claims to attack:
  - "On submissions … removing each community's loading on the platform's total activity … removes it" (l.188). See §7, E9: this may be an artifact of noisy loadings. Even taken at face value, +0.13 remains at gap 0, and +0.06 at 13 weeks with membership from the first half-year.
  - "On Facebook pages it is negative at every gap." At Facebook's settings and dates, Reddit is only −0.04 to +0.08 (SI l.225). Report the matched comparison.

## 4. Heterogeneous growth (R12)

**Design.** The three forms, the indirect-inference language, the matched control (A4), recovery against s = 0 on common seeds, and pre-written scoring are standard and well executed. The A3 and A4 disclosures are exemplary. It is still not sufficient:

- **The passing parts are frozen at their homogeneous estimates.** The income literature (Guvenen 2009; Hryshko 2012; Guvenen & Smith 2014) estimates permanent, transitory and heterogeneity components jointly. Here the frozen medium swing drives the result:
  - The homogeneous worlds give year-apart covariances of −0.05 to −0.13 at gap 0. They are still −0.04 to −0.05 at gap 52 (`r12_world__reddit_comments_ext__control_num__s0.json`, r12_aux).
  - The data's head stratum is ≈0 (−0.001 submissions, +0.011 comments).
  - Under fixed heterogeneity the covariance is s². The data's body value of 0.067 implies s ≈ 0.26 with no negative baseline. The world needs s = 0.5 only to climb out of −0.10.
  - The world's lag profile rises with the gap (fixed, comments: +0.010, +0.069, +0.094) while the data's falls (+0.145, +0.106, +0.095). That is the signature of a too-strong reverting medium swing.
  - The SI's synthetic check gets +0.17 to +0.19 from a spread of 0.4 in a simple process (l.209).
  - So the negative verdict is about heterogeneity combined with this model's medium swing and its ladder-holding drift. A drift that holds the ladder is a restoring force that offsets name-specific drift (FIK 2013).
- **No overidentification statement.** An objective of 351 over 9 moments with 2 parameters is a decisive rejection. Say "rejected", and give the J statistic.
- **The optimum is a compromise, and grid noise matters.** From `r12_grid*__fixed.json`, I split the objective into its covariance part and its lasting-variance part.
  - On submissions, in the extended grid, the covariance part is minimised at s 0.6 (36.4), not at 0.5 (94.7).
  - The total is 351 at 0.5 against 362 at 0.6, with 3 seeds a point, which is within simulation noise.
  - So "the minimum lies near 0.5" really means the compromise between conflicting moments sits there.
  - R12-1's upper bound of 0.5 equals the grid's edge, so it was a weak expectation.
- **The heterogeneity is the same at every rank.** The data's covariance grows down the ladder. Let σ_b differ by stratum (three parameters), as income work lets it vary by group.
- **No Monte Carlo of the estimator itself.** Plant s, run the indirect inference, recover ŝ. Recovery of σ0 and β is not recovery of the calibration.
- **Strata use whole-window permanent rank** (`r12_hetgrowth.py` `strata`, l.58–66). This is look-ahead against the paper's own rule (Methods l.244). It is harmless as an indirect-inference auxiliary statistic, but not for the descriptive claim "concentrated below the head". Persistent decliners from the head get reassigned to lower strata.
- **The comments boundary ratio** fails on an indexing limit (`mts_boundary_law.analyse` indexes rank K; 12,229 < 12,500). That is a code fix (use K′ = 12,000), not a limitation.

**The conclusions, and the strength each can bear.**
- "At most three fifths of the persistence": true at 13 and 26 weeks. False at 52 weeks on comments (+0.094 against +0.095). Restate it as "a quarter to two fifths at a 13-week gap, with a lag profile of the wrong shape."
- "Little of the head fall": supported. The paired effect is −0.029, with a 5–95% range of −0.42 to +0.10.
  - Explain the upper tails: the matched control's band reaches 0.897, and the fixed-differences comments world reaches 0.964. Worlds in which the top K/64 hold 90% of the activity are not credible.
- "What persists is decline, concentrated below the head":
  - The decline half is supported by the asymmetry.
  - "Below the head" rests on look-ahead strata, and conflicts with R10 at k = 78: Q1 slides at 0.0116 against 0.0074 for the rest, the largest contrast of any cut-off.
- "The head's fall is a shift in its drift after 2020": too strong. The data show the slide quadrupled. Whether the drift shifted or the head filled with decliners is not identified.

**What a labour or inequality econometrician would still demand.** Joint estimation; a J test; rank-specific spread; non-Gaussian, skewed heterogeneity; recovery of the calibration; a power statement for the out-of-period share (what spread would put 0.356 inside the band); simulation standard errors of the objective.

**R14 (mirrored type switching).** It is the right family: it is GLLM-faithful and asymmetric. But a stationary switching process calibrated on 2019–20 cannot forecast a 2021-specific acceleration unless its entry hazard changes, so its out-of-period test will likely add another negative. It would also lose to the same −0.10 baseline unless estimated jointly with the medium swing.

Before submission I would instead:
- re-run R12 with the medium swing freed (κ and its variance) and rank-specific spread;
- add cheap nonparametric diagnostics of decline spells: are run lengths geometric, and what is the hazard of leaving decline?

Then register R14 for the sealed data. It would not change a referee's view enough to delay submission.

## 5. What the paper should claim

**Contributions, as I would state them:**
1. Measured on platform data, the random-growth balance is c/(s̄−e) = 0.64 (comments 0.68, submissions 0.61). This implies a shape that changes several times more slowly than membership where the decline is moderate. The factor is 2 ln(K/k)(c/(s̄−e))², about 3–4 at the comments head and about 2 at the submissions head. This is conditional on the lasting-variance and composition issues in §3.
2. Geometry, not size protection, orders turnover across top sets. Turnover profiles need a ladder control (Δb ≈ 0.11). This is a measurement lesson for cultural evolution and for cross-platform comparisons.
3. Tools: a selection baseline (about −5% of share a year); an exact accounting, combining Gomez's within and between terms with an items × per-item split; and a benchmark run from the platform's own ladder.
4. A discovery: Reddit's changes persist across disjoint periods, asymmetrically and in participation, where every homogeneous world is negative. Standard symmetric heterogeneity, calibrated in the standard way, cannot reproduce this together with the lasting variance.
5. The submissions head fell through posting, from 22% to 9% of submissions, while karma per post rose from 2.3 to 3.9 times the universe's.

**Overstated:**
- "forecasts membership well";
- "on comments the data behave that way";
- "the two ladders favour different readings" (l.228; after R12 and E9);
- "below the 0.15–0.2 of firms" for comments;
- "nearly cancel" in the Significance (a third is not cancelled);
- the Facebook contrast in persistence;
- "a shift in its drift".

**Undersold:** the participation split, rank-one stability against the collapse of share (C(R)), the asymmetry of decline and its fit with the community life-cycle literature, and the selection baseline.

**Uses and non-uses.** The Discussion paragraphs are useful and honest. The SI tables are generic, and a referee may read them as padding. Make it concrete:
- Move the "what data a study needs" box into the main text.
- Add one worked example of re-reading a published entrenchment claim against the benchmark.
- Fix the "fourth tool" (E7).

**Horizons.** Mostly kept. The breaches:
- "two thirds within a decade" (l.163);
- Fig. 3B's 30-year axis and "Continued further … collapses";
- Fig. 2A plots durations of up to about 17 years.

Truncate Fig. 3B at 10 years and move the continuations to the SI.

## 6. Round-5 points

**Resolved:** G1, G2, G4, G5, G7 (R11), G8 (R9), G9 in design (R12), G10, G11, G13, G16–G22, G24–G26.

**Half-resolved:**
- **G3.** The main text quotes the linearized rate ("a tenth of its gap a year", l.147) next to the model's −0% in five years without reconciling them. The model relaxes in about 18–27 years.
- **G12.** The post hoc Eq. S2 turnover formula is presented in the main text without a label (l.141).
- **G14 and G15.** Now tested, with mixed results. The comments recovery bias is open.

**Open:**
- **G6.** The drift still comes only from the 2020–22 slides.
- **G23.** Protocol P5 spans the 2023 source switch and the blackout; this is an owner item.

**Round-5 single changes:**
- GPT's: done (R12).
- Fable's: net balance done; the earliest- and latest-cell reruns not done.
- Opus's: done (R10), post hoc.

## 7. Errors

**E1. The shape movement claim (l.110).** I recomputed U_k − U_K from `runs/2026-10-15_puzzle/puzzle__*.json`, the ladders Fig. 1B plots.
- At K/64, 2019→2022: comments 5.58, 5.13, 4.98, 4.95; submissions 4.57, 4.38, 4.10, 3.89.
- Comments 2019→20 moved −0.45 at K/64, −0.42 at K/32 and −0.41 at K/16, contradicting "in no year … more than a third".
- My 2020–21 and 2021–22 values match `obs_all` in the balance-law cells to within 0.01.
- Both heads fell by about 0.65 log units in three years. The model from the 2019 ladder gives −0.03 at two years on comments.
- Fig. 1A's own numbers (0.20 − 0.21 − 0.08) give −0.09 a year at the comments head, against the model's −0.0% in five years. The likely cause is the simulator's own e_k at the head; reconcile it.

**E2. Abstract (l.81).** "Three standard forms …, one of them calibrated to that persistence": two were calibrated. Fig. 4C's caption repeats this ("fixed differences are calibrated").

**E3. Significance (l.75).** It implies comments did not fall faster than forecast. Results 4 says they did (0.505 against 0.547–0.670).

**E4. Table 1.**
- The data column is labelled "(subm.)", but crossings/predicted 0.94 and crossings/slide 0.72 are pooled values. Submissions are 0.80 and 0.64; I recomputed 0.638 from the half-year cells.
- Its "constant slope" columns are the R9 worlds `sb_flat` and `firm_flat` (persistence −0.08, −0.09). The SI atlas's "constant slope" columns are `gibrat` and `atlas` (−0.02, −0.07). Use one set of names.
- At column width the table renders at about 4 pt and cannot be read.

**E5. Fig. 4C.** The dashed "net of platform loading" line is `D7.minus_aggregate_loading` (`make_v5_figures.py` l.371). That is the cohort-mean log level (`momentum_decomposition.py` l.125–132; values +0.149, +0.054, +0.006), which the SI labels "net of loading on the cohort's mean" (l.220). The SI also says it is the adjustment that is *not* robust to how the universe is chosen (l.228).

**E6.** Comments' observed share is "50%" at l.175 and "51%" at l.191; both are 0.505. Discussion l.226 says "supply little of it" and "at most three fifths" in consecutive sentences.

**E7. Discussion l.232.** "A sixth to a third of the effect … within five years" has no stated source. It seems to be the ratio of 5-year to 30-year counterfactual changes, and the "third" comes from removing *volatility*, not decline. It conflicts with l.147's rates, which imply about 40% (comments) and 75–85% (submissions) of the gap closed in five years.

**E8. SI.**
- l.118: "a member placed uniformly in *log* rank … one e-fold above its boundary" is wrong. Members are uniform in *rank*, and ∫₀¹ −ln u du = 1. Uniform in log rank would give (ln k)/2.
- l.188: "37–41% at K/64–K/32". Submissions at K/32 is 23% (0.72 against 0.93).
- l.166 main: "below the top 780 on comments it matches". At K/16 (781) the shortfall is 28%; the match starts at K/8 (1,562).

**E9. Factor residualisation, a probable artifact.**
- Loadings are unshrunk OLS fits on 51 weekly changes (`persistence_r3.py` l.88–97). R12-A3 found such loadings "almost pure noise", with corrected variance 0 in three of four strata.
- The noise term e_j·ΔF enters both changes. It adds negative covariance when the factor reverses between the two windows: submissions' path went from +0.0136 to −0.0035 a week. It adds positive covariance when the factor keeps its direction.
- That signature appears. On submissions, persistence is "removed". On comments, the residual *exceeds* the raw value: +0.191 against +0.175 (`out_persistence_r3_reddit_comments_ext.txt`).
- Redo the adjustment with shrunk or split-sample loadings and a placebo using random loadings of the same dispersion.

**E10.** SI §hetero has two paragraphs both titled "Calibration" (l.324, l.326). Loading stratum means are given as 0.41–1.09 (l.347) and as 0.83–1.03 (RESULTS R12); say which window each comes from. With 0.41 at the head and 1.09 at the bottom, a falling platform path *raises* the head's share: the factor form cannot produce the fall by construction. Say so.

**E11.** The data's lag profile at 52 weeks appears twice in the SI with different values: +0.046 and +0.108 (l.224) against +0.06 and +0.10 (l.339). Different code and universes; label them.

**E12.** "Overstated by about 30%" (l.191, two-spacing, s 0.5) and "a fifth … four fifths" (l.138, structure function, s 0.45) both describe lasting-variance inflation. Name the estimator in each place.

**E13.** The abstract's "a sixth to a quarter of each Reddit top set" excludes the top ten (27–33%). "Lost −0.06" (l.178) is a double negative.

**E14. Fig. 1D, 4A and 4D.** Labels and legends overlap the data ("r/dankmemes (#12)" runs into "r/politics"; the 4A legend covers the 2022 point). Fig. 4A's error bars run off the frame.

## 8. Writing and structure

**Sentences.** Sentence length is now fine: my count gives a median of about 19 words and about 4% of sentences over 40. The problem is now **paragraph overload and number density**:
- l.141: 290 words, 15 sentences, five topics (turnover formula, geometry, cultural evolution, β with recovery, funnel);
- l.138: 242 words, about 15 numbers;
- l.147, l.191 and l.232: over 200 words each.

**Structure.**
- Split Results 2 into "The balance, measured" (Eqs. 1–2, the instrument, 0.64), "Time scales" (durability, with the ln(K/k) caveat and the observed path), and a short "Geometry, not size, orders turnover".
- Give R12 its own subsection: "Standard heterogeneous growth does not reproduce it."
- Topic sentences are good. "That X is so, and …" (l.228) is hard to parse.

**Figures.**
- Make Table 1 full-width (`table*`), or cut it to five worlds and four rows.
- Fig. 3B: add the observed points and truncate at 10 years.
- Fig. 2A: plot rates, not years.

**SI.**
- Reorder it to follow the five Results subsections. Lock-in and the funnel sit under "Size", and "Rank change" is orphaned near the end.
- Remove internal pointers ("Section 4 of the run record", repository paths).
- Add a table mapping each main-text number to its SI section and output file.

**Rewrites.**

*Abstract, last two sentences:* "Unlike homogeneous random growth, Reddit's changes persist across periods that share no weeks, and what persists is decline, not gain. Fixed and decaying differences in growth calibrated to that persistence, and exposure to a platform-wide trend, reproduce at most two fifths of it at a quarter-year gap and none of its asymmetry, and none forecasts the fall. Random growth thus gives platform research a measured benchmark against which claims of entrenchment and of rapid deconcentration can be judged."

*l.141, split into two paragraphs:*
- "The slope that sets what newcomers bring also sets how fast members leave. The share of a top set that leaves in a year follows from how far members move against the local slope (*SI*, Eq. S2, derived after a registered linear form failed; within 0.82–1.18 of observed in 48 cells). The slope is flattest at the head (0.7–1.0, against 1.2–2.3 lower down), so the smallest top sets turn over fastest. Geometry alone does this. With the simulated dynamics fixed and only the starting ladder changed, the data's ladder turns the top 78 over faster than the top 2,500 (0.21 against 0.14 a year), and a ladder of constant slope does not (0.19 against 0.18). The same control shifts the turnover-profile exponent used in cultural evolution by 0.11, so inferences about biased copying from such profiles need a control for the ladder."
- "Size gives little protection. The one-year change needs no estimator. Its median, 90th and 95th percentiles rise only 1.2- to 1.6-fold from the top 1/64 of each universe to its lower half, across roughly 6–7 log units of size: an elasticity of 0.04–0.07, where firm-like volatility would give a 2.3- to 4-fold rise [authors to confirm from the ladders]. The model's estimator gives 0.05 on submissions and 0.06–0.13 on comments, but on simulated comment panels it returns about half the planted value. A scatter of changes against rank still looks like a widening funnel, because the lower strata hold thirty times as many communities."

*l.191:* "We built each reading into the calibrated model in its standard form. Fixed and decaying differences were calibrated by indirect inference to the covariance of year-apart changes and to the lasting variance; exposure to the platform's trend has no free parameter. No form fits. The covariances call for an annual spread of growth of 0.5 or more, the lasting variance for none, and at the compromise the fit misses by about 6 standard deviations per moment on submissions and 11 on comments. At that spread the correlation at a 13-week gap is a quarter to two fifths of the data's, and it rises with the gap where the data's falls. Fixed differences reproduce the turnover of the submission top ten and the faster slide of prior decliners, but give no asymmetry between risers and fallers. Fitted to 2019–20, no form forecasts the fall of either head (submissions 50–54% against 36%; comments 55–61% against 51%)."

## 9. The five most valuable changes before submission, using the data on hand

1. **Display and reconcile the durability prediction.**
   - Plot the observed shape path 2019–22 against the rate law and the model, at every cut-off, on both ladders.
   - Fix l.110.
   - Explain the 2019–20 comments drop. If it is the 2020 surge, state the claim outside it: in 2020–22 the comments head moved −0.17 in two years while a third of its top set turned over.
   - Reconcile the model's −0% with the measured −0.09 a year.
2. **Make the durability factor robust.**
   - Report all three lasting-variance estimators side by side.
   - Compute the factor from measured crossings, as 2 ln(K/k)(c/(s̄−e))², with and without the composition term, and report C in the worlds.
   - State the ln(K/k) dependence in the main text.
3. **Re-run R12 as joint indirect inference.**
   - Free the medium swing and use a rank-specific spread.
   - Report the J statistic, the simulation SE of the objective and recovery of ŝ.
   - Restate the conclusions at the strength §4 allows.
4. **Fix E2–E14.** In particular: redo the factor residualisation with shrunk loadings and a placebo, and correct the Fig. 4C label; correct Table 1's data column and make it legible; fix the "fourth tool".
5. **Reframe the GLLM link** as in §2: slow transition measured; the head's fall a transmitted shock; heterogeneity not asked to forecast shocks; GLLM's type switching named as untested. Change "attention" or defend it, and add Gleeson 2014, Luttmer 2011, Blumm 2012 and Ratkiewicz 2010.

**What needs the owner or new data:**
- the title;
- a recovery study of the comments exponent using the world's own passing parts and several planted β;
- whether to register R14 (for the sealed data);
- amending protocol P5 for the 2023 source switch and the blackout;
- Pushshift provenance and terms (an editorial risk at PNAS);
- an audit of the archive's capture, since persistence in item counts and in decline is also what slow capture loss would produce;
- community creation dates, for a life-cycle test of the decline state;
- the 2005–08 web quantile plot for the final version.