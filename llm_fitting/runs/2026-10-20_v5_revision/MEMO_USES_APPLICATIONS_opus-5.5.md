<!-- Provenance: memo on uses, avoidances and applications (brief BRIEF_USES_AND_APPLICATIONS.md), Claude Opus 5.5 sub-agent (Claude Code; WebSearch/WebFetch; with its own verification sub-agents), 2026-10-20, on manuscript v4. Saved verbatim by the lead agent. -->

# Uses, avoidances and applications of random-growth and rank models: memo for the v5 revision

_Claude Opus 5.5 (claude-opus-5-5), Claude Code sub-agent, 2026-10-20._
- **Brief:** `llm_fitting/runs/2026-10-20_v5_revision/BRIEF_USES_AND_APPLICATIONS.md`.
- **Manuscript read:** v4 (`paper/pnas_draft.tex`, `paper/pnas_si.tex`). Line numbers refer to it.
- **Also read:** SYNTHESIS.md, both T2 reports, `runs/2026-10-18_pnas_push/RESULTS.md`, and MODEL_STATUS §2z-cy, cz and da.
- **Rules kept:** no repository file was modified, and no raw or sealed data were opened.
- **Calculations made here:** two small ones, from saved aggregate outputs (§5). They are not in the record.

## Summary

1. **Three uses are mainstream across the six fields.**
   - Explaining a stable size distribution as growth held in check by a stabilizer.
   - Measuring latent components: lasting against passing, volatility against drift.
   - Serving as a null benchmark for a claimed departure.

   Two more are mainstream only recently, and only in economics: decomposing a change in concentration, and stating how fast transitions run. Forecasting individual units is mainstream in one place only, one-year credit-rating migration. Its known failure there, non-Markov "rating drift", has the form of our persistent decline: fallers keep falling, risers do not. In economics, policy questions are answered as comparative statics of a calibrated model.
2. **The draft already follows most of the cautions careful scholars apply:** on equifinality, on fitted exponents, and on selection on noise. Three it does not yet meet:
   - Long horizons appear as durations in the significance statement, abstract and Results: "years to decades", "within a decade", "after 30 years". That breaks the owner's horizon rule.
   - The simulation bands are spreads across runs with the parameters fixed, but they read like forecast bands.
   - l. 188 makes a structural claim about platform changes that a Lucas-type argument rules out.
3. **Five applications are clear wins, all resting on results already in hand.** They belong in the main text, mostly by re-framing what is there, as one Discussion paragraph (wording in Part 3, item 5).
   1. A null benchmark for claims of entrenchment, winner-take-all dynamics or amplification.
   2. A selection-on-noise baseline for any study that follows a top list.
   3. Turnover read against the ladder's geometry.
   4. An accounting of a change in concentration.
   5. How fast concentration can respond, given as an annual rate and as the share of the effect realized within five years.
4. **Four belong in the SI now:**
   - what data a platform study needs;
   - lock-in probabilities for members at 1–2 years, with the rank-only logistic shown winning;
   - passing against lasting position;
   - the June 2021 event study, as a worked example of placebo practice.
5. **Three are future work:**
   - Comparing platforms in dimensionless coordinates. This needs Facebook balance cells.
   - Measuring from top-K lists alone. This needs a test that truncates the panels and checks recovery.
   - Projecting the ladder's shape 1–5 years ahead. The homogeneous model's out-of-period projection failed at the submissions head: forecast 0.537, band 0.48–0.72, observed 0.356. Projections should be registered for 2023–24 from every eligible model, including R12's heterogeneous world.
6. **The saved runs bear directly on the owner's horizon rule.** In the first five years of the corrected (R11) counterfactuals:
   - The comments head does not move with both ingredients, and removing either one raises it. Its collapse without volatility appears only after a decade.
   - On submissions the five-year effects already have their long-run signs.

   So the counterfactual story should be told at five years for submissions, and given as model-implied rates everywhere else.

## 0. Scope and labels

**What is not repeated.** The T2 reports already cover the anatomy of the strongest papers, the GLLM arc, Baker & Smyth, the HIP/RIP literature, Acerbi & Bentley, the parameter-provenance table and dimensionless coordinates.

**What this memo adds.**
- A map of uses by field, saying which are mainstream.
- A checklist of cautions, with the draft's status on each.
- Applications ranked under the owner's horizon rule, with what is already on hand.

**Labels.** Each work carries one of [full text], [abstract only], [secondary] or [memory, unverified].
- Three verification sub-agents opened the sources in this session.
- "Via T2" means the work was read by the T2 reports of 2026-10-19.
- Claims are given as located paraphrases. There are no verbatim quotations from the literature.
- Proposals are marked as mine.

## Part 1. Uses

Table 1. What these models are used for. "Mainstream" means a standard use in the field's journals, not a single paper.

| use | mainstream in (exemplars) | occasional in | rare or absent | in our paper |
|---|---|---|---|---|
| U1. Explain a stable size distribution as growth plus a stabilizer | **economics:** cities (Gabaix 1999), firms (Luttmer 2007), wealth through Kesten-type returns (Benhabib, Bisin & Zhu 2011; Kesten 1973). **SPT:** the capital distribution curve (Fernholz 2002; Banner, Fernholz & Karatzas 2005). **Ecology:** species abundances under neutral drift (Hubbell 2001; Volkov et al. 2003). **Cultural evolution:** first names under random copying (Hahn & Bentley 2003). **Scientometrics:** cumulative advantage (Price 1976; Barabási & Albert 1999). **Online:** website sizes (Huberman & Adamic 1999) | — | — | Eq. 3 at stationarity, S* = σ²/2(s̄−e), measured rank by rank |
| U2. Time scales and the speed of transitions | **economics,** since GLLM 2016 (and Luttmer 2011); also the convention of stating slow convergence as an annual rate: about 2% a year, a half-life near 35 years (Barro & Sala-i-Martin 1992) | **finance** (Ichiba, Pal & Shkolnikov 2013; Baker & Smyth 2026); **ecology** (Kalyuzhny et al. 2015; Mallmin et al. 2024); **cultural evolution,** turnover rates (Bentley et al. 2007; Evans & Giometto 2011) | scientometrics; online, where only the decay of items is studied (Wu & Huberman 2007) | linearized relaxation; model counterfactuals |
| U3. Decompose a change in concentration | **economics:** top shares into within, displacement and demographic terms, on the Forbes 400 (Gomez 2023); mobility into change of shape and reranking (Van Kerm 2004). **Demography:** rate against composition (Kitagawa 1955) | **banking:** reversion against volatility (Fernholz & Koch 2016). **Ecology:** reordering against the shape of the rank–abundance curve (Avolio et al. 2019) | cultural evolution, scientometrics, online | Eq. 3 cells; the Gomez split; items × activity per item; R10 |
| U4. Null benchmark for a claimed departure | **ecology** (Gotelli & Graves 1996; Rosindell et al. 2011, 2012); **cultural evolution** (Bentley, Hahn & Shennan 2004; Acerbi & Bentley 2014); **industrial organization:** tests of Gibrat's law (Sutton 1997) and persistence of leadership (Sutton 2007); **finance:** luck against skill (Fama & French 2010) | **scientometrics** (Newman 2009); **online:** choosing a mechanism by temporal statistics (Gleeson et al. 2014; Ratkiewicz et al. 2010) | — | eight worlds, 20 runs each, same code as the data |
| U5. Measure latent components | **labour economics:** permanent against transitory earnings (MaCurdy 1982; Abowd & Card 1989; Gottschalk & Moffitt 1994; Meghir & Pistaferri 2004; Blundell, Pistaferri & Preston 2008). **Ecology:** process against observation noise (Dennis et al. 2006; Knape 2008) | **SPT estimators** (Fernholz 2017); **scientometrics,** a stable ability parameter Q (Sinatra et al. 2016) | online popularity | lasting against passing variance; structure-function β; gapped persistence |
| U6. Separate luck from lasting position | **finance** (Fama & French 2010; survivorship, Brown et al. 1992) | **management** (Henderson, Raynor & Ahmed 2012; Denrell & Liu 2012); **scientometrics** (Sinatra et al. 2016) | online | permanent-rank cohorts and the selection baseline; no share-of-luck number yet |
| U7. Forecast individual units | **credit risk:** one-year rating-migration matrices (Jarrow, Lando & Turnbull 1997), with documented downgrade momentum that breaks the Markov assumption (Lando & Skødeberg 2002). **Demography:** probabilistic projections of aggregates with bands (Lee & Carter 1992; Raftery et al. 2012) | **online:** per-item forecasts from early log popularity (Szabo & Huberman 2010). **Scientometrics,** contested: Wang, Song & Barabási 2013 against Wang, Mei & Hicks 2014 | **ecology:** near-term forecasting only, and still emerging (Dietze et al. 2018) | the movement gate: no better than simpler rules; lock-in: loses to a logistic on rank; A6 |
| U8. Policy or design scenarios | **macro and public economics,** as comparative statics of a calibrated model: the tail index and taxes (Benhabib, Bisin & Zhu 2011); policy-interpretable parameters (Jones & Kim 2018) | **finance, as engineering:** diversity-weighted indexing (Fernholz, Garvy & Hannon 1998) | ecology; online | removal counterfactuals, which are experiments on the model |

**By field.**
- **Economics** uses random growth to explain tails (U1), bound the speed of transitions (U2), decompose top shares (U3) and separate permanent from transitory income (U5). Policy enters as comparative statics. Forecasting individuals is not a goal.
- **Finance** uses:
  - stochastic portfolio theory for the capital distribution curve and for building portfolios;
  - rating-migration chains for one-year credit probabilities;
  - bootstrap nulls to separate luck from skill.
- **Ecology** uses neutral models mainly as nulls, after a decade of finding that fits to static patterns do not discriminate between mechanisms. State-space models separate process noise from observation noise. Forecasting is recent and explicitly near-term.
- **Cultural evolution** uses neutral copying as the null, and turnover profiles to infer biases in transmission.
- **Scientometrics** uses cumulative advantage to explain distributions. Forecasts of individual citations are contested. Models that separate luck from ability are the newer use.
- **Online popularity** uses multiplicative growth and preferential attachment to explain distributions, and early popularity to forecast items within days. Temporal statistics, not distributions, choose among mechanisms.

**What this adds to T2.**
- **The credit-migration parallel.** It is the one mainstream use of a rank-like Markov model for one-year individual forecasts. Its failure mode, downgrade momentum, is our persistent decline.
- **Sutton's benchmark.** Sutton (2007) tests how long leaders keep their lead against a zero-drift random walk in the gap between leader and rival, with variance scaled by share, before reading anything into it. He finds leadership lasts somewhat longer than the benchmark in some industries, and explicitly does not call this market power. That is application 1 in industrial-organization form.
- **The rate convention.** Economists state slow change as an annual rate rather than as a horizon. That is the owner's rule.

## Part 2. Avoidances

Table 2. Each caution, why careful scholars avoid the use, the practice that avoids it here, and the draft's status (v4 line numbers).

| caution | why (citations) | practice for this paper | draft status |
|---|---|---|---|
| A1. Point forecasts of individual trajectories | Social influence makes success unpredictable as well as unequal (Salganik, Dodds & Watts 2006). Cascade size has a predictability ceiling even with perfect knowledge: R² about 0.93 in simulation, 0.48 for the best empirical model (Martin et al. 2016). In a mass collaboration, individual life outcomes were predicted only somewhat better than by a four-variable benchmark (Salganik et al. 2020). A citation model forecast worse than short-term citations (Wang, Mei & Hicks 2014). Predictions should be evaluated in a standard way, and the limits of predictability stated (Hofman, Sharma & Watts 2017). | Forecast only quantities for whole sets, or probabilities per member. Always show three baselines (the logistic on rank, constant turnover, and carrying the last value forward) and the calibration. Never forecast the path of a named community. | **Follows** (l. 160; the SI gate table; A6). **Gap:** when lock-in is reported, show that the logistic wins (§2z-da). |
| A2. Long-horizon extrapolation and parameter instability | The error in a projected growth rate is roughly constant per year, so the error in projected size compounds with the horizon (Keyfitz 1981). Forecast skill usually falls with lead time (Petchey et al. 2015). Forecasting relations are widely unstable (Stock & Watson 1996). Stationarity cannot be assumed when conditions change (Milly et al. 2008). A century of equity data barely spans two half-lives of the slow mode (Baker & Smyth 2026, via T2). | State anything beyond five years as a model-implied rate per year, and as the share of a gap closed in 1, 2 and 5 years. Show how results change with the window the drift is taken from (2019–20 slides against 2021–22). Keep durations measured in decades out of the significance statement and the abstract. | **Does not yet follow.** l. 74 "years to decades"; l. 79 "within a decade … over decades"; l. 147 "about nine years … two decades"; l. 157 "after 30 years". l. 190 rightly says the balance works on a longer time scale than the record covers. |
| A3. Simulation bands read as forecast bands | Lee–Carter intervals rest mainly on the random walk of the time index (Lee 2000). Critics judged them too narrow (Alho 1992, as cited by Lee 2000; Li, Hardy & Tan 2009), and backtests found coverage mixed (Lee & Miller 2001). Near-term forecasts should carry parameter uncertainty and be scored against null models (Dietze et al. 2018). Probabilistic projections earn trust by out-of-sample validation (Raftery et al. 2012). | Label every band "across N runs, parameters fixed". For any statement about the future, resample the inputs (σ₀, β, the slide profile, the exits), or call it a scenario. | **Partly.** Captions say "20 runs, 90% band" without saying the parameters were held fixed. In A6 the band (0.48–0.72) excluded the observed head share (0.356). |
| A4. Mechanism inferred from a stationary distribution (equifinality) | Most reported power laws lack both statistical and mechanistic support (Stumpf & Porter 2012). Dozens of models explain the hollow curve of species abundances, and few are rejected because few predict anything else (McGill et al. 2007). Different community processes give similar static patterns (Chave, Muller-Landau & Levin 2002). Fits to aggregated metrics do not support neutrality (Clark 2012). Many model structures can be equally acceptable (Beven 2006). The field has concentrated on modelling, and validation is the harder step still to be taken (Mitzenmacher 2005). In rank-based models the stationary shape depends only on rank-specific volatility and reversion (Fernholz 2017, via T2). | Claim to measure the balance, not to identify a mechanism acting on each unit. Discriminate with temporal statistics (gapped persistence, turnover by rank) and with nulls run through the same code, as Gleeson et al. (2014) did online. State in advance that heterogeneous growth with the same σ_k and s̄_k gives the same stationary shape. | **Follows:** l. 139 "an instrument rather than a test of mechanism"; l. 147 "describes the ladder's state, not a mechanism"; SI "What is automatic". **Gaps:** the heading "Where the hierarchy comes from" and the significance statement's "From random growth held in check" read as claims of mechanism; there is no heterogeneous world yet (R12). |
| A5. A fitted power-law or Zipf exponent read as evidence of a process | Lognormals fit as well as power laws in most datasets (Clauset, Shalizi & Newman 2009, Sec. 6; in 88% of networks, Broido & Clauset 2019). A heavily truncated sample from the top of a lognormal mimics a power law (Perline 2005). The largest cities look Pareto, while the whole distribution is lognormal and consistent with Gibrat (Eeckhout 2004). Both points bear directly on top-K lists. | Report the local slope S_k by rank, with intervals, never a single Zipf exponent. Calibrate any exponent estimator on planted worlds before comparing across fields. | **Follows:** local slopes are used, and the raw β is calibrated (a planted 0 reads as 0.06). Keep "depends on the estimator" in the same sentence as the comparison with firms (l. 116). |
| A6. Causal claims about interventions (Lucas critique) | Parameters that are not invariant to the policy regime mislead about the effects of a new policy (Lucas 1976, p. 41). Amplification was measured against a randomized control group kept on a reverse-chronological feed (Huszár et al. 2022). At least three quarters of click-throughs on recommendations would have occurred without them (Sharma, Hofman & Watts 2015). | Call the removals "experiments on the model, with the other measured ingredients held fixed". Make any statement about platform design conditional, and list what a change can act on: volatility, the slide, entry and exit, dependence between communities, and the instrument. Estimate the effect of an intervention only from event designs with placebo years. | **Partly.** The removals are labelled as the model's counterfactuals. But l. 188 is unconditional: "A change in how a platform distributes attention acts on the balance through the relative decline …". And "Through that channel" at l. 74 is causal (synthesis C1). |
| A7. Calibration treated as validation | Models of open systems cannot be verified; agreement with data confirms them only partially (Oreskes, Shrader-Frechette & Belitz 1994). Calibration is estimation that reports no sampling error (Hansen & Heckman 1996). Models should be tested against several patterns at several scales (Grimm et al. 2005). | Add one SI table that separates calibration targets, internal checks and out-of-sample tests, and lists the failures. Never cite a world calibrated to hold its ladder as evidence about that ladder. | **Partly.** l. 155 asserts the parameters were "none fitted to the statistics we test it on". Fig. 4B is labelled a check, and the gate and A6 are reported. The provenance table is still missing (synthesis C6; T2 item 6). |
| A8. Selection, survivorship and measurement error ignored | Survivorship can create apparent persistence of performance (Brown et al. 1992). Studying successes alone links risky practices to success (Denrell 2003). Regression toward the mean does not mean a distribution is narrowing (Quah 1993; Friedman 1992). Not detecting something is not evidence of its absence (MacKenzie et al. 2002). Averaging a random walk gives a correlation near 0.25 between successive differences (Working 1960). | Fix cohorts by permanent rank on disjoint blocks. Track communities below the cut-off (to 4K). Report the selection-on-noise baseline beside every decline of a selected set. Model detection, as for Instagram. Audit the instrument. | **Follows strongly** (l. 139, 196; SI §2; the R7 baseline; the averaging null). **Gaps:** capture changes and deletions are not audited (l. 190); bans are counted as exits. |
| A9. Rank stability or turnover read as entrenchment or competition | Sutton (2007) tests leadership persistence against a zero-drift random walk and finds only a modest excess, which he does not attribute to market power. Which part of a ranking is stable depends on the flux of new elements (Iñiguez et al. 2022). | Read rank-one tenure against the gap between neighbouring ranks (about S/R), and set turnover against D⁺/S, before calling either one entrenchment. | **Partly.** The geometry is in the draft (l. 114, 118), but "Rank is a poor measure of entrenchment" (l. 188) is categorical. |
| A10. Rejecting one null as if rejecting the family | Turnover under neutral models depends on the update rule and the regime (Evans & Giometto 2011). Neutral theory serves as a null or an approximation, not a literal description (Rosindell et al. 2012). | Name the regime that was rejected: copying with innovation rate 0.003 and 1.5 million draws a week. | **Partly.** The SI names it; the main text says only "neutral copying" (l. 95, 139). |
| A11. Forking paths in post hoc analysis | Choices that depend on the data multiply comparisons even without deliberate fishing (Gelman & Loken 2014). | Write expectations before each run, label post hoc work, list failures, and confirm on sealed data. | **Follows** (l. 206; SI §registration). |

## Part 3. Applications for digital platform studies

**What platform researchers usually have.** Weekly activity for a ranked top-K list, often without views, on platforms that can end researchers' access at any time (Freelon 2018).
- Meta discontinued CrowdTangle on 14 August 2024 (Meta Transparency Center).
- Reddit announced charges for its API on 18 April 2023 (TechCrunch), and cut off Pushshift in early May 2023 (Coalition for Independent Technology Research letter).
- Nearly 7,000 subreddits went dark on 12–14 June 2023.

So the most valuable applications are those that need only such lists, or that tell a researcher how deep a list and how long a panel they need.

**What our framework measures:**
- the local slope S_k and lasting volatility σ_k²;
- the crossing gain c_k, the slide s̄_k, and the effect of entry and exit e_k;
- turnover from D⁺/S_k (Eq. 1);
- gapped persistence;
- the size exponent of lasting volatility.

Table 3. Applications, ranked.

| rank | application | placement | on hand now? | horizon |
|---|---|---|---|---|
| 1 | Null benchmark for claims of entrenchment, winner-take-all dynamics and amplification | **main text** | yes | 1–4 years |
| 2 | Selection-on-noise baseline for studies that follow a top list | **main text** (one sentence) + SI | yes | 1 year |
| 3 | Turnover read against geometry | **main text** (already there) + SI table | yes, plus one small calculation | 0.5–1 year |
| 4 | Accounting for a change in concentration | **main text** (already there) | yes, plus one small calculation | 1–3 years |
| 5 | How fast concentration can respond, stated as rates | **main text,** Discussion (a restatement) | yes, plus arithmetic | a rate per year; the share realized within 5 years |
| 6 | What data a platform study needs | SI | yes | — |
| 7 | Lock-in probabilities for members at 1–2 years | SI | yes (record §2z-cy, cz, da) | 0.5–2 years |
| 8 | Passing against lasting position | SI (variance share) / future (filtered shares) | partly | up to 1 year |
| 9 | Detecting platform interventions | SI (worked example) / future (registered test) | the example only | weeks to 1 year |
| 10 | Comparing platforms in dimensionless coordinates | SI (partial) / future (figure) | partly | — |
| 11 | Measurement from top-K lists alone | future | no | — |
| 12 | Projecting the ladder's shape 1–5 years ahead | future (registered) | only a failed test | 1–5 years |

### 1. A null benchmark for claims of entrenchment, winner-take-all dynamics and amplification (main text)

**Question.** Is a pattern offered as evidence of entrenchment more than random growth from the platform's own ladder would produce? Examples:
- a stable leader;
- a top that keeps its share;
- rich-get-richer growth;
- top items staying on a chart longer. Lech, Lehmann & Juul (2024) report that top Hot 100 songs now stay about three times longer than in the 1960s.

**Analysis.** Compute the claimed statistic with the same code in the data and in calibrated worlds started from the data's ladder (size-blind, firm-like, and the model; 20 runs each). Report where the data fall in each world's 5–95% band.

**What is on hand:**
- The submissions leader changed in 7% of weeks, against 9–12% in random growth from the same ladder (SI null-world table).
- The top ten turn over at 0.33 a year, at or above every calibrated world's 95th percentile (R9). The very top is less protected than random growth makes it.
- Decliners persist and risers do not (D-2), the reverse of rich-get-richer.
- The members of the submissions head lost 0.24–0.26 of their share a year in 2020–22 (Gomez's within term), against 0.05 from selection alone (R7).

**Data and feasibility.** A weekly top-K panel, tracked below the cut-off. Feasible now.

**Precedents.** Sutton 2007; Fama & French 2010; Henderson, Raynor & Ahmed 2012; Gotelli & Graves 1996.

**Risks:**
- The null takes the ladder as given, so it tests the dynamics, not the ladder.
- Rejecting the null does not identify the alternative.
- Amplification in Huszár's sense, exposure under the algorithm against a control, cannot be measured from activity data.
- The worlds share the same estimators and the same passing components.

**Recommendation.** One Discussion paragraph presenting the benchmark as a method, with the three results. An SI table with one row per claim: the statistic, the world's band, and the data.

### 2. A selection-on-noise baseline for top-list studies (main text, one sentence; SI)

**Question.** How much ground should the members of a top set chosen on this year's activity lose next year if nothing real happens?

**What is on hand:**
- In size-blind growth from the data's ladder, the head's within term is −0.053 a year (R7).
- In the calibrated world without drift, the slide is a third of the data's: 0.0019 against 0.0056 a week on submissions, 0.0011 against 0.0036 on comments (R10; the ratio is my arithmetic).
- The slide is positive in every world, including those without drift (NW-3).

**Precedents.** Quah 1993 and Friedman 1992 on the regression fallacy; Brown et al. 1992 on survivorship; Denrell 2003.

**Risks.** The baseline depends on how the passing noise is calibrated, and the model's changes are thin-tailed (excess kurtosis 2.9 against 22.1 in the data).

**Recommendation (my wording).** "The members of a top set chosen on one year's activity lose about 5% of their share the next year from selection alone, and lose ground against the re-selected universe without any drift. A decline of selected leaders is evidence only beyond that."

### 3. Turnover read against geometry (main text, already there; one small calculation)

**Question.** Does faster turnover at the top of one platform, or in one period, mean weaker incumbency, or only a flatter head?

**What is on hand.**
- Eq. 1 holds in all 48 cells (ratios 0.82–1.18).
- With the dynamics held fixed, the data's ladder turns the head over faster than a ladder of constant slope does, by 0.06 (0.03–0.09) (R9).

**Small calculation, done here** from the saved `turnover_kinematics.json`; it is not in the record (§5).
- D⁺/S, which measures mobility net of the ladder's geometry, is 0.27–0.34 a year at the Reddit head and 0.47–0.51 per half-year at the Facebook head.
- Facebook's head is flatter: S is 0.72–0.73, against 0.87–0.95 on Reddit.
- Facebook pages move about as far in half a year (E[D⁺] 0.34–0.37) as Reddit communities move in a year (0.25–0.30).
- So both terms, the size of the moves and the flatness of the head, contribute to Facebook's faster turnover.

**Precedents.** Kitagawa 1955 (standardization); Van Kerm 2004; Acerbi & Bentley 2014; Houssard et al. 2023 (a stable top on Twitch; via T1).

**Risks:**
- D⁺ includes passing swings.
- The ladders differ in metric and in block length.
- Geometry explains the ordering of turnover across top sets, not the level of turnover in the top ten.

**Recommendation.** One main-text sentence: turnover rates compare across platforms only after dividing by the ladder's slope. Add the Facebook–Reddit contrast once it is recorded, and an SI table.

### 4. An accounting for a change in concentration (main text, already there; present it as a method)

**Question.** When a platform's head loses share, how much of the loss comes from each source?
- the leaders' own decline;
- displacement by risers;
- entry and exit;
- posting against engagement per post;
- a few persistent decliners.

**What is on hand:**
- the Gomez split (A8, R3-E);
- items against engagement per item (R8): for the 2019 submissions head, the log change of share splits as −1.12 (items) + 0.43 (per item);
- the rate-law cells (c, s̄ and e);
- R10: prior decliners carry 40–60% of the slide's excess over random growth.

**Small calculation.** Two pieces:
- An attribution in the style of Fernholz & Koch: how much of the variation in shape change across cells comes from c, from s̄ and from e. It uses the saved Reddit law cells (`runs/2026-10-14_balance_law/cells`).
- One "budget" table for the submissions head in 2019–22, assembled from the terms above.

**Precedents.**
- Fernholz & Koch 2016. The finding that volatility, not reversion, raised commercial-bank concentration is in the working paper only; the 2017 AER Papers & Proceedings version covers bank-holding companies alone.
- Gomez 2023.
- Kitagawa 1955.

**Risks:**
- Mixing the arithmetic-share accounting with the mean-log accounting.
- An accounting is not a causal analysis.
- The link between persistence and the head's decliners has not been shown (C1).

**Recommendation.** Present this as an accounting that any top-K panel supports, and keep it descriptive.

### 5. How fast concentration can respond (main text Discussion; a restatement in rates)

**Question.** If a platform change alters the relative decline at the head, how much of its eventual effect on concentration appears within 1, 2 and 5 years?

**Analysis.** In Eq. 3 the rate of change of the shape responds at once, by Δs̄. The shape then approaches its new balance at a rate λ = 1/τ.

**What is on hand.** Three time scales τ:
- the model: two to three decades at the head (l. 147);
- the GLLM anchor, 8S²/σ²: 18–24 years on comments and 13–31 years on submissions (synthesis C2);
- linearized relaxation (§2z-cp): about 9 years at the comments head, 3–4 years at the submissions head.

**Arithmetic (mine), using 1 − e^(−t/τ).**
- Near balance, λ is about 0.03–0.08 a year. So 15–32% of an effect appears within five years, and 6–14% within two.
- At the linearized rates, the comments head closes about a tenth of its gap a year (43% in five years) and the submissions head a quarter to a third (71–81%).

**Five-year counterfactuals,** read from the saved R11 runs (yearly means of 20 runs; §5). Change in the head's shape, in log units a year:

| ladder | both ingredients | slide alone | volatility alone |
|---|---|---|---|
| submissions | −0.11 | −0.24 | +0.08 |
| comments | 0.00 | +0.05 | +0.05 |

On comments, removing either ingredient raises the head within five years.

**Precedents.** Barro & Sala-i-Martin 1992; GLLM 2016; Baker & Smyth 2026.

**Risks:**
- The linearization holds the slides fixed.
- Heterogeneous growth speeds transitions (GLLM), so the homogeneous λ is the slow benchmark; the submissions head did move faster.
- Four years of data cannot test rates that play out over decades.

**Recommendation.** Replace the durations at l. 74, 79, 147 and 157 with rates and five-year shares. Lead the counterfactual paragraph with the five-year numbers, and give the 30-year runs as model-implied rates.

**Proposed Discussion paragraph (my wording):**

> The framework gives platform research three tools that do not depend on the model's forecasts. The first is a benchmark: a statistic offered as evidence of entrenchment can be computed, with the same code, in random growth started from the platform's own ladder. On Reddit, leaders hold rank one about as long as such growth predicts, the top ten turn over faster than it predicts, and declines persist where gains do not. The second is a correction: the members of a top set chosen on one year's activity lose about 5% of their share the next year from selection alone, and turnover rates compare across platforms only after dividing by the ladder's slope. The third is an accounting: a change in concentration splits into displacement, the members' own decline, entry and exit, and posting against engagement per post. Under homogeneous random growth at the measured rates, a change in the decline at the head would show roughly a sixth to a third of its effect on the head's shape within five years. Where the submissions head moved faster, the homogeneous model did not forecast it.

### 6. What data a platform study needs (SI)

**What is on hand:**
- The structure-function estimator of β recovers planted values with a standard deviation of 0.03 at 160 weeks; at 86 weeks its null is biased by +0.045.
- At Facebook's 26-week horizon, Reddit's persistence is weak too (−0.04 to +0.08). Persistence therefore needs annual horizons.
- Turnover in the top ten rests on about 30 name-years per year pair, too few to separate the worlds.
- Crossings need levels below the cut-off; we track to 4K.

**Precedents.** Gerrodette 1987 (statistical power to detect a trend); Freelon 2018.

**Recommendation.** An SI paragraph, and one clause in Methods stating the tracking requirement.

### 7. Lock-in probabilities for members at 1–2 years (SI)

**What is on hand** (§2z-cy, cz, da):
- Forecasts from movement beat constant turnover by 13–21% in Brier score on four ladders.
- But the logistic on log rank wins on every ladder, by 0.0006–0.0048.
- Staying is under-forecast: by 2–5 points at the first horizon, and 5–17 points at 1.5–2.5 years. A post hoc forecast using the empirical shape of training changes halves the gap.
- The forecasts' strata are by current rank, a departure from the permanent-rank rule that is declared in the design.

**Precedent.** Credit-rating migrations (Jarrow, Lando & Turnbull 1997), where downgrade momentum breaks the Markov model (Lando & Skødeberg 2002), as persistent decline breaks ours.

**Recommendation.** SI, with the logistic shown. At most one main-text clause: rank alone predicts who stays as well as movement does. The confirmatory test is P4 on the sealed data.

### 8. Passing against lasting position (SI, or future)

**Question.** What share of this year's top set is there because of a passing swing?

**Small calculation.** The share of the variance of one-year changes that is lasting, by stratum, from the law cells' σ_k² and the block standard deviations. Whether the saved outputs support this still needs confirming.

**New runs.** The filtered lasting level of each member, which would give the share of the current top k whose lasting level is above the boundary.

**Precedents.** Fama & French 2010; Henderson et al. 2012; Sinatra et al. 2016.

**Risks.** Reversion and observation noise are weakly identified together (Knape 2008, via T2), and the tails are heavy.

### 9. Detecting platform interventions (SI worked example; future registered test)

**What is on hand** (§2z-da §7):
- The fall of the submissions head in June 2021 is not distinctive against placebo years: X1 failed, and 2022 was lower still (−0.075).
- What is distinctive is the gain of the next tier: +0.056, and +0.044 after adjusting for trend.

**Arithmetic (mine).** The head's normal slide accumulates about 0.066 log units in eight weeks, about the size of one placebo year. So at weekly resolution with eight-week windows on each side, only changes several times the normal slide can be detected.

**Precedents.** Abadie, Diamond & Hainmueller 2010 (placebo inference); MacKinlay 1997; Bai & Perron 1998; Chandrasekharan et al. 2017.

**Risks:**
- Event dates chosen after seeing the data.
- The archive's capture change on 1 July 2021 coincides with the event.
- Few events.

**Recommendation.** SI, as a worked example of placebo practice. Future work: the registered blackout test (P3), after the pending protocol amendment.

### 10. Comparing platforms in dimensionless coordinates (SI, partial; future)

**What it would use.** S/S*, D⁺/S, λ times the membership time, and the persistence share.

**What is on hand.** The Reddit law cells and the Facebook turnover cells (item 3). Facebook balance cells need a new run on development data.

**Precedents.** Iñiguez et al. 2022; Mallmin et al. 2026.

**Risks.** Two platforms, 86 weeks of Facebook data and different metrics. No claim of universality should be made.

### 11. Measurement from top-K lists alone (future)

**Analysis.**
- S and turnover can be observed inside a list.
- Inverting Eq. 1 then gives the boundary churn E[D⁺] from those two.
- Lasting σ needs, in addition, an assumption about passing swings.
- Test the method by truncating the development panels at K/4 and re-estimating.

**Precedents:**
- top-income shares estimated from tax tabulations (Piketty & Saez 2003);
- the Forbes 400 (Gomez 2023);
- detection models in ecology (MacKenzie et al. 2002);
- open markets in stochastic portfolio theory (Karatzas & Kim 2021, via T2);
- this paper's own Instagram apparatus.

**Risks:**
- Censoring biases σ.
- The inversion depends on the assumed shape of the distribution of moves.
- Perline's and Eeckhout's truncation cautions apply.

**Recommendation.** High value, but future work. Meanwhile, state the current tracking requirement in Methods.

### 12. Projecting the ladder's shape 1–5 years ahead, with bands (future)

**What is on hand.**
- The out-of-period test (A6 and R3-C) failed at the submissions head.
- The rate-law forecast from the previous year's slides beats "no change" on submissions and fails on comments.

**Precedents.** Raftery et al. 2012 (validated out of sample); Lee & Carter 1992, with Alho's 1992 comment; Dietze et al. 2018.

**Recommendation.** Nothing in v5 beyond the SI's failed test. Register projections for 2023–24 from every eligible model, including R12, with parameter uncertainty.

**Not recommended now:**
- first passage of newcomers to the top (too few events);
- mobility indices;
- synthetic ladders for sharing data under CrowdTangle's terms (repository only).

## Part 4. Results already on hand for the main-text applications

| application | answered by (location) | small calculation from saved outputs | new runs |
|---|---|---|---|
| 1. Null benchmark | SI null-world table; R7; R9; D-2; l. 114, 118, 168 | an SI table of claim, world band and data (assembly only) | none |
| 2. Selection baseline | R7 Gomez baseline (−0.053); R10 driftless slides; NW-3 | the baseline at K/16 and K/4, if `null_worlds_r7_summary.json` holds it | none |
| 3. Geometry | A1 (Eq. 1); R9 | D⁺/S by ladder and cut-off (done here; needs recording) | none |
| 4. Accounting | A8, R3-E, R8, R10; the rate-law cells | the Fernholz–Koch-style attribution from the law cells; the head's budget table | Facebook balance cells, if wanted |
| 5. Response time | §2z-cp; R6, R11; synthesis C2 (GLLM anchor) | the rates and five-year shares (arithmetic done here); five-year counterfactuals from the R11 outputs (read here; needs recording, with yearly bands if the runs saved them) | sensitivity to the drift window: R11 with drift taken from the 2019–20 slides against the 2021–22 slides (20 runs, development data) |

R11 itself must also enter the main text (synthesis plan, item 1). It changes "flatter at every rank, by 8–46%" and the comments collapse, which becomes −31%.

## 5. Checked against proposed, and the calculations made here

**Checked.**
- The draft's status on each caution was read from v4 at the line numbers given.
- Numbers from the record were quoted from RESULTS.md, the SI and MODEL_STATUS §2z-cy, cz and da.
- Three sub-agents checked every reference's details and the claim made of it, in this session. Their corrections are applied:
  - Sutton's actual null and conclusion.
  - The published scope of Fernholz & Koch.
  - The Lee–Carter critique, now attributed to Alho and to Li et al. rather than to Lee & Carter.
  - Keyfitz's result on how error grows.
  - Jones & Kim as comparative statics, not policy counterfactuals.
  - Newman's title.
  - Szabo & Huberman as per-item forecasts.
  - Rosindell et al. 2012 and Clark 2012 for the null-model framing.
  - Hofman et al.'s actual argument.
  - Gabaix 2009 cited as a survey, not as a warning that many mechanisms produce power laws.

**Computed here, not in the record.** All three need recording before they enter the manuscript.
1. D⁺/S and S by ladder and cut-off, as means over year pairs, from `runs/2026-10-18_pnas_push/turnover_kinematics.json`. I read the field `e` as E[D⁺]; confirm this against `turnover_kinematics.py`.
2. Five-year changes in the head's shape, from `churn_counterfactual__*__long30__noentry.json` (`shape_by_year`, read as means over runs).
3. Arithmetic on recorded numbers: 1 − e^(−t/τ), the slide ratios, and the eight-week accumulation of the slide.

**Proposals.** The ranking, the placements and the proposed wording are mine.

## References

Access labels: **[F]** full text; **[A]** abstract only; **[S]** secondary; **[M]** memory, unverified. "Via T2" means the work was read by the T2 reports of 2026-10-19.

**Economics and finance**
- Abowd J, Card D (1989). *Econometrica* 57:411. https://doi.org/10.2307/1912561 [A]
- Alho JM (1992). Comment. *JASA* 87:673. https://doi.org/10.1080/01621459.1992.10475267 [S]
- Bai J, Perron P (1998). *Econometrica* 66:47. https://doi.org/10.2307/2998540 [A]
- Baker G, Smyth C (2026). arXiv:2608.27156. https://arxiv.org/abs/2608.27156 [F, via T2]
- Banner A, Fernholz R, Karatzas I (2005). *Annals of Applied Probability* 15:2296. https://doi.org/10.1214/105051605000000449 [A]
- Barro R, Sala-i-Martin X (1992). *JPE* 100:223. https://doi.org/10.1086/261816 [A; the NBER working paper F]
- Benhabib J, Bisin A, Zhu S (2011). *Econometrica* 79:123. https://doi.org/10.3982/ECTA8416 [F]
- Blundell R, Pistaferri L, Preston I (2008). *AER* 98:1887. https://doi.org/10.1257/aer.98.5.1887 [A]
- Brown SJ, Goetzmann W, Ibbotson RG, Ross SA (1992). *RFS* 5:553. https://doi.org/10.1093/rfs/5.4.553 [A]
- Fama EF, French KR (2010). *Journal of Finance* 65:1915. https://doi.org/10.1111/j.1540-6261.2010.01598.x [A]
- Fernholz ER (2002). *Stochastic Portfolio Theory*. Springer. https://doi.org/10.1007/978-1-4757-3699-1 [S]
- Fernholz R, Garvy R, Hannon J (1998). *Journal of Portfolio Management* 24(2):74. https://doi.org/10.3905/jpm.24.2.74 [S]
- Fernholz RT (2017). *Journal of Applied Econometrics* 32:1244. https://arxiv.org/abs/1602.00159 [A, via T2]
- Fernholz RT, Koch C (2016). Dallas Fed WP 1604. https://www.dallasfed.org/research/papers/2016/wp1604 [F]
- Fernholz RT, Koch C (2017). *AER Papers & Proceedings* 107:603. https://doi.org/10.1257/aer.p20171007 [F]
- Friedman M (1992). *JEL* 30:2129. https://ideas.repec.org/a/aea/jeclit/v30y1992i4p2129-32.html [S]
- Gabaix X (1999). *QJE* 114:739. https://doi.org/10.1162/003355399556133 [F, via T2]
- Gabaix X (2009). *Annual Review of Economics* 1:255. https://doi.org/10.1146/annurev.economics.050708.142940 [F]
- Gabaix X, Lasry J-M, Lions P-L, Moll B (2016). *Econometrica* 84:2071. https://doi.org/10.3982/ECTA13569 [F, via T2]
- Gomez M (2023). *Econometrica* 91:979. https://doi.org/10.3982/ECTA16755 [A here; F via T2]
- Gottschalk P, Moffitt R (1994). *Brookings Papers on Economic Activity* 1994(2):217. https://doi.org/10.2307/2534657 [F]
- Hansen LP, Heckman JJ (1996). *JEP* 10(1):87. https://doi.org/10.1257/jep.10.1.87 [A]
- Henderson AD, Raynor ME, Ahmed M (2012). *Strategic Management Journal* 33:387. https://doi.org/10.1002/smj.1943 [A]
- Ichiba T, Pal S, Shkolnikov M (2013). *Probability Theory and Related Fields* 156:415. https://doi.org/10.1007/s00440-012-0432-5 [A, via T2]
- Jarrow RA, Lando D, Turnbull SM (1997). *RFS* 10:481. https://doi.org/10.1093/rfs/10.2.481 [A]
- Jones CI, Kim J (2018). *JPE* 126:1785. https://doi.org/10.1086/699190 [F]
- Karatzas I, Kim D (2021). *Mathematical Finance* 31:1111. https://doi.org/10.1111/mafi.12294 [A, via T2]
- Kesten H (1973). *Acta Mathematica* 131:207. https://doi.org/10.1007/BF02392040 [F]
- Kitagawa EM (1955). *JASA* 50:1168. https://doi.org/10.1080/01621459.1955.10501299 [S]
- Lando D, Skødeberg TM (2002). *Journal of Banking & Finance* 26:423. https://doi.org/10.1016/S0378-4266(01)00228-X [S]
- Lee RD (2000). *North American Actuarial Journal* 4(1):80 [S; DOI not checked]
- Lee RD, Carter LR (1992). *JASA* 87:659. https://doi.org/10.1080/01621459.1992.10475265 [S]
- Lee RD, Miller T (2001). *Demography* 38:537. https://doi.org/10.1353/dem.2001.0036 [S]
- Li JS-H, Hardy MR, Tan KS (2009). *ASTIN Bulletin* 39:137 [A; DOI not checked]
- Lucas RE (1976). *Carnegie-Rochester Conference Series on Public Policy* 1:19. https://doi.org/10.1016/S0167-2231(76)80003-6 [S]
- Luttmer EGJ (2007). *QJE* 122:1103. https://doi.org/10.1162/qjec.122.3.1103 [A]
- Luttmer EGJ (2011). *Review of Economic Studies* 78:1042. https://doi.org/10.1093/restud/rdq028 [A, via T2]
- MaCurdy T (1982). *Journal of Econometrics* 18:83. https://doi.org/10.1016/0304-4076(82)90096-3 [S]
- MacKinlay AC (1997). *JEL* 35:13. https://www.jstor.org/stable/2729691 [A]
- Meghir C, Pistaferri L (2004). *Econometrica* 72:1. https://doi.org/10.1111/j.1468-0262.2004.00476.x [A]
- Piketty T, Saez E (2003). *QJE* 118:1. https://ideas.repec.org/a/oup/qjecon/v118y2003i1p1-41..html [A]
- Quah D (1993). *Scandinavian Journal of Economics* 95:427. https://doi.org/10.2307/3440905 [A]
- Shorrocks AF (1978). *Econometrica* 46:1013. https://doi.org/10.2307/1911433 [A]
- Sornette D, Cont R (1997). *J. Phys. I France* 7:431. https://doi.org/10.1051/jp1:1997169 [A]
- Stanley MHR et al. (1996). *Nature* 379:804. https://doi.org/10.1038/379804a0 [A]. The exponent 0.20 ± 0.03 for sales is from Amaral et al. (1997), already cited in the draft.
- Stock JH, Watson MW (1996). *JBES* 14:11. https://doi.org/10.1080/07350015.1996.10524626 [A]
- Sutton J (1997). *JEL* 35:40. https://ideas.repec.org/a/aea/jeclit/v35y1997i1p40-59.html [A]
- Sutton J (2007). *AER* 97:222. https://doi.org/10.1257/aer.97.1.222 [A; the 2004 working paper F]
- Van Kerm P (2004). *Economica* 71:223. https://doi.org/10.1111/j.0013-0427.2004.00367.x [A]

**Ecology, cultural evolution, scientometrics and forecasting**
- Acerbi A, Bentley RA (2014). *Evolution and Human Behavior* 35:228. https://doi.org/10.1016/j.evolhumbehav.2014.02.003 [F, via T2]
- Avolio ML et al. (2019). *Ecosphere* 10:e02881 [A; DOI not checked]
- Barabási A-L, Albert R (1999). *Science* 286:509. https://doi.org/10.1126/science.286.5439.509 [metadata only]
- Bentley RA, Hahn MW, Shennan SJ (2004). *Proc. R. Soc. B* 271:1443. https://doi.org/10.1098/rspb.2004.2746 [A]
- Beven K (2006). *Journal of Hydrology* 320:18. https://doi.org/10.1016/j.jhydrol.2005.07.007 [A]
- Broido AD, Clauset A (2019). *Nature Communications* 10:1017. https://doi.org/10.1038/s41467-019-08746-5 [F]
- Chave J, Muller-Landau HC, Levin SA (2002). *American Naturalist* 159:1. https://doi.org/10.1086/324112 [A]
- Clark JS (2012). *TREE* 27:198. https://doi.org/10.1016/j.tree.2012.02.001 [A]
- Clauset A, Shalizi CR, Newman MEJ (2009). *SIAM Review* 51:661. https://doi.org/10.1137/070710111 [F]
- Dennis B et al. (2006). *Ecological Monographs* 76:323 [S, via T2; DOI not checked]
- Dietze MC et al. (2018). *PNAS* 115:1424. https://doi.org/10.1073/pnas.1710231115 [F]
- Evans TS, Giometto A (2011). arXiv:1105.4044. https://arxiv.org/abs/1105.4044 [F, via T2]
- Gerrodette T (1987). *Ecology* 68:1364. https://doi.org/10.2307/1939220 [A]
- Gotelli NJ, Graves GR (1996). *Null Models in Ecology*. Smithsonian Institution Press. https://www.uvm.edu/~ngotelli/nullmodelspage.html [S]
- Hahn MW, Bentley RA (2003). *Proc. R. Soc. Lond. B* 270:S120. https://doi.org/10.1098/rsbl.2003.0045 [A]
- Hubbell SP (2001). *The Unified Neutral Theory of Biodiversity and Biogeography*. Princeton. https://doi.org/10.1515/9781400837526 [S]
- Kalyuzhny M, Kadmon R, Shnerb NM (2015). *Ecology Letters* 18:572. https://doi.org/10.1111/ele.12439 [F, via T2]
- Keyfitz N (1981). *Population and Development Review* 7:579. https://doi.org/10.2307/1972799 [S]
- Knape J (2008). *Ecology* 89:2994 [A, via T2; DOI not checked]
- MacKenzie DI et al. (2002). *Ecology* 83:2248. https://doi.org/10.1890/0012-9658(2002)083[2248:ESORWD]2.0.CO;2 [A]
- Mallmin E, Traulsen A, De Monte S (2024). *PNAS* 121:e2312822121. https://doi.org/10.1073/pnas.2312822121 [F, via T2]
- Mallmin E, Traulsen A, De Monte S (2026). *Ecology Letters* 29:e70333. https://doi.org/10.1111/ele.70333 [F, via T2]
- McGill BJ (2003). *Nature* 422:881. https://doi.org/10.1038/nature01583 [A]
- McGill BJ et al. (2007). *Ecology Letters* 10:995. https://doi.org/10.1111/j.1461-0248.2007.01094.x [A]
- Milly PCD et al. (2008). *Science* 319:573. https://doi.org/10.1126/science.1151915 [A]
- Newman MEJ (2009). The first-mover advantage in scientific publication. *EPL* 86:68001. https://doi.org/10.1209/0295-5075/86/68001 [F]
- Petchey OL et al. (2015). *Ecology Letters* 18:597. https://doi.org/10.1111/ele.12443 [F, preprint]
- Price DJ de S (1976). *JASIS* 27:292. https://doi.org/10.1002/asi.4630270505 [S]
- Raftery AE et al. (2012). *PNAS* 109:13915. https://pmc.ncbi.nlm.nih.gov/articles/PMC3435191 [A, via a search summary]
- Rosindell J, Hubbell SP, Etienne RS (2011). *TREE* 26:340. https://doi.org/10.1016/j.tree.2011.03.024 [A]
- Rosindell J et al. (2012). *TREE* 27:203. https://doi.org/10.1016/j.tree.2012.01.004 [A]
- Sinatra R et al. (2016). *Science* 354:aaf5239. https://doi.org/10.1126/science.aaf5239 [A]
- Volkov I et al. (2003). *Nature* 424:1035. https://doi.org/10.1038/nature01883 [F, preprint]
- Wang D, Song C, Barabási A-L (2013). *Science* 342:127. https://doi.org/10.1126/science.1237825 [A]
- Wang J, Mei Y, Hicks D (2014). *Science* 345:149. https://doi.org/10.1126/science.1248770 [A]

**Online platforms, prediction and modelling practice**
- Chandrasekharan E et al. (2017). *Proc. ACM Hum.-Comput. Interact.* 1(CSCW):31. https://doi.org/10.1145/3134666 [A]
- Coalition for Independent Technology Research (2023). Reddit data access letter. https://independenttechresearch.org/reddit-data-access-letter/ [F]
- Denrell J (2003). *Organization Science* 14:227. https://doi.org/10.1287/orsc.14.2.227.15164 [A]
- Denrell J, Liu C (2012). *PNAS* 109:9331. https://doi.org/10.1073/pnas.1116048109 [F]
- Eeckhout J (2004). *AER* 94:1429. https://doi.org/10.1257/0002828043052303 [A]
- Freelon D (2018). *Political Communication* 35:665. https://doi.org/10.1080/10584609.2018.1477506 [S]
- Gelman A, Loken E (2014). *American Scientist* 102:460. https://doi.org/10.1511/2014.111.460 [S]
- Gleeson JP et al. (2014). *PNAS* 111:10411. https://doi.org/10.1073/pnas.1313895111 [F]
- Grimm V et al. (2005). *Science* 310:987. https://doi.org/10.1126/science.1116681 [A]
- Hofman JM, Sharma A, Watts DJ (2017). *Science* 355:486. https://doi.org/10.1126/science.aal3856 [A]
- Huberman BA, Adamic LA (1999). *Nature* 401:131. https://doi.org/10.1038/43604 [A; preprint F]
- Huszár F et al. (2022). *PNAS* 119:e2025334119. https://doi.org/10.1073/pnas.2025334119 [F]
- Iñiguez G et al. (2022). *Nature Communications* 13:1646. https://doi.org/10.1038/s41467-022-29256-x [F, via T2]
- Lech ME, Lehmann S, Juul JL (2024). arXiv:2405.07574. https://arxiv.org/abs/2405.07574 [A]
- Martin T et al. (2016). *WWW '16*, pp. 683–694. https://doi.org/10.1145/2872427.2883001 [F]
- Meta Transparency Center. CrowdTangle page. https://transparency.meta.com/researchtools/other-data-catalogue/crowdtangle/ [F]
- Mitzenmacher M (2005). *Internet Mathematics* 2:525. https://doi.org/10.1080/15427951.2005.10129112 [S]
- Oreskes N, Shrader-Frechette K, Belitz K (1994). *Science* 263:641. https://doi.org/10.1126/science.263.5147.641 [A]
- Perline R (2005). *Statistical Science* 20:68. https://doi.org/10.1214/088342304000000215 [A]
- Ratkiewicz J et al. (2010). *PRL* 105:158701. https://doi.org/10.1103/PhysRevLett.105.158701 [F]
- Salganik MJ, Dodds PS, Watts DJ (2006). *Science* 311:854. https://doi.org/10.1126/science.1121066 [A]
- Salganik MJ et al. (2020). *PNAS* 117:8398. https://doi.org/10.1073/pnas.1915006117 [F]
- Sharma A, Hofman JM, Watts DJ (2015). *EC '15*. https://arxiv.org/abs/1510.05569 [A]
- Stumpf MPH, Porter MA (2012). *Science* 335:665. https://doi.org/10.1126/science.1216142 [A]
- Szabo G, Huberman BA (2010). *Communications of the ACM* 53(8):80. https://doi.org/10.1145/1787234.1787254 [F]
- TechCrunch (18 April 2023). Reddit will begin charging for access to its API [F, via a sub-agent; URL not recorded]
- Working H (1960). *Econometrica* 28:916. https://doi.org/10.2307/1907574 [S]
- Wu F, Huberman BA (2007). *PNAS* 104:17599. https://doi.org/10.1073/pnas.0704916104 [M; already cited in the draft]
- Houssard et al. (2023), cited above, is in the v5 bibliography and was read by the T1 reports; its details were not re-checked here.