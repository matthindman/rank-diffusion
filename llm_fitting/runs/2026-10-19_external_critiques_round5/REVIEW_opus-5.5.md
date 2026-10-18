_Round-5 review by a fresh Claude Opus 5.5 (Anthropic), run as a read-only sub-agent of the orchestrating Claude Code session, 2026-10-19, from `CRITIQUE_PROMPT_R5.md` (committed c203b99 before the review). Returned as the agent's final message and saved verbatim by the orchestrating session. Model output: checked in `SYNTHESIS_R5.md`._

# Round-5 referee report: "How constant turnover holds up a durable hierarchy of online attention" (v3, commit 576a8e4)

**How I worked.** I worked read-only. I compiled the paper into the scratchpad: 7 pages, abstract 250 words, significance 120, every citation key resolves. I recomputed numbers from these saved outputs:
- `null_worlds_r7_summary.json`, including the β 0.05 and 0.10 worlds, which are uncommitted in the working tree;
- the `churn_counterfactual__*__long30/flat20.json` files;
- `runs/2026-10-14_balance_law_halfyear/cells/law__*.json`;
- the `head_components`, `gomez_r7_prior`, `turnover_kinematics`, `exponent_robustness` and `persistence_r3` outputs;
- `GATE_LEDGER.md`.

I opened no data files and touched no sealed paths.

## 1. Editor's decision now

**Send out, without enthusiasm. With a social-science handling editor there is a real risk of desk rejection.**

1. The question suits PNAS. Named communities followed rank by rank over four years, with 20-seed null worlds, scored failures and sealed confirmation data, are rarer than most submissions offer.
2. v3 itself concedes that the title's answer is automatic: "Much of this is automatic in random growth" (draft l.183).
   - S\* = σ²/2s̄ is the Fernholz/Gabaix/Gomez balance.
   - The 30-year counterfactuals show what any Atlas model does when σ or g is set to zero.
   - The "three-quarter balance" restates the observed flattening (c/s̄ = S\*/S).
3. What remains outside random growth is modest, depends on the estimator, and is not connected to the title's mechanism:
   - Reddit's persistent decline in participation;
   - a lasting β weaker than firms'.
4. The repository's most striking fact cuts against "durable": the submissions head lost a third of its share in three years (members' term −0.24 and −0.26 a year) while r/memes held rank 1. The model does not reproduce it (forecast 0.53–0.54 against 0.36).
5. Each referee on the panel has an obvious line of attack:
   - the stochastic-portfolio theorist will call the counterfactuals tautological;
   - the inequality economist will call "durable" a choice of metric;
   - the econometrician will count 82 gate evaluations and eight post hoc addenda on development data examined many times, with a sealed protocol that does not test the title.

**The single change that would most raise the odds.** Make the hierarchy's steepness depend on something random growth lacks.
- Split the measured slide (0.0055 a week) into the part a driftless world started from the same ladder produces (0.0026 from the data's ladder, 0.0044 from a constant slope) and the excess, on both ladders.
- Test whether the excess is carried by the communities whose decline persists: the gapped bottom quintile, in item counts.
- If it is, the title's mechanism and the paper's discovery become one finding, the head's fast fall becomes the fast transition of Gabaix, Lasry, Lions and Moll (2016), and the answer stops being automatic.
- Register that test for the sealed data.

## 2. The title and the answer

**The evidence, piece by piece:**
- **Flat start.** Fig. 4B shows the simulator obeying its stationary identity: S against σ²/2s̄ computed inside the simulation. The empirical content is that the measured moments give a steepness 0.54–0.93 of the observed one (K/64: comments 0.55 against 0.99, submissions 0.55 against 0.85).
  - So the model builds *a* hierarchy, 35–46% flatter at the head. It does not build *the* hierarchy, as the abstract's "forms the hierarchy" (l.79) says.
  - The bands are seed bands only.
- **30-year counterfactuals (R6).**
  - They are post hoc: PLAN R6 was committed together with its results in 576a8e4. The main text does not label them post hoc.
  - They replaced a registered two-year counterfactual that failed: A3-2 and A3-3, and on comments churn plus slide flattened at −0.080 a year against −0.038 for slide only.
  - The outcomes are structural. With no noise, a deterministic drift that is lower at the top compresses the ladder; pure diffusion spreads it. Any positive parameters give that.
  - The slide-only runs are degenerate. On comments the 30-year slopes are 0, 0, 0, 2.15, 1.06, 0.09: a flat top above a cliff. On submissions the head shape falls to about 0.2 by year 24, then rebounds to 0.47.
  - Comments' "−61%" is still falling (−1.68 in the last decade), so the figure depends on where the run stops.
  - With both ingredients the head still flattens by 18% (submissions) and 23% (comments).
  - The drift g comes from the 2020–22 slides (`churn_counterfactual.py`: `prs` = primary pairs). The noise baseline puts 2019–20's members' term at noise (−0.061 against −0.053). So the run extrapolates a two-year episode over 30 years.
- **The measured balance.** c/s̄ = 0.0040/0.0055 = 0.72: by the paper's own measure, turnover returns three quarters, so it does not hold the hierarchy up.
  - 0.72 is a ratio of means. Over cells, S\*/S has mean 0.79 and median 0.70.
  - Comments alone give 0.85, with 40% of comments cells above 1, that is, steepening.
  - The slide is 47–80% mechanical. Comments' own mean slide (0.0045) is about what driftless size-blind growth gives from a constant slope (0.0043).
  - The completed calibration world with **no drift by rank** and planted β = 0.10 gives c/s̄ = 0.76. So the "balance" statistic does not identify the decline ingredient.
- **Time scales.**
  - The paper's data figure (SI l.71, from §2z-cp) is 3–4 years at the submissions head. "2–6" spans every cut-off, so l.147's "2 to 6 on the submissions head" is mislabelled.
  - Membership at K/64 turns over 0.19–0.23 a year, an e-folding of about 4 years. At the submissions head there is **no separation** of time scales.
  - The model with the same ingredients relaxes far more slowly. At the submissions head the gap to its own equilibrium (3.32) goes from 1.19 to 0.39 in 30 years (τ ≈ 27 years). At the comments head it goes from 1.48 to 0.27 (τ ≈ 18 years), against §2z-cp's 9.
  - The paper's two time scales therefore differ by a factor of 2 to 8. The separation argument needs the model's number, which the data's number contradicts.
  - On comments the argument holds: annual shape changes stay within ±0.06 against 16–27% turnover.

**Weakest word: "holds up".** It is contradicted by c/s̄ = 0.72 and by the arithmetic share of every top set falling in every year.
- Next weakest is "durable": it fails at the submissions head, where the concentration story lives.
- "Turnover" names the wrong cause. What spreads the ladder is volatility. Figs. 3–4 call it "churn", the text "volatility", the title "turnover". Turnover is its kinematic by-product (Eq. 1).

**Proposed title:** *How the shape of online attention survives constant turnover*. It keeps the owner's question. "Shape" is what is measured as durable: the mean-log curve, not the shares. "Survives" claims persistence through turnover, not causation. If the change in §1 succeeds: *Random growth and persistent decline set the shape of online attention through constant turnover*.

## 3. Each Results section

**(1) The puzzle and the named leaders.** The section supports its lead for the mean-log shape.
- **First attack: the metric.** The hierarchy is durable in U_k − U_K, not in shares. The head's share went 50.2% → 34.0%, falling 16% and then 19% in 2020–21 and 2021–22.
- **The top-ten rates have no bands.** They rest on three pairs of ten names each (SE ≈ 0.08), and Figs. 1C and 2B show no data bands, despite AGENTS.md's rule for claims about the head.
- Comments' top ten (0.267) turn over no faster than its top 25 (0.267).
- Some exits are administrative (two bans in June 2020) or tied to events (r/place, Superstonk). No world has this hazard.
- "A span of three to four" is undefined. The head shape at the start of the 30-year runs is 4.51 (submissions) and 5.36 (comments); 3.3–4.0 is the K/16 shape.

**(2) Why the top turns over fastest, and size.** The lead is not supported as written.
- **First attack: the top ten.** l.114 says that, started from the data's ladder, "the top ten turn over fastest". In `null_worlds_r7_summary.json` the top 25 turn over faster than the top ten in every data-ladder world:

  | world | top 25 | top 10 |
  |---|---|---|
  | size-blind | 0.243 | 0.205 |
  | firm-like | 0.196 | 0.192 |
  | the model | 0.231 | 0.228 |
  | β 0.05 | 0.224 | 0.167 |
- The data's top ten (0.33) sit at the size-blind world's q95 (0.333) and above the firm-like (0.268) and model (0.302) q95s.
- At K/2 the data (0.178) lie outside every calibrated world's band (about [0.133, 0.144]). The world's drop from K/64 to K/2 (0.074) is 2.4 times the data's (0.031).
- So the geometry over-explains the body and under-explains the very top, and the Eq. 1 cells start at K/64, so they never reach the top ten. The extra body turnover is plausibly the persistent component; that link is worth testing.
- "Flattest at the head" is itself the head's large slide (S\* = σ²/2s̄), so the two are one fact.
- **Size.**
  - Fig. 2A still plots the raw β against a firm line, titled "Size gives limited protection", while l.116 says the raw exponent "says little here".
  - The calibration is now complete: planted 0, 0.05, 0.10, 0.2 give raw 0.062, 0.072, 0.078, 0.094 (slope about 0.16). Submissions' year-1 raw value of 0.045 lies *below* the planted-zero mean.
  - The lasting β is 0.047 (two-step) against 0.126 (structure function) on comments, and 0.070 against 0.045 on submissions: the two estimators order the ladders oppositely.
  - No lasting β has an interval. The recovery sd of 0.029 comes from Facebook-calibrated 160-week panels, so comments' 0.126 cannot be told apart from firms' 0.15.
  - For Facebook the text reports the two-step 0.02 and omits RESULTS R7's structure-function 0.11, biased by about +0.045 (so roughly 0.065).

**(3) What turnover returns, and durability.** "Returns three quarters" stands as a description. "Durable because the terms are small" falls to §2's time scales.
- **First attack: the RMSE comparison.** The comparison at l.147 (0.0040 / 0.0029 / 0.0011) is an accounting identity in which c is constant by construction. Leaving out a term of an identity naturally fits "badly".
- **The boundary ratio is presented as a finding.** The abstract gives it with an interval while l.139 calls it close to an identity.
  - Against the worlds' 1.04–1.12, the data's 0.94 is about 15% low.
  - Net of the 25–35% composition term, Reddit's lasting crossing gain is about 0.6–0.75 of prediction, so the "match" is a cancellation.
- **σ² is biased by the paper's own persistence.** Lasting variance from variance growth with spacing is inflated by heterogeneous drift, by about 3h·var(μ) for equally spaced blocks. That pushes S\* upward.

**(4) The generative model and the counterfactuals.** The lead is supported only as "the model class behaves this way".
- **First attack: structure and post hoc design.** The counterfactuals are structural, post hoc and partly degenerate, and they extrapolate an episode (§2).
- The model's head is 35–46% flatter than observed, with bands from seeds only.
- At submissions K/2 the 30-year slope moves *away* from the balance: 1.55 → 1.58, against 1.37.
- Held out, the model is never the best arm:
  - on comments it scores 0.130 against 0.100 without drift;
  - on submissions it loses to carrying forward (0.183 against 0.163);
  - on retention it loses to the fixed-home model on submissions and to carrying forward on comments.
- The variance correction was made after the gate failed and was rescored on the same panel.

**(5) Persistence and the head's fall.** The section supports persistence against homogeneous worlds.
- **First attack: "every world".** Every world lacks heterogeneity by construction. The SI's own heterogeneous-drift check (l.153) gives +0.17 to +0.19, the data's value.
  - No heterogeneous-drift or trend-exposure world is in the ledger.
  - On submissions, persistence is exposure to one platform trend, and it fades within a year.
- **The confounders.** They are excluded one at a time, on one to four overlapping origins, yet the abstract says "not explained by the 2020 surge or a 2021 change in capture".
- **The head split.** The item share is relative to the top 5,000's items, and no absolute counts are reported.
  - The rise in relative karma per post (2.25 → 3.92) is the signature of a denominator swollen by high-volume, low-karma posting elsewhere.
  - In that case "the head's participation fell" means the rest of the top 5,000 posted more.
- **The prior-year Gomez cohort is not an exact decomposition.** Within plus between is −0.253 + 0.139 = −0.114, not the share change of −0.161, because the base is the prior cohort's activity.

## 4. What remains discovered

As it stands, this is not enough for PNAS.

- **Persistence against every world.** Overstated. It rejects homogeneous Markov growth, which every world is by construction. Heterogeneous growth is known for firms (Coad), incomes (GLLM, Guvenen) and online groups (Kairam, Cunha).
  - What is new is its texture: in counts, in decline, a common factor on submissions, community-specific on comments, reversed on Facebook.
  - The gapped statistic is a sound methodological contribution.
- **The measured balance and its time scales.** This is not a discovery. It is automatic, and its two time scales are inconsistent.
- **The participation split.** A descriptive identity at one ladder's head, with the denominator problem above and no link to persistence (l.179 concedes the gap).
- **Weaker-than-firm size dependence.** It holds on submissions. On comments it depends on the estimator and has no interval. Fig. 2A contradicts the text.

**Undersold:**
- **The GLLM fast-transition point.** GLLM show that homogeneous random growth moves concentration too slowly and that heterogeneous types speed it up. Here the head fell at −0.25 a year, the homogeneous model relaxes over 18–27 years and misses the fall, and the data show type-like persistence. That is the strongest non-automatic story in the repository.
- **Rank stability against share collapse.** Rank 1 was held in 92% and 98% of weeks while the leaders' shares fell 74% and 57%. Rank is a poor measure of entrenchment, which matters for platform-power debates.
- **Excess turnover in the body over every calibrated world** (0.178 against 0.14).
- **The ledger of what is automatic,** as a contribution in its own right.

## 5. Round-4 points

**Resolved:**
- F2 (flattening restated as a state);
- F3 (20 seeds);
- F4 (worlds started from the data's ladder);
- F6 (rescoring);
- F7 (the causal clause removed);
- F9 (the noise baseline);
- F10 (0.031);
- F11 ("not in one direction");
- F12 (the head share defined);
- F13 (neutral copying is an edge effect);
- F14 (Facebook's rank one changes in 52% of weeks);
- F15 (stale SI rows);
- F16 (item range);
- F17 (the model world's inputs stated);
- F18 (c constant by construction, stated);
- F21 (no data transfer);
- most of F22 (figure labels);
- F23 (citations).

**Half-resolved:**
- **F1.** The long-run runs were added, but they are post hoc and structural, and the two-year failure was moved to the SI.
- **F5.** The calibration was run, but lasting β is not reconciled (0.047 against 0.126), Fig. 2A is still raw, and Facebook's estimator was chosen selectively.
- **F8.** A prior-year cohort was used, but the decomposition is no longer exact and this is unstated.
- **F19.** The composition term is stated, but the lasting-only ratio is not reported.
- **Title and "durable".** Retained, against all three round-4 referees.

**Open:**
- **F24:** SI l.187 still says the registered test excludes both confounders, but P5's second change (2024 Q1 − 2023 Q1) spans the 2023 source switch and the June 2023 blackout;
- a heterogeneous-drift world;
- fitted GLLM and Yule–Simon rivals;
- parameter uncertainty in the flat-start bands;
- cross-model recovery;
- the capture-delay audit;
- whether the head's decliners are the persistent decliners;
- l.118's S/R account of rank-1 stability: comments' C(1) of 0.03 is below every calibrated world (0.09–0.12).

## 6. Errors

1. The SI title (pnas_si.tex l.10) is the v2 title.
2. l.114: "the top ten turn over fastest" in the data-ladder worlds is false; the top 25 turn over faster (§3(2)).
3. **Fig. 5A crosses** are the round-3 single-seed worlds (`make_pnas_figures.py` l.311–313 loads `null_world__{w}.json`), although the text says 20 runs.
   - The caption says "the four simulated worlds of Fig. 2", but Fig. 2 shows five different worlds and not the model world.
4. **Post hoc labelling.** SI l.119 says the 30-year runs are "post hoc, expectations committed before the run". This contradicts SI l.252 and git (PLAN R5–R8 were committed with the results).
   - The main text labels none of R5–R8 post hoc, though Methods (l.203) promises labels.
5. l.171: "Choosing the universe from the first half-year alone changes these statistics by less than 0.01" contradicts:
   - SI l.172 (platform residuals move by 0.04: +0.020 → +0.063);
   - R4b (submissions 0.104 → 0.130);
   - SI l.187's "at most 0.03".
6. The abstract's "not explained by the 2020 surge or a 2021 change in capture" overstates; the body says neither accounts for it *alone*.
7. SI l.187: "which the registered test on 2023–2024 data does" exclude both is false (F24).
8. l.147: "S\*/S = 0.72 on average" is a ratio of means; the mean over cells is 0.79.
   - The same line's "2 to 6 on the submissions head" spans all cut-offs; the head is 3–4 years. Comments' head is 9 years, not "5 to 22".
9. The Fig. 3B caption says "30 half-year-start intervals each"; it is 30 *cells* each (five intervals × six cut-offs).
10. Fig. 4B plots the observed slopes against the *simulation's* σ²/2s̄. Its title, "Its steepness is churn over slide", presents an identity as a result.
11. Terminology:
    - Figs. 3–4 say "churn" where the text says "volatility" and the title "turnover".
    - Fig. 1's caption says "slowly flattening", against l.100's "not in one direction".
12. The β calibration (0.05 and 0.10, complete) is missing from SI l.235 and the main text. Committed RESULTS still says "still running".
13. Fig. 2A legend shows β = 0.04 for submissions where the text has 0.045. The raw β is shown as evidence the text disowns.
14. l.116: Facebook's structure-function 0.11 (bias +0.045) is omitted in favour of the two-step 0.02.
15. The abstract's "15–33%" mixes a by-cell range (15–26%) with top-ten means; by year the submissions top ten reach 40%.
16. The abstract's "forms the hierarchy" should be "a hierarchy, 35–46% flatter at the head".
17. The prior-year Gomez terms (l.179, SI l.207) are not an exact decomposition; say so.
18. SI l.252 says "83 evaluations". The ledger has 83 rows, one of them a CORRECTION row, so 82 evaluations.
19. Methods l.193, "Cohorts are fixed on a block disjoint…", is not true of the current-year Gomez cohorts (Fig. 5C) or the Eq. 1 inputs.
20. l.139: neutral copying "between K/16 and K/2 behaves like a diffusion" — its ratio at K/16 is 0.68.
21. Missing citations:
    - Pal & Pitman (2008) for the stationary-gap identity;
    - Gaffney & Matias (2018) on Pushshift's missing data, which is essential given the capture caveat.
    - Also check that Sangiorgio 2024 supports "grows independently of size"; the paper is about post virality against followers.
22. The text runs 7 pages against PNAS's standard 6. TODOs remain: DOI, Reddit acquisition, Page IDs, CrowdTangle List IDs.
23. "Every number in the paper was checked against saved outputs" (SI l.255) is now falsifiable by items 1–9. Soften it, or make it true.

## 7. The five most valuable changes before submission, ranked (data on hand)

1. **Split the slide into its mechanical part and the excess,** using a driftless calibrated world for *each* ladder (comments has none) at 20 seeds.
   - Test whether the excess is concentrated in the persistent decliners and in item counts, and whether the head's 2020–22 decliners are those communities.
   - Rebuild the title and abstract on the result.
2. **Fix durability.**
   - Use one relaxation-time definition, computed identically in the data and the model, against member residence by cut-off.
   - State that separation holds on comments and not at the submissions head.
   - Lead with the shape, not the shares, and retitle (§2).
3. **Repair Results 2.**
   - Restate the turnover claim as K/64 against K/2.
   - Report that the worlds miss body turnover (outside their bands) and the very top.
   - Add data bands.
   - Replace Fig. 2A with the lasting β, with bootstrap intervals and the four-point calibration line.
   - Report all three Facebook estimators.
4. **Honest labels and uncertainty.**
   - Label R5–R8 post hoc in the main text.
   - Set the failed registered two-year counterfactual beside the 30-year runs.
   - Add parameter uncertainty (σ0, β, slides) to the flat-start and counterfactual bands.
   - Describe the slide-only runs as a two-tier collapse.
   - Drop the abstract's boundary interval, which is an identity.
5. **Audit the head's fall.**
   - Report the head's and the top 5,000's absolute post counts by year, and its share of all-Reddit posts.
   - Identify which communities drive the denominator's growth.
   - Cross-check against Pushshift's known gaps.
   - Then correct every item in §6 and trim to 6 pages.

**Needs the owner or new data:**
- **Re-align bridge protocol v3.1 before adopting it.**
  - P1 and P2 test what the paper calls an identity.
  - P6 cannot fail under firm-like volatility: the firm-like world's raw β of 0.094 [0.077, 0.107] passes ≤ 0.12.
  - P5's second change spans the 2023 source switch and the June 2023 blackout.
  - S1 uses the 3–4-year relaxation time that the model contradicts.
  - Register as primaries: the out-of-time balance (the sign and size of the 2022→23 shape change from S\*/S by cut-off), excess slide > 0, a blackout-robust P5, and the lasting β with its calibration. Then adopt and open the bridge.
- **The capture-delay audit (WD drive).** Every ladder is karma, the metric most exposed to capture timing.
- **Owner decisions and items:** the title, Page IDs, CrowdTangle List IDs and access year, the Reddit acquisition source and terms, a DOI, authorship.
- **Generality.** Wikipedia's status is exposure unknown, so as a test of generality it is weak. Without a second annual-horizon platform, scope the title to Reddit or to online communities.
