_Round-4 review by a fresh Claude Opus 5.5 (Anthropic), run as a read-only sub-agent of the orchestrating Claude Code session, 2026-10-18/19, from `CRITIQUE_PROMPT_R4.md` (committed 6590c29 before the review). Returned as the agent's final message and saved verbatim by the orchestrating session. Model output: checked in `SYNTHESIS_R4.md`._

# Round-4 referee report: "Constant turnover holds up the durable hierarchy of online attention"

**How I worked.** I read `paper/pnas_draft.tex` and compiled it to scratch: 7 pages, abstract 249 words, significance 115, and the only warning is a duplicate `LastPage` label. I also read:
- `pnas_si.tex`, the five figures and `make_pnas_figures.py`;
- PLAN R3/R4/R5 and RESULTS;
- `null_worlds.py`, `churn_counterfactual.py`, `gomez_r3.py` and `persistence_post2020.py`;
- MODEL_STATUS §2z-dh/di, plus §2z-da §4/§7 and §2z-ca (the daily date check);
- bridge protocol v3.1.

I recomputed from saved JSON only: the 60 rate-law cells, `null_world__*.json`, the counterfactual JSONs, `turnover_kinematics.json` and `exponent_robustness.json`. I opened no raw data, touched no sealed path and wrote nothing in the repository.

---

## 1. Editor's decision now

**Send out, without enthusiasm.**

1. The question is right for PNAS. The data are unusual: three ladders, named units, rank by rank. The discipline (registration, disclosed failures, null worlds) is better than most submissions.
2. The title's answer is the stationary balance between random growth and rank-dependent decline. That is Fernholz's stable capital-distribution curve with churning ranks, and Gabaix (1999). The paper's own measurement is that turnover replaces only about three quarters of the loss, so "holds up" asserts more than is measured.
3. The null worlds now carry the "automatic vs discovered" split, but:
   - each world is run once (`null_worlds.py`, `seed=0`);
   - each starts from a constant-slope Zipf ladder far from its own stationary state;
   - two of the three "discoveries" do not survive them as run (§4).
4. The most quotable sentence ("persistence … cut the share of Reddit submissions' head from 50% to 34%") is contradicted by the repository's own daily check. The June 2021 step is in votes per post, not posting, and starts with Reddit's Home-feed rollout.
5. Two results are new and well defended:
   - Reddit's gapped persistence (+0.13 to +0.20), carried by item counts;
   - near size-blind lasting volatility on comments and Facebook.

   Built on those, with the balance as the frame, the paper would go out with enthusiasm.

**The single change:** rebuild the null worlds and rewrite the ledger from them:
- at least 20 seeds with bands;
- starts from each world's own stationary ladder and from the data's ladder;
- the data's passing parts;
- an added world with heterogeneous drift by community.

## 2. The title and the question

**Partly answered.** The paper gives an accounting, d(U_k − U_K)/dt = c_k − s̄_k + e_k. But the actual answer to "where does durability come from" sits in a subordinate clause (line 144). Names cross a boundary on a one-year scale (15–33% a year). The shape relaxes over 2–22 years because c and s̄ are both about 0.004–0.006 a week against a span of 3–4 log units. Durability is this separation of time scales. Lead with it and quantify it.

**"Holds up" is not earned.**
- **The measurement says the opposite.** Over the 60 cells, c/s̄ = 0.0040/0.0055 = 0.72 (recomputed). The abstract itself says "the hierarchy flattens at the difference."
- **The model says it on one ladder only.** In the comments counterfactual (`churn_counterfactual__reddit_comments_ext.json`), churn + slide flattens the K/64 shape at −0.080 a year against −0.038 for slide only. Churn + slide is at or below slide only at every cut-off (K/8: +0.013 against +0.040). The reason is that churn creates slide: 0.00295 a week with churn against 0.00064 without. On submissions churn does hold the head up (−0.160 against −0.259).
- **Turnover sits on both sides of the ledger.** Driftless Gibrat produces a slide of 0.0044 a week, 80% of the data's 0.0055.
- **The three supporting bullets are one inequality.** c_k/s̄_k = σ²/(2S_k s̄_k) = S*/S, so these are the same fact:
  - "three quarters";
  - "flatter by 8–46%";
  - "flattening".

  The flat start reaching σ²/(2s̄) is the simulator obeying the stationary identity. Its 90% bands are seed bands only. Uncertainty in σ² and s̄ (submissions ratio 0.63–1.01) would cover, for example, comments K/2: 1.97 against 2.13, an 8% gap.
- **"Without churn it collapses" is false on comments below K/32.** Slide only steepens K/16 … K/2 by +0.005 to +0.049 a year.

**Weakest word: "holds up."** Second: "durable."
- The head's arithmetic share fell by 32% in three years.
- Comments' mean-log shape steepened in 2 of 5 year intervals (+0.10 and +0.13 a year) and flattened in 3 (−0.04 to −0.24).

**Titles that keep the owner's question:**
- *Why the hierarchy of online attention outlasts its members*
- *How a durable hierarchy of online attention survives constant turnover*
- *Constant turnover nearly offsets decline in the hierarchy of online attention*

**The short seller's case against the title.** The title says turnover holds the hierarchy up. The repository's own numbers say otherwise:
1. Crossings fall a quarter short.
2. The crossing term in the rate law is a constant by construction (§3(3)).
3. The measured crossing statistic is 25–35% composition, generated by persistent drift (§3(3)).
4. In the authors' model, removing churn slows the comments head's fall by half.
5. Turnover's own re-selection produces most of the slide it is credited with offsetting.

## 3. Each Results section: the first attack

**(1) The puzzle.**
- Line 98, "moves … in one direction", is false for the mean-log shape on comments. By year interval, c/s̄ is 0.46, 0.68, 2.19, 2.40 and 0.72, and the two steepening intervals are H2 2020 → H2 2021 and H1 2021 → H1 2022.
- The top-ten rates come from 3 year pairs × 10 names, a standard error of about 0.08, with no bands. AGENTS.md requires bands for head claims.

**(2) Size, Eq. 1, C(R).**

*First attack: the "size-blindness exposes the top" contrast is single-seed.*
- Top-ten turnover is size-blind 0.33 against firm-like 0.17, about 1.5 SE apart.
- At K/64 (78 names, SE about 0.03) the size-blind world is flat: 0.23 against 0.22 at K/2. Beyond k = 25, "fastest at the top" in the data comes from the slope profile (Eq. 1: S is 0.7–1.0 at the head), not from size-blindness.
- In a stationary rank-based world the slope adjusts: S* = σ²/(2s̄), so turnover ≈ E[D⁺]/S scales like s̄/σ.
- A firm-like world at stationarity would therefore have a flatter head and at least as much head turnover. In Atlas models (Banner–Fernholz–Karatzas) crossing local-time rates are set by drifts, not variances.
- The nulls instead start at S = 1 and run 4 years, against the paper's own 5–22-year relaxation.

*Second attack: β.*
- Under the null's passing noise, the estimator returns 0.114 for a planted 0.2 (world ii). The abstract's "0.01–0.08 against 0.15–0.2 for firms" compares attenuated numbers with unattenuated ones.
- Submissions (0.045, 5–95% −0.013 to 0.143; 0.082, 0.039–0.150) cannot be separated from firm-like. Leaving out the top stratum gives 0.189 in year 1 (`exponent_robustness.json`).
- The model's own lasting β on comments is **0.126** (counterfactual JSON). The SI's "lasting 0.047 (0.025–0.078)" says otherwise.

*Also:*
- Facebook's rank-1 occupant changes in 52% of weeks, yet the abstract says rank one "rarely changes" and Fig. 2C's title says "Yet rank one rarely changes hands."
- The Eq. 1 cells start at K/64, so the decomposition never reaches the top ten.

**(3) The balance and the null worlds.**

*First attack: "excludes neutral copying" rests on one cell.*
- Neutral by cut-off, k = 312 … 5,000: 0.54, 0.82, 1.04, 1.17, **0.12**.
- Without k = K the pooled ratio is 0.86; over K/8 … K/2 it is 1.00.
- Wright–Fisher copying is itself a diffusion in frequency. Away from the universe edge's discreteness the relation should hold. The ratio robustly excludes only random-rank jumps (1.56).

*Second attack: the composition term.* C is +25–35% of the statistic on Reddit, arising from "persistent differences in drift" (§2z-da §4).
- Lasting crossings alone are about 0.77 of σ²/2S on comments, about 0.55–0.6 on submissions and about 1.3 on Facebook.
- The pooled 0.94 is a cancellation of opposite-signed terms.
- On Reddit the gap is filled by the very persistence that Results 5 says random growth lacks.

*Third attack: the estimator is biased by cut-off in known-truth worlds.*
- Gibrat runs 0.66 → 1.29 and Atlas 0.53 → 1.39 across cut-offs.
- Fig. 3A shows the null worlds only pooled.

*Fourth attack: the rate law.*
- σ² is one estimate per stratum, identical in all five pairs (`law__*.json`; `variance_out_of_sample` false in 4 of 5).
- So c is constant over time by construction, and "the variation comes from the slide" is a property of the design.
- Fig. 3B plots the 52-week design, where the comments points mostly lie above the diagonal. Its y-axis is the predicted σ²/2S. The text quotes the 26-week cells.

**(4) The generative model.**

*First attack: every in-sample "reproduction" is an input or automatic.*
- World (iv) uses the hold closure (`variant="hold"`, `null_worlds.py`:112), so the ladder is an input.
- β is an input, and turnover then follows by Eq. 1 kinematics.
- The boundary ratio is automatic, and C(1) follows from the ladder's gap.
- Even so, turnover at K/2 is 0.14 against the data's 0.18.

*Out of sample, the model is never the best arm:*
- on submissions no arm beats persistence (0.183 against 0.163);
- on comments, drift by rank worsens error (0.100 → 0.130);
- retention MAE loses to A on submissions (0.028 against 0.024) and to carrying forward on comments (0.022 against 0.013).

The flat start is a third configuration: "move" drift from the measured slides.

**(5) Persistence and the falling head.** Persistence itself stands.

*First attack: the head.*
- The §2z-ca daily check: the top five's karma share fell from 13.7% to 9.4% from 23–24 June 2021, while their share of posts "has no onset and stays near 1%."
- §2z-da §7: in the same weeks the head's comment activity rose (+0.107, above every placebo year). The record's own reading is "a change in exposure, not in participation."
- The paper's persistence is carried by counts (+0.13 to +0.19); engagement per item persists only +0.02 to +0.07. The head's step is in the component that does not persist.
- On submissions, persistence is a loading on the platform total that fades within a year.

*Second attack: Gomez's "permanent-rank cohorts".*
- They use permanent rank over the start block itself (`gomez_r3.py`:53), so "not selection" does not follow.
- This breaks Methods line 192 ("disjoint").
- The 2019–20 "−0.06 expected from selection on noise" has no computed baseline.

*Third attack: the confounder checks.*
- They rest on 2–4 and 1–2 overlapping origins, with single-seed worlds.
- Each excludes only one confounder, so line 166's "It is not the surge … nor the change in capture" overstates.
- The platform contrast: at Facebook's horizon Reddit gives −0.04 to +0.08, yet the abstract says "on Reddit, but not Facebook."

## 4. Automatic against discovered

**Called discoveries that null worlds produce:**
- **Crossings short of the slide.**
  - firm-like world: 0.0027 against 0.0048 (shape −0.0021 a week, flattening faster than the data);
  - neutral copying: 0.0044 against 0.0048;
  - the model world: 0.0030 against 0.0037.

  Only driftless Gibrat steepens. SI line 205 and §2z-dh ("against every null world") are contradicted by SI line 201. The shortfall means "the ladder started steeper than its stationary shape", an initial condition. It restates the observed flattening.
- **The top turning over fastest** depends on the calibration (stationary or data-ladder starts; see §3(2)).
- **Persistence** is discovered only against *homogeneous* random growth.
  - Heterogeneous fixed drifts produce it: the SI's own synthetic check, with a drift sd of 0.4 a year, gives +0.17 to +0.19.
  - So does heterogeneous loading on a trending aggregate, which is the submissions result.
  - The two-type GLLM rival is still not run.
- **β ≈ 0 on comments** depends on the estimator: 0.047, 0.075 or 0.126.

**Called automatic but informative:**
- **Rank-1 stability.** The size-blind world gives 0.13; the data give 0.03, 0.07 and 0.52. The comments leader sits about 1.5 log units clear in Fig. 1A, an outlier rather than S/R.
- **The composition term C.** It is persistence showing up inside the boundary statistic.
- **The boundary ratio by cut-off.** At the head the data give about 1.2, the diffusion nulls 0.5–0.7, and the model world 1.27.
- **The slide beyond its definitional part:** about 0.0011 a week.
- **Mean growth.** The data reject Gibrat's law in the mean: submissions drift is −0.0103 a week at the head against −0.0036 at K. That is what makes a stationary hierarchy possible, so "follows Gibrat's law" should say "in its second moment."

## 5. Round-3 points

**Resolved:**
- Eq. 1 recast as a decomposition, with v_k;
- Iñiguez, Blumm, Batty, Bentley, Coad, Guvenen, Kairam and Cunha cited; C(R) computed;
- one corrected model for the gates and A6, with the frozen 0.097 labelled in the SI;
- the model equations as coded, and the parameter structure stated;
- the gate's cohort and noise-scale recalibration disclosed;
- the A5c failure and the 13-week overshoot disclosed;
- the true-sign split with quintile means;
- prefix-only membership for persistence;
- the number errors of round 3 (W19);
- the joint bootstrap described in Methods;
- registration language; P7 names the corrected model;
- the SI TODOs; flat-start bands; the Fig. 5A caption; A7-4 in the main text; the definitional slide; the entry term in Eq. 3.

**Half-resolved:**
- the title verb ("sustains" → "holds up", the same counterfactual problem);
- "slowly changing" → "durable", still unquantified;
- the null worlds (one seed each, non-stationary starts, no heterogeneous-drift world);
- Gomez: the general form is in the main text, but SI lines 63 and 187 keep the Pareto form, and the cohorts are on the same block;
- V13: one confounder excluded at a time;
- the platform contrast: disclosed in the text, still in the abstract;
- V9: by-cut-off nulls exist but are not shown;
- V5 tails: kurtosis 22.1 against 2.9 appears in the SI only;
- "drift by rank worsens error" is buried behind "within 0.03";
- the head share, 0.340 against 0.356, is unreconciled;
- V8: "none fitted to the statistics we test it on" sits beside the hold closure.

**Open:**
- fitted level rivals (two-type GLLM, Yule–Simon);
- cross-model recovery;
- the capture-delay audit;
- gate median quantisation;
- a time-block bootstrap;
- the persistence → head-fall link, which is now stronger in the text and contradicted by the record.

## 6. Errors

1. SI line 205 lists "crossings fall short of the slide" as not automatic; the table at SI line 201 shows it in worlds ii, iii and iv.
2. "Excludes neutral copying" (line 77, line 136, Fig. 3A) rests on the k = K cell (0.12).
3. Line 177 says permanent cohorts rule out selection, but `gomez_r3.py`:53 takes permanent rank on the start block. Line 192's rule says disjoint.
4. SI lines 63 and 187 still present ½(ζ − 1)ν² as Gomez's formula, which was withdrawn.
5. SI line 147 is labelled "net of loading on the platform aggregate" but carries the cohort-mean values (0.149/0.054/0.006). The main text and Fig. 5B use the platform total (0.132/0.020/−0.029).
6. Line 169 says items carry "+0.14 to +0.19"; the SI gives +0.125 to +0.188.
7. The head share in 2022 is 34.0% at lines 77 and 98 but 0.36 at line 157 (SI 0.356). In 2020 it is 0.508 against the 0.497 implied by the SI's Gomez table. Define both denominators.
8. Lines 77 and 148 say "nearly size-blind volatility"; the model's comments β is 0.126.
9. Line 144, "on comments the two terms are of the same size at every rank", holds only on average (0.46–2.40 by interval).
10. Fig. 3B: the y-axis is the predicted c; the data come from the 52-week design and the nulls from the 26-week design.
11. Line 154, "without churn it collapses", is contradicted on comments (see §2).
12. "Rank one rarely changes" (abstract, Fig. 2C) fails for Facebook (0.52).
13. Line 157, "within 0.03": the comments spread is 0.0305, and SI line 94 says the expectation failed.
14. Line 154 says the model reproduces turnover at K/2 of 0.14; the data give 0.18.
15. SI line 169 says the registered test excludes both confounders. P5's change 2 (2024 Q1 − 2023 Q1) spans the Pushshift → Arctic Shift switch of April 2023 and the June 2023 blackout.
16. Line 185 says the protocol tests "the balance". P1–P2 test the boundary ratio; the shortfall is only S1, a secondary test.
17. Line 93 promises three findings, line 181 says "Two are not" automatic, and SI line 205 lists three.
18. Fig. 5A's legend says "Facebook (26-wk)" while the caption says 4-week windows. Fig. 2A has a single y-tick. Fig. 4D uses the internal label "(A)".
19. Citations:
    - `bakersmyth2026` lacks the authors' names;
    - CrowdTangle's List IDs and year of access are missing;
    - `fernholz2017`, local-time estimation from rank data and the closest methodological precedent, is in the bib but uncited.

    Also missing: Huberman & Adamic (Nature 1999, Gibrat growth of websites); Ratkiewicz et al. (PRL 2010); Riccaboni et al. (PNAS 2008); TeBlunthuis et al. on Reddit community ecology; Fernholz 2002's capital-distribution stability as the precedent of the puzzle.

## 7. Data availability

The statement is mostly adequate and close to the careful precedents (Grunwald et al. PNAS 2022; Sangiorgio et al. 2024). Fix:
- **The verification offer for CrowdTangle data.** Offering the CrowdTangle exports to "editors and reviewers on request" is not supported by the documented terms. The FAQ says raw exports may not be shared outside the university research team, and `DATA_AVAILABILITY_NOTES.md` found no such permission. Either obtain Meta's written consent (the terms allow it) or offer verification without transfer, for example supervised code runs or published output hashes.
- **The deposit.** "Every analysis output … in the deposit" means the public repository. Before submission, audit it for per-Page values (FB gate and card logs exist), and archive a frozen snapshot with a DOI, as PNAS requires.
- **Reddit.**
  - State how replicators obtain the Pushshift files: Reddit cut off Pushshift in 2023, and the dumps now circulate through third parties.
  - Check the current Reddit Data API terms; the notes say this is still to do.
  - Naming subreddits fits the 2023 statement as the notes summarise it (no users, no redistribution).
- **Citation.** Add the CrowdTangle List IDs. The owner must still confirm publishing Page IDs; the terms permit it.

## 8. The five most valuable changes, with data on hand

1. **Rebuild the null worlds** as specified in §1, and also:
   - add a size-blind world carrying the data's slide profile;
   - show the boundary ratio by cut-off;
   - drop or justify k = K.

   Rewrite the ledger from the result. My expectation for what survives:
   - discoveries: size-blind lasting volatility on comments and Facebook, and persistence against homogeneous growth, carried by counts;
   - descriptive, not a discovery: the shortfall, which is the observed flattening.
2. **Re-lead on the measured answer.**
   - Durability is slow relaxation against fast residence.
   - Turnover replaces about three quarters of decline.
   - Report the comments counterfactual.
   - Present the three restatements as one fact.
   - Retitle (§2).
3. **Uncouple persistence from the head's fall.**
   - Remove the causal claim from the abstract and significance, or test it.
   - Run the Gomez decomposition on item counts and on karma per item, with disjoint-block cohorts.
   - Put the June 2021 daily check in the main text.
   - Ask whether the head's decliners are the persistent decliners.
4. **Repair the boundary section.**
   - Report R_bc/c beside R_bc + C.
   - Show the nulls by cut-off.
   - Correct the neutral-copying claim.
   - Use a pair-specific, out-of-sample σ² in the rate law, or state that c is constant by construction.
   - Fix Fig. 3B.
5. **Make the model earn its place, or move it to the SI.**
   - Reconcile β (0.126 against 0.047).
   - State the inputs of world (iv).
   - Add parameter uncertainty to the flat-start bands.
   - Add a drift by community, one parameter from the gapped covariance, and test whether it reproduces persistence and the head out of period.

   Then fix every item in §6.

**Needs the owner or new data:**
- **Adopting protocol v3.1, amended:**
  - P5 reported without blackout participants, plus a 4-week variant within Arctic Shift;
  - P6's thresholds calibrated against the firm-like null (raw ≤ 0.12 passes a planted 0.2);
  - the balance shortfall and the K/64 turnover ordering registered as primaries;
  - P1 reported by cut-off.
- **Drives:** the capture-delay audit (WD) and post-count head shares for 2019–22 (T9).
- **Owner items:** CrowdTangle List IDs and access year, the Page-ID decision, Meta's consent for reviewer access; the Reddit terms check; a DOI; authorship.
- **A second platform with an annual horizon** (the sealed Wikipedia ladder), because Facebook has only 86 weeks.
