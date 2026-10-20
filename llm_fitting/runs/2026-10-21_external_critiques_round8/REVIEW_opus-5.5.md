_Round-8 review by a fresh Claude Opus 5.5 (Anthropic), read-only sub-agent, run 2026-10-21 on manuscript v7 (commit 1652645) with `CRITIQUE_PROMPT_R8.md`. Saved verbatim (the sub-agent's hand-back report) by the orchestrating session._

# Referee report, round 8: manuscript v7 ("When the hierarchy of online attention outlasts its members")

I checked the claims against the `.tex` sources, the compiled PDFs (8 and 22 pages, built 21 Oct 07:03), the figures, `PLAN.md` R15/R16/R16-G, `RESULTS.md` R15/R16, `r16_tier3.py`, `r16_capture.py`, `r12_hetgrowth.py`, `make_v5_figures.py` and the saved `r16_*.json` outputs. I recomputed the following from saved JSON: the R16-F fits, the R16-B profiles, the persistence of fallers and risers in each world, and the capture step sums. I opened no data panels and ran no model.

## 1. Editor's decision now

**Send out, without enthusiasm.**

- The question is general and the record is exceptionally transparent. Expectations were registered, failures are reported, and every statistic is computed by the same code in data and worlds. An editor will not desk-reject that.
- After R16, two of the three findings are now "consistent with the standard model." Durability is the balance, which is stationarity. Persistence is reproduced by fixed differences. The positive core is a measured accounting whose two central relations the paper itself calls near-identities (l.138, l.151).
- The remaining discovery, "declines persist and gains do not," is stated against zero. It does not hold against the paper's own homogeneous benchmark (§5 below).
- Everything is on development data examined through 16 rounds of analysis, with no confirmatory test.
- The text is extremely dense. It reports five findings and nine simulated models in eight pages, and the abstract has eleven sentences without one lead claim.

**Single change that most raises the odds:** rebuild the paper around one positive, benchmarked claim. The claim: the hierarchy's shape moves only through its leaders' relative decline, while crossings are a steady offset set by volatility and slope. Then make the asymmetry of decline a null-benchmarked, figure-led second result. Register both as primaries for the sealed 2023–24 test; that step is owner-gated.

## 2. The paper after R16

**Is it stronger or weaker?** More defensible, less novel. R16 removed three overclaims:
- "at most three fifths";
- the separation of time scales;
- the understated comments β.

That is good science. But it leaves the title's claim ("outlasts its members") as the textbook property of any stationary random-growth model with a stable drift.

**What is genuinely new and general:**
- **(i) Crossings steady, decline varies.** At the comments head the crossing gain is about 0.2 a year in every half-year cell, while the slide runs 0.12–0.47 (SI l.176; Fig. 2C).
- **(ii) Geometry sets which top sets churn (R9).**
- **(iii) Asymmetric persistence of decline.**

**Is the main contribution clear?** No. The significance statement leads with (i), the title with durability, and the abstract ends on heterogeneity.

**What to lead with.** Lead with (i) stated as a law with a number: the share of the variance in shape change across the 60 cells that comes from the decline term. It can be computed from the saved D1/balance cells. Then lead with (iii).

**Title options:**
- "Random growth and persistent decline in the hierarchy of online attention" (Fable, R7). It now fits the paper better than the current title.
- "The leaders' decline, not turnover, moves the hierarchy of online attention."

**Suggested opening for the abstract:** "Online attention is concentrated in a few communities, yet a fifth of every Reddit top set is replaced each year. Measured rank by rank, the gain from risers crossing into a top set is steady and is predicted by volatility and the hierarchy's slope; the shape of the hierarchy moves only when its members' relative decline departs from that gain…"

## 3. R16's designs and readings

**A (decline state).** The design is standard, but its reading needs changes.
- **Edges.** The optimum sits at the largest spread (0.7) and the longest half-life (208 weeks) on both ladders (RESULTS R16-A).
- **The state is effectively permanent.** At π_D 0.25 and hl 208, the hazard of leaving is b = 0.75 × (1 − 0.5^(1/208)) ≈ 0.0025 a week. The mean decline spell is therefore about 400 weeks (7.7 years), nearly twice the panel. "Occupy for years at a time" (l.205) understates this.
  - At the edge the model is a skewed two-point fixed type. Decliners lose δ(1−π) ≈ 1.2 log units a year relative; the rest gain 0.4.
  - Reversibility, the GLLM feature, is not identified. Say "a skewed, persistent type."
- **Persistence statistic.**
  - Main text says "a fifth of the persistence or less."
  - The paper's own statistic gives +0.075 vs +0.174 on submissions (43%) and +0.049 vs +0.154 on comments (32%), computed from `r16_world__*__decline_pi0.25*`. Table 1 prints +0.08.
  - The r12 Spearman statistic gives 5% and 26%.
  - The text uses the paper's statistic for the joint form (87%/70%) and the r12 statistic for the decline state. Pick one.
- **Decline-state persistence is depressed by two things:**
  - survivorship: decliners at −1.2/yr leave the universe, and `window_changes` needs finite y;
  - strata of whole-window rank.

  Say so, because it explains why skew and level do not coexist.
- **Comments.** The asymmetry overshoots (+0.126 vs +0.071). The top stratum's persistence is −0.10 vs data +0.11. The rise follow-up is −0.074 vs data +0.049. "The ordering after falls and rises" holds only as an inequality.
- **Registration.** The registered π_D (0.1) failed four of six expectations. The main text omits this.

**B (joint re-estimation).** This is the right idea, but it is a coarse 3-level grid, not an estimation.
- **Every parameter is at an edge on submissions.** The argmin is s 0.8 (max), head 0.25 (min), km 0.5 (min) and m 0.8 (min of {0.8, 1.0, 1.2}).
  - SI l.391 names only km and head. RESULTS adds s but not m.
- **The surface is noisy.** Profiles over s are 0.6 → 46.6 and 0.8 → 38.8. Profiles over m are 38.8 / 65.9 / 46.6 (non-monotone), which is 3-seed noise. Only km is sharply determined (39 / 338 / 1,752), and it is at the edge.
- **"Fits on submissions" uses a borrowed reference distribution.** It rests on the 9-moment score (8.0) judged against R16-C's distribution, which was built for a different estimator (the R12 2-parameter grid, truth on grid). The 24-moment objective (38.8) has no correct-specification calibration.
- **The fit has costs the main text omits** (they are only in Table 1 and the SI):
  - top-ten turnover falls to 0.18 against the data's 0.33;
  - the boundary ratio rises to 1.22 [1.17, 1.28] against the data's 0.80;
  - the raw β is 0.11, outside the data's 0.05–0.08.

  The world that reproduces persistence fails the paper's opening fact, turnover.
- **The "passing" swing is no longer passing.** A half-life of about 140 weeks is not passing on the paper's 1–2-year horizon. It erodes the lasting/passing split on which Eq. 1, the three-block variance and the "lasting crossing gain" depend. For example, 2v[ρ(52) − ρ(104)]/52 ≈ 0.0007·v a week leaks into the lasting variance at that half-life.
- **The A1 false-positive run tested the wrong direction.** It used a faster swing (26 weeks). R16-B says the slower swing does the work, so the needed check is a homogeneous world at km 0.5 calibrated by the joint estimator.

**C (A1 runs and the objective under correct specification).** Good practice; the reading is right. Two caveats:
- the grid is not re-simulated per replicate, so the simulation noise is common to all replicates;
- the truth lies exactly on the grid.

Both understate the null objective slightly; neither matters against 351 and 1,013. "Cannot tell fixed from decaying" is correctly reported.

**D (β recovery).** Standard and sufficient for homogeneous truth; the reading is right. It was not run under the joint world, which the paper now offers as the description of persistence. SI l.437 still carries the move-variant numbers ("low by 0.072"), which is confusing next to R16-D.

**E (capture audit).** This is a sensible event study.
- **What it excludes:**
  - level shifts of about 0.06 log or more at file boundaries, including 1 July 2021;
  - collapse-driven falls;
  - the 2K threshold.
- **What it cannot exclude:**
  - (a) smooth drift (admitted);
  - (b) within-file capture, which the audit itself detects;
  - (c) dated within-file events.
- **The step sums are a weak test.** In `r16_capture_daily__reddit_submissions_long.json` the mid-month steps sum to −1.11 and the boundary steps to +0.09, against a total of −0.93. Under a smooth trend each set would carry about −0.46. The data instead show a sawtooth of about 1.4% per file. "None of the decline at boundaries" is therefore partly produced by the sawtooth, not a clean null.
- **Two unexplained dated events.** Two mid-month steps, −0.243 on 15 Feb 2021 and −0.220 on 15 May 2021, together carry half the submissions head's item-share fall (−0.93). They exceed the whole 2021 block's change (−0.38).
  - "The largest steps are dated events" (SI l.23) names no event.
  - Plausible candidates are the end of the GameStop/WSB surge and the May 2021 crypto crash: event communities selected into the block's head and decaying. These would be selection, not capture.
  - Name them (communities may be named) and show the head's fall without them.
- **Labelling.** The within-file gradient is post hoc but is used unlabelled in the main text (l.189) as proof of power.

**F (durability in the calibrated model).** The design idea is right: a rank-based step in decline, on common seeds. The reading is overstated.
- **The missing fit code.** No committed script produces `r16_durability_scores.json`. I reproduced both fits exactly (τ 1.47/3.72 submissions, 2.17/3.41 comments at m 1) with a first-order lag of time constant hl/ln 2 on block means. Commit the code.
- **τ is weakly identified.** The response has not saturated by year 5: submissions increments are 0.061, 0.092, 0.099, 0.102, half the step rate. So τ is extrapolated and depends on the assumed lag.
  - Lag at 1.5× the median half-life: τ = 9.4 years on submissions (ratio 2.07) and 5.0 on comments (1.17).
  - Lag at 2×: τ is unbounded on submissions; 8.6 years on comments (2.02).
  - Lag free: half-life 80 weeks and τ 4.7 years (ratio 1.03) on submissions; 40 weeks and τ 3.8 (0.90) on comments.
  - "0.7–1.0" is too precise; "about one, not well bounded above" is honest.
- **Measure the lasting levels directly.** The model's lasting levels H are known in simulation, so measure the lasting shape's response directly and run 15–20 years (model-implied).
- **"Crossings offset about three fifths"** (l.241; SI l.161) is the observed shape, which includes the swing lag. In lasting levels the fitted response at 5 years is −0.55 (submissions) and −0.53 (comments), so crossings offset about 45–48%.
- **Run in the wrong world.** F was run only in the homogeneous model, while B's preferred world has a swing twice as slow, which would lengthen the observed lag.
- **The data's own episode does not fit the model's dynamics.** The block-1-minus-block-0 response is −0.094 per 0.2/yr step (comments, m 1). The observed −0.46 in 2019→20 would need about 1.0/yr of extra lasting decline, roughly three times the measured excess slide. A lasting shift would also keep the observed slide rising, but it fell to 0.12.
  - So the 2019–20 comments fall is not a lasting-decline shift in the model's own terms.
  - Test the obvious alternative: the 2020 surge × rank-dependent exposure. The shrunk stratum loadings run 0.41 → 1.09 from the top, and the comments factor rose +1.33 in 2019 (RESULTS D3, SI l.390).
- **F3 could hardly fail.** Both time scales scale roughly as 1/σ².

**G (decline state on the joint optimum).** The design is the weakest of the round.
- It adds variance (sd 0.3–0.7) on top of a fixed spread already at its edge-maximum (0.8). The covariance moments must overshoot, so G1's failure (39 → 80) is built in.
- "The moments that the persistence fits do not want the decline state that the asymmetry needs" (SI l.422) is therefore not shown.
- The informative design trades s for sd at constant total spread, with km freed. Note that along the s-slice the asymmetry grows more reversed as s rises (−0.041, −0.070, −0.078; `r16_joint_grid__reddit_submissions_long.json`).

## 4. Each Results section: is the evidence sufficient, and what will a referee attack first?

**R1 (turnover vs shape).** The evidence is sufficient for the mean-log span.
- First attack: "changes far less" is measure-dependent. Both heads lost 0.62 in three years, and the share fell from 50.2% to 34.0%.
- Put the measure in the heading: "…while the span of log activity changes far less."

**R2.** The heading "random growth shows when…" overclaims.
- First attack: both relations are near-identities. Also, the submissions crossing ratio, 0.80 (0.68–1.00), lies below every random-growth world (1.04–1.22, Table 1). The "instrument" fails on one of the two main ladders, and the cause is unexplained (SI l.114).
- Second attack: the time-scale paragraph (l.155) rests on F's lag assumption.

**R3.** Sufficient for "forms a hierarchy"; the flatter balance is honestly reported.
- First attack: the drift comes from 2020–22 slides, so "from equality" is not independent of the observed slides. The model world's ladder is an input.

**R4.** First attack: with a hold drift, "forecasts neither fall" is close to true by construction (G31 was softened, not removed). Second attack: the two 2021 within-file steps above.

**R5.** First attack, which I would lead with as a referee: "gains do not persist" against the paper's own null (§5). Second attack: the statistic switching between forms (§3A). Third attack: R16-G.

## 5. The asymmetry as the discovery

**Robustness of the statistic.** `h.asymmetry` computes Spearman correlations within the truncated halves x<0 and x>0 (`r12_hetgrowth.py` l.99–112). In the construction run identically in worlds (`r16_moments__*.json`):

| | data, fallers | data, risers | matched control, fallers | matched control, risers |
|---|---|---|---|---|
| submissions | +0.141 | +0.069 | −0.095 | −0.043 |
| comments | +0.113 | +0.042 | −0.083 | −0.038 |

- Relative to homogeneous growth, gains persist by +0.11 and +0.08, and declines by +0.24 and +0.20.
- Declines persist about twice as strongly. Gains persist too.
- The main text's "0.00 to +0.03" for risers (l.202) comes from the true-sign split, which was never computed in any world.
- The tail follow-ups (+0.03 after a top-decile rise) also lack a homogeneous benchmark. Under reversion the benchmark is negative, so +0.03 may itself be persistence.
- "Declines persist and gains do not" (title-level claim, l.192, l.81, l.75) must become "declines persist about twice as strongly as gains."

**Selection.** The strata use whole-window rank, which looks ahead. That is fine for the data-vs-world comparison, but it must be stated for this statistic.

**Mean reversion.** It is symmetric and cancels in the difference. The worlds' −0.05 baseline shows that the observed asymmetry is not zero under the null, and the text says so.

**Survivorship.** Exits are dropped (y must be finite). This biases against the finding, which is worth saying because it helps the case. Better: report a version with exits kept at the floor (the "shadow of death" form).

**Capture.** Not tested. R16-E recomputed persistence without collapses but not the asymmetry. Recompute it without terminal collapses, without the June 2020 bans, and before July 2021 and after the 2020 surge.

**Rank dependence.** Not reported. GKOS's central result is that sign asymmetry depends on earnings level. On submissions the top stratum has no persistence at all (−0.003), and B's head multiplier is pinned at the minimum. The asymmetry is a body phenomenon on submissions, while the head's fall is a head phenomenon.
- Say this plainly. It supports GLLM's scale-dependence channel for the fall and argues against the type channel at the head.

**Novelty.** The finding is not new as a phenomenon:
- earnings: Guvenen–Karahan–Ozkan–Song 2021 (cited), and Arellano–Blundell–Bonhomme 2017 (not cited), which is exactly "persistence depends on sign and size";
- credit-rating downgrade momentum: Lando & Skødeberg 2002, which is cited only in the SI uses table;
- equity momentum concentrated in losers: Hong–Lim–Stein 2000;
- firms' "shadow of death": Griliches–Regev 1995;
- online community decline: Kairam et al. 2012, which is in `references.bib` but uncited, and Butler 2001.

The new parts are:
- the attention domain;
- measurement against a calibrated random-growth null;
- the result that symmetric heterogeneity calibrated to the level reverses the sign.

**What would make it convincing:**
- **One figure:** a GKOS/ABB-style nonparametric impulse response, E[Δ₂ | decile of Δ₁], stratum-demeaned, for data, matched control, joint world and decline world, by rank stratum, with bootstrap bands.
- **One statistic** used everywhere.
- **The robustness splits above.**
- **A registered confirmatory test** on 2023–24.

## 6. Round-7 points

**Resolved:** G1–G5, G7–G12, G14–G19, G21–G23, G25, G27, G29, G31–G33; P1–P5, P9, P11, P12.

**Half-resolved:**

| point | what remains |
|---|---|
| G6 | The SI fix sits in a paragraph that is now commented out (SI l.162 appended to a `%%` line; absent from the PDF). |
| G13 | A1 was run, but its false-positive arm tests the opposite swing direction from the one R16-B needs. |
| G24 | Joint estimation was done, but at a corner. |
| G26 | Fixed at SI l.369 but contradicted at SI l.438 ("a post hoc extension shows it is the minimum"). |
| G28 | Estimator-free, but now dependent on F's lag. |
| G30 / P7 | "Some of which return" (l.107) is still unneeded. |
| P6 | Stated, not checked. |
| P8 | Capture audit done; smooth drift untested. |
| P10 | Defined, but it is the root of §5's error. |
| P13 | Results 2 is still overloaded; Discussion ¶2 still reads like a response letter. |

**Superseded:** G20 (withdrawn).

**Still open:**
- The R7 missing references: Gleeson 2014, Blumm 2012, Ratkiewicz 2010, van de Rijt 2014, Kairam 2012 and Houssard 2023 are all still uncited.
- Owner items.

## 7. Errors

1. **"A fifth of the persistence or less"** (l.205) conflicts with Table 1 (+0.08 against +0.17) and with the paper's statistic (43%, 32%). Even the r12 statistic gives 26% on comments.
2. **"Rising down the ladder as the data's does"** (l.199) is false on comments: joint −0.00, +0.14, +0.06 against data +0.11, +0.14, +0.15.
3. **Abstract (l.81): "make gains persist as much as declines."** They make gains persist more: risers +0.106 vs fallers +0.034; after a rise +0.19 vs after a fall −0.11.
4. **Edges:** on submissions all four joint parameters are at the grid's edges (SI l.391, l.407).
5. **SI l.369 vs l.438** contradict each other on the comments minimum (G26).
6. **Table 1, "crossings / slide" row.**
   - The data entry 0.64 is c/(s̄−e). The world entries are c/s̄: joint 0.82 and decline 1.07 equal `mean_c/mean_sbar` in `r16_world__*`.
   - The SI atlas gives 0.72 (= 0.0040/0.0055) for the same data column.
   - The main text (l.149) calls 0.64 "pooled."
   - Use one definition throughout.
7. **Table 1 note** ("every adequate world reproduces their range") is false for the joint world's β (0.11) and the decline state's crossings/slide (1.07).
8. **Commented-out text.** l.152 ("Part of the slide is mechanical … 0.0009–0.0019 a week less") is absent from the PDF, yet the Discussion (l.239) still says "Part of it is mechanical." SI l.162 (the heterogeneity caveat for every time scale) is also absent. Both are accidental appends to `%%` lines.
9. **"Crossings offset about three fifths"** (l.241, SI l.161) includes the observation lag; in lasting levels the figure is about 45–48%. "Absorbed over the next few years" is an extrapolation, since the response is unsaturated at year 5.
10. **"The step reaches the observed shape after about a year"** (l.155): year-1 response is 14% (submissions) and 26% (comments) of the undamped step. Also, τ "3–4 years" is the m = 1 value; across m it is 2.8–4.5.
11. **Fallers "+0.16 to +0.19"** (l.202); the SI gives +0.158 and +0.177.
12. **Fig. 1A** draws 2020–22 balance-law means (`_law()` in `make_v5_figures.py`) on the 2019 ladder. The caption implies 2019, when the comments head's slide was 0.47.
13. **Fig. 2A** keeps the linearized relaxation curve (9–17 years, comments) as its dominant element, while the text says it overstates the model's response by 2.6×.
14. **Post hoc labels missing in the main text:** the lag-aware fit, the within-file gradient, R16-G, and the failed registered π_D. Methods (l.259) promises such labels.
15. **SI l.491 is garbled** ("in a small recovery study calibrated to Facebook … +0.045 at 86 in a small recovery calibrated to Facebook, is nearly unbiased on comments…"). **SI l.271** has a comma splice ("+0.102, The platform-total…").
16. **SI l.390 ("What the forms do")** still asserts the R12 shortfall without saying that it holds with the passing parts fixed and is superseded two paragraphs later.
17. **Stale headers.** Both `.tex` headers say "DRAFT v5" and "R12 results are pending," in a public repository.

## 8. Writing and structure

**Structure.** Results 2 is seven paragraphs carrying four ideas: crossings, cautions, geometry, time scales. Split it:
- (2) "Crossings are steady; the decline moves the shape";
- (3) "Geometry sets which top sets churn," which is R9, the reviewers' strongest result, currently buried mid-paragraph at l.143;
- (4) "No slow transition," a short paragraph with the linearizations moved to the SI.

Fold the generative model (R3) into Methods plus one paragraph, since Fig. 3A/B carry it. Results 5 should lead with the impulse-response figure.

**Topic sentences** are good. Density is the problem: l.155 contains four estimators of a time scale in one paragraph.

**Figures.**
- The asymmetry, now the main discovery, has no panel.
- Fig. 4A's forecast markers, with intervals up to 86%, are unreadable.
- Fig. 3B hides the comments year-3 point behind the submissions point.

**Table 1** mixes coded and uncoded rows and two definitions. Restrict it to the six tests, and add a row "turnover cost" for the heterogeneous columns.

**SI organization.** The SI is ordered as a laboratory log. Reorder it to the main text's claims, each with claim, statistic, data value, null and registration status. Replace the 700-word registration paragraph (SI l.514) with a table.

**Rewrites:**
- **l.155 (time scales):** "In the calibrated model itself, a step of 0.2 a year in the head's decline moves the observed shape slowly at first, because observed activity follows the lasting level with a lag of one to two years, and then at about half the step's rate through year five (SI). Fitted with that lag, the response time is about one membership e-folding (3–5 years); the window does not bound it well from above. The shape therefore has no slow mode of its own: it holds when the decline matches the crossings it draws, as among comment communities in 2020–22."
- **l.202 (asymmetry):** "Relative to homogeneous growth, both declines and gains persist, but declines about twice as strongly. Measured identically in data and worlds, the year-apart correlation among communities that fell is +0.14 (submissions) and +0.11 (comments), against −0.10 and −0.08 in the matched homogeneous world; among those that rose it is +0.07 and +0.04, against −0.04 (Fig. 4E)."
- **Discussion ¶2:** open with one claim ("Neither capture nor collapses produce the head's fall") and move the remaining caveats to Limits.

## 9. The five most valuable changes before submission (data on hand)

1. **Re-benchmark the asymmetry.**
   - Use one construction; report fallers and risers each against the matched control.
   - Add the impulse-response figure by decile and stratum, with exits kept at the floor and with collapses, bans and periods excluded.
   - Rewrite the title, abstract, significance statement and the Results 5 heading accordingly.
   - Cost: light jobs (worlds at 10–20 seeds with tails saved).
2. **Repair R16-F.**
   - Measure the lasting-shape response directly, run 15–20 years, free the lag, and run it in the joint world too.
   - Correct "three fifths."
   - Test the 2019–20 comments fall against surge × rank loading.
   - Commit the fit code.
3. **Close R16-B and redo R16-G.**
   - Extend km (0.25, 0.35), head (0, 0.1), m (0.6) and s (1.0).
   - Calibrate the 24-moment objective under correct specification.
   - Run the slow-swing false-positive check.
   - Redo G trading s for sd.
   - Put the turnover and boundary costs in the main text.
4. **Fix the §7 errors, commented-out text and labels.** Name the communities behind the 15 Feb and 15 May 2021 steps, and show the head's fall without them.
5. **Restructure.**
   - Lead with the decline law and quantify the variance share of the decline term.
   - Promote R9.
   - Compress the model atlas.
   - Reorder the SI.
   - Add ABB 2017, Hong–Lim–Stein 2000, Griliches–Regev 1995 and Kairam 2012; move Lando–Skødeberg into the main text.

**Needs the owner or new data:**
- **The confirmatory test.** Amend the 2023–24 protocol for the source switch and the blackout, add the decline law and the null-relative asymmetry as primaries, adopt it, then open the bridge. This is the change that would turn stylized facts into discoveries.
- **The raw-file capture audit** with collection timestamps (WD drive).
- **A second platform for the asymmetry** (Wikipedia; exposure unknown).
- **Remaining owner items:** title; classification; DOI, Pushshift source and date, and Page IDs; the chapter's pages and the 2005–08 quantile plot.

The reproduction scripts I wrote are in the scratchpad and touch only saved JSON:
- `/private/tmp/claude-501/-Users-hindman-Documents-GitHub-rank-diffusion/a6cd00c7-4734-43aa-9ddc-bbbdd4d7c13b/scratchpad/fit_dur.py`
- `/private/tmp/claude-501/-Users-hindman-Documents-GitHub-rank-diffusion/a6cd00c7-4734-43aa-9ddc-bbbdd4d7c13b/scratchpad/fit_dur2.py`
