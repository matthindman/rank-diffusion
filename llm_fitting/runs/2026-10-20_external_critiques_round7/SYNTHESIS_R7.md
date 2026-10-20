# Round-7 referee synthesis: manuscript v5 (three models; GPT-6 Pro pending)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-20._
- **Brief:** `CRITIQUE_PROMPT_R7.md` (9d78e65, before the reviews).
- **Reports**, saved verbatim in this folder: gpt-6-astra (Codex CLI 0.163.0, read-only); Claude Fable 5.1; a fresh Claude Opus 5.5 (both read-only sub-agents).
- **Manuscript reviewed:** v5 at 61b6bbe.
- **Pending:** a self-contained prompt for GPT-6 Pro (`PROMPT_GPT6PRO_R7.md`), to be run by the owner in chat.

A claim below is CONFIRMED only after this session rechecked it against code or saved outputs. PLAUSIBLE means not yet rechecked.

## 1. Verdicts

| reviewer | decision | single most valuable change |
|---|---|---|
| gpt-6-astra | send out, without enthusiasm, after correcting substantive errors | rebuild the central comparison around what each model was calibrated on and what its failure establishes |
| Fable 5.1 | send out; not yet with enthusiasm | a planted-against-recovered diagnostic for R12; then restate R12, and move the asymmetry into the abstract as the finding |
| Opus 5.5 | send out, without enthusiasm | make durability a *displayed* prediction: the observed 2019–22 shape against the rate law and the model, at every cut-off |

**Where they agree:**
- The frame is right and the registration exemplary.
- The positive core is measurement of known theory, so enthusiasm must come from the discoveries. The discoveries end on negatives.
- R12 is stated above its evidence ("at most three fifths"; "none forecasts the fall"; the top-ten claim).
- The asymmetry of persistence (decline persists, gains do not) is the paper's real discovery and is undersold.
- The abstract's "one of them calibrated" is wrong: two forms were calibrated.
- Table 1 mixes pooled and submissions values, and is unreadable at column width.
- "A sixth to a third within five years" (Discussion) is unsourced and inconsistent with Results 2.
- The two Results 2 paragraphs and the R12 paragraph are overloaded.
- The weakest title word is "attention" (Fable, Opus) or "members" (gpt).

## 2. Verified findings

### A. Errors of fact in v5 (fix before anything else)

| # | finding | by | check | status |
|---|---|---|---|---|
| G1 | **The shape moved more than the paper says.** l.110 claims "in no year did it move by more than a third of a log unit". The comments head (K/64) fell 0.45 in 2019→20 (0.42 at K/32, 0.41 at K/16), then 0.14 and 0.03. The submissions head fell 0.18, 0.29 and 0.20. Over three years both heads lost about 0.65 log units | Opus (E1) | recomputed from `runs/2026-10-15_puzzle/puzzle__*.json` (block ladders interpolated in log rank): comments 5.58, 5.13, 4.98, 4.95; submissions 4.57, 4.38, 4.10, 3.89 | **CONFIRMED.** The sentence most likely generalized the 2020–22 rate-law cells, which omit 2019→20. The comments drop falls in the year of the 2020 surge. Consequences: R1's claim, R2's "on comments the data behave that way", and the comments durability claim all need restating; the model's "less than 1% in five years" from the 2019 ladder sits beside an observed −0.63 in three years. See §4 |
| G2 | Table 1's "data (subm.)" column shows pooled values. Crossings/predicted is 0.94 pooled; submissions is 0.80 | gpt, Opus | `null_worlds_r7_summary.json`; A9 | **CONFIRMED.** The codes (below/above) do not change against 0.80. Crossings/slide 0.72 is also pooled (Opus recomputes 0.64 for submissions; to recheck) |
| G3 | "Constant slope" names two different sets of worlds: Table 1 (`sb_flat`, `firm_flat`, R9) and the SI atlas (`gibrat`, `atlas`, Zipf ladders) | gpt, Fable, Opus | R7 summary: top-10 0.158 against 0.243 | **CONFIRMED** |
| G4 | The abstract's "one of them calibrated to that persistence": fixed and decaying both are. The Fig. 4C caption repeats it | all three | Methods; `r12_hetgrowth.py` grid | **CONFIRMED** |
| G5 | SI after Eq. S2: S* is defined with (s̄ − e), but the text then says c/s̄ = S*/S. It should be c/(s̄ − e). The Fig. 2C caption says the shape holds on c = s̄; with entry and exit it holds on c = s̄ − e | gpt | `pnas_si.tex` l.108; caption | **CONFIRMED.** Round-5 G1 was not carried into the SI |
| G6 | **My SI caveat has the wrong sign for relaxation.** τ = 2S*²ln(K/k)/σ² = σ²ln(K/k)/(2d²) ∝ σ², so an overstated σ² makes τ too *long*, not too short. The membership time S²/σ² is too short | gpt | algebra | **CONFIRMED** (introduced 2026-10-20 in 61b6bbe). The main-text sentence ("lasting movement slower; the balance flatter") is right; the SI's "times would then be longer" is wrong for τ |
| G7 | "A sixth to a third of the effect within five years" (l.232) contradicts l.147's rates. A tenth a year on comments and a quarter to a third at the submissions head give about 41% and 75–85% in five years. The "sixth to a third" comes from 8S²/σ² (13–31 yr) | gpt, Fable, Opus | arithmetic | **CONFIRMED** |
| G8 | The SI's "uniformly in *log* rank … one e-fold above its boundary": uniform in rank gives ∫₀¹ −ln u du = 1; uniform in log rank gives (ln k)/2 | gpt, Opus | algebra | **CONFIRMED** |
| G9 | Main text l.166, "below the top 780 on comments it matches": the SI says from K/8 (1,562) down; at K/16 the shortfall is 28% | Opus | SI l.188 | **CONFIRMED** (main text wrong) |
| G10 | Fig. 4C's dashed "net of platform loading" line is the cohort-mean adjustment (`D7.minus_aggregate_loading`: +0.149, +0.054, +0.006), not the platform-total loading the text describes (+0.132, +0.020, −0.029) | Opus | `make_v5_figures.py`; `out_persistence_r3_*.txt` | **CONFIRMED** |
| G11 | The SI calibration paragraph names the lasting-variance statistic "structure-function". The code uses the two-spacing slope [V(52) − V(26)]/26 (A4) | gpt | `r12_hetgrowth.py` `sf_lasting` | **CONFIRMED** |
| G12 | SI: "the estimators are rerun on panels simulated from each calibrated heterogeneous world". Only the fixed form was recovered | Fable | recovery mode | **CONFIRMED** |
| G13 | A1's misspecification run (fit (a) to a (b) world) and false-positive run (homogeneous world with a 26-week medium swing) were registered and never run, and are not listed as not run | gpt, Fable | PLAN A1 item 5; run scripts | **CONFIRMED** |
| G14 | "Fixed effects are truncated at ±2.5σ_b": the code clips (`np.clip`), so mass piles at the bounds | gpt | `rankdiff_rdm.py` `_vel_draw` | **CONFIRMED** (wording) |
| G15 | "They give no difference between risers and fallers": every world, heterogeneous ones included, gives a *reversed* asymmetry (−0.04 to −0.10), as the controls do | all three | `r12_table.json` | **CONFIRMED** |
| G16 | SI: two consecutive `\paragraph{Calibration.}` headings | Fable, Opus | `pnas_si.tex` | **CONFIRMED** |
| G17 | Discussion: "selection on noise is excluded by design". R12 strata use whole-window permanent rank, and R10 selects on block A, which enters the change | gpt, Opus | `r12_hetgrowth.py` `strata`; `slide_by_persistence.py` | **CONFIRMED.** Harmless for comparisons made identically in worlds, but the blanket claim is false |
| G18 | R10's excess-share accounting mixes eligibility sets. On matched sets the shares are 61.5% (comments, not 47.3%), 40.9% (submissions, not 39.4%), and 63.2% against the model world (not 60.0%) | gpt | recomputed from `slide_by_persistence__*.json` | **CONFIRMED.** "40–60%" becomes about 40–63% |
| G19 | Smaller slips: "27–33%" for the top ten is a range of three-year means (by year 20–40%); the 50% and 51% comments share appears twice (both 0.505); "lost −0.06" is a double negative; the McGill bib author field; "four rounds" of review (now six); the SI lag-profile values at 52 weeks appear twice from different code | Fable, Opus | — | **CONFIRMED** (each a one-line fix) |

### B. Claims stated above their evidence

| # | finding | by | check | status |
|---|---|---|---|---|
| G20 | "At most three fifths of the persistence" holds at 13 and 26 weeks, but at 52 weeks on comments the fixed form matches (+0.094 against +0.095). The world's lag profile also *rises* with the gap where the data's falls | all three | `r12_table.json` | **CONFIRMED.** Restate by gap, and add "with a lag profile of the wrong shape" |
| G21 | "Fixed differences reproduce the submission top ten's turnover, which homogeneous growth misses": against the matched control, 0.272 [0.133, 0.370], whose band contains the data's 0.333 | Fable | `r12_table.json` | **CONFIRMED.** The claim holds only against the closed-form control (0.23 [0.13, 0.30]) |
| G22 | Out-of-period labels. (a) and (b) take their spread, half-life and multiplier from full-panel grids; only the base is refitted before 2021. (c) uses the realized 2021–22 path and full-panel membership for its loadings. Also, (a) and (b) re-solve the drift to hold the 2020 ladder, so they forecast a fall only through composition | gpt, Fable | `run_r12_r13.sh` l.50–58; `het_params` | **CONFIRMED.** "Fitted to 2019–20, none of the three forecasts the fall" must become "retrospective projections, heterogeneity calibrated on the full panel" |
| G23 | **The R12 worlds carry less heterogeneity than planted.** The year-apart covariance rises from the control by +0.05 to +0.11, against a planted (52σ_b)² = 0.25, i.e. 20–45% recovered. The two-spacing variance rises by 0.0018 against the algebraic 0.0072. The comments body stays negative (−0.043). Likely causes: the re-solved drift by rank, which offsets name drift as communities climb (FIK 2013), and rebirth with fresh draws | Fable (also Opus) | `r12_grid__*__fixed.json` rows (0, 1.0) and (0.5, ·) | **PARTLY CONFIRMED.** Fable's "about 90% lost" compares levels; against the control baseline it is 55–80%. Either way, "calibrated to a spread of 0.5" overstates what the worlds carry. A cheap diagnostic (drift re-solve off, exits off) is needed before the R12 verdict is secure |
| G24 | The passing parts (the medium swing) are frozen at their homogeneous estimates; the income literature estimates jointly. The controls' gap-52 covariance is still −0.04 to −0.05 | Opus | `r12_world__…control_num…` | **PLAUSIBLE.** A design limitation to state. A joint re-estimation is new analysis (owner) |
| G25 | No overidentification statement: an objective of 351 over 9 moments with 2 parameters is a decisive rejection. With diagonal weights it is not chi-square distributed, but the misfit is unambiguous | Fable, Opus | grids | **CONFIRMED** (as a reporting gap) |
| G26 | The grid extension did not establish a minimum on comments: it omitted m 0.7, where the registered optimum lies. On submissions, 351 against 362 at 3 seeds a point is within simulation noise | gpt, Opus | `r12_gridx__*` | **CONFIRMED.** "The minimum lies near 0.5" must be withdrawn for comments and softened for submissions |
| G27 | **The loading adjustment of persistence is fragile.** With per-community 2019 loadings on the platform total, submissions persistence falls (0.188 → 0.020 at 13 weeks) but comments *rises* (0.158 → 0.177). R12-A3 found such loadings almost pure noise. Noise × a reversing factor (submissions path +0.0136 → −0.0035) can manufacture negative covariance | Opus (E9), Fable | `out_persistence_r3_*.txt` | **CONFIRMED** (pattern). Needs shrunk or split-sample loadings and a random-loading placebo before l.188's claim stands |
| G28 | **Lasting-variance estimators disagree about twofold at the comments head:** balance-law three-block 0.0064–0.0097 a week (ranks 1–781) against the R12 two-spacing 0.0039; the structure function recovers σ0 24% high. The durability factor scales as σ⁴ via S* | Opus | `law__reddit_comments_ext.json`; `r12_moments__*` | **CONFIRMED** (the disagreement). Opus suggests computing the factor from measured crossings, 2ln(K/k)(c/(s̄ − e))², with and without the composition term |
| G29 | "The rate law with the measured slide accounts for the fall" is near an accounting identity, and "a shift in its drift" is not identified: the drift could have shifted, or the head could have filled with decliners | gpt, Opus | — | **CONFIRMED** (wording) |
| G30 | R13's rationale ("passing swings reverse") is unsupported. A two-year/one-year ratio below 2 needs no reversals: pure diffusion gives about √2, and independent yearly replacement gives 2 − p ≈ 1.8 | gpt, Opus | algebra | **CONFIRMED.** R13-1 passes, but its stated reason is wrong |
| G31 | Results 4's "the model misses one thing it should be able to see": a hold-drift model forecasts near no change by construction | Fable | provenance table | **CONFIRMED** (wording) |
| G32 | Results 3's "forecasts membership well": on comments, carrying forward wins (0.013 against 0.022); on submissions the fixed-home model wins (0.024 against 0.028) | Opus, Fable | SI | **CONFIRMED** |
| G33 | Horizons: Fig. 3B's 30-year axis, "Continued further … collapses", and "two thirds within a decade" break the owner's 1–2/5-year rule visually | gpt, Opus, Fable | Fig. 3 | **CONFIRMED.** Truncate the main figure at 5–10 years; move the continuations to the SI |

### C. Framing (judgments, not errors)

- **GLLM** (Opus, gpt): there is no GLLM speed failure at the submissions head. Given the measured slide, rank-based growth transmits the change quickly (τ ≈ 3–4 yr). The fall is a shock: the slide quadrupled. Heterogeneity in GLLM governs how fast a shock propagates, not whether one happens. Recommended frame: durability is GLLM's slow transition, measured; the head's fall is a drift shift that rank-based growth transmits fast; persistence is a separate discovery. R12 tested income-dynamics forms, not GLLM's own type switching. **Adopt.** It is more accurate and more interesting.
- **Measure choice** (Opus): durability uses the mean-log shape, while the departure uses the arithmetic share; at a Zipf-like head displacement supports the share about ten times less. One paragraph must say why both are used, or referees will say the measure was chosen per claim.
- **Size in size units** (Fable, Opus): the estimator-free quantile rise converts to an elasticity to size of about 0.08 (comments: ln 1.64 / 6.4 log units) and 0.04 (submissions: ln 1.24 / 5.3). That is below firms' 0.15–0.2 and immune to the comments-estimator caution. **CONFIRMED** arithmetic (`lockin_funnel__*.json` strata sizes). Lead with it.
- **Missing references** (Opus): Gleeson et al. 2014 (PNAS), Luttmer 2011, Blumm et al. 2012, Ratkiewicz et al. 2010, van de Rijt et al. 2014, Kairam et al. 2012 and Houssard et al. 2023 are in `references.bib` but uncited. Ichiba–Pal–Shkolnikov 2013 is to verify. **To verify, then add the relevant ones.**
- **Title:** "attention" (karma and interactions are engaged activity) is the weakest word for two reviewers. Fable proposes "Random growth and persistent decline in the hierarchy of online attention". Owner item.

## 3. What the reviewers credit

- the registration record and the disclosed amendments (A3, A4);
- R9 (geometry isolated) as the strongest result;
- the posting split of the head's fall, and the absolute post counts;
- rank-one stability against the collapse of share;
- the asymmetry of persistence;
- the uses-and-non-uses material;
- sentence length now at exemplar values (their counts: median 19–21 words).

## 4. Judgment: what G1 does to the paper

G1 is the most consequential finding of the round. The comments head lost 0.45 log units of span in 2019→20 and 0.17 over 2020–22. The calibrated model, from the 2019 ladder, moves it by less than 1% in five years. Durability on comments is therefore a claim about 2020–22 and about the balance; it is not a claim about the observed 2019–22 path.

The honest restatement, with the data on hand:
1. Present the observed path at every cut-off, both ladders, against the rate law and the model (Opus's change 1).
2. Say plainly that both heads flattened by about 0.6–0.7 log units over three years: comments mostly in 2019→20, the surge year; submissions steadily.
3. Restate durability as what the balance implies and what 2020–22 shows on comments, not as what the whole period shows.

Whether the 2019→20 drop is the surge, a capture artefact or a real departure is open. The rate-law cells do not cover it, and this is checkable with the data on hand.

## 5. Ranked revision plan

**Tier 1: corrections with no new analysis.**
- **Facts:** G1 (the R1 sentence and the durability scope); G2–G19.
- **Restated claims:** G20–G22, G25–G26, G29–G33.
- **Framing:** the GLLM reframing and the paragraph on the choice of measure; size in size units.
- **Presentation:**
  - Table 1 to full width, with the submissions values and matched-control columns;
  - Fig. 3B truncated;
  - the Fig. 2A title;
  - the Fig. 4A axis;
  - splitting the overloaded paragraphs;
  - the asymmetry in the abstract and in the Results 5 heading.

**Tier 2: cheap measurements on development data** (light jobs, one at a time under the watchdog):
- the observed shape path at every cut-off against the rate law and the model, including 2019→20, and the rate-law cells for 2019→20 if computable;
- the R12 planted-against-recovered diagnostic: drift re-solve off, exits off; submissions first, 3 seeds a point;
- the loading adjustment with shrunk and split-sample loadings, plus a random-loading placebo;
- the durability factor from measured crossings, with and without the composition term;
- the composition term in the simulated worlds.

**Tier 3: new analyses** (owner's call):
- a joint R12 re-estimation with the medium swing freed and a rank-specific spread;
- the two A1 recovery runs, which are registered but not run;
- R14 (mirrored type switching), before or after submission;
- a recovery study of the comments size exponent with the world's own passing parts.

**Tier 4: owner items.**
- the title;
- the chapter's pages;
- the acknowledgement;
- data provenance (Pushshift terms; CrowdTangle Page IDs and date; DOI);
- classification;
- the protocol amendments (P5's source switch and blackout; P6).

## Addendum (2026-10-20): GPT-6 Pro

The owner ran `PROMPT_GPT6PRO_R7.md` in the ChatGPT interface with the v5 PDFs and pasted the report; it is saved verbatim as `REVIEW_gpt-6-pro.md`. The reviewer had the PDFs only, with no repository access. Its arithmetic was checked against the record. Its references were checked against publisher or Crossref records in this session.

**Decision.** "Send out, but not with enthusiasm in its present form." It names three repairs: the durability argument mixes time scales; the submissions departure crosses an incompletely audited capture change; and the heterogeneous-growth conclusions exceed the fitted specifications. It recommends the Social Sciences classification, with Economic Sciences as the alternative, and the title "Why online attention hierarchies can outlast their members". Both are owner items.

**Items that agree with G1–G33** (independent confirmations, no new work):
- Table 1's pooled value (G2);
- the missing e (G5);
- the sign of the variance correction for τ (G6);
- uniform in rank, not log rank (G8);
- "at most three fifths" (G20);
- "a sixth to a third within five years" (G7);
- 50% against 51% (G19);
- the out-of-period labels (G22);
- the Fig. 2C diagonal;
- Fig. 3B's horizon (G33);
- the Table 1 layout.

**New items, checked:**

| # | item | check | status / action |
|---|---|---|---|
| P1 | With net decline fixed, a 79% variance overstatement takes the comments-head ratio of relaxation to movement from 3.9 to about 1.2 | algebra: τ ∝ q, T_move ∝ 1/q, so the ratio ∝ q²; 3.9/1.79² ≈ 1.2 | **CONFIRMED.** Consistent with R15 D4 (crossings-based factor 1.98 gross, 0.97 net of C). Significance and abstract no longer claim "several times"; SI time scales give both rows |
| P2 | "Several times" fails at comments K/2: relaxation 5–10 years against movement 5.9 and membership about 5.7 | SI table | **CONFIRMED.** Restated by rank in the SI |
| P3 | "Stable parameters do not change the slide" is false at finite horizon | — | **CONFIRMED** (wording); removed |
| P4 | The abstract's "a sixth to a quarter of each Reddit top set" excludes the top ten (27–33%) | main text | **CONFIRMED.** Now "a sixth to a third" |
| P5 | The opening's "a sixth of the top 12,500" is supported at K/2 = 6,250 | SI turnover table | **CONFIRMED.** Now "the top 6,250" |
| P6 | Completeness is guaranteed only for weekly top sets, not for sets ranked by annual totals | definition | **CONFIRMED.** SI Data qualified; not checked numerically |
| P7 | The 1.3–1.7 two-year/one-year ratio does not identify passing swings (√2 under diffusion) | algebra | **CONFIRMED** (= G30) |
| P8 | Capture sensitivity: the head/rest odds of a recorded submission fell to 0.33 of 2019, so relative detection would have to fall by about two thirds to explain the item-share change | 0.087/0.913 ÷ 0.224/0.776 = 0.330 | **CONFIRMED** arithmetic; in the main text as a sensitivity quantity. **The capture audit itself is Tier 3** |
| P9 | Gaussian primitives need not give a symmetric observed asymmetry after rank drift, boundaries, exits and selection | R12 worlds give −0.04 to −0.10 | **CONFIRMED** (= G15); the text now uses the simulated reversal |
| P10 | Riser/faller figures: main 0.146/0.155 against the SI assessment's +0.07 | two constructions | **RESOLVED** as a definition: the true-sign split pools origins with one cohort, while the +0.07 uses 13-week origins within whole-window strata, as in the worlds. SI footnote added |
| P11 | Uses: agreement with the benchmark does not show that the design is irrelevant; internal concentration is not market power; "can respond" should read as a conditional model response | — | **ADOPTED** in the Discussion and SI uses |
| P12 | Define "homogeneous" (no persistent identity-specific component; rank- and size-dependence allowed) | — | **ADOPTED** (Introduction) |
| P13 | Reorganize Results 2; the Discussion's second paragraph reads like a response letter | — | Partly done (paragraphs split). A full restructure is not done |

**References.** Hagar & Shaw 2022 (J. Commun. 72(6):675–686), Ichiba–Pal–Shkolnikov 2013 (PTRF 156:415–448), TeBlunthuis 2025 (ICWSM 19:1880–1892) and Fernholz & Koch 2021 (EER 135:103723) were verified and added. Luttmer 2011 and Fernholz 2017 are now cited in the main text. Guvenen et al. 2021 is engaged on asymmetric earnings persistence.

**Tier 3 additions** (owner's call, not started):
- the durability comparison rebuilt in the calibrated curved model with entry and exit, with joint uncertainty in variance and decline (GPT-6 Pro's rank 1);
- the capture audit by date and rank, with alternative rosters and thresholds (rank 2);
- the reversible decline-state model (R14), with the identification constraint that its covariance equals an OU growth component's (rank 3). Sign-conditioned, recovery and rank-stratum outcomes must therefore stay outside calibration.

**What D1–D4 settle of rank 1.** D4 computes the factor from measured crossings, without the variance estimator. It gives 2 gross and 1 net of C at the comments head, and 1–2 at the submissions head. D1 shows the observed path against the rate law and the stable-parameter model. The calibrated-model rebuild with propagated uncertainty is not done.
