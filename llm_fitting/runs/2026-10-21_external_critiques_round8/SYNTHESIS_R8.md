# Referee round 8 on manuscript v7 — synthesis

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-21. Manuscript v7 at commit 1652645; brief `CRITIQUE_PROMPT_R8.md`; record §2z-dr. Every finding marked CONFIRMED was rechecked here against the sources or the saved outputs. PLANNING ONLY: no manuscript change is made in this file._

## 1. Reports and verdicts

The reports are saved verbatim in this folder. All three reviewers worked read-only; `git status` showed no tracked change after any of them.

| reviewer | how run | verdict |
|---|---|---|
| gpt-6-astra | Codex CLI 0.163.0, read-only | send out, without enthusiasm |
| Claude Opus 5.5 (fresh) | read-only sub-agent | send out, without enthusiasm |
| Claude Fable 5.1 | read-only sub-agent | send out, closer to enthusiasm than v5, not yet with it |

**What all three credit:**
- R16 withdrew two claims the evidence did not sustain (R12's "at most three fifths"; the separation of time scales);
- the registration record;
- R16-C (the objective calibrated under correct specification);
- R16-D (recovery with the full estimator);
- the capture audit's design;
- the disclosed failure of R16-F's registered fit.

**All three agree that:**
- the asymmetry is the paper's best claim;
- it is not yet presented as one banded statistic;
- in its world-comparable form it is "declines persist about twice as strongly as gains", not "gains do not persist".

## 2. Verified findings

### A. Errors in v7 (fix before anything else)

| # | finding | by | check | status |
|---|---|---|---|---|
| H1 | **Decline-state persistence.** "a fifth of the persistence or less" (main l.205; SI) conflicts with Table 1. In the paper's statistic it is 43% and 32% (0.075/0.174, 0.049/0.154); in the calibration statistic 5% and 26% | all three (and found here before the reviews) | `r16_world__*__decline_pi0.25*` | **CONFIRMED** |
| H2 | **Abstract.** "make gains persist as much as declines". In the joint world gains persist *more*: risers +0.110 against fallers +0.037 (submissions), and +0.19 after a large rise against −0.11 after a large fall | all three | `r16_world__*__joint_*`, r16_aux asym | **CONFIRMED** |
| H3 | **Fallers against risers.** "Among those that rose 0.00 to +0.03" (l.202) is the true-sign split, never computed in a world. In the world-comparable split the data's risers are +0.069 and +0.042, against the matched control's −0.043 and −0.038; fallers are +0.141 and +0.113 against −0.095 and −0.083. Relative to homogeneous growth, gains persist by +0.11 and +0.08 and declines by +0.24 and +0.20 | all three | `r16_moments__*.json`, `r12_world__*__control_num__*` | **CONFIRMED.** Title-level wording "declines persist and gains do not" becomes "declines persist about twice as strongly as gains" |
| H4 | **The multiplier.** The "lasting-variance multiplier 0.7–1.4" (main l.155; SI time scales) scales σ0, so the variance is scaled by 0.49–1.96 | gpt | `r12_hetgrowth.het_params` (`sigma0=p.sigma0 * m`) | **CONFIRMED** (also R12's "m" wording throughout) |
| H5 | RESULTS R16-F3: "less than ±12% under either fit". Comments' lag-aware ratios are 1.00, 0.80 and 0.69, i.e. +25% and −14% | gpt | `r16_durability_scores.json` | **CONFIRMED.** F3 still passes (±30%) |
| H6 | **Capture placebo bug.** Mid-month dates inherit the first-of-month eligibility: 15 Dec 2020 and 15 Dec 2021 cross roster boundaries, against PLAN R16-E | gpt | `r16_capture.py` l.104–110 | **CONFIRMED.** Fix and rerun (cheap); gpt's recomputation leaves the ratios 0.69–1.09 |
| H7 | **Commented-out text, absent from the PDFs:** the SI variance caveat (appended to a `%%` line, SI l.162), and main l.152 ("Part of the slide is mechanical …"), to which the Discussion still refers | gpt, Opus | sources | **CONFIRMED** |
| H8 | SI l.438: "a post hoc extension shows it is the minimum" contradicts SI l.369 (G26) | all three | SI | **CONFIRMED** |
| H9 | **Table 1.** (a) The caption says 20 runs; the decline column has 10. (b) "the calibration statistic" for fallers − risers, which is untargeted. (c) Crossings/slide: the data entry 0.64 is c/(s̄ − e) pooled, the worlds' entries are c/s̄. (d) "Every adequate world reproduces their range" is false for the joint world's β (0.11) and the decline state's 1.07 | gpt, Opus | `r12_table.py`; law cells | **CONFIRMED.** (c) needs the data's value computed by the worlds' code (one light job) |
| H10 | **Joint edges.** On submissions all four parameters are at grid edges (s 0.8 max, head 0.25 min, km 0.5 min, m 0.8 min). The SI names two | Opus, Fable | `r16_joint_grid__*.json` | **CONFIRMED** |
| H11 | "rising down the ladder as the data's does" is false on comments (joint −0.00, +0.14, +0.06 against +0.11, +0.14, +0.15) | Opus, Fable | SI table | **CONFIRMED** |
| H12 | **The lag-aware fit was run inline;** no committed script produces `r16_durability_scores.json` | all three | repository | **CONFIRMED.** Deposit `r16_durability_fit.py` |
| H13 | **Durability wording.** "Crossings offset about three fifths" is the observed shape, which includes the swing's lag, not a decomposition. "After about a year" and "3–4 years" are m 1.0 values; across m τ is 2.8–4.5 | all three | `r16_durability__*.json` | **CONFIRMED** |
| H14 | **Main l.193.** "+0.07 to +0.14" for comparisons ending before July 2021 mixes membership rules. With full membership it is +0.07 to +0.11 | Fable | SI persistence table | **CONFIRMED** |
| H15 | **Discussion l.237.** "lose about two thirds more of the head's submissions" misstates P8: relative capture would have had to fall to a third of its 2019 value | Fable | arithmetic | **CONFIRMED** |
| H16 | **Smaller slips:**<br>• SI duplicated clause (l.491);<br>• SI comma splice (l.271);<br>• SI "within-year change of −0.93" (it is the sum of four within-block changes);<br>• SI "six referee rounds";<br>• SI cohort statement without its exceptions (l.29);<br>• SI "What the forms do" not marked as superseded;<br>• stale `.tex` headers ("DRAFT v5", "R12 pending");<br>• the `init_share` 0.062 needs a note (only universe members carry history-set states);<br>• §2z-dq's "55–62%" (the paper's 55–65% is right) | Opus, Fable, gpt | sources | **CONFIRMED** |
| H17 | **Overstated exclusions.** "It does not reflect exposure to platform-wide trends" (Discussion) overstates what shrunk loadings can exclude | gpt | R15 D3 | **CONFIRMED** (wording) |
| H18 | Fig. 1A draws the 2020–22 balance-law means on the 2019 ladder; the caption implies 2019 | Opus | `make_v5_figures.py` `_law` | **CONFIRMED** (caption) |

### B. Claims stated above their evidence

| # | finding | by | status |
|---|---|---|---|
| K1 | **"Fits on submissions"** rests on the 9-moment score (8.0) judged against R16-C's distribution, which was built for another estimator. The 24-moment objective (38.8) has no correct-specification calibration | all three | **CONFIRMED.** Restate as "reproduces most of the persistence at its grid edge; the full objective is not calibrated" |
| K2 | **The decline state is effectively a skewed permanent type.** With π_D 0.25 and half-life 208 the mean spell is about 400 weeks (7.7 years). It is at the grid's edge. Its sign comes at 1.4–2.4 times the data's lasting variance (sf world 0.0072–0.019 against 0.0039–0.0138). On comments its within-stratum covariances are negative in every stratum, so its pooled persistence is a between-stratum effect | all three (variance and strata: Fable) | **CONFIRMED** (rechecked from `r16_decline_grid__*.json`) |
| K3 | **R16-F's τ is weakly identified.** The response is unsaturated at year 5, the fit depends on the assumed lag (Opus: a lag 1.5× the median gives τ 9.4 years on submissions), and the SSE profile is flat above about 4 years. "0.7–1.0" is too precise; "no evidence for a large separation" is what holds. F3 could hardly fail, because both times scale as 1/σ². F2's 2.6 used the data's S*; with the model's own S* = S the linearized factor is about 8 | all three | **CONFIRMED** in reading. Opus's lag sensitivities were recomputed by the reviewer from saved block means, not checked here |
| K4 | **The capture audit.** It excludes level shifts at file boundaries, including July 2021. Step sums do not partition the decline. The within-file gradient is post hoc and shows only that 1% steps are visible. Smooth drift, delayed removals and score maturation are untested. Two mid-month steps, 15 February 2021 (−0.243) and 15 May 2021 (−0.220), carry about half the submissions head's item-share fall and are unnamed | all three (events: Opus) | **CONFIRMED** (steps in `r16_capture_daily__*.json`). Naming the communities is one light job |
| K5 | **R16-G cannot answer whether the two forms combine.** It adds variance to a fit already at its edge; G1's failure is near-mechanical. "Do not combine" becomes "this addition, at the joint optimum with its spread held fixed, did not change the asymmetry" | all three | **CONFIRMED** |
| K6 | **A1's false-positive arm tested a faster swing (26 weeks).** R16-B's result calls for a slower one (km 0.5) with no heterogeneity, calibrated by the joint estimator | Opus, Fable | **CONFIRMED** (design gap) |
| K7 | **"Changes far less" is measure-dependent.** It compares a mean-log span with a membership share, while the arithmetic share fell 16 points | all three | **CONFIRMED** (wording; put the measure in the heading) |
| K8 | **The 2019–20 comments fall.** In the model's terms it is not a lasting-decline shift: a 0.2 a year step gives −0.09 in year 1, so −0.46 would need about 1.0 a year, and the slide then fell. Surge × rank exposure is the obvious rival | Opus | **PLAUSIBLE** (arithmetic from saved responses; not rechecked against an alternative) |
| K9 | **Novelty of the asymmetry.** Asymmetric, position-dependent persistence is known in earnings (Guvenen et al. 2021; Arellano, Blundell & Bonhomme 2017), credit ratings (Lando & Skødeberg 2002), equity momentum in losers (Hong, Lim & Stein 2000) and the firms' "shadow of death" (Griliches & Regev 1995). The contribution is its measurement in attention against a calibrated random-growth null, with symmetric and type-switching forms both failing | all three | **Accepted** (framing; references to verify before adding) |
| K10 | **The asymmetry is a body phenomenon on submissions:** the top K/16 has no persistence (−0.003). The persistent decliners cannot be the submissions head's fall; say so | Opus, Fable | **CONFIRMED** (`r16_moments__*.json` srho) |

### C. Framing and structure (judgments)

- **Lead.**
  - Opus: lead with the decline law, quantified as the share of the variance in shape change carried by the decline term across the 60 cells, then the asymmetry.
  - Fable: two-flow account, then the asymmetry in the middle of the abstract.
  - gpt: asymmetry earlier, balance as an accounting framework.
- **Title** (owner item):
  - "Random growth and persistent decline in the hierarchy of online attention" (Fable, Opus);
  - "Persistent decline and changing leadership in online attention" (gpt).
- **An asymmetry figure** (all three): the next year's change by decile of this year's change, stratum-demeaned, by stratum, with bootstrap bands, for the data, the matched control, the joint world and the decline world. It replaces Fig. 4C or 4D.
- **Results 2** should be split (balance and observed change; geometry; a short model-response paragraph), with the linearized factors moved to the SI. Fig. 2A's linearized line should be relabelled or dropped.
- **Length.** Fable assumes a six-page limit. PNAS's own page rules are to be checked: they are an owner item and not verified here.
- **The SI** is ordered as a laboratory log. Reorder it by claim and estimand, and replace the registration paragraph with a table.
- **References still uncited** from round 7: Gleeson 2014, Blumm 2012, Ratkiewicz 2010, van de Rijt 2014, Kairam 2012, Houssard 2023.

## 3. Ranked revision plan

**Tier 1: corrections and restatements with no new analysis.**
- **Errors:** H1–H5, H7, H8, H9 (a, b, d), H10, H11, H13–H18.
- **Restated claims:**
  - K1, K2, K5, K7, K10;
  - K3's wording ("no evidence for a large separation"; τ with its lag dependence);
  - K4's wording;
  - the asymmetry restated as "about twice as strongly" throughout.
- **Code and records:**
  - deposit the lag-aware fitter (H12);
  - fix the capture placebo filter and rerun the daily audit (H6; 20 seconds a ladder);
  - correct the record's 55–62%.

**Tier 2: cheap measurements on development data** (light jobs, one at a time under the watchdog):
- **The asymmetry made a result:**
  - one statistic;
  - bootstrap bands over communities within strata and over origins;
  - by stratum;
  - raw and demeaned splits;
  - before July 2021 and after the 2020 surge;
  - without terminal collapses;
  - exits kept at the floor;
  - the decile impulse-response curves for the data and the saved worlds (worlds may need tails saved by decile, at 10–20 seeds);
  - the opposite-platform-trend argument.
- **Table 1's crossings/slide for the data** by the worlds' code (H9c).
- **The two 2021 mid-month events:** name the head communities behind them, and the head's fall without them.
- **R16-F extended:**
  - the lasting-level response (known in simulation);
  - 520 weeks;
  - a free lag;
  - a uniform step in the slide at every rank;
  - the K/16 response.

**Tier 3: heavier new analyses** (owner's call):
- **The joint grid extended past its edges:** km 0.25 and 0.35; head 0 and 0.1; s 1.0; m 0.6. Plus a correct-specification calibration of the 24-moment objective, and the slow-swing false-positive world (K6).
- **R16-G redone** with the spreads traded (s against sd) and km free, the asymmetry untargeted.
- **R16-F in the joint world.**
- **A combined calibration** with every spread free, scored on untargeted asymmetry statistics.

**Tier 4: owner items:**
- the title and classification;
- PNAS's page limit;
- the DOI, acquisition source and date, Page IDs, acknowledgements, the chapter's pages;
- the 2023–24 protocol: add the decline law and the null-relative asymmetry as primaries, amend for the source switch and the blackout, and adopt it before the bridge is opened;
- the raw-file capture audit (WD drive).
