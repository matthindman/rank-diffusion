# Round-2 referee synthesis (three models), with the load-bearing claims checked

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18. Inputs:
- the brief, `CRITIQUE_PROMPT_R2.md` (committed 9266f50 before the reviews);
- three reports, saved verbatim in this folder:
  - gpt-6-astra, through the Codex CLI;
  - Claude Fable 5.1, as a subagent;
  - a fresh Claude Opus 5.5 session, as a subagent.

Each reviewer worked read-only, without data files or sealed paths. A claim
below is CONFIRMED only after this session rechecked it in code or saved
outputs (the commands are noted). Record: MODEL_STATUS §2z-df._

## 1. Verdicts

All three say the paper is **closer to publishable than in round 1**, and
an editor would now send it out. They name different largest gaps, which
fit into one picture:

| reviewer | largest gap |
|---|---|
| gpt-6-astra | No validated link from individual dynamics to actual (arithmetic) concentration that survives the measurement procedure, matched populations and capable rivals. Momentum may partly be an averaging artefact |
| Fable 5.1 | The model is validated at 1–13 weeks, where it nearly is model A, and fails at 52 weeks, where every headline claim lives. Next: the submissions exponent rests on one stratum, and the submissions gate win is on a 30-week panel |
| Opus 5.5 | Momentum: the most novel fact and the model's main failure, with its mechanism unknown. "Four partial claims, none both new and nailed down" |

**The common picture.** The new model is tested out of sample only where it
nearly coincides with A. Its new part, the wandering home held by drift by
rank, is tested only in sample. There it misses the annual-scale Reddit
facts the paper leads with. And the leading candidate for those misses,
momentum, is not yet shown to exceed what block-averaging alone produces.

## 2. Verified findings

| # | claim | raised by | check | status |
|---|---|---|---|---|
| V1 | The submissions size–variance exponent is fragile: dropping the top 78 in year 1 gives β = 0.19 | Fable, Opus | Leave-one-stratum-out refit of the printed table (`size_variance_output.txt`) | **CONFIRMED, and wider:** submissions year 1, leave-one-out range **0.00–0.19** (dropping ranks 79–156 gives 0.00); year 2 gives 0.082. Comments 0.067–0.081 and Facebook −0.001 to 0.015 are robust. Fold change, smallest stratum to largest: comments 1.58, Facebook 1.03, submissions not monotone |
| V2 | `share_K64_last` is the geometric-mean index of §2z-cw, not an arithmetic share, yet §2z-dd S4 and frame c call it arithmetic | gpt, Fable | `structure_suite.py:107–110` and `velocity_transport.py:77–78` exponentiate block-mean log levels | **CONFIRMED.** "The measured slides reproduce half of the fall" (frame c, Results 6) compares an index with the arithmetic 50.2% → 34.0%. Withdrawn until computed on arithmetic shares |
| V3 | The structure suite compares different populations: simulations drop reborn slots, while the data keep later entrants | gpt | `structure_suite.py:130–134` | **CONFIRMED in code;** size of the effect unknown. It affects retention and the head index, not the momentum or no-return statistics (those use block-0 members present in all three blocks) |
| V4 | "σ_perm² reduced by σ_L² so short-lag increments are unchanged" is approximate | gpt, Fable, Opus | the `movinghome` branch of `_State.step` in `rankdiff_rdm.py`: the home innovation enters L through κ | **CONFIRMED.** One-week innovation variance is σ_perm² − (1−κ²)σ_L² while the floor is inactive. The gate scores stand as scores of the implemented model |
| V5 | Comments: the 90th percentile of 13-week displacement is under-dispersed in every split | Fable | `em`/`sm` of `gate__reddit_comments.json` | **CONFIRMED.** Ratios 0.62 / 0.84 / 0.81 / 0.84 / 0.84; Facebook 0.23–1.30, under in 4 of 5. Whether A shares it awaits G2 (comments run in progress) |
| V6 | The submissions gate win is on a 30-week panel: train windows 12–17 weeks, no 13-week moment, below the 160 weeks recovery requires; the long panels were not gated | all three | `gate__reddit.json`; the `em` keys lack dRank13 | **CONFIRMED.** Paired by split, the RDM beats A on 5/5 (A in the same harness, G2 run: 0.128 ± 0.028, reproducing Codex's 0.129). It is a short-horizon result |
| V7 | The too-concentrated head was blamed on momentum, yet adding velocity raised the head index | Fable, Opus | `velocity_calibration_output.txt`; `structure__…__vel.json` | **CONFIRMED.** The index was 0.466–0.561 across the grid and 0.572 at the chosen point, against 0.515 without velocity; measured slides give 0.436. The attribution in §2z-dd and frame c is withdrawn: the cause is the hold drift's stationarity, by construction |
| V8 | κ_m and V_m are estimated and printed but unused by the frozen form; the dossier's parameter count describes the pre-freeze variant | Fable, Opus | `step()` uses A's κ(z) and σ_perm(z) knots | **CONFIRMED.** The frozen model is A plus σ_0, β and the drift closure, so strictly more complex than A |
| V9 | The RDM's own boundary ratios are structured by cut-off | Opus | `boundary_sim_output_submissions.txt` | **CONFIRMED.** At k = 625–1,250 the ratio is 0.68–0.97 in every seed; at 2,500–5,000 it is 1.16–1.30 in every seed. A's simulations scatter around 1 without that pattern. The by-rank test needs a model-derived null. A lead on submissions' 300–600 shortfall |
| V10 | Recovery used variant "move" with zero slides (the hold closure untested); six replications at 86 and 160 weeks only | Fable, gpt | `recovery.py:41`; `recovery_output.txt` | **CONFIRMED.** "Recovered at 160 weeks or more" becomes "at 160 weeks, six replications, self-consistency only" |
| V11 | The author-hosted later version of Gomez applies the decomposition across all income percentiles, for a flow with transitory shocks, after three-year averaging (Appendix F, Fig. F.1) | gpt | Read pp. 50–54 of `matthieugomez.com/files/topshares.pdf` ("Decomposing the growth of top wealth shares", submitted to Econometrica) | **CONFIRMED.** Fig. F.1a gives the within, between and demography terms at every top percentile, 1979–1990, IRS panel, 5,523 taxpayers; F.1b gives the model ingredients g·q²/(p·w) and ν² by percentile. So "whole ladders, not one top set" and "a flow dominated by transitory variation" are not novelty claims. What remains ours: a rank-by-rank test of measured displacement against the implied term, with bands, over thousands of ranks on four ladders; the permanent-rank cohort estimator; the attention application; the dynamics |
| V12 | Block-averaging a random walk alone makes consecutive changes correlate (+0.25 Pearson, +0.24 Spearman at B = 52; Working 1960), so zero is the wrong null for momentum | gpt | (B²−1)/(2(2B²+1)) = 0.2499; Spearman 0.2392 | **Formula CONFIRMED.** The model comparisons of §2z-dd already pass through identical averaging (RDM −0.01 comments, about +0.04 submissions), but D1's +0.10 threshold does not. Whether Reddit momentum survives a gapped test: §3 below |
| V13 | All pre-July-2021 triples include 2020 H1 (COVID onset) | Opus | `momentum/output.txt` | **CONFIRMED.** D1 excludes the July 2021 capture change, not a 2020 episode. The earliest triple (2019 H1 → 2020 H1) is the lowest: +0.199 and +0.126 |
| V14 | D2 measures first appearance in the panel, not age | all three | `momentum_diagnostics.py:81` | **CONFIRMED.** "Not a young-community lifecycle" becomes "not a lifecycle of communities first seen during the panel" |
| V15 | The Facebook momentum contrast is not like for like (13-week blocks, Dec 2020–Jun 2022, against Reddit's 26- and 52-week blocks from 2019) | all three | `momentum/output.txt` | **CONFIRMED.** Reddit at Facebook's dates and blocks not yet run |
| V16 | The neighbour-swap rival was not calibrated to 0.80 retention | gpt | `rivals_calibrated_output.txt`: 0.993 | **CONFIRMED** (its parameter is undefined; it has no turnover lever). Its ratio range 0.35–1.70 is a seed range |
| V17 | "16–21% of each Reddit top set is replaced each year" is comments-only | Fable | §2z-cu table: comments 0.79–0.84 and submissions 0.79–0.82 | **PARTLY.** §2z-cu supports it on both ladders. The structure suite's 0.83–0.87 on submissions uses a different window. Label the definition |
| V18 | Outline v2 is internally inconsistent (intro preview, figure list, M&M 5, β range from two estimators, Results 5 `\todo`) | Opus | read on revision | accepted; fix with the next outline revision |
| V19 | "Reproduces Facebook's absence of reversion" is in sample, and −0.076 is itself mild reversion | Opus | §2z-dd (in-sample, labelled) | **CONFIRMED** |
| V20 | In this harness A's home is the filtered end-of-train state; the train-mean home was never compared; no ablation separates the random-walk home from the drift by rank | Opus | `rk.sim_cohort_conditional`, `cond_home="state"` | **CONFIRMED** as a description of the harness |

**Not checked:**
- gpt's synthetic aggregation run (20 seeds, Spearman 0.2375). It agrees
  with the formula.
- Opus's paired t-statistics.
- The literature items listed below.

## 3. The momentum objection, tested

gpt-6-astra's point (V12) could have removed the paper's newest fact. So it
was tested before this synthesis. The design and expectations were committed
at 5de56b7 before the run (`runs/2026-10-18_momentum/DESIGN.md`, addendum
R2; `gapped_momentum.py`; `gapped_output.txt`).

**The test.** Correlate two one-year changes of 13-week window means whose
increment supports do not overlap. The second change starts g weeks after
the first ends. The cohort is the top K by mean level over weeks 0–25, a
window disjoint from every measurement window.

**Why it decides the question.** For any process with independent
increments, with volatility varying by community, plus white noise, the
expected correlation is zero. A short-memory swing pushes it negative. Only
persistence longer than the gap makes it positive.

| | gap 0 | gap 13 wk | gap 26 wk | adjacent 52-wk blocks (same cohort) | expectation |
|---|---|---|---|---|---|
| synthetic (i): random walk + noise + OU | −0.032 | −0.009 | −0.005 | +0.160 | I1: \|ρ\| ≤ 0.03. **Holds at 13 and 26**; at gap 0 it is −0.032, just outside, the sign the OU swing predicts |
| synthetic (ii): (i) + drift sd 0.4 a year | +0.171 | +0.190 | +0.194 | +0.389 | I2: ≥ +0.10. **PASS** |
| Reddit submissions (K 5,000) | **+0.201** | **+0.174** | **+0.140** | +0.396 | G1: ≥ +0.10 at 13 and 26. **PASS** |
| Reddit comments (K 12,500) | **+0.163** | **+0.154** | **+0.141** | +0.318 | G1: **PASS** |
| Facebook Era A (26-week changes; gaps 0/4/8) | −0.114 | −0.067 | −0.031 | −0.069, −0.139, −0.043 | F: ≤ +0.05. **PASS** |

Values are means over origins; per-origin values are in the output file,
with every origin positive on both Reddit ladders.

**Reading.**
- The momentum is not an averaging artefact. With disjoint supports and a
  half-year gap it is +0.14 on both Reddit ladders. In a world without
  persistence, the same code gives −0.005.
- The persistence decays with the gap on submissions (0.20 → 0.14) and
  barely on comments (0.16 → 0.14). Later origins are lower on submissions
  (+0.112 at week 52, gap 26), consistent with the timing profile of D1.
  This does not yet separate a velocity with a finite half-life, a 2020–21
  episode, and drift by community.
- Facebook goes from negative toward zero as the gap grows. That is
  short-memory reversion and no persistence. It is still not like for like
  (V15).
- So zero is the wrong null for the adjacent-block statistic (D1's +0.10
  threshold is withdrawn), but the regularity stands.
- The claim becomes: "changes in a community's activity a year apart remain
  positively correlated (Spearman +0.14 to +0.20) when the two changes share
  no weeks and are half a year apart; Facebook pages show none."

## 4. Where the reviewers agree, and what to do

### What all three agree on

1. **Momentum** is the most interesting new fact and the model's main gap.
   Report it as a Results section; do not lead with it yet. (§3 now answers
   the aggregation objection.) It must be decomposed before any mechanism is
   claimed. The candidate mechanisms:
   - Fable: a stock of participants that integrates net joins;
   - Opus: shared non-stationary trends from the 2020–21 reallocation;
   - gpt-6-astra: participation trends, community-specific or shared.
2. **The model is a candidate, not a validated explanation.** The gate tests
   it where it nearly coincides with A, and its new part is tested only in
   sample. The fair presentation is "A with a wandering home", with a list
   of its misses. The abstract's model sentence and "as well as or better"
   go.
3. **The headline is the right kind of lead but over-reads its numbers.**
   Quantify "barely" (fold changes), lead with comments and Facebook, show
   the submissions instability, and cite the fluctuation-scaling precedent.
   Each reviewer proposes a different title:
   - gpt-6-astra: "Size offers limited stability in online engagement";
   - Fable: "Leaders of online attention are nearly as volatile as
     newcomers";
   - Opus: "Attention does not diversify: online leaders are displaced at
     the same rate at every scale".
4. **Errors**, all confirmed (V2, V4, V7, V8, V10, V13–V16, V19):
   - the share label;
   - the head-share attribution;
   - the short-lag claim;
   - the parameter count;
   - the recovery wording;
   - D1/D2 wording;
   - the Facebook contrast;
   - the neighbour-swap calibration;
   - the in-sample label.

### Ranked next steps with data on hand (consensus order)

1. **Decompose the momentum** (all three):
   - items against engagement per item, split as in `portfolio.py`;
   - residualising on the platform aggregate and on co-movement clusters
     from a training prefix;
   - risers against fallers (sign asymmetry);
   - lag-2 and lag-3 profiles, and D(h) to 156 weeks;
   - Reddit at Facebook's dates and block lengths.

   This decides between name-level trends, shared trends and a 2020
   episode, and so what the model needs.
2. **Gate both models on the long panels** (`reddit_submissions_long`,
   `reddit_comments_ext`) at 20 reps, with:
   - paired splits and paired uncertainty;
   - the 90th percentiles reported;
   - two ablations: a random-walk home without drift by rank, and A's
     fixed home plus drift by rank;
   - cross-model recovery: A simulated with slow κ and fitted by the RDM,
     and the reverse.
3. **Repair the structure harness**, then run an out-of-period structure
   test (estimate on 2019–20, forecast 2021–22) for A, the RDM and a
   trend-carrying variant. The repair:
   - entrants as identities;
   - arithmetic shares from activity, not exponentiated block-mean logs;
   - matched populations.

   It would be the only out-of-sample structural result without sealed
   data.
4. **Make the headline robust:**
   - leave-one-stratum-out β in both years, on all ladders;
   - strata down to the whole tracked universe, with absences as collapses;
   - a noise-corrected β on Facebook;
   - β from item counts only;
   - retention of the top 10 and top 25;
   - fold changes alongside exponents.
5. **Tie the spine to concentration and calibrate the by-rank test:**
   - decompose the arithmetic 50 → 34% fall in Gomez's terms on
     permanent-rank cohorts;
   - a joint block-over-time bootstrap for the pooled ratios;
   - a direct comparison with Gomez's Appendix F;
   - the boundary estimator on RDM simulations with β = 0, to see whether
     the pattern by cut-off (V9) and the 300–600 shortfall are estimator
     effects;
   - fitted level rivals (two-type GLLM, Yule–Simon).

### What needs the owner

- **Paper decisions:** the title and claim language; whether the RDM stays
  in Results 4 or moves to the SI.
- **Bridge protocol v3,** before adoption. All three reviewers ask that
  momentum (half-year momentum ≥ 0.15 in 2023–24 if persistent, about 0 if
  a 2020 episode), β and the RDM gate become pre-registered primaries.
- **Names, for interpretation:** topic clusters and the 2020 ban wave.
- **The WD drive:**
  - the capture-delay audit;
  - per-item tail indices;
  - subreddit creation dates (a real lifecycle test).
- **Wikipedia (sealed):** views rather than karma, over a longer span.

### Literature to add

- de Menezes & Barabási 2004 (*PRL* 92:028701);
- Eisler, Bartos & Kertész 2008 (*Adv. Phys.*);
- Coad 2007 (serial growth correlation of firms);
- Gomez, the later version's Appendix F;
- Guvenen 2009 and Gabaix–Lasry–Lions–Moll 2016 (type dependence);
- Kairam, Wang & Leskovec 2012 and Cunha et al. 2019 (online group growth);
- Working 1960, for the averaging null;
- Salganik et al. 2006 and van de Rijt et al. 2014 (cumulative advantage).
