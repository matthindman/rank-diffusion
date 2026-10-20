# The PNAS push: plan and expectations, written before the runs

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18. The owner's decisions
are verbatim in the §6 addendum of MODEL_STATUS (record §2z-dg):
- the title states the foundational issue: how churn creates and maintains a
  (stable-ish) structure;
- the core of the model stays in the main text;
- momentum, β and the model gate are added to bridge protocol v3;
- do everything the paper needs.

Development data only. Nothing here opens a sealed folder._

## The paper's argument under the new title

**Puzzle.** About a fifth of every Reddit top set is replaced each year, at
every scale (§2z-cu), yet the rank-size hierarchy changes slowly.

**Resolution: churn is not opposed to the structure; it sustains it.** Three
measured pieces, plus a model.

1. **Churn is nearly scale-free.** Size does not protect: β 0.01–0.08
   (§2z-dc). So turnover is nearly scale-free. A kinematic identity makes
   that quantitative (A1): the replaced fraction at cut-off k is about
   E[downward move of names at the boundary] / S_k, and both terms vary
   little with k.
2. **Every name loses ground against the ladder** (the slide, s̄_k).
   **Churn returns names across each boundary at σ_k²/(2S_k)** (the boundary
   balance, pooled 0.96). The shape changes at the difference (the rate law,
   §2z-cd/§2z-cg). Where the two balance, the hierarchy holds (comments; the
   submissions body). Where the slide wins, it flattens at the predicted rate
   (the submissions head: 50% → 34%).
3. **The stationary slope is S_k* = σ_k²/(2 s̄_k).** Churn sets the
   hierarchy's steepness and the slide bounds it (Fernholz's gap identity;
   Gabaix; Gomez). Without churn a sliding ladder collapses; without the
   slide it spreads without limit (A3).
4. **The generative model's core, in the main text:**
   - lasting levels that wander with nearly scale-free volatility;
   - drift by rank;
   - fast passing swings, which make weekly ranks jitter without changing
     the structure.

   It forecasts held-out movement (gates, A5) and holds the ladder. It
   misses momentum, which is the "-ish": persistent growth (the gapped
   statistic, §2z-df) tips the balance.

**Positioning.** The mathematics is Fernholz/Gabaix/Gomez (theory note §3,
§9). What is ours:
- the paradox resolved by measurement, rank by rank, on named units, with
  heavy passing movement;
- the crossing term predicted from churn and tested;
- the scale-free churn behind scale-free turnover;
- the dynamics, and where they fail.

## Analyses, in order of execution

### A5. Long-panel gates and the 2×2 ablation (background, heavy)

Gate v1 at 20 reps and 2,000 draws on `reddit_submissions_long` (K 5,000)
and `reddit_comments_ext` (K 12,500). The four arms run in one harness
(`rankdiff_rdm.oos_rdm`):

| arm | home random walk | drift by rank |
|---|---|---|
| A (`home`) | no | no |
| RDM (`movinghome`) | yes | yes |
| (a) `movinghome_nodrift` | yes | no |
| (b) `homedrift` | no | yes (the same estimated drift, applied to the fixed home) |

The flags are A's for each ladder: temper, min-knot-n 8, md-lags 6, t-tails,
conditional state, plus mix-hetero on comments (GATE_LEDGER rows 79–84).

**Expectations:**
- **A5-1:** the RDM is non-inferior to A (≤ A + 0.05) on both long panels.
- **A5-2:** the RDM's dRank13 error is below A's on both (G2 on the long
  panels).
- **A5-3:** the four arms are within 0.03 of each other on both panels. The
  gate cannot identify the lasting process at its horizons; the structure
  tests (A6) must.

Prior: A5-1 75%; A5-2 70%; A5-3 60%.

### A1. Scale-free turnover from scale-free churn (kinematics)

**Data.** Block pairs of 52 weeks on both Reddit ladders and 26 weeks on
Facebook Era A. Cut-offs from K/64 to K/2; presence ≥ 0.9.

**Observed:** the replaced fraction of the top k.

**Predicted:**
- v_k + (1 − v_k) · E[(Δθ_k − Δ_i)⁺] / S_k, where:
  - v_k is the share of the top k absent at the next block (reported
    separately);
  - Δ_i is the change of names ranked (k/2, k] that are present at both
    blocks;
  - Δθ_k is the change of the rank-k level;
  - S_k is the local slope at the start block, from ranks k/√2 to k√2.
- It assumes locally linear density and changes that do not depend on the
  distance to the boundary within the band. It is kinematics, not a
  mechanism test: it explains why turnover is scale-free.

**Expectations:**
- **A1-1:** predicted/observed is within [0.8, 1.25] in at least 80% of
  cells.
- **A1-2:** across cut-offs, the coefficient of variation of the predicted
  fraction is < 0.20 on each Reddit ladder.
- **A1-3:** the two ingredients vary little with k: the sd of the boundary
  change has a CV < 0.25, and so does S_k.

Prior: 65%, 70%, 60%.

### A3. Churn creates and maintains: the counterfactual

**Method.** The lasting core of the moving-home model alone (`step_lasting`,
no passing parts), N = 4K:
- σ_L(z) estimated on the whole panel;
- drift by rank from B's measured slides (the "move" variant of §2z-dd), not
  the hold closure, so the ladder is not an input;
- starting from the observed end-of-block-0 ladder;
- 104 weeks, 20 seeds, on comments and submissions.

**Four worlds:**
- (i) churn + slide;
- (ii) slide without churn (σ_L = 0);
- (iii) churn without slide (drift 0);
- (iv) churn + slide from a flat start (all levels equal), run for 30 years.

**Expectations:**
- **A3-1:** in (i), the rate of shape change at K/64 … K/4 matches c_k − s̄_k
  from the simulation's own σ and slide within 25%.
- **A3-2:** in (ii), shape_k at K/64 falls by at least 0.15 log units a year
  on both ladders.
- **A3-3:** in (iii), shape_k at K/64 rises by at least 0.10 a year.
- **A3-4:** in (iv), a hierarchy forms. After 30 years the local slope at
  K/16 … K/2 is within 25% of σ_k²/(2 s̄_k) evaluated in the simulation.

Prior: 70%, 80%, 80%, 60%.

### A7. The headline exponent, made robust

Each item and its expectation:

| | item | expectation |
|---|---|---|
| A7-1 | leave-one-stratum-out β, both years, all ladders | comments within 0.03–0.10 in every fold and year |
| A7-2 | β down to rank 4K, absences counted as a fall to the universe floor | every ladder < 0.12 |
| A7-3 | Facebook lasting β: 26-week-block variance minus a scaled 13-week-block variance, by stratum | < 0.10 |
| A7-4 | β from item counts alone | < 0.10 on all ladders |
| A7-5 | one-year retention of the top 10 and top 25 (ranks only) | ≤ 0.90 on both Reddit ladders |
| A7-6 | fold change of the sd from the smallest to the largest stratum | < 2.0 on comments and Facebook |

Prior: 75%, 60%, 55%, 70%, 60%, 85%.

### A4. Momentum decomposition (all three referees)

Gapped statistic as §2z-df (13-week windows, 52-week changes, gaps 0, 13,
26); Facebook settings as there. Each item, with the readings it
distinguishes:
- **D6. Items against engagement per item.** Gapped momentum of
  Δlog(items) and of Δlog(activity per item).
  - Stock of participants: items carry it (≥ +0.10), per item < +0.05.
  - Exposure or recommender: per item carries it.
- **D7. Residualisation on an aggregate loading and on co-movement
  clusters.** Loadings and clusters come from weekly changes in weeks 0–51
  (k-means on correlation profiles, k = 20); momentum is measured afterwards.
  - Shared trends: residual momentum below half of raw.
  - Name-level persistence: at least 75% of raw survives.
- **D8. Sign asymmetry.** Spearman for change-1 risers and fallers
  separately.
  - Amplification of risers: risers > fallers by at least 0.05.
- **D9. Lag profile** (submissions, one origin): gaps 0, 26, 52.
  - Persistent drift by name: flat.
  - An episode: decays to < +0.05 at 52.
- **D10. Reddit at Facebook's settings and dates** (4-week windows, 26-week
  changes, from Dec 2020).
  - A platform contrast: Reddit > +0.05.

My priors:

| reading | prior |
|---|---|
| items carry it | 55% |
| shared trends explain half | 40% |
| asymmetry | 35% |
| decays by 52 | 50% |
| Reddit > +0.05 at Facebook's settings | 65% |

### A8. Gomez's decomposition of the arithmetic fall (submissions top 78; comments top 195)

Exact accounting of the annual growth of the arithmetic share of summed
weekly activity, per block pair: within (the start top-k's members against
the total), between (entrants against leavers), demography (entering or
leaving the universe). The measured between term is set against
½(ζ_k − 1)ν_k², with ζ_k from the local arithmetic tail and ν_k² the lasting
variance at the boundary.

**Expectations:**
- **A8-1:** the fall is dominated by the within term (|within| > 2·|between|).
- **A8-2:** the measured between term is within a factor of 2 of
  ½(ζ − 1)ν².

Prior: 75%, 50%.

### Later (designs before their runs)

- **A6:** harness repair (entrants as identities, arithmetic shares from
  activity, matched populations), then an out-of-period structure test:
  estimate on 2019–20, forecast 2021–22 for A, the RDM and a trend variant.
- **A9:** a joint block-over-time bootstrap of the pooled boundary ratio.
- **A10:** bridge protocol v3.1 with the three added primaries (owner
  approved).
- **A11:** frame d, the manuscript draft and the figures.
- **A12:** a round-3 referee review.

## Addendum, before its run: A6, the repaired harness and an out-of-period structure test

**Repairs** (round-2 referees V2 and V3):
- Simulated reborn slots are kept as new names, each in its own column, as
  the data keep later entrants. Data and simulations are then ranked on the
  same footing: everything present.
- Shares are arithmetic: weekly activity (expm1 of the log level) summed over
  each 52-week block, then the share of the year's top k by total within its
  top K, as in §2z-da.

**Out-of-period design** (`structure_oop.py`):
- Estimate on weeks 4–107 only (2019–2020, blocks 0–1).
- Simulate from the filtered state at week 108 through week 211 (blocks 2–3,
  2021–2022), 20 seeds.
- Arms: model A's dynamics; the moving home (hold closure from the end-2020
  ladder, the frozen form).
- Statistics, data against each arm, block 2 → 3:
  - one-year retention at K/64, K/16, K/8 and K;
  - the arithmetic share of the top K/64 in block 3, and its change since
    block 1;
  - momentum: Spearman of (block 2 − block 1, block 3 − block 2), block 1
    from the data;
  - excess kurtosis of 26-week changes.
- **Baselines:** retention carried forward from block 0 → 1, the last fully
  observed year before the origin; the block-1 share carried forward (no
  change).

**Expectations:**
- **A6-1:** the RDM's mean absolute retention error over the four cut-offs
  is ≤ A's on both Reddit ladders.
- **A6-2:** neither arm beats retention carried forward. The stationary
  models have no information about the year beyond the ladder.
- **A6-3:** both arms forecast a change of the top-K/64 arithmetic share
  smaller in magnitude than half the observed change on submissions. The
  stationary closures do not forecast the fall.
- **A6-4:** both arms' momentum is within ±0.05 of 0.
- **A6-5:** keeping entrants changes the RDM's simulated one-year retention
  by less than 0.02 at every cut-off. This is checked in sample on the
  full-panel run, so the V3 mismatch would be minor.

Prior: 55%, 70%, 80%, 85%, 50%.

## Addendum, before its run: A5b, the variance-preserving moving home (the V4 correction)

**Why (post hoc, disclosed).** The frozen moving home failed on the long
submissions panel: 0.246 ± 0.085, against persistence 0.163 and frozen A
0.194 (§2z-dg A5). Simulated rank changes are too small at every horizon:
median 13-week move 4–6 against 7–8 observed, with the 90th percentile low.
The round-2 referees had found the cause in the algebra (§2z-df V4). This
correction is made after seeing that failure.

**The algebra.** Write
- L_{t+1} = L_t − κ(L_t − H_t) + ε_t, Var ε = q;
- H_{t+1} = H_t + g + η_t, Var η = σ_L².

The stationary deviation then has Var(H − L) = (q + σ_L²)/(2κ − κ²), and
the one-step increment of L has variance

    Var ΔL = q + κ²(q + σ_L²)/(2κ − κ²) = [2q + κσ_L²]/(2 − κ)

- **Model A** (fixed home, q = σ_p²): 2σ_p²/(2 − κ).
- **The frozen form** (q = σ_p² − σ_L²): A's value minus σ_L². Its short-lag
  movement is short by the whole lasting variance.
- **Preserving A's one-step variance requires q = σ_p² − κσ_L²/2.**

**The variant.** `lasting="movinghome_vp"` in `rankdiff_rdm.py` (opt-in; the
frozen form is unchanged). It is the frozen form with
q = max(σ_p² − κ(z)·σ_L²/2, 0.1·σ_p²). There is no new parameter.

**Expectations, on both long panels at 20 reps:**
- **A5b-1:** non-inferior to A in the same harness (≤ A + 0.05) on both.
- **A5b-2:** the median simulated 13-week rank change within ±1 of the
  observed in at least 4 of 5 splits on submissions.
- **A5b-3:** a lower gate error than the frozen form on submissions.

Prior: 50%, 55%, 75%.

## Addendum, before its run: A9, the joint bootstrap (`joint_bootstrap.py`)

**Method.**
- One Poisson(1) weight per community in each replicate, shared by the
  variance cohort and every start's crossings.
- The starts themselves are resampled with replacement.
- Point estimates are the registered ones. The check is that the code
  reproduces the K/16 … K pooled values of §2z-da: comments 1.069,
  submissions 0.802, Facebook 0.977.

**Expectations:**
- **A9-1:** the point values reproduce §2z-da within 0.01.
- **A9-2:** on both Reddit ladders the joint 5–95% interval is at least 20%
  wider than the registered one.
- **A9-3:** the three-ladder pooled interval (comments, submissions,
  Facebook; Instagram has its own apparatus and is left out) contains 1.

Prior: 85%, 70%, 65%.

## Addendum, before its run: A5c, the 2×2 ablation around the corrected model

The ablation of A5 was planned around the frozen form. With the V4
correction the moving-home arms are:
- RDM′ `movinghome_vp`: random-walk home plus drift by rank;
- (a′) `movinghome_vp_nodrift`: random-walk home without drift.

They sit beside (b) `homedrift` (fixed home that slides by rank) and A
(`home`).

**Expectation (A5-3 restated):** on both long panels the four arms A, (b),
(a′) and RDM′ are within 0.03 of each other. The gate cannot identify the
lasting process at its horizons. Prior 60%.

## Addendum R3, before the runs: the round-3 referees (SYNTHESIS_R3.md)

### R3-A. Null worlds (`null_worlds.py`)

Every headline statistic is computed, with the paper's own estimators, in
four simulated worlds. The aim is to separate statistics that hold
automatically from discoveries. All four are planted except (iv).

**Common setup** (worlds i and ii): N = 20,000, K = 5,000, T = 212 weeks.
- Start: a Zipf ladder (S = 1) plus noise of sd 0.3.
- Passing parts: an AR(1) swing (half-life 5 weeks, stationary sd 0.3)
  plus white noise of sd 0.3.

**The worlds:**
- **(i) Driftless Gibrat:** lasting σ = 0.10 a week, no drift. The top K is
  re-selected weekly.
- **(ii) Firm-like Gabaix/Atlas:** σ(L) = 0.10·exp(−0.2(L − L_K)) (β = 0.2),
  a common drift −0.005, and a reflecting floor at the start's lowest level.
- **(iii) Neutral copying (Wright–Fisher):**
  - M = 1.5·10⁶ posts a week;
  - each copies last week's community distribution, or innovates a new
    community with μ = 0.003;
  - 300 weeks of burn-in; activity is the count.
- **(iv) The data-calibrated model:** RDM′ (`movinghome_vp`, hold drift),
  estimated in sample on `reddit_submissions_long` and simulated for
  212 weeks.

**The statistics:**
- turnover by k and the top 10 (52-week blocks);
- Eq. 1's ratio;
- the pooled boundary ratio over K/16 … K (`mts_boundary_law.analyse`, 26-week
  blocks);
- the slide s̄_k and the crossing gain c_k, and corr(shape change, −s̄)
  against corr(shape change, c − s̄) (`mts_balance_law.analyse`, 52-week
  blocks);
- β (strata of permanent rank, year 1 → 2);
- gapped persistence at gaps 13 and 26;
- Iñiguez's rank change C(R): the probability that the occupant of rank R
  differs from the previous week.

**Expectations:**

| | expectation | prior |
|---|---|---|
| NW-1 | Eq. 1 (exact form) within [0.8, 1.25] in ≥ 80% of cells in every world: it is kinematics | 85% |
| NW-2 | the pooled boundary ratio within [0.85, 1.18] in worlds i, ii and iv (level diffusions) | 75% |
| NW-2 | the same in world iii | 50% |
| NW-3 | s̄_k > 0 at every cut-off in every world, driftless Gibrat included: the slide is partly definitional | 85% |
| NW-4 | β: world i \|β\| < 0.05; ii in 0.12–0.28; iii ≥ 0.3; iv within ±0.05 of the data's 0.045–0.082. The data's 0.01–0.08 then discriminates against ii and iii | 80% |
| NW-5 | gapped persistence within ±0.05 in all four worlds, so the data's +0.14 to +0.20 discriminates | 85% |
| NW-6 | C(R) rises with R in every world and in the data (a stable top by Iñiguez's measure), while top-10 set replacement exceeds top-K/2 replacement wherever S is smaller at the head | 70% |
| NW-7 | in the null worlds, too, corr(obs, −s̄) ≥ corr(obs, c − s̄): the slide carries the variation in any world | 60% |

### R3-B. C(R) on the three data ladders

Computed from the weekly ranks, in the same script. **Expectation:** C(R)
rises with R on every ladder. It is below 0.5 a week at R ≤ 3 and above 0.9
at R ≥ 100. Prior 75%.

### R3-C. One model

- A6 rerun with `movinghome_vp`.
- The flat start rerun with 20 seeds and bands.

**Expectations:**
- **C-1:** RDM′'s one-year retention MAE is within 0.01 of the frozen
  form's on each ladder.
- **C-2:** the flat-start slopes (20 seeds) are within 5% of the 10-seed
  values.

Prior 60%, 90%.

### R3-D. Persistence, repaired

The universe is chosen from the cohort window only (`member_window`). The
split is at the true sign of change 1, with conditional means. The factor
comparison uses matched origins.

**Expectations:**
- **D-1:** the gapped statistic with prefix-only membership is within 0.03
  of the full-membership value on both ladders.
- **D-2:** at the true sign, the persistence of fallers exceeds that of
  risers on both ladders. Both conditional means are on the same side as
  change 1.

Prior 75%, 70%.

### R3-E. Gomez, done properly

- The general displacement term ½·f(q)q²/(p·ā_top)·ν², with f the density of
  activity at the threshold.
- Cohorts fixed on permanent rank; a regression-to-the-mean baseline from a
  share-stable year.

**Expectation:**
- **E-1:** where ζ > 1, the measured between term exceeds the implied one by
  at most a factor of 2.5.

Prior 50%.

## Addendum R4 (2026-10-18, after round 3; before any run): persistence after the 2020 surge

**Why.** Round-3 open item V13 (Fable): every annual triple behind the gapped
persistence statistic has its first change spanning the first half of 2020,
the surge in Reddit activity. The panels start in the week of 26 November 2018
(comments) and 3 December 2018 (submissions) and end in the week of
19 December 2022 (213 and 212 weeks). With 13-week windows a triple spans
143 weeks or more, so no annual triple with 13-week windows avoids 2020 H1.
With 4-week windows a triple spans 112 + gap weeks, so triples can start in
the week of 6 July 2020 (index 84 on comments, 83 on submissions) or later.
This is post hoc and is labelled so. Development data only (Reddit ladders
already exposed). The cohort is the top K by mean level over weeks 0–25, as in
R3-D.

**Configurations:**
- W = 13 at all origins (from week 26, every 13 weeks), gaps 0/13/26: the
  paper's statistic, recomputed.
- W = 4, H = 52, gaps 4/8/13, at all origins (from week 26, every 4 weeks)
  and at post-surge origins (first window starts on or after 6 July 2020,
  every 4 weeks).
- Universes with full-panel and prefix-only (member_window = 26) membership.
- The same statistics in two simulated worlds (driftless Gibrat with the
  passing parts of R3-A; the data-calibrated model world of R3-A).

**Expectations:**

| | expectation | prior |
|---|---|---|
| R4-1 | W = 4, all origins, gap 13: positive on both ladders and at least half the W = 13 value at the same gap | 70% |
| R4-2 | W = 4, post-surge origins: positive at gaps 8 and 13 on both ladders | 55% |
| R4-2b | the same, at least +0.05 on both ladders | 40% |
| R4-3 | both simulated worlds give at most +0.02 at gaps 8 and 13 with W = 4 | 80% |

**What follows.**
- If R4-2 and R4-3 hold, the paper says persistence is present in triples that
  begin after the surge.
- Otherwise it says that, in the development data, annual persistence cannot
  be separated from the 2020 surge, and that the sealed 2023–2024 test (P5 of
  the draft protocol) decides.
- Either way the half-year result from December 2020 stays in the text.

### R4b (2026-10-18, after R4 and before this run): gapped persistence in triples that end before the July 2021 capture change

**Why.** R4's post-surge triples straddle the July 2021 change in archive
capture. The earlier evidence that persistence predates that change is
§2z-de D1. D1 uses adjacent half-year blocks, which the averaging null
inflates. A gapped version is possible only in a few triples:
- 4-week windows and annual changes;
- gaps 4 and 8;
- a cohort taken from weeks 0–12;
- origins from week 13, every 4 weeks;
- the whole triple ending before 1 July 2021 (index 136 on comments, 135 on
  submissions; 135 in the worlds).

These triples contain 2020 H1, so with R4 each confounder is excluded by one
set of triples, never both by the same set. Post hoc; development data only.

**Expectations:**
- **R4-4 (prior 65%):** positive and ≥ +0.05 on both ladders at gaps 4 and 8.
- **R4-5 (prior 80%):** both worlds ≤ +0.02.

## Addendum R5 (2026-10-18, before the run): who the head is, and who fell (named; descriptive)

**Why.** The owner allows Reddit communities to be named (MODEL_STATUS
§2z-dh §10). A reader of a paper about the top of a hierarchy should know who
is at the top and who left it. The falling head of submissions (share of the
top 78 from 50.2% in 2019 to 34.0% in 2022) can be broad, with many members
declining, or concentrated, with a few giants collapsing. The persistence
result says broad.

**What is computed.** Development data, both Reddit ladders. Blocks are the
paper's: 52 weeks, aligned to the panel's end, labelled 2019–2022.
- the weekly rank-1 occupant: its identity and the share of weeks it holds
  rank 1;
- the top K/64 by 2019 total: each member's share of the top-K total in 2019
  and 2022, the share of members whose share fell, and the ten largest losers
  and gainers;
- the communities that entered the 2022 top K/64, and the communities that
  left it.

**Expectations:**

| | expectation | prior |
|---|---|---|
| R5-1 | the rank-1 occupant of comments is one community in ≥ 95% of weeks | 80% |
| R5-1b | the same for submissions, ≥ 90% | 60% |
| R5-2 | more than half of the 2019 submissions head (top 78) hold a lower share of top-K activity in 2022 than in 2019 | 75% |
| R5-3 | the decline is broad: the ten largest losers account for less than 60% of the 2019 head's summed share loss (its members' losses only) | 50% |

## Addendum R6 (2026-10-18, after the gpt-6-astra round-4 report; before the run): the long-run counterfactuals

**Why.** The round-4 reviewer showed that the two-year counterfactuals do
not support "without churn it collapses" on comments. With churn and slide
the head changes by −0.080 a year; with the slide only, by −0.038. Removing
churn slows the flattening there.

**Diagnosis from the saved JSONs (before this run).** The slide is measured
against the re-selected top-K mean U_K. In the simulation, the weekly slide
of the top k is the drift difference (ḡ_K − ḡ_k) plus the crossing gain at K,
c_K, because U_K rises as the universe re-selects. Check on comments at the
head:
- slide-only slide 0.0006 + c_K ≈ 0.0025 gives about 0.0031;
- the churn-and-slide slide is 0.0029.

So churn's net effect on the shape U_k − U_K is c_k − c_K. That is negative
at the comments head, where the model's crossing gain rises down the ladder,
and positive at the submissions head. The two-year runs mix this with drift.

The question the title asks, where the hierarchy comes from, is a long-run
one. In Gabaix and Atlas theory, diffusion spreads levels and the upper
ranks' drift deficit against the floor compresses them, and the steepness
S = σ²/(2s̄) is their balance.

**Run** (`churn_counterfactual.py <panel> long30`):
- the model's lasting core as in A3;
- three worlds, each from the observed end-2019 ladder, for 30 years, 20
  seeds: churn + slide, slide only (σ0 = 0), churn only (drift 0);
- the shape at K/64 … K/2 and the local slopes recorded yearly.

**Expectations:**

| | expectation | prior |
|---|---|---|
| CF-1 | slide only: after 30 years the head shape (U_{K/64} − U_K) has lost at least half of its starting value on both ladders | 65% |
| CF-2 | churn only: the head shape grows by more than 50% in 30 years on both ladders, and is still growing in the last decade (last-decade rise ≥ half the first decade's) | 65% |
| CF-3 | churn + slide: the 30-year local slopes at K/64 … K/2 are within 15% of the flat start's (R3-C), so the long-run hierarchy does not depend on where it starts | 70% |

**What follows.**
- If CF-1 to CF-3 hold, the paper's counterfactual statement becomes:
  - churn alone steepens without limit;
  - slide alone flattens it toward collapse;
  - together they hold a hierarchy whose steepness their balance sets,
    whatever the start.
- The two-year non-additivity on comments goes to the SI with the diagnosis
  above.

## Addendum R7 (2026-10-18, after two round-4 reports; before the runs): null worlds with bands, calibrated worlds, β calibration, a Gomez noise baseline

**Why.** Both round-4 reports (gpt-6-astra; Fable 5.1) make four points:
- The R3 null worlds are single seeds. The top-10 turnover rests on 1–10
  of 30 name-years.
- The size-blind world starts from a Zipf ladder with S = 1, so it cannot
  show the data's curvature.
- The β comparison with firms uses an estimator that attenuates a planted
  0.2 to 0.11.
- "Crossings fall short of the slide" is not a discovery: three of the four
  null worlds show it.

**Runs** (`null_worlds_r7.py`):
1. **The four R3 worlds** (gibrat, atlas, neutral, rdm), seeds 0–19, the
   same statistics.
2. **Calibrated worlds from the model estimated on Reddit submissions.** They
   use the data's starting ladder (filtered levels before week 30), its
   passing parts and its exits.
   - Size-blind: planted β = 0, no drift by rank. 20 seeds.
   - Firm-like: planted β = 0.2, no drift. 20 seeds.
   - Calibration: planted β = 0.05 and 0.10, no drift. 5 seeds each.
3. **Every world gets the Gomez decomposition of the arithmetic shares** of
   the top K/64 and K/16 by block total, with two cohorts: the current
   year's top k, and the top k of the previous year.
4. **Data:** the same prior-year cohort in `gomez_r3.py`'s decomposition
   (`gomez_r7_prior.py`).

**Disclosure.** Within each seed the boundary and rate-law bootstraps are cut
to 5 replications, because the bands come from the seeds.

**Expectations:**

| | expectation | prior |
|---|---|---|
| R7-1 | R3 worlds at 20 seeds. Size-blind: the 90% band of top-10 turnover lies above the K/2 mean. Firm-like and neutral: the top-10 mean lies below K/2's | 70% / 85% |
| R7-2 | calibrated size-blind world: top-10 turnover above K/2's (bands apart) | 75% |
| R7-2b | the same world's turnover at K/64, K/16, K/4 and K/2 within ±0.05 of the submissions data at each cut-off | 40% |
| R7-3 | calibrated firm-like world: top-10 turnover at least 0.05 below the calibrated size-blind world's | 70% |
| R7-4 | β estimator: estimated β rises with planted β; planted 0.2 is estimated at 0.10–0.16; the data's raw 0.045–0.082 (submissions) maps to planted β below 0.12 | 60% |
| R7-5 | c < s̄ (mean over cells) in at least two of the three non-Gibrat R3 worlds in at least 80% of seeds: flattening is not a discovery | 80% |
| R7-6 | calibrated size-blind world, current-year cohort: the head's within term is negative, −0.03 to −0.10 a year (the noise baseline) | 60% |
| R7-6b | data, prior-year cohort: the submissions head's within term is below −0.15 in 2020→21 and 2021→22 | 70% |

## Addendum R8 (2026-10-19, after the round-4 Opus report; before the run): what fell at the head, posting or engagement per post

**Why.** The Opus round-4 report sets two records against the abstract's
"persistence … cut the share of Reddit submissions' head":
- the June 2021 step at the submissions head is in votes per post, not in
  posting (§2z-ca);
- persistence is carried by item counts (§2z-dg A4).

So the karma share of the head is split exactly into the share of items and
karma per item relative to the top K:

share_karma = share_items × (karma per item of the head / karma per item of
the top K).

The split is computed for the 2019 head with fixed names, and for each
year's head re-selected. Blocks, universe and arithmetic are those of R5
(`head_named.py`). Development data; names allowed (§2z-dh §10).

**Expectations:**

| | expectation | prior |
|---|---|---|
| R8-1 | the 2019 submissions head (fixed names), 2019 → 2022: the change in log relative karma per item is larger in magnitude than the change in log item share | 55% |
| R8-2 | the re-selected submissions head: item share changes by less than 20% (relative) from 2019 to 2022, while its karma share falls by 32% | 45% |
| R8-3 | comments, re-selected head: the karma-share fall is mostly in item share (the larger part of the log change) | 50% |

**What follows.** If R8-1 and R8-2 hold, the abstract and Results drop the
causal clause linking persistence to the head's fall. The head's fall is then
stated as a fall in engagement per post, dated with §2z-ca. If they fail,
the text says the head lost posting, which is the component that persists.
Either way the causal link is not claimed without a test.

## Addendum R9 (2026-10-19, after the gpt-6-astra round-5 report; before the run): geometry isolated

**Why.** R7's comparison of calibrated and constant-slope worlds changes more
than the ladder: the dynamics, passing parts and exits differ too (gpt R5).
Here the calibrated dynamics are held fixed: the model estimated on Reddit
submissions, with no drift by rank, the same passing parts, exits and random
seeds. Only the starting ladder changes. The top K of the data's filtered
ladder is replaced by a ladder of constant slope with the same level at rank
1 and at rank K; below K the data's ladder is kept. Two worlds, 20 seeds
each:
- `sb_flat` (planted β = 0);
- `firm_flat` (planted β = 0.2).

They are compared with `sb_data` and `firm_data` (R7) at the same seeds.

**Expectations:**

| | expectation | prior |
|---|---|---|
| R9-1 | `sb_flat`: top-10 turnover within ±0.04 of its K/2 turnover (no order without the flat head) | 55% |
| R9-2 | the paired difference (top-10 − K/2) is at least 0.04 larger in `sb_data` than in `sb_flat`, averaged over seeds | 60% |
| R9-3 | `firm_flat`: top-10 turnover below `sb_flat`'s by at least 0.03 (size-dependence protects the top only where the ladder does not) | 50% |

## Addendum R10 (2026-10-19, after the round-5 reports; before the run): is the excess slide carried by persistent decliners?

**Why.** Opus R5's single most valuable change. The measured slide is partly
mechanical: a driftless world started from the same ladder produces one. The
question is whether the excess is carried by the communities whose decline
persists. If it is, the paper's discovery (persistence) and the hierarchy's
dynamics are one finding: random growth sets the shape, and persistent
decline moves it faster than homogeneous random growth can (the
fast-transition problem of GLLM 2016).

**Design** (`slide_by_persistence.py`; post hoc; development data).
- **Prior change:** for each start week s (every 26 weeks, from week 78 to
  T − 78), the change of mean log level between two 13-week windows a year
  apart, ending 13 weeks before s: mean[s−26, s−13) − mean[s−78, s−65).
- **Slide over the following year:** blocks A = [s, s+26) and
  B = [s+52, s+78). The slide of member i is
  −[(x_i(B) − x_i(A)) − (U_K(B) − U_K(A))], with U_K the mean of the top K by
  block level.
- **Cohorts:** the top k by block-A level, for k = K/64, K/16 and K/4.
- **Classes:**
  - Q1 = the bottom fifth of prior change among cohort members with a valid
    prior change;
  - Q5 = the top fifth;
  - the rest.
- **Statistics:** the slide of each class and of the cohort, the contrast
  s̄_Q1 − s̄_rest, and the slide's excess over a driftless world.
- **Universe:** full membership (restrict_universe, buffer 4); presence
  ≥ 90% per window.
- **Worlds** (5 seeds each, the same code):
  - submissions-calibrated driftless world (β = 0, no drift);
  - the submissions model world (hold drift);
  - a comments-calibrated driftless world (the model estimated on
    `reddit_comments_ext`, its own β, no drift).

**Expectations:**

| | expectation | prior |
|---|---|---|
| R10-1 | data: s̄_Q1 − s̄_rest > 0 in ≥ 80% of cells (cut-off × start) on both ladders | 80% |
| R10-2 | driftless worlds: the mean contrast across cells is ≤ 0.0005 a week (no persistence; passing parts make prior decliners regress upward) | 75% |
| R10-3 | data: the Q1 contrast, weighted by Q1's share (0.2 × (s̄_Q1 − s̄_rest)), is at least a quarter of the excess of the data's cohort slide over the driftless world's, pooled over cells, on both ladders | 40% |
| R10-4 | the data's cohort slide exceeds the driftless calibrated world's on both ladders (an excess exists) | 85% |

## Addendum R11 (2026-10-19, after the round-5 reports; before the run): is entry and exit counted twice in the model's drift?

**Why.** Fable R5 (G7). `churn_counterfactual.py` builds the drift by rank
from the net slide s̄ − e, which includes the data's entry-and-exit effect e
(negative). The simulator then runs its own exits and rebirth, which produce
their own entry effect. Entry and exit may therefore be counted twice, so
the drift deficit is overstated:
- by about 13% on average (e −0.0007 against s̄ 0.0055);
- by about half at the comments head (e −0.0018 against s̄ 0.0036).

A drift that is too large gives a balance S* = σ²/(2 · drift) that is too
flat. That may be part of why the model's balance is 35–46% flatter than the
observed ladder at the head.

**Run.** The same flat start (20 seeds, 30 years) and the same three 30-year
worlds from the observed ladder, with the drift built from s̄ alone
(`churn_counterfactual.py <panel> flat20 noentry` and `… long30 noentry`).

**Expectations:**

| | expectation | prior |
|---|---|---|
| R11-1 | flat start, drift from s̄ alone: the 30-year slope at K/64 is at least 15% steeper than with s̄ − e on comments, and at least 5% steeper on submissions | 60% |
| R11-2 | the shortfall of the model's K/64 slope against the observed one shrinks on both ladders | 75% |
| R11-3 | the qualitative long-run results survive: collapse without volatility, steepening without the slide, and agreement of the two starts within 20% | 85% |

## Addendum R13 (2026-10-20, after the round-6 reviews and the owner's directions of 2026-10-20; before any run): lock-in, the "funnel", turnover profiles, and 5-year values

**Why.** The owner asks for lock-in at the level of individual communities over 1–2 years. The owner states that the earlier web-traffic "funnel of churn" is largely a visual effect of the greater mass further down the tail: quantiles show only a modest rise. Round 6 (T1 Opus, T2 Opus, T2 GPT) asks whether turnover profiles are confounded with geometry. The owner limits system-level statements to about 5 years.

**Runs and calculations** (post hoc, descriptive; development data):
- **R13a.** Shape at 1, 2 and 5 years in the corrected runs (`…__noentry.json`, R11): flat start and the three worlds from the 2019 ladder. No new run.
- **R13b.** Shape relaxation 8S²/σ² against the membership time S²/σ² by rank. This is `runs/2026-10-19_literature_and_design_reviews/gllm_relaxation_check.py` (§2z-dl), already run; restated here for completeness.
- **R13c.** One- and two-year retention of each top set, k = 10, 25, K/64 … K/2 (`lockin_funnel.py <panel>`), on the data. The same measure goes into the R12 worlds.
- **R13d.** Quantiles of the one-year change of block-mean log activity by stratum of permanent rank, with stratum sizes doubling from K/64 to K/2: the 50th, 90th and 95th percentiles of |Δ|, the SD, and the stratum maximum (`lockin_funnel.py`).
- **R13e.** The turnover-profile exponent b in z = a·y^b (new entries into the top y in a year; Acerbi & Bentley 2014) for the data and for the 20-seed worlds, from saved turnover by k (`null_worlds_r7_summary.json` and R13c).

**Expectations:**

| | expectation | prior |
|---|---|---|
| R13-1 | at K/64 the share of a top set replaced in two years is 1.2–1.8 times the one-year share on both ladders (passing swings reverse, so it does not double) | 65% |
| R13-2 | from the top stratum (top K/64) to the bottom stratum (K/2–K), the 90th percentile of the one-year |Δ| rises by less than a factor of 1.5 on both ladders, while the stratum maximum rises by at least 1.3 times as much as the 90th percentile does | 60% |
| R13-3 | the profile exponent b differs between the calibrated size-blind world on the data's ladder and the same dynamics on a constant-slope ladder (sb_data against sb_flat) by at least 0.05 | 70% |

## Addendum R12 (2026-10-20, owner-approved analysis; written before any run): calibrated heterogeneous-growth worlds

**Why.** The owner's directions of 2026-10-20 (MODEL_STATUS §2z-dm) ask for "a new analysis with a calibrated heterogeneous growth world", with methodological choices "as standard as possible in the literature".
- Every simulated world so far has homogeneous mean growth. Persistence (+0.13 to +0.20) is the statistic they all miss.
- The correction appended to the round-6 synthesis makes the common-factor variant the decisive test for the submissions head. The rate law with the measured 2020–22 slide accounts for the head's fall. The members' decline quadrupled after 2020, and homogeneous growth with stable parameters does not change its slide.

**Methods.** These follow `runs/2026-10-20_v5_revision/MEMO_HETGROWTH_gpt-6-astra.md`. If the parallel Opus memo changes any choice, an amendment will be added here before any run. The lasting level of the calibrated model (movinghome_vp; frozen estimator settings as in R7 and A6) gains one term, a_it:

ΔH_it = g(r_it) + a_it + σ0 e^{−β(H_it − H0)} η_it

| variant | a_it | precedents |
|---|---|---|
| **(a) fixed heterogeneity** | b_i ~ N(0, σ_b²), drawn once per identity; a reborn identity gets a new draw | HIP: Guvenen 2009; Guvenen et al. 2021. Name drift: Fernholz, Ichiba & Karatzas 2013 |
| **(b) decaying heterogeneity** | u_{t+1} = ρu_t + σ_u √(1 − ρ²) ξ_t, stationary initialization | OU growth rates (Mallmin, Traulsen & De Monte 2026); AR growth components (Sterk, Sedláček & Pugsley 2021); type switching (GLLM 2016) |
| **(c) common-factor exposure** | (λ_i − λ̄_K) f_t | Carhart 1997; the common drift of hybrid Atlas models (Ichiba et al. 2011) |

Variants (a) and (b) use the existing persistent velocity of the home (`vel_var`, `vel_rho`; ρ = 1 for (a)). Variant (c) is a new opt-in term (`fac_path`, `fac_load`) with defaults off.

In variant (c):
- **f_t** is the observed weekly change in log total activity of the panel's tracked universe.
- **λ_i** is each community's OLS loading of its weekly ΔX on f_t, estimated on the training weeks only. Loadings are shrunk toward their permanent-rank stratum mean by empirical Bayes: (1 − se²/var) per stratum.
- **Assignment.** Each simulated community that starts as a data community carries its own shrunk loading. Reborn identities draw from the bottom stratum's loadings.
- **λ̄_K** is the mean loading in the top K, so that the factor moves relative positions, not the universe's mean.
- **Status.** A world driven by the observed platform path is a conditional historical world, not a forecast of platform activity.

**Drift.** The hold drift is solved numerically with the heterogeneity present (`drift_hold_numeric` on the parameters including a_it). The drift then absorbs the composition and does not count it twice (memo, "Revisit the inherited variance and drift estimates").

**Calibration of (a) and (b).** Equally weighted minimum distance (Altonji & Segal 1996; Abowd & Card 1989; Hryshko 2012) on a frozen moment vector:
- weekly growth autocovariances of ΔX at lags 1–8, 13, 26, 39 and 52, with the variances of 4-, 8- and 26-week changes;
- within three strata of permanent rank: K/64–K/16, K/16–K/4 and K/4–K;
- each moment scaled by its data standard deviation, from a bootstrap over communities;
- simulated moments computed through the same universe, selection and windowing as the data.

The grids, each with 3 seeds per point and the argmin and profile curve reported:
- **(a):** σ_b ∈ {0, 0.002, 0.004, 0.006, 0.008, 0.010} a week, crossed with a lasting-variance multiplier m ∈ {0.8, 0.9, 1.0} (joint with the nuisance parameter).
- **(b):** half-life ∈ {13, 26, 52, 104} weeks, crossed with σ_u ∈ {0.004, 0.008, 0.012, 0.016}, at the m chosen in (a).

Variant (c) has no free parameter to calibrate.

**Not used in any calibration:**
- the annual 13-week-window gapped Spearman correlations;
- the prior-decliner contrast (R10);
- head shares and their changes;
- turnover and retention;
- the boundary ratio.

These are the assessment statistics. Because development results already informed the mechanisms, a reproduction is an untargeted development check, not confirmation.

**Assessment.** 20 seeds at the calibrated parameters, both ladders, all three variants plus the matched homogeneous control. The control has the same code, the corrected R11 accounting where drift by slides is used, and the same seeds. The statistics, computed as before:
- the gapped persistence at gaps of 13 and 26 weeks (`null_worlds.stats`), and its lag profile;
- R10's slide by prior-change class (`slide_by_persistence.py`, world mode);
- turnover and one- and two-year retention (`lockin_funnel.measures`);
- the boundary ratio;
- the 5-year shape;
- **the out-of-period head share.** Fit on weeks before 2021, as in A6 (`structure_oop.py`), with each variant's term; the forecast arithmetic share of the top K/64 in 2022 against the observed 0.356 and the homogeneous 0.537 (5–95% 0.476–0.718).

**Recovery (G15).** The structure-function estimates of σ0 and β, and the two-spacing lasting variance at each stratum, are computed on 20 panels of each calibrated heterogeneous world with known truth, and compared with the truth.
- This deviates from the memo's suggestion of about 200 replications, for compute reasons; the Monte Carlo standard error will be reported.
- The memo's identity [V(2h) − V(h)]/h = q + 3Bh predicts upward bias in the lasting variance and attenuation of β.

**Expectations:**

| | expectation | prior |
|---|---|---|
| R12-1 | the EWMD optimum of (a) has σ_b > 0 on comments, with 52σ_b between 0.1 and 0.5 a year | 60% |
| R12-1b | the same on submissions | 50% |
| R12-2 | calibrated (a) or (b) gives gapped persistence ≥ +0.08 at a 13-week gap on comments (untargeted) | 65% |
| R12-2b | the same on submissions | 50% |
| R12-3 | (c) on submissions, with 2019–20 loadings and the observed 2021–22 platform path, forecasts a 2022 head share at least 0.05 below the homogeneous 0.537 | 45% |
| R12-3b | its 5–95% band contains the observed 0.356 | 20% |
| R12-4 | in calibrated (a) and (b) on comments, prior decliners slide faster than the rest: the R10 contrast is > 0, where every homogeneous world gives < 0 | 60% |
| R12-5 | recovery under calibrated (a): the two-spacing lasting variance at the head is inflated by more than 10%, and the |bias| of the structure-function β exceeds 0.02 | 50% |
| R12-6 | must not worsen against the homogeneous control: turnover at K/64 and K/2 within 0.03; boundary ratio within 0.90–1.15 | 70% |

**Status at writing.** BLOCKED. The T9 SSD with the registered panels is not mounted (2026-10-20). Code and synthetic tests can proceed; no run on data until it is.

### Amendment R12-A1 (2026-10-20, before any run; no R12 output exists)

The parallel Opus methods memo (`runs/2026-10-20_v5_revision/MEMO_HETGROWTH_opus-5.5.md`) changes five choices. The GPT memo is consistent with all five.

1. **The factor is leave-one-out.** For (c), f_t is the equal-weighted cross-sectional mean of weekly ΔX over the tracked universe, with community i excluded when its own loading is estimated. It is not the karma-weighted platform total. The submissions head holds up to half the activity, so a total-based loading would partly measure the head itself.
   - The loading spread is corrected for estimation noise: σ²_λ = Var(λ̂) − mean(se²), within stratum.
2. **No curvature moments.** The calibration vector drops the variances of 4-, 8- and 26-week changes. Rank-dependent volatility and passing variance can mimic h² terms (Hoffmann 2019). The vector keeps weekly growth autocovariances at lags 1–8 (for the passing nuisance) and 13, 26, 39 and 52 (non-overlapping, beyond the passing memory).
3. **(a) is bounded and run short.** b_i is truncated at ±2.5σ_b. Variant (a) runs over the panel's length and at most 5 years, never 30: fixed Gaussian drift at the measured spread can violate the second-order stability condition (Fernholz, Ichiba & Karatzas 2013).
4. **An added untargeted test: the riser/faller asymmetry** of the gapped correlation. In the data it is +0.16 to +0.19 among first-year decliners and 0.00 to +0.03 among risers. Gaussian heterogeneity predicts symmetry. Expectation R12-7 (prior 70%): calibrated (a) and (b) give an asymmetry (fallers minus risers) below half of the data's.
5. **The recovery plan is extended:**
   - planted annual spreads s ∈ {0, 0.15, 0.30, 0.45};
   - a misspecification run (fit (a) to a (b)-world);
   - a false-positive run (homogeneous world with a 26-week medium swing).

   Each is replicated 20 times, as already stated.

The SI cites the variance identity V(h) = qh + Bh² + C to Guvenen 2009 and Hryshko 2012 (standard HIP algebra), not to Gomez.

### Amendment R12-A2 (2026-10-20, before any run; no R12 output exists): power, and what each variant is tested on

**The power problem.** Under fixed heterogeneity, weekly growth autocovariances at lags beyond the passing memory equal σ_b² (Hryshko 2012, eq. 8). At the spreads of interest (s = 52σ_b ≈ 0.3 a year) that is about 4×10⁻⁵ a week. The sampling standard error of one such autocovariance in these panels is of order 10⁻⁴, from a weekly change variance near 0.1 over roughly 10⁶ community-weeks. This is the low-power problem the income literature documents (Guvenen 2009 §5; Baker 1997). Changes of 13-week window means over a year carry the same component as (52σ_b)², relative to a much smaller passing variance. Long differences are therefore the standard way to identify slope heterogeneity in short panels (Baker & Solon 2003; Guvenen 2009 §4).

**Changes to the calibration of (a) and (b).** Calibration is by indirect inference (Gourieroux, Monfort & Renault 1993; Guvenen & Smith 2014) on two auxiliary statistics, computed by the same code in data and world:

1. **Persistence.** The Pearson covariance, within strata of permanent rank, of changes between 13-week window means a year apart, at gaps of 0 and 13 weeks for (a), and at 0, 13, 26 and 39 weeks for (b).
2. **The structure-function lasting variance by stratum** (`rdm.structure_function`, the estimator of σ0 and β). With the multiplier m on σ0, this makes the world's measured lasting variance equal the data's under heterogeneity: the G15 correction.

The weekly autocovariances at lags 1–8 are kept only as a check on the passing parts.

**What is then a test, and what is not:**
- **(a) and (b).** Their gapped correlations at the calibration gaps are no longer tests. They are reported as fit. The untargeted tests are:
  - the lag profile beyond the calibration gaps (52 weeks for (a));
  - the riser/faller asymmetry (R12-7);
  - the prior-decliner contrast (R12-4);
  - the out-of-period head share (R12-3);
  - turnover and retention;
  - the boundary ratio (R12-6);
  - recovery (R12-5).

  R12-2 and R12-2b are withdrawn as tests for (a) and (b). They are replaced by: the calibrated (a) reproduces the 52-week value of the lag profile within 0.05 on comments (prior 50%) and does not on submissions (prior 65%), whose profile decays.
- **(c).** Loadings come from 2019 co-movement only. Every statistic, including the gapped correlation at every gap, remains untargeted. R12-2 applies to (c) as written: gapped persistence ≥ +0.08 at a 13-week gap on submissions (prior 45%) and on comments (prior 25%).

**The grids.**
- **(a):** s ∈ {0, 0.1, 0.2, 0.3, 0.4, 0.5} a year, crossed with m ∈ {0.7, 0.8, 0.9, 1.0}.
- **(b):** half-life ∈ {26, 52, 104} weeks, crossed with stationary s ∈ {0.2, 0.3, 0.4, 0.5}, at the m that (a) selects at its optimum.

Each grid point is run with 3 seeds on both ladders. The objective is the sum of squared differences scaled by the data's bootstrap standard deviations (communities resampled, 40 draws).

### Amendment R12-A3 (2026-10-20, after a smoke test and before any factor, out-of-period or recovery output; no R12 output used in any choice below)

**What had been seen when this was written.** T9 was mounted and `run_r12_r13.sh` ran:
- **Step 0** (R13c–d) and **step 1** (data moments) finished.
- **Step 2** stopped at its first line on a key error in `aux_vector`: the data moments are read back from JSON with string keys. No grid point was simulated.

A one-seed smoke test of every path on submissions (scratch script, outputs outside the run folder) then printed:
- the fixed (s 0.3, m 0.9) and decay (s 0.3, half-life 52, m 0.9) grid points, with their auxiliary vectors and objective;
- one world's statistics for each: gapped persistence at 13 weeks −0.027 and −0.078; boundary 1.16 and 1.12; R10 contrast −0.0004 and −0.0009; asymmetry −0.017 and −0.049;
- the factor's loading diagnostics: the noise-corrected loading variance is 0 in strata K/16–K/4, K/4–K and K–4K, and 0.036 of a raw 0.54 in the top K/16. Loadings estimated from one year of weekly changes are almost pure noise at the community level, so empirical Bayes shrinks them to their stratum means: 0.41, 0.56, 0.75 and 1.09.

The factor path then failed (below) before simulating anything.

**Changes:**
1. **`aux_vector`** reads string or integer keys. This is a bug fix that changes no number.
2. **The factor's path is the observed path, as registered.** The code had centred f_t on its full-panel mean, which the addendum does not ask for. Centring removes the platform-wide trend that variant (c) exists to carry: "communities share a platform-wide trend with different exposure to it". λ̄_K already removes the move of the top-K mean.
3. **The hold drift for (c).**
   - **Path.** It is solved with the factor term present, but with the path's weekly values centred and shuffled (seeded). The drift then includes the term's variance but not the observed path's trend. Because the loadings are nearly constant within strata, a drift by rank solved on the observed path would absorb its trend by stratum. In the out-of-period run it would absorb the 2021 path and cancel the term the test is about.
   - **Bug fixed.** The solver sorts its starting ladder (N slots), while the loadings were aligned to the starting communities (fewer slots). The loadings are now put in ladder order and padded from the reborn pool.
   - Variants (a) and (b) are unchanged: their terms have no observed path.
4. **`run_r12_r13.sh`** resumes at a given step (`FROM=2`). Each later step refuses to run without the calibration files it reads; before, the script would have gone on with empty parameters.

**Disclosed, not changed.** With the loadings shrunk to stratum means, variant (c) is in effect exposure to the platform path by 2019 stratum of rank. It is reported as such. No grid, threshold or expectation changes. The one-seed values above are not used in any calibration or score.

### Post hoc extension of the fixed-heterogeneity grid (2026-10-20, after the registered fixed grids; NOT registered)

The registered grid for (a) puts the submissions optimum at its edge (s 0.5, m 1.0; objective 351 over 9 moments). The objective is still falling. Beyond the registered grid, `r12_hetgrowth.py gridx <panel> fixed` evaluates s ∈ {0.6, 0.7, 0.8, 1.0} × m ∈ {0.8, 0.9, 1.0, 1.1} (3 seeds a point; output `r12_gridx__<panel>__fixed.json`), queued after the registered steps. It is descriptive only: it shows where the objective bottoms out. The registered pipeline (worlds, out-of-period run, scores) uses the registered grid's optimum and is not rerun on the extension.

### Amendment R12-A4 (2026-10-20, after the registered grids and before any world, out-of-period or recovery output): a matched control

**Found in review.** `het_params` re-solves the hold drift numerically (`drift_hold_numeric`) for every variant with a term. The control, and the recovery run with nothing planted, kept the closed-form drift of `estimate_rdm(variant="hold")`. A6 (`structure_oop.py`) and R7 (`null_worlds_r7.py`) used that closed form too. Variant against control therefore confounded the term with the drift method.

The registered design calls for "the matched homogeneous world… the same code… the same seeds". The only hint of size is the smoke test's one out-of-period seed: control 0.709, factor 0.486, fixed at s 0.3 0.470. By the loadings, the factor's own effect there should be small.

**Changes:**
1. **A matched control, `control_num`:** the same model with no term and the drift re-solved numerically, exactly as for the variants. It runs for the worlds (20 seeds) and the out-of-period head share (20 seeds), queued after the registered steps.
2. **The closed-form `control`** still runs, as registered, and links R12 to A6 (0.537) and R7.
3. **Recovery** with nothing planted now uses the numeric drift, so all four planted spreads share one drift method.
4. **Scoring:**
   - R12-3 is scored as registered, against 0.537, and also against the matched control's out-of-period share on the same seeds.
   - R12-4 and R12-6 compare each variant with `control_num`, and report the closed-form control beside it.
   - The difference between the two controls measures the drift method alone.
5. **The grids** are not rerun. Only their (s 0, m 1) points used the closed form, and no optimum is at that point.

**A disclosed deviation in the calibration statistic.** A2 names `rdm.structure_function` for the lasting variance. The code uses the two-spacing slope [V(52) − V(26)]/26 by stratum. It is the same family, but it also takes up medium-term variance. In the registered grids it puts even the homogeneous control 1.2–2.1 times above the data. Re-profiled from the saved grid output:
- the covariance moments alone choose s 0.5 (the edge) on both ladders;
- the lasting-variance moments alone choose s 0.

At s 0.5 the heterogeneity term alone adds about 3(0.5/52)²·26 ≈ 0.007 a week to any two-spacing estimate: more than the data's head value under the code's statistic (0.004–0.005) and about the size of the head's whole lasting variance under the model's σ0 and β. The deviation therefore does not decide the optimum. It is reported as is.

## Addendum R15 (2026-10-20, after referee round 7; written before the runs): four diagnostics the round asked for

**Why.** Referee round 7 (`runs/2026-10-20_external_critiques_round7/SYNTHESIS_R7.md`; §2z-do) found:
- the observed shape moved more than the paper says (G1);
- the R12 worlds carry less heterogeneity than planted (G23);
- the loading adjustment of persistence may be an artefact of noisy loadings (G27);
- the durability factor depends on an estimator choice (G28).

The owner asked for these cheap measurements on development data (2026-10-20: "Do 1 and 2"). Every job reads the lean universe cache and runs alone under the swap watchdog.

**Already seen while checking the reviews.** These values are not predicted below:
- the saved block ladders' head shape by year (`runs/2026-10-15_puzzle/`);
- one half-year rate-law cell: comments, start 1 (2019H2→2020H2), k = 195: observed −0.0077 a week, c − s̄ = −0.0054, entry −0.0018;
- the persistence adjustment values in `out_persistence_r3_*.txt`.

**D1, the shape path (descriptive, no expectations).**
- The exact U_k − U_K by year at K/64, K/32, K/16 and K/8, both ladders, from the cache.
- The half-year rate-law cells at non-overlapping starts (1, 3, 5) and overlapping ones (2, 4): observed against c − s̄ + e.
- The stable-parameter model from the 2019 ladder (R11 noentry runs) over years 1–5.

**D2, planted against recovered (R12 fixed form, submissions, s 0.5, m 1.0, 3 seeds).** Four settings × {planted, s 0}:
- drift re-solved numerically, or the closed-form drift;
- exits and rebirth on, or off (with the drift re-solved under the same setting).

Statistics: the R12 auxiliary covariances by stratum (gap 0 and 13), the two-spacing lasting variance, and the cross-sectional SD of per-community growth slopes among communities present at least 104 weeks, each as planted minus s 0.

**D3, the loading adjustment (both ladders, full membership as `persistence_r3.py`).**
- the platform-total adjustment reproduced from `persistence_r3.py`'s method;
- the split-half reliability of the 2019 loadings (weeks 0–25 against 26–51);
- a placebo: the same adjustment with the loadings permuted across the cohort, 50 draws;
- the adjustment with empirical-Bayes-shrunk loadings.

**D4, the durability factor from measured crossings (saved half-year cells, no data).**
- F = 2 ln(K/k) (c/(s̄ − e))² by cell and k;
- gross, and with the composition term removed (C = 0.25 and 0.35 of c, the SI's range).

**Expectations:**

| | expectation | prior |
|---|---|---|
| R15-1 | D2: with the closed-form drift, the bottom-stratum covariance increment of planted over s 0 is at least half the planted (52σ_b)² = 0.25 | 55% |
| R15-2 | D2: switching exits off raises that increment by less than switching the drift re-solve off does | 60% |
| R15-3 | D3: the split-half reliability (Pearson, across cohort communities) of the 2019 platform-total loadings is below 0.3 on both ladders | 70% |
| R15-4 | D3: on submissions, the permuted-loading placebo lowers persistence at a 13-week gap by at least half as much as the real loadings do (median over draws) | 60% |
| R15-5 | D4: the gross durability factor at K/64 on comments (median over cells) is at least 3 | 55% |
| R15-6 | D4: with a composition term of 0.30 removed, it is below 3 | 65% |

**R15 D2, post hoc extension (2026-10-20, after D2's four settings were read; not registered).** None of the four settings explains the attenuation: about 0.06–0.10 of the planted 0.25 reaches the covariance. The remaining candidate is the drift by rank itself, which is a restoring force under either drift solution. One more pair of worlds runs with no drift by rank (`lasting="movinghome_vp_nodrift"`), s 0 against s 0.5, exits on, 3 seeds. It is descriptive.
