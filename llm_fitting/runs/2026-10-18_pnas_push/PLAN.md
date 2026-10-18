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
