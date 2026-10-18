# Paper frame "c": the working draft after the model build (DRAFT, for the owner)

_Claude Opus 5.5 (Anthropic), 2026-10-18. NOT ADOPTED: claim language and the
title are the owner's. It consolidates three earlier frames into one working
draft:
- `PAPER_FRAME_DRAFT_2026-10-18.md`, the B spine (owner decision 1, §2z-db);
- `PAPER_FRAME_DRAFT_2026-10-18b.md`, "size does not protect" (§2z-dc);
- the moving-home model and its validation (§2z-dd).

Those two files stay as the record of how the frame got here. The outline is
`paper/pnas_outline_v2.tex`, revised to match. Every number cites its record
section._

## What changed with the model build, and how the frame absorbs it

| new result | where it goes |
|---|---|
| Model A's "home" is a medium-term swing (σ_perm² 0.02–0.12 a week, κ 0.1–0.2), not a lasting level; the home "does two jobs" (§2z-ay, §2z-dd) | Methods and the model section: the reason for the new model |
| **The moving-home model** (A's short-run block; a lasting level that random-walks under drift by rank). Gate v1 at 20 reps: submissions 0.097 (A 0.129, persistence 0.168); comments 0.152 (A 0.137); Facebook 0.123 (A 0.103, contaminated). No-return slope on Facebook reproduced (−0.089 against −0.076; A −0.157). Recovered on panels of 160 weeks or more (§2z-dd) | **Results 4**: a generative model consistent with the balance, validated on held-out movement |
| Jumps estimated from kurtosis are rejected by the ladder's stability; the hold drift then matches the measured head slides (§2z-dd) | Results 4; SI |
| **Reddit momentum:** consecutive annual changes correlate across communities (Spearman +0.37 submissions, +0.32 comments; robust to outliers); none on Facebook (−0.04). No tested model reproduces it; drift by name calibrated on submissions reaches +0.18 and fails transport to comments (§2z-dd) | **Results 5**: a new fact and the model's main open gap |
| Size–variance β 0.01–0.08; attention moves as a unit, not a portfolio (§2z-dc) | Results 1: the headline |

**The paper's claim in one sentence.** The leaders of online attention are
not protected by size. A balance at every rank, and a generative model built
on it, account for how they are displaced and how concentration moves.
Reddit's persistent growth (momentum) is the part neither explains yet.

## Title options (owner's choice)

1. **Size does not protect the leaders of online attention** (recommended)
2. Fast names, slow ladders: mobility, momentum and the concentration of online attention
3. A balance at every rank: how mobility sets the concentration of online attention

## Significance (draft; PNAS limit 120 words)

Large firms are steadier than small ones, and that steadiness makes market
leaders hard to dislodge. The leaders of online attention have almost no such
protection. On Reddit and Facebook, the year-to-year fluctuations of a
community's activity barely shrink with its size, so about a fifth of every
Reddit top set, at every scale, is replaced each year. A stochastic model in which each community's typical level wanders, and
the ranking is held in shape by drift that depends on rank rather than by
communities returning to fixed levels, forecasts held-out movement as well as
or better than the previous model. What it cannot yet explain is Reddit's
momentum: growth that persists from one year to the next.

## Abstract (draft; PNAS limit 250 words)

Online attention is concentrated, yet its leaders change. We ask whether size
protects them, as it protects large firms, whose growth fluctuates far less
than small firms'. In weekly records of Reddit communities (2019–2022) and
Facebook pages, the spread of a community's one-year change in activity
scales as size to the power −0.01 to −0.08, against −0.15 to −0.2 for firms.
On Reddit neither the volume of posts nor the engagement each earns steadies
with size. Leaders are therefore displaced at similar rates at every scale:
16–21% of each Reddit top set is replaced each year. Displacement at each rank
follows a balance known from top wealth shares: communities crossing a rank's
boundary return lasting variance over twice the local slope of the rank-size
curve (pooled ratio 0.96; 0.89–1.05, four ladders). Models of jumps to random
ranks fail this test. A generative model in which each community's lasting
level random-walks, and the ranking is held by drift that depends on rank
rather than by reversion to a fixed level, forecasts held-out weekly movement
better than the previous model on Reddit submissions and as well on comments,
and reproduces Facebook's absence of reversion. Unlike Facebook, Reddit shows
momentum: a community's growth in one year predicts its growth in the next
(rank correlation 0.32–0.37), which no model we tested reproduces. The 78
leading submissions communities' share fell from 50% to 34% in three years.

## Figures

1. **Size does not protect** (`runs/2026-10-18_altitude/fig_contestable.png`,
   panels A–B):
   - (A) size against volatility;
   - (B) turnover at every scale.
   - Add the portfolio test, engagement per item against item count.
2. **The balance at every rank** (`fig_shape_decomposition.png`;
   `fig_four_ladders.png`; `fig_variance_by_rank.png`):
   - the exact split;
   - displacement against lasting variance over twice the slope;
   - lasting variance by rank from three estimators.
3. **Movement in levels, not ranks** (`fig_contestable.png`, panel D): the
   calibrated rank rivals.
4. **A generative model** (`runs/2026-10-18_rdm/fig_rdm.png`):
   - (A) held-out movement on three panels against model A and persistence;
   - (C) turnover by scale;
   - (D) recovery.
5. **Momentum** (`fig_rdm.png`, panel B; to be redrawn with the rank
   correlation and with the diagnostics of §2z-de): data against both models
   on three ladders.
6. **Concentration and a dated change** (`fig_concentration.png`; the
   event-study figure, not drawn): arithmetic shares; the shift to the next
   tier.

## The argument, section by section

1. **Fast names, slow ladders; size does not protect.**
   - Turnover is nearly invariant to scale (§2z-cu).
   - β 0.01–0.08 against 0.15–0.2 for firms (§2z-dc).
   - Engagement per item has γ 0.04–0.12 against 0.5 for independent items
     (§2z-dc).
2. **A balance at every rank.**
   - Gomez's within/displacement decomposition, applied to every cut-off and
     to the mean-log shape (§2z-cr; theory note §9).
   - Lasting movement measured on permanent-rank cohorts. The estimand
     includes a composition term (§2z-da).
3. **Movement in levels.**
   - Displacement equals lasting variance over twice the slope: pooled
     0.955 (0.889–1.054) on four ladders (§2z-da), and a year ahead on
     comments (0.944; submissions fails at 0.617, §2z-dc).
   - Jumps to random ranks, calibrated to turnover, are excluded (1.56).
     Fixed-ladder rank diffusion is not (1.05) (§2z-dc).
   - A near-identity for level diffusion (theory note §8).
4. **A generative model: the moving home** (§2z-dd).
   - Why: A's home is a medium swing, and the data show no return to an
     endpoint's own level.
   - The model: A's short-run block, plus a lasting level that random-walks
     with size-law volatility under the drift by rank that holds the ladder.
   - Validation:
     - Gate v1 (submissions 0.097, comments 0.152; A 0.129 and 0.137);
     - the Facebook no-return slope reproduced;
     - recovery at 160 weeks or more;
     - its own boundary ratio near 1.
   - Limits: Facebook gate contaminated by development; jumps rejected;
     G2 not adjudicated.
5. **Momentum on Reddit** (§2z-dd; diagnostics §2z-de).
   - Consecutive annual changes correlate (+0.32 to +0.37); Facebook shows
     none.
   - Neither model makes it.
   - A stationary drift by name consistent with the turnover reaches only
     +0.18.
   - Diagnostics (§2z-de): present before the July 2021 capture change
     (half-year +0.29 and +0.20); at every size; robust to survivorship; not
     a young-community lifecycle (D2 failed).
   - Remaining candidates: persistent trends of established communities, or
     trends shared by topic groups.
6. **Concentration moves with drift by rank and persistent growth.**
   - Arithmetic shares 50.2% → 34.0% on submissions (§2z-da).
   - The measured slides reproduce half of the fall in-sample (§2z-dd).
   - Around the 2021 feed change, attention shifted to the next tier
     (§2z-da).
7. **Supplementary.**
   - who stays: retention by rank, and the Gaussian under-forecast
     (§2z-cy, §2z-da);
   - Instagram through the censoring apparatus;
   - robustness.

## Does not claim

- that the displacement law is new (Gomez 2023), or that the joint
  regularity is undocumented (Batty 2006);
- that the moving-home model reproduces Reddit's turnover, concentration or
  tails: it does not, because of momentum (§2z-dd);
- that leaders "lose ground" (generic in random growth, §2z-dc);
- that rank exchange of every kind is excluded;
- prediction ahead of time on Reddit submissions;
- the cause of drift by rank or of momentum;
- anything out of sample on sealed data;
- Instagram levels.
