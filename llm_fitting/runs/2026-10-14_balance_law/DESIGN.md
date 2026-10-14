# The balance by rank, out of equilibrium

_2026-10-14, Claude (Opus 5.5). Written before the runner touched a panel. The
owner's request: "think big, identify the biggest remaining obstacles to a
publishable paper, and tackle the most important/most tractable you can make
progress on your own in the next three hours without additional data."_

**Status: design. NOT RUN.** Exploratory, DEVELOPMENT data, in sample except
where marked. Data only: no fit, no model simulation, no gate evaluation.
Runner `llm_fitting/mts_balance_law.py`; planted-world tests in
`tests/test_mts_balance_law.py`.

## Why this, of the obstacles

As I rank them: (1) there is no tested statement that movement determines
structure once the ladder is allowed to move, and the one design for a slope
prediction is closed (§2z-by); (2) no planted world contains the data, so no
mechanism can be read (needs Codex); (3) one platform, with Facebook
unreadable and the new data sealed (needs the owner); (4) the window holds
dated platform changes, so a stationary balance cannot be the claim; (5) the
candidate for individual movement has no gate evaluation (owner-gated); (6)
the archive's capture delay is unmeasured on our files (needs the WD drive).
Of these only (1), which also answers (4), can be attacked today with what is
on disk, and it is the one the paper's central claim rests on.

## The statement

For the endpoints with a level in both of two blocks (a closed set P), let
U_k be the mean level of its top k positions and shape_k = U_k − U_K. Then,
for small steps,

    d shape_k / dt = c_k − s̄_k,        c_k = σ_k² / (2 S_k)

- σ_k²: lasting variance per week of named endpoints at rank k.
- S_k: the ladder's local slope at rank k.
- s̄_k: the mean slide, against U_K, of the named endpoints ranked above k.

The top k gains c_k per member per week from whoever crosses its boundary,
which depends on the crowding there (1/S_k) and the movement there (σ_k²),
and loses what its members lose against the pack. Where the two are equal
the shape stands; where they are not it moves, at that rate. At k = K the
statement is "slide equals diffusion at the boundary", which §2z-cc checked.
With one slide for everyone it is S = σ²/(2g). It is the stationary formula
of a rank-based diffusion written as a rate, so that it can be tested where
the ladder is not stationary: the submissions head sinks (§2z-bv) and the
comments ladder does not.

The left side is read off the ladder. The right side uses the first two
moments of named movement by rank and the slope at the start. They share no
computation: the left never sees who is where, the right never sees a later
ladder. It is not the accounting identity of step 1, which holds whatever
moves the endpoints; this holds only if movement near a boundary is a
small-step diffusion with the measured variance and members' mean movement
is what a cohort fixed earlier shows.

Entry is kept apart and exact: shape change of all endpoints = shape change
of P + an entry term, both ladders computed with and without entrants and
exits.

## Measurement

Two Reddit panels, four 52-week blocks, centre K, cut-offs K/64 to K. Strata
of permanent rank: one pooled at the top, (0, K/128], then octaves to
(K, 2K]. Certification as in step 1, to rank 1.42 K.

- **Observed**: [shape_k(b) − shape_k(a)] / weeks, on the ladder of P.
- **s̄_k**: for a cohort fixed on an earlier block, the mean change of each
  stratum's members against U_K of P, averaged over the strata above k with
  weights equal to their width in ranks. Primary cohort: the block before
  the pair (2019 for 2020→2021, 2020 for 2021→2022); the 2019 cohort on
  2021→2022 is reported as secondary.
- **σ_k²**: per stratum, [var(change 1→3) − ½(var(1→2) + var(2→3))]/52 for
  the 2019 cohort (§2z-cc's variance route); at a boundary, the member-
  weighted mean of the two strata that meet there. One value per rank for
  the panel: it uses both years and is not out of sample.
- **S_k**: local slope of P's ladder over ranks (k/√2, k√2], mean of the two
  blocks; for the lagged prediction, the start block only.
- **Lagged prediction** for 2021→2022: slides from 2020→2021, slope at the
  start of 2021→2022. Out of sample in the slide and the slope, not in the
  variance. Compared with "no change" and with "repeat last year's change".
- **Stationary slope** S*_k = σ_k² / (2(s̄_k − entry term)), beside the
  observed slope of the full ladder.

Intervals: 5–95% from 300 endpoint bootstraps within strata.

## What I have already seen

While planning I worked two cases by hand from §2z-cc's and §2z-bv's tables,
with slides against the full centre and no entry term. Comments 2021→2022:
the stationary slope came within about 20% of the observed one from K/64 to
K/2 and 30–50% too shallow at K and 2K. Submissions 2021→2022: predicted
rates of −0.0029, −0.0040, −0.0025, −0.0012, −0.0001 a week at K/32 to K/2,
against §2z-bv's shape terms of −0.0033, −0.0026, −0.0021, −0.0011, −0.0003.
So the in-sample fit for that pair is not blind. Not seen: anything for
2020→2021, the closed-set version with exact entry, the primary cohorts, and
the lagged prediction.

## Predictions

Cut-offs below K. "Rate" is per endpoint per week.

- **L1** (in sample, both panels, both primary pairs, 24 cells): the slope of
  observed on predicted through the origin is between 0.7 and 1.3, and the
  prediction's rms error is under half that of "no change".
- **L2**: where the prediction exceeds 0.0010 in size, its sign matches the
  observed one in at least 85% of cells.
- **L3** (lagged, 2021→2022, 12 cells): rms error below "no change" on both
  panels and below "repeat last year's change" on at least one. Least sure.
- **L4**: on comments S*/S is between 0.75 and 1.33 at every cut-off from
  K/64 to K/2 in 2021→2022; on submissions it is below 0.75 at K/32, K/16
  and K/8.
- **L5**: 2020→2021, which I have not looked at: the submissions head is
  predicted to sink (negative at K/32, K/16, K/8, intervals excluding zero)
  and does.

## Known-answer checks before the data

`tests/test_mts_balance_law.py`, planted random walks, 12,800 endpoints:
stationary (prediction and observation both near zero; slide equals diffusion
at K); the top K/8 pressed down by twice the diffusion rate (predicted and
observed rates agree by rank, from −1.6 to −0.4 in units of that rate, and
last year's slides predict this year's change); the head started 1.6 times
too steep (predicted to sink, and sinks). Averages over four seeds were
looked at before the thresholds were set.

## Limits

Two panels, two year pairs each, seven cut-offs that overlap: far fewer than
24 independent cells. The variance is measured once per panel. Small steps
are assumed and the head's steps are not small. Cohort strata stand in for
"the endpoints now ranked above k", and by 2021 three in ten of the top K
were not in the 2019 top K; the primary cohort is one block old for that
reason. Planted checks are random walks; nothing here has been on worlds with
passing swings (V4's nuisance) or births. In sample is in sample.

## Commands and outputs

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_balance_law.py run <panel>
    python llm_fitting/mts_balance_law.py report

`cells/law__<panel>.json`, `report_tables.md`, `REPORT.md`, a MODEL_STATUS
entry and a ledger row.
