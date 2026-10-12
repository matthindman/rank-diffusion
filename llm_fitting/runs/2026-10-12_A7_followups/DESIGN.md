# A7 — four follow-up measurements to A1 and A2

_Written 2026-10-12 by Claude (Opus 5.5) after the A1 and A2 results
(MODEL_STATUS §2z-au) and before any cell of this task was run. This task
is not in the work plan as adopted; it is added under the plan's rule that
measurements on development data need no permission._

Status: EXPLORATORY measurements on DEVELOPMENT data. No model code, no
gate evaluation. Statistics in `llm_fitting/residual_anatomy.py` with
known-answer tests in `tests/test_residual_anatomy.py`; runner
`llm_fitting/phase1_anatomy.py` (`a7`, `a7b`, `report7`). Data and
simulation go through the same functions, and the simulations are the same
fits and seeds as A1 and A2.

These were chosen after seeing A1 and A2, so they are post hoc as a set.
Within the set the predictions below were written first.

## Why each one

A1 and A2 left four things open, and each decides something about the
candidates for decision point D1.

**a. Tails at long horizons, on a yardstick that does not depend on
wandering.** The A1 statistic is higher for a series that wanders than for
one that reverts (correction to §2z-au). The anchored version divides each
endpoint's h-week changes by its own one-week scale; a Gaussian process
scores 1.57 at every horizon. Note what it can and cannot show: shocks that
are fat-tailed and independent from week to week average out over 26 or 52
weeks, so tails that survive to those horizons mean rare large moves,
volatility that changes over time, or endpoints that differ in how
long-run and short-run volatility relate.

**b. Relocation counted by endpoint.** One endpoint that moves to the
boundary makes many exit events. `cohort_fate` counts endpoints: how far
the cohort's permanent rank moved between the train and scoring windows,
how many relocated to the boundary zone, and the class of each endpoint's
first exit.

**c. Direction.** `momentum`: does the change of an endpoint's level
between two blocks of weeks predict its change between the next two? Zero
for a random walk, negative for a level that reverts, positive when
direction persists. The model reverts; the record has directional slow
movement on comments (§2p) and no dispersion of constant drifts (§2p, H2).

**d. Volatility over time.** `volatility_persistence`: is an endpoint's
volatility the same in the two halves of the panel as it is between
interleaved weeks? The model has one fixed amplitude per endpoint.

## Panels

a, c, d: FB Era A, long submissions, comments; full-window universe; frozen
structure stacks; seeds 0–19 (the A1 simulations). Momentum uses 13-week
blocks on all three panels and 26-week blocks on the two Reddit panels.
b: comments extension and long submissions with the A2 settings; seeds
0–29; main split and within-train split.

## Predictions

Middle rank group unless stated.

a1. At one week the anchored ratio is within 0.05 of the entity-scaled one,
    in data and simulation (a check on the statistic).
a2. At the longest horizon of each panel the data's anchored ratio exceeds
    the simulation's on all three panels, by at least 0.20 on submissions.
a3. On FB and comments that excess is smaller than the one-week excess.

b1. The share of the cohort relocated to the boundary zone (scoring-window
    permanent rank beyond 0.75 K, still present) is at least twice the
    simulation's on both panels.
b2. The median shift is larger in the data than in the simulation by less
    than 0.10; the 95th percentile is larger by at least 0.15.
b3. Among first exits, slides are a larger share in the data than in the
    simulation, by at least 0.10, on both panels.
b4. The share of scoring-window cohort-weeks spent outside the top-K is at
    least 1.5 times the simulation's on both panels.

c1. The simulation's skip slope is negative on all three panels (13-week
    blocks).
c2. The data's skip slope is higher than the simulation's on all three.
c3. On the two Reddit panels the data's skip slope with 26-week blocks is
    between −0.10 and +0.10.

d1. In the simulation the ratio of the half-to-half correlation to the
    interleaved correlation is at least 0.85 on all three panels.
d2. In the data the ratio is lower than the simulation's by at least 0.10
    on all three panels.

## What each outcome licenses

- a: an anchored excess at the longest horizon keeps "rare large lasting
  moves or changing volatility" alive on that panel; none drops C-T there.
- b: many relocated endpoints whose first exit is a slide means the level
  wanders to the boundary gradually; first exits that are mostly sudden,
  followed by flicker, means steps.
- c: data near zero with the simulation negative says the lasting level
  behaves like a random walk and the reversion is the misfit; data positive
  says direction persists, which a random walk does not give and which
  would reopen an endpoint-specific trend against §2p (H2) with new
  evidence; data equal to the simulation says reversion is not the problem.
- d: a lower ratio in the data means the amplitude is not a permanent
  property of an endpoint. That bears on C-A (one amplitude everywhere),
  on the spread s, and on the fat tails inside endpoints.

None of these readings is final until V2 shows the statistic separates the
planted alternatives at these panel sizes. The four statistics are in
`residual_anatomy.py` for V2 to run.
