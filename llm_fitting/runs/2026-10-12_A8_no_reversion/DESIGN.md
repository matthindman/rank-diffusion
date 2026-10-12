# A8 — the fitted model with reversion to the home switched off

_Written 2026-10-12 by Claude (Opus 5.5) before any cell was run. Not in the
work plan as adopted; added, like A7, under its rule that measurements on
development data need no permission._

Status: EXPLORATORY. An intervention in a fitted model on DEVELOPMENT data:
every parameter stays as fitted and the simulator is called with
`kappa=0.0`, an argument it already has, so the lasting level is a random
walk. No refit, no model code, no gate evaluation, not a candidate. Runner:
`llm_fitting/phase1_a8.py`.

## Why

Four results point at reversion to a fixed home: the reversion rate is not
identified band by band and sits on its slowest grid value in 31–52% of
bands (§2z-at); established endpoints relocate to the boundary 2.5–3.8
times as often as simulated (§2z-aw); year over year the data's level moves
like a random walk and the simulation's reverts (§2z-aw); and the variance
of h-week changes runs 10–21% short at 78 weeks (§2z-au). A data-only check
made after §2z-aw adds a fifth
(`runs/2026-10-12_A7_followups/posthoc_cycle_variance.json`): the data's
variance at 26 weeks sits 0.010–0.036 above the straight line through 13
and 52 weeks, the bulge a reverting level produces, and the annual cycle's
share of the variance, estimated from the covariance of quarterly changes
one year apart, accounts for all of it in each of nine panel-by-group
cells. With the cycle's share removed the variance grows in a straight line
from 13 to 78 weeks. The simulation's bulge is 0.012–0.042 and it falls
0.05–0.12 below the line at 78 weeks.

Before reversion is proposed for removal, the cheapest question is what
the existing model does without it.

## Cells

- `struct`, FB Era A, long submissions, comments: full-window universe, the
  frozen structure stack, 20 seeds, two arms (as fitted; no reversion).
  Card and macro index by the ladder run's functions, top-1 share, ladder
  drift, variance of changes at 13 to 78 weeks, direction at quarter and
  whole-year blocks.
- `dep`, comments extension and long submissions: the A2 settings, 30
  seeds, the same two arms. Departure rates by class and the relocation
  counts of A7.

The arm is deliberately crude. The other parameters were estimated jointly
with the reversion, so without it the lasting part carries too much
variance at medium horizons. What the arm shows is direction and rough
size, and which statistics depend on the home at all.

## Predictions

1. Departures of the established cohort rise from 0.44 (comments
   extension) and 0.37 (long submissions) of the data's rate to at least
   0.60 on both, and do not exceed 1.2.
2. The relocated share rises to at least half of the data's on both (from
   0.40 and 0.26 of it).
3. Whole-year blocks, both four-year panels: the adjacent slope moves to
   within 0.10 of +0.25 and the next to within 0.10 of zero.
4. Variance of 78-week changes in the middle group moves to within 10% of
   the data's on both; at 52 weeks the arm overshoots the data by more
   than 10% on at least one (the cost of leaving the other parameters
   alone).
5. The card loses at most two rows on each panel; the macro index gets
   worse on at least two of three; ladder drift gets worse on all three.
6. The FB top-1 share changes by less than one seed SD of the base arm.
7. Sudden lasting first exits stay over-produced on comments (deaths, not
   reversion, make them).

## What each outcome licenses

- Departures, relocation, long-horizon spread and direction all move to
  the data and the card holds: dropping the reversion is the lead candidate
  for D1, as a refit (reversion restricted to zero, or to one number, with
  the lasting volatility re-estimated), and it removes about 55 fitted
  values.
- They move and the card or the ladder breaks: the home is doing two jobs,
  holding the ladder and pulling endpoints back, and a candidate has to
  separate them (a pull on the ladder that is not a pull on the endpoint).
- Departures do not move: reversion is not why established endpoints leave
  too rarely, and the allocation of passing variance (§2z-x, §2z-y) is the
  remaining lead.

Note on order: the runner was smoke-tested on FB with two seeds, writing to
a scratch folder, in the same step that wrote this file. Its output was
seen after the predictions above were written, and they were not changed.
