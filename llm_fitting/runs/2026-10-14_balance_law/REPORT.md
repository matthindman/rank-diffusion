# The balance by rank, out of equilibrium: report

_2026-10-14, Claude (Opus 5.5). Design, predictions, runner and planted-world
tests committed at a2ce2a8 before the run._

**Status: RAN-OK.** Exploratory, DEVELOPMENT data, in sample except the lagged
test. No fit, no model simulation, no gate evaluation. Tables:
`report_tables.md`; cells in `cells/`.

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_balance_law.py run <panel>
    python llm_fitting/mts_balance_law.py report

**One implementation error, found on the first output and fixed.** The
cut-offs were whole numbers and the strata's edges are fractions of K (97.66
for K = 12,500), so four of the seven cut-offs found no stratum, returned no
prediction and had slides covering only the top stratum. Three cut-offs per
pair were valid and I saw them. The fix places the cut-offs on the strata
edges; no rule, threshold or prediction was changed; a test now covers it.
Both panels were re-run.

## The short answer

**Where named movement is out of balance the ladder moves, in the predicted
direction every time and at about three quarters of the predicted rate.**

The statement tested is d shape_k/dt = σ_k²/(2S_k) − s̄_k on the endpoints
that persist: what the top k gains from crossings at its boundary, minus
what its members lose against the pack. The left side is read off the
ladder; the right side uses only named endpoints' mean change and variance
by rank and the slope at the start.

| 24 cells: two panels, two year pairs, six cut-offs below K | |
|---|---|
| slope of observed on predicted | 0.76 |
| correlation | 0.94 |
| rms error, per week | 0.00094, against 0.00216 for "no change" |
| sign, where the prediction exceeds 0.0010 a week | 13 of 13 |
| sign, all cells | 21 of 24 |

Lagged, for 2021→2022 with slides measured on 2020→2021 and the slope at the
start (12 cells): rms error 0.00069, against 0.00086 for "repeat last year's
change" and 0.00194 for "no change"; sign right in 12 of 12.

## By panel and year

Predicted and observed rate of change of shape on the persisting set, per
1,000 weeks, at K/64, K/32, K/16, K/8, K/4, K/2:

| | predicted | observed | rms error against "no change" |
|---|---|---|---|
| submissions 2020→2021 | −4.0, −4.9, −4.0, −3.1, −1.1, −0.6 | −5.2, −4.3, −3.4, −2.5, −1.4, −0.7 | 0.7 against 3.3 |
| submissions 2021→2022 | −6.0, −5.9, −4.0, −2.2, −0.6, −0.2 | −4.0, −3.3, −2.6, −2.1, −1.1, −0.3 | 1.5 against 2.6 |
| comments 2021→2022 | −0.4, +0.1, +1.5, +1.8, +1.6, +0.8 | +0.5, +0.7, +1.1, +1.2, +1.1, +0.8 | 0.6 against 0.9 |
| comments 2020→2021 | +0.1, −0.0, +0.9, +1.3, +0.8, −0.1 | −0.9, −0.5, +0.0, +0.3, +0.6, +0.4 | 0.7 against 0.5 |

- **The submissions head sinks, and the law says so, by rank.** Its members
  lose 0.006–0.011 a week against the pack while crossings return 0.002–0.005,
  because the ladder there is steep for how little those communities move.
- **Comments 2021→2022: the persisting ladder steepens in the middle, as
  predicted** (correlation 0.99). The full ladder barely moves because entry
  takes back most of it: entrants land near K and lift the centre by 0.0005
  to 0.0011 a week against the head. That year's stability on comments is two
  opposing movements, not none.
- **Comments 2020→2021: the law does worse than "no change".** The observed
  changes are small (under 0.001) and the predictions are about 0.001 too
  high in the middle. This is the one pair where it fails.
- **Over-prediction.** Observed is 0.76 of predicted overall: 0.97 and 0.64
  on submissions, 0.68 on comments 2021→2022. Every primary cohort is fixed
  on the block next to the pair, and such a cohort can still carry passing
  swings that fade over the measured year, inflating its slide (R4, finding
  1). With the older 2019 cohort on 2021→2022 the slope is 1.10 on
  submissions and 0.73 on comments, so that may be part of it on submissions
  and is not the explanation on comments. I do not know the cause.

## The slope movement would sustain

S* = σ_k² / (2(s̄_k − entry term)) over the observed slope, 2021→2022:

| | K/64 | K/32 | K/16 | K/8 | K/4 | K/2 | K |
|---|---|---|---|---|---|---|---|
| comments | 0.71 | 0.78 | 1.15 | 1.32 | 1.37 | 1.14 | 0.85 |
| submissions | 0.47 | 0.27 | 0.39 | 0.59 | 0.88 | 0.97 | 1.03 |

On comments the ladder is within about 35% of what movement sustains at
every rank. On submissions the head is two to four times steeper than its
movement sustains, and the lower half is at balance. If slides and variances
stayed as measured, the submissions head would go on sinking after 2022.
That is a statement the sealed 2023–2024 archive could test; I have not
registered it.

## Predictions scored

| | | |
|---|---|---|
| L1 | in sample: slope 0.7–1.3 and rms error under half of "no change" | PASS (0.76; 0.43) |
| L2 | sign right in at least 85% of cells where the prediction exceeds 0.0010 | PASS (13 of 13) |
| L3 | lagged: beats "no change" on both panels and "repeat last year" on at least one | PASS (comments 0.00045 against 0.00094 and 0.00097; submissions 0.00086 against 0.00257 and 0.00074) |
| L4 | S*/S within 0.75–1.33 on comments from K/64 to K/2; below 0.75 on submissions at K/32, K/16, K/8 | FAIL on comments (0.71 at K/64, 1.37 at K/4); holds on submissions |
| L5 | 2020→2021, unseen: the submissions head is predicted to sink at K/32, K/16, K/8 and does | PASS (−4.9, −4.0, −3.1 against −4.3, −3.4, −2.5) |

L1 passes narrowly on both counts. On submissions "repeat last year's
change" beats the lagged prediction: the head simply kept sinking at four
fifths of the previous year's rate.

## Planted worlds

Committed tests: stationary walks (prediction and observation near zero); the
top K/8 pressed down (rates agree by rank; last year's slides predict this
year's change); a head started too steep (predicted to sink, and sinks).
Added after the run, also passing: the pressed head with AR(1) passing
swings on every endpoint and a newcomer a week near K. Predicted against
observed there, in units of the diffusion rate: −1.1, −1.2, −0.9, −0.7, −0.4
against −1.6, −1.3, −1.0, −0.7, −0.2.

## What it means

1. The stationary balance, which any stable ladder satisfies, becomes a rate
   law that can fail. It did not fail in three of four panel-years, including
   the one where the ladder was moving fastest, and it has one lagged test
   behind it.
2. It says what holds a ladder still: a top set is stable when crossings at
   its boundary return what its members lose. It says what moves one:
   anything that changes members' mean movement by rank, such as a feed
   change, moves the shape at once at the rate of the imbalance, and the
   slope then adjusts until crossings make up the difference.
3. It is the thing a sealed test could freeze: variance by rank and slides
   by rank from one period, the slope at the start of the next, and a
   predicted change of shape by rank, scored against "no change" and "repeat
   last change".

## What this does not show

A mechanism: the law takes members' mean movement by rank as given and does
not say why it is what it is. Out-of-sample validity beyond one lagged year
on two panels. Anything at the very top (above K/64) or about Facebook.

## Limits

Seven overlapping cut-offs on two panels and two year pairs: the 24 cells are
far from independent. The variance is measured once per panel from both
years, so no test here is out of sample in the variance. Steps at the head
are not small. The over-prediction is not corrected. The planted worlds are
random walks with optional swings and births; none has heavy tails or
endpoint-specific cycles. The first output was seen before the cut-off fix.
