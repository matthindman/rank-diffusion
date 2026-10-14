# Forward simulation: report

_2026-10-14, Claude (Opus 5.5). Design, predictions, runner and tests committed
at 7b9ff2f before the run._

**Status: RAN-OK.** Exploratory, DEVELOPMENT data. Tables: `report_tables.md`;
cells in `cells/`; a post hoc diagnosis in `diagnose_hybrids.py` and
`diagnose_output.txt`.

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_forward.py run <panel>
    python llm_fitting/mts_forward.py report

## The short answer

**On submissions, last year's movement pushed through this year's ladder
forecasts next year's ladder; on comments it does not.**

Rms error of the forecast change of shape over K/64 to K/2, log units over the
year (lower is better):

| | no change | repeat last change | forward simulation | two moments only | eight numbers |
|---|---|---|---|---|---|
| submissions 2020→2021 | 0.188 | 0.065 | 0.062 | 0.056 | 0.200 |
| submissions 2021→2022 | 0.133 | 0.056 | 0.036 | 0.075 | 0.036 |
| submissions, two years ahead | 0.321 | — | 0.108 | 0.111 | 0.447 |
| comments 2020→2021 | 0.085 | 0.279 | 0.263 | 0.265 | 0.531 |
| comments 2021→2022 | 0.021 | 0.080 | 0.075 | 0.080 | 0.093 |
| comments, two years ahead | 0.094 | — | 0.561 | 0.575 | 1.103 |

- **Submissions.** The head sank by 0.29 log units at K/64 in 2020→2021 and by
  0.21 in 2021→2022. Each year's forecast, made from the year before's
  movement, has a third to a quarter of the error of "no change", and is
  level with or better than "repeat last change". Two years ahead it has a
  third of the error of "no change". Slopes are forecast as well as
  persistence gives them, or better.
- **Comments.** The full ladder barely moved (0.02 to 0.14 log units at
  K/64), so "no change" is hard to beat, and the forecast is three to six
  times worse than it. It predicts the head sinking by 0.45 in 2020→2021;
  it sank by 0.14.
- **Two moments are enough where the forecast works**: Gaussian changes with
  each stratum's mean and variance do as well as resampled changes in three
  of four cases. Eight numbers are not: a straight line in log rank misses
  the shape of the drift.

## Why comments fail (post hoc)

Swapping the parts of a transition between years shows which part does not
carry over. On comments it is the movement of present endpoints, not entry:
with movement from the year before and entry from the target year the error
stays 0.248 and 0.041; with movement from the target year and entry from the
year before it falls to 0.021 and 0.026. The year 2019→2020 was the year of
platform growth (§2z-bz), and its movement by rank, a strong gain for the
lower ranks against the top, was not repeated. On submissions the same swap
shows movement carrying over and the residual error coming mostly from
movement too (0.025 and 0.014 with the target year's movement).

So the forecasting map works where last year's movement by rank is a good
guide to this year's, and on Reddit's comments it was not, in the two years
available. The rate law of §2z-cd did better on comments 2021→2022 because it
took lasting drift from a cohort and forecast the persisting set, with entry
taken from the target year; this map forecasts the whole ladder from raw
block changes.

## Predictions scored

| | | |
|---|---|---|
| F1 | beats "no change" in three of four one-step cases, and "repeat last change" in two | FAIL: beats "no change" in two (submissions); beats "repeat last change" in all four |
| F2 | two moments within 20% of resampled changes in three of four | PASS (0.91, 1.01, 1.07; 2.07 on submissions 2021→2022) |
| F3 | eight numbers within 30% in three of four | FAIL (two of four) |
| F4 | two steps ahead beats "no change" on both panels | FAIL: submissions yes (0.108 against 0.321), comments no |
| F5 | worst slope error within five points of persistence's in three of four | PASS (comments 2020→2021 fails: 14% against 4%) |

Two of five held.

## What it means

1. Where the ladder moves, the movement measured a year earlier forecasts
   where it goes, one and two years ahead, with no theory in between. That is
   the strongest out-of-sample evidence so far that structure follows
   movement.
2. Where the ladder holds still, as on comments, the forecast needs the drift
   by rank to carry over from year to year, and on comments it did not. The
   drift by rank is the time-varying ingredient (§2z-cc); a forecast of
   structure is only as good as a forecast of it.
3. Parsimony: two moments by rank suffice; a two-parameter line in log rank
   for the drift does not. The drift's shape in rank matters.

## Limits

Four one-step cases and two two-step cases. Raw block changes by current rank
mix lasting movement with swings and selection. The diagnosis is post hoc and
uses the target year's transitions.
