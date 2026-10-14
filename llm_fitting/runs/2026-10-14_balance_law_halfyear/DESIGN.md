# The rate law with half-year starts

_2026-10-14, Claude (Opus 5.5). Written before the half-year runner touched a
panel. Part of the owner's request to pursue the rate-law line._

**Status: design. NOT RUN.** Exploratory, DEVELOPMENT data. Same statement,
estimators and runner as §2z-cd (`mts_balance_law.py`, now with `step`), on
26-week blocks with pairs a year apart (`run-halfyear`).

## Why

§2z-cd had four panel-years and two lagged cases. With 26-week blocks, pairs
two blocks apart start every half year, so each panel gives five pairs and
three lagged ones, and changes are still a year long, so the annual cycle
cancels. The pairs run through June 2021, so they also show whether the law
follows the feed change through time on submissions.

## Design

Blocks of 26 weeks counted back from the end (eight per panel). Pairs (a, a+2)
for a = 1 to 5; the cohort for each is fixed on block a−1, the half year
before. Lasting variance from blocks 1, 3 and 5 of the cohort fixed on block
0, spacing 52 weeks: [var(1→5) − ½(var(1→3) + var(3→5))]/52. It is out of
sample only for the last pair. Lagged prediction for pair (a, a+2): slides
from pair (a−2, a), slope at the start of the pair; available for a = 3, 4,
5. Everything else as §2z-cd. Planted check in
`tests/test_mts_balance_law.py` (half-year version of the pressed head).

## What I have already seen

All of §2z-cd, which uses the same data in 52-week blocks; the pairs here
overlap those in time. So the pairs starting at whole years are near-repeats
of §2z-cd and the ones starting at half years are new.

## Predictions

- **H1**: pooled over both panels' ten pairs, cut-offs below K: slope of
  observed on predicted between 0.6 and 1.4, correlation at least 0.8, rms
  error under 0.6 of "no change".
- **H2**: of the six lagged pairs, the lagged prediction beats "no change" in
  at least five and "repeat last change" in at least three.
- **H3**: on submissions, across the five pairs in time order, the rank
  correlation between the predicted and the observed sinking of the head
  (mean over K/32, K/16, K/8) is at least 0.7.

## Limits

The pairs overlap one another by half a year; ten pairs are not ten
independent tests. Cohorts are half a year old, which may inflate slides
(§2z-cd). The variance is from the panel's middle years.
