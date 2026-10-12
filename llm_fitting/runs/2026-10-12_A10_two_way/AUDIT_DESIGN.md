# A10 — audit of the time-holdout comparison

_Written 2026-10-12 by Claude (Opus 5.5) before the audit was run. It answers
Codex's review (`llm_fitting/handoff/D1_debate/variance_structure/A10_REVIEW.md`)._

Status: EXPLORATORY, DEVELOPMENT data (long submissions, the comments
extension). Data only; no fit, no simulation, no gate evaluation.

## The error, confirmed by reading the code

In `phase1_a10.py`, `run`, the error of the second-half ladder predicted
from first-half variance is a root mean square over about 150 distinct
log-spaced ranks (`predict_ladder`). The figure it was set against, "the
ladder itself moved by", is a root mean square over every rank from 1 to K.
The log grid gives the head far more weight than the every-rank average
does. §2z-bh's statement that the prediction "loses to ladder persistence by
about three times" compares two different losses.

Codex recomputed the carried-forward ladder on the log grid: 0.462 (long
submissions) and 0.470 (comments extension) against 0.528 for the
reconstruction on both, a disadvantage of 14% and 12%.

## What the audit computes

`python llm_fitting/phase1_a10.py audit <panel>` repeats the recorded
holdout and scores both curves under both weightings: the log grid and every
rank. It also reports the carried-forward ladder with its location removed
(no anchor at rank K), and checks that the recorded numbers reproduce.

Both curves are anchored at the second half's level at rank K, and the
universe is chosen on the full window. So this is a comparison of shapes
given a future anchor, not a forecast of level, under either weighting.

## Predictions, written before the run

1. The recorded numbers reproduce (first-half g, 0.528 on both panels, and
   0.168 and 0.194 for the carried-forward ladder on every rank).
2. Carried forward on the log grid: 0.46 and 0.47, as Codex found.
3. On every rank the reconstruction's error is below 0.30 on both panels,
   because its large errors are at the head.
4. On every rank I do not expect the reconstruction to beat the
   carried-forward ladder on long submissions, where g changed by 30%
   between halves. On the comments extension, where g did not change, I
   have no confident direction.

Whatever the numbers, the prespecified condition of A10's prediction 6 (g
within 25% between halves, and the out-of-time error no more than twice the
in-sample error) is unaffected: it compares two errors on the same grid,
and it failed. (Wording of this paragraph corrected against A10's DESIGN.md
while the audit was running and before any of its output was read.)
