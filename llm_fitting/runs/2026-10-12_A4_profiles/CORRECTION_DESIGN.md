# A4 — correction of the half-sample comparison

_Written 2026-10-12 by Claude (Opus 5.5) before the corrected run. It answers
finding 1 of Codex's review R1 (`llm_fitting/handoff/R1_D1_review/REVIEW.md`)._

Status: EXPLORATORY measurement on DEVELOPMENT data (FB Era A, Reddit
comments, long submissions: the three panels of the recorded A4). No new
specification, no simulation, no gate evaluation, nothing adopted.

## The error, confirmed by reading the code

The recorded run split the entities into two halves and left their ranks as
they were in the full universe, intending the halves to cover the same range
of the rank coordinate. The estimator does not take the coordinate from the
frame. It recomputes N as the mean number of entities per period
(`minimal_rankdiff.py`, `estimate`), which is half as large in a half, and
sets each entity's permanent coordinate to log((mean rank − 0.5)/N) clipped
at 0. So in each half fit every coordinate is shifted up by log 2 and the
lower half of the entities sits at z = 0. The frame's own current-rank
coordinate, used for one assignment, was left on the full scale, so the two
were also inconsistent with each other. The half-sample correlation in
§2z-at therefore compares the two halves with each other on a shifted
coordinate covering the upper half of ranks, with the lower half lumped into
the last knots. It is not the reliability of the full profile, and dividing
the R² of a line on the full profile by it mixes two ranges.

Not affected: the full-fit curves' R², the scaling exponents, the counts of
knots at the edge of the κ grid, and the counts of zero σ_obs.

## The corrected comparison

`python llm_fitting/phase1_a4.py --rerank-halves`

- Each half is ranked within itself every week (`mrd._rank_within`), so
  rank, N and the coordinate agree inside the half. Alternating entities by
  permanent rank makes a half's quantile coordinate the full one's to within
  one rank.
- The halves are compared only on full-fit knots that lie inside both
  halves' knot ranges. The line's R² used in the ratio is computed on the
  same knots.
- Everything else is as recorded. Output: `summary_reranked_halves.json`.

Still not addressed, and to be said with the result: the two halves share
their calendar weeks, so common shocks and the common weekly factor give
their errors a shared part. Stepping the half correlation up by the
Spearman–Brown formula assumes independent errors and so can overstate
reliability.

## Predictions, written before the run

1. The three volatility profiles still mostly reproduce: reliability at
   least 0.6 for log σ_perm, log σ_trans and log σ_obs on all three panels,
   except log σ_trans on FB (recorded 0.44), for which no prediction.
2. The reversion rate still does not reproduce band by band: reliability of
   log κ below 0.5 on at least two panels.
3. log V still reproduces better than log κ on all three panels.
4. A straight line still falls short for most volatility profiles: its share
   of the reliability is below 0.8 for at least five of the nine.
5. No single reliability moves by more than 0.25 from the recorded value
   among the nine volatility profiles. This is the prediction I am least
   sure of: the recorded numbers were for the upper half of ranks only.

If 1, 2 and 3 hold, the readings of §2z-at stand with corrected numbers. If
one fails, the corresponding reading is withdrawn in the record.
