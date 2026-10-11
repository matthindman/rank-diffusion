# A5 — the ladder cell that was not run: swings + full amplitude

_Written 2026-10-12 by Claude (Opus 5.5) before any cell of this task was
run. Work plan `WORKPLAN_CLAUDE_2026-10-12.md`, Phase 1, task A5._

Status: EXPLORATORY measurement on DEVELOPMENT data. Structure only: no
gate evaluation. Nothing is a candidate and nothing is adopted.

## Question

In the ladder of §2z-ap the in-sample macro index rises at the amplitude
step (R4) on all five panels. At that step the simulators scale only the
passing components (σ_trans, σ_obs) by the endpoint amplitude v_i; the
stated model scales the lasting component too. Is that half-application the
cause of the loss?

## Cells

One new cell per panel, **R4m**: the R3 fit plus `--temperament
--mix-hetero` (the lasting component scaled by v_i^b, b estimated from the
horizon moment s(h*)/s(1) as the code does). Everything else as in the
ladder: same universe, `min_knot_n = 8`, exact NNLS, the same nineteen
statistics, seeds 0–19, through `ladder_explainer.run_struct` with only the
specification changed. Compared with the R3 and R4 cells already on disk
(`runs/2026-10-12_ladder/cells/`, same seeds, same estimator and simulator
code).

Panels: FB Era A, Reddit comments, long submissions, short submissions,
Instagram rescue.

A fact about the code, not a prediction: on the 30-week short submissions
panel no horizon has the eight non-overlapping changes the b estimate
needs, so b = 0 and R4m must equal R4 there. If it does not, something is
wrong with the run.

## Reported

Macro index and its four blocks for R3, R4, R4m; the paired difference
R4m − R4 with a 5th–95th percentile band from resampling the 20 seeds
jointly; b and s; simulated top-1 share over the data's.

## Predictions

1. On FB Era A, comments and long submissions the R4m index is not higher
   than the R4 index.
2. The recovery is partial: on comments and long submissions R4m removes
   less than half of the R3 → R4 increase. (Reason: adding the same flag at
   the end of the structure path moved the index by only −5% and −11%
   there, against increases of +24% and +25% at R4.)
3. On FB Era A the paired difference R4m − R4 is inside its seed band (not
   distinguishable from zero).
4. The simulated top-1 share on FB Era A is not lower at R4m than at R4
   (ratio to data 2.35 at R4): spreading lasting volatility across
   endpoints should not shrink the head.
5. Instagram rescue: R4m within ±5% of R4.

## What each outcome licenses

- The loss disappears at R4m (prediction 2 fails upward): the half-step is
  the cause; candidate C-A (one amplitude for the whole model) goes to a
  dossier with this as its main evidence.
- The loss stays: the amplitude itself strains the variance block; C-A is
  weaker, and the amplitude estimator's recovery (Codex task V2, part 2)
  becomes the next thing to look at.
