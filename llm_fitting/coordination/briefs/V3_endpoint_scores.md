# V3 — Endpoint-level scores (supplementary, retrospective)

Branch `codex/v3-endpoint-scores`. Handoff folder
`llm_fitting/handoff/V3_endpoint_scores/`. Working rules:
`llm_fitting/coordination/PROTOCOL.md`.

## Why

The program's second goal is to predict how individual endpoints move. No
recorded evaluation asks whether the model knows which endpoints will
move: Gate v1 and `--dist-scores` both score pooled displacement among
survivors (`evidence-standards.md` §4, §5). Two of the largest remaining
errors, departures of established endpoints and the upper tail of movement,
are invisible to both. This task builds the scorer that can see them. Its
results are supplementary and retrospective on development data. They never
re-adjudicate a recorded verdict, and whether the scorer is registered for
new data is the owner's decision.

## Read first

The skill and `references/evidence-standards.md` §4 and §5;
`RESEARCH_AGENDA_2026-10-08.md`, "Validation discipline" (yours);
`RESEARCH_AGENDA_CLAUDE_2026-10-08.md` W2 item 6;
`llm_fitting/rankdiff_kalman.py` (`oos_movement`,
`sim_cohort_conditional`).

## Input

Claude adds an opt-in export to the gate code (off by default, runs
bit-identical when off): for each split, one table of per-endpoint
predictive samples — endpoint id, origin week, origin rank and permanent
rank, horizon, simulation rep, simulated rank and presence, observed rank
and presence. The exact schema will be in
`llm_fitting/handoff/A6_endpoint_export/SCHEMA.md` on `main`. Until it is
there, build and test the scorer against synthetic tables that follow this
description.

## What to build

1. **Scores.** A proper score on log-rank at horizons 1, 4 and 13 from the
   per-endpoint samples, with leaving the scored set treated as a censored
   state and the finite number of samples allowed for; say which
   correction you use and under what sampling assumption it is valid. A
   probability score for "outside the top-K within h weeks" and for the
   established cohort (origin permanent rank ≤ K/2). Calibration of the
   10th, 50th and 90th percentiles by horizon and by rank group.
2. **Baselines**, each producing the same per-endpoint output from training
   data only: no change; the historical-mobility law used by the gate,
   applied to every endpoint; a transition kernel by origin-rank bin.
   State for each what it can and cannot be compared on.
3. **Uncertainty.** Paired comparisons on the same origins and endpoints,
   with blocks of weeks resampled together. Five overlapping origins are
   not five experiments; say what the intervals do and do not cover.
4. **Known-answer tests** on synthetic data: the true model scores better
   than a misspecified one; a forecast that is right on average and blind
   to the endpoint ties the pooled baseline; the scores are unaffected by
   relabelling endpoints.

## Boundaries

- Code under `llm_fitting/validation/endpoint_scores.py`; the two model
  files are not edited.
- No gate evaluation of a non-frozen specification. The retrospective
  table uses the frozen movement stacks on the four primary panels.
- No new headline criterion. Label every table "supplementary,
  retrospective, development data".

## Deliverables

The handoff folder as in PROTOCOL §5; the scorer with tests in
`tests/test_validation_endpoint_scores.py`; a retrospective table of skill
against each baseline by horizon and rank group, with intervals.
`scripts/health_check.sh` passing.

## Done when

The scorer passes its known-answer tests and the retrospective table
exists, with a plain statement of whether the frozen model shows
endpoint-level skill over each baseline and where.
