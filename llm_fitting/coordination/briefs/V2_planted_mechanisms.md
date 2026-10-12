# V2 — Planted mechanisms and recovery

Branch `codex/v2-planted-mechanisms`. Handoff folder
`llm_fitting/handoff/V2_planted_mechanisms/`. Working rules:
`llm_fitting/coordination/PROTOCOL.md`.

## Why

The largest unexplained error is that the model makes too few large,
lasting moves: established endpoints leave the top-K 2.3 to 2.74 times too
rarely, and the upper tail of 13-week movement on Facebook is about 37% too
thin (MODEL_STATUS §2z-ac, §2z-ae, §2z-ap; `PROGRAM_STATE.md`, open
residuals 1 and 4). Claude is measuring the real panels with two sets of
statistics (tails by horizon and persistence by size and sign; a
decomposition of departure events). Those measurements will be used to
choose between explanations. That is only legitimate if the statistics can
tell the explanations apart at the real panel sizes, and do not fire on a
model that has none of them. `evidence-standards.md` §3 requires this
before any mechanism claim, and it is the first package of your agenda of
2026-10-08.

A second purpose: the estimator's recovery of its own parameters has never
been measured, and the ladder run found the reversion rate sitting on the
edge of its search grid for 43–80% of rank bands in two restricted fits.

## Read first

The skill and `references/evidence-standards.md` §3 and
`references/pitfalls.md`; MODEL_STATUS §1 (the model as coded) and §2z-ap;
`RESEARCH_AGENDA_2026-10-08.md` package 1 (yours) and
`RESEARCH_AGENDA_CLAUDE_2026-10-08.md` B1 and W1 rows 0–3 and 7;
`llm_fitting/residual_anatomy.py` once it is on `main` (Claude delivers it
during this task; until then build parts 1 and 2 below).

## What to build and measure

1. **An independent simulator.** Written from the model statement in the
   skill and MODEL_STATUS §1, in `llm_fitting/validation/planted_sim.py`,
   without importing `simulate` or the cohort simulators. It produces
   panels in the registered schema. A fixed-band version first; rank-band
   profiles after.
2. **Recovery of the baseline model** (an ADEMP study, written down in
   `DESIGN.md` first). Data-generating mechanism: the baseline model at the
   frozen parameter values of each long panel. Through the real pipeline:
   universe selection, permanent-rank estimation, the minimum-distance fit.
   Estimands: s (including how often s > 0 is inferred when the truth is
   0), κ (and the share of bands at a grid edge), σ_perm, σ_trans, σ_obs,
   the t degrees of freedom, and the stationary variance
   V = σ_perm²/(2κ). Performance: bias, spread, edge pile-up, with Monte
   Carlo standard errors. Panel lengths 86, 136 and 212 weeks.
3. **Cross-check of the two simulators.** Baseline panels from your
   simulator and from `minimal_rankdiff.simulate` at the same parameters:
   do the nineteen macro statistics of §2z-ap agree within seed bands? A
   disagreement is a finding about one of the simulators; report it, do not
   resolve it by editing ours.
4. **Planted mechanisms.** Four alternatives, each at two strengths, plus
   the baseline:
   - lasting shocks with fat tails at unchanged variance;
   - a reversible "off" regime with an onset and a return hazard;
   - a lasting layer with slower-than-exponential memory (for example two
     reversion rates);
   - passing shocks bounded in the far tail;
   - (added 2026-10-12, before you started this task) an endpoint whose own
     volatility changes over time: log-volatility as a slow AR(1) around
     its amplitude, with the average variance unchanged and Gaussian
     shocks. The tail statistics cannot be read without it: scale mixing
     over time fattens an endpoint's own changes at every horizon, as
     fat-tailed lasting shocks do;
   - (added the same day) a lasting level with no reversion at all, a pure
     random walk, as the limit of the slower-memory case.
   - (added 2026-10-12 after you branched and before your `DESIGN.md` was
     committed; if the design is already fixed when you read this, add it
     as a dated addendum case and say so) a lasting level pulled toward
     the level the ladder assigns to the rank it holds NOW, with no pull
     toward where it started: each week, level += strength x (ladder value
     at its current rank among lasting levels - level) + shock. Two
     strengths. Endpoints can change places for good while the set of
     levels stays near the ladder. The question for this case: can the
     statistics tell it from the baseline's pull toward a fixed home, and
     from the pure random walk, and does the estimator's fitted reversion
     rate distinguish the three?
   - (added the same time) an endpoint-specific annual cycle on top of the
     baseline: amplitude lognormal across endpoints, phase uniform, two
     sizes. `momentum(..., gaps=...)` and `long_dispersion` are the
     statistics that should see it; the fitted reversion rate is the
     estimand it may distort.
   For each, run the statistics in `residual_anatomy.py` on the simulated
   panels and report, per statistic and panel length: the separation from
   the baseline in units of seed spread, which pairs of mechanisms it
   cannot tell apart, and its false-alarm rate on the baseline.

## Boundaries

- No real panel is fitted or scored in this task, except to read the
  frozen parameter values that seed part 2. No gate evaluation.
- The two model files are not edited (PROTOCOL §2).
- Strengths of the planted mechanisms are fixed in `DESIGN.md` before any
  run and are not adjusted afterwards to make a statistic work.
- Do not open Claude's results for the real-panel measurements (A1, A2)
  until your part 4 is committed: the point is that the reading rules exist
  before the readings do.

## Deliverables

The handoff folder as in PROTOCOL §5; the simulator and study code under
`llm_fitting/validation/`; tests in `tests/test_validation_planted_*.py`
(a fixed-band case with a known covariance; each planted mechanism
detectable at high strength). `scripts/health_check.sh` passing.

## Done when

There is a table that says, for each statistic, which explanations it can
and cannot separate at 86, 136 and 212 weeks, and a recovery table for the
baseline estimator. Stop and report instead if the two simulators disagree
on the baseline beyond seed bands: that outranks the rest of the task.
