# A1 and A2 — where the tails live, and what the missing departures are

_Written 2026-10-12 by Claude (Opus 5.5) before any cell of these tasks was
run. Work plan `WORKPLAN_CLAUDE_2026-10-12.md`, Phase 1._

Status: EXPLORATORY measurements on DEVELOPMENT data. No model code, no
gate evaluation, nothing adopted. The statistics are in
`llm_fitting/residual_anatomy.py` (known-answer tests in
`tests/test_residual_anatomy.py`); the runner is
`llm_fitting/phase1_anatomy.py`. Data and simulation go through the same
functions.

How to read the results is not settled by this file alone: Codex's task V2
plants each candidate mechanism in synthetic panels and reports which of
these statistics can tell them apart at these panel sizes. A statistic that
cannot separate the candidates there cannot adjudicate them here.

## A1 — where the tails live

**Panels and model.** FB Era A (K = 3,500, 86 weeks), Reddit comments
(K = 12,500, 136 weeks), long submissions (K = 5,000, 212 weeks). Universe:
the full-window top-coverage universe of the structure cards. Simulation:
each panel's frozen structure stack (the top structure rung of §2z-ap),
unconditional over the panel length, seeds 0–19, every slot tracked with
identities.

**Statistics.** `tails_by_horizon`: for h = 1, 2, 4, 8, 13, 26, 52 (a
horizon is skipped when the panel is shorter than 2.5 h) and three
permanent-rank groups (head: top 10% of K; middle: 10–50%; shell: 50–100%),
the quantile ratios q90/q50 and q99/q90 of standardized h-week changes net
of the common factor, the share beyond 3, and the 99th percentile of up and
down moves; standardized by each endpoint's own robust scale ("entity") and
by the group's ("group"). `retention`: the fraction of a one-week move still
in place after k = 1, 2, 4, 8, 13, 26 weeks, for big (beyond 2 robust SDs)
and small (inside 1) moves, up and down.

**Reported.** The data's value, the simulation's mean and SD over seeds, and
their difference in units of that SD. The headline is the middle group,
entity standardization, q99/q90.

**Predictions.**

1. At h = 1 the data's q99/q90 (entity, middle group) exceeds the
   simulation's by 5–20% on all three panels. (The record already has the
   data fatter at one week, §2z-x.)
2. The excess does not fade with horizon: at h = 13 the data-minus-
   simulation difference in q99/q90 is at least as large as at h = 1 on FB
   and on comments, and the simulation's own ratio has fallen to within
   0.10 of its Gaussian-walk benchmark by then. **This is the hypothesis of
   the work plan. If the excess is gone by h ≈ 4, the fat tails are in the
   passing component and candidate C-T is not licensed.**
3. Retention: at k = 13 big moves keep a larger fraction in the data than
   in the simulation, by at least 0.05, in the middle group on at least two
   of the three panels.
4. Asymmetry: in the data big drops are kept more than big rises at k = 13
   on FB and on comments; the simulation shows no such asymmetry.

## A2 — what the missing departures are

**Panels and model**, as recorded: the comments extension
(`reddit_comments_ext`, K = 12,500, cohort on weeks 0–135, scored on
136–212; §2z-ac) and long submissions (`reddit_submissions_long`, K = 5,000,
t0 derived from the anchor week 2021-07-05; §2z-ae P7). Train-only universe,
no survivor filter, the LONG stack fitted on the full universe, 30 seeds,
tracked slots split into identities, cohort = train permanent rank ≤ K/2
with presence ≥ 0.7, absent weeks at the recorded floor. These are the
settings of `exit_audit.py --aligned` and `--p7`.

**Statistics.** `departures` and `departure_summary`: every exit of the
cohort from the top-K, classed as

- *slide*: the median rank over the 13 weeks up to the exit was already
  beyond 0.75 K;
- *excursion*: not a slide, and back inside the top-K within 13 weeks;
- *lasting*: not a slide, not back within 13 weeks, with at least 13 weeks
  of panel left;
- *censored*: not a slide, not back, panel ends too soon;

with rates per cohort-week at risk, the margin to the cut the week before,
first exits against repeat exits, and the share back within 4, 13 and 26
weeks. The same on a within-train split (cohort on the first half of the
train window, scored on the second half). `long_dispersion`: spread of
changes at h = 13, 26, 52, 78 by rank group on the full-window universe.

**Predictions.**

1. Reproduction: the total rates at K/2 equal the recorded ones — data
   0.00800 and simulation 0.00350 on the comments extension; data 0.00851
   and simulation 0.00311 on long submissions — to the recorded precision.
   The new function is tested to compute the same estimand as
   `exit_audit.score_matrix`.
2. In the data, slides are the largest class on both panels (more than half
   of events).
3. The shortfall (data rate minus simulated rate) sits mostly in slides
   plus lasting exits (at least 60% of it), not in excursions.
4. The simulation under-produces lasting exits by a larger factor than it
   under-produces excursions, on both panels.
5. Within-train split: the data-to-simulation ratio is smaller than in the
   recorded split and still above 1.3 on both panels.
6. Long differences: the robust spread at 78 weeks over that at 26 weeks is
   larger in the data than in the simulation, in the middle group, on both
   panels.

**What each outcome licenses** (work plan, table A2). Mostly slides: a
lasting layer with slower memory, not jumps. Mostly sudden exits that last:
fat-tailed lasting shocks (C-T), together with A1 prediction 2. Mostly
excursions: a reversible regime, held in reserve. A shortfall that shrinks
in the within-train split: part of it is parameters moving between
windows.
