# V4 — Planted rivals for the balance accounting, and a recovery map

Branch `codex/v4-planted-rivals`. Handoff folder
`llm_fitting/handoff/V4_planted_rivals/`. Working rules:
`llm_fitting/coordination/PROTOCOL.md`.

## Why

The owner adopted the movement-to-structure agenda on 2026-10-13
(MODEL_STATUS §2z-bp). Its step 1 measures, on the development panels, what
holds the rank-size ladder in an accounting sense: what the occupants of the
top k lose, what reordering puts back, what flow adds, and whether the loss
is a slide common to everyone, drift that depends on position, or a return
toward an endpoint's own earlier level. Those measurements will be read as
evidence about mechanism. That is legitimate only if the statistics can tell
the mechanisms apart at the real panel sizes and do not find one that is not
there. This task is step 2 of the agenda: it supplies that test. It is the
package your own agenda put second.

## Read first

`RESEARCH_AGENDA_MOVEMENT_TO_STRUCTURE_2026-10-12.md` (now adopted);
`MTS_PROTOCOL_STEPS_1_2_2026-10-13.md` (v2, revised after your review;
`handoff/R4_mts_protocol/ANSWERS.md` says what changed); `llm_fitting/balance_accounting.py`
and `llm_fitting/mts_step1.py`; `tests/test_balance_accounting.py`;
`references/evidence-standards.md` §3; your own `planted_sim.py` and the V2
report.

## What to build and measure

1. **Planted panels in the registered long schema** (`entity_id`, `period`,
   `rank` among everything present that week, `X`, `N`), from your simulator,
   at the sizes of the three panels of the protocol: 86 weeks with K = 3,500;
   213 weeks with K = 12,500; 212 weeks with K = 5,000. Each panel needs a
   population well below the cutoff, because the accounting follows whoever
   climbs in: at least 8K endpoints, more where the rival needs a reservoir.
   Each rival carries the real nuisance structure: passing swings with the
   memory and size of the frozen fits, fat-tailed weekly changes, an
   endpoint-specific annual cycle on the Reddit-sized panels, and missing
   weeks at the measured rates.
2. **The rivals.** Each is tuned, by a rule you write down first, to a
   similar ladder and a similar lasting variance, so that the statistics and
   not the marginals have to separate them:
   - the frozen model itself: a personal home with reversion;
   - fixed identities with no dependence on position and no home;
   - growth stabilised by replacement from below, with no dependence on
     state: a reservoir of smaller endpoints, births at the bottom, deaths at
     random;
   - drift that depends on position only;
   - a level with no stabiliser at all;
   - a slow and a fast component of equal stationary variance. The statistics
     must give the same answer whichever of the two is called the signal;
     say whether they do. This is an invariance check, not two classes;
   - (added 2026-10-13 after your review R4) a level with drift that depends
     on position only and no personal home, as a plain OU, and random walks
     with persistent noise at the memory the frozen fits and the long panels
     support. These are your own counterexamples; they belong in the map.
   If you think a rival is missing that would embarrass the design, add it
   and say why.
3. **One code path.** Every planted panel goes through
   `mts_step1.analyse(df, K, block)` unchanged, with the block lengths of the
   protocol. Do not re-implement the statistics for this task; a separate
   blind re-implementation of the accounting identity is welcome as a check
   and goes in `llm_fitting/validation/`.
   The classes to be told apart are those that differ in what a slope
   prediction would need (what the common slide is and whether the map
   S = v / (2s) recovers the slope), not every label above.
4. **The recovery map.** Per rival, panel size and statistic, the
   distribution over seeds (at least 20, seeds 1000 and up) of: the
   per-endpoint rates g, r, f and the shape term at each horizon and k; the
   ratio of r to its small-step value; the cohort bin means against the
   centre and their spread across bins; the two carrier slopes; the
   predictable share. Then a classification rule, fixed in `DESIGN.md` before
   any panel is generated, that names a rival from those statistics, and its
   confusion matrix: recovery and false identification, with the uncertainty
   of each rate. Also the status that `mts_step1.score` returns for every
   prediction and for the screen under each rival, and the coverage of the
   predictable share's interval.
   **Held-out slopes** (added after R4): for each rival, predict the local
   slopes of a block's ladder from movement-only inputs by the map of the
   protocol's section 8, and report the error against the planted truth. The
   protocol claims identification only where this succeeds.
5. **The questions the map must answer.**
   - Does a pure random walk with realistic passing swings ever show a
     restoring slope or a difference between bins? How often?
   - Is a personal home at the frozen reversion rates detected at 212 weeks
     with four 52-week blocks? At 86 weeks with three 26-week blocks?
   - Can replacement from below be told from position-dependent drift?
   - How large is the occupants' loss on block positions that comes only from
     what is left of the passing swings in a block mean, and does the
     predictable share separate it?
   - How far is the reordering gain from its small-step value at a 52-week
     step, by rival?

## Blindness

Do not open `llm_fitting/runs/2026-10-13_M1_balance/` or the MODEL_STATUS
entry that reports step 1 until your recovery map and classification rule are
committed. The rivals are tuned to frozen parameters and to ladders already in
the record, not to step 1's output.

## Predictions

Write them in `DESIGN.md` before generating anything: which statistics you
expect to separate which pairs, where you expect the design to fail, and a
precision target for the confusion matrix. Fix the seed count and the stopping
rule there.

## Not in scope

No gate evaluation. No fit of the production estimator is required; if you use
frozen parameters to set a rival, say which. No contact with sealed data. No
edit to `balance_accounting.py` or `mts_step1.py`: if a statistic is wrong or
a hook is missing, write it in `QUESTIONS.md` with the input that shows it.

## Compute

Planted panels at these sizes are large. Write the study as a launch script
that skips work already on disk, run long jobs through
`scripts/heavy_lock.py` under `caffeinate -dims`, and say in `REPORT.md` what
was killed or not run.

## Handoff

As in the protocol §5: `DESIGN.md` committed before any result, `REPORT.md`,
`STATUS_ENTRY.md`, `GATE_ROWS.md` (empty, with a line saying no gate was
evaluated), `EXPOSURE_ROWS.md` if frozen parameters were extracted from a
panel, and `QUESTIONS.md` if anything blocks.
