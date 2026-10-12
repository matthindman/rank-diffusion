# Answers to V3 questions

_Claude (Opus 5.5), 2026-10-12, on `main`._

**Q1 (timing of the export).** Your reading is right, and I confirmed it in
the code: `sim_cohort_conditional` applies a weekly transition at loop step
0, drops that row, and records from step 1, so the first recorded row is
two transitions from the filtered end-of-train state. The comment in the
source says step 0 is the last train week; the code has already advanced
it. My schema repeated the comment and was wrong.

What is on `main` now:

- The gate is unchanged, output for output. Whether its conditional
  simulator should record from the first transition is a question about a
  frozen gate and is the owner's; it is flagged in MODEL_STATUS §2z-bg.
- The export has a second array, `sim_rank_aligned` (and
  `export["rank_aligned"]` from the simulator): the same paths one
  transition earlier, rows of loop steps 0 to T_sim − 1. Row `w` is `w + 1`
  transitions from the end-of-train state and lines up with test week `w`.
  `sim_rank_aligned[:, w+1] == sim_rank[:, w]`. No random draw is added.
- `tests/test_endpoint_export.py` checks the identity above, the shapes,
  and that the simulator makes T_sim + 1 transitions for T_sim gate rows.
- `handoff/A6_endpoint_export/SCHEMA.md` carries the correction.

For the calendar-aligned tables, score `sim_rank_aligned`. Re-exporting
needs a rerun of the four frozen gates with the flag (the cached exports
predate the new array); those are frozen-stack runs like the ones you
already made and go in `GATE_ROWS.md` as before. One thing to watch: with
the aligned rows the passing component has had one innovation from zero at
row 0, because the simulator starts it at zero at the end of the training
window; say so where it matters for the one-week horizon.
