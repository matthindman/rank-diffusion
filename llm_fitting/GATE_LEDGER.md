# Gate ledger — every out-of-sample gate evaluation, as it is run

_Started 2026-10-11. Append-only: one row per gate evaluation, added in the
session that ran it, whether or not the result is used._

The rolling-origin movement gate adjudicates adoption, and it is evaluated
on the same few panels again and again. Split-to-split SD is 0.03–0.06,
about the size of the differences being judged, so the best of many
specifications can look better by selection alone (MODEL_STATUS §2z-ai;
`lit_review/research_notes/…/validation_forecast_evaluation.md` §8). The
number of evaluations per panel is the multiplicity a referee will ask
about. This file makes it countable.

Rules:

- Log every evaluation, including abandoned specifications, reruns with more
  seeds, and runs that crashed after producing numbers. A run that produced
  no numbers gets a row saying so.
- Log before interpreting. Do not edit or delete rows; correct with a new
  row that names the one it corrects.
- `kind` is one of: `registered` (a protocol item), `candidate` (a proposed
  change against the incumbent), `reproduction` (re-running a recorded
  result), `exploratory`.
- An evaluation run by the owner or by another session and reported in
  conversation is logged by whoever first records it, marked second-hand,
  with its source.
- Evaluations before 2026-10-11 are not backfilled. They are recoverable from
  MODEL_STATUS §2 and `llm_fitting/runs/`; a backfill is a separate task.

| date | session | commit | panel | kind | spec (flags beyond defaults) | splits × reps | model rel err ± SD | baseline ± SD | coverage | log / MODEL_STATUS § | note |
|---|---|---|---|---|---|---|---|---|---|---|---|
