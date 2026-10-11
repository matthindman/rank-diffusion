# R — Review or blind check (standing brief)

Working rules: `llm_fitting/coordination/PROTOCOL.md` §9. The kickoff
message names what is to be reviewed or re-computed and the folder to write
to: `llm_fitting/handoff/R_<name>/` on branch `codex/r-<name>`.

## Reviewing a dossier or a change

You are given artifacts: a dossier, a diff, logs, commands. You are not
given the author's conclusions, and if you come across them, set them
aside.

Look for what would make the result wrong or the claim larger than the
evidence:

- identification: is each parameter carried by a stated moment, and can
  that moment tell this mechanism from the alternatives;
- leakage: does anything from the test window, or from a later look at the
  panel, reach the fit or the choice of specification;
- the observation mapping: are data and simulation built and scored the
  same way;
- counterexamples: a simpler explanation that predicts the same statistic;
- for code: flag-off identity, seeds, off-by-one in windows, cohort
  definitions (permanent rank, never current rank).

Report findings ranked by consequence. For each: the input or state that
produces the wrong result, what you ran to confirm it, and what would
settle it. Separate what you verified from what you suspect. One round; do
not re-review an unchanged result. Do not edit the files under review.

## A blind check

You are given a definition and a command or a panel, never a number.
Compute the quantity your own way in
`llm_fitting/validation/blind_<name>.py`, commit the result in your handoff
folder, and only then compare with the recorded value named in the kickoff
message. Report both numbers and any difference, with its cause if you
find it. A difference is a finding, not an error to be made to disappear.
