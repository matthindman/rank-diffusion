# V4 — IN PROGRESS: design and runner prepared; native study NOT RUN

2026-10-13 · OpenAI Codex · No change adopted.

The blind study has a committed initial design (`e12f948`) and a committed
frozen-benchmark correction (`ce3350c`). The final pre-study correction
replaces an aggregate absence probability with a measured permanent-rank
profile. It is recorded in DESIGN before any native study cell or native
absence-profile extraction. Small artificial fixtures have tested the code;
they are not the requested recovery map. Step 1's output is still blind.

## Prepared

- Ten rivals at the three prescribed K/T combinations, 8K endpoints each
  and 16K for replacement. Twenty evaluation seeds and twenty independent
  coverage-calibration seeds per rival/profile: 1,200 cells in total.
- The frozen home kernel is a V4-local copy of the existing independent
  simulator, with two output arrays added for the latent and amplitude
  layers. Tests compare every original output under shared seeds.
- Full registered long frames go through `mts_step1.analyse` and
  `mts_step1.score` unchanged. Source and input hashes accompany each
  atomic cell; an interrupted forecast cannot silently be replaced.
- A fixed classifier, full confusion matrices, all M1 score statuses,
  marginal-matching checks, held-out slope errors, and coverage targets
  with their Monte Carlo uncertainty. The reporter refuses an incomplete
  grid as a completed result and checks actual panel/population sizes.
- The slope predictor saves its movement-only forecast before generating
  the additional target block. It cannot read the M1 slope or R_diff fields.
- A one-process resumable launcher with caffeinate, the shared heavy lock,
  and one numerical-library thread. No empirical M1 path is opened by it.

The V2 parameter inputs were copied into the local ignored `inputs/`
folder and verified by checksum. `input_manifest.json` records their
source and calibration rule. Full empirical ladders are not staged.
The comments parameters come from the frozen 136-week fit and are
transported to 213 synthetic weeks. No new fit has been run.

## Validation and execution problems

The tests cover all ten generators' identity/rank schema and future-prefix
equality; the frozen kernel's original outputs; the OU lag-variance moment;
new identities on replacement and death alignment; movement-only slope
prediction; leave-one-paired-seed-out classification; independent coverage
targets; accounting-layer closure; restart identity checks; and rejection
of an incomplete grid or wrong native dimensions. A manufactured complete
grid exercises reporting only; its values are not study observations and
stay in the temporary test directory.

An initial fixture test exposed inconsistent in-memory versus JSON
representations on restart (NaN/null and numerical keys). The cell return
is now normalized by the same serializer as the checkpoint. The rerun
passed. No scientific seed was failed, killed, discarded or interpreted:
none has been launched. The final health check passes: 526 research tests
plus 13 subtests (including 21 V4 tests), 8 package tests, sealed-data guard,
and the legacy guard at 14/15 and churn 0.013. Results are in HEALTH_FINAL.log.

## Predictions and required results

| item | standing |
|---|---|
| P1 accounting and label invariance | NOT RUN at prescribed sizes/seeds |
| P2 false restoring signs | NOT RUN |
| P3 named-level recovery limits | NOT RUN |
| P4 replacement versus position | NOT RUN |
| P5 nuisance-only occupant loss | NOT RUN |
| P6 finite-horizon bridge error | NOT RUN |
| P7 held-out slope recovery | NOT RUN |
| P8 predictable-share interval coverage | NOT RUN |
| M1 A1–B10 and screen, every rival/profile | NOT RUN |
| Movement gate, every panel | NOT RUN; out of scope |

No recovery rate, power statement, mechanism verdict or step-3 license is
available. The eventual map must be committed before opening empirical M1
results. The script's completed grid is followed by a human-readable
interpretation and completion addendum to this report and STATUS_ENTRY.

## Reproduce and resume

From a user-owned Terminal:

```bash
bash /Users/hindman/.codex/worktrees/v4-planted-rivals/rank-diffusion/llm_fitting/handoff/V4_planted_rivals/launch_study.sh
```

The same command resumes complete atomic cells without rerunning them.
Its first stage measures only absence by permanent rank on the three
named DEVELOPMENT panels, using the registered loader and sealed-data
guard. The T9 drive is required for this stage; a missing drive stops it.
Saved nuisance profiles are immutable inputs on restart. The study then
runs the complete fixed grid and generates the recovery tables and the
machine-scored P1–P8 file. Logs and attempts stay in
`llm_fitting/runs/2026-10-13_V4_planted_rivals/`.

For a count-only progress snapshot from this working copy:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 llm_fitting/validation/balance_rivals_report.py --progress-only
```

No statistical interpretation is produced for a partial grid. A process
that is killed leaves a STARTED attempt without a finished event; its
unfinished cell is rerun from the identical seed. Completed-cell source
hash changes fail closed.

## Why the long run is waiting

AGENTS.md requires long runs from a user-owned Terminal when another agent
session is active, and says to give the owner the command when activity
cannot be determined. Claude processes are present on this machine.
The general coordination protocol's permission to launch long jobs does
not override that higher-priority condition. The concrete launcher and
tests are prepared; no permission to change a model or protocol is sought.

## Scope limits retained before results

Missingness is independent over weeks conditional on initial rank; annual
amplitude is the declared V2 stress value, not a fresh empirical estimate.
Some processes cannot preserve identical ladder curvature at fixed finite
variance. Their marginal mismatches are reported and bar a claim that
classification separated mechanisms at matched marginals. Coverage is for
the declared bin-population target with realised plug-in weights, not
exact conditioning on all endogenous centre/occupancy events. Twenty
calibration seeds may make that target too imprecise: the report must retain
the resulting NOT ADJUDICATED cases and coverage brackets. No nuisance or
threshold is tuned after a native study result.
