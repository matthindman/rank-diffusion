# Phase-0 intake gate — execution correction (2026-07-16)

**Status: disclosed post-contact implementation correction. No Phase-0
verdict has ever been emitted. All registered inputs, hashes, invariants,
and thresholds are UNCHANGED.**

## What happened

Two Phase-0 runs of `check_long_panels.py` were terminated before the
checker printed any result:

- attempt 1 (pid 44477): ran ~1h24m, terminated ~13:55:54, 0 bytes emitted.
- attempt 2 (pid 89974): ran ~3m18s, terminated ~14:01:40, 0 bytes emitted.

Neither produced a PASS/FAIL or any invariant output.

## Attribution — withdrawn

An earlier note in this session attributed the terminations to a "periodic
`pkill python` from a parallel Codex session (~2–3 min killer)." **That
attribution is withdrawn as unsupported.** The macOS `caffeinate ClientDied`
log lines only record that `powerd` saw the assertion-holder exit; they do
not identify what killed Python. The task status `killed` establishes
SIGKILL-like termination, not who issued it. No crash report and no
jetsam/OOM entry excludes neither memory pressure nor job-supervisor
termination. Three heterogeneous exits do not establish a periodic sweeper.
**The immediate termination signal for each attempt is unresolved.**

## Real defect found (independent of the killer)

The original checker was NOT bounded-memory despite its docstring claiming
"never loads the daily panel whole." It accumulated the cumulative
`(entity, week)` aggregate (`daily_sums`) across the entire stream — the
8-batch merge bounded only the number of un-merged fragments, not the
retained aggregate, which grows to the full panel's entity-week cardinality
= **53,723,472 rows** (verified from `reddit_weekly_long.parquet` metadata).
That both (a) contradicted the registered "streaming / never loads the full
panel" description and (b) drove an O(N²/8) incremental re-merge. Peak memory
therefore scaled with the panel; on the run host this was plausibly fatal and
was certainly non-viable. Credit: external (Codex) review; verified in code
and metadata before adopting.

## The correction (invariants unchanged)

`check_long_panels.py` was rewritten to bounded one-week streaming:

- daily is read one parquet row group at a time; per-`(entity, week)`
  aggregates are finalized and compared to the weekly panel as soon as the
  read frontier (next row group's min date; row groups are date-ordered
  non-overlapping ascending, verified) passes a week, then discarded.
- the weekday-presence OR-mask is vectorised (per-bit max) and popcount uses
  a lookup table — the per-group Python lambdas are gone.
- the weekly panel is read per complete week (order-robust) and reconciled
  by exact `(entity)` index equality + per-column equality; weekly week-set
  == the consecutive complete weeks is enforced by
  `want-coverage + weekly.num_rows == matched-cells`.

Retained state is now bounded by the row-group day span (a couple of weeks),
independent of the number of weeks. Every A1.1 invariant, threshold, input
path, and the IG member SHA are unchanged; only the execution changed, and it
moves *toward* the registered "never loads the full panel" contract.

## Verification

- All 7 pre-existing adversarial fixtures + 13 subtests still FAIL-closed
  (equivalence).
- New tests: multi-row-group week-boundary PASS; straddling value-mismatch
  FAIL; retained-state bounded and INDEPENDENT of #weeks (max open weeks
  equal at 8 and 40 weeks, ≤ 4).
- Full repo suite: 177 passed + 13 subtests.
- Real-data confirmation (this run): RSS oscillates 0.6–0.9 GB (not
  climbing), `open weeks == 1` throughout, steady row-group progress.

The corrected gate was relaunched fail-closed (direct redirect, no `tee`, so
the checker's real exit status propagates) via `phase0_launch_cmd.sh`.
