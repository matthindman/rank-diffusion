# V1 preflight

- Owner's current instruction overrides the brief's terminal-only launch line:
  run restartable heavy jobs here under caffeinate/heavy_lock; supply a launch
  script only if this environment cannot keep them alive.
- Protocol overrides the skills' direct record/ledger edits: only handoff rows
  and STATUS_ENTRY are written; Claude integrates them.
- Exact recorded settings use seeds 0 onward for reproduction. Independent
  instrument banks use 1000 onward. This is intentional, not a blind check:
  the brief and canonical record already supplied expected reference values.
- Synthetic unit tests and repository health checks are not V1 measurements;
  DESIGN is committed before the first task experiment.
- Relative-error medians require pooled distributions, not just per-rep moments.
  The wrapper preserves distributions and scale selection. Fixed-scale noise
  is reported separately rather than silently freezing calibration.
- Initial health: research 414 passed + 13 subtests (one existing warning),
  package 8 passed, legacy 14/15 with churn 0.013; sealed guard self-check passed.
- The assigned working copy lies outside the initial sandbox writable roots.
  Authorized commands therefore request scoped execution escalation; no writes
  are made in Claude's checkout, including through the data symlink.

- A second health check was initially queued behind Claude's three jobs.
  It was cancelled before execution and rerun without heavy_lock because
  the initial completed check demonstrated a duration below five minutes.
  This is not an interrupted experiment or a gate result. Its wait log is
  retained; all gate runs still use the heavy lock.

- The brief describes seven scored moments, but the frozen gate includes
  only horizons strictly shorter than its test window. Short submissions
  has a 13-week test window, so dRank13 is structurally unavailable and the
  maximum vector there has six moments. This is reported as unavailable,
  not as a MOM_FLOOR exclusion, and the gate is not changed.

- Before any seed-bank resampling, the analysis was made memory bounded by
  exact weighted-CDF binary search rather than materializing a dense
  bootstrap-by-rank histogram. A known-answer test checks identical pooled
  quantiles. Additional output separates calibration reselection, Jensen
  bias, and displacement of the mean simulated moment from the 80-seed
  reference; the latter is a finite-bank diagnostic, not bias against truth.

- One analysis health-check invocation mistakenly used zsh on the Bash
  script. The research suite passed (423 tests + 13 subtests), then Bash's
  PIPESTATUS access failed under zsh before package/guard execution. The
  complete check is rerun with Bash; the failed harness log is retained.
