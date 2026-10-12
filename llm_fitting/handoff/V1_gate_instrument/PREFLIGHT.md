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
