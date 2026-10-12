# V2 preflight

- Started from main 4ae73b43b852ac540cadd39808f27200885f1be7, after handing over
  V1 at 4d3a818. The untracked V1 seed archive remains local; it is not V2 work.
- The V2 blindness requirement overrides reading the newest PROGRAM_STATE and
  MODEL_STATUS entries, which now expose A1/A2. Their pre-A1 state was read in
  V1; §1, §2z-ap, the specified agendas, current exposure ledger, sealed list,
  skills and pure measurement source are read. No A1/A2 results or run-folder
  contents have been opened. Pure phase1_anatomy measurement functions were
  read to align observation conventions; report functions/results were not.
- The brief's phrase 'four alternatives' is stale: its six enumerated
  alternatives, including the two additions, govern. All are included.
- The exception for reading frozen parameter values is interpreted to allow
  reproducing the exact frozen full-panel structural fit solely to serialize
  its complete parameters. Stored ladder summaries omit w0, exits, loadings
  and other required inputs. No empirical score or residual is calculated.
- Native profile/length pairs are used; this is not a 3-by-3 length factorial.
  A2 departure rules are calibrated at a declared half-panel split because
  its extension-specific split does not exist in every native panel.
- The owner's instruction to proceed within each brief and commit its handoff
  governs the research outputs. Compact study summaries and reviewable figures
  belong to the handoff; large simulation banks, caches and raw panels stay
  local. No push. Canonical records and protected models are never edited.
- Repository health checks are prerequisite software checks, not V2 study
  measurements. DESIGN is committed before the first synthetic study or
  frozen-parameter extraction. The owner authorizes our own restartable jobs
  under the shared lock; no Terminal launch permission is requested again.
- Source inspection resolved a preparation error before any run: load_panel
  retains zero metrics. The synthetic recovery mapping therefore retains
  zero rows and clips negative simulated log activity to metric zero; it does
  not delete those observations. This may itself affect recovery and is logged.
- The native simulator indexes coefficients by latent-level rank whereas the
  estimator uses time-mean observed-rank bands. The independent implementation
  states that convention explicitly. It is a separate implementation, not a
  blind-to-source exercise: production code was necessarily read during V1.
- The daily Spec-B instrument is outside this weekly structural-stack study;
  no daily noise pin is invented. Local variance functionals are not asserted
  to equal global stationary endpoint variance under rank-dependent dynamics.
- The sandbox initially lists the main checkout, not this assigned copy.
  Scoped execution escalation is used for writes/runs here. Nothing is written
  through the data symlink into Claude's checkout or onto the external drive.

- Execution-only refinement before recovery: independent native profiles may run
  concurrently, bounded by the shared three-slot heavy lock. Each stage waits
  for all profiles before the next; seed counts and all scientific choices stay
  as declared. Full parameter files contain empirical seed ladders and remain
  local, with hashes and compact profile summaries committed for review.

- The queue was rechecked during V2. Main had amended the brief with two
  additional cases after branching. Its explicit dated-addendum instruction
  is followed: original running code/choices remain fixed; added cases and
  predictions are committed before their first run. No A-task results read.

- Execution interruption: repeated all-NaN warnings exposed quadratic warning-count
  bookkeeping in my runner. Three diagnostic workers were terminated, three
  queued jobs cancelled, and two prior extension fit cells archived unused.
  All 120 completed baseline/s=0 recovery cells remain valid. An opt-in
  wrapper captures each warning message once, stamps its hash, and replays
  the identical seeds; no simulator/statistic/source identity is rewritten.
