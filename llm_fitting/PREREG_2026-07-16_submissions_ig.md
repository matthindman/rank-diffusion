# PREREGISTRATION — Reddit submissions long panel (2018-12..2022-12) + Instagram 2023 dailies

Registered 2026-07-16, committed BEFORE any value-level analysis contact
with either dataset. **REQUIRED READING for any agent (Claude, Codex,
Gemini) before touching `reddit_{daily,weekly}_long.parquet` or
`full_ig.parquet`** — MODEL_STATUS §2z-ad is the pointer; AGENTS.md /
CLAUDE.md route every session through the newest MODEL_STATUS §2 sections.

## 0. Contact disclosure (exact, at registration time)

Inspected so far, NOTHING ELSE: file listings/sizes/mtimes; aggregation +
finalizer stdout logs; `reddit_monthly_processing_log.csv`;
`reddit_week_completeness.csv`; parquet SCHEMAS + row counts + the IG
date-column row-group min/max (metadata, no values); builder source code
(`build_reddit_panels.py`, `finalize_reddit_after_aggregation.py`). No
panel value has been read; no distributional statistic computed; the
0-byte `reddit_full_validation.json` means the output validator has NOT
run — it runs as Phase 0 below, after this document is committed.

## 1. What the data is (from logs/metadata/code only)

- **Submissions**: 49/49 monthly RS aggregates (2018-12..2022-12), all
  status ok / errors = 0 (A9-grade log check pending mechanically).
  Combined panels `reddit_daily_long.parquet` (142,438,022 rows) /
  `reddit_weekly_long.parquet` (53,723,472 rows), 212/214 complete weeks,
  weekly range 2018-12-03..2022-12-19. Schema = the 7 registered columns;
  builder sets **metric_value = max(submission_karma, 0) daily** — the
  submissions mirror of the comments convention (positive-part daily net
  submission karma; signed `submission_karma` is the audit column;
  comment fields carried alongside).
- **Instagram**: `full_ig.parquet`, 53,473,688 POST-LEVEL rows, calendar
  2023 (the daily complement of the existing weekly IG year), CrowdTangle
  export schema (contains text/url columns — SSD hygiene note recorded).
- Also observed (not this prereg's scope): a new `wikipedia/` tree on T9
  (raw months incl. a suspicious `2099-01` directory — flagged to owner).

## 2. Binding rules for the run (inherited, restated)

- Submissions 2021-07..2022-12 overlaps the comments confirmation era and
  the program has consumed the 2024-25 submissions panel: this run is an
  **outcome-unseen historical BACKTEST / cross-metric replication** —
  never "a new-platform confirmation" (§2z-w). Wikipedia remains the
  primary confirmation target and is never used to choose candidates.
- IG is known-mechanism censored: never calibrate to it; estimand
  language "of a-matching activity"; id = `user_name` (§2z-c).
- Trend test BEFORE any transport band (§2z-u registration lesson).
- One analysis pass per registered item; failures reported as findings;
  no refits; A10-style per-column intake semantics (roles swapped:
  submission_karma signed-audit, metric_value = its positive part;
  comment columns audit-only); frozen NNLS estimator; MOM_FLOOR 0.02;
  `caffeinate -dims` on all long runs.

## 3. Pre-registered predictions (scored PASS/FAIL in the report)

**Intake / instrument**
- P1: the output validator (Phase 0) passes: weekly = Σ daily exactly on
  every column; zero dup keys; zero negative `metric_value`/counts;
  212/214 complete weeks with the two partials excluded from weekly rows.
- P2: submissions is census-grade — day-guard flags ~0 days; smooth
  new-id inflow; top-of-ladder eyeball plausible.
- P3: universe K set by the K90 concentration rule alone, declared before
  any fit (owner precedent: fuller universes acceptable; rule, not
  number, is registered).

**Submissions science (the backtest core)**
- P4 — **the s-trend cross-metric replication (headline)**: temperament s
  estimated on three ~77-week sub-windows rises MONOTONICALLY across
  2019→2022 (the §2z-s era-dominant finding transported to a second
  metric). Falsifier: flat or declining. Level context (non-gating): old
  submissions panel recorded s ≈ 0.94 (metric-dependent).
- P5 — amplitude collapse: b(8) ∈ [0.95, 1.15]; s(h) flat in h.
- P6 — movement gate (5-split rolling, movement stack md6+t+temper+pool8
  + conditional state): at-par-or-better — pooled model rel err ≤
  baseline + 0.05. Context (non-gating): old subs gate 0.164 vs 0.168.
- P7 — **established-departure deficit replication (§2z-ac, the hardened
  finding, on outcome-unseen data)**: `exit_audit.py --aligned` analog
  (train window through 2021-06-28 for calendar comparability; train-only
  universe; symmetric identity construction; ≥20 seeds): sim
  under-produces K/2-cohort established departures by ≥1.5× (point
  expectation 2–3×) AND empirical departures are ≥90% returnable
  crossings. Falsifier: at-par rates or death-dominated empirical
  composition.
- P8 — head law (non-gating directional): sim S(1) > emp S(1) by >2
  seed-SD (20 seeds, ddof=0, within recorded top-M).
- P9 — VR decomposition transports: card VR13 excess positive with
  one-half to three-quarters functional under 50 phase-random surrogates.

**Instagram 2023 dailies**
- P10 — **Spec-B for IG** (the new capability): (i) daily-within-week
  noise floor scales ≈ 1/M in weekly post count (the §2z-c thinning law,
  now directly testable); (ii) centered floor curve lowest at the head,
  rising toward the tail (Reddit orientation); (iii) the §2z-c-recorded
  instagram_hm σ_obs sits INSIDE [centered floor, Spec-A] band-by-band;
  (iv) the instagram_hm_ts gate re-run with the Spec-B pin stays
  at-or-above the historical-mobility baseline (recorded 0.320 ± 0.189;
  predict pooled rel err within ±0.15 of it).
- P11 — instrument dropout (censoring layer 2): daily absence clusters by
  week far beyond binomial — week-correlated instrument dropout is
  directly visible in the 2023 dailies.

## 4. Analysis plan (registered; NOT executed at registration)

- **Phase 0 — gates**: run `validate_reddit_outputs.py` (the 0-byte JSON
  must become PASS); A10-style column-semantics + A9-grade log checks for
  the long panels; MANIFEST rows verified. ANY failure stops model
  contact (data problem, not a degree of freedom).
- **Phase 1 — forensics**: census profile + day guard + new-ids/week
  (submissions); IG daily-panel build from posts (`user_name`, dup
  audit, aggregate-safe columns only — text/url columns never leave
  raw_small), then the same profile classified as censored.
- **Phase 2 — universe registration** from concentration shares alone
  (P3), buffer 4×K, absence-penalized membership.
- **Phase 3 — the registered evaluations**, one pass, in order: P4 trend
  → P5 collapse → P6 gate → P7 exit audit → P8 head → P9 VR; then P10
  Spec-B → P11 dropout on IG.
- **Phase 4 — documentation**: dated MODEL_STATUS §2 section, predictions
  scored with the same prominence pass or fail; run archive + manifest;
  memory update.

Amendments to this document are legal only as dated commits strictly
before Phase 0 begins.
