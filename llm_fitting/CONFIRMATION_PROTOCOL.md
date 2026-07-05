# CONFIRMATION PROTOCOL — comments extension 2021-07..2022-12 (registered 2026-07-05)

This document REGISTERS the confirmation run on the unprocessed Reddit
comments extension (review C8/R6; MODEL_STATUS §2q handoff P2). It is
committed BEFORE any extension data is aggregated or read. **No threshold,
tolerance, flag, or universe rule below may change after any extension row
has been looked at.** Amendments are permitted only as dated commits made
strictly before data processing begins.

## 1. Frozen code and environment

- Frozen evaluation code: commit `682d03f` (2026-07-05; §2r centered-floor
  cutover + §2s stack freeze included). Later commits may add diagnostics but
  MUST NOT alter estimation/simulation defaults — enforced by the legacy guard
  (facebook 14/15 / churn 0.013) and the full test suite (45 passed at
  registration).
- Interpreter: `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`.
- Spec-B convention: the CENTERED (invariant) floor pᵀCΣCp is the pinned
  σ_obs quantity (`spec_b_curve` default; §2r). The legacy min-norm floor is
  reproduction-only.
- OOS gate denominator rule: MOM_FLOOR = 0.02 (§2q).

## 2. Data construction (registered before processing)

- Source: WD drive ("/Volumes/My Passport for Mac"), Pushshift monthlies
  `RC_2021-07.zst` .. `RC_2022-12.zst`; aggregation command as recorded in
  DATA_PHASE2_REPORT.md (comments-only resume, `--start 2021-07 --end 2022-12`).
- Panel build: the UNCHANGED existing pipeline (same scripts, same schema
  contract, same weekly=Σdaily invariant checks) produces
  `reddit_comments_2018-12_2022-12_{daily,weekly}` extension panels.
- Universe rule: K = 12,500, buffer B = 4×K = 50,000 via `restrict_universe`
  (top-coverage, absence-penalized membership over the FULL extended window);
  membership sensitivity reported at trailing-60-week windows exactly as in
  §2g-X P5 — reported, not used for selection.
- Census check: instrument_eras day-count guard must flag 0 days (Pushshift
  census property, §2g-X). If days are flagged, STOP and report — that is a
  data problem, not a modeling degree of freedom.

## 3. Registered evaluations (the ONLY evaluations run on the extension)

E1. **Parameter transport** (re-estimate on the extension segment
    2021-07..2022-12 alone, registered stacks):
    - temperament `s` (temper, min_changes=12)
    - mix exponent `b` (s(h*)/s(1))
    - κ(z) head/mid/tail at md6
    - Spec-B centered floor curve (12 bands, day_guard off — census)
E2. **Frozen-parameter movement gate** (single untouched future block, review
    C5): estimate on the existing T=136 panel (2018-12..2021-06) with the
    §2s comments movement-primary stack (md6 + t + mix + conditional state);
    forecast into the extension's first 34 weeks; persistence baseline and
    bootstrap-CI coverage exactly as in the recorded gate.
E3. **Descriptive card + bands** on the extended panel with the §2s comments
    structure-primary stack (LONG stack), reported via `scorecard_bands.py`
    (15-row card + entity/block bootstrap bands + MC bands + omnibus Q).
    Descriptive: no pass/fail criterion attaches to E3.

## 4. Pass criteria (pre-declared)

- E1 transport PASSES if: s ∈ [0.64, 0.74] (registered 0.692 ± the era-
  replication tolerance); b ∈ [0.95, 1.15] (registered 1.08; block-bootstrap
  CI width from §2t applies); κ(z) retains the declining head→tail shape at
  matched md_lags; the centered Spec-B floor at matched ranks is within ±25%
  of the registered curve (0.071 head → 0.248 deep tail).
- E2 PASSES if: model rel err ≤ persistence rel err + 0.05 AND bootstrap-CI
  coverage ≥ 60%, with per-split values reported (MOM_FLOOR rule active).
- Whatever the outcome, the result is REPORTED AS CONFIRMATION EVIDENCE —
  pass or fail. Failures are findings about the law's domain, not prompts to
  refit. Any post-hoc analysis of a failure is labeled exploratory and may
  not amend this protocol retroactively.

## 5. Status

- 2026-07-05: registered. WD drive not mounted this session — aggregation
  NOT started; no extension data has been read. Owner action required:
  mount the WD drive and run the DATA_PHASE2_REPORT.md resume command, then
  execute §3 exactly.
