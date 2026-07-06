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

## 6. AMENDMENT A1 (2026-07-05, same day, BEFORE any data processing): κ_i secondary diagnostic + surrogate-adjusted residual reference

Registered per the revised external verdict (rec. 4 / B4) and MODEL_STATUS
§2v. No extension data has been read at the time of this amendment (WD drive
still unmounted).

E4. **κ_i concentration diagnostic (secondary; does not gate E1/E2).**
    On the EXISTING panel (train), compute per-entity curvature
    κ̂_i = EB-shrunken log VR13 residual after 5×5 rank×volatility cell
    demeaning (`surrogate_test.kappa_probe` machinery; shrinkage factor =
    the measured split-half signal share). On the EXTENSION, compute the
    same per-entity log VR13 residual for the shared entity set. Registered
    test: Spearman(κ̂_i^train, resid_i^ext) and the quintile concentration
    ratio (mean |resid| in the extreme κ̂_i quintiles ÷ middle quintile).
    - PRE-DECLARED READING: κ_i is "predictive" if Spearman ≥ 0.20 AND
      concentration ratio ≥ 1.3. Only if E4 passes does a per-entity κ_i
      model layer get implemented — as a NEW pre-registered step (train-only
      EB, hard shrinkage, no score-tuned parameters), adopted only if it then
      improves the frozen OOS movement gate. If E4 fails, κ_i stays a
      bounded limitation (log-SD ≈ 0.30, §2v) and no layer is added.

E3 reference amendment: the comments in-sample VR residual is evaluated
against the SURROGATE-ADJUSTED target (§2v): the functional component
(≈ +0.04 of the +0.08 VR13 gap) is expected to reproduce on the extension
under any spectrum-equivalent dynamics; only the residual beyond the
surrogate band counts as evidence of missing dynamics.

## 7. AMENDMENT A2 (2026-07-06, BEFORE any data processing): stationary head-law diagnostic

Registered per MODEL_STATUS §2z-a/§2z-b (metrics-audit finding). No extension
data has been read at the time of this amendment (WD drive still unmounted;
T9 only). This amendment is registered only if committed before the
DATA_PHASE2_REPORT.md resume command is run.

E5. **Stationary head-law diagnostic (secondary; does not gate E1/E2).**
    On the EXTENDED panel with the registered E3 stack, compute via
    `community_metrics`: S(1) and S(10) time-mean top-share (emp and sim,
    ≥10 seeds) and the head ladder offset (mean sim−emp time-mean log-value
    over ranks 1–600). REGISTERED BASELINES (2026-07-06, T9 panels,
    structure-primary stacks, 10 seeds): FB Era A — emp S(1) 0.0170 /
    S(10) 0.1011, sim S(1) 0.047–0.051 / S(10) 0.151 (a confirmed ~2.7×/+50%
    overshoot; head offset +0.35..+0.46 log, stable across panel thirds =
    stationary law, not drift; mechanism attribution: mix-b setting moves
    S(1) only 0.051→0.041 at b=0, so the base (κ, σ_perm) head partition
    carries the bulk). Comments T=136 — emp S(1) 0.1057, sim 0.1397 ±
    0.0396 (directionally consistent, WITHIN seed noise — not confirmed).
    - PRE-DECLARED READING: the overshoot is "cross-platform structural" if
      the extension sim S(1) exceeds the empirical value by more than 2 sim
      seed-SDs in the same direction. Only then does the candidate fix get
      implemented — as a NEW pre-registered step: an Eulerian stationarity
      moment (empirical stationary band variance / head ladder) appended to
      the MD partition objective, OPT-IN like --md-vr (no new components;
      removes partition freedom), adopted only if the in-sample cards hold
      within one metric and the frozen OOS movement gates do not degrade.
      If E5 does not confirm, the head-law excess is recorded as an
      FB-measurement-regime residual and reported as a limitation; no
      model change.
