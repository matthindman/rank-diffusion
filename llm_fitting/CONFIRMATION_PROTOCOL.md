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
- 2026-07-12 (readiness check, MODEL_STATUS §2z-n): the owner-side
  aggregation resume RAN 2026-07-11T22:48Z..2026-07-12T10:46Z (18/18
  months ok, errors = 0 per the processing log). TIMELINE DISCLOSURE:
  amendments A6–A9 were committed after that mechanical aggregation began
  but derive exclusively from frozen-panel dry runs, code audits, and
  synthetic tests — no extension observation has been read by any analysis
  (verified: only log/manifest metadata inspected). "Data processing" in
  §0 is construed as ANALYSIS CONTACT for amendment validity; the owner
  must explicitly acknowledge this construction before E1–E5 runs.
  WARNING: the pipeline also built
  `reddit_comments_2018-12_2022-12_weekly.parquet` with the Monday-fold
  builder — it is PRESUMED to contain the A6.1 boundary fold and is NOT a
  registered input; the registered weekly panel is produced by
  `build_extension_weekly.py` from the frozen weekly + the extended daily,
  then gated. Frozen-baseline hashes pinned (manifest): weekly
  b00ee41f…0041, daily 19ea5eeb…2323.

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

## 8. AMENDMENT A3 (2026-07-11, BEFORE any data processing): E5 share-denominator clarification

External review (2026-07-11) noted the S(k) statistics are computed within
the RECORDED top-M rank-size slice (M = 2,000 at the standard settings), not
over the whole tracked/census universe, while §7's prose could be read as
whole-universe shares. Clarification, registered before any extension row is
read (WD drive still unmounted at the time of this amendment):

- Every S(1)/S(10) quantity in A2/E5 — the registered baselines AND the
  extension readouts — is a share WITHIN THE RECORDED TOP-2,000, empirical
  and simulated computed with the same denominator (`community_metrics.
  top_share`, ranksize width M=2,000 at K=12,500/B=50,000). The E5 diagnostic
  and its 2-seed-SD trigger are UNCHANGED (denominator-consistent both
  sides); only the labeling is corrected.
- Reporting rule going forward: S(k) values are always labeled "within
  recorded top-M" with M stated; cross-platform absolute comparisons must
  hold M fixed. (The 2026-07-11 code labels in community_metrics.py were
  updated to print this.)

## 9. AMENDMENT A4 (2026-07-11, BEFORE any data processing): estimator re-freeze under exact NNLS (owner adoption of Option A)

No extension data has been read at the time of this amendment (the WD drive
is mounted but untouched by any analysis session). Registered per the
2026-07-11 external review (finding 2 + round-3 recommendation) and the
owner's explicit adoption decision; full audit in MODEL_STATUS §2z-e/§2z-g.

- **The frozen scientific estimator for E1–E5 is the exact-NNLS MD solve**
  (`minimal_rankdiff._solve_nonneg` with nnls=True — the code DEFAULT as of
  the commit containing this amendment). The pre-2026-07-11 clipped-OLS
  convention is retained solely as the reproduction/sensitivity arm
  (`--legacy-clip`) and is NOT used in any registered evaluation.
- §1's frozen-code reference (682d03f) is superseded for the estimator by
  the commit containing this amendment; all other §1 items (interpreter,
  Spec-B centered floor, MOM_FLOOR) are unchanged. The legacy guard
  (facebook default: 14/15, churn 0.013) is unaffected — the v4.3 path does
  not use the MD solver.
- E1/E2/E3 registered bands and criteria are UNCHANGED and now apply to the
  NNLS estimator. The controlled audit (2z-e; PREREG written before any
  NNLS run) found: paper-primary FB Spec-B + conditional gate
  convention-robust (0.118 → 0.123 ± 0.033); cards within one knife-edge
  row; subs conditional at-par under NNLS (0.164 vs 0.168; the legacy
  "beats 4/5" is reported as legacy-convention sensitivity). E1 parameter
  bands (§4) were set from legacy-convention estimates; the E1 readout
  reports both solves if any transported parameter sits within 10% of a
  band edge — DECLARED here so it is not a post-hoc choice.
- FB calibrated-scale language: "no calibration freedom used on 4–5 of 5
  splits across solver conventions" (NNLS selects 1.0 on 4/5).

## 10. AMENDMENT A5 (2026-07-11, BEFORE any data processing): E2 membership must be TRAIN-ONLY (extension-leak fix); E4 conditioning made explicit

No extension data has been read at the time of this amendment (WD mounted,
untouched). Registered per the 2026-07-11 review round 4, which identified
that §2's universe rule ("absence-penalized membership over the FULL
extended window") would let extension-period activity select the 50,000
endpoints entering E2's supposedly untouched forecast — the same selection
error class as the Instagram pre-cut leak corrected in MODEL_STATUS §2z-f.
Pre-registering a selection does not make it out-of-sample.

- **E2 (frozen-parameter movement gate): membership is TRAIN-ONLY.** The
  K = 12,500 / B = 50,000 universe is selected by absence-penalized
  permanent rank computed on the EXISTING T=136 panel (2018-12..2021-06)
  ONLY — no extension week may influence it. The 50,000 entity ids are
  FROZEN (written to a members file whose SHA-256 is recorded in the run
  archive BEFORE the forecast is scored) and carried into the 34-week
  extension forecast via `restrict_universe(member_ids=...)` /
  `rankdiff_kalman --member-ids-file` (the §2z-f machinery); weekly ranks
  are recomputed within the fixed universe. Entities absent from the
  extension remain members (their exits are part of the forecast target,
  survivor-conditioning rules unchanged).
- **E1 / E3 / E5 retain full-extended-window membership** — they are
  parameter-transport and descriptive analyses of the extended panel, not
  held-out forecasts; full-window membership is the standard in-sample
  design there (§2's rule continues to govern them, including the
  registered trailing-window membership-sensitivity report).
- **E4 conditioning made explicit:** the "shared entity set" necessarily
  conditions on extension presence; E4 is declared
  SHARED-SURVIVOR-CONDITIONED and its pre-declared reading is unchanged.
- **No threshold, tolerance, stack, band, or pass criterion changes.**
  E2's criteria (§4) apply verbatim to the train-only-membership run.

**A5 execution record (2026-07-11, still before any extension row read):**
the frozen E2 membership was built and pinned this day —
`llm_fitting/e2_members_t136.parquet`, 50,000 unique ids, T0=136, SHA-256
`f0b463cab014855d72fd238a2b57a073f06cbe16eb65ff9287eb792d5c7f5562` (also in
`runs/2026-07-11_nnls_audit/MANIFEST.sha256`); source panel train-end
anchor: period 135 = week of 2021-06-28, which the E2 runner MUST verify
matches the extended panel's period 135 before scoring. Registered E2
command (single block, explicit design; `reddit_comments_ext` = the
PLATFORMS entry the §2 data build registers for the extended weekly panel):
```
python llm_fitting/rankdiff_kalman.py reddit_comments_ext --oos --top-k 12500 \
    --temperament --min-knot-entities 8 --md-lags 6 --t-tails --mix-hetero \
    --conditional state --dist-scores \
    --origins 136 --test-len 34 \
    --member-ids-file llm_fitting/e2_members_t136.parquet
```
_[E2 command superseded by the A6 form (adds frozen reps/boot and the
pre-score panel/membership enforcement); design unchanged.]_

## 11. AMENDMENT A6 (2026-07-12, BEFORE any data processing): final pre-run hardening — week-boundary leak closed, two invalid E1 comparisons corrected, E2 MC precision frozen, E4/E5 made executable, overall decision rule declared

Registered after two independent final audits of this protocol (2026-07-12;
one external, one internal — findings adjudicated in MODEL_STATUS §2z-j).
No extension data has been read; only the already-frozen T=136 panels were
inspected. Nothing below loosens any existing criterion; changes are error
corrections, ambiguity pins, execution-path specifications, and declared
non-gating readouts.

### A6.1 Data boundary and intake stop rules (closes a REAL leak)

MEASURED: the frozen weekly panel's final row (Monday 2021-06-28) is a
3-day PARTIAL week (dailies end Wed 2021-06-30; 110,508 rows / 154.76M
karma vs ~152k rows / ~334M for full weeks). A naive rebuild through
2022-12 would fold July 1–4 — the first four EXTENSION days — into that
row, which is E2 TRAINING period 135, and the period-135 date anchor would
not detect it. Therefore:

- The extended weekly panel PRESERVES the frozen T=136 prefix EXACTLY,
  byte-equal on every key and value, INCLUDING the partial 2021-06-28 row
  as frozen. July 1–4, 2021 are excluded from all weekly rows (aggregated
  and reported as boundary days, never silently discarded).
- Extension weekly rows are COMPLETE weeks only: 2021-07-05 .. 2022-12-19
  inclusive (77 weeks; extended T = 213; period 136 = week of 2021-07-05).
  The partial 2022-12-26 week is excluded (reported as boundary days).
- Intake stop rules (ANY failure stops model contact): exactly 18 monthly
  files RC_2021-07..RC_2022-12; zero missing months; zero parse errors;
  zero duplicate (entity, date) keys; zero negative metrics; exact
  weekly = Σ daily on every extension week; zero day-guard flags (census);
  frozen-prefix equality as above; period-136 date check.
- Enforcement is MECHANICAL: `llm_fitting/check_extension_panel.py` must
  print PASS before any model code touches the extension; the E2 runner
  independently re-verifies prefix equality and the membership-file
  SHA-256 (f0b463ca…7562) before scoring.

### A6.2 E1 corrections and pins

- κ criterion CORRECTED (the registered "declining head→tail" contradicts
  the registered reference itself — recorded md6 curves rise from the head:
  comments 0.010→0.100, subs 0.005→0.04, FB 0.005→0.100). Dry-running the
  frozen E1 reference under the A4 NNLS estimator (this amendment's own
  discipline) showed the pooled thirds are 0.0050 / 0.0198 / 0.0191 —
  head far below both, mid vs deep ordering WITHIN noise (Δ 0.0007 on
  ~0.02) — so a strict nondecreasing rule would fail the reference itself.
  REGISTERED RULE: E1's κ component passes if the HEAD third is strictly
  the most persistent — pooled head κ < min(pooled mid κ, pooled deep κ) —
  using head/mid/deep thirds (bands 1–4 / 5–8 / 9–12 of the 12-band
  summary) as the fixed coordinates; no ordering is imposed between mid
  and deep.
- b horizon FROZEN at h = 8 for BOTH sides: the recorded 1.08 reference is
  h*=13-specific, and `estimate_mix_b`'s rule (h* = longest of (13,8,4)
  with T//h ≥ min_changes+1) selects h*=8 on a 77-week segment — the
  registered comparison was horizon-mismatched. Reference recomputed at
  h=8 from the recorded s(h) curve: b(8) = s(8)/s(1) = 0.703/0.692 ≈ 1.016.
  Band UNCHANGED [0.95, 1.15].
- Spec-B ±25%: compared at all 12 extension z-coordinates against the
  frozen reference curve interpolated to those coordinates; every band
  within ±25%.
- E1 passes only if ALL FOUR components pass (s, b, κ orientation, Spec-B).
- A4's "within 10% of a band edge" is defined as 10% OF THE BAND WIDTH.
- A dedicated runner (`llm_fitting/e1_transport.py`) and a machine-readable
  frozen reference (`llm_fitting/e1_reference.json`, computed from the
  T=136 panel under the A4 NNLS estimator) are committed before extension
  access. FROZEN 2026-07-12: s = 0.6922, b8 = 1.0163, κ thirds
  0.0050/0.0198/0.0191, Spec-B 0.071..0.248; SHA-256
  f78a6ee27434dc0a1ad2501cdc837b69ae86852c09b1b601e2954ce23dc8c298
  (also in runs/2026-07-11_nnls_audit/MANIFEST.sha256).
- NON-GATING context, registered now: comments train-subwindow s ranged
  0.64–0.67 (§2g-X P3), i.e. the band's lower edge sits at observed
  within-panel variation; the extension readout reports a block-bootstrap
  CI alongside the point verdict. The band itself is unchanged.

### A6.3 E2 Monte Carlo precision and criterion algebra

- FROZEN: reps = 20 (seeds 0..19), boot = 2000 (bootstrap seed 0). The
  registered command's previous implicit defaults (reps=3, boot=400) put
  MC noise into scored collision moments; raising precision is neither a
  criterion nor a target change.
- Scored vector restated: dRank1/4/13, RACF1, coll1/5/20, MOM_FLOOR=0.02.
  Calibration: the recorded nine-point sigma_obs_scale grid on the TRAIN
  moment vector, unchanged.
- Coverage clause, stated algebraically for the single block (this IS the
  original criterion's letter — one split, so "≥60% of splits" means the
  block must be in-CI): PASS requires the model dRank1 median to lie
  inside the held-out 95% empirical-bootstrap CI of the empirical dRank1
  median. DECLARED DESCRIPTIVE (reported, can never rescue a failure):
  in-CI indicators at h=4 and h=13, CRPS/PIT/W1 (--dist-scores), and a
  clustered entity/week-block interval sensitivity.
- Registered command (supersedes the A5 form):
```
python llm_fitting/rankdiff_kalman.py reddit_comments_ext --oos --top-k 12500 \
    --temperament --min-knot-entities 8 --md-lags 6 --t-tails --mix-hetero \
    --conditional state --dist-scores \
    --origins 136 --test-len 34 --reps 20 --boot 2000 \
    --member-ids-file llm_fitting/e2_members_t136.parquet \
    --expect-member-sha f0b463cab014855d72fd238a2b57a073f06cbe16eb65ff9287eb792d5c7f5562
```

### A6.4 E4 made executable (registered construction)

Runner: `llm_fitting/e4_kappa_transport.py` (committed + synthetic-tested
before extension access). Pins:
- Populations: the scored complete-column population of each window
  (train = periods 0..135; extension = periods 136..212), shared set =
  intersection; n reported.
- Cells: the 5×5 rank×volatility cell EDGES are computed on TRAIN and
  REUSED on the extension (no extension-dependent recategorization).
- Train statistic: κ̂_i = ρ̂ · r_i^train, where r_i^train is the
  cell-demeaned per-entity log VR13 residual and ρ̂ is the split-half
  signal share measured on train (EB shrinkage toward 0).
- Extension statistic: r_i^ext = per-entity log VR13 residual on the
  extension window, demeaned within the TRAIN-edge cells.
- Test: Spearman(κ̂_i^train, r_i^ext) ≥ 0.20 AND concentration ratio
  mean|r^ext| over (Q1 ∪ Q5 of κ̂_i^train) ÷ mean|r^ext| over Q3 ≥ 1.3.
  Thresholds unchanged. Bootstrap CIs reported as secondary uncertainty,
  never as gates. Shared-survivor conditioning declared (A5).

### A6.5 E5 made executable

- `community_metrics` reports S(1) AND S(10) (within recorded top-2,000,
  A3) and the head offset over ranks 1–600 — RAW (as registered in A2) and
  per-week LEVEL-ADJUSTED (declared clarification: the §2z-a measurement
  lesson postdates A2 — raw log offsets are level-contaminated on a
  growing census; the level-adjusted value is the interpretable one).
- Seeds FROZEN at exactly 20 (0..19), replacing "≥10".
- Trigger, algebraically: mean_seed(S1_sim) − S1_emp > 2 · SD_seed(S1_sim),
  same direction as the recorded overshoot. S(10) and the offsets are
  diagnostic readouts only; S(1) alone triggers the registered reading.

### A6.6 E3 frozen workload

- Card + bands: `scorecard_bands.py reddit_comments_ext --top-k 12500`
  + LONG stack flags, reps = 20, boot = 500, bootstrap rng seed 0.
- Surrogate-adjusted VR reading: 50 phase-random draws with the §2v
  tooling's fixed seeds; descriptive.
- Membership sensitivity (§2 registration): the trailing-60-week
  invocation is run on the extended panel explicitly; reported, never used
  for selection.

### A6.7 Overall decision rule and reporting discipline (declared before outcomes exist)

- CORE CONFIRMATION = E1 AND E2 both pass. MIXED EVIDENCE = exactly one
  passes. CORE CONFIRMATION FAILURE = both fail. E3/E4/E5 are diagnostic
  and can never rescue or upgrade the verdict.
- All five evaluations run and are reported verbatim regardless of earlier
  outcomes, in the order E1→E2→E3→E4→E5; no conditional stopping; the
  confirmation report is written BEFORE any exploratory analysis touches
  the extension.
- NON-GATING outcome predictions, registered for interpretation only:
  E2 model rel err expected in ~0.16–0.24 (recorded split range) with the
  historical-mobility baseline itself regime-dependent (recorded 0.07–
  0.24); s expected near the sub-window range 0.64–0.69; b(8) expected
  near 1.02.

## 12. AMENDMENT A7 (2026-07-12, BEFORE any data processing): execution-truthing — the code now enforces what A6 claims (round-6 review; no scientific threshold or gate changed)

No extension data has been read. A round-6 execution audit found the A6
tooling failed OPEN in four places; this amendment records the corrections
so that every A6 claim is true of the command that will actually run.

- **Intake gate is FAIL-CLOSED** (`check_extension_panel.py`, 4 REQUIRED
  arguments: extended weekly, frozen weekly, extension daily, raw monthly
  dir). Enforced: exact schema equality (a missing frozen column FAILS —
  previously silently skipped); frozen-prefix equality on every column;
  complete-week window; weekly AND daily hygiene (dup keys, negatives, all
  numeric columns); the 18-file raw inventory; full calendar-day coverage
  2021-07-01..2022-12-25; weekly = Σ daily for EVERY shared numeric metric
  column; day-guard with the registered PRIOR-days trailing median
  (previous draft included the current day). Omitting the daily panel or
  raw dir is now impossible (required args), not a silent skip.
- **Prefix-preserving assembler exists** (`build_extension_weekly.py`):
  frozen weekly rows byte-identical + complete-week sums of extension
  dailies only; boundary days written to a side parquet, never folded.
  The official pipeline's Monday-fold builder is NOT used for the weekly
  assembly (it reproduces the A6.1 leak).
- **E1 daily-path propagation fixed** (P0): `_quantities` no longer
  hardcodes the frozen `reddit_comments` daily path — the Spec-B component
  uses the SELECTED platform's own `daily_path`, fail-closed if the
  platform entry lacks one. The extension platform entry MUST set
  `daily_path` to the extension daily panel. E1's s block bootstrap is now
  a true moving-block bootstrap (gapped relabeling keeps repeated blocks;
  the earlier set() deduplication made it a subsample statistic —
  non-gating, relabeled).
- **Registered E2 command CORRECTED to activate prefix enforcement**
  (supersedes the A5/A6.3 command text; design unchanged):
```
python llm_fitting/rankdiff_kalman.py reddit_comments_ext --oos --top-k 12500 \
    --temperament --min-knot-entities 8 --md-lags 6 --t-tails --mix-hetero \
    --conditional state --dist-scores \
    --origins 136 --test-len 34 --reps 20 --boot 2000 \
    --member-ids-file llm_fitting/e2_members_t136.parquet \
    --expect-member-sha f0b463cab014855d72fd238a2b57a073f06cbe16eb65ff9287eb792d5c7f5562 \
    --frozen-prefix data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet
```
- **E2 declared-descriptive outputs are now implemented** (not withdrawn):
  per-horizon (h=1/4/13) model-median-in-CI indicators and a week-block
  clustered CI sensitivity for the h=1 median (pairs sharing weeks are
  dependent). Both print in the gate log; the registered criterion remains
  the h=1 IID-bootstrap in-CI binary and nothing can rescue it.
- **E3 membership-sensitivity invocation**:
  `python llm_fitting/membership_robustness.py --platform reddit_comments_ext`
  (the tool previously hardcoded the old panel).
- **E5 frozen invocation with an executable trigger**
  (`e5_headlaw.py`; 20 seeds 0..19 hard-coded; prints the algebraic
  trigger verdict; S(10) and both offsets diagnostic):
```
python llm_fitting/e5_headlaw.py reddit_comments_ext --top-k 12500
```
All corrections are covered by synthetic tests (including the round-6
reproduced false-pass cases: missing frozen column, missing extension day,
omitted daily panel — each now FAILS).

## 13. AMENDMENT A8 (2026-07-12, BEFORE any data processing): intake enforcement completed + E5 SD convention frozen (round-7 review; no scientific threshold or gate changed)

No extension data has been read. Round 7 reproduced three remaining
false-PASS paths in the A7 intake gate; all are closed, each locked by an
adversarial test:

- **Exact (entity, week) index-set equality** in the weekly = Σ daily
  check, both directions (the A7 reindex silently DROPPED daily-only
  cells); every frozen numeric metric must be PRESENT in the daily panel
  (no silent intersection).
- **Day guard with frozen history**: the count series is frozen daily
  counts + extension counts, flagged by the registered
  `instrument_eras.flag_days` and adjudicated on extension dates only —
  July 1–7 (which contain E1/E2's first scored week) are judged against
  the frozen baseline, not against themselves. An aggregation-consistent
  90%-entity collapse of July 1–7 now FAILS (it passed A7's guard).
- **Aggregation-log validation replaces the directory glob** (a listing
  cannot establish parse success; 19 files and zero-byte files passed):
  the builder's coverage log is a REQUIRED input, with exactly one
  record per month 2021-07..2022-12, status == "ok", rows > 0, bytes > 0,
  no duplicates, no missing months. The A6.1 "18 monthly files in
  RAW_MONTHLY_DIR" clause is superseded by this strictly stronger check.
- **Boundary-day coverage extended through 2022-12-31** (A6 registers
  boundary days as REPORTED data; a daily panel ending Dec 25 previously
  passed).
- Gate signature (5 REQUIRED inputs):
```
python llm_fitting/check_extension_panel.py \
    EXT_WEEKLY FROZEN_WEEKLY EXT_DAILY FROZEN_DAILY COVERAGE_LOG
```
- **E5 SD convention FROZEN: POPULATION SD (ddof=0)** — the convention of
  the registered baselines (community_metrics / np.std default); the A7
  runner had used sample SD, which would have shifted the 2-SD trigger.
  Locked by an exact-threshold test on a fixed vector.

## 14. AMENDMENT A9 (2026-07-12, BEFORE any data processing): zero parse errors enforced mechanically (round-8 review; the final intake defect)

No extension data has been read. Round 8 reproduced one remaining false
PASS: the builder coverage log carries no parse-error field, and the raw
aggregator writes status="ok" even when errors > 0 — so A8's "ok and
nonempty" did not establish the registered zero-parse-errors rule (a
synthetic errors=123 month passed).

- The gate gains a SIXTH required input: the aggregator's own processing
  log (`logs/reddit_monthly_processing_log.csv` — an EXISTING pipeline
  artifact; the registered "unchanged pipeline" clause is untouched).
  For every month 2021-07..2022-12 there must exist a comments record,
  and the LATEST comments record per month (by finished_at_utc; re-runs
  append and the panel is built from the last run — declared) must have
  status == "ok", lines > 0, output_bytes > 0, and **errors == 0**.
  Nonzero parse errors are NOT acceptable at any tolerance; a month that
  cannot be re-aggregated to errors == 0 stops model contact (data
  problem, not a modeling degree of freedom).
- Gate signature (6 REQUIRED inputs):
```
python llm_fitting/check_extension_panel.py \
    EXT_WEEKLY FROZEN_WEEKLY EXT_DAILY FROZEN_DAILY COVERAGE_LOG PROCESSING_LOG
```
- Adversarial tests locked: errors=123 with status ok → FAIL (the round-8
  reproduction); missing comments month → FAIL; latest-record semantics
  (old errors superseded by a clean re-run → PASS; a newer errored run
  after a clean one → FAIL).
