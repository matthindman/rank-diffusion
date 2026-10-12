## 2z-XX. 2026-10-13 — R4 protocol review: PLANNING ONLY, nothing adopted; six corrections requested before movement-to-structure step 1 runs

_Provenance: OpenAI Codex (GPT-6), 2026-10-13; started from
a75b9c204d648a9da5dcf90ca1db9469658a6bab. The skills at that commit governed
the review. Claude to assign the final section letter on integration._

**Read and scope.** Draft v1 of MTS_PROTOCOL_STEPS_1_2_2026-10-13.md, its
two modules and eight tests; adopted agenda (§2z-bp), draft record (§2z-bq),
program/coordination documents, relevant skills, the theory note, and the
primary rank-semimartingale formula. Review at the owner's direct request.
Protocol and code unchanged. No scored empirical predictions.

**Recommendation.** Revise before empirical M1. The endpoint accounting is
correct, but six findings affect interpretation or execution: earlier/noisy
predictors do not uniquely identify personal-home versus positional drift;
finite endpoint reordering is not accumulated local time; the four-part
screen does not establish slope identification; attrition makes the drift
survivor-conditional while predicted loss weights all initial occupants;
summed marginal interval endpoints are not a 5–95% interval for the sum;
failed tracking certification does not invalidate scored output. Full
locations, evidence and repairs are in `handoff/R4_mts_protocol/REPORT.md`.

**Executable evidence.** Design committed before checks as a0251b1.
Exact population covariance for a position-only OU sampled weekly and
averaged into 52-week blocks gives carrier coefficients −0.344160 and
+0.073999 without name-specific homes. A single constructed random-walk
plus persistent-noise panel (6,000 endpoints, 208 weeks, K=1000, seed 1000,
400 endpoint bootstrap draws) gives recent coefficient −0.2482
[−0.3076, −0.2032] and older coefficient +0.2297 [0.1787, 0.2944]. This is
a counterexample, not an empirical estimate or multi-seed recovery result;
it does not estimate real-panel false-identification rates. A round trip
gives endpoint R=0 versus subdivided R=4. Deterministic missingness and
normal-sum examples expose the weighting and interval issues. A rotating
top-4K fixture has untracked block maximum 9 versus tracked 2K level 1.0045,
but the runner still emits an ordinary accounting pair.

**Execution.** The check script initially failed at import because its root
was one directory too high; corrected the import path and reran the same
checks successfully. No run was killed. Outputs and exact reproduction
commands are in REPORT.md, checks.py and CHECK_RESULTS.json. Initial health
check: 499 research tests passed (2 warnings, 13 subtests), 8 package tests
passed, sealed guard wired, legacy 14/15 and churn 0.013. Final health check:
PASS, again 499 research tests (2 warnings, 13 subtests), 8 package tests,
sealed guard wired, legacy 14/15 and churn 0.013; log in HEALTH_CHECK.txt.

**Not run / not changed.** No empirical M1 panel analysis or output read;
no V4 recovery/classification study; no candidate or Gate v1 evaluation;
no sealed data; no production edit, default, threshold, protocol adoption,
stack or claim change. Required health checks alone use existing DEVELOPMENT
fixtures and the legacy panel, declared in EXPOSURE_ROWS.md. GATE_ROWS.md is
empty. V4 requires the author's corrected-statistic version before its full
design is frozen. Nothing adopted and nothing pushed.
