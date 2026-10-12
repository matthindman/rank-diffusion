## 2026-10-12 — V2 independent simulation, recovery and mechanism discrimination: no change adopted

Provenance: OpenAI Codex, 2026-10-12; base main 4ae73b43b852ac540cadd39808f27200885f1be7, governing skills at that commit. The approved coordination protocol is §2z-ar. DESIGN was committed at 4f823c3 before the first V2 run; the two-case addendum was committed at aca704f before its runs. Scientific evaluators were committed before their outputs; final audit/report code is bbbaa86. Claude assigns the section letter and appends this entry. **Exploratory, DEVELOPMENT-derived synthetic study; no model, default, gate, threshold or agenda adopted.**

**Question.** Does an independently implemented process reproduce the frozen structural simulator, can the actual estimator recover known parameters, and which declared residual statistics distinguish eight alternative mechanisms at native panel sizes? This implements the V2 brief using §1 and the S4 structural stack of §2z-ap. It is not recovery of the separately frozen movement stacks or daily Spec-B instrument.

**Design and execution.** Facebook Era A: K=3,500, T=86; comments: K=12,500, T=136; long submissions: K=5,000, T=212. Native parameter/length pairs are confounded, not a length factorial. The independent simulator has no production simulation import. The fixed-band Gaussian covariance and high-strength mechanism checks pass. Twenty production seeds 0–19 and twenty independent seeds 1000–1019 per profile enter the 19-statistic cross-check. The fixed STOP rule is a 20,000-permutation discrepancy test with Bonferroni across 57 comparisons; all pass, minimum adjusted p=1 on each profile. Mean/SD and 5–95% seed bands for all 57 comparisons are in crosscheck.csv. This is not an equivalence or rare-tail power claim. Shares are within the recorded top min(N,2000).

After the passed cross-check: 20 baseline recovery fits and 20 at true s=0 per profile; all eight mechanisms at both fixed strengths, plus baseline and a matched Gaussian control, have 20 diagnostic seeds each. A further 39 baseline seeds 2000–2038 calibrate per-statistic min/max envelopes. Current-rank attraction, annual cycles and random walks also have 20 fits per strength/profile. The audit verifies all 1,797 saved cells and their source/input hashes: 120 cross-check, 120 baseline/null recovery, 1,197 diagnostics, and 360 added-case fits. There are **zero failed seed records** and no zero-risk departure/fate cells. Repeated uses of baseline paths are not independent replications.

**Operational recovery.** Synthetic activity passes through metric=max(expm1(X),0), the real absence-penalized universe selection and the unchanged S4 minimum-distance fit. Coefficients are generated at latent lasting-level rank; the estimator uses time-mean observed-rank bands. This includes observation-floor, selection and band-mapping effects. The following ± values are SD across 20 seeds, not estimation confidence intervals.

| Profile | κ grid-edge fraction | s estimate at true zero | Median relative RMSE: V / κ |
| --- | --- | --- | --- |
| facebook_a | 0.6759 ± 0.1245 | 0.13839 ± 0.00219 | 0.8980 / 0.9206 |
| reddit_comments | 0.4855 ± 0.0824 | 0.19017 ± 0.00182 | 0.7428 / 1.7435 |
| reddit_submissions_long | 0.4565 ± 0.1173 | 0.16733 ± 0.00157 | 0.5143 / 0.9405 |

Every s=0 fit returned s>1e-12: 20/20 per profile, Wilson 95% interval [0.8389,1]. This is a positive-estimate frequency, not a nominal Type I test. V recovers relatively better than κ but its absolute error remains large. V=σ_perm²/(2κ), and the supplied discrete counterpart, are local parameter functionals, not the global stationary variance of the interacting replacement process.

The Facebook fits return t_df=infinity on all 20 baseline and all 20 s=0 seeds, although the generating t_df is 4.3 and the fitted fast component is active. Infinite values are retained as an atom, not labelled failed fits. Comments baseline t_df is 10.7807 ± 1.0965 versus truth 5.1730; long submissions 5.1335 ± 0.4380 versus truth 4.3. Complete knot-level bias, SD, RMSE, quantiles, NNLS corners, grid edges and Monte Carlo SEs are in the recovery tables. The `valid` column counts finite estimates; finite-conditional tail summaries are explicitly separate.

**Diagnostic findings.** The archive contains 39,474 per-condition/statistic rows and 335,529 pair/statistic comparisons. Every strength and every available statistic is included. Labels are descriptive: unresolved below one pooled seed SD; separated at least two SD with disjoint 5–95% seed bands; otherwise overlapping, unavailable or degenerate. They are not formal tests or proof of unique mechanism identification.

- Strong reversible off states are detected by the absence-exit rate in 20/20 seeds of each profile (Wilson [0.8389,1]).
- Changing volatility strongly reduces the middle-rank volatility-persistence ratio relative to its matched Gaussian control; the signed effects are retained. Heavy lasting innovations raise the anchored head-tail ratio. Neither result alone identifies a mechanism on empirical data.
- The predicted 20% reduction in the head's standardized 99.99th percentile under bounded passing shocks fails: reductions are 12.48%, 18.13% and 19.19%. The means and bands below retain this negative result.
- At T=86 the four slow-memory/random-walk strength pairs on middle skip momentum are three separated and one overlapping; none is unresolved. The prediction of at least one unresolved pair fails.
- Strong current-rank attraction and a strong random walk are unresolved by 26-week middle dispersion on all three profiles. Fitted middle home κ also fails to separate these two strong alternatives. A smaller fitted κ does not identify which of them generated the panel.
- Strong annual cycles produce clear gap4-minus-gap2 momentum signatures, but **lower**, rather than raise, fitted middle κ on every profile. The predeclared sign prediction fails.

Head far-tail comparison (20 seeds, mean ± SD and 5–95% seed band):

| Profile | baseline | bounded_fast_1 |
| --- | --- | --- |
| facebook_a | 5.3787 ± 0.4428 [4.9101, 6.2149] | 4.7076 ± 0.2649 [4.3415, 5.1068] |
| reddit_comments | 5.6251 ± 0.2037 [5.3551, 5.9401] | 4.6051 ± 0.1249 [4.4809, 4.8142] |
| reddit_submissions_long | 7.2620 ± 0.6052 [6.4563, 8.4315] | 5.8684 ± 0.2785 [5.5587, 6.2050] |

Fitted middle κ under the added mechanisms (mean ± seed SD, 20 seeds):

| Profile | baseline | ladder_attraction_1 | random_walk_1 | annual_cycle_1 |
| --- | --- | --- | --- | --- |
| facebook_a | 0.033015 ± 0.012104 | 0.012389 ± 0.004518 | 0.011804 ± 0.003995 | 0.005044 ± 0.000198 |
| reddit_comments | 0.017459 ± 0.004275 | 0.007060 ± 0.001957 | 0.007000 ± 0.001750 | 0.005000 ± 0.000000 |
| reddit_submissions_long | 0.007958 ± 0.001546 | 0.005000 ± 0.000000 | 0.005071 ± 0.000317 | 0.005000 ± 0.000000 |

**False alarms and limits.** Every baseline evaluation seed triggers at least one available statistic: 20/20 on each profile (Wilson [0.8389,1]). This is not a 5% global test. The per-statistic 2/40 reference averages over calibration sets; conditional rate intervals omit calibration-set uncertainty. Evaluation seeds also entered the cross-check, so no post-selection coverage is claimed. With only 20 seeds, rates are coarse. Finite nuisance parameters were held fixed, not refitted under each alternative. Lasting t innovations with df=3 lack a fourth moment; variance-based Monte Carlo error descriptions can be unstable. Diagnostics follow the existing raw-simulator observation convention, distinct from the recovery intake floor.

**All predictions.**

| Prediction | Result | Verdict |
| --- | --- | --- |
| P1 | All 57 simulator discrepancy checks pass. | PASS |
| P2 | Mean κ grid-edge fraction >=25% on at least two profiles: all three. | PASS |
| P3 | Median relative RMSE of local V below κ on at least two profiles: all three; both remain inaccurate. | PASS |
| P4 | At true s=0, positive estimates in >=25% of seeds on at least two profiles: 100% on all three. | PASS |
| P5 | Strong off-state absence-exit detection >=80% on at least two profiles: 100% on all three. | PASS |
| P6 | Strong varying volatility lowers the middle ratio by >=2 matched-control SD on at least two profiles: all three. | PASS |
| P7 | Strong fat lasting shocks raise the head h13 anchored ratio by >=1 baseline SD on at least two profiles: all three. | PASS |
| P8 | Head far-tail reduction >=20% on at least two profiles: none; 12.48/18.13/19.19%. | FAIL |
| P9 | At least one T86 slow-memory/RW pair unresolved on skip momentum: none; three separated, one overlapping. | FAIL |
| P10 | Strong current-rank attraction fitted middle κ <=75% of baseline on at least two profiles: all three (37.52/40.44/62.83%). | PASS |
| P11 | Strong current-rank attraction and strong RW separated by h26 middle dispersion on at least two profiles: none; all unresolved. | FAIL |
| P12 | Strong annual-cycle gap4-minus-gap2 slope >=.5 on at least two profiles: all three (1.8131/1.8491/1.9038). | PASS |
| P13 | Strong annual cycle raises fitted middle κ by >=.005 on at least two profiles: none; differences -0.027971/-0.012459/-0.002958. | FAIL |

**Validation and gate standing.** Independent baseline → frozen simulator: all 57 checks pass. Baseline → planted alternatives: all 18 conditions and all pairs are tabulated, with the failures above retained. Gate: NOT RUN on every panel; GATE_ROWS.md has zero rows. No empirical candidate, adoption decision or re-adjudication. Health: 464 research tests plus 13 subtests, eight package tests; legacy guard 14/15, churn .013.

**Execution problems, recorded without erasure.** Original diagnostic workers were terminated after quadratic repeated-warning accounting stalled them; three queued workers were cancelled. Two completed added-case fit cells from those attempts were archived unused and replayed; all 120 original recovery fits survived. One restart failed before scientific work because concurrent attempt filenames collided, then restarted with namespaced IDs. A verified eligibility wrapper reduced work without changing any numeric diagnostic or count on controlled cases and all three native profiles. The comments worker was restarted for that exact optimization; completed atomic cells were retained, and no partial aggregate was interpreted. A redundant queued long-panel job and the comments automatic follow-on waiting on a stage lock were cancelled before scientific execution. The active original comments diagnostics completed independently. Every producer/wrapper hash and the native equivalence results are audited.

**Not run and remaining scope.** No movement gate, daily-instrument recovery, nuisance-refitted composite-mechanism comparison, isolated panel-length factorial, fresh confirmation, default change or empirical A1/A2 comparison was run. A1/A2 reports and D1 were not opened before this result commitment. A main commit-title listing incidentally exposed a qualitative A8 summary after every V2 design and prediction was fixed; PREFLIGHT records that exposure. Nothing in the design, strengths, seeds, diagnostics or classification changed. No V2 hook or input remains blocked; no QUESTIONS.md is needed.

**Reproduce.** Registered Python 3.11.6, pandas 2.2.2, NumPy 2.0.1. `bash llm_fitting/handoff/V2_planted_mechanisms/launch_crosscheck.sh`; then `bash llm_fitting/handoff/V2_planted_mechanisms/launch_complete_efficient.sh`; then `bash llm_fitting/handoff/V2_planted_mechanisms/finalize.sh`. Heavy jobs use caffeinate and the shared lock. `archive_tables.py --out llm_fitting/runs/2026-10-12_V2_planted_mechanisms --restore` restores large CSV files losslessly for reading/plotting. Original failed launch commands and all successful restarts remain in REPORT.md, PREFLIGHT.md, attempt events and logs.

Archive: `llm_fitting/runs/2026-10-12_V2_planted_mechanisms/`; handoff: `llm_fitting/handoff/V2_planted_mechanisms/`. Complete full-precision tables, compact seed evidence, source/input hashes and prediction outcomes accompany the report. Full empirical ladders, full fitted objects and figures remain local. Protected records and model files were not edited.
