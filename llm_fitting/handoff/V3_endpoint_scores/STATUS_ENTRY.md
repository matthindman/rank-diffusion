## 2026-10-12 — V3 endpoint proper scores: four frozen operational tables completed; an extra latent transition blocks a calendar-aligned forecast verdict. No adoption

Provenance: OpenAI Codex; base main 2ae195f806c299caedc4f86355fd08187979f97c; approved coordination protocol §2z-ar. DESIGN committed e5db2b8 before the first V3 run. Scorer/baselines/runner committed 291c41f before panel runs; audit/report 1577b23; conditional MC-band presentation 9abe8dc before first score analysis. Governing skills are those at the base. Claude appends this entry under the coordination protocol. **Supplementary, retrospective, DEVELOPMENT data.**

**Question.** Do the frozen conditional movement stacks score individual endpoint outcomes better than no-change, the gate's historical-mobility law lifted to signed endpoint forecasts, and a training-only anchored rank-bin path kernel? All four primary panels, original five origins, 20 members, seeds 0–19, 2, 000 gate bootstrap draws. No candidate or new fitting parameter.

**Timing finding.** A deterministic fixture with filtered levels[3, 2, 1] and lasting innovations[-.75, 0, 0] returns nominal h1 ranks[2, 1, 3], proving two latent transitions; one transition gives[1, 2, 3]. The original simulator advances at t=0 before dropping that row, then advances at t=1 before export. Baselines and observations use h actual future weeks. All tables therefore carry the legacy nominal-export qualifier. They evaluate the frozen operational output, not a correctly aligned h-week forecast. No protected file was changed and no prior gate verdict is re-adjudicated. Q1 requests a separate, correctly timed export from Claude.

**Scores and uncertainty.** Rank 0 and rank>K map to declared outside category K+1; CRPS acts on its log-rank. The fair ensemble CRPS/Brier corrections assume iid members conditional on fixed estimated inputs. An outside-within-h event includes returners. The historical-mobility comparator supplies only a survivor marginal and no event Brier forecast. Coarse strata use training absence-penalized permanent rank; established additionally requires presence>=.7; origin-inside established is separate. The direct-horizon rank-bin kernel transfers whole training path snippets to the endpoint's exact origin rank and recomputes crossings. Absent origins use an empirical joint terminal/event law. Empty-bin fallback and unavailable horizons are recorded.

Comparisons pair the same endpoints and origins with equal-origin weight.13-week calendar-block bands (26-week sensitivity) are descriptive, conditional on five sparse origins; they are not calibrated future-panel confidence intervals and omit refitting/selection uncertainty. Short-panel calendar intervals are withheld. Whole-member jackknife MCSE and estimate±1.96MCSE bands separately describe approximate simulation precision. All head rows use20 members and explicit bands. Observed ranks include present members; simulated ranks include all occupied slots, including new entrants competing with tracked members. This mapping difference remains visible.

**Operational findings.** Overall mean CRPS is lower than each baseline in all 12 panel/horizon comparisons. This does not establish calendar-aligned predictive skill. The advantage over rank-bin is smaller than over no-change on all four panels at nominal h4. Nominal h4 overall CRPS improvement (baseline minus model), 13-week block bracket, and conditional simulation MCSE:

| Panel | Baseline | Improvement |13-week bracket | MCSE |
| --- | --- | --- | --- | --- |
| facebook_a | no_change | 0.042864 | [0.042070, 0.044598] | 0.000134 |
| reddit | no_change | 0.020098 | withheld | 0.000079 |
| reddit_comments | no_change | 0.014653 | [0.013451, 0.016557] | 0.000027 |
| reddit_submissions_long | no_change | 0.010428 | [0.008849, 0.011619] | 0.000049 |
| facebook_a | historical_mobility | 0.034594 | [0.033638, 0.036478] | 0.000169 |
| reddit | historical_mobility | 0.017005 | withheld | 0.000073 |
| reddit_comments | historical_mobility | 0.013526 | [0.012375, 0.015380] | 0.000027 |
| reddit_submissions_long | historical_mobility | 0.009080 | [0.007533, 0.010251] | 0.000057 |
| facebook_a | rank_bin | 0.011602 | [0.010901, 0.012756] | 0.000278 |
| reddit | rank_bin | 0.003707 | withheld | 0.000094 |
| reddit_comments | rank_bin | 0.002504 | [0.001940, 0.003301] | 0.000050 |
| reddit_submissions_long | rank_bin | 0.001135 | [0.000188, 0.001668] | 0.000057 |

The overall result is not uniform across groups or outcomes. At h4 the comments tail is worse than rank-bin; long-submission middle and shell block brackets include zero. Its all-cohort h13 rank-bin improvement is 0.000470 with block bracket[-0.001266, 0.001496]. For established outside events at h13, mean Brier is better than rank-bin on Facebook/comments and worse on both submissions panels. Against no-change, short submissions is worse; other panel means improve but several block brackets include zero. Short-panel historical/kernel h13 comparisons have three common origins, versus five for no-change.

Prediction P7 fails in the opposite direction: model outside-event frequency is 0.042482 versus 0.025297 on comments, and0.047685 versus 0.031792 on long submissions. This any-event probability under conditional movement stacks is distinct from A2's repeated-departure intensity under structure stacks; the extra transition also matters. It does not reverse that earlier residual.

**All predictions.**

| Prediction | Verdict | Result |
| --- | --- | --- |
| P1 | PASS | 11 known-answer tests pass; full health log accompanies handoff. |
| P2 | PASS | Conditional primitive/export/cache replay exact; protected model files unchanged. |
| P3 | PASS | Two transitions at nominal h1; calendar-aligned skill withheld. |
| P4 | PASS | Positive nominal h4 all-cohort CRPS improvement on 4/4 panels against no_change. |
| P5 | PASS | Positive nominal h4 all-cohort CRPS improvement on 4/4 panels against historical_mobility. |
| P6 | PASS | Advantage over rank-bin smaller than over no-change on 4/4 panels. |
| P7 | FAIL | [('reddit_comments', -0.0171855500433559), ('reddit_submissions_long', -0.015892781915462298)] |
| P8 | PASS | All short-panel week-block intervals withheld. |

**Gate and execution record.** Four actual frozen-stack gate evaluations, all RAN-OK, no failed or killed V3 attempt. Per-seed calibration histograms and conditional rank arrays are restartable and source/input/full-parameter fenced. Runs used the shared heavy lock and caffeinate. Model gate means±split SD: FB 0.103361±0.044469, comments 0.136897±0.055575, short submissions 0.128612±0.028301, long submissions 0.193724±0.060631. Baselines 0.145285, 0.164842, 0.167808, 0.162609; coverage .8,.8,.6,.8. These are reproductions at20 reps, not a new candidate. Long submissions exactly matches V1's same-seed20-rep result dictionary (§2z-ax), including scales and coverage. Sources and input files matched their pre-run hashes after execution. GATE_ROWS.md has the four ledger rows.

**Validation.**11 added known-answer tests pass: true endpoint law versus pooled blindness, exact finite-ensemble expectations, discrete randomized PIT, brute-force member jackknife, outside-and-return, relabelling invariance, training-only baseline support, short-calendar withholding, permanent cohorts, and exact conditional primitive/export/cache replay. The full export audit passes all 20 splits. Tables contain2, 898 origin score rows, 420 paired comparisons and3, 528 aggregate calibration rows; all strata and horizons are retained. Formatting repairs after analysis added a missing table header, origin counts and an explicit nominal-horizon column; every pre-existing numeric table column remained exactly equal. Health: 458 research tests plus13 subtests; 8 package tests; legacy14/15, churn.013, PASS.

**Not run / waiting.** Correctly timed endpoint evaluation awaits Q1 in QUESTIONS.md; no ANSWERS.md is on main at handoff. No non-frozen specification, parameter/refitting bootstrap, candidate gate, new data, new threshold, package change or adoption. No sealed folder was touched. WD absence is intentional.

**Artifacts and reproduction.** Handoff llm_fitting/handoff/V3_endpoint_scores; aggregate archive llm_fitting/runs/2026-10-12_V3_endpoint_scores. Full local exports/caches remain under ignored output/V3_endpoint_scores. launch.sh PANEL runs each frozen stack with the registered interpreter, shared lock and caffeinate; endpoint_audit.py, endpoint_analysis.py and endpoint_report.py regenerate the audit/tables/report. Full commands, source/input hashes, attempts, seeds, all bands and data denominators accompany REPORT.md. Method sources: Ferro 2014 (DOI10.1002/qj.2270) and Gneiting–Raftery 2007 (DOI10.1198/016214506000001437).
