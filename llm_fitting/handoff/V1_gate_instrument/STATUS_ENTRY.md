## 2026-10-12 — V1 gate-instrument measurement: no change adopted

Provenance: OpenAI Codex, 2026-10-12, starting from ed75e9f4f0a95b2f479f1001835410073b6ed0fe; governing skills at that commit. Design and predictions committed in b787012 before any V1 measurement. Exploratory instrument study and frozen-stack reproduction on DEVELOPMENT panels; Claude integrates this handoff under the approved coordination protocol (§2z-ar).

**Question.** How much of the gate’s apparent improvement with more simulations is Monte Carlo bias, how noisy is a comparison, and which moments carry that noise? This measures Gate v1 unchanged, not a candidate or a replacement criterion.

**Execution.** Four recorded references reproduced (§2z-g, §2z-ae, §2z-ap): 3 reps / 400 empirical-bootstrap draws on Facebook Era A, comments and short submissions; 20 / 2000 on long submissions. All five frozen origins per panel, seeds starting at zero. Then 80 fresh seeds (1000–1079), nine train calibration scales and all nine corresponding conditional test forecasts per origin. Full sparse displacement histograms, seed RACFs and collision rates preserve pooled medians. Each simulator call is restartable; every actual gate call is in GATE_ROWS.md.

**Analysis.** 4,000 bootstrap ensembles per rep count 3/5/10/20/40/80 from the same 80-seed bank; shared weights across origins/scales/train/test, with calibration reselected. Two groups of 2,000 ensembles supply same-stack null differences. Actual disjoint 20/40-seed blocks are also reported. Bands below and in REPORT.md condition on the fixed panels, fits and finite bank: they are not temporal, estimation or population uncertainty.

| Panel | Recorded-setting reproduction ± split SD | 80-seed instrument call ± split SD | Historical baseline ± split SD | Coverage: reference → bank | Standing |
|---|---|---|---|---|---|
| Facebook Era A | 0.1228 ± 0.0333 | 0.1154 ± 0.0349 | 0.1453 ± 0.0307 | 60% → 60% | reference reproduced; no new verdict |
| Reddit comments | 0.1713 ± 0.0461 | 0.1429 ± 0.0621 | 0.1648 ± 0.0623 | 60% → 100% | reference reproduced; no new verdict |
| Short submissions | 0.1639 ± 0.0534 | 0.1279 ± 0.0293 | 0.1678 ± 0.0037 | 80% → 60% | reference reproduced; no new verdict |
| Long submissions | 0.1937 ± 0.0606 | 0.1725 ± 0.0644 | 0.1626 ± 0.0479 | 80% → 80% | reference reproduced; no new verdict |

| Panel | 3-rep error ± MC SD | 20-rep error ± MC SD | 3→20 decline | Jensen share of decline | 95% \|same-stack difference\|: 20 / 40 reps |
|---|---|---|---|---|---|
| Facebook Era A | 0.1261 ± 0.0128 | 0.1156 ± 0.0063 | 0.01047 | 84.1% | 0.01766 / 0.01306 |
| Reddit comments | 0.1681 ± 0.0217 | 0.1460 ± 0.0112 | 0.02211 | 103.1% | 0.03029 / 0.02540 |
| Short submissions | 0.1461 ± 0.0152 | 0.1309 ± 0.0080 | 0.01526 | 106.5% | 0.02233 / 0.01710 |
| Long submissions | 0.1833 ± 0.0233 | 0.1755 ± 0.0115 | 0.00785 | 74.6% | 0.03162 / 0.02471 |

Jensen, scale reselection and the score of the mean simulated moments are separately tabulated in REPORT.md and add to the total change. Jensen shares may exceed 100% when another term offsets the decline. The per-moment error and covariance-allocated variance shares, every origin’s resolution estimate and MOM_FLOOR exclusions are retained. Short submissions has only six available moments: h=13 is outside its test-window rule. Calibration itself does not apply MOM_FLOOR.

**Predictions, with failures retained.**

| Prediction | Declared expectation | Measured | Verdict |
|---|---|---|---|
| P1 | all four references reproduce | 4 / 4 | PASS |
| P2 Facebook Era A | positive decline in [0.01, 0.05] | 0.01047 | PASS |
| P3 Facebook Era A | Jensen ≥50% of positive decline | 84.1% | PASS |
| P4 Facebook Era A | SD40 / SD20 ≤0.85 | 0.757 | PASS |
| P5 Facebook Era A | collisions >50%; coll1 largest | 54.4%; coll1 | PASS |
| P2 Reddit comments | positive decline in [0.01, 0.06] | 0.02211 | PASS |
| P3 Reddit comments | Jensen ≥50% of positive decline | 103.1% | PASS |
| P4 Reddit comments | SD40 / SD20 ≤0.85 | 0.825 | PASS |
| P2 Short submissions | positive decline in [0.01, 0.06] | 0.01526 | PASS |
| P3 Short submissions | Jensen ≥50% of positive decline | 106.5% | PASS |
| P4 Short submissions | SD40 / SD20 ≤0.85 | 0.770 | PASS |
| P2 Long submissions | positive decline in [0, 0.06] | 0.00785 | PASS |
| P3 Long submissions | Jensen ≥50% of positive decline | 74.6% | PASS |
| P4 Long submissions | SD40 / SD20 ≤0.85 | 0.791 | PASS |
| P5 Long submissions | collisions >50%; coll1 largest | 75.7%; coll1 | PASS |
| P6 | R0 and R1 rejected on all panels | 8 / 8 | PASS |

**Validation and scope.** Every recorded reference reproduces; the new bank changes only simulation seeds/count and records all calibration possibilities. No adoption is adjudicated. R0 and R1 from the stored ladder are rejected on all four panels by the existing +0.05 error rule; no non-frozen rung was rerun. All 26 adjacent ladder pairs are shown under three illustrative margin rules, with coverage separate. No rule is selected. The ladder did not save per-seed outputs, as Claude confirmed in ANSWERS.md, so its measured split-pairing benefit cannot be interpreted as seed-pairing variance reduction.

**Limitations / not run.** No Instagram optional run, sealed data, WD archive, candidate gate, default change or paper-claim change. Candidate-specific paired resolution and power are not identified by same-stack noise. The empirical-median coverage criterion is not itself resampled. Whole-pipeline and future-time uncertainty are not measured here. No scientific run was killed or substituted. One queued health-check wrapper was cancelled before execution, and one health invocation used zsh on a Bash script and failed after the research suite; successful full Bash checks are retained separately.

**Code and validation.** The two protected model files remain unchanged. New wrappers, analysis, report and plotting code are under validation/. Eleven targeted known-answer tests cover exact pooled histograms, original-simulator parity, cache restart, calibration ties, floor/missing moments, folded-normal bias/noise, covariance allocation and full analysis decomposition. The full check at the latest recorded pre-analysis commit passed 425 research tests + 13 subtests, 8 package tests and the legacy 14/15, churn 0.013 guard. Final handoff health is recorded in HEALTH_FINAL.log. AUDIT.json checks unchanged inputs/fits/empirical moments/source hashes, reconstructs full-bank calibration and scores, and verifies 28,800 seed files with digests.

**Commands and archive.** Run from the assigned Codex copy with the registered Python 3.11 interpreter: `zsh llm_fitting/handoff/V1_gate_instrument/launch.zsh`, then `zsh llm_fitting/handoff/V1_gate_instrument/launch_analysis.zsh`; the launchers use heavy_lock.py and caffeinate. Plotting: `python llm_fitting/validation/gate_instrument_plot.py` with MPLCONFIGDIR inside the run directory. Archive: `llm_fitting/runs/2026-10-12_V1_gate_instrument/`; complete report, ledger rows, predictions and figures: `llm_fitting/handoff/V1_gate_instrument/`. Compact summaries/provenance are committed; full binary seed banks remain in the Codex working copy.
