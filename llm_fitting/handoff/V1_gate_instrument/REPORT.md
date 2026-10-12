# V1 — Gate instrument study: exploratory, no change adopted

2026-10-12 · OpenAI Codex · base ed75e9f. Predictions were committed in b787012 before the first measurement.

All four recorded reference settings reproduced. The tables below measure Monte Carlo variability conditional on the development panels, their fitted parameters and one finite seed bank. They do not re-adjudicate a frozen specification or define a new adoption rule.

## Reference reproduction

Recorded targets: MODEL_STATUS §2z-g for Facebook, comments and short submissions; §2z-ae P6 for long submissions; §2z-ap for the ladder rechecks. Mean ± population SD across the five frozen origins; coverage is the fraction of empirical-median bands containing the simulated median, not predictive coverage.

| Panel | Reps | Model | Historical mobility | Coverage | Train scales |
|---|---|---|---|---|---|
| Facebook Era A | 3 | 0.122821 ± 0.033257 | 0.145285 ± 0.030719 | 60% | [1.0, 1.0, 1.0, 1.0, 0.5] |
| Reddit comments | 3 | 0.171294 ± 0.046073 | 0.164842 ± 0.062282 | 60% | [0.7, 0.0, 1.0, 0.7, 1.0] |
| Short submissions | 3 | 0.163941 ± 0.053364 | 0.167808 ± 0.003711 | 80% | [0.35, 1.0, 0.5, 0.7, 0.0] |
| Long submissions | 20 | 0.193724 ± 0.060631 | 0.162609 ± 0.047871 | 80% | [0.0, 0.05, 0.1, 0.5, 0.1] |

## Error and repeatability against simulation count

Each row uses 4,000 independently resampled ensembles from seeds 1000–1079. Ensemble weights are shared across all origins, calibration scales and train/test calls. Training calibration is reselected in each ensemble. SD describes bootstrap ensemble spread; the band is its 2.5–97.5 percentile interval. MCSE describes only the finite resampling calculation, not uncertainty in the 80-seed bank.

| Panel | Reps | Error mean ± MC SD | 95% ensemble band | Mean MCSE | Same-stack difference SD | 95% \|difference\| |
|---|---|---|---|---|---|---|
| Facebook Era A | 3 | 0.1261 ± 0.0128 | [0.1009, 0.1510] | 0.00020 | 0.0183 | 0.0362 |
| Facebook Era A | 5 | 0.1209 ± 0.0106 | [0.1005, 0.1414] | 0.00017 | 0.0151 | 0.0301 |
| Facebook Era A | 10 | 0.1170 ± 0.0081 | [0.1013, 0.1325] | 0.00013 | 0.0116 | 0.0228 |
| Facebook Era A | 20 | 0.1156 ± 0.0063 | [0.1033, 0.1278] | 0.00010 | 0.0088 | 0.0177 |
| Facebook Era A | 40 | 0.1149 ± 0.0047 | [0.1058, 0.1239] | 0.00007 | 0.0067 | 0.0131 |
| Facebook Era A | 80 | 0.1148 ± 0.0036 | [0.1076, 0.1214] | 0.00006 | 0.0052 | 0.0101 |
| Reddit comments | 3 | 0.1681 ± 0.0217 | [0.1301, 0.2157] | 0.00034 | 0.0306 | 0.0592 |
| Reddit comments | 5 | 0.1593 ± 0.0173 | [0.1287, 0.1964] | 0.00027 | 0.0238 | 0.0474 |
| Reddit comments | 10 | 0.1503 ± 0.0138 | [0.1245, 0.1786] | 0.00022 | 0.0200 | 0.0388 |
| Reddit comments | 20 | 0.1460 ± 0.0112 | [0.1254, 0.1691] | 0.00018 | 0.0158 | 0.0303 |
| Reddit comments | 40 | 0.1440 ± 0.0092 | [0.1265, 0.1618] | 0.00015 | 0.0131 | 0.0254 |
| Reddit comments | 80 | 0.1433 ± 0.0074 | [0.1287, 0.1571] | 0.00012 | 0.0105 | 0.0201 |
| Short submissions | 3 | 0.1461 ± 0.0152 | [0.1183, 0.1776] | 0.00024 | 0.0215 | 0.0426 |
| Short submissions | 5 | 0.1396 ± 0.0124 | [0.1165, 0.1645] | 0.00020 | 0.0172 | 0.0334 |
| Short submissions | 10 | 0.1340 ± 0.0101 | [0.1151, 0.1547] | 0.00016 | 0.0143 | 0.0273 |
| Short submissions | 20 | 0.1309 ± 0.0080 | [0.1149, 0.1466] | 0.00013 | 0.0114 | 0.0223 |
| Short submissions | 40 | 0.1288 ± 0.0062 | [0.1166, 0.1409] | 0.00010 | 0.0088 | 0.0171 |
| Short submissions | 80 | 0.1286 ± 0.0049 | [0.1185, 0.1384] | 0.00008 | 0.0070 | 0.0140 |
| Long submissions | 3 | 0.1833 ± 0.0233 | [0.1411, 0.2322] | 0.00037 | 0.0327 | 0.0646 |
| Long submissions | 5 | 0.1801 ± 0.0189 | [0.1450, 0.2192] | 0.00030 | 0.0267 | 0.0522 |
| Long submissions | 10 | 0.1775 ± 0.0149 | [0.1493, 0.2067] | 0.00024 | 0.0212 | 0.0416 |
| Long submissions | 20 | 0.1755 ± 0.0115 | [0.1540, 0.1995] | 0.00018 | 0.0160 | 0.0316 |
| Long submissions | 40 | 0.1745 ± 0.0089 | [0.1580, 0.1936] | 0.00014 | 0.0127 | 0.0247 |
| Long submissions | 80 | 0.1732 ± 0.0064 | [0.1609, 0.1868] | 0.00010 | 0.0090 | 0.0178 |

## What causes the 3-to-20 change

The identity below is exact for the finite-bank resummaries: total change = change in calibration reselection effect + change in fixed-scale absolute-value/Jensen contribution + change in the score of the mean simulated moments. The last term includes finite-sample median bias and finite-bank effects; it is not known bias against the infinite-simulation truth. A scale is fixed separately at each origin to its full 80-seed selection.

| Panel | Total error decline | Scale reselection | Jensen | Mean-moment score | Jensen / total |
|---|---|---|---|---|---|
| Facebook Era A | 0.01047 | 0.00143 | 0.00880 | 0.00024 | 84.1% |
| Reddit comments | 0.02211 | 0.00087 | 0.02278 | -0.00154 | 103.1% |
| Short submissions | 0.01526 | -0.00002 | 0.01625 | -0.00097 | 106.5% |
| Long submissions | 0.00785 | -0.00068 | 0.00586 | 0.00268 | 74.6% |

## Moment contributions and exclusions

Error shares and variance shares at 20 reps. Variance is allocated as Cov(component,total)/Var(total), including cross terms; shares may be negative. MOM_FLOOR is evaluated on fixed empirical test moments, so its inclusion changes across origins, not across Monte Carlo seeds (missing simulated moments are handled separately). The training calibration does not apply this floor: its relative errors use a denominator bounded below by 1e-6. In the reference comments run, training coll1 is zero at origins 34, 51, 68 and 85, and about 0.00990 at origin 102. These moments remain in calibration even where the test-score vector excludes coll1. Short submissions has no h=13 statistic because its test window is only 13 weeks; that is a structural omission, separate from the floor.

| Panel | Moment | Mean-error share | MC variance share | Included /5 | Excluded origins |
|---|---|---|---|---|---|
| Facebook Era A | dRank1 | 11.7% | 19.3% | 5 | [] |
| Facebook Era A | dRank4 | 9.8% | 9.3% | 5 | [] |
| Facebook Era A | dRank13 | 18.6% | 5.1% | 5 | [] |
| Facebook Era A | RACF1 | 17.2% | 12.0% | 5 | [] |
| Facebook Era A | coll1 | 34.7% | 49.8% | 5 | [] |
| Facebook Era A | coll5 | 5.7% | 3.7% | 5 | [] |
| Facebook Era A | coll20 | 2.4% | 0.9% | 5 | [] |
| Reddit comments | dRank1 | 6.0% | 4.9% | 5 | [] |
| Reddit comments | dRank4 | 10.2% | 4.5% | 5 | [] |
| Reddit comments | dRank13 | 19.9% | 1.7% | 5 | [] |
| Reddit comments | RACF1 | 19.5% | 1.5% | 5 | [] |
| Reddit comments | coll1 | 32.9% | 82.6% | 3 | [34, 51] |
| Reddit comments | coll5 | 7.0% | 4.5% | 5 | [] |
| Reddit comments | coll20 | 4.5% | 0.3% | 5 | [] |
| Short submissions | dRank1 | 21.7% | 26.5% | 5 | [] |
| Short submissions | dRank4 | 31.3% | 8.6% | 5 | [] |
| Short submissions | RACF1 | 10.6% | 3.7% | 5 | [] |
| Short submissions | coll1 | 18.0% | 52.6% | 5 | [] |
| Short submissions | coll5 | 8.0% | 5.9% | 5 | [] |
| Short submissions | coll20 | 10.4% | 2.6% | 5 | [] |
| Long submissions | dRank1 | 11.5% | 8.9% | 5 | [] |
| Long submissions | dRank4 | 15.3% | 12.3% | 5 | [] |
| Long submissions | dRank13 | 17.3% | 3.2% | 5 | [] |
| Long submissions | RACF1 | 10.6% | -0.0% | 5 | [] |
| Long submissions | coll1 | 26.8% | 62.9% | 2 | [53, 80, 106] |
| Long submissions | coll5 | 11.7% | 11.0% | 5 | [] |
| Long submissions | coll20 | 6.8% | 1.7% | 5 | [] |

Full 40-rep decompositions, raw mean simulated moments, full-bank moments and calibration-selection frequencies are retained in analysis/*.json.

## Conditional simulation resolution

The following values are the 95th percentile of the absolute difference between two independent bootstrap ensembles of the SAME stack. They are a conditional simulation noise scale, not a powered minimum detectable candidate effect. Different specifications may have different variance and seed correlation. No candidate-versus-incumbent confidence interval can be inferred from this table alone.

| Panel | Reps | Origin | 95% \|difference\| | Difference SD |
|---|---|---|---|---|
| Facebook Era A | 20 | five-origin mean | 0.01766 | 0.00881 |
| Facebook Era A | 20 | 21 | 0.04470 | 0.02281 |
| Facebook Era A | 20 | 32 | 0.02479 | 0.01226 |
| Facebook Era A | 20 | 43 | 0.03300 | 0.01699 |
| Facebook Era A | 20 | 54 | 0.03942 | 0.01984 |
| Facebook Era A | 20 | 65 | 0.03531 | 0.01794 |
| Facebook Era A | 40 | five-origin mean | 0.01306 | 0.00666 |
| Facebook Era A | 40 | 21 | 0.03426 | 0.01748 |
| Facebook Era A | 40 | 32 | 0.01534 | 0.00785 |
| Facebook Era A | 40 | 43 | 0.02615 | 0.01338 |
| Facebook Era A | 40 | 54 | 0.02922 | 0.01502 |
| Facebook Era A | 40 | 65 | 0.02876 | 0.01455 |
| Reddit comments | 20 | five-origin mean | 0.03029 | 0.01585 |
| Reddit comments | 20 | 34 | 0.02633 | 0.01239 |
| Reddit comments | 20 | 51 | 0.02488 | 0.01212 |
| Reddit comments | 20 | 68 | 0.10425 | 0.05500 |
| Reddit comments | 20 | 85 | 0.04496 | 0.02299 |
| Reddit comments | 20 | 102 | 0.09063 | 0.04603 |
| Reddit comments | 40 | five-origin mean | 0.02540 | 0.01307 |
| Reddit comments | 40 | 34 | 0.01806 | 0.00854 |
| Reddit comments | 40 | 51 | 0.01633 | 0.00822 |
| Reddit comments | 40 | 68 | 0.08888 | 0.04544 |
| Reddit comments | 40 | 85 | 0.03937 | 0.02053 |
| Reddit comments | 40 | 102 | 0.06929 | 0.03581 |
| Short submissions | 20 | five-origin mean | 0.02233 | 0.01137 |
| Short submissions | 20 | 12 | 0.05793 | 0.02970 |
| Short submissions | 20 | 13 | 0.03680 | 0.01845 |
| Short submissions | 20 | 14 | 0.04217 | 0.02135 |
| Short submissions | 20 | 16 | 0.06024 | 0.03158 |
| Short submissions | 20 | 17 | 0.03938 | 0.01991 |
| Short submissions | 40 | five-origin mean | 0.01710 | 0.00876 |
| Short submissions | 40 | 12 | 0.04558 | 0.02274 |
| Short submissions | 40 | 13 | 0.03027 | 0.01500 |
| Short submissions | 40 | 14 | 0.03179 | 0.01589 |
| Short submissions | 40 | 16 | 0.05188 | 0.02655 |
| Short submissions | 40 | 17 | 0.02894 | 0.01475 |
| Long submissions | 20 | five-origin mean | 0.03162 | 0.01601 |
| Long submissions | 20 | 53 | 0.02626 | 0.01339 |
| Long submissions | 20 | 80 | 0.02473 | 0.01261 |
| Long submissions | 20 | 106 | 0.06315 | 0.03237 |
| Long submissions | 20 | 132 | 0.07711 | 0.03938 |
| Long submissions | 20 | 159 | 0.11581 | 0.05771 |
| Long submissions | 40 | five-origin mean | 0.02471 | 0.01267 |
| Long submissions | 40 | 53 | 0.01871 | 0.00987 |
| Long submissions | 40 | 80 | 0.01818 | 0.00939 |
| Long submissions | 40 | 106 | 0.05621 | 0.02924 |
| Long submissions | 40 | 132 | 0.06186 | 0.03044 |
| Long submissions | 40 | 159 | 0.08649 | 0.04274 |

### Actual disjoint seed blocks

These use non-overlapping trajectories, unlike sampling with replacement. There are only four 20-seed ensembles and two 40-seed ensembles; they are a check, not a reliable tail estimate. Adjacent-pair differences below compare the first with second, and third with fourth when available.

| Panel | Reps | Seed ranges | Ensemble errors | Disjoint differences |
|---|---|---|---|---|
| Facebook Era A | 20 | [[1000, 1019], [1020, 1039], [1040, 1059], [1060, 1079]] | 0.11836, 0.12201, 0.11240, 0.10782 | -0.00365, 0.00458 |
| Facebook Era A | 40 | [[1000, 1039], [1040, 1079]] | 0.11949, 0.10943 | 0.01007 |
| Reddit comments | 20 | [[1000, 1019], [1020, 1039], [1040, 1059], [1060, 1079]] | 0.15802, 0.14355, 0.14327, 0.14652 | 0.01447, -0.00325 |
| Reddit comments | 40 | [[1000, 1039], [1040, 1079]] | 0.14884, 0.14777 | 0.00107 |
| Short submissions | 20 | [[1000, 1019], [1020, 1039], [1040, 1059], [1060, 1079]] | 0.12654, 0.12967, 0.13728, 0.11916 | -0.00313, 0.01812 |
| Short submissions | 40 | [[1000, 1039], [1040, 1079]] | 0.13288, 0.12737 | 0.00551 |
| Long submissions | 20 | [[1000, 1019], [1020, 1039], [1040, 1059], [1060, 1079]] | 0.18372, 0.18010, 0.16826, 0.17867 | 0.00363, -0.01041 |
| Long submissions | 40 | [[1000, 1039], [1040, 1079]] | 0.17901, 0.17026 | 0.00875 |

## Margin illustrations on every adjacent ladder pair

Delta is newer minus preceding rung (negative favours newer). The three columns are descriptive error-only illustrations: A, delta ≤0.05; B, delta ≤ the frozen-stack 20-rep null noise scale; C, delta + that noise scale ≤0.05. B is merely absence of resolved worsening and does not establish equivalence. C is a conservative illustration, not a valid candidate-specific confidence bound. Coverage is displayed separately. No rule is selected.

Stored ladder cells contain five pooled split results, not per-seed forecasts. Therefore only pairing across time windows is measurable. The reduction column is 1 − Var(paired differences)/(Var(old)+Var(new)) across those five windows; it is NOT evidence for variance reduction from shared simulation seeds. Overlapping origins are not independent experiments.

| Panel | Pair | Delta | Paired split SD | Split variance reduction | A | B | C | Coverage |
|---|---|---|---|---|---|---|---|---|
| Facebook Era A | R0→R1 | -0.4865 | 0.1002 | 44.3% | yes | yes | yes | 60% |
| Facebook Era A | R1→R2 | -0.0913 | 0.0765 | 21.3% | yes | yes | yes | 60% |
| Facebook Era A | R2→R3 | -0.0119 | 0.0470 | 43.4% | yes | yes | yes | 100% |
| Facebook Era A | R3→R4 | 0.0066 | 0.0286 | 57.1% | yes | yes | yes | 60% |
| Facebook Era A | R4→R5 | 0.0016 | 0.0302 | 35.3% | yes | yes | yes | 40% |
| Facebook Era A | R5→G1 | -0.0006 | 0.0335 | 12.4% | yes | yes | yes | 60% |
| Facebook Era A | G1→G2 | -0.0190 | 0.0348 | 65.5% | yes | yes | yes | 80% |
| Reddit comments | R0→R1 | -1.0973 | 0.3564 | 20.0% | yes | yes | yes | 20% |
| Reddit comments | R1→R2 | -0.1370 | 0.0735 | 53.8% | yes | yes | yes | 100% |
| Reddit comments | R2→R3 | -0.0047 | 0.0845 | 62.2% | yes | yes | yes | 20% |
| Reddit comments | R3→R4 | -0.0165 | 0.0237 | 97.1% | yes | yes | yes | 40% |
| Reddit comments | R4→R5 | -0.0252 | 0.0289 | 95.3% | yes | yes | yes | 80% |
| Reddit comments | R5→G1 | -0.0078 | 0.0845 | 54.1% | yes | yes | yes | 60% |
| Reddit comments | G1→G2 | -0.0495 | 0.0396 | 84.3% | yes | yes | yes | 80% |
| Short submissions | R0→R1 | -0.4005 | 0.0187 | 75.8% | yes | yes | yes | 100% |
| Short submissions | R1→R2 | -0.0643 | 0.0179 | 79.8% | yes | yes | yes | 100% |
| Short submissions | R2→R3 | -0.0840 | 0.0272 | 75.4% | yes | yes | yes | 100% |
| Short submissions | R3→R4 | 0.0346 | 0.0310 | 62.7% | yes | no | no | 100% |
| Short submissions | R4→R5 | 0.0000 | 0.0000 | 100.0% | yes | yes | yes | 100% |
| Short submissions | R5→G1 | -0.0339 | 0.0286 | 43.0% | yes | yes | yes | 60% |
| Long submissions | R0→R1 | -1.1713 | 0.1299 | 30.5% | yes | yes | yes | 80% |
| Long submissions | R1→R2 | 0.1107 | 0.4110 | 29.8% | no | no | no | 60% |
| Long submissions | R2→R3 | -0.1613 | 0.2875 | 70.6% | yes | yes | yes | 60% |
| Long submissions | R3→R4 | -0.0169 | 0.0562 | 96.1% | yes | yes | yes | 40% |
| Long submissions | R4→R5 | 0.0309 | 0.0697 | 94.1% | yes | yes | no | 40% |
| Long submissions | R5→G1 | -0.1017 | 0.2171 | 15.3% | yes | yes | yes | 80% |

## Wrong-model controls

| Panel | Rung | Model ± split SD | Baseline ± split SD | Excess | Reject >0.05 |
|---|---|---|---|---|---|
| Facebook Era A | R0 | 0.7044 ± 0.1048 | 0.1453 ± 0.0307 | 0.5591 | yes |
| Facebook Era A | R1 | 0.2179 ± 0.0584 | 0.1453 ± 0.0307 | 0.0727 | yes |
| Reddit comments | R0 | 1.4749 ± 0.3516 | 0.1648 ± 0.0623 | 1.3100 | yes |
| Reddit comments | R1 | 0.3776 ± 0.0585 | 0.1648 ± 0.0623 | 0.2127 | yes |
| Short submissions | R0 | 0.6767 ± 0.0239 | 0.1678 ± 0.0037 | 0.5088 | yes |
| Short submissions | R1 | 0.2762 ± 0.0242 | 0.1678 ± 0.0037 | 0.1084 | yes |
| Long submissions | R0 | 1.5033 ± 0.1138 | 0.1626 ± 0.0479 | 1.3407 | yes |
| Long submissions | R1 | 0.3319 ± 0.0806 | 0.1626 ± 0.0479 | 0.1693 | yes |

R0/R1 are prior completed ladder cells (§2z-ap); no non-frozen stack was newly run.

## Predictions, including failures

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

## Execution, provenance and limits

Commands: `zsh llm_fitting/handoff/V1_gate_instrument/launch.zsh`; then, for each panel, the registered Python interpreter runs `scripts/heavy_lock.py -- caffeinate -dims python -u llm_fitting/validation/gate_instrument_analysis.py --panel PANEL`; finally `gate_instrument_report.py`. Substitute the registered interpreter for both occurrences of python. The exact executable is `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`. All expensive jobs use the shared lock.

The wrapper imports the frozen gate, caches primitive seed histograms atomically, preserves pooled medians and scale tie-breaking, and restores all wrapped functions. Both protected model files remain unchanged. Source/input SHA-256, runtime, commit, seeds, attempts and completion markers are beside each run. AUDIT.json verifies identical input hashes, fitted-parameter summaries and empirical moments between reference and bank, unchanged simulation sources, exact reconstruction of full-bank calibration and pooled score, exactly 7,200 seed files per panel, and an ordered digest of their paths and hashes. Full binary seed banks are retained locally under this run directory; compact summaries and provenance are committed. Nine possible test scales are collected for each bank so bootstrap calibration can be replayed without refitting. Descriptive distribution-score printing is off; it does not enter Gate v1.

No Instagram optional run, sealed data, WD archive, parameter/default change, candidate gate, adoption decision or paper-claim change was made. Finite-bank uncertainty, temporal dependence, estimation uncertainty and new-specification detection power remain outside this measurement. The empirical-median coverage criterion is reported for actual completed calls but is not itself resampled; this is not a power study of the complete joint gate rule. QUESTIONS.md records the missing ladder seed output; Claude’s committed answer confirms that none exists. This is a resolved limitation, not a pending input.

For later owner-approved D1 comparisons, the runner would need an explicit immutable specification/flag map and cache identity covering all parameter fields and forecast arguments. Its current CLI deliberately runs only the four hard-coded frozen stacks; arbitrary candidate support was not built. The primitive seed-histogram format can retain paired incumbent/candidate draws once that extension is approved.

Health-check logs in this handoff include one failed zsh invocation of a Bash script; this failed after the research suite and before package/guard checks. Its successful Bash rerun is separate. This is a harness error, not a failed or killed scientific run. All scientific attempts are in ATTEMPTS.jsonl; incomplete attempts must be disclosed if present.
