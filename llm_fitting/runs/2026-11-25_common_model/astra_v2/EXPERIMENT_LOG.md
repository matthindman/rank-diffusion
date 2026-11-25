# Astra development experiment log — 2026-11-25

OpenAI Astra, repository base 7ebb7613f0f1e3fd6599fdacb8ccae3ee1bed5de. Starting candidate and Round 3 source both SHA-256 33a8dcafa9610260a9c07d60db9e1c5afd5727abbcc6e6d0b3cc69944993ae79. Research context is MODEL_STATUS §§2z-dx–dy and the Round 3 report §§7–11. Only the four named anonymous development NPZs and the FB Spec-B instrument are panel inputs. Instagram is the §2z-c apparatus. The brief confines all writes and the gate/contact handoff to this folder. The repository health script uses an additional Facebook parquet and is outside this run's data allowance. Run the supplied synthetic suite before and after changes.

## Predictions before diagnostic runs

1. A split-half spread ratio compares two marginal dispersions and cannot identify the regression of lasting volatility on weekly volatility. The 13-week raw increment contains passing variance. Measure early short-to-late long covariation relative to early short-to-late short, a disjoint-block difference of variances, and the head's actual/fitted one-week variance. Test whether smooth variance fitting discards boundary bands and whether its head error remains after accounting for endpoint EB. If the fitted *total variance* is already much too large at h=1, repair identification before changing the transition law.
2. A surge fit on pooled cumulants may confuse a skewed lasting random walk (third cumulant proportional to h) with a transient wave whose half-life exceeds the observation window. Profile the baseline half-life with the mandatory linear-in-h competitor and inspect rank-stratified third/fifth cumulants. A positive marginal third cumulant on its own does not identify the sign of a cross-window conditional statistic. Check whether IG's signal disappears under averaging or instead survives at multiweek scales.
3. Separate 39-member Monte Carlo jitter from the future top-K normalization. Compare 39 and 195 draws, both on the stochastic and fixed training-week median, to the analytical S2 point at all five gold origins, one week ahead. Look for a common directional shift equal to the change in the simulated denominator.
4. If per-band marginal variance at short lags is misestimated, fit the existing variance laws directly to the variogram, including zero NNLS components. No CRPS, gate, coverage, or head outcomes enter this estimation.

Use one final benchmark at full precision. Assess both S2 and S3 on all panels and all available horizons. A universal adoption needs improvement in Submissions mobility and calibration, preserved forecast skill and no material multi-platform harm. A gate increase of 0.05 is a descriptive comparison convention (§2z-am), not an adopted threshold. A material loss of head structure, departure fidelity, asymmetry or time transfer also weighs against adoption. Negative findings are retained.

## Training diagnostics and candidate decision, recorded before edits

Baseline synthetic suite 14/14. The four-panel numerical records are diag_moments.json, diag_scale.json, diag_lasting.json, diag_surge.json, diag_surge_rank.json and diag_forecast.json. Their namesake scripts import the immutable baseline. All 20 rolling-origin point comparisons are included. Panel order below is FB, IG apparatus, Comments, Submissions.

- Cross-half lag-13/lag-1 spread ratios were 0.908, 0.921, 1.012, 0.963. Cross-lag slopes were 0.806, 0.877, 0.883, 0.856. Fitting a separate h-linear variance component in the opposite half, high/low volatility tertile ratios for lasting variance were 1.20, 1.17, 0.58, 0.70 against 3.32, 3.33, 3.85, 2.85 for weekly variance. Retain homogeneous lasting shocks. The identification estimate is noisy (especially IG). On Submissions, naive gamma=0.884 actually gives first-band mean v^gamma=1.263. Normalizing only the global expectation cannot reduce that band's scale.
- The proposed uniform head-variance excess was falsified. Full-window Submissions fitted/raw one-week variance ratios are 0.735, 0.747 in the first two bands and 1.43, 0.99, 1.40, 1.44, 0.89 beyond. Keep the existing aggregate variance estimator for this comparison.
- In Submissions the correlations with future squared median absolute change were 0.501 for early mean-square change and 0.678 for early squared median absolute change, after rank residualization. Same numbers on FB/IG/Comments are 0.631/0.682, 0.604/0.489, 0.595/0.693. The IG exception is a prospective risk. At K, first-half Submissions median observed mean-square/fitted variance is only 0.365 while its mean is 0.837. Implement a conditional median-absolute-increment instrument for passing volatility, identifying its scale from the fitted innovation law. Shrink against the existing lognormal prior. Do not force a mean-one empirical posterior among historically selected endpoints.
- Finite-member and denominator decomposition is in diag_forecast.log. At h=1, increasing 39 to 195 members moves the future-denominator MAE from 0.41163 to 0.40483 on FB, 0.66178 to 0.65150 on IG, 0.29231 to 0.28715 on Comments and 0.17343 to 0.16846 on Submissions. On IG the stochastic denominator is +0.10222 above the last training median and the fixed-denominator 195-member MAE is 0.63964. Mean raw draw minus analytical prediction tracks the balancing drift on each panel. Leave the evaluator and the simulated share/rank denominators intact.
- Bounded characteristic-function phase fits on the same marginal increments yielded (half-life, jump size) = (39,1.016), (52,0.734), (52,1.413), (52,1.100). Removing fifth powers did not resolve the slow-wave interpretation. A signed linear-in-h competitor almost exhausts the Reddit third cumulant, while IG four-week-block increments still have κ3=0.181. The independent adjacent-window check has the opposite sign to a fading positive surge at all 12 panel/lag combinations. At h=8, gap=1, empirical E[x²y] is +0.0518, +0.0642, +0.0078, +0.0196. All four isolated positive-wave predictions are negative. The pure-wave diagnostic numbers use the pre-budget fitted C3 for magnitude and the sign is exact. Switching the rank proxy improves some panels and worsens others. Neither changing the half-life grid nor choosing a platform slope is justified.
- Candidate S3 retains the original conditional jump law only where its necessary mixed-moment sign survives a training-only check. Use one mean of standardized mixed moments across the 4, 8 and 13 week disjoint differences, separated by one unused week, with positive-sign evidence above a one-sided 95% entity-cluster normal bound required to suppress the positive wave. This is an identifying restriction on the candidate. A suppression does not prove no real-world surge. It leaves persistent drift independently estimable. Synthetic tests will check positive-wave recovery and failure of the restriction for a wrong-sign process.
- Two separable changes. The conditional volatility estimator acts only in filtering. The sign restriction acts only when fitting the optional S3 wave. Predict a reduction in Submissions interval overcoverage of about 1–3 percentage points, lower short-horizon median displacement and X CRPS, approximate preservation on FB/Comments, with IG a risk. S2 unconditional structure must be exactly identical at common seeds. S3 attenuation should reduce FB/Comments head and boundary overshoot. Predict loss of some Reddit D if its wave is suppressed. Gate v1, out-of-period head stability, deciles, departures and endpoint comparisons are assessed without subsequent refitting to outcomes. No new fitted scalar. Test sign restriction alone and scale estimation on synthetic data. If either changes an intended outcome adversely, record it.

## Component attribution supplement

The project archives for the earlier gamma ablation also change phi (FB 0.45→0.84, IG 0.78→0.93, Submissions 0.84→0.89). Those results cannot isolate causation by gamma. The cross-fitted variance decomposition above is independent evidence about the coupling.

The two bundled changes only coexist in S3. To identify their separate effects, run the two off-diagonal S3 combinations on all four panels using the same split and member counts, with no parameter search. The first FB gate summary from the primary run was already visible at this decision. The two off-diagonal columns are (A) original S3 estimation + candidate filter and (B) candidate S3 estimation + original filter. Unconditional structure for A and B is identical by construction to the original and candidate S3, respectively. Evaluate Gate v1, individual forecasts and both conditional Reddit period transfers. Keep this result separate from the mandatory benchmark and retain every panel. The no-change fair head Brier omitted by the frozen export is computed separately on its exact cohort. A point forecast's activity CRPS is its MAE.

## Input and implementation manifest

| File | SHA-256 | Cached weekly labels |
|---|---|---|
| facebook_a.npz | `db6c16cc0944b287d7e51a248306f3b1a81e6d98b5329153ba3cce6567586588` | 2020-11-02 … 2022-06-27 |
| instagram_hm.npz | `dc8538f33173c99ee62dccf5dfdffcb6fbc26b8dcaf84e5c68a77ee1d8a42c49` | 2023-01-02 … 2023-12-25 |
| reddit_comments_ext.npz | `dc6fbcf98d953e79252460493efeb213188672632373a6a63c46f9a4a25a4e61` | 2018-11-26 … 2022-12-19; train end 2020-11-16, test start 2020-11-23 |
| reddit_submissions_long.npz | `a9110619e0696e6b0f11001d656ee2876126c9e4676473e5419b530da32dd654` | 2018-12-03 … 2022-12-19; train end 2020-11-23, test start 2020-11-30 |
| facebook_a_spec_b.json | `42ad0c139c2ae29d4a7a4d34a25d894f2e090639a3780a67b2da96656bf4eb75` |  |
| benchmark_candidate.py | `d3b2618b516ecad1cc35d9f5741b53334dfbc9583b5b1a306856b2840ed71272` |  |
| round3_frozen_gate.py | `616ba88dc08d7f2cb5e94952b2d44aeda8b812220a68b61631e26dffd1cd15fd` |  |
| round3_gold_benchmark.py | `44d68fa134b8e3905630ec9baaaeac20899a87dbdd218d1a204561064065b6c5` |  |
| rankdiff_kalman.py | `20496a90b6c9003b1f0bc457bcbe2160ca97be830612732ad995cfccf8977c7f` |  |
| round3_frozen_gate_results.json | `2d1eca491bfc474fe07638b63d2421da6ad27b4416f0973fb26e2ccf0adae6cf` |  |
| round3_gold_benchmark_results.json | `338f872e2cf1479e6132ed482701057b06e2165f984d32ed2c3d2880f329e2fa` |  |
| common_rankdiff_astra.py | `0dff33558e04f899522ba806ebf08951892dc0a093a9fc44ba539d75bc554c0f` |  |

## Calendar audit and extra forward evaluation

Reading only the dates in the two approved Reddit NPZs revealed that the first 104 weeks of each full cache end in November 2020. The mandatory evaluator is running unchanged. To fulfill the calendar-transfer request, separately run the same OOP function and 20 seeds after selecting weekly labels in 2019–22, verifying its 104-week prefix is exactly the 2019–20 subset, and compare the same observed 2021–22 target under the original and candidate models. This supplement follows a date rule and adds no model choice. The common historical 4K buffer is still a development limitation.

The authorized 2021–22 cache contains 103 weekly labels, Jan 4 2021–Dec 19 2022. A frozen 52-week-block statistic yields only one complete block and no year-to-year turnover. Supplement that row with paired 51-week calendar blocks, the first 51 recorded weeks of each test year. The model still advances every intervening calendar week, including the 52nd week of 2021. Reuse the OOP initial-state sampler and 20 seeds, computing block shares, ladder spans and turnover from those same draws. No missing week is imputed.

## Commands and seeds

Run from this directory. In this session PYTHONDONTWRITEBYTECODE=1 prevented imports from writing in the parent repository. The job wrapper used RANKDIFF_HEAVY_LOCK="$PWD/.benchmark_lock" to confine bookkeeping to the authorized directory.

```sh
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
export PYTHONDONTWRITEBYTECODE=1
"$PY" -m pytest test_common_rankdiff_astra.py -v -p no:cacheprovider
"$PY" -u diag_moments.py
"$PY" -u diag_forecast.py
"$PY" -u diag_scale.py
"$PY" -u diag_surge.py
"$PY" -u diag_lasting.py
"$PY" -u diag_surge_rank.py
"$PY" -m pytest test_common_rankdiff_astra.py -v
"$PY" -u ../benchmark_candidate.py --module common_rankdiff_astra.py --out-dir . --mode both
"$PY" -u diag_ablation.py
"$PY" -u diag_recovery.py
"$PY" -u diag_reference.py
"$PY" -u diag_presence.py
"$PY" -u diag_calendar_oop.py
"$PY" -u diag_calendar_blocks.py
```

The first pytest command was run on the untouched candidate and the second after implementation. Each diagnostic has a namesake .log and .json. Gate seeds are 1000 × split index + 0…19. In-sample structure uses 300…319. Forecast members use 2000 × split index + 0…38. Both calendar definitions use 7000…7019 for conditional time transfer. The forecast diagnostic extends each of the five one-week seed banks to 195; it is separate from scoring. The program's shared ledgers and canonical record are read-only for this task.

## Additional measured results

- Whole-panel and per-origin decisions from the implemented raw mixed moment, including SEs, are in diag_recovery.json. Synthetic suppression counts out of 24 at T=52/86/212 were 1/4/1 under symmetry, 0/0/0 under each positive-surge law, and 24/24/24 under the wrong-sign rival.
- Under the clean and contaminated scale-recovery designs, mean absolute log-scale errors (old → candidate) were 0.285→0.361 and 0.512→0.384 at 30 weeks; 0.181→0.225 and 0.379→0.233 at 100 weeks. The efficiency loss on clean data is real.
- The complete-case Instagram measurement (post-design) retained 3,902 IDs and gave D = −0.071 [−0.126, −0.012], marginal κ3(8)=+0.068. Intermittently absent IDs (4,954) gave D = −0.095 [−0.136, −0.046], κ3(8)=+0.205. See diag_presence.json for all four panels. The comparison holds the original within-stratum centering fixed.
- The diagnostic decomposition of the simulated Instagram denominator from the final training median was −0.294 at filtered points on the last observed mask, −0.223 on forecast masks, −0.156 after posterior sampling and +0.102 after the weekly transition/observation. All panels and all five origins are in diag_reference.json.

To close the confounding in the archived Stage 2 comparison, also evaluate the old optional fit_gamma_L=True in the immutable Round 3 S2 estimator on the exact gold origins and M=39 activity-forecast cohort. This diagnostic changes only that switch and never enters candidate selection. Reuse baseline rows when the estimator returns exactly zero. Activity has no stochastic PIT dependence for the CRPS or ordered interval.

## Primary frozen gate, completed before the gold verdict

| Panel | Candidate S2 ± SD / coverage / wins | Candidate S3 ± SD / coverage / wins | Historical mobility ± SD |
|---|---|---|---|
| facebook_a | 0.084 ± 0.037 / 80% / 4/5 | 0.078 ± 0.029 / 80% / 5/5 | 0.147 ± 0.030 |
| instagram_hm | 0.282 ± 0.219 / 80% / 4/5 | 0.304 ± 0.230 / 80% / 4/5 | 0.869 ± 0.554 |
| reddit_comments_ext | 0.134 ± 0.085 / 80% / 3/5 | 0.118 ± 0.064 / 100% / 4/5 | 0.158 ± 0.038 |
| reddit_submissions_long | 0.263 ± 0.067 / 60% / 0/5 | 0.214 ± 0.078 / 60% / 2/5 | 0.165 ± 0.044 |

Submissions mean across the five split medians (empirical / Round 3 / candidate) was 3.6/5.2/4.2, 4.8/7.2/6.4, 7.4/11.6/10.6 for S2 dRank1/4/13. For S3 the corresponding values were 3.6/4.6/4.2, 4.8/6.8/5.8, 7.4/11.2/10.0. RACF1 went further above observed on both. Comments S3 worsened overall. All split and eligible moment errors are preserved in candidate_frozen_gate_results.json.

The isolated original-gamma forecast control finished on every gold origin. Commands and all fold metrics are in diag_gamma_control.py/.json. The switch was inactive on FB and IG. On Comments gamma was 0,1,1,1,1; on Submissions 0,.984,.976,.990,.917. At 1/4/13/26/52 weeks its paired activity CRPS changes were +.000312/+.000323/+.000747/+.002675/+.012340 (Comments) and +.000335/+.000620/+.000815/+.001937/+.003753 (Submissions). Its Submissions 52-week coverage moved .944→.914. Every nonzero performance change here is attributable to the single switch and its consequent refit of rank-balancing drift.

## Completed benchmark and verdict

The prescribed --mode both process exited 0 and saved both candidate archives. Goal 1 uses 20 seeds; Goal 2 uses M=39 and five gold origins per panel. Both frozen Reddit forward tests completed. diag_calendar_oop.json separately contains the exact calendar test under both model versions. diag_calendar_blocks.json contains the two matched 51-week blocks for every seeded law. The additional gamma-only control is reproduced with `"$PY" -u diag_gamma_control.py`.

The full archive audit (diag_verify.py → verification.log) passed. The repeated local test suite passed 16/16 in 3.98 seconds. The checkout-wide health guard was not run under the owner’s input restriction. No sealed or exposure-unknown panel was opened. The source and harness hashes match the manifest. The complete machine-generated §5 tables are in ASTRA_REPORT.md; regenerate them with diag_result_tables.py. diag_summary.py produces the cross-panel comparisons in diag_summary.log. GATE_ROWS.md contains all 16 primary and factorial gate evaluations. STATUS_ENTRY.md is the local record handoff.

**Prediction scores.** Submissions movement and calibration improved together, with S2 activity and log-rank CRPS lower at every horizon. S2 coverage moved .944–.952 → .924–.935. Preserving cross-platform proper scores was partial: S2 improved activity CRPS in 13/17 and log-rank CRPS in 10/17; IG deteriorated on both at every scored horizon. S2 unconditional structure matched the original exactly. FB/Comments head turnover and boundary improved under the S3 sign check. The Reddit decile hook and positive D failed and both full-window terminal head shares worsened. Exact-calendar Submissions S3 D changed +.109 → −.018 against +.146 observed. The full-window scalar budget was met on all panels. All failures, ablations, seed bands, split SDs, and calendar conventions are reported in ASTRA_REPORT.md.

**Verdict.** Retain the robust filter as an opt-in research result. Keep Round 3 as the reference model and reject the bundled cross-platform replacement. Neither the partial filter nor the sign check alone dominates all aggregate and endpoint objectives. Nothing adopted or pushed.
a898b3272f1e63e5089b222698cd06d1d5f57e75861111c1b749e0d3e3a5fd2d  candidate_frozen_gate_results.json
f7bfec4e3b1a4f0fa0b747f892f480c681008b4e928c345bc00f879337027ea7  candidate_gold_benchmark_results.json
583d37320cf01f6a5c55fc2d7e3bc54b7ef5bcd6184e6413d261103aa80d8f9d  diag_calendar_oop.json
f91967534994192d1c8dfb557980ee0930a9a176e4b560ec6ef4fdd57aaa8bc5  diag_calendar_blocks.json
67210aedc1b444d2be88fb1714412b1fb8520cfe9f7342a86e952de91aa6fb2e  diag_ablation.json
