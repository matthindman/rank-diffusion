# Round 7 experiment log · 2026-11-26

## Before the evaluation

The starting workspace module and repository research module both had SHA-256 `3ac654e22618e171e0a1bb665f17aea9c230ecfc62aabae3fa81b03334b9e58b`. The repository HEAD was `e720db3986a39a2f42c5b398c563b274833c0b7a`, with pre-existing uncommitted research changes. The data contact was limited to the four anonymous DEVELOPMENT cache files named in the brief and Facebook’s cached daily replication instrument. The repository exposure ledger identifies this cache in its 2026-11-25 entries (§2z-dx through §2z-dz). The frozen scripts load the authorized caches and compare against prior result JSONs. The repository-wide health script also runs against a raw Facebook file outside the allowed inputs, so the isolated synthetic suite is the applicable check here. Bytecode is disabled and temporary files, caches, locks and logs are routed inside the workspace.

### Measurement before changing the model

I fitted the unmodified module with the cache-supplied absence floors, the Facebook instrument, and drift refinement off. For each permanent-rank band I compared the existing Gaussian jump variance with the measured positive mean-square minus robust-core lasting variance gap. The complete diagnostic is in `diagnostic_r7.log`. In head band 0 the gap / assigned variance were 0.00014 / 0.00295 (Facebook), 0.00011 / 0.00256 (Instagram), 0.00158 / 0.00174 (Comments) and 0.00248 / 0.00051 (Submissions). The error changes direction. A pooled rescaling of the existing rank profile cannot fix both heads. The core fit is also smoothed independently of the jump variance, so their sum need not equal the fitted smooth mean-square budget.

For S3 I charged the fall in the mean-square lasting slope after adding surge and drift against the S2 gap. Submissions had zero residual budget in all seven octaves. Comments had a positive residual only in the last octave. A test that forces symmetric jumps into both Reddit S3 arms would spend variance twice under this accounting. A large apparent robust gap on Instagram coexists with almost zero full-sample lasting variance at the head. I leave the fifth-cumulant surge estimator alone this round to isolate the lasting-noise change. Both a true rarity gradient and uncertain local variance decomposition remain possible. The first diagnostic line fit was unweighted and descriptive.

The artifact provenance matters. The staged Round 6 Gold S3 counts are 15/14/16/15 in FB/IG/Comments/Submissions order. The supplied source gives 14/14/15/15 on the same full panels. Compare directly with the staged files as requested and use the supplied-source arm for attribution where possible. The copied test file also still imports the repository module name. Repair its import before running it.

### Candidate and decision rule fixed before implementation

On both arms use the same Gaussian mixture as Round 6, at the inherited fixed weekly rate of 0.02 when active. Preserve the arm’s full mean-square lasting size law. Use the symmetric h≤13 robust scale fit as a common reference, never subtracting surge variance from an IQR. In band b, let t0 and tA be the mean-square lasting slopes without and with the arm’s optional components, and c the jump-free symmetric robust slope. The upper budget is max(0, min(t0−c, tA−c, 0.85 tA)). Project these budgets onto a single affine function of log permanent rank by the existing capped-octave weighted least squares, subject to lying below every budget. Take its positive part, extend it flat at both outer knot centers, and cap the result at 85% of the actual smooth total at every simulated level. Activate only if the **allocated** weighted variance exceeds the inherited 20% share of the total lasting budget (including one-step variance of S3 components). Missing support supplies no positive budget. The residual variance goes into diffusion. No platform labels. The two coefficients replace one coefficient when active. The fixed event rate and 15% minimum diffusive share are inherited conventions.

Pre-run predictions, compared to staged R6:

1. S2 FB top-10 turnover should drop at least 0.05 toward 0.350. Submissions should rise at least 0.025 toward 0.300 and its head share should fall toward 0.356. Compare medians and 20-seed bands.
2. Instagram S2 head noise should decline, improving Gate by about 0.01 or more. Expect one-week X CRPS to change by less than 0.005. With more diffusive variance traveling through the Submissions body, a material improvement in its overcoverage is unlikely (less than 0.01).
3. The S3 residual rule should leave full-panel Reddit surge/drift as the carrier of positive D. Expect no sign reversal or increase >0.03 in the mean absolute error across ten deciles of either Reddit directional hook. A large Submissions S3 coverage gain is not predicted. Instagram wrong-sign D is expected to persist.
4. Keep each arm’s Gate mean within +0.05 of staged R6, the project’s comparison convention. Expect boundary-ratio changes within 0.05, departure-rate changes within 0.005, and no new systematic worsening of the ladder or out-of-period Reddit structure. A materially worse gate, failure of the head predictions, or an unsupported extra coefficient argues against adoption. Score these predictions after one exact benchmark. No score-based refits or panel-specific adoption rules.

The initial isolated suite on the unmodified source passed 23 tests. After implementation, 27 tests passed including independent gradient plants (seeds 431 and 853), an exact constrained-profile test and a 40-step variance/tail check. The primary specification is locked for the single benchmark invocation in `benchmark.log`. The runner uses the registered interpreter, workspace-local heavy lock (the shared lock would write in /tmp), caffeinate, one BLAS thread, five Gate origins × 20 replicates, 2,000 bootstrap draws, 20 structure seeds and 39 forecast members. Route in `run_benchmark.sh`.

### Additional checks specified before reading scores

The frozen Gold transport routine cuts at row 104. The archived calendar audit and the cache builder indicate the weekly rows can start in late 2018. Run the identical dynamics, seeds and frozen transport statistics after selecting calendar years 2019–22 from the permitted caches. Compare the supplied R6 source (`scale_moment="meta"`) and R7 on identical train/test rows. Include year-to-year turnover using 51 observed weeks at the start of each test year because only 103 test weeks are available. Report this beside the official output. Also record per-panel jump budgets at the full fit and Gate training lengths to distinguish stable identification from moving activation; these diagnostics use train-window moments exclusively and do not score or refit a gate.

An independent recovery sensitivity uses seeds 431, 853 and 1289; 180 weeks, K=1,600, N=3,200, a broad log-rank ladder, weekly total lasting variance 0.018 and Gaussian observation SD 0.04. Plant no jumps, a rising jump budget, its reversed gradient, and a rising budget plus a fading surge (rate 0.015, scale 0.65, 13-week half-life). Recover the sign, coexistence, false activation and retained weekly variance with the same estimator. This tests the conservative projection where a small band is noisy; all configurations and seeds are retained irrespective of outcome.

The 12-case recovery stress test (`recovery_validation.json`) recovered the rising gradient on all three seeds, kept jumps off in all three nulls, failed to recover the falling gradient on all three seeds, and recovered surges while missing the permanent jumps on all three coexistence plants. There was one false-direction activation in the falling-gradient design. These are limitations of the estimator even when its local caps are exact. A legacy compatibility check on the rising and coexistence plants matched all structural fields, filtered states and the simulated observations, ranks and identities to the starting source with `scale_moment="meta"`.

The expanded 30-test suite initially reported two NameErrors after the independent planter was shared between the unit suite and recovery study. The old assertions referenced the moved local array. I restored their original numeric bounds using the explicitly planted body variance of 0.014 and reran the complete suite. The model and benchmark process were unaffected.

The corrected candidate suite passed 30/30 in 46.45 seconds (`candidate_tests.log`). The candidate module SHA-256 at benchmark launch and final test was `17ad9b9d6a049dde0f7de8f9ca22d01e4b8e42e5c5ef225f1e53991d5958acf3`. The pre-benchmark diagnostics and full in-K budgets are available in `diagnostic_r7.log`, `split_diagnostics.json` and Figure 1 of the report. There were no discarded gate specifications.

The additional calendar test completed 20 seeds for each Reddit panel, arm and source version. The full frozen row-104 tests start on 2020-11-23 (Comments) and 2020-11-30 (Submissions). Selecting the dates first gives 104 training weeks and 103 test weeks, 2021-01-04 through 2022-12-19. Exact-calendar top-10 turnover is unchanged from source R6 in both arms and on both Reddit panels. This is a separate time-transport check within the DEVELOPMENT caches.

To localize the brief’s Instagram CRPS and Submissions coverage residuals, run a supplementary S2 breakdown with the original Gold origins, M=39, truth-defined cohort and conditional draws, for both source R6 and R7. Partition by training permanent rank (1…K/16, K/16…K/2, K/2…K). Record below/above-interval fractions, width, absolute error, signed median-forecast error and CRPS. Validate the pooled R7 coverage and CRPS against the official JSON. This post-design diagnostic introduces no parameter or candidate selection.

### Reproduction fingerprints

Python 3.11.6, NumPy 2.0.1, SciPy 1.14.0, pandas 2.2.2. SHA-256 for the executed evaluators, staged comparison artifacts and the permitted inputs follows.

| Input | SHA-256 |
|---|---|
| candidate_frozen_gate_results.json | `078484135e759d2a2c2f325e8fe07264e74bccd14d3dc3937eaa89726113ed1c` |
| candidate_gold_benchmark_results.json | `ddc1ea94e51904499264261bb1e88ef715c7d53ef848315fd07417389d6e6735` |
| benchmark_candidate.py | `d3b2618b516ecad1cc35d9f5741b53334dfbc9583b5b1a306856b2840ed71272` |
| round3_frozen_gate.py | `616ba88dc08d7f2cb5e94952b2d44aeda8b812220a68b61631e26dffd1cd15fd` |
| round3_gold_benchmark.py | `44d68fa134b8e3905630ec9baaaeac20899a87dbdd218d1a204561064065b6c5` |
| facebook_a.npz | `db6c16cc0944b287d7e51a248306f3b1a81e6d98b5329153ba3cce6567586588` |
| instagram_hm.npz | `dc8538f33173c99ee62dccf5dfdffcb6fbc26b8dcaf84e5c68a77ee1d8a42c49` |
| reddit_comments_ext.npz | `dc6fbcf98d953e79252460493efeb213188672632373a6a63c46f9a4a25a4e61` |
| reddit_submissions_long.npz | `a9110619e0696e6b0f11001d656ee2876126c9e4676473e5419b530da32dd654` |
| facebook_a_spec_b.json | `42ad0c139c2ae29d4a7a4d34a25d894f2e090639a3780a67b2da96656bf4eb75` |

## Completed frozen gate · local handoff for the gate ledger

One benchmark invocation evaluated four panels and two arms. Every origin completed. The score is mean relative error ± between-origin SD. Medians in the bootstrap CI and wins against historical mobility use all five origins. No other Round 7 gate was evaluated.

| Date | Session | Source | Panel | Kind | Spec | Precision | Model | Historical | Coverage | Artifact | Note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | facebook_a | candidate | bounded S2 | 5 × 20, boot 2000 | 0.083 ± 0.038 | 0.147 ± 0.030 | 80% | candidate_frozen_gate_results.json | 4/5 wins |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | facebook_a | candidate | bounded S3 | 5 × 20, boot 2000 | 0.091 ± 0.016 | 0.147 ± 0.030 | 80% | candidate_frozen_gate_results.json | 5/5 wins |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | instagram_hm | candidate | bounded S2 | 5 × 20, boot 2000 | 0.282 ± 0.219 | 0.869 ± 0.554 | 80% | candidate_frozen_gate_results.json | 4/5 wins |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | instagram_hm | candidate | bounded S3 | 5 × 20, boot 2000 | 0.304 ± 0.230 | 0.869 ± 0.554 | 80% | candidate_frozen_gate_results.json | 4/5 wins |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | reddit_comments_ext | candidate | bounded S2 | 5 × 20, boot 2000 | 0.118 ± 0.062 | 0.158 ± 0.038 | 80% | candidate_frozen_gate_results.json | 4/5 wins |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | reddit_comments_ext | candidate | bounded S3 | 5 × 20, boot 2000 | 0.124 ± 0.057 | 0.158 ± 0.038 | 100% | candidate_frozen_gate_results.json | 4/5 wins |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | reddit_submissions_long | candidate | bounded S2 | 5 × 20, boot 2000 | 0.209 ± 0.068 | 0.165 ± 0.044 | 80% | candidate_frozen_gate_results.json | 2/5 wins |
| 2026-11-26 | Codex Round 7 | e720db3 + local 17ad9b9d | reddit_submissions_long | candidate | bounded S3 | 5 × 20, boot 2000 | 0.198 ± 0.079 | 0.165 ± 0.044 | 100% | candidate_frozen_gate_results.json | 3/5 wins |

A post hoc synthetic oracle clarified the reversed-gradient failure. For the planted ranks actually recovered in each training sample, I replaced estimated gaps with known planted weekly band variances. The feasible line then represented 19.96%, 20.05%, 19.97% of total variance in the falling design, almost exactly the inherited 20% threshold. The rising counterparts were 54.50%, 54.20%, 54.03%. Thus even infinite moment precision at fixed empirical rank ordering would switch the falling line off on two seeds. To distinguish this threshold effect from failure to estimate the gradient, repeat the three falling plants with an additive common jump variance of 0.007 (half the planted maximum) and add the same 0.007 to total variance, keeping diffusion, rate, gradient, seed and sample size fixed. This is a post hoc synthetic sensitivity only; the candidate stays fixed.

The stronger falling-gradient sensitivity had true feasible shares around 42%, yet activated with the correct sign on 1/3 seeds. The other two switched off. The final rerun after adding this optional synthetic-fixture offset passed all 30 tests in 44.73 s. The evaluated candidate source remained byte-identical. [Oracle output](planted_oracle.log)


## Completed Gold benchmark and decisions

**RAN-OK.** Both modes completed in one invocation, in the required order, using the registered interpreter and unmodified parent evaluator files. The final Gold contains all four panels, 20-seed bands, all 17 available panel–horizon cells, both arms, five origins, 39 members, and both frozen Reddit transport tests. Gaussian oracle coverage was 0.89855 and PIT SD 0.28951. The report tables were generated directly from the JSONs by `compare_rounds.py`. This script checked the empirical and historical comparator values, five-origin alignment, the exact-calendar split, finite primary metrics, and the supplementary rank-stratified scores against each official origin. Runtime empty-slice warnings on sparse histories appear in the benchmark log. All required primary scores are finite.

**Predictions.** (1) PARTIAL. FB turnover fell 0.075. Submissions rose 0.017, less than the predicted 0.025, and its head share fell 0.020. (2) PASS. IG Gate improved 0.024 and one-week X CRPS moved less than 0.005. Submissions S2 coverage moved less than 0.01 at each horizon. (3) PASS on the predicted preservation and persistence of limitations. Both Reddit S3 D values stayed positive and the ten-decile error changes were under 0.03. IG S3 D remained positive against negative empirical D. Submissions S3 remained overdispersed. (4) PARTIAL. All eight gate, boundary and departure-rate guardrails held. Ten of twelve non-Facebook final S2 ladder spans worsened. Frozen Submissions transfer head share worsened while its boundary ratio improved. Full bands and paired-origin uncertainty are in the report.

**Verdict.** Reject R7 as the common replacement. Compared with staged R6, S2 improves the gate on two panels and worsens it on two. Submissions moves 0.178 → 0.209 and Comments loses X and log-rank CRPS at every horizon. The full-panel and gate S3 fits revert to the incumbent source law on all four panels. No post-score model refit, fresh confirmation, production edit, shared-ledger edit or commit was performed. The exact-calendar, recovery and forecast-stratum diagnostics use the locked candidate. The shared status and gate handoffs are local to this workspace.

- candidate_frozen_gate_results.json · SHA-256 `728a6d03a3a60687caf2508e9f008111c9f9e3ff34fc2a30e51d70e200514338`
- candidate_gold_benchmark_results.json · SHA-256 `56179fd8f16504d42d0e59d785e76d6947ac62ee48629fcdfbe5e8f954e60197`

An activation audit of the 20 S3 Gold endpoint fits found one positive jump allocation, on the 22-week Instagram origin (20.094% feasible share), where both surge and persistent drift were zero. All remaining Gold S3 origins switched jumps off. Thus none of the full-panel, Gate or Gold endpoint fits identified simultaneous surges and permanent jumps. [Gold refit diagnostics](gold_s3_activation.json)

