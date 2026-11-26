## 2026-11-26 — EXPLORATORY DEVELOPMENT · bounded lasting-jump profile tested on four common-model panels; reject replacement, no change adopted

OpenAI Codex, Round 7, workspace `llm_fitting/runs/2026-11-25_common_model/astra_v3/`. Starting repository HEAD `e720db3986a39a2f42c5b398c563b274833c0b7a`, incorporating the owner’s supplied uncommitted R6 snapshot (SHA-256 starts `3ac654e2`). The tested candidate hash starts `17ad9b9d`. This local entry and `GATE_ROWS.md` are the handoff for the shared research record.

**Question.** Can a bandwise variance budget address opposite head allocation errors on Facebook and Submissions while allowing S3’s fading surges and persistent drift to coexist with permanent jumps? The inherited profile assigned weekly variance 0.00295 against a measured 0.00014 in Facebook’s head band and 0.00051 against 0.00248 on Submissions. The candidate projects residual band budgets onto a bounded affine function of log permanent rank and preserves the smooth total variance at every level. Two moment-identified coefficients replace one when the split activates. S2 full-fit counts are 13/13/11/13. S3 counts are 14/14/15/15.

**Gate v1.** Each entry is the mean ± SD over five origins, 20 replicates per origin and 2,000 bootstrap draws. All eight rows include the historical baseline, interval consistency and win counts in [the local gate handoff](GATE_ROWS.md).

| Panel | Staged R6 S2 → R7 S2 | Staged R6 S3 → R7 S3 |
|---|---|---|
| Facebook | 0.089 ± 0.037 → 0.083 ± 0.038 | 0.095 ± 0.018 → 0.091 ± 0.016 |
| Instagram apparatus | 0.306 ± 0.232 → 0.282 ± 0.219 | 0.340 ± 0.248 → 0.304 ± 0.230 |
| Reddit Comments | 0.113 ± 0.035 → 0.118 ± 0.062 | 0.177 ± 0.110 → 0.124 ± 0.057 |
| Reddit Submissions | 0.178 ± 0.066 → 0.209 ± 0.068 | 0.201 ± 0.069 → 0.198 ± 0.079 |

**Pre-run predictions.** PARTIAL on head turnover. For S2, Facebook moved from 0.475 to 0.400 [0.298, 0.650] against 0.350 observed. Submissions moved from 0.150 to 0.167 [0.065, 0.267] against 0.300. Bands are across 20 seeds. PASS on the direction and size of the IG gate/short-horizon CRPS change. PASS on preservation of Reddit S3 D and hook shape and on the predicted persistence of the IG sign error and Submissions overcoverage. PARTIAL on safeguards. All eight gate changes stayed within +0.05, but final and out-of-period ladder/head-share errors were mixed. Comments S2 lost X and log-rank CRPS at all five horizons. The bounded split switched off in all 20 S3 gate training fits, recovering the adopted R4/source-R6 S3 gate scores. The staged R6 S3 file differs from the brief and the supplied source; comparisons are labeled in the report.

**Mechanism and decision.** Independent plants recovered a rising jump gradient on 3/3 seeds and stayed inactive on 3/3 nulls. At an oracle fraction close to 20%, a head-concentrated gradient was never correctly recovered. Even a stronger falling-gradient sensitivity activated with the correct sign on just 1/3. All three joint plants identified surges while missing the permanent jumps. Recommend retaining R6. The results do not support promoting the extra parameter or a platform-specific rule.

**RAN-OK.** `bash run_benchmark.sh` ran the exact Gate and Gold via the specified external runner. `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m pytest -q -p no:cacheprovider test_common_rankdiff_astra_r7.py` passed 30 tests. The calendar sensitivity used 104 training weeks and 103 test weeks on both Reddit panels with 20 seeds per arm. All four panel inputs and the Facebook instrument were the authorized DEVELOPMENT cache files. Repo-wide raw-panel health check and held-out confirmation were outside scope. [Report](ASTRA_R7_REPORT.md) · [Experiment log](EXPERIMENT_LOG.md)
