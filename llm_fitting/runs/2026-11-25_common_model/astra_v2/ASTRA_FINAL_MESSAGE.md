**Partial accept. Keep Round 3 as the cross-platform reference and retain the new endpoint-volatility filter as an opt-in candidate.** The filter improves Submissions movement and calibration without adding a scalar. The bundled S3 change loses the observed Reddit asymmetry.

| Exact Gate v1 error, mean over five splits | Round 3 S2 → candidate | Round 3 S3 → candidate |
|---|---:|---:|
| Facebook | 0.105 → **0.084** | 0.108 → **0.078** |
| Instagram apparatus | 0.296 → **0.282** | 0.360 → **0.304** |
| Reddit Comments | **0.129** → 0.134 | **0.098** → 0.118 |
| Reddit Submissions | 0.346 → **0.263** | 0.310 → **0.214** |

**Submissions improved on more than the gate.** S2 lowered activity CRPS and log-rank CRPS at all five horizons. Its 90% interval coverage moved from 0.944–0.952 to 0.924–0.935, still above the 0.900 target. Applying the filter alone to Round 3 S3 gave the best Submissions candidate gate result, **0.198 ± 0.079**, against historical mobility at **0.165 ± 0.044**. On Instagram, activity and rank CRPS worsened slightly at every scored horizon.

**The structural trade-off decides the recommendation.** S2 reproduced every Round 3 in-sample structure statistic and seed band exactly. The S3 change corrected Facebook’s top-10 turnover overshoot, but reversed the sign of Reddit asymmetry. On the exact-calendar Submissions forward test, S3 predicted \(D=-0.018\) [−0.046, 0.020], against **+0.146 observed** and **+0.109** from Round 3.

The full four-panel benchmark completed, and **16/16 tests passed**. I also ran a separate calendar-aligned comparison after finding that the supplied evaluator’s nominal 2021–22 test begins in November 2020. All work stayed in `astra_v2/` and used only the authorized development inputs.

[Full report and before–after tables](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/astra_v2/ASTRA_REPORT.md) · [Experiment log](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/astra_v2/EXPERIMENT_LOG.md) · [Candidate implementation](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/astra_v2/common_rankdiff_astra.py)