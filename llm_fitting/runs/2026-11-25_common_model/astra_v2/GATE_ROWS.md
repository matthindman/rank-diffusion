# Gate ledger handoff

Exact frozen scoring on the four authorized DEVELOPMENT caches. See ASTRA_REPORT.md and EXPERIMENT_LOG.md.

| Date | Session | Commit | Panel | Kind | Specification | Splits × reps | Model error ± SD | Baseline ± SD | Coverage | Local archive | Note |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-25 | OpenAI Astra | 7ebb761+ | facebook_a K=3500 | candidate | Astra S2, robust filter | 5 × 20 (boot 2000) | 0.084 ± 0.037 | 0.147 ± 0.030 | 80% | candidate_frozen_gate_results.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | facebook_a K=3500 | candidate | Astra S3, robust filter and mixed-moment check | 5 × 20 (boot 2000) | 0.078 ± 0.029 | 0.147 ± 0.030 | 80% | candidate_frozen_gate_results.json | 5/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | instagram_hm K=10000 | candidate | Astra S2, robust filter | 5 × 20 (boot 2000) | 0.282 ± 0.219 | 0.869 ± 0.554 | 80% | candidate_frozen_gate_results.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | instagram_hm K=10000 | candidate | Astra S3, robust filter and mixed-moment check | 5 × 20 (boot 2000) | 0.304 ± 0.230 | 0.869 ± 0.554 | 80% | candidate_frozen_gate_results.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_comments_ext K=12500 | candidate | Astra S2, robust filter | 5 × 20 (boot 2000) | 0.134 ± 0.085 | 0.158 ± 0.038 | 80% | candidate_frozen_gate_results.json | 3/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_comments_ext K=12500 | candidate | Astra S3, robust filter and mixed-moment check | 5 × 20 (boot 2000) | 0.118 ± 0.064 | 0.158 ± 0.038 | 100% | candidate_frozen_gate_results.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_submissions_long K=5000 | candidate | Astra S2, robust filter | 5 × 20 (boot 2000) | 0.263 ± 0.067 | 0.165 ± 0.044 | 60% | candidate_frozen_gate_results.json | 0/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_submissions_long K=5000 | candidate | Astra S3, robust filter and mixed-moment check | 5 × 20 (boot 2000) | 0.214 ± 0.078 | 0.165 ± 0.044 | 60% | candidate_frozen_gate_results.json | 2/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | facebook_a K=3500 | exploratory | S3, robust filter alone | 5 × 20 (boot 2000) | 0.091 ± 0.016 | 0.147 ± 0.030 | 80% | diag_ablation.json | 5/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | facebook_a K=3500 | exploratory | S3, mixed-moment check alone | 5 × 20 (boot 2000) | 0.103 ± 0.029 | 0.147 ± 0.030 | 60% | diag_ablation.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | instagram_hm K=10000 | exploratory | S3, robust filter alone | 5 × 20 (boot 2000) | 0.304 ± 0.230 | 0.869 ± 0.554 | 80% | diag_ablation.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | instagram_hm K=10000 | exploratory | S3, mixed-moment check alone | 5 × 20 (boot 2000) | 0.360 ± 0.276 | 0.869 ± 0.554 | 80% | diag_ablation.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_comments_ext K=12500 | exploratory | S3, robust filter alone | 5 × 20 (boot 2000) | 0.124 ± 0.057 | 0.158 ± 0.038 | 100% | diag_ablation.json | 4/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_comments_ext K=12500 | exploratory | S3, mixed-moment check alone | 5 × 20 (boot 2000) | 0.090 ± 0.040 | 0.158 ± 0.038 | 100% | diag_ablation.json | 5/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_submissions_long K=5000 | exploratory | S3, robust filter alone | 5 × 20 (boot 2000) | 0.198 ± 0.079 | 0.165 ± 0.044 | 100% | diag_ablation.json | 3/5 vs historical mobility |
| 2026-11-25 | OpenAI Astra | 7ebb761+ | reddit_submissions_long K=5000 | exploratory | S3, mixed-moment check alone | 5 × 20 (boot 2000) | 0.303 ± 0.055 | 0.165 ± 0.044 | 0% | diag_ablation.json | 0/5 vs historical mobility |
