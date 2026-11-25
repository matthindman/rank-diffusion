# 2026-11-25 — Common-model endpoint volatility and surge direction, exploratory DEVELOPMENT, no default change adopted

OpenAI Astra, based on 7ebb7613f0f1e3fd6599fdacb8ccae3ee1bed5de and the Round 3 working copy in MODEL_STATUS §2z-dy. The owner authorized a local candidate only. [Research report](ASTRA_REPORT.md) · [Design, commands and input hashes](EXPERIMENT_LOG.md)

**Recommendation.** Keep Round 3 as the cross-platform reference. Retain the median-absolute-increment endpoint filter as a development option. The bundled S3 loses the Reddit decile hook and forward Submissions D. No general replacement meets all three goals.

The robust filter identifies passing variance from the conditional median absolute weekly change under the existing simulator and prior. The separate S3 check tests the negative mixed third moment required by an isolated fading positive wave on disjoint increments. Both changes use the same platform-independent code. The full-window structural counts are 11 on all four S2 fits and 11, 11, 12, 12 on S3, down from 14, 14, 15, 15. A training-origin S3 can still use 15.

| Exact Gate v1, five origins × 20, boot 2000 | Historical mobility | Astra S2 ± split SD | Astra S3 ± split SD |
|---|---:|---:|---:|
| Facebook | 0.147 | 0.084 ± 0.037 | 0.078 ± 0.029 |
| Instagram apparatus | 0.869 | 0.282 ± 0.219 | 0.304 ± 0.230 |
| Reddit Comments | 0.158 | 0.134 ± 0.085 | 0.118 ± 0.064 |
| Reddit Submissions | 0.165 | 0.263 ± 0.067 | 0.214 ± 0.078 |

On Submissions S2 improved against Round 3 on all five splits, lowered activity and log-rank CRPS at all five horizons, and brought exact coverage from .944–.952 to .924–.935. Applying only the filter to Round 3 S3 scored .198 ± .079 on the Submissions gate and .124 ± .057 on Comments, versus the original .310 and .098. Applying only the sign check scored .303 ± .055 and .090 ± .040, respectively. All 16 gate rows, including both off-diagonal comparisons, are in [the local ledger handoff](GATE_ROWS.md).

S2 in-sample structure and seed bands match Round 3 exactly. S3 reduced Facebook and Comments top-10 turnover overshoot but reversed the sign of in-sample Reddit D. On IG, negative empirical D persists among 3,902 continuously observed endpoints (−.071 [−.126, −.012]). IG S2 activity CRPS worsened at each horizon. Across all 17 cells, S2 improved on Round 3 activity CRPS in 13 and beat both share-shift MAE baselines in 14. The Submissions 52-week S3 head-shift correlation fell from .807 to .461.

The supplied forward evaluator's 104-week prefixes end in November 2020. The unchanged benchmark and a separate January 2019–December 2020 fit with January 2021–December 2022 test are both archived. On matched 51-week calendar blocks, the observed 2022 Submissions head share was .357 against .462 [.416, .518] for Round 3 S3 and .528 [.457, .653] for the candidate S3. Candidate calendar-forward Submissions D was −.018 [−.046, .020] against +.146 observed.

**Verification.** The original local suite passed 14/14. The candidate passed 16/16 including new synthetic recovery and invariance tests. Gaussian oracle coverage .89855, PIT SD .28951. The archive checker verified common cohorts, empirical moments, deterministic baselines, all five origins, and model scalar counts. The repository-wide health script requires an additional panel outside the owner’s allowance and was not run. No commit or push.

**Exposure handoff.** Read the four specifically authorized anonymous DEVELOPMENT NPZ matrices (FB Era A, IG §2z-c apparatus, Reddit Comments extension and 2018–22 long Submissions) plus the FB Spec-B JSON. Ran training moments, five-origin forecast and gate comparisons, and forward Reddit period tests. No contact with sealed or exposure-unknown data. Full-era cache buffer membership and the FB instrument preclude a strictly train-rebuilt-universe claim.
