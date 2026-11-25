# External Technical Review Brief — Common Rank-Diffusion Model (Stages 1–4)

**Strict rules for this review:**
1. **Read-only:** Do NOT modify, create, or delete any files in the repository. Return your complete review in your final response.
2. **Zero sealed data contact:** Do NOT read, inspect, or list any sealed or exposure-unknown datasets (`llm_fitting/SEALED_PATHS.json`: Reddit 2023–24 bridge on WD, Wikipedia on `/Volumes/T9/rank-diffusion-data/wikipedia`, or any YouTube files). Inspect only files inside `/Users/hindman/Documents/GitHub/rank-diffusion/`.

---

## Context & Files to Inspect

We have just completed a four-stage development pass on DEVELOPMENT data (`facebook_a` Era A, `instagram_hm` via the §2z-c censoring apparatus, `reddit_comments_ext`, and `reddit_submissions_long`) building a single, parsimonious, moment-identified common stochastic model targeting three goals simultaneously:
1. **Goal 1:** Reproduce aggregate rank-size structure and cross-platform dynamics (turnover, top-occupant churn, stable ladders, arithmetic head share without blowups, boundary crossing ratio, established-departure rate & crossing fraction, gapped persistence & asymmetry).
2. **Goal 2:** Accurately predict individual endpoint movement and audience-share shifts over any horizon ($h = 1, 4, 13, 26, 52$ weeks), strictly calendar-aligned, evaluated with proper rules (fair CRPS on relative log-level and coarsened log-rank, audience-share shift MAE/corr, top-$k$ retention Brier, 90% coverage, and Gate v1).
3. **Goal 3:** Maximum parsimony ("as parsimonious as possible, but not simpler"), with every parameter identified from a declared moment or independent instrument — zero tuning to scores.

Please read the actual code, tests, saved JSON outputs, and record entries before writing your assessment:
- Core model implementation: `llm_fitting/common_rankdiff.py`
- Unit & ADEMP recovery tests: `tests/test_common_rankdiff.py`
- Full Stage 1–4 report & cross-platform atlas: `llm_fitting/runs/2026-11-25_common_model/REPORT.md`
- Pre-run design & predictions: `llm_fitting/runs/2026-11-25_common_model/DESIGN.md`
- Benchmark runner & saved results: `llm_fitting/runs/2026-11-25_common_model/stage3_benchmark.py`, `stage3_benchmark_results.json`, `stage3_v1_uniform_surge_results.json`, `stage1_asymmetry_results.json`, `stage1_parsimony_predict_results.json`
- Canonical record & project state: `llm_fitting/MODEL_STATUS.md` (§2z-dw and §2z-dx), `llm_fitting/GATE_LEDGER.md` (tail), `llm_fitting/PROGRAM_STATE.md`, and `llm_fitting/TAKEOVER_ASSESSMENT_AND_PLAN_FABLE_2026-11-10.md` (for comparison to prior baseline state).

---

## Questions to Answer in Detail

Please be blunt, rigorous, and quantitative — cite exact line numbers, equations, and saved numbers wherever relevant.

### 1. Audit of Progress & Current Project State
- How strong is the Stage 2 (11-scalar) + Stage 3 (14–15-scalar) common model relative to the prior ~500-parameter `Model A` / `rankdiff_rdm` (`movinghome_vp`) line and relative to the three program goals?
- Audit the mathematical and statistical soundness of every estimator and simulator component in `llm_fitting/common_rankdiff.py`:
  - Split-half covariance estimator for $s_{\text{vol}}$ and empirical-Bayes $\hat{v}_i$ normalization
  - Structure-function profile NNLS, Spec-B lower bound, and variance-conserving Stage 3 subtraction ($D_{\text{sub}}$)
  - Fading positive surge identification from $\kappa_3(\Delta_h X)$ and the odd excess 4th moment $E[Z^3|Z|] - 4E[|Z|]E[Z^3]$, plus rank-scaling by $b_\xi$ and $5\mu_s$ capping
  - Persistent entity drift $\sigma_d$ identification from exact gapped covariance ($\text{hl}_d = 156$w fixed)
  - Smooth 7-knot all-rank drift refinement and zero-parameter starting-ladder deconvolution
  - Vectorized 2-state and 4-state Kalman filter $[L, \xi, d, s_{\text{obs}}]$ and `s >= 0` clamp compensation
  - Any subtle biases, leakage risks, misspecification, or evaluation harness discrepancies (e.g., exploratory Gate v1 harness vs. frozen `rankdiff_kalman.py` Gate v1, permanent-rank band leakage, etc.)
- Evaluate the scientific credibility of our **cross-platform atlas** finding (universal $\phi, s_{\text{vol}}, \text{hl}_s$, crossing dominance vs. broadcast/creator feed vs. community platform split in $\sigma_d$ and supply-vs-engagement-per-item). What could be an artifact of panel length ($T=52/86$ vs $212$) or censoring vs. genuine platform physics?

### 2. Explicit Diagnosis of Remaining Weaknesses & Open Gaps
Specifically diagnose the root causes of:
1. `reddit_submissions_long` arithmetic head share (`0.59` S2 / `0.68` S3 vs. empirical `0.36`) and exploratory Gate v1 (`0.43–0.53` vs. baseline `0.193`).
2. Residual Reddit decline-run ratios (`r2 = 1.06–1.09` vs. emp `1.24–1.28`) and volatility leverage (`0.89–0.90` vs. emp `1.11–1.13`), plus Submissions `D` (`+0.089` vs. emp `+0.222`).
3. Stage 3 regressions vs. Stage 2 where they occurred (e.g., Facebook `Turn top10` `0.50` vs `0.35` and `Boundary` `1.134` vs `0.997`; Submissions `share_corr` dropping `0.57 -> 0.36` at $h=26$; Comments `h=52` `logR CRPS` `0.1892 -> 0.1949`; Instagram 90% coverage `0.81`).
4. Missing evaluations that a tough PNAS referee would immediately demand (e.g., frozen `rankdiff_kalman.py` Gate v1 comparison, out-of-period 2019–20 $\to$ 2021–22 structure test, decile persistence curves, matched-length FB-vs-Reddit comparison at $T = 86$w, etc.).

### 3. Concrete, Prioritized Proposals for Additional Rounds of Experimentation
List concrete, prioritized changes, mechanism refinements, diagnostics, and experiments we should test next on DEVELOPMENT data before touching held-out data. For each proposal, state:
- **Priority (High / Medium / Low)** and expected impact on Goal 1, Goal 2, and Goal 3
- **Exact mathematical specification and non-circular identification moment** (zero score tuning!)
- **Pre-test falsification criterion** (what result would make us reject/abandon it)
- **What NOT to waste time on** (where we have already hit diminishing returns or where further work on DEVELOPMENT data risks overfitting).

### 4. Readiness Verdict for Held-Out Confirmation (Reddit 2023–24, YouTube, Wikipedia)
What is the minimum bar that must be cleared in our next round(s) before we freeze the model and write the pre-registered confirmation protocol?
