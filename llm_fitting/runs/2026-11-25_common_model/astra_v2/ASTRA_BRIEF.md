# Research Task Brief for Astra 6.2 — Principled Model Improvement Pass

**Role:** Independent stochastic-modeling researcher.  
**Working directory (WRITABLE):** `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/astra_v2/`  
**Repository root (READ-ONLY — do NOT modify any file outside `astra_v2/`):** `/Users/hindman/Documents/GitHub/rank-diffusion`  
**Anonymous DEVELOPMENT panel cache (READ-ONLY):** `/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache`  
**Python interpreter (REQUIRED — pandas 2.2.2, numpy 2.0.1, scipy 1.14.0):** `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`

---

## 1. Your Exact Objective & The Three Core Goals (Non-Negotiable)

Your task is to **improve the Common Rank-Diffusion stochastic model** (`common_rankdiff_astra.py`, initialized as an exact copy of the Round 3 baseline `llm_fitting/common_rankdiff.py`) across all four DEVELOPMENT platforms (`facebook_a`, `instagram_hm` via the §2z-c censoring apparatus, `reddit_comments_ext`, `reddit_submissions_long`), while **strictly adhering to the three core goals**:

1. **Goal 1 — Aggregate Structure Across Platforms:** Reproduce stable rank-size structure, turnover across scales (`top10`, `K/4`), weekly rank-1 occupant change, boundary ratio (`~1.0`), canonical §2z-ac departure decomposition (immediate crossings vs. 13-week intermittent returns), block-by-block head-share stability, and (in the asymmetry specification) out-of-moment gapped persistence $\rho$, asymmetry $D$, and the 10-decile $E[y \mid x]$ curve — both in-sample and in the true out-of-period 2019–20 → 2021–22 Reddit test.
2. **Goal 2 — Accurate Individual Endpoint Movement & Audience-Share Shifts Over Any Horizon (1w–52w):** Improve or preserve performance on:
   - **Exact Frozen Gate v1** (`rankdiff_kalman.py`'s exact scoring harness: `dRank1/4/13, RACF1, coll1/5/20`, 5 splits × 20 reps, boot 2000),
   - **Individual activity forecasts (`X MAE`, `X CRPS`)** beating `no_change` and train-tuned `local_level` EWMA,
   - **Exact distribution-free 90% interval calibration** (`[x_(2), x_(38)]` at `M = 39`, target `0.900`, PIT SD target `0.289`),
   - **Individual rank forecasts (`logR CRPS`) and fair head Brier** beating `no_change` and `rank_bin`,
   - **Head audience-share shifts (`shareHD_MAE`, `shareHD_corr`, top-$K/16$)** beating `zero_share_shift` and `local_level` EWMA.
3. **Goal 3 — Maximum Parsimony ("as simple as possible, but not simpler"):**
   - **Hard scalar budget:** `S2` (core, `asymmetry=False`) **$\le 12$ free scalars** (currently 11); `S3` (asymmetry comparator, `asymmetry=True`) **$\le 15$ free scalars** (currently 14–15). Fewer scalars is strictly better; if a single unified specification beats both S2 and S3 within $\le 15$ scalars without hurting FB or IG, that is even better!
   - **100% moment-identified or independent-instrument identified:** Every parameter must come from a declared closed-form or low-dimensional moment (or the independent Spec-B instrument). **ZERO tuning to Gate v1, CRPS, MAE, coverage, head share, or any evaluation metric.**
   - **Zero platform-specific logic:** `if panel == ...` or hardcoded platform thresholds are strictly forbidden. The exact same estimation and simulation code must run unchanged on all four platforms and generalize cleanly to unseen future platforms (YouTube, Wikipedia, Reddit 2023–24).

### Hard Safety & Integrity Rules
- **ZERO sealed or exposure-unknown data contact:** Use ONLY the four `.npz` + `facebook_a_spec_b.json` files inside `/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache`. NEVER read `/Volumes/T9`, the WD drive, Wikipedia, YouTube, or Reddit 2023–24.
- **Do NOT edit any file outside `astra_v2/`.** Do not touch `llm_fitting/common_rankdiff.py`, `minimal_rankdiff.py`, `rankdiff_kalman.py`, `rankdiff_rdm.py`, `MODEL_STATUS.md`, or any ledger.
- **Honesty over wins:** If a clean idea fails or causes regressions on another platform, report the failure openly and do NOT hide it or curve-fit around it. If you conclude after rigorous diagnostics that part of the model is already at its parsimony frontier, say so explicitly!

---

## 2. Exact Round 3 Baseline Scorecard (What You Must Beat or Match)

Read `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/REPORT.md` (especially §§7–11), `llm_fitting/common_rankdiff.py`, and your own prior Round 1 review (`REVIEW_R1_astra-6.2.md`) to see what we already fixed since Round 1 (exact capped-exponential moments, boundary-free $\kappa_5/\kappa_3$ surges, `hl_d = 52w`, symmetrized `innov_pool`, exact Spec-B lower-bound law, exact `P12` and `P[0,3]`, fixed `s >= 0` clamp, within-band Bessel variogram demeaning, stratum-demeaned drift covariance, `med_K` centering, `M = 39` exact coverage).

**Round 3 Exact Frozen Gate v1 (`round3_frozen_gate_results.json`, 5 × 20, boot 2000):**
- `facebook_a`: `S2 = 0.105 ± 0.043` (60% cov, 4/5 wins), `S3 = 0.108 ± 0.023` (60%, 4/5) vs. persistence `0.147` (frozen Model A `0.103`, RDM′ `0.123`).
- `instagram_hm`: `S2 = 0.296 ± 0.216` (80% cov, 4/5 wins), `S3 = 0.360 ± 0.276` (80%, 4/5) vs. persistence `0.869` (Model A `0.257`).
- `reddit_comments_ext`: `S2 = 0.129 ± 0.071` (80%, 3/5), **`S3 = 0.098 ± 0.033`** (100% cov, 5/5 wins) vs. persistence `0.158` (Model A `0.123`, RDM′ `0.130`).
- `reddit_submissions_long`: **`S2 = 0.346 ± 0.064` (0% cov, 0/5), `S3 = 0.310 ± 0.125` (20%, 0/5)** vs. persistence `0.165` (Model A `0.188`, RDM′ `0.183`).

**Round 3 Repaired Gold Benchmark (`round3_gold_benchmark_results.json`, 20 seeds / `M = 39`):**
- **Open Bottleneck #1 — `reddit_submissions_long` Gate v1 (`0.346 / 0.310`) AND 90% coverage over-dispersion (`0.944–0.952`, `pit_sd = 0.232–0.244`):**
  - Empirical top-100 on Submissions has extreme 1% outlier variance (`raw/iqr = 2.0–4.2×`, `kurt = 13–40`) and strong long-horizon volatility persistence (`corr(log v1_e, log v13_e) = 0.71`, `s_long = 0.71` vs. `s_short = 0.72`).
  - Because `gamma_L = 0` gives every entity homogeneous lasting variance matched to raw `D(h)`, typical quiet top-100 subreddits get `+1–4` ranks too much *median* displacement (`dRank1 5 vs 4, dRank4 7 vs 5, dRank13 10–12 vs 8`) and ~5% too-wide 90% intervals.
  - *Why naive `fit_gamma_L=True` failed earlier:* in Stage 2, naive `gamma_L` worsened `X CRPS` at long horizons. Diagnose mathematically WHY (e.g., `E[v_i^gamma]` normalization? `v_hat` noise vs. true persistent lasting scale? heavy-tailed vs. Gaussian lasting step? robust vs. raw variogram decomposition? `s_long` shrinkage / EB filtering?) and check whether a principled fix improves **BOTH** Gate v1 / coverage on Submissions **AND** CRPS across all 4 platforms!
- **Open Bottleneck #2 — `Common-S3` Head/Boundary Overshoot & Instagram Wrong-Sign $D$:**
  - `S3` currently overshoots top-10 turnover (`FB 0.55 vs 0.35`, `Comments 0.45 vs 0.27`), `Max X` (`FB 19.37 vs 16.85`), `Boundary` (`1.13–1.29 vs 0.95–1.01`), `m21` (`-0.32 to -0.35` vs. `-0.10 to -0.16`), and hurts Instagram (`Gate 0.360 vs 0.296`, `D = +0.082` vs. emp `-0.087 [-0.122, -0.050]`).
  - Investigate mathematically from first principles: (a) why `S3` picks `hl_s = 52w` and giant `mu = 1.23–1.44` (`5*mu ~ 7` log units!) with almost no attenuation at rank 1 (`b_xi ~ 0.06`), (b) why `instagram_hm` has positive marginal $\kappa_3$ yet negative gapped $D = -0.087$ (is `instagram_hm` right-skewed at 1-week measurement/censoring scale rather than a 39-week persistent surge?!), and (c) whether a cleaner surge/tail identification or amplitude scaling eliminates the head/boundary/IG regressions while keeping or improving Reddit's `0.098` Comments Gate v1 and out-of-moment 10-decile hook!
- **Open Bottleneck #3 — Ensemble Median vs. Analytical Kalman Mean MAE Gap & Filter/Forecast Calibration:**
  - In Round 3 Goal 2, `S2_ens` (ensemble median) is consistently `0.008–0.025` worse in `X MAE` than `S2_kal` (analytical Kalman mean `L_hat + phi^h * xi_hat - med_K[T0-1]`) across all 4 platforms (`FB h=1: 0.4116 vs 0.4027`, `IG h=1: 0.6618 vs 0.6370`, `Submissions h=1: 0.172 vs 0.166`). Diagnose why! Is it a `med_m` shift, a posterior-draw bias, `drift_g` centering, or `v_hat` dispersion? Fixing any genuine forecast/filter mismatch improves `X CRPS`, `logR CRPS`, and `shareHD_MAE` simultaneously with zero extra parameters!
- **Open Bottleneck #4 — Any Deeper First-Principles Improvement You Spot:**
  - You have full intellectual freedom to inspect the data (in the DEVELOPMENT cache) and `common_rankdiff_astra.py` and propose any cleaner mathematical reformulation that strictly obeys all 3 goals!

---

## 3. Exact Workflow & Deliverables (Follow Every Step)

1. **Step 1 — Diagnose First (Before Editing Code):**
   - Run targeted diagnostic scripts saved in `astra_v2/diag_*.py` (outputs logged in `astra_v2/`) to test your hypotheses on Bottlenecks #1–#4.
   - Maintain a running log `astra_v2/EXPERIMENT_LOG.md` documenting **every** hypothesis, diagnostic number, ablation, and failed attempt.
2. **Step 2 — Implement Clean Candidate in `astra_v2/common_rankdiff_astra.py`:**
   - Keep the public interface compatible with `benchmark_candidate.py`, `round3_frozen_gate.py`, and `round3_gold_benchmark.py` (`CommonParams`, `CommonState`, `estimate_common`, `filter_endpoints`, `forecast_endpoints`, `simulate_common_panel`, `deconvolve_starting_ladder`, `_refine_drift_smooth`, `_surge_mean`, `_surge_variance`, `p_abs_of`, `n_free_scalars()`).
   - Note: `round3_frozen_gate.py` and `round3_gold_benchmark.py` draw initial states from `filt` (`L_hat, xi_hat, P11, P12, P22, v_hat, post_s_v`, plus `P4, d_hat, s_hat, s_scale` when `"P4" in filt`) and instantiate `cr.CommonState(p, L_i, rng, xi0=xi_i, v0=v_i, d0=d_i, s0=s_i)`. Keep your changes cleanly compatible with that contract so the frozen evaluator runs apples-to-apples without modifying the evaluator!
   - Update `astra_v2/test_common_rankdiff_astra.py` with synthetic recovery and mathematical-invariant tests for every change you make, and run:
     `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m pytest test_common_rankdiff_astra.py -v`
3. **Step 3 — Fast Screen, Then Mandatory Full Apples-to-Apples Benchmark:**
   - During exploration, you may screen quickly with `--mode gate` (~3–4 min) or `--panels reddit_submissions_long,facebook_a`.
   - Before concluding, you **MUST** run the **FULL** apples-to-apples benchmark on ALL FOUR panels (`--mode both`, 5×20 Gate v1 + 20-seed Goal 1 + `M=39` Goal 2 + OOP 2019–20→2021–22):
     ```bash
     /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u ../benchmark_candidate.py \
       --module common_rankdiff_astra.py \
       --out-dir . \
       --mode both
     ```
     This produces `candidate_frozen_gate_results.json`, `candidate_gold_benchmark_results.json`, and prints exact side-by-side diffs vs. Round 3 baseline.
4. **Step 4 — Write `astra_v2/ASTRA_REPORT.md` (Required Final Deliverable):**
   Structure it clearly with:
   - **1. Executive Verdict & Adoption Recommendation** (crisp, honest: accept, partial accept, or keep Round 3?)
   - **2. Mathematical Thinking & Root-Cause Analysis** (explain *why* each bottleneck happens, *why* your solution works from first principles, and *how* every parameter is moment-identified — zero tuning to scores)
   - **3. Exact Parameter Count & Complexity Accounting** (`S2` and `S3` scalar counts before vs. after)
   - **4. What We Tried and Rejected** (every failed hypothesis with exact numbers)
   - **5. Full Before-vs-After Results Across All 4 Panels** (Exact Frozen Gate v1, Goal 1 structure + canonical departures + asymmetry, Goal 2 `M=39` MAE/CRPS/cov90/PIT/logR/head share shifts/fair Brier, OOP 2019–20→2021–22)
   - **6. Verification & Honest Remaining Limitations** (pytest results, any regressions or trade-offs).

Take your time, think deeply from first principles, log everything cleanly, and deliver your best scientific work.
