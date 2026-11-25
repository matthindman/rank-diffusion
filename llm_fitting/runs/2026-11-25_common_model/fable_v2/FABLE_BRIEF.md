# Research Task Brief for Fable 5.2 — Principled Model Improvement Pass (Round 5)

**Role:** Independent stochastic-modeling researcher.  
**Working directory (WRITABLE — write ONLY inside this folder):** `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/fable_v2/`  
**Repository root (READ-ONLY — do NOT modify any file outside `fable_v2/`):** `/Users/hindman/Documents/GitHub/rank-diffusion`  
**Anonymous DEVELOPMENT panel cache (READ-ONLY):** `/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache`  
**Python interpreter (REQUIRED — pandas 2.2.2, numpy 2.0.1, scipy 1.14.0):** `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`

---

## 1. Your Exact Objective & The Three Core Goals (Non-Negotiable)

Your task is to **improve the newly revised Common Rank-Diffusion stochastic model** (`common_rankdiff_fable.py`, initialized as an exact copy of our **Round 4** `llm_fitting/common_rankdiff.py`, which already incorporates Round 1–3 fixes plus Astra 6.2's adopted conditional-median endpoint-volatility filter `_volatility_posterior`) across all four DEVELOPMENT platforms (`facebook_a`, `instagram_hm` via the §2z-c censoring apparatus, `reddit_comments_ext`, `reddit_submissions_long`), while **strictly adhering to the three core goals**:

1. **Goal 1 — Aggregate Structure Across Platforms:** Reproduce stable rank-size structure, turnover across scales (`top10`, `K/4`), weekly rank-1 occupant change, boundary ratio (`~1.0`), canonical §2z-ac departure decomposition (immediate crossings vs. 13-week intermittent returns), block-by-block head-share stability, and (in `S3` / unified model) out-of-moment gapped persistence $\rho$, asymmetry $D$, and the non-monotonic 10-decile $E[y \mid x]$ curve — both in-sample and in the out-of-period 2019–20 → 2021–22 Reddit test.
2. **Goal 2 — Accurate Individual Endpoint Movement & Audience-Share Shifts Over Any Horizon (1w–52w):** Improve or preserve performance on:
   - **Exact Frozen Gate v1** (`rankdiff_kalman.py`'s exact scoring harness: `dRank1/4/13, RACF1, coll1/5/20`, 5 splits × 20 reps, boot 2000),
   - **Individual activity forecasts (`X MAE`, `X CRPS`)** beating `no_change` and train-tuned `local_level` EWMA,
   - **Exact distribution-free 90% interval calibration** (`[x_(2), x_(38)]` at `M = 39`, target `0.900`, PIT SD target `0.289`),
   - **Individual rank forecasts (`logR CRPS`) and fair head Brier** beating `no_change` and `rank_bin`,
   - **Head audience-share shifts (`shareHD_MAE`, `shareHD_corr`, top-$K/16$)** beating `zero_share_shift` and `local_level` EWMA.
3. **Goal 3 — Maximum Parsimony ("as simple as possible, but not simpler"):**
   - **Hard scalar budget:** `S2` (core, `asymmetry=False`) **$\le 12$ free scalars** (currently 11); `S3` (asymmetry comparator, `asymmetry=True`) **$\le 15$ free scalars** (currently 14–15). Fewer scalars is strictly better; unifying or simplifying without losing Goals 1 & 2 is a major win!
   - **100% moment-identified or independent-instrument identified:** Every parameter must come from a declared closed-form or low-dimensional moment (or the independent Spec-B instrument). **ZERO tuning to Gate v1, CRPS, MAE, coverage, head share, or any evaluation metric.**
   - **Zero platform-specific logic:** `if panel == ...` or hardcoded platform thresholds are strictly forbidden. The exact same estimation and simulation code must run unchanged on all four platforms and generalize cleanly to unseen future platforms (YouTube, Wikipedia, Reddit 2023–24).

### Hard Safety & Integrity Rules
- **ZERO sealed or exposure-unknown data contact:** Use ONLY the four `.npz` + `facebook_a_spec_b.json` files inside `/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache`. NEVER read `/Volumes/T9`, the WD drive, Wikipedia, YouTube, or Reddit 2023–24.
- **Write ONLY inside `fable_v2/`.** Do NOT edit `llm_fitting/common_rankdiff.py`, `minimal_rankdiff.py`, `rankdiff_kalman.py`, `rankdiff_rdm.py`, `MODEL_STATUS.md`, or any ledger.
- **Honesty over wins:** Report every failed hypothesis, every regression, and every trade-off openly. If you conclude after rigorous diagnostics that a part of the model has reached its genuine parsimony frontier, state that clearly!

---

## 2. Exact Current Baseline (Round 4 = Round 3 + Adopted Astra Volatility Filter)

Before starting, read:
- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/REPORT.md` (§§7–11)
- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/astra_v2/ASTRA_REPORT.md` (§§1–4 & Attribution table at end of §5)
- `fable_v2/common_rankdiff_fable.py` (our newly adopted Round 4 baseline!)

**What Astra 6.2 just established (do not waste time re-proving):**
1. **Adopted (`_volatility_posterior`, 0 new scalars):** Replacing raw `mean(dX^2)` EB volatility with a conditional-median `median(|dX_1|)` posterior inverted through the model's exact 1-week increment law (preserving the non-scaling `sigma_L^2` floor) improved Gate v1 across 3/4 platforms while leaving Goal 1 structure 100% bit-identical:
   - `facebook_a`: `S2 0.105 → 0.084 ± 0.037` (80% cov, 4/5 wins), `S3 0.108 → 0.091 ± 0.016` (80% cov, 5/5 wins) vs. persistence `0.147` (frozen Model A `0.103`).
   - `instagram_hm`: `S2 0.296 → 0.282 ± 0.219` (80%, 4/5), `S3 0.360 → 0.304 ± 0.230` (80%, 4/5) vs. persistence `0.869` (Model A `0.257`).
   - `reddit_comments_ext`: `S2 0.129 → 0.134 ± 0.085` (80%, 3/5), `S3 0.098 → 0.124 ± 0.057` (100%, 4/5) vs. persistence `0.158` (Model A `0.123`, RDM′ `0.130`).
   - `reddit_submissions_long`: `S2 0.346 → 0.263 ± 0.067` (60% cov, 5/5 vs R3), **`S3 0.310 → 0.198 ± 0.079` (100% cov, 3/5 wins vs persistence `0.165`!)** — nearly matching 50+ parameter Model A (`0.188`) and RDM′ (`0.183`) with 15 scalars!
   - Also improved `S2` `X CRPS` in 13/17 cells, `PIT SD` in 17/17 cells, `Submissions cov90` from `0.944–0.952 → 0.924–0.935`, and head share-shift MAE wins (vs. both zero-shift & EWMA) from 11/17 to 14/17.
2. **Tested and rejected by Astra:**
   - Opposite-half lasting-variance stratification confirmed `gamma_L = 0` (volatile endpoints have 2.85–3.85× weekly variance but only 0.58–1.20× fitted lasting variance; isolated `fit_gamma_L=True` hurt `X CRPS` at every horizon on Reddit).
   - Suppressing surges via the short-lag adjacent-window `E[x^2 y]` sign check fixed FB/Comments top-10 turnover (`0.55→0.35`, `0.45→0.25`), **but destroyed Reddit's out-of-moment asymmetry `D` (`+0.149 → -0.050`), 10-decile hook, and `r = +0.807 → +0.461` 52w Submissions head share-shift forecast.** Rejected!

### High-Value Open Scientific Opportunities for Fable 5.2
1. **The Big Prize — Fixing `S3`'s Head/Boundary Overshoot WITHOUT Losing Reddit's Asymmetry $D$ & 10-Decile Hook:**
   - Why does Round 4 `S3` overshoot top-10 turnover (`FB 0.55 vs 0.35`, `Comments 0.45 vs 0.27`), `Max X` (`FB 19.37 vs 16.85`), `Boundary` (`1.13–1.29`), and `m21` (`-0.32 to -0.35` vs `-0.10 to -0.16`), and get wrong-sign $D$ on Instagram (`+0.082` vs `-0.087`)?
   - Astra showed why: `b_xi` gives rank-1 entities `0.50–0.69×` of the rank-$K$ surge amplitude, so a single capped arrival (`5 * mu`) adds **3.1–4.7 log units (a 20×–107× jump!)** at rank 1, while `C5/C3` pooled across heterogeneous entities is inflated by `10 * Cov(var, k3)`.
   - **First-principles question for Fable 5.2:** Can a mathematically cleaner surge/skew mechanism or moment estimator (e.g., within-entity or standardized cumulant estimation free of pooling inflation; a lighter/bounded jump law; proper head attenuation or rank-scaling; or a cleaner skewed passing/surge process — with **zero** score tuning and $\le 15$ scalars!) preserve Reddit's out-of-moment $D$, 10-decile hook, `0.198` Submissions Gate v1, and `r ≈ +0.81` 52w share shifts **while** curing FB/Comments top-10 turnover, `Max X`, boundary overshoot, and Instagram?!
2. **Closing the Last Gap on `S2` Submissions (`Gate 0.263`, `cov90 = 0.924–0.935`, `dRank13 = 10.6 vs 7.4`) & Recovering Instagram's Tiny `X CRPS` Cost (`+0.0014–0.0020`):**
   - Notice TWO clean structural places to inspect:
     (a) **Efficiency of `_volatility_posterior` on short intermittent records (Instagram):** Astra used raw sample `median(|dX_1|)`, which has asymptotic relative efficiency `2/π = 0.637` on clean data and cost `+0.0014–0.0020` `X CRPS` on Instagram's short intermittent 52w histories. Does a smooth, contamination-resistant M-estimator / Huber / soft-clipped log-scale or trimmed-RMS moment (matched identically to the model's 1-week increment law!) keep ALL of Astra's Submissions gains while eliminating the Instagram efficiency cost (and maybe improving `Comments` Gate v1 back toward `0.098–0.115`)?
     (b) **Why does `S2` on Submissions still have `dRank13 = 10.6 vs 7.4` and `cov90 = 0.924–0.935`?** Look closely at `innov_pool` (pooled standardized 1-week `dX1` reused identically for lasting `sigma_L`, passing `xi`, and observation noise `eps`!) vs. actual multi-week lasting increment shape, OR look at how `sigma_L` is estimated from `D_b(h)` when 1% extreme subreddit jumps inflate the raw mean-square variogram by 2× on Submissions! Remember: any fix must be 100% moment-identified and preserve/improve ALL 4 platforms!
3. **Any Deeper Unification or Simplification You Discover:**
   - You have full intellectual freedom to inspect the data (in the DEVELOPMENT cache) and `common_rankdiff_fable.py` and propose your own cleaner first-principles reformulation, provided it strictly obeys Goals 1, 2, and 3!

---

## 3. Exact Workflow & Deliverables (Follow Every Step)

1. **Step 1 — Diagnose First (Before Editing Code):**
   - Run targeted diagnostic scripts in `fable_v2/diag_*.py` (logging outputs in `fable_v2/`) to test your hypotheses BEFORE editing `common_rankdiff_fable.py`.
   - Maintain `fable_v2/EXPERIMENT_LOG.md` documenting every hypothesis, diagnostic result, ablation, and failed attempt.
2. **Step 2 — Implement Clean Candidate in `fable_v2/common_rankdiff_fable.py`:**
   - Keep the exact public interface compatible with `benchmark_candidate.py`, `round3_frozen_gate.py`, and `round3_gold_benchmark.py` (`CommonParams`, `CommonState`, `estimate_common`, `filter_endpoints`, `forecast_endpoints`, `simulate_common_panel`, `deconvolve_starting_ladder`, `_refine_drift_smooth`, `_surge_mean`, `_surge_variance`, `p_abs_of`, `n_free_scalars()`).
   - Add synthetic recovery & invariant unit tests to `fable_v2/test_common_rankdiff_fable.py` and verify 100% pass:
     `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m pytest test_common_rankdiff_fable.py -v -p no:cacheprovider`
3. **Step 3 — Mandatory Full Apples-to-Apples Benchmark:**
   - Before concluding, you **MUST** run the **FULL** apples-to-apples benchmark on ALL FOUR panels (`--mode both`, 5×20 Gate v1 + 20-seed Goal 1 + `M=39` Goal 2 + OOP):
     ```bash
     /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u ../benchmark_candidate.py \
       --module common_rankdiff_fable.py \
       --out-dir . \
       --mode both
     ```
     Compare clearly against **BOTH** Round 3 (`../round3_frozen_gate_results.json`, `../round3_gold_benchmark_results.json`) **AND** Round 4 / Astra-adopted (`../round4_baseline/candidate_frozen_gate_results.json` and `../astra_v2/ASTRA_REPORT.md` Filter-Only numbers). If you make two distinct changes (e.g. one to S2/filter and one to S3), also check their separate attribution!
4. **Step 4 — Write `fable_v2/FABLE_REPORT.md` (Required Final Deliverable):**
   Structure it clearly with:
   - **1. Executive Verdict & Adoption Recommendation** (accept, partial accept, or keep Round 4?)
   - **2. Mathematical Thinking & Root-Cause Analysis** (explain clearly in plain, incisive prose *why* each bottleneck happens, *why* your fix works from first principles, and *how* every parameter is moment-identified)
   - **3. Exact Parameter Count & Complexity Accounting** (`S2` and `S3` scalar counts before vs. after)
   - **4. What You Tried and Rejected** (every failed hypothesis with exact numbers)
   - **5. Full Before-vs-After Results Across All 4 Panels** (vs. Round 3 AND Round 4 Astra-adopted: Exact Frozen Gate v1, Goal 1 structure + canonical departures + asymmetry + 10-decile curves, Goal 2 `M=39` MAE/CRPS/cov90/PIT/logR/head share shifts/fair Brier, OOP 2019–20→2021–22)
   - **6. Verification & Honest Remaining Limitations** (pytest results, any regressions or trade-offs).

Think deeply, keep it parsimonious and mathematically principled, log everything cleanly, and deliver your best scientific work.
