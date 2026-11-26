# Round 8 Model-Improvement Brief — Claude Opus 5.5

You are running **Round 8** — the final development squeeze before moving to pre-registered held-out evaluation — of the `rank-diffusion` Common Meta-Framework program.
Read this entire brief carefully before touching code or running any script.

---

## 1. Owner's Direction & Core Goals

The owner asks for **one more deep squeeze** from Claude Opus 5.5 against our newly adopted Round 6 model (which Astra 6.2 tested and recommended keeping in Round 7):

> *"In thinking about a common model, I really want a common meta-framework expressed as a core equation / set of equations that we use for all platforms. If there are one or two specific components in the meta-model that significantly help the fit for only one or two platforms and are otherwise zeroed out, that is not the end of the world. You can add parameters or complications iff you judge the better fit worth the price in parsimony."*

Your mission: **improve the Round 6 Common Meta-Framework (`common_rankdiff_opus_r8.py`, copied byte-identical from `llm_fitting/common_rankdiff.py`) across all three core goals — or honestly establish if a candidate fails to beat Round 6:**

1. **Goal 1 — Aggregate structure** across Facebook Era A (`K=3500`), Instagram §2z-c censoring apparatus (`K=10000`), Reddit Comments (`K=12500`), and Reddit Submissions (`K=5000`): stable rank-size ladder, head share, top-10 and `K/4` turnover, rank-1 churn, boundary crossing ratio (`~1.00`), canonical immediate-crossing departure composition (§2z-ac), gapped persistence `rho`, asymmetry `D`, 10-decile non-monotonic hook curves, and 2019–20 → 2021–22 out-of-period structure on Reddit.
2. **Goal 2 — Individual endpoint movement & audience-share shifts over any horizon** (`h = 1, 4, 13, 26, 52w`):
   - **Exact Frozen Rolling-Origin Movement Gate v1** (`5 × 20, boot 2000`, `round3_frozen_gate.py`) — **THE ADOPTION GATE.**
   - Exact `M = 39` endpoint forecasts (`round3_gold_benchmark.py`): `X CRPS`, analytical Kalman MAE vs. EWMA, exact `[x_(2), x_(38)]` 90% coverage (`0.900`) & PIT SD (`0.289`), `logR CRPS`, fair head Brier, and top-`K/16` head audience-share shift MAE & correlation.
3. **Goal 3 — Unified Common Meta-Framework & Parsimony:**
   - ONE clean set of equations across all platforms (zero platform-name conditionals, zero hardcoded platform switches).
   - Every parameter MUST come from a **declared empirical moment or independent instrument** — ZERO tuning to Gate v1, CRPS, coverage, `D`, or any evaluation score!
   - One or two platform-activating components that zero out cleanly by moment identification on platforms that do not need them are explicitly allowed if their fit gain clearly justifies the parsimony price.
   - Always validate recovery on planted synthetic panels (`test_common_rankdiff_opus_r8.py`).

---

## 2. Hard Safety & Workspace Rules (MANDATORY)

- **ZERO contact with SEALED or EXPOSURE-UNKNOWN data.** Do NOT open, inspect, or reference the Reddit 2023–24 bridge (WD drive), Wikipedia (`/Volumes/T9/rank-diffusion-data/wikipedia`), or YouTube files. Use ONLY the four anonymous DEVELOPMENT `.npz` caches + `facebook_a_spec_b.json` in `/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache`.
- **Confine ALL edits and output files strictly to your isolated workspace:**
  `llm_fitting/runs/2026-11-25_common_model/opus_r8/`
  Do NOT modify `llm_fitting/common_rankdiff.py`, `tests/test_common_rankdiff.py`, `MODEL_STATUS.md`, ledgers, or any file outside `opus_r8/`.
- **Registered interpreter:** `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3` (pandas 2.2.2, numpy 2.0.1, scipy 1.14.0).
- **Important test hygiene:** `test_common_rankdiff_opus_r8.py` was copied from `tests/test_common_rankdiff.py`; make sure it imports `common_rankdiff_opus_r8` (not the repo's `common_rankdiff`) before running `pytest`!

---

## 3. Current Champion to Beat — Round 6 (`llm_fitting/common_rankdiff.py`)

In `common_rankdiff_opus_r8.py`:
- **Equations:**
  $$X_{i,t} = L_{i,t} + \xi_{i,t} + s_{i,t} + \varepsilon_{i,t}$$
  $$L_{i,t+1} = L_{i,t} + g(z_{i,t}) + \sigma_L(L_{i,t})\,\eta^L_{i,t} + B^J_{i,t} J_{i,t} + d_{i,t}$$
- **`S2` (`asymmetry=False`, default `scale_moment="meta"`, 12 free scalars when lasting jump gap share > 20%, 11 otherwise):**
  Preserves exact mean-square `(phi, V_xi, sigma_obs)`, splits lasting variance into the `h <= 13` robust IQR diffusive core + rare permanent **Gaussian** jumps (`JUMP_LAM_FIXED = 0.02/wk`, `jump_c` closed-form from lasting gap, rank law inherited from `b_xi` and capped at `dz <= 0`). Uses Astra's Round 4 conditional-median `_volatility_posterior` (0 new scalars).
- **`S3` (`asymmetry=True`, 14–15 free scalars):**
  Uses the exact Round 4 variance path (`jump_lam = 0`) + variance-conserving `kappa_5/kappa_3` capped-exponential fading positive surges $s_{i,t}$ + 52w persistent entity drift $d_{i,t}$ ($\hat{\sigma}_d = 0$ automatically on FB/IG, active on Reddit).

### Exact Frozen Gate v1 Scoreboard (`5 × 20, boot 2000`; lower is better)

Baselines staged in `opus_r8/baseline_r6/`:
- `r6_s2_frozen_gate.json` = exact Round 6 `S2` (12 sc)
- `r6_s3_frozen_gate.json` = exact Round 6 source `S3` (14–15 sc; bit-identical to Round 4 adopted `S3`)
- `r6_s2_gold.json` = exact Round 6 `S2` Gold Benchmark; Round 6 source `S3` Gold is in `../astra_v3/candidate_gold_benchmark_results.json` (since Astra R7's S3 switched jumps off on all 4 full panels and all 20 gate origins, reproducing Round 6 source S3 identically!) and `../astra_v2/diag_ablation.json`.

| Platform | Persistence | Frozen Model A | Round 4 (`S2` / `S3`) | Round 5 Fable (`S2+J`) | **Round 6 Champion (`S2` 12sc / `S3` 14–15sc)** | Round 7 Astra (`S2`, rejected) |
|---|---:|---:|---:|---:|---:|---:|
| `facebook_a` | `0.147` | `0.103` | `0.084` / `0.091` | `0.093` | **`0.089 ± 0.037` (80%, 3/5)** / **`0.091 ± 0.016` (80%, 5/5)** | `0.083` |
| `instagram_hm` | `0.869` | — | `0.282` / `0.304` | `0.344` | **`0.306 ± 0.232` (80%, 4/5)** / **`0.304 ± 0.230` (80%, 4/5)** | `0.282` |
| `reddit_comments_ext` | `0.158` | `0.123` | `0.134` / `0.124` | `0.096` | **`0.113 ± 0.035` (100%, 5/5)** / **`0.124 ± 0.057` (100%, 4/5)** | `0.118` |
| `reddit_submissions_long` | `0.165` | `0.188` | `0.263` / `0.198` | `0.196` | **`0.178 ± 0.066` (80%, 2/5)** / **`0.198 ± 0.079` (100%, 3/5)** | `0.209` |

**Round 6 `S2` Goal 1 & Goal 2 highlights:**
- Boundary ratios: `FB 1.015 (0.980)`, `IG 0.993 (1.000)`, `Comments 1.013 (1.009)`, `Submissions 0.996 (1.116)`.
- Exact `cov90` (`M = 39`): `FB 0.882–0.891`, `IG 0.887–0.891`, `Comments 0.900–0.914`, `Submissions 0.923–0.935`.
- Sweeps `X CRPS` and `logR CRPS` vs Round 4 across all 5 horizons on both Reddit ladders.

---

## 4. Dead Ends Already Tested & Rejected (DO NOT Repeat!)

You may read `../astra_v3/ASTRA_R7_REPORT.md`, `../fable_v2/FABLE_REPORT.md`, `../astra_v2/ASTRA_REPORT.md`, and `../REPORT.md` for full details. Do NOT re-propose any of these:
1. **Suppressing `S3` surges via raw `mu21` sign check** (Round 4): killed Reddit $D$, 10-decile hook, and Submissions 52w head share corr (`+0.807 → +0.461`). **Hard rule: NEVER destroy Reddit $D$ or the 10-decile hook!**
2. **`trim10` filter statistic** (Round 6 ablation): worsened real-panel `S3` Gate v1 on IG, Comments, and Submissions vs Astra's `median`.
3. **`0.01/wk` Laplace jumps with log-fitted `jump_b`** (Round 6): pushed FB boundary to `1.295` and `dep_rate` to `0.068`.
4. **Running `S3` on `scale_moment="robust"`** (Round 5): subtracting mean-square surges from the IQR crushed `sL2 -> 0` and blew up Submissions S3 to `0.266`.
5. **`decline_skew` / decline-state** (§2z-dy): killed by sign-preserving time-permutation null (`r2/r3` 100% accounted for) and near-zero 3-window mixed third moment.
6. **Constrained affine minorant jump budget `scale_moment="bounded"`** (Round 7 Astra): improved FB (`0.083`) and IG (`0.282`, by turning jumps off) but worsened Submissions Gate (`0.178 → 0.209`), worsened Comments `X`/`logR` CRPS at all 5 horizons, worsened 10/12 non-FB ladder spans, and failed planted recovery on head-concentrated jumps (`0/3`) and jump+surge coexistence (`0/3`).

---

## 5. Fresh Clues & High-Value Opportunities Left on the Table

Think independently and ambitiously within the three core goals! The sharpest remaining open mysteries are:

1. **Fixing `S3`'s fading-surge law (and unifying `S2` and `S3`!):**
   - Look at `S3`'s four linked flaws: (a) `kappa_5 / kappa_3` is 79–91% driven by the top 0.5% of observations (`mu_s = 1.35–1.44`, `hl_s = 52w`), firing huge capped-exponential surges that overshoot top-10 turnover (`FB 0.55 vs 0.35`, `Comments 0.45 vs 0.27`) and boundary (`1.13–1.16`); (b) on Instagram, `S3` triggers fake positive surges giving **wrong-sign `D = +0.082`** vs empirical **`-0.087 [-0.122, -0.050]`**; (c) `S3` turns off the lasting-jump split, leaving Submissions `S3` `cov90` at `0.939–0.953` (`PIT SD 0.242`) and `S3` lagging `S2` on Submissions Gate (`0.198 vs 0.178`) and Comments Gate (`0.124 vs 0.113`).
   - **Can you replace the unstable 5th-cumulant `kappa_5/kappa_3` surge estimator with an outlier-resistant, lower-order moment estimator** (e.g., quantile skewness / L-skewness / Winsorized odd 3rd+4th moments, or a cleaner bounded/attenuated surge law + clean detection of whether skewness actually fades vs. belongs to IG's negative-dip censoring) AND/OR unify the rare-jump split cleanly with `S3` without double-counting?!
2. **Astra Round 7's upper-tail asymmetry discovery on Submissions (`../astra_v3/forecast_strata.json`):**
   - At `h = 52w` on Submissions `S2`, **5.0% of head outcomes fall below the lower 5% bound (calibrated!)**, while **only 1.1% of head and 1.8% of middle outcomes exceed the upper 95% bound (against 5.0% expected!)**. Why? Look at the empirical lasting innovation distribution or the empirical drift/skewness of multi-week changes! Does the bounded empirical innovation pool (or asymmetric/skewed lasting steps, or something in how `innov_pool` is constructed/demeaned) miss a real, moment-identified skewness in the data?
3. **Any other clean, moment-identified mechanism inside the unified meta-framework** (e.g., `S2` head share / top-10 turnover gap on Submissions `0.150 vs 0.300` & FB `0.475 vs 0.350`, or Instagram `S2` Gate `0.306 → ~0.28` without hurting Submissions `0.178` or Comments `0.113`, or Kalman filter / volatility posterior refinement that genuinely beats `median` on real panels!).

**Be ruthless about scientific honesty:** if your diagnostics show a promising hypothesis fails or trades off against Round 6, test cleanly, record it honestly, and recommend keeping Round 6 if no candidate clearly earns adoption across the four platforms!

---

## 6. Required Workflow, Verification & Deliverables

1. **Diagnose FIRST, write pre-run predictions into `opus_r8/EXPERIMENT_LOG.md`, then implement cleanly.** Zero tuning to evaluation scores.
2. **Run exact apples-to-apples benchmarks via `benchmark_candidate.py`:**
   ```bash
   /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u llm_fitting/runs/2026-11-25_common_model/benchmark_candidate.py \
     --module llm_fitting/runs/2026-11-25_common_model/opus_r8/common_rankdiff_opus_r8.py \
     --out-dir llm_fitting/runs/2026-11-25_common_model/opus_r8 \
     --mode both
   ```
   Compare directly against **Round 6** (`baseline_r6/r6_s2_frozen_gate.json` for S2, `baseline_r6/r6_s3_frozen_gate.json` for S3, and `baseline_r6/r6_s2_gold.json` + `../astra_v3/candidate_gold_benchmark_results.json` for S3 Gold)!
3. **Run unit & planted recovery tests:** update `opus_r8/test_common_rankdiff_opus_r8.py` to import `common_rankdiff_opus_r8`, add recovery tests for every new mechanism, and verify all tests pass with `pytest`.
4. **Deliverables in `opus_r8/`:**
   - `common_rankdiff_opus_r8.py` (clean, self-contained candidate)
   - `test_common_rankdiff_opus_r8.py` (all tests passing)
   - `EXPERIMENT_LOG.md` (every diagnostic, prediction, failed attempt, and ablation logged honestly)
   - `candidate_frozen_gate_results.json` & `candidate_gold_benchmark_results.json`
   - `OPUS_R8_REPORT.md` — clear, rigorous scientific report with:
     1. Diagnostic findings & exact mathematical changes (and exact free scalar count),
     2. Side-by-side comparison vs. **Round 6 champion** (and Round 4/5/7) on Gate v1, Goal 1 (including `D`, 10-decile hook, boundary, top-10, departures, OOP), Goal 2 (`X CRPS`, `cov90`, `PIT SD`, `logR CRPS`, head share-shift MAE/corr), and Goal 3 (parsimony),
     3. Honest limitations, regressions, and explicit adopt / partial-adopt / reject recommendation.
