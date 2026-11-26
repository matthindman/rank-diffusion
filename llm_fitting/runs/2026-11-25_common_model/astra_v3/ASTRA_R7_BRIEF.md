# Round 7 Model-Improvement Brief — OpenAI Astra 6.2

You are running **Round 7** of the `rank-diffusion` Common Meta-Framework development program.
Read this entire brief carefully before touching code or running any script.

---

## 1. Owner's Direction & Core Goals

The owner asks for **one more deep squeeze** on the DEVELOPMENT panels before moving to held-out confirmation:

> *"In thinking about a common model, I really want a common meta-framework expressed as a core equation / set of equations that we use for all platforms. If there are one or two specific components in the meta-model that significantly help the fit for only one or two platforms and are otherwise zeroed out, that is not the end of the world. You can add parameters or complications iff you judge the better fit worth the price in parsimony."*

Your mission: **improve the Round 6 Common Meta-Framework (`common_rankdiff_astra_r7.py`, copied byte-identical from the newly adopted Round 6 `llm_fitting/common_rankdiff.py`) across all three core goals:**

1. **Goal 1 — Aggregate structure** across Facebook Era A (`K=3500`), Instagram §2z-c censoring apparatus (`K=10000`), Reddit Comments (`K=12500`), and Reddit Submissions (`K=5000`): stable rank-size ladder, head share, top-10 and `K/4` turnover, rank-1 churn, boundary crossing ratio (`~1.00`), canonical immediate-crossing departure composition (§2z-ac), gapped persistence `rho`, asymmetry `D`, 10-decile non-monotonic hook curves, and 2019–20 → 2021–22 out-of-period structure on Reddit.
2. **Goal 2 — Individual endpoint movement & audience-share shifts over any horizon** (`h = 1, 4, 13, 26, 52w`):
   - **Exact Frozen Rolling-Origin Movement Gate v1** (`5 × 20, boot 2000`, `round3_frozen_gate.py`) — **THE ADOPTION GATE.**
   - Exact `M = 39` endpoint forecasts (`round3_gold_benchmark.py`): `X CRPS`, analytical Kalman MAE vs. EWMA, exact `[x_(2), x_(38)]` 90% coverage (`0.900`) & PIT SD (`0.289`), `logR CRPS`, fair head Brier, and top-`K/16` head audience-share shift MAE & correlation.
3. **Goal 3 — Unified Common Meta-Framework & Parsimony:**
   - ONE clean set of equations across all platforms (zero platform-name conditionals, zero hardcoded platform switches).
   - Every parameter MUST come from a **declared empirical moment or independent instrument** — ZERO tuning to Gate v1, CRPS, coverage, `D`, or any evaluation score!
   - One or two platform-activating components that zero out cleanly by moment identification on platforms that do not need them are explicitly allowed if their fit gain clearly justifies the parsimony price.
   - Always test recovery on planted synthetic panels (`test_common_rankdiff_astra_r7.py`).

---

## 2. Hard Safety & Workspace Rules (MANDATORY)

- **ZERO contact with SEALED or EXPOSURE-UNKNOWN data.** Do NOT open, list values from, or reference the Reddit 2023–24 bridge (WD drive), Wikipedia (`/Volumes/T9/rank-diffusion-data/wikipedia`), or YouTube files. Use ONLY the four anonymous DEVELOPMENT `.npz` caches + `facebook_a_spec_b.json` in `/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache`.
- **Confine ALL edits and output files strictly to your isolated workspace:**
  `llm_fitting/runs/2026-11-25_common_model/astra_v3/`
  Do NOT modify `llm_fitting/common_rankdiff.py`, `tests/test_common_rankdiff.py`, `MODEL_STATUS.md`, ledgers, or any file outside `astra_v3/`.
- **Registered interpreter:** `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3` (pandas 2.2.2, numpy 2.0.1, scipy 1.14.0).

---

## 3. Where the Model Stands Right Now (Round 6 Baseline — Your Target to Beat!)

The Round 6 baseline JSONs are staged in `astra_v3/baseline_r6/` (and Round 4 baseline in `../round4_baseline/`, Round 5 Fable in `../fable_v2/`):

- **Core Meta-Equations (`common_rankdiff_astra_r7.py`):**
  - $X_{i,t} = L_{i,t} + \xi_{i,t} + s_{i,t} + \varepsilon_{i,t}$
  - $L_{i,t+1} = L_{i,t} + g(z_{i,t}) + \sigma_L(L_{i,t})\,\eta^L_{i,t} + B^J_{i,t} J_{i,t} + d_{i,t}$
  - Default (`scale_moment="meta"`, `asymmetry=False`, **12 scalars** when `gap_share > 0.20`, 11 otherwise): preserves exact mean-square `(phi, V_xi, sigma_obs)` so short-lag forecast calibration stays intact, and splits ONLY lasting variance into the `h<=13` robust IQR diffusive core + rare permanent **Gaussian** jumps (`JUMP_LAM_FIXED = 0.02/wk`, `jump_c` closed-form from lasting-variance gap, rank law inherited from `b_xi` and capped at `dz <= 0` so the `K..4K` buffer never explodes). Uses your Round 4 conditional-median `_volatility_posterior` (0 new scalars).
  - Asymmetry arm (`asymmetry=True`, **14–15 scalars**): keeps symmetric jumps off and uses `kappa_5/kappa_3` fading positive surges $s_{i,t}$ + 52w persistent entity drift $d_{i,t}$ ($\hat{\sigma}_d = 0$ on FB/IG, active on Reddit).

### Round 6 Baseline Scoreboard (Exact Frozen Gate v1, `5 × 20, boot 2000`)

| Platform | Persistence | Frozen Model A | Round 4 (`S2` / `S3`) | Round 5 Fable (`S2+J`) | **Round 6 Baseline (`S2` 12sc / `S3` 14-15sc)** |
|---|---:|---:|---:|---:|---:|
| `facebook_a` | `0.147` | `0.103` | `0.084` / `0.091` | `0.093` | **`0.089 ± 0.037` (80%, 3/5)** / **`0.091 ± 0.016` (80%, 5/5)** |
| `instagram_hm` | `0.869` | — | `0.282` / `0.304` | `0.344` | **`0.306 ± 0.232` (80%, 4/5)** / **`0.304 ± 0.230` (80%, 4/5)** |
| `reddit_comments_ext` | `0.158` | `0.123` | `0.134` / `0.124` | `0.096` | **`0.113 ± 0.035` (100%, 5/5)** / **`0.124 ± 0.057` (100%, 4/5)** |
| `reddit_submissions_long` | `0.165` | `0.188` | `0.263` / `0.198` | `0.196` | **`0.178 ± 0.066` (80%, 2/5)** / **`0.198 ± 0.079` (100%, 3/5)** |

**Round 6 Goal 1 & Goal 2 highlights:**
- Boundary ratios (`S2`): `FB 1.015 (0.980)`, `IG 0.993 (1.000)`, `Comments 1.013 (1.009)`, `Submissions 0.996 (1.116)`.
- Goal 2 exact `cov90` (`S2`): `FB 0.882–0.891`, `IG 0.887–0.891`, `Comments 0.900–0.914`, `Submissions 0.923–0.935`; sweeps `X CRPS` and `logR CRPS` on both Reddit ladders.

---

## 4. What Already Failed / Was Rejected (DO NOT Re-Propose!)

1. **Suppressing `S3` surges via raw `mu21` sign check** (Round 4): suppressed surges on all 4 panels, wiping out Reddit $D$ (`+0.149 → -0.050`), the 10-decile hook, and Submissions 52w head share correlation (`+0.807 → +0.461`). **Hard rule: NEVER destroy Reddit $D$ or the 10-decile hook!**
2. **`trim10` filter statistic** (Round 6 ablation): beat `median` on clean synthetic data, but worsened real-panel `S3` Gate v1 on IG (`0.304 → 0.333`), Comments (`0.124 → 0.139`), and Submissions (`0.198 → 0.210`).
3. **Laplace jumps at `0.01/wk` with unconstrained log-fitted `jump_b`** (Round 6): 3–5 log-unit Laplace bombs + steep `jump_b=0.30` pushed FB boundary to `1.295` and `dep_rate` to `0.068`.
4. **Running `S3` on `scale_moment="robust"`** (Round 5): subtracting mean-square surges from the IQR crushed `sL2 -> 0` and blew up Submissions S3 to `0.266`.
5. **Decline-state / `decline_skew` parameter** (§2z-dy): sign-preserving time-permutation null accounted for 100% of Reddit decline runs; 3-window mixed third moment was near zero.

---

## 5. High-Value Remaining Gaps to Attack (Think Independently!)

You are free to diagnose and propose ANY principled, moment-identified improvement inside the unified meta-framework. The most glaring remaining gaps in Round 6 are:

1. **Unifying `S2` and `S3` cleanly — AND fixing `S3`'s head/boundary/IG/Submissions-`cov90` blemishes without killing Reddit $D$ or the 10-decile hook:**
   - Currently `S3` (`asymmetry=True`) turns off the lasting-jump split (`jump_lam = 0`), leaving Submissions `S3` `cov90` over-dispersed (`0.939–0.953`, `PIT SD 0.242`) and `S3` lagging `S2` on Submissions Gate v1 (`0.198` vs `0.178`) and Comments Gate v1 (`0.124` vs `0.113`).
   - Meanwhile `S3`'s `kappa_5/kappa_3` surge estimator is 79–91% driven by the top 0.5% of increments (`mu_s = 1.35–1.44`, `hl_s = 52w`), causing top-10 turnover overshoot (`FB 0.55 vs 0.35`, `Comments 0.45 vs 0.27`), boundary overshoot (`1.13–1.16`), and wrong-sign $D$ on Instagram (`+0.082` vs empirical `-0.087`).
   - **Can you build ONE truly unified meta-model** (e.g., an outlier-resistant lower-order surge estimator + clean coexistence with the lasting-jump split, or a principled rank/horizon structure) where **both** aggregate structure (`boundary ~ 1.00`, `top10`, Reddit $D$, 10-decile hook) **and** Gate v1 / Goal 2 calibration improve simultaneously?
2. **Core `S2` head turnover & head share gaps:**
   - Under Round 6 `S2`, Facebook `top10_turn` ticked up (`0.350 → 0.475`, emp `0.350`) while Submissions `top10_turn` dropped (`0.233 → 0.150`, emp `0.300`) and Submissions `head_share` rose (`0.511 → 0.577`, emp `0.356`). Root cause: Facebook's head (`band 0`) has ~0 lasting jump gap (`0.0001`), whereas Submissions' head has a 48% jump gap — yet inherited `b_xi` allocates jumps evenly! Can a bounded, total-variance-conserving jump-share law (never allowing `jvar > gap_b` or buffer blowups!) fix FB `top10` AND Submissions `top10` simultaneously?
3. **Instagram `S2` Gate (`0.306`) & `X CRPS` (`0.4726` vs R3 `0.4701`) and `Submissions` residual `cov90` (`0.923–0.935`)** — diagnose why they persist and whether a clean, moment-identified fix improves them without hurting other platforms.

---

## 6. Required Workflow, Verification & Deliverables

1. **Diagnose FIRST, write pre-run predictions into `astra_v3/EXPERIMENT_LOG.md`, then implement cleanly.** Never tune parameters to evaluation scores.
2. **Run exact apples-to-apples benchmarks via `benchmark_candidate.py`:**
   ```bash
   /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u llm_fitting/runs/2026-11-25_common_model/benchmark_candidate.py \
     --module llm_fitting/runs/2026-11-25_common_model/astra_v3/common_rankdiff_astra_r7.py \
     --out-dir llm_fitting/runs/2026-11-25_common_model/astra_v3 \
     --mode both
   ```
   (Remember: `benchmark_candidate.py` prints diffs against Round 3; YOU MUST ALSO compare your results directly against the **Round 6 baseline** in `astra_v3/baseline_r6/`!)
3. **Run unit & planted recovery tests:** add tests for every new mechanism into `astra_v3/test_common_rankdiff_astra_r7.py` and run `pytest` against your candidate module.
4. **Deliverables in `astra_v3/`:**
   - `common_rankdiff_astra_r7.py` (clean, self-contained candidate)
   - `test_common_rankdiff_astra_r7.py` (all tests passing)
   - `EXPERIMENT_LOG.md` (every diagnostic, prediction, failed attempt, and ablation logged honestly)
   - `candidate_frozen_gate_results.json` & `candidate_gold_benchmark_results.json`
   - `ASTRA_R7_REPORT.md` — clear, rigorous scientific report explaining:
     1. Your diagnostic findings & exact mathematical changes (and exact free scalar count),
     2. Side-by-side comparison vs. **Round 6 baseline** (and Round 4/5) on Gate v1, Goal 1 (including `D`, 10-decile hook, boundary, top-10, departures, OOP), Goal 2 (`X CRPS`, `cov90`, `PIT SD`, `logR CRPS`, head share-shift MAE/corr), and Goal 3 (parsimony),
     3. Honest limitations, regressions, and explicit adopt / partial-adopt / reject recommendation.
