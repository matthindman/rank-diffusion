## 1) What the master analyses tell us (weekly Facebook data)

### A. Data coverage and “universe” stability constraints

* The weekly panel spans **176 weeks** from **2020-10-26 to 2024-03-04**.
* There are **15,685 unique endpoints** in the file.
* The number of rows/endpoints observed **varies a lot by week** (roughly **4,214 to 14,714**, median ~14,066). This matters because “disappearing” endpoints may partly reflect truncation / incomplete coverage rather than true “exit.”

**Implication:** any turnover / retention metric must be interpreted as “still observed in our weekly capture,” not necessarily “still exists.”

---

### B. Macro structure: the system *does* look like a stationary ranked-weight environment

Even without quoting specific RMSE values from the “quarterly CDC stability” plot (the code computes them but doesn’t print a table), the analysis is explicitly structured around checking that the **capital distribution curve (CDC)** is stable enough to treat the platform as being in a roughly stationary regime. 

**Why this is SPT-relevant:** SPT’s ranked market weight framework is most compelling when there is a stable long-run “shape” of ranked weights (the CDC), with ongoing local rank rearrangements around that shape.

---

### C. Micro growth magnitudes are large and scale with horizon (and differ by size bucket)

The master analysis computes absolute log-growth over multiple horizons for “large/midsize/small” buckets. Key values:

* **1 week ahead:** median |Δlog share| is ~0.047–0.055 depending on bucket.
* **28 weeks ahead:** median |Δlog share| rises to ~0.213 (large), ~0.280 (midsize), ~0.335 (small).
* The **upper tail is huge**: at 28 weeks, the 90th percentile reaches **0.527–0.716** depending on bucket. 

**Interpretation:**

* There’s meaningful mobility even at weekly frequency, and it accumulates strongly over longer horizons.
* Smaller endpoints are (unsurprisingly) the most volatile over long horizons; but even “large” endpoints have substantial changes over ~6 months.

---

### D. Non-Gaussian shocks are a first-order feature (heavy tails)

After standardizing rank-slot log changes, the tail rates are extremely high relative to a Gaussian benchmark:

* **P(|z| ≥ 5)** is ~**0.27%–0.39%** across buckets.
* **P(|z| ≥ 3)** is ~**0.85%–1.46%**.
* Extreme quantiles reach roughly **q0.001 ≈ −6** and **q0.999 up to 7.76** (small bucket). 

**Implication for modeling:** a pure diffusion with Gaussian increments is not going to reproduce the tails unless you add (at minimum) stochastic volatility, jumps, or mixture structure.

---

### E. Log gaps (Xi) behave in a clean, SPT-friendly way, and imply a “single sigma” is plausible

The analysis computes the log gap
[
\Xi_k(t)=\log\Big(\frac{w_k(t)}{w_{k+1}(t)}\Big)
]
and builds a **v11-style** implied volatility curve:

* Define a smoothed drift proxy (g_k) and (S_k=\sum_{i\le k} g_i),
* Combine with the median gap ( \rho_k = \text{median}(\Xi_k)),
* Form (\hat\sigma_k=\sqrt{-2 S_k\rho_k}) where positive.

This produces an overall median implied sigma of **~0.0128**.

Stability is then checked over time (quarterly) with a bootstrap CI workflow.

**Why this is important for Adrian/SPT:**

* In classic Atlas/rank-based SPT models, the stationary distribution of gaps is tightly linked to rank-based drifts and a (possibly constant) diffusion scale.
* Getting a *stable* implied (\sigma) from gaps + drift proxy is one of your strongest “this really is SPT-like” diagnostics.

**Caveat (already implicit in the code):** here (g_k) is a drift proxy derived from **rank-slot changes in log weights**; in SPT, the cleanest identities typically reference rank-based drifts of **log capitalizations** (or log-name processes) plus local time terms. The gap (\Xi) is scale-invariant (good), but the mapping from rank-slot weight drift → SPT (g_k) is not guaranteed to be exact.

---

### F. Rank-conditioned mobility: high retention in being *observed*, but meaningful rank displacement

The cohort mobility table (identity-based: “endpoint that sits at rank k at week t”) shows:

* For **k=1** retention (still observed) is **0.989 (1w)**, **0.966 (4w)**, **0.920 (12w)**; median |rank change| grows from **1 → 2 → 3**.
* For **k=12** retention is **0.983 (1w)**, **0.966 (4w)**, **0.920 (12w)**; median |rank change| grows from **6 → 8 → 13**.
* The probabilities of “staying very close” (within 1 or 5 ranks) fall quickly with horizon. 

**Interpretation:**

* Even when endpoints remain “in the observed set,” their rank can move substantially.
* This is exactly the regime where rank-based modeling is meaningful: the *rank process* churns while the global ranked shape stays stable.

---

## 2) How well the rank-diffusion model reproduces macro + micro structure

### A. Baseline model configuration and what it matches

The master analysis’s **baseline simulation** uses:

* **Gaussian rank-slot increments** with **smoothed** per-rank mean/sd (smoothing window (h=5)),
* Global volatility scaling **sigma = 0.16** and **mu = 0**, with entry/refresh in the tail to keep a fixed-length state.
* It evaluates fit on **CDC**, **durable change**, and **Xi**.

The scoreboard reports:

* **rmse_cdc = 0.126**
* **rmse_durable = 0.0552**
* **rmse_xi = 0.00790** 

And the sigma calibration loop independently lands on essentially the same value:

* **best sigma = 0.16**, with **rmse_dur = 0.0546**, **rmse_cdc = 0.126** (score 0.0861). 

**Assessment:**

* On the *ranked-weight objects the model is built to match* (CDC + Xi + a bucketed durable-change target), the fit is meaningfully nontrivial and internally consistent.
* The fact that Xi is included as a fit dimension and is matched with low RMSE is a major “SPT-compatibility” plus.

---

### B. The model *does not* reproduce identity-tracking micro movement at the top (as currently compared)

For rank **k=12**, the empirical identity-tracking object vs the simulation’s rank-slot object looks like this:

* **Empirical (identity-based)** medians decline with horizon and have a wide 10–90 band:

  * h=1: median **0.00464**, p10 **0.00210**, p90 **0.00752**, retention **0.983**
  * h=4: median **0.00427**, p10 **0.000905**, p90 **0.00752**, retention **0.966**
  * h=12: median **0.00333**, p10 **0.000509**, p90 **0.00717**, retention **0.920**
* **Simulation (rank-slot)** is much tighter and higher:

  * h=1: median **0.00539**, p10 **0.00503**, p90 **0.00571**
  * h=4: median **0.00538**, p10 **0.00502**, p90 **0.00572**
  * h=12: median **0.00535**, p10 **0.00499**, p90 **0.00572** 

So: **the simulation produces almost no dispersion in the rank-slot share at k=12 across time**, while the empirical identity-tracked endpoint’s future share is highly dispersed.

Across ranks **k=10…200**, the same identity-vs-rank-slot mismatch is visible in RMSE (highest near the top):

* Example: **k=10 RMSE 0.00143**, dropping to **~0.0002–0.0004** by k≈150–200; average retention stays ~0.94–0.96. 

**Critical interpretation (and it’s explicitly in the master file):**

* The master analysis adds an **empirical rank-slot comparator** (new) to make “apples-to-apples” comparisons with the simulation: rank-slot tracking is conceptually different from identity tracking. 
* SPT rank-based diffusions are fundamentally about the dynamics of the *ranked weights* (and their gaps/local times). Identity tracking is a different object and typically requires labeled particles/names.

**Conclusion:** the current micro mismatch is not decisive evidence against an SPT-style rank diffusion; it’s evidence that you must be precise about whether you’re matching **rank-slot dynamics** or **name/endpoint dynamics conditional on starting rank**.

---

### C. Where the model is structurally missing key empirics (even if you fix the micro-object mismatch)

1. **Heavy tails** are not represented by Gaussian increments. 

2. **Common shocks / correlation structure**: the analysis includes a PCA diagnostic on rank-slot increments to reveal common movement (not quantified in the printed text output, but the section exists and the intent is clear). This is a typical reason diffusion models underperform if they assume conditional independence across ranks.

3. **Short-horizon mean reversion / serial dependence**: the Rmd includes a martingale-style regression diagnostic, and the plotted betas are clearly negative (roughly around −0.3 by eye). 
   A pure i.i.d. increment diffusion will not reproduce that.

---

## 3) Initial conclusions re: SPT-style consistency

### Strong points (consistent with SPT-style thinking)

* **Ranked shares behave like market weights** in the sense that there is a coherent, stable CDC structure that motivates rank-based modeling. 
* **Log gaps (Xi) behave cleanly** and support an SPT-like “drift + diffusion intensity ↔ spacing” relationship; implied sigma is stable and plausibly close to “single-sigma” Atlas-style behavior.
* A **smoothed rank-diffusion model** can simultaneously approximate CDC, durable-change, and Xi reasonably well (per the scoreboard). 

### Key tensions / likely departures from classical SPT assumptions

* **Birth/death / changing universe** (endpoints not observed every week; variable max rank) is a major structural difference from the usual fixed-universe equity setting.
* **Non-Gaussian jumps** are too prevalent for a simple Brownian diffusion. 
* **Serial dependence / mean reversion** appears at weekly scale. 
* **Identity-level heterogeneity** (some endpoints persistently “high quality,” others bursty) likely matters; pure rank-slot models can match ranked objects while failing conditional-on-name targets.

---

## 4) What remains unclear / not yet nailed down

### A. The most important unresolved conceptual point

**What is the primary object of interest for the “SPT analogy”?**

1. Ranked weights (w_{(k)}(t)), CDC, Xi gaps, local times (SPT core), or
2. Identity-based endpoint trajectories conditional on starting rank (requires labeled dynamics).

Right now the Rmd contains *both*, and the micro mismatch is largely driven by mixing these objects. 

### B. Is the v11-style (\hat\sigma) construction correctly mapped to SPT parameters?

You compute (\hat\sigma) using a drift proxy built from **rank-slot log-weight changes**.
But SPT identities often use rank-based drifts of log **capitalizations** (or name processes) and local time terms. The gap object is fine; the drift proxy mapping needs justification or replacement.

### C. Is there a regime change (or data artifact) in turnover metrics?

Because the weekly observed universe size varies widely, any time-local dip/spike in “top-K overlap” or retention might be:

* a real platform regime shift, or
* a data ingestion/coverage artifact.

The master file doesn’t yet pin this down with explicit data QA flags (e.g., dropping weeks where max_rank is far below typical).

### D. What exact features of the data force sigma to be small (~0.16)?

Sigma is strongly identified by your durable-change calibration and CDC penalty. 
But “why” sigma must be small (relative to naive per-rank sd estimates) is not fully decomposed (is it mostly about normalization constraints, sorting, smoothing, or entry process?).

---

## 5) How to fix/refine/expand the Rmd — prioritized by biggest payoff

### Priority 1 — Make “rank-slot vs identity” explicit everywhere (and score them separately)

**Problem:** the micro validation currently compares an identity-based object to a rank-slot simulation object, which *guarantees* systematic mismatch at top ranks. 

**Fix/refinement:**

1. Treat these as two different validation tracks:

   * **Rank-slot track (SPT core):** CDC, Xi, rank-slot micro, rank-bin transition matrix.
   * **Identity track (extension):** retention, identity-conditioned future share, distribution of future rank.
2. In the “Model fit” section, print **two scoreboards**:

   * Rank-slot fit metrics (CDC, Xi, rank-slot micro bands).
   * Identity fit metrics (retention curve by k/horizon; conditional future share distribution; rank-change distribution).

**Highest payoff change:** update micro scoring so the default “micro RMSE” compares **emp_rankslot** to simulation (not identity-based), and put identity-based micro in a separate section as “requires labeled particle model.”

---

### Priority 2 — Implement a labeled-particle simulation if identity-based movement is a target

If you want “endpoint at rank k at time t” to be a primary validation target, you need a simulation that keeps identities:

* Simulate named log-capitalizations (X_i(t)) (or latent “quality”) with rank-based drift/vol,
* Sort to get ranks each week,
* Track the particle that was at rank k at t forward.

Right now the simulation object is explicitly **rank-slot** (rank k at t+h, not same endpoint). 

**SPT connection:** this is much closer to Atlas / rank-based name dynamics and opens the door to local-time estimation and occupation-time diagnostics.

---

### Priority 3 — Replace Gaussian increments (or augment them) to match heavy tails

You already measured extreme tail frequencies that are orders of magnitude above Gaussian. 

**Concrete upgrades (in increasing complexity):**

1. **Student-t innovations** per rank (estimate df by bucket; keep the same mean/sd).
2. **Mixture-of-normals** innovations (e.g., small probability of “jump regime” with inflated variance).
3. **Jump diffusion**: (d\log w = \dots + \sigma dB + J dN), calibrating jump intensity and jump size distribution from tail diagnostics.

**Rmd improvements:** add a “tail calibration” target set (match p(|z|>3), p(|z|>5), q0.001/q0.999 by bucket), and include it in the fit scoreboard.

---

### Priority 4 — Add common shocks / factor structure (PCA section → model component)

The Rmd already includes a PCA diagnostic for common shocks. The next step is to encode it:

* Add a weekly common factor (F_t) with rank-dependent loading (b_k):
  [
  \Delta \log w_k(t) = \mu_k + b_k F_t + \sigma_k \epsilon_{k,t}
  ]
* Estimate (b_k) from the first PC loadings; calibrate (Var(F_t)) from PC1 variance.
* This will improve both macro stability and micro dispersion patterns (especially at top ranks).

---

### Priority 5 — Use an SPT-faithful drift proxy (and/or compute local-time-style objects)

Right now (\hat\sigma) uses a drift proxy based on rank-slot weight changes.

**Upgrades:**

1. Recompute drift using **metric_value** (the capitalization analog) at the name level:

   * log returns of metric_value by endpoint,
   * then translate into ranked objects.
2. Add “local time proxy” diagnostics using week-to-week crossings / time spent near boundaries (discrete analogue).
3. Check whether the implied drift/spacing identities hold when drift is estimated from metric_value rather than weights.

**Payoff:** much stronger discussion with an SPT expert about “are these *the same* dynamical objects?”

---

### Priority 6 — Universe/coverage QA integrated into every mobility metric

Given weekly row-count swings, add:

* flags for weeks with unusually low max_rank,
* sensitivity: recompute turnover/retention after excluding those weeks,
* show how much retention is true mobility vs missing coverage.

---

### Priority 7 — Improve reporting: print tables for the most decision-critical quantities

Several sections plot quantities but don’t print the key tables. For meeting-readiness:

* Print `cdc_rmse_vs_full` by quarter as a table (top 10 worst quarters).
* Print PCA variance-explained (top 10 PCs).
* Print full `mobility_summ` (currently truncated after 10 rows). 
* Print `emp_targets` durable-change targets (not just plots), since sigma calibration depends on them.

---

### Priority 8 — Make calibration “research-grade”

* Add block bootstrap CIs for the empirical targets (CDC, Xi, durable, mobility), then report whether simulation falls within CIs.
* Add train/test splits across time (e.g., calibrate on first 70% of weeks, score on last 30%) to detect overfitting to a stationary assumption.

---

## 6) Bottom line

* **Most convincing SPT-aligned evidence in the current results:** stable ranked macro structure + clean log-gap behavior + stable implied (\sigma), and a rank-diffusion model that can match CDC + Xi + durable-change simultaneously with a coherent parameter choice. 
* **Most important current limitation:** the model is being evaluated against identity-based micro movement without a labeled-particle simulation, and the data exhibit heavy tails and common shocks that a Gaussian i.i.d. rank diffusion will miss. 

