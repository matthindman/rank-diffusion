# Round 8 Experiment Log — Claude Opus 5.5 (`opus_r8/`)

Date: 2026-11-26. Workspace: `llm_fitting/runs/2026-11-25_common_model/opus_r8/`. Interpreter:
`/Library/Frameworks/Python.framework/Versions/3.11/bin/python3` (numpy 2.0.1, scipy 1.14.0).

**Data contact.** Only the four DEVELOPMENT caches (`facebook_a`, `instagram_hm`, `reddit_comments_ext`,
`reddit_submissions_long` `.npz`) and `facebook_a_spec_b.json` in the Antigravity scratch `panel_cache`.
No sealed or exposure-unknown file was opened, listed or referenced (no WD bridge, no T9 Wikipedia, no
YouTube). All writes are inside `opus_r8/` (logs in `logs/`, runtime temp in `.runtime/`), with
`PYTHONDONTWRITEBYTECODE=1` so that importing the shared harness writes no `__pycache__` outside.
Every heavy job ran alone under the swap watchdog `runs/2026-10-18_pnas_push/r12_safe_run.sh`
(peak RSS 0.5–1.4 GB; swap never grew).

**Starting point.** `common_rankdiff_opus_r8.py` byte-identical to `llm_fitting/common_rankdiff.py`
(checked with `cmp`); the untouched copy is kept as `baseline_r6/common_rankdiff_r6_source_copy.py`.
`test_common_rankdiff_opus_r8.py` re-pointed to `common_rankdiff_opus_r8`; on the unmodified copy
**23/23 passed** (`logs/baseline_tests.log`).

---

## 1. Diagnostics (before any model change)

All diagnostics read data moments and fitted parameters only. None reads a Gate v1 or Gold score of a
candidate; D3 and D4 compare the model with **training-window** data only.

### D1 · Band variance accounting (`diag_bands.py` → `diag_bands.json`)
Full-panel R6 S2 fits. The smooth laws mis-fit the extreme bands: Submissions head band D(52) measured
0.309 vs 0.196 implied by the laws; Comments bottom band (K/2..K) D(52) 1.375 vs 0.983. On Submissions the
measured mean-square lasting variance is flat (~0.005/wk) across bands 0–5, while the jump rank law
inherited from `b_xi` makes the assigned total rise 3× toward K (0.0030 → 0.0087).

### Gold strata re-read (`../astra_v3/forecast_strata.json`, R6 S2 rows)
The Submissions "upper-tail asymmetry" is mostly a **location bias**: the median forecast for the head
(K/16) is too high by −0.016 / −0.050 / −0.109 / −0.193 at h = 4 / 13 / 26 / 52, with the same sign at all
five origins and roughly linear in h (middle −0.073 at 52w). Lower-tail misses stay near 5% because the
head interval is also too wide (cov90 0.939–0.946).

### Goal 1 re-read (R6 S2 vs source S3, `baseline_r6/r6_s2_gold.json`, `../astra_v3/...gold...json`)
Reddit data show momentum (gapped ρ +0.145 / +0.155; rising hook; p_fall ≫ p_rise). R6 S2 has the
wrong sign (ρ −0.047 / −0.041, falling hook). S3 recovers sign and hook through rare large surges
(λ ≈ 0.0013, μ ≈ 1.2–1.4, hl at the 52w grid edge) plus a rank-uniform drift σ_d ≈ 0.0092–0.0096.
R6 S3's persistent drift turns Submissions head share-shift correlation from −0.035 / −0.250 / −0.437
(S2, h = 13/26/52) into +0.41 / +0.67 / +0.81.

Empirical ladder spans compress from 2019 to 2020 on both Reddit panels and then stay level (except
rank 1 on Submissions). This is a one-off shock, not a steady slide, so a measured-drift ladder
correction would extrapolate a shock. **Not pursued.**

### Gate v1 re-read (`baseline_r6/r6_s2_frozen_gate.json`)
On both Reddit panels R6 S2 moves the top-100 **more than both the training window it was fitted on
and the test window**. Submissions dRank13 train 7–8 / test 6–8 / S2 8–10; p90 too high; RACF1 too
high at 5/5 origins. Source S3 is worse (dRank13 up to 13 vs 7).

### D3 · In-sample head attribution (`diag_head.py`, training window only)
Simulated from the filtered state 52 weeks before the end of training and compared with those observed
training weeks. Ladder gaps match (model 0.014 vs data 0.0146). The excess is in X movement itself:
median one-week |ΔX| of the top-100 is 12–18% too large on Submissions, Comments and FB. On Submissions,
passing+observation noise alone already equals the data's total one-week movement. Removing the 2%/wk
jumps cuts Submissions dRank13 from 9.3 to 8.0 (data 7).

### D4 · Volatility posterior (`diag_vpost.py`, `diag_vpost2.py`)
For the top-200 the R6 posterior's v̂ implies a median |ΔX₁| **5–11% above the entity's own measured
median** (the posterior's input). The log ratio at v̂ is +0.085 / +0.063 / +0.096 (Subs / Comments / FB):
- about +0.04–0.05 is mean-vs-median of a wide posterior (SD of log v 0.38–0.48). This is correct
  Bayesian behaviour, because the gate harness draws v around v̂ as a posterior mean.
- about +0.03–0.04 is shrinkage of calm head entities toward a rank-blind prior centre.

Over all entities the sign reverses on Reddit (−0.07, −0.18). Later training weeks are a further 7–8%
calmer on Reddit. **Recorded, not implemented**: the correctable part is ~0.03–0.04, it would raise
RACF1 (already too high on Reddit), and it changes buffer noise in the opposite direction.

### D2 / D5 · Where the surge signal lives (`diag_skew.py`, `diag_asym_model.py`)
- The top 0.5% of |increments| carry 57–71% of κ₃ and 79–91% of κ₅ (confirms the brief).
- The bounded-influence tail asymmetry A(q) = P(Δ−med > q·s) − P(Δ−med < −q·s), s = IQR/1.349, is
  positive on all four panels. On Reddit it peaks at q≈2 (≈ +0.014–0.019) and stays positive to q = 6.
- An MC of the fitted laws without selection shows R6 S3 producing **less** A than the data, about
  half on Reddit.

### D6 / D7 · Symmetric-null and rank allocation (`diag_null_skew.py`, `diag_tiers.py`)
- Applied to whole panels simulated from fitted R6 S3, the surge estimator recovers its own parameters
  (self-consistent).
- S3-simulated panels match the data's **far-tail** A on Reddit closely (Comments h=13: A(4) 0.0041 vs
  0.0042, A(6) 0.0019 vs 0.0018), but fall short at q ≤ 2 for h ≥ 26.
- Panels simulated from fitted S2 trigger surges on FB and Submissions. Band-demeaning does not remove
  this, because the S2 "null" is not symmetric: the S2 empirical innovation pool inherits the data's
  positive skew.
- **Conclusion: the brief's suspicion that S3's surges are too big is not supported at the far tail,
  and the surge estimator shows no selection bias. No surge-estimator change is made.**
- **The persistent drift is strongly rank-dependent and R6 applies it uniformly.** Tier estimates net
  of passing and surge covariance (σ_d at K/16, K/4, K):

| Panel | K/16 | K/4 | K |
|---|---:|---:|---:|
| Comments | 0.0064 | 0.0075 | 0.0104 |
| Submissions | 0.0044 | 0.0069 | 0.0103 |

R6 S3 uses a pooled 0.0092–0.0096 for every rank, which overstates head drift variance by 2–4×. This
is a direct mechanism for S3's excess head movement.

### D8 · False-positive drift on drift-free planted panels (`diag_drift_null.py`)
Hypothesis: the within-tier spread of the Atlas balancing drift mimics entity drift. **Rejected**:
H²·Var_tier(g) ≈ 0.0001. The spurious σ_d ≈ 0.003–0.004 comes from **sampling noise** of the gapped
covariance (few overlapping windows; small head tier), and R6's pooled estimator has it too (it only
ran in S3). Fix: entity-clustered SE and a declared activation threshold of 2 SE.

### D9 · Is the Reddit drift a systematic rank trend? (`diag_drift_resid.py`)
Residualising window changes on a within-tier quadratic in permanent log-rank leaves the covariances
essentially unchanged (e.g. Submissions outer 0.0837 → 0.0792, 0.0400 → 0.0404). The outer tier is
positive in all 8 windows (0.02–0.08); the head tier changes sign around 0. This is genuine
entity-specific persistence, falling toward the head.

### D10 · Tier definition on planted rank-dependent drift (`diag_drift_tierdef.py`)
Tiers by whole-panel permanent rank **attenuate** the slope toward rank-uniform (planted 0.3 →
0.05–0.12; 0.6 → 0.32 / 0.58), because high-drift risers are counted in the head tier. Tiers by
window-start rank do not fix this reliably (0.11 / 0.19 / 0.18 / 2.99). Kept permanent rank (the R6
convention). The rank law is therefore **conservative**: it shrinks toward the R6 rank-uniform drift.

---

## 2. Candidate v1 — persistent drift in the core with a tiered rank law

Exact changes (no platform names, no switches on panel identity):

1. **Drift in both arms (`drift_core=True`).** The persistent entity drift d_i (AR(1),
   hl_d = DRIFT_HL_FIXED = 52, unchanged) is estimated in the S2 arm as well as S3, from the same
   gapped window-change covariance net of the passing AR(1) covariance (and, in S3, the surge
   covariance). It is zero when that net moment is ≤ 2 entity-clustered SE or T < 75.
2. **Rank law (`drift_rank_law=True`).**
   σ_d(z) = σ_d · exp(b_d · min(z − z_K, 0)): one law inside K, constant through the buffer (the
   surge/jump cap convention).

   The same moment is computed inside the K/16, K/4 and K permanent-rank tiers. Each tier subtracts
   the passing and surge covariance evaluated for that tier's own entities. (σ_d, b_d) come from a
   WLS line of log σ_d,tier on min(z̄_tier − z_K, 0) with delta-method weights (net/SE)². With fewer
   than 2 positive tiers, b_d = 0 and the pooled value is used.
3. The drift variance enters every place it did before, now per band or per entity:
   - variogram subtraction (variance-conserving NNLS refit);
   - jump-gap accounting;
   - robust-fit MC increment law;
   - simulator (initial draw at current rank; AR innovation scaled by current rank);
   - volatility posterior;
   - 4-state Kalman (per-entity q_d and prior variance).
4. **Free scalars.**
   - S2 arm: R6 count (11–12) + 2 (σ_d, b_d) when drift is active (Reddit), 0 otherwise.
   - S3 arm: R6 count + 1 (b_d).

   FB and IG never activate the drift (FB net moment negative, z ≈ −5.5; IG T < 75). Every FB and IG
   fit, and every Reddit fit with T < 75 (gate origin 53), is therefore identical to R6.
5. **Compatibility.** With `drift_core=False, drift_rank_law=False` the module reproduces the R6 source
   bit for bit: fitted scalars, drift profile, filtered states and simulated panel, both arms, real
   Submissions T0 = 106 window (`check_r6_identity.py`, `logs/check_r6_identity.log`), plus a planted
   unit test.

Full-panel parameters (`diag_params_r8.py` → `diag_params_r8.json`; no scores read):

| Panel | Arm | σ_d(K) | b_d | Tier σ_d (K/16, K/4, K) | Tier z (net/SE) | σ_L0 (R6 → R8) |
|---|---|---:|---:|---|---|---|
| Comments | S2′ | 0.0119 | +0.174 | 0.0067, 0.0080, 0.0110 | 5.2, 9.3, 16.0 | 0.074 → 0.039 |
| Comments | S3′ | 0.0114 | +0.186 | 0.0062, 0.0074, 0.0104 | 4.5, 7.9, 14.4 | 0.047 → 0.065 |
| Submissions | S2′ | 0.0124 | +0.345 | 0.0032, 0.0064, 0.0103 | 1.0, 6.1, 15.1 | 0.072 → 0.036 |
| Submissions | S3′ | 0.0127 | +0.341 | 0.0034, 0.0066, 0.0106 | 1.1, 6.5, 16.0 | 0.058 → 0.014 (β at −0.25 bound) |

Training windows: drift active at every Reddit window ≥ 80. b_d is +0.08…+0.29 on Comments. On
Submissions it is −0.11 / −0.03 / +0.03 / +0.27 at T0 = 80 / 106 / 133 / 160: the rank gradient only
emerges in later Submissions windows.

**Flag for interpretation.** On Reddit, S2′ moves a large share of the near-K lasting variance from the
diffusive core into the persistent drift (the variogram's h² curvature). On Submissions S3′ the smooth
core law tilts to the β lower bound. Both are consequences of variance conservation, not tuning, but
they are the riskiest part of v1.

Tests: **29/29 pass** (`logs/candidate_tests_v1.log`): the 23 original tests, with the S3 planted
scalar count corrected 15 → 16 plus an |b_d| < 0.15 check on rank-uniform planted drift, and 6 new tests:
- rank-dependent planted drift recovered conservatively;
- rank-uniform planted drift gives a flat law;
- drift stays off on 3 drift-free seeds;
- simulator stationary drift variance follows σ_d(z)² by rank;
- flags-off is bit-identical to the R6 source;
- the 4-state filter's d̂ tracks the planted drift.

---

## 3. Pre-run predictions for candidate v1 (written before any benchmark of the candidate)

FB and IG are identical to R6 in every Gate and Gold fit, and Reddit gate origin 53 is identical
(T < 75), so all differences come from the Reddit fits with T ≥ 75.

- **P1 · S3′ Gate.** Comments 0.124 → ≤ 0.120 and Submissions 0.198 → ≤ 0.195, via lower dRank13 /
  RACF1 errors at origins ≥ 80. FB 0.091 and IG 0.304 identical.
- **P2 · S2′ Gate.** FB 0.089 and IG 0.306 identical. Comments and Submissions within ±0.015 of
  0.113 / 0.178, direction not predicted.
- **P3 · Goal 1, S2′ on Reddit.**
  - gapped ρ > +0.05 on both panels (R6 S2 −0.047 / −0.041);
  - hook bottom decile < 0 < top decile, increasing over deciles 1–9;
  - |D| ≤ 0.05 (no asymmetric channel in S2′);
  - boundary ratio within ±0.05 of R6 S2;
  - top-10 turnover within ±0.05 of R6 S2.
- **P4 · Goal 1, S3′ on Reddit.**
  - D within ±0.03 of R6 S3 (0.149 / 0.163) and hook shape preserved;
  - Comments top-10 turnover 0.45 → ≤ 0.42;
  - Submissions top-10 turnover in [0.22, 0.35];
  - boundary within ±0.05 of R6 S3.
- **P5 · Goal 2, Reddit.**
  - S2′ Submissions head share-shift correlation > +0.30 at h = 52 (R6 −0.437) and > +0.20 at
    h = 26 (R6 −0.250);
  - S2′ Kalman MAE improves at h = 26 and 52 on both Reddit panels;
  - S2′ X CRPS at h = 1 and 4 within ±0.003 of R6 S2;
  - S3′ Submissions cov90 at h = 13–52 lower than R6 S3 (0.936–0.941) by ≥ 0.003.
- **P6.** FB and IG Goal 2 identical to R6.

**Decision rule (pre-registered).**
- Adopt the S3′ change if its gate mean is ≤ R6 S3 on both Reddit panels, D stays within ±0.03, the
  hook shape is kept, and no Reddit X CRPS cell worsens by > 0.002.
- Adopt `drift_core` for S2 only if S2′ gate ≤ R6 S2 + 0.005 on both Reddit panels, Reddit
  persistence and hook improve, and no Reddit X CRPS cell worsens by > 0.002. Otherwise S2 reverts to
  R6 (`drift_core=False`) and is reported as such.

---

## 4. Benchmark of candidate v1 (`runs/v1/`, launched 19:40, finished 20:21)

Command: `zsh run_benchmark.sh v1 both`, i.e. caffeinate → swap watchdog → `scripts/heavy_lock.py` (lock files
in `opus_r8/.runtime`) → `../benchmark_candidate.py --module runs/v1/module_snapshot.py --out-dir runs/v1 --mode both`.
The snapshot is byte-identical to the final `common_rankdiff_opus_r8.py`. Peak RSS 1.24 GB; swap 10672 → 10915 MB
(machine-wide, under the 400 MB watchdog limit; not killed). Gaussian oracle: cov90 0.89855, PIT SD 0.28951
(unchanged). Full tables: `COMPARISON_TABLES.md` (`compare_r8.py v1`).

**Gate v1** (paired over the 5 origins against the R6 comparators of the brief):

| Panel | R6 S2 | R8 S2′ | Paired Δ (↓/↑) | R6 S3 | R8 S3′ | Paired Δ (↓/↑) |
|---|---:|---:|---|---:|---:|---|
| FB | 0.089 | 0.089 | identical | 0.091 | 0.091 | identical |
| IG | 0.306 | 0.306 | identical | 0.304 | 0.304 | identical |
| Comments | 0.113 | 0.120 | +0.007 ± 0.031 (2/2) | 0.124 | **0.087** | **−0.037 ± 0.022 (4/0)** |
| Submissions | 0.178 | 0.191 | +0.013 ± 0.030 (1/3) | 0.198 | 0.260 | +0.062 ± 0.106 (1/3) |

All Reddit T0 = 53 origins reproduce R6 exactly (drift off at T < 75).

Comments S3′ improves at all 4 drift-active origins, on the predicted moments:

| Comments S3′ moment | R6 → R8 |
|---|---|
| dRank13 | 0.27 → 0.18 |
| RACF1 | 0.23 → 0.14 |
| coll1 | 0.61 → 0.41 and 0.88 → 0.28 |

Submissions S3′ loses mainly at T0 = 80 (0.177 → 0.427: dRank4 7 vs 4, dRank13 12 vs 6) and at T0 = 159 (rank-1
collisions on a 0.077 base rate, relative error 0.90 → 1.59).

### D11 · POST HOC (after the v1 gate): instability of the S3 smooth lasting law (`diag_corelaw.py`)
At Submissions T0 = 80 the drift barely changes (σ_d(K) 0.0084 → 0.0089, b_d −0.02), yet the S3 lasting law moves
from β = +0.085 / σ_L0 0.060 to β = −0.250 (the lower bound) / σ_L0 0.019, doubling the head lasting SD (0.037 → 0.075).

The cause is in R6 S3 itself. After the surge and drift variance are subtracted bandwise, the NNLS leaves most
bands with **exactly zero** lasting variance. Examples (×10⁻³, 7 in-K bands):

| Panel, window | Bandwise lasting variance | Fitted β |
|---|---|---|
| Comments T0 = 133 | 0, 0, 0.10, 0, 0, 4.17, 9.27 | +0.500 (upper bound) |
| Submissions full panel | 6 of 7 bands zero | 0 (fallback) |

The log-scale law drops exact zeros and gives near-zero bands full weight. Whether a band lands at 0 or at
0.0002 therefore decides β.

A variance-scale profile fit with the zero bands kept was checked as a stabiliser and **does not help**. On
Reddit it moves β to the opposite bound (+0.50 at Submissions 132, 159 and 212). The residual lasting budget
genuinely sits in the bottom band; the defect is the collinearity of the bandwise decomposition (drift h²,
surge saturation, AR(1), linear), not the scale of the law fit. **No further variant was run.** Fitting the
component rank laws jointly to the whole variogram is recorded as the next-round proposal.

**Gold** (20 seeds Goal 1; 5 origins × M = 39 Goal 2; R6 S2 from `baseline_r6/r6_s2_gold.json`, R6 S3 from
`../astra_v3/candidate_gold_benchmark_results.json`):
- FB identical in both arms. IG S2 identical. IG S3 differs from the astra_v3 comparator in the 4th decimal of
  Goal 2, because that comparator's IG T0 = 22 endpoint fit activated jumps (R7 report: "Only 1/20 S3 endpoint
  fits activated jumps, Instagram at 22 training weeks"). It is therefore not the R6 source at that origin;
  R8 S3 is (drift off at T < 75).
- Goal 2 cell counts over 17 panel-horizon cells, R8 vs R6:

| Score | S2′ better / worse / identical | S3′ better / worse / identical |
|---|---|---|
| X CRPS | 6 / 4 / 7 | 7 / 6 / 4 |
| logR CRPS | 1 / 9 / 7 | 2 / 11 / 4 |
| Kalman MAE | 4 / 6 / 7 | 5 / 8 / 4 |
| Head share MAE | 6 / 4 / 7 | 5 / 8 / 4 |
| Head share correlation | 6 / 4 / 7 | 3 / 10 / 4 |
| Fair head Brier | 3 / 7 / 7 | 4 / 9 / 4 |
| \|cov90 − 0.9\| | 3 closer / 7 farther | 10 closer / 3 farther |
| Kalman beats EWMA | 15/17 | 16/17 |

## 5. Prediction scoring (P1–P6 from section 3)

| ID | Prediction | Outcome | Verdict |
|---|---|---|---|
| P1 | S3′ Comments ≤ 0.120 | 0.087 | PASS |
| P1 | S3′ Submissions ≤ 0.195 | 0.260 | **FAIL** |
| P1 | FB/IG S3 identical | identical | PASS |
| P2 | S2′ FB/IG identical | identical | PASS |
| P2 | S2′ Reddit within ±0.015 | +0.007 / +0.013 | PASS (both worse) |
| P3 | S2′ gapped ρ > +0.05 | +0.088 / +0.103 | PASS |
| P3 | S2′ hook bottom < 0 < top | −0.069…+0.154 / −0.064…+0.182 | PASS |
| P3 | S2′ hook increasing over deciles 1–9 | deciles 1→2 dip (−0.069 → −0.079; −0.064 → −0.080) | PARTIAL |
| P3 | S2′ \|D\| ≤ 0.05 | −0.054 / −0.081 | **FAIL** |
| P3 | S2′ boundary within ±0.05 | 1.002 / 0.987 | PASS |
| P3 | S2′ top-10 within ±0.05 | 0.267 (Δ −0.050) / 0.167 | PASS (Comments at the edge) |
| P4 | S3′ D within ±0.03 | Comments 0.115 (Δ −0.034) | **FAIL** |
| P4 | S3′ D within ±0.03 | Submissions 0.139 (Δ −0.024) | PASS |
| P4 | S3′ hook shape preserved | top-decile dip kept on both | PASS |
| P4 | S3′ Comments top-10 ≤ 0.42 | 0.350 | PASS |
| P4 | S3′ Submissions top-10 in [0.22, 0.35] | 0.200 | **FAIL** |
| P4 | S3′ boundary within ±0.05 | 1.137 / 1.135 | PASS |
| P5 | S2′ Submissions share corr h=52 > +0.30 | +0.37 | PASS |
| P5 | S2′ Submissions share corr h=26 > +0.20 | +0.44 | PASS |
| P5 | S2′ Kalman MAE better at h=26/52 | Comments both better | PASS |
| P5 | S2′ Kalman MAE better at h=26/52 | Submissions 0.3755 → 0.3756, 0.5074 → 0.5104 | **FAIL** |
| P5 | S2′ X CRPS h=1/4 within ±0.003 | max +0.0009 | PASS |
| P5 | S3′ Submissions cov90 −0.003 at h=13–52 | −0.003 / −0.003 / −0.002 | PARTIAL (2/3) |
| P6 | FB/IG Goal 2 identical | FB, IG S2 identical; IG S3 against a non-source comparator | PASS (with provenance note) |

## 6. Decision under the pre-registered rule

- **S3 change (rank-law drift + clustered-SE activation): NOT ADOPTED.** It beats R6 S3 on Comments
  (0.087 vs 0.124, 4/4 drift-active origins) but loses on Submissions (0.260 vs 0.198), and Comments D moves
  by 0.034 (> 0.03).
- **drift_core for S2: NOT ADOPTED.** Gate +0.007 / +0.013 (> +0.005) and logR CRPS worse in 9/10 changed
  Reddit cells. This is despite large Goal 1 structural gains: gapped ρ sign fixed; Submissions head share
  0.577 → 0.436 against data 0.356, with a declining block trajectory like the data; Comments top-10 turnover
  equal to data. Head share-shift correlation at 26/52 weeks also turns positive.
- **Round 6 remains the champion.** The module ships the tested candidate; `drift_core=False,
  drift_rank_law=False` reproduces Round 6 bit for bit (`check_r6_identity.py`, unit test).

Final tests: **29/29 pass** (`logs/final_tests.log`). No file outside `opus_r8/` was written (checked with
`find -newer` against the session start).
