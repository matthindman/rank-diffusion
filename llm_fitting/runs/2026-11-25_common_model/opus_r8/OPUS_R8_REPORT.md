# Round 8 · Persistent drift by rank, and why Round 6 stays the champion

Claude Opus 5.5 · 2026-11-26 · workspace `llm_fitting/runs/2026-11-25_common_model/opus_r8/`

**Recommendation: keep Round 6. Adopt neither of the two Round 8 changes.**

Diagnosis found a real, moment-identified mis-specification in Round 6. Reddit's persistent entity drift is
strongly rank-dependent, while S3 applies one value to every rank. The candidate built on it does not clear
the pre-registered bar. Each arm got one change.
- **S3′ (rank-law drift).** Best Comments Gate v1 of any round (**0.087** against 0.124 for R6 S3 and 0.113
  for R6 S2), but it loses Submissions (0.260 against 0.198).
- **S2′ (drift in the symmetric arm).** Fixes the sign of Reddit persistence and much of the Submissions
  head-share collapse, but loses 0.007 / 0.013 on the Reddit gate.

The Submissions loss is not mainly a failure of the drift idea. It exposes an **identification defect inside
Round 6 S3**: the smooth lasting law swings between its bounds when a few percent of variance moves between
bands. That defect has to be repaired before any S3 change can be judged on Submissions.

| Gate v1 · lower is better | Persistence | R6 S2 | **R8 S2′** | R6 S3 | **R8 S3′** |
|---|---:|---:|---:|---:|---:|
| Facebook Era A | 0.147 | 0.089 | 0.089 (identical) | 0.091 | 0.091 (identical) |
| Instagram apparatus | 0.869 | 0.306 | 0.306 (identical) | 0.304 | 0.304 (identical) |
| Reddit Comments | 0.158 | **0.113** | 0.120 | 0.124 | **0.087** |
| Reddit Submissions | 0.165 | **0.178** | 0.191 | 0.198 | 0.260 |

Facebook and Instagram never activate the new component (net drift moment negative on Facebook, z ≈ −5;
Instagram T < 75). Every fit there is bit-identical to Round 6, and so is every Reddit fit at the first gate
origin (T0 = 53).

---

## 1. What the diagnostics found

All diagnostics read data moments, fitted parameters, or training-window data only. Code and outputs are in
`diag_*.py` / `diag_*.json`; the narrative is in `EXPERIMENT_LOG.md`.

### 1.1 The brief's surge suspicion is not supported at the far tail
- The top 0.5% of increments do carry 57–71% of κ₃ and 79–91% of κ₅ (confirmed).
- A bounded-influence statistic, the tail asymmetry A(q) = P(Δ − med > q·s) − P(Δ − med < −q·s), is positive
  on every panel. On Reddit it peaks at q ≈ 2 and stays positive at q = 6.
- Panels simulated from fitted R6 S3 reproduce the data's **far-tail** A on Reddit (Comments, 13 weeks:
  A(4) 0.0041 vs 0.0042; A(6) 0.0019 vs 0.0018), and the κ₃/κ₅ estimator recovers its own parameters from them.
- The estimator fires on panels simulated from S2. That is not a selection artifact: S2's empirical
  innovation pool keeps the data's positive skew (it is only symmetrised when surges are on).

No change to the surge law is justified by these data. S3's boundary overshoot (1.13–1.16) is not explained
by too much pooled skew.

### 1.2 Persistent drift is rank-dependent; Round 6 applies it uniformly
The Round 6 drift moment (gapped window-change covariance net of passing and surge covariance) was computed
inside the K/16, K/4 and K permanent-rank tiers:

| Full panel | K/16 | K/4 | K | Round 6 (all ranks) |
|---|---:|---:|---:|---:|
| Comments σ_d | 0.0064 | 0.0075 | 0.0104 | 0.0096 |
| Submissions σ_d | 0.0044 | 0.0069 | 0.0103 | 0.0092 |

The head's drift variance is overstated 2–4×. The signal is genuine entity persistence:
- residualising on a within-tier quadratic in rank leaves the covariances unchanged (D9);
- the outer tier is positive in all 8 windows; the head tier changes sign around 0.

### 1.3 The drift moment fires on drift-free panels without a noise rule (D8)
On drift-free planted panels the pooled moment reaches σ_d ≈ 0.003–0.004 from sampling noise alone. Round 6
shares this. The within-tier Atlas-drift explanation was tested and rejected (H²·Var_tier g ≈ 10⁻⁴).

### 1.4 The Reddit top-100 over-moves in sample (D3, D4)
Measured on training weeks only: ladder gaps match, but the median one-week |ΔX| of the top-100 is 12–18% too
large. One identified part is the volatility posterior's shrinkage of calm head entities toward a rank-blind
prior. Its v̂ implies a median |ΔX₁| 5–11% above each entity's own measured median, of which ~0.03–0.04 in log
is correctable. The posterior-mean part is correct Bayesian behaviour under the gate harness's draw convention.
This was **recorded, not implemented**: the gain is small, it would raise RACF1 (already too high on Reddit),
and it changes buffer noise in the opposite direction.

### 1.5 Submissions' long-horizon "upper-tail asymmetry" is a location bias
In R6's strata the head median forecast is too high by −0.016 / −0.050 / −0.109 / −0.193 at 4 / 13 / 26 / 52
weeks, with the same sign at all five origins. The ladder's 2019→20 compression on both Reddit panels is a
one-off shock followed by stability, so a measured-drift ladder correction was **not pursued** (it would
extrapolate a shock).

### 1.6 POST HOC: Round 6 S3's lasting law is not identified (D11)
After surge and drift variance are subtracted bandwise, the NNLS leaves most bands with exactly zero lasting
variance.

| Example | Bandwise lasting variance (×10⁻³) | Fitted β |
|---|---|---|
| Comments T0 = 133, R6 S3 | 0, 0, 0.10, 0, 0, 4.17, 9.27 | +0.500 (bound) |
| Submissions full panel, R6 S3 | 6 of 7 bands zero | 0 (fallback) |

The log-scale law then turns "0 vs 0.0002" into large swings of β:
- at Submissions T0 = 80, a 6% change in drift level moved β from +0.085 to −0.250 and doubled the head
  lasting SD;
- at T0 = 132 it moved the other way (0.000 → +0.312).

A variance-scale law (zeros kept) does not stabilise it: it pins β at +0.50 on late Submissions windows. The
fault is the collinearity of the bandwise decomposition (drift ∝ h², surge saturation, AR(1), linear lasting).

---

## 2. Exact changes (candidate as benchmarked)

One equation set for all panels, with no platform names or switches:

$$X_{i,t} = L_{i,t} + \xi_{i,t} + s_{i,t} + \varepsilon_{i,t}$$

$$L_{i,t+1} = L_{i,t} + g(z_{i,t}) + \sigma_L(L_{i,t})\,\eta^L_{i,t} + B^J_{i,t} J_{i,t} + d_{i,t}$$

$$d_{i,t+1} = \rho_d\, d_{i,t} + \sqrt{1-\rho_d^2}\;\sigma_d(z_{i,t})\,\eta^d_{i,t},\qquad
\sigma_d(z) = \sigma_d\, e^{\,b_d \min(z - z_K,\,0)},\qquad \rho_d = 2^{-1/52}$$

Passing ξ, observation ε, surges s and jumps B·J are exactly Round 6.

**New in Round 8.**
1. **`drift_core=True`.** The drift d is estimated in the symmetric arm as well as in S3, from the same moment
   (net of passing covariance; in S3 also net of surge covariance).
2. **Activation rule.** d is switched on only when the pooled net moment exceeds **2 entity-clustered standard
   errors** (`DRIFT_Z_MIN = 2`; each entity's mean window-product is one draw) and T ≥ 75.
3. **`drift_rank_law=True`.** (σ_d, b_d) come from a weighted line through the positive tier estimates (K/16,
   K/4, K) of log σ_d against min(z̄ − z_K, 0), with delta-method weights (net/SE)². Each tier subtracts the
   passing and surge covariance of its own entities.
4. **Per-rank propagation.** The rank law enters every place drift did before, per band or per entity:
   - variance-conserving variogram subtraction and jump-gap accounting;
   - the robust-fit MC increment law;
   - the simulator (AR innovation scaled at the entity's current rank);
   - the volatility posterior;
   - the 4-state Kalman filter (per-entity process and prior variance).

**Compatibility.** `drift_core=False, drift_rank_law=False` reproduces the Round 6 source bit for bit:
scalars, drift profile, filtered states and simulated panels, both arms (`check_r6_identity.py`; unit test
`test_flags_off_reproduce_round6_source_exactly`). With the defaults on, Facebook was also verified
bit-identical (`logs/check_fb_default_identity.log`).

**Free scalars (full panels).**

| Arm | FB / IG | Reddit |
|---|---|---|
| S2 | 12 / 12 (unchanged) | 12 → **14** (+σ_d, +b_d) |
| S3 | 14 / 14 (unchanged) | 15 → **16** (+b_d) |

No scalar is tuned to an evaluation score. The 2-SE activation threshold and the tier boundaries are declared
constants; ρ_d stays at Round 6's fixed 52-week half-life.

**Planted recovery** (`test_common_rankdiff_opus_r8.py`, **29/29 pass**, 6 new):
- rank-dependent planted drift is detected with σ_d(K) within 35% and the right slope sign;
- rank-uniform drift gives |b_d| < 0.15;
- three drift-free seeds stay off;
- the simulator's stationary drift variance follows σ_d(z)² by rank (within ±25%);
- the 4-state filter tracks planted drift (corr > 0.4).

**Stated limitation.** Tiers by whole-panel permanent rank **attenuate** the slope: planted b_d = 0.6 gives
0.32 / 0.58, and 0.3 gives 0.05–0.12. High-drift risers are counted in the head tier. The estimator is
therefore conservative, shrinking toward Round 6's rank-uniform drift. Window-start tiers did not fix this
reliably (D10).

---

## 3. Results against Round 6 (and Rounds 4, 5, 7)

### 3.1 Gate v1 · 5 × 20, boot 2000

| Panel | R4 S2 | R5 S2 | R7 S2 | R6 S2 | **R8 S2′** | R6 S3 = R4 = R7 S3 | R5 S3 | **R8 S3′** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| FB | 0.084 | 0.093 | 0.083 | 0.089 | 0.089 | 0.091 | 0.107 | 0.091 |
| IG | 0.282 | 0.344 | 0.282 | 0.306 | 0.306 | 0.304 | 0.274 | 0.304 |
| Comments | 0.134 | 0.096 | 0.118 | 0.113 | 0.120 (+0.007 ± 0.031) | 0.124 | 0.130 | **0.087** (−0.037 ± 0.022) |
| Submissions | 0.263 | 0.196 | 0.209 | 0.178 | 0.191 (+0.013 ± 0.030) | 0.198 | 0.266 | 0.260 (+0.062 ± 0.106) |

Comments S3′ improves at **all four** drift-active origins, on exactly the predicted moments:

| Comments S3′ moment error | R6 → R8 |
|---|---|
| dRank13 | 0.27 → 0.18 |
| RACF1 | 0.23 → 0.14 |
| Rank-1 collisions (T0 = 80) | 0.61 → 0.41 |
| Rank-1 collisions (T0 = 106) | 0.88 → 0.28 |

Submissions S3′ loses mainly at two origins:
- T0 = 80 (0.177 → 0.427), where the core law hit its lower bound (§1.6);
- T0 = 159, rank-1 collisions on a 0.077 base rate.

S2′'s Comments loss is concentrated in rank-1 collisions at T0 = 106. The signed error goes from −0.51 to
−0.88: the rank-1 occupant becomes too stable and changes too rarely.

### 3.2 Goal 1 · aggregate structure, full panels, 20 seeds (median)

FB and IG are identical to Round 6 in both arms and are omitted.

| Statistic | Comments: data | R6 S2 | **R8 S2′** | R6 S3 | **R8 S3′** | Submissions: data | R6 S2 | **R8 S2′** | R6 S3 | **R8 S3′** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Top-10 turnover | 0.267 | 0.317 | **0.267** | 0.450 | 0.350 | 0.300 | 0.150 | 0.167 | **0.300** | 0.200 |
| K/4 turnover | 0.162 | 0.124 | 0.142 | 0.147 | **0.159** | 0.162 | 0.146 | 0.159 | 0.177 | 0.169 |
| Head share (last block) | 0.506 | 0.575 | 0.562 | 0.591 | **0.539** | 0.356 | 0.577 | **0.436** | 0.565 | 0.476 |
| Boundary ratio | 1.009 | 1.013 | 1.002 | 1.131 | 1.137 | 1.116 | 0.996 | 0.987 | **1.111** | 1.135 |
| Departure rate | 0.016 | 0.015 | 0.019 | 0.014 | 0.018 | 0.012 | 0.017 | 0.017 | 0.021 | **0.014** |
| Immediate-crossing share | 0.989 | 0.916 | 0.925 | 0.892 | 0.911 | 0.992 | 0.977 | 0.977 | 0.981 | 0.972 |
| Gapped ρ | 0.145 | −0.047 | 0.088 | **0.104** | 0.085 | 0.155 | −0.041 | 0.103 | 0.088 | **0.143** |
| Asymmetry D | 0.132 | −0.021 | −0.054 | 0.149 | 0.115 | 0.222 | −0.041 | −0.081 | **0.163** | 0.139 |
| p_fall / p_rise | 0.158 / 0.026 | −0.06 / −0.04 | 0.05 / 0.10 | 0.135 / −0.016 | 0.109 / −0.008 | 0.240 / 0.018 | −0.06 / −0.03 | 0.05 / 0.13 | 0.123 / −0.036 | 0.149 / 0.010 |
| Decline runs r2 / r3 | 1.28 / 1.43 | 1.03 / 1.05 | 1.02 / 1.02 | 1.12 / 1.18 | **1.14 / 1.20** | 1.25 / 1.32 | 1.06 / 1.08 | 1.04 / 1.04 | 1.13 / 1.19 | **1.17 / 1.24** |

**Head share by block (four yearly blocks).**

| Series | Comments | Submissions |
|---|---|---|
| Data | 0.614 0.573 0.529 0.506 | 0.511 0.507 0.427 0.356 |
| R6 S2 | 0.600 0.588 0.573 0.575 | 0.494 0.522 0.548 **0.577** (rises) |
| R8 S2′ | 0.592 0.585 0.580 0.562 | 0.495 0.487 0.461 **0.436** (falls, like the data) |
| R6 S3 | 0.588 0.556 0.580 0.591 | 0.509 0.524 0.544 0.565 |
| R8 S3′ | 0.585 0.568 0.545 **0.539** (falls) | 0.486 0.483 0.481 0.476 |

**10-decile hook E[y | x decile].** Momentum rises through deciles 1–9, then the top decile falls back.

| Series | Comments | Submissions |
|---|---|---|
| Data | −0.216 … +0.107, +0.049 | −0.265 … +0.112, +0.033 |
| R6 S2 | +0.084 … −0.058 (wrong sign) | +0.079 … −0.035 (wrong sign) |
| R8 S2′ | −0.069 … +0.083, +0.154 (right sign, no top reversal) | −0.064 … +0.094, +0.182 (right sign, no top reversal) |
| R6 S3 | −0.188 … +0.142, −0.025 | −0.164 … +0.122, −0.056 |
| R8 S3′ | −0.157 … +0.125, −0.013 (shape kept) | −0.188 … +0.164, +0.015 (bottom closer, shape kept) |

**Out of period** (fit 2019–20, simulate 2021–22; data 2021–22 in brackets).
- S2′ gapped ρ: Comments −0.077 → +0.008 [0.124]; Submissions −0.028 → +0.065 [0.102].
- S3′ top-10 turnover: 0.400 → 0.300 on both panels [0.300 both].
- S3′ D on Submissions: 0.200 → 0.148 [0.133].
- Neither arm forecasts the 2021–22 head-share fall from 2019–20 data (Submissions S2′ 0.582, S3′ 0.498 vs
  data 0.356).
- S3 boundary stays too high (1.21 / 1.33 vs 0.95 / 0.88).

### 3.3 Goal 2 · M = 39 endpoints, exact [x₍₂₎, x₍₃₈₎] coverage (reference 0.900, PIT SD 0.289)

Over the 17 panel-horizon cells (7 FB/IG cells identical for S2; FB identical for S3):

| R8 vs R6 | S2′ better / worse | S3′ better / worse |
|---|---|---|
| X CRPS | 6 / 4 | 7 / 6 |
| logR CRPS | 1 / 9 | 2 / 11 |
| Kalman MAE | 4 / 6 | 5 / 8 |
| Head share-shift MAE | 6 / 4 | 5 / 8 |
| Head share-shift correlation | 6 / 4 | 3 / 10 |
| Fair head Brier | 3 / 7 | 4 / 9 |
| \|cov90 − 0.900\| | 3 closer / 7 farther | 10 closer / 3 farther |
| Kalman MAE beats train-tuned EWMA | 15/17 | 16/17 |

Selected Reddit cells:

| Cell | R6 | R8 |
|---|---:|---:|
| S2′ X CRPS, Comments h = 52 | 0.5018 | **0.4912** |
| S2′ X CRPS, Submissions h = 26 | 0.2823 | **0.2794** |
| S2′ X CRPS, Submissions h = 52 | 0.3808 | **0.3777** |
| S2′ head share-shift corr, Submissions h = 13 / 26 / 52 | −0.03 / −0.25 / −0.44 | **+0.35 / +0.44 / +0.37** |
| S2′ logR CRPS, Submissions h = 52 | 0.1962 | 0.1989 |
| S2′ logR CRPS, Comments h = 52 | 0.1871 | 0.1905 |
| S2′ cov90, Comments h = 52 | 0.900 | 0.915 |
| S2′ fair head Brier, Submissions h = 52 | 0.0722 | 0.0758 |
| S3′ X CRPS, Comments h = 26 / 52 | 0.3936 / 0.4971 | 0.3922 / 0.4951 |
| S3′ cov90, Submissions h = 13 / 26 / 52 | 0.941 / 0.936 / 0.941 | 0.938 / 0.933 / 0.939 |
| S3′ PIT SD, Submissions | 0.24–0.25 | 0.24–0.25 (unchanged) |

Short horizons (h = 1, 4) move by less than 0.001 in X CRPS. Instagram S3's 4th-decimal Goal 2 differences
reflect the comparator: the astra_v3 Gold activated jumps at the IG T0 = 22 origin, whereas R8 S3 is the
Round 6 source there.

### 3.4 Goal 3 · parsimony
Both changes are moment-identified and zero out cleanly on Facebook and Instagram (exactly Round 6 there).
The S2 arm grows from 12 to 14 scalars on Reddit, and the result does not earn the extra two on the adoption
gate. S3′'s one extra scalar earns its keep on Comments but not on Submissions.

---

## 4. Predictions scored (written before the run; `EXPERIMENT_LOG.md` §3, §5)

**18 pass, 2 partial, 5 fail.**

| Prediction | Outcome |
|---|---|
| S3′ Comments ≤ 0.120 | **0.087** (pass) |
| S3′ Submissions ≤ 0.195 | **0.260 (fail)** |
| S2′ Reddit gate within ±0.015 | +0.007 / +0.013, both worse (pass) |
| S2′ gapped ρ > 0.05 | 0.088 / 0.103 (pass) |
| S2′ \|D\| ≤ 0.05 | −0.054 / −0.081 **(fail)** |
| S3′ D within ±0.03 | Comments −0.034 **(fail)**; Submissions −0.024 (pass) |
| S3′ Comments top-10 ≤ 0.42 | 0.350 (pass) |
| S3′ Submissions top-10 in [0.22, 0.35] | 0.200 **(fail)** |
| S2′ Submissions head share-shift corr > 0.30 at 52 weeks | +0.37 (pass) |
| S2′ Kalman MAE better at 26/52 weeks | Comments pass; Submissions **fail** (0.5074 → 0.5104) |
| S3′ Submissions coverage −0.003 at h = 13–52 | 2 of 3 cells (partial) |

---

## 5. Limitations and regressions
- **The Submissions S3′ regression is real** (0.198 → 0.260 on the adoption gate). Part of it is attributable
  to the Round 6 lasting-law instability (§1.6), but that attribution is a post-hoc diagnosis, not a
  controlled test.
- **The Comments S3′ gain may partly reflect the same instability.** The improvement is consistent across
  4/4 origins and on the predicted moments, but the core law also moved at those windows (β 0.17–0.50 in R6
  vs 0.06–0.16 in R8). Mechanism and estimator noise cannot be fully separated.
- **S2′ trades the adoption gate and logR / Brier for structure.** It worsens the Reddit gate, logR CRPS
  (9/10 changed Reddit cells), fair head Brier and 52-week coverage. It still lacks the asymmetry channel
  (D more negative than R6 S2), so it is not a substitute for S3.
- **The tiered rank law is attenuated** by selection (§2) and noisy at the head tier (net/SE from −0.5 to
  5 at gate windows).
- **Unresolved and not addressed:** Instagram S3's wrong-sign D (+0.082 vs −0.087; surges reproduce IG's
  positive skew but not its reverting dips); S3's boundary overshoot on FB/IG/Comments; the
  volatility-posterior head bias (§1.4).
- **Data scope.** All evidence is DEVELOPMENT data, and several diagnostics read the full panels. Nothing
  here is held-out confirmation.

## 6. Recommendation and next step
1. **Reject both changes for adoption. Keep Round 6** (S2 12 scalars; S3 14–15) as the champion going into
   pre-registered held-out evaluation.
2. **Record the two findings that should shape the held-out protocol and any later round:**
   - Reddit's persistent drift falls toward the head (tier σ_d ratio head/outer ≈ 0.3–0.6; outer-tier z
     up to 16);
   - Round 6 S3's smooth lasting law is not identified on Reddit (most bands exactly zero after subtraction;
     β swings bound to bound). S3 results on Submissions should therefore be read as fragile.
3. **Proposed, not adopted:** re-estimate S3 by fitting the component **rank laws jointly** to the whole
   bandwise variogram (lasting σ_L(L), passing V_ξ(z), drift σ_d(z), surge scale), rather than bandwise NNLS
   followed by separate log-linear laws. Re-test the drift rank law only inside that estimator, under a new
   pre-registration.

## 7. Files and reproduction

| File | Content |
|---|---|
| `common_rankdiff_opus_r8.py` | Candidate as benchmarked (defaults `drift_core=True`, `drift_rank_law=True`; both False = Round 6) |
| `test_common_rankdiff_opus_r8.py` | 29 tests; `logs/final_tests.log` |
| `candidate_frozen_gate_results.json`, `candidate_gold_benchmark_results.json` | Copies of `runs/v1/` outputs (snapshot byte-identical to the module) |
| `COMPARISON_TABLES.md` | Full side-by-side tables (`compare_r8.py v1`) |
| `EXPERIMENT_LOG.md` | Diagnostics D1–D11, predictions, scoring, decision |
| `diag_*.py` / `diag_*.json`, `logs/` | Every diagnostic |
| `check_r6_identity.py` | Round 6 compatibility check |
| `baseline_r6/common_rankdiff_r6_source_copy.py` | Untouched Round 6 source |

```bash
cd llm_fitting/runs/2026-11-25_common_model/opus_r8
zsh run_benchmark.sh v1 both                    # exact Gate v1 + Gold via ../benchmark_candidate.py
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 compare_r8.py v1 > COMPARISON_TABLES.md
PYTHONDONTWRITEBYTECODE=1 /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m pytest -q -p no:cacheprovider test_common_rankdiff_opus_r8.py
```

**Data contact.** Only the four DEVELOPMENT caches and `facebook_a_spec_b.json`; no sealed or
exposure-unknown file was opened.

**Writes.** Only inside `opus_r8/`, verified with `find -newer` against the session start. Heavy jobs ran
one at a time under the swap watchdog; it never fired.
