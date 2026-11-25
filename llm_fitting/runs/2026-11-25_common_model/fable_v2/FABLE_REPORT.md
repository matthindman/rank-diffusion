# Fable 5.1 development pass on the common rank-diffusion model (Round 5)

Claude Fable 5.1 (Anthropic) · 2026-11-25 · DEVELOPMENT evidence only · nothing adopted, pushed, or changed outside `fable_v2/`.
Starting point: byte-identical copy of `llm_fitting/common_rankdiff.py` (Round 4 = Round 3 process + Astra's adopted
`_volatility_posterior`). Interpreter: registered Python 3.11 (pandas 2.2.2, numpy 2.0.1, scipy 1.14.0). Data contact:
the four anonymous DEVELOPMENT `.npz` caches and `facebook_a_spec_b.json` only. Round 4 baseline numbers are Astra's
archives, verified bit-identical to this machine on Facebook (`round4_check/`, EXPERIMENT_LOG §D0).

Companion files: `EXPERIMENT_LOG.md` (every hypothesis, prediction, result, and rejected variant), `diag_*.py/.log/.json`
(diagnostics), `trackA_gate/` (robust-core-only gate and structure), `final/` (full benchmark log, test log, module snapshot),
`candidate_frozen_gate_results.json`, `candidate_gold_benchmark_results.json`, `test_common_rankdiff_fable.py` (23 tests).

---

## 1. Executive verdict and adoption recommendation

**Partial accept, as a development option for the owner's decision. Recommend S2+J (12 scalars) as the new core
candidate; keep Round 4's S3 (variance-fit surge + drift) as the asymmetry comparator for now.** Nothing is adopted here.

What this pass found. The mean-square variogram that identifies the lasting step is dominated by rare permanent jumps
(30–55% of lasting variance in the body of every ladder, ≈ 0 at the Facebook head). Loading that variance onto a
diffusive step makes every typical entity move 15–40% too much — the common root of the Submissions gate overshoot,
the Reddit/Facebook over-dispersion (cov90 > 0.90, PIT SD < 0.289) and the compensating shrinkage Astra's median filter
had been performing. Matching typical movement alone (Track A) fixes individual forecasts and Submissions but halves
departure rates and worsens the Facebook/Comments gate, because rank displacement and boundary crossings are collective
statistics driven by neighbours' jumps. The candidate therefore pairs a robust (IQR-matched) diffusive core with an
explicit rare-jump process whose variance is the measured gap between the two variograms (λ_J fixed at 0.01/week from
the D1 diagnostic; rank attenuation inherited from b_ξ). S2: 11 → 12 scalars; S3: unchanged 14/15.

Scorecard against Round 4 (full tables in §5; paired across the same splits, seeds and cohorts):

| | Facebook | Instagram | Comments | Submissions |
|---|---|---|---|---|
| Gate v1 S2 (R4 → cand) | 0.084 → 0.093 (±0.031; 5/5 wins vs persistence, R4 4/5; paired +0.009 ± 0.030) | 0.282 → 0.344 (dRank1/4 now 15.0/17.6 vs emp 15.4/18.6, R4 17.2/19.4; the loss is RACF1 0.069 vs emp 0.019 at the 0.02 moment floor) | **0.134 → 0.096** (±0.028; 5/5 wins; best S2 on record, below Model A 0.123) | **0.263 → 0.196** (±0.103; 3/5 wins; Track A alone 0.167) |
| Gate v1 S3 (R4 → cand) | 0.091 → 0.107 | 0.304 → 0.274 | 0.124 → 0.130 | 0.198 → 0.266 (one split blows up) |
| Goal 2 S2 X CRPS | better at 4/4 horizons | back to Round 3 level (Astra's +0.0014–0.0020 cost removed) | better at 4/5, tie at 52w | tie at 1–13w, worse at 26/52w |
| Goal 2 S2 cov90 / PIT SD | 0.865–0.880 / 0.286–0.296: now under-dispersed | 0.874–0.876 / 0.293–0.295: under | **0.880–0.896 / 0.282–0.290**: calibrated (R4 0.902–0.917 / 0.264–0.279) | **0.905, 0.916** at 1w, 4w (R4 0.924, 0.928); 0.93–0.95 at 13–52w |
| Goal 2 S2 head share shift | MAE better at 13w/26w, tie 1w/4w; beats zero-shift and EWMA 4/4 | MAE better 3/3; 13w corr 0.63 (R4 0.58) | MAE better 5/5; beats EWMA 5/5 | MAE better 1w/13w/52w (805 vs 851 ×1e-6); corr 0.31–0.58 (R4 −0.02–0.45) |
| Goal 1 S2 (R3 → cand, 20 seeds) | departure rate 0.044 → 0.035 (emp 0.033); rank-1 change 0.459 → 0.506 (0.518); max X 17.27 → 17.07 (16.86); **top-10 turnover 0.35 → 0.45 (0.35)**; boundary 0.998 → 1.069 (0.98) | rank-1 change 0.814 → 0.539 (0.392); head share 0.392 → 0.422 (0.419); K/16 turnover 0.208 → 0.236 (0.313); boundary 0.978 → 1.016 (1.00) | departure rate 0.018 → 0.017 (0.016); head share 0.590 → 0.574 (0.506); K/16 0.151 → 0.166 (0.187); boundary 1.001 → 1.058 (1.009) | K/4 turnover 0.141 → 0.155 (0.162); rank-1 0.047 → 0.066 (0.057); **top-10 0.233 → 0.167 (0.300)**; boundary 0.986 → 1.046 (1.116) |
| Goal 1 S3 out-of-moment (R3 → cand) | D +0.044 → +0.080 (emp +0.031) | head rank-1 change 0.755 → 0.480 (0.392); head share 0.396 → 0.408 (0.419) | **ρ 0.104 → 0.159 (0.145); D 0.149 → 0.207 (0.132)**; hook kept | **ρ 0.088 → 0.152 (0.155); D 0.163 → 0.226 (0.222)**; 52w head-shift corr 0.80 kept |

Why "partial" and not "accept": the S2 gains are concentrated on Reddit and on calibration; Facebook pays in coverage
(0.87 vs 0.90) and head turnover, Instagram's gate number is a near-zero-moment artifact that still counts, the jump
rate is a convention rather than an estimate, and the inherited rank law places jumps at the Facebook head where the
data have none (§6). S3 on the robust core trades Goal 2 and gate for out-of-moment structure (Reddit ρ and D now exact,
Instagram head fixed) and is not a clean improvement; its surge size law is the open problem the brief called the big
prize, and κ₅ re-pooling does not solve it (§2.4). Ties go to the simpler model: on Facebook the S2 gate is a tie within
one split SD, so the recommendation rests on Comments, Submissions, the calibration columns and the mechanism evidence,
not on Facebook.

What the owner would be deciding: (i) whether the robust-IQR scale moment replaces the mean-square moment as the
identifying moment of the core (zero scalars, large interpretive change: the model describes typical dynamics and
carries the tail as a separate measured process); (ii) whether a cross-platform fixed jump rate is acceptable where a
per-panel estimate is not identified; (iii) whether S3 stays on the variance fit. The λ_J sensitivity (§5.5) shows the
convention matters for head structure: 0.02/week gives boundary ratios of 0.99–1.04 and the right Facebook top-10
turnover, 0.005 is clearly worse; I did not switch because that would select the constant on Goal 1 scores.

---

## 2. Mathematical thinking and root-cause analysis

### 2.1 The diagnostic that reorganized the pass (D1)

For every panel, every inside-K permanent-rank band and every horizon h = 1..52, I compared three scales of the
within-band, within-week demeaned h-week increments — exactly the objects the structure-function estimator uses —
between the data and a simulated Round 4 S2 world fitted to it: the mean square (what the NNLS variogram matches),
the robust scale (IQR/1.349)², and the excess kurtosis. Both sides go through one code path.

The simulated world matches the mean square by construction. Its robust scale is far too large. The empirical ratio
var/robust-var is 1.5–4.3 and essentially flat in h through h ≈ 13 (Submissions band 0: 2.03, 2.05, 1.96, 1.96, 1.95
for h = 1, 2, 4, 8, 13; 1.86 at 26; 1.46 at 52). The simulated ratio starts at 1.4–1.7 and decays to 1.0–1.1 by h = 52,
as the central-limit theorem demands of any i.i.d. innovation. Consequently the simulated *median* h-week movement of
the head band exceeds the data's by a factor 1/0.88–1/0.97 on Facebook, 1/0.82–1/0.87 on Instagram, 1/0.77–1/0.85 on
Comments and 1/0.71–1/0.79 on Submissions. At h = 13 on Submissions the factor is 1/0.72 = 1.39, which is the gate's
dRank13 overshoot (10.6 vs 7.4 = 1.43) almost exactly.

A ratio that is flat in h is the signature of **rare permanent jumps** (compound Poisson at a rate λ with λh ≪ 1 over
the plateau), not of a fat-tailed weekly innovation: for i.i.d. fat tails the ratio decays like the kurtosis, ~1/h. The
bounded, per-entity-standardized innovation pool — introduced to stop head-share blow-ups — removes exactly this tail,
and the mean-square fit then loads the jump variance (30–55% of lasting variance in the body of every ladder; ≈ 0 in
the Facebook head band, comparable to the body in the Reddit head bands) onto the diffusive lasting step. Everything the
brief listed as open follows: Submissions dRank1/4/13 too high, cov90 above 0.90 and PIT SD below 0.289 on Reddit and
Facebook, top-10 turnover too low on Submissions and Instagram (set changes come from jumps), and the small Facebook
under-prediction after Round 4 (the median-based filter was compensating at the entity level: D2 shows v̂ = 1 raises
dRank1 by 1–2 ranks).

### 2.2 Why the gate cannot be fixed by matching typical movement alone (Track A)

Track A re-estimates (σ_L, V_ξ, σ_obs) so that the model's IQR of h-week increments equals the data's in every band,
with the model's own non-Gaussian increment law supplying the IQR/variance ratio at the current parameters (a Monte-Carlo
fixed point; the constant 1.349 cancels, so the moment is literally "model IQR = data IQR"). Zero new scalars. Two
measurements shaped its form: the robust profile over all h is convex at long lags (jumps entering the IQR, and on
Reddit the S3 drift's h² term), which non-negative NNLS on {constant, saturating, linear} columns can only fit by
zeroing the passing column; and thirteen lags cannot separate an AR(1) from a random walk (the short-lag ridge recorded
in the pitfalls file; Instagram φ fell to 0.15). The working split is therefore: φ from the full-horizon mean-square
profile (unchanged), the three scale components per band from the robust profile over h ≤ 13 with φ pinned.

Track A did what it promised for individual endpoints — Submissions S2 gate 0.263 → 0.167 (better on 5/5 splits, at par
with historical mobility), Instagram 0.282 → 0.271 with 100% median consistency — and failed where the model is read as
a population: Facebook 0.084 → 0.100, Comments 0.134 → 0.177; departure rates fell to roughly half of empirical on every
panel (FB 0.044 → 0.023 vs 0.033; Comments 0.018 → 0.006 vs 0.016), K/4 turnover and rank-1 occupant change fell too.
The reason is structural. **Rank displacement, boundary crossings and set turnover are collective statistics**: an
entity is displaced when neighbours cross it, and a rare jumper crosses many ranks. Round 4 got those roughly right by
moving every typical entity too much; Track A gets the typical entity right and loses the crossings. Neither is
adoptable alone. The jump variance has to be put back where it belongs — as a rare, permanent, rank-attenuated event
process on top of a robust diffusive core.

### 2.3 The S2+J model and how each quantity is identified

Lasting step: L_{t+1} = L_t + g(r) + σ_d(L) η_t + B_t J_t with B_t ~ Bernoulli(λ_J) and J_t Laplace with
Var(J | z) = c_J · s_sc(z)², where s_sc is the rank attenuation already used by the Stage 3 surge (inherited from the
passing slope b_ξ; zero scalars). Jumps enter the simulator, the Kalman transition variance (q_L += λ_J Var(J | z)),
the volatility posterior's 1-week increment law, the Monte-Carlo IQR/variance ratio, and the closed-form Atlas drift
(which balances the total lasting variance). With jumps inactive every random stream is untouched.

- σ_d(L), V_ξ(z), σ_obs(z): the robust (IQR) variogram over h ≤ 13, φ pinned — declared moment "model IQR = data IQR".
- φ: the full-horizon mean-square variogram, exactly as in Rounds 3–4.
- c_J (one counted scalar): the band-weighted lasting-variance gap between the mean-square fit and the jump-free
  robust core, Σ_b w_b (s²_L,var,b − s²_L,rob,b) = λ_J c_J Σ_b w_b s_sc(z_b)². A declared moment with a transparent
  meaning: the share of lasting variance carried by rare events.
- λ_J = 0.01/week: a cross-platform constant fixed from D1 (the ratio has fallen 15–30% from its plateau by h = 52 on
  all four panels, i.e. λ·52 ≈ 0.3–0.7). It has the same status as the drift half-life fixed at 52 weeks from the
  multi-gap decay diagnostic. Per-panel profiling of λ on the long-horizon IQR profile was built and tested in five
  variants (EXPERIMENT_LOG §4) and is not reliably identified: the selected rate swung between grid edges under minor
  estimator changes, and the accompanying fit criterion fired on jump-free planted panels while missing planted jumps.
- Activation: jumps are instantiated when the gap carries ≥ 20% of total weekly lasting variance (development panels
  0.39–0.98; planted jump-free panels with degenerate head bands 0.15–0.17; a planted 64%-share panel 0.24). Below that
  the model is the 11-scalar robust core.
- S3 keeps its surge and drift on the robust core and does not take symmetric jumps: its tail process is the surge, and
  that keeps it at ≤ 15 scalars.

No evaluation score entered any of these choices. The horizon split (13), the jump rate, and the activation share are
conventions set from the D1 diagnostic and planted panels; their values and the evidence are in EXPERIMENT_LOG.

### 2.4 What the surge diagnostics said (S3, D4)

The pooled κ₅ that sizes the surge jump is 79–91% contributed by the top 0.5% of increments on Facebook, Comments and
Submissions. Re-pooling it (dropping the worst 0.5% of entities; averaging nine rank × volatility strata; entity
standardization) lowers the implied jump size μ by only 6–26% (Submissions 1.23 → 0.91), so a capped single arrival
still adds 4.5–7 log units at rank K. κ₅ is not a usable size moment on these panels; the S3 head overshoot has to be
addressed through the amplitude law or a bounded jump law, not by re-pooling. I did not pursue a new S3 surge
mechanism in this pass; the robust core changes S3's structure on its own (§5).

---

## 3. Parameter count and complexity accounting

| Arm | Round 4 | Round 5 candidate | Notes |
|---|---:|---:|---|
| S2 core (asymmetry=False) | 11 | **12** when jumps active (all four panels); 11 when the gap share is < 20% | +1 = c_J (jump variance), from the lasting-variance gap moment. λ_J fixed; rank law inherited. |
| S3 comparator (asymmetry=True) | 14 (FB, IG) / 15 (Reddit) | **14 / 15** (unchanged) | robust core + surge (3) + drift (1 on Reddit); no symmetric jumps. |

Fixed constants added (not fitted): ROBUST_H_MAX = 13, JUMP_LAM_FIXED = 0.01, JUMP_MIN_SHARE = 0.20. Removed freedom: none.
Non-free empirical inputs unchanged (ladder, bottom pool, bounded innovation pool, drift knots). The estimator is more
elaborate (a Monte-Carlo ratio inside a fixed point) but not more parameterized. `scale_moment="variance"` reproduces
Round 4 bit-for-bit (tested against `llm_fitting/common_rankdiff.py` on planted data and verified on all eight
real-panel fits).

---

## 4. What I tried and rejected (every item with its numbers)

| Hypothesis / variant | Measurement | Disposition |
|---|---|---|
| Robust variogram fit over all h with free φ (A1) | Submissions S2 V_ξ → 0 (a_ξ = −6.5), FB φ 0.84 → 0.93 | Degenerate: convex long-h robust profile vs non-negative NNLS. Rejected. |
| Robust fit over h ≤ 13 with free φ (A2) | IG φ 0.93 → 0.15, FB 0.84 → 0.72, Subs 0.93 → 0.45 | φ unidentified on 13 lags (short-lag ridge). Rejected. |
| Robust core alone, φ pinned (Track A, kept as a component) | Gate S2: FB .084→.100, IG .282→.271, Comments .134→.177, Subs .263→.167; departure rates halve everywhere | Right for the typical endpoint, wrong for collective statistics. Not adoptable alone. |
| Jump variance re-estimated from the moving robust core inside the fixed point (J-v1) | FB core → 0 in 5/7 bands, λ → grid edge | Runaway (law misallocation feeds the gap). Rejected. |
| λ_J profiled on the all-horizon IQR profile (J-v2) | λ = .02 FB, .005 Comments, .04 Subs/IG; planted jump-free panels activated; planted variance recovered at 42% | Unstable identification; kept only its fixed-gap structure. |
| λ_J by dimensionless log var/IQR profile with damped gap iteration (J-v3) | FB profile 45/10/26/10/14 (erratic); Comments core eroded to σ_L0 .033 | Rejected. |
| IQR-level SSE on h > 13 with one damped gap update (J-v4) | Comments core 0 in all bands; Subs flips to no jumps | Rejected. |
| Fixed λ_J + "25% fit-gain" activation rule (J-v5) | Misses a planted λ = .01 panel (gain 2%); at λ = .01 would reject jumps on both Reddit panels; fires on small-band jump-free panels | Rejected; activation by gap share only. |
| κ₅ re-pooling to tame the surge size (D4) | μ down 6–26% only; κ₅ 79–91% from top 0.5% of increments | Not a usable size moment. Parked. |
| Trimmed-mean / L1 filter statistic (D3) | Planted T=52: log-scale error median .291 / trim10 .225 / mean .194 clean; .454 / .395 / .410 contaminated | Promising and NOT adopted (one change at a time). Next cheapest step for the IG X-CRPS cost. |
| Planted misspecification checks | γ_L = 0.5 planted (model fixes 0): robust β biased +0.10, variance β unbiased. Rank-uniform planted jumps vs inherited attenuation: core law biased low, total lasting variance within 30% | Documented in tests; limitations carried into §6. |

---

## 5. Full before-vs-after results across all four panels

Sources: `candidate_frozen_gate_results.json`, `candidate_gold_benchmark_results.json` (this folder; `final/benchmark_full.log`),
compared with `../round3_frozen_gate_results.json`, `../round3_gold_benchmark_results.json` (Round 3), and Astra's
`candidate_*` (Round 4 S2) and `diag_ablation.json` filter-only slots (Round 4 S3). Round 4 unconditional structure equals
Round 3's because the filter does not touch it. Gate: 5 splits × 20 reps, 2,000 bootstrap draws. Goal 1: 20 paired seeds.
Goal 2: 39 members, 5 rolling origins, one truth-defined cohort. Everything below is the frozen harness's own output; the
tables were generated by `diag_report_tables.py` (`final/report_tables.md`).

Reading guide, by panel:
- **Facebook.** Gate S2 is a tie with Round 4 within one split SD (paired +0.009 ± 0.030) but now beats historical
  mobility on all five splits; dRank13 is back at 27.4 (Track A 26.0, emp 29.6) while dRank1 stays one rank low
  (12.2 vs 13.6) — the one-week passing tail is still too light. X CRPS, Kalman MAE and head Brier improve at every
  horizon; cov90 falls to 0.865–0.880 (under-dispersed) with PIT SD at 0.286–0.296. Structure: departure rate exact,
  rank-1 change and max X right; top-10 turnover overshoots (0.45 vs 0.35) and the boundary ratio sits at 1.07 — the
  inherited rank law puts jumps at a head that has none.
- **Instagram.** dRank1/dRank4 are now within one rank of empirical (15.0/17.6 vs 15.4/18.6; Round 4 17.2/19.4) and coll1
  is right; the gate number worsens only through RACF1 (0.069 vs an empirical 0.019 sitting at the 0.02 moment floor,
  eligible on 3 splits with relative errors 1–3.5). X CRPS recovers Round 3's level, removing Astra's cost; cov90 0.875
  (under). Structure improves broadly: rank-1 change 0.539 (emp 0.392; Round 3 0.814), head share 0.422 (0.419), K/16
  turnover 0.236 (0.313; 0.208), max X 18.4 (17.6; 19.0).
- **Comments.** The clearest win. Gate S2 0.096 ± 0.028 with 5/5 wins (Round 4 0.134, Round 3 0.129, Model A 0.123);
  dRank1/4/13 6.8/8.2/12.0 vs emp 6.0/8.6/12.2. X CRPS better at 1–26w, logR CRPS better at all five horizons, head Brier
  better at 4/5, head share-shift MAE better at all five and beats EWMA 5/5; cov90 0.880–0.896 and PIT SD 0.282–0.290 are
  calibrated for the first time (Round 4 0.902–0.917 / 0.264–0.279). Structure: departure rate 0.017 (emp 0.016), head
  share 0.574 (0.506; Round 3 0.590), boundary 1.058 (worse than 1.001).
- **Submissions.** Gate S2 0.263 → 0.196 (3/5 wins; 60% consistency); dRank1/4/13 4.2/5.8/9.8 vs emp 3.6/4.8/7.4 (Round 4
  4.2/6.4/10.6) — the jumps give back part of Track A's 0.167 because the inherited law places too much jump variance in
  the head bands relative to the data. Short-horizon calibration is fixed (cov90 0.905 and 0.916 at 1w and 4w; PIT SD
  0.276, 0.267) while 26–52w stay over-dispersed and 52w X CRPS is worse (0.386 vs 0.379). Head share-shift correlations
  rise at every horizon (52w 0.58 vs 0.45). Structure: K/4 turnover 0.155 (0.162), rank-1 change 0.066 (0.057), boundary
  1.046 (empirical 1.116, so closer than Round 3's 0.986); top-10 turnover falls to 0.167 (0.300).
- **S3 (robust core + surge + drift, 14/15 scalars).** Out of moment, Reddit's gapped ρ and D are now matched (Comments ρ
  0.159 vs 0.145, Submissions ρ 0.152 vs 0.155, D 0.226 vs 0.222), the decile hook survives, the Instagram head is fixed
  (rank-1 change 0.48 vs 0.39), and Submissions Goal 2 improves at every horizon with the 52w head-shift correlation at
  0.80. But the gate is worse than Round 4 on three panels, Comments Goal 2 worsens at every horizon (X CRPS 0.219 vs
  0.213 at 1w; cov90 0.85–0.92), and in the out-of-period Comments test the immediate-crossing share collapses to 0.70.
  The robust core shows that the surge plus drift already absorb all lasting variance in the Reddit head bands (core
  σ_L ≈ 0 there); S3 needs its surge size law repaired before the robust core helps it. Keep Round 4's S3.
- **Out-of-period (2019–20 → 2021–22).** S2+J predicts Comments 2021–22 top-10 turnover 0.300 (emp 0.300; Round 3 0.200),
  K/4 0.140 (0.153), head share 0.548 (0.506; Round 3 0.561), departure rate 0.010 (0.007; 0.013). On Submissions it is
  worse on K/4 (0.208 vs 0.151), boundary (1.16 vs 0.88) and departures (0.018 vs 0.005). S3 reproduces Comments' forward
  D (+0.127 vs +0.123; Round 4 +0.054) but loses ρ and departures.

### 5.1 Exact frozen Gate v1 (5 splits × 20 reps, boot 2000): mean rel. error ± split SD · median-consistency · wins vs persistence

| Panel | Persistence | R3 S2 | R4 S2 | **Cand S2** | paired ΔS2 vs R4 | R3 S3 | R4 S3 | **Cand S3** | paired ΔS3 vs R4 |
|---|---|---|---|---|---|---|---|---|---|
| FB | 0.147 ± 0.030 | 0.105 ± 0.043 · 60% · 4/5 | 0.084 ± 0.037 · 80% · 4/5 | **0.093 ± 0.031 · 60% · 5/5** | +0.009 ± 0.030 (1/5 better) | 0.108 ± 0.023 · 60% · 4/5 | 0.091 ± 0.016 · 80% · 5/5 | **0.107 ± 0.032 · 60% · 5/5** | +0.016 ± 0.026 (2/5 better) |
| IG | 0.869 ± 0.554 | 0.296 ± 0.216 · 80% · 4/5 | 0.282 ± 0.219 · 80% · 4/5 | **0.344 ± 0.216 · 100% · 5/5** | +0.062 ± 0.104 (2/5 better) | 0.360 ± 0.276 · 80% · 4/5 | 0.304 ± 0.230 · 80% · 4/5 | **0.274 ± 0.229 · 100% · 5/5** | -0.030 ± 0.062 (3/5 better) |
| Comments | 0.158 ± 0.038 | 0.129 ± 0.071 · 80% · 3/5 | 0.134 ± 0.085 · 80% · 3/5 | **0.096 ± 0.028 · 80% · 5/5** | -0.037 ± 0.071 (3/5 better) | 0.098 ± 0.033 · 100% · 5/5 | 0.124 ± 0.057 · 100% · 4/5 | **0.130 ± 0.057 · 100% · 4/5** | +0.006 ± 0.023 (1/5 better) |
| Submissions | 0.165 ± 0.044 | 0.346 ± 0.064 · 0% · 0/5 | 0.263 ± 0.067 · 60% · 0/5 | **0.196 ± 0.103 · 60% · 3/5** | -0.068 ± 0.074 (4/5 better) | 0.310 ± 0.125 · 20% · 0/5 | 0.198 ± 0.079 · 100% · 3/5 | **0.266 ± 0.241 · 60% · 3/5** | +0.068 ± 0.181 (2/5 better) |

Mean gate moments across the five origins (emp · historical · R4 S2 · Cand S2 · R4 S3 · Cand S3):

| Panel | moment | emp | hist | R4 S2 | Cand S2 | R4 S3 | Cand S3 |
|---|---|---|---|---|---|---|---|
| FB | dRank1 | 13.600 | 11.600 | 13.000 | 12.200 | 13.200 | 12.600 |
| FB | dRank4 | 19.500 | 16.600 | 18.200 | 18.000 | 19.000 | 18.600 |
| FB | dRank13 | 29.600 | 24.800 | 27.000 | 27.400 | 30.600 | 31.000 |
| FB | RACF1 | 0.381 | 0.509 | 0.382 | 0.423 | 0.394 | 0.427 |
| FB | coll1 | 0.520 | 0.481 | 0.575 | 0.516 | 0.440 | 0.421 |
| FB | coll5 | 0.910 | 0.911 | 0.899 | 0.896 | 0.876 | 0.864 |
| FB | coll20 | 0.950 | 0.947 | 0.966 | 0.964 | 0.957 | 0.963 |
| IG | dRank1 | 15.400 | 15.200 | 17.200 | 15.000 | 17.200 | 15.200 |
| IG | dRank4 | 18.600 | 18.200 | 19.400 | 17.600 | 20.000 | 17.600 |
| IG | RACF1 | 0.019 | 0.134 | 0.043 | 0.069 | 0.048 | 0.062 |
| IG | coll1 | 0.317 | 0.536 | 0.362 | 0.365 | 0.379 | 0.327 |
| IG | coll5 | 0.900 | 0.903 | 0.857 | 0.848 | 0.862 | 0.848 |
| IG | coll20 | 0.967 | 0.953 | 0.974 | 0.970 | 0.971 | 0.969 |
| Comments | dRank1 | 6.000 | 6.000 | 6.600 | 6.800 | 6.600 | 6.400 |
| Comments | dRank4 | 8.600 | 8.000 | 8.800 | 8.200 | 9.000 | 8.000 |
| Comments | dRank13 | 12.200 | 10.600 | 12.800 | 12.000 | 13.200 | 12.000 |
| Comments | RACF1 | 0.615 | 0.689 | 0.641 | 0.595 | 0.659 | 0.594 |
| Comments | coll1 | 0.046 | 0.020 | 0.025 | 0.082 | 0.131 | 0.143 |
| Comments | coll5 | 0.712 | 0.707 | 0.700 | 0.713 | 0.714 | 0.714 |
| Comments | coll20 | 0.915 | 0.953 | 0.929 | 0.926 | 0.922 | 0.922 |
| Submissions | dRank1 | 3.600 | 3.400 | 4.200 | 4.200 | 3.800 | 4.200 |
| Submissions | dRank4 | 4.800 | 5.000 | 6.400 | 5.800 | 5.800 | 6.000 |
| Submissions | dRank13 | 7.400 | 7.200 | 10.600 | 9.800 | 10.200 | 10.600 |
| Submissions | RACF1 | 0.679 | 0.774 | 0.781 | 0.759 | 0.775 | 0.742 |
| Submissions | coll1 | 0.023 | 0.089 | 0.016 | 0.053 | 0.066 | 0.137 |
| Submissions | coll5 | 0.546 | 0.454 | 0.541 | 0.554 | 0.590 | 0.642 |
| Submissions | coll20 | 0.831 | 0.832 | 0.880 | 0.890 | 0.882 | 0.889 |

### 5.2 Goal 1 — unconditional structure (20 paired seeds; median [q05, q95]); Round 4 structure = Round 3


**FB** (K = 3500). Scalars S2 11 → 12, S3 14 → 14. S2 σ_L0 0.1253 → 0.0776, β +0.079 → -0.013, φ 0.84 → 0.84, a_ξ -0.78 → -0.85, a_obs -0.83 → -1.15; S3 σ_L0 0.1136 → 0.0540, surge (λ 0.00096, μ 1.35, hl 52), σ_d 0.00000.

| Statistic | Empirical | R3/R4 S2 | **Cand S2** | R3/R4 S3 | **Cand S3** |
|---|---|---|---|---|---|
| Top-10 turnover | +0.350 | +0.350 [+0.250, +0.458] | **+0.450 [+0.247, +0.555]** | +0.550 [+0.400, +0.650] | **+0.600 [+0.450, +0.703]** |
| K/64 turnover | +0.315 | +0.255 [+0.194, +0.306] | **+0.287 [+0.259, +0.335]** | +0.343 [+0.314, +0.362] | **+0.324 [+0.287, +0.381]** |
| K/16 turnover | +0.259 | +0.268 [+0.247, +0.288] | **+0.271 [+0.241, +0.289]** | +0.295 [+0.274, +0.315] | **+0.259 [+0.240, +0.285]** |
| K/4 turnover | +0.237 | +0.212 [+0.201, +0.221] | **+0.205 [+0.195, +0.215]** | +0.223 [+0.214, +0.237] | **+0.185 [+0.168, +0.198]** |
| Rank-1 weekly change | +0.518 | +0.459 [+0.301, +0.616] | **+0.506 [+0.226, +0.671]** | +0.435 [+0.244, +0.730] | **+0.406 [+0.219, +0.547]** |
| Head share (last block) | +0.229 | +0.242 [+0.222, +0.276] | **+0.242 [+0.223, +0.276]** | +0.290 [+0.239, +0.350] | **+0.296 [+0.245, +0.397]** |
| Max log activity | +16.855 | +17.274 [+17.151, +18.100] | **+17.072 [+16.644, +17.841]** | +19.375 [+18.260, +20.708] | **+19.270 [+18.400, +21.180]** |
| Boundary ratio | +0.980 | +0.998 [+0.976, +1.024] | **+1.069 [+1.036, +1.105]** | +1.137 [+1.077, +1.179] | **+1.178 [+1.134, +1.224]** |
| Departure rate | +0.033 | +0.044 [+0.042, +0.046] | **+0.035 [+0.033, +0.037]** | +0.044 [+0.042, +0.046] | **+0.024 [+0.023, +0.025]** |
| Immediate crossing / dep. | +0.957 | +0.954 [+0.945, +0.958] | **+0.937 [+0.932, +0.944]** | +0.944 [+0.936, +0.949] | **+0.903 [+0.893, +0.921]** |
| Absent→returns ≤13w / dep. | +0.037 | +0.045 [+0.041, +0.053] | **+0.061 [+0.053, +0.066]** | +0.054 [+0.048, +0.062] | **+0.094 [+0.077, +0.102]** |
| Terminal / dep. | +0.005 | +0.001 [+0.000, +0.003] | **+0.001 [+0.000, +0.002]** | +0.002 [+0.001, +0.003] | **+0.003 [+0.001, +0.006]** |
| Gapped ρ | -0.095 | -0.068 [-0.084, -0.057] | **-0.068 [-0.085, -0.055]** | -0.067 [-0.079, -0.054] | **-0.083 [-0.099, -0.060]** |
| Asymmetry D | +0.031 | -0.027 [-0.045, -0.003] | **-0.034 [-0.058, -0.007]** | +0.044 [+0.003, +0.072] | **+0.080 [+0.036, +0.111]** |
| p_fall | -0.111 | -0.092 [-0.105, -0.064] | **-0.094 [-0.110, -0.059]** | -0.053 [-0.087, -0.040] | **-0.049 [-0.092, -0.030]** |
| p_rise | -0.142 | -0.062 [-0.087, -0.046] | **-0.061 [-0.073, -0.035]** | -0.104 [-0.124, -0.077] | **-0.133 [-0.146, -0.099]** |
| m21 | -0.107 | +0.023 [-0.008, +0.041] | **-0.001 [-0.032, +0.016]** | -0.163 [-0.211, -0.105] | **-0.265 [-0.302, -0.204]** |
| m12 | +0.098 | -0.017 [-0.036, +0.007] | **-0.023 [-0.066, +0.009]** | +0.015 [-0.016, +0.056] | **+0.047 [+0.019, +0.095]** |
| Leverage | +0.938 | +1.020 [+0.970, +1.063] | **+1.056 [+1.011, +1.129]** | +1.016 [+0.971, +1.102] | **+1.004 [+0.936, +1.043]** |
| Decline/gain run ratio r2 | +1.011 | +1.001 [+0.967, +1.038] | **+1.128 [+1.075, +1.187]** | +1.075 [+1.047, +1.104] | **+1.100 [+1.033, +1.148]** |
| Head share by block | 0.239 / 0.222 / 0.229 | 0.241 / 0.239 / 0.242 | **0.241 / 0.239 / 0.242** | 0.280 / 0.287 / 0.290 | **0.295 / 0.278 / 0.296** |
| E[y given x decile] d1..d10 | +0.16 +0.06 +0.07 +0.06 +0.00 +0.02 -0.03 -0.02 -0.07 -0.25 | +0.14 +0.06 +0.03 +0.01 -0.01 -0.02 -0.03 -0.04 -0.06 -0.09 | **+0.13 +0.05 +0.03 +0.01 +0.01 -0.01 -0.02 -0.04 -0.05 -0.10** | +0.08 +0.05 +0.03 +0.02 +0.02 +0.00 +0.00 -0.02 -0.03 -0.17 | **+0.06 +0.06 +0.05 +0.02 +0.03 +0.01 +0.01 -0.02 -0.05 -0.19** |

**IG** (K = 10000). Scalars S2 11 → 12, S3 14 → 14. S2 σ_L0 0.1222 → 0.0699, β +0.468 → +0.000, φ 0.93 → 0.93, a_ξ -0.51 → -0.49, a_obs -0.16 → -0.28; S3 σ_L0 0.0954 → 0.0836, surge (λ 0.01113, μ 0.78, hl 39), σ_d 0.00000.

| Statistic | Empirical | R3/R4 S2 | **Cand S2** | R3/R4 S3 | **Cand S3** |
|---|---|---|---|---|---|
| Top-10 turnover | +0.267 | +0.167 [+0.065, +0.202] | **+0.200 [+0.133, +0.300]** | +0.333 [+0.232, +0.433] | **+0.367 [+0.265, +0.473]** |
| K/64 turnover | +0.263 | +0.193 [+0.182, +0.223] | **+0.203 [+0.185, +0.227]** | +0.250 [+0.231, +0.267] | **+0.263 [+0.239, +0.287]** |
| K/16 turnover | +0.313 | +0.208 [+0.197, +0.219] | **+0.236 [+0.221, +0.245]** | +0.264 [+0.252, +0.276] | **+0.291 [+0.281, +0.297]** |
| K/4 turnover | +0.295 | +0.223 [+0.219, +0.227] | **+0.229 [+0.225, +0.233]** | +0.259 [+0.254, +0.263] | **+0.274 [+0.270, +0.281]** |
| Rank-1 weekly change | +0.392 | +0.814 [+0.643, +0.903] | **+0.539 [+0.149, +0.670]** | +0.755 [+0.385, +0.962] | **+0.480 [+0.176, +0.864]** |
| Head share (last block) | +0.419 | +0.392 [+0.371, +0.412] | **+0.422 [+0.397, +0.456]** | +0.396 [+0.374, +0.441] | **+0.408 [+0.391, +0.455]** |
| Max log activity | +17.628 | +18.971 [+18.247, +19.988] | **+18.386 [+17.971, +19.501]** | +18.935 [+18.499, +20.134] | **+18.862 [+18.246, +19.688]** |
| Boundary ratio | +1.000 | +0.978 [+0.960, +0.996] | **+1.016 [+0.998, +1.025]** | +1.157 [+1.137, +1.174] | **+1.164 [+1.150, +1.183]** |
| Departure rate | +0.138 | +0.112 [+0.110, +0.114] | **+0.105 [+0.104, +0.107]** | +0.121 [+0.119, +0.122] | **+0.116 [+0.115, +0.118]** |
| Immediate crossing / dep. | +0.762 | +0.748 [+0.744, +0.754] | **+0.726 [+0.719, +0.731]** | +0.729 [+0.725, +0.732] | **+0.711 [+0.704, +0.715]** |
| Absent→returns ≤13w / dep. | +0.218 | +0.242 [+0.237, +0.246] | **+0.262 [+0.256, +0.269]** | +0.260 [+0.257, +0.264] | **+0.278 [+0.273, +0.285]** |
| Terminal / dep. | +0.020 | +0.010 [+0.009, +0.012] | **+0.011 [+0.010, +0.013]** | +0.010 [+0.009, +0.012] | **+0.011 [+0.010, +0.012]** |
| Gapped ρ | -0.021 | -0.103 [-0.111, -0.092] | **-0.098 [-0.110, -0.090]** | -0.081 [-0.090, -0.071] | **-0.089 [-0.100, -0.080]** |
| Asymmetry D | -0.087 | +0.005 [-0.014, +0.028] | **+0.001 [-0.032, +0.013]** | +0.082 [+0.058, +0.106] | **+0.078 [+0.051, +0.101]** |
| p_fall | -0.081 | -0.110 [-0.131, -0.084] | **-0.106 [-0.131, -0.092]** | -0.041 [-0.064, -0.026] | **-0.058 [-0.080, -0.038]** |
| p_rise | +0.006 | -0.114 [-0.131, -0.094] | **-0.105 [-0.123, -0.086]** | -0.128 [-0.138, -0.114] | **-0.134 [-0.150, -0.120]** |
| m21 | +0.038 | -0.017 [-0.044, +0.005] | **-0.026 [-0.045, -0.005]** | -0.162 [-0.191, -0.124] | **-0.155 [-0.198, -0.124]** |
| m12 | +0.088 | +0.027 [-0.002, +0.059] | **+0.019 [-0.002, +0.034]** | +0.030 [-0.001, +0.051] | **+0.030 [-0.009, +0.044]** |
| Leverage | +0.938 | +0.975 [+0.935, +1.021] | **+0.983 [+0.957, +1.037]** | +1.026 [+0.989, +1.055] | **+1.023 [+0.998, +1.074]** |
| Decline/gain run ratio r2 | +1.214 | +0.995 [+0.982, +1.020] | **+1.049 [+1.027, +1.063]** | +1.201 [+1.181, +1.232] | **+1.189 [+1.163, +1.208]** |
| Head share by block | 0.437 / 0.432 / 0.424 / 0.419 | 0.396 / 0.407 / 0.383 / 0.392 | **0.422 / 0.421 / 0.415 / 0.422** | 0.399 / 0.403 / 0.399 / 0.396 | **0.406 / 0.414 / 0.422 / 0.408** |
| E[y given x decile] d1..d10 | +0.12 +0.02 +0.01 -0.04 -0.03 -0.03 -0.04 -0.04 +0.01 +0.01 | +0.17 +0.09 +0.07 +0.03 +0.01 -0.00 -0.03 -0.05 -0.09 -0.19 | **+0.16 +0.09 +0.05 +0.03 +0.01 -0.01 -0.03 -0.05 -0.10 -0.18** | +0.06 +0.06 +0.05 +0.05 +0.04 +0.03 +0.01 -0.01 -0.06 -0.25 | **+0.09 +0.07 +0.06 +0.05 +0.04 +0.03 +0.01 -0.03 -0.06 -0.26** |

**Comments** (K = 12500). Scalars S2 11 → 12, S3 15 → 15. S2 σ_L0 0.1135 → 0.0846, β +0.130 → +0.091, φ 0.93 → 0.93, a_ξ -0.84 → -1.25, a_obs -1.13 → -1.26; S3 σ_L0 0.0767 → 0.0700, surge (λ 0.00127, μ 1.44, hl 52), σ_d 0.00959.

| Statistic | Empirical | R3/R4 S2 | **Cand S2** | R3/R4 S3 | **Cand S3** |
|---|---|---|---|---|---|
| Top-10 turnover | +0.267 | +0.267 [+0.167, +0.372] | **+0.283 [+0.163, +0.370]** | +0.450 [+0.332, +0.570] | **+0.417 [+0.267, +0.570]** |
| K/64 turnover | +0.209 | +0.174 [+0.144, +0.190] | **+0.179 [+0.169, +0.211]** | +0.247 [+0.232, +0.274] | **+0.243 [+0.227, +0.262]** |
| K/16 turnover | +0.187 | +0.151 [+0.145, +0.156] | **+0.166 [+0.156, +0.175]** | +0.194 [+0.184, +0.205] | **+0.183 [+0.174, +0.191]** |
| K/4 turnover | +0.162 | +0.122 [+0.120, +0.128] | **+0.115 [+0.111, +0.119]** | +0.147 [+0.144, +0.151] | **+0.136 [+0.134, +0.139]** |
| Rank-1 weekly change | +0.028 | +0.000 [+0.000, +0.101] | **+0.066 [+0.000, +0.313]** | +0.200 [+0.084, +0.293] | **+0.177 [+0.056, +0.274]** |
| Head share (last block) | +0.506 | +0.590 [+0.548, +0.639] | **+0.574 [+0.536, +0.603]** | +0.591 [+0.532, +0.644] | **+0.608 [+0.562, +0.648]** |
| Max log activity | +17.919 | +17.898 [+17.422, +18.663] | **+17.500 [+17.148, +17.980]** | +18.900 [+17.963, +19.822] | **+18.935 [+18.170, +20.936]** |
| Boundary ratio | +1.009 | +1.001 [+0.990, +1.006] | **+1.058 [+1.044, +1.068]** | +1.131 [+1.113, +1.141] | **+1.137 [+1.121, +1.160]** |
| Departure rate | +0.016 | +0.018 [+0.017, +0.018] | **+0.017 [+0.017, +0.018]** | +0.014 [+0.013, +0.015] | **+0.011 [+0.011, +0.012]** |
| Immediate crossing / dep. | +0.989 | +0.927 [+0.921, +0.931] | **+0.922 [+0.919, +0.926]** | +0.892 [+0.886, +0.901] | **+0.874 [+0.867, +0.880]** |
| Absent→returns ≤13w / dep. | +0.006 | +0.072 [+0.068, +0.078] | **+0.077 [+0.073, +0.080]** | +0.107 [+0.098, +0.113] | **+0.124 [+0.118, +0.132]** |
| Terminal / dep. | +0.005 | +0.001 [+0.001, +0.001] | **+0.001 [+0.001, +0.002]** | +0.001 [+0.001, +0.002] | **+0.001 [+0.001, +0.002]** |
| Gapped ρ | +0.145 | -0.049 [-0.056, -0.039] | **-0.032 [-0.041, -0.024]** | +0.104 [+0.094, +0.109] | **+0.159 [+0.152, +0.169]** |
| Asymmetry D | +0.132 | -0.039 [-0.053, -0.021] | **-0.032 [-0.048, -0.017]** | +0.149 [+0.137, +0.170] | **+0.207 [+0.183, +0.222]** |
| p_fall | +0.158 | -0.072 [-0.084, -0.052] | **-0.053 [-0.062, -0.038]** | +0.135 [+0.115, +0.144] | **+0.199 [+0.183, +0.210]** |
| p_rise | +0.026 | -0.034 [-0.047, -0.024] | **-0.019 [-0.030, -0.008]** | -0.016 [-0.034, -0.007] | **-0.005 [-0.021, +0.009]** |
| m21 | -0.103 | +0.026 [+0.014, +0.043] | **+0.012 [+0.001, +0.031]** | -0.322 [-0.356, -0.294] | **-0.447 [-0.482, -0.401]** |
| m12 | -0.022 | -0.014 [-0.026, +0.001] | **-0.001 [-0.012, +0.009]** | +0.097 [+0.082, +0.111] | **+0.140 [+0.106, +0.160]** |
| Leverage | +1.112 | +1.022 [+0.991, +1.050] | **+1.003 [+0.980, +1.028]** | +0.893 [+0.862, +0.907] | **+0.833 [+0.802, +0.872]** |
| Decline/gain run ratio r2 | +1.281 | +1.009 [+1.000, +1.023] | **+1.078 [+1.072, +1.091]** | +1.122 [+1.113, +1.136] | **+1.138 [+1.123, +1.150]** |
| Head share by block | 0.614 / 0.573 / 0.529 / 0.506 | 0.580 / 0.590 / 0.592 / 0.590 | **0.572 / 0.571 / 0.575 / 0.574** | 0.588 / 0.556 / 0.580 / 0.591 | **0.597 / 0.591 / 0.580 / 0.608** |
| E[y given x decile] d1..d10 | -0.22 -0.12 -0.05 -0.01 +0.02 +0.05 +0.07 +0.09 +0.11 +0.05 | +0.10 +0.04 +0.02 -0.00 -0.01 -0.01 -0.02 -0.03 -0.04 -0.05 | **+0.06 +0.03 +0.01 -0.00 -0.01 -0.01 -0.01 -0.01 -0.02 -0.03** | -0.19 -0.09 -0.06 -0.02 +0.02 +0.04 +0.07 +0.11 +0.14 -0.02 | **-0.24 -0.12 -0.07 -0.02 +0.01 +0.05 +0.09 +0.13 +0.17 -0.01** |

**Submissions** (K = 5000). Scalars S2 11 → 12, S3 15 → 15. S2 σ_L0 0.0909 → 0.1174, β +0.054 → +0.159, φ 0.93 → 0.93, a_ξ -1.01 → -2.18, a_obs -1.22 → -1.83; S3 σ_L0 0.0577 → 0.0578, surge (λ 0.00139, μ 1.23, hl 52), σ_d 0.00921.

| Statistic | Empirical | R3/R4 S2 | **Cand S2** | R3/R4 S3 | **Cand S3** |
|---|---|---|---|---|---|
| Top-10 turnover | +0.300 | +0.233 [+0.098, +0.302] | **+0.167 [+0.100, +0.302]** | +0.300 [+0.232, +0.402] | **+0.267 [+0.133, +0.368]** |
| K/64 turnover | +0.179 | +0.194 [+0.170, +0.227] | **+0.203 [+0.183, +0.240]** | +0.295 [+0.264, +0.334] | **+0.248 [+0.220, +0.292]** |
| K/16 turnover | +0.189 | +0.165 [+0.156, +0.183] | **+0.180 [+0.163, +0.193]** | +0.237 [+0.214, +0.253] | **+0.204 [+0.175, +0.217]** |
| K/4 turnover | +0.162 | +0.141 [+0.133, +0.151] | **+0.155 [+0.147, +0.163]** | +0.177 [+0.169, +0.182] | **+0.154 [+0.146, +0.161]** |
| Rank-1 weekly change | +0.057 | +0.047 [+0.000, +0.131] | **+0.066 [+0.000, +0.228]** | +0.066 [+0.000, +0.175] | **+0.078 [+0.014, +0.253]** |
| Head share (last block) | +0.356 | +0.511 [+0.464, +0.594] | **+0.492 [+0.443, +0.527]** | +0.565 [+0.462, +0.652] | **+0.578 [+0.488, +0.690]** |
| Max log activity | +17.960 | +17.705 [+17.056, +18.561] | **+17.499 [+17.108, +18.162]** | +18.523 [+17.696, +19.723] | **+18.580 [+17.965, +19.792]** |
| Boundary ratio | +1.116 | +0.986 [+0.976, +1.001] | **+1.046 [+1.021, +1.071]** | +1.111 [+1.081, +1.145] | **+1.122 [+1.079, +1.155]** |
| Departure rate | +0.012 | +0.015 [+0.014, +0.016] | **+0.016 [+0.015, +0.017]** | +0.021 [+0.020, +0.022] | **+0.009 [+0.009, +0.010]** |
| Immediate crossing / dep. | +0.992 | +0.977 [+0.973, +0.981] | **+0.975 [+0.972, +0.979]** | +0.981 [+0.978, +0.984] | **+0.959 [+0.955, +0.965]** |
| Absent→returns ≤13w / dep. | +0.006 | +0.023 [+0.018, +0.027] | **+0.025 [+0.021, +0.028]** | +0.019 [+0.016, +0.021] | **+0.040 [+0.035, +0.044]** |
| Terminal / dep. | +0.001 | +0.000 [+0.000, +0.001] | **+0.000 [+0.000, +0.001]** | +0.000 [+0.000, +0.001] | **+0.000 [+0.000, +0.001]** |
| Gapped ρ | +0.155 | -0.039 [-0.051, -0.029] | **-0.031 [-0.040, -0.020]** | +0.088 [+0.075, +0.101] | **+0.152 [+0.135, +0.168]** |
| Asymmetry D | +0.222 | -0.035 [-0.053, -0.012] | **-0.048 [-0.070, -0.016]** | +0.163 [+0.136, +0.190] | **+0.226 [+0.195, +0.267]** |
| p_fall | +0.240 | -0.058 [-0.075, -0.042] | **-0.066 [-0.077, -0.039]** | +0.123 [+0.109, +0.143] | **+0.196 [+0.172, +0.230]** |
| p_rise | +0.018 | -0.029 [-0.044, -0.009] | **-0.017 [-0.033, +0.002]** | -0.036 [-0.059, -0.007] | **-0.029 [-0.055, -0.000]** |
| m21 | -0.163 | +0.024 [+0.011, +0.043] | **+0.022 [-0.001, +0.041]** | -0.348 [-0.402, -0.286] | **-0.486 [-0.552, -0.425]** |
| m12 | +0.007 | -0.016 [-0.036, -0.002] | **-0.001 [-0.023, +0.014]** | +0.101 [+0.073, +0.140] | **+0.143 [+0.111, +0.162]** |
| Leverage | +1.129 | +1.030 [+0.994, +1.081] | **+1.007 [+0.954, +1.061]** | +0.873 [+0.838, +0.936] | **+0.842 [+0.800, +0.862]** |
| Decline/gain run ratio r2 | +1.245 | +1.021 [+0.999, +1.036] | **+1.113 [+1.092, +1.147]** | +1.130 [+1.110, +1.143] | **+1.145 [+1.129, +1.164]** |
| Head share by block | 0.511 / 0.507 / 0.427 / 0.356 | 0.477 / 0.486 / 0.492 / 0.511 | **0.495 / 0.493 / 0.491 / 0.492** | 0.509 / 0.524 / 0.544 / 0.565 | **0.502 / 0.513 / 0.542 / 0.578** |
| E[y given x decile] d1..d10 | -0.27 -0.10 -0.04 +0.02 +0.04 +0.05 +0.08 +0.08 +0.11 +0.03 | +0.07 +0.03 +0.01 +0.00 -0.01 -0.01 -0.02 -0.02 -0.03 -0.04 | **+0.08 +0.02 -0.00 -0.01 -0.02 -0.01 -0.02 -0.01 -0.01 -0.02** | -0.16 -0.07 -0.04 -0.01 +0.01 +0.05 +0.07 +0.10 +0.12 -0.06 | **-0.22 -0.11 -0.06 -0.02 +0.02 +0.05 +0.09 +0.13 +0.17 -0.04** |

### 5.3 Goal 2 — individual endpoint forecasts (M = 39, 5 rolling origins, common truth cohort); each cell R3 → R4 → **Cand**


**FB**

| h | arm | X CRPS | Kalman MAE (no / EWMA) | cov90 (0.900) | PIT SD (0.289) | logR CRPS (no / rank-bin) | fair Brier head (rank-bin) | head share-shift MAE ×1e6 (zero / EWMA) | head shift corr (EWMA) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.2990 → 0.2987 → **0.2984** | 0.4027 → 0.4026 → **0.4020** (0.4490 / 0.4078) | 0.902 → 0.883 → **0.865** | 0.276 → 0.287 → **0.296** | 0.1965 → 0.1969 → **0.1972** (0.2966 / 0.2315) | 0.0721 → 0.0713 → **0.0707** (0.0847) | 580.4 → 570.1 → **570.6** (707.3 / 609.5) | 0.576 → 0.592 → **0.565** (0.554) |
| 1 | S3 | 0.3013 → 0.2994 → **0.2989** | 0.4024 → 0.4022 → **0.4018** (0.4490 / 0.4078) | 0.917 → 0.904 → **0.889** | 0.266 → 0.276 → **0.283** | 0.1974 → 0.1967 → **0.1967** (0.2966 / 0.2315) | 0.0718 → 0.0706 → **0.0707** (0.0847) | 592.1 → 577.5 → **571.5** (707.3 / 609.5) | 0.558 → 0.570 → **0.577** (0.554) |
| 4 | S2 | 0.3429 → 0.3430 → **0.3422** | 0.4661 → 0.4659 → **0.4655** (0.5379 / 0.4752) | 0.912 → 0.894 → **0.880** | 0.270 → 0.280 → **0.290** | 0.2302 → 0.2308 → **0.2309** (0.3589 / 0.2707) | 0.0864 → 0.0871 → **0.0876** (0.1008) | 660.1 → 652.7 → **652.7** (858.5 / 742.4) | 0.621 → 0.631 → **0.637** (0.555) |
| 4 | S3 | 0.3439 → 0.3424 → **0.3418** | 0.4663 → 0.4659 → **0.4660** (0.5379 / 0.4752) | 0.920 → 0.906 → **0.893** | 0.267 → 0.275 → **0.282** | 0.2311 → 0.2303 → **0.2304** (0.3589 / 0.2707) | 0.0882 → 0.0880 → **0.0884** (0.1008) | 684.2 → 657.0 → **661.9** (858.5 / 742.4) | 0.610 → 0.620 → **0.623** (0.555) |
| 13 | S2 | 0.4306 → 0.4301 → **0.4279** | 0.5783 → 0.5804 → **0.5797** (0.6724 / 0.6026) | 0.909 → 0.896 → **0.878** | 0.267 → 0.274 → **0.286** | 0.2707 → 0.2711 → **0.2701** (0.4360 / 0.3162) | 0.0972 → 0.0969 → **0.0966** (0.1149) | 801.8 → 797.8 → **781.1** (1093.2 / 969.4) | 0.632 → 0.630 → **0.647** (0.481) |
| 13 | S3 | 0.4311 → 0.4295 → **0.4288** | 0.5783 → 0.5801 → **0.5807** (0.6724 / 0.6026) | 0.912 → 0.901 → **0.887** | 0.268 → 0.275 → **0.283** | 0.2723 → 0.2714 → **0.2712** (0.4360 / 0.3162) | 0.0992 → 0.0986 → **0.0980** (0.1149) | 861.1 → 844.6 → **858.2** (1093.2 / 969.4) | 0.604 → 0.607 → **0.603** (0.481) |
| 26 | S2 | 0.5128 → 0.5127 → **0.5106** | 0.6934 → 0.6968 → **0.6956** (0.7773 / 0.7214) | 0.906 → 0.895 → **0.873** | 0.270 → 0.276 → **0.291** | 0.3062 → 0.3069 → **0.3062** (0.4857 / 0.3425) | 0.1058 → 0.1060 → **0.1047** (0.1165) | 878.2 → 895.9 → **882.0** (1140.6 / 1043.7) | 0.584 → 0.545 → **0.570** (0.438) |
| 26 | S3 | 0.5144 → 0.5131 → **0.5091** | 0.6924 → 0.6949 → **0.6950** (0.7773 / 0.7214) | 0.908 → 0.897 → **0.883** | 0.272 → 0.278 → **0.286** | 0.3089 → 0.3083 → **0.3060** (0.4857 / 0.3425) | 0.1065 → 0.1054 → **0.1049** (0.1165) | 964.7 → 948.4 → **947.1** (1140.6 / 1043.7) | 0.539 → 0.538 → **0.534** (0.438) |

**IG**

| h | arm | X CRPS | Kalman MAE (no / EWMA) | cov90 (0.900) | PIT SD (0.289) | logR CRPS (no / rank-bin) | fair Brier head (rank-bin) | head share-shift MAE ×1e6 (zero / EWMA) | head shift corr (EWMA) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.4701 → 0.4717 → **0.4704** | 0.6370 → 0.6366 → **0.6370** (0.7903 / 0.6378) | 0.896 → 0.890 → **0.876** | 0.283 → 0.286 → **0.293** | 0.2576 → 0.2591 → **0.2598** (0.4348 / 0.3112) | 0.0831 → 0.0839 → **0.0842** (0.1056) | 351.8 → 353.6 → **349.9** (470.5 / 407.9) | 0.566 → 0.555 → **0.560** (0.495) |
| 1 | S3 | 0.4708 → 0.4719 → **0.4687** | 0.6366 → 0.6363 → **0.6354** (0.7903 / 0.6378) | 0.900 → 0.893 → **0.878** | 0.280 → 0.284 → **0.292** | 0.2576 → 0.2590 → **0.2601** (0.4348 / 0.3112) | 0.0831 → 0.0839 → **0.0856** (0.1056) | 354.1 → 351.1 → **355.1** (470.5 / 407.9) | 0.571 → 0.581 → **0.581** (0.495) |
| 4 | S2 | 0.5099 → 0.5113 → **0.5103** | 0.6905 → 0.6902 → **0.6901** (0.8545 / 0.6940) | 0.893 → 0.887 → **0.875** | 0.282 → 0.286 → **0.293** | 0.2750 → 0.2764 → **0.2772** (0.4703 / 0.3294) | 0.0890 → 0.0900 → **0.0892** (0.1105) | 369.9 → 371.0 → **369.6** (514.7 / 446.0) | 0.627 → 0.631 → **0.605** (0.426) |
| 4 | S3 | 0.5114 → 0.5120 → **0.5083** | 0.6900 → 0.6898 → **0.6884** (0.8545 / 0.6940) | 0.897 → 0.891 → **0.873** | 0.280 → 0.284 → **0.295** | 0.2753 → 0.2764 → **0.2778** (0.4703 / 0.3294) | 0.0887 → 0.0890 → **0.0901** (0.1105) | 371.4 → 368.3 → **369.0** (514.7 / 446.0) | 0.620 → 0.624 → **0.631** (0.426) |
| 13 | S2 | 0.5856 → 0.5877 → **0.5855** | 0.7922 → 0.7924 → **0.7921** (0.9640 / 0.8020) | 0.896 → 0.886 → **0.874** | 0.281 → 0.285 → **0.295** | 0.2917 → 0.2938 → **0.2938** (0.5098 / 0.3469) | 0.0916 → 0.0926 → **0.0930** (0.1123) | 423.6 → 427.2 → **411.8** (585.2 / 520.4) | 0.582 → 0.577 → **0.630** (0.249) |
| 13 | S3 | 0.5862 → 0.5876 → **0.5823** | 0.7919 → 0.7923 → **0.7908** (0.9640 / 0.8020) | 0.897 → 0.888 → **0.866** | 0.281 → 0.284 → **0.298** | 0.2911 → 0.2927 → **0.2934** (0.5098 / 0.3469) | 0.0909 → 0.0918 → **0.0932** (0.1123) | 408.7 → 410.4 → **414.1** (585.2 / 520.4) | 0.641 → 0.633 → **0.625** (0.249) |

**Comments**

| h | arm | X CRPS | Kalman MAE (no / EWMA) | cov90 (0.900) | PIT SD (0.289) | logR CRPS (no / rank-bin) | fair Brier head (rank-bin) | head share-shift MAE ×1e6 (zero / EWMA) | head shift corr (EWMA) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.2091 → 0.2086 → **0.2081** | 0.2858 → 0.2852 → **0.2847** (0.3279 / 0.2867) | 0.907 → 0.905 → **0.896** | 0.277 → 0.279 → **0.285** | 0.0826 → 0.0822 → **0.0820** (0.1248 / 0.1234) | 0.0328 → 0.0324 → **0.0325** (0.0407) | 171.3 → 169.8 → **167.1** (180.2 / 168.0) | 0.362 → 0.402 → **0.481** (0.505) |
| 1 | S3 | 0.2128 → 0.2129 → **0.2193** | 0.2860 → 0.2854 → **0.2850** (0.3279 / 0.2867) | 0.932 → 0.934 → **0.922** | 0.263 → 0.261 → **0.270** | 0.0848 → 0.0847 → **0.0872** (0.1248 / 0.1234) | 0.0340 → 0.0338 → **0.0348** (0.0407) | 183.0 → 182.1 → **178.6** (180.2 / 168.0) | 0.278 → 0.277 → **0.460** (0.505) |
| 4 | S2 | 0.2548 → 0.2544 → **0.2528** | 0.3469 → 0.3460 → **0.3448** (0.3944 / 0.3483) | 0.912 → 0.908 → **0.890** | 0.272 → 0.275 → **0.288** | 0.1038 → 0.1034 → **0.1028** (0.1559 / 0.1415) | 0.0424 → 0.0422 → **0.0420** (0.0499) | 233.4 → 229.6 → **224.7** (256.7 / 233.1) | 0.383 → 0.399 → **0.376** (0.189) |
| 4 | S3 | 0.2580 → 0.2581 → **0.2641** | 0.3464 → 0.3455 → **0.3461** (0.3944 / 0.3483) | 0.927 → 0.929 → **0.901** | 0.264 → 0.262 → **0.280** | 0.1054 → 0.1054 → **0.1077** (0.1559 / 0.1415) | 0.0434 → 0.0432 → **0.0441** (0.0499) | 244.5 → 241.4 → **246.4** (256.7 / 233.1) | 0.327 → 0.350 → **0.228** (0.189) |
| 13 | S2 | 0.3235 → 0.3234 → **0.3211** | 0.4357 → 0.4344 → **0.4336** (0.4839 / 0.4395) | 0.922 → 0.917 → **0.896** | 0.262 → 0.264 → **0.282** | 0.1323 → 0.1321 → **0.1304** (0.1947 / 0.1656) | 0.0556 → 0.0555 → **0.0549** (0.0626) | 300.6 → 301.7 → **295.7** (325.2 / 313.8) | 0.371 → 0.331 → **0.275** (0.098) |
| 13 | S3 | 0.3236 → 0.3243 → **0.3324** | 0.4328 → 0.4320 → **0.4379** (0.4839 / 0.4395) | 0.927 → 0.927 → **0.883** | 0.260 → 0.259 → **0.286** | 0.1325 → 0.1328 → **0.1353** (0.1947 / 0.1656) | 0.0557 → 0.0558 → **0.0563** (0.0626) | 312.8 → 312.0 → **317.0** (325.2 / 313.8) | 0.318 → 0.311 → **0.141** (0.098) |
| 26 | S2 | 0.3964 → 0.3958 → **0.3935** | 0.5325 → 0.5309 → **0.5308** (0.5759 / 0.5350) | 0.918 → 0.913 → **0.891** | 0.263 → 0.265 → **0.285** | 0.1573 → 0.1568 → **0.1542** (0.2298 / 0.1871) | 0.0681 → 0.0672 → **0.0668** (0.0751) | 328.0 → 331.1 → **328.9** (374.1 / 352.0) | 0.633 → 0.611 → **0.504** (0.302) |
| 26 | S3 | 0.3929 → 0.3936 → **0.4040** | 0.5282 → 0.5276 → **0.5383** (0.5759 / 0.5350) | 0.922 → 0.923 → **0.866** | 0.262 → 0.262 → **0.293** | 0.1567 → 0.1570 → **0.1598** (0.2298 / 0.1871) | 0.0675 → 0.0677 → **0.0683** (0.0751) | 363.4 → 368.2 → **345.7** (374.1 / 352.0) | 0.527 → 0.502 → **0.560** (0.302) |
| 52 | S2 | 0.5048 → 0.5042 → **0.5051** | 0.6792 → 0.6766 → **0.6770** (0.6991 / 0.6698) | 0.905 → 0.902 → **0.880** | 0.269 → 0.270 → **0.290** | 0.1898 → 0.1890 → **0.1855** (0.2684 / 0.2102) | 0.0784 → 0.0784 → **0.0767** (0.0805) | 409.0 → 405.5 → **399.9** (459.5 / 447.6) | 0.667 → 0.666 → **0.650** (0.339) |
| 52 | S3 | 0.4965 → 0.4971 → **0.5119** | 0.6727 → 0.6720 → **0.6869** (0.6991 / 0.6698) | 0.917 → 0.914 → **0.851** | 0.265 → 0.264 → **0.295** | 0.1888 → 0.1891 → **0.1923** (0.2684 / 0.2102) | 0.0784 → 0.0787 → **0.0785** (0.0805) | 436.6 → 440.8 → **420.6** (459.5 / 447.6) | 0.642 → 0.632 → **0.604** (0.339) |

**Submissions**

| h | arm | X CRPS | Kalman MAE (no / EWMA) | cov90 (0.900) | PIT SD (0.289) | logR CRPS (no / rank-bin) | fair Brier head (rank-bin) | head share-shift MAE ×1e6 (zero / EWMA) | head shift corr (EWMA) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.1274 → 0.1239 → **0.1242** | 0.1663 → 0.1656 → **0.1664** (0.1839 / 0.1668) | 0.952 → 0.924 → **0.905** | 0.239 → 0.264 → **0.276** | 0.0677 → 0.0649 → **0.0648** (0.0955 / 0.1035) | 0.0221 → 0.0216 → **0.0219** (0.0346) | 206.0 → 198.0 → **196.5** (198.8 / 205.6) | 0.206 → 0.229 → **0.308** (0.182) |
| 1 | S3 | 0.1334 → 0.1294 → **0.1285** | 0.1665 → 0.1663 → **0.1656** (0.1839 / 0.1668) | 0.965 → 0.952 → **0.946** | 0.227 → 0.241 → **0.249** | 0.0712 → 0.0685 → **0.0684** (0.0955 / 0.1035) | 0.0239 → 0.0235 → **0.0236** (0.0346) | 249.1 → 236.2 → **243.5** (198.8 / 205.6) | 0.190 → 0.212 → **0.188** (0.182) |
| 4 | S2 | 0.1618 → 0.1590 → **0.1591** | 0.2095 → 0.2091 → **0.2106** (0.2291 / 0.2103) | 0.951 → 0.928 → **0.916** | 0.236 → 0.255 → **0.267** | 0.0875 → 0.0852 → **0.0850** (0.1229 / 0.1193) | 0.0332 → 0.0327 → **0.0329** (0.0426) | 280.1 → 275.0 → **275.3** (280.7 / 268.3) | 0.207 → 0.183 → **0.395** (0.116) |
| 4 | S3 | 0.1662 → 0.1617 → **0.1604** | 0.2097 → 0.2093 → **0.2089** (0.2291 / 0.2103) | 0.960 → 0.946 → **0.934** | 0.230 → 0.244 → **0.256** | 0.0903 → 0.0875 → **0.0871** (0.1229 / 0.1193) | 0.0342 → 0.0338 → **0.0339** (0.0426) | 318.7 → 306.3 → **324.0** (280.7 / 268.3) | 0.368 → 0.374 → **0.351** (0.116) |
| 13 | S2 | 0.2220 → 0.2192 → **0.2203** | 0.2869 → 0.2861 → **0.2874** (0.3064 / 0.2880) | 0.952 → 0.933 → **0.930** | 0.232 → 0.246 → **0.252** | 0.1229 → 0.1204 → **0.1207** (0.1675 / 0.1459) | 0.0432 → 0.0422 → **0.0419** (0.0489) | 485.8 → 475.5 → **467.7** (475.5 / 466.8) | 0.011 → -0.020 → **0.281** (0.059) |
| 13 | S3 | 0.2225 → 0.2184 → **0.2159** | 0.2855 → 0.2854 → **0.2849** (0.3064 / 0.2880) | 0.953 → 0.941 → **0.925** | 0.235 → 0.246 → **0.260** | 0.1243 → 0.1214 → **0.1200** (0.1675 / 0.1459) | 0.0449 → 0.0436 → **0.0434** (0.0489) | 502.7 → 482.8 → **498.2** (475.5 / 466.8) | 0.389 → 0.410 → **0.446** (0.059) |
| 26 | S2 | 0.2849 → 0.2828 → **0.2868** | 0.3776 → 0.3760 → **0.3760** (0.3929 / 0.3773) | 0.950 → 0.935 → **0.940** | 0.238 → 0.248 → **0.249** | 0.1546 → 0.1519 → **0.1546** (0.2121 / 0.1736) | 0.0540 → 0.0534 → **0.0535** (0.0575) | 640.9 → 625.8 → **632.0** (643.0 / 637.4) | 0.075 → 0.140 → **0.460** (0.082) |
| 26 | S3 | 0.2826 → 0.2800 → **0.2779** | 0.3723 → 0.3725 → **0.3731** (0.3929 / 0.3773) | 0.947 → 0.936 → **0.919** | 0.241 → 0.250 → **0.264** | 0.1564 → 0.1540 → **0.1521** (0.2121 / 0.1736) | 0.0570 → 0.0562 → **0.0553** (0.0575) | 623.9 → 614.0 → **638.2** (643.0 / 637.4) | 0.687 → 0.674 → **0.682** (0.082) |
| 52 | S2 | 0.3820 → 0.3788 → **0.3864** | 0.5123 → 0.5085 → **0.5044** (0.5187 / 0.5072) | 0.944 → 0.934 → **0.947** | 0.244 → 0.250 → **0.246** | 0.1990 → 0.1954 → **0.2018** (0.2706 / 0.2121) | 0.0714 → 0.0718 → **0.0749** (0.0752) | 865.7 → 851.2 → **804.9** (923.9 / 924.2) | 0.471 → 0.446 → **0.584** (0.078) |
| 52 | S3 | 0.3814 → 0.3784 → **0.3733** | 0.5035 → 0.5037 → **0.5031** (0.5187 / 0.5072) | 0.948 → 0.941 → **0.920** | 0.239 → 0.245 → **0.260** | 0.2030 → 0.2004 → **0.1961** (0.2706 / 0.2121) | 0.0772 → 0.0768 → **0.0750** (0.0752) | 838.7 → 837.8 → **818.4** (923.9 / 924.2) | 0.807 → 0.805 → **0.800** (0.078) |

### 5.4 Out-of-period structure (fit 2019–20 weeks 0..103 → roll 2021–22, 20 seeds): emp 21–22 | R3 S2 → **Cand S2** | R3 S3 → R4 S3 → **Cand S3**


**Comments**

| Statistic | emp 2021–22 | R3 S2 | **Cand S2** | R3 S3 | R4 S3 | **Cand S3** |
|---|---|---|---|---|---|---|
| Head share | +0.506 | +0.561 [+0.541, +0.587] | **+0.548 [+0.525, +0.621]** | +0.607 [+0.573, +0.687] | +0.586 [+0.559, +0.654] | **+0.597 [+0.536, +0.711]** |
| Top-10 turnover | +0.300 | +0.200 [+0.000, +0.400] | **+0.300 [+0.195, +0.405]** | +0.400 [+0.200, +0.500] | +0.400 [+0.295, +0.500] | **+0.300 [+0.200, +0.500]** |
| K/4 turnover | +0.153 | +0.132 [+0.127, +0.138] | **+0.140 [+0.136, +0.149]** | +0.143 [+0.137, +0.149] | +0.143 [+0.134, +0.148] | **+0.109 [+0.103, +0.117]** |
| Ladder gap rank 1 | +10.272 | +10.384 [+9.737, +11.037] | **+9.541 [+9.008, +10.862]** | +10.342 [+9.861, +11.157] | +10.420 [+9.390, +11.111] | **+10.841 [+10.299, +11.869]** |
| Boundary ratio | +0.947 | +0.987 [+0.979, +1.000] | **+1.043 [+1.027, +1.056]** | +1.186 [+1.167, +1.220] | +1.192 [+1.160, +1.216] | **+1.265 [+1.225, +1.299]** |
| Departure rate | +0.007 | +0.013 [+0.013, +0.013] | **+0.010 [+0.010, +0.011]** | +0.008 [+0.008, +0.009] | +0.008 [+0.008, +0.009] | **+0.002 [+0.002, +0.002]** |
| Immediate crossing / dep. | +0.956 | +0.946 [+0.943, +0.951] | **+0.934 [+0.927, +0.940]** | +0.910 [+0.894, +0.919] | +0.910 [+0.897, +0.919] | **+0.700 [+0.659, +0.750]** |
| Gapped ρ | +0.124 | -0.078 [-0.085, -0.070] | **-0.064 [-0.070, -0.057]** | -0.007 [-0.018, -0.002] | -0.010 [-0.016, -0.004] | **-0.030 [-0.039, -0.021]** |
| Asymmetry D | +0.123 | -0.042 [-0.064, -0.021] | **-0.050 [-0.070, -0.030]** | +0.055 [+0.035, +0.076] | +0.054 [+0.044, +0.070] | **+0.127 [+0.094, +0.140]** |
| E[y given x decile] d1..d10 | -0.18 -0.06 -0.03 -0.00 +0.02 +0.03 +0.05 +0.06 +0.06 +0.07 | | | +0.02 -0.01 -0.00 +0.00 +0.00 +0.01 +0.02 +0.03 +0.02 -0.09 | | **-0.00 +0.01 +0.01 +0.02 +0.02 +0.02 +0.02 +0.02 +0.01 -0.12** |

**Submissions**

| Statistic | emp 2021–22 | R3 S2 | **Cand S2** | R3 S3 | R4 S3 | **Cand S3** |
|---|---|---|---|---|---|---|
| Head share | +0.356 | +0.523 [+0.471, +0.601] | **+0.493 [+0.459, +0.547]** | +0.508 [+0.446, +0.569] | +0.497 [+0.441, +0.652] | **+0.520 [+0.471, +0.603]** |
| Top-10 turnover | +0.300 | +0.300 [+0.100, +0.305] | **+0.200 [+0.100, +0.300]** | +0.400 [+0.200, +0.505] | +0.400 [+0.295, +0.405] | **+0.300 [+0.100, +0.405]** |
| K/4 turnover | +0.151 | +0.183 [+0.172, +0.191] | **+0.208 [+0.203, +0.218]** | +0.197 [+0.188, +0.210] | +0.194 [+0.186, +0.206] | **+0.163 [+0.149, +0.173]** |
| Ladder gap rank 1 | +7.062 | +8.794 [+7.977, +9.833] | **+8.892 [+7.982, +9.508]** | +8.551 [+8.126, +9.428] | +8.430 [+7.960, +9.104] | **+9.125 [+8.687, +10.222]** |
| Boundary ratio | +0.881 | +0.985 [+0.970, +0.996] | **+1.160 [+1.127, +1.198]** | +1.291 [+1.256, +1.325] | +1.306 [+1.275, +1.361] | **+1.346 [+1.291, +1.414]** |
| Departure rate | +0.005 | +0.013 [+0.012, +0.014] | **+0.018 [+0.016, +0.019]** | +0.007 [+0.006, +0.008] | +0.005 [+0.005, +0.006] | **+0.005 [+0.004, +0.005]** |
| Immediate crossing / dep. | +0.950 | +0.986 [+0.982, +0.990] | **+0.990 [+0.983, +0.992]** | +0.972 [+0.961, +0.981] | +0.967 [+0.956, +0.972] | **+0.958 [+0.943, +0.964]** |
| Gapped ρ | +0.102 | -0.033 [-0.048, -0.017] | **-0.033 [-0.050, -0.013]** | +0.085 [+0.073, +0.102] | +0.110 [+0.100, +0.124] | **+0.071 [+0.055, +0.082]** |
| Asymmetry D | +0.133 | -0.029 [-0.051, -0.007] | **-0.056 [-0.076, -0.034]** | +0.163 [+0.144, +0.190] | +0.200 [+0.171, +0.223] | **+0.195 [+0.157, +0.221]** |
| E[y given x decile] d1..d10 | -0.13 -0.03 -0.02 +0.00 +0.01 +0.04 +0.03 +0.04 +0.05 +0.02 | | | -0.10 -0.06 -0.03 -0.01 +0.02 +0.03 +0.06 +0.08 +0.09 -0.08 | | **-0.08 -0.04 -0.02 +0.00 +0.02 +0.03 +0.04 +0.06 +0.08 -0.09** |


### 5.5 Sensitivity of the fixed jump rate (6-seed structure preview, `final/g1_quick_lam0005.log`, `final/g1_quick_lam002.log`)

| S2 statistic (emp) | λ_J = 0.005 | **λ_J = 0.01 (candidate)** | λ_J = 0.02 |
|---|---|---|---|
| FB top-10 turnover (0.350) | 0.600 | 0.450 | 0.325 |
| FB rank-1 change (0.518) | 0.265 | 0.535 | 0.353 |
| FB head share (0.229) / max X (16.86) | 0.275 / 18.38 | 0.239 / 16.98 | 0.238 / 17.36 |
| FB boundary (0.980) / departure rate (0.033) | 1.188 / 0.038 | 1.063 / 0.035 | 1.041 / 0.033 |
| IG rank-1 change (0.392) / head share (0.419) / boundary (1.000) | 0.510 / 0.419 / 1.040 | 0.343 / 0.421 / 1.016 | 0.431 / 0.413 / 0.994 |
| Comments top-10 (0.267) / rank-1 (0.028) / boundary (1.009) / dep. rate (0.016) | 0.450 / 0.146 / 1.132 / 0.027 | 0.250 / 0.057 / 1.052 / 0.017 | 0.283 / 0.026 / 1.023 / 0.013 |
| Submissions top-10 (0.300) / K/4 (0.162) / boundary (1.116) / dep. rate (0.012) | 0.183 / 0.136 / 1.094 / 0.012 | 0.183 / 0.156 / 1.045 / 0.016 | 0.167 / 0.141 / 1.011 / 0.009 |

Rarer, larger jumps (0.005) damage the Facebook head (top-10 0.60, max X 18.4, boundary 1.19); more frequent, smaller
jumps (0.02) give the best boundary ratios and the right Facebook top-10 turnover. The candidate's 0.01 is the value the
D1 ratio-decline evidence supported before any structure score was seen, and it was not moved afterwards. An identifying
moment for λ_J is the first thing a follow-up should build (§6).

---

## 6. Verification and honest remaining limitations

### 6.1 Verification

- `test_common_rankdiff_fable.py`: **23 passed** (`final/pytest_final.log`): the 15 inherited tests (one re-planted with the
  model's own γ_L = 0, its γ_L = 0.5 variant kept as a documented misspecification check; one enlarged so permanent-rank
  bands are not degenerate; one variance-conservation margin asserted on the bit-identical variance path) plus 8 Round 5
  tests: bit-identity of `scale_moment="variance"` against `llm_fitting/common_rankdiff.py` on planted data (S2 and S3,
  all scalars, pool and drift arrays); robust = variance on a Gaussian panel; Gaussian IQR/variance ratio = 1; jumps
  inactive on a jump-free panel (11 scalars); planted-jump recovery (λ fixed, variance within the documented 0.3–1.3×
  band, core σ_L0 within 30%); one-step simulator jump variance equal to λ_J c_J s_sc(z)² by rank; robust fit not
  inflated by external rank-uniform jumps with total lasting variance within 30%.
- Variance-path bit-identity also verified on all eight real-panel fits (`diag_bitident_quickfit*.log`).
- Harness determinism: the untouched baseline reproduces Astra's FB gate archives to every digit on all five splits
  (`round4_check/`), so Astra's archives are a valid Round 4 baseline for this machine.
- The mandatory benchmark ran unmodified (`../benchmark_candidate.py --mode both`, `final/benchmark_full.log`); the
  exact frozen gate functions and the gold evaluator were imported as-is. The repository health script was not run: its
  legacy guard reads a panel outside the authorized cache (same exclusion Astra recorded).
- Every diagnostic has a `.py`, `.log`, `.json` triple in this folder; predictions were written before each run and are
  scored in `EXPERIMENT_LOG.md`.

### 6.2 Limitations and open items

1. **The jump rate is a convention, not an estimate.** λ_J = 0.01/week is fixed from the D1 ratio-decline evidence
   (all four panels), with the same standing as the fixed 52-week drift half-life. Five per-panel identification
   schemes were tried and none was stable (EXPERIMENT_LOG §4). The brief's "100% moment-identified" standard is met
   for c_J (gap moment) and for every core scalar; it is met for λ_J only in the weaker sense of an independent
   diagnostic fixed before scoring.
2. **The jump variance is understated when λ_J·13 is not small.** The gap is taken once from the jump-free robust core,
   whose IQR at h ≤ 13 already carries some jump mass. Planted recovery: 0.4–0.6× at λ = 0.01 with a 64% jump share.
   Iterating the gap against a jump-aware core erodes the diffusive core band by band because of item 3. On Reddit the
   simulated total variance at h ≥ 13 is therefore below the data's even though typical movement is now right.
3. **The inherited rank attenuation of jumps is wrong in opposite directions on different platforms.** Facebook's head
   band carries no jump variance (gap 0.0001 of 0.0092) while the law assigns 0.0025; Reddit's head bands carry as
   much as the body. Consequences: FB top-10 turnover overshoots with jumps (6-seed preview 0.45 vs 0.35), the IG head
   variance is inflated (band 0, h = 26: 0.76 vs 0.64), and boundary ratios sit at 1.04–1.06 instead of ≈ 1.00.
   A two-scalar jump law fitted to the band gaps would fix this at the price of a 13th scalar.
4. **The passing/observation tails are still too light.** The data's 1-week var/IQR ratio is 1.8–4.3; the bounded pool
   with lognormal v_i gives 1.4–1.6. Permanent lasting jumps cannot represent transient bursts; the model under-predicts
   extreme one-week spikes by construction (this is also what protects head shares).
5. **Instagram's lasting law is on the φ/σ_L ridge under either moment** (T = 52, φ = 0.93 makes passing nearly
   linear over 13–26 lags); its lasting variance lands in one or two bands. Its gate scores are dominated by RACF1,
   whose empirical value (0.01–0.03) sits at `MOM_FLOOR`, so relative errors of 1–3.5 on three eligible splits decide
   the panel's number (§5.1). This is the near-zero-moment artifact recorded in the pitfalls file, not a movement defect:
   dRank1/dRank4 are within one rank of empirical on every split (`final/gate_decomp_instagram_hm.log`).
6. **The robust β is sensitive to unmodeled lasting-volatility heterogeneity** (planted γ_L = 0.5 biases it by
   +0.10 where the mean-square β is unbiased). Astra's evidence that γ_L = 0 makes this a second-order risk.
7. **S3's head/boundary overshoot is not fixed.** The surge's κ₅-based size is not repairable by re-pooling (D4); the
   robust core changes S3's structure (Reddit ρ and D now match out of moment; IG head statistics improve; FB/Comments
   top-10 turnover still 0.45–0.58) but the S3 gate on FB/Comments is worse than Round 4 and S3 Submissions is unstable
   across splits. S3 keeps the surge as its tail process to stay within 15 scalars.
8. **The filter statistic was measured, not changed.** D3 shows the plain mean |ΔX₁| is more efficient than the
   median on clean planted data and equally robust to a single 3-log-unit burst; adopting it needs its own gate run.
9. Inherited cache limitations (full-era universe membership, full-era FB Spec-B floor) and the gate's survivor-
   conditional, median-consistency nature are unchanged from Rounds 3–4.
