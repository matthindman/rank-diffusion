# Fable 5.1 development experiment log — Round 5 (2026-11-25)

Claude Fable 5.1 (Anthropic). Working copy `common_rankdiff_fable.py` started as a byte-identical
copy of `llm_fitting/common_rankdiff.py` (Round 4 = Round 3 process + Astra `_volatility_posterior`);
verified with `diff` at session start. Interpreter `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`
(pandas 2.2.2, numpy 2.0.1, scipy 1.14.0). Data contact: ONLY the four anonymous DEVELOPMENT `.npz`
caches plus `facebook_a_spec_b.json` in the authorized `panel_cache`. No sealed or exposure-unknown
data opened. All writes confined to `fable_v2/`.

Round 4 baseline archives: `../round4_baseline/` is EMPTY at session start. Round 4 S2 numbers are
therefore taken from Astra's `candidate_frozen_gate_results.json` / `candidate_gold_benchmark_results.json`
(the Astra S2 column IS Round 4 S2: the sign check touched only S3), and Round 4 S3 numbers from
Astra's factorial archive `diag_ablation.json` ("filter only" = Round 3 S3 process + adopted filter,
stored in the `s2_*` slots of that archive). Determinism of the harness on this machine is checked
below before those archives are used as the comparison baseline.

Status vocabulary: RAN-OK / RAN-FAILED / PARTIAL / NOT RUN.

---

## 0. Predictions written before any diagnostic ran

### D1 — Variogram shape: mean-square vs robust scale vs kurtosis by horizon (emp vs S2 world)

Hypothesis H1 (brief §2 opp. 2b): the lasting innovation pool is the pooled standardized 1-week
increment, so simulated h-week lasting sums Gaussianize by the CLT, while the empirical h-week
increments on Submissions keep a rare-jump tail (a few subreddits that change regime). A
homoscedastic lasting step matched to the MEAN-SQUARE variogram then over-predicts the MEDIAN
displacement (gate dRank1/4/13 too high; cov90 too wide) and under-predicts set turnover
(top-10 turnover too low, max X too low).

- P-D1.1: On `reddit_submissions_long`, emp ratio var/robust-var of within-band h-increments
  (top-K bands) exceeds the S2-simulated ratio at h >= 4 and the gap widens with h. Rough size:
  emp 2-4 at h=13 vs sim 1.3-1.8.
- P-D1.2: Gap smaller on `facebook_a`, intermediate on `reddit_comments_ext`, IG unclear (T=52).
- P-D1.3: Empirical excess kurtosis of h-increments decays slower than 1/h on Reddit; sim decays ~1/h.
- P-D1.4 (what must NOT be true for H1): if emp and sim var/robust ratios agree at h=13, H1 is dead
  and the Submissions overshoot must come from the conditional filter / head law instead.

### D2 — Gate decomposition on Submissions (which component drives the median overshoot)

- P-D2.1: Scaling sigma_L by 0.7 in the fitted S2 cuts dRank13 by 20-25%; `innov_mode="gaussian"`
  changes dRank13 by < 10%; v_hat = 1 changes dRank by < 10%.

### D3 — Filter statistic efficiency (planted, IG-like T=52 intermittent)

- P-D3.1: A trimmed mean of |dX_1| (upper 10% trimmed) has lower clean-data log-scale error than the
  median (closer to the raw mean-square estimator) and retains most contamination resistance.

### D4 — Surge estimator pathology (S3)

- P-D4.1: Pooled kappa_5/kappa_3 ratio is dominated by < 1% of entities on both Reddit panels; a
  per-stratum (rank x volatility tertile) averaged kappa_5 lowers the implied mu by >= 30%.

---

## 1. Diagnostics (results appended as they complete)

### D1 — RAN-OK (`diag_variogram_shape.py` -> `.log`, `.json`; S2 fitted on each full panel, one simulated world seed 300, burn 26)

Finding (all four panels, every inside-K band, every horizon 1..52): the simulated S2 world matches the
mean-square variogram (as designed) but its ROBUST scale (IQR^2/1.349^2) is far larger than the data's.
Ratio var/robust-var: empirical 1.5-4.3 and roughly FLAT in h (Subs band 0: 2.03, 2.05, 1.96, 1.96, 1.95,
1.86, 1.46 for h = 1..52); simulated 1.4-1.7 at h=1 decaying to 1.0-1.1 by h=52 (CLT on an iid pool).
Empirical / simulated median |increment| at band 0 (gate head cohort): FB 0.88-0.97, IG 0.82-0.87,
Comments 0.77-0.85, Submissions 0.71-0.79 (h=13: 0.72, matching the gate's dRank13 overshoot 10.6/7.4 = 1.43).
Empirical excess kurtosis of 1-week increments: FB 8.5, IG 6.8, Comments 15, Submissions 178 (pooled);
band 0 (head) is much lighter-tailed than bands 1-2 on Reddit (Subs 49 vs 1132 vs 92). The pool's
excess kurtosis is 0.6-2.8.

Verdicts: P-D1.1 PASS (stronger than predicted: universal, not Submissions-only). P-D1.2 PASS in
ordering (FB smallest gap, Subs largest; IG not "unclear" but intermediate). P-D1.3 PASS. P-D1.4 (kill
condition) not triggered.

Interpretation: a flat var/robust ratio in h is the signature of RARE, LASTING jumps (compound Poisson,
rate ~0.5-1%/wk), not of a fat-tailed weekly innovation (whose sums Gaussianize ~1/h). The bounded,
per-entity-standardized pool (introduced to stop head-share blowups) removes exactly this tail, and the
mean-square NNLS then loads the jump variance onto the diffusive lasting step -> every typical entity
moves too much. This is the common root of: Submissions dRank1/4/13 overshoot, cov90 > 0.90 and PIT SD
< 0.289 on Reddit/FB, too-low top-10 turnover on Submissions/IG (the real set changes come from jumps),
and (via the compensating median-based v_hat shrinkage) the small FB under-prediction after Round 4.

Decision: Track A = estimate the core from the ROBUST variogram with the model's own increment law
accounted for (simulated-moment consistency), zero new scalars. Track D (optional, later) = an explicit
jump component to restore the tail for Goal 1 if Track A lowers turnover too far.

#### Track A predictions (written before implementation)
- Submissions S2 gate 0.263 -> < 0.20; dRank13 10.6 -> 7.4-8.5; cov90 0.924-0.935 -> 0.89-0.92; X CRPS down at all h.
- Comments S2 gate 0.134 -> <= 0.134 (dRank1 6.6 -> ~6.0); IG S2 0.282 -> <= 0.282 (dRank1 17.2 -> ~16).
- FB S2 at risk (already under-predicting dRank after Round 4): 0.084 -> up to 0.11 would not surprise.
- Goal 1: turnover (top10, K/4) falls 10-25% on all panels; boundary ratio stays within [0.95, 1.05];
  head share stable or lower; max X lower. Departure rate/crossing% unchanged (absence law untouched).
- Must-not-worsen: Reddit S3 D sign and decile hook; boundary ratio; departure decomposition.

### D0 — Harness determinism check — RAN-OK (`round4_check/fb_gate_determinism.log`, `.json`)

`benchmark_candidate.py --mode gate --panels facebook_a` on the UNTOUCHED baseline copy reproduces Astra's
archives to every digit on all five splits: S2 0.08425216654612683 (= Astra S2), S3 0.09054511029555012
(= Astra filter-only S3 in `diag_ablation.json`, `s2_rel` slot). Decision: Astra's `candidate_*` (S2) and
`diag_ablation.json` (S3 filter-only) ARE the Round 4 baseline for this report; no separate full rerun needed.

### D2 — RAN-OK (`diag_gate_decomp.py reddit_submissions_long` -> `.log`, `.json`; exact frozen gate machinery, S2, 5 origins x 20 reps)

Fitted Round 4 S2 perturbed diagnostically (never a candidate). Median dRank13 by origin (emp / base / sL0x0.7 / gauss / vhat1):
7/13/10/12/13; 6/10/8/10/11; 8/10/8/10/11; 8/9/7/9/10; 8/11/9/11/13.
- sigma_L x 0.7 cuts dRank13 by 20-25% and dRank4 by ~15% -> P-D2.1(a) PASS: the lasting step is the lever.
- Gaussian pool: dRank unchanged (<= 1 rank) -> P-D2.1(b) PASS: the pool's mild tails are not the issue.
- v_hat := 1 RAISES dRank1 from 4 to 5-6 and dRank13 by ~1 -> P-D2.1(c) FAIL (predicted < 10%): the
  Round 4 median-based filter was already compensating for the inflated scale at the entity level.
RACF1 also moves toward the empirical 0.64-0.71 under sL0x0.7 (0.83 -> 0.79, 0.80 -> 0.75, 0.77 -> 0.70).

### D4 — RAN-OK (`diag_surge_cumulants.py` -> `.log`, `.json`; exact cumulant construction of `_estimate_stage3_surge`)

Share of pooled kappa_5 contributed by the top 0.5% |increments|: FB 0.78-0.86, Comments 0.79-0.88,
Submissions 0.79-0.91 (IG unstable, -0.46..0.51). Implied surge size mu under alternative poolings
(pooled / drop 0.5% worst entities / 9-stratum average / entity-standardized):
FB 1.35 / 1.10 / 1.07 / 1.15; Comments 1.44 / 1.35 / 1.35 / 1.25; Submissions 1.23 / 1.10 / 0.91 / 0.90;
IG 0.78 / unidentified / unidentified / 1.65.
- P-D4.1 first half PASS (kappa_5 is a < 1%-of-observations statistic), second half FAIL/PARTIAL: strata
  averaging lowers mu by 6-26%, not >= 30%. Even the most conservative mu (0.9) leaves a capped single
  arrival of 4.5 log units at rank K. The 5th cumulant is not a usable size moment on these panels;
  any S3 head fix must come from the amplitude LAW (rank attenuation) or a bounded jump law, not from
  re-pooling kappa_5. Parked; S3 surge work resumes after Track A is evaluated.

## 2. Track A implementation (robust scale-moment core) — design record

Edits to `common_rankdiff_fable.py` (all behind `scale_moment`, default "robust"; "variance" is the Round 4
path and is verified bit-identical to `llm_fitting/common_rankdiff.py` on all 8 real-panel fits,
`diag_bitident_quickfit*.log`):
1. `_structure_function(..., robust=True)` returns (IQR/1.349)^2 x n/(n-1) of the same within-band,
   within-week demeaned h-increments (declared moment: typical movement).
2. `_model_increment_ratio(...)`: Monte-Carlo (16384 entities, common random numbers, fixed seed) of the
   model's own h-increment law per band (bounded pool, lognormal v_i drawn as the simulator draws it,
   stationary AR(1), observation noise, Stage 3 surge/drift when active) -> ratio IQRv/var per (band, h).
   The constant 1.349 cancels: the moment condition is literally model IQR_b(h) = data IQR_b(h).
3. Fixed point (3 steps): target_b(h) = D_rob,b(h) / ratio_b(h; current fit) -> NNLS -> new fit.
4. `_empirical_innov_pool_quantiles` factored out (pool needed by the MC); s_vol computed before the fit.

Two design decisions forced by measurement (both logged as conventions, neither chosen on a score):
- Attempt A1 (robust over all h = 1..52, free phi): DEGENERATE on Submissions S2 (V_xi -> 0, a_xi = -6.5)
  and distorted on FB (phi 0.84 -> 0.93). Cause: the empirical robust profile is CONVEX in h at long lags
  (rare jumps entering the IQR; the S3 drift's h^2 term) which non-negative NNLS on {const, saturating,
  linear} columns can only fit by zeroing the saturating (passing) column.
- Attempt A2 (robust over h <= 13 = the D1 plateau, free phi): cures convexity but phi is unidentified on
  13 lags (IG phi 0.93 -> 0.15, FB 0.84 -> 0.72, Subs 0.93 -> 0.45): the short-lag ridge of pitfalls.md.
- ADOPTED A3: phi from the full-horizon mean-square profile (where the AR(1) saturation is visible;
  unchanged from Round 4), the three scale components per band from the robust profile over
  h = 1..ROBUST_H_MAX = 13 with phi pinned. Horizon convention: D1 shows var/IQRv flat through h = 13 on
  all four panels and falling 15-30% by h = 52.

Full-panel fits under A3 (variance -> robust), refine_drift off (`diag_bitident_quickfit_H13_phipin.log`):
- FB S2: sigma_L0 0.1253 -> 0.0761, beta +0.079 -> -0.009, a_xi -0.79 -> -0.80, a_obs -0.83 -> -1.20.
  Band-0 (head) lasting variance unchanged (0.0092 -> 0.0091, jump share 0.01); bands 1-6 jump share
  0.32-0.68. The positive beta of the variance fit is largely rank-dependent jump contamination.
- Comments S2: 0.1135 -> 0.0741 (band jump shares 0.31-0.77, one band negative), a_xi -0.84 -> -1.17.
- Submissions S2: 0.0909 -> 0.0724 (jump shares 0.31-0.48, every band), a_xi -1.01 -> -1.90 (passing
  variance at K down ~6x), a_obs -1.22 -> -1.84 (sigma_obs 0.30 -> 0.16): the 1-week increments are
  extremely heavy tailed (pooled excess kurtosis 178), so all three scales shrink.
- IG S2: on the phi/sigma_L ridge in BOTH fits (T = 52, phi = 0.93 pinned -> passing nearly linear over
  13 lags): lasting variance 0 in 6/7 bands under robust (0.0001-0.0096 under variance with beta at the
  0.5 clip). IG's lasting law is not identified at this length under either moment.
- S3 (all panels): the surge (identified from kappa_3/kappa_5 in variance units, 2 V_s (1 - rho^h)) plus
  drift absorb most or all of the robust-equivalent lasting variance in the head bands (Comments, Subs
  core sL2 -> ~0): S3-robust is effectively "surges + drift + passing" with little diffusion. Flagged;
  evaluated as-is first (one change at a time), budget-guard-in-robust-units is the obvious follow-up.

## 3. Track A evaluation (robust core alone) — RAN-OK (`trackA_gate/gate.log`, `candidate_frozen_gate_results.json`, `gate_compare_full.txt`; `g1_quick.log` 6 seeds)

Exact frozen Gate v1 (5 x 20, boot 2000), Round 4 -> Track A:
- S2: FB 0.084 -> 0.100 (paired +0.016 ± 0.030, better 1/5) | IG 0.282 -> 0.271 (-0.011 ± 0.059, 2/5; 100% cov, 5/5 vs persistence)
      | Comments 0.134 -> 0.177 (+0.044 ± 0.043, 1/5) | Submissions 0.263 -> 0.167 (-0.096 ± 0.034, better 5/5; at par with 0.165 persistence, 3/5 wins).
- S3: FB 0.091 -> 0.107 | IG 0.304 -> 0.274 | Comments 0.124 -> 0.130 | Submissions 0.198 -> 0.266 ± 0.241 (one split blew up).
Mean moments (emp / R4 S2 / A S2): FB dRank1 13.6/13.0/12.2, dRank4 19.5/18.2/17.8, RACF1 .381/.382/.418 -> deeper
under-prediction; IG dRank1 15.4/17.2/15.0, dRank4 18.6/19.4/17.2, coll1 .317/.362/.328 -> fixed; Comments dRank1
6.0/6.6/6.6, dRank4 8.6/8.8/8.0, RACF1 .615/.641/.522 (ranks now too noisy relative to slow movement), coll1
.046/.025/.006; Submissions dRank1 3.6/4.2/4.0, dRank4 4.8/6.4/5.4, RACF1 .679/.781/.682 (exact), coll5 .546/.541/.539.
Prediction score: Submissions (PASS, 0.167 < 0.20), IG (PASS), Comments (FAIL: predicted <= 0.134), FB (as feared).

Goal 1 quick (6 seeds; R3 -> A), S2: departure rate FB .044 -> .023 (emp .033), IG .112 -> .098 (.138), Comments
.018 -> .006 (.016), Subs .015 -> .004 (.012); turnover K/4 FB .212 -> .162 (.237), Comments .122 -> .090 (.162),
Subs .141 -> .104 (.162); top-10 Subs .233 -> .133 (.300); rank-1 change FB .459 -> .306 (.518), Subs .047 -> .005
(.057); dep imm% Comments .927 -> .809 (.989). Head share blocks stable, boundary 0.97-0.99, max X lower.
S3: IG head structure much better (top10 .333 -> .283 vs .267; rank-1 .755 -> .314 vs .392; head share .396 -> .423
vs .419); Reddit out-of-moment rho now exact (Comments .104 -> .160 vs .145; Subs .088 -> .151 vs .155), D
Comments .149 -> .205 (.132, overshoot), Subs .163 -> .205 (.222), decile hook preserved.

Interpretation (the central finding of this pass): rank displacement and boundary departures are COLLECTIVE
statistics -- an entity is displaced when neighbours cross it, and rare jumpers cross many ranks. Matching
typical (IQR) movement removes exactly the jump variance that was standing in, inflated, for those crossings.
Round 4 got the gate roughly right by moving every typical entity too much (bad for CRPS/cov90); Track A gets
the typical entity right and under-predicts crossings, departures, K/4 turnover and rank-1 change on every panel.
Neither is adoptable alone. The jump variance must be put back where it belongs: as a rare, permanent,
rank-attenuated event process (Track D / S2+J), with the diffusive core robust.

Design S2+J (written before coding): lasting step = sigma_d(L) eta + B_t J_t, B_t ~ Bern(lambda_J), J_t Laplace
with Var(J|z) = c_J s_sc(z)^2 (rank attenuation inherited from b_xi exactly as the surge; 0 scalars);
lambda_J c_J set so the band-weighted mean jump variance equals the band-weighted mean lasting gap
(mean-square fit minus robust core; declared moment, 0 scalars); lambda_J profiled on {0.005, 0.01, 0.02, 0.04}
by the fit of the model's IQR profile to the data's over ALL h = 1..H (the long-horizon bend identifies how rare
the events are, exactly as phi is profiled on the mean-square profile); jumps inactive when the gap share
< 5% of lasting variance (then 11 scalars). S2+J = 12 scalars. Predictions: departures/turnover/rank-1 change
recover toward Round 3 levels; Submissions gate stays < 0.20; FB/Comments gate return toward Round 4
(dRank13 up, RACF1 up); Goal 2 cov90/CRPS keep Track A's gains; head share stable (Laplace 6-scale event at
the head ~1.5 log units once per ~1,000 weeks for a top entity).

## 4. S2+J implementation record — five estimator variants, one kept (`diag_quickfit_jumps*.log`)

All variants share: Laplace permanent jumps on L, rank attenuation inherited from b_xi via `_surge_scale_of`,
jump variance entering the Kalman transition variance, `_volatility_posterior`, the closed-form Atlas drift
and `_model_increment_ratio`; jumps only in the core model (asymmetry=False), S3's tail process is its surge.
- v1 (per-jump variance recomputed each fixed-point step from the moving robust core; lambda profiled on
  {.005,.01,.02,.04} by the all-h IQR-level SSE): RUNAWAY. Bands where the inherited law puts more jump
  variance than their true gap push the core to the NNLS floor, the gap then equals the full lasting variance
  and feeds back (FB core -> 0 in 5/7 bands, lambda -> grid edge). Rejected.
- v2 (gap fixed once from the jump-free robust core; same lambda profile; strict "< sse0"): stable cores;
  lambda FB .02, Comments .005, Subs .04, IG .04; S3 Reddit chose lambda = 0. BUT jumps also activated on
  planted jump-free panels (gap-share noise 0.15-0.17 with 3-9-entity head bands) and the planted jump
  variance was recovered at only 42% (jumps inflate the h <= 13 IQR when lambda*13 ~ .13).
- v3 (lambda by the dimensionless log var/IQR ratio profile over h > 13, damped gap iteration): erratic
  profiles (FB 45/10/26/10/14), Comments core eroded to sL0 .033. Rejected.
- v4 (IQR-level SSE on h > 13 with one damped gap update): cores eroded again (Comments all bands 0),
  Subs flipped to no jumps. Rejected.
- v5 (v2 criterion + 25% SSE-gain margin + 10% share): the SSE criterion is unreliable BOTH ways -- fires
  on jump-free small-band panels, misses a planted lambda = .01 panel (gain 2%), and at lambda = .01 fixed
  it would have rejected jumps on both Reddit panels. Rejected.
- KEPT: lambda_J = 0.01/wk fixed cross-platform (D1: var/IQR flat through h = 13, down 15-30% by h = 52 on all
  four panels => lambda*52 ~ 0.3-0.7; same status as DRIFT_HL_FIXED = 52 from the D3 gap-decay diagnostic);
  per-jump variance from the band-weighted lasting gap of the jump-free robust core (one counted scalar);
  activation iff the gap carries >= 20% of total weekly lasting variance (dev panels .39-.98; planted
  jump-free noise .15-.17; planted 64%-share panel .24). The all-horizon IQR-profile SSE is kept in diag only.
  Known biases to carry into the report: (i) gap understated ~40-60% when planted lambda*13 is not small;
  (ii) inherited rank law misallocates jump variance across bands (FB head gap ~0 vs law .0025; Reddit head
  gap comparable to body); (iii) the rate is a convention, not an estimate.
Final full-panel fits (refine_drift off): FB S2+J sL0 .0776 (core bands .005-.008), jump sd .83 @K, var/wk
.0069; IG S2+J sL0 .070 (ridge), jump sd .70; Comments S2+J sL0 .085, jump sd .59, var/wk .0034; Subs S2+J
sL0 .117 (beta .159), jump sd .62, var/wk .0038. Scalars: S2 12 on all four; S3 14/14/15/15 (jumps off).
Test suite: 23 tests (15 inherited + 8 Round 5).

### D3 — Filter statistic efficiency — RAN-OK (`diag_filter_stat.py` -> `.log`, `.json`; planted panels N=3000, K=1500, 3 seeds, same 41-point posterior inversion for every statistic)

Mean |log(v_hat / v_true)| over top-K entities (median = Round 4 statistic; trim10 = upper-10%-trimmed mean |dX_1|; mean = plain mean |dX_1|):
- T=52 (IG-like, 7.7%/wk absence), clean: median 0.291 | trim10 0.225 | mean 0.194.
- T=52, contaminated (one +/-3 log-unit week per entity): 0.454 | 0.395 | 0.410.
- T=100, clean: 0.216 | 0.167 | 0.145.   T=100, contaminated: 0.302 | 0.252 | 0.249.
Verdict P-D3.1 PASS and more: in this design the median is the least efficient statistic in BOTH regimes; the
L1 mean |dX_1| (bounded influence 3/n per burst, unlike the mean square) is the most efficient on clean data and
ties the trimmed mean under contamination. NOT adopted this round (one change at a time; it would alter the
filter on every panel and needs its own gate run). Recorded as the cheapest next improvement for the IG
X-CRPS cost (+0.0014-0.0020) noted in the brief.

### D1 re-run on the final S2+J world — RAN-OK (`final/variogram_shape_s2j.log`, `.json`; one seed as before)

Empirical / simulated median |increment| (band 0 = head; pooled inside-K), Round 4 -> S2+J:
- FB band 0 h=1,4,13,26: .88,.94,.97,.96 -> .92,.98,.98,.99; pooled .81-.87 -> .93-.99.
- IG band 0: .82-.87 -> .79-.88 (unchanged); pooled .89-.92 -> .94-.99. Simulated head VARIANCE now exceeds the
  data's (h=26: .757 vs .637): the inherited rank law places too much jump variance at the IG head.
- Comments band 0: .83,.83,.77,.81,.85 -> .92,.94,.89,.88,.85; pooled .89,.86,.80,.79,.82 -> 1.03,1.04,.98,.95,.93.
- Submissions band 0: .79,.72,.72,.75,.82 -> .89,.91,.90,.90,.89; pooled .61-.73 -> .94,.88,.80,.77,.75.
Typical movement is now within ~10% at the head on all four panels (was 15-40% too large). The simulated var/IQR
ratio is elevated and no longer collapses to 1 (FB band 0 at h=52: 1.45 vs emp 1.28) but remains below the data's
at short lags (pooled h=1: FB 1.62 vs 2.46, Subs 1.59 vs 4.28): the PASSING/observation tails of the data are
heavier than the bounded pool's, which permanent lasting jumps cannot represent. On Reddit the simulated total
variance at long h is below the data's (Subs pooled h=13: .214 vs .366): the one-shot gap understates the jump
variance there, consistent with the planted-panel bias. Open items, not fixable in this pass.

## 5. Commands, seeds and status

Run from `fable_v2/` with `PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`, `PYTHONDONTWRITEBYTECODE=1`,
under `caffeinate -dims`. Gate seeds 1000*split + 0..19; gold structure seeds 300..319; forecast members 2000*split + 0..38;
OOP seeds 7000..7019; Monte-Carlo ratio seed 4242 (16384 entities); volatility posterior seed 71329 (8192 draws).

| Step | Command | Status |
|---|---|---|
| D0 determinism | `$PY -u ../benchmark_candidate.py --module common_rankdiff_fable.py --out-dir round4_check --mode gate --panels facebook_a` (untouched copy) | RAN-OK, bit-identical to Astra |
| D1 | `$PY -u diag_variogram_shape.py` | RAN-OK |
| D2 | `$PY -u diag_gate_decomp.py reddit_submissions_long` (Round 4 module state) | RAN-OK |
| D3 | `$PY -u diag_filter_stat.py` | RAN-OK |
| D4 | `$PY -u diag_surge_cumulants.py` | RAN-OK |
| Bit-identity + quick fits | `$PY -u diag_bitident_quickfit.py`; `$PY -u diag_quickfit_jumps.py` | RAN-OK |
| Track A gate (robust core only) | `$PY -u ../benchmark_candidate.py --module common_rankdiff_fable.py --out-dir trackA_gate --mode gate` (snapshot `trackA_gate/common_rankdiff_fable_trackA_snapshot.py`) | RAN-OK |
| Track A / S2+J structure previews | `$PY -u diag_g1_quick.py common_rankdiff_fable.py <out.json> 6` | RAN-OK (6 seeds, preview only) |
| Signed gate moments, final module | `$PY -u diag_gate_decomp.py instagram_hm` / `facebook_a` -> `final/gate_decomp_*.log` | RAN-OK |
| D1 on final world | `$PY -u diag_variogram_shape.py` -> `final/variogram_shape_s2j.*` | RAN-OK |
| Unit tests (final) | `$PY -m pytest test_common_rankdiff_fable.py -v -p no:cacheprovider` -> `final/pytest_final.log` | RAN-OK 23/23 |
| MANDATORY full benchmark | `$PY -u ../benchmark_candidate.py --module common_rankdiff_fable.py --out-dir . --mode both` -> `final/benchmark_full.log`, `candidate_frozen_gate_results.json`, `candidate_gold_benchmark_results.json` | see below |
| Report tables | `$PY diag_report_tables.py candidate_frozen_gate_results.json candidate_gold_benchmark_results.json > final/report_tables.md` | see below |

Not run: `scripts/health_check.sh` (its legacy guard reads a panel outside the authorized cache). No sealed or
exposure-unknown data opened. No file outside `fable_v2/` written (the harness writes its `__pycache__` only when
PYTHONDONTWRITEBYTECODE is unset; it was set for every run).

## 6. Mandatory full benchmark — RAN-OK (`final/benchmark_full.log`; gate 20:17-20:54, gold 20:54-21:29; oracle cov90 .8985, PIT SD .2895)

Gate v1, Round 4 -> candidate (S2+J / S3-robust): FB S2 .084 -> .093 ± .031 (60%, 5/5; paired +.009 ± .030), S3 .091 -> .107;
IG S2 .282 -> .344 ± .216 (100%, 5/5; dRank1/4 15.0/17.6 vs emp 15.4/18.6 but RACF1 .069 vs .019 at MOM_FLOOR), S3 .304 -> .274;
Comments S2 .134 -> .096 ± .028 (80%, 5/5; paired -.037 ± .071), S3 .124 -> .130; Submissions S2 .263 -> .196 ± .103 (60%, 3/5;
paired -.068 ± .074), S3 .198 -> .266 ± .241. Full moment, structure, Goal 2 and OOP tables: `final/report_tables.md`.
Prediction scores for S2+J (written in §3): departures/turnover/rank-1 change recovered toward Round 3 (PASS: FB dep .035 vs
.033, Comments .017 vs .016; FB top-10 overshoot .45 FAIL); Submissions gate < .20 (PASS, .196); FB/Comments gate back toward
Round 4 (PASS: FB .093, Comments .096 better than R4); cov90/CRPS gains kept (PASS on Comments; FB cov90 fell below .90, FAIL
in direction); head share stable (PASS: FB .242/.242, IG .422, Comments .574, Subs .492). S3 (unchanged from Track A) worse than
Round 4 on the gate (3/4) and on Comments Goal 2; Reddit rho/D exact out of moment.

lambda_J sensitivity (6 seeds, `final/g1_quick_lam0005.log`, `final/g1_quick_lam002.log`): 0.005 damages the FB head (top10 .60,
max X 18.4, boundary 1.19); 0.02 gives boundary .99-1.04 on all panels and FB top10 .325. Not acted on (would select the
constant on Goal 1 scores). Recorded in FABLE_REPORT §5.5.

Verdict written to FABLE_REPORT §1: partial accept as a development option (S2+J for the core; keep Round 4 S3). Nothing adopted.
