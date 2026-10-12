# Rank-Diffusion Model — Status & Locked-In Results

_Canonical record of the validated FB / Reddit / IG-rescue results and the model as written in code._
_Last updated: 2026-07-11 (§1 revised to the current model + explicit conditioning
assumption; research record through the latest §2 section). Newest §2 sections
supersede older ones._

## 1. Unified model (one structure, platform-specific parameters)

_(§1 revised 2026-07-11 — the previous §1 text described the pre-§2d structure
[RW/AR(1) home, no temperament, integrated factor]; the §2 history preserves
that evolution. This is the model as written in code today, per the 2026-07-11
external-review adjudication.)_

Observed log-activity of endpoint *i* at week *t*:

```
X_it = h_it + ξ_it (+ ξ2_it) + ε_it     — components scaled by ONE persistent
                                          entity amplitude v_i (lognormal, spread s;
                                          b = 1 factorization is the main law)
  h_it : OU "home" — reversion κ(z) toward THE ENTITY'S OWN home level,
         innovation σ_perm(z)                (κ, σ_obs MD-identified, §2d/§2i)
  ξ_it : fast transitory AR(1) — φ, σ_trans(z); Student-t innovations (--t-tails)
  ξ2   : optional medium AR(1) (--two-scale; D(h)-identified, long panels)
  ε_it : measurement noise σ_obs(z) — identified in shape everywhere, in level
         at the FB head, bounded in [centered floor, Spec-A] elsewhere (§2r)
```
- **Rank** each week by the *observed* X; **stationary common level** applied at
  observation (`--stat-factor`, ρ_L measured; §2j); **Gabaix rebirth** at the bottom;
  all on a **pre-registered top-coverage universe** (top-K, buffer B = 4K,
  absence-penalized permanent-rank membership; §2b).
- **Rank-dependence**: parameters vary by **permanent-rank band** (Lagrangian
  knots, sparse head knots pooled — immune to current-rank selection bias).

**EXPLICIT CONDITIONING ASSUMPTION (2026-07-11, review adjudication).** The
model is **entity-home OU with rank-dependent variances**, not reversion to a
rank-conditional level: each entity has a persistent home, and the
cross-section of homes is the MEASURED stationary ladder (simulators seed
homes from the period-0 ladder `w0`; the estimated `T_curve` is a diagnostic,
read by no simulator). Ladder stationarity is the universal stylized fact of
these systems (Zipf/Gabaix) and is taken as given — the ONE taken-as-given
input; everything else is measured. Goal-1 claims are therefore
**conditional reproduction + MAINTENANCE of the ladder** (the sim can still
drift or over-concentrate — the stationary-head-law residual, §2z-a/§2z-b, is
exactly such a measured failure, so the maintenance test has teeth). Ladder
GENESIS is out of scope (Gabaix explains it); the contribution is the
identified dynamics around it.

**Movement-gate glossary (binding language, 2026-07-11):** the OOS gate is a
**pooled-moment movement gate** (dRank medians at h=1/4/13, RACF1, coll1/5/20)
against a **train-window historical-mobility baseline** ("persistence" = the
train movement distribution asserted for the test window, not a no-change
forecast); displacement is **survivor-conditional** (both endpoints observed —
exits are tested separately by the boundary-flux rows); "bootstrap-CI
coverage" = the model median inside the empirical median's 95% sampling band
(NOT predictive coverage — PIT/CRPS via `--dist-scores` are the calibration
statistics); p90/Wasserstein are descriptive. Full-distribution and
identity-specific movement claims are NOT made. Parsimony language:
**parsimonious latent architecture with flexible nonparametric rank profiles**
(~500 moment-estimated band values across ~55 knots; never "a simple model"
without the qualification).

This is one generative law. Platforms differ in **parameters and measurement
regime, not theory**.

## 2. Locked-in results

| | Facebook (T=88) | Reddit (T=30) |
|---|---|---|
| In-sample goal-1 (15-metric) | **15/15** (Kalman params, ρ_perm≈0.90) | 13–14/15 |
| In-sample goal-2 churn error | **0.046** | 0.05 (obs_frac=0) / 0.10 (default) |
| **OOS movement** (rolling-origin, distributional) | **NOT yet robust** — see below | worse (T=30) |
| home-drift evidence (Kalman LR drift>fixed) | strong (213–594) | weak-but-consistent (6–63) |

**OOS movement is the acceptance gate** — now **rolling-origin + distributional** (`--oos`):
≥5 train/test splits; per split estimate the variance partition on TRAIN, calibrate one
`sigma_obs_scale` on the TRAIN **moment vector** (dRank1, dRank4, coll1, coll5, RACF1), then
predict the held-out displacement **distribution** (median, p90, Wasserstein, bootstrap-CI coverage).

**Honest FB verdict (5 splits):** the model does **NOT yet robustly pass**. Model rel err
**0.29 ± 0.16** vs persistence **0.15 ± 0.02**; `sigma_obs_scale` is **unstable across windows
(0.15–0.35, median 0.25)**; bootstrap-CI coverage only ~40% of splits; the model **under-disperses
the displacement tail** (p90). It **improves with more training data** (Wasserstein 38→17 across
origins; later splits land in-CI) — pointing to longer panels. A single 67/33 split gave a
flattering 0.081 ≈ 0.070; that was the best split, not the typical one. **Do not report single-split
numbers.**

### Observation noise (the central open problem)
`σ_obs` is the lever on observed rank movement; the pooled change-autocovariance over-states it
~2× for clean top entities, and **calibrating it on training displacement is not stable enough
across windows** to call the gate passed. Status: **calibrated, NOT yet identified.** The decisive
next step is to **identify σ_obs from an independent signal** (daily-within-week residual variance —
noise floor only, not a daily dynamics model; later, replicate measures) and report it as a second
specification (Spec B) alongside the train-calibrated one (Spec A); if B ≈ A's scale, the
observation model is validated. Then re-run this rolling-origin distributional gate.

## 2b. 2026-07-02 — Top-coverage universe (Reddit tractable end-to-end; estimand sharpened)

**Motivation.** The estimand is the macro distribution of attention; Reddit's uncapped panel
(~200k subreddits/wk) is dominated by a measurement-degenerate tail: 60% of the panel has weekly
karma ≤ 5; at rank 50k there are 620-way ties, at rank 100k 10,640-way ties. Tail "rank movement"
is tie-breaking noise. FB's CrowdTangle panel is top-truncated at source (~14.4k pages/wk), so the
uncapped Reddit panel was also a hidden cross-platform asymmetry. Full analysis + concentration
and boundary-flux tables: research_notes.md §3 ("Reddit / FB top-coverage universe").

**Design (in `minimal_rankdiff.restrict_universe`; tests in `tests/test_universe_restriction.py`).**
- Pre-registered coverage rule (`COVERAGE_K`, from concentration stats alone): Reddit K₈₀=2,500
  (80.0% of weekly karma), K₉₀=5,000 (89.2%), K₉₅=10,000 (95.6%); FB K₉₀=3,500.
- Closed Lagrangian universe: the B=4K entities with the best ABSENCE-PENALIZED permanent rank
  (absent weeks at the observation floor N_t+1). Buffer multiple 4 from the empirical excursion
  depth (p99 of drop-landings ≈ 4K). Membership computed on the TRAIN window only inside the OOS
  gate (`member_window=T0`). All member observations retained (no censoring — 99.93% of top-K
  droppers remain observed in the full panel); weekly ranks recomputed within the universe.
- Quantified losses: true weekly disappearance 0.07%; top-K entrants from unobserved 0.4–0.7%;
  train-defined closed set covers 97–98% of future weekly top-K at B=4K.
- Boundary flux is a TESTED PREDICTION: new scorecard rows outfluxK (weekly out-rate from top-K)
  and return4K (4-wk dropper return), computed identically for empirical and simulated ids.
- Estimand-faithful scoring: goal-1 metrics on tracked entities with time-mean rank ≤ K only
  (emp and sim identically); persistence-set size = 1% of K, not of the buffer.

**Corrected pitfall (locked by test):** universe membership by observed-week mean rank re-admits
Eulerian selection — 1–2-week spikers entered, pooled alone in the deepest knot, inflating its
exit rate to 0.60/wk (true: 0.0007/wk) → runaway diffusion (reddit K=2500 scored 4/15, sim dRank1
80.6 vs emp 13). The absence penalty fixes this at the membership stage.

**In-sample results (5 reps; knob settings declared per row):**

| run | obs_frac | goal-1 | churn err | boundary flux (emp→sim) |
|---|---|---|---|---|
| Reddit uncapped (baseline) | 0.4 | 13/15 | 0.151 | — |
| Reddit K=2,500 B=10k | 0.4 | 10/15 | 0.140 | out 0.099→0.113, ret 0.350→0.383 |
| Reddit K=5,000 B=20k | 0.4 | 11/15 | 0.123 | out 0.095→0.114, ret 0.369→0.391 |
| Reddit K=5,000 B=20k | 0.0 | 12/15 | **0.069** | out 0.095→0.113, ret 0.369→0.391 |
| Reddit K=10,000 B=40k | 0.4 | 12/15 | 0.167 | out 0.092→0.110, ret 0.392→0.400 |
| FB full panel (non-regression) | 0.4 | **14/15** | **0.013** | — |
| FB K=3,500 B=14k | 0.4 | 9/15 | 0.016 | out 0.173→0.131, ret 0.354→0.410 |
| FB K=3,500 B=14k | 0.0 | 9/15 | 0.018 | out 0.173→0.130, ret 0.354→0.408 |

Buffer invariance at Reddit K=5,000 (B ∈ {2K, 4K, 8K}): goal-1 11/11/10; RACF1_sim
0.464/0.467/0.463 and boundary flux invariant; coll1_sim (0.68–0.87) and dRank1_sim (17–23) show
residual B-sensitivity traceable to unstable 1–2-entity head knots (head σ_obs estimate swings
2.03 vs 0.46 across universes) — see "what the universe surfaced" below.

**OOS movement gate (rolling-origin, distributional; σ_obs grid extended to 0.0 after Reddit
pinned at the old 0.15 grid edge):**

| platform | model rel err | persistence | scale by split | CI coverage |
|---|---|---|---|---|
| Reddit K=2,500 | 0.302 ± 0.105 | 0.171 ± 0.004 | 0.0–0.10 | 0% (1 split beats persistence) |
| Reddit K=5,000 | 0.336 ± 0.055 | 0.168 ± 0.004 | 0.0 ×5 | 0% |
| Reddit K=10,000 | 0.389 ± 0.033 | 0.167 ± 0.004 | 0.0–0.10 | 0% |
| FB full (reference, new grid) | 0.276 ± 0.146 | 0.146 ± 0.022 | 0.0–0.35 | 40% |

Note the in-sample/OOS scissors across K: the aggregate in-sample score IMPROVES with deeper K
(10 → 11 → 12 of 15) while OOS movement WORSENS monotonically (0.302 → 0.336 → 0.389) — the
in-sample card is diluted by the broader population while the OOS cohort stays pinned on the
head, where the over-dispersion lives. This is the single-split/in-sample-overclaim pitfall in
miniature and is why the OOS gate remains the acceptance criterion.

The Reddit gate previously HUNG (full-N cohort sim); it now runs end-to-end in minutes. Reddit is
no longer categorically worse than FB — same regime, same failure signature.

**What the universe surfaced (the honest headline).** Scored on the estimand population, both
platforms show one coherent failure: the model over-disperses the head — sim RACF1 ~0.09–0.15 too
low, top-rank collisions and dRank over-predicted, OOS displacement over-predicted even at
calibrated scale 0.0. Setting obs_frac 0.4→0 barely moves RACF1 (0.467→0.476): the excess head
mixing lives in the estimated variance partition itself, not the iid/AR split. The previous
"13–14/15" scores averaged this away over the mid/tail population. Priority unchanged and
sharpened: identify σ_obs independently (Spec B, daily-within-week noise floor) and make the head
bands robust (hierarchical/entity-level σ_obs; pool the 1–2-entity top knots).

**Reproduction:**
```
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000            # or --coverage 90
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --buffer-mult 8   # invariance check
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000       # OOS gate, train-only membership
python llm_fitting/minimal_rankdiff.py facebook --top-k 3500          # FB symmetric protocol
```

## 2c. 2026-07-02 — Temperament: persistent entity-level volatility heterogeneity

**Diagnosis.** Scored on the estimand, both platforms failed one way: the head over-dispersed
(RACF low, top collisions/dRank high, OOS displacement over-predicted even at σ_obs scale 0).
Direct measurement on head entities (perm rank ≤ 500): per-entity change-variance dispersion is
**15× (Reddit) / 35× (FB)** pure χ² sampling noise; split-half log-variance Spearman ρ ≈ **0.6**
(persistent trait, not episodic); pooled excess kurtosis (4.8 / 8.0) collapses within-entity
(0.5 / 2.2) — the "heavy tails" are **variance mixing across entities**. The head is a quiet
persistent core + volatile fringe (FB's flat top-35 retention 20/19/20 across h=1/4/13 is the
fingerprint — homogeneous σ cannot produce it). Gap structure is NOT the problem (FB sim/emp
head steepness 1.229 vs 1.254).

**Model change (one parameter).** σ_i = σ(z̄_i)·√v_i, log v_i ~ N(−s²/2, s²), E[v_i]=1 — band
variance and the Eulerian structure preserved by construction. **Estimator** (`estimate_temperament`):
log-variance moment decomposition (Smyth-2004/limma digamma–trigamma χ² corrections) with
Satterthwaite effective df for the MA structure of weekly changes (κ=1.34 both platforms).
Identified from the variance-dispersion moment ONLY — never tuned to churn/displacement.
**Estimates: Reddit s = 0.941, FB s = 0.887** — nearly identical across platforms and flat across
all rank bands (0.84–0.99) ⇒ one global s; σ_i p90/p10 ≈ 3.3×, matching the direct measurement.
Companion fix: adaptive sparse-knot pooling (`--min-knot-entities 8`) — the 1–2-entity head knots
had let single volatile entities set band moments (head σ_obs 2.03 → 0.10 pooled).

**In-sample (estimand-faithful, obs_frac defaults, 5 reps):**

| Reddit K=5,000 | base | +pool | +temper | **+pool+temper** |
|---|---|---|---|---|
| goal-1 / churn err | 11/15 / 0.123 | 11/15 / 0.069 | 12/15 / 0.120 | **12/15 / 0.063** |
| coll1 / coll2 diff | +0.269 / +0.262 | +0.062 / +0.083 | +0.228 / +0.234 | **+0.048 / +0.014** |
| RACF1 diff | −0.119 | −0.117 | −0.057 | **−0.057** |
| dRank1 / dRank4 diff | +4.2 / +4.0 | +2.3 / +1.6 | +3.4 / +2.8 | **+2.0 / +0.8** |

Pooling fixes the head-knot means; temperament fixes the mixture; complementary, not redundant.
FB K=3,500 +pool+temper: RACF1 −0.093→−0.019, RACF4 now passes, dRank1 +5.6→+2.8; cost: VR8/13
inflate (composition shift; see scope note). Remaining misses: Reddit RACF4 (−0.094), RACF13
(−0.081), R2_4/R2_13 on FB/Reddit; boundary flux stays matched everywhere.

**OOS movement gate (temper+pool, movement-only scaling):**

| | before | **after** |
|---|---|---|
| Reddit K=5,000 | 0.336 ± 0.055, cov 0%, scale 0.0×5 | **0.254 ± 0.091, cov 40%**, scale 0.0 (2 splits beat/tie persistence) |
| FB | 0.276 ± 0.146, cov 40%, scale 0.0–0.35 | **0.243 ± 0.114, cov 60%, scale 0.25–1.0 (late splits 1.0)** |

The FB scale result is the pre-registered signature: with temperament, the best-trained splits
need **no σ_obs correction at all** (scale = 1.0) — the observation model approaches
self-consistency. `temper_s` is stable across every train window (FB 0.91–0.98, Reddit
0.95–0.96). Neither platform fully passes yet; Reddit still over-predicts at h=4.

**Scope decision (A vs B), decided by evidence, not the scorecard.** The s(h) horizon moment —
s measured from non-overlapping h-week changes — is **flat in h** (FB 0.86/0.86/0.86/0.90/0.88 at
h=1..13), so heterogeneity extends to the permanent component (structure B). But naive full-process
scaling explodes Reddit's held-out RW displacement (OOS 0.404 vs A's 0.254): a fat lognormal tail ×
short-window σ_perm estimates. FB (more train data) shows B beating persistence on its two
best-trained splits (0.111, 0.152). **Operational spec = A (movement-only)**, per the pre-declared
gate criterion; B is the target structure pending an EB-shrunken/lighter-tailed mixing
distribution and the longer Reddit panel.

**Alternative hypothesis tested and REJECTED — "just use finer rank bands"**
(`llm_fitting/temperament_vs_finebands.py`; both platforms, 2026-07-02). If the within-band
dispersion were an unresolved smooth σ(rank), (A) residual spread would vanish as bands shrink —
observed: plateaus (Reddit 0.941 @ 10 bands → 0.932 @ 2,000 bands; FB 0.888 → 0.883; a 200×
refinement explains ~1–2% more); (B) the log-variance variogram would be ~0 at adjacent ranks —
observed: flat at s from Δr=1 (Reddit 0.93 = 0.94 @ Δr=100; FB 0.88 = 0.89; impossible under any
deterministic σ(rank)); (C) an entity that changes rank would adopt the new rank's σ — observed:
movers keep their own (split-half residual ρ = 0.52 after conditioning each half on its own fine
k-NN rank curve; H-fine predicts ≈ 0); (D) predicting an entity's future variance from its exact
rank loses badly to its own shrunken history (MSE 1.143 vs 0.698 Reddit; 1.007 vs 0.676 FB).
Fit-side corroboration: the top of the knot grid was already per-rank fine, and that fineness was
the pathology (pooling it away improved in-sample AND OOS). Volatility is a property of the
entity, not the rank. Honest refinement note: observed split-half ρ (0.63) is below the pure
time-invariant-temperament benchmark (0.79 Reddit / 0.92 FB) ⇒ v_i evolves slowly; a
slowly-mean-reverting temperament is a future refinement (cf. Hospido 2012), not H-fine support.

**Reproduction:**
```
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --temperament --min-knot-entities 8
python llm_fitting/minimal_rankdiff.py facebook --top-k 3500 --temperament --min-knot-entities 8
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament --min-knot-entities 8
python llm_fitting/rankdiff_kalman.py facebook --oos --temperament --min-knot-entities 8
python llm_fitting/temperament_vs_finebands.py reddit facebook   # H-fine rejection battery
```

## 2d. 2026-07-02 — MD covariance estimator (OU home): Reddit passes the OOS gate criteria

**Diagnosis.** The change-autocovariance function has a persistent NEGATIVE tail at lags 3–6
(Reddit −0.006/−0.024/−0.032; FB −0.021/−0.028, head ≤ 500, normalized by γ0) that a
random-walk home cannot produce (RW changes are white). The estimator assumed RW and forced the
tail into the transitory/noise split while the simulator applied a hand-set κ=0.15 on top —
an estimator/simulator inconsistency. Summed, the tail cuts ~1.1·γ0 from 13-week change
variance: first-order at exactly the horizons (h ≥ 4) where OOS over-predicted.

**Change (net parsimony GAIN).** `--md-lags 6`: minimum-distance fit of γ0..γ6 per knot
(Chamberlain / Abowd–Card covariance-structure estimation) to OU-home + AR(1)-transitory +
iid-noise. Estimates κ(z) from the tail (hand-set κ retired) and σ_obs from the covariance
structure (obs_frac unused on this path). Also `--t-tails`: unit-variance Student-t transitory
innovations, df from the median within-entity excess kurtosis (a moment temperament cannot
produce; FB 1.23 → df 4.3, Reddit 0.17 → df 6.7). Tests: exact + simulated-panel MD recovery.

**Rejected after measurement (parsimony defended):** a common time-varying volatility factor —
Reddit's weekly cross-sectional change volatility is flat (0.94–1.09, log-SD 0.034) straight
through the 2024 US election; train/test volatility ratios 1.00 at every OOS origin.

**Results (stack = universe + temper + pool + md6 + t-tails):**

| | in-sample goal-1 | churn err | OOS rel err (persistence) | CI coverage | scale |
|---|---|---|---|---|---|
| Reddit K=5,000 | **14/15** (only R2_13 fails) | 0.074 | **0.171 ± 0.017** (0.168 ± 0.004) | **100%** | 0.25–1.0 interior |
| FB K=3,500 | 7/15 (see caveat) | 0.079 | **0.158 ± 0.027** (0.146 ± 0.022) | 60% | 0.25–1.0 interior |

Reddit: dRank1/4 in-sample +0.2/+0.6; held-out dRank1 median EXACT (6 vs 6), p90 24 vs 27;
Wasserstein 1.0–3.3 (was 6–10); estimated κ(z) = 0.005 (head) → 0.04 (tail); σ_obs head 0.03.
Every split's model error sits on the persistence baseline (0.140–0.190 vs 0.162–0.172), one
split beats it. **Reddit satisfies the distributional gate criteria for the first time — at par
with, not yet beating, persistence.** FB OOS: 2 of 5 splits beat persistence outright (0.133 vs
0.138; 0.138 vs 0.173); failures concentrate in nothing — all five splits ≤ 0.210.

**FB in-sample caveat (weak identification, documented — do not spec-fish).** On FB the raw MD
fast split lands on φ=0.4 / σ_obs≈0.03 at the head and over-persists every head metric (RACF1
+0.13, coll1 −0.16): as φ→0 an AR(1) transitory is observationally equivalent to iid noise
(design columns collide), so the fast split is weakly identified from weekly covariances.
Attempted resolutions — smallest-φ tie-break, largest-σ_e tie-break, hybrid (MD slow side +
obs_frac fast side) — were each tried and REJECTED: each reshuffles the degenerate surface
differently per platform without fixing FB (its κ_head estimate is also tail-noise sensitive),
and iterating tie-breaks against scores is spec-fishing. The declared resolution is EXTERNAL
identification: **Spec-B, σ_obs from the daily-within-week noise floor**
(`data/reddit/reddit_daily.parquet` exists) — now unambiguously the next work item. Note the
OOS gate already resolves the split empirically per split (train-calibrated scale, interior
0.25–1.0 on both platforms), which is why FB OOS is strong while FB in-sample raw-MD is not.
FB's best in-sample spec remains temper+pool (§2c: 9/15, churn 0.017, RACF1 −0.019).

**Reproduction:**
```
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --temperament --min-knot-entities 8 --md-lags 6 --t-tails
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament --min-knot-entities 8 --md-lags 6 --t-tails
python llm_fitting/rankdiff_kalman.py facebook --oos --temperament --min-knot-entities 8 --md-lags 6 --t-tails
```

## 2e. 2026-07-02 — Spec-B: σ_obs IDENTIFIED from the daily noise floor (validates Spec-A)

**Method (`llm_fitting/spec_b_sigma_obs.py`).** The weekly metric is the sum of daily karma
(verified exact), so within-week daily randomness that averages out cannot carry week-to-week
signal — its delta-method image on the weekly log-sum is a floor for σ_obs. PRIMARY estimator:
fit σ_d²·Toeplitz(1, ρ₁..ρ₆) to the within-week residual covariance (through the week-mean
centering projection) and map exactly via daily shares. Within-week residuals are mildly
mean-reverting (ρ₁..₃ ≈ −0.1, as the 2026-06 handoff warned): naive iid mappings (splithalf /
residual cross-checks, both implemented) overstate the floor ~2×. Used as a noise floor only —
no daily dynamics model. Reddit only (FB has no sub-weekly data).

**The validation result.** Spec-B (daily replication) vs Spec-A (MD weekly-covariance σ_obs):
0.100 vs 0.117 at rank ~800; 0.147 vs 0.148 at ~3,800; 0.231 vs 0.268 at ~10,000 — agreement
within ~25% across the universe from two fully independent identification strategies.
**σ_obs is now identified, not calibrated.** (Top-100 floor: 0.062 — adjudicating the head
between the degenerate MD solution 0.03 and the obs_frac curve 0.10.)

**Pinning σ_e in the MD fit** (`--spec-b`; per-split TRAIN-only curves in the OOS gate) breaks
the φ→0 weak identification externally — and the fitted **σ_trans collapses to ~0 everywhere**:
the weekly Reddit model reduces to **OU home (κ ≈ 0.01, σ_η ≈ 0.11) + identified measurement
noise (0.10–0.24) + temperament + rebirth** — a whole component eliminated by identification,
not assumption (the t-tails become inert with σ_trans = 0).

**Results (Reddit K=5,000, stack + spec-B):**
- In-sample: **14/15, churn err 0.053** (best recorded); dRank1/4/13 diffs +0.4/+0.7/+1.3
  (essentially exact at every horizon); Pers1 +0.4, Pers13 −0.6; only R2_13 fails (+0.110).
- OOS: 0.215 ± 0.059 vs persistence 0.168 ± 0.004, **100% CI coverage**, Wasserstein 2.1–3.4,
  two splits beat persistence, held-out dRank1 median exact (6 vs 6); scale interior
  (0.15–1.0) trending to 1.0 with training size.

**The three Reddit OOS specs side by side (all universe + temper + pool):**

| spec | σ_obs | rel err | coverage |
|---|---|---|---|
| obs_frac (§2c) | knob | 0.254 ± 0.091 | 40% |
| + md6 + t (Spec-A, §2d) | estimated (weekly) | **0.171 ± 0.017** | 100% |
| + spec-B pinned | **identified (daily)** | 0.215 ± 0.059 | 100% |

Spec-A remains the best point numbers; Spec-B matches distributionally, is fully identified,
simpler (no transitory component), and independently validates Spec-A's curve — the pairing is
the paper's identification argument. FB path forward: no daily data, so FB keeps gate-calibrated
Spec-A; the Reddit result (fast component ≈ noise) motivates re-examining FB's raw-MD head split
with a noise-favoring prior, and YouTube (daily views available?) can pre-register Spec-B.

**Reproduction:**
```
python llm_fitting/spec_b_sigma_obs.py 5000       # Spec-A vs Spec-B curve comparison
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --temperament --min-knot-entities 8 --md-lags 6 --t-tails --spec-b
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament --min-knot-entities 8 --md-lags 6 --t-tails --spec-b
```

## 2f. 2026-07-02 — Conditional forecasts: the model now BEATS persistence on Reddit

**Change (`--conditional {state,vhat}` on the OOS gate).** The unconditional gate simulated a
synthetic burned-in universe; persistence implicitly uses entity-level information, so the
comparison was handicapped. Now: `sim_cohort_conditional` simulates the ACTUAL member universe
forward from its steady-state-Kalman-filtered end-of-train levels (real gap structure, no
burn-in; transitory folded into measurement noise for filtering), and `--conditional vhat`
additionally gives each real entity its own EB-shrunken temperament multiplier
(`mrd.eb_vhat`: log v̂_i = s²/(s²+trig_i)·ê_i, mean-1 renormalized, prior for entities with <8
changes). All inputs train-only; calibration protocol unchanged.

**Results (rolling-origin, 5 splits):**

| Reddit K=5,000 (md-stack) | rel err | persistence | coverage |
|---|---|---|---|
| unconditional (§2d) | 0.171 ± 0.017 | 0.168 ± 0.004 | 100% |
| **conditional: state** | **0.118 ± 0.061** | 0.168 ± 0.004 | 100% |
| conditional: state+v̂ | 0.148 ± 0.059 | 0.168 ± 0.004 | 100% (best Wasserstein: 1.3–2.0) |
| spec-B + state+v̂ | 0.220 ± 0.050 | 0.168 ± 0.004 | 100% |

**Reddit conditional-state beats the persistence baseline on 4 of 5 splits** (0.041 vs 0.167;
0.071 vs 0.171; 0.128 vs 0.162; 0.131 vs 0.168; miss: 0.221 at the shortest train), with 100%
CI coverage — the first spec to clear the gate's full bar. Attribution: most of the gain is the
REAL INITIAL STATE (gap structure); per-entity v̂ yields the tightest distributional match
(Wasserstein) but slightly worse moment-vector error. The spec-B variant gains less because with
σ_trans = 0 temperament only scales the noise.

| FB (md-stack) | rel err | persistence | coverage |
|---|---|---|---|
| unconditional (§2d) | 0.158 ± 0.027 | 0.146 ± 0.022 | 60% |
| conditional: state | 0.152 ± 0.043 | 0.146 ± 0.022 | 40% (beats persistence on 2 splits) |
| conditional: state+v̂ | 0.161 ± 0.035 | 0.146 ± 0.022 | 40% |

FB stays at par (late-split held-out distributions essentially exact: dRank1 13/64 vs emp
14/65; dRank4 21/120 vs 20/116), but conditioning does not lift it above the baseline; CI
coverage dips 60→40%. FB's benchmark is also stronger (0.146).

**Known wrinkle:** at the shortest train origin (11 changes), `estimate_temperament`'s
min_changes=12 forces s=0, disabling temperament for that split (both conditional variants
identical there). Lowering the threshold for short windows is a pending robustness item.

**Reproduction:**
```
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state    # 0.118 vs 0.168
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional vhat
```

## 2g. 2026-07-03 — New data assets & measurement caveats (owner notes)

**New panels on the SSD** (`data/ssd -> /Volumes/T9/rank-diffusion-data`; see
DATA_PHASE2_REPORT.md and DATA_INVENTORY.md; all passed the schema contract, exact
weekly=Σdaily invariants, and loader smoke tests):
- `derived/fb_daily.parquet` + `derived/fb_weekly_rebuilt.parquet` — FB DAILY exists:
  1,191 complete days 2020-10-27..2024-03-06; 158 complete Monday weeks 2020-11-02..2024-02-12;
  **72 clean weeks beyond the old 2022-06-27 corruption point**. Keystone validation vs the
  trusted cutdown panel: join rate 1.0, metric correlation 0.99999 — via `account.name`
  (the trusted panel's ids ARE page names). **FB daily unlocks Spec-B for Facebook.**
- `derived/reddit_comments_2018-12_2021-06_{daily,weekly}.parquet` — Reddit COMMENT-karma
  panels, 136 weeks / 943 days (metric_value = comment karma; a different metric from the
  repo's submission-karma panel). Submissions 2021-07..2022-12 pending (resume command in
  DATA_PHASE2_REPORT.md); the 2023-01..2024-06 bridge remains an owner acquisition decision.

**CENSORING ASYMMETRY (owner directive — encode in every coverage claim):**
- **Reddit (Pushshift) is a complete census of the platform.** Top-K coverage shares computed
  on it ARE platform-wide shares ("top-5,000 = 89% of weekly karma" is a statement about Reddit).
- **Facebook (CrowdTangle) is a CENSORED sample**: it tracked only pages above inclusion
  thresholds (plus manual additions). "Top-K covers X% of interactions **in the data**" is a
  statement about the tracked universe, NOT the platform. The top-coverage rule still defines a
  valid estimand (the head of the tracked universe), but platform-wide coverage language must
  never be used for FB (or IG). Cross-platform comparisons of coverage percentages are
  apples-to-oranges and must be flagged.

**Further owner caveats on the current universe construction (to revisit):**
- The modeled endpoint set should be FULLER — for Reddit comments, top-2,500 (B=10k) is too
  small; owner suggests K≈12,500 (B=50k, ~98% of comment activity) as the working scale.
- **Absence-penalized membership is suspect on LONG panels**: over 2.5-4 years, entities that
  legitimately rose or died mid-panel are penalized for weeks before birth / after death, so
  full-panel membership drifts toward "always-existed" entities. Fine at T=30; at T≥136 use
  member_window sensitivity checks (trailing-window membership) and larger buffers, and treat
  membership choice as a reportable robustness dimension. The data can be reconstructed/
  re-derived later; current derived panels are better than what preceded them but not final.
- `fb_weekly_rebuilt` ids are **page names** (validated choice, but names can change or collide
  over 3.5 years — name churn will masquerade as exit+entry; an account.id-keyed rebuild is a
  documented future fix). Rebuilt panel has ~44.7k pages/week vs the trusted cutdown's ~14.4k —
  the old panel was itself top-truncated; expect different tail behavior.

**ADDENDUM (2026-07-03, owner context + measured): CrowdTangle instrument eras — SEGMENT
BEFORE FITTING.** The CrowdTangle collection degrades mid-series: tracked pages collapse
(owner: bottoming ~4–5k daily; reported mechanism — pages that grew past the inclusion
threshold were never added because FB had internally decided to kill CrowdTangle), partly
recovers, then slowly declines into the 2024 shutdown. Measured on the rebuilt panels
(pages/week, pages/day, new-ids/week):

| era | weekly span | collection health | use |
|---|---|---|---|
| A | 2020-11-02..2022-06-27 (~86 wks) | ~14.4k/wk, ~12.5k/day, stable | **PRIMARY** (matches trusted panel) |
| B collapse | ~2022-07..2022-09 | daily mean 6.6k, days down to ~600; weekly min 6.8k | **exclude** |
| C recovery | 2022-10..2022-12 (~13 wks) | 11–13.8k, occasional bad days | replication w/ caution |
| M 2023 mixture | 2023 | 37 days patched from full_fb (different, full-universe source) + backfill intensity swinging 2.2k–100k+/day; weekly unions reach 200k+ pages | **unusable as built** — rebuild single-source (owner decision) |
| D terminal | 2024-01..2024-03-06 | 12.9k/wk, enrollment 0, slow decline | pre-shutdown caution; robustness only |

Handling directives (instrument-health segmentation, standard practice for collection/sensor
changes — breakpoints from collection metadata ONLY, never from model fit):
1. Headline FB inference on Era A only; Spec-B identification on Era A dailies.
2. Eras C and D are REPLICATION segments (do s, kappa, sigma_obs reproduce?) — never new
   evidence for entry/boundary/coverage claims. Owner expectation: patterns should look
   similar once issues mostly resolve; extra caution at the very end (pre-shutdown).
3. NEVER bridge eras B or M with any window: membership windows, displacement horizons,
   OOS splits, and filtered-state initializations must sit inside one era.
4. On FB, ABSENCE IS NOT BEHAVIOR: in eras B/M absence mostly means the collector dropped
   the page. Absence-penalized membership is only meaningful within-era. (Reddit is a census;
   there absence = below-floor activity, as designed.)
5. Enrollment was frozen from the START (new ids ~0/wk after week 1): the backfill is a fixed
   ~14.5k-page panel by construction — same property as the old trusted panel. FB
   entry/boundary-influx metrics are within-panel quantities; say so in any writeup.
6. Sporadic low-count days exist even inside Era A (e.g. 2022-04-15: 94 pages) and are
   invisible to the complete-week filter (day "complete" = file nonempty). Daily/Spec-B work
   needs a LOW-COUNT-DAY GUARD (flag days below ~60% of trailing-median pages; exclude flagged
   days from noise-floor estimation and flag weeks containing them). The weekly keystone was
   unaffected because the trusted panel shares the same collection holes.

**P0 VERIFICATION (2026-07-03, committed in `llm_fitting/instrument_eras.py` — canonical
era table + guard from here on).** Health series (pages/day, pages/week, new-ids/week)
re-derived from the rebuilt SSD panels. Outcome: eras A/B/C/M CONFIRMED as tabled
(A: 86 complete wks, 14,362 pages/wk median, enrollment frozen at ~8 new ids/wk;
B: 24/82 days flagged, day median 5,712 — collapse; C: 12 complete wks, 3 flagged days;
M: complete-week medians look normal (13.8k) but weekly unions reach 420,592 pages and
new-ids/wk reach 143,915 — the mixture is confirmed and invisible to any single-week
health check). **ERA D AMENDED: only 2 complete weeks exist** (2024-01-01, 2024-02-12;
45/66 days present — the Jan–Mar 2024 daily gaps kill complete-week coverage; the
"~6–9 complete wks" above was wrong). Weekly estimation on D is INFEASIBLE; only daily
(noise-floor) statistics are estimable there, robustness only. Reddit comments panels:
CENSUS CONFIRMED — 0/943 days flagged, smooth growth 31k→71k subs/day, ~13k organically
new ids/wk; no eras. Low-count-day guard implemented as declared (trailing 28-day median,
60% threshold): 59 flagged days panel-wide; Era A contains 15 (2022-04-15 = 94 pages
among them), touching 10 of 86 Era-A weeks. DECLARED HANDLING: Spec-B/daily estimation
drops every week containing a flagged day; WEEKLY fits KEEP flagged weeks (platform-wide
undercount is mostly absorbed by the per-period common factor; the trusted-panel keystone
already contained the same holes; dropping interior weeks would break consecutive-week
change pairs for the MD/ACF estimators). New PLATFORMS entries: `facebook_a`, `facebook_c`,
`facebook_d` (era slices of `fb_weekly_rebuilt`), `reddit_comments`. Pre-registered
coverage K on Era A: top-1800 = 79.7%, top-3500 = 89.7%, top-5500 = 94.8% "of tracked
activity" — within 0.3pp of the trusted panel, so old-FB K values carry over
(comparability); reddit_comments K80/90/95 = 1000/2500/5000 (census shares), owner
working scale K=12,500 (B=50k, 98.8%).

## 2g-X. 2026-07-03 — Era-aware fits on the recovered data (P1–P5 running record)

### P1 — FB Era A, weekly (rebuilt panel, era-disciplined; K=3500 pre-registered)

Panel: `facebook_a` = Era-A slice of `fb_weekly_rebuilt` (T=86, mean N=14,365/wk,
"of tracked activity"). Legacy guard on the old cutdown panel: **14/15, churn 0.013 —
unchanged**. All numbers below from this session's runs.

**In-sample (K=3500, B=14k, 5 reps):**

| spec | goal-1 | churn err | signature |
|---|---|---|---|
| temper+pool (old FB: 9/15 / 0.017) | **13/15** | 0.045 | dRank1/4/13 exact (+1.0/+0.8/−2.4); misses RACF13 −0.099, Pers4 +7.2; coll1 −0.19 |
| + md6 + t-tails (old FB: 7/15) | 8/15 | 0.122 | SAME weak-identification signature as old FB: RACF1 +0.115, coll1 −0.219, head σ_obs → 0.000 |

Parameter consistency with the old panel: temperament **s = 0.890** (old FB 0.887),
t_df = 4.3 (old 4.3), κ(z) = 0.005..0.100 (old-style head→tail shape). The raw-MD
weak identification REPLICATES on the rebuilt data — external σ_obs identification
(P2 Spec-B) is confirmed as the binding constraint, not a data artifact.

**OOS movement gate (rolling origins 21/32/43/54/65, test 21 wks, temper+pool+md6+t):**

| spec | rel err | persistence | CI coverage | scale by split |
|---|---|---|---|---|
| **unconditional** | **0.114 ± 0.046** | 0.144 ± 0.030 | 60% | 1.0, 0.7, 1.0, 1.0, 1.0 |
| conditional: state | 0.140 ± 0.043 | 0.144 ± 0.030 | 60% | same |
| conditional: state+v̂ | 0.154 ± 0.049 | 0.144 ± 0.030 | 60% | same |

**First FB spec to beat persistence on EVERY split** (0.131<0.144, 0.172<0.179,
0.035<0.093, 0.131<0.136, 0.099<0.167; old-FB best was 0.158 ± 0.027 vs 0.146).
Calibrated scale sits at 1.0 on 4/5 splits — the estimated observation model is
self-consistent OOS (the 2c pre-registered signature, now on all splits, not just
late ones). Not yet the full bar: CI coverage 60% (<100%); last-split held-out
distributions near-exact (dRank1 13/65 vs emp 14/69; dRank4 18/119 vs 20/123).
Conditioning does NOT lift FB (matches 2f on the old panel) — the gain lives in
the σ_obs identification, not the initial state. temper_s stable across train
windows (0.91–0.99).



### P2 — FB Spec-B on Era A dailies (THE HEADLINE): σ_obs identified for FB for the first time

Machinery: `spec_b_curve` unchanged; FB daily loader with the P0 day guard (59 flagged
days → 33 of 176 member-weeks dropped from daily estimation). Per-band entity counts
813–1,494 vs ~1,167/band expected — the complete-positive-week skew toward big pages is
MILD; the floor curve covers essentially the whole universe. FB daily residual σ_d =
0.65 (head) → 0.95 (tail) with sum p² ≈ 0.19–0.24.

**Identified floor (toeplitz, primary): σ_obs,B = 0.207 (rank ~665) → 0.370 (rank ~11k)**
(iid variants 0.32→0.62, overstate ~1.6× as on Reddit).

**Pre-registered predictions, scored:**
1. *In-sample head metrics recover from raw-MD over-persistence* — **PASS**: 8/15 →
   **10/15**, churn 0.122 → 0.081; RACF1 +0.115 → +0.069 (passes), RACF4 +0.103 →
   +0.020 (passes), coll1 −0.219 → −0.155. (Still below temper+pool's 13/15: the VR
   block degrades as the freed fast power moves to σ_trans; VR4/8 fail at +0.11.)
2. *OOS calibrated scales move toward 1.0* — **PASS**: scale = 1.00 on **5/5 splits**
   (both spec-B runs). Caveat: 1.0 is the grid top; held-out p90s run slightly under
   (59 vs 69), so the unconstrained optimum may sit above 1.
3. *Fitted σ_trans collapses toward 0 (Reddit lesson)* — **PARTIAL**: collapses exactly
   at the head (σ_trans = 0.000, φ = 0 in the top knots — the FB weekly head model
   reduces to OU home + identified noise, as on Reddit), but the tail keeps
   σ_trans ≈ 0.57. The Reddit "whole component eliminated" result does NOT fully
   generalize to FB.
4. *Spec-A vs Spec-B curves agree (~25%, Reddit precedent)* — **FAIL beyond the head**:
   head 0.207 vs 0.176 (~18% ✓), but Spec-A collapses BELOW the floor exactly in the
   weakly-identified band (0.130 vs 0.253 at rank ~1.7k) and sits **40–65% ABOVE the
   floor in the mid/tail** (0.60 vs 0.36 at rank ~10k). On FB, weekly-covariance σ_obs
   and the daily noise floor are NOT measuring the same object outside the head —
   excess fast within-week dynamics and/or posting intermittency load onto the weekly
   "noise" term. This is a real cross-platform asymmetry of the measurement model,
   not an estimation bug (the same estimator agreed within 25% on Reddit).

**OOS movement gate (spec-B pinned, per-split train-only curves):**

| spec | rel err | persistence | CI coverage | scale |
|---|---|---|---|---|
| spec-B unconditional | 0.211 ± 0.030 | 0.144 ± 0.030 | 40% | 1.0 ×5 |
| spec-B + conditional state | 0.164 ± 0.049 | 0.144 ± 0.030 | 40% | 1.0 ×5 |
| (P1 spec-A calibrated, reference) | **0.114 ± 0.046** | 0.144 ± 0.030 | 60% | 0.7–1.0 |

Same ordering as Reddit 2e (spec-B matches distributionally, loses pointwise).
**Operational FB spec stays gate-calibrated Spec-A** — but FB σ_obs is now bracketed
by an independent instrument: the head value (~0.2) is validated, the raw-MD mid/tail
values are too high, and the raw-MD sub-floor collapse at ranks 1–2k is confirmed as
weak-identification pathology. Interesting inversion worth carrying forward: with
spec-B pinned, state-conditioning HELPS FB (0.211→0.164) — with spec-A it hurt
(0.114→0.140).

### P3 — Reddit comments at the owner scale (K=12,500, B=50k ≈ 98.8%; census; T=136)

First fits on the comment-karma metric at scale (draft was K=2,500, reps=1). All
runs: universe + temper + pool≥8 + md6 + t-tails, reps=5 in-sample.

**Cross-metric parameter report (the unified-law evidence the run was for):**
temperament **s = 0.692** (spread p90/p10 2.43×) — vs 0.941 submissions, 0.890 FB,
and 0.822 in the K=2,500 comments draft: **s is metric- and scale-dependent; the
"one global s ≈ 0.9" reading weakens** (train-window s on the OOS splits: 0.64–0.67).
κ(z) = 0.005–0.2 (same head→tail shape as subs/FB). σ_obs: Spec-B floor
**0.101 (rank ~1.1k) → 0.28–0.30 (deep tail)**; Spec-A within 13–20% of the floor
through head/mid (0.120 vs 0.101; 0.178 vs 0.157) but ~2× above it in the deep tail
(0.55–0.58 vs 0.28–0.30) — the depth-dependent Spec-A/Spec-B divergence seen on FB
(P2) appears on a census metric too, so it is NOT a CrowdTangle-censoring artifact;
it grows with rank depth on both platforms. t_df 4.6 (Spec-A) / 6.4 (Spec-B).

**In-sample (the diagnosis target):** the draft's VR over-persistence REPLICATES at
scale (spec-A: 8/15, churn 0.037; VR2..13 diffs +0.137/+0.205/+0.226/+0.219; head
σ_obs → 0.000, the degenerate fast split again). **Pre-registered prediction — "Spec-B
pinning fixes the VR block" — FAILS**: pinned run scores 9/15, churn 0.026, dRank1/4
near-exact (−0.6/+0.6), RACF13 passes, but VR diffs move only to
+0.121/+0.187/+0.204/+0.205. The comments over-persistence is STRUCTURAL, not a
noise-split identification artifact: the model lacks 4–13-week mean reversion that
the long panel measures precisely, and the top set is too sticky (Pers1/4/13 +9..+11,
outfluxK +0.075, return4K −0.125). This kills the weak-identification explanation
for comments and points at the home process (stronger/faster reversion, or
slow temperament drift) as the deficit.

**OOS movement gate (T=136, test 34, origins 34/51/68/85/102):**

| spec | rel err | persistence | coverage | scales |
|---|---|---|---|---|
| unconditional | 0.209 ± 0.062 | 0.160 ± 0.068 | 80% | 0.10–0.50, median 0.35 |
| conditional: state | **0.170 ± 0.063** | 0.160 ± 0.068 | 60% | same |
| spec-B pinned | 0.163/—/0.248/0.204/0.238 by split (see note) | 0.160 ± 0.068 | **100%** | 0.05–0.70 |

Conditioning helps (0.209→0.170) but lands AT PAR with persistence, not above it as
on submissions (0.118 vs 0.168) — the conditional edge does not transfer wholesale to
the longer, regime-varying panel. The persistence baseline itself is non-stationary
across origins (0.071 calm-2019 → 0.237 COVID-era): T=30 submissions never saw a
regime change; T=136 comments does, and both model and baseline degrade inside it.
Held-out distributions are tight everywhere (Wasserstein 0.6–3.0; last split dRank1
6/23 vs emp 6/26). NOTE (scoring artifact, not model): the spec-B aggregate prints
as 1154 ± 2308 because the T0=51 test window has empirical coll1 = 0 and the
rel-err denominator floors at 1e-6 — a sim coll1 of 0.006 scores as ~5772. The
per-split numbers above exclude nothing else; mean over the 4 clean splits = 0.213.
Known sharp edge for future gate runs: near-zero empirical moments (deep-K rank-1
collisions on a census head dominated by AskReddit) need a floor or exclusion rule —
declared here, not patched mid-experiment.

### P5 — Membership robustness on the long census panel (measured, not redesigned)

`llm_fitting/membership_robustness.py` (comments, K=12,500, B=50k, reps=3 declared).
Member-set overlap (share of B): full∩first-half 0.806, full∩second-half 0.920,
first∩second 0.729, second∩trailing-60 0.979. The owner-suspected drift is REAL —
half-window sets differ by ~27%, and full-window membership tilts toward the
second half (0.92 vs 0.81). But the HEADLINE METRICS BARELY MOVE across all four
membership choices: score 8–9/15, churn 0.027–0.052, s 0.669–0.697, empirical
moments essentially invariant (RACF1 0.66–0.69, VR4 0.33, coll1 0.044, outfluxK
0.089–0.090). Verdict: membership-window choice is a reportable robustness
dimension, not a headline-level threat, on this panel; the estimand population is
much more stable than the member list.

### P4 — Replication on eras C and D (owner question: do patterns reproduce post-recovery?)

`llm_fitting/era_replication.py` (declared adaptations: min_changes=8 and md_lags=4 on
C — the Era-A reference is recomputed at the same settings; A's s is 0.890 at BOTH
min_changes 8 and 12, so the adaptation itself is unbiased). NO OOS gates, no
entry/boundary claims on segments this short.

| quantity | Era A (ref) | Era C (T=12) | Era D (T=2) |
|---|---|---|---|
| temperament s | 0.890 (n=14,000) | **0.939** (n=12,945) | n/a |
| Spec-B floor σ_obs,B (head→tail) | 0.207→0.370 | 0.251→0.395 | 0.275→0.327 |
| κ(z) head/mid/tail (md4) | 0.200/0.140/0.005 | 0.200/0.005/0.005 | n/a |
| σ_obs MD head/mid/tail (md4) | 0.014/0.224/0.620 | 0.156/0.340/0.800 | n/a |
| σ_perm head/mid/tail | 0.162/0.190/0.294 | 0.219/0.000/0.422 | n/a |
| t_df | 4.3 | inf (T=12 can't measure) | n/a |
| in-sample card | 13/15 (P1, canonical) | 6/15 (max 11; h=13 undefined) | infeasible |

**Verdict: the transportable quantities replicate.** Temperament within 0.05, the
Spec-B noise-floor curve keeps its shape at a ~15–20% higher level (consistent with
the slightly degraded post-recovery collection: 11.6k vs 12.6k pages/day), and even
Era D's floor (the only thing estimable from 2 weeks) sits in the same band. The
non-transportable parts are exactly the short-segment-fragile ones: Era C's empirical
targets are themselves unstable at T=12 (coll5/10/20 = 1.000 exactly; dRank1 = 40 vs
A's 27; σ_F = 0.359 vs A's 0.146 — the common factor absorbs the recovering
instrument's intensity wobble, which is what it is for), so the 6/15 card reads as
segment noise plus genuine extra churn during recovery, not parameter drift. NOTE
(estimator sensitivity, worth carrying): A's κ head at md4 is 0.200 vs 0.005 at the
canonical md6 — the OU reversion estimate is lag-window sensitive; only compare κ
across segments at MATCHED md_lags.

**Era M diagnostic paragraph (why it is unusable as built):** 2023 mixes two
collection universes — 37 days are patched from `full_fb` (a full-universe export,
100k+ pages/day) into a ~12k-page backfill panel, with patch intensity swinging
2.2k–100k+ pages/day. A weekly sum in a patched week adds 1-day full-universe totals
to 7-day fixed-panel totals: weekly entity unions reach 420,592 "pages" and new-ids/wk
reach 143,915 in a panel whose true enrollment is frozen (~8/wk in Era A), while
complete-week medians look normal (13.8k) — the mixture is invisible to any
single-week health check and poisons ranks, absence penalties, and entry metrics
alike. Nothing short of a single-source rebuild (owner decision, documented in 2g)
makes 2023 usable.

## 2h. 2026-07-03 — SYNTHESIS: the unified law across two platforms, three metrics-eras, and 4.5× more data

All FB quantities are "of tracked activity" (CrowdTangle censored fixed panel);
Reddit quantities are platform-wide (Pushshift census). Gate = rolling-origin
distributional OOS movement, 5 splits; never single-split.

| | FB old (T=88) | FB Era A (T=86) | FB Era C (T=12) | FB Era D (T=2) | Reddit subs (T=30) | Reddit comments (T=136) |
|---|---|---|---|---|---|---|
| temperament s | 0.887 | 0.890 | 0.939 | n/a | 0.941 | **0.692** |
| κ(z) top→tail | (md6) ≈ Era A | 0.005→0.100 | 0.200/0.005/0.005 (md4) | n/a | 0.005→0.04 | 0.010→0.100 |
| σ_obs identified (Spec-B floor) | none (no dailies) | **0.207→0.370** | 0.251→0.395 | 0.275→0.327 | 0.10→0.24 | 0.101→0.28 |
| t_df | 4.3 | 4.3 | n/a (T=12) | n/a | 6.7 | 4.6 |
| in-sample best | 9/15 (temper+pool) | **13/15** (temper+pool) | 6/15 (max 11) | infeasible | 14/15 | 9/15 (spec-B) |
| OOS gate verdict | at par (0.152–0.158 vs 0.146) | **BEATS 5/5** (0.114±0.046 vs 0.144±0.030, cov 60%, scale→1.0) | no gate (short) | no gate | **BEATS 4/5** (cond. 0.118 vs 0.168, cov 100%) | at par (cond. 0.170 vs 0.160, cov 60–80%) |

**What held (the unified-law evidence):**
- The SAME estimator stack transported unchanged to a rebuilt panel, two new eras,
  and a new metric; every transportable parameter replicated: s within 0.05 across
  FB eras (0.887/0.890/0.939), κ(z) same head→tail shape everywhere, Spec-B floor
  same shape across eras AND platforms (head ~0.1–0.27 rising ~2–3× to the tail),
  t_df ~4–7 both platforms.
- σ_obs identification from the daily noise floor now works on BOTH platforms
  (P2 unlocked FB); at the universe head, Spec-A and Spec-B agree within ~20%
  everywhere tested.
- Both platforms now have a spec that beats persistence out-of-sample — and they are
  DIFFERENT specs in an instructive way: Reddit subs needed conditioning (real
  filtered state), FB Era A needed neither conditioning nor σ_obs correction
  (scale = 1.0; the estimated model is self-consistent). Held-out displacement
  distributions are near-exact on every panel (Wasserstein 0.6–3.0 comments,
  9–49 FB).
- Instrument-era discipline: nothing bridged B or M; the P4 replication says the
  post-recovery instrument measures the same process, just noisier (floor +15–20%).
- Membership choice on the long census panel: real drift (half-window overlap 0.73),
  headline-invariant (P5).

**What broke (each one a lesson, none fatal):**
1. *Spec-A ≈ Spec-B within ~25%* (2e) was a shallow-universe result: on BOTH
   platforms the divergence GROWS WITH RANK DEPTH (Spec-A 1.5–2× above the floor in
   the deep tail; sub-floor collapse in FB's weakly-identified mid band). σ_obs is
   identified at the head; below it, "weekly noise" contains structure the daily
   floor cannot see (posting intermittency / discreteness / fast dynamics).
2. *One global s ≈ 0.9*: comments at scale gives s = 0.692 (0.822 at K=2.5k draft).
   s is metric- and universe-scale-dependent — a property of the (metric, estimand),
   not of the platform.
3. *The weak-identification explanation of over-persistence*: killed on comments —
   Spec-B pinning fixed FB's head metrics (P2 prediction 1 PASS) but NOT the comments
   VR block (P3 prediction FAIL, +0.19..+0.21 remain). The long panel isolates a
   STRUCTURAL deficit: the model under-produces 4–13-week mean reversion and
   over-holds the top set (Pers +9..+11). The home process (or slowly-drifting
   temperament) is the deficit, measurable only at T ≫ 30.
4. *Conditional forecasting as the universal lever*: its edge does not transfer to
   the regime-varying long panel (comments cond. 0.170 ≈ persist 0.160; and on FB it
   has never helped). Conditioning wins where the panel is short and stable — it is
   not a substitute for getting the noise model (FB) or the home process (comments)
   right.
5. *Era D as a replication sample*: only 2 complete weeks exist (P0 amendment) —
   2024 is a daily-statistics-only segment; and 2023 (M) needs a single-source
   rebuild before it is anything.

**Sharpest next actions:**
1. **Home-process reversion at long horizons** (the comments VR block): estimate κ
   from longer-lag change autocovariances (md-lags ~13–26, feasible only on T=136)
   and/or a slowly-mean-reverting temperament (2c refinement); acceptance = comments
   VR4/8/13 in-sample + the comments OOS gate. This is the one place the model is
   structurally wrong on clean census data.
2. **Intermittency-aware noise floor** for the deep tail (the Spec-A/Spec-B
   divergence): model zero/absent days explicitly in the floor mapping before
   claiming σ_obs identification below the head; until then, spec language should be
   "σ_obs identified at the universe head, bracketed below it".
3. **Data reconstruction** (owner decisions, flagged not worked around): (a) resume
   comments aggregation 2021-07..2022-12 — extends the census panel into the same
   calendar window as FB eras B/C for a cross-platform same-period comparison and
   doubles the post-COVID regime coverage; (b) single-source 2023 FB rebuild (fixes
   M); (c) account.id-keyed FB rebuild (name churn currently reads as exit+entry —
   relevant to boundary-flux precision everywhere).

## 2i. 2026-07-03 — The VR over-persistence: mechanism identified, κ identification fixed (`--md-vr`), residual gap isolated as structural

**The error pattern (biggest continuing one across the program):** simulated 4–13-week
variance ratios sit far above empirical on every md-stack fit — comments +0.19..+0.23
(both noise specs), FB Era A +0.08..+0.12; first flagged as "VR8/13 inflate" in 2c.

**Diagnosis (measured, not conjectured).** (i) The md6-fitted parameters ALREADY imply
the over-persistence analytically (per-band implied VR13 0.26–0.33 vs empirical
0.18–0.25) — it is an estimation problem before it is a simulator problem. (ii) The
md6 objective is FLAT in the home-reversion rate: at a comments head knot, SSE(a)
varies only 3e-6→1e-5 across the whole κ grid, because the OU tail is spread thinly
over many lags (each γ_k ≈ 2e-4 vs γ0 ≈ 0.06) and the three free variance coefficients
absorb any a. κ was effectively UNIDENTIFIED and landed near 0 by noise. This is the
classic long-horizon identification problem: variance-ratio-type statistics, not
short-lag autocovariances, carry the power against slowly-decaying components
(Poterba–Summers 1988; Cochrane 1988 — both already in research_notes §7).

**Fix (`--md-vr`, opt-in; zero new model components — parsimony preserved).** Append
multi-horizon change variances D(h) = Var(X_{t+h}−X_t), h ∈ {2,4,8,13}, to the MD
moment vector; closed-form rows D(h) = 2W(1−a^h) + 2V(1−φ^h) + 2σ_e² in the SAME
three coefficients; reversion grid extended to κ ≤ 0.30 (A_GRID_VR, declared);
composes with the Spec-B pin. With the D moments the SSE(a) profile becomes sharply
V-shaped (interior minima, κ ≈ 0.02–0.04 head/mid on comments). Locked by two new
unit tests (exact recovery; noisy-panel recovery where plain md6 fails); default
paths byte-identical, legacy guard unchanged (14/15, churn 0.013), suite 31 passed.
DECLARED: D(h) are train-window second moments of the same change series the
estimator always used — in-sample VR is partially mechanical under --md-vr; the OOS
gate (never fitted) remains the acceptance criterion.

**Validation (all runs this session):**

| run | md6 (baseline) | + md-vr | verdict |
|---|---|---|---|
| comments in-sample spec-A | 8/15, VR +0.14..+0.23, Pers +7..+10 | **11/15**, VR +0.11..+0.15, **Pers1/4/13 +2.0/−0.2/−1.2** | structure WIN |
| comments in-sample spec-B | 9/15, churn 0.026 | 11/15, churn 0.036 | WIN |
| FB Era A in-sample | 8/15, churn 0.122 | 10/15, churn 0.086 (RACF1/4 pass) | win |
| reddit subs in-sample (T=30) | 14/15 | 13/15 (RACF13 −0.11; κ_head hits 0.30 edge) | slight LOSS |
| comments OOS uncond | 0.209 ± 0.062, cov 80% | 0.204 ± 0.095, cov 60% | neutral |
| comments OOS cond-state | 0.170 ± 0.063 | 0.196 ± 0.080 | slight loss |
| FB Era A OOS | **0.114 ± 0.046, cov 60%** | 0.179 ± 0.037, cov 0% (p90 209→136) | **LOSS** |

**Adoption verdict (by the pre-declared gate): defaults unchanged; --md-vr stays
opt-in.** D(13) needs many 13-week spans per TRAIN window: on FB's 21–65-wk rolling
train windows the noisy D moments destabilize the partition and truncate the
displacement tail (the gate catches exactly this); on subs (T=30) same story milder.
Scope: a LONG-panel identification tool and the program's diagnostic instrument.

**What the fix proves (the real yield).** With κ finally identified (comments
0.07→0.14 head→tail), the top-set persistence block and RACF1/4 snap into place —
the identification failure was real and is fixed. But VR4/8/13 STILL fail together
(+0.15) while every neighboring moment passes: a single-timescale OU home cannot
match the empirical D(h) curvature (strong 2–4-wk reversion AND the shallow 8→13-wk
tail) — pushing κ high enough for VR over-reverts RACF13 (−0.118) instead. The
residual is now cleanly isolated as STRUCTURAL: the home needs two timescales (fast
+ slow reversion — hybrid-Atlas-style multi-scale drift; equivalently a
slowly-mean-reverting temperament, the 2c refinement). That is the sharpest-scoped
next modeling item, with --md-vr as the measurement tool that makes it testable.

## 2j. 2026-07-03 — Stationary common factor (`--stat-factor`): the second layer of the VR block, measured and fixed

**Hypothesis chain (each step measured before coding).** After 2i, three candidate
explanations for the residual VR over-persistence were tested in order:
1. *Missing medium timescale (two-scale home)* — analytic feasibility says a φ₂≈0.95
   component fits the full moment vector where the current class strains, BUT the
   pooled knot moments turned out fittable by the current class already → not the
   dominant layer.
2. *Permanent-mix heterogeneity (2c's structure B)* — REJECTED by measurement: on the
   scorecard population, pooled VR13 (0.153) ≈ median per-entity VR13 (0.136); VR is
   scale-free, so only mix heterogeneity could bite, and there isn't enough
   (split-half ρ of per-entity VR13 = 0.34, Spearman(vol, VR13) = −0.22, mild).
3. *The integrated common factor* — CONFIRMED with a smoking gun: the simulator
   integrates F into every entity's permanent level (`mu += lam·F`), i.e. the
   platform-wide level random-walks; empirically the platform level MEAN-REVERTS
   strongly (comments: level VR13 = **0.121**, ΔLevel lag-1 autocorr = 0.02, and
   removing the level barely moves scorecard VR, 0.136→0.141). A factor-OFF sim probe
   drops sim VR13 0.273→0.218 — the integration manufactures ~0.05 of pure VR
   artifact at h = 8–13. The artifact is invisible to the OOS gate because the
   cohort simulators never had a factor term — it lives only in the in-sample
   goal-1 block.

**Fix (`--stat-factor`, opt-in; parsimony-POSITIVE — corrects a mis-specified
existing component, no new latents).** Same per-step F draw (rng stream identical);
when active, F feeds a stationary AR(1) common level L_t = ρ_L·L_{t−1} + F·√((1+ρ_L)/2)
applied at OBSERVATION (X += lam·L), so sd(ΔL) reproduces the measured σ_F and
nothing integrates into mu. ρ_L is MEASURED per platform from the detrended
cumulative-F path (comments 0.82, FB Era A 0.48, subs 0.90) — never tuned. Legacy
default (factor_rho=None) byte-identical; 4 new unit tests; suite 35 passed.

**Validation (5 reps each; all this session):**

| panel | spec | VR2/4/8/13 diffs | score | churn |
|---|---|---|---|---|
| comments | baseline md6 (P3) | +.137/+.205/+.226/+.219 | 8/15 | 0.037 |
| comments | + md-vr (2i) | +.106/+.150/+.152/+.148 | 11/15 | 0.038 |
| comments | + stat-factor only | +.117/+.168/+.160/+.152 | 10/15 | 0.024 |
| comments | **+ md-vr + stat-factor** | **+.104/+.127/+.111/+.096** | **11/15** | **0.025** |
| FB Era A | + md-vr + stat-factor | **+.015/+.040/+.033/+.027 — VR block PASSES** | **12/15** (was 8/15) | 0.097 |
| FB Era A | temper+pool + stat-factor | +.020(VR13), VR passes | 13/15 (held) | 0.044 |
| reddit subs | stack + stat-factor | +.035/+.004 (VR4/13) | **14/15 (locked score held)** | 0.064 |
| FB legacy guard (flag off) | | | 14/15 | 0.013 ✓ |

Comments extras at the combined spec: coll1 +0.003, Pers1/4/13 +2.8/+0.2/−2.2
(near-exact), dRank13 +4.8 (from +7.0). Pre-registered predictions: sim VR13 →
~0.22 as the factor-OFF probe forecast (landed 0.232) ✓; rank-based metrics hold or
improve ✓; FB same-direction ✓ (stronger: full VR pass); subs non-regression ✓;
legacy identical ✓. OOS gates untouched by construction (no factor in the cohort
sims) — this fix is a goal-1 (aggregate structure) correction and cannot be
credited against the movement gate.

**Cumulative decomposition of the VR-block error (comments VR13 sim, emp = 0.136):**
0.355 (P3 baseline) → 0.284 (κ identified, 2i) → **0.232 (stationary factor, 2j)**.
Two of three layers were artifacts (unidentified κ; integrated factor), now fixed
with net-zero new model structure. The remaining ~+0.10 at VR4 (with the RACF13
counter-tension: pushing κ higher over-reverts rank ACF) is the true structural
residue — the two-timescale home remains the sharpest open modeling item, now with
a much smaller target.

**Recommended usage going forward:** add `--stat-factor` to in-sample goal-1 runs on
all platforms (it held or improved every panel including the locked subs 14/15);
keep `--md-vr` long-panel-only (2i). Defaults unchanged to preserve committed
baselines; the legacy guard stays flag-off.

Reproduction:
```
python llm_fitting/minimal_rankdiff.py reddit_comments --top-k 12500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor
python llm_fitting/minimal_rankdiff.py facebook_a --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor
python llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --stat-factor
```

## 2k. 2026-07-03 — Two-timescale home (`--two-scale`): closes FB Era A to 14/15; comments residual is NOT class rigidity

**Change (nested, opt-in).** Second transitory component ξ₂ at a medium timescale:
`_md_partition2` fits OU-slow (a ≥ 0.93 by construction — "home" is the slow scale) +
AR-fast (φ₁ ≤ 0.65) + AR-medium (φ₂ ∈ 0.70–0.95) + noise, identified from the D(h)
moments (requires `--md-vr`; the γ tail alone cannot see φ₂ because B₂ = V₂(1−φ₂)²
is tiny). Simulated like ξ (temperament-scaled, Gaussian innovations — declared;
reset on rebirth) in `simulate` and BOTH cohort simulators; folded into R in the
conditional filter. σ₂ = 0 recovers the current model exactly; rng streams gated
(legacy byte-identical). 3 new unit tests incl. noisy-panel recovery of
(κ=0.02, φ₂=0.90) and zeros-inertness; suite 38 passed; legacy guard 14/15 /
0.013 unchanged. Cost: +2 parameters per knot, only where the flag is on.

**Validation (5 reps; baselines = 2j combined spec):**

| panel | spec | VR2/4/8/13 diffs | RACF13 | score | churn |
|---|---|---|---|---|---|
| FB Era A | 2j (md-vr+stat-factor) | +.015/+.040/+.033/+.027 | −0.022 | 12/15 | 0.097 |
| FB Era A | **+ two-scale** | **+.009/+.034/+.024/+.022** | **−0.010** | **14/15** | **0.045** |
| comments | 2j | +.104/+.127/+.111/+.096 | −0.112 | 11/15 | 0.025 |
| comments | + two-scale | +.104/+.138/+.116/+.097 | −0.120 | 11/15 | 0.030 |

**FB Era A: the class WAS binding — 14/15 is the best FB in-sample result ever
recorded** (equals the legacy v4.3 guard score, on the full unified stack with κ, σ_obs
head, φ₂ all estimated, not tuned): φ₂ = 0.70–0.75 with σ₂ = 0.14–0.21 (a real
component), the entire rank-dynamics panel within ±0.06, RACF13 essentially exact —
the κ tension is RESOLVED on FB (slow home + medium ξ₂ deliver VR and RACF13
simultaneously). Only miss: Pers4 +5.2 (tol 5); watch dRank now runs −5/−9 low.

**Comments: two-scale does NOT close the residual — important negative result.** The
fit takes φ₂ = 0.85–0.90 but with small head σ₂, and every headline metric is
unchanged within noise. Combined with the 2j feasibility check (the pooled comments
knot moments were already fittable by the 3-component class), this LOCALIZES the
remaining comments VR gap (+0.10..+0.14): it is not missing timescales and not the
noise split — it is a mismatch between the POOLED moments the estimator fits and the
MEDIAN-ENTITY statistic the scorecard reports, i.e. residual cross-entity
heterogeneity in the permanent/transitory MIX plus tracked-cohort composition (mild
per-entity dispersion measured: VR13 p10–p90 = 0.09–0.26, split-half ρ = 0.34). A
mix-heterogeneity extension (per-entity permanent share, EB-shrunken — the honest
version of 2c's structure B) is the correctly-scoped next item for comments;
two-scale is NOT it.

**OOS movement gates (comments; acceptance criterion):** unconditional + two-scale
0.188 ± 0.070 vs persistence 0.160 ± 0.068, coverage 60% (baseline 0.209 ± 0.062 /
80%; beats persistence on 2 of 5 splits incl. 0.124 vs 0.185); conditional + two-scale
clean-split mean 0.187, coverage 80% (T0=51 again hits the declared coll1=0
denominator artifact). Net: at-par point estimates, mildly better than baseline
unconditional, no harm — the gate neither adopts nor rejects two-scale on comments;
on FB, two-scale + md-vr remains long-panel-scoped for OOS use (2i caveat stands).

**Adoption:** `--two-scale` recommended WITH `--md-vr --stat-factor` for FB Era A
in-sample work (14/15); not recommended for comments (no gain) or short panels
(md-vr scope). Defaults unchanged everywhere.

Reproduction:
```
python llm_fitting/minimal_rankdiff.py facebook_a --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor --two-scale
python llm_fitting/minimal_rankdiff.py reddit_comments --top-k 12500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor --two-scale
```

## 2l. 2026-07-03 — Mix heterogeneity (`--mix-hetero`): structure B measured in, FB Era A reaches 15/15, comments conditional gate at 100% coverage

**The fix (one measured parameter, nested).** Per-entity permanent-share
heterogeneity: σ_perm,i = σ_perm(z̄)·v_i^(b/2)/norm (E[w]=1 — pooled moments and
Eulerian structure preserved; b=0 = movement-only legacy, b=1 = 2c's full structure
B). **b is identified from the s(h) horizon moment** (2c's own instrument): temperament
dispersion from non-overlapping h-week changes, b = s(h*)/s(1). Measured this
session: comments s(1..13) = 0.692/0.687/0.690/0.703/0.747 → **b = 1.08**; FB Era A
s(1..8) = 0.890/0.889/0.899/0.912 → **b = 1.02**. s(h) is FLAT on a 136-week census
metric — structure B is the measurement, not an assumption. Rationale for why
movement-only was wrong under the current fits: the permanent component carries ~51%
of 1-week change variance at the head, so scaling only the fast components
under-disperses entity variances by ~half at every horizon. Implementation: sqw
multiplies the σ_perm innovations in `simulate` + both cohort sims (vhat path uses
empirical-mean normalization of v̂^b — declared); rng streams gated; 3 new unit
tests (b-recovery separates b=1 from b=0 on synthetic panels; E[w]=1; b=0
byte-identical). Suite 41 passed; legacy guard 14/15 / 0.013 unchanged.

**Pre-registered predictions, scored:**
1. *Sim cross-sectional variance dispersion rises to the empirical level* — **PASS**:
   sd(log 1-wk change-var) 0.612 → 0.697 vs empirical 0.745 (a distribution-goal
   moment the movement-only spec could never produce).
2. *Median VR13 moves partially (~0.02–0.04), not fully* — **PASS as predicted**:
   sim median VR13 0.236 → 0.216 (emp 0.136).
3. *No OOS displacement-tail explosion (the 2c structure-B failure mode)* — **PASS**:
   held-out p90s 25/37/56 vs emp 26/34/50; the κ-confined home + T=136 tame the
   lognormal tail that blew up on subs T=30.
4. *Churn/dRank/Pers hold* — PASS (comments churn 0.034, dRank13 +2.0, Pers +3..+5).

**In-sample:**

| panel | spec | result |
|---|---|---|
| **FB Era A** | md-vr + stat-factor + two-scale + **mix (b=1.02)** | **15/15 — the first legitimate perfect goal-1 card** (the 2020-era 15/15 relied on the band-alignment bug). Whole panel essentially exact: VR −.007/+.009/+.008/+.008, RACF1 +0.005, RACF13 −0.013. Churn err 0.072 — head collisions still under-predicted (coll1 −0.167): goal-2 head churn is now FB's only open block. |
| comments | md-vr + stat-factor + **mix (b=1.08)** | 10/15, churn 0.034; VR13 sim 0.232 → **0.218**, RACF1 +0.014 (exact), dRank13 +2.0; R2_4 crosses the 0.08 knife-edge (+0.084, was +0.078) costing a point; RACF13 −0.125 persists. |

**OOS movement gates (comments; the acceptance criterion):**

| spec | rel err | persistence | coverage |
|---|---|---|---|
| conditional state (2f-style baseline) | 0.170 ± 0.063 | 0.160 ± 0.068 | 60% |
| **conditional state + mix** | **0.159 ± 0.070** | 0.160 ± 0.068 | **100%** |
| unconditional + mix | 0.189 ± 0.085 | 0.160 ± 0.068 | 80% |

**Best comments OOS result recorded**: at par with persistence with 100% bootstrap-CI
coverage and the last-split held-out distribution EXACT at every horizon (dRank1
6/23 vs 6/26; dRank4 8/33 vs 8/34; dRank13 11/50 vs 11/50) — comments now sits where
subs sat at 2d ("satisfies the distributional gate criteria"), on a panel 4.5× longer
with a regime change inside it.

**Adoption:** recommend `--mix-hetero` in both platforms' working specs (FB Era A
full stack → 15/15; comments 2j+mix → best gate profile). Defaults unchanged
(committed baselines preserved). **Cumulative comments VR13 decomposition:** 0.355
(P3) → 0.284 (κ identified, 2i) → 0.232 (stationary factor, 2j) → **0.218 (mix, 2l)**
vs emp 0.136 — the remaining ~0.08 now points at the estimation-vs-scorecard
POPULATION asymmetry (the 70%-presence complete-column scoring filter selects
empirically quiet entities; sim tracked columns have no such selection) and the
still-slightly-light dispersion tail (0.697 vs 0.745), not at model dynamics.
Sharpest remaining items: (1) population-matched scoring or estimation-side
presence filters; (2) FB goal-2 head collisions (coll1 −0.167 at 15/15 goal-1 —
a churn mechanism, not a variance mechanism).

Reproduction:
```
python llm_fitting/minimal_rankdiff.py facebook_a --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor --two-scale --mix-hetero
python llm_fitting/minimal_rankdiff.py reddit_comments --top-k 12500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr --stat-factor --mix-hetero
python llm_fitting/rankdiff_kalman.py reddit_comments --oos --top-k 12500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --mix-hetero --conditional state
```

## 2m. 2026-07-03 — END-OF-DAY SYNTHESIS (supersedes 2h's next-actions list; 2h's cross-panel table still stands for the pre-2i baselines)

**Where the model sits.** One generative law — OU home (κ(z) D(h)-identified) +
fast [+ medium] transitory + identified measurement noise + ONE entity amplitude
(v_i, spread s, scaling permanent AND transitory equally: b ≈ 1, measured) +
stationary common level (ρ_L measured) + Gabaix rebirth on a pre-registered
top-coverage universe. Every parameter is identified from a declared moment or an
independent instrument; nothing is tuned to a score.

**Current best specs and results (all this session, era-disciplined):**

| panel | in-sample spec | goal-1 | churn | OOS spec | OOS verdict |
|---|---|---|---|---|---|
| FB Era A | full stack: md-vr + stat-factor + two-scale + mix (b=1.02) | **15/15** (first legitimate perfect card) | 0.072 | P1 spec-A calibrated (md6, no vr/mix) | **beats persistence 5/5** (0.114 ± 0.046 vs 0.144 ± 0.030), cov 60% |
| Reddit comments | md-vr + stat-factor + mix (b=1.08) | 10/15 (R2_4 at +0.084 knife-edge), churn 0.034 | 0.034 | md6 + mix + conditional state | **at par, 100% coverage** (0.159 ± 0.070 vs 0.160 ± 0.068); last-split held-out distribution exact at all horizons |
| Reddit subs | 2d/2e stack (locked) | 14/15 | 0.053–0.074 | conditional state (2f) | **beats persistence 4/5** (0.118 vs 0.168), cov 100% |

CAVEAT (declared): FB Era A's 15/15 stack is IN-SAMPLE; its OOS behavior is
untested and expected fragile (md-vr regressed FB gates on short train windows —
2i). FB's operational OOS spec remains P1's. Legacy guard 14/15 / 0.013 at every
commit; suite 41 tests green.

**Remaining problems, ranked (with the honest attribution):**
1. *Estimation-vs-scorecard population asymmetry* (comments VR13 residual ~0.08 and
   likely RACF13 −0.125): the scoring filter (70% presence, complete columns)
   selects empirically quiet entities; the sim has no observation floor/missingness.
   This is a MEASUREMENT-ALIGNMENT problem, not dynamics.
2. *FB goal-2 head collisions* (coll1 −0.167 at 15/15 goal-1): rank-1 changes hands
   52%/wk empirically vs 35% sim — a head churn mechanism (near-ties, within-week
   burst aggregation), plausibly the same physics as the Spec-A/Spec-B deep-tail
   divergence (posting intermittency).
3. *Data reconstruction* (owner decisions): comments 2021-07..2022-12 resume;
   single-source 2023 FB rebuild; account.id-keyed FB rebuild.
4. *σ_obs below the universe head*: "identified at the head, bracketed below" until
   an intermittency-aware floor exists.

**Next steps and their complexity tradeoffs (recommendation: spend the budget on
elegance + measurement alignment + breadth, NOT more dynamics):**
- *b = 1 restriction test* — REDUCES complexity. If b=1 holds (measured 1.02/1.08),
  the model factorizes: one rank-conditional process × one entity amplitude — the
  paper's law, and one fewer parameter.
- *Population-matched scoring* (apply the empirical observation floor to sim
  diagnostics) — zero new model parameters; should close the last honest comments
  residual; the "complexity" is in the measurement model where it belongs.
- *Breadth* (Wikipedia pageviews / YouTube / GitHub stars / app charts through the
  unchanged pipeline) — zero model complexity, one PLATFORMS entry each; this is
  what the target literature rewards most.
- *FB head-churn mechanism* — REAL new complexity (burst/near-tie machinery for one
  scorecard row); defer until the intermittency floor work motivates it
  independently.
- *Slowly-drifting temperament, comments two-scale, deeper conditional machinery* —
  diminishing returns; park.

## 2n. 2026-07-03 — The b = 1 restriction test: rejected as exact, adequate as law

**Question.** If b = 1 (the mix exponent, 2l), the model factorizes — one entity
amplitude scales everything, the lognormal renormalization is exactly 1, and one
fitted parameter disappears. Measured values were 1.02 (FB) / 1.08 (comments).
Tested at two levels, pre-registered: (i) entity-bootstrap CI of b̂ = s(h*)/s(1)
(B=500, joint resampling of the shared entity set); (ii) imposed b=1 vs measured b
under COMMON RANDOM NUMBERS (`--mix-b-fix 1.0`, new additive override; 42 tests).

**Moment level: REJECTED as exact.** comments b = 1.079 [95% CI 1.065, 1.093];
FB Era A b = 1.024 [1.008, 1.040]. At census scale (30–50k entities) the data can
tell: the permanent component disperses slightly MORE than movement. Note the
statistic is exact under the null (at b=1 the fast-share approximation vanishes),
so the rejection is clean w.r.t. the estimator's approximation. DECLARED CAVEAT:
per-entity secular drift (real rising/dying lifecycles) inflates per-entity
long-horizon variance dispersion and biases b̂ UP — consistent with the ordering
(comments 2.5-yr census 1.08 > FB 1.6-yr fixed panel 1.02); the truth is likely
closer to 1 than the point estimates.

**Results level: practically INDISTINGUISHABLE (all knife-edge flips, both
directions; CRN so differences are purely b):**

| run | measured b | imposed b = 1 |
|---|---|---|
| comments in-sample | 10/15, churn 0.034 (b=1.08) | **11/15, churn 0.027** (R2_4 back under: +0.079) — best comments card yet |
| FB Era A in-sample | 15/15, churn 0.072 (b=1.02) | 14/15 (Pers4 +5.8 vs tol 5), churn **0.038** |
| comments OOS conditional | 0.159 ± 0.070, cov 100% | 0.175 ± 0.077, cov 100% (beats persistence 2/5) |

**Verdict and adoption.** b = 1 is the correct FIRST-ORDER LAW (2–8% deviations,
results-indistinguishable at tolerance level) but is statistically rejected as
exact. Spec decision: `--mix-hetero` KEEPS measuring b (one already-computed
moment; respects the data; default behavior unchanged); the paper states the
factorized b = 1 law with the measured super-unit deviations as the refinement —
"one entity-specific amplitude, with the permanent share dispersing ~2–8% more
than movement, an excess that grows with panel length and is at least partly
lifecycle drift." Follow-up worth one run someday: recompute b with per-entity
linear detrending to bound the drift contamination.

**Serendipitous lead on the FB head-collision problem (2m item #2):** under CRN,
moving b from 1.02 to 1.00 moved sim coll1 from 0.351 to **0.478** (emp 0.518) —
FB head collisions are hypersensitive to the amplitude scaling of the top
entities' permanent volatility. The coll1 gap may need NO new mechanism, just the
head-amplitude interaction (near-tie gap structure × w_i of the specific head
pages) — investigate before building any burst machinery.
_[SUPERSEDED 2026-07-03 by §2o: the b-sensitivity was MC noise (seed SD ±0.15);
the real mechanism is the top-2 GAP, and the root cause is election-week
initialization — see 2o.]_

## 2o. 2026-07-03 — coll1 investigated: the b-sensitivity was MC noise; the real story is head-GAP initialization, and the dynamics are exonerated

Three-part investigation (30-seed MC distributions; empirical head anatomy;
conditional-sim test) of the 2n lead. All numbers this session; FB Era A, full
2l stack (s=0.890, b=1.024, ρ_L=0.48).

**(1) The 2n "hypersensitivity to b" is RETRACTED — it was Monte Carlo noise.**
coll1 has seed SD **±0.15** (30 seeds): b=1.02 → 0.421 ± 0.155, b=1.00 → 0.428 ±
0.150 (statistically identical); the 5-rep values 0.351/0.478 both sit inside one
distribution. Corollary for reading ALL past scorecards: head-collision rows at
5 reps carry ~±0.07 SE each (coll1), injecting ~±0.03–0.04 noise into churn err —
5-rep coll1 point-diffs at the head are not interpretable without bands.

**(2) The real systematic shortfall is modest and GAP-driven, not amplitude-driven.**
Sim means vs emp: coll1 0.425 vs 0.518 ± 0.054 (emp at ~p78 of the seed
distribution), coll2 0.655 vs 0.765, coll5 0.822 vs 0.906 — a correlated ~1–1.7σ
family, not the alarming −0.17 point estimate. Seed-level corr(top-2 gap, coll1)
= **−0.71**, and the sim's stationary top-2 log-gap is **0.77 ± 0.31 vs the
empirical 0.217** (stationary all era: quarter means 0.215/0.228/0.253/0.174).
The 2n amplitude-occupancy story is REFUTED by measurement: empirical rank-1
occupants are QUIETER than typical — occupancy-weighted v̂ = 0.47 vs population
median 0.71 (19 distinct #1 pages in 86 weeks: Ben Shapiro 20 wks, Occupy
Democrats 19, ...). The head churns because top gaps are TINY, not because
occupants are loud. (Mix-hetero still helps head churn on average: b=0 gives
0.376/0.581/0.796.)

**(3) Root cause: ELECTION-WEEK INITIALIZATION; the model's dynamics are fine.**
The unconditional sim seeds w0 and the OU homes from period 0 = **2020-11-02,
election week, whose top-2 gap (0.511) is 2.4× the era norm** — the home
anchoring then holds the head near that anomalous spacing while near-zero head κ
lets realized gaps wander wider (hence 0.77 ± 0.31). The decisive test: the
CONDITIONAL sim (real filtered gap structure) gives **coll1 = 0.576–0.584, coll2
= 0.734, coll5 = 0.904** vs emp 0.518 ± 0.054 / 0.765 / 0.906 — all within bands.
**Given the real state, the fitted dynamics produce the right head churn. No new
churn mechanism is warranted; 2m item #2 is closed as understood.**

**Recommendations (flagged, not implemented — each is a new experiment):**
1. Read unconditional head-collision rows with MC bands (or reps ≥ 20), and prefer
   the conditional diagnostic for head-churn claims.
2. Candidate initialization fix: seed w0/homes from the era-median rank-size curve
   (T_curve) or a mid-panel week instead of period 0 — would de-confound every
   unconditional head metric on panels whose first week is anomalous (FB Era A
   starts at a US election; comments period 0 is unremarkable, and comments coll1
   was near-exact, consistent with this account).
3. This likely also explains part of the FB churn-err bounce across specs
   (0.038–0.122): it is ±0.04 MC noise on top of an initialization bias.

## 2p. 2026-07-03 — The comments residual root-caused: DIRECTIONAL slow movement (lifecycle arcs), fixed with long-horizon moments (`--md-vr-long`)

**The question.** Most important remaining unexplained pattern after 2i–2o: the
comments residual cluster (VR4/8/13 +0.08..+0.12, RACF13 −0.12, R2 rows +0.06–0.08)
on the cleanest panel at the best spec.

**Hypotheses eliminated by measurement (in order):**
- H2 *lifecycle drift as constant trend*: dead — true drift dispersion ≈ 0 (raw
  0.0395 < its own sampling floor); per-entity constant demeaning moves knot D(13)
  by <1%.
- H1 *scoring-filter selection*: dead — complete-column vs ≥70%-presence median
  VR13 = 0.136 vs 0.140.
- H1′ *estimation-population (low-presence entities in the knot pool)*: dead —
  survivor-only knot VR13 0.235 vs 0.242.

**The discovery (same 989 entities, two statistics, empirical vs sim):** the raw
(pooled, undemeaned) long-horizon moments MATCH — sim knot-style VR52 = 0.083–0.088
vs empirical 0.083 — but **windowed per-entity demeaning removes 50% of empirical
slow variance vs only 28% of the sim's** (scored/knot ratio at h=52: 0.49 vs 0.72;
at h=13: 0.855 vs 0.96). Empirical slow movement is DIRECTIONAL within the window —
multi-year lifecycle arcs (rise-then-fall, invisible to constant-drift demeaning) —
while the fitted slow component (κ = 0.07, 10-week half-life) wanders out and back.
One cause, both residuals: directional slow movement preserves rank order (high
RACF13) while being demeaned out of scored VR (low VR13); diffusive wander does the
opposite. D(h ≤ 13) cannot separate κ≈0.005 directional from κ≈0.07 diffusive —
**D(26)/D(52) can, and T = 136 affords them.**

**Fix (`--md-vr-long`; ZERO new components/parameters — two moments and a flag).**
VR_MOM_H_LONG = (2,4,8,13,26,52), guarded to T ≥ 2.5h; composes with two-scale/
spec-B; 43 tests (exact recovery of κ=0.01 slow + φ₂=0.90 medium through the long
moment set); legacy guard 14/15 / 0.013 unchanged.

**Validation vs pre-registered predictions (5 reps; baselines = 2l/2n):**
- *κ_slow → ≤0.02 with the medium component absorbing the rest* — **PASS**:
  comments κ(z) = 0.005 flat (was 0.070–0.140), φ₂ = 0.85..0.70 with σ₂ =
  0.143..0.215 (two-scale finally ACTIVATES on comments, as the mechanism demands).
- *RACF13 recovers to within ±0.08* — **PASS**: −0.116 → **−0.070** (passes);
  RACF4 −0.004 (exact).
- *Scored sim VR13 → ≤0.16* — **FAIL on magnitude**: 0.217 → 0.211 (+0.075);
  the demeaning differential closed less than hoped.
- Un-pre-registered wins: comments **12/15 (best; only VR4/8/13 still fail)**,
  boundary flux best yet (outfluxK +0.038, return4K −0.095); **FB Era A 15/15
  with churn error 0.018 — the best FB card ever recorded on any spec** (h=26
  active at T=86; κ = 0.070..0.040).
- OOS comments (conditional + two-scale + long): 0.169 ± 0.062 vs persistence
  0.160 ± 0.068 — at par on error, but coverage 60% vs the 2l gate spec's 100%.

**Adoption.** `--md-vr-long` joins the IN-SAMPLE working specs on both platforms
(comments: + two-scale now included; FB Era A unchanged stack + long). The OOS
gate specs stay as recorded (comments: 2l md6 + mix + conditional, 100% coverage;
FB: P1 md6). Defaults unchanged.

**What remains, and the complexity verdict.** The comments VR4/8 mid-block
(+0.10..+0.12) is the last standing residual: empirical mid-horizon movement is
still more within-window-reverting than the model's. The next structural candidate
is explicit non-Gaussian lifecycle asymmetry (rise-fall arc dynamics — a
birth-growth-death component). That IS real new complexity (a lifecycle state per
entity), it would blur the clean factorized-law story, and the marginal target has
shrunk to one metric family that no gate flags — NOT currently worth it. Better
next uses of the budget stand (2m): breadth, figures, b-detrending bound.

## 2q. 2026-07-05 — External review (GPT-5.5 Pro) adjudicated: one proven bug found (Spec-B projection), small fixes applied, big items scoped to the next session

Full review saved verbatim-in-substance at `llm_fitting/reviews/gpt55_pro_review_2026-07-05.md`.
Overall verdict: the covariance algebra, the D(h) identification logic, `_sqw`, the
stationary-factor mapping, and the temperament correction chain were all **verified
correct** by independent derivation (their B1/B3/B6/B10/B11); the flat-SSE degeneracy
we found empirically in 2i is exactly what mixture-of-exponentials theory predicts.
The review's real contributions are one proven algebraic bug and a set of
evidentiary-package critiques.

**THE PROVEN BUG (their B12/D7, confirmed against the code): Spec-B projection
non-identification.** The Toeplitz bases sum to the all-ones matrix J, and the
week-mean centering annihilates J (CJC = 0), so the centered residual covariance
leaves one Toeplitz direction unidentified; `np.linalg.lstsq`'s min-norm convention
resolves it silently, making BOTH `sigma_d` AND the reported floor pᵀΣp
convention-dependent by an additive constant. The invariant quantity — and the
defensible lower bound, since the within-week common component is confounded with
weekly signal — is the centered floor pᵀCΣCp. FIXED (diagnostic, additive):
`_toeplitz_floor` now returns both; the curve table gains `sigma_obsB_cent`; pinning
still uses the legacy floor pending the P0 rerun below. **First audit (subs, K=5000,
this session): centered floor sits 26% below the legacy floor at the head (0.074 vs
0.100 at rank ~800), −15% mid, −5..−7% deep.** Consequence: the 2e head-agreement
claim (Spec-A 0.117 vs floor 0.100, ~17%) weakens against the invariant bound
(0.117 vs 0.074, ~58%); mid-band agreement survives (0.148 vs 0.130). Until the P0
adjudication+re-pin, σ_obs language is: **bounded within [centered floor, Spec-A]
at the head; identified shape; exact head identification pending.** FB Era A and
comments audits BLOCKED this session: /Volumes/T9 not mounted (flagged to owner).

**Other fixes applied now:**
- OOS-gate denominator rule (their C4): declared `MOM_FLOOR = 0.02` — eval keys with
  |empirical| below it are excluded from the relative-error mean. All recorded clean
  splits had every |emp| ≥ 0.04, so no recorded number changes; only the documented
  degenerate coll1=0 split (2g-X P3) is affected.
- README language (their E5): stale "s ≈ 0.9 on both platforms" → metric-dependent
  values; "every parameter is moment-identified or instrument-identified" → adds
  "or train-only-calibrated and declared" + the head-identified/bracketed-below
  σ_obs scoping. (Paper drafts inherit the E5 audit; internal log language stays
  confident-and-correct, not hedged.)
- Their R3 was already run before the review (2p): comments long-stack conditional
  OOS = 0.169 ± 0.062, coverage 60% — matches their prediction; cited, not rerun.

**Accepted and scoped to the next session (see handoff):** P0 Spec-B adjudication +
re-pin + rerun of every spec-B-dependent result; P1 paper-primary stack freeze +
full spec×gate matrix incl. their R2 (FB full-stack OOS — genuinely never run);
P2 frozen confirmation protocol on the comments 2021-07..2022-12 extension;
P3 uncertainty-aware scorecard (bootstrap + MC bands, omnibus Q) and MC bands on
all churn rows; P4 b robustness (detrended, block-bootstrap, split-window) +
temperament κ_acf lag-depth sensitivity (their B7); P5 interpretation uniqueness
(phase-randomized surrogates for the 2p demeaning differential; per-entity κ_i
heterogeneity probe); P6 population-matched scoring; P7 breadth.

**Noted, no action:** their C5 (splits are dependent — we already report per-split
values; reporting practice for the paper); A6 (the 15/15 card was never claimed as
a hypothesis test internally — P3 is the paper answer); their suggested replacement
language is in places more hedged than the evidence requires — adopt precision,
not tepidity.

## 2r. 2026-07-05 — Spec-B centered floor ADOPTED (P0): the invariant pin fixes a floor violation, ties the calibrated FB gate, and every qualitative conclusion survives

**Adjudication (completes 2q's half-fix).** The centered floor pᵀCΣCp is now the
PINNED quantity: `spec_b_curve` returns it as `sigma_obs` by default
(`floor="legacy"` reproduces committed numbers; both columns always in the
table). Declared convention: the within-week common component is confounded
with weekly signal (CJC = 0 null direction) and counts as signal in a floor.
Locked by `tests/test_spec_b_projection.py` (non-identification made exact:
Σ and Σ+δJ are observationally equivalent after centering and the centered
floor equals the true invariant; convention switch pinned). Suite 45 passed;
legacy guard untouched.

**Audits complete (all three platforms, centered vs legacy by band):**

| platform | head | mid | deep |
|---|---|---|---|
| subs K=5000 (2q) | 0.100→0.074 (−26%) | −15% | −5..−7% |
| FB Era A K=3500 | 0.207→0.157 (−24%) | −15..−17% | −14% |
| comments K=12500 | 0.101→0.071 (−30%) | −19..−25% | −13% |

**The decisive detail: the legacy floor was VIOLATED on FB** — Spec-A head
(0.176) sat BELOW the claimed floor (0.207), an internal contradiction the
min-norm convention manufactured. Under the centered floor the ordering
Spec-A ≥ floor holds at every band on every platform. The centered convention
is not just the algebraically identified one; it is the only one consistent
with Spec-A.

**Reruns of every spec-B-dependent result (all this session; legacy → centered):**

| result | legacy (recorded) | centered (this session) | verdict |
|---|---|---|---|
| subs in-sample (2e stack) | 14/15, churn 0.053 | **14/15**, churn 0.102 (same sole miss R2_13 +0.102; coll rows carry ±0.07 5-rep SE, 2o) | holds |
| subs OOS pinned | 0.215 ± 0.059, cov 100% | **0.218 ± 0.028, cov 100%** | holds |
| FB Era A in-sample spec-B (P2) | 10/15, churn 0.081 | **10/15**, churn 0.106; RACF1 +0.063 / RACF4 +0.014 pass; head σ_trans = 0.000 exactly | holds |
| FB OOS spec-B uncond | 0.211 ± 0.030, cov 40% | **0.152 ± 0.040**, cov 40%, scale 1.0 ×5 | **improves** |
| FB OOS spec-B + cond state | 0.164 ± 0.049, cov 40% | **0.118 ± 0.038, cov 60%, beats persistence 4/5** (vs 0.145 ± 0.031) | **improves** |
| comments in-sample pinned (P3) | 9/15, churn 0.026, VR +0.12..+0.21 | **9/15**, churn 0.045, VR +0.12..+0.21 (block unchanged) | holds |
| comments OOS pinned | clean-4 mean 0.213, cov 100% | 0.187 ± 0.058, cov 60% (5 splits; MOM_FLOOR=0.02 active — no coll1=0 explosion) | holds (at par: persistence 0.165 ± 0.062) |

**The headline gain: FB spec-B + conditional state at the centered pin =
0.118 ± 0.038, coverage 60%, beating persistence on 4/5 splits — statistically
indistinguishable from the calibrated Spec-A operational spec (0.114 ± 0.046,
P1) with ZERO calibration freedom used (scale = 1.0 on 5/5 splits).** The
fully-identified FB observation model now clears the gate at the calibrated
spec's level; the 2q inversion (conditioning helps FB under spec-B) stands and
strengthens (0.152 → 0.118).

**The 2e "component eliminated" claim, re-measured:** with the lower centered
pin, subs σ_trans no longer collapses to exactly 0 — it lands at 0.026..0.077
(head..tail) against σ_perm 0.132..0.103 and σ_obs 0.074..0.222; at the head
that is ~4% of the permanent variance. Superseding language: the transitory
component is NEAR-eliminated (σ_trans ≤ 0.08 everywhere, a minor component at
every band), not identically zero. The reduction "OU home + identified noise +
temperament + rebirth" remains the right description of the subs weekly model.

**Final σ_obs language (per the review's decision rule):** identified in SHAPE
on all three platforms (12-band curves, two independent instruments). At the
head: **FB is identified** — Spec-A 0.176 vs centered floor 0.157 agree within
~12% (the agreement the legacy convention faked at 18% while violating the
bound). **Subs and comments are bounded**: Spec-A sits ~1.6× above the centered
floor at the head (0.117 vs 0.074; 0.120 vs 0.071), so σ_obs lies within
[centered floor, Spec-A] — a bracket, not a point. Below the head: bounded on
all platforms (the depth-growing Spec-A/Spec-B divergence of P2/P3 is
unchanged by the re-pin and remains the intermittency question). The
cross-platform irony is real and reportable: the platform without daily data
history (FB) is now the one with head identification, because its weekly
Spec-A and daily floor coincide there.

**Superseded numbers:** every `sigma_obsB` value in 2e/2g-X P2/P3/P4-era tables
is the legacy convention; the centered column is canonical from here. 2e's
"σ_obs identified, not calibrated" scopes to: FB head identified; elsewhere
bounded-within-bracket + shape.

## 2s. 2026-07-05 — PAPER-PRIMARY STACK FREEZE (P1): the full spec×gate matrix, every cell filled, failures included

**The run that was never run (review R2): the FB full in-sample stack through
the OOS gate — it LOSES, as 2i predicted.** facebook_a K=3500, temper + pool +
md6 + t + md-vr-long + two-scale + mix, rolling origins 21/32/43/54/65:
rel err **0.170 ± 0.040 vs persistence 0.145 ± 0.031, CI coverage 20%**, wins
2/5 splits, held-out displacement tail truncated (last-split p90 43/69/136 vs
emp 68/122/203). Mechanism already on record (2i): D(h)-moment estimation needs
long train windows; on 21–65-week rolling windows the noisy D moments
destabilize the partition and squash the tail. This is a FINDING about
estimator/sample-size scope, not a model defect — the same stack at T=86 full
sample produces the 15/15 in-sample card with near-exact D(h) curvature.

**The complete stack × {in-sample card, OOS gate} matrix (sources dated):**

*FB Era A (K=3500, T=86):*

| stack | in-sample card | OOS movement gate |
|---|---|---|
| FULL: md-vr-long+stat-factor+two-scale+mix | **15/15, churn 0.018** (2p) | 0.170 ± 0.040, cov 20% — **loses** (2s) |
| calibrated Spec-A: temper+pool+md6+t | 8/15, churn 0.122 (2g-X P1) | 0.114 ± 0.046, cov 60%, **beats persistence 5/5** (2g-X P1) |
| identified Spec-B (centered pin) + cond state | 10/15, churn 0.106 (2r) | **0.118 ± 0.038, cov 60%, beats 4/5, scale 1.0×5** (2r) |

*Reddit comments (K=12,500, T=136):*

| stack | in-sample card | OOS movement gate |
|---|---|---|
| LONG: md-vr-long+stat-factor+two-scale+mix | **12/15** (2p) | 0.169 ± 0.062, cov 60% (2p; cited, not rerun) |
| gate spec: md6+t+mix + cond state | 10/15, churn 0.038 (2s, this session) | **0.159 ± 0.070, cov 100%, at par** (2l) |
| identified Spec-B (centered pin) | 9/15, churn 0.045 (2r) | 0.187 ± 0.058, cov 60% (2r) |

*Reddit subs (K=5,000, T=30):*

| stack | in-sample card | OOS movement gate |
|---|---|---|
| 2d/2e stack: md6+t (Spec-A) | **14/15, churn 0.053** (2e) | uncond 0.171 ± 0.017, cov 100% (2d); **+cond state 0.118 ± 0.061, cov 100%, beats 4/5** (2f) |
| identified Spec-B (centered pin) | 14/15, churn 0.102 (2r) | 0.218 ± 0.028, cov 100% (2r) |

**PAPER-PRIMARY DECLARATIONS (frozen; the registration doc in P2 pins these):**
- *Structure estimand (in-sample card):* FB Era A = FULL stack; comments =
  LONG stack; subs = 2d/2e stack. These are descriptive in-sample cards under
  the working diagnostic stack (P3 adds bands + Q).
- *Movement estimand (OOS gate):* **FB = identified Spec-B (centered pin) +
  conditional state** — chosen over the calibrated Spec-A spec because it is
  point-indistinguishable (0.118 vs 0.114) with ZERO calibration freedom
  (scale 1.0 on 5/5); Spec-A stays as the declared sensitivity spec.
  Comments = md6+t+mix + conditional state (0.159, 100% coverage).
  Subs = md6+t + conditional state (0.118, 100% coverage, beats 4/5).
- *The paper reports the two estimands with different stacks BY DESIGN*, with
  the reason on the table: multi-horizon D(h) moments identify slow structure
  on full panels and destabilize short rolling train windows — a sample-size
  scope condition, stated and demonstrated (this section), not a
  specification search.

## 2t. 2026-07-05 — Uncertainty-aware scorecard (P3): bands + omnibus Q on the three headline cards; churn rows get MC bands everywhere

**Tool (`llm_fitting/scorecard_bands.py`, additive).** (a) Empirical bands:
joint entity bootstrap (resampled tracked columns; cross-metric covariance
captured) for VR/ACF/RACF/R2/dRank; moving-block bootstrap over weeks for
coll*/outfluxK/return4K; Pers{h} is a single-origin set overlap with no
empirical sampling band (MC band only — DECLARED; its z/Q contribution treats
the empirical value as noiseless and is therefore harsh). (b) Sim MC bands:
reps=20, cached seeds 0..19, full MC covariance. (c) Omnibus
Q = dᵀΩ⁺d over the 15 card moments, Ω = Cov_boot + Cov_MC/reps, 50% diagonal
shrinkage — a covariance-weighted DESCRIPTIVE distance (χ² df=15 is a
reference scale, not a test; moments overlap in windows).

**Headline cards at the §2s structure-primary stacks (reps=20, boot=100):**

| panel | threshold card (20 reps) | omnibus Q (ref: mean 15, p95 ≈ 24) | largest goal-1 z |
|---|---|---|---|
| FB Era A (full stack) | **15/15, churn 0.017 — holds at 20 reps** | 298 | ACF2 +3.2; all VR/RACF/R2 rows within ±2.5 z |
| subs (2d/2e stack) | 14/15, churn 0.056 | 217 | VR4 +6.3, RACF13 −3.4 (2j-known) |
| comments (long stack) | 12/15, churn 0.036 | 1275 | VR4 +25, VR13 +21 — the residual block, now in SDs |

Key readings, ground truth for the paper's SI-5:
- **The FB 15/15 card survives MC scrutiny**: every rank-dynamics row within
  ~±0.02 of razor-thin empirical bands; **coll1 diff is +0.028 with MC SD
  ±0.119** — the head-collision "problem" is formally within noise at 20
  reps, closing 2o's recommendation #1 (bands now standard tooling).
- **Q separates the platforms the thresholds blur**: FB 298 vs comments 1275.
  At census-scale precision (4,000–9,500 bootstrap entities) the model is a
  tight descriptive approximation everywhere and the exact truth nowhere —
  Q is the honest version of the review's A6/C3 point, and the comments VR
  block (z ≈ 21–25) is the ONLY place where the misfit is an order of
  magnitude above everything else.
- Churn rows now carry bands by default (empirical block-bootstrap + MC):
  comments coll1 +0.017 ± 0.061, subs coll1 −0.076 ± 0.27 — no churn row on
  any headline card is outside its joint band except comments coll10 (−0.138,
  z −4.6) and the boundary-flux pair (outfluxK +0.038, return4K −0.095, huge
  z from tiny bands) — the sharpest remaining goal-2 targets, stated in SDs.

## 2u. 2026-07-05 — b robustness (P4): the exact-b=1 rejection does NOT survive time-window uncertainty; b = 1 is the law, the excess is long-horizon lifecycle contamination

**Tools:** `llm_fitting/b_robustness.py` + additive `detrend`/`acf_lags`
options on `estimate_temperament`/`estimate_mix_b` (defaults byte-identical;
detrend = per-entity LINEAR detrending of the change series, i.e. quadratic
level detrending — the per-entity variance already removes constant drift,
so this is the leading-order arc term; ν loses one df).

**Results (both platforms, all this session):**

| variant | FB Era A (K=3500) | comments (K=12,500) |
|---|---|---|
| baseline b (committed) | 1.024 | 1.079 |
| detrended b | **1.024 (unmoved)** | **1.059 (−0.020, toward 1)** |
| split-window b (h=4): first / second | 0.968 / 0.995 | 1.042 / **0.994** |
| full-window b at common h=4 | 1.011 | **1.002** |
| block-bootstrap over weeks (median, 95% CI) | 0.983, **[0.939, 1.064]** | 1.005, **[0.958, 1.108]** |

- **Pre-registered prediction PASSES**: detrended comments b moves toward 1
  more than FB (−0.020 vs 0.000) — the super-unit excess carries the
  lifecycle-drift signature, as 2n's declared caveat predicted.
- **b = 1 sits inside the block-bootstrap 95% CI on BOTH platforms.** The 2n
  rejection came from entity-bootstrap CIs that treat the time window as
  fixed (review B9); once week-block uncertainty is counted, exact b = 1 is
  not rejected. Note the bootstrap medians (0.983 / 1.005) sit below the
  full-window points (1.024 / 1.079): block resampling limits contiguous
  spans to ~3h*, which strips the slow multi-year structure — the level
  shift is itself evidence that the excess lives at long contiguous
  horizons, not in local dynamics.
- The excess is HORIZON-SPECIFIC: at the common h=4, comments b = 1.002 and
  the second-half window gives 0.994. The 1.08 exists only at h*=13 over the
  full 2.5-year window — exactly where lifecycle arcs accumulate.
- **Temperament κ_acf sensitivity (review B7): empirically NIL.** Pooled-ACF
  depth 1→6 moves κ by <0.002 and s by <0.0001 on both platforms
  (FB: κ 1.3328→1.3345, s 0.8901 flat; comments: κ 1.3418→1.3434, s 0.6922
  flat) — within-entity dependence beyond lag 2 carries no κ mass; the
  s-biased-up concern is closed by measurement.

**Conservative b bound (the deliverable): b ∈ [0.94, 1.11] across every
variant and platform, with all central estimates in [0.98, 1.08] and b = 1
inside every time-aware CI.** Paper language upgrades from 2n's: the
factorized b = 1 law is not merely "adequate" — exact b = 1 is not rejected
once time-window uncertainty and the arc-linear term are accounted for; the
committed measured-b spec remains the default (respects the data), with the
super-unit point estimates attributed to long-horizon lifecycle structure.

## 2v. 2026-07-05 — Interpretation uniqueness (P5): surrogates say "excess low-frequency structure", and hand the VR residual a new decomposition; κ_i heterogeneity is real

**Tool (`llm_fitting/surrogate_test.py`).** Multivariate phase-randomized
surrogates of the comments complete-column scored population (n=9,511,
T=136, 50 draws): one common phase rotation on the increment panel per draw —
preserves every entity's increment amplitude spectrum AND all cross-spectra
(common factor, any stationary long memory); destroys only phase alignment
(the directional/arc structure). Statistics: demeaning-survival ratio ρ(h),
scored VR_sc(h), population-internal RACF (declared: differs from the
scorecard's full-universe RACF).

**(a) The pre-registered verdict is SPLIT, and the split is informative:**

| stat | empirical | surrogate 95% band | inside? |
|---|---|---|---|
| ρ(52) demeaning survival | 0.579 | [0.314, 0.634] | **yes** |
| ρ(26) | 0.845 | [0.670, 0.845] | yes (edge) |
| RACF13 (internal) | 0.309 | [0.313, 0.475] | no — **below** |
| VR_sc(13) | 0.136 | [0.167, 0.180] | no — **below** |
| VR_sc(52) | 0.042 | [0.045, 0.066] | no — below |
| RACF1 (internal) | 0.684 | [0.723, 0.783] | no — below |

The long-horizon demeaning loss IS reproducible by a stationary Gaussian
process with the empirical spectrum — so per the pre-registered rule, §2p's
"directional lifecycle arcs" softens to **"excess low-frequency structure,
consistent with lifecycle arcs but not uniquely established"** (the review's
A10/B14/D3 point, conceded by measurement). But the surrogates FAIL the rest
of the joint pattern in a direction nobody predicted: given the data's own
spectrum, Gaussian phase-random dynamics produce MORE scored persistence than
the data shows (VR_sc(13) 0.176 vs 0.136; RACF13 0.406 vs 0.309).

**The new quantitative lead this hands the program:** a process with EXACTLY
the empirical second-order structure overshoots scored VR13 by **+0.04** —
half of the model's remaining +0.08 residual — because the scored-VR
functional (median over entities of demeaned variance ratios) is not
spectrum-determined; the data's non-Gaussian phase/marginal structure pulls
it down. Chasing the last VR gap with linear-Gaussian dynamics is therefore
chasing a functional artifact for ~half the distance (connects review B15).
The honest target for any future dynamics work is the surrogate-adjusted
residual (~+0.04), not the raw +0.08.

**(b) κ_i heterogeneity probe (fine-bands' missing axis).** Per-entity
log VR13 curvature, demeaned within 5×5 rank×volatility cells, split-half:
Spearman **0.346**, noise-corrected true SD **0.302** (39% of observed
variance is signal). Persistent per-entity reversion-rate heterogeneity is
REAL beyond rank and volatility conditioning (2j's unconditioned 0.34
survives conditioning). D1 stays open as a measured, bounded axis — a
per-entity κ_i extension has ~0.30 log-SD of structure to work with; not
implemented (parsimony verdict of 2p stands, sharpened by (a)).

## 2w. 2026-07-05 — Population-matched scoring (P6): the prediction FAILS cleanly — the missingness explanation for the comments VR residual is dead

`scorecard_bands.py --censor`: rank-based weekly censoring of the simulated
tracked population at the empirical presence fraction (worst simulated rank
first), then identical complete-column scoring. Comments, long stack,
reps=20: **every VR row moves < 0.001** (VR13 +0.078 → +0.079; Q 1275 →
1277). Root cause measured: the comments scored census population is 99.0%
present per week (mean weekly absence 1.04%, max 7.9%) — there is nothing to
match. The 2l/2m residual attribution #1 ("estimation-vs-scorecard population
asymmetry / observation floor") is REFUTED on comments as a sim-side
mechanism, completing the kill chain started in 2p (H1/H1′ were the
empirical-side versions). What remains of the comments VR block is (i) the
functional/marginal-structure component measured in §2v (~+0.04) and (ii) a
genuine dynamics residue of similar size. Per the handoff decision rule, P5
alternatives were already revisited (§2v) — the surrogate decomposition IS
the revised account.

## 2x. 2026-07-05 — Revised external verdict adjudicated; THE research agenda (supersedes 2m's next-actions and 2q's scoped list)

Revised verdict saved verbatim at
`llm_fitting/reviews/gpt55_pro_revised_verdict_2026-07-05.md`. Headline:
upgraded from "technically promising but vulnerable" to "credible, deeply
validated model-discovery paper; remaining PNAS risks are confirmation and
breadth, not internal statistical mechanics." All six of the original
review's major internal objections are adjudicated closed (Spec-B projection,
full-stack OOS, coll1 MC noise, b=1 rejection, lifecycle uniqueness,
missingness). Fixed now: the stale legacy-map prose in `spec_b_sigma_obs.py`
(their B1 implementation note). Accepted framing changes, binding on all
future write-ups: b=1 is the MAIN model (measured b = refinement table);
structure stack vs movement stack is a MAIN-TEXT distinction with a "target"
column; Q is a residual-localization device, never a pass/fail gate ("15/15"
is descriptive; Q rejects exact equality as expected at census scale); σ_obs
= "identified in shape everywhere, identified in level at the FB head,
bounded in level elsewhere"; "excess low-frequency structure" replaces
lifecycle-arc lead language; the law is "dominant one-amplitude
factorization", not "all heterogeneity is amplitude" (κ_i measured, bounded,
second-order).

**REVISED AGENDA (in order):**
1. **Confirmation extension (owner-gated: mount WD)** — run the registered
   E1–E3 exactly. Do NOT submit to PNAS before this. No model work first.
2. **Breadth (owner-gated: data acquisition)** — 2+ ranked systems through
   the unchanged pipeline, Wikipedia pageviews first; claim to test =
   amplitude collapse (s(h) flat, b≈1) + OOS-at-par, not 15/15.
3. **Manuscript claim-set rewrite** (no data needed) — adopt §4 of the
   revised verdict: main claim, FB movement ordering (identified Spec-B
   first, calibrated 5/5 as SI), scorecard/comments language, spine sentence.
4. **Q block decomposition** (no data needed; their B5) — report Q by
   VR / ACF+RACF / persistence / churn / boundary blocks so the omnibus
   number localizes instead of condemns.
5. **Pre-register κ_i as a secondary diagnostic on the extension** (their
   B4/rec 4) — dated CONFIRMATION_PROTOCOL amendment BEFORE any extension
   data processing: train-only EB κ̂_i, hard shrinkage, adopt only if it
   improves frozen OOS / predicts the confirmed residual.
6. **Complete the §2v functional decomposition** (no data needed) — model-side
   surrogates: phase-randomize FITTED-model paths to measure how much of the
   +0.04 scored-VR functional gap the existing t-tails already produce;
   sets the honest dynamics target, feeds SI.
7. Weighting robustness (first review B4, still unmeasured): identity vs
   diagonal-precision MD weighting on one platform.
8. Figures: γ-tail + SSE(a) flat-to-V (Fig 2); Spec-A vs centered Spec-B
   with identified/bracketed shading (Fig 3); s(h) flatness + b-variants
   table (Fig 4); SI discovery chronology.
9. PARKED: κ_i implementation (until #1 says otherwise); population-matched
   scoring (dead, SI negative result); lifecycle-state models (target now
   ~+0.04 after §2v); data reconstruction decisions (owner).

## 2y. 2026-07-05 — Same-day follow-through on the revised verdict: Q localization, the functional-gap decomposition completed, weighting audit nil

Executes §2x items 4–7 (no new data). Item 5 (κ_i pre-registration) is
CONFIRMATION_PROTOCOL amendment A1; item 3 (claim-set rewrite) landed in
README + research_notes positioning update + §6 addendum.

**(1) Q block decomposition (`scorecard_bands.py`, review B5).** Per-block
Q_b/df at the structure-primary stacks (reps=20, boot=100; churn/boundary
bootstrapped JOINTLY over week blocks so their Ω is real):

| block (df) | FB Era A | subs | comments |
|---|---|---|---|
| VR (4) | 3.2 | 8.9 | **263.5** |
| ACF/RACF (5) | 3.5 | 9.5 | 18.8 |
| R2 (3) | 0.7 | 1.9 | 17.2 |
| Pers (3, MC-only — declared harsh) | 89.3 | 44.8 | 32.0 |
| churn (7) | 3.9 | 10.0 | 5.0 |
| boundary (2) | 669 | 85 | **927** |

The localization the verdict asked for, now on the record: FB's entire
omnibus excess lives in the no-empirical-band Pers rows and the boundary
pair; every dynamics block sits at Q/df ≈ 1–4. The comments story is two
blocks — the known VR residual and boundary flux (outfluxK +0.038 /
return4K −0.095) — while its churn block (5.0) is essentially fine. The
paper's Fig/SI table: "the only practically meaningful residuals are here."

**(2) Model-side surrogates (`surrogate_test.py --model`) — the §2v
decomposition is complete.** Phase-randomizing FITTED-model paths (long
stack, 3 seeds × 20 surrogates, comments): the model's own Gaussianized
equivalent scores VR_sc(13) only +0.010 above the model (data-side gap:
+0.039). **The existing t-tails supply just 24% of the data's
non-Gaussianity discount.** Final decomposition of the comments VR13 gap
(+0.078): ≈ +0.03 missing non-Gaussian phase/marginal structure (the data's
increments are burstier/more phase-structured than t-innovations produce)
+ ≈ +0.03–0.04 genuine second-order (spectrum) mismatch. Consistent
cross-check: model ρ(52) = 0.752 vs data 0.579 — the model still survives
demeaning more than data, the residual low-frequency signature of 2p. Any
future dynamics work has TWO measured targets, each ~0.03, of different
kinds (marginal structure vs spectrum) — neither justifying a lifecycle
state yet.

**(3) MD weighting robustness (`weighting_robustness.py`, review B4):
identity weighting is empirically INERT.** Pooled + head-quintile panel
moments with entity-bootstrap SEs, identity vs 1/SE diagonal weighting:
subs (γ only, 4× SE spread) — identical to 3 decimals; comments with the
FULL long-D set (γ0..6 + D(2..52), **55× SE spread at the head**) — κ
grid-identical (0.020), φ within one grid step, σ's within 5%. The
heterogeneous-precision concern does not move any parameter that matters;
one-line SI answer.

## 2z. 2026-07-06 — Metrics audit adjudicated (two independent audits, cross-reviewed): suite architecture CONFIRMED; community-canonical + proper-score layer added (`community_metrics.py`, `--dist-scores`)

**The audit question:** are the goal-1 (aggregate structure) and goal-2 (movement)
metrics theoretically correct and what PNAS referees expect?  Two independent
audits (this program's + an external model's), adjudicated head-to-head.

**Verdict: the core architecture stands.** The 15-card + Q + block decomposition
is already the answer to the standard "N/15 scorecard" critique (Windrum/Fagiolo/
Moneta 2007; Fagiolo et al. 2019 — threshold counts discard magnitude; the correct
summary is a covariance-weighted quadratic form, which Q is).  The frozen OOS gate
vs persistence is rarer than it should be in the ranking-dynamics literature
(Iñiguez et al. 2022 validate by qualitative overlay only).  NOTHING in the card,
`_score`, churn err, or the gate criterion changed; CONFIRMATION_PROTOCOL is
untouched.

**Gaps found and filled (all ADDITIVE, descriptive-only; `community_metrics.py`,
16 new tests, suite 61 green):**
1. *Community-canonical observables* (near-obligatory for Nat Comms/PNAS referees
   from the Iñiguez/Gershenson/Cocho lineage): rank-change curve C(r) as a FULL
   log-spaced curve (generalizes coll{cr}); rank flux F + turnover ō with the
   literature's names (F ≡ outfluxK); rank diversity d(k) — cumulative
   distinct-occupant structure that per-week-pair collision rates cannot see.
2. *Direct ladder metrics* (the goal-1 estimand was never scored directly;
   `_ranksize` was computed but unused): D_ladder (RMSE of the time-mean
   rank-size curve over log-rank bins) and D_share (max concentration-curve gap;
   FB language "of tracked activity", census language for Reddit).
3. *Rolling-origin R2_h and Pers_h* — the committed card rows are period-0
   anchored (a first-week statistic, not a stationarity statistic; FB Era A's
   period 0 is the 2020 election week, §2o).  Rolling variants reported
   alongside; committed rows stay as the regression guard.
4. *Rank-band transition matrices* (h=1/4/13) with occupancy-weighted TV
   distance — the mobility kernel, catching directional/band asymmetries that
   pooled |Δrank| medians miss.
5. *Proper scores on the OOS gate* (`rankdiff_kalman --oos --dist-scores`,
   opt-in, frozen criterion unchanged): ensemble CRPS skill vs persistence
   (Gneiting–Raftery 2007; the strictly proper generalization of the rel-err
   vector), pooled-PIT predictive quantile coverage at 10/50/90, and the
   Wasserstein REFERENCE SCALE (model W1 vs persistence W1 — a bare W1 has no
   scale).  DECLARED: the existing "bootstrap-CI coverage" is a
   model-median-in-empirical-sampling-CI check, NOT predictive coverage — paper
   language must say so; the PIT coverage is the calibration statistic.
6. *Cross-file inconsistency flagged (not yet changed):* card dRank pools
   rank ≤ 200 (`minimal_rankdiff:1139`) vs the OOS gate's cap = 100
   (`rankdiff_kalman:432`) — same name, different population; harmonize or
   rename before submission.

**Smoke run (facebook legacy panel, K=3500, default estimator, reps=2 — NOT a
paper number):** the new layer immediately localizes known physics: d(1) emp
0.216 vs sim 0.074 (the data cycles ~19 distinct rank-1 occupants in 88 wks,
the unconditional sim ~6 — the §2o head-gap initialization signature in
identity space); transition kernel shows the sim missing 2+-band jumps
(emp 0.5–4% vs sim ≈ 0); rolling Pers1 emp 24.8 vs the period-0 card row 20.0
(the anchoring bias, measured).

**Rejected after consideration (status-quo prior honored):** replacing card
composition/thresholds; energy score; RBO (tunable p invites quibble; rolling
top-k overlap + collisions cover it); NDCG/Kendall (mismatched to open
top-heavy lists); formal Diebold–Mariano on 5 dependent splits (per-split
reporting stands); GSL-div.  Paper-side to-dos from the audit: (F, ō)
placement figure among the Iñiguez 30 systems; P(x,t) displacement-overlay
figure; M&M table of fitted vs validation-only moments.

Reproduction:
```
python llm_fitting/community_metrics.py facebook_a --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long --stat-factor \
    --two-scale --mix-hetero --reps 20 --print-trans
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state --dist-scores
```

## 2z-a. 2026-07-06 — METRIC HIERARCHY (binding communication rule) + the paper-grade re-measurement: the smoke "errors" were spec artifacts; ONE real, previously-unscored residual found and characterized (stationary head law too wide)

**The hierarchy (how to talk about the suite; the metric COUNT is not the
worry-count — each metric has exactly ONE role and only Tier 0 can reject):**

| tier | role | contents | semantics |
|---|---|---|---|
| **0 — acceptance gate** (frozen, OOS) | adjudicates the model | rel err vs persistence + CI coverage (per platform) | pass/fail; registered (CONFIRMATION_PROTOCOL) |
| **1 — descriptive cards** | summarizes fit, preserves program-history comparability | 15-card + churn err + boundary pair; Q + per-block Q/df localizes | thresholds are descriptive; Q never gates (§2x) |
| **2 — presentation/diagnostic curves** (2z layer) | referee legibility + residual localization | C(r), d(k), F/ō, D_ladder/D_share/ladder-drift/S(1), rolling R2/Pers, transition kernels; OOS CRPS/PIT/W1-ref | NO thresholds, never gate; a Tier-2 signal is PROMOTED to a measured residual (like below), never to a new pass/fail row |
| 3 — probes | one-off experiments | surrogates, b_robustness, era_replication, head_diversity_probe, ... | session tools |

Decision surface = 2 numbers/platform (Tier 0). Description = ~18 (Tier 1).
Everything else is figures. The Tier-2 families are re-expressions of Tier-1
physics in community coordinates (d/C/coll = identity churn; F/ō = boundary;
kernel = dRank; CRPS/PIT = the gate's distributional check) — they add
referee-native views, not new obligations.

**Paper-grade re-measurement (FB legacy full stack, subs 2d/2e stack, 10 reps).**
The default-estimator smoke's alarming numbers were SPEC artifacts, gone at the
paper stacks: FB d(1) 0.291 vs emp 0.216 (was 0.074 — no identity-diversity
deficit; slight excess); head long jumps present with t-tails on (kernel row
1-10 ≥2-band mass sim 0.093 vs emp 0.061 — over, not under). Subs: near-exact
everywhere (kernel TV 0.018–0.05; rolling R2/Pers exact to 2 decimals; ladder
diff a uniform −0.1 level cosmetic). Two KNOWN residuals re-confirmed and
better localized: FB boundary out-flux deficit lives in mid/deep bands' big
falls (rows 51-200/201-1000 "out": emp 0.021/0.041 vs sim 0.003/0.009 — partly
instrument absence per 2g#4, not behavior); subs head-zone over-mobility at
ranks ~5–25 (C(10) 0.900 vs 0.759 — the §2c head family). The community layer
is also a sharp SPEC DISCRIMINATOR (default vs identified stack differ hugely
on it) — usable ablation evidence.

**THE REAL FINDING (previously unscored; Tier-2 catch): the fitted model's
STATIONARY HEAD CROSS-SECTION IS TOO WIDE on FB.** Time-resolved probe
(scratchpad, 3 seeds, thirds of the panel; burn=40 so the recorded sim IS the
stationary law): the sim ladder sits +0.35..+0.46 log ABOVE the empirical
time-mean at every bin in ranks 1–600, converging to +0.04 by ranks 1000–2000,
STABLE across thirds (not accumulating drift — it is the stationary law);
**S(1) top-1 activity share: emp 0.015–0.020 stable; sim 0.032–0.090, hugely
seed-dependent** — the process intermittently grows a runaway #1. Mechanism is
arithmetic, not conjecture: stationary home spread = σ_perm/√(1−a²) ≈ 0.36 at
the head (κ=0.07, σ_perm=0.131), and a p99 temperament entity (s=0.887,
√v≈2.3) carries home SD ≈ 0.8 log — the data's head is far more tightly
bounded (emp top-2 gap 0.217). **This subsumes §2o**: the "sim stationary
top-2 gap 0.77±0.31 vs emp 0.217" was the rank-1-2 slice of this pattern, and
the initialization attribution was incomplete — the width persists in every
third; it is the stationary law of the fitted dynamics (the §2o conditional-sim
exoneration was horizon-limited: 21 test weeks, real state). WHY THE CARD IS
BLIND: VR/ACF/RACF are change-based, R2 is a scale-free correlation, Pers/coll
are identity-based — no committed row constrains the stationary cross-section;
the estimator fits Lagrangian change-moments and nothing ties the implied
stationary Eulerian law to the observed ladder. Platform scope: FB legacy
measured (this section); subs = level-only cosmetic (T=30, little relaxation);
**Era A and comments UNMEASURED (SSD unmounted) — measure on next mount before
any paper claim about the unconditional head.**

**Verdict on model changes (cost/benefit, per the audit's mandate): NO new
model component now.** (i) The movement gates (Tier 0) are conditional/cohort
short-horizon objects and do not see this; the in-sample card survives it by
construction; the paper's Fig-1 ladder claim is EMPIRICAL. (ii) The correct
fix, if confirmed on Era A/comments, is a CONSTRAINT, not a component: tie the
(κ, σ_perm) split (and/or the amplitude loading on the permanent component at
the head) to the empirical stationary band variance — an Eulerian
stationarity moment added to the MD vector. That REMOVES a degree of freedom
(parsimony-positive) but unfreezes every committed fit, so it is scheduled
BEHIND the registered confirmation extension (§2x #1), pre-registered as an
extension diagnostic alongside κ_i (E4-style): "stationary head-spread moment:
sim S(1) and top-600 ladder offset within empirical bands." (iii) Tooling is
in place now: `ladder_drift` + `S(1) top_share` added to community_metrics
(suite 63); the head_diversity_probe.py initialization experiment (era-median
w0 via dataclasses.replace, §2o's flagged fix) exists but is MOOT for this
finding — the width is stationary, not initialization.

## 2z-b. 2026-07-06 — T9 mounted: the head-law finding ADJUDICATED on the paper-primary panels (Era A CONFIRMED, comments NOT confirmed); mechanism attributed to the base partition; protocol amendment A2 registered; no model change adopted

WD Passport still NOT mounted — the §2x confirmation extension (E1–E3)
remains owner-gated; this session ran only on the T9 panels.

**Era A (structure-primary full stack, 10 seeds): the stationary head-law
overshoot is CONFIRMED at the paper spec, on the level-robust share metrics:**
S(1) emp 0.0170 vs sim 0.0467 ± 0.0129 (~2.7×, ≈2.3 seed-SDs); S(10) emp
0.1011 vs sim 0.151 (+50%); D_share 0.086. Everything else on Era A is
healthy — C(1) 0.521 vs 0.518 (exact), d(1) 0.245 vs 0.221, kernel TV
0.065–0.091 with the misfit concentrated in the known boundary family
(mid-band "out" mass 0.021/0.041 emp vs 0.003/0.009 sim) — the head-share
excess is an ISOLATED residual, not part of a broader failure.

**Comments (long stack, 10 seeds): NOT confirmed.** S(1) emp 0.1057 vs sim
0.1397 ± 0.0396 — directionally consistent, within ~1 seed-SD. New
quantifications on comments in the community coordinates: the sim top-K set
is 3× too OPEN in turnover (ō 0.031 vs emp 0.0096; flux 0.128 vs 0.090 —
the recorded boundary residual, sharpened), while the very-head ranks 2–13
are too STICKY (d(8) 0.102 vs 0.199; C(6) 0.679 vs 0.793).

**Mechanism (CRN-style probe, 3 estimations × 6 seeds, Era A): the base
(κ, σ_perm) head partition carries the bulk.** Setting the amplitude loading
b: measured 1.02 → 0 moves S(1) only 0.051 → 0.041 (emp 0.017); b=1 imposed:
0.053. The amplitude tail explains ~25–30% of the excess; the rest is the
partition itself — Lagrangian change-moments admit a (κ, σ_perm) split whose
implied stationary cross-section is too wide at the head. Nothing in the
moment vector constrains it.

**Measurement lesson (encoded in community_metrics docstrings): D_ladder is
level-sensitive.** Comments' D_ladder = 1.23 decomposes as the un-modeled
secular census growth (sim uniformly −0.7..−1.6 BELOW the time-mean ladder —
opposite sign to FB); the model removes the platform level by design, so raw
log-ladder RMSE conflates level path with shape on non-stationary-level
panels. Share-based statistics (S(k), D_share, S(1)/S(10)) are the
level-robust primaries — and they are what confirms FB (whose declining
level would bias the sim ladder DOWN, understating the overshoot).

**Actions taken (this session):**
1. CONFIRMATION_PROTOCOL **Amendment A2 registered** (working tree; valid
   only if committed before the WD resume runs): E5 stationary head-law
   diagnostic on the extension — registered baselines above; pre-declared
   reading (cross-platform structural iff extension sim S(1) > emp by 2
   seed-SDs); the candidate fix named and scoped (Eulerian stationarity
   moment appended to the MD objective, OPT-IN like --md-vr, no new
   components, adopted only if cards hold and frozen gates don't degrade).
2. NO model change adopted now — per the pre-declared §2z-a rule the
   cross-platform trigger did NOT fire (comments inconclusive), the movement
   gates and card are structurally unaffected, and §2x item 1 (confirmation,
   "no model work first") stands.
3. Paper language (binding addition to the §2x claim set): the ladder-
   reproduction claim is scoped — "reproduces the stationary ladder in shape
   through the mid-ranks; the model's stationary head concentration
   overshoots on FB (S(1) ~2.7×, S(10) +50%), a measured residual of the
   unconditional stationary law" — and the boundary paragraph gains the
   turnover coordinate (comments ō 3×).

**Sharpest open item carried forward:** if E5 confirms on the extension, the
stationarity-moment constraint is the next estimator step (parsimony-
positive: removes partition freedom); if not, FB head concentration is
reported as a measurement-regime-scoped limitation. Either way the paper's
evidence spine is untouched.

## 2z-c. 2026-07-06 — Instagram RESCUED as a fourth system: the "a"-query censoring modeled as a 3-layer measurement process; the standard pipeline on a measurability-scoped universe gives 9/15 + an AT-PAR OOS gate with zero calibration freedom; per-post variant FAILS by the known φ→0 degeneracy (scored, kept)

**The question (owner-posed):** IG was collected through CrowdTangle's search
endpoint with the query "a" (posts included iff the caption matches;
account×day aggregation) — recorded here only as a negative control (§4:
"flattens the distribution → pathological rank displacement, R² collapse").
Knowing the censoring process, can added apparatus on top of the CORE model
recover structure and movement?  Everything below was pre-registered in
`llm_fitting/ig_censoring_prereg.md` (model + P1–P4 written before any
measurement; Amendment 1 — universe rule, K, stacks, expected failure
locus — dated after forensics, before any fit).

**The censoring model (formal, and now measured).** Y_it = Σ engagements over
M_it = `n_posts` matching posts, M_it ~ Binomial(P_it, q_i), q_i a persistent
account property (language/caption style). Three implications, all
independently measured on the FULL panel (`ig_weekly_ranked.parquet`,
24.5M rows, 2.31M accounts, T=53 w/ partial week 0 dropped → weeks 1–52):

- **L1 — post-level thinning noise ~ a/M (P1 PASS).** Per-post decomposition
  log Y = log M + log Ȳ: (Δlog Ȳ)² regresses on (1/M_t + 1/M_{t+1}) with
  slope c² = 0.253, binned means monotone 0.29 → 0.94; at M ≥ 30 both weeks
  the implied thinning share of per-post movement is ~5% (0.011 of 0.192),
  at M ≥ 100 ~2%. High-M Δlog Ȳ has excess kurtosis 11.0 — the t-tails
  family, visible once the thinning is stripped.
- **L2 — week-correlated INSTRUMENT dropout (P2 FAIL as registered; the
  registered Poisson-thinning story for absence is DEAD).** Accounts with
  m̄ ≥ 20 matching posts/week are absent 16.1% of weeks (thinning predicts
  e^-28 ≈ 0); the hazard is flat in posting rate (10–17% from m̄=10 to 140) —
  an instrument coin-flip, not behavior; cohort absence rate varies across
  weeks with sd 0.047 vs iid benchmark 0.0042 (11×) — week-level common
  shocks; collection RAMP era weeks 1–8 (absence 0.21–0.26, settling to
  ~0.10–0.12). Failure reported with full prominence: absence on IG is
  mostly instrument, not thinning zeros.
- **L3 — low-q ghost flicker at the head.** Top-5k accounts by level have
  MEDIAN presence 7.5% — mega-accounts whose captions rarely match appear
  once with huge engagement and vanish. This is precisely the ghost-spiker
  population the absence-penalized permanent rank exists to exclude, and it
  is what made naive IG panels pathological.
- **P3 (aggregation scaling) PASS:** cohort mean (Δlog Y)² = 0.957 / 0.595 /
  0.444 at 1w / 2w / 4w; the implied thinning constant is consistent across
  both steps (a_eff 3.2 vs 3.45) — a 1/M noise law, which true diffusion
  (growing in horizon) cannot mimic.

**The apparatus (zero new model components; the CORE model is untouched):**
(1) universe = absence-penalized permanent rank over ALL 2.31M accounts
(standard §2b rule — it sinks the L3 ghosts automatically: the selected 60k
have mean presence 0.889); (2) **K = 10,000 declared at the 80% coverage
point** (K90 ≈ 25k, but the marginal tail is M ~ 1–2 ≈ pure thinning noise —
a measurability cap, n_posts-based, score-blind, declared in Amendment 1
BEFORE any fit), B = 4K; (3) weeks 1–52 (partial week 0 dropped);
(4) standard stacks: card = temper + pool≥8 + md6 + t-tails + stat-factor;
gate = temper + pool≥8 + md6 + t-tails + `--conditional state`. New
platform entries `instagram_hm` (totals) and `instagram_pp` (per-post,
metric_value/n_posts — the q_i bias cancels exactly in Ȳ) — additive
PLATFORMS dict entries only; defaults byte-identical; suite 64 green.

**Results (P4 PASS on all four registered sub-criteria).**

| panel / spec | card | churn err | OOS rel err vs persistence | cov | scale |
|---|---|---|---|---|---|
| instagram_hm, card stack (20 reps) | **9/15** | 0.052 | — | — | — |
| instagram_hm, movement stack (5 splits) | — | — | **0.841 ± 0.536 vs 0.908 ± 0.637** (beats 2/5, AT PAR) _[SUPERSEDED 2026-07-11 by §2z-f: the 60k pre-cut leaked future membership; the citable gate is the EXACT-membership one — 0.317 ± 0.123 vs 0.593 ± 0.309, cov 40%, scale 1.0×5. §2z-e(5)'s intermediate 0.671 was NOT leak-free and is also superseded.]_ | 80% | **1.00×5** |
| instagram_pp, card stack (5 reps) | 7/15 | 0.032 | — | — | — |

- **No pathology on totals.** dRank1/4/13 sim 17.7/21.3/27.7 vs emp 19/24/30
  (the recorded "runaway displacement" is GONE); R2_1 0.789 vs 0.635; VR
  shape right with a uniform ~+0.06 excess. Held-out displacement
  distribution (T0=39): dR1 median/p90 model 13/88 vs emp 16/82.
- **Parameter transport (the new-system criteria of the skill §3):**
  κ(z) = 0.100 → 0.005 head→tail declining ✓; t_df = 4.4 ✓; s = 0.786 —
  between comments (0.69) and subs (0.94), metric-dependent as recorded ✓;
  φ = 0.25..0.65 healthy.
- **σ_obs externally validated by the censoring law (the Spec-B analogue).**
  The calibrated fit chose σ_obs = 0.209 (head) .. 0.732 (deep tail). The
  thinning envelope computed from n_posts alone — [√(c²·E[1/M]),
  √((1+c²)·E[1/M])] — is [0.134..0.298] at perm-rank 1–100 (mean M = 34) and
  [0.291..0.647] at 3k–10k (M = 6.7), extending to ~1.0 in the M≈1–2 buffer.
  The estimator, blind to n_posts, landed inside the envelope at both ends.
  Language: on IG, σ_obs is *bounded by the censoring process* and the
  calibrated values respect the bound — still not "identified in level"
  (that needs the IG dailies on the WD drive → true Spec-B).
- **The card misses are the pre-declared instrument locus, plus the known
  head family:** return4K −0.148 (instrument dropouts RETURN; the sim's
  exits don't — L2 exactly as pre-declared), outfluxK +0.082, RACF1 +0.256,
  RACF4 +0.172, R2_1 +0.154, coll1 +0.225 (5-rep MC ±0.15 caveat; persists
  at 20 reps), Pers13 +8.85. Sim slightly over-persistent overall — consistent
  with L2 dropout entering the empirical panel as extra apparent churn that
  the model (correctly) does not reproduce.
- **Gate reading:** AT PAR (0.841 vs 0.908, 80% coverage) with calibration
  selecting scale 1.00 on every split — zero calibration freedom used, as on
  FB Spec-B. Rel-err LEVELS are ~7× FB/subs (0.84 vs 0.118): the thinning
  noise floor hurts model and persistence alike; the split spread is large
  (0.25–1.65). By the registered new-system success criteria (OOS
  at-par-or-better + transported parameters + clean card), IG is a
  CONFIRMING fourth system — with the estimand scoped below.
- **Per-post variant (apparatus B): FAIL, kept with full prominence.** 7/15
  with the OLD pathology (sim dRank1 1620 vs emp 31; R2_13 0.016 vs 0.635)
  and the fit shows why: φ = 0.000 everywhere, σ_trans ≡ σ_obs — the KNOWN
  weak-identification degeneracy (skill §4 step-4 signature). Removing the
  M-dynamics removed the persistent signal that splits transitory from
  noise. The registered cure is an EXTERNAL σ_obs pin, and the censoring
  law supplies one (R_it = c²/M_it, c² = 0.253 measured) — that is
  apparatus D, opt-in estimator code, NOT built this session (measure-first
  discipline: build it only if the per-post estimand is ever needed).

**Estimand scope (binding claim language for any use of this result):** the
rescued system is "weekly engagement on a-matching posts, for the population
reliably measured through the a-filter" — all coverage language is "of
tracked (a-matching) activity" (censored-sample class, like FB CrowdTangle,
NOT census). The persistent inclusion propensity q_i is NOT identifiable
within-panel; the observed ladder is the true ladder convolved with the
cross-sectional law of log q — goal-1 shape claims about the TRUE IG ladder
are out of scope by construction (movement claims are not: log q_i cancels
in every within-account change).

**Adoption verdict:** no defaults changed; no core code touched (two additive
PLATFORMS entries + standalone forensics/build scripts); legacy guard
untouched, suite 64 green. IG's §4 status is UPGRADED from "negative control
only" to "recovered under the declared censoring model with scoped estimand;
UNCORRECTED IG panels remain a negative control and are still never to be
calibrated against." The pitfall-catalogue wording in CLAUDE.md /
.claude/skills (IG = negative control ONLY) is a method-contract line —
proposed amendment is OWNER-GATED; not edited this session.

**Follow-ups, in order of value:** (1) WD drive mount → IG account×day data →
true Spec-B daily floor on IG (turns "bounded" into "identified"), and a
registered mini-protocol if IG is promoted into the paper's breadth set;
(2) apparatus D (opt-in `--obs-pin-nposts`: heteroskedastic R_it = a/M_it +
absence-as-missing in the Kalman update) — the principled L1+L2 treatment,
expected to help the per-post estimand and the boundary/return rows;
(3) era-sensitivity re-run on stable weeks 9–52 (ramp era 1–8 declared but
not yet excluded from the primary).

Reproduction:
```
python -u llm_fitting/ig_censoring_forensics.py            # P1/P2 on the top-50k cut
python -u llm_fitting/ig_censoring_forensics2.py           # P1 refined (per-post decomposition)
python -u llm_fitting/ig_censoring_forensics3.py           # full-panel instrument health + P2 re-test
python -u llm_fitting/ig_censoring_forensics4.py           # cohort dropout correlation, K concentration, P3
python -u llm_fitting/ig_build_hm_panels.py                # builds ig_hm_totals / ig_hm_perpost parquets
python -u llm_fitting/ig_sigma_obs_check.py                # sigma_obs vs thinning envelope
python llm_fitting/minimal_rankdiff.py instagram_hm --top-k 10000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --stat-factor --reps 20
python llm_fitting/rankdiff_kalman.py instagram_hm --oos --top-k 10000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state
```

## 2z-d. 2026-07-06 — IG rescue OPERATIONALIZED (registered option, scope lines, regression lock, log archive) + measured through the audited metric hierarchy: Tier-2 localizes IG's residuals into the two KNOWN families; the stationary head-law overshoot REPRODUCES (~2.9×) on a third instrument

**The question (owner-posed):** how should §2z-c be operationalized without
over-optimizing for a special case, and how does the rescued panel read in
the metric hierarchy audited and extended this same day (§2z/§2z-a)?

**Ruling on scope (the anti-over-optimization line):** what gets encoded
operationally is the METHOD — "known-mechanism censoring is modelable; do
the censoring forensics before declaring a system unusable" — with IG kept
as the worked example. NO new pipeline flags, components, or IG-specific
code paths; apparatus D (heteroskedastic `--obs-pin-nposts`) stays parked;
`instagram_hm` is a **breadth demonstration, NOT paper-primary** — promotion
into the paper's breadth set is owner-gated behind a registered mini-protocol
(and ideally the WD dailies → true Spec-B).

**Tier-2 measurement (community layer, card stack, 10 reps; + `--dist-scores`
on the gate). The headline: the §2z-a/2z-b stationary head-law overshoot
reproduces on IG** — S(1) emp 0.0352 vs sim 0.1005 ± 0.0209 (~2.9×,
≈3.1 seed-SDs), d(1) emp 0.077 vs sim 0.248, C(1) emp 0.392 vs sim 0.598.
Same direction and nearly the same ratio as FB Era A (2.7×), now on a THIRD
platform/instrument with an entirely different censoring regime. Per the
A2 pre-declared reading this SUPPORTS the Eulerian-stationarity-moment
candidate fix but does NOT trigger E5 (registered on the comments extension
only); recorded here as supporting evidence, hierarchy discipline intact
(no new pass/fail row).

Rest of the Tier-2 read, localized into known families:
- **L2-instrument/boundary family:** turnover ō emp 0.0414 vs sim 0.1950
  (sim ~4.7× too open — same direction as comments' 3×, §2z-b), flux F
  0.336 vs 0.418; deep-band kernel out-mass 0.450 vs 0.364. On IG the
  empirical side is additionally suppressed by the 10–14% week-correlated
  instrument dropout (§2z-c L2), so these rows are scoped, not promoted.
- **Over-persistence family (L2-consistent):** rolling Pers1 emp 68.1 ± 3.2
  vs sim 79.1 ± 1.1; mid-band kernel diagonal 51-200 sim 0.764 vs emp 0.467.
  Kernel TV h=1 = 0.0891 ± 0.0011 — inside the FB Era A range (0.065–0.091).
  Rolling R2_13 emp 0.530 vs sim 0.388 (sim UNDER at long horizon; the
  period-0 card row shows the opposite sign — the §2z anchoring bias,
  measured again).
- **Ladder statistics behave exactly as the §2z-b measurement lesson says:**
  D_ladder 1.19 / D_share 0.348 are dominated by level-path and (on IG) the
  q-convolution of the observed ladder — the share statistics S(k) are the
  level-robust primaries, and they carry the head-law signal above.
- **Proper scores (the audited gate add-on):** CRPS skill vs persistence
  −0.004 ± 0.002 (dRank1) and −0.008 ± 0.005 (dRank4) — at par; PIT
  predictive coverage 0.09–0.16 / 0.45–0.52 / 0.86–0.91 vs nominal
  .10/.50/.90 — near-nominal calibration on a panel with a ~0.6-log-unit
  weekly noise floor; W1(model) > W1(persistence) on 4/5 splits (the
  declared reference-scale caveat: a point-mass forecast is W1-favored when
  the predictive spread is honest). Hierarchy reading: Tier-0 at par,
  Tier-1 9/15 descriptive, Tier-2 adds referee-native views and localizes —
  the audited architecture handles the special case without modification.

**Operationalization shipped (this session):**
1. **Registered option:** `COVERAGE_K["instagram_hm"] = {80: 10000}` — the
   run is now `--coverage 80` resolvable like every other platform; only the
   80 level registered (K90 ≈ 25k is n_posts ~1–2 pure-thinning tail;
   measurability cap, declared in the code comment). Platform entries
   `instagram_hm`/`instagram_pp` from §2z-c stand.
2. **Regression lock:** `tests/test_ig_hm_panel.py` — ghost-spiker exclusion
   (the L3 property that makes the rescue work) + present-week value
   preservation in `ig_build_hm_panels.build()`. Suite 66 green.
3. **Scope lines updated** (owner-authorized this session): CLAUDE.md quick
   rule, AGENTS.md digest, stochastic-modeling skill (§3 breadth row —
   labeled NOT paper-primary; §4.2 known-censoring forensics step —
   generalizable; §6 pitfall rewrite incl. the per-week-cut and
   iCloud-eviction gotchas; §7 agenda + open-items), data-intake skill
   (known-mechanism censoring = third instrument class). Mirrors synced
   (`.agents/skills`), sync test green. "Never calibrate to IG" is
   UNCHANGED and restated in every location.
4. **Log archive:** `llm_fitting/runs/2026-07-06_ig_rescue/` — verbatim
   outputs behind every §2z-c/§2z-d number (forensics regenerated from the
   committed deterministic scripts; model-run logs are session originals).

**Verdict:** defaults byte-identical; no core code touched; suite 66 green;
legacy guard untouched. The special case is banked as breadth evidence and
as a reusable intake procedure, not as machinery.

Reproduction (Tier-2 layer of this section):
```
python llm_fitting/community_metrics.py instagram_hm --top-k 10000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --stat-factor --reps 10 --print-trans
python llm_fitting/rankdiff_kalman.py instagram_hm --oos --top-k 10000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state --dist-scores
python -m pytest tests/test_ig_hm_panel.py -q
```

## 2z-e. 2026-07-11 — External review round 2 implemented: NNLS estimator audited (defaults unchanged; head σ_obs is a solver-convention artifact — Spec-B pinning confirmed as the resolution), conditional anchor adjudicated (filtered-state anchor KEPT), entity-home language made binding, S(k) labels fixed (A3), IG re-run LEAK-FREE (at-par verdict transports)

Implements the 2026-07-11 external review (four findings, all verified
against code before action). Runs archived with manifests in
`llm_fitting/runs/2026-07-11_nnls_audit/` (PREREG.md written before any
--nnls run). Suite 73 tests green (66 + 5 NNLS + 2 cond-home); legacy guard
14/15 / churn 0.013 EXACT; package suite 8 green.

**(1) NNLS audit (review finding 2 — the MD solves were clipped OLS, not
NNLS; the clipped SSE also drove the (a, φ) grid choice).** `--nnls` added
(exact Lawson–Hanson via `_solve_nonneg`; opt-in on both CLIs; defaults
byte-identical; 5 unit tests incl. an NNLS-strictly-better construction).
Controlled §2s-matrix rerun, legacy vs NNLS under identical code/seeds
(every legacy arm reproduced its recorded number exactly):

| target | legacy (reproduced) | NNLS | Δ verdict |
|---|---|---|---|
| FB Era A card (full stack) | **15/15, churn 0.018** | 14/15, churn 0.029 (sole flip: Pers4 +5.2 vs tol 5 — the §2k knife-edge, period-0-anchored row) | within P3 band |
| subs card (2d/2e) | 14/15, 0.074 | 14/15, 0.052 | unchanged |
| comments card (LONG) | 12/15, 0.037 | 12/15, 0.044 | unchanged |
| FB gate spec-B + cond (paper-primary) | **0.118 ± 0.038, cov 60%** | **0.123 ± 0.033, cov 60%**, scale 1.0 on 4/5 | **robust (P2 PASS)** |
| subs gate cond | 0.118 ± 0.061, cov 100% | 0.164 ± 0.053, cov 80% (+0.75 SD; at par w/ 0.168) | within P4 band; **edge softens** |
| comments gate cond | 0.167 ± 0.068, cov 100% (current-code baseline; §2l era 0.159 predates MOM_FLOOR) | 0.171 ± 0.046, cov 60% | unchanged (at par both) |

Pre-registered predictions: P2/P3/P4 **PASS**; P1 **PARTIAL** — interior
knots moved as predicted BUT the FB and comments head σ_obs endpoints
collapsed to exactly 0.000 (legacy 0.058/0.054), with FB head φ 0.30→0.20.
**The sharpened conclusion (the audit's real yield): the clip was an
ACCIDENTAL REGULARIZER keeping the unpinned head off the φ→0/σ_obs=0
degenerate corner; under the exact solve the head degeneracy is exact.
Raw-MD head noise values are solver-convention artifacts under EITHER
convention — only the Spec-B-pinned head is meaningful, which is the
paper-primary movement spec already (§2s), and that gate is
convention-robust (0.118→0.123).** Honest scoping: the subs conditional
"beats 4/5" is convention-sensitive (NNLS: at par, 0.164 vs 0.168);
report the committed number with the NNLS sensitivity in SI.
**Adoption (pre-declared rule): defaults stay legacy (bit-reproducibility
of every committed result); NNLS = SI robustness result; switching the
documented convention is an OWNER decision required BEFORE extension
processing** (the protocol freeze pins the estimator; if the convention
switches, re-freeze + re-record under NNLS).

**(2) Conditional anchor (review finding 4 — the conditional sim used the
filtered end-of-train state as BOTH state and OU home; "conditioning =
real initial state" was imprecise).** `--cond-home {state,trainmean}`
added (trainmean = entity's train-window mean level as a separate long-run
home; state path byte-identical, 2 unit tests). Both platforms, legacy
solver, state baselines reproduced exactly:

| gate | state (committed) | trainmean |
|---|---|---|
| FB spec-B + cond | **0.118 ± 0.038, cov 60%** | 0.137 ± 0.034, cov 60% |
| subs cond | **0.118 ± 0.061, cov 100%** | 0.153 ± 0.077, cov 80% |

**Verdict (pre-declared "adopt only if ≥ as good"): NOT adopted — the
committed forecaster stays, renamed honestly as FILTERED-STATE-ANCHORED
(binding glossary, §1).** The result is informative in itself: the filtered
state beats the train-mean as a home anchor on both platforms — the local
anchor carries real information, consistent with slowly-drifting homes /
excess low-frequency structure (§2v).

**(3) Entity-home model statement (review finding 1).** Verified: `T_curve`
is estimated and stored but read by NO simulator; both simulators revert
each entity to its own home (seeded from `w0`). Adjudication: own the
assumption, don't re-architect — §1 rewritten (2026-07-11) with the
EXPLICIT CONDITIONING ASSUMPTION (measured stationary ladder = the one
taken-as-given input; goal-1 = conditional reproduction + maintenance;
genesis out of scope), the movement-gate glossary (pooled-moment gate;
historical-mobility baseline; survivor-conditional displacement;
coverage ≠ predictive coverage), and the parsimony phrasing ("parsimonious
latent architecture with flexible nonparametric rank profiles"). Source
header + RankParams comments fixed (T_curve marked DIAGNOSTIC ONLY);
skill + mirror synced; paper outline M&M/R2 blocks updated. Emergent
stationarity REJECTED as a remedy: it would trade one defensible,
evidence-backed assumption (persistent entity homes at the measured
ladder — the program's own temperament/permanent-rank measurements) for
real dynamical complexity, to derive something Gabaix already explains.

**(4) S(k) denominator labels (review, IG section).** All S(k)/D_share
outputs relabeled "share within recorded top-M" (M stated;
community_metrics docstrings + prints); CONFIRMATION_PROTOCOL **Amendment
A3** registers the clarification for E5 pre-data (baselines valid — same
denominator both sides; label-only). Cross-platform absolute S(k)
comparisons must hold M fixed.

**(5) IG train-safe universe (review: the 60k pre-cut ranked by FULL-window
permanent rank — future-membership leakage; omitted share of the
train-selected 40k universe 15.3% @T0=13 .. 1.1% @T0=39).** Fix: UNION
pre-cut (`ig_hm_totals_ts.parquet` = full-window top-60k ∪ train-only
top-40k at every gate origin, computed on the full 2.31M-account panel;
67,524 accounts) — a superset of every train-only universe BY CONSTRUCTION,
**verified 0.0000% omitted at all five origins** (`ig_trainsafe_check.py`;
a keep=200k depth cut was tried first and REJECTED: 2.56% omitted at
T0=13). Declared scope: this removes the future-information EXCLUSION; the
pre-cut-internal ranking approximation of Amendment 1 is unchanged. New
platform entry `instagram_hm_ts` (additive). Gate on the leak-free
universe (registered movement spec): **0.671 ± 0.417 vs persistence
0.652 ± 0.436, cov 60%, scale 1.0 ×5, CRPS skill ≈ 0** — absolute numbers
differ from §2z-d (different universe ⇒ different cohort), but the VERDICT
transports: **at par with the historical-mobility baseline with zero
calibration freedom, now leak-free**. IG stays breadth-supporting,
non-primary; §2z-d's gate numbers are SUPERSEDED for citation by these
(leak-free) ones.
_[SUPERSEDED 2026-07-11 same-day by §2z-f: the equivalence step was INVALID
— candidate inclusion ≠ selection equivalence (restrict_universe re-ranks
within the pre-cut; measured overlap with full-population train-only
membership only 79–83%). The 0.671 gate is NOT leak-free and must not be
cited; the exact fixed-membership gate in §2z-f replaces it.]_

Reproduction:
```
python llm_fitting/minimal_rankdiff.py facebook_a --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long --stat-factor \
    --two-scale --mix-hetero --nnls          # NNLS arm of any card
python llm_fitting/rankdiff_kalman.py facebook_a --oos --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --spec-b --conditional state \
    --dist-scores --nnls                     # NNLS arm of the paper-primary gate
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state \
    --cond-home trainmean --dist-scores      # anchor experiment
python llm_fitting/ig_build_hm_panels.py --keep 200000 --suffix _k200 --variant totals
python llm_fitting/ig_trainsafe_check.py llm_fitting/ig_hm_totals_ts.parquet
python llm_fitting/rankdiff_kalman.py instagram_hm_ts --oos --top-k 10000 \
    --temperament --min-knot-entities 8 --md-lags 6 --t-tails \
    --conditional state --dist-scores        # [DO NOT USE -- §2z-f: NOT leak-free
                                             #  without --member-ids-file; see 2z-f
                                             #  reproduction block for the valid command]
# full logs: llm_fitting/runs/2026-07-11_nnls_audit/
```

## 2z-f. 2026-07-11 — Review round 3: the §2z-e(5) "leak-free" claim was WRONG and is corrected — exact per-origin fixed membership implemented; the citable IG gate is 0.317 ± 0.123 vs 0.593 ± 0.309 (above baseline 4/5, at par on proper scores); NNLS convention decision framed for the owner (WD now mounted — decide BEFORE E1–E5)

**The error (reviewer's finding, verified and reproduced).** §2z-e(5)
claimed the union pre-cut made the gate leak-free "by construction" because
it contained every full-population train-only candidate. That step was
INVALID: candidate inclusion ≠ selection equivalence. `restrict_universe`
re-ranks within the loaded panel, so absence floors (N_t+1 of 67.5k vs
~1M+) and weekly compressions differ, and membership selection INSIDE the
pre-cut diverges from full-population train-only selection. Measured
overlap (reviewer: 78.6–82.7%; independently reproduced this session with
the exact rule: **81.8 / 80.1 / 79.0 / 78.5 / 78.6% at T0=13..39**) — so
~18–21% of each modeled universe differed and the union's full-window
component let future information shape the re-ranked selection. The 0.671
gate is WITHDRAWN (inline notes at §2z-d and §2z-e(5)).

**The exact fix (the reviewer's recommendation, implemented).**
- `restrict_universe(member_ids=...)`: FIXED membership — selection skipped
  entirely, weekly re-ranking only (4 unit tests; suite 77).
- `ig_trainsafe_members.py`: computes each origin's full-population
  train-only top-40k under the program's EXACT rule (replicates
  `restrict_universe`'s selection block verbatim: metric-desc/entity-id
  weekly tiebreak, absence floor N_t+1, mergesort id tiebreak) and writes
  `ig_trainsafe_members.parquet` (T0 × 40k ids). It also SELF-HEALS the
  union data panel: the round-2 union was built with the checker's
  tie-breaking, so 5 tie-boundary ids were missing; appended, panel now
  67,529 accounts. The round-2 containment check is re-scoped to what it
  actually proves (data availability precondition), with the invalid step
  documented in its docstring.
- `rankdiff_kalman --member-ids-file`: consumes the fixed ids per origin.

**The citable IG gate (exact train-only membership, registered movement
spec, `runs/2026-07-11_nnls_audit/gate_ig_exact_members.log`):**

| | rel err | persistence (historical-mobility) | cov | scale | CRPS skill |
|---|---|---|---|---|---|
| exact membership | **0.317 ± 0.123** | 0.593 ± 0.309 | 40% | **1.00 ×5** | ≈ 0 (−0.00) |

Above the baseline on 4/5 splits (0.354<0.407, 0.282<0.598, 0.152<0.785,
0.525<1.038; loses T0=13) with zero calibration freedom; PIT coverage
0.14–0.16 / 0.46–0.53 / 0.85–0.88 vs nominal .10/.50/.90. The baseline
itself degrades sharply at later origins on the true train-only universe
(0.785, 1.038) — its train-window movement does not transport there, while
the model's does. CLAIM DISCIPLINE: IG remains breadth-supporting,
non-primary; the language is "at-or-above the historical-mobility baseline
on the pooled moments, at par on proper scores" — NOT "beats" as a
headline (CRPS ≈ 0, coverage 40%, and IG gate numbers have proven
universe-sensitive: 0.841 → 0.671 → 0.317 across the three universes).

**NNLS adoption (reviewer round 3, seconded): framed for the OWNER, with
the reviewer's recommendation on the table.** Their position: exact NNLS is
not a "convention" but the correct optimizer for the stated constrained MD
objective; re-freeze the scientific estimator under NNLS, keep clipped OLS
solely as the legacy reproduction/sensitivity arm, and treat the NNLS
results as current truth (FB spec-B cond 0.123 ± 0.033 strong; comments
0.171 at par; subs 0.164 at par — NOT robustly better; FB card 14/15 with
the Pers4 knife-edge). Nuance adopted into claim language NOW: under NNLS
the FB calibrated scale is 1.0 on **4/5** splits (0.50 at T0=65) — the
"zero calibration freedom on 5/5" sentence is legacy-convention-specific;
say "no calibration freedom used on 4–5 of 5 splits across solver
conventions". Re-freezing is an owner decision (defaults, frozen specs,
protocol) — REQUIRED, either way, before extension processing.

**Sequencing note: the WD drive is now MOUNTED (per the reviewer's session).
No extension data has been read by this session. Order: (1) owner decides
the NNLS convention (re-freeze + re-record, or legacy-primary + NNLS SI);
(2) any protocol amendment is committed; (3) E1–E5 runs.**

Reproduction:
```
python llm_fitting/ig_trainsafe_members.py     # exact per-origin ids + panel self-heal
python llm_fitting/rankdiff_kalman.py instagram_hm_ts --oos --top-k 10000 \
    --temperament --min-knot-entities 8 --md-lags 6 --t-tails \
    --conditional state --dist-scores \
    --member-ids-file llm_fitting/ig_trainsafe_members.parquet
```

## 2z-g. 2026-07-11 — OWNER ADOPTION OF OPTION A: the scientific estimator is RE-FROZEN under exact NNLS (protocol Amendment A4); the NNLS-primary record replaces the §2s numbers; clipped OLS retained as `--legacy-clip` reproduction arm

**The decision (owner, this session, after the §2z-e audit and the round-3
review):** exact NNLS is the correct optimizer for the stated constrained MD
objective; it becomes the scientific primary. Rationale, costs, and the
rejected alternative (legacy-primary + SI note) are in the decision memo
delivered to the owner; the deciding considerations were estimator/prose
consistency, doing the re-freeze BEFORE the extension (one confirmation run,
under the defensible estimator), and the fact that every solver-sensitive
claim was already flagged as fragile (Pers4 knife-edge, subs conditional
edge, unpinned head noise).

**Implementation:** `nnls=True` is now the DEFAULT throughout the MD path
(`_solve_nonneg`, `_md_partition`, `_md_partition2`, `estimate`,
`run_platform`, `_estimate_fast`, `oos_movement`); `--legacy-clip` on both
CLIs reproduces every pre-re-freeze recorded result; `--nnls` kept as a
compat no-op so §2z-e commands still run. The v4.3 legacy guard never
touches the MD path and is UNCHANGED (14/15, churn 0.013). Suite 77 green
(recovery tests pass under the exact solve); package suite 8 green.
Protocol **Amendment A4** registered (before any extension row read; WD
mounted but untouched): the frozen E1–E5 estimator = exact NNLS; E1 bands
unchanged, with a declared both-solves readout rule if any transported
parameter sits within 10% of a band edge.

**THE NNLS-PRIMARY RECORD (supersedes the §2s table for citation; legacy
values in parentheses are the `--legacy-clip` reproduction arm, cite only
as SI sensitivity):**

| panel | structure card | movement gate |
|---|---|---|
| FB Era A | **14/15, churn 0.029** (legacy 15/15/0.018; sole miss = the Pers4 period-0 knife-edge, §2k/§2t) | **Spec-B + cond state: 0.123 ± 0.033 vs 0.145 ± 0.031, cov 60%, beats the historical-mobility baseline 4/5, scale 1.0 on 4/5** (legacy 0.118 ± 0.038, scale 1.0×5). Calibration language: "no calibration freedom used on 4–5 of 5 splits across solver conventions" |
| Reddit comments | **12/15, churn 0.044** (legacy 12/15/0.037) | **0.171 ± 0.046 vs 0.165 ± 0.062, cov 60%, at par** (legacy 0.167 ± 0.068, cov 100%) |
| Reddit subs | **14/15, churn 0.052** (legacy 14/15/0.074) | **0.164 ± 0.053 vs 0.168 ± 0.004, cov 80%, AT PAR** (legacy 0.118 ± 0.061 "beats 4/5" = solver-convention-sensitive; SI only, never main text) |
| IG rescue (exact membership, §2z-f) | **8/15, churn 0.062** (20 reps; legacy 9/15/0.052) | **0.320 ± 0.189 vs 0.593 ± 0.309, above baseline 4/5, cov 60%, CRPS ≈ 0** (legacy 0.317 ± 0.123 — convention-robust; scale 1.0 on 3/5, interior on 2) |

Logs: `runs/2026-07-11_nnls_audit/` (card_ig_nnls, gate_ig_exact_members_nnls
added this section; all other cells from the §2z-e audit).

**What the re-freeze changes and does not change:**
- CHANGED: all main-text numbers now come from this table; the paper
  outline's R5 movement block updated accordingly; skill §3 table re-frozen.
- UNCHANGED: every §2s stack definition; the gate protocol and criteria;
  Spec-B centered pin; MOM_FLOOR; the b=1 law, temperament, κ(z), Spec-B
  identification results (all solver-independent); the legacy guard.
- LANGUAGE: subs movement claim is now "at par with the historical-mobility
  baseline" (its former edge was convention-dependent); FB scale claim is
  "1.0 on 4–5 of 5 across conventions"; IG stays "at-or-above on pooled
  moments, at par on proper scores".
- To re-record AT PAPER BUILD (declared, not blocking): §2t/§2y
  bands + per-block Q under NNLS (descriptive layers; the audit's card
  deltas bound the expected movement at ≤ one knife-edge row).

**Next step is E1–E5 exactly as registered** (A1–A4), on the mounted WD
data — no further model or estimator work first (§2x item 1 stands).
_[AMENDED same day by §2z-h: round-4 review found E2's registered
membership rule leaked the extension window; Amendment A5 (E2 train-only
frozen membership) was required and registered BEFORE starting. E1–E5 now
runs under A1–A5.]_

Reproduction:
```
# NNLS is the default -- the §2s stack commands now produce the primary record
python llm_fitting/minimal_rankdiff.py facebook_a --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long --stat-factor \
    --two-scale --mix-hetero                      # 14/15, churn 0.029
# pre-re-freeze reproduction arm:
python llm_fitting/minimal_rankdiff.py facebook_a ... --legacy-clip   # 15/15, churn 0.018
```

## 2z-h. 2026-07-11 — Round-4 review adjudicated: Option A verified externally; ONE blocking protocol defect found and fixed (E2 membership leaked the extension window — Amendment A5, train-only frozen membership); default-lock tests + manifest + CLI exclusivity added. E1–E5 now genuinely ready under A1–A5

**External verification (round 4, no files changed by the reviewer):**
Option A implementation CORRECT — headline numbers match archived logs;
exact IG membership structurally valid (200,000 rows, 40,000/origin, no
duplicates, fully contained); suites 77 + 8 green; live legacy guard
14/15 / 0.013; no extension data read by anyone.

**The blocking defect (accepted, fixed BEFORE any extension row read):
E2's registered universe leaked the confirmation period.** Protocol §2
computed membership "over the FULL extended window" while E2 estimates
through 2021-06 and forecasts the first 34 extension weeks — extension
activity would have selected the 50,000 endpoints entering the held-out
forecast. Same error class as the IG pre-cut (§2z-f); pre-registration
does not make a selection out-of-sample. **Amendment A5 registered:**
E2 membership = absence-penalized permanent rank on the T=136 panel ONLY;
the 50,000 ids FROZEN (SHA-256 recorded before scoring) and carried into
the extension forecast via the §2z-f fixed-membership machinery
(`--member-ids-file`); E1/E3/E5 keep full-window membership (descriptive/
transport, as designed); E4 explicitly declared shared-survivor-
conditioned; zero threshold/stack/criterion changes.

**Other round-4 corrections applied:**
1. *NNLS default regression-locked properly:* the prior lock compared
   default-vs-NNLS on a case where the solvers coincide (would not catch a
   reversion). Added: API-default inspection across all six entry points,
   a deterministic rng-reconstructed moment vector where legacy and NNLS
   provably diverge (default must equal NNLS there; reconstructed, not
   hard-coded — a 6-decimal hard-coding defused the discrimination once
   and was caught), and a CLI mutual-exclusivity test.
   `--nnls`/`--legacy-clip` are now an argparse mutually exclusive group
   on both CLIs (previously legacy silently won when both were passed).
   Suite 80 green.
2. *§2z-e's stale "leak-free" IG reproduction command* annotated DO NOT
   USE in place (attractive-nuisance removal; §2z-f's command is the
   valid one).
3. *Derived-artifact hashes recorded:*
   `runs/2026-07-11_nnls_audit/MANIFEST.sha256` pins ig_hm_totals_ts
   (5f37e3ab…), ig_trainsafe_members (130726eb…), and the two superseded
   panels — the mutable untracked parquets are now integrity-checkable
   against the archived results.

**Round-4 scientific assessment, on the record (it matches the program's
own claim set):** aggregate stationarity = reproduction/maintenance
conditional on the empirical endpoint-home ladder, not ladder genesis;
movement strongest on FB, at par on both Reddit panels, encouraging on IG
pooled moments with proper scores at par; parsimony defensible at the
component/mechanism level, not literally low-parameter; the FB stationary
head-law overshoot remains the meaningful open residual (E5).

**State: E1–E5 is now ready to execute once, under A1–A5, with no further
model or estimator work first.** Owner go required to read the first
extension row.
_[AMENDED same day by §2z-i: round-5 review — A5's evaluation was defined
but its EXECUTION PATH was not implementable (the gate auto-derived origins
and test length); now implemented, tested, and the E2 membership frozen.]_

## 2z-i. 2026-07-11 — Round-5 (implementation-readiness): the A5/E2 execution path built and dry-tested; E2 membership FROZEN (sha pinned); registered E2 command recorded. Extension now genuinely one command away

**The gap (round-5 review, accepted):** A5 defined E2 fully, but
`oos_movement` auto-derived origins and test length from the panel — the
registered single-block design (one origin at T0=136, exactly 34 held-out
weeks, frozen membership) had no execution path, and no membership builder
existed.

**Implemented (all additive; defaults byte-identical, locked by test):**
1. `_gate_windows(T, n_splits, test_len, origins)` — explicit designs with
   validation (T0 ≥ 2, T0+test_len ≤ T); defaults reproduce the committed
   auto-derivation EXACTLY (locked for the three recorded panel lengths:
   T=52 → [13,20,26,32,39]/13; T=86 → [21,32,43,54,65]/21; T=136 →
   [34,51,68,85,102]/34). CLI: `--origins ... --test-len ...`.
2. `_split_panel(df, T0, test_len)` — the single point where the gate
   touches time; unit test proves the E2 design trains on periods < 136
   and scores EXACTLY 136..169 (re-indexed 0..33), with no leakage even on
   a longer panel.
3. `build_e2_members.py` — selects the E2 universe from the EXISTING T=136
   panel only (standard rule, K=12,500/B=50,000); validates 50,000 unique
   ids; prints the SHA-256 and the train-end anchor date. **Executed and
   frozen this day (no extension data read):**
   `e2_members_t136.parquet`, sha256 f0b463ca…7562 (manifest + protocol A5
   execution record); anchor = period 135 = week of 2021-06-28, which the
   E2 runner must verify on the extended panel before scoring.
4. Gate-side pre-score validation: `--member-ids-file` now rejects
   duplicate (T0, entity_id) rows and prints the file's sha256 into the
   run log before any scoring output.
5. Round-4 record fixes: §2z-h wording corrected (the discriminating
   vector is rng-reconstructed, not hard-coded); CLI default-to-NNLS
   wiring now has its own tripwire test (`nnls = not args.legacy_clip`
   in both entry points), closing the reviewer's "wiring could evade the
   API lock" concern.

Suite **85 green** (80 + 5: window resolution, E2 split exactness ×2,
explicit-design validation, CLI wiring); package suite 8 green; legacy
guard unchanged. The registered E2 command is recorded verbatim in the A5
execution record (CONFIRMATION_PROTOCOL §10) — single block,
`--origins 136 --test-len 34`, frozen membership file.

**State: the extension is one owner "go" away.** E1/E3/E5 run on the
extended panel as registered; E2 runs the recorded command after the §2
data build registers `reddit_comments_ext` and the period-135 anchor date
is verified. No model or estimator work remains queued ahead of it.

## 2z-j. 2026-07-12 — FINAL pre-run hardening of E1–E5 (two independent audits adjudicated → Amendment A6): a REAL week-boundary leak closed, two invalid E1 comparisons fixed, E2 MC precision frozen, E4/E5 execution paths built and synthetic-tested, E1 reference frozen — and the dry-run discipline caught a defect in A6's own first draft

Two final audits of the protocol (one external, one internal — the owner's
"last chance to change or improve" pass), adjudicated and merged into
**Amendment A6** (registered pre-data; nothing loosened). Suite **94 green**
(85 + 9 synthetic runner tests); package 8 green; legacy guard unchanged.
No extension data read; only the frozen T=136 panels inspected.

**(1) The week-boundary leak (external audit's catch — REAL, verified):**
the frozen weekly panel's final row (2021-06-28) is a 3-day partial week
(dailies end Wed 2021-06-30: 110,508 rows / 154.76M vs ~152k / ~334M for
full weeks — verified this session). A naive rebuild through 2022-12 folds
July 1–4 — extension days — into that row, which is E2 TRAINING period
135, and the period-135 date anchor cannot see it. A6.1: frozen prefix
preserved byte-equal (partial week INCLUDED as frozen), extension =
complete weeks 2021-07-05..2022-12-19 only (77 wks; extended T=213),
boundary days reported never folded, full intake stop-rule list, enforced
mechanically by `check_extension_panel.py` (synthetic-tested: the leak
signature — a changed frozen-week VALUE with matching dates — is caught)
and re-verified inside the E2 runner (`--frozen-prefix`).

**(2) E1's two invalid comparisons (mine + external, converged):**
- κ "declining head→tail" contradicted the recorded md6 curves (they RISE
  from the head). And the corrected first draft ("nondecreasing with a
  strict increase") was ITSELF killed by the A6 dry-run discipline: the
  frozen reference's pooled thirds are 0.0050/0.0198/0.0191 — mid vs deep
  differ by 0.0007 (noise) — so strict monotonicity fails the reference.
  REGISTERED RULE: head third strictly the most persistent
  (head κ < min(mid, deep)); mid/deep unordered.
- b horizon mismatch (external's catch): the 1.08 reference is h*=13-
  specific; `estimate_mix_b` auto-selects h*=8 on a 77-week segment
  (rule verified in code: longest of (13,8,4) with T//h ≥ min_changes+1).
  FROZEN at h=8 both sides; reference b(8) = 1.0163; band unchanged.
- Plus: all-four-components-must-pass made explicit; Spec-B ±25% at all 12
  interpolated coordinates; A4's "10% of band edge" = 10% of band width;
  s-band context registered non-gating (sub-window range 0.64–0.67).

**(3) E2 precision + algebra (A6.3):** reps=20 (seeds 0..19), boot=2000
(the registered command's implicit defaults were reps=3/boot=400 — MC
noise in scored collision moments); coverage clause stated algebraically
for the single block (model dRank1 median inside the held-out 95% CI —
the original criterion's letter); h=4/13 in-CI, CRPS/PIT/W1, clustered
intervals all declared descriptive-can-never-rescue; runner now enforces
the membership sha (`--expect-member-sha`) and prefix equality before
scoring.

**(4) E4 made executable (external's catch — the registered statistic had
no implementation):** `e4_kappa_transport.py` with every construction pin
in A6.4 (train-edge cells reused on extension; EB shrinkage κ̂_i = ρ̂·r_i
with ρ̂ = train split-half signal share; Q1∪Q5 vs Q3; shared-survivor n
reported). Synthetic-tested both directions: persistent per-entity κ_i
transports (passes), shuffled extension breaks it (fails).

**(5) E5 made executable:** S(10) + head-offset readouts added to
community_metrics; offset reported RAW (registration fidelity) AND
per-week level-adjusted (the §2z-a level-contamination lesson, declared);
seeds frozen at 20; trigger algebraic (mean_seed S1_sim − S1_emp >
2·SD_seed). Level-adjustment behavior locked by test (kills a pure level
shift, preserves a head-only distortion).

**(6) E3 workload frozen** (reps=20, boot=500, seed 0; surrogate 50 draws;
explicit extended-panel membership-sensitivity invocation) and the
**overall decision rule declared before outcomes exist** (A6.7): core
confirmation = E1 AND E2; mixed = exactly one; failure = both; E3/E4/E5
diagnostic, can never rescue; fixed order, no conditional stopping,
report before any exploratory contact; non-gating outcome predictions
registered (E2 rel err ~0.16–0.24; s near 0.64–0.69; b(8) near 1.02).

**E1 reference FROZEN (2026-07-12, T=136 panel, NNLS estimator):**
s = 0.6922, b8 = 1.0163, κ thirds 0.0050/0.0198/0.0191, Spec-B
0.071..0.248; `e1_reference.json` sha256 f78a6ee2…c298 (manifest-pinned).

**State: E1–E5 executes under A1–A6 with every runner existing, every
criterion dry-run-validated against its own reference, and every input
hash-pinned. The remaining human action is the owner's go.**

## 2z-k. 2026-07-12 — Round-6 execution audit accepted in full (Amendment A7): the A6 tooling failed OPEN in four places; every gate now enforces what the registration claims, with the false-pass cases locked by test

The round-6 review's framing was exact: "execution corrections to make A6
true in code" — no scientific threshold or gate changed. All four blocking
findings verified and fixed; all smaller gaps closed (implemented, not
withdrawn). Suite **103 green** (94 + 9); package 8 green; no extension
data read.

**(1) Intake gate rewritten FAIL-CLOSED** (`check_extension_panel.py`, now
4 REQUIRED args): exact schema equality (a missing frozen column FAILED
OPEN via the shared-column intersection — now FAILS); daily panel and raw
monthly dir are required arguments (the no-daily branch previously reached
PASS); 18-file raw inventory; full calendar-day coverage
2021-07-01..2022-12-25; daily hygiene; weekly = Σ daily on EVERY shared
numeric metric column; day-guard corrected to the registered PRIOR-days
trailing median (the draft included the current day). The reviewer's three
reproduced false passes — missing frozen column, missing extension day,
omitted daily panel — are now failing tests.

**(2) Prefix-preserving assembler built** (`build_extension_weekly.py`):
frozen rows byte-identical + complete-week daily sums only; boundary days
(2021-07-01..04, post-2022-12-19) to a side parquet, never folded. The
official Monday-fold builder is explicitly NOT the weekly assembler
(verified: `build_reddit_comment_panels.py` folds every date to its
Monday — it would reproduce the A6.1 leak). Round-trip locked by test:
assemble → gate PASSes, prefix sums unchanged, July 1–4 in the boundary
file.

**(3) E1 daily-path P0 fixed:** `_quantities` required-arg `daily_path`,
resolved fail-closed from the SELECTED platform's entry (the hardcoded
`reddit_comments` path would have fed FROZEN-period dailies to the
extension Spec-B — wrong input, silently). Wiring locked by test
(fail-closed resolution + no-default signature). The s block bootstrap is
now a true MBB (gapped block relabeling keeps repeats; the set() version
was a subsample statistic — non-gating, relabeled honestly).

**(4) Registered E2 command corrected** to include `--frozen-prefix`
(A6's "independently re-verifies" claim was untrue of the command as
written). A7 carries the full corrected command.

**Smaller gaps, all implemented rather than withdrawn:** per-horizon
in-CI indicators + week-block clustered CI sensitivity now print in the
gate log (declared descriptive, can never rescue; `_boot_ci_weekblock`
covered by a synthetic coverage test); `membership_robustness.py
--platform` (was hardcoded); `e5_headlaw.py` frozen E5 invocation — 20
seeds (0..19) hard-coded, S(1)/S(10)/raw+level-adjusted offsets, and the
registered trigger computed algebraically with the verdict printed
(trigger algebra + direction + frozen seeds locked by test).

**Amendment A7 registered** (execution-truthing; pre-data; command texts
superseded where they omitted enforcement). The protocol now runs A1–A7:
every evaluation has a runner, every runner has synthetic tests INCLUDING
its failure modes, every criterion was dry-run against its own reference,
every input is hash-pinned, and the intake gate cannot pass on an
incomplete intake. **Awaiting the owner's go.**

## 2z-l. 2026-07-12 — Round-7 accepted (Amendment A8): the last three intake false-PASS paths closed with adversarial tests; E5 SD convention frozen at ddof=0. The gate now fails closed under every reproduced attack

Round 7 verified everything from A7 and reproduced three remaining
false-PASS paths, all intake-side. All closed; suite **107 green** (103 +
4 net new adversarial cases); package 8 green; no extension data read.

1. **Daily-only cells** (P0): the weekly=Σdaily check reindexed daily sums
   to the weekly index, silently discarding daily-only (entity, week)
   cells. Now: exact index-set equality in BOTH directions before value
   comparison, and every frozen numeric metric must exist in the daily
   panel. Adversarial test: a ghost entity's daily rows → FAIL.
2. **First-week guard blindness** (P0): the day guard built its trailing
   median from extension dates alone, leaving July 1–7 — which contain
   E1/E2's first scored week — with no baseline. Now: frozen daily counts
   prepended, the registered `instrument_eras.flag_days` applied,
   extension dates adjudicated. Adversarial test: an
   aggregation-consistent 90%-entity collapse of July 1–7 (weekly rebuilt
   from the collapsed dailies, so ONLY the guard can catch it) → FAIL.
3. **Inventory ≠ parse success** (P0): the directory glob passed 19 files
   and zero-byte files and could not see parse errors. Now: the builder's
   coverage log is a required gate input — exactly one "ok" nonempty
   record per month, no duplicates/missing. Adversarial tests: duplicate
   month, missing month, rows=0 → each FAILS.
4. Boundary-day coverage extended through 2022-12-31 (a daily panel ending
   Dec 25 previously passed while A6 registers those days as reported).
5. **E5 SD convention frozen** (P1): trigger uses POPULATION SD (ddof=0),
   matching the registered baselines' convention (A7's sample-SD would
   have shifted the 2-SD threshold); exact-threshold test on a fixed
   vector locks it, and the direction/firing tests still pass.

The intake fixture now builds its "good" panel THROUGH the assembler, so
every gate test also exercises the prefix-preserving build path
end-to-end. Amendment A8 registered (supersedes A6.1's directory-glob
clause with the strictly stronger log check; no scientific threshold or
gate changed). Protocol = A1–A8. **Awaiting the owner's go.**

## 2z-m. 2026-07-12 — Round-8 accepted (Amendment A9): zero parse errors now mechanically enforced from the aggregator's own processing log — the final intake defect. Reviewer's go-condition met

Round 8 verified all of A8 (including the E5 ddof=0 freeze) and reproduced
ONE remaining false PASS: the coverage log has no parse-error field, and
`aggregate_reddit_monthly.py` writes status="ok" even with errors > 0 — so
"ok and nonempty" never established the registered zero-parse-errors rule
(their synthetic errors=123 month printed PASS).

**Fix (A9, registered):** the gate's SIXTH required input is the
aggregator's existing processing log (pipeline untouched — it already
records `errors` per month). Rule: for every month, the LATEST comments
record (by finished_at_utc; re-runs append, the panel comes from the last
run — declared) must be ok, nonempty, and have **errors == 0**. No
tolerance; an un-fixable month stops model contact. Adversarial tests:
errors=123 → FAIL (the exact round-8 reproduction); missing month → FAIL;
latest-record semantics locked in both directions (clean re-run after an
errored one PASSES; an errored run after a clean one FAILS).

Suite **110 green**; package 8 green; legacy guard unchanged; no extension
data read. Protocol = **A1–A9**. Per the round-8 verdict ("once the
parse-error field is mechanically enforced, I give the go"), the
reviewer's final condition is now met. **E1–E5 executes on the owner's
word, once, exactly as registered.**

## 2z-n. 2026-07-12 — T9 readiness check for the confirmation battery: 25/25 input checks pass (two initial flags adjudicated as check artifacts); DISCOVERED that the owner-side aggregation already ran (timeline disclosed, owner acknowledgment required); frozen baselines hash-pinned; the pipeline's naive extended weekly is quarantined

**Checklist (read-only; no extension observation read — only log/manifest
metadata and file sizes inspected):**
- Mounts: T9 via `data/ssd` ✓; WD Passport ✓ (18/18 extension monthlies
  present, none zero-byte).
- Frozen registered artifacts: `e2_members_t136.parquet` and
  `e1_reference.json` match their protocol pins and the manifest ✓;
  members = 50,000 unique ids at T0=136 ✓; reference fields intact
  (s=0.6922, b8=1.0163) ✓.
- Frozen weekly panel (the E2 prefix baseline): T=136 ✓; last week
  2021-06-28 partial with EXACTLY the recorded signature (110,508 rows /
  154,761,599 metric total) ✓; schema = the 7 registered columns ✓; no
  duplicate keys ✓. Adjudicated flag #1: the FIRST weekly row (2018-11-26)
  is also partial — it covers Dec 1–2 only (daily data starts 2018-12-01);
  frozen design, no leak possible (nothing earlier exists), my check's
  expectation was wrong, not the panel.
- Frozen daily panel: ends 2021-06-30 ✓; 943 days (recorded census) ✓;
  carries ALL weekly numeric metrics (assembler + gate requirement) ✓.
- Logs: processing log exists with all A9-required columns ✓; latest
  comments records errors == 0 for every month on file ✓. Adjudicated flag
  #2: the coverage-schema probe had grabbed the FB coverage file; the
  comments coverage logs have exactly the required schema ✓.
- **Frozen-baseline hashes now pinned in the manifest** (they are the E2
  `--frozen-prefix` comparison baseline; integrity must be checkable):
  weekly `b00ee41f7d2813e7…0041`, daily `19ea5eeb846cd775…2323`.

**DISCOVERY (material, disclosed):** the extension aggregation ALREADY RAN
— owner-side resume, 2026-07-11T22:48Z..2026-07-12T10:46Z, 18/18 months
status ok with errors = 0 (processing log), and the pipeline built
`reddit_comments_2018-12_2022-12_{daily,weekly}` this morning. Two
consequences, both recorded in the protocol §5 status:
1. *Timeline vs registration language:* amendments A6–A9 were committed
   AFTER that mechanical aggregation began. They derive exclusively from
   frozen-panel dry runs, code audits, and synthetic tests — no extension
   observation has been read by any analysis session. §0's "before data
   processing" is construed as ANALYSIS CONTACT for amendment validity;
   this construction is disclosed rather than discovered, and the OWNER
   MUST EXPLICITLY ACKNOWLEDGE it before E1–E5 runs. (The alternative —
   voiding A6–A9 — would run the battery with a κ criterion its own frozen
   reference fails, which is strictly worse than the disclosed reading.)
2. *The pipeline's extended weekly is QUARANTINED:* it was built by the
   Monday-fold builder and is presumed to contain the A6.1 boundary fold
   (July 1–4 folded into E2 training period 135). It is NOT a registered
   input and must not be opened; the registered path is
   `build_extension_weekly.py` (frozen weekly + the extended DAILY, which
   is fold-free at day level) → the 6-input intake gate. I did not open
   either extended parquet.

**Go-time sequence (unchanged, now with every input verified present):**
(1) owner acknowledges the timeline construction; (2) assemble the
registered weekly via `build_extension_weekly.py`; (3)
`check_extension_panel.py` on the 6 inputs must print PASS; (4)
E1→E2→E3→E4→E5 exactly as registered; (5) the confirmation report.

## 2z-o. 2026-07-12 — CONFIRMATION BATTERY HALTED AT THE INTAKE GATE: `check_extension_panel.py` FAILS on "negative values in extended weekly column 'comment_karma'" — a failure OVER-DETERMINED by the frozen prefix itself (10,634 negative cells in the registered T=136 baseline); E1–E5 NOT run; owner adjudication required

**What was executed (owner GO + timeline acknowledgment recorded in protocol
§5, 2026-07-12; battery attempted once, exactly as registered under A1–A9,
per the §2z-n go-time sequence; ZERO code edits this session):**

- **Step 0 — preconditions: ALL HELD.** Suite 110 green from repo root;
  T9 (`data/ssd/derived`) and WD ("/Volumes/My Passport for Mac") mounted;
  `PLATFORMS["reddit_comments_ext"]` present with `daily_path` set to the
  extension daily; all four pinned hashes verified against the prompt AND
  `runs/2026-07-11_nnls_audit/MANIFEST.sha256` (e2_members f0b463ca…7562,
  e1_reference f78a6ee2…c298, frozen weekly b00ee41f…0041, frozen daily
  19ea5eeb…2323). Interpreter = the registered 3.11 framework python.
  Log: `runs/2026-07-12_confirmation/step0_preconditions.log`.
- **Step 1 — registered weekly assembled (A7 builder, non-destructive):
  output EXACTLY as registered.** Frozen prefix 14,099,317 rows unchanged +
  12,358,947 extension rows over 77 complete weeks (2021-07-05..2022-12-19);
  boundary days (2021-07-01..2022-12-31, 722,222 rows) written to the side
  parquet, never folded. Output sha256 recorded:
  REGISTERED weekly `93942240d766e5fa…380d`, boundary days
  `296d34ee0395c3d9…2046`. Log: `step1_build_weekly.log`.
- **Step 2 — the 6-input intake gate: FAIL.** Verbatim output
  (`step2_intake_gate.log`):
  ```
    [1/6] schema equality + frozen-prefix equality: OK
    [2/6] complete-week window: OK (77 weeks, period 136 = 2021-07-05)
  INTAKE FAIL: negative values in extended weekly column 'comment_karma'
  ```
  Per A6.1/A7/A8/A9 and the execution instructions, a non-PASS stops
  EVERYTHING: **E1–E5 were NOT run.** No workaround was attempted; no
  criterion, threshold, or gate line was touched.

**Diagnosis by measurement — on FROZEN, already-registered panels ONLY (no
extension observation was read by this session; the only extension contact
was the gate's own internal reads):**

- Frozen T=136 weekly (`b00ee41f…0041`): `comment_karma` has **10,634
  negative cells** (min −29,291) out of 14,099,317 rows. Frozen daily
  (`19ea5eeb…2323`): **89,477 negative** `comment_karma` cells (min −30,761)
  out of 47,307,511. `metric_value`, `submission_karma`, and both count
  columns: zero negatives in both panels.
  Log: `step2b_frozen_negative_context.log`.
- The gate's `_basic_hygiene` applies "no negatives" to EVERY numeric column
  over the ENTIRE extended weekly — including the frozen prefix, which check
  [1/6] had just verified byte-equal to the registered baseline. **The FAIL
  is therefore over-determined by the frozen prefix alone: no assembly of
  the extension, however perfect, could pass the gate as implemented.**
  Whether extension rows ALSO contain negative `comment_karma` was NOT
  examined (that would be extension analysis beyond the halted battery).
- Interpretation (measured, then plainly stated): negative comment karma is
  a real platform outcome (net-downvoted comments) and is present throughout
  the program's own frozen baseline — the registered "zero negative metrics"
  stop rule, as scoped in code to all numeric columns of the whole panel, is
  inconsistent with the registered frozen inputs themselves. This is the
  INVERSE of the false-PASS defect class that rounds 6–8 (A7–A9) hunted: a
  fail-closed check whose SCOPE was locked by synthetic adversarial tests
  (which contained no negatives) but was never dry-run end-to-end against
  the real frozen panel. The fail-closed discipline worked mechanically; the
  rule's scope was wrong for this platform's metric.

**Disposition (protocol §2: a data-gate failure is a DATA problem, never a
modeling degree of freedom):** battery halted at step 2; partial record
archived and committed. The negative-metrics stop rule is a frozen,
registered intake criterion — changing its scope (e.g., restricting the
no-negatives rule to `metric_value` and count columns, where the frozen
baseline is clean) is an OWNER decision. Note the timeline consequence for
any such change: the gate has now run over the assembled extended panel, so
a scope correction cannot be registered as a clean pre-data amendment; it
would need the same disclosed-construction + explicit-owner-acknowledgment
treatment as A6–A9 (protocol §5, 2026-07-12), with the mitigating facts
that (i) the failure is provable from the frozen prefix alone and (ii) no
extension observation has been read by any ANALYSIS. Whether and how to
proceed is not this session's call.

**Archive:** `llm_fitting/runs/2026-07-12_confirmation/` — step0–step2b
logs + `MANIFEST.sha256` (all 9 inputs incl. the REGISTERED weekly and
boundary-days parquets, all logs). Suites at session end: `tests/` 110
green; `Python/rankdiff/tests` 8 green; legacy guard untouched (no code
edits).

**Reproduction:**
```
python3 -m pytest tests/ -q                                    # 110 passed
python3 -u llm_fitting/build_extension_weekly.py \
    data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet \
    data/ssd/derived/reddit_comments_2018-12_2022-12_daily.parquet \
    data/ssd/derived/reddit_comments_2018-12_2022-12_weekly_REGISTERED.parquet
python3 -u llm_fitting/check_extension_panel.py \
    data/ssd/derived/reddit_comments_2018-12_2022-12_weekly_REGISTERED.parquet \
    data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet \
    data/ssd/derived/reddit_comments_2018-12_2022-12_daily.parquet \
    data/ssd/derived/reddit_comments_2018-12_2021-06_daily.parquet \
    data/ssd/manifest/reddit_comments_2018-12_2022-12_coverage.csv \
    data/ssd/logs/reddit_monthly_processing_log.csv               # INTAKE FAIL
```

## 2z-p. 2026-07-12 — The §2z-o intake FAIL adjudicated as a REGISTERED-RULE DEFECT (blanket non-negativity applied to a signed audit field the model never ingests), by frozen-data measurement and mechanical code verification; Amendment A10 drafted (per-column semantics; INERT until attested + acknowledged); battery restarts from Step 0 after registration

**Adjudication basis (all measurements FROZEN panels + code only; the
extension's negativity rate, distribution, and every other
extension-specific quantity remain unexamined — exact blindness language at
the end of this section). Three assessments converged (this session, one
external model review, the owner's own analysis); disagreements adjudicated
below. Archived: `runs/2026-07-12_confirmation/step2c_frozen_semantics_check.log`.**

**Verified facts (each mechanically checked this session):**
1. **The model never ingests signed `comment_karma`.** Daily-panel build:
   `metric_value = comment_karma.clip(lower=0)`
   (`scripts/data_wrangling/build_reddit_comment_panels.py`,
   `load_comment_month`). `load_panel` (`minimal_rankdiff.py`) reads ONLY
   `[id_col, ts_col, metric_col]`; a grep across `minimal_rankdiff.py`,
   `rankdiff_kalman.py`, `e1_transport.py`, `e4_kappa_transport.py`,
   `e5_headlaw.py`, `scorecard_bands.py`, `spec_b_sigma_obs.py` finds ZERO
   references to `comment_karma`/`submission_karma`. Negative signed karma
   cannot affect ranks, membership, estimates, simulations, or any E1–E5
   quantity.
2. **The daily identity `metric_value == max(comment_karma, 0)` holds on
   ALL 47,307,511 frozen daily rows** (exact). The weekly analogue is
   FALSE by construction (clipping precedes weekly aggregation — clip does
   not commute with the sum); weekly integrity is the existing
   weekly = Σ daily check, which already binds `comment_karma` exactly.
3. **`submission_karma` and `submission_count` are identically zero** in
   both frozen panels (comments-only panel).
4. **Clipped mass is measured, not assumed:** absolute negative karma
   removed by clipping ÷ total modeled positive-part karma =
   831,807 / 39,055,688,181 = **0.002130%** on the frozen daily (negative
   cells: 0.1891% daily, 0.0754% weekly — §2z-o).
5. **One outside-input claim REFUTED:** the external model's §2z-p draft
   asserted `metric_value = submission_karma + comment_karma` as the schema
   contract — false on the frozen daily (identity fails; submission fields
   are identically zero). Its draft is not committed; this section is the
   record.

**Adjudication.** A6.1's "zero negative metrics", as scoped in code to
every numeric column of both panels, is INFEASIBLE: the hash-pinned frozen
baseline itself fails it. This is the same defect class as the A6.2 κ
criterion (a registered rule its own frozen reference fails), which the
protocol already treats as correctable by dated amendment. The §2z-o halt
was the CORRECT execution of a frozen fail-closed protocol and stands
unrelabeled; the root cause is a rule defect, not a data defect. Rejected
along the way (with reasons, so it is not resurrected): a `[0.5×, 2×]`
extension-negativity-rate acceptance band — arbitrary new gate constants
that would convert legitimate voting-behavior drift into another false
data failure; negativity rate and clipped mass become MANDATORY DESCRIPTIVE
READOUTS (daily rate primary — clipping is daily; weekly rate describes
signed aggregation), never gates.

**Amendment A10 (drafted this session;
`runs/2026-07-12_confirmation/A10_DRAFT.md`; INERT for confirmation
purposes until registered):** *post-registration, post-intake-contact,
pre-confirmatory-outcome correction of an infeasible data-validation
rule.* Replaces the [3/6]/[5/6] blanket non-negativity with registered
per-column semantics (metric_value finite integral ≥ 0; comment_count
finite integral ≥ 0; comment_karma finite integral SIGNED; submission
fields ≡ 0; nulls/nonfinite/non-integral rejected everywhere; unregistered
numeric columns fail closed; daily-only identity
`metric_value == max(comment_karma, 0)`); every other check — prefix,
schema, week window, key sets, Σ-equality, coverage/processing logs,
parse errors, day guard — unchanged verbatim. Validation written into the
amendment: frozen-baseline self-test must PASS; adversarial tests both
directions; assembler rebuild at restart must reproduce the recorded
REGISTERED-weekly sha256 (93942240…380d); then a FULL restart Step 0→5,
once, zero discretion, with the §2z-o halted archive preserved unchanged.
Registration requires (i) external round-9 attestation that A10 is minimal
and outcome-blind, (ii) owner acknowledgment of the timeline construction
in protocol §5 — the A6–A9 mechanism.

**Estimand declaration (for the record and the paper's M&M):** the modeled
quantity is **positive-part daily net comment karma**; signed
`comment_karma` is retained solely as an audit field. Clipping affects
0.19% of frozen daily cells and 0.002130% of aggregate modeled karma mass;
the extension's shares will be REPORTED by the amended gate (descriptive,
never gating). Claim language for the eventual confirmation: "a registered
confirmatory evaluation with one disclosed post-registration, pre-outcome
technical correction" — never "executed exactly as originally
preregistered."

**Exact blindness statement (binding for the disclosure):** the automated
intake program accessed the assembled extension panel and printed a single
failure line, itself fully explained by the frozen prefix. No
extension-specific distribution, summary, model statistic, or E1–E5
outcome was observed by any person or analysis before A10 was frozen. The
extension is not claimed to be literally "unviewed."

**Implementation + validation executed same-session (all A10 requirements
that precede registration):** `check_extension_panel.py` hygiene rewritten
to the registered per-column semantics (`_column_semantics` +
`_signed_field_readout` + `--frozen-self-test` mode; `verify_frozen_prefix`
— the E2 runner's import — untouched). Suite **119 green** (110 + 9
adversarial cases locking BOTH directions: signed `comment_karma` with
negatives in daily AND assembled weekly PASSES; negative `metric_value`,
negative count, nonzero submission, broken daily identity, null,
non-integral, unregistered numeric column, broken-identity self-test each
FAIL); package suite 8 green; defaults elsewhere untouched. **Frozen
self-test on the real T=136 panels: PASS**, readouts reproducing the
adjudication numbers exactly (weekly 0.0754%, daily 0.1891%, clipped-mass
ratio 0.002130%) — `step2d_frozen_self_test.log`. **Determinism: the
assembler rebuild reproduces BOTH output hashes byte-exactly**
(93942240…380d weekly, 296d34ee…2046 boundary days) —
`step2e_determinism_check.log`; temp files removed. Manifest updated.
**State: BLOCKED on (i) round-9 external attestation, (ii) owner
acknowledgment in protocol §5. On registration, A10_DRAFT.md is appended
verbatim to CONFIRMATION_PROTOCOL.md as §15 and the battery restarts from
Step 0 under A1–A10 — once, zero discretion.**

## 2z-q. 2026-07-12 — THE CONFIRMATION BATTERY EXECUTED (once, complete, under A1–A10): **A6.7 VERDICT = MIXED EVIDENCE** — E2 (frozen-parameter movement gate) PASSES decisively (0.032 vs baseline 0.171, h=1 in-CI, scale 1.0); E1 (parameter transport) FAILS on s and the κ orientation; E5 head-law trigger FIRED (cross-platform structural); E4 knife-edge NOT-predictive

This is the confirmation report for the registered battery
(CONFIRMATION_PROTOCOL §3–§4 as amended A1–A10), executed once on the
extension 2021-07..2022-12, in order E1→E2→E3→E4→E5 with no conditional
stopping, scored against PRE-DECLARED criteria only. It is written before
any exploratory contact with the extension. Language note (A10): this is
**a registered confirmatory evaluation with one disclosed
post-registration, pre-outcome technical correction** (the A10 intake-rule
fix, §2z-o/§2z-p) — not "executed exactly as originally preregistered."
Archive: `llm_fitting/runs/2026-07-12_confirmation/restart/` (all logs +
MANIFEST.sha256); the §2z-o halted attempt's archive is preserved
unchanged beside it.

**Intake (restart steps 0–2, all PASS):** preconditions held (suite 119 =
the registered 110 + 9 A10 adversarial tests committed pre-restart;
package 8; mounts; platform entry; all four pinned input hashes);
assembler rebuild BYTE-DETERMINISTIC (weekly 93942240…380d, boundary
296d34ee…2046 — the A10 requirement); the 6-input A10 gate printed PASS
on all six checks. First legitimate extension observations (descriptive
readouts, never gating): negative `comment_karma` cells 0.1218% daily /
0.0429% weekly; clipped mass 0.001094% — all BELOW the frozen baseline's
shares (0.1891% / 0.0754% / 0.002130%).

### E1 — parameter transport (extension segment T0=136, T_seg=77): **FAIL** (2 of 4 components)

| component | extension | band / rule | verdict |
|---|---|---|---|
| s (temper, min_changes=12) | **0.8319** (block-boot CI 0.845–0.897) | [0.64, 0.74] | **FAIL** — above the band; the CI excludes the band entirely |
| b8 = s(8)/s(1) (frozen h=8) | **0.9699** | [0.95, 1.15] (ref 1.0163) | **PASS** |
| κ thirds (md6, bands 1–4/5–8/9–12) | 0.0390 / 0.0379 / 0.0591 (ref 0.0050/0.0198/0.0191) | head strictly most persistent | **FAIL** — head 0.0390 > mid 0.0379 (Δ 0.0011) |
| Spec-B centered floor (12 bands) | max band rel dev **0.073** | ±25% at all 12 coordinates | **PASS** |

Non-gating context, registered in A6.2: the train sub-window s range was
0.64–0.67; the extension block-bootstrap CI (0.845–0.897) sits wholly
above the band — the s failure is not a band-edge or noise call. The
amplitude-spread parameter did NOT transport across the 2021-07 era
boundary; the mix exponent (b8, the b≈1 law) and the Spec-B noise-floor
shape DID transport (Spec-B strikingly: 7.3% max deviation vs 25%
tolerance). κ: extension head reversion is ~8× the reference head value
and the head/mid ordering inverts by 0.0011 on ~0.04 — the registered
orientation rule fails as printed. MEASURED, not interpreted; any
mechanism analysis is exploratory and post-report. **A4 both-solves
readout note:** b8 = 0.9699 sits 0.0199 from the lower band edge — within
the declared 10%-of-band-width trigger (0.02) — but the frozen
`e1_transport.py` has NO legacy-solve arm; the registered readout cannot
be produced by the frozen tooling. Reported as a tooling gap, not patched
mid-battery; a both-solves readout, if produced later, is labeled
exploratory/supplementary.

### E2 — frozen-parameter movement gate (A7 command verbatim): **PASS**, decisively

Header enforcement verified before any score: origins=[136],
test_len=34, member sha f0b463ca…7562, frozen-prefix equality re-verified.

| quantity | value | criterion | verdict |
|---|---|---|---|
| model rel err | **0.032** | ≤ baseline + 0.05 = 0.221 | **PASS** |
| historical-mobility baseline | 0.171 | — | (model beats it outright, 5.3×) |
| h=1 model-median-in-CI | **in** | must be in | **PASS** |
| calibrated scale | **1.00** | — | zero calibration freedom used |

Descriptive (declared, can never rescue — none needed): h=4 and h=13
model-median-in-CI both **in**; week-block clustered CI(h=1) [6.0, 7.0]
contains the model median; model medians match empirical EXACTLY at all
three horizons (dR1 7/7, dR4 9/9, dR13 13/13); p90 under-dispersed
(27→25, 41→35, 65→51 — the known tail pattern); CRPS skill vs
persistence +0.001/+0.005/+0.002 at h=1/4/13 (~at par on proper scores);
PIT coverage 0.17/0.54/0.89 vs nominal .10/.50/.90; train fit s = 0.69
(reproduces the frozen reference — internal consistency).

### E3 — descriptive card + bands + surrogate + membership (no pass/fail attaches)

- **Card (LONG stack, reps=20, boot=500, seed 0): 11/15, churn err
  0.051** (T=136 record: 12/15 / 0.044). Omnibus Q = 2067 over 15
  moments; per-block Q/df localizes to VR (407) and boundary (3450);
  churn block healthiest (8.5).
- Boundary rows are the largest z's: outfluxK emp 0.086 vs sim 0.149,
  return4K emp 0.398 vs sim 0.294 — the extension's exit/return flux is
  materially calmer than the sim's (new, extension-specific residual;
  Tier-1 descriptive).
- **Surrogate-adjusted VR reading (A1; 50 phase-random draws, seed 0):**
  card VR13 residual +0.111; data-side functional gap (surrogate mean −
  emp) = **+0.085**; residual beyond the surrogate band ≈ **+0.03** —
  consistent with the §2v decomposition (functional component dominates;
  honest dynamics target ≈ +0.04 transports). κ_i probe: split-half
  Spearman 0.386, noise-corrected true log-SD **0.304** (recorded ≈ 0.30
  — reproduces).
- **Membership sensitivity (trailing-60 + halves):** overlaps 0.66–0.91;
  card 9/15 (8/15 trailing) with churn 0.059–0.087 across all four
  windows at the quick spec (reps=3) — drift real, headline-invariant, as
  on the frozen panel. Reported, not used for selection.

### E4 — κ_i transport (shared-survivor-conditioned, n=7,777): **NOT predictive** (as printed; knife-edge)

Spearman(κ̂_train, resid_ext) = **0.401** [0.381, 0.422] — clears the
0.20 gate by 2×. Concentration (Q1∪Q5 / Q3) = **1.299** [1.240, 1.362] —
fails the 1.3 gate by 0.001, CI spanning the threshold. The pre-declared
reading is binary and both must hold: **NOT predictive**; no κ_i layer is
built (CIs are registered secondary uncertainty, never gates — the
knife-edge is reported, not adjudicated away). ρ̂ (train split-half
signal share) = 0.410.

### E5 — stationary head-law diagnostic (20 frozen seeds, ddof=0): **TRIGGER FIRED**

Within recorded top-2,000 (A3): S(1) emp 0.0901, sim **0.1363 ± 0.0156**;
excess **+0.0461 > 2·SD = 0.0312**, same direction as the recorded
overshoot → **cross-platform structural** per the registered reading
(comments extension joins FB Era A ~2.7× and IG ~2.9×; here ~1.51×).
Diagnostics: S(10) emp 0.2291 vs sim 0.2999 ± 0.0144; head offset 1–600
level-adjusted +0.2559 ± 0.0108 (raw −1.3705, level-contaminated on the
growing census — declared). Consequence per A2: the candidate fix — the
Eulerian stationarity moment appended to the MD partition objective,
opt-in, removes freedom — is now ACTIVATED as a pre-registered next step
(adoption still gated on in-sample cards holding and the frozen OOS gates
not degrading). NOT implemented this session, as registered.

### OVERALL A6.7 VERDICT: **MIXED EVIDENCE** (E2 passes, E1 fails; E3/E4/E5 cannot change it)

Scored against the registered non-gating predictions: E2 rel err
predicted ~0.16–0.24 → actual **0.032 (better than predicted)**; s
predicted near 0.64–0.69 → actual **0.8319 (outside, FAIL)**; b(8)
predicted near 1.02 → actual **0.9699 (in band, low side)**. The
substantive shape: **the frozen T=136 model's held-out movement forecast
into the era it never saw is the battery's strongest result on record,
while the era's own re-estimated amplitude spread and head-κ orientation
drifted out of their transport bands** — movement law confirmed, two
parameter-level transports failed, head-law overshoot confirmed
structural. Failures carry the same prominence as the pass; none prompts
a refit.

### Execution record (honest, complete)

- 3 background runs were killed by the environment mid-battery (E3a at
  zero output, E4 at zero output, E5 twice — at 0, 9, and 12 of 20
  seeds); each produced no verdict when killed and was relaunched with
  the IDENTICAL registered command; E5 completed on the 4th attempt under
  `caffeinate` (machine idle-sleep suspected; severe memory pressure
  observed). Nothing outcome-contingent: no killed run produced a
  scoreable number.
  _[CORRECTION same-day (external review caught the arithmetic): FIVE
  killed runs across THREE commands — E3a ×1, E4 ×1, E5 ×3 (killed at 0,
  9, and 12 of 20 seeds; completed on attempt 4). The gloss for the SI:
  one complete scored execution per evaluation; every completed result
  came from a fresh identical command; no partial output was reused.]_
- The A4 both-solves E1 readout is unproducible by frozen tooling (gap
  reported above).
- Suites at battery end: `tests/` 119 green, `Python/rankdiff/tests` 8
  green; zero code edits during the battery (the A10 gate change predates
  the restart and is itself registered).

### Reproduction (exact commands; full logs in `runs/2026-07-12_confirmation/restart/`)

```
python3 -m pytest tests/ -q                                     # 119 passed
python3 -u llm_fitting/build_extension_weekly.py \
    data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet \
    data/ssd/derived/reddit_comments_2018-12_2022-12_daily.parquet \
    data/ssd/derived/reddit_comments_2018-12_2022-12_weekly_REGISTERED.parquet
python3 -u llm_fitting/check_extension_panel.py \
    data/ssd/derived/reddit_comments_2018-12_2022-12_weekly_REGISTERED.parquet \
    data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet \
    data/ssd/derived/reddit_comments_2018-12_2022-12_daily.parquet \
    data/ssd/derived/reddit_comments_2018-12_2021-06_daily.parquet \
    data/ssd/manifest/reddit_comments_2018-12_2022-12_coverage.csv \
    data/ssd/logs/reddit_monthly_processing_log.csv                # PASS
python3 -u llm_fitting/e1_transport.py --score 136 --platform reddit_comments_ext
python3 -u llm_fitting/rankdiff_kalman.py reddit_comments_ext --oos --top-k 12500 \
    --temperament --min-knot-entities 8 --md-lags 6 --t-tails --mix-hetero \
    --conditional state --dist-scores \
    --origins 136 --test-len 34 --reps 20 --boot 2000 \
    --member-ids-file llm_fitting/e2_members_t136.parquet \
    --expect-member-sha f0b463cab014855d72fd238a2b57a073f06cbe16eb65ff9287eb792d5c7f5562 \
    --frozen-prefix data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet
python3 -u llm_fitting/scorecard_bands.py reddit_comments_ext --top-k 12500 \
    --temperament --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long \
    --stat-factor --two-scale --mix-hetero --reps 20 --boot 500
python3 -u llm_fitting/membership_robustness.py --platform reddit_comments_ext
python3 -u llm_fitting/surrogate_test.py reddit_comments_ext 12500 50
python3 -u llm_fitting/e4_kappa_transport.py reddit_comments_ext 136 --top-k 12500
python3 -u llm_fitting/e5_headlaw.py reddit_comments_ext --top-k 12500
```

## 2z-r. 2026-07-12 — A4 both-solves sensitivity EXECUTED (declared pre-outcome, delayed by the §2z-q tooling gap): every E1 verdict component is SOLVER-ROBUST; s and b8 are bit-identical across solves (solver-invariant by construction, now verified); the κ head/mid inversion is PRESENT UNDER BOTH solves and larger under legacy

**What and why.** A4 declared, before any outcome existed: "the E1 readout
reports both solves if any transported parameter sits within 10% of a band
edge." The contingency fired in §2z-q (b8 = 0.9699, 0.0199 from the 0.95
edge; threshold 0.02 = 10% of band width) but the frozen runner had no
legacy arm — recorded there as a tooling gap, not patched mid-battery.
Executed now as the DECLARED-DELAYED SENSITIVITY (SI-grade; cannot alter
the A6.7 verdict — s fails robustly regardless): `--legacy-clip` added to
`e1_transport.py` (additive; `_quantities(nnls=True)` default locked +
forwarding locked by test; `--make-reference --legacy-clip` fail-closed —
the frozen NNLS reference is never rewritten, hash re-verified
f78a6ee2…c298). Suite **120 green** (119 + 1); the NNLS score re-ran first
and REPRODUCED §2z-q byte-identically (free reproduction check).

**Readout (log `runs/2026-07-12_confirmation/restart/e1_transport_a4_sensitivity.log`):**

| quantity | T=136 ref NNLS (frozen) | T=136 ref legacy | ext NNLS (§2z-q) | ext legacy |
|---|---|---|---|---|
| s | 0.6922 | **0.6922** | 0.8319 | **0.8319** |
| b8 | 1.0163 | **1.0163** | 0.9699 | **0.9699** |
| κ head/mid/deep | .0050/.0198/.0191 | .0061/.0173/.0191 | .0390/.0379/.0591 | .0410/.0364/.0591 |
| Spec-B range | 0.071–0.248 | 0.071–0.248 | (max dev 7.3%) | 0.067–0.253 |

**Conclusions (each mechanical):** (1) s and b8 are BIT-IDENTICAL across
solves on both panels — they derive from the temperament moment, which
never touches the MD solve; the b8 band-edge proximity that fired the
contingency is itself solver-invariant, so the contingency closes with no
caveat. (2) The κ head/mid inversion is NOT a solver artifact: legacy
inverts by MORE (head−mid +0.0046 vs NNLS +0.0011), and the κ level shift
(~4–8× the reference) appears under both solves. (3) Spec-B passes under
both conventions. **Every E1 verdict component is solver-robust; the A4
both-solves obligation is fully discharged.** Verdicts unchanged: E1 FAIL,
A6.7 MIXED EVIDENCE.

Reproduction:
```
python3 -u llm_fitting/e1_transport.py --score 136 \
    --platform reddit_comments_ext --legacy-clip
```

## 2z-s. 2026-07-12 — EXPLORATORY post-battery diagnostics (labeled per the §5 protocol line; nothing here amends E1–E5): the s failure is a SECULAR TREND, not composition; _[LANGUAGE REFINED same-day, §2z-u: "ERA-DOMINANT rise, already visible inside the frozen period" is the binding phrasing — composition is small (+0.018 mean contrast; ~+0.04 within the extension window) but not irrelevant at 0.575 membership overlap]_ the E2 tail under-dispersion is parameter-vintage-consistent; the boundary excess is UNIVERSE-WIDE hazard, not a shell artifact; the E4 knife-edge resolves DOWNWARD

All four diagnostics from the adjudicated post-battery plan, run on the
extension AFTER the §2z-q report was committed, all EXPLORATORY. Logs +
scripts: `runs/2026-07-12_confirmation/restart/x2*.log`,
`ext_s_decomposition.py`, `ext_boundary_flux.py`, `e4 --ext-window`,
platform `reddit_comments_ext_late`.

**(2a) s decomposition — ERA, not composition; and a trend, not a break.**
Four cells (era × membership; fixed-membership machinery; E1-estimand
replication 0.8319 exact; cell A reproduces the reference 0.6922 exact):

| | train mem | ext mem |
|---|---|---|
| train window | **0.6922** | 0.6885 |
| ext window | 0.7998 | **0.8401** |

Era effect **+0.130**, composition effect **+0.018** (train↔ext membership
Jaccard only 0.575, yet membership barely moves s). Matched-77-week frozen
sub-windows: [0,77) → **0.6465**, [59,136) → **0.7524** — no length bias
(they straddle 0.692), and s was RISING within the frozen panel already:
0.65 → 0.75 → 0.84 across 2019→2022. The E1 band was set from a
full-window average of a trending quantity; the "failure" is the trend's
continuation. Amplitude dispersion is a slowly increasing platform
property on Reddit comments (measured; mechanism unexplored — candidate
paper language: s is era-indexed, the FORM transports).

**(2b) E2 oracle arm (extension-estimated parameters, later 34-wk block;
era-block confound DECLARED; never comparable to the registered E2):**
`reddit_comments_ext_late --origins 43 --test-len 34`, reps 20/boot 2000.
Model 0.072 vs historical-mobility 0.038 (the late-2022 baseline is very
strong — the era had stabilized); h=1 in-CI; scale 0.15 (calibration
freedom USED — oracle-arm property). THE TAIL READOUT is the point:
p90 h=1 model 25 vs emp 25 (**exact**; frozen-parameter E2 gave 25 vs 27),
h=4 40 vs 37, h=13 66 vs 59 — with era-vintage parameters (s≈0.84) the
displacement tails are no longer under-dispersed (slightly OVER at long
h). SUPPORTS (directionally, confound declared) the hypothesis that E2's
growing p90 under-dispersion traces to the s trend, i.e. a
parameter-vintage effect, not a structural tail deficit.

**(2c) boundary-flux decomposition (3 seeds, MC caveat):** the §2z-q
excess is NOT a boundary-shell artifact — it is a universe-wide excess
exit hazard, LARGEST DEEP INSIDE the universe: out-flux by dropper's band
core/mid/shell sim÷emp = **4.0× / 2.2× / 1.3×**; permanent-exit share
(no return within 13) 0.268 vs 0.157 (1.7×); return rates uniformly low
(~0.29 vs ~0.39 at every h — the empirical return curve is FLAT in h,
returns happen fast or never). The sim ejects established core/mid
entities too often and drops them too deep; the truncation boundary
itself is nearly right. Points at the deep-displacement tail /
exit-hazard interaction, NOT at boundary handling. No parameter added.

**(2d) E4 second-half stability (SI-descriptive, non-confirmatory):**
ext residuals on periods [174, 213) only: Spearman **0.323** (still 1.6×
the gate), concentration **1.225** [1.166, 1.285] — clearly below 1.3.
The registered knife-edge (1.299) resolves DOWNWARD out of sample within
the extension: κ_i ordering is a persistent trait; its concentration does
not clear the pre-declared modeling bar. "NOT predictive" is robust, not
a coin flip.

## 2z-t. 2026-07-12 — Eulerian constraint, VARIANT 1 (within-entity level-variance anchor) implemented opt-in and REFUTED by its own pre-declared adoption gates: the anchor is contaminated by the measured excess low-frequency structure and moves the FB head law the WRONG way. No adoption; defaults unchanged; cross-sectional/ladder anchor is the surviving candidate

The A2 candidate fix (activated by the §2z-q E5 trigger) requires an
"empirical stationary band variance / head ladder" moment. Variant 1
implemented here reads that as the WITHIN-ENTITY stationary level
variance: `--eul-level` appends a finite-window-corrected level-variance
row (E[sample var] = Var·(1−S_T(c)/T²), linear in the solver
coefficients) to BOTH partition solvers, measured Lagrangian by permanent
rank, entity-run-weighted, pooled; zero new components, no new knobs (row
unweighted — the objective's existing convention, declared). Opt-in;
suite 126 green (6 new tests: exact recovery both solvers incl. Spec-B
composition, directional-live row, gating locks, e2e smoke); defaults
byte-identical.

**Adoption gates (A2, pre-declared): S(1)/S(10)/adjusted offset must
improve across platforms; cards hold; frozen gates not degrade. Scored:**

| dev panel | S(1) emp | flag OFF (NNLS re-measured, 10 seeds) | --eul-level | verdict |
|---|---|---|---|---|
| facebook_a | 0.0170 | 0.0463 ± 0.0073 | **0.0870 ± 0.0523** | **WORSE 2×, noisy** (S10 0.147→0.222, offset +0.196→+0.230: all three regress) |
| reddit_comments | 0.1057 | 0.1376 ± 0.0361 | 0.1156 ± 0.0256 | better, within ~1 seed-SD |
| reddit (subs) | 0.0165 | 0.0188 ± 0.0038 | 0.0201 ± 0.0032 | ~unchanged |

Gates (flag-on vs the §2z-g record): FB Spec-B+cond **0.110 ± 0.030** (vs
0.123 ± 0.033 — improves, cov 60%); subs 0.164 ± 0.035 (unchanged);
comments **0.195 ± 0.087** (vs 0.171 ± 0.046 — degrades ~0.5 SD, scale
leaves 1.0 on 3/5). Cards flag-on: FB **15/15 / 0.022** (Pers4 knife-edge
flips back), subs 14/15 / 0.042, comments 12/15 / 0.023.

**VERDICT (by the pre-declared gates): NOT ADOPTED — refuted.** The
S(1)-family regresses decisively on FB, the confirmed-overshoot platform,
and the comments gate degrades. **Mechanism (measured, and it teaches the
next design):** the within-entity level variance at the FB head is ~0.64
vs the partition's implied stationary level 0.19 — the anchor is
DOMINATED by the program's own measured excess low-frequency structure
(§2v home drift), so matching it inflates the head's stationary W
(κ pinned at grid-min, implied level 0.19→0.64) — the OPPOSITE of the
deflation E5 calls for. Where the anchor ≈ the implied level (comments:
0.49 vs 0.46) the row is a benign nudge. A time-variance anchor cannot
separate "wide stationary law" from "drifting homes"; the E5 overshoot is
about the CROSS-SECTIONAL width of the stationary head. **Surviving
candidate = the cross-sectional/head-ladder reading of A2** (constrain
the model's stationary within-band cross-sectional dispersion / head
ladder spacing) — requires measure-first design (how to net out ladder
curvature and temperament mixture) BEFORE implementation; queued as the
next model-development step, owner-visible. `--eul-level` is retained as
the measured negative arm (opt-in, never default).

Interesting non-adopting observations, recorded honestly: the FB gate
IMPROVED and the FB card returned to 15/15 under the refuted variant —
the level row is doing something right in the FB mid-band even while
wrecking the head; the cross-sectional variant should be checked against
both effects. Logs: `runs/2026-07-12_eul_level/`.

**Claim-language PROPOSAL for the owner** _[SUPERSEDED 2026-07-12 by the
§2z-u revision, ADOPTED by the owner same day — the single binding copy is
the §6 addendum 2026-07-12; this block is a historical draft]_ **(Phase 4;
§6/§2x edit is owner-gated, this is a proposal only):** "A parsimonious permanent–
transitory rank model trained through June 2021 transported central
conditional movement into a later era without recalibration (moment error
0.032 vs 0.171 for the historical-mobility baseline; medians exact at
h=1/4/13), while not transporting two parameter values — the amplitude
dispersion s, which is a measured secular trend (0.65→0.75→0.84 across
2019→2022), and the head reversion level — and generating an excessively
concentrated stationary head (S(1) 51% high), now confirmed on a third
instrument. The confirmation supports the movement mechanism while
identifying the stationary head law and movement-tail heterogeneity
(parameter-vintage-consistent, oracle-arm evidence) as the principal
remaining limitations; the first candidate head-law fix was implemented
and refuted by its own pre-declared gates." One disclosed
post-registration pre-outcome technical correction (A10); never "exactly
as originally preregistered."

## 2z-u. 2026-07-12 — Two independent external reviews of §2z-s/§2z-t adjudicated: all verdicts CONFIRMED (one reran the suite: 126 green); language refinements ADOPTED; the claim-set proposal SUPERSEDED by the reviewer's softer text; execution order re-sequenced (vintage-policy gates before head-law design); owner keystrokes pending on §6 adoption and execution go

Both reviews verified the archived numbers independently; neither found a
computational error; both endorse the variant-1 rejection and reject any
rescue by the favorable FB gate/card. Adopted refinements (each now the
binding phrasing of the record):

1. **s (2a):** "era-dominant increase, consistent with a secular rise
   already visible inside the frozen period" — NOT "secular trend, not
   composition" (composition is small, ~+0.04 within the extension-window
   contrast at 0.575 membership overlap, not zero; the matched windows
   rule out the 77-week length effect specifically, not every window
   effect). Registration-design lesson for the SI and ALL future
   protocols (Wikipedia mini-protocol included): transport bands for
   parameters require a PRIOR TREND TEST on the training panel; a
   significant trend makes the band an extrapolation interval, not a
   sample interval — the [0.64, 0.74] band averaged a quantity whose own
   sub-windows already spanned 0.11 of drift.
2. **Oracle arm (2b):** "tail calibration is parameter-vintage-CONSISTENT;
   the oracle arm does not causally isolate s" (all parameters re-estimated,
   different block/membership, scale 0.15, calmer era, loses rel-err to the
   stabilized-era baseline). Placement: exploratory/Discussion, never near
   the confirmatory E2 claim. The IDENTIFYING test is the declared
   trailing-window vintage policy run through the EXISTING five-split
   rolling gates on the frozen panels, scale pinned — same blocks, same
   baselines, only the vintage rule varies.
3. **Boundary (2c):** relative excess largest in the CORE (4.0×); ABSOLUTE
   excess largest in the MID-BAND (~0.047 vs ~0.004 core); shell nearly
   right (1.26×). "Deep-displacement tail / exit-hazard interaction" is a
   HYPOTHESIS, not an identified mechanism. Discriminator adopted for the
   next measurement: crossings vs absences, PLUS conditioning sim core-exit
   events on the exiting entity's v_i quintile — v-concentrated exits
   merge this with the E5 head-law family; v-flat exits point at the
   hazard machinery.
4. **E4 (2d):** the second half is an overlapping subset — a STABILITY
   analysis, not an independent replication; it removes the
   threshold-accident reading, no more.
5. **§2z-t scope:** the refutation is of "the within-entity level-variance
   IMPLEMENTATION of the Eulerian constraint" — never generalized to
   Eulerian stationarity constraints as a class. The FB-gate/card
   improvement under the head-wrecking constraint shows mid-band level
   information and the head problem are SEPARABLE — the next anchor may
   bind only where registered (head third), pre-declared, not
   all-or-nothing across knots.
6. **Paper evidence tiers (adopted):** registered confirmation (E1–E5) /
   delayed registered sensitivity (A4 both-solves) / post-confirmation
   exploratory diagnostics (§2z-s) / post-confirmation development
   negative result (§2z-t) — labeled as four distinct classes throughout.

**CLAIM-SET PROPOSAL, SUPERSEDING the §2z-t draft** (the §2z-t copy is now
a superseded draft; on owner adoption this text moves to §6 as the single
binding copy — the two-live-copies drift risk is declared):

> A parsimonious permanent–transitory rank model trained through June 2021
> transported the registered central conditional-movement moments into a
> later era without recalibration: relative moment error was 0.032 versus
> 0.171 for historical mobility, with median displacement reproduced at
> h=1, 4, and 13. Proper-score performance remained approximately at par
> with persistence, however, and upper movement tails were
> under-dispersed. Parameter transport was mixed: amplitude heterogeneity
> s failed its registered band and post-confirmation analyses showed an
> era-dominant increase already visible in the frozen period; κ
> orientation also failed and κ levels shifted, while the horizon-scaling
> exponent and observation-noise shape transported. The model generated an
> excessively concentrated stationary head, now confirmed on a third
> instrument. An exploratory oracle analysis was consistent with a
> parameter-vintage explanation for tail calibration, but did not isolate
> its cause. The first candidate stationary-law correction — a
> within-entity level-variance anchor — was rejected by its own adoption
> gates.

Presentation emphasis (the one inter-reviewer divergence, adjudicated):
the s trend is presented as a titled finding with the 0.65→0.75→0.84
figure — the confirmation design produced it — but the CLAIM text stays
the qualified version above ("measured and decomposed", not
"discovered"; the sub-window range was on record in §2g-X P3).

**Execution order (re-sequenced per review 1, endorsed):** (1) owner
adopts the claim set → §6 + outline edit with the four-tier labeling;
(2) vintage-policy gates on the frozen panels (small, registered
machinery, feeds the paper's limitation section); (3) head-law
measure-and-design phase on DEV PANELS ONLY under the 8-point spec
recorded from review 2 (permanent-rank bands; weekly common level
removed; ladder curvature smoothed; home-dispersion vs within-entity
drift separated; model-implied moment derived analytically — try the
Gaussian order-statistic linearization BEFORE any indirect-inference
machinery; weight fixed by sampling uncertainty or invariant
normalization, never by head-law fit; directional predictions
pre-declared; adoption gates unchanged) + the 2c crossings/absences/
v-quintile discriminator; (4) Wikipedia pageviews mini-protocol
(trend-aware bands) + acquisition — owner-gated, wall-clock-bound,
start in parallel. Any model produced from step 3 CANNOT be confirmed
on the 2021–22 comments extension (it now informs development);
confirmation requires a new period, platform, or the submissions
extension.

## 2z-v. 2026-07-12 — The 2z-u execution round: claim set ADOPTED into §6; vintage-policy gates run (harmless in-panel; the frozen panel CANNOT power the tail test); head-law measure-first session KILLS the partition-anchor family entirely and localizes the FB overshoot to the t-SPIKE CHANNEL (M1 closes ~60% of the gap); the deep-drop excess is a SECOND, t-exonerated mechanism; three candidate fixes refuted-by-measurement before any code

All exploratory (dev panels for design; extension only in the labeled D4
discriminator). Logs: `runs/2026-07-12_eul_level/` (gate_comments_baseline
_distscores, gate_comments_svintage77, xsec_fb, xsec_comments,
xsec_ext_discriminator, xsec_micro_interventions, xsec_kurtosis_by_band,
xsec_skew_by_band). `--s-vintage W` added (opt-in, default-locked; suite
127 green); `eul_xsec_measure.py` committed with its discriminations
declared in the header before running.

**(1) Claim set adopted (owner):** the §2z-u paragraph is the single
binding copy — §6 addendum 2026-07-12; §2z-t draft marked superseded;
paper outline R5 slot + SI-8 filled (executed results, four evidence
tiers, A10 deviation-table spec, framing rules).

**(2) Vintage-policy gates (identifying test, both arms, same blocks):**
trailing-77 s differs from full-train by only +0.011/+0.026 at the
in-panel origins — the s trend's in-panel increments are TOO SMALL to
power the tail test (rel err 0.168 ± 0.066 vs 0.171 ± 0.046; coverage
60% both; last-split p90 23 vs 24, emp 26). VERDICT: the policy is
harmless in-panel; the Δs ≈ 0.15 vintage effect exists only across the
frozen→extension boundary, so "parameter-vintage-CONSISTENT, not
isolated" remains the paper's ceiling claim. Policy retained as a
declared option.

**(3) Head-law design phase — the partition-anchor family is DEAD, by
measurement:**
- D1 (spacings): the FB excess is concentrated at the very top —
  X(1)−X(2) sim 0.548 ± 0.135 vs emp 0.217; X(10)−X(100) nearly right
  (1.55 vs 1.41). Comments spacings are at par (2.46 ± 0.28 vs 2.37) —
  FB is the offender.
- D3 (attribution grid, the decisive one): S(1) is INSENSITIVE to the
  partition levers — s×0.5 AND κ_head×4 leave S(1) at 0.036–0.045 vs emp
  0.017. No Eulerian moment on the MD objective can fix what the
  partition does not control. (With D2's tension — model around-home
  level 0.19 vs emp within-entity 0.26, yet sim top spacing 2.5× too
  wide — variant 1's failure is now over-determined.)
- M1/M2 (micro-interventions): t_df=inf collapses FB S(1) 0.0451 →
  **0.0241 ± 0.0012** (~60% of the gap; seed noise dies; spacings drop
  toward emp), while head σ_trans=0 does NOTHING (0.0441). **The FB
  overshoot is substantially the Student-t spike channel from BELOW the
  head** (heavy-tailed transitory draws transiting the #1 slot; S(1) is
  a time-mean of a max).
- Kurtosis/skew by rank third (both REFUTE their fix candidates): head
  kurtosis is flat-to-HIGHER (FB 1.27/1.27/1.19; comments
  0.96/0.81/0.10) — rank-dependent df would thicken, not thin, the head;
  skew is near-symmetric (head comments +0.14, FB +0.04) — asymmetric
  innovations are not licensed. So the spike channel's OWN identification
  moments match while its order-statistic output overshoots: the open
  discriminators for the next session are tail shape BEYOND the 4th
  moment (q99/q90 of head changes, emp vs t) and spike
  duration/reversal asymmetry (head-conditional autocorrelation of
  large positive vs negative moves).
- D4 + M3 (deep-drop excess = a SECOND mechanism): sim core exits are
  100% dynamics-driven CROSSINGS (not exit machinery), with the CORRECT
  realized-vol gradient (top-quintile rel rate ≈ 4.5 both sides) but ~4×
  the frequency, and t_df=inf does NOT reduce it (0.151 vs 0.148) — not
  the spike channel, not the hazard machinery, not the vol profile;
  candidate family = persistent-component / medium-timescale large moves.
  Open, measured, named.

**State:** three candidate fixes refuted by measurement before
implementation (Eulerian time-variance anchor §2z-t; rank-dependent
t-df; skewed innovations) — cheap kills, the method working. The E5
trigger's fix obligation now points at a SPIKE-CHANNEL treatment whose
design is constrained by: must preserve the matched kurtosis moment,
must reduce top-1 transit mass, must not degrade the passing movement
gates. Next-session measurement shortlist: q99/q90 tail-shape ratio and
spike-reversal asymmetry at the head; deep-drop event anatomy (size,
duration, component attribution) on dev panels. Owner decision then:
register the chosen candidate as a new pre-declared step (the A2 pattern)
for confirmation on data this program has not consumed.

## 2z-w. 2026-07-12 — Third external review adjudicated: §2z-v CORRECTED by appended note (three overstatements, one arithmetic error); the 20-paired-seed repeat PARTLY RETRACTS D3 — the amplitude tail (s) carries ~half the S(1) excess after all; the t-spike channel remains the largest single lever (66%); the mechanism is now JOINT, not single-channel

**Corrections to §2z-v (appended per the append-only convention; the §2z-v
text stands as the historical record with these notes superseding it):**

1. _"The partition-anchor family is dead" is WITHDRAWN._ Binding
   replacement: **the within-entity level-variance anchor is refuted, and
   local s/κ interventions are insufficient over the tested range.** The
   D3 grid tested two levers locally at 3 seeds; the eul-level arm itself
   proved partition choices can move S(1) (strongly, wrong direction).
2. _"~60% of the gap" was an arithmetic error_ (the correct 3-seed figure
   was 75%); superseded by the 20-paired-seed estimate below.
3. _"The fitted t reproduces the kurtosis; skew matches" was UNVERIFIED_ —
   empirical-only measurements; identification-by-formula is not
   demonstrated reproduction. Narrowed to: simple head-specific tail
   thinning and simple iid skewed innovations are unsupported; matched
   emp-vs-sim moment tables under identical construction are REQUIRED
   before any statement about what the fitted model reproduces.
4. _"From below the head" is a HYPOTHESIS_ pending event tracing (origin
   rank, responsible component, duration, reversal; identical event
   definitions both panels so conditioning-induced mean reversion cancels).
5. _Vintage arms confound declared:_ the σ_obs scale recalibrates after
   the s replacement, so the arms compare two complete policies, not s
   alone — on top of the in-panel underpowering. Frozen-scale rerun
   queued low-priority; "vintage-consistent, not isolated" is the ceiling
   regardless.
6. _Outcome grading (no aggregate "kills" tally):_ anchor = refuted;
   κ_i layer = registered non-activation; df(z) and iid-skew = simple
   forms unsupported; s/κ grid = locally insufficient; vintage policy =
   underpowered, not refuted.

**The 20-paired-seed interventions (CRN, FB, emp S(1) = 0.0170;
`xsec_interventions_20seed.log`):**

| arm | S(1), 20 seeds | paired diff | excess removed |
|---|---|---|---|
| baseline | 0.0474 ± 0.0101 | — | — |
| t_df=inf | 0.0272 ± 0.0045 | −0.0202 ± 0.0083 | **66%** |
| s ×0.5 | 0.0330 ± 0.0075 | −0.0144 ± 0.0089 | **47%** |
| κ_head ×4 | 0.0423 ± 0.0118 | −0.0051 ± 0.0112 | 17% (within noise) |

**PARTIAL RETRACTION of D3:** the 3-seed grid's "S(1) is insensitive to
s" was an underpowered artifact — at 20 seeds the amplitude tail carries
~half the excess. The corrected mechanism statement: **the FB S(1) excess
is JOINTLY produced by the t-spike channel (largest single lever, 66%)
and the temperament amplitude tail (47%), with head-κ weak; the channels
overlap non-additively** (both act through the weekly max — the effective
head spike distribution is the product v_i × t-draw, lognormal×t, whose
tail is heavier than either marginal; each entity's own kurtosis moment
can match while the cross-entity mixture at the head over-spikes). This
interaction is the sharpest current hypothesis for "matched moments,
overshooting order statistics" — it goes into the anatomy session as a
named, testable target (contender-region strata, one-sided observable
tails, matched construction, per the adopted estimand spec).

**Also adopted from the review:** mechanism verdicts SEPARATED from
model-adoption verdicts (the spike candidate is NOT required to repair
outflux — separate mechanisms); the Gaussian scale mixture DEMOTED to
one candidate among several (two shape parameters vs t's one — not more
parsimonious by count; entity-iid vs common-week volatility distinction
mandatory, common volatility being a previously dead hypothesis);
confirmation targets re-ranked — **Wikipedia primary** (protocol frozen
before contact, trend-aware bands), the 2018–22 submissions archive
SECONDARY as an outcome-unseen temporal/cross-metric BACKTEST (the
2024–25 submissions panel is already consumed; same metric family; era
overlap declared — never described as a clean new-platform confirmation).

## 2z-x. 2026-07-12 — THE ANATOMY SESSION (declared predictions A1–A4, scored): A2 REFUTED — empirical per-entity tails are FATTER than the sim's, killing the "t power tail too fat" story; A4's named hypothesis REFUTED — the FAST-VARIANCE channel carries ~70% of the outflux excess; A3 supports the v×t product; the sharpened head hypothesis is RANK-DEPENDENT AMPLITUDE ("quiet giants")

Measurement only (`anatomy_measure.py`, predictions in the committed
header before running; logs `runs/2026-07-12_eul_level/anatomy_*.log`).
Dev panels for A1–A3; extension (labeled exploratory) for A4.

**A1 — spike anatomy (identical event definitions both sides): PARTIAL on
FB; comments REPRODUCED.** FB emp: 0.52 incursions/wk, median origin rank
3, 17% from beyond rank 10, modal #1 holds 23% of weeks (19 distinct).
FB sim: frequency matches (0.55/wk) but origins are DEEPER (origin>10:
33% ± 8) and the modal #1 holds TWICE the share (0.47 ± 0.19 — the high
seed-SD is the runaway-dominant-entity signature). Comments: the
empirical #1 is a stable monarch (3 distinct in 136 wks, modal share
0.97) and the sim reproduces it (3.1, 0.94) — the head-anatomy problem is
FB-SPECIFIC, consistent with §2z-v spacings.
_[CORRECTED 2026-07-13, §2z-z: the ANATOMY (monarch/spacing/identity) is
FB-specific; the S(1) CONCENTRATION residual is NOT — the registered E5
result on the comments extension is emp 0.0901 vs sim 0.1363, FIRED.
Comments is a control for the anatomy only, never for S(1); an
FB-specific mechanism (quiet giants) cannot explain the cross-platform
E5 residual by itself.]_

**A2 — observable tails (one-sided, entity-standardized, identical
construction): PREDICTION REFUTED, both platforms.** In every powered
stratum the EMPIRICAL standardized tails are FATTER than the sim's
(FB 101–1000: pos q99/q90 1.87 vs 1.62; comments 101–1000: 1.93 vs 1.63;
comments 21–100: 1.76 vs 1.61). The sim's standardized tail is nearly
uniform across strata by construction (~1.59–1.66). **The per-entity
innovation tail is not too fat — if anything too thin — yet empirical
S(1) is lower.** The contender strata (perm 2–20) were UNPOWERED
empirically (FB 1 tracked column, comments 0 — the random tracked sample
misses the head; a head-targeted tracked sample is a recorded tooling
item before any contender-stratum claim).

**A3 — v×t product factorial (2×2, 20 paired seeds): DIRECTIONAL
SUPPORT.** Interaction +0.0139 ± 0.0104; the t-removal effect on S(1) is
3× larger at full s (0.0202) than at half s (0.0063). Combined with A2's
inversion, the sharpened FB-head hypothesis is: **the problem is WHO
occupies the head, not each entity's tail** — the sim draws v_i
INDEPENDENT of rank, seating high-amplitude entities at the top where
their v-scaled spikes transit #1; the empirical head may be populated by
low-amplitude "quiet giants" (rank-dependent amplitude). MEASURABLE next:
empirical amplitude residual (realized vol net of the band profile) vs
permanent rank among the top ~100, against the sim's zero-by-construction
correlation.

**A4 — deep-drop component attribution (5 paired seeds; attribution
probes, not candidate models; path-replay deferred as declared): the
NAMED HYPOTHESIS (persistent/medium) IS REFUTED.**

| arm | outfluxK | (emp 0.0861) |
|---|---|---|
| baseline | 0.1485 ± 0.0005 | — |
| **fast σ_trans = 0** | **0.1050 ± 0.0004** | closes ~70% of the excess |
| medium σ_trans2 = 0 | 0.1450 ± 0.0009 | ≈ nil |
| factor off | 0.1481 ± 0.0009 | nil |
| σ_perm × 0.5 | 0.1699 ± 0.0004 | BACKFIRES (packing-density effect: tighter stationary spread → more boundary crossings) |

With M3 (t_df=inf leaves outflux unchanged) this pins the outflux excess
on the fast channel's VARIANCE, not its tail shape. OPEN (recorded): the
arm outputs total outflux only — whether fast=0 also normalizes the
CORE-band excess and the return deficit is unmeasured; next session.

**Emerging picture (labeled hypothesis, not conclusion):** both residuals
implicate the FAST-TRANSITORY LAYER'S ALLOCATION — its variance drives
boundary churn (A4), and its v-scaled spikes drive the FB head (A3 +
§2z-w), while its per-entity standardized tail is if anything too thin
(A2). This points the candidate search at the σ_trans/σ_obs allocation
and the rank-dependence of amplitude — NOT at new components and NOT at
tail-shape surgery. Next measurements, in order: (1) amplitude-vs-rank at
the head (the quiet-giants test; cheap); (2) fast=0 arm re-run with the
band decomposition + returns; (3) head-targeted tracked sample, then the
contender-stratum tails. Candidate selection only after these.

## 2z-y. 2026-07-12 — Quiet-giants + powered-tails + fast-by-band (Q1–Q3, declared, scored): the CORE outflux excess is ENTIRELY the fast channel (core 0.0045→0.0010 vs emp 0.0011); empirical tails are fatter at EVERY powered stratum (tail surgery dead at all ranks); quiet-giants gets DIRECTIONAL support on FB (Spearman +0.333 vs +0.159) with a mixed bin profile; the return deficit is a distinct residual

Measurement only (`quietgiants_measure.py`, predictions declared in the
committed header; logs `runs/2026-07-12_eul_level/qg_*.log`). Head panels
built DIRECTLY from the universe panels (perm rank ≤ 1000) — closes the
§2z-x tracked-sample power gap for comments; FB's contender stratum
remains under the one-sided n≥500 floor (19 head entities × 85 wks),
declared.

**Q1 — quiet giants: DIRECTIONAL SUPPORT on FB; comments = clean negative
control.** FB within-top-100 Spearman(vol, perm rank): emp **+0.333**
(n=42) vs sim +0.159 (n=186, pooled) — the empirical volatility decline
toward #1 is twice as steep as what occupancy selection alone induces in
the sim. Bin profile MIXED (11–40: emp 0.616 ≈ sim 0.636; 41–100: emp
0.847 vs sim 0.676; 1–10 emp under the n≥5 floor) — the signal lives in
the within-top-100 ordering, not the coarse bins; moderate evidence, not
decisive. Comments (where the head is RIGHT): profiles and Spearman
match (emp +0.164 vs sim +0.136) — exactly what the control should show,
tying the amplitude-seating hypothesis specifically to the platform with
the S(1) problem.

**Q2 — tails at every powered stratum: the A2 inversion is universal.**
Comments contender 2–20 (now powered, n=1,890 both sides): emp pos
q99/q90 **1.990** vs sim 1.479. FB 21–100: 1.811 vs 1.600. Every powered
stratum, both platforms, both sides: EMPIRICAL FATTER. **Tail-shape
surgery (mixture/tempered/df changes) is dead at all ranks** — the
model's per-entity standardized tails are uniformly too thin, yet its
head is too concentrated and its boundary too hot: the pathology is
cross-entity ALLOCATION, not per-entity shape.

**Q3 — fast=0 by band (extension, 3 seeds): the CORE excess is ENTIRELY
the fast channel; a smaller second residual remains in mid/shell and the
RETURN DEFICIT is untouched.**

| | outflux | core | mid | shell | ret(4) | perm-exit |
|---|---|---|---|---|---|---|
| baseline | 0.148 | 0.0045 | 0.088 | 0.056 | 0.29 | 0.27 |
| fast=0 | 0.105 | **0.0010** | 0.053 | 0.051 | 0.31 | 0.25 |
| emp | 0.086 | **0.0011** | 0.041 | 0.044 | **0.40** | 0.16 |

_[CORRECTED 2026-07-13, §2z-z: "core" here is CURRENT rank at the exit
week (`ext_boundary_flux.flux_decomp` uses the dropper's position at t),
not permanent rank or tenure — a transient spiker visiting rank 5,000
counts as a core exit, which may be exactly why fast=0 removes the
excess; "EXACTLY" is false precision (no bootstrap interval; "near the
empirical point estimate"). The permanent-rank cohort rerun is §2z-z.]_
Core lands EXACTLY on emp. Residuals after fast=0: mid +0.012, shell
+0.007, and the return rate stays ~0.30 vs emp ~0.40 — droppers in the
real system come back MORE (an iid-like, instantly-reverting signature),
which the fast-variance removal cannot produce.

**The unified read (labeled hypothesis; this is now candidate-design
territory):** every surviving symptom points at the fast layer's
CROSS-ENTITY ALLOCATION and PERSISTENCE SPLIT, not its distributional
shape: (i) FB head — v-scaled fast spikes seated too loud at the top
(A3 interaction + Q1 Spearman gap); (ii) comments-ext core — fast
variance large enough to eject established entities (Q3, exact core
match); (iii) returns — empirical weekly movement is more
instantly-reverting than the fitted AR split (emp ret1 0.39 vs sim 0.29),
i.e. the σ_trans/σ_obs allocation the program has flagged since §4
("more σ_obs helps RACF, less helps OOS displacement"). Candidate
directions for the owner to choose among (each a REALLOCATION or
rank-dependence of existing components, none a new component; each needs
its own registered step with separated mechanism/adoption verdicts and
Spec-B consistency): (a) rank-dependent amplitude at the head
(quiet-giants correction to v-seating); (b) σ_trans/σ_obs re-split
constrained by the return-rate moment (a NEW identifying moment the
current stack never uses — boundary-return rates are measured, cheap,
and Lagrangian); (c) both. Confirmation of any adopted candidate:
Wikipedia primary, submissions backtest secondary (§2z-w).

## 2z-z. 2026-07-13 — Fourth review adjudicated (all corrections ACCEPTED; candidate selection DEFERRED) and its top question answered: under a PERMANENT-RANK cohort the sign FLIPS — the sim ejects established entities HALF as often as reality (0.00115 vs 0.00223/wk); the "core excess" was transient spikers visiting core ranks; the boundary residual is a WRONG MIX, not a wrong level

**Adjudication (all accepted; §2z-x/§2z-y carry inline correction notes):**
(1) "core" was CURRENT-rank at exit week — "established entities collapse
too often" was unsupported wording; (2) "exactly" → "near the point
estimate" (no bootstrap); (3) tails narrowed to "simple global thinning
unsupported in every adequately measured stratum" — FB's contender
stratum is still unmeasured; n≥500 counts dependent entity-weeks; thin
per-entity tails and wrong cross-entity allocation are NOT mutually
exclusive; (4) **comments is NOT a negative control for S(1)** — the
registered E5 result on the extension is emp 0.0901 vs sim 0.1363
(FIRED); comments controls the anatomy only, so an FB-specific
quiet-giants mechanism cannot explain the cross-platform E5 residual by
itself; (5) the boundary return rate is an EULERIAN boundary-event
statistic with Lagrangian follow-up — usable only via indirect inference
with identical selection, never appended to the MD objective as an
identifying row; (6) return-horizon convention mismatch recorded
(`flux_decomp` t+1+h vs card t+h) — unify + regression-test before any
identification use; (7) the fast=0 attribution ran WITHOUT the Spec-B
pin — repeat pinned before claiming a re-split fits the allowed noise
range; (8) candidates (a)/(b)/(c) NOT selected; (c) would violate
one-change-per-experiment; the branching rule and separated
mechanism/adoption verdicts are adopted as written.

**The permanent-rank cohort rerun (the review's top question;
`qg_permrank_cohort.log`; cohort = absence-penalized perm rank ≤ K/2,
presence ≥ 0.7, identical construction both sides, 5 paired seeds):**

| | weekly exit rate (cohort in current top-K → out) |
|---|---|
| empirical (n=461, 97,321 cohort-wks) | **0.00223** |
| sim baseline (n≈493/seed) | **0.00115 ± 0.00018** |
| sim fast=0 | 0.00063 ± 0.00011 |

**SIGN FLIP.** The sim UNDER-produces established-entity departures by
~2×, and removing fast variance makes it worse. Combined with §2z-y: the
boundary residual is a WRONG MIX — too many transient crossings (fast
channel; the current-rank "core 4×" was spiker visits), too few genuine
established-entity departures. Plausible suspect for the deficit
(hypothesis, NOT measured): the rank-dependent exit hazard
p_exit ∝ (r/N)^α protects high-permanent-rank entities too much — the
program's own old IG-era observation. Any fast-variance-reduction
candidate must now carry a mechanism prediction that it does NOT worsen
the established-attrition deficit.

**Defensible unified statement (supersedes §2z-y's phrasing):** fast
transitory variance is the dominant source of simulated one-week
boundary crossings among CURRENT top-half occupants; the sim
simultaneously UNDER-produces established-entity attrition ~2×;
cross-entity amplitude allocation is a plausible contributor to the
FB-specific head anatomy; the cross-platform S(1) residual and the
return deficit remain unattributed; no corrective parameterization is
identified yet.

**Recorded next phase (the review's hardening + identifiability round,
adopted verbatim):** unify/lock the return-horizon convention; 20-seed
paired boundary attribution with entity/block bootstrap intervals,
current-rank AND permanent-rank decompositions reported separately,
absolute (not only conditional) permanent-exit flow; Spec-B-pinned
fast attribution; cross-fitted quiet-giants (half-panel rank / half-panel
vol, swapped) + the comments-EXTENSION quiet-giants (exploratory); EB
amplitude-residual decomposition (mean v vs s(z) vs rank–v correlation);
declared response surface for the variance split (σ_trans, φ, σ_obs
within Spec-B, s) tracking covariances, a permanent-rank reversal
statistic, boundary return, outflux, S(1), and the OOS gate — if multiple
reallocations produce the same return curve, return does not identify
the split. THEN one candidate per residual by the branching rule;
Wikipedia never used to choose between candidates. The binding paper
claim set is UNCHANGED by all of this.

## 2z-aa. 2026-07-13 — Exit audit + auditable sign-flip runner (`exit_audit.py`, self-tested, reproduction command in header): the SIGN FLIP SURVIVES the outcome-independent design (5/6 grid cells, 1 marginal); the established deficit is DYNAMICS (crossings, both sides — exit machinery inert for the cohort); empirical absences are 95–98% TEMPORARY everywhere, relocating the estimand-mismatch hypothesis to the tail/shell where it plausibly drives the RETURN deficit

Fifth-review items 1/2/4 executed as committed, self-testing code
(`exit_audit.py`: P1 hard-asserts a constructed spiker is excluded and an
established departure counts once; log `runs/2026-07-12_eul_level/
exit_audit.log`). All exploratory.

**P2 — the sign flip HARDENS (train-window cohort: defined on periods
0..135 only, scored 136..212 — outcome-independent by construction;
pre-declared grid, ALL cells reported; entity bootstrap, 500 draws; 10
sim seeds):** emp K/2 cohort rate **0.01177** [0.00861, 0.01514] vs sim
**0.00608 ± 0.00088** — the ~2× established-attrition deficit holds in
5/6 cells (sim below the empirical CI's lower bound; the K/4 × 0.8 cell
is marginal: 0.00104 vs lower bound 0.00103). Rates are higher than the
§2z-z quick version because the train-defined cohort scored on the
extension removes survival conditioning — the design correction mattered
in level, not in sign.

_[WITHDRAWN 2026-07-13, §2z-ab: P4's sim side followed fixed SLOTS, not
identities — `tranks` stays positive through rebirth, so "zero deaths"
was guaranteed by the representation (sixth review). Identity-safe rerun:
deaths are 10% (K/2) to 40% (K/4) of sim departure events, and the
sign-flip STRENGTHENS (sim 0.00469 vs emp 0.01177 at K/2). The empirical
half of this paragraph stands.]_
**P4 — the exit/rebirth machinery is EXONERATED for the established
deficit:** cohort exits are essentially ALL rank-crossings on BOTH sides
(emp 441 crossings / 1 absence; sim ~245 crossings / 0 deaths). Real
established entities are genuinely DISPLACED below K twice as often as
simulated ones — the deficit is in the displacement dynamics, not the
death apparatus. **Convergence note (hypothesis, labeled): this connects
directly to the A2 inversion — the model's per-entity tails are too THIN
(§2z-x), and the missing established-entity large displacements are
plausibly the same missing tail mass.** "Tail-shape surgery is dead"
(§2z-y) was scoped to THINNING for the head; the data now motivate the
opposite sign — fatter per-entity displacement tails — jointly with the
allocation story.

**P3 — the estimand conflation is REAL but lives at the TAIL, and the
fitted-vs-raw comparison as printed is MIS-ALIGNED (declared):**
empirical absence events return within 13 weeks 95/97/98% of the time
(head/mid/tail entity-terciles; never-return only 1–4%) — so the
estimator's "absent next week = exit" estimand is ~97% temporary gaps,
while the simulator implements exit as PERMANENT rebirth. This cannot
drive the established deficit (P4: machinery inert there) but is the
leading candidate for the RETURN deficit at the boundary/shell, where
the raw absence rate is large (tail 7.3%/wk) and empirical droppers
return ~0.40 vs sim ~0.29. CAVEAT: the printed fitted exit_rate(z)
thirds (0.0000/0.0001/0.0144) are KNOT-third means and my empirical
strata are ENTITY terciles — non-comparable coordinates; the
knot-aligned estimator audit is still owed before any estimand-change
candidate is designed.

**The three-mechanism map as it now stands (each labeled by evidence
grade):** (1) established-attrition deficit ×2 — HARDENED (P2 grid),
mechanism = displacement dynamics, plausibly thin per-entity tails (A2
convergence, hypothesis); (2) transient-crossing excess — fast-channel
attribution (Q3/A4, current-rank), allocation/seating hypothesis (Q1
directional, FB); (3) return deficit — temporary-absence-simulated-as-
death at the tail (P3/P4 relocation of the fifth review's hypothesis;
knot-aligned audit + Spec-B-pinned re-attribution still required).
Candidate registration remains DEFERRED per the branching rule; the
remaining identification items are unchanged (§2z-z list) plus the
knot-aligned exit audit. The binding claim set is untouched.

## 2z-ab. 2026-07-13 — _[PROVISIONAL, 2z-ac: the empirical side still carried two extension-conditioned selections — full-window universe membership (`restrict_universe` without `member_window=136`) and the tracked sample's ≥70% full-panel presence filter (suppresses extension absences; the 441/1 composition is provisional) — and the sim cohort required 100% train-window identity survival while the empirical permitted partial presence. Superseded by the aligned rerun in §2z-ac.]_ Sixth review adjudicated (slot-vs-identity flaw CONFIRMED in code and fixed with opt-in `track_ids`; 130 tests): the IDENTITY-SAFE rerun STRENGTHENS the sign flip (sim 0.00469 vs emp 0.01177 at K/2 — a 2.5× established-departure deficit, robust under corrected entity AND week-block bootstraps) and REVERSES "exit machinery exonerated" — deaths are 10–40% of the sim's rare departures

**The flaw, confirmed and fixed.** `simulate()` replaces `ids[ex]` in
place at rebirth; tracked slots keep emitting positive ranks under the
new identity, so `tranks <= 0` can never observe death and my §2z-aa P4
("zero deaths") was representational. Fix: `simulate(track_ids=True)`
records the occupant id per tracked slot per week (pure recording, zero
rng draws, flag-off output byte-identical — locked by test);
`exit_audit.idsafe_cohort_events` scores frozen ORIGINAL identities
(dead exactly once — locked by the reviewer's synthetic test: an
established id dying into an inherited slot counts one death, not a
crossing). Suite **130 green** (127 + 3). Also fixed per the review: the
entity bootstrap now resamples events AND exposure together
(ratio-of-sums) with a week-block variant; the presence grid is reported
as what it was (two distinct cohorts, thresholds inactive over 0.6–0.8).

**Identity-safe result (20 seeds; `exit_audit_idsafe.log`):**

| cohort | empirical (ent-CI / wk-CI) | sim identity-safe | deficit | sim death share |
|---|---|---|---|---|
| K/4 (n=258) | 0.00282 [0.00113, 0.00472] / [0.00228, 0.00345] | 0.00060 ± 0.00026 | ~4.7× | **0.40** |
| K/2 (n=514) | 0.01177 [0.00852, 0.01489] / [0.01033, 0.01329] | 0.00469 ± 0.00085 | ~2.5× | 0.10 |

Sign flip TRUE at both cuts under both CI types — the slot conflation had
been INFLATING the sim rate with post-rebirth newcomer collapses, so the
honest deficit is larger than §2z-z/§2z-aa reported. And the exit
machinery is a MATERIAL component of what few departures the sim does
produce (40% at the deep-established cut) — "exonerated" is withdrawn;
the corrected statement is: **the sim under-produces established
departures 2.5–5×, with the missing events overwhelmingly the CROSSING
type the empirical panel exhibits (emp: 441 crossings / 1 absence), while
a substantial share of the sim's own few departures are permanent deaths
the empirical cohort essentially never has.** Both the level AND the
composition of established attrition are wrong, in the directions the
fifth review anticipated.

**Mechanism map, re-graded:** (1) established-departure deficit —
HARDENED at 2.5–5×, identity-safe, both bootstraps; missing mass =
large genuine displacements (the A2 thin-tails convergence stands as the
named hypothesis); (2) the sim's departure COMPOSITION is wrong (deaths
where reality has returnable crossings) — the temporary-absence-as-death
estimand mismatch (P3: 95–98% of empirical absences return) is now
implicated in BOTH the return deficit and the established-departure
composition; the knot-aligned exit-estimand audit is the single highest-
leverage next measurement; (3) transient-crossing excess and the FB head
allocation story unchanged from §2z-y/§2z-z. Candidates remain DEFERRED;
the branching rule stands; the binding claim set is untouched.

## 2z-ac. 2026-07-13 — THE ALIGNED RERUN (seventh review's design, executed exactly): train-only universe, NO survivor filter, SYMMETRIC identity-level construction — the established-departure deficit SURVIVES its third adversarial hardening (2.3× at K/2, 3.4× at K/4) and the composition mismatch is now cleanly measured (empirical departures 98.6% crossings; sim departures 14–52% permanent deaths)

`exit_audit.py --aligned` (committed; suite **131 green** incl. the
identity-history reconstruction test); log `exit_audit_aligned.log`.
Design per the seventh review, all four P1/P2 items fixed: universe
membership frozen on periods 0–135 (`member_window=136`); the empirical
side uses the FULL 50,000-entity identity matrix (no ≥70% full-panel
tracked filter — extension disappearances can no longer be excluded);
the simulated side reconstructs per-identity histories from
`track_ids` (zeros before birth / after death) and applies the SAME
absence-penalized cohort rule; composition from POOLED counts; 30 seeds
with quantiles.

| cohort (train-defined) | empirical rate (events) | sim rate [q10, q90] | deficit | sim pooled death share |
|---|---|---|---|---|
| K/4 (n=2,544) | 0.00172 (311 cross, 19 abs: 10 return≤13, 9 never) | 0.00050 [0.00028, 0.00077] | **3.4×** | **0.52** |
| K/2 (n=5,138) | 0.00800 (3,000 cross, 43 abs: 22 return≤13, 19 never) | 0.00350 [0.00283, 0.00410] | **2.3×** | 0.14 |

**Both findings now stand on a construction with no known selection
defect:** (1) the sim under-produces established departures 2.3–3.4×,
with the empirical q-range nowhere near the sim's; (2) the composition
is inverted — empirical established departures are ~98.6% returnable
rank-crossings (never-returning absences are 0.6% of events), while the
sim's departures are 14% (K/2) to 52% (K/4) permanent deaths. The
temporary-absence-as-death estimand mismatch and the missing
large-displacement dynamics are BOTH required to explain this jointly.

One structural note (so the axis is not re-run): the presence grid is
inert BY CONSTRUCTION — an absence-penalized mean rank ≤ K/2 with the
50,001 penalty mechanically forces ≳87% train presence; the only live
robustness axis is the rank cut. Empirical rates differ from §2z-ab
(0.0080 vs 0.0118 at K/2) because the cohort is now the full universe
population, not the 4,000-slot tracked subsample — this construction
supersedes.

**Status:** the established-departure finding is, after three consecutive
adversarial rounds (slot→identity, §2z-ab; selection→train-only and
asymmetry→symmetric, this section), as hardened as dev-panel measurement
can make it. Candidates remain DEFERRED; the knot-aligned exit-estimand
audit is the next measurement; the three candidate branches (displacement
tails / exit-estimand correction / fast-amplitude allocation) stay
separate per the branching rule; the binding claim set is untouched.

## 2z-ad. 2026-07-16 — NEW DATA REGISTERED, NOT YET ANALYZED: Reddit submissions long panels (2018-12..2022-12, owner-built, validator NOT yet run) + Instagram 2023 post-level dailies. **PREREGISTRATION COMMITTED FIRST — read `llm_fitting/PREREG_2026-07-16_submissions_ig.md` BEFORE any contact with these files** (binding for every agent: Claude, Codex, Gemini)

Owner added to T9: (1) 49/49 submissions monthly aggregates
(RS 2018-12..2022-12, all ok/errors=0 per the processing log) and combined
panels `reddit_daily_long.parquet` (142.4M rows) /
`reddit_weekly_long.parquet` (53.7M rows; 212/214 complete weeks;
metric_value = max(submission_karma, 0) daily — the submissions mirror of
the comments convention, established from builder CODE);
(2) `raw_small/instagram/full_ig.parquet` — 53.5M POST-LEVEL rows,
calendar 2023, the daily complement of the existing weekly IG year
(enables IG Spec-B for the first time). Also present: a new `wikipedia/`
T9 tree (owner acquisition started 2026-07-14; contains a suspicious
`2099-01` raw month directory — flagged, not investigated).

**Intake state:** aggregation logs clean; the finalizer's own validation
step did NOT complete (`reddit_full_validation.json` is 0 bytes) — the
output validator is Phase 0 of the registered plan and MUST print PASS
before any model contact. **No value-level contact has occurred**: only
listings, logs, schemas/row counts, parquet metadata statistics, and
builder source were read (exact disclosure in the prereg §0).

**The prereg registers 11 scored predictions** — headline items: P4, the
s-secular-trend replication on a second metric (monotone rise 2019→2022);
P7, the §2z-ac established-departure deficit replicated on outcome-unseen
data (≥1.5× with ≥90%-crossing composition); P10, IG Spec-B (thinning
law, floor orientation, §2z-c σ_obs inside the identified envelope,
pinned gate at-or-above baseline). Binding rules restated there:
submissions = outcome-unseen BACKTEST (never "new-platform
confirmation"); IG never calibrated to; trend test before any transport
band; one pass per item; failures are findings. Analysis is PLANNED, not
executed; amendments legal only before Phase 0.

_Addendum same-day:_ **AMENDMENT 1 registered** (external review of the
prereg, all findings code-verified: the named Phase-0 validator loads both
full panels into pandas and exits 0 regardless of failed invariants; gate
CLI defaults reps=3/boot=400; the IG gate without `--member-ids-file`
reproduces §2z-f leakage; P3 carried a fuller-universe escape route).
A1.1–A1.11 freeze: a fail-closed streaming intake gate
(`check_long_panels.py`, to be committed + adversarially tested before
Phase 0); exact P4 windows [0,71)/[71,142)/[142,212) + the era-vs-
composition four-cell guard as P4b; b(4) AND b(8) bands for P5; the P6
command verbatim with reps=20/boot=2000 AND the ≥60% in-CI coverage
clause; P7 primary arm = the §2z-ac structural-residual design (30 seeds,
T0 = week of 2021-07-05, transport arm descriptive); P8/P9 algebra; the
P10 daily↔weekly RECONCILIATION GATE (mismatch > 0.1% STOPS P10) + 1/M
slope ∈ [0.5, 1.5] + envelope ≥ 10/12 bands + the pinned gate command
with the member-file hash 130726eb…; P11 clustering algebra; K = smallest
grid value reaching 90% share, NO post-observation override; every hard
test requires a dry-tested runner before Phase 0.

## 3. The three corrected estimation pitfalls (do not regress)

1. **Band-alignment bug (fixed, committed):** `mean_rank` is sorted but entity columns were not —
   rank-band masks selected the wrong entities, flattening all rank curves. Fixed via `mean_rank_ids`;
   locked by `tests/test_rankdiff_regressions.py`. (FB 15/15→14/15 on the v4.3 model after the fix —
   the drop was real, the bug had hidden it.)
2. **Current-rank (Eulerian) estimation is selection-biased:** conditioning on current rank
   oversamples transient spikers → inflates σ ~3× → runaway diffusion. **Always estimate by
   permanent (time-averaged) rank.**
3. **Observed-week mean rank re-admits the same bias at the universe-membership stage** (2026-07-02):
   compute permanent rank over ALL window periods with absent weeks at the observation floor
   (N_t+1). Locked by `tests/test_universe_restriction.py` (ghost-spiker exclusion).

## 4. Known limitations / open items
- **σ_obs identification** is THE crux. Calibrated for now; identify organically next (daily-within-week
  variance — but note the daily model needs heavy DoW/ToD damping; or replicate measures once Reddit
  comment data is merged — current `metric_value` = submission_karma only).
  _2026-07-02:_ Reddit's train calibration now selects scale **0.0** on every split and STILL
  over-predicts held-out displacement — the excess head dispersion is in the variance partition,
  not just the noise split. Spec B plus robust head bands (pool 1–2-entity top knots) is the path.
- **Reddit** OOS movement not fully passing (short panel: train windows are only 12–17 weeks).
  Now RUNNABLE and in FB's regime under the top-coverage universe (§2b). Needs the longer panel.
- **In-sample RACF vs OOS displacement tension:** more σ_obs helps in-sample rank-autocorrelation,
  less σ_obs helps OOS displacement. Real fit tension; report both.
- **Instagram = negative control, do NOT calibrate to it** ("a"-query censoring flattens its
  distribution → pathological rank displacement, R² collapse).
  _[PARTIALLY SUPERSEDED 2026-07-06 by §2z-c: with the censoring process modeled (3-layer:
  thinning a/M, week-correlated instrument dropout, low-q ghost heads) and a measurability-scoped
  K=10k universe, the UNCHANGED pipeline gives 9/15 + an at-par OOS gate on `instagram_hm`;
  estimand scoped "of a-matching activity". Uncorrected IG panels remain a negative control;
  the never-calibrate rule stands.]_

## 5. Reproduction
```
python llm_fitting/minimal_rankdiff.py facebook reddit instagram   # prototype scorecard (knobs)
python llm_fitting/rankdiff_kalman.py facebook reddit               # drift analysis (LR, OOS CRPS, propagator)
python llm_fitting/rankdiff_kalman.py facebook reddit --scorecard   # wire drift params into generative score
python llm_fitting/rankdiff_kalman.py facebook reddit --oos         # OOS movement gate (calibrated σ_obs)
python llm_fitting/rankdiff_kalman.py --selftest                    # Kalman recovers synthetic truth
```
Data: FB `data/raw/fb_ranked_weekly_cutdown.parquet`; Reddit `data/reddit/reddit_weekly.parquet`;
IG `llm_fitting/ig_weekly_ranked_top50k.parquet` (use top-20k; negative control only).

## 6. Framing for the paper
> Digital-attention rankings show **Eulerian stability with Lagrangian churn**: the rank-size curve and
> the per-rank share are stationary while identities churn through fixed ranks. A successful model must
> reproduce (i) the stationary ladder, (ii) fixed-rank occupant turnover, and (iii) **held-out** individual
> displacement. We combine a rank-based diffusion with Gabaix rebirth for the ladder and a state-space
> observation model (permanent + transitory + measurement) for the dynamics, unified across platforms
> with parameters that differ by regime, and we validate movement out-of-sample.

_Addendum 2026-07-03 (post 2i–2l; venue analysis in research_notes §6c):_ the
sharper spine, if the b = 1 restriction holds, is a **Q-model-like law for
attention**: every endpoint follows the same rank-conditional stochastic process up
to one entity-specific amplitude (lognormal, spread s), with measurement noise
identified from an independent daily-replication instrument and movement validated
out of sample against persistence — across two platforms, three metrics, and a
segmented collection instrument. Differentiators vs the ranking-dynamics
literature (Iñiguez et al. 2022; Blumm et al. 2012): identification of the
measurement model, out-of-sample distributional gates, and instrument forensics.
Main gap to close for a PNAS-class submission: SYSTEM BREADTH (add 2–4 ranked
systems through the unchanged pipeline) and data-collapse figures; not more
dynamics.

_Addendum 2026-07-05 (post §2r–§2x; binding claim set per the revised external
verdict, saved in reviews/):_ **The paper's spine:** digital attention rankings
obey an approximately factorized rank-diffusion law — a shared rank-conditional
stochastic process with independently bounded measurement noise and boundary
rebirth, multiplied to first order by one persistent endpoint amplitude —
which predicts held-out rank movement and reproduces the main churn structure
across Facebook tracked activity and Reddit census attention, with residual
deviations localized to low-frequency structure and bounded reversion
heterogeneity. **Main model: b = 1** (measured b = refinement table; block vs
entity bootstrap in SI). **Claim discipline:** "to first order, endpoint
heterogeneity collapses to one amplitude; residual reversion-rate heterogeneity
is measurable but second-order under the validation gates" — never "every
endpoint follows the same process up to one amplitude." Structure stack vs
movement stack is a MAIN-TEXT design distinction with a target column. FB
movement leads with the identified Spec-B + conditional spec (0.118, 4/5, no
calibration freedom); the calibrated 5/5 number is the parenthetical/SI
reference. "15/15" is a descriptive scorecard that survives 20 reps; Q rejects
exact equality and is used to LOCALIZE residuals (per-block Q/df table), never
as a pass/fail gate. σ_obs: identified in shape everywhere, identified in level
at the FB head, bounded in level elsewhere. Lifecycle language: "excess
low-frequency structure"; arcs are one plausible source, not uniquely
established (spectrum-preserving surrogates, §2v).

_Addendum 2026-07-12 (post confirmation battery §2z-q–§2z-u; OWNER-ADOPTED —
the single binding copy; supersedes the 2026-07-05 addendum's claim set where
they conflict):_ **The confirmation claim paragraph (verbatim, binding):**
"A parsimonious permanent–transitory rank model trained through June 2021
transported the registered central conditional-movement moments into a later
era without recalibration: relative moment error was 0.032 versus 0.171 for
historical mobility, with median displacement reproduced at h=1, 4, and 13.
Proper-score performance remained approximately at par with persistence,
however, and upper movement tails were under-dispersed. Parameter transport
was mixed: amplitude heterogeneity s failed its registered band and
post-confirmation analyses showed an era-dominant increase already visible
in the frozen period; κ orientation also failed and κ levels shifted, while
the horizon-scaling exponent and observation-noise shape transported. The
model generated an excessively concentrated stationary head, now confirmed
on a third instrument. An exploratory oracle analysis was consistent with a
parameter-vintage explanation for tail calibration, but did not isolate its
cause. The first candidate stationary-law correction — a within-entity
level-variance anchor — was rejected by its own adoption gates."
**Framing rules:** the battery is "a registered confirmatory evaluation with
one disclosed post-registration, pre-outcome technical correction" (A10),
never "exactly as originally preregistered" and never "a literally untouched
holdout"; the A6.7 verdict (MIXED EVIDENCE) is stated plainly; the s trend
(0.65→0.75→0.84, 2019→2022) is a titled finding with its own figure; four
evidence tiers are labeled throughout (registered confirmation / delayed
registered sensitivity / exploratory diagnostics / development negative
result); the survivor-conditional scope line on the movement claim names the
boundary-flux residual; S(k) stays "within recorded top-M".

## 2z-ae. 2026-07-16/17 — THE SUBMISSIONS BACKTEST EXECUTED (PREREG_2026-07-16, one pass, outcome-unseen cross-metric replication per §2z-w — NOT confirmation; Wikipedia remains the primary target): MIXED, but the headline hardened finding TRANSPORTS — the §2z-ac established-departure deficit REPLICATES on a second, outcome-unseen metric (P7 sim/emp ratio 2.74 at K/2, squarely in the pre-registered 2–3× point range) and amplitude collapse (b≈1) + era-dominance carry over; the monotone s-trend (P4a) and the functional VR share (P9b) do NOT replicate; IG P10/P11 are NOT ADJUDICATED (build fail-closed on the wrong observational unit + an infeasible registered reconciliation target)

Panel: `reddit_submissions_long` (2018-12..2022-12, 142,438,022 daily /
53,723,472 weekly rows, metric_value = max(submission_karma,0) daily). Frame:
this run is a BACKTEST / cross-metric replication (§2z-w), scored one pass per
registered item, failures reported as findings, no refits. Run archive:
`llm_fitting/runs/2026-07-16_submissions_ig/` (per-item logs, input SHAs in
`PHASE0_MANIFEST.csv`).

**Two disclosed execution corrections (neither changes any threshold, universe
rule, or expectation):**
1. The A1.1 intake gate (`check_long_panels.py`) was rewritten to bounded
   one-week streaming after two terminations with no verdict — the original
   materialised the full 53.7M-row entity-week aggregate (violating its own
   registered "never loads the full panel" description). Byte-identical
   invariants; all 7 adversarial fixtures + 3 new scale/boundary tests green;
   full suite 177+13. See `PHASE0_EXECUTION_CORRECTION.md`.
2. The heavy movement evaluations were repeatedly SIGKILL-class terminated as
   agent-managed background tasks (no crash, no jetsam; a 150+-process Codex
   session ran concurrently). P6–P9 were run to completion (exit 0) from a
   user-owned Terminal via `run_submissions_p6_p9.sh`. Results unaffected.

**Registered scorecard (equal prominence, pass or fail):**
- **P0/P1 intake gate — PASS.** 142,438,022 daily reconcile to 53,723,472
  weekly EXACTLY (both directions, every column, all 212 consecutive complete
  weeks); day-guard 0 flags; full calendar coverage 2018-12-01..2022-12-31;
  49/49 RS months log-clean; 380,306 boundary cells excluded.
- **P2 census — PASS.** entities/week 126k–357k (median 257k); new-ids/week
  ~55k, CV 0.206, largest wk/wk ratio 1.39 (smooth organic inflow, no frozen-
  panel collapse); top ladder plausible (memes, dankmemes, aww, politics,
  WhitePeopleTwitter…).
- **P3 universe — K = 5,000** (smallest grid value with mean weekly share
  ≥ 0.90: K=2,500→0.856, K=5,000→0.918; frozen, no override, A1.10).
- **P4a s-trend (HEADLINE, hard) — FAIL.** s = 1.113 → 1.082 → 1.130 across
  windows [0,71)/[71,142)/[142,212): NON-monotone (W2 dips below W1), MBB CIs
  overlapping. The predicted monotone 2019→2022 rise does not replicate on
  submissions.
- **P4b era-vs-composition (hard) — PASS.** four-cell A=1.113 B=1.130 C=1.088
  D=1.620; era effect +0.274 dominates composition −0.257 (era-dominant,
  consistent with §2z-s) — the trend is era-driven but not monotone.
- **P5 amplitude collapse (hard) — PASS.** s(1)=1.119; b(4)=0.977,
  b(8)=0.965, both inside [0.95,1.15]. The b≈1 law transports to a second
  metric.
- **P6 movement gate (hard) — PASS.** model rel err 0.194 ± 0.061 ≤ baseline
  0.163 + 0.05 = 0.213; dRank1 median-in-CI on 80% of splits (≥60%). NUANCE:
  the model is AT-PAR-within-0.05, it does NOT beat persistence (0.194 vs
  0.163) and CRPS skill vs persistence ≈ +0.000 — no distributional edge on
  this metric (cf. old subs gate 0.164 vs 0.168).
- **P7 established-departure deficit (HEADLINE hardened finding, hard) —
  PASS.** anchor 2021-07-05 → derived t0=135. K/2: empirical 0.00851
  [entity-CI 0.00728,0.00982; week-block 0.00746,0.00968] vs sim 0.00311 [q10
  0.00214,q90 0.00408] → sim below the empirical entity-CI lower bound, ratio
  2.74 (inside the pre-registered 2–3× point expectation); composition PASS
  (empirical departures ≥90% returnable crossings). K/4: 0.00302 vs 0.00092
  (~3.3×). **The §2z-ac deficit — the current research frontier — replicates
  on outcome-unseen, cross-metric data.** This is the run's strongest result.
- **P8 head law (non-gating directional) — NOT fired.** S(1) emp 0.0724, sim
  0.0541 ± 0.0111 → excess −0.0183 (sim UNDER-produces the head share, the
  OPPOSITE sign to the FB overshoot), vs 2·SD 0.0222. The FB head-law
  overshoot does not appear on submissions — consistent with it being an
  FB-measurement-regime artifact, not a universal model failure.
- **P9a VR13 excess (hard) — PASS.** VR13 emp 0.2031, sim 0.2500, excess
  +0.0469 > 0.
- **P9b functional share (hard) — FAIL.** F = 0.260, outside the registered
  [0.5,0.75] (comments measured F ≈ 0.77). On submissions most of the VR
  excess is card/compositional, not functional — the functional-vs-card
  mismatch is much smaller here; reported as a finding about the prediction.
- **P10 Spec-B for IG / P11 instrument dropout — NOT ADJUDICATED** (not
  scientific failures). The IG daily build failed FAIL-CLOSED: 659,028 urls
  (1.27%; full-file Codex census — the builder's 41,256 was one of 16 hash
  partitions) carry conflicting identity, essentially all username-only,
  same url/date/title, near-identical interactions — one content item
  attributed to 2–4 accounts (Instagram Collab structure). Two issues:
  (a) the registered global-`url` dedup encodes the WRONG observational unit
  (it conflates repeated extraction snapshots with multi-endpoint
  attributions; the endpoint unit is `(user_name,url,date)`); (b) the A2.1
  EXACT-reconciliation target (`ig_hm_totals_ts.parquet`) was itself built by
  no-dedup raw aggregation, so a correct dedup'd build could never match it —
  the gate was infeasible regardless. A corrected build (endpoint key,
  collapse snapshots keep-max, preserve same-url/different-user, never merge
  on display `account`, preserve zero-interaction presence, quarantine the
  lone date anomaly) is chosen from data semantics — never from whether it
  improves P10/P11 — is EXPLORATORY/sensitivity for 2023, and needs fresh
  preregistration on an untouched period (2024) for confirmatory status.
  OWNER decision; not executed. See `PHASE1_IG_BUILD_FINDING.md`; Codex audit
  artifacts on T9 `raw_small/instagram/_conflict_audit_20260716/`.

**Reading.** The generative core transports to an independent metric where it
counts most: the hardened established-departure deficit (P7, the frontier
finding), the b≈1 amplitude law (P5), era-dominant movement (P4b), and the
at-par movement gate (P6) all hold. The narrower shape predictions do not —
the s-trend is era-driven but non-monotone (P4a), and the VR excess is
card-dominated not functional (P9b). The FB head-law overshoot is absent
(P8), reinforcing it as FB-measurement-specific. Nothing here is confirmation
(Wikipedia remains that); it is consistent cross-metric replication of the
mechanism with two clean prediction misses recorded as findings. The IG 2023
capability is deferred pending an owner decision on the corrected endpoint-
level construction.

## 2z-af. 2026-10-08 — Project re-entry and literature-based research agenda: PLANNING ONLY; no model change adopted

**Diagnosis.** Owner requested an assessment against joint structural stability,
individual movement prediction, and simplicity, plus extensive methodological
research and a next-step plan. Read the canonical skills, this record, the
confirmation protocol, relevant implementation paths, the July 18 IG design,
and the October 7 reply-tree/x-collection review. The latest substantive
measurements remain §§2z-q/ac/ae; this session produced no new empirical fit,
scorecard, OOS result, or data-intake result.

**Evidence synthesis (historical results, not reruns).** The architecture has
substantial pooled-mobility and conditional ladder-maintenance support. It
does not yet establish endpoint-specific probabilistic forecasting skill or
minimal effective complexity. The §2z-q confirmation verdict remains MIXED
EVIDENCE: E2 passed on the registered single future block, E1 parameter
transport failed. The long submissions P6 passed within tolerance while its
point error was worse than the historical-mobility baseline. The established
departure deficit in §§2z-ac/ae remains the strongest replicated dynamical
target. Simulated medians lying in bootstrap CIs are not predictive-interval
coverage, and the current pooled proper scores do not establish individual
conditional skill.

**Interpretation clarification, superseding only the stronger causal reading
at the end of §2z-ae.** Head-share excess was recorded on FB AND on the
comments extension (§2z-q E5); its absence/opposite point-estimate sign on
submissions demonstrates heterogeneity, not identification of a uniquely
Facebook measurement cause. All historical measurements and verdicts remain
unchanged. Likewise, a returnable rank crossing is not necessarily an
observed return, and finite nonreturn alone does not identify permanent death.

**Change.** Added two planning documents:

- [RESEARCH_AGENDA_2026-10-08.md](RESEARCH_AGENDA_2026-10-08.md): assessment,
  evidence table, ordered experiments, decision rules, validation design,
  and analytical checks.
- [LITERATURE_REVIEW_2026-10-08.md](LITERATURE_REVIEW_2026-10-08.md): 31
  annotated primary research/methods sources, search scope, access limits,
  and explicit distinctions between source findings and proposed applications.

The proposed order is own-model recovery and forecast-state coherence;
knot-aligned identity/exit/return audit; refitted compression and ablations;
a bounded comparison of mechanisms and baselines; explicit structural
stationarity tests; then reserved distinct-platform transport. Earnings-panel
identification, state-space recovery, capture–recapture observation distinctions,
and proper predictive scoring are the main additions to the existing rank-law
inspirations. The x-collection review supplies selection/ablation discipline,
not its branching-process architecture.

**Pre-registered predictions scored:** NONE. Proposed recovery and forecasting
comparisons are unexecuted; numerical tolerances and any new acceptance rule
must be specified before those experiments. The agenda is not a frozen
protocol, owner adoption, or permission to change defaults. The current
movement gate remains the adoption criterion. Extra endpoint scores are
proposed diagnostics and claim checks until separately registered otherwise.

**Validation for this documentation session:**

| check | result |
|---|---|
| Research regression suite, registered Python 3.11, repo root | 177 passed, 13 subtests passed; one existing fixture dtype FutureWarning |
| Python package suite, same interpreter/root | 8 passed |
| Legacy empirical guard | Not rerun; no simulator/default changes made in this session |
| New empirical evaluation / new-data analysis | Not run |
| Frozen protocol, estimator, model defaults, packages | Unchanged by this session |

**Verdict: PLANNING ONLY.** No new mechanism adopted and no scientific pass
claimed from software tests. Existing unrelated staged/unstaged work was
preserved; the new documents and this appended entry are left for review in
the mixed working tree. No active-spec or skill table was changed, because
the agenda remains a proposal rather than an adopted re-freeze.

**Reproduction / source trail.** The two documents identify the status
sections, implementation functions, external papers, and cross-project review
files used. Software checks executed from repo root:

```sh
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m pytest tests/ -q
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -m pytest Python/rankdiff/tests -q
```


## 2z-ag. 2026-10-08 — WD/T9 preservation audit: transfer IN PROGRESS; no scientific evaluation or model change

**Question.** Owner requested a careful project-record review and preservation
on the SSD of useful data still confined to the spinning WD drive. The data
intake, stochastic-modeling, record-authoring and maintenance contracts were
read, together with the relevant frozen protocols and newer IG/research plans.

**Measured storage state.** Both drives are mounted and `data/ssd` resolves to
T9. A non-hidden-file inventory found 4,131 WD files / 4,166,925,464,679 bytes
and 3,173 SSD files / 312,263,013,442 bytes, with no scan errors. The 98 Reddit
monthly aggregates (49 comments, 49 submissions, December 2018–December 2022)
are present; latest processing records are all `ok`, errors zero. Both
registered comments weekly/boundary files independently match the hashes in
`runs/2026-07-12_confirmation/restart/MANIFEST.sha256`; these two files were
missing from the main SSD manifest, not from the disk.

**Preservation decision.** The owner's broader preservation request now includes
remaining CrowdTangle CSV/TSV/consolidated/helper/backup exports whose overlap
has not been proven, plus the one unpaired backfill TSV and debug log. The
frozen plan copies 842 files / 199,032,451,889 bytes and then verifies previously
manifested SSD files. Existing raw-copy sources account for 1,178 files /
242,076,327,863 bytes. The remainder stays on WD under the established archive
policy (Reddit raw/fallbacks, paired backfill TSVs, images, installers). Raw
Reddit still matters for future fields absent from aggregates; WD is not
made disposable. Projected storage stays below 600 GB with over 40% free.

**Contact and scope.** New exports are copied as opaque bytes, not parsed or
scored. Their export dates are not asserted to be observation dates. Preserved
`exclude/bad_data` files retain that source label and are not validated inputs.
No new scientific analysis, aggregation, universe selection, allocation, or
model evaluation occurred. IG P10/P11 remain NOT ADJUDICATED, and the July 18
allocation plan remains unimplemented. Wikipedia's January 2025 raw/hourly,
tail-sample and scratch files were inventoried only; no final daily/weekly
panel is present. Its `2099-01` directories contain no ordinary files; the
existing tail samples still require a separate exposure audit before an
untouched-data claim.

**Execution.** Added an additive preservation runner with source-stream SHA-256,
flushed staging writes, independent destination rereads, refusal to overwrite
conflicts, per-file manifest updates and an append-only verification log.
Original files remain intact. Audit artifacts and the pre-run manifest are
under T9 `manifest/drive_audit_20261008/`; new raw copies preserve their WD
paths under `raw_small/wd_preserved_20261008/`. macOS identifies the current
T9 USB link as 480 Mb/s (WD: 5 Gb/s), explaining the initial 10–15 MB/s
verified-copy throughput. A safe stop for a cable change has been offered.

**Validation and verdict.** Pre-transfer research + package suites: 185 passed,
13 subtests passed, one existing fixture dtype warning. Synthetic preservation
checks passed: exact byte copy, source retained, hash recording, resume, and
conflicting destination refusal. Empirical legacy guard not rerun (no model
changes). No scientific predictions were tested; no OOS or adoption verdict
is claimed. Defaults, protocols, thresholds and historical results unchanged.
**TRANSFER IN PROGRESS, NOT COMPLETE** at this entry; later completion or
failure must be reported explicitly. Existing unrelated working-tree changes
were preserved and no commit was made.

**Reproduction / operational detail.** See the dated preservation section in
`DATA_PHASE2_REPORT.md`, including all dispositions, storage budget, transfer
log and invocation:

```sh
/usr/bin/caffeinate -dims /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u scripts/data_wrangling/preserve_wd_sources_20261008.py /private/tmp/rankdiff-drive-audit-20261008 --execute
```


**§2z-ag operational addendum, 2026-10-08 — cable disconnect recovered;
transfer still IN PROGRESS.** Owner reseated the partly disconnected T9 cord.
The initial worker stopped on a staging-write I/O error; 43 verified records
survived remount. The interrupted staging file was preserved separately,
completed files were rehashed, and the copy resumed successfully with
`--recheck-completed`. T9 now negotiates 10 Gb/s rather than 480 Mb/s; initial
resumed throughput: 6.31 GB in about 1.6 minutes. No source/canonical-file
overwrite, new-data analysis, protocol change or completion claim. Exact
recovery details and current command/log are appended to DATA_PHASE2_REPORT.


**§2z-ag completion addendum, 2026-10-08 — PRESERVATION COMPLETE; supersedes
this section's IN PROGRESS status.** The resumed worker completed at 18:49:43
Japan time (09:49:43 UTC), exit 0. All 842 new files (199,032,451,889 bytes)
passed source-stream/destination-reread SHA-256 verification. All 1,293 original
main-manifest files (255,340,648,275 bytes) independently matched their recorded
hashes; no post-restart checksum failures. Final audit: 2,135 manifest entries
agree with completed verification records, 2,020 selected WD sources retain
their inventoried existence/size/mtime, and the two registered comments inputs
match the confirmation archive separately. T9 has 487.86 GB free (48.78%);
both storage limits pass. The single interrupted 250.5 MB staging artifact is
preserved separately and is not a scientific input. No source deletion,
scientific evaluation, default change or protocol amendment. Raw Reddit and
other documented exclusions remain WD-only; the drive is not disposable.
Exact scope, evidence, failures and reproduction remain in the dated completion
section of DATA_PHASE2_REPORT and T9 `manifest/drive_audit_20261008/`.

Final post-recovery regression check: 185 tests and 13 subtests passed in
72.93 seconds; the same preexisting fixture dtype warning remains. No commit
was made; unrelated working-tree changes were preserved.


## 2z-ah. 2026-10-08 — Missing Reddit bridge located; raw acquisition IN PROGRESS, no analytical contact

Owner requested January 2023–June 2024 comments/submissions be found and
downloaded onto WD. The maintainers' combined Pushshift/Arctic Shift release
was located on Academic Torrents (infohash
`ba051999301b109eab37d16f027b3f49ade2de13`), with 36 selected month/type files
and exact lengths from the torrent metadata. Comments: 542,245,694,286 bytes;
submissions: 276,592,887,959 bytes; combined: 818,838,582,245 bytes. Initial WD
free space: 807,083,687,936 bytes, insufficient for the full request.

Comments-only acquisition started in the new WD directory
`pushshift/bridge_2023-01_2024-06/`, with checksummed, resumable torrent
transfer; approximately 1.8 GiB of payload was observed after startup.
Submissions await the owner's storage choice. A specific 88.32 GB folder of
WD Facebook CSV copies already verified on T9 was proposed for removal;
**no deletion authorized or performed at this entry**. Existing raw sources
are untouched. The download request authorizes adding files to WD, not
changing frozen scientific specifications or retrospectively classifying the
bridge as confirmation evidence.

Source provenance is material: Pushshift through March 2023, Arctic Shift
from April 2023, and a documented 36-hour second retrieval/score update from
November 2023. Raw preservation is not scientific continuity. Source and
score-timing forensics must precede use in a karma panel. Only web/source
metadata, archive listings/lengths, and opaque transfer bytes were accessed;
no rows were interpreted, no aggregation/fitting/evaluation occurred, and no
predictions or adoption gate were scored. Defaults/protocols unchanged.

The former PullPush HTTP webseed returned 403; the maintained torrent is
receiving data. Its initial absent-file integrity warning and missing fresh
DHT-state file are recorded in the operational report, not suppressed or
misreported as verified downloads. Status: **IN PROGRESS; not acquired in
full and not validated for modeling**. DATA_PHASE2_REPORT records the exact
source URLs, torrent SHA-256, selection indices, configuration, command,
provenance cautions and storage decision. Acquisition manifests/logs and
upstream documentation snapshots are saved beside the WD download. No commit
or code changes in this acquisition step; earlier 185-test/13-subtest green
regression result remains the last software check, not a data-validation result.

**§2z-ah storage addendum, 2026-10-08 — owner requested compression first.**
The pending storage choice above is superseded by authorized lossless
compression of WD `reddit-uncompressed/RS_2022-12.json` (139,650,513,046 bytes).
The torrent paused cleanly at 21:18:19 Japan time with resumable state saved.
Compression is IN PROGRESS; no saving is yet claimed. Full decoded SHA-256
and byte-count agreement and unchanged source metadata are required before
replacing this single original with its `.json.zst` representation. No Facebook
CSV removal is authorized/performed. Pre-change checks: 185 tests/13 subtests
passed; four compression safety checks passed. This is opaque byte storage
maintenance only, with no new analytical contact or scientific changes.
DATA_PHASE2_REPORT and the beside-source compression log retain the procedure.

**§2z-ah compression completion addendum, 2026-10-08, 22:11:02 Japan time.**
Compression and independent full decode verification passed, exit 0:
139,650,513,046 bytes became 15,147,456,307 bytes, saving 124,503,056,739 bytes.
Decoded count and SHA-256 equal the original source stream; source metadata
remained unchanged throughout. The verified `.json.zst` and audit sidecar
replace the single uncompressed JSON, preserving every decoded byte. No
Facebook CSV removal. Exact checksum, paths and command are in
DATA_PHASE2_REPORT, with evidence on WD and a copied verification record in
T9's preservation audit. This supersedes IN PROGRESS for compression only.

Post-compression WD free space was 713,161,048,064 bytes. Accounting for
218,347,704,451 bytes already allocated to selected files and conservatively
allowing 45,075,407,792 additional boundary bytes leaves a projected
67,594,762,478 bytes free after the entire requested bridge. Both comments
and submissions (36 files, indices 206–223 and 441–458) were therefore
resumed with persistent state and integrity checking. Acquisition remains
IN PROGRESS; no completed-download or scientific-continuity claim. Final
software checks: 189 tests and 13 subtests passed; the preexisting dtype
warning remains. No rows interpreted, no panels changed, no fitting or
evaluation, no frozen-default/protocol changes, and no commit.


## 2z-ai. 2026-10-08/09 — Second, independent research agenda with literature notes (PROPOSED, not adopted); the uncommitted July record, package-track Phase 1 and July scripts COMMITTED; the research suite found NOT pandas-3 compatible (CI pinned below 3). PLANNING + REPOSITORY RECORD ONLY — no model change, no fit, no new-data contact

**Question.** The owner's re-entry request recorded in §2z-af was also given
to a second model (Claude Opus 5.5) in a concurrent session. The owner then
directed that session to commit the outstanding July work, the package-track
batch, and the 2026-10-08 planning and preservation records.

**Planning output (nothing adopted).**
- `RESEARCH_AGENDA_CLAUDE_2026-10-08.md`, prepared independently of §2z-af's
  agenda; its Part F lists where the two agree and differ. Neither is
  adopted. §2x remains the binding agenda until the owner records otherwise.
- `lit_review/research_notes/Rank dynamics modeling best practices/`: six
  commissioned literature notes and the fact sheet used to brief them.
  Citations carry verification tags; several searches ran out of budget, so
  primary sources must be re-read before anything is cited in the paper. A
  consolidated report and a memo on the x-collection review are held out of
  this commit (public repository; owner's decision).

The agenda's proposals are hypotheses. Its central ones — that the slow layer
is too stationary, that the FB head-share overshoot follows from unbounded
log-scale t shocks, that the band profiles compress to a few scaling
exponents — are CONJECTURES with named discriminating measurements, none run.

**Clarifications of the record (no historical number changes).**
1. Departure events (§2z-ac, §2z-ae P7): the cohort is train-window
   absence-penalized permanent rank ≤ K/2 (or K/4); the event is an exit from
   the current top-K at t+1; a "crossing" is an entity still observed below
   K. No duration or return-time distribution for crossings is on record
   (agrees with §2z-af). The literature brief first written in this session
   said departures "come back"; a researcher caught it, and the fact sheet
   carries the dated correction.
2. FB Era A is 86 complete weeks (`instrument_eras.py`); 88 is the legacy
   cutdown panel.

**Code readings that bear on interpretation (verified in source 2026-10-08;
not new measurements).**
- `--dist-scores` scores the POOLED simulated displacement law against the
  pooled train law (declared in the `rankdiff_kalman.py` comment). Recorded
  "CRPS skill ≈ 0" values are therefore statements about pooled displacement
  distributions. No recorded evaluation scores endpoint-level predictive
  skill.
- v_i scales σ_perm only when `--mix-hetero` is on, in `simulate` and in
  both cohort simulators; the frozen FB and submissions movement stacks do
  not list that flag.
- Fast innovations are unit-variance Student-t with no bound (`_tdraw`),
  while S(k) is computed on `expm1(X)`. §2z-v's note that seed noise "dies"
  under Gaussian innovations is what an ill-conditioned exponential moment
  would produce — conjecture until the agenda's head-law two-by-two is run.

**Finding — the research suite is not pandas-3 compatible.** In a fresh
environment built from the CI workflow's dependency list (pandas 3.0.6,
numpy 2.4.6, pyarrow 25.0.1, pytest 9.1.1): 22 failed, 153 passed — 21 in
`tests/test_a6_runners.py`, 1 in `tests/test_prereg_runners.py`; first error
`TypeError: Cannot interpret '<StringDtype(na_value=nan)>' as a data type`
at `check_extension_panel.py:108`. The same environment with pandas 2.3.3:
175 passed + 13 subtests. The package suite passes on both (8). No recorded
result is implicated: the failures are exceptions under pandas 3, and the
registered interpreter has pandas 2.2.2 / numpy 2.0.1. Consequences:
`.github/workflows/ci.yml` pins `pandas<3` (bdd1042); the analysis
interpreter must stay on pandas 2.x; the fix touches registered intake
tooling and is OWNER-GATED (flagged, not done).

**Change.** No model, estimator, default, threshold or protocol change.
Repository record only, all owner-directed, on main:
- July record: f6d8c91 (bounded intake-gate rewrite + 3 tests), ce9d84f
  (§2z-ae, run archive, `subs_forensics.py`).
- Package track Phase 1 as executed 2026-07-07: 0383785 (Python), 25ff0a2
  (R; `rankdiffR/` → `archive/`), 0d709b8 (roadmap, repo-maintenance skill,
  CLAUDE/AGENTS pointers), bdd1042 (CI + ignore rules).
- July scripts and plan: 6560d4b (Reddit long-panel builder provenance and
  the DATA_PHASE2_REPORT section of 2026-07-16), b1f6ce3 (Wikipedia pilot
  tooling — no modeling contact; protocol still to be frozen), 3e55e22 (IG
  allocation plan, unimplemented).
- 2026-10-09: §2z-af with its two planning documents; §2z-ag with the
  preservation runner and report sections; both package LICENSE files now
  name both authors (owner instruction); this entry with the agenda and
  notes.

Dating notes were added to `PACKAGING_ROADMAP.md` and the IG plan at commit
time. §2z-ah (the Reddit bridge acquisition, IN PROGRESS in another session,
together with the compression work it records) precedes this entry in the
working copy and is committed when that acquisition completes — hence the
gap in lettering in the committed file.

**Pre-registered predictions scored:** NONE. The agenda lists directional
predictions for its proposed measurement round; they are unregistered until
a dated measurement plan exists.

**Validation.**

| check | result |
|---|---|
| Each of the nine 2026-10-08 commits, clean checkout of its own tree | research suite 175 passed + 13 subtests (177 once the Wikipedia pilot tests are in); package suite 8 passed from 0383785 on |
| 2026-10-09 commits (documents, one runner script, licence text) | both suites re-run on a clean checkout before commit: 177 + 13 and 8 |
| R package | `R CMD check --no-manual` Status OK; with `--as-cran`, NOTEs only; testthat 75 passed (re-run 2026-10-09 with the two-holder LICENSE) |
| Python package | wheel builds and contains the toy parquet and LICENSE |
| CI workflow | not yet run on GitHub when this was written; its Python steps reproduced locally in a fresh environment (green with the pin); R job arguments run locally |
| Legacy guard (`minimal_rankdiff.py facebook`, defaults) | 14/15, churn 0.013 (2026-10-08, twice) |
| New empirical evaluation / new-data contact | none |

**Verdict: PLANNING + REPOSITORY RECORD ONLY.** Nothing adopted; frozen
specs, protocols and the binding claim set are unchanged.

**Reproduction.**

```sh
python -m pytest tests/ -q
python -m pytest Python/rankdiff/tests -q
# pandas-3 failure, in a scratch venv outside the repo:
python -m venv /tmp/v && /tmp/v/bin/python -m pip install numpy pandas scipy matplotlib pyarrow zstandard duckdb certifi psutil pytest
/tmp/v/bin/python -m pytest tests/ -q    # 22 failures on pandas 3.0.6; green after: pip install "pandas<3"
```

## 2z-aj. 2026-10-11 — Reddit bridge restarted after owner stop; acquisition IN PROGRESS, no analytical contact

The owner requested a status update, restart and completion estimate for
§2z-ah's 36-file January 2023–June 2024 raw bridge. Both drives were mounted;
no downloader was active. The October 9 log ended at 09:41:52 Japan time
with an emergency shutdown, reporting 307/762 GiB (40%). About 489 GB of the
818,838,582,245-byte selected corpus remains, subject to rechecking saved
pieces. File allocation is not downloaded content.

The torrent hash and saved state were checked; state backups and a fresh
allocation-aware budget were saved under the existing acquisition directory.
WD had 146,578,333,696 bytes free; accounting for remaining selected-file
allocation and conservative boundary overhead leaves a projected
67,837,713,976 bytes free at completion. The exact same full selection,
resume configuration and integrity check were restarted, with a new stdout
log and `caffeinate` attached. No original sources were removed.

Prior-run sustained rates were 9.18/11.58/10.82 MB/s over its final
1/3/6-hour windows, or 7.70 MB/s over the entire session including startup.
The owner-facing estimate is provisional: 14–22 hours including startup
checks, late October 11 to the morning of October 12 Japan time, assuming
similar connectivity and uninterrupted operation. It is not a new-session
speed measurement or a completion claim. Exact logs, budget and commands
are in DATA_PHASE2_REPORT and WD `acquisition/restart_20261011*.json`.

Validation before restart: 189 tests and 13 subtests passed; existing fixture
dtype warning only. No model code, scientific defaults, protocol or panel
changes; no record parsing, aggregation, fitting, or evaluation. Predictions
and adoption gates: not applicable to opaque raw transfer. Source-transition
caveats in §2z-ah continue to bind. Verdict: **acquisition IN PROGRESS**.

Restart confirmation: launched at 09:29:47 Japan time; at approximately
09:32 the client and keep-awake process are active, saved state is being
persisted, and integrity checking is under way with 307/762 GiB reported.
Measured WD throughput during two five-second check samples was
104.96/109.97 MB/s (disk activity, not network speed). Post-restart software
check: 189 tests and 13 subtests passed, existing warning only. New-session
network speed remains unmeasured; the ETA above remains provisional.

**§2z-aj correction, 2026-10-11 approximately 15:28 Japan time — actual
transfer restored after owner challenge.** The morning restart did not
resume network transfer: `check-integrity=true` forced a startup disk scan
and held connections/download speed at zero for nearly six hours. The
earlier ETA understated this overhead and is superseded. A running process
and active disk reads were insufficient evidence of a resumed download.

The scan was stopped gracefully, saved control state was verified
byte-identical to the October 9 state backed up before the restart, and
`full_bridge.conf` was corrected to `check-integrity=false` for normal
saved-state resumption. Standard hash checks on incoming BitTorrent pieces
remain active (verified in aria2 1.37.0 source); the interrupted extra scan
is not claimed as completed validation. Configuration/control backups and
the diagnosis are retained in WD acquisition metadata.

Actual new network payload is now measured: exact client bytes rose from
330,319,167,488 to 332,729,827,328 by approximately 15:27:52, a
2,410,659,840-byte increase, with seven seed peers and recent rates about
13–20 MB/s. New log:
`acquisition/download-resume-fixed-20261011.stdout.log`. No errors reported.
DATA_PHASE2_REPORT records the correction, source evidence and resume
procedure. No model/code/default/protocol changes; no row parsing or
analysis. Verdict: acquisition IN PROGRESS, **actual transfer confirmed**.


## 2z-ak. 2026-10-11 — Skills REVISED for current frontier models (owner-directed): core skill cut from about 7,000 to about 3,400 tokens with purpose and decision rights first; references, one shared entry file, an exposure ledger and a gate ledger added; frozen stacks, Gate v1 and the registered protocols UNCHANGED. In the working tree, not committed — owner review requested

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-11; started
from commit aca4f69._

**Question.** The owner asked for a review of the four skills, which were
written in July so that weaker models could carry the program, against (a)
authoritative guidance on designing skills for the current models on both
sides (Claude Opus 5.5 / Fable 5.1; GPT-6 Astra) and (b) the modeling-practice
review behind §2z-ai, and for a revision that gets the best model on the
structure / movement / parsimony trade-off and has the program ready to
restart: development on data already seen, then evaluation on the Reddit
2023–24 archive and Wikipedia pageviews.

**What was consulted.** Three commissioned briefs in
`lit_review/research_notes/Skill design for agent-run research/` (Anthropic
guidance; OpenAI and cross-vendor guidance with the empirical studies; rigor
for agent-run science), the vendor skill-authoring guide, and the §2z-ai
notes. The briefs paraphrase their sources and tag what was read in full,
summarized by a fetch tool, abstract-only or recalled; nothing from them is
citable in the paper without a primary-source check.

**What the sources said that drove the design** (as reported in the briefs):
- Both vendors' current guidance says instruction files written for earlier
  models are over-prescriptive for current ones and should be audited, and
  that current models follow instructions more literally, so a blanket
  instruction to be conservative suppresses useful work.
- An invoked skill is not re-read, and after context compaction only its
  first 5,000 tokens or so are re-attached; the open skill specification
  recommends the same size. The July core skill was about 7,000 tokens with
  its pitfalls, state and handoff sections last.
- With both entry files present, Claude Code loads only `CLAUDE.md` and Codex
  only `AGENTS.md` and `.agents/skills/`, so the two agents had been starting
  from different digests.
- The empirical studies are few and mostly preprints: compact, curated
  skills helped; comprehensive or model-generated ones did not; stale
  guidance is the one kind with measured harm.
- Studies of agents doing analysis report characteristic failures:
  incomplete work reported as done, gaps filled with an expected value that
  was shown to the agent, specification search when a request is framed as
  reporting a range, and correlated errors across models. A named
  stop-and-flag outcome and giving reviewers logs and code were the cheapest
  measured remedies.

**The change.** No model code, default, stack, threshold or protocol was
touched.
- `stochastic-modeling/SKILL.md` rewritten: purpose, open and gated zones,
  session start, the model with its binding language, thirteen numbered
  principles each with its reason, the development loop, the confirmation
  loop, collaboration. The instruction that the reader should "almost never
  invent" is withdrawn.
- Three references split out, loaded by task: `frozen-specs.md` (stacks,
  reference values, flags, Gate v1 in full, recorded gate commands, known
  gaps between the stated model and the frozen simulators),
  `pitfalls.md` (the catalogue, extended with the July–October incidents),
  `evidence-standards.md` (what a number carries, specification discipline,
  identification and recovery, the gate's limits, supplementary scores and
  baselines, the candidate dossier, protocol contents, verification,
  provenance, paper checklist).
- `data-intake`, `model-status-authoring`, `repo-maintenance` rewritten to
  the same pattern and brought up to date.
- `AGENTS.md` is now the single always-loaded contract; `CLAUDE.md` imports
  it and adds three Claude-specific lines.
- New project files: `llm_fitting/PROGRAM_STATE.md` (one-page digest that
  replaces the dated state section of the old skill),
  `llm_fitting/EXPOSURE_LEDGER.md`, `llm_fitting/GATE_LEDGER.md`,
  `scripts/health_check.sh`, `tests/skill_scenarios/`.
- `tests/test_skills_sync.py` now compares every file in the two skill
  trees and checks that frontmatter uses only the two portable fields.

**What is new in the contract** (everything else is restated, not changed):
1. Data confirms once, tracked in the exposure ledger: DEVELOPMENT, CONSUMED,
   SEALED, EXPOSURE UNKNOWN. The ledger's initial statuses were compiled from
   this record and are the owner's to correct.
2. Every gate evaluation is logged in the gate ledger, used or not, because
   the gate's split-to-split SD is the size of the differences it judges.
3. A mechanism is read as real only after recovery and model-recovery
   evidence; ablations are refits.
4. Agreement between models is not evidence; verification is blind to the
   recorded value where independence matters; reviewers get code and logs.
5. Every reported number carries a status (RAN-OK, RAN-FAILED, PARTIAL, NOT
   RUN) and a trail; grids, seed counts and stopping rules are fixed before
   running; stopping to flag an impossible task is a successful outcome.
6. Each record entry carries a provenance line (model, vendor, date,
   starting commit).
7. A default comparison margin for candidate changes: the convention the
   registered evaluations used (0.05 in gate relative error, 20 reps,
   2,000 bootstrap draws) unless the owner sets another. **This is a
   threshold and needs the owner's confirmation.**
8. Proper scores at endpoint level and named baselines are described as
   supplementary reporting; they gate nothing unless a protocol registers
   them.

**Behavioral check (not a registered evaluation).** Fifteen owner-message
scenarios with 56 expectations; two fresh Claude sessions planning under the
July skills and two under the revision; one blind grader. Scores: July 52.5
and 51.0, revision 54.5 and 53.5, of 56. No plan under either version read
sealed data, changed a default or reported a single split. The one outright
failure was under the July skills (asked permission to start on open work).
The difference is within what two runs can resolve: this shows no
regression, not improvement. The baseline was contaminated (those agents
could see the new ledgers and skill descriptions), both arms had the July
`CLAUDE.md` injected by the harness, the answers are plans not behavior, and
Codex was not tested. The agents' reports of unclear or contradictory
instructions led to about twenty further edits the same day. Details:
`tests/skill_scenarios/results_2026-10-11.md`.

**Pre-registered predictions scored:** none.

**Validation.**

| check | result |
|---|---|
| `scripts/health_check.sh` | research suite 182 passed + 13 subtests (181 before the new frontmatter test); package suite 8 passed; legacy guard 14/15, churn 0.013 |
| Skill mirror | byte-identical across all seven files |
| Sizes (chars/4) | core 3,400 tokens (was 7,000); other skills 1,800–2,700; references 2,700–3,100; `AGENTS.md` 1,150 |
| Model code, defaults, stacks, protocols | untouched |
| Data contact | file listings; date range and row counts of two DEVELOPMENT panels |

**Verdict.** Skills revised in the working tree and mirrored, so both
vendors' agents now load the revision. NOT committed. The owner is asked to
review, in particular item 7 above and the initial ledger statuses.

**Not done.** Enforcement of the sealed-data rule by a hook or permission
rule (the sources say prose is advisory); a run of the scenarios under
Codex; the vendors' own prompt-audit tools; description-triggering tests; a
backfill of the gate ledger; a machine-written run ledger and claims
manifest. All are listed in `PROGRAM_STATE.md` as open decisions.

**Reproduction.**

```sh
scripts/health_check.sh
# scenarios: tests/skill_scenarios/README.md
```

**§2z-ak addendum, 2026-10-11 — COMMITTED at the owner's direction;
supersedes this section's "not committed" status.** Owner decisions in the
same exchange: commit the revision; the scenarios need not be run under
Codex. Not decided: the candidate-comparison margin (item 7) and whether to
enforce the sealed-data rule mechanically; the owner asked for both to be
explained first. Pending that, item 7 was reworded before the commit: the
skill no longer calls 0.05 a default, and says to use the registered
convention (0.05 in gate relative error, 20 reps, 2,000 bootstrap draws)
labelled as a convention until the owner sets a standing margin. The
ledger's initial statuses were committed as compiled and remain the
owner's to correct. §2z-ah and §2z-aj, which the exposure ledger and the
data-intake skill cite for the Reddit 2023–24 archive, are another
session's in-progress entries and are not in this commit; they and the
gap in lettering are resolved when that acquisition is recorded complete.

## 2z-al. 2026-10-11 — WD USB/read failure diagnosed; download recovery IN PROGRESS pending physical reconnection

Provenance: OpenAI Codex (GPT-6), 2026-10-11; work started from
`c5df907ee25f73d77730cd8e89f5c32590e82285`, whose data-intake,
model-status-authoring and repo-maintenance instructions governed this work.

The owner requested diagnosis, repair and download resumption after §2z-aj's
corrected transfer subsequently stopped. The download process exited with
code 17. Its last log modification was 16:40:18 Japan time; saved control
metadata showed 16:39:23. Neither file's contents were readable at this
check, so no new completion percentage is asserted.

Measured failure: logs, configuration and saved resume state all returned
errno 5 (input/output error), including outside the filesystem sandbox.
The system log records WD `disk4s2` I/O errors and USB host-controller errors
beginning at 16:40:17. The WD still enumerated at USB 5 Gb/s, but this did
not establish working storage access. About 131.7 GB remained free according
to mounted-volume metadata. No active downloader or open files on WD were
found. The physical failing component is not isolated; cable, port,
enclosure, drive and filesystem health are not adjudicated by these logs.

Recovery attempts: normal volume unmount failed; normal whole-drive unmount
failed; forced whole-drive unmount succeeded after the no-open-files check.
Normal remount and read-only remount both failed. `diskutil verifyVolume`
invoked non-writing `fsck_hfs -fn -x`, which failed immediately with exit 8
and diskutil -69845. This is a failed check, not a diagnosis of a specific
repairable filesystem defect. No repair-mode fsck, formatting, erasure or
source deletion was performed. The volume was left unmounted and the owner
was asked to physically disconnect/reconnect WD, preferably using another
cable/port, before any download writes resume.

Evidence is stored locally because WD is inaccessible:
`/private/tmp/wd-recovery-20261011-diagnosis.json`,
`/private/tmp/wd-recovery-20261011-system.log`,
`/private/tmp/wd-recovery-20261011-usb.log` and the USB device inventory.
Data contact is recorded in EXPOSURE_LEDGER: storage/log metadata only;
bridge status remains SEALED, no rows interpreted or scientific work run.

Software health check passed: research 182 tests plus 13 subtests; package
8 tests; default legacy guard 14/15, churn 0.013. Existing fixture warning
only. No model/software/default/protocol change. Download NOT restarted
while its destination and resume state remain unreadable. Verdict:
**recovery IN PROGRESS; physical reconnection required to continue**.

Commands used on the observed device identity (re-identify after reconnect;
never assume the same disk number): `diskutil info`, `diskutil unmount`,
`diskutil unmountDisk`, `diskutil unmountDisk force`, `diskutil mount`,
`diskutil verifyVolume`, `diskutil mount readOnly`. The intended resumed
download remains §2z-aj's corrected full selection with startup rescan off,
normal received-piece checks, preserved resume state and caffeinate.

**§2z-al recovery addendum, 2026-10-11 approximately 17:57 Japan time —
drive access restored; actual download resumed.** On the owner's retry
request, the WD was mounted again and configuration, control state and
logs were readable. The volume UUID matched
`F86ED844-5004-3419-B776-CE777D5F8FAF`; write verification also succeeded.
The recovered saved control file was 48,960 bytes, SHA-256
`89221f81f4d9678364f7f88651e7841b059e23b5456d317c4efacc8f188ef07e`.
The now-readable prior log ends at 16:38:49 with approximately 366.4 GB
received; this supersedes the earlier inability to read the saved progress,
not the observed USB error diagnosis. The physical faulty component remains
unidentified, and successful resumption is not a full drive-health verdict.

Fresh budget: 131,698,802,688 bytes free; 18,812,155,131 additional selected
allocation and 45,052,552,112 conservative boundary allowance leave
67,834,095,445 projected free bytes. Control/config backups and this budget
are saved in WD acquisition metadata with the `reconnected_20261011_1755`
identifier. The initial sandboxed disk-info query was denied; the same
read-only query succeeded with approved disk-service access. No raw values
were read; EXPOSURE_LEDGER remains SEALED.

The download launched at 17:56:58 in a user-owned Terminal window, with
caffeinate and the corrected `check-integrity=false` saved-state resume.
New payload was directly observed: 366,436,827,136 bytes at startup became
366,697,381,888 by 17:57:29, with live rates approximately 14–15 MB/s.
Progress is now also logged on the Mac at
`/private/tmp/reddit-reconnected-20261011-1755.stdout.log`, so a WD failure
will not hide the console diagnostics. The launch script is
`/private/tmp/reddit-resume-reconnected-20261011-1755.zsh`; it checks the
volume UUID, resume state and selection and refuses a second active client.
Terminal window 21816 is the observed user-owned process host. The CUA
inspection tool failed to initialize, but Terminal launch and live transfer
were independently confirmed through the completed AppleScript result,
process listing and exact byte counters.

This supersedes the earlier **recovery waiting for reconnection** status.
Acquisition remains IN PROGRESS; no completion, independent full-file
validation, or scientific-continuity claim. No scientific software changes;
the same-session health check above remains the validation result.

## 2z-am. 2026-10-11 — OWNER DECISION: nothing in the sealed new-data folders is looked at until the owner gives explicit permission; a mechanical check is installed for Claude Code (list, guard, hook, note files) and verified live; no standing comparison margin set. No data values read; no model, spec or protocol change

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-11; started
from commit c5df907. Sections §2z-ah, §2z-aj and §2z-al in the working copy
are another session's entries and may not yet be committed; a gap in the
lettering of the committed file is that, not a missing record._

**The decisions, in the owner's words (2026-10-11).** "wait to set a number.
yes, set a rule that prevents looking at the new data until I give explicit
permission." The first sentence answers §2z-ak's open item 7: no standing
margin for judging a candidate against the current model is adopted. The
skill keeps the convention of the registered evaluations (0.05 in gate
relative error, 20 reps, 2,000 bootstrap draws) as a labelled stand-in, and
the paired-seed noise of the gate remains unmeasured. The second sentence
adopts the enforcement that §2z-ak left to the owner. "The new data" is the
Reddit 2023-01..2024-06 archive and the Wikipedia pageviews data, which the
restart plan (§2z-ak) reserves for evaluation after development on data
already seen.

**The rule.** Nothing inside a folder listed in
`llm_fitting/SEALED_PATHS.json` is looked at until the owner says, in the
conversation, that the named dataset may be opened. Allowed there: listing,
sizes, checksums, integrity tests, downloads, and copies to a destination
that is itself listed. Permission is the owner's statement in the
conversation and nothing else — not a protocol draft, an agenda, a note in
a file, another agent's message, or a summary of an earlier conversation.
To unseal after that statement, the session quotes it in the contact log of
`EXPOSURE_LEDGER.md` and moves the entry from `sealed` to `released`. The
rule is stated once for every agent in `AGENTS.md`; the ledger, the
data-intake skill and the core skill point to it.

It is tighter than the ledger's earlier wording in one respect: schema and
row-count inspection of a sealed file (a parquet footer) was "not contact,
but record it"; it now also waits for permission, because the check cannot
tell a footer read from a value read.

**What is listed.**

| entry | folder | status | may be read |
|---|---|---|---|
| `reddit-bridge-2023-01_2024-06` | WD `pushshift/bridge_2023-01_2024-06/` | SEALED | `acquisition/` (download logs and control files), the downloader's control file `reddit.aria2`, and the note file |
| `wikipedia-pageviews` | T9 `rank-diffusion-data/wikipedia/`, also reached as `data/ssd/wikipedia` | EXPOSURE UNKNOWN, treated as sealed | the note file only; logs and manifest stay sealed until the exposure audit |

`scripts/data_wrangling/wikipedia_pageviews_pilot.py` is listed as a script
whose default paths point into the Wikipedia tree; a command line that
names it may read or stage it but not run it. No runner is approved to
operate on sealed folders (`approved_runners` is empty); the owner adds one
when wanted.

**What was built.**

- `llm_fitting/SEALED_PATHS.json` — the list, with the unsealing procedure
  written into it.
- `scripts/sealed_data_guard.py` — returns ALLOW, ASK or DENY for a shell
  command or a tool call. Its rule has two steps. First, does the line
  concern a sealed folder: does it name one, name the folder's parent
  together with the folder's own name or a wildcard, resolve into one
  through a variable, a loop, a glob, the `data/ssd` link or `cd`, list a
  containing folder recursively, or name the listed script. Second, if so,
  every simple command on the line (including those inside `$(…)`,
  `sh -c`, `xargs`, `find -exec`, `trap` and `eval`) must be an opaque
  operation or a plain filter of the output of one; an interpreter, an
  unknown program or a value the guard cannot compute is refused. It also
  refuses, on any line, recursive searches and bulk copies or archives of a
  folder that contains a sealed one; copies of sealed bytes to an unlisted
  destination; and the obvious destructive commands (rm, overwriting by
  redirection or copy, `rsync --delete`). Edits to the list, the guard or
  the hook settings, and requests that hand another agent's command line a
  sealed path, return ASK, an approval prompt for the owner. Modes:
  `--hook`, `--check-command`, `--check-read`, `--list`, `--self-check`; and
  `require_released(path)` for new builders and intake gates to call on
  their inputs.
- `.claude/settings.json` — a Claude Code hook that runs the guard before
  every tool call of every tool (`.gitignore` now lets this one file be
  versioned). If the guard cannot run, the hook refuses anything that names
  an external data drive and lets the rest through, so a broken guard
  neither opens the data nor locks the session.
- `00_SEALED_README.txt` in each of the two folders, saying in plain words
  what the folder is, what is allowed, and where the rule lives (SHA-256
  `f602ff95…faa4ee` on WD, `72f38c97…fe660` on T9). These are additions to
  the drives; a later inventory of either tree shows one more file than
  §2z-ag recorded.
- `tests/test_sealed_data_guard.py` (222 cases on a synthetic layout; no
  drive access) and a guard self-check in `scripts/health_check.sh`.

**How it was checked.**

1. _Independent adversarial review of the first version._ A separate agent
   session, given the code and not my conclusions, asked the guard about
   6,550 commands and tool calls (about 550 written by hand, 6,000 random,
   26 hook runs) on a throwaway layout; it read nothing in the real
   folders. It found fifteen groups of commands that would have read sealed
   bytes and were allowed (assignment from a substitution; `cd` then a
   relative path inside a substitution; the listed script by any spelling
   other than the plain one; Python here-documents whose lines happened to
   parse as harmless shell; a path assembled from the parent folder and the
   folder's name; loops over a parent folder; `bash -lc`; `${VAR:-default}`;
   exported variables; `$_` and `$OLDPWD`; positional arguments to `sh -c`;
   names carried by a file or pipe; and others), fourteen groups of
   everyday commands that were refused wrongly (among them the standard
   multi-line commit message), an exponential slowdown on inline code with
   many brace groups, and a hook that exited with an error instead of a
   decision when the list was unreadable. Its diagnosis: the first version
   judged only the words that visibly carried a sealed path and treated
   anything it could not compute as harmless.
2. _Rewrite on the reviewer's principle,_ as described above: once a line
   concerns a sealed folder, everything on it is refused unless shown to be
   opaque. Here-document bodies and `-c` strings are judged by what
   consumes them, not parsed as shell. The hook no longer exits with an
   error on any input tried.
3. _Replay of the reviewer's probe scripts against the rewrite_ (run by this
   session, not by the reviewer): every flagged route is now refused except
   those accepted as metadata (`file`, `xattr -l`, `gzip -l`, `zstd -l`,
   `tar -t`, `diff -q`, `cmp -s`, `wc -c`), a nested `claude -p` (itself
   hooked) and `git stash`. Of the reviewer's 169 everyday commands, 159
   are allowed, 1 asks and 9 are refused by design (an interpreter, `perl`,
   `open`, `mdls` or an unapproved runner on a line that names a sealed
   folder). 6,000 random commands: no exception, none slower than one
   second. On 51 commands of the kind this project runs, against the real
   list: 35 expected allowed and 16 expected refused, none wrong.
4. _Live, with the hook installed in this session._ A request for zero
   bytes of a lock file in the Wikipedia folder was refused by absolute
   path, through the Read tool, and by a relative path after `cd data`
   (so the hook sees the session's working directory); a listing of the
   same folder ran. A subagent given the same three calls got the same
   results. The refused commands never executed.
5. `scripts/health_check.sh`: research suite 404 passed and 13 subtests
   (182 before this entry plus the 222 guard cases), package suite 8,
   legacy guard 14/15 and 0.013, guard self-check wired. PASS.

**Not verified.**

- A fresh, separate session loading the hook: the command-line client could
  not authenticate ("OAuth session expired"), so that test did not run.
- The approval prompt (ASK) for rule-file edits and for handing a sealed
  path to another agent: never triggered, so whether the prompt appears in
  every permission mode is untested.
- The rewrite has not had a second independent review; step 3 is my replay.
- The hook's fallback for a guard that cannot run was bench-tested in bash,
  zsh and sh on the first draft of the hook command. The shell logic is
  unchanged since; only the message text differs. A repeat on the final
  string was refused by a built-in safety check on scripts passed through a
  shell variable and was not rerun. Behaviour on a hook timeout
  (`onFailure: block`) is as documented, not tested.

**What the check does not do.** It reads the text of one command. It cannot
see what a program opens once it is running, so a script that reaches a
sealed folder through a default path is caught only if the script is
listed; new builders should call `require_released`. It does not follow
names written to a file by one command and read back by the next. It covers
Claude Code sessions in checkouts that contain `.claude/settings.json`,
which other checkouts and worktrees will not have until the file is
committed. Codex is held by the same rule in `AGENTS.md` and by the note
files, not by a mechanism; a Codex-side hook was not built, because an
untested configuration in the other vendor's control folder could have
interfered with the acquisition that session was running. The WD volume has
ownership disabled and T9 is exFAT, so file permissions cannot enforce the
rule on either drive. It is a guard against mistakes, not a lock.

**Data contact.** Folder and file names, sizes and dates were listed in
both sealed folders; the two note files were written; the downloader's own
log in `acquisition/` was read (below). No data file was opened and no
values were read; both statuses are unchanged (`EXPOSURE_LEDGER.md` contact
log, 2026-10-11). No gate evaluation was run (`GATE_LEDGER.md` unchanged).

**Observation passed to the owner, not acted on.** The Reddit bridge
download (§2z-al recovery addendum) stopped at 18:01:19 Japan time, about
four minutes after its restart was confirmed. `acquisition/aria2.log` ends
with "Download aborted … errorCode=17 Write disk cache flush failure
index=96057", then "not complete" and a saved session; the control file was
last written at 18:01 and no downloader process existed at 19:16. The note
file was written to the same drive at 18:21, before I had read that log,
and compared byte-identical afterwards. Restarting or diagnosing the
download is the other session's operation and the owner's call.

**Not changed.** Frozen stacks, Gate v1, the registered protocols, the
claim set and every default. No scientific code was touched;
`wikipedia_pageviews_pilot.py` itself is unmodified.

**Open.** Whether to give Codex the same automatic check. Whether the
Wikipedia pilot and future loaders should call `require_released`
themselves, which no spelling of a command can avoid. The standing
comparison margin (deferred by the owner, above). The exposure audit of
Wikipedia 2025-01. None of this session's files are committed.

## 2z-an. 2026-10-11 — Reddit restart FAILED on recurrent WD write error; acquisition remains IN PROGRESS but stopped

**Provenance:** OpenAI Codex (GPT-6), 2026-10-11; starting commit
`c5df907ee25f73d77730cd8e89f5c32590e82285`. Data-intake and record-authoring
skills, including their current working-tree revisions, governed this operation.
Owner instruction: “restart and continue.” This supersedes the running status
in §2z-al's 17:57 recovery addendum; §2z-am independently records the earlier
18:01 failure.

**Failure and retry.** The 17:56 restart failed at 18:01:19 with errno 5,
aria2 error 17, writing `RC_2023-05.zst`, cache piece 96057. At 21:27 the WD
was mounted and its identity, configuration and saved control were readable.
Free space was 131,698,728,960 bytes; the allocation-aware completion budget
left 67,834,021,717 bytes, above the 50 GB acquisition guard. The 49,520-byte
control file was backed up on WD and locally before launching at 21:29:33
in the owner's Terminal with caffeinate. Its SHA-256 was
`edc4caff28945202219c4bdaa423f1019d0a9c9e8d5da3e9a7e0b199fc4d52a3`.
The existing 36-file selection and `check-integrity=false` were retained;
incoming torrent pieces still undergo normal hash verification.

**Measured result: FAILED.** The client counter advanced from
368,813,965,312 to 368,878,190,592 bytes after rolling back the failed piece:
64,225,280 additional bytes, still 45% of the client total 818,879,135,744.
At 21:30:51 a two-second process sample showed the main thread blocked in
`write`, reached through the torrent piece cache flush. Kernel logs showed
repeated USB transfer errors `0xe0005000`. At 21:31:11 the client reported
the same file, piece 96057 and errno 5/error 17 as the 18:01 failure. A
SIGINT requested graceful shutdown; the client logged its saved session
and exited by 21:31:14. No downloader remained on the subsequent process
check. No sustained transfer or reliable completion ETA is established.
The cable, port, enclosure and disk have not been isolated as the cause.

**Evidence and reproduction.** Mac-local files (retained if WD becomes
unreadable): `/private/tmp/reddit-current-restart.json`,
`/private/tmp/reddit-restarted-20261011-212925.stdout.log`,
`/private/tmp/reddit-restart-212925-process-sample.txt`,
`/private/tmp/reddit-restart-212925-kernel.log`, and the pre-restart backup
`/private/tmp/reddit-control-before-20261011-212925.aria2`.
Launch script: `/private/tmp/reddit-restart-20261011-212925.zsh`;
run with `/bin/zsh` only after WD access is restored and no downloader is
active. A subsequent retry should use a fresh stdout log to preserve evidence.

**Contact and validation.** Download/control/log/drive metadata only; no
archive rows or statistics read, no source deleted, no analysis or gate
run. EXPOSURE_LEDGER contact appended; SEALED status unchanged. No software
or scientific defaults changed; no tests rerun for this restart and record
update (earlier session health results remain recorded in §2z-al, and the
other agent's newer guard tests in §2z-am). No commit made while acquisition
and the shared working-tree changes remain unfinished. Physical connection
attention is required before another retry; do not loop restarts on this
repeated write failure.


**Later retry after owner-reported cable/port change, 2026-10-11 22:48–22:50
Japan time (still FAILED).** The owner confirmed changing the cable or port
and explicitly requested a restart. WD UUID matched; free space was
131,698,647,040 bytes. The saved 49,100-byte control was backed up locally
(SHA-256 `a9614b61ec52128b00de69286165384f1b319e8e0c3c5dd8c870a6d09c2be5be`).
Started in Terminal with caffeinate at 22:48:20 using the unchanged selection
and resume configuration. At 22:49:30 a process sample again found the
main thread blocked in a piece-cache `write`; kernel logs again showed
USB errors `0xe0005000`. At 22:49:48 aria2 failed on precisely the same
`RC_2023-05.zst` write, piece 96057, errno 5 / code 17. It saved the session
and exited itself by 22:49:50; no stop signal was sent in this attempt.
The final counter was 368,893,657,088 bytes, 15,466,496 beyond the preceding
saved counter, still 45%. The changed connection did not resolve the failure;
which physical or filesystem component is responsible remains unisolated.
No active downloader and no reliable ETA.

Evidence: `/private/tmp/reddit-new-connection-restart.json`,
`/private/tmp/reddit-restarted-20261011-224810.stdout.log`,
`/private/tmp/reddit-224810-process-sample.txt`,
`/private/tmp/reddit-224810-kernel.log`, and
`/private/tmp/reddit-control-before-20261011-224810.aria2`.
Launch script `/private/tmp/reddit-restart-20261011-224810.zsh`.
Download/control/log metadata only; SEALED unchanged and contact logged.
No source deletion, raw-value contact, scientific change, test rerun or commit.

## 2z-ao. 2026-10-11 — DATA OPERATION: the WD write failures are a bad disk region under `RC_2023-05.zst` (it fails reads as well as writes); download restarted without that one archive and IN PROGRESS for the other 35; the sealed-data guard is ruled out as a cause. No data values read; no model, spec or protocol change

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-11, 23:00–23:25
Japan time for the part recorded here; started from commit c5df907. Commit
71c7e07 (the sealed-data rule, §2z-am) was made by another session during
this work, and its guard checked every command here. §2z-ah, §2z-aj, §2z-al
and §2z-an in the working copy are another session's entries and may not
yet be committed._

**Owner request.** "check the WD digital drive that keeps failing to
download the reddit archives. diagnose the problem, fix it if you can, and
restart downloads. If downloads start OK, check every 5 minutes for a
while"; and, during the work, "make sure that this has nothing to do with
the guard being set up." This entry supersedes the stopped status at the
end of §2z-an. It does not complete the acquisition.

**Finding.** One fixed place on the WD disk, under the archive
`reddit/comments/RC_2023-05.zst`, can be neither written nor read. Every
restart since 18:01 died there because the client kept returning to the
same unfinished piece. It is not the cable or port (§2z-an's retry after
the connection change failed identically, as this predicts), and it is not
the guard.

| measurement | result |
|---|---|
| failing torrent piece (aria2 log, three runs: 18:01:19, 21:31:11, 22:49:48) | 96057 each time, errno 5, aria2 code 17 |
| what that piece is (torrent metadata: 16 MiB pieces; file 210 spans pieces 94783–96468) | bytes 21,364,171,976–21,380,949,191 of `RC_2023-05.zst`, 75.6% into a file already at its full 28,273,907,001 bytes, so a fixed set of disk blocks |
| writes elsewhere in the same runs | went through: the client counter rose by 2.4 GB, 64 MB and 15 MB before each failure (§2z-al, §2z-an) |
| read test: `shasum -a 256` of the whole file, started about 23:05:50 | no error for about 290 s (nine throughput samples: eight at 52–73 MB/s, one at 107), then stalled from 23:10:41 to 23:12:57 |
| kernel log during the stall | `disk4s2: I/O error` at 23:11:47.886 and 23:12:57.474; about 43 USB transfer errors `0xe0005000` some 3.3 s apart, the same pattern as during the failed writes |
| what `shasum` reported | exit 0 and a digest, within 5 s of the second I/O error. It is a digest of a truncated read, not the file's hash, and is not recorded here |

The sampled rates put the stall at roughly 19–22 GB into the file; the
failing piece is at 21.4 GB. That is consistent with the write failures,
not a measurement of the offset: the throughput was sampled, not logged.

**Two things about the evidence that later sessions need.** (1) `shasum`
does not report a failed read. It stops and prints a digest of what it
got, with exit 0. Read from the installed source (Digest::SHA 6.02): for a
named file `addfile` calls the compiled reader `_addfilebin` and checks
nothing afterwards, unlike its other two paths, which bail with "Read
failed". A checksum of a file on the WD counts only if the kernel log
shows no `I/O error` for the disk during the read, or if it comes from a
tool that raises on a failed read (Python's `hashlib` loop does). (2) The kernel logs one
`AppleUSBXHCI … status 0xe0005000` every 120 s while the drive is idle, and
one for each `diskutil info` query. §2z-an cited these messages as
corroboration; single ones are background. The fault signature is a run of
them about 3.3 s apart together with a blocked read or write.

**The guard is not the cause.** The first failure was at 16:40:17 and the
second at 18:01:19 (§2z-al, §2z-an). Nothing of the guard existed then:
`SEALED_PATHS.json` was created at 18:17:05, the note file was written to
the WD folder at 18:21:11, the guard script at 19:07:49 and the hook in
`.claude/settings.json` at 19:14:23 (file birth times). The hook inspects
the text of Claude Code's own tool calls; it does not run for a downloader
started in Terminal, and the guard script contains no call that changes a
file, a permission or a mount. The bridge folder and its files carry no
flags and no access-control entries. A refusal by the guard would be a
denied tool call, and a permission problem would be errno 1 or 13; the
client got errno 5 from the device, with matching kernel entries.

**Change made.** The download was restarted at 23:17:46 in a user-owned
Terminal window, with caffeinate, without torrent file 210. The registered
configuration `acquisition/full_bridge.conf` is unchanged; two settings are
overridden on the command line:

- `--select-file=206-209,211-223,441-458` (210 left out);
- `--bt-remove-unselected-file=false`. With the configured `true`, the
  client would delete the now-unselected `RC_2023-05.zst` when the others
  finish. That file must stay where it is: it holds about 14.2 GB of
  received data, and while it exists the bad blocks cannot be handed to
  another file.

The saved control file was backed up first as
`acquisition/reddit.aria2.before_skip210_20261011_2318` (48,960 bytes,
SHA-256 `1417fb363a8f31acf1987efc3e15ff18c1ee9473e21c512264bd788716aae83c`,
verified against the original). The client total is now 790,626,304,000
bytes for 35 files; the counter started at 354,718,629,888 (the 36-file
counter was 368,893,657,088). At 23:19:07 it stood at 355.33 GB with a live
rate of 13.8 MB/s, no error in the client log and no disk I/O error in the
kernel log. Earlier restarts failed within 90 s. Later observations are in
the addendum below.

**Not determined, not done.**

- The extent of the bad area. No read that reports an offset was run: the
  guard refuses `dd` on a sealed folder, and each read of the area stalls
  the drive for about two minutes. The first failure, at 16:40, took the
  whole drive off the bus.
- Whether other bad areas exist. A failure on a different piece would show
  one. SMART attributes are not readable through this USB bridge
  (`diskutil`: not supported; `system_profiler` reports only "Verified").
- `RC_2023-05.zst` is not being downloaded. The bridge will be 35 of 36
  archives until it is fetched into different disk blocks.
- No full-file hash verification of any archive. Received pieces are
  hash-checked by the client as they arrive (§2z-aj); that is all.
- The 23:00 volume was 98% full (131.7 GB free). Projected free space at
  completion is unchanged from §2z-an, about 67.8 GB, because file 210 was
  already at full length.

**For the owner to decide.**

1. How to get `RC_2023-05.zst`. Proposed: after the 35 finish, download
   file 210 alone into a separate folder inside the bridge tree with its
   own control file, leave the damaged file in place under a new name, and
   move the fresh copy to the proper name. Cost: 28.3 GB held permanently
   by the damaged file, and up to 27.5 GB more while it runs (HFS+ has no
   sparse files, so the boundary fragment of `RC_2023-04` is allocated in
   full and removed at the end).
2. The boundary fragments of the unselected months (`RC_2022-12`,
   `RC_2024-07`, `RS_2022-12`, `RS_2024-07`) will now remain after
   completion, up to 45 GB. Removing them is a deletion on the WD.
3. Whether to keep writing to this drive. It is the source of record for
   the raw archives, 98% full, with a confirmed media fault; §2z-ag copied
   199.0 GB of it to T9.

**Contact and validation.** Download control data, client and kernel logs,
file listings, torrent metadata, drive metadata, and one checksum read of a
sealed archive, whose output is a digest of opaque bytes and is discarded.
No row parsed, no value read; the bridge stays SEALED; contact logged in
`EXPOSURE_LEDGER.md`. No code, default, protocol or gate touched; the
health check was not rerun and nothing was committed.

**Files.** Launch script, health-check script and console log:
`tmp/reddit_bridge/` in the repository (untracked):
`resume_skip_RC_2023-05.zsh`, `check_download.zsh`,
`download-skip210-20261011-231745.stdout.log`. Diagnosis and change note:
`acquisition/skip210_restart_20261011.json` on the WD, with a copy of the
launch script. To resume after any stop, when no client is running:

```sh
/bin/zsh tmp/reddit_bridge/resume_skip_RC_2023-05.zsh
```

Do not run the earlier launch scripts or the bare command of §2z-aj: they
select all 36 files and will fail on piece 96057 again.

**§2z-ao addendum, 2026-10-11 23:29 Japan time — download STOPPED by
decision, not by failure; CrowdTangle coverage on T9 measured.** This
supersedes "IN PROGRESS" in the headline above: the acquisition is stopped
pending the owner's decision on the drive.

The restarted download ran from 23:17:46 to 23:28:38 with no client error
and no kernel disk I/O error. The counter went from 354,718,629,888 to
365,550,043,136 bytes (+10.83 GB, 46% of the 35-file total); checks at
23:19, 23:24 and 23:26 were clean, with five-minute rates of about 18 MB/s.
Earlier restarts failed within 90 s, so leaving out file 210 did avoid the
failure, for 11 minutes and 10.8 GB. That is not evidence about the rest of
the disk.

The owner then asked whether the best course is to stop and replace the
drive, and said: "The crowdtangle is irreplaceable so it is the priority."
The client was stopped with SIGINT at 23:28:36; it saved its session and
exited (status in-progress, no error). Reason: the Reddit archive is the
one replaceable thing on this disk, it is not needed until a protocol
exists, and random writes onto a 98%-full disk are the heaviest load it
can be given. The saved state resumes with the script above, on this drive
or, after copying the partial files and `reddit.aria2`, on another.

CrowdTangle material on the WD against T9. Sources: the per-file table of
the §2z-ag audit (T9 `manifest/drive_audit_20261008/source_disposition.csv`),
a listing of names and sizes on the WD tonight, and a size check of each T9
destination. No file was opened and nothing was re-hashed tonight; the
hashes are those verified on 2026-10-08.

| WD material | files | GB | copy on T9 |
|---|---:|---:|---|
| `crowdtangle/`, consolidated exports | 846 | 287.4 | all |
| `crowdtangle_backfill/`, daily parquets, plus one unpaired TSV and a log | 1,174 | 153.7 | all |
| `crowdtangle_backfill/`, daily TSVs | 1,173 | 352.6 | none. Each has a same-day parquet on T9, matched by file name; row equivalence was never tested (§2z-ag) |
| `crowdtangle_backfill/`, image archives `2023-02-19` to `2024-03-06` | 328 | 1,817.4 | none |
| total | 3,521 | 2,611.1 | 441.1 GB, 16.9% of the bytes |

All 2,020 T9 destinations exist at the recorded size. Six hidden files on
the WD (3.26 GB: two `.DS_Store`, four partial-transfer temporaries) are
outside the table. T9 has 487.7 GB free. The Pushshift raw archives
(§2z-ag: retained on WD only) can be downloaded again from the torrent of
§2z-ah; the CrowdTangle material cannot.

Not done: no copy was started. What goes to T9 first, whether its budget
(600 GB, 40% free) is waived for that, and the replacement drive are the
owner's decisions.

**§2z-ao addendum 2, 2026-10-11 late evening — OWNER DECISION on the TSVs
and the drive.** In the owner's words: "we don't need to back up the tsv's
if we have the parquet -- that is just the same file in a different format.
though we do need to get a different drive if this one is failing. ordered
the new drive." This settles two of the items above:

- The 1,173 daily backfill TSVs (352.6 GB) are not preservation targets.
  No copy to T9 was made and its budget is untouched. This rests on the
  owner's knowledge of how the files were produced; the row-for-row
  equivalence remains untested in this record (§2z-ag), and no session
  should cite the TSVs as an independent source.
- A replacement drive is ordered. Its model and capacity were not stated
  in the conversation.

What then exists only on the WD and cannot be re-acquired is the 328 image
archives (1,817.4 GB). Whether those are to be kept was not stated; the
plan below assumes they are. Proposed order when the new drive is mounted,
for the owner to confirm then: image archives; the 441.1 GB already on T9,
so that it has a second copy once the WD is retired; the Pushshift raw
archives; the partial bridge files and `reddit.aria2`, after the new
location is added to `SEALED_PATHS.json`; then resume the download there
and fetch `RC_2023-05.zst` fresh. Hashes from a reader that raises on a
failed read, with the kernel log checked for disk I/O errors. Until then
the WD is idle: no client is running and nothing was written to it after
23:28:38.

At the owner's instruction ("eject the drive.") the WD was ejected with
`diskutil eject` at 23:57 Japan time, without force and with no client
running; the volume unmounted cleanly and T9 stayed mounted. It is to stay
unplugged until the replacement drive is in hand, so a session that finds
`/Volumes/My Passport for Mac` missing should treat that as intended.

## 2z-ap. 2026-10-12 — EXPLORATORY MEASUREMENT: a step-by-step ladder from a simple rank diffusion to the frozen stacks, five development panels, 80 cells including 37 gate evaluations. Noise and a home carry 83% of the combined improvement and the first three steps 86%; the amplitude step raises the macro error on all five panels; the frozen structure cards and the long-submissions gate reproduce exactly, and the other four gates score lower at 20 reps than their recorded 3-rep values. No change adopted; no default, frozen stack, gate, threshold or protocol touched

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12, 00:10–02:18
Japan time; started from commit 71c7e07. The ladder scripts, their tests and
the run folder were uncommitted when the cells ran, so the ledger rows name
71c7e07 for the estimator, simulator and gate, which were unmodified. §2z-ah,
§2z-aj, §2z-al, §2z-an and §2z-ao in the working copy may not yet be
committed._

**Owner request.** "in the x-collection project we just did an explainer,
showing how adding additional model components increased accuracy. I want
you to do something similar in spirit for the core models here. Start from
the baseline of a simple rank diffusion model, and show how adding
additional features improves fit. Think hard about how best to do this.
Weight the two primary goals -- capturing macro-level stability and
individual endpoint movement -- equally; and find the appropriate way to
weight across the different data sets."

**Tier.** Exploratory diagnostic on DEVELOPMENT data (`EXPOSURE_LEDGER.md`,
contact line of 2026-10-12). Nothing here confirms anything, no rung is a
candidate, and the index defined below is a presentation device (a Tier-3
probe in the §2z-a hierarchy), not a criterion. Gate v1 is used unchanged.

**Design, written before the first cell**
(`llm_fitting/runs/2026-10-12_ladder/DESIGN.md`, file time 00:27; the first
cell finished 00:33). Every rung is a refit.

| id | adds | fit |
|---|---|---|
| R0 | rank diffusion: random walk in log activity, volatility by permanent-rank band | all of γ0 permanent |
| R1 | noise | γ0..γ6, random-walk level + white noise |
| R2 | home | γ0..γ6, OU level on the committed reversion grid + white noise |
| R3 | short-lived swings | the unrestricted `--md-lags 6` fit |
| R4 | endpoint amplitude | `--temperament` |
| R5 | fat tails | `--t-tails` |
| S1–S4 | structure only: `--md-vr-long`, `--stat-factor`, `--two-scale`, `--mix-hetero` | FB Era A, comments, long submissions; Instagram S2 only; short submissions none |
| G1, G2 | movement only: `--conditional state`; then `--spec-b` (FB) or `--mix-hetero` (comments) | G1 all panels |

The last structure rung is each panel's frozen structure stack and the last
movement rung its frozen movement stack (`frozen-specs.md` §2; long
submissions: the LONG stack of `subs_backtest.py` and the P6 command of
PREREG_2026-07-16). R0–R2 are restrictions of `_md_partition` swapped in for
one fit and restored (`ladder_explainer.py`: reversion grid fixed at 0.9999,
φ fixed at 0, white part booked as σ_obs). All rungs share K, B = 4K,
`min_knot_n = 8`, exact NNLS, exit and rebirth, the common factor, seeds
0..19.

- **Movement score:** Gate v1 exactly (5 rolling origins, 20 reps, 2,000
  bootstrap draws, the seven moments, `MOM_FLOOR`), with the gate's train
  calibration of the σ_obs scale at every rung. For R0, σ_obs ≡ 0 and the
  search is skipped at scale 1.0; a synthetic run confirmed the skip returns
  the same number as the committed search.
- **Macro score (in sample):** unconditional simulation over the panel
  length, 20 seeds; 19 statistics in four blocks (ladder: S(1), S(10), S(100)
  within the common recorded top-M; variance: VR2/4/8/13, ACF1/2; order:
  rolling R²(1/4/13), RACF1/4/13; top set: rolling Pers(1/4/13), out-flux at
  K), each error in the 15-card's own tolerance units (ratio statistics
  |ln(sim/emp)|/ln 1.2; correlations |diff|/0.08; Pers |diff|/max(3,
  0.15·top_k)), capped at 20; index = mean of the four block means.
- **Combined:** per panel, exp(½ ln[macro/macro at R0] + ½ ln[gate/gate at
  R0]); across panels a weighted geometric mean. Primary weights: one vote
  per platform-and-metric system (FB 1/3; comments 1/3; submissions 1/3,
  split long 1/6 and short 1/6); Instagram shown, weight 0. Four alternative
  weightings were fixed in the design and are all reported.

**Macro index by rung** (tolerance units, 1.0 = the card's pass line; bracket
= 5th–95th percentile over seeds; then the committed card at 5 seeds, passes
of 15 and churn error).

| rung | FB Era A | comments | subs long | subs short | IG rescue |
|---|---|---|---|---|---|
| R0 | 3.48 [3.41, 3.55]; 3/15, 0.051 | 3.73 [3.58, 3.87]; 5/15, 0.074 | 3.53 [3.46, 3.59]; 6/15, 0.142 | 2.06 [1.96, 2.18]; 5/15, 0.108 | 4.12 [4.02, 4.21]; 6/15, 0.058 |
| R1 | 2.34 [1.99, 2.70]; 7/15, 0.086 | 1.31 [1.13, 1.48]; 9/15, 0.036 | 0.95 [0.87, 1.03]; 11/15, 0.037 | 0.63 [0.53, 0.74]; 14/15, 0.103 | 2.27 [2.17, 2.35]; 7/15, 0.018 |
| R2 | 0.90 [0.88, 0.92]; 12/15, 0.059 | 1.16 [1.13, 1.18]; 11/15, 0.027 | 1.14 [1.14, 1.15]; 11/15, 0.135 | 0.27 [0.25, 0.29]; 14/15, 0.062 | 1.85 [1.83, 1.87]; 7/15, 0.042 |
| R3 | 1.42 [1.35, 1.49]; 11/15, 0.049 | 1.17 [1.10, 1.30]; 10/15, 0.063 | 0.73 [0.72, 0.76]; 13/15, 0.115 | 0.27 [0.24, 0.32]; 14/15, 0.066 | 1.77 [1.68, 1.85]; 9/15, 0.044 |
| R4 | 1.55 [1.48, 1.61]; 9/15, 0.056 | 1.46 [1.30, 1.59]; 9/15, 0.046 | 0.92 [0.91, 0.96]; 11/15, 0.090 | 0.32 [0.30, 0.34]; 14/15, 0.072 | 1.83 [1.78, 1.88]; 9/15, 0.039 |
| R5 | 1.76 [1.69, 1.83]; 9/15, 0.042 | 1.35 [1.22, 1.47]; 9/15, 0.041 | 0.89 [0.88, 0.96]; 10/15, 0.082 | 0.33 [0.30, 0.36]; 14/15, 0.052 | 2.08 [2.02, 2.14]; 5/15, 0.069 |
| S1 | 1.23 [1.17, 1.29]; 11/15, 0.010 | 1.02 [0.95, 1.10]; 11/15, 0.040 | 0.71 [0.69, 0.73]; 10/15, 0.064 | — | — |
| S2 | 1.11 [1.05, 1.17]; 15/15, 0.030 | 0.94 [0.89, 0.99]; 12/15, 0.064 | 0.72 [0.70, 0.73]; 10/15, 0.064 | — | 2.12 [2.07, 2.18]; 7/15, 0.061 |
| S3 | 1.11 [1.06, 1.15]; 14/15, 0.016 | 0.90 [0.83, 0.98]; 12/15, 0.048 | 0.62 [0.60, 0.64]; 11/15, 0.061 | — | — |
| S4 | 1.09 [1.05, 1.13]; 14/15, 0.029 | 0.86 [0.77, 0.94]; 12/15, 0.044 | 0.55 [0.53, 0.58]; 13/15, 0.068 | — | — |

Simulated top-1 share over the data's, by rung (R0 … top): FB 2.61, 4.56,
1.52, 2.58, 2.35, 3.38, 2.34, 2.38, 2.45, 2.78; comments 0.22, 1.36, 1.30,
1.12, 1.38, 1.23, 1.32, 1.32, 1.26, 1.38; subs long 0.16, 0.58, 0.57, 0.57,
0.56, 0.59, 0.70, 0.71, 0.73, 0.75; subs short 0.83, 1.93, 1.14, 1.15, 1.06,
1.07; IG 5.16, 3.36, 2.02, 2.00, 2.07, 3.15, 3.66 (20-seed means; the
seed-SD caveat of `pitfalls.md` §3 applies to every head-share number).

**Gate by rung** (mean ± SD over the 5 splits; share of splits with the
model median inside the held-out interval; then the paired change from the
rung below, mean ± SD over splits, and the number of splits that improved).

| rung | FB Era A | comments | subs long | subs short | IG rescue |
|---|---|---|---|---|---|
| R0 | 0.704 ± 0.105; 100% | 1.475 ± 0.352; 0% | 1.503 ± 0.114; 0% | 0.677 ± 0.024; 0% | 2.179 ± 1.554; 0% |
| R1 | 0.218 ± 0.058; 60%; −0.486 ± 0.090 (5/5) | 0.378 ± 0.058; 20%; −1.097 ± 0.319 (5/5) | 0.332 ± 0.081; 80%; −1.171 ± 0.116 (5/5) | 0.276 ± 0.024; 100%; −0.400 ± 0.017 (5/5) | 0.439 ± 0.199; 60%; −1.740 ± 1.381 (4/5) |
| R2 | 0.127 ± 0.050; 60%; −0.091 ± 0.068 (4/5) | 0.241 ± 0.077; 100%; −0.137 ± 0.066 (5/5) | 0.443 ± 0.431; 60%; +0.111 ± 0.368 (3/5) | 0.212 ± 0.026; 100%; −0.064 ± 0.016 (5/5) | 0.354 ± 0.097; 40%; −0.085 ± 0.227 (4/5) |
| R3 | 0.115 ± 0.024; 100%; −0.012 ± 0.042 (3/5) | 0.236 ± 0.096; 20%; −0.005 ± 0.076 (2/5) | 0.281 ± 0.198; 60%; −0.161 ± 0.257 (2/5) | 0.128 ± 0.041; 100%; −0.084 ± 0.024 (5/5) | 0.228 ± 0.096; 60%; −0.126 ± 0.120 (4/5) |
| R4 | 0.121 ± 0.031; 60%; +0.007 ± 0.026 (2/5) | 0.219 ± 0.081; 40%; −0.016 ± 0.021 (3/5) | 0.265 ± 0.158; 40%; −0.017 ± 0.050 (2/5) | 0.163 ± 0.019; 100%; +0.035 ± 0.028 (0/5) | 0.326 ± 0.138; 20%; +0.098 ± 0.073 (1/5) |
| R5 | 0.123 ± 0.014; 40%; +0.002 ± 0.027 (3/5) | 0.194 ± 0.087; 80%; −0.025 ± 0.026 (4/5) | 0.295 ± 0.202; 40%; +0.031 ± 0.062 (3/5) | 0.163 ± 0.019; 100%; 0.000 (0/5) | 0.302 ± 0.109; 20%; −0.024 ± 0.038 (2/5) |
| G1 | 0.122 ± 0.029; 60%; −0.001 ± 0.030 (2/5) | 0.186 ± 0.070; 60%; −0.008 ± 0.076 (3/5) | 0.194 ± 0.061; 80%; −0.102 ± 0.194 (3/5) | 0.129 ± 0.028; 60%; −0.034 ± 0.026 (4/5) | 0.257 ± 0.109; 60%; −0.044 ± 0.031 (4/5) |
| G2 | 0.103 ± 0.044; 80%; −0.019 ± 0.031 (3/5) | 0.137 ± 0.056; 80%; −0.049 ± 0.035 (4/5) | — | — | — |
| baseline | 0.145 ± 0.031 | 0.165 ± 0.062 | 0.163 ± 0.048 | 0.168 ± 0.004 | 0.593 ± 0.309 |

Short submissions R4 and R5 are the same number because the t-df moment
returns Gaussian on 12–17-week train windows. Pooled CRPS skill at the top
rungs is within ±0.006 of zero at every horizon on every panel, as recorded
before (§2z-q, §2z-ae).

**Combined error left, relative to R0** (bracket = 5th–95th percentile from
jointly resampling seeds and splits, 2,000 draws; it describes simulation
and split variability and is not a confidence interval for a population of
systems).

| weighting | R1 | R2 | R3 | R4 | R5 | + structure steps | + movement steps |
|---|---|---|---|---|---|---|---|
| primary: one vote per system | 0.343 [0.322, 0.359] | 0.229 [0.203, 0.249] | 0.217 [0.197, 0.234] | 0.238 [0.219, 0.257] | 0.238 [0.219, 0.256] | 0.196 [0.180, 0.210] | 0.170 [0.156, 0.182] |
| A: one vote per platform | 0.368 | 0.226 | 0.226 | 0.247 | 0.252 | 0.205 | 0.180 |
| B: one vote per panel | 0.330 | 0.234 | 0.206 | 0.229 | 0.231 | 0.193 | 0.167 |
| C: proportional to panel length | 0.298 | 0.256 | 0.212 | 0.231 | 0.232 | 0.186 | 0.157 |
| D: Instagram as a fourth system | 0.340 | 0.239 | 0.216 | 0.243 | 0.245 | 0.212 | 0.187 |

Primary, by goal at the top: macro 0.225, movement 0.129. Share of the total
log improvement (primary): R1 60%, R2 23%, R3 3%, R4 −5%, R5 0%, structure
steps 11%, movement steps 8%. Under all five weightings R1 is the largest
step, R2 the second, R4 raises the combined error, and the top sits between
0.157 and 0.187.

**What the measurements show.**

1. Noise is the largest step on both goals on every panel: gate error down
   59–80%, macro index down 33–73%. At R0 the held-out median 13-week move
   is 3.7 (FB), 6.5 (comments) and 5.4 (subs long) times the data's, and
   VR13 is 0.76–0.87 on those panels against 0.14–0.20 in the data.
2. Home lowers the gate by 19–42% on four panels and is the largest macro
   step on FB. On long submissions R2 is worse than R1 on both scores: with
   φ fixed at 0 the fitted κ is on the top of the committed grid (0.20) for
   43% of knots there, and simulated RACF13 is 0.10 against 0.60 in the
   data. R3 repairs it (κ median 0.005, RACF13 0.52).
3. After R2 or R3 the gate does not separate the rungs on FB (0.127 → 0.103
   over five steps, paired changes all within their split SD). On comments
   it falls from 0.241 to 0.137, with the largest late step at G2
   (`--mix-hetero`, −0.049 ± 0.035, 4/5).
4. The amplitude step raises the macro index on all five panels (FB +9%,
   comments +24%, subs long +25%, subs short +16%, IG +4%), mostly through
   the variance block; the seed bands separate cleanly only on long
   submissions. At R4 the simulators scale σ_trans and σ_obs by the
   amplitude and not σ_perm (the gap of `frozen-specs.md` §5). S1–S4 more
   than recover the loss on the three long panels, mostly at S1; S4 itself
   moves the index by −1%, −5% and −11%. A rung with `--mix-hetero` directly
   after R3 was NOT RUN, so whether the half-step causes the loss is a
   conjecture.
5. On FB the three-profile R2 has the lowest macro index on the ladder (0.90
   against 1.09 for the frozen stack), through the ladder block: top-1 share
   1.52× the data against 2.78×. The frozen stack is better on the variance
   block (0.13 against 0.45) and the card (14/15 against 12/15). R2's κ is at
   the top of the grid on 80% of FB knots (35% on comments), so it is a
   boundary value, not an identified estimate, and R2 is not a candidate. The
   head-share residual of §2z-b is visible from R3 on at 2.3–3.4× on FB and
   2.0–3.7× on Instagram.
6. The upper tail of held-out movement (90th percentile, recorded per split,
   not scored by the gate) is under-predicted by 37–39% at 13 weeks on FB
   from R3 to the top; R4 and R5 do not change it. On comments it is within
   +10% at every rung from R3.
7. Neither score contains the moments that identify the amplitude and the
   tails (the variance-dispersion moment, s = 0.69–1.12 on these panels; the
   within-entity kurtosis moment, df 4.3–4.6 where it is finite). "No gain on
   this index" is therefore not evidence that either is unnecessary.
8. On long submissions the held-out coll1 is 0 in three of five splits and
   0.038 and 0.077 in the other two, so that one moment dominates the gate
   there (R2's split at T0 = 132 scores 1.246).

**Predictions written in DESIGN.md, scored.**

| # | prediction | outcome | verdict |
|---|---|---|---|
| 1 | R0 fails both goals everywhere: VR near 1, head shares at least twice the data, gate at least twice the baseline | gate 3.7–9.2× baseline; VR13 0.61–0.87 on four panels, 0.34 on the 30-week panel; top-1 share 2.6× and 5.2× on FB and IG but 0.16–0.83× on the three Reddit panels | PARTIAL |
| 2 | R1 is the largest single movement gain on every panel, 40% or more | 59–80% on all five | PASS |
| 3 | R2 is the largest single macro gain on the long panels and moves the gate by less than 0.05 | largest macro step on FB only; gate moved 0.064–0.137 on every panel (up on subs long) | FAIL |
| 4 | R3 removes less than 20% of the remaining error on either goal | gate −36% to −40% on three panels; macro −36% on subs long and +58% on FB | FAIL |
| 5 | R4 improves the gate on the Reddit panels by 0.02 or more and raises the S(1) error on FB | −0.016, −0.017, +0.035; FB top-1 ratio fell from 2.58 to 2.35 | FAIL |
| 6 | R5 changes the gate by less than 0.02 and raises the S(1) error on FB | FB top-1 ratio 2.35 → 3.38; gate moved 0.024–0.031 on three panels, each inside its paired split SD | PARTIAL |
| 7 | G1 improves the gate on the submissions panels and does little on FB; G2 brings FB to about 0.12 | −0.034 and −0.102; −0.001 on FB; FB top 0.103 ± 0.044 | PASS |
| 8 | S1–S4 improve the variance block on FB and comments; S3 helps FB more than comments | variance block 1.28 → 0.13 and 2.39 → 1.32; S3 moved the index 1% on FB and 4% on comments | PARTIAL |
| 9 | the top rungs reproduce the recorded values within their recorded split spread | see the next table | PASS |
| 10 | R0–R3 account for at least 70% of the combined log improvement | 86% | PASS |

**Reproduction.** The recorded gate commands of `frozen-specs.md` §2 were
also run verbatim (CLI defaults: 3 reps, 400 bootstrap draws).

| panel | structure card, record → ladder top | gate, record | recorded command, today | ladder top, 20 reps / 2,000 draws |
|---|---|---|---|---|
| FB Era A | 14/15, 0.029 (§2z-g) → 14/15, 0.029 | 0.123 ± 0.033, 60% (§2z-g) | 0.123 ± 0.033, 60% | 0.103 ± 0.044, 80%; below baseline 4/5; scale 1.0 on 5/5 |
| comments | 12/15, 0.044 → 12/15, 0.044 | 0.171 ± 0.046, 60% | 0.171 ± 0.046, 60% | 0.137 ± 0.056, 80%; below baseline 4/5 |
| subs short | 14/15, 0.052 → 14/15, 0.052 | 0.164 ± 0.053, 80% | 0.164 ± 0.053, 80% | 0.129 ± 0.028, 60%; below baseline 5/5 |
| IG rescue | 8/15, 0.062 at 20 seeds → 8/15, 0.062 at 20 seeds | 0.320 ± 0.189, 60% | 0.320 ± 0.189, 60% | 0.257 ± 0.109, 60%; below baseline 4/5 |
| subs long | none recorded for this card → 13/15, 0.068 | 0.194 ± 0.061, 80%, at 20 reps (§2z-ae P6) | not rerun | 0.194 ± 0.061, 80%; below baseline 2/5 |

So the code and the panels are unchanged, and the four reference gates of
§2z-g were recorded at 3 reps. At 20 reps the same stacks score 0.020–0.063
lower on all four. That direction is what Monte Carlo noise in an absolute
deviation would produce when the baseline has none; the cause was NOT
isolated here (no rep sweep was run). The frozen reference values and every
recorded verdict stand as recorded. Whether to re-record the reference gates
at the registered precision is an owner decision.

**Verdict.** No adoption rule applies and nothing is adopted. As a
description: the three-component partition (noise, home, short-lived swings)
carries 86% of the combined improvement on this index; the later features
are identified from moments this index does not score, and their measured
contribution to it is small and mixed in sign. This is development evidence
about parsimony (`PROGRAM_STATE.md`, open residual 7), not a ranking of
models.

**Limits.** The macro score is in sample and the variance block is partly a
fitting target from R1 on. The gains are path-dependent: the order is the
logical nesting and the program's history, was fixed in advance and was not
searched, and no leave-one-out was run. The gate limits of
`evidence-standards.md` §4 apply to every gate number here. 37 exploratory
and 4 reproduction evaluations were added to the same development panels;
all are in `GATE_LEDGER.md`.

**Execution record.** 43 structure cells and 37 gate cells, 00:33–02:11, all
completed; no cell was killed after producing output. Two execution
problems, neither affecting a number: (i) the first launch at 00:32 from the
desktop app's Terminal panel left the two processes that read `/Volumes/T9`
blocked in `open()` (`sample` showed the stack; nothing was read or scored);
they were killed and relaunched at 00:35 from the agent shell; (ii) the
single Instagram process started at 01:15:04 was stopped during its first
structure cell, before any cell was written, and restarted as three
processes to shorten the wall time. `scripts/health_check.sh` after the
work: PASS (research suite 414 passed, including the 10 new tests; package
suite 8 passed; legacy guard 14/15, churn 0.013). Not committed in this
session: the owner had not asked for a commit.

**Files.** `llm_fitting/ladder_explainer.py` (runner and summary),
`llm_fitting/ladder_explainer_page.py` with
`ladder_explainer_template.tmpl` (page builder), `tests/test_ladder_explainer.py`
(10 tests: recovery by each restricted partition, restoration of the
committed estimator and grids, the top rungs equal the frozen stacks, index
units, weights, the gate formula), and
`llm_fitting/runs/2026-10-12_ladder/` (DESIGN.md, launch.zsh, 80 cell JSONs,
gate logs, the four reproduction logs, summary.json, explainer_text.json,
gate_ledger_rows.md). The built page is `explainer.html` in that folder
(git-ignored as a build product) and was published to the owner as a private
artifact. No existing code file was modified.

**Commands.**
```
zsh llm_fitting/runs/2026-10-12_ladder/launch.zsh          # every cell; skips cells already on disk
python -u llm_fitting/ladder_explainer.py run --panel facebook_a --out llm_fitting/runs/2026-10-12_ladder
python llm_fitting/ladder_explainer.py summarize --out llm_fitting/runs/2026-10-12_ladder
python llm_fitting/ladder_explainer_page.py --out llm_fitting/runs/2026-10-12_ladder
```

**For the owner.** (a) Whether to re-record the four reference gates at 20
reps. (b) The b = 1 implementation gap already on the open list now has
measurements on both sides: the macro index rises at the movement-only
amplitude step on five panels, and `--mix-hetero` is the largest late gate
step on comments; a direct test (the full amplitude straight after R3, on
the gate for FB and short submissions as well) would be a candidate
evaluation with its own dossier and was not run. (c) Whether a leave-one-out
from each frozen stack is wanted beside this forward path.

## 2z-aq. 2026-10-12 — PLANNING ONLY: a work plan for the development phase and a working protocol for two agents, both PROPOSED; nothing adopted, no model or protocol change, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; started
from commit 71c7e07, with §2z-ah, -aj, -al, -an, -ao and -ap present in the
working copy and not yet committed._

**Owner request.** With downloading blocked by the drive, propose a
concrete plan to advance the agenda on the existing models and data,
reviewing both agendas of 2026-10-08, aimed at the largest remaining
sources of error per unit of effort; with Claude in charge of Codex/Astra,
including prompts, setup and protection against overwriting; and adjusted
for the ladder explainer of §2z-ap only where it has actionable lessons.

**What was read.** `PROGRAM_STATE.md`; §2z-ap in full and its
`runs/2026-10-12_ladder/summary.json`; the published explainer page;
`RESEARCH_AGENDA_2026-10-08.md` and `RESEARCH_AGENDA_CLAUDE_2026-10-08.md`
(parts B–F); `frozen-specs.md` §2, §4, §5 and `evidence-standards.md` §3,
§5, §6, §8; the committed briefs on agent coordination. No panel was opened
apart from the legacy fit run by the health check.

**What is proposed** (`llm_fitting/WORKPLAN_CLAUDE_2026-10-12.md`).

- Where the remaining error is, from the record: too few large lasting
  moves (established departures, the 13-week upper tail, FB boundary
  out-flux); head share on FB and the Instagram rescue; the amplitude
  applied to half of the model; and a gate that cannot resolve what is
  left. Gate v1 scores none of the first three.
- A stated hypothesis, to be tested and not assumed: the first two are one
  error, fat tails placed in the passing component and absent from the
  lasting one.
- Phase 1, no model code and no candidate gate evaluations: five
  measurements by Claude (A1 tails by horizon, A2 anatomy of departure
  events, A3 attribution of the head share, A4 compressibility of the band
  profiles and V = σ_perm²/(2κ) from existing fits, A5 the ladder cell that
  was not run) and three tasks for Codex (V1 the gate as an instrument, V2
  planted mechanisms and recovery with an independent simulator, V3
  endpoint-level scores, supplementary and retrospective).
- A decision point, then at most three candidates with dossiers, one gate
  evaluation per candidate per panel and at most 15 in the phase: one
  amplitude for the whole model; tails where they persist; compressed
  profiles. None adds a free parameter.

**What the explainer changed in the plan.** The instrument study comes
first and no candidate is evaluated before it; the amplitude gap becomes
the first candidate and its missing cell is run first; the head-share
measurement becomes an attribution across components, because the excess
appears with Gaussian shocks at the swings step (my earlier account blamed
the unbounded t alone); the unchanged 13-week tail on FB under the
amplitude and tail steps is what makes the hypothesis testable; gate
differences on long submissions are not used to rank candidates until V1
reports.

**Working protocol for two agents** (`llm_fitting/coordination/PROTOCOL.md`,
briefs in `coordination/briefs/`). Codex works only in its own working copy
on `codex/<task>` branches; Claude is the only writer of MODEL_STATUS, the
ledgers, PROGRAM_STATE and the two model files; each task ends in a handoff
folder that includes the record entry for Claude to append; heavy jobs
share three slots; blind checks get definitions, not numbers.

**Tooling added, tested, not committed.** `scripts/agent_worktree.sh`
(creates a second working copy, links the data folder, makes git ignore the
link; a throwaway copy passed 230 tests and reproduced the legacy fit,
14/15 and 0.013, and was then removed; the script added `/data` to the
local `.git/info/exclude`). `scripts/heavy_lock.py` (three job slots held
by operating-system file locks; a third job waited for a free slot in a
two-slot test). `scripts/health_check.sh` after the work: PASS (research
suite 414 passed and 13 subtests, package suite 8, legacy guard 14/15 and
0.013).

**Not done.** Nothing was sent to Codex. No measurement in the plan was
run. `AGENTS.md` was not changed; if the protocol is approved it needs a
pointer there. No commit.

**For the owner.** Adopt, amend or reject the plan; approve the protocol;
say who commits the pending work on `main`; approve the instrument runs of
V1.

## 2z-ar. 2026-10-12 — OWNER DECISION: the work plan of §2z-aq is ADOPTED as the working agenda for the development phase; the two-agent protocol is APPROVED; Codex's instrument runs are approved; pending work on main is committed and pushed. No model, spec or gate change

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; started
from commit 71c7e07 with the pending sections listed in §2z-aq._

**The decision, in the owner's words.** "implement the plan. do commit and
push what is pending on main. I approve the protocol and codex's instrument
runs. you have permission to do direct calls for small jobs using the codex
plugin. Do use sleep/wake when you are just waiting for results, or waiting
on codex."

**What it adopts and supersedes.**

- `WORKPLAN_CLAUDE_2026-10-12.md` is the working agenda for the development
  phase. It replaces §2x in that role. The two agendas of 2026-10-08
  (§2z-af, §2z-ai) stay on file as background; neither was adopted.
- `coordination/PROTOCOL.md` governs how Claude and Codex share the
  repository. `AGENTS.md` now points to it, and says that for Codex the
  record entry in a handoff folder stands in for a direct append to this
  file. Claude appends those entries unchanged, with Codex named.
- The instrument runs of brief V1 are approved: frozen movement stacks
  only, logged as reproductions. No candidate evaluation is approved; that
  waits for decision point D1 of the plan.
- Claude may call Codex directly through the Codex plugin for small jobs
  (a review, a blind check, a question), and sets a watch to be woken while
  it waits. Both are written into the protocol.

**What stays the owner's.** Every gated item in the skill: frozen stacks,
Gate v1, margins, protocols, re-freezes, paper claims, sealed data. The
comparison margin remains unset (§2z-am); V1 is to supply the evidence.

**What changed in the repository with this entry.** The plan and protocol
carry their new status; `coordination/QUEUE.md` lists Codex's tasks V1–V3
in order with what each waits on; `AGENTS.md` gained a section "Two agents
at once"; the repo-maintenance skill and its mirror list the new paths.
The pending work of other sessions is committed by Claude at the owner's
instruction, in separate commits that name their authors: the data
operations (§2z-ah, -aj, -al, -an by OpenAI Codex; §2z-ao by another
Claude session) and the ladder measurement (§2z-ap, another Claude
session). Two literature documents that describe another project in detail
remain uncommitted, as held since 2026-10-09; `tmp/` is not in git.

**Not done at the time of writing.** No Phase 1 measurement has been run
and nothing has been sent to Codex. Each task will get its own entry.

## 2z-as. 2026-10-12 — EXPLORATORY MEASUREMENT (work plan A5): the ladder cell that was skipped. Applying the endpoint amplitude to the lasting component repairs 49–94% of the variance-block loss of the amplitude step and raises the FB top-1 share from 2.35× to 3.54× the data; three of five predictions failed; nothing adopted, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; design and
predictions committed at 5072f89 before the first cell, results at d7a6ff5._

**Tier.** Exploratory diagnostic on DEVELOPMENT data, structure only, in
sample. First task of Phase 1 of the work plan adopted in §2z-ar.

**Question.** §2z-ap found the macro index rising at the amplitude step (R4)
on all five panels, with the simulators scaling only σ_trans and σ_obs by
v_i. Is the half-application the cause? The cell R4m = R3 + `--temperament
--mix-hetero` was run through `ladder_explainer.run_struct`, 20 seeds (0–19),
and compared with the R3 and R4 cells of §2z-ap (same seeds and code).

**Result** (macro index in scorecard-tolerance units; bracket = 5th–95th
percentile from resampling the 20 seeds jointly).

| panel | R3 | R4 | R4m | R4m − R4 | variance block R3 / R4 / R4m | ladder block R3 / R4 / R4m | b | top-1 share over data, R4 → R4m |
|---|---|---|---|---|---|---|---|---|
| FB Era A | 1.42 | 1.55 | 1.71 | +0.159 [+0.026, +0.305] | 0.81 / 1.26 / 1.04 | 3.46 / 3.19 / 4.17 | 1.02 | 2.35 → 3.54 |
| Reddit comments | 1.17 | 1.46 | 1.43 | −0.029 [−0.233, +0.182] | 1.92 / 2.37 / 2.06 | 0.92 / 1.55 / 1.86 | 1.08 | 1.38 → 1.51 |
| Reddit submissions, long | 0.73 | 0.92 | 0.77 | −0.150 [−0.204, −0.109] | 0.94 / 1.75 / 1.13 | 1.06 / 1.07 / 1.24 | 0.95 | 0.56 → 0.53 |
| Reddit submissions, short | 0.27 | 0.32 | 0.32 | 0 | 0.11 / 0.30 / 0.30 | 0.38 / 0.26 / 0.26 | 0 | 1.06 → 1.06 |
| Instagram rescue | 1.77 | 1.83 | 1.64 | −0.196 [−0.247, −0.132] | 1.26 / 1.42 / 1.27 | 3.37 / 3.40 / 2.93 | 1.02 | 2.07 → 1.79 |

**What the measurements show.**

1. The half-application is a real cause of the variance-block loss at R4:
   the full amplitude removes 49% of it on FB, 69% on comments, 77% on long
   submissions and 94% on Instagram.
2. The full amplitude also moves the head. On FB the simulated top-1 share
   goes from 2.35 to 3.54 times the data's and the index gets worse, with a
   seed band that excludes zero; the ladder block worsens on comments and
   long submissions too.
3. On the 30-week panel b = 0 by construction (no horizon has the eight
   non-overlapping changes the estimate needs), so R4m equals R4.

**Predictions (DESIGN.md), scored.** (1) R4m not higher than R4 on the three
long panels: FAILED on FB. (2) Recovery under half on comments and long
submissions: HELD on comments (10%), FAILED on long submissions (81%). (3)
FB difference inside its seed band: FAILED. (4) FB top-1 share not lower at
R4m: HELD. (5) Instagram within ±5%: FAILED (−10.7%).

**Verdict.** No adoption rule applies. For the plan: candidate C-A ("one
amplitude for the whole model") is not a flag change. It repairs horizon
scaling and feeds the head-share error, which is the largest macro error on
FB. Its dossier has to say what happens at the head, and task A3 has to
measure how amplitude and rank relate among the top endpoints. A related
lead from the first A1 panel (FB, not yet written up): the simulation's
endpoint-level one-week changes are close to Gaussian while the data's are
fat-tailed, which raises the possibility that the amplitude spread s is
inflated by within-endpoint fat tails. Codex's recovery study (V2, part 2)
tests exactly that; it is a conjecture until then.

**Not run.** Any gate evaluation; any variant with the amplitude shrunk or
bounded. **Files.** `llm_fitting/phase1_a5.py`;
`llm_fitting/runs/2026-10-12_A5_full_amplitude/` (DESIGN.md, cells,
summary.json, REPORT.md). Command: `python llm_fitting/phase1_a5.py run`,
then `report`.

## 2z-at. 2026-10-12 — EXPLORATORY MEASUREMENT (work plan A4): the band profiles of the three volatilities reproduce between halves of the entities and are not straight lines in rank; the reversion-rate profile does not reproduce and sits on its slowest grid value at 31–52% of knots. Four of eight sub-predictions failed; nothing adopted, no simulation, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; design,
predictions and runner committed at a2728b7 before any fit, results at
adec301._

**Tier.** Exploratory diagnostic on DEVELOPMENT data; fits only. Phase 1 of
the work plan adopted in §2z-ar. First measurement against open residual 7
(parsimony of the band profiles).

**Question.** How much of each fitted profile over about 55 rank knots is
structure that reproduces, how much of that a low-dimensional form
captures, and whether κ is identified band by band. Nine fits of the frozen
structure stacks: FB Era A, long submissions and comments, each on the full
universe and on two random halves of the entities.

**Result.** "Reliability" is the half-sample correlation of a profile
across knots, stepped up to full size. "Line share" is the R² of a straight
line in the rank coordinate over the reliability.

| panel | profile | reliability | line share | scaling exponent |
|---|---|---|---|---|
| FB Era A | log σ_perm | 0.84 | 0.12 | −0.03 |
| | log σ_trans | 0.44 | 1.14 | −0.08 |
| | log σ_obs | 0.78 | 0.76 | −0.18 |
| | log κ | 0.34 | 0.10 | |
| | log V | 0.69 | 0.15 | |
| Reddit submissions, long | log σ_perm | 0.74 | 0.69 | −0.03 |
| | log σ_trans | 0.89 | 0.76 | −0.12 |
| | log σ_obs | 0.94 | 0.79 | −0.23 |
| | log κ | none (half-sample correlation −0.44) | | |
| | log V | 0.63 | 0.86 | |
| Reddit comments | log σ_perm | 0.77 | 0.54 | −0.05 |
| | log σ_trans | 0.89 | 0.79 | −0.12 |
| | log σ_obs | 0.97 | 0.56 | −0.14 |
| | log κ | 0.07 | | |
| | log V | 0.52 | 0.55 | |

Share of knots with κ at the slowest grid value, 0.005: FB 31%, long
submissions 52%, comments 44% (50% in the top third of knots). No knot is at
the fastest value; the largest κ used is 0.07 on FB and comments and 0.02 on
long submissions, where κ takes three values. Seen while tabulating and not
in the design: σ_obs is exactly zero at 15 of 54 knots on FB, 18 of 54 on
long submissions and 21 of 55 on comments.

**What the measurements show.**

1. The volatility profiles are mostly reproducible structure (0.74–0.97,
   except σ_trans on FB at 0.44).
2. A straight line in rank captures 54–79% of the reproducible share for
   five of the nine volatility profiles and 12% for σ_perm on FB. Post hoc,
   polynomials of up to six parameters do not close the gap: for σ_perm on
   FB the R² stays at 0.13 against a reliability of 0.84.
3. κ is not identified band by band. V = σ_perm²/(2κ) reproduces better
   than κ on every panel.
4. Volatility depends weakly on size: exponents −0.03 to −0.23, far from
   the −0.5 of counting noise.

**Predictions (DESIGN.md), scored.** (1) A line reaches 0.8 of reliability
for σ_trans and σ_obs on all three panels: FAILED (one of six). (2) A line
reaches 0.6 of reliability for σ_perm: FAILED on two panels; κ reliability
under 0.5 on at least two panels: HELD. (3) σ_obs exponent between −0.6 and
−0.4 on the Reddit panels: FAILED (−0.23, −0.14). (4) σ_perm and σ_trans
exponents within 0.1 of each other: HELD. (5) At least 30% of knots at a
grid edge, and at least half of the top third on comments at the slowest
value: HELD. (6) log V more reliable than log κ: HELD.

**Verdict.** No adoption rule applies. For the plan: candidate C-P as
written (each profile replaced by a scaling curve of a few numbers) is not
licensed. What is licensed is narrower: replacing κ(z) by one or two
numbers, and stating transport between eras in V. The head κ of 0.0050 used
as the reference in §2z-q is the slowest value the grid allows, so the 4–8×
"shift" in κ recorded there (open residual 3) is at least partly a boundary
estimate moving off its boundary. Fewer, wider bands remain an open form of
compression.

**Not run.** Any refit with fewer bands; V by era on the extension panel;
the sensitivity of each estimate to each moment. **Files.**
`llm_fitting/phase1_a4.py`; `llm_fitting/runs/2026-10-12_A4_profiles/`
(DESIGN.md, summary.json, posthoc_polynomials.json, REPORT.md). Command:
`python llm_fitting/phase1_a4.py`.

## 2z-au. 2026-10-12 — EXPLORATORY MEASUREMENT (work plan A1, A2): the missing departures are repeat crossings by endpoints already at the boundary, not sudden lasting exits, which the simulation over-produces; an endpoint's own changes are fat-tailed at every horizon and the simulation's are near Gaussian; the data spreads faster with horizon. Four of ten predictions held in full, five in part, one failed; nothing adopted, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; statistics,
tests, design and predictions committed at f284b9b before any cell; results
committed with this entry._

**Tier.** Exploratory diagnostic on DEVELOPMENT data, in sample for A1; A2
uses the train-only cohort and scoring windows of §2z-ac and §2z-ae P7. Not
yet interpretable as mechanism evidence: the planted-mechanism study that
says which of these statistics can separate the candidates (Codex, V2) has
not run.

**Question.** Open residuals 1 and 4. A1: do the fat tails of movement fade
with horizon (passing component) or persist? A2: are the departures the
simulation fails to make slow slides, short excursions or sudden lasting
exits?

**Method.** `llm_fitting/residual_anatomy.py` (known-answer tests in
`tests/test_residual_anatomy.py`), one code path for data and simulation.
A1: FB Era A, long submissions, comments; full-window universe; frozen
structure stacks; 20 seeds; h-week changes net of the common factor, scaled
by each endpoint's own robust scale. A2: comments extension and long
submissions with the settings of `exit_audit.py --aligned` and `--p7`; 30
seeds; each exit of the cohort (train permanent rank ≤ K/2) classed as a
slide (median rank over the previous 13 weeks beyond 0.75 K), an excursion
(back within 13 weeks), a lasting exit, or censored.

**Result, A1.** q99/q90 of an endpoint's own |h-week change|, middle rank
group; "walk" is a Gaussian random walk of the same length.

| panel | h | data | simulation (seed SD) | walk |
|---|---|---|---|---|
| FB Era A | 1 / 13 / 26 | 2.22 / 1.92 / 1.90 | 1.69 / 1.65 / 1.65 (0.01) | 1.63 / 1.72 / 1.83 |
| Long submissions | 1 / 13 / 52 | 2.13 / 2.17 / 2.06 | 1.73 / 1.63 / 1.63 (0.01) | 1.59 / 1.64 / 1.76 |
| Comments | 1 / 13 / 52 | 1.90 / 1.89 / 1.85 | 1.68 / 1.62 / 1.64 (0.01) | 1.60 / 1.68 / 1.82 |

Fraction of a big one-week move (beyond 2 robust SDs) still in place after
13 weeks, middle group, data / simulation: rises 0.49 / 0.55, 0.60 / 0.60,
0.56 / 0.58; drops 0.58 / 0.47, 0.56 / 0.47, 0.54 / 0.50 (FB, submissions,
comments).

**Result, A2.** Rates per cohort-week at risk; brackets are the 10th–90th
percentile over seeds.

| | comments ext., data | simulation | long submissions, data | simulation |
|---|---|---|---|---|
| all exits | 0.00800 | 0.00350 [0.00283, 0.00410] | 0.00851 | 0.00311 [0.00214, 0.00408] |
| slides | 0.00512 | 0.00156 | 0.00584 | 0.00175 |
| excursions | 0.00269 | 0.00142 | 0.00248 | 0.00114 |
| lasting exits | 0.00011 | 0.00040 [0.00027, 0.00050] | 0.00010 | 0.00016 [0.00006, 0.00026] |
| first exits | 0.00161 | 0.00127 | 0.00180 | 0.00089 |
| repeat exits | 0.00639 | 0.00223 | 0.00671 | 0.00221 |
| back inside within 13 weeks | 0.950 | 0.830 | 0.936 | 0.903 |
| within-train split, all exits | 0.00832 | 0.00343 | 0.00642 | 0.00360 |

Spread of h-week changes, middle group, full-window universe, data /
simulation at h = 13, 26, 52, 78. Robust (MAD²): comments extension 0.181 /
0.300, 0.266 / 0.420, 0.417 / 0.574, 0.671 / 0.677; long submissions 0.081 /
0.117, 0.144 / 0.177, 0.275 / 0.274, 0.464 / 0.352. Variance: comments
extension 0.385 / 0.417, 0.521 / 0.579, 0.704 / 0.785, 1.036 / 0.923; long
submissions 0.236 / 0.250, 0.337 / 0.364, 0.508 / 0.542, 0.751 / 0.678.

**What the measurements show.**

1. The departure deficit is not a deficit of sudden lasting exits. They are
   1.2–1.4% of the data's events, and the simulation makes 1.6 to 3.6 times
   as many as the data has.
2. Slides are 64–69% of the data's events and 76–79% of the shortfall. Four
   in five data events are repeat exits. First exits are 1.3 times the
   simulation's on comments and 2.0 times on submissions; repeat exits are
   2.9 and 3.0 times.
3. On comments the deficit is as large inside the training window (2.42×)
   as in the recorded split (2.29×). On submissions it falls from 2.74× to
   1.79×.
4. The data spreads faster with horizon. The simulation's variance is
   within 12% of the data's to 52 weeks and 10–21% short at 78 in every
   rank group on both panels. In the robust measure the simulation is 23–66%
   too wide at 13 and 26 weeks and then nearly stops growing, while the
   data's grows about in proportion to the horizon.
5. An endpoint's own changes are fat-tailed at every horizon measured (at
   least 20 seed SDs above the simulation in the middle group at each one).
   The simulation's are 0.06–0.14 above the Gaussian-walk value at one
   week; the data's are 0.30–0.59 above. With endpoints pooled instead, the
   one-week gap shrinks from 31% to 14% on FB and from 23% to 6% on
   submissions, and is unchanged on comments (13%, 14%).
6. Big drops are kept 0.04–0.11 more than simulated after 13 weeks on every
   panel; big rises are not kept more on any.

**Predictions (DESIGN.md), scored.** A1: (1) one-week excess of 5–20% on all
three panels: direction HELD, size FAILED on two (31%, 23%, 13%). (2) Excess
at 13 weeks at least as large as at one week on FB and comments: HELD on
comments (0.22 → 0.27), FAILED on FB (0.53 → 0.27, then 0.25 at 26 weeks);
simulation within 0.10 of the Gaussian-walk value at 13 weeks: HELD. The
stated kill condition, an excess gone by about 4 weeks, was not met on any
panel. (3) Big moves keep at least 0.05 more in the data on two panels: HELD
for drops, FAILED for rises. (4) Drops kept more than rises in the data on
FB and comments, and no asymmetry in the simulation: HELD on FB, FAILED on
comments; the simulation has the opposite asymmetry on all three panels,
which was not anticipated. A2: (1) recorded rates reproduce: HELD, and the
data rate equals `exit_audit.score_matrix` exactly. (2) Slides more than
half of events: HELD. (3) At least 60% of the shortfall in slides plus
lasting exits: HELD, all of it slides. (4) Lasting exits under-produced by
more than excursions: FAILED on both panels; they are over-produced. (5)
Within-train ratio smaller and above 1.3: HELD on submissions, FAILED on
comments. (6) Spread at 78 weeks over 26 larger in the data: HELD (robust
2.52 against 1.61 and 3.21 against 1.98; variance 1.99 against 1.59 and
2.23 against 1.87).

**Verdict.** No adoption rule applies. For the plan: candidate C-T (fat
tails in the lasting shocks) loses the departure argument it was proposed
on, since the data has fewer sudden lasting exits than the simulation. Its
remaining support, tails that persist with horizon, would also follow from
an endpoint's volatility changing over time; V2 plants both. Findings 2 and
4 point at the lasting level and at reversion to a fixed home: in the data
an established endpoint can move to the boundary and stay there. With §2z-at
(κ at its slowest grid value in a third to a half of bands) this makes the
form of the lasting layer the leading question for decision point D1.
Finding 5 is consistent, on FB and submissions, with the conjecture of
§2z-as that the amplitude spread s absorbs tail weight that sits inside
endpoints; it is not established.

**Not shown.** Whether slides are slow drift or one step down followed by
flicker at the boundary: the classes count events, and one relocation makes
many. Whether the persistent tails are lasting shocks or changing
volatility. Why the simulation keeps rises more than drops (possibly
endpoints that stay down leaving the recorded range; not checked). Small
moves on the two Reddit panels show a steady loss of ground by the middle
group relative to the panel mean, four to five times the simulation's;
recorded as an observation beside §2p.

**Not run.** Any gate evaluation; any model change; FB in A2 (no scoring
window at 86 weeks); the 104-week horizon. Follow-up measurements are
designed separately as A7. **Files.** `llm_fitting/residual_anatomy.py`,
`tests/test_residual_anatomy.py`, `llm_fitting/phase1_anatomy.py`;
`llm_fitting/runs/2026-10-12_A1A2_anatomy/` (DESIGN.md, cells, logs,
report_tables.txt, gaussian_walk_benchmark.json, REPORT.md). Commands:
`python llm_fitting/phase1_anatomy.py a1 <panel>`, `a2 <panel>`, `report`,
`benchmark`.

**Correction, 2026-10-12, same session, after the entry above was committed
(07fe478).** Finding 5 and the A1 result table compare the data's tail ratio
with the simulation's at every horizon. At long horizons that comparison
overstates the case. The statistic scales each endpoint's overlapping h-week
changes by that endpoint's own robust scale, and for a series that wanders
it is higher than for one that reverts, with Gaussian shocks in both: the
"walk" column of the table rises from 1.59–1.63 at one week to 1.76–1.83 at
the longest horizon, while the reverting simulation stays at 1.63–1.65.
Whether the data wanders is the open question, so the walk column is the
conservative yardstick. Against it the data's excess is 0.59, 0.44, 0.20
and 0.07 on FB at 1, 4, 13 and 26 weeks; 0.30, 0.32, 0.21 and 0.03 on
comments at 1, 4, 13 and 52 weeks; and 0.54, 0.61, 0.53 and 0.30 on
submissions at the same four. Finding 5 should therefore read: an
endpoint's own changes are fatter-tailed than any Gaussian benchmark at
short horizons on all three panels, and at long horizons on submissions
only; on FB and comments the long-horizon excess lies between 0.03–0.07
and 0.21–0.25 depending on how far the data reverts. A1 prediction 2 keeps
its scores as stated (they were written against the simulation), with this
caveat on what the comments result means. The verdict's sentence on C-T is
strengthened, not reversed: its remaining support is now submissions only.
A version of the statistic that does not depend on reversion (each
endpoint's long-horizon changes scaled by its own one-week scale) is part
of the follow-up design A7. `runs/2026-10-12_A1A2_anatomy/REPORT.md` is
corrected in place, with the correction marked.

## 2z-av. 2026-10-12 — EXPLORATORY MEASUREMENT (work plan A3): the head-share error on the two Reddit panels is the first-week seeding of the simulation, sign change included; on Facebook and Instagram it is rare spike weeks from the passing shocks, and the data's own far tails are heavier than the simulation's, so a bound on the standardized shock is not licensed. Seven of twelve predictions held in full, two in part, three failed; nothing adopted, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; design,
predictions and runner committed at 3db30e6 before the first cells, the
addendum (far tail, bounded shocks) at 7c53c4b before its cells; results
committed with this entry._

**Tier.** Exploratory. Interventions in each panel's fitted structure stack
on DEVELOPMENT data, in sample, 20 seeds, same seeds in every arm. Not a
refit and not a candidate comparison. Extends §2z-w, §2z-x and §2z-y (FB)
to four panels and runs the seeding change recommended in §2o and never
run.

**Question.** Open residual 2: why the simulated top-1 share is too high on
FB, Instagram and comments and too low on long submissions.

**Result.** Simulated top-1 share within the recorded top 2,000 as a
multiple of the data's.

| arm | FB Era A | Instagram rescue | comments | long submissions |
|---|---|---|---|---|
| data, top-1 share (first week alone) | 0.0170 (0.0245) | 0.0352 (0.0302) | 0.1057 (0.1361) | 0.0724 (0.0383) |
| base (seed SD) | 2.78 (0.61) | 3.66 (0.89) | 1.38 (0.49) | 0.75 (0.16) |
| Gaussian passing shocks | 1.60 | 2.03 | 1.12 | 0.68 |
| passing component off | 1.42 | 1.91 | 1.18 | 0.69 |
| amplitude off | 1.93 | 2.54 | 1.26 | 0.66 |
| homes frozen | 3.04 | 3.20 | 1.35 | 0.63 |
| seeded from the time-median ladder | 2.54 | 3.47 | 0.87 | 1.08 |
| seeded from endpoint means | 2.36 | 4.18 | 1.00 | 0.97 |
| endpoint means and Gaussian shocks | 1.51 | 2.78 | 1.23 | 0.88 |
| passing shocks bounded at 12 / 8 / 6 SDs | 2.26 / 1.96 / 1.82 | 2.94 / 2.35 / 2.08 | 1.36 / 1.35 / 1.35 | 0.69 / 0.68 / 0.67 |

Share of the summed weekly top-1 share from the 5% of weeks where it is
highest: data 0.084, 0.086, 0.076, 0.093; base 0.408, 0.347, 0.107, 0.191;
passing component off 0.077, 0.106, 0.074, 0.089. Share of one-week changes
beyond 6 of the endpoint's own robust SDs, middle group, data / simulation:
3.0e-3 / 1.2e-4, 1.3e-3 / 2.4e-4, 1.2e-3 / 8.2e-5, 3.2e-3 / 2.3e-4. Largest
one-week rise in log units among the top 2% of K: 1.88 / 2.10, 3.12 / 4.17,
2.46 / 2.06, 2.27 / 1.56. Largest rise anywhere on the list, data against
the simulation's largest over 20 seeds: 9.5 / 35.7, 9.7 / 60.6, 9.8 / 15.4,
14.5 / 22.7.

**What the measurements show.**

1. On comments and long submissions the head-share error is the seeding.
   Homes are set to the first week's sorted observed values; that week's
   top share is 29% above the window mean on comments and 47% below it on
   long submissions. Seeding from sorted endpoint means brings the
   simulation to 1.00 and 0.97 of the data (paired differences of 3.5 and
   5 standard errors).
2. On FB and Instagram seeding is a small part (FB 24% of the excess at 2.0
   standard errors with endpoint means, 14% with the time-median ladder;
   Instagram 7% with the time-median ladder). The endpoint-mean arm is not
   valid on Instagram, where means over observed weeks are biased by
   censoring.
3. On FB and Instagram the excess is carried by the passing shocks through
   spike weeks: 76% and 66% removed with the passing component off; 35–41%
   of the simulated top-1 share comes from 5% of weeks against 8–9% in the
   data. With those weeks taken out the base arm is still 1.70 (FB) and 2.5
   (Instagram) times the data, a remainder this task does not attribute.
4. The data's far tails are heavier than the simulation's in an endpoint's
   own units, in every rank group on every panel (5 to 25 times as many
   changes beyond 6 robust SDs in the middle group), including the top 2%
   on three of four panels. A bound at 8 innovation SDs would give 70–80%
   of the Gaussian arm's reduction on FB and Instagram, and the data does
   not support it.
5. What differs is raw size. The largest log-unit rises among the top 2%
   match; across the whole list the simulation's worst seeds contain rises
   of 36 (FB) and 61 (Instagram) log units against 9.5–9.7 in the data.

**Predictions (DESIGN.md), scored.** (1) Base 2.3–3.2× on FB, above on
Instagram and comments, below on submissions: HELD. (2) FB Gaussian shocks
remove more than half: HELD (66%, equal to §2z-w). (3) Endpoint-mean
seeding moves all four toward the data: HELD on FB, comments and
submissions, FAILED on Instagram. (4) Time-median ladder moves FB most:
FAILED. (5) Frozen homes lower the share everywhere: FAILED on FB. (6)
Amplitude off removes at least 40% on FB: HELD (48%). (7) No single arm
within 25% on all four: HELD; endpoint means with Gaussian shocks within
50% on FB: FAILED (1.51). (8) Among large endpoints the data's largest
standardized rise is smaller than the simulation's: FAILED on FB, comments
and (head group) Instagram. (9) Shell: at least as many changes beyond 6
robust SDs in the data: HELD. (10) FB bounds at 8 and 6 give half and three
quarters of the Gaussian reduction: HELD (70%, 81%). (11) FB top-5%-weeks
share below 0.25 at a bound of 8: HELD. (12) Long submissions unmoved by a
bound at 12: HELD.

**Verdict.** No adoption rule applies. Open residual 2 splits in two. (a) A
seeding artifact, which accounts for comments and long submissions and for
the difference in sign: repairable with no parameter by seeding the
unconditional simulation from the ladder of lasting levels. That changes a
default of the structure simulation and every structure card, so it is the
owner's decision; it does not touch the gate, whose four primary stacks
simulate from the filtered state. (b) Spike weeks on FB and Instagram. The
work plan's candidate "passing shocks bounded at the largest standardized
move in the data" would not bind, because the data's largest standardized
moves exceed the simulation's; what the data constrains is the raw size of
a jump given where the endpoint sits. Testing a raw-size restriction needs
an opt-in change in the simulator (Phase 2).

**Not shown, not run.** The source of the steadier part of the FB and
Instagram excess. A raw-unit bound. Seeding and bounding together. The
effect of any arm on the structure cards. A lasting-ladder estimate that is
valid under censoring. **Files.** `llm_fitting/phase1_a3.py`;
`llm_fitting/residual_anatomy.py` (`far_tail`);
`llm_fitting/runs/2026-10-12_A3_head_share/` (DESIGN.md, cells, logs,
report_tables.txt, REPORT.md). Commands: `python llm_fitting/phase1_a3.py
run <panel>`, `run_b <panel>`, `report`.

## 2z-aw. 2026-10-12 — EXPLORATORY MEASUREMENT (A7, follow-ups to A1 and A2; not in the work plan as adopted): year over year an endpoint's level moves like a random walk where the simulation's reverts; the Reddit panels have an endpoint-specific annual cycle the model lacks; 2.5–3.8 times as many established endpoints relocate to the boundary as simulated; volatility is not fixed over time, and long-run and short-run volatility are not one amplitude. Twelve of sixteen predictions held in full, three in part, one failed; nothing adopted, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; design,
statistics and tests committed at 38bb5a6 before the first five cells; a
design addendum at baad58a before four more cells; results committed with
this entry._

**Tier.** Exploratory diagnostic on DEVELOPMENT data. In sample, except the
relocation counts, which use the train-only cohort and scoring windows of
§2z-ac and §2z-ae P7. The task was designed after §2z-au was seen, so it is
post hoc as a set; each prediction was written before its cell ran. One
group of values was seen before its prediction could be written and is
labelled so below. The planted-mechanism study (Codex, V2) has not yet
shown which of these statistics separate the candidate mechanisms.

**Question.** What §2z-au left open: whether slides are drift or steps;
whether the lasting level reverts; whether an endpoint's volatility is one
fixed amplitude; and the tails on a yardstick that does not depend on
wandering.

**Method.** New statistics in `llm_fitting/residual_anatomy.py`, each with
a known-answer test: an anchored scale in `tails_by_horizon`, `cohort_fate`,
`momentum`, `volatility_persistence`, `amplitude_coherence`. Same fits and
seeds as §2z-au (20; 30 for relocation); the comments extension over its
full 213 weeks uses the A2 full-window fit.

**Result, direction.** Slope of a later change in an endpoint's block-mean
level on an earlier one, group mean removed, middle group, data /
simulation (seed SD). A random walk gives +0.25 for adjacent changes and 0
otherwise.

| | whole-year blocks, adjacent | whole-year, one apart | 13-week blocks, half a year apart | one year | two years |
|---|---|---|---|---|---|
| long submissions | +0.313 / −0.011 (0.026) | +0.007 / −0.206 (0.024) | −0.033 / −0.093 | +0.119 / −0.046 | +0.077 / −0.029 |
| comments extension | +0.279 / −0.118 (0.011) | +0.071 / −0.228 (0.018) | −0.110 / −0.145 | +0.182 / −0.045 | +0.146 / −0.020 |
| comments | | | −0.053 / −0.134 | +0.101 / −0.040 | +0.096 / −0.019 |
| FB Era A | | | −0.038 / −0.148 | +0.010 / −0.042 | |

**Result, relocation.** Cohort with train permanent rank ≤ K/2, main
split, data / simulation: share relocated to the boundary zone
(scoring-window permanent rank beyond 0.75 K, still present) 0.049 / 0.020
on the comments extension and 0.069 / 0.018 on long submissions (0.047 /
0.019 and 0.049 / 0.019 in the within-train split); shift in log permanent
rank, median +0.089 / −0.001 and +0.134 / +0.035, 95th percentile 0.97 /
0.60 and 1.06 / 0.68; first exit sudden and lasting 0.054 / 0.298 and 0.053
/ 0.167; exits per exiting endpoint 4.97 / 2.76 and 4.74 / 3.49; share of
the cohort gone 0.007 / 0.030 and 0.012 / 0.011.

**Result, amplitude and tails.** Middle group, data / simulation, FB,
long submissions, comments. Correlation of an endpoint's volatility between
the two halves of the panel over that between interleaved weeks: 0.802 /
0.948, 0.817 / 0.978, 0.847 / 0.950. Correlation across endpoints between
long-run (13-week) and short-run volatility, sampling noise divided out:
0.849 / 0.996, 0.846 / 1.000, 0.844 / 0.994 (comments extension 0.831 /
0.995; at 26 weeks 0.77–0.80 / 0.99–1.00). q99/q90 of h-week changes over
the endpoint's own one-week scale at the longest horizon: 2.09 / 1.64, 2.14
/ 1.61, 2.19 / 1.61 (Gaussian 1.57).

**What the measurements show.**

1. Year over year, an endpoint's level moves like a random walk on both
   four-year panels; the simulation's reverts.
2. The three Reddit panels have an endpoint-specific annual cycle: the
   slope is positive at one and two years in every rank group and negative
   at half a year in all but one. The simulation has none. Seasonality was
   listed as not yet examined. The fit's long-difference moments at 26 and
   52 weeks straddle the cycle, which reads as reversion; how much of the
   fitted reversion is the cycle is not measured.
3. Established endpoints relocate to the boundary 2.5 and 3.8 times as
   often as simulated, and the cohort as a whole loses about a tenth in log
   rank between windows where the simulation's does not. Steps that stick
   are rare in the data (5% of first exits) and common in the simulation
   (17–30%).
4. On comments the simulated cohort is outside the top-K as often as the
   data's, by dying (3.0% against 0.7%) where the data's relocate.
5. An endpoint's volatility is not fixed over time (80–85% of the
   interleaved correlation survives between halves, against 95–98%).
6. Long-run and short-run volatility are mostly, not wholly, one amplitude
   (0.83–0.85 against 0.99–1.00, on four panels). The statistic reads
   0.03–0.04 high at 13 weeks when two amplitudes exist.
7. On the anchored scale the tails are fat at every horizon on every panel
   and grow with horizon on comments. Findings 5 and 6 are possible
   sources, with rare large lasting moves; not separated here. This
   replaces the reading of long-horizon tails in §2z-au and its correction.

**Predictions (DESIGN.md), scored.** a1 anchored and entity scales agree at
one week: HELD. a2 anchored excess at the longest horizon: HELD. a3 smaller
than at one week on FB and comments: HELD on FB, FAILED on comments. b1
relocated share at least twice the simulation's: HELD. b2 median shift
larger by under 0.10 and 95th percentile by at least 0.15: HELD. b3 slides
a larger share of first exits: HELD. b4 time outside at least 1.5 times:
HELD on submissions, FAILED on comments. c1 simulation's 13-week skip slope
negative: HELD. c2 data's higher: HELD. c3 data's 26-week skip slope within
±0.10 on Reddit: FAILED (+0.126, +0.118), which is what exposed the annual
cycle. d1, d2 volatility ratio: HELD. Addendum, with the data's whole-year
and multi-spacing values already seen and the simulation's not: e1
simulation reverts with whole-year blocks: HELD. e2 simulation has no
positive slope at one year: HELD. f1 simulation's amplitude figure at least
0.85: HELD. f2 data's lower by at least 0.15: HELD on submissions (0.154),
short on FB (0.147) and comments (0.1499).

**Verdict.** No adoption rule applies. For decision point D1: (i) reversion
to a fixed home is contradicted at the scale of a year and is the
simulation's shortfall in relocation, so with §2z-at the form of the
lasting layer is the leading question; (ii) before any candidate for it,
one measurement is owed, a refit with the annual cycle taken out of the
long-difference moments, on data and simulation through one filter; (iii)
candidate C-A (one amplitude for the whole model) is not what the data
shows, consistent with §2z-as; (iv) the simulated exit hazard for
established endpoints on comments is too high. The claim language around
b = 1 (equal spread of the two amplitudes is not one amplitude) is the
owner's to revisit; nothing is changed here.

**Not shown, not run.** How much of the fitted reversion is the cycle.
Whether the cohort's loss of ground is newer endpoints rising. FB at a
year. Any gate evaluation, any model change. **Files.**
`llm_fitting/residual_anatomy.py`, `tests/test_residual_anatomy.py`,
`llm_fitting/phase1_anatomy.py`; `llm_fitting/runs/2026-10-12_A7_followups/`
(DESIGN.md, cells, logs, report_tables.txt, two posthoc_momentum JSON
files, REPORT.md). Commands: `python llm_fitting/phase1_anatomy.py a7
<panel>`, `a7b <panel>`, `a7c <panel>`, `report7`.

**Addendum, 2026-10-12, same session: a first, data-only answer to verdict
item (ii).** Post hoc and descriptive; no refit. Variance of h-week changes
(full-window universe, as in §2z-au) against the straight line through its
values at 13 and 52 weeks, and the same after subtracting the annual
cycle's share, 2S(1 − cos(2πh/52)), with the cycle's variance S estimated
from the covariance of quarterly block-mean changes one year apart
(`runs/2026-10-12_A7_followups/posthoc_cycle_variance.json`).

| | 26 weeks against the line | cycle's share removed | 78 weeks against the line | cycle's share removed |
|---|---|---|---|---|
| long submissions: head / middle / shell | +0.010 / +0.010 / +0.018 | +0.003 / −0.009 / −0.017 | +0.006 / +0.062 / +0.087 | −0.009 / +0.024 / +0.016 |
| comments extension | +0.036 / +0.029 / +0.027 | −0.004 / −0.019 / −0.020 | +0.085 / +0.120 / +0.123 | +0.005 / +0.022 / +0.027 |
| comments (136 weeks) | +0.034 / +0.015 / +0.023 | −0.006 / −0.018 / −0.018 | | |
| simulation, long submissions | +0.012 / +0.016 / +0.019 | | −0.048 / −0.059 / −0.067 | |
| simulation, comments extension | +0.029 / +0.039 / +0.042 | | −0.084 / −0.107 / −0.122 | |

The variances themselves are 0.15 to 1.6, so these are departures of a few
percent. In the data the bulge at half a year, which is what a reverting
level produces and what the fit can read as reversion, is accounted for by
the cycle in all nine cells, and with the cycle's share removed the
variance is close to a straight line in the horizon from 13 to 78 weeks: a
random walk plus a constant passing part. The simulation keeps a bulge of
the same size and falls 0.05–0.12 below the line at 78 weeks. The cycle's
variance estimated from changes two years apart is 55–115% of the one-year
estimate, so the pattern is stable over the panel. Caveats: the subtraction
assumes a sinusoid of period one year; the estimate uses 13-week block
means; a refit with the cycle filtered out of the long-difference moments,
on data and simulation alike, is still the stronger test and is not run.
The next entry tests the other side: what the fitted model does with its
reversion switched off.

_Correction to the addendum above, same session: the two-year estimate of
the cycle's variance is 55–140% of the one-year estimate across the nine
cells, not 55–115%; the high values are on the 136-week comments panel,
which has few pairs two years apart._

## 2z-ax. 2026-10-12 — V1 GATE-INSTRUMENT MEASUREMENT (OpenAI Codex; integrated by Claude): the four recorded reference gates reproduce; the gate's error falls 0.008–0.022 from 3 to 20 simulations, 75–107% of it the bias of an absolute error; two 20-simulation evaluations of the same stack differ by up to 0.018–0.032 (95%); the top-1 collision moment carries 50–83% of the simulation noise. Sixteen of sixteen predictions held; no change adopted, no candidate evaluated

_Integration note (Claude Opus 5.5, 2026-10-12): the entry below is Codex's `llm_fitting/handoff/V1_gate_instrument/STATUS_ENTRY.md`, appended verbatim under the two-agent protocol (§2z-ar) after one review round and a health check on the merged tree (research suite 443, package 8, legacy guard 14/15 and 0.013). Its heading in the handoff was: "2026-10-12 — V1 gate-instrument measurement: no change adopted". The heading above is mine and quotes its tables. Its two figures are not in git. Its gate rows are appended to `GATE_LEDGER.md`._

Provenance: OpenAI Codex, 2026-10-12, starting from ed75e9f4f0a95b2f479f1001835410073b6ed0fe; governing skills at that commit. Design and predictions committed in b787012 before any V1 measurement. Exploratory instrument study and frozen-stack reproduction on DEVELOPMENT panels; Claude integrates this handoff under the approved coordination protocol (§2z-ar).

**Question.** How much of the gate’s apparent improvement with more simulations is Monte Carlo bias, how noisy is a comparison, and which moments carry that noise? This measures Gate v1 unchanged, not a candidate or a replacement criterion.

**Execution.** Four recorded references reproduced (§2z-g, §2z-ae, §2z-ap): 3 reps / 400 empirical-bootstrap draws on Facebook Era A, comments and short submissions; 20 / 2000 on long submissions. All five frozen origins per panel, seeds starting at zero. Then 80 fresh seeds (1000–1079), nine train calibration scales and all nine corresponding conditional test forecasts per origin. Full sparse displacement histograms, seed RACFs and collision rates preserve pooled medians. Each simulator call is restartable; every actual gate call is in GATE_ROWS.md.

**Analysis.** 4,000 bootstrap ensembles per rep count 3/5/10/20/40/80 from the same 80-seed bank; shared weights across origins/scales/train/test, with calibration reselected. Two groups of 2,000 ensembles supply same-stack null differences. Actual disjoint 20/40-seed blocks are also reported. Bands below and in REPORT.md condition on the fixed panels, fits and finite bank: they are not temporal, estimation or population uncertainty.

| Panel | Recorded-setting reproduction ± split SD | 80-seed instrument call ± split SD | Historical baseline ± split SD | Coverage: reference → bank | Standing |
|---|---|---|---|---|---|
| Facebook Era A | 0.1228 ± 0.0333 | 0.1154 ± 0.0349 | 0.1453 ± 0.0307 | 60% → 60% | reference reproduced; no new verdict |
| Reddit comments | 0.1713 ± 0.0461 | 0.1429 ± 0.0621 | 0.1648 ± 0.0623 | 60% → 100% | reference reproduced; no new verdict |
| Short submissions | 0.1639 ± 0.0534 | 0.1279 ± 0.0293 | 0.1678 ± 0.0037 | 80% → 60% | reference reproduced; no new verdict |
| Long submissions | 0.1937 ± 0.0606 | 0.1725 ± 0.0644 | 0.1626 ± 0.0479 | 80% → 80% | reference reproduced; no new verdict |

| Panel | 3-rep error ± MC SD | 20-rep error ± MC SD | 3→20 decline | Jensen share of decline | 95% \|same-stack difference\|: 20 / 40 reps |
|---|---|---|---|---|---|
| Facebook Era A | 0.1261 ± 0.0128 | 0.1156 ± 0.0063 | 0.01047 | 84.1% | 0.01766 / 0.01306 |
| Reddit comments | 0.1681 ± 0.0217 | 0.1460 ± 0.0112 | 0.02211 | 103.1% | 0.03029 / 0.02540 |
| Short submissions | 0.1461 ± 0.0152 | 0.1309 ± 0.0080 | 0.01526 | 106.5% | 0.02233 / 0.01710 |
| Long submissions | 0.1833 ± 0.0233 | 0.1755 ± 0.0115 | 0.00785 | 74.6% | 0.03162 / 0.02471 |

Jensen, scale reselection and the score of the mean simulated moments are separately tabulated in REPORT.md and add to the total change. Jensen shares may exceed 100% when another term offsets the decline. The per-moment error and covariance-allocated variance shares, every origin’s resolution estimate and MOM_FLOOR exclusions are retained. Short submissions has only six available moments: h=13 is outside its test-window rule. Calibration itself does not apply MOM_FLOOR.

**Predictions, with failures retained.**

| Prediction | Declared expectation | Measured | Verdict |
|---|---|---|---|
| P1 | all four references reproduce | 4 / 4 | PASS |
| P2 Facebook Era A | positive decline in [0.01, 0.05] | 0.01047 | PASS |
| P3 Facebook Era A | Jensen ≥50% of positive decline | 84.1% | PASS |
| P4 Facebook Era A | SD40 / SD20 ≤0.85 | 0.757 | PASS |
| P5 Facebook Era A | collisions >50%; coll1 largest | 54.4%; coll1 | PASS |
| P2 Reddit comments | positive decline in [0.01, 0.06] | 0.02211 | PASS |
| P3 Reddit comments | Jensen ≥50% of positive decline | 103.1% | PASS |
| P4 Reddit comments | SD40 / SD20 ≤0.85 | 0.825 | PASS |
| P2 Short submissions | positive decline in [0.01, 0.06] | 0.01526 | PASS |
| P3 Short submissions | Jensen ≥50% of positive decline | 106.5% | PASS |
| P4 Short submissions | SD40 / SD20 ≤0.85 | 0.770 | PASS |
| P2 Long submissions | positive decline in [0, 0.06] | 0.00785 | PASS |
| P3 Long submissions | Jensen ≥50% of positive decline | 74.6% | PASS |
| P4 Long submissions | SD40 / SD20 ≤0.85 | 0.791 | PASS |
| P5 Long submissions | collisions >50%; coll1 largest | 75.7%; coll1 | PASS |
| P6 | R0 and R1 rejected on all panels | 8 / 8 | PASS |

**Validation and scope.** Every recorded reference reproduces; the new bank changes only simulation seeds/count and records all calibration possibilities. No adoption is adjudicated. R0 and R1 from the stored ladder are rejected on all four panels by the existing +0.05 error rule; no non-frozen rung was rerun. All 26 adjacent ladder pairs are shown under three illustrative margin rules, with coverage separate. No rule is selected. The ladder did not save per-seed outputs, as Claude confirmed in ANSWERS.md, so its measured split-pairing benefit cannot be interpreted as seed-pairing variance reduction.

**Limitations / not run.** No Instagram optional run, sealed data, WD archive, candidate gate, default change or paper-claim change. Candidate-specific paired resolution and power are not identified by same-stack noise. The empirical-median coverage criterion is not itself resampled. Whole-pipeline and future-time uncertainty are not measured here. No scientific run was killed or substituted. One queued health-check wrapper was cancelled before execution, and one health invocation used zsh on a Bash script and failed after the research suite; successful full Bash checks are retained separately.

**Code and validation.** The two protected model files remain unchanged. New wrappers, analysis, report and plotting code are under validation/. Eleven targeted known-answer tests cover exact pooled histograms, original-simulator parity, cache restart, calibration ties, floor/missing moments, folded-normal bias/noise, covariance allocation and full analysis decomposition. The full check at the latest recorded pre-analysis commit passed 425 research tests + 13 subtests, 8 package tests and the legacy 14/15, churn 0.013 guard. Final handoff health is recorded in HEALTH_FINAL.log. AUDIT.json checks unchanged inputs/fits/empirical moments/source hashes, reconstructs full-bank calibration and scores, and verifies 28,800 seed files with digests.

**Commands and archive.** Run from the assigned Codex copy with the registered Python 3.11 interpreter: `zsh llm_fitting/handoff/V1_gate_instrument/launch.zsh`, then `zsh llm_fitting/handoff/V1_gate_instrument/launch_analysis.zsh`; the launchers use heavy_lock.py and caffeinate. Plotting: `python llm_fitting/validation/gate_instrument_plot.py` with MPLCONFIGDIR inside the run directory. Archive: `llm_fitting/runs/2026-10-12_V1_gate_instrument/`; complete report, ledger rows, predictions and figures: `llm_fitting/handoff/V1_gate_instrument/`. Compact summaries/provenance are committed; full binary seed banks remain in the Codex working copy.

## 2z-ay. 2026-10-12 — EXPLORATORY MEASUREMENT (A8; not in the work plan as adopted): the fitted model simulated with reversion to the home switched off. Direction moves to the data and relocation and departures move about half of the way; the rank-size ladder no longer holds (drift 1.4–3.4 times the data's, FB top-1 share 15 times). The home is doing two jobs. Three of seven predictions held in full, three in part, one failed; not a candidate, nothing adopted, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; design,
predictions and runner committed at 7a16d1a before any cell (a two-seed
smoke test to a scratch folder excepted, as the design says); results
committed with this entry._

**Tier.** Exploratory. An intervention in the fitted model on DEVELOPMENT
data: every parameter as fitted, `simulate(..., kappa=0.0)`. Structure
cells in sample, 20 seeds, same seeds in both arms; departure cells with
the train-only cohort of §2z-au, 30 seeds. Not a refit and not a candidate.

**Question.** §2z-at, §2z-au and §2z-aw point at reversion to a fixed home.
What does the existing model do without it?

**Result.** Data / as fitted / reversion off.

| | comments extension | long submissions |
|---|---|---|
| exits of the established cohort per cohort-week | 0.00800 / 0.00350 / 0.00481 | 0.00851 / 0.00311 / 0.00449 |
| excursions | 0.00269 / 0.00142 / 0.00130 | 0.00248 / 0.00114 / 0.00131 |
| relocated to the boundary zone | 0.049 / 0.019 / 0.033 | 0.069 / 0.018 / 0.034 |
| median shift in log permanent rank | 0.089 / −0.001 / 0.079 | 0.134 / 0.035 / 0.149 |
| first exit sudden and lasting | 0.054 / 0.298 / 0.288 | 0.053 / 0.167 / 0.128 |

| | FB Era A | long submissions | comments |
|---|---|---|---|
| direction, quarterly blocks half a year apart | −0.038 / −0.148 / −0.051 | −0.033 / −0.093 / −0.041 | −0.053 / −0.134 / −0.082 |
| direction, whole-year blocks, adjacent; one apart | | +0.313 / −0.011 / +0.213; +0.007 / −0.206 / +0.020 | |
| variance of 52-week changes, middle group | | 0.508 / 0.542 / 0.690 | 0.773 / 0.819 / 1.019 |
| ladder drift | 0.267 / 0.115 / 0.907 | 0.501 / 0.136 / 1.145 | 0.258 / 0.172 / 0.370 |
| top-1 share over the data's (seed SD) | 2.78 (0.61) → 14.7 (11.5) | 0.75 (0.16) → 4.02 (2.13) | 1.38 (0.49) → 0.99 (0.29) |
| card rows passed; macro index | 15 → 14; 1.09 → 2.67 | 13 → 14; 0.55 → 1.43 | 12 → 12; 0.86 → 0.64 |

**What the measurements show.**

1. Without reversion the direction statistics are at or near the data's.
   The annual cycle stays missing.
2. Exits of the established cohort rise from 0.44 to 0.60 of the data's
   rate (comments extension) and from 0.37 to 0.53 (long submissions);
   relocation from 0.40 to 0.68 and from 0.26 to 0.49. The cohort's loss of
   ground between windows, absent from the fitted model, is reproduced.
3. Excursions, repeat exits and the over-production of sudden lasting exits
   on comments do not move. Reversion is not their cause.
4. The home was holding the ladder: without it ladder drift is 3.4, 2.3 and
   1.4 times the data's and the top-1 share runs away on FB and long
   submissions.
5. As fitted the simulated ladder is too still: its drift is 0.43, 0.27 and
   0.67 of the data's. The data sits between the two arms.
6. The lasting volatility, estimated together with the reversion, is too
   large without it: 52- and 78-week variance overshoots by 27–36% in the
   middle group.

**Predictions (DESIGN.md), scored.** (1) Departures to at least 0.60 of the
data's on both: HELD on the comments extension, FAILED on long submissions
(0.53). (2) Relocated share at least half the data's: HELD on the comments
extension, short on long submissions (0.49). (3) Whole-year slopes within
0.10 of a random walk's: HELD on long submissions; not run on the comments
extension. (4) 78-week variance within 10% of the data's: FAILED (27%
over); overshoot at 52 weeks: HELD. (5) Card loses at most two rows, index
worse on two of three, ladder drift worse on all three: HELD. (6) FB top-1
share changes by less than one seed SD: FAILED (2.8 → 14.7 times the
data's). (7) Sudden lasting first exits stay over-produced on comments:
HELD.

**Verdict.** No adoption rule applies, and setting the reversion to zero is
not a candidate. The home does two jobs: it pulls each endpoint back to
where it started, which the data contradicts at the scale of a year, and it
holds the ladder, which the data needs, though less tightly than the fitted
model has it. A candidate for the lasting layer has to keep a pull on the
ladder and drop the pull on the endpoint, with the lasting volatility
re-estimated, and it will leave the boundary flicker and the deaths of
finding 3 to be explained separately. This is the main input to the
lasting-layer dossier for decision point D1.

**Not run.** Any refit; the comments extension in the structure cells;
Instagram; any gate evaluation. **Files.** `llm_fitting/phase1_a8.py`;
`llm_fitting/runs/2026-10-12_A8_no_reversion/` (DESIGN.md, cells, logs,
report_tables.txt, REPORT.md). Commands: `python llm_fitting/phase1_a8.py
dep <panel>`, `struct <panel>`, `report`.

## 2z-az. 2026-10-12 — EXPLORATORY MEASUREMENT (A3, second addendum): the structure card under two parameter-free seedings on five panels. Both improve the macro index on FB, comments and long submissions with bands excluding zero; the time-median ladder harms no panel; endpoint means are worse on the censored panel. Three of four predictions held; nothing adopted, no gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; design
and predictions committed at b13bf01 before the cells; results committed
with this entry. Continues §2z-av._

**Tier.** Exploratory, in sample, DEVELOPMENT data. An intervention: the
seed ladder is replaced in the fitted parameters of each panel's frozen
structure stack; 20 seeds, same seeds in every arm; card and macro index by
the functions of §2z-ap.

**Result.** Card rows passed; macro index; index minus the as-fitted arm
with a 5th–95th percentile bracket from resampling seeds jointly; simulated
top-1 share over the data's.

| panel | first week (as fitted) | time-median ladder | endpoint means |
|---|---|---|---|
| FB Era A: card; index; index minus base [5%, 95%]; top-1 over data | 15/15; 1.09; 2.78 | 15/15; 0.86; −0.231 [−0.312, −0.161]; 2.54 | 15/15; 0.77; −0.317 [−0.408, −0.235]; 2.36 |
| comments | 12/15; 0.86; 1.38 | 12/15; 0.65; −0.210 [−0.295, −0.125]; 0.87 | 12/15; 0.59; −0.265 [−0.320, −0.164]; 1.00 |
| long submissions | 13/15; 0.55; 0.75 | 13/15; 0.45; −0.105 [−0.141, −0.067]; 1.08 | 12/15; 0.49; −0.061 [−0.088, −0.017]; 0.97 |
| short submissions | 14/15; 0.33; 1.07 | 14/15; 0.29; −0.040 [−0.064, +0.009]; 1.00 | 14/15; 0.39; +0.059 [−0.008, +0.127]; 1.17 |
| Instagram rescue | 8/15; 2.12; 3.66 | 8/15; 2.11; −0.013 [−0.096, +0.057]; 3.47 | 9/15; 2.25; +0.126 [+0.053, +0.197]; 4.18 |

**What the measurements show.** Both seedings lower the index on FB,
comments and long submissions, with brackets excluding zero, and move the
top-1 share toward the data on those panels. The time-median ladder is
neutral on short submissions and Instagram. Endpoint means are worse than
the as-fitted arm on Instagram (bracket excluding zero) and on the 30-week
panel (bracket including zero), and lose one card row on long submissions.
Ladder drift is not moved by seeding.

**Predictions (DESIGN.md), scored.** (13) Card changes by at most one row
on the four uncensored panels under endpoint means: HELD. (14) Index falls
on comments and long submissions and does not rise by more than 0.10 on FB
or short submissions: HELD. (15) Instagram: endpoint means worse, time-median
within 0.10: HELD. (16) Ladder drift nearer the data's on three of four:
FAILED.

**Verdict.** No adoption rule applies; a seeding change is the owner's
decision (it alters the structure-simulation default and every structure
card). Of the two variants, the time-median
ladder is the one this table supports proposing: it improves three panels
and harms none. Choosing between two variants on in-sample cards is a
selection and is stated as such in the decision document
(`llm_fitting/D1_CANDIDATES_CLAUDE_2026-10-12.md`).

**Correction to §2z-av.** Its verdict says the seeding change "does not
touch the gate, whose four primary stacks simulate from the filtered
state". That is true of the gate's forecasts and not of the gate as a
whole: its calibration step on the training window (`_calibrate_scale`)
runs the unconditional cohort simulation, which is seeded from the same
first-week ladder. A seeding change confined to the structure simulation
leaves the gate alone; one made inside the fit would reach the gate
through the calibrated noise scale and would need gate evaluations. Found
by reading the code while writing the decision document; no run depends
on it.

**Not run.** Any gate evaluation; a censoring-aware estimate of the lasting
ladder. **Files.** `llm_fitting/phase1_a3.py` (`run_c`);
`llm_fitting/runs/2026-10-12_A3_head_share/` (cells `a3c__*.json`, logs,
`launch_a3c.zsh`, report_tables.txt, REPORT.md).

## 2z-ba. 2026-10-12 — OWNER DECISION at decision point D1 (partial): the seeding change is approved; candidate 2 (the lasting layer) is NOT approved and goes to a structured debate between Claude and Codex; nothing else decided. No model, stack or gate change yet

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12. Records
the owner's reply to `llm_fitting/D1_CANDIDATES_CLAUDE_2026-10-12.md`._

**The owner's words.** "definitely do the seeding change." And on
candidate 2: "I'm not sure about proposal 2, pulling to its current rank
rather than a supposed home rank ... this means that the rank structure
cannot emerge organically from the micro-level behavior, correct, which we
would ideally like? If I want to publish this in PNAS, is this shift to
pulling toward the current rank the right decision? ... we want to
illustrate deep causes not just produce shallow predictions already baked
into our modeling assumptions. give the prompt to Codex too to consider
this and debate until you reach consensus."

**How the seeding approval is read** (Claude's reading, stated to the owner
the same day): an opt-in option of the structure simulation that seeds the
homes from the time-median ladder (§2z-az), defaults unchanged and
bit-identical with the option off; not applied in the gate's calibration
step, so no gate evaluation is spent; a re-freeze of the structure stacks
to use it remains the owner's separate decision. Not yet implemented at
this entry.

**Candidate 2.** Not approved. The question, a neutral evidence sheet and
the rules of the debate are in `llm_fitting/handoff/D1_debate/`. Claude's
first position was written before Codex's and is held back until Codex's
is committed; its SHA-256 is in `CLAUDE_1.sha256`. No model code, fit,
simulation or gate evaluation belongs to the debate.

**Still open from the decision document.** The comparison margin; whether
the 80-seed values become the recorded reference gates; the wording around
b = 1; candidates 3 and 4.
