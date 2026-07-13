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
