# Rank-Diffusion Model — Status & Locked-In Results

_Canonical record of the validated FB / Reddit results and the model as written in code._
_Last updated: 2026-07-02 (top-coverage universe; supersedes parts of §2 — see §2b)._

## 1. Unified model (one structure, platform-specific parameters)

Observed log-activity of endpoint *i* at week *t*:

```
X_it = h_it + ξ_it + ε_it
  h_it = ρ_perm · h_i,t-1 + η_it      permanent "home" (level)   η ~ N(0, σ_perm²)
  ξ_it = φ · ξ_i,t-1     + ν_it       transitory activity (AR1)   ν ~ N(0, σ_trans²)
  ε_it ~ N(0, σ_obs²)                 iid measurement noise
```
- **Rank** each week by the *observed* `X = h + ξ + ε`.
- **Gabaix rebirth** at the bottom: rank-dependent exit; re-entry near the lower tail (keeps the
  distribution stationary, reproduces bottom churn).
- **Common factor** removed by per-period de-meaning (platform-wide moves preserve relative ranks).
- **Rank-dependence**: `(ρ_perm, σ_perm, φ, σ_trans, σ_obs)` vary by **permanent-rank band**
  (each entity binned by its *time-averaged* rank — Lagrangian, immune to current-rank selection bias).
- `ρ_perm = 1` ⇒ random-walk home; `ρ_perm < 1` ⇒ persistent reverting home; `σ_perm = 0` ⇒ fixed home.
  Empirically **fixed home ≈ slowly-drifting home over these horizons** — use the simpler fixed home;
  evolving home is a long-horizon refinement, not required for the current windows.

This is one generative law. FB and Reddit differ in **parameters and measurement regime, not theory**.

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
