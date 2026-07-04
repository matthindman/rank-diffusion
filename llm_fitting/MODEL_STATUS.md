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
