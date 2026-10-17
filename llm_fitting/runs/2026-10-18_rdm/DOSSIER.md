# The moving-home (rank-drift) model: dossier, written before any code or result

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18. Owner's request:
"build and validate that model. be ambitious … No new data yet: just the
current data." Development data only. Record: MODEL_STATUS §2z-dd.
Gate runs are logged in `GATE_LEDGER.md`. Nothing here changes a default or
a frozen stack: the model lives in a new module (`llm_fitting/rankdiff_rdm.py`)
and imports the gate's scoring code unchanged._

## 1. The residual it addresses, measured

1. **Model A's "permanent" component does not last.**
   - On Facebook pages (frozen movement-stack estimator, run this session):
     σ_perm² is 0.02–0.12 a week with home pull κ ≈ 0.1–0.2 a week, a
     half-life of weeks, at most knots.
   - B's lasting variance at the same ranks is 0.011–0.018 a week (§2z-da).
   - So A's "home" holds a medium swing and the whole ladder at once; this is
     the "home doing two jobs" of §2z-ay.
2. **The data's levels do not return to an endpoint's own level at a year**
   (§2z-bu, §2z-aw). A's simulated levels revert.
3. **A's stationary head is too concentrated** (§2z-d, §2z-q E5).
4. **A's upper movement tails are under-dispersed, and Gaussian lasting
   movement under-forecasts who stays** (§2z-q, §2z-cy, §2z-cz).
   Block-scale jumps reproduce the under-forecast (§2z-cz).
5. **Lasting volatility barely depends on size,** β 0.01–0.08 (§2z-dc). A
   models it with about 55 knot values of σ_perm.

## 2. The model

X_it = L_it + m_it + ξ_it + ε_it, the log activity of endpoint i in week t.

| part | dynamics | parameters | identified by |
|---|---|---|---|
| ε measurement noise | iid, σ_obs(z) | A's (Spec-B on Facebook; otherwise MD plus the gate's one train-calibrated scale) | the daily-replication floor or short-lag MD, unchanged |
| ξ fast swing | AR(1), φ(z), σ_tr(z), Student-t | A's | short-lag MD, unchanged |
| m medium swing | OU toward 0, pooled rate κ_m, variance V_m(z) by band | 1 + 8 | the curvature of the structure function D(h) |
| L lasting level | dL = g(rank of L) + σ_L(L)·√(1−π)·N(0,1) + jumps | see below | slope of D(h) at long h; kurtosis and tail of long changes; the ladder or the slides |
| — size law | σ_L(L) = σ_0 exp(−β (L − L_K)) | σ_0, β | the slope of D(h) by band, regressed on band level |
| — jumps | weekly probability λ, N(0, σ_J²), carrying a share π of σ_L² | λ, π | excess kurtosis and the share beyond 3 sd of standardised long changes |
| — drift by current rank | g(r) | hold: none free; move: band slides | hold: the train ladder (Atlas balance s_k = σ_k²/2S_k); move: lasting slides of permanent-rank cohorts (§2z-cd, §2z-cm) |
| entry and exit | A's exit hazard by rank, rebirth from the bottom levels | A's | unchanged |

**What changes from A.**
- A's fixed personal home becomes a *moving* lasting level. It holds the
  ladder through drift by rank, as in hybrid Atlas models (Ichiba et al.
  2011), not through reversion.
- A's home component becomes the medium swing around it.

**Count of parameters.** A's κ(z) (about 55) and σ_perm(z) (about 55) are
replaced by:
- κ_m (1);
- V_m (8 bands);
- σ_0 and β (2);
- λ and π (2);
- drift: none (hold) or 7 band slides (move).

## 3. Estimation, from declared moments only (train window in the gate)

1. **Fast parts.** A's estimator with the frozen movement-stack flags gives
   σ_obs, φ, σ_tr, t_df, temperament, exit and rebirth.
2. **Structure function.** For each of 8 bands of permanent rank, the
   structure function D_b(h) = Var(X_{t+h} − X_t) over persisting endpoints,
   for h = 1 … H, with H = min(52, ⌊T_train/2⌋).
3. **Residual.** Subtract the known parts:
   r_b(h) = D_b(h) − 2σ_obs² − 2σ_tr²(1 − φ^h)/(1 − φ²).
4. **Medium and lasting fit.** Fit V_m,b(1 − e^{−κ h}) + s_b h by
   non-negative least squares for each κ on a grid, and choose κ_m by total
   squared error.
5. **Size law.** WLS of log s_b on band mean level gives σ_0 and β.
6. **Jumps.** Standardised changes over h_J = min(26, H) weeks of 4-week
   block means give excess kurtosis K and the share beyond 3 sd. The pair
   (λ, π) of a compound-Poisson-plus-diffusion mixture is matched to them on
   a grid, allowing for the swing share of those changes. If K < 0.3, there
   are no jumps.
7. **Drift.**
   - *Hold:* the slope S_k of the end-of-train ladder; the net slide
     s_k = σ_L²(k)/(2S_k); the stratum drift from k·s_k.
   - *Move:* lasting slides of permanent-rank cohorts. This needs ≥ 2 years,
     so it is used for structure on the long panels only.

## 4. Validation (fixed now)

### Gate v1

- **Panels:** `facebook_a` (K 3,500, Spec-B), `reddit_comments` (K 12,500)
  and `reddit` (K 5,000).
- **Settings:** 5 splits, 20 reps, 2,000 bootstrap draws, train-only
  membership, the conditional start from the filtered end-of-train state
  (with the gate's step-0 timing replicated), and the same σ_obs scale
  calibration on the train moment vector.
- **Variant:** hold.
- **Reference:** the frozen movement stacks rerun at the same reps.

### Structure on the long panels

- **Panels:** `reddit_comments_ext`, `reddit_submissions_long`, `facebook_a`.
- **Settings:** estimated on the whole panel (in-sample, labelled so); a
  conditional start from the filtered state at the end of the first block;
  the rest of the panel simulated; 20 seeds.
- **Compared:** RDM-move, RDM-hold, RDM without jumps, and A's dynamics
  (fixed home, the same simulator with the switch). All go through the
  *same* estimators as the data.

### Recovery (ADEMP)

Panels simulated from the RDM with known parameters, then put through the
estimator, 10 replications.

## 5. Predictions (scored in §2z-dd)

| | prediction |
|---|---|
| G1 | RDM gate error ≤ A's + 0.05 on each of the three panels (non-inferiority, the registered convention) |
| G2 | RDM's error at h = 13 (dRank13) is below A's on at least two of three panels. Without a fixed home, the long-horizon spread grows as the data's does |
| S1 | No return to own level: the slope of year-2 change on year-1 deviation from the endpoint's own year-0 level lies within the data's 90% interval for RDM, and outside it (toward reversion) for A |
| S2 | One-year retention of the top K/64, K/8 and K within ±0.05 of the data for RDM on each ladder |
| S3 | Raw size–variance β of simulated panels within ±0.05 of the data's (§2z-dc) for RDM |
| S4 | Arithmetic share of the top K/64 at the end of the span within ±5 points of the data for RDM-move. A misses it (too concentrated) |
| S5 | Two-year retention of the top K/16 within ±0.05 for RDM with jumps. Without jumps it is below the data (under-forecast) |
| S6 | Boundary ratio of RDM simulations, pooled over K/8 … K, inside [0.85, 1.18] |
| S7 | Excess kurtosis of 26-week changes of block means within ±50% of the data for RDM. A's is lower |
| R1 | Recovery of β within ±0.03, σ_0 within ±15%, π within ±0.15, κ_m within ±30% |

**Must not worsen** (against A): the gate (G1), head share, VR13, the
departure rate.

**Recommendation rule** (skill §"Improving the model"): recommended to the
owner only if G1 holds on every panel, the targets above hold where they
apply, and no parameter lacks an identifying moment. Ties go to the simpler
model. Adoption or re-freeze is the owner's.
