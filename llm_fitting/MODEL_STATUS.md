# Rank-Diffusion Model — Status & Locked-In Results

_Canonical record of the validated FB / Reddit results and the model as written in code._
_Last updated: 2026-06-12._

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
| **OOS movement** (calibrated σ_obs, train-only) | **PASS** — err **0.081** ≈ persistence 0.070 | partial — err 0.45 (was 0.76); persistence 0.045 |
| home-drift evidence (Kalman LR drift>fixed) | strong (213–594) | weak-but-consistent (6–63) |

**OOS movement test is the acceptance gate** (estimate on first ~67% of weeks; predict held-out
rank-displacement + collisions; never use test data in estimation). FB passes. Reddit is improved
by the σ_obs calibration but still over-predicts top displacement ~1.7× — its dense ladder
(level spread 2.54 vs FB 1.51; within/level movement 0.30 vs 0.61) amplifies any residual noise,
and T=30 limits estimation precision. **Resolve with the longer-horizon Reddit panel.**

### Calibrated observation noise (honest, transparent)
`σ_obs` is the lever on observed rank movement; the pooled change-autocovariance over-states it
~2× for clean top entities. We therefore treat σ_obs as a **calibrated measurement parameter**:
fit a single `sigma_obs_scale` on **training** short-horizon displacement, then **validate on
held-out** movement. Calibrated scale = **0.35** for both platforms. This is calibration, not a
fudge: the target (train displacement) and the validation (test displacement) are different data.

## 3. The two corrected estimation pitfalls (do not regress)

1. **Band-alignment bug (fixed, committed):** `mean_rank` is sorted but entity columns were not —
   rank-band masks selected the wrong entities, flattening all rank curves. Fixed via `mean_rank_ids`;
   locked by `tests/test_rankdiff_regressions.py`. (FB 15/15→14/15 on the v4.3 model after the fix —
   the drop was real, the bug had hidden it.)
2. **Current-rank (Eulerian) estimation is selection-biased:** conditioning on current rank
   oversamples transient spikers → inflates σ ~3× → runaway diffusion. **Always estimate by
   permanent (time-averaged) rank.**

## 4. Known limitations / open items
- **σ_obs identification** is THE crux. Calibrated for now; identify organically next (daily-within-week
  variance — but note the daily model needs heavy DoW/ToD damping; or replicate measures once Reddit
  comment data is merged — current `metric_value` = submission_karma only).
- **Reddit** OOS movement not fully passing (short panel + dense ladder). Needs the longer panel.
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
