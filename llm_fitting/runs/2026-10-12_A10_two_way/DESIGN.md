# A10 — the variance–structure relation estimated both ways

_Written 2026-10-12 by Claude (Opus 5.5) before any of it was run. Asked
for by the owner the same day, after A9 (MODEL_STATUS §2z-bf): "estimate
this problem both ways: from the structure to the corresponding
individual-level, rank-specific variance, and the other way around from
estimated (with proper noise management) variance to the corresponding
structure. How closely do they correspond? What does this tell us about
what we are missing?"_

Status: EXPLORATORY, descriptive, DEVELOPMENT data. Data only, plus the
frozen structure fit for one variance column. No model code, simulation or
gate evaluation. No planted validation exists for these comparisons. A9
has been seen; what it showed is listed under "Already known" and is not
predicted again. Runner: `llm_fitting/phase1_a10.py`.

## The relation

One-constant rank diffusion: local log-log slope of the ladder s(r) =
σ²(r) / (2 g), with σ² the variance per week of the lasting level at rank
r and g one number per panel.

- **Structure → variance.** From the observed ladder's local slope, the
  variance each rank must have: σ²_implied(r) = 2 g s(r).
- **Variance → structure.** From an estimated variance profile, the ladder:
  s_pred(r) = σ²(r) / (2 g), integrated in log rank upward from the
  observed value at rank K.

g is not known independently yet. In both directions it is set from the
variance side as the median over rank bins of σ²/(2s) (top bin excluded),
which uses one number from the ladder; what is tested is shape, across
about ten octave bins. Where a holdout is possible it is used (below).

## Variance estimates, in order of noise management

- V0, none: the variance of one-week changes. Mostly noise and passing
  swings.
- V1, model-based: the fitted σ_perm² of the frozen structure stack.
- V2, model-free: the growth of the variance of h-week changes between 13
  and 26 weeks, per week. Anything stationary (measurement noise, passing
  swings that have died out by 13 weeks) is in both horizons and cancels.
  The same horizons are used on every panel so that g is comparable; A9
  used 13 to 52 on Reddit.
- V3, V2 with the annual cycle allowed for (Reddit panels): the cycle adds
  2S(1 − cos(2πh/52)) to the variance at horizon h, with S estimated in
  three rank groups from the covariance of quarterly changes a year apart
  (§2z-aw addendum); its contribution to the 13-to-26 growth is subtracted.

Structure: the time-median ladder (primary) and the ladder of sorted
endpoint means (secondary, without weekly noise).

## What is reported

1. Structure → variance: per bin, σ²_implied against each estimate; the
   root-mean-square log ratio across bins; the share of bins whose
   bootstrap 5–95% bracket for V2 contains the implied value.
2. Variance → structure: the predicted ladder against the observed one at
   200 ranks spaced evenly in log rank; root-mean-square error in log
   units, the largest error, and the error at rank 1. The yardstick with
   the same number of free quantities (one shape, one level) is the best
   single power law. A quadratic in log rank (two shape numbers) is shown
   as a ceiling.
3. Holdouts. In time, on the two 212-week panels: variance and g from the
   first half of the window, the ladder of the second half. Across panels,
   on Reddit: each panel's ladder predicted with another panel's g.
4. Where the two directions disagree, by rank: head, body, bottom octave.

## Already known from A9, not predicted here

The ratio σ²/(2s) is flat across octaves within 12–25%; it is higher in the
bottom octave on the two comments panels; the fitted σ_perm² tracks the
ladder less well than the model-free variance; g is about 0.003 on Reddit
and 0.0067 on Facebook with A9's horizons.

## Predictions

1. Variance → structure with V2: lower root-mean-square error than the best
   single power law on all four panels.
2. Noise management matters and in a known direction: with V0 the ratio
   σ²/(2s) rises with rank more steeply than with V2 on at least three
   panels (measurement noise grows faster toward the bottom than lasting
   variance does), and V0's ladder is worse than V2's on at least three.
3. Structure → variance: the implied variance lies inside V2's bootstrap
   bracket in at least 60% of bins on each panel (top bin excluded).
4. The head is where it fails: on both comments panels the predicted top
   value falls short of the observed one by at least 0.3 log units (one
   endpoint sits far ahead of the rest).
5. Allowing for the cycle (V3) changes g by less than 25% and does not
   change the coefficient of variation of the ratio by more than 0.05.
6. Holdout in time: g from the first half is within 25% of g from the
   second half on both long panels, and the second-half ladder predicted
   from first-half variance has an error no more than twice the in-sample
   one.
7. With the common horizons, g on the three Reddit panels agrees within
   25%, and Facebook's is at least 1.5 times theirs.

## What the pattern of disagreement would mean

- Estimated variance above implied at the bottom of the list: movement
  there that does not steepen the ladder. Candidates: turnover across the
  cutoff of the list, and noise that the horizon difference does not
  remove.
- Estimated variance below implied at the head: the top is steeper than
  diffusion among exchangeable endpoints accounts for. Candidates:
  advantages attached to particular endpoints, or drift that weakens at
  the top.
- A constant that does not transport across time or panels: the relation
  is a coincidence of two smooth profiles and not a law.
- In every case the constant itself stays unexplained until it is measured
  from movement (the decline of incumbents relative to the whole, and the
  entry that balances it).
