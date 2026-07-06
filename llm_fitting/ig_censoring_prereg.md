# IG "a"-query censoring — pre-registered rescue model & predictions

Date: 2026-07-06. Written BEFORE any forensic measurement on the IG panel
(only schema inspected: `llm_fitting/ig_weekly_ranked_top50k.parquet`, cols
date/account/user_name/metric_value/likes/comments/n_posts/rank, 2,650,405 rows).
Registered per the stochastic-modeling skill rule 5 (pre-register predictions).
IG remains non-calibration data; everything below is externally identified
from the KNOWN censoring process, never tuned to a score.

## The censoring process (known, mechanical)

CrowdTangle's IG endpoint required a search term; collection used the word
"a". A post enters the dataset iff its caption matches. Aggregation is
account x day (weekly panel held locally); `n_posts` = number of MATCHING
posts in the period.

## Formal model (thinned compound observation)

For account i, week t:
- true posts P_it (weekly intensity lambda_i), per-post engagement e_ij ~ F_i
  with mean mu_i and CV c_i (heavy-tailed);
- inclusion: each post matches with probability q_i — a PERSISTENT account
  property (language, caption style; emoji/hashtag-only captions -> low q);
- observed M_it = n_posts ~ Binomial(P_it, q_i);
  Y_it = metric_value = sum of engagements over matching posts;
- true weekly activity X_it = sum over ALL posts ~ P_it * mu_i.

Consequences, in the model's coordinates:

1. **Level**: E[Y|X] = q_i * X. log q_i is a persistent per-entity offset —
   it RELOCATES entities in the ladder (observed ladder = true ladder
   convolved with the cross-sectional law of log q) but adds NO dynamics.
   Goal-1 claims are therefore scoped "of a-matching activity"; the
   per-entity level bias is not identifiable from within-panel data.
2. **Observation noise**: conditional on X and M_it = m > 0,
   Var(log Y) ~= a_i / m with a_i ~= c_i^2 + (1 - q_i). The noise variance
   per account-week is a computable function of the OBSERVED n_posts.
   This is the IG analogue of Spec-B: sigma_obs identified from an
   independent instrument (the known thinning process), not calibrated.
3. **Zero-censoring**: P(M_it = 0) ~= exp(-lambda_i * q_i). Absence is
   MEASUREMENT (a thinning zero), not behavioral exit, for low-lambda*q
   accounts. The recorded IG pathologies (57% weekly exit, 74%/wk top-200
   turnover) should be largely this artifact plus item 2.
4. **Movement identity** (the identification moment):
   E[(dlog Y)^2 | M_t, M_t+1] = true-movement + a * (1/M_t + 1/M_t+1).
   Within-account regression of squared log-changes on (1/M_t + 1/M_t+1)
   identifies a; the intercept is the TRUE latent movement variance.

## Rescue apparatus to be tested (all additive; defaults untouched)

- **A. High-inclusion sub-universe**: accounts with high mean n_posts have
  thinning noise ~a/M -> small; run the UNCHANGED pipeline there.
- **B. Per-post metric**: Ȳ = Y/M is unbiased for mu_i REGARDLESS of q_i —
  the persistent selection bias cancels exactly. Changed estimand
  (per-post engagement, not totals) — declared, not hidden.
- **C. Time aggregation**: 2/4-week periods multiply effective M; thinning
  noise shrinks proportionally; true dynamics don't. FB = no-thinning control.
- **D. (only if A–C confirm)** Heteroskedastic sigma_obs pin R_it = a/M_it
  in the Kalman movement stack + absence-as-missing (skip update, not exit).

## Pre-registered predictions (scored PASS/FAIL in the writeup)

- **P1 (noise law)**: pooled within-account regression of (dlog Y)^2 on
  (1/M_t + 1/M_t+1), present-week pairs only: slope a positive and in
  [0.5, 5]; roughly stable (within a factor ~2) across rank bands; for
  account-weeks with M <= 3 the thinning term exceeds the intercept
  (measurement dominates), while for M >= 30 the thinning share of
  E[(dlog Y)^2] is < 25%.
- **P2 (absence is thinning)**: P(absent at t+1 | present at t) declines
  ~exponentially in the account's mean matching count m̄; accounts with
  m̄ >= 20 have weekly absence < 5%; the exit pathology is concentrated
  in low-m̄ accounts.
- **P3 (aggregation scaling)**: moving 1w -> 4w periods shrinks the
  thinning component ~4x; movement statistics move monotonically toward
  FB-like values; the same aggregation applied to FB changes its movement
  statistics comparatively little.
- **P4 (recovery on the clean slice)**: the high-inclusion sub-universe
  (mean n_posts >= 20, presence >= 80%) fitted with the standard stack
  (temperament + pooled knots + md6 + t-tails) yields (i) kappa declining
  head -> tail, (ii) t_df in ~4–10, (iii) a scorecard >= 8/15 with no
  runaway-diffusion signature (R2 rows finite and same order as empirical),
  and (iv) an OOS movement gate that is no longer pathological (rel-err
  same order as persistence; recorded historical behavior was R^2 collapse).
  Per skill §3, a 10–12/15 card + clean gate + transported parameter shapes
  would be a CONFIRMING result for the law on a fourth system.

Failure conditions (honest kill criteria): if P1's slope is ~0, or the
intercept dominates even at M <= 3, thinning does NOT explain the IG
pathology and the censoring rescue is dead — reported as a finding, not
retried with new knobs.

---

## Amendment 1 (2026-07-06, registered AFTER forensics P1–P3, BEFORE any fit)

Forensic adjudication so far: P1 PASS (refined: per-post decomposition;
slope positive, monotone binned curve; thinning share ~5% at M>=30).
P2 FAIL as registered — absence is NOT post-level thinning; diagnosis
revised to a 3-layer censoring stack: (L1) post-level "a"-thinning
(noise ~ a/M, confirmed); (L2) week-correlated INSTRUMENT dropout
(~10–14% weekly in the stable era; cohort absence sd across weeks 11x the
iid benchmark; ramp era weeks 1–8 at 21–26%); (L3) low-q flicker heads
(top-5k-by-level median presence 7.5% — ghost spikers by construction).
P3 PASS (movement 0.957/0.595/0.444 at 1w/2w/4w; implied thinning constant
consistent across steps, 3.2 vs 3.45).

Pre-registered decisions for the P4 fit (declared before ANY model run):
- Drop week 0 (partial collection: 104k vs ~400k accounts). Primary panel
  = weeks 1–52 (T=52); era-sensitivity check on stable era weeks 9–52.
- Universe: absence-penalized permanent rank computed on the FULL account
  population (absent weeks at floor N_t+1), the program's standard rule.
  **K = 10,000** (mean weekly coverage 0.80 of measurable-population
  activity). Declared deviation from the K90 convention: K90 here is
  ~25k, but the marginal tail accounts have M ~ 1–2 matching posts/week —
  i.e. pure thinning noise — so the universe is capped at the 80% point
  on MEASURABILITY grounds (n_posts-based, score-blind). B = 4K = 40k.
- Two metric variants, both run through the UNCHANGED pipeline:
  (A) totals  metric_value  — estimand: weekly a-matching engagement;
  (B) per-post  metric_value / n_posts — estimand: per-post engagement
  (q-bias cancels exactly; thinning noise c^2/M with c^2 ~= 0.25).
- Stacks: card = temperament + pooled knots (--min-knot-entities 8) +
  md6 + t-tails (+ --stat-factor); gate = md6 + t-tails + temperament +
  pooled knots + --conditional state. Spec-B unavailable locally (IG
  dailies on the WD drive) — sigma_obs is calibrated Spec-A, declared.
- P4 scoring as registered (kappa declining head->tail, t_df ~4–10,
  card >= 8/15, gate non-pathological). Expected NEW failure mode, declared
  in advance: the L2 instrument dropout (~10–14% weekly, week-correlated)
  enters the pipeline as spurious exit/entry churn and absence-penalty jitter
  in permanent ranks; boundary/churn rows and entry/exit-linked moments are
  therefore expected to carry an instrument component (FB 2g#4 analogue,
  stronger). If the card fails ONLY there while VR/ACF/RACF/R2/dRank hold,
  that is consistent with the censoring model, not against it.
