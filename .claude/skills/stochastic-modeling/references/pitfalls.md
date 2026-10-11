# Pitfall catalogue

Read the relevant part before estimating, building cohorts, diagnosing a
residual, touching data, or launching a long run. Every entry is a mistake
this program actually made, with the section of MODEL_STATUS that records
it. They are listed because each one produced confident, wrong numbers
before it was caught — usually by measurement or by the other model's
review, not by reasoning.

## Contents

1. Selection and cohort construction
2. Identification
3. Scoring and statistics
4. Data and instruments
5. Protocol execution
6. Environment and code

## 1. Selection and cohort construction

- **Conditioning on current rank is selection on noise.** Entities at a
  given rank this week are disproportionately transient spikers. Estimating
  by current rank inflated σ about threefold and produced runaway diffusion.
  Estimate and stratify by permanent (time-averaged) rank (§3).
- **Observed-week mean rank lets the same bias back in.** Membership must be
  absence-penalized: absent weeks count at the floor N_t + 1. Otherwise one-
  or two-week spikers enter the universe and inflate the deepest knot's exit
  rate (0.60 per week against a true 0.0007). The ghost-spiker test locks
  this (§2b, §3).
- **A cohort defined at the moment of the event inherits the event.** "Core
  exits four times too high" was transient spikers visiting core ranks;
  under a permanent-rank cohort the sign flipped (§2z-z). Define cohorts on a
  window that ends before scoring starts.
- **Simulated tracks follow slots, not identities.** After an exit and
  rebirth the slot keeps a positive rank under a new identity, so slot arrays
  cannot see a death. Use `simulate(track_ids=True)` and reconstruct
  identity histories (§2z-ab).
- **Both sides of a comparison must be built the same way.** The departure
  result needed three rounds: slot → identity, full-window universe →
  train-only, survivor-filtered empirics → no filter (§2z-ab, §2z-ac). When
  data and simulation pass through different selection, the difference is
  the selection.
- **A departure "crossing" is an entity still observed below rank K.** It is
  able to return; it has not been observed to return. No return-time
  distribution for crossings is on record (§2z-ai).
- **Boundary return rates are selected statistics.** Events are picked at
  the boundary and then followed. Such a rate can enter estimation only by
  indirect inference with identical selection on the simulated side, never
  as a row appended to the MD objective. The card and `flux_decomp` use
  different horizon conventions (t+h and t+1+h); unify them before any
  identifying use (§2z-z).
- **Positional masks.** A sorted mask applied to unsorted columns scrambled
  the rank bands, flattened every rank curve and made a bad model score
  15/15. It is regression-locked; treat any new positional mask as suspect
  until an alignment test covers it (§3).

## 2. Identification

- **φ → 0 makes the fast AR(1) indistinguishable from white noise.** The
  symptom is head σ_obs at 0.000 or pinned at a grid edge with rank
  autocorrelation too persistent. Resolve it with an external pin on σ_obs
  (Spec-B) where daily data exists, never with a tie-break chosen against
  scores (§2e, §2r).
- **κ is nearly flat in the short-lag objective.** The OU tail is spread
  thinly over γ₀…γ₆. Long differences D(h) identify it, on long panels only.
  κ estimates from different `md_lags`, different moment sets or very
  different panel lengths are not comparable; a 4–8× "shift" between a
  136-week and a 77-week window fitted with md6 alone may be the ridge, not
  the process (§2i, §2z-q, §2z-ai).
- **The Spec-B floor has one invariant convention.** Week-mean centering
  annihilates the all-ones direction, so only the centered floor pᵀCΣCp is
  invariant. The legacy min-norm floor violated Spec-A ≥ floor on FB (§2r).
- **What may be said about σ_obs** (binding language): identified in shape
  everywhere; identified in level at the FB head only; bounded between the
  centered floor and Spec-A elsewhere; below the universe head, bounded
  pending an intermittency-aware floor (§2r, §2x).
- **Unpinned head noise is a solver artifact.** NNLS puts the unpinned head
  exactly on the σ_obs = 0 corner; under either solver only Spec-B-pinned
  head noise is physics (§2z-e).

## 3. Scoring and statistics

- **Head-collision rows are noisy.** coll1 has seed SD about ±0.15; churn
  error carries ±0.03–0.04 from it. A five-rep difference at the head means
  nothing without bands. Use 20 or more reps or `scorecard_bands.py` (§2o,
  §2t).
- **Three-seed grids mislead about the head.** A three-seed attribution said
  S(1) was insensitive to s; at 20 paired seeds halving s removed about half
  the excess (§2z-w).
- **Near-zero empirical moments explode relative error.** A coll1 = 0 split
  once scored about 5772. `MOM_FLOOR = 0.02` handles it in the gate; check
  every new metric for the same failure (§2).
- **Scored variance ratios are not determined by the spectrum.**
  Phase-randomized surrogates with the empirical spectrum overshoot scored
  VR13 by about +0.04. Part of that residual is functional, not dynamic;
  do not chase it with dynamics (§2v, §2y). The functional share itself did
  not replicate on submissions (§2z-ae P9b).
- **Head share is a few-entity functional.** S(1) is a time-mean of a
  maximum; under unbounded log-scale t shocks its seed distribution is wide.
  Report the seed distribution, not only the mean. Share statistics are
  robust to a drifting common level; the ladder distance is not (§2z-a,
  §2z-v).
- **Period-0 anchoring.** Pers{h} card rows are first-week statistics with
  no empirical band, and unconditional simulations seed homes from period 0.
  FB Era A starts in the 2020 election week, whose top-2 gap is 2.4 times
  the era norm (§2o).
- **Pooled fits against median scores diagnose heterogeneity.** When the
  pooled moment fits and the median entity misses, look for a mixing
  distribution or a population-selection asymmetry before adding dynamics
  (`research_notes.md` §6c).
- **Mechanism verdicts are not adoption verdicts.** A measurement can show a
  channel carries a residual without licensing a change; adoption is decided
  separately, by pre-declared gates (§2z-w).

## 4. Data and instruments

- **Facebook.** Never `fb_ranked_weekly.parquet` (corrupt after
  2022-06-27). Use `fb_ranked_weekly_cutdown.parquet` (legacy) or the SSD
  `fb_weekly_rebuilt` Era A slice for headline claims. Era C is replication
  with caution, D supports daily statistics only, B and M are unusable. No
  window of any kind may straddle a broken era. `fb_weekly_rebuilt` ids are
  page names, so a rename reads as an exit plus an entry
  (`instrument_eras.py`).
- **Instagram.** The raw panels are a negative control and are never
  calibrated to. The only modeled use is the §2z-c censoring apparatus
  (`instagram_hm`: K = 10,000 measurability universe, weeks 1–52, estimand
  "of a-matching activity"). The id is `user_name`. The old top-50k file is
  a per-week cut, so presence analyses on it confuse "below the cut" with
  "absent". Week 0 of the full panel is partial.
- **Census and censored instruments say different things.** On a census
  (Reddit) coverage shares are platform statements and absence is
  below-floor activity. On a censored panel (FB, IG) every coverage
  statement is "of tracked activity" and absence is not behavior. Never
  compare coverage percentages across the two.
- **s is specific to a metric and an era.** Submission karma and comment
  karma give different s; it also drifts in time. Never assume the level
  transports (§2g-X, §2z-s).
- **Decide the observational unit from the data's semantics before writing
  a dedup rule.** A global-URL dedup on Instagram would have erased
  collaborators: one post legitimately belongs to two to four accounts. The
  endpoint unit was `(user_name, url, date)` (§2z-ae).
- **A reconciliation target must be built on the same unit.** The registered
  exact-reconciliation gate for Instagram was infeasible because its target
  had been built without dedup (§2z-ae).
- **Test an intake rule against data already registered.** A blanket
  non-negativity rule failed on the frozen baseline itself, because a signed
  audit column was never a model input (§2z-o, §2z-p).
- **Local files can be evicted.** Repo parquets under iCloud can fail with
  errno 89; `brctl download <path>` restores them. SSD panels need
  `/Volumes/T9`; raw archives need the WD drive. If a drive is missing, say
  so and stop that thread; never substitute other data.

## 5. Protocol execution

- **A validator that exits 0 on failure is not a gate.** The first Phase-0
  validator reported and carried on, and loaded the whole panel into memory.
  Intake gates fail closed, stream, and ship with adversarial tests that
  reproduce each false pass (§2z-ad Amendment 1, §2z-k, §2z-l).
- **Never rehearse on the sealed data.** A real-data dry run would have
  consumed the one-pass evaluation; a synthetic full-path preflight replaced
  it (PREREG_2026-07-16 Amendment 6).
- **Tightening is allowed before contact; loosening is not, ever.** An
  amendment once loosened a criterion while claiming otherwise; the next
  amendment reversed it (PREREG_2026-07-16 Amendment 2).
- **Check that a scripted edit applied.** A patch silently did nothing and
  left a registered CLI unable to run (PREREG_2026-07-16 Amendment 4).
- **A killed run has no result.** Do not interpret or reuse partial output;
  record the kill and relaunch the identical registered command. Five runs
  were killed during the confirmation battery, with machine idle sleep
  suspected; the record states that no killed run produced a scoreable
  number (§2z-q).

## 6. Environment and code

- **Stay on pandas 2.x.** The research suite fails 22 tests on pandas 3.0 in
  the registered intake tooling; CI pins `pandas<3`. Do not upgrade the
  registered interpreter, and build scratch environments with the pin
  (§2z-ai).
- **Long runs.** Use `python -u` when redirecting output and wrap in
  `caffeinate -dims`. Gates on large panels take minutes to hours. When
  another vendor's agent session is active on the machine, agent-managed
  background jobs have been killed without a crash report; run heavy jobs
  from a user-owned Terminal (§2z-ae).
- **zsh does not word-split unquoted variables.** A multi-word variable
  stays one token and argparse swallows it silently. Identical outputs
  across a sweep are the tell.
- **Tests import `archive.src.rankdiff…`.** Run `python -m pytest tests/ -q`
  from the repo root. The packages under `Python/` and `R/` lag the research
  line by design; `llm_fitting/` is the active code.
- **Simulator internals.** Arrays are sized `n_sim`, not `n_global`, for the
  random stream. Persistence `top_ids` uses a −1 sentinel; filter it before
  set intersection. New behavior must leave flag-off random streams
  bit-identical.
- **The repository is public.** A push publishes. Material about the
  owner's other projects, and anything personal, needs the owner's explicit
  go-ahead (repo-maintenance skill).
