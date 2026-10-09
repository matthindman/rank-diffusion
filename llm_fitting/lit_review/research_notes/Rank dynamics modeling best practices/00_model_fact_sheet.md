# Model fact sheet — rank-diffusion program (brief for literature researchers)

_Written 2026-10-08 as the shared brief for the literature review. Numbers are
quoted from `llm_fitting/MODEL_STATUS.md` (the canonical record); if this sheet
and MODEL_STATUS disagree, MODEL_STATUS wins._

## Goal (owner's statement)

Build a stochastic model that explains AND predicts (1) the observed structural
rank-size stability of endpoints on digital platforms (the rank-size curve and
per-rank shares are stationary) and (2) the movement of individual endpoints
through those ranks over time, while (3) being as simple as possible but no
simpler. Target venue: PNAS-class. Owner is a senior scholar of online
attention concentration; do not explain the basics of Zipf/Gibrat/Gabaix.

## Data structure

Weekly (and daily) panels of endpoint activity, ranked each period:
- Facebook pages (CrowdTangle; interactions; ~14k tracked pages; censored
  sample with instrument "eras"; healthy Era A = 86 complete weeks,
  2020-11-02..2022-06-27; the legacy cutdown panel is T=88).
  _[Corrected 2026-10-08 after fan-out: the researchers were briefed with
  "≈ 88 weeks"; any note that reasons from T≈88 for Era A should read 86.]_
- Reddit subreddits (Pushshift census): comment karma 2018-12..2022-12
  (T≈213 weeks, ~12.5k-entity universe); submission karma 2018-12..2022-12
  (T=212 weeks, 142M daily rows; universe K=5,000).
- Instagram (CrowdTangle, heavily query-censored; modeled only through an
  explicit censoring/thinning apparatus; breadth evidence, not primary).
- Wikipedia pageviews: acquired/piloted, NOT yet analysed — the designated
  primary confirmation target; a mini-protocol must be frozen before contact.
Universe = closed "top-coverage" set: top-K by absence-penalized permanent
(time-averaged) rank, K chosen from concentration statistics alone (≈90% of
activity), buffer B=4K. Heavy tails, entry/exit at the boundary, zeros/ties in
the deep tail, census vs censored instruments.

## The model (as in code)

Log-activity of entity i in week t, common per-period factor removed:

    X_it = h_it + xi_it (+ xi2_it) + eps_it
      h    OU "home": slow reversion kappa(z) toward the ENTITY'S OWN home
           level, innovation sigma_perm(z)
      xi   fast transitory AR(1) (phi, sigma_trans(z)), Student-t innovations
      xi2  optional medium-timescale AR(1) (long panels only)
      eps  measurement noise sigma_obs(z), pinned/bounded from an INDEPENDENT
           instrument: the within-week daily-replication noise floor ("Spec-B")

- All components multiplied by ONE persistent entity amplitude v_i
  ("temperament"; lognormal, spread s). b=1 factorization is the main law:
  permanent and transitory volatility scale by the same v_i (measured
  b≈0.97–1.08). Analogy drawn to the Sinatra et al. Q-model.
- z = permanent-rank band. Parameters are nonparametric step/knot profiles in
  permanent rank: ~55 knots, ~500 moment-estimated band values in total.
  This is the main parsimony liability ("parsimonious latent architecture
  with flexible nonparametric rank profiles").
- Stationary AR(1) common platform level; Gabaix-style rebirth at the buffer
  bottom; entities ranked each week by observed X.
- The stationary rank-size ladder is TAKEN AS GIVEN (homes are seeded from
  the measured ladder). Claims are "conditional reproduction + maintenance"
  of the ladder, not ladder genesis.

## Estimation (current practice)

- Method-of-moments / minimum-distance on per-band change autocovariances
  gamma_0..gamma_6 plus long-difference variances D(h), h up to 52 weeks;
  exact NNLS solves; equal/identity-style weighting (a weighting audit found
  nil effect).
- Every parameter comes from a declared moment or an independent instrument;
  nothing is tuned to a score. sigma_obs from daily replication; amplitude
  spread s from the cross-entity dispersion of log change-variance; kappa
  from long-difference variances; t df from within-entity kurtosis.
- Estimate by PERMANENT rank only: conditioning on current rank oversamples
  transient spikers and inflates sigma ~3x (the program's "Eulerian selection
  bias").
- A scalar steady-state Kalman filter is used only to get filtered end-of-train
  states for conditional forecasts. No likelihood-based or Bayesian estimation
  of the full model is in use; no simulation-based inference beyond forward
  simulation for scoring.

## Validation (current practice)

- Tier 0 (the only adjudicator): rolling-origin (>=5 splits) out-of-sample
  "movement gate" — pooled moments of held-out rank displacement (median
  |dRank| at h=1/4/13 weeks, rank autocorrelation, head collision rates)
  vs a train-window historical-mobility baseline ("persistence"), with
  bootstrap bands. CRPS/PIT available as descriptive add-ons.
- Tier 1: a 15-row in-sample moment scorecard (variance ratios, ACF, rank
  ACF, persistence, R2), churn, boundary flux, omnibus Q used only to
  localize residuals.
- Preregistration with dated amendments before data contact; failures are
  reported as findings; one-change-per-experiment; common random numbers;
  >=20 seeds for head claims; append-only results log; repeated adversarial
  cross-model review (Claude <-> GPT/Codex).

## Where it stands (measured)

Works:
- In-sample cards 12–15 of 15 on FB / Reddit; amplitude collapse b≈1
  replicates on a second, outcome-unseen metric.
- Registered confirmation (Reddit comments extension, frozen parameters): the
  movement gate PASSED decisively (rel. moment error 0.032 vs 0.171 baseline).
- OOS gate otherwise "at par or modestly better" than historical mobility;
  proper-score (CRPS) skill vs persistence ≈ 0.

Fails / open (the research frontier):

_[CORRECTION 2026-10-08, after the researchers were briefed — item 1 below
overstates the record; read it with this note. Per `exit_audit.py` and
MODEL_STATUS §2z-ac/§2z-ae: the cohort is defined on the TRAIN window
(absence-penalized permanent rank ≤ K/2 or ≤ K/4); the event is a
week-over-week exit from the current top-K (not the top-K/2) scored on the
later window; a "crossing" is an entity still OBSERVED at t+1 with rank > K
(so ABLE to return), not one observed to return. ~98.6% of empirical events
are crossings; the simulation's are 14–52% permanent deaths. The 95–98%
"temporary" figure is for ABSENCE gaps returning within 13 weeks (§2z-aa).
No duration or return-time distribution for crossings is on record, so
whether these events are reversible excursions from an intact home, slow
drift of the home toward the boundary since the train window, or one-way
declines is OPEN. Notes that reason from "large persistent displacements
followed by return" inherit this overstatement.]_

1. ESTABLISHED-DEPARTURE DEFICIT (hardened, replicated on two metrics): the
   simulation UNDER-produces departures of established (high permanent-rank)
   entities from the top-K/2 by 2.3–2.7x (3.3x at K/4). Empirically ≥90–98%
   of such departures are returnable boundary CROSSINGS (large persistent
   displacements followed by return); the sim instead produces 14–52% of its
   (too few) departures as permanent "deaths". Empirical per-entity
   standardized change tails are FATTER than the simulated ones at every
   powered stratum. Diagnosis so far: missing large-displacement dynamics for
   established entities AND an estimand mismatch in the exit machinery
   (absence-as-death vs temporary absence; 95–98% of empirical absences are
   temporary).
2. STATIONARY HEAD LAW too concentrated on Facebook (sim top-1 share ≈2.7x
   empirical; also fired on the comments extension), but the sign REVERSES on
   Reddit submissions (sim under-concentrates) — so possibly measurement-
   regime-specific. Mechanism implicated: the product tail of lognormal
   amplitude x Student-t fast shocks at the head. A within-entity
   level-variance "stationarity anchor" was tried and REFUTED.
3. PARAMETER NON-STATIONARITY: the amplitude spread s shows a secular/era
   trend on Reddit comments (0.65 -> 0.75 -> 0.84, 2019->2022), non-monotone
   on submissions (1.11 -> 1.08 -> 1.13); kappa levels shift 4–8x between
   eras. Frozen-parameter forecasts under-disperse upper movement tails (p90).
4. NO PROPER-SCORE EDGE over persistence; the gate is on pooled moments,
   survivor-conditional.
5. Variance-ratio residual at 13 weeks (sim too persistent by ~0.03–0.05),
   partly a functional/non-Gaussianity artifact of the scored statistic.
6. Residual reversion-rate heterogeneity kappa_i (log-SD≈0.30) is real but
   did not predict out of sample.
7. Breadth: 2 platforms / 3 metrics (+ a censored IG demonstration) vs ~30
   systems in the closest published comparator (Iñiguez et al. 2022).

Already tried and refuted (do not re-propose without new evidence): finer rank
bands; a common time-varying volatility factor; constant-drift lifecycle
demeaning; rank-dependent t df; skewed innovations; time-variance stationarity
anchors; burst/near-tie head machinery; missingness/population-matched scoring
as the VR explanation; "tail-shape surgery" on the fast innovations.

Literature the program already knows and cites (do not spend effort
re-deriving): Gabaix 1999 / Gabaix & Ibragimov 2011 / Luttmer 2007 / Axtell
2001; Banner–Fernholz–Karatzas Atlas models, hybrid Atlas; Iñiguez et al.
2022; Blumm et al. 2012; Wu & Huberman 2007; Lorenz-Spreen et al. 2019;
Sinatra et al. 2016; Cochrane 1988, Poterba–Summers, Fama–French variance
ratios; Stock–Watson 1998 pile-up, Harvey 1989, Kamber–Morley–Wong 2018,
Auger-Méthé et al. 2016; Clauset–Shalizi–Newman 2009; Cabral & Mata,
Clementi & Palazzo, Haltiwanger et al. on selection/entry-exit.

## What the literature review is for

The owner wants a research agenda to IMPROVE THE MODEL (fit, prediction,
parsimony, breadth) and to bring estimation/validation practice to the
current state of the art. For every method or finding you report, say
concretely how it would apply to THIS model, data structure and frontier
residuals — and where it would not transfer. Prefer primary sources;
distinguish what a source demonstrates from what you infer.
