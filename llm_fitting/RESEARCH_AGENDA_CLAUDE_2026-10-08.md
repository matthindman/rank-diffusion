# Research agenda (Claude) — rank-diffusion program, October 2026

**Date:** 2026-10-08
**Status:** PROPOSED. Owner-gated; nothing here is adopted. It amends no frozen
spec, threshold, protocol, preregistration, or the binding claim set
(MODEL_STATUS §6 addendum 2026-07-12). No model was fit and no new data were
contacted to write it.
**Author:** Claude Opus 5.5.
**Companions:** the commissioned literature notes and the model fact sheet
in `lit_review/research_notes/`. A consolidated report
(`lit_review/reports/`) and a memo on what transfers from the x-collection
review also exist in the working copy; they were not committed with this
file on 2026-10-09 because this repository is public and they describe that
project in detail — the owner's decision.
**Relation to `RESEARCH_AGENDA_2026-10-08.md`:** that file was written the same
afternoon by a concurrent session of the other model, at the path this agenda
was first drafted to. The two were prepared independently; Part F compares
them. Nothing in that file or in its MODEL_STATUS §2z-af entry was edited.
**Numbers:** quoted from MODEL_STATUS.md with section references. If this file
and MODEL_STATUS disagree, MODEL_STATUS wins.

---

## Summary

The architecture has earned its place: one registered confirmation of
frozen-parameter movement, an amplitude law (b ≈ 1) that has now replicated on
an outcome-unseen metric, and a measurement-noise instrument that neither
the program's notes nor this review found anywhere else in the
ranking-dynamics literature. Five things stand between that and a model that
"explains and predicts" both structure and movement as simply as possible:

1. **The slow layer may be too stationary.** Entities revert forever to a
   fixed home. The replicated established-departure deficit, the 4–8×
   era-dependence of κ, and the under-dispersed 13-week tails are all what
   that would produce. The record's own evidence is split (B1), and the
   measurement that would settle it — dispersion beyond 52 weeks, and whether
   the missing departures are drifters or excursionists — has not been made.
2. **The model's distinctive content has never been scored at the endpoint
   level.** The gate and the CRPS add-on both score pooled displacement
   distributions, where a train-window mobility baseline is calibrated by
   construction. "At par" there says little either way.
3. **Parsimony is asserted, not measured.** ~500 band values have never been
   tested against the one-exponent fluctuation-scaling forms that three
   literatures suggest, and the first test needs no refit.
4. **There is no sampling theory or identification exhibit**: no intervals on
   band values, no moment-sensitivity table, no whole-pipeline recovery, no
   evidence on whether the gate can reject a wrong model.
5. **Breadth, with the confirmation resource protected.** Wikipedia is the
   only clean target and is being approached without an exposure ledger or a
   plan for what it tests.

The plan: bank the July record; run one pre-declared **measurement round** on
already-consumed panels (no model code) in parallel with inference and
evaluation infrastructure; then at most one candidate change per residual,
adjudicated by the existing gate; then freeze a Wikipedia protocol with
lockboxes before any contact.

---

## Part A — Where the program stands

### A1. Scoreboard against the three goals

**Goal 1 — structure (conditional reproduction and maintenance of the ladder).**

| panel | in-sample card | churn err | head share S(1), empirical vs simulated |
|---|---|---|---|
| FB Era A (K=3,500; 86 weeks) | 14/15 | 0.029 | 0.017 vs 0.047 ± 0.013 — ~2.7× over (§2z-b) |
| Reddit comments (K=12,500) | 12/15 | 0.044 | dev panel within ~1 seed-SD; extension 0.090 vs 0.136, E5 fired (§2z-q) |
| Reddit submissions (K=5,000) | 14/15 | 0.052 | long panel 0.072 vs 0.054 ± 0.011 — sim under (§2z-ae P8) |
| IG rescue (K=10,000) | 8/15 | 0.062 | 0.035 vs 0.101 ± 0.021 — ~2.9× over (§2z-d) |

Cards are the NNLS-primary record (§2z-g). The ladder itself is an input
(homes are seeded from the measured period-0 ladder), so the claim is
maintenance, not genesis (§1).

**Goal 2 — movement.**

| evaluation | model | baseline | reading |
|---|---|---|---|
| FB Era A gate (Spec-B + conditional state) | 0.123 ± 0.033 | 0.145 ± 0.031 | better on 4 of 5 splits |
| Comments gate | 0.171 ± 0.046 | 0.165 ± 0.062 | at par |
| Submissions gate (short panel) | 0.164 ± 0.053 | 0.168 ± 0.004 | at par |
| IG gate (exact membership) | 0.320 ± 0.189 | 0.593 ± 0.309 | better on 4 of 5; pooled CRPS skill ≈ 0 |
| **E2 registered confirmation**, frozen T=136 parameters, one 34-week block | **0.032** | 0.171 | PASS (§2z-q) |
| Submissions long-panel backtest P6 | 0.194 ± 0.061 | 0.163 | inside the +0.05 margin; no edge (§2z-ae) |

The ± values are split-to-split spread, not confidence intervals. The baseline
is the train window's displacement distribution, not a no-change forecast.

**The frontier residual.** Take the cohort whose *train-window* permanent rank
is ≤ K/2 and count week-over-week exits from the current top-K in the later
window. Comments: empirical 0.00800 vs simulated 0.00350 [q10 0.00283, q90
0.00410], a 2.3× deficit (3.4× for the ≤ K/4 cohort; §2z-ac). Submissions,
outcome-unseen: 0.00851 vs 0.00311, 2.74× (§2z-ae P7). About 98.6% of the
empirical events are *crossings* — the entity is still observed at t+1, below
rank K — while 14–52% of the simulated ones are permanent deaths. "Crossing"
means able to return, not observed to return: no duration or return-time
distribution for these events is on record.

**Goal 3 — simplicity.** The mechanism list is short and b = 1 keeps
surviving (b ≈ 1.02–1.08 on FB and comments; b(4) = 0.977, b(8) = 0.965 on
submissions). The liability is ~500 moment-estimated band values over ~55
knots (§1).

**Transport.** What moved: s (0.65 → 0.75 → 0.84 on comments, era-dominant,
§2z-s; non-monotone 1.113 → 1.082 → 1.130 on submissions, P4a FAIL) and κ
levels (4–8× into the extension, E1 FAIL). What held: the horizon-scaling
exponent, observation-noise shape, b ≈ 1, era-dominance (P4b), and the
departure deficit.

### A2. The last active stretch (2026-07-11 → 07-19)

1. Confirmation battery E1–E5 executed once under A1–A10: MIXED EVIDENCE (E2
   pass, E1 fail, E5 fired); binding claim paragraph adopted (§6).
2. Post-battery anatomy (§2z-s … §2z-ac): three head-law fixes and the
   within-entity stationarity anchor refuted by measurement; the "core exits
   4× too high" reading reversed sign under a permanent-rank cohort and
   hardened into the deficit above. Candidate fixes were deferred behind a
   measurement round that has not been run.
3. Submissions backtest (PREREG_2026-07-16 + 7 amendments; §2z-ae): eight
   registered items passed, P4a and P9b failed, P7 replicated.
4. Instagram 2023 dailies: P10/P11 not adjudicated; the build failed closed on
   collaborative posts. `IG_JOINT_POST_ALLOCATION_PLAN_2026-07-18.md` designs
   the corrected construction; not implemented.
5. Wikipedia: one month (2025-01, English) downloaded and aggregated on T9;
   no modeling contact. The `2099-01` raw directory flagged in §2z-ad is
   unexplained, and a `wikipedia/derived/tail_estimator_samples_2025-01`
   directory (dated 07-31) suggests another project has read that month.

### A3. Repository state (checked 2026-10-08)

- Suite green in the working tree: 177 passed, 13 subtests.
- **The newest canonical results were uncommitted** when this was written
  (last commit f7af440, 2026-07-16): the §2z-ae section of MODEL_STATUS.md,
  the run archive `runs/2026-07-16_submissions_ig/`, the bounded intake-gate
  rewrite and its tests, the IG allocation plan, the Wikipedia pilot script
  and test, and the whole package-track batch.
  _Update, 2026-10-08/09, at the owner's request:_ all of it is now
  committed — the July record (f6d8c91, ce9d84f), the package-track batch
  (0383785, 25ff0a2, 0d709b8, bdd1042), and the Reddit builder scripts,
  Wikipedia pilot and IG plan (6560d4b, b1f6ce3, 3e55e22); this session is
  recorded in MODEL_STATUS §2z-ai. One finding from that work bears on this
  agenda: the research suite fails 22 tests on pandas 3.0, so CI pins
  pandas below 3 and the analysis environment should stay on pandas 2.x
  until the registered intake tooling is fixed.
- T9 and the WD Passport are both mounted.

---

## Part B — Diagnosis: what a fresh read of the record and code adds

Each item separates what the record shows from what I infer.

### B1. The slow layer may be too stationary (conjecture; decisive test is cheap)

*Record.* In the simulator each entity reverts to a fixed home for the whole
run (`minimal_rankdiff.py`, `simulate`: `mu = mu - kap*(mu - home)`), with
Gaussian home innovations and Gaussian medium-scale innovations; heavy tails
live only in the fast component, by declaration. D(h) moments are fitted to
h ≤ 52. The cohort in the deficit measurement is defined on weeks 0–135 and
scored on 136–212. κ "shifts" 4–8× between a 136-week and a 77-week window.
On within-entity level variance the record is split: at the FB head it is
~0.64 against a model-implied stationary level of 0.19, attributed to "§2v
home drift" (§2z-t); on comments the two nearly agree (0.49 vs 0.46, same
section). So in-window, at the comments head, the OU level is about right —
on the panel where the deficit was first measured. That is evidence against
the conjecture as far as it reaches; it does not reach the ≤ K/2 cohort or
the later window, which is where the deficit lives.

*Inference.* A process with slower-than-OU memory, fitted by an OU, yields a
window-dependent κ, too little dispersion beyond 1/κ, under-dispersed
long-horizon tails, and too few established entities arriving at the
boundary a year after the cohort was defined. That would be one
misspecification behind several recorded residuals. The alternative reading — intact
homes with rare reversible excursions — calls for a jump or regime component
instead. The two readings are separated by measurements W1-0 and W1-7 below,
and nothing recorded so far distinguishes them. Note that a jump-driven OU
has the same autocovariances as a Gaussian one, so no γ_k or D(h) moment the
program currently fits can see the difference; only path or higher-order
statistics can.

### B2. Candidate causes of the deficit that the record has never examined

By keyword search of MODEL_STATUS: no mention of seasonality or annual
periodicity; none of cross-sectional dependence or co-movement beyond the
single common factor; none of within-entity volatility clustering (the
refuted item was a *common* volatility factor). Item counts
(`post_count`, `comment_count`, `submission_count`) are present in every
primary panel but have only been used on Instagram, so whether large drops
are in posting supply or in per-item engagement is unknown for FB and Reddit.

### B3. The head-law overshoot has a specific, checkable candidate cause

*Record.* Fast innovations are unit-variance Student-t with no bound
(`_tdraw`); S(1) is a share of `expm1(X)` (`community_metrics.top_share`).
§2z-v: switching to Gaussian fast innovations moves FB S(1) from 0.0451 to
0.0241 ± 0.0012, and "seed noise dies"; the overshoot is "heavy-tailed
transitory draws transiting the #1 slot". Head kurtosis and skew match, and
empirical q99/q90 is *fatter* than simulated in powered strata (1.87 vs 1.62).

*Inference.* A log-scale Student-t shock has no finite exponential moment, so
a mean share in activity units is dominated by the largest draws: that is
what a tenfold collapse in seed SD under Gaussian innovations looks like.
The data can be fatter than the t at q99 and still bounded where it matters
(no page multiplies its week by e^15). So the candidate is not "thinner
tails" (refuted) but a bound in the far tail that leaves the fitted kurtosis
moment alone. The sign reversal on submissions needs a second term: homes are
seeded from one observed week (`w0` = sorted period-0 values), so S(1) also
inherits how typical that week's head was over a four-year panel. Era-median
seeding was found moot for the FB overshoot (§2z-a), which is consistent
with spikes dominating there; it has not been examined on submissions.

### B4. "At par" on pooled moments is the expected result, not a finding

*Record.* The gate scores pooled displacement moments; `--dist-scores`
computes CRPS of the *pooled* simulated displacement distribution against
the pooled train distribution (`rankdiff_kalman.py`, declared in the code
comment as "unconditional over the cohort, not per-entity conditional").

*Inference, following Gneiting–Balabdaoui–Raftery.* A train-window mobility
distribution is a climatological forecast: under stationarity it is
marginally calibrated by construction. Any adequate model ties it on pooled
quantities and can win only when mobility shifts between train and test —
which is what E2 showed. Whether the model knows *which* endpoints will move
(state, amplitude) is a separate question that no recorded evaluation asks.
Three further limits of the registered criterion are worth knowing, without
changing it: relative error is minimised by a forecast shaded low; MOM_FLOOR
selects moments on realised values; medians cannot see the p90
under-dispersion. And survivor conditioning removes the largest known misfit
from the score.

### B5. Adaptive overfitting to the gate is a live exposure

Split-to-split SD is 0.03–0.06, the size of the differences being
adjudicated. At the low end of that range, the best of ~20 independent
specifications would gain about 0.056 by selection alone — more than the
+0.05 margin. Five origins cannot
reject at 5% with a sign test (floor 0.0625). No ledger of gate evaluations
exists, and no study of what the gate passes when the model is wrong.

### B6. Some "non-stationarity" may be weak identification or instrument

The E1 κ comparison (§2z-q: head 0.0390 vs reference 0.0050) used the md6
moment set on both sides — γ₀…γ₆, no long differences — which is the set the
program's own pitfall catalogue describes as flat in κ. Where long
differences are used, the scope rule (T ≥ 2.5h) limits a 77- or 86-week
window to h ≤ 26. A 4–8× shift may therefore be movement along the
(σ_perm, κ) ridge, in which case the stationary variance V = σ_perm²/(2κ)
would be the stable quantity.
The s estimator corrects for χ² sampling noise in log sample-variances; its
recovery under t-tails and short T has not been tested, and s differs most
where tails and T differ most. Separately, Pushshift fixes scores at capture
time: if the creation-to-capture lag changed between 2019 and 2022, comment
karma dispersion would drift mechanically, and more for comments than for
submissions — the recorded pattern. (Inference from the Pushshift FAQ; not
audited.)

### B7. The amplitude law is not implemented uniformly

In `simulate` and in both cohort simulators behind the gate, v_i scales the
fast and observation components always, and the permanent component only
under `--mix-hetero`, because raw lognormal scaling of σ_perm "explodes
held-out displacement" on short train windows (code comment; Reddit OOS
0.254 → 0.404). The frozen movement stacks for FB and submissions do not
list `--mix-hetero`, so as specified they do not carry b = 1 into the
permanent layer; the comments movement stack and the exit audit (LONG stack)
do. The paper's main law and the gated simulators should be reconciled, or
the difference declared.

---

## Part C — What the literature review changes

Full argument and citations are in the research notes; verification status
varies (several researchers exhausted their search budget — each note tags
what was read in full, what was abstract-only and what was recalled, so
check those tags before citing anything in the paper).

1. **Estimation is orthodox; the gaps are around it.** Identity-weighted
   minimum distance on covariance structures is what the earnings-dynamics
   literature converged on after Altonji–Segal. Missing here: block-bootstrap
   inference, a moment-sensitivity table (closed-form for NNLS), and
   weak-identification Monte Carlo.
2. **The earnings literature has met the same residual.** Guvenen, Karahan,
   Ozkan & Song find non-Gaussianity mainly in *persistent* innovations plus a
   position-dependent, long-lasting but non-permanent "off" state, and show
   transitory fat tails cannot produce large-change-then-partial-reversion.
   Kurtosis by horizon is their identifying device. Daly, Hryshko & Manovskii
   trace levels-versus-differences discrepancies to observations adjacent to
   gaps — all of this program's moments are in the differences family.
3. **Fluctuation scaling is the obvious compression.** Firm growth, Taylor's
   law and online-activity studies describe one form: SD of log growth ∝
   size^(−β), β ≈ 0.1–0.25, and ½ for pure counting noise. Moran, Secchi &
   Bouchaud (2024) find the same three layers as this model: size-dependent
   mean volatility × a size-independent entity multiplier × fat-tailed
   standardized shocks. No published exponent exists for subreddits, pages or
   articles; this would be a new measurement.
4. **In stochastic-portfolio-theory terms this model is the name-based corner
   of the hybrid Atlas family.** Rank-only models wrongly let every name visit
   every rank; the selection bias of conditioning on current rank is only
   heuristically recognised there. Fernholz's gap identity is usable as a
   head diagnostic computed identically on data and simulation, never as a
   constraint on Lagrangian parameters.
5. **Neural simulation-based inference is the wrong tool for the core
   model** — by that literature's own account it is unreliable under known
   misspecification, and ~500 parameters is far outside its regime.
   Legitimate narrow uses exist (robust synthetic likelihood as a
   misspecification localiser).
6. **Wikipedia is structurally unlike the current panels**: bursts decay as
   power laws, new articles are born at the head, articles almost never die,
   and identity is safe only by page ID from the dumps. The instrument has
   dated breaks (2020-04-29 bot class; 2021-06 to 2022-01 data loss; 2025
   detection rebuild). The 90% coverage rule may imply K of 10^5–10^6.
7. **From the x-collection review**, what transfers is discipline
   (whole-pipeline bootstrap, model recovery, moment sensitivity, typed
   specification curves, exposure ledgers, decision registers); what does not
   is anything about optimizer convergence, count-family selection, i.i.d.
   bootstraps over units, or random-split critiques. The decisive structural
   differences: one coupled ranked panel instead of many independent trees;
   rank statistics available only by simulation; the estimation problem is
   variance-component separation under weak identification, not offspring
   distributions. One process lesson applied directly while writing this: the
   brief given to my own researchers overstated the departure finding
   ("returns" for "crossings"), exactly the fact-sheet error that propagated
   through that project. It is corrected in the fact sheet and flagged to the
   report writer.

---

## Part D — The agenda

Rules carried over unchanged: measure before code; predictions written before
running; one change per experiment; the rolling-origin gate adjudicates
adoption; Wikipedia is never used to choose between candidates; frozen
objects are owner-gated.

### W0. Bank and coordinate (hours; owner-gated)

- Commit the July record: §2z-ae, the run archive, the intake-gate rewrite.
  Decide separately whether the package-track batch goes in the same series.
- **Exposure ledger** (new file): for every dataset and time block, which
  analyses and which model sessions have had value-level contact. Start with
  Wikipedia 2025-01 (pilot aggregation; tail-estimator samples) and the
  2099-01 directory.
- **Gate ledger**: every gate evaluation ever run, with spec, seeds, result.
  Reconstructable from MODEL_STATUS and `runs/`; needed for B5.
- **Coordination rule for the two models**: author-suffixed filenames for
  drafts, or a claims file, so that two sessions cannot write the same path.
  Today's collision cost nothing; a collision on MODEL_STATUS would.

### W1. One measurement round on consumed panels (no model code)

Panels: FB Era A, Reddit comments, Reddit submissions. Identical code path on
empirical and simulated panels (frozen specs, ≥ 20–30 seeds, common random
numbers, entity and week-block bootstrap). Predictions and numeric
tolerances go into a single dated measurement plan before anything runs.
Ordered by information per unit cost.

| # | Measurement | What each outcome licenses |
|---|---|---|
| 0 | **Decompose the existing departure events.** For each event: margin to the cut the week before; scoring-window median rank vs train permanent rank; first exit of an episode or a re-exit. Report episodes per cohort-week and the shares from drifters, excursionists and one-way declines. Add a within-train split (cohort on first half, scored on second). | Mostly drifters or one-way → long-horizon under-dispersion of homes (go to 7); no jump or regime component is licensed. Mostly excursions from intact homes → continue to 1–5. Deficit shrinks in the within-train split → part of it is parameter drift. |
| 1 | **Excursion catalogue**: depth, duration to re-entry (Kaplan–Meier, right-censored), return hazard, depth–duration slope, onset and recovery lumpiness. Includes the knot-aligned exit-estimand audit owed since §2z-ac and the t+h vs t+1+h convention fix. | Duration rising with log depth at slope ≈ 1/κ, gradual recovery → heavy-tailed home innovations. Depth-independent duration, abrupt recovery → reversible regime. Peaked durations → seasonal (go to 6). Short diffusive excursions → no new component. |
| 2 | **Tails by horizon**: q99/q90 and a quantile kurtosis of standardized h-week changes, h = 1 … 52, upside and downside. | Gap gone by h ≈ 4 → fast-shaped; nothing persistent licensed. Gap growing then flat → non-Gaussian persistent or regime component, plateau gives its timescale. Flat from h = 1 → scale mixing over time. |
| 3 | **Persistence by size and sign**: retained fraction of large vs small, up vs down moves, k = 1–26. | Large moves retained longer than simulated → a medium- or long-lived large-move channel exists. |
| 4 | **Volatility clustering**: within-entity autocorrelation of absolute changes; pooled vs within-entity ARCH slopes (Meghir–Pistaferri); opposite-sign share among paired large moves. Doubles as a split-sample test of b = 1. | Slow decay with random signs → entity stochastic volatility. Sign share near one at a lag → regime. Neither → isolated jumps. |
| 5 | **Supply vs engagement**: split Δlog Y into Δlog(item count) and Δlog(per-item mean) on all three panels; share of catalogued drops carried by supply; zero-item weeks. Also: the 1/M noise law against the Spec-B floor, and how much of log v̂_i is explained by item count. | Supply-dominated → activity regime with an observed off-state. Engagement-dominated → demand or algorithmic shock. If counts explain much of v̂_i, part of "temperament" is posting structure. |
| 6 | **Calendar structure**: per-entity annual periodicity (lag-52 autocorrelation, D(26) vs D(52)); week-of-year concentration of departures; co-exceedance against an entity-wise circular-shift null (the P11 machinery); residual cross-sectional dependence after the common factor. | Annual periodicity → a measured deterministic seasonal profile removed before estimation (no stochastic parameters), which may also relieve κ. Community clustering → changes bands and effective sample size, not the mean rate. Clustering across unrelated communities on specific dates → instrument artefact. |
| 7 | **Long-horizon dispersion**: D(h) for h = 52 … 156 where T allows, by band; rank autocorrelation at lags 52/104; dispersion of scoring-window median rank around train permanent rank. | Still growing where the simulation saturates, Gaussian long differences → slower or fractional home; one memory exponent could replace κ(z) and the medium component. Growing with fat-tailed long differences → rare level shifts. Saturating as simulated → drift is not the explanation. |
| 8 | **Head-law two-by-two** on the three panels, and on `instagram_hm` only through the §2z-c apparatus: seeding (period-0 ladder vs time-averaged ladder) × far tail (unbounded t vs bounded at the largest empirical standardized move). Share of the S(1) mean contributed by the top 5% of weeks, data vs simulation. Gap, gap-variance and collision diagnostics for ranks 1–50. | Attributes the overshoot to spikes, seeding, or neither, and explains the sign change across panels without appeal to "Facebook measurement". |
| 9 | **Amplitude audit**: recovery of s under t-tails at T = 86/136/212; out-of-sample persistence of v̂_i (train → test rank correlation; realised |Δ| by v̂ decile); upper tail of v̂ against the lognormal (Kiefer–Wolfowitz NPMLE as a diagnostic); the cross-fitted amplitude–rank check owed since §2z-z (the uncrossed version is on record, §2z-y); era-by-era quantiles of per-entity change variance on a composition-fixed set (Jensen–Shore — a different question from the era-vs-composition four-cell already in §2z-s). | Whether s differences across panels are estimator artefacts; whether amplitude is predictive out of sample (the Q-model claim); whether the s trend is a uniform widening or the already-volatile getting more so. |
| 10 | **Slow-component reparameterisation**: recompute V = σ_perm²/(2κ) from existing era estimates; weak-identification sets for κ by Monte Carlo at each T. No new data contact. | V stable while κ moves → the κ "shift" is a ridge; forecasts and transport bands should be stated in V. |
| 11 | **Profile compressibility**: regress the existing band values on home log-level, per component and platform; exponents with bootstrap intervals; a Houdayer–Hartmann collapse statistic. No refit. | Tests H1 β_perm = β_trans (the size analogue of b = 1), H2 β_obs ≈ ½ (counting noise), H3 a common β across platforms. Failure of H3 is informative; H1 and H2 are the parsimony claims. |
| 12 | **Forensics**: `retrieved_on − created_utc` by month in the Pushshift raw files (WD drive; under the data-intake skill, already-consumed months only); gap-adjacent moments (do observations within three weeks of an absence differ?). | A capture-lag trend would make the comments s trend an instrument artefact. Gap-adjacent differences bias difference-based permanent variance upward; this bears on the shell and tail, not the established cohort, whose events are almost all crossings by observed entities. |

Predictions I would register for this round (directional; numeric tolerances
belong in the measurement plan):
- W1-0/7: drifters are the majority of empirical departure events on both
  Reddit panels; empirical D(104)/D(52) exceeds the simulated ratio in the
  established cohort; the within-train split reduces the deficit without
  removing it.
- W1-8: on FB, the top 5% of simulated weeks carry more than half of the
  simulated-minus-empirical S(1) excess, and bounding the far tail brings
  S(1) to within two seed-SDs of the Gaussian-innovation value while leaving
  the kurtosis moment and the gate inside Monte Carlo noise.
- W1-9: v̂_i is persistent out of sample (train → test rank correlation
  clearly positive in every band), and realised |Δ| is monotone in v̂ decile.
- W1-11: H2 holds within sampling error on Reddit; H1 holds where b ≈ 1 does.
If W1-0/7 fail, B1 is wrong and the jump/regime branch is the live one. That
outcome is as useful as the other.

### W2. Inference and evaluation infrastructure (parallel with W1)

1. **Identification table.** The Andrews–Gentzkow–Shapiro sensitivity matrix
   is the NNLS pseudo-inverse on the active set: per parameter, which moments
   carry it, and how much of κ rests on D(26)/D(52).
2. **Whole-pipeline recovery, registered as an ADEMP study.** Simulate at the
   frozen spec; push through the real universe selection, permanent-rank
   estimation, aggregation and noise-floor construction; report bias,
   interval coverage, boundary pile-up, and the rate of inferring
   heterogeneity when s = 0. Then **model recovery**: plant each W1 candidate
   mechanism and show the W1 statistics tell them apart. Without this, W1
   outcomes cannot be read.
3. **Gate operating characteristics.** On synthetic panels: P(pass | model
   true), P(pass | each ablation true), P(baseline passes). This is the
   missing evidence that the adjudicator can reject.
4. **Parametric-bootstrap intervals** on band values and on every headline
   moment, with re-estimation inside each resample, and the two-step
   uncertainty from Spec-B.
5. **A coherent forecast filter.** The conditional forecast uses a scalar
   level filter with the transitory component folded into noise and its state
   started at zero. Compare, on the same origins, a filter over the model's
   actual slow/fast/medium states with a t-robust (score-driven) update. This
   is the one item aimed squarely at endpoint-level skill.
6. **Evaluation suite v2 — supplementary, frozen before first use.** Gate v1
   is computed verbatim everywhere and keeps its role. Added: an
   endpoint-level primary (fair CRPS of log-rank from per-entity predictive
   samples, exits treated as a censored state); event scores (Brier for
   "outside the top-k within h weeks"); a baseline set (no-change; rank-bin
   transition kernel; per-entity local level; pooled shrunk AR; a
   gradient-boosted learner as predictability ceiling; the Iñiguez et al.
   two-parameter model); paired block inference; and "at par" defined as an
   interval inside a pre-set margin instead of a failure to detect a
   difference. v2 scores on already-seen panels are labelled retrospective.
7. **Independent reference implementation** of the MD estimator and the gate
   scorer by the second model, checked against fixtures. Cross-model review
   has caught execution errors here repeatedly; it cannot catch choices both
   models share.

### W3. Candidate changes — at most one per residual, licensed by W1

| Residual | Licensed by | Minimal candidate | Free parameters |
|---|---|---|---|
| Departure deficit, drift reading | W1-0, W1-7 | slower or fractional home; one memory exponent replacing κ(z) and the medium component | −1 to 0 |
| Departure deficit, excursion reading (gradual recovery) | W1-1, 2, 3 | home innovations share the fast t df at fixed variance | 0 |
| Departure deficit, excursion reading (abrupt recovery) | W1-1, 4, 5 | reversible activity regime with onset and return hazards read off the catalogue | 2 |
| Seasonal returns | W1-6 | measured per-entity seasonal profile removed before estimation | 0 stochastic |
| Exit composition | already measured | identity and home retained through absence (replaces absence-as-death) | 0 |
| Head law | W1-8 | bounded far tail at a declared empirical quantity; time-averaged seeding | 0 |
| Profiles | W1-11 | fluctuation-scaling profiles (~10–15 numbers) vs the full band set | ~500 → ~10–15 |
| Vintage | W1-9, 10 | state transport in V; rolling re-estimation as declared forecast policy | 0 |
| Endpoint skill | W2-5 | coherent filter; v̂ conditioning | 0 |

Every candidate carries pre-registered predictions on the statistic it
targets *and* on what it must not worsen: head share, VR13, the departure
rate, the movement gate. Mechanism verdicts stay separate from adoption
verdicts. The compressed-profile comparison is the one I would run first
whatever W1 says: it removes freedom, it should stabilise short train
windows, and a practically equivalent model with a fortieth of the numbers is
the result goal 3 asks for.

If the drift reading holds, a theory branch opens that is worth stating now:
a home that wanders needs something other than a fixed anchor to keep the
ladder stationary — a rank-conditional restoring drift (the Atlas mechanism)
or boundary flux. That would move the claim from "maintains a supplied
ladder" toward "generates stability with churn", which is the program's
title. It is a larger change than anything else here and should be scoped as
its own registered step, not folded into a residual fix.

### W4. Confirmation and breadth

1. **Wikipedia protocol, frozen before contact.** Entity = page ID from the
   dumps, namespace 0, agent = user; registered instrument boundaries; a
   pre-declared cap or alternative coverage target for K; closed cohort
   primary with births analysed separately; weekday removal fixed in advance
   for Spec-B; topic-clustered bootstrap; Gate v1 and suite v2 both computed.
   Predict the misfits in advance (power-law burst decay; near-zero true
   deaths, which makes "crossing share" a headline diagnostic).
2. **Lockbox schedule.** Wikipedia has years and language editions. Declare
   which block tests the current frozen generation and which blocks stay
   sealed for its successor, so the one clean target is not spent in a single
   pass. Three transport arms, kept distinct: frozen parameters; narrowly
   specified recalibration; same architecture refit.
3. **Further systems, chosen by which assumption each stresses**: GitHub
   stars 2015–2024 (the comparator's most open system; head-to-head with
   Iñiguez et al.); package downloads (machine-driven demand, strong growth,
   near-zero sampling noise); sports rankings as negative controls where the
   noise instrument should return ≈ 0; YouTube (YouNiverse) only as
   Instagram-tier censored evidence. Access terms change fast; verify before
   committing.
4. **Benchmark against the displacement-plus-replacement model** on the
   ranking community's own statistics (flux, turnover, rank diversity,
   re-entry), out of sample. Expect to win on memory and re-entry and to lose
   on head flux until the deficit is fixed; report both.
5. **Instagram allocation build** proceeds on its own track as designed;
   exploratory for 2023.

### W5. Paper

No change to the binding claim set from this document. Three things the
agenda could add to it if they come out: a measured scaling law for
volatility by size with the amplitude factorization as its companion; an
endpoint-level predictive result; and a scorecard relabelled by targeted vs
untargeted rows with comparator columns, so no count is headlined.

---

## Part E — Sequencing, roles, decisions

**Order.** W0 → (W1 ∥ W2) → W3, one candidate at a time → W4 freeze → W4
execution → W5. W1-0, 7, 8, 10 and 11 are the cheapest and bear on the most;
start there. W2-2 (recovery) must exist before W1 outcomes are used to
choose a candidate.

**Two models.** For each package one model writes the estimand, equations
and predictions and the other independently checks identifiability, leakage
and the observation mapping before any outcome is seen; roles swap on the
next package. Reference implementations are written blind (W2-7). Review
rounds are budgeted: the July prereg took seven amendments and eleven
reviews, most of which caught real defects, but an unchanged result reviewed
again is not progress. Heavy runs go in a user Terminal under `caffeinate`
when a companion session is active.

**Owner decisions.**
1. Commit the July record now; with or without the package batch.
2. Adopt this agenda, the other, or a merge; record the adoption in
   MODEL_STATUS as the section superseding §2x.
3. Whether suite v2 becomes a registered supplementary primary for new
   platforms.
4. Wikipedia: primary window, K rule, and which model generation the first
   lockbox tests.
5. Whether to replace the kurtosis-based t-df moment (undefined at df ≤ 4)
   with a quantile-based one.
6. Instagram allocation build: now or deferred.
7. Whether to extend Reddit past 2023-05 (a new collector, so a new
   instrument era).

**What I would not do.** Add a latent component before W1. Promote gate
statistics or head shares to estimation targets. Run neural SBI on the full
model. Touch Wikipedia before the ledger and protocol exist. Re-open refuted
fixes (thinner t-tails, rank-dependent df, skewed innovations,
time-variance anchors, common volatility) without the new evidence named
above. Chase 15/15.

---

## Part F — Relation to the other model's agenda

Written independently; read after my diagnosis was formed.

**Agreement.** Own-model recovery before interpreting components; the
exit/return audit with right-censoring; a refitted complexity ladder;
bounded comparators including Iñiguez et al.; endpoint-level proper scores;
Wikipedia only after an exposure audit; neural SBI not first; the caution
that "returnable" is not "returned" and that the head-share sign change does
not isolate a Facebook cause.

**In that agenda and not in my first draft — adopted here.** The audit of
the forecast construction (scalar filter, transitory state at zero); the
permanent-rank-estimated vs latent-rank-applied coefficient mapping; the
burn-in arithmetic (at ρ = 0.995, 40 steps reach about a third of stationary
variance); the three-way distinction between maintaining a supplied ladder,
attraction to it, and generating it.

**Here and not there.** The slow-layer conjecture and its two discriminating
measurements; seasonality, calendar clustering and the supply/engagement
split; fluctuation-scaling profiles with three stated hypotheses; the
exponential-moment account of the head law, with the seed-SD evidence already
in the record; V-parameterisation of the slow component; the capture-lag
forensic; gate operating characteristics and the gate ledger; lockbox
scheduling for Wikipedia; the non-uniform implementation of b = 1 across
stacks.

**Difference in order.** That agenda puts recovery first and measurement
second. I would run them together: most of W1 is model-free and cheap, and
W1-0 alone decides which mechanism family the recovery study needs to plant.

---

## Provenance and caveats

- Read: the stochastic-modeling skill; MODEL_STATUS §1, §2x, §2z-v, §2z-z,
  §2z-ac … §2z-ae, §3–§6; project memory; `simulate`, `_tdraw`, the
  `--dist-scores` block, `exit_audit.py` header and event definitions;
  `IG_JOINT_POST_ALLOCATION_PLAN_2026-07-18.md` (opening sections);
  `research_notes.md`. MODEL_STATUS was not re-read end to end; "never
  examined" claims in B2 rest on keyword searches.
- Ran: the repo-root test suite (green). Read parquet schemas only. No model
  fit, simulation or new-data contact.
- The literature notes were produced by seven delegated researchers. Several
  ran out of search budget; many classic references are cited from memory or
  abstract only and are tagged as such in the notes. Nothing from them should
  enter the paper without a primary-source check.
- Every item labelled inference or conjecture above is untested on program
  data.
