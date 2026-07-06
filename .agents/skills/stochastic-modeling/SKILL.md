---
name: stochastic-modeling
description: "Core methodology for creating, testing, and iteratively improving the rank-diffusion stochastic model. MUST be read in full before any session that fits, evaluates, extends, or modifies the model, adds a new dataset/platform, or interprets scorecards/OOS gates. Written by Claude Fable 5 (2026-07-06) to transfer the program's working method to any successor model (Claude Opus, ChatGPT/Codex, or other)."
---

# Rank-Diffusion Stochastic Modeling — Core Skill

You are continuing a mature, externally reviewed research program (PNAS-track).
The model works; the method that made it work is encoded here. Your job is
almost never to invent — it is to **measure, extend, and protect**. The two
biggest dangers for a successor model are (1) breaking committed results while
"improving" something, and (2) reporting a flattering number that the program's
own standards would reject. This skill exists to prevent both.

## 0. Read-first protocol (every session)

1. Read `llm_fitting/MODEL_STATUS.md` — **the canonical record**. At minimum:
   §1 (model), the LAST ~4 dated §2 subsections (current truth), §3 (corrected
   pitfalls), §4 (open items), §6 (paper framing). Newer dated sections
   supersede older ones; when they conflict, the newest wins and says so.
2. Read `llm_fitting/CONFIRMATION_PROTOCOL.md` — registered, frozen
   evaluations for new data. If your task touches new/extension data, this
   document constrains you absolutely (see §4 below).
3. Run the health check before changing anything:
   ```
   python -m pytest tests/ -q          # from repo root; 63 tests green as of 2026-07-06
   ```
   If `python` is not on PATH, use the registered interpreter:
   `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`.
4. If your task involves paper text: `paper/` outline + `llm_fitting/reviews/`
   (external review + revised verdict) contain the **binding claim language**
   (MODEL_STATUS §2x and §6 addenda).

Other key docs: `llm_fitting/research_notes.md` (literature, venue analysis),
`llm_fitting/SI_DISCOVERY_CHRONOLOGY.md`, `DATA_PHASE2_REPORT.md` /
`DATA_INVENTORY.md` (SSD data), `llm_fitting/instrument_eras.py` (canonical
FB era table + low-count-day guard).

## 1. The model in one page

Observed log-activity of entity *i* in week *t* (per-period common factor removed):

```
X_it = h_it + ξ_it (+ ξ2_it) + ε_it
  h  : OU "home"  — slow reversion κ(z) to a rank-conditional level, innovation σ_perm(z)
  ξ  : fast transitory AR(1) — φ, σ_trans(z); Student-t innovations (--t-tails)
  ξ2 : optional medium AR(1) (--two-scale; identified only with D(h) moments)
  ε  : measurement noise σ_obs(z) — identified/bounded from the daily-replication floor (Spec-B)
```

multiplied by **one persistent entity amplitude** v_i (temperament): lognormal,
spread s, scaling permanent and transitory equally to first order (**b = 1 is
the main law**; measured b ≈ 1.02–1.08, not rejected as exactly 1 time-aware).
Plus: stationary common level (`--stat-factor`, ρ_L measured per platform),
Gabaix rebirth at the bottom, all on a **pre-registered top-coverage universe**
(top-K by absence-penalized permanent rank, buffer B = 4K). Rank each week by
observed X. Parameters vary by permanent-rank band ("knots", sparse head knots
pooled). Entry points: `llm_fitting/minimal_rankdiff.py` (in-sample card),
`llm_fitting/rankdiff_kalman.py` (Kalman, OOS gate), plus additive tools
(`scorecard_bands.py`, `community_metrics.py`, `spec_b_sigma_obs.py`,
`surrogate_test.py`, `b_robustness.py`, `membership_robustness.py`,
`era_replication.py`, `weighting_robustness.py`).

**Every parameter is identified from a declared moment or an independent
instrument — nothing is tuned to a score.** Keep it that way.

### Flag glossary (scope conditions matter)

| flag | what | scope/warning |
|---|---|---|
| `--top-k K` | pre-registered coverage universe | K from concentration stats ALONE, never from fit |
| `--temperament` | entity amplitude v_i | s from the variance-dispersion moment only |
| `--min-knot-entities 8` | pool sparse head knots | always on in working specs |
| `--md-lags 6` | MD covariance fit γ0..γ6; estimates κ(z), σ_obs | κ comparisons only at MATCHED md_lags |
| `--t-tails` | Student-t fast innovations, df from within-entity kurtosis | inert if σ_trans ≈ 0 |
| `--md-vr` | add D(h), h∈{2,4,8,13} moments — identifies κ | **LONG panels only**; destabilizes short rolling train windows (regressed the FB OOS gate 0.114→0.179) |
| `--md-vr-long` | add D(26), D(52); needs T ≥ 2.5h | separates directional slow movement from diffusive wander |
| `--stat-factor` | stationary common level (ρ_L measured) | recommended on all in-sample goal-1 runs; absent from cohort/OOS sims by construction |
| `--two-scale` | medium transitory ξ2 | requires `--md-vr`; helped FB (14/15), NOT comments |
| `--mix-hetero` | b = s(h*)/s(1) scales σ_perm by v^(b/2) | b=1 is the main model; `--mix-b-fix 1.0` imposes it under CRN |
| `--spec-b` | pin σ_obs to the **centered** daily noise floor | the centered floor pᵀCΣCp is canonical (§2r); legacy floor is reproduction-only |
| `--conditional state\|vhat` | cohort sim from filtered end-of-train state | the lever that made subs beat persistence; does not help FB Spec-A |
| `--oos` | rolling-origin distributional movement gate | THE acceptance criterion |
| `--dist-scores` | CRPS skill, PIT coverage, W1 reference on the gate | descriptive add-on; frozen criterion unchanged |

## 2. The epistemic contract (non-negotiable)

These rules are the program. Violating them produces numbers that will be
retracted later (it has happened; see the band-alignment bug and the 2n
b-sensitivity retraction).

1. **The OOS movement gate is the only adjudicator.** Rolling-origin (≥5
   splits), distributional, vs the persistence baseline, with bootstrap-CI
   coverage. In-sample scorecards NEVER justify adopting a model change.
   **Never report single-split OOS numbers** (a 67/33 split once gave a
   flattering 0.081; the honest 5-split answer was 0.29 ± 0.16).
   Denominator rule: MOM_FLOOR = 0.02 (moments with |emp| below it are
   excluded from the rel-err mean).
2. **Metric hierarchy (binding, §2z-a).** Tier 0 = OOS gate (adjudicates).
   Tier 1 = 15-card + churn + boundary pair + Q (descriptive; thresholds never
   gate; Q localizes residuals via per-block Q/df and is never pass/fail).
   Tier 2 = community/presentation curves (C(r), d(k), F/ō, ladder, rolling
   R2/Pers, kernels, CRPS/PIT) — NO thresholds; a Tier-2 signal is promoted to
   a *measured residual*, never to a new pass/fail row. Tier 3 = one-off probes.
3. **No spec-fishing.** Every parameter comes from a declared moment or an
   independent instrument. If two conventions/tie-breaks are possible, resolve
   by EXTERNAL identification (as Spec-B did for σ_obs), never by iterating
   against scores. Calibrated-and-declared is acceptable (Spec-A σ_obs scale);
   silently tuned is not.
4. **Measure before you code.** Every hypothesis gets a cheap direct
   measurement before any implementation (the comments-VR investigation killed
   3 hypotheses by measurement before the real fix). When a residual appears,
   suspect in order: (a) estimation identification failure, (b) measurement/
   scoring artifact, (c) MC noise, (d) genuine missing structure. Historically
   ~2 of 3 "structural" residuals were (a) or (b).
5. **Pre-register predictions.** Before running a new spec, write down what
   should happen if the mechanism is right (numbers, directions). Score the
   predictions PASS/FAIL in the writeup. Failed predictions are reported, not
   buried — several of the program's most valuable results are failures
   (e.g., "Spec-B pinning fixes comments VR" — FAIL → localized a structural
   deficit).
6. **Defaults stay byte-identical.** All new behavior behind opt-in flags;
   rng streams gated so flag-off runs are bit-reproducible; the **legacy
   guard** (facebook legacy panel, default settings: **14/15, churn 0.013**)
   and the full test suite must pass at every commit. New estimators get
   exact-recovery + noisy-panel-recovery unit tests.
7. **MC noise discipline.** Head-collision rows (coll1) have seed SD ≈ ±0.15
   (~±0.07 SE at 5 reps); churn err carries ±0.03–0.04 from it. Never
   interpret 5-rep head-churn point-diffs without bands; use reps ≥ 20 or
   `scorecard_bands.py` (entity/block bootstrap + MC bands) for any claim
   about the head. Prefer common random numbers (CRN) when comparing specs.
8. **One change per experiment.** Compare against the recorded baseline spec,
   same seeds, same universe. Validation tables list baseline → change →
   verdict per panel.
9. **Documentation protocol** (full conventions in the companion
   `model-status-authoring` skill). Append a new dated subsection to
   MODEL_STATUS.md §2 for every substantive session (format: what was asked,
   diagnosis with measurements, the change, pre-registered predictions scored,
   validation table, adoption verdict *by the pre-declared gate*, reproduction
   commands). Never rewrite history — supersede explicitly ("SUPERSEDED by
   §2x"). Record exact CLI commands. Internal language is confident-and-
   correct, not hedged; paper language follows the §2x binding claim set.
10. **Honest reporting.** If a run fails, times out, or contradicts the
    expectation, that IS the result. Do not re-run until it looks better.
    Distinguish "measured" from "conjectured" in every writeup.

## 3. Frozen per-platform specs (§2s; do not silently change)

Two estimands, two stacks BY DESIGN (D(h) moments identify slow structure on
full panels but destabilize short rolling train windows — a stated
sample-size scope condition, not a spec search).

| panel | structure stack (in-sample card) | movement stack (OOS gate) |
|---|---|---|
| FB Era A (`facebook_a`, K=3500) | full: `--temperament --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long --stat-factor --two-scale --mix-hetero` → **15/15, churn 0.018** | **Spec-B (centered) + `--conditional state`** → 0.118 ± 0.038, cov 60%, beats persistence 4/5, scale 1.0×5 (zero calibration freedom). Sensitivity spec: calibrated Spec-A md6+t → 0.114 ± 0.046, beats 5/5 |
| Reddit comments (`reddit_comments`, K=12500) | LONG: `--md-vr-long --stat-factor --two-scale --mix-hetero` (+temper/pool/md6/t) → 12/15 | md6+t+mix + `--conditional state` → 0.159 ± 0.070, cov 100%, at par |
| Reddit subs (`reddit`, K=5000) | 2d/2e: temper+pool+md6+t → 14/15 | same + `--conditional state` → 0.118 ± 0.061, cov 100%, beats 4/5 |

Canonical reproduction commands are at the end of each MODEL_STATUS section
(§2l, §2p, §2z have the current ones). Interpreter used for registered runs:
`/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`.

**Success criteria for a NEW ranked system** (breadth, the program's next
phase): the claim to test is **amplitude collapse** (s(h) flat in h, b ≈ 1)
**+ OOS-at-par-or-better vs persistence** — NOT 15/15. A 10–12/15 card with a
clean gate and transported parameter shapes (κ head→tail declining, Spec-B
floor shape, t_df ~4–7, s in a plausible metric-dependent range) is a
confirming result.

## 4. Workflow A — adding a new dataset / platform

This is the most likely future task. Order matters; several steps are
irreversible once data has been looked at. **The companion `data-intake`
skill (same skills directory) has the detailed procedure** — drive layout,
schema contract, aggregation runbook, validation commands; this section is
the summary.

1. **Register BEFORE looking.** If a confirmation protocol applies (it does
   for the comments 2021-07..2022-12 extension), run its §3 evaluations
   EXACTLY, no more, no less. No threshold/tolerance/universe rule may change
   after any data row has been read. Amendments only as dated commits strictly
   before processing. Failures are reported as confirmation evidence — they
   are findings about the law's domain, **never prompts to refit**. For a
   brand-new platform, write and commit the analogous mini-protocol first:
   universe rule, stacks, E1 parameter-transport bands, E2 gate criterion.
2. **Data intake & instrument forensics** (before any fitting):
   - Schema contract + weekly=Σdaily invariant checks (pipeline in
     DATA_PHASE2_REPORT.md).
   - Run instrument-health series (pages/day, pages/week, new-ids/week —
     `instrument_eras.py` pattern). Segment by collection health from
     metadata ONLY, never from model fit. Never bridge bad eras with any
     window (membership, OOS splits, displacement horizons, filtered inits).
   - Low-count-day guard for daily/Spec-B work (flag days < 60% of trailing
     28-day median; drop weeks containing flagged days from daily estimation;
     KEEP flagged weeks in weekly fits).
   - Classify census vs censored sample. Census (Pushshift Reddit): coverage
     shares are platform statements; absence = below-floor activity. Censored
     (CrowdTangle FB, IG): all coverage language is "of tracked activity";
     absence is NOT behavior; entry/boundary metrics are within-panel
     quantities. Never compare coverage percentages across the two types.
3. **Universe construction.** Pre-register K from concentration stats alone
   (K90 typical); B = 4K buffer; membership by **absence-penalized permanent
   rank** (absent weeks at floor N_t+1), train-window-only inside the OOS
   gate. On long panels (T ≳ 100), report membership sensitivity
   (full/half/trailing windows — `membership_robustness.py`; drift is real
   but headline-invariant on comments).
4. **Estimate with the standard stack** (short panel: md6+t; long panel: add
   md-vr-long etc. per §3 scope table). Check the known weak-identification
   signature: φ→0 makes AR(1) transitory ≈ iid noise (head σ_obs → 0.000 or
   pinned at grid edge, RACF over-persistent) → resolve with Spec-B pinning
   if daily data exists, never with tie-breaks.
5. **Evaluate in fixed order:** (a) parameter transport vs registered bands;
   (b) OOS movement gate (+ `--dist-scores`); (c) descriptive card with
   bands + per-block Q (`scorecard_bands.py`); (d) community/Tier-2 layer
   (`community_metrics.py`) for localization and figures.
6. **Document** per §2 rule 9, including negative results and exact commands.

## 5. Workflow B — the improvement loop (when a residual matters)

1. **Confirm the residual is real:** bands/MC (rule 7), not an artifact of a
   spec mismatch (the 2z smoke "errors" vanished at the paper stacks), not
   period-0 initialization (FB Era A starts at the 2020 election week —
   prefer conditional sims or rolling variants for head/identity claims).
2. **Enumerate hypotheses, cheapest measurement first.** Kill each with a
   direct measurement before writing model code. Template: §2i–§2l (four
   layers: unidentified κ → integrated factor → missing timescale → mix
   heterogeneity; two of four were artifacts).
3. **Prefer fixes that REMOVE freedom:** identification moments (D(h)),
   corrected mis-specifications (stat-factor), constraints (the planned
   Eulerian stationarity moment), restrictions (b=1). New latent components
   are last resort and need a measured moment they alone can produce.
4. **Implement additively:** opt-in flag, gated rng, exact + noisy-recovery
   tests, legacy guard green.
5. **Pre-register predictions → validation table across ALL platforms**
   (a fix that helps one panel and regresses another gets scoped, not
   defaulted — cf. md-vr).
6. **Adoption verdict by the gate**, in-sample only as description. Update
   MODEL_STATUS with the cumulative decomposition of the residual.

## 6. Pitfall catalogue (each cost real time; do not re-learn)

**Estimation / statistics**
- **Eulerian selection bias** (the program's #1 recurring trap): conditioning
  on *current* rank oversamples transient spikers → σ inflated ~3× → runaway
  diffusion. Always estimate by **permanent (time-averaged) rank**; membership
  must be absence-penalized (observed-week mean rank re-admits the bias —
  ghost-spiker test locks this).
- **Band alignment:** sorted-mask-on-unsorted-columns scrambled rank bands and
  flattened every rank curve (made a bad model look 15/15). Regression-locked;
  beware any new positional mask.
- **Weak identification φ→0** (AR fast ≈ iid noise) and **κ flat-SSE** (OU
  tail spread thinly over short lags): both produce confident nonsense
  parameters. Cures: external σ_obs pin; D(h) moments. κ is lag-window
  sensitive — compare only at matched md_lags.
- **Spec-B floor convention:** week-mean centering annihilates the all-ones
  Toeplitz direction (CJC = 0); only the CENTERED floor pᵀCΣCp is invariant.
  The legacy min-norm floor violated Spec-A ≥ floor on FB.
- **Near-zero empirical moments** explode relative errors (coll1 = 0 split
  scored ~5772) — MOM_FLOOR = 0.02 handles it; check any new metric for the
  same failure mode.
- σ_obs language (binding): identified in **shape** everywhere; identified in
  **level** at the FB head only; **bounded** in [centered floor, Spec-A]
  elsewhere; below the universe head, bounded pending an intermittency-aware
  floor.
- Scored-VR-type functionals are NOT spectrum-determined: phase-random
  surrogates with the empirical spectrum overshoot scored VR13 by +0.04 —
  half the comments residual is functional/marginal-structure, honest
  dynamics target ≈ +0.04. Don't chase functional artifacts with dynamics.
- Pers{h} card rows are period-0 anchored (first-week statistics) and carry
  no empirical band (declared harsh in Q); rolling variants exist in the
  Tier-2 layer.
- dRank population mismatch: card pools rank ≤ 200, OOS gate caps at 100 —
  harmonize or rename before submission (flagged §2z).

**Data**
- FB: NEVER `fb_ranked_weekly.parquet` (corrupt after ~88 wks); use
  `fb_ranked_weekly_cutdown.parquet` (legacy) or SSD `fb_weekly_rebuilt`
  **Era A only** for headline claims (eras B/M unusable, C replication-only,
  D = 2 complete weeks, daily-stats only). Era table: `instrument_eras.py`.
- SSD data needs `/Volumes/T9` mounted (`data/ssd` symlink); comments
  extension raw data needs the WD drive ("My Passport for Mac"). If a drive
  is unmounted, say so and stop that thread — don't substitute other data.
- `fb_weekly_rebuilt` ids are page NAMES (name churn reads as exit+entry).
- Instagram = negative control ONLY ("a"-query censoring); never calibrate to
  it. IG: use `user_name` as id, top-50k pre-cut + `--max-rank-filter 20000`.
- Reddit `metric_value`: submissions panel = submission karma; comments
  panels = comment karma — different metrics, different s (0.94 vs 0.69); s is
  (metric, estimand)-dependent, never assume it transports.

**Environment / code**
- Tests import `archive.src.rankdiff...`; run `python -m pytest tests/ -q`
  from repo root. Production-ish packages in `Python/rankdiff/`, `R/rankdiff/`
  may LAG the research line — `llm_fitting/` is the active truth.
- Long runs: `python -u` when redirecting; OOS gates on big panels take
  minutes-to-hours — don't kill them early.
- zsh (Claude Code Bash tool): unquoted multi-word variables stay ONE token;
  argparse swallows them silently — identical sweep outputs are the red flag.
- Simulator internals: arrays sized `n_sim` (not `n_global`) for rng;
  persistence top_ids uses -1 sentinel — filter before set intersection.

## 7. Current state & parked items (as of 2026-07-06)

**The agenda (§2x, in order):** (1) confirmation extension E1–E4 (owner-gated:
WD mount) — **do NOT let anyone submit the paper before this**; no model work
first; (2) breadth — new ranked systems through the UNCHANGED pipeline
(Wikipedia pageviews first); (3) manuscript claim-set rewrite per the revised
verdict; then figures/SI. Paper outline lives in `paper/`.

**Open, measured, waiting:** FB stationary head law too wide (subsumes the
§2o top-2-gap story) — ADJUDICATED §2z-b: Era A CONFIRMED at the paper spec
(S(1) emp 0.017 vs sim 0.047 ± 0.013, ~2.7×; an isolated residual, everything
else healthy), comments NOT confirmed (within ~1 seed-SD). Protocol amendment
A2 registers E5 (stationary head-law diagnostic on the extension); the
candidate fix — an Eulerian stationarity CONSTRAINT on the MD vector (removes
freedom, opt-in) — is implemented ONLY if E5 fires cross-platform. Related
measurement lesson: D_ladder is level-sensitive on non-stationary-level
panels; share statistics (S(k), D_share) are the level-robust primaries.
κ_i heterogeneity (true
log-SD ≈ 0.30) sits behind protocol E4 (Spearman ≥ 0.20 AND concentration
≥ 1.3, train→extension). Comments VR13 residual is decomposed (~0.03
non-Gaussian marginal + ~0.03–0.04 spectrum) — two small targets, neither
justifying a lifecycle state.

**Dead / parked (do not resurrect without new evidence):** missingness/
population-matched-scoring explanation of comments VR (refuted §2w);
"just use finer rank bands" (refuted §2c); common time-varying volatility
factor (flat through the 2024 election); constant-drift lifecycle demeaning
(§2p H2); lifecycle-arc language stronger than "excess low-frequency
structure" (§2v surrogates); burst/near-tie head-churn machinery (§2o/§2t:
within MC noise given real state).

## 8. Handoff hygiene for successor models

- If you are not certain a number is in MODEL_STATUS, grep for it before
  citing it. Do not reconstruct results from memory of this skill — the skill
  describes method; MODEL_STATUS holds the numbers.
- When your session produces results, write the dated MODEL_STATUS section
  BEFORE ending the session, and update the paper outline only per the §2x
  binding claim set.
- If you find yourself wanting to change a default, a threshold, a frozen
  spec, or the confirmation protocol: stop and ask the owner. Those are
  owner-gated by construction.
- When in doubt about whether a result is real: more seeds, bands, and one
  independent instrument beat any amount of reasoning.
