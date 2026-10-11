# Program state — one-page orientation

**As of 2026-10-11.** This page is a digest for starting a session quickly.
It goes stale; MODEL_STATUS.md is the record and wins every disagreement.
A session that finds this page out of date says so and corrects it in the
same commit as its own record entry. It is rewritten whenever the owner
adopts an agenda, re-freezes a spec, or a dataset changes status in
`EXPOSURE_LEDGER.md`.

## Where the program is

The owner is restarting the program in two phases (direction of 2026-10-11):

1. **Development on data already seen** — go as far as possible on the
   DEVELOPMENT panels toward the best trade-off between reproducing the
   stable rank-size structure, predicting individual endpoint movement, and
   parsimony.
2. **Evaluation on new data** — the Reddit 2023-01..2024-06 archive
   ("bridge") and Wikipedia pageviews, each under a protocol frozen before
   any contact.

Agenda status: §2x (2026-07-05) is the last agenda the owner adopted, and
most of it is done. Two proposals for what comes next were written
independently on 2026-10-08 and neither is adopted:
`RESEARCH_AGENDA_2026-10-08.md` (recorded in §2z-af) and
`RESEARCH_AGENDA_CLAUDE_2026-10-08.md` (§2z-ai). Read both before proposing
work; do not treat either as binding until MODEL_STATUS records an adoption.
Adoption sets priorities and any registered plan. Measurements and
diagnostics on DEVELOPMENT data do not wait for it.

## Evidence to date

| what | result | record |
|---|---|---|
| Frozen stacks: cards and gates on four panels | cards 8–14 of 15; gates at par or modestly better than historical mobility | §2z-g; `frozen-specs.md` in the stochastic-modeling skill |
| Registered confirmation, comments extension, frozen parameters | MIXED EVIDENCE: movement gate passed (0.032 vs 0.171, one 34-week block); parameter transport failed on s and κ; head-law trigger fired | §2z-q, §2z-r |
| Outcome-unseen backtest, long submissions panel | eight registered items passed; s-trend (P4a) and functional VR share (P9b) failed; departure deficit replicated | §2z-ae |
| Instagram 2023 dailies (P10, P11) | NOT ADJUDICATED: wrong observational unit in the registered build | §2z-ae |
| Binding claim paragraph for the paper | adopted by the owner | §6 addendum 2026-07-12 |

## Open residuals (measured)

1. **Established-departure deficit.** For the cohort with train-window
   permanent rank ≤ K/2, weekly exits from the top-K are under-produced
   2.3× on comments and 2.74× on submissions (3.3–3.4× for ≤ K/4). Empirical
   events are about 98.6% crossings by still-observed entities; simulated
   ones are 14–52% permanent deaths. Whether crossings are short
   excursions, slow drift toward the boundary, or one-way declines is
   unmeasured (§2z-ac, §2z-ae P7, §2z-ai).
2. **Head share.** S(1) over-produced on FB Era A (about 2.7×), on the
   comments extension, and on the Instagram rescue; under-produced on long
   submissions. The sign difference shows heterogeneity and does not isolate
   a cause (§2z-b, §2z-d, §2z-q, §2z-ae P8, §2z-af).
3. **Parameters are not stationary.** s drifts by era and differs by metric;
   κ levels moved 4–8× into the extension, with identification at that
   panel length in question (§2z-q, §2z-s, §2z-ae, §2z-ai).
4. **No endpoint-level predictive score exists.** The gate and
   `--dist-scores` both score pooled displacement distributions; frozen
   parameters under-disperse upper movement tails (§2z-q, §2z-ai).
5. **VR13** slightly too persistent in simulation; partly a property of the
   scored functional (§2v, §2y, §2z-ae P9).
6. **Reversion-rate heterogeneity** κ_i is real (log-SD about 0.30) and did
   not predict out of sample (§2z-q E4, §2z-s).
7. **Parsimony is unmeasured.** About 500 moment-estimated band values over
   about 55 knots have never been tested against a low-dimensional form
   (§1, §2z-ai).
8. **Breadth.** Two platforms and three metrics, plus a censored Instagram
   demonstration.

## Refuted or withdrawn — not to be re-proposed without new evidence

| idea | status | record |
|---|---|---|
| Finer rank bands as the explanation of within-band volatility dispersion (a 200× refinement explains 1–2% more) | refuted | §2c |
| A common time-varying volatility factor | rejected by measurement | §2d |
| `--md-vr` on short rolling train windows | regresses the gate; long panels only | §2i |
| Lifecycle drift as a constant trend | dead | §2p (H2) |
| "Lifecycle arcs" as more than "excess low-frequency structure" | language not supported | §2v |
| Missingness or population-matched scoring as the comments VR explanation | refuted | §2w |
| Burst or near-tie machinery for head churn | within Monte Carlo noise given the real state | §2o, §2t |
| Per-post Instagram variant | fails by the φ → 0 degeneracy | §2z-c |
| Within-entity level-variance stationarity anchor (`--eul-level`) | refuted by its own adoption gates | §2z-t |
| Rank-dependent t df; skewed fast innovations | refuted by head kurtosis and skew | §2z-v |
| Thinner fast-innovation tails | refuted: empirical standardized tails are fatter in every powered stratum | §2z-x, §2z-y |
| κ_i as a predictive lever | not predictive on the extension | §2z-q, §2z-s |
| In-panel parameter-vintage policy as the tail fix | harmless, and unable to power the test | §2z-v |
| "Core entities exit too often" | reversed under a permanent-rank cohort | §2z-z |
| "Exit machinery is exonerated" | withdrawn | §2z-ab |
| "The partition-anchor family is dead" | withdrawn; only the time-variance anchor is refuted, the cross-sectional or head-ladder reading is open | §2z-w |

## Not yet examined

By keyword search of MODEL_STATUS on 2026-10-08 (verify before relying on
it): seasonality or annual periodicity; cross-sectional dependence beyond
the single common factor; within-entity volatility clustering; whether large
drops sit in item supply or in per-item engagement on FB and Reddit (the
count columns exist); dispersion beyond 52 weeks; excursion durations and
return times; recovery of the full pipeline on its own simulated data; a
moment-sensitivity exhibit; the gate's ability to reject a wrong model.

## Data

`EXPOSURE_LEDGER.md` is authoritative. In short: all FB, Reddit 2018-12 to
2022-12, the short Reddit 2024-07 to 2025-01 panel and the Instagram 2023
panels are DEVELOPMENT. The Reddit bridge (2023-01 to 2024-06) is SEALED
and still downloading. Wikipedia 2025-01 is EXPOSURE UNKNOWN; nothing else
from Wikipedia is acquired.

## Decisions waiting on the owner

- Which agenda (or merge) to adopt.
- Whether endpoint-level proper scores and named baselines become a
  registered supplementary evaluation for the new datasets.
- Wikipedia protocol choices: entity key, primary window, universe rule and
  cap, which model generation the first block tests.
- The Reddit bridge protocol: source-transition and score-timing forensics
  before any karma panel.
- A standing margin for judging a candidate change against the current
  model on the gate. The skill uses the registered convention (0.05, 20
  reps) as a labelled stand-in; the paired-seed noise of the gate has not
  been measured, and that measurement is what a tighter margin should rest
  on.
- Whether to replace the kurtosis-based t-df moment.
- The b = 1 implementation gap between the main law and the frozen movement
  stacks (`frozen-specs.md` §5).
- A pandas-3 fix for the registered intake tooling (§2z-ai).
- The Instagram joint-post allocation build
  (`IG_JOINT_POST_ALLOCATION_PLAN_2026-07-18.md`).
- Enforcement and provenance tooling proposed in the skills review of
  2026-10-11 (§2z-ak) and not built: an agent hook or permission rule that
  blocks reads of SEALED paths; a machine-written run ledger; a manifest
  linking each published number to its run; hash-pinned evaluator files; a
  blind-verification harness; a backfill of `GATE_LEDGER.md`. The owner
  decided on 2026-10-11 that the skill scenarios need not be run under
  Codex.
