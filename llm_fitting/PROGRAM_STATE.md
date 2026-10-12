# Program state — one-page orientation

**As of 2026-10-13.** This page is a digest for starting a session quickly.
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

**The working agenda is
`RESEARCH_AGENDA_MOVEMENT_TO_STRUCTURE_2026-10-12.md`, adopted by the owner
on 2026-10-13 (§2z-bp).** Its question is whether movement and turnover
measured without the ladder explain the ladder. It replaces Phase 2 of
`WORKPLAN_CLAUDE_2026-10-12.md`, which was adopted on 2026-10-12 (§2z-aq,
§2z-ar), narrowed the two October agendas and replaced §2x; Phase 1 of that
plan is complete and its results are listed below. Claude leads and Codex/Astra
validates independently under `coordination/PROTOCOL.md`; Codex's tasks
are in `coordination/QUEUE.md`. Phase 1 has no model code and no candidate
gate evaluations. Phase 1 results so far: §2z-as (A5), §2z-at (A4), §2z-au
(A1, A2), §2z-av (A3), §2z-aw (A7, follow-ups added to the plan), §2z-ax
(Codex's V1, the gate as an instrument), §2z-ay (A8, reversion switched
off: the home both pulls endpoints back, which the data contradicts, and
holds the ladder, which it needs), §2z-az (the card under two seedings),
§2z-bc (Codex's V2: independent simulator, recovery, planted mechanisms)
and §2z-bd (what V2 changes in the entries before it), §2z-be (the debate
on candidate 2, closed), §2z-bf (A9: lasting variance over the ladder's
local slope is close to constant across ranks), §2z-bg (Codex's V3:
endpoint scores at nominal rows; a timing discrepancy in the gate's
conditional simulator, flagged for the owner). §2z-bh (A10: the
variance–structure relation both ways; it holds in shape with noise
handled, and what it lacks is an independent measurement of its one
constant; its "factor of three" against carrying the ladder forward and
its reading of 1/g as an adjustment time are corrected in §2z-bk).
§2z-bi (Codex's review R1 of the D1 document), §2z-bj (Codex's
independent recomputation of the variance–slope correspondence, its
review of A10, a proposed agenda and three proof targets), §2z-bk
(corrections after both reviews: A10's holdout comparison, A4's
half-sample reliabilities), §2z-bl (the proposed next phase: PROPOSED,
not adopted), §2z-bm (Codex's first proof result: the instantaneous
variance of the ladder formula is not identified from weekly data in
general) and §2z-bn (Claude's review: no gap found; the consequence is to
measure matched finite-horizon moments and crossings instead).
The per-endpoint export for Codex's V3 (A6) is on `main`.

**Where the adopted agenda stands (§2z-bq).** Its first deliverable, one
protocol for steps 1 and 2, is drafted and waits for Codex's review (queue
row R4): `MTS_PROTOCOL_STEPS_1_2_2026-10-13.md`. The statistics and the
runner are built and tested on synthetic panels only
(`balance_accounting.py`, `mts_step1.py`). **Nothing has been run on any
panel**, and the runner refuses a registered panel until the review is
answered. Step 2, planted rivals and a recovery map, is Codex's task V4. The
protocol's first decision is whether step 3 (predicting slopes with no
constant taken from the ladder) is identified on development data.

**The count-identification proofs (§2z-bo).** Proof 7 was reviewed: no gap
found, no existing instrument supplies its experiment, and its certificate
is numerically vacuous. Proofs 4 to 6, 8 and 9 and a second audit of Proof 7
are uncommitted in Codex's debate working copy and are not in the record.
The typed-renewal result on `codex/typed-renewal-proof` is unreviewed.

## Evidence to date

| what | result | record |
|---|---|---|
| Frozen stacks: cards and gates on four panels | cards 8–14 of 15; gates at par or modestly better than historical mobility | §2z-g; `frozen-specs.md` in the stochastic-modeling skill |
| Registered confirmation, comments extension, frozen parameters | MIXED EVIDENCE: movement gate passed (0.032 vs 0.171, one 34-week block); parameter transport failed on s and κ; head-law trigger fired | §2z-q, §2z-r |
| Outcome-unseen backtest, long submissions panel | eight registered items passed; s-trend (P4a) and functional VR share (P9b) failed; departure deficit replicated | §2z-ae |
| Instagram 2023 dailies (P10, P11) | NOT ADJUDICATED: wrong observational unit in the registered build | §2z-ae |
| Binding claim paragraph for the paper | adopted by the owner | §6 addendum 2026-07-12 |
| Step-by-step ladder from a simple rank diffusion to the frozen stacks, five development panels (exploratory) | noise, home and short-lived swings carry 86% of the combined improvement on an equal-weight index of in-sample macro stability and the held-out gate; the amplitude step raises the macro index on all five panels; the reference gates of §2z-g were recorded at 3 reps and score 0.02–0.06 lower at 20 | §2z-ap; `runs/2026-10-12_ladder/` |

## Open residuals (measured)

1. **Established-departure deficit.** For the cohort with train-window
   permanent rank ≤ K/2, weekly exits from the top-K are under-produced
   2.3× on comments and 2.74× on submissions (3.3–3.4× for ≤ K/4). Empirical
   events are about 98.6% crossings by still-observed entities; simulated
   ones are 14–52% permanent deaths (§2z-ac, §2z-ae P7, §2z-ai). Measured
   2026-10-12 (§2z-au): 64–69% of the data's events are exits by endpoints
   already sitting near the boundary, four in five are repeat exits, and
   sudden lasting exits are 1.2–1.4% of events and over-produced by the
   simulation. On comments the deficit is as large inside the training
   window. Counted by endpoint (§2z-aw): 2.5–3.8 times as many relocate
   to the boundary zone as simulated, steps that stick are rare (5% of
   first exits against 17–30% simulated), and year over year the data's
   level moves like a random walk where the simulation's reverts.
2. **Head share.** S(1) over-produced on FB Era A (about 2.7×), on the
   comments extension, and on the Instagram rescue; under-produced on long
   submissions. The sign difference shows heterogeneity and does not isolate
   a cause (§2z-b, §2z-d, §2z-q, §2z-ae P8, §2z-af). Attributed on
   2026-10-12 (§2z-av): on comments and long submissions it is the
   first-week seeding of the unconditional simulation, sign included
   (seeding from endpoint means gives 1.00 and 0.97 of the data); on FB and
   Instagram it is rare spike weeks from the passing shocks, with the
   data's own far tails heavier than the simulation's in standardized
   units and bounded in raw size.
3. **Parameters are not stationary.** s drifts by era and differs by metric;
   κ levels moved 4–8× into the extension, with identification at that
   panel length in question (§2z-q, §2z-s, §2z-ae, §2z-ai).
4. **No endpoint-level predictive score exists.** The gate and
   `--dist-scores` both score pooled displacement distributions; frozen
   parameters under-disperse upper movement tails (§2z-q, §2z-ai). An
   endpoint's own changes are fat-tailed at every horizon from 1 to 52
   weeks and the simulation's are near Gaussian; the simulation's variance
   is 10–21% short at 78 weeks (§2z-au).
5. **VR13** slightly too persistent in simulation; partly a property of the
   scored functional (§2v, §2y, §2z-ae P9).
6. **Reversion-rate heterogeneity** κ_i is real (log-SD about 0.30) and did
   not predict out of sample (§2z-q E4, §2z-s).
7. **Parsimony is unmeasured.** About 500 moment-estimated band values over
   about 55 knots have never been tested against a low-dimensional form
   (§1, §2z-ai). A first measurement on the component side exists:
   the forward ladder of §2z-ap. It does not test the band profiles, and it
   has no leave-one-out. The band profiles were measured on 2026-10-12
   (§2z-at, with its half-sample numbers corrected in §2z-bk): the
   volatility profiles reproduce (0.71–0.97; σ_trans on FB 0.59); a
   straight line in rank reaches 76–97% of the reproducible share for six
   of nine and fails for the lasting volatility on FB and comments; the κ
   profile does not reproduce on FB or long submissions, does on comments
   (0.64), and sits on its slowest grid value at 31–52% of knots, so part
   of the κ shift in residual 3 is a boundary estimate.
   Recovery on simulated data (§2z-bc): fitted reversion has a median
   relative error of 0.9–1.7 and sits at a bound in 46–68% of bands; the
   tail-thickness moment is not self-consistent; the amplitude spread is
   recovered at its frozen value and reads 0.14–0.19 when the truth is 0.
8. **Breadth.** Two platforms and three metrics, plus a censored Instagram
   demonstration.
9. **An endpoint's volatility changes over time.** The half-to-half
   ratio is 0.80–0.85 in the data; only planted time-varying volatility
   lowers it (§2z-aw, §2z-bd). Forcing one amplitude on the lasting and
   passing parts inflates the FB head (§2z-as). The earlier reading that
   long-run and short-run volatility are different amplitudes is not
   established: that statistic detects the annual cycle (§2z-bd).
10. **An annual cycle on Reddit** that the model does not have and that the
    long-difference moments straddle (§2z-aw).

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
it): cross-sectional dependence beyond the single common factor; whether large
drops sit in item supply or in per-item engagement on FB and Reddit (the
count columns exist); recovery of the full pipeline on its own simulated
data (Codex task V2); a moment-sensitivity exhibit; the gate's ability to
reject a wrong model. Dispersion to 78 weeks and return times of departures
were measured in §2z-au. An endpoint-specific annual cycle on the Reddit
panels and volatility that changes over time within endpoints were found
in §2z-aw; how much of the fitted reversion is the cycle is not measured.

## Data

`EXPOSURE_LEDGER.md` is authoritative. In short: all FB, Reddit 2018-12 to
2022-12, the short Reddit 2024-07 to 2025-01 panel and the Instagram 2023
panels are DEVELOPMENT. The Reddit bridge (2023-01 to 2024-06) is SEALED
and its acquisition is STOPPED by decision at 365.6 of 790.6 GB (35 of the
36 archives); a replacement drive is ordered and the WD was ejected at the
owner's instruction, to stay unplugged until it arrives (2026-10-11), so a
missing WD mount is intended. The WD write
failures of October 11 were one bad disk region under
`RC_2023-05.zst`, which fails reads as well as writes (§2z-ao); a restart
at 23:17 that day without that archive ran cleanly for 11 minutes and was
then stopped so the drive is not loaded while it holds CrowdTangle material
with no second copy: 1,817 GB of image archives. The 353 GB of daily TSVs
there are, by the owner's decision, the parquets in another format and are
not backed up. Resume only
with `tmp/reddit_bridge/resume_skip_RC_2023-05.zsh`: the earlier launch
scripts select all 36 files and fail on the same piece. Do not delete or
read through `RC_2023-05.zst` on the WD. A `shasum` of a file on that drive
is valid only if the kernel log shows no disk I/O error during the read.
Wikipedia 2025-01 is EXPOSURE UNKNOWN; nothing else
from Wikipedia is acquired.

By the owner's rule of 2026-10-11 (§2z-am), nothing in a sealed folder is
looked at until the owner says, in the conversation, that the named dataset
may be opened. The folders are listed in `SEALED_PATHS.json`;
`scripts/sealed_data_guard.py` checks every Claude Code tool call against
the list, and other agents are bound by the same rule in `AGENTS.md`.

As of 19:16 Japan time on 2026-10-11 the archive download was stopped,
after a disk write error at 18:01 (§2z-am, observation). Its current state
is in the newest data-operation entries of MODEL_STATUS.

## Decisions waiting on the owner

- **D1, decided so far (§2z-ba):** the seeding change is approved (opt-in,
  structure simulation, time-median ladder; re-freeze still to be decided).
  Candidate 2 is withdrawn by a signed Claude–Codex consensus (§2z-be,
  `handoff/D1_debate/CONSENSUS.md`).
- **Whether the proof line continues.** Claude recommended on 2026-10-13
  (§2z-bp) to stop the count-identification proofs, keep the first proof
  result, and have the typed-renewal result read by a mathematician. The
  owner adopted the agenda and has not ruled on this.
- **The design of the sealed-data test of step 3**: measure movement on a
  new panel and predict its ladder with no constant taken from it. It has
  to be committed before any sealed folder is opened (agenda, step 3).
- **The rest of D1.** `D1_CANDIDATES_CLAUDE_2026-10-12.md` now carries
  Codex's review (§2z-bi) and a current decision list in its second status
  section: the re-freeze of the structure stacks with the seeding option;
  the comparison margin, as a working rule whose procedure has to be
  completed first; whether the 80-seed values become the recorded
  reference gates (three of the four old values were at 3 simulations,
  long submissions at 20); no change proposed to the wording around b = 1;
  whether the annual cycle and a limit on raw jump size wait.
- Whether the frozen gate's conditional simulator, which takes two weekly
  steps before its first recorded week, should be changed (§2z-bg).
- Whether endpoint-level proper scores and named baselines become a
  registered supplementary evaluation for the new datasets.
- Wikipedia protocol choices: entity key, primary window, universe rule and
  cap, which model generation the first block tests.
- The Reddit bridge protocol: source-transition and score-timing forensics
  before any karma panel.
- Moving off the WD when the replacement drive arrives (§2z-ao, addendum
  2): whether the 1,817 GB of CrowdTangle image archives are kept, and the
  copy order proposed there. The bridge download then resumes on the new
  drive, where `RC_2023-05.zst` is fetched fresh; the new location must be
  added to `SEALED_PATHS.json` before any bridge file is copied.
- A standing margin for judging a candidate change against the current
  model on the gate. The owner decided on 2026-10-11 to wait before setting
  a number (§2z-am). Until then the skill uses the registered convention
  (0.05, 20 reps) as a labelled stand-in. Measured on 2026-10-12 by Codex
  (§2z-ax): two 20-simulation evaluations of the same frozen stack differ
  by up to 0.018 (FB), 0.030 (comments), 0.022 (short submissions) and
  0.032 (long submissions) at the 95% level, and 0.013–0.025 at 40
  simulations; the error itself falls 0.008–0.022 from 3 to 20 simulations,
  mostly as the bias of an absolute error. Noise between a candidate and
  the incumbent on shared seeds is not yet measured: no candidate has been
  run.
- Whether to replace the kurtosis-based t-df moment.
- The b = 1 implementation gap between the main law and the frozen movement
  stacks (`frozen-specs.md` §5). §2z-ap adds measurements on both sides (the
  macro index rises at the movement-only amplitude step on five panels;
  `--mix-hetero` is the largest late gate step on comments); the direct test
  has not been run.
- Whether to re-record the four reference gates of §2z-g at 20 reps and
  2,000 bootstrap draws. They were recorded at the command-line defaults (3
  reps) and reproduce exactly at those; at 20 reps the same stacks score
  0.103, 0.137, 0.129 and 0.257 (§2z-ap).
- Whether a leave-one-out from each frozen stack is wanted beside the
  forward ladder of §2z-ap.
- A pandas-3 fix for the registered intake tooling (§2z-ai).
- The Instagram joint-post allocation build
  (`IG_JOINT_POST_ALLOCATION_PLAN_2026-07-18.md`).
- Whether to give Codex the same automatic check that Claude Code now has
  for sealed folders (§2z-am). Until then Codex is held by the written rule
  and the note files only.
- Whether the Wikipedia pilot and future loaders should themselves refuse
  to run on a sealed folder (`require_released`), which catches what a
  check of command text cannot (§2z-am).
- Enforcement and provenance tooling proposed in the skills review of
  2026-10-11 (§2z-ak) and not built: a machine-written run ledger; a
  manifest linking each published number to its run; hash-pinned evaluator
  files; a blind-verification harness; a backfill of `GATE_LEDGER.md`. The
  owner decided on 2026-10-11 that the skill scenarios need not be run
  under Codex.
