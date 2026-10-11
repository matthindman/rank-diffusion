# Work plan (Claude) — more from the models and data we already have

**Status: ADOPTED by the owner on 2026-10-12** ("implement the plan";
MODEL_STATUS §2z-ar). It is the working agenda for the development phase
and replaces §2x in that role. Development data only. Nothing here changes
a default, a frozen stack, Gate v1 or a protocol; those stay with the
owner. Written after re-reading `RESEARCH_AGENDA_2026-10-08.md`
(Codex/Astra), `RESEARCH_AGENDA_CLAUDE_2026-10-08.md` (mine), the record
through §2z-ap, and the ladder explainer of 2026-10-12. Every number below
is quoted from MODEL_STATUS or from
`runs/2026-10-12_ladder/summary.json`; none is new.

How the two agents work together is in `coordination/PROTOCOL.md`; the
tasks for Codex are in `coordination/briefs/`.

## 1. The plan in one page

**Where the error is.** Four things account for most of what the frozen
models still get wrong, and the adoption gate sees none of the first three:

1. too few large, lasting moves (established endpoints leave the top too
   rarely; the upper tail of 13-week movement is too thin);
2. too much activity at the very top on Facebook and Instagram;
3. the endpoint amplitude is applied to half of the model;
4. the gate cannot resolve differences of the size that are left.

**The bet.** Items 1 and 2 may be one error seen twice: the model puts its
fat tails in the passing component, where they make a brief spike that can
take the top slot, and none in the lasting component, where they would make
an endpoint actually move. That is a hypothesis. Two cheap, model-free
measurements decide it, and it is written here so that it can fail.

**The order.** Fix the measuring instrument; measure where the tails live
and what the missing departures are; then test at most three changes, each
of which adds no free parameter or removes many; judge each once, under a
rule fixed before the run.

**The two agents.** Claude leads: residual measurements, the model code, the
record, integration. Codex/Astra validates independently: the gate as an
instrument, recovery of planted mechanisms with its own simulator,
endpoint-level scoring, blind re-implementation and adversarial review. The
agent that wrote the estimator does not also certify it.

**Cost.** About four working days to the first decision point, with no model
code and no candidate gate evaluations before it; about a week for the
candidates after it.

## 2. Where the error is

| # | error | size on record | scored by Gate v1? | source |
|---|---|---|---|---|
| 1 | Too few large, lasting moves | Established departures (train permanent rank ≤ K/2 leaving the top-K) under-produced 2.3× on comments and 2.74× on long submissions. Held-out 13-week 90th-percentile move 37–39% too small on FB from the swings step to the top of the ladder. Comments extension p90 simulated 25/35/51 against 27/41/65. FB boundary out-flux 0.132 against 0.173 at the frozen stack. | No: the gate scores survivor-conditional medians | §2z-ac, §2z-ae P7, §2z-q, §2z-ap |
| 2 | Head share | Simulated top-1 share over the data's: 2.78 on FB and 3.66 on the Instagram rescue at the frozen stacks (5.6 and 7.1 scorecard-tolerance units, the largest single macro errors there); 1.38 on comments; 0.75 on long submissions | No | §2z-b, §2z-ap |
| 3 | Amplitude on half the model | The macro index rises at the amplitude step on all five panels (+4% to +25%); the simulators then scale only the passing parts. Extending it to the lasting part is the largest late gate step on comments (−0.049 ± 0.035, better on 4 of 5 splits). The direct test was not run. | Partly | `frozen-specs.md` §5, §2z-ap |
| 4 | The gate's resolution | After the home and swings steps every paired rung change on FB sits inside its split SD (0.014–0.050). The reference gates were recorded at 3 reps and score 0.020–0.063 lower at 20. The long-submissions gate is dominated by one moment that is zero in three of five test windows. | — | §2z-ap |
| 5 | Comments horizon scaling | VR13 simulated 0.217 against 0.136 (2.5 tolerance units); boundary out-flux 1.42× the data's while established departures are 2.3× too few | No | §2z-ap, §2y |
| 6 | Parsimony unmeasured on the profile side | About 500 band values over about 55 knots; three components carry 86% of the ladder's improvement | — | §2z-ap, open residual 7 |
| 7 | Endpoint-level skill unmeasured | Pooled CRPS skill within ±0.006 of zero at every horizon | — | §2z-q, §2z-ae, §2z-ap |

Row 5 belongs with row 1. On comments the model moves too many endpoints a
little (pooled 13-week variance and boundary churn too high) and too few a
lot (established departures too rare). A process with the same variance and
fatter lasting shocks does both. That is the shape of the bet, not evidence
for it.

## 3. The two agendas, reviewed

**They agree on** own-model recovery before reading components as real; the
exit and return audit with right-censoring; a refitted complexity ladder;
endpoint-level proper scores as a supplement; Wikipedia only after an
exposure audit; no neural inference first.

**Codex/Astra's agenda** is strongest on whether the inference can be
trusted: recovery first, the audit of how the forecast is built (a scalar
level filter, the passing state started at zero, the home set to the last
filtered level), the burn-in arithmetic, and the three meanings of
"structural explanation". Its cost is the order: a full recovery study
before any measurement delays measurements that are cheap, model-free and
decide which mechanisms the recovery study needs to plant. Its packages 4
and 5 (comparator models, stationary-law theory) matter for the paper and
do little for the remaining error now.

**My agenda** is strongest on the residuals: the slow-layer conjecture and
the measurements that separate drift from excursion, tails by horizon, the
V = σ²/(2κ) reading of the κ shift, gate operating characteristics, and the
non-uniform amplitude. Its cost is breadth: thirteen measurements in the
first round. Several are low-yield now (seasonality, supply against
engagement, volatility clustering) and one needs the WD drive, which is
unplugged (capture-lag forensics).

**What the ladder explainer changes.** Five lessons are actionable, and the
plan is adjusted for each.

1. *The gate cannot tell the late rungs apart, and its recorded reference
   values depend on the number of simulations.* So the first task is a
   study of the gate as an instrument, and no candidate is evaluated before
   it reports. This also supplies what the owner's margin decision is
   waiting for.
2. *The amplitude step hurts the macro score everywhere while the code
   scales only half the model.* The amplitude gap moves from a footnote in
   both agendas to the first candidate. The one cell the ladder did not run
   (full amplitude directly after the swings step) is run first, on
   structure only, where it costs minutes.
3. *The head-share excess appears when the swings component enters, with
   Gaussian shocks, and grows again with fat tails* (FB: 1.52 → 2.58 →
   3.38). My agenda blamed the unbounded t alone (B3). That is at most half
   of it. The head measurement becomes an attribution across components,
   not a test of the tail bound.
4. *The 13-week upper tail on FB is unchanged by the amplitude and tail
   steps.* Both steps act on the passing component. This is the sharpest
   existing evidence that the missing large moves are a property of the
   lasting component, and it is what turns the bet into a testable
   statement.
5. *On long submissions the gate is mostly one near-zero moment.* Gate
   differences on that panel are not used to rank candidates until the
   instrument study says what they are worth.

Three predictions of mine failed on the ladder (the size of the home,
swings and amplitude effects). I take from that what the skill already
says: measure before building, and write predictions as ranges.

**Not adopted from either agenda, for now:** comparator models, stationary-
law theory, a reversible-regime component, seasonality, the Instagram
allocation build, anything on new data. A leave-one-out from the frozen
stacks is not planned on the gate; the macro side is cheap and can be added
to A5 if wanted.

## 4. The plan

### Phase 0 — make parallel work safe (half a day)

- Commit what is pending on `main` (the ladder session's files and §2z-ap;
  the data-operation entries §2z-ah, -aj, -al, -an, -ao and their files),
  so both agents start from the same record.
- Adopt `coordination/PROTOCOL.md`. Create Codex's working copy with
  `scripts/agent_worktree.sh` (tested 2026-10-12: a fresh copy passes the
  tests and reproduces the legacy fit, 14/15 and 0.013).

### Phase 1 — instrument and anatomy (about four days)

No model code. No candidate is evaluated on the gate. Each task writes its
design and predictions to a file before its first run.

**Claude**

| id | measurement | cost | what each outcome licenses |
|---|---|---|---|
| A5 | The cell the ladder skipped: swings + full amplitude (`--temperament --mix-hetero`), structure only, five panels, 20 seeds | minutes | Macro loss at the amplitude step disappears → the half-step causes it; candidate C-A goes ahead. Loss remains → amplitude itself strains the variance block; C-A is weaker and the amplitude estimator goes to the recovery study. |
| A4 | From fits already on disk, no refit: regress each band profile on home log-level (one exponent per component per platform, with intervals); recompute V = σ_perm²/(2κ) by era; share of bands on a solver corner or a grid edge in the frozen stacks | hours | A two- or three-number curve explains most of each profile → candidate C-P. V stable while κ moves → the κ "shift" is a ridge, and transport is stated in V. |
| A1 | Where the tails live. Tail ratios and a quantile kurtosis of standardized h-week changes, h = 1 to 52, up and down, by rank group; fraction of a move retained after k weeks, by size and sign. Data against the frozen-stack simulation through one code path, FB Era A, comments, long submissions | a day | Excess gone by h ≈ 4 in data as in simulation → tails are passing; C-T is not licensed. Excess persisting to h = 13–26 in data and not in simulation → fat-tailed lasting shocks; C-T licensed, and the plateau gives its timescale. |
| A2 | What the missing departures are. For every established-departure event: margin to the cut the week before; drift (scoring-window median rank against train permanent rank), excursion (time to re-entry, right-censored) or one-way; first exit or re-exit; the same on a within-train split. Dispersion of long differences beyond 52 weeks where the panel allows | a day | Mostly drift → a slower lasting layer, not jumps. Mostly excursions with gradual recovery → C-T. Abrupt, reversible → the regime component, held in reserve. Deficit shrinks in the within-train split → part of it is parameter drift. |
| A3 | What makes the top too heavy. On the frozen structure stacks (FB, Instagram rescue, comments, long submissions), 20 seeds or more: switch one thing at a time at the head — Gaussian, t, and bounded-t passing shocks; passing component off; homes frozen; first-week against time-averaged seeding — and report the top-1 share each time. Share of the simulated top-1 mean carried by the top 5% of weeks, against the data. These are interventions in a fitted model, not refits | a day | Attributes the excess to passing spikes, to wandering homes, to seeding, or to none, and has to explain the sign change on long submissions. |

A1 and A2 are written as a small module of functions on a panel
(`llm_fitting/residual_anatomy.py`) so that Codex's planted-mechanism study
runs the same statistics.

**Codex/Astra** (briefs in `coordination/briefs/`)

| id | task | why it is theirs |
|---|---|---|
| V1 | The gate as an instrument: error against number of simulations by resampling one long run per panel; the noise of a difference between two evaluations; which moments carry the error and its variance; the smallest difference five splits can resolve. Frozen stacks only. Output: evidence for the owner's margin and for re-recording the reference gates | Independent of the model's author; careful bookkeeping; no judgment about any candidate involved |
| V2 | Planted mechanisms and recovery: an independently written simulator; panels with fat-tailed lasting shocks, a reversible regime, a slower home, or none; pushed through the real pipeline and through A1 and A2. Which statistics can tell them apart at the real N and T, and how often they fire on a model that has none. Recovery of s, κ and the variances, with the share at grid edges | It is the first package of their own agenda; an independent simulator also checks ours |
| V3 | Endpoint-level scores, supplementary and retrospective: per-endpoint predictive samples scored with a proper rule (log-rank with exit as a censored state; probability of leaving the top-K within h weeks), against no-change, a rank-bin transition kernel and the historical-mobility law | Builds the instrument that can see errors 1 and 7; the scorer is not written by the forecaster |

Claude supplies V3 with an opt-in export of per-endpoint simulated and
observed outcomes from the gate code (off by default, bit-identical when
off).

**Cross-checks in Phase 1.** Codex re-computes two of my headline statistics
(the established-departure rate; the tail ratio by horizon) from a written
definition before seeing my numbers. I do the same for V1's noise estimate.
One review round per deliverable.

### Decision point D1

I write a dossier for each candidate the measurements license
(`evidence-standards.md` §6): residual, mechanism, identifying moment,
recovery evidence from V2, predictions with a must-not-worsen list,
parameter count, margin and seed count. Codex reviews each dossier before
anything runs. The owner picks which go ahead and sets or defers the
margin.

### Phase 2 — candidates, one at a time (about a week)

| id | change | free parameters | targets | licensed by |
|---|---|---|---|---|
| C-A | One amplitude for the whole model: `--mix-hetero` in every stack. If raw scaling explodes held-out movement on short train windows, as recorded, shrink each endpoint's estimated amplitude toward 1 by its own sampling variance (no tuned constant) | 0; removes a difference between the stated law and the code | macro index at the amplitude step; gate on comments; nothing worse on short windows | A5, V2 (recovery of s) |
| C-T | Tails where they persist: lasting shocks drawn from the same standardized t at unchanged variance; passing shocks bounded at the largest standardized move in the data | 0 | 13-week upper tail, established-departure rate, top-1 share, comments VR13 | A1, A2, A3, V2 |
| C-P | Compressed profiles: each band profile replaced by a fitted scaling form | about 500 → about 15 | practical equivalence on both scores; fewer estimates at grid edges; steadier short-window fits | A4, V1 (equivalence margin) |

In reserve, each needing its own licence: a coherent forecast filter over
the model's actual states (needs V3 to be scored); a reversible regime (two
parameters; only if A2 shows abrupt reversible excursions and V2 shows the
statistics can identify them).

Rules for this phase:

- One implementer per candidate, behind an opt-in flag, flag-off runs
  bit-identical. The other agent writes the recovery and identity tests
  from the dossier, without reading the implementation first, and reviews
  the diff.
- One gate evaluation per candidate per panel where it applies, at the
  precision V1 recommends, paired seeds against the incumbent. At most 15
  candidate evaluations in the phase. A variant is a new candidate with a
  new dossier. Every run is logged, used or not.
- Verdict by the rule in the skill (step 7 of the development loop), with
  the margin the owner sets at D1, or the labelled convention if none is
  set. Mechanism verdicts and adoption verdicts are recorded separately.
  Failures are reported with the same prominence.

### Decision point D2

The owner decides any re-freeze. With the replacement drive in hand, the
protocols for the Reddit archive and Wikipedia are written for whichever
model generation stands at that point.

## 5. How this can fail

- **The bet is wrong.** A1 may show the tails are passing after all. Then
  C-T is dropped, the missing departures point to drift (A2) or to nothing
  cheap, and the plan still delivers the instrument study, C-A and C-P.
- **The gate cannot adjudicate.** V1 may show that no difference of the
  size at stake is resolvable in five splits. Adoption would then rest on
  "not worse" plus the target moments, and whether the endpoint-level
  scores become a registered supplement stops being optional.
- **Shared blind spots.** Both models read the same record. Agreement
  between them is not evidence; the checks that count are executable:
  planted-mechanism recovery, blind re-computation, flag-off identity.
- **Overuse of the development panels.** Every look is a selection step.
  Nothing here confirms anything. The sealed data is the test.

## 6. Decisions for the owner

Decided on 2026-10-12 (§2z-ar): the plan is adopted; the pending work on
`main` is committed and pushed by Claude; the two-agent protocol is
approved; the instrument runs of V1 are approved; Claude may call Codex
directly for small jobs and wakes itself while waiting.

At D1: which candidates; the margin; whether to re-record the reference
gates at 20 reps; whether the endpoint-level scores become a registered
supplement for the new data.
