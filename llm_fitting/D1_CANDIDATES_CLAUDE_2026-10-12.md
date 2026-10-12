# Decision point D1 — what Phase 1 found and what I propose to test

_Claude (Opus 5.5), 2026-10-12. Status: DRAFT FOR THE OWNER'S DECISION.
Nothing here is adopted. Codex has not yet reviewed it (queue row R1). Every
number is quoted from the MODEL_STATUS section named beside it._

## Status after 2026-10-12, afternoon

- **Candidate 1 is approved by the owner and implemented, opt-in** (§2z-ba,
  §2z-bb). Re-freezing the structure stacks to use it is still open.
- **Candidate 2 is not approved.** The owner questioned it, I withdrew it
  in the form written here, and it is under debate with Codex
  (`handoff/D1_debate/`).
- **Codex's V2 has landed** (§2z-bc) and changes three readings below
  (§2z-bd): the "not one amplitude" result is not established (the
  statistic detects the annual cycle); planted cycles lower the fitted
  reversion rate, so the cycle is not why the model reverts; and the
  amplitude spread is recovered without bias at its frozen value. The
  evidence against reversion to a home is strengthened. The text below is
  left as written on the morning of 2026-10-12.

## In one paragraph

Phase 1 was meant to choose among three candidate changes. None of the
three survives as written. The measurements point somewhere else, and more
sharply: the model pulls every endpoint back to the place it started, and
the data does not do that. Year over year an endpoint's level moves like a
random walk, established endpoints relocate to the boundary two and a half
to four times as often as simulated, and the reversion the model fits at
the scale of half a year is, on the two long Reddit panels, an annual cycle
the model does not have. The same home also holds the rank-size ladder in
place, which the data does need, so the reversion cannot simply be switched
off. The proposal is one change that is ready to evaluate (how the
simulation is seeded), one that needs a short design step before any code
(a pull on the ladder in place of a pull on the endpoint), and two that
should wait.

## What Phase 1 measured

| task | question | answer | record |
|---|---|---|---|
| A5 | Is the half-applied amplitude why the amplitude step hurts? | Partly: applying it fully repairs 49–94% of the variance loss and raises the FB top-1 share from 2.35 to 3.54 times the data's | §2z-as |
| A4 | Do the band profiles reproduce, and are they simple curves? | Volatility profiles reproduce and are not lines in rank. The reversion-rate profile does not reproduce and sits on its slowest grid value in 31–52% of bands | §2z-at |
| A1, A2 | Where are the tails, and what are the missing departures? | The missing departures are repeat crossings by endpoints already at the boundary, not sudden lasting exits, which the simulation over-produces | §2z-au |
| A3 | Why is the simulated top too heavy? | On the two Reddit panels it is the first-week seeding, sign change included. On FB and Instagram it is rare spike weeks from the passing shocks | §2z-av |
| A7 | Drift or steps? Does the level revert? Is amplitude one number? | The level does not revert year over year; the Reddit panels have an annual cycle; volatility changes over time and long-run and short-run volatility correlate about 0.85, not 1 | §2z-aw |
| A8 | What does the model do without reversion? | Direction matches the data and departures get about halfway there, and the ladder stops holding | §2z-ay |
| A6 | Per-endpoint export for endpoint-level scores | On `main`, off by default, gate numbers unchanged | commit db7693e |
| V1 (Codex) | How noisy is the gate? | Two 20-simulation evaluations of one stack differ by up to 0.018–0.032; the 3-simulation reference values are biased upward by 0.008–0.022 | §2z-ax |

Predictions were written before each run. Of 60 across my tasks (5, 6, 10,
16, 16 and 7, counted from the record entries), 33 held in full, 15 in part
and 12 failed; Codex's 16 all held. The failures did more
work than the passes: one exposed the annual cycle, one showed that a cap
on standardized shocks would not bind, and one showed that the FB head
depends on the home.

Still to come from Codex: V2 (whether these statistics can tell planted
mechanisms apart, and whether the estimator recovers its own parameters)
and V3 (endpoint-level scores). No mechanism claim below is final before
V2.

## What happened to the plan's three candidates

- **One amplitude for the whole model (C-A): withdrawn.** Long-run and
  short-run volatility are not one amplitude in the data (correlation
  0.83–0.85 on four panels against 1.00 simulated, §2z-aw), an endpoint's
  volatility is not fixed over time, and forcing one amplitude inflates the
  FB head (§2z-as).
- **Fat-tailed lasting shocks with bounded passing shocks (C-T):
  withdrawn.** The data has fewer sudden lasting exits than the simulation
  (§2z-au). A bound on the standardized shock would not bind: the data has
  5 to 25 times as many changes beyond 6 robust SDs as the simulation
  (§2z-av).
- **Profiles compressed to short curves (C-P): narrowed.** The volatility
  profiles carry reproducible structure a curve misses. Only the
  reversion-rate profile is unidentified (§2z-at), and that is taken up in
  candidate 2 below.

## Proposed candidates

### 1. Seed the simulation from a time-averaged ladder — ready to evaluate

**Residual.** The simulated top-1 share is 1.38 times the data's on
comments and 0.75 on long submissions (§2z-av); the registered
confirmation's head-law trigger fired on the comments extension (§2z-q).

**Mechanism.** The unconditional simulation sets each home to the sorted
observed values of the first week. Observed values contain that week's
passing swing and noise, and the simulation adds both again when it
observes, so the top of the ladder is widened twice; and one week can be
unrepresentative (its top share is 29% above the window mean on comments
and 47% below on long submissions). A ladder averaged over the window
removes the second problem; a ladder of sorted endpoint means removes
both, where endpoints are observed often enough for a mean to be fair.

**Identifying moment.** None needed: no parameter. On a synthetic panel
with known homes the first-week ladder puts the top value 0.34 log units
above the true top home and the endpoint-mean ladder 0.02 above.

**Evidence.** Endpoint-mean seeding gives 1.00 (comments) and 0.97 (long
submissions) of the data's top-1 share, paired differences of 3.5 and 5
standard errors; 24% of the FB excess; not valid on Instagram, where means
over observed weeks are biased by censoring (§2z-av).

**Card comparison on five panels** (§2z-az; card rows passed, macro index
where lower is better, change against the present seeding with a seed
bracket, simulated top-1 share over the data's):

| panel | first week (as fitted) | time-median ladder | endpoint means |
|---|---|---|---|
| FB Era A: card; index; index minus base [5%, 95%]; top-1 over data | 15/15; 1.09; 2.78 | 15/15; 0.86; −0.231 [−0.312, −0.161]; 2.54 | 15/15; 0.77; −0.317 [−0.408, −0.235]; 2.36 |
| comments | 12/15; 0.86; 1.38 | 12/15; 0.65; −0.210 [−0.295, −0.125]; 0.87 | 12/15; 0.59; −0.265 [−0.320, −0.164]; 1.00 |
| long submissions | 13/15; 0.55; 0.75 | 13/15; 0.45; −0.105 [−0.141, −0.067]; 1.08 | 12/15; 0.49; −0.061 [−0.088, −0.017]; 0.97 |
| short submissions | 14/15; 0.33; 1.07 | 14/15; 0.29; −0.040 [−0.064, +0.009]; 1.00 | 14/15; 0.39; +0.059 [−0.008, +0.127]; 1.17 |
| Instagram rescue | 8/15; 2.12; 3.66 | 8/15; 2.11; −0.013 [−0.096, +0.057]; 3.47 | 9/15; 2.25; +0.126 [+0.053, +0.197]; 4.18 |

Two parameter-free variants were tried. Both improve FB, comments and long
submissions with brackets excluding zero. The time-median ladder (at each
rank, the median over weeks of the value observed there) harms no panel.
Endpoint means are better on FB and comments and worse on the 30-week and
the censored panel. **I propose the time-median ladder**, and flag that
picking between two variants on in-sample cards is a selection.

**Scope and cost.** Proposed for the structure simulation only, as an
option of that simulation. One caution, found while checking this
paragraph: the gate is not untouched by construction. Its forecasts start
from the filtered state, but its calibration step on the training window
runs the unconditional simulation, which is seeded from the same
first-week ladder. If the seeding were changed inside the fit, the gate's
calibration, and so its score, could move, and that would cost five gate
evaluations. Confined to the structure simulation it costs none. Either
way it changes a default of the structure simulation and every structure
card: your decision, as a re-freeze of the structure stacks.

### 2. The lasting layer: keep the pull on the ladder, drop the pull on the endpoint — design step first

**Residuals.** Departures of established endpoints under-produced 2.3 to
2.74 times (§2z-ac, §2z-ae); relocation to the boundary 2.5 to 3.8 times
(§2z-aw); variance of 78-week changes 10–21% short (§2z-au); year-over-year
slopes of −0.01 to −0.23 where the data has +0.28 to +0.31 and +0.01 to
+0.07 (§2z-aw); a simulated ladder that drifts 0.27 to 0.67 as much as the
data's (§2z-ay).

**What the measurements say.** The home does two jobs. It pulls each
endpoint back to where it started, which the data contradicts. It holds
the ladder, which the data needs: with reversion switched off the ladder
drifts 1.4 to 3.4 times as much as the data's and the FB top-1 share runs
to 15 times the data's (§2z-ay). The data sits between the two.

**Proposed form.** Replace the pull toward an endpoint's own starting level
by a pull toward the level the ladder assigns to the rank the endpoint
holds now. Endpoints can then change places for good, and the set of levels
is still held to the ladder. One strength for the pull, per panel, in
place of a reversion rate per band.

**Identifying moment.** Ladder drift: how far the rank-size curve moves
between the first and last third of the panel. It is 0.27–0.50 in the data,
below it with the present home and above it with none, so it brackets the
strength.

**Parameters.** About 55 fitted values removed per panel, one added.

**Predictions if it is right.** Year-over-year slopes within 0.10 of a
random walk's; relocated share at least 0.6 of the data's (from 0.26–0.40);
departures of the established cohort at least 0.55 of the data's (from
0.37–0.44); 78-week variance within 10% of the data's after the lasting
volatility is re-estimated. Must not worsen: the FB and Instagram top-1
share; the card by more than one row on any panel; the gate on any panel
beyond the margin. It is not expected to repair the boundary flicker or
the excess of deaths on comments, which did not move when reversion was
switched off.

**Why not straight to code.** Three things are owed first, none of which
needs your approval: the design written out with the estimator for the
lasting volatility under the new pull; V2's result on whether the
estimator and the statistics recognise a level without reversion when one
is planted; and one test for it added to V2's list. **Gate evaluations if
it goes ahead: five** (four primary panels and Instagram).

### 3. The annual cycle — wait

On the three Reddit panels quarterly changes a year apart correlate +0.10
to +0.18 and half a year apart −0.03 to −0.11; the simulation has −0.04 at
a year (§2z-aw). The half-year bulge in the data's variance, which the fit
can read as reversion, is accounted for by the cycle in all nine
panel-by-group cells (§2z-aw addendum). Candidate 2 stops estimating
reversion from that bulge, so the cycle would no longer distort the fit.
Modelling the cycle itself is new structure for one platform and no scored
statistic asks for it yet. Proposal: record it as a known omission, state
it in the paper's limitations, and revisit after candidate 2.

### 4. A limit on the raw size of passing jumps — design step, after candidate 2

On FB and Instagram 35–41% of the simulated top-1 share comes from 5% of
weeks, against 8–9% in the data, and the simulation's worst seeds contain
one-week rises of 36 and 61 log units against 9.5–9.7 in the data (§2z-av).
The data's far tails are heavier than the simulation's in an endpoint's own
units, so the restriction has to be on raw size given where the endpoint
sits, not on standardized size. Two forms are possible (a cap by rank band
taken from the data's largest rises; a ceiling on how far a passing spike
can carry an endpoint above the top of the ladder). Neither is specified
well enough to test. V2's recovery study bears on it too: if the amplitude
spread is overstated, the spikes shrink without any cap.

### 5. Smaller items

- **Deaths on comments.** 3.0% of the simulated established cohort dies
  against 0.7% in the data (§2z-aw). A measurement of the exit-rate
  profile by permanent rank comes before any proposal.
- **Reference gates.** The four recorded reference values were taken at 3
  simulations and are biased upward by 0.008–0.022. Codex's 80-seed values
  are 0.115, 0.143, 0.128 and 0.172 (§2z-ax). Recording those as the
  references is already on your list.

## The comparison margin

You decided on 2026-10-11 to wait for a measurement before setting a
number. The measurement is in (§2z-ax): two evaluations of the same frozen
stack differ, at the 95% level, by up to 0.018 (FB), 0.030 (comments),
0.022 (short submissions) and 0.032 (long submissions) at 20 simulations,
and 0.013–0.025 at 40. The convention used so far, 0.05, is 1.6 to 2.8
times the 20-simulation noise. Noise between a candidate and the incumbent
on shared seeds has not been measured, because no candidate has been run.

My recommendation, for you to accept, change or defer:

- candidate and incumbent evaluated on the same 40 seeds, per-seed outputs
  saved, so the paired noise is measured on the first comparison;
- "not worse" means the candidate's error minus the incumbent's is at most
  0.05 on every panel;
- "better" is claimed only when the paired difference exceeds that panel's
  measured noise; anything inside it is reported as unresolved.

## Gate budget

The plan allows 15 candidate evaluations. Candidate 1 uses none if it is
confined to the structure simulation, five if it is also applied in the
gate's calibration step. Candidate 2 uses five. Candidate 4, if it reaches a test, uses up to five. That
leaves five in hand.

## What I am asking you to decide

1. Whether candidate 1 goes ahead as an opt-in seeding option of the
   structure simulation, with a card comparison at registered precision,
   for you to judge as a re-freeze of the structure stacks; and whether it
   should also be tried in the gate's calibration step, which spends gate
   evaluations.
2. Whether candidate 2 goes to its design step now and to implementation
   once V2 is in and Codex has reviewed the design.
3. The margin: the recommendation above, another rule, or still deferred.
4. Whether the 80-seed values become the recorded reference gates.
5. Whether the wording around b = 1 in the binding claim set should change.
   What was measured is equal spread of the two amplitudes; they are not
   one amplitude. I have changed nothing.
6. Whether candidates 3 and 4 wait as proposed.

## What I will do meanwhile

Write the design for candidate 2; add one planted case to Codex's V2; measure the exit-rate profile behind
the deaths on comments; integrate V2 and V3 as they land. None of this
changes a default, a stack or the gate.

## What could be wrong with this reading

- V2 has not yet shown that the new statistics separate the alternatives
  at these panel sizes. The direction and amplitude statistics passed
  known-answer tests, which is weaker.
- The follow-up tasks (A7, A8) were designed after earlier results were
  seen. Each prediction preceded its own run; the choice of what to
  measure did not.
- Interventions are not refits. Switching reversion off leaves the other
  parameters where a model with reversion put them.
- The cycle's share of the variance assumes a sinusoid of period one year.
  The refit with the cycle filtered out, which would test it directly, has
  not been run.
- Almost everything here is in sample on development panels. Only the
  relocation and departure counts use a held-out window, and Facebook, at
  86 weeks, cannot speak to anything at the scale of a year.
