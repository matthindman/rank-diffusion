# Proposed next phase: from measured movement to the ladder

_2026-10-12. Codex's proposal
(`handoff/D1_debate/RESEARCH_AGENDA_CODEX_2026-10-12.md`), integrated into
the program's documents by Claude (Opus 5.5) at the owner's request, with
Claude's amendments marked as such._

**Status: ADOPTED by the owner on 2026-10-13** ("adopt and implement the
movement-to-structure plan"; MODEL_STATUS §2z-bp). It is the working agenda
and replaces Phase 2 ("candidates, one at a time") of
`WORKPLAN_CLAUDE_2026-10-12.md`, whose candidates were withdrawn or narrowed
at decision point D1 (MODEL_STATUS §2z-ba, §2z-be). The text below is as
proposed on 2026-10-12; where it says "proposed" or "if adopted", read it
with this line. Its first deliverable is
`MTS_PROTOCOL_STEPS_1_2_2026-10-13.md` (§2z-bq).
Nothing here changes a frozen stack, the gate, a threshold, a protocol or
the paper's claims. Numbers are quoted from MODEL_STATUS with their section.

## The question

Can movement and turnover that are measured without using the ladder
explain the ladder's shape, its stability, and who moves through it?

This is the second of the three levels of claim agreed in the debate on
candidate 2 (§2z-be): conditional dynamics, where the model's binding claim
sits now; distributional explanation; social mechanism. Reaching one level
does not establish the next.

## The relation the phase is built on

Take the lasting log level of every endpoint in a stationary cross-section.
Number positions from the top as a share u of the population. Write S(u)
for the ladder's local slope (log level per unit of log rank), a(u) for
the weekly variance of lasting movement of whoever is at u, G(u) for the
downward drift summed over everyone above u, measured against a declared
centre, and Φ(u) for the net current that entry and exit create through the
positions above u. Then

    S(u) = u · a(u) / ( 2 · [ G(u) + Φ(u) ] ).

If everyone above the bottom drifts down at one rate g and there is no
flow, this is S = a / (2g), the form checked in §2z-bf and §2z-bh. Codex's
derivation, with assumptions and reading-status tags on every citation, is
`handoff/D1_debate/variance_structure/THEORY.md`.

Three properties decide how it can be used.

1. **It is a balance that every stationary cross-section satisfies.** It
   holds under reversion to personal homes, under drift that depends only
   on position, and under replacement from below. Finding that it closes
   tests the measurements and stationarity. It does not choose a mechanism.
   (Claude's amendment; Codex's note makes the same point through the
   mimicking results it cites.)
2. **The ladder cannot set the clock.** Multiplying a and G by the same
   number leaves the ladder unchanged. A constant read off the ladder, as g
   was in §2z-bf and §2z-bh, is not a measured force.
3. **Mechanisms differ in who carries G.** Drift common to everyone at a
   position; drift particular to a name (movement back toward its own
   earlier level); or flow. That split, and whether the pieces predict
   structure they were not fitted to, is where the evidence is.

## What is already in hand

| finding | record |
|---|---|
| a/(2S) is close to constant across rank octaves on four development panels (two platforms; the comments panels overlap) | §2z-bf |
| The correspondence appears only when variance is measured with noise handled; the one-week variance and the model's fitted lasting variance both fail | §2z-bh |
| Recomputed independently by Codex with matched start weeks and a same-season contrast: slope error 6–12% over seven fixed octaves, 8–17% over all but the top bin | §2z-bj |
| The constant is taken from the ladder; nothing independent measures it | §2z-bh, §2z-bj |
| A half-window holdout failed its prespecified condition. Scored alike, carrying the ladder forward is the better predictor in seven of eight scorings, by 10–42%, and worse in one by 7%; the recorded "three times" was a scoring error | §2z-bh as corrected by §2z-bk |
| Year over year the lasting level moves like a random walk where the fitted model reverts | §2z-aw, §2z-bd |
| The fitted reversion rates are not recovered on simulated data | §2z-bc |
| An independent simulator with planted mechanisms exists | §2z-bc |

## The six steps

Codex's sequence, kept in its order. Each step names what Claude would add.

**1. Account for the balance with separately measured parts.** Variance,
signed drift, the mapping from permanent-rank cohorts to current positions,
and entry and exit, on one population and one time scale, with the centre,
the population boundary and the information behind each cohort declared.
Output: at each rank, how much of u·a/(2S) is covered by measured drift and
flow, with uncertainty, across calendar windows and nested boundaries.

Claude's amendments:
- Measure drift with three windows: choose the cohort on the first, take
  the starting level on the second, measure the change to the third, with a
  gap after the first long enough for passing swings to die out. A level
  with no restoring drift then shows no expected change whatever the
  selection. This keeps the program's rule against conditioning on current
  rank and still measures drift by position.
- Membership uses the first window only. Endpoints that fall below the
  cutoff are followed in the buffer; those that leave the record are
  counted as flow and not dropped.
- Split the drift on one footing into the three carriers above. This
  merges the restoring-response comparison of §2z-be into the balance.
- Separate the common level from the normalized shape from the start:
  the ladder-drift statistic used in §2z-ay responds to common growth with
  no change of shape (Codex's review R1, §2z-bi).
- Boundaries at K/2, K and 2K inside the 4K buffer.
- Added after Codex's first proof result (§2z-bm, reviewed in §2z-bn): the
  estimands are finite-horizon and matched. Mean change, mean squared
  change and crossings in each direction are measured at the same stated
  horizons for the same cohort, fixed on an earlier window. The
  instantaneous variance of the formula above cannot be recovered from
  weekly observations in general, and it is not needed: the ladder depends
  on the ratio of movement to restoring drift on a common clock, and at a
  finite horizon the exact form of the balance is that as many endpoints
  cross a level downward as upward. On observed positions the one-week
  balance has to close in a stationary cross-section, so it is run first as
  a check of the instrument.

**2. Validate the diagnostic on planted rivals.** Processes that look alike
in variance and ladder and differ in cause, at the real panel lengths and
with the real noise, seasonality, selection and missingness. Report false
identification as well as recovery. Predictions, precision targets and
stopping rules written first; one code path for real and planted panels.

Claude's amendment: the rival set includes the frozen model itself, fixed
identities with no dependence on position, replacement-stabilised growth
with no dependence on state, drift that depends only on position, and a
level with no stabiliser at all. Codex's simulator from V2 is the natural
place to build them, and Codex stays blind to the empirical step-1 numbers
until its recovery map is committed, as in V2. One more case after the
proof result: a slow and a fast component of equal stationary variance, on
which the measurement must give the same answer whichever is called the
signal.

**3. Predict structure that was not used for calibration.** Freeze the
measured inputs, then predict an unused window's local slopes and
normalized shape, with no future value supplied as an anchor and the same
weights, horizons, universe and information for every comparator.

Claude's amendments:
- Carrying the ladder forward is a strong comparator on these panels over
  two years (§2z-bh, §2z-bk), so a later-window test has little power to
  separate models. The informative contrasts are between panels and
  between nested universes (the constant is about twice as large on
  Facebook as on Reddit, §2z-bh), and responses to change (step 5).
- The strongest form of this test is on sealed data: freeze the procedure,
  measure movement there, and predict that panel's ladder with no constant
  taken from it. That design has to exist before a sealed folder is opened,
  and nothing is opened without the owner's word.

**4. Build the smallest named-endpoint process the measurements support.**
Measured diffusion, durable endpoint characteristics and measured turnover;
further forces only where a discriminating measurement requires them. A
fixed personal home is a hypothesis to be supported, not a default. Judged
on collective outcomes and on named outcomes together, with complexity
counted in full.

Claude's amendments: any candidate still has to pass Gate v1 against the
frozen stacks, inside the 15 candidate evaluations of the adopted plan,
none of which is spent. And one parsimony target is worth stating early:
whether variance by size has a low-dimensional form, because the ladder's
curvature would then follow from a few numbers.

**5. Test adjustment and change.** Whether measured changes in volatility,
drift or turnover predict changes in concentration and their timing;
whether the process reaches similar structure from different starting
distributions with no target in its parameters; and the adjustment time,
derived from the process and not taken as 1/g.

Claude's note, a sketch and not a measurement: in the simplest one-constant
case (a level reflected at the bottom, drift g, variance a) the whole
ladder relaxes on a time of 2a/g², which is 4S/g: about twice 1/g for
slopes near one half, and longer than any panel here. The shape inside one
octave of rank adjusts in roughly 0.2 to 0.7 times S/g, which for slopes
near one half and the constants of §2z-bh is eight months to two years on
Reddit and four months to a year on Facebook. If that carries over, local
slopes are testable inside the long panels and the level of the whole
ladder is not. Step 5 would establish the real times.

**6. Identify a social mechanism, then confirm transport.** What produces
the measured movement: supply of items, engagement per item, audience
persistence, competition, platform exposure. Supply against engagement
first; the count columns exist on Facebook and Reddit and have not been
examined (`PROGRAM_STATE.md`, "Not yet examined"). Reserved data tests
transport only after the model and its discriminating predictions are
fixed. Named as the route to the third level; not scheduled.

## Mathematical work

Codex prepared three proof targets and a referee prompt
(`handoff/D1_debate/proof_prompts/`): what the finite-lag variance
instrument identifies; stability of a typed population with replacement;
what weekly crossings say about relative diffusion. None is certified
open. The first has been attempted by Codex (`PROOF_1_RESULT.md`, §2z-bm):
the instantaneous variance by current position is not identified from
weekly data in general, and is identified under stated restrictions or
with much shorter lags. Claude's review found no gap and draws the
consequence used in step 1 (`PROOF_1_REVIEW_CLAUDE.md`, §2z-bn). The other
two have not been run.
One elementary bound in the theory note already constrains the design: the
relative error of a predicted slope is at most the sum of the relative
errors of variance and cumulative drift, inflated as the drift error
approaches one. Drift has to be measured as well as variance is.

## Who does what

Under `coordination/PROTOCOL.md`: Claude writes the measurement protocol
for steps 1 and 2 (estimands, windows, predictions, stopping rules) and the
statistics; Codex builds the planted rivals and the recovery map, and
reviews the protocol; each design is committed before its run. The proof
targets run only if the owner asks.

## What stays in force

The frozen stacks as the conditional benchmark. Gate v1 as the adoption
criterion. The sealed-data rule. Every parameter from a declared moment or
an independent instrument. Permanent-rank estimation. The three levels of
claim, with the paper's language at the first until a second-level result
exists.

## First deliverable and stopping point

One protocol for steps 1 and 2, reviewed before anything runs. Its first
decision is whether step 3 is identified with the development data. If it
is not, the result is a stated requirement for further observations or a
narrower claim, and not another flexible model fitted to the same ladder.

## Where Claude and Codex differ

On substance, nowhere that changes the sequence. Codex's reviews corrected
several of Claude's statements on the way (§2z-bi, §2z-bj, §2z-bk). Claude's additions
are those marked above: the balance is an identity and not a test; the
three-window drift design; the weakness of a later-window test against
persistence and the sealed-data form of step 3; the two time scales; the
standing gate budget; and, after the first proof result, matched
finite-horizon estimands in place of an instantaneous variance, which
Codex has not yet seen. Agreement between the two models is not evidence
for any of it.

## For the owner

1. Whether this becomes the working agenda for the next phase.
   _Decided 2026-10-13: adopted (§2z-bp)._
2. Whether the proof targets are run, and by whom. _Open._
