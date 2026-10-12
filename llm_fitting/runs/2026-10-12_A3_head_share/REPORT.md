# A3 — results

_Written 2026-10-12 by Claude (Opus 5.5). Design and predictions:
`DESIGN.md` (committed at 3db30e6 before the first cells; addendum at
7c53c4b before the far-tail and bounded-shock cells)._

Status: EXPLORATORY. Interventions in each panel's fitted structure stack
on DEVELOPMENT data, 20 seeds, in sample. No refit, no gate evaluation,
nothing adopted. Every number is in `report_tables.txt`
(`python llm_fitting/phase1_a3.py report`). An arm that removes the excess
shows which part of the simulation carries it. It does not show that the
part is wrong in the data.

## Result

Simulated top-1 share within the recorded top 2,000, as a multiple of the
data's. Arms differ from the base in one thing; the same seeds are used in
every arm.

| arm | FB Era A | Instagram rescue | comments | long submissions |
|---|---|---|---|---|
| data, top-1 share | 0.0170 | 0.0352 | 0.1057 | 0.0724 |
| data, first week alone | 0.0245 | 0.0302 | 0.1361 | 0.0383 |
| base | 2.78 | 3.66 | 1.38 | 0.75 |
| Gaussian passing shocks | 1.60 | 2.03 | 1.12 | 0.68 |
| passing component off | 1.42 | 1.91 | 1.18 | 0.69 |
| amplitude off | 1.93 | 2.54 | 1.26 | 0.66 |
| homes frozen | 3.04 | 3.20 | 1.35 | 0.63 |
| homes seeded from the time-median ladder | 2.54 | 3.47 | 0.87 | 1.08 |
| homes seeded from endpoint means | 2.36 | 4.18 | 1.00 | 0.97 |
| endpoint means and Gaussian shocks | 1.51 | 2.78 | 1.23 | 0.88 |
| passing shocks bounded at 12 / 8 / 6 SDs | 2.26 / 1.96 / 1.82 | 2.94 / 2.35 / 2.08 | 1.36 / 1.35 / 1.35 | 0.69 / 0.68 / 0.67 |

Seed SD of the base arm, in the same units: 0.61, 0.89, 0.49, 0.16, so the
standard error of a 20-seed mean is 0.14, 0.20, 0.11 and 0.04. Paired
differences are tighter; the ones that matter are given below.

Share of the summed weekly top-1 share that comes from the 5% of weeks
where it is highest:

| | FB | Instagram | comments | long submissions |
|---|---|---|---|---|
| data | 0.084 | 0.086 | 0.076 | 0.093 |
| base | 0.408 | 0.347 | 0.107 | 0.191 |
| Gaussian shocks | 0.130 | 0.116 | 0.086 | 0.104 |
| passing component off | 0.077 | 0.106 | 0.074 | 0.089 |
| bounded at 8 SDs | 0.224 | 0.222 | 0.089 | 0.113 |

Far tail of one-week changes, data / simulation (median over seeds):

| | FB | Instagram | comments | long submissions |
|---|---|---|---|---|
| share beyond 6 robust SDs, middle group | 3.0e-3 / 1.2e-4 | 1.3e-3 / 2.4e-4 | 1.2e-3 / 8.2e-5 | 3.2e-3 / 2.3e-4 |
| 99.99th percentile of rises, robust SDs, middle group | 13.2 / 5.4 | 9.1 / 5.9 | 11.4 / 5.2 | 27.5 / 6.2 |
| largest rise in the top 2% of K, robust SDs | 6.6 / 5.5 | 5.8 / 7.0 | 12.0 / 7.5 | 19.2 / 7.4 |
| largest rise in the top 2% of K, log units | 1.88 / 2.10 | 3.12 / 4.17 | 2.46 / 2.06 | 2.27 / 1.56 |
| largest rise anywhere, log units: data; simulation's largest over 20 seeds | 9.5; 35.7 | 9.7; 60.6 | 9.8; 15.4 | 14.5; 22.7 |

## What it shows

1. **On the two Reddit panels the head-share error is the seeding, sign
   change included.** The simulator sets homes to the first week's sorted
   observed values. That week's top share is 29% above the window's mean on
   comments and 47% below it on long submissions. Seeding from the sorted
   endpoint means instead moves the simulated share from 1.38 to 1.00 of
   the data on comments (paired difference 3.5 standard errors) and from
   0.75 to 0.97 on long submissions (5 standard errors). The time-median
   ladder does the same within noise (0.87, 1.08).
2. **On Facebook and Instagram it is mostly not the seeding.** Endpoint
   means remove 24% of the FB excess (2.0 standard errors) and the
   time-median ladder 14%; on Instagram the time-median ladder removes 7%.
   The endpoint-mean arm goes the wrong way on Instagram, where a mean over
   observed weeks is biased upward by the censoring that panel is built
   around; that arm is not valid there.
3. **There the excess is carried by the passing shocks, through rare spike
   weeks.** Passing component off removes 76% (FB) and 66% (Instagram);
   Gaussian shocks 66% and 61%, the FB figure reproducing §2z-w exactly.
   In the simulation 41% and 35% of the summed top-1 share comes from 5%
   of weeks, against 8–9% in the data, and the figure falls to the data's
   with the passing component off. With those weeks taken out the FB base
   arm is still 1.70 times the data and the Instagram one 2.5 times, so a
   steadier part of the excess remains that this task does not attribute.
4. **Bounding the standardized shock would fix the simulation's spikes and
   contradict the data.** A bound at 8 innovation SDs gives 70% (FB) and
   80% (Instagram) of what Gaussian shocks give. But the data's own
   changes are far heavier in the far tail than the simulation's, in every
   rank group on every panel: 5 to 25 times as many changes beyond 6
   robust SDs in the middle group, and the same holds among the top 2% of
   endpoints on FB, comments and submissions.
5. **What differs is the raw size of the largest jumps, not their size in
   an endpoint's own units.** Among the top 2% the largest one-week rise
   in log units is about the same in data and simulation. Across the whole
   list the data's largest rise is 9.5 to 14.5 log units; the simulation's
   reaches 36 on FB and 61 on Instagram in its worst seed. The simulation
   multiplies three heavy things (the band's volatility, the endpoint's
   amplitude and a power-law draw), and now and then an endpoint from the
   middle or bottom of the list lands far above the top for a week. In the
   data the extreme standardized moves belong to endpoints whose raw moves
   stay bounded.
6. **Frozen homes do not lower the head share on FB** (3.04 against 2.78,
   inside the noise). Amplitude off removes 48% there; halving the spread
   removed 47% in §2z-w.

## Predictions, scored

1. *Base: 2.3–3.2 times the data on FB, above on Instagram and comments,
   below on long submissions.* HELD (2.78, 3.66, 1.38, 0.75).
2. *FB, Gaussian shocks remove more than half.* HELD (66%).
3. *Endpoint-mean seeding moves all four toward the data, by at least 20%
   of the excess on FB, Instagram and comments, and up on submissions.*
   HELD on FB (24%), comments (100%) and submissions; FAILED on Instagram
   (−19%).
4. *The time-median ladder moves FB most, as a share of the excess.*
   FAILED: 14% on FB against 134% and 130% on the Reddit panels.
5. *Frozen homes lower the share on every panel.* FAILED on FB.
6. *Amplitude off removes at least 40% of the FB excess.* HELD (48%).
7. *No single arm brings all four within 25%; endpoint means with Gaussian
   shocks bring FB within 50%.* First part HELD; second FAILED by a hair
   (1.51).
8. *Among large endpoints the data's largest standardized rise is smaller
   than the simulation's.* FAILED on FB and comments in both groups and on
   Instagram in the head group; the data's is larger.
9. *In the shell the data has at least as many changes beyond 6 robust
   SDs.* HELD (16 to 39 times as many).
10. *FB: a bound at 8 gives at least half of the Gaussian arm's reduction,
    a bound at 6 three quarters.* HELD (70%, 81%).
11. *FB: the top-5%-weeks share falls below 0.25 at a bound of 8.* HELD
    (0.224).
12. *Long submissions: a bound at 12 moves the share by less than its seed
    SD.* HELD.

Seven held in full, two in part, three failed. Prediction 8 was the stated
kill condition for reading the spikes as a far-tail problem of the
standardized shock, and it failed: see finding 5 for what replaces that
reading.

## Consequences for the plan

- **A candidate with no parameter: seed the unconditional simulation from
  the ladder of lasting levels.** It repairs the head share on both Reddit
  panels and removes the one sign change in open residual 2. It changes a
  default of the structure simulation and every structure card, so it is
  the owner's decision and needs its own card comparison on all five
  panels (addendum 2 below). *Corrected the same day:* confined to the
  structure simulation it does not touch the gate; made inside the fit it
  would, because the gate's forecasts start from the filtered state but its
  calibration step on the training window runs the unconditional
  simulation from the same seeded ladder. A censored
  panel needs a different estimate of the lasting ladder than a mean over
  observed weeks.
- **The far tail of the passing shock is the FB and Instagram lever, and
  the right restriction is on raw size, not on standardized size.** The
  plan's candidate "passing shocks bounded at the largest standardized move
  in the data" would not bind: the data's largest standardized moves are
  larger than the simulation's. What the data constrains is how large a
  jump can be in log units given where an endpoint sits. Testing that needs
  an opt-in change inside the simulator, which is Phase 2 work.
- **The amplitude lead from §2z-as and §2z-au gains a mechanism.** The
  spike weeks need a high-amplitude endpoint and a far-tail draw together.
  Whether the amplitude spread is overstated is V2's question.

## Addendum 2: the structure card under each seeding

Run after the report above was committed; design and predictions 13–16 in
`DESIGN.md` (b13bf01). Card rows passed, macro index in tolerance units
(lower is better), and the simulated top-1 share over the data's; 20 seeds,
bracket from resampling the seeds jointly with the base arm.

| panel | first week (as fitted) | time-median ladder | endpoint means |
|---|---|---|---|
| FB Era A: card; index; index minus base [5%, 95%]; top-1 over data | 15/15; 1.09; 2.78 | 15/15; 0.86; −0.231 [−0.312, −0.161]; 2.54 | 15/15; 0.77; −0.317 [−0.408, −0.235]; 2.36 |
| comments | 12/15; 0.86; 1.38 | 12/15; 0.65; −0.210 [−0.295, −0.125]; 0.87 | 12/15; 0.59; −0.265 [−0.320, −0.164]; 1.00 |
| long submissions | 13/15; 0.55; 0.75 | 13/15; 0.45; −0.105 [−0.141, −0.067]; 1.08 | 12/15; 0.49; −0.061 [−0.088, −0.017]; 0.97 |
| short submissions | 14/15; 0.33; 1.07 | 14/15; 0.29; −0.040 [−0.064, +0.009]; 1.00 | 14/15; 0.39; +0.059 [−0.008, +0.127]; 1.17 |
| Instagram rescue | 8/15; 2.12; 3.66 | 8/15; 2.11; −0.013 [−0.096, +0.057]; 3.47 | 9/15; 2.25; +0.126 [+0.053, +0.197]; 4.18 |

Ladder drift barely moves under either seeding (FB 0.115 → 0.126 and 0.125
against the data's 0.267; long submissions 0.136 → 0.157 and 0.144 against
0.501): it is a property of the dynamics, not of the start.

Predictions: (13) the card changes by at most one row on the four
uncensored panels under endpoint means: HELD (one row lost on long
submissions). (14) The index falls on comments and long submissions, with
a band excluding zero on at least one, and does not rise by more than 0.10
on FB or short submissions: HELD. (15) On Instagram endpoint means are
worse than the base and the time-median ladder is within 0.10 of it: HELD.
(16) Ladder drift nearer the data's on at least three of the four
uncensored panels: FAILED (two of four, and by almost nothing).

What it adds to the consequences above: both seedings improve the index
with a band excluding zero on FB, comments and long submissions. They
differ elsewhere. The time-median ladder does no harm on any of the five
panels. Endpoint means are better on FB and comments, worse on long
submissions, and worse than the base on the 30-week panel (inside its band)
and on the censored one (outside it). The argument from first principles
favoured endpoint means; the table favours the time-median ladder as the
one to propose, with the caution that choosing between two variants on
in-sample cards is itself a selection.

## Not shown, not run

Where the steadier part of the FB and Instagram excess comes from. A
bounded arm in raw units (needs model code). Bounded-t and seeding arms in
combination. The effect of the non-seeding arms on the structure cards. Why the comments
arms with Gaussian shocks are so noisy (seed SD up to half the data's
share: one endpoint holds the top there, with a gap of 1.24 log units to
the second).

One correction made while tabulating: the bounded arms share their random
stream with the base arm of the first cells, not with the base arm inside
the addendum cells (which tracks every slot and so draws a different
tracking sample). The table pairs them correctly; the first printout did
not.

## Files and commands

`llm_fitting/phase1_a3.py`; `llm_fitting/residual_anatomy.py` (`far_tail`);
this folder: `DESIGN.md`, `cells/`, `logs/`, `report_tables.txt`.

```
python llm_fitting/phase1_a3.py run <panel>      # facebook_a, instagram, reddit_comments, reddit_submissions_long
python llm_fitting/phase1_a3.py run_b <panel>
python llm_fitting/phase1_a3.py report
```
