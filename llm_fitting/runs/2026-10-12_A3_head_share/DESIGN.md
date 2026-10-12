# A3 — what makes the simulated top too heavy

_Written 2026-10-12 by Claude (Opus 5.5) before any cell was run. Work plan
`WORKPLAN_CLAUDE_2026-10-12.md`, Phase 1, task A3._

Status: EXPLORATORY. Interventions in a fitted model on DEVELOPMENT data;
no refit, no gate evaluation, nothing adopted. Runner:
`llm_fitting/phase1_a3.py`.

## What is already on record

The head share was worked on in July, on Facebook, and this task does not
repeat that as if it were new:

- §2z-w, 20 paired seeds, FB: Gaussian passing shocks remove 66% of the
  top-1 excess, halving the amplitude spread 47%, a fourfold head κ 17%
  (inside noise). The two large effects overlap.
- §2z-x, §2z-y: the data's own tails are fatter than the simulation's in
  every rank stratum, so thinner tails are not licensed; among the top 100
  on FB, volatility falls toward rank 1 twice as steeply in the data as in
  the simulation ("quiet giants", directional support); comments shows no
  such difference.
- §2o: homes are seeded from the sorted observed values of the first week;
  on FB that week is the 2020 election week and its top gap is 2.4 times
  the era's norm. Seeding from a time-averaged ladder was recommended and
  never run.
- The top-1 share is over-produced on FB, on the comments extension and on
  the Instagram rescue, and under-produced on long submissions (open
  residual 2).

## What this task adds

1. **The seeding intervention, on four panels.** Two alternatives to the
   first-week ladder, each a statistic of the data with nothing chosen:
   - `seed_ladder`: at each rank, the median over weeks of the value
     observed at that rank. Removes the first week's idiosyncrasy. Keeps
     the noise that every observed ladder contains.
   - `seed_means`: each endpoint's mean over the weeks it is observed,
     sorted. This is the ladder of lasting levels. The reason to expect it
     to matter: the simulator sets homes to the first week's *observed*
     values, which include that week's passing swing and noise, and then
     adds a passing swing and noise on top when it observes. Sorting noisy
     values widens the ladder at the top, so the simulated top is widened
     twice. On a synthetic panel with known homes the first-week ladder
     puts the top value 0.34 above the true top home and the endpoint-mean
     ladder puts it 0.02 above (checked before this design was committed).
2. **The July interventions on the three panels that never had them**, at
   20 seeds: Gaussian passing shocks; amplitude off; passing component
   off; homes frozen.
3. **One combination**: endpoint-mean seeding with Gaussian shocks.

Panels: FB Era A, Instagram rescue, comments, long submissions; the frozen
structure stack of each; full-window universe; seeds 0–19, the same seeds
in every arm, so differences are paired. Statistics, from the sorted
values of the recorded top-M each week, by one function for data and
simulation: top-1, top-10 and top-100 shares; the mean gaps between ranks
1 and 2, 1 and 10, 10 and 100 in log value; the share of the summed weekly
top-1 share that comes from the 5% of weeks where it is highest.

Not run: bounded-t shocks. The Gaussian arm bounds what any truncation of
the t can do.

## Predictions

1. Base arm: the simulated top-1 share is 2.3 to 3.2 times the data's on
   FB, above the data's on Instagram and comments, and below it on long
   submissions.
2. FB, Gaussian shocks: more than half of the excess removed (a
   reproduction of §2z-w).
3. Endpoint-mean seeding moves the top-1 share toward the data on all four
   panels: down on FB, Instagram and comments, by at least 20% of the
   excess on each, and up on long submissions.
4. The time-median ladder moves FB by more than it moves any other panel,
   as a share of the excess (the election week).
5. Frozen homes lower the top-1 share on every panel, and on FB by less
   than Gaussian shocks do.
6. Amplitude off removes at least 40% of the FB excess.
7. No single arm brings all four panels within 25% of the data. With
   endpoint-mean seeding and Gaussian shocks together, FB is within 50%.

## What each outcome licenses

- Seeding moves the head toward the data on every panel: the head-share
  residual is partly an artifact of how the unconditional simulation is
  started, not of the dynamics. The repair has no parameter and would be a
  candidate for D1 (it changes a default of the structure simulation, so
  it is owner-gated and needs its own card comparison).
- Seeding does nothing or goes the wrong way: the residual is in the
  dynamics, and the July reading (who sits at the top, and with what
  amplitude) stands as the lead.
- The sign on long submissions: explained if an arm that lowers the other
  three raises it. If every arm moves it the same way as the others, the
  sign change is not explained by anything here and stays open.

These are attributions inside one fitted model. An arm that removes the
excess shows which component carries it in the simulation; it does not show
that the component is wrong in the data.
