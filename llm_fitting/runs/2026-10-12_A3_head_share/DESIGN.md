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

## Addendum, 2026-10-12, after the first cells and before the cells below

Seen in the first cells (three panels finished, comments running): in the
simulation the 5% of weeks with the highest top-1 share carry 41% of the
summed share on FB, 35% on Instagram and 19% on long submissions; in the
data 8–9% on all three. With the passing component off the simulation's
figure is at the data's. The simulated top is heavy because of rare spike
weeks. Also seen, while checking the bounded draw below: two million
unit-variance draws from the fitted t (4.3 degrees of freedom) have a
largest value of 128 standard deviations.

That reconciles two results that looked opposed: the data's own changes
are fatter-tailed than the simulation's out to the 99th percentile
(§2z-x, §2z-y, §2z-au), and Gaussian shocks remove two thirds of the FB
head excess (§2z-w, reproduced here). A top-1 share is a maximum over
about a million endpoint-weeks, so it is set by draws far beyond the 99th
percentile, where a power-law tail and the data may differ in the other
direction. That is a conjecture until the far tail is measured.

Added, as `run_b`, on the same four panels, 20 seeds:

- `far_tail` (in `residual_anatomy.py`, with a known-answer test): the
  99.9th and 99.99th percentile and the largest of the one-week rises, in
  units of the endpoint's own robust scale and in log units, and the share
  of changes beyond 6 and 10 robust SDs, by rank group and for the top 2%
  of K; data and simulation through one function.
- Passing shocks bounded at 12, 8 and 6 innovation SDs and rescaled to
  unit variance, from the same random stream as the base arm. The three
  bounds were fixed here before any run. They are a grid for a diagnostic,
  not candidates: a bound taken into the model would have to come from a
  declared moment of the far tail.

Predictions:

8. In the head group and in the top 2%, the data's largest standardized
   one-week rise is smaller than the simulation's median largest rise on
   FB, Instagram and comments.
9. In the shell group the share of changes beyond 6 robust SDs is at
   least as large in the data as in the simulation on the three long
   panels (the data's tails are fatter where endpoints are small).
10. On FB a bound at 8 gives at least half of the Gaussian arm's reduction
    in the top-1 share, and a bound at 6 at least three quarters.
11. On FB the top-5%-weeks share falls below 0.25 at a bound of 8.
12. A bound at 12 changes the top-1 share by less than its seed SD on long
    submissions.

If 8 fails (the data's largest rises among large endpoints are as extreme
as the simulation's), the spike weeks are not a far-tail problem, and the
July reading (who sits near the top, and with what amplitude) is what is
left.

## Addendum 2, 2026-10-12, after §2z-av and before the cells below

§2z-av found that seeding the homes from sorted endpoint means repairs the
top-1 share on the two Reddit panels, and said the change "needs its own
card comparison on all five panels" before it can be put to the owner. This
is that comparison, run as an intervention like the rest of A3 (the seed
ladder is replaced in the fitted parameters; no model code): `run_c`, 20
seeds, three arms (first week as fitted; time-median ladder; endpoint
means), on FB Era A, comments, long submissions, short submissions and the
Instagram rescue, with the card, the macro index and the ladder statistics
computed by the ladder run's own functions.

Predictions:

13. Under endpoint-mean seeding the card changes by at most one row on each
    of the four uncensored panels.
14. The macro index falls on comments and on long submissions, with a
    seed band excluding zero on at least one of them, and does not rise by
    more than 0.10 on FB or on short submissions.
15. On Instagram the endpoint-mean arm is worse than the base on the macro
    index (the censoring bias seen in §2z-av) and the time-median arm is
    within 0.10 of the base.
16. Ladder drift under endpoint-mean seeding is nearer the data's than the
    base arm's on at least three of the four uncensored panels.
