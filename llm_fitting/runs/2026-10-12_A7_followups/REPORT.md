# A7 — results

_Written 2026-10-12 by Claude (Opus 5.5). Design and predictions:
`DESIGN.md` (committed at 38bb5a6 before the first five cells; addendum at
baad58a before the four `a7c` cells)._

Status: EXPLORATORY measurements on DEVELOPMENT data, in sample except the
relocation counts, which use the train-only cohort of A2. No model code, no
gate evaluation, nothing adopted. The task was added after A1 and A2 were
seen, so it is post hoc as a set; within it, every prediction below was
written before its cell ran, except where the text says a value had already
been seen. Numbers: `report_tables.txt` (`python llm_fitting/phase1_anatomy.py
report7`) and the two `posthoc_momentum_*.json` files. Simulation columns
are means over seeds (20; 30 for relocation). V2 has not yet shown which of
these statistics separate the candidate mechanisms.

## c, e — direction: does an endpoint's past movement predict its later movement?

Slope of a later change in an endpoint's block-mean level on an earlier
one, the rank group's mean change removed; middle group; data /
simulation (seed SD). A random walk gives +0.25 for adjacent changes and 0
otherwise; a level that reverts gives negative values.

Whole-year blocks, which average an annual cycle out:

| | adjacent | one block apart |
|---|---|---|
| long submissions | +0.313 / −0.011 (0.026) | +0.007 / −0.206 (0.024) |
| comments extension | +0.279 / −0.118 (0.011) | +0.071 / −0.228 (0.018) |

13-week blocks, by the spacing between the two changes:

| | half a year | three quarters | one year | two years |
|---|---|---|---|---|
| long submissions | −0.033 / −0.093 | +0.026 / −0.059 | +0.119 / −0.046 | +0.077 / −0.029 |
| comments | −0.053 / −0.134 | +0.005 / −0.062 | +0.101 / −0.040 | +0.096 / −0.019 |
| comments extension | −0.110 / −0.145 | +0.001 / −0.069 | +0.182 / −0.045 | +0.146 / −0.020 |
| FB Era A | −0.038 / −0.148 | −0.101 / −0.065 | +0.010 / −0.042 | |

Seed SDs of the 13-week values are 0.005 to 0.035. The design's 26-week
"skip" (two changes one year apart): +0.126 / −0.135 on long submissions,
+0.118 / −0.147 on comments; positive in each of the five pairs of periods
on submissions and both on comments.

## b — relocation, counted by endpoint

The established cohort of A2 (train permanent rank ≤ K/2), main split;
data / simulation [10th, 90th percentile over seeds].

| | comments extension | long submissions |
|---|---|---|
| relocated to the boundary zone (scoring-window permanent rank beyond 0.75 K, still present) | 0.049 / 0.020 [0.010, 0.027] | 0.069 / 0.018 [0.011, 0.028] |
| gone (present under 70% of scoring weeks) | 0.007 / 0.030 | 0.012 / 0.011 |
| cohort-weeks outside the top-K | 0.026 / 0.029 | 0.038 / 0.016 |
| endpoints that exit at least once | 0.119 / 0.094 | 0.131 / 0.067 |
| exits per exiting endpoint | 4.97 / 2.76 | 4.74 / 3.49 |
| shift in log permanent rank: median | +0.089 / −0.001 | +0.134 / +0.035 |
| 75th / 95th percentile | 0.40 / 0.20; 0.97 / 0.60 | 0.47 / 0.23; 1.06 / 0.68 |
| first exit is a slide | 0.22 / 0.07 | 0.29 / 0.17 |
| first exit is an excursion | 0.71 / 0.57 | 0.64 / 0.62 |
| first exit is sudden and lasting | 0.054 / 0.298 | 0.053 / 0.167 |

The within-train split gives the same relocation shares (0.047 / 0.019 and
0.049 / 0.019).

## d, f — is an endpoint's volatility one fixed amplitude?

Middle group, data / simulation (seed SD).

| | FB Era A | long submissions | comments |
|---|---|---|---|
| volatility correlated between interleaved weeks | 0.843 / 0.865 | 0.926 / 0.963 | 0.892 / 0.876 |
| between the two halves of the panel | 0.676 / 0.820 | 0.757 / 0.941 | 0.755 / 0.832 |
| ratio (1 = fixed over time) | 0.802 / 0.948 (0.008) | 0.817 / 0.978 (0.003) | 0.847 / 0.950 (0.004) |
| long-run against short-run amplitude, 13 weeks (1 = one amplitude) | 0.849 / 0.996 (0.006) | 0.846 / 1.000 (0.002) | 0.844 / 0.994 (0.005) |
| the same at 26 weeks | | 0.802 / 1.001 | 0.801 / 0.992 |

On the comments extension the amplitude figure is 0.831 / 0.995 at 13 weeks
and 0.769 / 0.995 at 26. In the test world with two separate amplitudes the
statistic reads 0.03–0.04 above the true value at 13 weeks and about 0.10
above at 26, so the data's true values are if anything lower.

## a — tails on a scale that does not depend on wandering

q99/q90 of |h-week change| over the endpoint's own one-week scale; a
Gaussian process gives 1.57 at every horizon. Middle group, data /
simulation.

| | 1 week | 4 | 13 | longest (26 FB, 52 Reddit) |
|---|---|---|---|---|
| FB Era A | 2.23 / 1.69 | 2.19 / 1.66 | 2.09 / 1.64 | 2.09 / 1.64 |
| long submissions | 2.12 / 1.73 | 2.29 / 1.66 | 2.31 / 1.62 | 2.14 / 1.61 |
| comments | 1.90 / 1.68 | 2.07 / 1.63 | 2.20 / 1.62 | 2.19 / 1.61 |

## What it shows

1. **Year over year, an endpoint's level moves like a random walk, and the
   simulation's reverts.** With whole-year blocks the data's adjacent
   slope is +0.28 to +0.31 and the next one +0.01 to +0.07; the
   simulation's are −0.01 to −0.12 and −0.21 to −0.23. The head group on
   submissions is the one place where the data leans the simulation's way
   (+0.18 and −0.08).
2. **The Reddit panels have an endpoint-specific annual cycle, which the
   model does not have.** The slope is negative half a year apart, zero at
   three quarters, and positive at one and at two years. The positive
   values hold on all three panels in every rank group, strongest at the
   head of comments (+0.18 to +0.20 at a year); the negative one holds
   everywhere except the head of submissions. The simulation's is −0.04 at a year.
   FB at 86 weeks has one usable pair a year apart and cannot say.
   Seasonality was on the program's not-yet-examined list. It matters for
   estimation: the fit's long-difference moments at 26 and 52 weeks
   straddle the cycle, and movement that is present at half a year and
   gone at a year is what reversion looks like. How much of the fitted
   reversion is the cycle has not been measured.
3. **More than twice as many established endpoints relocate to the
   boundary as simulated (2.5 and 3.8 times), and the whole cohort loses
   ground.** The shift in log permanent rank between the two windows is
   about twice as wide in the data at the 75th percentile and 1.6 times at
   the 95th, and its median is +0.09 and +0.13 where the simulation's is
   0.00 and +0.03. A step that sticks is rare: 5% of first exits, against
   30% and 17% simulated. Most first exits are brief excursions, in data
   and simulation alike, and the data then returns to the boundary again
   and again (five exits per exiting endpoint against three).
4. **On comments the simulated cohort is outside the top-K as often as the
   data's, by a different route.** 3.0% of it dies, against 0.7% of the
   data's that disappear; the data's leavers relocate and stay observed.
5. **An endpoint's volatility is not fixed over time.** The correlation of
   volatility between the two halves of a panel is 80–85% of the
   correlation between interleaved weeks; a fixed amplitude gives 95–98%
   in the simulation.
6. **Long-run and short-run volatility are mostly, not wholly, one
   amplitude.** Their correlation across endpoints is 0.83–0.85 at 13
   weeks and 0.77–0.80 at 26, against 0.99–1.00 in the simulation, on all
   four panels.
7. **On the anchored scale the tails are fat at every horizon on every
   panel**, and on comments they grow with horizon (1.90 to 2.19) while the
   simulation's fall to 1.61. Finding 6 is one source of that: endpoints
   that differ in long-run relative to short-run volatility. Rare large
   lasting moves and changing volatility (finding 5) are others, and these
   measurements do not separate them.

## Predictions, scored

| | prediction | outcome |
|---|---|---|
| a1 | anchored and entity scales agree at one week | HELD |
| a2 | anchored excess at the longest horizon on all three, at least 0.20 on submissions | HELD (+0.46, +0.53, +0.58) |
| a3 | on FB and comments it is smaller than at one week | HELD on FB (0.54 → 0.46); FAILED on comments (0.21 → 0.58) |
| b1 | relocated share at least twice the simulation's | HELD (2.5×, 3.8×) |
| b2 | median shift larger by under 0.10; 95th percentile larger by at least 0.15 | HELD (0.090, 0.099; 0.37, 0.38) |
| b3 | slides a larger share of first exits by at least 0.10 | HELD (0.15, 0.11) |
| b4 | time outside the top-K at least 1.5 times the simulation's | HELD on submissions (2.4×); FAILED on comments (0.9×) |
| c1 | simulation's 13-week skip slope negative | HELD |
| c2 | data's higher than the simulation's | HELD (4 to 10 seed SDs) |
| c3 | data's 26-week skip slope between −0.10 and +0.10 on Reddit | FAILED (+0.126, +0.118): the annual cycle |
| d1 | simulation's volatility ratio at least 0.85 | HELD |
| d2 | data's lower by at least 0.10 | HELD (0.146, 0.161, 0.103) |
| e1 | simulation, whole-year blocks: adjacent below +0.15, next below −0.05 | HELD |
| e2 | simulation, slope at one year at most +0.02 | HELD |
| f1 | simulation's amplitude figure at least 0.85 | HELD |
| f2 | data's lower by at least 0.15 on all three | HELD on submissions (0.154); short on FB (0.147) and comments (0.1499) |

Twelve held in full, three in part, one failed. The failure is the most
useful result here: it is what exposed the annual cycle.

## Consequences for the plan

- **Reversion to a fixed home is contradicted where it can be tested
  cleanly**, at the scale of a year on both four-year panels, and it is the
  simulation's shortfall in relocation. With A4 (the reversion rate is not
  identified band by band and sits on its slowest grid value in a third to
  a half of bands), the form of the lasting layer is the first question for
  decision point D1.
- **One measurement should come before any candidate for it:** refit with
  the annual cycle taken out of the long-difference moments, on data and on
  simulated panels through the same filter, and see whether the fitted
  reversion survives. It is cheap and needs no model code.
- **Candidate C-A ("one amplitude for the whole model") is not what the
  data shows.** The amplitudes of the lasting and passing parts have the
  same spread and are not the same number; A5 had already shown that
  forcing them to be one inflates the FB head. The paper's wording of the
  b = 1 law may need the same care. That wording is the owner's.
- **The exit hazard for established endpoints on comments is too high in
  the simulation** (finding 4). It was hidden because deaths and
  relocations both count as departures.

## Added after the report was first committed: the cycle's share of the variance

Data only, post hoc (`posthoc_cycle_variance.json`). The variance of
26-week changes sits 0.010 to 0.036 above the straight line through its
values at 13 and 52 weeks, in each of nine panel-by-group cells. That bulge
is what a reverting level produces. Subtracting the annual cycle's share,
with the cycle's variance estimated from the covariance of quarterly
changes one year apart, removes it in all nine (−0.020 to +0.003), and
brings the 78-week variance from 0.006–0.123 above the line to within
−0.009 to +0.027 of it. The simulation has the same bulge (0.012–0.042)
and is 0.05–0.12 below the line at 78 weeks. So in the data the variance
grows in a straight line from 13 to 78 weeks once the cycle is allowed
for, and the reversion the model fits is, on this evidence, the cycle. The
stronger test, a refit with the cycle filtered out on data and simulation
alike, is not run.

## Not shown, not run

The refit with the cycle filtered out. Whether the cycle is a fixed
pattern per endpoint or drifts. Whether the cohort's loss of ground is
newer endpoints rising. Slides against steps beyond the first-exit classes.
FB at a year. Any gate evaluation, any model change.

## Files and commands

`llm_fitting/residual_anatomy.py` (`tails_by_horizon` anchor scale,
`cohort_fate`, `momentum`, `volatility_persistence`, `amplitude_coherence`),
`tests/test_residual_anatomy.py`, `llm_fitting/phase1_anatomy.py`; this
folder: `DESIGN.md`, `cells/`, `logs/`, `report_tables.txt`,
`posthoc_momentum_by_pair.json`, `posthoc_momentum_seasonality.json`.

```
python llm_fitting/phase1_anatomy.py a7 <panel>     # facebook_a, reddit_submissions_long, reddit_comments
python llm_fitting/phase1_anatomy.py a7b <panel>    # reddit_comments_ext, reddit_submissions_long
python llm_fitting/phase1_anatomy.py a7c <panel>    # the three above and reddit_comments_ext
python llm_fitting/phase1_anatomy.py report7
```
