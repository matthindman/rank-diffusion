# Replenishment test: report

_2026-10-14, Claude (Opus 5.5). Design, predictions, runner and planted-world
tests committed at 884c3ec before the run._

**Status: RAN-OK, two runs** (submissions also with the two flagged weeks set
to missing). Exploratory and descriptive, DEVELOPMENT data. No fit, no model
simulation, no gate evaluation. Every pair is certified exact. Nothing was
changed after a number was seen. Tables: `report_tables.md`; cells in `cells/`.

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_replenish.py run <panel>
    python llm_fitting/mts_replenish.py report

## The short answer

**The accounting closes, and the simplest account does not survive its one
discriminating test.**

- The incumbents' slide splits exactly into displacement by entrants and a
  slide against the endpoints that persist. Entry net of exit is 29–41% of
  the slide on comments and 8–19% on submissions. Four fifths of the entry
  term is endpoints new to the record.
- What is left, the slide against persisting endpoints, agrees within about
  12% with the lasting rate measured two other ways. So the excess of the
  slide over the reordering rate (§2z-by) is entry.
- But a named endpoint's mean change depends on its rank, which diffusion and
  entry alone do not produce. On submissions, lower-ranked communities gained
  on higher-ranked ones in both years. On comments there is no such tilt in
  2020→2021 and the opposite one in 2021→2022.

## 1. The split (exact)

Per endpoint per week; cohort fixed on 2019; centre K.

| | slide (5–95%) | entry E | exit X | I = E − X | I over the slide | slide against persisting endpoints (5–95%) | new endpoints' share of E |
|---|---|---|---|---|---|---|---|
| comments 2020→2021 | 0.00556 (0.00537–0.00577) | 0.00280 | 0.00049 | 0.00230 | 41% | 0.00326 (0.00306–0.00347) | 84% |
| comments 2021→2022 | 0.00428 (0.00407–0.00446) | 0.00180 | 0.00057 | 0.00123 | 29% | 0.00305 (0.00284–0.00323) | 80% |
| submissions 2020→2021 | 0.00510 (0.00478–0.00538) | 0.00140 | 0.00041 | 0.00099 | 19% | 0.00411 (0.00379–0.00439) | 84% |
| submissions 2021→2022 | 0.00435 (0.00407–0.00468) | 0.00080 | 0.00046 | 0.00033 | 8% | 0.00402 (0.00373–0.00435) | 76% |

The identity's residual is zero to rounding in every pair. The slides are
those of step 1. Most of the difference between the two years on comments
(0.0056 against 0.0043) is entry: the slide against persisting endpoints is
0.0033 and 0.0031.

## 2. Three routes to the lasting rate

| | slide against persisting endpoints, 2021→2022 | reordering route (§2z-by) | variance route, new here (5–95%) |
|---|---|---|---|
| comments | 0.0031 | 0.0034–0.0036 | 0.0034 (0.0033–0.0036) |
| submissions | 0.0040 | 0.0035–0.0040 | 0.0039 (0.0035–0.0042) |

Three estimators that share no step agree within about 12%. This is a check
on the estimators and on the stability of the persisting set's shape; in a
stationary closed set it holds whatever holds the ladder. The variance route
had not been on real data before.

By bin the variance route is not flat on comments: 0.0039–0.0045 for ranks
391 to 3,125, then 0.0037, 0.0030 and 0.0024 for the three bins down to 2K.
The lasting variance stops rising below K/4 (0.013 a week in all three)
while the ladder keeps steepening. On submissions it is 0.0030–0.0046 below
rank 312, with wide intervals above.

## 3. Does mean change depend on rank? (the discriminating part)

Fitted bottom bin (K to 2K) minus top bin (K/128 to K/64), per week:

| | 2020→2021 | 2021→2022 |
|---|---|---|
| comments | +0.0002 (−0.0004 to +0.0009) | −0.0020 (−0.0027 to −0.0014) |
| submissions | +0.0035 (+0.0025 to +0.0044) | +0.0013 (+0.0004 to +0.0022) |

- **Submissions: a positive tilt in both years.** Named communities ranked
  156 to 625 in 2019 fell by 0.0023 a week against the cohort in 2020→2021
  (more than three bootstrap standard deviations in both bins); the bin from
  K to 2K rose by 0.0005. The size across the cohort, 0.0035 a week, is two
  thirds of the slide. I predicted the sign for 2021→2022; I did not expect
  it larger the year before.
- **Comments: none, then negative.** In 2021→2022 the bins from 781 to 3,125
  rose against the cohort and the bin from K to 2K fell.
- **Survivors.** The lowest bin loses 5–7% of its members a year to the
  presence rule and the top bins under 1%, which flatters the lowest bins.
  On submissions the gradient is present among the bins with at most 2%
  attrition (ranks 156 to 2,500), so it is not that. On comments the bias
  works against the tilt found.

**A caution found after the run.** The rule I fixed takes "no drift by rank"
to mean equal mean change in logs. A process that is unbiased in levels has
a mean log change of minus half its variance, and variance rises down the
ladder; with the lasting variances of section 2 that alone gives a tilt of
−0.0020 a week on comments and about −0.006 on submissions. The comments
tilt of 2021→2022 equals that benchmark, and the comments tilt of 2020→2021
equals the other. So on comments the two years differ by 0.002 a week and I
cannot say which is "no drift". On submissions the tilt is positive against
either benchmark, and larger against the second. This is post hoc and
changes no score.

## Predictions scored

| | comments | submissions |
|---|---|---|
| R1 entry net of exit is 20–40% (comments), 5–25% (submissions) of the slide in both pairs | FAIL (41%, 29%) | PASS (19%, 8%) |
| R2 slide against persisting endpoints within 15% of the reordering route's range | PASS (0.0031) | PASS (0.0040) |
| R3 variance route within 25% of it | PASS (1.13) | PASS (0.96) |
| R4 new endpoints carry at least 70% of the entry term | PASS (84%, 80%) | PASS (84%, 76%) |
| R5a comments: no tilt in either pair | FAIL (−0.0020 in 2021→2022) | — |
| R5b submissions, 2021→2022: a positive tilt | — | PASS (+0.0013) |
| R5c the bin from K to 2K does not rise by more than 0.0010 a week | PASS | PASS (+0.0005 at most) |

Ten of twelve held. R1's miss is by one point. R5a's is the finding of
section 3.

## Flagged weeks

With the weeks of 27 April 2020 and 7 June 2021 set to missing, no figure on
submissions moves by more than 0.00006 a week and no score changes.

## What it means

1. **Replenishment is real and sized.** On comments a third of what pushes
   incumbents down is new communities arriving above them; on submissions a
   tenth to a fifth. That is the "10–25% excess" of §2z-by, located.
2. **The lasting rate is now measured three ways that agree**, which was the
   weak link named in §2z-by. On Reddit it is 0.0031–0.0036 a week on
   comments and 0.0035–0.0040 on submissions.
3. **"Diffusion and entry and nothing else" is too simple.** Mean growth
   depends on rank, differently on the two metrics and in the two years. On
   submissions the direction is the one the platform's documented feed
   changes would give (§2z-bz), and it is large. A candidate model needs a
   drift that depends on size and can change in time; this test measures it.
4. **"One constant across ranks" weakens.** With this estimator the ratio of
   lasting variance to slope falls by a third to nearly a half over the
   bottom two octaves on comments.

## What this does not show

Why growth depends on rank. Whether the submissions tilt is the feed changes:
the dates fit and nothing here ties them. Anything below 2K, where the block
ladder is not certified. Anything about Facebook. Which convention, logs or
levels, is the right null.

## Limits

Two year pairs, both containing dated platform changes. The persisting set is
defined by the 90% presence rule. Endpoint bootstraps ignore dependence
between endpoints. The variance route is noisy at the head. The benchmark of
section 3 is post hoc.
