# Replenishment test: how much of the incumbents' slide is entry

_2026-10-14, Claude (Opus 5.5). Written before any statistic here was computed.
Asked for by the owner ("add the flag list, then run the replenishment test")._

**Status: design. NOT RUN.** Exploratory and descriptive, DEVELOPMENT data.
Data only: no fit, no simulation of a model, no gate evaluation. The two
Reddit panels of step 1; Facebook is left out (three blocks, and §2z-bw found
nothing readable there). Runner: `llm_fitting/mts_replenish.py`; known-answer
tests on planted worlds in `tests/test_mts_replenish.py`.

## Question

On Reddit's calm year the incumbents' slide exceeds the lasting reordering
rate by 10–25% (§2z-by). The simplest account says the top is held by
replenishment: incumbents lose ground because other persisting endpoints
diffuse past them and because new endpoints arrive above them, and for no
other reason. Three questions follow. How much of the slide is entry? Does
the rest equal the lasting rate measured from reordering and from variance?
And does a named endpoint's mean change depend on its rank, which it should
not if diffusion and entry are all that move it?

## What is exact and what is a test

For a cohort fixed on block 0 and two later blocks a and b, D weeks apart:

    slide = slide_P + I,     I = E − X

- **slide**: minus the cohort's mean change against the centre (mean level of
  the top K positions) of all endpoints, per week. Step 1's slide.
- **slide_P**: the same against the centre of the endpoints with a level in
  both blocks. No entrant or exit enters it.
- **E**: the end block's centre with entrants minus without them. **X**: the
  start block's centre with later exits minus without them.

This is an identity; the run prints its residual. It attributes; it tests
nothing. The tests are:

1. **slide_P against two lasting rates measured otherwise**: the reordering
   route of §2z-by (0.0034–0.0036 comments, 0.0035–0.0040 submissions), and a
   variance route computed here, per cohort bin over blocks 1 to 3:
   σ² = [var(change 1→3) − ½(var(1→2) + var(2→3))] / 52, which cancels both
   the averaging offset and a constant left by passing swings, and
   c_var = σ² / (2S) with S the ladder's local slope over the bin's ranks.
   In any stationary closed set these agree whatever holds the ladder (R4),
   so agreement checks the estimators and the stability of the persisting
   set's shape. It does not select a mechanism.
2. **Whether mean change depends on rank among named endpoints.** Eight bins
   of permanent rank from (K/128, K/64] to (K, 2K]. The **tilt**: a weighted
   least-squares line through the bin means against log rank, reported as the
   fitted bottom bin minus the top bin, per week. Diffusion and entry alone
   give zero. A platform that favours small communities, or presses on large
   ones, gives a positive tilt. This is the part that can tell the simplest
   account from drift by position. It is centre-free.

Intervals: 5–95% from 400 endpoint bootstraps within bins; centres held
fixed. Endpoint bootstraps ignore dependence between endpoints.

**Flagged weeks.** Submissions are run twice: all weeks, and with the two
weeks of the score-capture guard (`instrument_eras`, weeks of 27 April 2020
and 7 June 2021) set to missing. Comments have no flagged week. Scores are
taken on all weeks; the second run is reported beside it.

## What I have already seen

Step 1's tables: the slide (0.0043 and 0.0044 in the primary pair; 0.0056 and
0.0051 with no gap), flow at K per endpoint per week (0.0011 and 0.0003;
0.0021 and 0.0009), "little difference between bins" on comments within K.
M2: 81–87% of entrants are new. §2z-by's reordering rates. So R1, R2 and R4
are informed. Not seen: the exact entry term, slide_P, the variance route by
bins with spacing, the tilt, and anything about the bin from K to 2K.

## Predictions

Pairs 1→2 (2020→2021) and 2→3 (2021→2022); "primary" is 2→3.

- **R1**: entry net of exit, I, is 20–40% of the slide on comments and 5–25%
  on submissions, in both pairs.
- **R2**: slide_P in the primary pair is within 15% of the reordering route's
  range (so 0.0029–0.0041 on comments, 0.0030–0.0046 on submissions).
- **R3**: the variance route's pooled c_var is within 25% of slide_P in the
  primary pair. Least sure: this estimator has not been on real data.
- **R4**: endpoints first seen during the earlier block carry at least 70% of
  the entry term E in both pairs.
- **R5a** (comments): no tilt in either pair. A tilt counts if it exceeds
  0.0010 a week in size with an interval excluding zero.
- **R5b** (submissions, primary pair): a positive tilt by the same rule. The
  feed change of 2021–2022 pressed on the largest communities. Less sure.
- **R5c** (both panels, both pairs): the bin from K to 2K does not rise
  against the cohort by more than 0.0010 a week and three bootstrap standard
  deviations.

Scored by `mts_replenish.score`, fixed before the run. With two pairs and a
5–95% interval, R5a fails by chance about one time in five when the true
tilt is zero and the interval is narrow enough for the size threshold not to
protect it.

## Known-answer checks before the data

In `tests/test_mts_replenish.py`, on planted random walks: with no entry, I is
zero to rounding, slide_P is within 30% of σ²/(2S) and the variance route
within 35%; with births planted inside the top K, the identity holds to
rounding, E is positive and wholly new; with a drift planted on the endpoints
first ranked K to 2K, the tilt and the bin rule both detect it.

## Limits

One calm year and one less calm. The both-present set is defined by the 90%
presence rule, so about one entrant in six is an endpoint that merely missed
the rule in the earlier block (§2z-bv); R4 separates those. The variance
route needs the same endpoints in three blocks and is noisy at the head. A
zero tilt among named endpoints within 2K says nothing about ranks below 2K,
where the block ladder is not certified. Nothing here compares a model.

## Commands and outputs

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_replenish.py run <panel>
    python llm_fitting/mts_replenish.py report

`cells/replenish__<panel>.json`, `report_tables.md`, `REPORT.md`, a
MODEL_STATUS entry and a ledger row.
