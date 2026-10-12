# A8 — results

_Written 2026-10-12 by Claude (Opus 5.5). Design and predictions:
`DESIGN.md` (committed at 7a16d1a before any cell)._

Status: EXPLORATORY. An intervention in the fitted model on DEVELOPMENT
data: parameters as fitted, the simulator called with its existing
`kappa=0.0` so the lasting level is a random walk. Not a refit, not a
candidate, no gate evaluation. Numbers: `report_tables.txt`
(`python llm_fitting/phase1_a8.py report`). Structure cells in sample, 20
seeds; departure cells with the train-only cohort of A2, 30 seeds.

## Result

| | data | as fitted | reversion off |
|---|---|---|---|
| **Established cohort, comments extension** | | | |
| exits per cohort-week | 0.00800 | 0.00350 | 0.00481 |
| of which excursions | 0.00269 | 0.00142 | 0.00130 |
| relocated to the boundary zone | 0.049 | 0.019 | 0.033 |
| shift in log permanent rank, median / 95th percentile | 0.089 / 0.97 | −0.001 / 0.60 | 0.079 / 0.85 |
| first exit sudden and lasting | 0.054 | 0.298 | 0.288 |
| **Established cohort, long submissions** | | | |
| exits per cohort-week | 0.00851 | 0.00311 | 0.00449 |
| of which excursions | 0.00248 | 0.00114 | 0.00131 |
| relocated to the boundary zone | 0.069 | 0.018 | 0.034 |
| shift, median / 95th percentile | 0.134 / 1.06 | 0.035 / 0.68 | 0.149 / 0.87 |
| **Direction, long submissions, whole-year blocks** | | | |
| adjacent / one apart | +0.313 / +0.007 | −0.011 / −0.206 | +0.213 / +0.020 |
| **Direction, 13-week blocks, half a year apart** | | | |
| FB / long submissions / comments | −0.038 / −0.033 / −0.053 | −0.148 / −0.093 / −0.134 | −0.051 / −0.041 / −0.082 |
| **Variance of changes, middle group** | | | |
| long submissions, 52 / 78 weeks | 0.508 / 0.751 | 0.542 / 0.677 | 0.690 / 0.953 |
| comments, 52 weeks | 0.773 | 0.819 | 1.019 |
| **Ladder and head** | | | |
| ladder drift, FB / long submissions / comments | 0.267 / 0.501 / 0.258 | 0.115 / 0.136 / 0.172 | 0.907 / 1.145 / 0.370 |
| top-1 share over the data's (seed SD) | 1 | 2.78 (0.61) / 0.75 (0.16) / 1.38 (0.49) | 14.7 (11.5) / 4.02 (2.13) / 0.99 (0.29) |
| card rows passed | | 15 / 13 / 12 | 14 / 14 / 12 |
| macro index | | 1.09 / 0.55 / 0.86 | 2.67 / 1.43 / 0.64 |

## What it shows

1. **Direction moves to the data.** Without reversion the year-over-year
   slopes on long submissions are +0.21 and +0.02 against the data's +0.31
   and +0.01, and the half-year slope with quarterly blocks is at the
   data's on all three panels. The annual cycle stays missing (slope a year
   apart 0.00 against +0.10 to +0.12).
2. **Relocation and departures move about half of the way.** Exits of the
   established cohort rise from 0.44 to 0.60 of the data's rate on the
   comments extension and from 0.37 to 0.53 on long submissions; the
   relocated share from 0.40 to 0.68 and from 0.26 to 0.49. The median
   shift of the cohort, which the fitted model did not have at all, is
   reproduced (0.079 and 0.149 against 0.089 and 0.134): a wandering level
   and a cohort chosen for being high loses ground by itself.
3. **What stays missing is flicker at the boundary and what stays wrong is
   death.** Excursions are unchanged at about half the data's rate, repeat
   exits are half the data's, and sudden lasting first exits on comments
   stay at 29% against 5%. Reversion was not the cause of these.
4. **The home was holding the ladder.** Without it the simulated ladder
   drifts 1.4 to 3.4 times as much as the data's and the top-1 share runs
   to 15 times the data's on FB and 4 times on long submissions. Comments,
   whose top endpoint sits 1.2 log units clear of the second, is the
   exception (0.99).
5. **With reversion the simulated ladder is too still.** As fitted, ladder
   drift is 0.4, 0.3 and 0.7 of the data's on the three panels. The data's
   ladder moves more than the model allows and less than free random walks
   would make it.
6. **The lasting volatility is too large for a model without reversion**,
   as expected from fitting the two together: variance at 52 and 78 weeks
   overshoots by 27–36% in the middle group and about 50% in the head, and
   is right in the shell.

## Predictions, scored

1. *Departures rise to at least 0.60 of the data's on both, at most 1.2.*
   HELD on the comments extension (0.60); FAILED on long submissions (0.53).
2. *Relocated share at least half the data's on both.* HELD on the comments
   extension (0.68); short on long submissions (0.49).
3. *Whole-year slopes within 0.10 of +0.25 and of zero.* HELD on long
   submissions. Not run on the comments extension: the structure cells did
   not include it, an oversight in the design.
4. *78-week variance within 10% of the data's; overshoot above 10% at 52
   weeks on at least one panel.* First part FAILED (27% over); second HELD.
5. *Card loses at most two rows; index worse on at least two of three;
   ladder drift worse on all three.* HELD.
6. *FB top-1 share changes by less than one seed SD.* FAILED: it goes from
   2.8 to 14.7 times the data's.
7. *Sudden lasting first exits stay over-produced on comments.* HELD.

Three held, three in part, one failed. The failure matters most: I expected
the FB head not to depend on the home, on the strength of A3's frozen-home
arm, and it depends on it more than on anything else measured.

## What it licenses

The second outcome named in the design: the home is doing two jobs. It
pulls each endpoint back to where it started, which the data contradicts at
the scale of a year, and it holds the rank-size ladder in place, which the
data needs (though less tightly than the fitted model has it). Setting the
reversion to zero is not a candidate. A candidate has to keep a pull on the
ladder and drop the pull on the endpoint: for example a pull that depends
on where an endpoint currently ranks and not on where it began, which is
what the simplest rank diffusion at the bottom of the ladder run already
has, or a home that itself moves. Either needs the lasting volatility
re-estimated with it, and either leaves the boundary flicker and the deaths
of finding 3 to be explained separately.

## Not run

A refit of any kind. The comments extension in the structure cells.
Instagram. Any gate evaluation.

## Files and commands

`llm_fitting/phase1_a8.py`; this folder: `DESIGN.md`, `cells/`, `logs/`,
`report_tables.txt`.

```
python llm_fitting/phase1_a8.py dep <panel>       # reddit_comments_ext, reddit_submissions_long
python llm_fitting/phase1_a8.py struct <panel>    # facebook_a, reddit_submissions_long, reddit_comments
python llm_fitting/phase1_a8.py report
```
