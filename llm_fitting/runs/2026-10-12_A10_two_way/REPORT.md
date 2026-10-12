# A10 — results

_Written 2026-10-12 by Claude (Opus 5.5). Design and predictions:
`DESIGN.md` (committed at 430b27d before anything ran)._

Status: EXPLORATORY, descriptive, DEVELOPMENT data. Data only, plus the
frozen structure fit for one variance column. No planted-data validation
of these comparisons exists. Numbers: `report_tables.txt`
(`python llm_fitting/phase1_a10.py report`).

The relation under test: local log-log slope of the ladder = lasting
variance per week / (2 g), one constant g per panel, set as the median of
variance/(2 slope) over rank bins. Variance here is the growth of the
variance of changes between 13 and 26 weeks on every panel, so that g is
comparable across panels (A9 used 13 to 52 on Reddit).

## Variance → structure

Root-mean-square error of the predicted ladder in log units, at 200 ranks
spaced evenly in log rank from 1 to K. A single power law has the same
number of free quantities; a quadratic in log rank has one more.

| variance estimate | FB Era A | comments | long submissions | comments extension |
|---|---|---|---|---|
| none: one-week variance | 0.548 | 1.100 | 0.578 | 1.375 |
| model-based: fitted σ_perm² | 0.207 | 0.548 | 0.156 | 0.629 |
| model-free: growth of variance, 13 to 26 weeks | **0.097** | **0.274** | **0.130** | **0.155** |
| the same less an estimated annual cycle | 0.179 | 0.412 | 0.226 | 0.767 |
| best single power law | 0.252 | 0.423 | 0.199 | 0.391 |
| quadratic in log rank | 0.034 | 0.147 | 0.072 | 0.139 |
| g, model-free (per week) | 0.0067 | 0.0036 | 0.0033 | 0.0036 |
| g, one-week variance | 0.125 | 0.039 | 0.030 | 0.037 |

Error at rank 1 with the model-free variance: +0.35, −0.26, +0.15, +0.11.

## Structure → variance

Variance implied by the ladder's slope, 2 g s, against the model-free
estimate, by octave of rank (top bin excluded).

| | FB Era A | comments | long submissions | comments extension |
|---|---|---|---|---|
| typical factor between implied and estimated | 1.25 | 1.31 | 1.16 | 1.14 |
| bins where the implied value is inside the estimate's 5–95% bracket | 8 of 8 | 5 of 10 | 7 of 8 | 7 of 10 |

Bins outside the bracket: on the three Reddit panels one head bin each
with estimated variance below implied (ranks 24–49 on comments, 0.0033
against 0.0060; 20–39 on submissions, 0.0041 against 0.0055; 12–24 on the
extension, 0.0041 against 0.0048); on comments the bottom octave above
(0.0178 against 0.0152) and a bump at ranks 391–781 above (0.0112 against
0.0084), with ranks 1,562–6,250 below.

## Holdouts

| | long submissions | comments extension |
|---|---|---|
| g from the first half / the second half of the window | 0.0039 / 0.0027 | 0.0038 / 0.0039 |
| second-half ladder from first-half variance and g, error | 0.528 | 0.528 |
| the same in sample | 0.249 | 0.175 |
| carrying the first-half ladder forward, error | 0.168 | 0.194 |

Across the three Reddit panels g agrees within 9% (0.0033 to 0.0036).
Predicting one panel's ladder with another's g gives errors of 0.14 to
0.64: a 6% change in g moves the top of the predicted ladder by about 0.6
log units, because an error in slope accumulates over eight or nine units
of log rank.

## How closely the two directions correspond

In shape, closely, once the variance is measured properly. The variance a
rank must have for the ladder to be a stationary state and the variance it
does have agree within a typical factor of 1.14 to 1.31 across eight to
ten octaves of rank. Going the other way, the variance profile and one
number reproduce the ladder with 40 to 65% of the error of the best power
law on every panel, and on the comments extension nearly as well as a
curve with one more free number.

Without noise handling the relation does not appear at all: the one-week
variance gives a constant 9 to 19 times too large and a ladder two to
three and a half times worse than a power law. The model's own fitted lasting variance
sits in between and is worse than a power law on both comments panels.

In level and over time, not closely. See below.

## What this says is missing

1. **The constant.** It is read off the ladder here. It is the rate at
   which incumbents lose ground relative to the whole, and nothing in the
   project measures it from movement yet. It also has to be known to a few
   percent before the level of the ladder, as opposed to its shape, can be
   predicted. It is the same on three Reddit panels and 1.9 times larger
   on Facebook, and it is not stable in time on long submissions (0.0039,
   then 0.0027).
2. **Time.** 1/g is about 280 to 300 weeks on Reddit and 150 on Facebook.
   That is how long a ladder would take to adjust to its variance profile,
   and it is longer than any panel here (86 to 213 weeks). The observed
   ladder is therefore mostly inherited: carrying the earlier ladder
   forward beats the variance-based prediction by a factor of three over
   half a window. A stationary-state relation cannot be tested as a
   forecast inside these panels. It needs longer ones.
3. **A variance partition that is right for the long run.** The fitted
   σ_perm² is estimated together with a reversion that is not recovered
   (§2z-bc) and predicts the ladder worse than the plain growth of
   variance with horizon.
4. **The head.** Bins of 5 to 16 endpoints. On each Reddit panel one head
   bin is quieter than its steepness implies, and the predicted top value
   is off by −0.26 to +0.35 with no consistent sign. Whether the top of
   the list is held by something attached to particular endpoints cannot
   be read from this.
5. **Rank-dependent drift, possibly.** On the comments panels the variance
   has a bump around ranks 400 to 1,500 that the ladder's slope does not
   follow. One constant is an approximation; the size of the departures
   (a factor of about 1.3) is what a drift profile would have to explain.
6. **The annual cycle, handled properly.** Subtracting a cycle estimated
   in three coarse rank groups made every prediction worse. The cycle is
   between a seventh and a third of the 13-to-26-week variance growth on
   Reddit, so it matters, and the correction used here is too crude to
   help.

## Predictions, scored

1. *Model-free variance beats the best power law on all four panels.*
   HELD.
2. *Noise handling matters in a known direction: the one-week variance
   makes the ratio rise with rank, and its ladder is worse.* HELD on all
   four.
3. *The implied variance is inside the bracket in at least 60% of bins on
   each panel.* HELD on three; FAILED on comments (50%).
4. *The predicted top falls short by at least 0.3 on both comments
   panels.* FAILED (−0.26 and +0.11).
5. *Allowing for the cycle changes g by under 25% and leaves the spread of
   the ratio alone.* FAILED on both comments panels: the spread of the
   ratio rises by 0.10 to 0.11 on both, and g moves by 34% on the
   extension (22% on comments, 14% on long submissions).
6. *g agrees between halves within 25% and the second-half ladder is
   predicted within twice the in-sample error.* FAILED (44% apart on long
   submissions; errors 2.1 and 3.0 times the in-sample ones).
7. *g agrees within 25% on the three Reddit panels and Facebook's is at
   least 1.5 times theirs.* HELD (9%; 1.9 times).

Three held, one held on three of four panels, three failed. The failures
are all about level and time; the passes are all about shape.

## Cautions

Two smooth profiles that both rise toward the bottom of a list can
correspond in shape without a causal link; the test that would separate a
law from a coincidence is the constant measured independently, and
transport to data not used here. Permanent rank is a fixed label, not the
occupant of a rank. The integration of the predicted ladder puts about
+0.16 log units of binning error at rank 1 in a synthetic check. Nothing
here has a planted-data validation.

## Files and commands

`llm_fitting/phase1_a10.py` (uses `phase1_a9.py`); this folder:
`DESIGN.md`, `cells/`, `report_tables.txt`.

```
python llm_fitting/phase1_a10.py run <panel>   # facebook_a, reddit_comments, reddit_submissions_long, reddit_comments_ext
python llm_fitting/phase1_a10.py report
```
