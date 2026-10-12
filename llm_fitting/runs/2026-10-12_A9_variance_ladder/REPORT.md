# A9 — results

_Written 2026-10-12 by Claude (Opus 5.5). Design and predictions:
`DESIGN.md` (committed at a07043f before the check ran)._

Status: EXPLORATORY, descriptive, DEVELOPMENT data, in sample. Data only,
plus the frozen structure fit for one secondary column. No planted-data
validation of this comparison exists yet. Numbers: `report_tables.txt`
(`python llm_fitting/phase1_a9.py report`).

## Result

ρ = σ² / (2 s): lasting variance per week (model-free, from the growth of
the variance of changes between 13 and 52 weeks; 13 and 26 on Facebook)
over twice the local log-log slope of the time-median ladder, by octave of
permanent rank. Under the one-constant hypothesis ρ is the same at every
rank.

| ranks (as a share of K) | FB Era A | comments | long submissions | comments extension |
|---|---|---|---|---|
| K/128 – K/64 | 0.0072 | 0.0029 | 0.0037 | 0.0031 |
| K/64 – K/32 | 0.0048 | 0.0028 | 0.0031 | 0.0033 |
| K/32 – K/16 | 0.0077 | 0.0032 | 0.0028 | 0.0030 |
| K/16 – K/8 | 0.0061 | 0.0026 | 0.0031 | 0.0026 |
| K/8 – K/4 | 0.0063 | 0.0025 | 0.0030 | 0.0025 |
| K/4 – K/2 | 0.0066 | 0.0031 | 0.0028 | 0.0026 |
| K/2 – K | 0.0068 | 0.0037 | 0.0032 | 0.0033 |
| all bins but the top one: median; coefficient of variation | 0.0067; 0.25 | 0.0027; 0.25 | 0.0030; 0.12 | 0.0026; 0.17 |
| log-log slope of ρ on rank (0 = constant) | −0.06 | +0.10 | +0.01 | +0.04 |
| Spearman correlation of slope and variance across bins | +0.71 | +0.96 | +0.90 | +0.95 |

Over the same bins the variance rises by a factor of 1.6 to 2.2 from the
octave near K/16 to the bottom octave and the ladder's slope by 1.7 to 1.8.
The bins above K/128 hold 5 to 48 endpoints each and their brackets are
wide (table). With the ladder of sorted endpoint means in place of the
time-median ladder the coefficients of variation are 0.25, 0.21, 0.14 and
0.23. With the fitted σ_perm² in place of the model-free variance the
relation is weaker (coefficients of variation 0.28, 0.29, 0.18, 0.40).

## What it shows

1. **Where the list is more volatile it is steeper, in proportion.** The
   ratio of lasting variance to local slope is flat across more than six
   octaves of rank on every panel, within 12–25%, and its log-log trend
   against rank is between −0.06 and +0.10.
2. **The constant is the same on the three Reddit panels** (0.0026 to
   0.0030 per week, two metrics, two window lengths) **and about twice as
   large on Facebook** (0.0067).
3. **The model-free variance does better than the fitted one.** The fitted
   σ_perm² is estimated together with a reversion rate that is not
   recovered (§2z-bc); the growth of variance with horizon does not need
   it.

## Predictions, scored

1. *Variance in the bottom octave at least 1.5 times the octave near
   K/16.* HELD (1.58, 2.17, 1.90, 2.05).
2. *The ladder steepens with rank on Facebook and long submissions.* HELD
   (1.79, 1.68), and on both comments panels as well (1.82, 1.83).
3. *Slope and variance positively related on at least three panels.* HELD
   on all four.
4. *The one-constant form does not hold cleanly: coefficient of variation
   of ρ between 0.2 and 0.6 on at least three panels, and a log-log slope
   of s on σ² below 1.* FAILED in its first part: it is in that range on
   two panels and below it on the other two (0.12, 0.17). The second part
   held (0.58 to 0.76), though noise in the variance pulls that slope
   toward zero. The data are closer to one constant than I expected.
5. *ρ between 0.001 and 0.01 per week in the middle of the list.* HELD.

## What it licenses, and what it does not

By the rule written beforehand (coefficient of variation under 0.2 on at
least two panels), the simplest form of the owner's hypothesis is
supported at first pass: the profile of lasting variance and one number
give the shape of the ladder. That is enough to organise the diagnostic
design around it. It is not yet a derivation of the ladder:

- The constant was read off the ladder. It has to be measured from
  movement (the relative decline of incumbents and the turnover that
  balances it) before the ladder is predicted and not fitted. A rough
  comparison with the established cohort's loss of rank (§2z-aw) suggests
  a decline two to three times smaller than this constant implies; that
  comparison converts a rank shift into a level shift crudely and is not a
  measurement.
- Two smooth profiles that both rise toward the bottom of a list can have
  a flat ratio without one causing the other. Features that are not
  monotone would be a sharper test; the head of the comments panels, where
  both the slope and the variance are higher in the top bin than in the
  next two, is suggestive and is five to twelve endpoints.
- The formula is for an idealised continuous process, and no planted check
  of this comparison under the project's observation rules exists.
- The variance is indexed by permanent rank, the theory's by whoever holds
  the rank.

## Files and commands

`llm_fitting/phase1_a9.py`; this folder: `DESIGN.md`, `cells/`,
`report_tables.txt`.

```
python llm_fitting/phase1_a9.py run <panel>   # facebook_a, reddit_comments, reddit_submissions_long, reddit_comments_ext
python llm_fitting/phase1_a9.py report
```
