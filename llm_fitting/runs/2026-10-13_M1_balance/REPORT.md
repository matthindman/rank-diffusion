# M1 — the balance accounting on the development panels: report

_2026-10-13, Claude (Opus 5.5). Protocol: `MTS_PROTOCOL_STEPS_1_2_2026-10-13.md`,
v2, committed at 223f5e2 before any run. Codex: do not read this file until
your V4 recovery map is committed._

**Correction, 2026-10-13 (MODEL_STATUS §2z-bw; `runs/2026-10-13_M1_review/`).** On block means the
reordering gain and the occupants' loss are about a third short of their
continuous-time values, and a cohort's slide is not. So this report's
statements that the reordering gain equals the constant of §2z-bf, that the
slide over-predicts it by 1.6 to 1.7 times, and that the simple map would give
slopes 40% too shallow are withdrawn as stated: divided by the averaging
factor the reordering rate is 0.0036–0.0043 a week on Reddit, and the slide
exceeds it by about 10%. The numbers below are unchanged.

**Status: RAN-OK, three primary runs.** Exploratory and descriptive,
DEVELOPMENT data, in sample. No fit, no simulation, no gate evaluation. The
scoring rules and the screen were fixed in the protocol and are applied by
`mts_step1.score`; nothing was changed after a number was seen. Mechanism
readings wait for step 2 (V4). The three declared secondary runs are reported
at the end.

## What was run

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_step1.py run facebook_a --reviewed
    … run reddit_submissions_long --reviewed
    … run reddit_comments_ext --reviewed
    python llm_fitting/mts_step1.py report

| panel | weeks | endpoints tracked | blocks | certified exact top of each block ladder | run time |
|---|---|---|---|---|---|
| `facebook_a` (K = 3,500) | 86 | 15,455 | 3 × 26 weeks | 13,181; 13,171; 13,018 | 9 s |
| `reddit_comments_ext` (K = 12,500) | 213 | 532,903 | 4 × 52 weeks | 29,181; 32,925; 36,312; 36,899 | 4 min |
| `reddit_submissions_long` (K = 5,000) | 212 | 113,929 | 4 × 52 weeks | 13,516; 14,364; 15,257; 15,571 | 3 min |

Every scored cell is certified exact: the certified rank exceeds 2K and the
small-step band in every block used as a start. No run was killed. Cells are
in `cells/`, logs in `logs/`, all tables, flags and score details in
`report_tables.md`.

One thing about order: I read the tables for Facebook and submissions while
the comments run was still going. Nothing was changed between runs.

## Scores

| | Facebook Era A | Reddit comments | Reddit submissions |
|---|---|---|---|
| A1 identity closes | PASS | PASS | PASS |
| A2 one-week reordering within 0.6–1.2 of its small-step value | PASS | PASS | FAIL |
| A2b below 1 at the head | PASS | FAIL | FAIL |
| A3 one-week loss at least 5× the longest-horizon loss | PASS (12×) | PASS (5.8×) | FAIL (3.6×) |
| A4 weekly shape term under 10% of R | PASS | FAIL (28%) | FAIL (40%) |
| B1 block reordering gain inside the §2z-bf range | PASS | PASS | PASS |
| B2 every cohort bin slides; slide within 0.5–2 of the gain at K | not adjudicated | PASS | PASS |
| B3 little difference between bins | inconclusive | PASS | inconclusive |
| B4 no association with the still earlier level | not adjudicated | FAIL | inconclusive |
| B5 predictable share 0.5–1.2 | not adjudicated | FAIL (high) | FAIL (high) |
| B6 flow small | FAIL | FAIL | FAIL |
| B7 shape stable over a block step | FAIL | PASS | FAIL |
| B9 block reordering within 0.5–1.1 of its small-step value | FAIL (1.10 at K/2) | PASS | FAIL |
| B10 no-gap slide within 0.7–1.5 of the slide with a gap | not adjudicated | PASS (1.30) | PASS (1.17) |
| **feasibility screen** | **FAIL** | **FAIL** | **FAIL** |

B8, across platforms: FAIL. The Facebook slide is 0.82 of the Reddit mean; I
predicted 1.3 to 3.

Of 42 panel-level scores: 19 PASS, 16 FAIL, 3 inconclusive, 4 not adjudicated
(all on Facebook, which has one pair, no gap and no earlier block). Sixteen
failures is more than I expected. Most of them say the same two things: flow
and shape change are not small, and the submissions panel behaves differently
from the comments panel.

## What was measured

Primary pair, centre K. Rates are per endpoint per week.

| | Facebook (26-week blocks, no gap) | comments (year 3 to 4) | submissions (year 3 to 4) |
|---|---|---|---|
| reordering gain, K/16 to K/2 | 0.0061–0.0077 | 0.0026–0.0030 | 0.0025–0.0034 |
| the constant of §2z-bf, variance over twice the slope | 0.0067 | 0.0026 | 0.0030 |
| reordering gain over its small-step value, K/16 to K | 0.70–1.10 | 0.88–1.06 | 0.68–1.48 |
| slide of the cohort fixed on block 0 (seven octave bins pooled) | 0.0044 | 0.0043 | 0.0043 |
| same, no-gap year | — | 0.0056 | 0.0051 |
| slide over the reordering gain at K | 0.84 | 1.73 | 1.63 |
| flow over the reordering gain, k up to K/2 | 3–24% | 0–29% | 8–18% |
| shape term over the reordering gain, K/16 to K/2 | 5–46% | 6–18% | 12–76% |
| predictable share of the reordering gain at K/4, K/2, K | 0.50, 0.62, 0.59 | 1.21, 1.27, 1.16 | 1.68, 1.42, 1.08 |
| association of the change with the level one block earlier | −0.002 (−0.023 to 0.015) | +0.152 (0.129 to 0.173) | +0.118 (0.051 to 0.180) |
| and with the still earlier level | — | −0.129 (−0.151 to −0.107) | −0.134 (−0.189 to −0.075) |
| occupants of the top K at the start block that were not in the cohort | 919 of 3,500 | 3,702 of 12,500 | 1,615 of 5,000 |

Weekly positions: the share of the top k replaced within one week is 17–30% on
Facebook, 8–10% on comments and 6–7% on submissions; within a year it is
23–35% on Reddit.

### 1. The constant has now been measured a second way, and the two agree

The reordering gain is computed from who replaced whom on the block ladders.
It uses no variance and no slope. On all three panels it lands on the constant
that §2z-bf obtained as variance over twice the slope: 0.0026–0.0030 against
0.0026 on comments, 0.0025–0.0034 against 0.0030 on submissions,
0.0061–0.0077 against 0.0067 on Facebook. Computed in the same run, the
small-step value agrees with the reordering gain to within 13% at every k on
comments; on submissions and Facebook it is within 0.7 to 1.5. This is the
first measurement of that constant that does not take it from the ladder.

### 2. Incumbents slide, at about the same rate everywhere

A cohort fixed in year 1 loses 0.0043 per week against the mean level of the
current top K between years 3 and 4, on both Reddit panels. Every octave bin
from K/128 to K slides, with intervals that exclude zero. On Facebook the one
available pair gives 0.0044. That is about 20% a year. The value barely moves
with the boundary (0.0041–0.0044 on Reddit at K/2, K and 2K; 0.0036–0.0046 on
Facebook).

The slide is not mostly the fading of whatever selected the cohort: in the
year next to the selection year it is 1.17 and 1.30 times larger, inside the
range I set. It differs little between rank bins on comments (five of seven
bins within 30% of the pooled value; the other two too wide to say). On
submissions the top bins slide faster, but their intervals are wide.

That the slide is the same on Facebook as on Reddit, when the reordering gain
is twice as large there, was not predicted. B8 predicted the opposite and
failed.

### 3. No sign of a return toward an endpoint's own level; the association runs the other way

On both Reddit panels the change from year 3 to 4 is positively associated
with the year-2 level and negatively with the year-1 level, at about ±0.13:
endpoints that rose from year 1 to year 2 tend to keep rising two years later.
A personal home would give the opposite signs. Following the review, I do not
read this as a mechanism. As a description it agrees with §2z-aw (direction
persists) and with the predictable share in finding 5.

### 4. Flow and shape change are not small

- **Flow.** Endpoints with no block level a year earlier supply 15–29% of the
  reordering gain between K/8 and K/2 on comments, and 31% of everything that
  replaces incumbents at K. Two years after selection, 30% and 32% of the top K on
  the two Reddit panels are endpoints that were not in the cohort.
- **Shape.** On submissions the top of the ladder is sinking toward the centre:
  the shape term is 45–76% of the reordering gain between K/16 and K/4 in
  years 3 to 4, and larger in years 2 to 3. On weekly positions the same term runs at about −0.004 per
  week at K/16 on both Reddit panels over the whole window, which is 0.8 log
  units in four years. On Facebook the head is rising against the centre
  (46% of the gain at K/16). Only comments in years 3 to 4 has a stable shape.

So on two panels of three the ladder is not stationary to the accuracy this
accounting needs, and entry is a first-order term on the largest one.

### 5. The slide over-predicts the reordering gain on Reddit

Weighting each occupant by the mean slide of its cohort bin gives 1.1 to 1.7
times the reordering gain on Reddit: 1.42 to 1.68 at K/4 and K/2 on
submissions, with intervals wholly above 1.2, and 1.16 to 1.27 on comments,
where the failure is marginal. Between K/4 and K the same sum is 1.3 to 1.9
times what those same occupants actually lost on comments and 1.4 to 1.6
times on submissions. The endpoints that hold the top k at the start lose less than their
cohort does: they are, within their cohort, the ones that had been rising.
On Facebook the share is 0.5–0.6: there the block ladder still carries enough
passing swing (a quarter of the top k is replaced every week) that reordering
exceeds what the cohort's slide accounts for.

### 6. The small-step relation at one week

It holds on Facebook and comments within my range and fails on submissions,
where the one-week reordering gain is 0.37–0.71 of its small-step value. Large
single-week changes on that panel add variance without adding proportionate
reordering near a rank.

## The feasibility screen

FAIL on all three panels.

| part | Facebook | comments | submissions |
|---|---|---|---|
| identity closes | pass | pass | pass |
| predictable share material and determined | fail | pass | fail |
| shape stable | fail | pass | fail |
| flow small | fail | **fail** | fail |
| small-step bridge within 0.5–1.5 | pass | pass | pass |
| two pairs of one cohort agree | not adjudicated | pass | pass |

Comments fails on flow alone. By the rule fixed beforehand, step 3 in the form
the protocol wrote it, slopes from S = v / (2s) with s the cohort's slide, is
**not worth attempting on these panels as they stand**. The numbers say why:
the slide is 1.6 to 1.7 times the reordering gain on Reddit, so that map would
give slopes about 40% too shallow.

The result of the phase is therefore the stated requirement, not a model:

1. **The balance has to be used with all its terms.** Occupant loss equals
   reordering gain plus flow minus shape change, exactly. A prediction of the
   ladder from movement needs entry measured as a quantity of its own and has
   to allow the shape to move.
2. **Who holds a position is selected on recent direction.** A cohort's slide
   is not the loss of the endpoints currently at the top. A prediction needs
   the mapping from cohorts to current occupants, including that selection.
3. **Facebook needs a longer or a quieter instrument**: three 26-week blocks
   give one pair with no gap, and its block ladder is not free of passing
   swings.

## What this does and does not show

It shows, descriptively and in sample: the reordering rate of the ladder
equals variance over twice the slope when both are measured; incumbents
decline against the current top K at about 0.0043 per week on three panels
and two platforms, at a rate that does not depend much on rank and that
persists from year to year; there is no return toward an endpoint's own
earlier level at the scale of a year; entry and shape drift are too large to
ignore.

It does not show what holds the ladder. The common slide is what my
hypothesis of §2z-bq predicts (B2 and B10 passed on both Reddit panels, B3 on
one), but the same slide would appear under other mechanisms, and Codex's
review showed that nothing here can separate them without the planted study.
It does not show stationarity; it found the opposite on two panels. It says
nothing about any sealed panel.

## Deviations

None from the protocol v2. The secondary runs were started after the primary
results had been read.

## Secondary runs (declared; reported, not scored)

Twenty-six-week blocks on the two Reddit panels, read only for pairs of
blocks a whole year apart, with a cohort chosen on each block in turn (22
cohort-and-pair combinations per panel), and the 136-week comments window
(3 combinations). All certified. Tables are in `report_tables.md`.

| at K/4, range over combinations | comments | submissions |
|---|---|---|
| slide per week | 0.0041–0.0060 | 0.0035–0.0057 |
| reordering gain per endpoint per week | 0.0032–0.0035 | 0.0029–0.0039 |
| reordering gain over its small-step value | 0.85–1.00 | 0.56–1.15 |
| flow over the reordering gain | 13–36% | 5–24% |
| shape term over the reordering gain | −130% to +48% | −99% to −18% |

- The slide is there in every cohort and every year, between 0.0035 and 0.0060
  per week. For one fixed pair of outcome blocks it is about 10% lower when
  the cohort is chosen two years earlier than when it is chosen just before
  (comments, last pair: 0.0045 with no gap, 0.0041 four blocks back). It is
  lower in the later calendar years.
- The reordering gain is slightly above its 52-week value, as shorter blocks
  carry more of the passing swings.
- On submissions the shape term is negative in every pair: the top keeps
  sinking toward the centre. On comments it changes sign from year to year. So
  the stable shape that comments showed in the primary pair is one year's
  value and not a property of the panel.
- The 136-week comments window reproduces the first pairs of the extension.
