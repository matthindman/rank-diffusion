# M2 — who the entrants are, and where and when the ladder's shape moves

_2026-10-13, Claude (Opus 5.5). Written before any M2 statistic was computed.
Asked for by the owner the same day ("design and measure who the entrants
are (new endpoints or returning ones), and where and when the ladder's shape
moves"). Codex: this folder reports output that depends on step 1; do not read
it until your V4 recovery map is committed._

**Status: design. NOT RUN.** Exploratory and descriptive, DEVELOPMENT data,
in sample. Data only: no fit, no simulation, no gate evaluation. A new design
under section 7 of `MTS_PROTOCOL_STEPS_1_2_2026-10-13.md`, not a change to it.
Runner: `llm_fitting/mts_step1b.py`; statistics tested on synthetic panels in
`tests/test_mts_step1b.py`.

## Why

Step 1 (MODEL_STATUS §2z-bu) failed its feasibility screen on all three
panels for two reasons: flow is not small, and the ladder's shape moves. Both
were measured only as totals. Before the slope prediction is posed again, the
two terms have to be opened up: flow by where the entrants come from, and
shape change by rank and by date.

**What I have already seen.** Step 1's tables: the size of flow and of the
shape term by k and by year pair, the signs of the shape term (negative on
both Reddit panels, positive at the Facebook head), and the count of
occupants from outside the cohort. And, received from the owner while this
design was being written and before any M2 statistic existed, a chart from
earlier work (below). So the predictions about shape on the submissions panel
are informed twice over and are best read as a reproduction. The predictions
about who the entrants are, and everything about the comments panel's timing,
are not informed: nothing so far shows them.

## The owner's context, and a registered date

The owner, 2026-10-13: "we know in some cases the kinds of things that
reshape the ladder. We can see easily in the fuller Reddit data when things
changed: in response to a few top subreddit running away with more and more
market share, the new recommendation algorithm immediately stopped them from
growing further, and everything gradually produced the new, differently
shaped curve that was less steep in the upper head." Asked for the date, the
owner was not sure and supplied a chart used before: "Change in submission
karma share by subreddit rank", monthly, 2019 to 2022, NSFW subreddits
excluded, share of all submission karma at ranks 1, 3, 5, 10, 25, 50, 100,
250, 500, 1,000 and 5,000, with a dotted line captioned "June 28, 2021
algorithm change".

What the chart shows, as I read it: the share at rank 1 rises from about 4% at
the start of 2019 to about 12% late in 2020, holds near 10%, and falls from
the dotted line to about 1.4% by the end of 2022; ranks 3 to 10 decline
slowly throughout; ranks 250 to 1,000 are flat and step up after the line;
rank 5,000 rises throughout.

**Registered before any run: 28 June 2021, on both Reddit panels.** Its source
is the chart's caption. I have not verified the date against Reddit's own
announcements, and the owner is not sure of it. The chart is of submissions;
that the same date matters for comments is a prediction.

This turns part of the shape question from "does it drift" into "how does the
ladder adjust to a dated change at the top", which is step 5 of the agenda
and something a rank diffusion makes a prediction about: the top responds at
once, and each lower stretch of the ladder follows on its own time scale. M2
measures that time course. It does not compare it with a model's prediction;
that needs a model and is a later step.

One consequence for step 1 (§2z-bu) is already clear from the calendar and
will be reported with the block dates: the cohort of its primary pair was
chosen before the change and measured after it.

## Declarations carried over from step 1

Same three panels, loader, level (`log1p` of the weekly metric), tracking
(ever inside the weekly top 4K), blocks (52 weeks on Reddit, 26 on Facebook,
counted back from the end), presence rule (a block level needs 90% of the
block's weeks) and certification (anything that depends on a block ladder
beyond its certified rank is NOT ADJUDICATED). Weekly ladders are exact down
to 4K.

## Part E — entrants and exits

**Entrant** (as in step 1): an endpoint in the top k of the later block's
ladder with no block level in the earlier block, for consecutive blocks and
k = K/4, K/2, K and 2K. Each is classified by what the record shows up to the
end of the earlier block:

| class | definition |
|---|---|
| new | first observed during the earlier block (never observed before it began) |
| silent | observed before the earlier block began, absent in every week of it |
| intermittent | observed before it began, present in under half of its weeks |
| nearly continuous | observed before it began, present in 50–89% of its weeks: it missed the 90% rule |

For each class: the count, the share of entrants, the share of the entrants'
gain (the amount by which they exceed the level at rank k), the median rank
at which they sit in the later block, and for new ones the median number of
weeks since first observation.

"New" means new to the record. On Reddit that is no archived item since the
panel began; the instrument holds no creation dates, so a subreddit dormant
since before December 2018 is indistinguishable from a new one. On Facebook
ids are page names and enrolment was frozen, so a new id is a rename, a late
addition or a page that had not posted. For the first pair of blocks only the
few weeks dropped at the start of the panel precede the earlier block, so
that pair is reported and marked left-censored, and no prediction uses it.

**The top K's origins.** For each later block, the occupants of the top K
classified by their status in the first block: in the first block's cohort
(permanent rank within K, 90% presence); permanent rank in (K, 2K]; beyond 2K;
partly present in the first block; not observed through the end of the first
block.

**Exits.** An endpoint in the top k of the earlier block with no level in the
later one: gone (not observed again in the panel), partly present in the
later block, or silent in the later block and observed after it. The last
pair is right-censored and marked so.

## Part S — shape

**Where.** For every week, the mean level of the ladder over bands of current
rank: (0, K/128], then octaves up to (K/2, K], then (K, 2K] and (2K, 4K]. These
are properties of positions, not of endpoints. Three series per band: the
level itself; the level against the top-K centre; the level against the log of
the platform's weekly total of the metric. For each: the trend per year by
least squares over the panel, with a 5–95% interval from a circular
moving-block bootstrap of residuals (13-week blocks, 400 draws), and the block
means with their changes from block to block. The same for the cumulative
top-k means that step 1's shape term is built from.

**When.** For the bands (K/32, K/16] and (K/8, K/4] and the top band: the
series of 13-week means against the centre, the change from each 13-week
period to the next with its start date, and the share of the summed absolute
changes carried by the largest one.

**By term.** The exact accounting of step 1 for every consecutive pair of
blocks (including the first pair, which step 1 did not compute because it
needs no cohort): occupant loss, reordering gain, flow and shape per endpoint
per week, at k from K/64 to K.

**Shares, as in the owner's chart.** For every week, the log share of the
platform's total of the metric at each of the chart's ranks that the tracked
set reaches, for the top 1, 5 and 20 positions together, and for the five
names that held the top over the 26 weeks before the registered date. The
last separates what happens to positions from what happens to the endpoints
that held them. The panel includes NSFW subreddits and weekly sums, so levels
will differ from the chart; the shape of the series should not.

**Before and after the registered date.** For each of those series and for
each band against the centre: the trend before, the trend after, their
difference, and the mean of the 26 weeks after minus the 26 weeks before.
For each band, the number of weeks after the date at which its 13-week mean
has covered half of the distance from the 13 weeks before the date to the last
13 weeks of the panel.

**Intervals.** The moving-block bootstrap runs short on persistent series. In
synthetic checks its nominal 5–95% interval covers a true zero trend about
75–80% of the time for moderately persistent residuals and 55–60% for
strongly persistent ones, at any block length from 13 to 39 weeks. Tables
give the nominal interval and one doubled about the estimate, which covered
at least 90% in the same checks. Every scoring rule that says "interval
excluding zero" uses the doubled one.

**Facebook's collection holes.** Ten of Era A's weeks contain a flagged
low-count day (`instrument_eras.py`). Trends on Facebook are computed on all
weeks (primary) and with those weeks left out (secondary, reported beside
it).

## Predictions

Entrants (not yet seen). k = K, pairs with at least one full block of history
before the earlier block: blocks 1→2 and 2→3 on Reddit.

- **E1**: on both Reddit panels, new endpoints are between 25% and 60% of
  entrants by count in each of the two pairs.
- **E2**: silent endpoints are under 10% of entrants on Reddit.
- **E3**: entrants come in low: their median rank is beyond K/2, on Reddit, in
  both pairs.
- **E4**: by the last block, endpoints not observed through the end of the
  first block are at least 10% of the top K on both Reddit panels.
- **E5**: on Reddit fewer than 30% of exits from the top K are gone from the
  record (pair 1→2, which has two later blocks to return in).
- **E6**: on Facebook (blocks 1→2, the only pair with a block of history
  before it) new ids are under 30% of entrants.

Shape (informed by step 1).

- **S1**: on both Reddit panels the level against the centre has a negative
  trend, with an interval excluding zero, in every band above K/4, and the
  trend of (K/128, K/64] is at least 1.5 times that of (K/8, K/4] in size.
- **S2**: on both Reddit panels the centre's own level rises (trend above
  zero, interval excluding zero), and the top band falls, if at all, by less
  than the centre rises. That is: the middle is catching up; the top is not
  collapsing.
- **S3**: on both Reddit panels the change against the centre at the
  cumulative top K/16 is larger in size from block 0 to 1 than from block 2
  to 3.
- **S4**: the movement is spread out: for (K/32, K/16] no single 13-week step
  carries more than 30% of the summed absolute steps, on any panel.
- **S5**: on Facebook the centre's level falls over Era A (interval excluding
  zero) and the top band falls by less.

Around the registered date (added with the owner's context, before any run).

- **S6** (submissions; a reproduction of the chart, not a prediction): the log
  share at rank 1 has a positive trend before the date and a negative one
  after, and the deepest chart rank reached has a positive trend in both
  periods, each with a doubled interval excluding zero.
- **S7** (comments; not in the chart): the trend of the top band against the
  centre is lower after the date than before, with the doubled interval of
  the difference below zero.
- **S8** (both Reddit panels; the owner's "gradually"): the top moves first.
  The top band reaches half of its change sooner after the date than the band
  (K/8, K/4] does.

Scored PASS or FAIL by these numbers in `mts_step1b.score`; NOT ADJUDICATED
where a cell is not certified or a quantity is undefined. I expect E1, S2 and
S8 to be the least safe. S4 was written before I saw the chart; on
submissions the chart suggests the top moved in an episode, and S4 is scored
at (K/32, K/16] as written.

## What this can and cannot say

It can say where the flow term comes from and whether the shape change is a
steady drift of the middle against the top or a set of episodes. It cannot say
why. "New" is new to the record. Bands by current rank describe the ladder and
say nothing about what happens to the endpoints in them. Nothing here is a
test of a mechanism, and none of it uses or informs the frozen model.

## Commands and outputs

    python llm_fitting/mts_step1b.py run <panel>
    python llm_fitting/mts_step1b.py report

through `scripts/heavy_lock.py` under `caffeinate -dims`. Output:
`cells/m2__<panel>.json`, `report_tables.md`, `REPORT.md` scoring every
prediction, a MODEL_STATUS entry and a line in the exposure ledger.
