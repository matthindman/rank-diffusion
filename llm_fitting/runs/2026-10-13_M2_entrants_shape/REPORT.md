# M2 — who the entrants are, and where and when the ladder's shape moves: report

_2026-10-13, Claude (Opus 5.5). Design, predictions and runner committed at
62b795e before any run. Codex: do not read this file until your V4 recovery
map is committed._

**Status: RAN-OK, three runs.** Exploratory and descriptive, DEVELOPMENT data,
in sample. No fit, no simulation, no gate evaluation. Every scored cell is
certified exact. No run was killed. Nothing was changed after a number was
seen. Tables and score details: `report_tables.md`; cells in `cells/`.

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_step1b.py run <panel>
    python llm_fitting/mts_step1b.py report

for `facebook_a`, `reddit_submissions_long`, `reddit_comments_ext`.

Blocks: on Reddit four 52-week blocks beginning 2018-12-31, 2019-12-30,
2020-12-28 and 2021-12-27; on Facebook three 26-week blocks beginning
2020-12-28, 2021-06-28 and 2022-01-03. The registered date, 28 June 2021,
falls in the middle of Reddit's third block.

## The short answer

**Who the entrants are.** On Reddit they are new endpoints. Of the endpoints
that enter the top K with no level a year earlier, 81–87% were first seen in
the record during that earlier year; almost none are endpoints that had been
seen before and went silent. Exits are not disappearances: of 370 exits from
the top K in the pair that has later years to return in, 7 never reappear.
The top K is being added to from below by births, and is losing almost
nothing to deaths.

**Where the shape moves.** Everywhere, as a tilt. The lower the rank, the
faster its level rises. Against the mean of the top K, every band above K/4
falls and the bands below K/2 rise, in order of rank, on both Reddit panels.

**When.** Mostly before the algorithm change, not after it. On comments about
two thirds of the head's relative fall happened between 2019 and 2020. On
both panels the tilt slows after mid-2021, when the top K's own level stops
rising. What the registered date marks is narrower and sharper: on
submissions the share of the first-ranked position falls from 8.3% to 4.3% in
one quarter, as in the owner's chart, and ranks 250 to 1,000 step up in the
same quarter. Rank 1 then goes on falling for a year and a half.

## Scores

| | Facebook | comments | submissions |
|---|---|---|---|
| E1 new endpoints are 25–60% of entrants | — | FAIL (84%, 81%) | FAIL (87%, 81%) |
| E2 silent endpoints under 10% | — | PASS (0%) | PASS (0%) |
| E3 entrants come in below K/2 | — | PASS | PASS |
| E4 by the last block, at least 10% of the top K unseen through the first | — | PASS (13.2%) | PASS (10.9%) |
| E5 fewer than 30% of exits gone from the record | — | PASS (2.7%) | PASS (0%) |
| E6 Facebook: new ids under 30% of entrants | FAIL (34%) | — | — |
| S1 every band above K/4 falls against the centre, more toward the head | — | PASS | PASS |
| S2 the centre rises; the top falls by less than that, if at all | — | PASS | PASS |
| S3 larger change from block 0 to 1 than from 2 to 3 | — | PASS | PASS |
| S4 no 13-week step above 30% of the movement at K/16 | FAIL (34%) | PASS | PASS |
| S5 Facebook: centre falls, top falls by less | FAIL | — | — |
| S6 submissions: the chart's pattern at rank 1 and rank 5,000 | — | — | PASS |
| S7 comments: the top band's trend is lower after the date | — | FAIL | — |
| S8 the top band moves first after the date | — | FAIL | PASS |

"—" is not adjudicated: the prediction was stated for other panels. Of 25
scored: 18 passed, 7 failed. My one wholly uninformed prediction about
entrants, E1, failed on both panels and in the direction that matters: I
expected a mix of new and returning endpoints and found almost only new ones.

## Part E — entrants

Entrants to the top K of a block that had no block level in the block before
(pairs with a full year of earlier history):

| | comments 2020→21 | comments 2021→22 | submissions 2020→21 | submissions 2021→22 | Facebook (26-week blocks) |
|---|---|---|---|---|---|
| entrants | 1,200 | 812 | 339 | 231 | 95 |
| as a share of K | 9.6% | 6.5% | 6.8% | 4.6% | 2.7% |
| new: first seen during the earlier block | 84% | 81% | 87% | 81% | 34% |
| seen before, silent through the earlier block | 0% | 0.2% | 0% | 0.4% | 0% |
| seen before, present in under half of its weeks | 3% | 6% | 3% | 5% | 13% |
| present in 50–89% of its weeks | 13% | 13% | 10% | 14% | 54% |
| median rank at which they sit | 8,174 | 7,999 | 3,127 | 3,520 | 2,033 |
| new ones: median weeks since first seen | 24 | 27 | 27 | 28 | 17 |

The same holds at K/4, K/2 and 2K (71–87% new on Reddit across all four
cut-offs). New endpoints also carry their share of the entrants' gain
(72–89%), so they are not marginal cases just over the line.

"New" is new to the record: on Reddit, no archived item in any earlier week
since December 2018. For the 2021→22 pair that is two full years of silence
followed by a year of presence in at least 90% of weeks, so these are either
newly created communities or ones dormant for years; the instrument holds no
creation dates to tell which.

Where the top K came from, three years on (status in 2019):

| | comments, 2022 | submissions, 2022 |
|---|---|---|
| in the 2019 cohort | 63.0% | 59.6% |
| ranked between K and 2K in 2019 | 11.0% | 12.9% |
| ranked beyond 2K in 2019 | 2.8% | 8.1% |
| partly present in 2019 | 10.0% | 8.6% |
| not seen through 2019 | 13.2% | 10.9% |

Of the top quarter of the top K in 2022, 8.1% (comments) and 7.1%
(submissions) had not been seen through 2019.

Exits from the top K: 258 and 309 on comments, 112 and 146 on submissions in
the last two pairs. Gone from the record: 7 and 1; 0 and 0. Every other exit
is an endpoint still posting that no longer has 90% presence. The last pair
is right-censored.

Facebook is different in kind, as expected of a fixed list: about a third of
its entrants are new ids and half are pages that missed the presence rule,
which with ten weeks of collection holes is mostly the instrument.

## Part S — shape

### Where

Trend per year of each band's mean level against the mean level of the top K
(nominal 5–95% interval for the top band; the doubled interval used for
scoring excludes zero for every Reddit band shown except K/4 to K/2, where
the trend is near zero):

| band of current rank | comments | submissions |
|---|---|---|
| top K/128 | −0.220 (−0.263 to −0.181) | −0.298 (−0.324 to −0.272) |
| K/128 to K/64 | −0.179 | −0.192 |
| K/64 to K/32 | −0.155 | −0.193 |
| K/32 to K/16 | −0.119 | −0.162 |
| K/16 to K/8 | −0.078 | −0.128 |
| K/8 to K/4 | −0.045 | −0.061 |
| K/4 to K/2 | −0.009 | −0.005 |
| K/2 to K | +0.044 | +0.058 |
| K to 2K | +0.121 | +0.124 |
| 2K to 4K | +0.223 | +0.216 |

It is monotone in rank on both panels and nearly the same on both. In absolute
level the top K's mean rises (comments +0.33 a year, submissions +0.21) while
the platform total rises by less (+0.16 and +0.04). On comments the top band
itself rises (+0.11 a year); on submissions it falls slightly (−0.08, doubled
interval including zero). The ranks from 2K to 4K rise by +0.56 and +0.43 a
year. So the shape changes because the middle and the bottom grow, not
because the top collapses. Rank 1 on submissions is the exception, below.

On Facebook there is no such tilt. The whole ladder falls together (centre
−0.17 a year, platform total −0.21), the top band falls a little faster than
the centre, and the bottom of the tracked list, 2K to 4K, falls much faster
(−0.41 a year against the centre), which is the fixed panel decaying.
Leaving out the ten flagged weeks changes no Facebook trend by more than
0.02.

### When

Mean level of the top K in each block, and the top band against it:

| | 2019 | 2020 | 2021 | 2022 |
|---|---|---|---|---|
| comments: top-K mean | 7.61 | 8.13 | 8.40 | 8.61 |
| comments: top band minus it | 6.07 | 5.64 | 5.47 | 5.42 |
| submissions: top-K mean | 9.68 | 10.16 | 10.29 | 10.31 |
| submissions: top band minus it | 5.12 | 4.96 | 4.57 | 4.27 |

The top K's mean jumped by about half a log unit between 2019 and 2020 on
both panels and had stopped rising on submissions by 2022. On comments the
head's relative fall is mostly that first step (−0.43 of −0.65). On
submissions it is spread over all three, largest from 2020 to 2021.

No single quarter dominates at K/16 (largest step 15–16% of the movement). For
the top band on submissions the three largest quarterly steps are consecutive:
those beginning 2020-12-28 (−0.125), 2021-03-29 (−0.114) and 2021-06-28
(−0.214).

### The registered date, 28 June 2021

Share of the platform's weekly total: 13-week means of the log share, shown
as shares.

| quarter beginning | 2020-09-28 | 2020-12-28 | 2021-03-29 | **2021-06-28** | 2021-09-27 | 2021-12-27 | 2022-09-26 |
|---|---|---|---|---|---|---|---|
| submissions, rank 1 | 10.1% | 8.6% | 8.3% | **4.3%** | 2.7% | 2.2% | 1.2% |
| submissions, top 5 positions | 17.7% | 15.8% | 13.8% | **8.7%** | 6.7% | 7.0% | 4.6% |
| submissions, the 5 names at the top before the date | 16.8% | 14.5% | 13.7% | **8.3%** | 5.8% | 5.6% | 3.5% |
| comments, rank 1 | 7.6% | 7.1% | 7.2% | **6.8%** | 5.5% | 5.6% | 4.0% |
| comments, top 5 positions | 16.8% | 16.1% | 14.1% | **13.0%** | 11.7% | 13.6% | 10.5% |
| comments, the 5 names at the top before the date | 15.8% | 15.7% | 12.4% | **11.4%** | 9.8% | 10.3% | 8.6% |

- **Submissions reproduce the owner's chart.** Rank 1's share rises from 4.5%
  in early 2019 to 10.3% in mid-2020, and its log share then has a trend of
  +0.29 a year before the date and −1.05 after (doubled intervals 0.06 to
  0.51 and −1.35 to −0.77). Rank 5,000 rises in both periods. The panel here
  includes NSFW subreddits and the chart does not, so levels differ a little.
- **It is the same endpoints losing, not only the positions.** The five names
  at the top before the date go from 13.7% to 8.3% in one quarter and to 3.5%
  by the end. Part of any such fall is selection, because the names were
  chosen for being at the top; the positional series, which have no such
  selection, fall alongside.
- **The top had stopped growing a year before the registered date, and the
  top five were already falling.** Rank 1 on submissions peaked in the quarter
  beginning 2020-06-29 (10.3%) and stood at 8.3% just before the date. The
  top five go 17.7%, 15.8%, 13.8% over the three quarters before it. On
  comments the step is in the quarter beginning 2021-03-29 (16.1% to 14.1%
  for positions, 15.7% to 12.4% for the names), one quarter early. Either
  something else was already acting, or the change took effect earlier than
  the chart's caption says for part of the top. I cannot tell which from
  these panels.
- **The middle steps up in the same quarter.** In log share of the platform
  total, ranks 250, 500 and 1,000 on submissions rise by 0.10, 0.13 and 0.14
  in the quarter beginning 2021-06-28, the largest quarterly step in each of
  those series, and are close to flat afterwards (rank 1,000 adds 0.06 in the
  next quarter). Rank 1 losing four points of share raises every other share
  by 0.04 from the arithmetic alone; these rise by two to three times that.
  Ranks 3 to 25 change by −0.01 to −0.19, no more than in their earlier
  quarters; rank 5,000 rises by about 0.06 a quarter before and after.
- **Against the top-K mean that step is invisible**, because the mean, which
  is mostly ranks beyond 1,000, steps with it. For bands from K/64 to K/4 on
  submissions the trend against the centre only slows after the date (by 0.05
  to 0.07 a year), and the centre's own level stops rising (+0.41 a year
  before, +0.03 after).
- **What is immediate and what is gradual, on submissions.** Immediate: rank 1
  halves and the middle ranks step up. Gradual: rank 1 goes on falling for the
  next year and a half (4.3% to 1.2%), and the top five with it. The owner's
  description was that the change "immediately stopped them from growing
  further, and everything gradually produced the new, differently shaped
  curve". The half-change measure of the design, which is against the top-K
  mean, gives 10 weeks for the top band, 26 for the next and 43 to 53 for the
  bands down to K/4. A single step at the date would give 6.5 and a steady
  trend about 33, so in that frame the lower bands show steady or back-loaded
  movement and no response timed to the date. S8 passes by its rule; the
  share series above are the better evidence on timing.
- **Comments: a smaller and less sharply dated version.** Rank 1's share falls
  steadily from 12.4% to 4.0% across the panel; its largest quarterly step
  (−0.22 in log share) is in the quarter after the date's. Rank 5 has its
  largest step (−0.26) in the quarter of the date and stays down. The top five
  together step down one quarter before it. Ranks 250 to 1,000 edge up by 0.01
  to 0.07 a quarter over the three quarters from 2021-03-29. S7 failed because
  the top band's fall against the centre is slower after the date, not faster:
  on comments most of the flattening was over by then. The half-change
  measure says nothing there: every band reaches half within the first
  quarter because there is so little post-date change against the centre to
  time.

## What this means for step 1 and for the agenda

1. **Flow is births.** Step 1 found that entrants supply up to 29% of the
   reordering gain. They are new endpoints, 5–10% of the top K a year, and
   nothing comparable leaves. On these panels Reddit is a growing population,
   not a stationary one with matched entry and exit.
2. **The shape change is mostly growth from below, plus one episode at the
   top.** The broad tilt predates the algorithm change and tracks the years in
   which the top K's own level was rising. The change of mid-2021 is visible
   and sharp on submissions: the first rank halves and ranks 250 to 1,000 step
   up in the same quarter.
3. **Step 1's primary pair sits after both.** Its cohort was chosen in 2019 and
   measured from 2021 to 2022, when the top K's mean had stopped rising on
   submissions. Its slide of 0.0043 a week was therefore measured in the
   calmest part of the panel. The earlier, larger slides in step 1's
   secondary runs (up to 0.0060) coincide with the years the centre was
   climbing. How much of a cohort's slide is the centre outgrowing it, and
   how much is its own decline, differs by year and is not separated here.
4. **A stationary balance is the wrong frame for these four years.** Any
   account of the ladder from movement has to carry growth in the number of
   endpoints and in the level of the lower ranks. The theory note already
   says a growing population adds a dilution term; these measurements say the
   term is not small.
5. **The 2021 episode is a usable natural experiment for the top of the
   submissions ladder**, with a date that came from outside the data and a
   response by rank that is now measured in two frames. It is one event on one
   metric, and whether the response matches what a rank diffusion predicts
   has not been tested. A first look says the redistribution to the middle
   was immediate and not a slow relaxation.

## What this does not show

Why new communities arrive at this rate, why the lower ranks grow, or what the
recommendation change did mechanically. That "new" means created: it means
unseen since December 2018. Anything about the period after 2022 or about any
sealed panel. Whether the registered date is right: it is the caption of the
owner's chart and was not checked against Reddit's announcements.

## Limits

- The trend intervals come from a block bootstrap that runs short on
  persistent series: about 75–80% coverage of a nominal 90% in synthetic
  checks. Scores use an interval doubled about the estimate.
- Bands by current rank describe positions. The endpoints in them change.
- The share of the five names is selected on the window before the date.
- Half-change timing is defined against the end of the panel, 18 months after
  the date; a longer panel could move it.
- One registered date, one platform, and the outcome on submissions was known
  from the owner's chart before the run.

## Deviations

None from the design. The design itself was extended once before any run,
when the owner supplied the chart and its date.
