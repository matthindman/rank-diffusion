# Daily check: 28 June or 1 July 2021, and the short-delay pockets

_2026-10-14, Claude (Opus 5.5). Written before any statistic here was computed.
Asked for by the owner ("T9 is mounted, run the daily check")._

**Status: design. NOT RUN.** Exploratory and descriptive, DEVELOPMENT data.
Data only: no fit, no simulation, no gate evaluation.

## Question

§2z-bv found a step on submissions in the quarter beginning 28 June 2021: the
first rank's share of karma halves and the middle ranks step up. §2z-bz found
two candidate causes three days apart. Reddit moved all mobile users to a
personalised Home feed "starting on June 28". And the archive's files change
at 00:00 UTC on 1 July 2021 from scores re-collected about 20 days after
posting to scores re-collected about 360 days after. Weekly data cannot
separate them. Which date carries the step? Separately: do the four
short-delay pockets that §2z-bz lists show in our files?

## What I already know that bears on it

By late 2022 the archive's delay is back to 8–26 days (§2z-bz) and the first
rank's share is 1.2% (§2z-bv), against 8.3% in spring 2021. So a year-later
re-collection cannot explain the level of the fall; only the timing of the
step could be confounded with the boundary. I have seen no daily number.

## Data

`data/ssd/derived/reddit_daily_long.parquet` (submissions; the source of
`reddit_submissions_long`) and `reddit_comments_2018-12_2022-12_daily.parquet`
(comments; the source of `reddit_comments_ext`). Daily rows per subreddit, UTC
days, 2018-12-01 to 2022-12-31: `metric_value` (karma, as the panels use it)
and the count of items. No subreddit name is printed or written.

## Statistics

**Pocket screen, whole panel, both metrics.** Per day: platform karma divided
by the platform count of items. Its ratio to the median of the same weekday
in the four weeks before and the four weeks after. Days below 0.7 are listed,
all of them, and the ratio on each predicted date is reported whatever it is.

**The date, 3 May to 1 August 2021.** Two fixed sets of subreddits, chosen on
31 May to 27 June 2021 with 12 June left out: the five with the most karma
("top five"), and those ranked 201 to 1,000 by karma ("middle"). Per day, in
logs: each set's share of platform karma; each set's share of the platform
count of items; the top five's karma per item minus the rest's. Also the
share of karma at position 1 and at positions 201–1,000 of the day.

For a series x, d7(t) = x(t) − x(t − 7), which removes the weekday pattern.
Noise scale: 1.4826 times the median absolute deviation of d7 over 10 May to
20 June 2021, leaving out pairs that touch a pocket day.

- **Onset**: the first day from 21 June on which |d7| exceeds three noise
  scales with the same sign on that day and the next two.
- **Share in place before the boundary**: D = [mean x(28–30 June) − mean
  x(21–23 June)] / [mean x(5–7 July) − mean x(21–23 June)], Monday to
  Wednesday in each case. One means the change was complete before 1 July;
  zero means none of it was.

The d7 profile from 14 June to 20 July is printed for every series.

## Predictions

- **D1** (pockets, submissions): 29 and 30 April 2020 are below 0.2; 28 April
  2020 and 12 June 2021 are below 0.7. For about 29 May 2022 and the last
  week of August 2019 I expect at least one day below 0.85 each, less surely.
  Comments: no day below 0.7 in the whole panel.
- **D2** (the date): for the top five's log share of submission karma, the
  onset is on or before 30 June 2021. I expect 28 or 29 June.
- **D3**: D is at least one third for that series.
- **D4**: the top five's share of the count of submissions has no onset
  between 21 June and 14 July: the change is in votes per post.
- **D5**: the middle set's log share of karma has an onset of the opposite
  sign on or before 30 June.
- **D6** (comments): the top five's share of comment karma has no onset
  between 21 June and 14 July.

**How it is read.** Onset on 28–30 June with D of a third or more: the step
begins before the archive boundary, as Reddit's date says. Onset on 1 July
with d7 inside two noise scales on 28–30 June and D near zero: the step sits
on the archive boundary and §2z-bv's dating is not established. Anything
else is reported as it is.

If D1 fails on the April 2020 days, our files do not match what the public
API serves and §2z-bz's delay table does not describe them.

## Limits

A rollout "starting on" a date may ramp, and Reddit says experiments ran
before it. Reddit's day is Pacific time; ours is UTC. Sets chosen on June are
selected for being large in June. One event.

## Command and outputs

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/runs/2026-10-14_daily_date_check/daily_check.py

`output.txt`, `daily_series.csv`, a MODEL_STATUS entry and a ledger row.
