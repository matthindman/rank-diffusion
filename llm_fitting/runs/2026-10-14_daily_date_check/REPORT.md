# Daily check: 28 June or 1 July 2021, and the short-delay pockets — report

_2026-10-14, Claude (Opus 5.5). Design, predictions and runner committed at
2be99df before the run._

**Status: RAN-OK, one run.** Exploratory and descriptive, DEVELOPMENT data.
No fit, no simulation, no gate evaluation. No subreddit name was printed or
written. Nothing was changed after a number was seen. Full output:
`output.txt`; daily series: `daily_series.csv`.

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/runs/2026-10-14_daily_date_check/daily_check.py

## The answer

**Neither date carries the step, and the archive boundary is not its cause.**
On submissions the top five's share of karma starts to fall on 23–24 June
2021, a week before the archive's files change on 1 July and four to five
days before the date Reddit announced. Nothing happens at the boundary
itself: 30 June is 8.3%, 1 July is 7.6%, and the share then rises. Reddit
announced the change on 22 June and described slow rollouts.

The fall is real and fast, and it is not one clean step:

| week beginning | top five, share of karma | first position | middle (ranks 201–1,000 before the date) | top five, share of posts | top five, karma per post over the rest's |
|---|---|---|---|---|---|
| 3 May to 14 June (seven weeks) | 13.4–14.4% | 8.2–8.5% | 20.5–20.9% | 0.93–1.10% | 14.2–16.6 |
| 21 June | 11.2% | 6.3% | 22.1% | 0.96% | 13.1 |
| 28 June | 9.4% | 4.9% | 22.8% | 1.00% | 10.2 |
| 5 July | 11.5% | 6.9% | 21.9% | 1.05% | 12.2 |
| 12 July | 10.6% | 6.0% | 22.2% | 1.01% | 11.6 |
| 19 July | 9.5% | 5.3% | 22.8% | 0.85% | 12.3 |
| 26 July | 9.3% | 5.2% | 22.5% | 0.89% | 11.3 |

- From 3 May–20 June to 19 July–1 August the top five go from 13.7% to 9.4%
  of karma (−31%) and the first position from 8.3% to 5.2% (−37%); the middle
  set goes from 20.7% to 22.6% (+9%).
- The lowest days are 29 June to 1 July (8.6%, 8.3%, 7.6%). On 3–7 July the
  share is back near 11%, and it declines again from mid-July. Reddit's
  follow-up post saying the new feed was live for everyone is dated 13 July.
  A rollout that was advanced, eased and completed would look like this; I
  have no source that says that is what happened.
- **The change is in votes per post, not in posting.** The top five's share
  of the number of submissions stays near 1%, while their karma per post
  falls from about 15 to about 12 times the rest's.
- **Comments show nothing** at these dates: the top five hold 13–15% of
  comment karma throughout, with no onset on any series.

## The pockets

| day | karma per submission, against the same weekday in the eight weeks around it |
|---|---|
| 28 April 2020 | 0.57 |
| 29 April 2020 | 0.015 (1.15 karma per submission) |
| 30 April 2020 | 0.015 (1.14) |
| 12 June 2021 | 0.63 |

These four are the only days below 0.7 in 1,492. The two other pockets the
external review listed do not reach that level in our files: 0.90 at the
lowest around 29 May 2022 and 0.90 in the last week of August 2019. Comments
have no day below 0.7. So the delay table of §2z-bz describes our files where
it matters most, and **the week of 27 April 2020 in the submissions panel is
damaged**: two days carry scores of about 1 and a third is short by 43%.

One more thing the pocket shows: on 12 June 2021, with scores read within
hours, the top five's share is 16.6% and the first position's 10.4%, both
high. Reading scores early inflates the top's share. That is the opposite
direction from the worry in §2z-bz.

## Predictions scored

| | | result |
|---|---|---|
| D1 | 29 and 30 April 2020 below 0.2; 28 April 2020 and 12 June 2021 below 0.7; no comments day below 0.7 | PASS |
| D1, less sure | at least one day below 0.85 around 29 May 2022 and in the last week of August 2019 | FAIL (0.90, 0.90) |
| D2 | onset for the top five's karma share on or before 30 June | PASS (24 June; I expected 28 or 29) |
| D3 | at least a third of the change in place on 28–30 June | PASS, with an overshoot: the change on 28–30 June is 2.8 times the change on 5–7 July |
| D4 | no onset in the top five's share of the number of submissions | PASS |
| D5 | onset of opposite sign in the middle set's karma share on or before 30 June | FAIL by the rule: 3.5, 2.9, 3.6, 2.4, 3.3 noise scales on 24–28 June, never three days in a row above 3. The share at positions 201–1,000 of the day does have its onset on 24 June |
| D6 | no onset on comments | PASS |

## What it changes

- §2z-bz's open question is closed for timing: the step precedes the archive
  boundary and has no discontinuity at it.
- §2z-bv dated the step to the quarter beginning 28 June. By day it begins
  23–24 June, is deepest at the turn of the month and partly reverses in the
  first week of July; the quarterly means of §2z-bv (8.3% to 4.3% for the
  first position) include further decline later in the quarter. "Within a
  quarter" stands; "at the date" should read "within days of the
  announcement".
- The submissions panel has one damaged week that no analysis so far has
  screened. In shares and ranks it should matter little, since every
  subreddit is hit; in levels that week is low by about a third.

## Limits

One event. The top five and the middle set are chosen on four weeks of June
and are selected for being large then. The noise scale comes from six quiet
weeks. Days are UTC. This check times the step; it does not show what Reddit
did on which day, and it cannot rule out an archive effect of some other
kind inside June, though the two collection batches the external review
found there change at about 20 June, not 24 June.
