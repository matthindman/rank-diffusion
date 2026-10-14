# How fast can a change in drift by rank be detected? Report

_2026-10-14, Claude (Opus 5.5). Design, predictions, runner and tests committed
at 7206cf2 before the run (that commit went in with one test failing on an
index; fixed at 2097e45; the runner was unaffected)._

**Status: RAN-OK.** Exploratory, DEVELOPMENT data. Tables: `report_tables.md`;
cells in `cells/`. A post hoc scan of the whole panel, written after the
scored results were seen: `scan_post_hoc.py`, `scan_post_hoc_output.txt`.

## The short answer

**A change in drift by rank the size of the slide is detectable within weeks
for the large strata and within months for the head. But on comments drift by
rank changes all the time, in an annual pattern, so "detect a change" has to
mean "detect a departure from the seasonal pattern", and that needs two or
three years of history first.**

## How big a change can be seen, and when

Smallest change in a stratum's drift detectable at z = 3 against a 26-week
baseline, per 1,000 weeks (for scale: the slide is about 4.3):

| | members | after 4 weeks | after 13 weeks | after 26 weeks |
|---|---|---|---|---|
| comments, top K/16 (ranks 1–781) | about 700 | 7.8 | 4.4 | 3.4 |
| comments, ranks 1,562–3,125 | 1,475 | 6.4 | 3.7 | 2.8 |
| comments, ranks 12,500–25,000 | 9,590 | 2.6 | 1.4 | 1.0 |
| submissions, top K/16 (ranks 1–312) | about 290 | 8.9 | 5.9 | 5.2 |
| submissions, ranks 1–39 | 35 | 15.1 | 10.5 | 8.9 |
| submissions, ranks 5,000–10,000 | 4,170 | 3.1 | 1.8 | 1.4 |

The detectable change falls with the number of communities, roughly as one
over its square root, and more slowly with the window, because the baseline's
own error does not shrink. The head of either ladder needs a change larger
than the slide itself to be seen within a quarter.

## What actually changes, and how fast it shows

**June 2021.** Within one to two weeks of the feed change the trailing 4-week
windows depart from the baseline, but not the way I predicted. On submissions
the very top (ranks 1–39) falls (z −2.4 at two weeks), ranks 156 to 1,250 rise
(z +2 to +3) and the bottom of the top 2K falls (z −2.8). That is the pattern
of §2z-ca: the top lost, the middle gained. I predicted the pooled head would
fall; it is dominated by ranks 78–312, which rose. On comments every stratum
above rank 6,250 rises against the bottom one from the first week (z +4 to +8;
the bottom stratum z −9 to −12).

**But the summers repeat.** Scanning the whole panel every 4 weeks (post hoc):

| | windows | head departs (|z| ≥ 3) | tilt departs |
|---|---|---|---|
| comments, 4-week windows | 38 | 42% | 53% |
| comments, 8-week windows | 38 | 55% | 68% |
| submissions, 4-week windows | 37 | 5% | 38% |
| submissions, 8-week windows | 37 | 14% | 54% |

On comments the largest departures of the head come every summer: z +7 and +6
in May–June 2020, +10 in July 2021, +9 in July 2022; and the other way in
winter (−8 in January 2022). The comments signal at June 2021 is the size of
the other summers' and cannot be told from them with this statistic. On
submissions the head departs rarely, near the 5% a 4-week z = 3 rule gives by
chance, and the June 2021 departure (z +2 to +3) is also about the size of the
other summers'. The clear signature of June 2021 remains the daily share of
the first few positions (§2z-ca), which no summer repeats.

## Predictions scored

| | | |
|---|---|---|
| D1 | the largest stratum detects under 2.0 per 1,000 weeks after 13 weeks, both panels | PASS (1.4, 1.8) |
| D2 | the head needs more than 2.0 after 13 weeks, both panels | PASS (4.4, 5.9) |
| D3 | on comments, the head's drift changes between successive windows by more than its 26-week detectable change in at least half the pairs | PASS (two of three) |
| D4 | June 2021 on submissions detected at the head as a fall (z ≤ −3 twice) within 8 weeks, no false alarm before | FAIL: no fall at the pooled head; the departures are of the other sign and at z 2–3; no window before had |z| ≥ 3 |

## What it means

1. **Large changes are seen in weeks, small ones at the head in months.** The
   rate law can be updated within a quarter for a broad change in drift by
   rank; the very top needs positional daily statistics, which saw June 2021
   within a day.
2. **Drift by rank has an annual cycle, at least on comments.** Small
   communities gain on the head in winter and lose in summer, each year. A
   cycle is forecastable. It is also a candidate for why forecasts of the
   comments ladder failed (§2z-ce, §2z-cg). The cycle may be partly the
   endpoint-specific annual cycle of §2z-aw seen through a cohort selected a
   year earlier; that is not separated here.
3. **So the time series needed is two-part**: two or three years to learn the
   seasonal pattern of drift by rank, then weeks to months after a change,
   by its size and the number of communities at that rank, to see a
   departure from it.

## Limits

Endpoint bootstraps ignore dependence between communities, so the rates of
departure are upper bounds on true changes and lower bounds on noise only if
that dependence is small. The scan and the seasonal reading are post hoc. One
platform, two metrics, one dated event.
