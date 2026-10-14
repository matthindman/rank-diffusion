# How fast can a change in drift by rank be detected?

_2026-10-14, Claude (Opus 5.5). Written before the runner touched a panel. The
owner: "Can we reliably detect when drift by rank changes? How long of a time
series do we need? If we can detect it quickly we are in good shape!"_

**Status: design. NOT RUN.** Exploratory, DEVELOPMENT data. Runner
`llm_fitting/mts_drift_detect.py`; tests `tests/test_mts_drift_detect.py`.

## Why it matters

The rate law (§2z-cd, §2z-cg) turns drift by rank into structural change, and
it forecasts well while the drift by rank holds still and badly when it
turns (comments). If a turn can be seen soon after it happens, the law
carries it to structure from then on. So the question is the delay between
a change in drift by rank and its detection, and how that depends on the
size of the change, the number of communities at that rank, and the window.

## Statistic

Weekly levels, smoothed to 4-week means. Rank strata as §2z-cd, from
(0, K/128] to (K, 2K]. For a window from week t to week t + τ, a cohort fixed
by permanent rank on the 26 weeks before t; each stratum's drift is the mean
change of its members' 4-week mean over the window, per week, minus the
cohort's, so platform-wide moves drop out. Also the head (ranks 1 to K/16
pooled) and the tilt (fitted bottom minus top, as §2z-cc). Sampling error from
200 endpoint bootstraps within strata, at start weeks every 4 weeks through
the panel; the median over start weeks is used.

**Detectable change.** Against a 26-week baseline, the smallest change in
drift found at z = 3 after τ weeks: MDE(τ) = 3 √(SE(τ)² + SE(26)²), for τ =
4, 8, 13, 26.

**What actually happens.** The head's drift and the tilt in successive
non-overlapping 26-week windows, with their errors.

**June 2021.** A cohort fixed on the 26 weeks before a 26-week baseline that
ends at the week of 21 June 2021; trailing windows of 4 and 8 weeks from 8
weeks before to 26 after; z of the head's drift against the baseline.
Detected when two consecutive windows have z ≤ −3. Both panels; comments as a
comparison.

Endpoint bootstraps treat communities as independent within a stratum. Real
shocks shared by related communities are counted as signal, which is what a
change in drift by rank is.

## What I have already seen

The slides of §2z-cc and §2z-cg (yearly and half-year), the daily share
series of §2z-ca around June 2021 (the top five fell from 13–14% to 10% of
submission karma within four days of 23 June), and the planted test here: a
head of about 80 endpoints with lasting variance 0.02 a week has a 4-week
detectable change of about 0.03 a week, and a planted change of that size was
found 10 weeks after with 8-week windows.

## Predictions

- **D1**: for the largest stratum, K to 2K (thousands of members), the change
  detectable after 13 weeks is under 0.0020 a week on both panels.
- **D2**: for the head, ranks 1 to K/16, it is above 0.0020 a week on both
  panels.
- **D3**: on comments, the change of the head's drift between successive
  26-week windows exceeds the head's 26-week detectable change in at least
  half of the consecutive pairs.
- **D4**: on submissions the June 2021 change at the head is detected within
  8 weeks with 4-week windows, and no window ending in the 8 weeks before has
  |z| ≥ 3. Least sure.

## Limits

One platform, two metrics, one dated event. The cohort is half a year old. A
4-week mean still carries passing swings. "Detected" at z = 3 per window, with
no correction for looking every week; the pre-event windows show the false
alarm rate for that.
