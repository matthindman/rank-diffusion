# Forecasting a year of structural change from a quarter of movement, across Reddit's June 2021 Home-feed change: design and predictions (before any run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-15. Committed with
`llm_fitting/mts_intervention.py` and `tests/test_mts_intervention.py` before
any panel is read with this design._

## Why

§2z-co found that, where nothing changes, the rate law forecasts structure no
better than repeating the ladder's own last change. A reviewer will ask what
the law is then good for. Where drift by rank has just changed, as after a
platform's algorithm change, the ladder's past trend no longer applies. The
law reads the new drift from the most recent movement and turns it into the
coming change of shape, so that is where it should beat the trend. This tests
whether a quarter of movement after an intervention forecasts the next year's
restructuring.

## Method

- **Blocks.** 13-week blocks. At each origin, the end of block o, the
  persisting set's change of shape over the next 52 weeks (o → o+4) is
  forecast at K/64 … K/2.
- **Forecasts**, from data up to the origin:

  | | forecast |
  |---|---|
  | N | no change |
  | T | the trend: the change of shape over the last 52 weeks |
  | LQ | the law with the last quarter's slides: 52 × [c(S_o) − s̄(o−1 → o)], cohort fixed on o−2 |
  | LY | the law with the last year's slides |
  | LQ, early σ² | LQ with σ² from 2019–2020 only |

- **The variance.** c uses σ² from the variance route on 26-week blocks,
  pooled over 2019–2021. For origins before 2021 H2 that is a look-ahead;
  the LQ variant with σ² from 2019–2020 checks it.
- **Origins.** All with five blocks behind them and four ahead.
- **The change.** Taken from the week of 2021-06-28 (§2z-bz, §2z-ca).
  - An origin is *after the change* if its whole last quarter, block o−1,
    starts on or after that date.
  - Its trend *straddles the change* if the change falls between the start
    of block o−4 and the end of block o.

**Planted check (passes).** Walks in which the top K/8 starts losing 3C a week
at week 104. At origins after the change, at K/16 to K/4, LQ's rms error is
under 0.7 of the trend's and 0.6 of no change. At K/64 and K/32 the
boundary strata hold 12–50 members, and a quarter's slide there is noise. The
panels' head strata are of similar size on submissions (31–145), so scoring
is at K/16 … K/2 and the head is reported apart.

## Predictions (scored at K/16 … K/2)

| | prediction | passes if |
|---|---|---|
| J1 | submissions: after the change, the last quarter's movement forecasts the next year better than trend and no change | over origins after the change, rms(LQ) < rms(T) and rms(LQ) < rms(N) |
| J2 | submissions: where nothing changed, the law is no better and no worse than trend | over origins whose last quarter is before the change and whose trend does not straddle it, rms(LQ)/rms(T) in [0.75, 1.25] |
| J3 | comments: the head's broad gain of mid-2021 (§2z-cr) is not forecastable | over all origins, rms(N) is the lowest of N, T, LQ, LY |

**What it means.** A J1 pass says that after an algorithm change, a quarter of
measured movement forecasts the year's restructuring, where extrapolating the
ladder fails. That is the law's practical use and a policy-relevant result.
It rests on one event and a handful of origins, which are correlated, and is
reported as such.

## Not in this design

Facebook. Sealed data. Fitting anything.
