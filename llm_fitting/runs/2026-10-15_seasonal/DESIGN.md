# Seasonal forecasts of the ladder's half-year changes: design and predictions (before any run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-15. Committed with
`llm_fitting/mts_seasonal.py` and `tests/test_mts_seasonal.py` before any
panel is read._

## Why

Structure can be forecast where drift by rank persists (submissions) and not
where it turns (comments; §2z-ce, §2z-cg). On comments, drift by rank has an
annual cycle: small communities gain on the head in winter and lose in summer
(§2z-ci). If that cycle is the reason, the same season a year earlier should
forecast each half-year's change of the comments ladder, where last half-year
fails. A pass would make the paper's claim whole: structure is forecastable
to the extent that drift by rank is, and drift by rank is forecastable once
its season is known.

## Method

**Data.** 26-week blocks beginning 2018-12-31. A change between two
consecutive block means runs from mid-block to mid-block, roughly April to
October (the summer half) or October to April (the winter half).

**What is forecast.** The persisting set's change of shape over each
half-year at K/64 … K/2. Four targets per panel, the half-years from blocks
3→4, 4→5, 5→6 and 6→7: two winters and two summers, one of them containing
June 2021.

**Forecasts**, made from blocks before the target only:

| | forecast |
|---|---|
| F0 | no change |
| F1 | the rate law with last half-year's slides: c(S_a) − s̄(a−1 → a) |
| F2 | the rate law with the slides of the same half-year a year earlier: c(S_a) − s̄(a−2 → a−1) |
| E1 | last half-year's change of shape |
| E2 | the same half-year's change of shape a year earlier |

- c = σ²/(2S_a), with σ² from the variance route (cohort of block 0, blocks
  1, 3, 5) and the slope at the start block.
- Slides use cohorts fixed by permanent rank on the block before the pair they
  are measured over, as in §2z-cd.
- Scored by rms error over the cells where all five are defined.

**Planted worlds (pass).** A head whose drift alternates by season, switching
at block midpoints: F2 3.55, F0 4.02, F1 6.92 per 1,000 weeks, and E2 beats
E1. A head pressed down throughout: F1 and F2 beat F0 by more than half.

## Predictions

| | prediction | passes if |
|---|---|---|
| Z1 | comments: the same season's slides forecast the half-year's change | rms(F2) < rms(F0) and rms(F2) < rms(F1), pooled over 4 targets × 6 cut-offs |
| Z2 | comments: the ladder's own change repeats by season | rms(E2) < rms(F0) and rms(E2) < rms(E1) |
| Z3 | submissions: drift by rank persists, so either lag forecasts | rms(F1) < rms(F0) and rms(F2) < rms(F0) |

**What a result means.**
- *Z1 pass.* The comments failure of §2z-ce and §2z-cg is the annual cycle,
  and a seasonal baseline repairs it.
- *Z1 fail with Z2 pass.* The cycle is in the ladder but the law's seasonal
  inputs do not carry it.
- *Both fail.* The comments ladder's half-year changes are not forecastable
  on this record.

Four targets per panel is a small test: cells within a target are
correlated, and one target holds the June 2021 shock.

## Not in this design

Year-long changes (§2z-cg). Facebook. Sealed data.
