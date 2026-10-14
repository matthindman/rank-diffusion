# A second estimator of lasting variance by rank: design and predictions (before any run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-15. Committed with
`llm_fitting/mts_variogram.py` and `tests/test_mts_variogram.py` before any
panel is read. Follows §2z-cm._

**Why.** On submissions §2z-cm could not test the boundary theory at the head:
the variance route's estimates in strata of 31–145 communities were erratic.
The boundary result on comments also rests on one variance estimator. A
second estimator, built on a different principle, tests both.

**Estimator.** For the cohort fixed on block 0 by permanent rank:
- rolling 4-week means of weekly levels;
- the cross-sectional variance of each member's change over lags of 52 and 104
  weeks, averaged over every weekly origin from block 1 to the end;
- σ² = (V(104) − V(52))/52. Lags a year apart leave the annual cycle out, and
  swings that fade within months leave no slope between them.

The cut-off value takes the two strata meeting there, weighted by members, as
in §2z-cm. c = σ²/(2S), with S and the crossings R̃ the pooled values of
§2z-cm.

**Planted worlds, three findings:**
- *Accuracy.* With 10-week swings and weekly noise, the estimate is within 7%
  in strata of ≥ 300 members.
- *Precision.* It is only about √3 better than the variance route, since each
  member has only a few independent year-long changes. Strata under about 200
  members stay noisy (−23% at 177), so this cannot rescue the submissions
  head; I had expected otherwise.
- *Eras.* A shorter-lag slope on 78-week windows, comparing the eras before
  and after June 2021, is biased upward by unfaded swings and is noisy below
  several hundred members. It is reported, not scored.

**Predictions.**

| | prediction | passes if |
|---|---|---|
| V1 | the two estimators agree where both are precise | in strata with ≥ 1,000 members, variogram / variance route within [0.75, 1.25] in every stratum, on each panel |
| V3 | the boundary theory holds with the second estimator | R̃ / c in [0.75, 1.33] at ≥ 5 of 7 cut-offs, on each panel |

V3 on submissions is expected to be decided in the body; at the head both
estimators are noisy.

**Not in this design.** Any change to §2z-cm's scores. Sealed data.
