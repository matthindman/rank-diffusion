# Ex ante forecasts of lock-in at the top and the upper middle: design and predictions (before any run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-15. Committed with
`llm_fitting/mts_lockin.py` and `tests/test_mts_lockin.py` before any panel
is read with this design. The owner's request: "Flesh out in a rigorous way
what these models provide in terms of ex ante prediction for the rate at
which the top / 'upper middle class' endpoints turn over. Validate these
predictions using the historical data across all three platforms."_

## The question

Platforms tend to say there is little lock-in at the top, citing examples or
early history. A forecast of lock-in should say, before the fact, how likely
each member of today's top k is to still be in the top k after a given time.
It should be scored afterwards against what happened. The model's claim is
that this follows from measured movement alone: no retention outcomes are
used.

## The forecast

Retention after h blocks is end-point retention: the member is in the top k
at block a+h, whatever happened in between. At origin a, using only blocks
a−2, a−1 and a:

**Movement moments.** For each stratum of current rank at a−2:
- the mean and variance of members' change of level against the top-K mean
  over one and two blocks (μ₁, μ₂, v₁, v₂), and the share still present
  (q₁, q₂);
- the diffusion structure extends these to any horizon:
  μ(h) = μ₁ + (μ₂ − μ₁)(h − 1) and v(h) = v₁ + (v₂ − v₁)(h − 1);
- the one-time constant is passing swings falling back, minus momentum
  (§2z-cm).

**The boundary.** The level of rank k against the top-K mean drifts at its
rate over the last two blocks.

**The forecast for member i:**

  p_i(h) = q_s(h) · Φ((g_i + μ_s(h) − β_k h) / √v_s(h))

where g_i is its gap above the rank-k level at a and s the stratum of its
current rank.

**Baselines:**
- *Constant turnover:* the share of the top k retained over one and two
  blocks in the training window, extrapolated geometrically, the same for
  every member. This is the "X% turn over each year" claim.
- *Logistic:* a regression of retention on log rank and log horizon, fitted
  to the training window's outcomes. It uses outcomes, which the model does
  not.

## Ladders and settings

| ladder | blocks | presence | K | origins | longest horizon |
|---|---|---|---|---|---|
| Reddit comments | eight of 26 weeks | 90% | 12,500 | 5 | 130 weeks |
| Reddit submissions | eight of 26 weeks | 90% | 5,000 | 5 | 130 weeks |
| Facebook pages, Era A | six of 13 weeks | 90% | 3,500 | 3 | 39 weeks |
| Instagram, apparatus (§2z-cx) | six of 7 weeks, weeks 9–52 of 2023 | 50% | 10,000 | 3 | 21 weeks |

- Ranks are by current block level.
- Top sets k = K/64, K/16 and K/4: the top, the upper middle and the middle.

**Planted check (passes).** Diffusion worlds with passing swings or a
pressed head:
- the model's per-member Brier score is 15–25% below constant turnover's;
- it is about equal to the fitted logistic's, without using outcomes;
- the aggregate retention errors are 0.01–0.11.

## Predictions

| | prediction | passes if |
|---|---|---|
| T1 | the forecast discriminates who stays | on each of the four ladders, the pooled per-member Brier score is below constant turnover's |
| T2 | the forecast is calibrated in aggregate | the forecast retention share is within ±0.07 of observed in ≥ 75% of all (ladder, origin, k, h) cells |
| T3 | it extrapolates in horizon better than constant turnover | at h ≥ 3 blocks, the mean absolute error of the model's retention share is ≤ the geometric baseline's |

**Reported, not scored:**
- the logistic baseline;
- calibration by decile;
- excess lock-in at the top: observed minus forecast retention of the top
  K/64, by ladder and horizon. Positive means the top holds on longer than
  its measured movement implies.

## Not in this design

Sealed data; fitting anything to retention outcomes.
