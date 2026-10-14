# Review of an external result: a count-only test of fixed volatility types

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-14. The result (GPT-6 pro)
was supplied by the owner and is in `SOURCE.md`. Synthetic simulation only;
no data file opened. Record entry: MODEL_STATUS §2z-cl._

## What it is

The result tests whether each community's volatility is a fixed trait (β = 0)
or responds to the community's own level, with variance α_i + β tanh X. It
uses only that community's daily counts. The idea:

- A second difference over three equal blocks of a year, weights (1, −2, 1)/L,
  cancels the community's level and its own straight-line trend.
- If volatility is fixed, that contrast is Gaussian and symmetric **for every
  community separately**, whatever its type, starting size or passing noise.
- If volatility responds to level, an early rise predicts calmer movement
  later. That skews the contrast; it is a third-moment, "leverage" signature.
- The transform Γ(c+1)/Γ(c+1−iτ) of a Poisson count estimates exp(iτ log λ)
  with a bounded error and handles zeros. Its products give the sine
  (imaginary) parts of the contrast's characteristic function at two
  frequencies; score B combines them so that a small common shift cancels.
- A betting (e-value) mixture over 32 fixed stakes gives an exact level-0.05
  test. The unknown common seasonal phase is profiled over its allowed range:
  the test must reject at every phase.

It claims validity (size and coverage) under its whole parameter box and says
plainly that power is not established.

## Parameter box, as inferred from the text

The question was not supplied. Inferred:
- variance α_i in [0.001, 0.0015] a day, β in [−0.0005, 0.0005];
- passing OU noise with variance up to 0.09 and rate κ in [1/30, 1/7] a day;
- the community's own drift up to 0.001 a day in size; starting level at least −1;
- common annual seasonality with amplitudes up to 0.0003;
- daily counts Poisson with intensity 100 ∫ exp(X+Z) over the day.

## Checked

- **Identity (11) and bound (12).** Checked by direct summation at λ = 0.01,
  1, 10 and 100 and four values of τ: the error never exceeds the bound. |f_τ|
  is nonincreasing, with its maximum M_τ at zero.
- **The filter.** Its moments hold. ∫k² = L, and the kernel of the third
  moment is ∫k²K = −0.4 L².
- **(44), the filtered OU variance.** It matches a direct double integral to
  five digits; v F = 0.16004 at L = 121, κ = 1/30.
- **Not checked:** the daily-integration bounds (22)–(26) step by step, the
  envelope (33)–(35), and the interval certificate (its archive was not supplied).

## Simulation of its own model (`sim_power.py`, `sim_power.json`, `output.txt`)

Set-up:
- Each scenario is 20 independent sets of 10,000 communities over 363 days,
  with 8 steps a day.
- Types, drifts and starts are spread over the box; seasonality is zero (the
  test still profiles over it).
- "Heavy" passing noise is v = 0.09 at κ = 1/30 (the result's own example);
  "quiet" is v = 0.02 at κ = 1/7.
- Rejection rates are over 20 sets of 10,000 names and over 5 sets of 40,000
  names made from them.

| scenario | mean of B (± se) | rejects at 10,000 | at 10,000, phase known | at 40,000 |
|---|---|---|---|---|
| null, heavy | −0.0024 ± 0.0013 | 0/20 | 0/20 | 0/5 |
| β = −0.0004, heavy | −0.0066 ± 0.0013 | 0/20 | 0/20 | 0/5 |
| β = −0.0001, heavy | −0.0037 ± 0.0013 | 0/20 | 0/20 | 0/5 |
| null, quiet | −0.0003 ± 0.0008 | 0/20 | 0/20 | 0/5 |
| β = −0.0004, quiet | −0.0131 ± 0.0008 | 7/20 | 9/20 | 5/5 |
| β = −0.0001, quiet | −0.0023 ± 0.0008 | 0/20 | 0/20 | 0/5 |
| *fixed volatility, outside the result's model:* | | | | |
| drift by level −0.0003 tanh X, heavy / quiet | +0.0013 / −0.0015 | 0/20, 0/20 | 0/20, 0/20 | 0/5, 0/5 |
| drift by level −0.00005 X, heavy | +0.0001 | 0/20 | 0/20 | 0/5 |
| short spikes (decay 1/7 a day), heavy / quiet | +0.0014 / +0.0004 | 0/20, 0/20 | 0/20, 0/20 | 0/5, 0/5 |
| slow spikes (decay 1/60 a day), quiet | **+0.0072 ± 0.0008** | 1/20 | 1/20 | **1/5** |
| concave life-cycle arcs on 30% of names, quiet | +0.0004; **A −0.0275** | 0/20 | **20/20** | 0/5 |

Signal in B against the null, and the names an ideal one-sided test with the
phase known would need for 80% power:

| | signal | ideal test | after the bias allowance 0.0018 |
|---|---|---|---|
| β = −0.0004, heavy | 0.0042 ± 0.0018 | about 120,000 | about 370,000 |
| β = −0.0001, heavy | 0.0013 ± 0.0018 | about 1.2 million | none |
| β = −0.0004, quiet | 0.0128 ± 0.0011 | about 4,400 | about 6,000 |
| β = −0.0001, quiet | 0.0020 ± 0.0011 | about 180,000 | tens of millions |

Phase sensitivity: moving the phase to the edge of its range shifts the mean
of A by 0.038–0.040. The mean of B shifts by 0.020–0.024 in heavy noise and
0.001–0.003 in quiet noise; B cancels a shift only when the contrast's
variance is near 0.17.

## Findings

1. **Validity holds where it is claimed.** Every null inside the model stays
   within the bias bounds, and none rejects.
2. **Power exists only for a strong response in quiet noise.** At β = −0.0004
   with quiet noise it rejects 7 of 20 sets of 10,000 and 5 of 5 of 40,000.
   With the result's own example noise, it rejects none even at 40,000.
3. **Our Reddit gradient is weaker than the alternative tested.**
   - Measured: lasting variance by stratum falls with level by 0.8–1.2 × 10⁻⁴ a
     day per log unit on comments and 1.8–3.6 × 10⁻⁴ on submissions (§2z-cd
     cells). That is a fifth to a third of β = −0.0004 on comments, and a half to
     nine tenths on submissions. This is the case only if *all* of the gradient
     were a response to level; composition makes it smaller.
   - Consequence: at comments' size the test has no power at 40,000
     communities in either noise level. The ideal test would need 10⁵ to 10⁶
     communities. Our panels have 12,500–25,000, over four years.
4. **Two features of our panels break it.**
   - *Slow passing swings that fade over weeks to months.* The record has them:
     a fitted slow component with a 10-week half-life on comments (§2p), and
     weekly positions moving half again as much as block means at a year
     (§2z-bw). Slow spikes give a false signal over half the size of
     the strongest quiet alternative's. They reject in 1 of 5 sets of 40,000, and
     the rate grows with the number of names.
   - *Life-cycle arcs.* The record documents rise-then-fall arcs on comments
     (§2p). A curve inside the year passes straight through the second
     difference. Here only the seasonal profile absorbs it; with the phase known,
     the test rejects fixed volatility in every set. Arcs larger than these, with
     a mean shift past the profile's range (0.044), would, I expect, beat it too;
     not run.
   - In both cases the profile that rescues validity is the same one that removes
     power in heavy noise.
   - Drift that depends on level, even six times our measured tilt, and short
     spikes do not break it.
5. **Karma is not a count.** The Poisson part of the construction does not
   apply to our metric. At top-K volumes the Poisson error is negligible anyway,
   but karma's own non-Gaussian noise would need its own bound.
6. **The sign is not identified,** as the result says.

## What to take from it

The design principle is the right one for the within-community test of
§2z-cj and §2z-ck: a statistic whose expectation is zero **for every community
separately** under fixed volatility. Such a null holds whatever a community's
type, its association with size, or its age. My own extension, from the same
argument: it also holds under any deterministic change of volatility with age,
since the contrast stays Gaussian. So such a test separates "responds to its
own level" from all three age-related rivals of §2z-ck at once.

The third-moment route is too weak for our panels, and its null is broken by
features the panels have. Two routes I expect to carry more signal; both are
untested, and V5 should compare them:
- **A second-moment route:** does a community's later lasting variance change
  after its own lasting move, against communities that did not move? This needs
  a per-community version of the variance route's noise cancellation (§2z-cc),
  because observation noise also falls with size.
- **A contrast that removes curvature as well as slope**, such as a third
  difference over four blocks, against arcs, at a further cost in power.
