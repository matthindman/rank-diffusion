# Depth tests for the chosen headline: expectations written before the runs

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18, following `PROMPT.md`.
Committed before either test is computed._

## Why these two tests

The first headline candidate ("leaders lose ground; turnover sustains
inequality") was killed by its method check before any data was read
(`planted_within.py`):
- with no drift at all, the lagged within estimator reads −0.08 a year;
- a negative within term balanced by displacement is the generic condition
  for a stationary power law.

The remaining candidate is:

> How fast leaders are displaced at every rank follows from how much
> communities there move and how crowded the ranks are. It does so ahead of
> time, and only if movement is growth in levels.

The hostile expert attacks it twice:
- "it is an identity in period";
- "any rank model would pass too".

## O: out-of-time crossings (development data, post hoc design, run once)

**Set-up.**
- 13-week blocks (16 blocks; §2z-cu).
- Volatility σ² by the variance route, on the block-0 cohort, over blocks
  1, 3 and 5: weeks 13–91, 2019 to mid-2020.
- Slope S at the origin block a of each crossing window: known when the
  window opens.
- Crossings: the spacing difference 2–4 from starts a = 6 … 11, blocks
  6 … 15. These do not overlap the volatility window.
- Ratio: R̃ pooled over those starts, over σ²_early / (2 S_a).

| ladder | source |
|---|---|
| comments | the saved run `boundary__reddit_comments_ext__block13__K12500.json` |
| submissions | the same analysis, run once with 13-week blocks and K = 5,000 |

| | expectation |
|---|---|
| O1 | comments, the geometric mean over K/8 … K, lies in [0.85, 1.18] |
| O2 | submissions, the same over K/4 … K, lies in [0.85, 1.18] |
| O3 | on both, the out-of-time ratio exceeds the in-period ratio of the same starts. Lasting variance rose after mid-2021 (§2z-co), so early volatility under-predicts later crossings |

My own odds, written down:
- O1 about even;
- O2 below even, since submissions' variance rose 1.36–1.55×;
- O3 likely.

## V: calibrated rank-model rivals (planted; identical estimators)

**World.** The world of the power study (N 25,600, K 1,600, S 1.5, 26-week
blocks, 208 weeks). Levels are the fixed ladder's value at the name's rank,
plus noise of sd 0.05.

**Calibration.** Each rival is tuned so that a one-year retention of the top
K/4 is 0.80. This is the data's value for the top K/4 on both Reddit ladders,
0.79–0.84 (§2z-cu).

| | rival | expectation |
|---|---|---|
| V1 | uniform rank displacement (Iñiguez-type): each week a name moves to a random rank with probability p | pooled ratio over K/8 … K **outside** [0.85, 1.18] (≥ 1.5) |
| V2 | log-rank diffusion with the ladder fixed: each week every name's log rank moves by N(0, s²), names are re-sorted, and the ladder is re-imposed | ratio **inside** [0.85, 1.18]. The test would then *not* separate rank-space diffusion with a fixed ladder from level diffusion |
| V3 | neighbour swaps (as §2z-cv), uncalibrated | erratic, outside the band at most cut-offs |

**What V2 decides.** If V2 passes, the claim narrows. It becomes "excludes
displacement to random ranks and local swaps", not "excludes rank exchange".
What would then separate level diffusion from fixed-ladder rank diffusion is
whether the ladder itself moves, as it does in the data (§2z-cw, §2z-da).

## Added before its run: Z, the size–variance exponent measured as the firm literature measures it

**Why.** The candidate headline "size does not protect" rests on β. That is
the exponent in sd(growth) ∝ size^(−β). The value in §2z-da is read from the
crossing-implied lasting variance, which is model-based. Stanley et al.
(1996) and Riccaboni et al. (2008) measure the *raw* spread of annual log
growth against size, and find β ≈ 0.15–0.2 for firms. A fair comparison
needs the same statistic.

**Set-up** (`size_variance.py`; development panels).
- Cohorts by permanent rank over the first block. Strata are octaves of
  rank, K/64 … K.
- Size is the stratum's mean block-mean log activity.
- Raw growth is the change of block-mean log level over one year:
  - Reddit, 52-week blocks: block 1 → block 2, and block 2 → block 3;
  - Facebook pages, 13-week blocks: block 1 → block 5.
- The lasting version (Reddit only) is Var(Δ over two years) − Var(Δ over
  one year), each from block 1.
- Members present in ≥ 90% of the weeks of every block used.
- β is −(slope of log sd on size), by least squares over strata.
- Intervals: 5–95%, from 400 bootstrap draws over names within strata.

| | expectation |
|---|---|
| Z1 | raw β < 0.10 on all three ladders |
| Z2 | lasting β < 0.10 on both Reddit ladders |
| Z3 | the upper 95% bound of raw β is below 0.15, the low end of the firm range, on at least two ladders |
