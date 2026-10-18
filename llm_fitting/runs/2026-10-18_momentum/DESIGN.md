# Is Reddit's momentum a mechanism or an artefact? Diagnostics, with expectations written before the run

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18. The owner asked for a
self-assessment before more referee feedback. This is the obvious gap it
found. Record: MODEL_STATUS §2z-de._

**What is known (§2z-dd).**
- On the top 5,000 Reddit submissions communities and the top 12,500
  comments communities, consecutive annual changes of 52-week block means
  correlate: Spearman +0.373 and +0.320, robust to outliers.
- On Facebook pages there is none (−0.035).
- No tested model reproduces it.

**The diagnostics** (`momentum_diagnostics.py`; development panels; no names
read).

| | diagnostic | measure | expectation |
|---|---|---|---|
| D1 | Timing | Spearman of consecutive *half-year* changes (26-week block means; cohort = the top K of the first block of each triple) for each triple of consecutive blocks, separately for triples ending before 1 July 2021 (the archive's capture-regime change) and after | momentum > +0.10 on both Reddit ladders in the triples that end before July 2021. The capture change is then not its cause |
| D2 | Community age (lifecycle) | annual triple 2020 → 2021 → 2022 (cohort = the top K in 2020); age = first week observed in the panel; "young" = first observed after week 26, "old" = observed in the first 4 weeks | momentum higher for young communities than for old, on both Reddit ladders |
| D3 | Size | the annual momentum of §2z-dd by stratum of block-0 rank (top K/16, K/16–K/4, K/4–K) | positive in every stratum (not confined to the body) |
| D4 | Survivorship | the annual momentum with communities absent from a block kept at that block's floor (the K-th level) instead of dropped | stays above +0.25 on both Reddit ladders |
| D5 | Facebook at half-year spacing | as D1, 13-week blocks | within ±0.10 of 0 |

**What each outcome would license.**
- **D1 and D2 hold:** a lifecycle or age-drift mechanism. The model would
  get drift by age, identified from the age profile of growth in train data.
- **D1 fails** (momentum only after July 2021): the archive artefact is the
  leading suspect, and the raw-archive audit (WD drive) comes first.

## Addendum R2: the aggregation null (expectations written before the run)

_Claude Opus 5.5, 2026-10-18, after the round-2 reviews
(`runs/2026-10-18_external_critiques_round2/`)._

**The objection** (gpt-6-astra R2 §4). Consecutive changes of
*block-averaged* log levels correlate even when weekly increments are
independent. For a random walk averaged over blocks of B weeks the Pearson
correlation is (B²−1)/(2(2B²+1)), so +0.2499 at B = 52 and a Gaussian
Spearman of about +0.24 (Working 1960). So zero is the wrong null for D1 and
for the annual statistic. The model-based comparisons of §2z-dd are already
passed through identical averaging and selection: the moving home gives
−0.01 on comments and about +0.04 on submissions. But they depend on how the
model splits variance between lasting and medium parts.

**The test, model-free** (`gapped_momentum.py`). Correlate two changes whose
increment supports do not overlap:
- change 1 = mean over window W2 minus mean over window W1, h weeks later;
- change 2 = mean over W4 minus mean over W3, with W3 starting g weeks after
  W2 ends.

For any process with independent increments, including increments whose
volatility differs by community, plus white noise, the expected correlation
is zero. A stationary short-memory part (the medium swing, half-life 3–7
weeks) makes it slightly negative. Only persistent drift, or a velocity that
lasts longer than the gap, makes it positive.

**Settings.**
- **Reddit** (both long panels):
  - windows of w = 13 weeks, changes over h = 52 weeks, gaps g = 0, 13, 26;
  - origins every 13 weeks from week 26 while the four windows fit;
  - cohort = the top K by mean level over weeks 0–25 (presence ≥ 0.9). That
    window is disjoint from every measurement window, so no window is
    selected on;
  - presence ≥ 0.9 in all four windows.
- **Facebook Era A:** w = 4, h = 26, g = 0, 4, 8; cohort from weeks 0–12.
- **Reported alongside, on the same cohort:** the adjacent-block statistic
  (consecutive 52-week block means, as §2z-dd; 13-week blocks on Facebook).

**Instrument check, run through the same code** (synthetic, N = 5,000,
T = 212):
- world (i): random walk with volatility varying by community, plus white
  noise, plus an OU swing (half-life 5 weeks);
- world (ii): world (i) plus a persistent drift by community.

**Expectations.**

| | expectation |
|---|---|
| I1 | world (i): gapped \|ρ\| ≤ 0.03 at every gap; adjacent-block ρ ≥ +0.15 |
| I2 | world (ii): gapped ρ ≥ +0.10 |
| G1 | Reddit, both ladders: gapped Spearman (mean over origins) ≥ +0.10 at g = 13 and g = 26. Momentum is then persistent growth, not averaging |
| G0 | the alternative: \|ρ\| ≤ 0.05 at g ≥ 13 on a ladder. The annual momentum is then mainly averaging plus noise, and §2z-dd/§2z-de's momentum claim is withdrawn for that ladder |
| F | Facebook: gapped ρ ≤ +0.05 at every gap |

Prior: about 60% that G1 holds on submissions and 55% on comments. A decay
of ρ with g estimates how long the persistence lasts. A plateau means
community-specific drift; a decay means a velocity with a finite half-life.
