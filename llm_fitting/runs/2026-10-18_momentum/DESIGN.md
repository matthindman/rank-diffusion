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
