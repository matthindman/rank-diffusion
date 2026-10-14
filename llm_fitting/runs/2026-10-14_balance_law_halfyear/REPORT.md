# The rate law with half-year starts: report

_2026-10-14, Claude (Opus 5.5). Design and runner option committed at 700ec29
before the run._

**Status: RAN-OK.** Exploratory, DEVELOPMENT data. Tables: `report_tables.md`;
cells in `cells/`.

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_balance_law.py run-halfyear <panel>
    python llm_fitting/mts_balance_law.py report-halfyear

## The short answer

**The law accounts for the ladder's change in every half-year pair, including
the reversals on comments; as a forecast from the year before it works on
submissions and fails on comments, because on comments the drift by rank
reverses from one year to the next.**

| | cells | slope of observed on predicted | correlation | rms error against "no change" (per 1,000 weeks) | sign right |
|---|---|---|---|---|---|
| same-year slides, ten pairs | 60 | 0.88 | 0.90 | 1.13 against 2.89 | 47 of 48 above 1.0 |
| slides from the year before, six pairs | 36 | 0.17 | — | 3.51 against 2.56 ("repeat last" 3.73) | 17 of 36 |

## By pair

Comments, persisting set, per 1,000 weeks, mean over K/64 to K/2:

| pair | predicted | observed |
|---|---|---|
| Jul 2019 → Jun 2020 | −4.4 | −4.5 |
| Jan 2020 → Dec 2020 | −1.8 | −2.0 |
| Jul 2020 → Jun 2021 | +2.1 | +2.0 |
| Jan 2021 → Dec 2021 | +2.3 | +2.5 |
| Jul 2021 → Jun 2022 | −1.4 | −0.9 |

The comments head sank against the top K in the growth year, rose in
2020–2021 and sank again in 2021–2022, and the law follows each turn. The
yearly analysis of §2z-cd failed on comments 2020→2021 because a sinking and
a rise inside that year averaged out in 52-week blocks.

Lagged: from the year before, the forecast for comments has the wrong sign in
all three pairs (rms 6.2, 4.3, 3.0 against 2.1, 2.6, 1.0 for "no change"). On
submissions it beats "no change" in all three (1.6, 0.9, 2.1 against 3.6,
2.8, 2.6) and "repeat last change" in two.

On submissions the head sinks at a nearly steady 2.6–3.4 a week per 1,000
across all five pairs, and the law's prediction for it ranges from −1.9 to
−3.2 with no matching order (rank correlation 0.10). It over-predicts the
sinking at K/64–K/32 and under-predicts it at K/16–K/8 in the later pairs.

## Predictions scored

| | | |
|---|---|---|
| H1 | pooled: slope 0.6–1.4, correlation ≥ 0.8, rms under 0.6 of "no change" | PASS (0.88, 0.90, 0.39) |
| H2 | lagged beats "no change" in five of six, "repeat last" in three | FAIL: three of six (submissions); "repeat last" four of six |
| H3 | submissions: rank correlation of predicted and observed head sinking over the five pairs ≥ 0.7 | FAIL (0.10) |

One of three held.

## What this changes, found while writing it up

In the same-year form the centre's change cancels between the two sides.
Observed minus predicted is, exactly, the change of the persisting set's
top-k mean, minus the mean change of the named endpoints ranked above k, minus
σ_k²/(2S_k). So the same-year test asks whether the positional top k moves as
its named members do plus what diffusion across the boundary adds. That can
fail, and it holds, but it is an accounting of change by named movement with
a theory of the boundary, not a forecast. The lagged form is the forecast.
§2z-cd described the same-year fit more strongly than this, and is qualified
in the record.

## What it means

1. The boundary-crossing account holds: at every rank tested, on two metrics,
   over ten overlapping year-long pairs, the ladder's change is what members'
   mean movement plus σ²/(2S) at the boundary gives, with a slope of 0.88.
   Nothing else is needed: no correlated movement, no term for heavy tails.
2. Forecasting the ladder is forecasting the drift by rank. Where it
   persists, as on submissions after 2020, a forecast from the year before
   works; where it turns, as on comments every half year here, it does not.
3. What sets the drift by rank, and whether it can be forecast, is now the
   open question on the path from movement to structure.
