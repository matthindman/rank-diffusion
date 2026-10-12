# A6 — per-endpoint export from the gate: schema

_Claude (Opus 5.5), 2026-10-12. Input to Codex task V3
(`coordination/briefs/V3_endpoint_scores.md`)._

## What it is

An opt-in flag on the gate. With it, each split also writes every member's
simulated ranks, from the same conditional simulations the gate scores,
beside what was observed. Off by default. The gate's printed and returned
numbers are the same with or without it, and no random draw is added
(`tests/test_endpoint_export.py`).

```
python llm_fitting/rankdiff_kalman.py <platform> --oos <the frozen movement flags> \
    --reps 20 --boot 2000 --endpoint-export output/endpoint_export/<platform>
```

or `oos_movement(..., endpoint_export="<dir>")`. The frozen movement flags
for each panel are in `stochastic-modeling/references/frozen-specs.md`. The
flag needs a conditional stack (`--conditional state` or `vhat`); it refuses
to run without one, because the unconditional simulation has no link to the
panel's endpoints. All four primary movement stacks are conditional.

A run with this flag is a gate evaluation like any other: it goes through
`scripts/heavy_lock.py` and into `GATE_ROWS.md`. Write under `output/`,
which is not in git.

## Files

One per split: `<dir>/<platform>__T0_<origin, four digits>.npz`, written
with `numpy.savez_compressed`; read with `numpy.load(path)` (no pickle).

| array | shape | type | meaning |
|---|---|---|---|
| `entity_id` | (N,) | str | the split's member universe: the 4K endpoints selected on weeks before `T0` only. Column order of every other array |
| `platform`, `stack` | scalar | str | panel key; the gate's option string for the run |
| `T0`, `test_len`, `top_k` | scalar | int | first test week (panel period index); number of test weeks; K |
| `seeds` | (reps,) | int | the simulation seed of each rep: 0, 1, …, reps − 1, the gate's own |
| `train_perm_rank` | (N,) | float | mean within-universe rank over the train weeks, absent weeks counted at that week's member count + 1 |
| `train_presence` | (N,) | float | share of train weeks observed |
| `origin_rank` | (N,) | uint32 | observed within-universe rank in the last train week, `T0 − 1`; 0 = not observed that week |
| `obs_rank` | (test_len, N) | uint32 | observed within-universe rank in test week `w` (panel period `T0 + w`); 0 = not observed that week |
| `sim_rank` | (reps, test_len, N) | uint32 | simulated rank in test week `w`; 0 = the endpoint has exited in that rep |

The forecast origin is the last train week, so the horizon of row `w` is
`w + 1` weeks.

## Things to know before scoring

- **Two kinds of zero.** In `obs_rank` a zero is a week without an
  observation; the endpoint can come back. In `sim_rank` a zero is the
  model's exit: the slot has been refilled by a new entrant and the
  endpoint does not return in that rep. The brief's "leaving the scored set
  as a censored state" has to cover both, and "outside the top-K" is
  `rank == 0 or rank > top_k` in both arrays.
- **The two rank scales differ slightly.** A simulated rank is a rank among
  all N slots, every one of them occupied (entrants fill the slots of
  exited endpoints; entrants are not exported). An observed rank is a rank
  among the members observed that week, which can be fewer than N. The gate
  compares the two directly, so the export does too. If you rescale, say so.
- **What the gate scores is a subset of this.** The gate follows the 200
  top-ranked slots of the first test week and pools their absolute rank
  changes at horizons 1, 4 and 13 from every start week. Row 0 of
  `sim_rank` with ranks 1–200 picks out that cohort for a rep; the test
  file shows the recomputation.
- **Reps are draws from the predictive law given the train window**, not
  independent refits: parameters, filtered states and (with `vhat`)
  amplitudes are estimated once per split and shared by all reps.
- **Splits overlap in time.** Five origins on one panel are not five
  independent experiments (your brief, item 3).
- **`train_perm_rank` is within the universe.** Membership itself was
  decided on the full-population permanent rank; the two orderings agree
  closely and are not identical. The established cohort of the departure
  measurements is `train_perm_rank <= top_k / 2` with
  `train_presence >= 0.7`.
- **Size.** N × test_len × reps × 4 bytes before compression: about 136 MB
  per split on the comments panel at 20 reps (N = 50,000, 34 weeks).

## Not included

Simulated values (log activity): the gate tracks ranks. Entrants. The
unconditional simulation. Baselines: those are V3's to build from the
training window.
