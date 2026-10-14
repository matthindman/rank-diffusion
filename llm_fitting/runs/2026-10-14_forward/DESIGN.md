# Forward simulation: one year's movement pushed through the next year's ladder

_2026-10-14, Claude (Opus 5.5). Written before the runner touched a panel.
Part of the owner's request of the same day to "pursue this line of research
aggressively and rigorously"._

**Status: design. NOT RUN.** Exploratory, DEVELOPMENT data. Data only: the
simulation is of measured transitions, not of a fitted model; no gate
evaluation. Runner `llm_fitting/mts_forward.py`; tests
`tests/test_mts_forward.py`.

## Question

The rate law of §2z-cd predicts how fast the ladder's shape changes from two
moments of named movement by rank, under a small-step assumption, one year
at a time. Three things it does not show. Whether the information is there
without any theory: if one year's movement by rank is simply pushed through
the next year's ladder, does the result look like the year after? Whether two
moments by rank are enough, and whether eight numbers are. And whether the
forecast holds two years out, when the ladder's own change feeds back on
where everyone stands.

## Method

Two Reddit panels, four 52-week blocks; the top W = 2K of each block,
certified exact. Rank strata as §2z-cd: one at the top, (0, K/128], then
octaves to (K, 2K].

**A transition** (blocks a → a+1), by stratum of rank in block a: the changes
of block level of the members still present, and the share with no level
next block; and the entrants, endpoints in the top W of a+1 that were not in
the top W of a (new, or climbed from below W), with their levels at a+1 as
offsets from block a's K-th level.

**Applied to block b**: each member of b's top W draws a change for its
stratum and survives at that stratum's rate; the entrants are added at the
offsets from b's K-th level; the top W is kept. Repeated 40 times.

**Versions**: *empirical* (changes resampled from the stratum's own),
*moments* (Gaussian with the stratum's mean and variance), *smooth* (mean and
log variance linear in log rank, attrition logistic in log rank, entrant
count and a Gaussian for offsets: eight numbers).

**Cases.** One step out of sample: transition (0→1) applied to block 1,
predicting block 2; transition (1→2) applied to block 2, predicting block 3.
Two steps out of sample: (0→1) twice from block 1, predicting block 3. Also
one step in sample (a transition applied to its own start), as a check of the
machinery only.

**Scored**: the change of shape (top-k mean minus top-K mean) at K/64, K/32,
K/16, K/8, K/4, K/2; rms error over the six against "no change" and "repeat
last change". Local slopes at the same cut-offs, against persistence.

Everything in the transition is a function of current block rank, so it
carries whatever block means carry: drift, passing swings, selection on the
block. That is deliberate. It is a forecasting map, not a structural
estimate; the rate law is the structural one.

## What I have already seen

The rate law's results (§2z-cd), including its lagged prediction, which is
close to the second one-step case here. The planted worlds of the tests.
Nothing of this runner on the panels.

## Predictions

- **F1**: in the four one-step out-of-sample cases, the empirical version
  beats "no change" in at least three, and beats "repeat last change" in at
  least two.
- **F2**: the moments version is within 20% of the empirical version's rms
  error in at least three of the four.
- **F3**: the smooth version is within 30% of it in at least three of the
  four. Least sure: in the planted worlds, where the drift is a step in rank,
  it was much worse.
- **F4**: two steps out of sample beats "no change" on both panels.
- **F5**: in the one-step out-of-sample cases the worst slope error over the
  cut-offs is no more than five points above persistence's in at least three
  of four.

## What the planted worlds showed before the data

With the head pressed down by a fixed rule, one year's transition forecasts
the next year's change at a fifth to a half of the error of "no change", one
and two steps ahead, about level with "repeat last change". In a stationary
world it does worse than "no change", as any forecast that adds its own noise
must when nothing moves. The smooth version did badly where the drift was a
step in rank.

## Limits

Four one-step cases, two of them sharing a panel's history. Block-rank strata
mix lasting movement with swings and selection; a transition is assumed to
carry over from one year to the next unchanged. The entrants' offsets carry
the year's common growth, which cancels in shape. W = 2K is a modelling
boundary: climbers from below it count as entrants.

## Commands and outputs

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/mts_forward.py run <panel>
    python llm_fitting/mts_forward.py report
