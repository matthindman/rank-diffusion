# Step-by-step ladder from a simple rank diffusion to the frozen stacks

_Design written 2026-10-12 by Claude (Opus 5.5) before any ladder cell was
run. Owner request of 2026-10-12: an explainer, in the spirit of the
reply-tree ladder, that starts from a simple rank diffusion and shows what
each added feature does to fit, with the two primary goals weighted equally
and a reasoned weighting across data sets._

Status: EXPLORATORY, on DEVELOPMENT data only. Nothing here is an adoption
test. No default, frozen stack, gate, threshold or protocol is changed. The
index defined below is a presentation device (a Tier-3 probe in the §2z-a
hierarchy), not a new criterion. Gate v1 stays the only adjudicator.

## Panels

| panel | registry names (structure / gate) | K | weeks | role |
|---|---|---|---|---|
| Facebook Era A | `facebook_a` | 3,500 | 86 | paper-primary |
| Reddit comments | `reddit_comments` | 12,500 | 136 | paper-primary |
| Reddit submissions, long | `reddit_submissions_long` | 5,000 | 212 | registered backtest panel, now DEVELOPMENT |
| Reddit submissions, short | `reddit` | 5,000 | 30 | paper-primary |
| Instagram rescue | `instagram_hm` / `instagram_hm_ts` with exact membership | 10,000 | 52 | breadth only; outside the composite |

## Rungs

Every rung is a refit (never a fitted parameter set to zero). All rungs share
the universe, the pooling of sparse head knots (`min_knot_n = 8`), exact
NNLS, exit and rebirth, the common factor, and seeds 0..19.

Core, scored on both goals on every panel:

| id | adds | how it is fitted |
|---|---|---|
| R0 | rank diffusion: a random walk in log activity whose weekly volatility depends on permanent rank | all of γ0 is permanent |
| R1 | noise: part of each week's change does not carry over | γ0..γ6 fit with a random-walk level plus white noise |
| R2 | home: the lasting level is pulled back toward the endpoint's own home | γ0..γ6 fit with an OU level (κ grid as in the code) plus white noise |
| R3 | short-lived swings: the passing part splits into an AR(1) swing and noise | the unrestricted `--md-lags 6` fit |
| R4 | endpoint amplitude | `--temperament` |
| R5 | fat tails | `--t-tails` |

Structure-only steps, in the order the program added them, where the
panel's frozen structure stack has them (Facebook Era A, comments, long
submissions: S1–S4; Instagram: S2; short submissions: none):
S1 long-horizon moments (`--md-vr-long`), S2 stationary common level
(`--stat-factor`), S3 medium component (`--two-scale`), S4 amplitude on the
permanent part (`--mix-hetero`).

Movement-only steps: G1 forecast from each endpoint's filtered end-of-train
state (`--conditional state`), all panels; G2 Facebook: noise pinned from the
daily floor (`--spec-b`); comments: `--mix-hetero`.

The last structure rung is the panel's frozen structure stack and the last
movement rung is its frozen movement stack, so the top of the ladder is also
a reproduction check against the §2z-g and §2z-ae reference values.

R0–R2 are restrictions of `_md_partition` applied through a wrapper in the
ladder script (reversion grid fixed near 1; AR coefficient fixed at 0; the
white part booked as σ_obs). No estimator code is changed.

## Scores

**Movement (held out).** Gate v1 exactly: rolling origin, 5 splits, 20 reps,
2,000 bootstrap draws, mean relative error over median |ΔRank| at h = 1, 4,
13, RACF1 and coll1/5/20, with `MOM_FLOOR`. The gate's train-window
calibration of the σ_obs scale runs at every rung, as in the frozen gate.
For R0 σ_obs is identically zero, the nine calibration candidates are the
same simulation, and the call is skipped with scale 1.0.

**Macro stability (in sample).** Unconditional simulation over the panel
length, 20 seeds, statistics on the seed mean. Nineteen statistics in four
blocks; each error is in tolerance units (1 = the 15-card's own tolerance):

| block | statistics | error |
|---|---|---|
| ladder | S(1), S(10), S(100) within the common recorded top-M | \|ln(sim/emp)\| / ln 1.2 |
| variance structure | VR2, VR4, VR8, VR13; ACF1, ACF2 | ratio as above; \|diff\| / 0.08 |
| order persistence | rolling R²(1, 4, 13); RACF1, RACF4, RACF13 | \|diff\| / 0.08 |
| top set and boundary | rolling Pers(1, 4, 13); out-flux at K | \|diff\| / max(3, 0.15·top_k); ratio |

Each error is capped at 20. The index is the mean of the four block means.
The rolling R² and Pers rows replace the card's period-0 rows; the committed
card (n/15, churn error) is reported beside the index for continuity.

**Combined.** For panel p and rung r,
`I_p(r) = exp( ½ ln[E_macro(p,r)/E_macro(p,R0)] + ½ ln[E_move(p,r)/E_move(p,R0)] )`:
the error left relative to the simple rank diffusion, with the two goals
weighted equally on the log scale. A goal-specific step leaves the other
goal's value unchanged.

**Across data sets.** `I(r) = exp( Σ_p w_p ln I_p(r) )`. Primary weights: one
vote per platform-and-metric system among the model-grade panels; panels of
one system share its vote equally. Facebook interactions 1/3; Reddit comment
karma 1/3; Reddit submission karma 1/3 (long 1/6, short 1/6); Instagram 0.
Reason: the claim is about how the model does on a system of this kind, the
systems differ in what helps them, and with four panels the between-system
spread cannot be estimated, so equal votes over systems are the unbiased
average; panels of one system share entities or a metric and are not
independent replications; Instagram is censored and is recorded as a breadth
demonstration.

Sensitivity weightings, fixed now and all reported: (A) one vote per platform
(Facebook 1/2; comments 1/4; submissions long 1/8, short 1/8); (B) one vote
per panel (1/4 each); (C) proportional to panel length in weeks; (D)
Instagram added as a fourth system.

**Bands.** Macro: bootstrap over the 20 seeds. Movement: mean ± SD over the
five splits, as the gate reports it. Composite: joint resampling of seeds and
splits, 2,000 draws, 5th–95th percentile. These describe simulation and split
variability. They are not confidence intervals for a population of systems.

## Predictions, written before the run

1. R0 fails both goals everywhere: variance ratios near 1, head shares at
   least twice the data, gate error at least twice the historical-mobility
   baseline.
2. R1 (noise) is the largest single movement gain on every panel: gate error
   down by 40% or more.
3. R2 (home) is the largest single macro gain on the long panels and moves
   the gate by less than 0.05.
4. R3 (AR swing) removes less than 20% of the remaining error on either goal.
5. R4 (amplitude) improves the gate on the Reddit panels by 0.02 or more and
   raises the S(1) error on Facebook.
6. R5 (tails) changes the gate by less than 0.02 and raises the S(1) error on
   Facebook.
7. G1 (actual state) improves the gate on the submissions panels and does
   little on Facebook; G2 brings Facebook to about 0.12.
8. S1–S4 improve the variance block on Facebook and comments; S3 helps
   Facebook more than comments.
9. The top rungs reproduce the recorded values within their recorded split
   spread: gates 0.123 (Facebook), 0.171 (comments), 0.164 (short
   submissions), 0.194 (long submissions), 0.320 (Instagram); cards 14, 12,
   14 and 8 of 15.
10. R0–R3 account for at least 70% of the combined log improvement.

## What this cannot show

- The macro score is in sample and later rungs fit more moments of the same
  data; the variance block is partly targeted from R1 on. The ladder, order
  and top-set blocks are never fitted.
- The gains are path-dependent. The order is the logical nesting and the
  program's own history, not a search.
- The gate is pooled, survivor-conditional and blind to departures and to
  upper tails; a tie with the baseline is the expected result for an adequate
  model under stationarity.
- About thirty more gate evaluations on the development panels. All are
  logged in `GATE_LEDGER.md` as exploratory. None is a candidate.
