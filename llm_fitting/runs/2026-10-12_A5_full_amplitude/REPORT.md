# A5 — report

Run 2026-10-12 by Claude (Opus 5.5) from commit 5072f89 (design and
predictions committed first). Structure only; 20 seeds (0–19); five cells,
all completed; no gate evaluation. Command:
`python llm_fitting/phase1_a5.py run`, then `report`. Numbers:
`summary.json`; cells in `cells/`. R3 and R4 are the ladder's cells
(§2z-ap), same seeds and code.

## Result

Macro index (scorecard-tolerance units; lower is better). The band on the
difference is the 5th–95th percentile from resampling the 20 seeds jointly.

| panel | R3 | R4 (amplitude on passing parts) | R4m (amplitude on all parts) | R4m − R4 | share of the R3→R4 loss removed | b | top-1 share over data, R4 → R4m |
|---|---|---|---|---|---|---|---|
| FB Era A | 1.42 | 1.55 | 1.71 | +0.159 [+0.026, +0.305] | none; worse | 1.02 | 2.35 → 3.54 |
| Reddit comments | 1.17 | 1.46 | 1.43 | −0.029 [−0.233, +0.182] | 10% | 1.08 | 1.38 → 1.51 |
| Reddit submissions, long | 0.73 | 0.92 | 0.77 | −0.150 [−0.204, −0.109] | 81% | 0.95 | 0.56 → 0.53 |
| Reddit submissions, short | 0.27 | 0.32 | 0.32 | 0 | — | 0 | 1.06 → 1.06 |
| Instagram rescue | 1.77 | 1.83 | 1.64 | −0.196 [−0.247, −0.132] | all, and more | 1.02 | 2.07 → 1.79 |

By block:

| panel | variance block R3 / R4 / R4m | ladder (head-share) block R3 / R4 / R4m |
|---|---|---|
| FB Era A | 0.81 / 1.26 / 1.04 | 3.46 / 3.19 / 4.17 |
| Reddit comments | 1.92 / 2.37 / 2.06 | 0.92 / 1.55 / 1.86 |
| Reddit submissions, long | 0.94 / 1.75 / 1.13 | 1.06 / 1.07 / 1.24 |
| Reddit submissions, short | 0.11 / 0.30 / 0.30 | 0.38 / 0.26 / 0.26 |
| Instagram rescue | 1.26 / 1.42 / 1.27 | 3.37 / 3.40 / 2.93 |

## What it shows

- Applying the amplitude to the lasting part as well repairs much of the
  variance-block loss of the amplitude step on every panel where it can be
  estimated: 49% of it on FB, 69% on comments, 77% on long submissions, 94%
  on Instagram. The half-application is a real cause of that loss.
- It also moves the head. On FB the simulated top-1 share goes from 2.35 to
  3.54 times the data's, and the ladder block worsens on comments and long
  submissions too; only Instagram improves there. On FB this outweighs the
  variance gain and the index gets worse, with a seed band that excludes
  zero.
- So "the same amplitude everywhere" is not a free repair. It fixes horizon
  scaling and feeds the head-share error, which is already the largest
  macro error on FB. Whatever makes the head too heavy has to be dealt with
  in the same change, or the amplitude of head endpoints is not the
  unbounded lognormal the simulator draws.
- Short submissions: identical to R4, as the code implies (b = 0 on a
  30-week panel).

## Predictions, scored

| # | prediction | outcome | verdict |
|---|---|---|---|
| 1 | R4m index not higher than R4 on FB, comments, long submissions | lower on comments and long submissions; higher on FB (+0.159, band excludes zero) | FAILED on FB |
| 2 | recovery is partial: less than half of the R3→R4 loss on comments and long submissions | 10% on comments; 81% on long submissions | HELD on comments, FAILED on long submissions |
| 3 | FB difference inside its seed band | band [+0.026, +0.305] | FAILED |
| 4 | FB top-1 share not lower at R4m | 2.35 → 3.54 | HELD |
| 5 | Instagram within ±5% of R4 | −10.7%, band excludes zero | FAILED |

Three of five failed or half-failed. The direction I had wrong is the
size of the effect on the lasting component: it is large in both
directions, not a small correction.

## What it licenses

Neither branch of the design cleanly. The loss "disappears" on long
submissions and Instagram and not on FB or comments. Candidate C-A cannot
be a flag change: its dossier has to state what happens to the head, and
the head measurement (A3) has to include how amplitude and rank relate
among the top endpoints. Not run here: any gate evaluation; a variant with
the amplitude shrunk or bounded at the head.
