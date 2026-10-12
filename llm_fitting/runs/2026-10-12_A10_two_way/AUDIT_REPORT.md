# A10 — audit of the time-holdout comparison: result

Run 2026-10-12 by Claude (Opus 5.5), after `AUDIT_DESIGN.md` was written.
Data only; no fit, simulation or gate evaluation. Command:
`python llm_fitting/phase1_a10.py audit <panel>`; cells
`cells/audit__<panel>.json`. The error was found by Codex
(`llm_fitting/handoff/D1_debate/variance_structure/A10_REVIEW.md`).

## Result

Error of the second-half ladder, log units. "Reconstruction" is the ladder
integrated from first-half variance and first-half g. "Carried forward" is
the first-half ladder. "In sample" is the reconstruction from second-half
variance and g. The first two rows of each panel anchor every curve at the
second half's value at rank K, as A10 did. The last two remove each curve's
own location and use no second-half value as an anchor; they were added
after the first audit run and are post hoc.

| panel | scored on | reconstruction | carried forward | ratio | in sample |
|---|---|---|---|---|---|
| long submissions | log-spaced ranks (149), anchored at K | 0.528 | 0.462 | 1.14 | 0.249 |
| | every rank 1 to 5,000, anchored at K | 0.155 | 0.168 | 0.93 | 0.061 |
| | log-spaced ranks, location removed | 0.379 | 0.268 | 1.42 | 0.247 |
| | every rank, location removed | 0.155 | 0.128 | 1.21 | 0.061 |
| comments extension | log-spaced ranks (156), anchored at K | 0.528 | 0.470 | 1.12 | 0.175 |
| | every rank 1 to 12,500, anchored at K | 0.232 | 0.194 | 1.19 | 0.147 |
| | log-spaced ranks, location removed | 0.248 | 0.209 | 1.19 | 0.124 |
| | every rank, location removed | 0.121 | 0.110 | 1.10 | 0.081 |

The recorded numbers reproduce exactly (first-half g; 0.528 on both panels;
0.168 and 0.194 for the carried-forward ladder on every rank).

## What it shows

- §2z-bh's "factor of about three" set 0.528, scored on log-spaced ranks,
  against 0.168 and 0.194, scored on every rank. It is wrong. Codex's
  matched-grid values, 0.462 and 0.470, are reproduced.
- Scored alike, carrying the ladder forward is the better predictor in
  seven of the eight rows, by 10% to 42%. The reconstruction is better in
  one (long submissions, every rank, anchored at K), by 7%.
- The choice of weighting and of anchor moves the ratio from 0.93 to 1.42
  on one panel. A comparison of this kind needs its loss fixed beforehand.
- Against the in-sample reconstruction on every rank, both predictors have
  about two and a half times its error on long submissions (0.061) and
  about one and a half times on the comments extension (0.147).

## Predictions (`AUDIT_DESIGN.md`), scored

1. The recorded numbers reproduce. HELD.
2. Carried forward on the log grid is 0.46 and 0.47. HELD.
3. On every rank the reconstruction's error is below 0.30 on both panels.
   HELD (0.155, 0.232).
4. On every rank the reconstruction does not beat the carried-forward
   ladder on long submissions. FAILED: 0.155 against 0.168. With the
   location removed (post hoc) it does not beat it: 0.155 against 0.128.

A10's own prediction 6 is unaffected and still failed: g moved by more than
25% between halves on long submissions, and the out-of-time error on the
log grid is 2.1 and 3.0 times the in-sample error.

## Limits

Two panels, one split each, universes chosen on the full window, and no
uncertainty interval on any ratio. Neither curve is a forecast of level in
the anchored rows. None of this is a test of the stationary relation
itself.
