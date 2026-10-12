# A4 — report

Run 2026-10-12 by Claude (Opus 5.5) from commit a2728b7 (design and
predictions committed first). Nine fits (three panels × full universe and
two halves), all completed; no simulation, no gate evaluation. Command:
`python llm_fitting/phase1_a4.py`. Numbers: `summary.json`. The run took
over ten minutes on a loaded machine and finished in the background.

## Result

"Reliability" is the share of a profile's variance across knots that
reproduces between two halves of the entities (half-sample correlation,
stepped up to full size). "Line" is the R² of a straight line in the rank
coordinate.

| panel | profile | reliability | R² line | R² quadratic | line as a share of reliability | scaling exponent |
|---|---|---|---|---|---|---|
| FB Era A | log σ_perm | 0.84 | 0.10 | 0.12 | 0.12 | −0.03 |
| | log σ_trans | 0.44 | 0.50 | 0.56 | 1.14 | −0.08 |
| | log σ_obs | 0.78 | 0.59 | 0.64 | 0.76 | −0.18 |
| | log κ | 0.34 | 0.03 | 0.18 | 0.10 | |
| | log V | 0.69 | 0.11 | 0.20 | 0.15 | |
| Reddit submissions, long | log σ_perm | 0.74 | 0.51 | 0.51 | 0.69 | −0.03 |
| | log σ_trans | 0.89 | 0.67 | 0.69 | 0.76 | −0.12 |
| | log σ_obs | 0.94 | 0.75 | 0.75 | 0.79 | −0.23 |
| | log κ | none (half-sample correlation −0.44) | 0.26 | 0.43 | | |
| | log V | 0.63 | 0.54 | 0.62 | 0.86 | |
| Reddit comments | log σ_perm | 0.77 | 0.41 | 0.42 | 0.54 | −0.05 |
| | log σ_trans | 0.89 | 0.71 | 0.74 | 0.79 | −0.12 |
| | log σ_obs | 0.97 | 0.54 | 0.60 | 0.56 | −0.14 |
| | log κ | 0.07 | 0.03 | 0.08 | | |
| | log V | 0.52 | 0.28 | 0.32 | 0.55 | |

Reversion rate: the search grid runs from 0.005 to 0.30. Share of knots at
the slowest value, 0.005: FB 31%, long submissions 52%, comments 44% (50%
in the top third of knots). No knot is at the fastest grid value; the
largest κ used is 0.07 on FB and comments and 0.02 on long submissions,
where κ takes only three values.

Seen while tabulating, not in the design: σ_obs is exactly zero (a solver
corner) at 15 of 54 knots on FB, 18 of 54 on long submissions and 21 of 55
on comments; σ_trans is zero at 4 knots on each Reddit panel.

## What it shows

- The band profiles of the three volatilities mostly reproduce (0.74–0.97,
  except σ_trans on FB), so they are not mostly noise.
- A straight line in rank is not enough. It reaches 54–79% of the
  reproducible share for five of the nine volatility profiles, all of it
  for one (σ_trans on FB) and 12% for σ_perm on FB.
- The reversion rate is not identified band by band: its profile does not
  reproduce (0.34, none, 0.07), a third to a half of the knots sit on the
  slowest grid value, and on the 212-week panel it takes three values.
  The stationary variance V reproduces better than κ on every panel
  (0.69, 0.63, 0.52).
- Volatility depends weakly on size: exponents of −0.03 to −0.12 for the
  lasting and passing parts and −0.14 to −0.23 for the noise part, far from
  the −0.5 of counting noise.

## Predictions, scored

| # | prediction | outcome | verdict |
|---|---|---|---|
| 1 | a line reaches 0.8 of reliability for σ_trans and σ_obs on all three panels | σ_trans 1.14, 0.76, 0.79; σ_obs 0.76, 0.79, 0.56 | FAILED (one of six) |
| 2 | a line reaches 0.6 of reliability for σ_perm; κ reliability under 0.5 on at least two panels | σ_perm 0.12, 0.69, 0.54; κ 0.34, none, 0.07 | first part FAILED on two panels; second part HELD |
| 3 | σ_obs exponent between −0.6 and −0.4 on the Reddit panels | −0.23, −0.14 | FAILED |
| 4 | σ_perm and σ_trans exponents within 0.1 of each other | differences 0.05, 0.09, 0.07 | HELD |
| 5 | at least 30% of knots at a grid edge; top third on comments at least half at the slowest | 31%, 52%, 44% at the slowest; 50% | HELD |
| 6 | log V more reliable than log κ | 0.69 vs 0.34; 0.63 vs none; 0.52 vs 0.07 | HELD |

## Post hoc (not in the design; labelled as such)

From the full-fit profiles on disk (`runs/2026-10-12_ladder/cells/*S4.json`):
polynomials in the rank coordinate with up to six parameters reach the
reliability only for σ_trans on FB (two parameters) and σ_obs on FB (five).
For σ_perm on FB the R² stays at 0.13 with six parameters against a
reliability of 0.84. The reproducible structure is not a smooth curve in
log rank. Numbers: `posthoc_polynomials.json`.

## What it licenses

- Candidate C-P in the form written in the plan (each profile replaced by a
  scaling curve of a few numbers) is NOT licensed: the curves miss
  structure that reproduces.
- What is licensed is narrower and concerns the reversion rate: replacing
  κ(z) by one or two numbers, and stating transport between eras in V. The
  head κ of 0.0050 used as the reference in the registered confirmation
  (§2z-q) is the slowest value the grid allows, so the recorded 4–8× "shift"
  in κ is at least partly a boundary estimate moving off its boundary.
- A different compression is still open: fewer, wider bands (a refit, not a
  curve fit). Not run here.

Not run: any refit with fewer bands; V by era on the extension panel; the
closed-form sensitivity of each estimate to each moment.
