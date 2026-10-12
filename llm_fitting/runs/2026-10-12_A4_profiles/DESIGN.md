# A4 — how much of the band profiles is structure

_Written 2026-10-12 by Claude (Opus 5.5) before any cell of this task was
run. Work plan `WORKPLAN_CLAUDE_2026-10-12.md`, Phase 1, task A4._

Status: EXPLORATORY measurement on DEVELOPMENT data. No new specification:
every fit is a panel's frozen structure stack, on the full universe and on
two halves of it. No simulation, no gate evaluation, nothing adopted.

## Questions

1. The model carries about 55 band values per profile. How much of each
   profile is a smooth curve in permanent rank, and how much is band-level
   wiggle that does not reproduce?
2. Is the reversion rate κ identified? The search grid has eight to ten
   values of κ between 0.005 and 0.30.
3. Is the stationary variance of the lasting component, V = σ_perm²/(2κ),
   better determined than κ itself?

## Method

Panels: FB Era A, Reddit comments, long submissions. Fits: the top
structure rung of §2z-ap (`ladder_explainer._fit`), (a) on the full
top-coverage universe, (b) on each of two halves of its entities, formed by
ordering entities by permanent rank and alternating (ranks and the rank
coordinate are left as they are in the full universe, so the two halves
cover the same range).

For each profile (log σ_perm, log σ_trans, log σ_obs, log κ, and log V):

- *Reliability*: the correlation across the full fit's knots between the
  two half-fits (each interpolated to those knots), stepped up to full
  sample size by the Spearman–Brown formula. This is the share of the
  profile's variance across knots that reproduces.
- *Curves*: R² of a straight line and of a quadratic in the rank coordinate
  z, fitted to the full-fit profile.
- *Scaling exponent*: slope of the log profile on the home log-level at the
  knot (activity units), the fluctuation-scaling form σ ∝ size^β.
- *Grid edges*: share of knots whose κ is the slowest or the fastest value
  on the grid, overall and in the top third of knots.

## Predictions

1. For log σ_trans and log σ_obs a straight line in z reaches an R² of at
   least 0.8 of the reliability on all three panels.
2. For log σ_perm a straight line reaches at least 0.6 of the reliability;
   for log κ the reliability itself is below 0.5 on at least two panels.
3. The scaling exponent of σ_obs on the two Reddit panels is between −0.6
   and −0.4 (counting noise would give −0.5).
4. The exponents of σ_perm and σ_trans differ by less than 0.1 on each
   panel.
5. At least 30% of knots have κ at a grid edge on each panel, and in the
   top third of knots on comments at least half sit at the slowest value.
6. log V is more reliable than log κ on each panel.

## What each outcome licenses

- Lines explain most of what reproduces (1, 2 hold): candidate C-P
  (compressed profiles) goes to a dossier; the band profiles are mostly
  noise around a few numbers.
- κ unreliable and on its edges, V reliable (5, 6 hold): the κ "shift"
  between eras (§2z-q) is at least partly movement along a ridge;
  transport should be stated in V, and the head κ of the reference panel is
  a boundary value.
- Lines fail: the profiles carry real band-level structure and C-P is not
  licensed in this form.
