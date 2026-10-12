# A9 — does the lasting-variance profile predict the ladder's local slope?

_Written 2026-10-12 by Claude (Opus 5.5) before the check was run. Asked
for by the owner the same day ("do the first concrete check"), following
the consensus on candidate 2 (MODEL_STATUS §2z-be) and the owner's
question whether the structure of the variance should reproduce the
observed rank structure and its local slope._

Status: EXPLORATORY, descriptive, DEVELOPMENT data, in sample. Data only,
plus the existing frozen structure fit for one secondary column. No model
code, no simulation, no gate evaluation. No planted-data validation exists
for this comparison yet, so it cannot establish a mechanism; it can say
whether the simplest form of the hypothesis is worth designing around.
Runner: `llm_fitting/phase1_a9.py`.

## The hypothesis in its simplest form

In a rank-based diffusion the mean gap in log size between ranks k and k+1
is (σ_k² + σ_{k+1}²) / (4 G_k), where σ² is the variance per unit time of an
endpoint's lasting log level and G_k is the cumulative restoring drift of
ranks 1 to k (repository literature note, from Fernholz & Fernholz; exact
only for an idealised continuous process). The local log-log slope of the
rank-size curve is s_k ≈ k × gap_k. If every rank has the same relative
drift g (the Atlas case), G_k = g k and

    s_k = σ_k² / (2 g):   the local slope is proportional to the lasting variance.

So define, per rank bin, ρ = σ² / (2 s). Under the one-constant hypothesis
ρ is the same in every bin and equals g. If ρ varies with rank, ρ × k is the
implied cumulative drift and its shape says how drift depends on position.

## What is computed

Panels: FB Era A (K = 3,500, 86 weeks), comments (K = 12,500, 136 weeks),
long submissions (K = 5,000, 212 weeks), and the comments extension over
its full window (213 weeks). Full-window top-coverage universe. Instagram
is left out (censored).

Rank bins: octaves of permanent rank, (K/2^(j+1), K/2^j], down to an edge of
at least 12, plus one top bin.

- **Lasting variance per week, σ².** Primary, model-free: the growth rate
  of the variance of h-week changes in log level net of the common factor,
  (D(52) − D(13)) / 39 on the Reddit panels and (D(26) − D(13)) / 13 on
  Facebook, for endpoints whose permanent rank is in the bin. It uses no
  fitted reversion rate. An endpoint bootstrap gives a 5–95% bracket.
  Secondary: the fitted σ_perm² of the frozen structure stack, averaged
  over the bin. A robust version (from the MAD) is reported beside the
  primary.
- **Local slope of the ladder, s.** Primary: minus the regression slope of
  the time-median ladder (at each rank, the median over weeks of the value
  observed there) on log rank, within the bin; the interquartile range of
  the same slope computed week by week is its spread. Secondary: the same
  on the ladder of sorted endpoint means, which leaves out weekly noise.
- **ρ = σ² / (2 s)** per bin, with the bracket carried from σ².

Known weaknesses, stated before looking. The Reddit variance rate still
contains an annual cycle's contribution at 13 weeks, which lowers it by a
few percent. The observed ladder is widened by noise, most at the head.
The top bins hold few endpoints. Permanent rank is a label fixed over the
window, while the theory's σ_k belongs to whoever holds rank k. The ladder
drifts over the window (§2z-ay).

## Predictions

1. σ² rises from the head to the bottom of the list on all four panels:
   the bottom octave's value is at least 1.5 times the value in the octave
   around K/16.
2. The ladder steepens with rank on Facebook and long submissions (the
   log-log curve is concave): s in the bottom octave exceeds s around K/16.
   On the two comments panels the head is anomalous (one endpoint far
   ahead) and this may not hold there.
3. Across bins, s and σ² are positively related on at least three panels
   (Spearman correlation above 0.5, top bin excluded).
4. The one-constant form does NOT hold cleanly: ρ varies across bins with a
   coefficient of variation between 0.2 and 0.6 on at least three panels,
   and the log-log slope of s on σ² is positive and below 1.
5. ρ is between 0.001 and 0.01 per week in the middle of the list on every
   panel.

Prediction 4 is my honest expectation, not a hope: from the three-group
numbers already on record the variance roughly doubles from head to shell,
and I expect the slope to change by less.

## What each outcome licenses

- ρ constant across bins (coefficient of variation under 0.2) on at least
  two panels: the variance profile and one number predict the shape of the
  ladder at first pass. That becomes the organising hypothesis of the
  diagnostic design, with the constant to be pinned from turnover and the
  comparison validated on planted data.
- s and σ² related, ρ trending with rank: drift depends on position. The
  implied cumulative drift profile is then a target that the
  state-dependent-growth diagnostic must reproduce from movement.
- No relation: the simplest form is not supported at this resolution, and
  the design should not be organised around it.
