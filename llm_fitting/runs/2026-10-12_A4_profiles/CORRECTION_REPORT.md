# A4 — corrected half-sample comparison

Run 2026-10-12 by Claude (Opus 5.5), after `CORRECTION_DESIGN.md` was
written. Nine fits (three panels, the full universe and two halves ranked
within themselves), all completed; no simulation, no gate evaluation.
Command: `python llm_fitting/phase1_a4.py --rerank-halves`. Numbers:
`summary_reranked_halves.json`. The recorded run's numbers stay in
`summary.json` and `REPORT.md`. Without the flag the script takes the same
path as before and writes three more fields; that was checked by reading,
and the nine recorded fits were not run again.

The error was found by Codex (review R1, finding 1): the recorded half fits
were on a rank coordinate shifted by log 2, with the lower half of each
half's endpoints clipped at the bottom. Each half now has about half the
full universe's entities per week (6,736 and 6,719 on FB; 9,906 and 9,902
on long submissions; 21,235 and 21,226 on comments), one knot fewer than
the full fit, and the comparison uses the 52 or 53 full-fit knots that lie
inside both halves' range.

## Result

Reliability is the half-sample correlation stepped up to full size. The
line's share is the R² of a straight line in the rank coordinate, on the
knots compared, over the reliability. Every full-fit quantity (the curves'
R² on all knots, the scaling exponents, the share of knots at the slowest
reversion value, the zero counts) is identical to the recorded run.

| panel | profile | reliability, recorded | reliability, corrected | line's share, recorded | line's share, corrected |
|---|---|---|---|---|---|
| FB Era A | log σ_perm | 0.84 | 0.75 | 0.12 | 0.13 |
| | log σ_trans | 0.44 | 0.59 | 1.14 | 0.82 |
| | log σ_obs | 0.78 | 0.75 | 0.76 | 0.77 |
| | log κ | 0.34 | none (half correlation −0.10) | | |
| | log V | 0.69 | 0.42 | 0.15 | 0.23 |
| Reddit submissions, long | log σ_perm | 0.74 | 0.71 | 0.69 | 0.97 |
| | log σ_trans | 0.89 | 0.76 | 0.76 | 0.88 |
| | log σ_obs | 0.94 | 0.97 | 0.79 | 0.76 |
| | log κ | none (−0.44) | none (−0.25) | | |
| | log V | 0.63 | 0.36 | 0.86 | above 1 (line 0.73, reliability 0.36) |
| Reddit comments | log σ_perm | 0.77 | 0.94 | 0.54 | 0.48 |
| | log σ_trans | 0.89 | 0.88 | 0.79 | 0.78 |
| | log σ_obs | 0.97 | 0.86 | 0.56 | 0.60 |
| | log κ | 0.07 | 0.64 | | |
| | log V | 0.52 | 0.83 | 0.55 | 0.39 |

## What changes

- **The volatility profiles still mostly reproduce**: 0.71–0.97, with
  σ_trans on FB at 0.59 (recorded: 0.74–0.97, and 0.44). No reliability of
  the nine moved by more than 0.17.
- **A straight line does better than recorded.** It reaches 76–97% of the
  reproducible share for six of the nine volatility profiles (recorded:
  54–79% for five). It still fails for the lasting volatility on FB (13%)
  and on comments (48%), and for the noise volatility on comments (60%).
  Part of the gain on long submissions comes from leaving out the two top
  knots, where the line fits worst (its R² for σ_perm is 0.51 on all knots
  and 0.69 on the knots compared).
- **The reversion-rate profile does not reproduce on FB or long
  submissions** (half correlations −0.10 and −0.25) **and does on
  comments** (0.64, recorded 0.07). A profile that reproduces between
  halves is not thereby recovered: 44% of the comments knots sit on the
  slowest grid value in the full fit, and both halves can sit there
  together. Codex's recovery study found the fitted reversion rates far
  from the planted ones (MODEL_STATUS §2z-bc).
- **The stationary variance V still reproduces better than κ on every
  panel**, but less well than recorded on FB and long submissions (0.42 and
  0.36, recorded 0.69 and 0.63) and better on comments (0.83, recorded
  0.52).

## Predictions (`CORRECTION_DESIGN.md`), scored

| # | prediction | outcome | verdict |
|---|---|---|---|
| 1 | reliability at least 0.6 for the three volatilities on every panel, FB σ_trans excepted | 0.75, 0.75; 0.71, 0.76, 0.97; 0.94, 0.88, 0.86 (FB σ_trans 0.59) | HELD |
| 2 | κ reliability below 0.5 on at least two panels | none, none, 0.64 | HELD (two of three; comments is above) |
| 3 | log V more reliable than log κ on all three | 0.42 vs none; 0.36 vs none; 0.83 vs 0.64 | HELD |
| 4 | the line's share below 0.8 for at least five of the nine volatility profiles | six: 0.13, 0.77, 0.76, 0.48, 0.78, 0.60 | HELD |
| 5 | no volatility reliability moves by more than 0.25 | largest move 0.17 | HELD |

The recorded run's own predictions, rescored on the corrected numbers:
(1) a line reaches 0.8 of reliability for σ_trans and σ_obs on all three
panels: still FAILED, now two of six at or above 0.8 and five of six at or
above 0.76. (2) A line reaches 0.6 for σ_perm: still FAILED on two panels
(0.13, 0.97, 0.48); κ reliability under 0.5 on at least two panels: HELD.
(6) log V more reliable than log κ: HELD. Predictions 3, 4 and 5 do not
involve the half fits.

## What it licenses, corrected

- A low-dimensional form for the passing and noise volatilities is better
  supported than recorded, and is still not established: no refit with such
  a form has been run.
- The lasting volatility on FB and on comments has reproducible structure
  that a line does not describe. That reading stands.
- "κ is not identified band by band" stands for FB and long submissions.
  For comments it rests on the recovery study and on the grid bound, not on
  this comparison.

## Limits

The two halves share their calendar weeks and the common weekly factor, so
their errors are not independent and the stepped-up correlation can
overstate reliability (Codex, R1). That applies to the recorded and the
corrected numbers alike and is not measured here.
