# NNLS estimator audit — pre-registered predictions (2026-07-11)

Written BEFORE any --nnls run was executed or examined (only the synthetic
unit tests in tests/test_nnls_estimator.py had been run at registration).

Context: external review (2026-07-11) finding 2 — the MD moment solves use
clipped OLS, not NNLS, and the clipped SSE also drives the (a, phi) grid
choice. `--nnls` (this session) provides the exact solve. This audit reruns
the §2s headline matrix legacy-vs-NNLS under identical code, seeds, and
universes. Legacy remains the default (committed convention, byte-identical).

## Matrix

Cards (in-sample, reps=5, run_platform CLI):
- facebook_a FULL stack        x {legacy, nnls}
- reddit_comments LONG stack   x {legacy, nnls}
- reddit 2d/2e stack           x {legacy, nnls}
Gates (rolling-origin OOS, --dist-scores on):
- facebook_a spec-B + conditional state (paper-primary movement) x {legacy, nnls}
- reddit md6+t + conditional state                               x {legacy, nnls}
- reddit_comments md6+t+mix + conditional state                  x {legacy, nnls}
Legacy guard: facebook default flags (must remain 14/15, churn 0.013).

## Predictions

P1 (parameter level): differences concentrate in the weakly-identified
interior — sigma_trans/sigma_obs mid-band knots move at the 0.1–0.3 level
where the unconstrained solution had negative components; head/tail endpoint
summaries essentially unchanged (reviewer's in-memory finding).

P2 (Spec-B-pinned specs move least): pinning sigma_e removes the noise column
from the design, shrinking the clip-active set. Predicted: the FB
spec-B + conditional gate changes by less than the recorded split-to-split
noise (rel err within ±0.04 of 0.118; coverage within one split).

P3 (cards): no card changes by more than 1 pass of 15; churn err within MC
noise (±0.04). Reason: cards are driven by the simulated moments, which are
continuous in the partition; the clip binds on components whose variance is
near zero, where either solve returns a small number.

P4 (calibrated/unpinned specs move most): the subs and comments conditional
gates (no sigma_e pin) may shift more than FB spec-B; predicted still within
±1 recorded SD of the committed rel errs (subs 0.118 ± 0.061,
comments 0.159 ± 0.070).

## Adoption rule (pre-declared)

If P2–P4 hold: NNLS is reported as an SI robustness result (weighting-audit
pattern, §2y); the default stays legacy for bit-reproducibility, and the
question of switching the documented convention goes to the owner as a
pre-extension decision (protocol freeze).
If any gate moves beyond the stated bands: the NNLS number is REPORTED as the
honest one (the prose describes NNLS), the discrepancy becomes a §2z-e
finding, and adoption/re-freeze is an owner decision before the extension.
Failures are findings, not prompts to iterate.
