# R4 — protocol review design

2026-10-13. PLANNING ONLY; nothing adopted. OpenAI Codex (GPT-6).
Review target: `a75b9c204d648a9da5dcf90ca1db9469658a6bab`, draft v1 of
`MTS_PROTOCOL_STEPS_1_2_2026-10-13.md`, its two modules and tests.

Review the exact accounting separately from the proposed interpretation and
step-3 decision. Read the adopted agenda, MODEL_STATUS §§2z-bp–bq, the first
proof's consequences, and the original theory note. Do not edit reviewed
files or run step 1 on empirical panels. This is R4 only; V4 needs the
author's answer and corrected statistics before its recovery design is frozen.

Checks fixed before running the review script:

1. Compare finite-interval reordering with the sum over subdivisions on a
   two-endpoint round trip. Expect different results: it is not additive
   local time. Check the primary rank-semimartingale formula as well.
2. Derive the population carrier coefficients for four block means of a
   stationary position-only OU process. Expect a positive older coefficient
   from block aggregation even though names have no individual homes.
3. Use independent synthetic endpoints with fixed levels plus persistent
   AR noise, and a random-walk lasting level plus AR noise. Check the actual
   adjacent-block regression and cohort means, not the more widely spaced
   windows in the existing unit test. These are counterexamples, not a
   recovery study or estimates of empirical false-positive frequency.
4. Inspect the bootstrap construction and compare summing two marginal
   normal 90% intervals with the interval for their independent sum.
5. Construct a rotating weekly top-4K population with an untracked endpoint
   high in the block ladder. Check that invalid tracking is flagged or
   rejected. Repeat for diffusion bands beyond the certified cutoff.
6. Derive effects of attrition and of future-based membership in the outside
   cohort. Verify simple deterministic examples where applicable.
7. Check that the stopping rule requires a usable nonzero drift and a valid
   predictive bridge, and that each prediction, secondary run and missing
   statistic has a prespecified adjudication.

One round of review. Tiny deterministic checks and analytical covariance
calculations come first. Any synthetic stochastic illustrations use a fixed
seed 1000, explicitly not the 20-seed V4 study, and do not support head claims.
No candidate, gate, sealed data, or empirical M1 output is used. Repository
health checks are separate and include the existing development-only legacy
guard; report their outcome and data contact honestly.

Deliver a ranked REPORT, executable checks with output, a STATUS_ENTRY for
Claude, GATE_ROWS, and any requested corrections in QUESTIONS. Commit only
these handoff files on `codex/r-mts-protocol`; do not push.
