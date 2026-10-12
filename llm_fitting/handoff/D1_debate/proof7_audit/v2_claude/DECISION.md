# Proof 7 audit, version 2 (Claude) — decision

2026-10-12 · Claude Opus 5.5 (Anthropic). Three verdicts, kept apart. A favourable
answer to one implies nothing about the others. Nothing here is adopted; the owner
decides what follows.

## 1. Mathematical validity — NO GAP FOUND UNDER STATED ASSUMPTIONS

The submitted theorem survives review. Lemmas 1–7, Theorems 1 and 2, and Propositions
8 and 9 were re-derived by hand, and every identity and explicit inequality in them
that can be computed was tested numerically (43 of 43 checks, plus an independent
finite-difference solver for the rank-feedback derivative that agrees to 2e-6).

Qualifications:

- Two side remarks in §8 of the report are asserted without a written proof (transfer
  of the feature argument to the averaged-intensity model in §8.3; the regularity
  constants of the zero-variance certificate in §8.4). Neither carries the theorem.
- Nine small corrections, none of which changes the statement (`REVIEW.md` §4).
- The cited lecture notes cover weak existence from a fixed initial point only; random
  initial laws and uniqueness in law are supplied by the report's own short arguments,
  which I checked, and by a textbook I did not re-read.
- The three-day retention corollary is true and is proved in
  `REPAIRS_AND_PROPOSALS_v1.md`. It is a separate proposed design.
- This is a model's review. Lemma 3 and Lemma 4 should be read by a mathematician
  before the result is used in a paper.

## 2. Empirical applicability — NOT APPLICABLE TO ANY EXISTING DEVELOPMENT INSTRUMENT

The experiment needs daily event counts, a true birth mark, a true survival flag and
recorded zeros, with the entry law and death rate known from elsewhere.

- Reddit 2018-12 to 2022-12 and 2024-07 to 2025-01: has genuine daily event counts
  (items created per subreddit-day), which no fit in the record has used. Fails on
  birth (creation time not held), survival (not observed; inactivity is 95–98%
  temporary), zeros (no zero rows; cohort membership depends on the counts) and both
  known inputs. The modeled metric is clipped signed karma, not a count.
- Facebook, Era A: has post counts per page-day. Fails on birth by construction
  (enrolment frozen), and on survival, zeros and both inputs. The modeled metric is a
  snapshot of interaction totals attributed to the posting day.
- Instagram 2023: fails every requirement; query-censored.
- Reddit bridge and Wikipedia: sealed, not audited.

Eligible independent retained records: zero on every instrument. In addition, on the
metrics the record has modeled, Gaussian increments, constant volatility, independence
across endpoints, a single type and a stationary population are each contradicted by
recorded measurements. On the event counts they are untested.

One finding bears on any future use: at the base point the five features cannot
separate the entry mean from the OU variance and rate, so the "known entry law" cannot
be replaced by an estimate from the same features.

## 3. Numerical usefulness — NONE ESTABLISHED

- The certificate as written: radius `10^-402,641`, stability constant `10^1,326,345`,
  break-even retained sample `10^3,457,972` at the proof's suggested nominal point.
  Vacuous at all eight benchmarks.
- With the two sharpenings the proof already licenses: `10^-3,007`, `10^9,776`,
  `10^25,565`. Vacuous.
- With the true constants at the base point put into the same formulas: radius at most
  `1.2e-7`, break-even sample at least `10^48`, or `10^25` with a variance-based
  concentration step. The form of the certificate is the obstacle.
- Without any certificate, in the small-epsilon limit and assuming iid records: the
  five features need `7e9` to `6e16` retained records for ±10% of scale on the binding
  movement parameter. Rank feedback is the weak direction.
- Not run: the features at a fixed moderate `epsilon`. That would need an evaluator I
  did not build, and it could change the last bullet in either direction. It could not
  change the first three.

The problem itself is not shown to be hopeless. A lower bound from the latent path
allows the same precision on rank feedback with `75` to `1.2e4` records over fourteen
days, and an idealized high-count calculation (daily log-activity seen without count
noise) needs 2 to 34 times that. Those are reference points, not results about counts.

## Next concrete action

My recommendation, for the owner to accept or not:

1. **Stop work on tightening this certificate and on birth-cohort instruments for it.**
   The gap is structural on both the instrument side and the precision side.
2. **If this line continues, change the instrument before proving anything more:**
   established endpoints followed over fixed lags, features that difference out an
   endpoint's own level, entry law treated as unknown. `SENSITIVITY.md` §6 gives the
   reasons and the assumptions it would rest on (the transitory component mixes within
   the window; attrition over the lag is small and independent of state; transitory
   variance bounded). Lemmas 2–5 of Proof 7 would carry over.
3. **Before that proof, one measurement on development data would say whether it is
   worth posing:** whether daily event counts of established endpoints are large
   enough for contrasts to be usable, and whether the variance of their log-changes by
   lag has the shape "linear plus a saturating term". It is a measurement on
   DEVELOPMENT data and needs a short design first. It was not authorized here and was
   not run.

While this audit was running, another session committed a new proof prompt on this
branch (`5da9bfe`, "pose practical bounded-count inverse proof target"). I have not
read it. Items 1, 2, 3 and 9 of `SENSITIVITY.md` §6 are relevant to whether it should
be run as posed.

## What would change these verdicts

- **Validity:** a specific failed implication found by the other audit or by a
  mathematician. The two audits should be compared on executable quantities, not on
  agreement. Mine, for the point `a = k = v = lambda = v_0 = 1, mu_0 = 0`:
  `log10 L = 1,326,344.6`; smallest singular value of the low-order Jacobian `0.02474`;
  `chi = 0.025371`; determinant `1622.95`.
- **Applicability:** a birth registry and a survival record for a census instrument,
  and an independent measurement of the entry law. Even then the five features would
  fail on precision.
- **Usefulness:** a certified evaluator at moderate `epsilon` showing sensitivity far
  above the small-epsilon limit, together with an inverse argument that does not rest
  on one linearization.

## Files

`DESIGN.md` · `REVIEW.md` · `REPAIRS_AND_PROPOSALS_v1.md` · `INSTRUMENT_AUDIT.md` ·
`SENSITIVITY.md` · `STATUS_ENTRY.md` · `CONTACT_LOG.md` · `RUN_RECORD.md`, with the
scripts and their JSON outputs beside them.
