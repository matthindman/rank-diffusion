# Proof 7 audit, version 2 (Claude) — design, written before any sensitivity or conditioning number was computed

2026-10-12, about 23:40 JST. Claude Opus 5.5 (Anthropic), Claude Code desktop session.
Working copy `/Users/hindman/Documents/GitHub/rank-diffusion-codex-debate`, branch
`codex/d1-debate`, HEAD `93353608f9c2bc9da300046fd03d77155819ca18`.

## What this is and is not

An independent review of `proof_prompts/PROOF_7_RESULT.md`, a documentary audit of what
the development instruments observe, and a deterministic evaluation of the theorem's
constants and of the sensitivity of its observation map. I did not write Proof 7 and had
not seen it or its derivation before this session.

A complete audit by another session already sits in `proof7_audit/` (files dated
22:26–22:51 today). I have seen its file names only and will not open any of them until
this version is finished, so that the two audits are independent. Everything of mine goes
in `proof7_audit/v2_claude/`.

State at the time of writing: the proof, its prompt and the referee prompt have been read
in full, and the main chain has been checked once by hand. No numerical value of any
constant, Jacobian, singular value or sample size has been computed. No data file has
been opened.

Reviewed artifacts (SHA-256):

| file | sha256 |
|---|---|
| `proof_prompts/PROOF_7_FINITE_SAMPLE_IDENTIFICATION.md` | `1252a732efbaff3810e359b376510a1ea4afbc316815b24b7767dbc78ae49f3b` |
| `proof_prompts/PROOF_7_RESULT.md` | `ba3b4ef140e13c244002bc39cbd61abc05d06b3d5c9377a760594c194be60be5` |
| `proof_prompts/PROOF_VERIFIER.md` | `4d3a5236e59ebaf321f76bddd733667b989002f2b831f0de8c5c377eb40f7c63` |
| `proof_prompts/PROOF_7_REVIEW_APPLICABILITY_PROMPT.md` (the brief) | `f9d49ff1cddaa89cee24bf00276e0c7576fe5a13b1cfad2639942a4f382220f3` |

The brief reached me as pasted text that matches the committed prompt apart from its
headings and three sentences (the request for a fresh session, the author-disclosure
sentence, and "saving this prompt does not launch it").

Governing files: `AGENTS.md` and the four skills are identical on this branch and on
`main`. `PROGRAM_STATE.md`, `MODEL_STATUS.md` and `EXPOSURE_LEDGER.md` are older on this
branch (it lacks 861 lines of the record), so I read them from `main` at `5384430`, which
is the canonical copy.

## Environment and limits

- Registered interpreter (`/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`)
  has numpy and scipy but neither mpmath nor sympy. It is not changed. Numerical work runs
  in a scratch virtual environment outside the repository
  (Python 3.11.6, numpy 2.4.6, scipy 1.17.1, mpmath 1.3.0, sympy 1.14.0).
- Machine: 10 cores, 16 GB. Every script is deterministic, single process, and meant to
  finish in under five minutes. Nothing is run through the research model, no stochastic
  simulation, no fit, no gate evaluation, no health script.
- No agents spawned, no messages to other sessions.

## Stage B data-contact plan

Documentary first: ledger, `DATA_INVENTORY.md`, `DATA_PHASE2_REPORT.md`,
`instrument_eras.py`, builder sources, and the record. The WD drive is unplugged by the
owner's decision, so raw archives are unavailable and nothing is substituted for them.
The Wikipedia tree on T9 and the Reddit bridge are sealed and are not touched in any way.

Value-level contact is made only if a documentary question stays open, and then at most
this: for one DEVELOPMENT daily file per platform and metric, the parquet footer schema
and three scalars of the metric column (share of rows equal to zero, share of
non-integer values, minimum). Any such contact is logged in `CONTACT_LOG.md`.

## Stage C: what is evaluated

### Benchmarks (all theoretical; fixed now)

Units are days and natural-log intensity, as in the proof. The proof fixes only the
nominal triple `(a*, k*, v*)` and suggests `a* = k* = v* = 1`; it leaves `lambda`, `mu_0`
and `v_0` as known inputs. No independently measured value of any of the six exists in the
canonical record in these units: the recorded parameters belong to a different model
(weekly `log1p` metric values, a top-K universe, no birth or death registry), and I do
not translate them by name. The "slow" rows below are illustrative orders of magnitude
only. Their one link to the record is qualitative: recorded lasting variances are of
order 0.01 per week or less (MODEL_STATUS §2z-bf), so one unit of log-variance per day is
not a plausible platform scale. **Platform relevance of every benchmark is therefore not
established.**

| id | a* | k* | v* | lambda | mu_0 | v_0 | label |
|---|---|---|---|---|---|---|---|
| B1 | 1 | 1 | 1 | 1 | 0 | 1 | unit (the proof's suggested triple) |
| B2 | 1 | 1 | 1 | 0.05 | 0 | 1 | unit, slow death |
| B3 | 0.01 | 1 | 0.25 | 0.01 | 0 | 1 | slow movement |
| B4 | 0.001 | 1 | 0.25 | 0.01 | 0 | 1 | slower movement |
| B5 | 0.01 | 1 | 0.25 | 0.01 | 3 | 1 | slow, high counts |
| B6 | 0.01 | 1 | 0.25 | 0.01 | -3 | 1 | slow, low counts |
| B7 | 0.01 | 0.1 | 0.25 | 0.01 | 0 | 1 | slow, slow OU |
| B8 | 0.01 | 10 | 0.25 | 0.01 | 0 | 1 | slow, fast OU |

The base point of each is the proof's `x* = (g, v, a, k, d) = (0, v*, a*, k*, 0)`. All
eight are reported whatever they show. No benchmark is added after results are seen
unless it is labelled post hoc.

### C1. The theorem's own certificate

Evaluate (3.1)–(3.8), (5.2), (5.4), (5.5), (6.1), (6.4), (7.2)–(7.4) and (1.6) exactly as
written, at each benchmark, in mpmath at 60 significant digits, and report base-10
logarithms to four significant figures: `D`, `s`, `epsilon`, `r`, `L`, the movement
diameter `4r` that the parameter restriction alone guarantees, the retained sample size
of (7.3) at `alpha = 0.05` for `t = r`, the smallest `N` at which the statistical term of
(7.2) falls below `4r`, and the retention factor. A value that underflows a float is
reported as a logarithm, not as zero.

### C2. Sharpenings that stay inside the submitted proof

Two changes that the submitted argument already licenses, evaluated the same way and
labelled as variants of the certificate, not as the theorem:

- V-H4: horizon 4 in place of 15 in every envelope, since the five functions use ages
  below 4 only.
- V-H4J: V-H4 and additionally `|J_t| <= 2 sup|B|` in place of `j_0 = 1 + k_+ H`.

### C3. The exact low-order Jacobian at the base point

At `d = 0` the auxiliary moments are Gaussian integrals. Compute the 5-by-5 matrix `A*`
by Gauss–Legendre quadrature, the `d` column from the tilt formula, with the phase
integrated explicitly. Report singular values in raw units and in the declared scales
`(s_g, s_v, s_a, s_k, s_d) = (a*, v*, a*, k*, 2 sqrt(a* lambda))`, the last being the
half-width of the admissible range of `d`. Report the full five-parameter conditioning
and the movement sensitivity after projecting the movement columns off the nuisance
columns. These are numerical estimates. A claim of a verified nonsingular derivative is
made only where a rigorous entry enclosure is produced, and a claim of an inverse on a
region only with a bound on derivative variation there.

Checks on the evaluator, each able to fail: the proof's closed forms (4.1), (4.3), (4.4)
against quadrature with the phase integrated numerically; the determinant identity of
Lemma 6 against the determinant of the full matrix; a version with a separate phase per
bin and a version with mean-one `exp(Z)`, which must both disagree with the correct one;
the tilt identity of Lemma 5 against a direct two-dimensional Gaussian quadrature; and
the `d` derivative of the day means against a finite-difference solution of the backward
equation with the self-consistent density recomputed at each `d`. At a nonzero `d` the
same solver gives the `d` derivative with the density recomputed and with it frozen, to
show the size of the self-consistent term. Quadrature orders are doubled until the
relative change is below `1e-9`; the PDE grid is halved once. Agreement across
resolutions is a diagnostic, not a certificate.

### C4. Sampling precision in the small-epsilon limit

As `epsilon -> 0` the five scaled features become the raw moments
`(E C_1, E C_2, E C_3, E C_1 C_2, E C_2 C_3)`. Compute their exact covariance at the base
point (Gaussian integrals to fourth order) and the delta-method covariance
`J^{-1} Sigma J^{-T} / N`. Report the retained `N` at which the 95% half-width equals 10%
of each declared scale, and the registered births that implies. This is an asymptotic
estimate under the theorem's iid sampling idealization. It is not a finite-sample
guarantee and it is not the theorem's estimator at a fixed positive `epsilon`.

A rigorous companion is also evaluated: Chebyshev's inequality with the same covariance
gives a finite-sample bound on the scaled feature error that does not contain
`epsilon^{-2}`. Combined with Theorem 1's inverse bound it is a restricted variant of
Theorem 2 (weighted distance, `1/alpha` in place of `log(1/alpha)`), reported as such.

### C5. Declared variants, kept apart from the five-feature theorem

Fixed now, each evaluated with the tools of C3–C4 and reported whether or not it helps:

- W1: three-day retention with the same five functions (the corollary Stage A is asked
  to check).
- W2: `(k, v)` known from an outside instrument; movement Jacobian in `(g, a, d)` only.
- W3: `g` known as well; `(a, d)` only.
- W4: the fourteen daily means added to the two adjacent cross moments (sixteen raw
  moments, efficiently weighted), under the fourteen-day cohort.

Fixed positive `epsilon` (0.01, 0.1, 0.5) is evaluated only if a deterministic evaluator
of the bounded features with a stated error estimate is completed within the limits
above; otherwise it is reported NOT RUN.

### What would count as each outcome

- Useful precision established: a certified or carefully estimated bound that improves on
  the supplied parameter region at a retained sample size that an audited instrument
  could supply.
- Vacuous: the statistical term does not fall below the region's own diameter until `N`
  exceeds any conceivable count, or the region is smaller than any prior knowledge could
  justify.
- Not established: the numbers depend on a step that could not be certified; the step is
  named.

Stage A's verdict gates every Stage C conclusion: if a load-bearing error is found, the
Stage C numbers are labelled conditional.
