# Entry for MODEL_STATUS.md — ready to append on `main`

_Notes for whoever integrates this, not part of the entry. (1) Take the next free
section letter. (2) The reviewed result, `PROOF_7_RESULT.md`, and its own status entry
are untracked files in the debate working copy and are not yet in the record; this
entry should follow the one that records them, or say that it precedes it. (3) A second
audit of the same proof by another session is in `handoff/D1_debate/proof7_audit/`; this
entry was written without reading it. (4) The contact rows are in `CONTACT_LOG.md`.
(5) PROGRAM_STATE needs one clause if the owner accepts the recommendation; nothing in
it changes otherwise._

---

## 2z-??. 2026-10-12 — REVIEW of Proof 7 (five bounded daily-count statistics, finite-record identification), at the owner's request: no gap found under its stated assumptions; no existing development instrument supplies its experiment; its certificate is numerically vacuous and its five features are weak on rank feedback. PLANNING ONLY, nothing adopted; one bounded schema-and-scalars check on DEVELOPMENT panels; no fit, simulation or gate evaluation

_Provenance: Claude Opus 5.5 (Anthropic), Claude Code; 2026-10-12; work done in the
debate working copy on `codex/d1-debate` starting from `9335360`, reading the canonical
records from `main` at `5384430`. Requested by the owner by relaying the brief
`proof_prompts/PROOF_7_REVIEW_APPLICABILITY_PROMPT.md`. The reviewer did not write
Proof 7 and had not seen it before. Design saved before any sensitivity number was
computed (`handoff/D1_debate/proof7_audit/v2_claude/DESIGN.md`)._

**Tier.** Review of a mathematical result, with deterministic calculations on the
stated model and a documentary audit of instruments. No empirical claim. Agreement
between this review and any other model's is not evidence.

**What was read.** `PROOF_7_RESULT.md` (SHA-256 `ba3b4ef1…e60be5`) in full, against its
prompt and the referee prompt; the exposure ledger; `DATA_INVENTORY.md`,
`DATA_PHASE2_REPORT.md`, `instrument_eras.py` and the four panel builders; the record's
entries on instrument eras, censoring, departures, tails and the annual cycle (§2g
addendum, §2z-c, §2z-o, §2z-aa, §2z-ac, §2z-ae, §2z-au, §2z-aw, §2z-bf); pages 1–5 and
9–12 of Scheutzow's lecture notes for the Girsanov, Novikov and weak-solution
statements.

**1. Mathematical validity: NO GAP FOUND UNDER STATED ASSUMPTIONS.** Lemmas 1–7,
Theorems 1–2 and Propositions 8–9 were re-derived by hand. Forty-three executable
checks of the identities and explicit inequalities pass (closed forms to 1e-15, the
determinant identity to 1e-14, two planted errors detected), and an independent
finite-difference solver that recomputes the self-consistent density reproduces the
rank-feedback derivative of the day means to 2e-6. Two side remarks in the report's §8
are asserted without written proofs and are not load-bearing. Nine wording or looseness
corrections, none changing the statement. The self-consistent term is 0.5% of the
rank-feedback derivative at the edge of the proof's parameter box and 23–24% at half
the admissible bound. A corollary was proved: retaining newborns to the end of the
third complete day instead of the fourteenth leaves the law of the three counts used
unchanged and multiplies retention by `exp(11 lambda)`; it is a separate proposed
design. A model's review is not certification.

**2. Empirical applicability: none of the existing development instruments.** The
experiment needs daily event counts, a true birth mark, a true survival flag and
recorded zeros, with the entry law and death rate known. Reddit 2018–2022 and
2024-07..2025-01 carry genuine daily event counts (items created per subreddit-day) that
no fit has used, and fail on birth, survival, zeros and both known inputs; the modeled
metric is clipped signed karma. Facebook Era A carries post counts and fails on birth
by construction (frozen enrolment) and on the rest; its modeled metric is a snapshot of
interaction totals. Instagram fails every requirement. The Reddit bridge and Wikipedia
were not audited (sealed). Eligible independent retained records: zero. Measured in one
bounded check: no file contains a zero row (minimum item or post count is 1); 0.19% of
comment subreddit-days in 2019-06 have negative karma. At the proof's base point the
five features see the entry mean, the OU variance and the OU rate through two
quantities only, so the known entry law cannot be replaced by an estimate from the same
features.

**3. Numerical usefulness: none established.** At the proof's suggested nominal point
(`a* = k* = v* = 1`, with `lambda = 1`, `mu_0 = 0`, `v_0 = 1` chosen as a theoretical
benchmark) the certified ball has radius `10^-402,641`, the stability constant is
`10^1,326,345`, and the sampling term of the error bound falls below what the ball
already guarantees only for more than `10^3,457,972` retained records. Two sharpenings
the proof already licenses give `10^-3,007`, `10^9,776` and `10^25,565`. With the true
constants at the base point in the same formulas the radius is at most `1.2e-7` and the
break-even sample at least `10^48` (`10^25` with a variance-based concentration step):
the form of the certificate is the obstacle. Without a certificate, in the
small-epsilon limit under iid sampling, the five features need `7e9` to `6e16` retained
records for a 95% half-width of 10% of scale on the binding movement parameter over
eight declared benchmarks; rank feedback is the weak direction. For reference (post
hoc): the Fisher information in the latent path allows that precision on rank feedback
with 75 to `1.2e4` records over fourteen days, and an idealized calculation on daily
log-activity without count noise needs 2 to 34 times that. Platform relevance of every
benchmark is not established.

**Proposed (nothing adopted).** Stop tightening this certificate and stop work toward
birth-cohort instruments for it. If the line continues, pose a different instrument
first: established endpoints over fixed lags, features that difference out an
endpoint's own level, entry law unknown; the assumptions it would rest on are listed in
`SENSITIVITY.md` §6. One measurement on development data would say whether that is
worth posing; it was not authorized here and was not run.

**Not run.** The features at fixed positive `epsilon` (no evaluator built). A rigorous
enclosure of the low-order Jacobian. Any simulation, fit, gate evaluation or health
script. Any contact with sealed data. One run was killed for slowness and rerun after a
caching fix; nothing was read from the killed run.

**Deviations from the design** are tabled in `RUN_RECORD.md` (six, none affecting a
reported figure; five post hoc calculations added after the owner asked mid-task for
assumptions that would tighten practical use, labelled as such).

**Commands** (from `llm_fitting/handoff/D1_debate/proof7_audit/v2_claude/`, scratch
environment except where noted): `stageA_checks.py`, `pde_check.py`, `certificate.py`,
`sensitivity.py`, `info_bounds.py`, `convergence_check.py`, `posthoc_variants.py`;
`instrument_scalars.py` with the registered interpreter. Outputs are the JSON files of
the same names.

**Ledgers.** No gate evaluation. Data contact: `CONTACT_LOG.md` in the same folder (six
rows, all DEVELOPMENT, footers and scalars only; no status changed).

**Files.** `REVIEW.md`, `REPAIRS_AND_PROPOSALS_v1.md`, `INSTRUMENT_AUDIT.md`,
`SENSITIVITY.md`, `DECISION.md`, `RUN_RECORD.md`, uncommitted, for the owner's review.
