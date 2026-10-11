# Evidence standards

Read this before designing an experiment, proposing a model change, writing
a protocol for new data, reviewing another session's work, or writing up a
result or paper text.

These are the standards a careful referee will apply, plus the ones that
apply because agents did the analysis. Where a standard is not yet met by
tooling, this file says so; the standard still holds. Sources and fuller
argument are in `llm_fitting/lit_review/research_notes/` (two folders: "Rank
dynamics modeling best practices" and "Skill design for agent-run
research"). Citations there carry verification tags; re-read a primary
source before citing it in the paper.

## Contents

1. What every reported number carries
2. Specifications, seeds and stopping
3. Identification and recovery
4. What Gate v1 does and does not show
5. Baselines and proper scores (supplementary)
6. The candidate dossier and the adoption rule
7. A protocol for new data
8. Verification and review
9. Provenance and AI-use records
10. Paper claims and the reporting checklist

## 1. What every reported number carries

- **A status.** Each piece of work is RAN-OK, RAN-FAILED, PARTIAL or NOT
  RUN, stated in the session report and the record. Agent-run research
  fails most often by reporting incomplete work as done; a plain status
  with a pointer to the output is the remedy.
- **A trail.** The command, the commit it ran on, the input files, the
  seeds, and the log or output file. A number that cannot be traced this
  way is not recorded. Numbers taken from a summary, a memory file or
  another agent's report are re-read from the artifact before they are
  used.
- **Uncertainty of the right kind.** Seed variability, split-to-split
  spread and sampling uncertainty are different things and are labelled as
  what they are. Use entity and week-block bootstraps for panel statistics;
  observations within a week share common shocks, and N × T is not a count
  of independent observations.
- **Its denominator.** How many specifications, hypotheses or seeds were
  tried to get it.

## 2. Specifications, seeds and stopping

- Declare the grid before running it and report the whole grid. A request
  phrased as robustness — "report the range across alternatives", "the
  best case", "an upper bound" — is a specification search unless the set
  was fixed beforehand; agents that refuse explicit p-hacking have been
  shown to do it readily under that framing. Never report the best of
  several.
- Fix the seed count and the stopping rule before the first run. Do not
  add seeds because the result is close.
- Do not substitute a smaller sample, fewer seeds, a shorter window or a
  synthetic stand-in for what was specified without saying so in the
  result's first line. A silent simplification is a deviation.
- Stopping and reporting "this cannot be done as specified" is a
  successful outcome. Say what blocked it, what was tried, and what
  evidence there is. It is always preferable to a result that passes by
  changing the test.
- An evaluator and a result that benefits from changing it never land in
  the same commit.

## 3. Identification and recovery

- **Per-parameter identification.** For each parameter, be able to say
  which moments carry it. The minimum-distance solves are linear, so the
  sensitivity of estimates to moments is available in closed form (the
  Andrews–Gentzkow–Shapiro matrix is the NNLS pseudo-inverse on the active
  set). Report the share of bands sitting on an NNLS corner.
- **Weak identification is checked before interpreting a difference.** κ
  on short panels and φ near zero are the known cases. A shift between
  eras is a finding only if it exceeds what the estimator does on data
  simulated with no shift at those panel lengths.
- **Recovery studies follow ADEMP:** aims, data-generating mechanisms,
  estimands, methods, performance measures, with replication counts and
  Monte Carlo standard errors. Simulate through the real pipeline:
  universe selection, permanent-rank estimation, aggregation, the daily
  noise floor.
- **Model recovery before mechanism claims.** Plant each candidate
  mechanism, and its absence, and show the diagnostic statistics tell them
  apart at the real N and T. A statistic that cannot separate the
  candidates in simulation cannot adjudicate them on data.
- **Ablations are refits.** Setting a parameter to zero in a fitted model
  tests an intervention; whether a simpler model suffices is answered by
  refitting the simpler model.

## 4. What Gate v1 does and does not show

Gate v1 stays the adoption criterion. Report its limits beside its results
rather than working around them:

- It scores pooled displacement moments against the train window's own
  displacement distribution. Under stationarity that baseline is calibrated
  by construction, so a tie is the expected result for any adequate model.
  The model can win only when mobility shifts between train and test,
  which is what the registered confirmation showed.
- It is survivor-conditional, so it cannot see the departure deficit.
- Its medians cannot see tail under-dispersion.
- Relative error rewards a forecast shaded low, and `MOM_FLOOR` selects
  moments on realized values.
- Five overlapping origins are not five independent experiments. A sign
  test across five cannot reach the 5% level.
- "At par" is a statement about an interval inside a margin fixed in
  advance. It is not the absence of a detected difference.

## 5. Baselines and proper scores (supplementary)

These are descriptive unless a protocol registers them. They never
re-adjudicate a recorded verdict.

- **Endpoint-level predictive scores.** The gate and `--dist-scores` say
  nothing about whether the model knows which endpoints will move. That
  claim needs per-entity predictive samples scored with a proper rule
  (CRPS on log-rank, with exit treated as a censored state; Brier scores
  for "outside the top-k within h weeks"), finite-ensemble corrected, and
  calibration by horizon and stratum.
- **Baselines a referee will expect:** no-change; the historical-mobility
  law already in the gate; a rank-bin transition kernel; a per-entity local
  level; a pooled shrunk AR; the two-parameter displacement-plus-
  replacement model of Iñiguez et al. on its own statistics; and a
  flexible learner as a ceiling on what is predictable. State what each
  can and cannot be compared on.
- **Paired inference.** Compare on the same origins, seeds and universes,
  with block-level uncertainty.

## 6. The candidate dossier and the adoption rule

Before any model code for a proposed change, write:

1. The residual it targets, with bands, and the measurements that pointed
   to it.
2. The mechanism and the moment that identifies it. A new latent component
   needs a moment it alone produces.
3. Recovery and model-recovery evidence (§3).
4. Predictions: the target statistic, and a must-not-worsen list (head
   share, VR13, the departure rate and its composition, the gate on every
   platform), each with direction and rough size.
5. Parsimony accounting: parameters added and removed, and effective
   degrees of freedom for any smoothed profile.
6. The comparison margin and seed count for the gate.

Recommend the change to the owner only if the target prediction holds
within bands wherever it applies, the gate is not worse on any platform
beyond the margin, and no parameter lacks an identifying moment. Ties go to
the simpler model. Report the candidate on a complexity-against-performance
table with every failed and tied variant. The owner decides adoption and
any re-freeze. Mechanism verdicts and adoption verdicts are recorded
separately.

## 7. A protocol for new data

Committed before any value-level contact, with runners tested on synthetic
data. It fixes:

- The exposure audit and what it found.
- The entity key, the metric, the window, and the instrument's dated
  change log as registered candidate boundaries, with the expected
  direction of each.
- The intake gate's invariants and the universe rule, including any cap.
- Which model generation and which frozen parameters are being tested, by
  commit.
- Three transport arms kept distinct: frozen parameters; a narrowly
  specified recalibration on an allowed training segment; the same
  architecture refit. They answer different questions about universality.
- Each registered evaluation with its statistic, command, seeds, pass rule
  and margin. Margins are numbers fixed from already-seen data.
- Predicted misfits. Say in advance where the model is expected to fail on
  this system and how that will be scored.
- What is held back: which later blocks or sibling datasets stay sealed for
  the next model generation.
- What the new data will not be used for: choosing between candidates,
  tuning, or setting any threshold.
- One pass per item, the deviation table format, and the status of
  anything that cannot be adjudicated.

Amendments are legal only before contact and may tighten, never loosen.

## 8. Verification and review

- **Verify blind.** A reproducer or verifier gets the code, the data and
  the command, and produces its number before seeing the recorded one.
  Agents shown the expected value have been measured filling gaps with it.
- **Reviewers get artifacts.** Code, logs and commands, not the write-up
  alone; a reviewer with logs and code catches far more than one with the
  paper only. Record which model reviewed and what it was shown.
- **Independent re-implementation** of the estimator and the gate scorer by
  the other vendor's model, checked on fixtures, is the strongest
  cross-model check available. Agreement between models on a judgment call
  is weak evidence: when two strong models are both wrong they usually
  agree.
- **Known-answer and mutation checks.** A deliberately broken model must
  fail the gate; a synthetic panel with planted parameters must be
  recovered.
- **The owner's checks are part of the evidence.** Record each owner
  decision and each result a human re-ran or checked. Human accountability
  for every claim is what the journals require, and this record is what
  shows it.

## 9. Provenance and AI-use records

Kept now, so an honest methods section can be written later:

- Each MODEL_STATUS entry names the model and vendor that did the work,
  the date, and the commit it started from. The commit identifies the
  skills in force.
- Owner decisions are recorded as entries of their own.
- Every gate evaluation is in `GATE_LEDGER.md`; every data contact is in
  `EXPOSURE_LEDGER.md`.
- Deviations from a registered plan are tabled: planned, done, why,
  decided before or after seeing results, effect.
- Corrections and retractions stay visible in the record.
- No citation enters paper text from model memory; each is resolved to its
  source and the supporting passage located.

Not yet built, and worth building before submission: a machine-written run
ledger the agents cannot edit; a manifest linking every published number to
its run; hash-pinned evaluator files with the hash stamped into results.
`PROGRAM_STATE.md` lists them as open decisions.

## 10. Paper claims and the reporting checklist

- Paper language follows the binding claim set (MODEL_STATUS §6, addendum
  of 2026-07-12). Interpretive sentences beyond it need the owner's
  sign-off.
- Every result is labelled with its tier: registered confirmation, delayed
  registered sensitivity, exploratory diagnostic, or development negative
  result.
- Scope qualifiers travel with the claim: platform, era, "of tracked
  activity" on censored instruments, "within the recorded top-M",
  survivor-conditional.
- Scorecard rows are labelled targeted or untargeted, with comparator
  columns; no count is headlined.
- A mechanism is called a hypothesis unless a registered test supports it.
- The consolidated checklist for the methods, SI and reproducibility
  package (model description, simulation design, out-of-sample evaluation,
  data, code and environment, registrations and deviations, AI-use
  disclosure, statistical reporting, independent verification) is in
  `Skill design for agent-run research/rigor_for_agent_run_science.md`,
  final section. Journal AI policies change; re-read the venue's current
  policy before drafting the disclosure.
