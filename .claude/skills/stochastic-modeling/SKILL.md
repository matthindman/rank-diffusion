---
name: stochastic-modeling
description: "Method, evidence standards and decision rights for the rank-diffusion research program: building, estimating, testing and improving the stochastic model of rank dynamics in digital attention. Use when fitting, simulating or evaluating the model; proposing, measuring or testing a mechanism; diagnosing a residual; interpreting a scorecard or an out-of-sample gate; planning experiments or an agenda; registering or running an evaluation on new data; or drafting claims for the paper — including when the request does not name this skill."
---

# Rank-diffusion: method and standards

## What the program is for

One stochastic model has to do three things at once: reproduce the stable
rank-size structure of endpoints on digital platforms, predict how
individual endpoints move through the ranks, and be as simple as it can be
without failing at the first two. The owner wants the best model on that
three-way trade-off, in a form that survives peer review.

Improving the model is the job. Propose mechanisms, question the program's
own assumptions, look for simpler forms, design sharper tests. The earlier
version of this skill told its reader to protect and seldom invent; that
instruction is withdrawn.

The discipline below exists because almost every analysis here is run by AI
agents across many sessions, and the ways such a program fools itself are
known: selecting on noise, tuning to the score, reusing held-out data,
claiming more than was measured, and one session quietly contradicting
another. Each rule blocks one of those and says which.

## Two zones

**Open — proceed without asking.** Measurements and diagnostics on
DEVELOPMENT data; competing hypotheses; opt-in experimental flags;
simulation and recovery studies; critiques of the program's assumptions;
analysis scripts and tests; drafts of protocols, agendas and paper text.
This work is reversible and needs no adopted agenda. Keep going; a wrong
measurement costs minutes.

**Gated — stop and ask the owner first.**

- Changing a default, a frozen stack, Gate v1, any registered threshold or
  margin, or a committed protocol or preregistration.
- First value-level contact with SEALED or EXPOSURE-UNKNOWN data; acquiring
  data; changing a dataset's status in the exposure ledger.
- Adopting an agenda, declaring a re-freeze, or promoting an opt-in change
  into a frozen stack.
- Changing paper claim language or the binding claim set.
- Deleting or rewriting anything already in MODEL_STATUS, a protocol or a
  ledger.
- Package-track changes, pushes, and anything irreversible outside the repo.

These are gated because they cannot be undone or because they spend
something that can be spent once. The owner's instruction in the
conversation outranks this file. When the owner asks for a gated action
directly, say in one or two lines what it spends or changes and ask once;
then do what the owner decides.

## Start of a session

1. Read `llm_fitting/PROGRAM_STATE.md`, then the newest dated §2 sections
   of `llm_fitting/MODEL_STATUS.md` that bear on the task (the page names
   them; recent data-operation entries can be skimmed). MODEL_STATUS is the
   record; its newest sections supersede older ones, and every number
   lives there.
2. Read `llm_fitting/EXPOSURE_LEDGER.md` before opening any data file. The
   sealed folders are listed in `llm_fitting/SEALED_PATHS.json`; nothing in
   them is looked at until the owner says so in the conversation.
3. Run `scripts/health_check.sh` before changing code and before every
   commit. A session that only reads and advises can skip it.
4. Load the reference for the task:

| read | before |
|---|---|
| `references/frozen-specs.md` | running or interpreting any fit, card or gate; proposing a change to a stack |
| `references/pitfalls.md` | estimating, building a cohort, diagnosing a residual, touching data, launching a long run |
| `references/evidence-standards.md` | designing an experiment, proposing a model change, writing a protocol, writing up a result or paper text |
| the `data-intake` skill | acquiring, building, validating or registering any panel |
| the `model-status-authoring` skill | writing a MODEL_STATUS entry |
| the `repo-maintenance` skill | committing, moving files, CI, package trees |

## The model in one page

Log-activity of entity *i* in week *t*, common per-period factor removed:

```
X_it = h_it + ξ_it (+ ξ2_it) + ε_it
  h   OU "home": slow reversion κ(z) toward the entity's own home level,
      innovation σ_perm(z)
  ξ   fast transitory AR(1): φ, σ_trans(z), Student-t innovations
  ξ2  optional medium AR(1), long panels only
  ε   measurement noise σ_obs(z), pinned or bounded from the within-week
      daily-replication floor (Spec-B)
```

One persistent entity amplitude v_i (lognormal, spread s) multiplies the
components; b = 1, the same amplitude for permanent and transitory
volatility, is the main law. A stationary common level, Gabaix-style
rebirth at the bottom, and a pre-registered top-coverage universe (top-K by
absence-penalized permanent rank, buffer 4K) complete it. z is the
permanent-rank band; parameters are step profiles in z, about 500
moment-estimated values over about 55 knots.

Language that is binding because reviewers settled it (MODEL_STATUS §1):

- This is entity-home OU with rank-dependent variances. Homes are seeded
  from the measured ladder, which is the one input taken as given. Structure
  claims are "conditional reproduction and maintenance of the ladder",
  never its genesis.
- The gate is a pooled-moment movement gate against a train-window
  historical-mobility baseline. Displacement is survivor-conditional. Its
  "coverage" is a median-consistency check, not predictive coverage.
- Head shares are "within the recorded top-M", never whole-universe.
- "Parsimonious latent architecture with flexible nonparametric rank
  profiles", never "a simple model" unqualified.

Code: `llm_fitting/minimal_rankdiff.py` (estimation, simulation, card),
`llm_fitting/rankdiff_kalman.py` (filter, gate), and the additive tools
beside them.

## How claims are earned

1. **Held-out movement decides adoption.** Gate v1 (rolling origin, at least
   five splits, defined in `frozen-specs.md`) is the adoption criterion.
   In-sample cards describe and localize; they do not justify a change,
   because a flexible model can match any card. One split is never
   reported: a flattering 0.081 on one split was 0.29 ± 0.16 over five.
2. **Every parameter comes from a declared moment or an independent
   instrument.** Nothing is tuned to a score, because a tuned parameter
   does not transport and identification is what this paper adds to its
   literature. When two conventions are possible, settle it with outside
   information, as Spec-B did for σ_obs.
3. **Measure before building.** Each hypothesis gets a cheap direct
   measurement before any model code. Suspect, in order: an identification
   failure, a scoring or measurement artifact, Monte Carlo noise, and only
   then missing structure. About two in three "structural" residuals here
   turned out to be one of the first two.
4. **Write the prediction down before looking.** Direction and rough size,
   including what must not get worse. Score every prediction in the
   write-up. Failed predictions are findings and are reported with the
   same prominence.
5. **One change at a time,** against the recorded baseline, with common
   random numbers and the same universe.
6. **Noise is part of every number.** Bands on every headline number; 20
   or more seeds for anything about the head; splits, seeds and bootstrap
   draws stated.
7. **Data confirms once.** DEVELOPMENT data can never confirm. SEALED data
   is first read by the intake gate of a protocol committed beforehand, and
   only once the owner has said the dataset may be opened. Reading it
   earlier, even a top-ten list, spends the confirmation.
8. **The gate is a finite resource.** Its split-to-split SD is about the
   size of the differences it judges, so each extra evaluation on the same
   panels makes a lucky pass more likely. Log every evaluation in
   `llm_fitting/GATE_LEDGER.md`, used or not.
9. **Committed results stay reproducible.** New behavior goes behind an
   opt-in flag with gated random streams, so flag-off runs are
   bit-identical. The legacy guard and both suites pass at every commit.
10. **Parsimony is measured.** Count what a change adds. Prefer changes
    that remove freedom: an identifying moment, a restriction, a functional
    form in place of bands. A new latent component needs a measured moment
    that it alone can produce. A simpler model that is practically
    equivalent is a result worth reporting.
11. **A mechanism is established by recovery, not by fit.** Before reading
    a fitted component as real, show the pipeline recovers it from data
    simulated with it and does not find it in data simulated without it.
12. **Agreement between models is not evidence.** Strong models share
    blind spots. What counts is an executable check, an independent
    re-implementation, or data.
13. **The record is written before the session ends,** append-only, with
    commands, and says plainly what failed, what was killed and what was
    not run. Paper language follows the binding claim set (MODEL_STATUS
    §6), with each claim labelled by its evidence tier.

## Easy to get wrong

- Estimate and stratify by permanent rank (time-averaged, absent weeks
  counted at the floor), never by current rank. Current rank selects
  transient spikers and has inflated σ about threefold.
- Facebook is `fb_ranked_weekly_cutdown.parquet` or the SSD Era A slice,
  nothing else. Instagram raw panels are a negative control; the only
  modeled use is the §2z-c censoring apparatus.
- Each panel has two frozen stacks, one for structure and one for
  movement. Long-difference moments belong on long panels only.
- A departure "crossing" is an entity still observed below rank K, not one
  seen to return. Gate "coverage" is not predictive coverage. Recorded
  "CRPS skill ≈ 0" is about pooled displacement, not endpoint forecasts.
- Asked to reproduce or check a recorded number, run the recorded command
  and report what it gives before comparing. Reference values are for
  comparison afterwards, not targets. Where independence matters, give a
  fresh session the command and not the number.
- "Report the range across alternatives" is a specification search unless
  the alternatives were fixed first.
- Stopping to say "this cannot be done as specified" is a good outcome. It
  is always better than a pass obtained by changing the test.
- The registered interpreter runs pandas 2.x. Do not upgrade it.

## Improving the model on development data

1. Confirm the residual is real: bands, the right stack, no period-0
   artifact, data and simulation built the same way.
2. List competing explanations, cheapest measurement first, and what each
   outcome would license. Check `PROGRAM_STATE.md` for ideas already
   refuted; re-proposing one needs new evidence, named.
3. Run the measurements on empirical and simulated panels through one code
   path. Record them, including the ones that kill your favorite.
4. For a surviving candidate, write a short dossier before coding: the
   mechanism; the moment that identifies it; recovery evidence; predictions
   with a must-not-worsen list (head share, VR13, departure rate, the
   gate); parameters added or removed.
5. Implement behind an opt-in flag with tests: exact recovery, noisy-panel
   recovery, flag-off bit-identity.
6. Evaluate on every platform, paired seeds, both stacks where they apply.
   Log the gate runs.
7. Verdict, by rules stated before the run. A candidate is recommended to
   the owner only if its own target prediction holds within bands wherever
   it applies, the gate is not worse on any platform by more than the
   margin written in the dossier, and it adds no parameter without an
   identifying moment. The owner has not set a standing margin. Until one
   is set, use the convention of the registered evaluations (0.05 in gate
   relative error, 20 reps, 2,000 bootstrap draws) and say in the write-up
   that it is that convention, not an adopted threshold. Ties go to the
   simpler model. A change that helps one panel and
   hurts another is scoped to where it helps, not defaulted. Stacks change
   only when the owner re-freezes.

## Evaluating on new data

New data is evaluated once, under a protocol committed before contact.
`evidence-standards.md` lists what the protocol must contain and the
`data-intake` skill gives the intake procedure. The shape is fixed:
exposure audit; protocol and runners committed and dry-tested on synthetic
data; fail-closed intake gate; registered evaluations in order, one pass
each; every deviation disclosed; then the dataset becomes DEVELOPMENT. A
failed registered prediction is a finding about where the law holds. It is
never a prompt to refit, and nothing learned from the new data may choose
between candidates.

## Working with other sessions and models

Two vendors' agents work in this repository, sometimes at once. Check
`git status` and file times before writing a shared file; give drafts an
author suffix; append to MODEL_STATUS only after reading what the other
session appended. When delegating, verify the brief against MODEL_STATUS
first: an error in a fact sheet propagates into everything written from
it. Use the other model for independent derivation and blind
re-implementation before outcomes are seen, and for adversarial review
after. Review rounds are budgeted; reviewing an unchanged result again is
not progress.

## Revision note

Rewritten 2026-10-11 for current frontier models (MODEL_STATUS §2z-ak):
purpose and decision rights first, references split out, project state
moved to `PROGRAM_STATE.md`, and rules 7, 8, 11 and 12 added. Frozen
specifications, Gate v1 and the registered protocols are unchanged.
