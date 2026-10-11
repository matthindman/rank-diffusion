# V1 — The gate as a measuring instrument

Branch `codex/v1-gate-instrument`. Handoff folder
`llm_fitting/handoff/V1_gate_instrument/`. Working rules:
`llm_fitting/coordination/PROTOCOL.md`.

## Why

Gate v1 decides adoption, and nobody has measured what it can resolve. The
ladder run of 2026-10-12 (MODEL_STATUS §2z-ap) found three things that make
this urgent:

- the four reference gates in `frozen-specs.md` §2 were recorded at the
  command-line default of 3 simulations per split; the same stacks at 20
  score 0.020–0.063 lower, and the cause was not isolated;
- after the first two or three model components, the paired change from
  one rung to the next sits inside its split-to-split SD on Facebook;
- on the long submissions panel one moment (the change rate at rank 1) is
  zero in three of five test windows and dominates the score.

The owner has deferred setting a margin for comparing a candidate with the
incumbent until the noise is measured (§2z-am). This task supplies that
measurement. It involves no candidate model and no judgment about one.

## Read first

`.agents/skills/stochastic-modeling/SKILL.md` and its
`references/frozen-specs.md` (§2 stacks and commands, §4 Gate v1) and
`references/evidence-standards.md` (§1, §2, §4);
MODEL_STATUS §2z-ap and §2z-g; `llm_fitting/rankdiff_kalman.py`
(`oos_movement` and what it calls); `llm_fitting/ladder_explainer.py` for
an example of driving the gate from another module without editing it;
`llm_fitting/runs/2026-10-12_ladder/` for cells already on disk.

## Questions to answer

1. **Error against number of simulations.** For each panel's frozen
   movement stack, how does the gate error depend on reps? Is the 3-to-20
   drop reproduced, and is it the bias of an absolute deviation of a noisy
   simulated moment? One long run per panel with per-rep moments kept,
   resampled to smaller rep counts, answers this without many runs.
2. **Noise of one evaluation and of a difference.** The spread of the gate
   error over simulation seeds at 20 and at 40 reps, per panel and per
   split. The spread of the difference between two evaluations of the same
   stack with disjoint seeds. For pairs of different specifications with
   common random numbers, use the rung pairs already on disk from the
   ladder run and say how much pairing removes.
3. **Where the error and its variance come from.** Per panel, the share of
   the mean error and of its variance carried by each of the seven moments,
   and how often a moment enters or leaves the average through `MOM_FLOOR`
   across splits.
4. **Resolution.** With five splits, what paired difference between a
   candidate and the incumbent can be told from zero, per panel, at 20 and
   40 reps? State it as a table the owner can read a margin from. Offer two
   or three margin rules with what each would have concluded on the ladder
   rung pairs; do not choose one.
5. **A wrong model fails.** Confirm from cells on disk (ladder rungs R0 and
   R1) or from one added mutation that a clearly wrong model is rejected on
   every panel.

Panels: `facebook_a`, `reddit_comments`, `reddit` (short submissions), the
long submissions panel with the P6 command of
`PREREG_2026-07-16_submissions_ig.md`. The Instagram rescue is optional.

## Boundaries

- Frozen movement stacks only. No other specification is evaluated.
- Gate v1 is not changed and no replacement is proposed. A supplementary
  summary may be suggested, labelled as such.
- `rankdiff_kalman.py` and `minimal_rankdiff.py` are not edited. Wrap them
  from `llm_fitting/validation/gate_instrument.py`. If per-rep moments
  cannot be obtained without an edit, write the smallest hook you need in
  `QUESTIONS.md` and continue with what can be done.
- DEVELOPMENT panels only. Every evaluation is logged in `GATE_ROWS.md` as
  `reproduction`, with reps and seeds.
- Heavy runs go through `scripts/heavy_lock.py`; a run over about fifteen
  minutes goes in a launch script for the owner to start.

## Deliverables

The handoff folder as in PROTOCOL §5, with `DESIGN.md` committed before the
first run and predictions in it (for example: the size of the rep bias at 3
reps per panel; which moment dominates each panel's variance). Code in
`llm_fitting/validation/gate_instrument.py` with tests in
`tests/test_validation_gate_instrument.py` (a synthetic case where the bias
and the noise are known). `scripts/health_check.sh` passing.

## Done when

The five questions are answered with numbers and bands, every prediction is
scored, and the report says what was not run. Stop and report instead if a
recorded reference gate does not reproduce at its recorded settings.
