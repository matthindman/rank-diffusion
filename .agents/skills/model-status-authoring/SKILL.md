---
name: model-status-authoring
description: "How to write, amend, and read llm_fitting/MODEL_STATUS.md — the canonical results record of the rank-diffusion program. Read before documenting any research session, superseding a result, or citing recorded numbers. The document's conventions are load-bearing: program continuity across sessions and models depends on them."
---

# MODEL_STATUS.md Authoring

MODEL_STATUS.md is the program's memory. Sessions end; models change; the
document is what makes the next session start where the last one stopped
instead of re-deriving or — worse — contradicting it. Treat writing it with
the same care as the analysis itself.

## Structure and reading order

- **§1** unified model (rarely edited; only when the model class itself
  changes, with the old text preserved by the section history).
- **§2** dated, append-only research record: `## 2x. YYYY-MM-DD — headline`.
  Lettered in sequence (…2z, 2z-a, 2z-b, …). NEWEST SECTIONS SUPERSEDE OLDER
  ONES; the reader is expected to read the last ~4 first.
- **§3** corrected estimation pitfalls (append when a new one is locked by a
  regression test).
- **§4** known limitations / open items. **§5** reproduction. **§6** paper
  framing (edit only per the binding claim set, currently §2x's).

## The section template (every substantive session appends one)

A good §2 section contains, in roughly this order:

1. **Headline line** in the `##` title: date + the one-sentence result,
   including its epistemic status (CONFIRMED / NOT confirmed / REJECTED /
   adopted / opt-in / no change adopted).
2. **The question/motivation** — what was uncertain and why it mattered.
3. **Diagnosis by measurement** — the measurements that discriminated between
   hypotheses, with numbers. Hypotheses killed along the way are listed WITH
   the measurement that killed them (future sessions must not resurrect them).
4. **The change** — flag name, what it does, parsimony accounting ("zero new
   components", "+2 params per knot, only where flag on"), rng gating, test
   count, legacy-guard status.
5. **Pre-registered predictions, scored** — each marked PASS / FAIL /
   PARTIAL. Failed predictions stay in the record with the same prominence.
6. **Validation table** — baseline spec → new spec, per panel, with the gate
   verdict. Standard columns: goal-1 card, churn err, OOS rel err ±SD vs
   persistence ±SD, CI coverage, calibrated scale by split.
7. **Adoption verdict by the pre-declared criterion** — explicitly "defaults
   unchanged" when true (it almost always is; changes ship as opt-in flags).
8. **Reproduction block** — exact CLI commands, copy-pasteable.

## Binding conventions

- **Append-only; supersede explicitly.** Never rewrite or delete recorded
  results. When a result is invalidated, add an inline bracketed note at the
  OLD location — `_[SUPERSEDED 2026-07-03 by §2o: …one-line reason…]_` — and
  state the replacement in the new section ("Superseded numbers: every
  sigma_obsB value in 2e/2g-X tables is the legacy convention; the centered
  column is canonical from here").
- **Numbers carry uncertainty and provenance.** OOS results as mean ± SD over
  named splits, never single-split; head-churn/coll rows with bands or an
  explicit MC caveat (seed SD ±0.15); every number attributable to a session
  date and a command. Say how many reps/seeds/boot draws.
- **Distinguish measured from conjectured**, always ("Mechanism is
  arithmetic, not conjecture: …" / "plausibly the same physics as… —
  investigate before building"). Distinguish CONFIRMED / directionally
  consistent / within noise.
- **Declare, don't hide**: knob settings per table row; declared caveats
  (e.g., "in-sample VR is partially mechanical under --md-vr"); scoring
  artifacts (the coll1=0 denominator explosion was declared, then fixed via
  MOM_FLOOR — "declared here, not patched mid-experiment").
- **Language register**: internal record is confident-and-correct, never
  hedged for its own sake ("adopt precision, not tepidity") — but claim
  language that the paper inherits follows the binding claim set (§2x/§6
  addenda): e.g., σ_obs "identified in shape everywhere, identified in level
  at the FB head, bounded elsewhere"; "excess low-frequency structure", not
  "lifecycle arcs".
- **Owner-gated items are labeled as such** and never silently worked
  around (drive mounts, data acquisition, protocol changes, defaults).
- **Cross-references by section letter** (§2i, 2g-X P3) — use them densely;
  they are the document's link graph.

## Amending CONFIRMATION_PROTOCOL.md

Only as a new dated `## AMENDMENT An` section, committed STRICTLY BEFORE the
covered data is processed (state that explicitly in the amendment, as A1/A2
do: "No extension data has been read at the time of this amendment").
Amendments add registered evaluations (E4, E5, …) with pre-declared readings
and named candidate fixes; they never loosen an existing criterion. If data
has already been touched, the analysis is exploratory and must be labeled so
— it cannot be registered retroactively.

## When you finish a session

Write the §2 section BEFORE ending the session — an undocumented result is a
lost result (memory does not transfer between models; this file does). Then:
update §4 if the open-items list changed; update the memory index and the
skills (`.claude/skills/*/SKILL.md` §3/§7 tables) if frozen specs or the
agenda moved; commit with a message that summarizes the section headline.
