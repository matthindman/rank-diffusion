---
name: model-status-authoring
description: "How to write, amend, supersede and cite llm_fitting/MODEL_STATUS.md, the canonical append-only record of the rank-diffusion program, and how to amend a registered protocol. Use when documenting any session (experiment, registered evaluation, measurement, planning, data operation), correcting or superseding a recorded result, recording an owner decision, quoting recorded numbers anywhere, or committing a record entry while another session has uncommitted entries in the same file."
---

# Writing the record

MODEL_STATUS.md is the program's memory. Sessions end, models change, and
two vendors' agents alternate; the file is what lets the next session start
where the last one stopped instead of re-deriving it or contradicting it.
It is also the audit trail a referee can be shown. An undocumented result
is a lost result, and a number that cannot be traced to a command is not a
result at all.

## Structure and reading order

- **§1** the model as written in code. Edited only when the model class
  changes, with the reason.
- **§2** the dated research record, `## 2x. YYYY-MM-DD — headline`, lettered
  in sequence (… 2z, 2z-a, … 2z-z, 2z-aa, …). Newest sections supersede
  older ones; a reader starts with the last four or so. Since §2z-ae new
  entries are appended at the end of the file, after §3–§6, so find the
  newest with `grep -n '^## 2' llm_fitting/MODEL_STATUS.md | tail`.
- **§3** corrected estimation pitfalls, added when a regression test locks
  one. **§4** limitations. **§5** reproduction. **§6** paper framing and the
  binding claim set, changed only by the owner.

`llm_fitting/PROGRAM_STATE.md` is a one-page digest of this file. When an
entry changes what that page says, update the page in the same commit.

## Every substantive session appends one entry

A session is substantive if it produced a measurement, a result, a
decision, a data operation, or a change to code or method. Answering a
question, declining a request or drafting a sentence needs no entry.

Say in the headline what kind of entry it is and what its standing is. The
kind determines what the body must contain.

| kind | the body must contain |
|---|---|
| **Experiment or measurement** | the question; the measurements that discriminated between hypotheses, with numbers and bands; hypotheses killed, each with the measurement that killed it; any change (flag, what it does, parameters added or removed, test count, guard status); predictions written beforehand, each scored PASS, FAIL or PARTIAL; a validation table (baseline → new, per panel, gate verdict); the verdict by the pre-declared rule; exact commands |
| **Registered evaluation** | the protocol and amendments in force; each registered item with its verdict at equal prominence, pass or fail; every deviation and execution problem; what was not adjudicated and why; exact commands and the run archive |
| **Planning or review** | what was read; what is proposed; "PLANNING ONLY" and "nothing adopted" in the headline; no scored predictions |
| **Data operation** | what was moved, built or acquired; checksums and manifests; whether any values were read; the ledger rows changed |
| **Owner decision** | the decision in the owner's words where possible, its date, and what it supersedes |

Standing vocabulary for headlines, used consistently: CONFIRMED, NOT
confirmed, REFUTED, WITHDRAWN, adopted, opt-in only, no change adopted,
NOT ADJUDICATED, IN PROGRESS, PLANNING ONLY.

Each entry also carries a provenance line: the model and vendor that did the
work, the date, and the commit the work started from. The skills that
governed it are identified by that commit. This is what makes an honest
account of AI use in the methods section possible later.

## Conventions that keep the record trustworthy

- **Append; never rewrite.** A recorded result is not edited or deleted,
  because later sections and the paper cite it by section letter. When a
  result is invalidated, add a bracketed note at the old location —
  `_[SUPERSEDED 2026-07-03 by §2o: one-line reason]_` — and state the
  replacement in the new section. Corrections found the same day are
  appended as labelled correction notes, not folded in silently.
- **Numbers carry uncertainty and provenance.** Gate results as mean ± SD
  over named splits, never one split; head rows with bands or the seed-SD
  caveat; reps, seeds and bootstrap draws stated; every number traceable to
  a command and a log. Quote a number from this file with its section; do
  not restate one from memory.
- **Say what kind of evidence it is.** Registered confirmation, delayed
  registered sensitivity, exploratory diagnostic, or development negative
  result (the four tiers of §6). A code reading is a fact about the code;
  a mechanism inferred from it is a conjecture until measured.
- **Failures get the same prominence as successes.** A failed prediction, a
  killed run, an evaluation that could not be adjudicated, and a tooling gap
  are each stated plainly. "Not run" and "not verified" are legitimate
  entries; silence is not.
- **Operations that span sessions** are recorded as IN PROGRESS, then closed
  with a dated completion addendum that says it supersedes the earlier
  status. Do not claim completion by implication.
- **Owner-gated items are labelled** and never worked around.
- **Cross-reference by section letter,** densely. The letters are the
  document's link graph.
- **Log beside the record.** Gate evaluations go in `GATE_LEDGER.md`; data
  contact goes in `EXPOSURE_LEDGER.md`. The entry cites both.

Internal language is confident where the measurement is, and exact about
what it does not show. Language the paper inherits follows the binding
claim set.

## When another session has entries in the same file

Two agents append to this file, sometimes on the same day. Read what the
other session appended before writing. Take the next free letter after
everything in the working copy, including entries not yet committed. Do not
reorder or renumber anyone's entry. When committing your entry while
another's is unfinished, commit only your own text (the repo-maintenance
skill describes how) and say in your entry that the gap in lettering is an
uncommitted entry, so a reader of the committed file is not misled.

## Amending a registered protocol

Only as a new dated `## AMENDMENT An` section, committed before the data it
covers is processed, and saying so in the text ("no extension data has been
read at the time of this amendment"). An amendment may add evaluations or
tighten a criterion. It may not loosen one. If data has already been
touched, the analysis is exploratory and is labelled so; it cannot be
registered after the fact. A correction to an infeasible rule made after
registration and before outcomes is disclosed as exactly that, in the
record and in the paper.

## Before the session ends

Write the entry. Update `PROGRAM_STATE.md` and §4 if the open items changed,
and the skills' `frozen-specs.md` if a stack or the agenda moved by owner
decision. Commit with a message that carries the entry's headline and the
test status.
