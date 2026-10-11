# Two agents, one repository — working protocol

**Status: APPROVED by the owner on 2026-10-12 (MODEL_STATUS §2z-ar), with
`WORKPLAN_CLAUDE_2026-10-12.md`.** The owner's instruction in a
conversation outranks this file.

Two agents work on this program at the same time: Claude (Claude Code) and
Codex (GPT-6 Astra). Three things have gone wrong or nearly wrong when they
shared a checkout: one overwrote the other's draft, both appended to the
same record file on the same day, and both launched heavy runs on a
16 GB machine. This protocol removes those by separating where each agent
writes, not by asking either to be careful.

## 1. Roles

| | Claude (lead) | Codex / Astra (independent validator) |
|---|---|---|
| owns | residual measurements; the model code; candidate dossiers; the record; integration | the gate as an instrument; planted-mechanism and recovery studies with its own simulator; endpoint-level scoring; blind re-computation; adversarial review |
| decides | what is merged to `main` | nothing about adoption; reports findings with evidence |

The split is deliberate. The agent that wrote the estimator and the
simulator does not also certify them, and the tasks given to Codex are the
ones its own agenda argued for. Roles can be swapped for a task by saying
so in that task's brief.

The owner adopts plans, approves gate spending and re-freezes, relays
prompts, and starts long runs in a Terminal window when asked.

## 2. Where each agent writes

| path | writer |
|---|---|
| `main` checkout (`…/GitHub/rank-diffusion`) | Claude only. Codex does not write here. |
| Codex's working copy (`…/GitHub/rank-diffusion-codex`), branches `codex/<task>` | Codex only |
| `llm_fitting/minimal_rankdiff.py`, `llm_fitting/rankdiff_kalman.py` | Claude only, in any branch. Codex wraps or imports them and never edits them; a needed hook is requested in `QUESTIONS.md`. |
| `llm_fitting/MODEL_STATUS.md`, `GATE_LEDGER.md`, `EXPOSURE_LEDGER.md`, `PROGRAM_STATE.md`, `SEALED_PATHS.json`, `AGENTS.md`, the skills | Claude only, on `main` |
| `llm_fitting/validation/**`, `tests/test_validation_*.py`, `llm_fitting/handoff/<task>/**`, `llm_fitting/runs/<date>_<task>/**` | the agent that owns the task |

A second Claude session uses its own working copy as well
(`scripts/agent_worktree.sh <path> claude/<topic>`); only one session at a
time works in the `main` checkout.

## 3. Working copies and branches

- Create or repair a working copy with
  `scripts/agent_worktree.sh <path> <branch>`. It links the data folder and
  leaves caches and run outputs separate, so intermediate files cannot be
  shared by accident.
- Each task starts on a fresh branch from the current `main`:
  `git switch -c codex/<task> main`. Do not rebase a branch that has been
  handed over.
- Claude merges a handed-over branch into `main` after review and a clean
  test run. Pushing is the owner's call.

## 4. The record has one writer

Codex does not edit MODEL_STATUS, the ledgers or PROGRAM_STATE, even though
`AGENTS.md` asks every session to append an entry. For work done under this
protocol the entry is written into the handoff folder and Claude appends it
on `main`, unchanged apart from its section letter, with Codex named in the
provenance line. This keeps two sessions from appending to one file at
once; it does not reduce what is recorded.

## 5. Handoff

Every task ends with a folder `llm_fitting/handoff/<task>/` on the task
branch, containing:

- `DESIGN.md` — written before the first run: question, design,
  predictions with direction and rough size, what would count as failure.
  Its commit precedes the commit of any result.
- `REPORT.md` — what was run, the commands, results with bands, every
  prediction scored, what failed, what was killed, what was not run.
- `STATUS_ENTRY.md` — the entry for MODEL_STATUS, ready to append, in the
  form the `model-status-authoring` skill describes.
- `GATE_ROWS.md` — one row per gate evaluation, in the ledger's columns,
  including runs that were abandoned.
- `QUESTIONS.md` — only if something blocks the task or needs a change to a
  file the agent does not own. If it is not empty, stop there and say so.

The last message of the session names the branch and the final commit.

**How the two agents talk.** Claude speaks to Codex through files committed
on `main`: the queue (`llm_fitting/coordination/QUEUE.md`), the briefs, and
answers to questions in `llm_fitting/handoff/<task>/ANSWERS.md`. Codex
speaks to Claude through its handoff folder on the task branch. Both
working copies share one git repository, so each can read the other's
commits as soon as they exist (`git show main:<path>`,
`git show codex/<task>:<path>`) without anything being pasted. Neither
agent sees the other's conversation; what is not in a committed file has
not been said.

**The queue.** The owner starts Codex once. From then on Codex works
through the queue in order: when a task is handed over it reads the queue
from `main`, starts the next open task on a fresh branch from the current
`main`, and does not wait to be told. If a task needs an input that is not
on `main` yet, Codex does everything that does not need it, says in
`REPORT.md` what is waiting on what, hands over, and moves on; it returns
to the waiting part when the queue marks the input as ready. When nothing
in the queue can be advanced, it stops and says so. Claude is the only
writer of the queue.

**Waking and direct calls** (owner's instruction, 2026-10-12). Claude does
not sit idle waiting: it sets a background watch on the task branch and is
woken when a handoff commit appears. For small jobs — a review, a blind
check, a question about a brief — Claude may call Codex directly through
the Codex plugin. Such a call writes nothing in the `main` checkout;
Claude saves the reply, as Codex's words, under
`llm_fitting/handoff/R_<name>/`.

## 6. Compute

The machine has 10 cores and 16 GB. A gate evaluation at 20 reps takes
about 4 minutes on the short panels, 12 on long submissions and Instagram,
and 22 on comments, one core each.

- Run every gate evaluation, and any job longer than about five minutes,
  through `python scripts/heavy_lock.py -- <command>`. It allows three
  heavy jobs at a time across both agents and frees its slot however the
  job ends.
- A job expected to run longer than about fifteen minutes is written as a
  launch script that skips work already on disk. Ask the owner to start it
  in a Terminal window under `caffeinate -dims`. Runs started from the
  desktop app's own terminal panel have stalled on the T9 drive.
- Do not interpret a run that was killed.

## 7. Seeds and pairing

- Comparisons between specifications use common random numbers: seeds
  0–19, the same splits and the same universe.
- An independent replication uses seeds 1000 and up, so that it shares no
  draws with the run it checks.

## 8. Gate evaluations

Gate v1 is frozen and is a finite resource. Under this protocol:

- no candidate is evaluated before its dossier is reviewed and the owner
  has approved the run;
- instrument and reproduction runs use frozen stacks only;
- every evaluation gets a row in `GATE_ROWS.md`, used or not.

## 9. Blind checks and review

- A blind check is given the definition and the command, never the number.
  The checker commits its result before reading the other agent's. Do not
  open the other agent's `REPORT.md` or run folder for that task until
  then.
- A reviewer gets code, logs and commands, not a summary. One review round
  and one round of fixes per deliverable. Each finding names the input or
  state that produces the wrong result, and is backed by something run
  where that is possible.
- Agreement between the two agents is not evidence. A disagreement is
  settled by a check that can be executed, or goes to the owner.

## 10. Data

All work under the current plan uses DEVELOPMENT panels only
(`EXPOSURE_LEDGER.md`). Nothing in a folder listed in `SEALED_PATHS.json`
is read; on the T9 drive that is `rank-diffusion-data/wikipedia`, which
sits beside the panels in use. The WD drive is unplugged on purpose. Codex
is not covered by the automatic check that Claude Code runs and is bound by
the rule in `AGENTS.md`.
