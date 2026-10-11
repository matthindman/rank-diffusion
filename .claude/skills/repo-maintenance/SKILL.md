---
name: repo-maintenance
description: "How to maintain the rank-diffusion repository itself: layout, tests and CI, the Python and R package trees, the skills and their mirror, git hygiene, commits and pushes in a public repository shared by two vendors' agents. Use when committing or pushing; moving, renaming or deleting files; editing packaging, CI, READMEs or the agent entry files; revising a skill; committing while another session has uncommitted work; or doing any git surgery."
---

# Repository maintenance

This repository is two things at once: an active research program with a
canonical record and frozen protocols, and the home of Python and R
packages meant for outside researchers. It is public, and two vendors'
agents work in it, sometimes at the same moment. Maintenance means keeping
all of that healthy without letting work on one part damage another. When
unsure: preserve, verify, document, in that order.

## Health check

```
scripts/health_check.sh
```

runs the research suite, the package suite and the legacy guard (FB legacy
panel at defaults: 14/15, churn 0.013) with the registered interpreter, and
warns if pandas is 3 or later. Run it before changing anything and before
every commit; both suites and the guard pass at every commit. For R changes
also run `R CMD build R/rankdiff` and `R CMD check --no-manual` on the
tarball; Status: OK is the bar.

If the task touches modeling, fitting, evaluation or data in any way, the
stochastic-modeling skill governs and this one does not override it. If it
touches `Python/rankdiff/` or `R/rankdiff/`, read `PACKAGING_ROADMAP.md`
first.

## Map

| path | what it is | rule |
|---|---|---|
| `llm_fitting/` | the active research line | research rules only; maintenance tasks do not edit model code |
| `llm_fitting/MODEL_STATUS.md` | the canonical record | append-only (model-status-authoring skill) |
| `llm_fitting/PROGRAM_STATE.md` | one-page digest of the record | kept current; the record wins |
| `llm_fitting/EXPOSURE_LEDGER.md`, `GATE_LEDGER.md` | which data has been seen; every gate evaluation | append-only |
| `llm_fitting/CONFIRMATION_PROTOCOL.md`, `PREREG_*.md` | registered protocols | amended only by dated amendment before data contact |
| `llm_fitting/lit_review/` | literature notes behind the agendas | citations carry verification tags; re-check before citing |
| `tests/` | regression suite for the research line, the archived core, and the skills mirror | never weaken a test to make a change pass |
| `tests/skill_scenarios/` | behavioral scenarios for evaluating the skills | rerun when a skill or the model generation changes |
| `Python/rankdiff/`, `R/rankdiff/` | the production packages | package track; `PACKAGING_ROADMAP.md` |
| `archive/` | historical code | reference, but load-bearing: `tests/` imports `archive.src.rankdiff…` |
| `paper/` | manuscript scaffold | claim language is bound to MODEL_STATUS §6; owner-gated |
| `scripts/` | health check and data-wrangling pipeline | data scripts follow the data-intake skill |
| `data/`, `cache/`, `output/`, `tmp/` | local panels and artifacts | not in git; SSD panels need `/Volumes/T9` |
| `.claude/skills/`, `.agents/skills/` | the skills and their mirror | see "Skills" below |
| `AGENTS.md`, `CLAUDE.md` | agent entry files | see "Skills" below |
| `llm_fitting/coordination/`, `llm_fitting/handoff/`, `scripts/agent_worktree.sh`, `scripts/heavy_lock.py` | the two-agent protocol, the task queue and briefs, each task's handoff folder, and the tools for a second working copy and for sharing the machine | Claude is the only writer on `main`; Codex writes in its own working copy (`coordination/PROTOCOL.md`) |
| `llm_fitting/SEALED_PATHS.json`, `scripts/sealed_data_guard.py`, `.claude/settings.json` | the sealed-folder list, the check that enforces it, and the Claude Code hook that runs the check before every tool call | owner-gated: an entry leaves `sealed` only on the owner's word; edits prompt the owner; `tests/test_sealed_data_guard.py` must pass |
| `.github/workflows/ci.yml` | CI | see "CI" below |

## Two tracks

The research track (`llm_fitting/`, `tests/`, the record) moves fast and its
protocols are frozen. The package track ships stabilized model generations
(v0.1.x is the v4.3 generation) and lags the research line on purpose. Flow
is one way, research to package, in graduations the owner declares.
Package internals are not edited to follow a research result, and packaging
or cleanup work does not modify `llm_fitting/`, test defaults or frozen
specs. A bug present on both tracks is reported on both and fixed under each
track's rules. The Python package is the reference implementation when the
two languages disagree.

## Git and documents

- **The repository is public.** A push publishes and cannot be withdrawn:
  pushed history is never rewritten. Push only when the owner asks. Before a
  push, scan the unpushed range for credentials and data files. Material
  that describes the owner's other projects in detail, and anything
  personal, is held for the owner's explicit go-ahead.
- **Another session may have uncommitted work in the tree.** Look at
  `git status` and `git diff --cached` first. Commit by explicit path; no
  `git add -A`, no `git commit -a`. Do not revert, restage or commit files
  you did not write unless the owner asks, and then say in the message who
  wrote them.
- **Committing part of a shared file** (your entry in MODEL_STATUS while
  another session's entry is unfinished). Build the commit in a temporary
  index so the working tree is never touched:
  1. Write the text you want committed to a scratch file: the HEAD version
     plus your section only. Check that HEAD's content is a prefix of it.
  2. `export GIT_INDEX_FILE=<scratch index>`; `git read-tree HEAD`.
  3. `git update-index --cacheinfo 100644,$(git hash-object -w
     --path=<file> <scratch file>),<file>`; `git update-index --add` for
     any whole files of yours; `tree=$(git write-tree)`; `unset
     GIT_INDEX_FILE`.
  4. Test that tree: `git archive $tree | tar -x -C <scratch dir>` and run
     both suites there.
  5. Confirm HEAD has not moved, then `commit=$(git commit-tree $tree -p
     HEAD -F <message file>)`, `git reset --soft $commit`, and `git reset
     -q -- <exactly the paths you committed>`.
  Afterwards `git status` shows the other session's text as an unstaged
  change and nothing staged.
- **Small, coherent commits.** The message says what changed and why, the
  original date of work when it is being committed later, and the test
  status, including whether the commit's own tree was tested clean.
- **Move, don't delete; `git mv`, don't copy.** Superseded code goes to
  `archive/` with history. Deleting anything tracked is owner-gated.
- **Attribution is load-bearing.** `DESCRIPTION` `Authors@R`, `pyproject`
  `authors` and both LICENSE files name both authors. Never drop, rename or
  placeholder an author when copying or consolidating trees.
- **Extend documents in their own voice.** The READMEs and package docs have
  deliberate structure; improve by extension and small correction. A rewrite
  is the owner's decision. Python docstrings are worded from the R roxygen
  titles; keep that coupling.
- **Numbers live in MODEL_STATUS.** Cite the section or copy verbatim with
  it. Plans and reports carry their date and status, and say so when they
  are committed long after they were written.
- **No artifacts in git.** `.gitignore` covers data, caches, PDFs, PNGs and
  build directories. A few PNGs tracked from before those rules, the PNAS
  logo and the toy parquets are intentional.

## Skills

- `.claude/skills/` is canonical (Claude Code reads only it). `.agents/skills/`
  is a byte-identical mirror (Codex reads only it). After editing any skill
  file, including references, copy the skill directory across.
  `tests/test_skills_sync.py` fails when any file differs.
- `AGENTS.md` is the one always-loaded entry file for every agent.
  `CLAUDE.md` imports it and adds only Claude-specific lines. Each rule is
  stated once; do not restate skill content in the entry file.
- Frontmatter is `name` and `description` only, so both vendors load the
  same file. Descriptions say what the skill does and when to use it, in
  under 1,024 characters.
- Keep each `SKILL.md` under about 5,000 tokens, with purpose and stop
  conditions first: that is what survives context compaction. Detail goes
  in `references/`, one level deep, each with a stated reason to load it.
- Skills hold method. Project state belongs in `PROGRAM_STATE.md` and the
  record; stale state in a skill is the one kind of instruction with
  measured harm.
- When the method changes by owner decision, change the skill, the mirror
  and `AGENTS.md` in the same commit, and record it in MODEL_STATUS.
- Revisit the skills when the model generation changes. Rerun
  `tests/skill_scenarios/` on each vendor's current model, remove
  instructions that no longer change behavior, and add one only for a
  failure that was observed.

## CI

`.github/workflows/ci.yml` runs the research suite, the package suite, a
wheel build and `R CMD check`.

- CI has no local data and no drives. Every test synthesizes its inputs; a
  test that needs a local panel is skipped when the file is absent.
- The Python job installs an explicit dependency list with pandas held
  below 3 (MODEL_STATUS §2z-ai). A tested module that gains a third-party
  import adds it to that list in the same commit.
- Keep main green. A change that cannot pass CI is not ready to push.

## Owner-gated

Changing frozen specs, thresholds, defaults or a registered protocol.
Deleting tracked files or history surgery. Pushing. Renaming a package,
changing version numbers, publishing a release. Rewriting a README or major
document. Editing paper claim language. Moving files between tracks.

## Pitfalls that have bitten

- The archive is not inert: check imports before touching `archive/src/`.
- Package tests run uninstalled through `Python/rankdiff/conftest.py`; keep
  it.
- Do not mass-regenerate `R/rankdiff/man/` with a roxygen2 version other
  than the `RoxygenNote` in DESCRIPTION; hand-write a single new `.Rd` in
  the house format.
- New `stats` functions used in R go in both the `@importFrom` block and
  NAMESPACE.
- Repo parquets can be evicted by iCloud (errno 89); `brctl download <path>`
  restores them.
- zsh does not word-split unquoted variables; identical outputs across a
  sweep are the tell.
- `paper archive/` has a space in its name.
- A scratch environment built from the latest packages gets pandas 3 and
  fails the research suite; install with `"pandas<3"`.
