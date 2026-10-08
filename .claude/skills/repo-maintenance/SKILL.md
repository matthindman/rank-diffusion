---
name: repo-maintenance
description: "How to maintain the rank-diffusion repository itself — layout, tests, CI, the package trees, skills mirroring, git hygiene, and commit discipline. Read before restructuring or moving files, editing packaging/CI/READMEs, or doing any git surgery. Written for any agent (Claude, ChatGPT/Codex, Gemini). Companion to stochastic-modeling (the science), data-intake (raw data), and model-status-authoring (the canonical record)."
---

# Rank-Diffusion Repository Maintenance — Skill

This repository is two things at once: an **active research program** with a
canonical results record and frozen protocols, and the home of **production
Python and R packages** meant for outside researchers. Maintenance means
keeping both healthy without letting work on either damage the other. The
program is externally reviewed (PNAS-track) and multi-author; changes you
make will be read, rerun, and built on by people and by other models. When
in doubt, the maintainer's virtues here are: preserve, verify, document —
in that order.

## 0. Read-first, and the health check

1. Entry points: `CLAUDE.md` (Claude) / `AGENTS.md` (ChatGPT/Codex and
   others). Same rules, two digests.
2. If the task touches modeling, fitting, evaluation, or data in ANY way,
   stop and read `stochastic-modeling/SKILL.md` in full first. Repo
   maintenance never overrides it.
3. If the task touches `Python/rankdiff/` or `R/rankdiff/`, read
   `PACKAGING_ROADMAP.md` — it governs the package track and lists what is
   owner-gated.
4. Run the health check BEFORE and AFTER any change:

   ```
   python -m pytest tests/ -q                    # research regression suite; from repo root
   python -m pytest Python/rankdiff/tests -q     # package suite; also from repo root
   ```

   Both suites green at every commit, no exceptions. If `python` is not on
   PATH: `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`.
   Use `python -m pytest` (not bare `pytest`) — the module form puts the
   repo root on `sys.path`, which the research tests rely on. For R
   changes, additionally run `R CMD build R/rankdiff` +
   `R CMD check --no-manual` on the tarball; Status: OK is the bar.

## 1. Repository map (what lives where, and each area's rules)

| path | what it is | maintenance rules |
|---|---|---|
| `llm_fitting/` | ACTIVE research line + `MODEL_STATUS.md` (canonical record) | research rules only (stochastic-modeling skill); maintenance tasks do not edit it |
| `tests/` | regression suite guarding the research line, the archived core, and skills sync | never weaken a test to make a change pass; new estimators need recovery tests |
| `Python/rankdiff/` | production Python package (src layout, own pyproject/tests/README) | package track; roadmap rules; ships stabilized model generations |
| `R/rankdiff/` | production R package — the ONE canonical R tree | same; check-clean as of 2026-07-07, keep it that way |
| `archive/` | historical code and superseded trees | reference only, but **load-bearing**: `tests/` imports `archive.src.rankdiff...` — moving or deleting under `archive/src/` breaks the suite |
| `paper/` | PNAS LaTeX scaffold | claim language is bound to MODEL_STATUS §2x/§6; owner-gated |
| `data/`, `cache/`, `output/`, `llm_outputs/` | local panels and artifacts | gitignored; never commit data; SSD panels need `/Volumes/T9` (`data/ssd` symlink), extension raw data needs the WD drive — if unmounted, report and stop that thread |
| `.claude/skills/` + `.agents/skills/` | dual-published skills (this file included) | §4 below |
| `.github/workflows/ci.yml` | CI: both pytest suites, wheel build, `R CMD check` | §5 below |
| top-level `*.md` | README, PACKAGING_ROADMAP, DATA_INVENTORY, DATA_PHASE2_REPORT | stewardship rules in §3 |

## 2. The two tracks (the central maintenance fact)

- **Research track** (`llm_fitting/`, `tests/`, MODEL_STATUS): moves fast,
  results are canonical, protocols are frozen.
- **Package track** (`Python/rankdiff/`, `R/rankdiff/`): moves
  deliberately, ships *stabilized model generations* (v0.1.x = the v4.3
  generation), and **lags the research line by design** — that is policy,
  not neglect.
- Flow is **one-way, research → package, in owner-declared graduations**.
  Never edit package internals to chase a research result; never let
  packaging or cleanup work modify `llm_fitting/`, test defaults, or
  frozen specs. A bug found on one track that also exists on the other is
  reported in both places but fixed under each track's own rules.
- The two packages are translations of each other (R was ported from the
  Python reference). When they disagree on algorithm behavior, the Python
  package is the reference implementation — verify against it before
  "fixing" either. Keep public APIs and documentation wording consistent
  across the two languages when you touch them.

## 3. Git and documentation discipline

- **Suite green at every commit** (both suites, §0). The legacy guard
  (facebook legacy panel, default settings: 14/15, churn 0.013) must hold;
  defaults stay byte-identical.
- **Never rewrite pushed history.** MODEL_STATUS.md is append-only —
  supersede explicitly, never edit old sections (see the
  model-status-authoring skill).
- **Move, don't delete; `git mv`, don't copy.** Superseded code goes to
  `archive/` with history preserved. Deleting anything tracked is
  owner-gated.
- **Attribution metadata is load-bearing.** `DESCRIPTION` `Authors@R`,
  `pyproject.toml` `authors`, LICENSE files: never drop, rename, or
  placeholder an author when copying, splitting, or consolidating trees.
  This has been gotten wrong once (a working snapshot with placeholder
  metadata); it is exactly the kind of error that outlives the session
  that made it.
- **Extend documents in their own structure and voice.** The READMEs and
  package docs have deliberate structure and phrasing from the people who
  wrote them; improve by extension and small correction, not rewrite. If a
  rewrite genuinely seems necessary, stop and ask the owner. When adding
  Python docstrings or R man pages, reuse the existing documentation's
  wording (the Python docstrings are worded from the R roxygen titles —
  keep that coupling).
- **Numbers live in MODEL_STATUS.** Don't duplicate result numbers into
  READMEs or docs from memory — cite the section, or copy verbatim with
  the section reference. If you cannot find a number in MODEL_STATUS, grep
  for it before citing it anywhere.
- **No artifacts in git.** `.gitignore` already covers data, caches, PDFs,
  PNGs, `tmp_*`; some pre-gitignore PNGs remain tracked under
  `llm_fitting/` and `figures/` — removing them is owner-gated (the paper
  may reference them). `paper/PNAS_Logo.pdf` and the two toy parquets are
  intentional exceptions.
- Commit messages: what changed and why, plus test status. Small, coherent
  commits over omnibus ones.

## 4. Skills maintenance (this directory)

- `.claude/skills/` is **canonical**; `.agents/skills/` is a byte-identical
  mirror for tools that read that path. After editing any skill:
  `cp -R .claude/skills/<name> .agents/skills/` (or copy the file).
  `tests/test_skills_sync.py` enforces identity — divergent copies would
  give different models different instructions, which is worse than both
  being stale.
- When the method changes (new frozen spec, new pitfall, changed agenda),
  update `stochastic-modeling/SKILL.md` and the digests in CLAUDE.md /
  AGENTS.md in the same commit as the change. A skill that contradicts the
  repo is a trap for the next session.
- Skills describe method and procedure; MODEL_STATUS holds results. Don't
  let numbers accumulate in skills beyond the frozen-spec table.

## 5. CI maintenance

`.github/workflows/ci.yml` runs: (a) `python -m pytest tests/ -q`,
(b) `python -m pytest Python/rankdiff/tests -q`, (c) a wheel build of the
Python package, (d) `R CMD check` on `R/rankdiff` via r-lib actions.

- CI has **no access to local data or drives**. Every test that runs in CI
  must synthesize its own inputs (they all currently do). If you add a test
  that needs a local panel or a mounted drive, gate it with a skip
  (`pytest.mark.skipif` on the file's existence), never let CI depend on it.
- The Python job installs an explicit dependency list (numpy, pandas,
  scipy, matplotlib, pyarrow, zstandard, pytest). If a tested module gains
  a new third-party import, add it there in the same commit.
- Keep CI green. A red main branch blocks everyone; if your change cannot
  pass CI, it is not ready to push.

## 6. Owner-gated — stop and ask before

- Changing frozen specs, thresholds, defaults, the confirmation protocol,
  or anything the roadmap marks **[owner]**.
- Deleting tracked files, removing tracked figures, or history surgery.
- Renaming either package, changing version numbers, or publishing
  releases (PyPI/CRAN).
- Rewriting (as opposed to extending) any README or major doc.
- Editing paper claim language.
- Moving files between the research and package tracks.

## 7. Repo-specific pitfalls (each has bitten a session; don't re-learn)

- `tests/` imports `archive.src.rankdiff...` — the archive is not inert;
  check imports before touching anything under `archive/src/`.
- The Python package tests run uninstalled via `Python/rankdiff/conftest.py`
  (src-layout `sys.path` bootstrap) — don't remove it, and don't "fix" the
  tests by requiring an install.
- Don't mass-regenerate `R/rankdiff/man/` with a roxygen2 version different
  from the `RoxygenNote` in DESCRIPTION (currently 7.3.2) — it churns all
  ~70 Rd files for nothing. For a single new internal helper, hand-write
  the `dot-<name>.Rd` in the existing house format; for real doc work,
  match the pinned roxygen version or ask the owner.
- New `stats`-namespace functions used in R code must be added to BOTH the
  `@importFrom` block in `R/rankdiff-package.R` and (if not regenerating)
  the NAMESPACE, alphabetically.
- Repo parquets can be iCloud-evicted (reads fail with errno 89) —
  `brctl download <path>` restores them.
- zsh (the default shell in agent harnesses here) does not word-split
  unquoted variables; argparse swallows multi-word args silently —
  identical sweep outputs are the tell.
- `paper archive/` has a space in the name — quote paths in shell commands.
- Long runs: `python -u` when redirecting output; OOS gates take
  minutes-to-hours — do not kill them early, and do not conclude anything
  from a run you killed.
