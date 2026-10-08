# Packaging Roadmap — `rankdiff` (Python) and `rankdiff` (R)

This document is the reference plan for turning the production packages into
polished, releasable Python and R packages while active research continues in
`llm_fitting/`. It defines the two-track policy, the current state of each
package, the phased workplan, and binding rules for agents doing package work.

Owner-gated decisions are marked **[owner]**. Everything else can proceed.

_Dating note. Written 2026-07-07, with Phase 1 executed the same day; first
committed 2026-10-08, re-verified but otherwise unchanged. Every status line
below is as of the date it carries. Since it was written, the confirmation
battery E1–E5 has been executed (2026-07-12, MODEL_STATUS §2z-q); the Phase 3
graduation remains owner-gated, and plans made after 2026-07-07 take
precedence wherever they differ from this document._

## 1. The two tracks

The repository deliberately runs two lines of work with different rules:

- **Research track** — `llm_fitting/` + `tests/` + `MODEL_STATUS.md`. Moves
  fast, governed by `.claude/skills/stochastic-modeling/SKILL.md`,
  `CLAUDE.md`, and `AGENTS.md`. This is where the canonical results live.
- **Package track** — `Python/rankdiff/` and `R/rankdiff/`. Moves
  deliberately. Ships *stabilized* model generations with a stable public
  API, documentation, tests, and (eventually) CI and releases. The package
  track was established as "prod v1" in May 2026 (P. Waggoner) and its
  structure and conventions — src layout, paired Python/R implementations,
  packaged toy data, README-driven documentation, the v0.1.0 API surface —
  are the foundation everything below builds on. Extend them; don't rework
  them.

**Flow between tracks is one-way and discrete: research → package, in
"graduations."** The packages intentionally lag the research line. v0.1.x
packages the v4.3-generation permanent-transitory model, and that is correct
as documented: it is a complete, self-consistent generation. The current
research generation (minimal Lagrangian + Kalman/OOS line) graduates to
v0.2 only after the confirmation protocol and paper claim-set are frozen
**[owner]**. Until then, no research result — however exciting — justifies
editing package internals, and no package cleanup may touch `llm_fitting/`,
test defaults, or frozen specs.

## 2. Current state (assessed 2026-07-07)

### Python — `Python/rankdiff/`
Clean src-layout package: 13 typed modules (~2,500 LoC), own
`pyproject.toml`, a thorough 181-line README, examples, 7 test files, and
packaged toy data. The layered module structure (schema → preprocess →
initializers → fit → simulator/diagnostics → ablation/sensitivity/plotting,
orchestrated by `pipeline.run_pipeline`) is sound and should be preserved.

Gaps to releasable (status as of 2026-07-07):
- ~~The **repo-root `pyproject.toml`** points at a nonexistent root `src/`~~
  **DONE** — the stale root stub was removed; the package's own
  `pyproject.toml` is the single install path
  (`cd Python/rankdiff && pip install -e .`, as its README says).
- ~~Toy data won't ship in a wheel~~ **DONE** — `toy_rank_data.parquet` now
  lives at `src/rankdiff/data/`, ships via `[tool.setuptools.package-data]`,
  and is located with `rankdiff.toy_data_path()` (new `datasets.py`);
  quickstart/tests/README use the accessor, so they work installed or from a
  checkout and from any working directory. Verified: wheel builds and
  contains the parquet; quickstart reproduces the README's documented output.
- ~~`__init__.py` exports only `Config`/`run_pipeline`~~ **DONE** — exports
  widened to the full documented API (mirrors the pre-restructure reference
  list, plus `run_pipeline` and `toy_data_path`). This also fixed
  `test_quickstart.py`, which already imported the wider surface.
- ~~Function/class docstrings sparse~~ **DONE (first pass)** — one-line
  docstrings on all public functions/classes, worded to match the R
  package's roxygen titles so the two languages' docs stay consistent.
  Fuller parameter-level docstrings remain Phase 2.
- ~~Package tests not wired in~~ **DONE** — a `conftest.py` makes the
  src-layout importable uninstalled, so
  `python -m pytest Python/rankdiff/tests -q` runs from the repo root
  (8 tests, green); CI runs it alongside the research suite.
- ~~No LICENSE~~ **DONE** — MIT LICENSE added (matches the R package) and
  declared in `pyproject.toml`. Still open: lower-bounded deps, PyPI
  (Phase 2).

### R — `R/rankdiff/`
The most complete package in the repo and the canonical R tree: full
DESCRIPTION (both authors, MIT license), NAMESPACE with 30 exports + S3
methods, 69 generated man pages, NEWS.md, README, packaged toy data under
`inst/extdata/`, and a testthat suite including an end-to-end pipeline test
against the packaged data. Plausibly close to passing `R CMD check` already.

- ~~`rankdiffR/` reconciliation~~ **DONE (2026-07-07)** — the
  consolidation-era working snapshot's numerical-robustness fixes were
  ported into `R/rankdiff` and verified against the Python reference
  implementation, which already had all three: the `track_count` clamp
  (matches `preprocess.py`), recording tracked ranks after dead-entity
  masking (matches `simulator.py`), and the binomial `.split_entry_counts()`
  entry split (matches `simulator.py`), plus R-specific finite guards for
  `sd()` on short vectors and the variance-ratio filter in `diagnostics.R`.
  The four accompanying regression tests were ported into
  `tests/testthat/test-corrections.R`. The snapshot now lives at
  `archive/rankdiffR/`; `R/rankdiff` is the one canonical R tree. Its
  DESCRIPTION metadata (authorship, imports, RoxygenNote) was correct and
  stays as-is.
- ~~`R CMD check`~~ **DONE** — `R CMD build` + `R CMD check --no-manual`
  pass clean (Status: OK; testthat suite 75/75) with the ported fixes in.
- `archive/R/` is the older jump-model-zoo lineage — reference only, not an
  ancestor of the package.

### Cross-cutting
- ~~No CI~~ **DONE (pending first push)** — `.github/workflows/ci.yml` runs
  (a) the research-line regression suite, (b) the package's pytest suite,
  (c) a wheel build, and (d) `R CMD check` on `R/rankdiff` via r-lib
  actions. Both suites and the check pass locally; the workflow itself is
  verified on the first push to GitHub.
- ~33 regenerable PNGs under `llm_fitting/` and `figures/` are git-tracked
  from before the current `.gitignore` rules (`git rm --cached` candidates)
  **[owner — some may be referenced by the paper]**.
- README's Repository Structure block omits `paper/`, `report/`, `data/`,
  `scripts/` — small accuracy update, Phase 2.
- The two implementations both target model v4.3 (`R/rankdiff/R/run.R` and
  `llm_fitting/model_v43.py` are the provenance trail). `model_v43.py`'s
  path shim points at the pre-restructure root `src/`; it should point at
  `archive/src/` or be retired to `archive/`.

## 3. Workplan

### Phase 1 — make v0.1 installable and checkable (no model changes)
**COMPLETE 2026-07-07** except the items noted:
1. ✅ R consolidation: fixes ported into `R/rankdiff` with tests,
   `R CMD check` clean, `rankdiffR/` archived.
2. ✅ Python install path: root `pyproject.toml` stub removed; toy data
   packaged with a `toy_data_path()` accessor; MIT LICENSE added;
   `__init__` exports widened to the documented API.
3. ✅ Tests wired: `python -m pytest Python/rankdiff/tests -q` runs from the
   repo root (uninstalled) via `conftest.py`; research suite untouched and
   green (66/66) throughout.
4. ✅ CI workflow added (`.github/workflows/ci.yml`) — needs its first push
   to be confirmed green on GitHub runners. README badges: after that.
5. ✅ First-pass docstrings on all public Python functions, worded to match
   the R roxygen titles.

Exit criterion: fresh clone → `pip install Python/rankdiff` and
`R CMD build R/rankdiff` both succeed; CI green; repo-root suite green.
Verified locally 2026-07-07 (wheel builds with data; R check Status: OK;
66 + 8 + 75 tests green); the CI-on-GitHub leg confirms on first push.

### Phase 2 — polish for external users (still the v4.3 generation)
- Vignette (R) and example notebook (Python) with feature parity; pkgdown
  site; Python API reference (Sphinx or mkdocs).
- Version/NEWS discipline: every user-visible change gets a NEWS.md /
  CHANGELOG entry; semantic versioning from v0.1.1 on.
- Distribution decision **[owner]**: PyPI; CRAN vs r-universe for R.
- Harmonize the two implementations' option surfaces and document any
  intentional differences.

### Phase 3 — graduate the current research generation **[owner-gated]**
Preconditions: confirmation protocol E1–E4 complete, paper claim-set frozen.
- Port `minimal_rankdiff.py` / `rankdiff_kalman.py` into package modules,
  keeping the estimation-identification discipline (every parameter from a
  declared moment; no score-tuned defaults).
- Regression-lock the port against the reproduction commands and numbers
  recorded in `MODEL_STATUS.md` before exposing any API.
- R port follows the Python port as reference, the same way the v4.3 R
  translation was done.
- Release as v0.2; v0.1.x remains the documented v4.3 generation.

## 4. Rules for agents doing package work (binding)

1. Read `.claude/skills/stochastic-modeling/SKILL.md` first regardless of
   track — its data rules and pitfalls apply everywhere in this repo. For
   repository mechanics (layout, tests/CI, git hygiene, skills mirroring),
   `.claude/skills/repo-maintenance/SKILL.md` is the reference.
2. Package-track changes must not touch `llm_fitting/`, `tests/` defaults,
   frozen specs, or `MODEL_STATUS.md` results. The legacy guard and full
   pytest suite stay green at every commit, exactly as on the research track.
3. Preserve the packages' existing structure, naming, comment style, README
   text, and API conventions. These were deliberate design choices; improve
   by extension and correction, not by rewrite. If a rewrite genuinely seems
   necessary, stop and ask **[owner]**.
4. Authorship and attribution metadata (DESCRIPTION `Authors@R`, pyproject
   `authors`, LICENSE) are load-bearing. Never drop, rename, or placeholder
   an author when copying or consolidating trees.
5. Research results never justify package changes between graduations. If a
   bug found in package code also exists in research code (or vice versa),
   report it in both places but fix each on its own track's rules.
6. The packages ship a *generation*, not a snapshot: version numbers, NEWS,
   and docs must always say which model generation they implement.
7. File moves between tracks follow the plan above; anything not listed
   there is **[owner]**.
