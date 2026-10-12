# Proof 7 audit, version 2 (Claude) — run record

**Who and when.** Claude Opus 5.5 (Anthropic, model id `claude-opus-5-5`), Claude Code
desktop session, 2026-10-12, about 22:57 to about midnight JST. One session, no
sub-agents, no messages to other sessions.

**Where.** `/Users/hindman/Documents/GitHub/rank-diffusion-codex-debate`, branch
`codex/d1-debate`. HEAD at the start: `93353608f9c2bc9da300046fd03d77155819ca18`. Every
file of mine is in `llm_fitting/handoff/D1_debate/proof7_audit/v2_claude/`. Nothing else
in the working copy was created, edited or committed by this session, and nothing was
pushed. The canonical records on `main` were read and not written.

**Other sessions in this working copy.** At the start, `proof7_audit/` already held an
audit by another session (files dated 22:26–22:51). I listed its file names and opened
none. That session was still writing: by the end, files there carried times up to
23:01 and four more had appeared (`CHECKS_FINAL.json`, `COMMANDS.md`, `MANIFEST.json`,
`validate_handoff.py`). If its manifest or validator enumerates the folder, my
sub-folder `v2_claude/` will be new to it. At 23:28 another session committed `5da9bfe`
(a Proof 9 prompt) on this branch; an untracked folder `proof9_direct_inverse/` and a
file `PROOF_9_RESULT.md` appeared later. I opened none of these. The reviewed proof
file had the same SHA-256 at the end as at the start.

**Reviewed artifacts.** Hashes are in `DESIGN.md`.

**Tools.** File reads; `git` status, log and show; Python scripts; one web fetch.
Registered interpreter (Python 3.11.6, numpy 2.0.1, scipy 1.14.0, pandas 2.x) for
`instrument_scalars.py` only. Scratch virtual environment outside the repository
(numpy 2.4.6, scipy 1.17.1, mpmath 1.3.0, sympy 1.14.0) for everything else. The
registered environment was not modified.

**Resource limits actually in force.** 10 cores, 16 GB, single process. No budget was
set by the brief. Longest script: `stageA_checks.py`, 3 min 50 s. All others under
15 s. No job went through `heavy_lock.py`; none was long enough to need it.

**Web.** One document retrieved and read in part: M. Scheutzow, *Stochastic Processes
III / Wahrscheinlichkeitstheorie IV*, lecture notes,
`https://page.math.tu-berlin.de/~scheutzow/WT4main.pdf` (408 KB; the fetch tool saved a
copy in the session's own tool-results folder, outside the repository). Pages read:
title, contents, 1–5, 9–12.

## Commands, in order

Run from this folder; `PY` is the scratch environment's interpreter.

| # | command | inputs | output | status |
|---|---|---|---|---|
| 1 | `PY stageA_checks.py` | `p7core.py` | `stageA_checks.json` | RAN-OK, 43 of 43 checks pass; rerun at the end against the final `p7core.py`, same result, 2 min 58 s |
| 2 | `PY pde_check.py` (first attempt) | `p7core.py` | none | KILLED by me after 10 minutes: a quadrature rule was being rebuilt on every call. Nothing from it was read or used |
| 3 | `PY pde_check.py` (after caching the rule and vectorizing one integral; numerics unchanged) | `p7core.py` | `pde_check.json` | RAN-OK, 11 s |
| 4 | `PY certificate.py` | — | `certificate.json` | RAN-OK |
| 5 | `PY sensitivity.py` | `p7core.py` | `sensitivity.json` | RAN-OK; rerun once after a reporting fix (below) |
| 6 | `PY info_bounds.py` | `p7core.py`, `sensitivity.py` | `info_bounds.json` | RAN-OK; POST HOC |
| 7 | `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 instrument_scalars.py` | five parquet files on T9 (`CONTACT_LOG.md`) | `instrument_scalars.json` | first start failed on a date-type error before reading anything; second RAN-OK |
| 8 | `PY convergence_check.py` | `p7core.py`, `sensitivity.py` | `convergence_check.json` | RAN-OK |
| 9 | `PY posthoc_variants.py` | `sensitivity.json` | `posthoc_variants.json` | RAN-OK; POST HOC |

`info_bounds.py` and `convergence_check.py` import `sensitivity.py`, which runs on
import and rewrites `sensitivity.json` with identical values.

## Deviations from `DESIGN.md`

| planned | done | why | decided before or after results | effect |
|---|---|---|---|---|
| Quadrature orders doubled until the relative change is below 1e-9 | One comparison at much higher orders. Largest change: 6e-9 in the Jacobian, 1.2e-7 in its smallest singular value (benchmark B2); below 1e-10 elsewhere | The slow-death benchmark converges more slowly in the age integral; the conclusion does not depend on the seventh digit | after | none on any reported figure |
| Variant "horizon 4 and `j_0 = 2`" | Evaluated as declared; it is worse than horizon 4 alone at benchmark B7 | `1 + 4 k_+ < 2` there | after | reported; the write-up recommends the minimum of the two |
| Fixed positive `epsilon` if an evaluator is completed | NOT RUN | no evaluator was built | before | stated in `SENSITIVITY.md` §5 and §8 |
| Variants W1–W4 only | Added, labelled POST HOC: W4 with nuisance known; entry mean unknown; rank feedback known to be absent; the information floor; the idealized high-count benchmark | the owner asked, mid-task, for assumptions or stylized facts that would tighten practical use | after the declared results had been seen | reported separately; no declared result changed |
| Post hoc "entry mean unknown" block | First version reported sample sizes from a pseudo-inverse of a singular matrix. Replaced by an identification flag before anything was written up | the first numbers were meaningless | after seeing them | those numbers appear nowhere |
| Stage A checks and Stage C sensitivity as separate steps | The Stage A determinant check assembles the full Jacobian at two test points and so showed its singular values before `sensitivity.py` ran | the check needs the matrix | `DESIGN.md` was saved before either ran | none |

## What was not run

Any stochastic simulation, fit, gate evaluation, health script or research-model code.
The bounded features at fixed positive `epsilon`. A rigorous enclosure of the Jacobian.
Any contact with sealed data. Any reading of the other audit, of Proofs 4–6, 8 or 9, or
of their status entries.
