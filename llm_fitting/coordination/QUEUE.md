# Task queue for Codex/Astra

Single writer: Claude, on `main`. Read the current version with
`git show main:llm_fitting/coordination/QUEUE.md`; the copy in your working
tree may be older. How to use it: `PROTOCOL.md` §5, "The queue".

| order | task | brief | branch | status | inputs from Claude |
|---|---|---|---|---|---|
| 1 | V1 — the gate as a measuring instrument | `briefs/V1_gate_instrument.md` | `codex/v1-gate-instrument` | DONE, merged to `main` (MODEL_STATUS §2z-ax) | none |
| 2 | V2 — planted mechanisms and recovery | `briefs/V2_planted_mechanisms.md` | `codex/v2-planted-mechanisms` | DONE, merged to `main` (MODEL_STATUS §2z-bc; what it changes is §2z-bd) | parts 1–3: none. Part 4 needs `llm_fitting/residual_anatomy.py`: READY on `main` since f284b9b (functions `from_long`, `tails_by_horizon`, `retention`, `departures`, `departure_summary`, `long_dispersion`; input format in its docstring). Added since, for the same part: an `anchor` scale in `tails_by_horizon`, and `cohort_fate`, `momentum`, `volatility_persistence` (design in `runs/2026-10-12_A7_followups/DESIGN.md`) |
| 3 | V3 — endpoint-level scores | `briefs/V3_endpoint_scores.md` | `codex/v3-endpoint-scores` | tables DONE at nominal rows, merged to `main` (MODEL_STATUS §2z-bg); calendar-aligned tables OPEN: rerun the four frozen exports and score `sim_rank_aligned` (see `handoff/V3_endpoint_scores/ANSWERS.md`) | the scorer and its tests: none. The retrospective table needs the export flag and its schema: READY on `main` (`--endpoint-export DIR` on the gate; `llm_fitting/handoff/A6_endpoint_export/SCHEMA.md`; `tests/test_endpoint_export.py` shows how to read a file) |

| R1 | Review of the decision document for D1 | `briefs/R_review.md`; the document is `llm_fitting/D1_CANDIDATES_CLAUDE_2026-10-12.md` on `main` | `codex/r-d1-review` | DONE, merged to `main` (MODEL_STATUS §2z-bi; every finding accepted, corrections in §2z-bk) | none |

| R2 | Review of A9 and A10 (variance against ladder slope, both ways) | `briefs/R_review.md`; MODEL_STATUS §2z-bf and §2z-bh; `runs/2026-10-12_A9_variance_ladder/`, `runs/2026-10-12_A10_two_way/` | done in the debate session on `codex/d1-debate` (`handoff/D1_debate/variance_structure/A10_REVIEW.md`) | DONE in substance, merged to `main` (MODEL_STATUS §2z-bj; A10 corrected in §2z-bk). Not covered and still open: a planted check of whether a flat ratio of two profiles can arise without a link. It belongs to step 2 of the proposed next phase and is not to be started as a separate task | none |

| R3 | Check of the integrated next-phase page | `llm_fitting/RESEARCH_AGENDA_MOVEMENT_TO_STRUCTURE_2026-10-12.md` on `main`: your agenda with my amendments marked | none: write `llm_fitting/handoff/R3_agenda_check/REVIEW.md` on your current branch | open, short: say where the page misstates your proposal, and which of my five amendments you reject and why. One round. The page is PROPOSED; the owner has not adopted it, so do not start any of its steps | MODEL_STATUS §2z-bk, §2z-bl |

Reviews and blind checks are added here as rows when they are needed, or
sent as direct calls.

## Log

- 2026-10-12 — queue opened with V1–V3 (MODEL_STATUS §2z-ar).
- 2026-10-12 — `residual_anatomy.py` is on `main` (f284b9b): V2 part 4 is unblocked.
- 2026-10-12 — V2 brief amended on `main` before V2 started: two more planted alternatives (time-varying endpoint volatility; a random-walk lasting level). Read the brief from `main`.
- 2026-10-12 — A1/A2 results are on `main` (MODEL_STATUS §2z-au, with a same-day correction on the tail yardstick). V2's rule stands: do not open them until V2 part 4 is committed.
- 2026-10-12 — four more statistics in `residual_anatomy.py` for V2 part 4 (the `anchor` scale, `cohort_fate`, `momentum`, `volatility_persistence`). One of them matters for your design: the `entity` tail ratio is higher for a series that wanders than for one that reverts, with Gaussian shocks in both; the `anchor` ratio is not.
- 2026-10-12 — the per-endpoint export is on `main`: V3's retrospective table is unblocked.
- 2026-10-12 — V1 reviewed and merged. Its record entry is MODEL_STATUS §2z-ax and its eight gate rows are in `GATE_LEDGER.md`. One change on integration: the two PNG figures are left out of git (the repository keeps images out; `gate_instrument_plot.py` regenerates them) and `llm_fitting/handoff/**/figures/*.png` is now ignored. Start V2 from current `main`: `git fetch` is not needed for a local branch, so `git switch -c codex/v2-planted-mechanisms main`.
- 2026-10-12 — for V2: Phase 1 on `main` now reads differently from the brief's "Why". Do not open the A-task reports before part 4 is committed (the brief's rule), but the list of planted alternatives is unchanged and stands as written.
- 2026-10-12 — V2 brief amended on `main` after you branched (your branch had no commit yet): two more planted cases, a level pulled toward the ladder at its current rank, and an endpoint-specific annual cycle. Read the brief from `main`.
- 2026-10-12 — row R1 added: review of the D1 decision document, after V2 part 4.
- 2026-10-12 — a debate on candidate 2 of the D1 document runs in a SEPARATE Codex session on branch `codex/d1-debate` (folder `llm_fitting/handoff/D1_debate/`). The V2 session does not read that folder until V2 part 4 is committed. Row R1 (review of the D1 document) is unchanged, except that its candidate 2 is under debate and may be replaced.
- 2026-10-12 — V2 reviewed and merged (§2z-bc). On integration two result archives (33 MB: `compact/diagnostics_seeds.jsonl.gz`, `diagnostic_pairs.csv.gz`) were left off `main` and ignored; they stay on your branch, checksums in merge commit 98a6e15. Your results changed three of my readings and strengthened three (§2z-bd). The blind is lifted: you may now read every A-task entry.
- 2026-10-12 — next for the V2 session: V3 (row 3), then R1. Note for R1: candidate 2 of the D1 document is withdrawn in the form written and is being debated in a separate session (`handoff/D1_debate/`); review candidates 1, 3, 4 and the margin, and say what you think of §2z-bd.
- 2026-10-12 — the debate on candidate 2 is closed: signed consensus in `handoff/D1_debate/CONSENSUS.md`, recorded as MODEL_STATUS §2z-be, branch `codex/d1-debate` merged. Candidate 2 is withdrawn. For R1: review candidates 1, 3 and 4 and the margin only; nothing to review in candidate 2 beyond the consensus.
- 2026-10-12 — V3 reviewed and merged (§2z-bg). Your timing finding is confirmed. Q1 is answered in `handoff/V3_endpoint_scores/ANSWERS.md`: the export now also writes `sim_rank_aligned`; the gate is untouched. Next for you: the calendar-aligned tables, then R1.
- 2026-10-12 — for information: the owner asked for a first check of whether the lasting-variance profile predicts the ladder's local slope (§2z-bf). A two-way estimate follows. A review row will be added when it is written up.
- 2026-10-12 — R1 reviewed and merged (§2z-bi). All five findings accepted. The A4 half-sample comparison is rerun on a common coordinate (§2z-bk); the decision document has a second status section with your corrections and a current decision list.
- 2026-10-12 — your variance–structure follow-up, A10 review, agenda and proof prompts from the debate session are merged (§2z-bj). Your A10 finding is confirmed: I reproduced the matched-grid numbers and scored both curves under both weightings (§2z-bk). §2z-bh is corrected, including the statements about 1/g.
- 2026-10-12 — your agenda is carried into the program's documents as a PROPOSAL, with my amendments marked: `llm_fitting/RESEARCH_AGENDA_MOVEMENT_TO_STRUCTURE_2026-10-12.md` (§2z-bl). The owner has not adopted it. Row R3 asks you to check that page. Row R2 is closed in substance by your A10 review.
- 2026-10-12 — still open from before: the calendar-aligned V3 tables (row 3).
