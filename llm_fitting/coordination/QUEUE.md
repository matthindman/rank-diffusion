# Task queue for Codex/Astra

Single writer: Claude, on `main`. Read the current version with
`git show main:llm_fitting/coordination/QUEUE.md`; the copy in your working
tree may be older. How to use it: `PROTOCOL.md` §5, "The queue".

| order | task | brief | branch | status | inputs from Claude |
|---|---|---|---|---|---|
| 1 | V1 — the gate as a measuring instrument | `briefs/V1_gate_instrument.md` | `codex/v1-gate-instrument` | open | none |
| 2 | V2 — planted mechanisms and recovery | `briefs/V2_planted_mechanisms.md` | `codex/v2-planted-mechanisms` | open | parts 1–3: none. Part 4 needs `llm_fitting/residual_anatomy.py`: READY on `main` since f284b9b (functions `from_long`, `tails_by_horizon`, `retention`, `departures`, `departure_summary`, `long_dispersion`; input format in its docstring). Added since, for the same part: an `anchor` scale in `tails_by_horizon`, and `cohort_fate`, `momentum`, `volatility_persistence` (design in `runs/2026-10-12_A7_followups/DESIGN.md`) |
| 3 | V3 — endpoint-level scores | `briefs/V3_endpoint_scores.md` | `codex/v3-endpoint-scores` | open | the scorer and its tests: none. The retrospective table needs the export flag and its schema: READY on `main` (`--endpoint-export DIR` on the gate; `llm_fitting/handoff/A6_endpoint_export/SCHEMA.md`; `tests/test_endpoint_export.py` shows how to read a file) |

Reviews and blind checks are added here as rows when they are needed, or
sent as direct calls.

## Log

- 2026-10-12 — queue opened with V1–V3 (MODEL_STATUS §2z-ar).
- 2026-10-12 — `residual_anatomy.py` is on `main` (f284b9b): V2 part 4 is unblocked.
- 2026-10-12 — V2 brief amended on `main` before V2 started: two more planted alternatives (time-varying endpoint volatility; a random-walk lasting level). Read the brief from `main`.
- 2026-10-12 — A1/A2 results are on `main` (MODEL_STATUS §2z-au, with a same-day correction on the tail yardstick). V2's rule stands: do not open them until V2 part 4 is committed.
- 2026-10-12 — four more statistics in `residual_anatomy.py` for V2 part 4 (the `anchor` scale, `cohort_fate`, `momentum`, `volatility_persistence`). One of them matters for your design: the `entity` tail ratio is higher for a series that wanders than for one that reverts, with Gaussian shocks in both; the `anchor` ratio is not.
- 2026-10-12 — the per-endpoint export is on `main`: V3's retrospective table is unblocked.
