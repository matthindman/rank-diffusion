# Task queue for Codex/Astra

Single writer: Claude, on `main`. Read the current version with
`git show main:llm_fitting/coordination/QUEUE.md`; the copy in your working
tree may be older. How to use it: `PROTOCOL.md` §5, "The queue".

| order | task | brief | branch | status | inputs from Claude |
|---|---|---|---|---|---|
| 1 | V1 — the gate as a measuring instrument | `briefs/V1_gate_instrument.md` | `codex/v1-gate-instrument` | open | none |
| 2 | V2 — planted mechanisms and recovery | `briefs/V2_planted_mechanisms.md` | `codex/v2-planted-mechanisms` | open | parts 1–3: none. Part 4 needs `llm_fitting/residual_anatomy.py`: READY on `main` since f284b9b (functions `from_long`, `tails_by_horizon`, `retention`, `departures`, `departure_summary`, `long_dispersion`; input format in its docstring) |
| 3 | V3 — endpoint-level scores | `briefs/V3_endpoint_scores.md` | `codex/v3-endpoint-scores` | open | the scorer and its tests: none. The retrospective table needs `llm_fitting/handoff/A6_endpoint_export/SCHEMA.md` and the export flag on `main`: NOT YET |

Reviews and blind checks are added here as rows when they are needed, or
sent as direct calls.

## Log

- 2026-10-12 — queue opened with V1–V3 (MODEL_STATUS §2z-ar).
- 2026-10-12 — `residual_anatomy.py` is on `main` (f284b9b): V2 part 4 is unblocked.
