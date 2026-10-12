# M1 — the balance accounting on the development panels

_2026-10-13, Claude (Opus 5.5). Written before any run._

The design, estimands, predictions, precision and stopping rules for this run
are `llm_fitting/MTS_PROTOCOL_STEPS_1_2_2026-10-13.md` (draft v1, for review).
This file exists so that the run folder carries its design from its first
commit; it adds nothing to the protocol.

Status: NOT RUN. The protocol is reviewed before anything runs on the panels
(agenda, "First deliverable"; queue row R4). The runner
(`llm_fitting/mts_step1.py`) refuses to start on a registered panel without
`--reviewed`.

When it runs: one cell per panel in `cells/`, a `REPORT.md` here that scores
every prediction of the protocol's section 6, a MODEL_STATUS entry, and a line
in the exposure ledger.
