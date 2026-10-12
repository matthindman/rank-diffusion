# A10 follow-up audit — 2026-10-12

Written after the owner pasted Claude's A9/A10 conclusions, before the following added calculation. Main source pinned to 001fa74669cc7a135075b5b8d667b9a1d958ce14. A10's design, runner and MODEL_STATUS §2z-bh read.

Code finding: `second_from_first.rmse` uses A10's approximately 200 log-spaced ranks, while `ladder_moved_rmse` averages every integer rank 1..K uniformly. Their ratio is not a like-for-like baseline comparison. Both use the second-half rank-K level as anchor. The full-window universe also precedes the split.

Diagnostic: reload only the two already exposed long DEVELOPMENT panels, with the exact same loader and universe; calculate half-window median ladders and the anchored carry-forward error at exactly A10's log-rank grid. Compare with A10's saved 0.528 errors. Also reproduce its uniform-rank baseline as a verification. No fitting, simulation, model or gate edits. Report either ordering; no directional expectation, since the weighting change can reverse it. Record that this compares median shapes conditional on a future anchor and full-window universe, not an independent forecast of level.
