# V1 simulation archive

Exploratory measurement of the unchanged gate on the four development panels.
The design, report, record entry and ledger rows are in
`llm_fitting/handoff/V1_gate_instrument/`.

Tracked summaries retain the exact input/source hashes, commands, software,
seed ranges, attempts, fitted-parameter summaries, per-origin empirical and
simulated moments, actual gate results, resampling summaries and an ordered
SHA-256 digest of every seed file. `AUDIT.json` is written only after all four
panels complete and pass reconstruction checks.

The 28,800 primitive binary seed files remain local in this working copy and
are excluded from Git. They contain sparse displacement histograms and scalar
RACF/collision values; they can be rebuilt using the committed launcher and
the identified development panels. Each seed file is an atomic checkpoint.
Do not interpret partial runs or mix caches whose source/input hashes differ.

`analysis/` contains the completed finite-bank resampling summaries. Its bands
condition on the fixed empirical panels and fits; they do not cover temporal,
estimation, population, or finite-bank uncertainty. Layout-test fixtures are
synthetic and excluded from Git; only the final handoff figures are results.
