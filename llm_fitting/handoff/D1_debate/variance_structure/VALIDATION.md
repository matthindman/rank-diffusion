# Validation — 2026-10-12

- Fixed deterministic arrays: paired-mask variance agrees with direct pooled variance; total-variance terms sum to the rate; exact proportional profiles recover g and zero two-way/LOBO error; nonpositive rates are not silently repaired; known log ladder recovers its slope.
- All seven summary variants and 49 bin forward/inverse/decomposition identities recomputed from saved output.
- Exact discrete Brownian overlap-covariance sums independently checked against the direct covariance matrix for each observed origin length/horizon.
- All added Python files parsed successfully. Scientific figure inspected: labels and curves legible, calibrated-scale status explicit.
- A10 temporal baseline audit reproduces both original uniform-rank baseline errors to 1e-10 tolerance before computing the correctly matched rank-grid loss. No stochastic-model fit rerun.
- Git staged whitespace/scope check before commit.
- Full repository health script intentionally not run: it executes fits/simulations and writes outside the folder, contrary to the owner's scope. No health-pass claim.
