# Tail Estimator Audit Handoff

## Scope

This report covers the polynomial-in-log-`alpha` tail estimator in:

- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_analysis.py`
- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_explainer.md`
- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_onepager.md`

The goal was to reconcile the implementation with the equations in the explainer, then refresh the published example outputs from the corrected code.

## Changes Made

### 1. Fixed the polynomial normalization in the code

The implementation of `P(s)` in `log alpha` was off by one factorial order.

- Intended equation from the explainer:
  - `P(s) = eta0 * s + 1/2 * eta1 * s^2 + 1/6 * eta2 * s^3 + ...`
- Old code behavior:
  - degree 2 used `eta1 * s^2`
  - degree 3 used `1/2 * eta2 * s^3`
- New code:
  - `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_analysis.py`
  - `_log_alpha_poly(...)` now uses `(k + 1)!` in the denominator, which matches the documented model exactly.

Effect on outputs:

- The degree-1 model was unchanged.
- The degree-2 and degree-3 tail masses increased because the higher-order steepening terms are now weaker than the buggy implementation had implied.

### 2. Fixed predictions for ranks above the censoring boundary

The plotting path for `r < r_c` was not using the signed integral implied by

- `log y(u) = log y0 - integral_0^u alpha(s) ds`

Old behavior:

- For negative `u`, the code evaluated the positive-side tail polynomial at `-u`.
- That produced a curve that was not the same model shown in the math.

New code:

- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_analysis.py`
- `_integrated_alpha(...)` now returns the signed integral `integral_0^u`.
- `predict_poly(...)` now uses the same signed integral for both `u > 0` and `u < 0`.

Impact:

- Tail mass estimates were already based on `u > 0`, so totals were unaffected by this fix.
- The fitted degree-2 and degree-3 curves shown in the observed region are now mathematically consistent with the written model.

### 3. Removed the overflow warnings in quadrature

The old implementation emitted `overflow encountered in exp` while integrating very steep tails.

New code:

- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_analysis.py`
- Added `_exp_from_log(...)` with clipping at standard double-precision limits.
- The tail integrand now returns `0.0` once the log-integrand is below the underflow threshold.

Impact:

- The estimator run completes without the previous NumPy overflow warnings.
- The remaining `sysctlbyname` messages come from PyArrow in the sandboxed environment, not from the estimator math.

### 4. Fixed early truncation in near-power-law tails

The original numerical integration always stopped at `u = 30`.

That was fine when curvature was sizable, but it undercounted the tail when
the curvature coefficients were positive and very small. In those cases the
tail behaves almost like a power law for a long interval before the
super-exponential suppression becomes relevant.

New code:

- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_analysis.py`
- Added `_tail_remainder_bound(...)` using monotonicity of `alpha(u)`.
- `tail_mass_poly(...)` now integrates in expanding chunks until the remaining
  mass bound is below tolerance, instead of assuming `u = 30` is always enough.

Impact:

- Near-power-law cases are now continuous with the power-law limit.
- Example stress test:
  - `alpha0 = 1.01`, `eta0 = 1e-6`
  - old degree-1 tail integral: about `25.9`
  - new degree-1 tail integral: about `99.0`
  - power-law benchmark: `100.0`

### 5. Improved degree-1 numerical stability near `eta = 0`

New code:

- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_analysis.py`
- `predict_deg1(...)` now uses `expm1(...)` instead of `exp(...) - 1`.

Impact:

- Avoids cancellation error when `eta0 * u` is very small.

### 6. Corrected the documentation

The explainer previously said the degree-1 case had a closed form involving `erfc`, but the formula shown was still an integral. That statement was inaccurate.

Updated text:

- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_explainer.md`
- It now says the degree-1 case reduces to a one-dimensional integral and can be written using the upper incomplete gamma function after substitution.
- It now also notes that the numerical integration range is extended adaptively for near-power-law cases.

Published example tables were also refreshed from the corrected code:

- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_explainer.md`
- `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/tail_estimation_onepager.md`

## Verification Run

Command used:

```bash
MPLCONFIGDIR=/tmp/mpl python3 llm_fitting/tail_estimation_analysis.py
```

Regression test command:

```bash
MPLCONFIGDIR=/tmp/mpl python3 -m unittest discover -s tests -p 'test_*.py'
```

Key corrected outputs from that run:

### Week `2021-11-29`, `r_c = 5000`

- `alpha0 = 1.986`
- `eta0 = 0.7771`
- `eta1 = 0.3799`
- `eta2 = 0.0000`
- PL = `10.0%`
- D1 = `5.7%`
- D2 = `5.4%`
- D3 = `5.4%`

### Week `2022-02-07`, `r_c = 5000`

- `alpha0 = 1.963`
- `eta0 = 0.3143`
- `eta1 = 1.0352`
- `eta2 = 5.1505`
- PL = `9.5%`
- D1 = `6.8%`
- D2 = `5.6%`
- D3 = `5.0%`

### Threshold sensitivity for `2021-11-29`

- `r_c = 3000`: `24.8%, 13.6%, 12.5%, 12.3%`
- `r_c = 5000`: `10.0%, 5.7%, 5.4%, 5.4%`
- `r_c = 8000`: `2.1%, 1.4%, 1.2%, 1.2%`

## Suggested Audit Checklist For Another Model

1. Re-derive the polynomial term normalization from the equation in the explainer and confirm the code now matches it exactly.
2. Check the signed-integral logic for `u < 0` and confirm the plotted in-sample curve is now the same model defined for the tail.
3. Review the clipping used in `_exp_from_log(...)` and confirm it is numerically safe for the parameter ranges produced by the Facebook examples.
4. Re-run `MPLCONFIGDIR=/tmp/mpl python3 llm_fitting/tail_estimation_analysis.py` and confirm the markdown tables match the script output.
5. Spot-check that the monotone ordering `PL >= D1 >= D2 >= D3` still holds across the published examples after the fix.
6. Check the near-power-law stress case `alpha0 = 1.01`, `eta0 = 1e-6` and confirm the degree-1 result stays close to the power-law limit rather than collapsing because of early truncation.

## Residual Notes

- A lightweight regression suite now exists at `/Users/hindman/Documents/GitHub/rank-diffusion/tests/test_tail_estimation_analysis.py`.
- Verification here was done both by rerunning the analysis script and by running the regression tests.
- The fallback in `estimate_log_alpha_poly(...)` still sets missing staggered fits to a flat `eta` path. That behavior was not changed in this pass.
