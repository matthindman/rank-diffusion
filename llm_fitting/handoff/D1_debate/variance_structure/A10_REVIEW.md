# Review of Claude's pasted A9/A10 conclusions

2026-10-12. Read the owner's pasted text, MODEL_STATUS §2z-bh, A10 DESIGN.md, and phase1_a10.py at main 001fa74669cc7a135075b5b8d667b9a1d958ce14. The independent variance–slope measurements in REPORT.md were completed before this A10 inspection. This review is exploratory, not a gate.

## What is supported

A10's reported static reconstructions are consistent with our independent finding that long-horizon variance profiles track ladder slopes. Its V2 ladder RMSEs are 0.097, 0.274, 0.130 and 0.155 log units, versus single-power-law errors 0.252, 0.423, 0.199 and 0.391 (MODEL_STATUS §2z-bh, read in this session). I did not rerun A10's stochastic-model fits. Our matched-origin and annual-phase calculations provide a separate descriptive robustness check, rather than reproducing every A10 column.

A10 also reports that a quadratic in log rank has errors 0.034, 0.147, 0.072 and 0.139, better than V2 in every panel (§2z-bh). That does not discredit a mechanistic diffusion explanation, but prevents treating the power-law win alone as evidence of one. “Same number of free quantities” means the same number calibrated from the ladder; the variance reconstruction additionally uses an empirically estimated multiband profile. That profile is legitimate movement information, but it has sampling error and flexibility absent from a single power law.

## Corrections and qualifications

**The temporal baseline has mismatched rank weighting.** `predict_on_grid` computes variance-reconstruction error on unique rounded logarithmically spaced ranks. `ladder_moved_rmse` computes carry-forward error on every integer rank 1..K. The ratio of those two errors does not establish which method is better under a common loss. A10_AUDIT_DESIGN.md records this finding before our corrective calculation. Matched-grid results are appended below.

**The holdout is conditional shape transfer.** `predict_ladder(..., med2, K)` anchors the prediction at the second-half rank-K level. The carry-forward baseline is likewise level-adjusted with second-half information. Full-window universe selection precedes the split. Neither result is an independent forecast of the ladder's absolute level, and neither spends fresh confirmation evidence.

**g is not yet an observed incumbent-decline rate.** Under a zero-current stationary continuum model, a_eff/(2S)=G(u)/u, the average upper-tail drift magnitude. Identifying this with one common incumbent drift requires the model assumptions and the permanent-rank-to-occupancy mapping. With renewal, there is an additional integrated-current term. A10 estimates the ratio from structure and movement jointly; interpreting it causally before measuring those components would reverse the inference.

**1/g is not generally a relaxation time.** For the simple reflected diffusion dX=−g dt+√a dW+dL, set X=(a/g)Z and t=(a/g²)τ. The rescaled equation has unit drift and unit variance. Thus its intrinsic time unit is a/g²; finite boundaries add dimensionless dependence on their distance measured in units a/g. Knowing 1/g alone does not establish equilibration speed. Other named, rank, renewal and covariance mechanisms can introduce further slow modes. The claim that the ladder is “mostly inherited,” or that equilibrium cannot be tested in these panels, is not proved by the numerical reciprocals of g. We can test current-balance restrictions without observing a full relaxation from an arbitrary initial condition. Transient forecasts require their own dynamics.

**Noise cancellation is conditional.** Stationary noise contributes 2[C(0)−C(h)] to an increment variance. It cancels from two horizons if its covariance is effectively the same at both and observation windows/masks are comparable. Stationarity alone does not imply this. Trends and annual cycles can remain. Nor is long-lag variance growth necessarily instantaneous diffusion: for stationary OU dX=−κXdt+√a dW, D(h)=a(1−e^(−κh))/κ, whose long-lag slope tends to zero despite a>0. “Noise-managed variance proxy” is the supported description here.

**Bracket overlap does not test a stationary law.** The implied value shares the estimated g and observed ladder with the data comparison. A9/A10 endpoint bootstrap brackets omit long temporal dependence and ladder uncertainty. Our calendar-block checks also have inadequate effective block counts. The observed overlap frequencies are descriptive; there is no calibrated coverage test or proof of ratio constancy.

**Seasonality correction failure is not evidence that seasonality is absent.** A10's subtraction depends on the sinusoidal/coarse-group moment interpretation. Our 13/65 comparison preserves the broad relationship but changes the scale, notably by about 16% on the longer comments panel relative to 13/52. Neither is a completed identification of seasonal effects.

The current theory supports a concrete diagnostic target: independently measure cumulative signed drift and population flow, construct the correct relative variance with named-type occupancy and covariance, then test the stationary balance and temporal changes. It does not justify a model adoption or a claim that the existing personal-home mechanism has been established.

## Matched-grid temporal result

Both original uniform-rank baseline numbers were reproduced to floating-point tolerance. Reweighting the baseline to the exact A10 prediction grid gives:

| Panel | Variance reconstruction RMSE | Carry-forward RMSE, same log-rank grid | Variance / carry-forward | Original uniform-rank baseline |
|---|---:|---:|---:|---:|
| Reddit submissions | 0.527698 | 0.462310 | 1.141 | 0.167538 |
| Reddit comments extension | 0.527888 | 0.469634 | 1.124 | 0.194223 |

The variance reconstruction is about 14% and 12% worse, respectively, on the common loss—not a factor of three. This does not reverse the ordering. It materially changes the strength of the negative comparison. A10's prespecified half-window prediction condition still fails: its comparison to the second-half in-sample reconstruction uses the same log-rank weighting, and the submissions g change remains. No uncertainty interval for the difference between methods was estimated here.

Both are anchored at the observed future rank-K value and use full-window-selected universes. A proper forecast comparison still needs a training-only universe/anchor policy, identical evaluation weights, multiple time origins, and uncertainty. No new gate was run.

Audit output records 149 unique grid ranks for submissions and 156 for comments extension; rounding/deduplication means the nominal 200-point log grid has fewer distinct positions. The extension has 213 weeks in the registered loader, split 106/107, rather than exactly two 106-week halves. audit_a10.py only reloads those development panels and reconstructs the baseline, using A10's saved variance error; it does not rerun model fits.
