# Variance–structure comparison: independent follow-up

2026-10-12. **EXPLORATORY DEVELOPMENT measurement.** The owner requested estimates in both directions after the completed D1 debate. No stochastic-model fit, simulation, gate, model change, adoption, or manuscript change. The design was written before this follow-up's raw measurements; it was informed by A9, not blind.

## Answer

Yes: named movement and the stationary ladder can constrain one another. The new data computation supports a close relationship between long-lag variance growth and local ladder slope across rank. It does not yet show that individual diffusion causes the stable structure. The missing quantities are independently measured cumulative drift/renewal, the mapping from permanent endpoint cohorts to current rank occupancy, and a validated link between long-lag variance growth and instantaneous diffusion variance.

The four panels are not four independent replications: the two comments panels overlap, and submissions share a platform with comments. The evidence spans two platforms.

## What was computed

Starting evidence: MODEL_STATUS §2z-bf and A9 cells at main commit c7c1767fc6aa5ddf48278b43567d9b000190c88a, read in this session. The original claim uses a/(2S), not a/S; its approximate 0.003/week Reddit and 0.0066/week Facebook figures refer to that convention. A9 is an exploratory measurement, not planted validation or a gate.

This independent diagnostic uses the same four previously exposed DEVELOPMENT panels, the registered read-only loader and universe definition, B=4K buffered membership, absence-penalized permanent ranks, and presence ≥0.7. K is 3,500 Facebook Era A, 12,500 comments, 5,000 submissions, and 12,500 comments extension. Full-window membership deliberately makes this descriptive. It must not be presented as out-of-time prediction.

After removing the common weekly mean change, compute D(h), pooled increment variance, and a_hat=[D(h2)−D(13)]/(h2−13). Both horizons use identical start weeks and the intersection of finite observations. No missing levels are imputed. Primary h2 is 26 weeks for Facebook and 52 for Reddit; 65 weeks is the prespecified Reddit annual-phase sensitivity. We do not assert that annual cycling, transient noise, trends, or censoring have thereby been eliminated.

At each current rank position, the time-median observed log(1+count) ladder supplies S, a local slope over an octave of log rank. Endpoint variance cohorts use permanent rank. This mismatch is acknowledged, not assumed away. Mean ladder slopes are a sensitivity. Neither time-median nor time-mean observed ladders are automatically the latent stationary quantile curve.

For each panel and reported band set, g_hat=median[a_hat/(2S)]. Then structure→variance gives a_pred=2g_hat S; variance→structure gives S_pred=a_hat/(2g_hat). MARE is the equal-band mean absolute relative error. These two errors describe the same restriction with different denominators; they are not two independent tests. Leave-one-band-out (LOBO) uses the other bands to estimate g and assesses rank interpolation only. It still uses the ladder and is not independent identification.

## Results: fixed seven octaves K/128 through K

This range was fixed in DESIGN.md before the new raw calculations. It excludes the sparse extreme head, and it does not cover the entire buffered universe.

| Panel | Horizons (weeks) | g from ladder | Variance → slope MARE | Slope → variance MARE | Spearman | LOBO slope MARE |
|---|---:|---:|---:|---:|---:|---:|
| Facebook Era A | 13–26 | 0.006423 | 11.5% | 11.2% | 0.750 | 13.8% |
| Reddit comments | 13–52 | 0.002811 | 9.7% | 10.1% | 0.857 | 12.8% |
| Reddit comments | 13–65 | 0.002815 | 11.0% | 9.3% | 0.893 | 14.8% |
| Reddit submissions | 13–52 | 0.003170 | 6.0% | 6.3% | 0.857 | 7.1% |
| Reddit submissions | 13–65 | 0.003267 | 8.3% | 9.0% | 0.857 | 10.5% |
| Reddit comments extension | 13–52 | 0.002807 | 9.1% | 8.5% | 0.857 | 12.4% |
| Reddit comments extension | 13–65 | 0.003259 | 5.5% | 5.5% | 0.964 | 7.6% |

The relationship survives matching origins and the same-phase comparison. Replacing the median ladder with the mean ladder produces slope errors of 11.7%, 9.3%, 5.3%, and 11.2% in the primary horizon comparison (Facebook, comments, submissions, extension). The corresponding same-phase Reddit errors are 10.7%, 7.7%, and 7.0%.

The longer comments panel's g rises from 0.002807 to 0.003259, about 16%, under the 13/65-week contrast. Its shape match improves. On short comments the scale barely changes while the local discrepancies move. These facts favor robustness of the broad shape relationship over precision of the absolute weekly rate. A 52-week phase comparison is a sensitivity, not a selected replacement estimator.

## Broader bands, preserving the original A9 convention

Here all A9 bands except its first are retained. Both the favorable interior comparison and this broader result must be reported.

| Panel | A9 original slope / variance MARE | Matched-origin slope / variance MARE |
|---|---:|---:|
| Facebook Era A | 16.4% / 14.5% | 17.1% / 15.2% |
| Reddit comments | 18.3% / 21.7% | 17.1% / 22.5% |
| Reddit submissions | 7.8% / 8.1% | 8.3% / 9.4% |
| Reddit comments extension | 13.8% / 13.1% | 12.6% / 12.6% |

Thus the broader shape errors are about 8–17% for slope reconstruction and 9–23% for variance reconstruction in the new matched-origin primary comparison. The original A9 all-but-top results satisfy the prior exploratory expectation of <30% slope MARE on all panels. This is not a threshold that adopts a model. All measured bins, including the excluded first bin, are retained in the individual JSON outputs; no rate was clipped to rescue a comparison.

The seven-band piecewise slope reconstruction, with one arbitrary additive anchor at K/128, has maximum absolute log-level discrepancies 0.130, 0.325, 0.123, and 0.206 for the four primary comparisons. The short-comments 13/65 reconstruction reaches 0.702. These are integrals of the band slopes, not a separately validated fit to raw endpoint sizes. Small local slope errors can accumulate to material level differences: 0.325 log units is about a factor 1.38 and 0.702 about a factor 2.02. “Slope error ≈10%” must not become “the full rank-size curve is predicted within 10%.” The absolute level and the clock are not independently predicted.

## Noise and uncertainty: what improved and what remains

Matching origins/masks removes a straightforward comparability problem in differences of variances. Paired coverage among the chosen endpoints exceeds 98.4% in every seven-band comparison. It does not validate full-window survival/membership selection, nor measure endpoints excluded by that selection. An exact annual cycle contributes equally to 13- and 65-week displacement variances only under suitable phase averaging and cross-covariance assumptions. Unequal origins and other periods remain concerns.

Four hundred circular calendar-block resamples of the common-origin moments use a block as long as the larger horizon. These give conditional descriptive 5–95% brackets. They hold membership and the observed ladder fixed and omit endpoint-sampling and denominator uncertainty. The available time series contains too few effectively distinct long blocks to claim nominal coverage.

| Panel / larger horizon | Origin weeks / block length | Conditional band halfwidth / rate, range | Endpoint-mean component, range | Drift-free Brownian benchmark |
|---|---:|---:|---:|---:|
| Facebook Era A / 26 | 2.31 | 24–96% | 26–74% | 54% |
| Reddit comments / 52 | 1.62 | 11–33% | 42–72% | 61% |
| Reddit comments / 65 | 1.09 | 3–10% | 55–83% | 75% |
| Reddit submissions / 52 | 3.08 | 16–70% | 20–41% | 36% |
| Reddit submissions / 65 | 2.26 | 14–64% | 24–47% | 45% |
| Reddit comments extension / 52 | 3.10 | 15–23% | 23–46% | 36% |
| Reddit comments extension / 65 | 2.28 | 13–23% | 27–51% | 45% |

The very narrow short-comments 65-week brackets are especially misleading as measures of precision: only 71 origins are available for a 65-week block. Circular resampling then nearly rotates the same entire record. Consequently the prior qualitative expectation that temporal uncertainty would weaken precision is supported for several panels, but these brackets are not a reliable universal test of ratio constancy. No p-value or confidence claim of a constant ratio is made.

The endpoint-mean component is an exact weighted total-variance decomposition of the paired increments, not an estimate of permanent drift variance. THEORY.md derives the drift-free Brownian benchmark in the last column. The observed fractions straddle or resemble those benchmarks. We therefore have no basis here to subtract this component as “trend” or conclude that it proves endpoint drift heterogeneity. This was a useful negative diagnostic.

Long-horizon growth can still contain drift heterogeneity, nonstationarity, jumps, or slowly decaying noise. Observation noise can also distort the ladder that serves as the denominator. No planted recovery or independent observation-noise instrument was available within this no-simulation/no-fit follow-up. This is improved noise management, not a certified latent-diffusion estimate.

## What the comparison says about the debate

It strengthens the case for seeking an endogenous generator of structure. A low-dimensional variance–drift–flow relation may replace some of the work currently done by a supplied ladder or home. It does not rehabilitate a supplied ladder as a causal explanation, and does not vindicate the current model's personal restoring force. Both must meet the new joint constraint.

Under zero-current stationary diffusion, the general relation is S(u)=u a_eff(u)/(2G(u)), with G the cumulative upper-tail restoring drift. The common-g formula is a special case. For named endpoint characteristics, a_eff is weighted by rank occupancy; neighbor covariance controls relative movement, and population turnover adds a current term. THEORY.md supplies the derivation, assumptions, counterexamples, and tagged primary literature.

The discriminating measurement is therefore **movement and renewal → predicted structure, with the structure withheld from calibration**. A practical sequence is:

1. Establish named innovation variance, transient-noise covariance, seasonal behavior, and drift heterogeneity using multiple horizons and independently defined endpoint cohorts. Reserve calendar windows for checks. Use independent repeated counts or disjoint observation streams when available; simple count thinning addresses sampling noise only, not all measurement error or activity dynamics.
2. Estimate the occupancy mapping and signed latent movement without selection on the same noisy rank measurement. Compare current-state behavior to permanent-type behavior. Measure covariances among potential neighbors, crossings, residence times, and displacement/return responses.
3. Separate genuine entry/death, temporary inactivity, crossing K, and loss of observation. Measure their flow across the ladder and the buffer. Define the common growth/centering reference independently.
4. Predict cumulative drift, local slope, and the integrated curve from those measurements. Check across time windows, universes, and observation resolutions before opening any new confirmation data. A change in variance at fixed independently measured drift implies a proportional slope change; measured renewal provides a competing prediction.

I would change my view toward a structural diffusion explanation if this independent forward calculation succeeds and named movements also agree. I would weaken it if the matching profile disappears with valid noise/selection correction, or if measured drift/current contradicts the inferred balance. Agreement between Claude and Codex supplies no evidence for either outcome.

## Later owner-provided A10 conclusions

After the independent calculations above, the owner supplied Claude's full A9/A10 conclusions. A10_REVIEW.md records the comparison and a newly found mismatch between log-rank weighting for the variance prediction and uniform-rank weighting for the temporal baseline. The corrected same-grid baseline errors are 0.462 and 0.470 versus variance errors of 0.528 on both: a 14% and 12% disadvantage, not a factor of three. The original ordering survives; the magnitude does not. The interpretation of 1/g as relaxation time is also unsupported without further model assumptions. These findings do not negate the static shape evidence above.

## Reproducibility and scope

Files: DESIGN.md (written first), measure.py, summarize.py, a9_bidirectional.json, four measure_*.json files, diagnostics.json, INPUT_MANIFEST.json, RESULTS_TABLE.md, THEORY.md and STATUS_ENTRY.md. The figure is locally generated and excluded from Git; summarize.py reproduces it. Input fingerprints and library versions are recorded. Runs were sequential and all completed (approximately 4, 42, 228 and 82 seconds); no heavy job exceeded five minutes.

Fixed-array checks passed for paired-mask variance against direct pooled variance, the total-variance decomposition, exact proportionality in both directions including LOBO, nonpositive-rate handling, and the known logarithmic slope. Summary arithmetic and the figure were inspected. No claim of full repository health checks: health_check.sh launches fits/simulations and writes outside this handoff, conflicting with the owner's task restrictions. No model, package, default, protocol, gate, or ledger file was edited. The two additional matched-grid audits completed in approximately 192 and 72 seconds, reproducing the original uniform-rank baseline to floating-point tolerance. Claude can append STATUS_ENTRY.md to the canonical record.
