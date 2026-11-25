# Astra development pass on the common rank-diffusion model

OpenAI Astra · 2026-11-25 · development evidence · repository base `7ebb7613f0f1e3fd6599fdacb8ccae3ee1bed5de`

## 1. Executive verdict and adoption recommendation

**Keep Round 3 as the cross-platform reference. Retain the robust endpoint-volatility filter as a development option. Reject the bundled S3 replacement.** Submissions has a genuine movement improvement, but the candidate still loses to historical mobility there. The surge check diagnoses a failure of the isolated positive-wave interpretation. Turning the wave off also removes some of the empirical behavior that motivated S3.

| Five-origin Gate v1 error | Round 3 S2 | Astra S2 | Round 3 S3 | Astra S3 |
|---|---:|---:|---:|---:|
| Facebook | .105 ± .043 | **.084 ± .037** | .108 ± .023 | **.078 ± .029** |
| Instagram apparatus | .296 ± .216 | **.282 ± .219** | .360 ± .276 | **.304 ± .230** |
| Reddit Comments | **.129 ± .071** | .134 ± .085 | **.098 ± .033** | .118 ± .064 |
| Reddit Submissions | .346 ± .064 | **.263 ± .067** | .310 ± .125 | **.214 ± .078** |

On Submissions, S2 improved relative to Round 3 on all five splits. S3 improved on four. Across split medians, the one-week displacement moved from 5.2 to 4.2 for S2, against an observed 3.6. At 13 weeks it moved from 11.6 to 10.6, against 7.4. Both overshoot the gate’s RACF1 statistic, with the S2 five-split mean moving from .723 to .781 against .679 observed. Historical mobility scores .165 ± .044 and wins 5/5 against candidate S2 and 3/5 against candidate S3.

Submissions also improves on the individual forecasts. For S2, activity CRPS and log-rank CRPS fall at all five horizons and coverage moves from .944–.952 to .924–.935. The corresponding S3 coverage is .911–.926. PIT SD remains below .289 on Submissions. Instagram pays a measurable cost. Its S2 activity CRPS worsens by .00135–.00205 at all three horizons, and its 90% coverage falls to .886–.890.

Across the 17 panel–horizon combinations, S2 gains against Round 3 on activity CRPS in 13, log-rank CRPS in 10, and ensemble-median MAE in all 17. S2's head shift MAE beats both zero shift and EWMA in 14/17, up from 11/17. Its 39-member activity median beats both point baselines in only 2/17, while its analytical point does so in 15/17. These counts describe correlated outcomes. Section 5 gives the five-origin paired dispersion by cell. On Submissions at 52 weeks, the bundled S3's head-shift correlation drops from .807 to .461 and its MAE increases from .000839 to .000920.

The factorial comparison separates two different improvements. Holding the Round 3 S3 process fixed and changing only the filter gives Submissions .198 ± .079, 100% gate median consistency and 3/5 wins over historical mobility. It retains Round 3’s unconditional surge-generated decile curve by construction, but gives .124 ± .057 on Comments. Conversely, changing only the surge sign check yields .090 ± .040 on Comments and .303 ± .055 on Submissions. Choosing a different estimator per platform after seeing these scores would violate the intended common specification.

The full-panel S3 head-turnover correction is real on Facebook and Comments. Facebook top-10 turnover moved from .550 to .350 [.250, .458], against .350 observed. Comments moved from .450 to .250 [.200, .372], against .267. On Comments the final head share nevertheless rose to .671 [.604, .803], against .506, and D changed from +.149 to −.050 [−.059, −.040], against +.132 [.109, .155]. On Instagram, D moved from +.082 to +.005 [−.014, .028], still outside the empirical interval [−.122, −.050].

The date audit changes the interpretation of the supplied forward test. On the exact January 2021–December 2022 Submissions target, the S3 boundary ratio moves from 1.263 to .971 [.953, 1.001], against .891 observed. Its 2022 head share on matched 51-week blocks worsens from .462 [.416, .518] to .528 [.457, .653], against .357. Forward D changes from +.109 [.086, .138] to −.018 [−.046, .020], against +.146. This trade-off is large enough to decide against a universal S3 replacement.

## 2. Mathematical thinking and root-cause analysis

### Lasting volatility on Submissions

A correlation between two estimates of endpoint volatility can persist even when every endpoint has the same lasting diffusion. For entity i and lag h, the core implies the following increment variance at fixed rank and level.

$$
D_i(h) = h q_i v_i^{\gamma_L} + 2 V_{\xi,i}(1-\phi^h)v_i + 2 R_i v_i.
$$

The fast and observation components appear at both lags. Also, a ratio of two marginal standard deviations lacks the correlation factor needed for a regression coefficient. The cross-half lag-13/lag-1 spread ratio on Submissions was 0.963. The cross-lag slope was 0.856. Neither quantity isolates the coefficient of h. The diagnostic fitted the coefficient of h in the opposite half inside three permanent-rank strata, paired the halves in both directions, and compared high and low thirds of training volatility.

| High/low ratio, six stratum-half cells weighted equally | Facebook | Instagram | Comments | Submissions |
|---|---:|---:|---:|---:|
| Observed one-week variance | 3.32 | 3.33 | 3.85 | 2.85 |
| Fitted lasting variance | 1.20 | 1.17 | 0.58 | 0.70 |

Nonnegative variogram fits hit zero in some cells. These estimates support persistent heterogeneity in fast variance and leave the lasting coefficient weakly identified. They give little reason to increase lasting variance in historically volatile accounts. Under the old optional coupling, the Submissions first-band posterior mean of v^γ is 1.263 and its median v is 1.022. The full-window raw variance in that band already exceeds the baseline one-week smoothed variance by a factor of 1/0.735. For an unclipped mean-one lognormal, E[v^γ] = exp[(γ²−γ)s²/2]. At the full Submissions γ = 0.884 and s = 0.718 this equals 0.974. Dividing by it would increase the first-band scale further. Global normalization misses both rank dependence and the distinction between median movement and the second moment. The candidate retains γ = 0.

A single-switch control tested that explanation under the immutable Round 3 model. Turning on the old γ fit yielded exactly zero on every Facebook and Instagram gold origin. Relative to γ = 0, five-origin mean activity CRPS increased at 1, 4, 13, 26 and 52 weeks by +0.00031, +0.00032, +0.00075, +0.00268, +0.01234 on Comments and +0.00033, +0.00062, +0.00082, +0.00194, +0.00375 on Submissions. Submissions 52-week coverage moved from .944 to .914 while CRPS deteriorated. Changing the marginal width can help coverage while putting forecast dispersion on the wrong endpoints. [Isolated coupling control](diag_gamma_control.json)

Rare changes degrade the measurement of an individual endpoint's usual volatility. On Submissions, early log mean-square change correlated 0.501 with late log squared median absolute change after rank adjustment. Using early squared median absolute change raised the correlation to 0.678. At 100 weeks in the planted study, replacing one observation with a signed 3-log-unit contamination at each endpoint raised average log-scale error from 0.181 to 0.379 for the original filter and from 0.225 to 0.233 for the candidate. The original estimator was more efficient on clean draws.

### A conditional median moment for the endpoint filter

For each endpoint, the instrument is the median of its absolute, cross-sectionally centered one-week changes at consecutive observed weeks. For trial v, the candidate evaluates that same absolute median under the fitted stationary, locally fixed-rank increment law. The simulated increment is the sum of the lasting innovation, the stationary AR increment, the difference of two observation errors and, where present, drift and the change in the surge state. The non-scaling lasting variance floor therefore remains in the inversion. The median of a sum generally differs from a sum of component medians.

Numerical integration uses 8,192 common draws independent of the evaluation seeds. The calculation interpolates the ratio of median absolute change to RMS in permanent rank and restores the endpoint's modeled RMS at its own level. The prior is the existing lognormal volatility law. Its working likelihood compares the logs of observed and model medians. A finite difference between the model's 45th and 55th percentiles estimates the density at the median. With m the median and f its density, the asymptotic standard error of log m is 1/(2mf√n). The implementation uses half the adjacent-pair count as its working effective n, integrates at 41 points within the evaluator's support, and returns the posterior arithmetic mean and log-scale SD through the original interface. Fewer than four changes leave the prior prediction in place.

This is an approximate empirical-Bayes measurement. Dependence can extend beyond one week, estimated rank is uncertain, and the evaluator compresses the posterior into a clipped lognormal draw. The previous estimator used raw squared changes, a 3/n variance approximation and forced a mean of one over the selected training top K. Under either estimator the latent population prior has unit-mean scale by convention. The particular observed cohort need not share that mean.

### Identifying a fading positive surge

Write the stationary surge as S(t) = ρ S(t−1) + J(t), with independent, positively skewed arrivals. For a = ρ^h, the third cumulant of its h-week change equals 3κ₃(S)a(1−a). At short horizons relative to the half-life this grows approximately linearly with h. A random walk with independent skewed increments has κ₃(ΔₕL) = hκ₃(η). The measured Reddit third cumulants grow almost linearly across the original grid. With a signed linear term allowed, an unconstrained eight-week-wave fit returns a slightly negative amplitude on both Reddit panels. Slow positive surges are therefore one possible interpretation of the marginal fit. Pooled fifth cumulants also include covariance between conditional variance and conditional skew, which can inflate a jump-size estimate in heterogeneous data. Round 3's C5/C3 inversion uses the leading rare-arrival ratio for size and the exact capped third cumulant for rate.

For zero-mean strata the exact pooling identity is κ₅(pool) = mean κ₅(stratum) + 10 Cov(stratum variance, stratum κ₃). Consequently the marginal ratio can identify a large amplitude even when most entities experience small events. At rank 1 the Round 3 scale factors are 0.580, 0.690, 0.651 and 0.501. The corresponding maximum *single arrival* adds 3.91, 2.68, 4.67 and 3.08 log units. On Comments that is a 107-fold multiplier on activity plus one before other terms. Event selection from a large competitive field and exponentiation for audience shares make such a cap consequential. A frequency-domain diagnostic with bounded empirical moments still found a Comments jump size of 1.41. Heterogeneity is a source of distortion, while the data also sustain large marginal skew under an alternative estimator.

The candidate checks an independent implication. Let x = S(t)−S(t−h), y = S(t+g+h)−S(t+g), with a one-week gap. The old state and all future arrivals are independent. Only the −ρᵍ(1−a)S(t) term in y contributes to E[x²y]. Expanding x in the old state and intervening arrivals gives

$$
E[x^2y]=-\kappa_3(S)\,\rho^g(1-a)^2(1+2a).
$$

A positive fading wave produces a negative value. Independent symmetric processes make zero contribution to the central mixed cumulant under the local model. The candidate pools dimensionless central moments at 4, 8 and 13 weeks. It suppresses the positive wave when the pooled estimate exceeds 1.645 working entity-cluster SEs above zero. Each endpoint is one cluster across all lags. This is a one-sided specification check, conditional on calendar demeaning and empirical scale estimates. Common shocks, informative missingness and rank-dependent feedback can affect it. The calendar rule, significance level and range of lags were chosen on development data before candidate scoring. The gold faller–riser statistic and its deciles do not enter estimation.

On the four full panels, the standardized mixed moment ± working SE was +0.078 ± 0.015, +0.041 ± 0.007, +0.038 ± 0.010 and +1.027 ± 0.146. All four full fits reject the isolated positive-wave interpretation. Shorter training windows sometimes retain it. Across the five gold origins, the sign check triggered on 2/5, 1/5, 4/5 and 5/5 windows, respectively. A trigger can coincide with an already inactive Round 3 wave. On Instagram the sign-only ablation leaves the rolling endpoint scores identical to Round 3 S3 even though the full-panel fit changes. On planted positive waves at half-lives 8 and 52, the check suppressed 0/24 at each of three panel lengths in each case. A negative transient plus positively skewed lasting steps triggered it 24/24 at each length. This rival illustrates how the two signs can coexist. It does not identify the platform mechanism.

Within the original gapped windows, the 3,902 Instagram endpoints observed every week still have positive eight-week κ₃ (+0.068) and negative D (−0.071, 90% entity band [−0.126, −0.012]). Their faller slope is −0.079 and their riser slope is −0.008. The 4,954 eligible intermittently observed endpoints give D = −0.095 [−0.136, −0.046]. Negative D here means the biggest falls reverse more strongly than the biggest rises. Intermittency is insufficient to explain the sign. Both groups remain subject to the same post-query measurement apparatus. Selection into the complete group limits causal interpretation. [Presence diagnostic](diag_presence.json)

### Why the analytical activity forecast beats a 39-member median

The frozen analytical S2 point uses L̂+φʰξ̂ measured against the final training median. Each ensemble member includes balancing drift, state uncertainty and future innovations, then subtracts its own future observed top-K median. The diagnostic used every gold origin, increased the ensemble from 39 to 195, and recomputed one-week errors using both denominators. The small fraction of departed original IDs was excluded from this diagnostic. The mandatory benchmark uses its prescribed survival clone.

| Five-origin mean | Analytical MAE | Fixed median, M=39 | Future median, M=39 | Fixed median, M=195 | Future median, M=195 | Future minus training median | Raw mean minus analytical |
|---|---:|---:|---:|---:|---:|---:|---:|
| Facebook | .40275 | .41129 | .41163 | .40452 | .40483 | +.01554 | −.00849 |
| Instagram apparatus | .63702 | .64987 | .66178 | .63964 | .65150 | +.10222 | −.01064 |
| Comments | .28578 | .29223 | .29231 | .28709 | .28715 | +.00941 | −.00376 |
| Submissions | .16628 | .17243 | .17343 | .16747 | .16846 | +.01874 | −.00269 |

The medians' Monte Carlo error explains most of the Facebook and Comments gap. Instagram also has a material denominator shift. In a separate 39-draw decomposition on Instagram, the median of the analytical point vector among last-week present endpoints lay 0.294 below the training median. Using a simulated observation mask moved it to −0.223, drawing initial states to −0.156, then adding the weekly transition and observation innovations to +0.102. Selection of the largest observed values transmits dispersion into a higher denominator. A uniform shift to every simulated level cancels from relative X and ranks. Substituting the training denominator for simulated totals would change the forecast target. Neither operation addresses the data-generating mechanism. In all four panels the mean uncentered draw minus analytical point agreed with the measured balancing drift to within 0.0002 log units.

## 3. Parameter count and complexity

| Full-window active scalar count | Facebook | Instagram apparatus | Comments | Submissions |
|---|---:|---:|---:|---:|
| Round 3 S2 → Astra S2 | 11 → 11 | 11 → 11 | 11 → 11 | 11 → 11 |
| Round 3 S3 → Astra S3 | 14 → 11 | 14 → 11 | 15 → 12 | 15 → 12 |

The same S3 code can retain all 15 on a shorter window. The exact-calendar Comments training fit retains all 15, while the exact-calendar Submissions fit uses 12. Counts follow the incumbent’s convention for active structural coefficients. The sign decision itself is an additional model-selection operation.

The core has eleven fitted structural scalars. Two describe lasting SD by level. Three describe passing persistence and rank-dependent passing SD. Two describe weekly observation SD, two describe intermittent absence, one is the volatility spread, and one is the bottom exit hazard. Per-band variograms and a profiled AR persistence identify the first seven. The Facebook Spec-B instrument supplies a lower-bound profile. Cross-half covariance of log one-week variances identifies the spread. Active-span absence fractions and terminal disappearance counts identify the last three observation/exit quantities, counting the two absence coefficients. The candidate uses the same aggregate fit and simulator for S2.

S3 can add three scalar surge parameters from the marginal third and fifth cumulants and one drift variance from gapped covariance after accounting for the passing and surge components. Drift half-life is fixed at 52 weeks. The directional check and the median instrument introduce zero platform-fitted scalars. The complexity ceiling for the evaluated specification is 11 for S2 and 15 for S3. The inherited optional gamma ablation is off. The training ladder, empirical innovation and rebirth pools, drift refinement, and endpoint-specific state estimates remain substantive estimated inputs. Evaluating a likelihood on a numerical grid costs computation even though its grid points are not fitted parameters. Conditional gapped persistence is related to the covariance used to fit drift. D and the full decile curve have a stronger claim to being checks outside the fitting moments.

## 4. What we tried and rejected

| Hypothesis or intervention | Measurement and disposition |
|---|---|
| Raw long/short volatility spread identifies the lasting exponent | Opposite-half Submissions lasting high/low ratio 0.70 versus weekly variance ratio 2.85. Retain the homogeneous lasting shock. The archived Stage 2 comparisons also changed φ on three panels. The precise share of the old CRPS loss attributable to γ alone is unidentified by those archives. |
| First-band variance is too large | Fitted/raw D(1) is 0.929, 0.885, 0.824, 0.735 across panels. Full-window first-band excess fails on all four. Lags and bands vary. No smooth-law refit was justified by this hypothesis. |
| Extreme fifth moments alone drive implausible surge amplitudes | Bounded-frequency phase matching gave half-life/size 39/1.016, 52/0.734, 52/1.413, 52/1.100. On Comments the size remained close to Round 3's 1.436. This alternative used the stationary capped jump transform and frequencies 0.5, 1 and 1.5 divided by each lag's empirical SD. It was not advanced to the gate. |
| A stronger universal inherited rank slope fixes the head | At h=13, observed head/body κ₃ ratios were 0.365, 0.317, 0.140, 0.295. The passing-amplitude law implies 0.459, 0.585, 0.539, 0.375. Borrowing total weekly innovation SD gives 0.284, 0.312, 0.401, 0.094. The second proxy overshoots attenuation on Submissions and still underestimates it on Comments. No slope was selected from head scores. |
| IG asymmetry is entirely a one-week artifact | Its nonoverlapping four-week means still give κ₃ = +0.181 at two blocks' separation. Marginal skew does not determine the sign of gapped D. The small, fixed IG calendar limits timescale inference. |
| A constant posterior-draw bias explains the MAE gap | Mean raw draw error follows the modeled drift. Finite-M jitter and the simulated denominator have distinct measured effects. No score-based centering change. |
| Robust estimation is always more efficient | On clean planted panels at T=30/100, old vs new mean absolute log-scale error was 0.285/0.361 and 0.181/0.225. With one contaminated week it was 0.512/0.384 and 0.379/0.233. A single misplaced burst in a short record has a cost, as does discarding information from clean second moments. |
| The directional cutoff is an exact finite-sample test | False positive-sign flags on symmetric synthetic panels were 1/24, 4/24, 1/24 at 52, 86, 212 weeks. The working SE does not guarantee nominal size. |

## 5. Full before-and-after results

### Frozen Gate v1

Five rolling origins and 20 members per origin, with 2,000 bootstrap draws. Each cell gives mean relative error ± split SD, the percentage of empirical displacement-median intervals containing the model median, and wins against historical mobility. Paired differences reuse origins and seeds. The five overlapping splits limit inference.

| Panel | Model | Historical mobility | Round 3 | Astra | Paired Δ ± SD | Astra wins vs R3 |
|---|---|---|---|---|---|---|
| Facebook | S2 | 0.147 ± 0.030 | 0.105 ± 0.043 · 60% · 4/5 | 0.084 ± 0.037 · 80% · 4/5 | -0.020 ± 0.039 | 3/5 |
| Facebook | S3 | 0.147 ± 0.030 | 0.108 ± 0.023 · 60% · 4/5 | 0.078 ± 0.029 · 80% · 5/5 | -0.030 ± 0.028 | 4/5 |
| Instagram apparatus | S2 | 0.869 ± 0.554 | 0.296 ± 0.216 · 80% · 4/5 | 0.282 ± 0.219 · 80% · 4/5 | -0.014 ± 0.017 | 3/5 |
| Instagram apparatus | S3 | 0.869 ± 0.554 | 0.360 ± 0.276 · 80% · 4/5 | 0.304 ± 0.230 · 80% · 4/5 | -0.056 ± 0.050 | 4/5 |
| Reddit Comments | S2 | 0.158 ± 0.038 | 0.129 ± 0.071 · 80% · 3/5 | 0.134 ± 0.085 · 80% · 3/5 | +0.004 ± 0.017 | 1/5 |
| Reddit Comments | S3 | 0.158 ± 0.038 | 0.098 ± 0.033 · 100% · 5/5 | 0.118 ± 0.064 · 100% · 4/5 | +0.020 ± 0.038 | 2/5 |
| Reddit Submissions | S2 | 0.165 ± 0.044 | 0.346 ± 0.064 · 0% · 0/5 | 0.263 ± 0.067 · 60% · 0/5 | -0.083 ± 0.042 | 5/5 |
| Reddit Submissions | S3 | 0.165 ± 0.044 | 0.310 ± 0.125 · 20% · 0/5 | 0.214 ± 0.078 · 60% · 2/5 | -0.096 ± 0.090 | 4/5 |

<details><summary>All gate moments · arithmetic means of the per-origin statistics</summary>

The eligibility column counts splits in which the frozen moment floor admits the moment. Means here use all available split values. Gate scores above average eligible relative errors within each split.

| Panel | Moment | Eligible | Observed | Historical | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|---|---|---|
| Facebook | dRank1 | 5/5 | 13.600 | 11.600 | 14.400 | 13.000 | 14.400 | 13.200 |
| Facebook | dRank4 | 5/5 | 19.500 | 16.600 | 20.200 | 18.200 | 21.000 | 18.600 |
| Facebook | dRank13 | 5/5 | 29.600 | 24.800 | 29.600 | 27.000 | 33.400 | 29.200 |
| Facebook | RACF1 | 5/5 | 0.381 | 0.509 | 0.351 | 0.382 | 0.359 | 0.390 |
| Facebook | coll1 | 5/5 | 0.520 | 0.481 | 0.634 | 0.575 | 0.494 | 0.472 |
| Facebook | coll5 | 5/5 | 0.910 | 0.911 | 0.912 | 0.899 | 0.887 | 0.880 |
| Facebook | coll20 | 5/5 | 0.950 | 0.947 | 0.972 | 0.966 | 0.969 | 0.965 |
| Instagram apparatus | dRank1 | 5/5 | 15.400 | 15.200 | 17.200 | 17.200 | 17.200 | 17.200 |
| Instagram apparatus | dRank4 | 5/5 | 18.600 | 18.200 | 19.600 | 19.400 | 20.200 | 20.000 |
| Instagram apparatus | RACF1 | 2/5 | 0.019 | 0.134 | 0.040 | 0.043 | 0.047 | 0.048 |
| Instagram apparatus | coll1 | 5/5 | 0.317 | 0.536 | 0.387 | 0.362 | 0.418 | 0.379 |
| Instagram apparatus | coll5 | 5/5 | 0.900 | 0.903 | 0.876 | 0.857 | 0.886 | 0.862 |
| Instagram apparatus | coll20 | 5/5 | 0.967 | 0.953 | 0.975 | 0.974 | 0.976 | 0.971 |
| Reddit Comments | dRank1 | 5/5 | 6.000 | 6.000 | 6.800 | 6.600 | 6.400 | 6.200 |
| Reddit Comments | dRank4 | 5/5 | 8.600 | 8.000 | 8.800 | 8.800 | 8.600 | 8.800 |
| Reddit Comments | dRank13 | 5/5 | 12.200 | 10.600 | 12.600 | 12.800 | 12.800 | 12.600 |
| Reddit Comments | RACF1 | 5/5 | 0.615 | 0.689 | 0.634 | 0.641 | 0.661 | 0.655 |
| Reddit Comments | coll1 | 3/5 | 0.046 | 0.020 | 0.030 | 0.025 | 0.113 | 0.087 |
| Reddit Comments | coll5 | 5/5 | 0.712 | 0.707 | 0.711 | 0.700 | 0.718 | 0.719 |
| Reddit Comments | coll20 | 5/5 | 0.915 | 0.953 | 0.922 | 0.929 | 0.921 | 0.920 |
| Reddit Submissions | dRank1 | 5/5 | 3.600 | 3.400 | 5.200 | 4.200 | 4.600 | 4.200 |
| Reddit Submissions | dRank4 | 5/5 | 4.800 | 5.000 | 7.200 | 6.400 | 6.800 | 5.800 |
| Reddit Submissions | dRank13 | 5/5 | 7.400 | 7.200 | 11.600 | 10.600 | 11.200 | 10.000 |
| Reddit Submissions | RACF1 | 5/5 | 0.679 | 0.774 | 0.723 | 0.781 | 0.733 | 0.774 |
| Reddit Submissions | coll1 | 2/5 | 0.023 | 0.089 | 0.026 | 0.016 | 0.078 | 0.025 |
| Reddit Submissions | coll5 | 5/5 | 0.546 | 0.454 | 0.635 | 0.541 | 0.658 | 0.530 |
| Reddit Submissions | coll20 | 5/5 | 0.831 | 0.832 | 0.903 | 0.880 | 0.897 | 0.873 |

</details>

The historical frozen Model A errors in §2z-dy are 0.103, 0.257, 0.123 and 0.188 in panel order. The corresponding RDM′ values where reported are 0.123 on Facebook, 0.130 on Comments and 0.183 on Submissions. These comparisons reuse recorded runs.

### Structure, endpoint forecasts and time transfer

Structure cells give the median [5th, 95th percentile] of 20 seeded worlds. Observed asymmetry bands use the evaluator’s 200 endpoint-cluster resamples. Each block share uses top K/64 within the recorded top K. The endpoint forecast head is top K/16. Conditional activity scores use the common observed-truth cohort. PIT SD targets 0.289, coverage targets 0.900, and randomized PIT mean targets 0.500. All endpoint rows average the same five origins. Each arrow runs from Round 3 to Astra. Share MAEs are multiplied by 10⁶. A constant zero shift has undefined correlation. The JSON archives contain unrounded values, including every split and origin.

#### Facebook · K = 3,500

<details><summary>All in-sample structure statistics, blocks and deciles</summary>

| Statistic | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|
| Top-10 turnover | 0.350 | 0.350 [0.250, 0.458] | 0.350 [0.250, 0.458] | 0.550 [0.400, 0.650] | 0.350 [0.250, 0.458] |
| K/64 turnover | 0.315 | 0.255 [0.194, 0.306] | 0.255 [0.194, 0.306] | 0.343 [0.314, 0.362] | 0.255 [0.194, 0.306] |
| K/16 turnover | 0.259 | 0.268 [0.247, 0.288] | 0.268 [0.247, 0.288] | 0.295 [0.274, 0.315] | 0.268 [0.247, 0.288] |
| K/4 turnover | 0.237 | 0.212 [0.201, 0.221] | 0.212 [0.201, 0.221] | 0.223 [0.214, 0.237] | 0.212 [0.201, 0.221] |
| K/2 turnover | 0.205 | 0.194 [0.187, 0.198] | 0.194 [0.187, 0.198] | 0.198 [0.193, 0.205] | 0.194 [0.187, 0.198] |
| Weekly occupant change at rank 1 | 0.518 | 0.459 [0.301, 0.616] | 0.459 [0.301, 0.616] | 0.435 [0.244, 0.730] | 0.459 [0.301, 0.616] |
| Weekly occupant change at rank 10 | 0.941 | 0.941 [0.917, 0.977] | 0.941 [0.917, 0.977] | 0.953 [0.882, 0.989] | 0.941 [0.917, 0.977] |
| Maximum log activity | 16.855 | 17.274 [17.151, 18.100] | 17.274 [17.151, 18.100] | 19.375 [18.260, 20.708] | 17.274 [17.151, 18.100] |
| Boundary ratio | 0.980 | 0.998 [0.976, 1.024] | 0.998 [0.976, 1.024] | 1.137 [1.077, 1.179] | 0.998 [0.976, 1.024] |
| Departure rate | 0.033 | 0.044 [0.042, 0.046] | 0.044 [0.042, 0.046] | 0.044 [0.042, 0.046] | 0.044 [0.042, 0.046] |
| Immediate crossing / departures | 0.957 | 0.954 [0.945, 0.958] | 0.954 [0.945, 0.958] | 0.944 [0.936, 0.949] | 0.954 [0.945, 0.958] |
| Absent then returns / departures | 0.037 | 0.045 [0.041, 0.053] | 0.045 [0.041, 0.053] | 0.054 [0.048, 0.062] | 0.045 [0.041, 0.053] |
| Absent beyond window / departures | 0.005 | 0.001 [0.000, 0.003] | 0.001 [0.000, 0.003] | 0.002 [0.001, 0.003] | 0.001 [0.000, 0.003] |
| Gapped persistence ρ | -0.095 [-0.112, -0.080] | -0.068 [-0.084, -0.057] | -0.068 [-0.084, -0.057] | -0.067 [-0.079, -0.054] | -0.068 [-0.084, -0.057] |
| Asymmetry D | 0.031 [-0.013, 0.079] | -0.027 [-0.045, -0.003] | -0.027 [-0.045, -0.003] | 0.044 [0.003, 0.072] | -0.027 [-0.045, -0.003] |
| Faller slope | -0.111 [-0.140, -0.080] | -0.092 [-0.105, -0.064] | -0.092 [-0.105, -0.064] | -0.053 [-0.087, -0.040] | -0.092 [-0.105, -0.064] |
| Riser slope | -0.142 [-0.170, -0.111] | -0.062 [-0.087, -0.046] | -0.062 [-0.087, -0.046] | -0.104 [-0.124, -0.077] | -0.062 [-0.087, -0.046] |
| Mixed moment m21 | -0.107 [-0.183, -0.032] | 0.023 [-0.008, 0.041] | 0.023 [-0.008, 0.041] | -0.163 [-0.211, -0.105] | 0.023 [-0.008, 0.041] |
| Mixed moment m12 | 0.098 [0.027, 0.162] | -0.017 [-0.036, 0.007] | -0.017 [-0.036, 0.007] | 0.015 [-0.016, 0.056] | -0.017 [-0.036, 0.007] |
| Leverage | 0.938 [0.878, 1.007] | 1.020 [0.970, 1.063] | 1.020 [0.970, 1.063] | 1.016 [0.971, 1.102] | 1.020 [0.970, 1.063] |
| Two-block decline/gain run ratio | 1.011 | 1.001 [0.967, 1.038] | 1.001 [0.967, 1.038] | 1.075 [1.047, 1.104] | 1.001 [0.967, 1.038] |
| Three-block decline/gain run ratio | 1.029 | 0.994 [0.932, 1.085] | 0.994 [0.932, 1.085] | 1.123 [1.066, 1.207] | 0.994 [0.932, 1.085] |
| Head share, block 1 | 0.239 | 0.241 [0.228, 0.253] | 0.241 [0.228, 0.253] | 0.280 [0.231, 0.358] | 0.241 [0.228, 0.253] |
| Head share, block 2 | 0.222 | 0.239 [0.215, 0.272] | 0.239 [0.215, 0.272] | 0.287 [0.242, 0.366] | 0.239 [0.215, 0.272] |
| Head share, block 3 | 0.229 | 0.242 [0.222, 0.276] | 0.242 [0.222, 0.276] | 0.290 [0.239, 0.350] | 0.242 [0.222, 0.276] |
| Log ladder span, rank 1, block 1 | 5.995 | 6.107 [5.857, 6.458] | 6.107 [5.857, 6.458] | 6.328 [5.644, 8.179] | 6.107 [5.857, 6.458] |
| Log ladder span, rank 1, block 2 | 5.733 | 6.124 [5.731, 6.484] | 6.124 [5.731, 6.484] | 6.478 [5.842, 7.703] | 6.124 [5.731, 6.484] |
| Log ladder span, rank 1, block 3 | 5.762 | 6.123 [5.705, 7.018] | 6.123 [5.705, 7.018] | 6.617 [5.743, 7.237] | 6.123 [5.705, 7.018] |
| Log ladder span, rank 54, block 1 | 4.036 | 3.970 [3.890, 4.059] | 3.970 [3.890, 4.059] | 4.069 [3.914, 4.135] | 3.970 [3.890, 4.059] |
| Log ladder span, rank 54, block 2 | 3.884 | 3.970 [3.846, 4.077] | 3.970 [3.846, 4.077] | 4.055 [3.978, 4.121] | 3.970 [3.846, 4.077] |
| Log ladder span, rank 54, block 3 | 4.003 | 3.960 [3.848, 4.083] | 3.960 [3.848, 4.083] | 4.080 [3.911, 4.130] | 3.960 [3.848, 4.083] |
| Log ladder span, rank 218, block 1 | 2.916 | 2.921 [2.887, 2.969] | 2.921 [2.887, 2.969] | 2.916 [2.886, 2.953] | 2.921 [2.887, 2.969] |
| Log ladder span, rank 218, block 2 | 2.912 | 2.941 [2.874, 2.997] | 2.941 [2.874, 2.997] | 2.914 [2.872, 2.984] | 2.941 [2.874, 2.997] |
| Log ladder span, rank 218, block 3 | 2.979 | 2.940 [2.851, 3.011] | 2.940 [2.851, 3.011] | 2.959 [2.903, 3.005] | 2.940 [2.851, 3.011] |
| Log ladder span, rank 875, block 1 | 1.683 | 1.690 [1.668, 1.736] | 1.690 [1.668, 1.736] | 1.701 [1.667, 1.719] | 1.690 [1.668, 1.736] |
| Log ladder span, rank 875, block 2 | 1.663 | 1.705 [1.673, 1.727] | 1.705 [1.673, 1.727] | 1.712 [1.659, 1.743] | 1.705 [1.673, 1.727] |
| Log ladder span, rank 875, block 3 | 1.671 | 1.709 [1.635, 1.740] | 1.709 [1.635, 1.740] | 1.708 [1.665, 1.743] | 1.709 [1.635, 1.740] |
| E[y given decile 1] | 0.164 | 0.135 [0.094, 0.153] | 0.135 [0.094, 0.153] | 0.078 [0.058, 0.127] | 0.135 [0.094, 0.153] |
| E[y given decile 2] | 0.058 | 0.064 [0.044, 0.101] | 0.064 [0.044, 0.101] | 0.052 [0.016, 0.093] | 0.064 [0.044, 0.101] |
| E[y given decile 3] | 0.072 | 0.031 [0.015, 0.047] | 0.031 [0.015, 0.047] | 0.032 [0.006, 0.061] | 0.031 [0.015, 0.047] |
| E[y given decile 4] | 0.059 | 0.012 [-0.003, 0.047] | 0.012 [-0.003, 0.047] | 0.020 [0.004, 0.058] | 0.012 [-0.003, 0.047] |
| E[y given decile 5] | 0.003 | -0.005 [-0.027, 0.012] | -0.005 [-0.027, 0.012] | 0.019 [-0.015, 0.042] | -0.005 [-0.027, 0.012] |
| E[y given decile 6] | 0.015 | -0.018 [-0.031, -0.003] | -0.018 [-0.031, -0.003] | 0.003 [-0.013, 0.020] | -0.018 [-0.031, -0.003] |
| E[y given decile 7] | -0.033 | -0.030 [-0.054, -0.011] | -0.030 [-0.054, -0.011] | 0.002 [-0.034, 0.026] | -0.030 [-0.054, -0.011] |
| E[y given decile 8] | -0.020 | -0.038 [-0.065, -0.011] | -0.038 [-0.065, -0.011] | -0.025 [-0.043, 0.028] | -0.038 [-0.065, -0.011] |
| E[y given decile 9] | -0.068 | -0.058 [-0.091, -0.028] | -0.058 [-0.091, -0.028] | -0.033 [-0.056, -0.007] | -0.058 [-0.091, -0.028] |
| E[y given decile 10] | -0.251 | -0.092 [-0.131, -0.069] | -0.092 [-0.131, -0.069] | -0.172 [-0.210, -0.121] | -0.092 [-0.131, -0.069] |

</details>

| Week | Model | No change MAE | EWMA MAE | Kalman X MAE | Median X MAE | Fair X CRPS |
|---|---|---|---|---|---|---|
| 1 | S2 | 0.4490 | 0.4078 | 0.4027 → 0.4026 | 0.4116 → 0.4091 | 0.2990 → 0.2987 |
| 1 | S3 | 0.4490 | 0.4078 | 0.4024 → 0.4025 | 0.4162 → 0.4117 | 0.3013 → 0.2999 |
| 4 | S2 | 0.5379 | 0.4752 | 0.4661 → 0.4659 | 0.4786 → 0.4766 | 0.3429 → 0.3430 |
| 4 | S3 | 0.5379 | 0.4752 | 0.4663 → 0.4661 | 0.4793 → 0.4762 | 0.3439 → 0.3432 |
| 13 | S2 | 0.6724 | 0.6026 | 0.5783 → 0.5804 | 0.5955 → 0.5930 | 0.4306 → 0.4301 |
| 13 | S3 | 0.6724 | 0.6026 | 0.5783 → 0.5803 | 0.5974 → 0.5935 | 0.4311 → 0.4300 |
| 26 | S2 | 0.7773 | 0.7214 | 0.6934 → 0.6968 | 0.7147 → 0.7113 | 0.5128 → 0.5127 |
| 26 | S3 | 0.7773 | 0.7214 | 0.6924 → 0.6951 | 0.7198 → 0.7125 | 0.5144 → 0.5121 |

| Week | Model | 90% coverage | PIT mean | PIT SD | PIT KS |
|---|---|---|---|---|---|
| 1 | S2 | 0.9017 → 0.8834 | 0.5144 → 0.4975 | 0.2763 → 0.2871 | 0.0479 → 0.0248 |
| 1 | S3 | 0.9173 → 0.8948 | 0.5256 → 0.5069 | 0.2664 → 0.2811 | 0.0743 → 0.0381 |
| 4 | S2 | 0.9117 → 0.8937 | 0.5132 → 0.4968 | 0.2702 → 0.2801 | 0.0520 → 0.0376 |
| 4 | S3 | 0.9203 → 0.9009 | 0.5231 → 0.5048 | 0.2666 → 0.2779 | 0.0685 → 0.0383 |
| 13 | S2 | 0.9087 → 0.8956 | 0.5188 → 0.5045 | 0.2669 → 0.2744 | 0.0682 → 0.0457 |
| 13 | S3 | 0.9120 → 0.8995 | 0.5302 → 0.5130 | 0.2678 → 0.2754 | 0.0818 → 0.0549 |
| 26 | S2 | 0.9058 → 0.8948 | 0.5266 → 0.5142 | 0.2703 → 0.2761 | 0.0755 → 0.0580 |
| 26 | S3 | 0.9083 → 0.8962 | 0.5390 → 0.5238 | 0.2719 → 0.2776 | 0.0893 → 0.0696 |

| Week | Model | No change logR | Rank-bin logR | Fair logR CRPS | No change Brier | Rank-bin Brier | Fair head Brier |
|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.2966 | 0.2315 | 0.1965 → 0.1969 | 0.1167 | 0.0847 | 0.0721 → 0.0713 |
| 1 | S3 | 0.2966 | 0.2315 | 0.1974 → 0.1970 | 0.1167 | 0.0847 | 0.0718 → 0.0712 |
| 4 | S2 | 0.3589 | 0.2707 | 0.2302 → 0.2308 | 0.1444 | 0.1008 | 0.0864 → 0.0871 |
| 4 | S3 | 0.3589 | 0.2707 | 0.2311 → 0.2308 | 0.1444 | 0.1008 | 0.0882 → 0.0876 |
| 13 | S2 | 0.4360 | 0.3162 | 0.2707 → 0.2711 | 0.1737 | 0.1149 | 0.0972 → 0.0969 |
| 13 | S3 | 0.4360 | 0.3162 | 0.2723 → 0.2715 | 0.1737 | 0.1149 | 0.0992 → 0.0981 |
| 26 | S2 | 0.4857 | 0.3425 | 0.3062 → 0.3069 | 0.1836 | 0.1165 | 0.1058 → 0.1060 |
| 26 | S3 | 0.4857 | 0.3425 | 0.3089 → 0.3073 | 0.1836 | 0.1165 | 0.1065 → 0.1052 |

| Week | Model | Zero shift MAE | EWMA MAE | Head shift MAE | EWMA corr | Head shift corr |
|---|---|---|---|---|---|---|
| 1 | S2 | 707.25 | 609.54 | 580.44 → 570.09 | 0.554 | 0.576 → 0.592 |
| 1 | S3 | 707.25 | 609.54 | 592.11 → 580.62 | 0.554 | 0.558 → 0.580 |
| 4 | S2 | 858.52 | 742.39 | 660.14 → 652.74 | 0.555 | 0.621 → 0.631 |
| 4 | S3 | 858.52 | 742.39 | 684.16 → 655.30 | 0.555 | 0.610 → 0.611 |
| 13 | S2 | 1093.16 | 969.40 | 801.78 → 797.78 | 0.481 | 0.632 → 0.630 |
| 13 | S3 | 1093.16 | 969.40 | 861.13 → 821.94 | 0.481 | 0.604 → 0.616 |
| 26 | S2 | 1140.62 | 1043.71 | 878.18 → 895.89 | 0.438 | 0.584 → 0.545 |
| 26 | S3 | 1140.62 | 1043.71 | 964.67 → 937.92 | 0.438 | 0.539 → 0.542 |

<details><summary>Paired forecast changes across five origins · mean Δ ± split SD</summary>

| Week | Model | X CRPS | Kalman MAE | Coverage | logR CRPS | Head Brier | Head shift MAE × 10⁶ |
|---|---|---|---|---|---|---|---|
| 1 | S2 | -0.0003 ± 0.0011 | -0.0002 ± 0.0008 | -0.0182 ± 0.0051 | +0.0004 ± 0.0008 | -0.0008 ± 0.0011 | -10.3508 ± 9.9646 |
| 1 | S3 | -0.0014 ± 0.0012 | +0.0001 ± 0.0009 | -0.0225 ± 0.0135 | -0.0004 ± 0.0010 | -0.0006 ± 0.0018 | -11.4989 ± 13.5805 |
| 4 | S2 | +0.0000 ± 0.0021 | -0.0002 ± 0.0008 | -0.0180 ± 0.0056 | +0.0006 ± 0.0012 | +0.0007 ± 0.0026 | -7.4027 ± 8.0032 |
| 4 | S3 | -0.0007 ± 0.0024 | -0.0003 ± 0.0008 | -0.0194 ± 0.0096 | -0.0003 ± 0.0017 | -0.0006 ± 0.0038 | -28.8574 ± 16.3332 |
| 13 | S2 | -0.0005 ± 0.0013 | +0.0021 ± 0.0016 | -0.0132 ± 0.0033 | +0.0004 ± 0.0005 | -0.0004 ± 0.0015 | -4.0016 ± 20.8065 |
| 13 | S3 | -0.0011 ± 0.0029 | +0.0020 ± 0.0017 | -0.0125 ± 0.0024 | -0.0008 ± 0.0014 | -0.0010 ± 0.0017 | -39.1870 ± 43.0051 |
| 26 | S2 | -0.0001 ± 0.0022 | +0.0034 ± 0.0024 | -0.0110 ± 0.0040 | +0.0006 ± 0.0015 | +0.0002 ± 0.0015 | +17.7093 ± 14.3648 |
| 26 | S3 | -0.0024 ± 0.0014 | +0.0027 ± 0.0022 | -0.0121 ± 0.0080 | -0.0017 ± 0.0016 | -0.0013 ± 0.0007 | -26.7499 ± 26.0247 |

</details>

#### Instagram apparatus · K = 10,000

<details><summary>All in-sample structure statistics, blocks and deciles</summary>

| Statistic | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|
| Top-10 turnover | 0.267 | 0.167 [0.065, 0.202] | 0.167 [0.065, 0.202] | 0.333 [0.232, 0.433] | 0.167 [0.065, 0.202] |
| K/64 turnover | 0.263 | 0.193 [0.182, 0.223] | 0.193 [0.182, 0.223] | 0.250 [0.231, 0.267] | 0.193 [0.182, 0.223] |
| K/16 turnover | 0.313 | 0.208 [0.197, 0.219] | 0.208 [0.197, 0.219] | 0.264 [0.252, 0.276] | 0.208 [0.197, 0.219] |
| K/4 turnover | 0.295 | 0.223 [0.219, 0.227] | 0.223 [0.219, 0.227] | 0.259 [0.254, 0.263] | 0.223 [0.219, 0.227] |
| K/2 turnover | 0.287 | 0.229 [0.226, 0.233] | 0.229 [0.226, 0.233] | 0.240 [0.238, 0.245] | 0.229 [0.226, 0.233] |
| Weekly occupant change at rank 1 | 0.392 | 0.814 [0.643, 0.903] | 0.814 [0.643, 0.903] | 0.755 [0.385, 0.962] | 0.814 [0.643, 0.903] |
| Weekly occupant change at rank 10 | 0.980 | 0.961 [0.901, 1.000] | 0.961 [0.901, 1.000] | 0.941 [0.921, 1.000] | 0.961 [0.901, 1.000] |
| Maximum log activity | 17.628 | 18.971 [18.247, 19.988] | 18.971 [18.247, 19.988] | 18.935 [18.499, 20.134] | 18.971 [18.247, 19.988] |
| Boundary ratio | 1.000 | 0.978 [0.960, 0.996] | 0.978 [0.960, 0.996] | 1.157 [1.137, 1.174] | 0.978 [0.960, 0.996] |
| Departure rate | 0.138 | 0.112 [0.110, 0.114] | 0.112 [0.110, 0.114] | 0.121 [0.119, 0.122] | 0.112 [0.110, 0.114] |
| Immediate crossing / departures | 0.762 | 0.748 [0.744, 0.754] | 0.748 [0.744, 0.754] | 0.729 [0.725, 0.732] | 0.748 [0.744, 0.754] |
| Absent then returns / departures | 0.218 | 0.242 [0.237, 0.246] | 0.242 [0.237, 0.246] | 0.260 [0.257, 0.264] | 0.242 [0.237, 0.246] |
| Absent beyond window / departures | 0.020 | 0.010 [0.009, 0.012] | 0.010 [0.009, 0.012] | 0.010 [0.009, 0.012] | 0.010 [0.009, 0.012] |
| Gapped persistence ρ | -0.021 [-0.034, -0.010] | -0.103 [-0.111, -0.092] | -0.103 [-0.111, -0.092] | -0.081 [-0.090, -0.071] | -0.103 [-0.111, -0.092] |
| Asymmetry D | -0.087 [-0.122, -0.050] | 0.005 [-0.014, 0.028] | 0.005 [-0.014, 0.028] | 0.082 [0.058, 0.106] | 0.005 [-0.014, 0.028] |
| Faller slope | -0.081 [-0.108, -0.051] | -0.110 [-0.131, -0.084] | -0.110 [-0.131, -0.084] | -0.041 [-0.064, -0.026] | -0.110 [-0.131, -0.084] |
| Riser slope | 0.006 [-0.023, 0.030] | -0.114 [-0.131, -0.094] | -0.114 [-0.131, -0.094] | -0.128 [-0.138, -0.114] | -0.114 [-0.131, -0.094] |
| Mixed moment m21 | 0.038 [-0.013, 0.084] | -0.017 [-0.044, 0.005] | -0.017 [-0.044, 0.005] | -0.162 [-0.191, -0.124] | -0.017 [-0.044, 0.005] |
| Mixed moment m12 | 0.088 [0.044, 0.132] | 0.027 [-0.002, 0.059] | 0.027 [-0.002, 0.059] | 0.030 [-0.001, 0.051] | 0.027 [-0.002, 0.059] |
| Leverage | 0.938 [0.888, 0.998] | 0.975 [0.935, 1.021] | 0.975 [0.935, 1.021] | 1.026 [0.989, 1.055] | 0.975 [0.935, 1.021] |
| Two-block decline/gain run ratio | 1.214 | 0.995 [0.982, 1.020] | 0.995 [0.982, 1.020] | 1.201 [1.181, 1.232] | 0.995 [0.982, 1.020] |
| Three-block decline/gain run ratio | 1.353 | 0.981 [0.947, 1.034] | 0.981 [0.947, 1.034] | 1.360 [1.299, 1.429] | 0.981 [0.947, 1.034] |
| Head share, block 1 | 0.437 | 0.396 [0.370, 0.415] | 0.396 [0.370, 0.415] | 0.399 [0.377, 0.440] | 0.396 [0.370, 0.415] |
| Head share, block 2 | 0.432 | 0.407 [0.369, 0.448] | 0.407 [0.369, 0.448] | 0.403 [0.365, 0.462] | 0.407 [0.369, 0.448] |
| Head share, block 3 | 0.424 | 0.383 [0.371, 0.422] | 0.383 [0.371, 0.422] | 0.399 [0.376, 0.446] | 0.383 [0.371, 0.422] |
| Head share, block 4 | 0.419 | 0.392 [0.371, 0.412] | 0.392 [0.371, 0.412] | 0.396 [0.374, 0.441] | 0.392 [0.371, 0.412] |
| Log ladder span, rank 1, block 1 | 7.824 | 7.721 [7.382, 8.007] | 7.721 [7.382, 8.007] | 7.621 [7.361, 8.543] | 7.721 [7.382, 8.007] |
| Log ladder span, rank 1, block 2 | 8.088 | 7.576 [7.407, 8.338] | 7.576 [7.407, 8.338] | 7.689 [7.324, 8.824] | 7.576 [7.407, 8.338] |
| Log ladder span, rank 1, block 3 | 8.331 | 7.608 [7.303, 8.078] | 7.608 [7.303, 8.078] | 7.819 [7.222, 8.691] | 7.608 [7.303, 8.078] |
| Log ladder span, rank 1, block 4 | 8.127 | 7.542 [7.329, 7.905] | 7.542 [7.329, 7.905] | 8.027 [7.372, 9.257] | 7.542 [7.329, 7.905] |
| Log ladder span, rank 156, block 1 | 4.227 | 4.136 [4.091, 4.189] | 4.136 [4.091, 4.189] | 4.112 [4.047, 4.172] | 4.136 [4.091, 4.189] |
| Log ladder span, rank 156, block 2 | 4.278 | 4.127 [4.071, 4.190] | 4.127 [4.071, 4.190] | 4.122 [4.067, 4.190] | 4.127 [4.071, 4.190] |
| Log ladder span, rank 156, block 3 | 4.416 | 4.104 [4.067, 4.178] | 4.104 [4.067, 4.178] | 4.107 [4.037, 4.184] | 4.104 [4.067, 4.178] |
| Log ladder span, rank 156, block 4 | 4.518 | 4.106 [4.029, 4.184] | 4.106 [4.029, 4.184] | 4.094 [4.042, 4.183] | 4.106 [4.029, 4.184] |
| Log ladder span, rank 625, block 1 | 2.903 | 2.827 [2.788, 2.857] | 2.827 [2.788, 2.857] | 2.855 [2.807, 2.910] | 2.827 [2.788, 2.857] |
| Log ladder span, rank 625, block 2 | 2.923 | 2.829 [2.801, 2.875] | 2.829 [2.801, 2.875] | 2.850 [2.805, 2.888] | 2.829 [2.801, 2.875] |
| Log ladder span, rank 625, block 3 | 3.035 | 2.812 [2.785, 2.844] | 2.812 [2.785, 2.844] | 2.850 [2.813, 2.892] | 2.812 [2.785, 2.844] |
| Log ladder span, rank 625, block 4 | 3.191 | 2.794 [2.749, 2.859] | 2.794 [2.749, 2.859] | 2.842 [2.818, 2.895] | 2.794 [2.749, 2.859] |
| Log ladder span, rank 2500, block 1 | 1.587 | 1.507 [1.495, 1.522] | 1.507 [1.495, 1.522] | 1.519 [1.502, 1.533] | 1.507 [1.495, 1.522] |
| Log ladder span, rank 2500, block 2 | 1.582 | 1.496 [1.477, 1.510] | 1.496 [1.477, 1.510] | 1.519 [1.492, 1.536] | 1.496 [1.477, 1.510] |
| Log ladder span, rank 2500, block 3 | 1.641 | 1.475 [1.455, 1.494] | 1.475 [1.455, 1.494] | 1.520 [1.502, 1.539] | 1.475 [1.455, 1.494] |
| Log ladder span, rank 2500, block 4 | 1.733 | 1.458 [1.440, 1.477] | 1.458 [1.440, 1.477] | 1.512 [1.497, 1.523] | 1.458 [1.440, 1.477] |
| E[y given decile 1] | 0.115 | 0.170 [0.132, 0.205] | 0.170 [0.132, 0.205] | 0.064 [0.042, 0.103] | 0.170 [0.132, 0.205] |
| E[y given decile 2] | 0.017 | 0.088 [0.070, 0.116] | 0.088 [0.070, 0.116] | 0.058 [0.034, 0.077] | 0.088 [0.070, 0.116] |
| E[y given decile 3] | 0.012 | 0.067 [0.031, 0.081] | 0.067 [0.031, 0.081] | 0.048 [0.031, 0.071] | 0.067 [0.031, 0.081] |
| E[y given decile 4] | -0.035 | 0.033 [0.015, 0.050] | 0.033 [0.015, 0.050] | 0.049 [0.032, 0.075] | 0.033 [0.015, 0.050] |
| E[y given decile 5] | -0.027 | 0.006 [-0.025, 0.023] | 0.006 [-0.025, 0.023] | 0.041 [0.032, 0.069] | 0.006 [-0.025, 0.023] |
| E[y given decile 6] | -0.032 | -0.003 [-0.025, 0.012] | -0.003 [-0.025, 0.012] | 0.033 [0.018, 0.055] | -0.003 [-0.025, 0.012] |
| E[y given decile 7] | -0.036 | -0.032 [-0.045, -0.001] | -0.032 [-0.045, -0.001] | 0.011 [-0.010, 0.032] | -0.032 [-0.045, -0.001] |
| E[y given decile 8] | -0.039 | -0.051 [-0.075, -0.036] | -0.051 [-0.075, -0.036] | -0.015 [-0.040, 0.006] | -0.051 [-0.075, -0.036] |
| E[y given decile 9] | 0.014 | -0.090 [-0.110, -0.059] | -0.090 [-0.110, -0.059] | -0.061 [-0.077, -0.038] | -0.090 [-0.110, -0.059] |
| E[y given decile 10] | 0.013 | -0.187 [-0.213, -0.152] | -0.187 [-0.213, -0.152] | -0.247 [-0.271, -0.224] | -0.187 [-0.213, -0.152] |

</details>

| Week | Model | No change MAE | EWMA MAE | Kalman X MAE | Median X MAE | Fair X CRPS |
|---|---|---|---|---|---|---|
| 1 | S2 | 0.7903 | 0.6378 | 0.6370 → 0.6366 | 0.6618 → 0.6581 | 0.4701 → 0.4717 |
| 1 | S3 | 0.7903 | 0.6378 | 0.6366 → 0.6363 | 0.6623 → 0.6581 | 0.4708 → 0.4719 |
| 4 | S2 | 0.8545 | 0.6940 | 0.6905 → 0.6902 | 0.7149 → 0.7117 | 0.5099 → 0.5113 |
| 4 | S3 | 0.8545 | 0.6940 | 0.6900 → 0.6898 | 0.7173 → 0.7134 | 0.5114 → 0.5120 |
| 13 | S2 | 0.9640 | 0.8020 | 0.7922 → 0.7924 | 0.8199 → 0.8164 | 0.5856 → 0.5877 |
| 13 | S3 | 0.9640 | 0.8020 | 0.7919 → 0.7923 | 0.8195 → 0.8172 | 0.5862 → 0.5876 |

| Week | Model | 90% coverage | PIT mean | PIT SD | PIT KS |
|---|---|---|---|---|---|
| 1 | S2 | 0.8955 → 0.8896 | 0.5506 → 0.5461 | 0.2830 → 0.2861 | 0.0908 → 0.0826 |
| 1 | S3 | 0.8997 → 0.8931 | 0.5526 → 0.5472 | 0.2797 → 0.2837 | 0.0956 → 0.0861 |
| 4 | S2 | 0.8932 → 0.8872 | 0.5464 → 0.5420 | 0.2820 → 0.2856 | 0.0840 → 0.0756 |
| 4 | S3 | 0.8968 → 0.8907 | 0.5497 → 0.5447 | 0.2804 → 0.2842 | 0.0918 → 0.0823 |
| 13 | S2 | 0.8957 → 0.8863 | 0.5407 → 0.5377 | 0.2811 → 0.2846 | 0.0764 → 0.0701 |
| 13 | S3 | 0.8966 → 0.8882 | 0.5448 → 0.5411 | 0.2807 → 0.2844 | 0.0859 → 0.0774 |

| Week | Model | No change logR | Rank-bin logR | Fair logR CRPS | No change Brier | Rank-bin Brier | Fair head Brier |
|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.4348 | 0.3112 | 0.2576 → 0.2591 | 0.1603 | 0.1056 | 0.0831 → 0.0839 |
| 1 | S3 | 0.4348 | 0.3112 | 0.2576 → 0.2590 | 0.1603 | 0.1056 | 0.0831 → 0.0839 |
| 4 | S2 | 0.4703 | 0.3294 | 0.2750 → 0.2764 | 0.1745 | 0.1105 | 0.0890 → 0.0900 |
| 4 | S3 | 0.4703 | 0.3294 | 0.2753 → 0.2764 | 0.1745 | 0.1105 | 0.0887 → 0.0890 |
| 13 | S2 | 0.5098 | 0.3469 | 0.2917 → 0.2938 | 0.1847 | 0.1123 | 0.0916 → 0.0926 |
| 13 | S3 | 0.5098 | 0.3469 | 0.2911 → 0.2927 | 0.1847 | 0.1123 | 0.0909 → 0.0918 |

| Week | Model | Zero shift MAE | EWMA MAE | Head shift MAE | EWMA corr | Head shift corr |
|---|---|---|---|---|---|---|
| 1 | S2 | 470.53 | 407.90 | 351.79 → 353.56 | 0.495 | 0.566 → 0.555 |
| 1 | S3 | 470.53 | 407.90 | 354.14 → 351.15 | 0.495 | 0.571 → 0.581 |
| 4 | S2 | 514.71 | 446.04 | 369.92 → 371.05 | 0.426 | 0.627 → 0.631 |
| 4 | S3 | 514.71 | 446.04 | 371.36 → 368.35 | 0.426 | 0.620 → 0.624 |
| 13 | S2 | 585.21 | 520.38 | 423.57 → 427.21 | 0.249 | 0.582 → 0.577 |
| 13 | S3 | 585.21 | 520.38 | 408.72 → 410.36 | 0.249 | 0.641 → 0.633 |

<details><summary>Paired forecast changes across five origins · mean Δ ± split SD</summary>

| Week | Model | X CRPS | Kalman MAE | Coverage | logR CRPS | Head Brier | Head shift MAE × 10⁶ |
|---|---|---|---|---|---|---|---|
| 1 | S2 | +0.0016 ± 0.0005 | -0.0004 ± 0.0003 | -0.0059 ± 0.0027 | +0.0016 ± 0.0003 | +0.0008 ± 0.0007 | +1.7692 ± 1.8782 |
| 1 | S3 | +0.0011 ± 0.0002 | -0.0003 ± 0.0002 | -0.0066 ± 0.0022 | +0.0014 ± 0.0003 | +0.0008 ± 0.0004 | -2.9906 ± 5.0201 |
| 4 | S2 | +0.0014 ± 0.0008 | -0.0003 ± 0.0008 | -0.0060 ± 0.0012 | +0.0014 ± 0.0004 | +0.0009 ± 0.0005 | +1.1259 ± 4.7252 |
| 4 | S3 | +0.0006 ± 0.0007 | -0.0002 ± 0.0008 | -0.0062 ± 0.0019 | +0.0011 ± 0.0005 | +0.0003 ± 0.0004 | -3.0114 ± 4.7925 |
| 13 | S2 | +0.0021 ± 0.0013 | +0.0002 ± 0.0004 | -0.0094 ± 0.0021 | +0.0021 ± 0.0006 | +0.0009 ± 0.0005 | +3.6439 ± 10.4496 |
| 13 | S3 | +0.0015 ± 0.0009 | +0.0004 ± 0.0004 | -0.0083 ± 0.0024 | +0.0016 ± 0.0004 | +0.0009 ± 0.0005 | +1.6432 ± 9.9681 |

</details>

#### Reddit Comments · K = 12,500

<details><summary>All in-sample structure statistics, blocks and deciles</summary>

| Statistic | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|
| Top-10 turnover | 0.267 | 0.267 [0.167, 0.372] | 0.267 [0.167, 0.372] | 0.450 [0.332, 0.570] | 0.250 [0.200, 0.372] |
| K/64 turnover | 0.209 | 0.174 [0.144, 0.190] | 0.174 [0.144, 0.190] | 0.247 [0.232, 0.274] | 0.180 [0.160, 0.207] |
| K/16 turnover | 0.187 | 0.151 [0.145, 0.156] | 0.151 [0.145, 0.156] | 0.194 [0.184, 0.205] | 0.161 [0.154, 0.172] |
| K/4 turnover | 0.162 | 0.122 [0.120, 0.128] | 0.122 [0.120, 0.128] | 0.147 [0.144, 0.151] | 0.124 [0.121, 0.128] |
| K/2 turnover | 0.154 | 0.133 [0.132, 0.138] | 0.133 [0.132, 0.138] | 0.140 [0.136, 0.143] | 0.137 [0.134, 0.140] |
| Weekly occupant change at rank 1 | 0.028 | 0.000 [0.000, 0.101] | 0.000 [0.000, 0.101] | 0.200 [0.084, 0.293] | 0.021 [0.000, 0.118] |
| Weekly occupant change at rank 10 | 0.877 | 0.875 [0.830, 0.906] | 0.875 [0.830, 0.906] | 0.877 [0.816, 0.911] | 0.861 [0.829, 0.906] |
| Maximum log activity | 17.919 | 17.898 [17.422, 18.663] | 17.898 [17.422, 18.663] | 18.900 [17.963, 19.822] | 18.207 [17.399, 19.738] |
| Boundary ratio | 1.009 | 1.001 [0.990, 1.006] | 1.001 [0.990, 1.006] | 1.131 [1.113, 1.141] | 0.990 [0.983, 0.996] |
| Departure rate | 0.016 | 0.018 [0.017, 0.018] | 0.018 [0.017, 0.018] | 0.014 [0.013, 0.015] | 0.017 [0.017, 0.018] |
| Immediate crossing / departures | 0.989 | 0.927 [0.921, 0.931] | 0.927 [0.921, 0.931] | 0.892 [0.886, 0.901] | 0.917 [0.912, 0.921] |
| Absent then returns / departures | 0.006 | 0.072 [0.068, 0.078] | 0.072 [0.068, 0.078] | 0.107 [0.098, 0.113] | 0.082 [0.078, 0.087] |
| Absent beyond window / departures | 0.005 | 0.001 [0.001, 0.001] | 0.001 [0.001, 0.001] | 0.001 [0.001, 0.002] | 0.001 [0.001, 0.002] |
| Gapped persistence ρ | 0.145 [0.135, 0.156] | -0.049 [-0.056, -0.039] | -0.049 [-0.056, -0.039] | 0.104 [0.094, 0.109] | 0.081 [0.073, 0.087] |
| Asymmetry D | 0.132 [0.109, 0.155] | -0.039 [-0.053, -0.021] | -0.039 [-0.053, -0.021] | 0.149 [0.137, 0.170] | -0.050 [-0.059, -0.040] |
| Faller slope | 0.158 [0.138, 0.178] | -0.072 [-0.084, -0.052] | -0.072 [-0.084, -0.052] | 0.135 [0.115, 0.144] | 0.041 [0.030, 0.052] |
| Riser slope | 0.026 [0.013, 0.038] | -0.034 [-0.047, -0.024] | -0.034 [-0.047, -0.024] | -0.016 [-0.034, -0.007] | 0.091 [0.082, 0.100] |
| Mixed moment m21 | -0.103 [-0.141, -0.066] | 0.026 [0.014, 0.043] | 0.026 [0.014, 0.043] | -0.322 [-0.356, -0.294] | 0.047 [0.040, 0.056] |
| Mixed moment m12 | -0.022 [-0.055, 0.020] | -0.014 [-0.026, 0.001] | -0.014 [-0.026, 0.001] | 0.097 [0.082, 0.111] | -0.022 [-0.033, -0.012] |
| Leverage | 1.112 [1.060, 1.158] | 1.022 [0.991, 1.050] | 1.022 [0.991, 1.050] | 0.893 [0.862, 0.907] | 1.044 [1.025, 1.068] |
| Two-block decline/gain run ratio | 1.281 | 1.009 [1.000, 1.023] | 1.009 [1.000, 1.023] | 1.122 [1.113, 1.136] | 1.000 [0.993, 1.009] |
| Three-block decline/gain run ratio | 1.434 | 1.011 [0.995, 1.044] | 1.011 [0.995, 1.044] | 1.183 [1.156, 1.215] | 0.994 [0.977, 1.013] |
| Head share, block 1 | 0.614 | 0.580 [0.562, 0.609] | 0.580 [0.562, 0.609] | 0.588 [0.549, 0.648] | 0.593 [0.571, 0.617] |
| Head share, block 2 | 0.573 | 0.590 [0.552, 0.622] | 0.590 [0.552, 0.622] | 0.556 [0.527, 0.633] | 0.616 [0.587, 0.689] |
| Head share, block 3 | 0.529 | 0.592 [0.555, 0.657] | 0.592 [0.555, 0.657] | 0.580 [0.523, 0.615] | 0.645 [0.602, 0.760] |
| Head share, block 4 | 0.506 | 0.590 [0.548, 0.639] | 0.590 [0.548, 0.639] | 0.591 [0.532, 0.644] | 0.671 [0.604, 0.803] |
| Log ladder span, rank 1, block 1 | 11.986 | 10.955 [10.622, 11.398] | 10.955 [10.622, 11.398] | 10.933 [10.256, 11.383] | 11.270 [10.698, 11.658] |
| Log ladder span, rank 1, block 2 | 10.994 | 11.151 [10.374, 11.556] | 11.151 [10.374, 11.556] | 10.023 [9.459, 11.096] | 11.301 [10.851, 12.313] |
| Log ladder span, rank 1, block 3 | 10.543 | 11.200 [10.362, 12.119] | 11.200 [10.362, 12.119] | 10.130 [9.242, 10.770] | 11.250 [10.345, 12.634] |
| Log ladder span, rank 1, block 4 | 10.272 | 10.928 [9.823, 11.869] | 10.928 [9.823, 11.869] | 9.855 [9.183, 11.258] | 11.212 [10.189, 12.974] |
| Log ladder span, rank 195, block 1 | 6.943 | 6.228 [6.183, 6.271] | 6.228 [6.183, 6.271] | 6.274 [6.190, 6.344] | 6.212 [6.148, 6.273] |
| Log ladder span, rank 195, block 2 | 6.186 | 6.202 [6.159, 6.237] | 6.202 [6.159, 6.237] | 6.278 [6.209, 6.342] | 6.216 [6.148, 6.256] |
| Log ladder span, rank 195, block 3 | 6.046 | 6.137 [6.075, 6.195] | 6.137 [6.075, 6.195] | 6.292 [6.180, 6.346] | 6.177 [6.096, 6.246] |
| Log ladder span, rank 195, block 4 | 6.098 | 6.043 [5.983, 6.091] | 6.043 [5.983, 6.091] | 6.339 [6.238, 6.417] | 6.167 [6.065, 6.256] |
| Log ladder span, rank 781, block 1 | 5.332 | 4.740 [4.713, 4.756] | 4.740 [4.713, 4.756] | 4.725 [4.697, 4.779] | 4.710 [4.691, 4.739] |
| Log ladder span, rank 781, block 2 | 4.672 | 4.756 [4.717, 4.797] | 4.756 [4.717, 4.797] | 4.757 [4.723, 4.803] | 4.714 [4.692, 4.775] |
| Log ladder span, rank 781, block 3 | 4.572 | 4.692 [4.669, 4.743] | 4.692 [4.669, 4.743] | 4.765 [4.717, 4.804] | 4.685 [4.635, 4.731] |
| Log ladder span, rank 781, block 4 | 4.681 | 4.634 [4.573, 4.666] | 4.634 [4.573, 4.666] | 4.766 [4.682, 4.824] | 4.644 [4.590, 4.707] |
| Log ladder span, rank 3125, block 1 | 3.230 | 2.723 [2.693, 2.744] | 2.723 [2.693, 2.744] | 2.768 [2.746, 2.779] | 2.730 [2.708, 2.748] |
| Log ladder span, rank 3125, block 2 | 2.790 | 2.668 [2.644, 2.686] | 2.668 [2.644, 2.686] | 2.721 [2.692, 2.754] | 2.661 [2.631, 2.683] |
| Log ladder span, rank 3125, block 3 | 2.728 | 2.535 [2.504, 2.556] | 2.535 [2.504, 2.556] | 2.651 [2.622, 2.697] | 2.523 [2.495, 2.557] |
| Log ladder span, rank 3125, block 4 | 2.812 | 2.373 [2.330, 2.414] | 2.373 [2.330, 2.414] | 2.580 [2.555, 2.609] | 2.402 [2.364, 2.429] |
| E[y given decile 1] | -0.216 | 0.099 [0.071, 0.115] | 0.099 [0.071, 0.115] | -0.188 [-0.200, -0.160] | -0.056 [-0.071, -0.042] |
| E[y given decile 2] | -0.120 | 0.041 [0.029, 0.050] | 0.041 [0.029, 0.050] | -0.092 [-0.104, -0.079] | -0.076 [-0.091, -0.061] |
| E[y given decile 3] | -0.049 | 0.015 [0.004, 0.028] | 0.015 [0.004, 0.028] | -0.058 [-0.069, -0.038] | -0.060 [-0.073, -0.045] |
| E[y given decile 4] | -0.007 | -0.000 [-0.009, 0.017] | -0.000 [-0.009, 0.017] | -0.019 [-0.034, -0.010] | -0.045 [-0.064, -0.032] |
| E[y given decile 5] | 0.021 | -0.005 [-0.019, 0.005] | -0.005 [-0.019, 0.005] | 0.015 [0.006, 0.030] | -0.028 [-0.035, -0.017] |
| E[y given decile 6] | 0.050 | -0.013 [-0.026, 0.003] | -0.013 [-0.026, 0.003] | 0.041 [0.028, 0.055] | -0.002 [-0.018, 0.008] |
| E[y given decile 7] | 0.075 | -0.022 [-0.037, -0.015] | -0.022 [-0.037, -0.015] | 0.073 [0.064, 0.087] | 0.018 [0.005, 0.026] |
| E[y given decile 8] | 0.091 | -0.029 [-0.044, -0.015] | -0.029 [-0.044, -0.015] | 0.108 [0.092, 0.121] | 0.045 [0.030, 0.062] |
| E[y given decile 9] | 0.107 | -0.036 [-0.049, -0.026] | -0.036 [-0.049, -0.026] | 0.142 [0.122, 0.154] | 0.074 [0.065, 0.092] |
| E[y given decile 10] | 0.049 | -0.048 [-0.066, -0.034] | -0.048 [-0.066, -0.034] | -0.025 [-0.055, -0.011] | 0.128 [0.118, 0.143] |

</details>

| Week | Model | No change MAE | EWMA MAE | Kalman X MAE | Median X MAE | Fair X CRPS |
|---|---|---|---|---|---|---|
| 1 | S2 | 0.3279 | 0.2867 | 0.2858 → 0.2852 | 0.2923 → 0.2912 | 0.2091 → 0.2086 |
| 1 | S3 | 0.3279 | 0.2867 | 0.2860 → 0.2880 | 0.2972 → 0.2953 | 0.2128 → 0.2116 |
| 4 | S2 | 0.3944 | 0.3483 | 0.3469 → 0.3460 | 0.3554 → 0.3539 | 0.2548 → 0.2544 |
| 4 | S3 | 0.3944 | 0.3483 | 0.3464 → 0.3470 | 0.3598 → 0.3575 | 0.2580 → 0.2569 |
| 13 | S2 | 0.4839 | 0.4395 | 0.4357 → 0.4344 | 0.4475 → 0.4459 | 0.3235 → 0.3234 |
| 13 | S3 | 0.4839 | 0.4395 | 0.4328 → 0.4330 | 0.4480 → 0.4454 | 0.3236 → 0.3240 |
| 26 | S2 | 0.5759 | 0.5350 | 0.5325 → 0.5309 | 0.5480 → 0.5461 | 0.3964 → 0.3958 |
| 26 | S3 | 0.5759 | 0.5350 | 0.5282 → 0.5274 | 0.5432 → 0.5404 | 0.3929 → 0.3923 |
| 52 | S2 | 0.6991 | 0.6698 | 0.6792 → 0.6766 | 0.6983 → 0.6966 | 0.5048 → 0.5042 |
| 52 | S3 | 0.6991 | 0.6698 | 0.6727 → 0.6727 | 0.6840 → 0.6837 | 0.4965 → 0.4960 |

| Week | Model | 90% coverage | PIT mean | PIT SD | PIT KS |
|---|---|---|---|---|---|
| 1 | S2 | 0.9074 → 0.9053 | 0.5073 → 0.5045 | 0.2772 → 0.2793 | 0.0356 → 0.0317 |
| 1 | S3 | 0.9324 → 0.9104 | 0.5125 → 0.5141 | 0.2632 → 0.2751 | 0.0596 → 0.0451 |
| 4 | S2 | 0.9121 → 0.9075 | 0.5044 → 0.5024 | 0.2724 → 0.2748 | 0.0398 → 0.0349 |
| 4 | S3 | 0.9265 → 0.9246 | 0.5098 → 0.5176 | 0.2640 → 0.2642 | 0.0547 → 0.0623 |
| 13 | S2 | 0.9222 → 0.9172 | 0.4831 → 0.4820 | 0.2617 → 0.2639 | 0.0681 → 0.0665 |
| 13 | S3 | 0.9275 → 0.9267 | 0.4965 → 0.5062 | 0.2602 → 0.2554 | 0.0613 → 0.0688 |
| 26 | S2 | 0.9178 → 0.9133 | 0.4757 → 0.4745 | 0.2631 → 0.2649 | 0.0753 → 0.0748 |
| 26 | S3 | 0.9220 → 0.9211 | 0.4939 → 0.5019 | 0.2625 → 0.2595 | 0.0678 → 0.0716 |
| 52 | S2 | 0.9049 → 0.9021 | 0.4637 → 0.4619 | 0.2688 → 0.2701 | 0.0797 → 0.0802 |
| 52 | S3 | 0.9166 → 0.9151 | 0.4850 → 0.4950 | 0.2647 → 0.2636 | 0.0697 → 0.0664 |

| Week | Model | No change logR | Rank-bin logR | Fair logR CRPS | No change Brier | Rank-bin Brier | Fair head Brier |
|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.1248 | 0.1234 | 0.0826 → 0.0822 | 0.0497 | 0.0407 | 0.0328 → 0.0324 |
| 1 | S3 | 0.1248 | 0.1234 | 0.0848 → 0.0831 | 0.0497 | 0.0407 | 0.0340 → 0.0322 |
| 4 | S2 | 0.1559 | 0.1415 | 0.1038 → 0.1034 | 0.0646 | 0.0499 | 0.0424 → 0.0422 |
| 4 | S3 | 0.1559 | 0.1415 | 0.1054 → 0.1044 | 0.0646 | 0.0499 | 0.0434 → 0.0423 |
| 13 | S2 | 0.1947 | 0.1656 | 0.1323 → 0.1321 | 0.0812 | 0.0626 | 0.0556 → 0.0555 |
| 13 | S3 | 0.1947 | 0.1656 | 0.1325 → 0.1329 | 0.0812 | 0.0626 | 0.0557 → 0.0557 |
| 26 | S2 | 0.2298 | 0.1871 | 0.1573 → 0.1568 | 0.1009 | 0.0751 | 0.0681 → 0.0672 |
| 26 | S3 | 0.2298 | 0.1871 | 0.1567 → 0.1571 | 0.1009 | 0.0751 | 0.0675 → 0.0683 |
| 52 | S2 | 0.2684 | 0.2102 | 0.1898 → 0.1890 | 0.1098 | 0.0805 | 0.0784 → 0.0784 |
| 52 | S3 | 0.2684 | 0.2102 | 0.1888 → 0.1908 | 0.1098 | 0.0805 | 0.0784 → 0.0799 |

| Week | Model | Zero shift MAE | EWMA MAE | Head shift MAE | EWMA corr | Head shift corr |
|---|---|---|---|---|---|---|
| 1 | S2 | 180.19 | 168.04 | 171.30 → 169.81 | 0.505 | 0.362 → 0.402 |
| 1 | S3 | 180.19 | 168.04 | 182.97 → 168.15 | 0.505 | 0.278 → 0.444 |
| 4 | S2 | 256.73 | 233.09 | 233.39 → 229.59 | 0.189 | 0.383 → 0.399 |
| 4 | S3 | 256.73 | 233.09 | 244.51 → 234.25 | 0.189 | 0.327 → 0.298 |
| 13 | S2 | 325.23 | 313.79 | 300.60 → 301.71 | 0.098 | 0.371 → 0.331 |
| 13 | S3 | 325.23 | 313.79 | 312.82 → 304.57 | 0.098 | 0.318 → 0.287 |
| 26 | S2 | 374.10 | 351.97 | 328.04 → 331.07 | 0.302 | 0.633 → 0.611 |
| 26 | S3 | 374.10 | 351.97 | 363.42 → 352.15 | 0.302 | 0.527 → 0.512 |
| 52 | S2 | 459.46 | 447.65 | 408.98 → 405.52 | 0.339 | 0.667 → 0.666 |
| 52 | S3 | 459.46 | 447.65 | 436.61 → 441.83 | 0.339 | 0.642 → 0.585 |

<details><summary>Paired forecast changes across five origins · mean Δ ± split SD</summary>

| Week | Model | X CRPS | Kalman MAE | Coverage | logR CRPS | Head Brier | Head shift MAE × 10⁶ |
|---|---|---|---|---|---|---|---|
| 1 | S2 | -0.0005 ± 0.0003 | -0.0006 ± 0.0001 | -0.0021 ± 0.0036 | -0.0004 ± 0.0001 | -0.0003 ± 0.0002 | -1.4978 ± 2.1281 |
| 1 | S3 | -0.0012 ± 0.0030 | +0.0019 ± 0.0032 | -0.0220 ± 0.0150 | -0.0017 ± 0.0020 | -0.0018 ± 0.0014 | -14.8169 ± 15.6578 |
| 4 | S2 | -0.0004 ± 0.0004 | -0.0009 ± 0.0003 | -0.0046 ± 0.0039 | -0.0004 ± 0.0002 | -0.0002 ± 0.0002 | -3.8002 ± 2.1927 |
| 4 | S3 | -0.0011 ± 0.0031 | +0.0005 ± 0.0024 | -0.0020 ± 0.0153 | -0.0010 ± 0.0016 | -0.0011 ± 0.0008 | -10.2605 ± 9.6468 |
| 13 | S2 | -0.0001 ± 0.0007 | -0.0013 ± 0.0003 | -0.0050 ± 0.0023 | -0.0003 ± 0.0002 | -0.0001 ± 0.0004 | +1.1048 ± 4.0793 |
| 13 | S3 | +0.0004 ± 0.0043 | +0.0002 ± 0.0028 | -0.0008 ± 0.0137 | +0.0004 ± 0.0023 | +0.0001 ± 0.0018 | -8.2527 ± 5.7540 |
| 26 | S2 | -0.0006 ± 0.0008 | -0.0016 ± 0.0006 | -0.0044 ± 0.0027 | -0.0005 ± 0.0002 | -0.0009 ± 0.0008 | +3.0323 ± 5.8647 |
| 26 | S3 | -0.0006 ± 0.0049 | -0.0008 ± 0.0029 | -0.0009 ± 0.0095 | +0.0005 ± 0.0024 | +0.0009 ± 0.0013 | -11.2648 ± 12.9078 |
| 52 | S2 | -0.0005 ± 0.0016 | -0.0026 ± 0.0018 | -0.0027 ± 0.0019 | -0.0007 ± 0.0004 | -0.0001 ± 0.0004 | -3.4635 ± 4.9378 |
| 52 | S3 | -0.0006 ± 0.0062 | +0.0000 ± 0.0068 | -0.0015 ± 0.0122 | +0.0021 ± 0.0039 | +0.0015 ± 0.0021 | +5.2165 ± 25.5446 |

</details>

**Forward time transfer.** Fit and filter the first 104 weeks, then simulate all 109 subsequent weeks. Each column uses its own training fit.

<details><summary>All forward structure statistics, blocks and deciles</summary>

| Statistic | Train observed | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|---|
| Top-10 turnover | 0.200 | 0.300 | 0.200 [0.000, 0.400] | 0.200 [0.100, 0.400] | 0.400 [0.200, 0.500] | 0.200 [0.100, 0.300] |
| K/64 turnover | 0.205 | 0.215 | 0.162 [0.143, 0.190] | 0.164 [0.138, 0.185] | 0.251 [0.220, 0.273] | 0.208 [0.190, 0.232] |
| K/16 turnover | 0.175 | 0.193 | 0.166 [0.151, 0.177] | 0.161 [0.150, 0.177] | 0.200 [0.190, 0.216] | 0.188 [0.175, 0.201] |
| K/4 turnover | 0.164 | 0.153 | 0.132 [0.127, 0.138] | 0.130 [0.127, 0.137] | 0.143 [0.137, 0.149] | 0.142 [0.135, 0.144] |
| K/2 turnover | 0.151 | 0.149 | 0.153 [0.149, 0.156] | 0.151 [0.146, 0.156] | 0.142 [0.134, 0.145] | 0.157 [0.153, 0.161] |
| Weekly occupant change at rank 1 | 0.019 | 0.037 | 0.037 [0.000, 0.206] | 0.019 [0.000, 0.204] | 0.171 [0.072, 0.242] | 0.097 [0.000, 0.178] |
| Weekly occupant change at rank 10 | 0.883 | 0.870 | 0.852 [0.796, 0.926] | 0.852 [0.796, 0.890] | 0.829 [0.731, 0.880] | 0.829 [0.722, 0.881] |
| Maximum log activity | 17.469 | 17.919 | 17.489 [17.107, 17.963] | 17.559 [17.122, 17.857] | 18.858 [18.214, 20.572] | 17.464 [17.099, 18.182] |
| Boundary ratio | 1.065 | 0.947 | 0.987 [0.979, 1.000] | 0.987 [0.982, 1.002] | 1.186 [1.167, 1.220] | 0.985 [0.971, 0.996] |
| Departure rate | 0.007 | 0.007 | 0.013 [0.013, 0.013] | 0.012 [0.012, 0.013] | 0.008 [0.008, 0.009] | 0.013 [0.012, 0.014] |
| Immediate crossing / departures | 0.981 | 0.956 | 0.946 [0.943, 0.951] | 0.944 [0.937, 0.948] | 0.910 [0.894, 0.919] | 0.948 [0.941, 0.953] |
| Absent then returns / departures | 0.013 | 0.019 | 0.053 [0.048, 0.056] | 0.055 [0.051, 0.062] | 0.088 [0.078, 0.105] | 0.051 [0.046, 0.057] |
| Absent beyond window / departures | 0.006 | 0.025 | 0.001 [0.001, 0.002] | 0.001 [0.001, 0.002] | 0.002 [0.000, 0.003] | 0.001 [0.000, 0.002] |
| Gapped persistence ρ | 0.100 | 0.124 | -0.078 [-0.085, -0.070] | -0.073 [-0.083, -0.065] | -0.007 [-0.018, -0.002] | 0.004 [-0.005, 0.015] |
| Asymmetry D | 0.115 | 0.123 | -0.042 [-0.064, -0.021] | -0.043 [-0.053, -0.027] | 0.055 [0.035, 0.076] | -0.042 [-0.061, -0.032] |
| Faller slope | 0.133 | 0.175 | -0.110 [-0.125, -0.100] | -0.106 [-0.116, -0.089] | -0.014 [-0.040, 0.006] | -0.041 [-0.058, -0.021] |
| Riser slope | 0.018 | 0.052 | -0.072 [-0.082, -0.058] | -0.065 [-0.074, -0.053] | -0.067 [-0.085, -0.055] | 0.004 [-0.007, 0.019] |
| Mixed moment m21 | -0.088 | -0.109 | 0.021 [0.007, 0.036] | 0.016 [0.001, 0.034] | -0.250 [-0.288, -0.220] | 0.034 [0.012, 0.052] |
| Mixed moment m12 | 0.038 | 0.001 | 0.029 [0.015, 0.042] | 0.030 [0.021, 0.051] | 0.094 [0.073, 0.124] | 0.043 [0.031, 0.064] |
| Leverage | 1.018 | 1.013 | 0.958 [0.942, 0.978] | 0.945 [0.925, 0.972] | 0.930 [0.889, 0.975] | 0.943 [0.913, 0.969] |
| Two-block decline/gain run ratio | 1.349 | 1.070 | 1.052 [1.038, 1.064] | 1.046 [1.028, 1.061] | 1.235 [1.210, 1.254] | 1.047 [1.031, 1.078] |
| Three-block decline/gain run ratio | 1.610 | 1.120 | 1.094 [1.064, 1.145] | 1.082 [1.053, 1.125] | 1.401 [1.351, 1.458] | 1.098 [1.047, 1.152] |
| Head share, block 1 | 0.621 | 0.529 | 0.575 [0.558, 0.586] | 0.576 [0.558, 0.595] | 0.593 [0.549, 0.750] | 0.573 [0.556, 0.593] |
| Head share, block 2 | 0.575 | 0.506 | 0.561 [0.541, 0.587] | 0.568 [0.529, 0.593] | 0.607 [0.573, 0.687] | 0.584 [0.558, 0.618] |
| Log ladder span, rank 1, block 1 | 12.098 | 10.543 | 10.557 [10.217, 10.884] | 10.707 [10.166, 11.023] | 10.415 [9.893, 11.447] | 10.331 [10.050, 10.995] |
| Log ladder span, rank 1, block 2 | 11.069 | 10.272 | 10.384 [9.737, 11.037] | 10.378 [9.564, 11.115] | 10.342 [9.861, 11.157] | 10.390 [9.731, 11.235] |
| Log ladder span, rank 195, block 1 | 7.026 | 6.046 | 5.913 [5.854, 5.978] | 5.924 [5.859, 5.993] | 5.988 [5.941, 6.042] | 5.832 [5.769, 5.872] |
| Log ladder span, rank 195, block 2 | 6.217 | 6.098 | 5.816 [5.736, 5.857] | 5.822 [5.767, 5.891] | 6.015 [5.958, 6.078] | 5.740 [5.704, 5.810] |
| Log ladder span, rank 781, block 1 | 5.406 | 4.572 | 4.473 [4.451, 4.503] | 4.485 [4.467, 4.497] | 4.531 [4.495, 4.571] | 4.425 [4.397, 4.446] |
| Log ladder span, rank 781, block 2 | 4.717 | 4.681 | 4.443 [4.405, 4.465] | 4.438 [4.405, 4.477] | 4.566 [4.509, 4.596] | 4.386 [4.352, 4.438] |
| Log ladder span, rank 3125, block 1 | 3.281 | 2.728 | 2.548 [2.527, 2.563] | 2.552 [2.517, 2.573] | 2.603 [2.574, 2.635] | 2.525 [2.497, 2.557] |
| Log ladder span, rank 3125, block 2 | 2.812 | 2.812 | 2.397 [2.356, 2.424] | 2.400 [2.377, 2.433] | 2.552 [2.526, 2.584] | 2.370 [2.339, 2.391] |
| E[y given decile 1] | -0.150 | -0.184 | 0.130 [0.119, 0.148] | 0.121 [0.102, 0.132] | 0.015 [-0.007, 0.044] | 0.049 [0.024, 0.070] |
| E[y given decile 2] | -0.060 | -0.061 | 0.052 [0.041, 0.068] | 0.050 [0.032, 0.062] | -0.006 [-0.017, 0.013] | -0.015 [-0.028, 0.002] |
| E[y given decile 3] | -0.026 | -0.034 | 0.029 [0.008, 0.040] | 0.025 [0.007, 0.037] | -0.003 [-0.016, 0.007] | -0.020 [-0.029, -0.004] |
| E[y given decile 4] | -0.003 | -0.005 | 0.007 [-0.012, 0.023] | 0.008 [-0.007, 0.021] | 0.003 [-0.007, 0.011] | -0.020 [-0.034, -0.007] |
| E[y given decile 5] | 0.015 | 0.015 | -0.003 [-0.017, 0.009] | -0.003 [-0.015, 0.006] | 0.003 [-0.009, 0.016] | -0.012 [-0.020, -0.003] |
| E[y given decile 6] | 0.022 | 0.033 | -0.015 [-0.022, 0.000] | -0.015 [-0.025, -0.005] | 0.013 [-0.008, 0.027] | -0.009 [-0.021, 0.003] |
| E[y given decile 7] | 0.033 | 0.051 | -0.029 [-0.038, -0.011] | -0.025 [-0.034, -0.011] | 0.019 [0.004, 0.030] | 0.001 [-0.014, 0.012] |
| E[y given decile 8] | 0.068 | 0.055 | -0.036 [-0.046, -0.017] | -0.034 [-0.050, -0.017] | 0.025 [0.016, 0.035] | 0.005 [-0.011, 0.017] |
| E[y given decile 9] | 0.072 | 0.064 | -0.050 [-0.067, -0.031] | -0.047 [-0.064, -0.026] | 0.023 [0.011, 0.041] | 0.015 [0.001, 0.030] |
| E[y given decile 10] | 0.028 | 0.067 | -0.090 [-0.105, -0.073] | -0.079 [-0.091, -0.065] | -0.089 [-0.118, -0.073] | 0.005 [-0.009, 0.025] |

</details>

**Exact calendar transfer.** Weekly labels 2019-01-07–2020-12-28 train the model, then 2021-01-04–2022-12-19 supply the target. There are only 103 test weeks, so the original 52-week block turnover is undefined. The annual comparison below uses the first 51 available weeks of each year. The omitted final week of 2021 is still simulated.

<details><summary>Exact calendar results, full frozen statistics</summary>

| Statistic | Train observed | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|---|
| Top-10 turnover | 0.200 | — | — | — | — | — |
| K/64 turnover | 0.205 | — | — | — | — | — |
| K/16 turnover | 0.173 | — | — | — | — | — |
| K/4 turnover | 0.165 | — | — | — | — | — |
| K/2 turnover | 0.152 | — | — | — | — | — |
| Weekly occupant change at rank 1 | 0.019 | 0.029 | 0.039 [0.000, 0.119] | 0.000 [0.000, 0.151] | 0.147 [0.065, 0.275] | 0.162 [0.019, 0.324] |
| Weekly occupant change at rank 10 | 0.893 | 0.863 | 0.877 [0.774, 0.922] | 0.863 [0.773, 0.913] | 0.843 [0.744, 0.885] | 0.824 [0.763, 0.912] |
| Maximum log activity | 17.469 | 17.919 | 17.406 [17.113, 18.155] | 17.562 [17.068, 18.246] | 19.015 [18.078, 20.451] | 18.830 [17.824, 21.138] |
| Boundary ratio | 1.039 | 0.945 | 0.999 [0.987, 1.005] | 1.000 [0.985, 1.007] | 1.204 [1.165, 1.223] | 1.198 [1.155, 1.233] |
| Departure rate | 0.008 | 0.007 | 0.014 [0.013, 0.014] | 0.013 [0.012, 0.014] | 0.009 [0.009, 0.010] | 0.010 [0.009, 0.010] |
| Immediate crossing / departures | 0.981 | 0.950 | 0.941 [0.937, 0.945] | 0.939 [0.933, 0.946] | 0.908 [0.894, 0.917] | 0.907 [0.895, 0.913] |
| Absent then returns / departures | 0.013 | 0.021 | 0.058 [0.053, 0.062] | 0.059 [0.053, 0.064] | 0.090 [0.081, 0.103] | 0.092 [0.085, 0.104] |
| Absent beyond window / departures | 0.005 | 0.030 | 0.001 [0.001, 0.003] | 0.001 [0.000, 0.003] | 0.002 [0.001, 0.003] | 0.002 [0.001, 0.003] |
| Gapped persistence ρ | 0.116 | 0.114 | -0.043 [-0.055, -0.034] | -0.042 [-0.051, -0.039] | 0.007 [-0.003, 0.015] | 0.005 [-0.003, 0.013] |
| Asymmetry D | 0.083 | 0.117 | -0.048 [-0.068, -0.039] | -0.048 [-0.067, -0.030] | 0.044 [0.016, 0.063] | 0.047 [0.021, 0.070] |
| Faller slope | 0.126 | 0.155 | -0.078 [-0.087, -0.067] | -0.073 [-0.083, -0.065] | -0.012 [-0.028, 0.006] | -0.013 [-0.030, 0.005] |
| Riser slope | 0.043 | 0.038 | -0.027 [-0.038, -0.011] | -0.025 [-0.038, -0.011] | -0.057 [-0.074, -0.038] | -0.056 [-0.074, -0.042] |
| Mixed moment m21 | -0.026 | -0.133 | 0.032 [0.020, 0.045] | 0.032 [0.018, 0.051] | -0.233 [-0.272, -0.189] | -0.240 [-0.269, -0.204] |
| Mixed moment m12 | 0.054 | -0.002 | 0.030 [0.014, 0.043] | 0.032 [0.012, 0.049] | 0.095 [0.072, 0.125] | 0.097 [0.075, 0.124] |
| Leverage | 0.979 | 1.033 | 0.955 [0.936, 0.976] | 0.953 [0.927, 0.973] | 0.930 [0.866, 0.975] | 0.930 [0.889, 0.978] |
| Two-block decline/gain run ratio | 1.347 | — | — | — | — | — |
| Three-block decline/gain run ratio | 1.578 | — | — | — | — | — |
| Head share, block 1 | 0.613 | 0.506 | 0.551 [0.515, 0.588] | 0.554 [0.514, 0.586] | 0.608 [0.550, 0.724] | 0.598 [0.554, 0.690] |
| Log ladder span, rank 1, block 1 | 11.965 | 10.272 | 10.552 [9.576, 11.134] | 10.572 [9.613, 11.122] | 10.220 [9.752, 11.731] | 10.295 [9.662, 12.104] |
| Log ladder span, rank 195, block 1 | 6.921 | 6.098 | 5.773 [5.746, 5.825] | 5.788 [5.758, 5.850] | 5.953 [5.880, 6.014] | 5.932 [5.858, 6.045] |
| Log ladder span, rank 781, block 1 | 5.309 | 4.681 | 4.404 [4.373, 4.444] | 4.420 [4.385, 4.451] | 4.487 [4.415, 4.511] | 4.473 [4.429, 4.516] |
| Log ladder span, rank 3125, block 1 | 3.221 | 2.812 | 2.366 [2.333, 2.394] | 2.361 [2.334, 2.391] | 2.481 [2.462, 2.509] | 2.485 [2.468, 2.525] |
| E[y given decile 1] | -0.137 | -0.165 | 0.090 [0.078, 0.100] | 0.084 [0.074, 0.095] | 0.013 [-0.007, 0.030] | 0.014 [-0.006, 0.032] |
| E[y given decile 2] | -0.077 | -0.074 | 0.033 [0.018, 0.045] | 0.029 [0.019, 0.044] | -0.021 [-0.032, -0.011] | -0.016 [-0.032, 0.005] |
| E[y given decile 3] | -0.033 | -0.026 | 0.006 [-0.006, 0.023] | 0.008 [-0.002, 0.017] | -0.016 [-0.033, -0.003] | -0.015 [-0.026, 0.001] |
| E[y given decile 4] | -0.030 | -0.002 | 0.000 [-0.012, 0.009] | -0.001 [-0.008, 0.013] | -0.008 [-0.028, 0.009] | -0.006 [-0.019, 0.001] |
| E[y given decile 5] | 0.009 | 0.025 | -0.010 [-0.023, -0.001] | -0.009 [-0.019, 0.005] | -0.000 [-0.010, 0.017] | -0.003 [-0.019, 0.021] |
| E[y given decile 6] | 0.016 | 0.042 | -0.014 [-0.032, 0.003] | -0.011 [-0.021, -0.006] | 0.013 [0.000, 0.026] | 0.016 [0.004, 0.029] |
| E[y given decile 7] | 0.049 | 0.037 | -0.024 [-0.033, -0.012] | -0.022 [-0.035, -0.003] | 0.025 [0.014, 0.039] | 0.024 [0.015, 0.039] |
| E[y given decile 8] | 0.058 | 0.063 | -0.021 [-0.037, -0.004] | -0.029 [-0.037, -0.015] | 0.034 [0.022, 0.041] | 0.030 [0.021, 0.041] |
| E[y given decile 9] | 0.084 | 0.053 | -0.028 [-0.044, -0.014] | -0.027 [-0.040, -0.009] | 0.033 [0.020, 0.053] | 0.033 [0.017, 0.052] |
| E[y given decile 10] | 0.061 | 0.048 | -0.034 [-0.047, -0.013] | -0.031 [-0.046, -0.013] | -0.077 [-0.103, -0.051] | -0.076 [-0.100, -0.056] |

</details>

| 51-week annual blocks | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|
| Top-10 turnover | 0.300 | 0.300 [0.100, 0.400] | 0.300 [0.100, 0.405] | 0.400 [0.295, 0.505] | 0.400 [0.295, 0.505] |
| K/4 turnover | 0.154 | 0.138 [0.130, 0.145] | 0.137 [0.132, 0.144] | 0.143 [0.138, 0.147] | 0.141 [0.134, 0.149] |
| 2021 head share | 0.528 | 0.560 [0.538, 0.588] | 0.564 [0.538, 0.586] | 0.575 [0.552, 0.657] | 0.585 [0.552, 0.669] |
| 2022 head share | 0.506 | 0.551 [0.515, 0.589] | 0.554 [0.514, 0.586] | 0.609 [0.550, 0.724] | 0.599 [0.553, 0.691] |
| 2021 rank-1 span | 10.535 | 10.521 [10.009, 11.168] | 10.636 [10.124, 11.066] | 10.412 [9.954, 11.616] | 10.517 [9.981, 11.726] |
| 2022 rank-1 span | 10.264 | 10.551 [9.573, 11.137] | 10.572 [9.609, 11.124] | 10.213 [9.754, 11.782] | 10.309 [9.670, 12.084] |

#### Reddit Submissions · K = 5,000

<details><summary>All in-sample structure statistics, blocks and deciles</summary>

| Statistic | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|
| Top-10 turnover | 0.300 | 0.233 [0.098, 0.302] | 0.233 [0.098, 0.302] | 0.300 [0.232, 0.402] | 0.217 [0.100, 0.302] |
| K/64 turnover | 0.179 | 0.194 [0.170, 0.227] | 0.194 [0.170, 0.227] | 0.295 [0.264, 0.334] | 0.199 [0.154, 0.231] |
| K/16 turnover | 0.189 | 0.165 [0.156, 0.183] | 0.165 [0.156, 0.183] | 0.237 [0.214, 0.253] | 0.182 [0.172, 0.201] |
| K/4 turnover | 0.162 | 0.141 [0.133, 0.151] | 0.141 [0.133, 0.151] | 0.177 [0.169, 0.182] | 0.142 [0.138, 0.147] |
| K/2 turnover | 0.153 | 0.137 [0.132, 0.142] | 0.137 [0.132, 0.142] | 0.162 [0.157, 0.166] | 0.145 [0.139, 0.148] |
| Weekly occupant change at rank 1 | 0.057 | 0.047 [0.000, 0.131] | 0.047 [0.000, 0.131] | 0.066 [0.000, 0.175] | 0.033 [0.000, 0.110] |
| Weekly occupant change at rank 10 | 0.744 | 0.739 [0.617, 0.820] | 0.739 [0.617, 0.820] | 0.725 [0.681, 0.779] | 0.678 [0.544, 0.782] |
| Maximum log activity | 17.960 | 17.705 [17.056, 18.561] | 17.705 [17.056, 18.561] | 18.523 [17.696, 19.723] | 18.523 [17.687, 20.094] |
| Boundary ratio | 1.116 | 0.986 [0.976, 1.001] | 0.986 [0.976, 1.001] | 1.111 [1.081, 1.145] | 0.974 [0.955, 0.989] |
| Departure rate | 0.012 | 0.015 [0.014, 0.016] | 0.015 [0.014, 0.016] | 0.021 [0.020, 0.022] | 0.016 [0.015, 0.017] |
| Immediate crossing / departures | 0.992 | 0.977 [0.973, 0.981] | 0.977 [0.973, 0.981] | 0.981 [0.978, 0.984] | 0.975 [0.971, 0.978] |
| Absent then returns / departures | 0.006 | 0.023 [0.018, 0.027] | 0.023 [0.018, 0.027] | 0.019 [0.016, 0.021] | 0.024 [0.021, 0.028] |
| Absent beyond window / departures | 0.001 | 0.000 [0.000, 0.001] | 0.000 [0.000, 0.001] | 0.000 [0.000, 0.001] | 0.000 [0.000, 0.001] |
| Gapped persistence ρ | 0.155 [0.138, 0.171] | -0.039 [-0.051, -0.029] | -0.039 [-0.051, -0.029] | 0.088 [0.075, 0.101] | 0.109 [0.099, 0.124] |
| Asymmetry D | 0.222 [0.182, 0.266] | -0.035 [-0.053, -0.012] | -0.035 [-0.053, -0.012] | 0.163 [0.136, 0.190] | -0.031 [-0.049, -0.022] |
| Faller slope | 0.240 [0.204, 0.274] | -0.058 [-0.075, -0.042] | -0.058 [-0.075, -0.042] | 0.123 [0.109, 0.143] | 0.074 [0.062, 0.096] |
| Riser slope | 0.018 [-0.004, 0.039] | -0.029 [-0.044, -0.009] | -0.029 [-0.044, -0.009] | -0.036 [-0.059, -0.007] | 0.109 [0.095, 0.124] |
| Mixed moment m21 | -0.163 [-0.221, -0.111] | 0.024 [0.011, 0.043] | 0.024 [0.011, 0.043] | -0.348 [-0.402, -0.286] | 0.033 [0.024, 0.053] |
| Mixed moment m12 | 0.007 [-0.054, 0.063] | -0.016 [-0.036, -0.002] | -0.016 [-0.036, -0.002] | 0.101 [0.073, 0.140] | -0.021 [-0.034, -0.003] |
| Leverage | 1.129 [1.013, 1.277] | 1.030 [0.994, 1.081] | 1.030 [0.994, 1.081] | 0.873 [0.838, 0.936] | 1.038 [0.998, 1.067] |
| Two-block decline/gain run ratio | 1.245 | 1.021 [0.999, 1.036] | 1.021 [0.999, 1.036] | 1.130 [1.110, 1.143] | 0.991 [0.974, 1.009] |
| Three-block decline/gain run ratio | 1.322 | 1.032 [0.990, 1.062] | 1.032 [0.990, 1.062] | 1.187 [1.163, 1.223] | 0.986 [0.963, 1.019] |
| Head share, block 1 | 0.511 | 0.477 [0.457, 0.499] | 0.477 [0.457, 0.499] | 0.509 [0.477, 0.575] | 0.492 [0.485, 0.516] |
| Head share, block 2 | 0.507 | 0.486 [0.456, 0.536] | 0.486 [0.456, 0.536] | 0.524 [0.441, 0.656] | 0.529 [0.468, 0.586] |
| Head share, block 3 | 0.427 | 0.492 [0.454, 0.560] | 0.492 [0.454, 0.560] | 0.544 [0.450, 0.668] | 0.575 [0.490, 0.681] |
| Head share, block 4 | 0.356 | 0.511 [0.464, 0.594] | 0.511 [0.464, 0.594] | 0.565 [0.462, 0.652] | 0.607 [0.546, 0.766] |
| Log ladder span, rank 1, block 1 | 9.090 | 8.802 [8.333, 9.189] | 8.802 [8.333, 9.189] | 9.578 [8.914, 10.198] | 9.025 [8.733, 9.398] |
| Log ladder span, rank 1, block 2 | 9.134 | 8.873 [8.361, 9.942] | 8.873 [8.361, 9.942] | 9.609 [8.342, 10.853] | 9.323 [8.643, 10.139] |
| Log ladder span, rank 1, block 3 | 8.380 | 9.038 [8.442, 10.068] | 9.038 [8.442, 10.068] | 9.342 [8.490, 11.008] | 9.601 [8.942, 10.906] |
| Log ladder span, rank 1, block 4 | 7.062 | 9.007 [8.290, 9.936] | 9.007 [8.290, 9.936] | 9.680 [8.434, 10.661] | 9.934 [9.076, 11.493] |
| Log ladder span, rank 78, block 1 | 5.483 | 5.275 [5.228, 5.355] | 5.275 [5.228, 5.355] | 5.272 [5.193, 5.395] | 5.271 [5.177, 5.344] |
| Log ladder span, rank 78, block 2 | 4.927 | 5.310 [5.199, 5.396] | 5.310 [5.199, 5.396] | 5.297 [5.207, 5.503] | 5.305 [5.188, 5.394] |
| Log ladder span, rank 78, block 3 | 4.732 | 5.296 [5.229, 5.453] | 5.296 [5.229, 5.453] | 5.432 [5.277, 5.514] | 5.420 [5.361, 5.538] |
| Log ladder span, rank 78, block 4 | 4.701 | 5.260 [5.173, 5.404] | 5.260 [5.173, 5.404] | 5.477 [5.335, 5.685] | 5.510 [5.410, 5.680] |
| Log ladder span, rank 312, block 1 | 4.023 | 3.923 [3.883, 3.955] | 3.923 [3.883, 3.955] | 3.941 [3.907, 3.989] | 3.906 [3.880, 3.957] |
| Log ladder span, rank 312, block 2 | 3.604 | 3.939 [3.918, 3.990] | 3.939 [3.918, 3.990] | 4.006 [3.943, 4.042] | 3.973 [3.909, 4.012] |
| Log ladder span, rank 312, block 3 | 3.445 | 3.962 [3.890, 3.996] | 3.962 [3.890, 3.996] | 4.078 [4.012, 4.137] | 4.036 [3.967, 4.086] |
| Log ladder span, rank 312, block 4 | 3.433 | 3.928 [3.836, 3.991] | 3.928 [3.836, 3.991] | 4.131 [4.050, 4.182] | 4.095 [4.022, 4.158] |
| Log ladder span, rank 1250, block 1 | 2.327 | 2.257 [2.230, 2.283] | 2.257 [2.230, 2.283] | 2.257 [2.218, 2.277] | 2.248 [2.229, 2.281] |
| Log ladder span, rank 1250, block 2 | 2.017 | 2.235 [2.209, 2.267] | 2.235 [2.209, 2.267] | 2.266 [2.242, 2.323] | 2.260 [2.226, 2.296] |
| Log ladder span, rank 1250, block 3 | 1.955 | 2.168 [2.119, 2.203] | 2.168 [2.119, 2.203] | 2.269 [2.244, 2.300] | 2.280 [2.234, 2.317] |
| Log ladder span, rank 1250, block 4 | 2.058 | 2.085 [2.034, 2.143] | 2.085 [2.034, 2.143] | 2.248 [2.216, 2.288] | 2.275 [2.224, 2.323] |
| E[y given decile 1] | -0.265 | 0.070 [0.050, 0.091] | 0.070 [0.050, 0.091] | -0.164 [-0.191, -0.147] | -0.090 [-0.117, -0.076] |
| E[y given decile 2] | -0.102 | 0.031 [0.011, 0.048] | 0.031 [0.011, 0.048] | -0.072 [-0.095, -0.049] | -0.087 [-0.105, -0.073] |
| E[y given decile 3] | -0.041 | 0.010 [-0.008, 0.031] | 0.010 [-0.008, 0.031] | -0.042 [-0.054, -0.022] | -0.064 [-0.085, -0.047] |
| E[y given decile 4] | 0.018 | 0.003 [-0.008, 0.023] | 0.003 [-0.008, 0.023] | -0.011 [-0.034, -0.001] | -0.044 [-0.061, -0.018] |
| E[y given decile 5] | 0.043 | -0.007 [-0.025, 0.015] | -0.007 [-0.025, 0.015] | 0.012 [-0.007, 0.032] | -0.025 [-0.039, 0.000] |
| E[y given decile 6] | 0.052 | -0.011 [-0.032, 0.008] | -0.011 [-0.032, 0.008] | 0.049 [0.023, 0.062] | 0.001 [-0.015, 0.018] |
| E[y given decile 7] | 0.076 | -0.020 [-0.037, -0.000] | -0.020 [-0.037, -0.000] | 0.072 [0.044, 0.095] | 0.027 [0.011, 0.044] |
| E[y given decile 8] | 0.076 | -0.018 [-0.040, -0.001] | -0.018 [-0.040, -0.001] | 0.102 [0.083, 0.124] | 0.057 [0.044, 0.083] |
| E[y given decile 9] | 0.112 | -0.027 [-0.041, -0.007] | -0.027 [-0.041, -0.007] | 0.122 [0.107, 0.133] | 0.090 [0.071, 0.104] |
| E[y given decile 10] | 0.033 | -0.036 [-0.054, -0.011] | -0.036 [-0.054, -0.011] | -0.056 [-0.092, -0.012] | 0.135 [0.120, 0.154] |

</details>

| Week | Model | No change MAE | EWMA MAE | Kalman X MAE | Median X MAE | Fair X CRPS |
|---|---|---|---|---|---|---|
| 1 | S2 | 0.1839 | 0.1668 | 0.1663 → 0.1656 | 0.1734 → 0.1696 | 0.1274 → 0.1239 |
| 1 | S3 | 0.1839 | 0.1668 | 0.1665 → 0.1671 | 0.1808 → 0.1707 | 0.1334 → 0.1246 |
| 4 | S2 | 0.2291 | 0.2103 | 0.2095 → 0.2091 | 0.2185 → 0.2152 | 0.1618 → 0.1590 |
| 4 | S3 | 0.2291 | 0.2103 | 0.2097 → 0.2096 | 0.2247 → 0.2152 | 0.1662 → 0.1582 |
| 13 | S2 | 0.3064 | 0.2880 | 0.2869 → 0.2861 | 0.2981 → 0.2949 | 0.2220 → 0.2192 |
| 13 | S3 | 0.3064 | 0.2880 | 0.2855 → 0.2870 | 0.3028 → 0.2939 | 0.2225 → 0.2165 |
| 26 | S2 | 0.3929 | 0.3773 | 0.3776 → 0.3760 | 0.3879 → 0.3836 | 0.2849 → 0.2828 |
| 26 | S3 | 0.3929 | 0.3773 | 0.3723 → 0.3767 | 0.3862 → 0.3795 | 0.2826 → 0.2784 |
| 52 | S2 | 0.5187 | 0.5072 | 0.5123 → 0.5085 | 0.5191 → 0.5139 | 0.3820 → 0.3788 |
| 52 | S3 | 0.5187 | 0.5072 | 0.5035 → 0.5116 | 0.5189 → 0.5120 | 0.3814 → 0.3765 |

| Week | Model | 90% coverage | PIT mean | PIT SD | PIT KS |
|---|---|---|---|---|---|
| 1 | S2 | 0.9520 → 0.9236 | 0.5240 → 0.5003 | 0.2392 → 0.2637 | 0.1077 → 0.0573 |
| 1 | S3 | 0.9648 → 0.9106 | 0.5459 → 0.5093 | 0.2265 → 0.2726 | 0.1462 → 0.0586 |
| 4 | S2 | 0.9513 → 0.9281 | 0.5177 → 0.5000 | 0.2356 → 0.2549 | 0.1093 → 0.0715 |
| 4 | S3 | 0.9595 → 0.9190 | 0.5438 → 0.5140 | 0.2303 → 0.2622 | 0.1395 → 0.0759 |
| 13 | S2 | 0.9515 → 0.9335 | 0.5056 → 0.4929 | 0.2321 → 0.2456 | 0.1039 → 0.0886 |
| 13 | S3 | 0.9531 → 0.9248 | 0.5425 → 0.5166 | 0.2350 → 0.2546 | 0.1300 → 0.0828 |
| 26 | S2 | 0.9505 → 0.9349 | 0.4960 → 0.4850 | 0.2378 → 0.2478 | 0.0937 → 0.0900 |
| 26 | S3 | 0.9473 → 0.9210 | 0.5387 → 0.5142 | 0.2409 → 0.2573 | 0.1218 → 0.0735 |
| 52 | S2 | 0.9444 → 0.9339 | 0.4896 → 0.4805 | 0.2435 → 0.2499 | 0.0886 → 0.0907 |
| 52 | S3 | 0.9484 → 0.9259 | 0.5379 → 0.5184 | 0.2390 → 0.2541 | 0.1237 → 0.0908 |

| Week | Model | No change logR | Rank-bin logR | Fair logR CRPS | No change Brier | Rank-bin Brier | Fair head Brier |
|---|---|---|---|---|---|---|---|
| 1 | S2 | 0.0955 | 0.1035 | 0.0677 → 0.0649 | 0.0319 | 0.0346 | 0.0221 → 0.0216 |
| 1 | S3 | 0.0955 | 0.1035 | 0.0712 → 0.0650 | 0.0319 | 0.0346 | 0.0239 → 0.0220 |
| 4 | S2 | 0.1229 | 0.1193 | 0.0875 → 0.0852 | 0.0447 | 0.0426 | 0.0332 → 0.0327 |
| 4 | S3 | 0.1229 | 0.1193 | 0.0903 → 0.0847 | 0.0447 | 0.0426 | 0.0342 → 0.0331 |
| 13 | S2 | 0.1675 | 0.1459 | 0.1229 → 0.1204 | 0.0556 | 0.0489 | 0.0432 → 0.0422 |
| 13 | S3 | 0.1675 | 0.1459 | 0.1243 → 0.1192 | 0.0556 | 0.0489 | 0.0449 → 0.0422 |
| 26 | S2 | 0.2121 | 0.1736 | 0.1546 → 0.1519 | 0.0713 | 0.0575 | 0.0540 → 0.0534 |
| 26 | S3 | 0.2121 | 0.1736 | 0.1564 → 0.1516 | 0.0713 | 0.0575 | 0.0570 → 0.0537 |
| 52 | S2 | 0.2706 | 0.2121 | 0.1990 → 0.1954 | 0.0986 | 0.0752 | 0.0714 → 0.0718 |
| 52 | S3 | 0.2706 | 0.2121 | 0.2030 → 0.1987 | 0.0986 | 0.0752 | 0.0772 → 0.0759 |

| Week | Model | Zero shift MAE | EWMA MAE | Head shift MAE | EWMA corr | Head shift corr |
|---|---|---|---|---|---|---|
| 1 | S2 | 198.81 | 205.57 | 205.99 → 198.03 | 0.182 | 0.206 → 0.229 |
| 1 | S3 | 198.81 | 205.57 | 249.07 → 203.35 | 0.182 | 0.190 → 0.173 |
| 4 | S2 | 280.75 | 268.26 | 280.06 → 274.98 | 0.116 | 0.207 → 0.183 |
| 4 | S3 | 280.75 | 268.26 | 318.68 → 277.75 | 0.116 | 0.368 → 0.257 |
| 13 | S2 | 475.45 | 466.79 | 485.79 → 475.54 | 0.059 | 0.011 → -0.020 |
| 13 | S3 | 475.45 | 466.79 | 502.69 → 475.52 | 0.059 | 0.389 → 0.362 |
| 26 | S2 | 643.03 | 637.39 | 640.90 → 625.84 | 0.082 | 0.075 → 0.140 |
| 26 | S3 | 643.03 | 637.39 | 623.92 → 619.34 | 0.082 | 0.687 → 0.614 |
| 52 | S2 | 923.94 | 924.22 | 865.72 → 851.17 | 0.078 | 0.471 → 0.446 |
| 52 | S3 | 923.94 | 924.22 | 838.68 → 920.43 | 0.078 | 0.807 → 0.461 |

<details><summary>Paired forecast changes across five origins · mean Δ ± split SD</summary>

| Week | Model | X CRPS | Kalman MAE | Coverage | logR CRPS | Head Brier | Head shift MAE × 10⁶ |
|---|---|---|---|---|---|---|---|
| 1 | S2 | -0.0035 ± 0.0010 | -0.0007 ± 0.0010 | -0.0284 ± 0.0082 | -0.0027 ± 0.0005 | -0.0005 ± 0.0005 | -7.9626 ± 5.1408 |
| 1 | S3 | -0.0088 ± 0.0028 | +0.0005 ± 0.0005 | -0.0542 ± 0.0100 | -0.0062 ± 0.0020 | -0.0019 ± 0.0016 | -45.7249 ± 18.4617 |
| 4 | S2 | -0.0028 ± 0.0007 | -0.0004 ± 0.0011 | -0.0232 ± 0.0085 | -0.0022 ± 0.0004 | -0.0004 ± 0.0003 | -5.0765 ± 4.5537 |
| 4 | S3 | -0.0080 ± 0.0033 | -0.0001 ± 0.0011 | -0.0405 ± 0.0071 | -0.0057 ± 0.0017 | -0.0011 ± 0.0023 | -40.9229 ± 30.0610 |
| 13 | S2 | -0.0028 ± 0.0013 | -0.0008 ± 0.0007 | -0.0180 ± 0.0072 | -0.0025 ± 0.0004 | -0.0010 ± 0.0005 | -10.2565 ± 17.2355 |
| 13 | S3 | -0.0060 ± 0.0037 | +0.0015 ± 0.0006 | -0.0283 ± 0.0078 | -0.0051 ± 0.0019 | -0.0027 ± 0.0011 | -27.1710 ± 45.9232 |
| 26 | S2 | -0.0022 ± 0.0016 | -0.0016 ± 0.0008 | -0.0156 ± 0.0066 | -0.0027 ± 0.0003 | -0.0006 ± 0.0008 | -15.0643 ± 14.4199 |
| 26 | S3 | -0.0041 ± 0.0024 | +0.0044 ± 0.0011 | -0.0263 ± 0.0064 | -0.0048 ± 0.0016 | -0.0034 ± 0.0013 | -4.5785 ± 37.2988 |
| 52 | S2 | -0.0032 ± 0.0012 | -0.0037 ± 0.0019 | -0.0105 ± 0.0027 | -0.0036 ± 0.0006 | +0.0004 ± 0.0005 | -14.5500 ± 10.6925 |
| 52 | S3 | -0.0048 ± 0.0026 | +0.0081 ± 0.0022 | -0.0225 ± 0.0081 | -0.0043 ± 0.0011 | -0.0013 ± 0.0026 | +81.7539 ± 99.2652 |

</details>

**Forward time transfer.** Fit and filter the first 104 weeks, then simulate all 108 subsequent weeks. Each column uses its own training fit.

<details><summary>All forward structure statistics, blocks and deciles</summary>

| Statistic | Train observed | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|---|
| Top-10 turnover | 0.300 | 0.300 | 0.300 [0.100, 0.305] | 0.300 [0.195, 0.400] | 0.400 [0.200, 0.505] | 0.200 [0.095, 0.400] |
| K/64 turnover | 0.218 | 0.167 | 0.205 [0.141, 0.269] | 0.205 [0.140, 0.257] | 0.282 [0.231, 0.333] | 0.192 [0.141, 0.219] |
| K/16 turnover | 0.234 | 0.170 | 0.199 [0.170, 0.219] | 0.196 [0.176, 0.219] | 0.247 [0.227, 0.273] | 0.192 [0.170, 0.206] |
| K/4 turnover | 0.174 | 0.151 | 0.183 [0.172, 0.191] | 0.182 [0.167, 0.193] | 0.197 [0.188, 0.210] | 0.173 [0.163, 0.186] |
| K/2 turnover | 0.163 | 0.149 | 0.181 [0.177, 0.189] | 0.179 [0.172, 0.187] | 0.168 [0.161, 0.179] | 0.180 [0.167, 0.188] |
| Weekly occupant change at rank 1 | 0.078 | 0.037 | 0.000 [0.000, 0.085] | 0.000 [0.000, 0.085] | 0.084 [0.000, 0.152] | 0.000 [0.000, 0.010] |
| Weekly occupant change at rank 10 | 0.718 | 0.766 | 0.780 [0.672, 0.870] | 0.752 [0.641, 0.815] | 0.813 [0.682, 0.888] | 0.701 [0.587, 0.814] |
| Maximum log activity | 17.960 | 17.868 | 18.268 [17.844, 19.246] | 18.248 [17.909, 19.419] | 18.328 [17.891, 19.884] | 18.927 [18.193, 20.086] |
| Boundary ratio | 1.145 | 0.881 | 0.985 [0.970, 0.996] | 0.986 [0.970, 1.001] | 1.291 [1.256, 1.325] | 0.965 [0.946, 0.984] |
| Departure rate | 0.005 | 0.005 | 0.013 [0.012, 0.014] | 0.010 [0.008, 0.010] | 0.007 [0.006, 0.008] | 0.009 [0.008, 0.010] |
| Immediate crossing / departures | 0.998 | 0.950 | 0.986 [0.982, 0.990] | 0.982 [0.975, 0.988] | 0.972 [0.961, 0.981] | 0.980 [0.975, 0.986] |
| Absent then returns / departures | 0.002 | 0.028 | 0.014 [0.009, 0.018] | 0.018 [0.012, 0.024] | 0.027 [0.019, 0.039] | 0.020 [0.014, 0.025] |
| Absent beyond window / departures | 0.000 | 0.022 | 0.000 [0.000, 0.001] | 0.000 [0.000, 0.001] | 0.000 [0.000, 0.003] | 0.000 [0.000, 0.001] |
| Gapped persistence ρ | 0.117 | 0.102 | -0.033 [-0.048, -0.017] | -0.027 [-0.035, -0.012] | 0.085 [0.073, 0.102] | 0.046 [0.034, 0.064] |
| Asymmetry D | 0.140 | 0.133 | -0.029 [-0.051, -0.007] | -0.024 [-0.045, 0.005] | 0.163 [0.144, 0.190] | -0.020 [-0.042, -0.000] |
| Faller slope | 0.175 | 0.158 | -0.053 [-0.065, -0.036] | -0.044 [-0.066, -0.022] | 0.110 [0.085, 0.141] | 0.018 [0.001, 0.046] |
| Riser slope | 0.034 | 0.025 | -0.024 [-0.039, -0.004] | -0.020 [-0.031, -0.006] | -0.061 [-0.069, -0.015] | 0.039 [0.025, 0.059] |
| Mixed moment m21 | -0.072 | -0.123 | 0.018 [0.001, 0.038] | 0.019 [-0.003, 0.033] | -0.471 [-0.537, -0.415] | 0.022 [0.001, 0.041] |
| Mixed moment m12 | 0.155 | 0.040 | 0.002 [-0.017, 0.022] | -0.003 [-0.021, 0.016] | 0.145 [0.111, 0.190] | -0.011 [-0.029, 0.015] |
| Leverage | 0.862 | 1.191 | 0.993 [0.955, 1.047] | 1.009 [0.960, 1.053] | 0.915 [0.799, 0.947] | 1.016 [0.969, 1.048] |
| Two-block decline/gain run ratio | 1.370 | 1.111 | 1.031 [1.002, 1.055] | 1.029 [1.007, 1.045] | 1.288 [1.236, 1.309] | 1.008 [0.987, 1.031] |
| Three-block decline/gain run ratio | 1.615 | 1.108 | 1.053 [0.979, 1.098] | 1.033 [0.985, 1.070] | 1.435 [1.345, 1.510] | 1.025 [0.966, 1.079] |
| Head share, block 1 | 0.515 | 0.427 | 0.517 [0.481, 0.564] | 0.514 [0.490, 0.562] | 0.484 [0.446, 0.514] | 0.526 [0.497, 0.568] |
| Head share, block 2 | 0.509 | 0.356 | 0.523 [0.471, 0.601] | 0.523 [0.474, 0.620] | 0.508 [0.446, 0.569] | 0.572 [0.494, 0.688] |
| Log ladder span, rank 1, block 1 | 9.067 | 8.380 | 9.161 [8.636, 9.768] | 9.176 [8.760, 9.748] | 8.962 [8.540, 9.517] | 9.308 [8.916, 9.835] |
| Log ladder span, rank 1, block 2 | 9.135 | 7.062 | 8.794 [7.977, 9.833] | 8.822 [8.094, 10.168] | 8.551 [8.126, 9.428] | 9.615 [8.815, 10.742] |
| Log ladder span, rank 78, block 1 | 5.514 | 4.732 | 4.856 [4.761, 4.941] | 4.833 [4.772, 4.919] | 4.845 [4.786, 4.939] | 4.807 [4.753, 4.889] |
| Log ladder span, rank 78, block 2 | 4.947 | 4.701 | 4.825 [4.738, 4.991] | 4.854 [4.726, 4.977] | 4.971 [4.839, 5.084] | 4.889 [4.796, 4.952] |
| Log ladder span, rank 312, block 1 | 4.060 | 3.445 | 3.496 [3.453, 3.552] | 3.492 [3.460, 3.554] | 3.530 [3.486, 3.573] | 3.496 [3.460, 3.537] |
| Log ladder span, rank 312, block 2 | 3.629 | 3.433 | 3.497 [3.434, 3.561] | 3.482 [3.439, 3.543] | 3.633 [3.584, 3.697] | 3.506 [3.456, 3.563] |
| Log ladder span, rank 1250, block 1 | 2.362 | 1.955 | 1.931 [1.918, 1.959] | 1.932 [1.906, 1.955] | 1.992 [1.974, 2.022] | 1.919 [1.885, 1.942] |
| Log ladder span, rank 1250, block 2 | 2.025 | 2.058 | 1.876 [1.839, 1.900] | 1.870 [1.831, 1.915] | 2.066 [2.041, 2.111] | 1.890 [1.857, 1.934] |
| E[y given decile 1] | -0.152 | -0.125 | 0.051 [0.035, 0.063] | 0.040 [0.020, 0.060] | -0.099 [-0.125, -0.076] | -0.016 [-0.039, -0.001] |
| E[y given decile 2] | -0.065 | -0.033 | 0.022 [-0.004, 0.041] | 0.014 [-0.001, 0.026] | -0.056 [-0.072, -0.038] | -0.029 [-0.047, -0.018] |
| E[y given decile 3] | -0.026 | -0.023 | 0.006 [-0.015, 0.031] | 0.009 [-0.006, 0.014] | -0.032 [-0.050, -0.018] | -0.023 [-0.033, -0.006] |
| E[y given decile 4] | 0.010 | 0.000 | 0.003 [-0.012, 0.012] | -0.004 [-0.016, 0.013] | -0.007 [-0.024, 0.007] | -0.019 [-0.031, -0.005] |
| E[y given decile 5] | 0.017 | 0.009 | -0.003 [-0.018, 0.006] | -0.005 [-0.018, 0.012] | 0.016 [-0.008, 0.036] | -0.006 [-0.023, 0.005] |
| E[y given decile 6] | 0.028 | 0.037 | -0.006 [-0.026, 0.011] | -0.002 [-0.015, 0.013] | 0.029 [0.021, 0.040] | -0.001 [-0.018, 0.013] |
| E[y given decile 7] | 0.037 | 0.025 | -0.016 [-0.036, 0.005] | -0.008 [-0.024, 0.002] | 0.055 [0.029, 0.071] | 0.007 [-0.002, 0.021] |
| E[y given decile 8] | 0.054 | 0.037 | -0.010 [-0.041, 0.002] | -0.012 [-0.029, -0.002] | 0.076 [0.056, 0.088] | 0.021 [0.010, 0.034] |
| E[y given decile 9] | 0.054 | 0.051 | -0.018 [-0.040, 0.001] | -0.010 [-0.023, -0.000] | 0.095 [0.078, 0.111] | 0.029 [0.014, 0.048] |
| E[y given decile 10] | 0.042 | 0.022 | -0.024 [-0.039, -0.004] | -0.019 [-0.029, -0.006] | -0.075 [-0.088, -0.019] | 0.036 [0.023, 0.054] |

</details>

**Exact calendar transfer.** Weekly labels 2019-01-07–2020-12-28 train the model, then 2021-01-04–2022-12-19 supply the target. There are only 103 test weeks, so the original 52-week block turnover is undefined. The annual comparison below uses the first 51 available weeks of each year. The omitted final week of 2021 is still simulated.

<details><summary>Exact calendar results, full frozen statistics</summary>

| Statistic | Train observed | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|---|
| Top-10 turnover | 0.200 | — | — | — | — | — |
| K/64 turnover | 0.218 | — | — | — | — | — |
| K/16 turnover | 0.228 | — | — | — | — | — |
| K/4 turnover | 0.168 | — | — | — | — | — |
| K/2 turnover | 0.165 | — | — | — | — | — |
| Weekly occupant change at rank 1 | 0.049 | 0.039 | 0.000 [0.000, 0.059] | 0.000 [0.000, 0.102] | 0.034 [0.000, 0.130] | 0.000 [0.000, 0.021] |
| Weekly occupant change at rank 10 | 0.738 | 0.775 | 0.784 [0.735, 0.892] | 0.721 [0.627, 0.805] | 0.789 [0.725, 0.863] | 0.725 [0.607, 0.833] |
| Maximum log activity | 17.960 | 17.788 | 18.198 [17.802, 19.243] | 18.256 [17.731, 19.058] | 18.211 [17.845, 19.241] | 18.694 [17.813, 19.831] |
| Boundary ratio | 1.164 | 0.891 | 0.993 [0.972, 1.005] | 0.983 [0.962, 1.001] | 1.263 [1.189, 1.286] | 0.971 [0.953, 1.001] |
| Departure rate | 0.005 | 0.005 | 0.011 [0.010, 0.012] | 0.008 [0.007, 0.008] | 0.009 [0.008, 0.010] | 0.007 [0.007, 0.008] |
| Immediate crossing / departures | 0.997 | 0.946 | 0.987 [0.982, 0.990] | 0.980 [0.973, 0.985] | 0.982 [0.973, 0.987] | 0.981 [0.973, 0.988] |
| Absent then returns / departures | 0.003 | 0.029 | 0.013 [0.010, 0.018] | 0.018 [0.014, 0.027] | 0.018 [0.012, 0.027] | 0.018 [0.012, 0.027] |
| Absent beyond window / departures | 0.000 | 0.025 | 0.000 [0.000, 0.001] | 0.000 [0.000, 0.001] | 0.001 [0.000, 0.002] | 0.000 [0.000, 0.001] |
| Gapped persistence ρ | 0.139 | 0.096 | -0.038 [-0.056, -0.025] | -0.028 [-0.044, -0.014] | 0.033 [0.017, 0.051] | 0.051 [0.041, 0.067] |
| Asymmetry D | 0.136 | 0.146 | -0.026 [-0.056, -0.000] | -0.016 [-0.043, 0.013] | 0.109 [0.086, 0.138] | -0.018 [-0.046, 0.020] |
| Faller slope | 0.181 | 0.164 | -0.059 [-0.077, -0.032] | -0.041 [-0.070, -0.010] | 0.040 [0.011, 0.069] | 0.031 [0.011, 0.052] |
| Riser slope | 0.045 | 0.018 | -0.031 [-0.065, -0.003] | -0.021 [-0.051, -0.004] | -0.071 [-0.092, -0.047] | 0.045 [0.025, 0.069] |
| Mixed moment m21 | -0.060 | -0.137 | 0.012 [-0.002, 0.035] | 0.010 [-0.005, 0.026] | -0.392 [-0.437, -0.338] | 0.021 [-0.006, 0.050] |
| Mixed moment m12 | 0.151 | 0.069 | 0.007 [-0.010, 0.035] | 0.005 [-0.020, 0.027] | 0.110 [0.082, 0.161] | 0.003 [-0.017, 0.030] |
| Leverage | 0.898 | 1.164 | 0.992 [0.950, 1.030] | 0.983 [0.955, 1.034] | 0.937 [0.838, 1.037] | 0.992 [0.960, 1.049] |
| Two-block decline/gain run ratio | 1.356 | — | — | — | — | — |
| Three-block decline/gain run ratio | 1.570 | — | — | — | — | — |
| Head share, block 1 | 0.510 | 0.356 | 0.504 [0.459, 0.593] | 0.502 [0.454, 0.579] | 0.462 [0.417, 0.518] | 0.528 [0.458, 0.652] |
| Log ladder span, rank 1, block 1 | 9.099 | 7.062 | 8.855 [8.141, 9.860] | 8.824 [8.109, 9.798] | 8.583 [7.948, 9.306] | 9.346 [8.553, 10.464] |
| Log ladder span, rank 78, block 1 | 5.471 | 4.701 | 4.818 [4.732, 4.941] | 4.814 [4.758, 4.969] | 4.889 [4.788, 4.952] | 4.872 [4.750, 4.950] |
| Log ladder span, rank 312, block 1 | 4.034 | 3.433 | 3.475 [3.412, 3.511] | 3.459 [3.413, 3.518] | 3.583 [3.517, 3.619] | 3.483 [3.452, 3.559] |
| Log ladder span, rank 1250, block 1 | 2.319 | 2.058 | 1.899 [1.854, 1.914] | 1.889 [1.858, 1.903] | 2.023 [1.987, 2.056] | 1.903 [1.877, 1.955] |
| E[y given decile 1] | -0.149 | -0.131 | 0.056 [0.030, 0.073] | 0.036 [0.009, 0.062] | -0.038 [-0.067, -0.011] | -0.026 [-0.043, -0.009] |
| E[y given decile 2] | -0.076 | -0.011 | 0.023 [0.003, 0.037] | 0.013 [0.007, 0.036] | -0.033 [-0.053, -0.016] | -0.030 [-0.052, -0.020] |
| E[y given decile 3] | -0.019 | -0.013 | 0.007 [-0.010, 0.032] | 0.001 [-0.016, 0.019] | -0.018 [-0.033, 0.002] | -0.021 [-0.048, -0.008] |
| E[y given decile 4] | -0.021 | -0.015 | 0.006 [-0.013, 0.021] | 0.002 [-0.011, 0.033] | -0.007 [-0.028, 0.004] | -0.019 [-0.031, 0.006] |
| E[y given decile 5] | 0.011 | 0.018 | -0.004 [-0.014, 0.015] | -0.003 [-0.017, 0.013] | 0.007 [-0.008, 0.024] | -0.012 [-0.026, 0.004] |
| E[y given decile 6] | 0.042 | 0.019 | -0.005 [-0.024, 0.014] | -0.002 [-0.015, 0.015] | 0.026 [0.011, 0.045] | 0.004 [-0.008, 0.017] |
| E[y given decile 7] | 0.030 | 0.045 | -0.008 [-0.039, 0.008] | -0.005 [-0.026, 0.012] | 0.037 [0.021, 0.056] | 0.008 [-0.001, 0.031] |
| E[y given decile 8] | 0.057 | 0.034 | -0.017 [-0.036, 0.002] | -0.004 [-0.036, 0.004] | 0.055 [0.028, 0.075] | 0.020 [-0.000, 0.035] |
| E[y given decile 9] | 0.068 | 0.039 | -0.020 [-0.044, -0.001] | -0.019 [-0.036, 0.007] | 0.065 [0.040, 0.086] | 0.032 [0.020, 0.051] |
| E[y given decile 10] | 0.057 | 0.017 | -0.031 [-0.066, -0.003] | -0.019 [-0.048, -0.004] | -0.096 [-0.126, -0.061] | 0.040 [0.022, 0.060] |

</details>

| 51-week annual blocks | Observed | R3 S2 | Astra S2 | R3 S3 | Astra S3 |
|---|---|---|---|---|---|
| Top-10 turnover | 0.300 | 0.300 [0.195, 0.500] | 0.250 [0.100, 0.400] | 0.300 [0.100, 0.500] | 0.300 [0.100, 0.405] |
| K/4 turnover | 0.151 | 0.186 [0.177, 0.197] | 0.186 [0.173, 0.197] | 0.205 [0.194, 0.216] | 0.175 [0.165, 0.186] |
| 2021 head share | 0.426 | 0.496 [0.464, 0.545] | 0.492 [0.468, 0.549] | 0.467 [0.441, 0.495] | 0.507 [0.469, 0.536] |
| 2022 head share | 0.357 | 0.504 [0.459, 0.594] | 0.502 [0.454, 0.580] | 0.462 [0.416, 0.518] | 0.528 [0.457, 0.653] |
| 2021 rank-1 span | 8.367 | 8.979 [8.545, 9.599] | 8.948 [8.532, 9.590] | 8.912 [8.518, 9.189] | 9.189 [8.766, 9.594] |
| 2022 rank-1 span | 7.053 | 8.853 [8.136, 9.848] | 8.829 [8.103, 9.792] | 8.584 [7.949, 9.306] | 9.349 [8.544, 10.477] |

### Attribution within S3

The off-diagonal runs apply each change on its own. The identical evaluator uses its two output slots for the filter-only and sign-only S3 variants, respectively. The separate ablation archive gives all proper scores by horizon and both Reddit forward tests. Unconditional structure depends only on the fitted process.

| Panel | Round 3 S3 | Filter only | Sign only | Both |
|---|---|---|---|---|
| Facebook | 0.108 ± 0.023 · 60% · 4/5 | 0.091 ± 0.016 · 80% · 5/5 | 0.103 ± 0.029 · 60% · 4/5 | 0.078 ± 0.029 · 80% · 5/5 |
| Instagram apparatus | 0.360 ± 0.276 · 80% · 4/5 | 0.304 ± 0.230 · 80% · 4/5 | 0.360 ± 0.276 · 80% · 4/5 | 0.304 ± 0.230 · 80% · 4/5 |
| Reddit Comments | 0.098 ± 0.033 · 100% · 5/5 | 0.124 ± 0.057 · 100% · 4/5 | 0.090 ± 0.040 · 100% · 5/5 | 0.118 ± 0.064 · 100% · 4/5 |
| Reddit Submissions | 0.310 ± 0.125 · 20% · 0/5 | 0.198 ± 0.079 · 100% · 3/5 | 0.303 ± 0.055 · 0% · 0/5 | 0.214 ± 0.078 · 60% · 2/5 |

<details><summary>All horizons for the four S3 combinations · activity CRPS / coverage / head shift MAE × 10⁶</summary>

| Panel | Week | Round 3 S3 | Filter only | Sign only | Both |
|---|---|---|---|---|---|
| Facebook | 1 | 0.3013 / 0.917 / 592.11 | 0.2994 / 0.904 / 577.51 | 0.3010 / 0.912 / 594.07 | 0.2999 / 0.895 / 580.62 |
| Facebook | 4 | 0.3439 / 0.920 / 684.16 | 0.3424 / 0.906 / 656.99 | 0.3439 / 0.916 / 676.26 | 0.3432 / 0.901 / 655.30 |
| Facebook | 13 | 0.4311 / 0.912 / 861.13 | 0.4295 / 0.901 / 844.61 | 0.4309 / 0.912 / 836.09 | 0.4300 / 0.900 / 821.94 |
| Facebook | 26 | 0.5144 / 0.908 / 964.67 | 0.5131 / 0.897 / 948.36 | 0.5137 / 0.907 / 941.36 | 0.5121 / 0.896 / 937.92 |
| Instagram apparatus | 1 | 0.4708 / 0.900 / 354.14 | 0.4719 / 0.893 / 351.15 | 0.4708 / 0.900 / 354.14 | 0.4719 / 0.893 / 351.15 |
| Instagram apparatus | 4 | 0.5114 / 0.897 / 371.36 | 0.5120 / 0.891 / 368.35 | 0.5114 / 0.897 / 371.36 | 0.5120 / 0.891 / 368.35 |
| Instagram apparatus | 13 | 0.5862 / 0.897 / 408.72 | 0.5876 / 0.888 / 410.36 | 0.5862 / 0.897 / 408.72 | 0.5876 / 0.888 / 410.36 |
| Reddit Comments | 1 | 0.2128 / 0.932 / 182.97 | 0.2129 / 0.934 / 182.05 | 0.2120 / 0.892 / 169.18 | 0.2116 / 0.910 / 168.15 |
| Reddit Comments | 4 | 0.2580 / 0.927 / 244.51 | 0.2581 / 0.929 / 241.43 | 0.2558 / 0.912 / 236.55 | 0.2569 / 0.925 / 234.25 |
| Reddit Comments | 13 | 0.3236 / 0.927 / 312.82 | 0.3243 / 0.927 / 312.04 | 0.3210 / 0.923 / 305.85 | 0.3240 / 0.927 / 304.57 |
| Reddit Comments | 26 | 0.3929 / 0.922 / 363.42 | 0.3936 / 0.923 / 368.16 | 0.3898 / 0.917 / 347.42 | 0.3923 / 0.921 / 352.15 |
| Reddit Comments | 52 | 0.4965 / 0.917 / 436.61 | 0.4971 / 0.914 / 440.76 | 0.4938 / 0.912 / 430.43 | 0.4960 / 0.915 / 441.83 |
| Reddit Submissions | 1 | 0.1334 / 0.965 / 249.07 | 0.1294 / 0.952 / 236.19 | 0.1270 / 0.936 / 212.53 | 0.1246 / 0.911 / 203.35 |
| Reddit Submissions | 4 | 0.1662 / 0.960 / 318.68 | 0.1617 / 0.946 / 306.33 | 0.1616 / 0.942 / 286.10 | 0.1582 / 0.919 / 277.75 |
| Reddit Submissions | 13 | 0.2225 / 0.953 / 502.69 | 0.2184 / 0.941 / 482.75 | 0.2205 / 0.945 / 479.10 | 0.2165 / 0.925 / 475.52 |
| Reddit Submissions | 26 | 0.2826 / 0.947 / 623.92 | 0.2800 / 0.936 / 613.96 | 0.2812 / 0.941 / 621.09 | 0.2784 / 0.921 / 619.34 |
| Reddit Submissions | 52 | 0.3814 / 0.948 / 838.68 | 0.3784 / 0.941 / 837.77 | 0.3794 / 0.940 / 929.23 | 0.3765 / 0.926 / 920.43 |

</details>

## 6. Verification and limitations

| Prediction | Verdict |
|---|---|
| Submissions median displacement falls; activity calibration and CRPS improve | **Met in direction.** S2 improves all five activity and log-rank CRPS cells, raises PIT SD at all five, and brings coverage 1.0–2.8 percentage points closer to .900. It still misses the historical gate and remains overdispersed. |
| Preserve S2 in-sample structure | **Met exactly.** All four complete structure dictionaries, including their seed bands, are identical to Round 3. |
| Preserve cross-platform endpoint accuracy | **Partial.** Both candidates beat both deterministic activity baselines on fair X CRPS and both log-rank baselines on fair logR CRPS in 17/17 cells. S2 fair head Brier wins both baselines in 17/17 and S3 in 16/17. Against Round 3, IG loses activity and rank CRPS at every horizon. Aggregate S2 PIT SD moves closer to its target in 17/17 cells, while interval coverage does so in 13/17. |
| Reduce S3 head and boundary overshoot | **Partial.** Facebook and Comments top-10 turnover and boundary errors shrink. Final head shares on the two Reddit panels increase to .671 and .607, further from measured .506 and .357. The Submissions S3 size-law slope also reaches its inherited upper bound β = .50. |
| Retain Reddit asymmetry and forward head stability | **Failed.** The final two Comments decile predictions change from +.142, −.025 to +.074, +.128. The final two Submissions predictions change from +.122, −.056 to +.090, +.135. Both empirical curves turn downward. Forward Submissions D reverses sign under both date definitions. |
| Maximum simplicity | **Met on scalar budget.** No new fitted scalar. Robust quantile integration and a data-dependent decision make the estimator more elaborate. The study establishes a trade-off and leaves the global parsimony frontier open. |

The registered interpreter was Python 3.11.6 with pandas 2.2.2, NumPy 2.0.1 and SciPy 1.14.0. The baseline passed 14/14 supplied tests. The candidate passed 16/16, including planted positive-wave recovery, the independent wrong-sign check, a common-time-trend invariant, amplitude recovery with an additive noise floor, short-history fallback and contamination resistance. The repeated synthetic comparison used 24 panels per mechanism and length for surge direction and 12 seeds per length for endpoint scale.

The exact gate scoring functions and gold evaluator were imported unmodified. The Gaussian reference gave coverage .89855 and PIT SD .28951. Validation checked all five origins, deterministic comparator values, empirical gate moments, exact S2 structure identity, finite primary forecast fields and both calendar archives. The repository-wide health script was excluded because its legacy guard reads another panel outside the specified cache. The local suite and benchmark logs cover the tested candidate. [Archive checks](verification.log) · [Final pytest run](final_tests.log)

Gate coverage is a pooled median-consistency statistic. The conditional activity interval uses the second and 38th order statistics of 39 continuous members. Its exchangeable forecast reference is 36/40. Shared seeds reduce simulation noise but do not establish an independent test. Full-era buffer membership and the full-era Facebook Spec-B floor are inherited limitations of the authorized cache. Both Reddit transport designs condition on 104 training weeks and simulate forward without refitting to later values. The mandatory full-cache test starts on November 23, 2020 for Comments and November 30, 2020 for Submissions. A separate exact-calendar test starts on January 4, 2021 for both, runs through December 19, 2022 and uses January 7, 2019 through December 28, 2020 as training. Dates refer to week-start labels. The complete-block rule retains only the final 52 weeks in the 103-week test for the frozen annual ladder. Matched 51-week calendar blocks supply the additional year-to-year turnover test. Buffer membership was selected using the longer period. The Facebook estimand is tracked activity and the Instagram estimand is a-matching activity. All four panels supply DEVELOPMENT evidence.

The seeded worlds supply bands for head statistics. Entity-cluster bands condition on the observed calendar. The three-way departure split inherits right truncation near the evaluation endpoint. The full gold output uses finite conditional draws and a survival-clone approximation for departed slots. Its report of the analytical Kalman point omits future balancing drift and the future stochastic denominator. The cache allows exact comparison with Round 3, while rebuilding the universe inside each origin requires inputs outside this brief.

The individual results cover 1, 4, 13, 26 and 52 weeks where the panel permits them. Intermediate unscored weeks have no separate performance estimate. The two deterministic activity baselines have CRPS equal to absolute error. A stochastic EWMA forecast distribution was outside the frozen comparison.

Reproduction commands, data contact, input hashes, and run outcomes are in [the experiment log](EXPERIMENT_LOG.md). The [candidate](common_rankdiff_astra.py), [tests](test_common_rankdiff_astra.py), [frozen gate archive](candidate_frozen_gate_results.json), [gold archive](candidate_gold_benchmark_results.json) and [factorial attribution archive](diag_ablation.json) are local to this run.
