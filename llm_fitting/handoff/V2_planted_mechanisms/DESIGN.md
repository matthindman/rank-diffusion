# V2 — Independent simulation, recovery, and planted mechanisms

2026-10-12 · OpenAI Codex · base 4ae73b43b852ac540cadd39808f27200885f1be7.
Exploratory, DEVELOPMENT inputs only. This design precedes every V2 study
run. No gate, adoption decision, or change to a protected model file.

## Aims and order

1. Implement the stated stochastic architecture independently, without
   importing any production simulation/cohort routine. Check a fixed-band
   Gaussian limit against its known covariance, then implement rank profiles.
2. Cross-check the independent and production simulators at the full frozen
   structural parameters. This check precedes recovery and alternatives.
   **Stop V2 and hand over if the declared baseline check fails.** Do not
   add seeds or change the comparison to obtain agreement.
3. Measure operational recovery through the actual universe and estimator.
4. Measure conditional discrimination of six planted alternatives, at two
   fixed strengths each, using the supplied residual statistics. Do not read
   Claude's A1/A2 empirical results before the part-4 results are committed.

## Data-generating mechanisms and inputs

Native combinations, not a factorial of every profile and length:

| Profile | K | Weeks |
|---|---:|---:|
| Facebook Era A | 3500 | 86 |
| Reddit comments | 12500 | 136 |
| Long submissions | 5000 | 212 |

Use the full-panel structure stack S4 of MODEL_STATUS §2z-ap and
`ladder_explainer.spec_for`: temperament, pooling 8, MD6, t fast shocks,
long-difference moments, stationary common level, medium AR layer and
permanent-share heterogeneity. This is the frozen structural stack, not
V1's movement stack. Reproduce its parameter extraction solely to obtain
full frozen parameter values; do not calculate a real-panel score or new
real-panel residual. Save every parameter, specification, input/source hash
and software version. No real-panel diagnostics from A1/A2 are inputs.

The independent simulator uses Euler OU updates around each endpoint's own
home, fast and medium AR layers, observation noise, the common level and
replacement identities. Homes start from the measured period-0 ladder;
there are 40 burn-in updates, matching the frozen simulator. Coefficients
are interpolated at the current **latent lasting-level rank**, as the coded
model does, not at noisy observed rank. This differs from the estimator's
time-mean observed-rank bands and is recorded as a source of operational
recovery error. Independent random streams separate layers; common seeds
couple alternatives. Replacement changes identity and resets the home and
transitory states. The independent code must contain no production
simulation imports or calls.

For the fixed-band Gaussian check, with a=1-kappa,
C(h)=sp²/[kappa(2-kappa)] a^h + st²/(1-phi²) phi^h + so² 1[h=0].
The unit check uses a long burn-in and controlled homogeneous coefficients.
Native studies retain the frozen 40-step initialization; equilibrium is not
assumed to have been reached at every slow rate.

## Baseline cross-check and its stopping rule

Exactly 20 production seeds 0–19 and 20 independent seeds 1000–1019 per
profile. Use the identical 19 macro statistics of §2z-ap (`MACRO_BLOCKS` and
`_stats`), track 4000 slots, recorded top-M=min(N,2000), and the same K and
1%-of-K top-set definition. Compare raw simulator arrays, with the frozen
statistic conventions. No real observations are scored.

Report both seed distributions, mean ± SD, 5–95% seed bands, mean difference
and its Monte Carlo SE. For the STOP decision, use 20,000 independent random
label permutations of the 40 whole-seed statistic vectors per profile,
RNG 2026101202. The two-sided statistic is the absolute difference in means;
p=(1+number of permuted statistics at least as extreme)/(20001), with ties
counted conservatively. Apply Bonferroni over all 57 comparisons at family
level .05. Report the corresponding permutation null band and adjusted p.
A rejection or an unavailable required statistic stops V2; all remaining
study stages are NOT RUN. No extra replication is triggered by a close
result. This is a discrepancy check, not proof of equivalence or a power
claim for rare differences. The finite randomization calculation follows
[Phipson and Smyth](https://gksmyth.github.io/pubs/PermPValuesPreprint.pdf).

## Recovery: estimands, methods, and performance

After a passed cross-check, use 20 independent panels per native profile,
seeds 1000–1019, plus 20 with true s=0 at the same seeds. Keep the frozen
background parameters; at s=0 the permanent/movement mixing amplitudes are
both one. Fit the same S4 estimator with its existing moments, grids and
NNLS convention; do not change the default or tune a moment weight.

Synthetic observations follow the actual weekly intake domain:
metric=max(expm1(X),0), retaining zero-valued rows as `load_panel` does;
recompute X=log1p(metric), rank with the existing metric/id tie convention,
N and z. Non-finite off observations are absent. Check this construction
against the real loader on a fixture. Then call the actual
`restrict_universe(..., buffer_mult=4)` and the actual estimator. Births are
separate identities. The registered absence penalty determines membership;
existing estimator band conventions remain unchanged. Record zero-floor
frequency, selected membership and weekly sample sizes. Numerical overflow
is a failed seed, not permission to cap a tail. Do not silently exclude a
failed or undefined estimate from a denominator.

Required estimands: s; kappa and its grid-edge fraction; sigma_perm,
sigma_trans, sigma_obs; t degrees of freedom; and V=sp²/(2 kappa). Evaluate
profiles on the generating z-knot grid and retain every knot. Also report
the exact discrete fixed-coefficient variance sp²/[kappa(2-kappa)], and
companion parameters for the medium component where helpful. Both variance
formulas are local parameter functionals in the rank-varying model; neither
is automatically the actual stationary variance of an endpoint in the
interacting, replacement process.

For each estimand report truth, mean, bias, SD, RMSE, quantiles, valid/failed
counts and MCSE of mean/bias (seed SD/sqrt(valid n)); use seed jackknife MCSE
for SD and RMSE. Retain knot-level results and summarize unweighted knots
by fixed head/middle/shell/tail regions, with denominators. Grid-edge and
NNLS-zero fractions are calculated within each seed then summarized across
seeds. At true s=0 report the frequency s_hat>1e-12, its binomial MCSE and
Wilson interval: this is a positive-estimate frequency, not a nominal
hypothesis-test false-positive rate. For infinite t_df report the atom
explicitly; raw mean bias is undefined if needed. Give finite conditional
summaries and the finite companion 1/(t_df-4), zero at infinity, separately.

These are conditional-on-frozen-parameters operational recovery studies.
They include ranking, selection, the observation floor and the coded
coefficient/band mapping. The weekly structural stack does not use a daily
Spec-B pin; daily aggregation and uncertainty of that external instrument
are not evaluated here. Native combinations confound profile and length,
so differences between rows are not an isolated causal effect of T.
The reporting structure follows
[Morris, White and Crowther](https://pmc.ncbi.nlm.nih.gov/articles/PMC6492164/).

## Fixed planted alternatives

Each alternative changes only the stated part, retaining the frozen
background specification. Strengths are not adjusted after any run.

| Mechanism | Low strength | High strength | Construction |
|---|---|---|---|
| Fat lasting shocks | Student-t df 8 | Student-t df 3 | Analytically unit-variance innovations replace Gaussian lasting innovations. |
| Reversible off state | onset .005/week | onset .02/week | Return hazard .125/week at both strengths. While off, rank=0 and X=NaN; latent dynamics and identity continue. Rebirth still replaces identity. No arbitrary depth parameter. |
| Slower memory | slow variance fraction .25 | slow variance fraction .50 | Two lasting OU components, rates kappa and kappa/10. Split the local discrete stationary variance, adjusting innovation scales analytically. |
| Bounded passing tail | clip at 8 unit SD | clip at 4 unit SD | Winsorize the baseline unit-variance fast t innovation, then normalize by its analytically integrated clipped variance. No sample-variance normalization. |
| Time-varying volatility | log-variance SD .7 | log-variance SD 1.4 | AR coefficient .95, stationary log-variance mean -SD²/2, so mean multiplier variance is one. Scale Gaussian lasting, passing, medium and observation innovations around the endpoint amplitude; common factor unchanged. |
| Random-walk lasting level | .5 times baseline sp | 1 times baseline sp | Set lasting reversion to zero; preserve the other layers. No stationary V exists for a nonzero random walk. |

The volatility alternative uses Gaussian shocks as required by the brief.
Add a **matched Gaussian fixed-volatility control**, so removing the frozen
fast t tail is not mistaken for the effect of changing volatility over
time. Compare volatility with both this control and the original baseline.

For baseline evaluation and every alternative/control use seeds 1000–1019
(20 each), paired across conditions. Add 39 independent baseline calibration
seeds 2000–2038, diagnostic-only. Native baseline paths may be regenerated
identically after the cross-check and reused for recovery and diagnostics;
this is shared data, not an independent replication. No seed count is
adapted after results.

## Diagnostics, false alarms, and separation

Use the committed `residual_anatomy.py` functions and hash the exact source.
For these statistics follow A1/A7's simulator-side observation convention:
raw log values, all slots tracked, and replacement slots split into identity
histories. This is distinct from the recovery intake transform above.

Include all numeric diagnostic outputs from tails_by_horizon (all three
standardizations), retention, departures/departure_summary, long_dispersion,
cohort_fate, momentum, volatility_persistence, amplitude_coherence and
far_tail. Counts are precision/availability fields, not mechanism detectors.
Use the functions' permanent-rank groups, presence rules and minimum counts.
Departures/fate use t0=floor(T/2), t1=T, cut=K/2 and a fixed absent-rank floor
N+1. This is a native-window analogue, not exact replication of A2's longer
extension/P7 calendar split. Momentum uses blocks 13 and 26 when T>=5b;
also the available gaps 1/2/3/4/6/8 at block13 and block52 gaps1/2 when T>=208.
Coherence uses h13/26 when T>=5h. Unavailable horizons/strata stay unavailable.

For each statistic, a baseline envelope is the min/max of its 39 calibration
seeds. Under exchangeability, the strict outside-envelope probability is at
most 2/40=.05 averaged over calibration sets (ties conservative). Evaluate
false alarms on the separate 20 baseline seeds and detection on each
alternative's 20 seeds. Report fractions, denominators, binomial MCSE and
Wilson intervals; 20 replications give coarse rate precision. These are
per-statistic envelopes, not familywise tests. Also report the baseline
fraction triggering any available statistic; do not call that a 5% test.
Calibration-set uncertainty is distinct from the conditional MCSE.

Report alternative-minus-baseline mean differences in baseline seed-SD
units and paired seed MCSE. For every condition pair and available statistic
report absolute mean difference divided by pooled seed SD and 5–95% seed
bands. Predeclared descriptive reading: **unresolved** if standardized
separation <1; **separated** only if it is >=2 and the seed bands do not
overlap; **overlapping** otherwise. Degenerate or missing spreads are
labelled, never made significant with an epsilon denominator. These labels
are not equivalence tests or evidence of unique mechanistic identification.
Save complete tables, including every strength pair, rather than a best case.

Use a small predeclared display set alongside the complete tables: head
anchor r9990 at h13; head big_down retention at k13; absence/lasting/repeat
exit rates; cohort share_time_outside; middle robust dispersion at h26;
middle block13 skip momentum; middle volatility ratio; head far-tail
z_q9999_up; middle h13 amplitude coherence. Missing display cells remain so.
All conclusions condition on the fixed nuisance parameters. No competing
composite model is refitted under each planted alternative; this is a
conditional diagnostic study, not complete nuisance-robust model recovery.

## Predictions, scored without omission

P1 All 57 baseline comparisons pass the declared simultaneous discrepancy
check. Failure stops all later stages.
P2 At least 25% of fitted kappa knots are at a search-grid edge, on average,
on at least two of the three native profiles.
P3 The median knot-wise relative RMSE of V is below that of kappa on at
least two profiles (use only strictly positive finite truths for this
comparison; report excluded counts).
P4 At true s=0, s_hat is positive in at least 25% of seeds on at least two
profiles. This predicts an estimation artifact, not a test rejection rate.
P5 High-strength off states exceed the baseline envelope for the absence
exit rate in at least 80% of seeds on at least two profiles.
P6 High-strength changing volatility lowers the middle volatility ratio
by at least two matched-control seed SDs on at least two profiles.
P7 High-strength fat lasting shocks raise the head anchor r9990 at h13 by
at least one baseline seed SD on at least two profiles.
P8 High-strength bounded fast shocks reduce head z_q9999_up by at least
20% relative to baseline on at least two profiles.
P9 At T=86 at least one slow-memory/random-walk strength pair remains
unresolved by middle block13 skip momentum under the declared rule.

## Execution and validation

Run every full study stage through heavy_lock.py and caffeinate, with atomic
per-seed checkpoints and source/parameter identities checked on restart.
Preserve failures and attempts. A partial or killed aggregate is not scored.
No change to model defaults, gates, canonical records, packages or paper
claims. Handoff STATUS_ENTRY and GATE_ROWS (zero evaluations) replace direct
record edits. Required tests cover fixed-band covariance, explicit
high-strength signatures in controlled cases, replacement identity, the
registered zero-floor/tie mapping, and reporting/statistical calculations.
The full repository health check passes before commits. Results are committed
separately from the pre-run design and evaluator code.

## Dated addendum — two additional cases from main, 2026-10-12

Read from main 2016d486, brief amendment fdc7508, after the original design
and evaluator commits. The amendment explicitly says to add a dated
addendum if DESIGN is already fixed. At this point the original 57 native
baseline checks have passed and recovery jobs have begun. No native planted
diagnostic output or recovery parameter value has been inspected; progress
logs contain only status/timing. A1/A2 results remain unopened.

The original experiments, strengths, seeds, predictions and stopping rule
stand. Add these cases, with 20 seeds 1000–1019 per native profile:

1. **Current-rank ladder attraction**, strengths .02 and .10 per week:
   mu <- mu + strength * (L[rank(mu)] - mu) + original lasting shock.
   L is the fixed extended period-0 ladder; ranks are among lasting levels
   before the update. There is no endpoint-home pull in this case.
   Replacements and all other layers retain their frozen rules.
2. **Annual cycle**, mean peak amplitudes .5 and 1.5 in log-activity units.
   Each endpoint receives A = mean_amplitude * exp(-.7²/2 + .7 Z), with
   Z standard normal, and an independent uniform phase on [0,2pi).
   Add A*sin(2pi*t/52 + phase) at observation, on top of the baseline.
   A and phase persist for the endpoint's life and are redrawn at rebirth.
   The cycle does not enter the latent rank used to index coefficients.
   Cycle variance is added; it is not compensated by removing baseline noise.

The extra cases use a separately versioned extension of our own independent
simulator, with a new independent cycle random stream. The original
simulator and running checkpoints are unchanged. The extension must be
bit-identical to the original for every original mechanism on controlled
fixtures; controlled tests must also detect both new mechanisms at high
strength. This preserves the original native simulator cross-check rather
than changing its implementation while recovery is running.

Run the same residual functions, envelopes, all-pair separation tables and
availability rules on the added cases. In addition, fit the S4 estimator to
all 20 seeds of both added cases and both already-declared random-walk
strengths. Compare fitted kappa profiles, per-seed group-median kappa and
grid-edge fractions with the original 20 baseline fits. The fitted home
reversion is a diagnostic under a misspecified fitted model; the
current-rank attraction coefficient is not an endpoint-home kappa truth.
No bias-to-.02/.10 recovery claim will be made for it. Retain all fitted
profiles and report the same seed-spread separation labels for fitted
kappa summaries, including every pair. The Gaussian, off, other tail and
slow-memory cases do not receive additional fits under this addendum.

Additional predictions (original P1–P9 unchanged):
- P10: high-strength current-rank attraction yields a mean per-seed median
  middle-group fitted kappa at least 25% below the baseline fit on at least
  two native profiles.
- P11: high-strength current-rank attraction is separated from high-strength
  pure random walk by middle-group robust dispersion at h26 on at least two
  profiles, using the original >=2 pooled-SD and disjoint-band rule.
- P12: for the high-amplitude cycle, middle block13 momentum slope at gap4
  exceeds that at gap2 by at least .5 on at least two profiles. This is the
  predeclared quarter-year/half-year/full-year signature.
- P13: the high-amplitude cycle raises the mean per-seed median middle
  fitted kappa by at least .005 above the baseline fitted value on at least
  two profiles.

All four predictions can fail. Native profile/length confounding remains;
a distinguishable fixed-parameter case is not nuisance-robust identification.
The added fits and diagnostics are exploratory and consume no movement gate.
