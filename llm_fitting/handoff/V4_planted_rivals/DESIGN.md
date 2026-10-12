# V4 — planted rivals and held-out slope recovery

2026-10-13 · OpenAI Codex · Exploratory synthetic validation; no change
adopted. Written before generation. Base main `bbc8645`; M1 statistics and
protocol v2 are unchanged from `223f5e2`. The M1 output remains blind until
this design, classification rule, and recovery map are committed.

## Aim and fixed grid

Test whether the balance statistics distinguish processes that require
different inputs to predict a ladder, and whether the proposed map actually
predicts an unused ladder. An accounting identity is not identification.

Three profiles: Facebook (K=3500,T=86,block=26), comments
(K=12500,T=213,block=52), submissions (K=5000,T=212,block=52).
The corresponding V2 frozen structure parameters are used; comments uses
the 136-week fit transported to 213 weeks, not an extension refit. Each
panel has 8K slots; replacement uses 16K, with genuinely new identities.
All present slots are ranked before any top-4K tracking. No cut of the long
frame before `mts_step1.analyse(df,K,block)` with all its defaults.

Ten rivals, 20 paired evaluation seeds 1000–1019 each, all three profiles:
600 analysis cells. Another 20 independent seeds 2000–2019 per rival/profile
are calibration cells for bin-mean coverage only (no M1 analysis needed).
Every path also has one extra block after T, withheld from every predictor,
cohort, tracked-set selection and classifier. No adaptive seeds, strengths,
thresholds, early success stop, or retuning after results. Failed cells stay
in the denominator. Restart only incomplete cells with identical hashes.

## Calibration fixed before simulation

Let H be the declared block length. Use V2's `w0` ladder, extended by its
existing `extend_homes` log-rank tail rule. Preserve coefficients at their
original absolute rank when increasing N (shift the log-fraction knots).
Set S0 to the median local slope over K/16,K/8,K/4,K/2,K on that ladder.
Set a to the median across the same rank band of
`2 sigma_perm^2 (1-(1-kappa)^H)/(kappa(2-kappa) H)`.
These are declared marginal moments, never diagnostic-score targets.

Each rival begins with the same ladder (except plain OU below) and targets
the same H-week lasting increment variance aH. This does not guarantee
that their later ladders match. Measure the resulting block slope ratios
and variance ratios; report mismatch rather than selecting seeds or
recalibrating. A pair with median slope or H-week latent variance differing
by more than a factor 1.5 is flagged MARGINAL MISMATCH: classification of
that pair cannot demonstrate separation at matched marginals.

Passing layers use V2's phi, sigma_trans, phi2, sigma_trans2, sigma_obs,
Student-t degrees of freedom and endpoint amplitude distribution, assigned
by the endpoint's initial permanent rank and retained for its life.
Stationary passing states receive 400 warm-up steps, independent of the
lasting dynamics. Reddit additionally gets a random-phase annual sinusoid
with peak amplitude 0.5 log units times the endpoint amplitude (the smaller
declared V2 stress level, not a new empirical estimate). The observation
mask is IID at `1 - rows/(identities*T)` from the saved V2 universe.
This is a measured aggregate absence rate but not a model of its temporal
pattern; missingness and the transported comments fit limit scope.
The recorded frozen common-factor AR layer is included. Innovations and
observation masks use separate reproducible streams; comparisons share
draws where their populations and layer definitions agree.

The long schema uses X directly (finite log-level), without exponentiation
or clipping, as accepted by M1. This avoids introducing a hidden reflecting
floor in a nominal random walk. A common additive shift is immaterial to
M1; this is a log-level experiment, not a count or daily-intake experiment.

## Generating processes and slope-relevant classes

| rival | lasting dynamics | class |
|---|---|---|
| home | V2 independent simulator's personal-home Euler OU, current latent-rank coefficient profiles, frozen replacement; nuisance overlays above | named stationary level |
| fixed_walk | independent walks, sigma fixed at initial permanent rank and scaled once so median H-variance is aH; no personal restoring term, no replacement | common slide |
| replacement | constant-variance Brownian growth, births at the bottom, random death; variance a, hazard delta=a/S0², growth mu=delta*S0-a/(2*S0); N=16K | flow |
| rank_drift | variance a; downward drift g=a/(2S0) above bottom tenth, compensating upward drift in bottom tenth; no personal home | position |
| unstabilized | walk with current latent-rank sigma profile, scaled by the same moment rule as fixed_walk, no drift, no replacement | common slide |
| equal_components | fixed offsets plus independent slow and fast AR states, each stationary variance V=aH/[2(1-rho_s^H)]; rho_s=1-median frozen kappa, rho_f=median frozen phi | named stationary level |
| position_ou | one common OU centre, no personal offsets; Gaussian ladder matched to S0 over the declared ranks; rho chosen by 2 V_L (1-rho^H)=aH | position |
| rw_medium | fixed_walk plus a stationary component with frozen median phi2, stationary variance equal to median frozen passing variance | common slide |
| rw_persistent | fixed_walk plus same variance at rho=.98 (R4 counterexample) | common slide |
| fixed_static | no lasting movement, same fixed offsets and nuisances | nuisance only |

The last is deliberately unmatched in lasting variance: it directly
measures passing-swing contamination. Plain OU is infeasible if aH>=2V_L;
report that cell, do not change rho. Replacement's tail calibration is the
continuous-time Brownian renewal relation; weekly discretization and finite
reservoir error are measured, not assumed away. No burn-in tuned to results.
The home arm is the frozen dynamics with the brief's extra reservoir and
observation nuisances; it is not a new production default.

Equal-component relabelling uses exactly the same observed array and RNG
draws, merely swapping the metadata names. Verify the full analysis input
hash and statistic output are identical; it is one class and one seed,
not two independent cases. Matching stationary variance does not make
the two components have equal finite-horizon variance.

## Statistics and classification fixed now

Save full M1 output and `mts_step1.score` for each evaluation cell, including
all boundaries, weekly horizons, block pairs, missing/uncertified cells,
cohort means, spreads, two carrier slopes, predictable shares, and every
score and screen. Tabulate each numeric leaf by rival/profile, with valid
count, mean, seed SD, 5–95% seed quantiles and MCSE. Compute B8 using the
three paired profile seeds and M1 report's exact rule.

Primary classification is leave-one-seed-out nearest centroid, frozen here:
hold out the same seed across ALL rivals within a profile; train on the
other 19 seeds per rival. Features are ONLY the primary centre-K M1
statistics: primary cohort slide, SD across its seven bin means, carrier
pos/home, mean predictable share at K/4,K/2,K, block mean g/r/f/shape and
R/small-step over KR, no-gap/gap slide ratio, weekly mean g/r/f/shape and
R/small-step over KR at every available horizon. No N, identities, planted
parameters, marginal slope, truth, or held-out block enters classification.
Standardize each feature by pooled training SD; drop zero-SD/all-missing
features. Impute missing with pooled training mean and add one missingness
indicator per feature with unit scale. Squared Euclidean distance to rival
centroids; ties resolved by the table order above. Map the named rival to
its declared class. Also publish rival-level confusion, so class pooling
cannot hide misidentification. Report per-cell missing feature fractions.

All confusion rates carry denominator, MCSE and Wilson 95% intervals.
Because training sets overlap these intervals describe marginal binomial
precision and do not incorporate uncertainty in the fitted classifier.
The worst-case 20-seed Wilson half-width is about .20; target <=.21.
Useful class recovery requires lower Wilson bound >=.80 and upper bound
on false named-level identification <=.20 in EACH applicable rival/profile,
and matched marginals for the pair being interpreted. No mechanism claim
is rescued by a favorable single statistic or profile.

Additional direct false-sign summaries: carrier pos upper endpoint<0;
joint pos upper<0 and home lower>0; and B3 FAIL. These quantify restoring
associations and bin differences under walks without treating them as a
valid mechanistic test. Home detection must be read against all no-home
rivals, not merely against zero carrier slopes.

## Held-out slopes

Use the primary M1 cohort slide s (seven octave bins, survivor weights) and
the primary diffusion-band variance divided by its weeks; predict
`S_hat(k)=v(k)/(2s)` at K/16,K/8,K/4,K/2,K. This function receives only
changes, ranks, counts and time; it must not read the diffusion S or R_diff
fields. Freeze predictions before generating/accessing the extra block's
truth. The truth is each local regression slope of that complete additional
block's observed ladder (>=90% presence); also report its latent ladder
slope, using the same observation mask. The target is the next block,
not the block used in v. A nonpositive s or uncertified input is a failed
prediction, not dropped from accuracy. Missing truth is NOT ADJUDICATED.

Success per seed: all five absolute relative observed-slope errors <=25%.
Recovery for a rival/profile requires Wilson lower bound >=.80, all 20
seeds attempted. Publish signed errors, median and maximum absolute error,
seed bands, valid/invalid counts, and latent-target errors. Also report
performance conditional on screen PASS, with its smaller denominator.
Step 3 is not identified across a class unless these conditions hold for
every required rival in it. The fixed-static arm is a negative control,
not a candidate class required to pass slope recovery.

## Predictable-share coverage and passing-only loss

The supplied 90% interval conditions on realised centre, occupancy and R.
It is not a forecast interval. Define the coverage target explicitly:
for each selection bin, the population survivor-conditional mean ABSOLUTE
block change under that generating process, minus the evaluation seed's
realised centre change. Weight by that seed's `bin_counts` and divide by
its realised R. Estimate the population absolute bin means from the 20
independent calibration seeds, pooling endpoint sums/counts; use a seed
jackknife for their joint Monte Carlo covariance. The weights and centre
are not re-estimated in this calculation. This tests calibration for the
bin-population target with plug-in realised weights; it does not prove
coverage conditional on every possible endogenous centre/occupancy/R.

Publish point coverage plus definite/possible coverage using a 95% Monte
Carlo band on the target. If that target band exceeds 10% of the reported
interval width, precise coverage is NOT ADJUDICATED. Nominal target is
90%; Wilson intervals and all eligibility denominators accompany rates.
Do not silently call containment of the sample mean 'coverage'.

Save the nuisance and lasting contributions to each observed block mean.
Using the exact observed occupant sets and centre occupant sets, decompose
G linearly into nuisance and lasting contributions, and check their sum
equals G. Report nuisance G/(k*weeks), its fraction of G when G>0, and
the bin-weighted predictable nuisance loss divided by R. This answers how
much occupant loss is passing swings retained by a block mean. It avoids
re-ranking a counterfactual nuisance-only panel for that decomposition.
The separate fixed-static rival is the pure-noise negative control.

## Predictions and stop rules

| id | fixed prediction |
|---|---|
| P1 | Every computed accounting cell closes below 1e-9 per endpoint-week; relabelling equal components is bit-identical for all 60 evaluation seeds. |
| P2 | At least one no-home rival has the joint restoring carrier sign in >=20% of seeds on at least one Reddit profile. |
| P3 | Named-level vs all no-home class recovery fails the .80/.20 Wilson requirement on at least one profile; Facebook is expected weakest. |
| P4 | Replacement vs position class recovery has point accuracy >=80% on at least two profiles; marginal mismatch flags qualify this result. |
| P5 | Fixed-static has positive block nuisance occupant loss at K/4 in >=80% of seeds on every profile. |
| P6 | At least one Reddit rival's median 52-week R/small-step falls outside .8–1.2 at at least one KR cell. |
| P7 | The map fails the declared held-out slope-recovery criterion for at least one no-home rival; an exact identity does not suffice. |
| P8 | At least one rival/profile has coverage below .80 or precise coverage NOT ADJUDICATED, exposing the interval's limited conditioning/precision. |

No gate evaluation, adoption, sealed contact or production-model edits.
Run the full grid once through a restartable launcher, one cell at a time,
under caffeinate and the shared heavy lock. Each completed cell is atomic
with source and input hashes. A whole-grid report requires every expected
cell to have a terminal status; it must never describe a partial grid as a
completed study. New execution fixes preserve failed attempts and do not
alter the above scientific choices. Resource failure is reported openly.

## Pre-generation implementation correction — 2026-10-13

No V4 panel or fixture has yet been generated. The home benchmark uses the
V2 independent simulator's exact kernel, including current-latent-rank
passing coefficients, replacement amplitude resets, and its 40-step burn.
The general 400-step passing-only warm-up above applies to the other
rivals. Only the reservoir extension, IID observation mask, and Reddit
annual overlay change the home arm. This corrects an ambiguity in the
initial design: assigning fixed passing coefficients in that benchmark
would no longer be the frozen model itself. A V4-local copy adds two
read-only latent/amplitude output arrays; equality of every original
output with `planted_sim.simulate` is tested. No production or V2 file is
edited. The additional held-out block is obtained by deterministic replay
from the seed only AFTER the forecast is saved, with prefix equality
tested. No threshold, seed count, class, prediction, or score changes.

## Pre-study nuisance correction — 2026-10-13

Only small artificial code fixtures have run; no V4 study panel, calibration
seed, native absence profile, or empirical M1 outcome has been opened or
generated. The overall absence-rate shortcut is replaced before the study.
An overall rate applied to every endpoint would penalize the head's
permanent ranks by roughly N times that rate and could empty its cohort
bins for an artificial reason. That does not implement the brief's real
nuisance requirement. The earlier overall rates remain in the input
manifest for provenance and are not used by native cells.

Use the native registered DEVELOPMENT panels at their declared T to measure
only the absence probability in fixed permanent-rank bins: edges 0 and
K*2^j for j=-8,...,3. Permanent rank includes each absent week's N+1 floor.
Within each populated bin retain mean absence and median permanent rank;
interpolate probability linearly in log permanent rank, with constant
extrapolation beyond the measured knots. Each simulated identity retains
the probability at its initial rank; a new birth uses the reservoir's
bottom rank. Weeks remain independently missing conditional on that
probability. This reproduces rank variation, not temporal clustering or
birth/death selection. No X change, balance, M1 score, ladder or residual
is retained from this preparation. The sealed-data guard protects every
input; a missing T9 drive stops preparation. The profiles, loader/producer
hashes and contact rows must accompany the result. Thresholds, classes,
seed counts, stopping and all eight predictions remain fixed.
