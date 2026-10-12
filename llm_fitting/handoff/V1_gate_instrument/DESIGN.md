# V1 — gate instrument study

2026-10-12 · OpenAI Codex · started at ed75e9f4f0a95b2f479f1001835410073b6ed0fe.
Exploratory instrument/reproduction study on DEVELOPMENT panels. No candidate,
new gate, or margin adopted. This design is committed before the first V1 run.

## Question and fixed design

Measure the repeatability, rep-count dependence, calibration contribution,
moment composition and conditional simulation resolution of frozen Gate v1.
Panels: facebook_a K3500; reddit_comments K12500; reddit K5000;
reddit_submissions_long K5000. Instagram is not run (optional in the brief).
Use the frozen movement stacks and their five automatic origins. First rerun
the recorded settings: 3 reps/400 draws on the first three and P6's
20 reps/2000 draws on long submissions, seeds starting at 0. Compare the
rounded mean, split SD, baseline and coverage against their recorded outputs.
STOP V1 and report if any reference does not reproduce; do not repair the
reference or proceed to a different experiment to obtain it.

After successful reproduction, one bank of 80 new seed trajectories (1000–1079)
per panel/split/scale in training and testing. Keep the full displacement
histogram for each horizon/seed, per-seed RACF and collision rates. Nine frozen
calibration scales, in their original tie-breaking order. Full distributions
are necessary: median of pooled displacements is not mean/median of per-rep
medians. Cache only exact deterministic inputs/results, never approximate the
simulator. Do not edit either protected model file.

Primary resampling: 2000 bootstrap ensembles, RNG 20261012, at reps
3, 5, 10, 20, 40, 80, sampling with replacement from the 80-seed bank. The
same replicate weights travel across all five splits, calibration scales,
and training/test simulations, preserving the original shared-seed structure.
At each draw reselect the train scale from the frozen calibration vector,
then score the corresponding conditional test forecasts. In parallel hold
the scale at its 80-seed selection to separate aggregation noise from scale
selection. Report the mean, SD and 2.5/97.5 percentiles, and actual disjoint
20/40-seed blocks as a finite-bank diagnostic. The bootstrap distribution is
conditional on this finite simulation bank and the fixed empirical panels;
it is not population/time-sampling uncertainty or the power of an unknown
candidate. Resample in two independent groups to estimate same-stack null
differences. No extra seeds will be added because of a close result.

Decompose the difference between E|M_n-e| and |E M_n-e|, which is the
absolute-value/Jensen contribution, separately from finite-sample bias in
the pooled median and from reselected calibration. Attribute error to each
included moment. Attribute variance using Cov(moment contribution,total)
so covariance is allocated and shares sum to one (shares can be negative).
Report MOM_FLOOR inclusion, including structurally unavailable horizons.

For existing ladder pairs use every adjacent gate pair on the four panels.
Quantify paired versus independent SPLIT dispersion from their stored five
vectors. If per-seed gate outputs were not saved, seed-CRN variance reduction
is unidentifiable from those cells: report that limit, do not rerun non-frozen
rungs. R0 and R1 on disk supply wrong-model controls; no new mutation is run.

Resolution table: empirical 95th percentile of |same-stack score difference|
at 20/40 reps, for each split and the five-split average. This is a simulation
noise floor, not a general candidate-detection threshold. Offer three rules
without choosing: fixed +0.05 convention; positive worsening greater than the
null noise floor; and +0.05 after adding that noise floor to the paired
point difference. Apply them descriptively to all adjacent ladder pairs,
keeping original coverage separate. These are illustrations, not adoption.

## Predictions to score, including failures

P1 Recorded settings reproduce rounded summary and coverage on all panels.
P2 Mean error at 3 minus 20 reps is positive: 0.01–0.05 FB,
0.01–0.06 comments, 0.01–0.06 short submissions, 0–0.06 long submissions.
P3 The fixed-scale absolute-value/Jensen contribution accounts for at least
half the 3-to-20 decline on each panel with a decline; calibration selection
may explain the remainder. No positive decline means this prediction fails.
P4 At 40 reps same-stack difference SD is at most 0.85 of that at 20 reps
on every panel (square-root benchmark about 0.71; medians may depart).
P5 Collision moments jointly account for over half of Monte Carlo variance
on FB and long submissions; coll1 is the largest single contributor there.
P6 R0 is rejected on every panel under model > baseline+0.05; R1 is also
rejected on every panel under this error rule. This uses already-run cells.

## Execution and stopping

Every empirical gate invocation is a reproduction entry in GATE_ROWS.md,
including failed or interrupted attempts. Derived bootstrap ensembles are
resummaries of the same bank, not new fitted evaluations; retain their seed
manifest. All heavy work uses heavy_lock.py and caffeinate; launch scripts
resume completed atomic seed files. A killed/incomplete invocation is not
interpreted: it is logged, and identical work resumes to a complete bank
before any inference. Model hashes, input hashes, software, seed manifests,
commands and completion markers are saved. No sealed folder or WD is used.

Known-answer tests cover exact pooled medians (including unequal counts),
MOM_FLOOR, known folded-normal absolute-deviation bias/noise, variance
allocation, calibration tie breaking and wrapper parity. Both suites and
legacy guard must pass before each commit.
