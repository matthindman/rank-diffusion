# V3 design — supplementary, retrospective, development data
Date: 2026-10-12. Author: OpenAI Codex. Base: 2ae195f806c299caedc4f86355fd08187979f97c.
No V3 study or test has run before this design. Existing V1/V2 findings and the A6 schema/source were read.

## Aim and fixed design
Measure endpoint-specific forecast quality from the four frozen movement stacks, with the original five rolling origins, 20 simulation members (seeds 0–19), and 2,000 gate bootstrap draws. DEVELOPMENT panels only: FB Era A (weekly plus its daily floor), original comments, short submissions, long submissions. No new specification, re-freeze, or adoption decision. Preserve original gate outputs and log every actual call.

Read-only wrappers under validation/ add restartable per-seed caches and training-only baseline exports. Gate calls and long jobs use heavy_lock and caffeinate. Full input/source/parameter hashes fence caches; all interrupted attempts remain logged. The two model files stay untouched.

## Outcomes and estimands
At nominal h=1,4,13, coarsen rank zero or rank>K to the explicitly declared category K+1, then score log(rank). This measures the coarsened distribution, not a full rank law or a death process. Outside within h means any future week 1..h is absent or below top K, including returners; it does not include the observed origin week. Simulated zero is absorbing, observed zero need not be. No rank-scale rescaling.

Fair ensemble CRPS subtracts the off-diagonal U-statistic pair-distance term; fair Brier subtracts p(1-p)/(M-1). These estimate the underlying predictive-law scores for iid members conditional on fixed fitted parameters/states, not independent refits. No-change is an exact degenerate distribution. Randomized ensemble rank-PIT indicators integrate ties and finite M exactly; report raw quantile brackets too. Stratify by absence-penalized training permanent rank: head<=.1K, middle(.1,.5K], shell(.5K,K], tail>K, all, established(pr<=K/2,presence>=.7), established and origin-inside.

## Training-only baselines
1. No change: repeat origin state, including outside persistence.
2. Gate historical absolute-displacement distribution at each horizon, with equiprobable sign, anchored at each endpoint's observed origin rank, clamped at rank 1. Origin absent stays outside. This is a survivor-marginal lift, not an estimated signed law or joint path: terminal CRPS/calibration only, no fabricated event Brier.
3. Direct-horizon rank-bin kernel: bins absent,(0,.02K],(.02,.1K],(.1,.5K],(.5,K],(K,2K],>2K. Sample full h-step training snippets in the same origin bin, transfer signed rank changes to the endpoint's exact current rank, preserve absences, recompute all crossings after anchoring. Absent origins use the empirical joint terminal-state/any-outside law. Empty bins fall back to no-change and are counted. Insufficient h+1 training weeks means unavailable, never test-data borrowing. Positive training ranks are unique within week; ordering by week and numeric starting rank ensures relabel invariance. This is a direct-horizon comparator, not a coherent cross-horizon process. Origin rank is used only in these requested baselines; estimators/cohorts remain permanent-rank based.

M=20 independent members per case for every baseline. Uniforms indexed by numeric origin rank, not identity labels, preserve relabel invariance; dependence across cases/origins is retained in uncertainty. Baselines have their own fixed seed 2026101203+101h.

## Uncertainty
Compare identical cases and common origins, equal origin weight. Positive improvement=baseline minus model; relative skill only when baseline score>0. Primary calendar block length13, sensitivity26, 2,000 circular-block resamples of whole origin bundles, preserving all endpoints, methods and horizons together. Withhold intervals if fewer than three origins or origin calendar span<2 blocks. These sparse, overlapping-origin intervals are descriptive, conditional on the fits and observed panel; they are not calibrated future-population confidence statements and do not cover refitting, selection or Monte Carlo uncertainty. Record empty draws; wrap-around adjacency is artificial. Simulation MCSE is a separate whole-member jackknife, deleting the same member across all cases/origins; head summaries use all20 members and both forms of uncertainty where available.

## Preflight timing audit
Source inspection suggests sim_cohort_conditional updates at t=0 and skips that row, so nominal h1 may contain two transitions from the filtered state. Before retrospective scoring, run a deterministic three-endpoint fixture: initial levels 3,2,1, lasting innovations -.75,0,0 each step, all other noise/exit zero. One-step ranks should be1,2,3; two-step ranks2,1,3. If confirmed, commit QUESTIONS for Claude, preserve frozen operational exports for a clearly labelled nominal-row table, and withhold a calendar-aligned scientific skill verdict pending an approved hook. Do not fix the protected simulator or silently relabel.

## Predictions fixed before running
P1: known-answer tests recover exact fair-score expectations and randomized PIT nominality to1e-12; true endpoint law beats the blind pooled law; relabels leave scores and baselines identical.
P2: wrapper reproduces frozen conditional primitive outputs and cached replay exactly on a synthetic fixture; flag-off model files remain unchanged.
P3: timing fixture will expose two lasting transitions at nominal h1 (ranks2,1,3); failure would overturn the inspection concern.
P4: operational frozen model beats no-change in all-cohort CRPS on at least3/4 panels at h4 (positive equal-origin improvement).
P5: operational frozen model beats the historical-mobility lift in all-cohort CRPS on at least3/4 panels at h4.
P6: model advantage over anchored rank-bin baseline will be smaller than over no-change on at least3/4 panels at h4.
P7: established-cohort outside-event forecast underpredicts observed frequency at h13 on both long Reddit panels by at least.02. This needs explicit event-probability and event-frequency summaries.
P8: short-panel week-block intervals are withheld, not invented.
P4–P7 are descriptive operational predictions, not acceptance rules. If timing is confirmed they do not establish correctly aligned endpoint skill.

## Sources
Ferro (2014), fair ensemble scores, DOI10.1002/qj.2270, https://empslocal.ex.ac.uk/people/staff/ferro/Publications/ferro2013.pdf.
Gneiting & Raftery (2007), proper scores, https://sites.stat.washington.edu/raftery/Research/PDF/Gneiting2007jasa.pdf.
Full grid, failures and missing cells are reported; no score tuning or seed expansion.

## Presentation clarification, before first score analysis
The already declared whole-member jackknife MCSE will also be printed as estimate±1.96MCSE in mc95 columns, explicitly a normal-approximation conditional simulation-error band, to accompany all head rows even where calendar-block intervals are withheld. This changes no scores, comparisons, decision thresholds or predictions. The simulation draws have begun but no endpoint-score table has been inspected.
