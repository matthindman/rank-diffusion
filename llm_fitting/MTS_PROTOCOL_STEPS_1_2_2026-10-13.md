# Movement to structure: measurement protocol for steps 1 and 2

_2026-10-13. Claude (Opus 5.5). First deliverable of the agenda
`RESEARCH_AGENDA_MOVEMENT_TO_STRUCTURE_2026-10-12.md`, which the owner adopted
on 2026-10-13 (MODEL_STATUS §2z-bp)._

**Status: v2, REVISED AFTER REVIEW. This is the version that runs.** Draft v1
(commit 1741db3) was reviewed by Codex under queue row R4
(`handoff/R4_mts_protocol/REPORT.md`): six findings, each with an executable
counterexample, all reproduced and all accepted. What changed and why is in
`handoff/R4_mts_protocol/ANSWERS.md`. The changes were made before any
statistic was computed on any panel. One round of review and one round of
fixes, as the coordination protocol provides; Codex has not re-reviewed v2.

DEVELOPMENT data only. No fit, no simulation of the frozen model, no gate
evaluation, no sealed data. Everything here is descriptive. It can say what
holds the ladder in an accounting sense. It cannot by itself establish a
mechanism, and after the review it does not try to read one from a
regression.

## 1. The question this step answers

The record has one relation between movement and structure: lasting variance
divided by twice the ladder's local slope is close to one constant across
ranks (§2z-bf, §2z-bj). The constant was read off the ladder, so nothing
independent measures it (§2z-bh). Step 1 measures it from named changes and
says, as far as the data can, what it is made of.

## 2. The accounting

Take the levels of all endpoints at two times. Let A be the k endpoints with
the highest level at the first time and A' at the second, and let ell be the
k-th highest level at the second time. Then, exactly,

    k × (change in the mean level of the top k positions)
        = N    the change of the endpoints in A that are still observed
        + R    reordering: by how much the endpoints that climbed into the top k
               exceed ell, plus by how much those that fell out lie below it
        + F    flow: entrants with no level at the first time, and exits with
               no level at the second.

R is never negative. Measured against a centre c, the mean level of the top
k_c positions, the left side becomes the change in the ladder's **shape** and

    shape change = −G + R + F,      G = what the occupants of the top k lose
                                        relative to the centre.

This holds for any process and any horizon.

**What R is and is not.** R is the endpoint reordering gain over the
interval. It keeps the names that held the top k at the start, so it nets out
everything that crossed and crossed back, and it is not additive over
sub-intervals. It is not the accumulated local time of the gap between ranks
k and k+1. Only for a short interval in a diffusion does its expectation
approach half of that, and then its small-step value is k v / (2S), with v the
variance of the changes near rank k and S the local slope. So:

| term | what it is | status |
|---|---|---|
| G / k | occupant loss per endpoint | exact, from named changes |
| R / k | endpoint reordering gain per endpoint | exact, from who replaced whom |
| v / (2S) | small-step value of R / k, the quantity of §2z-bf | an approximation whose error at each horizon is measured here (A2, B9) and must be validated on planted panels before it is inverted to predict a slope |

Every term is a finite-horizon quantity measured for the same endpoints over
the same interval (§2z-bn). No instantaneous variance is estimated. The
variance in v / (2S) is taken over endpoints chosen by their rank at the start
of the interval, so it is affected by selection on that time's noise; it is a
diagnostic of observed positions, not a structural variance by permanent
rank.

**The centre and the common slide.** The centre is positional: the mean level
of whoever holds the top k_c at each time. If no endpoint has any restoring
force and the ladder holds its shape because others climb in from below, every
occupant still loses ground against that centre, at one common rate. I call
that the **common slide**. The measurements below keep apart:

1. the slide of a cohort fixed beforehand: its mean change against the
   centre;
2. differences in that mean change between rank bins of the cohort;
3. how the change is associated with the cohort's earlier levels;
4. flow.

A cohort's slide can also contain the decay of whatever made its members rank
high on the selection block and has not yet died out. The design separates the
two by time: a slide persists at one rate; decay shrinks with distance from
the selection block (prediction B10).

## 3. Declarations

**Panels.** `facebook_a` (K = 3,500; 86 weeks), `reddit_comments_ext`
(K = 12,500; 213 weeks), `reddit_submissions_long` (K = 5,000; 212 weeks),
loaded with `minimal_rankdiff.load_panel`. Instagram is left out (censored).

**Level.** `log1p` of the weekly metric, as loaded. No common factor is
removed; the centre does that work.

**Population and tracking.** Positions are ranks among everything the
instrument observed that week. Endpoints ever inside the weekly top 4K are
tracked. Weekly ladders are exact down to 4K by construction. A block ladder
is exact only down to the rank whose level is strictly above the most an
untracked endpoint could have had in that block (the largest weekly level at
rank 4K + 1). The runner computes that certified rank per block and **fails
closed**: any pair whose centre is not certified in both blocks, any cell
whose k is not, and any small-step value whose rank band is not, is returned
as NOT ADJUDICATED and never as a number. Centres and cohort cutoffs at K/2, K
and 2K; K is primary.

The tracked set uses later weeks. Cohort members are unaffected, because a
permanent rank inside the cutoff on the selection block already implies
tracking. Everything else that is tracked is there because it reached the top
4K at some time, possibly later. That "outside" group is reported, is labelled
retrospective, and enters no predicted quantity.

**Blocks.** Whole blocks counted back from the end of the panel: 52 weeks on
the Reddit panels (four blocks), 26 weeks on Facebook (three blocks; this
drops the election weeks at the start of Era A). A block level is the mean
over the weeks an endpoint is present and is undefined below 90% presence. A
block mean is a filter, not a level: it averages an annual cycle out only when
coverage is complete, and it passes slow components through. Both limits are
handled by design (whole-year blocks, the presence rule, B10) and by step 2,
not assumed away.

**Cohorts and information.** A cohort is defined on one block only, by
absence-penalized permanent rank over that block with at least 90% presence,
in octave bins of rank down to the cutoff. Its changes are measured between
two later blocks.

**Which comparisons count.** For a cohort chosen on block 0 of four blocks:

| change | role |
|---|---|
| block 2 to block 3 | **primary**: one whole block lies between selection and the start |
| block 1 to block 2 | no gap: read against the primary (B10) |
| block 1 to block 3 | long, no gap: reported |

The cohort chosen on block 1 and measured from block 2 to 3 uses the same
outcome blocks as the primary. It is reported as a replicate cohort and is not
counted as a second result. Facebook has three blocks: one pair, block 1 to 2,
with no gap and no still-earlier block. Predictions that need a gap or an
earlier block are NOT ADJUDICATED on Facebook; they do not pass by default.

**Survivors.** A cohort bin's mean change is over members with a level at both
blocks. That is a survivor-conditional quantity. Members who leave the record
are counted as attrition, their changes are unobserved, and nothing corrects
for selection on survival. The predicted loss below weights exactly the
survivors that G also covers.

**Identifiers.** Facebook ids are page names; a rename reads as an exit and an
entry and lands in F. On Facebook absence is not behavior, so F there is a
property of the collection.

## 4. Part A — weekly positions

For horizons 1, 4, 13, 26 and 52 weeks (those under half the panel), origins
every half horizon, and k from the centre's k down by halving to 12: G, R, F,
the shape change, k v / (2S) with v and S from the ranks within a factor √2
of k, the step size √v / S, and the share of occupants replaced. Reported as
rates per endpoint per week: mean over origins, and the range over four
consecutive calendar segments.

The identity must close to rounding in every cell; that checks the code. The
comparison of R with k v / (2S) at one week is a measurement: how well the
small-step relation holds when both sides carry the passing swings.

## 5. Part B — block levels

For every counted pair:

- the same accounting on block levels;
- **cohort drift:** per bin, the survivors' mean and mean squared change
  against the centre, the attrition, and a 5–95% endpoint bootstrap (400
  draws);
- **occupancy:** the share of each cohort bin inside the top k at the start
  block;
- **the predictable share:** over the occupants of the top k at the start
  block that survive and belong to the cohort, minus the sum of the mean
  change of the bin each belongs to, divided by R. Its interval is the 5–95%
  range of that weighted sum over the same bootstrap draws for all bins. It is
  conditional on the realized centre, on who occupies the top k and on R. It
  is not the uncertainty of a future slope. The same sum is also reported as a
  share of those survivors' actual loss. Where R is not positive the share is
  not computed;
- **predictive associations:** for adjacent blocks, the regression of the
  change on the level one block before the start and, where a still earlier
  block exists, on that level too, all against the centre, within the cohort.

**The associations are not read as mechanisms.** Draft v1 gave a table of
signs (no force: zero, zero; position: negative, zero; own level: negative,
positive). The review showed it is false: a level with drift that depends on
position only gives −0.34 and +0.07 once it is averaged into 52-week blocks,
with no personal home anywhere, and a random walk plus a component with a
half-life of 34 weeks gives −0.25 and +0.23. Putting the predictors before the
interval removes selection on the interval's own noise and nothing more. What
a pattern of slopes means is decided in step 2, against planted rivals at
these block lengths, and a personal home is read only if its slope lies
outside what the rivals without one produce.

## 6. Predictions and how each is scored

Written before any run; B10 and the scoring rules were added in v2, also
before any run. Statuses: PASS, FAIL, INCONCLUSIVE, NOT ADJUDICATED. A
prediction of "no effect" passes only if its interval lies inside a stated
margin; an interval that merely includes zero is INCONCLUSIVE. The rules are
implemented in `mts_step1.score` and tested on synthetic panels. "KR" means k
from K/16 to K; the centre's own k is left out wherever the shape term is
scored, because the shape term is zero there by definition.

Part A (per panel, centre K)

- **A1** (code): the identity's residual is below 1e-9 per endpoint-week in
  every cell computed.
- **A2**: at one week R over k v / (2S) is between 0.6 and 1.2 at every k in
  KR. **A2b**: it is below 1 at every k below K/16.
- **A3**: at k = K/4 the occupants' loss per week at one week is at least five
  times the loss per week at the longest horizon run (§2z-bn's prediction).
- **A4**: at horizons up to 13 weeks the shape term is under a tenth of R at
  every k in KR.

Part B (per panel, primary pair, centre K)

- **B1**: the reordering gain per endpoint per week at K/16, K/8, K/4 and K/2
  is inside 0.0018–0.0045 on Reddit, 0.0045–0.010 on Facebook (a factor 1.5
  around the constants of §2z-bf).
- **B2** (Reddit; the independent measurement of the constant): each of the
  seven octave bins from (K/128, K/64] to (K/2, K] has a negative mean change
  with its 95% point below zero, and the pooled slide per week is between 0.5
  and 2 times the reordering gain per endpoint per week at K. FAIL if any
  bin's 5% point is above zero or the ratio is outside; otherwise
  INCONCLUSIVE.
- **B3**: little difference between bins. A bin passes if the interval of its
  mean minus the pooled mean lies within ±30% of the pooled mean, fails if it
  lies wholly outside. PASS with five of seven bins passing, FAIL with three
  failing, otherwise INCONCLUSIVE.
- **B4** (Reddit): the association with the still earlier level is small: its
  interval lies within ±0.10 per 52 weeks. FAIL if it lies wholly beyond.
  This scores a predictive association; see §5.
- **B5** (Reddit): the predictable share is between 0.5 and 1.2 at K/4, K/2
  and K. FAIL if any interval lies wholly outside; INCONCLUSIVE if a point is
  outside and its interval is not.
- **B6**: on Reddit |F| is under 15% of R at every k up to K/2. On Facebook
  entrants are under 1% of k at every k up to K/2.
- **B7**: the shape term is under 30% of R at every k in KR.
- **B8** (across platforms): the slide per week on Facebook is 1.3 to 3 times
  the mean on the two Reddit panels. It can only be computed on the no-gap
  pairs, whose block lengths differ, so it is weak evidence either way and is
  labelled so.
- **B9**: R over k v / (2S) is between 0.5 and 1.1 at every k in KR.
- **B10** (Reddit; added in v2): the slide per week from block 1 to 2 is
  between 0.7 and 1.5 times the slide from block 2 to 3. Above 1.5 means the
  no-gap slide is mostly decay of what selected the cohort.

I expect some to fail; B3, B5, B8 and B10 are the ones I am least sure of.

## 7. Precision, runs and stopping

One run per panel with the committed runner. Two declared secondary runs,
reported and not scored: 26-week blocks on the Reddit panels and the 136-week
comments window, both computed only for pairs of blocks a whole number of
years apart. Any other variation of block length, horizon, boundary, bin or
presence rule is a new design, written and dated before it runs. No number
from one panel chooses a setting for another.

Uncertainty is of three kinds and is labelled: the endpoint bootstrap on
cohort means and on the predictable share, conditional as stated in §5; the
range across calendar segments and across the counted pairs; and, from step
2, the spread across seeds under each planted rival. Dependence across
endpoints and over time is not removed by any of them.

## 8. A feasibility screen, not an identification result

Step 3 would predict local slopes from measured movement with no constant
taken from the ladder. The map it would use is S_k = v_k / (2 s), with s the
common slide per week of a cohort fixed beforehand and v_k the variance per
week of changes near rank k. Whether that map recovers slopes is not
something step 1 can show. Step 1 can only say whether trying is worthwhile.
The screen passes on a panel if all of these hold on the primary pair:

1. A1.
2. The predictable share at K/4 and K/2 is material and determined: its
   interval lies inside 0.5 to 1.5 and has half-width at most 0.25. A share
   that is precisely near zero fails.
3. The shape is stable (B7).
4. Flow is small: |F| under 15% of R up to K/2.
5. The small-step bridge holds within 0.5 to 1.5 over KR.
6. Two pairs of one cohort agree (B10).

Any FAIL fails the screen; any part that is INCONCLUSIVE or NOT ADJUDICATED
makes it INCONCLUSIVE. Facebook cannot pass part 6 with three blocks.

A screen that passes licenses step 3 to be attempted. Identification is
claimed only within the class of rivals of step 2, and only if step 2 shows
that held-out slopes are recovered from movement-only inputs across that
class, with error rates and their uncertainty fixed in Codex's design. The
theory note's bound is an upper bound: the relative error of a predicted slope
is at most the sum of the relative errors of variance and drift, inflated as
the drift error approaches one. It is not a lower bound, and equal errors in
both cancel.

If the screen fails, the result of this phase is a stated requirement (longer
panels, more endpoints, a different instrument) or a narrower claim. It is
not another model fitted to the same ladder.

## 9. Step 2 — planted rivals (Codex, brief V4)

As in the brief, with what the review adds: the rival set includes a
position-only OU and random walks with persistent noise at the measured
memory; the classes to be told apart are those that differ in what a slope
prediction would need, so the relabelled slow and fast components are an
invariance check and not two classes; the study reports error rates with
their uncertainty and the recovery of held-out slopes from movement-only
inputs. It runs `mts_step1.analyse` and `mts_step1.score` at this version.
Codex does not read step 1's empirical output until its recovery map is
committed.

## 10. Limits known in advance

- R is exact and is not local time; v / (2S) is an approximation.
- Cohort means are survivor-conditional, and their selection block's noise can
  still be decaying when the change is measured. B10 measures that on Reddit;
  nothing measures it on Facebook.
- Three counted pairs on Reddit share blocks and are not independent. Facebook
  has one.
- Nothing here distinguishes endpoints by type.
- The associations of §5 await step 2 before they mean anything.

## 11. Commands and outputs

    python llm_fitting/mts_step1.py run <panel> --reviewed
    python llm_fitting/mts_step1.py run <reddit panel> --reviewed --block 26     (secondary)
    python llm_fitting/mts_step1.py run reddit_comments --reviewed              (secondary)
    python llm_fitting/mts_step1.py report

run under `caffeinate -dims`, through `scripts/heavy_lock.py` if a panel takes
more than five minutes. Output: `llm_fitting/runs/2026-10-13_M1_balance/cells/`
and `report_tables.md` (tables, validity flags and every score, written by the
runner), a `REPORT.md` in that folder, and a dated entry in MODEL_STATUS. Data
contact is logged in `EXPOSURE_LEDGER.md`.
