# Movement to structure: measurement protocol for steps 1 and 2

_2026-10-13. Claude (Opus 5.5). First deliverable of the agenda
`RESEARCH_AGENDA_MOVEMENT_TO_STRUCTURE_2026-10-12.md`, which the owner adopted
on 2026-10-13._

**Status: DRAFT v1, FOR REVIEW. Nothing has been run on any panel.** The agenda
requires this protocol to be reviewed before anything runs. Codex reviews it
under queue row R4. The runner refuses to start on a registered panel without
a flag that is to be used only after that review is answered. The statistics
and the runner exist and are tested on synthetic panels only
(`balance_accounting.py`, `mts_step1.py`, `tests/test_balance_accounting.py`).

DEVELOPMENT data only. No fit, no simulation of the frozen model, no gate
evaluation, no sealed data. Everything here is descriptive; it can establish
what holds the ladder in an accounting sense and cannot by itself establish a
mechanism.

## 1. The question this step answers

The record has one relation between movement and structure: lasting variance
divided by twice the ladder's local slope is close to one constant across
ranks (§2z-bf, §2z-bj). The constant was read off the ladder, so nothing
independent measures it (§2z-bh). Step 1 measures it independently, and says
who carries it.

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

This holds for any process and any horizon. It is the finite-horizon form of
the balance the agenda is built on. In a diffusion R is half the local time of
the gap between ranks k and k+1, and for small steps R = k v / (2 S), with v
the variance of the changes near rank k and S the local slope. So
G = R = k v / (2S) is the relation S = u a / (2G) of the agenda, with each of
the three sides now measured separately:

| term | what it is | measured from |
|---|---|---|
| G / k | occupant loss per endpoint | named changes |
| R / k | reordering gain per endpoint | who replaced whom |
| v / (2S) | the small-step value of R / k | variance and slope, as in §2z-bf |

Following Claude's review of the first proof result (§2z-bn), every term is a
finite-horizon quantity measured for the same endpoints over the same
interval. No instantaneous variance is estimated.

**The centre and what "drift" then means.** The centre is positional: the
mean level of whoever holds the top k_c at each time. If no endpoint has any
restoring force and the ladder holds its shape because others climb in from
below, every occupant still loses ground against that centre, at one common
rate. I call that the **common slide**. It is the Atlas picture, and it needs
no force on anyone. The measurements below keep three things apart that the
single symbol G would merge:

1. the common slide: the mean change, against the centre, of a cohort fixed
   beforehand;
2. drift that depends on position: differences in that mean change between
   rank bins of the cohort;
3. return toward an endpoint's own earlier level: whether, given where an
   endpoint was, its still earlier level predicts its later change.

Flow is the fourth carrier.

## 3. Declarations

**Panels.** `facebook_a` (K = 3,500; 86 weeks), `reddit_comments_ext`
(K = 12,500; 213 weeks), `reddit_submissions_long` (K = 5,000; 212 weeks),
loaded with `minimal_rankdiff.load_panel`. The 136-week comments panel is the
first part of the comments extension and is not run separately. Instagram is
left out (censored).

**Level.** `log1p` of the weekly metric, as loaded. No common factor is
removed; the centre does that work.

**Population and boundary.** Positions are ranks among everything the
instrument observed that week. Endpoints ever inside the weekly top 4K are
tracked. The runner records, for each block, the highest level an untracked
endpoint could have had and the level at rank 2K, so that exactness of the
block ladder down to 2K can be checked. Centres and cohort cutoffs at K/2, K
and 2K; K is primary.

**Blocks.** Whole blocks counted back from the end of the panel, so the
earliest weeks are dropped: 52 weeks on the two Reddit panels (four blocks),
which averages out the annual cycle of §2z-aw; 26 weeks on Facebook (three
blocks; this also drops the election weeks at the start of Era A). A block
level is the mean over the weeks an endpoint is present, and is undefined
below 70% presence.

**Cohorts and information.** A cohort is defined on one block only, by
absence-penalized permanent rank over that block, in octave bins of rank down
to the cutoff, plus one bin for everything else. Its changes are measured
between two later blocks. Nothing from the later blocks enters its
definition.

**Identifiers.** Facebook ids are page names; a rename reads as an exit and an
entry and lands in F. On Facebook absence is not behavior, so F there is a
property of the collection.

## 4. Part A — weekly positions (a check of the instrument, and one measurement)

For horizons 1, 4, 13, 26 and 52 weeks (those under half the panel), origins
every half horizon, and k from the centre's k down by halving to 12: G, R, F,
the shape change, k v / (2S) with v and S from the ranks within a factor √2
of k, the step size √v / S, and the share of occupants replaced. Reported as
rates per endpoint per week: mean over origins, and the range over four
consecutive calendar segments.

On observed weekly positions the identity must close to rounding in every
cell. That is the check of the code. The comparison of R with k v / (2S) at one
week is the measurement: it says how well the small-step relation holds when
both sides carry the passing swings.

## 5. Part B — block levels (the measurement)

Between blocks, for every pair with the cohort's block strictly earlier:

- the same accounting on block levels;
- **cohort drift:** per bin, the mean and the mean squared change against the
  centre, the attrition, and a 5–95% endpoint bootstrap (400 draws);
- **occupancy:** the share of each cohort bin inside the top k at the start
  block;
- **the predictable share:** minus the sum, over the occupants of the top k
  at the start block, of the mean change of the cohort bin each belongs to,
  divided by R. It is the part of the reordering gain accounted for by drift
  measured without using the starting position;
- **carriers:** for adjacent blocks, the regression of the change on the level
  one block before the start and, where a still earlier block exists, on that
  level too, all against the centre, within the cohort.

How the carrier regression reads:

| | slope on the level one block earlier | slope on the still earlier level |
|---|---|---|
| no restoring force | zero | zero |
| drift that depends on position | negative | zero |
| return toward own earlier level | negative | positive |

Every predictor precedes the interval over which the change is measured, so
noise in a predictor cannot produce a restoring slope. The synthetic tests
confirm zero slopes on a random walk with autocorrelated noise and the third
row on a reverting level.

## 6. Predictions, written before any run

Each has a direction and a rough size. Scored PASS, FAIL or PARTIAL in the
write-up. I expect some to fail; B3, B5 and B8 are the ones I am least sure
of.

Part A

- **A1** (code): the identity's residual is below 1e-9 per endpoint-week in
  every cell.
- **A2**: at one week, R over k v / (2S) is between 0.6 and 1.2 for k from
  K/16 to K on all three panels, and below 1 at the head, where changes are
  fat-tailed.
- **A3**: the occupants' loss per week at one week is at least five times
  the loss per week at the longest horizon, at k = K/4, on every panel. This
  is the prediction made in §2z-bn: at one week the restoring drift is mostly
  passing swings decaying.
- **A4**: at horizons up to 13 weeks the shape term is under a tenth of R for
  k ≥ K/16.

Part B

- **B1**: the block-level reordering gain per endpoint per week, for k from
  K/16 to K/2, is within a factor 1.5 of the constants of §2z-bf: 0.0018 to
  0.0045 on the Reddit panels, 0.0045 to 0.010 on Facebook.
- **B2** (the independent measurement of the constant): on the Reddit panels
  every cohort bin from K/64 to K has a negative mean change against the
  centre over 52 weeks, and the pooled value per week is within a factor 2 of
  the reordering gain at K.
- **B3**: the common slide dominates position-dependent drift: in at least
  five of the seven octave bins the bin's mean differs from the pooled cohort
  mean by less than 30% of the pooled mean, or the difference is inside the
  bin's bracket.
- **B4**: no return toward own level at a year: the slope on the still
  earlier level is below 0.10 per 52 weeks or its bracket includes zero.
  Reversion at 0.01 per week, the middle of the range considered in §2z-bn,
  would give about 0.2.
- **B5**: the predictable share is between 0.5 and 1.2 for k from K/4 to K on
  the Reddit panels.
- **B6**: flow is under 15% of R for k ≤ K/2 on Reddit and larger at 2K; on
  Facebook entrants are near zero.
- **B7**: over one block step the shape term is under 30% of R for
  k ≥ K/16.
- **B8**: the common slide per week on Facebook is 1.3 to 3 times that on
  Reddit (the constant is about twice as large there, §2z-bh).
- **B9**: on block levels R over k v / (2S) is between 0.5 and 1.1; block
  steps are not small against the slope.

What would change my reading most: a clearly positive slope on the earlier
level (B4 failing) would revive a personal home; a predictable share near
zero with a stable shape (B5 failing low) would mean the occupants' loss is
selection on what is left of the passing swings in a block mean, and that the
block length is too short for this design.

## 7. Precision, runs and stopping

One run per panel with the committed runner, as declared. Two declared
secondary runs, reported whatever they show: 26-week blocks on the Reddit
panels, read only for pairs of blocks a whole year apart; and the 136-week
comments window alone. Any other variation of block length, horizon,
boundary, bin or presence rule is a new design, written and dated before it
runs. No number from one panel chooses a setting for another.

Uncertainty is of three kinds and is labelled as such: the endpoint bootstrap
on cohort means; the range across calendar segments and across block pairs;
and, from step 2, the spread across seeds of each statistic under each planted
rival. Sampling uncertainty of G, R and F for one pair of blocks is not
defined (they are totals over the realized ladder); their variation over
block pairs and planted seeds stands in for it.

## 8. The first decision: is step 3 identified on development data?

Step 3 would predict local slopes from measured movement, with no constant
taken from the ladder. On the development panels it is identified only if all
four hold on both platforms:

1. A1 holds.
2. The common slide is determined: the 5–95% bracket of the predictable share
   at K/4, K/2 and K has half-width at most 0.25. The theory note's bound makes
   the relative error of a predicted slope at least the relative error of the
   drift.
3. Two block pairs of the same panel agree within their brackets where two
   exist.
4. In step 2 the statistics of §5 tell the planted rivals apart at the real
   panel sizes: no rival is taken for another in more than 20% of seeds by the
   rule fixed in Codex's design.

If any fails, the result of this phase is a stated requirement (longer panels,
more endpoints, or a different instrument) or a narrower claim. It is not
another model fitted to the same ladder.

## 9. Step 2 — planted rivals (Codex, brief V4)

Processes that give a similar ladder and similar variance and differ in what
holds the ladder, at the real panel lengths and sizes, with the real noise,
annual cycle, selection and missingness, passed through `mts_step1.analyse`
unchanged:

1. the frozen model itself (a personal home);
2. fixed identities with no dependence on position and no home;
3. growth stabilised by replacement from below, with no dependence on state;
4. drift that depends on position only;
5. a level with no stabiliser at all;
6. a slow and a fast component of equal stationary variance, on which the
   statistics must give the same answer whichever is called the signal.

Codex writes its design, predictions and classification rule first, and does
not read step 1's empirical output until its recovery map is committed. The
map reports, per rival and statistic, the distribution over seeds, and both
recovery and false identification.

## 10. Limits known in advance

- The small-step value k v / (2S) is an approximation; R is not. Where they
  differ, R is the fact.
- A block mean still carries some of the passing swings. Cohort means are
  protected from it by construction; G on block positions is not, which is
  why the predictable share is reported separately from G.
- Three or four blocks give one to three pairs. Calendar replication is thin
  and Facebook has one pair and no still-earlier block, so the third carrier
  cannot be separated there.
- Tracking is exact for the top 2K only if no untracked endpoint could have
  reached it; the runner reports the margin and the result is void for any k
  where it fails.
- Nothing here distinguishes endpoints by type.

## 11. Commands and outputs

    python llm_fitting/mts_step1.py run <panel> --reviewed
    python llm_fitting/mts_step1.py run <reddit panel> --reviewed --block 26     (secondary)
    python llm_fitting/mts_step1.py run reddit_comments --reviewed              (secondary)
    python llm_fitting/mts_step1.py report

run under `caffeinate -dims`, through `scripts/heavy_lock.py` if a panel takes
more than five minutes. Output: `llm_fitting/runs/2026-10-13_M1_balance/cells/`,
with a `REPORT.md` in that folder scoring every prediction, and a dated entry
in MODEL_STATUS. Data contact is logged in `EXPOSURE_LEDGER.md`.
