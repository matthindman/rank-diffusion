# Status for Codex, 2026-10-14: where the program is and where your effort counts most

_From Claude (lead), at the owner's request, on `main` at the commit that adds
this file. Read it with `git show main:llm_fitting/coordination/briefs/STATUS_FOR_CODEX_2026-10-14.md`.
Everything cited is in `llm_fitting/MODEL_STATUS.md` by section; quote numbers
from there, not from this page. **Your blind is lifted** (since §2z-by): read
anything. The owner has seen and agreed the direction below; the queue rows at
the end are the actionable part._

## 1. The claim the paper is now built around

**The rate law.** For the communities present at both ends of a period (the
persisting set), let U_k be the mean level of its top k positions and
shape_k = U_k − U_K. Then

    d shape_k / dt  =  σ_k² / (2 S_k)  −  s̄_k

- σ_k²: lasting variance per week of named communities at rank k;
- S_k: the ladder's local slope at rank k (minus d level / d log rank);
- s̄_k: mean slide, against U_K, of the named communities ranked above k.

In words: the top k gains from crossings at its boundary, which depend only
on how much communities there move (σ²) and how crowded the ladder is there
(1/S), and loses whatever its members lose against the pack. Equal: the shape
holds. Unequal: the shape moves at the difference, and the slope then adjusts
until crossings make up the loss. With one slide for everyone this is the old
stationary formula S = σ²/(2g); the rate form can be tested where the ladder
moves. Entry is a separate, exactly computed term. (§2z-cd, §2z-cg.)

**Owner's framing (§2z-cj): the paper can be agnostic about what sets the
drift by rank.** Drift by rank is a measured input. The claim is that
structure follows movement through this boundary balance, and that structure
can be forecast to the extent drift by rank can be forecast or detected.

**What the evidence supports, and what it does not:**

| claim | standing |
|---|---|
| The ladder's change equals members' mean movement plus σ²/(2S) at the boundary, at every rank from K/64 to K/2, on two Reddit metrics, through dated platform changes | holds in same-year form: 24 yearly cells, slope 0.76, r 0.94 (§2z-cd); 60 half-year cells, slope 0.88, r 0.90, including three reversals of the comments head (§2z-cg). **But in the same-year form the change of the centre cancels**: observed − predicted = (change of the persisting top-k mean) − (mean change of named members above k) − σ_k²/(2S_k). So it is an accounting of change by named movement with a theory of the boundary, not a forecast (§2z-cg qualifies §2z-cd). |
| Structure can be forecast from earlier movement | only where drift by rank persists: lagged law and forward simulation beat "no change" on submissions (one and two years ahead, error a quarter to a third of "no change"); both fail on comments, where drift by rank turns every half year (§2z-cd, §2z-ce, §2z-cg) |
| What sets drift by rank | not claimed (owner). Platform growth does not track it (§2z-ch). The June 2021 Home-feed change acted on the top of submissions within days (§2z-bz, §2z-ca). Comments drift by rank has an annual cycle (§2z-ci). |

## 2. Facts you need, with sections

**Data.** Two Reddit panels carry the work: `reddit_comments_ext` (comment
karma, K = 12,500) and `reddit_submissions_long` (submission karma, K = 5,000),
December 2018 to December 2022, both DEVELOPMENT. Karma is a sum of scores,
not an event count (relevant to your count-sensor proofs). Facebook Era A is
unreadable for this work (passing swings dominate 26-week blocks). The Reddit
2023–24 bridge and Wikipedia are SEALED. Score-capture flag list (opt-in,
`instrument_eras.REDDIT_SCORE_FLAGGED_DAYS`): 28–30 April 2020 (scores about 1
per post) and 12 June 2021 in submissions; the week of 27 April 2020 is
damaged (§2z-ca, §2z-cb). The archive's collection boundary at 1 July 2021 is
**not** the cause of the June 2021 step, which begins 23–24 June (§2z-ca).

**Movement.**
- No return toward an endpoint's own level at a year; year-over-year levels
  move like a random walk (§2z-aw, §2z-bu).
- Block means shorten variance-like quantities by (D − L/3)/D and leave mean
  drifts alone. That was the error in step 1's reading (§2z-bw); V4's nuisance
  layers put back part of it, so the full factor over-corrects (§2z-by).
- Lasting rate, three routes that now agree within about 12% (§2z-cc):
  comments 0.0031 (slide against persisting) / 0.0034–0.0036 (reordering,
  spacing-difference estimator, §2z-by item 5) / 0.0034 (variance route);
  submissions 0.0040 / 0.0035–0.0040 / 0.0039, per week.
- Variance by rank falls with size on both panels; on comments the ratio
  σ²/(2S) drops by a third to a half over the bottom two octaves with the
  variance-route estimator (§2z-cc), so "one constant across ranks" (§2z-bf)
  is weaker than first reported.

**Entry.** 81–87% of entrants to the top K are new to the record; exits are
almost never disappearances (§2z-bv). Entry net of exit is 29–41% of the
incumbents' slide on comments, 8–19% on submissions (§2z-cc).

**Drift by rank.** Named communities' mean change depends on rank (§2z-cc
tilt, bottom minus top per week): comments +0.0002 (2020→21), −0.0020
(2021→22); submissions +0.0035, +0.0013. Caution, post hoc: "no drift" in logs
and "no drift" in levels differ by −σ²/2, which itself tilts by rank because σ²
does; the comments 2021→22 tilt equals the level-unbiased benchmark (§2z-cc).

**Detecting a change in drift by rank (§2z-ci).** Smallest change detectable
at z = 3 against a 26-week baseline, per 1,000 weeks (the slide is about
4.3): comments top K/16 7.8 / 4.4 / 3.4 after 4 / 13 / 26 weeks, ranks
12.5k–25k 2.6 / 1.4 / 1.0; submissions top K/16 8.9 / 5.9 / 5.2. June 2021
shows in one to two weeks, as the very top falling and ranks 156–1,250 rising.
On comments the head's drift departs from its previous half year in 42–55% of
windows, peaking every summer and reversing in winter (the owner's reading:
school holidays). So detection needs a seasonal baseline, and the comments
forecast failures may be largely this cycle; not yet tested. The bootstrap
treats communities as independent.

**Your V4 (§2z-bx, review §2z-by).** The protocol's slope map recovers
held-out slopes in none of the ten planted worlds; return-to-home signs appear
in persistent-noise walks; your fixed classifier puts the real step 1 results
far outside all ten worlds (squared distances 15–329 against 0.5–1.9 within
worlds), because every planted world is stationary and the data are not.

**Your proof campaign (§2z-cj).** Reviewed; no error found in what I checked
(the finite-age theorem's single-age core and bound; counterexample 3
reproduced exactly). Most useful consequence: a **within-community test** of
whether volatility is a trait carried by a community or a response to its
current size. The theorem cannot decide it on Reddit 2019–22 because the window
has both of its escapes (positive common growth, a burst of entry). Count-sensor
bounds are conditional on event counts and wide; the proportional-growth
witness needs about 21 million events a day and widens the ladder. Your
recommended structural test, prespecified targets, drift and variance by rank
measured beforehand, no conditioning on the target year, is the design of
§2z-cd's lagged test and of the draft 2023 protocol (§2z-cf). The campaign
and proofs 9–13 are **uncommitted** in your debate working copy.

**Draft protocol, not adopted (§2z-cf).**
`llm_fitting/BRIDGE_FORECAST_PROTOCOL_DRAFT_2026-10-14.md` with forecasts fixed
from 2019–22 data before any contact: both forecasters say the submissions
head keeps sinking against the top 5,000 through 2023 (about −0.19 at K/64,
−0.09 at K/8 log units). Conditions before scoring: the April 2023 source
change, capture delay by day, the June 2023 blackout, the missing
`RC_2023-05.zst`. Adoption and the first sealed contact are the owner's.

## 3. Where your effort counts most, in order

**1. Row R5, rate law first.** Attack §2z-cd and §2z-cg before anything else.
Specific questions:
- Is the same-year law more than R_k/k = σ_k²/(2S_k) at the boundary plus
  "cohort drift stands in for occupants' drift"? If it is just those two,
  say so; the paper's wording depends on it.
- Recompute the lagged tests independently (code: `mts_balance_law.py`,
  `run` and `run-halfyear`; cells in `runs/2026-10-14_balance_law*/`). Do they
  survive your nuisance layers and heavy tails?
- Is the over-prediction (observed ≈ 0.76 × predicted) the half-year-old
  cohort inflating slides, or something structural?
- The spacing-difference estimator (§2z-by item 5) and the variance route
  (§2z-cc): bias with persistent swings, momentum and the annual cycle.
- The detection study's bootstrap (§2z-ci): how far does cross-community
  dependence inflate the departure rates?

**2. A new planted study, V5 (queue row below): worlds that contain the data.**
Your simulator is the program's independent check, and every world so far was
stationary. Needed:
- a growing population with entry at measured rates and offsets;
- drift by rank that changes in time: a step at a known date at the top (a
  June 2021 analogue), a slow tilt, and an annual cycle by rank;
- volatility that is (a) a fixed trait of each community and (b) a function
  of current size, with the same marginal variance-by-rank profile;
- realistic passing swings and annual cycles per community, as in V2/V4.

Questions for it:
- Do the rate law's estimators (same-year and lagged) recover the planted
  shape change?
- How fast does `mts_drift_detect.py` find a planted change, and with what
  false-alarm rate, with and without a seasonal baseline?
- Can a within-community test tell trait volatility from size-dependent
  volatility, and at what panel length?
- Does a seasonal baseline rescue forecasts when drift by rank cycles?

Fix the classification and scoring rules before generation, as in V4.

**3. Review the draft 2023 protocol (row R6 below)** before the owner decides.
Is the primary test (forward simulation beats "no change" on submissions)
the right one? Are the blackout and source-change conditions sufficient? Is
anything in it scored on a quantity a 2023 data artefact could fake?

**4. Commit the proof campaign and proofs 9–13** on `codex/d1-debate`, so they
can be merged and recorded. No further broad inverse or identification
proofs, count-sensor certificates for karma, or stationarity-based
identities: the review (§2z-cj) and your own report both say these are not
the bottleneck. A proof targeted at the within-community test's power, or at
the rate law's error terms (finite steps at the head, cohort-versus-occupant
mismatch), would be useful.

## 4. What Claude is doing next, so you do not duplicate it

- A seasonal model of drift by rank (three years to estimate, one to test) and
  whether it rescues the comments forecast.
- The within-community volatility test on the development panels, once you
  have said what power it needs (V5).
- Integration of whatever you hand over.

## 5. Coordination facts

- Claude is the only writer of MODEL_STATUS, the ledgers, PROGRAM_STATE and
  QUEUE. Your handoff `STATUS_ENTRY.md` stands in for your record entry.
- `main` is 12 commits ahead of `origin`, unpushed by the owner's choice; the
  owner pushed through 97f09c9 today. Your worktrees share refs with the main
  checkout, so `git show main:<path>` reads current files.
- Sealed folders: never look inside without the owner's word in conversation
  (`SEALED_PATHS.json`). The 2023 forecast numbers were made without contact.
- Heavy jobs through `scripts/heavy_lock.py`; long runs from a user-owned
  Terminal when another agent is active.
- Health check before every commit. Gate it on the exit code: a chained
  `(...; grep) && git commit` let a failing commit through once today
  (§2z-ci).
- Two of your branches are unknown to the record: `codex/assumption-menu`
  (cc5f8ac) and `codex/bootstrap-best-practices` (f4c4ac6). Hand them over or
  say what they are.

## 6. Files

| what | where |
|---|---|
| rate law, runner and tests | `llm_fitting/mts_balance_law.py`, `tests/test_mts_balance_law.py`, `runs/2026-10-14_balance_law/`, `runs/2026-10-14_balance_law_halfyear/` |
| replenishment split | `llm_fitting/mts_replenish.py`, `runs/2026-10-14_replenishment/` |
| forward simulation | `llm_fitting/mts_forward.py`, `runs/2026-10-14_forward/` (incl. post hoc `diagnose_hybrids.py`) |
| drift detection | `llm_fitting/mts_drift_detect.py`, `runs/2026-10-14_drift_detection/` (incl. post hoc `scan_post_hoc.py`) |
| daily check and flag list | `runs/2026-10-14_daily_date_check/`, `llm_fitting/instrument_eras.py` (`--score-guard`) |
| external evidence | `runs/2026-10-13_external_evidence/REPORT.md` |
| draft 2023 protocol and forecasts | `llm_fitting/BRIDGE_FORECAST_PROTOCOL_DRAFT_2026-10-14.md`, `runs/2026-10-14_bridge_forecast/` |
| earlier steps | M1 `runs/2026-10-13_M1_balance/` (read with §2z-bw, §2z-by), M2 `runs/2026-10-13_M2_entrants_shape/`, V4 review `runs/2026-10-14_V4_review/` |
