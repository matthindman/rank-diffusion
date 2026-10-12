# A1 and A2 — results

_Written 2026-10-12 by Claude (Opus 5.5). Design and predictions:
`DESIGN.md` in this folder, committed before any cell was run (f284b9b)._

Status: EXPLORATORY measurements on DEVELOPMENT data. No model code, no
gate evaluation, nothing adopted. Every number below is in
`report_tables.txt`, which `python llm_fitting/phase1_anatomy.py report`
regenerates from `cells/`. Simulation columns are means over seeds (20 for
A1, 30 for A2); "SD" is the spread across seeds.

One caution governs how far these can be read. Codex's task V2 has not yet
shown which of these statistics can tell the candidate mechanisms apart at
these panel sizes. Until it has, the measurements below say what the data
does that the simulation does not. They do not say which mechanism is
missing.

## A1 — where the tails live

### Tail shape of an endpoint's own changes

q99/q90 of |h-week change|, each endpoint's changes divided by its own
robust scale, middle rank group (10–50% of K). A larger ratio is a fatter
tail. "Walk" is the same statistic on a Gaussian random walk of the same
length (`phase1_anatomy.py benchmark`): the large-sample value is 1.57, and
it is higher here because each endpoint's scale is estimated from
overlapping changes.

| panel | h | data | simulation (SD) | data − sim | walk |
|---|---|---|---|---|---|
| FB Era A | 1 | 2.22 | 1.69 (0.01) | 0.53 | 1.63 |
| | 4 | 2.08 | 1.67 (0.01) | 0.41 | 1.64 |
| | 13 | 1.92 | 1.65 (0.01) | 0.27 | 1.72 |
| | 26 | 1.90 | 1.65 (0.01) | 0.25 | 1.83 |
| Long submissions | 1 | 2.13 | 1.73 (0.01) | 0.40 | 1.59 |
| | 4 | 2.21 | 1.67 (0.00) | 0.54 | 1.60 |
| | 13 | 2.17 | 1.63 (0.00) | 0.54 | 1.64 |
| | 52 | 2.06 | 1.63 (0.01) | 0.42 | 1.76 |
| Comments | 1 | 1.90 | 1.68 (0.00) | 0.22 | 1.60 |
| | 4 | 1.93 | 1.64 (0.00) | 0.28 | 1.61 |
| | 13 | 1.89 | 1.62 (0.00) | 0.27 | 1.68 |
| | 52 | 1.85 | 1.64 (0.01) | 0.21 | 1.82 |

The head and shell groups show the same pattern (tables). With the changes
divided by the whole group's scale instead, so that differences between
endpoints stay in, the one-week values are 2.37 against 2.07 on FB, 2.53
against 2.39 on submissions and 2.24 against 1.97 on comments (data
against simulation). On FB and submissions that gap (14% and 6%) is much
smaller than the gap with each endpoint scaled by itself (31% and 23%), and
on FB it is gone by 13 weeks. On comments the two gaps are the same size
(14% and 13%) and the pooled one widens with horizon (2.57 against 1.91 at
13 weeks).

### How much of a one-week move is kept

Middle group, fraction of the move still in place 13 weeks later.

| panel | big rise, data / sim | big drop, data / sim | drop minus rise, data / sim (SD) |
|---|---|---|---|
| FB Era A | 0.49 / 0.55 | 0.58 / 0.47 | +0.09 / −0.08 (0.02) |
| Long submissions | 0.60 / 0.60 | 0.56 / 0.47 | −0.04 / −0.12 (0.02) |
| Comments | 0.56 / 0.58 | 0.54 / 0.50 | −0.02 / −0.08 (0.01) |

Small moves (inside one robust SD), 26 weeks later: small rises keep 0.41
on FB, 0.06 on submissions and 0.16 on comments (simulation 0.45, 0.47,
0.41); small drops keep 0.57, 1.04 and 0.89 (simulation 0.58, 0.68, 0.65).

### Predictions, scored

1. *Data's one-week ratio exceeds the simulation's by 5–20% on all three
   panels.* Direction HELD on all three. The stated size FAILED on two: the
   excess is 31% on FB and 23% on submissions (13% on comments).
2. *The excess does not fade: at 13 weeks it is at least as large as at one
   week on FB and on comments, and the simulation is within 0.10 of the
   Gaussian-walk benchmark by then.* HELD on comments (0.22 → 0.27). FAILED
   on FB as stated: the excess halves (0.53 → 0.27) and then stays (0.25 at
   26 weeks). The second part HELD: the simulation sits 0.01–0.07 below the
   walk benchmark at 13 weeks. The benchmark was computed after the run; I
   had the large-sample value 1.57 in mind when writing the prediction, and
   against that the simulation is 0.05–0.08 above, so the clause holds
   either way. The stated kill condition (excess gone by about 4 weeks) is
   not met on any panel: the excess is at least 20 seed SDs at every
   horizon in the middle group.
3. *At 13 weeks big moves keep at least 0.05 more in the data than in the
   simulation on at least two panels.* HELD for drops (+0.11 FB, +0.09
   submissions, +0.04 comments). FAILED for rises (−0.06, 0.00, −0.02). The
   prediction did not separate the signs; as a statement about big moves in
   general it failed.
4. *In the data big drops are kept more than big rises on FB and on
   comments; the simulation shows no such asymmetry.* HELD on FB, FAILED on
   comments. Not anticipated: the simulation has the opposite asymmetry on
   all three panels (rises kept more than drops by 0.08 to 0.12). Relative
   to the simulation the data keeps drops more on all three.

## A2 — what the missing departures are

Cohort: train-window permanent rank ≤ K/2. Rates are per cohort-week at
risk; the simulation's range is the 10th to 90th percentile over 30 seeds.

| | comments extension | | long submissions | |
|---|---|---|---|---|
| | data | simulation | data | simulation |
| all exits | 0.00800 | 0.00350 [0.00283, 0.00410] | 0.00851 | 0.00311 [0.00214, 0.00408] |
| slides | 0.00512 | 0.00156 | 0.00584 | 0.00175 |
| excursions | 0.00269 | 0.00142 | 0.00248 | 0.00114 |
| lasting exits | 0.00011 | 0.00040 [0.00027, 0.00050] | 0.00010 | 0.00016 [0.00006, 0.00026] |
| first exit of an endpoint | 0.00161 | 0.00127 | 0.00180 | 0.00089 |
| repeat exits | 0.00639 | 0.00223 | 0.00671 | 0.00221 |
| back inside within 13 weeks | 0.950 | 0.830 | 0.936 | 0.903 |
| median margin the week before | 0.195 | 0.237 | 0.154 | 0.178 |
| share of the shortfall: slides / excursions / lasting | 79% / 28% / −6% | | 76% / 25% / −1% | |
| within-train split, data / simulation | 0.00832 / 0.00343 (2.42×) | | 0.00642 / 0.00360 (1.79×) | |

Spread of h-week changes, middle group, full-window universe, data /
simulation:

| | h = 13 | 26 | 52 | 78 | 78 over 26, data / sim (SD) |
|---|---|---|---|---|---|
| comments ext., robust (MAD²) | 0.181 / 0.300 | 0.266 / 0.420 | 0.417 / 0.574 | 0.671 / 0.677 | 2.52 / 1.61 (0.02) |
| comments ext., variance | 0.385 / 0.417 | 0.521 / 0.579 | 0.704 / 0.785 | 1.036 / 0.923 | 1.99 / 1.59 (0.01) |
| long submissions, robust | 0.081 / 0.117 | 0.144 / 0.177 | 0.275 / 0.274 | 0.464 / 0.352 | 3.21 / 1.98 (0.04) |
| long submissions, variance | 0.236 / 0.250 | 0.337 / 0.364 | 0.508 / 0.542 | 0.751 / 0.678 | 2.23 / 1.87 (0.03) |

At 78 weeks the simulation's variance is 10–21% below the data's in every
rank group on both panels.

### Predictions, scored

1. *The total rates equal the recorded ones.* HELD: 0.00800 and 0.00350;
   0.00851 and 0.00311. The data rate equals `exit_audit.score_matrix` to
   the last digit.
2. *Slides are more than half of the data's events on both panels.* HELD:
   64% and 69%.
3. *At least 60% of the shortfall is in slides plus lasting exits.* HELD
   (73% and 75%), and all of it is slides: lasting exits contribute a
   negative share.
4. *The simulation under-produces lasting exits by a larger factor than
   excursions.* FAILED on both panels. The simulation over-produces lasting
   exits, 3.6 times on the comments extension and 1.6 times on submissions,
   where the data's rate is inside the simulation's range.
5. *Within-train split: a smaller ratio than the recorded split, still
   above 1.3.* HELD on submissions (1.79 against 2.74). FAILED on comments
   (2.42 against 2.29): the deficit there is as large inside the training
   window as outside it.
6. *Spread at 78 weeks over spread at 26 is larger in the data.* HELD on
   both panels, in the robust measure and in the variance.

## What the measurements show

1. **The departure deficit is not a deficit of sudden lasting exits.** An
   established endpoint that leaves the top-K abruptly and stays out is
   rare in the data (1.2–1.4% of events) and the simulation makes more of
   these than the data has, largely because simulated exits include deaths
   (0.00048 against 0.00011 per week on comments).
2. **Three quarters of the missing events are exits by endpoints that were
   already sitting near the boundary.** The typical data event is an
   endpoint from the top half of the list that spent the previous quarter
   around 0.83–0.86 K and crosses the cut repeatedly: four in five events
   are repeat exits, and 94–95% are back inside within 13 weeks.
3. **Most of the deficit is repeat crossing.** Endpoints that leave at all
   are 1.3 times as common as simulated on comments and 2.0 times on
   submissions; repeat exits are 2.9 and 3.0 times. Read together with 2:
   in the data an established endpoint can relocate to the boundary and
   stay there; in the simulation it is pulled back to a home in the top
   half or it dies.
4. **On comments the deficit is not a parameter-drift effect.** It is as
   large when cohort and scoring window both lie inside the training
   window. On submissions about a third of the ratio goes away.
5. **The data spreads faster with horizon than the simulation.** Variances
   are within 12% of each other to 52 weeks, the longest horizon the fit
   uses, with the simulation the higher in the middle group; at 78 weeks
   the simulation is 10–21% short. The robust spread tells more: the centre of the
   simulated distribution is 23–66% too wide at 13 and 26 weeks and then
   stops growing, while the data's grows nearly in proportion to the
   horizon (on submissions 0.0062, 0.0055, 0.0053 and 0.0059 per week at
   the four horizons).
6. **An endpoint's own changes are fat-tailed at every horizon measured,
   and the simulation's are close to Gaussian** (0.06–0.14 above the walk
   benchmark at one week, against 0.30–0.59 for the data). On FB and
   submissions the simulation is much nearer the data when endpoints are
   pooled than when each is scaled by itself, which is what a model does
   when it carries its fat tails as differences in scale between
   endpoints. On comments it is equally far either way.
7. **Big drops stick more than the model allows and big rises do not.**
   The data keeps 0.04–0.11 more of a big drop after 13 weeks on every
   panel, and no more of a big rise.

## What they do not show

- Whether the persistent fat tails are fat-tailed lasting shocks or an
  endpoint's volatility changing over time. Both fatten an endpoint's own
  changes at every horizon. V2 plants both.
- Whether the slides are slow drift or a step down followed by flicker at
  the boundary. The event classes count events, and one relocation produces
  many. An endpoint-level count (how far the cohort's level moved between
  the two windows, and its shape) is the measurement that separates them;
  it is not in this design.
- Why the simulation keeps rises more than drops. A likely cause is that
  endpoints that drop and stay down leave the recorded range, which removes
  them from the average in data and simulation alike. Not checked.
- Anything about small-move drift beyond the fact. On the two Reddit panels
  small rises are almost entirely undone after 26 weeks and small drops
  grow, with the same average as the simulation's. That is a steady loss of
  ground by the middle group relative to the panel mean, four to five times
  the simulation's. The record already has a related measurement: directional
  slow movement (§2p) and no dispersion in constant drifts on comments
  (§2p, H2). This is recorded as an observation, not a new claim.

## Consequences for the plan

- **C-T ("tails where they persist") loses its departure argument.** It was
  proposed to create sudden lasting moves; the data has fewer of those than
  the simulation. What A1 leaves standing for it is the tail shape at long
  horizons, which time-varying volatility would also produce.
- **The lasting level is the place to look, and reversion to a fixed home
  is the suspect.** Findings 2, 3 and 5 all describe endpoints whose level
  moves and stays moved. A4 found the reversion rate at its slowest grid
  value in a third to a half of bands, so the fit is already asking for
  this.
- **A lead on amplitude, on two panels.** Finding 6 is consistent with the
  conjecture recorded in §2z-as on FB and submissions: the model may be
  assigning to permanent differences between endpoints (the amplitude
  spread s) tail weight that in the data sits inside each endpoint. If so,
  s is too large there, and A5 showed what too much amplitude does to the
  FB head. Comments does not show the pattern. V2 part 2 measures whether
  the estimator inflates s when the truth has fat tails inside endpoints.
- **Follow-up measurements** are designed separately (`A7`), with their own
  predictions written first: the endpoint-level count named above, whether
  an endpoint's past direction predicts its later movement, and whether an
  endpoint's volatility persists over time.

## Not run

Any gate evaluation. Any model change. FB in A2 (86 weeks leave no scoring
window). The 104-week horizon (too few windows).

## Files and commands

`llm_fitting/residual_anatomy.py`, `tests/test_residual_anatomy.py`,
`llm_fitting/phase1_anatomy.py`; this folder: `DESIGN.md`, `cells/`,
`logs/`, `report_tables.txt`, `gaussian_walk_benchmark.json`.

```
python llm_fitting/phase1_anatomy.py a1 facebook_a            # also reddit_submissions_long, reddit_comments
python llm_fitting/phase1_anatomy.py a2 reddit_comments_ext   # also reddit_submissions_long
python llm_fitting/phase1_anatomy.py report
python llm_fitting/phase1_anatomy.py benchmark
```
