# Instagram Joint-Post Allocation Plan

**Date:** 2026-07-18  
**Status:** owner-directed design plan; not yet implemented  
**Scope:** Instagram 2023 CrowdTangle post-level data and any future Instagram
panel built under the same instrument  
**Protocol status:** this document does **not** amend or revive
`PREREG_2026-07-16_submissions_ig.md`. Under that registration, P10 and P11
remain **NOT ADJUDICATED**. The corrected 2023 construction is exploratory /
technical-sensitivity work. Confirmatory use requires a new registration on
untouched data.

**Repository note (2026-10-08):** committed as written on 2026-07-18/19,
without re-validation. Not implemented as of this date. Research priorities
were under review on 2026-10-08, so check `MODEL_STATUS.md` for the current
agenda before acting on this plan.

## 1. Executive decision

The observational unit for the endpoint model is an **account-post
attribution**, not a globally unique URL and not a raw extraction row.

For a collaborative post:

1. retain one observation for every participating `user_name`;
2. count the post as observed presence for every participating account;
3. count the post's aggregate interactions only once across the platform;
4. apportion those interactions across participating accounts in proportion
   to their independently estimated expected interaction levels;
5. use subscriber count as the cold-start exposure signal;
6. update the subscriber-based prior with empirical-Bayes account effects
   learned only from the account's earlier **non-joint** posts.

This separates two quantities that the failed build conflated:

- **incidence / presence:** which account profiles carried the post;
- **attention:** how the shared interaction total is attributed among those
  profiles.

The allocation is a measurement model. It is not a new component of the
rank-diffusion process, and it must be selected and validated without looking
at downstream scorecards, movement gates, P10, or P11.

## 2. Why a correction is required

The 2026-07-16 full-file audit established:

- 53,473,688 raw post rows and 52,023,601 distinct URLs;
- 659,028 URLs with conflicting identity, almost entirely one URL attributed
  to multiple usernames;
- 1,387,834 affected rows (2.60% of rows) carrying approximately 10.58% of
  recorded interactions;
- 97.3% of the conflicting URLs map to multiple display-account names;
- all but one share the exact post date/time, link, and title;
- the multiplicity is predominantly two to four usernames, consistent with
  collaborative/co-authored posts;
- the old analyzed weekly panel was built by effectively summing raw rows,
  including both legitimate multi-account attributions and repeated
  extraction snapshots;
- the registered global-URL deduplication rule would arbitrarily erase
  collaborator attributions;
- the registered exact reconciliation target was infeasible because the old
  weekly target and proposed daily builder used different observational
  units.

Therefore neither of the two existing constructions is a satisfactory
scientific primary:

- **raw-row aggregation** double-counts extraction snapshots and gives every
  collaborator full credit for one shared interaction total;
- **global URL deduplication** counts the content once but assigns it to only
  one endpoint, deleting the others.

The required construction is a conserved allocation over cleaned
account-post attributions.

## 3. Estimand

Let post content item \(p\) have:

- canonical URL \(u_p\);
- post date \(t_p\);
- observed aggregate interaction total \(Y_p\);
- participating accounts \(C_p\);
- subscriber count \(S_{ip}\) for participating account \(i\);
- an ex-ante expected interaction score \(q_{i,t_p^-}\).

The account-level allocation is:

\[
w_{ip} = \frac{q_{i,t_p^-}}
               {\sum_{j \in C_p} q_{j,t_p^-}},
\qquad
Y_{ip} = w_{ip}Y_p.
\]

Required identities:

\[
w_{ip} \ge 0, \qquad
\sum_{i \in C_p} w_{ip}=1, \qquad
\sum_{i \in C_p}Y_{ip}=Y_p.
\]

Interpretation:

- `metric_value` measures each endpoint's apportioned share of the observed
  interaction total;
- `n_posts` measures account-post incidence, so each collaborator receives
  `n_posts = 1` even if its allocated interactions are zero;
- platform totals count each content item's interactions once;
- the model remains scoped to tracked / query-matching Instagram activity,
  not platform-wide attention.

The allocation does **not** identify causal audience contribution. Audience
overlap and collaborator-specific impression logs are unobserved. It is a
conservative, explicit attribution convention based on expected reach.

## 4. Raw-data normalization

### 4.1 Required source fields

The raw input must provide:

- `url`;
- `user_name`;
- `post_created_date`;
- `total_interactions`;
- subscriber count (currently stored in the raw `followers` column);
- post type, retained for diagnostics;
- any available extraction timestamp or source-record identifier.

The plan uses the generic term **subscriber count** below. The implementation
must document the exact mapping to `followers` and must not silently switch
fields.

### 4.2 Strict hygiene

Fail before allocation on:

- null or empty URL;
- null `user_name`;
- null post date;
- nonfinite, fractional, or negative `total_interactions`;
- an unexpected schema;
- a date conflict within the same `(url, user_name)` unless that URL has been
  explicitly quarantined by the date-anomaly rule below.

Subscriber-count problems do not silently become zero. Missing, zero,
negative, nonfinite, and implausibly large values receive distinct audit
codes and are handled only through the frozen fallback hierarchy in §7.

### 4.3 Account-post key

The cleaned endpoint observation key is:

```text
(url, user_name, post_created_date)
```

Rules:

- same URL + same username + same date = repeated extraction snapshots of
  one endpoint attribution; collapse to one row;
- same URL + different username + same date = joint/co-attributed post;
  preserve every username;
- never merge identities on display field `account`; it is neither unique nor
  stable;
- same URL + same username + conflicting date = quarantine the entire URL
  pending source adjudication;
- same URL + different usernames + conflicting dates = quarantine the entire
  URL pending source adjudication.

The known single date-conflict URL stays quarantined. No generalized date
tolerance is introduced from one anomaly.

### 4.4 Snapshot collapse

Within each account-post key:

- `total_interactions_assignment = max(total_interactions)`;
- retain `snapshot_count`, interaction minimum/maximum, and disagreement
  flags as insurance columns;
- if a trustworthy extraction timestamp exists, use subscriber count from
  the latest snapshot;
- if no extraction timestamp exists and subscriber counts disagree, use the
  median nonmissing subscriber count for that account-post assignment and
  retain the minimum, maximum, and number of distinct values;
- if all subscriber values are missing or invalid, invoke the fallback
  hierarchy in §7 rather than inventing a value.

Using the median subscriber count when ordering is unavailable prevents
interaction-max selection from also selecting the allocation weight.

### 4.5 Content-level interaction total

For each URL/date content item:

```text
Y_p = max(total_interactions_assignment across participating accounts)
```

The maximum is used because collaborator rows are near-contemporaneous
snapshots of one shared counter and differ slightly in a minority of cases.
The build must report:

- fraction exactly equal across collaborators;
- absolute and relative interaction ranges;
- interaction mass affected by disagreement;
- results by collaborator count and calendar month.

This convention is fixed before any allocated panel is scored.

## 5. Subscriber-field audit: first decision gate

Subscriber count is the allocation instrument. Its semantics must be audited
before fitting the allocation model.

### 5.1 Completeness and validity

Report over all cleaned account-post assignments and separately for joint
posts:

- null rate;
- zero rate;
- negative/nonfinite rate;
- number of unique values per account;
- distribution by month and post type;
- interaction mass and account count affected by invalid values;
- rates within the registered IG modeled-universe union.

### 5.2 Temporal semantics

For accounts observed repeatedly:

- plot and summarize subscriber count against post date;
- measure the fraction of within-account changes that are zero, positive, and
  negative;
- identify discontinuities and implausible resets;
- test whether historical posts all carry one extraction-date subscriber
  snapshot or a plausibly time-varying value;
- compare duplicate snapshots of the same account-post;
- inspect a stratified sample from the head, middle, and tail.

The field need not be monotone—real subscriber counts can fall—but it must
contain defensible relative audience information. A field that is constant at
a later extraction snapshot for all historical posts must be labeled as such
and cannot support claims of post-time audience measurement.

### 5.3 Subscriber-audit stop rule

Stop and return to the owner before allocation if:

- the field cannot be tied to any interpretable audience snapshot;
- invalid/missing values are concentrated enough that the fallback would
  determine a material fraction of joint-post interaction mass;
- the subscriber values exhibit source discontinuities that cannot be
  segmented from metadata alone.

No downstream model result can override this stop.

## 6. Expected-interaction model

### 6.1 Training population

Train only on cleaned **non-joint** posts: URLs attributed to exactly one
`user_name` after snapshot collapse.

Joint-post outcomes never train either the subscriber curve or the account
effect. This prevents allocated outcomes from recursively determining future
allocations.

### 6.2 Baseline subscriber curve

The cold-start model is a monotone, overdispersion-aware smoother:

\[
Y_{ip} \sim \operatorname{NegBin}(\mu_{ip}, \theta),
\qquad
\log \mu_{ip} = f\!\left(\log(1+S_{ip})\right).
\]

Requirements for `f`:

- monotone nondecreasing in subscriber count;
- fitted on the interaction mean, not merely the median;
- accommodates zero-interaction posts;
- regularized so sparse subscriber ranges cannot oscillate;
- extrapolates as a linear continuation of the boundary slope, with the
  slope capped at the fitted boundary rather than allowed to explode;
- returns a strictly positive finite expected-interaction score.

Recommended implementation: a shape-constrained penalized spline under a
negative-binomial/quasi-Poisson mean model. The smoothing penalty is selected
only through held-out solo-post prediction or pseudo-joint recovery, never
through rank-model performance.

If the full-row fit is not memory-safe, an equivalent streamed fit may use
fixed subscriber bins and sufficient statistics, provided a synthetic test
shows it recovers the same curve as the row-level implementation within a
predeclared numerical tolerance.

### 6.3 Strict time ordering and the cold-start week

The modeled 2023 panel begins with the complete week starting Monday
2023-01-02. The excluded boundary day 2023-01-01 is therefore the preferred
cold-start calibration block.

Execution order:

1. Audit 2023-01-01 instrument health.
2. Fit the initial subscriber curve on cleaned, non-joint posts from
   2023-01-01 only.
3. Use that curve for every account entering the first complete week.
4. At the start of each later Monday week, update the global subscriber curve
   using non-joint posts from completed prior dates only.
5. Freeze the curve for the entire Monday-Sunday week.

If the boundary day is unusable or too sparse, stop and register an alternate
cold-start source. Do not silently train on later 2023 outcomes.

### 6.4 Empirical-Bayes account effect

The subscriber curve supplies the prior mean. An account-specific random
effect captures stable engagement efficiency beyond subscriber count:

\[
\log q_{i,t^-}
= f_{t^-}\!\left(\log(1+S_{it})\right) + \widehat{a}_{i,t^-},
\qquad
a_i \sim N(0,\tau^2).
\]

Rules:

- estimate `a_i` only from the account's non-joint posts strictly before the
  Monday of the target week;
- use all available prior solo-post history with empirical-Bayes shrinkage;
- estimate the prior variance and observation variance from the solo-post
  training data, not from model scores;
- at zero prior solo posts, `a_i = 0` exactly;
- as evidence accumulates, posterior shrinkage moves continuously from the
  subscriber prior toward the account's measured engagement efficiency;
- joint-post allocations never update `a_i`;
- the account effect is frozen within each week.

The implementation must expose:

- number of prior solo posts;
- shrinkage weight;
- posterior account effect;
- cold-start indicator;
- fallback indicator.

### 6.5 Why weekly freezing is required

Updating allocation weights after every target post would let very recent
realizations feed directly into the current rank path. Weekly freezing:

- aligns the allocation chronology with the weekly stochastic model;
- prevents within-week feedback;
- makes look-ahead tests mechanical;
- keeps the measurement model parsimonious.

## 7. Missing-subscriber fallback hierarchy

For each collaborator, compute `q_i` using the first available rule:

1. valid current subscriber count + prior account effect;
2. if current subscriber count is missing but the account has at least one
   valid prior solo post and a finite shrunk posterior, use its posterior
   expected-interaction level;
3. if neither exists, use the global median cold-start score from the frozen
   subscriber curve for that week;
4. if every collaborator receives the identical fallback score, the result
   is an equal split.

Every fallback is recorded. The build reports fallback rates by:

- URL;
- account-post attribution;
- allocated interaction mass;
- permanent-rank band after the panel is constructed.

No missing subscriber count is imputed from the target joint post's
interaction total.

## 8. Integer-conserving allocation

`total_interactions` is integer-valued. To preserve exact daily-weekly
reconciliation, allocate integers by the largest-remainder method:

1. compute real-valued shares `r_i = Y_p w_i`;
2. assign `b_i = floor(r_i)`;
3. compute remainder `R = Y_p - sum(b_i)`;
4. add one interaction to the `R` largest fractional remainders;
5. break exact ties lexicographically by `user_name`.

Properties:

- allocated values are nonnegative integers;
- the allocation is deterministic;
- the content total is preserved exactly;
- rounding error is less than one interaction per collaborator;
- low-interaction collaborators may receive zero interactions while still
  retaining `n_posts = 1` and presence.

Retain both the real-valued expected share and integer allocation in the
raw-scoped intermediate; only the integer allocation enters the canonical
daily/weekly panel.

## 9. External validation using pseudo-joint posts

True collaborator-specific contributions are not observed. Validate the
allocation model on held-out solo posts whose individual outcomes are known.

### 9.1 Pseudo-joint construction

Construct groups of two to four solo posts matched on:

- calendar week;
- post type where feasible;
- collaborator-count distribution of real joint posts;
- subscriber-ratio distribution of real joint posts.

Do not match on realized interaction outcomes. Group construction may use
only variables that would be available before the pseudo-joint post.

For pseudo-group `g`:

```text
Y_g = sum of the observed solo-post interactions
true share_i = observed interactions_i / Y_g
```

Hide the individual outcomes from the allocation procedure and recover shares
using only frozen ex-ante information.

Groups with `Y_g = 0` test conservation and presence but are excluded from
share-error denominators.

### 9.2 Leakage control

- account-block the global subscriber-curve and hyperparameter fit so an
  evaluation account cannot determine its own population prior;
- allow an evaluation account's strictly earlier solo posts to update its own
  empirical-Bayes effect, exactly as they would in production;
- use forward weekly origins;
- fit every curve and account effect using data available before the
  pseudo-group week;
- keep all posts from one URL/account in one fold;
- seed and hash the pseudo-group construction.

### 9.3 Candidate allocation rules

Evaluate, in this order:

1. equal split;
2. subscriber-only smoother;
3. subscriber smoother + sequential empirical-Bayes account effect.

The raw-row/full-credit construction and global-URL/single-owner construction
are legacy/pathological references, not adoption candidates.

### 9.4 Allocation metrics

Report across all forward folds and by collaborator count:

- total-variation share error:
  `0.5 * sum_i |predicted_share_i - true_share_i|`;
- interaction-weighted absolute allocation error;
- largest-contributor identification accuracy;
- calibration by predicted-share decile;
- error by subscriber-ratio quartile;
- cold-start versus experienced-account error;
- error by month and post type;
- block-bootstrap intervals by calendar week.

No single favorable fold is reported as the result.

### 9.5 Adoption rule

The subscriber-only smoother is the required cold-start baseline. Add the
account-effect layer only if, across the full forward validation:

- it improves pooled total-variation error over subscriber-only allocation;
- its worst-subscriber-ratio-quartile error is no more than one
  block-bootstrap standard error worse than subscriber-only allocation;
- it is nonworse in at least four of five forward folds;
- predicted-share-decile means and observed-share-decile means have positive
  Spearman association.

Use the simplest candidate within one block-bootstrap standard error of the
best pooled total-variation score. If neither subscriber-based candidate
improves on equal splitting, stop and report the negative measurement result;
do not select a rule from downstream model behavior.

This is an allocation-model adoption decision, not a stochastic-model gate.

## 10. Sensitivity constructions

Build all of the following from the same cleaned account-post table:

1. **Primary candidate:** subscriber smoother plus adopted account-effect
   layer, integer-conserved;
2. **subscriber-only:** same chronology, no account random effect;
3. **equal split:** `1 / |C_p|` for every collaborator;
4. **legacy raw-row reconstruction:** reproduction only;
5. **global URL dedup:** pathological reference demonstrating collaborator
   deletion, not a scientific sensitivity.

The primary rule is chosen by §9 before any rank-diffusion run. The equal and
subscriber-only panels measure attribution uncertainty. The legacy panel
reproduces archived IG results but cannot validate the corrected data model.

## 11. Derived data products

URL-bearing outputs and scratch stay under:

```text
/Volumes/T9/rank-diffusion-data/raw_small/instagram/
```

Aggregate-safe outputs go under:

```text
/Volumes/T9/rank-diffusion-data/derived/
```

### 11.1 Raw-scoped cleaned account-post table

Proposed artifact:

```text
raw_small/instagram/ig_account_post_allocated_2023.parquet
```

Required columns:

- `url`;
- `date`;
- `user_name`;
- `post_type`;
- `n_collaborators`;
- `is_joint`;
- `snapshot_count`;
- `content_interactions_raw`;
- `subscriber_count_used`;
- `subscriber_source_code`;
- `expected_interactions_score`;
- `account_history_n`;
- `account_shrinkage_weight`;
- `cold_start`;
- `fallback_code`;
- `allocation_weight_real`;
- `metric_value_allocated`;
- interaction/subscriber disagreement diagnostics.

### 11.2 Aggregate-safe daily panel

Proposed artifact:

```text
derived/ig_daily_2023_allocated.parquet
```

Columns:

- `date`;
- `user_name` or canonical `endpoint_id` in the model-facing copy;
- `metric_value`;
- `n_posts`;
- `n_joint_posts`;
- `joint_metric_value`;
- `n_cold_start_posts`;
- `n_fallback_posts`.

### 11.3 Weekly panel

Proposed artifact:

```text
derived/ig_weekly_2023_allocated.parquet
```

It is generated only by summing the daily panel over complete Monday-Sunday
weeks. It is never built independently from raw posts.

### 11.4 Diagnostics and manifest

Produce:

- subscriber-field audit JSON/CSV;
- conflict and quarantine report;
- allocation-model fit summary;
- pseudo-joint validation table;
- allocation conservation report;
- panel reconciliation report;
- instrument-health/day-guard report;
- SHA-256 manifest covering raw input, code commit, allocation-model state,
  member files, and every derived artifact.

No URLs or post text leave `raw_small`.

## 12. Streaming and interruption safety

The corrected builder must preserve the A7 operational guarantees:

- bounded external aggregation;
- URL-hash partitioning for snapshot collapse and collaborator grouping;
- account-hash repartitioning for daily aggregation;
- raw-scoped scratch only;
- refuse nonempty stale scratch;
- `.staging` output sibling;
- writer closed in `finally`;
- atomic rename only after every invariant passes;
- forced-failure tests leave any prior official output byte-identical;
- no partial canonical panel after SIGKILL or exception.

The subscriber model must be serialized with:

- training cutoff date;
- spline/curve representation;
- dispersion and shrinkage parameters;
- software versions;
- code commit;
- training-data hash or sufficient-statistic hash.

## 13. Mechanical invariants and adversarial tests

### 13.1 Row and identity invariants

- no duplicate cleaned `(url, user_name, date)` key;
- same URL/different username survives as separate attributions;
- same URL/same username repeated snapshots collapse once;
- display-account equality never merges usernames;
- conflicting dates quarantine the URL;
- null/empty URL fails;
- null username/date fails.

### 13.2 Allocation invariants

- solo post receives weight 1 and the complete interaction total;
- joint-post weights are finite, nonnegative, and sum to one;
- integer allocations sum exactly to the content total;
- deterministic tie resolution;
- a 9:1 expected-interaction ratio yields the corresponding allocation up to
  integer rounding;
- missing-subscriber fallback follows the declared hierarchy;
- joint outcomes never enter a later account effect;
- no post or account uses information after its target week cutoff;
- cold-start accounts have account effect exactly zero.

### 13.3 Presence invariants

- every collaborator receives `n_posts = 1` per joint post;
- zero-interaction posts remain present with `metric_value = 0`;
- snapshot duplicates do not increment `n_posts`;
- P11 presence is derived from `n_posts`, never from positive interactions.

### 13.4 Panel invariants

- daily keys unique;
- weekly keys unique;
- dates timezone-naive;
- weekly dates Monday-stamped;
- all modeled metrics finite and nonnegative;
- weekly equals the sum of daily exactly for every component column;
- incomplete boundary weeks excluded explicitly;
- day-guard flags computed platform-wide before universe restriction;
- complete-week index consecutive;
- content-level total equals the total apportioned interaction mass over the
  included calendar window.

### 13.5 Synthetic failure cases

Tests must include:

- two-account joint post with unequal subscribers;
- four-account joint post;
- identical subscriber counts/equal split;
- zero-interaction joint post;
- one collaborator missing subscribers but with solo history;
- collaborator with neither subscribers nor history;
- duplicate snapshots with changing interaction counts;
- duplicate snapshots with changing subscriber counts;
- same display name but distinct usernames;
- the observed date-conflict pattern;
- forced allocation-model failure;
- forced Parquet-writer failure;
- stale scratch after simulated interruption;
- attempted future-data leakage.

## 14. Execution phases and stop points

### Phase A — register the measurement plan

1. Owner reviews this document.
2. Append a dated pointer/status note to `MODEL_STATUS.md` without rewriting
   §2z-ae.
3. Freeze the implementation branch/commit and raw input hash.
4. State explicitly that 2023 P10/P11 remain unadjudicated.

**Stop:** no allocation or model run before the plan is accepted.

### Phase B — subscriber and source audit

1. Run §5 subscriber audit.
2. Verify 2023-01-01 as the cold-start calibration block.
3. Measure missingness and fallback exposure.
4. Verify interaction/snapshot conventions.
5. Write an audit-only report.

**Stop:** any subscriber-semantic or instrument-era failure returns to the
owner before fitting.

### Phase C — implement and test the cleaned account-post layer

1. Implement bounded snapshot collapse and collaborator grouping.
2. Implement quarantine handling.
3. Add all §13 identity and interruption tests.
4. Generate no canonical daily/weekly output yet.

**Stop:** all synthetic and failure-mode tests must pass.

### Phase D — fit and validate the allocation model

1. Fit cold-start curve from the pre-panel boundary day.
2. Implement weekly sequential updates.
3. Implement empirical-Bayes account effects.
4. Run pseudo-joint forward validation once under §9.
5. Apply the §9.5 adoption rule.
6. Freeze the selected allocation estimator and serialize it.

**Stop:** a subscriber-based rule that fails external allocation recovery is
reported, not replaced based on rank results.

### Phase E — build candidate panels

1. Build the primary allocated account-post table.
2. Build aggregate-safe daily and weekly panels.
3. Build subscriber-only and equal-split sensitivities.
4. Run all conservation, reconciliation, and health gates.
5. Hash and manifest outputs.

**Stop:** any invariant failure leaves the canonical outputs unchanged.

### Phase F — data-level scientific characterization

Before model contact, report by construction arm:

- concentration curve and K90;
- ladder differences by rank;
- share of allocated joint interactions by rank band;
- endpoint membership overlap;
- zero-interaction presence rates;
- day/week coverage and dropout diagnostics;
- amount of raw-row inflation removed;
- sensitivity to equal and subscriber-only allocation.

The allocation rule is already frozen. These readouts cannot select it.

### Phase G — 2023 exploratory model sensitivity

Only after Phase F is complete:

1. create new membership files from the allocated full-population panel using
   the existing absence-penalized, train-only rule;
2. do not reuse the old membership hash as though the universe were
   unchanged;
3. rerun the P10/P11 machinery as explicitly **exploratory / technical
   sensitivity**, not as completion of the old preregistration;
4. run the primary, subscriber-only, and equal-split panels through the same
   frozen model stack;
5. never calibrate model parameters or allocation choices to IG;
6. report all arms, including failures, with equal prominence.

The old P10 hard thresholds may be displayed as historical references but do
not adjudicate the corrected 2023 analysis.

### Phase H — untouched confirmation

For an untouched future Instagram period:

1. register the input period and data source;
2. freeze the 2023-trained subscriber curve prior and sequential update rule;
3. freeze fallback rules, allocation sensitivities, universe construction,
   P10/P11 criteria, and run order;
4. process the new period exactly once;
5. distinguish measurement-model validation from stochastic-model validation;
6. report any intake or allocation failure as a data finding.

If 2024 is already value-viewed for this purpose, select another untouched
period rather than retroactively labeling it confirmatory.

## 15. Model-specific consequences to monitor

The allocation correction may affect the stochastic model through measurement
rather than dynamics:

- joint posts are disproportionately interaction-heavy, so allocation may
  change the head ladder materially;
- subscriber-weighted shares may reduce transient rank jumps relative to
  full-credit duplication;
- empirical-Bayes account effects are persistent and could mechanically
  stabilize ranks if future information leaks into them;
- integer rounding can create zero allocated interactions for low-share
  collaborators, while `n_posts` correctly preserves observation;
- joint allocation creates cross-account dependence within a post;
- Spec-B's per-entity daily floor may absorb allocation uncertainty unless
  joint-post shares and cold-start rates are reported by band;
- the train-safe universe and old recorded σ_obs curve may change because the
  underlying weekly metric changes.

Therefore the corrected analysis must report:

- joint-post share by permanent-rank band;
- cold-start/fallback share by band;
- P10 floor results with and without joint-post days as a diagnostic only;
- membership overlap with the legacy panel;
- movement and head statistics across all allocation sensitivities.

These diagnostics localize measurement effects; they do not authorize
allocation-rule selection.

## 16. Publication and claim language

The manuscript/SI treatment should state:

1. The preregistered 2023 daily build stopped on a data-construction
   assumption: global URL uniqueness was false for collaborative posts.
2. P10/P11 were not adjudicated under that registration.
3. The corrected estimator treats one collaborative post as multiple endpoint
   incidences but conserves its shared interaction total through ex-ante
   subscriber-based allocation.
4. Subscriber count supplies cold-start exposure; only earlier solo posts
   update account-specific engagement propensity.
5. The rule was selected through held-out pseudo-joint recovery, not through
   stochastic-model performance.
6. Corrected 2023 results are exploratory/technical sensitivities.
7. Any confirmatory claim comes only from a subsequently registered untouched
   period.
8. Instagram remains a censored, query-matching instrument and is never used
   to calibrate the stochastic model.

Avoid:

- “the original preregistration passed after correction”;
- “collaborator contributions are observed”;
- “subscriber weighting identifies causal audience contribution”;
- “the panel measures all Instagram attention”;
- selecting the allocation arm that produces the best rank score.

## 17. Deliverables checklist

- [ ] owner-reviewed measurement-plan commit;
- [ ] subscriber temporal-semantics audit;
- [ ] raw input and audit-artifact manifest;
- [ ] bounded account-post cleaner;
- [ ] subscriber smoother with serialized state;
- [ ] sequential empirical-Bayes updater;
- [ ] deterministic integer allocator;
- [ ] pseudo-joint generator and forward-validation report;
- [ ] dual-direction synthetic tests;
- [ ] interruption/atomic-output tests;
- [ ] primary allocated account-post table;
- [ ] daily and weekly allocated panels;
- [ ] subscriber-only and equal-split sensitivity panels;
- [ ] exact daily-to-weekly reconciliation report;
- [ ] instrument-health and day-guard report;
- [ ] new train-safe member files and hashes;
- [ ] 2023 exploratory P10/P11 sensitivity report;
- [ ] `MODEL_STATUS.md` dated result section after execution;
- [ ] untouched-period preregistration before confirmatory processing.

## 18. Definition of done

The 2023 correction is complete only when:

1. the subscriber field has a documented interpretation;
2. the allocation rule is frozen from external share-recovery evidence;
3. every joint post conserves its content-level interaction total exactly;
4. every collaborator remains represented as observed account-post presence;
5. repeated extraction snapshots are removed;
6. daily and weekly panels reconcile exactly;
7. all artifacts are deterministic, hash-pinned, and interruption-safe;
8. all allocation sensitivities are reported;
9. the corrected IG model runs are labeled exploratory;
10. no frozen model parameter, old result, or original preregistration claim
    is silently changed.
