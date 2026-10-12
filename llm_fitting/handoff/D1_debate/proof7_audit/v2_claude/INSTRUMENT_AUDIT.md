# Proof 7 — what the existing instruments observe (Stage B)

2026-10-12 · Claude Opus 5.5 · independent of Stage A's outcome.

## Verdict

**No existing development instrument supplies the observation design of Proof 7.** The
design needs four things at once: daily event counts, a true birth mark, a true
survival flag that does not depend on activity, and recorded zeros. Two instruments
carry genuine daily event counts (Reddit items created per subreddit-day; Facebook posts
per page-day). None carries a birth mark that is usable, none carries a survival flag,
and none records zeros. The entry law and the death rate that the theorem treats as
known inputs have not been measured for any platform, and the five features cannot
absorb an unknown entry mean. The number of eligible independent retained records is
therefore zero for every instrument, and no platform-specific precision is stated
anywhere in this audit.

What was audited: the Reddit panels of 2018-12 to 2022-12 and 2024-07 to 2025-01, the
Facebook CrowdTangle panels (Era A, with notes on the other eras), and the Instagram
2023 files. What was not: the Reddit bridge of 2023-01 to 2024-06 (SEALED) and the
Wikipedia tree (EXPOSURE UNKNOWN). Nothing inside either was listed, opened or read,
and nothing below is a statement about their contents.

## Sources

- Ledger and status: `llm_fitting/EXPOSURE_LEDGER.md` and `SEALED_PATHS.json` on `main`
  at `5384430`.
- Collection documents: `DATA_INVENTORY.md`, `DATA_PHASE2_REPORT.md`,
  `llm_fitting/instrument_eras.py` (docstring and era table).
- Builder code: `scripts/data_wrangling/aggregate_reddit_monthly.py`,
  `build_reddit_panels.py`, `build_reddit_comment_panels.py`, `aggregate_fb.py`.
- The record: MODEL_STATUS §1, the §2g addendum on instrument eras, §2z-c, §2z-o, §2z-p,
  §2z-aa, §2z-ac, §2z-ad, §2z-ae, §2z-ah, §2z-au, §2z-aw.
- One value-level check, declared in `DESIGN.md` before it was run and logged in
  `CONTACT_LOG.md`: parquet footers and three scalars per metric column
  (`instrument_scalars.py`, `instrument_scalars.json`).

Each statement below is labelled as a schema fact (S), a collection guarantee stated in
the documents (G), a diagnostic that was measured (D), or an assumption that has not
been tested (A).

## Inventory

### Reddit, Pushshift archive, 2018-12 to 2022-12; and the short 2024-07 to 2025-01 panel

| question | finding |
|---|---|
| What is measured each day | Three different things per subreddit and UTC day. (i) `submission_count`, `comment_count`: the number of archived items whose `created_utc` falls on that day (S: `aggregate_reddit_monthly.py` lines 181–196 date each item by `created_utc` and line 130 increments `count`). These are integer event counts dated by activity. (ii) `submission_karma`, `comment_karma`: the sum of item `score` as stored in the archive, a vote total captured at some time after creation. It is signed (D: in the 2019-06 comment aggregate the minimum is -7,583 and 0.19% of subreddit-days are negative; §2z-o found 10,634 negative cells in the registered baseline). (iii) `metric_value`, the quantity every fit in the record uses: karma clipped at zero (S: `build_reddit_panels.py` line 104, `build_reddit_comment_panels.py` line 69). Only (i) is an event count. The capture time of `score` relative to creation is not kept in the aggregates and is not established in the record for these years (for the sealed bridge, §2z-ah records a change of source and of score re-retrieval). |
| Endpoint and identifier | The subreddit name string (S). Its stability has not been audited in the record. |
| Entry | Only the first day with at least one archived item inside the window. Creation time of the subreddit is not in the aggregates and not held. The window is left-censored at 2018-12. |
| Death | Not observed. Only absence of items. The record measured that absences are overwhelmingly temporary: 95–98% of weekly absences return (§2z-aa), and 98.6% of departures from the top-K are crossings by endpoints still observed (§2z-ac). Inactivity is therefore a poor proxy for death, and survival to a boundary cannot be verified apart from activity. |
| Zeros | Not recorded. Every row has at least one item (D: minimum `count` is 1 in both 2019-06 aggregates; 1,135,132 and 1,617,577 rows). A zero is implied only for a subreddit known to exist on a day the archive covers completely. Existence is not known. Membership of any cohort defined from these files depends on the counts themselves. |
| Calendar, exposure, collection health | UTC days from the posting timestamp (S). Comments 2018-12 to 2021-06: no low-count day flagged in 943, counts of active subreddits growing from 31k to 71k per day (D: §2g addendum, `instrument_eras.py`). Weekly equals the sum of daily exactly (G: `DATA_PHASE2_REPORT.md`). Completeness of the archive itself against the platform has not been audited in the record: UNKNOWN. |
| Entry law in latent units | Not measured. |
| Death rate | Not measured; no lifetimes exist. |
| Eligible independent records | None under the theorem's cohort, which cannot be formed. For scale only: about 13k identifiers per week appear for the first time in the comment panel (D: §2g addendum). A first appearance is not a birth. |
| Types | `nsfw_count` for submissions only. |

### Facebook, CrowdTangle, Era A 2020-11-02 to 2022-06-27 (Era C usable with caution; Eras B, M, D restricted)

| question | finding |
|---|---|
| What is measured each day | Post-level rows. `metric_value` per page-day is the sum, over the posts the page created that day, of ten interaction totals as they stood at retrieval (S: `aggregate_fb.py` lines 26–35, 173, 180–189). It is an accumulating snapshot attributed to the posting date, not a count of interactions that happened that day. `post_count` is the number of retrieved posts created that day: an integer event count dated by posting (S). The raw schema carries an `updated` field per post, so capture time exists in the raw files; it is not carried into the panels (S, from the footer of one Era A file; no values read). |
| Endpoint and identifier | `account.name`, the display name, chosen because it alone joined to the trusted weekly panel (G: join rate 1.0 against 0 for `account.id` and `account.platformId`, `DATA_PHASE2_REPORT.md`). A name key reads a rename as an exit and an entry. Stable numeric identifiers exist in the raw schema (S). |
| Entry | None inside the window. The collection is a fixed list of about 14.5k pages whose enrolment was frozen from the start, about 8 new identifiers a week (D: §2g addendum). The raw schema has `account.pageCreatedDate` (S; values not read). That is creation of the page, which for a fixed list of established pages lies before the window, and it is not entry into the process the theorem samples. |
| Death | Not observed. "Absence is not behavior" on this instrument: absence means no post or a page the collector dropped (§2g addendum, item 4). |
| Zeros | Not recorded: every Era A row has at least one post (D: minimum `post_count` 1 over 7,364,451 page-days). A day file with no rows is a collection failure, not a platform zero (G: 18 such days, 55 missing days, `DATA_INVENTORY.md`); 15 further low-count days sit inside Era A (D). The list of tracked pages on a given day is not recorded, so zeros cannot be filled in from a registry. |
| Calendar, exposure | Every statement is "of tracked activity" (G). Five eras; no window may straddle Era B or M. |
| Entry law, death rate | Not measured. |
| Eligible independent records | Zero newborn records by construction. |
| Types | The raw schema has `account.pageCategory`, `account.accountType`, `account.pageAdminTopCountry`, `account.verified` and `account.subscriberCount` (S). None is in the panels. |

### Instagram, CrowdTangle, 2023

Query-censored collection (§2z-c). The raw weekly panel is a negative control and is
never calibrated to. The post-level file has `account`, `user_name`, `followers`,
`post_created` with date and time, `likes`, `comments`, `views` and
`total_interactions` (S, footer only; 53,473,688 rows), with no capture-time field and
no account-creation field. The registered daily build failed closed on the wrong
observational unit (§2z-ae). Interaction totals are snapshots. No births, no deaths, no
zeros, censoring by query. It fails every core requirement.

### Wikipedia pageviews and the Reddit bridge

Not audited. Both are sealed, and the brief excludes them.

## Assumptions of the theorem against the evidence

| assumption of Proof 7 | verdict | evidence | consequence |
|---|---|---|---|
| Daily observations are integer event counts | SUPPORTED BY INSTRUMENT for Reddit item counts and Facebook post counts (schema). CONTRADICTED for the modeled metrics: karma is signed and clipped; interactions are snapshot totals attributed to the posting day | builder code; `instrument_scalars.json` | The theorem can only ever concern the count columns, which no fit in the record has used |
| Counts are conditionally Poisson given the intensity, independent across days | MODEL ASSUMPTION ONLY | no test in the record; comments arrive in threads, posts in batches | Over-dispersion would bias every feature of the form `1 - (1 - eps)^C` |
| True birth-bin mark | UNKNOWN for Reddit (creation time not held). Not usable for Facebook (creation field exists; enrolment frozen). Absent for Instagram | above | The retained-birth cohort cannot be formed. First appearance is selection on the count process and is not a substitute |
| True survival flag through the boundary, independent of activity | UNKNOWN: no instrument records death | §2z-aa, §2z-ac show inactivity is mostly temporary | Lemma 1's phase law and retention probability have no empirical counterpart |
| All zeros retained | CONTRADICTED as recorded: no file has a zero row. Implied zeros need a registry that is not held | scalar check | Zero-heavy newborn records, the regime of Proposition 9, are exactly the ones that are missing |
| Known exposure and time units | Time units SUPPORTED for Reddit (UTC posting day). Exposure UNKNOWN for Reddit (archive completeness not audited), CONTRADICTED as a constant for Facebook (eras, collection failures, "of tracked activity") | era table | A change in coverage is indistinguishable from a change in intensity |
| Entry law known in latent log-intensity units | UNKNOWN: never measured | — | At the base point the five features see the entry mean, the OU variance and the OU rate through two quantities only, so an unknown entry mean leaves them unidentified (`SENSITIVITY.md`, post hoc check). The theorem's inputs cannot be replaced by estimates from the same features |
| Death rate known; death exponential and independent of state | UNKNOWN: no lifetime records. The available proxies (rank crossings, inactivity) are CONTRADICTED as measures of death | §2z-aa, §2z-ac | `lambda` enters the phase law, the retention factor and the rank-feedback column |
| Lasting part Brownian; transitory part a stationary Gaussian OU; constant coefficients | CONTRADICTED on the modeled weekly metric: changes are fat-tailed at every horizon from 1 to 52 weeks (§2z-au); an endpoint's volatility changes over time and Reddit has an endpoint-specific annual cycle (§2z-aw). UNKNOWN on daily event counts, which have not been examined | record | Gaussian moment identities such as (4.2) need not hold. Absence of a test on counts is not support |
| No common shocks; records independent | CONTRADICTED at panel level: a common per-period factor is estimated and removed in every fit (§1), and collection-wide low-count days exist (§2g addendum) | record | The iid bound of Theorem 2 does not apply to calendar-overlapping records. No effective sample size is offered here; the record's standard is an entity and week-block bootstrap |
| One type | CONTRADICTED on the modeled metric: a persistent endpoint amplitude with lognormal spread is part of the main law (§1) and drifts by era (§2z-q, §2z-s) | record | The five-parameter certificate does not cover a pooled heterogeneous population |
| Stationary population | CONTRADICTED: active subreddits per day more than doubled in the comment panel; parameters drift by era (PROGRAM_STATE, residual 3) | record | The renewal density and its rank function are defined for a stationary population only |
| Rank feedback through the latent rank of `X`, drift `c + d u(X)` | MODEL ASSUMPTION ONLY | — | Nothing in the instruments observes latent rank; observed rank mixes in the transitory part |
| Population is all endpoints | Not met by the panels in use: they are top-K by absence-penalized permanent rank (§2b) | record | Selection on the outcome the theorem models |

## How many records

The brief asks for genuinely eligible independent records under the declared cohort.
The honest count is zero on every instrument, because the cohort is not definable. Raw
counts of things that are not the cohort: about 14.4k pages a week on Facebook Era A;
31k–71k subreddits a day and about 13k first appearances a week in the Reddit comment
panel; 126k–357k subreddits a week in the long submissions panel (§2z-ae). Calendar
overlap and the common factor mean these are not independent, and I do not propose a
correction.

## The smallest things that would have to be added

Acquiring any of them is outside this task and is the owner's decision.

1. **A birth registry for a census instrument.** For Reddit, a table of subreddit
   creation times joined to the subreddit-day item counts already built, with zeros
   filled in from creation onward. This would supply the birth mark, the event counts
   and the zeros. It would not supply a survival flag, the entry law or the death rate.
2. **A survival flag, or an assumption that removes the need for one.** With the
   three-day design of `REPAIRS_AND_PROPOSALS_v1.md`, a record needs to survive four
   days from the start of its birth bin. If early mortality is small, treating every
   registered birth as retained mislabels at most a fraction `1 - exp(-4 lambda)` of
   records (3.9% at `lambda` = 0.01 per day). That is an added assumption with a
   computable contamination bound, not a measurement, and a theorem using it would be a
   new restricted claim.
3. **An observation model that does not take the entry law as known.** No registry
   fixes this. It needs features that difference out an endpoint's own level, which the
   five features do not (`SENSITIVITY.md`, last section).
4. **Or a different sampling design altogether:** established endpoints followed over
   fixed lags, which need stable identifiers and counts but no birth mark. This is a
   different theorem. The instruments come much closer to supplying it, and the record
   already measures lasting variance this way (§2z-bf).

Facebook cannot supply newborn cohorts under any addition, because enrolment was
frozen.

## Limits of this audit

Documentary, plus one bounded check. I did not read any raw post or comment, any value
of `account.pageCreatedDate`, or any file outside the eras the ledger marks usable. The
statement that Reddit names are stable identifiers is not established by anything I
read. Whether archive scores for 2018–2022 were captured at a fixed delay is not
established either; it would need the raw archives, which are on the unplugged drive.
