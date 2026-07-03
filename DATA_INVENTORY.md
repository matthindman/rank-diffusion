# Data Inventory: External Drives for Rank Diffusion

Phase: 1 inventory only. No source files were copied, moved, deleted, or processed.

Inventory date: 2026-07-03

## Drive map

| role | mount | device | format | size | free | notes |
|---|---:|---:|---:|---:|---:|---|
| WD archival source | `/Volumes/My Passport for Mac` | `/dev/disk10s2` | HFS+ | 4.5 TiB | 752 GiB | Western Digital `My Passport 260F`; spinning external drive |
| SSD target | `/Volumes/T9` | `/dev/disk11s1` | ExFAT | 931 GiB | 931 GiB | Samsung `PSSD T9`; currently only Samsung launcher files |

The source and target drives are unambiguous.

## Repo contracts read before drive inventory

- `README.md`: active model line uses Reddit `data/reddit/reddit_weekly.parquet` and `reddit_daily.parquet`; Facebook must use `data/raw/fb_ranked_weekly_cutdown.parquet`, not the corrupt post-2022 full weekly file.
- `llm_fitting/MODEL_STATUS.md` section 2b: top-coverage universe is the live estimand; Reddit K90 is 5,000 and FB K90 is 3,500.
- `llm_fitting/MODEL_STATUS.md` section 2e: Spec-B depends on exact daily-to-weekly summation; Facebook is blocked unless sub-weekly data exists.
- `llm_fitting/minimal_rankdiff.py::load_panel()`: weekly model contract is `endpoint_id`, `date`, `metric_value`, all raw nonnegative activity; loader applies `log1p`, drops invalid rows, collapses duplicate `(date, endpoint_id)` by max, and ranks within period.
- `llm_fitting/spec_b_sigma_obs.py::load_daily()`: daily contract is `date`, `endpoint_id`, `metric_value`; week is Monday anchored by subtracting `date.dt.weekday`.
- `archive/scripts/reddit/`: existing Pushshift pipeline streams `.zst` NDJSON without materializing decompressed data, but currently keeps only `score_sum`, `count`, and `nsfw_count`.

## WD top-level inventory

Structured read-only walk: 4,140 files, 4,170,183,298,171 bytes, zero scan errors.

| top-level path | files | bytes | role |
|---|---:|---:|---|
| `/Volumes/My Passport for Mac/crowdtangle_backfill` | 2,680 | 2,326,981,615,661 | CrowdTangle API-style daily backfill plus TSV duplicates and image tarballs |
| `/Volumes/My Passport for Mac/pushshift` | 609 | 1,416,129,372,955 | Pushshift raw `.zst` dumps plus December 2022 comments parquet fragments |
| `/Volumes/My Passport for Mac/crowdtangle` | 847 | 287,420,559,070 | Consolidated CrowdTangle parquet/TSV/CSV exports |
| `/Volumes/My Passport for Mac/reddit-uncompressed` | 1 | 139,650,513,046 | Uncompressed duplicate-ish `RS_2022-12.json` |

## Reddit Pushshift inventory

Primary raw source:

`/Volumes/My Passport for Mac/pushshift/raw/raw/{RS,RC}_YYYY-MM.zst`

| record type | files | month range | bytes |
|---|---:|---:|---:|
| submissions `RS_*.zst` | 49 | 2018-12 through 2022-12 | 385,191,595,119 |
| comments `RC_*.zst` | 49 | 2018-12 through 2022-12 | 1,004,903,223,889 |
| total `.zst` | 98 | 2018-12 through 2022-12 | 1,390,094,819,008 |

Coverage map:

| year | submissions | comments |
|---|---|---|
| 2018 | 12 | 12 |
| 2019 | 01 02 03 04 05 06 07 08 09 10 11 12 | 01 02 03 04 05 06 07 08 09 10 11 12 |
| 2020 | 01 02 03 04 05 06 07 08 09 10 11 12 | 01 02 03 04 05 06 07 08 09 10 11 12 |
| 2021 | 01 02 03 04 05 06 07 08 09 10 11 12 | 01 02 03 04 05 06 07 08 09 10 11 12 |
| 2022 | 01 02 03 04 05 06 07 08 09 10 11 12 | 01 02 03 04 05 06 07 08 09 10 11 12 |

Sampled actual dump rows:

- `RS_2018-12.zst` and `RS_2022-12.zst` contain `subreddit`, `score`, `created_utc`, and `over_18`.
- `RC_2018-12.zst` and `RC_2022-12.zst` contain `subreddit`, `score`, and `created_utc`; sampled comments did not expose `over_18`.
- Sample timestamps are UTC epoch seconds and fall in the filename month.

Other Reddit row-level files found:

| path | bytes | interpretation |
|---|---:|---|
| `/Volumes/My Passport for Mac/reddit-uncompressed/RS_2022-12.json` | 139,650,513,046 | uncompressed row-level submissions; duplicate source candidate, do not copy |
| `/Volumes/My Passport for Mac/pushshift/raw/raw/December-2022-comments` | 26,034,539,603 | 509 parquet fragments, row-level comments for 2022-11-30 and 2022-12-01..2022-12-31; duplicate/fallback candidate, do not copy by default |

Existing repo Reddit aggregate examples:

| file(s) | rows | bytes |
|---|---:|---:|
| `data/reddit/monthly/submissions_2024-07..2025-01.parquet` | 25,417,614 | 366.4 MB |
| `data/reddit/reddit_daily.parquet` | 15,693,228 | 238.4 MB |
| `data/reddit/reddit_weekly.parquet` | 6,016,948 | 91.8 MB |

Estimated full-history Reddit derived size:

- Current monthly submission aggregate is about 52 MB/month with `score_sum`, `count`, `nsfw_count`.
- Applying the current compression ratio to WD submissions gives about 2 GB for submission monthly aggregates.
- Comments are larger in raw bytes, but aggregate cardinality is bounded by subreddit-day, not row count. Conservative expectation with added insurance columns: 10-20 GB for monthly aggregates and final daily/weekly panels, likely below 30 GB.
- Raw `.zst` dumps are 1.39 TB and must remain on the WD drive.

Recommended Reddit kept aggregate columns:

- Submissions: `submission_karma_sum`, `submission_count`, `submission_karma_max`, `submission_nsfw_count`, plus cheap distribution scalars such as positive/negative/zero counts, min score, sum of squared scores, and sum/sum-square of `log1p(max(score, 0))`.
- Comments: `comment_karma_sum`, `comment_count`, `comment_karma_max`, plus the same score distribution scalars. Direct comment NSFW is not available in sampled comment rows; use `comment_nsfw_count = 0/null` unless a documented join/inference is added.
- Drop only row-level-only fields from derived aggregates: ids, authors, body/text, URLs, media, link metadata, flair, and other per-row content.

## CrowdTangle / Facebook inventory

Key finding: daily Facebook data exists, but the raw CrowdTangle area is not "small" if copied wholesale.

### Useful Facebook row-data candidates

| source | files | rows | bytes | date evidence | cadence verdict |
|---|---:|---:|---:|---|---|
| `/Volumes/My Passport for Mac/crowdtangle_backfill/*_test.parquet` | 1,172 | 154,366,108 | 153,681,868,128 | actual `date` stats: 2020-10-27 00:00:00 through 2024-03-06 23:59:58; all nonempty files stay inside their filename day window | daily, with gaps |
| `/Volumes/My Passport for Mac/crowdtangle/full_fb.parquet` | 1 | 108,422,786 | 49,502,751,913 | `post_created_date`: 2023-01-01 through 2023-12-31; 364 unique dates; missing 2023-06-09 | daily 2023 consolidated export |
| `/Volumes/My Passport for Mac/crowdtangle/full_fb_tuesdays.parquet` | 1 | 15,192,670 | 7,224,204,847 | 51 unique `post_created_date`s, 2023-01-03 through 2023-12-26; mostly 7-day gaps | weekly-ish Tuesday/Wednesday subset, not daily |
| `/Volumes/My Passport for Mac/crowdtangle/fb_leaderboard.parquet` | 1 | 8,455,741 | 325,568,208 | same 51-date Tuesday/Wednesday cadence | likely pre-aggregated leaderboard subset, not daily |

Backfill daily parquet details:

- All 1,172 files have the metric columns:
  `statistics.actual.likeCount`, `shareCount`, `commentCount`, `loveCount`, `wowCount`, `hahaCount`, `sadCount`, `angryCount`, `thankfulCount`, `careCount`.
- Identifier candidates: `account.id` is stable page id; `account.name` is display name; `account.platformId` is also present. Use `account.id` unless overlap validation shows the existing weekly `endpoint_id` uses another id.
- Metric candidate for `metric_value`: sum the actual interaction counts listed above, with `thankfulCount` included for compatibility even if usually zero. For old CSV/parquet exports this corresponds to `Total Interactions`.
- There are 55 missing calendar days in the 2020-10-27..2024-03-06 backfill filename range.
- There are 18 present daily parquet files with zero rows:
  2021-10-18, 2022-08-04, 2022-08-10, 2022-08-13, 2022-08-15, 2022-08-19, 2022-08-28, 2022-09-05, 2022-09-09, 2022-09-11, 2022-09-15, 2022-09-19, 2022-09-20, 2022-09-22, 2022-10-07, 2023-04-28, 2023-11-17, 2023-11-20.

Missing backfill days:

`2023-01-08`, `2023-04-21`, `2023-05-30`, `2023-06-19`, `2023-06-22`, `2023-07-05`, `2023-07-21`, `2023-08-11`, `2023-08-25`, `2023-08-28`, `2023-08-30`, `2023-09-02`, `2023-09-08`, `2023-09-13`, `2023-09-26`, `2023-09-28`, `2023-10-13`, `2023-10-17`, `2023-10-30`, `2023-11-02`, `2023-11-04`, `2023-11-08`, `2023-11-11`, `2023-11-13`, `2023-11-16`, `2023-11-21`, `2023-11-23`, `2023-11-26`, `2023-11-28`, `2023-12-06`, `2023-12-08`, `2023-12-11`, `2023-12-13`, `2023-12-15`, `2024-01-11`, `2024-01-13`, `2024-01-15`, `2024-01-16`, `2024-01-18`, `2024-01-20`, `2024-01-23`, `2024-01-25`, `2024-01-27`, `2024-01-29`, `2024-02-02`, `2024-02-09`, `2024-02-11`, `2024-02-19`, `2024-02-20`, `2024-02-22`, `2024-02-24`, `2024-02-27`, `2024-02-29`, `2024-03-01`, `2024-03-03`.

### Duplicate or non-model raw candidates

| bucket | files | bytes | recommendation |
|---|---:|---:|---|
| `crowdtangle_backfill/*_test.tsv` | 1,174 | 352,640,097,000 | TSV duplicates of daily parquet exports; do not copy unless a parquet read fails |
| `crowdtangle_backfill/*_images.tar.gz` | 328 | 1,817,401,852,294 | images/media, not needed for rank model; do not copy |
| `crowdtangle/root/*.csv` | 107 | 24,672,556,000 | old CrowdTangle export chunks; likely source for/contemporaneous with `full_fb.parquet`; do not copy initially |
| `crowdtangle/apr_2024/fb/fb/*.csv` | 364 | 88,317,503,000 | old CrowdTangle export chunks; sample rows contain 2023 posts; inspect only if needed to patch gaps |
| `crowdtangle/add/*.csv` | 2 | 280,993,000 | small additional old-schema export chunks; possible gap patch source |
| Instagram files under `crowdtangle` | many | at least 55.6 GB CSV plus 36.5 GB parquet/TSV | out of scope for current Facebook/Reddit goal |

### Trusted repo Facebook panels

| file | rows | dates | cadence | notes |
|---|---:|---:|---|---|
| `data/raw/fb_ranked_weekly_cutdown.parquet` | 1,263,987 | 2020-10-26..2022-06-27, 88 dates | every Monday | trusted validation target |
| `data/raw/fb_ranked_weekly.parquet` | 2,391,203 | 2020-10-26..2024-03-04, 176 dates | every Monday | known corrupt after 2022-06-27; do not validate against post-cutdown weeks |

## CrowdTangle cadence verdict

Facebook daily data is present and likely unlocks Spec-B-style sub-weekly work:

- `crowdtangle_backfill` provides one-day Facebook post windows from 2020-10-27 through 2024-03-06, verified by parquet `date` statistics.
- `full_fb.parquet` provides daily 2023 post data, verified by 364 distinct `post_created_date`s and one missing day.
- The daily series is not gap-free. Backfill has 55 missing days and 18 zero-row days. The 2023 consolidated file can probably patch many 2023 gaps, but it is missing 2023-06-09 itself. January-March 2024 gaps need further investigation.
- The Tuesday files and `fb_leaderboard.parquet` are not daily; they are useful for validation or legacy provenance, not the primary daily source.

## SSD budget projection

Hard project budget: SSD usage for this project must stay <= 600 GB, leaving >= 40% free.

Recommended Phase 2 fit:

| item | copy raw to SSD? | estimated SSD bytes |
|---|---:|---:|
| Reddit raw `.zst` dumps | no | 0 |
| Reddit monthly aggregates plus daily/weekly derived panels | yes, derived only | 10-30 GB |
| Facebook daily derived aggregates/panels | yes, derived only | likely < 10 GB |
| Facebook useful raw parquets (`backfill` + `full_fb` + weekly-ish helper parquets) | optional, only if owner accepts 210.7 GB copy | 210.7 GB |
| CrowdTangle TSV duplicates | no | 0 |
| CrowdTangle images | no | 0 |
| CrowdTangle CSV chunks | no by default | 0 |
| Manifest/checksums/scripts/logs | yes | < 1 GB |

Projected total:

- Derived-only plan: comfortably < 50 GB.
- With useful Facebook raw parquets copied: roughly 250 GB, still well below 600 GB.
- Copying all CrowdTangle raw artifacts would exceed 2 TB and does not fit.

## Recommended source to target mapping

Target root for Phase 2:

`/Volumes/T9/rank-diffusion-data/`

Recommended layout:

- `raw_small/`
  - Copy only small/trusted legacy panels and, if approved after this inventory pause, selected Facebook parquet raw files. Do not copy Reddit raw dumps, TSV duplicates, images, or bulk CSV chunks.
- `aggregates/reddit/monthly/{submissions,comments}/`
  - Stream WD `.zst` files directly into monthly per-subreddit-day aggregates with insurance columns.
- `aggregates/facebook/daily/`
  - Aggregate CrowdTangle post-level parquets directly into page-day panels with interaction component columns.
- `derived/`
  - `reddit_daily_long.parquet`
  - `reddit_weekly_long.parquet`
  - `fb_daily.parquet`
  - `fb_weekly_rebuilt.parquet`
- `manifest/`
  - `MANIFEST.csv` with every SSD file, source path(s), bytes, sha256 where copied, script, parameters, and production date.

## Phase 2 implementation notes

- Extend, do not replace, the existing Reddit streaming pipeline.
- Add a resumable process that validates each monthly parquet before skipping it.
- Add `karma_max` and cheap score distribution scalars while streaming; this is the right time to avoid ever re-streaming the multi-TB dumps.
- Keep Reddit raw row-level data on the WD drive only.
- For Facebook, build daily aggregates from post-level rows and then Monday weekly sums. Validate `metric_value` mapping empirically against `data/raw/fb_ranked_weekly_cutdown.parquet`.
- Create `data/ssd -> /Volumes/T9/rank-diffusion-data` symlink only in Phase 2/4 after target directories exist.

## Surprises and pause points

1. CrowdTangle is daily, but not small if treated as all raw artifacts. The useful Facebook parquet row data is manageable; the TSV/image/CSV bulk is not.
2. Facebook backfill is daily but gappy. It should still enable a much longer clean panel if gaps can be patched or explicitly handled.
3. Pushshift coverage on WD ends at 2022-12. The repo already has 2024-07..2025-01 Reddit aggregates from a later source/version, so full Reddit continuity still needs a documented bridge for 2023-01..2024-06 and 2025-02 onward if those months are required.
4. Comment dumps lack `over_18` in sampled rows. Comment NSFW counts cannot be populated directly from comments alone.
5. No `.rds` files were found in the WD inventory.

