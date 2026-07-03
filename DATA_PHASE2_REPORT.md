# Phase 2 Data Migration Report

Status: paused at a clean travel checkpoint. Facebook raw copy, aggregation, validation, and smoke-load are complete. Reddit comment aggregation is complete through 2021-06, the comments-only daily/weekly panels are built, and a draft comments model run has completed.

Run date: 2026-07-03

## Travel Pause Plan

The Reddit aggregation was stopped after the requested checkpoint: `comments_2021-06.parquet` was written and validated. The worker had begun reading `RC_2021-07.zst` and reached 10,000,000 rows, but no July output was written; July will rerun from the beginning on resume.

Owner priority update:

- Keep prioritizing Reddit comments. The model behavior with comments is the unknown to test.
- Do not pivot the travel-day run to submissions merely to produce submission coverage; the project already understands the submission-only fit.
- At pause time, preserve the largest clean prefix of completed `RC_*` monthly comment aggregates.

Completed closeout tasks:

- Captured completed Reddit comments coverage and the stop boundary from `/Volumes/T9/rank-diffusion-data/logs/reddit_monthly_aggregation.stdout.log`.
- Built a comments-focused short panel from the clean `RC_2018-12..RC_2021-06` prefix.
- Validated the comments daily/weekly pair, including exact Monday-week sums.
- Smoke-loaded the comments daily/weekly pair through `minimal_rankdiff.load_panel`.
- Ran a draft `minimal_rankdiff` model on the comments weekly panel without editing model code.
- Captured manifest counts and a final SSD size table.

Recommended comments-only resume command after return:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 scripts/data_wrangling/aggregate_reddit_monthly.py --wd-root "/Volumes/My Passport for Mac" --ssd-root /Volumes/T9/rank-diffusion-data --start 2021-07 --end 2022-12 --record-types comments --progress-interval 5000000
```

After comments are complete, run submissions separately if needed. The 2023-01..2024-06 Reddit bridge remains out of scope and requires an owner source decision.

## Locations

- Inventory: `DATA_INVENTORY.md`
- SSD root: `/Volumes/T9/rank-diffusion-data`
- Manifest: `/Volumes/T9/rank-diffusion-data/manifest/MANIFEST.csv`
- Repo symlink: `data/ssd -> /Volumes/T9/rank-diffusion-data` (local-only, excluded from git)

## Facebook Raw Copy

Approved CrowdTangle raw files were copied to `/Volumes/T9/rank-diffusion-data/raw_small/facebook` with source and destination SHA-256 verification:

- `crowdtangle_backfill/*_test.parquet`
- `crowdtangle/full_fb.parquet`
- `crowdtangle/full_fb_tuesdays.parquet`
- `crowdtangle/fb_leaderboard.parquet`
- `crowdtangle/add/*.csv`

Skipped by decision: TSV duplicates, image tarballs, and bulk CSV chunk trees except the two approved `crowdtangle/add/*.csv` files.

Copy notes:

- Several early files had transient checksum mismatches and succeeded on retry after the T9 cable was reseated.
- `crowdtangle/full_fb.parquet` (49.5 GB) copied and verified successfully:
  `a3f6ba76097e47a9cbcf6795a84bf5b4f33aeeb66f411e8e38dee14d1ec8f6ca`
- `crowdtangle/full_fb_tuesdays.parquet` copied and verified successfully:
  `6e0f99dfcb5d78e1def72c9c2564adb14898ff96e73062e385044e2c34a57e50`
- `crowdtangle/fb_leaderboard.parquet` had one retry; second attempt matched:
  `578e68d91d0a65ee369476d53835b1e8cc2881008c446b24f3b11296b47b50df`

## Facebook Aggregation

Generated files:

| file | rows | size |
|---|---:|---:|
| `/Volumes/T9/rank-diffusion-data/aggregates/facebook/fb_daily_aggregates.parquet` | 19,440,552 | 635 MB |
| `/Volumes/T9/rank-diffusion-data/derived/fb_daily.parquet` | 19,359,204 | 432 MB |
| `/Volumes/T9/rank-diffusion-data/derived/fb_weekly_rebuilt.parquet` | 7,068,123 | 191 MB |
| `/Volumes/T9/rank-diffusion-data/manifest/fb_day_sources.csv` | 1,227 days | 194 KB |
| `/Volumes/T9/rank-diffusion-data/manifest/fb_completeness.csv` | 1,227 days | 194 KB |
| `/Volumes/T9/rank-diffusion-data/manifest/fb_complete_week_coverage.csv` | 158 complete weeks | 3.4 KB |

Day source table:

```text
Complete days: 1191 / 1227
source_kind
backfill    1154
full_fb       37
              36
```

Patch policy applied:

- Backfill nonzero daily parquet files are primary.
- Zero-row days are treated as collection failures, not as platform zeros.
- 2023 missing/zero days are patched from `full_fb.parquet` when available.
- No day unions multiple sources.

## Facebook Keystone Validation

Trusted target: `data/raw/fb_ranked_weekly_cutdown.parquet`, validating from 2020-11-02 onward because 2020-10-26 is not fully matchable from a 2020-10-27 daily start.

ID bake-off:

| candidate | weeks | mean trusted join rate | min trusted join rate | mean metric correlation | min metric correlation |
|---|---:|---:|---:|---:|---:|
| `account.name` | 86 | 1.0 | 1.0 | 0.9999953131712196 | 0.9999330544112632 |
| `account.id` | 86 | 0.0 | 0.0 | NaN | NaN |
| `account.platformId` | 86 | 0.0 | 0.0 | NaN | NaN |

Winner: `account.name`.

Clean coverage:

- Clean complete daily span: 2020-10-27 through 2024-03-06, with missing/incomplete days explicitly excluded from weekly panels.
- Complete weekly span: 2020-11-02 through 2024-02-12.
- Complete weeks beyond 2022-06-27: 72.

## Facebook Verification

Validation command:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 scripts/data_wrangling/validate_fb_outputs.py
```

Key results:

```text
daily_rows: 19,359,204
daily_periods: 1,191
daily_mean_entities_per_period: 16,254.579345088161
daily_duplicate_keys: 0
daily_negative_metric_rows: 0
daily_date_tz: None

weekly_rows: 7,068,123
weekly_periods: 158
weekly_mean_entities_per_period: 44,734.95569620253
weekly_duplicate_keys: 0
weekly_negative_metric_rows: 0
weekly_date_tz: None
weekly_dates_all_monday: true

weekly_sum_compare_rows: 7,068,123
weekly_sum_left_only: 0
weekly_sum_right_only: 0
weekly_sum_metric_value_mismatches: 0
weekly_sum_like_count_mismatches: 0
weekly_sum_share_count_mismatches: 0
weekly_sum_comment_count_mismatches: 0
weekly_sum_love_count_mismatches: 0
weekly_sum_wow_count_mismatches: 0
weekly_sum_haha_count_mismatches: 0
weekly_sum_sad_count_mismatches: 0
weekly_sum_angry_count_mismatches: 0
weekly_sum_thankful_count_mismatches: 0
weekly_sum_care_count_mismatches: 0
weekly_sum_post_count_mismatches: 0
```

Sample week top pages for 2021-01-04:

| endpoint_id | metric_value |
|---|---:|
| Occupy Democrats | 20,833,016 |
| Fox News | 10,574,905 |
| Trending World by The Epoch Times | 10,566,081 |
| 9GAG | 8,670,753 |
| CNN | 8,241,679 |

Smoke-load through `minimal_rankdiff.load_panel`:

```text
fb_daily.parquet:
  rows: 19,359,204
  periods: 1,191
  mean_entities_per_period: 16,254.579345088161
  sample period: 827
  top-5: Basketball Forever, LADbible Australia, 9GAG, Dr. Venus Opal Reese, UNILAD

fb_weekly_rebuilt.parquet:
  rows: 7,068,123
  periods: 158
  mean_entities_per_period: 44,734.95569620253
  sample period: 138
  top-5: Bleacher Report Football, Screamin' Tiki Tattoo, Occupy Democrats, Sam Daily, LADbible
```

Repair note:

- The first generated `fb_daily.parquet` had corrupt snappy pages in `sad_count` and `angry_count`.
- The clean aggregate and weekly parquets were readable.
- `fb_daily.parquet` and `fb_weekly_rebuilt.parquet` were rebuilt from `/Volumes/T9/rank-diffusion-data/aggregates/facebook/fb_daily_aggregates.parquet` with `aggregate_fb.py --rebuild-derived-only --endpoint-winner account.name`.
- Full validation passed after rebuild.

## SSD Size Table

Current size table after Facebook outputs, Reddit comments monthly aggregates through 2021-06, and the comments short panels:

```text
197G  /Volumes/T9/rank-diffusion-data/raw_small
1.9G  /Volumes/T9/rank-diffusion-data/aggregates
1.5G  /Volumes/T9/rank-diffusion-data/derived
2.4M  /Volumes/T9/rank-diffusion-data/manifest
512K  /Volumes/T9/rank-diffusion-data/logs
```

`df -h` after the comments checkpoint:

```text
/Volumes/T9  Size 931Gi  Used 200Gi  Avail 731Gi  Capacity 22%
```

Budget check: currently well below 600 GB, with more than 40% free.

## Reddit Status

Reddit monthly aggregation was started first and is resumable per month. It was stopped after `RC_2021-06.zst` completed, per the owner’s travel-day priority change.

Completed comments monthly outputs:

- 31 files, `comments_2018-12.parquet` through `comments_2021-06.parquet`
- aggregate bytes: 1,377,535,341
- final completed month log line:
  `wrote /Volumes/T9/rank-diffusion-data/aggregates/reddit/monthly/comments/comments_2021-06.parquet rows=2,194,969 bytes=62,757,107 sha256=6f92a07f4e1e77ce84c2ce18cb7017eb81fa2817e319d1eaf0b72fdf28105fab`

No submissions monthly aggregates were processed in this travel-day pass, by decision: the immediate scientific unknown is the comments fit.

Generated comments-focused short panels:

| file | rows | size | date range |
|---|---:|---:|---|
| `/Volumes/T9/rank-diffusion-data/derived/reddit_comments_2018-12_2021-06_daily.parquet` | 47,307,511 | 701,712,771 bytes | 2018-12-01..2021-06-30 |
| `/Volumes/T9/rank-diffusion-data/derived/reddit_comments_2018-12_2021-06_weekly.parquet` | 14,099,317 | 220,272,950 bytes | 2018-11-26..2021-06-28 |
| `/Volumes/T9/rank-diffusion-data/manifest/reddit_comments_2018-12_2021-06_coverage.csv` | 31 months | 3.7 KB | 2018-12..2021-06 |

Build command:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 scripts/data_wrangling/build_reddit_comment_panels.py --start 2018-12 --end 2021-06 --require-complete
```

Validation command:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 scripts/data_wrangling/validate_reddit_comment_outputs.py --stem reddit_comments_2018-12_2021-06
```

Key validation results:

```text
daily_rows: 47,307,511
daily_periods: 943
daily_mean_entities_per_period: 50,167.03181336161
daily_duplicate_keys: 0
daily_negative_metric_rows: 0
daily_date_tz: None

weekly_rows: 14,099,317
weekly_periods: 136
weekly_mean_entities_per_period: 103,671.44852941176
weekly_duplicate_keys: 0
weekly_negative_metric_rows: 0
weekly_date_tz: None
weekly_dates_all_monday: true
weekly_submission_columns_all_zero: true

weekly_sum_compare_rows: 14,099,317
weekly_sum_left_only: 0
weekly_sum_right_only: 0
weekly_sum_metric_value_mismatches: 0
weekly_sum_submission_karma_mismatches: 0
weekly_sum_comment_karma_mismatches: 0
weekly_sum_submission_count_mismatches: 0
weekly_sum_comment_count_mismatches: 0
```

Sample weekly top subreddits:

```text
2020-01-06: AskReddit 26,139,704; AmItheAsshole 6,857,952; worldnews 6,640,269; nfl 5,712,293; politics 4,198,493
```

Smoke-load through `minimal_rankdiff.load_panel`:

```text
daily:
  rows: 47,307,511
  periods: 943
  mean_entities_per_period: 50,167.03181336161
  sample top-5: AskReddit, AmItheAsshole, politics, PublicFreakout, news

weekly:
  rows: 14,099,317
  periods: 136
  mean_entities_per_period: 103,671.44852941176
  sample top-5: AskReddit, AmItheAsshole, politics, memes, news
```

Coverage by fixed comment universe, measured as mean weekly share of `metric_value`:

| top K | mean weekly share |
|---:|---:|
| 1,000 | 0.8142 |
| 1,800 | 0.8784 |
| 2,500 | 0.9084 |
| 3,500 | 0.9342 |
| 5,000 | 0.9558 |
| 7,500 | 0.9736 |
| 10,000 | 0.9826 |
| 15,000 | 0.9909 |

Draft model command:

```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 scripts/data_wrangling/run_reddit_comments_draft_model.py --top-k 2500 --reps 1 --md-lags 6 --min-knot-entities 8
```

Draft model result:

```text
REDDIT_COMMENTS_SHORT  | periods=136 mean_N=9985 entities=10,000 top_k=25 universe=top-2500 (buffer B=10000) temper pool>=8 md6 t-tails
temperament: s = 0.822 (sigma_i spread p90/p10 = 2.87x)
MD partition: kappa(z) = 0.005..0.040 (top..tail, estimated -- hand-set kappa retired)
transitory tails: t_df = 4.5
v4.3-style score: 9/15
mean churn error: 0.026
factor sigma_F=0.145  N=9985
```

Notable fit diagnostics:

```text
VR2 emp=0.604 sim=0.758 diff=+0.154
VR4 emp=0.343 sim=0.587 diff=+0.244
VR8 emp=0.209 sim=0.487 diff=+0.278
VR13 emp=0.146 sim=0.392 diff=+0.246
ACF1 emp=-0.309 sim=-0.238 diff=+0.071
RACF13 emp=0.318 sim=0.369 diff=+0.051
coll20 emp=0.933 sim=0.933 diff=+0.000
outfluxK emp=0.076 sim=0.060 diff=-0.015
return4K emp=0.409 sim=0.420 diff=+0.011
```

Interpretation for next model run: comments are analyzable with the current draft machinery over the 2018-12..2021-06 short panel. Churn and persistence are usable in a first pass, but the variance-ratio block remains too persistent in simulation relative to empirical comments. The short panel is enough to iterate on comments-specific tuning without waiting for the full 2018-12..2022-12 run.
