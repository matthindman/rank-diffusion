# Phase 2 Data Migration Report

Status: Facebook processing is complete. Reddit comments and submissions are fully aggregated for every available WD month (2018-12 through 2022-12), and the combined model-ready daily/weekly panels have passed validation and smoke loading. The 2023-01 through 2024-06 Reddit bridge is not present on the WD and remains an owner acquisition decision.

Run dates: 2026-07-03 through 2026-07-16

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

## Reddit Full Completion - 2026-07-16

This section supersedes the operational status in the earlier travel-pause section. The comments-only short panels remain preserved as historical analysis inputs.

Monthly aggregation completed for all WD Pushshift archives:

| type | months | coverage | monthly aggregate size |
|---|---:|---|---:|
| submissions | 49 | 2018-12 through 2022-12 | approximately 3.0 GB |
| comments | 49 | 2018-12 through 2022-12 | approximately 2.5 GB |

Submission processing log summary:

```text
source rows:       1,360,306,176
aggregate pairs:     118,585,558
successful months:            49
parse errors:                  0
months requiring retry:        0
```

The raw `RS_*.zst` and `RC_*.zst` files remain only on the WD. They were streamed directly and were never copied or decompressed onto T9.

Final combined panels:

| file | rows | bytes | date range |
|---|---:|---:|---|
| `/Volumes/T9/rank-diffusion-data/derived/reddit_daily_long.parquet` | 142,438,022 | 2,483,250,581 | 2018-12-01 through 2022-12-31 |
| `/Volumes/T9/rank-diffusion-data/derived/reddit_weekly_long.parquet` | 53,723,472 | 923,586,541 | 2018-12-03 through 2022-12-19 |
| `/Volumes/T9/rank-diffusion-data/derived/reddit_week_completeness.csv` | 214 weeks | 8.4 KB | 2018-11-26 through 2022-12-26 |

The daily panel retains all 1,492 recovered calendar days. The weekly panel contains the 212 complete Monday-through-Sunday weeks. Two boundary weeks were excluded: 2018-11-26 has only December 1-2, and 2022-12-26 has only December 26-31.

Validation results:

```text
schema matches existing daily:  true
schema matches existing weekly: true
daily duplicate keys:           0
weekly duplicate keys:          0
daily negative metric rows:     0
weekly negative metric rows:    0
daily timezone:                 None
weekly timezone:                None
weekly dates all Monday:        true

weekly sum left-only keys:      0
weekly sum right-only keys:     0
metric_value mismatches:        0
submission_karma mismatches:    0
comment_karma mismatches:       0
submission_count mismatches:    0
comment_count mismatches:       0
```

Both files smoke-loaded through `minimal_rankdiff.load_panel`:

```text
daily:  1,492 periods; mean 95,467.84 entities/period
weekly:   212 periods; mean 253,412.60 entities/period
```

Sample weekly leaders for 2020-01-06 were `memes`, `dankmemes`, `PewdiepieSubmissions`, `aww`, and `funny`. This is plausible for a submissions-karma activity metric; comments remain populated as separate insurance/model columns.

Manifest and regression verification:

```text
submission monthly manifest entries: 49
derived Reddit manifest entries:      3
files independently rehashed:        52
bytes independently rehashed:        6,744,720,699
SHA-256 mismatches:                   0
temporary files remaining:           0
pytest:                               133 passed in 6.69s
```

Final T9 capacity check:

```text
/Volumes/T9: 931 GiB total, 289 GiB used, 643 GiB available, 32% used
/Volumes/T9/rank-diffusion-data: 289 GB
Reddit aggregate directory: 5.5 GB
derived directory: 6.7 GB
```

The project remains below the 600 GB ceiling and T9 remains approximately 68% free. The only Reddit continuity gap between this panel and the existing 2024-07 through 2025-01 repository panel is 2023-01 through 2024-06; those 18 months are not present on the WD and were not downloaded during this phase.

Completion logs:

- `/Volumes/T9/rank-diffusion-data/logs/reddit_monthly_processing_log.csv`
- `/Volumes/T9/rank-diffusion-data/logs/reddit_monthly_aggregation.stdout.log`
- `/Volumes/T9/rank-diffusion-data/logs/reddit_full_validation.json`
- `/Volumes/T9/rank-diffusion-data/logs/reddit_full_pytest.log`

## Preservation audit started — 2026-10-08

Owner request: review both attached drives against the project records and
preserve on T9 all useful WD material needed for future work. This is a storage
and provenance operation, not a new panel build or scientific evaluation.

Read the project skills, MODEL_STATUS through §2z-af, the confirmation protocol,
the submissions/IG registration and its recorded disposition, the July 18 IG
measurement plan, and the October research agendas. Historical results and
frozen protocols remain unchanged.

The non-hidden research/ordinary-file inventory found 4,131 WD files totaling
4,166,925,464,679 bytes and 3,173 SSD files totaling 312,263,013,442 bytes, with
zero scan errors. macOS hidden service directories and dotfiles were excluded;
these counts are logical file bytes, not filesystem allocation. The SSD
inventory includes its Samsung launcher files. Both mounts and the local
`data/ssd` symlink resolve correctly.

The review confirmed 49 comments and 49 submissions monthly aggregates on T9
(2018-12 through 2022-12). The latest processing record for each of the 98
type/month pairs says `ok`, with zero parse errors. The stored full-panel
validation report is populated and records exact daily/weekly reconciliation;
that scientific validation was not rerun during this storage audit. The two
registered comments weekly/boundary files, absent from the main SSD manifest,
were independently rehashed against the confirmation restart archive and both
match their recorded SHA-256 values.

The July copy policy omitted the bulk CrowdTangle CSV exports by default.
For this owner's broader preservation request, those exports and remaining
consolidated/helper/backup files are included because their overlap with the
saved Parquets has not been proven. This does not assert that they contain new
dates or authorize analytical ingestion. In particular, an export filename
dated 2024 does not establish that its observations are from 2024.

Frozen transfer plan:

| disposition | files | bytes |
|---|---:|---:|
| New preservation copies | 842 | 199,032,451,889 |
| Existing raw copies to verify against recorded hashes | 1,178 | 242,076,327,863 |
| Retained only on WD under the existing archive policy | 2,111 | 3,725,816,684,927 |

New material goes under
`/Volumes/T9/rank-diffusion-data/raw_small/wd_preserved_20261008/`, preserving
WD-relative paths. This includes the remaining `crowdtangle/` exports,
`full_ig_tusedays.parquet` (source spelling retained), and the unpaired backfill
TSV `2024-01-23--2024-01-24_test.tsv` plus the backfill debug log. Files under
the source's `exclude/bad_data` tree keep that provenance; preservation never
promotes them to validated inputs. The complete per-file disposition is in
`manifest/drive_audit_20261008/source_disposition.csv` on T9.

Reddit raw archives/fallback fragments, the uncompressed Reddit duplicate,
paired backfill TSVs, images, and drive installers remain on WD. The paired-TSV
decision follows the project's existing duplicate policy, not a newly performed
row-by-row equivalence test. Raw Reddit remains necessary for future questions
about fields absent from the saved aggregates (for example retrieval-time
forensics); this transfer does not make WD disposable. The 2023-01..2024-06
Reddit bridge is still absent from the WD inventory.

T9's preexisting Wikipedia acquisition is inventoried, not analyzed: January
2025 raw/hourly files, tail-estimator samples, and scratch are present; no final
daily/weekly panel is present. `raw/2099-01` and `tmp/2099-01` contain no ordinary
files. Existing tail samples mean an untouched-data claim must be audited
separately before future confirmation work.

The transfer uses source-stream SHA-256, flushed staging output, an independent
destination reread, and rename only after a match. Existing destination conflicts
stop execution without overwriting either file. Each completed copy is added to
the main manifest and an append-only verification log. The pre-run manifest is
saved as `manifest/drive_audit_20261008/MANIFEST.before.csv`. After copying,
the runner checks all hash-bearing entries from that original manifest, including
Reddit aggregates and derived panels. The complete plan fits below 600 GB and
keeps over 40% of the entire T9 volume free; both limits are checked in code.

**Status at this entry: transfer in progress, not complete.** macOS reports T9
connected at USB 2 speed (480 Mb/s), while WD is at 5 Gb/s; the initial verified
copy rate is roughly 10–15 MB/s. The owner has been offered a safe stop before
changing the T9 cable. Sources are untouched. Do not unplug either drive during
the copy. Completion must be established from the verification log, not inferred
from this plan.

Validation before transfer: 185 pytest tests and 13 subtests passed (research
plus Python package suites), with the existing fixture dtype warning. Synthetic
copy checks passed for byte identity, retaining the source, checksum recording,
resume, and refusing an existing destination conflict. No model defaults or
protocol thresholds changed; the empirical legacy guard was not rerun.

Runner and exact invocation from the repository root:

```sh
/usr/bin/caffeinate -dims /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u scripts/data_wrangling/preserve_wd_sources_20261008.py /private/tmp/rankdiff-drive-audit-20261008 --execute
```

The inventory and plan are also preserved under T9's
`manifest/drive_audit_20261008/`, which can replace the temporary directory in
the command after checking any interrupted `.preservation-staging` file.
Live log during this invocation:
`/private/tmp/rankdiff-drive-audit-20261008/transfer.log`.


### 2026-10-08 cable-reseat recovery — transfer resumed

The owner reported that the T9 cord had been partly unplugged and reseated it.
The first worker stopped with `OSError: [Errno 5] Input/output error` while
writing a staging file. After remount, 43 completed verification records were
present (the pre-disconnect stdout had advanced farther; durable records, not
stdout progress, determine recovery). One 250,465,220-byte staging file was
moved intact to `raw_small/_interrupted_20261008/`, preserving its source-relative
path. No WD source or canonical SSD file was deleted or overwritten.

macOS now reports T9 at **10 Gb/s**, WD at **5 Gb/s**. The runner was restarted
with `--recheck-completed`, which rereads and hashes previously completed files
after the disconnect. Completed-file rechecks succeeded and copying continued
beyond the interruption; main-manifest rows and the verification log agree.
Initial resumed throughput was 6.31 GB newly copied/verified in approximately
1.6 minutes. This is a progress measurement, not a completion claim.

The interrupted log, reconnect record and revised runner snapshot are saved
under T9 `manifest/drive_audit_20261008/`; the supplemental
`REGISTERED_INPUTS.csv` records the two independently verified registered
comments inputs. The second full regression run passed: 185 tests and 13
subtests, with the same existing warning (before the reconnect-only flag).

Current command:

```sh
/usr/bin/caffeinate -dims /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u scripts/data_wrangling/preserve_wd_sources_20261008.py /private/tmp/rankdiff-drive-audit-20261008 --execute --recheck-completed
```

Current stdout log:
`/private/tmp/rankdiff-drive-audit-20261008/transfer-resumed.log`.
**Still in progress**: completion requires the runner's final success and the
per-file verification results, not an extrapolated transfer-time estimate.


### 2026-10-08 completion — preservation and checksum verification PASSED

This completion entry supersedes the IN PROGRESS operational status above.
The resumed worker exited successfully and printed `COMPLETE: copy and manifest
verification` at **18:49:43 Japan time (09:49:43 UTC)** on October 8, 2026.

| final check | result |
|---|---:|
| New files copied, source-stream SHA-256 = destination reread SHA-256 | 842 / 842 |
| New verified bytes | 199,032,451,889 |
| Previously manifested files independently rehashed | 1,293 / 1,293 |
| Previously manifested bytes reverified | 255,340,648,275 |
| Post-restart checksum failures | 0 |
| Main-manifest entries matching completed verification records | 2,135 / 2,135 |
| Selected WD source existence/size/mtime checks against initial inventory | 2,020 / 2,020 |
| Registered weekly/boundary inputs separately verified against run archive | 2 / 2 |
| Final existence, size, manifest-hash consistency audit issues | 0 |
| T9 used / free at final audit (decimal GB) | 512.31 / 487.86 |
| T9 free fraction | 48.78% |
| Below 600 GB and at least 40% free | PASS / PASS |
| WD sources removed | 0 |

The source-stat check is not a second full read of every WD source; the new
copies' source hashes were computed while streaming, and historical copies
were checked against their previously recorded source/destination hashes.
No integrity claim is made for the entire multi-terabyte WD archive or every
unmanifested Wikipedia scratch file. The task's selected data and prior main
manifest are the verified scope.

The failed first attempt remains disclosed and archived. Its one
250,465,220-byte interrupted staging artifact is retained in
`raw_small/_interrupted_20261008/`; it is not an input or an additional completed
copy. There are no remaining staging files in the new preservation destination.
The WD-only dispositions in the preceding inventory still apply: raw Reddit,
paired daily TSV duplicates and image archives remain there. Do not erase WD;
future raw-field forensics may still require it, and the original files serve
as a second copy.

Final machine-readable evidence lives under T9
`manifest/drive_audit_20261008/`: `completion_summary.json`,
`verification.jsonl`, `source_disposition.csv`, `REGISTERED_INPUTS.csv`,
`transfer_completed.log`, plus the initial inventories, pre-run manifest,
interruption/recovery evidence and runner snapshots. The task added no new
scientific results, adopted no model changes, and altered no frozen threshold,
specification or confirmation protocol.

Final post-recovery regression check: 185 tests and 13 subtests passed in
72.93 seconds; the same preexisting fixture dtype warning remains. No commit
was made; unrelated working-tree changes were preserved.
