# Proof 7 audit, version 2 (Claude) — data contact log

For integration into `llm_fitting/EXPOSURE_LEDGER.md` on `main`. Written in the
ledger's columns. No dataset changes status. No sealed folder was listed, opened or
read: the Wikipedia tree on T9 was not named in any command, and the WD drive holding
the Reddit bridge is not mounted.

All contact was made by one script, `instrument_scalars.py`, run once (after one failed
start that read nothing) with the registered interpreter, plus three directory listings.
Scope was declared in `DESIGN.md` before the run. Output: `instrument_scalars.json`.

| date | session | dataset / period | contact | covered by |
|---|---|---|---|---|
| 2026-10-12 | Claude Opus 5.5 | `facebook_a` dailies — T9 `derived/fb_daily.parquet`, rows filtered to Era A 2020-11-02..2022-06-27 before any column was read | Footer schema and row count of the file. Columns `metric_value` and `post_count` read for the 7,364,451 Era A rows to compute three scalars each: share of zeros, share of non-integers, minimum. No row, name or date inspected. Rows outside Era A were dropped by a date filter inside the reader, and no statistic was computed from them. | DEVELOPMENT. Proof 7 audit v2, `DESIGN.md`; owner's request of 2026-10-12 |
| 2026-10-12 | Claude Opus 5.5 | Facebook raw post-level, one Era A day — T9 `raw_small/facebook/crowdtangle_backfill/2021-03-01--2021-03-02_test.parquet` | Footer only: 69 column names and the row count. No values read, including none from `account.pageCreatedDate`. | DEVELOPMENT (Era A source). Same |
| 2026-10-12 | Claude Opus 5.5 | Reddit subreddit-day aggregates, 2019-06 — T9 `aggregates/reddit/monthly/{comments,submissions}/…_2019-06.parquet` | Footer schema and row counts. Columns `count` and `score_sum` read to compute the three scalars and the share of negative values. No subreddit name inspected. | DEVELOPMENT (inside `reddit_comments` and `reddit_submissions_long`). Same |
| 2026-10-12 | Claude Opus 5.5 | Reddit derived daily panels — T9 `derived/reddit_daily_long.parquet`, `derived/reddit_comments_2018-12_2021-06_daily.parquet` | Footer only: column names and row counts. | DEVELOPMENT. Same |
| 2026-10-12 | Claude Opus 5.5 | Instagram post-level 2023 — T9 `raw_small/instagram/full_ig.parquet` | Footer only: column names and row count. | DEVELOPMENT (structural), as already recorded for this file. Same |
| 2026-10-12 | Claude Opus 5.5 | T9 `derived/`, `aggregates/`, `raw_small/` and two sub-folders | File names and sizes listed. | — |

Not contacted: any weekly panel; any Reddit file other than the two 2019-06 aggregates
and the two footers; Facebook Eras B, C, M, D; the Instagram weekly panels; the
Wikipedia tree; anything on the WD drive.
