# Exposure ledger — which data this program has seen

_Compiled 2026-10-11 from MODEL_STATUS.md, the registered protocols, the
platform registry in `minimal_rankdiff.py`, and the drives. The statuses
bind as written. The owner may correct an initial entry; otherwise the
ledger changes only by appended lines._

A dataset can serve as confirmation only once, and only if nothing was
learned from it beforehand. This file is the single place that says, for
every dataset and period, whether that is still possible. Check it before
opening any data file. Append a dated line whenever a session makes
value-level contact with data, or acquires data without contact.

## Status vocabulary

| status | meaning | what is allowed |
|---|---|---|
| **DEVELOPMENT** | Values have been fitted, scored or inspected. | Any exploratory or design work. Results are development evidence, never confirmation. |
| **CONSUMED** | A registered evaluation was run on it once; the verdict is in MODEL_STATUS. | As DEVELOPMENT from the consumption date. The registered verdict is never re-adjudicated. |
| **SEALED** | In hand, but no values have been read: only acquisition, checksums, file listings, schemas and row counts. | Opaque-byte handling only. First value-level contact is the registered intake gate of a protocol committed beforehand. |
| **EXPOSURE UNKNOWN** | Some contact happened outside this program's record. | Treat as SEALED until an exposure audit is recorded here; the audit decides whether an untouched-data claim survives. |
| **NOT ACQUIRED** | Not in hand. | Acquisition is owner-gated; acquiring does not unseal. |
| **RESTRICTED** | Usable only under a stated rule. | As stated in the row. |

"Value-level contact" means reading rows or any statistic computed from them
— a top-10 list, a concentration share, a histogram, a fit. Reading file
sizes, checksums, parquet footer metadata (schema, row counts) and builder
source is not contact, but record it anyway when it concerns SEALED data.
For row-oriented dumps such as zstd NDJSON there is no footer: the opaque
checks are sizes, checksums against the source's published hashes, and a
decompression integrity test. Seeing a schema there means parsing rows,
which is contact.

Any session adds a row for a dataset that is not listed, conservatively as
SEALED or NOT ACQUIRED, and appends to the contact log. Changing an existing
status — unsealing, or clearing EXPOSURE UNKNOWN — is the owner's decision.
An exposure audit examines provenance (listings, timestamps, the code and
logs of whatever touched the data), not values. Contact with part of a
dataset exposes at least that part; what remains usable as confirmation is
for the protocol and the owner to say.

## Ledger

### Facebook (CrowdTangle; censored sample — "of tracked activity")

| dataset | period | status | record |
|---|---|---|---|
| `facebook` — `data/raw/fb_ranked_weekly_cutdown.parquet` | 2020-10-26..2022-06-27, 88 weeks | DEVELOPMENT; also the legacy-guard panel | §2 onward |
| `facebook_a` — Era A weekly and dailies (`fb_weekly_rebuilt`, `fb_daily`) | 2020-11-02..2022-06-27, 86 complete weeks | DEVELOPMENT | §2g-X P1–P2 onward |
| `facebook_c` — Era C | 2022-10-01..2022-12-31, 12 complete weeks | DEVELOPMENT; replication with caution | §2g-X P4 |
| `facebook_d` — Era D | 2024-01-01..2024-03-06, 2 complete weeks | RESTRICTED: daily noise-floor statistics only | §2g-X P4; `instrument_eras.py` |
| Era B (collapse), Era M (2023 two-source mixture) | 2022-06-28..2022-09-30; 2023 | RESTRICTED: unusable as built; never bridged by any window | `instrument_eras.py` |
| `data/raw/fb_ranked_weekly.parquet` | — | RESTRICTED: corrupt after 2022-06-27; never used | §3; data-intake skill |

### Reddit (Pushshift / Arctic Shift; census)

| dataset | period | status | record |
|---|---|---|---|
| `reddit` — `data/reddit/reddit_weekly.parquet` (submission karma) | 2024-07-01..2025-01-20, 30 weeks | DEVELOPMENT | §2b–§2f onward |
| `reddit_comments` (comment karma) | 2018-12..2021-06, 136 weeks | DEVELOPMENT | §2g-X P3 onward |
| Comments extension (`reddit_comments_ext`, `…_ext_late`) | 2021-07..2022-12, 77 weeks | CONSUMED 2026-07-12 by the E1–E5 battery; DEVELOPMENT since | §2z-q; §2z-s onward; §2z-u |
| `reddit_submissions_long` (submission karma) | 2018-12..2022-12, 212 complete weeks | CONSUMED 2026-07-16/17 by PREREG_2026-07-16 as an outcome-unseen backtest; DEVELOPMENT since | §2z-ad, §2z-ae |
| **Reddit bridge** — raw comments and submissions on WD `pushshift/bridge_2023-01_2024-06/` | 2023-01..2024-06 | **SEALED.** Acquisition in progress as opaque bytes; no rows interpreted | §2z-ah, §2z-aj |

The bridge is not a continuation of the 2018–2022 instrument: per §2z-ah the
source is Pushshift through 2023-03 and Arctic Shift from 2023-04, with a
change in score re-retrieval from 2023-11. Source and score-timing forensics
come before any karma panel, and those forensics are themselves value-level
contact that a protocol has to cover.

### Instagram (CrowdTangle; query-censored)

| dataset | period | status | record |
|---|---|---|---|
| `instagram` — raw weekly top-50k | 2023, weeks 1–52 | RESTRICTED: negative control; never calibrated to | §4; §2z-c |
| `instagram_hm`, `instagram_hm_ts`, `instagram_pp` — the censoring-apparatus panels | 2023, weeks 1–52 | DEVELOPMENT, only through the §2z-c apparatus; estimand "of a-matching activity" | §2z-c, §2z-d, §2z-f |
| Post-level dailies — T9 `raw_small/instagram/full_ig.parquet` | 2023 | DEVELOPMENT (structural): schema, row counts, a full-file duplicate/identity census and a fail-closed build attempt. P10/P11 not adjudicated. Same year as the modeled weekly panel, so never confirmatory | §2z-ad, §2z-ae |
| Any 2024 Instagram data | 2024 | NOT ACQUIRED per the record | §2z-ae |

### Wikipedia pageviews

| dataset | period | status | record |
|---|---|---|---|
| English Wikipedia, hourly dumps and hourly aggregates on T9; no daily or weekly panel has been built | 2025-01 | **EXPOSURE UNKNOWN.** Built by the acquisition pilot on 2026-07-15 with no model contact; three daily sample files under `derived/tail_estimator_samples_2025-01` dated 2026-07-31 show values were read for another purpose. Audit required before any untouched-data claim about this month | §2z-ad; §2z-ag; commit b1f6ce3 |
| Every other month and every other language edition | — | NOT ACQUIRED | — |

The `2099-01` directories in the Wikipedia tree contain no ordinary files
(§2z-ag).

## Contact log (append below; newest last)

| date | session | dataset / period | contact | covered by |
|---|---|---|---|---|
| 2026-10-11 | Claude Opus 5.5 | all | Ledger compiled from records; date range and row counts read for `reddit` and `facebook`, both DEVELOPMENT; file listings only for the Wikipedia tree | — |
