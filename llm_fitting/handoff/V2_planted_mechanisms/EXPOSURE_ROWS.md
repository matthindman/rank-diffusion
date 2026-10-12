# V2 development-data contact

2026-10-12. Frozen structural parameter extraction only; no empirical
residual, scorecard or movement score was computed. The existing DEVELOPMENT
classification was checked before contact. The three files and their hashes
are also in the compact parameter manifests.

| Panel | Input | SHA-256 | Use |
|---|---|---|---|
| facebook_a | `data/ssd/derived/fb_weekly_rebuilt.parquet` | `deb56413f0b5cf24c590d87a84b735aa959ff535b9c3e027c2625f0682916701` | Complete frozen S4 parameters, including local-only initial/replacement ladders |
| reddit_comments | `data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet` | `b00ee41f7d2813e744b1dc9663a9a8df5c6d7c3dc07d24bbbb2cbf7fb8070041` | Complete frozen S4 parameters, including local-only initial/replacement ladders |
| reddit_submissions_long | `data/ssd/derived/reddit_weekly_long.parquet` | `57074a57077f9e66d5f24d70df736e33c6a54d9291c1197f5432d71e2f5c8259` | Complete frozen S4 parameters, including local-only initial/replacement ladders |

The legacy Facebook cutdown is read by the required health guard.
No daily Spec-B panel was read for V2 recovery. No SEALED folder contents
were read; the WD drive was not used. Canonical ledgers were not edited.
Synthetic seed outputs are generated evidence, not new empirical exposure.
