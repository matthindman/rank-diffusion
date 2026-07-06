---
name: data-intake
description: "Procedure for acquiring, aggregating, validating, and registering ranked-panel data for the rank-diffusion program — new platforms, panel extensions, and drive-based (WD/T9) pipeline runs. Read before touching raw data, building panels, resuming aggregation, or defining a universe. Companion to the stochastic-modeling skill (which owns what happens AFTER a panel exists)."
---

# Data Intake — Rank-Diffusion Program

Data mistakes here are the most expensive kind: they contaminate every
downstream fit and can invalidate a registered protocol. Order of operations
matters — several steps are irreversible once model-relevant data has been
looked at.

## 0. Before anything

1. Read `.claude/skills/stochastic-modeling/SKILL.md` §2 (epistemic contract)
   and §4 (Workflow A) — this skill implements its intake steps in detail.
2. Read `llm_fitting/CONFIRMATION_PROTOCOL.md`. If the data you are about to
   touch is covered by a registered protocol (the comments 2021-07..2022-12
   extension IS), the protocol's §2–§3 are binding: run exactly its data
   construction and evaluations, nothing exploratory first. Amendments to a
   protocol are legal only as dated commits made strictly BEFORE any
   extension row is read — check `git log` on the protocol file to confirm
   amendments (A1, A2/E5, ...) are committed before you start.
3. Ground truth documents: `DATA_INVENTORY.md` (drive map, what exists where,
   what NOT to copy), `DATA_PHASE2_REPORT.md` (pipeline runbook, validated
   outputs, resume commands), `llm_fitting/instrument_eras.py` (canonical FB
   era table + day guard).

## 1. Drives, budgets, and what lives where

| location | what | rules |
|---|---|---|
| WD "My Passport for Mac" (`/Volumes/My Passport for Mac`, HFS+, 4.5 TiB) | archival raw: Pushshift `{RS,RC}_YYYY-MM.zst` (2018-12..2022-12), CrowdTangle backfill/exports | READ-ONLY source. Raw row-level data NEVER leaves it (1.39 TB Reddit alone). Spinning disk — slow, retry checksum failures (cable reseat has fixed transient mismatches) |
| T9 SSD (`/Volumes/T9/rank-diffusion-data`, ExFAT) | `raw_small/`, `aggregates/`, `derived/`, `manifest/`, `logs/` | HARD BUDGET ≤ 600 GB, keep ≥ 40% free. Repo symlink `data/ssd -> /Volumes/T9/rank-diffusion-data` (local-only, gitignored) |
| repo `data/raw/`, `data/reddit/` | small trusted legacy panels | `fb_ranked_weekly_cutdown.parquet` is the FB keystone; NEVER use `fb_ranked_weekly.parquet` (corrupt after 2022-06-27) |

If a needed drive is not mounted: **report it and stop that thread.** Do not
substitute other data; do not "start early" on protocol-covered data.

Copy policy (owner-decided, recorded in DATA_INVENTORY): derived aggregates
to SSD yes; Reddit raw dumps no; CrowdTangle TSV duplicates / image tarballs
/ bulk CSV chunk trees no. Every SSD file gets a `manifest/MANIFEST.csv` row
(source path, bytes, sha256 where copied, script, parameters, date).

## 2. Panel schema contract (the model's interface)

Weekly panel (`minimal_rankdiff.load_panel`): columns `endpoint_id`, `date`
(Monday-stamped), `metric_value` (raw nonnegative activity). The loader
applies log1p, drops invalid rows, collapses duplicate `(date, endpoint_id)`
by max, ranks within period. Daily panel (`spec_b_sigma_obs.load_daily`):
`date`, `endpoint_id`, `metric_value`; weeks are Monday-anchored by
subtracting `date.dt.weekday`.

Hard invariants every new panel must pass before ANY model use:
- zero duplicate `(date, endpoint_id)` keys; zero negative metric rows;
  tz-naive dates; weekly dates all Mondays.
- **weekly = Σ daily EXACTLY** (per entity-week, every component column) —
  this is what makes Spec-B (daily noise floor) valid. Follow the
  `validate_fb_outputs.py` / `validate_reddit_comment_outputs.py` pattern:
  every metric column compared, mismatches = 0, left/right-only = 0.
- Incomplete/missing days are EXCLUDED from weekly sums explicitly (a week is
  complete only if all 7 days are present); zero-row day files are collection
  failures, NOT platform zeros.
- Smoke-load through `load_panel` and eyeball the top-5 of a sample week
  (they should be recognizable names — e.g. AskReddit for Reddit comments,
  Occupy Democrats/Fox News for 2020-era FB).

Aggregation principles (from the Phase-2 build): stream, never materialize
decompressed dumps; resumable per month with per-file validation before
skipping; add "insurance columns" (karma_max, score-distribution scalars,
count decompositions) at streaming time — you never want to re-stream
multi-TB raw; keep only aggregate-safe columns (no ids/authors/text/URLs).

Known resume command (comments extension, protocol-covered):
```bash
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 \
  scripts/data_wrangling/aggregate_reddit_monthly.py \
  --wd-root "/Volumes/My Passport for Mac" \
  --ssd-root /Volumes/T9/rank-diffusion-data \
  --start 2021-07 --end 2022-12 --record-types comments \
  --progress-interval 5000000
```
Then `build_reddit_comment_panels.py --start 2018-12 --end 2022-12
--require-complete` and the matching validator. Scripts live in
`scripts/data_wrangling/`.

## 3. Instrument forensics (before any fitting)

Run the health profile FIRST on every new/extended panel:
pages(entities)/day, pages/week, new-ids/week — the
`instrument_eras.py profile` pattern. Then:

1. **Classify the instrument: census or censored sample.**
   - Census (Pushshift Reddit): coverage shares are platform-wide
     statements; absence = below-floor activity; day-guard should flag ~0
     days; organic new-id inflow is smooth.
   - Censored/fixed panel (CrowdTangle FB, IG): all coverage language is
     "of tracked activity"; ABSENCE IS NOT BEHAVIOR; entry/boundary metrics
     are within-panel quantities; check whether enrollment is frozen
     (new-ids/week ≈ 0 after week 1 on FB).
   - Never compare coverage percentages across the two types.
2. **Segment by collection health — breakpoints from metadata ONLY, never
   from model fit.** FB eras (canonical, `instrument_eras.py`): A =
   2020-11-02..2022-06-27 PRIMARY; B = collapse, exclude; C = recovery,
   replication-with-caution; M = 2023 two-source mixture, UNUSABLE as built;
   D = 2024, only 2 complete weeks, daily-stats only. NEVER let any window
   (membership, OOS split, displacement horizon, filtered-state init)
   straddle a broken era. Beware the M-type failure: weekly medians can look
   normal while weekly entity unions explode — check unions and new-id
   spikes, not just per-week counts.
3. **Low-count-day guard** (GUARD_FRAC 0.60 of trailing 28-day median):
   daily/Spec-B estimation DROPS every week containing a flagged day; weekly
   fits KEEP flagged weeks (declared: the common factor absorbs platform-wide
   undercounts; dropping interior weeks breaks consecutive-change pairs).
4. **Identity audit.** Choose the id column by keystone bake-off against a
   trusted panel (join rate + metric correlation — FB: `account.name` won
   with corr 0.99999; `account.id` joined at 0.0). Record known weaknesses:
   name-keyed ids churn (reads as exit+entry); IG `account` has ~2.4% dups →
   use `user_name`; Reddit dense/tied ranks are NOT unique 1..N.

## 4. Universe registration (before scoring anything)

1. Compute concentration shares from the panel alone (mean weekly share of
   `metric_value` by top-K, as in DATA_PHASE2_REPORT's coverage table).
2. **Pre-register K from those shares only** (K90 typical; owner has
   preferred fuller universes — comments run at K=12,500 ≈ 98.8%). Never
   pick K by fit quality.
3. Buffer B = 4×K; membership = absence-penalized permanent rank (absent
   weeks at floor N_t+1) via `restrict_universe`; train-window-only
   membership inside the OOS gate.
4. Long panels (T ≳ 100): report membership sensitivity
   (`membership_robustness.py`: full/half/trailing-60 overlaps) — a
   robustness dimension, not a selection knob.
5. On censored platforms, phrase K coverage as "of tracked activity".

## 5. First model contact (after 1–4 pass)

- If protocol-covered: run the registered evaluations E1..En EXACTLY, in
  order, and nothing else first. Failures are reported as confirmation
  evidence, never refit.
- If a brand-new platform: commit a mini-protocol FIRST (universe rule,
  stacks, parameter-transport bands, gate criterion), then a draft run at
  the standard short-panel stack (`--temperament --min-knot-entities 8
  --md-lags 6 --t-tails`, reps small) purely to confirm the plumbing, then
  the registered evaluations. The claim to test on new systems is amplitude
  collapse (s(h) flat, b ≈ 1) + OOS-at-par — not 15/15.
- Record everything in a dated MODEL_STATUS §2 section (see the
  model-status-authoring skill), including exact commands and validation
  output blocks.

## 6. Known open data items (owner-gated; do not work around)

- Comments extension 2021-07..2022-12: WD-gated, protocol-registered — the
  program's #1 priority.
- Reddit 2023-01..2024-06 bridge: needs an owner source decision (WD
  Pushshift ends 2022-12; repo has 2024-07..2025-01 from a later source).
- FB 2023 (era M): unusable until a single-source rebuild (owner decision).
- FB `account.id`-keyed rebuild: documented future fix for name churn.
- Submissions 2021-07..2022-12: run only after comments complete.
