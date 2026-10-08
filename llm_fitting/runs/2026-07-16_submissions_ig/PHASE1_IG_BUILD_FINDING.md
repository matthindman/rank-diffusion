# Phase 1 — IG 2023 daily build: fail-closed data finding (2026-07-16)

**Outcome: BUILD FAIL (fail-closed, by design). The IG daily panel was NOT
built. P10 and P11 are recorded as NOT ADJUDICATED (their registered data
construction was not executable and encoded the wrong observational unit) —
NOT as scientific failures. No post-hoc tolerance decision is taken; any
corrected construction is an owner decision.**

## UPDATE (2026-07-16, after a full-file Codex census — supersedes the count
## and the "corrupt data" first read below)

The 41,256 was only the FIRST hash partition's share (the builder stops at the
first failing partition, ig_daily_2023.py:128). Full-file census (Codex,
artifacts on T9 `raw_small/instagram/_conflict_audit_20260716/`):

- 52,023,601 total urls; **659,028 (1.27%)** have conflicting identity,
  affecting 1,387,834 raw rows (2.60%) and ~9.37B interactions (10.58%).
- Conflicts are **username-only** (659,027 username-only; 1 date-only; 0
  both). 97.3% map to multiple display-account names; every affected url is a
  normal `/p/<shortcode>/` permalink with identical date/title and ~identical
  interactions (73% exactly equal, 99th-pct discrepancy 2%).
- Structure is strongly consistent with **Instagram Collab / co-authored
  posts** (one content item attributed to 2–4 accounts; growth over 2023
  matches the Aug-2023 Collab expansion). This is Codex's mechanism
  hypothesis; the operative, structurally-verified fact is one-content-item →
  multiple-endpoint-attributions.

### Two distinct issues (both real)

1. **Wrong observational unit.** The registered rule dedups globally on `url`
   keeping max interactions — conflating (a) repeated extraction snapshots of
   the SAME endpoint's post (should collapse) with (b) one collaborative post
   attributed to SEVERAL endpoints (should be retained). For a model of
   endpoint attention the unit is an account–post attribution
   `(user_name, url, date)`, not globally-unique content. Global url dedup
   would delete ~704,781 account–post attributions (~4.92B interactions,
   5.55%); within the modeled-universe union, 43.7% of accounts participate in
   ≥1 conflict.
2. **The reconciliation gate was infeasible regardless.** Codex reconstructed
   the analyzed weekly panel (`ig_hm_totals_ts.parquet`, the A2.1 target) and
   finds it was built by RAW-ROW aggregation with NO dedup (raw aggregation
   reproduces it to 57 differing cells / 0.00026%). A dedup'd daily-derived
   weekly can NEVER match a no-dedup weekly exactly, so the registered A2.1
   EXACT-reconciliation gate could not have passed under any correct build.
   (Codex reconstruction; archived diffs, not independently re-run here.)

Also: 1,789 raw-only weekly cells hold 3,160 real posts with ZERO
interactions — these are account PRESENCE and must not be read as instrument
absence in P11.

### Disposition

- P10/P11: **not adjudicated** under this prereg.
- A corrected construction (key on `(user_name, url, date)`; collapse
  snapshots keep-max; preserve same-url-different-username; never merge on
  display `account`; preserve zero-interaction posts; keep failing on null
  urls and on date-conflicts within `(user_name, url)`; quarantine the single
  date anomaly) is the scientifically defensible build — but it is a POST-
  Phase-0 change chosen from estimand/data-semantics (never from whether it
  improves P10/P11), so it is EXPLORATORY/sensitivity for 2023 and would need
  fresh preregistration before an untouched period (e.g. 2024) for
  confirmatory status. OWNER decision; not executed here.
- Submissions (P2–P9) is independent and proceeds.

---

## Original note (first partition only; superseded above)

## What fired

`ig_daily_2023.py build`, pass 2, A4.5 conflict guard (lines 125–130):

    BUILD FAIL: 41256 duplicate urls with conflicting (user_name, date)
    (A4.5 anomaly)

The registered build convention dedups posts on `url` alone, keeping max
`total_interactions` — which assumes a url uniquely identifies one
(`user_name`, `post_created_date`) post. For **41,256 urls** that assumption
is false: the same url maps to more than one `user_name` and/or more than one
`post_created_date`. The A4.5 guard (registered in Amendment 4 after the
tenth review, precisely to forbid silent zero-fill / silent collapse of such
collisions) treats this as a hard FAIL.

## Integrity of the stop

Interruption/atomic safety (A7.1) held: no `ig_daily_2023.parquet`, no
`.staging` leftover, url-bearing scratch cleaned. Nothing partial was
written; prior state intact.

## Why this is NOT auto-fixable here

- A build convention change (drop conflicting urls; key on
  `(url, user_name, date)`; pick a tie-break) would alter a FROZEN
  registered convention. Amendments are legal only BEFORE Phase 0; Phase 0
  has passed and value contact has begun, so any such change is a **post-hoc
  deviation**, explicitly forbidden by "no post-hoc tolerance decision"
  (A1.8 / A2.1). It is an OWNER decision, and a disclosed deviation if taken.
- IG is known-mechanism-censored data the program never calibrates to
  (§2z-c); resolving an identity collision by fiat is exactly the kind of
  discretion the freeze exists to prevent.

## Scope

- Blocks: **P10** (Spec-B for IG; the reconciliation gate can't run without a
  built daily panel) and **P11** (instrument-dropout; consumes the daily
  panel).
- Does NOT block: the entire **submissions backtest (P2–P9)** — an
  independent panel with no such issue.

## Not yet done (pending owner direction)

A characterization of the 41,256 (conflict in `user_name` vs `date` vs both;
fraction of total unique urls; whether they cluster) has NOT been run —
it requires re-scanning the 31 GB raw. Offered, not assumed.
