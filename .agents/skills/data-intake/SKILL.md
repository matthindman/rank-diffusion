---
name: data-intake
description: "Procedure for acquiring, preserving, aggregating, validating and registering ranked-panel data for the rank-diffusion program, and for the first contact with any new dataset. Use when touching raw data or the WD/T9 drives; building, extending or validating a daily or weekly panel; auditing an instrument or its collection eras; defining a universe; writing the intake part of a protocol; or preparing the Reddit 2023–24 archive or Wikipedia pageviews for evaluation — including when the request only says 'load', 'peek at' or 'check' a file."
---

# Data intake

Data mistakes are the most expensive kind here. They contaminate every fit
downstream, and one of them cannot be undone at all: once values from a
sealed dataset have been read, that dataset can never again serve as
confirmation. So the order of operations matters more than speed.

## Before touching anything

1. Find the dataset in `llm_fitting/EXPOSURE_LEDGER.md`. Its status decides
   what you may do. If it is not listed, add a row for it as SEALED or NOT
   ACQUIRED before going further. Adding a row and logging contact are
   expected of every session; changing an existing status is the owner's.
2. If it is DEVELOPMENT or CONSUMED, work freely and log any new kind of
   contact.
3. If it is SEALED or EXPOSURE UNKNOWN, the only permitted operations are
   opaque ones: download, copy, checksum, list, read file sizes and parquet
   footer metadata. A row-oriented dump (zstd NDJSON) has no footer; its
   opaque checks are sizes, published hashes and a decompression test. Reading rows, or any statistic computed
   from rows, is value-level contact and waits for a committed protocol and
   the owner's go-ahead. "Just the top few names to sanity-check the file"
   is contact.

   The folders that hold such data are listed in
   `llm_fitting/SEALED_PATHS.json`, and each carries a `00_SEALED_README.txt`.
   By the owner's rule of 2026-10-11 nothing inside them is looked at until
   the owner says, in the conversation, that the named dataset may be
   opened. `scripts/sealed_data_guard.py` enforces the list: Claude Code
   runs it before every tool call, and `--check-command '…'` answers for any
   other agent. Its rule is that a command line which names a sealed folder,
   or can reach one, may contain only opaque operations and plain filters
   of their output (`ls … | sort | head`, `find … | xargs shasum`,
   `zstd -t`); an interpreter, an unknown program or an uncomputable
   variable on that line is refused, so run other work as a separate
   command and do arithmetic on sizes with `awk`. Commit messages and
   documents may name the paths. It reads the text of one command, so it
   cannot see a program that opens a sealed folder by a default path, or
   names saved to a file by one command and read by the next; the rule
   covers those cases even where the check does not. When the check blocks
   something, stop and ask; do not find another route. When sealed bytes are copied or moved, add the
   destination to the entry's `paths` first. To unseal after the owner's
   word: move the entry from `sealed` to `released` with the date and the
   owner's words, and log it in the ledger. Edits to the list, the guard and
   `.claude/settings.json` prompt the owner for approval.
4. Ground truth for what exists where: `DATA_INVENTORY.md` (drive map and
   copy policy), `DATA_PHASE2_REPORT.md` (pipeline runbook, validated
   outputs, resume commands), `llm_fitting/instrument_eras.py` (FB era table
   and day guard).

## The sequence for a new or extended dataset

1. **Acquire as opaque bytes.** Checksums and a manifest row for every
   file; source URL, version and retrieval date recorded. Log it in the
   ledger. Acquisition does not unseal.
2. **Exposure audit.** State who or what has already read values (a pilot,
   another project, a builder's smoke test). Record the result in the
   ledger. If contact occurred, say what claim survives.
3. **Write and commit the protocol** (contents in the stochastic-modeling
   skill's `evidence-standards.md`), with its runners built and tested on
   synthetic data. Never rehearse on the real files.
   A new builder or intake gate calls
   `sealed_data_guard.require_released(path)` on each input, so that it
   refuses to run while its dataset is still sealed.
4. **Run the fail-closed intake gate.** This is the first value-level
   contact. It checks schema, hygiene, calendar coverage and exact
   reconciliation, and it stops everything if any invariant fails.
5. **Instrument forensics**, as registered.
6. **Universe registration**, by the registered rule.
7. **Registered evaluations**, in order, one pass each.
8. **Record and re-status.** A dated MODEL_STATUS section; the ledger row
   becomes CONSUMED.

For DEVELOPMENT data, steps 4–6 are still the right order; the protocol and
one-pass constraints fall away.

## Drives and budgets

| location | holds | rules |
|---|---|---|
| WD "My Passport for Mac" (HFS+, 4.5 TiB, spinning) | archival raw: Pushshift `{RS,RC}_YYYY-MM.zst` 2018-12..2022-12, the Reddit bridge under `pushshift/bridge_2023-01_2024-06/`, CrowdTangle exports | Source of record. Nearly full: check `df` before writing. Raw Reddit stays here. Slow; retry checksum failures (a cable reseat has fixed transient mismatches). |
| T9 SSD `/Volumes/T9/rank-diffusion-data` (ExFAT) | `raw_small/`, `aggregates/`, `derived/`, `manifest/`, `logs/`, `wikipedia/` | Budget: project tree ≤ 600 GB and drive ≥ 40% free. Repo symlink `data/ssd` points here. |
| repo `data/raw/`, `data/reddit/` | small legacy panels | `fb_ranked_weekly_cutdown.parquet` is the FB keystone; `fb_ranked_weekly.parquet` is corrupt after 2022-06-27 and is never used. |

If a drive is not mounted, report it and stop that thread. Do not substitute
other data. Copy and deletion policy is the owner's and is recorded in
`DATA_INVENTORY.md` and MODEL_STATUS §2z-ag; nothing on the WD is
disposable. Every file placed on the SSD gets a `manifest/MANIFEST.csv` row
(source path, bytes, SHA-256, script, parameters, date).

## Panel schema contract

The weekly panel read by `minimal_rankdiff.load_panel` has `endpoint_id`,
`date` (Monday-stamped) and `metric_value` (raw, nonnegative). The loader
applies log1p, drops invalid rows, collapses duplicate `(date,
endpoint_id)` by max, and ranks within period. The daily panel read by
`spec_b_sigma_obs.load_daily` has `date`, `endpoint_id`, `metric_value`,
with weeks Monday-anchored.

Every new panel passes these before any model sees it:

- No duplicate `(date, endpoint_id)` keys; dates timezone-naive; weekly
  dates all Mondays.
- Column semantics declared per column: which are model inputs and must be
  nonnegative, which are signed audit fields. A blanket rule once failed on
  the frozen baseline itself (§2z-o, §2z-p).
- **Weekly equals the sum of daily, exactly,** per entity-week and for
  every component column, in both directions. Spec-B is valid only because
  of this.
- A week is complete only if all seven days are present. Incomplete weeks
  are excluded from weekly sums explicitly. A zero-row day file is a
  collection failure, not a platform zero.
- A smoke load through `load_panel`, and a look at the top of one week for
  recognizable names. On SEALED data this look happens inside the intake
  gate, not before it.

How aggregation is done, and why: stream, never materialize decompressed
dumps (multi-terabyte sources); make each month resumable and validate a
file before skipping it; add insurance columns at streaming time (maxima,
counts, score-distribution scalars), because re-streaming the raw is the
cost you most want to avoid; keep only aggregate-safe columns, no ids,
authors, text or URLs.

Intake gates are code with tests. A gate exits nonzero on any failed
invariant, holds bounded memory whatever the panel length, and ships with
adversarial tests that reproduce each way it could pass wrongly
(`check_long_panels.py`, `check_extension_panel.py` and their tests are the
pattern). Decide the observational unit from the data's semantics before
writing a dedup rule, and build any reconciliation target on that same unit
(§2z-ae).

## Instrument forensics

Run the health profile first: entities per day, entities per week, new ids
per week (`instrument_eras.py profile`). Then:

1. **Classify the instrument.** A census (Reddit dumps): coverage shares
   are platform statements, absence is below-floor activity, new-id inflow
   is smooth. A censored or fixed panel (CrowdTangle FB, IG): every coverage
   statement is "of tracked activity", absence is not behavior, and entry
   and boundary statistics are within-panel quantities. Known-mechanism
   censoring is a third case and can be modeled: pre-register the
   observation model and its predictions, then use a measurability-scoped
   universe through the unchanged pipeline (`ig_censoring_prereg.md` and
   §2z-c are the worked example). Coverage percentages are never compared
   across types.
2. **Segment by collection health, from metadata only.** Era boundaries
   come from the instrument's own change log and from collection-health
   series, never from model fit. Register the candidate boundaries, with
   the direction of the expected effect, before looking at outcomes. No
   window of any kind (membership, split, displacement horizon, filtered
   state) straddles a broken era. Weekly medians can look normal while
   weekly unions of entities explode; check unions and new-id spikes.
3. **Low-count-day guard.** A day below 0.60 of its trailing 28-day median
   is flagged. Daily and Spec-B estimation drop any week containing a
   flagged day; weekly fits keep it, because the common factor absorbs a
   platform-wide undercount and dropping interior weeks breaks consecutive
   pairs.
4. **Identity audit.** Choose the id by a keystone comparison against a
   trusted panel (join rate and metric correlation). Report the share of
   activity on entities whose key changed. Name-keyed ids churn, and a
   rename reads as an exit plus an entry.
5. **Capture and definition audit** for accumulating metrics: when was the
   value fixed relative to when the item was created, and did that change
   within the window?

## Universe registration

1. Compute concentration shares from the panel alone.
2. Fix K from those shares by a rule written beforehand (the smallest grid
   value reaching 90% is the usual rule), never from fit quality. For a
   system with a very long low-count tail, declare a cap or an alternative
   coverage target in the protocol.
3. Buffer B = 4K. Membership by absence-penalized permanent rank
   (`restrict_universe`), on the train window only inside the gate.
4. On long panels report membership sensitivity (`membership_robustness.py`)
   as a robustness dimension. It is not a knob.

## The two datasets being prepared

These notes orient the protocol writer. They are not findings; verify each
against the primary source before registering it.

**Reddit archive, 2023-01 to 2024-06 (SEALED).** Not a continuation of the
2018–2022 instrument: per MODEL_STATUS §2z-ah the source changes from
Pushshift to Arctic Shift at 2023-04, and score re-retrieval changes at
2023-11. Treat it as new instrument eras, never pooled with earlier years in
one estimate until forensics show continuity. Register source-transition and
score-timing forensics as the first evaluations, and the June 2023 API
protest as a dated shock. It lies between two DEVELOPMENT periods, so write
down in advance what the earlier and later panels predict for it.

**Wikipedia pageviews (2025-01 is EXPOSURE UNKNOWN; nothing else
acquired).** Points recorded in
`llm_fitting/lit_review/research_notes/Rank dynamics modeling best
practices/breadth_data_attention_dynamics.md`, to be checked against
Wikimedia's documentation: identity is stable only by page ID from the
dumps (the pilot's `endpoint_id` is the title); only the "user" agent class
is human traffic, and its definition has dated breaks; the 90% coverage
rule may give a very large K; traffic is event-driven, new articles enter
near the top, and articles rarely disappear, so the model's rebirth
machinery and its AR(1) burst decay are expected to misfit in ways the
protocol should predict.
