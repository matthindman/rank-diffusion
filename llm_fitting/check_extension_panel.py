#!/usr/bin/env python3
"""A6.1/A7/A8 intake gate for the comments extension — FAIL-CLOSED.

Must print PASS before any model code touches the extension. Round-6 made
the gate's inputs required; round-7 (A8) closed the remaining false-PASS
paths: daily-only cells silently dropped by reindexing, a day guard with no
frozen-period history for the first extension week, a directory glob that
could not establish parse success, and boundary-day coverage stopping at
Dec 25. Every input is REQUIRED:

  python llm_fitting/check_extension_panel.py \
      EXT_WEEKLY FROZEN_WEEKLY EXT_DAILY FROZEN_DAILY COVERAGE_LOG PROCESSING_LOG

  1. SCHEMA EQUALITY: extended weekly columns == frozen weekly columns.
  2. FROZEN-PREFIX EQUALITY: every row with date <= 2021-06-28 equal to the
     frozen panel on every column — catches the July-1..4 fold-in (E2
     training period 135) that the date anchor cannot see.
  3. COMPLETE-WEEK WINDOW: extension weekly rows exactly the complete weeks
     2021-07-05 .. 2022-12-19 (77 weeks; extended T = 213).
  4. AGGREGATION LOGS (replaces the A6 directory glob, which could not see
     parse errors and passed 19 or zero-byte files):
     - builder coverage log: exactly ONE record per month
       2021-07..2022-12, status == "ok", rows > 0, bytes > 0, no duplicate
       or missing months;
     - aggregator PROCESSING log (round-8/A9: the coverage log has no
       parse-error field and the aggregator writes status="ok" even with
       errors > 0): for every month, the LATEST comments record must have
       status == "ok", lines > 0, output_bytes > 0, and **errors == 0** —
       the registered zero-parse-errors rule, enforced mechanically.
  5. Daily panel (REQUIRED): every frozen numeric metric column PRESENT
     (no silent intersection); no duplicate keys; A10 column semantics
     (see below) incl. the daily identity
     metric_value == max(comment_karma, 0); EVERY
     calendar day 2021-07-01 .. 2022-12-31 present (boundary days through
     year end included — they are registered as reported data);
     weekly == sum(daily) for EVERY frozen metric with EXACT (entity, week)
     INDEX-SET EQUALITY in both directions (a daily-only or weekly-only
     cell is a FAIL, not a reindex drop).
  6. Day guard on the extension days with FROZEN-PERIOD HISTORY: the count
     series is frozen daily counts + extension daily counts, flagged by
     instrument_eras.flag_days (trailing prior-days median), adjudicated on
     extension dates only — so July 1-7 are judged against the frozen
     baseline, not against themselves.

A10 (drafted 2026-07-12; MODEL_STATUS §2z-o/§2z-p — the blanket
non-negativity rule was INFEASIBLE, the frozen baseline itself fails it):
hygiene checks [3] and [5] use registered PER-COLUMN semantics
(comment_karma is a signed audit field the model never ingests;
metric_value = its daily positive part), with negative-cell rates and the
daily clipped-mass ratio printed as descriptive readouts that never gate.

Self-test mode (A10 validation requirement 1 — the dry-run whose absence
caused the §2z-o halt):

  python llm_fitting/check_extension_panel.py --frozen-self-test \
      FROZEN_WEEKLY FROZEN_DAILY

ANY failure => nonzero exit. Failures are data problems, not modeling
degrees of freedom (protocol §2).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from instrument_eras import flag_days  # noqa: E402

FROZEN_LAST_WEEK = "2021-06-28"
EXT_FIRST_WEEK = "2021-07-05"
EXT_LAST_WEEK = "2022-12-19"
EXT_N_WEEKS = 77
EXT_FIRST_DAY = "2021-07-01"     # boundary days included in daily coverage
EXT_LAST_DAY = "2022-12-31"      # through year end (A8: boundary days are
                                 # registered as REPORTED data)
MONTHS = pd.period_range("2021-07", "2022-12", freq="M")
ID, DT = "endpoint_id", "date"


def _fail(msg):
    raise SystemExit(f"INTAKE FAIL: {msg}")


def _num_cols(df):
    return [c for c in df.columns if c not in (ID, DT)
            and np.issubdtype(df[c].dtype, np.number)]


def verify_frozen_prefix(ext_path: str, frozen_path: str) -> None:
    """Schema equality + exact prefix equality (all columns, keys AND values)."""
    fro = pd.read_parquet(frozen_path)
    ext = pd.read_parquet(ext_path)
    if set(fro.columns) != set(ext.columns):
        _fail(f"schema mismatch: frozen-only={sorted(set(fro.columns) - set(ext.columns))} "
              f"ext-only={sorted(set(ext.columns) - set(fro.columns))}")
    cols = list(fro.columns)
    pre = ext[pd.to_datetime(ext[DT]) <= pd.Timestamp(FROZEN_LAST_WEEK)][cols]
    key = [ID, DT]
    fro = fro.sort_values(key).reset_index(drop=True)
    pre = pre.sort_values(key).reset_index(drop=True)
    if len(fro) != len(pre):
        _fail(f"prefix row count {len(pre):,} != frozen {len(fro):,}")
    for c in cols:
        a, b = fro[c].to_numpy(), pre[c].to_numpy()
        if np.issubdtype(fro[c].dtype, np.number):
            ok = np.array_equal(a, b)
        else:
            ok = (pd.Series(a).astype(str) == pd.Series(b).astype(str)).all()
        if not ok:
            _fail(f"prefix column '{c}' differs from the frozen panel "
                  f"(the A6.1 boundary-leak signature if it is the "
                  f"{FROZEN_LAST_WEEK} week)")


# A10 registered per-column semantics (comments-only panel). Draft text:
# runs/2026-07-12_confirmation/A10_DRAFT.md — INERT for confirmation
# purposes until registered in CONFIRMATION_PROTOCOL.md (attestation +
# owner acknowledgment). Derived from panel construction and verified
# against the frozen baseline (MODEL_STATUS §2z-p):
#   metric_value      finite integral >= 0 (positive-part daily net karma)
#   comment_count     finite integral >= 0
#   comment_karma     finite integral SIGNED (net votes; negatives valid)
#   submission_karma  identically 0   (comments-only panel)
#   submission_count  identically 0
# Any other numeric column has no registered semantics -> FAIL-CLOSED.
A10_NONNEG = {"metric_value", "comment_count"}
A10_SIGNED = {"comment_karma"}
A10_ZERO = {"submission_karma", "submission_count"}


def _column_semantics(df, what, daily=False):
    """A10 hygiene: keys unique; nulls rejected everywhere; every numeric
    field finite + integral; sign/zero semantics per registered column;
    daily panels additionally satisfy metric_value == max(comment_karma, 0)
    exactly (binds the audit field to the modeled field cell-by-cell)."""
    if df.duplicated([ID, DT]).any():
        _fail(f"duplicate (entity, date) keys in the {what} panel")
    nulls = df.columns[df.isna().any()].tolist()
    if nulls:
        _fail(f"null values in {what} columns {nulls}")
    for c in _num_cols(df):
        if c not in (A10_NONNEG | A10_SIGNED | A10_ZERO):
            _fail(f"unregistered numeric column '{c}' in the {what} panel "
                  f"(no A10 semantics -- fail-closed)")
        v = df[c]
        if not np.issubdtype(v.dtype, np.integer):
            a = v.to_numpy(dtype=float)
            if not np.isfinite(a).all():
                _fail(f"non-finite values in {what} column '{c}'")
            if (a != np.floor(a)).any():
                _fail(f"non-integral values in {what} column '{c}'")
        if c in A10_NONNEG and (v < 0).any():
            _fail(f"negative values in {what} column '{c}'")
        if c in A10_ZERO and (v != 0).any():
            _fail(f"nonzero values in {what} column '{c}' "
                  f"(identically 0 in this comments-only panel)")
    if daily and "comment_karma" in df.columns:
        if not (df["metric_value"] == df["comment_karma"].clip(lower=0)).all():
            _fail(f"daily identity violated in the {what} panel: "
                  f"metric_value != max(comment_karma, 0)")


def _signed_field_readout(df, what, daily=False):
    """A10 mandatory DESCRIPTIVE readouts — reported, NEVER gates."""
    if "comment_karma" not in df.columns or not len(df):
        return
    ck = df["comment_karma"]
    neg = ck < 0
    line = (f"  [descriptive, never gates] {what}: negative comment_karma "
            f"cells {int(neg.sum()):,}/{len(df):,} ({neg.mean():.4%})")
    if daily:
        clipped = int(-ck[neg].sum())
        total = int(df["metric_value"].sum())
        ratio = clipped / total if total else float("nan")
        line += (f"; abs negative karma removed by clipping / total modeled "
                 f"positive-part karma = {clipped:,}/{total:,} ({ratio:.6%})")
    print(line)


def _check_coverage_log(path: str) -> None:
    log = pd.read_csv(path, dtype={"month": str})
    for col in ("month", "status", "rows", "bytes"):
        if col not in log.columns:
            _fail(f"coverage log lacks required column '{col}'")
    want = set(MONTHS.astype(str))
    sub = log[log["month"].isin(want)]
    dup = sub["month"].duplicated()
    if dup.any():
        _fail(f"coverage log has duplicate records for months "
              f"{sorted(sub.loc[dup, 'month'].unique())}")
    missing = sorted(want - set(sub["month"]))
    if missing:
        _fail(f"coverage log missing months {missing}")
    bad = sub[(sub["status"] != "ok")
              | (pd.to_numeric(sub["rows"], errors="coerce").fillna(0) <= 0)
              | (pd.to_numeric(sub["bytes"], errors="coerce").fillna(0) <= 0)]
    if len(bad):
        _fail(f"coverage log has non-ok/empty months: "
              f"{bad[['month', 'status', 'rows']].to_dict('records')}")


def _check_processing_log(path: str) -> None:
    """A9: zero parse errors, from the aggregator's own processing log
    (`reddit_monthly_processing_log.csv`). The LATEST comments record per
    month governs (re-runs append; the panel is built from the last run) —
    it must be ok, nonempty, and have errors == 0."""
    log = pd.read_csv(path, dtype={"month": str})
    for col in ("record_type", "month", "status", "lines", "errors",
                "output_bytes", "finished_at_utc"):
        if col not in log.columns:
            _fail(f"processing log lacks required column '{col}'")
    sub = log[(log["record_type"] == "comments")
              & log["month"].isin(set(MONTHS.astype(str)))].copy()
    sub["_t"] = pd.to_datetime(sub["finished_at_utc"], errors="coerce")
    if sub["_t"].isna().any():
        _fail("processing log has unparseable finished_at_utc timestamps")
    missing = sorted(set(MONTHS.astype(str)) - set(sub["month"]))
    if missing:
        _fail(f"processing log has no comments record for months {missing}")
    latest = sub.sort_values("_t").groupby("month").tail(1)
    bad = latest[(latest["status"] != "ok")
                 | (pd.to_numeric(latest["lines"], errors="coerce").fillna(0) <= 0)
                 | (pd.to_numeric(latest["output_bytes"], errors="coerce").fillna(0) <= 0)
                 | (pd.to_numeric(latest["errors"], errors="coerce").fillna(1) != 0)]
    if len(bad):
        _fail(f"processing log: latest comments record violates the "
              f"zero-parse-errors rule for "
              f"{bad[['month', 'status', 'errors']].to_dict('records')}")


def check(ext_weekly: str, frozen_weekly: str, ext_daily: str,
          frozen_daily: str, coverage_log: str, processing_log: str) -> None:
    # [1] schema + frozen prefix
    verify_frozen_prefix(ext_weekly, frozen_weekly)
    print("  [1/6] schema equality + frozen-prefix equality: OK")

    ext = pd.read_parquet(ext_weekly)
    ext[DT] = pd.to_datetime(ext[DT])
    new = ext[ext[DT] > pd.Timestamp(FROZEN_LAST_WEEK)]

    # [2] complete-week window
    weeks = pd.DatetimeIndex(np.sort(new[DT].unique()))
    want = pd.date_range(EXT_FIRST_WEEK, EXT_LAST_WEEK, freq="7D")
    assert len(want) == EXT_N_WEEKS
    if not weeks.equals(want):
        _fail(f"extension week set wrong: extra={list(weeks.difference(want).date)[:4]} "
              f"missing={list(want.difference(weeks).date)[:4]}")
    print(f"  [2/6] complete-week window: OK ({EXT_N_WEEKS} weeks, "
          f"period 136 = {EXT_FIRST_WEEK})")

    # [3] weekly hygiene (A10 per-column semantics; no weekly clipping
    #     identity -- clip precedes weekly aggregation by construction)
    _column_semantics(ext, "extended weekly")
    print("  [3/6] weekly keys unique + A10 column semantics: OK")
    _signed_field_readout(new, "extension weekly")

    # [4] aggregation logs (parse success is only observable here)
    _check_coverage_log(coverage_log)
    _check_processing_log(processing_log)
    print("  [4/6] aggregation logs: 18 months, one ok nonempty coverage "
          "record each + latest processing record errors == 0: OK")

    # [5] daily panel: required metrics, hygiene, calendar coverage,
    #     aggregation equality with exact index-set equality
    d = pd.read_parquet(ext_daily)
    d[DT] = pd.to_datetime(d[DT])
    dd = d[(d[DT] >= pd.Timestamp(EXT_FIRST_DAY))
           & (d[DT] <= pd.Timestamp(EXT_LAST_DAY))]
    metrics = _num_cols(ext)
    lacking = [c for c in metrics if c not in dd.columns]
    if lacking:
        _fail(f"daily panel lacks frozen metric columns {lacking} "
              f"(no silent intersection)")
    _column_semantics(dd, "extension daily", daily=True)
    _signed_field_readout(dd, "extension daily", daily=True)
    have_days = pd.DatetimeIndex(np.sort(dd[DT].unique())).normalize()
    want_days = pd.date_range(EXT_FIRST_DAY, EXT_LAST_DAY, freq="D")
    missing_days = want_days.difference(have_days)
    if len(missing_days):
        _fail(f"{len(missing_days)} missing extension calendar days "
              f"(first: {list(missing_days.date)[:3]})")
    dw = dd[dd[DT] >= pd.Timestamp(EXT_FIRST_WEEK)].copy()
    dw["wk"] = dw[DT] - pd.to_timedelta(dw[DT].dt.dayofweek, unit="D")
    dw = dw[dw["wk"] <= pd.Timestamp(EXT_LAST_WEEK)]
    ds = dw.groupby([ID, "wk"])[metrics].sum().sort_index()
    ws = new.set_index([ID, DT])[metrics].sort_index()
    ds.index.names = ws.index.names
    only_daily = ds.index.difference(ws.index)
    only_weekly = ws.index.difference(ds.index)
    if len(only_daily) or len(only_weekly):
        _fail(f"(entity, week) index sets differ: {len(only_daily)} daily-only, "
              f"{len(only_weekly)} weekly-only cells (first daily-only: "
              f"{list(only_daily[:2])}; first weekly-only: {list(only_weekly[:2])})")
    if not np.allclose(ds.to_numpy(dtype=float), ws.to_numpy(dtype=float),
                       rtol=0, atol=0):
        bad = (ds.to_numpy(dtype=float) != ws.to_numpy(dtype=float)).any(axis=1)
        _fail(f"weekly != sum(daily) on {int(bad.sum()):,} extension "
              f"(entity, week) cells across columns {metrics}")
    print(f"  [5/6] daily metrics complete + hygiene + calendar through "
          f"{EXT_LAST_DAY} + weekly=Σdaily with exact index equality: OK")

    # [6] day guard WITH frozen history (registered instrument_eras guard):
    #     July 1-7 must be judged against the frozen baseline
    fd = pd.read_parquet(frozen_daily)
    fd[DT] = pd.to_datetime(fd[DT])
    fro_counts = fd.groupby(fd[DT].dt.normalize())[ID].size()
    ext_counts = dd.groupby(dd[DT].dt.normalize())[ID].size().reindex(want_days)
    counts = pd.concat([fro_counts[~fro_counts.index.isin(ext_counts.index)],
                        ext_counts]).sort_index()
    flagged = flag_days(counts)
    flagged_ext = flagged[flagged >= pd.Timestamp(EXT_FIRST_DAY)]
    if len(flagged_ext):
        _fail(f"day-guard flagged {len(flagged_ext)} extension days "
              f"(first: {list(flagged_ext.date)[:3]}) -- census property violated")
    print("  [6/6] day-guard (frozen-history trailing prior-days median): "
          "0 flagged extension days: OK")

    print("PASS")


def frozen_self_test(frozen_weekly: str, frozen_daily: str) -> None:
    """A10 validation requirement 1: the gate's column semantics must PASS
    on the frozen T=136 baseline panels themselves."""
    fw = pd.read_parquet(frozen_weekly)
    fw[DT] = pd.to_datetime(fw[DT])
    _column_semantics(fw, "frozen weekly")
    _signed_field_readout(fw, "frozen weekly")
    fd = pd.read_parquet(frozen_daily)
    fd[DT] = pd.to_datetime(fd[DT])
    _column_semantics(fd, "frozen daily", daily=True)
    _signed_field_readout(fd, "frozen daily", daily=True)
    print("FROZEN SELF-TEST PASS")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--frozen-self-test":
        frozen_self_test(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 7:
        check(*sys.argv[1:7])
    else:
        raise SystemExit(__doc__)
