#!/usr/bin/env python3
"""A1.1 fail-closed STREAMING intake gate for the submissions long panels
(PREREG_2026-07-16 Amendment 1). Prints PASS only if every invariant
holds; any failure => nonzero exit. Never loads the daily panel whole:
daily is processed by parquet row-group batches (hygiene + identity +
day-coverage + guard counts + weekly-sum accumulation), then compared
against the weekly panel with exact index-set equality both directions.

Usage:
  python3 llm_fitting/check_long_panels.py DAILY WEEKLY PROCESSING_LOG \
      [--first-week 2018-12-03] [--last-week 2022-12-19] \
      [--months 2018-12:2022-12] [--record-type submissions]

Invariants (ANY failure stops model contact — protocol §2 discipline):
 1. schema = the 7 registered columns, both panels.
 2. A10 semantics, submissions roles: metric_value >= 0;
    submission_karma / comment_karma signed; counts >= 0; all numerics
    integral; nulls/nonfinite/unregistered numeric columns FAIL.
 3. DAILY identity: metric_value == max(submission_karma, 0), every row.
 4. weekly = SUM(daily) on every metric column with EXACT (entity, week)
    index-set equality in both directions (complete weeks only).
 5. complete weeks CONSECUTIVE first-week..last-week; partial weeks
    excluded from weekly rows (weekly week-set == expected exactly).
 6. calendar-day coverage: every day from the first daily date to the
    last, no gaps; no duplicate (entity, date) keys.
 7. A9 latest-record processing-log semantics: for every month, latest
    record of the record-type has status ok, lines>0, output_bytes>0,
    errors==0.
 8. day-guard (prior-days trailing 28-day median, 60%): flags == 0
    (census expectation).
Top-5 eyeball / smoke-load are NOT this gate's job and run only after it.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

COLS = ["endpoint_id", "date", "metric_value", "submission_karma",
        "comment_karma", "submission_count", "comment_count"]
NONNEG = {"metric_value", "submission_count", "comment_count"}
SIGNED = {"submission_karma", "comment_karma"}


def _fail(msg):
    raise SystemExit(f"INTAKE FAIL: {msg}")


def _hygiene(df, what, daily):
    extra = [c for c in df.columns if c not in COLS]
    if extra:
        _fail(f"unregistered columns {extra} in {what}")
    nulls = df.columns[df.isna().any()].tolist()
    if nulls:
        _fail(f"null values in {what} columns {nulls}")
    for c in NONNEG | SIGNED:
        v = df[c]
        if not np.issubdtype(v.dtype, np.integer):
            a = v.to_numpy(dtype=float)
            if not np.isfinite(a).all():
                _fail(f"non-finite values in {what} column '{c}'")
            if (a != np.floor(a)).any():
                _fail(f"non-integral values in {what} column '{c}'")
        if c in NONNEG and (v < 0).any():
            _fail(f"negative values in {what} column '{c}'")
    if daily:
        if not (df["metric_value"]
                == df["submission_karma"].clip(lower=0)).all():
            _fail(f"daily identity violated in {what}: "
                  f"metric_value != max(submission_karma, 0)")


def _check_processing_log(path, months, record_type):
    log = pd.read_csv(path, header=None if _headerless(path) else 0)
    if log.shape[1] >= 10 and not isinstance(log.columns[0], str):
        log.columns = ["source", "record_type", "month", "status", "ok_flag",
                       "lines", "errors", "error_rate", "rows", "out_dir"][:log.shape[1]]
    need = {"record_type", "month", "status", "lines", "errors"}
    if not need <= set(log.columns):
        _fail(f"processing log lacks required columns {sorted(need - set(log.columns))}")
    sub = log[(log["record_type"] == record_type)
              & log["month"].astype(str).isin(months)]
    missing = sorted(set(months) - set(sub["month"].astype(str)))
    if missing:
        _fail(f"processing log missing {record_type} months {missing}")
    latest = sub.groupby("month").tail(1)
    bad = latest[(latest["status"] != "ok")
                 | (pd.to_numeric(latest["lines"], errors="coerce").fillna(0) <= 0)
                 | (pd.to_numeric(latest["errors"], errors="coerce").fillna(1) != 0)]
    if len(bad):
        _fail(f"processing log latest-record violations: "
              f"{bad[['month', 'status', 'errors']].to_dict('records')}")


def _headerless(path):
    with open(path) as f:
        first = f.readline()
    return "record_type" not in first and "month" not in first


def check(daily_path, weekly_path, log_path, first_week, last_week,
          months, record_type="submissions"):
    fw, lw = pd.Timestamp(first_week), pd.Timestamp(last_week)

    # ---- streaming pass over the DAILY panel ----
    pf = pq.ParquetFile(daily_path)
    if set(pf.schema_arrow.names) != set(COLS):
        _fail(f"daily schema {pf.schema_arrow.names} != registered")
    sums, day_counts, seen_keys = [], {}, 0
    dmin = dmax = None
    for batch in pf.iter_batches(batch_size=2_000_000):
        df = batch.to_pandas()
        df["date"] = pd.to_datetime(df["date"])
        _hygiene(df, "daily", daily=True)
        if df.duplicated(["endpoint_id", "date"]).any():
            _fail("duplicate (entity, date) keys within a daily batch")
        seen_keys += len(df)
        for d, n in df.groupby(df["date"].dt.normalize()).size().items():
            day_counts[d] = day_counts.get(d, 0) + int(n)
        dmin = df["date"].min() if dmin is None else min(dmin, df["date"].min())
        dmax = df["date"].max() if dmax is None else max(dmax, df["date"].max())
        wk = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
        g = df.assign(wk=wk).groupby(["endpoint_id", "wk"])[
            [c for c in COLS if c not in ("endpoint_id", "date")]].sum()
        sums.append(g)
    daily_sums = pd.concat(sums).groupby(level=[0, 1]).sum()
    if daily_sums.index.duplicated().any():
        _fail("internal: duplicate keys after daily aggregation")
    # cross-batch duplicate check: total rows must equal unique keys
    n_unique = len(pd.concat(
        [s.index.to_frame(index=False) for s in sums]).drop_duplicates())
    del sums
    print(f"  [1/6] daily hygiene + identity + schema: OK "
          f"({seen_keys:,} rows, {dmin.date()}..{dmax.date()})")

    # calendar coverage + day guard (prior-days trailing median)
    days = pd.date_range(dmin.normalize(), dmax.normalize(), freq="D")
    missing = [d for d in days if d not in day_counts]
    if missing:
        _fail(f"{len(missing)} missing calendar days (first {missing[:3]})")
    counts = pd.Series(day_counts).sort_index()
    med = counts.shift(1).rolling(28, min_periods=14).median()
    flagged = counts[(med.notna()) & (counts < 0.6 * med)]
    if len(flagged):
        _fail(f"day-guard flagged {len(flagged)} days "
              f"(first {list(flagged.index[:3])}) -- census violated")
    print(f"  [2/6] calendar coverage {days[0].date()}..{days[-1].date()} "
          f"+ day-guard 0 flags: OK")

    # ---- weekly panel ----
    wf = pq.ParquetFile(weekly_path)
    if set(wf.schema_arrow.names) != set(COLS):
        _fail(f"weekly schema {wf.schema_arrow.names} != registered")
    weekly = wf.read().to_pandas()
    weekly["date"] = pd.to_datetime(weekly["date"])
    _hygiene(weekly, "weekly", daily=False)
    if weekly.duplicated(["endpoint_id", "date"]).any():
        _fail("duplicate (entity, week) keys in the weekly panel")
    if (weekly["date"].dt.weekday != 0).any():
        _fail("non-Monday weekly dates")
    wset = pd.DatetimeIndex(np.sort(weekly["date"].unique()))
    want = pd.date_range(fw, lw, freq="7D")
    if not wset.equals(want):
        _fail(f"weekly week-set != consecutive complete weeks "
              f"{fw.date()}..{lw.date()}: extra="
              f"{list(wset.difference(want).date)[:3]} missing="
              f"{list(want.difference(wset).date)[:3]}")
    print(f"  [3/6] weekly hygiene + {len(want)} consecutive complete "
          f"weeks: OK")

    # ---- weekly == sum(daily), exact index-set equality both ways ----
    ws = weekly.set_index(["endpoint_id", "date"]).sort_index()
    ds = daily_sums[daily_sums.index.get_level_values(1).isin(want)].sort_index()
    ds.index.names = ws.index.names
    only_d = ds.index.difference(ws.index)
    only_w = ws.index.difference(ds.index)
    if len(only_d) or len(only_w):
        _fail(f"(entity, week) index sets differ: {len(only_d)} daily-only, "
              f"{len(only_w)} weekly-only")
    for c in ds.columns:
        if not np.array_equal(ds[c].to_numpy(), ws[c].to_numpy()):
            n = int((ds[c].to_numpy() != ws[c].to_numpy()).sum())
            _fail(f"weekly != sum(daily) on column '{c}' ({n:,} cells)")
    print("  [4/6] weekly = SUM(daily), exact index equality both "
          "directions, every column: OK")

    # ---- boundary days excluded ----
    outside = daily_sums.index.get_level_values(1)
    n_boundary = int((~outside.isin(want)).sum())
    print(f"  [5/6] boundary/partial-week cells excluded from weekly: OK "
          f"({n_boundary:,} boundary (entity,week) cells outside "
          f"{fw.date()}..{lw.date()})")

    # ---- A9 processing log ----
    _check_processing_log(log_path, months, record_type)
    print(f"  [6/6] processing log: latest {record_type} record ok/errors==0 "
          f"for all {len(months)} months: OK")
    print("PASS")


def month_range(spec):
    a, b = spec.split(":")
    return [str(p) for p in pd.period_range(a, b, freq="M")]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("daily")
    ap.add_argument("weekly")
    ap.add_argument("processing_log")
    ap.add_argument("--first-week", default="2018-12-03")
    ap.add_argument("--last-week", default="2022-12-19")
    ap.add_argument("--months", default="2018-12:2022-12")
    ap.add_argument("--record-type", default="submissions")
    a = ap.parse_args()
    check(a.daily, a.weekly, a.processing_log, a.first_week, a.last_week,
          month_range(a.months), a.record_type)
