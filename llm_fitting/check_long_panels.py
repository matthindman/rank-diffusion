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
              & log["month"].astype(str).isin(months)].copy()
    missing = sorted(set(months) - set(sub["month"].astype(str)))
    if missing:
        _fail(f"processing log missing {record_type} months {missing}")
    # A4.3: latest = max finished_at_utc when present, else FILE ORDER
    # (declared append order)
    if "finished_at_utc" in sub.columns:
        sub["_t"] = pd.to_datetime(sub["finished_at_utc"], errors="coerce")
        if sub["_t"].isna().any():
            _fail("processing log has unparseable finished_at_utc")
        sub = sub.sort_values("_t", kind="stable")
    latest = sub.groupby("month").tail(1)
    bad = (latest["status"] != "ok") \
        | (pd.to_numeric(latest["lines"], errors="coerce").fillna(0) <= 0) \
        | (pd.to_numeric(latest["errors"], errors="coerce").fillna(1) != 0)
    if "output_bytes" in latest.columns:
        bad |= pd.to_numeric(latest["output_bytes"], errors="coerce").fillna(0) <= 0
    elif "rows" in latest.columns:
        bad |= pd.to_numeric(latest["rows"], errors="coerce").fillna(0) <= 0
    else:
        _fail("processing log lacks BOTH output_bytes and rows columns "
              "(A4.3 numeric rule cannot be established)")
    if bad.any():
        _fail(f"processing log latest-record violations: "
              f"{latest[bad][['month', 'status', 'errors']].to_dict('records')}")


def _headerless(path):
    with open(path) as f:
        first = f.readline()
    return "record_type" not in first and "month" not in first


def check(daily_path, weekly_path, log_path, first_week, last_week,
          months, record_type="submissions",
          first_day="2018-12-01", last_day="2022-12-31"):
    fw, lw = pd.Timestamp(first_week), pd.Timestamp(last_week)
    fd, ld = pd.Timestamp(first_day), pd.Timestamp(last_day)
    if fw < fd or lw + pd.Timedelta(days=6) > ld:
        _fail(f"complete-week range {fw.date()}..{lw.date()} does not fit "
              f"inside the required day span {fd.date()}..{ld.date()} "
              f"(the final week needs all 7 days)")

    # ---- streaming pass over the DAILY panel ----
    pf = pq.ParquetFile(daily_path)
    if set(pf.schema_arrow.names) != set(COLS):
        _fail(f"daily schema {pf.schema_arrow.names} != registered")
    metrics_cols = [c for c in COLS if c not in ("endpoint_id", "date")]
    sums, day_counts, seen_keys = [], {}, 0
    dmin = dmax = None
    n_batch = 0
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
        # A4.2: rows-per-cell + weekday-presence BITMASK (OR-mergeable);
        # rows > popcount(mask) <=> a duplicate (entity, date) key,
        # detectable across batches
        g = (df.assign(wk=wk,
                       _rows=1,
                       _mask=np.left_shift(1, df["date"].dt.weekday
                                           .to_numpy()).astype("int64"))
             .groupby(["endpoint_id", "wk"])
             .agg({**{c: "sum" for c in metrics_cols},
                   "_rows": "sum",
                   "_mask": lambda x: int(np.bitwise_or.reduce(x))}))
        sums.append(g)
        n_batch += 1
        if n_batch % 8 == 0:                    # A4.4 incremental merge
            merged = pd.concat(sums).groupby(level=[0, 1]).agg(
                {**{c: "sum" for c in metrics_cols}, "_rows": "sum",
                 "_mask": lambda x: int(np.bitwise_or.reduce(x))})
            sums = [merged]
    daily_sums = pd.concat(sums).groupby(level=[0, 1]).agg(
        {**{c: "sum" for c in metrics_cols}, "_rows": "sum",
         "_mask": lambda x: int(np.bitwise_or.reduce(x))})
    del sums
    popcount = daily_sums["_mask"].map(lambda m: bin(int(m)).count("1"))
    dup = daily_sums["_rows"] > popcount
    if dup.any():
        _fail(f"{int(dup.sum())} (entity, week) cells with more rows than "
              f"distinct weekdays -- cross-batch duplicate (entity, date) "
              f"keys")
    daily_sums = daily_sums.drop(columns=["_rows", "_mask"])
    print(f"  [1/6] daily hygiene + identity + schema + cross-batch dup "
          f"rule: OK ({seen_keys:,} rows, {dmin.date()}..{dmax.date()})")

    # calendar coverage (A4.1: REQUIRED endpoints, not observed range)
    if dmin.normalize() != fd or dmax.normalize() != ld:
        _fail(f"daily coverage {dmin.date()}..{dmax.date()} != required "
              f"{fd.date()}..{ld.date()}")
    days = pd.date_range(fd, ld, freq="D")
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

    # ---- weekly panel, YEAR-CHUNKED (A4.4: the daily-sums table is the
    #      declared memory peak; the weekly never loads whole) ----
    wf = pq.ParquetFile(weekly_path)
    if set(wf.schema_arrow.names) != set(COLS):
        _fail(f"weekly schema {wf.schema_arrow.names} != registered")
    want = pd.date_range(fw, lw, freq="7D")
    weeks_seen = []
    n_weekly_rows = 0
    import pyarrow.compute as pc
    for yr in range(fw.year, lw.year + 1):
        tbl = pq.read_table(
            weekly_path,
            filters=[("date", ">=", pd.Timestamp(f"{yr}-01-01")),
                     ("date", "<", pd.Timestamp(f"{yr + 1}-01-01"))])
        if tbl.num_rows == 0:
            continue
        weekly = tbl.to_pandas()
        weekly["date"] = pd.to_datetime(weekly["date"])
        _hygiene(weekly, f"weekly[{yr}]", daily=False)
        if weekly.duplicated(["endpoint_id", "date"]).any():
            _fail(f"duplicate (entity, week) keys in weekly[{yr}]")
        if (weekly["date"].dt.weekday != 0).any():
            _fail(f"non-Monday weekly dates in weekly[{yr}]")
        weeks_seen.append(pd.DatetimeIndex(np.sort(weekly["date"].unique())))
        n_weekly_rows += len(weekly)
        ws = weekly.set_index(["endpoint_id", "date"]).sort_index()
        yr_weeks = want[(want >= pd.Timestamp(f"{yr}-01-01"))
                        & (want < pd.Timestamp(f"{yr + 1}-01-01"))]
        ds = daily_sums[daily_sums.index.get_level_values(1)
                        .isin(yr_weeks)].sort_index()   # A5.3: year slice
        # directly from the sums table (the declared peak); no near-full copy
        ds.index.names = ws.index.names
        only_d = ds.index.difference(ws.index)
        only_w = ws.index.difference(ds.index)
        if len(only_d) or len(only_w):
            _fail(f"(entity, week) index sets differ in {yr}: "
                  f"{len(only_d)} daily-only, {len(only_w)} weekly-only")
        for c in ds.columns:
            if not np.array_equal(ds[c].to_numpy(), ws[c].to_numpy()):
                n = int((ds[c].to_numpy() != ws[c].to_numpy()).sum())
                _fail(f"weekly != sum(daily) on '{c}' in {yr} ({n:,} cells)")
        del weekly, ws, ds
    wset = weeks_seen[0]
    for w in weeks_seen[1:]:
        wset = wset.append(w)
    wset = pd.DatetimeIndex(np.sort(np.unique(wset)))
    if not wset.equals(want):
        _fail(f"weekly week-set != consecutive complete weeks "
              f"{fw.date()}..{lw.date()}: extra="
              f"{list(wset.difference(want).date)[:3]} missing="
              f"{list(want.difference(wset).date)[:3]}")
    print(f"  [3/6] weekly hygiene + {len(want)} consecutive complete "
          f"weeks (year-chunked): OK")
    print(f"  [4/6] weekly = SUM(daily), exact index equality both "
          f"directions, every column, every year ({n_weekly_rows:,} weekly "
          f"rows): OK")

    # ---- boundary days excluded ----
    n_boundary = int((~daily_sums.index.get_level_values(1).isin(want)).sum())
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
    ap.add_argument("--first-day", default="2018-12-01")
    ap.add_argument("--last-day", default="2022-12-31")
    a = ap.parse_args()
    check(a.daily, a.weekly, a.processing_log, a.first_week, a.last_week,
          month_range(a.months), a.record_type, a.first_day, a.last_day)
