#!/usr/bin/env python3
"""A1.1 fail-closed BOUNDED-MEMORY intake gate for the submissions long
panels (PREREG_2026-07-16 Amendment 1). Prints PASS only if every invariant
holds; any failure => nonzero exit.

Memory contract (2026-07-16 execution correction, disclosed): the daily
panel is streamed ONE ROW GROUP at a time; per-(entity, week) aggregates are
finalized and compared against the weekly panel AS SOON AS the read frontier
passes a week, then discarded. Retained state is bounded by the row-group
span (a couple of weeks), NEVER by the number of weeks -- the previous
implementation materialised the full ~53.7M-row entity-week aggregate, which
both violated this "never loads the full panel" contract and drove an
O(N^2/8) incremental re-merge. Invariants, thresholds, inputs, and hashes are
UNCHANGED; only the execution is corrected. Both long parquets are
date-ordered by row group (non-overlapping ascending), which makes the
one-week merge exact; the weekly side is read per-week and is order-robust.

Usage:
  python3 llm_fitting/check_long_panels.py DAILY WEEKLY PROCESSING_LOG \
      [--first-week 2018-12-03] [--last-week 2022-12-19] \
      [--months 2018-12:2022-12] [--record-type submissions]

Invariants (ANY failure stops model contact -- protocol §2 discipline):
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

# weekday bit masks are 0..127; vectorised popcount by lookup
_POPCOUNT_LUT = np.array([bin(i).count("1") for i in range(128)],
                         dtype=np.int64)

# structural probe: populated at the end of a successful check() so tests can
# assert retained state is bounded and INDEPENDENT of the number of weeks.
LAST_RUN_STATS: dict = {}


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


def _mask_agg(frame, metrics_cols):
    """frame is indexed by [endpoint_id, wk] with per-row (or per-chunk)
    columns metrics + '_rows' + '_mask'; possibly with repeated index rows.
    Return ONE row per (endpoint_id, wk): metric sums, '_rows' summed, and
    '_mask' = bitwise-OR of the weekday masks -- computed by per-bit max so
    the OR is fully vectorised (no Python-per-group lambda) and mergeable
    across row groups."""
    out = frame.groupby(level=[0, 1])[metrics_cols + ["_rows"]].sum()
    m = frame["_mask"].to_numpy()
    bit_df = pd.DataFrame({f"_b{b}": ((m >> b) & 1) for b in range(7)},
                          index=frame.index)
    bit_max = bit_df.groupby(level=[0, 1]).max().reindex(out.index)
    mask = np.zeros(len(out), dtype=np.int64)
    for b in range(7):
        mask |= bit_max[f"_b{b}"].to_numpy().astype(np.int64) << b
    out["_mask"] = mask
    return out


def _compare_week(w, dcells, weekly_path, metrics_cols, stats):
    """dcells: one row per entity for complete week w (index [endpoint_id,
    wk==w]) with the daily-summed metric columns. Read week w from the weekly
    panel and enforce exact (entity) index equality + per-column equality."""
    tbl = pq.read_table(weekly_path,
                        filters=[("date", "==", pd.Timestamp(w))])
    ww = tbl.to_pandas()
    if len(ww):
        ww["date"] = pd.to_datetime(ww["date"])
        _hygiene(ww, f"weekly[{w.date()}]", daily=False)
        if ww.duplicated(["endpoint_id", "date"]).any():
            _fail(f"duplicate (entity, week) keys in weekly {w.date()}")
        if (ww["date"].dt.weekday != 0).any():
            _fail(f"non-Monday weekly date in weekly {w.date()}")
    ws = ww.set_index("endpoint_id").sort_index()
    d = dcells.droplevel(1).sort_index()
    only_d = d.index.difference(ws.index)
    only_w = ws.index.difference(d.index)
    if len(only_d) or len(only_w):
        _fail(f"(entity, week) index sets differ in week {w.date()}: "
              f"{len(only_d)} daily-only, {len(only_w)} weekly-only")
    ws = ws.reindex(d.index)
    for c in metrics_cols:
        if not np.array_equal(d[c].to_numpy(), ws[c].to_numpy()):
            n = int((d[c].to_numpy() != ws[c].to_numpy()).sum())
            _fail(f"weekly != sum(daily) on '{c}' in week {w.date()} "
                  f"({n:,} cells)")
    stats["weekly_rows_matched"] += len(ws)
    stats["want_weeks_seen"].add(w)


def _finalize_week(w, parts, want_set, weekly_path, metrics_cols, stats):
    agg = _mask_agg(pd.concat(parts), metrics_cols)
    pc = _POPCOUNT_LUT[agg["_mask"].to_numpy()]
    dup = agg["_rows"].to_numpy() > pc
    if dup.any():
        _fail(f"{int(dup.sum())} (entity, week={w.date()}) cells with more "
              f"rows than distinct weekdays -- duplicate (entity, date) keys")
    cells = agg[metrics_cols]
    if w in want_set:
        _compare_week(w, cells, weekly_path, metrics_cols, stats)
    else:
        stats["boundary_cells"] += len(cells)


def check(daily_path, weekly_path, log_path, first_week, last_week,
          months, record_type="submissions",
          first_day="2018-12-01", last_day="2022-12-31"):
    fw, lw = pd.Timestamp(first_week), pd.Timestamp(last_week)
    fd, ld = pd.Timestamp(first_day), pd.Timestamp(last_day)
    if fw < fd or lw + pd.Timedelta(days=6) > ld:
        _fail(f"complete-week range {fw.date()}..{lw.date()} does not fit "
              f"inside the required day span {fd.date()}..{ld.date()} "
              f"(the final week needs all 7 days)")
    want = pd.date_range(fw, lw, freq="7D")
    want_set = set(want)
    metrics_cols = [c for c in COLS if c not in ("endpoint_id", "date")]

    # ---- schema ----
    pf = pq.ParquetFile(daily_path)
    if set(pf.schema_arrow.names) != set(COLS):
        _fail(f"daily schema {pf.schema_arrow.names} != registered")
    wf = pq.ParquetFile(weekly_path)
    if set(wf.schema_arrow.names) != set(COLS):
        _fail(f"weekly schema {wf.schema_arrow.names} != registered")

    # ---- per-row-group daily statistics for the merge frontier (A5.3:
    #      row groups are date-non-overlapping ascending; next group's min
    #      date is a hard lower bound on all remaining rows) ----
    dcol = pf.schema_arrow.names.index("date")
    rg_min = []
    for i in range(pf.num_row_groups):
        st = pf.metadata.row_group(i).column(dcol).statistics
        rg_min.append(pd.Timestamp(st.min) if st is not None else None)

    # ---- BOUNDED streaming pass over the DAILY panel ----
    open_weeks: dict = {}          # week_ts -> list of per-group partials
    day_counts, seen_keys = {}, 0
    dmin = dmax = None
    stats = {"boundary_cells": 0, "weekly_rows_matched": 0,
             "want_weeks_seen": set(), "max_open_weeks": 0}

    for i in range(pf.num_row_groups):
        df = pf.read_row_group(i).to_pandas()
        df["date"] = pd.to_datetime(df["date"])
        _hygiene(df, "daily", daily=True)
        if df.duplicated(["endpoint_id", "date"]).any():
            _fail("duplicate (entity, date) keys within a daily row group")
        seen_keys += len(df)
        for d, n in df.groupby(df["date"].dt.normalize()).size().items():
            day_counts[d] = day_counts.get(d, 0) + int(n)
        dmin = df["date"].min() if dmin is None else min(dmin, df["date"].min())
        dmax = df["date"].max() if dmax is None else max(dmax, df["date"].max())

        wk = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
        raw = df[["endpoint_id"] + metrics_cols].copy()
        raw["wk"] = wk
        raw["_rows"] = 1
        raw["_mask"] = np.left_shift(
            1, df["date"].dt.weekday.to_numpy()).astype(np.int64)
        part = _mask_agg(raw.set_index(["endpoint_id", "wk"]), metrics_cols)
        for w, sub in part.groupby(level=1):
            open_weeks.setdefault(w, []).append(sub)

        # finalize every week whose last day is strictly behind the frontier
        next_min = rg_min[i + 1] if i + 1 < pf.num_row_groups else None
        if next_min is not None:
            for w in sorted(open_weeks):
                if w + pd.Timedelta(days=6) < next_min:
                    _finalize_week(w, open_weeks.pop(w), want_set,
                                   weekly_path, metrics_cols, stats)
        stats["max_open_weeks"] = max(stats["max_open_weeks"],
                                      len(open_weeks))
        if pf.num_row_groups > 16 and (i % 16 == 0
                                       or i == pf.num_row_groups - 1):
            print(f"  ... daily row-group {i + 1}/{pf.num_row_groups} "
                  f"@ {dmax.date()}, open weeks={len(open_weeks)}, "
                  f"rows so far={seen_keys:,}", file=sys.stderr, flush=True)

    for w in sorted(open_weeks):    # end of stream: nothing more can arrive
        _finalize_week(w, open_weeks.pop(w), want_set, weekly_path,
                       metrics_cols, stats)

    if dmin is None:
        _fail("daily panel is empty")
    print(f"  [1/6] daily hygiene + identity + schema + per-week dup rule: "
          f"OK ({seen_keys:,} rows, {dmin.date()}..{dmax.date()})")

    # ---- calendar coverage (A4.1: REQUIRED endpoints, not observed range) ----
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

    # ---- weekly week-set == consecutive complete weeks, exactly ----
    missing_want = want_set - stats["want_weeks_seen"]
    if missing_want:
        _fail(f"weekly panel missing {len(missing_want)} complete weeks "
              f"(first {sorted(w.date() for w in missing_want)[:3]})")
    n_weekly_rows = wf.metadata.num_rows
    if n_weekly_rows != stats["weekly_rows_matched"]:
        _fail(f"weekly has {n_weekly_rows:,} rows but only "
              f"{stats['weekly_rows_matched']:,} matched complete-week "
              f"(entity, week) cells -- extra weekly rows/weeks outside "
              f"{fw.date()}..{lw.date()}")
    print(f"  [3/6] weekly hygiene + {len(want)} consecutive complete "
          f"weeks (one-week merge): OK")
    print(f"  [4/6] weekly = SUM(daily), exact index equality both "
          f"directions, every column, every week ({n_weekly_rows:,} weekly "
          f"rows): OK")

    print(f"  [5/6] boundary/partial-week cells excluded from weekly: OK "
          f"({stats['boundary_cells']:,} boundary (entity,week) cells "
          f"outside {fw.date()}..{lw.date()})")

    _check_processing_log(log_path, months, record_type)
    print(f"  [6/6] processing log: latest {record_type} record ok/errors==0 "
          f"for all {len(months)} months: OK")

    LAST_RUN_STATS.clear()
    LAST_RUN_STATS.update(
        max_open_weeks=stats["max_open_weeks"],
        weekly_rows_matched=stats["weekly_rows_matched"],
        boundary_cells=stats["boundary_cells"],
        n_row_groups=pf.num_row_groups, n_weeks=len(want))
    print("PASS")


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
