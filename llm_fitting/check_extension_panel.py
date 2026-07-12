#!/usr/bin/env python3
"""A6.1/A7 intake gate for the comments extension — FAIL-CLOSED.

Must print PASS before any model code touches the extension. Round-6 review
finding (accepted): the first version failed OPEN — shared-column-only
comparison, optional daily panel, no raw-file inventory, day guard including
the current day. This version enforces every registered stop rule and every
input is REQUIRED:

  python llm_fitting/check_extension_panel.py \
      EXT_WEEKLY FROZEN_WEEKLY EXT_DAILY RAW_MONTHLY_DIR

  1. SCHEMA EQUALITY: extended weekly columns == frozen weekly columns,
     exactly. A missing (or extra) column is a FAIL, not a silent skip.
  2. FROZEN-PREFIX EQUALITY: every row with date <= 2021-06-28 equal to the
     frozen panel on every column (keys AND values) — catches the July-1..4
     fold-in (E2 training period 135) that the date anchor cannot see.
  3. COMPLETE-WEEK WINDOW: extension weekly rows exactly the complete weeks
     2021-07-05 .. 2022-12-19 (77 weeks; extended T = 213).
  4. Weekly panel: no duplicate (entity, date) keys; no negative values in
     any numeric column.
  5. RAW INVENTORY: exactly the 18 monthly files RC_2021-07..RC_2022-12
     present in RAW_MONTHLY_DIR (missing month = FAIL).
  6. Daily panel (REQUIRED): no duplicate (entity, date) keys; no negative
     numerics; EVERY calendar day 2021-07-01 .. 2022-12-25 present
     (extension weeks + boundary days); weekly == sum(daily) EXACTLY for
     EVERY shared numeric metric column on every extension (entity, week);
     day-guard (trailing 28-day PRIOR-days median, 60% — the registered
     instrument_eras convention) flags 0 extension days.

ANY failure => nonzero exit. Failures are data problems, not modeling
degrees of freedom (protocol §2).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

FROZEN_LAST_WEEK = "2021-06-28"
EXT_FIRST_WEEK = "2021-07-05"
EXT_LAST_WEEK = "2022-12-19"
EXT_N_WEEKS = 77
EXT_FIRST_DAY = "2021-07-01"     # boundary days included in daily coverage
EXT_LAST_DAY = "2022-12-25"      # last day of the last complete week
MONTHS = pd.period_range("2021-07", "2022-12", freq="M")
ID, DT = "endpoint_id", "date"
GUARD_WINDOW, GUARD_FRAC = 28, 0.60


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


def _basic_hygiene(df, what):
    if df.duplicated([ID, DT]).any():
        _fail(f"duplicate (entity, date) keys in the {what} panel")
    for c in _num_cols(df):
        if (df[c] < 0).any():
            _fail(f"negative values in {what} column '{c}'")


def check(ext_weekly: str, frozen_weekly: str, ext_daily: str,
          raw_dir: str) -> None:
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

    # [3] weekly hygiene
    _basic_hygiene(ext, "extended weekly")
    print("  [3/6] weekly keys unique, all numerics non-negative: OK")

    # [4] raw inventory
    raw = Path(raw_dir)
    missing = [f"RC_{m}" for m in MONTHS.astype(str)
               if not any(raw.glob(f"RC_{m}*"))]
    if missing:
        _fail(f"raw monthly inventory incomplete: missing {missing}")
    print(f"  [4/6] raw inventory: OK (18 monthly files present)")

    # [5] daily panel: hygiene, calendar coverage, all-metric aggregation
    d = pd.read_parquet(ext_daily)
    d[DT] = pd.to_datetime(d[DT])
    dd = d[(d[DT] >= pd.Timestamp(EXT_FIRST_DAY))
           & (d[DT] <= pd.Timestamp(EXT_LAST_DAY))]
    _basic_hygiene(dd, "extension daily")
    have_days = pd.DatetimeIndex(np.sort(dd[DT].unique())).normalize()
    want_days = pd.date_range(EXT_FIRST_DAY, EXT_LAST_DAY, freq="D")
    missing_days = want_days.difference(have_days)
    if len(missing_days):
        _fail(f"{len(missing_days)} missing extension calendar days "
              f"(first: {list(missing_days.date)[:3]})")
    metrics = [c for c in _num_cols(ext) if c in dd.columns]
    if not metrics:
        _fail("no shared numeric metric columns between weekly and daily panels")
    dw = dd[dd[DT] >= pd.Timestamp(EXT_FIRST_WEEK)].copy()
    dw["wk"] = dw[DT] - pd.to_timedelta(dw[DT].dt.dayofweek, unit="D")
    dw = dw[dw["wk"] <= pd.Timestamp(EXT_LAST_WEEK)]
    ds = dw.groupby([ID, "wk"])[metrics].sum()
    ws = new.set_index([ID, DT])[metrics].sort_index()
    ds.index.names = ws.index.names
    if not ds.sort_index().reindex(ws.index).fillna(np.inf).equals(
            ws.astype(ds.dtypes.to_dict())):
        j = ds.sort_index().reindex(ws.index)
        bad = (j.fillna(-1) != ws.fillna(-2)).any(axis=1)
        _fail(f"weekly != sum(daily) on {int(bad.sum()):,} extension "
              f"(entity, week) cells across columns {metrics}")
    print(f"  [5/6] daily hygiene + full calendar coverage + "
          f"weekly=Σdaily on ALL metrics {metrics}: OK")

    # [6] day guard — trailing PRIOR-days median (instrument_eras convention)
    counts = dd.groupby(dd[DT].dt.normalize())[ID].size().reindex(want_days)
    trail = counts.rolling(GUARD_WINDOW, min_periods=7).median().shift(1)
    flagged = counts < GUARD_FRAC * trail
    if flagged.fillna(False).any():
        _fail(f"day-guard flagged {int(flagged.sum())} extension days "
              f"(census property violated)")
    print("  [6/6] day-guard (trailing prior-days median): 0 flagged: OK")

    print("PASS")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit(__doc__)
    check(*sys.argv[1:5])
