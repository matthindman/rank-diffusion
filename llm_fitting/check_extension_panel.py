#!/usr/bin/env python3
"""A6.1 intake gate for the comments extension (CONFIRMATION_PROTOCOL §11).

Must print PASS before any model code touches the extension. Enforces,
mechanically, every computable intake stop rule:

  1. FROZEN-PREFIX EQUALITY: every row of the extended weekly panel with
     date <= 2021-06-28 must be exactly equal (keys AND values, all shared
     columns) to the frozen T=136 panel — INCLUDING the partial 2021-06-28
     week as frozen (dailies ended Wed 2021-06-30; a naive rebuild folds
     July 1-4 extension days into that row, which is E2 TRAINING period
     135, invisible to the date anchor — the A6.1 measured leak).
  2. COMPLETE-WEEK WINDOW: extension weekly rows are exactly the complete
     weeks 2021-07-05 .. 2022-12-19 (77 weeks; extended T = 213; period
     136 = week of 2021-07-05). July 1-4 2021 and the partial 2022-12-26
     week must NOT appear as weekly rows.
  3. No duplicate (entity, date) keys; no negative metrics.
  4. weekly = sum(daily) exactly on every extension week (when the extended
     daily panel is supplied).
  5. Day-guard: 0 flagged days on the extension dailies (census property).

Any failure => FAIL (nonzero exit). Failures are data problems, not
modeling degrees of freedom (protocol §2).

Usage:
  python llm_fitting/check_extension_panel.py EXT_WEEKLY FROZEN_WEEKLY [EXT_DAILY]
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

FROZEN_LAST_WEEK = "2021-06-28"
EXT_FIRST_WEEK = "2021-07-05"
EXT_LAST_WEEK = "2022-12-19"
EXT_N_WEEKS = 77
ID, DT, MV = "endpoint_id", "date", "metric_value"


class IntakeFailure(SystemExit):
    pass


def _fail(msg):
    raise IntakeFailure(f"INTAKE FAIL: {msg}")


def verify_frozen_prefix(ext_path: str, frozen_path: str) -> None:
    """Rows of the extended panel with date <= FROZEN_LAST_WEEK must equal
    the frozen panel exactly on all shared columns."""
    fro = pd.read_parquet(frozen_path)
    ext = pd.read_parquet(ext_path)
    cols = [c for c in fro.columns if c in ext.columns]
    fro = fro[cols].copy()
    pre = ext[pd.to_datetime(ext[DT]) <= pd.Timestamp(FROZEN_LAST_WEEK)][cols].copy()
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


def check(ext_weekly: str, frozen_weekly: str, ext_daily: str | None = None) -> None:
    verify_frozen_prefix(ext_weekly, frozen_weekly)
    print("  [1/5] frozen-prefix equality: OK")

    ext = pd.read_parquet(ext_weekly)
    ext[DT] = pd.to_datetime(ext[DT])
    new = ext[ext[DT] > pd.Timestamp(FROZEN_LAST_WEEK)]
    weeks = pd.DatetimeIndex(np.sort(new[DT].unique()))
    want = pd.date_range(EXT_FIRST_WEEK, EXT_LAST_WEEK, freq="7D")
    if len(want) != EXT_N_WEEKS:
        _fail(f"internal: expected-week grid has {len(want)} != {EXT_N_WEEKS}")
    if not weeks.equals(want):
        extra = weeks.difference(want)
        missing = want.difference(weeks)
        _fail(f"extension week set wrong: extra={list(extra.date)[:4]} "
              f"missing={list(missing.date)[:4]} "
              f"(must be exactly {EXT_FIRST_WEEK}..{EXT_LAST_WEEK})")
    print(f"  [2/5] complete-week window: OK ({EXT_N_WEEKS} weeks, "
          f"period 136 = {EXT_FIRST_WEEK})")

    if ext.duplicated([ID, DT]).any():
        _fail("duplicate (entity, date) keys in the extended weekly panel")
    if (ext[MV] < 0).any():
        _fail("negative metric values in the extended weekly panel")
    print("  [3/5] keys unique, metrics non-negative: OK")

    if ext_daily is not None:
        d = pd.read_parquet(ext_daily)
        d[DT] = pd.to_datetime(d[DT])
        d = d[d[DT] > pd.Timestamp(FROZEN_LAST_WEEK) + pd.Timedelta(days=6)]
        d["wk"] = d[DT] - pd.to_timedelta(d[DT].dt.dayofweek, unit="D")
        d = d[(d["wk"] >= pd.Timestamp(EXT_FIRST_WEEK))
              & (d["wk"] <= pd.Timestamp(EXT_LAST_WEEK))]
        ds = d.groupby([ID, "wk"])[MV].sum()
        ws = new.set_index([ID, DT])[MV]
        j = pd.concat([ds.rename("daily"), ws.rename("weekly")], axis=1)
        bad = j["daily"].fillna(-1) != j["weekly"].fillna(-2)
        if bad.any():
            _fail(f"weekly != sum(daily) on {int(bad.sum()):,} extension "
                  f"(entity, week) cells")
        print("  [4/5] weekly = sum(daily) on the extension: OK")
        counts = d.groupby(d[DT].dt.date)[ID].size()
        med = counts.rolling(28, min_periods=7).median()
        flagged = counts < 0.6 * med
        if flagged.fillna(False).any():
            _fail(f"day-guard flagged {int(flagged.sum())} extension days "
                  f"(census property violated)")
        print("  [5/5] day-guard 0 flagged extension days: OK")
    else:
        print("  [4/5,5/5] SKIPPED (no extension daily panel supplied) -- "
              "the full intake gate REQUIRES the daily checks")

    print("PASS")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    check(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
