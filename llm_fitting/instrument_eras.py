#!/usr/bin/env python3
"""CrowdTangle instrument eras + low-count-day guard (P0, 2026-07-03).

The CrowdTangle FB collection is NON-STATIONARY as an instrument (MODEL_STATUS
2g addendum): a fixed ~14.5k-page panel with frozen enrollment, a mid-series
collection collapse, a 2023 two-source mixture, and a terminal decline into the
2024 shutdown.  Standard instrument-health segmentation applies: breakpoints
come from collection METADATA ONLY (pages/day, pages/week, new-ids/week) --
never from model fit -- and no estimation or evaluation window may straddle a
broken era (B or M).

This module is the canonical committed record of (i) the measured era table,
(ii) the low-count-day guard used by all daily/Spec-B work, and (iii) the
health-profile CLI that reproduces the verification numbers.

P0 VERIFICATION (2026-07-03, re-derived from the rebuilt SSD panels):
  era A  2020-11-02..2022-06-27  86 complete wks | pages/wk med 14,362 (13,771..14,714)
         pages/day med 12,644 | new-ids/wk med 8 (enrollment frozen) | 15 flagged days
  era B  2022-06-28..2022-09-30   6 complete wks | wk min 6,834; day med 5,712,
         24/82 days flagged -> collapse CONFIRMED, exclude
  era C  2022-10-01..2022-12-31  12 complete wks | pages/wk med 13,812; day med 11,624;
         3 flagged days -> replication with caution
  era M  2023-01-01..2023-12-31  52 complete wks | weekly unions to 420,592 pages
         (two-source mixture; new-ids/wk max 143,915) -> UNUSABLE as built
  era D  2024-01-01..2024-03-06  ** AMENDED: only 2 complete wks ** (2024-01-01,
         2024-02-12; 45/66 days present -- the Jan..Mar 2024 daily gaps kill
         complete-week coverage; the 2g table's "~6-9 complete wks" was wrong).
         0 flagged days, day med 10,817.  Weekly estimation on D is INFEASIBLE;
         only daily (noise-floor) statistics are estimable, robustness only.
Reddit comments panels (2018-12..2021-06): CENSUS CONFIRMED -- 0/943 days
flagged, smooth growth 31k->71k subs/day, ~13k organically new ids/wk.  No
eras; the era machinery does not apply to Reddit.

LOW-COUNT-DAY GUARD (declared): a day is flagged when its page count falls
below GUARD_FRAC (=0.60) of the trailing GUARD_WINDOW (=28)-day median count
(min 7 prior days; first days of a panel are never flagged).  Era A contains
15 such hidden bad days (e.g. 2022-04-15: 94 pages) that are invisible to the
complete-week filter.  Usage:
  * Spec-B / daily noise-floor estimation: DROP every week containing a
    flagged day (partial-day counts inflate the within-week variance).
  * WEEKLY fits: flagged weeks are KEPT (declared decision) -- a platform-wide
    undercount week is mostly absorbed by the per-period common factor, the
    trusted-panel keystone already contained the same collection holes, and
    dropping interior weeks would break the consecutive-week change pairs the
    MD/ACF estimators rely on.  They are reported, not silently used.
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

# Inclusive calendar spans; apply to weekly Monday stamps and daily dates alike.
# Breakpoints from collection metadata only (see module docstring).
FB_ERAS = {
    "A": ("2020-11-02", "2022-06-27"),   # PRIMARY (matches trusted cutdown panel)
    "B": ("2022-06-28", "2022-09-30"),   # collapse -- EXCLUDE
    "C": ("2022-10-01", "2022-12-31"),   # recovery -- replication w/ caution
    "M": ("2023-01-01", "2023-12-31"),   # two-source mixture -- UNUSABLE as built
    "D": ("2024-01-01", "2024-03-06"),   # terminal; 2 complete wks -- daily stats only
}

GUARD_WINDOW = 28    # trailing days for the median
GUARD_FRAC = 0.60    # flag a day when count < GUARD_FRAC * trailing median
GUARD_MIN_PERIODS = 7

SSD = "data/ssd/derived"
DAILY_PATHS = {
    "facebook": f"{SSD}/fb_daily.parquet",
    "reddit_comments": f"{SSD}/reddit_comments_2018-12_2021-06_daily.parquet",
}
WEEKLY_PATHS = {
    "facebook": f"{SSD}/fb_weekly_rebuilt.parquet",
    "reddit_comments": f"{SSD}/reddit_comments_2018-12_2021-06_weekly.parquet",
}


def day_count_series(path: str) -> pd.Series:
    """Entities per day from a derived daily panel.  The derived panels have
    zero duplicate (date, endpoint_id) keys (validated), so row counts per
    date equal distinct-entity counts -- lets us read one column only."""
    d = pd.read_parquet(path, columns=["date"])
    return pd.to_datetime(d["date"]).value_counts().sort_index()


def flag_days(counts: pd.Series, window: int = GUARD_WINDOW,
              frac: float = GUARD_FRAC) -> pd.DatetimeIndex:
    """Days whose count falls below frac * trailing-median (prior days only)."""
    trailing = counts.rolling(window, min_periods=GUARD_MIN_PERIODS).median().shift(1)
    return counts.index[counts < frac * trailing]


def weeks_containing(days: pd.DatetimeIndex) -> set:
    """Monday-anchored weeks that contain any of the given days."""
    days = pd.DatetimeIndex(days)
    return set(days - pd.to_timedelta(days.weekday, unit="D"))


def era_span(era: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    lo, hi = FB_ERAS[era]
    return pd.Timestamp(lo), pd.Timestamp(hi)


def profile(platform: str) -> None:
    """Reproduce the P0 collection-health verification tables."""
    counts = day_count_series(DAILY_PATHS[platform])
    flags = flag_days(counts)
    print(f"{platform}: {len(counts)} days {counts.index.min().date()}"
          f"..{counts.index.max().date()}; {len(flags)} flagged days "
          f"(<{GUARD_FRAC:.0%} of trailing {GUARD_WINDOW}d median)")

    w = pd.read_parquet(WEEKLY_PATHS[platform], columns=["date", "endpoint_id"])
    w["date"] = pd.to_datetime(w["date"])
    wk = w.groupby("date")["endpoint_id"].nunique().sort_index()
    new = w.groupby("endpoint_id")["date"].min().value_counts().sort_index()
    prof = pd.DataFrame({"pages": wk, "new_ids": new}).fillna(0).astype(int)

    spans = (FB_ERAS.items() if platform == "facebook"
             else [("census", (str(counts.index.min().date()),
                               str(counts.index.max().date())))])
    for era, (lo, hi) in spans:
        md = counts[(counts.index >= lo) & (counts.index <= hi)]
        fl = [d for d in flags if lo <= str(d.date()) <= hi]
        pw = prof[(prof.index >= lo) & (prof.index <= hi)]
        if md.empty and pw.empty:
            continue
        print(f"  era {era}: {len(pw):3d} complete wks | pages/wk med "
              f"{int(pw['pages'].median()) if len(pw) else 0:>7,} "
              f"(min {int(pw['pages'].min()) if len(pw) else 0:,}) | "
              f"pages/day med {int(md.median()) if len(md) else 0:>7,} | "
              f"new-ids/wk med {int(pw['new_ids'].iloc[1:].median()) if len(pw) > 1 else 0:>6,} | "
              f"flagged days {len(fl)}")
        if fl and era in ("A", "C", "D"):
            print(f"      flagged: {[(str(d.date()), int(counts[d])) for d in fl]}")
    bad_wk = sorted(weeks_containing(flags))
    print(f"  weeks containing flagged days: {len(bad_wk)}"
          f" -> {[str(pd.Timestamp(x).date()) for x in bad_wk]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("platforms", nargs="*", default=["facebook", "reddit_comments"],
                    choices=list(DAILY_PATHS))
    args = ap.parse_args()
    for p in args.platforms:
        profile(p)
