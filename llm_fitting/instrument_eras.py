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

SCORE-CAPTURE GUARD, REDDIT (declared 2026-10-14 on the owner's decision;
MODEL_STATUS 2z-ca, 2z-cb).  The Reddit panels are sums of scores, and the
archive read each item's score some time after it was posted.  Where that
delay was hours or less the scores are short.  This is invisible to the
low-count-day guard: the items are all there, their scores are not.  A day is
flagged when the platform's karma per item falls below SCORE_GUARD_FRAC
(=0.70) of the median of the same weekday in the SCORE_GUARD_WEEKS (=4) weeks
before and the four after (at least four of those eight days must exist).
Measured on the daily panels of 2018-12-01..2022-12-31 (1,492 days):
  submissions  2020-04-28 (0.57), 2020-04-29 (0.015), 2020-04-30 (0.015),
               2021-06-12 (0.63).  On the two days at 0.015 the scores are
               about 1 per submission.
  comments     none.
REDDIT_SCORE_FLAGGED_DAYS records these so that no drive is needed to use
them; `python instrument_eras.py --score-guard` re-derives them from the
daily panels.  Usage:
  * NOTHING changes by default.  Frozen stacks, fits and gates keep every week.
  * Analyses of levels (trends, block means, anything in log units over time)
    report their result with the flagged weeks left out beside the result with
    them in.  In shares and ranks the damage is mostly common to every
    subreddit; reading scores early raises the largest subreddits' share
    (2z-ca), so it is not exactly common.
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

SCORE_GUARD_FRAC = 0.70   # flag a day when karma per item < this fraction of the local same-weekday median
SCORE_GUARD_WEEKS = 4     # weeks on each side
SCORE_GUARD_MIN = 4       # of the 2 * SCORE_GUARD_WEEKS neighbouring days, at least this many must exist
REDDIT_SCORE_FLAGGED_DAYS = {
    "submissions": ("2020-04-28", "2020-04-29", "2020-04-30", "2021-06-12"),
    "comments": (),
}

SSD = "data/ssd/derived"
DAILY_PATHS = {
    "facebook": f"{SSD}/fb_daily.parquet",
    "reddit_comments": f"{SSD}/reddit_comments_2018-12_2021-06_daily.parquet",
}
WEEKLY_PATHS = {
    "facebook": f"{SSD}/fb_weekly_rebuilt.parquet",
    "reddit_comments": f"{SSD}/reddit_comments_2018-12_2021-06_weekly.parquet",
}
# Daily panels for the score-capture guard: (path, column holding the number of items).
SCORE_DAILY = {
    "submissions": (f"{SSD}/reddit_daily_long.parquet", "submission_count"),
    "comments": (f"{SSD}/reddit_comments_2018-12_2022-12_daily.parquet", "comment_count"),
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


def karma_per_item_series(kind: str) -> pd.Series:
    """Platform karma divided by the platform count of items, per day, from a
    Reddit daily panel.  Reads three columns, one row group at a time."""
    import pyarrow.parquet as pq
    path, cnt = SCORE_DAILY[kind]
    pf = pq.ParquetFile(path)
    parts = []
    for i in range(pf.metadata.num_row_groups):
        t = pf.read_row_group(i, columns=["date", "metric_value", cnt]).to_pandas()
        parts.append(t.groupby("date")[["metric_value", cnt]].sum())
    tot = pd.concat(parts).groupby(level=0).sum().sort_index()
    tot.index = pd.to_datetime(tot.index)
    return tot["metric_value"] / tot[cnt]


def score_ratio(kpi: pd.Series, weeks: int = SCORE_GUARD_WEEKS) -> pd.Series:
    """Each day's karma per item over the median of the same weekday in the
    `weeks` weeks before and after.  NaN where fewer than SCORE_GUARD_MIN of
    those days exist."""
    v = kpi.reindex(pd.date_range(kpi.index.min(), kpi.index.max()))
    nb = pd.concat([v.shift(7 * j, freq="D").reindex(v.index) for j in range(-weeks, weeks + 1) if j], axis=1)
    med = nb.median(axis=1).where(nb.notna().sum(axis=1) >= SCORE_GUARD_MIN)
    return (v / med).reindex(kpi.index)


def flag_score_days(kpi: pd.Series, frac: float = SCORE_GUARD_FRAC,
                    weeks: int = SCORE_GUARD_WEEKS) -> pd.DatetimeIndex:
    """Days whose karma per item is below frac of the local same-weekday median."""
    r = score_ratio(kpi, weeks)
    return r.index[r < frac]


def reddit_flagged_weeks(kind: str) -> set:
    """Monday-anchored weeks containing a recorded score-flagged day."""
    return weeks_containing(pd.DatetimeIndex(pd.to_datetime(list(REDDIT_SCORE_FLAGGED_DAYS[kind]))))


def profile_scores(kind: str) -> None:
    """Re-derive the score-capture flags from the daily panel and compare them
    with the recorded list."""
    kpi = karma_per_item_series(kind)
    r = score_ratio(kpi)
    days = flag_score_days(kpi)
    print(f"reddit {kind}: {len(kpi)} days {kpi.index.min().date()}..{kpi.index.max().date()}; "
          f"{len(days)} days below {SCORE_GUARD_FRAC:.0%} of the local same-weekday median")
    for d in days:
        print(f"      {d.date()}  ratio {r[d]:.3f}  karma per item {kpi[d]:.2f}")
    recorded = set(pd.to_datetime(list(REDDIT_SCORE_FLAGGED_DAYS[kind])))
    print(f"  matches the recorded list: {set(days) == recorded}; weeks: "
          f"{[str(pd.Timestamp(x).date()) for x in sorted(weeks_containing(days))]}")


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
    # no `choices` on the positional: with nargs="*" and nothing given, argparse (3.11) checks
    # the empty list itself against the choices and rejects it, so the bare command failed
    ap.add_argument("platforms", nargs="*", default=None,
                    help=f"any of {', '.join(DAILY_PATHS)}; default: all")
    ap.add_argument("--score-guard", action="store_true",
                    help="instead: re-derive the Reddit score-capture flags from the daily panels")
    args = ap.parse_args()
    for p in args.platforms or []:
        if p not in DAILY_PATHS:
            ap.error(f"unknown platform {p!r} (choose from {', '.join(DAILY_PATHS)})")
    if args.score_guard:
        for kind in SCORE_DAILY:
            profile_scores(kind)
    else:
        for p in args.platforms or ["facebook", "reddit_comments"]:
            profile(p)
