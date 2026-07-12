#!/usr/bin/env python3
"""A7 prefix-preserving extension assembler (round-6 review: a checker
existed but no builder — the official pipeline folds every date to its
Monday, so a naive rebuild pushes July 1-4 2021 into the frozen partial
2021-06-28 TRAINING week).

Constructs the registered extended weekly panel as:
    frozen weekly rows, byte-identical (INCLUDING the partial 2021-06-28
    week as frozen)
  + weekly sums of the extension dailies for COMPLETE weeks
    2021-07-05 .. 2022-12-19 only (all frozen numeric columns summed).

Boundary days (2021-07-01..04 and 2022-12-20..31) are EXCLUDED from weekly
rows and written to a separate boundary-days parquet (reported, never
silently discarded — A6.1).

Usage:
  python llm_fitting/build_extension_weekly.py FROZEN_WEEKLY EXT_DAILY OUT_WEEKLY
Then run check_extension_panel.py on the output before any model contact.
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

FROZEN_LAST_WEEK = pd.Timestamp("2021-06-28")
EXT_FIRST_WEEK = pd.Timestamp("2021-07-05")
EXT_LAST_WEEK = pd.Timestamp("2022-12-19")
ID, DT = "endpoint_id", "date"


def assemble(frozen_weekly: str, ext_daily: str, out_weekly: str) -> None:
    fro = pd.read_parquet(frozen_weekly)
    d = pd.read_parquet(ext_daily)
    d[DT] = pd.to_datetime(d[DT])
    # dailies strictly AFTER the frozen daily end (2021-06-30) are extension
    # data; July 1-4 land in the frozen partial week's Monday and are
    # boundary days by construction (their wk < EXT_FIRST_WEEK)
    d = d[d[DT] > pd.Timestamp("2021-06-30")]
    d["wk"] = d[DT] - pd.to_timedelta(d[DT].dt.dayofweek, unit="D")

    in_window = (d["wk"] >= EXT_FIRST_WEEK) & (d["wk"] <= EXT_LAST_WEEK)
    boundary = d[~in_window]
    if len(boundary):
        bpath = out_weekly.replace(".parquet", "_boundary_days.parquet")
        boundary.drop(columns=["wk"]).to_parquet(bpath, index=False)
        print(f"boundary days EXCLUDED from weekly rows, written to {bpath}: "
              f"{len(boundary):,} rows, dates "
              f"{boundary[DT].min().date()}..{boundary[DT].max().date()}")

    metrics = [c for c in fro.columns if c not in (ID, DT)
               and np.issubdtype(fro[c].dtype, np.number)]
    missing = [c for c in metrics if c not in d.columns]
    if missing:
        raise SystemExit(f"daily panel lacks frozen metric columns {missing} "
                         f"-- cannot assemble the registered schema")
    wk = (d[in_window].groupby([ID, "wk"], as_index=False)[metrics].sum()
          .rename(columns={"wk": DT}))
    for c in fro.columns:
        if c not in wk.columns:
            raise SystemExit(f"internal: assembled panel missing column {c}")
    wk = wk[list(fro.columns)]

    out = pd.concat([fro, wk], ignore_index=True).sort_values([DT, ID])
    out.to_parquet(out_weekly, index=False)
    n_new = wk[DT].nunique()
    print(f"wrote {out_weekly}: frozen prefix {len(fro):,} rows (unchanged) "
          f"+ {len(wk):,} extension rows over {n_new} complete weeks "
          f"({EXT_FIRST_WEEK.date()}..{EXT_LAST_WEEK.date()})")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    assemble(*sys.argv[1:4])
