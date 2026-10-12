"""Stage B: the one value-level check declared in DESIGN.md, on DEVELOPMENT data only.

For one daily file per platform and metric: the parquet footer schema, and three scalars per
metric column (share of rows equal to zero, share of non-integer values, minimum). Nothing under
the sealed Wikipedia tree or the Reddit bridge is named, opened or listed. Read-only.

Facebook rows are restricted to Era A (2020-11-02..2022-06-27); Eras B, M and D are RESTRICTED in
the exposure ledger and are filtered out before any column is read.
Run with the registered interpreter:  python3 instrument_scalars.py  ->  instrument_scalars.json
"""
import json

import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = "/Volumes/T9/rank-diffusion-data"
OUT = {}


def footer(path):
    pf = pq.ParquetFile(path)
    return dict(path=path, rows=pf.metadata.num_rows, columns=[f"{f.name}: {f.type}" for f in pf.schema_arrow])


def scalars(arr):
    a = arr.to_numpy(zero_copy_only=False)
    a = a[~np.isnan(a.astype("float64"))]
    return dict(n=int(a.size), dtype=str(arr.type), share_zero=float(np.mean(a == 0)),
                share_non_integer=float(np.mean(a != np.floor(a))), minimum=float(a.min()), share_negative=float(np.mean(a < 0)))


# 1. Facebook page-day panel, Era A only
p = f"{ROOT}/derived/fb_daily.parquet"
OUT["fb_daily"] = footer(p)
d = ds.dataset(p)
import pandas as pd
flt = (pc.field("date") >= pd.Timestamp("2020-11-02")) & (pc.field("date") <= pd.Timestamp("2022-06-27"))
cols = [c for c in ("metric_value", "post_count") if c in d.schema.names]
tab = d.to_table(columns=cols, filter=flt)
OUT["fb_daily"]["era_A_rows"] = tab.num_rows
for c in cols:
    OUT["fb_daily"][c] = scalars(tab[c].combine_chunks())

# 2. One raw CrowdTangle post-level day file inside Era A: column names only
OUT["fb_raw_post_level_schema"] = footer(f"{ROOT}/raw_small/facebook/crowdtangle_backfill/2021-03-01--2021-03-02_test.parquet")

# 3. Reddit subreddit-day monthly aggregates, one month inside the 2018-12..2022-12 instrument
for kind in ("comments", "submissions"):
    p = f"{ROOT}/aggregates/reddit/monthly/{kind}/{kind}_2019-06.parquet"
    OUT[f"reddit_{kind}_2019-06"] = footer(p)
    t = pq.read_table(p, columns=["count", "score_sum"])
    OUT[f"reddit_{kind}_2019-06"]["count"] = scalars(t["count"].combine_chunks())
    OUT[f"reddit_{kind}_2019-06"]["score_sum"] = scalars(t["score_sum"].combine_chunks())

# 4. Derived Reddit daily panels: footers only
OUT["reddit_daily_long"] = footer(f"{ROOT}/derived/reddit_daily_long.parquet")
OUT["reddit_comments_daily"] = footer(f"{ROOT}/derived/reddit_comments_2018-12_2021-06_daily.parquet")

# 5. Instagram post-level 2023: footer only (ledger: structural contact already made)
OUT["instagram_post_level_schema"] = footer(f"{ROOT}/raw_small/instagram/full_ig.parquet")

with open("instrument_scalars.json", "w") as f:
    json.dump(OUT, f, indent=1)
for k, v in OUT.items():
    print(k, v["rows"], "rows")
    print("   ", "; ".join(v["columns"])[:1500])
    for c in ("metric_value", "post_count", "count", "score_sum"):
        if c in v:
            print("   ", c, v[c])
