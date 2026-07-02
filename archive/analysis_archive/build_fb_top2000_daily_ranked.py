#!/usr/bin/env python3
"""
build_fb_top2000_daily_ranked.py
--------------------------------
• Reads each daily CrowdTangle Parquet file (posts) one-at-a-time
• Aggregates page-level interactions for that day
• Ranks pages, keeps the top-2 000
• Writes a single Parquet panel:
  ./data/rank-diffusion/fb_top2000_ranked_daily.parquet
"""

import duckdb
import pathlib
import os
import re
import datetime as dt

# ----------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------
RAW_DIR = pathlib.Path("/Volumes/My Passport for Mac/crowdtangle_backfill")
OUT_DIR = pathlib.Path("./data/rank-diffusion")
OUT_DIR.mkdir(parents=True, exist_ok=True)

daily_files = sorted(RAW_DIR.glob("*.parquet"))
if not daily_files:
    raise FileNotFoundError(f"No Parquet files found in {RAW_DIR}")

date_regex = re.compile(r"\d{4}-\d{2}-\d{2}")

# ----------------------------------------------------------------------
# DuckDB session
# ----------------------------------------------------------------------
con = duckdb.connect()
con.execute(f"PRAGMA threads = {os.cpu_count()};")
con.execute("SET memory_limit='12GB';")
con.execute("SET temp_directory='/tmp';")

con.execute("""
CREATE OR REPLACE TABLE fb_top2000_ranked_daily (
    date          DATE,
    endpoint_id   TEXT,
    metric_value  BIGINT,
    rank          INTEGER
);
""")

# ----------------------------------------------------------------------
# Interaction expression (with try() for absent column)
# ----------------------------------------------------------------------
INTERACTIONS_EXPR = """
    COALESCE("statistics.actual.likeCount", 0) +
    COALESCE("statistics.actual.shareCount", 0) +
    COALESCE("statistics.actual.commentCount", 0) +
    COALESCE("statistics.actual.loveCount", 0) +
    COALESCE("statistics.actual.wowCount", 0) +
    COALESCE("statistics.actual.hahaCount", 0) +
    COALESCE("statistics.actual.sadCount", 0) +
    COALESCE("statistics.actual.angryCount", 0) +
    COALESCE(try("statistics.actual.thankfulCount"), 0) +
    COALESCE("statistics.actual.careCount", 0)
"""

# ----------------------------------------------------------------------
# Process each daily Parquet file
# ----------------------------------------------------------------------
for idx, pfile in enumerate(daily_files, 1):
    m = date_regex.search(pfile.name)
    if m:
        file_date = dt.date.fromisoformat(m.group())
    else:  # fallback to min(date) inside file
        file_date = con.sql(
            "SELECT MIN(CAST(date AS DATE)) FROM parquet_scan(?)",
            [str(pfile)]
        ).fetchone()[0]

    con.execute(
        f"""
        INSERT INTO fb_top2000_ranked_daily
        WITH p AS (
            SELECT
                "account.name" AS page,
                {INTERACTIONS_EXPR} AS interactions
            FROM parquet_scan(?, HIVE_PARTITIONING=0, union_by_name=true)
        ),
        page_tot AS (
            SELECT page, SUM(interactions) AS metric_value
            FROM p
            GROUP BY page
        ),
        ranked AS (
            SELECT
                ?::DATE                     AS date,
                page                        AS endpoint_id,
                metric_value,
                DENSE_RANK() OVER (ORDER BY metric_value DESC) AS rank
            FROM page_tot
            ORDER BY rank
            LIMIT 2000
        )
        SELECT * FROM ranked;
        """,
        [str(pfile), file_date]
    )

    if idx % 50 == 0 or idx == len(daily_files):
        print(f"  processed {idx}/{len(daily_files)} files …")

# ----------------------------------------------------------------------
# Final COPY  (embed path literal—no ? placeholder!)
# ----------------------------------------------------------------------
out_path = OUT_DIR / "fb_top2000_ranked_daily.parquet"
con.execute(
    f"""
    COPY fb_top2000_ranked_daily
    TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION 'zstd');
    """
)

con.close()
print(f"✔︎ Completed – panel saved to {out_path}")
