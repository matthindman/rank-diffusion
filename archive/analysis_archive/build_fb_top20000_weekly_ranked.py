#!/usr/bin/env python3
"""
build_fb_top20000_weekly_ranked.py
----------------------------------
• Reads each daily CrowdTangle Parquet file (posts) one-at-a-time
• Aggregates page-level interactions by ISO week (week starts Monday)
• Ranks pages per week, keeps the top-100 000
• Writes a single Parquet panel:
  ./data/rank-diffusion/fb_top100000_ranked_weekly.parquet

Notes
- Input Parquet files are expected to be daily dumps of CrowdTangle posts.
- We sum per-page interactions within each day, then aggregate to weeks.
- Week key uses DuckDB date_trunc('week', date) which is Monday-based (ISO).
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

# No large staging tables: process week-by-week directly from source files.

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
# Group input files by ISO week and process week-by-week
# ----------------------------------------------------------------------
def week_start_for(d: dt.date) -> dt.date:
    return d - dt.timedelta(days=d.weekday())  # Monday start

week_map: dict[dt.date, list[pathlib.Path]] = {}
for pfile in daily_files:
    m = date_regex.search(pfile.name)
    if m:
        file_date = dt.date.fromisoformat(m.group())
    else:  # fallback to min(date) inside file
        file_date = con.sql(
            "SELECT MIN(CAST(date AS DATE)) FROM parquet_scan(?)",
            [str(pfile)]
        ).fetchone()[0]
    ws = week_start_for(file_date)
    week_map.setdefault(ws, []).append(pfile)

weeks_sorted = sorted(week_map.keys())
print(f"Found {len(weeks_sorted)} weeks to process …")

# Prepare final table schema (matches daily file's schema)
con.execute(
    """
    CREATE OR REPLACE TABLE fb_top100000_ranked_weekly (
        date          DATE,
        endpoint_id   TEXT,
        metric_value  BIGINT,
        rank          INTEGER
    );
    """
)

for widx, ws in enumerate(weeks_sorted, 1):
    files = week_map[ws]
    # Build read_parquet([...]) with properly quoted file paths
    file_list_sql = ",".join(
        ["'" + f.as_posix().replace("'", "''") + "'" for f in files]
    )
    read_src = f"read_parquet([{file_list_sql}], union_by_name=true)"

    con.execute(
        f"""
        WITH p AS (
            SELECT
                "account.name" AS endpoint_id,
                {INTERACTIONS_EXPR} AS interactions
            FROM {read_src}
        ), weekly AS (
            SELECT endpoint_id, SUM(interactions) AS metric_value
            FROM p
            GROUP BY 1
        ), ranked AS (
            SELECT
                ?::DATE AS date,
                endpoint_id,
                metric_value,
                CAST(DENSE_RANK() OVER (ORDER BY metric_value DESC) AS INTEGER) AS rank,
                ROW_NUMBER() OVER (ORDER BY metric_value DESC) AS rn
            FROM weekly
        )
        INSERT INTO fb_top100000_ranked_weekly
        SELECT date, endpoint_id, metric_value, rank
        FROM ranked
        WHERE rn <= 100000;
        """,
        [ws]
    )

    if widx % 4 == 0 or widx == len(weeks_sorted):
        print(f"  processed week {widx}/{len(weeks_sorted)} (files: {len(files)}) …")

# ----------------------------------------------------------------------
# Rank within each week (top-20 000), with consistent schema
# ----------------------------------------------------------------------
# Note: table population is performed in the week-by-week loop above.

# ----------------------------------------------------------------------
# Final COPY  (embed path literal—no ? placeholder!)
# ----------------------------------------------------------------------
out_path = OUT_DIR / "fb_top100000_ranked_weekly.parquet"
con.execute(
    f"""
    COPY fb_top100000_ranked_weekly
    TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION 'zstd');
    """
)

con.close()
print(f"✔︎ Completed – weekly (top-100k) panel saved to {out_path}")
