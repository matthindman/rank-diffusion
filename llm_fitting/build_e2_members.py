#!/usr/bin/env python3
"""A5/E2 frozen membership builder (2026-07-11; MODEL_STATUS 2z-i).

Selects the E2 confirmation universe from the EXISTING T=136 comments panel
ONLY (2018-12..2021-06) -- the train side of the registered single-block
forecast -- using the program's standard rule (restrict_universe: K=12,500,
B=4K=50,000, absence-penalized permanent rank over the full T=136 window,
which is train-only RELATIVE TO THE EXTENSION).  Writes
`e2_members_t136.parquet` with columns (T0=136, entity_id) for
`rankdiff_kalman --member-ids-file`, prints the SHA-256 (recorded in the run
archive per A5 BEFORE any scoring), and validates 50,000 unique ids.

Reads NO extension data.  Period-alignment assumption (checked at E2 run
time): the extended panel's periods 0..135 must be the same calendar weeks
as this panel's 0..135 (both series start 2018-12; the E2 runner must verify
the period-135 date matches before scoring).

Usage:
  python llm_fitting/build_e2_members.py
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

K, BUF_MULT, T0 = 12_500, 4, 136
OUT = "llm_fitting/e2_members_t136.parquet"


def main() -> None:
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments"])
    T = int(df["period"].max()) + 1
    assert T == T0, f"expected the T=136 comments panel, got T={T}"
    last_date = df.loc[df["period"] == T0 - 1, "ts"].iloc[0] \
        if "ts" in df.columns else "n/a"
    uni = mrd.restrict_universe(df, K, buffer_mult=BUF_MULT)
    ids = sorted(uni["entity_id"].astype(str).unique())
    assert len(ids) == BUF_MULT * K == 50_000, f"expected 50,000 ids, got {len(ids):,}"
    out = pd.DataFrame({"T0": T0, "entity_id": ids})
    out.to_parquet(OUT, index=False)
    sha = hashlib.sha256(Path(OUT).read_bytes()).hexdigest()
    print(f"wrote {OUT}: {len(out):,} rows (T0={T0}, {len(ids):,} unique ids)")
    print(f"sha256: {sha}")
    print(f"panel period {T0 - 1} date: {last_date}  "
          f"(E2 runner must verify the extended panel matches)")


if __name__ == "__main__":
    main()
