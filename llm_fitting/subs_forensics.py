#!/usr/bin/env python3
"""P2 census-grade instrument forensics for reddit_submissions_long
(PREREG_2026-07-16 Phase 1). Day-guard 0 flags + full calendar coverage were
established by the Phase-0 gate; this adds the remaining P2 readouts:
smooth new-id inflow and a top-of-ladder eyeball. Descriptive; the census
classification is the judgement, not a single threshold."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

df = mrd.load_panel(mrd.PLATFORMS["reddit_submissions_long"])
T = int(df["period"].max()) + 1
print(f"reddit_submissions_long: {len(df):,} (entity,week) rows, T={T} weeks, "
      f"{df['entity_id'].nunique():,} distinct entities")

# entities per week
per_wk = df.groupby("period")["entity_id"].nunique()
print(f"\nentities/week: min {per_wk.min():,} median {int(per_wk.median()):,} "
      f"max {per_wk.max():,}")

# new-id inflow: first period each entity appears; count per week (skip wk 0)
first_seen = df.groupby("entity_id")["period"].min()
new_per_wk = first_seen.value_counts().reindex(range(T), fill_value=0).sort_index()
nz = new_per_wk.iloc[1:]                       # week 0 is all-new by construction
print(f"\nnew-ids/week (weeks 1..{T - 1}): min {nz.min():,} "
      f"median {int(nz.median()):,} mean {nz.mean():.0f} max {nz.max():,}")
# smoothness: robust coefficient of variation + largest single-week jump ratio
cv = nz.std() / nz.mean()
ratio = (nz / nz.shift(1)).replace([np.inf, -np.inf], np.nan).dropna()
print(f"  CV {cv:.3f}; largest wk/wk inflow ratio {ratio.max():.2f} "
      f"(smooth census => no order-of-magnitude enrollment spikes)")
print(f"  first 6 weeks new-ids: {list(nz.iloc[:6].astype(int))}")
print(f"  last 6 weeks new-ids:  {list(nz.iloc[-6:].astype(int))}")

# top-of-ladder eyeball at three sample weeks
print("\ntop-5 by metric (eyeball; expect recognizable subreddits):")
for frac in (0.05, 0.5, 0.95):
    p = int(round(frac * (T - 1)))
    wk = df[df["period"] == p].nlargest(5, "metric")
    ts = wk["ts"].iloc[0].date() if "ts" in wk else "?"
    names = ", ".join(f"{r.entity_id}" for r in wk.itertuples())
    print(f"  week {p:>3} ({ts}): {names}")
