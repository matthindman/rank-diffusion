#!/usr/bin/env python3
"""
Quick analysis of gap structure for near-complete pages.
How long are the gaps? Where do they occur? What are the metric values
before/after gaps?
"""

import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# ── Load data ─────────────────────────────────────────────────────
df = pd.read_parquet('/Users/hindman/Documents/github/rank-diffusion/data/raw/fb_ranked_weekly_cutdown.parquet')
df['date'] = pd.to_datetime(df['date'])
dates = sorted(df['date'].unique())
n_weeks = len(dates)
date_to_idx = {d: i for i, d in enumerate(dates)}
df['week_idx'] = df['date'].map(date_to_idx)

ep_counts = df.groupby('endpoint_id')['date'].nunique()
bp_eps = set(ep_counts[ep_counts == n_weeks].index)

# Focus on non-BP pages present >= 50% of weeks
nonbp_eps = ep_counts[(ep_counts < n_weeks) & (ep_counts >= n_weeks * 0.50)]
print(f"Pages present >=50% but not all weeks: {len(nonbp_eps)}")
print(f"  >=95%: {(nonbp_eps >= n_weeks * 0.95).sum()}")
print(f"  90-94%: {((nonbp_eps >= n_weeks * 0.90) & (nonbp_eps < n_weeks * 0.95)).sum()}")
print(f"  80-89%: {((nonbp_eps >= n_weeks * 0.80) & (nonbp_eps < n_weeks * 0.90)).sum()}")
print(f"  60-79%: {((nonbp_eps >= n_weeks * 0.60) & (nonbp_eps < n_weeks * 0.80)).sum()}")
print(f"  50-59%: {((nonbp_eps >= n_weeks * 0.50) & (nonbp_eps < n_weeks * 0.60)).sum()}")

# ── Analyze gap structure ─────────────────────────────────────────
# For each non-BP page, find the gaps (runs of missing weeks)
gap_lengths = []
gap_positions = []  # 'interior' vs 'edge' (start/end of time series)
gap_ranks_before = []
gap_ranks_after = []

ep_subset = nonbp_eps.index.tolist()
ep_weeks = df[df['endpoint_id'].isin(ep_subset)].groupby('endpoint_id')['week_idx'].apply(set).to_dict()
ep_rank_lookup = dict(zip(zip(df['endpoint_id'], df['week_idx']), df['rank']))

for ep in ep_subset:
    present = sorted(ep_weeks[ep])
    present_set = set(present)
    first, last = min(present), max(present)

    # Find interior gaps (between first and last observed week)
    for w in range(first, last + 1):
        if w not in present_set:
            # Start of a gap — find its length
            gap_len = 0
            w2 = w
            while w2 <= last and w2 not in present_set:
                gap_len += 1
                w2 += 1
            # Only count the gap starting at w (avoid double-counting)
            if w == first or (w - 1) in present_set:
                gap_lengths.append(gap_len)
                gap_positions.append('interior')
                # Rank before gap
                if (w - 1) in present_set:
                    gap_ranks_before.append(ep_rank_lookup.get((ep, w - 1), np.nan))
                else:
                    gap_ranks_before.append(np.nan)
                # Rank after gap
                if w2 in present_set:
                    gap_ranks_after.append(ep_rank_lookup.get((ep, w2), np.nan))
                else:
                    gap_ranks_after.append(np.nan)

    # Leading/trailing gaps
    if first > 0:
        gap_lengths.append(first)
        gap_positions.append('leading')
        gap_ranks_before.append(np.nan)
        gap_ranks_after.append(ep_rank_lookup.get((ep, first), np.nan))
    if last < n_weeks - 1:
        gap_lengths.append(n_weeks - 1 - last)
        gap_positions.append('trailing')
        gap_ranks_before.append(ep_rank_lookup.get((ep, last), np.nan))
        gap_ranks_after.append(np.nan)

gap_df = pd.DataFrame({
    'length': gap_lengths,
    'position': gap_positions,
    'rank_before': gap_ranks_before,
    'rank_after': gap_ranks_after,
})

print(f"\n{'='*60}")
print(f"GAP STRUCTURE FOR PAGES PRESENT >=50%")
print(f"{'='*60}")
print(f"Total gaps: {len(gap_df)}")
print(f"\nBy position:")
for pos in ['interior', 'leading', 'trailing']:
    sub = gap_df[gap_df['position'] == pos]
    print(f"  {pos:>10s}: {len(sub):>5d} gaps")

print(f"\nInterior gap length distribution:")
interior = gap_df[gap_df['position'] == 'interior']
for gl in [1, 2, 3, 4, 5, '6-10', '11-20', '21+']:
    if isinstance(gl, int):
        n = (interior['length'] == gl).sum()
        print(f"  {gl:>3d} week{'s' if gl > 1 else ' '}: {n:>5d} ({n/len(interior)*100:.1f}%)")
    elif gl == '6-10':
        n = ((interior['length'] >= 6) & (interior['length'] <= 10)).sum()
        print(f"  6-10 wks: {n:>5d} ({n/len(interior)*100:.1f}%)")
    elif gl == '11-20':
        n = ((interior['length'] >= 11) & (interior['length'] <= 20)).sum()
        print(f"  11-20 wk: {n:>5d} ({n/len(interior)*100:.1f}%)")
    elif gl == '21+':
        n = (interior['length'] >= 21).sum()
        print(f"  21+ wks : {n:>5d} ({n/len(interior)*100:.1f}%)")

print(f"\n  Mean interior gap: {interior['length'].mean():.1f} weeks")
print(f"  Median interior gap: {interior['length'].median():.1f} weeks")

# Rank at gap boundaries
print(f"\nRank of pages at gap boundaries (interior gaps only):")
print(f"  Rank before gap: median={interior['rank_before'].median():.0f}, "
      f"mean={interior['rank_before'].mean():.0f}")
print(f"  Rank after gap:  median={interior['rank_after'].median():.0f}, "
      f"mean={interior['rank_after'].mean():.0f}")

# By gap length, what's the typical rank?
print(f"\nMedian rank before gap, by gap length:")
for gl in [1, 2, 3, '4-10', '11+']:
    if isinstance(gl, int):
        sub = interior[interior['length'] == gl]
        if len(sub) > 0:
            print(f"  {gl:>3d} week : median rank before = {sub['rank_before'].median():.0f} (n={len(sub)})")
    elif gl == '4-10':
        sub = interior[(interior['length'] >= 4) & (interior['length'] <= 10)]
        if len(sub) > 0:
            print(f"  4-10 wk : median rank before = {sub['rank_before'].median():.0f} (n={len(sub)})")
    elif gl == '11+':
        sub = interior[interior['length'] >= 11]
        if len(sub) > 0:
            print(f"  11+ wks : median rank before = {sub['rank_before'].median():.0f} (n={len(sub)})")

# Leading/trailing gaps
print(f"\nLeading gaps (page not yet observed at start):")
leading = gap_df[gap_df['position'] == 'leading']
print(f"  N = {len(leading)}, mean length = {leading['length'].mean():.1f} weeks")
print(f"  Median rank at first observation = {leading['rank_after'].median():.0f}")

trailing = gap_df[gap_df['position'] == 'trailing']
print(f"\nTrailing gaps (page disappears before end):")
print(f"  N = {len(trailing)}, mean length = {trailing['length'].mean():.1f} weeks")
print(f"  Median rank at last observation = {trailing['rank_before'].median():.0f}")
