#!/usr/bin/env python3
"""
Descriptive analysis: exit probability by rank.

For each page-week, compute whether the page is absent from the data
in the next 1, 4, 8, 16 weeks. Plot exit probability as a function
of the page's rank at its last observed week.
"""

import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ── Load full (unbalanced) data ──────────────────────────────────
df = pd.read_parquet('/Users/hindman/Documents/github/rank-diffusion/data/raw/fb_ranked_weekly_cutdown.parquet')
df['date'] = pd.to_datetime(df['date'])
dates = sorted(df['date'].unique())
n_weeks = len(dates)
date_to_idx = {d: i for i, d in enumerate(dates)}
df['week_idx'] = df['date'].map(date_to_idx)

print(f"  {n_weeks} weeks, {df['endpoint_id'].nunique()} unique endpoints")
print(f"  {len(df)} total page-week observations")

# ── Build presence lookup: set of endpoints present each week ────
weekly_eps = {}
for d in dates:
    weekly_eps[date_to_idx[d]] = set(df[df['date'] == d]['endpoint_id'])

# ── Build rank lookup ────────────────────────────────────────────
print("  Building rank lookup...")
rank_df = df[['endpoint_id', 'week_idx', 'rank']].copy()
rank_dict = dict(zip(zip(rank_df['endpoint_id'], rank_df['week_idx']),
                      rank_df['rank']))

# ── For each page-week, check absence at horizons 1, 4, 8, 16 ───
horizons = [1, 4, 8, 16]
print("  Computing exit probabilities...")

records = []
for w in range(n_weeks):
    eps_this_week = weekly_eps[w]
    for ep in eps_this_week:
        rank = rank_dict.get((ep, w), np.nan)
        if np.isnan(rank):
            continue
        row = {'rank': int(rank), 'week': w}
        for h in horizons:
            if w + h >= n_weeks:
                row[f'exit_{h}'] = np.nan
            else:
                absent = all(ep not in weekly_eps[w + j] for j in range(1, h + 1))
                row[f'exit_{h}'] = 1 if absent else 0
        records.append(row)

print(f"  {len(records)} page-week records")
result = pd.DataFrame(records)

# ── Bin by rank ──────────────────────────────────────────────────
bin_edges = np.unique(np.concatenate([
    np.arange(1, 101, 10),
    np.arange(100, 1001, 50),
    np.arange(1000, 5001, 250),
    np.arange(5000, 15001, 500),
]))

result['rank_bin'] = pd.cut(result['rank'], bins=bin_edges, labels=False)
bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

# ── Single overlaid plot ─────────────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 7))

colors = {1: '#2196F3', 4: '#FF9800', 8: '#F44336', 16: '#9C27B0'}
for h in horizons:
    col = f'exit_{h}'
    grouped = result.dropna(subset=[col]).groupby('rank_bin')[col]
    rates = grouped.mean()
    valid = rates.index[rates.notna() & (rates.index < len(bin_centers))]
    x = bin_centers[valid.values.astype(int)]
    y = rates[valid].values * 100
    ax.plot(x, y, '-', color=colors[h], linewidth=2,
            label=f'Absent next {h} week{"s" if h > 1 else ""}', alpha=0.85)

ax.set_xlabel('Rank', fontsize=13)
ax.set_ylabel('Probability (%)', fontsize=13)
ax.set_title('Exit Probability by Rank\n(probability page is not seen for next h consecutive weeks)',
             fontsize=14, fontweight='bold')
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3)
ax.set_xlim(0, 15000)

plt.tight_layout()
plt.savefig('exit_by_rank.png', dpi=150, bbox_inches='tight')
print(f"  Saved: exit_by_rank.png")

# ── Summary table ────────────────────────────────────────────────
print("\nExit rates by rank band:")
print(f"{'Band':>15s}  {'1wk':>6s}  {'4wk':>6s}  {'8wk':>6s}  {'16wk':>6s}  {'N':>8s}")
for lo, hi in [(1, 100), (101, 500), (501, 2000), (2001, 5000),
               (5001, 10000), (10001, 15000)]:
    mask = (result['rank'] >= lo) & (result['rank'] <= hi)
    sub = result[mask]
    n = len(sub)
    parts = [f'{lo:>5d}-{hi:<5d}']
    for h in horizons:
        col = f'exit_{h}'
        rate = sub[col].dropna().mean() * 100
        parts.append(f'{rate:5.2f}%')
    parts.append(f'{n:>8d}')
    print(f"{'  '.join(parts)}")
