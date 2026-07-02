#!/usr/bin/env python3
"""
Quick diagnostic: Does the RACF gap come from composition noise?

The empirical RACF uses `rank` from the raw data = rank among ALL observed
pages (~14,363, varying each week). Non-BP pages churn in/out, creating
composition noise in the rank time series.

The simulation's RACF uses rank among all detected pages (~14,222, nearly
fixed). So the simulation has less composition noise.

Test: recompute empirical RACF using ranks among BP pages ONLY (fixed set
of 10,257). If BP-only RACF is higher (closer to sim), composition noise
is the explanation.
"""

import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Load data
df = pd.read_parquet('/Users/hindman/Documents/github/rank-diffusion/data/raw/fb_ranked_weekly_cutdown.parquet')
df['date'] = pd.to_datetime(df['date'])
dates = sorted(df['date'].unique())
n_weeks = len(dates)

ep_counts = df.groupby('endpoint_id')['date'].nunique()
all_weeks_eps = sorted(ep_counts[ep_counts == n_weeks].index)
N_balanced = len(all_weeks_eps)

print(f"N_balanced = {N_balanced}, n_weeks = {n_weeks}")

# --- Method 1: RACF from raw data ranks (current approach) ---
rank_pivot = df[df['endpoint_id'].isin(all_weeks_eps)].pivot_table(
    index='date', columns='endpoint_id', values='rank').sort_index()

sample_eps = list(all_weeks_eps)[:2000]
racf_raw = {}
for lag in [1, 4, 13]:
    cors = [rank_pivot[ep].dropna().autocorr(lag) for ep in sample_eps
            if len(rank_pivot[ep].dropna()) > lag + 5]
    racf_raw[lag] = np.nanmedian(cors)

# --- Method 2: RACF from BP-only ranks (re-ranked among BP pages only) ---
metric_pivot = df[df['endpoint_id'].isin(all_weeks_eps)].pivot_table(
    index='date', columns='endpoint_id', values='metric_value').sort_index()

# Rank BP pages among themselves each week (descending by metric_value)
bp_rank_pivot = metric_pivot.rank(axis=1, ascending=False, method='min').astype(int)

racf_bp = {}
for lag in [1, 4, 13]:
    cors = [bp_rank_pivot[ep].dropna().autocorr(lag) for ep in sample_eps
            if len(bp_rank_pivot[ep].dropna()) > lag + 5]
    racf_bp[lag] = np.nanmedian(cors)

# --- Comparison ---
print(f"\n{'Lag':<6s}  {'Raw RACF':>10s}  {'BP-only RACF':>12s}  {'Sim RACF':>10s}  {'Raw err':>8s}  {'BP err':>8s}")
print("-" * 65)
sim_racf = {1: 0.5482, 4: 0.3378, 13: 0.1643}  # from v4.1 run
for lag in [1, 4, 13]:
    raw_err = abs(sim_racf[lag] - racf_raw[lag])
    bp_err = abs(sim_racf[lag] - racf_bp[lag])
    print(f"  {lag:<4d}  {racf_raw[lag]:>10.4f}  {racf_bp[lag]:>12.4f}  {sim_racf[lag]:>10.4f}  "
          f"{raw_err:>8.4f}  {bp_err:>8.4f}")

print(f"\nThreshold for PASS: < 0.08")
print(f"\nInterpretation:")
print(f"  If BP-only RACF is higher than Raw RACF → composition noise reduces RACF")
print(f"  If BP-only errors are < 0.08 → the model is correct; the diagnostic was unfair")

# Also check: how much does the observed set change week to week?
weekly_eps = {d: set(df[df['date'] == d]['endpoint_id']) for d in dates}
weekly_counts = [len(weekly_eps[d]) for d in dates]
exits = [len(weekly_eps[dates[i-1]] - weekly_eps[dates[i]]) for i in range(1, len(dates))]
entries = [len(weekly_eps[dates[i]] - weekly_eps[dates[i-1]]) for i in range(1, len(dates))]
print(f"\nWeekly composition changes:")
print(f"  Pages/week: mean={np.mean(weekly_counts):.0f}, std={np.std(weekly_counts):.0f}")
print(f"  Exits/week: mean={np.mean(exits):.0f}, std={np.std(exits):.0f}")
print(f"  Entries/week: mean={np.mean(entries):.0f}, std={np.std(entries):.0f}")
print(f"  Non-BP pages in any given week: ~{np.mean(weekly_counts) - N_balanced:.0f}")
