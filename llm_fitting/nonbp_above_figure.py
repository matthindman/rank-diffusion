#!/usr/bin/env python3
"""
Figure: For each rank position held by a balanced-panel (BP) endpoint,
how many NON-BP endpoints are ranked above it, segmented by presence rate.

Segments: >=80%, 60-79%, 40-59%, 20-39%, <20% of weeks present.
"""

import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ── Load data ─────────────────────────────────────────────────────
df = pd.read_parquet('/Users/hindman/Documents/github/rank-diffusion/data/raw/fb_ranked_weekly_cutdown.parquet')
df['date'] = pd.to_datetime(df['date'])
dates = sorted(df['date'].unique())
n_weeks = len(dates)

# Identify balanced-panel endpoints (present every week)
ep_counts = df.groupby('endpoint_id')['date'].nunique()
bp_eps = set(ep_counts[ep_counts == n_weeks].index)
N_bp = len(bp_eps)
print(f"N_balanced = {N_bp}, n_weeks = {n_weeks}")

# Compute presence fraction for every non-BP endpoint
ep_presence_frac = (ep_counts / n_weeks).to_dict()

# Presence segments for non-BP pages
segments = [
    ('>=95%',  0.95, 1.00, '#0D47A1'),  # miss 1-4 weeks out of 88
    ('90-94%', 0.90, 0.95, '#1976D2'),
    ('80-89%', 0.80, 0.90, '#42A5F5'),
    ('60-79%', 0.60, 0.80, '#388E3C'),
    ('40-59%', 0.40, 0.60, '#F57C00'),
    ('20-39%', 0.20, 0.40, '#D32F2F'),
    ('<20%',   0.00, 0.20, '#7B1FA2'),  # rarely present
]

# Classify each non-BP endpoint into a segment
nonbp_eps = set(ep_counts.index) - bp_eps
ep_segment = {}
for ep in nonbp_eps:
    frac = ep_presence_frac[ep]
    for label, lo, hi, _ in segments:
        if lo <= frac < hi or (hi == 1.00 and frac < 1.0 and frac >= lo):
            ep_segment[ep] = label
            break

# Count endpoints per segment
seg_counts = {}
for label, _, _, _ in segments:
    seg_counts[label] = sum(1 for v in ep_segment.values() if v == label)
    print(f"  Non-BP segment {label:>6s}: {seg_counts[label]:>5d} endpoints")

df['is_bp'] = df['endpoint_id'].isin(bp_eps)

# Tag each non-BP row with its segment
def get_seg(ep):
    return ep_segment.get(ep, None)

# ── For each week, compute cumulative counts by segment ───────────
# For efficiency, pre-map endpoint -> segment index
seg_labels = [s[0] for s in segments]
seg_idx_map = {}  # endpoint_id -> segment index (0-4)
for ep, label in ep_segment.items():
    seg_idx_map[ep] = seg_labels.index(label)

n_seg = len(segments)

# Collect per-week data
# For each BP page-week, record: (rank, nonbp_above_by_segment[0..4])
all_bp_ranks = []
all_seg_above = [[] for _ in range(n_seg)]

for i, d in enumerate(dates):
    week = df[df['date'] == d].sort_values('rank')
    ranks = week['rank'].values
    ep_ids = week['endpoint_id'].values
    is_bp = week['is_bp'].values
    n_obs = len(week)

    # For each row, determine which segment it belongs to (or -1 if BP)
    row_seg = np.full(n_obs, -1, dtype=int)
    for j in range(n_obs):
        if not is_bp[j]:
            s = seg_idx_map.get(ep_ids[j], -1)
            row_seg[j] = s

    # Cumulative count per segment as we go down the ranking
    seg_cumsums = np.zeros((n_obs, n_seg), dtype=int)
    running = np.zeros(n_seg, dtype=int)
    for j in range(n_obs):
        if row_seg[j] >= 0:
            running[row_seg[j]] += 1
        seg_cumsums[j] = running.copy()

    # Extract BP rows
    bp_mask = is_bp
    bp_ranks_week = ranks[bp_mask]
    all_bp_ranks.append(bp_ranks_week)
    for s in range(n_seg):
        all_seg_above[s].append(seg_cumsums[bp_mask, s])

    if i % 20 == 0:
        seg_this_week = [np.sum(row_seg == s) for s in range(n_seg)]
        print(f"  Week {i+1}/{n_weeks}: {n_obs} pages, non-BP by seg: {seg_this_week}")

all_bp_ranks = np.concatenate(all_bp_ranks)
for s in range(n_seg):
    all_seg_above[s] = np.concatenate(all_seg_above[s])

print(f"\n  Total BP page-week obs: {len(all_bp_ranks)}")

# ── Bin by rank ───────────────────────────────────────────────────
bin_edges = np.arange(0, int(all_bp_ranks.max()) + 200, 100)
bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
bin_idx = np.digitize(all_bp_ranks, bin_edges) - 1
n_bins = len(bin_centers)

# Compute mean count above per segment per bin
mean_seg_above = np.full((n_seg, n_bins), np.nan)
mean_total_above = np.full(n_bins, np.nan)
count_per_bin = np.zeros(n_bins)

for b in range(n_bins):
    mask = bin_idx == b
    cnt = mask.sum()
    count_per_bin[b] = cnt
    if cnt > 50:
        total = np.zeros(cnt)
        for s in range(n_seg):
            mean_seg_above[s, b] = np.mean(all_seg_above[s][mask])
            total += all_seg_above[s][mask]
        mean_total_above[b] = np.mean(total)

valid = count_per_bin > 50

# ── Figure 1: Stacked area — count of non-BP above by segment ────
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 11), height_ratios=[3, 2])

# Stacked area plot
x = bin_centers[valid]
bottoms = np.zeros(valid.sum())
for s in range(n_seg):
    y = mean_seg_above[s, valid]
    y = np.nan_to_num(y, 0)
    label, lo, hi, color = segments[s]
    pres_str = f"{label} present" if label != '<20%' else f"{label} present"
    ax1.fill_between(x, bottoms, bottoms + y, alpha=0.7, color=color,
                     label=f'{pres_str} (N={seg_counts[label]:,})')
    ax1.plot(x, bottoms + y, '-', color=color, linewidth=0.5, alpha=0.5)
    bottoms = bottoms + y

ax1.plot(x, bottoms, '-', color='black', linewidth=1.5, alpha=0.8, label='Total non-BP')
ax1.set_xlabel('Rank of BP endpoint (in full observed ranking)', fontsize=12)
ax1.set_ylabel('Mean count of non-BP endpoints ranked above', fontsize=12)
ax1.set_title('Non-BP Endpoints Ranked Above Each BP Position, by Presence Rate\n'
              '(how often the non-fully-observed pages appear in our 88-week panel)',
              fontsize=13, fontweight='bold')
ax1.legend(fontsize=10, loc='upper left')
ax1.grid(True, alpha=0.3)

# ── Bottom panel: percentage breakdown ────────────────────────────
# Fraction of rank positions above held by each segment
bottoms2 = np.zeros(valid.sum())
for s in range(n_seg):
    y = mean_seg_above[s, valid]
    y = np.nan_to_num(y, 0)
    pct = np.where(x > 0, y / x * 100, 0)
    label, lo, hi, color = segments[s]
    ax2.fill_between(x, bottoms2, bottoms2 + pct, alpha=0.7, color=color,
                     label=f'{label} present')
    bottoms2 = bottoms2 + pct

ax2.set_xlabel('Rank of BP endpoint (in full observed ranking)', fontsize=12)
ax2.set_ylabel('Non-BP fraction above (%)', fontsize=12)
ax2.set_title('Fraction of higher-ranked positions held by non-BP endpoints, by presence rate',
              fontsize=11)
ax2.legend(fontsize=9, loc='upper left')
ax2.grid(True, alpha=0.3)
ax2.set_ylim(0, min(bottoms2.max() * 1.15, 50))

plt.tight_layout()
plt.savefig('/Users/hindman/Documents/github/rank-diffusion/llm_fitting/nonbp_contamination.png',
            dpi=150, bbox_inches='tight')
print(f"\n  Saved: nonbp_contamination.png")

# ── Summary table ─────────────────────────────────────────────────
print(f"\nSummary by rank and segment (mean non-BP above):")
header = f"{'Rank':>8s}  {'Total':>6s}"
for label, _, _, _ in segments:
    header += f"  {label:>8s}"
header += f"  {'Total%':>7s}"
print(header)
print("-" * len(header))

for r_check in [100, 500, 1000, 2000, 5000, 8000, 10000, 12000]:
    idx = np.argmin(np.abs(bin_centers - r_check))
    if not np.isnan(mean_total_above[idx]):
        row = f"{r_check:>8d}  {mean_total_above[idx]:>6.1f}"
        for s in range(n_seg):
            val = mean_seg_above[s, idx]
            row += f"  {val:>8.1f}" if not np.isnan(val) else f"  {'N/A':>8s}"
        pct = mean_total_above[idx] / r_check * 100 if r_check > 0 else 0
        row += f"  {pct:>6.1f}%"
        print(row)
