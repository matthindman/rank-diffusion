#!/usr/bin/env python3
"""
Build expanded panel with Tobit-style imputation for below-threshold pages.

For pages present >=50% of weeks, impute missing weeks by drawing from
the truncated left tail of the change distribution (below the weekly
detection threshold). This follows the environmental science / proteomics
literature on left-censored data imputation.

Steps:
  1. Estimate weekly detection threshold
  2. Verify change distribution symmetry by rank band
  3. Estimate change distribution parameters (mean, variance) by rank band
  4. Impute missing page-weeks via truncated normal draws below threshold
  5. Re-rank all pages and save expanded panel
"""

import numpy as np
import pandas as pd
from scipy import stats as sp_stats
import warnings
warnings.filterwarnings('ignore')
import time

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

t_start = time.time()

# ══════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════
print("=" * 70)
print("STEP 1: LOAD DATA AND IDENTIFY PANELS")
print("=" * 70)

df = pd.read_parquet('/Users/hindman/Documents/github/rank-diffusion/data/raw/fb_ranked_weekly_cutdown.parquet')
df['date'] = pd.to_datetime(df['date'])
dates = sorted(df['date'].unique())
n_weeks = len(dates)
date_to_idx = {d: i for i, d in enumerate(dates)}
df['week_idx'] = df['date'].map(date_to_idx)

ep_counts = df.groupby('endpoint_id')['date'].nunique()
bp_eps = set(ep_counts[ep_counts == n_weeks].index)
N_bp = len(bp_eps)

# Pages present >= 50% but not every week
min_presence = 0.50
nonbp_candidates = ep_counts[(ep_counts < n_weeks) & (ep_counts >= n_weeks * min_presence)]
expanded_eps = bp_eps | set(nonbp_candidates.index)
N_expanded = len(expanded_eps)
N_impute = N_expanded - N_bp

print(f"  n_weeks = {n_weeks}")
print(f"  Balanced panel (100%): {N_bp} endpoints")
print(f"  Additional (>={min_presence*100:.0f}%):  {N_impute} endpoints")
print(f"  Expanded panel total:  {N_expanded} endpoints")
print(f"  Presence distribution of additional pages:")
for lo, hi, label in [(0.95, 1.0, '>=95%'), (0.90, 0.95, '90-94%'),
                       (0.80, 0.90, '80-89%'), (0.60, 0.80, '60-79%'),
                       (0.50, 0.60, '50-59%')]:
    n = ((nonbp_candidates >= n_weeks * lo) & (nonbp_candidates < n_weeks * hi)).sum()
    print(f"    {label:>8s}: {n:>5d}")


# ══════════════════════════════════════════════════════════════════
# STEP 2: ESTIMATE WEEKLY DETECTION THRESHOLD
# ══════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("STEP 2: ESTIMATE WEEKLY DETECTION THRESHOLD")
print("=" * 70)

df['log_metric'] = np.log(df['metric_value'].clip(lower=1.0))

weekly_threshold = {}
weekly_n_obs = {}
for w in range(n_weeks):
    week_data = df[df['week_idx'] == w]
    # Use 1st percentile as threshold (robust to zero-metric outliers)
    weekly_threshold[w] = week_data['log_metric'].quantile(0.01)
    weekly_n_obs[w] = len(week_data)

thresholds = np.array([weekly_threshold[w] for w in range(n_weeks)])
print(f"  Weekly log-threshold: mean={np.mean(thresholds):.3f}, "
      f"std={np.std(thresholds):.3f}")
print(f"  Weekly log-threshold: min={np.min(thresholds):.3f}, "
      f"max={np.max(thresholds):.3f}")
print(f"  Weekly N_obs: mean={np.mean(list(weekly_n_obs.values())):.0f}, "
      f"range=[{min(weekly_n_obs.values())}, {max(weekly_n_obs.values())}]")


# ══════════════════════════════════════════════════════════════════
# STEP 3: CHANGE DISTRIBUTION BY RANK BAND
# ══════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("STEP 3: CHANGE DISTRIBUTION BY RANK BAND")
print("=" * 70)

# Build log-metric pivot for BP pages (complete observations)
bp_df = df[df['endpoint_id'].isin(bp_eps)]
log_pivot = bp_df.pivot_table(index='week_idx', columns='endpoint_id',
                               values='log_metric').sort_index()
rank_pivot = bp_df.pivot_table(index='week_idx', columns='endpoint_id',
                                values='rank').sort_index()

# Compute changes
log_changes = log_pivot.diff().iloc[1:]

# Mean rank per endpoint (for band assignment)
mean_ranks = rank_pivot.mean()

# Rank bands for characterization
bands = [(1, 500), (501, 2000), (2001, 5000), (5001, 8000),
         (8001, 10000), (10001, 12000), (12001, 15000)]

print(f"\n  {'Band':>15s}  {'N_eps':>6s}  {'Mean Δy':>8s}  {'Std Δy':>8s}  "
      f"{'Skew':>6s}  {'Kurt':>6s}  {'Med |Δy|':>9s}")
print(f"  {'-'*75}")

band_params = {}  # (lo, hi) -> (mean_change, std_change)
for lo, hi in bands:
    eps_in_band = mean_ranks[(mean_ranks >= lo) & (mean_ranks < hi)].index
    if len(eps_in_band) == 0:
        continue
    changes = log_changes[eps_in_band].values.flatten()
    changes = changes[np.isfinite(changes)]
    mu = np.mean(changes)
    sigma = np.std(changes)
    skew = sp_stats.skew(changes)
    kurt = sp_stats.kurtosis(changes, fisher=True)
    med_abs = np.median(np.abs(changes))
    band_params[(lo, hi)] = (mu, sigma)
    print(f"  {lo:>6d}-{hi:<6d}  {len(eps_in_band):>6d}  {mu:>8.4f}  {sigma:>8.4f}  "
          f"{skew:>6.2f}  {kurt:>6.1f}  {med_abs:>9.4f}")

# Fit a smooth relationship between rank and change variance
band_centers = np.array([(lo + hi) / 2 for lo, hi in bands if (lo, hi) in band_params])
band_stds = np.array([band_params[(lo, hi)][1] for lo, hi in bands if (lo, hi) in band_params])
band_means = np.array([band_params[(lo, hi)][0] for lo, hi in bands if (lo, hi) in band_params])

# Log-linear fit: log(sigma) = a + b * log(rank)
log_bc = np.log(band_centers)
log_bs = np.log(band_stds)
poly_fit = np.polyfit(log_bc, log_bs, 1)
print(f"\n  Variance scaling: log(σ) = {poly_fit[1]:.3f} + {poly_fit[0]:.3f} × log(rank)")
print(f"    σ doubles every {2**(1/poly_fit[0]):.0f}× increase in rank")


def get_change_std(rank):
    """Interpolate/extrapolate change std for a given rank."""
    lr = np.log(np.clip(rank, 10, 20000))
    return np.exp(np.polyval(poly_fit, lr))


# Verify extrapolation
print(f"\n  Extrapolated σ_change:")
for r in [100, 1000, 5000, 10000, 13000, 15000]:
    print(f"    Rank {r:>6d}: σ = {get_change_std(r):.4f}")


# ══════════════════════════════════════════════════════════════════
# STEP 4: VERIFY SYMMETRY
# ══════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("STEP 4: VERIFY CHANGE DISTRIBUTION SYMMETRY")
print("=" * 70)

fig_sym, axes_sym = plt.subplots(2, 3, figsize=(16, 10))
axes_flat = axes_sym.flatten()

for idx, (lo, hi) in enumerate([(1, 500), (2001, 5000), (5001, 8000),
                                  (8001, 10000), (10001, 12000), (12001, 15000)]):
    if idx >= 6:
        break
    eps_in_band = mean_ranks[(mean_ranks >= lo) & (mean_ranks < hi)].index
    if len(eps_in_band) == 0:
        continue
    changes = log_changes[eps_in_band].values.flatten()
    changes = changes[np.isfinite(changes)]

    ax = axes_flat[idx]
    # Histogram with mirrored negative/positive
    bins = np.linspace(-3, 3, 80)
    changes_std = changes / np.std(changes)
    ax.hist(changes_std, bins=bins, density=True, alpha=0.6, color='#1976D2',
            label='Empirical')
    # Overlay normal
    x = np.linspace(-3, 3, 200)
    ax.plot(x, sp_stats.norm.pdf(x), 'r-', linewidth=1.5, label='N(0,1)')
    ax.set_title(f'Ranks {lo}-{hi} (skew={sp_stats.skew(changes):.2f})', fontsize=10)
    ax.legend(fontsize=8)
    ax.set_xlim(-3, 3)

plt.suptitle('Standardized Change Distribution by Rank Band\n(verifying approximate symmetry)',
             fontsize=13, fontweight='bold')
plt.tight_layout()
plt.savefig('/Users/hindman/Documents/github/rank-diffusion/llm_fitting/change_symmetry.png',
            dpi=150, bbox_inches='tight')
print("  Saved: change_symmetry.png")


# ══════════════════════════════════════════════════════════════════
# STEP 5: IMPUTE MISSING PAGE-WEEKS
# ══════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("STEP 5: IMPUTE MISSING PAGE-WEEKS")
print("=" * 70)

rng = np.random.RandomState(42)

# Build lookup: (endpoint_id, week_idx) -> log_metric
obs_lookup = dict(zip(zip(df['endpoint_id'], df['week_idx']), df['log_metric']))
rank_lookup = dict(zip(zip(df['endpoint_id'], df['week_idx']), df['rank']))

# For each non-BP page in the expanded set, identify observed and missing weeks
eps_to_impute = sorted(expanded_eps - bp_eps)
print(f"  Pages to impute: {len(eps_to_impute)}")

# Compute median observed rank for each page (for variance band selection)
ep_median_rank = {}
for ep in eps_to_impute:
    ep_data = df[df['endpoint_id'] == ep]
    ep_median_rank[ep] = ep_data['rank'].median()

# Imputation storage: (endpoint_id, week_idx) -> imputed_log_metric
imputed = {}
n_imputed_total = 0
n_interior = 0
n_leading = 0
n_trailing = 0
gap_length_counts = {}

for ep_i, ep in enumerate(eps_to_impute):
    # Get observed weeks for this endpoint
    ep_data = df[df['endpoint_id'] == ep].sort_values('week_idx')
    observed_weeks = set(ep_data['week_idx'].values)
    all_weeks = set(range(n_weeks))
    missing_weeks = sorted(all_weeks - observed_weeks)

    if not missing_weeks:
        continue

    # Get log-metric for observed weeks
    ep_obs = dict(zip(ep_data['week_idx'], ep_data['log_metric']))

    # Determine median rank for variance estimation
    med_rank = ep_median_rank[ep]
    sigma_change = get_change_std(med_rank)

    # Mean change is approximately 0 (random walk)
    mu_change = 0.0

    # Classify and impute gaps
    first_obs = min(observed_weeks)
    last_obs = max(observed_weeks)

    # Build a complete value series: observed + imputed
    values = {}
    for w in range(n_weeks):
        if w in ep_obs:
            values[w] = ep_obs[w]

    # Forward-fill interior and trailing gaps
    for w in range(n_weeks):
        if w in values:
            continue  # already observed

        # Find the most recent observed/imputed value
        prev_w = w - 1
        while prev_w >= 0 and prev_w not in values:
            prev_w -= 1

        if prev_w < 0:
            continue  # leading gap — handle separately below

        # Predict from previous value
        steps = w - prev_w
        predicted = values[prev_w] + mu_change * steps
        sigma_total = sigma_change * np.sqrt(steps)  # RW variance scaling

        # Threshold for this week
        thresh = weekly_threshold[w]

        # Draw from N(predicted, sigma_total²) truncated above at threshold
        # Using scipy's truncated normal for numerical stability
        a_tn = -np.inf  # no lower bound
        b_tn = (thresh - predicted) / sigma_total  # upper bound in std units
        if b_tn < -6:
            # Predicted value far above threshold — page very unlikely below
            # Place just below threshold with small noise
            imputed_val = thresh - abs(rng.exponential(sigma_total * 0.05))
        else:
            imputed_val = sp_stats.truncnorm.rvs(
                a_tn, b_tn, loc=predicted, scale=sigma_total,
                random_state=rng)

        values[w] = imputed_val
        imputed[(ep, w)] = imputed_val
        n_imputed_total += 1

        if w > first_obs and w < last_obs:
            n_interior += 1
        elif w > last_obs:
            n_trailing += 1

        gl = steps
        gap_length_counts[gl] = gap_length_counts.get(gl, 0) + 1

    # Backward-fill leading gaps
    if first_obs > 0:
        for w in range(first_obs - 1, -1, -1):
            next_w = w + 1
            predicted = values[next_w] + mu_change  # step backwards
            sigma_total = sigma_change  # single step

            thresh = weekly_threshold[w]
            a_tn = -np.inf
            b_tn = (thresh - predicted) / sigma_total
            if b_tn < -6:
                imputed_val = thresh - abs(rng.exponential(sigma_total * 0.05))
            else:
                imputed_val = sp_stats.truncnorm.rvs(
                    a_tn, b_tn, loc=predicted, scale=sigma_total,
                    random_state=rng)

            values[w] = imputed_val
            imputed[(ep, w)] = imputed_val
            n_imputed_total += 1
            n_leading += 1

    if ep_i % 500 == 0:
        print(f"    Page {ep_i+1}/{len(eps_to_impute)}: "
              f"{len(missing_weeks)} missing, med_rank={med_rank:.0f}, σ={sigma_change:.3f}")

print(f"\n  Imputation summary:")
print(f"    Total imputed page-weeks: {n_imputed_total}")
print(f"    Interior gaps: {n_interior}")
print(f"    Leading gaps:  {n_leading}")
print(f"    Trailing gaps: {n_trailing}")
print(f"    Gap step distribution (from forward-fill):")
for gl in sorted(gap_length_counts.keys())[:10]:
    print(f"      Step {gl}: {gap_length_counts[gl]}")


# ══════════════════════════════════════════════════════════════════
# STEP 6: BUILD EXPANDED PANEL AND RE-RANK
# ══════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("STEP 6: BUILD EXPANDED PANEL AND RE-RANK")
print("=" * 70)

# Build the full panel: for each week, all expanded_eps have a value
expanded_eps_sorted = sorted(expanded_eps)
ep_to_idx = {ep: i for i, ep in enumerate(expanded_eps_sorted)}
N = len(expanded_eps_sorted)

# Arrays: (n_weeks, N)
panel_log_metric = np.full((n_weeks, N), np.nan)
panel_is_observed = np.zeros((n_weeks, N), dtype=bool)
panel_rank = np.zeros((n_weeks, N), dtype=int)

# Fill observed values
for (ep, w), val in obs_lookup.items():
    if ep in ep_to_idx:
        panel_log_metric[w, ep_to_idx[ep]] = val
        panel_is_observed[w, ep_to_idx[ep]] = True

# Fill imputed values
for (ep, w), val in imputed.items():
    idx = ep_to_idx[ep]
    panel_log_metric[w, idx] = val
    # panel_is_observed stays False for imputed

# Check completeness
n_missing = np.sum(np.isnan(panel_log_metric))
n_total = n_weeks * N
print(f"  Panel shape: {n_weeks} weeks × {N} endpoints = {n_total} cells")
print(f"  Observed: {np.sum(panel_is_observed)} ({np.sum(panel_is_observed)/n_total*100:.1f}%)")
print(f"  Imputed:  {n_imputed_total} ({n_imputed_total/n_total*100:.1f}%)")
print(f"  Missing:  {n_missing} ({n_missing/n_total*100:.2f}%)")

# Re-rank each week (descending by exp(log_metric))
for w in range(n_weeks):
    vals = panel_log_metric[w]
    valid = ~np.isnan(vals)
    order = np.argsort(-vals[valid])
    ranks = np.full(N, N + 1, dtype=int)
    valid_idx = np.where(valid)[0]
    ranks[valid_idx[order]] = np.arange(1, valid.sum() + 1)
    panel_rank[w] = ranks

# Summary stats
bp_idx = np.array([ep_to_idx[ep] for ep in sorted(bp_eps)])
nonbp_idx = np.array([ep_to_idx[ep] for ep in sorted(expanded_eps - bp_eps)])

print(f"\n  Rank statistics for imputed pages:")
imputed_ranks = panel_rank[:, nonbp_idx]
observed_mask = panel_is_observed[:, nonbp_idx]
print(f"    When observed: median rank = {np.median(imputed_ranks[observed_mask]):.0f}")
print(f"    When imputed:  median rank = {np.median(imputed_ranks[~observed_mask]):.0f}")

# Weekly detection threshold in terms of rank
print(f"\n  Imputed values vs threshold:")
for w_check in [0, 22, 44, 66, 87]:
    n_obs_w = panel_is_observed[w_check].sum()
    n_imp_w = (~panel_is_observed[w_check] & ~np.isnan(panel_log_metric[w_check])).sum()
    thresh_w = weekly_threshold[w_check]
    imp_vals = panel_log_metric[w_check, ~panel_is_observed[w_check] &
                                 ~np.isnan(panel_log_metric[w_check])]
    if len(imp_vals) > 0:
        print(f"    Week {w_check:>3d}: obs={n_obs_w}, imp={n_imp_w}, "
              f"threshold={thresh_w:.2f}, imp_max={imp_vals.max():.2f}, "
              f"imp_med={np.median(imp_vals):.2f}")


# ══════════════════════════════════════════════════════════════════
# STEP 7: SAVE EXPANDED PANEL
# ══════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("STEP 7: SAVE EXPANDED PANEL")
print("=" * 70)

# Save as numpy arrays for the model to load
output_path = '/Users/hindman/Documents/github/rank-diffusion/llm_fitting/expanded_panel.npz'
np.savez_compressed(output_path,
                    log_metric=panel_log_metric,
                    is_observed=panel_is_observed,
                    rank=panel_rank,
                    endpoint_ids=np.array(expanded_eps_sorted),
                    bp_mask=np.array([ep in bp_eps for ep in expanded_eps_sorted]),
                    dates=np.array(dates))
print(f"  Saved: {output_path}")
print(f"  Shape: {panel_log_metric.shape}")


# ══════════════════════════════════════════════════════════════════
# STEP 8: DIAGNOSTIC COMPARISON — BP-only vs EXPANDED
# ══════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("STEP 8: DIAGNOSTIC COMPARISON")
print("=" * 70)

# Compute diagnostics on both the original BP panel and expanded panel
def compute_diagnostics(log_y, ranks, label, max_eps=2000):
    T, N_p = log_y.shape
    sdf = pd.DataFrame(log_y)
    sch = sdf.diff().iloc[1:]
    sv1 = sch.var()

    diag = {}
    # VR
    for k in [2, 4, 8, 13]:
        if k < T:
            diag[f'VR({k})'] = (sdf.diff(k).iloc[k:].var() / (k * sv1)).median()

    # ACF of changes
    sample = list(range(min(max_eps, N_p)))
    for lag in [1, 2]:
        cors = [sch[i].dropna().autocorr(lag) for i in sample
                if len(sch[i].dropna()) > lag + 5]
        diag[f'ACF({lag})'] = np.nanmedian(cors)

    # RACF
    rdf = pd.DataFrame(ranks)
    for lag in [1, 4, 13]:
        cors = [rdf[i].dropna().autocorr(lag) for i in sample
                if len(rdf[i].dropna()) > lag + 5]
        diag[f'RACF({lag})'] = np.nanmedian(cors)

    # Persistence
    for k in [1, 4, 13]:
        if k < T:
            t0 = set(np.where(ranks[0] <= 100)[0])
            tk = set(np.where(ranks[k] <= 100)[0])
            diag[f'Pers({k})'] = len(t0 & tk)

    # R²
    for k in [1, 4, 13]:
        if k < T:
            diag[f'R²({k})'] = np.corrcoef(log_y[0], log_y[k])[0, 1] ** 2

    return diag

# BP-only diagnostics (from original data)
bp_log = panel_log_metric[:, bp_idx]
bp_rk = panel_rank[:, bp_idx]  # ranks in expanded panel
diag_bp = compute_diagnostics(bp_log, bp_rk, "BP-only")

# Expanded panel diagnostics (all pages)
valid_mask = ~np.any(np.isnan(panel_log_metric), axis=0)
valid_idx = np.where(valid_mask)[0]
exp_log = panel_log_metric[:, valid_idx]
exp_rk = panel_rank[:, valid_idx]
diag_exp = compute_diagnostics(exp_log, exp_rk, "Expanded")

# Original BP diagnostics (ranked among BP only, as in v3.9)
bp_only_rank = np.zeros_like(bp_rk)
for w in range(n_weeks):
    order = np.argsort(-bp_log[w])
    bp_only_rank[w, order] = np.arange(1, len(bp_idx) + 1)
diag_bp_orig = compute_diagnostics(bp_log, bp_only_rank, "BP-orig-rank")

print(f"\n  {'Diagnostic':>12s}  {'BP (orig rank)':>14s}  {'BP (exp rank)':>14s}  {'Expanded':>14s}")
print(f"  {'-'*60}")
all_keys = sorted(set(diag_bp.keys()) | set(diag_exp.keys()) | set(diag_bp_orig.keys()))
for key in all_keys:
    v_orig = diag_bp_orig.get(key, float('nan'))
    v_bp = diag_bp.get(key, float('nan'))
    v_exp = diag_exp.get(key, float('nan'))
    print(f"  {key:>12s}  {v_orig:>14.4f}  {v_bp:>14.4f}  {v_exp:>14.4f}")


elapsed = time.time() - t_start
print(f"\n  Total elapsed: {elapsed:.1f}s")
print("Done.")
