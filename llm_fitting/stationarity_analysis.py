#!/usr/bin/env python3
"""
Stationarity Analysis for Facebook Ranked Ecosystem
====================================================
Addresses peer-review concern C: "Is the empirical period actually stationary?"

Three analyses:
  1. Rolling-window stylized facts (26-week window, 6 key statistics)
  2. Sub-period parameter estimation (3 periods, full pipeline on each)
  3. Formal change-point detection (Pettitt test on rolling statistics)

If the system is approximately stationary, this validates the modeling frame.
If not, it delimits regimes and shows which parameters shift.
"""

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy import stats as sp_stats
import warnings, time
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

t_start = time.time()

# ============================================================
# DATA LOADING
# ============================================================
print("=" * 70)
print("STATIONARITY ANALYSIS — DATA LOADING")
print("=" * 70)

df = pd.read_parquet('/Users/hindman/Documents/github/rank-diffusion/data/raw/fb_ranked_weekly_cutdown.parquet')
df['date'] = pd.to_datetime(df['date'])
dates = sorted(df['date'].unique())
n_weeks = len(dates)
print(f"  {n_weeks} weeks: {dates[0].date()} to {dates[-1].date()}")

ep_counts = df.groupby('endpoint_id')['date'].nunique()
all_weeks_eps = sorted(ep_counts[ep_counts == n_weeks].index)
N_balanced = len(all_weeks_eps)

metric_pivot = df[df['endpoint_id'].isin(all_weeks_eps)].pivot_table(
    index='date', columns='endpoint_id', values='metric_value').sort_index()
rank_pivot = df[df['endpoint_id'].isin(all_weeks_eps)].pivot_table(
    index='date', columns='endpoint_id', values='rank').sort_index()
log_metric = np.log1p(metric_pivot)
log_changes = log_metric.diff().iloc[1:]

print(f"  Balanced panel: {N_balanced} endpoints × {n_weeks} weeks")

# Full-panel (not just balanced) for Zipf and cross-sectional stats
weekly_data = {}
for d in dates:
    wd = df[df['date'] == d].sort_values('rank')
    weekly_data[d] = wd

sample_eps = list(all_weeks_eps)[:2000]

# ============================================================
# PART 1: ROLLING-WINDOW STYLIZED FACTS
# ============================================================
print(f"\n{'='*70}")
print("PART 1: ROLLING-WINDOW STATISTICS (26-week window)")
print(f"{'='*70}")

W = 26  # window size in weeks
half_w = W // 2

# Storage for rolling statistics
roll_dates = []
roll_xsec_var = []       # cross-sectional variance of log-activity
roll_zipf_slope = []     # Zipf slope (power-law exponent)
roll_vr4 = []            # median VR(4) across endpoints
roll_acf1 = []           # median ACF(1) of changes
roll_top100_pers4 = []   # top-100 4-week persistence
roll_median_var = []     # median within-endpoint change variance

for center in range(half_w, n_weeks - half_w):
    t0 = max(0, center - half_w)
    t1 = min(n_weeks, center + half_w)
    center_date = dates[center]
    roll_dates.append(center_date)

    # Slice the balanced panel for this window
    win_log = log_metric.iloc[t0:t1]
    win_changes = win_log.diff().iloc[1:]
    win_ranks = rank_pivot.iloc[t0:t1]
    win_n = t1 - t0

    # 1. Cross-sectional variance (at center week)
    xsec = log_metric.iloc[center]
    roll_xsec_var.append(xsec.var())

    # 2. Zipf slope (at center week, full panel)
    wd = weekly_data[center_date]
    wd_pos = wd[(wd['rank'] >= 1) & (wd['rank'] <= 5000) & (wd['metric_value'] > 0)]
    if len(wd_pos) > 100:
        slope = np.polyfit(np.log(wd_pos['rank'].values), np.log(wd_pos['metric_value'].values), 1)[0]
        roll_zipf_slope.append(slope)
    else:
        roll_zipf_slope.append(np.nan)

    # 3. Median VR(4) within window
    if win_n > 5:
        var_1w = win_changes.var()
        var_4w = win_log.diff(4).iloc[4:].var()
        vr4_per_ep = var_4w / (4 * var_1w)
        vr4_per_ep = vr4_per_ep.replace([np.inf, -np.inf], np.nan)
        roll_vr4.append(vr4_per_ep.median())
    else:
        roll_vr4.append(np.nan)

    # 4. Median ACF(1) of changes within window
    acfs = []
    for ep in sample_eps[:500]:
        ch = win_changes[ep].dropna().values
        if len(ch) > 5:
            ac = np.corrcoef(ch[:-1], ch[1:])[0, 1] if len(ch) > 2 else np.nan
            if np.isfinite(ac):
                acfs.append(ac)
    roll_acf1.append(np.nanmedian(acfs) if acfs else np.nan)

    # 5. Top-100 persistence at lag 4
    if center + 4 < n_weeks:
        t0_top = set(rank_pivot.iloc[center].nsmallest(100).index)
        t4_top = set(rank_pivot.iloc[center + 4].nsmallest(100).index)
        roll_top100_pers4.append(len(t0_top & t4_top))
    else:
        roll_top100_pers4.append(np.nan)

    # 6. Median within-endpoint change variance
    roll_median_var.append(win_changes.var().median())

    if center == half_w or center == n_weeks - half_w - 1:
        print(f"  Window [{dates[t0].date()} — {dates[t1-1].date()}]: "
              f"xsec_var={xsec.var():.2f}, zipf={roll_zipf_slope[-1]:.3f}, "
              f"VR4={roll_vr4[-1]:.3f}, ACF1={roll_acf1[-1]:.3f}")

print(f"  Computed {len(roll_dates)} rolling windows")

# Convert to arrays
roll_dates = np.array(roll_dates)
roll_xsec_var = np.array(roll_xsec_var)
roll_zipf_slope = np.array(roll_zipf_slope)
roll_vr4 = np.array(roll_vr4)
roll_acf1 = np.array(roll_acf1)
roll_top100_pers4 = np.array(roll_top100_pers4, dtype=float)
roll_median_var = np.array(roll_median_var)


# ============================================================
# PART 2: CHANGE-POINT DETECTION (Pettitt test)
# ============================================================
print(f"\n{'='*70}")
print("PART 2: CHANGE-POINT DETECTION")
print(f"{'='*70}")


def pettitt_test(x):
    """
    Pettitt's test for a single change point in the mean.
    Returns: (change_point_index, test_statistic, p_value)
    """
    x = np.asarray(x)
    n = len(x)
    if n < 5:
        return 0, 0, 1.0
    # Compute U statistic
    U = np.zeros(n)
    for t in range(n):
        for i in range(n):
            U[t] += np.sign(x[t] - x[i])
    # Cumulative sum approach
    S = np.cumsum(np.sign(np.subtract.outer(x, x)).sum(axis=1))
    # Actually, the standard Pettitt uses rank-based:
    # U_t = 2 * sum_{i=1}^{t} rank(x_i) - t*(n+1)
    ranks = sp_stats.rankdata(x)
    U = np.zeros(n)
    for t in range(1, n + 1):
        U[t-1] = 2 * np.sum(ranks[:t]) - t * (n + 1)
    K = np.max(np.abs(U))
    tau = np.argmax(np.abs(U))
    # Approximate p-value
    p = 2 * np.exp(-6 * K**2 / (n**3 + n**2))
    p = min(p, 1.0)
    return tau, K, p


stat_series = {
    'Cross-sec variance': roll_xsec_var,
    'Zipf slope': roll_zipf_slope,
    'VR(4)': roll_vr4,
    'ACF(1)': roll_acf1,
    'Top-100 pers(4)': roll_top100_pers4,
    'Median change var': roll_median_var,
}

change_points = {}
for name, series in stat_series.items():
    valid = series[np.isfinite(series)]
    if len(valid) > 10:
        tau, K, p = pettitt_test(valid)
        # Map tau back to date
        valid_idx = np.where(np.isfinite(series))[0]
        cp_idx = valid_idx[min(tau, len(valid_idx)-1)]
        cp_date = roll_dates[cp_idx] if cp_idx < len(roll_dates) else roll_dates[-1]
        change_points[name] = {'tau': tau, 'K': K, 'p': p, 'date': cp_date}

        mean_before = np.mean(valid[:tau+1]) if tau > 0 else np.nan
        mean_after = np.mean(valid[tau+1:]) if tau < len(valid)-1 else np.nan
        cv = np.std(valid) / abs(np.mean(valid)) if np.mean(valid) != 0 else np.nan

        sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else "ns"
        print(f"  {name:<22s}: p={p:.4f} {sig:>3s}  CP at week {tau} ({pd.Timestamp(cp_date).date()})  "
              f"before={mean_before:.4f}  after={mean_after:.4f}  CV={cv:.3f}")
    else:
        print(f"  {name:<22s}: insufficient data")


# ============================================================
# PART 3: SUB-PERIOD PARAMETER ESTIMATION
# ============================================================
print(f"\n{'='*70}")
print("PART 3: SUB-PERIOD PARAMETER ESTIMATION")
print(f"{'='*70}")

# Split into 3 periods (with small overlap for VR estimation)
period_size = n_weeks // 3
periods = [
    ('Early', 0, period_size + 2),
    ('Middle', period_size - 2, 2 * period_size + 2),
    ('Late', 2 * period_size - 2, n_weeks),
]

# Helper: estimate σ_obs from ACF structure in a given window
def estimate_sigma_obs(log_ch_window):
    """Estimate σ_obs from ACF lag structure."""
    acfs_by_lag = {}
    eps_list = list(log_ch_window.columns)[:1000]
    for lag in [1, 2, 3, 4]:
        cors = []
        for ep in eps_list:
            ch = log_ch_window[ep].dropna().values
            if len(ch) > lag + 5:
                ac = np.corrcoef(ch[:-lag], ch[lag:])[0, 1] if len(ch) > lag + 1 else np.nan
                if np.isfinite(ac):
                    cors.append(ac)
        acfs_by_lag[lag] = np.nanmedian(cors) if cors else 0.0

    median_var = log_ch_window.var().median()
    phi_agg = acfs_by_lag[3] / acfs_by_lag[2] if abs(acfs_by_lag[2]) > 1e-6 else 0.5
    gamma1 = acfs_by_lag[1] * median_var
    gamma2 = acfs_by_lag[2] * median_var
    s2_obs = -gamma1 + gamma2 / phi_agg if abs(phi_agg) > 1e-6 else 0.05
    return np.sqrt(np.clip(s2_obs, 0.01**2, 0.50**2)), acfs_by_lag, median_var


# Helper: estimate band parameters
def model_vr(k, se2, phi, sn2, sobs2=0):
    sc2 = sn2 / (1 - phi ** 2) if abs(phi) < 0.999 else sn2 * 1000
    vd = se2 + 2 * sc2 * (1 - phi) + 2 * sobs2
    if vd <= 0: return 1.0
    vk = k * se2 + 2 * sc2 * (1 - phi ** k) + 2 * sobs2
    return vk / (k * vd)


def model_acf1_fn(se2, phi, sn2, sobs2=0):
    sc2 = sn2 / (1 - phi ** 2) if abs(phi) < 0.999 else sn2 * 1000
    vd = se2 + 2 * sc2 * (1 - phi) + 2 * sobs2
    if vd <= 0: return 0.0
    return (-sc2 * (1 - phi) ** 2 - sobs2) / vd


def fit_band_params(emp_var, emp_vr4, emp_acf1, emp_vr13, sobs2):
    def objective(p):
        se2, phi, sn2 = np.exp(p[0]), 0.95 / (1 + np.exp(-p[1])), np.exp(p[2])
        sc2 = sn2 / (1 - phi ** 2) if abs(phi) < 0.999 else sn2 * 1000
        mvar = se2 + 2 * sc2 * (1 - phi) + 2 * sobs2
        L = 10 * (np.log(mvar) - np.log(max(emp_var, 1e-10))) ** 2
        L += 5 * (model_vr(4, se2, phi, sn2, sobs2) - emp_vr4) ** 2
        L += 3 * (model_acf1_fn(se2, phi, sn2, sobs2) - emp_acf1) ** 2
        if emp_vr13 is not None:
            L += 2 * (model_vr(13, se2, phi, sn2, sobs2) - emp_vr13) ** 2
        return L
    best = None
    for _ in range(100):
        x0 = [np.random.uniform(-9, -1), np.random.uniform(-2, 2), np.random.uniform(-4, 1)]
        try:
            r = minimize(objective, x0, method='Nelder-Mead',
                         options={'maxiter': 10000, 'xatol': 1e-10, 'fatol': 1e-12})
            if best is None or r.fun < best.fun:
                best = r
        except:
            pass
    if best is None:
        return 0.1, 0.5, 0.3
    return np.sqrt(np.exp(best.x[0])), 0.95 / (1 + np.exp(-best.x[1])), np.sqrt(np.exp(best.x[2]))


bands = [(1, 100), (101, 500), (501, 2000), (2001, 5000), (5001, 12000)]
avg_rank_full = rank_pivot.mean()

period_params = {}

for pname, t0, t1 in periods:
    print(f"\n  --- Period: {pname} (weeks {t0}-{t1-1}, {dates[t0].date()} to {dates[t1-1].date()}) ---")

    p_log = log_metric.iloc[t0:t1]
    p_changes = p_log.diff().iloc[1:]
    p_ranks = rank_pivot.iloc[t0:t1]
    p_var_1 = p_changes.var()

    # σ_obs
    s_obs, acfs, med_var = estimate_sigma_obs(p_changes)
    sobs2 = s_obs ** 2
    print(f"    σ_obs = {s_obs:.4f}")

    # σ_het
    mean_var = p_var_1.mean()
    var_ratio = mean_var / med_var if med_var > 0 else 1.5
    s_het = np.sqrt(max(np.log(var_ratio) / 2, 0.01))
    print(f"    σ_het = {s_het:.4f}")

    # ACF(1) and VR(4)
    print(f"    ACF(1) = {acfs[1]:.4f}")

    # VR at multiple horizons
    vr_vals = {}
    for k in [2, 4, 8, 13]:
        if k < t1 - t0 - 1:
            vr_k = (p_log.diff(k).iloc[k:].var() / (k * p_var_1)).median()
            vr_vals[k] = vr_k
    for k in sorted(vr_vals.keys()):
        print(f"    VR({k}) = {vr_vals[k]:.4f}")

    # Band-level estimation
    np.random.seed(42)
    bp = {}
    for lo, hi in bands:
        beps = avg_rank_full[(avg_rank_full >= lo) & (avg_rank_full <= hi)].index
        beps_valid = [ep for ep in beps if ep in p_changes.columns]
        if len(beps_valid) < 5:
            continue
        bc = p_changes[beps_valid]
        bm = p_log[beps_valid]
        total_var = bc.var().median()
        vr4_b = (bm.diff(4).iloc[4:].var() / (4 * bc.var())).median() if t1 - t0 > 5 else 0.5
        vr13_b = (bm.diff(13).iloc[13:].var() / (13 * bc.var())).median() if t1 - t0 > 14 else None
        acf1_b_vals = [bc[ep].dropna().autocorr(1) for ep in list(beps_valid)[:300]
                       if len(bc[ep].dropna()) > 6]
        acf1_b = np.nanmedian(acf1_b_vals) if acf1_b_vals else -0.3

        se, phi, sn = fit_band_params(max(total_var, 1e-6), vr4_b, acf1_b, vr13_b, sobs2)
        bp[(lo, hi)] = {'se': se, 'phi': phi, 'sn': sn, 'var': total_var}
        print(f"    Band {lo:5d}-{hi:5d}: se={se:.4f} phi={phi:.4f} sn={sn:.4f} var={total_var:.4f}")

    # κ estimation
    mean_se2 = np.mean([bp[k]['se']**2 for k in bp if bp[k]['se'] > 0])
    E_h2 = np.exp(2 * s_het ** 2)
    xsec_var_p = p_log.iloc[0].var()

    # Simplified κ: κ = E[h²] * mean_se2 / (2 * xsec_var)
    kappa_est = E_h2 * mean_se2 / (2 * max(xsec_var_p, 0.1))
    kappa_est = max(kappa_est, 0.001)
    hl = np.log(2) / kappa_est
    print(f"    κ_base = {kappa_est:.6f} (half-life = {hl:.0f} weeks)")

    # Top-100 persistence
    pers_vals = {}
    for k in [1, 4, 13]:
        if t0 + k < t1:
            t0_top = set(df[(df['date'] == dates[t0]) & (df['rank'] <= 100)]['endpoint_id'])
            tk_top = set(df[(df['date'] == dates[t0+k]) & (df['rank'] <= 100)]['endpoint_id'])
            pers_vals[k] = len(t0_top & tk_top)
    for k in sorted(pers_vals.keys()):
        print(f"    Top-100 pers({k}) = {pers_vals[k]}")

    period_params[pname] = {
        'sigma_obs': s_obs, 'sigma_het': s_het, 'acf1': acfs[1],
        'vr': vr_vals, 'band_params': bp, 'kappa': kappa_est,
        'pers': pers_vals, 'median_var': med_var,
    }


# ============================================================
# PART 4: PARAMETER STABILITY SUMMARY
# ============================================================
print(f"\n\n{'='*70}")
print("PARAMETER STABILITY ACROSS PERIODS")
print(f"{'='*70}")

# Key parameter comparison
print(f"\n{'Parameter':<20s} {'Early':>10s} {'Middle':>10s} {'Late':>10s} {'CV':>8s} {'Stable?':>8s}")
print("-" * 64)

stability_items = []
for param_name, key in [('σ_obs', 'sigma_obs'), ('σ_het', 'sigma_het'),
                          ('ACF(1)', 'acf1'), ('Median Δ-var', 'median_var'),
                          ('κ_base', 'kappa')]:
    vals = [period_params[p][key] for p in ['Early', 'Middle', 'Late']]
    mean_v = np.mean(vals)
    cv = np.std(vals) / abs(mean_v) if abs(mean_v) > 1e-6 else 0
    stable = "YES" if cv < 0.15 else "MARGINAL" if cv < 0.30 else "NO"
    stability_items.append((param_name, vals, cv, stable))
    print(f"  {param_name:<18s} {vals[0]:>10.4f} {vals[1]:>10.4f} {vals[2]:>10.4f} {cv:>7.3f} {stable:>8s}")

# VR comparison
for k in [4, 13]:
    vals = []
    for p in ['Early', 'Middle', 'Late']:
        vals.append(period_params[p]['vr'].get(k, np.nan))
    valid = [v for v in vals if np.isfinite(v)]
    if valid:
        cv = np.std(valid) / abs(np.mean(valid)) if abs(np.mean(valid)) > 1e-6 else 0
        stable = "YES" if cv < 0.15 else "MARGINAL" if cv < 0.30 else "NO"
        print(f"  {'VR('+str(k)+')':<18s} {vals[0]:>10.4f} {vals[1]:>10.4f} {vals[2]:>10.4f} {cv:>7.3f} {stable:>8s}")

# Band-level parameter comparison
print(f"\n  Band-level σ_η (permanent innovation std) across periods:")
for lo, hi in bands:
    vals = []
    for p in ['Early', 'Middle', 'Late']:
        bp = period_params[p]['band_params']
        if (lo, hi) in bp:
            vals.append(bp[(lo, hi)]['se'])
        else:
            vals.append(np.nan)
    valid = [v for v in vals if np.isfinite(v)]
    if len(valid) >= 2:
        cv = np.std(valid) / abs(np.mean(valid)) if abs(np.mean(valid)) > 1e-6 else 0
        stable = "YES" if cv < 0.20 else "MARGINAL" if cv < 0.40 else "NO"
        print(f"    Band {lo:5d}-{hi:5d}: {vals[0]:>8.4f} {vals[1]:>8.4f} {vals[2]:>8.4f}  CV={cv:.3f} [{stable}]")

print(f"\n  Band-level φ (AR persistence) across periods:")
for lo, hi in bands:
    vals = []
    for p in ['Early', 'Middle', 'Late']:
        bp = period_params[p]['band_params']
        if (lo, hi) in bp:
            vals.append(bp[(lo, hi)]['phi'])
        else:
            vals.append(np.nan)
    valid = [v for v in vals if np.isfinite(v)]
    if len(valid) >= 2:
        cv = np.std(valid) / abs(np.mean(valid)) if abs(np.mean(valid)) > 1e-6 else 0
        stable = "YES" if cv < 0.20 else "MARGINAL" if cv < 0.40 else "NO"
        print(f"    Band {lo:5d}-{hi:5d}: {vals[0]:>8.4f} {vals[1]:>8.4f} {vals[2]:>8.4f}  CV={cv:.3f} [{stable}]")


# ============================================================
# STATIONARITY VERDICT
# ============================================================
print(f"\n\n{'='*70}")
print("STATIONARITY VERDICT")
print(f"{'='*70}")

n_cp_sig = sum(1 for v in change_points.values() if v['p'] < 0.05)
n_stable_params = sum(1 for _, _, cv, s in stability_items if s == "YES")
n_total_params = len(stability_items)

# Coefficient of variation of rolling statistics
rolling_cvs = {}
for name, series in stat_series.items():
    valid = series[np.isfinite(series)]
    if len(valid) > 5:
        rolling_cvs[name] = np.std(valid) / abs(np.mean(valid)) if abs(np.mean(valid)) > 1e-6 else 0

print(f"\n  Change-point tests: {n_cp_sig}/{len(change_points)} significant at p<0.05")
print(f"  Parameter stability: {n_stable_params}/{n_total_params} key params stable (CV<0.15)")
print(f"\n  Rolling-window CVs:")
for name, cv in rolling_cvs.items():
    verdict = "stable" if cv < 0.10 else "moderate" if cv < 0.20 else "variable"
    print(f"    {name:<22s}: CV={cv:.3f} [{verdict}]")

# Overall assessment
if n_cp_sig <= 1 and n_stable_params >= 3:
    print(f"\n  OVERALL: System is APPROXIMATELY STATIONARY over the study period.")
    print(f"  The modeling frame (stationary ranked ecosystem) is supported.")
elif n_cp_sig <= 2 and n_stable_params >= 2:
    print(f"\n  OVERALL: System shows MILD non-stationarity.")
    print(f"  Core dynamics are stable; some parameters show moderate drift.")
    print(f"  The modeling frame is supported with caveats.")
else:
    print(f"\n  OVERALL: System shows SIGNIFICANT non-stationarity.")
    print(f"  Consider delimiting regimes or adding time-varying parameters.")


# ============================================================
# FIGURE: ROLLING-WINDOW STYLIZED FACTS
# ============================================================
print(f"\n\nGenerating stationarity figure...")

fig, axes = plt.subplots(3, 2, figsize=(14, 12))

plot_items = [
    ('Cross-Sectional Variance', roll_xsec_var, 'Cross-sec var of log-activity'),
    ('Zipf Slope', roll_zipf_slope, 'Power-law exponent (top 5K)'),
    ('VR(4)', roll_vr4, 'Median variance ratio at 4 weeks'),
    ('ACF(1) of Changes', roll_acf1, 'Median lag-1 autocorrelation'),
    ('Top-100 Persistence (4wk)', roll_top100_pers4, 'Endpoints remaining in top 100'),
    ('Median Change Variance', roll_median_var, 'Median within-endpoint Δ variance'),
]

for idx, (title, series, ylabel) in enumerate(plot_items):
    ax = axes[idx // 2, idx % 2]
    valid_mask = np.isfinite(series)
    dates_valid = roll_dates[valid_mask]
    vals_valid = series[valid_mask]

    ax.plot(dates_valid, vals_valid, 'b-', linewidth=1.0, alpha=0.8)

    # Overall mean and ±1 std band
    mean_v = np.mean(vals_valid)
    std_v = np.std(vals_valid)
    ax.axhline(y=mean_v, color='red', linestyle='--', alpha=0.5, linewidth=1)
    ax.fill_between(dates_valid, mean_v - std_v, mean_v + std_v,
                     alpha=0.1, color='red')

    # Mark change point if significant
    if title.replace(' (4wk)', '').replace(' of Changes', '').replace('Median ', '') in change_points:
        # Try to find the right key
        for cp_key in change_points:
            if cp_key.lower().replace('-', '').replace('_', '') in title.lower().replace('-', '').replace('_', ''):
                cp = change_points[cp_key]
                if cp['p'] < 0.05:
                    ax.axvline(x=cp['date'], color='orange', linestyle='-', alpha=0.7,
                               linewidth=2, label=f'CP (p={cp["p"]:.3f})')
                    ax.legend(fontsize=7, loc='best')
                break

    # Sub-period boundaries
    for _, _, t1_p in periods[:-1]:
        if t1_p < len(dates):
            ax.axvline(x=dates[t1_p], color='gray', linestyle=':', alpha=0.4)

    ax.set_title(title, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.tick_params(axis='x', rotation=30, labelsize=8)
    ax.grid(True, alpha=0.3)

    # Annotate CV
    cv = std_v / abs(mean_v) if abs(mean_v) > 1e-6 else 0
    ax.text(0.02, 0.95, f'CV={cv:.3f}', transform=ax.transAxes,
            fontsize=8, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

plt.suptitle('Rolling-Window Stylized Facts (26-week window)\nGray dashed: sub-period boundaries',
             fontsize=13, y=1.01)
plt.tight_layout()
plt.savefig('/Users/hindman/Documents/github/rank-diffusion/llm_fitting/stationarity_rolling.png',
            dpi=200, bbox_inches='tight')
print(f"  Saved: stationarity_rolling.png")
plt.close()


# ============================================================
# FIGURE: SUB-PERIOD PARAMETER COMPARISON
# ============================================================
fig2, axes2 = plt.subplots(2, 3, figsize=(15, 8))

# Panel 1: σ_obs across periods
ax = axes2[0, 0]
pnames = ['Early', 'Middle', 'Late']
vals = [period_params[p]['sigma_obs'] for p in pnames]
ax.bar(pnames, vals, color=['#2196F3', '#4CAF50', '#FF9800'], edgecolor='white')
ax.set_title('σ_obs', fontsize=11); ax.set_ylabel('Value')
for i, v in enumerate(vals):
    ax.text(i, v + 0.002, f'{v:.4f}', ha='center', fontsize=8)

# Panel 2: σ_het
ax = axes2[0, 1]
vals = [period_params[p]['sigma_het'] for p in pnames]
ax.bar(pnames, vals, color=['#2196F3', '#4CAF50', '#FF9800'], edgecolor='white')
ax.set_title('σ_het', fontsize=11); ax.set_ylabel('Value')
for i, v in enumerate(vals):
    ax.text(i, v + 0.005, f'{v:.4f}', ha='center', fontsize=8)

# Panel 3: κ_base
ax = axes2[0, 2]
vals = [period_params[p]['kappa'] for p in pnames]
ax.bar(pnames, vals, color=['#2196F3', '#4CAF50', '#FF9800'], edgecolor='white')
ax.set_title('κ_base', fontsize=11); ax.set_ylabel('Value')
for i, v in enumerate(vals):
    ax.text(i, v + 0.0003, f'{v:.6f}', ha='center', fontsize=7)

# Panel 4: VR(4) across periods
ax = axes2[1, 0]
vals = [period_params[p]['vr'].get(4, np.nan) for p in pnames]
ax.bar(pnames, vals, color=['#2196F3', '#4CAF50', '#FF9800'], edgecolor='white')
ax.set_title('VR(4)', fontsize=11); ax.set_ylabel('Value')
for i, v in enumerate(vals):
    if np.isfinite(v):
        ax.text(i, v + 0.005, f'{v:.4f}', ha='center', fontsize=8)

# Panel 5: ACF(1)
ax = axes2[1, 1]
vals = [period_params[p]['acf1'] for p in pnames]
ax.bar(pnames, vals, color=['#2196F3', '#4CAF50', '#FF9800'], edgecolor='white')
ax.set_title('ACF(1)', fontsize=11); ax.set_ylabel('Value')
for i, v in enumerate(vals):
    ax.text(i, v - 0.02, f'{v:.4f}', ha='center', fontsize=8)

# Panel 6: Band σ_η across periods (grouped bar)
ax = axes2[1, 2]
x_pos = np.arange(len(bands))
width = 0.25
for pi, (pn, color) in enumerate(zip(pnames, ['#2196F3', '#4CAF50', '#FF9800'])):
    bp = period_params[pn]['band_params']
    vals = [bp.get((lo, hi), {}).get('se', 0) for lo, hi in bands]
    ax.bar(x_pos + pi * width, vals, width, label=pn, color=color, edgecolor='white')
ax.set_xticks(x_pos + width)
ax.set_xticklabels([f'{lo}-{hi}' for lo, hi in bands], fontsize=7, rotation=30)
ax.set_title('Band σ_η (permanent std)', fontsize=11)
ax.set_ylabel('Value'); ax.legend(fontsize=8)

plt.suptitle('Sub-Period Parameter Comparison (Early / Middle / Late)', fontsize=13, y=1.01)
plt.tight_layout()
plt.savefig('/Users/hindman/Documents/github/rank-diffusion/llm_fitting/stationarity_params.png',
            dpi=200, bbox_inches='tight')
print(f"  Saved: stationarity_params.png")
plt.close()

elapsed = time.time() - t_start
print(f"\n{'='*70}")
print(f"STATIONARITY ANALYSIS COMPLETE — {elapsed:.0f}s")
print(f"{'='*70}")
