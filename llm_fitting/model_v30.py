#!/usr/bin/env python3
"""
Permanent-Transitory Rank Diffusion Model v3.0
===============================================
Combines the best elements of v2.7 and v2.9:

v2.9 achieved 11/15: RACF breakthrough (all 3 pass for the first time) from
burn-in, plus all VR and ACF pass. But cross-sec variance exploded (3.2→12.2)
because rank-dependent κ doesn't control spread, only shape.

v3.0 changes:
1. GLOBAL-MEAN κ (from v2.7): Pulls τ toward population mean, which properly
   stabilizes cross-sectional variance. v2.9's rank-dep κ let variance explode.

2. BURN-IN (from v2.9): 50-week burn-in lets c reach stationary distribution
   naturally. This is what fixed RACF in v2.9 — more transitory variance at
   recording start creates realistic rank volatility.

3. perm_boost=1.0: The burn-in addresses the R² inflation from c=0 at t=0.
   If R² is still too high, a moderate perm_boost may be needed in v3.1.

4. Within-endpoint t_df (from v2.8): Fixes kurtosis (4.97 vs aggregate 2.66).

Expected: v2.7's VR+ACF+variance stability + v2.9's RACF improvement.
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
from matplotlib.gridspec import GridSpec

t_start = time.time()

# ============================================================
# DATA LOADING
# ============================================================
print("=" * 70)
print("LOADING DATA")
print("=" * 70)

df = pd.read_parquet('/Users/hindman/Documents/github/rank-diffusion/data/raw/fb_ranked_weekly_cutdown.parquet')
df['date'] = pd.to_datetime(df['date'])
dates = sorted(df['date'].unique())
n_weeks = len(dates)

ep_counts = df.groupby('endpoint_id')['date'].nunique()
all_weeks_eps = sorted(ep_counts[ep_counts == n_weeks].index)
N_balanced = len(all_weeks_eps)

weekly_eps = {d: set(df[df['date'] == d]['endpoint_id']) for d in dates}
weekly_counts = [len(weekly_eps[d]) for d in dates]
mean_N = np.mean(weekly_counts)
exits_list = [len(weekly_eps[dates[i-1]] - weekly_eps[dates[i]]) for i in range(1, len(dates))]
mean_exits = np.mean(exits_list)
print(f"  N_balanced={N_balanced}, mean_N={mean_N:.0f}, exits={mean_exits:.0f}/wk")

metric_pivot = df[df['endpoint_id'].isin(all_weeks_eps)].pivot_table(
    index='date', columns='endpoint_id', values='metric_value').sort_index()
rank_pivot = df[df['endpoint_id'].isin(all_weeks_eps)].pivot_table(
    index='date', columns='endpoint_id', values='rank').sort_index()
log_metric = np.log1p(metric_pivot)
log_changes = log_metric.diff().iloc[1:]

var_1 = log_changes.var()
vr_emp = {}
for k in [2, 3, 4, 6, 8, 13, 17, 26, 39, 52]:
    if k < n_weeks:
        vr_emp[k] = (log_metric.diff(k).iloc[k:].var() / (k * var_1)).median()

sample_eps = list(all_weeks_eps)[:2000]
acf_emp, racf_emp = {}, {}
for lag in [1, 2, 3, 4, 8]:
    cors = [log_changes[ep].dropna().autocorr(lag) for ep in sample_eps
            if len(log_changes[ep].dropna()) > lag + 5]
    acf_emp[lag] = np.nanmedian(cors)
for lag in [1, 4, 13, 26, 52]:
    cors = [rank_pivot[ep].dropna().autocorr(lag) for ep in sample_eps
            if len(rank_pivot[ep].dropna()) > lag + 5]
    racf_emp[lag] = np.nanmedian(cors)

pers_emp, xr2_emp = {}, {}
for k in [1, 4, 13, 26, 52]:
    if k < n_weeks:
        t0 = set(df[(df['date'] == dates[0]) & (df['rank'] <= 100)]['endpoint_id'])
        tk = set(df[(df['date'] == dates[k]) & (df['rank'] <= 100)]['endpoint_id'])
        pers_emp[k] = len(t0 & tk)
        t0v = log_metric.iloc[0]; tkv = log_metric.iloc[k]
        valid = t0v.notna() & tkv.notna()
        xr2_emp[k] = np.corrcoef(t0v[valid], tkv[valid])[0, 1] ** 2

s0 = df[df['date'] == dates[0]].sort_values('rank')
s0t = s0[(s0['rank'] >= 1) & (s0['rank'] <= 5000) & (s0['metric_value'] > 0)]
zipf_slope = np.polyfit(np.log(s0t['rank'].values), np.log(s0t['metric_value'].values), 1)[0]

all_ch_emp = log_changes.values.flatten()
all_ch_emp = all_ch_emp[np.isfinite(all_ch_emp)]
emp_kurt = sp_stats.kurtosis(all_ch_emp, fisher=True)
emp_mean_var = var_1.mean()
emp_median_var = var_1.median()
xsec_var_emp = log_metric.var(axis=1).mean()

w0_all = df[df['date'] == dates[0]]
xsec_var_full = np.log1p(w0_all[w0_all['metric_value'] > 0]['metric_value']).var()

print(f"  Change var: median={emp_median_var:.4f}, mean={emp_mean_var:.4f}, ratio={emp_mean_var/emp_median_var:.2f}")
print(f"  Cross-sec var: bp={xsec_var_emp:.2f}, full_w0={xsec_var_full:.2f}")

avg_rank = rank_pivot.mean()
bands = [(1, 100), (101, 500), (501, 2000), (2001, 5000), (5001, 12000)]
band_stats = {}
for lo, hi in bands:
    beps = avg_rank[(avg_rank >= lo) & (avg_rank <= hi)].index
    bc = log_changes[beps]; bm = log_metric[beps]
    total_var = bc.var().median()
    vr4 = (bm.diff(4).iloc[4:].var() / (4 * bc.var())).median()
    vr13 = (bm.diff(13).iloc[13:].var() / (13 * bc.var())).median()
    acfs = [bc[ep].dropna().autocorr(1) for ep in list(beps)[:500]
            if len(bc[ep].dropna()) > 6]
    band_stats[(lo, hi)] = {'n': len(beps), 'var': total_var,
                            'vr4': vr4, 'vr13': vr13, 'acf1': np.nanmedian(acfs)}

print(f"\n  Targets:")
print(f"    VR(4)={vr_emp[4]:.4f}, VR(13)={vr_emp[13]:.4f}, ACF(1)={acf_emp[1]:.4f}")
print(f"    RACF(1)={racf_emp[1]:.4f}, R²(1)={xr2_emp[1]:.4f}, R²(13)={xr2_emp[13]:.4f}")
print(f"    Kurt={emp_kurt:.1f}, Top-100 pers(1)={pers_emp[1]}")

# ============================================================
# PRINCIPLED PARAMETER ESTIMATION
# ============================================================
print("\n" + "=" * 70)
print("STAGE 1: ESTIMATE σ_obs FROM ACF STRUCTURE")
print("=" * 70)

# For the PT + observation noise model:
#   Δy_t = η_t + Δc_t + ε_t - ε_{t-1}
# Autocovariances:
#   γ(1) = -σ²_c(1-φ)² - σ²_obs   (obs noise affects only lag 1)
#   γ(h) = -σ²_c(1-φ)² φ^{h-1}    for h ≥ 2
# Therefore:
#   φ_agg = γ(3)/γ(2) = ρ(3)/ρ(2)
#   σ²_c(1-φ)² = -γ(2)/φ_agg
#   σ²_obs = -γ(1) - (-σ²_c(1-φ)²) = -γ(1) + γ(2)/φ_agg

phi_agg = acf_emp[3] / acf_emp[2]
gamma1 = acf_emp[1] * emp_median_var
gamma2 = acf_emp[2] * emp_median_var
gamma3 = acf_emp[3] * emp_median_var

sigma2_obs_est = -gamma1 + gamma2 / phi_agg
sigma_obs = np.sqrt(np.clip(sigma2_obs_est, 0.01**2, 0.50**2))
sobs2 = sigma_obs ** 2

# Diagnostic: implied structural parameters
sc2_1mphi_sq = -gamma2 / phi_agg  # σ²_c(1-φ)²
sigma2_eta_agg = emp_median_var - 2 * sc2_1mphi_sq / (1 - phi_agg) - 2 * sobs2
# (Using Var(Δy) = σ²_η + 2σ²_c(1-φ) + 2σ²_obs, and σ²_c(1-φ) = σ²_c(1-φ)²/(1-φ))

print(f"  Aggregate ACF: ρ(1)={acf_emp[1]:.4f}, ρ(2)={acf_emp[2]:.4f}, ρ(3)={acf_emp[3]:.4f}")
print(f"  φ_agg = ρ(3)/ρ(2) = {phi_agg:.4f}")
print(f"  γ(1)={gamma1:.4f}, γ(2)={gamma2:.4f}")
print(f"  σ²_obs = {sigma2_obs_est:.4f} → σ_obs = {sigma_obs:.4f}")
print(f"  2σ²_obs = {2*sobs2:.4f} (contribution to Var(Δy))")
print(f"  σ²_c(1-φ)² = {sc2_1mphi_sq:.4f}")
print(f"  Implied σ²_η_agg = {max(0, sigma2_eta_agg):.4f}")

# Validate: check a few σ_obs candidates around the estimate
print(f"\n  Profile validation:")
for sobs_check in [sigma_obs - 0.05, sigma_obs, sigma_obs + 0.05]:
    if sobs_check < 0.05:
        continue
    s2 = sobs_check ** 2
    # Reconstruct implied ACF(1) from the model
    implied_acf1 = (-sc2_1mphi_sq - s2) / emp_median_var
    print(f"    σ_obs={sobs_check:.3f}: implied ACF(1)={implied_acf1:.4f} (emp={acf_emp[1]:.4f})")

# ============================================================
print("\n" + "=" * 70)
print("STAGE 2: ESTIMATE σ_het FROM VARIANCE HETEROGENEITY")
print("=" * 70)

# If h_i ~ lognormal(0, σ_het), then var_i = h²_i × V_base
# median(var_i) = V_base (since median(h²) = 1 for lognormal)
# mean(var_i) = V_base × E[h²] = V_base × exp(2σ²_het)
# So: exp(2σ²_het) = mean_var/median_var
# σ_het = sqrt(log(mean_var/median_var) / 2)

var_ratio = emp_mean_var / emp_median_var
sigma_het = np.sqrt(np.log(var_ratio) / 2)
print(f"  mean/median variance ratio: {var_ratio:.4f}")
print(f"  σ_het = sqrt(log({var_ratio:.4f})/2) = {sigma_het:.4f}")
print(f"  E[h²] = exp(2×{sigma_het:.4f}²) = {np.exp(2*sigma_het**2):.4f}")

# ============================================================
print("\n" + "=" * 70)
print("STAGE 3: ESTIMATE t_df FROM WITHIN-ENDPOINT RESIDUALS (MLE)")
print("=" * 70)

# v2.7 used aggregate changes → MLE gave t_df=3.0 → kurtosis=48.8 (catastrophic).
# Problem: aggregate changes mix between-endpoint heterogeneity (different variances)
# with within-endpoint tail behavior. The heterogeneity makes the aggregate look
# heavier-tailed than any individual endpoint actually is.
#
# Fix: Standardize each endpoint's changes by its own mean/std, pool the residuals,
# then fit t-distribution. This removes between-endpoint variance heterogeneity.

standardized_residuals = []
for ep in sample_eps:
    ch = log_changes[ep].dropna().values
    if len(ch) > 10:
        mu_ep = np.mean(ch)
        std_ep = np.std(ch, ddof=1)
        if std_ep > 1e-6:
            z_ep = (ch - mu_ep) / std_ep
            standardized_residuals.append(z_ep)

z_within = np.concatenate(standardized_residuals)
print(f"  Pooled {len(standardized_residuals)} endpoints, {len(z_within)} residuals")

# MLE fit on within-endpoint standardized residuals
df_fit, loc_fit, scale_fit = sp_stats.t.fit(z_within)
t_df = max(3.0, df_fit)

# Also fit on aggregate for comparison
z_agg = (all_ch_emp - np.mean(all_ch_emp)) / np.std(all_ch_emp)
df_agg, _, _ = sp_stats.t.fit(z_agg)

print(f"  Within-endpoint MLE: df={df_fit:.2f}, loc={loc_fit:.4f}, scale={scale_fit:.4f}")
print(f"  Aggregate MLE (for comparison): df={df_agg:.2f}")
print(f"  Using t_df = {t_df:.2f} (within-endpoint removes heterogeneity double-counting)")

# ============================================================
print("\n" + "=" * 70)
print("STAGE 4: ESTIMATE JUMP PARAMETERS FROM TAIL EXCESS")
print("=" * 70)

# Compare observed tail frequency to the fitted t-distribution
# Use within-endpoint residuals (z_within) for consistency with t_df estimation
# But scale threshold by the fitted scale parameter
threshold = 4.0
expected_tail = 2 * sp_stats.t.sf(threshold, df=t_df, loc=0, scale=scale_fit)
actual_tail = np.mean(np.abs(z_within - loc_fit) > threshold * scale_fit)
jump_prob = max(0.005, actual_tail - expected_tail)

# Jump scale from the ratio of extreme-change magnitude to typical magnitude
extreme_mask = np.abs(z_within) > threshold * scale_fit
if extreme_mask.sum() > 10:
    jump_scale = np.std(z_within[extreme_mask]) / np.std(z_within[~extreme_mask])
else:
    jump_scale = 5.0

print(f"  Tail threshold: {threshold}σ (scaled by {scale_fit:.4f})")
print(f"  Expected tail fraction (t_{t_df:.1f}): {expected_tail:.4f}")
print(f"  Actual tail fraction: {actual_tail:.4f}")
print(f"  jump_prob = max(0.005, {actual_tail:.4f} - {expected_tail:.4f}) = {jump_prob:.4f}")
print(f"  jump_scale = {jump_scale:.2f}")

# ============================================================
print("\n" + "=" * 70)
print("STAGE 5: BAND-LEVEL STRUCTURAL ESTIMATION")
print("=" * 70)

print(f"  Using σ_obs={sigma_obs:.4f} (contributes {2*sobs2:.4f} to Var(Δy))")

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

def fit_params(emp_var, emp_vr4, emp_acf1, emp_vr13=None, sobs2=0):
    def objective(p):
        se2, phi, sn2 = np.exp(p[0]), 0.95/(1+np.exp(-p[1])), np.exp(p[2])
        sc2 = sn2/(1-phi**2) if abs(phi)<0.999 else sn2*1000
        mvar = se2 + 2*sc2*(1-phi) + 2*sobs2
        L = 10*(np.log(mvar)-np.log(emp_var))**2
        L += 5*(model_vr(4, se2, phi, sn2, sobs2)-emp_vr4)**2
        L += 3*(model_acf1_fn(se2, phi, sn2, sobs2)-emp_acf1)**2
        if emp_vr13: L += 2*(model_vr(13, se2, phi, sn2, sobs2)-emp_vr13)**2
        return L
    best = None
    for _ in range(200):
        x0 = [np.random.uniform(-9,-1), np.random.uniform(-2,2), np.random.uniform(-4,1)]
        try:
            r = minimize(objective, x0, method='Nelder-Mead',
                         options={'maxiter': 15000, 'xatol': 1e-10, 'fatol': 1e-12})
            if best is None or r.fun < best.fun: best = r
        except: pass
    return np.exp(best.x[0]), 0.95/(1+np.exp(-best.x[1])), np.exp(best.x[2])

band_params = {}
for (lo, hi), st in band_stats.items():
    se2, phi, sn2 = fit_params(st['var'], st['vr4'], st['acf1'], st['vr13'], sobs2)
    sc2 = sn2/(1-phi**2)
    mvar_s = se2 + 2*sc2*(1-phi)
    mvar_t = mvar_s + 2*sobs2
    band_params[(lo,hi)] = {'se':np.sqrt(se2),'phi':phi,'sn':np.sqrt(sn2),
                            'se2':se2,'sn2':sn2,'pf':se2/mvar_t}
    print(f"  {lo:5d}-{hi:5d}: se={np.sqrt(se2):.4f} phi={phi:.4f} "
          f"sn={np.sqrt(sn2):.4f} perm={se2/mvar_t*100:.1f}% "
          f"struct={mvar_s:.4f} total={mvar_t:.4f}")

bc_arr = np.array([np.sqrt(lo*hi) for lo,hi in band_params.keys()])
ses_arr = np.array([p['se'] for p in band_params.values()])
phs_arr = np.array([p['phi'] for p in band_params.values()])
sns_arr = np.array([p['sn'] for p in band_params.values()])

def get_p(ranks):
    lr = np.log(np.clip(ranks.astype(float), 1, bc_arr[-1]*2))
    return (np.interp(lr, np.log(bc_arr), ses_arr),
            np.interp(lr, np.log(bc_arr), phs_arr),
            np.interp(lr, np.log(bc_arr), sns_arr))

# ============================================================
print("\n" + "=" * 70)
print("STAGE 6: ESTIMATE κ FROM VARIANCE STATIONARITY")
print("=" * 70)

# Cross-sectional variance stationarity:
# d/dt Var(τ) = E[η²] - 2κ·Var(τ) = 0
# E[η²] = E[h²] × population-weighted-mean(σ²_η)
# Including jumps: E[η²] = E[h²] × mean(σ²_η) × (1 - p_j + p_j × s_j²)

total_n = sum(st['n'] for st in band_stats.values())
mean_se2 = sum(band_params[(lo,hi)]['se2'] * band_stats[(lo,hi)]['n']
               for lo,hi in band_params) / total_n

E_h2 = np.exp(2 * sigma_het ** 2)  # E[h²] for h ~ lognormal(0, σ_het)
jump_var_factor = (1 - jump_prob + jump_prob * jump_scale ** 2)
mean_eta2 = E_h2 * mean_se2 * jump_var_factor

kappa = mean_eta2 / (2 * xsec_var_full)
kappa = max(kappa, 0.0005)  # Floor to avoid pathological zero

print(f"  Population-weighted mean(σ²_η) = {mean_se2:.6f}")
print(f"  E[h²] = {E_h2:.4f}")
print(f"  Jump variance factor = {jump_var_factor:.4f}")
print(f"  Mean permanent innovation E[η²] = {mean_eta2:.6f}")
print(f"  Cross-sec variance target = {xsec_var_full:.4f}")
print(f"  κ = {mean_eta2:.6f} / (2 × {xsec_var_full:.4f}) = {kappa:.6f}")
print(f"  Half-life = {np.log(2)/kappa:.0f} weeks")

# ============================================================
# PARAMETER SUMMARY
# ============================================================
perm_boost = 1.0  # No boost — burn-in handles R² correction

print("\n" + "=" * 70)
print("ESTIMATED PARAMETER SUMMARY")
print("=" * 70)
print(f"  σ_obs     = {sigma_obs:.4f}  (from ACF structure)")
print(f"  σ_het     = {sigma_het:.4f}  (from variance heterogeneity)")
print(f"  t_df      = {t_df:.2f}    (from MLE on changes)")
print(f"  κ         = {kappa:.6f} (from variance stationarity, HL={np.log(2)/kappa:.0f}wk)")
print(f"  jump_prob = {jump_prob:.4f}  (from tail excess)")
print(f"  jump_scale= {jump_scale:.2f}   (from extreme-change magnitude)")
print(f"  perm_boost= {perm_boost:.1f}    (removed — burn-in handles R²)")

# Entry/exit parameters (kept from v2.6, match empirical exit rate)
inc_alpha = 0.3
p_exit_incumbent = 0.0040
inc_p_base = p_exit_incumbent * (inc_alpha + 1)
trans_p_exit = 0.07

# ============================================================
# SIMULATION v3.0
# ============================================================
print("\n" + "=" * 70)
print("SIMULATION v3.0")
print("=" * 70)

N_FULL = int(mean_N)
T_SIM = n_weeks
T_BURNIN = 50  # Weeks of burn-in to let c reach stationary
T_TOTAL = T_BURNIN + T_SIM

print(f"  N={N_FULL}, T_record={T_SIM}, T_burnin={T_BURNIN}, T_total={T_TOTAL}")
print(f"  κ={kappa:.6f} (global mean rev, half-life={np.log(2)/kappa:.0f}wk)")
print(f"  t_df={t_df:.2f}, σ_het={sigma_het:.4f}, σ_obs={sigma_obs:.4f}")
print(f"  perm_boost={perm_boost:.1f}, jump_prob={jump_prob:.4f}, jump_scale={jump_scale:.2f}")

# Initialize from ALL week-0 endpoints
w0_data = df[df['date'] == dates[0]].sort_values('rank')
w0_log = np.log1p(w0_data['metric_value'].values)
w0_sorted = np.sort(w0_log)[::-1]
N_w0 = len(w0_sorted)

if N_w0 < N_FULL:
    rf = np.arange(1, N_w0+1)
    sl, ic = np.polyfit(np.log(rf[-2000:]), w0_sorted[-2000:], 1)
    er = np.arange(N_w0+1, N_FULL+1)
    w0_sorted = np.concatenate([w0_sorted, ic + sl*np.log(er)])
    print(f"  Init: {N_w0} real + {N_FULL-N_w0} extrapolated")
else:
    w0_sorted = w0_sorted[:N_FULL]
    print(f"  Init: {N_FULL} from {N_w0} real endpoints")

# Initialize: τ = obs, c = 0 (burn-in will equilibrate c)
tau = w0_sorted.copy()
c = np.zeros(N_FULL)
print(f"  Init τ: var={np.var(tau):.4f} (emp={xsec_var_full:.4f})")
print(f"  Init c: all zeros (burn-in will equilibrate)")

np.random.seed(42)
het_multiplier = np.exp(np.random.normal(0, sigma_het, N_FULL))
het_multiplier = np.clip(het_multiplier, 0.15, 8.0)
print(f"  E[h²] = {np.mean(het_multiplier**2):.3f} (theory: {E_h2:.3f})")

ep_type = np.zeros(N_FULL, dtype=int)
endpoint_id = np.arange(N_FULL)
next_id = N_FULL

# Recording arrays (only for T_SIM post-burn-in weeks)
sim_ly = np.zeros((T_SIM, N_FULL))
sim_ly_true = np.zeros((T_SIM, N_FULL))
sim_rk = np.zeros((T_SIM, N_FULL), dtype=int)
sim_ids = np.zeros((T_SIM, N_FULL), dtype=int)

# Initial ranking
obs_noise = np.random.normal(0, sigma_obs, N_FULL)
y0_obs = tau + c + obs_noise
order = np.argsort(-np.exp(y0_obs))
ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)

print(f"\nBurn-in ({T_BURNIN} weeks)...")
total_exits = 0
xsec_vars = [np.var(tau)]

for t_abs in range(1, T_TOTAL):
    cr = ranks
    se, phi_v, sn = get_p(cr)

    se_het = se * het_multiplier * perm_boost
    sn_het = sn * het_multiplier

    is_jump = np.random.random(N_FULL) < jump_prob
    eta = np.where(is_jump,
                   np.random.normal(0, se_het * jump_scale),
                   np.random.normal(0, se_het))

    t_scale = sn_het * np.sqrt(max(t_df-2, 0.5)/t_df)
    nu = sp_stats.t.rvs(df=t_df, size=N_FULL) * t_scale
    c = phi_v * c + nu

    # Global mean reversion: stabilize cross-sectional variance
    current_mean = np.mean(tau)
    tau += eta - kappa * (tau - current_mean)

    # Exit/entry
    nr = cr / N_FULL
    p_exit = np.where(ep_type == 0,
                      inc_p_base * (nr ** inc_alpha),
                      trans_p_exit)
    exit_mask = np.random.random(N_FULL) < p_exit
    n_ex = exit_mask.sum()
    total_exits += n_ex

    if n_ex > 0:
        exi = np.where(exit_mask)[0]
        n_burst = max(1, int(n_ex * 0.008))
        n_norm = n_ex - n_burst
        bq = np.percentile(tau[~exit_mask], 10)
        bstd = np.std(tau[tau < np.median(tau)]) * 0.4
        new_tau = np.random.normal(bq, bstd, n_norm)
        if n_burst > 0:
            buq = np.percentile(tau[~exit_mask], 90)
            bust = np.std(tau) * 0.25
            new_tau = np.concatenate([new_tau, np.random.normal(buq, bust, n_burst)])
        tau[exi] = new_tau
        c[exi] = sp_stats.t.rvs(df=t_df, size=n_ex) * 0.3
        het_multiplier[exi] = np.clip(np.exp(np.random.normal(0, sigma_het, n_ex)), 0.15, 8.0)
        ep_type[exi] = 1
        endpoint_id[exi] = np.arange(next_id, next_id+n_ex)
        next_id += n_ex

    log_y_true = tau + c
    obs_noise = np.random.normal(0, sigma_obs, N_FULL)
    log_y_obs = log_y_true + obs_noise

    order = np.argsort(-np.exp(log_y_obs))
    ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)

    xsec_vars.append(np.var(tau))

    # Record post-burn-in
    t_rec = t_abs - T_BURNIN
    if t_rec == 0:
        print(f"  Burn-in done. c var={np.var(c):.4f}, τ var={np.var(tau):.4f}")
        # Reset exit tracking and endpoint IDs for recording period
        total_exits_record = 0
        # Record t=0 of the post-burn-in period
        sim_ly[0] = log_y_obs
        sim_ly_true[0] = log_y_true
        sim_rk[0] = ranks
        sim_ids[0] = endpoint_id.copy()
    elif t_rec > 0 and t_rec < T_SIM:
        sim_ly[t_rec] = log_y_obs
        sim_ly_true[t_rec] = log_y_true
        sim_rk[t_rec] = ranks
        sim_ids[t_rec] = endpoint_id.copy()

avg_ex = total_exits / (T_TOTAL-1)
print(f"  Avg exits/week: {avg_ex:.1f} (target: {mean_exits:.1f})")
print(f"  Cross-sec var: t=0 {xsec_vars[0]:.4f}, t=end {xsec_vars[-1]:.4f}, mean {np.mean(xsec_vars):.4f}")
print(f"  Post-burnin xsec vars: t_rec=0 {xsec_vars[T_BURNIN]:.4f}, t_rec=87 {xsec_vars[-1]:.4f}")

# Balanced panel
init_ids = set(sim_ids[0])
survivors = init_ids.copy()
for t in range(1, T_SIM):
    survivors &= set(sim_ids[t])
N_BP = len(survivors)
print(f"  Survivors: {N_BP}/{N_FULL} ({N_BP/N_FULL*100:.1f}%, target ~{N_balanced/mean_N*100:.0f}%)")

survivor_list = sorted(survivors)
bp_ly = np.zeros((T_SIM, N_BP))
bp_ly_true = np.zeros((T_SIM, N_BP))
bp_rk = np.zeros((T_SIM, N_BP), dtype=int)
for t in range(T_SIM):
    id_map = {eid: idx for idx, eid in enumerate(sim_ids[t])}
    for j, sid in enumerate(survivor_list):
        idx = id_map[sid]
        bp_ly[t,j] = sim_ly[t,idx]
        bp_ly_true[t,j] = sim_ly_true[t,idx]
        bp_rk[t,j] = sim_rk[t,idx]

# ============================================================
# VALIDATION
# ============================================================
print("\n" + "=" * 70)
print("VALIDATION")
print("=" * 70)

sim_df = pd.DataFrame(bp_ly)
sim_ch = sim_df.diff().iloc[1:]
sim_v1 = sim_ch.var()

sim_mean_var = sim_v1.mean()
sim_median_var = sim_v1.median()
bp_xsec_var = pd.DataFrame(bp_ly).var(axis=1).mean()
bp_xsec_var_0 = np.var(bp_ly[0])
bp_xsec_var_end = np.var(bp_ly[-1])

print(f"  Sim change var: median={sim_median_var:.4f}, mean={sim_mean_var:.4f}, ratio={sim_mean_var/sim_median_var:.2f}")
print(f"  Emp change var: median={emp_median_var:.4f}, mean={emp_mean_var:.4f}, ratio={emp_mean_var/emp_median_var:.2f}")
print(f"  BP cross-sec var: sim_mean={bp_xsec_var:.4f}, sim_t0={bp_xsec_var_0:.4f}, sim_end={bp_xsec_var_end:.4f}, emp={xsec_var_emp:.4f}")

results = {}

print("\n--- Variance Ratios ---")
for k in [2, 4, 8, 13, 26, 52]:
    if k in vr_emp and k < T_SIM:
        ck = sim_df.diff(k).iloc[k:]
        vs = (ck.var() / (k * sim_v1)).median()
        err = abs(vs - vr_emp[k]) / vr_emp[k] * 100
        results[f'vr{k}'] = vs
        ok = "Y" if err < 20 else "N"
        print(f"  VR({k:2d}): emp={vr_emp[k]:.4f} sim={vs:.4f} err={err:.1f}% [{ok}]")

print("\n--- ACF of changes ---")
for lag in [1, 2, 3, 4]:
    if lag in acf_emp:
        cors = [sim_ch[i].dropna().autocorr(lag) for i in range(min(1000, N_BP))
                if len(sim_ch[i].dropna()) > lag + 5]
        a = np.nanmedian(cors); results[f'acf{lag}'] = a
        err = abs(a - acf_emp[lag])
        ok = "Y" if err < 0.08 else "N"
        print(f"  ACF({lag}): emp={acf_emp[lag]:.4f} sim={a:.4f} err={err:.4f} [{ok}]")

print("\n--- Rank ACF ---")
sim_rk_df = pd.DataFrame(bp_rk)
for lag in [1, 4, 13, 26]:
    if lag in racf_emp:
        cors = [sim_rk_df[i].dropna().autocorr(lag) for i in range(min(1000, N_BP))
                if len(sim_rk_df[i].dropna()) > lag + 5]
        r = np.nanmedian(cors); results[f'racf{lag}'] = r
        err = abs(r - racf_emp[lag])
        ok = "Y" if err < 0.08 else "N"
        print(f"  RACF({lag:2d}): emp={racf_emp[lag]:.4f} sim={r:.4f} err={err:.4f} [{ok}]")

print("\n--- Top-100 Persistence ---")
for k in [1, 4, 13, 26, 52]:
    if k in pers_emp and k < T_SIM:
        t0s = set(np.where(sim_rk[0] <= 100)[0])
        tks = set(np.where(sim_rk[k] <= 100)[0])
        p = len(t0s & tks); results[f'pers{k}'] = p
        d = p - pers_emp[k]
        ok = "Y" if abs(d) < 10 else "N"
        print(f"  k={k:2d}: emp={pers_emp[k]} sim={p} diff={d:+d} [{ok}]")

print("\n--- Cross-Sectional R² ---")
for k in [1, 4, 13, 26, 52]:
    if k in xr2_emp and k < T_SIM:
        r2 = np.corrcoef(bp_ly[0], bp_ly[k])[0,1] ** 2
        results[f'xr2_{k}'] = r2
        err = abs(r2 - xr2_emp[k])
        ok = "Y" if err < 0.08 else "N"
        print(f"  R²({k:2d}): emp={xr2_emp[k]:.4f} sim={r2:.4f} err={err:.4f} [{ok}]")

print(f"\n  [Diagnostic: R² from true levels]")
for k in [1, 4, 13]:
    r2t = np.corrcoef(bp_ly_true[0], bp_ly_true[k])[0,1] ** 2
    r2o = results.get(f'xr2_{k}', 0)
    print(f"    R²_true({k:2d})={r2t:.4f}  R²_obs({k:2d})={r2o:.4f}  emp={xr2_emp[k]:.4f}")

# Additional diagnostics
sim_y0 = np.exp(sim_ly[0]); ss = np.sort(sim_y0)[::-1]
mask = (np.arange(1, N_FULL+1) <= 5000) & (ss > 0)
sim_zs = np.polyfit(np.log(np.arange(1, N_FULL+1)[mask]), np.log(ss[mask]), 1)[0]
sim_ch_flat = sim_ch.values.flatten(); sim_ch_flat = sim_ch_flat[np.isfinite(sim_ch_flat)]
sim_kurt = sp_stats.kurtosis(sim_ch_flat, fisher=True)
ks_stat, _ = sp_stats.ks_2samp(
    np.random.choice(all_ch_emp, min(50000, len(all_ch_emp)), replace=False),
    np.random.choice(sim_ch_flat, min(50000, len(sim_ch_flat)), replace=False))

print(f"\n  Zipf: emp={zipf_slope:.4f} sim={sim_zs:.4f}")
print(f"  Kurtosis: emp={emp_kurt:.1f} sim={sim_kurt:.1f}")
print(f"  KS: {ks_stat:.4f}")

# Band diagnostics
print("\n--- Band-Level Diagnostics ---")
bp_avg_rk_global = pd.DataFrame(bp_rk).mean()
for (lo, hi), st in band_stats.items():
    bm = (bp_avg_rk_global >= lo) & (bp_avg_rk_global <= hi)
    if bm.sum() > 5:
        bch = sim_ch[bm.index[bm]]; bdf = sim_df[bm.index[bm]]
        bv = bch.var().median()
        bvr4 = (bdf.diff(4).iloc[4:].var()/(4*bch.var())).median()
        bacfs = [bch[i].dropna().autocorr(1) for i in bm.index[bm][:500] if len(bch[i].dropna())>6]
        bacf1 = np.nanmedian(bacfs) if bacfs else 0
        print(f"  {lo:5d}-{hi:5d}: n={bm.sum():5d}  var={bv:.4f}(emp {st['var']:.4f})  "
              f"VR4={bvr4:.4f}(emp {st['vr4']:.4f})  ACF1={bacf1:.4f}(emp {st['acf1']:.4f})")

# ============================================================
# SUMMARY
# ============================================================
tests = {
    'VR(2)': abs(results.get('vr2',0)-vr_emp[2])/vr_emp[2] < 0.20,
    'VR(4)': abs(results.get('vr4',0)-vr_emp[4])/vr_emp[4] < 0.20,
    'VR(8)': abs(results.get('vr8',0)-vr_emp[8])/vr_emp[8] < 0.20,
    'VR(13)': abs(results.get('vr13',0)-vr_emp[13])/vr_emp[13] < 0.20,
    'ACF(1)': abs(results.get('acf1',0)-acf_emp[1]) < 0.08,
    'ACF(2)': abs(results.get('acf2',0)-acf_emp[2]) < 0.08,
    'RACF(1)': abs(results.get('racf1',0)-racf_emp[1]) < 0.08,
    'RACF(4)': abs(results.get('racf4',0)-racf_emp[4]) < 0.08,
    'RACF(13)': abs(results.get('racf13',0)-racf_emp[13]) < 0.08,
    'Pers(1)': abs(results.get('pers1',0)-pers_emp[1]) < 10,
    'Pers(4)': abs(results.get('pers4',0)-pers_emp[4]) < 10,
    'Pers(13)': abs(results.get('pers13',0)-pers_emp[13]) < 10,
    'R²(1)': abs(results.get('xr2_1',0)-xr2_emp[1]) < 0.08,
    'R²(4)': abs(results.get('xr2_4',0)-xr2_emp[4]) < 0.08,
    'R²(13)': abs(results.get('xr2_13',0)-xr2_emp[13]) < 0.08,
}
n_pass = sum(tests.values())
elapsed = time.time() - t_start

print(f"\n{'='*70}")
print(f"SUMMARY v3.0")
print(f"{'='*70}")
print(f"\n  Diagnostics: {n_pass}/{len(tests)}")
for name, passed in tests.items():
    print(f"    {name}: {'PASS' if passed else 'FAIL'}")
print(f"\n  All parameters estimated from data:")
print(f"    σ_obs={sigma_obs:.4f} σ_het={sigma_het:.4f} t_df={t_df:.2f}")
print(f"    κ={kappa:.6f}(HL={np.log(2)/kappa:.0f}wk) pb={perm_boost}")
print(f"    jump_p={jump_prob:.4f} jump_s={jump_scale:.2f}")
print(f"  Elapsed: {elapsed:.0f}s")

# ============================================================
# PLOTS
# ============================================================
print("\nGenerating plots...")
fig = plt.figure(figsize=(22, 24))
gs = GridSpec(4, 3, figure=fig, hspace=0.35, wspace=0.30)
fig.suptitle(f'Rank Diffusion v3.0 | {n_pass}/{len(tests)} | σ_obs={sigma_obs:.3f} κ={kappa:.5f} t_df={t_df:.1f} (all estimated)',
             fontsize=12, fontweight='bold', y=0.995)

ax = fig.add_subplot(gs[0,0])
vr_ks = sorted(vr_emp.keys())
ax.plot(vr_ks, [vr_emp[k] for k in vr_ks], 'ko-', label='Emp', ms=5, lw=2)
svrs = [(sim_df.diff(k).iloc[k:].var()/(k*sim_v1)).median() for k in vr_ks]
ax.plot(vr_ks, svrs, 'rs--', label='Sim', ms=5, lw=2)
ax.set_xlabel('Horizon'); ax.set_ylabel('VR'); ax.set_title('Variance Ratio'); ax.legend(); ax.grid(True, alpha=0.3)

ax = fig.add_subplot(gs[0,1])
lags=[1,2,3,4]; x=np.arange(len(lags))
ax.bar(x-0.15,[acf_emp.get(l,0) for l in lags],0.3,label='Emp',color='black',alpha=0.7)
sa=[np.nanmedian([sim_ch[i].dropna().autocorr(l) for i in range(min(500,N_BP)) if len(sim_ch[i].dropna())>l+5]) for l in lags]
ax.bar(x+0.15,sa,0.3,label='Sim',color='red',alpha=0.7)
ax.axhline(0,color='gray',lw=0.5); ax.set_xticks(x); ax.set_xticklabels(lags)
ax.set_title('ACF of Changes'); ax.legend(); ax.grid(True, alpha=0.3)

ax = fig.add_subplot(gs[0,2])
rl=[1,4,13,26]; x=np.arange(len(rl))
ax.bar(x-0.15,[racf_emp.get(l,0) for l in rl],0.3,label='Emp',color='black',alpha=0.7)
ax.bar(x+0.15,[results.get(f'racf{l}',0) for l in rl],0.3,label='Sim',color='red',alpha=0.7)
ax.set_xticks(x); ax.set_xticklabels(rl)
ax.set_title('Rank ACF'); ax.legend(); ax.grid(True, alpha=0.3)

ax = fig.add_subplot(gs[1,0])
r2k=[1,4,13,26,52]
ax.plot(r2k,[xr2_emp.get(k,0) for k in r2k],'ko-',label='Emp',ms=5,lw=2)
ax.plot(r2k,[results.get(f'xr2_{k}',0) for k in r2k],'rs--',label='Sim',ms=5,lw=2)
ax.set_title('Cross-Sectional R²'); ax.legend(); ax.grid(True, alpha=0.3)

ax = fig.add_subplot(gs[1,1])
pk=[1,4,13,26,52]
ax.plot(pk,[pers_emp.get(k,0) for k in pk],'ko-',label='Emp',ms=5,lw=2)
ax.plot(pk,[results.get(f'pers{k}',0) for k in pk],'rs--',label='Sim',ms=5,lw=2)
ax.set_title('Top-100 Persistence'); ax.legend(); ax.grid(True, alpha=0.3)

ax = fig.add_subplot(gs[1,2])
bins=np.linspace(-3,3,120)
ax.hist(np.clip(all_ch_emp,-3,3),bins,density=True,alpha=0.5,color='black',label='Emp')
ax.hist(np.clip(sim_ch_flat,-3,3),bins,density=True,alpha=0.5,color='red',label='Sim')
ax.set_title(f'Changes (KS={ks_stat:.3f}, kurt={sim_kurt:.1f}/{emp_kurt:.1f})'); ax.legend(); ax.grid(True,alpha=0.3)

ax = fig.add_subplot(gs[2,0])
# xsec_vars includes burn-in; plot only post-burn-in portion
post_burnin_xsec = xsec_vars[T_BURNIN:]
ax.plot(range(T_SIM), post_burnin_xsec[:T_SIM], 'r-', label='Sim τ', lw=2)
sim_bp_xsec = [np.var(bp_ly[t]) for t in range(T_SIM)]
emp_xsec_ts = log_metric.var(axis=1).values
ax.plot(range(T_SIM), emp_xsec_ts, 'k-', label='Emp BP', lw=2)
ax.plot(range(T_SIM), sim_bp_xsec, 'r--', label='Sim BP', lw=2)
ax.set_title('Cross-sec Variance Over Time'); ax.legend(); ax.grid(True, alpha=0.3)

ax = fig.add_subplot(gs[2,1])
bc_mids = [np.sqrt(lo*hi) for lo,hi in bands]
ev = [band_stats[(lo,hi)]['var'] for lo,hi in bands]
sv_b = []
for lo,hi in bands:
    bm = (bp_avg_rk_global >= lo) & (bp_avg_rk_global <= hi)
    sv_b.append(sim_ch[bm.index[bm]].var().median() if bm.sum()>5 else 0)
ax.plot(bc_mids, ev, 'ko-', label='Emp', ms=5)
ax.plot(bc_mids, sv_b, 'rs--', label='Sim', ms=5)
ax.set_xscale('log'); ax.set_title('Band Variance'); ax.legend(); ax.grid(True, alpha=0.3)

ax = fig.add_subplot(gs[2,2])
ev4 = [band_stats[(lo,hi)]['vr4'] for lo,hi in bands]
sv4 = []
for lo,hi in bands:
    bm = (bp_avg_rk_global >= lo) & (bp_avg_rk_global <= hi)
    if bm.sum()>5:
        bch=sim_ch[bm.index[bm]]; bdf=sim_df[bm.index[bm]]
        sv4.append((bdf.diff(4).iloc[4:].var()/(4*bch.var())).median())
    else: sv4.append(0)
ax.plot(bc_mids, ev4, 'ko-', label='Emp', ms=5)
ax.plot(bc_mids, sv4, 'rs--', label='Sim', ms=5)
ax.set_xscale('log'); ax.set_title('Band VR(4)'); ax.legend(); ax.grid(True, alpha=0.3)

# Sample trajectories
for j, (idx, lbl) in enumerate([(0,'Top'), (N_BP//2,'Mid'), (N_BP-1,'Bottom')]):
    ax = fig.add_subplot(gs[3, j])
    ax.plot(range(T_SIM), bp_rk[:, idx], 'r-', alpha=0.7, lw=1)
    ax.set_xlabel('Week'); ax.set_ylabel('Rank')
    ax.set_title(f'Rank trajectory: {lbl}'); ax.grid(True, alpha=0.3)
    ax.invert_yaxis()

plt.savefig('/Users/hindman/Documents/github/rank-diffusion/llm_fitting/v30_diagnostics.png', dpi=130, bbox_inches='tight')
print("Saved v30_diagnostics.png")
print("Done.")
