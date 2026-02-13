#!/usr/bin/env python3
"""
Permanent-Transitory Rank Diffusion Model v3.7 — Rank-Dependent Tail Shape
================================================================================
Changes from v3.6 (15/15, ARCH(1) + publication diagnostics):

v3.7 addresses the next biggest gap: band-level kurtosis heterogeneity.
v3.6 uses a single t_df=4.97 for all ranks, but empirical data shows:
  - Top band (1-100):   emp_kurt=1.77 (near-Gaussian, light tails)
  - Mid bands (101-5K): emp_kurt=6.5-7.4 (heavy tails)
  - Bottom (5K+):       emp_kurt=6.45 (heavy tails)
The model produced sim_kurt=2.49 (top) and ~4.5-5.3 (mid) — wrong in both
directions.

Key change: Rank-dependent t_df
  - Estimate t_df per band from within-endpoint standardized residual MLE
  - Interpolate t_df(rank) via log-rank, same scheme as (σ_η, φ, σ_ν)
  - Vectorized simulation: t(df_i) = Z_i / sqrt(chi2(df_i)/df_i) with per-element df
  - Variance normalization preserved: scale by sqrt((df_i-2)/df_i) per element

Why this is safe for calibration diagnostics:
  - t_df affects innovation *shape*, not *variance* (normalized to unit variance)
  - VR, ACF, RACF, R², Pers depend on variance/covariance structure -> unchanged
  - ARCH interaction: higher df for top ranks -> less extreme z2 -> less ARCH
    amplification for top ranks (desired: top ranks are already too kurtotic)

All v3.6 features retained: ARCH(1), publication diagnostics.
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
from matplotlib.colors import LogNorm

t_start = time.time()

# ============================================================
# DATA LOADING (same as v3.4)
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

print(f"  Change var: median={emp_median_var:.4f}, mean={emp_mean_var:.4f}")
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
# ESTIMATION STAGES 1-5 (same as v3.4)
# ============================================================
print("\n" + "=" * 70)
print("STAGE 1: ESTIMATE σ_obs FROM ACF STRUCTURE")
print("=" * 70)

phi_agg = acf_emp[3] / acf_emp[2]
gamma1 = acf_emp[1] * emp_median_var
gamma2 = acf_emp[2] * emp_median_var

sigma2_obs_est = -gamma1 + gamma2 / phi_agg
sigma_obs = np.sqrt(np.clip(sigma2_obs_est, 0.01**2, 0.50**2))
sobs2 = sigma_obs ** 2
print(f"  σ_obs = {sigma_obs:.4f}")

print("\n" + "=" * 70)
print("STAGE 2: ESTIMATE σ_het")
print("=" * 70)

var_ratio = emp_mean_var / emp_median_var
sigma_het = np.sqrt(np.log(var_ratio) / 2)
E_h2 = np.exp(2 * sigma_het ** 2)
print(f"  σ_het = {sigma_het:.4f}, E[h²] = {E_h2:.4f}")

print("\n" + "=" * 70)
print("STAGE 3: ESTIMATE BAND-LEVEL t_df (within-endpoint MLE per band)")
print("=" * 70)

# v3.7: Estimate t_df per band to capture kurtosis heterogeneity
# Top-ranked endpoints have lighter tails (near-Gaussian), mid/bottom heavier
standardized_residuals = []
for ep in sample_eps:
    ch = log_changes[ep].dropna().values
    if len(ch) > 10:
        mu_ep = np.mean(ch); std_ep = np.std(ch, ddof=1)
        if std_ep > 1e-6:
            standardized_residuals.append((ch - mu_ep) / std_ep)

z_within = np.concatenate(standardized_residuals)
df_fit, loc_fit, scale_fit = sp_stats.t.fit(z_within)
t_df_global = max(3.0, df_fit)
print(f"  Global MLE: df={df_fit:.2f} → t_df_global = {t_df_global:.2f}")

# Per-band t_df estimation with observation-noise correction
# The MLE on within-endpoint standardized residuals is biased toward higher df
# (lighter tails) when observation noise dominates, because σ_obs adds a Gaussian
# component. The correction: estimate the signal fraction and inflate the MLE df
# for bands where observation noise dominates.
obs_noise_var = 2 * sobs2  # variance of Δε = ε_t - ε_{t-1}

band_tdf = {}
band_tdf_raw = {}
for lo, hi in bands:
    beps = avg_rank[(avg_rank >= lo) & (avg_rank <= hi)].index
    beps_sample = [ep for ep in beps if ep in set(sample_eps)]
    band_std_resid = []
    for ep in beps_sample:
        ch = log_changes[ep].dropna().values
        if len(ch) > 10:
            mu_ep = np.mean(ch); std_ep = np.std(ch, ddof=1)
            if std_ep > 1e-6:
                band_std_resid.append((ch - mu_ep) / std_ep)
    if len(band_std_resid) > 5:
        z_band = np.concatenate(band_std_resid)
        df_band, _, _ = sp_stats.t.fit(z_band)
        df_band = max(3.0, df_band)
    else:
        df_band = t_df_global

    band_tdf_raw[(lo, hi)] = df_band

    # Observation noise correction: when obs noise dominates (signal_frac < 0.30),
    # the MLE underestimates the true df because Gaussian contamination makes
    # residuals look lighter-tailed than the true innovations. Combined with
    # high φ and ARCH, this creates kurtosis overshoot for obs-noise-dominated bands.
    # Correction: inflate df by 1/signal_frac (only for heavily noise-dominated bands).
    total_var = band_stats[(lo, hi)]['var']
    signal_frac = max(0.05, 1 - obs_noise_var / total_var)
    if signal_frac < 0.30:
        df_corrected = df_band / signal_frac
        df_corrected = min(df_corrected, 200.0)  # cap at near-Gaussian
    else:
        df_corrected = df_band

    band_tdf[(lo, hi)] = df_corrected
    print(f"  Band {lo:5d}-{hi:5d}: MLE_df={df_band:.2f}  signal_frac={signal_frac:.2f}  "
          f"→ t_df={df_corrected:.2f} (n_ep={len(beps_sample)})")

# Build interpolation arrays for t_df(rank)
tdf_arr = np.array([band_tdf[(lo, hi)] for lo, hi in bands])

def get_tdf(ranks):
    """Interpolate t_df as function of rank (log-rank scale)."""
    lr = np.log(np.clip(ranks.astype(float), 1, bc_arr[-1]*2))
    return np.interp(lr, np.log(bc_arr), tdf_arr)

# For backward compatibility (jump estimation, publication diagnostics)
t_df = t_df_global

print("\n" + "=" * 70)
print("STAGE 4: JUMP PARAMETERS")
print("=" * 70)

threshold = 4.0
expected_tail = 2 * sp_stats.t.sf(threshold, df=t_df, loc=0, scale=scale_fit)
actual_tail = np.mean(np.abs(z_within - loc_fit) > threshold * scale_fit)
jump_prob = max(0.005, actual_tail - expected_tail)

extreme_mask = np.abs(z_within) > threshold * scale_fit
jump_scale = np.std(z_within[extreme_mask]) / np.std(z_within[~extreme_mask]) if extreme_mask.sum() > 10 else 5.0
print(f"  jump_prob = {jump_prob:.4f}, jump_scale = {jump_scale:.2f}")

print("\n" + "=" * 70)
print("STAGE 4.5: ARCH COEFFICIENT FROM SQUARED-RESIDUAL AUTOCORRELATION")
print("=" * 70)

# Estimate ARCH(1) coefficient from within-endpoint squared standardized residuals.
# For each endpoint, compute z = (Δy - mean) / std, then ACF(z², 1).
# This measures how much a large shock predicts the next period's shock magnitude.
z_sq_acfs = []
for ep in sample_eps:
    ch = log_changes[ep].dropna().values
    if len(ch) > 15:
        mu_ep = np.mean(ch); std_ep = np.std(ch, ddof=1)
        if std_ep > 1e-6:
            z_ep = (ch - mu_ep) / std_ep
            z_sq = z_ep ** 2
            z_sq_dm = z_sq - np.mean(z_sq)
            var_z_sq = np.var(z_sq)
            if var_z_sq > 1e-10:
                acf_sq1 = np.sum(z_sq_dm[:-1] * z_sq_dm[1:]) / ((len(z_sq_dm) - 1) * var_z_sq)
                if np.isfinite(acf_sq1):
                    z_sq_acfs.append(acf_sq1)

alpha_arch_raw = np.median(z_sq_acfs)
alpha_arch = np.clip(alpha_arch_raw, 0.01, 0.50)
print(f"  Raw median ACF(z², 1) = {alpha_arch_raw:.4f}")
print(f"  α_arch = {alpha_arch:.4f}")
print(f"  Interpretation: after a 2σ shock, next-period transitory σ scales by "
      f"{np.sqrt((1-alpha_arch) + alpha_arch * 4):.3f}×")

print("\n" + "=" * 70)
print("STAGE 5: BAND-LEVEL STRUCTURAL ESTIMATION")
print("=" * 70)

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

np.random.seed(123)
band_params = {}
for (lo, hi), st in band_stats.items():
    se2, phi, sn2 = fit_params(st['var'], st['vr4'], st['acf1'], st['vr13'], sobs2)
    sc2 = sn2/(1-phi**2)
    mvar_s = se2 + 2*sc2*(1-phi)
    mvar_t = mvar_s + 2*sobs2
    band_params[(lo,hi)] = {'se':np.sqrt(se2),'phi':phi,'sn':np.sqrt(sn2),
                            'se2':se2,'sn2':sn2,'pf':se2/mvar_t}
    print(f"  {lo:5d}-{hi:5d}: se={np.sqrt(se2):.4f} phi={phi:.4f} "
          f"sn={np.sqrt(sn2):.4f} perm={se2/mvar_t*100:.1f}%")

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
print("STAGE 6: RANK-DEPENDENT κ CALIBRATION")
print("=" * 70)

N_FULL = int(mean_N)
alpha_kappa = 0.5

total_n = sum(st['n'] for st in band_stats.values())
mean_se2 = sum(band_params[(lo,hi)]['se2'] * band_stats[(lo,hi)]['n']
               for lo,hi in band_params) / total_n
jump_var_factor = (1 - jump_prob + jump_prob * jump_scale ** 2)
mean_eta2 = E_h2 * mean_se2 * jump_var_factor

w0_data = df[df['date'] == dates[0]].sort_values('rank')
w0_log = np.log1p(w0_data['metric_value'].values)
w0_sorted = np.sort(w0_log)[::-1]
N_w0 = len(w0_sorted)
if N_w0 < N_FULL:
    rf = np.arange(1, N_w0+1)
    sl, ic = np.polyfit(np.log(rf[-2000:]), w0_sorted[-2000:], 1)
    er = np.arange(N_w0+1, N_FULL+1)
    w0_sorted = np.concatenate([w0_sorted, ic + sl*np.log(er)])
else:
    w0_sorted = w0_sorted[:N_FULL]

init_mean = np.mean(w0_sorted)
init_dev2 = (w0_sorted - init_mean) ** 2
init_ranks = np.arange(1, N_FULL + 1)
rank_weight = (init_ranks / N_FULL) ** alpha_kappa

weighted_dev2 = np.mean(rank_weight * init_dev2)
kappa_base = mean_eta2 / (2 * weighted_dev2)
kappa_base = max(kappa_base, 0.001)

print(f"  α = {alpha_kappa}")
print(f"  κ_base = {kappa_base:.6f}")
for r_check in [1, 100, 1000, 5000, N_FULL]:
    k_r = kappa_base * (r_check / N_FULL) ** alpha_kappa
    hl = np.log(2) / k_r if k_r > 0 else float('inf')
    print(f"    Rank {r_check:5d}: κ={k_r:.6f} (HL={hl:.0f}wk)")

# ============================================================
perm_boost = 1.0

print("\n" + "=" * 70)
print("PARAMETER SUMMARY (v3.7: v3.6 engine + rank-dependent t_df)")
print("=" * 70)
print(f"  σ_obs     = {sigma_obs:.4f}")
print(f"  σ_het     = {sigma_het:.4f}")
print(f"  t_df      = {t_df:.2f} (global; band-level t_df used in simulation)")
for (lo, hi), tdf_val in band_tdf.items():
    print(f"    Band {lo:5d}-{hi:5d}: t_df = {tdf_val:.2f}")
print(f"  κ_base    = {kappa_base:.6f} (rank-dep global-mean, α={alpha_kappa})")
print(f"  jump_prob = {jump_prob:.4f}")
print(f"  jump_scale= {jump_scale:.2f}")
print(f"  α_arch    = {alpha_arch:.4f} (ARCH(1) on transitory innovation)")

inc_alpha = 0.3
p_exit_incumbent = 0.0040
inc_p_base = p_exit_incumbent * (inc_alpha + 1)
trans_p_exit = 0.07

# ============================================================
# SIMULATION (v3.6 engine + rank-dependent t_df)
# ============================================================
T_SIM = n_weeks
T_BURNIN = 50
T_TOTAL = T_BURNIN + T_SIM
N_REP = 25

print(f"\n{'='*70}")
print(f"SIMULATION v3.7 — {N_REP} MC REPS — ARCH(1) + rank-dep t_df")
print(f"{'='*70}")
print(f"  N={N_FULL}, T_record={T_SIM}, T_burnin={T_BURNIN}, T_total={T_TOTAL}")


def run_simulation(seed):
    """Run one replication and return diagnostics dict + optional extra data."""
    rng = np.random.RandomState(seed)

    tau = w0_sorted.copy()
    c_state = np.zeros(N_FULL)

    het_multiplier = np.exp(rng.normal(0, sigma_het, N_FULL))
    het_multiplier = np.clip(het_multiplier, 0.15, 8.0)

    ep_type = np.zeros(N_FULL, dtype=int)
    endpoint_id = np.arange(N_FULL)
    next_id = N_FULL

    sim_ly = np.zeros((T_SIM, N_FULL))
    sim_ly_true = np.zeros((T_SIM, N_FULL))
    sim_rk = np.zeros((T_SIM, N_FULL), dtype=int)
    sim_ids = np.zeros((T_SIM, N_FULL), dtype=int)

    obs_noise = rng.normal(0, sigma_obs, N_FULL)
    y0_obs = tau + c_state + obs_noise
    order = np.argsort(-np.exp(y0_obs))
    ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)

    total_exits = 0
    xsec_vars = [np.var(tau)]

    # ARCH(1) state: normalized squared transitory innovation z² = ν² / E[ν²]
    # Initialized at unconditional mean (z² = 1)
    last_z_sq = np.ones(N_FULL)

    for t_abs in range(1, T_TOTAL):
        cr = ranks
        se, phi_v, sn = get_p(cr)

        se_het = se * het_multiplier * perm_boost
        sn_het = sn * het_multiplier

        is_jump = rng.random(N_FULL) < jump_prob
        eta = np.where(is_jump,
                       rng.normal(0, se_het * jump_scale),
                       rng.normal(0, se_het))

        # v3.6: ARCH(1) scaling on transitory innovation
        # arch_var = (1-α) + α × z²_{t-1}, where z² = ν²/E[ν²]
        # E[arch_var] = 1, so unconditional variance is preserved
        arch_var = (1 - alpha_arch) + alpha_arch * last_z_sq
        arch_scale = np.sqrt(np.clip(arch_var, 0.1, 10.0))

        # v3.7: Rank-dependent t_df — scipy vectorized t.rvs with per-element df
        df_vec = get_tdf(cr)
        t_raw = sp_stats.t.rvs(df=df_vec, random_state=rng)
        t_var_factor = np.sqrt(np.maximum(df_vec - 2, 0.5) / df_vec)
        nu = sn_het * t_var_factor * arch_scale * t_raw
        c_state = phi_v * c_state + nu

        # Update ARCH state: z² = ν² / unconditional E[ν²]
        # E[ν²] = sn_het² (by construction of t_var_factor without arch_scale)
        # Clip z² at 4.0 to prevent extreme events from creating runaway
        # amplification. Most vol clustering comes from moderate z² (1-4);
        # clipping limits kurtosis overshoot while preserving the effect.
        last_z_sq = np.clip(nu ** 2 / (sn_het ** 2 + 1e-10), 0, 4.0)

        # v3.4: RANK-DEPENDENT global mean reversion
        current_mean = np.mean(tau)
        kappa_r = kappa_base * (cr / N_FULL) ** alpha_kappa
        tau += eta - kappa_r * (tau - current_mean)

        # Exit/entry — ONLY during recording period
        t_rec = t_abs - T_BURNIN
        if t_rec >= 0:
            nr = cr / N_FULL
            p_exit = np.where(ep_type == 0,
                              inc_p_base * (nr ** inc_alpha),
                              trans_p_exit)
            exit_mask = rng.random(N_FULL) < p_exit
            n_ex = exit_mask.sum()
            total_exits += n_ex

            if n_ex > 0:
                exi = np.where(exit_mask)[0]
                n_burst = max(1, int(n_ex * 0.008))
                n_norm = n_ex - n_burst
                bq = np.percentile(tau[~exit_mask], 10)
                bstd = np.std(tau[tau < np.median(tau)]) * 0.4
                new_tau = rng.normal(bq, bstd, n_norm)
                if n_burst > 0:
                    buq = np.percentile(tau[~exit_mask], 90)
                    bust = np.std(tau) * 0.25
                    new_tau = np.concatenate([new_tau, rng.normal(buq, bust, n_burst)])
                tau[exi] = new_tau
                c_state[exi] = sp_stats.t.rvs(df=t_df, size=n_ex, random_state=rng) * 0.3
                het_multiplier[exi] = np.clip(np.exp(rng.normal(0, sigma_het, n_ex)), 0.15, 8.0)
                last_z_sq[exi] = 1.0  # Reset ARCH state for new entrants
                ep_type[exi] = 1
                endpoint_id[exi] = np.arange(next_id, next_id+n_ex)
                next_id += n_ex

        log_y_true = tau + c_state
        obs_noise = rng.normal(0, sigma_obs, N_FULL)
        log_y_obs = log_y_true + obs_noise

        order = np.argsort(-np.exp(log_y_obs))
        ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)

        xsec_vars.append(np.var(tau))

        if t_rec == 0:
            sim_ly[0] = log_y_obs; sim_ly_true[0] = log_y_true
            sim_rk[0] = ranks; sim_ids[0] = endpoint_id.copy()
        elif 0 < t_rec < T_SIM:
            sim_ly[t_rec] = log_y_obs; sim_ly_true[t_rec] = log_y_true
            sim_rk[t_rec] = ranks; sim_ids[t_rec] = endpoint_id.copy()

    # Balanced panel
    init_ids = set(sim_ids[0])
    survivors = init_ids.copy()
    for t in range(1, T_SIM):
        survivors &= set(sim_ids[t])
    N_BP = len(survivors)

    survivor_list = sorted(survivors)
    bp_ly = np.zeros((T_SIM, N_BP))
    bp_ly_true = np.zeros((T_SIM, N_BP))
    bp_rk = np.zeros((T_SIM, N_BP), dtype=int)
    for t in range(T_SIM):
        id_map = {eid: idx for idx, eid in enumerate(sim_ids[t])}
        for j, sid in enumerate(survivor_list):
            bp_ly[t,j] = sim_ly[t, id_map[sid]]
            bp_ly_true[t,j] = sim_ly_true[t, id_map[sid]]
            bp_rk[t,j] = sim_rk[t, id_map[sid]]

    # Diagnostics
    sim_df = pd.DataFrame(bp_ly)
    sim_ch = sim_df.diff().iloc[1:]
    sim_v1 = sim_ch.var()
    diag = {}

    for k in [2, 4, 8, 13]:
        if k < T_SIM:
            diag[f'vr{k}'] = (sim_df.diff(k).iloc[k:].var() / (k * sim_v1)).median()

    for lag in [1, 2]:
        cors = [sim_ch[i].dropna().autocorr(lag) for i in range(min(1000, N_BP))
                if len(sim_ch[i].dropna()) > lag + 5]
        diag[f'acf{lag}'] = np.nanmedian(cors)

    sim_rk_df = pd.DataFrame(bp_rk)
    for lag in [1, 4, 13]:
        cors = [sim_rk_df[i].dropna().autocorr(lag) for i in range(min(1000, N_BP))
                if len(sim_rk_df[i].dropna()) > lag + 5]
        diag[f'racf{lag}'] = np.nanmedian(cors)

    for k in [1, 4, 13, 26, 52]:
        if k < T_SIM:
            t0s = set(np.where(sim_rk[0] <= 100)[0])
            tks = set(np.where(sim_rk[k] <= 100)[0])
            diag[f'pers{k}'] = len(t0s & tks)

    for k in [1, 4, 13, 26, 52]:
        if k < T_SIM:
            diag[f'xr2_{k}'] = np.corrcoef(bp_ly[0], bp_ly[k])[0,1] ** 2

    sim_ch_flat = sim_ch.values.flatten()
    sim_ch_flat = sim_ch_flat[np.isfinite(sim_ch_flat)]
    diag['kurtosis'] = sp_stats.kurtosis(sim_ch_flat, fisher=True)
    ks_stat, _ = sp_stats.ks_2samp(
        rng.choice(all_ch_emp, min(50000, len(all_ch_emp)), replace=False),
        rng.choice(sim_ch_flat, min(50000, len(sim_ch_flat)), replace=False))
    diag['ks'] = ks_stat

    sim_y0 = np.exp(sim_ly[0]); ss = np.sort(sim_y0)[::-1]
    mask = (np.arange(1, N_FULL+1) <= 5000) & (ss > 0)
    diag['zipf_slope'] = np.polyfit(np.log(np.arange(1, N_FULL+1)[mask]), np.log(ss[mask]), 1)[0]

    diag['survivor_pct'] = N_BP / N_FULL * 100
    diag['xsec_var_start'] = xsec_vars[T_BURNIN]
    diag['xsec_var_end'] = xsec_vars[-1]
    diag['avg_exits'] = total_exits / T_SIM

    extra = None
    if seed == 42:
        extra = {
            'sim_ly': sim_ly, 'sim_rk': sim_rk, 'bp_ly': bp_ly,
            'bp_ly_true': bp_ly_true, 'bp_rk': bp_rk,
            'sim_ch': sim_ch, 'sim_df': sim_df, 'sim_v1': sim_v1,
            'sim_ch_flat': sim_ch_flat, 'xsec_vars': xsec_vars,
            'N_BP': N_BP, 'bp_avg_rk_global': pd.DataFrame(bp_rk).mean(),
            'sim_rk_full': sim_rk,  # full-panel ranks for transition matrix
            'sim_ly_full': sim_ly,  # full-panel log-values for CDC
        }
    return diag, extra


# Run replications
print(f"\nRunning {N_REP} replications...")
all_diags = []
rep_extra = None
seeds = [42] + list(range(100, 100 + N_REP - 1))

for i, seed in enumerate(seeds):
    t_rep = time.time()
    diag, extra = run_simulation(seed)
    all_diags.append(diag)
    if extra is not None:
        rep_extra = extra
    if i == 0 or (i+1) % 5 == 0:
        print(f"  Rep {i+1}/{N_REP} (seed={seed}): {time.time()-t_rep:.1f}s")

print(f"  Total MC time: {time.time() - t_start:.0f}s")

# Aggregate
mc_stats = {}
for key in sorted(all_diags[0].keys()):
    vals = np.array([d[key] for d in all_diags])
    mc_stats[key] = {
        'mean': np.mean(vals), 'std': np.std(vals),
        'lo': np.percentile(vals, 2.5), 'hi': np.percentile(vals, 97.5),
        'median': np.median(vals),
    }

# ============================================================
# VALIDATION (same as v3.4)
# ============================================================
print(f"\n{'='*70}")
print(f"VALIDATION (mean ± 95% CI, {N_REP} replications)")
print(f"{'='*70}")

print("\n--- Variance Ratios ---")
for k in [2, 4, 8, 13]:
    s = mc_stats[f'vr{k}']
    err = abs(s['mean'] - vr_emp[k]) / vr_emp[k] * 100
    ok = "Y" if err < 20 else "N"
    print(f"  VR({k:2d}): emp={vr_emp[k]:.4f}  sim={s['mean']:.4f} [{s['lo']:.4f}, {s['hi']:.4f}]  err={err:.1f}% [{ok}]")

print("\n--- ACF of changes ---")
for lag in [1, 2]:
    s = mc_stats[f'acf{lag}']
    err = abs(s['mean'] - acf_emp[lag])
    ok = "Y" if err < 0.08 else "N"
    print(f"  ACF({lag}): emp={acf_emp[lag]:.4f}  sim={s['mean']:.4f} [{s['lo']:.4f}, {s['hi']:.4f}]  err={err:.4f} [{ok}]")

print("\n--- Rank ACF ---")
for lag in [1, 4, 13]:
    s = mc_stats[f'racf{lag}']
    err = abs(s['mean'] - racf_emp[lag])
    ok = "Y" if err < 0.08 else "N"
    print(f"  RACF({lag:2d}): emp={racf_emp[lag]:.4f}  sim={s['mean']:.4f} [{s['lo']:.4f}, {s['hi']:.4f}]  err={err:.4f} [{ok}]")

print("\n--- Top-100 Persistence ---")
for k in [1, 4, 13, 26, 52]:
    if f'pers{k}' in mc_stats:
        s = mc_stats[f'pers{k}']
        d = s['mean'] - pers_emp[k]
        ok = "Y" if abs(d) < 10 else "N"
        print(f"  k={k:2d}: emp={pers_emp[k]}  sim={s['mean']:.1f} [{s['lo']:.0f}, {s['hi']:.0f}]  diff={d:+.1f} [{ok}]")

print("\n--- Cross-Sectional R² ---")
for k in [1, 4, 13, 26, 52]:
    if f'xr2_{k}' in mc_stats:
        s = mc_stats[f'xr2_{k}']
        err = abs(s['mean'] - xr2_emp[k])
        ok = "Y" if err < 0.08 else "N"
        print(f"  R²({k:2d}): emp={xr2_emp[k]:.4f}  sim={s['mean']:.4f} [{s['lo']:.4f}, {s['hi']:.4f}]  err={err:.4f} [{ok}]")

print(f"\n--- Additional ---")
for key, label, emp_val in [
    ('kurtosis', 'Kurtosis', emp_kurt), ('ks', 'KS stat', None),
    ('zipf_slope', 'Zipf slope', zipf_slope),
    ('survivor_pct', 'Survivors %', N_balanced/mean_N*100),
]:
    s = mc_stats[key]
    if emp_val is not None:
        print(f"  {label}: emp={emp_val:.2f}  sim={s['mean']:.2f} [{s['lo']:.2f}, {s['hi']:.2f}]")
    else:
        print(f"  {label}: sim={s['mean']:.3f} [{s['lo']:.3f}, {s['hi']:.3f}]")

s_start = mc_stats['xsec_var_start']
s_end = mc_stats['xsec_var_end']
print(f"  Cross-sec var: start={s_start['mean']:.2f}  end={s_end['mean']:.2f}  (emp={xsec_var_full:.2f})")

# Summary
tests = {
    'VR(2)': abs(mc_stats['vr2']['mean']-vr_emp[2])/vr_emp[2] < 0.20,
    'VR(4)': abs(mc_stats['vr4']['mean']-vr_emp[4])/vr_emp[4] < 0.20,
    'VR(8)': abs(mc_stats['vr8']['mean']-vr_emp[8])/vr_emp[8] < 0.20,
    'VR(13)': abs(mc_stats['vr13']['mean']-vr_emp[13])/vr_emp[13] < 0.20,
    'ACF(1)': abs(mc_stats['acf1']['mean']-acf_emp[1]) < 0.08,
    'ACF(2)': abs(mc_stats['acf2']['mean']-acf_emp[2]) < 0.08,
    'RACF(1)': abs(mc_stats['racf1']['mean']-racf_emp[1]) < 0.08,
    'RACF(4)': abs(mc_stats['racf4']['mean']-racf_emp[4]) < 0.08,
    'RACF(13)': abs(mc_stats['racf13']['mean']-racf_emp[13]) < 0.08,
    'Pers(1)': abs(mc_stats['pers1']['mean']-pers_emp[1]) < 10,
    'Pers(4)': abs(mc_stats['pers4']['mean']-pers_emp[4]) < 10,
    'Pers(13)': abs(mc_stats['pers13']['mean']-pers_emp[13]) < 10,
    'R²(1)': abs(mc_stats['xr2_1']['mean']-xr2_emp[1]) < 0.08,
    'R²(4)': abs(mc_stats['xr2_4']['mean']-xr2_emp[4]) < 0.08,
    'R²(13)': abs(mc_stats['xr2_13']['mean']-xr2_emp[13]) < 0.08,
}
n_pass = sum(tests.values())
elapsed = time.time() - t_start

print(f"\n{'='*70}")
print(f"SUMMARY v3.7 ({N_REP} replications)")
print(f"{'='*70}")
print(f"\n  Diagnostics: {n_pass}/{len(tests)}")
for name, passed in tests.items():
    print(f"    {name}: {'PASS' if passed else 'FAIL'}")

# ============================================================
# PUBLICATION DIAGNOSTICS — NEW IN v3.5
# ============================================================
print(f"\n{'='*70}")
print("PUBLICATION DIAGNOSTICS (v3.7)")
print(f"{'='*70}")

if rep_extra is not None:
    sim_ly = rep_extra['sim_ly']; sim_rk = rep_extra['sim_rk']
    bp_ly = rep_extra['bp_ly']; bp_ly_true = rep_extra['bp_ly_true']
    bp_rk = rep_extra['bp_rk']; sim_ch = rep_extra['sim_ch']
    sim_df_plot = rep_extra['sim_df']; sim_v1 = rep_extra['sim_v1']
    sim_ch_flat = rep_extra['sim_ch_flat']; xsec_vars = rep_extra['xsec_vars']
    N_BP = rep_extra['N_BP']; bp_avg_rk_global = rep_extra['bp_avg_rk_global']
    sim_rk_full = rep_extra['sim_rk_full']
    sim_ly_full = rep_extra['sim_ly_full']

    # ------------------------------------------------------------------
    # 1. FORMAL STATISTICAL TESTS
    # ------------------------------------------------------------------
    print("\n--- Formal Statistical Tests ---")

    # 1a. Anderson-Darling test (tail-sensitive GoF)
    # Compare simulated changes vs empirical distribution
    sim_subsample = np.random.choice(sim_ch_flat, min(5000, len(sim_ch_flat)), replace=False)
    emp_subsample = np.random.choice(all_ch_emp, min(5000, len(all_ch_emp)), replace=False)
    ad_stat, ad_crit, ad_sig = sp_stats.anderson_ksamp([emp_subsample, sim_subsample])
    print(f"  Anderson-Darling (2-sample): stat={ad_stat:.3f}, p={ad_sig:.4f}")
    print(f"    Interpretation: {'Cannot reject same distribution' if ad_sig > 0.05 else 'Distributions differ significantly'} at α=0.05")

    # 1b. Jarque-Bera test on within-endpoint standardized residuals
    # Tests for normality of the standardized innovations
    sim_std_resid = []
    for i in range(min(500, N_BP)):
        ch = sim_ch[i].dropna().values
        if len(ch) > 10:
            mu_i = np.mean(ch); std_i = np.std(ch, ddof=1)
            if std_i > 1e-6:
                sim_std_resid.append((ch - mu_i) / std_i)
    sim_std_resid_flat = np.concatenate(sim_std_resid)
    jb_stat, jb_pval = sp_stats.jarque_bera(sim_std_resid_flat)
    sim_resid_skew = sp_stats.skew(sim_std_resid_flat)
    sim_resid_kurt = sp_stats.kurtosis(sim_std_resid_flat, fisher=True)
    print(f"\n  Jarque-Bera (sim standardized residuals): stat={jb_stat:.1f}, p={jb_pval:.2e}")
    print(f"    Skewness={sim_resid_skew:.3f}, Excess kurtosis={sim_resid_kurt:.2f}")
    print(f"    (Rejection expected — innovations are t-distributed, not Gaussian)")

    # Same for empirical
    emp_resid_skew = sp_stats.skew(z_within)
    emp_resid_kurt = sp_stats.kurtosis(z_within, fisher=True)
    jb_stat_emp, jb_pval_emp = sp_stats.jarque_bera(z_within)
    print(f"  Jarque-Bera (emp standardized residuals): stat={jb_stat_emp:.1f}, p={jb_pval_emp:.2e}")
    print(f"    Skewness={emp_resid_skew:.3f}, Excess kurtosis={emp_resid_kurt:.2f}")

    # 1c. Ljung-Box test for serial correlation in residuals
    print(f"\n  Ljung-Box test (residual serial correlation):")
    lb_lags_test = [5, 10, 20]
    # Test a representative sample of endpoints
    lb_reject_pct = {}
    for lag_test in lb_lags_test:
        n_reject = 0
        n_tested = 0
        for i in range(min(200, N_BP)):
            ch = sim_ch[i].dropna().values
            if len(ch) > lag_test + 5:
                # Manual Ljung-Box computation
                n_obs = len(ch)
                acf_vals = [np.corrcoef(ch[:-k], ch[k:])[0,1] for k in range(1, lag_test+1)]
                q_stat = n_obs * (n_obs + 2) * sum(a**2 / (n_obs - k - 1)
                                                     for k, a in enumerate(acf_vals))
                p_val = 1 - sp_stats.chi2.cdf(q_stat, lag_test)
                n_tested += 1
                if p_val < 0.05:
                    n_reject += 1
        if n_tested > 0:
            lb_reject_pct[lag_test] = n_reject / n_tested * 100
            print(f"    Lag {lag_test:2d}: {n_reject}/{n_tested} ({lb_reject_pct[lag_test]:.1f}%) reject at α=0.05")
            print(f"      (Expected ~5% under null of no serial correlation)")

    # 1d. Hill estimator for tail index
    print(f"\n  Hill Tail Index Estimator:")
    def hill_estimator(x, k):
        """Hill estimator for tail index α from the k largest observations."""
        x_sorted = np.sort(np.abs(x))[::-1]
        if k < 2 or k >= len(x_sorted):
            return np.nan, np.nan
        log_ratios = np.log(x_sorted[:k]) - np.log(x_sorted[k])
        alpha_hat = k / np.sum(log_ratios)
        se = alpha_hat / np.sqrt(k)
        return alpha_hat, se

    # Empirical tail index
    for label, data in [("Empirical", all_ch_emp), ("Simulated", sim_ch_flat)]:
        n_data = len(data)
        k_opt = int(np.sqrt(n_data))  # sqrt(n) rule of thumb
        alpha_hill, se_hill = hill_estimator(data, k_opt)
        ci_lo = alpha_hill - 1.96 * se_hill
        ci_hi = alpha_hill + 1.96 * se_hill
        print(f"    {label}: α̂={alpha_hill:.3f} ± {1.96*se_hill:.3f} [{ci_lo:.3f}, {ci_hi:.3f}] (k={k_opt})")

    # 1e. Shorrocks Mobility Index from transition matrix
    print(f"\n  Rank Transition Matrix & Shorrocks Mobility Index:")
    n_quintiles = 5
    quintile_labels = ['Q1 (Top)', 'Q2', 'Q3', 'Q4', 'Q5 (Bot)']

    def compute_transition_matrix(rk_data, n_q, horizon):
        """Compute quintile transition matrix from rank data."""
        t0_ranks = rk_data[0]
        tk_ranks = rk_data[min(horizon, rk_data.shape[0]-1)]
        n_total = len(t0_ranks)
        q_size = n_total / n_q

        q0 = np.clip(np.floor((t0_ranks - 1) / q_size).astype(int), 0, n_q-1)
        qk = np.clip(np.floor((tk_ranks - 1) / q_size).astype(int), 0, n_q-1)

        trans = np.zeros((n_q, n_q))
        for i in range(n_total):
            trans[q0[i], qk[i]] += 1

        # Normalize rows
        row_sums = trans.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1
        trans_norm = trans / row_sums
        return trans_norm

    for horizon, hlabel in [(1, "1-week"), (4, "4-week"), (13, "13-week")]:
        # Simulated
        sim_trans = compute_transition_matrix(sim_rk_full, n_quintiles, horizon)
        trace_sim = np.trace(sim_trans)
        shorrocks_sim = (n_quintiles - trace_sim) / (n_quintiles - 1)

        # Empirical (from rank_pivot data)
        emp_rk_arr = rank_pivot.values  # (n_weeks, n_endpoints)
        emp_trans = compute_transition_matrix(emp_rk_arr, n_quintiles, horizon)
        trace_emp = np.trace(emp_trans)
        shorrocks_emp = (n_quintiles - trace_emp) / (n_quintiles - 1)

        print(f"    {hlabel}: Shorrocks M = {shorrocks_sim:.4f} (sim) vs {shorrocks_emp:.4f} (emp)")

    # Print 13-week transition matrix
    print(f"\n  13-week Transition Matrix (Simulated):")
    sim_trans_13 = compute_transition_matrix(sim_rk_full, n_quintiles, 13)
    print(f"  {'':12s}", end='')
    for ql in quintile_labels:
        print(f"  {ql:>10s}", end='')
    print()
    for i, ql in enumerate(quintile_labels):
        print(f"  {ql:12s}", end='')
        for j in range(n_quintiles):
            print(f"  {sim_trans_13[i,j]:10.3f}", end='')
        print()

    print(f"\n  13-week Transition Matrix (Empirical):")
    emp_trans_13 = compute_transition_matrix(emp_rk_arr, n_quintiles, 13)
    print(f"  {'':12s}", end='')
    for ql in quintile_labels:
        print(f"  {ql:>10s}", end='')
    print()
    for i, ql in enumerate(quintile_labels):
        print(f"  {ql:12s}", end='')
        for j in range(n_quintiles):
            print(f"  {emp_trans_13[i,j]:10.3f}", end='')
        print()

    # 1f. Half-life of rank persistence by stratum
    print(f"\n  Half-Life of Rank Persistence by Stratum:")
    strata = [(1, 100, "Top 100"), (101, 500, "101-500"), (501, 2000, "501-2K"),
              (2001, 5000, "2K-5K"), (5001, N_FULL, "5K+")]

    for rk_lo, rk_hi, slabel in strata:
        # Find endpoints starting in this stratum
        t0_in_stratum = set(np.where((sim_rk_full[0] >= rk_lo) & (sim_rk_full[0] <= rk_hi))[0])
        n_in = len(t0_in_stratum)
        if n_in == 0:
            continue
        # For each horizon, count fraction still in stratum
        half_life = None
        for k in range(1, T_SIM):
            tk_in = set(np.where((sim_rk_full[k] >= rk_lo) & (sim_rk_full[k] <= rk_hi))[0])
            frac = len(t0_in_stratum & tk_in) / n_in
            if frac < 0.5 and half_life is None:
                # Linear interpolation
                prev_k = k - 1
                tk_prev = set(np.where((sim_rk_full[prev_k] >= rk_lo) & (sim_rk_full[prev_k] <= rk_hi))[0])
                frac_prev = len(t0_in_stratum & tk_prev) / n_in
                if frac_prev > frac:
                    half_life = prev_k + (frac_prev - 0.5) / (frac_prev - frac)
                else:
                    half_life = k
                break
        if half_life is None:
            half_life_str = f">{T_SIM} wk"
        else:
            half_life_str = f"{half_life:.1f} wk"
        # Same for empirical
        t0_in_emp = set(np.where((emp_rk_arr[0] >= rk_lo) & (emp_rk_arr[0] <= rk_hi))[0])
        n_in_emp = len(t0_in_emp)
        half_life_emp = None
        if n_in_emp > 0:
            for k in range(1, emp_rk_arr.shape[0]):
                tk_in_emp = set(np.where((emp_rk_arr[k] >= rk_lo) & (emp_rk_arr[k] <= rk_hi))[0])
                frac_emp = len(t0_in_emp & tk_in_emp) / n_in_emp
                if frac_emp < 0.5 and half_life_emp is None:
                    prev_k = k - 1
                    tk_prev_emp = set(np.where((emp_rk_arr[prev_k] >= rk_lo) & (emp_rk_arr[prev_k] <= rk_hi))[0])
                    frac_prev_emp = len(t0_in_emp & tk_prev_emp) / n_in_emp
                    if frac_prev_emp > frac_emp:
                        half_life_emp = prev_k + (frac_prev_emp - 0.5) / (frac_prev_emp - frac_emp)
                    else:
                        half_life_emp = k
                    break
        if half_life_emp is None:
            hl_emp_str = f">{emp_rk_arr.shape[0]} wk"
        else:
            hl_emp_str = f"{half_life_emp:.1f} wk"
        print(f"    {slabel:>10s}: sim={half_life_str:>10s}  emp={hl_emp_str:>10s}")

    # 1g. Kurtosis by rank band
    print(f"\n  Kurtosis by Rank Band:")
    for lo, hi in bands:
        # Empirical
        beps_emp = avg_rank[(avg_rank >= lo) & (avg_rank <= hi)].index
        emp_band_ch = log_changes[beps_emp].values.flatten()
        emp_band_ch = emp_band_ch[np.isfinite(emp_band_ch)]
        emp_band_kurt = sp_stats.kurtosis(emp_band_ch, fisher=True) if len(emp_band_ch) > 20 else np.nan

        # Simulated
        bm = (bp_avg_rk_global >= lo) & (bp_avg_rk_global <= hi)
        if bm.sum() > 5:
            sim_band_ch = sim_ch[bm.index[bm]].values.flatten()
            sim_band_ch = sim_band_ch[np.isfinite(sim_band_ch)]
            sim_band_kurt = sp_stats.kurtosis(sim_band_ch, fisher=True) if len(sim_band_ch) > 20 else np.nan
        else:
            sim_band_kurt = np.nan

        print(f"    {lo:5d}-{hi:5d}: emp={emp_band_kurt:6.2f}  sim={sim_band_kurt:6.2f}")

    # 1h. ACF of absolute and squared changes (volatility clustering)
    print(f"\n  Volatility Clustering (ACF of |changes| and changes²):")
    emp_abs_acf = {}
    sim_abs_acf = {}
    emp_sq_acf = {}
    sim_sq_acf = {}
    for lag in [1, 2, 4, 8]:
        # Empirical
        abs_cors = [log_changes[ep].dropna().abs().autocorr(lag) for ep in sample_eps[:500]
                    if len(log_changes[ep].dropna()) > lag + 5]
        emp_abs_acf[lag] = np.nanmedian(abs_cors)
        sq_cors = [(log_changes[ep].dropna()**2).autocorr(lag) for ep in sample_eps[:500]
                   if len(log_changes[ep].dropna()) > lag + 5]
        emp_sq_acf[lag] = np.nanmedian(sq_cors)
        # Simulated
        abs_cors_sim = [sim_ch[i].dropna().abs().autocorr(lag) for i in range(min(500, N_BP))
                        if len(sim_ch[i].dropna()) > lag + 5]
        sim_abs_acf[lag] = np.nanmedian(abs_cors_sim)
        sq_cors_sim = [(sim_ch[i].dropna()**2).autocorr(lag) for i in range(min(500, N_BP))
                       if len(sim_ch[i].dropna()) > lag + 5]
        sim_sq_acf[lag] = np.nanmedian(sq_cors_sim)
        print(f"    Lag {lag}: |Δy| ACF: emp={emp_abs_acf[lag]:.4f} sim={sim_abs_acf[lag]:.4f}  "
              f"Δy² ACF: emp={emp_sq_acf[lag]:.4f} sim={sim_sq_acf[lag]:.4f}")

    # ------------------------------------------------------------------
    # 2. PLOTS — Original v3.4 diagnostics (Figure 1)
    # ------------------------------------------------------------------
    print("\n\nGenerating plots...")

    # === FIGURE 1: Core calibration diagnostics (same as v3.4) ===
    fig = plt.figure(figsize=(22, 28))
    gs = GridSpec(5, 3, figure=fig, hspace=0.35, wspace=0.30)
    fig.suptitle(f'Rank Diffusion v3.7 | {n_pass}/{len(tests)} | {N_REP} MC reps | '
                 f'rank-dep κ + rank-dep t_df',
                 fontsize=12, fontweight='bold', y=0.995)

    # VR
    ax = fig.add_subplot(gs[0,0])
    vr_ks = sorted([k for k in vr_emp.keys() if k <= 52])
    ax.plot(vr_ks, [vr_emp[k] for k in vr_ks], 'ko-', label='Emp', ms=5, lw=2)
    svrs = [(sim_df_plot.diff(k).iloc[k:].var()/(k*sim_v1)).median() for k in vr_ks]
    ax.plot(vr_ks, svrs, 'rs--', label='Sim', ms=5, lw=2)
    for k in [2,4,8,13]:
        s = mc_stats.get(f'vr{k}')
        if s: ax.errorbar(k, s['mean'], yerr=[[s['mean']-s['lo']],[s['hi']-s['mean']]],
                         fmt='b^', ms=4, capsize=3, alpha=0.7)
    ax.set_xlabel('Horizon'); ax.set_ylabel('VR'); ax.set_title('Variance Ratio')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # ACF
    ax = fig.add_subplot(gs[0,1])
    lags=[1,2,3,4]; x=np.arange(len(lags))
    ax.bar(x-0.15,[acf_emp.get(l,0) for l in lags],0.3,label='Emp',color='black',alpha=0.7)
    sa=[np.nanmedian([sim_ch[i].dropna().autocorr(l) for i in range(min(500,N_BP)) if len(sim_ch[i].dropna())>l+5]) for l in lags]
    ax.bar(x+0.15,sa,0.3,label='Sim',color='red',alpha=0.7)
    ax.axhline(0,color='gray',lw=0.5); ax.set_xticks(x); ax.set_xticklabels(lags)
    ax.set_title('ACF of Changes'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # RACF
    ax = fig.add_subplot(gs[0,2])
    rl=[1,4,13,26]; x=np.arange(len(rl))
    sim_rk_df_plot = pd.DataFrame(bp_rk)
    emp_racf = [racf_emp.get(l,0) for l in rl]
    sim_racf_rep = [np.nanmedian([sim_rk_df_plot[i].dropna().autocorr(l)
                     for i in range(min(1000,N_BP)) if len(sim_rk_df_plot[i].dropna())>l+5]) for l in rl]
    ax.bar(x-0.15, emp_racf, 0.3, label='Emp', color='black', alpha=0.7)
    ax.bar(x+0.15, sim_racf_rep, 0.3, label='Sim', color='red', alpha=0.7)
    for j, l in enumerate([1,4,13]):
        s = mc_stats.get(f'racf{l}')
        if s: ax.errorbar(j+0.15, s['mean'], yerr=[[s['mean']-s['lo']],[s['hi']-s['mean']]],
                         fmt='b_', ms=8, capsize=3, alpha=0.7)
    ax.set_xticks(x); ax.set_xticklabels(rl)
    ax.set_title('Rank ACF'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # R²
    ax = fig.add_subplot(gs[1,0])
    r2k=[1,4,13,26,52]
    ax.plot(r2k,[xr2_emp.get(k,0) for k in r2k],'ko-',label='Emp',ms=5,lw=2)
    sim_r2 = [np.corrcoef(bp_ly[0], bp_ly[k])[0,1]**2 if k<T_SIM else 0 for k in r2k]
    ax.plot(r2k, sim_r2, 'rs--', label='Sim', ms=5, lw=2)
    for k in [1,4,13]:
        s = mc_stats.get(f'xr2_{k}')
        if s: ax.errorbar(k, s['mean'], yerr=[[s['mean']-s['lo']],[s['hi']-s['mean']]],
                         fmt='b^', ms=4, capsize=3, alpha=0.7)
    ax.set_title('Cross-Sectional R²'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # Persistence
    ax = fig.add_subplot(gs[1,1])
    pk=[1,4,13,26,52]
    ax.plot(pk,[pers_emp.get(k,0) for k in pk],'ko-',label='Emp',ms=5,lw=2)
    sim_pers = [len(set(np.where(sim_rk[0]<=100)[0]) & set(np.where(sim_rk[k]<=100)[0]))
                if k<T_SIM else 0 for k in pk]
    ax.plot(pk, sim_pers, 'rs--', label='Sim', ms=5, lw=2)
    for k in pk:
        s = mc_stats.get(f'pers{k}')
        if s: ax.errorbar(k, s['mean'], yerr=[[s['mean']-s['lo']],[s['hi']-s['mean']]],
                         fmt='b^', ms=4, capsize=3, alpha=0.7)
    ax.set_title('Top-100 Persistence'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # Distribution
    ax = fig.add_subplot(gs[1,2])
    bins=np.linspace(-3,3,120)
    ax.hist(np.clip(all_ch_emp,-3,3),bins,density=True,alpha=0.5,color='black',label='Emp')
    ax.hist(np.clip(sim_ch_flat,-3,3),bins,density=True,alpha=0.5,color='red',label='Sim')
    ax.set_title(f'Changes (kurt={mc_stats["kurtosis"]["mean"]:.1f}/{emp_kurt:.1f})')
    ax.legend(fontsize=8); ax.grid(True,alpha=0.3)

    # Cross-sec var
    ax = fig.add_subplot(gs[2,0])
    post_burnin_xsec = xsec_vars[T_BURNIN:]
    ax.plot(range(T_SIM), post_burnin_xsec[:T_SIM], 'r-', label='Sim τ', lw=2)
    sim_bp_xsec = [np.var(bp_ly[t]) for t in range(T_SIM)]
    emp_xsec_ts = log_metric.var(axis=1).values
    ax.plot(range(T_SIM), emp_xsec_ts, 'k-', label='Emp BP', lw=2)
    ax.plot(range(T_SIM), sim_bp_xsec, 'r--', label='Sim BP', lw=2)
    ax.set_title('Cross-sec Variance'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # Band variance
    ax = fig.add_subplot(gs[2,1])
    bc_mids = [np.sqrt(lo*hi) for lo,hi in bands]
    ev = [band_stats[(lo,hi)]['var'] for lo,hi in bands]
    sv_b = []
    for lo,hi in bands:
        bm = (bp_avg_rk_global >= lo) & (bp_avg_rk_global <= hi)
        if bm.sum() > 5:
            sv_b.append(sim_ch[bm.index[bm]].var().median())
        else:
            sv_b.append(0)
    ax.plot(bc_mids, ev, 'ko-', label='Emp', ms=5)
    ax.plot(bc_mids, sv_b, 'rs--', label='Sim', ms=5)
    ax.set_xscale('log'); ax.set_title('Band Variance'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # Band VR4
    ax = fig.add_subplot(gs[2,2])
    ev4 = [band_stats[(lo,hi)]['vr4'] for lo,hi in bands]
    sv4 = []
    for lo,hi in bands:
        bm = (bp_avg_rk_global>=lo)&(bp_avg_rk_global<=hi)
        if bm.sum()>5:
            bch=sim_ch[bm.index[bm]]; bdf=sim_df_plot[bm.index[bm]]
            sv4.append((bdf.diff(4).iloc[4:].var()/(4*bch.var())).median())
        else: sv4.append(0)
    ax.plot(bc_mids, ev4, 'ko-', label='Emp', ms=5)
    ax.plot(bc_mids, sv4, 'rs--', label='Sim', ms=5)
    ax.set_xscale('log'); ax.set_title('Band VR(4)'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # Trajectories
    for j, (idx, lbl) in enumerate([(0,'Top'), (N_BP//2,'Mid'), (N_BP-1,'Bottom')]):
        ax = fig.add_subplot(gs[3, j])
        ax.plot(range(T_SIM), bp_rk[:, idx], 'r-', alpha=0.7, lw=1)
        ax.set_xlabel('Week'); ax.set_ylabel('Rank')
        ax.set_title(f'Rank trajectory: {lbl}'); ax.grid(True, alpha=0.3)
        ax.invert_yaxis()

    # MC histograms
    ax = fig.add_subplot(gs[4,0])
    vals = [d['pers13'] for d in all_diags]
    ax.hist(vals, bins=15, color='steelblue', alpha=0.7, edgecolor='white')
    ax.axvline(pers_emp[13], color='black', lw=2, ls='--', label=f'Emp={pers_emp[13]}')
    ax.axvline(np.mean(vals), color='red', lw=2, label=f'Mean={np.mean(vals):.1f}')
    ax.set_title(f'Pers(13) MC dist'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    ax = fig.add_subplot(gs[4,1])
    vals = [d['kurtosis'] for d in all_diags]
    ax.hist(vals, bins=15, color='steelblue', alpha=0.7, edgecolor='white')
    ax.axvline(emp_kurt, color='black', lw=2, ls='--', label=f'Emp={emp_kurt:.1f}')
    ax.axvline(np.mean(vals), color='red', lw=2, label=f'Mean={np.mean(vals):.1f}')
    ax.set_title(f'Kurtosis MC dist'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    ax = fig.add_subplot(gs[4,2])
    vals = [d['racf1'] for d in all_diags]
    ax.hist(vals, bins=15, color='steelblue', alpha=0.7, edgecolor='white')
    ax.axvline(racf_emp[1], color='black', lw=2, ls='--', label=f'Emp={racf_emp[1]:.3f}')
    ax.axvline(np.mean(vals), color='red', lw=2, label=f'Mean={np.mean(vals):.3f}')
    ax.set_title(f'RACF(1) MC dist'); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    plt.savefig('/Users/hindman/Documents/github/rank-diffusion/llm_fitting/v37_diagnostics.png',
                dpi=130, bbox_inches='tight')
    print("Saved v37_diagnostics.png")
    plt.close()

    # ------------------------------------------------------------------
    # === FIGURE 2: Publication diagnostics (NEW in v3.5) ===
    # ------------------------------------------------------------------
    fig2 = plt.figure(figsize=(24, 32))
    gs2 = GridSpec(5, 3, figure=fig2, hspace=0.38, wspace=0.32)
    fig2.suptitle('Rank Diffusion v3.7 — Publication Diagnostics',
                  fontsize=13, fontweight='bold', y=0.995)

    # --- 2a. QQ Plot: innovations vs fitted t ---
    ax = fig2.add_subplot(gs2[0, 0])
    # Use within-endpoint standardized residuals
    n_qq = min(10000, len(sim_std_resid_flat))
    qq_sample = np.sort(np.random.choice(sim_std_resid_flat, n_qq, replace=False))
    theoretical_q = sp_stats.t.ppf(np.linspace(0.001, 0.999, n_qq), df=t_df, loc=loc_fit, scale=scale_fit)
    ax.scatter(theoretical_q, qq_sample, s=1, alpha=0.3, color='steelblue')
    lims = [min(theoretical_q.min(), qq_sample.min()), max(theoretical_q.max(), qq_sample.max())]
    ax.plot(lims, lims, 'r-', lw=1.5, label='45° line')
    ax.set_xlabel(f't({t_df:.1f}) quantiles'); ax.set_ylabel('Sim std residuals')
    ax.set_title(f'QQ Plot: Innovations vs t(df={t_df:.1f})')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)
    ax.set_aspect('equal', adjustable='box')

    # --- 2b. Log-log Zipf rank-size plot ---
    ax = fig2.add_subplot(gs2[0, 1])
    # Empirical
    emp_w0 = df[df['date'] == dates[0]].sort_values('rank')
    emp_ranks_z = emp_w0['rank'].values
    emp_vals_z = emp_w0['metric_value'].values
    emp_mask = (emp_vals_z > 0) & (emp_ranks_z <= 10000)
    ax.scatter(np.log10(emp_ranks_z[emp_mask]), np.log10(emp_vals_z[emp_mask]),
               s=1, alpha=0.3, color='black', label='Emp')
    # Simulated
    sim_y0_vals = np.exp(sim_ly[0])
    sim_y0_sorted = np.sort(sim_y0_vals)[::-1]
    sim_ranks_z = np.arange(1, len(sim_y0_sorted)+1)
    sim_mask_z = (sim_y0_sorted > 0) & (sim_ranks_z <= 10000)
    ax.scatter(np.log10(sim_ranks_z[sim_mask_z]), np.log10(sim_y0_sorted[sim_mask_z]),
               s=1, alpha=0.3, color='red', label='Sim')
    # OLS fit lines
    ax.plot([0, 4], [np.log10(emp_vals_z[emp_mask][0]),
                     np.log10(emp_vals_z[emp_mask][0]) + zipf_slope * 4],
            'k--', lw=1.5, alpha=0.7, label=f'Emp slope={zipf_slope:.2f}')
    ax.set_xlabel('log₁₀(Rank)'); ax.set_ylabel('log₁₀(Value)')
    ax.set_title('Zipf Rank-Size Plot (log-log)')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # --- 2c. Innovation density on log y-scale ---
    ax = fig2.add_subplot(gs2[0, 2])
    bins_log = np.linspace(-5, 5, 200)
    emp_hist, edges = np.histogram(np.clip(all_ch_emp, -5, 5), bins=bins_log, density=True)
    sim_hist, _ = np.histogram(np.clip(sim_ch_flat, -5, 5), bins=bins_log, density=True)
    bin_centers = (edges[:-1] + edges[1:]) / 2
    ax.semilogy(bin_centers, np.maximum(emp_hist, 1e-6), 'k-', lw=1.5, alpha=0.8, label='Emp')
    ax.semilogy(bin_centers, np.maximum(sim_hist, 1e-6), 'r-', lw=1.5, alpha=0.8, label='Sim')
    # Gaussian reference
    gauss_ref = sp_stats.norm.pdf(bin_centers, 0, np.std(all_ch_emp))
    ax.semilogy(bin_centers, gauss_ref, 'b--', lw=1, alpha=0.5, label='Gaussian ref')
    ax.set_xlabel('Δ log(y)'); ax.set_ylabel('Density (log scale)')
    ax.set_title('Innovation Density (Log Scale)')
    ax.set_ylim(1e-5, 10); ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # --- 2d. Rank transition heatmap (quintile, 13-week) ---
    ax = fig2.add_subplot(gs2[1, 0])
    im = ax.imshow(sim_trans_13, cmap='YlOrRd', vmin=0, vmax=1, aspect='auto')
    ax.set_xticks(range(n_quintiles)); ax.set_xticklabels(['Q1','Q2','Q3','Q4','Q5'], fontsize=8)
    ax.set_yticks(range(n_quintiles)); ax.set_yticklabels(['Q1','Q2','Q3','Q4','Q5'], fontsize=8)
    for i in range(n_quintiles):
        for j in range(n_quintiles):
            ax.text(j, i, f'{sim_trans_13[i,j]:.2f}', ha='center', va='center',
                    color='white' if sim_trans_13[i,j] > 0.5 else 'black', fontsize=9)
    ax.set_xlabel('Destination quintile'); ax.set_ylabel('Origin quintile')
    ax.set_title('Sim: 13-Week Rank Transition')
    plt.colorbar(im, ax=ax, shrink=0.8)

    # --- 2e. Empirical transition heatmap for comparison ---
    ax = fig2.add_subplot(gs2[1, 1])
    im = ax.imshow(emp_trans_13, cmap='YlOrRd', vmin=0, vmax=1, aspect='auto')
    ax.set_xticks(range(n_quintiles)); ax.set_xticklabels(['Q1','Q2','Q3','Q4','Q5'], fontsize=8)
    ax.set_yticks(range(n_quintiles)); ax.set_yticklabels(['Q1','Q2','Q3','Q4','Q5'], fontsize=8)
    for i in range(n_quintiles):
        for j in range(n_quintiles):
            ax.text(j, i, f'{emp_trans_13[i,j]:.2f}', ha='center', va='center',
                    color='white' if emp_trans_13[i,j] > 0.5 else 'black', fontsize=9)
    ax.set_xlabel('Destination quintile'); ax.set_ylabel('Origin quintile')
    ax.set_title('Emp: 13-Week Rank Transition')
    plt.colorbar(im, ax=ax, shrink=0.8)

    # --- 2f. CCDF on log-log axes ---
    ax = fig2.add_subplot(gs2[1, 2])
    # Compute CCDF for absolute changes
    emp_abs_sorted = np.sort(np.abs(all_ch_emp))[::-1]
    sim_abs_sorted = np.sort(np.abs(sim_ch_flat))[::-1]
    emp_ccdf_y = np.arange(1, len(emp_abs_sorted)+1) / len(emp_abs_sorted)
    sim_ccdf_y = np.arange(1, len(sim_abs_sorted)+1) / len(sim_abs_sorted)
    # Subsample for plotting
    n_plot = 5000
    emp_idx = np.linspace(0, len(emp_abs_sorted)-1, n_plot).astype(int)
    sim_idx = np.linspace(0, len(sim_abs_sorted)-1, n_plot).astype(int)
    ax.loglog(emp_abs_sorted[emp_idx], emp_ccdf_y[emp_idx], 'k-', lw=1.5, alpha=0.8, label='Emp')
    ax.loglog(sim_abs_sorted[sim_idx], sim_ccdf_y[sim_idx], 'r-', lw=1.5, alpha=0.8, label='Sim')
    ax.set_xlabel('|Δ log(y)|'); ax.set_ylabel('P(|Δ log(y)| > x)')
    ax.set_title('CCDF of Absolute Changes (log-log)')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3, which='both')

    # --- 2g. Hill plot (tail index vs k) ---
    ax = fig2.add_subplot(gs2[2, 0])
    ks_range = np.unique(np.logspace(1, np.log10(len(all_ch_emp)//5), 80).astype(int))
    emp_hills = []; sim_hills = []; emp_hill_se = []; sim_hill_se = []
    for k_h in ks_range:
        a_e, se_e = hill_estimator(all_ch_emp, k_h)
        a_s, se_s = hill_estimator(sim_ch_flat, k_h)
        emp_hills.append(a_e); sim_hills.append(a_s)
        emp_hill_se.append(se_e); sim_hill_se.append(se_s)
    emp_hills = np.array(emp_hills); sim_hills = np.array(sim_hills)
    emp_hill_se = np.array(emp_hill_se); sim_hill_se = np.array(sim_hill_se)
    ax.semilogx(ks_range, emp_hills, 'k-', lw=1.5, label='Emp')
    ax.fill_between(ks_range, emp_hills-1.96*emp_hill_se, emp_hills+1.96*emp_hill_se,
                    color='black', alpha=0.15)
    ax.semilogx(ks_range, sim_hills, 'r-', lw=1.5, label='Sim')
    ax.fill_between(ks_range, sim_hills-1.96*sim_hill_se, sim_hills+1.96*sim_hill_se,
                    color='red', alpha=0.15)
    ax.axhline(t_df, color='blue', ls='--', lw=1, alpha=0.5, label=f't_df={t_df:.1f}')
    ax.set_xlabel('k (order statistics)'); ax.set_ylabel('α̂ (tail index)')
    ax.set_title('Hill Plot: Tail Index vs k')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)
    ax.set_ylim(0, max(15, max(emp_hills.max(), sim_hills.max()) * 1.1))

    # --- 2h. ACF of |changes| and changes² (volatility clustering) ---
    ax = fig2.add_subplot(gs2[2, 1])
    vol_lags = [1, 2, 4, 8]
    x_v = np.arange(len(vol_lags))
    w = 0.18
    ax.bar(x_v - 1.5*w, [emp_abs_acf[l] for l in vol_lags], w, label='|Δy| Emp', color='black', alpha=0.7)
    ax.bar(x_v - 0.5*w, [sim_abs_acf[l] for l in vol_lags], w, label='|Δy| Sim', color='red', alpha=0.7)
    ax.bar(x_v + 0.5*w, [emp_sq_acf[l] for l in vol_lags], w, label='Δy² Emp', color='gray', alpha=0.7)
    ax.bar(x_v + 1.5*w, [sim_sq_acf[l] for l in vol_lags], w, label='Δy² Sim', color='salmon', alpha=0.7)
    ax.axhline(0, color='gray', lw=0.5)
    ax.set_xticks(x_v); ax.set_xticklabels(vol_lags)
    ax.set_xlabel('Lag'); ax.set_ylabel('ACF')
    ax.set_title('Volatility Clustering: ACF of |Δy| and Δy²')
    ax.legend(fontsize=7, ncol=2); ax.grid(True, alpha=0.3)

    # --- 2i. Kurtosis by rank band ---
    ax = fig2.add_subplot(gs2[2, 2])
    emp_kurts_band = []; sim_kurts_band = []
    band_labels = []
    for lo, hi in bands:
        band_labels.append(f'{lo}-{hi}')
        beps_emp = avg_rank[(avg_rank >= lo) & (avg_rank <= hi)].index
        emp_band_ch_p = log_changes[beps_emp].values.flatten()
        emp_band_ch_p = emp_band_ch_p[np.isfinite(emp_band_ch_p)]
        emp_kurts_band.append(sp_stats.kurtosis(emp_band_ch_p, fisher=True) if len(emp_band_ch_p) > 20 else 0)

        bm = (bp_avg_rk_global >= lo) & (bp_avg_rk_global <= hi)
        if bm.sum() > 5:
            sim_band_ch_p = sim_ch[bm.index[bm]].values.flatten()
            sim_band_ch_p = sim_band_ch_p[np.isfinite(sim_band_ch_p)]
            sim_kurts_band.append(sp_stats.kurtosis(sim_band_ch_p, fisher=True) if len(sim_band_ch_p) > 20 else 0)
        else:
            sim_kurts_band.append(0)

    x_b = np.arange(len(bands))
    ax.bar(x_b - 0.15, emp_kurts_band, 0.3, label='Emp', color='black', alpha=0.7)
    ax.bar(x_b + 0.15, sim_kurts_band, 0.3, label='Sim', color='red', alpha=0.7)
    ax.set_xticks(x_b); ax.set_xticklabels(band_labels, fontsize=7, rotation=20)
    ax.set_xlabel('Rank Band'); ax.set_ylabel('Excess Kurtosis')
    ax.set_title('Kurtosis by Rank Band')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # --- 2j. Capital Distribution Curves (CDC) at multiple dates ---
    ax = fig2.add_subplot(gs2[3, 0])
    # Plot CDC: cumulative share of total value vs rank percentile
    cdc_times = [0, T_SIM//4, T_SIM//2, 3*T_SIM//4, T_SIM-1]
    colors_cdc = ['navy', 'steelblue', 'green', 'orange', 'red']
    for ti, tc in zip(cdc_times, colors_cdc):
        # Simulated
        sim_vals = np.exp(sim_ly_full[ti])
        sim_sorted = np.sort(sim_vals)[::-1]
        cum_share = np.cumsum(sim_sorted) / np.sum(sim_sorted)
        rank_pct = np.arange(1, len(sim_sorted)+1) / len(sim_sorted) * 100
        ax.plot(rank_pct, cum_share * 100, color=tc, lw=1.5, alpha=0.8,
                label=f'Sim wk {ti}')
    # Empirical at t=0
    emp_w0_vals = df[df['date'] == dates[0]].sort_values('rank')['metric_value'].values
    emp_w0_sorted = np.sort(emp_w0_vals[emp_w0_vals > 0])[::-1]
    cum_share_emp = np.cumsum(emp_w0_sorted) / np.sum(emp_w0_sorted)
    rank_pct_emp = np.arange(1, len(emp_w0_sorted)+1) / len(emp_w0_sorted) * 100
    ax.plot(rank_pct_emp, cum_share_emp * 100, 'k--', lw=2, label='Emp wk 0')
    ax.set_xlabel('Rank percentile'); ax.set_ylabel('Cumulative share (%)')
    ax.set_title('Capital Distribution Curves (CDC)')
    ax.set_xlim(0, 50); ax.legend(fontsize=7, ncol=2); ax.grid(True, alpha=0.3)

    # --- 2k. Kaplan-Meier survival curves for top-K persistence ---
    ax = fig2.add_subplot(gs2[3, 1])
    top_ks = [50, 100, 200, 500]
    colors_km = ['navy', 'steelblue', 'green', 'orange']
    for top_k, ckm in zip(top_ks, colors_km):
        # Simulated
        t0_top = set(np.where(sim_rk_full[0] <= top_k)[0])
        n_init = len(t0_top)
        surv_sim = []
        for wk in range(T_SIM):
            tw = set(np.where(sim_rk_full[wk] <= top_k)[0])
            surv_sim.append(len(t0_top & tw) / n_init * 100)
        ax.plot(range(T_SIM), surv_sim, color=ckm, lw=1.5, label=f'Sim top-{top_k}')

        # Empirical
        t0_top_emp = set(np.where(emp_rk_arr[0] <= top_k)[0])
        n_init_emp = len(t0_top_emp)
        surv_emp = []
        for wk in range(emp_rk_arr.shape[0]):
            tw_emp = set(np.where(emp_rk_arr[wk] <= top_k)[0])
            surv_emp.append(len(t0_top_emp & tw_emp) / n_init_emp * 100)
        ax.plot(range(len(surv_emp)), surv_emp, color=ckm, lw=1.5, ls='--', alpha=0.7)

    ax.axhline(50, color='gray', ls=':', lw=1)
    ax.set_xlabel('Weeks'); ax.set_ylabel('Survival (%)')
    ax.set_title('Top-K Persistence Survival (solid=sim, dash=emp)')
    ax.legend(fontsize=7); ax.grid(True, alpha=0.3)

    # --- 2l. QQ plot: Empirical standardized residuals vs t ---
    ax = fig2.add_subplot(gs2[3, 2])
    n_qq_emp = min(10000, len(z_within))
    qq_emp_sample = np.sort(np.random.choice(z_within, n_qq_emp, replace=False))
    theoretical_q_emp = sp_stats.t.ppf(np.linspace(0.001, 0.999, n_qq_emp), df=t_df, loc=loc_fit, scale=scale_fit)
    ax.scatter(theoretical_q_emp, qq_emp_sample, s=1, alpha=0.3, color='black')
    lims = [min(theoretical_q_emp.min(), qq_emp_sample.min()), max(theoretical_q_emp.max(), qq_emp_sample.max())]
    ax.plot(lims, lims, 'r-', lw=1.5)
    ax.set_xlabel(f't({t_df:.1f}) quantiles'); ax.set_ylabel('Emp std residuals')
    ax.set_title(f'QQ Plot: Emp Residuals vs t(df={t_df:.1f})')
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal', adjustable='box')

    # --- 2m. Rank-rank scatter (binned) at different horizons ---
    ax = fig2.add_subplot(gs2[4, 0])
    for k_rr, c_rr in [(1, 'navy'), (4, 'green'), (13, 'orange'), (26, 'red')]:
        if k_rr < T_SIM:
            r0 = sim_rk_full[0]
            rk = sim_rk_full[k_rr]
            # Bin by initial rank
            n_bins_rr = 50
            bin_edges = np.linspace(1, N_FULL, n_bins_rr + 1)
            bin_means_r0 = []
            bin_means_rk = []
            for bi in range(n_bins_rr):
                mask = (r0 >= bin_edges[bi]) & (r0 < bin_edges[bi+1])
                if mask.sum() > 0:
                    bin_means_r0.append(np.mean(r0[mask]))
                    bin_means_rk.append(np.mean(rk[mask]))
            ax.plot(bin_means_r0, bin_means_rk, '-', color=c_rr, lw=1.5, alpha=0.8,
                    label=f'k={k_rr}')
    ax.plot([1, N_FULL], [1, N_FULL], 'k--', lw=1, alpha=0.5)
    ax.set_xlabel('Initial rank (binned)'); ax.set_ylabel('Mean rank at week k')
    ax.set_title('Rank-Rank Regression (Simulated)')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    # --- 2n. Cross-sectional density snapshots ---
    ax = fig2.add_subplot(gs2[4, 1])
    snap_times = [0, T_SIM//2, T_SIM-1]
    snap_colors = ['navy', 'green', 'red']
    for ti, sc in zip(snap_times, snap_colors):
        vals = sim_ly_full[ti]
        ax.hist(vals, bins=80, density=True, alpha=0.4, color=sc, label=f'Sim wk {ti}')
    # Empirical t=0
    emp_lm_t0 = log_metric.iloc[0].dropna().values
    ax.hist(emp_lm_t0, bins=80, density=True, histtype='step', color='black',
            lw=2, label='Emp wk 0')
    ax.set_xlabel('log(1+value)'); ax.set_ylabel('Density')
    ax.set_title('Cross-Sectional Density Snapshots')
    ax.legend(fontsize=7); ax.grid(True, alpha=0.3)

    # --- 2o. Shorrocks index over horizons ---
    ax = fig2.add_subplot(gs2[4, 2])
    horizons_sh = [1, 2, 4, 8, 13, 26, 52]
    shor_sim = []; shor_emp = []
    for h in horizons_sh:
        if h < T_SIM:
            st_sim = compute_transition_matrix(sim_rk_full, n_quintiles, h)
            shor_sim.append((n_quintiles - np.trace(st_sim)) / (n_quintiles - 1))
        else:
            shor_sim.append(np.nan)
        if h < emp_rk_arr.shape[0]:
            st_emp = compute_transition_matrix(emp_rk_arr, n_quintiles, h)
            shor_emp.append((n_quintiles - np.trace(st_emp)) / (n_quintiles - 1))
        else:
            shor_emp.append(np.nan)
    ax.plot(horizons_sh, shor_emp, 'ko-', label='Emp', ms=5, lw=2)
    ax.plot(horizons_sh, shor_sim, 'rs--', label='Sim', ms=5, lw=2)
    ax.set_xlabel('Horizon (weeks)'); ax.set_ylabel('Shorrocks M')
    ax.set_title('Shorrocks Mobility Index vs Horizon')
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    plt.savefig('/Users/hindman/Documents/github/rank-diffusion/llm_fitting/v37_pub_diagnostics.png',
                dpi=130, bbox_inches='tight')
    print("Saved v37_pub_diagnostics.png")
    plt.close()

# Final summary
print(f"\n{'='*70}")
print(f"v3.7 COMPLETE")
print(f"{'='*70}")
print(f"  Calibration: {n_pass}/{len(tests)} diagnostics pass")
print(f"  Parameters: σ_obs={sigma_obs:.4f} σ_het={sigma_het:.4f}")
print(f"              t_df: {', '.join(f'{lo}-{hi}={band_tdf[(lo,hi)]:.1f}' for lo,hi in bands)}")
print(f"              κ_base={kappa_base:.6f} α_κ={alpha_kappa}")
print(f"              jump_p={jump_prob:.4f} jump_s={jump_scale:.2f}")
print(f"              α_arch={alpha_arch:.4f}")
print(f"  Key change: Rank-dependent t_df (band-level tail shape estimation)")
print(f"  Elapsed: {time.time()-t_start:.0f}s")
print("Done.")
