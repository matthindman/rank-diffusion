#!/usr/bin/env python3
"""
Ablation Study for Rank Diffusion Model v3.9
=============================================
Demonstrates that each model component is necessary by building up from a
minimal permanent-transitory model to the full v3.9, evaluating the same
15 calibration diagnostics at each level.

Ablation levels:
  1. Base:     PT decomposition + band params + σ_obs + σ_het + entry/exit, Gaussian
  2. +Burn-in: 50-week burn-in before recording
  3. +κ:       Global mean reversion (uniform κ, no rank dependence)
  4. +κ(r):    Rank-dependent κ with α=0.5
  5. +Tails:   Heavy-tailed innovations (global t_df) + jump mixture
  6. +ARCH:    ARCH(1) on transitory innovations
  7. +RankTdf: Rank-dependent t_df + two-pass kurtosis calibration
  8. +κ-stab:  κ variance-stabilization factor (1.20×) = full v3.9

Outputs:
  - Console table: diagnostics pass/fail at each level
  - ablation_table.png: heatmap of pass/fail + score trajectory
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
from matplotlib.colors import ListedColormap

t_start = time.time()

# ============================================================
# DATA LOADING (identical to v3.9)
# ============================================================
print("=" * 70)
print("ABLATION STUDY — DATA LOADING")
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
print(f"  N_balanced={N_balanced}, mean_N={mean_N:.0f}")

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

all_ch_emp = log_changes.values.flatten()
all_ch_emp = all_ch_emp[np.isfinite(all_ch_emp)]
emp_kurt = sp_stats.kurtosis(all_ch_emp, fisher=True)
emp_mean_var = var_1.mean()
emp_median_var = var_1.median()
xsec_var_emp = log_metric.var(axis=1).mean()

w0_all = df[df['date'] == dates[0]]
xsec_var_full = np.log1p(w0_all[w0_all['metric_value'] > 0]['metric_value']).var()

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

# ============================================================
# PARAMETER ESTIMATION (identical to v3.9)
# ============================================================
print("\n" + "=" * 70)
print("PARAMETER ESTIMATION")
print("=" * 70)

# Stage 1: σ_obs
phi_agg = acf_emp[3] / acf_emp[2]
gamma1 = acf_emp[1] * emp_median_var
gamma2 = acf_emp[2] * emp_median_var
sigma2_obs_est = -gamma1 + gamma2 / phi_agg
sigma_obs = np.sqrt(np.clip(sigma2_obs_est, 0.01**2, 0.50**2))
sobs2 = sigma_obs ** 2
print(f"  σ_obs = {sigma_obs:.4f}")

# Stage 2: σ_het
var_ratio = emp_mean_var / emp_median_var
sigma_het = np.sqrt(np.log(var_ratio) / 2)
E_h2 = np.exp(2 * sigma_het ** 2)
print(f"  σ_het = {sigma_het:.4f}")

# Stage 3: t_df (global and per-band)
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
print(f"  t_df_global = {t_df_global:.2f}")

obs_noise_var = 2 * sobs2
band_tdf = {}
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
    total_var = band_stats[(lo, hi)]['var']
    signal_frac = max(0.05, 1 - obs_noise_var / total_var)
    if signal_frac < 0.30:
        df_corrected = min(df_band / signal_frac, 200.0)
    else:
        df_corrected = df_band
    band_tdf[(lo, hi)] = df_corrected

# Stage 4: Jump parameters
threshold = 4.0
expected_tail = 2 * sp_stats.t.sf(threshold, df=t_df_global, loc=0, scale=scale_fit)
actual_tail = np.mean(np.abs(z_within - loc_fit) > threshold * scale_fit)
jump_prob = max(0.005, actual_tail - expected_tail)
extreme_mask = np.abs(z_within) > threshold * scale_fit
jump_scale = np.std(z_within[extreme_mask]) / np.std(z_within[~extreme_mask]) if extreme_mask.sum() > 10 else 5.0
print(f"  jump_prob = {jump_prob:.4f}, jump_scale = {jump_scale:.2f}")

# Stage 4.5: ARCH
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
alpha_arch = np.clip(np.median(z_sq_acfs), 0.01, 0.50)
print(f"  α_arch = {alpha_arch:.4f}")

# Stage 5: Band-level structural estimation
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
    print(f"  Band {lo:5d}-{hi:5d}: se={np.sqrt(se2):.4f} phi={phi:.4f} sn={np.sqrt(sn2):.4f}")

bc_arr = np.array([np.sqrt(lo*hi) for lo,hi in band_params.keys()])
ses_arr = np.array([p['se'] for p in band_params.values()])
phs_arr = np.array([p['phi'] for p in band_params.values()])
sns_arr = np.array([p['sn'] for p in band_params.values()])

def get_p(ranks):
    lr = np.log(np.clip(ranks.astype(float), 1, bc_arr[-1]*2))
    return (np.interp(lr, np.log(bc_arr), ses_arr),
            np.interp(lr, np.log(bc_arr), phs_arr),
            np.interp(lr, np.log(bc_arr), sns_arr))

tdf_arr = np.array([band_tdf[(lo, hi)] for lo, hi in bands])

def get_tdf(ranks):
    lr = np.log(np.clip(ranks.astype(float), 1, bc_arr[-1]*2))
    return np.interp(lr, np.log(bc_arr), tdf_arr)

# Stage 6: κ calibration
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
kappa_base_raw = max(mean_eta2 / (2 * weighted_dev2), 0.001)
kappa_base_unstab = kappa_base_raw          # without stabilization factor
kappa_base_stab = kappa_base_raw * 1.20     # with stabilization factor

# Global (uniform) κ: use mean of rank-weighted κ as a single value
kappa_global = kappa_base_raw * np.mean((init_ranks / N_FULL) ** alpha_kappa)

print(f"  κ_base_raw = {kappa_base_raw:.6f}")
print(f"  κ_base_stab = {kappa_base_stab:.6f}")
print(f"  κ_global = {kappa_global:.6f}")

# Entry/exit parameters
inc_alpha = 0.3
p_exit_incumbent = 0.0040
inc_p_base = p_exit_incumbent * (inc_alpha + 1)
trans_p_exit = 0.07

T_SIM = n_weeks
T_BURNIN_FULL = 50

# ============================================================
# TWO-PASS KURTOSIS CALIBRATION (for levels 7-8)
# ============================================================
# Run a small calibration pass to get calibrated band t_df values.
# This mirrors v3.9's approach.

def run_sim_for_kurtosis_cal(seed, use_rank_tdf, use_arch, kappa_base_val,
                              rank_dep_kappa, use_burnin):
    """Lightweight sim to measure band kurtosis for calibration."""
    rng = np.random.RandomState(seed)
    tau = w0_sorted.copy()
    c_state = np.zeros(N_FULL)
    het_mul = np.clip(np.exp(rng.normal(0, sigma_het, N_FULL)), 0.15, 8.0)
    ep_type = np.zeros(N_FULL, dtype=int)
    endpoint_id = np.arange(N_FULL)
    next_id = N_FULL
    last_z_sq = np.ones(N_FULL)
    t_burnin = T_BURNIN_FULL if use_burnin else 0
    t_total = t_burnin + T_SIM

    sim_ly = np.zeros((T_SIM, N_FULL))
    sim_rk = np.zeros((T_SIM, N_FULL), dtype=int)
    sim_ids = np.zeros((T_SIM, N_FULL), dtype=int)
    obs_noise = rng.normal(0, sigma_obs, N_FULL)
    y0_obs = tau + c_state + obs_noise
    order = np.argsort(-np.exp(y0_obs))
    ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)

    for t_abs in range(1, t_total):
        cr = ranks
        se, phi_v, sn = get_p(cr)
        se_het = se * het_mul
        sn_het = sn * het_mul
        is_jump = rng.random(N_FULL) < jump_prob
        eta = np.where(is_jump, rng.normal(0, se_het * jump_scale),
                       rng.normal(0, se_het))
        if use_arch:
            arch_var = (1 - alpha_arch) + alpha_arch * last_z_sq
            arch_sc = np.sqrt(np.clip(arch_var, 0.1, 10.0))
        else:
            arch_sc = np.ones(N_FULL)
        if use_rank_tdf:
            df_vec = get_tdf(cr)
            t_raw = sp_stats.t.rvs(df=df_vec, random_state=rng)
            t_vf = np.sqrt(np.maximum(df_vec - 2, 0.5) / df_vec)
        else:
            t_raw = sp_stats.t.rvs(df=t_df_global, size=N_FULL, random_state=rng)
            t_vf = np.sqrt(max(t_df_global - 2, 0.5) / t_df_global)
        nu = sn_het * t_vf * arch_sc * t_raw
        c_state = phi_v * c_state + nu
        last_z_sq = np.clip(nu ** 2 / (sn_het ** 2 + 1e-10), 0, 4.0)
        current_mean = np.mean(tau)
        if kappa_base_val > 0:
            if rank_dep_kappa:
                kappa_r = kappa_base_val * (cr / N_FULL) ** alpha_kappa
            else:
                kappa_r = kappa_base_val * np.ones(N_FULL)
            tau += eta - kappa_r * (tau - current_mean)
        else:
            tau += eta
        t_rec = t_abs - t_burnin
        if t_rec >= 0:
            nr = cr / N_FULL
            p_exit = np.where(ep_type == 0, inc_p_base * (nr ** inc_alpha), trans_p_exit)
            exit_mask = rng.random(N_FULL) < p_exit
            n_ex = exit_mask.sum()
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
                c_state[exi] = sp_stats.t.rvs(df=t_df_global, size=n_ex, random_state=rng) * 0.3
                het_mul[exi] = np.clip(np.exp(rng.normal(0, sigma_het, n_ex)), 0.15, 8.0)
                last_z_sq[exi] = 1.0
                ep_type[exi] = 1
                endpoint_id[exi] = np.arange(next_id, next_id+n_ex)
                next_id += n_ex
        log_y_obs = tau + c_state + rng.normal(0, sigma_obs, N_FULL)
        order = np.argsort(-np.exp(log_y_obs))
        ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)
        if t_rec == 0:
            sim_ly[0] = log_y_obs; sim_rk[0] = ranks; sim_ids[0] = endpoint_id.copy()
        elif 0 < t_rec < T_SIM:
            sim_ly[t_rec] = log_y_obs; sim_rk[t_rec] = ranks; sim_ids[t_rec] = endpoint_id.copy()

    # Balanced panel
    init_ids = set(sim_ids[0])
    survivors = init_ids.copy()
    for t in range(1, T_SIM):
        survivors &= set(sim_ids[t])
    survivor_list = sorted(survivors)
    N_BP = len(survivor_list)
    bp_ly = np.zeros((T_SIM, N_BP))
    bp_rk = np.zeros((T_SIM, N_BP), dtype=int)
    for t in range(T_SIM):
        id_map = {eid: idx for idx, eid in enumerate(sim_ids[t])}
        for j, sid in enumerate(survivor_list):
            bp_ly[t, j] = sim_ly[t, id_map[sid]]
            bp_rk[t, j] = sim_rk[t, id_map[sid]]
    bp_avg_rk = pd.DataFrame(bp_rk).mean()
    sim_ch = pd.DataFrame(bp_ly).diff().iloc[1:]
    bk = {}
    for lo, hi in bands:
        bm = (bp_avg_rk >= lo) & (bp_avg_rk <= hi)
        if bm.sum() > 5:
            bch = sim_ch[bm.index[bm]].values.flatten()
            bch = bch[np.isfinite(bch)]
            bk[(lo, hi)] = sp_stats.kurtosis(bch, fisher=True) if len(bch) > 20 else None
        else:
            bk[(lo, hi)] = None
    return bk


print("\n  Running kurtosis calibration pass (5 reps)...")
emp_band_kurt_target = {}
for lo, hi in bands:
    beps_emp = avg_rank[(avg_rank >= lo) & (avg_rank <= hi)].index
    emp_band_ch = log_changes[beps_emp].values.flatten()
    emp_band_ch = emp_band_ch[np.isfinite(emp_band_ch)]
    emp_band_kurt_target[(lo, hi)] = sp_stats.kurtosis(emp_band_ch, fisher=True)

N_CAL = 5
cal_kurts = {(lo, hi): [] for lo, hi in bands}
for cseed in range(200, 200 + N_CAL):
    bk = run_sim_for_kurtosis_cal(cseed, use_rank_tdf=True, use_arch=True,
                                   kappa_base_val=kappa_base_stab,
                                   rank_dep_kappa=True, use_burnin=True)
    for key, val in bk.items():
        if val is not None:
            cal_kurts[key].append(val)

OVERSHOOT = 1.5
PROTECTED_BANDS = [(1, 100), (101, 500)]
band_tdf_calibrated = {}
for lo, hi in bands:
    old_df = band_tdf[(lo, hi)]
    cal_vals = cal_kurts[(lo, hi)]
    emp_k = emp_band_kurt_target[(lo, hi)]
    if (lo, hi) in PROTECTED_BANDS:
        new_df = old_df
    elif len(cal_vals) >= 2:
        sim_k = np.median(cal_vals)
        if sim_k > 0.5 and emp_k > 0.5 and abs(sim_k - emp_k) / emp_k > 0.10:
            old_t_kurt = 6.0 / max(old_df - 4.0, 0.3)
            ratio = emp_k / sim_k
            target_t_kurt = old_t_kurt * (ratio ** OVERSHOOT)
            new_df = np.clip(4.0 + 6.0 / target_t_kurt, 4.2, 200.0)
        else:
            new_df = old_df
    else:
        new_df = old_df
    band_tdf_calibrated[(lo, hi)] = new_df
    print(f"    {lo:5d}-{hi:5d}: t_df {old_df:.2f} -> {new_df:.2f}")

tdf_arr_calibrated = np.array([band_tdf_calibrated[(lo, hi)] for lo, hi in bands])

def get_tdf_calibrated(ranks):
    lr = np.log(np.clip(ranks.astype(float), 1, bc_arr[-1]*2))
    return np.interp(lr, np.log(bc_arr), tdf_arr_calibrated)


# ============================================================
# CONFIGURABLE SIMULATION
# ============================================================

def run_ablation_sim(seed, config):
    """
    Run one simulation replication with configurable features.

    config keys:
      burn_in:        bool  — 50-week burn-in before recording
      kappa:          bool  — any mean reversion on τ
      rank_dep_kappa: bool  — rank-dependent κ(r) vs uniform κ
      kappa_stab:     bool  — apply 1.20× stabilization factor
      heavy_tails:    bool  — t-distributed innovations + jump mixture
      arch:           bool  — ARCH(1) on transitory innovations
      rank_dep_tdf:   bool  — rank-dependent t_df (requires heavy_tails)
      calibrated_tdf: bool  — two-pass kurtosis-calibrated t_df (requires rank_dep_tdf)
    """
    rng = np.random.RandomState(seed)

    use_burnin = config.get('burn_in', False)
    use_kappa = config.get('kappa', False)
    use_rank_dep_kappa = config.get('rank_dep_kappa', False)
    use_kappa_stab = config.get('kappa_stab', False)
    use_heavy_tails = config.get('heavy_tails', False)
    use_arch = config.get('arch', False)
    use_rank_dep_tdf = config.get('rank_dep_tdf', False)
    use_calibrated_tdf = config.get('calibrated_tdf', False)

    # Determine κ
    if use_kappa:
        if use_kappa_stab:
            kb = kappa_base_stab
        else:
            kb = kappa_base_unstab
    else:
        kb = 0.0

    t_burnin = T_BURNIN_FULL if use_burnin else 0
    t_total = t_burnin + T_SIM

    tau = w0_sorted.copy()
    c_state = np.zeros(N_FULL)
    het_multiplier = np.clip(np.exp(rng.normal(0, sigma_het, N_FULL)), 0.15, 8.0)
    ep_type = np.zeros(N_FULL, dtype=int)
    endpoint_id = np.arange(N_FULL)
    next_id = N_FULL
    last_z_sq = np.ones(N_FULL)

    sim_ly = np.zeros((T_SIM, N_FULL))
    sim_rk = np.zeros((T_SIM, N_FULL), dtype=int)
    sim_ids = np.zeros((T_SIM, N_FULL), dtype=int)

    obs_noise = rng.normal(0, sigma_obs, N_FULL)
    y0_obs = tau + c_state + obs_noise
    order = np.argsort(-np.exp(y0_obs))
    ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)

    total_exits = 0
    xsec_vars = [np.var(tau)]

    # When there's no burn-in, store the initial state (t_rec=0 is never
    # reached inside the loop because range starts at 1)
    if t_burnin == 0:
        sim_ly[0] = y0_obs
        sim_rk[0] = ranks
        sim_ids[0] = endpoint_id.copy()

    for t_abs in range(1, t_total):
        cr = ranks
        se, phi_v, sn = get_p(cr)
        se_het = se * het_multiplier
        sn_het = sn * het_multiplier

        # Permanent innovations
        if use_heavy_tails:
            is_jump = rng.random(N_FULL) < jump_prob
            eta = np.where(is_jump,
                           rng.normal(0, se_het * jump_scale),
                           rng.normal(0, se_het))
        else:
            eta = rng.normal(0, se_het)

        # ARCH scaling
        if use_arch:
            arch_var = (1 - alpha_arch) + alpha_arch * last_z_sq
            arch_sc = np.sqrt(np.clip(arch_var, 0.1, 10.0))
        else:
            arch_sc = np.ones(N_FULL)

        # Transitory innovations
        if use_heavy_tails:
            if use_rank_dep_tdf:
                if use_calibrated_tdf:
                    df_vec = get_tdf_calibrated(cr)
                else:
                    df_vec = get_tdf(cr)
                t_raw = sp_stats.t.rvs(df=df_vec, random_state=rng)
                t_vf = np.sqrt(np.maximum(df_vec - 2, 0.5) / df_vec)
            else:
                t_raw = sp_stats.t.rvs(df=t_df_global, size=N_FULL, random_state=rng)
                t_vf = np.sqrt(max(t_df_global - 2, 0.5) / t_df_global)
            nu = sn_het * t_vf * arch_sc * t_raw
        else:
            nu = sn_het * arch_sc * rng.normal(0, 1, N_FULL)

        c_state = phi_v * c_state + nu

        # ARCH state update
        if use_arch:
            last_z_sq = np.clip(nu ** 2 / (sn_het ** 2 + 1e-10), 0, 4.0)

        # Mean reversion
        current_mean = np.mean(tau)
        if kb > 0:
            if use_rank_dep_kappa:
                kappa_r = kb * (cr / N_FULL) ** alpha_kappa
            else:
                kappa_r = kappa_global
            tau += eta - kappa_r * (tau - current_mean)
        else:
            tau += eta

        xsec_vars.append(np.var(tau))

        # Entry/exit — only during recording
        t_rec = t_abs - t_burnin
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
                c_state[exi] = rng.normal(0, 0.3, n_ex)
                het_multiplier[exi] = np.clip(
                    np.exp(rng.normal(0, sigma_het, n_ex)), 0.15, 8.0)
                last_z_sq[exi] = 1.0
                ep_type[exi] = 1
                endpoint_id[exi] = np.arange(next_id, next_id+n_ex)
                next_id += n_ex

        log_y_obs = tau + c_state + rng.normal(0, sigma_obs, N_FULL)
        order = np.argsort(-np.exp(log_y_obs))
        ranks = np.empty(N_FULL, dtype=int); ranks[order] = np.arange(1, N_FULL+1)

        if t_rec == 0:
            sim_ly[0] = log_y_obs; sim_rk[0] = ranks
            sim_ids[0] = endpoint_id.copy()
        elif 0 < t_rec < T_SIM:
            sim_ly[t_rec] = log_y_obs; sim_rk[t_rec] = ranks
            sim_ids[t_rec] = endpoint_id.copy()

    # --- Balanced panel ---
    init_ids = set(sim_ids[0])
    survivors = init_ids.copy()
    for t in range(1, T_SIM):
        survivors &= set(sim_ids[t])
    survivor_list = sorted(survivors)
    N_BP = len(survivor_list)

    bp_ly = np.zeros((T_SIM, N_BP))
    bp_rk = np.zeros((T_SIM, N_BP), dtype=int)
    for t in range(T_SIM):
        id_map = {eid: idx for idx, eid in enumerate(sim_ids[t])}
        for j, sid in enumerate(survivor_list):
            bp_ly[t, j] = sim_ly[t, id_map[sid]]
            bp_rk[t, j] = sim_rk[t, id_map[sid]]

    # --- Compute diagnostics ---
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

    for k in [1, 4, 13]:
        if k < T_SIM:
            t0s = set(np.where(sim_rk[0] <= 100)[0])
            tks = set(np.where(sim_rk[k] <= 100)[0])
            diag[f'pers{k}'] = len(t0s & tks)

    for k in [1, 4, 13]:
        if k < T_SIM:
            diag[f'xr2_{k}'] = np.corrcoef(bp_ly[0], bp_ly[k])[0, 1] ** 2

    sim_ch_flat = sim_ch.values.flatten()
    sim_ch_flat = sim_ch_flat[np.isfinite(sim_ch_flat)]
    diag['kurtosis'] = sp_stats.kurtosis(sim_ch_flat, fisher=True)
    diag['xsec_var_drift'] = xsec_vars[-1] / max(xsec_vars[t_burnin], 0.01)

    return diag


# ============================================================
# ABLATION LEVELS
# ============================================================

ablation_levels = [
    {
        'name': '1. Base (PT+Gauss)',
        'short': 'Base',
        'config': {
            'burn_in': False, 'kappa': False, 'rank_dep_kappa': False,
            'kappa_stab': False, 'heavy_tails': False, 'arch': False,
            'rank_dep_tdf': False, 'calibrated_tdf': False,
        },
    },
    {
        'name': '2. +Burn-in',
        'short': '+Burn-in',
        'config': {
            'burn_in': True, 'kappa': False, 'rank_dep_kappa': False,
            'kappa_stab': False, 'heavy_tails': False, 'arch': False,
            'rank_dep_tdf': False, 'calibrated_tdf': False,
        },
    },
    {
        'name': '3. +kappa (global)',
        'short': '+kappa',
        'config': {
            'burn_in': True, 'kappa': True, 'rank_dep_kappa': False,
            'kappa_stab': False, 'heavy_tails': False, 'arch': False,
            'rank_dep_tdf': False, 'calibrated_tdf': False,
        },
    },
    {
        'name': '4. +kappa(r)',
        'short': '+kappa(r)',
        'config': {
            'burn_in': True, 'kappa': True, 'rank_dep_kappa': True,
            'kappa_stab': False, 'heavy_tails': False, 'arch': False,
            'rank_dep_tdf': False, 'calibrated_tdf': False,
        },
    },
    {
        'name': '5. +Heavy tails',
        'short': '+Tails',
        'config': {
            'burn_in': True, 'kappa': True, 'rank_dep_kappa': True,
            'kappa_stab': False, 'heavy_tails': True, 'arch': False,
            'rank_dep_tdf': False, 'calibrated_tdf': False,
        },
    },
    {
        'name': '6. +ARCH(1)',
        'short': '+ARCH',
        'config': {
            'burn_in': True, 'kappa': True, 'rank_dep_kappa': True,
            'kappa_stab': False, 'heavy_tails': True, 'arch': True,
            'rank_dep_tdf': False, 'calibrated_tdf': False,
        },
    },
    {
        'name': '7. +Rank-dep t_df',
        'short': '+Rank-tdf',
        'config': {
            'burn_in': True, 'kappa': True, 'rank_dep_kappa': True,
            'kappa_stab': False, 'heavy_tails': True, 'arch': True,
            'rank_dep_tdf': True, 'calibrated_tdf': True,
        },
    },
    {
        'name': '8. +kappa-stab (v3.9)',
        'short': 'Full v3.9',
        'config': {
            'burn_in': True, 'kappa': True, 'rank_dep_kappa': True,
            'kappa_stab': True, 'heavy_tails': True, 'arch': True,
            'rank_dep_tdf': True, 'calibrated_tdf': True,
        },
    },
]

# Diagnostic thresholds (identical to v3.9)
diag_names = [
    'VR(2)', 'VR(4)', 'VR(8)', 'VR(13)',
    'ACF(1)', 'ACF(2)',
    'RACF(1)', 'RACF(4)', 'RACF(13)',
    'Pers(1)', 'Pers(4)', 'Pers(13)',
    'R²(1)', 'R²(4)', 'R²(13)',
]
diag_keys = [
    'vr2', 'vr4', 'vr8', 'vr13',
    'acf1', 'acf2',
    'racf1', 'racf4', 'racf13',
    'pers1', 'pers4', 'pers13',
    'xr2_1', 'xr2_4', 'xr2_13',
]
emp_vals = [
    vr_emp[2], vr_emp[4], vr_emp[8], vr_emp[13],
    acf_emp[1], acf_emp[2],
    racf_emp[1], racf_emp[4], racf_emp[13],
    pers_emp[1], pers_emp[4], pers_emp[13],
    xr2_emp[1], xr2_emp[4], xr2_emp[13],
]

def passes(key, sim_val, emp_val):
    """Check if a diagnostic passes its threshold."""
    if key.startswith('vr'):
        return abs(sim_val - emp_val) / emp_val < 0.20
    elif key.startswith('pers'):
        return abs(sim_val - emp_val) < 10
    else:  # acf, racf, xr2
        return abs(sim_val - emp_val) < 0.08


# ============================================================
# RUN ABLATION
# ============================================================

N_REP = 15  # 15 reps per level for reliable pass/fail on marginal diagnostics
seeds = [42] + list(range(100, 100 + N_REP - 1))

print(f"\n{'='*70}")
print(f"RUNNING ABLATION STUDY — {len(ablation_levels)} levels × {N_REP} reps")
print(f"{'='*70}")

# Results storage: level -> {diag_key: [values across reps]}
all_results = []

for lvl_idx, level in enumerate(ablation_levels):
    print(f"\n  Level {lvl_idx+1}: {level['name']}")
    level_diags = []
    for ri, seed in enumerate(seeds):
        t0 = time.time()
        diag = run_ablation_sim(seed, level['config'])
        level_diags.append(diag)
        if ri == 0 or ri == N_REP - 1:
            print(f"    Rep {ri+1}/{N_REP}: {time.time()-t0:.1f}s")

    # Aggregate
    mc = {}
    for key in diag_keys:
        vals = [d[key] for d in level_diags if key in d]
        mc[key] = np.mean(vals) if vals else np.nan

    # Extra diagnostics
    mc['kurtosis'] = np.mean([d['kurtosis'] for d in level_diags])
    mc['xsec_var_drift'] = np.mean([d['xsec_var_drift'] for d in level_diags])

    # Pass/fail
    pf = {}
    for key, emp in zip(diag_keys, emp_vals):
        pf[key] = passes(key, mc[key], emp)
    mc['n_pass'] = sum(pf.values())
    mc['pass_fail'] = pf

    all_results.append({'level': level, 'mc': mc})

    # Print summary line
    score = mc['n_pass']
    fails = [diag_names[i] for i, k in enumerate(diag_keys) if not pf[k]]
    fail_str = ', '.join(fails) if fails else '(none)'
    print(f"    Score: {score}/15  |  Fails: {fail_str}")
    print(f"    Kurtosis: {mc['kurtosis']:.1f} (emp={emp_kurt:.1f})  "
          f"Var drift: {mc['xsec_var_drift']:.2f}")


# ============================================================
# SUMMARY TABLE
# ============================================================
print(f"\n\n{'='*70}")
print("ABLATION SUMMARY TABLE")
print(f"{'='*70}")

# Header
hdr = f"{'Level':<24s} {'Score':>5s}"
for dn in diag_names:
    hdr += f" {dn:>7s}"
hdr += f" {'Kurt':>6s} {'VarDr':>6s}"
print(hdr)
print("-" * len(hdr))

for res in all_results:
    mc = res['mc']
    pf = mc['pass_fail']
    row = f"{res['level']['short']:<24s} {mc['n_pass']:>2d}/15"
    for key in diag_keys:
        mark = "  Y" if pf[key] else " *N"
        row += f" {mark:>7s}"
    row += f" {mc['kurtosis']:>6.1f} {mc['xsec_var_drift']:>6.2f}"
    print(row)

# Show which diagnostics each feature FIXES (first level where it passes)
print(f"\n\n{'='*70}")
print("FEATURE CONTRIBUTION: which diagnostics each feature first fixes")
print(f"{'='*70}")

for i in range(1, len(all_results)):
    prev_pf = all_results[i-1]['mc']['pass_fail']
    curr_pf = all_results[i]['mc']['pass_fail']
    newly_passing = [diag_names[j] for j, k in enumerate(diag_keys)
                     if curr_pf[k] and not prev_pf[k]]
    newly_failing = [diag_names[j] for j, k in enumerate(diag_keys)
                     if not curr_pf[k] and prev_pf[k]]
    fixed_str = ', '.join(newly_passing) if newly_passing else '(none)'
    broke_str = ', '.join(newly_failing) if newly_failing else '(none)'
    delta = all_results[i]['mc']['n_pass'] - all_results[i-1]['mc']['n_pass']
    print(f"  {all_results[i]['level']['name']}")
    print(f"    Fixed: {fixed_str}  |  Broke: {broke_str}  |  Delta: {delta:+d}")


# ============================================================
# DETAILED ERROR TABLE
# ============================================================
print(f"\n\n{'='*70}")
print("DETAILED ERRORS (sim_mean - emp for absolute; pct for VR)")
print(f"{'='*70}")

for res in all_results:
    mc = res['mc']
    print(f"\n  {res['level']['name']}  [{mc['n_pass']}/15]")
    for key, dn, emp in zip(diag_keys, diag_names, emp_vals):
        sim = mc[key]
        if key.startswith('vr'):
            err_str = f"{(sim-emp)/emp*100:+.1f}%"
        elif key.startswith('pers'):
            err_str = f"{sim-emp:+.1f}"
        else:
            err_str = f"{sim-emp:+.4f}"
        mark = "Y" if mc['pass_fail'][key] else "N"
        print(f"    {dn:>8s}: emp={emp:.4f}  sim={sim:.4f}  err={err_str:>8s}  [{mark}]")


# ============================================================
# ABLATION FIGURE
# ============================================================
print(f"\n\nGenerating ablation figure...")

fig, axes = plt.subplots(1, 2, figsize=(16, 7), gridspec_kw={'width_ratios': [3, 1]})

# Left panel: pass/fail heatmap
ax = axes[0]
n_levels = len(all_results)
n_diags = len(diag_names)
heatmap_data = np.zeros((n_levels, n_diags))
for i, res in enumerate(all_results):
    for j, key in enumerate(diag_keys):
        heatmap_data[i, j] = 1.0 if res['mc']['pass_fail'][key] else 0.0

cmap = ListedColormap(['#d32f2f', '#4caf50'])  # red=fail, green=pass
im = ax.imshow(heatmap_data, aspect='auto', cmap=cmap, vmin=0, vmax=1,
               interpolation='nearest')

# Labels
ax.set_xticks(range(n_diags))
ax.set_xticklabels(diag_names, rotation=45, ha='right', fontsize=8)
ax.set_yticks(range(n_levels))
level_labels = [res['level']['short'] for res in all_results]
ax.set_yticklabels(level_labels, fontsize=9)

# Annotate cells with Y/N
for i in range(n_levels):
    for j in range(n_diags):
        txt = 'Y' if heatmap_data[i, j] > 0.5 else 'N'
        color = 'white'
        ax.text(j, i, txt, ha='center', va='center', fontsize=7,
                fontweight='bold', color=color)

# Score annotations on right edge
for i, res in enumerate(all_results):
    score = res['mc']['n_pass']
    ax.text(n_diags + 0.3, i, f"{score}/15", ha='left', va='center',
            fontsize=9, fontweight='bold')

ax.set_title('Ablation: Diagnostic Pass/Fail by Model Level', fontsize=12, pad=10)
ax.set_xlabel('Diagnostic')
ax.set_ylabel('Model Level (cumulative features)')

# Right panel: score trajectory
ax2 = axes[1]
scores = [res['mc']['n_pass'] for res in all_results]
y_pos = range(n_levels)
ax2.barh(y_pos, scores, color=['#4caf50' if s == 15 else '#ff9800' if s >= 12
                                 else '#d32f2f' for s in scores],
         height=0.7, edgecolor='white', linewidth=0.5)
ax2.set_yticks(y_pos)
ax2.set_yticklabels(['' for _ in y_pos])  # already labeled on left
ax2.set_xlim(0, 16)
ax2.set_xlabel('Diagnostics Passing (/15)')
ax2.set_title('Score', fontsize=12, pad=10)
ax2.axvline(x=15, color='green', linestyle='--', alpha=0.5, linewidth=1)
for i, s in enumerate(scores):
    ax2.text(s + 0.2, i, str(s), ha='left', va='center', fontsize=9)

ax2.invert_yaxis()
axes[0].invert_yaxis()

plt.tight_layout()
plt.savefig('/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/ablation_table.png',
            dpi=200, bbox_inches='tight')
print(f"  Saved: ablation_table.png")

elapsed = time.time() - t_start
print(f"\n{'='*70}")
print(f"ABLATION STUDY COMPLETE — {elapsed:.0f}s total")
print(f"{'='*70}")
