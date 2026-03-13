from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats as sp_stats
from scipy.optimize import minimize

from .types import Config, DataBundle, InitialParams


def _adaptive_bin_count(mean_n: float, cfg: Config) -> int:
    raw = int(round(np.log2(max(mean_n, 512)) - 5))
    return int(np.clip(raw, cfg.min_anchor_bins, cfg.max_anchor_bins))


def build_anchor_bins(bundle: DataBundle, cfg: Config) -> pd.DataFrame:
    emp = bundle.empirical
    mean_rank = np.asarray(emp["mean_rank"], dtype=float)
    if mean_rank.size == 0:
        raise ValueError("No balanced tracked entities available for anchor construction.")

    n_bins = _adaptive_bin_count(bundle.mean_n, cfg)
    edges = np.exp(np.linspace(np.log(1.0), np.log(bundle.mean_n + 1.0), n_bins + 1)) - 1.0
    rank_lo = np.maximum(1, np.floor(edges[:-1]).astype(int))
    rank_hi = np.maximum(rank_lo, np.ceil(edges[1:]).astype(int))

    log_metric = emp["log_metric"]
    log_changes = emp["log_changes"]
    rank_wide = emp["rank_wide"]
    local_slope_mean = np.asarray(emp["local_slope_mean"], dtype=float)

    rows: list[dict[str, float]] = []
    for lo, hi in zip(rank_lo, rank_hi, strict=True):
        mask = (mean_rank >= lo) & (mean_rank <= hi)
        if mask.sum() < cfg.min_anchor_bin_size:
            continue

        cols = list(rank_wide.columns[mask])
        ch = log_changes[cols]
        lv = log_metric[cols]
        total_var = float(ch.var().median())
        vr4 = float((lv.diff(4).iloc[4:].var() / (4 * ch.var())).median()) if lv.shape[0] > 4 else np.nan
        vr13 = float((lv.diff(13).iloc[13:].var() / (13 * ch.var())).median()) if lv.shape[0] > 13 else np.nan
        acf1_vals = []
        for col in cols[: min(cfg.acf_sample_size, len(cols))]:
            arr = ch[col].dropna().to_numpy(dtype=float)
            if arr.size > 6 and np.std(arr[:-1]) > 1e-12 and np.std(arr[1:]) > 1e-12:
                acf1_vals.append(np.corrcoef(arr[:-1], arr[1:])[0, 1])
        band_changes = ch.to_numpy(dtype=float).ravel()
        band_changes = band_changes[np.isfinite(band_changes)]
        band_kurt = float(sp_stats.kurtosis(band_changes, fisher=True)) if band_changes.size > 20 else np.nan
        rows.append(
            {
                "rank_lo": float(lo),
                "rank_hi": float(hi),
                "rank_mid": float(np.sqrt(lo * hi)),
                "z_center": float(np.median(np.log(np.clip((mean_rank[mask] - 0.5) / bundle.mean_n, cfg.z_rank_clip, 1.0)))),
                "n_entities": float(mask.sum()),
                "total_var": total_var,
                "vr4": vr4,
                "vr13": vr13,
                "acf1": float(np.nanmedian(acf1_vals)) if acf1_vals else 0.0,
                "local_slope": float(np.nanmedian(local_slope_mean[mask])) if local_slope_mean.size else np.nan,
                "kurtosis": band_kurt,
            }
        )

    if not rows:
        raise ValueError("Unable to construct anchor bins with enough entities.")
    return pd.DataFrame(rows)


def _model_vr(k: int, se2: float, phi: float, sn2: float, sobs2: float) -> float:
    sc2 = sn2 / (1.0 - phi**2) if abs(phi) < 0.999 else sn2 * 1000.0
    vd = se2 + 2.0 * sc2 * (1.0 - phi) + 2.0 * sobs2
    if vd <= 0:
        return 1.0
    vk = k * se2 + 2.0 * sc2 * (1.0 - phi**k) + 2.0 * sobs2
    return vk / (k * vd)


def _model_acf1(se2: float, phi: float, sn2: float, sobs2: float) -> float:
    sc2 = sn2 / (1.0 - phi**2) if abs(phi) < 0.999 else sn2 * 1000.0
    vd = se2 + 2.0 * sc2 * (1.0 - phi) + 2.0 * sobs2
    if vd <= 0:
        return 0.0
    return (-sc2 * (1.0 - phi) ** 2 - sobs2) / vd


def _fit_band_params(emp_var: float, emp_vr4: float, emp_acf1: float, emp_vr13: float, sobs2: float, cfg: Config) -> tuple[float, float, float]:
    def objective(p: np.ndarray) -> float:
        se2 = np.exp(p[0])
        phi = 0.95 / (1.0 + np.exp(-p[1]))
        sn2 = np.exp(p[2])
        sc2 = sn2 / (1.0 - phi**2) if abs(phi) < 0.999 else sn2 * 1000.0
        mvar = se2 + 2.0 * sc2 * (1.0 - phi) + 2.0 * sobs2
        loss = 10.0 * (np.log(mvar) - np.log(emp_var)) ** 2
        loss += 5.0 * (_model_vr(4, se2, phi, sn2, sobs2) - emp_vr4) ** 2
        loss += 3.0 * (_model_acf1(se2, phi, sn2, sobs2) - emp_acf1) ** 2
        if np.isfinite(emp_vr13):
            loss += 2.0 * (_model_vr(13, se2, phi, sn2, sobs2) - emp_vr13) ** 2
        return float(loss)

    best = None
    rng = np.random.default_rng(cfg.random_seed)
    n_restarts = min(cfg.n_optim_restarts, 50) if cfg.dev_mode else cfg.n_optim_restarts
    for _ in range(n_restarts):
        x0 = np.array(
            [
                rng.uniform(-9, -1),
                rng.uniform(-2, 2),
                rng.uniform(-4, 1),
            ],
            dtype=float,
        )
        try:
            result = minimize(objective, x0, method="Nelder-Mead", options={"maxiter": 5000, "xatol": 1e-8, "fatol": 1e-10})
        except Exception:
            continue
        if best is None or result.fun < best.fun:
            best = result

    if best is None:
        raise RuntimeError("Band parameter optimization failed.")
    return float(np.exp(best.x[0])), float(0.95 / (1.0 + np.exp(-best.x[1]))), float(np.exp(best.x[2]))


def estimate_initial_params(bundle: DataBundle, cfg: Config) -> InitialParams:
    emp = bundle.empirical
    anchors = build_anchor_bins(bundle, cfg)

    log_changes: pd.DataFrame = emp["log_changes"]
    var_1: pd.Series = emp["var_1"]
    emp_median_var = float(emp["emp_median_var"])
    emp_mean_var = float(emp["emp_mean_var"])

    sample_cols = list(log_changes.columns[: min(cfg.acf_sample_size, log_changes.shape[1])])
    acf3_vals = []
    for col in sample_cols:
        arr = log_changes[col].dropna().to_numpy(dtype=float)
        if arr.size > 8:
            lag2 = np.corrcoef(arr[:-2], arr[2:])[0, 1] if np.std(arr[:-2]) > 1e-12 and np.std(arr[2:]) > 1e-12 else np.nan
            lag3 = np.corrcoef(arr[:-3], arr[3:])[0, 1] if np.std(arr[:-3]) > 1e-12 and np.std(arr[3:]) > 1e-12 else np.nan
            if np.isfinite(lag2) and np.isfinite(lag3):
                acf3_vals.append((lag2, lag3))

    if acf3_vals:
        acf2_ref = float(np.nanmedian([a for a, _ in acf3_vals]))
        acf3_ref = float(np.nanmedian([b for _, b in acf3_vals]))
    else:
        acf2_ref = float(emp["acf_emp"].get(2, 0.0))
        acf3_ref = 0.5 * acf2_ref

    if abs(acf2_ref) > 1e-3:
        phi_agg = acf3_ref / acf2_ref
    else:
        phi_agg = 0.5
    gamma1 = float(emp["acf_emp"].get(1, 0.0) * emp_median_var)
    gamma2 = float(emp["acf_emp"].get(2, 0.0) * emp_median_var)

    sigma2_obs_est = -gamma1 + gamma2 / phi_agg
    # Adaptive upper bound: obs noise cannot absorb more than max_noise_frac of
    # change variance. Prevents the pile-up problem where the permanent component
    # is estimated as zero (Stock & Watson 1998, Kamber, Morley & Wong 2018).
    sigma2_obs_upper = cfg.sigma_obs_bounds[1] ** 2
    if cfg.max_noise_frac < 1.0 and emp_median_var > 0:
        sigma2_obs_adaptive = cfg.max_noise_frac * emp_median_var / 2.0
        sigma2_obs_upper = min(sigma2_obs_upper, sigma2_obs_adaptive)
    sigma2_obs = float(np.clip(sigma2_obs_est, cfg.sigma_obs_bounds[0] ** 2, sigma2_obs_upper))
    sigma_obs = float(np.sqrt(sigma2_obs))
    sobs2 = sigma_obs**2

    var_ratio = emp_mean_var / max(emp_median_var, 1e-12)
    sigma_het = float(np.sqrt(max(np.log(max(var_ratio, 1.0)) / 2.0, 0.0)))

    standardized = []
    for col in sample_cols:
        arr = log_changes[col].dropna().to_numpy(dtype=float)
        if arr.size > 10:
            mu = arr.mean()
            sd = arr.std(ddof=1)
            if sd > 1e-8:
                standardized.append((arr - mu) / sd)
    z_within = np.concatenate(standardized) if standardized else np.array([0.0, 0.0])
    df_fit, _, scale_fit = sp_stats.t.fit(z_within)
    t_df_global = float(np.clip(df_fit, cfg.tdf_bounds[0], cfg.tdf_bounds[1]))

    obs_noise_var = 2.0 * sobs2
    t_df_anchor = []
    sigma_eta_anchor = []
    phi_anchor = []
    sigma_nu_anchor = []

    tracked_balanced_ids = np.asarray(emp["tracked_balanced_ids"], dtype=str)
    id_to_pos = {eid: idx for idx, eid in enumerate(tracked_balanced_ids)}
    for row in anchors.itertuples(index=False):
        mask = (emp["mean_rank"] >= row.rank_lo) & (emp["mean_rank"] <= row.rank_hi)
        cols = [tracked_balanced_ids[i] for i, keep in enumerate(mask) if keep and tracked_balanced_ids[i] in id_to_pos]
        if not cols:
            t_df_anchor.append(t_df_global)
            sigma_eta_anchor.append(np.nan)
            phi_anchor.append(np.nan)
            sigma_nu_anchor.append(np.nan)
            continue

        band_std = []
        for col in cols:
            arr = log_changes[col].dropna().to_numpy(dtype=float)
            if arr.size > 10:
                mu = arr.mean()
                sd = arr.std(ddof=1)
                if sd > 1e-8:
                    band_std.append((arr - mu) / sd)
        if band_std:
            z_band = np.concatenate(band_std)
            df_band, _, _ = sp_stats.t.fit(z_band)
            df_band = float(np.clip(df_band, cfg.tdf_bounds[0], cfg.tdf_bounds[1]))
        else:
            df_band = t_df_global

        signal_frac = max(0.05, 1.0 - obs_noise_var / max(row.total_var, obs_noise_var + 1e-8))
        if signal_frac < 0.30:
            df_band = float(np.clip(df_band / signal_frac, cfg.tdf_bounds[0], cfg.tdf_bounds[1]))
        t_df_anchor.append(df_band)

        se2, phi, sn2 = _fit_band_params(row.total_var, row.vr4, row.acf1, row.vr13, sobs2, cfg)
        # Enforce minimum permanent fraction (Harvey 1989, Stock & Watson 1998):
        # prevent the permanent component from piling up at zero.
        signal_var = max(row.total_var - obs_noise_var, 1e-8)
        se2_floor = cfg.min_perm_frac * signal_var
        if se2 < se2_floor:
            se2 = se2_floor
        sigma_eta_anchor.append(np.sqrt(se2))
        phi_anchor.append(phi)
        sigma_nu_anchor.append(np.sqrt(sn2))

    threshold = 4.0
    expected_tail = 2.0 * sp_stats.t.sf(threshold, df=t_df_global, loc=0.0, scale=scale_fit)
    actual_tail = float(np.mean(np.abs(z_within) > threshold * scale_fit))
    jump_prob = float(max(cfg.jump_prob_floor, actual_tail - expected_tail))

    extreme_mask = np.abs(z_within) > threshold * scale_fit
    if extreme_mask.sum() > 10 and (~extreme_mask).sum() > 10:
        jump_scale = float(np.std(z_within[extreme_mask]) / max(np.std(z_within[~extreme_mask]), 1e-8))
    else:
        jump_scale = 5.0

    z_sq_acfs = []
    for col in sample_cols:
        arr = log_changes[col].dropna().to_numpy(dtype=float)
        if arr.size > 15:
            mu = arr.mean()
            sd = arr.std(ddof=1)
            if sd > 1e-8:
                z_sq = ((arr - mu) / sd) ** 2
                centered = z_sq - z_sq.mean()
                denom = np.var(z_sq)
                if denom > 1e-12:
                    z_sq_acfs.append(float(np.sum(centered[:-1] * centered[1:]) / ((len(centered) - 1) * denom)))
    alpha_arch_raw = float(np.nanmedian(z_sq_acfs)) if z_sq_acfs else 0.1
    alpha_arch = float(np.clip(alpha_arch_raw, cfg.alpha_arch_bounds[0], cfg.alpha_arch_bounds[1]))

    e_h2 = float(np.exp(2.0 * sigma_het**2))
    jump_var_factor = (1.0 - jump_prob) + jump_prob * jump_scale**2
    mean_eta2 = float(e_h2 * np.nanmean(np.square(sigma_eta_anchor)) * jump_var_factor)
    w0_sorted = np.asarray(emp["w0_sorted"], dtype=float)
    init_mean = float(np.mean(w0_sorted))
    init_dev2 = np.square(w0_sorted - init_mean)
    init_ranks = np.arange(1, w0_sorted.size + 1)
    rank_weight = np.power(init_ranks / max(w0_sorted.size, 1), cfg.alpha_kappa_default)
    weighted_dev2 = float(np.mean(rank_weight * init_dev2))
    kappa_base_raw = max(mean_eta2 / max(2.0 * weighted_dev2, 1e-8), 1e-6)

    anchor_signal_frac = []
    for row in anchors.itertuples(index=False):
        sf = max(0.05, 1.0 - obs_noise_var / max(row.total_var, obs_noise_var + 1e-8))
        anchor_signal_frac.append(sf)

    return InitialParams(
        sigma_obs=sigma_obs,
        sigma_het=sigma_het,
        alpha_arch=alpha_arch,
        t_df_global=t_df_global,
        jump_prob=jump_prob,
        jump_scale=jump_scale,
        alpha_kappa=cfg.alpha_kappa_default,
        kappa_base_raw=kappa_base_raw,
        z_knots=anchors["z_center"].to_numpy(dtype=float),
        sigma_eta_anchor=np.asarray(sigma_eta_anchor, dtype=float),
        phi_anchor=np.asarray(phi_anchor, dtype=float),
        sigma_nu_anchor=np.asarray(sigma_nu_anchor, dtype=float),
        t_df_anchor=np.asarray(t_df_anchor, dtype=float),
        threshold=bundle.threshold,
        top_k=int(emp["top_k"]),
        metadata={
            "phi_agg": phi_agg,
            "sigma2_obs_est": sigma2_obs_est,
            "anchor_table": anchors,
            "anchor_kurtosis": anchors["kurtosis"].to_numpy(dtype=float),
            "anchor_signal_frac": np.asarray(anchor_signal_frac, dtype=float),
        },
    )
