from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

from .types import Config, ThresholdModel


def _safe_autocorr(x: np.ndarray, lag: int) -> float:
    if x.size <= lag + 5:
        return np.nan
    a = x[:-lag]
    b = x[lag:]
    if np.nanstd(a) < 1e-12 or np.nanstd(b) < 1e-12:
        return np.nan
    return float(np.corrcoef(a, b)[0, 1])


def _nanvar_ddof1(x: np.ndarray) -> np.ndarray:
    arr = np.asarray(x, dtype=float)
    if arr.ndim != 2:
        raise ValueError("_nanvar_ddof1 expects a 2-D array.")
    out = np.full(arr.shape[1], np.nan, dtype=float)
    valid = np.sum(np.isfinite(arr), axis=0) >= 2
    if valid.any():
        out[valid] = np.nanvar(arr[:, valid], axis=0, ddof=1)
    return out


def _make_wide(panel: pd.DataFrame, ids: np.ndarray, value_col: str) -> pd.DataFrame:
    if ids.size == 0:
        return pd.DataFrame()
    subset = panel[panel["entity_id"].isin(ids)]
    if subset.empty:
        return pd.DataFrame()
    wide = (
        subset.pivot(index="period_index", columns="entity_id", values=value_col)
        .sort_index()
        .reindex(columns=pd.Index(ids, dtype=str))
    )
    return wide


def compute_empirical_targets(
    panel: pd.DataFrame,
    balanced_ids: np.ndarray,
    tracked_ids: np.ndarray,
    threshold: ThresholdModel,
    cfg: Config,
) -> dict[str, object]:
    tracked_balanced = np.array([eid for eid in tracked_ids if eid in set(balanced_ids)], dtype=str)
    metric_wide = _make_wide(panel, tracked_balanced, "metric_value")
    rank_wide = _make_wide(panel, tracked_balanced, "rank")

    log_metric = np.log1p(metric_wide) if not metric_wide.empty else pd.DataFrame()
    log_changes = log_metric.diff().iloc[1:] if not log_metric.empty else pd.DataFrame()
    var_1 = log_changes.var() if not log_changes.empty else pd.Series(dtype=float)

    vr_emp: dict[int, float] = {}
    if not log_metric.empty and not var_1.empty:
        for k in cfg.vr_lags:
            if k < log_metric.shape[0]:
                numer = log_metric.diff(k).iloc[k:].var()
                vr_emp[k] = float((numer / (k * var_1)).median())

    acf_emp: dict[int, float] = {}
    racf_emp: dict[int, float] = {}
    sample_cols = list(log_changes.columns[: min(cfg.acf_sample_size, log_changes.shape[1])]) if not log_changes.empty else []
    for lag in cfg.acf_lags:
        vals = [_safe_autocorr(log_changes[col].dropna().to_numpy(dtype=float), lag) for col in sample_cols]
        vals = [v for v in vals if np.isfinite(v)]
        acf_emp[lag] = float(np.median(vals)) if vals else 0.0

    sample_rank_cols = list(rank_wide.columns[: min(cfg.acf_sample_size, rank_wide.shape[1])]) if not rank_wide.empty else []
    for lag in cfg.racf_lags:
        vals = [_safe_autocorr(rank_wide[col].dropna().to_numpy(dtype=float), lag) for col in sample_rank_cols]
        vals = [v for v in vals if np.isfinite(v)]
        racf_emp[lag] = float(np.median(vals)) if vals else 0.0

    counts = panel.groupby("period_index")["entity_id"].nunique().sort_index()
    mean_n = float(counts.mean())
    top_k = max(cfg.min_top_k, int(round(cfg.top_k_pct * mean_n)))
    window_turnover_n: int | None = None
    window_turnover_rate = np.nan
    window_turnover_count = np.nan

    if cfg.universe_mode == "topk_buffered" and cfg.top_k_focus is not None and cfg.buffer_k > 0:
        window_turnover_n = int(min(round(mean_n), cfg.top_k_focus + cfg.buffer_k))
        if window_turnover_n > 0:
            top_window = panel.loc[panel["rank"] <= window_turnover_n, ["period_index", "entity_id"]]
            period_sets = {int(period): set(group["entity_id"].astype(str)) for period, group in top_window.groupby("period_index")}
            turnover_counts: list[float] = []
            turnover_rates: list[float] = []
            for period_idx in range(int(counts.size) - 1):
                ids_now = period_sets.get(period_idx, set())
                ids_next = period_sets.get(period_idx + 1, set())
                base = min(window_turnover_n, len(ids_now), len(ids_next))
                if base <= 0:
                    continue
                overlap = len(ids_now & ids_next)
                turnover = max(base - overlap, 0)
                turnover_counts.append(float(turnover))
                turnover_rates.append(float(turnover / base))
            if turnover_rates:
                window_turnover_rate = float(np.mean(turnover_rates))
                window_turnover_count = float(np.mean(turnover_counts))

    period_entity_sets = {}
    for pidx in range(int(counts.size)):
        period_entity_sets[pidx] = set(panel.loc[panel["period_index"] == pidx, "entity_id"])
    exit_counts: list[int] = []
    for pidx in range(int(counts.size) - 1):
        ids_now = period_entity_sets[pidx]
        ids_next = period_entity_sets[pidx + 1]
        exit_counts.append(len(ids_now - ids_next))
    mean_exit_count = float(np.mean(exit_counts)) if exit_counts else 0.0
    mean_exit_rate = mean_exit_count / max(mean_n, 1.0)

    pers_emp: dict[int, int] = {}
    xr2_emp: dict[int, float] = {}
    if not panel.empty:
        period0 = panel[panel["period_index"] == 0].sort_values("rank")
        t0_ids = set(period0.loc[period0["rank"] <= top_k, "entity_id"])
        for k in cfg.pers_horizons:
            if k >= counts.size:
                continue
            tk = panel[panel["period_index"] == k].sort_values("rank")
            tk_ids = set(tk.loc[tk["rank"] <= top_k, "entity_id"])
            pers_emp[k] = len(t0_ids & tk_ids)
        for k in cfg.r2_horizons:
            if k >= log_metric.shape[0]:
                continue
            start = log_metric.iloc[0].to_numpy(dtype=float)
            end = log_metric.iloc[k].to_numpy(dtype=float)
            valid = np.isfinite(start) & np.isfinite(end)
            xr2_emp[k] = float(np.corrcoef(start[valid], end[valid])[0, 1] ** 2) if valid.sum() > 5 else np.nan

    period0 = panel[panel["period_index"] == 0].sort_values("rank")
    zipf_n = max(10, int(round(cfg.zipf_fit_fraction * len(period0))))
    zipf_subset = period0.iloc[:zipf_n]
    zipf_mask = zipf_subset["metric_value"] > 0
    if zipf_mask.sum() > 5:
        zipf_slope = float(
            np.polyfit(np.log(zipf_subset.loc[zipf_mask, "rank"]), np.log(zipf_subset.loc[zipf_mask, "metric_value"]), 1)[0]
        )
    else:
        zipf_slope = np.nan

    all_changes = log_changes.to_numpy(dtype=float).ravel() if not log_changes.empty else np.array([], dtype=float)
    all_changes = all_changes[np.isfinite(all_changes)]
    emp_kurt = float(sp_stats.kurtosis(all_changes, fisher=True, bias=False)) if all_changes.size > 20 else np.nan
    emp_mean_var = float(var_1.mean()) if not var_1.empty else np.nan
    emp_median_var = float(var_1.median()) if not var_1.empty else np.nan

    xsec_var_emp = (
        panel.assign(log_metric=np.log1p(panel["metric_value"]))
        .groupby("period_index")["log_metric"]
        .var()
        .mean()
    )

    mean_rank = rank_wide.mean(axis=0).sort_values() if not rank_wide.empty else pd.Series(dtype=float)
    z_rank_mean = np.log(np.clip((mean_rank.to_numpy(dtype=float) - 0.5) / mean_n, cfg.z_rank_clip, 1.0)) if mean_rank.size else np.array([], dtype=float)
    local_slope_mean = (
        panel[panel["entity_id"].isin(tracked_balanced)]
        .groupby("entity_id")["local_slope"]
        .mean()
        .reindex(mean_rank.index)
        .to_numpy(dtype=float)
        if mean_rank.size
        else np.array([], dtype=float)
    )

    # Common factor estimation: cross-sectional mean of log-changes per period
    cf_phi_est = 0.0
    cf_sigma_est = 0.0
    cf_r2_median = 0.0
    cf_loading_by_z: list[tuple[float, float]] = []
    if not log_changes.empty and log_changes.shape[1] > 10:
        F_t = log_changes.mean(axis=1)
        cf_sigma_est = float(F_t.std())
        if len(F_t) > 5:
            cf_phi_est = float(np.clip(np.corrcoef(F_t.values[:-1], F_t.values[1:])[0, 1], -0.99, 0.99))
        F_var = float(F_t.var())
        if F_var > 1e-12:
            betas = log_changes.apply(lambda col: float(col.cov(F_t)) / F_var)
            entity_r2 = (betas ** 2 * F_var) / var_1.clip(lower=1e-12)
            cf_r2_median = float(entity_r2.median())
            # Loadings by rank band: bin entities by z_rank_mean
            if z_rank_mean.size == betas.size:
                for z_val, beta_val in zip(z_rank_mean, betas.values):
                    if np.isfinite(z_val) and np.isfinite(beta_val):
                        cf_loading_by_z.append((float(z_val), float(beta_val)))

    return {
        "counts_by_period": counts.to_numpy(dtype=int),
        "top_k": top_k,
        "tracked_balanced_ids": tracked_balanced,
        "metric_wide": metric_wide,
        "rank_wide": rank_wide,
        "log_metric": log_metric,
        "log_changes": log_changes,
        "var_1": var_1,
        "vr_emp": vr_emp,
        "acf_emp": acf_emp,
        "racf_emp": racf_emp,
        "pers_emp": pers_emp,
        "xr2_emp": xr2_emp,
        "zipf_slope": zipf_slope,
        "emp_kurt": emp_kurt,
        "emp_mean_var": emp_mean_var,
        "emp_median_var": emp_median_var,
        "xsec_var_emp": float(xsec_var_emp),
        "w0_sorted": np.sort(np.log1p(period0["metric_value"].to_numpy(dtype=float)))[::-1],
        "mean_rank": mean_rank.to_numpy(dtype=float),
        "z_rank_mean": z_rank_mean,
        "local_slope_mean": local_slope_mean,
        "threshold_by_period": threshold.threshold_by_period,
        "window_turnover_n": window_turnover_n,
        "window_turnover_rate": window_turnover_rate,
        "window_turnover_count": window_turnover_count,
        "mean_exit_count": mean_exit_count,
        "mean_exit_rate": mean_exit_rate,
        "cf_phi": cf_phi_est,
        "cf_sigma": cf_sigma_est,
        "cf_r2_median": cf_r2_median,
        "cf_loading_by_z": cf_loading_by_z,
    }


def compute_sim_diagnostics(sim: dict[str, object], cfg: Config) -> dict[str, float]:
    tracked_values = np.asarray(sim["tracked_values"], dtype=float)
    tracked_ranks = np.asarray(sim["tracked_ranks"], dtype=float)

    observed_all = np.all(np.isfinite(tracked_values), axis=0)
    if observed_all.sum() >= 10:
        values = tracked_values[:, observed_all]
        ranks = tracked_ranks[:, observed_all]
    else:
        values = tracked_values
        ranks = tracked_ranks

    changes = np.diff(values, axis=0)
    var_1 = _nanvar_ddof1(changes)

    diag: dict[str, float] = {}
    for k in cfg.vr_lags:
        if k < values.shape[0]:
            numer = _nanvar_ddof1(values[k:] - values[:-k])
            valid = np.isfinite(var_1) & (var_1 > 1e-12) & np.isfinite(numer)
            if valid.any():
                diag[f"vr{k}"] = float(np.nanmedian(numer[valid] / (k * var_1[valid])))

    for lag in cfg.acf_lags:
        vals = [_safe_autocorr(changes[:, i][np.isfinite(changes[:, i])], lag) for i in range(min(changes.shape[1], cfg.acf_sample_size))]
        vals = [v for v in vals if np.isfinite(v)]
        diag[f"acf{lag}"] = float(np.median(vals)) if vals else np.nan

    for lag in cfg.racf_lags:
        vals = [_safe_autocorr(ranks[:, i][ranks[:, i] > 0], lag) for i in range(min(ranks.shape[1], cfg.acf_sample_size))]
        vals = [v for v in vals if np.isfinite(v)]
        diag[f"racf{lag}"] = float(np.median(vals)) if vals else np.nan

    top_ids = np.asarray(sim["top_ids"])
    for k in cfg.pers_horizons:
        if k < top_ids.shape[0]:
            t0_set = set(top_ids[0]) - {-1}
            tk_set = set(top_ids[k]) - {-1}
            diag[f"pers{k}"] = float(len(t0_set & tk_set))

    for k in cfg.r2_horizons:
        if k < values.shape[0]:
            start = values[0]
            end = values[k]
            valid = np.isfinite(start) & np.isfinite(end)
            diag[f"xr2_{k}"] = float(np.corrcoef(start[valid], end[valid])[0, 1] ** 2) if valid.sum() > 5 else np.nan

    flat_changes = changes.ravel()
    flat_changes = flat_changes[np.isfinite(flat_changes)]
    diag["kurtosis"] = float(sp_stats.kurtosis(flat_changes, fisher=True, bias=False)) if flat_changes.size > 20 else np.nan

    first_sorted = np.asarray(sim["period0_sorted_values"], dtype=float)
    zipf_n = max(10, int(round(cfg.zipf_fit_fraction * first_sorted.size)))
    zipf_vals = first_sorted[:zipf_n]
    if zipf_vals.size > 5:
        diag["zipf_slope"] = float(np.polyfit(np.log(np.arange(1, zipf_vals.size + 1)), zipf_vals, 1)[0])
    else:
        diag["zipf_slope"] = np.nan

    xsec_var = np.asarray(sim["xsec_var"], dtype=float)
    diag["xsec_var_start"] = float(xsec_var[0])
    diag["xsec_var_end"] = float(xsec_var[-1])
    diag["xsec_var_drift"] = float(xsec_var[-1] / max(xsec_var[0], 1e-8))
    diag["mean_observed_n"] = float(np.mean(sim["observed_counts"]))
    return diag


def score_diagnostics(emp: dict[str, object], sim_diags: Sequence[dict[str, float]], cfg: Config) -> dict[str, object]:
    mc_stats: dict[str, dict[str, float]] = {}
    keys = sorted({key for diag in sim_diags for key in diag.keys()})
    for key in keys:
        vals = np.array([diag.get(key, np.nan) for diag in sim_diags], dtype=float)
        vals = vals[np.isfinite(vals)]
        if vals.size == 0:
            continue
        mc_stats[key] = {
            "mean": float(np.mean(vals)),
            "std": float(np.std(vals, ddof=1)) if vals.size > 1 else 0.0,
            "lo": float(np.percentile(vals, 2.5)),
            "hi": float(np.percentile(vals, 97.5)),
        }

    tests: dict[str, bool] = {}
    for lag in cfg.vr_lags:
        key = f"vr{lag}"
        if key in mc_stats and lag in emp["vr_emp"]:
            tests[f"VR({lag})"] = abs(mc_stats[key]["mean"] - emp["vr_emp"][lag]) / emp["vr_emp"][lag] < cfg.vr_threshold

    for lag in cfg.acf_lags:
        key = f"acf{lag}"
        if key in mc_stats:
            tests[f"ACF({lag})"] = abs(mc_stats[key]["mean"] - emp["acf_emp"][lag]) < cfg.acf_threshold

    for lag in cfg.racf_lags:
        key = f"racf{lag}"
        if key in mc_stats:
            tests[f"RACF({lag})"] = abs(mc_stats[key]["mean"] - emp["racf_emp"][lag]) < cfg.racf_threshold

    pers_tol = max(cfg.pers_threshold_min, int(round(cfg.pers_threshold_pct * emp["top_k"])))
    for horizon in cfg.pers_horizons:
        key = f"pers{horizon}"
        if key in mc_stats and horizon in emp["pers_emp"]:
            tests[f"Pers({horizon})"] = abs(mc_stats[key]["mean"] - emp["pers_emp"][horizon]) <= pers_tol

    for horizon in cfg.r2_horizons:
        key = f"xr2_{horizon}"
        if key in mc_stats and horizon in emp["xr2_emp"]:
            tests[f"R2({horizon})"] = abs(mc_stats[key]["mean"] - emp["xr2_emp"][horizon]) < cfg.r2_threshold

    return {
        "mc_stats": mc_stats,
        "tests": tests,
        "n_pass": int(sum(tests.values())),
        "n_total": int(len(tests)),
        "pers_tolerance": pers_tol,
    }
