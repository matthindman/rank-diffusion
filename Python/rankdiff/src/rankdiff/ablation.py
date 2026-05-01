from __future__ import annotations

import numpy as np

from .diagnostics import score_diagnostics
from .simulator import simulate_many
from .types import Config, DataBundle, EstimatedParams, SimFeatures


ABLATION_LEVELS = [
    {
        "name": "1. Base (PT+Gauss)",
        "short": "Base",
        "features": SimFeatures(
            burn_in=False, kappa=False, rank_dep_kappa=False,
            kappa_stab=False, heavy_tails=False, arch=False,
            obs_noise=True, exit_entry=False, calibrated_tdf=False,
        ),
    },
    {
        "name": "2. +Burn-in",
        "short": "+Burn-in",
        "features": SimFeatures(
            burn_in=True, kappa=False, rank_dep_kappa=False,
            kappa_stab=False, heavy_tails=False, arch=False,
            obs_noise=True, exit_entry=False, calibrated_tdf=False,
        ),
    },
    {
        "name": "3. +kappa (global)",
        "short": "+kappa",
        "features": SimFeatures(
            burn_in=True, kappa=True, rank_dep_kappa=False,
            kappa_stab=False, heavy_tails=False, arch=False,
            obs_noise=True, exit_entry=False, calibrated_tdf=False,
        ),
    },
    {
        "name": "4. +kappa(r)",
        "short": "+kappa(r)",
        "features": SimFeatures(
            burn_in=True, kappa=True, rank_dep_kappa=True,
            kappa_stab=False, heavy_tails=False, arch=False,
            obs_noise=True, exit_entry=False, calibrated_tdf=False,
        ),
    },
    {
        "name": "5. +Heavy tails",
        "short": "+Tails",
        "features": SimFeatures(
            burn_in=True, kappa=True, rank_dep_kappa=True,
            kappa_stab=False, heavy_tails=True, arch=False,
            obs_noise=True, exit_entry=False, calibrated_tdf=False,
        ),
    },
    {
        "name": "6. +ARCH(1)",
        "short": "+ARCH",
        "features": SimFeatures(
            burn_in=True, kappa=True, rank_dep_kappa=True,
            kappa_stab=False, heavy_tails=True, arch=True,
            obs_noise=True, exit_entry=False, calibrated_tdf=False,
        ),
    },
    {
        "name": "7. +Calibrated t_df",
        "short": "+Cal-tdf",
        "features": SimFeatures(
            burn_in=True, kappa=True, rank_dep_kappa=True,
            kappa_stab=False, heavy_tails=True, arch=True,
            obs_noise=True, exit_entry=False, calibrated_tdf=True,
        ),
    },
    {
        "name": "8. +kappa-stab (full)",
        "short": "Full",
        "features": SimFeatures(
            burn_in=True, kappa=True, rank_dep_kappa=True,
            kappa_stab=True, heavy_tails=True, arch=True,
            obs_noise=True, exit_entry=True, calibrated_tdf=True,
        ),
    },
]

DIAG_NAMES = [
    "VR(2)", "VR(4)", "VR(8)", "VR(13)",
    "ACF(1)", "ACF(2)",
    "RACF(1)", "RACF(4)", "RACF(13)",
    "Pers(1)", "Pers(4)", "Pers(13)",
    "R2(1)", "R2(4)", "R2(13)",
]

DIAG_KEYS = [
    "vr2", "vr4", "vr8", "vr13",
    "acf1", "acf2",
    "racf1", "racf4", "racf13",
    "pers1", "pers4", "pers13",
    "xr2_1", "xr2_4", "xr2_13",
]


def _diag_passes(key: str, sim_val: float, emp_val: float, cfg: Config, top_k: int = 100) -> bool:
    if not np.isfinite(sim_val):
        return False
    if key.startswith("vr"):
        return abs(sim_val - emp_val) / max(abs(emp_val), 1e-6) < cfg.vr_threshold
    elif key.startswith("acf"):
        return abs(sim_val - emp_val) < cfg.acf_threshold
    elif key.startswith("racf"):
        return abs(sim_val - emp_val) < cfg.racf_threshold
    elif key.startswith("pers"):
        pers_tol = max(cfg.pers_threshold_min, int(round(cfg.pers_threshold_pct * top_k)))
        return abs(sim_val - emp_val) <= pers_tol
    elif key.startswith("xr2"):
        return abs(sim_val - emp_val) < cfg.r2_threshold
    else:
        return abs(sim_val - emp_val) < cfg.acf_threshold


def _get_emp_values(emp: dict) -> dict[str, float]:
    vals = {}
    for k in [2, 4, 8, 13]:
        vals[f"vr{k}"] = emp["vr_emp"].get(k, np.nan)
    for k in [1, 2]:
        vals[f"acf{k}"] = emp["acf_emp"].get(k, np.nan)
    for k in [1, 4, 13]:
        vals[f"racf{k}"] = emp["racf_emp"].get(k, np.nan)
    for k in [1, 4, 13]:
        vals[f"pers{k}"] = float(emp["pers_emp"].get(k, np.nan))
    for k in [1, 4, 13]:
        vals[f"xr2_{k}"] = emp["xr2_emp"].get(k, np.nan)
    return vals


def run_ablation(
    params: EstimatedParams,
    bundle: DataBundle,
    cfg: Config,
    levels: list[dict] | None = None,
) -> list[dict]:
    if levels is None:
        levels = ABLATION_LEVELS

    emp = bundle.empirical
    emp_vals = _get_emp_values(emp)
    top_k = int(emp.get("top_k", 100))
    results = []

    for lvl in levels:
        features = lvl["features"]
        sims = simulate_many(params, bundle, cfg, features=features)
        sim_diags = [sim["diagnostics"] for sim in sims]

        mc_means: dict[str, float] = {}
        for key in DIAG_KEYS:
            vals = np.array([d.get(key, np.nan) for d in sim_diags], dtype=float)
            vals = vals[np.isfinite(vals)]
            mc_means[key] = float(np.mean(vals)) if vals.size else np.nan

        for extra in ["kurtosis", "xsec_var_drift"]:
            vals = np.array([d.get(extra, np.nan) for d in sim_diags], dtype=float)
            vals = vals[np.isfinite(vals)]
            mc_means[extra] = float(np.mean(vals)) if vals.size else np.nan

        pass_fail = {}
        for key in DIAG_KEYS:
            ev = emp_vals.get(key, np.nan)
            sv = mc_means.get(key, np.nan)
            pass_fail[key] = _diag_passes(key, sv, ev, cfg, top_k=top_k)

        n_pass = sum(pass_fail.values())

        results.append({
            "level": lvl,
            "mc_means": mc_means,
            "pass_fail": pass_fail,
            "n_pass": n_pass,
            "n_total": len(DIAG_KEYS),
        })

    return results


def format_ablation_summary(results: list[dict]) -> str:
    lines = []
    hdr = f"{'Level':<24s} {'Score':>5s}"
    for dn in DIAG_NAMES:
        hdr += f" {dn:>7s}"
    hdr += f" {'Kurt':>6s} {'VarDr':>6s}"
    lines.append(hdr)
    lines.append("-" * len(hdr))

    for res in results:
        mc = res["mc_means"]
        pf = res["pass_fail"]
        row = f"{res['level']['short']:<24s} {res['n_pass']:>2d}/{res['n_total']:>2d}"
        for key in DIAG_KEYS:
            mark = "  Y" if pf[key] else " *N"
            row += f" {mark:>7s}"
        kurt = mc.get("kurtosis", np.nan)
        drift = mc.get("xsec_var_drift", np.nan)
        row += f" {kurt:>6.1f} {drift:>6.2f}"
        lines.append(row)

    lines.append("")
    lines.append("Feature Contribution:")
    for i in range(1, len(results)):
        prev_pf = results[i - 1]["pass_fail"]
        curr_pf = results[i]["pass_fail"]
        newly_passing = [DIAG_NAMES[j] for j, k in enumerate(DIAG_KEYS) if curr_pf[k] and not prev_pf[k]]
        newly_failing = [DIAG_NAMES[j] for j, k in enumerate(DIAG_KEYS) if not curr_pf[k] and prev_pf[k]]
        delta = results[i]["n_pass"] - results[i - 1]["n_pass"]
        fixed_str = ", ".join(newly_passing) if newly_passing else "(none)"
        broke_str = ", ".join(newly_failing) if newly_failing else "(none)"
        lines.append(f"  {results[i]['level']['name']}")
        lines.append(f"    Fixed: {fixed_str}  |  Broke: {broke_str}  |  Delta: {delta:+d}")

    return "\n".join(lines)
