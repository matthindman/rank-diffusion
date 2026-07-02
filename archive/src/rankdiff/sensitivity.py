from __future__ import annotations

from dataclasses import replace

import numpy as np

from .ablation import DIAG_KEYS, DIAG_NAMES, _diag_passes, _get_emp_values
from .simulator import simulate_many
from .types import Config, DataBundle, EstimatedParams

SENSITIVITY_PARAMS = [
    ("sigma_obs", "sigma_obs"),
    ("sigma_het", "sigma_het"),
    ("kappa_base", "kappa_base_raw"),
    ("alpha_kappa", "alpha_kappa"),
    ("alpha_arch", "alpha_arch"),
    ("t_df_global", "t_df_global"),
]


def _perturb_params(
    params: EstimatedParams,
    attr: str,
    delta: float,
) -> EstimatedParams:
    base_val = getattr(params, attr)
    new_val = base_val * (1.0 + delta)

    if attr == "kappa_base_raw":
        scale = new_val / max(base_val, 1e-12)
        return replace(
            params,
            kappa_base_raw=new_val,
            kappa_curve=params.kappa_curve * scale,
        )
    elif attr == "alpha_kappa":
        new_curve = params.kappa_base_raw * params.kappa_stab_factor * np.exp(new_val * params.z_knots)
        return replace(params, alpha_kappa=new_val, kappa_curve=new_curve)
    else:
        return replace(params, **{attr: new_val})


def run_sensitivity(
    params: EstimatedParams,
    bundle: DataBundle,
    cfg: Config,
    param_list: list[tuple[str, str]] | None = None,
    deltas: tuple[float, ...] | None = None,
) -> dict[str, dict[float, dict]]:
    if param_list is None:
        param_list = SENSITIVITY_PARAMS
    if deltas is None:
        deltas = cfg.sensitivity_deltas

    emp = bundle.empirical
    emp_vals = _get_emp_values(emp)
    top_k = int(emp.get("top_k", 100))
    all_results: dict[str, dict[float, dict]] = {}

    for pname, attr in param_list:
        base_val = getattr(params, attr)
        all_results[pname] = {}

        for delta in deltas:
            perturbed = _perturb_params(params, attr, delta)
            sims = simulate_many(perturbed, bundle, cfg)
            sim_diags = [sim["diagnostics"] for sim in sims]

            mc_means: dict[str, float] = {}
            for key in DIAG_KEYS:
                vals = np.array([d.get(key, np.nan) for d in sim_diags], dtype=float)
                vals = vals[np.isfinite(vals)]
                mc_means[key] = float(np.mean(vals)) if vals.size else np.nan

            pass_fail = {}
            for key in DIAG_KEYS:
                ev = emp_vals.get(key, np.nan)
                sv = mc_means.get(key, np.nan)
                pass_fail[key] = _diag_passes(key, sv, ev, cfg, top_k=top_k)

            n_pass = sum(pass_fail.values())
            all_results[pname][delta] = {
                "value": base_val * (1.0 + delta),
                "mc_means": mc_means,
                "pass_fail": pass_fail,
                "n_pass": n_pass,
                "n_total": len(DIAG_KEYS),
            }

    return all_results


def format_sensitivity_summary(
    results: dict[str, dict[float, dict]],
    params: EstimatedParams,
    deltas: tuple[float, ...] = (-0.20, -0.10, 0.0, 0.10, 0.20),
) -> str:
    lines = []
    hdr = f"{'Parameter':<14s}"
    for d in deltas:
        hdr += f" {d * 100:+5.0f}%"
    lines.append(hdr)
    lines.append("-" * len(hdr))

    for pname in results:
        row = f"{pname:<14s}"
        for d in deltas:
            entry = results[pname].get(d)
            if entry is None:
                row += "   -- "
            else:
                sc = entry["n_pass"]
                marker = "*" if sc < entry["n_total"] else " "
                row += f"  {sc:>2d}{marker} "
        lines.append(row)

    lines.append("")
    lines.append("Diagnostics failing at +/-20% perturbation:")
    for pname in results:
        fails_m = set()
        fails_p = set()
        entry_m = results[pname].get(-0.20)
        entry_p = results[pname].get(0.20)
        if entry_m:
            fails_m = {DIAG_NAMES[i] for i, k in enumerate(DIAG_KEYS) if not entry_m["pass_fail"][k]}
        if entry_p:
            fails_p = {DIAG_NAMES[i] for i, k in enumerate(DIAG_KEYS) if not entry_p["pass_fail"][k]}
        all_fails = sorted(fails_m | fails_p)
        lines.append(f"  {pname:<14s}: {', '.join(all_fails) if all_fails else '(robust to +/-20%)'}")

    return "\n".join(lines)
