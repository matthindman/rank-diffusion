from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd

from .types import Config, DataBundle, EstimatedParams, FitResult, InitialParams, ThresholdModel


def _to_jsonable(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="list")
    if isinstance(value, pd.Series):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    return value


def save_fit_result(result: FitResult, out_dir: str | Path | None = None) -> Path:
    target = Path(out_dir or result.config.output_dir or Path("output") / "rankdiff")
    target.mkdir(parents=True, exist_ok=True)

    curves = pd.DataFrame(
        {
            "z_knots": result.params.z_knots,
            "sigma_eta": result.params.sigma_eta_curve,
            "phi": result.params.phi_curve,
            "sigma_nu": result.params.sigma_nu_curve,
            "kappa": result.params.kappa_curve,
            "t_df": result.params.t_df_curve,
        }
    )
    curves.to_csv(target / "curves.csv", index=False)

    empirical = result.data.empirical
    empirical_summary = {
        "counts_by_period": empirical["counts_by_period"],
        "top_k": empirical["top_k"],
        "vr_emp": empirical["vr_emp"],
        "acf_emp": empirical["acf_emp"],
        "racf_emp": empirical["racf_emp"],
        "pers_emp": empirical["pers_emp"],
        "xr2_emp": empirical["xr2_emp"],
        "zipf_slope": empirical["zipf_slope"],
        "emp_kurt": empirical["emp_kurt"],
        "emp_mean_var": empirical["emp_mean_var"],
        "emp_median_var": empirical["emp_median_var"],
        "xsec_var_emp": empirical["xsec_var_emp"],
        "window_turnover_n": empirical.get("window_turnover_n"),
        "window_turnover_rate": empirical.get("window_turnover_rate"),
        "window_turnover_count": empirical.get("window_turnover_count"),
    }

    initial_payload = asdict(result.initial)
    params_payload = asdict(result.params)
    for payload in (initial_payload, params_payload):
        if "metadata" in payload:
            payload["metadata"] = _to_jsonable(payload["metadata"])

    payload = {
        "config": _to_jsonable(asdict(result.config)),
        "data_summary": {
            "platform": result.data.platform,
            "cadence": result.data.cadence,
            "n_periods": result.data.n_periods,
            "n_entities": result.data.n_entities,
            "mean_n": result.data.mean_n,
            "max_n": result.data.max_n,
            "threshold_by_period": result.data.threshold.threshold_by_period.tolist(),
            "max_missing_value_by_period": result.data.threshold.max_missing_value_by_period.tolist(),
            "empirical": _to_jsonable(empirical_summary),
        },
        "initial": _to_jsonable(initial_payload),
        "params": _to_jsonable(params_payload),
        "diagnostics": _to_jsonable(result.diagnostics),
    }
    with (target / "fit_result.json").open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return target


_INITIAL_ARRAY_FIELDS = {"z_knots", "sigma_eta_anchor", "phi_anchor", "sigma_nu_anchor", "t_df_anchor"}
_PARAMS_ARRAY_FIELDS = {
    "z_knots", "sigma_eta_curve", "phi_curve", "sigma_nu_curve",
    "kappa_curve", "t_df_curve", "t_df_curve_precal", "w0_sorted",
}
_THRESHOLD_ARRAY_FIELDS = {"threshold_by_period", "max_missing_value_by_period"}


def _reconstruct_arrays(d: dict, array_fields: set[str]) -> dict:
    out = {}
    for k, v in d.items():
        if k in array_fields and isinstance(v, list):
            out[k] = np.asarray(v, dtype=float)
        elif k in array_fields and v is None:
            out[k] = None
        elif k == "threshold" and isinstance(v, dict):
            out[k] = ThresholdModel(**_reconstruct_arrays(v, _THRESHOLD_ARRAY_FIELDS))
        elif k == "metadata" and isinstance(v, dict):
            out[k] = v
        else:
            out[k] = v
    return out


def load_fit_result(path: str | Path) -> FitResult:
    root = Path(path)
    if root.is_dir():
        json_path = root / "fit_result.json"
    else:
        json_path = root
        root = root.parent

    with json_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    cfg = Config(**payload["config"])
    threshold = ThresholdModel(
        threshold_by_period=np.asarray(payload["data_summary"]["threshold_by_period"], dtype=float),
        max_missing_value_by_period=np.asarray(payload["data_summary"]["max_missing_value_by_period"], dtype=float),
        effectively_exact_above_threshold=True,
    )
    data = DataBundle(
        panel=pd.DataFrame(),
        platform=payload["data_summary"]["platform"],
        cadence=payload["data_summary"]["cadence"],
        dates=pd.Index([]),
        n_periods=int(payload["data_summary"]["n_periods"]),
        n_entities=int(payload["data_summary"]["n_entities"]),
        mean_n=float(payload["data_summary"]["mean_n"]),
        max_n=int(payload["data_summary"]["max_n"]),
        balanced_ids=np.array([], dtype=str),
        tracked_entity_ids=np.array([], dtype=str),
        threshold=threshold,
        empirical=payload["data_summary"]["empirical"],
    )
    initial = InitialParams(**_reconstruct_arrays(payload["initial"], _INITIAL_ARRAY_FIELDS))
    params = EstimatedParams(**_reconstruct_arrays(payload["params"], _PARAMS_ARRAY_FIELDS))
    return FitResult(config=cfg, data=data, initial=initial, params=params, diagnostics=payload["diagnostics"])
