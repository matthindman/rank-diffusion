from __future__ import annotations

from .types import Config, FitResult
from .preprocess import build_data_bundle
from .initializers import estimate_initial_params
from .fit import (
    fit_parameter_curves,
    estimate_alpha_kappa,
    calibrate_kappa_stab,
    calibrate_kurtosis,
)


def run_pipeline(cfg: Config) -> FitResult:
    """
    Run the end-to-end rankdiff pipeline.

    Parameter
    ----------
    cfg : Config
        Package configuration, including data path, column mappings,
        and modeling options.

    Return
    -------
    FitResult
        Object containing the processed data bundle, initial parameters,
        fitted parameters, and diagnostics.
    """
    data = build_data_bundle(cfg)
    initial = estimate_initial_params(data, cfg)
    params = fit_parameter_curves(data, initial, cfg)
    params = estimate_alpha_kappa(params, data, cfg)
    params = calibrate_kappa_stab(params, data, cfg)

    try:
        params = calibrate_kurtosis(params, data, cfg)
    except Exception:
        pass

    diagnostics = data.empirical

    return FitResult(
        config=cfg,
        data=data,
        initial=initial,
        params=params,
        diagnostics=diagnostics,
    )