#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from rankdiff.diagnostics import score_diagnostics
from rankdiff.fit import calibrate_kappa_stab, calibrate_kurtosis, estimate_alpha_kappa, fit_parameter_curves
from rankdiff.initializers import estimate_initial_params
from rankdiff.io import save_fit_result
from rankdiff.preprocess import build_data_bundle
from rankdiff.simulator import simulate_many
from rankdiff.types import Config, FitResult


def main(cfg: Config) -> FitResult:
    print("=" * 70)
    print("BUILDING DATA BUNDLE")
    print("=" * 70)
    bundle = build_data_bundle(cfg)
    print(f"  Platform: {bundle.platform}")
    print(f"  Periods: {bundle.n_periods}, Entities: {bundle.n_entities}")
    print(f"  Mean N: {bundle.mean_n:.0f}, Balanced: {bundle.balanced_ids.size}")
    print(f"  Exit rate: {bundle.empirical.get('mean_exit_rate', 0):.4f}")

    print("\n" + "=" * 70)
    print("ESTIMATING INITIAL PARAMETERS")
    print("=" * 70)
    initial = estimate_initial_params(bundle, cfg)
    print(f"  sigma_obs: {initial.sigma_obs:.4f}")
    print(f"  sigma_het: {initial.sigma_het:.4f}")
    print(f"  t_df_global: {initial.t_df_global:.2f}")
    print(f"  kappa_base_raw: {initial.kappa_base_raw:.6f}")
    print(f"  jump_prob: {initial.jump_prob:.4f}")
    print(f"  alpha_arch: {initial.alpha_arch:.4f}")

    print("\n" + "=" * 70)
    print("FITTING PARAMETER CURVES")
    print("=" * 70)
    params = fit_parameter_curves(bundle, initial, cfg)
    print(f"  z_knots: {params.z_knots}")
    print(f"  sigma_eta_curve: {np.round(params.sigma_eta_curve, 4)}")
    print(f"  kappa_curve: {np.round(params.kappa_curve, 6)}")
    print(f"  exit_p_base: {params.exit_p_base:.6f}")

    print("\n" + "=" * 70)
    print("CALIBRATING alpha_kappa")
    print("=" * 70)
    params = estimate_alpha_kappa(params, bundle, cfg)
    print(f"  alpha_kappa: {params.alpha_kappa:.3f}")

    print("\n" + "=" * 70)
    print("CALIBRATING kappa_stab")
    print("=" * 70)
    params = calibrate_kappa_stab(params, bundle, cfg)
    print(f"  kappa_stab_factor: {params.kappa_stab_factor:.3f}")

    print("\n" + "=" * 70)
    print("CALIBRATING KURTOSIS")
    print("=" * 70)
    params = calibrate_kurtosis(params, bundle, cfg)
    print(f"  t_df_curve (post-cal): {np.round(params.t_df_curve, 2)}")

    print("\n" + "=" * 70)
    print(f"RUNNING MC SIMULATION ({cfg.resolved_mc_reps} reps)")
    print("=" * 70)
    sims = simulate_many(params, bundle, cfg)
    score = score_diagnostics(bundle.empirical, [sim["diagnostics"] for sim in sims], cfg)

    result = FitResult(
        config=cfg,
        data=bundle,
        initial=initial,
        params=params,
        diagnostics=score,
    )

    artifact_dir = save_fit_result(result, out_dir=cfg.output_dir or (ROOT / "output" / "rankdiff" / bundle.platform))
    print(f"\n  Score: {score['n_pass']}/{score['n_total']}")
    print(f"  Artifacts: {artifact_dir}")

    if not cfg.skip_plots:
        from rankdiff.plotting import plot_core_diagnostics
        plot_core_diagnostics(bundle, score, artifact_dir, bundle.platform)

    # Print scorecard
    print("\n" + "=" * 70)
    print("DIAGNOSTIC SCORECARD")
    print("=" * 70)
    tests = score["tests"]
    mc = score["mc_stats"]
    emp = bundle.empirical
    for name, passed in tests.items():
        mark = "PASS" if passed else "FAIL"
        key = name.lower().replace("(", "").replace(")", "")
        key = key.replace("vr", "vr").replace("acf", "acf").replace("racf", "racf")
        key = key.replace("pers", "pers").replace("r2", "xr2_")
        sim_val = mc.get(key, {}).get("mean", float("nan")) if key in mc else float("nan")
        print(f"  [{mark:>4s}] {name:<10s}  sim={sim_val:.3f}" if np.isfinite(sim_val) else f"  [{mark:>4s}] {name:<10s}")
    print(f"\n  Total: {score['n_pass']}/{score['n_total']}")

    # Ablation study
    print("\n" + "=" * 70)
    print("ABLATION STUDY")
    print("=" * 70)
    from rankdiff.ablation import format_ablation_summary, run_ablation

    abl_results = run_ablation(params, bundle, cfg)
    print(format_ablation_summary(abl_results))
    if not cfg.skip_plots:
        from rankdiff.plotting import plot_ablation
        plot_ablation(abl_results, artifact_dir, bundle.platform)
        print(f"  Saved ablation figure to {artifact_dir}")

    # Sensitivity analysis
    print("\n" + "=" * 70)
    print("SENSITIVITY ANALYSIS")
    print("=" * 70)
    from rankdiff.sensitivity import format_sensitivity_summary, run_sensitivity

    sens_results = run_sensitivity(params, bundle, cfg)
    print(format_sensitivity_summary(sens_results, params, cfg.sensitivity_deltas))
    if not cfg.skip_plots:
        from rankdiff.plotting import plot_sensitivity
        plot_sensitivity(sens_results, cfg.sensitivity_deltas, artifact_dir, bundle.platform)
        print(f"  Saved sensitivity figure to {artifact_dir}")

    summary = {
        "platform": bundle.platform,
        "cadence": bundle.cadence,
        "n_pass": score["n_pass"],
        "n_total": score["n_total"],
        "artifact_dir": str(artifact_dir),
        "top_k": bundle.empirical["top_k"],
        "mean_n": bundle.mean_n,
        "kappa_stab_factor": params.kappa_stab_factor,
        "alpha_kappa": params.alpha_kappa,
        "exit_p_base": params.exit_p_base,
    }
    print("\n" + json.dumps(summary, indent=2))
    return result


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Run rankdiff v43 fit.")
    parser.add_argument("data_path", help="Path to parquet input")
    parser.add_argument("--id-col", default="endpoint_id")
    parser.add_argument("--timestamp-col", default="date")
    parser.add_argument("--metric-col", default="metric_value")
    parser.add_argument("--rank-col", default="rank")
    parser.add_argument("--platform", default="auto")
    parser.add_argument("--cadence", default="auto", choices=["auto", "daily", "weekly"])
    parser.add_argument("--universe-mode", default="full", choices=["full", "topk_buffered"])
    parser.add_argument("--top-k-focus", type=int, default=None)
    parser.add_argument("--buffer-k", type=int, default=0)
    parser.add_argument("--simulate-periods", type=int, default=None)
    parser.add_argument("--burnin-periods", type=int, default=None)
    parser.add_argument("--calibration-periods", type=int, default=None)
    parser.add_argument("--mc-reps", type=int, default=25)
    parser.add_argument("--n-jobs", type=int, default=1)
    parser.add_argument("--track-entity-count", type=int, default=5000)
    parser.add_argument("--calibration-track-entity-count", type=int, default=None)
    parser.add_argument("--dev-mode", action="store_true")
    parser.add_argument("--no-obs-noise", action="store_true", help="Disable observation noise")
    parser.add_argument("--no-exit", action="store_true", help="Disable entry/exit process")
    parser.add_argument("--max-rank-filter", type=int, default=None,
                        help="Keep only top-K entities per period (by metric_value rank)")
    parser.add_argument("--min-presence-frac", type=float, default=1.0,
                        help="Balanced panel requires presence in this fraction of periods (default: 1.0 = all)")
    parser.add_argument("--max-noise-frac", type=float, default=0.50,
                        help="Max fraction of change variance attributable to obs noise (default: 0.50)")
    parser.add_argument("--skip-plots", action="store_true")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    return Config(
        data_path=args.data_path,
        id_col=args.id_col,
        timestamp_col=args.timestamp_col,
        metric_col=args.metric_col,
        rank_col=args.rank_col,
        platform=args.platform,
        cadence=args.cadence,
        universe_mode=args.universe_mode,
        top_k_focus=args.top_k_focus,
        buffer_k=args.buffer_k,
        simulate_periods=args.simulate_periods,
        burnin_periods=args.burnin_periods,
        calibration_periods=args.calibration_periods,
        mc_reps=args.mc_reps,
        n_jobs=args.n_jobs,
        track_entity_count=args.track_entity_count,
        calibration_track_entity_count=args.calibration_track_entity_count,
        dev_mode=args.dev_mode,
        use_obs_noise=not args.no_obs_noise,
        exit_enabled=not args.no_exit,
        max_rank_filter=args.max_rank_filter,
        min_presence_frac=args.min_presence_frac,
        max_noise_frac=args.max_noise_frac,
        skip_plots=args.skip_plots,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main(parse_args())
