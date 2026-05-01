#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from rankdiff import (
    Config,
    build_data_bundle,
    estimate_initial_params,
    fit_parameter_curves,
    estimate_alpha_kappa,
    calibrate_kappa_stab,
    simulate_many,
    score_diagnostics,
    save_fit_result,
)
from rankdiff.types import FitResult
from rankdiff.plotting import plot_core_diagnostics, plot_ablation, plot_sensitivity
from rankdiff.ablation import run_ablation, format_ablation_summary
from rankdiff.sensitivity import run_sensitivity, format_sensitivity_summary


def write_json(obj, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)


def write_score_tables(score: dict, out_dir: Path) -> None:
    mc_rows = []
    for key, stats in sorted(score["mc_stats"].items()):
        mc_rows.append(
            {
                "metric": key,
                "mean": stats.get("mean"),
                "std": stats.get("std"),
                "lo": stats.get("lo"),
                "hi": stats.get("hi"),
            }
        )
    pd.DataFrame(mc_rows).to_csv(out_dir / "mc_diagnostics.csv", index=False)

    test_rows = [{"test": k, "pass": v} for k, v in score["tests"].items()]
    pd.DataFrame(test_rows).to_csv(out_dir / "diagnostic_tests.csv", index=False)

def run_one(data_path: Path, output_dir: Path, label: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    #exit_enabled = False if "submissions" in label else True # COMMENT OUT FOR PROD RUN

    cfg = Config(
        data_path=data_path,
        id_col="entity_id",
        timestamp_col="timestamp",
        metric_col="metric_value",
        rank_col=None,
        cadence="weekly",
        platform=f"reddit_{label}",
        fit_start="2024-04-01",
        fit_end="2026-02-28",
        output_dir=output_dir,

        # "prod"/full-scale run settings
        dev_mode=False,
        mc_reps=25,
        mc_reps_dev=5,
        n_jobs=1,
        random_seed=42,

        # tracking/calib
        track_entity_count=3000,
        calibration_track_entity_count=3000,
        max_dense_entities=50000,
        max_rank_filter=50000,

        # weekly panels
        min_presence_frac=0.90,

        # hp to allow turning exit/entry on/off for val
        #exit_enabled=exit_enabled,  # COMMENT OUT FOR PROD RUN
        
        # keep full diagnostic grid
        skip_plots=False,
    )

    print(f"\n===== {label.upper()} =====")
    print("STEP 1: building data bundle...")
    bundle = build_data_bundle(cfg)
    print(f"  n_periods={bundle.n_periods:,}")
    print(f"  n_entities={bundle.n_entities:,}")
    print(f"  balanced_ids={len(bundle.balanced_ids):,}")
    print(f"  tracked_ids={len(bundle.tracked_entity_ids):,}")

    print("\nSTEP 2: estimating initial parameters...")
    init = estimate_initial_params(bundle, cfg)

    print("\nSTEP 3: fitting parameter curves...")
    params = fit_parameter_curves(bundle, init, cfg)

    print("\nSTEP 4: calibrating alpha_kappa...")
    params = estimate_alpha_kappa(params, bundle, cfg)

    print("\nSTEP 5: calibrating kappa stability...")
    params = calibrate_kappa_stab(params, bundle, cfg)

    print("\nSTEP 6: running Monte Carlo simulations for diagnostics...")
    sims = simulate_many(params, bundle, cfg)
    sim_diags = [sim["diagnostics"] for sim in sims]
    score = score_diagnostics(bundle.empirical, sim_diags, cfg)

    print(f"  Diagnostics passing: {score['n_pass']}/{score['n_total']}")

    print("\nSTEP 7: saving fit result...")
    diagnostics_payload = {
        "status": "production_fit_completed",
        "platform": bundle.platform,
        "cadence": bundle.cadence,
        "n_periods": bundle.n_periods,
        "n_entities": bundle.n_entities,
        "balanced_ids": len(bundle.balanced_ids),
        "tracked_ids": len(bundle.tracked_entity_ids),
        "score_summary": {
            "n_pass": score["n_pass"],
            "n_total": score["n_total"],
            "pers_tolerance": score["pers_tolerance"],
        },
    }
    result = FitResult(
        config=cfg,
        data=bundle,
        initial=init,
        params=params,
        diagnostics=diagnostics_payload,
    )
    save_fit_result(result, output_dir)

    print("\nSTEP 8: writing detailed diagnostics tables...")
    write_json(score, output_dir / "diagnostics_score.json")
    write_score_tables(score, output_dir)

    print("\nSTEP 9: plotting core diagnostics...")
    plot_core_diagnostics(bundle, score, output_dir, prefix=label)

    print("\nSTEP 10: running ablation...")
    ablation_results = run_ablation(params, bundle, cfg)
    with (output_dir / "ablation_summary.txt").open("w", encoding="utf-8") as f:
        f.write(format_ablation_summary(ablation_results))
    write_json(ablation_results, output_dir / "ablation_results.json")
    plot_ablation(ablation_results, output_dir, prefix=label)

    print("\nSTEP 11: running sensitivity...")
    sensitivity_results = run_sensitivity(params, bundle, cfg)
    with (output_dir / "sensitivity_summary.txt").open("w", encoding="utf-8") as f:
        f.write(format_sensitivity_summary(sensitivity_results, params))
    write_json(sensitivity_results, output_dir / "sensitivity_results.json")
    plot_sensitivity(sensitivity_results, cfg.sensitivity_deltas, output_dir, prefix=label)

    print(f"\nDONE: {label}")
    print(f"Output directory: {output_dir}")

def main():
    weekly_dir = Path("/Users/philipwaggoner/Desktop/reddit_weekly") # CHANGE / NON-HARDCODE FOR FINAL VERSION
    base_out = Path("/Users/philipwaggoner/Desktop/rankdiff_output_production") # CHANGE / NON-HARDCODE FOR FINAL VERSION

    run_one(
        weekly_dir / "reddit_comments_weekly_panel.parquet",
        base_out / "reddit_comments_weekly_production",
        "reddit_comments_weekly",
    )
    
    run_one(
        weekly_dir / "reddit_submissions_weekly_panel.parquet",
        base_out / "reddit_submissions_weekly_production",
        "reddit_submissions_weekly",
    )

if __name__ == "__main__":
    main()