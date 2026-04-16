#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from rankdiff import (
    Config,
    build_data_bundle,
    estimate_initial_params,
    fit_parameter_curves,
    estimate_alpha_kappa,
    calibrate_kappa_stab,
    save_fit_result,
)
from rankdiff.types import FitResult


def main():
    # ------------------------------------------------------
    # CHANGE ONLY THESE LINES WHEN SWITCHING DATASETS: comments vs. submissions (will clean up / not hardcode paths for final version)
    # ------------------------------------------------------
    #data_path = Path("/Users/philipwaggoner/Desktop/reddit_merged/reddit_comments_daily_panel.parquet")
    #output_dir = Path("/Users/philipwaggoner/Desktop/rankdiff_output/reddit_comments_fit")

    data_path = Path("/Users/philipwaggoner/Desktop/reddit_merged/reddit_submissions_daily_panel.parquet")
    output_dir = Path("/Users/philipwaggoner/Desktop/rankdiff_output/reddit_submissions_fit")
    # ------------------------------------------------------

    cfg = Config(
        data_path=data_path,
        id_col="entity_id",
        timestamp_col="timestamp",
        metric_col="metric_value",
        rank_col=None,
        cadence="daily",
        platform="reddit",
        fit_start="2024-04-01",
        fit_end="2026-02-28",
        output_dir=output_dir,

        dev_mode=True,
        mc_reps=5,
        mc_reps_dev=3,
        n_jobs=1,
        random_seed=42,

        track_entity_count=1000,
        max_dense_entities=25000,
        calibration_track_entity_count=1000,
        max_rank_filter=50000,
        min_presence_frac=1.0,
    )

    print("STEP 1: building data bundle...")
    bundle = build_data_bundle(cfg)
    print(f"  n_periods={bundle.n_periods:,}")
    print(f"  n_entities={bundle.n_entities:,}")
    print(f"  balanced_ids={len(bundle.balanced_ids):,}")
    print(f"  tracked_ids={len(bundle.tracked_entity_ids):,}")

    print("\nSTEP 2: estimating initial parameters...")
    init = estimate_initial_params(bundle, cfg)
    print(f"  sigma_obs={init.sigma_obs:.4f}")
    print(f"  sigma_het={init.sigma_het:.4f}")
    print(f"  alpha_arch={init.alpha_arch:.4f}")
    print(f"  t_df_global={init.t_df_global:.4f}")
    print(f"  jump_prob={init.jump_prob:.4f}")
    print(f"  jump_scale={init.jump_scale:.4f}")
    print(f"  alpha_kappa={init.alpha_kappa:.4f}")
    print(f"  kappa_base_raw={init.kappa_base_raw:.6f}")
    print(f"  top_k={init.top_k}")

    print("\nSTEP 3: fitting parameter curves...")
    params = fit_parameter_curves(bundle, init, cfg)
    print(f"  burnin_periods={params.burnin_periods}")
    print(f"  n_full={params.n_full:,}")

    print("\nSTEP 4: calibrating alpha_kappa...")
    params = estimate_alpha_kappa(params, bundle, cfg)
    print(f"  alpha_kappa={params.alpha_kappa:.4f}")

    print("\nSTEP 5: calibrating kappa stability...")
    params = calibrate_kappa_stab(params, bundle, cfg)
    print(f"  kappa_stab_factor={params.kappa_stab_factor:.4f}")

    diagnostics = {
        "status": "fit_completed",
        "platform": bundle.platform,
        "cadence": bundle.cadence,
        "n_periods": bundle.n_periods,
        "n_entities": bundle.n_entities,
        "balanced_ids": len(bundle.balanced_ids),
        "tracked_ids": len(bundle.tracked_entity_ids),
    }

    result = FitResult(
        config=cfg,
        data=bundle,
        initial=init,
        params=params,
        diagnostics=diagnostics,
    )

    out = save_fit_result(result, output_dir)
    print(f"\nDONE. Saved fit output to:\n  {out}")


if __name__ == "__main__":
    main()