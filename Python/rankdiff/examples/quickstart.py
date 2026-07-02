from rankdiff import Config, run_pipeline


def main():
    cfg = Config(
        data_path="data/toy_rank_data.parquet",
        id_col="entity_id",
        timestamp_col="timestamp",
        metric_col="metric_value",
        rank_col=None,
        dev_mode=True,
        track_entity_count=50,
        max_duplicate_entity_period_rate=0.05,
        min_anchor_bins=1,
        max_anchor_bins=3,
        min_anchor_bin_size=5,
        acf_sample_size=25,
        n_optim_restarts=5,
        mc_reps=1,
        mc_reps_dev=1,
        kurtosis_cal_reps=1,
    )

    result = run_pipeline(cfg)

    print("=== PARAMETER RESULTS ===")
    print(f"sigma_obs:      {result.params.sigma_obs}")
    print(f"sigma_het:      {result.params.sigma_het}")
    print(f"alpha_arch:     {result.params.alpha_arch}")
    print(f"t_df_global:    {result.params.t_df_global}")
    print(f"jump_prob:      {result.params.jump_prob}")
    print(f"top_k:          {result.params.top_k}")
    print(f"burnin_periods: {result.params.burnin_periods}")

    print("\n=== DIAGNOSTIC SUMMARY ===")
    for key in ["emp_median_var", "emp_mean_var", "emp_kurt", "zipf_slope", "mean_exit_rate"]:
        if key in result.diagnostics:
            print(f"{key}: {result.diagnostics[key]}")


if __name__ == "__main__":
    main()