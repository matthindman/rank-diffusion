from rankdiff import Config, load_panel, canonicalize_panel, infer_cadence, add_period_index


def test_toy_pipeline_smoke():
    cfg = Config(
        data_path="data/toy_rank_data.parquet",
        id_col="entity_id",
        timestamp_col="timestamp",
        metric_col="metric_value",
        rank_col=None,
        fit_start=None,
        fit_end=None,
        max_duplicate_entity_period_rate=0.05,
    )

    df = load_panel(cfg)
    panel = canonicalize_panel(df, cfg)
    cadence = infer_cadence(panel["timestamp"], requested=cfg.cadence)
    panel = add_period_index(panel, cadence=cadence)

    assert len(panel) == 3000
    assert panel["entity_id"].nunique() == 100
    assert panel["timestamp"].nunique() == 30

    assert "entity_id" in panel.columns
    assert "timestamp" in panel.columns
    assert "metric_value" in panel.columns
    assert "rank" in panel.columns
    assert "period_start" in panel.columns
    assert "period_index" in panel.columns