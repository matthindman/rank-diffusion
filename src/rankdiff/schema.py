from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .types import Config


def load_panel(cfg: Config) -> pd.DataFrame:
    data_path = Path(cfg.data_path)
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")
    if data_path.suffix != ".parquet":
        raise ValueError(f"Only parquet inputs are currently supported, got: {data_path.suffix}")
    return pd.read_parquet(data_path)


def canonicalize_panel(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    rename_map = {
        cfg.id_col: "entity_id",
        cfg.timestamp_col: "timestamp",
        cfg.metric_col: "metric_value",
    }
    if cfg.rank_col and cfg.rank_col in df.columns:
        rename_map[cfg.rank_col] = "rank"

    missing = [col for col in [cfg.id_col, cfg.timestamp_col, cfg.metric_col] if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    panel = df.rename(columns=rename_map).copy()
    panel["entity_id"] = panel["entity_id"].astype(str)
    panel["timestamp"] = pd.to_datetime(panel["timestamp"])
    panel["metric_value"] = pd.to_numeric(panel["metric_value"], errors="coerce")
    panel = panel.dropna(subset=["entity_id", "timestamp", "metric_value"])
    panel = panel[panel["metric_value"] >= 0].copy()
    if cfg.fit_start is not None:
        panel = panel[panel["timestamp"] >= pd.Timestamp(cfg.fit_start)]
    if cfg.fit_end is not None:
        panel = panel[panel["timestamp"] <= pd.Timestamp(cfg.fit_end)]

    if panel.empty:
        raise ValueError("No rows left after canonicalization and date filtering.")

    dup_mask = panel.duplicated(subset=["timestamp", "entity_id"], keep=False)
    dup_rows = int(dup_mask.sum())
    if dup_rows:
        dup_rate = dup_rows / max(len(panel), 1)
        if dup_rate > cfg.max_duplicate_entity_period_rate:
            dup_groups = int(panel.loc[dup_mask, ["timestamp", "entity_id"]].drop_duplicates().shape[0])
            raise ValueError(
                "Duplicate entity-period rows exceed the allowed rate; "
                f"the chosen id column '{cfg.id_col}' may not uniquely identify entities "
                f"({dup_rows} duplicate rows across {dup_groups} timestamp-entity groups, rate={dup_rate:.4%})."
            )
        panel = (
            panel.groupby(["timestamp", "entity_id"], as_index=False, sort=False)["metric_value"]
            .max()
            .sort_values(["timestamp", "metric_value", "entity_id"], ascending=[True, False, True])
        )
    else:
        panel = panel.sort_values(["timestamp", "metric_value", "entity_id"], ascending=[True, False, True])

    panel["rank"] = panel.groupby("timestamp")["metric_value"].rank(method="first", ascending=False).astype(int)

    return panel.reset_index(drop=True)


def infer_cadence(ts: pd.Series, requested: str) -> str:
    if requested != "auto":
        return requested

    ordered = pd.Series(pd.Index(ts.dropna().unique()).sort_values())
    if ordered.size < 3:
        return "weekly"

    day_diffs = ordered.diff().dropna().dt.days.to_numpy()
    median_gap = float(np.median(day_diffs)) if day_diffs.size else 7.0
    if median_gap <= 2:
        return "daily"
    return "weekly"


def add_period_index(df: pd.DataFrame, cadence: str) -> pd.DataFrame:
    panel = df.copy()
    if cadence == "daily":
        panel["period_start"] = panel["timestamp"].dt.floor("D")
    elif cadence == "weekly":
        panel["period_start"] = panel["timestamp"].dt.to_period("W").dt.start_time
    else:
        raise ValueError(f"Unsupported cadence: {cadence}")

    unique_periods = pd.Index(panel["period_start"].sort_values().unique())
    period_map = {period: idx for idx, period in enumerate(unique_periods)}
    panel["period_index"] = panel["period_start"].map(period_map).astype(int)
    return panel
