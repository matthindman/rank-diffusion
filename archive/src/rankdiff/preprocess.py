from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .schema import add_period_index, canonicalize_panel, infer_cadence, load_panel
from .types import Config, DataBundle, ThresholdModel


def estimate_threshold_model(panel: pd.DataFrame, cfg: Config) -> ThresholdModel:
    per_period = panel.groupby("period_index")["metric_value"].min().sort_index()

    if cfg.threshold_mode == "provided":
        if cfg.activity_threshold is None:
            raise ValueError("activity_threshold must be set when threshold_mode='provided'")
        threshold_by_period = np.full(per_period.size, float(cfg.activity_threshold), dtype=float)
    else:
        threshold_by_period = per_period.to_numpy(dtype=float)

    max_missing_value = threshold_by_period.copy()
    return ThresholdModel(
        threshold_by_period=threshold_by_period,
        max_missing_value_by_period=max_missing_value,
        effectively_exact_above_threshold=True,
    )


def compute_log_rank_coord(rank: np.ndarray, n_t: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    normalized = (rank.astype(float) - 0.5) / n_t.astype(float)
    return np.log(np.clip(normalized, eps, 1.0))


def compute_local_slope(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    log_rank = np.log(np.clip(out["rank"].to_numpy(dtype=float), 1.0, None))
    log_metric = np.log1p(np.clip(out["metric_value"].to_numpy(dtype=float), 0.0, None))
    local_slope = np.zeros(out.shape[0], dtype=np.float64)

    for _, idx in out.groupby("period_index", sort=False).indices.items():
        x = log_rank[idx]
        y = log_metric[idx]
        if x.size >= 3:
            slope = np.gradient(y, x)
            if not np.isfinite(slope).all():
                fallback = (y[-1] - y[0]) / max(x[-1] - x[0], 1e-8)
                slope = np.where(np.isfinite(slope), slope, fallback)
        else:
            slope = np.zeros_like(x)
        local_slope[idx] = slope

    out["local_slope"] = local_slope
    return out


def _infer_platform_name(cfg: Config) -> str:
    if cfg.platform != "auto":
        return cfg.platform
    stem = Path(cfg.data_path).stem.lower()
    if "instagram" in stem or stem.startswith("ig_"):
        return "instagram"
    if "facebook" in stem or stem.startswith("fb_"):
        return "facebook"
    return stem


def _select_tracked_ids(
    panel: pd.DataFrame,
    balanced_ids: np.ndarray,
    track_entity_count: int,
    seed: int,
) -> np.ndarray:
    if balanced_ids.size == 0:
        return np.array([], dtype=str)

    if balanced_ids.size <= track_entity_count:
        return balanced_ids.copy()

    rng = np.random.default_rng(seed)
    sampled = rng.choice(balanced_ids, size=track_entity_count, replace=False)
    return sampled


def build_data_bundle(cfg: Config) -> DataBundle:
    raw = load_panel(cfg)

    panel = canonicalize_panel(raw, cfg)
    cadence = infer_cadence(panel["timestamp"], cfg.cadence)
    panel = add_period_index(panel, cadence)

    # Apply rank filter after canonicalization (which assigns unique 1..N ranks).
    if cfg.max_rank_filter is not None:
        pre_n = int(panel["entity_id"].nunique())
        panel = panel[panel["rank"] <= cfg.max_rank_filter].reset_index(drop=True)
        post_n = int(panel["entity_id"].nunique())
        print(f"  Rank filter: {pre_n:,} -> {post_n:,} unique entities (rank <= {cfg.max_rank_filter:,})")

    panel = compute_local_slope(panel)

    counts = panel.groupby("period_index")["entity_id"].nunique().sort_index()
    dates = pd.Index(panel.groupby("period_index")["period_start"].first().sort_index())
    n_periods = int(counts.size)
    n_entities = int(panel["entity_id"].nunique())
    mean_n = float(counts.mean())
    max_n = int(counts.max())

    ep_counts = panel.groupby("entity_id")["period_index"].nunique()
    min_periods = max(1, int(np.ceil(cfg.min_presence_frac * n_periods)))
    balanced_ids = ep_counts[ep_counts >= min_periods].index.to_numpy(dtype=str)
    if balanced_ids.size == 0:
        raise ValueError("No balanced-panel entities found in the selected fit window.")
    tracked_ids = _select_tracked_ids(
        panel=panel,
        balanced_ids=balanced_ids,
        track_entity_count=min(cfg.track_entity_count, max(cfg.max_dense_entities, cfg.track_entity_count)),
        seed=cfg.random_seed,
    )

    panel = panel.copy()
    n_t = panel.groupby("period_index")["entity_id"].transform("size").to_numpy(dtype=float)
    panel["z_rank"] = compute_log_rank_coord(panel["rank"].to_numpy(dtype=float), n_t, eps=cfg.z_rank_clip)

    threshold = estimate_threshold_model(panel, cfg)

    from .diagnostics import compute_empirical_targets

    empirical = compute_empirical_targets(panel, balanced_ids, tracked_ids, threshold, cfg)

    return DataBundle(
        panel=panel,
        platform=_infer_platform_name(cfg),
        cadence=cadence,
        dates=dates,
        n_periods=n_periods,
        n_entities=n_entities,
        mean_n=mean_n,
        max_n=max_n,
        balanced_ids=balanced_ids,
        tracked_entity_ids=tracked_ids,
        threshold=threshold,
        empirical=empirical,
    )
