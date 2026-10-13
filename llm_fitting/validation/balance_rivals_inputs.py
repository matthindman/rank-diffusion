"""V4 nuisance input: absence by absence-penalized permanent rank only.

No M1 accounting, cohort outcome, residual or score is computed here. Only
the named DEVELOPMENT registry panels can be loaded, through the guard.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/"llm_fitting"))
sys.path.insert(0, str(ROOT/"scripts"))


def absence_profile(df, K):
    """Absence penalty uses each week's full observed N, before binning."""
    T = int(df["period"].max())+1
    if df.duplicated(["period", "entity_id"]).any():
        raise ValueError("Duplicate endpoint-period key")
    floor = df.groupby("period")["N"].first().reindex(range(T))+1
    if floor.isna().any():
        raise ValueError("Nonconsecutive registered periods")
    counted = df[["entity_id", "rank"]].copy()
    counted["floor"] = df["period"].map(floor).to_numpy()
    agg = counted.groupby("entity_id").agg(rank_sum=("rank", "sum"), n=("rank", "size"), floor_sum=("floor", "sum"))
    pr = (agg["rank_sum"]+floor.sum()-agg["floor_sum"])/T
    absent = 1-agg["n"]/T
    edges = np.array([0.] + [K*2.**i for i in range(-8, 4)])
    rows = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        keep = (pr > lo) & (pr <= hi)
        if keep.any():
            rows.append(dict(lo=float(lo), hi=float(hi), rank=float(np.median(pr[keep])),
                             probability=float(absent[keep].mean()), entities=int(keep.sum())))
    if not rows:
        raise ValueError("No absence-profile bands")
    return dict(T=T, K=K, rows=rows, rule="mean absence in fixed octave bins of absence-penalized permanent rank")


def extract(panel, config):
    import minimal_rankdiff as mrd
    from sealed_data_guard import require_released
    if panel not in ("facebook_a", "reddit_comments_ext", "reddit_submissions_long"):
        raise ValueError("Only V4's named development panels are allowed")
    if not Path("/Volumes/T9").is_dir():
        raise RuntimeError("Required /Volumes/T9 is not mounted; no substitute allowed")
    cfg = mrd.PLATFORMS[panel]
    path = Path(cfg["path"])
    require_released(path)
    if not path.is_file():
        raise FileNotFoundError(path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(2**20), b""):
            h.update(block)
    df = mrd.load_panel(cfg)
    result = absence_profile(df, config["K"])
    if result["T"] != config["T"]:
        raise ValueError(f"Wrong native length: {result['T']} != {config['T']}")
    result.update(panel=panel, input_path=str(path), input_sha256=h.hexdigest(),
                  contact="Registered DEVELOPMENT loader; only absence-by-permanent-rank nuisance profile retained.")
    return result
