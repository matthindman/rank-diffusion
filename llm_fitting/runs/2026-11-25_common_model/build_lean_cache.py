#!/usr/bin/env python3
"""Build lean, memory-safe, exact universe caches (including item counts) for the
four DEVELOPMENT panels: facebook_a, reddit_comments_ext, reddit_submissions_long,
and instagram_hm (under the §2z-c censoring apparatus).

Zero contact with any sealed or exposure-unknown folder.
Outputs live outside the repository in the session scratch directory.
"""
from __future__ import annotations

import gc
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402

CACHE_DIR = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")

SPECS = {
    "facebook_a": dict(
        platform="facebook_a",
        K=3500,
        buffer_mult=4,
        count_col="post_count",
        stream=False,
    ),
    "instagram_hm": dict(
        platform="instagram_hm",
        K=10000,
        buffer_mult=4,
        count_col="n_posts",
        stream=False,
    ),
    "reddit_comments_ext": dict(
        platform="reddit_comments_ext",
        K=12500,
        buffer_mult=4,
        count_col="comment_count",
        stream=True,
    ),
    "reddit_submissions_long": dict(
        platform="reddit_submissions_long",
        K=5000,
        buffer_mult=4,
        count_col="submission_count",
        stream=True,
    ),
}


def _resolve_path(p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else ROOT / pp


def build_direct(name: str, spec: dict) -> tuple[pd.DataFrame, dict]:
    """Direct loader for small/medium panels (facebook_a, instagram_hm)."""
    cfg = mrd.PLATFORMS[spec["platform"]]
    path = _resolve_path(cfg["path"])
    idc, tsc, mc, cc = cfg["id_col"], cfg["ts_col"], cfg["metric_col"], spec["count_col"]
    df = pd.read_parquet(path, columns=[idc, tsc, mc, cc])
    df = df.rename(columns={idc: "entity_id", tsc: "ts", mc: "metric", cc: "item_count"})
    df["entity_id"] = df["entity_id"].astype(str)
    df["ts"] = pd.to_datetime(df["ts"])
    if cfg.get("date_min") is not None:
        df = df[df["ts"] >= pd.Timestamp(cfg["date_min"])]
    if cfg.get("date_max") is not None:
        df = df[df["ts"] <= pd.Timestamp(cfg["date_max"])]
    df["metric"] = pd.to_numeric(df["metric"], errors="coerce")
    df["item_count"] = pd.to_numeric(df["item_count"], errors="coerce").fillna(0.0)
    df = df.dropna(subset=["entity_id", "ts", "metric"])
    df = df[df["metric"] >= 0]
    df = df.groupby(["ts", "entity_id"], as_index=False, sort=False).agg(
        metric=("metric", "max"),
        item_count=("item_count", "max"),
    )
    df["period"] = df["ts"].rank(method="dense").astype(int) - 1
    df["X"] = np.log1p(df["metric"].to_numpy(dtype=float))
    df = mrd._rank_within(df)

    # Exact mrd.restrict_universe selection (preserves item_count column)
    df_u = mrd.restrict_universe(df, spec["K"], buffer_mult=spec["buffer_mult"])
    if "item_count" not in df_u.columns:
        cnt_map = df[["period", "entity_id", "item_count"]]
        df_u = df_u.merge(cnt_map, on=["period", "entity_id"], how="left")

    # Verify exact equality of core columns with standard mrd path
    ref = mrd.restrict_universe(mrd.load_panel(cfg), spec["K"], buffer_mult=spec["buffer_mult"])
    assert len(df_u) == len(ref), f"{name}: row mismatch {len(df_u)} vs {len(ref)}"
    assert np.allclose(df_u["X"].to_numpy(), ref["X"].to_numpy()), f"{name}: X mismatch"
    assert np.array_equal(df_u["rank"].to_numpy(), ref["rank"].to_numpy()), f"{name}: rank mismatch"

    meta = dict(
        panel=name,
        K=spec["K"],
        buffer_mult=spec["buffer_mult"],
        T=int(df_u["period"].max()) + 1,
        M=int(df_u["entity_id"].nunique()),
        rows=int(len(df_u)),
        date_min=str(df_u["ts"].min().date()),
        date_max=str(df_u["ts"].max().date()),
        count_col=cc,
    )
    return df_u, meta


def build_stream(name: str, spec: dict) -> tuple[pd.DataFrame, dict]:
    """Row-group streaming builder for large Reddit panels (matches r12_cache.py exactly)."""
    cfg = mrd.PLATFORMS[spec["platform"]]
    path = _resolve_path(cfg["path"])
    idc, tsc, mc, cc = cfg["id_col"], cfg["ts_col"], cfg["metric_col"], spec["count_col"]
    f = pq.ParquetFile(path)

    # Pass 1: unique sorted entity ids
    uniq = None
    for g in range(f.num_row_groups):
        u = pc.unique(f.read_row_group(g, columns=[idc]).column(0))
        uniq = u if uniq is None else pc.unique(pa.concat_arrays([uniq, u]))
    names = np.array(sorted(uniq.to_pylist()), dtype=object)
    vs = pa.array(names.tolist(), type=pa.string())

    # Pass 2: integer codes, timestamps, metric, item_count
    code_l, ts_l, met_l, cnt_l = [], [], [], []
    for g in range(f.num_row_groups):
        t = f.read_row_group(g, columns=[idc, tsc, mc, cc])
        code_l.append(pc.index_in(t.column(0), value_set=vs).to_numpy().astype(np.int32))
        ts_l.append(t.column(1).cast(pa.int64()).to_numpy())
        met_l.append(t.column(2).to_numpy().astype(np.float64))
        cnt_l.append(t.column(3).to_numpy().astype(np.float64))
    code = np.concatenate(code_l)
    ts = np.concatenate(ts_l)
    met = np.concatenate(met_l)
    cnt = np.concatenate(cnt_l)
    del code_l, ts_l, met_l, cnt_l
    gc.collect()

    keep = np.isfinite(met) & (met >= 0)
    code, ts, met, cnt = code[keep], ts[keep], met[keep], np.nan_to_num(cnt[keep], nan=0.0)

    uts, period = np.unique(ts, return_inverse=True)
    period = period.astype(np.int32)
    T = uts.size

    # Rank within each week: metric desc, entity_id asc (codes are string-sorted)
    order = np.argsort(period, kind="stable")
    bounds = np.searchsorted(period[order], np.arange(T + 1))
    rank = np.empty(code.size, np.int64)
    N = np.empty(T, np.int64)
    for p in range(T):
        idx = order[bounds[p]:bounds[p + 1]]
        oo = np.lexsort((code[idx], -met[idx]))
        rank[idx[oo]] = np.arange(1, idx.size + 1)
        N[p] = idx.size
    del order
    gc.collect()

    # Absence-penalized permanent rank over full window
    floors = N + 1
    sum_rank = np.bincount(code, weights=rank, minlength=names.size)
    sum_floor_present = np.bincount(code, weights=floors[period], minlength=names.size)
    present = np.bincount(code, minlength=names.size) > 0
    perm = (sum_rank + (float(floors.sum()) - sum_floor_present)) / T
    ids = np.flatnonzero(present)
    o2 = np.lexsort((ids, perm[ids]))
    B = spec["K"] * spec["buffer_mult"]
    mem = ids[o2[:B]]

    sel = np.isin(code, mem)
    df = pd.DataFrame({
        "ts": pd.to_datetime(ts[sel]),
        "entity_id": names[code[sel]].astype(object),
        "metric": met[sel],
        "item_count": cnt[sel],
        "period": period[sel].astype(np.int64),
    })
    del code, ts, met, cnt, period, rank
    gc.collect()

    df["X"] = np.log1p(df["metric"].to_numpy(dtype=float))
    df = mrd._rank_within(df)

    meta = dict(
        panel=name,
        K=spec["K"],
        buffer_mult=spec["buffer_mult"],
        T=int(df["period"].max()) + 1,
        M=int(df["entity_id"].nunique()),
        rows=int(len(df)),
        date_min=str(df["ts"].min().date()),
        date_max=str(df["ts"].max().date()),
        count_col=cc,
    )
    return df, meta


def save_cache(name: str, df: pd.DataFrame, meta: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Save compact integer-coded parquet (no string bloat, preserves sort order!)
    # Sort unique entity_ids so integer codes match string sort order
    uniq_ids = np.sort(df["entity_id"].unique())
    id_to_code = pd.Series(np.arange(len(uniq_ids), dtype=np.int32), index=uniq_ids)
    codes = df["entity_id"].map(id_to_code).to_numpy(dtype=np.int32)

    T = meta["T"]
    M = len(uniq_ids)
    p = df["period"].to_numpy(dtype=np.int32)

    X = np.full((T, M), np.nan, dtype=np.float32)
    R = np.full((T, M), np.nan, dtype=np.float32)
    C = np.full((T, M), np.nan, dtype=np.float32)
    X[p, codes] = df["X"].to_numpy(dtype=np.float32)
    R[p, codes] = df["rank"].to_numpy(dtype=np.float32)
    C[p, codes] = df["item_count"].to_numpy(dtype=np.float32)

    floors = df.groupby("period")["N"].first().reindex(range(T)).to_numpy(dtype=np.float64) + 1.0
    dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).to_numpy()

    # Absence-penalized permanent rank within restricted universe
    pen = np.where(np.isfinite(R), R.astype(np.float64), floors[:, None])
    rperm = np.mean(pen, axis=0)
    pres = np.isfinite(R).mean(axis=0)

    # Order columns by permanent rank so column 0 is permanent rank 1!
    col_order = np.argsort(rperm, kind="stable")
    X = X[:, col_order]
    R = R[:, col_order]
    C = C[:, col_order]
    rperm = rperm[col_order]
    pres = pres[col_order]

    # Save matrices as compressed npz (fast, tiny, anonymous — zero entity names stored!)
    np.savez_compressed(
        CACHE_DIR / f"{name}.npz",
        X=X,
        R=R,
        C=C,
        floors=floors,
        rperm=rperm,
        pres=pres,
        dates=dates,
        K=np.int64(meta["K"]),
    )
    # Also save lean parquet with integer entity_id strings "e000001" for mrd/rk compatibility
    # (preserves exact rank ordering and privacy — never prints/writes raw names)
    inv_order = np.empty_like(col_order)
    inv_order[col_order] = np.arange(M)
    ranked_codes = inv_order[codes]
    df_out = pd.DataFrame({
        "period": p.astype(np.int32),
        "entity_code": ranked_codes.astype(np.int32),
        "ts": df["ts"].to_numpy(),
        "metric": df["metric"].to_numpy(dtype=np.float64),
        "item_count": df["item_count"].to_numpy(dtype=np.float64),
        "X": df["X"].to_numpy(dtype=np.float64),
        "rank": df["rank"].to_numpy(dtype=np.int32),
        "N": df["N"].to_numpy(dtype=np.int32),
        "z": df["z"].to_numpy(dtype=np.float64),
    })
    df_out.to_parquet(CACHE_DIR / f"{name}.parquet", index=False)
    (CACHE_DIR / f"{name}.json").write_text(json.dumps(meta, indent=2))
    print(f"CACHED {name}: T={T}, M={M}, K={meta['K']}, rows={meta['rows']:,}, "
          f"dates={meta['date_min']}..{meta['date_max']}", flush=True)


def cache_spec_b_facebook(df_u: pd.DataFrame) -> None:
    import spec_b_sigma_obs as sb
    cfg = mrd.PLATFORMS["facebook_a"]
    daily = sb.load_daily(set(df_u["entity_id"].unique()), path=str(_resolve_path(cfg["daily_path"])), day_guard=True)
    cur = sb.spec_b_curve(df_u, daily)
    out = dict(
        z=np.asarray(cur["z"], dtype=float).tolist(),
        sigma_obs=np.asarray(cur["sigma_obs"], dtype=float).tolist(),
        sigma_obs_legacy=np.asarray(cur["sigma_obs_legacy"], dtype=float).tolist() if "sigma_obs_legacy" in cur else None,
    )
    (CACHE_DIR / "facebook_a_spec_b.json").write_text(json.dumps(out, indent=2))
    print(f"CACHED facebook_a Spec-B curve ({len(out['z'])} bands)", flush=True)


def main():
    targets = sys.argv[1:] if len(sys.argv) > 1 else list(SPECS.keys())
    for name in targets:
        spec = SPECS[name]
        print(f"Building lean cache for {name}...", flush=True)
        df, meta = build_stream(name, spec) if spec["stream"] else build_direct(name, spec)
        save_cache(name, df, meta)
        if name == "facebook_a":
            cache_spec_b_facebook(df)
        del df
        gc.collect()


if __name__ == "__main__":
    main()
