"""R16-E of PLAN.md: a capture audit of the 2018-12..2022-12 Reddit panels (DEVELOPMENT), by date and rank.

The archive's monthly files were collected with delays that differ by file (MODEL_STATUS 2z-bz), so a capture
artefact would appear as a step at a first-of-month boundary.  Without the raw files (WD drive) the delay itself
cannot be read; what the daily panels can show is whether the head's shares move at file boundaries.

    python r16_capture.py daily <panel>      head (top K/64 of each 52-week block) shares of items and karma by day;
                                             steps at first-of-month boundaries against mid-month placebos;
                                             the four short-delay pockets
    python r16_capture.py rosters <panel>    head share by block and gapped persistence with terminal collapses
                                             excluded, and with a 2K tracking threshold

Reads the lean universe cache ($R12_CACHE) and, for `daily`, the T9 daily panel by streaming its row groups.
Aggregates only; no name is printed except the terminally collapsed communities of the head (Reddit names are
allowed, MODEL_STATUS 2z-dh 10).
"""
import gc
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(HERE))
import mts_step1 as m1  # noqa: E402
import r12_hetgrowth as h  # noqa: E402

DAILY = {"reddit_submissions_long": ("data/ssd/derived/reddit_daily_long.parquet", "submission_count", "submission_karma"),
         "reddit_comments_ext": ("data/ssd/derived/reddit_comments_2018-12_2022-12_daily.parquet", "comment_count", "comment_karma")}
POCKETS = ("2019-08-26", "2020-04-27", "2021-06-07", "2022-05-23")      # Mondays of the suspect weeks (2z-bz item 5)


def blocks_and_rosters(pn):
    """52-week blocks aligned to the panel's end (as R5, R8); each block's top K and top K/64 by total activity."""
    df, K = h.load(pn)
    T = int(df["period"].max()) + 1
    nb = T // 52
    start = T - nb * 52
    dates = df.groupby("period")["ts"].first().sort_index()
    d = df[df["period"] >= start][["entity_id", "period", "X"]].copy()
    d["block"] = (d["period"] - start) // 52
    d["a"] = np.expm1(d["X"])
    A = d.groupby(["block", "entity_id"])["a"].sum().unstack(0).fillna(0.0)
    tops = {b: A[b].sort_values(ascending=False).index[:K] for b in range(nb)}
    heads = {b: A[b].sort_values(ascending=False).index[:K // 64] for b in range(nb)}
    edges = [(pd.Timestamp(dates[start + 52 * b]), pd.Timestamp(dates[start + 52 * (b + 1) - 1]) + pd.Timedelta(days=6)) for b in range(nb)]
    return df, K, T, nb, start, dates, A, tops, heads, edges


def mode_daily(pn):
    df, K, T, nb, start, dates, A, tops, heads, edges = blocks_and_rosters(pn)
    del df
    gc.collect()
    path, ccol, kcol = DAILY[pn]
    ids = sorted(set().union(*[set(map(str, tops[b])) for b in range(nb)]))
    head_of = {b: set(map(str, heads[b])) for b in range(nb)}
    top_of = {b: set(map(str, tops[b])) for b in range(nb)}
    pf = pq.ParquetFile(ROOT / path)
    parts = []
    for i in range(pf.num_row_groups):
        t = pf.read_row_group(i, columns=["date", "endpoint_id", ccol, kcol])
        t = t.filter(pc.is_in(t["endpoint_id"], value_set=pa.array(ids)))
        if t.num_rows == 0:
            continue
        g = t.to_pandas()
        g["date"] = pd.to_datetime(g["date"])
        g["block"] = -1
        for b, (lo, hi) in enumerate(edges):
            g.loc[(g["date"] >= lo) & (g["date"] <= hi), "block"] = b
        g = g[g["block"] >= 0]
        for b in range(nb):
            sb = g[g["block"] == b]
            if sb.empty:
                continue
            in_top = sb["endpoint_id"].isin(top_of[b])
            in_head = sb["endpoint_id"].isin(head_of[b])
            agg = pd.DataFrame(dict(date=sb["date"], top_n=np.where(in_top, sb[ccol], 0), head_n=np.where(in_head, sb[ccol], 0),
                                    top_k=np.where(in_top, sb[kcol].clip(lower=0), 0), head_k=np.where(in_head, sb[kcol].clip(lower=0), 0)))
            parts.append(agg.groupby("date").sum())
        del t, g
        gc.collect()
    D = pd.concat(parts).groupby(level=0).sum().sort_index()
    D["share_n"] = D["head_n"] / D["top_n"]
    D["share_k"] = D["head_k"] / D["top_k"]
    D["kpi_rel"] = (D["head_k"] / D["head_n"]) / (D["top_k"] / D["top_n"])
    L = np.log(D[["share_n", "share_k", "kpi_rel"]].replace(0, np.nan))

    def step(day, w=14):
        before = L[(L.index >= day - pd.Timedelta(days=w)) & (L.index < day)]
        after = L[(L.index >= day) & (L.index < day + pd.Timedelta(days=w))]
        if len(before) < w - 1 or len(after) < w - 1:
            return None
        return (after.mean() - before.mean()).to_dict()

    bstarts = [lo for lo, _ in edges] + [edges[-1][1] + pd.Timedelta(days=1)]

    def near_block_edge(day):
        return min(abs((day - e).days) for e in bstarts) < 15

    days = pd.date_range(D.index.min() + pd.Timedelta(days=15), D.index.max() - pd.Timedelta(days=15), freq="MS")
    firsts = [d for d in days if not near_block_edge(d)]
    mids = [d + pd.Timedelta(days=14) for d in days if not near_block_edge(d + pd.Timedelta(days=14))]   # fixed 2026-10-21 (round 8, H6): each date checked itself
    S1 = {str(d.date()): step(d) for d in firsts}
    S2 = {str(d.date()): step(d) for d in mids}
    S1 = {k: v for k, v in S1.items() if v}
    S2 = {k: v for k, v in S2.items() if v}
    within = {}
    for b, (lo, hi) in enumerate(edges):
        seg = L[(L.index >= lo) & (L.index <= hi)]
        within[b] = (seg.iloc[-28:].mean() - seg.iloc[:28].mean()).to_dict()
    summ = {}
    for col in ("share_n", "share_k", "kpi_rel"):
        b1 = np.array([v[col] for v in S1.values()])
        b2 = np.array([v[col] for v in S2.values()])
        jul = S1.get("2021-07-01", {}).get(col)
        summ[col] = dict(boundary_mean_abs=float(np.mean(np.abs(b1))), placebo_mean_abs=float(np.mean(np.abs(b2))),
                         boundary_sum=float(np.sum(b1)), placebo_sum=float(np.sum(b2)),
                         within_block_total=float(sum(within[b][col] for b in within)),
                         july2021=jul, july2021_pct_rank=None if jul is None else float(np.mean(b1 <= jul)),
                         boundary_q05_q95=[float(np.quantile(b1, 0.05)), float(np.quantile(b1, 0.95))], n_boundaries=int(b1.size))
    pockets = {}
    for mon in POCKETS:
        d0 = pd.Timestamp(mon)
        wk = L[(L.index >= d0) & (L.index < d0 + pd.Timedelta(days=7))]
        ref = L[(L.index >= d0 - pd.Timedelta(days=28)) & (L.index < d0)]
        pockets[mon] = (wk.mean() - ref.median()).to_dict() if len(wk) and len(ref) else None
    out = dict(panel=pn, K=K, k_head=K // 64, blocks=[[str(lo.date()), str(hi.date())] for lo, hi in edges], steps_first=S1, steps_mid=S2,
               within_block=within, summary=summ, pockets=pockets,
               block_share_n=[float(D.loc[(D.index >= lo) & (D.index <= hi), "head_n"].sum() / D.loc[(D.index >= lo) & (D.index <= hi), "top_n"].sum()) for lo, hi in edges])
    (HERE / f"r16_capture_daily__{pn}.json").write_text(json.dumps(out, indent=1, default=float))
    print(json.dumps(dict(summary=summ, pockets=pockets, block_share_n=out["block_share_n"]), default=float, indent=1), flush=True)


def head_shares(df, K, start, nb, drop=()):
    d = df[(df["period"] >= start) & ~df["entity_id"].isin(drop)][["entity_id", "period", "X"]].copy()
    d["block"] = (d["period"] - start) // 52
    d["a"] = np.expm1(d["X"])
    A = d.groupby(["block", "entity_id"])["a"].sum().unstack(0).fillna(0.0)
    out = []
    for b in range(nb):
        v = np.sort(A[b].to_numpy())[::-1]
        out.append(float(v[:K // 64].sum() / v[:K].sum()))
    return out


def persistence(df, K):
    X, R, floors, _, _ = m1.matrices(df, K)
    lab = h.strata(R, floors, K)
    g = h.gapped_cov(X, lab, (13, 26))
    return {str(k): v["rho"] for k, v in g.items()}


def terminal_collapses(df, drop_lu=3.0, min_left=8):
    """Communities whose weekly level falls at least drop_lu log units below their trailing 13-week mean at some week
    t <= T - min_left and never again comes within 2 log units of that mean (absence counts as below)."""
    T = int(df["period"].max()) + 1
    piv = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    X = piv.to_numpy(float)
    ok = np.isfinite(X)
    cs = np.vstack([np.zeros((1, X.shape[1])), np.cumsum(np.where(ok, X, 0.0), 0)])
    cn = np.vstack([np.zeros((1, X.shape[1])), np.cumsum(ok, 0)])
    t = np.arange(13, T - min_left + 1)
    n13 = cn[t] - cn[t - 13]
    ref = (cs[t] - cs[t - 13]) / np.maximum(n13, 1)
    suf = np.maximum.accumulate(np.where(ok, X, -np.inf)[::-1], axis=0)[::-1]       # max over weeks >= t
    xt = X[t]
    hit = (n13 >= 10) & (~np.isfinite(xt) | (xt <= ref - drop_lu)) & (suf[t] < ref - 2.0)
    out = []
    for j in np.flatnonzero(hit.any(0)):
        i = int(np.argmax(hit[:, j]))
        out.append((str(piv.columns[j]), int(t[i]), float(ref[i, j])))
    return out


def mode_rosters(pn):
    df, K = h.load(pn)
    T = int(df["period"].max()) + 1
    nb = T // 52
    start = T - nb * 52
    res = dict(panel=pn, K=K)
    res["baseline"] = dict(head_share=head_shares(df, K, start, nb), persistence=persistence(df, K))
    col = terminal_collapses(df)
    ids = {c[0] for c in col}
    first = df[df["period"] < 52].groupby("entity_id")["rank"].mean()
    head_ids = set(first[first <= K // 16].index.astype(str))
    res["collapses"] = dict(n=len(col), n_in_2019_top_K16=len(ids & head_ids),
                            in_2019_top_K64=sorted(e for e in ids if e in set(first[first <= K // 64].index.astype(str))))
    sub = df[~df["entity_id"].isin(ids)]
    res["no_collapses"] = dict(head_share=head_shares(df, K, start, nb, drop=ids), persistence=persistence(sub, K))
    minr = df.groupby("entity_id")["rank"].min()
    keep2 = set(minr[minr <= 2 * K].index)
    sub2 = df[df["entity_id"].isin(keep2)]
    res["track_2K"] = dict(n_tracked=len(keep2), n_tracked_4K=int(minr.size), persistence=persistence(sub2, K))
    (HERE / f"r16_capture_rosters__{pn}.json").write_text(json.dumps(res, indent=1, default=float))
    print(json.dumps(res, default=float, indent=1), flush=True)


if __name__ == "__main__":
    {"daily": mode_daily, "rosters": mode_rosters}[sys.argv[1]](sys.argv[2])
