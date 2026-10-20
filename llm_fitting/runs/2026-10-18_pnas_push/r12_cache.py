"""R12, memory: build the universe frames once, lean, so that no later job loads a full panel.

Owner's instruction (2026-10-20): sustained swapping is not acceptable on this machine.  The full Reddit panels
(26-54 million rows) take several GB as pandas frames with string ids, and every R12 job loaded one.  This builds
the same universe frames as

    mrd.restrict_universe(mrd.load_panel(cfg), K, buffer_mult=4[, member_window=t_origin])

without the string-id frame: ids become integer codes in string-sort order (so the entity_id tie-breaks are the
same), weekly ranks and the absence-penalized permanent rank are computed exactly (integer sums), and only the
member rows are passed through the original mrd._rank_within.  Exactness is checked downstream: the cached frame
must reproduce r12_moments__<panel>.json and the saved out-of-period control shares bit for bit.

Writes $R12_CACHE/univ__<panel>.parquet and $R12_CACHE/univoop__<panel>.parquet ($R12_CACHE is a scratch folder
outside the repository; the panels are not redistributed).

    R12_CACHE=<dir> python llm_fitting/runs/2026-10-18_pnas_push/r12_cache.py <panel>
"""
import gc
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402


def t_origin_of(T):
    nb = T // 52
    return T - nb * 52 + 2 * 52


def read_lean(cfg):
    f = pq.ParquetFile(cfg["path"])
    idc, tsc, mc = cfg["id_col"], cfg["ts_col"], cfg["metric_col"]
    assert cfg.get("date_min") is None and cfg.get("date_max") is None and cfg.get("max_rank") is None
    uniq = None
    for g in range(f.num_row_groups):                    # pass 1: the id dictionary, sorted as Python sorts str
        u = pc.unique(f.read_row_group(g, columns=[idc]).column(0))
        uniq = u if uniq is None else pc.unique(pa.concat_arrays([uniq, u]))
    names = np.array(sorted(uniq.to_pylist()), dtype=object)
    vs = pa.array(names.tolist(), type=pa.string())
    code, ts, met = [], [], []
    for g in range(f.num_row_groups):                    # pass 2: codes, timestamps, metric
        t = f.read_row_group(g, columns=[idc, tsc, mc])
        for c in range(3):
            assert t.column(c).null_count == 0, "nulls present: the lean path assumes none"
        code.append(pc.index_in(t.column(0), value_set=vs).to_numpy().astype(np.int32))
        ts.append(t.column(1).cast(pa.int64()).to_numpy())
        met.append(t.column(2).to_numpy().astype(np.int64))
    return names, np.concatenate(code), np.concatenate(ts), np.concatenate(met)


def main():
    pn = sys.argv[1]
    out = Path(os.environ["R12_CACHE"])
    out.mkdir(parents=True, exist_ok=True)
    K = m1.PANELS[pn]["K"]
    B = 4 * K
    cfg = mrd.PLATFORMS[m1.PANELS[pn]["platform"]]
    names, code, ts, met = read_lean(cfg)
    keep = met >= 0                                       # load_panel: metric >= 0 (no nulls, checked)
    code, ts, met = code[keep], ts[keep], met[keep]
    o = np.lexsort((code, ts))                            # duplicates of (ts, entity): load_panel keeps the max
    dup = (np.diff(ts[o]) == 0) & (np.diff(code[o]) == 0)
    assert not dup.any(), f"{int(dup.sum())} duplicate (ts, entity) rows: the lean path assumes none"
    del o, dup
    uts, period = np.unique(ts, return_inverse=True)      # dense rank of ts, from 0
    period = period.astype(np.int32)
    T = uts.size
    order = np.argsort(period, kind="stable")
    bounds = np.searchsorted(period[order], np.arange(T + 1))
    rank = np.empty(code.size, np.int64)
    N = np.empty(T, np.int64)
    for p in range(T):                                    # rank within the week: metric desc, entity_id asc
        idx = order[bounds[p]:bounds[p + 1]]
        oo = np.lexsort((code[idx], -met[idx]))
        rank[idx[oo]] = np.arange(1, idx.size + 1)
        N[p] = idx.size
    del order
    gc.collect()

    def members(window):
        w = period < window if window is not None else np.ones(period.size, bool)
        n_periods = int(np.unique(period[w]).size)
        floors = (N[:window] if window is not None else N) + 1           # rank N_t + 1, integers
        sum_rank = np.bincount(code[w], weights=rank[w], minlength=names.size)
        sum_floor_present = np.bincount(code[w], weights=(N[period[w]] + 1), minlength=names.size)
        present = np.bincount(code[w], minlength=names.size) > 0
        perm = (sum_rank + (float(floors.sum()) - sum_floor_present)) / n_periods
        ids = np.flatnonzero(present)                     # restrict_universe ranks entities present in the window
        o2 = np.lexsort((ids, perm[ids]))                 # value, then entity_id (codes are in string order)
        return ids[o2[:B]]

    frames = {}
    for name, window in (("univ", None), ("univoop", t_origin_of(T))):
        mem = members(window)
        sel = np.isin(code, mem)
        df = pd.DataFrame({"ts": pd.to_datetime(ts[sel]), "entity_id": names[code[sel]].astype(object), "metric": met[sel],
                           "period": period[sel].astype(np.int64)})
        df["X"] = np.log1p(df["metric"].to_numpy(dtype=float))
        df["rank"] = np.int64(0)
        df["N"] = np.int64(0)
        df["z"] = 0.0
        df = mrd._rank_within(df)
        frames[name] = df
        print(f"{pn} {name}: {len(df):,} rows, {df['entity_id'].nunique():,} entities, T {T}", flush=True)
    del code, ts, met, period, rank
    gc.collect()
    for name, df in frames.items():
        f = out / f"{name}__{pn}.parquet"
        df.to_parquet(f, index=False)
        back = pd.read_parquet(f)
        same = back.equals(df) and list(back.dtypes) == list(df.dtypes)
        print(f"{pn} {name}: read-back identical: {same}; dtypes {dict(df.dtypes.astype(str))}", flush=True)
        if not same:
            f.unlink()
            raise SystemExit(f"{f.name}: read-back differs; not cached")


if __name__ == "__main__":
    main()
