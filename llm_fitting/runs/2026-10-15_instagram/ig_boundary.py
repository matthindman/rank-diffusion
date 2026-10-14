"""The boundary theory on Instagram through the 2z-c censoring apparatus (design in DESIGN.md beside this file).

    python llm_fitting/runs/2026-10-15_instagram/ig_boundary.py instagram_hm      # totals, K = 10,000
    python llm_fitting/runs/2026-10-15_instagram/ig_boundary.py instagram_pp      # per post (q-free)
    python llm_fitting/runs/2026-10-15_instagram/ig_boundary.py instagram         # raw top-50k: negative control

Weeks 9-52 of 2023 (the partial week 0 is already dropped from the panels; the collection ramp, weeks
1-8, is dropped here: 2z-c L2); 7-week blocks, one start; presence 0.5 and the present-weeks permanent
rank for the apparatus panels (instrument dropout is not behaviour); the raw panel is run both with the
default rules and with the apparatus options.  For the totals panel the change of log activity is also
split into the change of log matching posts (log M: posting volume and the a-match share q) and of log
engagement per matching post (q-free), by the same variance route.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

K, BLOCK, DATE_MIN, PRES = 10000, 7, "2023-02-27", 0.5
HERE = Path(__file__).resolve().parent
panel = sys.argv[1]
cfg = dict(mrd.PLATFORMS[panel])
cfg["date_min"] = DATE_MIN
df = mrd.load_panel(cfg)
T = int(df["period"].max()) + 1
dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).str[:10].tolist()
print(f"[{panel}] weeks {dates[0]} .. {dates[-1]} (T = {T}), rows {len(df)}", flush=True)
out = dict(panel=panel, K=K, block=BLOCK, date_min=DATE_MIN, first_date=dates[0], last_date=dates[-1])
variants = [("apparatus", dict(pres=PRES, rank_rule="present_weeks"))]
if panel == "instagram":
    variants = [("default", {}), ("apparatus", dict(pres=PRES, rank_rule="present_weeks"))]
for name, kw in variants:
    r = bd.analyse(df, K, BLOCK, **kw)
    out[name] = r
    if "not_adjudicated" in r:
        print(f"  {name}: NOT ADJUDICATED ({r['not_adjudicated']}); certified top {r['certified_top']}", flush=True)
    else:
        print(f"  {name}: " + "; ".join(f"k={x['k']}: R~/c {x['ratio']:.2f} ({x['ratio_q'][0]:.2f}-{x['ratio_q'][1]:.2f}), "
                                         f"E~-L~ {1000 * (x['E_lasting'] - x['L_lasting']):+.2f}" for x in r["pooled"]), flush=True)

if panel == "instagram_hm":
    raw = pd.read_parquet(ROOT / cfg["path"], columns=["date", "user_name", "metric_value", "n_posts"])
    raw = raw.rename(columns={"user_name": "entity_id", "date": "ts"})
    raw["ts"] = pd.to_datetime(raw["ts"])
    # duplicate (account, week) rows: keep the one the loader keeps, the largest metric (load_panel collapses by max)
    raw = (raw.sort_values("metric_value", ascending=False).drop_duplicates(["entity_id", "ts"])
              .drop(columns="metric_value"))
    d = df.merge(raw, on=["entity_id", "ts"], how="left")
    X, Rk, floors, outside, ids = m1.matrices(d, K)
    lm = d.assign(lm=np.log(d["n_posts"].clip(lower=1))).pivot(index="period", columns="entity_id", values="lm")
    LM = lm.reindex(index=range(T), columns=ids).to_numpy(dtype=float)
    nb = T // BLOCK
    start = T - nb * BLOCK
    st = bl.strata(K)
    pr, pres = bd._perm(Rk, start, start + BLOCK, floors, "present_weeks")
    which = bd._assign_at(pr, pres, st, PRES)
    rng = np.random.default_rng(0)
    parts = {}
    for key, Z in (("log Y", X), ("log M", LM), ("log Ybar", X - LM)):
        BM = ba.block_means(Z, BLOCK, start=start, min_frac=PRES)
        rows, _ = bl.lasting_variance(BM, which, st, (1, 3, 5), 2 * BLOCK, rng, 1)
        parts[key] = [dict(lo=float(lo), hi=float(hi), n=v["n"], sigma2=v["sigma2"]) for (lo, hi), v in zip(st, rows)]
    out["decomposition"] = parts
    print("  displacement variance per week by stratum (14 -> 28 weeks), log Y / log M / log Ybar:")
    for j, (lo, hi) in enumerate(st):
        a, m, y = (parts[k][j]["sigma2"] for k in ("log Y", "log M", "log Ybar"))
        print(f"    ({lo:.0f}, {hi:.0f}] n {parts['log Y'][j]['n']}: {a:.4f} / {m:.4f} / {y:.4f}  (M share {m / a if a else np.nan:.2f})", flush=True)
(HERE / f"ig_boundary__{panel}.json").write_text(json.dumps(out, indent=1, default=float))
