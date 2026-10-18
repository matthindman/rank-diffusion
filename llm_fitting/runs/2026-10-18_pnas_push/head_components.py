"""R8 of PLAN.md: the head's karma share split exactly into item share and relative karma per item.

share_karma = share_items * (karma per item of the head / karma per item of the top K), annual blocks as R5.

    python llm_fitting/runs/2026-10-18_pnas_push/head_components.py <reddit_submissions_long|reddit_comments_ext>
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as pads

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

pn = sys.argv[1]
COUNT = {"reddit_comments_ext": "comment_count", "reddit_submissions_long": "submission_count"}[pn]
K = m1.PANELS[pn]["K"]
k = K // 64
pc = mrd.PLATFORMS[m1.PANELS[pn]["platform"]]
df = mrd.load_panel(pc)
T = int(df["period"].max()) + 1
nb = T // 52
start = T - nb * 52
dates = df.groupby("period")["ts"].first().sort_index()
df = df[df["period"] >= start].copy()
df["block"] = (df["period"] - start) // 52
df["a"] = np.expm1(df["X"])
A = df.groupby(["block", "entity_id"])["a"].sum().unstack(0).fillna(0.0)
del df
keep = sorted(set().union(*[set(A[b].sort_values(ascending=False).index[:K]) for b in range(nb)]))   # every year's top K
A = A.loc[keep]
dset = pads.dataset(ROOT / pc["path"] if not Path(pc["path"]).is_absolute() else pc["path"])
tab = dset.to_table(columns=[pc["id_col"], pc["ts_col"], COUNT], filter=pads.field(pc["id_col"]).isin(keep)).to_pandas()
tab.columns = ["entity_id", "ts", "n"]
tab["entity_id"] = tab["entity_id"].astype(str)
tab["ts"] = pd.to_datetime(tab["ts"])
tab = tab.groupby(["ts", "entity_id"], as_index=False)["n"].max()
tab["period"] = tab["ts"].map({pd.Timestamp(t): int(i) for i, t in dates.items()})
tab = tab.dropna(subset=["period"])
tab = tab[tab["period"] >= start].copy()
tab["block"] = ((tab["period"] - start) // 52).astype(int)
C = tab.groupby(["block", "entity_id"])["n"].sum().unstack(0).reindex(A.index).fillna(0.0)
del tab
out = dict(panel=pn, K=K, k=k, years=[2019 + b for b in range(nb)])
rows = []
head19 = A[0].sort_values(ascending=False).index[:k]
for b in range(nb):
    topK = A[b].sort_values(ascending=False).index[:K]
    head = A[b].sort_values(ascending=False).index[:k]
    r = dict(year=2019 + b)
    for name, H in (("reselected", head), ("fixed2019", head19)):
        sk = A.loc[H, b].sum() / A.loc[topK, b].sum()
        si = C.loc[H, b].sum() / C.loc[topK, b].sum()
        r[name] = dict(share_karma=float(sk), share_items=float(si), rel_karma_per_item=float(sk / si))
    rows.append(r)
out["rows"] = rows
for name in ("reselected", "fixed2019"):
    a, z = rows[0][name], rows[-1][name]
    dk, di, dr = np.log(z["share_karma"] / a["share_karma"]), np.log(z["share_items"] / a["share_items"]), np.log(z["rel_karma_per_item"] / a["rel_karma_per_item"])
    out[f"dlog_{name}"] = dict(karma=float(dk), items=float(di), per_item=float(dr))
    print(f"{pn} {name} head (top {k}): " + "; ".join(f"{r['year']}: karma {r[name]['share_karma']:.3f} items {r[name]['share_items']:.4f} "
          f"rel. karma/item {r[name]['rel_karma_per_item']:.2f}" for r in rows) + f"\n   dlog 2019->2022: karma {dk:+.3f} = items {di:+.3f} + per item {dr:+.3f}", flush=True)
Path(__file__).with_name(f"head_components__{pn}.json").write_text(json.dumps(out, indent=1))
