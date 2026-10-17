"""P1-P2 of DESIGN_depth.md: is a community's attention a portfolio of its items?

    python llm_fitting/runs/2026-10-18_altitude/portfolio.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as pads

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

panel = sys.argv[1]
COUNT = {"reddit_comments_ext": "comment_count", "reddit_submissions_long": "submission_count", "facebook_a": "post_count"}[panel]
cfg = m1.PANELS[panel]
pc = mrd.PLATFORMS[cfg["platform"]]
K = cfg["K"]
block, (a, b) = ((52, (1, 2)) if panel.startswith("reddit") else (13, (1, 5)))
df = mrd.load_panel(pc)
dates = df.groupby("period")["ts"].first().sort_index()
X, Rk, floors, outside, ids = m1.matrices(df, K)
del df
T = X.shape[0]
nb = T // block
start = T - nb * block
M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
pr, pres = ba.perm_rank(Rk, start, start + block, floors)
order = np.argsort(pr, kind="stable")
# item counts for the tracked endpoints, on the same weekly periods (duplicates collapsed by max, as the loader)
dset = pads.dataset(ROOT / pc["path"] if not Path(pc["path"]).is_absolute() else pc["path"])
tab = dset.to_table(columns=[pc["id_col"], pc["ts_col"], COUNT], filter=pads.field(pc["id_col"]).isin(list(ids))).to_pandas()
tab.columns = ["entity_id", "ts", "n"]
tab["entity_id"] = tab["entity_id"].astype(str)
tab["ts"] = pd.to_datetime(tab["ts"])
tab = tab.groupby(["ts", "entity_id"], as_index=False)["n"].max()
per = {t: i for i, t in enumerate(pd.to_datetime(dates.to_numpy()))}
tab["period"] = tab["ts"].map(per)
tab = tab.dropna(subset=["period"])
col = {e: j for j, e in enumerate(ids)}
Nmat = np.zeros(X.shape)
Nmat[tab["period"].astype(int).to_numpy(), tab["entity_id"].map(col).to_numpy()] = tab["n"].to_numpy(float)
A = np.nan_to_num(np.expm1(X), nan=0.0)
def blocksum(Z, bb):
    return Z[start + bb * block:start + (bb + 1) * block].sum(0)
Aa, Ab, Na, Nb = blocksum(A, a), blocksum(A, b), blocksum(Nmat, a), blocksum(Nmat, b)
edges = [K // 64 * 2 ** i for i in range(7)]
strata = [order[lo:hi] for lo, hi in zip([0] + edges[:-1], edges)]
rng = np.random.default_rng(0)


def parts(m):
    dA = np.log(Ab[m]) - np.log(Aa[m])
    dN = np.log(Nb[m]) - np.log(Na[m])
    return dA, dN, dA - dN


def members(s):
    ok = np.isfinite(M[a][s]) & np.isfinite(M[b][s]) & (Na[s] > 0) & (Nb[s] > 0) & (Aa[s] > 0) & (Ab[s] > 0)
    return s[ok]


def gamma(pick, which):
    x, y = [], []
    for s in strata:
        m = pick(members(s))
        x.append(np.mean(np.log(Na[m])))
        y.append(np.log(np.std(parts(m)[which], ddof=1)))
    return float(-np.polyfit(x, y, 1)[0])


out = dict(panel=panel, count=COUNT, block=block, blocks=[a, b], parts={}, strata=[])
for which, name in ((0, "activity"), (1, "items"), (2, "per_item")):
    pt = gamma(lambda m: m, which)
    dr = [gamma(lambda m: m[rng.integers(0, m.size, m.size)], which) for _ in range(400)]
    out["parts"][name] = dict(gamma=pt, q=[float(np.quantile(dr, 0.05)), float(np.quantile(dr, 0.95))])
    print(f"{panel}: {name:9s} sd of one-year change vs log item count: gamma {pt:+.3f} (5-95% {np.quantile(dr, 0.05):+.3f} to {np.quantile(dr, 0.95):+.3f})", flush=True)
shares = []
for s, (lo, hi) in zip(strata, zip([0] + edges[:-1], edges)):
    m = members(s)
    dA, dN, dP = parts(m)
    sh = float(np.var(dN) / np.var(dA))
    shares.append(sh)
    out["strata"].append(dict(lo=int(lo), hi=int(hi), n=int(m.size), mean_log_items=float(np.mean(np.log(Na[m]))), sd_activity=float(np.std(dA, ddof=1)),
                              sd_items=float(np.std(dN, ddof=1)), sd_per_item=float(np.std(dP, ddof=1)), corr=float(np.corrcoef(dN, dP)[0, 1]), var_share_items=sh))
    print(f"   ranks {lo + 1:>6}-{hi:<6} n {m.size:>5}  mean items {np.exp(np.mean(np.log(Na[m]))):10.0f}  sd: activity {np.std(dA, ddof=1):.3f}, items {np.std(dN, ddof=1):.3f}, per item {np.std(dP, ddof=1):.3f}; corr {np.corrcoef(dN, dP)[0, 1]:+.2f}; items' share of variance {sh:.2f}")
out["mean_items_share"] = float(np.mean(shares))
print(f"   items' share of the variance of the one-year change, mean over strata: {np.mean(shares):.2f}")
Path(__file__).with_name(f"portfolio__{panel}.json").write_text(json.dumps(out, indent=1))
