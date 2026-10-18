"""A4 of PLAN.md: what carries Reddit's momentum (D6-D10), with the gapped statistic of 2z-df.

Windows of w weeks; change 1 = level(W2) - level(W1), h weeks apart; change 2 the same, starting g weeks after W2
ends; cohort = the top K by mean log level over the selection window (disjoint from every measurement window).
  D6  items against activity per item: window levels log(sum of items), log(sum of activity / sum of items),
      log(sum of activity) (an exact additive split of the change)
  D7  residualisation, loadings and clusters from weekly changes in weeks 0-51 only, momentum from origins >= 52:
      (raw), minus loading x aggregate level, minus the co-movement cluster's mean level (k-means, k = 20)
  D8  sign asymmetry: Spearman within change-1 risers and fallers (split at the median), and quintile rank means
  D9  lag profile (one origin): gaps 0, 26, 52 (Reddit)
  D10 Reddit at Facebook's settings and dates: 4-week windows, 26-week changes, gaps 0/4/8, from 2020-12-28

    python llm_fitting/runs/2026-10-18_pnas_push/momentum_decomposition.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as pads
from scipy.cluster.vq import kmeans2
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

PRES = 0.9
panel = sys.argv[1]
COUNT = {"reddit_comments_ext": "comment_count", "reddit_submissions_long": "submission_count", "facebook_a": "post_count"}[panel]
reddit = panel.startswith("reddit")
cfg = m1.PANELS[panel]
pc = mrd.PLATFORMS[cfg["platform"]]
K = cfg["K"]
df = mrd.restrict_universe(mrd.load_panel(pc), K, buffer_mult=4)
T = int(df["period"].max()) + 1
dates = pd.to_datetime(df.groupby("period")["ts"].first().reindex(range(T)).to_numpy())
Xp = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
ids = np.asarray(Xp.columns)
X = Xp.to_numpy(float)
del df, Xp
dset = pads.dataset(ROOT / pc["path"] if not Path(pc["path"]).is_absolute() else pc["path"])
tab = dset.to_table(columns=[pc["id_col"], pc["ts_col"], COUNT], filter=pads.field(pc["id_col"]).isin(list(ids))).to_pandas()
tab.columns = ["entity_id", "ts", "n"]
tab["entity_id"] = tab["entity_id"].astype(str)
tab["ts"] = pd.to_datetime(tab["ts"])
tab = tab.groupby(["ts", "entity_id"], as_index=False)["n"].max()
per = {t: i for i, t in enumerate(dates)}
tab["period"] = tab["ts"].map(per)
tab = tab.dropna(subset=["period"])
col = {str(e): j for j, e in enumerate(ids)}
Nm = np.full(X.shape, np.nan)
Nm[tab["period"].astype(int).to_numpy(), tab["entity_id"].map(col).to_numpy()] = tab["n"].to_numpy(float)
del tab
Am = np.expm1(X)


def wmean(Z, lo, hi):
    seg = Z[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def wsum_log(Z, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        s = np.nansum(Z[lo:hi], 0)
        return np.where(ok & (s > 0), np.log(np.maximum(s, 1e-300)), np.nan)


def levels(kind, lo, hi, Y=None):
    if kind == "x":
        return wmean(X if Y is None else Y, lo, hi)
    if kind == "act":
        return wsum_log(Am, lo, hi)
    if kind == "items":
        return wsum_log(Nm, lo, hi)
    return wsum_log(Am, lo, hi) - wsum_log(Nm, lo, hi)     # activity per item


def cohort(lo, hi):
    m = wmean(X, lo, hi)
    top = np.argsort(np.where(np.isfinite(m), -m, np.inf))[:K]
    c = np.zeros(X.shape[1], bool)
    c[top[np.isfinite(m[top])]] = True
    return c


def changes(kind, coh, a, w, h, g, Y=None):
    x = levels(kind, a + h, a + h + w, Y) - levels(kind, a, a + w, Y)
    b = a + h + w + g
    y = levels(kind, b + h, b + h + w, Y) - levels(kind, b, b + w, Y)
    ok = coh & np.isfinite(x) & np.isfinite(y)
    return x[ok], y[ok]


def rho(x, y):
    return float(spearmanr(x, y).correlation) if x.size > 30 else float("nan")


def origins(a0, w, h, g, end):
    out, a = [], a0
    while a + 2 * h + 2 * w + g <= end:
        out.append(a)
        a += w
    return out


w, h, gaps, sel = ((13, 52, (0, 13, 26), 26) if reddit else (4, 26, (0, 4, 8), 13))
coh = cohort(0, sel)
res = dict(panel=panel, T=T, K=K, D6={}, D7={}, D8={}, D9={}, D10={})
# D6
for kind in ("x", "act", "items", "per_item"):
    res["D6"][kind] = {}
    for g in gaps:
        vals = [rho(*changes(kind, coh, a, w, h, g)) for a in origins(sel, w, h, g, T)]
        res["D6"][kind][g] = float(np.nanmean(vals))
    print(f"D6 {kind:9s}: gapped momentum by gap " + ", ".join(f"g{g} {v:+.3f}" for g, v in res["D6"][kind].items()), flush=True)
# D7: loadings and clusters from weekly changes in the training prefix
tr = 52 if reddit else 26
dX = np.diff(X[:tr], axis=0)
agg_lvl = np.array([np.nanmean(X[t][coh]) for t in range(T)])
dA = np.diff(agg_lvl[:tr])
ok_agg = np.isfinite(dA)
load = np.full(X.shape[1], np.nan)
for j in np.flatnonzero(coh):
    m = np.isfinite(dX[:, j]) & ok_agg
    if m.sum() >= 20:
        load[j] = np.polyfit(dA[m], dX[m, j], 1)[0]
load = np.where(np.isfinite(load), load, 1.0)
Y_agg = X - load[None, :] * agg_lvl[:, None]
prof = np.nan_to_num(dX[:, coh], nan=0.0).T
prof = (prof - prof.mean(1, keepdims=True)) / np.maximum(prof.std(1, keepdims=True), 1e-9)
_, lab = kmeans2(prof, 20, seed=1, minit="++")
labels = np.full(X.shape[1], -1)
labels[np.flatnonzero(coh)] = lab
Y_cl = X.copy()
for c in range(20):
    mem = labels == c
    if mem.sum() > 1:
        Y_cl[:, mem] = X[:, mem] - np.nanmean(X[:, mem], axis=1, keepdims=True)
for name, Y in (("raw", X), ("minus_aggregate_loading", Y_agg), ("minus_cluster_mean", Y_cl)):
    res["D7"][name] = {}
    for g in gaps:
        vals = [rho(*changes("x", coh, a, w, h, g, Y)) for a in origins(tr, w, h, g, T)]
        res["D7"][name][g] = float(np.nanmean(vals)) if vals else float("nan")
    print(f"D7 {name:24s} (origins >= {tr}): " + ", ".join(f"g{g} {v:+.3f}" for g, v in res["D7"][name].items()), flush=True)
res["D7"]["cluster_sizes"] = np.bincount(lab, minlength=20).tolist()
# D8: sign asymmetry at the middle gap, pooled over origins
g = gaps[1]
xs, ys = [], []
for a in origins(sel, w, h, g, T):
    x, y = changes("x", coh, a, w, h, g)
    xs.append(x - np.median(x))
    ys.append(y - np.median(y))
x, y = np.concatenate(xs), np.concatenate(ys)
up, dn = x > 0, x <= 0
ry = (np.argsort(np.argsort(y)) + 0.5) / y.size
qx = np.argsort(np.argsort(x)) / x.size
res["D8"] = dict(risers=rho(x[up], y[up]), fallers=rho(x[dn], y[dn]), top_quintile_rank_mean=float(ry[qx >= 0.8].mean()),
                 bottom_quintile_rank_mean=float(ry[qx < 0.2].mean()))
print(f"D8 (gap {g}): Spearman within risers {res['D8']['risers']:+.3f}, fallers {res['D8']['fallers']:+.3f}; mean rank of change 2: "
      f"top change-1 quintile {res['D8']['top_quintile_rank_mean']:.3f}, bottom {res['D8']['bottom_quintile_rank_mean']:.3f}", flush=True)
# D9 (Reddit): lag profile at one origin
if reddit:
    for g in (0, 26, 52):
        a = sel
        if a + 2 * h + 2 * w + g <= T:
            res["D9"][g] = rho(*changes("x", coh, a, w, h, g))
    print("D9 lag profile (origin week 26): " + ", ".join(f"g{g} {v:+.3f}" for g, v in res["D9"].items()), flush=True)
    # D10: Facebook's settings and dates
    t_fb = int(np.argmin(np.abs(dates - pd.Timestamp("2020-12-28"))))
    coh_fb = cohort(t_fb, t_fb + 13)
    for g in (0, 4, 8):
        vals = [rho(*changes("x", coh_fb, a, 4, 26, g)) for a in origins(t_fb + 13, 4, 26, g, min(T, t_fb + 86))]
        res["D10"][g] = float(np.nanmean(vals)) if vals else float("nan")
    print(f"D10 Reddit at Facebook's settings from {dates[t_fb].date()}: " + ", ".join(f"g{g} {v:+.3f}" for g, v in res["D10"].items()), flush=True)
Path(__file__).with_name(f"momentum_decomposition__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
