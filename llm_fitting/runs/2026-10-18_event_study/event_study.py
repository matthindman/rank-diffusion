"""June 2021 with placebo years and control ladders (DESIGN.md).

    python llm_fitting/runs/2026-10-18_event_study/event_study.py
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

PRE, POST, PERM, BOOT = 8, 8, 13, 1000
YEARS = (2019, 2020, 2021, 2022)


def load(pn):
    cfg = m1.PANELS[pn]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    dates = df.groupby("period")["ts"].first().sort_index()
    X, Rk, floors, outside, ids = m1.matrices(df, cfg["K"])
    return cfg["K"], X, Rk, floors, ids, pd.to_datetime(dates.to_numpy())


def event_week(dates, y):
    d = pd.Timestamp(f"{y}-06-28")
    return int(np.flatnonzero(dates <= d)[-1])


def cohorts(Rk, floors, t0, K):
    pr, pres = ba.perm_rank(Rk, t0 - PERM, t0, floors)
    o = np.argsort(pr, kind="stable")
    return dict(head=o[:K // 64], upper=o[K // 64:K // 16], control=o[K // 4:K])


def window_means(X, t0, members):
    pre, post = X[t0 - PRE:t0, members], X[t0 + 1:t0 + 1 + POST, members]
    ok = (np.isfinite(pre).mean(0) >= m1.PRES) & (np.isfinite(post).mean(0) >= m1.PRES)
    return np.nanmean(pre[:, ok], 0), np.nanmean(post[:, ok], 0)


def delta(X, t0, coh, rng, which="head"):
    pa, qa = window_means(X, t0, coh[which])
    pc, qc = window_means(X, t0, coh["control"])
    d = (qa - pa).mean() - (qc - pc).mean()
    bs = [(qa - pa)[rng.integers(0, pa.size, pa.size)].mean() - (qc - pc)[rng.integers(0, pc.size, pc.size)].mean() for _ in range(BOOT)]
    return dict(delta=float(d), q=[float(np.quantile(bs, 0.05)), float(np.quantile(bs, 0.95))], n=int(pa.size), n_control=int(pc.size))


rng = np.random.default_rng(0)
P = {pn: load(pn) for pn in ("reddit_submissions_long", "reddit_comments_ext")}
out = {}
for pn, (K, X, Rk, floors, ids, dates) in P.items():
    out[pn] = {}
    for y in YEARS:
        t0 = event_week(dates, y)
        if t0 + 1 + POST > X.shape[0] or t0 - PERM - PRE < 0:
            continue
        coh = cohorts(Rk, floors, t0, K)
        out[pn][y] = {w: delta(X, t0, coh, rng, w) for w in ("head", "upper")}
        out[pn][y]["event_week"] = str(dates[t0].date())
        for w in ("head", "upper"):
            r = out[pn][y][w]
            print(f"{pn} {y} (week of {dates[t0].date()}) {w}: delta {r['delta']:+.3f} ({r['q'][0]:+.3f} to {r['q'][1]:+.3f}), n {r['n']}, control n {r['n_control']}", flush=True)
# within community: the submissions head cohort's comment activity, against the comments control cohort
Ks, Xs, Rs, fs, ids_s, ds = P["reddit_submissions_long"]
Kc, Xc, Rc, fc, ids_c, dc = P["reddit_comments_ext"]
pos = {e: i for i, e in enumerate(ids_c)}
out["within"] = {}
for y in YEARS:
    ts, tc = event_week(ds, y), event_week(dc, y)
    if ts + 1 + POST > Xs.shape[0] or tc + 1 + POST > Xc.shape[0]:
        continue
    coh_s = cohorts(Rs, fs, ts, Ks)
    head_c = np.array([pos[e] for e in ids_s[coh_s["head"]] if e in pos], dtype=int)
    coh_c = cohorts(Rc, fc, tc, Kc)
    r = delta(Xc, tc, dict(head=head_c, control=coh_c["control"]), rng, "head")
    r["matched"] = int(head_c.size)
    out["within"][y] = r
    print(f"within {y}: submissions head cohort's comment activity, delta {r['delta']:+.3f} ({r['q'][0]:+.3f} to {r['q'][1]:+.3f}), matched {head_c.size} of {coh_s['head'].size}, present {r['n']}", flush=True)
Path(__file__).with_name("event_study.json").write_text(json.dumps(out, indent=1, default=str))
