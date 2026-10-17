"""POST HOC sensitivity of the registered event study (DESIGN.md), written after its results were seen:
  (a) the event week moved to the week containing 21 June in every year, so that the 2021 pre window ends
      before the daily onset of 23-24 June (2z-ca); the registered pre window contained it;
  (b) a pre-trend adjustment: Delta minus the same contrast one window earlier (weeks -16..-9 against
      -8..-1), the change in the cohort's relative drift across the event rather than its level step.
Same strata, windows, controls and bootstrap as event_study.py; the cohort is fixed on the 13 weeks before all three
windows (weeks -29..-17), so that no window is selected on.

    python llm_fitting/runs/2026-10-18_event_study/event_study_post_hoc.py
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
    return int(np.flatnonzero(dates <= pd.Timestamp(f"{y}-06-21"))[-1])


def cohorts(Rk, floors, t0, K):
    pr, pres = ba.perm_rank(Rk, t0 - PERM - 2 * PRE, t0 - 2 * PRE, floors)   # cohort fixed before all three windows
    o = np.argsort(pr, kind="stable")
    return dict(head=o[:K // 64], upper=o[K // 64:K // 16], control=o[K // 4:K])


def change(X, t_pre, t_post, members):
    a, b = X[t_pre:t_pre + PRE, members], X[t_post:t_post + POST, members]
    ok = (np.isfinite(a).mean(0) >= m1.PRES) & (np.isfinite(b).mean(0) >= m1.PRES)
    return np.nanmean(b[:, ok], 0) - np.nanmean(a[:, ok], 0)


def contrast(X, t0, coh, which, rng):
    pairs = [(t0 - PRE, t0 + 1), (t0 - 2 * PRE, t0 - PRE)]              # event change; the change one window earlier
    out = []
    for tp, tq in pairs:
        out.append((change(X, tp, tq, coh[which]), change(X, tp, tq, coh["control"])))
    d = [h.mean() - c.mean() for h, c in out]
    bs = []
    for _ in range(BOOT):
        v = [h[rng.integers(0, h.size, h.size)].mean() - c[rng.integers(0, c.size, c.size)].mean() for h, c in out]
        bs.append((v[0], v[0] - v[1]))
    bs = np.array(bs)
    q = lambda x: [float(np.quantile(x, 0.05)), float(np.quantile(x, 0.95))]  # noqa: E731
    return dict(delta=float(d[0]), delta_q=q(bs[:, 0]), trend_adjusted=float(d[0] - d[1]), trend_adjusted_q=q(bs[:, 1]))


rng = np.random.default_rng(0)
res = {}
P = {pn: load(pn) for pn in ("reddit_submissions_long", "reddit_comments_ext")}
for pn, (K, X, Rk, floors, ids, dates) in P.items():
    res[pn] = {}
    for y in YEARS:
        t0 = event_week(dates, y)
        if t0 + 1 + POST > X.shape[0] or t0 - PERM - 2 * PRE < 0:
            continue
        coh = cohorts(Rk, floors, t0, K)
        res[pn][y] = {w: contrast(X, t0, coh, w, rng) for w in ("head", "upper")}
        for w in ("head", "upper"):
            r = res[pn][y][w]
            print(f"{pn} {y} (week of {dates[t0].date()}) {w}: delta {r['delta']:+.3f} ({r['delta_q'][0]:+.3f} to {r['delta_q'][1]:+.3f}); "
                  f"trend-adjusted {r['trend_adjusted']:+.3f} ({r['trend_adjusted_q'][0]:+.3f} to {r['trend_adjusted_q'][1]:+.3f})", flush=True)
Path(__file__).with_name("event_study_post_hoc.json").write_text(json.dumps(res, indent=1))
