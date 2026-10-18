"""R4 of PLAN.md (post hoc): gapped persistence in triples that begin after the 2020 surge.

Annual triples with 13-week windows span >= 143 weeks, so in these panels (Nov/Dec 2018 - Dec 2022) every one has its
first change across 2020 H1.  With 4-week windows a triple spans 112 + gap weeks and can begin in the week of 6 July
2020 or later.  Statistic: Spearman between change 1 = mean(a+H .. a+H+W) - mean(a .. a+W) and change 2, the same
starting g weeks after change 1 ends; cohort = the top K by mean level over weeks 0-25; presence >= 0.9 per window.
Data (both Reddit ladders; full and prefix-only membership) and two simulated worlds (driftless Gibrat with the
passing parts of R3-A; the data-calibrated model world of R3-A, seed 0).

    python llm_fitting/runs/2026-10-18_pnas_push/persistence_post2020.py <reddit_comments_ext|reddit_submissions_long|gibrat|rdm> [pre]

With "pre" (R4b): only triples that END before 1 July 2021 (the archive's capture change), 4-week windows, gaps 4 and
8, cohort from weeks 0-12, origins from week 13; written to persistence_pre2021__<which>.json.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

PRES, H, SEL = 0.9, 52, 26
which = sys.argv[1]
PRE = len(sys.argv) > 2 and sys.argv[2] == "pre"


def wmean(X, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def cohort(X, K):
    m = wmean(X, 0, SEL)
    top = np.argsort(np.where(np.isfinite(m), -m, np.inf))[:K]
    c = np.zeros(X.shape[1], bool)
    c[top[np.isfinite(m[top])]] = True
    return c


def gapped(X, coh, W, g, a0, step, T=None):
    T = X.shape[0] if T is None else T
    vals, n = [], []
    a = a0
    while a + 2 * H + 2 * W + g <= T:
        x = wmean(X, a + H, a + H + W) - wmean(X, a, a + W)
        b = a + H + W + g
        y = wmean(X, b + H, b + H + W) - wmean(X, b, b + W)
        ok = coh & np.isfinite(x) & np.isfinite(y)
        vals.append(float(spearmanr(x[ok], y[ok]).correlation))
        n.append(int(ok.sum()))
        a += step
    return dict(mean=float(np.mean(vals)) if vals else None, by_origin=vals, n=n, origins=list(range(a0, a, step)))


def battery(X, K, surge, cut=None):
    if PRE:                                    # R4b: whole triple before the capture change; cohort weeks 0-12
        m = wmean(X, 0, 13)
        top = np.argsort(np.where(np.isfinite(m), -m, np.inf))[:K]
        coh = np.zeros(X.shape[1], bool)
        coh[top[np.isfinite(m[top])]] = True
        return {"W4_pre": {g: gapped(X, coh, 4, g, 13, 4, T=cut) for g in (4, 8)}}
    coh = cohort(X, K)
    r = {"W13_all": {g: gapped(X, coh, 13, g, SEL, 13) for g in (0, 13, 26)},
         "W4_all": {g: gapped(X, coh, 4, g, SEL, 4) for g in (4, 8, 13)},
         "W4_post": {g: gapped(X, coh, 4, g, surge, 4) for g in (4, 8, 13)}}
    return r


def matrix(df, T):
    return df.pivot(index="period", columns="entity_id", values="X").reindex(range(T)).to_numpy(float)


out = dict(which=which)
if which.startswith("reddit"):
    K = m1.PANELS[which]["K"]
    full = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[which]["platform"]])
    T = int(full["period"].max()) + 1
    wk = full.groupby("period")["ts"].first().sort_index()
    surge = int((wk < pd.Timestamp("2020-07-01")).sum())
    cut = int((wk < pd.Timestamp("2021-07-01")).sum())
    out.update(K=K, T=T, surge_index=surge, surge_week=str(wk.iloc[surge].date()), cut_index=cut)
    for label, kw in (("full_membership", {}), ("prefix_membership", dict(member_window=SEL))):
        df = mrd.restrict_universe(full, K, buffer_mult=4, **kw)
        out[label] = battery(matrix(df, T), K, surge, cut)
else:
    K, T, surge, cut = 5000, 212, 83, 135
    rng = np.random.default_rng(0)
    if which == "gibrat":
        N = 24000
        L = 12.0 - np.log(np.arange(1, N + 1)) + 0.3 * rng.standard_normal(N)
        phi = 0.5 ** (1 / 5)
        xi = np.zeros((T, N))
        for t in range(1, T):
            xi[t] = phi * xi[t - 1] + 0.3 * np.sqrt(1 - phi * phi) * rng.standard_normal(N)
        Xw = L[None, :] + np.cumsum(0.10 * rng.standard_normal((T, N)), axis=0) + xi + 0.3 * rng.standard_normal((T, N))
        X = np.where(Xw >= np.sort(Xw, axis=1)[:, [-(4 * K + 50)]], Xw, np.nan)   # observed inside 4K + 50, as the frames
    else:
        import rankdiff_kalman as rk
        import rankdiff_rdm as rdm
        EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True, mix_hetero=False)
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS["reddit_submissions_long"]), K, buffer_mult=4)
        base = mrd.estimate(df, **EST)
        p = rdm.estimate_rdm(df, K, base=base, jumps=False, variant="hold")
        L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
        Xs, ids = rdm.simulate_panel(p, L0, T, seed=0, lasting="movinghome_vp")
        rows = []
        for t in range(T):
            o = np.argsort(-Xs[t])[:4 * K + 50]
            rows.append(pd.DataFrame(dict(entity_id=ids[t][o].astype(str), period=t, X=Xs[t][o])))
        X = matrix(pd.concat(rows, ignore_index=True), T)
    out.update(K=K, T=T, surge_index=surge, cut_index=cut)
    out["world"] = battery(X, K, surge, cut)


def brief(r):
    return {k: {g: (None if v["mean"] is None else round(v["mean"], 3), len(v["by_origin"])) for g, v in d.items()} for k, d in r.items()}


for k, v in out.items():
    if isinstance(v, dict):
        print(which, k, json.dumps(brief(v)), flush=True)
Path(__file__).with_name(f"persistence_{'pre2021' if PRE else 'post2020'}__{which}.json").write_text(json.dumps(out, indent=1))
