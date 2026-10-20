"""R15 D3 (PLAN.md addendum R15): is the platform-total loading adjustment of persistence (persistence_r3.py, D-3) an
artefact of noisy loadings?  Full membership (the lean cache, = restrict_universe(full, K, 4)), the same cohort,
windows, origins and loadings as persistence_r3.py.  Reports:
  - the raw and adjusted gapped persistence (must reproduce persistence_r3 to rounding);
  - the split-half reliability of the loadings (weeks 0-25 against 26-51; Pearson across cohort communities);
  - a placebo: the same adjustment with the loadings permuted across the cohort, 50 draws;
  - the adjustment with empirical-Bayes-shrunk loadings (shrunk toward the cohort mean by signal / (signal + se²)).

    R12_CACHE=<dir> python llm_fitting/runs/2026-10-18_pnas_push/r15_d3_loading_placebo.py
"""
import gc
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
import mts_step1 as m1  # noqa: E402

PRES, W, H, SEL, TR = 0.9, 13, 52, 26, 52


def wmean(X, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def gapped(X, coh, a0, g, T):
    vals, a = [], a0
    while a + 2 * H + 2 * W + g <= T:
        x = wmean(X, a + H, a + H + W) - wmean(X, a, a + W)
        b = a + H + W + g
        y = wmean(X, b + H, b + H + W) - wmean(X, b, b + W)
        ok = coh & np.isfinite(x) & np.isfinite(y)
        vals.append(spearmanr(x[ok], y[ok]).correlation)
        a += W
    return float(np.mean(vals))


def loadings(dX, dA, cols, lo, hi):
    """OLS slope of each column's weekly change on the factor's, over weeks lo..hi-1 (as persistence_r3: >= 20 obs)."""
    lam = np.full(dX.shape[1], np.nan)
    se2 = np.full(dX.shape[1], np.nan)
    for j in cols:
        m = np.isfinite(dX[lo:hi, j]) & np.isfinite(dA[lo:hi])
        if m.sum() >= 20:
            a, y = dA[lo:hi][m], dX[lo:hi, j][m]
            b1, b0 = np.polyfit(a, y, 1)
            lam[j] = b1
            res = y - (b0 + b1 * a)
            se2[j] = float(np.sum(res ** 2) / max(m.sum() - 2, 1) / np.sum((a - a.mean()) ** 2))
    return lam, se2


def main():
    out = {}
    rng = np.random.default_rng(15)
    for pn in ("reddit_submissions_long", "reddit_comments_ext"):
        K = m1.PANELS[pn]["K"]
        df = pd.read_parquet(Path(os.environ["R12_CACHE"]) / f"univ__{pn}.parquet")
        T = int(df["period"].max()) + 1
        X = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T)).to_numpy(float)
        del df
        gc.collect()
        mm = wmean(X, 0, SEL)
        top = np.argsort(np.where(np.isfinite(mm), -mm, np.inf))[:K]
        coh = np.zeros(X.shape[1], bool)
        coh[top[np.isfinite(mm[top])]] = True
        cols = np.flatnonzero(coh)
        agg = np.log(np.array([np.nansum(np.expm1(X[t])) for t in range(T)]))
        dX = np.diff(X[:TR], axis=0)
        dA = np.diff(agg[:TR])
        raw = {g: gapped(X, coh, TR, g, T) for g in (0, 13, 26)}

        def adjusted(lam):
            load = np.ones(X.shape[1])
            fin = np.isfinite(lam)
            load[fin] = lam[fin]
            return {g: gapped(X - load[None, :] * agg[:, None], coh, TR, g, T) for g in (0, 13, 26)}

        lam, se2 = loadings(dX, dA, cols, 0, TR - 1)
        adj = adjusted(lam)
        l1, _ = loadings(dX, dA, cols, 0, 25)
        l2, _ = loadings(dX, dA, cols, 25, TR - 1)
        both = np.isfinite(l1) & np.isfinite(l2)
        rel = float(np.corrcoef(l1[both], l2[both])[0, 1])
        fin = np.isfinite(lam) & coh
        pl = []
        for _ in range(50):
            lp = lam.copy()
            lp[fin] = rng.permutation(lam[fin])
            pl.append(adjusted(lp))
        mu, v = float(np.mean(lam[fin])), float(np.var(lam[fin]))
        sig = max(v - float(np.nanmean(se2[fin])), 0.0)
        leb = lam.copy()
        leb[fin] = mu + (sig / (sig + se2[fin])) * (lam[fin] - mu)
        eb = adjusted(leb)
        pq = {g: [float(np.median([d[g] for d in pl])), float(np.quantile([d[g] for d in pl], 0.05)), float(np.quantile([d[g] for d in pl], 0.95))]
              for g in (0, 13, 26)}
        out[pn] = dict(raw=raw, adjusted=adj, split_half_reliability=rel, n_cohort=int(fin.sum()), loading_mean=mu, loading_var=v,
                       mean_se2=float(np.nanmean(se2[fin])), signal_var=sig, placebo=pq, eb=eb,
                       factor_change_2019=float(agg[TR - 1] - agg[0]), factor_change_after=float(agg[-1] - agg[TR - 1]))
        print(f"== {pn}: raw {np.round(list(raw.values()), 3).tolist()}; adjusted (real loadings) {np.round(list(adj.values()), 3).tolist()}; "
              f"placebo median [5,95] by gap {[np.round(pq[g], 3).tolist() for g in (0, 13, 26)]}; EB-shrunk {np.round(list(eb.values()), 3).tolist()}; "
              f"split-half reliability {rel:+.3f}; loading var {v:.3f} vs mean se² {np.nanmean(se2[fin]):.3f} (signal {sig:.3f}); "
              f"factor change in 2019 {agg[TR - 1] - agg[0]:+.3f}, after {agg[-1] - agg[TR - 1]:+.3f}", flush=True)
        del X, dX
        gc.collect()
    (HERE / "r15_d3_loading_placebo.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
