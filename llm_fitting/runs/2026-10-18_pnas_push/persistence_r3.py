"""R3-D of PLAN.md: the persistence statistic repaired (round-3 referees W11-W13).

  D-1  membership chosen from the cohort window only (restrict_universe member_window = 26) against full-panel
       membership; the gapped statistic (13-week windows, 52-week changes, gaps 0/13/26)
  D-2  split at the TRUE sign of change 1 (not the median): within-group Spearman, the conditional mean of change 2,
       and the mean rank of change 2 by change-1 quintile
  D-3  the factor comparison on matched origins (>= 52 for raw and residual alike), with two factors: the cohort's
       mean log activity, and the platform total (log of summed activity over the universe)

    python llm_fitting/runs/2026-10-18_pnas_push/persistence_r3.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

PRES = 0.9
panel = sys.argv[1]
K = m1.PANELS[panel]["K"]
full = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]])
T = int(full["period"].max()) + 1
W, H, SEL = 13, 52, 26


def matrix(df):
    return df.pivot(index="period", columns="entity_id", values="X").reindex(range(T)).to_numpy(float)


def wmean(X, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def cohort(X):
    m = wmean(X, 0, SEL)
    top = np.argsort(np.where(np.isfinite(m), -m, np.inf))[:K]
    c = np.zeros(X.shape[1], bool)
    c[top[np.isfinite(m[top])]] = True
    return c


def changes(X, coh, a, g):
    x = wmean(X, a + H, a + H + W) - wmean(X, a, a + W)
    b = a + H + W + g
    y = wmean(X, b + H, b + H + W) - wmean(X, b, b + W)
    ok = coh & np.isfinite(x) & np.isfinite(y)
    return x[ok], y[ok]


def origins(a0, g):
    out, a = [], a0
    while a + 2 * H + 2 * W + g <= T:
        out.append(a)
        a += W
    return out


res = dict(panel=panel)
for label, df in (("full_membership", mrd.restrict_universe(full, K, buffer_mult=4)),
                  ("prefix_membership", mrd.restrict_universe(full, K, buffer_mult=4, member_window=SEL))):
    X = matrix(df)
    coh = cohort(X)
    r = dict(gapped={})
    for g in (0, 13, 26):
        r["gapped"][g] = float(np.mean([spearmanr(*changes(X, coh, a, g)).correlation for a in origins(SEL, g)]))
    # D-2 at gap 13, pooled over origins, true sign
    xs, ys = zip(*[changes(X, coh, a, 13) for a in origins(SEL, 13)])
    x, y = np.concatenate(xs), np.concatenate(ys)
    up, dn = x > 0, x <= 0
    ry = (np.argsort(np.argsort(y)) + 0.5) / y.size
    qx = np.argsort(np.argsort(x)) / x.size
    r["sign"] = dict(share_rising=float(up.mean()), spearman_risers=float(spearmanr(x[up], y[up]).correlation),
                     spearman_fallers=float(spearmanr(x[dn], y[dn]).correlation), mean_y_risers=float(y[up].mean()),
                     mean_y_fallers=float(y[dn].mean()), mean_y_all=float(y.mean()),
                     quintile_mean_rank=[float(ry[(qx >= q / 5) & (qx < (q + 1) / 5)].mean()) for q in range(5)])
    # D-3 matched origins (>= 52): raw, net of cohort-mean loading, net of platform-total loading
    tr = 52
    dX = np.diff(X[:tr], axis=0)
    agg_mean = np.array([np.nanmean(X[t][coh]) for t in range(T)])
    agg_tot = np.log(np.array([np.nansum(np.expm1(X[t])) for t in range(T)]))
    r["factor"] = {}
    for fname, agg in (("cohort_mean", agg_mean), ("platform_total", agg_tot)):
        dA = np.diff(agg[:tr])
        load = np.full(X.shape[1], 1.0)
        for j in np.flatnonzero(coh):
            m = np.isfinite(dX[:, j]) & np.isfinite(dA)
            if m.sum() >= 20:
                load[j] = np.polyfit(dA[m], dX[m, j], 1)[0]
        Y = X - load[None, :] * agg[:, None]
        r["factor"][fname] = {g: float(np.mean([spearmanr(*changes(Y, coh, a, g)).correlation for a in origins(tr, g)])) for g in (0, 13, 26)}
    r["factor"]["raw_matched"] = {g: float(np.mean([spearmanr(*changes(X, coh, a, g)).correlation for a in origins(tr, g)])) for g in (0, 13, 26)}
    res[label] = r
    print(f"{panel} [{label}]: gapped {r['gapped']}; true-sign: rising share {r['sign']['share_rising']:.2f}, Spearman risers "
          f"{r['sign']['spearman_risers']:+.3f} fallers {r['sign']['spearman_fallers']:+.3f}, mean change 2 risers {r['sign']['mean_y_risers']:+.3f} "
          f"fallers {r['sign']['mean_y_fallers']:+.3f} (all {r['sign']['mean_y_all']:+.3f}); quintile mean ranks {np.round(r['sign']['quintile_mean_rank'], 3).tolist()}; "
          f"factor (origins >= 52): {r['factor']}", flush=True)
Path(__file__).with_name(f"persistence_r3__{panel}.json").write_text(json.dumps(res, indent=1))
