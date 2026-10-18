"""DESIGN.md addendum R2: momentum against the aggregation null.

Two changes whose increment supports do not overlap (change 1 = mean(W2) - mean(W1), h weeks apart; change 2 the
same over W3, W4, with W3 starting g weeks after W2 ends) are uncorrelated under any independent-increment process
plus white noise, whatever each community's volatility.  A short-memory stationary swing makes the correlation
slightly negative; only persistent drift, or a velocity lasting longer than the gap, makes it positive.

Instrument check first (synthetic, same code): world (i) random walk (volatility varying by community) + white
noise + OU swing (half-life 5 weeks); world (ii) adds a persistent drift by community, sd 0.4 a year, chosen
before the run for an expected gapped correlation of about +0.2.

    python llm_fitting/runs/2026-10-18_momentum/gapped_momentum.py
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


def wmean(X, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def cohort(X, sel, K):
    """Top K by mean level over weeks 0..sel-1 (presence >= PRES); disjoint from every measurement window."""
    m = wmean(X, 0, sel)
    top = np.argsort(np.where(np.isfinite(m), -m, np.inf))[:K]
    c = np.zeros(X.shape[1], bool)
    c[top[np.isfinite(m[top])]] = True
    return c


def gapped(X, coh, a, w, h, g):
    x = wmean(X, a + h, a + h + w) - wmean(X, a, a + w)
    b = a + h + w + g
    y = wmean(X, b + h, b + h + w) - wmean(X, b, b + w)
    ok = coh & np.isfinite(x) & np.isfinite(y)
    return float(spearmanr(x[ok], y[ok]).correlation), int(ok.sum())


def adjacent(X, coh, start, B):
    """The 2z-dd statistic on the same cohort: consecutive B-week block means from week `start`, one value per triple."""
    nb = (X.shape[0] - start) // B
    M = [wmean(X, start + i * B, start + (i + 1) * B) for i in range(nb)]
    out = []
    for i in range(nb - 2):
        x, y = M[i + 1] - M[i], M[i + 2] - M[i + 1]
        ok = coh & np.isfinite(x) & np.isfinite(y)
        out.append(float(spearmanr(x[ok], y[ok]).correlation))
    return out


def run(X, K, sel, w, h, gaps, step, B):
    coh = cohort(X, sel, K)
    T = X.shape[0]
    res = dict(n_cohort=int(coh.sum()), adjacent=adjacent(X, coh, sel, B), gapped={})
    for g in gaps:
        rows = []
        a = sel
        while a + 2 * h + 2 * w + g <= T:
            r, n = gapped(X, coh, a, w, h, g)
            rows.append(dict(a=a, rho=r, n=n))
            a += step
        res["gapped"][g] = dict(rows=rows, mean=float(np.mean([r["rho"] for r in rows])))
    return res


def show(name, res):
    print(f"{name}: cohort {res['n_cohort']}; adjacent-block " + ", ".join(f"{r:+.3f}" for r in res["adjacent"]), flush=True)
    for g, v in res["gapped"].items():
        print(f"   gap {g:>2}: mean {v['mean']:+.3f}  by origin " + ", ".join(f"wk{r['a']} {r['rho']:+.3f} (n {r['n']})" for r in v["rows"]), flush=True)


def synthetic(drift_sd_year, seed, N=5000, T=212):
    rng = np.random.default_rng(seed)
    sig = 0.10 * np.exp(0.3 * rng.standard_normal(N))            # weekly lasting sd, varying by community
    rw = np.cumsum(sig * rng.standard_normal((T, N)), axis=0)
    kap = np.log(2) / 5
    ou = np.zeros((T, N))
    for t in range(1, T):
        ou[t] = (1 - kap) * ou[t - 1] + 0.3 * np.sqrt(1 - (1 - kap) ** 2) * rng.standard_normal(N)
    mu = drift_sd_year / 52 * rng.standard_normal(N)
    return 2.0 * rng.standard_normal(N) + rw + ou + 0.3 * rng.standard_normal((T, N)) + np.arange(T)[:, None] * mu


out = {"synthetic": {}}
for world, d in (("i", 0.0), ("ii", 0.4)):
    reps = [run(synthetic(d, s), 2500, 26, 13, 52, (0, 13, 26), 13, 52) for s in range(5)]
    summ = dict(adjacent=float(np.mean([r["adjacent"][0] for r in reps])),
                gapped={g: float(np.mean([r["gapped"][g]["mean"] for r in reps])) for g in (0, 13, 26)})
    out["synthetic"][world] = summ
    print(f"synthetic world ({world}), drift sd {d}/yr, 5 seeds: adjacent {summ['adjacent']:+.3f}; gapped "
          + ", ".join(f"g{g} {v:+.3f}" for g, v in summ["gapped"].items()), flush=True)

for panel, kw in (("reddit_submissions_long", dict(sel=26, w=13, h=52, gaps=(0, 13, 26), step=13, B=52)),
                  ("reddit_comments_ext", dict(sel=26, w=13, h=52, gaps=(0, 13, 26), step=13, B=52)),
                  ("facebook_a", dict(sel=13, w=4, h=26, gaps=(0, 4, 8), step=4, B=13))):
    K = m1.PANELS[panel]["K"]
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]]), K, buffer_mult=4)
    T = int(df["period"].max()) + 1
    X = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T)).to_numpy(float)
    res = run(X, K, **kw)
    res["T"] = T
    out[panel] = res
    show(f"{panel} (T {T}, K {K})", res)
Path(__file__).with_name("gapped_momentum.json").write_text(json.dumps(out, indent=1))
