"""Lasting variance by rank stratum from the variogram of weekly levels
(design in runs/2026-10-15_variogram/DESIGN.md).

For a cohort fixed by permanent rank and a window of weeks, the cross-sectional variance
of a member's change over a lag D, averaged over every weekly origin in the window,
    V(D) = const + sigma^2 D       (D beyond the life of passing swings)
where the constant holds passing swings, weekly noise and the averaging offset of the
rolling 4-week means used.  The primary estimate is (V(104) - V(52)) / 52 over the whole
panel: lags a year apart leave the annual cycle out, and swings that fade within months
have no slope left between them.  It uses every weekly origin, not three block means, but
precision is set by the few independent year-long changes each member has: about the square
root of three better than the variance route of 2z-cc on planted worlds, not enough for strata
of a few dozen members.  A shorter-lag slope (26, 39, 52 weeks) on 78-week windows compares the eras
before and after June 2021; swings that have not faded by 26 weeks bias it upward by the
same amount in both eras if they are alike (planted: +0.003 a week at v = 0.3, half-life
10 weeks).

    python llm_fitting/mts_variogram.py run <panel>
    python llm_fitting/mts_variogram.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-15_variogram")
PANELS = bl.PANELS
LAGS = (26, 39, 52)
LONG = (52, 104)
SMOOTH = 4
BOOT = 200
MIN_N = 10


def rolling_means(X, w=SMOOTH, min_frac=0.75):
    """Mean of weeks t .. t+w-1 for each t (NaN unless at least min_frac of them are present)."""
    T = X.shape[0]
    ok = np.isfinite(X)
    Z = np.where(ok, X, 0.0)
    cs = np.vstack([np.zeros((1, X.shape[1])), np.cumsum(Z, axis=0)])
    cn = np.vstack([np.zeros((1, X.shape[1])), np.cumsum(ok, axis=0)])
    s = cs[w:] - cs[:-w]
    n = cn[w:] - cn[:-w]
    out = np.full((T, X.shape[1]), np.nan)
    out[:T - w + 1] = np.where(n >= min_frac * w, s / np.maximum(n, 1), np.nan)
    return out


def _vario(Xs, lags):
    """V(D) for a (weeks x members) block: per-origin cross-sectional variance of the change over
    D, averaged over origins weighted by the members present at both ends."""
    v = []
    for D in lags:
        d = Xs[D:] - Xs[:-D]
        ok = np.isfinite(d)
        cnt = ok.sum(axis=1)
        dz = np.where(ok, d, 0.0)
        mu = dz.sum(axis=1) / np.maximum(cnt, 1)
        ss = (np.where(ok, d - mu[:, None], 0.0) ** 2).sum(axis=1)
        use = cnt >= MIN_N
        v.append(float((ss[use] / (cnt[use] - 1) * cnt[use]).sum() / cnt[use].sum()) if use.any() else np.nan)
    return np.array(v)


def _slope(v, lags=LAGS):
    D = np.asarray(lags, float)
    ok = np.isfinite(v)
    if ok.sum() < 2:
        return np.nan
    return float(np.polyfit(D[ok], v[ok], 1)[0])


def variogram_by_stratum(X4, which, st, t_lo, t_hi, rng, boot=BOOT, lags=LAGS):
    """Per stratum of `which`: lasting variance (slope of V on D), its bootstrap draws over
    members, the member count and V(D) itself, over origins t_lo .. t_hi - D."""
    rows, draws = [], []
    for j in range(len(st)):
        mem = np.flatnonzero(which == j)
        if mem.size < MIN_N:
            rows.append(dict(n=int(mem.size), sigma2=np.nan, V=[np.nan] * len(lags)))
            draws.append(np.full(boot, np.nan))
            continue
        Xs = X4[t_lo:t_hi][:, mem]
        v = _vario(Xs, lags)
        bs = np.array([_slope(_vario(Xs[:, rng.integers(0, mem.size, mem.size)], lags), lags) for _ in range(boot)])
        rows.append(dict(n=int(mem.size), sigma2=_slope(v, lags), V=[float(x) for x in v]))
        draws.append(bs)
    return rows, np.array(draws)


def analyse(df, K, boot=BOOT, seed=0, by_start=None):
    """by_start: the saved start-by-start crossings of 2z-cm (by_start__<panel>.json), read for
    the slope S and the crossings R~ at each cut-off; None in the planted tests."""
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    block = bd.BLOCK
    nb = T // block
    start = T - nb * block
    st, ks = bl.strata(K), bl.cutoffs(K)
    rng = np.random.default_rng(seed)
    wk = lambda b: start + b * block  # noqa: E731
    out = dict(K=int(K), T=int(T), lags=list(LAGS), smooth=SMOOTH, starts=[], eras={})
    cohorts = {}
    for b in (0, 4):
        pr, pres = ba.perm_rank(Rk, wk(b), wk(b + 1), floors)
        cohorts[b] = bl._assign(pr, pres, st)
    cols = np.flatnonzero(np.any([w >= 0 for w in cohorts.values()], axis=0))
    X4 = rolling_means(X[:, cols])                    # only the members any cohort uses
    del X

    def cohort(b):
        return cohorts[b][cols]

    # primary: cohort fixed on block 0, origins from block 1 to the end, lags 52 and 104
    rows, dr = variogram_by_stratum(X4, cohort(0), st, wk(1), T, rng, boot, lags=LONG)
    vn = [r["n"] for r in rows]
    out["pooled"] = dict(cohort_block=0, window=[wk(1), T], lags=list(LONG),
                         strata=[dict(lo=float(lo), hi=float(hi), **r) for (lo, hi), r in zip(st, rows)])
    if by_start is not None:
        bnd = by_start["pooled_rows"]
        cells = []
        for i, (j, k) in enumerate(ks):
            r0 = bnd[i]
            s2 = bl._at_boundary([r["sigma2"] for r in rows], vn, j)
            s2b = np.array([bl._at_boundary(dr[:, t], vn, j) for t in range(boot)])
            c, cb = s2 / (2 * r0["S"]), s2b / (2 * r0["S"])
            cells.append(dict(k=int(k), S=r0["S"], sigma2=float(s2), c=float(c),
                              c_q=[float(np.nanquantile(cb, 0.05)), float(np.nanquantile(cb, 0.95))],
                              c_var_route=r0["c"], c_var_route_q=r0["c_q"], R=r0["R_lasting"], R_q=r0["R_lasting_q"],
                              ratio=float(r0["R_lasting"] / c),
                              ratio_q=[float(np.nanquantile(r0["R_lasting"] / cb, 0.05)), float(np.nanquantile(r0["R_lasting"] / cb, 0.95))]))
        out["pooled"]["cells"] = cells

    # eras before and after June 2021: cohort fixed on block 0, origins in blocks 1..3; cohort
    # fixed on block 4, origins in blocks 5..7
    for name, b0 in (("before", 0), ("after", 4)):
        rows, dr = variogram_by_stratum(X4, cohort(b0), st, wk(b0 + 1), wk(b0 + 4), rng, boot)
        out["eras"][name] = dict(cohort_block=b0, window=[wk(b0 + 1), wk(b0 + 4)],
                                 strata=[dict(lo=float(lo), hi=float(hi), **r) for (lo, hi), r in zip(st, rows)],
                                 draws=dr)
    top = []
    for j in range(len(st)):
        b, a_ = out["eras"]["before"]["draws"][j], out["eras"]["after"]["draws"][j]
        pb, pa = out["eras"]["before"]["strata"][j]["sigma2"], out["eras"]["after"]["strata"][j]["sigma2"]
        rat = a_ / b
        top.append(dict(lo=float(st[j][0]), hi=float(st[j][1]), before=pb, after=pa, ratio=float(pa / pb),
                        ratio_q=[float(np.nanquantile(rat, 0.05)), float(np.nanquantile(rat, 0.95))]))
    out["era_ratio"] = top
    for e in out["eras"].values():
        e.pop("draws")
    return out


def run(panel):
    import minimal_rankdiff as mrd
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    prev = json.loads((bd.OUT / "cells" / f"boundary__{panel}.json").read_text())
    res = analyse(df, cfg["K"], by_start=dict(pooled_rows=prev["pooled"]))
    dates = df.groupby("period")["ts"].first().reindex(range(res["T"])).astype(str).tolist() if "ts" in df.columns else None
    if dates:
        for e in list(res["eras"].values()) + [res["pooled"]]:
            e["window_dates"] = [dates[e["window"][0]], dates[min(e["window"][1], res["T"]) - 1]]
    res.update(panel=panel, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"vario__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] written", flush=True)


def score():
    """The predictions of DESIGN.md, rules as registered there before the run."""
    res = {pn: json.loads((OUT / "cells" / f"vario__{pn}.json").read_text()) for pn in PANELS
           if (OUT / "cells" / f"vario__{pn}.json").exists()}
    prev = {pn: json.loads((bd.OUT / "cells" / f"boundary__{pn}.json").read_text()) for pn in res}
    out = {}
    for pn, r in res.items():
        K = r["K"]
        vr = np.array([v["sigma2"] for v in prev[pn]["variance"]])
        vg_ = np.array([v["sigma2"] for v in r["pooled"]["strata"]])
        nn = np.array([v["n"] for v in r["pooled"]["strata"]])
        body = nn >= 1000
        rel = vg_[body] / vr[body]
        cells = r["pooled"]["cells"]
        inside = sum(0.75 <= c["ratio"] <= 1.33 for c in cells)
        head = [c for c in cells if c["k"] <= K / 16 + 1]
        w_new = float(np.mean([c["c_q"][1] - c["c_q"][0] for c in head]))
        w_old = float(np.mean([c["c_var_route_q"][1] - c["c_var_route_q"][0] for c in head]))
        out[pn] = dict(V1=dict(ratios=[float(x) for x in rel], verdict="PASS" if np.all(np.abs(rel - 1) <= 0.25) else "FAIL"),
                       V3=dict(inside=f"{inside} of {len(cells)}", ratios=[round(c["ratio"], 2) for c in cells],
                               verdict="PASS" if inside >= 5 else "FAIL"),
                       head_interval_width=dict(variogram=w_new, variance_route=w_old))
    print(json.dumps(out, indent=1))
    return out


def report():
    lines = []
    for pn in PANELS:
        f = OUT / "cells" / f"vario__{pn}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        P = r["pooled"]
        t = lambda v: 1000 * v  # noqa: E731
        lines += [f"\n## {pn} (K = {r['K']}; cohort of block 0; origins {P.get('window_dates')}; lags {P['lags']} weeks on rolling "
                  f"{r['smooth']}-week means; c and crossings per 1,000 weeks)", "",
                  "| k | S | c, variogram (5–95%) | c, variance route (5–95%) | crossings R~ (5–95%) | R~ / c, variogram (5–95%) |",
                  "|---|---|---|---|---|---|"]
        for c in P["cells"]:
            lines.append(f"| {c['k']} | {c['S']:.2f} | {t(c['c']):.2f} ({t(c['c_q'][0]):.2f}–{t(c['c_q'][1]):.2f}) | "
                         f"{t(c['c_var_route']):.2f} ({t(c['c_var_route_q'][0]):.2f}–{t(c['c_var_route_q'][1]):.2f}) | "
                         f"{t(c['R']):.2f} ({t(c['R_q'][0]):.2f}–{t(c['R_q'][1]):.2f}) | {c['ratio']:.2f} ({c['ratio_q'][0]:.2f}–{c['ratio_q'][1]:.2f}) |")
        lines += ["", "Lasting variance per week by stratum (members), variogram: " +
                  "; ".join(f"({v['lo']:.0f}, {v['hi']:.0f}] {v['sigma2']:.4f} ({v['n']})" for v in P["strata"]), "",
                  "### before and after June 2021 (cohorts fixed on blocks 0 and 4; lags 26–52 on 78-week windows)", "",
                  "| stratum | before | after | after / before (5–95%) |", "|---|---|---|---|"]
        for e in r["era_ratio"]:
            lines.append(f"| ({e['lo']:.0f}, {e['hi']:.0f}] | {e['before']:.4f} | {e['after']:.4f} | "
                         f"{e['ratio']:.2f} ({e['ratio_q'][0]:.2f}–{e['ratio_q'][1]:.2f}) |")
        lines.append(f"\nWindows: before {r['eras']['before'].get('window_dates')}, after {r['eras']['after'].get('window_dates')}")
    (OUT / "report_tables.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) == 2 and args[1] in PANELS:
        run(args[1])
    elif args[:1] == ["report"]:
        report()
        score()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
