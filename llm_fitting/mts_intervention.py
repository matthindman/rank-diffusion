"""Forecasting a year of structural change from a quarter of movement, at rolling origins and
across Reddit's June 2021 Home-feed change (design in runs/2026-10-15_intervention/DESIGN.md).

13-week blocks.  At each origin o (the end of block o) the persisting set's change of shape over
the next 52 weeks, block o -> o+4, at cut-offs K/64 ... K/2, is forecast from data up to the origin:
    N   no change
    T   trend: the persisting set's change of shape over the last 52 weeks, o-4 -> o
    LQ  the rate law with the last quarter's slides:  52 x [c(S_o) - sbar(o-1 -> o)], cohort o-2
    LY  the rate law with the last year's slides:     52 x [c(S_o) - sbar(o-4 -> o)], cohort o-5
c = sigma^2 / (2 S_o), sigma^2 by the variance route on 26-week blocks (cohort of the first block,
blocks 1, 3, 5), the same for every origin: a disclosed look-ahead for origins before 2021 H2,
checked by a variant with sigma^2 from 2019-2020 only (blocks 1, 2, 3 at 26-week spacing).

    python llm_fitting/mts_intervention.py run <panel>
    python llm_fitting/mts_intervention.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-15_intervention")
PANELS = bl.PANELS
Q = 13
H = 4                                     # horizon in blocks (52 weeks)
CHANGE = "2021-06-28"                     # first Monday-week of the Home-feed change (2z-bz, 2z-ca)
FORECASTS = ("N", "T", "LQ", "LY", "LQ_early_var")


def variance_c(X, Rk, floors, K, st, ks, early=False):
    """sigma^2 at each cut-off from the variance route on 26-week blocks."""
    T = X.shape[0]
    nb = T // 26
    s0 = T - nb * 26
    M = ba.block_means(X, 26, start=s0, min_frac=m1.PRES)
    pr, pres = ba.perm_rank(Rk, s0, s0 + 26, floors)
    which = bl._assign(pr, pres, st)
    vb, spacing = ((1, 2, 3), 26) if early else ((1, 3, 5), 52)
    rows, _ = bl.lasting_variance(M, which, st, vb, spacing, np.random.default_rng(0), 1)
    vn = [r["n"] for r in rows]
    return {k: bl._at_boundary([r["sigma2"] for r in rows], vn, j) for j, k in ks}


def analyse(df, K, dates=None):
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    st, ks = bl.strata(K), bl.cutoffs(K)
    s2 = variance_c(X, Rk, floors, K, st, ks)
    s2e = variance_c(X, Rk, floors, K, st, ks, early=True)
    T = X.shape[0]
    nb = T // Q
    start = T - nb * Q
    M = ba.block_means(X, Q, start=start, min_frac=m1.PRES)
    del X
    rng = np.random.default_rng(0)
    which = {}
    for b in range(nb):
        pr, pres = ba.perm_rank(Rk, start + b * Q, start + (b + 1) * Q, floors)
        which[b] = bl._assign(pr, pres, st)
    first = [None if dates is None else dates[start + b * Q] for b in range(nb)]
    out = dict(K=int(K), T=int(T), block=Q, n_blocks=int(nb), first_week=first, change=CHANGE, origins=[])
    for o in range(5, nb - H):
        tgt = {r["k"]: r for r in bl.pair_terms(M, which[o - 1], st, o, o + H, K, ks, Q, rng, 1)["rows"]}
        rq = {r["k"]: r for r in bl.pair_terms(M, which[o - 2], st, o - 1, o, K, ks, Q, rng, 1)["rows"]}
        ry = {r["k"]: r for r in bl.pair_terms(M, which[o - 5], st, o - H, o, K, ks, Q, rng, 1)["rows"]}
        rows = []
        for j, k in ks:
            if k >= K:
                continue
            S = tgt[k]["S_a"]
            c, ce = s2[k] / (2 * S), s2e[k] / (2 * S)
            w = H * Q
            rows.append(dict(k=int(k), obs=tgt[k]["obs_P"] * w, N=0.0, T=ry[k]["obs_P"] * w,
                             LQ=(c - rq[k]["sbar"]) * w, LY=(c - ry[k]["sbar"]) * w, LQ_early_var=(ce - rq[k]["sbar"]) * w,
                             c=float(c), sbar_q=rq[k]["sbar"], sbar_y=ry[k]["sbar"]))
        post = None if dates is None else (first[o - 1] >= CHANGE)          # last quarter entirely after the change
        straddle = None if dates is None else (first[o - H] < CHANGE <= (first[o + 1] if o + 1 < nb else "9999"))
        out["origins"].append(dict(origin_block=o, origin_end=None if dates is None else dates[start + (o + 1) * Q - 1],
                                   last_quarter_post_change=post, trend_straddles_change=straddle, rows=rows))
    return out


def rms(rows, key):
    ok = [r for r in rows if all(np.isfinite(r[f]) for f in FORECASTS) and np.isfinite(r["obs"])]
    return float(np.sqrt(np.mean([(r["obs"] - r[key]) ** 2 for r in ok]))) if ok else np.nan


def run(panel):
    import minimal_rankdiff as mrd
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    T = int(df["period"].max()) + 1
    dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).str[:10].tolist() if "ts" in df.columns else None
    res = analyse(df, cfg["K"], dates)
    res.update(panel=panel, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"intervention__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] written", flush=True)


def score():
    """The predictions of DESIGN.md, rules as registered there before the run."""
    out = {}
    for pn in PANELS:
        f = OUT / "cells" / f"intervention__{pn}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        body = lambda o: [x for x in o["rows"] if x["k"] >= r["K"] // 16 - 1]  # noqa: E731  K/16 .. K/2, scored
        head = lambda o: [x for x in o["rows"] if x["k"] < r["K"] // 16 - 1]   # noqa: E731  K/64, K/32, reported
        post = [x for o in r["origins"] if o["last_quarter_post_change"] for x in body(o)]
        pre = [x for o in r["origins"] if not o["last_quarter_post_change"] and not o["trend_straddles_change"] for x in body(o)]
        allr = [x for o in r["origins"] for x in body(o)]
        post_head = [x for o in r["origins"] if o["last_quarter_post_change"] for x in head(o)]
        res = dict(post_change={k: rms(post, k) for k in FORECASTS}, before={k: rms(pre, k) for k in FORECASTS},
                   all={k: rms(allr, k) for k in FORECASTS}, post_change_head={k: rms(post_head, k) for k in FORECASTS},
                   n=dict(post=len(post), before=len(pre), all=len(allr)))
        if pn == "reddit_submissions_long":
            p = res["post_change"]
            res["J1"] = "PASS" if p["LQ"] < p["T"] and p["LQ"] < p["N"] else "FAIL"
            b = res["before"]
            res["J2"] = "PASS" if abs(b["LQ"] / b["T"] - 1) <= 0.25 else "FAIL"
        else:
            a = res["all"]
            res["J3"] = "PASS" if a["N"] <= min(a["T"], a["LQ"], a["LY"]) else "FAIL"
        out[pn] = res
    print(json.dumps(out, indent=1))
    return out


def report():
    lines = []
    for pn in PANELS:
        f = OUT / "cells" / f"intervention__{pn}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        lines += [f"\n## {pn} (K = {r['K']}; 13-week blocks; change of shape over the 52 weeks after each origin, log units)", "",
                  "| origin (end of) | last quarter after the change | trend straddles it | rms: N | T | LQ | LY | LQ, early σ² |",
                  "|---|---|---|---|---|---|---|---|"]
        for o in r["origins"]:
            lines.append(f"| {o['origin_end']} | {o['last_quarter_post_change']} | {o['trend_straddles_change']} | " +
                         " | ".join(f"{rms(o['rows'], k):.3f}" for k in FORECASTS) + " |")
        lines += ["", "By origin and cut-off (observed; T; LQ):", ""]
        for o in r["origins"]:
            lines.append(f"- {o['origin_end']}: " + "; ".join(f"{x['k']}: {x['obs']:+.3f}, {x['T']:+.3f}, {x['LQ']:+.3f}" for x in o["rows"]))
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
