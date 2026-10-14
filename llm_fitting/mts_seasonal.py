"""Seasonal forecasts of the ladder's half-year changes (design in runs/2026-10-15_seasonal/DESIGN.md).

On 26-week blocks, the persisting set's change of shape over each half-year (a -> a+1) is
forecast before it happens, at cut-offs K/64 ... K/2, by:
    F0  no change
    F1  the rate law with last half-year's slides:      c(S_a) - sbar(a-1 -> a)
    F2  the rate law with the same half-year a year ago: c(S_a) - sbar(a-2 -> a-1)
    E1  last half-year's change of shape
    E2  the change of shape of the same half-year a year ago
c = sigma^2 / (2 S_a): variance from the variance route (cohort of block 0, blocks 1, 3, 5),
slope at the start block only.  Slides as 2z-cd: cohort fixed by permanent rank on the block
before the pair it is measured over.  Nothing from the target half-year enters a forecast.

    python llm_fitting/mts_seasonal.py run <panel>
    python llm_fitting/mts_seasonal.py report
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

OUT = Path("llm_fitting/runs/2026-10-15_seasonal")
PANELS = bl.PANELS
BLOCK = 26
FORECASTS = ("F0", "F1", "F2", "E1", "E2")


def analyse(df, K, block=BLOCK, seed=0):
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
    del X
    rng = np.random.default_rng(seed)
    st, ks = bl.strata(K), bl.cutoffs(K)
    dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).tolist() if "ts" in df.columns else None
    which = {}
    for b in range(nb - 1):
        pr, pres = ba.perm_rank(Rk, start + b * block, start + (b + 1) * block, floors)
        which[b] = bl._assign(pr, pres, st)
    var_rows, _ = bl.lasting_variance(M, which[0], st, (1, 3, 5), 2 * block, rng, 1)
    vn = [v["n"] for v in var_rows]
    s2 = {k: bl._at_boundary([v["sigma2"] for v in var_rows], vn, j) for j, k in ks}
    pairs = {}
    for a in range(1, nb - 1):
        t = bl.pair_terms(M, which[a - 1], st, a, a + 1, K, ks, block, rng, 1)
        pairs[a] = {r["k"]: r for r in t["rows"]}
    out = dict(K=int(K), T=int(T), block=block, n_blocks=int(nb),
               first_week=None if dates is None else [dates[start + b * block] for b in range(nb)],
               variance=[dict(lo=float(lo), hi=float(hi), **v) for (lo, hi), v in zip(st, var_rows)],
               observed={str(a): {str(k): r["obs_P"] for k, r in p.items()} for a, p in pairs.items()},
               targets=[])
    for a in range(3, nb - 1):                    # needs slides from a-2 -> a-1 (cohort a-3)
        rows = []
        for j, k in ks:
            if k >= K:
                continue
            r, r1, r2 = pairs[a][k], pairs[a - 1][k], pairs[a - 2][k]
            c = s2[k] / (2 * r["S_a"])
            rows.append(dict(k=int(k), obs=r["obs_P"], F0=0.0, F1=float(c - r1["sbar"]), F2=float(c - r2["sbar"]),
                             E1=r1["obs_P"], E2=r2["obs_P"], c=float(c), sbar_last=r1["sbar"], sbar_season=r2["sbar"]))
        out["targets"].append(dict(start=a, end=a + 1, rows=rows,
                                   dates=None if dates is None else [dates[start + a * block], dates[start + (a + 1) * block]]))
    return out


def rms(rows, key):
    """Over the cells where every forecast is defined (all five are compared on the same cells)."""
    ok = [r for r in rows if all(np.isfinite(r[f]) for f in FORECASTS) and np.isfinite(r["obs"])]
    return float(np.sqrt(np.mean([(r["obs"] - r[key]) ** 2 for r in ok]))) if ok else np.nan


def run(panel):
    import minimal_rankdiff as mrd
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    res = analyse(df, cfg["K"])
    res.update(panel=panel, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"seasonal__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] written", flush=True)


def score():
    """The predictions of DESIGN.md, rules as registered there before the run."""
    out = {}
    for pn in PANELS:
        f = OUT / "cells" / f"seasonal__{pn}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        rows = [x for t in r["targets"] for x in t["rows"]]
        e = {key: rms(rows, key) for key in FORECASTS}
        res = dict(rms_per_1000_weeks={k: round(1000 * v, 3) for k, v in e.items()}, cells=len(rows))
        if pn == "reddit_comments_ext":
            res["Z1"] = "PASS" if e["F2"] < e["F0"] and e["F2"] < e["F1"] else "FAIL"
            res["Z2"] = "PASS" if e["E2"] < e["F0"] and e["E2"] < e["E1"] else "FAIL"
        else:
            res["Z3"] = "PASS" if e["F1"] < e["F0"] and e["F2"] < e["F0"] else "FAIL"
        out[pn] = res
    print(json.dumps(out, indent=1))
    return out


def report():
    lines = []
    for pn in PANELS:
        f = OUT / "cells" / f"seasonal__{pn}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        lines += [f"\n## {pn} (K = {r['K']}; 26-week blocks from {r['first_week'][0] if r['first_week'] else ''}; "
                  "change of shape of the persisting set over each half-year, per 1,000 weeks)", "",
                  "Observed change by half-year, K/64 … K/2:", ""]
        fw = r["first_week"]
        for a, row in r["observed"].items():
            ks = [k for k in row if int(k) < r["K"]]
            lab = f"{fw[int(a)]} → {fw[int(a) + 1]}" if fw else a
            lines.append(f"- {lab}: " + ", ".join(f"{1000 * row[k]:+.2f}" for k in ks))
        lines += ["", "| target half-year | k | observed | F1 law, last half-year | F2 law, same season | E1 last change | E2 same season's change |",
                  "|---|---|---|---|---|---|---|"]
        for t in r["targets"]:
            for x in t["rows"]:
                lines.append(f"| {t['dates'][0] if t['dates'] else t['start']} | {x['k']} | {1000 * x['obs']:+.2f} | {1000 * x['F1']:+.2f} | "
                             f"{1000 * x['F2']:+.2f} | {1000 * x['E1']:+.2f} | {1000 * x['E2']:+.2f} |")
        rows = [x for t in r["targets"] for x in t["rows"]]
        lines += ["", "rms error (per 1,000 weeks): " + ", ".join(f"{k} {1000 * rms(rows, k):.2f}" for k in FORECASTS)]
        for t in r["targets"]:
            lines.append(f"- target starting {t['dates'][0] if t['dates'] else t['start']}: " +
                         ", ".join(f"{k} {1000 * rms(t['rows'], k):.2f}" for k in FORECASTS))
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
