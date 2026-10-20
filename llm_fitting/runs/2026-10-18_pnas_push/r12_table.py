"""R12: every world statistic by variant, from the per-seed files (r12_world__<panel>__<variant>__s<seed>.json), with the
data's values where this run computed them (r12_moments__<panel>.json, lockin_funnel__<panel>.json).  Descriptive;
the registered scores are in r12_score.py.  Writes r12_table.json and prints a table per panel.

    python llm_fitting/runs/2026-10-18_pnas_push/r12_table.py
"""
import json
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
PANELS = ("reddit_submissions_long", "reddit_comments_ext")
VARIANTS = ("control", "control_num", "fixed", "decay", "factor")


def band(v):
    v = np.array([x for x in v if x is not None and np.isfinite(x)], float)
    if not v.size:
        return None
    return dict(mean=float(v.mean()), q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)), n=int(v.size))


def rows_for(W, K):
    k64, k2 = str(K // 64), str(K // 2)
    get = {
        "turnover, top 10": lambda w: w["turnover"]["10"],
        f"turnover, top K/64": lambda w: w["turnover"][k64],
        f"turnover, top K/2": lambda w: w["turnover"][k2],
        "retained 2 yr, top 10": lambda w: w["lockin"]["retention"]["10"]["2"]["mean"],
        "retained 2 yr, top K/64": lambda w: w["lockin"]["retention"][k64]["2"]["mean"],
        "replaced 2yr / 1yr, K/64": lambda w: (1 - w["lockin"]["retention"][k64]["2"]["mean"]) / max(1 - w["lockin"]["retention"][k64]["1"]["mean"], 1e-9),
        "crossings / predicted (boundary)": lambda w: w.get("boundary_pooled"),
        "crossings / slide": lambda w: w["law"]["mean_c"] / w["law"]["mean_sbar"] if "law" in w else None,
        "persistence, 13-wk gap": lambda w: w["gapped"]["13"],
        "persistence, 26-wk gap": lambda w: w["gapped"]["26"],
        "lag profile rho, gap 0": lambda w: w["r12_aux"]["gapped"]["0"]["rho"],
        "lag profile rho, gap 26": lambda w: w["r12_aux"]["gapped"]["26"]["rho"],
        "lag profile rho, gap 52": lambda w: w["r12_aux"]["gapped"]["52"]["rho"],
        "asymmetry fallers - risers": lambda w: w["r12_aux"]["asym"]["diff"],
        "R10 contrast Q1 - rest": lambda w: np.mean([r["Q1"] - r["rest"] for r in w["r10"]]),
        "R10 slide (all)": lambda w: np.mean([r["slide"] for r in w["r10"]]),
        "raw size exponent": lambda w: w["beta"],
        "lasting var (2-spacing), head": lambda w: w["r12_aux"]["sf"][0],
        "funnel p90 ratio (K/2-K over top K/64)": lambda w: w["lockin"]["strata"][-1]["p90"] / w["lockin"]["strata"][0]["p90"],
    }
    return {name: band([f(w) for w in W]) for name, f in get.items()}


def data_values(pn, K):
    dm = json.loads((HERE / f"r12_moments__{pn}.json").read_text())
    lf = json.loads((HERE / f"lockin_funnel__{pn}.json").read_text())
    k64 = str(K // 64)
    r = lf["retention"]
    return {
        "turnover, top 10": 1 - r["10"]["1"]["mean"],
        "turnover, top K/64": 1 - r[k64]["1"]["mean"],
        "turnover, top K/2": 1 - r[str(K // 2)]["1"]["mean"],
        "retained 2 yr, top 10": r["10"]["2"]["mean"],
        "retained 2 yr, top K/64": r[k64]["2"]["mean"],
        "replaced 2yr / 1yr, K/64": (1 - r[k64]["2"]["mean"]) / (1 - r[k64]["1"]["mean"]),
        "lag profile rho, gap 0": dm["aux"]["gapped"]["0"]["rho"],
        "lag profile rho, gap 26": dm["aux"]["gapped"]["26"]["rho"],
        "lag profile rho, gap 52": dm["aux"]["gapped"]["52"]["rho"],
        "asymmetry fallers - risers": dm["aux"]["asym"]["diff"],
        "lasting var (2-spacing), head": dm["aux"]["sf"][0],
        "funnel p90 ratio (K/2-K over top K/64)": lf["strata"][-1]["p90"] / lf["strata"][0]["p90"],
    }


def main():
    out = {}
    for pn in PANELS:
        K = json.loads((HERE / f"r12_moments__{pn}.json").read_text())["K"]
        tab = {}
        for v in VARIANTS:
            W = [json.loads(f.read_text()) for f in sorted(HERE.glob(f"r12_world__{pn}__{v}__s*.json"))]
            if W:
                tab[v] = dict(n=len(W), s=W[0].get("s"), half_life=W[0].get("half_life"), m=W[0].get("m"), rows=rows_for(W, K))
        dv = data_values(pn, K)
        out[pn] = dict(K=K, data=dv, worlds=tab)
        print(f"\n== {pn} (K {K}); worlds: " + ", ".join(f"{v} n={t['n']} s={t['s']} hl={t['half_life']} m={t['m']}" for v, t in tab.items()))
        names = list(next(iter(tab.values()))["rows"]) if tab else []
        print(f"{'statistic':40s} {'data':>8s} " + " ".join(f"{v:>22s}" for v in tab))
        for nm in names:
            d = dv.get(nm)
            cells = []
            for v in tab:
                b = tab[v]["rows"][nm]
                cells.append(f"{b['mean']:+.3f} [{b['q05']:+.3f},{b['q95']:+.3f}]" if b else "-")
            print(f"{nm:40s} {('%+.3f' % d) if d is not None else '-':>8s} " + " ".join(f"{c:>22s}" for c in cells))
    (HERE / "r12_table.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
