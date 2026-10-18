"""Summarise R7's seed files: mean, 5% and 95% across seeds for every statistic, per world.

    python llm_fitting/runs/2026-10-18_pnas_push/summarize_r7.py      -> null_worlds_r7_summary.json
"""
import glob
import json
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
WORLDS = ("gibrat", "atlas", "neutral", "rdm", "sb_data", "firm_data", "beta05", "beta10")


def stat(vals):
    v = np.array([x for x in vals if x is not None and np.isfinite(x)], float)
    if v.size == 0:
        return None
    return dict(mean=float(v.mean()), q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)), n=int(v.size))


out = {}
for w in WORLDS:
    files = sorted(glob.glob(str(HERE / f"null_world_r7__{w}__s*.json")))
    if not files:
        continue
    R = [json.loads(Path(f).read_text()) for f in files]
    o = dict(n_seeds=len(R))
    o["turnover"] = {k: stat([r["turnover"][k] for r in R]) for k in R[0]["turnover"]}
    o["beta"] = stat([r["beta"] for r in R])
    o["gapped"] = {g: stat([r["gapped"][g] for r in R]) for g in R[0]["gapped"]}
    o["eq1_within"] = stat([r["eq1_within"] for r in R])
    o["boundary_pooled"] = stat([r.get("boundary_pooled") for r in R])
    o["boundary_by_k"] = {k: stat([r.get("boundary_by_k", {}).get(k) for r in R]) for k in R[0].get("boundary_by_k", {})}
    o["C_R"] = {k: stat([r["C_R"][k] for r in R]) for k in R[0]["C_R"]}
    if all("law" in r for r in R):
        o["law_c"] = stat([r["law"]["mean_c"] for r in R])
        o["law_sbar"] = stat([r["law"]["mean_sbar"] for r in R])
        o["law_c_over_s"] = stat([r["law"]["mean_c"] / r["law"]["mean_sbar"] for r in R])
        o["frac_seeds_c_lt_s"] = float(np.mean([r["law"]["mean_c"] < r["law"]["mean_sbar"] for r in R]))
    g = {}
    for r in R:
        for row in r["gomez"]:
            key = f"k{row['k']}_pair{row['pair'][0]}"
            for t in ("share_change", "current_within", "current_between", "prior_within", "prior_between"):
                if t in row:
                    g.setdefault(key, {}).setdefault(t, []).append(row[t])
    o["gomez"] = {key: {t: stat(v) for t, v in d.items()} for key, d in g.items()}
    # pooled over pairs: the head's within term when the share is roughly stable (|share change| < 0.05)
    for k in sorted({row["k"] for r in R for row in r["gomez"]}):
        rows = [row for r in R for row in r["gomez"] if row["k"] == k]
        o[f"gomez_k{k}_all"] = {t: stat([row[t] for row in rows if t in row]) for t in ("share_change", "current_within", "current_between", "prior_within")}
        st = [row for row in rows if abs(row["share_change"]) < 0.05]
        o[f"gomez_k{k}_stable"] = dict(n=len(st), current_within=stat([row["current_within"] for row in st]),
                                       prior_within=stat([row["prior_within"] for row in st if "prior_within" in row]))
    if "world_beta" in R[0]:
        o["world_beta"] = R[0]["world_beta"]
    out[w] = o
    t = o["turnover"]
    print(f"{w} ({len(R)} seeds): top10 {t['10']['mean']:.3f} [{t['10']['q05']:.3f}, {t['10']['q95']:.3f}]; K/2 {t['2500']['mean']:.3f} "
          f"[{t['2500']['q05']:.3f}, {t['2500']['q95']:.3f}]; beta {o['beta']['mean']:.3f} [{o['beta']['q05']:.3f}, {o['beta']['q95']:.3f}]; "
          f"gapped13 {o['gapped']['13']['mean']:+.3f}; boundary {o['boundary_pooled']['mean'] if o['boundary_pooled'] else None}; "
          f"c/s {o.get('law_c_over_s', {}).get('mean') if o.get('law_c_over_s') else None} (c<s in {o.get('frac_seeds_c_lt_s')}); "
          f"head within (current, all pairs) {o['gomez_k78_all']['current_within']['mean']:+.3f}")
(HERE / "null_worlds_r7_summary.json").write_text(json.dumps(out, indent=1))
