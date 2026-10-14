"""Post hoc, unscored: does the year's platform growth go with the year's tilt of the ladder?
For each half-year-start pair of 2z-cg: log growth of the platform's total over the pair (mean weekly
total in the end block over the start block) against the observed change of the persisting set's shape
(mean over K/64..K/2) and against the predicted rate's slide term."""
import json, sys, numpy as np
sys.path.insert(0, "llm_fitting")
import minimal_rankdiff as mrd, mts_step1 as m1
for panel in ("reddit_comments_ext", "reddit_submissions_long"):
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    tot = df.groupby("period")["metric"].sum().sort_index().to_numpy(float)
    r = json.load(open(f"llm_fitting/runs/2026-10-14_balance_law_halfyear/cells/law__{panel}.json"))
    K, block, start = r["K"], r["block"], r["blocks"]["start"]
    bt = [tot[start + b * block:start + (b + 1) * block].mean() for b in range(r["blocks"]["n"])]
    xs, ys, ss = [], [], []
    for p in r["pairs"]:
        g = np.log(bt[p["end"]] / bt[p["start"]])
        below = [x for x in p["rows"] if x["k"] < K]
        xs.append(g); ys.append(1000 * np.mean([x["obs_P"] for x in below])); ss.append(1000 * np.mean([x["sbar"] for x in below]))
    print(panel, "growth of platform total by pair:", [round(x, 3) for x in xs])
    print("   observed shape change (per 1,000 wk):", [round(y, 2) for y in ys], " corr with growth", round(float(np.corrcoef(xs, ys)[0, 1]), 2))
    print("   mean slide above k (per 1,000 wk):  ", [round(s, 2) for s in ss], " corr with growth", round(float(np.corrcoef(xs, ss)[0, 1]), 2))
