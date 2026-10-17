"""O1-O3 of DESIGN_depth.md: crossings in 2020-2022 predicted from volatility measured in 2019-mid 2020.

13-week blocks.  Volatility: the variance route on the block-0 cohort over blocks 1, 3, 5 (the 'variance' rows of
mts_boundary_law.analyse).  Slope: S at the origin block a of each crossing window.  Crossings: the spacing
difference 2-4 at starts a = 6 ... 11 (blocks 6 ... 15, disjoint from the volatility window).  In-period
comparison (O3): the same starts with each start's own volatility is not in the saved run, so the in-period
reference is the same starts' crossings over the ratio of 2z-cu (pooled c), reported for context.

    python llm_fitting/runs/2026-10-18_altitude/out_of_time.py comments      (saved run; no data file opened)
    python llm_fitting/runs/2026-10-18_altitude/out_of_time.py submissions   (runs analyse once, 13-week blocks)
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import mts_balance_law as bl  # noqa: E402

which = sys.argv[1]
cells = ROOT / "llm_fitting/runs/2026-10-14_boundary_law/cells"
if which == "comments":
    d = json.loads((cells / "boundary__reddit_comments_ext__block13__K12500.json").read_text())
    band = (1 / 8, 1.0)
else:
    import minimal_rankdiff as mrd  # noqa: E402
    import mts_boundary_law as bd  # noqa: E402
    import mts_step1 as m1  # noqa: E402
    f = Path(__file__).with_name("boundary__reddit_submissions_long__block13.json")
    if f.exists():
        d = json.loads(f.read_text())
    else:
        cfg = m1.PANELS["reddit_submissions_long"]
        res = bd.analyse(mrd.load_panel(mrd.PLATFORMS[cfg["platform"]]), cfg["K"], 13)
        for s in res["starts"]:                      # drop bootstrap arrays before saving
            for D, rows in s["diff"].items():
                for r in rows:
                    for key in ("R_bs", "E_bs", "L_bs"):
                        v = r.pop(key, None)
                        if v is not None:
                            r[key + "_q"] = [float(np.nanquantile(v, 0.05)), float(np.nanquantile(v, 0.95))]
        for r in res.get("pooled", []):
            r.pop("ratio_draws", None)
        d = res
        f.write_text(json.dumps(d, default=float))
    band = (1 / 4, 1.0)
K = d["K"]
st, ks = bl.strata(K), bl.cutoffs(K)
var = d["variance"]
vn = [v["n"] for v in var]
s2 = {k: bl._at_boundary([v["sigma2"] for v in var], vn, j) for j, k in ks}
late = [s for s in d["starts"] if s["start"] >= 6 and "2-4" in s["diff"]]
print(f"{which}: K {K}, {d['n_blocks']} blocks, late starts {[s['start'] for s in late]}")
rows = []
for i, (j, k) in enumerate(ks):
    R = np.mean([s["diff"]["2-4"][i]["R"] for s in late])
    Sa = np.mean([s["single"]["2"][i]["S_a"] for s in late])
    c = s2[k] / (2 * Sa)
    pooled_c = next((r["c"] for r in d["pooled"] if r["k"] == k), np.nan)
    rows.append(dict(k=int(k), R_late=float(R), S_a=float(Sa), sigma2_early=float(s2[k]), c_pred=float(c), ratio_out_of_time=float(R / c),
                     ratio_in_period_pooled_c=float(R / pooled_c)))
    print(f"  k={k:>6}: crossings {1000 * R:.2f} per 1,000 wk; predicted from 2019-20 volatility {1000 * c:.2f}; out-of-time ratio {R / c:.3f}; "
          f"(same crossings over the pooled c of all starts: {R / pooled_c:.3f})")
sel = [r for r in rows if band[0] * K - 1 <= r["k"] <= band[1] * K + 1]
g = float(np.exp(np.mean([np.log(r["ratio_out_of_time"]) for r in sel])))
g_in = float(np.exp(np.mean([np.log(r["ratio_in_period_pooled_c"]) for r in sel])))
print(f"  pooled out-of-time ratio over k/K in [{band[0]:.3f}, 1]: {g:.3f}  (in [0.85, 1.18]: {0.85 <= g <= 1.18}); with pooled c: {g_in:.3f}")
Path(__file__).with_name(f"out_of_time__{which}.json").write_text(json.dumps(dict(rows=rows, pooled=g, pooled_with_all_start_c=g_in, band=band), indent=1))
