"""Table 1 of the manuscript, built by one code path (referee round 8, H9; PLAN.md R17-B): every cell, the data's
included, is a `null_worlds.stats` statistic (turnover from block 1 to block 2, the pooled boundary ratio, the gapped
persistence at 13 weeks) or the R17 asymmetry statistic D, computed identically on data and worlds.

  data:            r17_table_data__reddit_submissions_long.json, r17_asym_data__reddit_submissions_long.json
  atlas worlds:    null_worlds_r7_summary.json (20 runs each)
  matched control: r12_worlds__reddit_submissions_long__control_num.json (20 runs)
  joint, decline:  the per-seed files r16_world__reddit_submissions_long__{joint,decline}_*__s*.json
  D in worlds:     r17_asym_worlds__reddit_submissions_long__*.json

The code compares the data with each world's 5-95% range across runs: in, above, below (edge: within 0.005 of a bound).
Writes r17_table1.json and prints the LaTeX rows.

    python r17_table1.py
"""
import json
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
PN = "reddit_submissions_long"
COLS = [("sb_flat", "constant-slope, size-blind"), ("firm_flat", "constant-slope, firm-like"), ("sb_data", "data's ladder, size-blind"),
        ("firm_data", "data's ladder, firm-like"), ("neutral", "neutral copying"), ("rdm", "calibrated model"),
        ("control", "matched control"), ("joint", "fixed growth diff., joint"), ("decline", "decline state")]
TAGS = {"joint": "joint_s0.8_head0.25_km0.5_m0.8", "decline": "decline_pi0.25_s0.7_hl208_m1.0", "control": "control_num_"}


def band(v):
    v = np.array([x for x in v if x is not None and np.isfinite(x)], float)
    return dict(mean=float(v.mean()), q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)), n=int(v.size))


def per_seed(tag):
    W = [json.loads(f.read_text()) for f in sorted(HERE.glob(f"r16_world__{PN}__{tag}__s*.json"))]
    return dict(turn10=band([w["turnover"]["10"] for w in W]), turn78=band([w["turnover"]["78"] for w in W]),
                turn2500=band([w["turnover"]["2500"] for w in W]), boundary=band([w.get("boundary_pooled") for w in W]),
                gap13=band([w["gapped"]["13"] for w in W]))


def code(x, b):
    if b is None:
        return ""
    lo, hi = b["q05"], b["q95"]
    if abs(x - lo) < 0.005 or abs(x - hi) < 0.005:
        return "edge"
    return "in" if lo <= x <= hi else ("above" if x > hi else "below")


def main():
    S7 = json.loads((HERE / "null_worlds_r7_summary.json").read_text())
    W = {}
    for key, _ in COLS:
        if key in S7:
            s = S7[key]
            W[key] = dict(turn10=s["turnover"]["10"], turn78=s["turnover"]["78"], turn2500=s["turnover"]["2500"],
                          boundary=s["boundary_pooled"] if isinstance(s["boundary_pooled"], dict) else None, gap13=s["gapped"]["13"])
    c = json.loads((HERE / f"r12_worlds__{PN}__control_num.json").read_text())
    W["control"] = dict(turn10=c["turnover"]["10"], turn78=c["turnover"]["78"], turn2500=c["turnover"]["2500"], boundary=c["boundary"],
                        gap13=c["gapped_stats"]["13"])
    W["joint"], W["decline"] = per_seed(TAGS["joint"]), per_seed(TAGS["decline"])
    for key in ("control", "joint", "decline"):
        f = HERE / f"r17_asym_worlds__{PN}__{TAGS[key]}.json"
        W[key]["D"] = json.loads(f.read_text())["D"] if f.exists() else None
    d = json.loads((HERE / f"r17_table_data__{PN}.json").read_text())
    a = json.loads((HERE / f"r17_asym_data__{PN}.json").read_text())
    data = dict(turn78=d["turnover"]["78"], turn2500=d["turnover"]["2500"], turn10=d["turnover"]["10"], boundary=d["boundary_pooled"],
                gap13=d["gapped"]["13"], D=a["primary"]["D"])
    rows = [("turnover, top 78", "turn78", "{:.2f}"), ("turnover, top 2,500", "turn2500", "{:.2f}"), ("turnover, top 10", "turn10", "{:.2f}"),
            ("crossings / predicted", "boundary", "{:.2f}"), ("persistence, 13-wk gap", "gap13", "{:+.2f}"), ("asymmetry $D$", "D", "{:+.2f}")]
    out = dict(data=data, worlds=W, rows=[])
    for lab, key, fmt in rows:
        cells = [fmt.format(data[key]).replace("+", "$+$").replace("-", "$-$")]
        for ck, _ in COLS:
            b = W.get(ck, {}).get(key)
            if b is None:
                cells.append("")
                continue
            cells.append((fmt.format(b["mean"]).replace("+", "$+$").replace("-", "$-$") + " " + code(data[key], b)).strip())
        line = f"{lab} & " + " & ".join(cells) + r" \\"
        out["rows"].append(line)
        print(line)
    (HERE / "r17_table1.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
