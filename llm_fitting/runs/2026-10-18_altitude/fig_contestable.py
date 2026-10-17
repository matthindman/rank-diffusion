"""Candidate Figure 1 for the headline chosen in MEMO.md (saved outputs only; no data file opened).
  A  size does not protect: sd of one-year change of block-mean log activity against size (permanent-rank strata),
     three ladders, with the firm benchmark slope beta = 0.2 for reference (size_variance_output.txt);
  B  leaders are displaced at the same rate at every scale: one-year retention of the top k against k/K
     (runs/2026-10-15_puzzle);
  C  displacement predicted ahead of time: crossings 2020-22 against the prediction from 2019-20 volatility, by rank
     (out_of_time__*.json);
  D  only level growth (or a fixed-ladder rank diffusion) passes: the boundary ratio by rank for rank-model rivals
     calibrated to the data's turnover (rivals_calibrated.json) and for planted level growth (bridge_power.json).

    python llm_fitting/runs/2026-10-18_altitude/fig_contestable.py
"""
import json
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

HERE = Path(__file__).resolve().parent
R = HERE.parent
L = lambda p: json.loads(Path(p).read_text())  # noqa: E731
COL = {"reddit_comments_ext": "#1f6fb4", "reddit_submissions_long": "#e07b00", "facebook_a": "#2a9d5c"}
LAB = {"reddit_comments_ext": "Reddit comments", "reddit_submissions_long": "Reddit submissions", "facebook_a": "Facebook pages"}

fig, ax = plt.subplots(1, 4, figsize=(18, 4.3))
# A
a = ax[0]
txt = (HERE / "size_variance_output.txt").read_text().splitlines()
cur = None
pts = {}
for line in txt:
    m = re.match(r"(\w+): (raw one-year growth|lasting)", line)
    if m:
        cur = m.group(1)
        continue
    m = re.match(r"\s+ranks\s+\d+-\d+\s+n\s+(\d+)\s+size\s+(-?[\d.]+)\s+sd of one-year change ([\d.]+)", line)
    if m and cur:
        pts.setdefault(cur, []).append((float(m.group(2)), float(m.group(3))))
for pn, v in pts.items():
    v = np.array(v)
    beta = next((L(HERE / f"size_variance__{pn}.json")["raw"][k]["beta"] for k in L(HERE / f"size_variance__{pn}.json")["raw"]), np.nan)
    a.plot(v[:, 0], v[:, 1], "o-", color=COL[pn], label=f"{LAB[pn]} (β = {beta:.2f})")
    x0, y0 = v[-1]
    xs = np.linspace(v[:, 0].min(), v[:, 0].max(), 10)
    a.plot(xs, y0 * np.exp(-0.2 * (xs - x0)), ":", color=COL[pn], lw=1)
a.plot([], [], ":", color="grey", label="firms' slope, β = 0.2, for reference")
a.set_yscale("log")
a.set_xlabel("size: mean log activity of the rank stratum")
a.set_ylabel("sd of one-year change of log activity")
a.set_title("A  Size does not protect", loc="left", fontsize=10)
a.legend(fontsize=7)
# B
a = ax[1]
for pn in COL:
    d = L(R / "2026-10-15_puzzle" / f"puzzle__{pn}.json")
    K, bw = d["K"], d["block"]
    steps = int(round(52 / bw))
    ks = sorted(int(k) for k in d["turnover"])
    ret = [d["turnover"][str(k)][steps - 1] for k in ks]
    a.plot(np.array(ks) / K, ret, "o-", color=COL[pn], label=LAB[pn])
a.set_xscale("log")
a.set_ylim(0.5, 1.0)
a.set_xlabel("top set, k / K")
a.set_ylabel("share of the top k still in it a year later")
a.set_title("B  Displaced at the same rate at every scale", loc="left", fontsize=10)
a.legend(fontsize=7)
# C
a = ax[2]
for pn, key in (("reddit_comments_ext", "comments"), ("reddit_submissions_long", "submissions")):
    f = HERE / f"out_of_time__{key}.json"
    if not f.exists():
        continue
    rows = L(f)["rows"]
    c = np.array([1000 * r["c_pred"] for r in rows])
    m = np.array([1000 * r["R_late"] for r in rows])
    a.scatter(c, m, color=COL[pn], s=28, label=f"{LAB[pn]}: pooled {L(f)['pooled']:.2f}")
xs = np.linspace(0, 8, 10)
a.fill_between(xs, 0.85 * xs, 1.18 * xs, color="#dddddd", alpha=0.6, lw=0)
a.plot(xs, xs, color="grey", lw=1)
a.set_xlim(0, 8)
a.set_ylim(0, 8)
a.set_xlabel("predicted from 2019–20 volatility (per 1,000 weeks)")
a.set_ylabel("measured displacement gain, 2020–22")
a.set_title("C  A year ahead: comments within the margin, submissions short", loc="left", fontsize=9)
a.legend(fontsize=7, loc="upper left")
# D
a = ax[3]
rv = L(HERE / "rivals_calibrated.json")
for kind, lab, col in (("displacement", "jumps to random ranks (Iñiguez-type)", "#c0392b"), ("logrank", "log-rank diffusion, fixed ladder", "#8e44ad"),
                       ("local_swaps", "neighbour swaps", "#7f8c8d")):
    r = rv[kind]["ratio_by_k"]
    ks = sorted(int(k) for k in r)
    a.plot(np.array(ks) / 1600, [r[str(k)] for k in ks], "o-", color=col, label=lab)
a.axhspan(0.85, 1.18, color="#dddddd", alpha=0.6, lw=0)
a.axhline(1, color="grey", lw=1)
a.set_xscale("log")
a.set_ylim(0, 2.6)
a.set_xlabel("cut-off, k / K")
a.set_ylabel("measured / predicted displacement")
a.set_title("D  Rival rank models, calibrated to the data's turnover", loc="left", fontsize=9)
a.legend(fontsize=7, loc="upper left")
fig.tight_layout()
fig.savefig(HERE / "fig_contestable.png", dpi=150)
print("written fig_contestable.png")
