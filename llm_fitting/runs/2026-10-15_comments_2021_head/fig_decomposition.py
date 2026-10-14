"""Figure: each half-year's change of the ladder's shape split exactly into members' movement and
reordering (crossings), from the saved outputs of broad_or_few.py.

    python llm_fitting/runs/2026-10-15_comments_2021_head/fig_decomposition.py
"""
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

HERE = Path(__file__).resolve().parent
PAT = re.compile(r"k=(\d+): shape ([+-][\d.]+) = members ([+-][\d.]+) \+ reordering ([+-][\d.]+)")


def parse(f):
    rows, label = {}, None
    for line in (HERE / f).read_text().splitlines():
        if "->" in line:
            label = line.strip()
        m = PAT.search(line)
        if m and label:
            rows.setdefault(int(m.group(1)), []).append((label, *map(float, m.groups()[1:])))
    return rows


fig, ax = plt.subplots(1, 2, figsize=(12, 4.4), sharey=True)
for a, (f, k, title) in zip(ax, (("output_comments.txt", 781, "Reddit comments, top 781 (K/16)"),
                                 ("output_submissions.txt", 312, "Reddit submissions, top 312 (K/16)"))):
    r = parse(f)[k]
    x = np.arange(len(r))
    mem = np.array([v[2] for v in r])
    reo = np.array([v[3] for v in r])
    shp = np.array([v[1] for v in r])
    a.bar(x - 0.2, mem, 0.4, color="#1f5fa8", label="members' movement against the pack")
    a.bar(x + 0.2, reo, 0.4, color="#d9772b", label="reordering (crossings at the boundary)")
    a.plot(x, shp, "ko-", label="change of shape (their sum)")
    a.axhline(0, color="#777777", lw=0.8)
    labs = ["Oct 19–Mar 20", "Apr–Sep 20", "Oct 20–Mar 21", "Apr–Sep 21", "Oct 21–Mar 22", "Apr–Sep 22"]
    a.set_xticks(x)
    a.set_xticklabels(labs[:len(r)], fontsize=8, rotation=20)
    a.set_title(title)
    a.set_ylabel("log units over the half-year")
h, l = ax[0].get_legend_handles_labels()
fig.legend(h, l, fontsize=8, loc="lower center", ncol=3, frameon=False)
fig.suptitle("Crossings return a steady amount each half-year; the ladder moves when members' drift by rank changes", fontsize=11)
fig.tight_layout(rect=(0, 0.07, 1, 1))
fig.savefig(HERE / "fig_shape_decomposition.png", dpi=150)
print("written")
