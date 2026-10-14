"""POST HOC (after 2z-co): was the comments head's gain from about March to September 2021 broad,
or carried by a few communities?  For each half-year pair of 26-week blocks and k in K/64, K/16,
K/4: the persisting top k at the start (T_a), each member's change against the change of U_K,
the mean (the members' part of the shape change, -E_k), the median, the share of the summed gain
carried by the 10 largest gainers, and the share of members gaining.  No names are read.

    python llm_fitting/runs/2026-10-15_comments_2021_head/broad_or_few.py <panel>
"""
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

panel = sys.argv[1]
cfg = m1.PANELS[panel]
df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
K = cfg["K"]
X, Rk, floors, outside, ids = m1.matrices(df, K)
T = X.shape[0]
nb = T // 26
start = T - nb * 26
M = ba.block_means(X, 26, start=start, min_frac=m1.PRES)
dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).tolist()
print(f"{panel}: per member over the half-year, log units against the change of U_K")
for a in range(1, nb - 1):
    b = a + 1
    P = np.isfinite(M[a]) & np.isfinite(M[b])
    La, Lb = M[a][P], M[b][P]
    oa = np.argsort(-La, kind="stable")
    dUK = np.sort(Lb)[::-1][:K].mean() - np.sort(La)[::-1][:K].mean()
    cells = []
    for k in (K // 64, K // 16, K // 4):
        d = (Lb - La)[oa[:k]] - dUK
        top10 = np.sort(d)[::-1][:10]
        pos = d[d > 0].sum()
        Uk_b = np.sort(Lb)[::-1][:k].mean()
        shape = (Uk_b - np.sort(Lb)[::-1][:K].mean()) - (La[oa[:k]].mean() - np.sort(La)[::-1][:K].mean())
        cells.append(f"k={k}: shape {shape:+.3f} = members {d.mean():+.3f} + reordering {shape - d.mean():+.3f}; "
                     f"median {np.median(d):+.3f}; gaining {np.mean(d > 0):.0%}; top-10 gainers {top10.sum() / max(pos, 1e-12):.0%} of all gains; "
                     f"mean without top 10 {np.sort(d)[:-10].mean():+.3f}")
    print(f"\n{dates[start + a * 26]} -> {dates[start + b * 26]}")
    for c in cells:
        print("  " + c)
