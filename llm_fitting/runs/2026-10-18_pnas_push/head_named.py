"""R5 of PLAN.md (descriptive, named; owner permission MODEL_STATUS 2z-dh section 10): who the head is and who fell.

Blocks: 52 weeks aligned to the panel's end (2019..2022).  Shares are of the year's top-K total (K the ladder's
universe), as the paper's arithmetic shares.  Head = top K/64 by the 2019 total.

    python llm_fitting/runs/2026-10-18_pnas_push/head_named.py <reddit_submissions_long|reddit_comments_ext>
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

pn = sys.argv[1]
K = m1.PANELS[pn]["K"]
k = K // 64
df = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[pn]["platform"]])
T = int(df["period"].max()) + 1
nb = T // 52
start = T - nb * 52
out = dict(panel=pn, K=K, k=k, T=T, years=[2019 + b for b in range(nb)])

# rank-1 occupant
r1 = df[df["rank"] == 1].sort_values("period")
vc = r1["entity_id"].value_counts()
out["rank1"] = dict(weeks=int(len(r1)), occupants={str(n): int(c) for n, c in vc.items()},
                    changes=int((r1["entity_id"].values[1:] != r1["entity_id"].values[:-1]).sum()))

# block totals
df = df[df["period"] >= start].copy()
df["block"] = (df["period"] - start) // 52
df["a"] = np.expm1(df["X"])
tot = df.groupby(["block", "entity_id"])["a"].sum().unstack(0).fillna(0.0)
topK = {b: tot[b].sort_values(ascending=False).iloc[:K] for b in range(nb)}
D = {b: float(topK[b].sum()) for b in range(nb)}
share = {b: tot[b] / D[b] for b in range(nb)}
head19 = topK[0].index[:k]
head22 = topK[nb - 1].index[:k]
s19, s22 = share[0][head19], share[nb - 1][head19]
d = (s22 - s19).sort_values()
loss = -d[d < 0].sum()
out["head"] = dict(
    share_2019=float(s19.sum()), share_2022_same_names=float(s22.sum()),
    share_2022_head_2022=float(share[nb - 1][head22].sum()),
    frac_fell=float((d < 0).mean()), n=int(len(d)),
    top10_losers=[(str(n), float(s19[n]), float(s22[n])) for n in d.index[:10]],
    top10_loss_frac=float(-d.iloc[:10].sum() / loss) if loss > 0 else None,
    top10_gainers=[(str(n), float(s19[n]), float(s22[n])) for n in d.index[::-1][:10]],
    rank_2022_of_2019_head=[(str(n), int((topK[nb - 1].index.get_loc(n) + 1) if n in topK[nb - 1].index else -1)) for n in head19],
    entrants_2022=[(str(n), float(share[nb - 1][n]), int((topK[0].index.get_loc(n) + 1) if n in topK[0].index else -1)) for n in head22 if n not in set(head19)],
    head_2019_top10=[(str(n), float(s19[n])) for n in head19[:10]],
    head_2022_top10=[(str(n), float(share[nb - 1][n])) for n in head22[:10]],
    share_by_year={2019 + b: float(share[b][topK[b].index[:k]].sum()) for b in range(nb)})
h = out["head"]
print(f"{pn}: rank-1 occupants {list(out['rank1']['occupants'].items())[:4]} over {out['rank1']['weeks']} weeks, {out['rank1']['changes']} changes")
print(f"  head (top {k}) share by year {np.round(list(h['share_by_year'].values()), 3).tolist()}; 2019 names: 2019 {h['share_2019']:.3f} -> 2022 {h['share_2022_same_names']:.3f}; "
      f"fell {h['frac_fell']:.2f} of {h['n']}; top-10 losers carry {h['top10_loss_frac']:.2f} of the loss")
print("  2019 top 10:", [(n, round(s, 3)) for n, s in h["head_2019_top10"]])
print("  2022 top 10:", [(n, round(s, 3)) for n, s in h["head_2022_top10"]])
print("  largest losers:", [(n, round(a, 3), round(b, 3)) for n, a, b in h["top10_losers"]])
print("  largest gainers:", [(n, round(a, 3), round(b, 3)) for n, a, b in h["top10_gainers"]])
print(f"  entrants to the 2022 head ({len(h['entrants_2022'])}):", [(n, round(s, 4), r) for n, s, r in h["entrants_2022"]][:20])
Path(__file__).with_name(f"head_named__{pn}.json").write_text(json.dumps(out, indent=1))
