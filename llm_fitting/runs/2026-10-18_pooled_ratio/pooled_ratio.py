"""One number per ladder: the pooled ratio of measured crossings to sigma^2/(2S), from the saved boundary
outputs with their bootstrap draws (no data file opened).

Pooled ratio = geometric mean of R~/c over the scored cut-offs (K/16 ... K, where the boundary strata hold
at least a few hundred names on every ladder; and K/64 ... K on comments, whose head strata are large);
interval = 5-95% of the same statistic over the bootstrap draws, draw t at every cut-off taken together.
Also the ratio's spread across cut-offs (the largest departure from 1), and a cross-ladder pooled value.

    python llm_fitting/runs/2026-10-18_pooled_ratio/pooled_ratio.py
"""
import json
from pathlib import Path

import numpy as np

R = Path(__file__).resolve().parents[1]
SRC = {"Reddit comments": R / "2026-10-14_boundary_law/cells/boundary__reddit_comments_ext.json",
       "Reddit submissions": R / "2026-10-14_boundary_law/cells/boundary__reddit_submissions_long.json",
       "Facebook pages": R / "2026-10-14_boundary_law/cells/boundary__facebook_a__block13.json",
       "Instagram (a-matching)": R / "2026-10-15_instagram/ig_boundary__instagram_hm.json"}


def load(f):
    d = json.loads(f.read_text())
    return d["apparatus"] if "apparatus" in d else d


out = {}
logs_all, draws_all = [], []
for name, f in SRC.items():
    d = load(f)
    K = d["K"]
    for label, kmin in (("K/16 … K", K // 16 - 1), ("K/64 … K", 0)):
        rows = [r for r in d["pooled"] if r["k"] >= kmin]
        if not all("ratio_draws" in r for r in rows):
            raise SystemExit(f"{name}: no saved draws; rerun the boundary analysis")
        lr = np.log([r["ratio"] for r in rows])
        D = np.array([r["ratio_draws"] for r in rows], dtype=float)          # cut-offs x draws
        ok = np.all(D > 0, axis=0)
        ld = np.log(D[:, ok]).mean(axis=0)
        out.setdefault(name, {})[label] = dict(
            cutoffs=[r["k"] for r in rows], pooled=float(np.exp(lr.mean())),
            interval=[float(np.exp(np.quantile(ld, 0.05))), float(np.exp(np.quantile(ld, 0.95)))],
            min=float(np.exp(lr.min())), max=float(np.exp(lr.max())), draws_used=int(ok.sum()))
        if label == "K/16 … K":
            logs_all.append(lr.mean())
            draws_all.append(ld)
n = min(len(x) for x in draws_all)
cross = np.mean([x[:n] for x in draws_all], axis=0)
out["all four ladders, K/16 … K"] = dict(pooled=float(np.exp(np.mean(logs_all))),
                                         interval=[float(np.exp(np.quantile(cross, 0.05))), float(np.exp(np.quantile(cross, 0.95)))])
Path(__file__).with_name("pooled_ratio.json").write_text(json.dumps(out, indent=1))
for name, v in out.items():
    if "pooled" in v:
        print(f"{name}: {v['pooled']:.3f} ({v['interval'][0]:.3f}-{v['interval'][1]:.3f})")
        continue
    for label, x in v.items():
        print(f"{name}, {label}: pooled {x['pooled']:.3f} (5-95% {x['interval'][0]:.3f}-{x['interval'][1]:.3f}); range over cut-offs {x['min']:.2f}-{x['max']:.2f}; draws {x['draws_used']}")
