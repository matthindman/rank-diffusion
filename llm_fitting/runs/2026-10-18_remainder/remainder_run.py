"""The boundary statistic on one survivor population, with its exact finite-horizon split (DESIGN.md).

    python llm_fitting/runs/2026-10-18_remainder/remainder_run.py <panel> [block]
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

panel = sys.argv[1]
block = int(sys.argv[2]) if len(sys.argv) > 2 else 26
cfg = m1.PANELS[panel]
K = cfg["K"]
tag = "" if block == 26 else f"__block{block}"
saved = json.loads((ROOT / f"llm_fitting/runs/2026-10-14_boundary_law/cells/boundary__{panel}{tag}.json").read_text())
cpool = {r["k"]: r for r in saved["pooled"]}
df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
X, Rk, floors, outside, ids = m1.matrices(df, K)
T = X.shape[0]
nb = T // block
start = T - nb * block
M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
st, ks = bl.strata(K), bl.cutoffs(K)
rows = {}
for a in range(1, nb - 4):
    pr, pres = ba.perm_rank(Rk, start + (a - 1) * block, start + a * block, floors)
    which = bl._assign(pr, pres, st)
    for r in bd.remainder(M, which, st, ks, K, a, a + 2, a + 4):
        rows.setdefault(r["k"], []).append(r)
w = 2 * block
out = []
for k, rs in rows.items():
    Rt = float(np.mean([(r["R_ac"] - r["R_ab"]) / w for r in rs]))
    c = cpool[k]["c"]
    out.append(dict(k=k, starts=len(rs), n=[r["n"] for r in rs], R_common=Rt, ratio_common=Rt / c, ratio_original=cpool[k]["ratio"],
                    R_bc=float(np.mean([r["R_bc"] / w for r in rs])), C=float(np.mean([r["C"] / w for r in rs])),
                    max_residual=float(max(abs(r["residual"]) for r in rs))))
    print(f"{panel} k={k}: ratio common {Rt / c:.3f} vs original {cpool[k]['ratio']:.3f}; R_bc {1000 * out[-1]['R_bc']:.2f}, C {1000 * out[-1]['C']:+.2f} per 1,000 weeks; residual {out[-1]['max_residual']:.1e}", flush=True)
Path(__file__).with_name(f"remainder__{panel}{tag}.json").write_text(json.dumps(out, indent=1))
