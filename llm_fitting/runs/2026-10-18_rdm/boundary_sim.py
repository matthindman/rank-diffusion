"""S6 of DOSSIER.md: the boundary ratio (2z-cm estimator, unchanged) on panels simulated by the frozen moving-home
RDM and by model A's fixed-home dynamics, from the same start as structure_suite.py.

    python llm_fitting/runs/2026-10-18_rdm/boundary_sim.py <panel> [seeds]
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
import rankdiff_rdm as rdm  # noqa: E402

panel = sys.argv[1]
SEEDS = int(sys.argv[2]) if len(sys.argv) > 2 else 5
K = m1.PANELS[panel]["K"]
EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True,
           mix_hetero=panel != "reddit_submissions_long")
df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]]), K, buffer_mult=4)
T = int(df["period"].max()) + 1
t0 = T - (T // 52) * 52 + 52
base = mrd.estimate(df, **EST)
p = rdm.estimate_rdm(df, K, base=base, jumps=False)
levels = rk._filtered_levels(df[df["period"] < t0].copy(), base).to_numpy()
ks_pool = [K // 8, K // 4, K // 2, K]
out = dict(panel=panel, K=K, seeds=SEEDS, t0=int(t0), weeks=int(T - t0))
for lasting in ("movinghome", "home"):
    pooled, byk = [], {}
    for s in range(SEEDS):
        Xs, ids = rdm.simulate_panel(p, levels, T - t0, seed=s, lasting=lasting)
        res = bd.analyse(rdm.panel_long_frame(Xs, ids), K, 26, boot=10)
        rat = {r["k"]: r["ratio"] for r in res.get("pooled", [])}
        for k, v in rat.items():
            byk.setdefault(int(k), []).append(float(v))
        if all(k in rat for k in ks_pool):
            pooled.append(float(np.exp(np.mean([np.log(rat[k]) for k in ks_pool]))))
        print(f"  {lasting} seed {s}: ratio by k " + ", ".join(f"{k}: {v:.2f}" for k, v in sorted(rat.items())), flush=True)
    out[lasting] = dict(pooled_K8_K=pooled, by_k={k: float(np.mean(v)) for k, v in byk.items()})
    print(f"{panel} {lasting}: pooled K/8..K {np.mean(pooled):.3f} (seeds {min(pooled):.3f}-{max(pooled):.3f})" if pooled else f"{panel} {lasting}: not adjudicated", flush=True)
Path(__file__).with_name(f"boundary_sim__{panel}.json").write_text(json.dumps(out, indent=1))
