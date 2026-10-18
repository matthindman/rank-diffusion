"""A1 of PLAN.md: scale-free turnover from scale-free churn (kinematics).

The replaced fraction of the top k between two blocks is predicted as
    v_k + (1 - v_k) * E[(dtheta_k - d_i)^+] / S_k
v_k: share of the top k absent at the next block; d_i: change of names ranked (k/2, k] present at both blocks;
dtheta_k: change of the rank-k level; S_k: local slope (minus d level / d log rank) over ranks k/sqrt2..k*sqrt2 at
the start block.  Locally linear density; changes independent of the distance to the boundary within the band.

POST HOC (2z-dg, after the registered version over-predicted by ~30% on submissions): on a log-linear ladder the
density of names u above the boundary is (k/S) exp(-u/S), not k/S, so the exact kinematics is
    v_k + (1 - v_k) * E[1 - exp(-(dtheta_k - d_i)^+ / S_k)]
reported beside the registered formula ('pred_exp'); no parameter.

    python llm_fitting/runs/2026-10-18_pnas_push/turnover_kinematics.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402


def order(L):
    o = np.flatnonzero(np.isfinite(L))
    return o[np.argsort(-L[o], kind="mergesort")]


def cell(M, a, b, k):
    La, Lb = M[a], M[b]
    oa, ob = order(La), order(Lb)
    Ta, Tb = oa[:k], ob[:k]
    obs = 1.0 - np.intersect1d(Ta, Tb).size / k
    dth = Lb[ob[k - 1]] - La[oa[k - 1]]
    lo, hi = int(np.floor(k / np.sqrt(2))), int(np.ceil(k * np.sqrt(2)))
    r = np.arange(lo, hi + 1)
    S = float(-np.polyfit(np.log(r), La[oa[lo - 1:hi]], 1)[0])
    v = float(np.mean(~np.isfinite(Lb[Ta])))
    band = oa[k // 2:k]
    band = band[np.isfinite(Lb[band])]
    d = Lb[band] - La[band]
    e = float(np.mean(np.maximum(dth - d, 0.0)))
    ex = float(np.mean(1.0 - np.exp(-np.maximum(dth - d, 0.0) / S)))
    return dict(k=k, obs=obs, pred=v + (1 - v) * e / S, pred_exp=v + (1 - v) * ex, v=v, S=S, sd=float(np.std(d, ddof=1)), e=e,
                dtheta=float(dth))


out = {}
for panel, block in (("reddit_submissions_long", 52), ("reddit_comments_ext", 52), ("facebook_a", 26)):
    K = m1.PANELS[panel]["K"]
    df = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]])
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    del df
    T = X.shape[0]
    nb = T // block
    M = ba.block_means(X, block, start=T - nb * block, min_frac=m1.PRES)
    ks = [K // 64 * 2 ** i for i in range(6)]
    rows = [dict(pair=[a, a + 1], **cell(M, a, a + 1, k)) for a in range(nb - 1) for k in ks]
    out[panel] = rows
    print(f"{panel} (K {K}, {block}-week blocks)", flush=True)
    for r in rows:
        print(f"   blocks {r['pair']} k {r['k']:>5}: replaced obs {r['obs']:.3f} pred {r['pred']:.3f} (ratio {r['pred'] / r['obs']:.2f}), "
              f"exact {r['pred_exp']:.3f} (ratio {r['pred_exp'] / r['obs']:.2f}); "
              f"absent {r['v']:.3f}, S {r['S']:.2f}, sd of boundary change {r['sd']:.3f}, E[(dtheta-d)+] {r['e']:.3f}", flush=True)
    rat = np.array([r["pred"] / r["obs"] for r in rows])
    rex = np.array([r["pred_exp"] / r["obs"] for r in rows])
    print(f"   exact (post hoc) within [0.8, 1.25]: {np.mean((rex >= 0.8) & (rex <= 1.25)):.0%}, median ratio {np.median(rex):.2f}", flush=True)
    print(f"   within [0.8, 1.25]: {np.mean((rat >= 0.8) & (rat <= 1.25)):.0%} of {rat.size} cells; "
          f"CV across k of predicted (by pair): {[round(float(np.std([r['pred'] for r in rows if r['pair'][0] == a]) / np.mean([r['pred'] for r in rows if r['pair'][0] == a])), 3) for a in range(nb - 1)]}; "
          f"CV of sd: {[round(float(np.std([r['sd'] for r in rows if r['pair'][0] == a]) / np.mean([r['sd'] for r in rows if r['pair'][0] == a])), 3) for a in range(nb - 1)]}; "
          f"CV of S: {[round(float(np.std([r['S'] for r in rows if r['pair'][0] == a]) / np.mean([r['S'] for r in rows if r['pair'][0] == a])), 3) for a in range(nb - 1)]}", flush=True)
Path(__file__).with_name("turnover_kinematics.json").write_text(json.dumps(out, indent=1))
