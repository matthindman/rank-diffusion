"""A6 of PLAN.md (addendum): the repaired structure harness and an out-of-period structure test.

Repairs: simulated reborn slots are new names in their own columns (as the data keep later entrants); shares are
arithmetic (weekly activity summed over each 52-week block; the year's top k by total within its top K).
Out of period: estimate on weeks < 108 (2019-2020) with the universe from those weeks only; simulate from the
filtered state at week 108 to the panel's end (2021-2022); arms model A's dynamics ('home') and the moving home
('movinghome', hold closure from the end-2020 ladder).  Data statistics use every community present.
Check A6-5 (deviation from PLAN: on the same out-of-period simulations rather than a separate in-sample run):
simulated retention with entrants kept against entrants dropped (the old suite's rule).

    python llm_fitting/runs/2026-10-18_pnas_push/structure_oop.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
import rankdiff_rdm as rdm  # noqa: E402

panel = sys.argv[1]
ARMS = tuple(sys.argv[2].split(",")) if len(sys.argv) > 2 else ("home", "movinghome")     # R3-C: "home,movinghome_vp"
TAG = "" if ARMS == ("home", "movinghome") else "__" + "_".join(ARMS)
K = m1.PANELS[panel]["K"]
BLOCK, PRES, SEEDS = 52, 0.9, 20
EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True,
           mix_hetero=panel != "reddit_submissions_long")
KS = (K // 64, K // 16, K // 8, K)
full = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]])
T = int(full["period"].max()) + 1
nb = T // BLOCK
start = T - nb * BLOCK
t_origin = start + 2 * BLOCK                                   # 2021-01: blocks 0-1 train, blocks 2-3 forecast
dfd = mrd.restrict_universe(full, K, buffer_mult=4)            # data: every community present over the panel
piv = dfd.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
dcols = {e: j for j, e in enumerate(piv.columns)}
Xd = piv.to_numpy(float)
del dfd, piv


def bmeans(X, b0, nblk):
    out = []
    for b in range(nblk):
        seg = X[b0 + b * BLOCK:b0 + (b + 1) * BLOCK]
        ok = np.isfinite(seg).mean(0) >= PRES
        with np.errstate(all="ignore"):
            out.append(np.where(ok, np.nanmean(seg, 0), np.nan))
    return np.array(out)


def btotals(X, b0, nblk):
    A = np.nan_to_num(np.expm1(X), nan=0.0)
    return np.array([A[b0 + b * BLOCK:b0 + (b + 1) * BLOCK].sum(0) for b in range(nblk)])


def top(v, k):
    o = np.flatnonzero(np.isfinite(v) & (v > -np.inf))
    return o[np.argsort(-v[o], kind="mergesort")][:k]


def retention(Ba, Bb):
    return {int(k): float(np.intersect1d(top(Ba, k), top(Bb, k)).size / k) for k in KS}


def ashare(tot):
    o = np.argsort(-tot, kind="stable")
    return float(tot[o[:K // 64]].sum() / tot[o[:K]].sum())


def momentum(B0, B1, B2):
    t = top(B0, K)
    ok = t[np.isfinite(B1[t]) & np.isfinite(B2[t])]
    return float(spearmanr(B1[ok] - B0[ok], B2[ok] - B1[ok]).correlation)


def kurt26(X):
    d = (X[26:] - X[:-26]).ravel()
    d = d[np.isfinite(d)]
    d = (d - d.mean()) / d.std()
    return float(np.mean(d ** 4) - 3)


# ---- data statistics, blocks 0..3 (block 1 = 2020, blocks 2-3 = 2021-2022)
Bd = bmeans(Xd, start, nb)
Td = btotals(Xd, start, nb)
data = dict(ret_23=retention(Bd[2], Bd[3]), ret_01=retention(Bd[0], Bd[1]), share_1=ashare(Td[1]), share_3=ashare(Td[3]),
            momentum=momentum(Bd[1], Bd[2], Bd[3]))
print(f"{panel}: data retention 2021->2022 {data['ret_23']}; carried forward (2019->2020) {data['ret_01']}; "
      f"arithmetic share K/64 2020 {data['share_1']:.3f} -> 2022 {data['share_3']:.3f}; momentum {data['momentum']:+.3f}", flush=True)

# ---- out-of-period fit on weeks < t_origin, universe from those weeks only
dtr = mrd.restrict_universe(full, K, buffer_mult=4, member_window=t_origin)
dtr = dtr[dtr["period"] < t_origin].copy()
base = mrd.estimate(dtr, **EST)
p = rdm.estimate_rdm(dtr, K, base=base, jumps=False, variant="hold")
levels = rk._filtered_levels(dtr, base)
start_ids = levels.index.to_numpy()
del full
print(f"  fit on weeks < {t_origin}: sigma0 {p.sigma0:.4f}, beta {p.beta:+.3f}; {start_ids.size} names at the origin", flush=True)


def sim_matrix(Xs, ids):
    """Entrants as identities: one column per occupant id (reborn slots are new names)."""
    u, inv = np.unique(ids, return_inverse=True)
    inv = inv.reshape(ids.shape)
    M = np.full((Xs.shape[0], u.size), np.nan)
    rows = np.repeat(np.arange(Xs.shape[0])[:, None], Xs.shape[1], 1)
    M[rows, inv] = Xs
    return M, u


arms = {}
for lasting in ARMS:
    runs = []
    for sd in range(SEEDS):
        Xs, ids = rdm.simulate_panel(p, levels.to_numpy(), T - t_origin, seed=sd, lasting=lasting)
        Ms, u = sim_matrix(Xs, ids)
        Bs = bmeans(Ms, 0, 2)
        Ts = btotals(Ms, 0, 2)
        # momentum: block 1 from the data for the names that start the simulation (ids 0..N-1 are the start names)
        B1 = np.full(u.size, np.nan)
        is_start = u < start_ids.size
        cidx = np.array([dcols.get(e, -1) for e in start_ids[u[is_start]]])          # -1: not in the data universe
        B1[np.flatnonzero(is_start)[cidx >= 0]] = Bd[1][cidx[cidx >= 0]]
        startcols = np.flatnonzero(is_start)
        Bdrop = Bs[:, startcols]
        runs.append(dict(ret_23=retention(Bs[0], Bs[1]), ret_23_dropped=retention(Bdrop[0], Bdrop[1]), share_3=ashare(Ts[1]),
                         momentum=momentum(B1, Bs[0], Bs[1]), kurt=kurt26(Ms[:, startcols])))
    summ = dict(ret_23={int(k): float(np.mean([r["ret_23"][k] for r in runs])) for k in KS},
                ret_23_dropped={int(k): float(np.mean([r["ret_23_dropped"][k] for r in runs])) for k in KS},
                share_3=float(np.mean([r["share_3"] for r in runs])), share_3_q=[float(np.quantile([r["share_3"] for r in runs], q)) for q in (0.05, 0.95)],
                momentum=float(np.mean([r["momentum"] for r in runs])), kurt=float(np.mean([r["kurt"] for r in runs])))
    summ["ret_mae"] = float(np.mean([abs(summ["ret_23"][k] - data["ret_23"][k]) for k in KS]))
    arms[lasting] = summ
    print(f"  {lasting}: retention {summ['ret_23']} (MAE {summ['ret_mae']:.3f}); share 2022 {summ['share_3']:.3f} "
          f"({summ['share_3_q'][0]:.3f}-{summ['share_3_q'][1]:.3f}); momentum {summ['momentum']:+.3f}; kurtosis {summ['kurt']:.2f}; "
          f"retention with entrants dropped {summ['ret_23_dropped']}", flush=True)
data["kurt"] = kurt26(Xd[t_origin:][:, [dcols[e] for e in start_ids if e in dcols]])
base_mae = float(np.mean([abs(data["ret_01"][k] - data["ret_23"][k]) for k in KS]))
print(f"  baselines: retention carried forward MAE {base_mae:.3f}; share carried forward {data['share_1']:.3f} (observed {data['share_3']:.3f}); "
      f"data kurtosis of 26-week changes (start names) {data['kurt']:.2f}", flush=True)
out = dict(panel=panel, K=K, t_origin=t_origin, data=data, arms=arms, baseline_ret_mae=base_mae,
           params=dict(sigma0=p.sigma0, beta=p.beta))
Path(__file__).with_name(f"structure_oop__{panel}{TAG}.json").write_text(json.dumps(out, indent=1, default=float))
