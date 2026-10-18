"""Structure validation of the frozen moving-home RDM against model A's fixed-home dynamics (DOSSIER.md §4, S1-S7).

In-sample, labelled so: parameters estimated on the whole panel (A's frozen structure-stack estimator minus the
two components the simulator does not carry, --two-scale and --stat-factor).  Both models start from the
same communities' filtered state at the end of the first block and simulate the rest of the panel; the SAME
estimators run on the data and on each simulation.

    python llm_fitting/runs/2026-10-18_rdm/structure_suite.py <panel> [seeds] [velocity]
    (velocity: the drift-by-name extension, DOSSIER addendum 2; then only the RDM is simulated)
"""
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
import rankdiff_rdm as rdm  # noqa: E402

panel = sys.argv[1]
SEEDS = int(sys.argv[2]) if len(sys.argv) > 2 else 20
VEL = len(sys.argv) > 3 and sys.argv[3] == "velocity"
MOVE = len(sys.argv) > 3 and sys.argv[3] == "move"      # POST HOC diagnostic (2z-dd): drift by rank from B's measured slides
K = m1.PANELS[panel]["K"]
BLOCK = 52 if panel.startswith("reddit") else 26
EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True,
           mix_hetero=panel != "reddit_submissions_long")
PRES = 0.9

df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]]), K, buffer_mult=4)
T = int(df["period"].max()) + 1
nb = T // BLOCK
start = T - nb * BLOCK                      # blocks aligned to the end, as every B analysis
t0 = start + BLOCK                          # simulations start after block 0
print(f"{panel}: K {K}, T {T}, {nb} blocks of {BLOCK} weeks from week {start}; simulation weeks {t0}..{T - 1}", flush=True)

base = mrd.estimate(df, **EST)
slides = None
if MOVE:
    law = json.loads((ROOT / "llm_fitting/runs/2026-10-14_balance_law/cells" / f"law__{panel}.json").read_text())
    prs = [q for q in law["pairs"] if q["primary"]]
    kk = np.array(sorted(r["k"] for r in prs[0]["rows"]), float)
    net = np.mean([[r["sbar"] - r["entry"] for r in sorted(q["rows"], key=lambda r: r["k"])] for q in prs], axis=0)
    # extend the measured cumulative net slide (K/64 ... K/2) to rank 1 (held) and to the universe bottom (held)
    kk = np.concatenate([[1.0], kk, [4.0 * K]])
    net = np.concatenate([[net[0]], net, [net[-1]]])
    slides = (kk, net)
    print(f"  move: cumulative net slide by k {dict(zip(kk.astype(int).tolist(), np.round(1000 * net, 2).tolist()))} per 1,000 weeks", flush=True)
p = rdm.estimate_rdm(df, K, base=base, jumps=False, velocity=VEL, variant="move" if MOVE else "hold", slides=slides)
print(f"  estimated: sigma0 {p.sigma0:.4f}, beta {p.beta:+.3f}, s_band {np.round(p.s_band, 4).tolist()}"
      + (f"; velocity var {p.vel_var:.2e} per week^2 (sd {np.sqrt(p.vel_var) * 52:.3f} a year), half-life {np.log(0.5) / np.log(p.vel_rho):.0f} weeks" if VEL and p.vel_var > 0 else ""), flush=True)
df0 = df[df["period"] < t0].copy()
levels = rk._filtered_levels(df0, base)
start_ids = levels.index.to_numpy()

Xd, Rd, rperm_d, pres_d, ids_d = rdm._matrices(df)


def block_means_matrix(X, b_lo, b_hi):
    """Block means of a (weeks x entities) matrix for blocks of BLOCK weeks, presence >= PRES, from row 0."""
    out = []
    for b in range(b_lo, b_hi):
        seg = X[b * BLOCK:(b + 1) * BLOCK]
        ok = np.isfinite(seg).mean(0) >= PRES
        with np.errstate(all="ignore"):
            m = np.where(ok, np.nanmean(seg, 0), np.nan)
        out.append(m)
    return np.array(out)


def metrics(Bm, perm_rank):
    """Bm: block means (n_blocks x entities), block 0 = the conditioning block; perm_rank: rank of block 0."""
    res = {}
    top = np.argsort(np.where(np.isfinite(Bm[0]), -Bm[0], np.inf))
    inK = np.zeros(Bm.shape[1], bool)
    inK[top[:K]] = True
    # S1: no return -- slope of (B2 - B1) on (B1 - B0) among block-0 top-K members present in all three
    if Bm.shape[0] >= 3:
        ok = inK & np.isfinite(Bm[0]) & np.isfinite(Bm[1]) & np.isfinite(Bm[2])
        x, y = Bm[1][ok] - Bm[0][ok], Bm[2][ok] - Bm[1][ok]
        res["no_return_slope"] = float(np.polyfit(x, y, 1)[0])
    # S2 / S5: retention of the top k from block 1 to block 2 (one step) and block 1 to block 3 (two steps)
    for k in (K // 64, K // 16, K // 8, K):
        for steps in (1, 2):
            if Bm.shape[0] > 1 + steps:
                a = np.argsort(np.where(np.isfinite(Bm[1]), -Bm[1], np.inf))[:k]
                b = np.argsort(np.where(np.isfinite(Bm[1 + steps]), -Bm[1 + steps], np.inf))[:k]
                res[f"ret_{k}_{steps}"] = float(len(np.intersect1d(a, b)) / k)
    # S3: raw size-variance beta of the one-block change B1 -> B2 over octave strata of block-0 rank
    if Bm.shape[0] >= 3:
        edges = [0] + [K // 64 * 2 ** i for i in range(7)]
        xs, ys = [], []
        for lo, hi in zip(edges[:-1], edges[1:]):
            idx = top[lo:hi]
            idx = idx[np.isfinite(Bm[1][idx]) & np.isfinite(Bm[2][idx])]
            if idx.size > 20:
                xs.append(np.mean(Bm[1][idx]))
                ys.append(np.log(np.std(Bm[2][idx] - Bm[1][idx], ddof=1)))
        res["beta_raw"] = float(-np.polyfit(xs, ys, 1)[0])
    # S4: arithmetic share of the top K/64 within the top K, last block
    last = Bm[-1]
    srt = np.sort(np.expm1(last[np.isfinite(last)]))[::-1]
    res["share_K64_last"] = float(srt[:K // 64].sum() / srt[:K].sum())
    return res


# --- data
Bd = block_means_matrix(Xd[start:], 0, nb)
emp = metrics(Bd, None)
r0 = np.empty(Xd.shape[1])
r0[np.argsort(np.where(np.isfinite(Bd[0]), -Bd[0], np.inf))] = np.arange(1, Xd.shape[1] + 1)
band0 = np.where(np.isfinite(Bd[0]), np.clip(np.searchsorted(p.band_ranks, r0, side="right") - 1, 0, 7), -1)
emp["kurtosis"] = rdm._std_kurtosis(Xd[t0:], band0, 8, 26)[0]
print("  data:", {k: round(v, 3) for k, v in emp.items()}, flush=True)

# --- simulations: both models start from the filtered state at t0 of the same communities
col = {e: j for j, e in enumerate(ids_d)}
cols = np.array([col[e] for e in start_ids])


def sim_metrics(lasting, seed):
    Xs, ids = rdm.simulate_panel(p, levels.to_numpy(), T - t0, seed=seed, lasting=lasting)
    alive = ids == ids[0][None, :]                       # the original communities, until a slot is reborn
    Xs = np.where(alive, Xs, np.nan)
    full = np.full((T - start, Xd.shape[1]), np.nan)
    full[:t0 - start] = Xd[start:t0]                     # block 0 is the data (the conditioning block)
    full[t0 - start:, cols] = Xs
    Bs = block_means_matrix(full, 0, nb)
    m = metrics(Bs, None)
    m["kurtosis"] = rdm._std_kurtosis(full[t0 - start:], band0, 8, 26)[0]
    return m


out = dict(panel=panel, K=K, block=BLOCK, seeds=SEEDS, data=emp, params=dict(sigma0=p.sigma0, beta=p.beta, s_band=p.s_band.tolist(),
           vel_var=p.vel_var, vel_rho=p.vel_rho))
for lasting in (("movinghome",) if (VEL or MOVE) else ("movinghome", "home")):
    runs = [sim_metrics(lasting, s) for s in range(SEEDS)]
    keys = runs[0].keys()
    summ = {k: dict(mean=float(np.nanmean([r[k] for r in runs])), q=[float(np.nanquantile([r[k] for r in runs], 0.05)),
                                                                      float(np.nanquantile([r[k] for r in runs], 0.95))]) for k in keys}
    out[lasting] = summ
    print(f"  {lasting}:", {k: f"{v['mean']:.3f} ({v['q'][0]:.3f}-{v['q'][1]:.3f})" for k, v in summ.items()}, flush=True)
Path(__file__).with_name(f"structure__{panel}{'__vel' if VEL else ''}{'__move' if MOVE else ''}.json").write_text(json.dumps(out, indent=1))
