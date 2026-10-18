"""DOSSIER addendum 3: drift by name (a persistent velocity of the moving home) calibrated on Reddit submissions'
momentum, then transported with no refit to Reddit comments.

    python llm_fitting/runs/2026-10-18_rdm/velocity_transport.py calibrate          (submissions; grid)
    python llm_fitting/runs/2026-10-18_rdm/velocity_transport.py transport q H      (comments; 10 seeds)
"""
import json
import math
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
import rankdiff_rdm as rdm  # noqa: E402

PRES = 0.9


def setup(panel):
    K = m1.PANELS[panel]["K"]
    BLOCK = 52
    est = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True,
               mix_hetero=panel != "reddit_submissions_long")
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]]), K, buffer_mult=4)
    T = int(df["period"].max()) + 1
    nb = T // BLOCK
    start = T - nb * BLOCK
    t0 = start + BLOCK
    base = mrd.estimate(df, **est)
    p = rdm.estimate_rdm(df, K, base=base, jumps=False, variant="move", slides=(np.array([1.0, 2.0]), np.zeros(2)))
    levels = rk._filtered_levels(df[df["period"] < t0].copy(), base)
    Xd, _, _, _, ids_d = rdm._matrices(df)
    col = {e: j for j, e in enumerate(ids_d)}
    cols = np.array([col[e] for e in levels.index])
    return dict(panel=panel, K=K, BLOCK=BLOCK, T=T, nb=nb, start=start, t0=t0, p=p, levels=levels.to_numpy(), Xd=Xd, cols=cols)


def block_means(X, BLOCK, nb):
    out = []
    for b in range(nb):
        seg = X[b * BLOCK:(b + 1) * BLOCK]
        ok = np.isfinite(seg).mean(0) >= PRES
        with np.errstate(all="ignore"):
            out.append(np.where(ok, np.nanmean(seg, 0), np.nan))
    return np.array(out)


def metrics(Bm, K):
    top = np.argsort(np.where(np.isfinite(Bm[0]), -Bm[0], np.inf))
    inK = np.zeros(Bm.shape[1], bool)
    inK[top[:K]] = True
    ok = inK & np.isfinite(Bm[0]) & np.isfinite(Bm[1]) & np.isfinite(Bm[2])
    x, y = Bm[1][ok] - Bm[0][ok], Bm[2][ok] - Bm[1][ok]
    res = dict(momentum_spearman=float(spearmanr(x, y).correlation))
    for k in (K // 64, K // 16, K // 8, K):
        for steps in (1, 2):
            if Bm.shape[0] > 1 + steps:
                a = np.argsort(np.where(np.isfinite(Bm[1]), -Bm[1], np.inf))[:k]
                b = np.argsort(np.where(np.isfinite(Bm[1 + steps]), -Bm[1 + steps], np.inf))[:k]
                res[f"ret_{k}_{steps}"] = float(len(np.intersect1d(a, b)) / k)
    edges = [0] + [K // 64 * 2 ** i for i in range(7)]
    xs, ys = [], []
    for lo, hi in zip(edges[:-1], edges[1:]):
        idx = top[lo:hi]
        idx = idx[np.isfinite(Bm[1][idx]) & np.isfinite(Bm[2][idx])]
        if idx.size > 20:
            xs.append(np.mean(Bm[1][idx]))
            ys.append(np.log(np.std(Bm[2][idx] - Bm[1][idx], ddof=1)))
    res["beta_raw"] = float(-np.polyfit(xs, ys, 1)[0])
    srt = np.sort(np.expm1(Bm[-1][np.isfinite(Bm[-1])]))[::-1]
    res["share_K64_last"] = float(srt[:K // 64].sum() / srt[:K].sum())
    return res


def with_velocity(S, q, H):
    p = S["p"]
    sv = q * p.sigma0 * math.sqrt(52.0) / 52.0
    pv = replace(p, vel_var=sv * sv, vel_rho=0.5 ** (1.0 / H), variant="hold")
    lad = np.sort(S["levels"])[::-1]
    dz, dg = rdm.drift_hold(dict(sigma0=pv.sigma0, beta=pv.beta, L_ref=pv.L_ref, lam=0.0, pi=0.0,
                                 band_lo=float(np.nanmin(pv.band_level)), band_hi=float(np.nanmax(pv.band_level))), lad, S["levels"].size)
    pv = replace(pv, drift_z=dz, drift_g=dg)
    dz, dg, hist = rdm.drift_hold_numeric(pv, lad)
    return replace(pv, drift_z=dz, drift_g=dg), hist


def simulate(S, pv, seeds):
    runs = []
    for sd in seeds:
        Xs, ids = rdm.simulate_panel(pv, S["levels"], S["T"] - S["t0"], seed=sd, lasting="movinghome")
        Xs = np.where(ids == ids[0][None, :], Xs, np.nan)
        full = np.full((S["T"] - S["start"], S["Xd"].shape[1]), np.nan)
        full[:S["t0"] - S["start"]] = S["Xd"][S["start"]:S["t0"]]
        full[S["t0"] - S["start"]:, S["cols"]] = Xs
        runs.append(metrics(block_means(full, S["BLOCK"], S["nb"]), S["K"]))
    return {k: dict(mean=float(np.nanmean([r[k] for r in runs])), q=[float(np.nanquantile([r[k] for r in runs], 0.05)),
                                                                     float(np.nanquantile([r[k] for r in runs], 0.95))]) for k in runs[0]}


HERE = Path(__file__).resolve().parent
if sys.argv[1] == "calibrate":
    S = setup("reddit_submissions_long")
    data = metrics(block_means(S["Xd"][S["start"]:], S["BLOCK"], S["nb"]), S["K"])
    print("submissions data:", {k: round(v, 3) for k, v in data.items()}, flush=True)
    grid = []
    for H in (52.0, 104.0, 208.0):
        for q in (0.25, 0.5, 0.75, 1.0, 1.5):
            pv, hist = with_velocity(S, q, H)
            m = simulate(S, pv, range(5))
            grid.append(dict(q=q, H=H, hold_rms=hist[-1], **{k: v["mean"] for k, v in m.items()}))
            print(f"  q {q:.2f} H {H:.0f}: momentum {m['momentum_spearman']['mean']:+.3f}; one-year retention "
                  + "/".join(f"{m[f'ret_{k}_1']['mean']:.3f}" for k in (78, 312, 625, 5000))
                  + f"; share {m['share_K64_last']['mean']:.3f}; beta {m['beta_raw']['mean']:.3f}; hold rms {hist[-1]:.4f}", flush=True)
    best = min(grid, key=lambda g: abs(g["momentum_spearman"] - data["momentum_spearman"]))
    print(f"chosen: q {best['q']}, H {best['H']} (momentum {best['momentum_spearman']:+.3f} against {data['momentum_spearman']:+.3f})", flush=True)
    (HERE / "velocity_calibration.json").write_text(json.dumps(dict(data=data, grid=grid, chosen=best), indent=1))
else:
    q, H = float(sys.argv[2]), float(sys.argv[3])
    S = setup("reddit_comments_ext")
    data = metrics(block_means(S["Xd"][S["start"]:], S["BLOCK"], S["nb"]), S["K"])
    pv, hist = with_velocity(S, q, H)
    m = simulate(S, pv, range(10))
    print("comments data:", {k: round(v, 3) for k, v in data.items()}, flush=True)
    print(f"comments, transported q {q} H {H:.0f} (hold rms {hist[-1]:.4f}):", {k: f"{v['mean']:.3f} ({v['q'][0]:.3f}-{v['q'][1]:.3f})" for k, v in m.items()}, flush=True)
    (HERE / "velocity_transport_comments.json").write_text(json.dumps(dict(q=q, H=H, data=data, sim=m), indent=1))
