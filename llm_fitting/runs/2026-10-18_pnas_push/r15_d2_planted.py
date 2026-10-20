"""R15 D2 (PLAN.md addendum R15): how much of the planted fixed heterogeneity reaches the R12 statistics, with the drift
re-solve and exits switched off in turn.  Submissions, calibrated model (as R12), fixed form s 0.5 against s 0, m 1.0,
3 seeds a world.  Per world: the R12 auxiliary covariances by stratum (gaps 0, 13), the two-spacing lasting variance,
and the cross-sectional SD of per-community growth slopes (communities present >= 104 weeks), each reported as
planted minus s 0.  Exits off: the base's exit_rate set to zero; under a numeric drift the drift is re-solved with the
same setting.  Reads the lean cache ($R12_CACHE).

    R12_CACHE=<dir> python llm_fitting/runs/2026-10-18_pnas_push/r15_d2_planted.py
"""
import gc
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import r12_hetgrowth as h  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
import rankdiff_rdm as rdm  # noqa: E402

PN, S_PLANT, SEEDS = "reddit_submissions_long", 0.5, (0, 1, 2)


def world(p, ladder, s, numeric, exits):
    q = p
    if not exits:
        q = replace(q, base=replace(q.base, exit_rate=np.zeros_like(np.asarray(q.base.exit_rate, float))))
    if s > 0:
        q = replace(q, vel_var=(s / 52.0) ** 2, vel_rho=1.0, vel_clip=h.CLIP)
    if numeric:
        dz, dg, _ = rdm.drift_hold_numeric(q, ladder)
        q = replace(q, drift_z=dz, drift_g=dg)
    return q


def slopes_sd(fr, T, min_weeks=104):
    piv = fr.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    X = piv.to_numpy(float)
    ok = np.isfinite(X)
    keep = ok.sum(0) >= min_weeks
    X, ok = X[:, keep], ok[:, keep]
    t = np.arange(T, dtype=float)[:, None] * np.ones((1, X.shape[1]))
    n = ok.sum(0)
    tm = np.where(ok, t, 0).sum(0) / n
    xm = np.nansum(X, 0) / n
    cov = np.where(ok, (t - tm) * (X - xm), 0).sum(0)
    var = np.where(ok, (t - tm) ** 2, 0).sum(0)
    b = 52.0 * cov / var
    return float(np.std(b)), int(keep.sum())


def sim_frame_lasting(q, L0, T, seed, K, lasting):
    Xs, ids = rdm.simulate_panel(q, L0, T, seed=seed, lasting=lasting)
    return h._nw(K, T).frame_from_matrix(Xs, ids), Xs, ids


def main():
    nodrift_only = len(sys.argv) > 1 and sys.argv[1] == "nodrift"
    df, K = h.load(PN)
    T = int(df["period"].max()) + 1
    p, base, ladder = h.fit_model(df, K, PN)
    L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
    del df
    gc.collect()
    out = dict(panel=PN, planted_s=S_PLANT, planted_cov=S_PLANT ** 2, planted_sf_algebra=3 * (S_PLANT / 52) ** 2 * 26, settings={})
    combos = [(None, True)] if nodrift_only else [(n, e) for n in (True, False) for e in (True, False)]
    for numeric, exits in combos:
            lasting = "movinghome_vp_nodrift" if numeric is None else "movinghome_vp"
            tag = "no drift by rank, exits on (post hoc)" if numeric is None else f"drift {'numeric' if numeric else 'closed-form'}, exits {'on' if exits else 'off'}"
            res = {}
            for s in (0.0, S_PLANT):
                q = world(p, ladder, s, bool(numeric), exits)
                vals = []
                for seed in SEEDS:
                    fr, _, _ = sim_frame_lasting(q, L0, T, seed, K, lasting)
                    X, R, floors, _, _ = h.m1.matrices(fr, K)
                    a = h.aux(X, R, floors, K, (0, 13))
                    sd, n = slopes_sd(fr, T)
                    vals.append(dict(cov0=a["gapped"][0]["cov"], cov13=a["gapped"][13]["cov"], sf=a["sf"], rho13=a["gapped"][13]["rho"],
                                     slope_sd=sd, n_long=n))
                    del fr, X, R
                    gc.collect()
                res[str(s)] = {k: (np.mean([v[k] for v in vals], axis=0).tolist() if isinstance(vals[0][k], list) else float(np.mean([v[k] for v in vals])))
                               for k in vals[0]}
            d = {k: (np.subtract(res[str(S_PLANT)][k], res["0.0"][k]).tolist() if isinstance(res["0.0"][k], list) else res[str(S_PLANT)][k] - res["0.0"][k])
                 for k in ("cov0", "cov13", "sf", "rho13")}
            d["slope_var_increment"] = res[str(S_PLANT)]["slope_sd"] ** 2 - res["0.0"]["slope_sd"] ** 2
            out["settings"][tag] = dict(s0=res["0.0"], planted=res[str(S_PLANT)], increment=d)
            print(f"{tag}: cov0 increment by stratum {np.round(d['cov0'], 3).tolist()} (planted 0.25); cov13 {np.round(d['cov13'], 3).tolist()}; "
                  f"two-spacing sf increment {np.round(d['sf'], 4).tolist()} (algebra {out['planted_sf_algebra']:.4f}); rho13 {d['rho13']:+.3f}; "
                  f"slope variance increment {d['slope_var_increment']:.3f} (planted 0.25); s0 rho13 {res['0.0']['rho13']:+.3f}", flush=True)
    (HERE / ("r15_d2_planted__nodrift.json" if nodrift_only else "r15_d2_planted.json")).write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
