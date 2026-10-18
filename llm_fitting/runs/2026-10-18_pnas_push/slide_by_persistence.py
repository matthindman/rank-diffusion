"""R10 of PLAN.md (post hoc): is the slide's excess over a driftless world carried by persistent decliners?

For start weeks s (every 26 weeks): prior change = mean[s-26, s-13) - mean[s-78, s-65) (13-week windows a year apart,
ending 13 weeks before s); slide of member i over the next year = -[(x_i(B) - x_i(A)) - (U_K(B) - U_K(A))] / 52 per
week, A = [s, s+26), B = [s+52, s+78), U_K the mean of the top K by block level.  Cohort = top k by block-A level
(k = K/64, K/16, K/4), persisting members only.  Classes by prior change: Q1 bottom fifth, Q5 top fifth, rest.

    python llm_fitting/runs/2026-10-18_pnas_push/slide_by_persistence.py data
    python llm_fitting/runs/2026-10-18_pnas_push/slide_by_persistence.py world <reddit_submissions_long|reddit_comments_ext> <driftless|model> <n_seeds>
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

HERE = Path(__file__).resolve().parent
PRES = 0.9


def wmean(X, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def topmean(v, K):
    f = v[np.isfinite(v)]
    return float(np.sort(f)[::-1][:K].mean())


def cells(X, K):
    T = X.shape[0]
    out = []
    for s in range(78, T - 78 + 1, 26):
        A, B = wmean(X, s, s + 26), wmean(X, s + 52, s + 78)
        prior = wmean(X, s - 26, s - 13) - wmean(X, s - 78, s - 65)
        dU = topmean(B, K) - topmean(A, K)
        fa = np.flatnonzero(np.isfinite(A))
        oa = fa[np.argsort(-A[fa], kind="stable")]
        for k in (K // 64, K // 16, K // 4):
            coh = oa[:k]
            coh = coh[np.isfinite(B[coh])]
            sl = -((B[coh] - A[coh]) - dU) / 52.0
            pr = prior[coh]
            v = np.isfinite(pr)
            if v.sum() < 25:
                continue
            q20, q80 = np.quantile(pr[v], [0.2, 0.8])
            Q1, Q5 = v & (pr <= q20), v & (pr >= q80)
            rest = v & ~Q1
            out.append(dict(s=int(s), k=int(k), n=int(coh.size), n_prior=int(v.sum()), slide=float(sl.mean()),
                            slide_valid=float(sl[v].mean()), Q1=float(sl[Q1].mean()), Q5=float(sl[Q5].mean()),
                            rest=float(sl[rest].mean()), w_Q1=float(Q1.sum() / v.sum())))
    return out


def summary(rows):
    c = np.array([r["Q1"] - r["rest"] for r in rows])
    return dict(n_cells=len(rows), mean_contrast=float(c.mean()), frac_pos=float((c > 0).mean()),
                mean_slide=float(np.mean([r["slide"] for r in rows])),
                mean_Q1=float(np.mean([r["Q1"] for r in rows])), mean_Q5=float(np.mean([r["Q5"] for r in rows])),
                mean_rest=float(np.mean([r["rest"] for r in rows])))


mode = sys.argv[1]
if mode == "data":
    out = {}
    for pn in ("reddit_submissions_long", "reddit_comments_ext"):
        K = m1.PANELS[pn]["K"]
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[pn]["platform"]]), K, buffer_mult=4)
        T = int(df["period"].max()) + 1
        X = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T)).to_numpy(float)
        del df
        rows = cells(X, K)
        out[pn] = dict(K=K, rows=rows, summary=summary(rows),
                       by_k={int(k): summary([r for r in rows if r["k"] == k]) for k in sorted({r["k"] for r in rows})})
        print(pn, json.dumps(out[pn]["summary"]), flush=True)
        for k, v in out[pn]["by_k"].items():
            print(f"   k {k}: {json.dumps(v)}", flush=True)
    (HERE / "slide_by_persistence__data.json").write_text(json.dumps(out, indent=1))
else:
    import rankdiff_kalman as rk
    import rankdiff_rdm as rdm
    pn, kind, ns = sys.argv[2], sys.argv[3], int(sys.argv[4])
    K = m1.PANELS[pn]["K"]
    EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True, mix_hetero=pn != "reddit_submissions_long")
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[pn]["platform"]]), K, buffer_mult=4)
    T = int(df["period"].max()) + 1
    base = mrd.estimate(df, **EST)
    p = rdm.estimate_rdm(df, K, base=base, jumps=False, variant="hold")
    L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
    del df
    if kind == "driftless":
        p = replace(p, drift_g=np.zeros_like(p.drift_g))
    print(f"{pn} {kind}: world beta {p.beta:+.3f}, N {L0.size}, T {T}", flush=True)
    allrows = []
    for seed in range(ns):
        Xs, ids = rdm.simulate_panel(p, L0, T, seed=seed, lasting="movinghome_vp")
        # observed as the data are: a name's level only while it is inside the weekly top 4K + 50
        thr = np.sort(Xs, axis=1)[:, [-min(4 * K + 50, Xs.shape[1])]]
        Xo = np.where(Xs >= thr, Xs, np.nan)
        # follow names, not slots: reborn ids start new columns
        uid, inv = np.unique(ids, return_inverse=True)
        M = np.full((T, uid.size), np.nan)
        M[np.repeat(np.arange(T), ids.shape[1]), inv.ravel()] = Xo.ravel()
        rows = cells(M, K)
        for r in rows:
            r["seed"] = seed
        allrows += rows
        print(f"  seed {seed}: {json.dumps(summary(rows))}", flush=True)
    out = dict(panel=pn, kind=kind, beta=float(p.beta), rows=allrows, summary=summary(allrows),
               by_k={int(k): summary([r for r in allrows if r["k"] == k]) for k in sorted({r["k"] for r in allrows})})
    print(pn, kind, json.dumps(out["summary"]), flush=True)
    (HERE / f"slide_by_persistence__{pn}__{kind}.json").write_text(json.dumps(out, indent=1))
