"""R18 of PLAN.md (owner, 2026-10-21: "a common or nearly-common model across these different platforms"): the paper's
model, the moving home with drift by rank (`movinghome_vp`), fitted on Facebook's Era A panel with the paper's estimator,
and the Facebook statistics computed by one code path (`fb_stats`) on the data and on worlds of that model, at Facebook's
settings (26-week blocks for turnover and ladders, 13-week blocks for the crossing ratio, 4-week windows and 26-week
changes for persistence and the asymmetry).

    python r18_facebook.py fit                          fit the model on facebook_a; pickle to $R18_DIR, summary JSON here
    python r18_facebook.py data                         fb_stats on the data's universe frame
    python r18_facebook.py worlds <calib|control|flat> <s0> <s1>
    python r18_facebook.py summ
    python r18_facebook.py boot                         R18-A1 (post hoc): bootstrap bands for persistence and D_FB

Run without $R12_CACHE (Facebook has no lean cache; its panel is small).  $R18_DIR is a scratch folder for the pickle.
"""
import gc
import json
import os
import pickle
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import balance_accounting as ba  # noqa: E402
import mts_boundary_law as mb  # noqa: E402
import mts_step1 as m1  # noqa: E402
import r12_hetgrowth as h  # noqa: E402
import r17_round8 as r17  # noqa: E402

PN = "facebook_a"
PRES = 0.9
BLOCK = 26
KS_TURN = (10, 54, 219, 875, 1750)
KS_LAD = (1, 54, 219, 875)
SEL, W, H, GAPS, STEP = 13, 4, 26, (0, 4, 8), 4          # gapped_momentum.py, Facebook's settings
G_D = 4                                                   # D_FB: the second change starts 4 weeks after the first ends
L0_WEEKS = 13
SEEDS = range(20)


def _dir():
    d = Path(os.environ.get("R18_DIR", "/tmp"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def wmean(X, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def _order(v):
    f = np.flatnonzero(np.isfinite(v))
    return f[np.argsort(-v[f], kind="stable")]


def fb_stats(fr, K, T):
    X, R, floors, _, ids = m1.matrices(fr, K)
    nb = T // BLOCK
    start = T - nb * BLOCK
    M = ba.block_means(X, BLOCK, start=start, min_frac=PRES)
    tops = [_order(M[b]) for b in range(nb)]
    out = {}
    # 1. turnover between consecutive 26-week blocks
    out["turnover"] = {str(k): float(np.mean([1 - np.intersect1d(tops[b][:k], tops[b + 1][:k]).size / k for b in range(nb - 1)]))
                       for k in KS_TURN}
    # 2. weekly change of the occupant of ranks 1 and 10
    occ = {}
    for r in (1, 10):
        o = np.full(T, -1)
        for t in range(T):
            w = np.flatnonzero(R[t] == r)
            if w.size:
                o[t] = w[0]
        ok = (o[1:] >= 0) & (o[:-1] >= 0)
        occ[str(r)] = float(np.mean(o[1:][ok] != o[:-1][ok]))
    out["occupant_change"] = occ
    # 3. ladder levels above rank K, per block, and the first-to-last change
    lad = []
    for b in range(nb):
        L = M[b][tops[b]]
        lad.append({str(k): float(L[k - 1] - L[K - 1]) for k in KS_LAD})
    out["ladder"] = lad
    out["ladder_change"] = {str(k): lad[-1][str(k)] - lad[0][str(k)] for k in KS_LAD}
    # 4. crossing ratio, 13-week blocks, pooled over K/16 ... K
    try:
        bd = mb.analyse(fr, K, block=13, boot=20)
        rows = [r for r in bd.get("pooled", []) if r["k"] >= K // 16 - 1]
        out["boundary_by_k"] = {str(int(r["k"])): float(r["ratio"]) for r in bd.get("pooled", [])}
        out["boundary_pooled"] = float(np.exp(np.mean(np.log([r["ratio"] for r in rows if r["ratio"] > 0])))) if rows else None
        if "not_adjudicated" in bd:
            out["boundary_note"] = bd["not_adjudicated"]
    except Exception as ex:                                  # noqa: BLE001
        out["boundary_error"] = repr(ex)
    # 5. persistence at Facebook's settings (gapped_momentum.py)
    m = wmean(X, 0, SEL)
    top = _order(m)[:K]
    coh = np.zeros(X.shape[1], bool)
    coh[top] = True
    gp = {}
    for g in GAPS:
        vals, a = [], SEL
        while a + 2 * H + 2 * W + g <= T:
            x = wmean(X, a + H, a + H + W) - wmean(X, a, a + W)
            b0 = a + H + W + g
            y = wmean(X, b0 + H, b0 + H + W) - wmean(X, b0, b0 + W)
            ok = coh & np.isfinite(x) & np.isfinite(y)
            vals.append(float(spearmanr(x[ok], y[ok]).correlation))
            a += STEP
        gp[str(g)] = float(np.mean(vals))
    out["gapped"] = gp
    # 6. D_FB: R17-A's tail ratios in Facebook's construction
    lab = h.strata(R, floors, K)
    Ds, a = [], SEL
    while a + 2 * H + 2 * W + G_D <= T:
        x = wmean(X, a + H, a + H + W) - wmean(X, a, a + W)
        b0 = a + H + W + G_D
        y = wmean(X, b0 + H, b0 + H + W) - wmean(X, b0, b0 + W)
        ok = (lab >= 0) & np.isfinite(x) & np.isfinite(y)
        xd, yd = x.copy(), y.copy()
        for j in range(3):
            s = ok & (lab == j)
            xd[s] -= x[s].mean()
            yd[s] -= y[s].mean()
        f, r = r17.tail_ratios(xd, yd, ok)
        Ds.append(dict(origin=a, D=f - r, p_fall=f, p_rise=r))
        a += STEP
    out["D_FB"] = dict(D=float(np.mean([d["D"] for d in Ds])), p_fall=float(np.mean([d["p_fall"] for d in Ds])),
                       p_rise=float(np.mean([d["p_rise"] for d in Ds])), by_origin=Ds)
    # 7. head share in the last block
    k64 = K // 64
    lo, hi = start + (nb - 1) * BLOCK, start + nb * BLOCK
    A = np.nan_to_num(np.expm1(X[lo:hi]), nan=0.0)
    out["head_share"] = float(A[:, tops[-1][:k64]].sum() / A[:, tops[-1][:K]].sum())
    return out


def _fit_path():
    return _dir() / "r18_fit.pkl"


def mode_fit():
    import rankdiff_kalman as rk
    assert not os.environ.get("R12_CACHE"), "run without R12_CACHE"
    df, K = h.load(PN)
    T = int(df["period"].max()) + 1
    p, base, ladder = h.fit_model(df, K, PN)
    L0 = rk._filtered_levels(df[df["period"] < L0_WEEKS].copy(), base).to_numpy()
    with open(_fit_path(), "wb") as f:
        pickle.dump(dict(p=p, ladder=ladder, L0=L0, K=K, T=T), f)
    summ = dict(panel=PN, K=K, T=T, sigma0=float(p.sigma0), beta=float(p.beta), n_L0=int(np.isfinite(L0).sum()), n_ladder=len(ladder))
    (HERE / "r18_fit_summary.json").write_text(json.dumps(summ, indent=1))
    print(json.dumps(summ), flush=True)


def mode_data():
    assert not os.environ.get("R12_CACHE"), "run without R12_CACHE"
    df, K = h.load(PN)
    T = int(df["period"].max()) + 1
    out = dict(panel=PN, K=K, T=T, **fb_stats(df, K, T))
    (HERE / "r18_data__facebook_a.json").write_text(json.dumps(out, indent=1, default=float))
    print(json.dumps({k: out[k] for k in ("turnover", "occupant_change", "ladder_change", "boundary_pooled", "gapped", "head_share")}, default=float), flush=True)
    print("D_FB", round(out["D_FB"]["D"], 3), flush=True)


def _world_q(variant, p, ladder, L0, K):
    if variant == "calib":
        return h.het_params(p, ladder, "control"), L0
    q = h.het_params(p, ladder, "control_num")
    if variant == "flat":                                   # R9: constant-slope top K, same levels at ranks 1 and K
        o = np.argsort(np.where(np.isfinite(L0), -L0, np.inf), kind="stable")
        top, LK = L0[o[0]], L0[o[K - 1]]
        lr = np.log(np.arange(1, K + 1))
        L0 = L0.copy()
        L0[o[:K]] = top - (top - LK) * lr / lr[-1]
    return q, L0


def mode_worlds(variant, s0, s1):
    with open(_fit_path(), "rb") as f:
        fit = pickle.load(f)
    K, T = fit["K"], fit["T"]
    q, L0 = _world_q(variant, fit["p"], fit["ladder"], fit["L0"], K)
    for seed in range(s0, s1 + 1):
        fr, _, _ = h.sim_frame(q, L0, T, seed, K)
        out = dict(panel=PN, world=variant, seed=seed, **fb_stats(fr, K, T))
        (HERE / f"r18_world__facebook_a__{variant}__s{seed}.json").write_text(json.dumps(out, indent=1, default=float))
        print(f"{variant} seed {seed}: turnover {[round(v, 3) for v in out['turnover'].values()]}; rank-1 change "
              f"{out['occupant_change']['1']:.2f}; boundary {out.get('boundary_pooled')}; gapped "
              f"{[round(v, 3) for v in out['gapped'].values()]}; D_FB {out['D_FB']['D']:+.3f}; head share {out['head_share']:.3f}",
              flush=True)
        del fr
        gc.collect()


def _band(v):
    v = np.array([x for x in v if x is not None and np.isfinite(x)], float)
    return dict(mean=float(v.mean()), q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)), n=int(v.size)) if v.size else None


def mode_summ():
    d = json.loads((HERE / "r18_data__facebook_a.json").read_text())
    out = dict(data=d, worlds={})
    for variant in ("calib", "control", "flat"):
        W = [json.loads(f.read_text()) for f in sorted(HERE.glob(f"r18_world__facebook_a__{variant}__s*.json"))]
        if not W:
            continue
        s = dict(n=len(W))
        s["turnover"] = {k: _band([w["turnover"][k] for w in W]) for k in W[0]["turnover"]}
        s["occupant_change"] = {k: _band([w["occupant_change"][k] for w in W]) for k in W[0]["occupant_change"]}
        s["ladder_change"] = {k: _band([w["ladder_change"][k] for w in W]) for k in W[0]["ladder_change"]}
        s["ladder_first"] = {k: _band([w["ladder"][0][k] for w in W]) for k in W[0]["ladder"][0]}
        s["boundary_pooled"] = _band([w.get("boundary_pooled") for w in W])
        s["gapped"] = {k: _band([w["gapped"][k] for w in W]) for k in W[0]["gapped"]}
        s["D_FB"] = _band([w["D_FB"]["D"] for w in W])
        s["p_fall"] = _band([w["D_FB"]["p_fall"] for w in W])
        s["p_rise"] = _band([w["D_FB"]["p_rise"] for w in W])
        s["head_share"] = _band([w["head_share"] for w in W])
        out["worlds"][variant] = s
    (HERE / "r18_summary.json").write_text(json.dumps(out, indent=1, default=float))
    print(json.dumps(out["worlds"], default=float)[:3000], flush=True)


def mode_boot(n=200):
    """R18-A1 (post hoc): bootstrap bands for the data's persistence (pages drawn from the cohort) and D_FB (pages drawn
    within strata of whole-window permanent rank)."""
    assert not os.environ.get("R12_CACHE"), "run without R12_CACHE"
    df, K = h.load(PN)
    T = int(df["period"].max()) + 1
    X, R, floors, _, _ = m1.matrices(df, K)
    del df
    gc.collect()
    rng = np.random.default_rng(1818)
    m = wmean(X, 0, SEL)
    coh = _order(m)[:K]
    pairs_g = {}
    for g in GAPS:
        lst, a = [], SEL
        while a + 2 * H + 2 * W + g <= T:
            x = wmean(X, a + H, a + H + W) - wmean(X, a, a + W)
            b0 = a + H + W + g
            y = wmean(X, b0 + H, b0 + H + W) - wmean(X, b0, b0 + W)
            lst.append((x, y))
            a += STEP
        pairs_g[g] = lst
    lab = h.strata(R, floors, K)
    pairs_d, a = [], SEL
    while a + 2 * H + 2 * W + G_D <= T:
        x = wmean(X, a + H, a + H + W) - wmean(X, a, a + W)
        b0 = a + H + W + G_D
        y = wmean(X, b0 + H, b0 + H + W) - wmean(X, b0, b0 + W)
        pairs_d.append((x, y))
        a += STEP
    members = [np.flatnonzero(lab == j) for j in range(3)]

    def gapped_of(idx):
        out = {}
        for g, lst in pairs_g.items():
            vals = []
            for x, y in lst:
                xs, ys = x[idx], y[idx]
                ok = np.isfinite(xs) & np.isfinite(ys)
                vals.append(float(spearmanr(xs[ok], ys[ok]).correlation))
            out[str(g)] = float(np.mean(vals))
        return out

    def d_of(groups):
        Ds = []
        for x, y in pairs_d:
            xs, ys = [], []
            for gi in groups:
                xg, yg = x[gi], y[gi]
                ok = np.isfinite(xg) & np.isfinite(yg)
                xs.append(xg[ok] - xg[ok].mean())
                ys.append(yg[ok] - yg[ok].mean())
            xd, yd = np.concatenate(xs), np.concatenate(ys)
            f, r = r17.tail_ratios(xd, yd, np.ones(xd.size, bool))
            Ds.append(f - r)
        return float(np.mean(Ds))

    point = dict(gapped=gapped_of(coh), D_FB=d_of(members))
    draws_g, draws_d = [], []
    for _ in range(n):
        draws_g.append(gapped_of(rng.choice(coh, coh.size, replace=True)))
        draws_d.append(d_of([rng.choice(mm, mm.size, replace=True) for mm in members]))
    summ = json.loads((HERE / "r18_summary.json").read_text())["worlds"]["control"]
    q = lambda v: [float(np.quantile(v, 0.05)), float(np.quantile(v, 0.5)), float(np.quantile(v, 0.95))]
    out = dict(panel=PN, n=n, point=point,
               gapped_band={g: q([d[g] for d in draws_g]) for g in point["gapped"]},
               D_FB_band=q(draws_d),
               excess_over_control=dict(gapped={g: q([d[g] - summ["gapped"][g]["mean"] for d in draws_g]) for g in point["gapped"]},
                                        D_FB=q([v - summ["D_FB"]["mean"] for v in draws_d])))
    (HERE / "r18_boot__facebook_a.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1), flush=True)


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "fit":
        mode_fit()
    elif mode == "data":
        mode_data()
    elif mode == "worlds":
        mode_worlds(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
    elif mode == "summ":
        mode_summ()
    elif mode == "boot":
        mode_boot()
    else:
        sys.exit(__doc__)
