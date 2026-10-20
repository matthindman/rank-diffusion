"""R12 of PLAN.md (with amendments A1 and A2): calibrated heterogeneous-growth worlds.

Three standard forms of a community-specific growth term a_it added to the lasting level of the calibrated model
(movinghome_vp; estimator settings as R7 and A6):
  fixed   a_it = b_i ~ N(0, sigma_b^2), drawn once per identity, truncated at +/- 2.5 sd (vel_rho = 1, vel_clip = 2.5)
  decay   a_it = u_it, AR(1) with weekly persistence rho and stationary sd sigma_u (vel_rho < 1)
  factor  a_it = (lambda_i - lambar_K) f_t, f_t the equal-weighted mean weekly change of the tracked universe
          (leave-one-out for estimation), loadings from 2019 co-movement, spread corrected for estimation noise
  control the same model without the term, closed-form hold drift (as A6 and R7)
  control_num the same model without the term, hold drift re-solved numerically like every variant (A4: the matched control)
Annual spreads s are 52 * sigma.  Calibration of fixed and decay by indirect inference on (i) the Pearson covariance
of year-apart changes of 13-week window means at the declared gaps, within three strata of permanent rank, and
(ii) the structure-function lasting variance by band, with a multiplier m on sigma_0 (the G15 correction).  The hold
drift is re-solved with the heterogeneity present.  Every statistic is computed by the same code on data and worlds.

    python r12_hetgrowth.py moments <panel>                       data auxiliary statistics + bootstrap sd; data tests
    python r12_hetgrowth.py grid <panel> <fixed|decay> [m]        3 seeds per grid point (decay at the given m)
    python r12_hetgrowth.py gridx <panel> fixed                   post hoc: spreads 0.6-1.0 beyond the registered grid
    python r12_hetgrowth.py worlds <panel> <fixed|decay|factor|control|control_num> <s0> <s1> [s] [half_life] [m]
    python r12_hetgrowth.py summarize <panel> <variant>
    python r12_hetgrowth.py oop <panel> <fixed|decay|factor|control|control_num> [s] [half_life] [m]
    python r12_hetgrowth.py recovery <panel> <planted_s> <s0> <s1>

Outputs: r12_moments__<panel>.json, r12_grid__<panel>__<variant>.json, r12_world__<panel>__<variant>__s<seed>.json,
r12_worlds__<panel>__<variant>.json, r12_oop__<panel>__<variant>.json, r12_recovery__<panel>__s<planted>.json.
"""
import json
import math
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(HERE))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402

PRES, W, SPAN = 0.9, 13, 52
GAPS_FIXED, GAPS_DECAY, GAPS_TEST = (0, 13), (0, 13, 26, 39), (0, 13, 26, 39, 52)
EST_BASE = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True)
CLIP = 2.5


# --------------------------------------------------------------------------- statistics (data and worlds alike)
def wmean(X, lo, hi):
    seg = X[lo:hi]
    ok = np.isfinite(seg).mean(0) >= PRES
    with np.errstate(all="ignore"):
        return np.where(ok, np.nanmean(seg, 0), np.nan)


def strata(R, floors, K):
    """Permanent rank over the whole window (absences at the floor); strata [0, K/16), [K/16, K/4), [K/4, K)."""
    T = R.shape[0]
    pr, _ = ba.perm_rank(R, 0, T, floors)
    o = np.argsort(pr, kind="stable")
    lab = np.full(R.shape[1], -1)
    for j, (lo, hi) in enumerate(((0, K // 16), (K // 16, K // 4), (K // 4, K))):
        lab[o[lo:hi]] = j
    return lab


def window_changes(X, a):
    return wmean(X, a + SPAN, a + SPAN + W) - wmean(X, a, a + W)


def gapped_cov(X, lab, gaps):
    """Pearson covariance and Spearman correlation of year-apart changes of 13-week window means sharing no weeks,
    by stratum and pooled, averaged over origins every 13 weeks (the R3/R7 design, with Pearson added)."""
    T = X.shape[0]
    out = {}
    for g in gaps:
        covs = {j: [] for j in range(3)}
        rhos = []
        a = 0
        while a + 2 * (SPAN + W) + g <= T:
            x = window_changes(X, a)
            y = window_changes(X, a + SPAN + W + g)
            for j in range(3):
                ok = (lab == j) & np.isfinite(x) & np.isfinite(y)
                if ok.sum() > 30:
                    xx, yy = x[ok] - x[ok].mean(), y[ok] - y[ok].mean()
                    covs[j].append(float(np.mean(xx * yy)))
            ok = (lab >= 0) & np.isfinite(x) & np.isfinite(y)
            if ok.sum() > 30:
                rhos.append(spearmanr(x[ok], y[ok]).correlation)
            a += 13
        out[int(g)] = dict(cov=[float(np.mean(covs[j])) if covs[j] else float("nan") for j in range(3)],
                           rho=float(np.mean(rhos)) if rhos else float("nan"), n_origins=len(rhos))
    return out


def asymmetry(X, lab, g=13):
    """Gapped Spearman correlation among first-period fallers and among risers (R12-7)."""
    T = X.shape[0]
    fall, rise = [], []
    a = 0
    while a + 2 * (SPAN + W) + g <= T:
        x = window_changes(X, a)
        y = window_changes(X, a + SPAN + W + g)
        ok = (lab >= 0) & np.isfinite(x) & np.isfinite(y)
        for sel, acc in ((ok & (x < 0), fall), (ok & (x > 0), rise)):
            if sel.sum() > 30:
                acc.append(spearmanr(x[sel], y[sel]).correlation)
        a += 13
    return dict(fallers=float(np.mean(fall)), risers=float(np.mean(rise)), diff=float(np.mean(fall) - np.mean(rise)))


def sf_lasting(X, lab, h1=26, h2=52):
    """Two-spacing lasting variance per week by stratum: [var(x_{t+h2}-x_t) - var(x_{t+h1}-x_t)] / (h2 - h1), pooled
    over t, with each stratum's cross-sectional mean change removed (the structure-function slope)."""
    T = X.shape[0]
    out = []
    for j in range(3):
        cols = lab == j
        v = {}
        for h in (h1, h2):
            d = X[h:, cols] - X[:-h, cols]
            d = d - np.nanmean(d, axis=1, keepdims=True)
            v[h] = float(np.nanmean(d ** 2))
        out.append((v[h2] - v[h1]) / (h2 - h1))
    return out


def weekly_autocov(X, lab, lags=range(1, 9)):
    d = X[1:] - X[:-1]
    d = d - np.nanmean(np.where(lab >= 0, d, np.nan), axis=1, keepdims=True)
    out = []
    for j in range(3):
        dj = d[:, lab == j]
        out.append([float(np.nanmean(dj[:-l] * dj[l:])) for l in lags])
    return out


def aux(X, R, floors, K, gaps):
    lab = strata(R, floors, K)
    return dict(gapped=gapped_cov(X, lab, sorted(set(gaps) | set(GAPS_TEST))), sf=sf_lasting(X, lab),
                wk=weekly_autocov(X, lab), asym=asymmetry(X, lab))


def aux_vector(a, gaps):
    gp = a["gapped"]                                 # int keys in memory, str keys once read back from JSON
    return np.array([c for g in gaps for c in gp.get(int(g), gp.get(str(g)))["cov"]] + list(a["sf"]))


# --------------------------------------------------------------------------- data and model
def load(pn, member_window=None):
    import minimal_rankdiff as mrd
    K = m1.PANELS[pn]["K"]
    full = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[pn]["platform"]])
    df = mrd.restrict_universe(full, K, buffer_mult=4, **({"member_window": member_window} if member_window else {}))
    return df, K


def fit_model(df, K, pn):
    import minimal_rankdiff as mrd
    import rankdiff_kalman as rk
    import rankdiff_rdm as rdm
    base = mrd.estimate(df, **EST_BASE, mix_hetero=pn != "reddit_submissions_long")
    p = rdm.estimate_rdm(df, K, base=base, jumps=False, variant="hold")
    lev = rk._filtered_levels(df, base)
    ladder = np.sort(lev.to_numpy()[np.isfinite(lev.to_numpy())])[::-1]
    return p, base, ladder


def het_params(p, ladder, variant, s=0.0, half_life=None, m=1.0, fac=None):
    """The calibrated model with the variant's growth term; the hold drift re-solved with the term present."""
    import rankdiff_rdm as rdm
    q = replace(p, sigma0=p.sigma0 * m)
    if variant == "fixed" and s > 0:
        q = replace(q, vel_var=(s / 52.0) ** 2, vel_rho=1.0, vel_clip=CLIP)
    elif variant == "decay" and s > 0:
        rho = 0.5 ** (1.0 / half_life)
        q = replace(q, vel_var=(s / 52.0) ** 2, vel_rho=rho)
    elif variant == "factor":
        q = replace(q, fac_path=fac["path"], fac_load=fac["load"], fac_pool=fac["pool"])
    if variant == "control_num" or (variant != "control" and (s > 0 or variant == "factor" or m != 1.0)):
        # A4: control_num is the matched homogeneous world (the drift re-solved numerically, no term); control keeps the
        # closed-form drift of A6 and R7 for continuity with their numbers
        qd = _factor_for_drift(q, ladder, fac) if variant == "factor" else q
        dz, dg, _ = rdm.drift_hold_numeric(qd, ladder)
        q = replace(q, drift_z=dz, drift_g=dg)
    return q


def _factor_for_drift(q, ladder, fac):
    """Amendment A3: the hold drift for (c) is solved with the factor term present, but with the path's weekly
    values centred and shuffled (so the drift carries the term's variance, not the observed path's trend, which
    a drift by rank would otherwise absorb, the loadings being nearly constant within strata), and with the
    loadings in ladder order (drift_hold_numeric sorts its starting levels), padded from the pool."""
    rng = np.random.default_rng(12)
    path = np.asarray(fac["path"], float)
    path = rng.permutation(path - path.mean())
    L0 = np.asarray(fac["L0"], float)
    o = np.argsort(np.where(np.isfinite(L0), -L0, np.inf), kind="stable")
    load = np.asarray(fac["load"], float)[o]
    n = len(ladder)
    if load.size < n:
        load = np.concatenate([load, np.asarray(fac["pool"], float)[rng.integers(0, len(fac["pool"]), n - load.size)]])
    return replace(q, fac_path=path, fac_load=load[:n])


def _nw(K, T):
    """null_worlds with its module constants set to this panel (its functions read K and T as globals; R7 used
    K = 5,000, T = 212 only)."""
    import null_worlds as nw
    nw.K, nw.T = int(K), int(T)
    return nw


def sim_frame(q, L0, T, seed, K):
    import rankdiff_rdm as rdm
    Xs, ids = rdm.simulate_panel(q, L0, T, seed=seed, lasting="movinghome_vp")
    return _nw(K, T).frame_from_matrix(Xs, ids), Xs, ids


def factor_inputs(df, K, weeks, start_ids, T_path):
    """Leave-one-out factor f_t (equal-weighted mean weekly change of the tracked universe), loadings estimated on
    `weeks`, spread corrected for estimation noise within strata and shrunk (empirical Bayes); deviations from the
    top-K mean.  Returns the path over the panel's weeks (T_path - 1 changes), loadings aligned to start_ids."""
    T = int(df["period"].max()) + 1
    piv = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    X = piv.to_numpy(float)
    ids = np.asarray(piv.columns).astype(str)
    d = X[1:] - X[:-1]
    n_t = np.isfinite(d).sum(1)
    tot = np.nansum(d, 1)
    f_all = tot / np.maximum(n_t, 1)
    w = np.array([t for t in weeks if t < d.shape[0]])
    lam = np.full(d.shape[1], np.nan)
    se2 = np.full(d.shape[1], np.nan)
    for i in range(d.shape[1]):
        di = d[w, i]
        ok = np.isfinite(di)
        if ok.sum() < 20:
            continue
        f_loo = (tot[w] - np.nan_to_num(d[w, i])) / np.maximum(n_t[w] - np.isfinite(d[w, i]), 1)
        fx, dy = f_loo[ok] - f_loo[ok].mean(), di[ok] - di[ok].mean()
        vf = float(np.sum(fx ** 2))
        if vf <= 0:
            continue
        b = float(np.sum(fx * dy) / vf)
        res = dy - b * fx
        lam[i] = b
        se2[i] = float(np.sum(res ** 2) / max(ok.sum() - 2, 1) / vf)
    # strata by mean rank in the estimation weeks
    rk_ = df[df["period"].isin(w)].groupby("entity_id")["rank"].mean()
    order = rk_.reindex(ids).to_numpy()
    lab = np.full(ids.size, -1)
    o = np.argsort(np.where(np.isfinite(order), order, np.inf), kind="stable")
    for j, (lo, hi) in enumerate(((0, K // 16), (K // 16, K // 4), (K // 4, K), (K, 4 * K))):
        lab[o[lo:hi]] = j
    shr = lam.copy()
    var_corr = {}
    for j in range(4):
        sel = (lab == j) & np.isfinite(lam)
        if sel.sum() < 10:
            continue
        mu, v = float(np.mean(lam[sel])), float(np.var(lam[sel]))
        s2 = max(v - float(np.mean(se2[sel])), 0.0)
        var_corr[j] = dict(mean=mu, var_raw=v, var_corrected=s2, n=int(sel.sum()))
        shr[sel] = mu + (s2 / (s2 + se2[sel])) * (lam[sel] - mu)
    top = (lab >= 0) & (lab <= 2) & np.isfinite(shr)
    lbar = float(np.mean(shr[top]))
    dev = dict(zip(ids, np.nan_to_num(shr - lbar)))
    load = np.array([dev.get(str(e), 0.0) for e in start_ids])
    pool = np.nan_to_num(shr[(lab == 3) & np.isfinite(shr)] - lbar)
    path = f_all                                   # the observed path (A3: not centred; lambar removes the mean move)
    return dict(path=path[:max(T_path - 1, 1)], load=load, pool=pool if pool.size else np.zeros(1), strata=var_corr, lbar=lbar,
                f_mean=float(np.mean(f_all)))


# --------------------------------------------------------------------------- R10, lock-in, Gomez on a world
def r10_rows(Xs, ids, K, T):
    thr = np.sort(Xs, axis=1)[:, [-min(4 * K + 50, Xs.shape[1])]]
    Xo = np.where(Xs >= thr, Xs, np.nan)
    uid, inv = np.unique(ids, return_inverse=True)
    M = np.full((T, uid.size), np.nan)
    M[np.repeat(np.arange(T), ids.shape[1]), inv.ravel()] = Xo.ravel()
    out = []
    for s in range(78, T - 78 + 1, 26):
        A, B = wmean(M, s, s + 26), wmean(M, s + 52, s + 78)
        prior = wmean(M, s - 26, s - 13) - wmean(M, s - 78, s - 65)
        fA, fB = np.sort(A[np.isfinite(A)])[::-1][:K].mean(), np.sort(B[np.isfinite(B)])[::-1][:K].mean()
        fa = np.flatnonzero(np.isfinite(A))
        oa = fa[np.argsort(-A[fa], kind="stable")]
        for k in (K // 64, K // 16, K // 4):
            coh = oa[:k]
            coh = coh[np.isfinite(B[coh])]
            sl = -((B[coh] - A[coh]) - (fB - fA)) / 52.0
            pr = prior[coh]
            v = np.isfinite(pr)
            if v.sum() < 25:
                continue
            q20 = np.quantile(pr[v], 0.2)
            Q1 = v & (pr <= q20)
            out.append(dict(s=int(s), k=int(k), Q1=float(sl[Q1].mean()), rest=float(sl[v & ~Q1].mean()), slide=float(sl.mean())))
    return out


def world_stats(df, Xs, ids, K, T, label):
    import lockin_funnel as lf
    res = _nw(K, T).stats(df, label)
    X, R, floors, _, _ = m1.matrices(df, K)
    a = aux(X, R, floors, K, GAPS_TEST)
    res["r12_aux"] = a
    res["lockin"] = lf.measures(df, K)
    res["r10"] = r10_rows(Xs, ids, K, T)
    return res


# --------------------------------------------------------------------------- modes
def main():
    mode, pn = sys.argv[1], sys.argv[2]
    if mode == "moments":
        df, K = load(pn)
        X, R, floors, _, _ = m1.matrices(df, K)
        a = aux(X, R, floors, K, GAPS_TEST)
        lab = strata(R, floors, K)
        rng = np.random.default_rng(0)
        boots = []
        for _ in range(40):                              # communities resampled within strata
            cols = np.concatenate([rng.choice(np.flatnonzero(lab == j), (lab == j).sum(), replace=True) for j in range(3)])
            Xb, Rb = X[:, cols], R[:, cols]
            labb = lab[cols]
            boots.append(np.array([c for g in GAPS_DECAY for c in gapped_cov(Xb, labb, [g])[g]["cov"]] + sf_lasting(Xb, labb)))
        sd = np.std(np.array(boots), axis=0, ddof=1)
        out = dict(panel=pn, K=K, aux=a, boot_sd=sd.tolist(), boot_order=f"gapped cov at gaps {list(GAPS_DECAY)} x 3 strata, then sf x 3")
        (HERE / f"r12_moments__{pn}.json").write_text(json.dumps(out, indent=1, default=float))
        print(json.dumps({k: a[k] for k in ("gapped", "asym")}, default=float)[:2000], flush=True)
        return
    if mode == "summarize":
        variant = sys.argv[3]
        files = sorted(HERE.glob(f"r12_world__{pn}__{variant}__s*.json"))
        W_ = [json.loads(f.read_text()) for f in files]
        def band(vals):
            v = np.array(vals, float)
            return dict(mean=float(np.nanmean(v)), q05=float(np.nanquantile(v, 0.05)), q95=float(np.nanquantile(v, 0.95)), n=int(np.isfinite(v).sum()))
        out = dict(panel=pn, variant=variant, n_seeds=len(W_),
                   gapped={str(g): band([w["r12_aux"]["gapped"][str(g)]["rho"] for w in W_]) for g in GAPS_TEST},
                   gapped_stats={str(g): band([w["gapped"][str(g)] for w in W_]) for g in (13, 26)},
                   asym=band([w["r12_aux"]["asym"]["diff"] for w in W_]),
                   turnover={k: band([w["turnover"][k] for w in W_]) for k in W_[0]["turnover"]},
                   boundary=band([w.get("boundary_pooled") for w in W_]),
                   r10_contrast=band([np.mean([r["Q1"] - r["rest"] for r in w["r10"]]) for w in W_]),
                   r10_Q1=band([np.mean([r["Q1"] for r in w["r10"]]) for w in W_]),
                   r10_rest=band([np.mean([r["rest"] for r in w["r10"]]) for w in W_]),
                   retention={k: {h: band([w["lockin"]["retention"][k][h]["mean"] for w in W_]) for h in ("1", "2")} for k in W_[0]["lockin"]["retention"]},
                   sf=[band([w["r12_aux"]["sf"][j] for w in W_]) for j in range(3)])
        (HERE / f"r12_worlds__{pn}__{variant}.json").write_text(json.dumps(out, indent=1, default=float))
        print(json.dumps(out, default=float)[:3000], flush=True)
        return

    df, K = load(pn)
    T = int(df["period"].max()) + 1
    import rankdiff_kalman as rk
    if mode in ("grid", "gridx"):
        mode_x = mode == "gridx"
        variant = sys.argv[3]
        m_fixed = float(sys.argv[4]) if len(sys.argv) > 4 else None
        p, base, ladder = fit_model(df, K, pn)
        L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
        dm = json.loads((HERE / f"r12_moments__{pn}.json").read_text())
        gaps = GAPS_FIXED if variant == "fixed" else GAPS_DECAY
        sd_all = np.array(dm["boot_sd"])
        idx = [GAPS_DECAY.index(g) * 3 + j for g in gaps for j in range(3)] + [len(GAPS_DECAY) * 3 + j for j in range(3)]
        target, sd = aux_vector(dm["aux"], gaps), sd_all[idx]
        if variant == "fixed" and mode_x:          # post hoc extension beyond the registered grid (PLAN.md, after A3)
            pts = [(s_, None, m_) for s_ in (0.6, 0.7, 0.8, 1.0) for m_ in (0.8, 0.9, 1.0, 1.1)]
        elif variant == "fixed":
            pts = [(s_, None, m_) for s_ in (0.0, 0.1, 0.2, 0.3, 0.4, 0.5) for m_ in (0.7, 0.8, 0.9, 1.0)]
        else:
            pts = [(s_, h, m_fixed or 1.0) for h in (26, 52, 104) for s_ in (0.2, 0.3, 0.4, 0.5)]
        rows = []
        for s_, h, m_ in pts:
            q = het_params(p, ladder, variant, s=s_, half_life=h, m=m_)
            vals = []
            for seed in range(3):
                fr, _, _ = sim_frame(q, L0, T, seed, K)
                X, R, floors, _, _ = m1.matrices(fr, K)
                vals.append(aux_vector(aux(X, R, floors, K, gaps), gaps))
            v = np.mean(vals, 0)
            obj = float(np.nansum(((v - target) / sd) ** 2))
            rows.append(dict(s=s_, half_life=h, m=m_, obj=obj, aux=v.tolist()))
            print(f"{pn} {variant} s {s_} hl {h} m {m_}: objective {obj:.2f}", flush=True)
        best = min(rows, key=lambda r: r["obj"])
        out = dict(panel=pn, variant=variant, target=target.tolist(), sd=sd.tolist(), rows=rows, best=best)
        (HERE / f"r12_grid{'x' if mode_x else ''}__{pn}__{variant}.json").write_text(json.dumps(out, indent=1, default=float))
        print("best", best, flush=True)
        return
    if mode in ("worlds", "oop"):
        variant = sys.argv[3]
        args = sys.argv[6:] if mode == "worlds" else sys.argv[4:]
        s_ = float(args[0]) if len(args) > 0 else 0.0
        h = float(args[1]) if len(args) > 1 and args[1] != "-" else None
        m_ = float(args[2]) if len(args) > 2 else 1.0
        if mode == "worlds":
            s0, s1 = int(sys.argv[4]), int(sys.argv[5])
            p, base, ladder = fit_model(df, K, pn)
            lev0 = rk._filtered_levels(df[df["period"] < 30].copy(), base)
            L0 = lev0.to_numpy()
            fac = None
            if variant == "factor":
                nb = T // 52
                start = T - nb * 52
                fac = factor_inputs(df, K, range(start, start + 52), lev0.index.to_numpy(), T)
                fac["L0"] = L0
            q = het_params(p, ladder, variant, s=s_, half_life=h, m=m_, fac=fac)
            for seed in range(s0, s1 + 1):
                fr, Xs, ids = sim_frame(q, L0, T, seed, K)
                res = world_stats(fr, Xs, ids, K, T, f"{variant}")
                res.update(seed=seed, s=s_, half_life=h, m=m_)
                if fac is not None:
                    res["factor_strata"] = fac["strata"]
                (HERE / f"r12_world__{pn}__{variant}__s{seed}.json").write_text(json.dumps(res, indent=1, default=float))
                print(f"{pn} {variant} seed {seed}: gapped {res['gapped']}; asym {res['r12_aux']['asym']}; "
                      f"turnover {res['turnover']}", flush=True)
            return
        # oop: as A6 (structure_oop.py): fit on weeks before 2021, simulate 2021-22 from the filtered state
        import minimal_rankdiff as mrd
        full = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[pn]["platform"]])
        nb = T // 52
        start = T - nb * 52
        t_origin = start + 2 * 52
        dtr = mrd.restrict_universe(full, K, buffer_mult=4, member_window=t_origin)
        dtr = dtr[dtr["period"] < t_origin].copy()
        p, base, ladder = fit_model(dtr, K, pn)
        levels = rk._filtered_levels(dtr, base)
        fac = None
        if variant == "factor":
            dfull = mrd.restrict_universe(full, K, buffer_mult=4)
            fx = factor_inputs(dfull, K, range(start, t_origin), levels.index.to_numpy(), T)
            fac = dict(path=fx["path"][t_origin - 1:], load=fx["load"], pool=fx["pool"], L0=levels.to_numpy(),
                       f_mean_test=float(np.mean(fx["path"][t_origin - 1:])), f_mean_train=float(np.mean(fx["path"][start:t_origin - 1])),
                       strata=fx["strata"])
        q = het_params(p, ladder, variant, s=s_, half_life=h, m=m_, fac=fac)
        test_len = T - t_origin
        shares = []
        import rankdiff_rdm as rdm
        for seed in range(20):
            Xs, ids = rdm.simulate_panel(q, levels.to_numpy(), test_len, seed=seed, lasting="movinghome_vp")
            u, inv = np.unique(ids, return_inverse=True)
            M = np.full((test_len, u.size), np.nan)
            M[np.repeat(np.arange(test_len), ids.shape[1]), inv.ravel()] = Xs.ravel()
            A = np.nan_to_num(np.expm1(M[-52:]), nan=0.0).sum(0)
            o = np.argsort(-A, kind="stable")
            shares.append(float(A[o[:K // 64]].sum() / A[o[:K]].sum()))
        if fac is not None:
            fac_diag = {k: fac[k] for k in ("f_mean_test", "f_mean_train", "strata")}
        out = dict(panel=pn, variant=variant, s=s_, half_life=h, m=m_, share_3=float(np.mean(shares)),
                   share_3_q=[float(np.quantile(shares, 0.05)), float(np.quantile(shares, 0.95))], shares=shares)
        if fac is not None:
            out["factor"] = fac_diag
        (HERE / f"r12_oop__{pn}__{variant}.json").write_text(json.dumps(out, indent=1, default=float))
        print(json.dumps({k: v for k, v in out.items() if k != "shares"}), flush=True)
        return
    if mode == "recovery":
        planted, s0, s1 = float(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
        import rankdiff_rdm as rdm
        p, base, ladder = fit_model(df, K, pn)
        L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
        q = het_params(p, ladder, "fixed" if planted > 0 else "control_num", s=planted, m=1.0)   # A4: one drift method
        rows = []
        for seed in range(s0, s1 + 1):
            fr, _, _ = sim_frame(q, L0, T, seed, K)
            # oracle passing parts (the data's base): isolates the effect of heterogeneity on sigma_0 and beta
            pw = rdm.estimate_rdm(fr, K, base=base, jumps=False, variant="move", slides=(np.array([1.0, 4.0 * K]), np.zeros(2)))
            rows.append(dict(seed=seed, sigma0=float(pw.sigma0), beta=float(pw.beta), s_band=np.asarray(pw.s_band).tolist()))
            print(f"{pn} planted s {planted} seed {seed}: sigma0 {pw.sigma0:.4f} (true {q.sigma0:.4f}), beta {pw.beta:+.3f} (true {q.beta:+.3f})", flush=True)
        out = dict(panel=pn, planted_s=planted, true_sigma0=float(q.sigma0), true_beta=float(q.beta), rows=rows)
        (HERE / f"r12_recovery__{pn}__s{planted}.json").write_text(json.dumps(out, indent=1))
        return
    raise SystemExit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
