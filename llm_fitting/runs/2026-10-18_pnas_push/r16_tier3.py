"""R16 of PLAN.md (Tier 3 after referee round 7; owner, 2026-10-21: "try all the tier 3 items"): model-side analyses.

  A  a reversible decline state (GLLM-type switching, mirrored): the home drifts by -delta (Z - pi_D), Z in {0, 1}
     entering at weekly hazard a and leaving at b, pi_D = a / (a + b); calibrated like R12 (b) at three pi_D
  B  a joint re-estimation of fixed heterogeneity: a spread for the head (communities starting in the top K/16) apart
     from the body's, the moving home's OU reversion freed (variance kept), the lasting multiplier m; 24 moments
     including weekly autocovariances
  C  the two registered A1 runs (fit the fixed form to a decaying world; a homogeneous world with a 26-week medium
     swing) and a calibration of the objective's size under correct specification
  D  recovery of the size exponent with the world's own passing parts (the full estimator, as on the data)
  F  the durability comparison in the calibrated model: the shape's response to a step in the head's decline
     against membership turnover, at three lasting-variance multipliers

Every statistic is computed by the same code on data and worlds (r12_hetgrowth); the lean cache ($R12_CACHE) is read.

    python r16_tier3.py moments <panel>
    python r16_tier3.py decline_grid <panel>
    python r16_tier3.py joint_grid <panel>
    python r16_tier3.py worlds <panel> <decline|joint> <s0> <s1> <json params>
    python r16_tier3.py oop <panel> <decline|decline_hist|joint|control_num> <json params>
    python r16_tier3.py summarize <panel> <tag>
    python r16_tier3.py a1 <panel>
    python r16_tier3.py beta <panel> <s0> <s1>
    python r16_tier3.py durability <panel>
"""
import gc
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402
import r12_hetgrowth as h  # noqa: E402

GAPS_ALL = (0, 13, 26, 39, 52)
GAPS_J, LAGS_J = (0, 13, 26), (1, 2, 4, 8)
SEEDS_GRID = (0, 1, 2)
M_GRID = {"reddit_submissions_long": (0.8, 1.0, 1.2), "reddit_comments_ext": (0.5, 0.7, 0.9)}   # centred on R12's optima
M_FIXED = {"reddit_submissions_long": 1.0, "reddit_comments_ext": 0.7}                         # R12 (a)'s optima


# --------------------------------------------------------------------------- statistics
def stratum_rho(X, lab, g=13):
    """Gapped Spearman correlation of year-apart changes within each stratum (mean over origins every 13 weeks)."""
    T = X.shape[0]
    out = {j: [] for j in range(3)}
    a = 0
    while a + 2 * (h.SPAN + h.W) + g <= T:
        x, y = h.window_changes(X, a), h.window_changes(X, a + h.SPAN + h.W + g)
        for j in range(3):
            ok = (lab == j) & np.isfinite(x) & np.isfinite(y)
            if ok.sum() > 30:
                out[j].append(spearmanr(x[ok], y[ok]).correlation)
        a += 13
    return [float(np.mean(out[j])) if out[j] else float("nan") for j in range(3)]


def tail_followup(X, lab, g=13, q=0.1):
    """Sign-conditioned transitions: the next year's change (stratum-demeaned) after the bottom and the top decile of
    this year's change, mean over origins every 13 weeks."""
    T = X.shape[0]
    lo, hi = [], []
    a = 0
    while a + 2 * (h.SPAN + h.W) + g <= T:
        x, y = h.window_changes(X, a), h.window_changes(X, a + h.SPAN + h.W + g)
        ok = (lab >= 0) & np.isfinite(x) & np.isfinite(y)
        xd, yd = x.copy(), y.copy()
        for j in range(3):
            s = ok & (lab == j)
            if s.sum() > 30:
                xd[s] -= x[s].mean()
                yd[s] -= y[s].mean()
        if ok.sum() > 100:
            ql, qh = np.quantile(xd[ok], [q, 1 - q])
            lo.append(float(yd[ok & (xd <= ql)].mean()))
            hi.append(float(yd[ok & (xd >= qh)].mean()))
        a += 13
    return dict(after_fall=float(np.mean(lo)), after_rise=float(np.mean(hi)))


def aux16(X, R, floors, K):
    lab = h.strata(R, floors, K)
    return dict(gapped=h.gapped_cov(X, lab, GAPS_ALL), sf=h.sf_lasting(X, lab), wk=h.weekly_autocov(X, lab, lags=LAGS_J),
                asym=h.asymmetry(X, lab), srho=stratum_rho(X, lab), tails=tail_followup(X, lab))


def _cov(a, g):
    gp = a["gapped"]
    return gp.get(int(g), gp.get(str(g)))["cov"]


def vec(a, kind):
    if kind == "fixed":
        return np.array([c for g in (0, 13) for c in _cov(a, g)] + list(a["sf"]))
    if kind == "decay":
        return np.array([c for g in (0, 13, 26, 39) for c in _cov(a, g)] + list(a["sf"]))
    return np.array([c for g in GAPS_J for c in _cov(a, g)] + list(a["sf"]) + [c for row in a["wk"] for c in row])


# --------------------------------------------------------------------------- parameters
def _hold(q, ladder):
    import rankdiff_rdm as rdm
    dz, dg, _ = rdm.drift_hold_numeric(replace(q, dec_init=None), ladder)
    return replace(q, drift_z=dz, drift_g=dg)


def dec_params(p, ladder, pi, s, hl, m, init=None):
    """Decline state with annual spread s = 52 delta sqrt(pi (1 - pi)) and state half-life hl weeks."""
    r = 1.0 - 0.5 ** (1.0 / hl)
    delta = (s / 52.0) / np.sqrt(pi * (1 - pi))
    q = replace(p, sigma0=p.sigma0 * m, dec_delta=float(delta), dec_enter=float(pi * r), dec_exit=float((1 - pi) * r), dec_init=init)
    return _hold(q, ladder)


def joint_params(p, ladder, K, s_body, head_mult, km, m):
    q = replace(p, sigma0=p.sigma0 * m, vel_var=(s_body / 52.0) ** 2, vel_rho=1.0, vel_clip=h.CLIP,
                vel_head_k=int(K // 16), vel_head_mult=float(head_mult), ou_kappa_mult=float(km))
    return _hold(q, ladder)


def build(variant, p, ladder, K, prm, init=None):
    if variant in ("decline", "decline_hist"):
        return dec_params(p, ladder, prm["pi"], prm["s"], prm["hl"], prm["m"], init=init)
    if variant == "joint":
        return joint_params(p, ladder, K, prm["s"], prm["head"], prm["km"], prm["m"])
    return h.het_params(p, ladder, variant, s=prm.get("s", 0.0), half_life=prm.get("hl"), m=prm.get("m", 1.0))


def _setup(pn):
    import rankdiff_kalman as rk
    df, K = h.load(pn)
    T = int(df["period"].max()) + 1
    p, base, ladder = h.fit_model(df, K, pn)
    L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
    return df, K, T, p, base, ladder, L0


def _world_aux(q, L0, T, seed, K):
    fr, Xs, ids = h.sim_frame(q, L0, T, seed, K)
    X, R, floors, _, _ = m1.matrices(fr, K)
    a = aux16(X, R, floors, K)
    return a, fr, Xs, ids


def _dump(path, obj):
    path.write_text(json.dumps(obj, indent=1, default=float))


# --------------------------------------------------------------------------- modes
def mode_moments(pn):
    df, K = h.load(pn)
    X, R, floors, _, _ = m1.matrices(df, K)
    del df
    gc.collect()
    a = aux16(X, R, floors, K)
    lab = h.strata(R, floors, K)
    rng = np.random.default_rng(16)
    boots = []
    for _ in range(40):                                  # communities resampled within strata, as R12
        cols = np.concatenate([rng.choice(np.flatnonzero(lab == j), (lab == j).sum(), replace=True) for j in range(3)])
        Xb, labb = X[:, cols], lab[cols]
        ab = dict(gapped=h.gapped_cov(Xb, labb, GAPS_J), sf=h.sf_lasting(Xb, labb), wk=h.weekly_autocov(Xb, labb, lags=LAGS_J))
        boots.append(vec(ab, "joint"))
    sd = np.std(np.array(boots), axis=0, ddof=1)
    _dump(HERE / f"r16_moments__{pn}.json", dict(panel=pn, K=K, aux=a, boot_sd_joint=sd.tolist(),
                                                 order="cov at gaps 0,13,26 x 3 strata; sf x 3; weekly autocov lags 1,2,4,8 x 3 strata"))
    print(json.dumps(dict(srho=a["srho"], tails=a["tails"], asym=a["asym"]), default=float), flush=True)


def _grid(pn, variant, pts, kind):
    df, K, T, p, base, ladder, L0 = _setup(pn)
    del df
    gc.collect()
    if kind == "joint":
        dm = json.loads((HERE / f"r16_moments__{pn}.json").read_text())
        target, sd = vec(dm["aux"], "joint"), np.array(dm["boot_sd_joint"])
    else:                                                 # the R12 decay vector and its bootstrap sd
        dm = json.loads((HERE / f"r12_moments__{pn}.json").read_text())
        target, sd = vec(dm["aux"], "decay"), np.array(dm["boot_sd"])
    rows = []
    for prm in pts:
        q = build(variant, p, ladder, K, prm)
        auxs = [_world_aux(q, L0, T, sd_, K)[0] for sd_ in SEEDS_GRID]
        gc.collect()
        v = np.mean([vec(a, kind) for a in auxs], 0)
        obj = float(np.nansum(((v - target) / sd) ** 2))
        rows.append(dict(prm=prm, obj=obj, aux=v.tolist(),
                         asym=float(np.mean([a["asym"]["diff"] for a in auxs])),
                         rho52=float(np.mean([a["gapped"][52]["rho"] for a in auxs]))))
        print(f"{pn} {variant} {prm}: objective {obj:.1f}", flush=True)
    return dict(panel=pn, variant=variant, kind=kind, target=target.tolist(), sd=sd.tolist(), rows=rows,
                n_moments=int(target.size))


def mode_decline_grid(pn):
    m = M_FIXED[pn]
    pts = [dict(pi=pi, s=s, hl=hl, m=m) for pi in (0.1, 0.25, 0.5) for hl in (52, 104, 208) for s in (0.3, 0.5, 0.7)]
    out = _grid(pn, "decline", pts, "decay")
    out["best_by_pi"] = {str(pi): min([r for r in out["rows"] if r["prm"]["pi"] == pi], key=lambda r: r["obj"]) for pi in (0.1, 0.25, 0.5)}
    out["best"] = min(out["rows"], key=lambda r: r["obj"])
    _dump(HERE / f"r16_decline_grid__{pn}.json", out)
    print("best by pi", {k: (v["prm"], round(v["obj"], 1)) for k, v in out["best_by_pi"].items()}, flush=True)


def mode_joint_grid(pn):
    pts = [dict(s=s, head=hm, km=km, m=m) for s in (0.4, 0.6, 0.8) for hm in (0.25, 0.5, 1.0) for km in (0.5, 1.0, 2.0)
           for m in M_GRID[pn]]
    out = _grid(pn, "joint", pts, "joint")
    out["best"] = min(out["rows"], key=lambda r: r["obj"])
    _dump(HERE / f"r16_joint_grid__{pn}.json", out)
    print("best", out["best"]["prm"], round(out["best"]["obj"], 1), flush=True)


def mode_worlds(pn, variant, s0, s1, prm):
    df, K, T, p, base, ladder, L0 = _setup(pn)
    del df
    gc.collect()
    q = build(variant, p, ladder, K, prm)
    tag = f"{variant}_{'_'.join(f'{k}{v}' for k, v in prm.items())}"
    for seed in range(s0, s1 + 1):
        fr, Xs, ids = h.sim_frame(q, L0, T, seed, K)
        res = h.world_stats(fr, Xs, ids, K, T, variant)
        X, R, floors, _, _ = m1.matrices(fr, K)
        res["r16_aux"] = aux16(X, R, floors, K)
        res.update(seed=seed, prm=prm)
        _dump(HERE / f"r16_world__{pn}__{tag}__s{seed}.json", res)
        print(f"{pn} {tag} seed {seed}: asym {res['r16_aux']['asym']['diff']:+.3f}; srho {np.round(res['r16_aux']['srho'], 3).tolist()}; "
              f"tails {res['r16_aux']['tails']}", flush=True)
        del fr, Xs, ids, X, R
        gc.collect()


def mode_summarize(pn, tag):
    files = sorted(HERE.glob(f"r16_world__{pn}__{tag}__s*.json"))
    W_ = [json.loads(f.read_text()) for f in files]

    def band(vals):
        v = np.array(vals, float)
        return dict(mean=float(np.nanmean(v)), q05=float(np.nanquantile(v, 0.05)), q95=float(np.nanquantile(v, 0.95)), n=int(np.isfinite(v).sum()))
    out = dict(panel=pn, tag=tag, n_seeds=len(W_),
               gapped={str(g): band([w["r16_aux"]["gapped"][str(g)]["rho"] for w in W_]) for g in GAPS_ALL},
               asym=band([w["r16_aux"]["asym"]["diff"] for w in W_]),
               srho=[band([w["r16_aux"]["srho"][j] for w in W_]) for j in range(3)],
               after_fall=band([w["r16_aux"]["tails"]["after_fall"] for w in W_]),
               after_rise=band([w["r16_aux"]["tails"]["after_rise"] for w in W_]),
               turnover={k: band([w["turnover"][k] for w in W_]) for k in W_[0]["turnover"]},
               boundary=band([w.get("boundary_pooled") for w in W_]),
               r10_contrast=band([np.mean([r["Q1"] - r["rest"] for r in w["r10"]]) for w in W_]),
               retention={k: {hh: band([w["lockin"]["retention"][k][hh]["mean"] for w in W_]) for hh in ("1", "2")}
                          for k in W_[0]["lockin"]["retention"]})
    _dump(HERE / f"r16_worlds__{pn}__{tag}.json", out)
    print(json.dumps(out, default=float)[:2500], flush=True)


def mode_oop(pn, variant, prm):
    """As r12_hetgrowth oop (A6's design): fit on weeks before 2021, simulate 2021-22 from the filtered state.
    decline_hist starts each community's state from its training history: in decline if its last training-year
    change of 13-week window means is in the bottom pi_D of its stratum (by training permanent rank)."""
    import rankdiff_kalman as rk
    import rankdiff_rdm as rdm
    df, K = h.load(pn)
    T = int(df["period"].max()) + 1
    del df
    gc.collect()
    nb = T // 52
    start = T - nb * 52
    t_origin = start + 2 * 52
    dtr, _ = h.load(pn, member_window=t_origin)
    dtr = dtr[dtr["period"] < t_origin].copy()
    p, base, ladder = h.fit_model(dtr, K, pn)
    levels = rk._filtered_levels(dtr, base)
    init = None
    if variant == "decline_hist":
        X, R, floors, _, ids = m1.matrices(dtr, K)
        chg = h.wmean(X, t_origin - h.W, t_origin) - h.wmean(X, t_origin - h.SPAN - h.W, t_origin - h.SPAN)
        lab = h.strata(R, floors, K)
        flag = np.zeros(chg.size, bool)
        for j in range(3):
            s_ = (lab == j) & np.isfinite(chg)
            if s_.sum() > 30:
                flag[s_ & (chg <= np.quantile(chg[s_], prm["pi"]))] = True
        f_by_id = dict(zip(np.asarray(ids).astype(str), flag))
        init = np.array([bool(f_by_id.get(str(e), False)) for e in levels.index.to_numpy()])   # aligned to L0
    del dtr
    gc.collect()
    q = build(variant, p, ladder, K, prm, init=init)
    test_len = T - t_origin
    shares = []
    for seed in range(20):
        Xs, ids = rdm.simulate_panel(q, levels.to_numpy(), test_len, seed=seed, lasting="movinghome_vp")
        u, inv = np.unique(ids, return_inverse=True)
        M = np.full((test_len, u.size), np.nan)
        M[np.repeat(np.arange(test_len), ids.shape[1]), inv.ravel()] = Xs.ravel()
        A = np.nan_to_num(np.expm1(M[-52:]), nan=0.0).sum(0)
        o = np.argsort(-A, kind="stable")
        shares.append(float(A[o[:K // 64]].sum() / A[o[:K]].sum()))
    out = dict(panel=pn, variant=variant, prm=prm, share=float(np.mean(shares)),
               share_q=[float(np.quantile(shares, 0.05)), float(np.quantile(shares, 0.95))], shares=shares,
               init_share=None if init is None else float(init.mean()))
    _dump(HERE / f"r16_oop__{pn}__{variant}.json", out)
    print(json.dumps({k: v for k, v in out.items() if k != "shares"}, default=float), flush=True)


def mode_a1(pn):
    """(i) fixed form fit to a decaying world, (ii) a homogeneous world with a 26-week medium swing, (iii) the fixed
    world at its own optimum: 20 pseudo-data panels each (seeds 1000-1019), each calibrated by the registered fixed
    grid's saved simulated moments (r12_grid__<panel>__fixed.json) and the data's bootstrap sd."""
    df, K, T, p, base, ladder, L0 = _setup(pn)
    del df
    gc.collect()
    grid = json.loads((HERE / f"r12_grid__{pn}__fixed.json").read_text())
    G = np.array([r["aux"] for r in grid["rows"]])
    sd = np.array(grid["sd"])
    dec = json.loads((HERE / f"r12_grid__{pn}__decay.json").read_text())["best"]
    fx = grid["best"]
    kz = np.asarray(p.base.kappa_z if p.base.kappa_z is not None else [p.base.kappa], float)
    km = (1 - 0.5 ** (1 / 26.0)) / float(np.median(kz))
    worlds = {"decay_world": h.het_params(p, ladder, "decay", s=dec["s"], half_life=dec["half_life"], m=dec["m"]),
              "swing26_world": replace(h.het_params(p, ladder, "control_num"), ou_kappa_mult=float(km)),
              "fixed_world": h.het_params(p, ladder, "fixed", s=fx["s"], m=fx["m"])}
    out = dict(panel=pn, grid_points=[dict(s=r["s"], m=r["m"]) for r in grid["rows"]], data_obj=grid["best"]["obj"],
               base_ou_half_life_median=float(np.log(0.5) / np.log(1 - np.median(kz))), km_swing26=float(km),
               decay_truth=dict(s=dec["s"], half_life=dec["half_life"], m=dec["m"]), fixed_truth=dict(s=fx["s"], m=fx["m"]), worlds={})
    for name, q in worlds.items():
        reps = []
        for seed in range(1000, 1020):
            a = _world_aux(q, L0, T, seed, K)[0]
            v = vec(a, "fixed")
            obj = np.nansum(((G - v) / sd) ** 2, axis=1)
            j = int(np.argmin(obj))
            reps.append(dict(seed=seed, s=grid["rows"][j]["s"], m=grid["rows"][j]["m"], obj=float(obj[j]),
                             rho52=a["gapped"][52]["rho"], asym=a["asym"]["diff"]))
            gc.collect()
        out["worlds"][name] = reps
        ss = [r["s"] for r in reps]
        print(f"{pn} {name}: chosen s {np.round(ss, 2).tolist()}; min objective median {np.median([r['obj'] for r in reps]):.1f} "
              f"[{np.quantile([r['obj'] for r in reps], 0.05):.1f}, {np.quantile([r['obj'] for r in reps], 0.95):.1f}] (data {out['data_obj']:.1f})", flush=True)
        _dump(HERE / f"r16_a1__{pn}.json", out)


def _world_frame_for_fit(fr, K):
    import minimal_rankdiff as mrd
    fr = fr.copy()
    fr["metric"] = np.expm1(fr["X"].to_numpy())
    fr["ts"] = np.datetime64("2019-01-07") + 7 * fr["period"].to_numpy().astype("timedelta64[D]")
    fr["z"] = np.log(np.clip((fr["rank"].to_numpy() - 0.5) / fr["N"].to_numpy(), 1e-12, 1.0))
    return mrd.restrict_universe(fr, K, buffer_mult=4)


def mode_beta(pn, s0, s1):
    """D: the full estimator (base passing parts and the hold model's sigma_0 and beta, as fit_model on the data) on
    world panels simulated from the matched homogeneous control at the fitted beta and at beta 0."""
    df, K, T, p, base, ladder, L0 = _setup(pn)
    del df
    gc.collect()
    truths = {"fitted": h.het_params(p, ladder, "control_num"), "zero": _hold(replace(p, beta=0.0), ladder)}
    f = HERE / f"r16_beta__{pn}.json"
    out = json.loads(f.read_text()) if f.exists() else dict(panel=pn, data_beta=float(p.beta), data_sigma0=float(p.sigma0), rows=[])
    for name, q in truths.items():
        for seed in range(s0, s1 + 1):
            fr, _, _ = h.sim_frame(q, L0, T, seed, K)
            fu = _world_frame_for_fit(fr, K)
            del fr
            gc.collect()
            pw, _, _ = h.fit_model(fu, K, pn)
            out["rows"] = [r for r in out["rows"] if not (r["truth"] == name and r["seed"] == seed)]
            out["rows"].append(dict(truth=name, seed=seed, true_beta=float(q.beta), beta=float(pw.beta), true_sigma0=float(q.sigma0),
                                    sigma0=float(pw.sigma0)))
            print(f"{pn} truth {name} seed {seed}: beta {pw.beta:+.3f} (true {q.beta:+.3f}); sigma0 {pw.sigma0:.4f} (true {q.sigma0:.4f})", flush=True)
            _dump(f, out)
            del fu
            gc.collect()


def _block_shape_and_retention(Xs, ids, K, ks, nblocks):
    """52-week block means by identity (present >= 90% of the block), the shape U_k - U_K per block, and the share of
    each block-0 top k still in the top k at later blocks."""
    T = Xs.shape[0]
    uid, inv = np.unique(ids, return_inverse=True)
    M = np.full((T, uid.size), np.nan)
    M[np.repeat(np.arange(T), ids.shape[1]), inv.ravel()] = Xs.ravel()
    B = [h.wmean(M, 52 * b, 52 * (b + 1)) for b in range(nblocks)]
    shape, ret = [], []
    tops = []
    for b in range(nblocks):
        fin = np.flatnonzero(np.isfinite(B[b]))
        o = fin[np.argsort(-B[b][fin], kind="stable")]
        lad = B[b][o]
        shape.append([float(lad[:k].mean() - lad[:K].mean()) for k in ks])
        tops.append({k: set(o[:k].tolist()) for k in ks})
    for b in range(1, nblocks):
        ret.append([len(tops[0][k] & tops[b][k]) / k for k in ks])
    return np.array(shape), np.array(ret)


def mode_durability(pn):
    """F: from the observed 2019 ladder, the matched homogeneous model at lasting multipliers m 0.7, 1.0, 1.4 (drift
    re-solved to hold the ladder at each), 10 seeds, 260 weeks; a step of DELTA a year in the decline of the current top
    K/64 from week 0, against the same seeds without it."""
    DELTA = 0.2
    df, K, T, p, base, ladder, L0 = _setup(pn)
    del df
    gc.collect()
    import rankdiff_rdm as rdm
    ks = (K // 64, K // 16)
    N = L0.size
    out = dict(panel=pn, delta_per_year=DELTA, ks=list(ks), by_m={})
    for m in (0.7, 1.0, 1.4):
        q = h.het_params(p, ladder, "control_num", m=m)
        zk = np.log((ks[0] - 0.5) / N)
        z = np.unique(np.concatenate([q.drift_z, [zk - 1e-6, zk + 1e-6]]))
        g = np.interp(z, q.drift_z, q.drift_g) - (DELTA / 52.0) * (z < zk)
        qs = replace(q, drift_z=z, drift_g=g)
        qc = replace(q, drift_z=z, drift_g=np.interp(z, q.drift_z, q.drift_g))
        rows = []
        for seed in range(10):
            res = {}
            for name, qq in (("control", qc), ("shock", qs)):
                Xs, ids = rdm.simulate_panel(qq, L0, 260, seed=seed, lasting="movinghome_vp")
                sh, rt = _block_shape_and_retention(Xs, ids, K, ks, 5)
                res[name] = dict(shape=sh.tolist(), retention=rt.tolist())
                del Xs, ids
                gc.collect()
            rows.append(dict(seed=seed, **res))
            print(f"{pn} m {m} seed {seed}: response at K/64 by year "
                  f"{np.round(np.array(res['shock']['shape'])[:, 0] - np.array(res['control']['shape'])[:, 0], 3).tolist()}; "
                  f"retention K/64 {np.round(np.array(res['control']['retention'])[:, 0], 3).tolist()}", flush=True)
        resp = np.array([np.array(r["shock"]["shape"]) - np.array(r["control"]["shape"]) for r in rows])     # seeds x blocks x ks
        ret = np.array([r["control"]["retention"] for r in rows])                                               # seeds x 4 x ks
        out["by_m"][str(m)] = dict(rows=rows, response_mean=resp.mean(0).tolist(), response_q=np.quantile(resp, [0.05, 0.95], axis=0).tolist(),
                                   retention_mean=ret.mean(0).tolist(), sigma0=float(q.sigma0))
        _dump(HERE / f"r16_durability__{pn}.json", out)


def main():
    mode, pn = sys.argv[1], sys.argv[2]
    if mode == "moments":
        return mode_moments(pn)
    if mode == "decline_grid":
        return mode_decline_grid(pn)
    if mode == "joint_grid":
        return mode_joint_grid(pn)
    if mode == "worlds":
        return mode_worlds(pn, sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), json.loads(sys.argv[6]))
    if mode == "summarize":
        return mode_summarize(pn, sys.argv[3])
    if mode == "oop":
        return mode_oop(pn, sys.argv[3], json.loads(sys.argv[4]))
    if mode == "a1":
        return mode_a1(pn)
    if mode == "beta":
        return mode_beta(pn, int(sys.argv[3]), int(sys.argv[4]))
    if mode == "durability":
        return mode_durability(pn)
    raise SystemExit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
