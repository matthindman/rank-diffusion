"""R20 of PLAN.md (owner, 2026-10-21: "do the fading surge model"): a positively skewed transient component added to
observed log activity, calibrated to the marginal shape of single changes (never to persistence, D or any joint
statistic of two changes), and scored against the same worlds without it.

    s_it = rho s_i,t-1 + J_it,   J_it = E_it B_it,   B ~ Bernoulli(lam),  E ~ Exponential(mean mu),  rho = 0.5 ** (1 / hl)

The simulator's rank-dependent parameters and exits read only the lasting level (`rankdiff_rdm._State.step`), so
adding s to the simulated observed matrix after simulation is exactly an observation-level component.  s restarts at
0 when a slot is reborn as a new unit; the first occupants get a 104-week burn-in; its random stream is seed + 7,000,000.

Bases: on Reddit the matched control (`control_num`) and the jointly estimated fixed differences at R17's optimum
(`joint`); on Facebook the matched control at beta 0.019 (`fbcontrol`) and, for R20-A2, the same from R18-D's
matched start (`fbcontrolm`).

    python r20_surge.py moments <panel>                         data shape moments and bootstrap SDs
    python r20_surge.py calib <panel> <control_num|joint|fbcontrol>
    python r20_surge.py eval <panel> <base> <s0> <s1>           base and base + surge, paired, per seed
    python r20_surge.py summ <panel> <base>
    python r20_surge.py calibx <panel> <base>                   R20-A1 (post hoc): half-lives 78, 104, 156 added
    python r20_surge.py evalx <panel> <base> <s0> <s1>          at the extended optimum (r20_worldx files)
    python r20_surge.py summx <panel> <base>

Reddit runs need $R12_CACHE; Facebook runs need it unset and $R18_DIR (the R18 fit).
"""
import gc
import itertools
import json
import pickle
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import mts_step1 as m1  # noqa: E402
import r12_hetgrowth as h  # noqa: E402
import r16_tier3 as t3  # noqa: E402
import r17_round8 as r17  # noqa: E402
import r18_facebook as fb  # noqa: E402

GRID = dict(lam=(0.002, 0.005, 0.01, 0.02), mu=(0.5, 1.0, 2.0), hl=(4, 13, 26, 52))
HL_X = (78, 104, 156)                                     # R20-A1 (post hoc): beyond the registered grid
CAL_SEEDS, BURN, WW = (200, 201), 104, 4
OFFSET = 7_000_000


def horizons(pn):
    return (4, 13, 26) if pn == "facebook_a" else (4, 13, 26, 52)


def step_of(pn):
    return 4 if pn == "facebook_a" else 13


# --------------------------------------------------------------------------- the calibration moments
def shape_moments(X, lab, hz, step):
    """Kelly skewness and tail asymmetry of x, the change of 4-week window means H weeks apart, demeaned within strata
    and pooled over origins, for each horizon H."""
    T = X.shape[0]
    out = []
    for H in hz:
        xs, a = [], 0
        while a + H + WW <= T:
            x = h.wmean(X, a + H, a + H + WW) - h.wmean(X, a, a + WW)
            ok = (lab >= 0) & np.isfinite(x)
            for j in range(3):
                s = ok & (lab == j)
                if s.sum() > 30:
                    x[s] -= x[s].mean()
            xs.append(x[ok])
            a += step
        v = np.concatenate(xs)
        q10, q50, q90 = np.quantile(v, [0.1, 0.5, 0.9])
        top, bot = v[v >= q90].mean(), v[v <= q10].mean()
        out += [(q90 + q10 - 2 * q50) / (q90 - q10), (top + bot) / (top - bot)]
    return np.array(out)


def moment_names(pn):
    return [f"{m}_{H}" for H in horizons(pn) for m in ("kelly", "tail")]


def mode_moments(pn):
    df, K = h.load(pn)
    X, R, floors, _, _ = m1.matrices(df, K)
    del df
    gc.collect()
    lab = h.strata(R, floors, K)
    hz, st = horizons(pn), step_of(pn)
    m = shape_moments(X, lab, hz, st)
    rng = np.random.default_rng(20)
    boots = []
    for _ in range(40):
        cols = np.concatenate([rng.choice(np.flatnonzero(lab == j), (lab == j).sum(), replace=True) for j in range(3)])
        boots.append(shape_moments(X[:, cols], lab[cols], hz, st))
    sd = np.std(np.array(boots), axis=0, ddof=1)
    out = dict(panel=pn, K=K, names=moment_names(pn), moments=m.tolist(), boot_sd=sd.tolist())
    (HERE / f"r20_moments__{pn}.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out), flush=True)


# --------------------------------------------------------------------------- worlds
def setup(pn, base):
    """(q, L0, K, T) of the base world."""
    if base in ("fbcontrol", "fbcontrolm"):
        with open(fb._fit_path(), "rb") as f:
            fit = pickle.load(f)
        v = "controlb0.019"
        if base == "fbcontrolm":                         # R20-A2: from R18-D's matched start
            v += f"m{json.loads((HERE / 'r18d_matchcal.json').read_text())['c']}"
        q, L0 = fb._world_q(v, fit["p"], fit["ladder"], fit["L0"], fit["K"])
        return q, L0, fit["K"], fit["T"]
    df, K, T, p, _, ladder, L0 = t3._setup(pn)
    del df
    gc.collect()
    q = t3.build("joint", p, ladder, K, r17.JOINT[pn]) if base == "joint" else h.het_params(p, ladder, "control_num")
    return q, L0, K, T


def simulate(q, L0, T, seed):
    import rankdiff_rdm as rdm
    return rdm.simulate_panel(q, L0, T, seed=seed, lasting="movinghome_vp")


def draws(T, N, seed):
    """Common random numbers for the surge: uniforms and unit exponentials, burn-in rows first."""
    rng = np.random.default_rng(seed + OFFSET)
    return rng.random((BURN + T, N)), rng.standard_exponential((BURN + T, N))


def surge(ids, lam, mu, hl, U, E):
    T, N = ids.shape
    rho = 0.5 ** (1.0 / hl)
    s = np.zeros(N)
    for t in range(BURN):
        s = rho * s + (U[t] < lam) * mu * E[t]
    S = np.empty((T, N))
    for t in range(T):
        if t > 0:
            s[ids[t] != ids[t - 1]] = 0.0
        s = rho * s + (U[BURN + t] < lam) * mu * E[BURN + t]
        S[t] = s
    return S


def frame_of(X, ids, K, T):
    return h._nw(K, T).frame_from_matrix(X, ids)


def moments_of(fr, K, pn):
    X, R, floors, _, _ = m1.matrices(fr, K)
    return shape_moments(X, h.strata(R, floors, K), horizons(pn), step_of(pn))


def mode_calib(pn, base, ext=False):
    q, L0, K, T = setup(pn, base)
    dm = json.loads((HERE / f"r20_moments__{pn}.json").read_text())
    target, sd = np.array(dm["moments"]), np.array(dm["boot_sd"])
    sims = []
    for seed in CAL_SEEDS:
        Xs, ids = simulate(q, L0, T, seed)
        sims.append((Xs, ids, *draws(T, Xs.shape[1], seed)))
    base_m = np.mean([moments_of(frame_of(Xs, ids, K, T), K, pn) for Xs, ids, _, _ in sims], 0)
    rows = []
    for lam, mu, hl in itertools.product(GRID["lam"], GRID["mu"], HL_X if ext else GRID["hl"]):
        ms = [moments_of(frame_of(Xs + surge(ids, lam, mu, hl, U, E), ids, K, T), K, pn) for Xs, ids, U, E in sims]
        v = np.mean(ms, 0)
        obj = float(np.sum(((v - target) / sd) ** 2))
        rows.append(dict(lam=lam, mu=mu, hl=hl, obj=obj, moments=v.tolist()))
        print(f"{pn} {base} lam {lam} mu {mu} hl {hl}: objective {obj:.1f}", flush=True)
        gc.collect()
    if ext:                                             # R20-A1: the registered rows plus the new half-lives
        rows = json.loads((HERE / f"r20_calib__{pn}__{base}.json").read_text())["rows"] + rows
    best = min(rows, key=lambda r: r["obj"])
    out = dict(panel=pn, base=base, names=dm["names"], target=target.tolist(), sd=sd.tolist(), base_moments=base_m.tolist(),
               base_obj=float(np.sum(((base_m - target) / sd) ** 2)), rows=rows, best=best)
    (HERE / f"r20_calib{'x' if ext else ''}__{pn}__{base}.json").write_text(json.dumps(out, indent=1))
    print("best", json.dumps(best), "base objective", round(out["base_obj"], 1), flush=True)


def secants(curve):
    c = np.asarray(curve, float)
    xs, ys = c[:, 0], c[:, 1]
    return dict(low=float((ys[1] - ys[0]) / (xs[1] - xs[0])), mid=float((ys[7] - ys[2]) / (xs[7] - xs[2])),
                high=float((ys[9] - ys[8]) / (xs[9] - xs[8])))


def score(fr, K, T, pn):
    out = dict(moments=moments_of(fr, K, pn).tolist())
    if pn == "facebook_a":
        out.update(fb.fb_stats(fr, K, T))
        return out
    st = h._nw(K, T).stats(fr, "r20")
    out.update(turnover=st["turnover"], gapped=st["gapped"], boundary_pooled=st.get("boundary_pooled"))
    X, R, floors, _, _ = m1.matrices(fr, K)
    a = r17.asym_stat(X, h.strata(R, floors, K))
    out.update(D=a["D"], p_fall=a["p_fall"], p_rise=a["p_rise"], curve=a["curve"], secants=secants(a["curve"]))
    return out


def mode_eval(pn, base, s0, s1, ext=False):
    x = "x" if ext else ""
    q, L0, K, T = setup(pn, base)
    best = json.loads((HERE / f"r20_calib{x}__{pn}__{base}.json").read_text())["best"]
    for seed in range(s0, s1 + 1):
        Xs, ids = simulate(q, L0, T, seed)
        U, E = draws(T, Xs.shape[1], seed)
        S = surge(ids, best["lam"], best["mu"], best["hl"], U, E)
        del U, E
        res = dict(panel=pn, base=base, seed=seed, surge=dict(lam=best["lam"], mu=best["mu"], hl=best["hl"]),
                   base_world=score(frame_of(Xs, ids, K, T), K, T, pn), surge_world=score(frame_of(Xs + S, ids, K, T), K, T, pn))
        res = json.loads(json.dumps(res, default=float))       # string keys, as the saved file has them
        (HERE / f"r20_world{x}__{pn}__{base}__s{seed}.json").write_text(json.dumps(res, indent=1))
        b, s = res["base_world"], res["surge_world"]
        if pn == "facebook_a":
            print(f"{pn} {base} seed {seed}: D_FB {b['D_FB']['D']:+.3f} -> {s['D_FB']['D']:+.3f}; top-54 turnover "
                  f"{b['turnover']['54']:.3f} -> {s['turnover']['54']:.3f}", flush=True)
        else:
            print(f"{pn} {base} seed {seed}: D {b['D']:+.3f} -> {s['D']:+.3f}; gapped 13 {b['gapped']['13']:+.3f} -> "
                  f"{s['gapped']['13']:+.3f}", flush=True)
        del Xs, ids, S
        gc.collect()


def mode_summ(pn, base, ext=False):
    x = "x" if ext else ""
    W = [json.loads(f.read_text()) for f in sorted(HERE.glob(f"r20_world{x}__{pn}__{base}__s*.json"))]
    band = lambda v: dict(mean=float(np.mean(v)), median=float(np.median(v)), q05=float(np.quantile(v, 0.05)),
                          q95=float(np.quantile(v, 0.95)), n=len(v))
    if pn == "facebook_a":
        keys = dict(D=lambda w: w["D_FB"]["D"], p_fall=lambda w: w["D_FB"]["p_fall"], p_rise=lambda w: w["D_FB"]["p_rise"],
                    **{f"turn{k}": (lambda w, k=k: w["turnover"][k]) for k in ("10", "54", "219", "875", "1750")},
                    **{f"gap{g}": (lambda w, g=g: w["gapped"][g]) for g in ("0", "4", "8")},
                    boundary=lambda w: w["boundary_pooled"], head_share=lambda w: w["head_share"])
    else:
        tk = list(W[0]["base_world"]["turnover"].keys())
        keys = dict(D=lambda w: w["D"], p_fall=lambda w: w["p_fall"], p_rise=lambda w: w["p_rise"],
                    **{f"turn{k}": (lambda w, k=k: w["turnover"][k]) for k in tk},
                    **{f"gap{g}": (lambda w, g=g: w["gapped"][g]) for g in W[0]["base_world"]["gapped"]},
                    boundary=lambda w: w["boundary_pooled"], **{f"secant_{k}": (lambda w, k=k: w["secants"][k]) for k in ("low", "mid", "high")})
    out = dict(panel=pn, base=base, n=len(W), surge=W[0]["surge"], base_world={}, surge_world={}, paired={})
    for k, f in keys.items():
        b = [f(w["base_world"]) for w in W]
        s = [f(w["surge_world"]) for w in W]
        if any(v is None for v in b + s):
            continue
        out["base_world"][k], out["surge_world"][k] = band(b), band(s)
        out["paired"][k] = band(np.array(s) - np.array(b))
    out["moments"] = dict(base=np.mean([w["base_world"]["moments"] for w in W], 0).tolist(),
                          surge=np.mean([w["surge_world"]["moments"] for w in W], 0).tolist())
    if pn != "facebook_a":
        out["curve"] = dict(base=np.mean([w["base_world"]["curve"] for w in W], 0).tolist(), surge=np.mean([w["surge_world"]["curve"] for w in W], 0).tolist())
    (HERE / f"r20_summary{x}__{pn}__{base}.json").write_text(json.dumps(out, indent=1))
    for k in out["paired"]:
        print(f"{pn} {base} {k}: base {out['base_world'][k]['mean']:+.3f}  surge {out['surge_world'][k]['mean']:+.3f} "
              f"[{out['surge_world'][k]['q05']:+.3f}, {out['surge_world'][k]['q95']:+.3f}]  paired {out['paired'][k]['mean']:+.3f}", flush=True)


if __name__ == "__main__":
    mode, pn = sys.argv[1], sys.argv[2]
    if mode == "moments":
        mode_moments(pn)
    elif mode == "calib":
        mode_calib(pn, sys.argv[3])
    elif mode == "eval":
        mode_eval(pn, sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))
    elif mode == "summ":
        mode_summ(pn, sys.argv[3])
    elif mode == "calibx":
        mode_calib(pn, sys.argv[3], ext=True)
    elif mode == "evalx":
        mode_eval(pn, sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), ext=True)
    elif mode == "summx":
        mode_summ(pn, sys.argv[3], ext=True)
    else:
        sys.exit(__doc__)
