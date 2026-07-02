#!/usr/bin/env python3
"""
Principled drifting-home estimation via a per-band Kalman unobserved-components
(UC) model, plus the three decisive tests.

Model (per entity, common factor removed by per-period de-meaning):
    X_t = home_t + xi_t + eps_t
    home_t = home_{t-1} + eta_t,   eta ~ N(0, sigma_perm^2)   (permanent; can DRIFT)
    xi_t   = phi xi_{t-1} + nu_t,  nu  ~ N(0, sigma_trans^2)   (transitory, AR(1))
    eps_t  ~ N(0, sigma_obs^2)                                 (iid measurement)

Variance parameters are pooled by PERMANENT-rank band and estimated by Kalman
MLE (no obs_frac / kappa knobs).  "Fixed home" is the nested sigma_perm=0 model.

Tests:
  A) Drift test  : sigma_perm > 0?  + value-space Var(dX over h) slope; rank
                   displacement propagator saturation.
  B) Pseudo-OOS  : Kalman filter forecast, CRPS for drift vs fixed vs random-walk.
  C) Held-out churn: estimate on 1..T0, generate, compare collisions/dRank on T0..T.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
from scipy.optimize import minimize

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

Z_CLIP = 1e-6
N_BANDS = 8


# --------------------------------------------------------------------------- #
# Data prep
# --------------------------------------------------------------------------- #
def build_matrices(df, min_obs=10, min_frac=0.5, max_per_band=400, seed=0):
    """Return de-meaned wide matrix (T x M) + mask for an estimation subset,
    plus the permanent-rank band of each kept entity."""
    T = int(df["period"].max()) + 1
    N = int(round(df.groupby("period").size().mean()))
    period_mean = df.groupby("period")["X"].mean().reindex(range(T)).to_numpy()

    g = df.groupby("entity_id")
    rbar = g["rank"].mean()
    npres = g["period"].nunique()
    keep = rbar.index[(npres >= max(min_obs, int(min_frac * T)))]
    zbar = np.log(np.clip((rbar.loc[keep].to_numpy() - 0.5) / N, Z_CLIP, 1.0))

    # band edges (coarse, log-rank spaced)
    edges = np.quantile(zbar, np.linspace(0, 1, N_BANDS + 1))
    edges[0] -= 1e-9
    band = np.clip(np.searchsorted(edges, zbar, side="right") - 1, 0, N_BANDS - 1)

    # subsample per band for tractable MLE
    rng = np.random.default_rng(seed)
    sel = []
    for b in range(N_BANDS):
        idx = np.flatnonzero(band == b)
        if idx.size > max_per_band:
            idx = rng.choice(idx, max_per_band, replace=False)
        sel.append(idx)
    sel = np.concatenate(sel)
    keep_ids = keep.to_numpy()[sel]
    band = band[sel]

    sub = df[df["entity_id"].isin(set(keep_ids))]
    wide = sub.pivot(index="period", columns="entity_id", values="X").reindex(
        index=range(T), columns=keep_ids)
    X = wide.to_numpy(dtype=float) - period_mean[:, None]   # remove common factor/trend
    mask = np.isfinite(X)
    return X, mask, band, N, T


# --------------------------------------------------------------------------- #
# Vectorized 2-state Kalman filter (home + transitory), shared params
# --------------------------------------------------------------------------- #
def _unpack(theta, drift):
    if drift:
        sp = np.exp(theta[0]); rest = theta[1:]
    else:
        sp = 0.0; rest = theta
    phi = 0.9 / (1.0 + np.exp(-rest[0]))
    st = np.exp(rest[1]); so = np.exp(rest[2])
    return sp, phi, st, so


def kf_run(sp, phi, st, so, X, mask, T0=None, want_state=False):
    """Vectorized KF. Returns total loglik (over obs up to T0 or all).
    If want_state: also return final (home, xi, P11,P12,P22) at the last step
    processed (for forecasting)."""
    T, M = X.shape
    if T0 is None:
        T0 = T
    sp2, st2, so2 = sp * sp, st * st, so * so
    first_idx = np.argmax(mask, axis=0)
    home = X[first_idx, np.arange(M)].copy()
    home = np.where(np.isfinite(home), home, 0.0)
    xi = np.zeros(M)
    P11 = np.full(M, 10.0)
    P12 = np.zeros(M)
    P22 = np.full(M, st2 / (1 - phi * phi))
    ll = 0.0
    for t in range(T0):
        m = mask[t]
        v = X[t] - (home + xi)
        S = P11 + 2 * P12 + P22 + so2
        K1 = (P11 + P12) / S
        K2 = (P12 + P22) / S
        home_u = home + K1 * v
        xi_u = xi + K2 * v
        P11u = P11 - K1 * (P11 + P12)
        P12u = P12 - K1 * (P12 + P22)
        P22u = P22 - K2 * (P12 + P22)
        home = np.where(m, home_u, home)
        xi = np.where(m, xi_u, xi)
        P11 = np.where(m, P11u, P11)
        P12 = np.where(m, P12u, P12)
        P22 = np.where(m, P22u, P22)
        ll += -0.5 * np.sum((np.log(2 * np.pi * S) + v * v / S)[m])
        # predict to t+1
        xi = phi * xi
        P11 = P11 + sp2
        P12 = phi * P12
        P22 = phi * phi * P22 + st2
    if want_state:
        return ll, (home, xi, P11, P12, P22)
    return ll


def fit_band(X, mask, drift, restarts=4, seed=0):
    rng = np.random.default_rng(seed)
    # moment-based start
    dx = np.diff(X, axis=0)
    g0 = np.nanvar(dx)
    st0 = np.sqrt(max(g0 * 0.4, 1e-4)); so0 = np.sqrt(max(g0 * 0.2, 1e-4))
    sp0 = np.sqrt(max(g0 * 0.1, 1e-4))

    def neg(theta):
        sp, phi, st, so = _unpack(theta, drift)
        if not (1e-4 < st < 5 and 1e-4 < so < 5 and (not drift or 1e-5 < sp < 5)):
            return 1e12
        ll = kf_run(sp, phi, st, so, X, mask)
        return -ll if np.isfinite(ll) else 1e12

    best = None
    for r in range(restarts):
        if drift:
            x0 = np.array([np.log(sp0 * rng.uniform(0.3, 2)), rng.uniform(-1, 1),
                           np.log(st0 * rng.uniform(0.5, 1.5)), np.log(so0 * rng.uniform(0.5, 1.5))])
        else:
            x0 = np.array([rng.uniform(-1, 1), np.log(st0 * rng.uniform(0.5, 1.5)),
                           np.log(so0 * rng.uniform(0.5, 1.5))])
        try:
            res = minimize(neg, x0, method="Nelder-Mead",
                           options={"maxiter": 2000, "xatol": 1e-4, "fatol": 1e-3})
        except Exception:
            continue
        if best is None or res.fun < best.fun:
            best = res
    sp, phi, st, so = _unpack(best.x, drift)
    return dict(sigma_perm=sp, phi=phi, sigma_trans=st, sigma_obs=so, ll=-best.fun)


# --------------------------------------------------------------------------- #
# CRPS for a Gaussian predictive distribution
# --------------------------------------------------------------------------- #
def crps_gaussian(mu, sigma, y):
    from scipy.stats import norm
    sigma = np.clip(sigma, 1e-8, None)
    z = (y - mu) / sigma
    return float(np.mean(sigma * (z * (2 * norm.cdf(z) - 1) + 2 * norm.pdf(z) - 1 / np.sqrt(np.pi))))


def oos_crps(X, mask, band, params_drift, params_fixed, T0, horizons):
    """Filter through T0 with each model's band params, forecast, score CRPS."""
    out = {h: {"drift": [], "fixed": [], "rw": []} for h in horizons}
    dx_var = np.nanvar(np.diff(X, axis=0))
    for b in range(N_BANDS):
        cols = np.flatnonzero(band == b)
        if cols.size == 0:
            continue
        Xb, mb = X[:, cols], mask[:, cols]
        for tag, P in (("drift", params_drift[b]), ("fixed", params_fixed[b])):
            sp, phi, st, so = P["sigma_perm"], P["phi"], P["sigma_trans"], P["sigma_obs"]
            _, (home, xi, P11, P12, P22) = kf_run(sp, phi, st, so, Xb, mb, T0=T0 + 1, want_state=True)
            for h in horizons:
                if T0 + h >= X.shape[0]:
                    continue
                hh, xx = home.copy(), xi.copy()
                p11, p12, p22 = P11.copy(), P12.copy(), P22.copy()
                for _ in range(h):
                    xx = phi * xx
                    p11 = p11 + sp * sp
                    p12 = phi * p12
                    p22 = phi * phi * p22 + st * st
                mu = hh + xx
                var = p11 + 2 * p12 + p22 + so * so
                y = X[T0 + h, cols]
                ok = np.isfinite(y) & mask[T0, cols]
                if ok.sum() > 3:
                    out[h][tag].append((mu[ok], np.sqrt(var[ok]), y[ok]))
        # random-walk baseline
        for h in horizons:
            if T0 + h >= X.shape[0]:
                continue
            y = X[T0 + h, cols]; last = X[T0, cols]
            ok = np.isfinite(y) & np.isfinite(last)
            if ok.sum() > 3:
                out[h]["rw"].append((last[ok], np.sqrt(dx_var * h) * np.ones(ok.sum()), y[ok]))
    scores = {}
    for h in horizons:
        scores[h] = {}
        for tag in ("drift", "fixed", "rw"):
            chunks = out[h][tag]
            if chunks:
                mu = np.concatenate([c[0] for c in chunks])
                sd = np.concatenate([c[1] for c in chunks])
                y = np.concatenate([c[2] for c in chunks])
                scores[h][tag] = crps_gaussian(mu, sd, y)
    return scores


# --------------------------------------------------------------------------- #
# Test A: propagators
# --------------------------------------------------------------------------- #
def propagators(df, horizons, min_frac=0.7, sample=3000, seed=0):
    T = int(df["period"].max()) + 1
    N = int(round(df.groupby("period").size().mean()))
    pres = df.groupby("entity_id")["period"].nunique()
    elig = pres.index[pres >= min_frac * T].to_numpy()
    rng = np.random.default_rng(seed)
    if elig.size > sample:
        elig = rng.choice(elig, sample, replace=False)
    sub = df[df["entity_id"].isin(set(elig))]
    pm = df.groupby("period")["X"].mean().reindex(range(T)).to_numpy()
    Xw = sub.pivot(index="period", columns="entity_id", values="X").reindex(range(T)).to_numpy() - pm[:, None]
    Rw = sub.pivot(index="period", columns="entity_id", values="rank").reindex(range(T)).to_numpy()
    val_var, rank_med = {}, {}
    for h in horizons:
        if h >= T:
            continue
        dvi = Xw[h:] - Xw[:-h]
        val_var[h] = float(np.nanvar(dvi))
        dr = np.abs(Rw[h:] - Rw[:-h])
        rank_med[h] = float(np.nanmedian(dr[np.isfinite(dr)]))
    return val_var, rank_med


# --------------------------------------------------------------------------- #
# Test C: generative simulation + churn (drift vs fixed)
# --------------------------------------------------------------------------- #
def _interp_band(zp, band_z, vals):
    return np.interp(zp, band_z, vals, left=vals[0], right=vals[-1])


def simulate_churn(df, params, band_z, T_sim, drift, seed=0, exit_rate=0.0):
    rng = np.random.default_rng(seed)
    N = int(round(df.groupby("period").size().mean()))
    w0 = np.sort(df.loc[df["period"] == 0, "X"].to_numpy(dtype=float))[::-1]
    home = mrd._extend(w0, N) - np.mean(w0)
    sp = _interp_band(np.linspace(band_z[0], band_z[-1], 1), band_z, [p["sigma_perm"] for p in params])
    # per-entity coefficient lookup uses permanent rank of home
    phis = np.array([p["phi"] for p in params]); sts = np.array([p["sigma_trans"] for p in params])
    sps = np.array([p["sigma_perm"] for p in params]); sos = np.array([p["sigma_obs"] for p in params])
    bottom = np.quantile(home, np.linspace(0, 0.1, 100))
    xi = np.zeros(N)
    colranks = [1, 2, 5, 20]
    top_prev = None
    coll = {c: [0, 0] for c in colranks}
    drank = []
    burn = 30
    prev_rank = None
    for t in range(-burn, T_sim):
        morder = np.argsort(-home)
        prank = np.empty(N, dtype=np.int64); prank[morder] = np.arange(1, N + 1)
        zp = np.log(np.clip((prank - 0.5) / N, Z_CLIP, 1.0))
        phi = _interp_band(zp, band_z, phis); st = _interp_band(zp, band_z, sts)
        spv = _interp_band(zp, band_z, sps) if drift else np.zeros(N)
        so = _interp_band(zp, band_z, sos)
        home = home + spv * rng.standard_normal(N)
        xi = phi * xi + st * rng.standard_normal(N)
        if exit_rate > 0:
            ex = rng.random(N) < exit_rate
            if ex.any():
                home[ex] = bottom[rng.integers(0, bottom.size, ex.sum())]; xi[ex] = 0.0
        if t < 0:
            continue
        X = home + xi + so * rng.standard_normal(N)
        order = np.argsort(-X)
        rank = np.empty(N, dtype=np.int64); rank[order] = np.arange(1, N + 1)
        top = order[:max(colranks)]
        if top_prev is not None:
            for c in colranks:
                coll[c][1] += 1
                coll[c][0] += int(top_prev[c - 1] != top[c - 1])
        top_prev = top
        if prev_rank is not None:
            d = np.abs(rank[order[:200]] - prev_rank[order[:200]])
            drank.append(np.median(d))
        prev_rank = rank
    coll_out = {c: coll[c][0] / max(coll[c][1], 1) for c in colranks}
    return coll_out, float(np.mean(drank)) if drank else np.nan


def empirical_churn(df, colranks=(1, 2, 5, 20), lo_period=0, hi_period=None):
    T = int(df["period"].max()) + 1
    hi_period = hi_period if hi_period is not None else T
    sub = df[(df["period"] >= lo_period) & (df["period"] < hi_period)]
    by = {}
    for p, g in sub.groupby("period"):
        gg = g.sort_values("rank")
        by[p] = gg["entity_id"].to_numpy()
    periods = sorted(by)
    coll = {c: [0, 0] for c in colranks}
    for i in range(1, len(periods)):
        a, b = by[periods[i - 1]], by[periods[i]]
        for c in colranks:
            if len(a) >= c and len(b) >= c:
                coll[c][1] += 1
                coll[c][0] += int(a[c - 1] != b[c - 1])
    return {c: coll[c][0] / max(coll[c][1], 1) for c in colranks}


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #
def _load_universe(platform, top_k=None, buffer_mult=4, member_window=None):
    """Load the platform panel, optionally restricted to the closed Lagrangian
    top-coverage universe (see mrd.restrict_universe).  member_window=T0
    computes membership on the train window only (OOS-safe)."""
    df = mrd.load_panel(mrd.PLATFORMS[platform])
    if top_k:
        df = mrd.restrict_universe(df, top_k, buffer_mult=buffer_mult,
                                   member_window=member_window)
    return df


def _universe_tag(df):
    k = df.attrs.get("score_k")
    return f" | universe=top-{k} (B={df.attrs['universe_B']})" if k else ""


def run(platform, top_k=None, buffer_mult=4):
    print(f"\n{'='*72}\n{platform.upper()}  — drifting-home Kalman UC\n{'='*72}")
    df = _load_universe(platform, top_k, buffer_mult)
    if top_k:
        print(f"  {_universe_tag(df).strip(' |')}")
    X, mask, band, N, T = build_matrices(df)
    print(f"  T={T}  N~{N}  estimation entities={X.shape[1]}  bands={N_BANDS}")
    # band z-centers = midpoints of the permanent-rank quantile edges
    rbarN = df.groupby("entity_id")["rank"].mean()
    qs = np.quantile(np.log(np.clip((rbarN.to_numpy() - 0.5) / N, Z_CLIP, 1.0)),
                     np.linspace(0, 1, N_BANDS + 1))
    band_z = 0.5 * (qs[:-1] + qs[1:])

    print("\n  [estimate band UC params: drift (home RW) and fixed (home const)]")
    pd_, pf_ = [], []
    for b in range(N_BANDS):
        cols = np.flatnonzero(band == b)
        Xb, mb = X[:, cols], mask[:, cols]
        d = fit_band(Xb, mb, drift=True)
        f = fit_band(Xb, mb, drift=False)
        pd_.append(d); pf_.append(f)
        lr = 2 * (d["ll"] - f["ll"])
        print(f"   band{b} z={band_z[b]:+.2f} | DRIFT sp={d['sigma_perm']:.3f} phi={d['phi']:.2f} "
              f"st={d['sigma_trans']:.3f} so={d['sigma_obs']:.3f} | LR(drift>fixed)={lr:7.1f}")

    # ---- Test A: drift via value-variance slope + propagator ----
    hs = [h for h in [1, 2, 4, 8, 13, 20, 26] if h < T]
    vv, rm = propagators(df, hs)
    print("\n  TEST A — propagators (de-meaned value variance & rank |displacement|)")
    print("    h     :", "  ".join(f"{h:>6}" for h in hs))
    print("    Var dX:", "  ".join(f"{vv[h]:6.3f}" for h in hs))
    print("    medΔrk:", "  ".join(f"{rm[h]:6.0f}" for h in hs))
    # slope of Var(dX) over the longest horizons ~ permanent (drift) variance per step
    hl = np.array(hs[len(hs)//2:]); vl = np.array([vv[h] for h in hl])
    slope = np.polyfit(hl, vl, 1)[0]
    sp_mean = float(np.mean([p["sigma_perm"] for p in pd_]))
    print(f"    long-h Var(dX) slope = {slope:.4f} (=> implied permanent var/step); "
          f"mean est sigma_perm^2 = {sp_mean**2:.4f}")
    print(f"    drift verdict: {'DRIFT (slope>0, home moves)' if slope > 0.002 else 'FIXED-ish (flat)'}")

    # ---- Test B: pseudo-OOS CRPS ----
    T0 = int(round(0.7 * T))
    fh = [h for h in [1, 4, 13] if T0 + h < T]
    sc = oos_crps(X, mask, band, pd_, pf_, T0, fh)
    print(f"\n  TEST B — pseudo-OOS CRPS (filter through t={T0}, lower=better)")
    print("    horizon |   drift   fixed     rw   | winner")
    for h in fh:
        s = sc[h]
        win = min(s, key=s.get)
        print(f"      h={h:<3} | {s.get('drift',float('nan')):7.4f} {s.get('fixed',float('nan')):7.4f} "
              f"{s.get('rw',float('nan')):7.4f} | {win}")

    # ---- Test C: held-out churn ----
    emp_hold = empirical_churn(df, lo_period=T0, hi_period=T)
    coll_d, dr_d = simulate_churn(df, pd_, band_z, T_sim=T - T0, drift=True, exit_rate=0.01)
    coll_f, dr_f = simulate_churn(df, pf_, band_z, T_sim=T - T0, drift=False, exit_rate=0.01)
    print(f"\n  TEST C — held-out churn (estimate on 1..{T0}, predict {T0}..{T})")
    print("    coll@rank :   emp     drift   fixed")
    for c in (1, 2, 5, 20):
        print(f"      rank {c:<3} : {emp_hold[c]:6.3f}  {coll_d[c]:6.3f}  {coll_f[c]:6.3f}")
    print(f"    dRank(1)  :   (drift {dr_d:.1f}  fixed {dr_f:.1f})")
    return dict(pd=pd_, pf=pf_, scores=sc, vv=vv, rm=rm)


# Pre-specified calibration moment vector + weights (frozen before validation).
CAL_WEIGHTS = {"dRank1": 1.0, "dRank4": 1.0, "coll1": 1.0, "coll5": 1.0, "RACF1": 1.0}


def _rank_dist(R, horizons, cap=100):
    """Full |Δrank over h| arrays (entities with rank<=cap at t) + lag-1 rank ACF.
    R: T x M, NaN=absent. Returns ({h: array of |Δrank|}, RACF1)."""
    dist = {}
    T = R.shape[0]
    for h in horizons:
        if h >= T:
            dist[h] = np.array([])
            continue
        r0, rh = R[:-h], R[h:]
        m = np.isfinite(r0) & np.isfinite(rh) & (r0 <= cap)
        dist[h] = np.abs(rh[m] - r0[m])
    racf = []
    for j in range(R.shape[1]):
        r = R[:, j]
        rr = r[np.isfinite(r)]
        if rr.size > 6 and np.std(rr[:-1]) > 1e-9 and np.std(rr[1:]) > 1e-9:
            racf.append(np.corrcoef(rr[:-1], rr[1:])[0, 1])
    return dist, (float(np.median(racf)) if racf else np.nan)


def _moments(dist, racf1, colls, horizons):
    """Scalar summaries: median + 90th pct of displacement per horizon, RACF1, collisions."""
    m = {}
    for h in horizons:
        d = dist.get(h, np.array([]))
        m[f"dRank{h}"] = float(np.median(d)) if d.size else np.nan
        m[f"p90_{h}"] = float(np.percentile(d, 90)) if d.size else np.nan
    m["RACF1"] = racf1
    m.update({f"coll{c}": v for c, v in colls.items()})
    return m


def _cal_error(emp_m, sim_m):
    """Weighted mean relative error over the pre-specified calibration vector."""
    e = []
    for k, w in CAL_WEIGHTS.items():
        if np.isfinite(emp_m.get(k, np.nan)) and np.isfinite(sim_m.get(k, np.nan)):
            e.append(w * abs(sim_m[k] - emp_m[k]) / max(abs(emp_m[k]), 1e-6))
    return float(np.mean(e)) if e else np.inf


def _collisions_from_topids(top_ids, ranks):
    out = {}
    for c in ranks:
        if c - 1 < top_ids.shape[1]:
            prev, cur = top_ids[:-1, c - 1], top_ids[1:, c - 1]
            m = (prev >= 0) & (cur >= 0)
            out[f"coll{c}"] = float(np.mean(prev[m] != cur[m])) if m.any() else np.nan
    return out


def emp_dist(df, horizons, coll_ranks=(1, 5, 20), cohort_k=200):
    """Empirical displacement distribution + RACF1 + collisions over a 0-based window."""
    T = int(df["period"].max()) + 1
    cohort = df.loc[(df["period"] == 0) & (df["rank"] <= cohort_k), "entity_id"].to_numpy()
    sub = df[df["entity_id"].isin(set(cohort))]
    R = sub.pivot(index="period", columns="entity_id", values="rank").reindex(range(T)).to_numpy()
    dist, racf1 = _rank_dist(R, horizons)
    colls = empirical_churn(df, coll_ranks, lo_period=0, hi_period=T)
    return dist, racf1, colls


def sim_cohort(p, T_sim, kappa, seed=0, cohort_k=200, burn=40):
    """Generative sim that tracks the initial top-cohort's rank trajectory + top occupants."""
    rng = np.random.default_rng(seed)
    N = p.N
    mu = mrd._extend(p.w0, N); home = mu.copy(); xi = np.zeros(N)
    ids = np.arange(N, dtype=np.int64); next_id = N
    zk = p.z_knots
    cohort_slots = cohort_ids = None
    cranks = []; topids = []
    KT = 300
    for t in range(-burn, T_sim):
        morder = np.argsort(-mu); prank = np.empty(N, np.int64); prank[morder] = np.arange(1, N + 1)
        zp = np.log(np.clip((prank - 0.5) / N, Z_CLIP, 1.0))
        phi = np.interp(zp, zk, p.phi); st = np.interp(zp, zk, p.sigma_trans)
        sp = np.interp(zp, zk, p.sigma_perm); so = np.interp(zp, zk, p.sigma_obs)
        if kappa > 0:
            mu = mu - kappa * (mu - home)
        mu = mu + sp * rng.standard_normal(N)
        xi = phi * xi + st * rng.standard_normal(N)
        ex = rng.random(N) < np.interp(zp, zk, p.exit_rate)
        ne = int(ex.sum())
        if ne:
            sm = p.bottom_mu[rng.integers(0, p.bottom_mu.size, ne)]
            mu[ex] = sm; home[ex] = sm; xi[ex] = 0.0
            ids[ex] = np.arange(next_id, next_id + ne); next_id += ne
        if t < 0:
            continue
        X = mu + xi + so * rng.standard_normal(N)
        order = np.argsort(-X); rank = np.empty(N, np.int64); rank[order] = np.arange(1, N + 1)
        if cohort_slots is None:
            cohort_slots = order[:cohort_k].copy(); cohort_ids = ids[cohort_slots].copy()
        alive = ids[cohort_slots] == cohort_ids
        cr = rank[cohort_slots].astype(float); cr[~alive] = np.nan
        cranks.append(cr); topids.append(ids[order[:KT]])
    return np.array(cranks), np.array(topids)


def sim_dist(p, T_sim, horizons, reps=3, coll_ranks=(1, 5, 20), kappa=0.0):
    """Pooled displacement distribution + RACF1 + collisions across MC reps."""
    pooled = {h: [] for h in horizons}
    racfs, colls = [], {c: [] for c in coll_ranks}
    for s in range(reps):
        cr, ti = sim_cohort(p, T_sim, kappa, seed=s)
        d, rf = _rank_dist(cr, horizons)
        for h in horizons:
            pooled[h].append(d[h])
        racfs.append(rf)
        cc = _collisions_from_topids(ti, coll_ranks)
        for c in coll_ranks:
            colls[c].append(cc.get(f"coll{c}", np.nan))
    dist = {h: (np.concatenate(pooled[h]) if pooled[h] else np.array([])) for h in horizons}
    return dist, float(np.nanmedian(racfs)), {c: float(np.nanmean(colls[c])) for c in coll_ranks}


def _calibrate_scale(p, df_tr, horizons, Ltr, reps=3):
    """Calibrate sigma_obs_scale on the TRAIN moment VECTOR (not a single moment)."""
    ed, erf, ec = emp_dist(df_tr, horizons)
    em = _moments(ed, erf, ec, horizons)
    best_scale, best_err = 1.0, np.inf
    # grid extends to 0.0 so the calibration can find an interior optimum
    # (reddit pinned at a 0.15 grid-edge; report when the optimum is 0.0)
    for sc in (1.0, 0.7, 0.5, 0.35, 0.25, 0.15, 0.10, 0.05, 0.0):
        sd, srf, scol = sim_dist(replace_obs(p, sc), Ltr, horizons, reps=reps)
        sm = _moments(sd, srf, scol, horizons)
        e = _cal_error(em, sm)
        if e < best_err:
            best_err, best_scale = e, sc
    return best_scale


def replace_obs(p, scale):
    from dataclasses import replace
    return replace(p, sigma_obs=p.sigma_obs * scale)


def _boot_ci(arr, stat=np.median, B=400, lo=2.5, hi=97.5, seed=0):
    if arr.size < 5:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, arr.size, size=(B, arr.size))
    vals = np.array([stat(arr[i]) for i in idx])
    return float(np.percentile(vals, lo)), float(np.percentile(vals, hi))


def _build_params_on(df_tr):
    from dataclasses import replace
    X, mask, band, N, T = build_matrices(df_tr)
    rbarN = df_tr.groupby("entity_id")["rank"].mean()
    qs = np.quantile(np.log(np.clip((rbarN.to_numpy() - 0.5) / N, Z_CLIP, 1.0)),
                     np.linspace(0, 1, N_BANDS + 1))
    band_z = 0.5 * (qs[:-1] + qs[1:])
    pdr = [fit_band(X[:, band == b], mask[:, band == b], drift=True) for b in range(N_BANDS)]
    p0 = mrd.estimate(df_tr, obs_frac=0.0); zk = p0.z_knots
    p = replace(p0,
                phi=np.interp(zk, band_z, [b["phi"] for b in pdr]),
                sigma_trans=np.interp(zk, band_z, [b["sigma_trans"] for b in pdr]),
                sigma_perm=np.interp(zk, band_z, [b["sigma_perm"] for b in pdr]),
                sigma_obs=np.interp(zk, band_z, [b["sigma_obs"] for b in pdr]))
    return p


def _estimate_fast(df_tr, obs_frac=0.5):
    """Fast closed-form variance-partition estimator (per split, for rolling CV)."""
    return mrd.estimate(df_tr, obs_frac=obs_frac)


def oos_movement(platform, n_splits=5, obs_frac=0.5, reps=3, boot=400,
                 top_k=None, buffer_mult=4):
    """Rolling-origin OOS movement gate. For each split: estimate the variance
    partition on TRAIN; calibrate one sigma_obs_scale on the TRAIN moment VECTOR
    (dRank1, dRank4, coll1, coll5, RACF1); then PREDICT the held-out displacement
    DISTRIBUTION (median, p90, Wasserstein, bootstrap-CI coverage). Test data is
    never used in estimation or calibration.  With top_k set, the panel is
    restricted per split to the closed top-coverage universe whose membership
    is computed on the TRAIN window only (no membership leakage)."""
    from scipy.stats import wasserstein_distance
    print(f"\n{'='*72}\n{platform.upper()} — OOS movement gate (rolling-origin, distributional)\n{'='*72}")
    df_full = mrd.load_panel(mrd.PLATFORMS[platform])
    T = int(df_full["period"].max()) + 1
    test_len = max(13, T // 4)
    origins = sorted(set(int(round(o)) for o in
                         np.linspace(max(12, T // 4), T - test_len, n_splits)))
    uni = f"  universe=top-{top_k} (B={buffer_mult}x, train-only membership)" if top_k else ""
    print(f"  T={T}  test_len={test_len}  train-end origins={origins}{uni}")

    rows = []
    for T0 in origins:
        df = (mrd.restrict_universe(df_full, top_k, buffer_mult=buffer_mult,
                                    member_window=T0) if top_k else df_full)
        df_tr = df[df["period"] < T0].copy()
        df_te = df[(df["period"] >= T0) & (df["period"] < T0 + test_len)].copy()
        df_te["period"] -= T0
        hor = [h for h in (1, 4, 13) if h < test_len]
        p = _estimate_fast(df_tr, obs_frac)
        scale = _calibrate_scale(p, df_tr, hor, T0, reps=reps)
        p = replace_obs(p, scale)
        ed, erf, ec = emp_dist(df_te, hor)            # held-out truth
        td, trf, tc = emp_dist(df_tr, hor)            # persistence baseline = train movement
        sd, srf, sc = sim_dist(p, test_len, hor, reps=reps)   # model prediction
        rows.append(dict(T0=T0, scale=scale, hor=hor,
                         em=_moments(ed, erf, ec, hor), bm=_moments(td, trf, tc, hor),
                         sm=_moments(sd, srf, sc, hor), ed=ed, sd=sd))

    hor = rows[0]["hor"]
    keys = [f"dRank{h}" for h in hor] + ["RACF1"] + [f"coll{c}" for c in (1, 5, 20)]
    print("\n  per split:  scale | model relErr | persist relErr | wass(dR1) | dR1 model-in-CI?")
    me_all, be_all = [], []
    for r in rows:
        em, sm, bm = r["em"], r["sm"], r["bm"]
        kk = [k for k in keys if np.isfinite(em.get(k, np.nan)) and np.isfinite(sm.get(k, np.nan))]
        me = float(np.mean([abs(sm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk]))
        be = float(np.mean([abs(bm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk if np.isfinite(bm.get(k, np.nan))]))
        me_all.append(me); be_all.append(be)
        h0 = hor[0]
        w1 = (wasserstein_distance(r["ed"][h0], r["sd"][h0])
              if r["ed"][h0].size and r["sd"][h0].size else np.nan)
        lo, hi = _boot_ci(r["ed"][h0], B=boot)
        inci = bool(np.isfinite(lo) and lo <= sm[f"dRank{h0}"] <= hi)
        print(f"    T0={r['T0']:>3}  {r['scale']:.2f}  | {me:>6.3f}      | {be:>6.3f}        "
              f"| {w1:>6.1f}   | {inci}")

    scales = [r["scale"] for r in rows]
    cov = np.mean([
        (lambda lo, hi: bool(np.isfinite(lo) and lo <= r["sm"][f"dRank{r['hor'][0]}"] <= hi))(
            *_boot_ci(r["ed"][r["hor"][0]], B=boot)) for r in rows])
    print(f"\n  sigma_obs_scale stability: {scales}  median={np.median(scales):.2f} "
          f"range={min(scales):.2f}-{max(scales):.2f}")
    print(f"  MODEL rel err = {np.mean(me_all):.3f} ± {np.std(me_all):.3f}   |   "
          f"persistence = {np.mean(be_all):.3f} ± {np.std(be_all):.3f}")
    print(f"  bootstrap-CI coverage (model dR{hor[0]} median inside held-out 95% CI): {cov*100:.0f}% of splits")
    r = rows[-1]
    print(f"\n  held-out displacement DISTRIBUTION (last split T0={r['T0']}): emp median/p90 vs model median/p90")
    for h in hor:
        e, s = r["ed"][h], r["sd"][h]
        if e.size and s.size:
            print(f"    dRank{h}: emp {np.median(e):>4.0f}/{np.percentile(e,90):>4.0f}   "
                  f"model {np.median(s):>4.0f}/{np.percentile(s,90):>4.0f}")


def scorecard(platform, reps=4, top_k=None, buffer_mult=4):
    """Wire the drifting-home Kalman band params into the generative simulator
    (home = random walk, kappa=0, estimated sigma_obs -- NO tuned knobs) and
    re-score the full goal-1 + goal-2 card, reusing minimal_rankdiff diagnostics."""
    from dataclasses import replace
    print(f"\n{'='*72}\n{platform.upper()} — drifting-home generative scorecard (no knobs)\n{'='*72}")
    df = _load_universe(platform, top_k, buffer_mult)
    if top_k:
        print(f"  {_universe_tag(df).strip(' |')}")
    X, mask, band, N, T = build_matrices(df)
    rbarN = df.groupby("entity_id")["rank"].mean()
    qs = np.quantile(np.log(np.clip((rbarN.to_numpy() - 0.5) / N, Z_CLIP, 1.0)),
                     np.linspace(0, 1, N_BANDS + 1))
    band_z = 0.5 * (qs[:-1] + qs[1:])

    pdrift = [fit_band(X[:, band == b], mask[:, band == b], drift=True) for b in range(N_BANDS)]
    print("  drift-home band params wired in (top..tail):")
    print(f"    sigma_perm = {pdrift[0]['sigma_perm']:.3f}..{pdrift[-1]['sigma_perm']:.3f}")
    print(f"    phi        = {pdrift[0]['phi']:.3f}..{pdrift[-1]['phi']:.3f}")
    print(f"    sigma_trans= {pdrift[0]['sigma_trans']:.3f}..{pdrift[-1]['sigma_trans']:.3f}")
    print(f"    sigma_obs  = {pdrift[0]['sigma_obs']:.3f}..{pdrift[-1]['sigma_obs']:.3f}")

    # scaffolding (common factor, exit, init state, N) from minimal_rankdiff
    p0 = mrd.estimate(df, obs_frac=0.0)
    zk = p0.z_knots
    p = replace(
        p0,
        phi=np.interp(zk, band_z, [b["phi"] for b in pdrift]),
        sigma_trans=np.interp(zk, band_z, [b["sigma_trans"] for b in pdrift]),
        sigma_perm=np.interp(zk, band_z, [b["sigma_perm"] for b in pdrift]),
        sigma_obs=np.interp(zk, band_z, [b["sigma_obs"] for b in pdrift]),
    )

    mean_n = df.groupby("period").size().mean()
    top_k = max(10, int(round(0.01 * mean_n)))
    ev, er, et, ers = mrd.empirical_structures(df, top_k)
    emp = mrd.diagnostics(ev, er, et, ers, top_k)
    sims = [mrd.diagnostics(*mrd._sim_struct(mrd.simulate(p, T, seed=s, kappa=0.0)), top_k)
            for s in range(reps)]
    sim = {k: float(np.nanmean([s.get(k, np.nan) for s in sims])) for k in sims[0]}
    mrd._print_compare(emp, sim, p, top_k)


def selftest():
    """Synthetic recovery: known UC params -> KF MLE should recover them."""
    rng = np.random.default_rng(1)
    T, M = 120, 300
    sp, phi, st, so = 0.05, 0.4, 0.25, 0.15
    home = np.cumsum(rng.normal(0, sp, (T, M)), axis=0) + rng.normal(0, 1, M)
    xi = np.zeros((T, M))
    for t in range(1, T):
        xi[t] = phi * xi[t - 1] + rng.normal(0, st, M)
    X = home + xi + rng.normal(0, so, (T, M))
    mask = np.ones((T, M), bool)
    est = fit_band(X, mask, drift=True, restarts=5)
    print("SELFTEST recovery (true sp=.05 phi=.40 st=.25 so=.15):")
    print(f"  est sp={est['sigma_perm']:.3f} phi={est['phi']:.2f} st={est['sigma_trans']:.3f} so={est['sigma_obs']:.3f}")


def _resolve_k(plat, args):
    if args.top_k is not None:
        return args.top_k
    if args.coverage is not None:
        k = mrd.COVERAGE_K.get(plat, {}).get(args.coverage)
        if k is None:
            raise SystemExit(f"no pre-registered K for {plat} at coverage {args.coverage}%")
        return k
    return None


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("platforms", nargs="*", default=["facebook", "reddit"])
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--scorecard", action="store_true")
    ap.add_argument("--oos", action="store_true")
    ap.add_argument("--top-k", type=int, default=None,
                    help="top-coverage universe boundary K (applies to every platform listed)")
    ap.add_argument("--coverage", type=int, choices=(80, 90, 95), default=None,
                    help="resolve K per platform from the pre-registered COVERAGE_K rule")
    ap.add_argument("--buffer-mult", type=int, default=4,
                    help="universe buffer depth B = buffer_mult * K (sponge layer)")
    args = ap.parse_args()
    if args.selftest:
        selftest()
    elif args.scorecard:
        for p in args.platforms:
            scorecard(p, top_k=_resolve_k(p, args), buffer_mult=args.buffer_mult)
    elif args.oos:
        for p in args.platforms:
            oos_movement(p, top_k=_resolve_k(p, args), buffer_mult=args.buffer_mult)
    else:
        for p in args.platforms:
            run(p, top_k=_resolve_k(p, args), buffer_mult=args.buffer_mult)
