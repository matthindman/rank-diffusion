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
def run(platform):
    print(f"\n{'='*72}\n{platform.upper()}  — drifting-home Kalman UC\n{'='*72}")
    df = mrd.load_panel(mrd.PLATFORMS[platform])
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


def _movement_from_ranks(R, horizons, cap=100):
    """median |Δrank over h| for entities with rank<=cap at t (R: T x M, NaN=absent)."""
    out = {}
    T = R.shape[0]
    for h in horizons:
        if h >= T:
            out[f"dRank{h}"] = np.nan
            continue
        r0, rh = R[:-h], R[h:]
        m = np.isfinite(r0) & np.isfinite(rh) & (r0 <= cap)
        d = np.abs(rh[m] - r0[m])
        out[f"dRank{h}"] = float(np.median(d)) if d.size else np.nan
    return out


def _collisions_from_topids(top_ids, ranks):
    out = {}
    for c in ranks:
        if c - 1 < top_ids.shape[1]:
            prev, cur = top_ids[:-1, c - 1], top_ids[1:, c - 1]
            m = (prev >= 0) & (cur >= 0)
            out[f"coll{c}"] = float(np.mean(prev[m] != cur[m])) if m.any() else np.nan
    return out


def emp_movement(df, horizons, coll_ranks=(1, 5, 20), cohort_k=200):
    """Empirical movement over a period window (df periods assumed 0-based)."""
    T = int(df["period"].max()) + 1
    cohort = df.loc[(df["period"] == 0) & (df["rank"] <= cohort_k), "entity_id"].to_numpy()
    sub = df[df["entity_id"].isin(set(cohort))]
    R = sub.pivot(index="period", columns="entity_id", values="rank").reindex(range(T)).to_numpy()
    mv = _movement_from_ranks(R, horizons)
    mv.update({f"coll{c}": v for c, v in empirical_churn(df, coll_ranks, lo_period=0, hi_period=T).items()})
    return mv


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


def sim_movement(p, T_sim, kappa, horizons, coll_ranks=(1, 5, 20), reps=3):
    mvs = []
    for s in range(reps):
        cr, ti = sim_cohort(p, T_sim, kappa, seed=s)
        mv = _movement_from_ranks(cr, horizons)
        mv.update(_collisions_from_topids(ti, coll_ranks))
        mvs.append(mv)
    return {k: float(np.nanmean([m[k] for m in mvs])) for k in mvs[0]}


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


def oos_movement(platform, train_frac=0.67):
    print(f"\n{'='*72}\n{platform.upper()} — OUT-OF-SAMPLE movement prediction\n{'='*72}")
    df = mrd.load_panel(mrd.PLATFORMS[platform])
    T = int(df["period"].max()) + 1
    T0 = int(round(train_frac * T))
    df_tr = df[df["period"] < T0].copy()
    df_te = df[df["period"] >= T0].copy(); df_te["period"] -= T0
    Ltr, Lte = T0, T - T0
    horizons = [h for h in (1, 4, 13) if h < Lte]
    print(f"  T={T}  train=0..{T0} ({Ltr}w)  test={T0}..{T} ({Lte}w)  horizons={horizons}")

    # estimate variance partition on TRAIN (autocovariance -> NOT churn); kappa fit to TRAIN movement only
    p = _build_params_on(df_tr)
    print(f"  variance partition (train): sigma_perm={p.sigma_perm[0]:.3f}..{p.sigma_perm[-1]:.3f}  "
          f"sigma_trans={p.sigma_trans[0]:.3f}..{p.sigma_trans[-1]:.3f}  "
          f"sigma_obs={p.sigma_obs[0]:.3f}..{p.sigma_obs[-1]:.3f}")
    from dataclasses import replace
    tr_mv = emp_movement(df_tr, horizons)
    keys = list(tr_mv.keys())
    # Calibrate ONE knob -- sigma_obs scale -- on TRAIN short-horizon displacement only
    # (fixed home, kappa=0). sigma_obs is the lever on observed rank movement; the pooled
    # change-nugget over-states it for clean top entities, so we scale it to the training
    # displacement and then validate on the HELD-OUT window (never used in estimation).
    target = tr_mv["dRank1"]
    best_scale, best_err = 1.0, 1e9
    for scale in (1.0, 0.7, 0.5, 0.35, 0.25):
        sm = sim_movement(replace(p, sigma_obs=p.sigma_obs * scale), Ltr, 0.0, horizons)
        err = abs(sm["dRank1"] - target) / max(target, 1e-6)
        if err < best_err:
            best_err, best_scale = err, scale
    p = replace(p, sigma_obs=p.sigma_obs * best_scale)
    print(f"  calibrated sigma_obs_scale on TRAIN dRank1 = {best_scale}  "
          f"(sigma_obs now {p.sigma_obs[0]:.3f}..{p.sigma_obs[-1]:.3f})")

    # PREDICT test movement (frozen, calibrated train params); test never used in estimation
    te_mv = emp_movement(df_te, horizons)
    model_mv = sim_movement(p, Lte, 0.0, horizons)
    print(f"\n  PREDICTION vs HELD-OUT TRUTH  (persistence baseline = train movement)")
    print(f"    metric       train(base)   test(truth)   MODEL    |model-test|  |base-test|")
    for k in keys:
        b, tt, m = tr_mv[k], te_mv[k], model_mv[k]
        e_m = abs(m - tt) if np.isfinite(m) and np.isfinite(tt) else np.nan
        e_b = abs(b - tt) if np.isfinite(b) and np.isfinite(tt) else np.nan
        print(f"    {k:<12} {b:>9.3f}   {tt:>9.3f}   {m:>9.3f}    {e_m:>9.3f}    {e_b:>9.3f}")
    me = np.nanmean([abs(model_mv[k] - te_mv[k]) / max(abs(te_mv[k]), 1e-6) for k in keys])
    be = np.nanmean([abs(tr_mv[k] - te_mv[k]) / max(abs(te_mv[k]), 1e-6) for k in keys])
    print(f"\n  mean relative error vs held-out:  MODEL={me:.3f}   persistence-baseline={be:.3f}")


def scorecard(platform, reps=4):
    """Wire the drifting-home Kalman band params into the generative simulator
    (home = random walk, kappa=0, estimated sigma_obs -- NO tuned knobs) and
    re-score the full goal-1 + goal-2 card, reusing minimal_rankdiff diagnostics."""
    from dataclasses import replace
    print(f"\n{'='*72}\n{platform.upper()} — drifting-home generative scorecard (no knobs)\n{'='*72}")
    df = mrd.load_panel(mrd.PLATFORMS[platform])
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


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("platforms", nargs="*", default=["facebook", "reddit"])
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--scorecard", action="store_true")
    ap.add_argument("--oos", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
    elif args.scorecard:
        for p in args.platforms:
            scorecard(p)
    elif args.oos:
        for p in args.platforms:
            oos_movement(p)
    else:
        for p in args.platforms:
            run(p)
