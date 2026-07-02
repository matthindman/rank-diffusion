#!/usr/bin/env python3
"""
Minimal rank-based diffusion (Atlas / Gabaix) prototype.

Theory
------
A stable rank-size distribution that churns at the entity level is the signature
of a rank-based interacting diffusion (Banner-Fernholz-Karatzas Atlas models;
Gabaix random-growth-with-a-barrier).  Each entity i carries a log-activity X_i
whose one-step dynamics depend only on its CURRENT rank:

    X_i(t+1) = X_i(t) + b(z_i) + lambda(z_i) * F_t + sigma(z_i) * eps_i

    z   = log((r - 0.5) / N)         rank coordinate (same as the v4.3 core)
    F_t ~ N(0, sigma_F)              one common (market-wide) factor
    eps ~ N(0, 1)                    idiosyncratic innovation
    + Gabaix rebirth boundary:       rank-dependent exit -> reseed near the bottom

`b(z)` is the centripetal (mean-reverting) drift that holds the rank-size curve
stationary; `sigma(z)` is the idiosyncratic diffusion that drives rank crossings;
`lambda(z) F_t` is the shared move that preserves proportional shares.  No heavy
tails, no ARCH, no jumps, no separate transitory AR(1), no kappa-stab grid: the
mean-reversion (low variance ratio) is meant to emerge from b(z) via rank, and
the heavy aggregate kurtosis is meant to emerge from rank composition.

Estimation is ONE pass, no optimization:

    b(z)      = E[dX | z]
    F_t       = mean_i (dX_i - b(z_i))
    lambda(z) = slope of (dX - b) on F within the z-bin
    sigma(z)  = std(dX - b - lambda F | z)
    exit(z)   = P(present at t, absent at t+1 | z)
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# Platform configs
# --------------------------------------------------------------------------- #
PLATFORMS = {
    "facebook": dict(path="data/raw/fb_ranked_weekly_cutdown.parquet",
                     id_col="endpoint_id", ts_col="date", metric_col="metric_value",
                     max_rank=None),
    "reddit": dict(path="data/reddit/reddit_weekly.parquet",
                   id_col="endpoint_id", ts_col="date", metric_col="metric_value",
                   max_rank=None),
    "instagram": dict(path="llm_fitting/ig_weekly_ranked_top50k.parquet",
                      id_col="user_name", ts_col="date", metric_col="metric_value",
                      max_rank=20000),
}

COLLISION_RANKS = [1, 2, 5, 10, 20, 50, 100]
DRANK_HORIZONS = [1, 4, 13]
VR_LAGS = [2, 4, 8, 13]
ACF_LAGS = [1, 2]
RACF_LAGS = [1, 4, 13]
HORIZONS = [1, 4, 13]
Z_CLIP = 1e-6


# Pre-registered top-coverage thresholds: K = smallest round K whose MEAN weekly
# share of total raw activity (karma / interactions) reaches the coverage level.
# Chosen from concentration statistics alone, BEFORE any fit comparison (never
# adjusted because a different K scores better).  Measured 2026-07-02:
#   reddit  (T=30, N~200k/wk): top-2500 = 80.0%, top-5000 = 89.2%, top-10000 = 95.6%
#   facebook(T=88, N~14.4k/wk): top-1800 = 80%,  top-3500 = 90%,   top-5500 = 95%
COVERAGE_K = {
    "reddit": {80: 2500, 90: 5000, 95: 10000},
    "facebook": {80: 1800, 90: 3500, 95: 5500},
}


# --------------------------------------------------------------------------- #
# Data loading / canonicalization
# --------------------------------------------------------------------------- #
def _rank_within(df: pd.DataFrame) -> pd.DataFrame:
    """Assign unique 1..N_t ranks per period (metric desc, entity_id tiebreak)
    and recompute N and the rank coordinate z."""
    df = df.sort_values(["period", "metric", "entity_id"], ascending=[True, False, True])
    df["rank"] = df.groupby("period").cumcount() + 1
    df["N"] = df.groupby("period")["entity_id"].transform("size")
    df["z"] = np.log(np.clip((df["rank"] - 0.5) / df["N"], Z_CLIP, 1.0))
    return df.reset_index(drop=True)


def load_panel(cfg: dict) -> pd.DataFrame:
    df = pd.read_parquet(cfg["path"], columns=[cfg["id_col"], cfg["ts_col"], cfg["metric_col"]])
    df = df.rename(columns={cfg["id_col"]: "entity_id", cfg["ts_col"]: "ts", cfg["metric_col"]: "metric"})
    df["entity_id"] = df["entity_id"].astype(str)
    df["ts"] = pd.to_datetime(df["ts"])
    df["metric"] = pd.to_numeric(df["metric"], errors="coerce")
    df = df.dropna(subset=["entity_id", "ts", "metric"])
    df = df[df["metric"] >= 0]
    # collapse duplicate (ts, entity) by max, then assign a unique 1..N rank per period
    df = df.groupby(["ts", "entity_id"], as_index=False, sort=False)["metric"].max()
    df["period"] = df["ts"].rank(method="dense").astype(int) - 1
    df["X"] = np.log1p(df["metric"].to_numpy(dtype=float))
    df = _rank_within(df)
    if cfg["max_rank"] is not None:
        # open weekly cap (legacy; used by the IG negative control)
        df = _rank_within(df[df["rank"] <= cfg["max_rank"]].copy())
    return df


def restrict_universe(df: pd.DataFrame, top_k: int, buffer_mult: int = 4,
                      member_window: int | None = None) -> pd.DataFrame:
    """Closed Lagrangian top-coverage universe with an observed buffer.

    Restrict the panel to the B = buffer_mult * top_k entities with the best
    PERMANENT (time-averaged, full-panel) rank.  Membership is computed on
    periods < member_window only (pass the train end T0 in OOS settings so the
    test window never influences membership; None = all periods, in-sample).

    ALL observations of members are kept -- including weeks they dip below
    top_k (they remain observed in the full panel; no censoring, no
    imputation) -- and weekly ranks are recomputed WITHIN the universe.
    Selection is by entity_id list, never positional masks (band-alignment
    pitfall).  Diagnostics should score ranks <= top_k only; the buffer is a
    sponge layer that absorbs boundary flux (empirically p99 of drop-landings
    and entrant origins is ~4*K, hence buffer_mult=4).
    """
    B = int(buffer_mult * top_k)
    win = df if member_window is None else df[df["period"] < member_window]
    # ABSENCE-PENALIZED permanent rank: average over ALL window periods, with
    # absent periods counted at the observation floor (rank N_t + 1).  Averaging
    # over observed weeks only would re-admit Eulerian selection at the
    # membership stage: an entity observed 2 of 30 weeks at rank ~3000 gets a
    # "great" mean rank, enters the universe, then churns as a phantom
    # exit/entry every week (empirically this put 1-2-week entities alone in
    # the deepest parameter knot and blew up its exit rate to 0.60/wk vs the
    # true 0.0007/wk disappearance rate).
    n_periods = win["period"].nunique()
    floors = win.drop_duplicates("period").set_index("period")["N"] + 1.0
    g = win.groupby("entity_id")
    sum_rank = g["rank"].sum()
    sum_floor_present = (win["N"] + 1.0).groupby(win["entity_id"]).sum()
    perm_rank = (sum_rank + (floors.sum() - sum_floor_present)) / n_periods
    # stable value sort after an index sort => deterministic id tiebreak
    perm_rank = perm_rank.sort_index().sort_values(kind="mergesort")
    members = set(perm_rank.index[:B])
    out = _rank_within(df[df["entity_id"].isin(members)].copy())
    out.attrs["score_k"] = int(top_k)
    out.attrs["universe_B"] = B
    return out


# --------------------------------------------------------------------------- #
# Estimation (one pass, no optimization)
# --------------------------------------------------------------------------- #
@dataclass
class RankParams:
    z_knots: np.ndarray      # indexed by PERMANENT rank coordinate (Lagrangian)
    phi: np.ndarray          # transitory AR(1) persistence
    sigma_trans: np.ndarray  # transitory innovation std (drives crossings)
    sigma_perm: np.ndarray   # permanent random-walk innovation std (long-run mobility)
    sigma_obs: np.ndarray    # iid measurement noise std (high-freq rank jitter)
    lam: np.ndarray          # common-factor loading on the permanent level
    exit_rate: np.ndarray
    T_curve: np.ndarray      # rank-size target E[X | rank] (centripetal anchor)
    kappa: float             # permanent reversion strength toward T_curve
    sigma_F: float
    N: int
    w0: np.ndarray           # sorted period-0 log-values (initial permanent levels)
    bottom_mu: np.ndarray    # low permanent levels for rebirth seeding


def _knot_grid(N: int, n_knots: int = 60) -> np.ndarray:
    ranks = np.unique(np.round(np.geomspace(1, max(N, 2), n_knots)).astype(int))
    return np.log(np.clip((ranks - 0.5) / N, Z_CLIP, 1.0))


def _assign(z: np.ndarray, z_knots: np.ndarray) -> np.ndarray:
    a = np.clip(np.searchsorted(z_knots, z), 0, len(z_knots) - 1)
    left = (a > 0) & (np.abs(z - z_knots[np.clip(a - 1, 0, None)]) < np.abs(z - z_knots[a]))
    return np.where(left, a - 1, a)


def _lagged_cov(u: np.ndarray, same: np.ndarray, abar: np.ndarray, nk: int,
                lag: int, mean: np.ndarray) -> np.ndarray:
    """Per-knot autocovariance of the change series u at `lag`, pooling only
    products of changes `lag` apart that belong to the SAME entity (the rows
    i..i+lag must be a consecutive same-entity run)."""
    n = len(u)
    idx = np.arange(n - lag)
    valid = np.ones(n - lag, dtype=bool)
    for j in range(lag + 1):
        valid &= same[idx + j]
    prod = u[idx] * u[idx + lag]
    valid &= np.isfinite(prod)
    a = abar[idx][valid]
    cnt = np.bincount(a, minlength=nk).astype(float)
    s = np.bincount(a, weights=prod[valid], minlength=nk)
    eprod = np.divide(s, cnt, out=np.zeros(nk), where=cnt > 0)
    return eprod - mean ** 2


def estimate(df: pd.DataFrame, obs_frac: float = 0.5) -> RankParams:
    """One-pass LAGRANGIAN estimator.

    Decompose X_i(t) = mu_i + xi_i(t) where mu_i is the entity's permanent level
    (its time-mean) and xi the transitory deviation.  Rank-dependence is indexed
    by the entity's PERMANENT rank z-bar (immune to the regression-to-the-mean
    selection bias that conditioning on current rank suffers).
    """
    N = int(round(df.groupby("period")["entity_id"].size().mean()))
    z_knots = _knot_grid(N)
    nk = len(z_knots)

    df = df.sort_values(["entity_id", "period"])
    eid = df["entity_id"].to_numpy()
    per = df["period"].to_numpy()
    X = df["X"].to_numpy()
    last_period = int(per.max())

    # permanent rank z-bar (per entity, from its mean rank), broadcast to rows
    g = df.groupby("entity_id", sort=False)
    rbar = g["rank"].transform("mean").to_numpy()
    zbar = np.log(np.clip((rbar - 0.5) / N, Z_CLIP, 1.0))
    abar = _assign(zbar, z_knots)              # entity's permanent-rank knot

    # transition mask (consecutive same entity)
    same = np.zeros(len(df), dtype=bool)
    same[:-1] = (eid[1:] == eid[:-1]) & (per[1:] == per[:-1] + 1)
    dX = np.full(len(df), np.nan); dX[:-1] = X[1:] - X[:-1]

    # common factor (applied to the permanent level): F_t = cross-sec mean dX
    per_t = per[same]
    pc = np.bincount(per_t, minlength=last_period + 1).astype(float)
    F = np.divide(np.bincount(per_t, weights=dX[same], minlength=last_period + 1), pc,
                  out=np.zeros(last_period + 1), where=pc > 0)
    sigma_F = float(np.std(F[pc > 0]))
    u = np.where(same, dX - F[np.where(same, per, 0)], np.nan)   # factor-removed change

    a_t = abar[same]
    ct = np.bincount(a_t, minlength=nk).astype(float)
    ut = u[same]
    mean_u = np.divide(np.bincount(a_t, weights=ut, minlength=nk), ct, out=np.zeros(nk), where=ct > 0)
    # autocovariances of the factor-removed change, by permanent rank
    g0 = np.divide(np.bincount(a_t, weights=ut * ut, minlength=nk), ct, out=np.zeros(nk), where=ct > 0) - mean_u ** 2
    g1 = _lagged_cov(u, same, abar, nk, lag=1, mean=mean_u)
    g2 = _lagged_cov(u, same, abar, nk, lag=2, mean=mean_u)

    # Stable 2-component inversion of the *change* autocovariances
    # (permanent RW + transitory AR(1)):
    #   gamma2/gamma1 = phi ;  gamma1 = -sigma_xi^2 (1-phi)^2 ;
    #   gamma0 = sigma_perm^2 + 2 sigma_xi^2 (1-phi)
    phi = np.clip(np.divide(g2, g1, out=np.zeros(nk), where=np.abs(g1) > 1e-12), 0.0, 0.97)
    sigma_xi2 = np.where(g1 < 0, -g1 / np.clip((1.0 - phi) ** 2, 1e-6, None), 0.0)
    sigma_perm = np.sqrt(np.clip(g0 - 2.0 * sigma_xi2 * (1.0 - phi), 1e-6, None))
    # Reallocate a fraction obs_frac of the transitory stationary variance from
    # persistent AR(1) to iid measurement noise.  Total short-term power is
    # preserved; obs_frac is the single knob that trades RACF/ACF1 (more iid =>
    # faster rank decorrelation) -- a regularized stand-in for the unstable
    # 3-moment split.
    sigma_obs = np.sqrt(np.clip(obs_frac * sigma_xi2, 0.0, None))
    sigma_trans = np.sqrt(np.clip((1.0 - obs_frac) * sigma_xi2 * (1.0 - phi ** 2), 1e-10, None))

    Fi = F[per_t]
    num = np.bincount(a_t, weights=dX[same] * Fi, minlength=nk)
    den = np.bincount(a_t, weights=Fi * Fi, minlength=nk)
    lam = np.clip(np.divide(num, den, out=np.ones(nk), where=den > 1e-12), 0.0, 3.0)

    # exit by permanent rank
    exit_known = per < last_period
    exited = (~same) & exit_known
    ae = abar[exit_known]
    ec = np.bincount(ae, minlength=nk).astype(float)
    es = np.bincount(ae, weights=exited[exit_known].astype(float), minlength=nk)
    exit_rate = np.divide(es, ec, out=np.zeros(nk), where=ec > 0)

    phi, sigma_trans, sigma_perm, sigma_obs, lam, exit_rate = (
        _fill(ct, v) for v in (phi, sigma_trans, sigma_perm, sigma_obs, lam, exit_rate))

    # rank-size target curve T(z) = E[X | current rank] (unbiased; the centripetal
    # anchor the permanent level reverts toward to keep the distribution stationary)
    acur = _assign(df["z"].to_numpy(), z_knots)
    cc = np.bincount(acur, minlength=nk).astype(float)
    T_curve = _fill(cc, np.divide(np.bincount(acur, weights=X, minlength=nk), cc,
                                  out=np.zeros(nk), where=cc > 0))

    w0 = np.sort(df.loc[df["period"] == 0, "X"].to_numpy(dtype=float))[::-1]
    mu_entity = df.groupby("entity_id")["X"].mean().to_numpy(dtype=float)
    bottom_mu = np.quantile(mu_entity, np.linspace(0.0, 0.10, 200))
    return RankParams(z_knots, phi, sigma_trans, sigma_perm, sigma_obs, lam, exit_rate,
                      T_curve, 0.0, sigma_F, N, w0, bottom_mu)


def _fill(cnt: np.ndarray, vals: np.ndarray) -> np.ndarray:
    good = cnt > 0
    if good.all() or not good.any():
        return vals
    idx = np.arange(len(vals))
    return np.interp(idx, idx[good], vals[good])


# --------------------------------------------------------------------------- #
# Simulation
# --------------------------------------------------------------------------- #
def _interp(z, p: RankParams, arr):
    return np.interp(z, p.z_knots, arr, left=arr[0], right=arr[-1])


def simulate(p: RankParams, T: int, seed: int = 0, *, use_factor=True, use_exit=True,
             factor_head_damp: float = 1.0, kappa: float | None = None, track: int = 4000,
             top_record: int | None = None) -> dict:
    kappa = p.kappa if kappa is None else kappa
    rng = np.random.default_rng(seed)
    N = p.N
    mu = _extend(p.w0, N)                       # permanent level
    home = mu.copy()                            # OU anchor (rank "home")
    xi = np.zeros(N)                            # transitory deviation
    ids = np.arange(N, dtype=np.int64)
    next_id = N
    burn = 40

    ntrack = min(track, N)
    tsel = np.sort(rng.choice(N, ntrack, replace=False))
    tvals = np.full((T, ntrack), np.nan)
    tranks = np.zeros((T, ntrack), dtype=np.int32)
    # record occupant ids down to the boundary-flux depth when a universe is active
    topK = min(N, max(max(COLLISION_RANKS), top_record or 0))
    top_ids = np.full((T, topK), -1, dtype=np.int64)
    ranksize = np.zeros((T, min(N, 2000)))

    for t in range(-burn, T):
        # coefficients indexed by PERMANENT rank (rank of mu) -> no selection bias
        morder = np.argsort(-mu)
        prank = np.empty(N, dtype=np.int64)
        prank[morder] = np.arange(1, N + 1)
        zp = np.log(np.clip((prank - 0.5) / N, Z_CLIP, 1.0))
        phi = _interp(zp, p, p.phi)
        st = _interp(zp, p, p.sigma_trans)
        sp = _interp(zp, p, p.sigma_perm)
        lam = _interp(zp, p, p.lam) if use_factor else np.zeros(N)
        if use_factor and factor_head_damp != 1.0:
            lam = np.where(prank <= 100, lam * factor_head_damp, lam)

        so = _interp(zp, p, p.sigma_obs)
        F = rng.normal(0.0, p.sigma_F) if use_factor else 0.0
        # OU confinement of the permanent level toward each entity's home rank
        # (finite long-run rank-band width => caps VR and rank autocorrelation).
        if kappa > 0:
            mu = mu - kappa * (mu - home)
        mu = mu + lam * F + sp * rng.standard_normal(N)
        xi = phi * xi + st * rng.standard_normal(N)

        if use_exit:
            ex = rng.random(N) < _interp(zp, p, p.exit_rate)
            ne = int(ex.sum())
            if ne:
                seed_mu = p.bottom_mu[rng.integers(0, p.bottom_mu.size, ne)]
                mu[ex] = seed_mu
                home[ex] = seed_mu
                xi[ex] = 0.0
                ids[ex] = np.arange(next_id, next_id + ne)
                next_id += ne

        if t < 0:
            continue
        # observe and rank by true level + iid measurement noise (lowers RACF
        # without adding permanent variance), matching v4.3's rank-by-observed.
        X = mu + xi + so * rng.standard_normal(N)
        order = np.argsort(-X)
        rank = np.empty(N, dtype=np.int64)
        rank[order] = np.arange(1, N + 1)
        tvals[t] = X[tsel]
        tranks[t] = rank[tsel]
        top_ids[t] = ids[order[:topK]]
        ranksize[t] = X[order[:ranksize.shape[1]]]

    return dict(tvals=tvals, tranks=tranks, top_ids=top_ids, ranksize=ranksize)


def _extend(w0: np.ndarray, N: int) -> np.ndarray:
    if w0.size >= N:
        return w0[:N].astype(float).copy()
    tail = min(2000, w0.size)
    r = np.arange(w0.size - tail + 1, w0.size + 1, dtype=float)
    sl, ic = np.polyfit(np.log(r), w0[-tail:], 1)
    ext = ic + sl * np.log(np.arange(w0.size + 1, N + 1, dtype=float))
    return np.concatenate([w0, ext]).astype(float)


# --------------------------------------------------------------------------- #
# Diagnostics (identical for empirical and simulated structures)
# --------------------------------------------------------------------------- #
def _safe_acf(x, lag):
    x = x[np.isfinite(x)]
    if x.size <= lag + 5 or np.std(x[:-lag]) < 1e-12 or np.std(x[lag:]) < 1e-12:
        return np.nan
    return float(np.corrcoef(x[:-lag], x[lag:])[0, 1])


def _boundary_flux(top_ids: np.ndarray, K: int, ret_h: int = 4) -> tuple[float, float]:
    """Boundary-crossing diagnostics at the top-K universe boundary:
    (i) mean weekly out-flux -- share of the top-K set at t not in the top-K at
    t+1; (ii) mean return rate -- share of those droppers back inside the top-K
    at t+ret_h.  Computed identically for empirical and simulated occupant-id
    arrays, so the truncation boundary is a tested prediction, not an assumption."""
    T = top_ids.shape[0]
    if top_ids.shape[1] < K or T < 2:
        return np.nan, np.nan
    sets = [set(top_ids[t, :K]) - {-1} for t in range(T)]
    outr, back = [], []
    for t in range(T - 1):
        cur = sets[t]
        if not cur:
            continue
        dropped = cur - sets[t + 1]
        outr.append(len(dropped) / len(cur))
        if t + ret_h < T and dropped:
            back.append(len(dropped & sets[t + ret_h]) / len(dropped))
    return (float(np.mean(outr)) if outr else np.nan,
            float(np.mean(back)) if back else np.nan)


def diagnostics(values: np.ndarray, ranks: np.ndarray, top_ids: np.ndarray,
                ranksize: np.ndarray | None, top_k: int, score_k: int | None = None) -> dict:
    """values: (T, n) entity log-values (NaN = absent); ranks: (T, n) 1-based, 0=absent;
    top_ids: (T, Kmax) occupant id at each rank; ranksize: (T, M) sorted values.
    score_k: top-coverage universe boundary; adds boundary-flux diagnostics."""
    obs_all = np.all(np.isfinite(values), axis=0)
    V = values[:, obs_all] if obs_all.sum() >= 10 else values
    R = ranks[:, obs_all] if obs_all.sum() >= 10 else ranks
    ch = np.diff(V, axis=0)
    out = {}

    var1 = np.nanvar(ch, axis=0, ddof=1)
    for k in VR_LAGS:
        if k < V.shape[0]:
            num = np.nanvar(V[k:] - V[:-k], axis=0, ddof=1)
            m = np.isfinite(var1) & (var1 > 1e-12) & np.isfinite(num)
            out[f"VR{k}"] = float(np.nanmedian(num[m] / (k * var1[m]))) if m.any() else np.nan
    for lag in ACF_LAGS:
        vals = [_safe_acf(ch[:, i], lag) for i in range(ch.shape[1])]
        vals = [v for v in vals if np.isfinite(v)]
        out[f"ACF{lag}"] = float(np.median(vals)) if vals else np.nan
    for lag in RACF_LAGS:
        vals = [_safe_acf(R[:, i][R[:, i] > 0].astype(float), lag) for i in range(R.shape[1])]
        vals = [v for v in vals if np.isfinite(v)]
        out[f"RACF{lag}"] = float(np.median(vals)) if vals else np.nan
    for h in HORIZONS:
        if h < V.shape[0]:
            a, b = V[0], V[h]
            m = np.isfinite(a) & np.isfinite(b)
            out[f"R2_{h}"] = float(np.corrcoef(a[m], b[m])[0, 1] ** 2) if m.sum() > 5 else np.nan

    # persistence of the top-k set
    for h in HORIZONS:
        if h < top_ids.shape[0]:
            s0 = set(top_ids[0, :top_k]) - {-1}
            sh = set(top_ids[h, :top_k]) - {-1}
            out[f"Pers{h}"] = float(len(s0 & sh))

    # GOAL-2 churn: collision (occupant change) rate at fixed ranks
    for cr in COLLISION_RANKS:
        if cr - 1 < top_ids.shape[1]:
            prev, cur = top_ids[:-1, cr - 1], top_ids[1:, cr - 1]
            m = (prev >= 0) & (cur >= 0)
            out[f"coll{cr}"] = float(np.mean(prev[m] != cur[m])) if m.any() else np.nan

    # GOAL-2 churn: median |rank change| over horizon among top entities
    for h in DRANK_HORIZONS:
        if h < R.shape[0]:
            d = []
            for i in range(R.shape[1]):
                r = R[:, i]
                ok = (r[:-h] > 0) & (r[h:] > 0) & (r[:-h] <= 200)
                if ok.any():
                    d.append(np.abs(r[h:][ok] - r[:-h][ok]))
            out[f"dRank{h}"] = float(np.median(np.concatenate(d))) if d else np.nan

    if score_k is not None:
        out["outfluxK"], out["return4K"] = _boundary_flux(top_ids, score_k)

    if ranksize is not None:
        out["_ranksize"] = np.nanmean(ranksize, axis=0)
    return out


def empirical_structures(df: pd.DataFrame, top_k: int, track: int = 4000, seed: int = 0,
                         topid_k: int | None = None):
    T = int(df["period"].max()) + 1
    # tracked sample: entities present in >=70% of periods (keeps RACF/VR estimable)
    pres = df.groupby("entity_id")["period"].nunique()
    elig = pres[pres >= 0.7 * T].index.to_numpy()
    rng = np.random.default_rng(seed)
    if elig.size > track:
        elig = rng.choice(elig, track, replace=False)
    sub = df[df["entity_id"].isin(elig)]
    vx = sub.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    vr = sub.pivot(index="period", columns="entity_id", values="rank").reindex(range(T))
    values = vx.to_numpy(dtype=float)
    ranks = np.nan_to_num(vr.to_numpy(dtype=float), nan=0.0).astype(np.int64)

    Kmax = max(max(COLLISION_RANKS), topid_k or 0)
    top_ids = np.full((T, Kmax), -1, dtype=object)
    rs_M = min(2000, int(df.groupby("period").size().min()))
    ranksize = np.full((T, rs_M), np.nan)
    for t, g in df.groupby("period"):
        g = g.sort_values("rank")
        ids = g["entity_id"].to_numpy()
        top_ids[t, :min(Kmax, len(ids))] = ids[:Kmax]
        xs = g["X"].to_numpy()[:rs_M]
        ranksize[t, :len(xs)] = xs
    # map string ids -> ints for set ops (consistent within emp)
    uniq = {v: i for i, v in enumerate(pd.unique(top_ids.ravel()))}
    uniq[-1] = -1
    top_ids_int = np.vectorize(lambda v: uniq.get(v, -1))(top_ids).astype(np.int64)
    return values, ranks, top_ids_int, ranksize


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #
def run_platform(name: str, reps: int = 5, obs_frac: float = 0.4, kappa: float = 0.15,
                 top_k_u: int | None = None, buffer_mult: int = 4, **sim_kw) -> dict:
    cfg = PLATFORMS[name]
    df = load_panel(cfg)
    if top_k_u:
        df = restrict_universe(df, top_k_u, buffer_mult=buffer_mult)
    score_k = df.attrs.get("score_k")
    T = int(df["period"].max()) + 1
    mean_n = df.groupby("period").size().mean()
    top_k = max(10, int(round(0.01 * mean_n)))
    uni = (f" universe=top-{score_k} (buffer B={df.attrs['universe_B']})"
           if score_k else "")
    print(f"\n{'='*72}\n{name.upper()}  | periods={T} mean_N={mean_n:.0f} "
          f"entities={df['entity_id'].nunique():,} top_k={top_k}{uni}\n{'='*72}")

    ev, er, et, ers = empirical_structures(df, top_k, topid_k=score_k)
    emp = diagnostics(ev, er, et, ers, top_k, score_k=score_k)

    p = estimate(df, obs_frac=obs_frac)
    sims = [diagnostics(*_sim_struct(simulate(p, T, seed=s, kappa=kappa,
                                              top_record=score_k, **sim_kw)),
                        top_k, score_k=score_k)
            for s in range(reps)]
    sim = {k: np.nanmean([s[k] for s in sims]) for k in emp if not k.startswith("_")}

    _print_compare(emp, sim, p, top_k, score_k=score_k)
    return dict(name=name, emp=emp, sim=sim, params=p, df_T=T, mean_n=mean_n)


def _score(emp: dict, sim: dict, top_k: int) -> tuple[int, int, float]:
    """v4.3-style pass count over the 15 goal-1 metrics + mean |churn error|."""
    pers_tol = max(3, int(round(0.15 * top_k)))
    passes, total = 0, 0
    for k in ["VR2", "VR4", "VR8", "VR13"]:
        total += 1; e, s = emp[k], sim[k]
        passes += np.isfinite(e) and np.isfinite(s) and abs(s - e) / max(abs(e), 1e-6) < 0.20
    for k in ["ACF1", "ACF2", "RACF1", "RACF4", "RACF13", "R2_1", "R2_4", "R2_13"]:
        total += 1; e, s = emp[k], sim[k]
        passes += np.isfinite(e) and np.isfinite(s) and abs(s - e) < 0.08
    for k in ["Pers1", "Pers4", "Pers13"]:
        total += 1; e, s = emp[k], sim[k]
        passes += np.isfinite(e) and np.isfinite(s) and abs(s - e) <= pers_tol
    churn = [abs(sim[f"coll{c}"] - emp[f"coll{c}"]) for c in COLLISION_RANKS
             if np.isfinite(emp.get(f"coll{c}", np.nan)) and np.isfinite(sim.get(f"coll{c}", np.nan))]
    return passes, total, float(np.mean(churn)) if churn else np.nan


def _sim_struct(sim: dict):
    return sim["tvals"], sim["tranks"], sim["top_ids"], sim["ranksize"]


def _print_compare(emp: dict, sim: dict, p: RankParams, top_k: int,
                   score_k: int | None = None):
    groups = [("RANK DYNAMICS (goal 1)", ["VR2", "VR4", "VR8", "VR13", "ACF1", "ACF2",
                                          "RACF1", "RACF4", "RACF13", "R2_1", "R2_4", "R2_13"]),
              ("CHURN @ rank (goal 2)", [f"coll{c}" for c in COLLISION_RANKS]),
              ("rank displacement", [f"dRank{h}" for h in DRANK_HORIZONS]),
              ("top-k persistence", [f"Pers{h}" for h in HORIZONS])]
    if score_k is not None:
        groups.append((f"boundary flux @ top-{score_k} (out-rate / 4wk return)",
                       ["outfluxK", "return4K"]))
    for title, keys in groups:
        print(f"\n  {title}")
        print(f"    {'metric':<10}{'emp':>9}{'sim':>9}{'diff':>9}")
        for k in keys:
            e, s = emp.get(k, np.nan), sim.get(k, np.nan)
            d = s - e if np.isfinite(e) and np.isfinite(s) else np.nan
            print(f"    {k:<10}{e:>9.3f}{s:>9.3f}{d:>+9.3f}")
    npass, ntot, churn_err = _score(emp, sim, top_k)
    print(f"\n  >>> v4.3-style score: {npass}/{ntot}   |   mean churn error: {churn_err:.3f}")
    print(f"  factor sigma_F={p.sigma_F:.3f}  N={p.N}   (top..tail by permanent rank)")
    print(f"    phi        ={p.phi[0]:.3f}..{p.phi[-1]:.3f}")
    print(f"    sigma_trans={p.sigma_trans[0]:.3f}..{p.sigma_trans[-1]:.3f}")
    print(f"    sigma_perm ={p.sigma_perm[0]:.3f}..{p.sigma_perm[-1]:.3f}")
    print(f"    sigma_obs  ={p.sigma_obs[0]:.3f}..{p.sigma_obs[-1]:.3f}")
    print(f"    lambda     ={p.lam[0]:.2f}..{p.lam[-1]:.2f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("platforms", nargs="*", default=["facebook", "reddit", "instagram"])
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--no-factor", action="store_true")
    ap.add_argument("--no-exit", action="store_true")
    ap.add_argument("--factor-head-damp", type=float, default=1.0)
    ap.add_argument("--kappa", type=float, default=0.15, help="permanent home-reversion strength")
    ap.add_argument("--obs-frac", type=float, default=0.4, help="share of transitory variance treated as iid obs noise")
    ap.add_argument("--top-k", type=int, default=None,
                    help="top-coverage universe boundary K (applies to every platform listed)")
    ap.add_argument("--coverage", type=int, choices=(80, 90, 95), default=None,
                    help="resolve K per platform from the pre-registered COVERAGE_K rule")
    ap.add_argument("--buffer-mult", type=int, default=4,
                    help="universe buffer depth B = buffer_mult * K (sponge layer)")
    args = ap.parse_args()
    for plat in args.platforms:
        top_k_u = args.top_k
        if top_k_u is None and args.coverage is not None:
            top_k_u = COVERAGE_K.get(plat, {}).get(args.coverage)
            if top_k_u is None:
                raise SystemExit(f"no pre-registered K for {plat} at coverage {args.coverage}%")
        run_platform(plat, reps=args.reps, obs_frac=args.obs_frac, kappa=args.kappa,
                     top_k_u=top_k_u, buffer_mult=args.buffer_mult,
                     use_factor=not args.no_factor, use_exit=not args.no_exit,
                     factor_head_damp=args.factor_head_damp)
