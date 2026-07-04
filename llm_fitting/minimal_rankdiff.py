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
# 2026-07-03 additions (data on the SSD via the local data/ssd symlink; see
# DATA_PHASE2_REPORT.md).  The facebook_* entries are CrowdTangle INSTRUMENT-ERA
# slices of the rebuilt weekly panel (era table + directives: instrument_eras.py
# and MODEL_STATUS 2g): CrowdTangle is a censored fixed ~14.5k-page panel, so
# every FB coverage/entry statement is "of tracked activity", never platform-
# wide, and no window may straddle eras B or M.  date_min/date_max slice the
# panel BEFORE period indexing.  daily_path enables Spec-B; day_guard applies
# the instrument_eras low-count-day guard to the daily noise-floor estimation
# (weekly fits keep flagged weeks -- declared in instrument_eras.py).
# reddit_comments is a census (Pushshift) -- platform-wide language is fine,
# no eras, guard verified unnecessary (0/943 days flagged).
from instrument_eras import DAILY_PATHS as _ERA_DAILY, FB_ERAS as _FB_ERAS

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
    "facebook_a": dict(path="data/ssd/derived/fb_weekly_rebuilt.parquet",
                       id_col="endpoint_id", ts_col="date", metric_col="metric_value",
                       max_rank=None, date_min=_FB_ERAS["A"][0], date_max=_FB_ERAS["A"][1],
                       daily_path=_ERA_DAILY["facebook"], day_guard=True),
    "facebook_c": dict(path="data/ssd/derived/fb_weekly_rebuilt.parquet",
                       id_col="endpoint_id", ts_col="date", metric_col="metric_value",
                       max_rank=None, date_min=_FB_ERAS["C"][0], date_max=_FB_ERAS["C"][1],
                       daily_path=_ERA_DAILY["facebook"], day_guard=True),
    "facebook_d": dict(path="data/ssd/derived/fb_weekly_rebuilt.parquet",
                       id_col="endpoint_id", ts_col="date", metric_col="metric_value",
                       max_rank=None, date_min=_FB_ERAS["D"][0], date_max=_FB_ERAS["D"][1],
                       daily_path=_ERA_DAILY["facebook"], day_guard=True),
    "reddit_comments": dict(
        path="data/ssd/derived/reddit_comments_2018-12_2021-06_weekly.parquet",
        id_col="endpoint_id", ts_col="date", metric_col="metric_value",
        max_rank=None, daily_path=_ERA_DAILY["reddit_comments"], day_guard=False),
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
    # facebook_a (2026-07-03): measured on the Era-A slice of the rebuilt panel
    # BEFORE any fit -- top-1800 = 79.7%, top-3500 = 89.7%, top-5500 = 94.8% "of
    # tracked activity" (CrowdTangle is censored; never platform-wide).  Within
    # 0.3pp of the trusted-panel numbers (same fixed instrument panel), so the
    # SAME K values are pre-registered for comparability with old-FB results.
    "facebook_a": {80: 1800, 90: 3500, 95: 5500},
    # reddit_comments (2026-07-03): census; measured in DATA_PHASE2_REPORT.md
    # before any fit -- top-1000 = 81.4%, top-2500 = 90.8%, top-5000 = 95.6%.
    # Owner-mandated WORKING scale is K=12,500 (B=50k, 98.8%) -- a scale
    # decision on top of, not a replacement for, this rule.
    "reddit_comments": {80: 1000, 90: 2500, 95: 5000},
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
    # optional instrument-era slice (inclusive; applied BEFORE period indexing
    # so periods are consecutive within the era -- see instrument_eras.py)
    if cfg.get("date_min") is not None:
        df = df[df["ts"] >= pd.Timestamp(cfg["date_min"])]
    if cfg.get("date_max") is not None:
        df = df[df["ts"] <= pd.Timestamp(cfg["date_max"])]
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
                      member_window: int | None = None,
                      member_span: tuple[int, int] | None = None) -> pd.DataFrame:
    """Closed Lagrangian top-coverage universe with an observed buffer.

    Restrict the panel to the B = buffer_mult * top_k entities with the best
    PERMANENT (time-averaged, full-panel) rank.  Membership is computed on
    periods < member_window only (pass the train end T0 in OOS settings so the
    test window never influences membership; None = all periods, in-sample).
    member_span=(lo, hi) computes membership on lo <= period < hi instead
    (P5 membership-robustness probes on long census panels, where full-window
    absence-penalized membership drifts toward "always-existed" entities --
    MODEL_STATUS 2g owner caveat); mutually exclusive with member_window.

    ALL observations of members are kept -- including weeks they dip below
    top_k (they remain observed in the full panel; no censoring, no
    imputation) -- and weekly ranks are recomputed WITHIN the universe.
    Selection is by entity_id list, never positional masks (band-alignment
    pitfall).  Diagnostics should score ranks <= top_k only; the buffer is a
    sponge layer that absorbs boundary flux (empirically p99 of drop-landings
    and entrant origins is ~4*K, hence buffer_mult=4).
    """
    B = int(buffer_mult * top_k)
    if member_span is not None:
        assert member_window is None, "member_span and member_window are exclusive"
        win = df[(df["period"] >= member_span[0]) & (df["period"] < member_span[1])]
    else:
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
    temper_s: float = 0.0    # log-SD of the persistent entity volatility multiplier
    #                          v_i (E[v_i]=1); 0 = homogeneous (temperament off)
    kappa_z: np.ndarray | None = None  # per-knot OU home-reversion rate (MD-estimated;
    #                          None = legacy hand-set global kappa)
    t_df: float = float("inf")  # Student-t df for transitory innovations (inf = Gaussian)
    factor_rho: float | None = None  # AR(1) coefficient of the STATIONARY common level
    #                          (--stat-factor, measured from the cumulative-F path).
    #                          None = legacy: F integrates into mu (a random-walk
    #                          common level -- measurably wrong: the empirical
    #                          platform level mean-reverts, e.g. comments level
    #                          VR13 = 0.121, dLevel ~ white).
    phi2: np.ndarray | None = None         # medium-timescale transitory AR(1)
    sigma_trans2: np.ndarray | None = None  # (--two-scale; None/zeros = off, nested)
    mix_b: float = 0.0  # permanent-share MIX exponent (--mix-hetero):
    #                     sigma_perm,i = sigma_perm(z) * v_i^(b/2) / norm, so the
    #                     permanent component disperses with spread b*s across
    #                     entities (E[w]=1 renormalized -- pooled moments and the
    #                     Eulerian structure preserved).  b=0 = movement-only
    #                     temperament (legacy); b=1 = full structure B (2c).
    #                     IDENTIFIED from the s(h) horizon moment: b = s(h*)/s(1),
    #                     h* = longest horizon with >=8 non-overlapping changes
    #                     (measured 2026-07-03: comments 1.08, FB Era A 1.02 --
    #                     s(h) is flat, replicating 2c's finding).


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


# Minimum-distance grids for the OU-home + AR(1)-transitory + noise partition.
A_GRID = np.array([0.995, 0.99, 0.98, 0.96, 0.93, 0.90, 0.86, 0.80])   # a = 1 - kappa
PHI_GRID = np.arange(0.0, 0.66, 0.05)

# Multi-horizon change-variance moments D(h) = Var(X_{t+h} - X_t) for the
# --md-vr identification fix (2026-07-03).  Diagnosis: the gamma_0..gamma_6
# objective is FLAT in the home-reversion rate a -- the OU tail is spread
# thinly over many lags (each gamma_k ~ 2e-4 vs gamma_0 ~ 0.06) and the three
# free variance coefficients absorb any a, so kappa was effectively
# unidentified and the simulator over-persisted every 4-13-week moment (the
# comments VR block).  Long-horizon change variances aggregate exactly that
# tail (Cochrane 1988; Poterba-Summers 1988: variance ratios are the powerful
# statistic against slowly-decaying components), making SSE(a) sharply
# V-shaped.  Same model, same parameters -- only the moment set changes.
# A_GRID_VR extends the reversion grid (declared): md6 fits were already
# pinned at the old kappa=0.2 edge on some knots.
VR_MOM_H = (2, 4, 8, 13)
A_GRID_VR = np.array([0.995, 0.99, 0.98, 0.96, 0.93, 0.90, 0.86, 0.80, 0.75, 0.70])

# Two-timescale decomposition (--two-scale, 2026-07-03).  With kappa identified
# (2i) and the common factor stationary (2j), the residual VR over-persistence
# carries a kappa TENSION: a single OU home strong enough for the 4-13-week
# variance ratios over-reverts the 13-week rank autocorrelation.  Resolution: a
# SECOND transitory component at a medium timescale (half-life ~3-13 wks).  It
# contributes almost nothing to the short-lag gamma tail (B2 = V2(1-phi2)^2 is
# tiny for phi2 near 1) while supplying the missing mid-horizon D(h) curvature,
# so the home can stay SLOW (grid constrained to kappa <= 0.07 -- identification
# by construction: "home" is the slow scale) and rank ACF survives.  Identified
# from the D(h) moments, hence requires --md-vr; sigma2 = 0 recovers the current
# model exactly (nested).
A2_GRID = np.array([0.995, 0.99, 0.98, 0.96, 0.93])   # slow home only
PHI2_GRID = np.array([0.70, 0.75, 0.80, 0.85, 0.90, 0.95])


def _md_partition(gk: np.ndarray, s_e_fix: float | None = None,
                  d_mom: np.ndarray | None = None,
                  d_h: tuple = VR_MOM_H) -> tuple[float, float, float, float, float]:
    """Minimum-distance fit of the change-autocovariance function gamma_0..L
    (Chamberlain / Abowd-Card covariance-structure estimation) to
        X_it = h_it + xi_it + eps_it
        h:  OU with reversion kappa = 1-a, innovation sd sigma_eta
        xi: AR(1) phi, innovation sd sigma_nu;   eps: iid sd sigma_e
    Change autocovariances:
        gamma_0 = 2W(1-a) + 2V(1-phi) + 2 s_e     (W=Var h, V=Var xi, s_e=sigma_e^2)
        gamma_1 = -A - B - s_e                     (A=W(1-a)^2, B=V(1-phi)^2)
        gamma_k = -A a^(k-1) - B phi^(k-1), k>=2
    A pure random-walk home (a=1) forces gamma_k = 0 for k>=2 -- the observed
    negative tail at lags 3-6 is what identifies kappa (previously a hand-set
    knob, and the estimator/simulator were inconsistent about it).

    s_e_fix: PIN sigma_e externally (Spec-B daily noise floor).  This breaks
    the phi->0 weak identification of the fast split (AR(1) transitory vs iid
    noise are observationally equivalent in weekly covariances): the noise
    contribution is subtracted from gamma_0/gamma_1 and only (OU, AR) are fit.

    d_mom: multi-horizon change-variance moments D(h) for h in d_h, appended
    to the objective (--md-vr).  Model rows are closed-form in the same three
    coefficients:  D(h) = 2W(1-a^h) + 2V(1-phi^h) + 2 s_e
    (W-coef A = W(1-a)^2, V-coef B = V(1-phi)^2, as for the gammas).  These
    moments identify the home-reversion rate a where the gamma tail cannot
    (see VR_MOM_H note); the reversion grid extends to A_GRID_VR.
    Returns (kappa, sigma_eta, phi, sigma_nu, sigma_e)."""
    L = len(gk) - 1
    if d_mom is not None:
        ok_d = np.isfinite(d_mom)
        d_mom = d_mom[ok_d]
        d_h = tuple(h for h, ok in zip(d_h, ok_d) if ok)
    if s_e_fix is not None:
        gk = gk.copy()
        gk[0] -= 2.0 * s_e_fix ** 2
        gk[1] += s_e_fix ** 2
        if d_mom is not None:
            d_mom = d_mom - 2.0 * s_e_fix ** 2
    y = gk if d_mom is None or not len(d_mom) else np.concatenate([gk, d_mom])
    best = (np.inf, None)
    for a in (A_GRID if d_mom is None else A_GRID_VR):
        for phi in PHI_GRID:
            rows = [[2.0 / (1 - a), 2.0 / (1 - phi)],
                    [-1.0, -1.0]]
            rows += [[-a ** (k - 1), -phi ** (k - 1)] for k in range(2, L + 1)]
            n_g = len(rows)
            if d_mom is not None and len(d_mom):
                rows += [[2.0 * (1 - a ** h) / (1 - a) ** 2,
                          2.0 * (1 - phi ** h) / (1 - phi) ** 2] for h in d_h]
            X = np.array(rows)
            if s_e_fix is None:
                noise = np.array([[2.0], [-1.0]] + [[0.0]] * (n_g - 2)
                                 + [[2.0]] * (len(rows) - n_g))
                X = np.hstack([X, noise])
            coef, *_ = np.linalg.lstsq(X, y, rcond=None)
            coef = np.clip(coef, 0.0, None)
            sse = float(np.sum((y - X @ coef) ** 2))
            if sse < best[0]:
                best = (sse, (a, phi, coef))
    a, phi, coef = best[1]
    A, B = coef[0], coef[1]
    s_e = s_e_fix ** 2 if s_e_fix is not None else coef[2]
    sigma_eta = np.sqrt(max(A * (1 + a) / (1 - a), 0.0))
    sigma_nu = np.sqrt(max(B * (1 + phi) / (1 - phi), 0.0))
    return 1.0 - a, sigma_eta, phi, sigma_nu, np.sqrt(max(s_e, 0.0))


def _hstep_var(u: np.ndarray, same: np.ndarray, abar: np.ndarray, nk: int,
               h: int) -> np.ndarray:
    """Per-knot variance of the h-step same-entity sum of the factor-removed
    change u (= X_{t+h} - X_t net of the common factor), pooling only rows
    i..i+h-1 that form a consecutive same-entity run.  This is the D(h)
    moment for the --md-vr identification fix."""
    n = len(u)
    idx = np.arange(n - h)
    valid = np.ones(n - h, dtype=bool)
    for j in range(h):
        valid &= same[idx + j]
    cs = np.cumsum(np.where(np.isfinite(u), u, 0.0))
    S = cs[idx + h - 1] - np.where(idx > 0, cs[np.maximum(idx - 1, 0)], 0.0)
    a = abar[idx][valid]
    Sv = S[valid]
    cnt = np.bincount(a, minlength=nk).astype(float)
    m1 = np.divide(np.bincount(a, weights=Sv, minlength=nk), cnt,
                   out=np.zeros(nk), where=cnt > 0)
    m2 = np.divide(np.bincount(a, weights=Sv * Sv, minlength=nk), cnt,
                   out=np.zeros(nk), where=cnt > 0)
    out = m2 - m1 ** 2
    out[cnt <= 0] = np.nan
    return out


def _md_partition2(gk: np.ndarray, s_e_fix: float | None = None,
                   d_mom: np.ndarray | None = None,
                   d_h: tuple = VR_MOM_H):
    """Two-timescale minimum-distance fit (see A2_GRID note):
        X = h(OU slow, a in A2_GRID) + xi1(AR fast, phi1 in PHI_GRID)
          + xi2(AR medium, phi2 in PHI2_GRID) + eps(iid)
    Same moment rows as _md_partition plus a third decay column; the D(h)
    moments (required) are what identify phi2 -- the gamma tail alone cannot.
    Returns (kappa, s_eta, phi1, s_nu1, phi2, s_nu2, s_e)."""
    assert d_mom is not None, "--two-scale requires the D(h) moments (--md-vr)"
    L = len(gk) - 1
    ok_d = np.isfinite(d_mom)
    d_mom = d_mom[ok_d]
    d_h = tuple(h for h, ok in zip(d_h, ok_d) if ok)
    if s_e_fix is not None:
        gk = gk.copy()
        gk[0] -= 2.0 * s_e_fix ** 2
        gk[1] += s_e_fix ** 2
        d_mom = d_mom - 2.0 * s_e_fix ** 2
    y = np.concatenate([gk, d_mom])
    best = (np.inf, None)
    for a in A2_GRID:
        for p2 in PHI2_GRID:
            for p1 in PHI_GRID:
                comps = (a, p1, p2)
                rows = [[2.0 / (1 - c) for c in comps],
                        [-1.0, -1.0, -1.0]]
                rows += [[-c ** (k - 1) for c in comps] for k in range(2, L + 1)]
                n_g = len(rows)
                rows += [[2.0 * (1 - c ** h) / (1 - c) ** 2 for c in comps]
                         for h in d_h]
                X = np.array(rows)
                if s_e_fix is None:
                    noise = np.array([[2.0], [-1.0]] + [[0.0]] * (n_g - 2)
                                     + [[2.0]] * len(d_h))
                    X = np.hstack([X, noise])
                coef, *_ = np.linalg.lstsq(X, y, rcond=None)
                coef = np.clip(coef, 0.0, None)
                sse = float(np.sum((y - X @ coef) ** 2))
                if sse < best[0]:
                    best = (sse, (a, p1, p2, coef))
    a, p1, p2, coef = best[1]
    A, B1, B2 = coef[0], coef[1], coef[2]
    s_e = s_e_fix ** 2 if s_e_fix is not None else coef[3]
    return (1.0 - a,
            np.sqrt(max(A * (1 + a) / (1 - a), 0.0)), p1,
            np.sqrt(max(B1 * (1 + p1) / (1 - p1), 0.0)), p2,
            np.sqrt(max(B2 * (1 + p2) / (1 - p2), 0.0)),
            np.sqrt(max(s_e, 0.0)))


def estimate_tail_df(df: pd.DataFrame, trans_share: float, min_changes: int = 12,
                     kurt_floor: float = 0.05) -> float:
    """Student-t df for the transitory innovations, identified from the median
    WITHIN-entity excess kurtosis of weekly changes (a moment temperament cannot
    produce -- v_i scales, it does not fatten, each entity's tails).  With phi
    small, Delta xi ~ difference of two iid innovations, so
        excess_kurt(u) ~ trans_share^2 * (6/(nu-4)) / 2   =>  nu = 4 + 3 w^2 / k_u.
    Gaussian within-entity changes (k_u <= kurt_floor) => inf (no t needed)."""
    eid, u, same, _ = _change_panel(df)
    ok = same & np.isfinite(u)
    sub = pd.DataFrame({"eid": eid[ok], "u": u[ok]})
    g = sub.groupby("eid")["u"]
    n_i = g.count()
    k_i = g.apply(pd.Series.kurt)
    k_u = float(k_i[n_i >= min_changes].median())
    if not np.isfinite(k_u) or k_u <= kurt_floor:
        return float("inf")
    return float(np.clip(4.0 + 3.0 * trans_share ** 2 / k_u, 4.3, 200.0))


def _pool_sparse(vals_list: list, weights: np.ndarray, ent_counts: np.ndarray,
                 min_n: int) -> list:
    """Adaptive symmetric-window pooling of per-knot (count-weighted mean)
    statistics: each knot's window expands until it covers >= min_n entities.
    Fixes the 1-2-entity head knots whose raw moments are set by whichever
    volatile entity happens to live there (e.g. head sigma_obs estimates
    swinging 2.03 vs 0.46 across universes)."""
    nk = len(ent_counts)
    out = [v.astype(float).copy() for v in vals_list]
    for k in range(nk):
        w = 0
        while True:
            lo, hi = max(0, k - w), min(nk, k + w + 1)
            if ent_counts[lo:hi].sum() >= min_n or (lo == 0 and hi == nk):
                break
            w += 1
        if w == 0:
            continue
        cw = weights[lo:hi].astype(float)
        if cw.sum() <= 0:
            continue
        for v, o in zip(vals_list, out):
            o[k] = float(np.sum(cw * v[lo:hi]) / cw.sum())
    return out


def estimate(df: pd.DataFrame, obs_frac: float = 0.5, temper: bool = False,
             min_knot_n: int | None = None, md_lags: int | None = None,
             t_tails: bool = False,
             sigma_obs_fix: tuple[np.ndarray, np.ndarray] | None = None,
             md_vr: bool = False, stat_factor: bool = False,
             two_scale: bool = False, mix_hetero: bool = False,
             mix_b_fix: float | None = None) -> RankParams:
    """One-pass LAGRANGIAN estimator.

    Decompose X_i(t) = mu_i + xi_i(t) where mu_i is the entity's permanent level
    (its time-mean) and xi the transitory deviation.  Rank-dependence is indexed
    by the entity's PERMANENT rank z-bar (immune to the regression-to-the-mean
    selection bias that conditioning on current rank suffers).

    temper: estimate the persistent entity-volatility dispersion (temperament)
    and store it in params (see estimate_temperament).
    min_knot_n: pool sparse knots' moment statistics until each covers >= this
    many entities (adaptive window; None = off).
    md_lags: minimum-distance fit of gamma_0..gamma_md_lags per knot with an
    OU home (see _md_partition) -- estimates kappa_z and sigma_obs from the
    covariance structure, replacing BOTH the hand-set global kappa AND the
    obs_frac reallocation knob.  None = legacy 3-moment inversion.
    t_tails: Student-t transitory innovations, df from within-entity kurtosis.
    sigma_obs_fix: (z, sigma) curve pinning sigma_obs EXTERNALLY (Spec-B daily
    noise floor) inside the MD fit -- breaks the phi->0 weak identification.
    md_vr: append multi-horizon change-variance moments D(h), h in VR_MOM_H,
    to the MD objective -- identifies the home-reversion rate kappa that the
    short-lag gamma tail cannot (see the VR_MOM_H note; requires md_lags).
    stat_factor: measure the AR(1) coefficient of the (linearly detrended)
    cumulative common-factor path and store it as factor_rho -- the simulator
    then applies the common factor as a STATIONARY level at observation
    instead of integrating it into mu (see RankParams.factor_rho).
    two_scale: second (medium-timescale) transitory component, identified from
    the D(h) moments (requires md_lags AND md_vr; see A2_GRID note).
    mix_hetero: per-entity permanent-share heterogeneity, exponent b identified
    from the s(h) horizon moment (requires temper; see RankParams.mix_b).
    mix_b_fix: IMPOSE the mix exponent instead of measuring it (the b = 1
    restriction test, 2n: b = 1 means one amplitude scales everything and the
    lognormal renormalization is exactly 1 -- the factorized law).
    """
    if two_scale and not (md_lags and md_vr):
        raise ValueError("two_scale requires md_lags and md_vr (the D(h) moments)")
    if mix_hetero and not temper:
        raise ValueError("mix_hetero requires temper (b scales the v_i multiplier)")
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
    n_lags = max(2, md_lags or 0)
    gs = [g0] + [_lagged_cov(u, same, abar, nk, lag=l, mean=mean_u)
                 for l in range(1, n_lags + 1)]

    d_hs: tuple = ()
    d_list: list = []
    if md_vr and md_lags:
        d_hs = tuple(h for h in VR_MOM_H if h < last_period + 1)
        for h in d_hs:
            d = _hstep_var(u, same, abar, nk, h)
            # interpolate the (rare) knots with no h-length runs before pooling
            good = np.isfinite(d).astype(float)
            d_list.append(_fill(good, np.where(np.isfinite(d), d, 0.0)))

    ent_per_knot = None
    if min_knot_n is not None:
        first = np.r_[True, eid[1:] != eid[:-1]]
        ent_per_knot = np.bincount(abar[first], minlength=nk)
        gs = _pool_sparse(gs, ct, ent_per_knot, min_knot_n)
        if d_list:
            d_list = _pool_sparse(d_list, ct, ent_per_knot, min_knot_n)
    g0, g1, g2 = gs[0], gs[1], gs[2]

    kappa_z = None
    phi2 = sigma_trans2 = None
    if md_lags:
        # minimum-distance covariance-structure fit per knot (OU home)
        kappa_z = np.zeros(nk)
        phi = np.zeros(nk); sigma_perm = np.zeros(nk)
        sigma_trans = np.zeros(nk); sigma_obs = np.zeros(nk)
        if two_scale:
            phi2 = np.zeros(nk); sigma_trans2 = np.zeros(nk)
        gmat = np.stack(gs, axis=1)   # (nk, L+1)
        so_fix = (np.interp(z_knots, sigma_obs_fix[0], sigma_obs_fix[1])
                  if sigma_obs_fix is not None else None)
        for j in range(nk):
            if ct[j] <= 0 and ent_per_knot is None:
                continue
            d_j = np.array([d[j] for d in d_list]) if d_list else None
            sef = None if so_fix is None else float(so_fix[j])
            if two_scale:
                (kap, s_eta, ph, s_nu, ph2, s_nu2, s_e) = _md_partition2(
                    gmat[j], s_e_fix=sef, d_mom=d_j, d_h=d_hs)
                phi2[j], sigma_trans2[j] = ph2, s_nu2
            else:
                kap, s_eta, ph, s_nu, s_e = _md_partition(
                    gmat[j], s_e_fix=sef, d_mom=d_j, d_h=d_hs)
            kappa_z[j], sigma_perm[j], phi[j] = kap, s_eta, ph
            sigma_trans[j], sigma_obs[j] = s_nu, s_e
    else:
        # Legacy stable 2-component inversion (permanent RW + transitory AR(1)):
        #   gamma2/gamma1 = phi ;  gamma1 = -sigma_xi^2 (1-phi)^2 ;
        #   gamma0 = sigma_perm^2 + 2 sigma_xi^2 (1-phi)
        phi = np.clip(np.divide(g2, g1, out=np.zeros(nk), where=np.abs(g1) > 1e-12), 0.0, 0.97)
        sigma_xi2 = np.where(g1 < 0, -g1 / np.clip((1.0 - phi) ** 2, 1e-6, None), 0.0)
        sigma_perm = np.sqrt(np.clip(g0 - 2.0 * sigma_xi2 * (1.0 - phi), 1e-6, None))
        # Reallocate a fraction obs_frac of the transitory stationary variance from
        # persistent AR(1) to iid measurement noise.  Total short-term power is
        # preserved; obs_frac is the single knob that trades RACF/ACF1 (more iid =>
        # faster rank decorrelation) -- a regularized stand-in for the unstable
        # 3-moment split (superseded by md_lags, which identifies sigma_obs).
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

    if ent_per_knot is not None:
        (lam,) = _pool_sparse([lam], den, ent_per_knot, min_knot_n)
        (exit_rate,) = _pool_sparse([exit_rate], ec, ent_per_knot, min_knot_n)

    phi, sigma_trans, sigma_perm, sigma_obs, lam, exit_rate = (
        _fill(ct, v) for v in (phi, sigma_trans, sigma_perm, sigma_obs, lam, exit_rate))
    if kappa_z is not None:
        kappa_z = _fill(ct, kappa_z)
    if phi2 is not None:
        phi2, sigma_trans2 = _fill(ct, phi2), _fill(ct, sigma_trans2)

    # rank-size target curve T(z) = E[X | current rank] (unbiased; the centripetal
    # anchor the permanent level reverts toward to keep the distribution stationary)
    acur = _assign(df["z"].to_numpy(), z_knots)
    cc = np.bincount(acur, minlength=nk).astype(float)
    T_curve = _fill(cc, np.divide(np.bincount(acur, weights=X, minlength=nk), cc,
                                  out=np.zeros(nk), where=cc > 0))

    factor_rho = None
    if stat_factor:
        # AR(1) coefficient of the platform-wide level: cumulative F, linearly
        # detrended (growth trend is common and rank-neutral).  Measured, not
        # tuned; clip to [0, 0.95] for simulator stability.
        lev = np.cumsum(F[pc > 0])
        if lev.size > 10:
            t_idx = np.arange(lev.size, dtype=float)
            lev_d = lev - np.polyval(np.polyfit(t_idx, lev, 1), t_idx)
            factor_rho = (float(np.clip(np.corrcoef(lev_d[:-1], lev_d[1:])[0, 1],
                                        0.0, 0.95))
                          if np.std(lev_d) > 1e-12 else 0.0)
        else:
            factor_rho = 0.0

    w0 = np.sort(df.loc[df["period"] == 0, "X"].to_numpy(dtype=float))[::-1]
    mu_entity = df.groupby("entity_id")["X"].mean().to_numpy(dtype=float)
    bottom_mu = np.quantile(mu_entity, np.linspace(0.0, 0.10, 200))
    temper_s = estimate_temperament(df)["s"] if temper else 0.0
    mix_b = 0.0
    if mix_hetero and temper_s > 0:
        mix_b = float(mix_b_fix) if mix_b_fix is not None else estimate_mix_b(df, temper_s)
    t_df = float("inf")
    if t_tails:
        # variance share of the transitory CHANGE in the weekly change, for the
        # kurtosis -> df mapping (count-weighted across knots)
        V = sigma_trans ** 2 / np.clip(1.0 - phi ** 2, 1e-6, None)
        share = np.clip(2.0 * V * (1.0 - phi) / np.clip(g0, 1e-12, None), 0.0, 1.0)
        w_share = float(np.sum(ct * share) / max(ct.sum(), 1.0))
        t_df = estimate_tail_df(df, w_share)
    return RankParams(z_knots, phi, sigma_trans, sigma_perm, sigma_obs, lam, exit_rate,
                      T_curve, 0.0, sigma_F, N, w0, bottom_mu, temper_s, kappa_z, t_df,
                      factor_rho, phi2, sigma_trans2, mix_b)


def _fill(cnt: np.ndarray, vals: np.ndarray) -> np.ndarray:
    good = cnt > 0
    if good.all() or not good.any():
        return vals
    idx = np.arange(len(vals))
    return np.interp(idx, idx[good], vals[good])


def _change_panel(df: pd.DataFrame):
    """Factor-removed same-entity weekly changes u_it, plus per-entity zbar.
    Returns (eid, per, u, same, zbar_by_entity: pd.Series)."""
    df = df.sort_values(["entity_id", "period"])
    eid = df["entity_id"].to_numpy()
    per = df["period"].to_numpy()
    X = df["X"].to_numpy()
    last_period = int(per.max())
    same = np.zeros(len(df), dtype=bool)
    same[:-1] = (eid[1:] == eid[:-1]) & (per[1:] == per[:-1] + 1)
    dX = np.full(len(df), np.nan)
    dX[:-1] = X[1:] - X[:-1]
    per_t = per[same]
    pc = np.bincount(per_t, minlength=last_period + 1).astype(float)
    F = np.divide(np.bincount(per_t, weights=dX[same], minlength=last_period + 1), pc,
                  out=np.zeros(last_period + 1), where=pc > 0)
    u = np.where(same, dX - F[np.where(same, per, 0)], np.nan)
    N = int(round(df.groupby("period")["entity_id"].size().mean()))
    rbar = df.groupby("entity_id")["rank"].mean()
    zbar = np.log(np.clip((rbar - 0.5) / N, Z_CLIP, 1.0))
    return eid, u, same, zbar


def estimate_temperament(df: pd.DataFrame, n_bands: int = 10, min_changes: int = 12) -> dict:
    """Estimate the dispersion s of the persistent entity-level volatility
    multiplier ("temperament"):  sigma_i = sigma(zbar_i) * sqrt(v_i),
    log v_i ~ N(-s^2/2, s^2), E[v_i] = 1.

    Method (log-variance moment decomposition, Smyth 2004 limma-style, with a
    Satterthwaite effective-df correction for the MA autocorrelation of weekly
    changes):
        log s_i^2 = log sigma^2(zbar_i) + log v_i + log(chi2_nu_i / nu_i)
        E[log chi2_nu/nu]   = psi(nu/2) - log(nu/2)      (bias, removed per entity)
        Var[log chi2_nu/nu] = psi'(nu/2)                 (sampling noise, subtracted)
        nu_i = (n_i - 1) / kappa,  kappa = 1 + 2*sum_k rho_k^2   (rho = pooled
        within-entity ACF of u; sample variance of an autocorrelated series has
        inflated variance => fewer effective df)
    The band term log sigma^2(zbar) is removed by demeaning within zbar-quantile
    bands, so s^2 = Var_within-band(corrected log s_i^2) - mean_i psi'(nu_i/2).
    Identified from the variance-dispersion moment ONLY -- never tuned to churn
    or displacement.
    """
    from scipy.special import digamma, polygamma
    eid, u, same, zbar = _change_panel(df)
    ok = same & np.isfinite(u)
    sub = pd.DataFrame({"eid": eid[ok], "u": u[ok]})
    g = sub.groupby("eid")["u"]
    n_i = g.count()
    s2_i = g.var(ddof=1)
    keep = n_i >= min_changes
    n_i, s2_i = n_i[keep], s2_i[keep]
    if len(n_i) < 50:
        return dict(s=0.0, n_entities=int(len(n_i)), kappa=np.nan, per_band=[])

    # pooled within-entity ACF of u at lags 1..2 -> Satterthwaite kappa
    rho = []
    for lag in (1, 2):
        idx = np.arange(len(u) - lag)
        v = np.ones(len(u) - lag, dtype=bool)
        for j in range(lag + 1):
            v &= same[idx + j]
        prod = u[idx] * u[idx + lag]
        v &= np.isfinite(prod)
        var0 = np.nanvar(u[ok])
        rho.append(float(np.mean(prod[v]) / var0) if v.any() else 0.0)
    kappa = 1.0 + 2.0 * sum(r * r for r in rho)
    nu = (n_i.to_numpy() - 1.0) / kappa

    # bias-corrected log variance
    e = np.log(np.clip(s2_i.to_numpy(), 1e-12, None)) - (digamma(nu / 2) - np.log(nu / 2))
    trig = polygamma(1, nu / 2)

    # remove the band profile log sigma^2(zbar) by quantile-band demeaning
    zb = zbar.reindex(s2_i.index).to_numpy()
    edges = np.quantile(zb, np.linspace(0, 1, n_bands + 1))
    band = np.clip(np.searchsorted(edges, zb, side="right") - 1, 0, n_bands - 1)
    e_hat = e.copy()
    per_band = []
    for b in range(n_bands):
        m = band == b
        if m.sum() < 5:
            continue
        e_hat[m] = e[m] - e[m].mean()
        s2_b = max(0.0, float(np.var(e[m], ddof=1) - trig[m].mean()))
        per_band.append((float(np.median(zb[m])), np.sqrt(s2_b), int(m.sum())))
    s2 = max(0.0, float(np.sum(e_hat ** 2) / (len(e_hat) - n_bands) - trig.mean()))
    return dict(s=float(np.sqrt(s2)), n_entities=int(len(n_i)), kappa=float(kappa),
                per_band=per_band,
                entities=pd.DataFrame({"e_hat": e_hat, "trig": trig},
                                      index=s2_i.index))


def estimate_mix_b(df: pd.DataFrame, s1: float, min_changes: int = 8) -> float:
    """Mix exponent b for --mix-hetero, identified from the s(h) HORIZON moment
    (2c): temperament dispersion measured from NON-OVERLAPPING h-week changes.
    At long h the permanent component dominates the change variance, so
    s(h)/s(1) ~ the relative dispersion of the permanent multiplier, i.e. b.
    h* = longest of (13, 8, 4) with >= min_changes non-overlapping changes.
    Approximation declared: the mapping ignores the (small) residual fast
    share at h*; clip to [0, 1.5].  Never tuned to any scorecard metric."""
    if s1 <= 0:
        return 0.0
    T = int(df["period"].max()) + 1
    for h in (13, 8, 4):
        if T // h >= min_changes + 1:
            sub = df[df["period"] % h == 0].copy()
            sub["period"] //= h
            sh = estimate_temperament(sub, min_changes=min_changes)["s"]
            return float(np.clip(sh / s1, 0.0, 1.5))
    return 0.0


def _sqw(sqv: np.ndarray, b: float, s: float) -> np.ndarray:
    """sqrt of the permanent multiplier w = v^b / E[v^b] from the movement
    multiplier's sqrt (sqv = sqrt(v_i)); E[v^b] = exp(b(b-1)s^2/2) analytic for
    the lognormal draws (E[w] = 1 preserves band-level permanent variance)."""
    if b <= 0:
        return np.ones_like(sqv)
    return sqv ** b / np.exp(b * (b - 1.0) * s * s / 4.0)


def eb_vhat(df: pd.DataFrame, s: float | None = None, min_changes: int = 8) -> pd.Series:
    """Empirical-Bayes SHRUNKEN per-entity temperament multipliers v_hat_i.

    For each entity, its band-demeaned bias-corrected log change-variance e_hat
    is shrunk toward 0 (the band average) by exactly its sampling noise:
        log v_hat_i = s^2 / (s^2 + trig_i) * e_hat_i
    (posterior mean under the lognormal mixing model).  Entities with fewer
    than min_changes observed changes get v_hat = 1 (pure prior).  The result
    is renormalized to mean 1 so band-level variance is preserved.

    Used for CONDITIONAL forecasting: initialize the simulated cohort with the
    real entities' own multipliers instead of random draws.  Estimated on
    TRAIN data only in OOS settings (pass the train panel)."""
    t = estimate_temperament(df, min_changes=min_changes)
    ent = t.get("entities")
    s_use = t["s"] if s is None else s
    if ent is None or s_use <= 0:
        return pd.Series(dtype=float)
    s2 = s_use ** 2
    logv = s2 / (s2 + ent["trig"].to_numpy()) * ent["e_hat"].to_numpy()
    v = np.exp(logv)
    v = v / v.mean()
    return pd.Series(v, index=ent.index)


# --------------------------------------------------------------------------- #
# Simulation
# --------------------------------------------------------------------------- #
def _interp(z, p: RankParams, arr):
    return np.interp(z, p.z_knots, arr, left=arr[0], right=arr[-1])


def _tdraw(rng, df: float, n: int) -> np.ndarray:
    """Unit-variance innovation draw: Gaussian, or scaled Student-t (df finite)."""
    if not np.isfinite(df):
        return rng.standard_normal(n)
    return rng.standard_t(df, n) / np.sqrt(df / (df - 2.0))


def simulate(p: RankParams, T: int, seed: int = 0, *, use_factor=True, use_exit=True,
             factor_head_damp: float = 1.0, kappa: float | None = None, track: int = 4000,
             top_record: int | None = None) -> dict:
    # kappa: None -> per-knot MD-estimated kappa_z when available, else p.kappa
    rng = np.random.default_rng(seed)
    N = p.N
    mu = _extend(p.w0, N)                       # permanent level
    home = mu.copy()                            # OU anchor (rank "home")
    xi = np.zeros(N)                            # transitory deviation
    ids = np.arange(N, dtype=np.int64)
    next_id = N
    burn = 40
    # persistent entity volatility multiplier ("temperament"): sqrt(v_i) scales
    # the MOVEMENT components (sigma_trans, sigma_obs), log v_i ~ N(-s^2/2, s^2),
    # E[v_i]=1 -- band-level variance (the Eulerian structure) is preserved by
    # construction.  NOTE: the s(h) horizon moment says heterogeneity extends
    # to sigma_perm as well (s flat in h), but scaling sigma_perm by the raw
    # lognormal tail explodes held-out displacement when sigma_perm comes from
    # short train windows (reddit OOS 0.254 -> 0.404); movement-only scaling is
    # the operational spec until the mixing tail is EB-shrunken / the panel is
    # longer.  See MODEL_STATUS.md.
    ts = p.temper_s
    sqv = (np.sqrt(rng.lognormal(-0.5 * ts * ts, ts, N)) if ts > 0
           else np.ones(N))
    # permanent-share heterogeneity (--mix-hetero): w_i = v_i^b / E[v^b]
    mix_b = getattr(p, "mix_b", 0.0)
    sqw = _sqw(sqv, mix_b, ts) if (ts > 0 and mix_b > 0) else 1.0

    # stationary common level (--stat-factor): F applied as an AR(1) level at
    # OBSERVATION, innovation scaled so sd(dL) reproduces the measured sigma_F;
    # None = legacy (F integrates into mu).  Same rng draw either way.
    rho_L = getattr(p, "factor_rho", None)
    L = 0.0
    # medium-timescale transitory (--two-scale); rng draws gated so legacy
    # streams are untouched when the component is absent/zero
    st2_arr = getattr(p, "sigma_trans2", None)
    two = st2_arr is not None and np.any(np.asarray(st2_arr) > 0)
    xi2 = np.zeros(N) if two else 0.0

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
        if use_factor and rho_L is not None:
            L = rho_L * L + F * np.sqrt((1.0 + rho_L) / 2.0)
            F = 0.0   # nothing integrates into mu
        # OU confinement of the permanent level toward each entity's home rank
        # (finite long-run rank-band width => caps VR and rank autocorrelation).
        # kappa=None -> per-knot MD-estimated kappa_z when available.
        kap = (_interp(zp, p, p.kappa_z) if (kappa is None and p.kappa_z is not None)
               else (p.kappa if kappa is None else kappa))
        if np.any(np.asarray(kap) > 0):
            mu = mu - kap * (mu - home)
        mu = mu + lam * F + sp * sqw * rng.standard_normal(N)
        xi = phi * xi + st * sqv * _tdraw(rng, p.t_df, N)
        if two:
            ph2 = _interp(zp, p, p.phi2)
            st2 = _interp(zp, p, st2_arr)
            # Gaussian innovations (declared): excess kurtosis lives in the
            # fast component; the medium one aggregates within-entity swings
            xi2 = ph2 * xi2 + st2 * sqv * rng.standard_normal(N)

        if use_exit:
            ex = rng.random(N) < _interp(zp, p, p.exit_rate)
            ne = int(ex.sum())
            if ne:
                seed_mu = p.bottom_mu[rng.integers(0, p.bottom_mu.size, ne)]
                mu[ex] = seed_mu
                home[ex] = seed_mu
                xi[ex] = 0.0
                if two:
                    xi2[ex] = 0.0
                ids[ex] = np.arange(next_id, next_id + ne)
                next_id += ne
                if ts > 0:  # fresh temperament for reborn entities
                    sqv[ex] = np.sqrt(rng.lognormal(-0.5 * ts * ts, ts, ne))
                    if mix_b > 0:
                        sqw[ex] = _sqw(sqv[ex], mix_b, ts)

        if t < 0:
            continue
        # observe and rank by true level + iid measurement noise (lowers RACF
        # without adding permanent variance), matching v4.3's rank-by-observed.
        # The stationary common level (if active) enters at observation only.
        X = mu + xi + (xi2 if two else 0.0) + so * sqv * rng.standard_normal(N)
        if rho_L is not None and use_factor:
            X = X + lam * L
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
    score_k: top-coverage universe boundary.  When set, the goal-1 metrics are
    computed on the ESTIMAND population only -- tracked entities whose time-mean
    observed rank is <= score_k (applied identically to empirical and simulated
    structures) -- so the score measures the top-K and is invariant to the
    buffer depth B except through genuine boundary effects.  Also adds the
    boundary-flux diagnostics."""
    if score_k is not None:
        with np.errstate(invalid="ignore"):
            rf = np.where(ranks > 0, ranks.astype(float), np.nan)
            mean_rank = np.nanmean(rf, axis=0)
        in_k = np.isfinite(mean_rank) & (mean_rank <= score_k)
        if in_k.sum() >= 10:
            values, ranks = values[:, in_k], ranks[:, in_k]
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
def run_platform(name: str, reps: int = 5, obs_frac: float = 0.4, kappa: float | None = None,
                 top_k_u: int | None = None, buffer_mult: int = 4,
                 temper: bool = False, min_knot_n: int | None = None,
                 md_lags: int | None = None, t_tails: bool = False,
                 spec_b: bool = False, md_vr: bool = False,
                 stat_factor: bool = False, two_scale: bool = False,
                 mix_hetero: bool = False, mix_b_fix: float | None = None,
                 **sim_kw) -> dict:
    # kappa None: hand-set legacy default 0.15 UNLESS the MD estimator supplies
    # a per-knot kappa_z (then the simulator uses that -- one less knob)
    if kappa is None and md_lags is None:
        kappa = 0.15
    cfg = PLATFORMS[name]
    df = load_panel(cfg)
    if top_k_u:
        df = restrict_universe(df, top_k_u, buffer_mult=buffer_mult)
    score_k = df.attrs.get("score_k")
    T = int(df["period"].max()) + 1
    mean_n = df.groupby("period").size().mean()
    # persistence-set size follows the ESTIMAND (top-K universe), not the
    # buffer depth, so scores are comparable across buffer sizes
    top_k = max(10, int(round(0.01 * (score_k if score_k else mean_n))))
    uni = (f" universe=top-{score_k} (buffer B={df.attrs['universe_B']})"
           if score_k else "")
    opts = ((" temper" if temper else "") + (f" pool>={min_knot_n}" if min_knot_n else "")
            + (f" md{md_lags}" if md_lags else "") + (" t-tails" if t_tails else "")
            + (" spec-B" if spec_b else "") + (" vr-mom" if md_vr else "")
            + (" stat-factor" if stat_factor else "")
            + (" two-scale" if two_scale else "")
            + (" mix-b" if mix_hetero else ""))
    print(f"\n{'='*72}\n{name.upper()}  | periods={T} mean_N={mean_n:.0f} "
          f"entities={df['entity_id'].nunique():,} top_k={top_k}{uni}{opts}\n{'='*72}")

    ev, er, et, ers = empirical_structures(df, top_k, topid_k=score_k)
    emp = diagnostics(ev, er, et, ers, top_k, score_k=score_k)

    sigma_obs_fix = None
    if spec_b:
        import spec_b_sigma_obs as sb
        daily_path = cfg.get("daily_path", sb.DAILY_PATH if name == "reddit" else None)
        if daily_path is None:
            raise SystemExit(f"--spec-b requires daily data (none wired for {name})")
        daily = sb.load_daily(set(df["entity_id"].unique()), path=daily_path,
                              day_guard=cfg.get("day_guard", False))
        cur = sb.spec_b_curve(df, daily)
        sigma_obs_fix = (cur["z"], cur["sigma_obs"])
        print(f"  Spec-B sigma_obs (daily noise floor, Toeplitz-corrected): "
              f"{cur['sigma_obs'][0]:.3f}..{cur['sigma_obs'][-1]:.3f} (head..tail)")
    p = estimate(df, obs_frac=obs_frac, temper=temper, min_knot_n=min_knot_n,
                 md_lags=md_lags, t_tails=t_tails, sigma_obs_fix=sigma_obs_fix,
                 md_vr=md_vr, stat_factor=stat_factor, two_scale=two_scale,
                 mix_hetero=mix_hetero, mix_b_fix=mix_b_fix)
    if mix_hetero:
        tag = " (IMPOSED -- restriction test)" if mix_b_fix is not None else \
              " (s(h*)/s(1) horizon moment)"
        print(f"  mix heterogeneity: b = {p.mix_b:.2f}{tag}; "
              f"permanent spread = b*s = {p.mix_b * p.temper_s:.2f}")
    if two_scale:
        print(f"  two-scale: phi2 = {p.phi2[0]:.2f}..{p.phi2[-1]:.2f}  "
              f"sigma_trans2 = {p.sigma_trans2[0]:.3f}..{p.sigma_trans2[-1]:.3f} "
              f"(top..tail; medium timescale, D(h)-identified)")
    if stat_factor:
        print(f"  stationary common factor: rho_L = {p.factor_rho:.2f} "
              f"(measured from the detrended cumulative-F path; legacy = integrated)")
    if temper:
        print(f"  temperament: s = {p.temper_s:.3f} "
              f"(sigma_i spread p90/p10 = {np.exp(1.2816 * p.temper_s):.2f}x)")
    if md_lags:
        print(f"  MD partition: kappa(z) = {p.kappa_z[0]:.3f}..{p.kappa_z[-1]:.3f} "
              f"(top..tail, estimated -- hand-set kappa retired)")
    if t_tails:
        print(f"  transitory tails: t_df = {p.t_df:.1f}"
              + ("  (Gaussian -- no excess within-entity kurtosis)" if not np.isfinite(p.t_df) else ""))
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
    ap.add_argument("--kappa", type=float, default=None,
                    help="permanent home-reversion strength (default: 0.15 legacy, "
                         "or the MD-estimated kappa_z when --md-lags is set)")
    ap.add_argument("--md-lags", type=int, default=None,
                    help="minimum-distance fit of gamma_0..gamma_L (OU home; estimates "
                         "kappa and sigma_obs, retiring the kappa/obs-frac knobs); try 6")
    ap.add_argument("--t-tails", action="store_true",
                    help="Student-t transitory innovations (df from within-entity kurtosis)")
    ap.add_argument("--md-vr", action="store_true",
                    help="append multi-horizon change-variance moments D(2,4,8,13) to the "
                         "MD fit -- identifies kappa where the gamma tail is flat")
    ap.add_argument("--stat-factor", action="store_true",
                    help="STATIONARY common factor: apply F as a measured AR(1) level at "
                         "observation instead of integrating it into mu (the empirical "
                         "platform level mean-reverts; integration inflates VR at h>=4)")
    ap.add_argument("--two-scale", action="store_true",
                    help="second (medium-timescale) transitory component, identified "
                         "from the D(h) moments (requires --md-lags and --md-vr)")
    ap.add_argument("--mix-hetero", action="store_true",
                    help="per-entity permanent-share heterogeneity: sigma_perm scales "
                         "by v^(b/2), b = s(h*)/s(1) horizon moment (requires --temperament)")
    ap.add_argument("--mix-b-fix", type=float, default=None,
                    help="impose the mix exponent b instead of measuring it "
                         "(the b=1 restriction test; requires --mix-hetero)")
    ap.add_argument("--spec-b", action="store_true",
                    help="pin sigma_obs to the Spec-B daily noise floor (reddit only)")
    ap.add_argument("--obs-frac", type=float, default=0.4, help="share of transitory variance treated as iid obs noise")
    ap.add_argument("--top-k", type=int, default=None,
                    help="top-coverage universe boundary K (applies to every platform listed)")
    ap.add_argument("--coverage", type=int, choices=(80, 90, 95), default=None,
                    help="resolve K per platform from the pre-registered COVERAGE_K rule")
    ap.add_argument("--buffer-mult", type=int, default=4,
                    help="universe buffer depth B = buffer_mult * K (sponge layer)")
    ap.add_argument("--temperament", action="store_true",
                    help="persistent entity-level volatility multiplier (moment-identified)")
    ap.add_argument("--min-knot-entities", type=int, default=None,
                    help="pool sparse knots' moments to cover >= this many entities")
    args = ap.parse_args()
    for plat in args.platforms:
        top_k_u = args.top_k
        if top_k_u is None and args.coverage is not None:
            top_k_u = COVERAGE_K.get(plat, {}).get(args.coverage)
            if top_k_u is None:
                raise SystemExit(f"no pre-registered K for {plat} at coverage {args.coverage}%")
        run_platform(plat, reps=args.reps, obs_frac=args.obs_frac, kappa=args.kappa,
                     top_k_u=top_k_u, buffer_mult=args.buffer_mult,
                     temper=args.temperament, min_knot_n=args.min_knot_entities,
                     md_lags=args.md_lags, t_tails=args.t_tails, spec_b=args.spec_b,
                     md_vr=args.md_vr, stat_factor=args.stat_factor,
                     two_scale=args.two_scale, mix_hetero=args.mix_hetero,
                     mix_b_fix=args.mix_b_fix,
                     use_factor=not args.no_factor, use_exit=not args.no_exit,
                     factor_head_damp=args.factor_head_damp)
