#!/usr/bin/env python3
"""Common Rank-Diffusion Model (Stage 2 unified parsimonious core, opt-in module).

Replaces ~500 knot-level parameters of Model A / movinghome_vp with 12 interpretable,
moment-identified scalars shared across platforms (Facebook, Instagram apparatus,
Reddit comments & submissions, and future panels):

    X_it = L_it + xi_it + eps_it     (observed when O_it = 1, intermittent absence p_abs(z))

    L_{i,t+1}  = L_it + g(r_it) + sigma_L(L_it) * v_i^(gamma_L/2) * eta^L_it
    xi_{i,t+1} = phi * xi_it    + sigma_xi(z_it) * v_i^(1/2)      * eta^xi_it
    eps_it     = sigma_obs(z_it) * v_i^(1/2)                      * eta^eps_it

where:
  - sigma_L(L)   = sigma_L0 * exp(-beta * clip(L - L_K, u_lo, u_hi))          [2 scalars]
  - phi          = pooled passing AR(1) persistence                           [1 scalar]
  - log sqrt(V_xi(z))  = a_xi  + b_xi  * (z - z_K)                            [2 scalars]
  - log sigma_obs(z)   = a_obs + b_obs * (z - z_K)  (pinned to Spec-B if given)[2 scalars]
  - s            = lognormal entity volatility spread, split-half identified   [1 scalar]
  - gamma_L      = lasting-vs-passing volatility coupling (b = s_long/s_short) [1 scalar]
  - logit p_abs(z) = a_abs + b_abs * (z - z_K)  (non-terminal intermittency)   [2 scalars]
  - lam_exit     = true bottom-buffer permanent attrition rate                 [1 scalar]
  Total: 12 scalars (10 when Spec-B pins sigma_obs).

Key architectural fixes over Model A / rankdiff_rdm:
  1. Split-half covariance estimator for s (unbiased at true s = 0; fixes V2 bias).
  2. Smooth all-rank balancing drift g(r) from r = 1..N (no rank-20 freeze).
  3. Bounded empirical standardized innovations (eliminates expm1 blowups).
  4. Deconvolved latent starting ladder L0 (eliminates ad-hoc c = 0.96 patch).
  5. Explicit separation of intermittent absence from permanent death.
  6. Calendar-aligned 2-state Kalman filter with empirical-Bayes per-entity v_i
     for individual endpoint forecasts of rank, relative level, and audience share.

Does NOT modify minimal_rankdiff.py, rankdiff_kalman.py, or any frozen default.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace

import numpy as np
from scipy.optimize import nnls

Z_CLIP = 1e-6
PHI_GRID = np.array([0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.72, 0.78, 0.84, 0.89, 0.93, 0.96])
SURGE_HL_GRID = np.array([8.0, 13.0, 26.0, 39.0, 52.0])
DRIFT_HL_FIXED = 52.0  # 1-year persistence for Stage 3 entity drift (supported by multi-gap decay in D3)
SURGE_EXP_CAP = 5.0    # Upper cap on unit-exponential surge draws (prevents extreme expm1 blowups)
DRIFT_Z_MIN = 2.0      # Round 8/9 entity-clustered SE threshold for activating persistent drift

# Exact raw moments E[min(Exp(1), C)^k] = k! * (1 - e^-C * sum_{l=0}^{k-1} C^l / l!)
_EC = math.exp(-SURGE_EXP_CAP)
CAP_M1 = 1.0 * (1.0 - _EC)
CAP_M2 = 2.0 * (1.0 - _EC * (1.0 + SURGE_EXP_CAP))
CAP_M3 = 6.0 * (1.0 - _EC * (1.0 + SURGE_EXP_CAP + 0.5 * SURGE_EXP_CAP ** 2))
CAP_M5 = 120.0 * (1.0 - _EC * sum((SURGE_EXP_CAP ** l) / math.factorial(l) for l in range(5)))


@dataclass
class CommonParams:
    # Core 11 scalars (12 if gamma_L > 0)
    sigma_L0: float          # lasting weekly SD at rank K (level L_K)
    beta: float              # size-volatility exponent for lasting SD
    phi: float               # pooled passing AR(1) persistence
    a_xi: float              # log sqrt(V_xi) at z_K
    b_xi: float              # slope of log sqrt(V_xi) in (z - z_K)
    a_obs: float             # log sigma_obs (or log excess sigma_obs when Spec-B pinned) at z_K
    b_obs: float             # slope of log sigma_obs (or log excess) in (z - z_K)
    s_vol: float             # entity volatility log-variance SD (split-half identified)
    gamma_L: float           # exponent coupling v_i to lasting variance (0..1)
    a_abs: float             # logit weekly intermittent absence rate at z_K
    b_abs: float             # slope of logit absence in (z - z_K)
    lam_exit: float          # weekly permanent attrition rate (bottom half of buffer)

    # Reference coordinates & empirical non-free inputs (0 tuned degrees of freedom)
    K: int                   # scored universe size
    N: int                   # full simulated universe size (typically ~4K)
    L_K: float               # reference log-level at rank K
    z_K: float               # reference log-rank at rank K: log((K-0.5)/N)
    u_lo: float              # lower clamp on (L - L_K) from estimation bands
    u_hi: float              # upper clamp on (L - L_K) from estimation bands
    drift_z: np.ndarray      # z grid for smooth rank-balancing drift g(r)
    drift_g: np.ndarray      # weekly rank-balancing drift g(z)
    bottom_pool: np.ndarray  # empirical bottom-buffer levels for permanent rebirth
    innov_pool: np.ndarray   # standardized, zero-mean unit-var empirical innovations (bounded)
    ladder_obs: np.ndarray   # observed smoothed/first-block ladder (length N, descending)
    spec_b_pinned: bool = False
    a_obs_floor: float = -20.0   # exact Spec-B lower-bound log-SD at z_K (active when spec_b_pinned)
    b_obs_floor: float = 0.0     # exact Spec-B lower-bound slope in (z - z_K)
    innov_mode: str = "empirical"  # "empirical" or "gaussian"

    # Optional Stage 3 asymmetry fields (OFF by default in Stage 2: all 0.0 -> bit-identical Stage 2!)
    surge_lam: float = 0.0       # weekly arrival prob of positive transient surge
    surge_mu: float = 0.0        # mean exponential jump size of surge
    surge_rho: float = 0.0       # weekly decay factor of surge
    surge_b: float | None = None # optional rank-scaling slope for surge amplitude (None -> inherit b_xi)
    decline_var: float = 0.0     # variance of persistent entity drift at rank K
    decline_rho: float = 0.0     # weekly persistence of entity drift (1.0 = fixed, <1 = AR(1))
    decline_b: float = 0.0       # rank-scaling slope for persistent drift SD: exp(decline_b * min(z - z_K, 0))
    decline_skew: float = 0.0    # asymmetry parameter (0 = symmetric, >0 = negatively skewed / decline-state)
    decline_enter: float = 0.0   # weekly hazard of entering decline state (if state-switching)
    decline_exit: float = 0.0    # weekly hazard of leaving decline state (0 = absorbing to exit)
    decline_delta: float = 0.0   # weekly drift rate inside decline state

    # Round 5-6 rare permanent jumps on the lasting level (inert when jump_lam == 0)
    jump_lam: float = 0.0        # weekly probability of a lasting jump (0.02 meta, 0.01 robust)
    jump_c: float = 0.0          # per-jump variance at the s_sc normalization: Var(J | z) = jump_c * s_sc(z)^2
    jump_dist: str = "gaussian"  # "gaussian" (Round 6 meta default) or "laplace" (Round 5 opt-in robust)

    diag: dict | None = None

    def n_free_scalars(self) -> int:
        # 11 core scalars when gamma_L == 0 (default):
        # sigma_L0, beta, phi, a_xi, b_xi, a_obs, b_obs, s_vol, a_abs, b_abs, lam_exit
        base = 11 + (1 if self.gamma_L > 0 else 0)
        extra = 0
        if self.surge_lam > 0:
            extra += 3 + (1 if self.surge_b is not None else 0)  # surge_lam, surge_mu, surge_rho [+ surge_b]
        if self.decline_var > 0:
            extra += 1 + (1 if self.decline_b != 0.0 else 0) + (1 if self.decline_skew != 0 else 0)  # sigma_d [+ decline_b] [+ skew]
        if self.decline_delta > 0:
            extra += 2 + (1 if self.decline_exit > 0 else 0)
        if self.jump_lam > 0 and self.jump_c > 0:
            extra += 1  # jump_c from the lasting-variance gap moment (rate fixed cross-platform, rank law inherited)
        return base + extra


# --------------------------------------------------------------------------- #
# Helper functions: profiles & math
# --------------------------------------------------------------------------- #
def _sigmoid(x: np.ndarray | float) -> np.ndarray | float:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -25.0, 25.0)))


def _logit(p: np.ndarray | float) -> np.ndarray | float:
    pc = np.clip(p, 1e-5, 1.0 - 1e-5)
    return np.log(pc / (1.0 - pc))


def sigma_L_of(p: CommonParams, L: np.ndarray) -> np.ndarray:
    u = np.clip(L - p.L_K, p.u_lo, p.u_hi)
    return p.sigma_L0 * np.exp(-p.beta * u)


def v_xi_of(p: CommonParams, z: np.ndarray) -> np.ndarray:
    dz = np.clip(z - p.z_K, -8.0, 2.0)
    return np.exp(2.0 * np.clip(p.a_xi + p.b_xi * dz, -6.0, 3.0))


def sigma_obs_of(p: CommonParams, z: np.ndarray) -> np.ndarray:
    dz = np.clip(z - p.z_K, -8.0, 2.0)
    base = np.exp(np.clip(p.a_obs + p.b_obs * dz, -8.0, 3.0))
    if p.spec_b_pinned:
        fl = np.exp(np.clip(p.a_obs_floor + p.b_obs_floor * dz, -8.0, 3.0))
        return np.sqrt(fl * fl + base * base)
    return base


def p_abs_of(p: CommonParams, z: np.ndarray) -> np.ndarray:
    dz = np.clip(z - p.z_K, -8.0, 2.0)
    return _sigmoid(p.a_abs + p.b_abs * dz)


def _wls_line(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> tuple[float, float]:
    ok = np.isfinite(x) & np.isfinite(y) & np.isfinite(w) & (w > 0)
    if ok.sum() < 2:
        val = float(np.nanmedian(y[ok])) if ok.any() else 0.0
        return val, 0.0
    xo, yo, wo = x[ok], y[ok], np.sqrt(w[ok])
    A = np.column_stack([np.ones_like(xo), xo]) * wo[:, None]
    coef = np.linalg.lstsq(A, yo * wo, rcond=None)[0]
    return float(coef[0]), float(coef[1])


def _band_edges(K: int, N: int) -> np.ndarray:
    e = np.array([0, K / 64, K / 32, K / 16, K / 8, K / 4, K / 2, K, max(N, K + 1)], dtype=float)
    return np.maximum.accumulate(e)


def _top_k_center(X: np.ndarray, R: np.ndarray, K: int) -> np.ndarray:
    """Per-period median of X among currently ranked top-K entities (immune to 3K buffer drift)."""
    T = X.shape[0]
    c = np.empty(T, dtype=np.float64)
    for t in range(T):
        in_k = np.isfinite(R[t]) & (R[t] <= K) & np.isfinite(X[t])
        if in_k.any():
            c[t] = float(np.median(X[t, in_k]))
        else:
            f = np.isfinite(X[t])
            c[t] = float(np.median(X[t, f])) if f.any() else 0.0
    return c[:, None]


IQR_TO_SD = 1.349  # Gaussian IQR / SD; cancels out of the robust moment condition (see _model_increment_ratio)
# The robust scale of an h-week increment measures TYPICAL movement only while rare regime jumps stay
# rare over the horizon (lambda_J * h << 1). D1 (Round 5) found the empirical var / robust-var ratio
# flat through h = 13 on all four panels and falling 15-30% by h = 52 as jumps enter the central half
# of the distribution. The robust moment is therefore declared on h = 1..13 (one quarter); longer lags
# keep informing the mean-square fit that is stored beside it in diag for the jump-share accounting.
ROBUST_H_MAX = 13
# Round 5 S2+J: rare permanent lasting jumps on the core (asymmetry=False) model. The weekly rate is a
# cross-platform CONSTANT fixed from the D1 diagnostic (the var/robust-var ratio of h-increments is flat
# through h = 13 and has fallen 15-30% by h = 52 on all four panels, i.e. lambda*52 ~ 0.3-0.7), in the
# same way DRIFT_HL_FIXED is fixed from the multi-gap decay diagnostic; profiling it per panel on the
# long-horizon IQR profile was tried and is not reliably identified (grid-edge solutions, EXPERIMENT_LOG).
# The per-jump variance is identified by the lasting-variance gap between the mean-square and the robust
# core fits (one counted scalar); the rank law is inherited from the passing slope b_xi (0 scalars).
# Jumps are instantiated only when the gap carries at least JUMP_MIN_SHARE of total weekly lasting variance
# (planted jump-free panels with degenerate 3-8-entity head bands show noise gaps of 0.15-0.17; the four
# development panels carry 0.39-0.98; a planted 64%-share panel measures 0.24). An additional "must improve
# the all-horizon IQR-profile fit by 25%" rule was tried and rejected: it fires on jump-free small-band panels
# and misses planted jumps (EXPERIMENT_LOG); the fit of that profile is kept in diag for the record only.
JUMP_LAM_FIXED = 0.02   # Round 6 meta Gaussian-jump rate (2%/wk); Fable opt-in 'robust' uses 0.01
JUMP_LAM_ROBUST = 0.01
JUMP_MIN_SHARE = 0.20


def _structure_function(X: np.ndarray, band_of: np.ndarray, nb: int, H: int, robust: bool = False) -> np.ndarray:
    """D_b(h) = E[Var_b(X_{t+h} - X_t)] by permanent-rank band, removing within-band common
    time/tier shifts with exact Bessel correction n_t / (n_t - 1).

    robust=True returns the ROBUST scale of the same within-band demeaned increments,
    (IQR / 1.349)^2 x n/(n-1), i.e. the variance a Gaussian with that interquartile range would
    have. It is the declared moment of the Round 5 core fit: the typical (central) h-week movement,
    insensitive to the rare regime jumps that dominate the mean square (Round 5 D1 diagnostic)."""
    T = X.shape[0]
    D = np.full((nb, H), np.nan)
    band_idx = [np.flatnonzero(band_of == b) for b in range(nb)]
    for b, idx in enumerate(band_idx):
        if idx.size < 5:
            continue
        Xb = X[:, idx]
        for h in range(1, H + 1):
            if h >= T:
                break
            d = Xb[h:] - Xb[:-h]
            fin = np.isfinite(d)
            cnt = np.sum(fin, axis=1)
            ok_t = cnt >= 5
            if not ok_t.any():
                continue
            d_ok = d[ok_t]
            cnt_ok = cnt[ok_t].astype(np.float64)
            with np.errstate(all="ignore"):
                mu = np.nanmean(d_ok, axis=1, keepdims=True)
            dm = d_ok - mu
            df = float(np.sum(cnt_ok - 1.0))
            if df < 35.0:
                continue
            if robust:
                v = dm[np.isfinite(dm)]
                q25, q75 = np.quantile(v, [0.25, 0.75])
                D[b, h - 1] = ((q75 - q25) / IQR_TO_SD) ** 2 * (float(np.sum(cnt_ok)) / df)
            else:
                ss = float(np.nansum(dm * dm))
                D[b, h - 1] = ss / df
    return D


def _empirical_innov_pool_quantiles(Xc: np.ndarray, pr_ord: np.ndarray, pres: np.ndarray, K: int) -> np.ndarray | None:
    """Deterministic 4096-point quantile grid (0.05%..99.95%) of per-entity standardized 1-week
    increments of present top-K entities (unstandardized overall; None when < 500 values)."""
    dX1 = np.diff(Xc, axis=0)
    dX1 = dX1 - np.nanmean(dX1, axis=1, keepdims=True)
    in_k_ent = (pr_ord <= K) & (pres >= 0.5)
    dx_in = dX1[:, in_k_ent]
    sd_i = np.nanstd(dx_in, axis=0)
    ok_u = np.isfinite(sd_i) & (sd_i > 1e-4)
    u = dx_in[:, ok_u] / sd_i[ok_u][None, :]
    uf = u[np.isfinite(u)]
    if uf.size >= 500:
        qs = np.linspace(0.0005, 0.9995, 4096)
        return np.quantile(uf, qs)
    return None


def _split_half_vol_spread(Xc: np.ndarray, pr_ord: np.ndarray, K: int, N: int, step: int = 1) -> float:
    """Bias-free split-half covariance estimator of log-variance spread s^2.

    For each top-K entity with enough observations in BOTH halves of the panel,
    compute log(hat_var_1) and log(hat_var_2), and residualize smoothly against
    a quadratic in z_i = log((pr_ord - 0.5)/N) to remove the continuous
    rank-volatility profile. Because sampling noise in disjoint halves is
    independent, Cov(resid_1, resid_2) = Var(log v_i) = s^2 is EXACTLY 0
    when true s = 0, regardless of kurtosis or short-lag autocorrelation!
    """
    T = Xc.shape[0]
    d = Xc[step:] - Xc[:-step]
    d = d - np.nanmean(d, axis=1, keepdims=True)
    # Leave a gap of `step` weeks between halves so increments do not overlap
    mid = d.shape[0] // 2
    gap = max(step, 2)
    d1 = d[:max(1, mid - gap // 2)]
    d2 = d[min(d.shape[0] - 1, mid + (gap + 1) // 2):]
    if d1.shape[0] < 6 or d2.shape[0] < 6:
        return 0.0
    n1 = np.sum(np.isfinite(d1), axis=0)
    n2 = np.sum(np.isfinite(d2), axis=0)
    with np.errstate(all="ignore"):
        v1 = np.nanmean(d1 * d1, axis=0)
        v2 = np.nanmean(d2 * d2, axis=0)
    min_n = max(5, min(10, d1.shape[0] // 2))
    ok = (pr_ord <= K) & (n1 >= min_n) & (n2 >= min_n) & (v1 > 1e-8) & (v2 > 1e-8)
    if ok.sum() < 50:
        return 0.0
    l1 = np.log(v1[ok])
    l2 = np.log(v2[ok])
    z_ok = np.log(np.clip((pr_ord[ok] - 0.5) / N, Z_CLIP, 1.0))
    A = np.column_stack([np.ones_like(z_ok), z_ok, z_ok * z_ok])
    r1 = l1 - A @ np.linalg.lstsq(A, l1, rcond=None)[0]
    r2 = l2 - A @ np.linalg.lstsq(A, l2, rcond=None)[0]
    cov = float(np.mean(r1 * r2))
    return float(math.sqrt(max(cov, 0.0)))


def _smooth_ladder_slope(ladder_desc: np.ndarray, grid_r: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Cubic polynomial in log(rank) fit to the descending ladder; returns (slope S(r), level(r))."""
    N = ladder_desc.size
    r = np.arange(1, N + 1, dtype=float)
    lr = np.log(r)
    # Weight slightly more toward head/body so top-K slope is accurate
    w = 1.0 / np.sqrt(r)
    c = np.polyfit(lr, ladder_desc, 3, w=w)
    dc = np.polyder(c)
    lg = np.log(np.clip(grid_r, 1.0, float(N)))
    S = np.clip(-np.polyval(dc, lg), 0.15, None)
    lev = np.polyval(c, lg)
    return S, lev


def _closed_form_atlas_drift(sigma_L0: float, beta: float, L_K: float, u_lo: float, u_hi: float,
                             ladder_desc: np.ndarray, N: int, jump_var_fn=None) -> tuple[np.ndarray, np.ndarray]:
    """Closed-form Atlas balance drift g(k) = -d(k s_k)/dk with s_k = sigma_L^2(k) / (2 S_k)
    (sigma_L^2 is the TOTAL weekly lasting variance: diffusive core plus jump variance when active)."""
    k = np.unique(np.round(np.geomspace(1, N, 64)).astype(int)).astype(float)
    S, lev = _smooth_ladder_slope(ladder_desc, k)
    u = np.clip(lev - L_K, u_lo, u_hi)
    sig2 = (sigma_L0 * np.exp(-beta * u)) ** 2
    if jump_var_fn is not None:
        sig2 = sig2 + jump_var_fn(np.log(np.clip((k - 0.5) / N, Z_CLIP, 1.0)))
    s_k = sig2 / (2.0 * S)
    F = k * s_k
    km = 0.5 * (k[1:] + k[:-1])
    g = -np.diff(F) / np.diff(k)
    # Extend cleanly to rank 1 and rank N
    k_all = np.concatenate([[1.0], km, [float(N)]])
    g_all = np.concatenate([[g[0]], g, [g[-1]]])
    z_all = np.log(np.clip((k_all - 0.5) / N, Z_CLIP, 1.0))
    return z_all, g_all


def _refine_drift_smooth(p: CommonParams, L0: np.ndarray, iters: int = 6, weeks: int = 52,
                         seeds: tuple[int, ...] = (101, 102, 103, 104), damp: float = 0.65) -> tuple[np.ndarray, np.ndarray, list[float]]:
    """Smooth all-rank fixed-point refinement of drift g(z) from rank 1 to N.

    Unlike rankdiff_rdm.drift_hold_numeric (which freezes drift above k_min = 20),
    this projects the simulated ladder drift onto a smooth 7-knot piecewise-linear
    basis in log(rank) spanning rank 1..N, so ranks 1..20 receive regularized,
    noise-free balancing drift without freezing the head.
    """
    L_start = np.sort(np.asarray(L0, dtype=float))[::-1]
    N = L_start.size
    zq = p.drift_z.copy()
    g = p.drift_g.copy()
    kq = np.clip(np.round(np.exp(zq) * N + 0.5).astype(int), 1, N)
    # Smooth knot basis in log(r): 7 knots from rank 1 to N
    knot_lr = np.linspace(0.0, math.log(N), 7)
    lr_q = np.log(kq.astype(float))
    # Piecewise-linear hat basis B (len(kq) x 7)
    B = np.zeros((kq.size, knot_lr.size))
    for j in range(knot_lr.size):
        if j == 0:
            B[:, j] = np.clip((knot_lr[1] - lr_q) / (knot_lr[1] - knot_lr[0]), 0.0, 1.0)
        elif j == knot_lr.size - 1:
            B[:, j] = np.clip((lr_q - knot_lr[-2]) / (knot_lr[-1] - knot_lr[-2]), 0.0, 1.0)
        else:
            left = (lr_q - knot_lr[j - 1]) / (knot_lr[j] - knot_lr[j - 1])
            right = (knot_lr[j + 1] - lr_q) / (knot_lr[j + 1] - knot_lr[j])
            B[:, j] = np.clip(np.minimum(left, right), 0.0, 1.0)
    # Weights: slight downweight on rank 1..3 extreme order stat variance, still included!
    w_q = np.sqrt(np.minimum(kq.astype(float), 50.0) / 50.0)
    Bw = B * w_q[:, None]

    hist = []
    tt = np.arange(weeks, dtype=float) - (weeks - 1) / 2.0
    tt2 = float(tt @ tt)
    for _ in range(iters):
        q = replace(p, drift_g=g)
        rates = []
        for sd in seeds:
            rng = np.random.default_rng(sd)
            st = CommonState(q, L_start, rng)
            lad = np.empty((weeks, kq.size))
            for t in range(weeks):
                st.step_lasting()
                srt = np.sort(st.L)[::-1]
                lad[t] = srt[kq - 1]
            rates.append((tt @ (lad - lad.mean(axis=0))) / tt2)
        rate = np.mean(rates, axis=0)
        # Smooth the rate correction onto the 7-knot log-rank spline
        coef, *_ = np.linalg.lstsq(Bw, rate * w_q, rcond=None)
        rate_smooth = B @ coef
        hist.append(float(np.sqrt(np.mean(rate_smooth[kq <= p.K] ** 2))))
        g = g - damp * rate_smooth
    return zq, g, hist


def deconvolve_starting_ladder(p: CommonParams, target_block_ladder: np.ndarray, block: int = 26,
                               iters: int = 5, seeds: tuple[int, ...] = (201, 202, 203),
                               damp: float = 0.7) -> np.ndarray:
    """Principled order-statistic deconvolution (replaces ad-hoc scalar c = 0.96).

    Finds sorted latent starting levels L0 (length N) such that the expected sorted
    first-block mean ladder of simulated observed X = L + xi + eps matches the
    observed first-block ladder across all ranks, using a smooth rank-wise update
    pinned at rank K. Zero free parameters.
    """
    target = np.sort(np.asarray(target_block_ladder, dtype=float))[::-1]
    N = target.size
    L0 = target.copy()
    # Evaluate on log-spaced rank grid + smooth interpolation back to all N ranks
    kq = np.unique(np.round(np.geomspace(1, N, 40)).astype(int))
    lr_all = np.log(np.arange(1, N + 1, dtype=float))
    lr_q = np.log(kq.astype(float))

    for _ in range(iters):
        sim_lads = []
        for sd in seeds:
            rng = np.random.default_rng(sd)
            # Draw stationary initial passing swing xi0 ~ N(0, V_xi * v_i)
            st = CommonState(p, L0, rng)
            _, zp0 = st._ranks_and_z()
            st.xi = np.sqrt(v_xi_of(p, zp0)) * st.sqv_pass * rng.standard_normal(N)
            acc = np.zeros(N)
            for _t in range(block):
                acc += st.step(apply_absence=False)
            m = acc / block
            ms = np.sort(m)[::-1]
            # Compare relative to rank K so overall level shift does not distort shape
            sim_lads.append((ms[kq - 1] - ms[p.K - 1]) - (target[kq - 1] - target[p.K - 1]))
        err_q = np.mean(sim_lads, axis=0)
        # Smooth residual gently along log-rank (3-point moving average on interior)
        if err_q.size >= 5:
            err_s = err_q.copy()
            err_s[1:-1] = 0.25 * err_q[:-2] + 0.5 * err_q[1:-1] + 0.25 * err_q[2:]
            err_q = err_s
        corr = np.interp(lr_all, lr_q, err_q)
        L0 = np.sort(L0 - damp * corr)[::-1]
    return L0


# --------------------------------------------------------------------------- #
# Stage 3 exact closed-form moment helpers (fading surges + persistent drift)
# --------------------------------------------------------------------------- #
def _gapped_weights(W: int, H: int, g: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Exact linear weight vectors for gapped window changes (x, y) over span 2H + g."""
    T_span = 2 * H + g
    a = np.zeros(T_span, dtype=float)
    b = np.zeros(T_span, dtype=float)
    a[:W] = -1.0 / W
    a[H - W:H] = 1.0 / W
    b[H + g:H + g + W] = -1.0 / W
    b[2 * H + g - W:2 * H + g] = 1.0 / W
    # Suffix sums for integrated process U_t = sum_{u<t} d_u: sum_t a_t U_t = sum_u a_tilde_u d_u
    a_tilde = np.cumsum(a[::-1])[::-1][1:]
    b_tilde = np.cumsum(b[::-1])[::-1][1:]
    return a, b, a_tilde, b_tilde


def _G_ar1(rho: float, W: int, H: int, g: int) -> float:
    """Exact Cov(x, y) for unit-variance stationary AR(1) with persistence rho."""
    if rho <= 0.0:
        return 0.0
    a, b, _, _ = _gapped_weights(W, H, g)
    idx = np.arange(a.size)
    C = rho ** np.abs(idx[:, None] - idx[None, :])
    return float(a @ C @ b)


def _G_drift(rho_d: float, W: int, H: int, g: int) -> float:
    """Exact Cov(x, y) for integrated unit-variance AR(1) drift with persistence rho_d."""
    _, _, at, bt = _gapped_weights(W, H, g)
    idx = np.arange(at.size)
    C = rho_d ** np.abs(idx[:, None] - idx[None, :])
    return float(at @ C @ bt)


def _f_vel(hs: np.ndarray, rho_d: float) -> np.ndarray:
    """Exact structure function Var(sum_{t=0}^{h-1} d_t) / sigma_d^2 for AR(1) drift."""
    h = np.asarray(hs, dtype=float)
    if rho_d >= 1.0 - 1e-9:
        return h * h
    if rho_d <= 1e-9:
        return h
    return h * (1.0 + rho_d) / (1.0 - rho_d) - 2.0 * rho_d * (1.0 - rho_d ** h) / ((1.0 - rho_d) ** 2)


def _surge_innov_var(lam: float, mu: float) -> float:
    """Exact variance of single-week capped Bernoulli-Exponential jump J = B * mu * min(Exp(1), C)."""
    if lam <= 0.0 or mu <= 0.0:
        return 0.0
    return float((lam * CAP_M2 - (lam * lam) * (CAP_M1 * CAP_M1)) * (mu * mu))


def _surge_variance(lam: float, mu: float, rho: float) -> float:
    if lam <= 0.0 or mu <= 0.0 or rho <= 0.0:
        return 0.0
    return float(_surge_innov_var(lam, mu) / max(1.0 - rho * rho, 1e-9))


def _surge_mean(lam: float, mu: float, rho: float) -> float:
    if lam <= 0.0 or mu <= 0.0 or rho <= 0.0:
        return 0.0
    return float(lam * CAP_M1 * mu / max(1.0 - rho, 1e-9))


def _surge_scale_of(b_xi: float, z: np.ndarray, z_K: float) -> np.ndarray:
    """Rank-dependent surge amplitude scaling (0 extra scalars when b_xi inherited; +1 when surge_b fit).
    Normalized so top-K RMS scale is 1.0 when b > 0, while damping extreme head jumps."""
    b_pos = max(float(b_xi), 0.0)
    if b_pos <= 1e-6:
        return np.ones_like(z, dtype=float)
    dz = np.clip(z - z_K, -8.0, 0.0)
    raw = np.exp(b_pos * dz)
    # Analytical RMS over top-K (integral_0^1 u^(2*b) du = 1/(1+2b))
    rms = math.sqrt(1.0 / (1.0 + 2.0 * b_pos))
    return raw / rms


def _surge_b_of(p: CommonParams) -> float:
    return float(p.surge_b) if p.surge_b is not None else float(p.b_xi)


def _drift_scale_of(b_d: float, z: np.ndarray, z_K: float) -> np.ndarray:
    if abs(float(b_d)) <= 1e-9:
        return np.ones_like(z, dtype=float)
    dz = np.clip(np.asarray(z, dtype=float) - z_K, -8.0, 0.0)
    return np.exp(np.clip(float(b_d) * dz, -4.0, 2.0))


def _drift_sd_of(p: CommonParams, z: np.ndarray) -> np.ndarray:
    if p.decline_var <= 0.0:
        return np.zeros(np.shape(z), dtype=float)
    return math.sqrt(p.decline_var) * _drift_scale_of(p.decline_b, np.asarray(z, dtype=float), p.z_K)


def _estimate_surge_b(
    Xc: np.ndarray,
    band_of: np.ndarray,
    band_z: np.ndarray,
    z_K: float,
    band_n: np.ndarray,
    H: int,
) -> float:
    """Estimate surge amplitude rank slope b_surge from bandwise kappa_3(Delta_h X) across medium lags."""
    hs_k = [h for h in (8, 13, 20, 26) if h <= H]
    if not hs_k:
        hs_k = [min(4, H)]
    k3_band = np.full(7, np.nan)
    for b in range(7):
        idx = np.flatnonzero(band_of == b)
        if idx.size < 20:
            continue
        vals = []
        for h in hs_k:
            dx = Xc[h:, idx] - Xc[:-h, idx]
            dx = dx - np.nanmean(dx, axis=1, keepdims=True)
            v = dx[np.isfinite(dx)]
            if v.size < 100:
                continue
            sd = float(np.std(v))
            if sd < 1e-6:
                continue
            vw = np.clip(v, -7.0 * sd, 7.0 * sd)
            vc = vw - float(np.mean(vw))
            vals.append(float(np.mean(vc ** 3)))
        if vals:
            k3_band[b] = float(np.mean(vals))
    ok = np.isfinite(k3_band) & (k3_band > 1e-4)
    if ok.sum() < 3:
        return 0.0
    dz = band_z[:7] - z_K
    w = np.minimum(band_n[:7], 100.0)
    _, slope_k3 = _wls_line(dz[ok], np.log(k3_band[ok]), w[ok])
    return float(np.clip(slope_k3 / 3.0, 0.0, 0.50))


def _jump_scale_of(b: float, z: np.ndarray, z_K: float) -> np.ndarray:
    """Rank-attenuation multiplier for rare lasting jumps, capped at rank K (dz <= 0) so the K..4K buffer never explodes."""
    b_pos = max(float(b), 0.0)
    dz = np.clip(np.asarray(z, dtype=float) - z_K, -8.0, 0.0)
    raw = np.exp(b_pos * dz)
    rms = math.sqrt(1.0 / (1.0 + 2.0 * b_pos))
    return raw / rms


def _jump_sd_of(p: CommonParams, z: np.ndarray) -> np.ndarray:
    """Per-jump SD by log-rank z: sqrt(jump_c) x inherited rank attenuation (0 extra scalars)."""
    if p.jump_lam <= 0.0 or p.jump_c <= 0.0:
        return np.zeros(np.shape(z), dtype=float)
    if getattr(p, "jump_dist", "gaussian") == "laplace":
        return math.sqrt(p.jump_c) * _surge_scale_of(p.b_xi, np.asarray(z, dtype=float), p.z_K)
    return math.sqrt(p.jump_c) * _jump_scale_of(p.b_xi, np.asarray(z, dtype=float), p.z_K)


def _draw_laplace_unit(rng: np.random.Generator, n: int) -> np.ndarray:
    """Unit-variance Laplace (two-sided exponential) draws: sign x Exp(1) / sqrt(2)."""
    return (rng.standard_exponential(n) * np.where(rng.random(n) < 0.5, -1.0, 1.0)) / math.sqrt(2.0)


def _draw_jump_unit(p: CommonParams, rng: np.random.Generator, n: int) -> np.ndarray:
    """Unit-variance rare permanent jump draw: Gaussian (Round 6 meta) or Laplace (Round 5 robust)."""
    if getattr(p, "jump_dist", "gaussian") == "laplace":
        return _draw_laplace_unit(rng, n)
    return rng.standard_normal(n)


def _estimate_stage3_surge(
    Xc: np.ndarray,
    pr_ord: np.ndarray,
    pres: np.ndarray,
    K: int,
    D_med: np.ndarray,
) -> tuple[float, float, float, dict]:
    """Identify fading positive surges (lam, mu, rho) strictly from marginal single-horizon
    additive cumulants kappa_3(Delta_h X) and kappa_5(Delta_h X), using exact capped-exponential
    moments matching the simulated jump law min(Exp(1), SURGE_EXP_CAP).

    Because kappa_3 and kappa_5 are exact additive cumulants, ALL symmetric noise (Gaussian,
    heavy-tailed, empirical) cancels out identically without any empirical tuning divisor.
    NEVER uses asymmetry D, p_fall, p_rise, or any cross-window persistence!
    Returns (surge_lam, surge_mu, surge_rho, diag).
    """
    T = Xc.shape[0]
    sel = (pr_ord <= K) & (pres >= 0.5)
    if sel.sum() < 50 or T < 16:
        return 0.0, 0.0, 0.0, {"active": False, "reason": "insufficient_data"}

    hs = np.array([h for h in (4, 8, 13, 20, 26, 39, 52) if h <= min(52, T // 2)], dtype=float)
    if hs.size < 2:
        return 0.0, 0.0, 0.0, {"active": False, "reason": "too_short"}

    Xsub = Xc[:, sel]
    k3s = np.empty(hs.size)
    k5s = np.empty(hs.size)
    for i, h in enumerate(hs.astype(int)):
        dx = Xsub[h:] - Xsub[:-h]
        dx = dx - np.nanmean(dx, axis=1, keepdims=True)
        v = dx[np.isfinite(dx)]
        if v.size < 100:
            k3s[i] = 0.0
            k5s[i] = 0.0
            continue
        sd = float(np.std(v))
        if sd < 1e-6:
            k3s[i] = 0.0
            k5s[i] = 0.0
            continue
        vw = np.clip(v, -7.0 * sd, 7.0 * sd)
        vc = vw - float(np.mean(vw))
        v2 = float(np.mean(vc ** 2))
        v3 = float(np.mean(vc ** 3))
        v5 = float(np.mean(vc ** 5)) - 10.0 * v2 * v3
        k3s[i] = v3
        k5s[i] = v5

    # Exact identity: kappa_3(Delta_h s) = C3 * 3 * rho^h * (1 - rho^h)
    best = None
    for hl_g in SURGE_HL_GRID:
        rg = float(0.5 ** (1.0 / hl_g))
        f3 = 3.0 * (rg ** hs) * (1.0 - rg ** hs)
        C3 = max(float((f3 @ k3s) / max(float(f3 @ f3), 1e-12)), 0.0)
        err = float(np.sum((k3s - C3 * f3) ** 2))
        if best is None or err < best[0]:
            best = (err, float(hl_g), rg, C3)

    _, hl_h, rho_h, C3_h = best
    f5 = 5.0 * (rho_h ** hs) * (1.0 - rho_h ** hs) * (1.0 - 2.0 * (rho_h ** hs) + 2.0 * (rho_h ** (2.0 * hs)))
    C5_h = float((f5 @ k5s) / max(float(f5 @ f5), 1e-12))

    if C3_h < 0.025 or C5_h <= 0.01 or float(np.sum(k3s)) <= 0.02:
        return 0.0, 0.0, 0.0, {"active": False, "C3": C3_h, "C5": C5_h, "k3s": k3s.tolist()}

    # Exact additive cumulant ratio: kappa_5(J) / kappa_3(J) = (CAP_M5 / CAP_M3) * mu^2 (leading order)
    ratio_J = (C5_h * (1.0 - rho_h ** 5)) / max(C3_h * (1.0 - rho_h ** 3), 1e-12)
    mu_h = float(np.clip(math.sqrt(max(ratio_J * (CAP_M3 / CAP_M5), 0.0)), 0.15, 2.50))

    # Solve exact capped kappa_3(J) = lam*M3 - 3*lam^2*M1*M2 + 2*lam^3*M1^3 = C3*(1-rho^3)/mu^3
    rhs = C3_h * (1.0 - rho_h ** 3) / max(mu_h ** 3, 1e-9)
    lam_h = float(np.clip(rhs / CAP_M3, 0.0, 0.15))
    for _ in range(4):
        f = lam_h * CAP_M3 - 3.0 * (lam_h ** 2) * CAP_M1 * CAP_M2 + 2.0 * (lam_h ** 3) * (CAP_M1 ** 3) - rhs
        fp = CAP_M3 - 6.0 * lam_h * CAP_M1 * CAP_M2 + 6.0 * (lam_h ** 2) * (CAP_M1 ** 3)
        lam_h = float(np.clip(lam_h - f / max(fp, 1e-6), 0.0, 0.15))

    # Variance conservation guard: surge variance 2*V_s*(1 - rho^h) <= 0.35 * median_D(h)
    Vs = _surge_variance(lam_h, mu_h, rho_h)
    h_idx = np.clip(hs.astype(int) - 1, 0, D_med.size - 1)
    d_ref = D_med[h_idx]
    ok_d = np.isfinite(d_ref) & (d_ref > 1e-6)
    budget_hit = False
    if ok_d.any() and Vs > 0.0:
        ds_h = 2.0 * Vs * (1.0 - rho_h ** hs[ok_d])
        ratio = float(np.max(ds_h / d_ref[ok_d]))
        if ratio > 0.35:
            scale = 0.35 / ratio
            lam_h = float(lam_h * scale)
            Vs = _surge_variance(lam_h, mu_h, rho_h)
            budget_hit = True

    if lam_h < 1e-4:
        return 0.0, 0.0, 0.0, {"active": False, "C3": C3_h, "C5": C5_h, "k3s": k3s.tolist()}

    return lam_h, mu_h, rho_h, {
        "active": True, "hl_s": hl_h, "hl_at_grid_max": bool(hl_h >= float(SURGE_HL_GRID[-1])),
        "C3": C3_h, "C5": C5_h, "V_s": Vs, "budget_hit": budget_hit,
        "k3s": k3s.tolist(), "k5s": k5s.tolist(), "hs": hs.tolist(),
    }


def _estimate_stage3_drift(
    Xc: np.ndarray,
    pr_ord: np.ndarray,
    pres: np.ndarray,
    K: int,
    phi: float,
    Vxi_mean: float,
    Vs: float,
    rho_s: float,
    vxi_fn=None,
    ssc2_fn=None,
    z_K: float | None = None,
    rank_law: bool = False,
) -> tuple[float, float, float, dict]:
    """Identify persistent entity drift variance sigma_d^2 (fixed hl_d = 52w) in closed form
    from gapped window-change covariance Cov(x, y) net of passing AR(1) and surge covariances.

    When rank_law=True, adds entity-clustered 2-SE activation threshold and 3-tier WLS rank law
    sigma_d(z) = sigma_d(K) * exp(b_d * min(z - z_K, 0)).
    Returns (decline_var, decline_rho, decline_b, diag).
    """
    T = Xc.shape[0]
    rho_d = float(0.5 ** (1.0 / DRIFT_HL_FIXED))
    if T >= 160:
        W, H, g = 13, 52, 13
    elif T >= 75:
        W, H, g = 4, 26, 4
    else:
        return 0.0, 0.0, 0.0, {"active": False, "reason": "short_panel", "T": int(T)}

    sel = (pr_ord <= K) & (pres >= 0.5)
    if sel.sum() < 50:
        return 0.0, 0.0, 0.0, {"active": False, "reason": "few_entities"}

    pr_sub = pr_ord[sel]
    lab_sub = np.where(pr_sub <= (K / 16.0), 0, np.where(pr_sub <= (K / 4.0), 1, 2))
    Xsub = Xc[:, sel]
    covs = []
    covs_t = {0: [], 1: [], 2: []}
    n_ent = Xsub.shape[1]
    ent_sum = np.zeros(n_ent)
    ent_cnt = np.zeros(n_ent)
    ent_sum_t = np.zeros(n_ent)
    ent_cnt_t = np.zeros(n_ent)
    for o in range(0, T - (2 * H + g) + 1, W):
        with np.errstate(all="ignore"):
            a0 = np.nanmean(Xsub[o:o + W], axis=0)
            a1 = np.nanmean(Xsub[o + H - W:o + H], axis=0)
            b0 = np.nanmean(Xsub[o + H + g:o + H + g + W], axis=0)
            b1 = np.nanmean(Xsub[o + 2 * H + g - W:o + 2 * H + g], axis=0)
        ok = np.isfinite(a0) & np.isfinite(a1) & np.isfinite(b0) & np.isfinite(b1)
        if ok.sum() < 50:
            continue
        x_all = a1 - a0
        y_all = b1 - b0
        xd = np.full_like(x_all, np.nan)
        yd = np.full_like(y_all, np.nan)
        for j in (0, 1, 2):
            sj = ok & (lab_sub == j)
            if sj.sum() >= 20:
                xd[sj] = x_all[sj] - float(np.mean(x_all[sj]))
                yd[sj] = y_all[sj] - float(np.mean(y_all[sj]))
                if rank_law:
                    xj, yj = xd[sj], yd[sj]
                    sxj, syj = float(np.std(xj)), float(np.std(yj))
                    if sxj > 1e-6 and syj > 1e-6:
                        prod_j = np.clip(xj, -6.0 * sxj, 6.0 * sxj) * np.clip(yj, -6.0 * syj, 6.0 * syj)
                        covs_t[j].append(float(np.mean(prod_j)))
                        idx_j = np.flatnonzero(sj)
                        ent_sum_t[idx_j] += prod_j
                        ent_cnt_t[idx_j] += 1.0
        ok2 = np.isfinite(xd) & np.isfinite(yd)
        if ok2.sum() < 50:
            xd[ok] = x_all[ok] - float(np.mean(x_all[ok]))
            yd[ok] = y_all[ok] - float(np.mean(y_all[ok]))
            ok2 = ok
        x = xd[ok2]
        y = yd[ok2]
        sx, sy = float(np.std(x)), float(np.std(y))
        if sx < 1e-6 or sy < 1e-6:
            continue
        xw = np.clip(x, -6.0 * sx, 6.0 * sx)
        yw = np.clip(y, -6.0 * sy, 6.0 * sy)
        covs.append(float(np.mean(xw * yw)))
        if rank_law:
            idx2 = np.flatnonzero(ok2)
            ent_sum[idx2] += xw * yw
            ent_cnt[idx2] += 1.0

    if not covs:
        return 0.0, 0.0, 0.0, {"active": False, "reason": "no_origins"}

    cov_emp = float(np.mean(covs))
    c_xi = Vxi_mean * _G_ar1(phi, W, H, g)
    c_s = Vs * _G_ar1(rho_s, W, H, g) if (Vs > 0.0 and rho_s > 0.0) else 0.0
    gd = _G_drift(rho_d, W, H, g)
    net_cov = cov_emp - c_xi - c_s
    sd2 = max(net_cov / max(gd, 1e-9), 0.0)
    se_cov = float("nan")
    if rank_law:
        m_e = ent_cnt > 0
        if m_e.sum() >= 20:
            c_i = ent_sum[m_e] / ent_cnt[m_e]
            se_cov = float(np.std(c_i, ddof=1) / math.sqrt(m_e.sum()))
        if not (np.isfinite(se_cov) and net_cov > DRIFT_Z_MIN * se_cov):
            sd2 = 0.0
    if sd2 < 1e-7:
        return 0.0, 0.0, 0.0, {
            "active": False, "W": W, "H": H, "g": g,
            "cov_emp": cov_emp, "c_xi": c_xi, "c_s": c_s, "net_cov": net_cov, "G_d": gd,
            "se_cov": se_cov,
        }
    diag = {
        "active": True, "W": W, "H": H, "g": g, "hl_d": DRIFT_HL_FIXED,
        "cov_emp": cov_emp, "c_xi": c_xi, "c_s": c_s, "net_cov": net_cov, "G_d": gd,
        "sigma_d_pooled": math.sqrt(sd2), "se_cov": se_cov,
    }
    b_d = 0.0
    sd2_K = sd2
    if rank_law and vxi_fn is not None and z_K is not None:
        N = Xc.shape[1]
        z_sub = np.log(np.clip((pr_sub - 0.5) / N, Z_CLIP, 1.0))
        g_xi = _G_ar1(phi, W, H, g)
        g_s = _G_ar1(rho_s, W, H, g) if (Vs > 0.0 and rho_s > 0.0) else 0.0
        tiers = []
        for j in (0, 1, 2):
            mj = lab_sub == j
            if not covs_t[j] or mj.sum() < 20:
                continue
            zj = z_sub[mj]
            c_xi_j = float(np.mean(vxi_fn(zj))) * g_xi
            c_s_j = Vs * float(np.mean(ssc2_fn(zj))) * g_s if (ssc2_fn is not None and g_s != 0.0) else 0.0
            net_j = float(np.mean(covs_t[j])) - c_xi_j - c_s_j
            me = mj & (ent_cnt_t > 0)
            se_j = (float(np.std(ent_sum_t[me] / ent_cnt_t[me], ddof=1) / math.sqrt(me.sum()))
                    if me.sum() >= 10 else float("nan"))
            tiers.append(dict(tier=j, n=int(mj.sum()), z_mean=float(np.mean(zj)),
                              net=net_j, se=se_j, sd2=max(net_j / max(gd, 1e-9), 0.0)))
        pos = [t for t in tiers if t["sd2"] > 1e-9 and np.isfinite(t["se"]) and t["se"] > 0]
        if len(pos) >= 2:
            xz = np.minimum(np.array([t["z_mean"] - z_K for t in pos]), 0.0)
            yl = np.array([0.5 * math.log(t["sd2"]) for t in pos])
            wt = np.array([(t["net"] / t["se"]) ** 2 for t in pos])
            sw = np.sqrt(wt)
            A = np.column_stack([np.ones_like(xz), xz]) * sw[:, None]
            a_l, b_l = np.linalg.lstsq(A, yl * sw, rcond=None)[0]
            b_d = float(np.clip(b_l, -0.5, 1.0))
            a_l = float(np.sum(wt * (yl - b_d * xz)) / np.sum(wt))
            sd2_K = float(math.exp(2.0 * a_l))
        diag.update(rank_law=True, tiers=tiers, b_d=b_d)
    diag["sigma_d"] = math.sqrt(sd2_K)
    return float(sd2_K), rho_d, float(b_d), diag


def _fit_structure_profiles(
    D: np.ndarray,
    hs: np.ndarray,
    nb: int,
    spec_b_pinned: bool,
    so_band_fixed: np.ndarray | None,
    D_subtract: np.ndarray | None = None,
    phi_grid: np.ndarray | None = None,
    lag_weight: str = "uniform",
) -> tuple[float, float, np.ndarray, np.ndarray, np.ndarray]:
    """Profile NNLS over pooled phi for D_b(h) (minus optional Stage 3 variance D_subtract(h))."""
    best = None
    for phi in (PHI_GRID if phi_grid is None else np.asarray(phi_grid, dtype=float)):
        col_xi = 2.0 * (1.0 - phi ** hs)
        col_L = hs
        sse = 0.0
        so2_b = np.full(nb, np.nan)
        vxi_b = np.full(nb, np.nan)
        sL2_b = np.full(nb, np.nan)
        for b in range(nb):
            y = D[b]
            ok = np.isfinite(y)
            if ok.sum() < 4:
                continue
            y_ok = y[ok]
            h_ok = hs[ok]
            if lag_weight == "uniform":
                w_ok = np.ones_like(y_ok)
            elif lag_weight == "inv_sqrt_h":
                w_ok = 1.0 / np.sqrt(h_ok)
            elif lag_weight == "rel":
                w_ok = 1.0 / np.maximum(y_ok, 1e-4)
            elif lag_weight == "rel_sqrt_h":
                w_ok = 1.0 / (np.maximum(y_ok, 1e-4) * np.sqrt(h_ok))
            else:
                raise ValueError(f"Unknown lag_weight={lag_weight!r}")
            if D_subtract is not None:
                sub_b = D_subtract[b, ok] if D_subtract.ndim == 2 else D_subtract[ok]
                sub_ok = np.minimum(sub_b, 0.85 * y_ok)
                y_net = np.maximum(y_ok - sub_ok, 0.0)
            else:
                y_net = y_ok
            floor_var = (so_band_fixed[b] ** 2) if spec_b_pinned else 0.0
            y_adj = np.maximum(y_net - 2.0 * floor_var, 0.0)
            A = np.column_stack([2.0 * np.ones(ok.sum()), col_xi[ok], col_L[ok]]) * w_ok[:, None]
            coef, res = nnls(A, y_adj * w_ok)
            so2_b[b] = floor_var + coef[0]
            vxi_b[b] = coef[1]
            sL2_b[b] = coef[2]
            denom = max(float(np.sum((y_ok * w_ok) ** 2)), 1e-12)
            if b < nb - 1:
                sse += (res ** 2) / denom
        if best is None or sse < best[0]:
            best = (sse, float(phi), so2_b, vxi_b, sL2_b)
    return best


def _model_increment_ratio(
    sL2_b: np.ndarray,
    vxi_b: np.ndarray,
    so2_b: np.ndarray,
    phi: float,
    s_vol: float,
    pool: np.ndarray,
    innov_mode: str,
    H: int,
    surge: tuple[float, float, float, np.ndarray] | None = None,
    drift: tuple[float, float] | None = None,
    jump: tuple[float, np.ndarray] | None = None,
    n_mc: int = 16384,
    seed: int = 4242,
) -> np.ndarray:
    """Model-implied ratio  (IQR/1.349)^2 / Var  of the h-week increment X_{t+h} - X_t, h = 1..H,
    for a representative entity in each permanent-rank band, under the model's OWN innovation law:
    bounded empirical pool (or Gaussian), lognormal entity amplitude v_i (drawn exactly as the
    simulator draws it), stationary passing AR(1), observation noise, and the Stage 3 surge and
    drift states when active. Common random numbers across bands; deterministic seed.

    Used by the Round 5 robust scale-moment fit: the moment condition is
        model IQR_b(h) = empirical IQR_b(h)
    which, divided through by the model variance, becomes the linear NNLS target
        Var_model,b(h) = D_rob,b(h) / ratio_b(h).
    The constant 1.349 cancels; no scalar is fitted here."""
    nb = len(sL2_b)
    rng = np.random.default_rng(seed)

    def draw(n: int) -> np.ndarray:
        if innov_mode == "empirical":
            return pool[rng.integers(0, pool.size, size=n)]
        return rng.standard_normal(n)

    if s_vol > 0:
        z_v = np.clip(rng.standard_normal(n_mc), -2.5, 2.5)
        v = np.exp(s_vol * z_v)
        v = v / np.mean(v)
    else:
        v = np.ones(n_mc)
    sqv = np.sqrt(v)
    etaL = np.stack([draw(n_mc) for _ in range(H)])
    etaX = np.stack([draw(n_mc) for _ in range(H + 1)])
    etaE = np.stack([draw(n_mc) for _ in range(H + 1)])
    xi0 = rng.standard_normal(n_mc)
    c_xi = math.sqrt(max(1.0 - phi * phi, 1e-6))

    drift_path = None
    if drift is not None and drift[0] > 0.0:
        d_var, rho_d = drift[0], drift[1]
        sd_d = math.sqrt(d_var)
        d = sd_d * rng.standard_normal(n_mc)
        innov_sd = math.sqrt(d_var * max(1.0 - rho_d * rho_d, 0.0))
        acc = np.zeros(n_mc)
        drift_path = np.empty((H, n_mc), dtype=np.float64)
        for t in range(H):
            acc = acc + d
            drift_path[t] = acc
            d = rho_d * d + innov_sd * rng.standard_normal(n_mc)
    drift_sc_b = drift[2] if (drift is not None and len(drift) >= 3 and drift[2] is not None) else np.ones(nb)
    surge_path = None
    if surge is not None and surge[0] > 0.0 and surge[1] > 0.0 and 0.0 < surge[2] < 1.0:
        lam_s, mu_s, rho_s, _ = surge
        s = np.zeros(n_mc)
        for _ in range(int(math.ceil(math.log(0.001) / math.log(rho_s)))):
            s = rho_s * s + (rng.random(n_mc) < lam_s) * mu_s * np.minimum(rng.standard_exponential(n_mc), SURGE_EXP_CAP)
        surge_path = np.empty((H, n_mc))
        s0 = s.copy()
        for t in range(H):
            s = rho_s * s + (rng.random(n_mc) < lam_s) * mu_s * np.minimum(rng.standard_exponential(n_mc), SURGE_EXP_CAP)
            surge_path[t] = s - s0

    jump_cum = None
    if jump is not None and jump[0] > 0.0:
        hits = rng.random((H, n_mc)) < float(jump[0])
        jl = np.where(rng.random((H, n_mc)) < 0.5, -1.0, 1.0) * rng.standard_exponential((H, n_mc)) / math.sqrt(2.0)
        jump_cum = np.cumsum(hits * jl, axis=0)

    out = np.full((nb, H), np.nan)
    for b in range(nb):
        if not (np.isfinite(sL2_b[b]) and np.isfinite(vxi_b[b]) and np.isfinite(so2_b[b])):
            continue
        sL = math.sqrt(max(float(sL2_b[b]), 0.0))
        sx = math.sqrt(max(float(vxi_b[b]), 0.0))
        so = math.sqrt(max(float(so2_b[b]), 0.0))
        xi_prev = sx * sqv * xi0
        xi_cum = np.empty((H, n_mc))
        for t in range(H):
            xi_t = phi * xi_prev + sx * c_xi * sqv * etaX[t + 1]
            xi_cum[t] = xi_t - sx * sqv * xi0
            xi_prev = xi_t
        dX = np.cumsum(sL * etaL, axis=0) + xi_cum + so * sqv * (etaE[1:] - etaE[0][None, :])
        if drift_path is not None:
            dX = dX + float(drift_sc_b[b]) * drift_path
        if surge_path is not None:
            dX = dX + float(surge[3][b]) * surge_path
        if jump_cum is not None:
            dX = dX + float(jump[1][b]) * jump_cum
        var = np.var(dX, axis=1)
        q25, q75 = np.quantile(dX, [0.25, 0.75], axis=1)
        iqrv = ((q75 - q25) / IQR_TO_SD) ** 2
        with np.errstate(all="ignore"):
            r = np.where(var > 1e-14, iqrv / var, 1.0)
        out[b] = r
    return out


# --------------------------------------------------------------------------- #
# Estimation (strictly from declared moments; zero tuning to scores)
# --------------------------------------------------------------------------- #
_DEFAULT_LAG_WEIGHT = "uniform"
_DEFAULT_S3_RANK_LAW = False
_DEFAULT_S3_META_JUMPS = False


def estimate_common(
    X: np.ndarray,
    R: np.ndarray,
    K: int,
    floors: np.ndarray | None = None,
    spec_b: tuple[np.ndarray, np.ndarray] | None = None,
    refine_drift: bool = True,
    innov_mode: str = "empirical",
    min_pres_band: float = 0.5,
    fit_gamma_L: bool = False,
    asymmetry: bool = False,
    scale_moment: str = "meta",
    lag_weight: str | None = None,
    s3_rank_law: bool | None = None,
    s3_meta_jumps: bool | None = None,
) -> CommonParams:
    """Estimate the Common Rank-Diffusion meta-model (11-12 core scalars; 14-15 with asymmetry=True).

    Parameters
    ----------
    X : (T, N) log1p activity matrix (NaN when absent)
    R : (T, N) weekly rank matrix (NaN when absent)
    K : int, scored top-K universe size
    floors : (T,) absence rank floor (N_t + 1); computed if None
    spec_b : optional (z_sb, sigma_obs_sb) daily replication floor pinning sigma_obs(z)
    refine_drift : whether to run smooth all-rank fixed-point refinement of g(r)
    innov_mode : "empirical" (bounded empirical standardized residuals) or "gaussian"
    asymmetry : opt-in Stage 3 (fading positive surges + persistent entity drift, variance-conserving;
        uses surges as its rare-event channel while keeping symmetric jumps off)
    scale_moment :
        "meta" (default, Round 6): total-variance-conserving lasting split. Keeps mean-square
        (phi, V_xi, sigma_obs) intact (preserving short-lag calibration on every platform) and,
        when asymmetry=False and lasting jump gap share > 20%, splits lasting variance into the
        robust h<=13 diffusive core + rare permanent Gaussian jumps (JUMP_LAM_FIXED=0.02/wk,
        +1 scalar jump_c, rank law inherited from b_xi and capped at rank K). When asymmetry=True,
        uses the clean Round 4 mean-square + surges + persistent drift path.
        "variance" (Round 4): pure mean-square within-band variogram fit (11 sc S2 / 14-15 sc S3).
        "robust" (Round 5 Fable S2+J): robust IQR fit for (sigma_L, V_xi, sigma_obs) + Laplace
        jumps at JUMP_LAM_ROBUST=0.01/wk.
    """
    if scale_moment not in ("meta", "robust", "variance"):
        raise ValueError(f"scale_moment must be 'meta', 'robust', or 'variance', got {scale_moment!r}")
    if lag_weight is None:
        lag_weight = _DEFAULT_LAG_WEIGHT
    if s3_rank_law is None:
        s3_rank_law = _DEFAULT_S3_RANK_LAW
    if s3_meta_jumps is None:
        s3_meta_jumps = _DEFAULT_S3_META_JUMPS
    X = np.asarray(X, dtype=np.float64)
    R = np.asarray(R, dtype=np.float64)
    T, N = X.shape
    if floors is None:
        floors = np.sum(np.isfinite(R), axis=1).astype(float) + 1.0

    # Absence-penalized permanent rank (NEVER current rank!)
    pen = np.where(np.isfinite(R), R, floors[:, None])
    rperm = np.mean(pen, axis=0)
    pres = np.isfinite(R).mean(axis=0)
    pr_ord = np.empty(N, dtype=float)
    pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)

    edges = _band_edges(K, N)
    nb = len(edges) - 1
    band_all = np.clip(np.searchsorted(edges, pr_ord, side="right") - 1, 0, nb - 1)
    band_of = np.where(pres >= min_pres_band, band_all, -1)
    band_z = np.log(np.clip((np.sqrt(np.maximum(edges[:-1], 1.0) * edges[1:]) - 0.5) / N, Z_CLIP, 1.0))
    z_K = float(math.log(max((K - 0.5) / N, Z_CLIP)))

    # Top-K median reference removed (immune to 3K buffer compositional drift)
    pmean = _top_k_center(X, R, K)
    Xc = X - pmean
    # Reference ladder: median/mean observed level across first half-year (or min(26, T) weeks)
    b0_len = min(26, max(4, T // 3))
    with np.errstate(all="ignore"):
        m0 = np.nanmean(X[:b0_len], axis=0)
        m_all = np.nanmean(X, axis=0)
    fill_val = float(np.nanmin(m_all)) if np.any(np.isfinite(m_all)) else 0.0
    m0 = np.where(np.isfinite(m0), m0, np.where(np.isfinite(m_all), m_all, fill_val))
    ladder_obs = np.sort(m0)[::-1]
    L_K = float(ladder_obs[min(K, N) - 1])

    band_level = np.array([
        float(np.nanmean(X[:, band_of == b])) if np.any(band_of == b) else np.nan
        for b in range(nb)
    ])
    band_n = np.array([float(np.sum(band_of == b)) for b in range(nb)])

    # 1. Structure function D_b(h) across h = 1..H
    H = int(min(52, max(6, T // 2)))
    D = _structure_function(Xc, band_of, nb, H)
    hs = np.arange(1, H + 1, dtype=float)

    # Optional Spec-B pinned sigma_obs lower-bound profile (2 scalars: log-linear in z - z_K)
    spec_b_pinned = spec_b is not None
    a_obs_floor, b_obs_floor = -20.0, 0.0
    if spec_b_pinned:
        z_sb, so_sb = np.asarray(spec_b[0], float), np.asarray(spec_b[1], float)
        ok_sb = np.isfinite(z_sb) & np.isfinite(so_sb) & (so_sb > 1e-5)
        a_obs_floor, b_obs_floor = _wls_line(z_sb[ok_sb] - z_K, np.log(so_sb[ok_sb]), np.ones(ok_sb.sum()))
        so_band_fixed = np.exp(np.clip(a_obs_floor + b_obs_floor * (band_z - z_K), -6.0, 3.0))
    else:
        so_band_fixed = None

    in_k = np.arange(nb) < nb - 1
    dz_b = band_z - z_K
    w_oct = np.minimum(band_n, 100.0)

    # 1b. Entity volatility spread (split-half) and the raw bounded empirical innovation pool are
    #     inputs of the robust scale-moment fit below, so they are built first (same formulas as before).
    s_short = _split_half_vol_spread(Xc, pr_ord, K, N, step=1)
    s_vol = float(np.clip(s_short, 0.0, 1.5))
    q_pool = _empirical_innov_pool_quantiles(Xc, pr_ord, pres, K)
    if q_pool is not None:
        pool_mc = (q_pool - np.mean(q_pool)) / max(float(np.std(q_pool)), 1e-6)
    else:
        pool_mc = np.linspace(-3.0, 3.0, 1024)
        pool_mc = pool_mc / np.std(pool_mc)

    # Stage 3 surge (opt-in) is identified from marginal kappa_3 / kappa_5 and the mean-square budget
    # guard; it does not depend on the core's scale moment, so it is estimated once here.
    surge_pre = None
    surge_b_est = None
    if asymmetry:
        with np.errstate(all="ignore"):
            D_med = np.nanmedian(D[in_k], axis=0)
        surge_pre = _estimate_stage3_surge(Xc, pr_ord, pres, K, D_med)
        if s3_rank_law and surge_pre[0] > 0.0:
            surge_b_est = _estimate_surge_b(Xc, band_of, band_z, z_K, band_n, H)

    def _fit_core(D_target: np.ndarray, phi_grid: np.ndarray | None = None, D_extra: np.ndarray | None = None) -> dict:
        sse_c, phi_c, so2_c, vxi_c, sL2_c = _fit_structure_profiles(
            D_target, hs, nb, spec_b_pinned, so_band_fixed,
            D_subtract=D_extra, phi_grid=phi_grid, lag_weight=lag_weight,
        )
        s_lam = s_mu = s_rho = 0.0
        d_var = d_rho = d_b = 0.0
        s3diag = None
        b_xi_ref = 0.0
        if asymmetry:
            s_lam, s_mu, s_rho, s3_surge = surge_pre
            Vs = _surge_variance(s_lam, s_mu, s_rho)
            for _pass in range(2):
                ok_xi0 = in_k & np.isfinite(vxi_c) & (vxi_c > 1e-7)
                if ok_xi0.sum() >= 2:
                    a_xi0, b_xi0 = _wls_line(dz_b[ok_xi0], 0.5 * np.log(vxi_c[ok_xi0]), w_oct[ok_xi0])
                    b_xi_ref = b_xi0
                    sel_k = (pr_ord <= K) & (pres >= 0.5)
                    z_sel = np.log(np.clip((pr_ord[sel_k] - 0.5) / N, Z_CLIP, 1.0))
                    Vxi_mean = float(np.mean(np.exp(2.0 * np.clip(a_xi0 + b_xi0 * (z_sel - z_K), -6.0, 3.0))))
                    vxi_fn = lambda z_arr, _a=a_xi0, _b=b_xi0, _zk=z_K: np.exp(2.0 * np.clip(_a + _b * (z_arr - _zk), -6.0, 3.0))
                else:
                    Vxi_mean = 0.0
                    vxi_fn = None

                b_s_use = surge_b_est if (s3_rank_law and surge_b_est is not None) else b_xi_ref
                ssc2_fn = (lambda z_arr, _bs=b_s_use, _zk=z_K: _surge_scale_of(_bs, z_arr, _zk) ** 2) if (Vs > 0.0 and s_rho > 0.0) else None
                d_var, d_rho, d_b, s3_drift = _estimate_stage3_drift(
                    Xc, pr_ord, pres, K, phi_c, Vxi_mean, Vs, s_rho,
                    vxi_fn=vxi_fn, ssc2_fn=ssc2_fn, z_K=z_K, rank_law=s3_rank_law,
                )
                D_sub = np.zeros((nb, hs.size), dtype=float) if D_extra is None else D_extra.copy()
                if Vs > 0.0 and s_rho > 0.0:
                    s_sc_b = _surge_scale_of(b_s_use, band_z, z_K)
                    D_sub = D_sub + (s_sc_b[:, None] ** 2) * (2.0 * Vs * (1.0 - s_rho ** hs))[None, :]
                if d_var > 0.0 and d_rho > 0.0:
                    d_sc_b = _drift_scale_of(d_b, band_z, z_K)
                    D_sub = D_sub + (d_var * (d_sc_b[:, None] ** 2)) * _f_vel(hs, d_rho)[None, :]

                if np.any(D_sub > 0.0):
                    sse_c, phi_c, so2_c, vxi_c, sL2_c = _fit_structure_profiles(
                        D_target, hs, nb, spec_b_pinned, so_band_fixed,
                        D_subtract=D_sub, phi_grid=phi_grid, lag_weight=lag_weight,
                    )
                else:
                    break
            s3diag = {"surge": s3_surge, "drift": s3_drift, "surge_b": surge_b_est}
        return dict(sse=sse_c, phi=phi_c, so2_b=so2_c, vxi_b=vxi_c, sL2_b=sL2_c,
                    surge_lam=s_lam, surge_mu=s_mu, surge_rho=s_rho, surge_b=(surge_b_est if (s3_rank_law and s_lam > 0.0) else None),
                    decline_var=d_var, decline_rho=d_rho, decline_b=d_b, stage3_diag=s3diag, b_xi_ref=b_xi_ref)

    # 2. Core fit. First on the mean-square variogram over h = 1..H (the Round 3/4 moment; final when
    #    scale_moment == "variance"; it identifies the passing persistence phi from the visible AR(1)
    #    saturation). Then -- Round 5 default -- three fixed-point steps on the ROBUST variogram over
    #    h = 1..ROBUST_H_MAX with phi pinned: the model's IQR of h-week increments is required to equal
    #    the data's in every band, with the model's own non-Gaussian increment law supplying the
    #    IQR/variance ratio at the current parameters. Rare regime jumps (which the bounded pool
    #    cannot and should not reproduce) thereby stop inflating the diffusive lasting step and the
    #    passing/observation scales. phi is pinned because 13 lags cannot separate an AR(1) from a
    #    random walk (the short-lag ridge recorded in pitfalls.md); the scale components can.
    fit = _fit_core(D)
    fit_var = fit
    D_rob = None
    ratio_last = None
    jump_lam = 0.0
    jump_c = 0.0
    jump_dist = "laplace" if scale_moment == "robust" else "gaussian"
    jump_diag = None
    if scale_moment == "robust" or (scale_moment == "meta" and ((not asymmetry) or s3_meta_jumps)):
        D_rob_all = _structure_function(Xc, band_of, nb, H, robust=True)
        D_rob = D_rob_all.copy()
        D_rob[:, ROBUST_H_MAX:] = np.nan  # declared horizon set of the robust SCALE fit (see ROBUST_H_MAX)
        phi_pin = np.array([fit_var["phi"]])

        def _b_xi_of(vxi_c: np.ndarray) -> float:
            ok_b = in_k & np.isfinite(vxi_c) & (vxi_c > 1e-7)
            if ok_b.sum() >= 2:
                return _wls_line(dz_b[ok_b], 0.5 * np.log(vxi_c[ok_b]), w_oct[ok_b])[1]
            return 0.0

        def _robust_fit_for(lam_j: float, lam_c: float, fit_start: dict, n_it: int) -> tuple[dict, float, np.ndarray, float]:
            """Fixed point of the robust scale fit (h <= ROBUST_H_MAX, phi pinned) for a given jump rate lam_j.
            The weekly jump variance lam_c (at the s_sc normalization; per-jump variance c = lam_c / lam_j) is
            the band-weighted mean of (mean-square lasting variance - robust core), re-evaluated each step
            with damping once the jumps are inside the model's increment law (so the core is jump-clean).
            Returns (fit, c, ratio, long-horizon robust SSE)."""
            fit_j = fit_start
            c_j = (lam_c / lam_j) if (lam_j > 0.0 and lam_c > 0.0) else 0.0
            ratio_j = None
            D_extra = None
            for _it in range(n_it):
                s_sc_b = _surge_scale_of(_b_xi_of(fit_j["vxi_b"]), band_z, z_K)
                jump_arg = None
                D_extra = None
                if c_j > 0.0:
                    jump_arg = (lam_j, math.sqrt(c_j) * s_sc_b)
                    D_extra = (lam_c * (s_sc_b[:, None] ** 2)) * hs[None, :]
                surge_arg = None
                if fit_j["surge_lam"] > 0.0:
                    b_s_j = fit_j["surge_b"] if fit_j.get("surge_b") is not None else fit_j["b_xi_ref"]
                    surge_arg = (fit_j["surge_lam"], fit_j["surge_mu"], fit_j["surge_rho"],
                                 _surge_scale_of(b_s_j, band_z, z_K))
                drift_arg = (fit_j["decline_var"], fit_j["decline_rho"],
                             _drift_scale_of(fit_j.get("decline_b", 0.0), band_z, z_K)) if fit_j["decline_var"] > 0.0 else None
                ratio_j = _model_increment_ratio(
                    fit_j["sL2_b"], fit_j["vxi_b"], fit_j["so2_b"], fit_j["phi"], s_vol, pool_mc, innov_mode, H,
                    surge=surge_arg, drift=drift_arg, jump=jump_arg,
                )
                with np.errstate(all="ignore"):
                    D_target = np.where(np.isfinite(ratio_j) & (ratio_j > 1e-6) & np.isfinite(D_rob),
                                        D_rob / ratio_j, np.nan)
                fit_j = _fit_core(D_target, phi_grid=phi_pin, D_extra=D_extra)
            # Identification of lam_j: the model's IQR profile against the data's over ALL h = 1..H (per-band
            # normalized). The short horizons are matched by construction, so the criterion is carried by
            # h > ROBUST_H_MAX: how far the IQR has absorbed jumps by h = 26..52 is set by lambda h.
            # The weekly jump variance lam_c is fixed ONCE from the jump-free robust core (iterating it
            # against the jump-aware core erodes the diffusive core band by band; recorded in EXPERIMENT_LOG).
            s_sc_b = _surge_scale_of(_b_xi_of(fit_j["vxi_b"]), band_z, z_K)
            jump_arg = (lam_j, math.sqrt(c_j) * s_sc_b) if c_j > 0.0 else None
            D_extra = (lam_c * (s_sc_b[:, None] ** 2)) * hs[None, :] if c_j > 0.0 else None
            surge_arg = None
            if fit_j["surge_lam"] > 0.0:
                surge_arg = (fit_j["surge_lam"], fit_j["surge_mu"], fit_j["surge_rho"],
                             _surge_scale_of(fit_j["b_xi_ref"], band_z, z_K))
            drift_arg = (fit_j["decline_var"], fit_j["decline_rho"]) if fit_j["decline_var"] > 0.0 else None
            ratio_j = _model_increment_ratio(
                fit_j["sL2_b"], fit_j["vxi_b"], fit_j["so2_b"], fit_j["phi"], s_vol, pool_mc, innov_mode, H,
                surge=surge_arg, drift=drift_arg, jump=jump_arg,
            )
            var_model = (2.0 * fit_j["so2_b"][:, None] + 2.0 * fit_j["vxi_b"][:, None] * (1.0 - fit_j["phi"] ** hs)[None, :]
                         + fit_j["sL2_b"][:, None] * hs[None, :])
            if D_extra is not None:
                var_model = var_model + D_extra
            if surge_arg is not None:
                Vs_j = _surge_variance(fit_j["surge_lam"], fit_j["surge_mu"], fit_j["surge_rho"])
                var_model = var_model + (surge_arg[3][:, None] ** 2) * (2.0 * Vs_j * (1.0 - fit_j["surge_rho"] ** hs))[None, :]
            if drift_arg is not None:
                var_model = var_model + (fit_j["decline_var"] * (drift_arg[2][:, None] ** 2)) * _f_vel(hs, fit_j["decline_rho"])[None, :]
            iqr_model = ratio_j * var_model
            sse = 0.0
            for b in range(nb - 1):
                okh = np.isfinite(D_rob_all[b]) & np.isfinite(iqr_model[b])
                if okh.sum() >= 4:
                    sse += float(np.sum((D_rob_all[b, okh] - iqr_model[b, okh]) ** 2) / max(float(np.sum(D_rob_all[b, okh] ** 2)), 1e-12))
            return fit_j, c_j, ratio_j, sse

        # (i) jump-free robust core
        fit0, _, ratio0, sse0 = _robust_fit_for(0.0, 0.0, fit_var, 3)
        # (ii) the lasting-variance gap between the mean-square fit and the jump-free robust core, computed ONCE
        if scale_moment == "meta":
            s_sc0 = _jump_scale_of(_b_xi_of(fit_var["vxi_b"]), band_z, z_K)
        else:
            s_sc0 = _surge_scale_of(_b_xi_of(fit0["vxi_b"]), band_z, z_K)
        gap0 = np.where(np.isfinite(fit_var["sL2_b"]) & np.isfinite(fit0["sL2_b"]),
                        np.maximum(fit_var["sL2_b"] - fit0["sL2_b"], 0.0), np.nan)
        ok0 = in_k & np.isfinite(gap0) & np.isfinite(fit_var["sL2_b"])
        lam_c0 = float(np.sum(w_oct[ok0] * gap0[ok0]) / max(float(np.sum(w_oct[ok0] * s_sc0[ok0] ** 2)), 1e-12)) if ok0.any() else 0.0
        # share of the TOTAL weekly lasting variance (core + surge + drift) carried by the gap
        tot_last = fit_var["sL2_b"].copy()
        if fit_var["surge_lam"] > 0.0:
            Vs0 = _surge_variance(fit_var["surge_lam"], fit_var["surge_mu"], fit_var["surge_rho"])
            bs0 = fit_var["surge_b"] if fit_var.get("surge_b") is not None else fit_var["b_xi_ref"]
            tot_last = tot_last + (_surge_scale_of(bs0, band_z, z_K) ** 2) * 2.0 * Vs0 * (1.0 - fit_var["surge_rho"])
        if fit_var["decline_var"] > 0.0:
            tot_last = tot_last + fit_var["decline_var"] * (_drift_scale_of(fit_var.get("decline_b", 0.0), band_z, z_K) ** 2)
        gap_share = float(np.sum(w_oct[ok0] * gap0[ok0]) / max(float(np.sum(w_oct[ok0] * tot_last[ok0])), 1e-12)) if ok0.any() else 0.0
        sse_profile = {0.0: sse0}

        if scale_moment == "meta":
            fit, ratio_last = fit_var, ratio0
            if ((not asymmetry) or s3_meta_jumps) and gap_share > JUMP_MIN_SHARE and lam_c0 > 0.0:
                jump_lam = JUMP_LAM_FIXED
                jump_c = lam_c0 / jump_lam
                sL2_rem = np.where(
                    np.isfinite(fit_var["sL2_b"]) & np.isfinite(fit0["sL2_b"]),
                    np.clip(fit0["sL2_b"], 0.15 * fit_var["sL2_b"], fit_var["sL2_b"]),
                    fit_var["sL2_b"],
                )
                fit = {**fit_var, "sL2_b": sL2_rem}
                jump_diag = dict(active=True, mode="meta_gaussian_split", gap_share=gap_share,
                                 gap_band=gap0.tolist(), lam_c=lam_c0,
                                 jump_var_per_week_at_K=jump_lam * jump_c, jump_sd_at_K=math.sqrt(jump_c))
        else:
            fit, ratio_last = fit0, ratio0
            # Symmetric jumps belong to the core model; the Stage 3 comparator's tail process is its surge.
            if (not asymmetry) and gap_share > JUMP_MIN_SHARE and lam_c0 > 0.0:
                fit_j, c_j, ratio_j, sse_j = _robust_fit_for(JUMP_LAM_ROBUST, lam_c0, fit0, 2)
                sse_profile[JUMP_LAM_ROBUST] = sse_j
                if c_j > 0.0:
                    jump_lam, fit, jump_c, ratio_last = JUMP_LAM_ROBUST, fit_j, c_j, ratio_j
                    jump_diag = dict(active=True, gap_share=gap_share, gap_band=gap0.tolist(), lam_c=lam_c0,
                                     sse_profile=sse_profile, jump_var_per_week_at_K=jump_lam * jump_c,
                                     jump_sd_at_K=math.sqrt(jump_c))
        if jump_diag is None:
            jump_diag = dict(active=False, gap_share=gap_share, gap_band=gap0.tolist(), lam_c=lam_c0, sse_profile=sse_profile)

    sse_best, phi_hat = fit["sse"], fit["phi"]
    so2_b, vxi_b, sL2_b = fit["so2_b"], fit["vxi_b"], fit["sL2_b"]
    surge_lam, surge_mu, surge_rho = fit["surge_lam"], fit["surge_mu"], fit["surge_rho"]
    surge_b = fit.get("surge_b")
    decline_var, decline_rho, decline_b = fit["decline_var"], fit["decline_rho"], fit.get("decline_b", 0.0)
    stage3_diag = fit["stage3_diag"]

    # 3. Fit 2-parameter smooth laws across inside-K bands (b = 0..nb-2)
    #    Each band is one octave of log-rank; cap band weights at 100 so the bottom
    #    octave (K/2..K) does not overwhelm the head octaves by 32x!

    # Size law for lasting volatility: log(sL2_b) = 2 log(sigma_L0) - 2 beta (L_b - L_K)
    ok_L = in_k & np.isfinite(sL2_b) & (sL2_b > 1e-7) & np.isfinite(band_level)
    if ok_L.sum() >= 3:
        c0_L, c1_L = _wls_line(band_level[ok_L] - L_K, np.log(sL2_b[ok_L]), w_oct[ok_L])
        sigma_L0 = float(math.exp(0.5 * c0_L))
        beta = float(np.clip(-0.5 * c1_L, -0.25, 0.50))
    else:
        med_s = float(np.nanmedian(sL2_b[in_k & (sL2_b > 0)])) if np.any(sL2_b[in_k] > 0) else 1e-3
        sigma_L0, beta = float(math.sqrt(med_s)), 0.0

    # Log-linear passing swing SD profile: log(sqrt(V_xi)) = a_xi + b_xi * (z - z_K)
    ok_xi = in_k & np.isfinite(vxi_b) & (vxi_b > 1e-7)
    if ok_xi.sum() >= 2:
        a_xi, b_xi = _wls_line(dz_b[ok_xi], 0.5 * np.log(vxi_b[ok_xi]), w_oct[ok_xi])
    else:
        a_xi, b_xi = -2.5, 0.0

    # Log-linear measurement/weekly-burst noise SD profile.
    # When Spec-B is provided, fit (a_obs, b_obs) strictly to excess variance so
    # sigma_obs_of(p, z) = sqrt(floor(z)^2 + excess(z)^2) >= floor(z) at EVERY rank z!
    if spec_b_pinned:
        exc2_b = np.maximum(so2_b - (so_band_fixed ** 2), 0.0)
        ok_exc = in_k & np.isfinite(exc2_b) & (exc2_b > 1e-7)
        if ok_exc.sum() >= 2:
            a_obs, b_obs = _wls_line(dz_b[ok_exc], 0.5 * np.log(exc2_b[ok_exc]), w_oct[ok_exc])
        elif ok_exc.any():
            a_obs, b_obs = 0.5 * math.log(float(np.nanmedian(exc2_b[ok_exc]))), 0.0
        else:
            a_obs, b_obs = -8.0, 0.0
    else:
        ok_so = in_k & np.isfinite(so2_b) & (so2_b > 1e-7)
        if ok_so.sum() >= 2:
            a_obs, b_obs = _wls_line(dz_b[ok_so], 0.5 * np.log(so2_b[ok_so]), w_oct[ok_so])
        else:
            med_g0 = float(np.nanmedian(D[in_k, 0])) if np.any(np.isfinite(D[in_k, 0])) else 0.05
            a_obs, b_obs = 0.5 * math.log(max(0.05 * med_g0, 1e-6)), 0.0

    # 4. Split-half entity volatility spread s_vol (bias-free at s = 0!) -- computed in step 1b.
    #    Default gamma_L = 0.0 (passing/burst volatility heterogeneity only; Stage 2 ablation
    #    proved gamma_L = 0 beats gamma_L > 0 across all horizons while saving 1 free scalar!).
    s_long = 0.0
    if fit_gamma_L and s_vol > 0.05:
        blk_v = 4 if T >= 48 else 2
        nb_v = T // blk_v
        if nb_v >= 12:
            with np.errstate(all="ignore"):
                Xb = np.array([np.nanmean(Xc[i * blk_v:(i + 1) * blk_v], axis=0) for i in range(nb_v)])
            h_blk = max(2, min(6, nb_v // 4))
            s_long = _split_half_vol_spread(Xb, pr_ord, K, N, step=h_blk)
            gamma_L = float(np.clip(s_long / s_vol, 0.0, 1.0))
        else:
            gamma_L = 0.0
    else:
        gamma_L = 0.0

    # 5. Intermittent weekly absence profile p_abs(z) vs. true permanent exit lam_exit
    obs_mat = np.isfinite(R)
    # Identify terminal disappearances: absent for the last >= 13 weeks (or last T//4 weeks if short)
    term_win = min(13, max(6, T // 5))
    if T > term_win + 4:
        ever_before = np.any(obs_mat[:T - term_win], axis=0)
        absent_tail = ~np.any(obs_mat[T - term_win:], axis=0)
        is_terminal = ever_before & absent_tail
        # Weekly permanent attrition rate in bottom half of buffer (ranks > K)
        bot_mask = pr_ord > K
        n_bot_wk = float(np.sum(obs_mat[:T - term_win, bot_mask]))
        lam_exit = float(np.clip(np.sum(is_terminal & bot_mask) / max(n_bot_wk, 1.0), 1e-4, 0.05))
    else:
        lam_exit = 0.002

    # Non-terminal weekly absence rate per band (exclude weeks strictly after an entity's last observed week)
    last_obs = np.where(np.any(obs_mat, axis=0), T - 1 - np.argmax(obs_mat[::-1], axis=0), -1)
    first_obs = np.where(np.any(obs_mat, axis=0), np.argmax(obs_mat, axis=0), T)
    t_idx = np.arange(T)[:, None]
    active_span = (t_idx >= first_obs[None, :]) & (t_idx <= last_obs[None, :])
    abs_rates = np.full(nb, np.nan)
    abs_w = np.zeros(nb)
    for b in range(nb):
        sel = band_all == b
        sp = active_span[:, sel]
        if sp.sum() >= 100:
            r_abs = float(np.sum(sp & (~obs_mat[:, sel])) / sp.sum())
            abs_rates[b] = r_abs
            abs_w[b] = float(sel.sum())
    ok_a = np.isfinite(abs_rates) & (abs_rates > 1e-5) & (abs_rates < 0.95)
    if ok_a.sum() >= 2:
        a_abs, b_abs = _wls_line(dz_b[ok_a], _logit(abs_rates[ok_a]), abs_w[ok_a])
    else:
        a_abs, b_abs = -8.0, 0.0

    # 6. Empirical standardized innovation pool (bounded by observed 0.05%–99.95%, zero-mean unit-var)
    if q_pool is not None:
        # Deterministic quantile grid -> compact & exact!
        pool = q_pool
        if asymmetry and surge_lam > 0.0:
            # Symmetrize core innovation pool when surges are active (eliminates skew double-counting!)
            pool = np.concatenate([pool, -pool])
        pool = (pool - np.mean(pool)) / max(float(np.std(pool)), 1e-6)
    else:
        pool = np.linspace(-3.0, 3.0, 1024)
        pool = pool / np.std(pool)

    # Bottom-buffer rebirth levels (bottom 20% of observed levels)
    bot_cut = max(int(0.8 * N), K)
    bottom_pool = ladder_obs[bot_cut:].copy()
    if bottom_pool.size < 10:
        bottom_pool = ladder_obs[-max(10, N // 10):].copy()

    u_lo = float(np.nanmin(band_level[in_k]) - L_K) if np.any(np.isfinite(band_level[in_k])) else -2.0
    u_hi = float(np.nanmax(band_level[in_k]) - L_K) if np.any(np.isfinite(band_level[in_k])) else 6.0

    # 7. Closed-form Atlas drift (total lasting variance incl. jumps) + optional smooth all-rank refinement
    jump_var_fn = None
    if jump_lam > 0.0 and jump_c > 0.0:
        _jfn = _surge_scale_of if jump_dist == "laplace" else _jump_scale_of
        def jump_var_fn(z_arr, _lam=jump_lam, _c=jump_c, _b=b_xi, _zk=z_K, _fn=_jfn):
            return _lam * _c * _fn(_b, z_arr, _zk) ** 2
    dz, dg = _closed_form_atlas_drift(sigma_L0, beta, L_K, u_lo, u_hi, ladder_obs, N, jump_var_fn=jump_var_fn)

    diag = dict(
        D=D.tolist(), H=H, sse_phi=float(sse_best), scale_moment=scale_moment,
        band_z=band_z.tolist(), band_level=band_level.tolist(),
        sL2_band=sL2_b.tolist(), vxi_band=vxi_b.tolist(), so2_band=so2_b.tolist(),
        abs_rates=abs_rates.tolist(), s_short=s_short, s_long=s_long,
        # Round 5: the mean-square fit kept beside the robust one; their lasting-variance gap is the
        # share of lasting variance carried by rare jumps that the core deliberately does not model.
        sL2_band_var=fit_var["sL2_b"].tolist(), vxi_band_var=fit_var["vxi_b"].tolist(),
        so2_band_var=fit_var["so2_b"].tolist(), phi_var=float(fit_var["phi"]),
    )
    if D_rob is not None:
        diag["D_rob"] = D_rob.tolist()
        diag["iqr_var_ratio_model"] = ratio_last.tolist()
        diag["jumps"] = jump_diag
    if stage3_diag is not None:
        diag["stage3"] = stage3_diag

    p = CommonParams(
        sigma_L0=sigma_L0, beta=beta, phi=phi_hat,
        a_xi=a_xi, b_xi=b_xi, a_obs=a_obs, b_obs=b_obs,
        s_vol=s_vol, gamma_L=gamma_L, a_abs=a_abs, b_abs=b_abs, lam_exit=lam_exit,
        K=int(K), N=int(N), L_K=L_K, z_K=z_K, u_lo=u_lo, u_hi=u_hi,
        drift_z=dz, drift_g=dg, bottom_pool=bottom_pool, innov_pool=pool,
        ladder_obs=ladder_obs, spec_b_pinned=spec_b_pinned,
        a_obs_floor=a_obs_floor, b_obs_floor=b_obs_floor, innov_mode=innov_mode,
        surge_lam=surge_lam, surge_mu=surge_mu, surge_rho=surge_rho, surge_b=surge_b,
        decline_var=decline_var, decline_rho=decline_rho, decline_b=decline_b,
        jump_lam=jump_lam, jump_c=jump_c, jump_dist=jump_dist,
        diag=diag,
    )

    if refine_drift:
        dz2, dg2, hist = _refine_drift_smooth(p, ladder_obs)
        p.diag["drift_rms_hist"] = hist
        p = replace(p, drift_z=dz2, drift_g=dg2)

    return p


# --------------------------------------------------------------------------- #
# Simulator state & panel generator
# --------------------------------------------------------------------------- #
class CommonState:
    """Weekly state of the Common Rank-Diffusion model."""

    def __init__(
        self,
        p: CommonParams,
        L0: np.ndarray,
        rng: np.random.Generator,
        xi0: np.ndarray | None = None,
        v0: np.ndarray | None = None,
        s0: np.ndarray | None = None,
        d0: np.ndarray | None = None,
        dec_state0: np.ndarray | None = None,
    ):
        self.p = p
        self.rng = rng
        self.L = np.asarray(L0, dtype=float).copy()
        self.N = self.L.size
        self.ids = np.arange(self.N, dtype=np.int64)
        self.next_id = self.N

        # Persistent entity amplitude v_i (clipped at +/- 2.5 s to avoid extreme lognormal draws)
        if v0 is not None:
            self.v = np.asarray(v0, dtype=float).copy()
        elif p.s_vol > 0:
            z_v = np.clip(rng.standard_normal(self.N), -2.5, 2.5)
            raw_v = np.exp(p.s_vol * z_v)
            self.v = raw_v / np.mean(raw_v)  # exact E[v] = 1
        else:
            self.v = np.ones(self.N)
        self.sqv_pass = np.sqrt(self.v)
        self.sqv_last = np.power(self.v, 0.5 * p.gamma_L) if p.gamma_L > 0 else np.ones(self.N)

        if xi0 is None:
            _, zp0 = self._ranks_and_z()
            self.xi = np.sqrt(v_xi_of(p, zp0)) * self.sqv_pass * rng.standard_normal(self.N)
        else:
            self.xi = np.asarray(xi0, dtype=float).copy()

        # Optional Stage 3 states (inert when parameters == 0)
        self.surge_mean = _surge_mean(p.surge_lam, p.surge_mu, p.surge_rho)
        if p.surge_lam > 0 and p.surge_mu > 0:
            if s0 is not None:
                self.surge = np.asarray(s0, dtype=float).copy()
            else:
                # Stationary burn-in for surge component (120-wk warmup so rho_s^120 < 0.20 even at hl=52)
                s_init = np.zeros(self.N)
                for _ in range(120):
                    s_init = p.surge_rho * s_init + (rng.random(self.N) < p.surge_lam) * p.surge_mu * np.minimum(rng.standard_exponential(self.N), SURGE_EXP_CAP)
                self.surge = s_init
        else:
            self.surge = None

        if p.decline_var > 0:
            if d0 is not None:
                self.drift_i = np.asarray(d0, dtype=float).copy()
            else:
                _, zp_d = self._ranks_and_z()
                self.drift_i = self._draw_entity_drift(self.N, zp_d)
        else:
            self.drift_i = None

        if p.decline_delta > 0 and p.decline_enter > 0:
            self.dec_pi = p.decline_enter / max(p.decline_enter + max(p.decline_exit, p.lam_exit), 1e-9)
            if dec_state0 is not None:
                self.dec_state = np.asarray(dec_state0, dtype=bool).copy()
            else:
                self.dec_state = rng.random(self.N) < self.dec_pi
        else:
            self.dec_state = None

    def _draw_innov(self, n: int) -> np.ndarray:
        if self.p.innov_mode == "empirical":
            pool = self.p.innov_pool
            return pool[self.rng.integers(0, pool.size, size=n)]
        return self.rng.standard_normal(n)

    def _draw_entity_drift(self, n: int, zp: np.ndarray | None = None) -> np.ndarray:
        sd = _drift_sd_of(self.p, zp) if (zp is not None and self.p.decline_b != 0.0) else math.sqrt(self.p.decline_var)
        if self.p.decline_skew == 0.0:
            return np.clip(sd * self.rng.standard_normal(n), -2.5 * sd, 2.5 * sd)
        # Negatively skewed centered exponential mixture: -(Exp(1) - 1) scaled to sd
        z = -(self.rng.standard_exponential(n) - 1.0)
        z = np.clip(z, -4.0, 1.5)
        z = (z - np.mean(z)) / max(float(np.std(z)), 1e-6) if n > 20 else z
        w = float(np.clip(self.p.decline_skew, 0.0, 1.0))
        g = w * z + math.sqrt(max(1.0 - w * w, 0.0)) * self.rng.standard_normal(n)
        return np.clip(sd * g, -3.5 * sd, 2.5 * sd)

    def _draw_v(self, n: int) -> np.ndarray:
        if self.p.s_vol <= 0:
            return np.ones(n)
        z_v = np.clip(self.rng.standard_normal(n), -2.5, 2.5)
        return np.exp(-0.5 * self.p.s_vol * self.p.s_vol + self.p.s_vol * z_v)

    def _ranks_and_z(self) -> tuple[np.ndarray, np.ndarray]:
        order = np.argsort(-self.L)
        prank = np.empty(self.N, dtype=np.int64)
        prank[order] = np.arange(1, self.N + 1)
        zp = np.log(np.clip((prank - 0.5) / self.N, Z_CLIP, 1.0))
        return prank, zp

    def step_lasting(self) -> None:
        p, rng, N = self.p, self.rng, self.N
        prank, zp = self._ranks_and_z()
        sL = sigma_L_of(p, self.L)
        g = np.interp(zp, p.drift_z, p.drift_g)
        self.L = self.L + g + sL * self.sqv_last * self._draw_innov(N)

        if p.jump_lam > 0.0 and p.jump_c > 0.0:
            hit = rng.random(N) < p.jump_lam
            nh = int(hit.sum())
            if nh:
                self.L[hit] = self.L[hit] + _jump_sd_of(p, zp[hit]) * _draw_jump_unit(p, rng, nh)

        if self.drift_i is not None:
            self.L = self.L + self.drift_i
            if p.decline_rho < 1.0:
                sd_d_cur = _drift_sd_of(p, zp) if p.decline_b != 0.0 else math.sqrt(p.decline_var)
                innov_sd = sd_d_cur * math.sqrt(max(1.0 - p.decline_rho ** 2, 0.0))
                self.drift_i = p.decline_rho * self.drift_i + innov_sd * rng.standard_normal(N)

        if self.dec_state is not None:
            # Decline-state drift, centered so mean across population is 0
            self.L = self.L - p.decline_delta * (self.dec_state.astype(float) - self.dec_pi)
            u = rng.random(N)
            if p.decline_exit > 0:
                self.dec_state = np.where(self.dec_state, u >= p.decline_exit, u < p.decline_enter)
            else:
                # Absorbing until rebirth!
                self.dec_state = self.dec_state | (u < p.decline_enter)

        # True permanent attrition only in bottom half of buffer (ranks > K)
        ex = (prank > p.K) & (rng.random(N) < p.lam_exit)
        ne = int(ex.sum())
        if ne:
            self.L[ex] = p.bottom_pool[rng.integers(0, p.bottom_pool.size, ne)]
            self.xi[ex] = 0.0
            self.ids[ex] = np.arange(self.next_id, self.next_id + ne)
            self.next_id += ne
            if p.s_vol > 0:
                self.v[ex] = self._draw_v(ne)
                self.sqv_pass[ex] = np.sqrt(self.v[ex])
                self.sqv_last[ex] = np.power(self.v[ex], 0.5 * p.gamma_L) if p.gamma_L > 0 else 1.0
            if self.surge is not None:
                self.surge[ex] = 0.0
            if self.drift_i is not None:
                self.drift_i[ex] = self._draw_entity_drift(ne, zp[ex])
            if self.dec_state is not None:
                self.dec_state[ex] = rng.random(ne) < self.dec_pi

    def step(self, apply_absence: bool = True) -> np.ndarray:
        p, rng, N = self.p, self.rng, self.N
        self.step_lasting()
        _, zp = self._ranks_and_z()
        vxi = v_xi_of(p, zp)
        so = sigma_obs_of(p, zp)
        s_xi_innov = np.sqrt(vxi * max(1.0 - p.phi * p.phi, 1e-6))
        self.xi = p.phi * self.xi + s_xi_innov * self.sqv_pass * self._draw_innov(N)
        X = self.L + self.xi + so * self.sqv_pass * self._draw_innov(N)

        if self.surge is not None:
            self.surge = p.surge_rho * self.surge + (rng.random(N) < p.surge_lam) * p.surge_mu * np.minimum(rng.standard_exponential(N), SURGE_EXP_CAP)
            s_sc = _surge_scale_of(_surge_b_of(p), zp, p.z_K)
            X = X + s_sc * (self.surge - self.surge_mean)

        if apply_absence and p.a_abs > -7.5:
            p_abs = p_abs_of(p, zp)
            absent = rng.random(N) < p_abs
            if absent.any():
                X = np.where(absent, np.nan, X)
        return X


def simulate_common_panel(
    p: CommonParams,
    L0: np.ndarray,
    T_sim: int,
    seed: int = 0,
    apply_absence: bool = True,
    burn: int = 0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Simulate a full weekly panel (T_sim x N). Returns (X, R, ids)."""
    rng = np.random.default_rng(seed)
    st = CommonState(p, L0, rng)
    for _ in range(burn):
        st.step(apply_absence=False)
    Xs = np.empty((T_sim, st.N), dtype=np.float64)
    Rs = np.full((T_sim, st.N), np.nan, dtype=np.float64)
    ids = np.empty((T_sim, st.N), dtype=np.int64)
    for t in range(T_sim):
        x = st.step(apply_absence=apply_absence)
        Xs[t] = x
        ids[t] = st.ids
        ok = np.flatnonzero(np.isfinite(x))
        if ok.size:
            ord_ok = ok[np.argsort(-x[ok], kind="stable")]
            Rs[t, ord_ok] = np.arange(1, ord_ok.size + 1, dtype=float)
    return Xs, Rs, ids


# --------------------------------------------------------------------------- #
# Calendar-aligned 2-state / 4-state Kalman Filter + Robust EB v_i (Goal 2!)
# --------------------------------------------------------------------------- #
def _volatility_posterior(
    p: CommonParams,
    dx: np.ndarray,
    z: np.ndarray,
    sL: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Estimate passing amplitude v_i from the median absolute 1-week increment.

    Adopted from Astra 6.2 (2026-11-25): at fixed permanent rank, integrates over
    the model's independent innovations (preserving the non-scaling lasting-variance
    floor sigma_L^2). For each trial log(v) on a 41-point lognormal prior grid,
    compares the model-implied median |dX_1| against the endpoint's observed median
    |dX_1| using the asymptotic order-statistic standard error 1 / (2 m f(m) sqrt(n/2)).
    Zero new free scalars; immune to rare extreme bursts that distort mean(dX^2).
    """
    n = np.isfinite(dx).sum(axis=0)
    valid = n >= 4
    v_hat = np.ones(z.size, dtype=np.float64)
    post_sd = np.full(z.size, p.s_vol, dtype=np.float64)
    if not valid.any():
        return v_hat, post_sd
    measured = np.nanmedian(np.abs(dx[:, valid]), axis=0)
    rng = np.random.default_rng(71329)
    size = 8192

    def draw() -> np.ndarray:
        if p.innov_mode == "empirical":
            return rng.choice(p.innov_pool, size=size)
        return rng.standard_normal(size)

    xi = rng.standard_normal(size)
    n_burn_xi = max(1, math.ceil(math.log(0.001) / math.log(p.phi))) if 0.0 < p.phi < 1.0 else 1
    for _ in range(n_burn_xi):
        xi = p.phi * xi + math.sqrt(max(1.0 - p.phi ** 2, 0.0)) * draw()
    xi_delta = (p.phi - 1.0) * xi + math.sqrt(max(1.0 - p.phi ** 2, 0.0)) * draw()
    obs_delta = draw() - draw()
    lasting = draw()
    drift_unit = rng.standard_normal(size)
    surge_delta = np.zeros(size, dtype=np.float64)
    if p.surge_lam > 0.0 and 0.0 < p.surge_rho < 1.0:
        level = np.full(size, _surge_mean(p.surge_lam, p.surge_mu, p.surge_rho), dtype=np.float64)
        for _ in range(math.ceil(math.log(0.001) / math.log(p.surge_rho))):
            jump = (rng.random(size) < p.surge_lam) * np.minimum(rng.standard_exponential(size), SURGE_EXP_CAP)
            level = p.surge_rho * level + p.surge_mu * jump
        jump = (rng.random(size) < p.surge_lam) * np.minimum(rng.standard_exponential(size), SURGE_EXP_CAP)
        surge_delta = (p.surge_rho - 1.0) * level + p.surge_mu * jump

    jump_delta = np.zeros(size, dtype=np.float64)
    if p.jump_lam > 0.0 and p.jump_c > 0.0:
        jump_delta = (rng.random(size) < p.jump_lam) * _draw_jump_unit(p, rng, size)

    order = np.argsort(z)
    nodes = np.unique(np.linspace(0, z.size - 1, min(25, z.size)).round().astype(int))
    nodes = np.unique(np.r_[nodes, np.geomspace(1, z.size, min(25, z.size)).round().astype(int) - 1])
    ix = order[nodes]
    z_grid = z[ix]
    z_eval = z[valid]
    passing_var = 2.0 * (sigma_obs_of(p, z) ** 2) + 2.0 * (1.0 - p.phi) * v_xi_of(p, z)
    scale = _surge_scale_of(_surge_b_of(p), z, p.z_K)
    drift_sd = _drift_sd_of(p, z)
    jump_sd = _jump_sd_of(p, z)
    fixed_var = ((drift_sd ** 2) + (scale ** 2) * 2.0 * _surge_variance(p.surge_lam, p.surge_mu, p.surge_rho) * (1.0 - p.surge_rho)
                 + p.jump_lam * jump_sd ** 2)

    grid = np.linspace(-2.5, 2.5, 41) * p.s_vol
    log_prior = -0.5 * ((grid + 0.5 * (p.s_vol ** 2)) / p.s_vol) ** 2
    log_weights = np.empty((grid.size, int(valid.sum())), dtype=np.float64)
    pass_draws = np.sqrt(v_xi_of(p, z_grid))[:, None] * xi_delta + sigma_obs_of(p, z_grid)[:, None] * obs_delta
    fixed_draws = drift_sd[ix, None] * drift_unit + scale[ix, None] * surge_delta + jump_sd[ix, None] * jump_delta
    for j, logv in enumerate(grid):
        v = math.exp(logv)
        samples = fixed_draws + sL[ix, None] * (v ** (p.gamma_L / 2.0)) * lasting + math.sqrt(v) * pass_draws
        q45, med, q55 = np.quantile(np.abs(samples), [0.45, 0.5, 0.55], axis=1)
        variance = np.maximum((sL ** 2) * (v ** p.gamma_L) + passing_var * v + fixed_var, 1e-12)
        pred = np.interp(z_eval, z_grid, med / np.sqrt(variance[ix])) * np.sqrt(variance[valid])
        unit_se = np.interp(z_eval, z_grid, (q55 - q45) / np.maximum(0.2 * med, 1e-12))
        se = np.maximum(unit_se, 1e-4) / np.sqrt(n[valid] / 2.0)
        residual = np.log(np.maximum(measured, 1e-12) / np.maximum(pred, 1e-12))
        log_weights[j] = log_prior[j] - np.log(se) - 0.5 * ((residual / se) ** 2)
    weights = np.exp(log_weights - log_weights.max(axis=0))
    weights /= weights.sum(axis=0)
    v_hat[valid] = np.exp(grid) @ weights
    mean_log = grid @ weights
    post_sd[valid] = np.sqrt(np.maximum(0.0, (grid ** 2) @ weights - mean_log ** 2))
    return v_hat, post_sd


def filter_endpoints(
    X_tr: np.ndarray,
    R_tr: np.ndarray,
    p: CommonParams,
    floors_tr: np.ndarray | None = None,
) -> dict:
    """Vectorized 2-state/4-state Kalman filter with robust Empirical-Bayes entity volatility v_i.

    Operates strictly on the training matrix X_tr (T0 x N) with ZERO lookahead.
    Returns posterior means (L_hat, xi_hat[, d_hat, s_hat]), posterior covariances
    (P11, P12, P22[, P4]), shrunk entity amplitude v_hat and posterior log-v SD (post_s_v).
    """
    X_tr = np.asarray(X_tr, dtype=np.float64)
    R_tr = np.asarray(R_tr, dtype=np.float64)
    T0, N = X_tr.shape
    if floors_tr is None:
        floors_tr = np.sum(np.isfinite(R_tr), axis=1).astype(float) + 1.0

    # Top-K median reference removed for filtering, restored at the end
    pmean = _top_k_center(X_tr, R_tr, p.K)
    last_pmean = float(pmean[-1, 0]) if np.isfinite(pmean[-1, 0]) else 0.0
    Xc = X_tr - pmean

    # Permanent rank & z in train window
    pen = np.where(np.isfinite(R_tr), R_tr, floors_tr[:, None])
    rperm = np.mean(pen, axis=0)
    pr_ord = np.empty(N, dtype=float)
    pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
    z_i = np.log(np.clip((pr_ord - 0.5) / N, Z_CLIP, 1.0))

    # 1. Robust Empirical-Bayes entity amplitude v_hat from median |dX_1|
    dX = np.diff(Xc, axis=0)
    dX = dX - np.nanmean(dX, axis=1, keepdims=True)
    vxi_i = v_xi_of(p, z_i)
    so_i = sigma_obs_of(p, z_i)
    with np.errstate(all="ignore"):
        mean_X_i = np.nanmean(X_tr, axis=0)
    mean_X_i = np.where(np.isfinite(mean_X_i), mean_X_i, p.L_K - 1.0)
    sL_i = sigma_L_of(p, mean_X_i)
    has_drift = p.decline_var > 0.0
    has_surge = (p.surge_lam > 0.0) and (p.surge_mu > 0.0) and (p.surge_rho > 0.0)
    has_stage3 = has_drift or has_surge
    Vs = _surge_variance(p.surge_lam, p.surge_mu, p.surge_rho) if has_surge else 0.0
    if p.s_vol > 0.02:
        v_hat, post_s_v = _volatility_posterior(p, dX, z_i, sL_i)
    else:
        v_hat = np.ones(N)
        post_s_v = np.zeros(N)

    # 2. Vectorized Kalman filter: exact 2-state [L, xi] for Stage 2 (bit-identical!),
    #    exact 4-state [L, xi, d, s_tilde] when Stage 3 asymmetry is active.
    qL = (sL_i ** 2) * np.power(v_hat, p.gamma_L) + p.jump_lam * (_jump_sd_of(p, z_i) ** 2)
    q_xi = vxi_i * max(1.0 - p.phi * p.phi, 1e-6) * v_hat
    R_obs = np.maximum((so_i ** 2) * v_hat, 1e-6)
    g_i = np.interp(z_i, p.drift_z, p.drift_g)

    if not has_stage3:
        L = np.full(N, np.nan)
        xi = np.zeros(N)
        P11 = np.full(N, 1.0)
        P12 = np.zeros(N)
        P22 = vxi_i * v_hat
        phi = p.phi

        for t in range(T0):
            obs = np.isfinite(Xc[t])
            # Predict (only for already-initialized entities)
            init = np.isfinite(L)
            if t > 0 and init.any():
                L[init] = L[init] + g_i[init]
                xi[init] = phi * xi[init]
                P11[init] = P11[init] + qL[init]
                P12[init] = phi * P12[init]
                P22[init] = (phi * phi) * P22[init] + q_xi[init]

            # First observation initializes L = X0, xi = 0 -> exact error cov Cov(L0 - X0, xi0) = -V_xi*v_hat
            first = obs & ~init
            if first.any():
                L[first] = Xc[t, first]
                xi[first] = 0.0
                P11[first] = R_obs[first] + vxi_i[first] * v_hat[first]
                P12[first] = -vxi_i[first] * v_hat[first]
                P22[first] = vxi_i[first] * v_hat[first]

            # Kalman update for observed & initialized entities
            u = obs & init
            if u.any():
                S = P11[u] + 2.0 * P12[u] + P22[u] + R_obs[u]
                K1 = (P11[u] + P12[u]) / S
                K2 = (P12[u] + P22[u]) / S
                innov = Xc[t, u] - L[u] - xi[u]
                L[u] = L[u] + K1 * innov
                xi[u] = xi[u] + K2 * innov
                p11, p12, p22 = P11[u], P12[u], P22[u]
                P11[u] = np.maximum(p11 - K1 * (p11 + p12), 1e-8)
                P12[u] = p12 - K1 * (p12 + p22)
                P22[u] = np.maximum(p22 - K2 * (p12 + p22), 1e-8)

        # Restore level scale to last training week's common level
        fallback_L = float(np.nanmin(L)) - 0.5 if np.any(np.isfinite(L)) else 0.0
        L = np.where(np.isfinite(L), L, fallback_L) + last_pmean

        return dict(
            L_hat=L, xi_hat=xi, P11=P11, P12=P12, P22=P22,
            v_hat=v_hat, post_s_v=post_s_v, last_pmean=last_pmean,
        )

    # Exact vectorized 4-state Kalman filter for [L, xi, d, s_obs] where s_obs = s_sc_i * (s - s_mean)
    phi = float(p.phi)
    rho_d = float(p.decline_rho) if has_drift else 0.0
    rho_s = float(p.surge_rho) if has_surge else 0.0
    d_sd_i = _drift_sd_of(p, z_i) if has_drift else np.zeros(N)
    v_d0_i = d_sd_i ** 2
    q_d_i = v_d0_i * max(1.0 - rho_d * rho_d, 0.0) if has_drift else np.zeros(N)
    s_sc_i = _surge_scale_of(_surge_b_of(p), z_i, p.z_K) if has_surge else np.ones(N)
    q_s_i = (s_sc_i ** 2) * _surge_innov_var(p.surge_lam, p.surge_mu) if has_surge else np.zeros(N)
    v_s0_i = (s_sc_i ** 2) * float(Vs) if has_surge else np.zeros(N)

    F = np.array([
        [1.0, 0.0, 1.0 if has_drift else 0.0, 0.0],
        [0.0, phi, 0.0, 0.0],
        [0.0, 0.0, rho_d, 0.0],
        [0.0, 0.0, 0.0, rho_s],
    ], dtype=np.float64)
    FT = F.T
    H_vec = np.array([1.0, 1.0, 0.0, 1.0 if has_surge else 0.0], dtype=np.float64)

    x_st = np.zeros((N, 4), dtype=np.float64)
    x_st[:, 0] = np.nan
    P = np.zeros((N, 4, 4), dtype=np.float64)
    P[:, 0, 0] = 1.0
    P[:, 1, 1] = vxi_i * v_hat
    P[:, 2, 2] = v_d0_i
    P[:, 3, 3] = v_s0_i

    for t in range(T0):
        obs = np.isfinite(Xc[t])
        init = np.isfinite(x_st[:, 0])
        if t > 0 and init.any():
            xi_sub = x_st[init]
            # x <- F x + [g_i, 0, 0, 0]
            x_new = xi_sub @ FT
            x_new[:, 0] += g_i[init]
            x_st[init] = x_new
            # P <- F P F^T + diag(Q)
            P_sub = np.matmul(np.matmul(F, P[init]), FT)
            P_sub[:, 0, 0] += qL[init]
            P_sub[:, 1, 1] += q_xi[init]
            if has_drift:
                P_sub[:, 2, 2] += q_d_i[init]
            if has_surge:
                P_sub[:, 3, 3] += q_s_i[init]
            P[init] = P_sub

        first = obs & ~init
        if first.any():
            x_st[first, 0] = Xc[t, first]
            x_st[first, 1:] = 0.0
            P[first] = 0.0
            P[first, 0, 0] = R_obs[first] + vxi_i[first] * v_hat[first] + v_s0_i[first]
            P[first, 0, 1] = -vxi_i[first] * v_hat[first]
            P[first, 1, 0] = -vxi_i[first] * v_hat[first]
            P[first, 1, 1] = vxi_i[first] * v_hat[first]
            P[first, 2, 2] = v_d0_i[first]
            P[first, 0, 3] = -v_s0_i[first]
            P[first, 3, 0] = -v_s0_i[first]
            P[first, 3, 3] = v_s0_i[first]

        u = obs & init
        if u.any():
            Pu = P[u]
            HP = np.einsum("nij,j->ni", Pu, H_vec)
            S = np.maximum(HP @ H_vec + R_obs[u], 1e-8)
            K_gain = HP / S[:, None]
            innov = Xc[t, u] - (x_st[u] @ H_vec)
            x_st[u] = x_st[u] + K_gain * innov[:, None]
            Pu = Pu - K_gain[:, :, None] * HP[:, None, :]
            Pu = 0.5 * (Pu + Pu.swapaxes(-1, -2))
            Pu[:, 0, 0] = np.maximum(Pu[:, 0, 0], 1e-8)
            Pu[:, 1, 1] = np.maximum(Pu[:, 1, 1], 1e-8)
            if has_drift:
                Pu[:, 2, 2] = np.maximum(Pu[:, 2, 2], 1e-10)
            if has_surge:
                Pu[:, 3, 3] = np.maximum(Pu[:, 3, 3], 1e-10)
            P[u] = Pu

    L = x_st[:, 0]
    fallback_L = float(np.nanmin(L)) - 0.5 if np.any(np.isfinite(L)) else 0.0
    L = np.where(np.isfinite(L), L, fallback_L) + last_pmean

    return dict(
        L_hat=L, xi_hat=x_st[:, 1], d_hat=x_st[:, 2], s_hat=x_st[:, 3], s_scale=s_sc_i, d_sd=d_sd_i,
        P11=P[:, 0, 0], P12=P[:, 0, 1], P22=P[:, 1, 1], P4=P,
        v_hat=v_hat, post_s_v=post_s_v, last_pmean=last_pmean,
    )


def forecast_endpoints(
    filt: dict,
    p: CommonParams,
    horizons: list[int],
    n_ens: int = 20,
    seed: int = 0,
    apply_absence: bool = True,
    use_eb_vol: bool = True,
    d0: np.ndarray | None = None,
    s0: np.ndarray | None = None,
    dec_state0: np.ndarray | None = None,
    return_totals: bool = False,
) -> dict[int, tuple]:
    """STRICTLY CALENDAR-ALIGNED ensemble forecasts from end-of-train filtered state.

    Step 1 corresponds to week T0 (exactly h = 1 week after last train week T0 - 1),
    fixing the 1-week extra-transition timing bug discovered by Codex V3 in rankdiff_kalman.py.

    Returns dict mapping h -> (X_ens, R_ens) [or (X_ens, R_ens, med_K_ens, tot_K_ens) when
    return_totals=True, computed on ALL currently present top-K slots including reborn
    competitors before masking departed original IDs].
    """
    H_max = max(horizons)
    hor_set = set(horizons)
    L_hat, xi_hat = filt["L_hat"], filt["xi_hat"]
    P11, P12, P22 = filt["P11"], filt["P12"], filt["P22"]
    v_hat, post_s_v = filt["v_hat"], filt["post_s_v"]
    N = L_hat.size

    use_4state = "P4" in filt
    if use_4state:
        P4 = filt["P4"]
        # Regularize slightly on diagonal for numerical PSD guarantee before Cholesky
        eye4 = np.eye(4, dtype=np.float64)[None, :, :]
        chol4 = np.linalg.cholesky(P4 + 1e-10 * eye4)
        d_hat = filt["d_hat"]
        s_hat = filt["s_hat"]
        s_sc = filt.get("s_scale", np.ones(N))
        has_drift = p.decline_var > 0.0
        has_surge = (p.surge_lam > 0.0) and (p.surge_mu > 0.0) and (p.surge_rho > 0.0)
        sd_d = filt.get("d_sd", math.sqrt(p.decline_var)) if has_drift else 0.0
        s_mean = _surge_mean(p.surge_lam, p.surge_mu, p.surge_rho) if has_surge else 0.0
    else:
        # Cholesky of 2x2 posterior covariance per entity for exact state uncertainty sampling
        c11 = np.sqrt(np.maximum(P11, 1e-10))
        c21 = P12 / c11
        c22 = np.sqrt(np.maximum(P22 - c21 * c21, 1e-10))

    out_X = {h: np.empty((N, n_ens), dtype=np.float64) for h in horizons}
    out_R = {h: np.full((N, n_ens), np.nan, dtype=np.float64) for h in horizons}
    out_med = {h: np.empty(n_ens, dtype=np.float64) for h in horizons} if return_totals else None
    out_tot = {h: np.empty(n_ens, dtype=np.float64) for h in horizons} if return_totals else None

    for m in range(n_ens):
        rng = np.random.default_rng(seed + m)
        if use_4state:
            z4 = rng.standard_normal((N, 4))
            draw = np.einsum("nij,nj->ni", chol4, z4)
            L_init = L_hat + draw[:, 0]
            xi_init = xi_hat + draw[:, 1]
            if has_drift:
                d_init = d0 if d0 is not None else np.clip(d_hat + draw[:, 2], -2.8 * sd_d, 2.8 * sd_d)
            else:
                d_init = d0
            if has_surge:
                if s0 is not None:
                    s_init = s0
                else:
                    # Clamp physical surge non-negative WITHOUT permanently shifting L_init!
                    s_obs_raw = s_hat + draw[:, 3]
                    s_unit_raw = s_obs_raw / np.maximum(s_sc, 1e-4) + s_mean
                    s_init = np.maximum(s_unit_raw, 0.0)
            else:
                s_init = s0
        else:
            z1 = rng.standard_normal(N)
            z2 = rng.standard_normal(N)
            L_init = L_hat + c11 * z1
            xi_init = xi_hat + c21 * z1 + c22 * z2
            d_init = d0
            s_init = s0

        if use_eb_vol:
            if p.s_vol > 0.02:
                # Mean-preserving posterior draw around log(v_hat): E[v_init | data] = v_hat
                lv_draw = np.log(v_hat) - 0.5 * (post_s_v ** 2) + post_s_v * np.clip(rng.standard_normal(N), -2.0, 2.0)
                v_init = np.exp(np.clip(lv_draw, -2.5 * p.s_vol, 2.5 * p.s_vol))
            else:
                v_init = np.ones(N)
        else:
            v_init = None  # fresh random v_i (ablation control!)

        st = CommonState(p, L_init, rng, xi0=xi_init, v0=v_init, d0=d_init, s0=s_init, dec_state0=dec_state0)
        for step_h in range(1, H_max + 1):
            x = st.step(apply_absence=apply_absence)
            if step_h in hor_set:
                # Track original identities: if a slot was reborn, mark as NaN (exited)!
                alive = st.ids == np.arange(N, dtype=np.int64)
                x_alive = np.where(alive, x, np.nan)
                out_X[step_h][:, m] = x_alive
                # Rank among ALL currently present slots (including reborn ones, exact match to reality!)
                ok = np.flatnonzero(np.isfinite(x))
                if ok.size:
                    ord_ok = ok[np.argsort(-x[ok], kind="stable")]
                    rk = np.full(N, np.nan)
                    rk[ord_ok] = np.arange(1, ord_ok.size + 1, dtype=float)
                    out_R[step_h][:, m] = np.where(alive, rk, np.nan)
                    if return_totals:
                        top_k_idx = ord_ok[:min(p.K, ord_ok.size)]
                        out_med[step_h][m] = float(np.median(x[top_k_idx]))
                        out_tot[step_h][m] = float(np.sum(np.expm1(np.clip(x[top_k_idx], 0.0, 40.0))))
                elif return_totals:
                    out_med[step_h][m] = np.nan
                    out_tot[step_h][m] = np.nan

    if return_totals:
        return {h: (out_X[h], out_R[h], out_med[h], out_tot[h]) for h in horizons}
    return {h: (out_X[h], out_R[h]) for h in horizons}
