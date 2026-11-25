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
    decline_var: float = 0.0     # variance of persistent entity drift (or skewed decline component)
    decline_rho: float = 0.0     # weekly persistence of entity drift (1.0 = fixed, <1 = AR(1))
    decline_skew: float = 0.0    # asymmetry parameter (0 = symmetric, >0 = negatively skewed / decline-state)
    decline_enter: float = 0.0   # weekly hazard of entering decline state (if state-switching)
    decline_exit: float = 0.0    # weekly hazard of leaving decline state (0 = absorbing to exit)
    decline_delta: float = 0.0   # weekly drift rate inside decline state

    diag: dict | None = None

    def n_free_scalars(self) -> int:
        # 11 core scalars when gamma_L == 0 (default):
        # sigma_L0, beta, phi, a_xi, b_xi, a_obs, b_obs, s_vol, a_abs, b_abs, lam_exit
        base = 11 + (1 if self.gamma_L > 0 else 0)
        extra = 0
        if self.surge_lam > 0:
            extra += 3  # surge_lam, surge_mu, surge_rho (hl_s)
        if self.decline_var > 0:
            extra += 1 + (1 if self.decline_skew != 0 else 0)  # sigma_d (hl_d=156 fixed) [+ skew]
        if self.decline_delta > 0:
            extra += 2 + (1 if self.decline_exit > 0 else 0)
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


def _structure_function(X: np.ndarray, band_of: np.ndarray, nb: int, H: int) -> np.ndarray:
    """D_b(h) = E[Var_b(X_{t+h} - X_t)] by permanent-rank band, removing within-band common
    time/tier shifts with exact Bessel correction n_t / (n_t - 1)."""
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
            ss = float(np.nansum(dm * dm))
            df = float(np.sum(cnt_ok - 1.0))
            if df >= 35.0:
                D[b, h - 1] = ss / df
    return D


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
                             ladder_desc: np.ndarray, N: int) -> tuple[np.ndarray, np.ndarray]:
    """Closed-form Atlas balance drift g(k) = -d(k s_k)/dk with s_k = sigma_L^2(k) / (2 S_k)."""
    k = np.unique(np.round(np.geomspace(1, N, 64)).astype(int)).astype(float)
    S, lev = _smooth_ladder_slope(ladder_desc, k)
    u = np.clip(lev - L_K, u_lo, u_hi)
    sig2 = (sigma_L0 * np.exp(-beta * u)) ** 2
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
    """Rank-dependent surge amplitude scaling inherited from the passing profile b_xi (0 extra scalars).
    Normalized so top-K RMS scale is 1.0 when b_xi > 0, while damping extreme head jumps."""
    b_pos = max(float(b_xi), 0.0)
    if b_pos <= 1e-6:
        return np.ones_like(z, dtype=float)
    dz = np.clip(z - z_K, -8.0, 0.0)
    raw = np.exp(b_pos * dz)
    # Analytical RMS over top-K (integral_0^1 u^(2*b) du = 1/(1+2b))
    rms = math.sqrt(1.0 / (1.0 + 2.0 * b_pos))
    return raw / rms


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
) -> tuple[float, float, dict]:
    """Identify persistent entity drift variance sigma_d^2 (fixed hl_d = 156w) in closed form
    from gapped window-change covariance Cov(x, y) net of passing AR(1) and surge covariances.

    Returns (decline_var, decline_rho, diag). Automatically returns 0 when net Cov <= 0 or T < 75.
    """
    T = Xc.shape[0]
    rho_d = float(0.5 ** (1.0 / DRIFT_HL_FIXED))
    if T >= 160:
        W, H, g = 13, 52, 13
    elif T >= 75:
        W, H, g = 4, 26, 4
    else:
        return 0.0, 0.0, {"active": False, "reason": "short_panel", "T": int(T)}

    sel = (pr_ord <= K) & (pres >= 0.5)
    if sel.sum() < 50:
        return 0.0, 0.0, {"active": False, "reason": "few_entities"}

    # Permanent-rank strata (K/16, K/4, K) so tier-level growth trends cannot mimic entity drift!
    pr_sub = pr_ord[sel]
    lab_sub = np.where(pr_sub <= (K / 16.0), 0, np.where(pr_sub <= (K / 4.0), 1, 2))
    Xsub = Xc[:, sel]
    covs = []
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

    if not covs:
        return 0.0, 0.0, {"active": False, "reason": "no_origins"}

    cov_emp = float(np.mean(covs))
    c_xi = Vxi_mean * _G_ar1(phi, W, H, g)
    c_s = Vs * _G_ar1(rho_s, W, H, g) if (Vs > 0.0 and rho_s > 0.0) else 0.0
    gd = _G_drift(rho_d, W, H, g)
    net_cov = cov_emp - c_xi - c_s
    sd2 = max(net_cov / max(gd, 1e-9), 0.0)
    if sd2 < 1e-7:
        return 0.0, 0.0, {
            "active": False, "W": W, "H": H, "g": g,
            "cov_emp": cov_emp, "c_xi": c_xi, "c_s": c_s, "net_cov": net_cov, "G_d": gd,
        }
    return float(sd2), rho_d, {
        "active": True, "W": W, "H": H, "g": g, "hl_d": DRIFT_HL_FIXED,
        "cov_emp": cov_emp, "c_xi": c_xi, "c_s": c_s, "net_cov": net_cov, "G_d": gd,
        "sigma_d": math.sqrt(sd2),
    }


def _fit_structure_profiles(
    D: np.ndarray,
    hs: np.ndarray,
    nb: int,
    spec_b_pinned: bool,
    so_band_fixed: np.ndarray | None,
    D_subtract: np.ndarray | None = None,
) -> tuple[float, float, np.ndarray, np.ndarray, np.ndarray]:
    """Profile NNLS over pooled phi for D_b(h) (minus optional Stage 3 variance D_subtract(h))."""
    w_h = np.ones_like(hs)
    best = None
    for phi in PHI_GRID:
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
            if D_subtract is not None:
                # Subtract Stage 3 variance (1D or band-specific 2D) while keeping >= 15% for core
                sub_b = D_subtract[b, ok] if D_subtract.ndim == 2 else D_subtract[ok]
                sub_ok = np.minimum(sub_b, 0.85 * y_ok)
                y_net = np.maximum(y_ok - sub_ok, 0.0)
            else:
                y_net = y_ok
            floor_var = (so_band_fixed[b] ** 2) if spec_b_pinned else 0.0
            y_adj = np.maximum(y_net - 2.0 * floor_var, 0.0)
            A = np.column_stack([2.0 * np.ones(ok.sum()), col_xi[ok], col_L[ok]]) * w_h[ok, None]
            coef, res = nnls(A, y_adj * w_h[ok])
            so2_b[b] = floor_var + coef[0]
            vxi_b[b] = coef[1]
            sL2_b[b] = coef[2]
            denom = max(float(np.sum((y_ok * w_h[ok]) ** 2)), 1e-12)
            if b < nb - 1:
                sse += (res ** 2) / denom
        if best is None or sse < best[0]:
            best = (sse, float(phi), so2_b, vxi_b, sL2_b)
    return best


# --------------------------------------------------------------------------- #
# Estimation (strictly from declared moments; zero tuning to scores)
# --------------------------------------------------------------------------- #
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
) -> CommonParams:
    """Estimate the 11-scalar Common Rank-Diffusion model (or opt-in Stage 3 with asymmetry=True).

    Parameters
    ----------
    X : (T, N) log1p activity matrix (NaN when absent)
    R : (T, N) weekly rank matrix (NaN when absent)
    K : int, scored top-K universe size
    floors : (T,) absence rank floor (N_t + 1); computed if None
    spec_b : optional (z_sb, sigma_obs_sb) daily replication floor pinning sigma_obs(z)
    refine_drift : whether to run smooth all-rank fixed-point refinement of g(r)
    innov_mode : "empirical" (bounded empirical standardized residuals) or "gaussian"
    asymmetry : opt-in Stage 3 (fading positive surges + persistent entity drift, variance-conserving)
    """
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

    # 2. Profile NNLS over pooled phi:
    #    D_b(h) = 2*so_b^2 + 2*V_xi_b*(1 - phi^h) + sL2_b * h [+ Stage 3 D_s(h) + D_d(h)]
    #    When Spec-B is provided, enforce it as its exact mathematical lower bound:
    #    so_b^2 = so_floor_b^2 + so_excess_b^2 (so_excess_b^2 >= 0 captures 1-week bursts
    #    without forcing phi to collapse!). Uniform lag weights w_h = 1 fit short, medium & long cleanly.
    sse_best, phi_hat, so2_b, vxi_b, sL2_b = _fit_structure_profiles(
        D, hs, nb, spec_b_pinned, so_band_fixed, D_subtract=None
    )

    in_k = np.arange(nb) < nb - 1
    dz_b = band_z - z_K
    w_oct = np.minimum(band_n, 100.0)

    # Optional Stage 3 (opt-in): fading positive surges + persistent entity drift,
    # strictly moment-identified and 100% variance-conserving via D_subtract!
    surge_lam = 0.0
    surge_mu = 0.0
    surge_rho = 0.0
    decline_var = 0.0
    decline_rho = 0.0
    stage3_diag = None
    if asymmetry:
        with np.errstate(all="ignore"):
            D_med = np.nanmedian(D[in_k], axis=0)
        surge_lam, surge_mu, surge_rho, s3_surge = _estimate_stage3_surge(
            Xc, pr_ord, pres, K, D_med
        )
        Vs = _surge_variance(surge_lam, surge_mu, surge_rho)

        # Two-pass self-consistent estimation of (decline_var, phi, V_xi, sigma_L)
        b_xi_ref = 0.0
        for _pass in range(2):
            ok_xi0 = in_k & np.isfinite(vxi_b) & (vxi_b > 1e-7)
            if ok_xi0.sum() >= 2:
                a_xi0, b_xi0 = _wls_line(dz_b[ok_xi0], 0.5 * np.log(vxi_b[ok_xi0]), w_oct[ok_xi0])
                b_xi_ref = b_xi0
                sel_k = (pr_ord <= K) & (pres >= 0.5)
                z_sel = np.log(np.clip((pr_ord[sel_k] - 0.5) / N, Z_CLIP, 1.0))
                Vxi_mean = float(np.mean(np.exp(2.0 * np.clip(a_xi0 + b_xi0 * (z_sel - z_K), -6.0, 3.0))))
            else:
                Vxi_mean = 0.0

            decline_var, decline_rho, s3_drift = _estimate_stage3_drift(
                Xc, pr_ord, pres, K, phi_hat, Vxi_mean, Vs, surge_rho
            )
            D_sub = np.zeros((nb, hs.size), dtype=float)
            if Vs > 0.0 and surge_rho > 0.0:
                s_sc_b = _surge_scale_of(b_xi_ref, band_z, z_K)
                D_sub = D_sub + (s_sc_b[:, None] ** 2) * (2.0 * Vs * (1.0 - surge_rho ** hs))[None, :]
            if decline_var > 0.0 and decline_rho > 0.0:
                D_sub = D_sub + (decline_var * _f_vel(hs, decline_rho))[None, :]

            if np.any(D_sub > 0.0):
                sse_best, phi_hat, so2_b, vxi_b, sL2_b = _fit_structure_profiles(
                    D, hs, nb, spec_b_pinned, so_band_fixed, D_subtract=D_sub
                )
            else:
                break
        stage3_diag = {"surge": s3_surge, "drift": s3_drift}

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

    # 4. Split-half entity volatility spread s_vol (bias-free at s = 0!)
    #    Default gamma_L = 0.0 (passing/burst volatility heterogeneity only; Stage 2 ablation
    #    proved gamma_L = 0 beats gamma_L > 0 across all horizons while saving 1 free scalar!).
    s_short = _split_half_vol_spread(Xc, pr_ord, K, N, step=1)
    s_vol = float(np.clip(s_short, 0.0, 1.5))
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
    dX1 = np.diff(Xc, axis=0)
    dX1 = dX1 - np.nanmean(dX1, axis=1, keepdims=True)
    in_k_ent = (pr_ord <= K) & (pres >= 0.5)
    dx_in = dX1[:, in_k_ent]
    sd_i = np.nanstd(dx_in, axis=0)
    ok_u = np.isfinite(sd_i) & (sd_i > 1e-4)
    u = dx_in[:, ok_u] / sd_i[ok_u][None, :]
    uf = u[np.isfinite(u)]
    if uf.size >= 500:
        # Deterministic quantile grid -> compact & exact!
        qs = np.linspace(0.0005, 0.9995, 4096)
        pool = np.quantile(uf, qs)
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

    # 7. Closed-form Atlas drift + optional smooth all-rank refinement
    dz, dg = _closed_form_atlas_drift(sigma_L0, beta, L_K, u_lo, u_hi, ladder_obs, N)

    diag = dict(
        D=D.tolist(), H=H, sse_phi=float(sse_best),
        band_z=band_z.tolist(), band_level=band_level.tolist(),
        sL2_band=sL2_b.tolist(), vxi_band=vxi_b.tolist(), so2_band=so2_b.tolist(),
        abs_rates=abs_rates.tolist(), s_short=s_short, s_long=s_long,
    )
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
        surge_lam=surge_lam, surge_mu=surge_mu, surge_rho=surge_rho,
        decline_var=decline_var, decline_rho=decline_rho,
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
                self.drift_i = self._draw_entity_drift(self.N)
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

    def _draw_entity_drift(self, n: int) -> np.ndarray:
        sd = math.sqrt(self.p.decline_var)
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

        if self.drift_i is not None:
            self.L = self.L + self.drift_i
            if p.decline_rho < 1.0:
                innov_sd = math.sqrt(p.decline_var * max(1.0 - p.decline_rho ** 2, 0.0))
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
                self.drift_i[ex] = self._draw_entity_drift(ne)
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
            s_sc = _surge_scale_of(p.b_xi, zp, p.z_K)
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
# Calendar-aligned 2-state Kalman Filter + Empirical-Bayes v_i (Goal 2!)
# --------------------------------------------------------------------------- #
def filter_endpoints(
    X_tr: np.ndarray,
    R_tr: np.ndarray,
    p: CommonParams,
    floors_tr: np.ndarray | None = None,
) -> dict:
    """Vectorized 2-state Kalman filter for [L_it, xi_it] with Empirical-Bayes entity volatility v_i.

    Operates strictly on the training matrix X_tr (T0 x N) with ZERO lookahead.
    Returns posterior means (L_hat, xi_hat), posterior covariances (P11, P12, P22),
    shrunk entity amplitude v_hat and posterior log-v SD (post_s_v).
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

    # 1. Empirical-Bayes entity amplitude v_hat (shrunk toward band prior)
    dX = np.diff(Xc, axis=0)
    dX = dX - np.nanmean(dX, axis=1, keepdims=True)
    n_d = np.sum(np.isfinite(dX), axis=0).astype(float)
    with np.errstate(all="ignore"):
        var_i = np.nanmean(dX * dX, axis=0)

    # Expected baseline 1-week variance at z_i from the model
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
    base_var1 = 2.0 * (so_i ** 2) + 2.0 * vxi_i * (1.0 - p.phi) + (sL_i ** 2)
    if has_stage3:
        s_sc_eb = _surge_scale_of(p.b_xi, z_i, p.z_K) if has_surge else np.ones(N)
        base_var1 = base_var1 + (s_sc_eb ** 2) * (2.0 * Vs * (1.0 - p.surge_rho)) + (p.decline_var if has_drift else 0.0)

    if p.s_vol > 0.02:
        s2 = p.s_vol ** 2
        # Sampling variance of log(hat_var_i) ~ 2 / max(n_d, 1) (plus heavy-tail factor 1.5)
        tau2 = 3.0 / np.maximum(n_d, 1.0)
        w_eb = np.where(np.isfinite(var_i) & (var_i > 1e-8) & (n_d >= 4), s2 / (s2 + tau2), 0.0)
        log_ratio = np.where(w_eb > 0, np.log(np.maximum(var_i, 1e-8) / np.maximum(base_var1, 1e-8)), 0.0)
        # Subtract mean bias on top-K so prior is centered, then apply EXACT E[v_hat] = 1 normalization!
        inK = pr_ord <= p.K
        if np.any(inK & (w_eb > 0)):
            log_ratio = log_ratio - float(np.mean(log_ratio[inK & (w_eb > 0)]))
        log_v_hat = np.clip(w_eb * log_ratio, -2.2 * p.s_vol, 2.2 * p.s_vol)
        raw_v = np.exp(log_v_hat)
        # Normalize so mean(v_hat) == 1.0 on top-K (exact E[v] = 1)
        norm_scale = float(np.mean(raw_v[inK])) if np.any(inK) else float(np.mean(raw_v))
        v_hat = raw_v / max(norm_scale, 1e-6)
        post_s_v = np.sqrt(np.maximum(s2 * (1.0 - w_eb), 0.0))
    else:
        v_hat = np.ones(N)
        post_s_v = np.zeros(N)

    # 2. Vectorized Kalman filter: exact 2-state [L, xi] for Stage 2 (bit-identical!),
    #    exact 4-state [L, xi, d, s_tilde] when Stage 3 asymmetry is active.
    qL = (sL_i ** 2) * np.power(v_hat, p.gamma_L)
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
    q_d = float(p.decline_var * max(1.0 - rho_d * rho_d, 0.0)) if has_drift else 0.0
    s_sc_i = _surge_scale_of(p.b_xi, z_i, p.z_K) if has_surge else np.ones(N)
    q_s_i = (s_sc_i ** 2) * _surge_innov_var(p.surge_lam, p.surge_mu) if has_surge else np.zeros(N)
    v_d0 = float(p.decline_var) if has_drift else 0.0
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
    P[:, 2, 2] = v_d0
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
                P_sub[:, 2, 2] += q_d
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
            P[first, 2, 2] = v_d0
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
        L_hat=L, xi_hat=x_st[:, 1], d_hat=x_st[:, 2], s_hat=x_st[:, 3], s_scale=s_sc_i,
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
        sd_d = math.sqrt(p.decline_var) if has_drift else 0.0
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
