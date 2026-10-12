"""Deterministic evaluation of the Proof 7 observation map at d = 0, and of its d derivative.

Independent audit code (Claude, 2026-10-12). Nothing here imports the research model, draws
random numbers, or reads data. Units: days, natural-log intensity. Coordinates as in the proof:
x = (g, v, a, k, d), g = c + a/2, intensity exp(X_t + Z_t), Z stationary OU with E Z = 0.

Everything is derived from the covariance function of Y = X + Z at d = 0,

    E Y_t = mu0 + c t,   Cov(Y_s, Y_t) = v0 + a min(s, t) + v exp(-k |t - s|),

and from the conditional phase density f_U(u) = lam exp(-lam u) / (1 - exp(-lam)) on (0, 1).
The proof's closed forms are NOT used here; stageA_checks.py compares against them.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, replace
from functools import lru_cache

import numpy as np
from numpy.polynomial.laguerre import laggauss
from numpy.polynomial.legendre import leggauss
from scipy.special import ndtr


@lru_cache(maxsize=None)
def _leg(n):
    return leggauss(n)


@lru_cache(maxsize=None)
def lag(n):
    return laggauss(n)


def gl(n, lo=0.0, hi=1.0):
    x, w = _leg(n)
    return 0.5 * (hi - lo) * x + 0.5 * (hi + lo), 0.5 * (hi - lo) * w


@dataclass(frozen=True)
class Par:
    g: complex
    v: complex
    a: complex
    k: complex
    lam: float
    mu0: float
    v0: float

    @property
    def c(self):
        return self.g - self.a / 2


def T_phase(h, lam):
    """E exp(h U) under the retained-cohort phase density. Complex-safe."""
    z = h - lam
    f = 1.0 if z == 0 else np.expm1(z) / z
    return lam * f / (1.0 - np.exp(-lam))


def phase_nodes(lam, n):
    u, w = gl(n)
    return u, w * lam * np.exp(-lam * u) / (1.0 - np.exp(-lam))


def _offset_grid(bins, nz):
    """Offsets tau_i = (bin_i - 1) + z_i and weights for E prod_i A_{bin_i}.

    Points sharing a bin are integrated over the ordered simplex (the covariance has a kink
    on the diagonal). Supports up to two points per bin.
    """
    bins = list(bins)
    counts = {b: bins.count(b) for b in set(bins)}
    if max(counts.values()) > 2:
        raise ValueError("at most two points per bin")
    z, w = gl(nz)
    axes, wts, recipe = [], [], []
    for b in sorted(counts):
        if counts[b] == 1:
            axes.append(z); wts.append(w); recipe.append((b, "single", len(axes) - 1))
        else:
            axes.append(z); wts.append(w)           # t  (the larger point)
            axes.append(z); wts.append(w)           # s = t * q
            recipe.append((b, "pair", len(axes) - 2))
    mesh = np.meshgrid(*axes, indexing="ij")
    wmesh = np.meshgrid(*wts, indexing="ij")
    W = np.ones_like(mesh[0])
    for wm in wmesh:
        W = W * wm
    taus = []
    for b, kind, i in recipe:
        if kind == "single":
            taus.append((b - 1) + mesh[i])
        else:
            t = mesh[i]; s = t * mesh[i + 1]
            taus.append((b - 1) + t); taus.append((b - 1) + s)
            W = W * 2.0 * t
    return [t.ravel() for t in taus], W.ravel()


def amoment(bins, P: Par, nz=16, separate_phase=False, mean_one_expZ=False):
    """E[prod_i A_{bin_i}] at d = 0, phase integrated in closed form.

    separate_phase / mean_one_expZ are deliberately WRONG variants used as mutation checks.
    """
    taus, W = _offset_grid(bins, nz)
    n = len(taus)
    st = sum(taus)
    smin = 0.0
    sexp = n * 1.0                                   # diagonal terms exp(0)
    for i, j in itertools.combinations(range(n), 2):
        smin = smin + 2.0 * np.minimum(taus[i], taus[j])
        sexp = sexp + 2.0 * np.exp(-P.k * np.abs(taus[i] - taus[j]))
    expo = P.g * st + 0.5 * P.a * smin + 0.5 * P.v * sexp
    if mean_one_expZ:
        expo = expo - 0.5 * P.v * n                   # exp(Z - v/2): a different normalization
    core = np.sum(W * np.exp(expo))
    pref = np.exp(n * P.mu0 + 0.5 * n * n * P.v0)
    if separate_phase:
        # WRONG: an independent phase per point -- breaks the U n(n-1) a / 2 coupling
        ph = T_phase(P.g, P.lam) ** n
    else:
        ph = T_phase(n * P.g + 0.5 * P.a * n * (n - 1), P.lam)
    return pref * ph * core


def amoment_numeric_phase(bins, P: Par, nz=16, nU=24):
    """Same quantity with the phase integrated numerically from the covariance function.

    No closed form in U is used: an independent route for checking amoment and (4.1), (4.3).
    Distinct bins only.
    """
    if len(set(bins)) != len(bins):
        raise ValueError("distinct bins only")
    taus, W = _offset_grid(bins, nz)
    n = len(taus)
    u, wu = phase_nodes(P.lam, nU)
    tot = 0.0
    for uu, ww in zip(u, wu):
        t = [uu + tau for tau in taus]
        mean = n * P.mu0 + P.c * sum(t)
        var = 0.0
        for i in range(n):
            for j in range(n):
                var = var + P.v0 + P.a * np.minimum(t[i], t[j]) + P.v * np.exp(-P.k * np.abs(t[i] - t[j]))
        tot = tot + ww * np.sum(W * np.exp(mean + 0.5 * var))
    return tot


def u0_tilted(r, shift, P: Par, nrho=64):
    """P(xi > X_r) for xi ~ d=0 renewal density and X_r ~ N(mu0 + c r + shift, v0 + a r).

    xi | age rho ~ N(mu0 + c rho, v0 + a rho), rho ~ Exp(lam). Gauss-Laguerre in lam*rho.
    r, shift: arrays of equal shape. Real parameters only.
    """
    x, w = lag(nrho)
    rho = x / P.lam
    r = np.asarray(r)[..., None]; shift = np.asarray(shift)[..., None]
    c = float(np.real(P.c)); a = float(np.real(P.a))
    num = c * (rho - r) - shift
    den = np.sqrt(2.0 * P.v0 + a * (rho + r))
    return np.sum(w * ndtr(num / den), axis=-1)


def dmoment_dd(bins, P: Par, nz=12, nU=12, nr=12, nrho=64):
    """d/dd of E[prod_i A_{bin_i}] at d = 0 (distinct bins), by the pathwise derivative

        d/dd X_t = int_0^t u_0(X_r) dr,

    with the Gaussian tilt by exp(sum_i X_{t_i}): X_r gains mean n v0 + a sum_j min(r, t_j).
    """
    if len(set(bins)) != len(bins):
        raise ValueError("distinct bins only")
    taus, W = _offset_grid(sorted(bins), nz)
    n = len(taus)
    c = float(np.real(P.c)); a = float(np.real(P.a)); v = float(np.real(P.v)); k = float(np.real(P.k))
    u, wu = phase_nodes(P.lam, nU)
    xr, wr = gl(nr)
    tot = 0.0
    for uu, ww in zip(u, wu):
        t = [uu + tau for tau in taus]                 # increasing in i because bins are sorted
        mean = n * P.mu0 + c * sum(t)
        var = 0.0
        for i in range(n):
            for j in range(n):
                var = var + P.v0 + a * np.minimum(t[i], t[j]) + v * np.exp(-k * np.abs(t[i] - t[j]))
        e0 = np.exp(mean + 0.5 * var)
        G = np.zeros_like(e0)
        lo = np.zeros_like(e0)
        for seg in range(n):                            # r in (t_{seg-1}, t_seg): n - seg paths still running
            hi = t[seg]
            rr = lo[:, None] + (hi - lo)[:, None] * xr[None, :]
            shift = n * P.v0 + a * sum(np.minimum(rr, tj[:, None]) for tj in t)
            pr = u0_tilted(rr, shift, P, nrho)
            G = G + (n - seg) * (hi - lo) * np.sum(wr[None, :] * pr, axis=1)
            lo = hi
        tot = tot + ww * np.sum(W * e0 * G)
    return tot


FEATURE_BINS = [(1,), (2,), (3,), (1, 2), (2, 3)]
COORDS = ("g", "v", "a", "k")


def raw_F(P: Par, nz=16, bins_list=FEATURE_BINS):
    return np.array([amoment(b, P, nz) for b in bins_list])


def jacobian_gvak(P: Par, nz=16, bins_list=FEATURE_BINS, h=1e-30):
    """Columns (g, v, a, k) of D F by complex-step differentiation (no subtraction error)."""
    J = np.zeros((len(bins_list), 4))
    for jcol, name in enumerate(COORDS):
        Pp = replace(P, **{name: getattr(P, name) + 1j * h})
        J[:, jcol] = np.imag(raw_F(Pp, nz, bins_list)) / h
    return J


def jacobian_full(P: Par, nz=16, bins_list=FEATURE_BINS, nU=12, nr=12, nrho=64):
    """D F at d = 0 with columns ordered (g, v, a, k, d), as in the proof."""
    J4 = jacobian_gvak(P, nz, bins_list)
    dcol = np.array([dmoment_dd(b, P, min(nz, 12), nU, nr, nrho) for b in bins_list])
    return np.column_stack([J4, dcol])


def count_moment(mult, P: Par, nz=12):
    """E[prod_b C_b^{m_b}] for multiplicities m_b in {1, 2}: E C = A, E C^2 = A + A^2 given A."""
    bins = sorted(mult)
    tot = 0.0
    for choice in itertools.product(*[(1, 2) if mult[b] == 2 else (1,) for b in bins]):
        pts = []
        for b, p in zip(bins, choice):
            pts += [b] * p
        tot = tot + amoment(pts, P, nz)
    return tot


def stat_cov(P: Par, stats=FEATURE_BINS, nz=12):
    """Exact covariance of the raw statistics prod_{b in s} C_b at d = 0."""
    m = np.array([np.real(amoment(s, P, nz)) for s in stats])
    S = np.zeros((len(stats), len(stats)))
    for i, si in enumerate(stats):
        for j, sj in enumerate(stats):
            if j < i:
                S[i, j] = S[j, i]; continue
            mult = {}
            for b in si + sj:
                mult[b] = mult.get(b, 0) + 1
            S[i, j] = np.real(count_moment(mult, P, nz)) - m[i] * m[j]
    return S, m
