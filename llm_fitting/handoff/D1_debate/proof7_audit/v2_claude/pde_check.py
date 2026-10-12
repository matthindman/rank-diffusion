"""Independent finite-difference check of the d derivative of the day means (Lemma 5), with the
self-consistent density recomputed at every d, and a measurement of the self-consistent term
away from d = 0.

No tilt, no pathwise derivative and no closed form from the proof is used on the PDE side:

  stationary density:  0 = (a/2) p'' - ((c + d u) p)' - lam p + lam nu,   u(x) = int_x^inf p
  backward equation:   v(t, x) = E_x exp(X_t),  v = e^x w,
                       w_t = (a/2) w_xx + (b + a) w_x + (b + a/2) w,  b = c + d u,  w(0) = 1
  m(t) = E exp(X_t + Z_t) = e^{v_Z/2} int nu(x) e^x w(t, x) dx.

Deterministic. Agreement across grids is a diagnostic, not an error certificate.
Run:  python pde_check.py  ->  pde_check.json
"""
import json
import math

import numpy as np
from scipy.linalg import solve_banded
from scipy.special import ndtr

from p7core import gl, lag, phase_nodes

BENCH = {
    "B1": dict(a=1.0, k=1.0, v=1.0, lam=1.0, mu0=0.0, v0=1.0),
    "B3": dict(a=0.01, k=1.0, v=0.25, lam=0.01, mu0=0.0, v0=1.0),
}
TMAX = 4.0


def grid(p, dx):
    return np.arange(p["mu0"] - 40.0, p["mu0"] + 30.0 + dx / 2, dx)


def nu(x, p):
    return np.exp(-(x - p["mu0"]) ** 2 / (2 * p["v0"])) / math.sqrt(2 * math.pi * p["v0"])


def tail(pd, dx):
    """u(x_i) = int_{x_i}^inf p, trapezoid from the right."""
    seg = 0.5 * (pd[1:] + pd[:-1]) * dx
    u = np.zeros_like(pd)
    u[:-1] = np.cumsum(seg[::-1])[::-1]
    return u


def stationary(p, d, x, dx, u_frozen=None, tol=1e-14, itmax=500):
    a, lam = p["a"], p["lam"]
    c = -a / 2
    rhs = -lam * nu(x, p)
    u = np.zeros_like(x) if u_frozen is None else u_frozen
    pd = None
    for it in range(itmax):
        b = c + d * u
        ab = np.zeros((3, x.size))
        ab[0, 1:] = a / (2 * dx * dx) - b[1:] / (2 * dx)        # coefficient of p_{i+1} in row i
        ab[1, :] = -a / (dx * dx) - lam
        ab[2, :-1] = a / (2 * dx * dx) + b[:-1] / (2 * dx)      # coefficient of p_{i-1} in row i
        new = solve_banded((1, 1), ab, rhs)
        if u_frozen is not None:
            return new, u, 1
        unew = tail(new, dx)
        err = np.max(np.abs(unew - u))
        u, pd = unew, new
        if err < tol:
            return pd, u, it + 1
    raise RuntimeError("stationary iteration did not converge")


def mean_path(p, d, u, x, dx, dt):
    """m(t)/exp(v/2) on the time grid, Crank-Nicolson."""
    a = p["a"]
    b = -a / 2 + d * u
    lo = a / (2 * dx * dx) - (b + a) / (2 * dx)
    di = -a / (dx * dx) + (b + a / 2)
    up = a / (2 * dx * dx) + (b + a) / (2 * dx)
    n = x.size
    L_lo, L_di, L_up = lo.copy(), di.copy(), up.copy()
    L_up[0] = lo[0] + up[0]                                     # Neumann: ghost value mirrors
    L_lo[-1] = lo[-1] + up[-1]
    A = np.zeros((3, n))
    A[0, 1:] = -0.5 * dt * L_up[:-1]
    A[1, :] = 1 - 0.5 * dt * L_di
    A[2, :-1] = -0.5 * dt * L_lo[1:]
    w = np.ones(n)
    weight = nu(x, p) * np.exp(x)
    nt = int(round(TMAX / dt))
    m = np.empty(nt + 1)
    m[0] = np.trapezoid(weight * w, dx=dx)
    for it in range(nt):
        r = w + 0.5 * dt * (L_di * w + np.r_[L_up[:-1] * w[1:], 0.0] + np.r_[0.0, L_lo[1:] * w[:-1]])
        w = solve_banded((1, 1), A, r)
        m[it + 1] = np.trapezoid(weight * w, dx=dx)
    return m


def h_proof(s, p, nrho=96):
    """h(s) of (5.1) for an array of s."""
    xx, w = lag(nrho)
    r = xx / p["lam"]
    s = np.atleast_1d(np.asarray(s, dtype=float))[:, None]
    return np.sum(w * ndtr(-np.sqrt(2 * p["v0"] + p["a"] * (s + r)) / 2), axis=1)


def H0(t, p):
    s, ws = gl(64, 0.0, t)
    return float(np.sum(ws * h_proof(s, p)))


def day_means(series, dt, lam):
    """E_U int_0^1 f(U + j - 1 + z) dz for j = 1, 2, 3 from a series on the time grid."""
    tt = np.arange(series.size) * dt
    cum = np.concatenate([[0.0], np.cumsum(0.5 * (series[1:] + series[:-1]) * dt)])
    u, wu = phase_nodes(lam, 32)
    return [float(sum(w * (np.interp(uu + j, tt, cum) - np.interp(uu + j - 1, tt, cum)) for uu, w in zip(u, wu))) for j in (1, 2, 3)]


OUT = {}
for name, p in BENCH.items():
    a, lam = p["a"], p["lam"]
    scale = 2 * math.sqrt(a * lam)                     # admissible |d| < scale
    Kc = math.exp(p["mu0"] + p["v0"] / 2)              # without the exp(v/2) factor, which cancels
    res = {}
    for label, dx, dt in (("coarse", 0.02, 4e-3), ("fine", 0.01, 2e-3)):
        x = grid(p, dx)
        p0, u0, _ = stationary(p, 0.0, x, dx)
        mass = float(np.trapezoid(p0, dx=dx))
        m0 = mean_path(p, 0.0, u0, x, dx, dt)
        delta = 0.02 * scale
        out = {}
        for dlt in (delta, delta / 2):
            pp, up, itp = stationary(p, +dlt, x, dx)
            pm, um, itm = stationary(p, -dlt, x, dx)
            mp = mean_path(p, +dlt, up, x, dx, dt)
            mm = mean_path(p, -dlt, um, x, dx, dt)
            out[dlt] = dict(d1=(mp - mm) / (2 * dlt), d2=(mp - 2 * m0 + mm) / dlt ** 2,
                            dp_L1=float(np.trapezoid(np.abs(pp - pm), dx=dx) / (2 * dlt)))
        d1 = (4 * out[delta / 2]["d1"] - out[delta]["d1"]) / 3     # Richardson in delta
        tt = np.arange(m0.size) * dt
        checks = {}
        for t in (0.5, 1.0, 2.0, 3.0, 4.0):
            i = int(round(t / dt))
            ana = Kc * H0(t, p)
            checks[str(t)] = dict(pde=float(d1[i]), tilt_formula=ana, relerr=abs(d1[i] - ana) / abs(ana))
        dm = day_means(d1, dt, lam)
        u, wu = phase_nodes(lam, 32)
        z, wz = gl(24)
        Dj = [sum(wu[i] * wz[j] * H0(u[i] + jj - 1 + z[j], p) for i in range(len(u)) for j in range(len(z))) for jj in (1, 2, 3)]
        res[label] = dict(dx=dx, dt=dt, mass_of_p0=mass, m0_drift_from_constant=float(np.max(np.abs(m0 / m0[0] - 1))),
                          pointwise=checks, day_mean_derivative_over_K=[v / Kc for v in dm], D_j_from_proof=Dj,
                          day_mean_relerr=[abs(v / Kc - dj) / dj for v, dj in zip(dm, Dj)],
                          chi_pde=-(dm[2] - 2 * dm[1] + dm[0]) / Kc, chi_proof=-(Dj[2] - 2 * Dj[1] + Dj[0]),
                          second_d_derivative_of_day_means_over_K=[v / Kc for v in day_means(out[delta / 2]["d2"], dt, lam)],
                          dp_dd_L1_norm_at_0=out[delta / 2]["dp_L1"], sup_p0=float(p0.max()),
                          sup_abs_p0_prime=float(np.max(np.abs(np.gradient(p0, dx)))))
    # self-consistent versus frozen density away from d = 0 (fine grid only)
    dx, dt = 0.01, 2e-3
    x = grid(p, dx)
    D_box = min(1.0, 0.5 * math.sqrt(a / 2 * lam), math.sqrt(a / 2 * lam) / (16 * math.sqrt(math.pi)))
    away = {}
    for tag, d1v in (("d=D (edge of the proof's box)", D_box), ("d=half the admissible bound", 0.5 * scale)):
        dlt = 0.01 * scale
        pc, uc, itc = stationary(p, d1v, x, dx)
        _, up, _ = stationary(p, d1v + dlt, x, dx)
        _, um, _ = stationary(p, d1v - dlt, x, dx)
        full = (mean_path(p, d1v + dlt, up, x, dx, dt) - mean_path(p, d1v - dlt, um, x, dx, dt)) / (2 * dlt)
        froz = (mean_path(p, d1v + dlt, uc, x, dx, dt) - mean_path(p, d1v - dlt, uc, x, dx, dt)) / (2 * dlt)
        f, z_ = day_means(full, dt, lam), day_means(froz, dt, lam)
        away[tag] = dict(d=d1v, iterations=itc, day_mean_derivative_self_consistent_over_K=[v / Kc for v in f],
                         day_mean_derivative_frozen_density_over_K=[v / Kc for v in z_],
                         relative_size_of_self_consistent_term=[abs(fi - zi) / abs(fi) for fi, zi in zip(f, z_)])
    res["away_from_zero"] = away
    res["D_box"] = D_box
    res["admissible_bound"] = scale
    OUT[name] = res

with open("pde_check.json", "w") as f:
    json.dump(OUT, f, indent=1, default=float)
for name, res in OUT.items():
    print(name, "fine pointwise:", {t: f"{v['relerr']:.2e}" for t, v in res["fine"]["pointwise"].items()})
    print("  day-mean relerr coarse/fine:", [f"{v:.2e}" for v in res["coarse"]["day_mean_relerr"]], [f"{v:.2e}" for v in res["fine"]["day_mean_relerr"]])
    print("  chi pde / proof:", res["fine"]["chi_pde"], res["fine"]["chi_proof"])
    print("  d2/dd2 day means over K:", res["fine"]["second_d_derivative_of_day_means_over_K"])
    print("  ||dp/dd||_1, sup p, sup|p'|:", res["fine"]["dp_dd_L1_norm_at_0"], res["fine"]["sup_p0"], res["fine"]["sup_abs_p0_prime"])
    for tag, v in res["away_from_zero"].items():
        print("  ", tag, v["d"], "self-consistent share:", [f"{s:.3e}" for s in v["relative_size_of_self_consistent_term"]])
