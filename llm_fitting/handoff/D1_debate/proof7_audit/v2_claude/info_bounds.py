"""POST HOC (not in DESIGN.md; added after the owner asked, mid-task, to look for assumptions or
stylized facts that would tighten practical use). Two deterministic reference calculations at
the proof's base point (d = 0, g = 0). Neither is a statement about the five-feature theorem.

(1) FLOOR. Fisher information about d in the complete latent path of X over [0, T], T = U + m,
    with c unknown and a known (a continuous path reveals a). Changing d at fixed a is a drift
    change, so the score is (1/sqrt a) int u_0(X_t) dW_t and

        I_d(T) = (1/a) [ int_0^T E u_0(X_t)^2 dt - (int_0^T E u_0(X_t) dt)^2 / T ].

    The counts are a function of this path and of noise whose law does not depend on d, so no
    regular estimator from the m-day count record can have asymptotic variance below
    1 / (N I_d). The floor is generous: it ignores Poisson noise, the OU nuisance and daily binning.

(2) IDEALIZED HIGH-COUNT BENCHMARK. Log-intensity Y observed without count noise once a day
    (ages t_j = E U + j - 1/2, j = 1..m). At d = 0 the vector is Gaussian; the information in its
    mean and covariance about (c, a, k, v, d), optionally with the entry law (mu0, v0) unknown, is

        I = M' S^-1 M + (1/2) tr(S^-1 S_p S^-1 S_q),

    where d enters the mean through int_0^t E u_0(X_s) ds and the covariance through
    Cov(X_s, int_0^t u_0(X_r) dr) = - int_0^t (v0 + a min(s, r)) E p_0(X_r) dr  (Stein's lemma).
    This is NOT the count instrument and NOT a bound for it; it shows what first and second
    moments of daily log-activity could carry if counts were large.

Run:  python info_bounds.py  ->  info_bounds.json
"""
import json
import math

import numpy as np
from numpy.polynomial.hermite_e import hermegauss
from scipy.special import ndtr

from p7core import gl, lag, phase_nodes
from sensitivity import BENCH, REL, Z95

XH, WH = hermegauss(120)
WH = WH / math.sqrt(2 * math.pi)


def u0(x, p):
    xx, w = lag(96)
    r = xx / p["lam"]
    c = -p["a"] / 2
    return np.sum(w * (1 - ndtr((x[..., None] - p["mu0"] - c * r) / np.sqrt(p["v0"] + p["a"] * r))), axis=-1)


def p0(x, p):
    xx, w = lag(96)
    r = xx / p["lam"]
    c = -p["a"] / 2
    s2 = p["v0"] + p["a"] * r
    return np.sum(w * np.exp(-(x[..., None] - p["mu0"] - c * r) ** 2 / (2 * s2)) / np.sqrt(2 * math.pi * s2), axis=-1)


def E_at(t, f, p):
    """E f(X_t) for arrays of t; X_t ~ N(mu0 + c t, v0 + a t) at the base point."""
    t = np.atleast_1d(t)
    x = p["mu0"] - p["a"] / 2 * t[:, None] + np.sqrt(p["v0"] + p["a"] * t[:, None]) * XH[None, :]
    return np.sum(WH * f(x, p), axis=1)


def floor_info(p, m):
    u, wu = phase_nodes(p["lam"], 12)
    tot = 0.0
    for uu, ww in zip(u, wu):
        T = uu + m
        t, wt = gl(48, 0.0, T)
        e1 = E_at(t, u0, p)
        e2 = E_at(t, lambda x, q: u0(x, q) ** 2, p)
        tot += ww * (np.sum(wt * e2) - np.sum(wt * e1) ** 2 / T) / p["a"]
    return float(tot)


def gaussian_benchmark(p, m, entry_unknown=False, nuisance_known=False):
    a, k, v, v0, lam = p["a"], p["k"], p["v"], p["v0"], p["lam"]
    EU = 1 / lam - math.exp(-lam) / (1 - math.exp(-lam))
    t = EU + np.arange(1, m + 1) - 0.5
    dlt = np.abs(t[:, None] - t[None, :])
    mn = np.minimum(t[:, None], t[None, :])
    S = v0 + a * mn + v * np.exp(-k * dlt)
    md = np.zeros(m)
    C = np.zeros((m, m))                                # C[i, j] = Cov(X_{t_i}, int_0^{t_j} u0(X_r) dr)
    for j in range(m):
        r, wr = gl(64, 0.0, t[j])
        md[j] = np.sum(wr * E_at(r, u0, p))
        ep = E_at(r, p0, p)
        for i in range(m):
            C[i, j] = -np.sum(wr * (v0 + a * np.minimum(t[i], r)) * ep)
    dS = dict(a=mn, k=-v * dlt * np.exp(-k * dlt), v=np.exp(-k * dlt), d=C + C.T, c=np.zeros((m, m)),
              mu0=np.zeros((m, m)), v0=np.ones((m, m)))
    dM = dict(c=t, d=md, a=np.zeros(m), k=np.zeros(m), v=np.zeros(m), mu0=np.ones(m), v0=np.zeros(m))
    names = ["c", "a", "d"] if nuisance_known else ["c", "a", "k", "v", "d"]
    if entry_unknown:
        names = names + ["mu0", "v0"]
    Si = np.linalg.inv(S)
    n = len(names)
    I = np.zeros((n, n))
    for i, pi in enumerate(names):
        for j, pj in enumerate(names):
            I[i, j] = dM[pi] @ Si @ dM[pj] + 0.5 * np.trace(Si @ dS[pi] @ Si @ dS[pj])
    cov = np.linalg.inv(I)
    scale = dict(c=a, a=a, k=k, v=v, d=2 * math.sqrt(a * lam), mu0=1.0, v0=v0)
    return {nm: float((Z95 / REL) ** 2 * cov[i, i] / scale[nm] ** 2) for i, nm in enumerate(names)}


OUT = {}
for name, b in BENCH.items():
    sd = 2 * math.sqrt(b["a"] * b["lam"])
    res = {}
    for m in (3, 14):
        I = floor_info(b, m)
        res[f"floor_m{m}"] = dict(fisher_information_d_per_record=I,
                                  N_floor_for_d_10pct_of_scale=float((Z95 / REL) ** 2 / (I * sd ** 2)))
        res[f"gaussian_m{m}"] = gaussian_benchmark(b, m)
        res[f"gaussian_m{m}_entry_law_unknown"] = gaussian_benchmark(b, m, entry_unknown=True)
        res[f"gaussian_m{m}_nuisance_known"] = gaussian_benchmark(b, m, nuisance_known=True)
    OUT[name] = res

with open("info_bounds.json", "w") as f:
    json.dump(OUT, f, indent=1)
f3 = lambda x: f"{x:9.3g}"
print("N for 10% of scale.  floor(d): m=3, m=14 | Gaussian daily idealization m=14: c a k v d | same, entry law unknown: a d | m=3: a d")
for n, r in OUT.items():
    g = r["gaussian_m14"]; ge = r["gaussian_m14_entry_law_unknown"]; g3 = r["gaussian_m3"]
    print(n, f3(r["floor_m3"]["N_floor_for_d_10pct_of_scale"]), f3(r["floor_m14"]["N_floor_for_d_10pct_of_scale"]), "|",
          " ".join(f3(g[x]) for x in ("c", "a", "k", "v", "d")), "|", f3(ge["a"]), f3(ge["d"]), "|", f3(g3["a"]), f3(g3["d"]))
