"""Stage A: executable checks of the identities and inequalities Proof 7 relies on.

Each check can fail. The proof's closed forms are typed here from PROOF_7_RESULT.md and compared
with p7core.py, which is derived separately from the covariance function. Deterministic; no data.
Run:  python stageA_checks.py  ->  stageA_checks.json
"""
import json
import math

import numpy as np
from numpy.polynomial.hermite_e import hermeval
from numpy.polynomial.laguerre import laggauss
from scipy import integrate
from scipy.special import ndtr
from scipy.stats import norm

from p7core import (Par, T_phase, amoment, amoment_numeric_phase, dmoment_dd, gl,
                    jacobian_full, phase_nodes)

OUT = {}
TP = {
    "unit": dict(a=1.0, k=1.0, v=1.0, lam=1.0, mu0=0.0, v0=1.0),
    "generic": dict(a=0.3, k=0.7, v=0.5, lam=0.2, mu0=-1.0, v0=0.6),
}


def rel(x, y):
    return abs(x - y) / max(abs(x), abs(y), 1e-300)


# ---- A. Hermite bound used for the kernel constants (3.1) -------------------------------------
herm = {}
for n in range(1, 7):
    c = [0] * n + [1]
    val = 2 * integrate.quad(lambda x: abs(hermeval(x, c)) * norm.pdf(x), 0, 40, limit=400)[0]
    herm[n] = dict(L1=val, bound=math.sqrt(math.factorial(n)), ok=bool(val <= math.sqrt(math.factorial(n)) + 1e-12))
OUT["A_hermite_L1_le_sqrt_factorial"] = herm

# ---- B. time exponents of (3.1) and the resolvent kernel ---------------------------------------
expo = {}
for l in (0, 1):
    for i in range(3):
        for j in range(3 - i):
            n = 2 * i + j + l
            e = i + j - n / 2
            expo[f"i{i}j{j}l{l}"] = dict(t_exponent=e, claimed=(j - l) / 2, integrable=bool(e > -1), ok=bool(e == (j - l) / 2))
OUT["B_time_exponents"] = dict(all_ok=all(v["ok"] and v["integrable"] for v in expo.values()), table=expo)

kern = {}
for name, p in TP.items():
    a, lam = p["a"], p["lam"]
    c = -a / 2
    rho = math.sqrt(c * c + 2 * a * lam)
    rker = lambda y: math.exp((c * y - abs(y) * rho) / a) / rho
    # numerical Laplace integral of the translated Gaussian at a few points
    worst = 0.0
    for y in (-1.3, -0.2, 0.4, 2.0):
        f = lambda t: math.exp(-lam * t) * norm.pdf(y - c * t, scale=math.sqrt(a * t))
        num = integrate.quad(f, 0, np.inf, limit=400)[0]
        worst = max(worst, rel(num, rker(y)))
    mass = lam * (integrate.quad(rker, -np.inf, 0)[0] + integrate.quad(rker, 0, np.inf)[0])
    dnorm_exact = 2 / rho
    K0 = math.sqrt(math.pi) / math.sqrt((a / 2) * lam)
    kern[name] = dict(closed_form_vs_laplace_relerr=worst, mass_lambda_R=mass,
                      L1_norm_of_dR_exact=dnorm_exact, K0_at_a_minus=K0, ok=bool(worst < 1e-7 and abs(mass - 1) < 1e-9 and dnorm_exact <= K0))
OUT["B_resolvent_kernel"] = kern

# ---- C. Lemma 1: retention and phase -------------------------------------------------------------
ret = {}
for lam in (0.01, 0.2, 1.0):
    for m in (3, 14):
        num = integrate.quad(lambda u: math.exp(-lam * (m + u)), 0, 1)[0]
        formula = math.exp(-lam * m) * (1 - math.exp(-lam)) / lam
        dens = integrate.quad(lambda u: lam * math.exp(-lam * u) / (1 - math.exp(-lam)), 0, 1)[0]
        Tn = integrate.quad(lambda u: math.exp(0.37 * u) * lam * math.exp(-lam * u) / (1 - math.exp(-lam)), 0, 1)[0]
        ret[f"lam{lam}_m{m}"] = dict(rho_numeric=num, rho_formula=formula, density_mass=dens,
                                    T_closed_vs_numeric=rel(Tn, float(np.real(T_phase(0.37, lam)))),
                                    ok=bool(rel(num, formula) < 1e-12 and abs(dens - 1) < 1e-12))
    ret[f"lam{lam}_ratio_rho3_over_rho14"] = dict(value=math.exp(11 * lam))
OUT["C_retention_phase"] = ret


# ---- D/E. Section 4 closed forms, and mutation checks -------------------------------------------
def closed_mu(j, g, p):
    K = math.exp(p["mu0"] + (p["v0"] + p["v"]) / 2)
    h1 = 1.0 if g == 0 else (math.exp(g) - 1) / g
    return K * float(np.real(T_phase(g, p["lam"]))) * h1 * math.exp(g * (j - 1))


def closed_Q(j, g, p, nz=40):
    a, v, k = p["a"], p["v"], p["k"]
    C0 = math.exp(2 * p["mu0"] + 2 * p["v0"] + v)
    z, w = gl(nz)
    Z, Y = np.meshgrid(z, z, indexing="ij")
    integ = np.sum(np.outer(w, w) * np.exp(g * (1 + Z + Y) + a * Z + v * np.exp(-k * (1 + Y - Z))))
    return C0 * float(np.real(T_phase(2 * g + a, p["lam"]))) * math.exp((2 * g + a) * (j - 1)) * integ


sec4 = {}
for name, p in TP.items():
    for g in (0.0, 0.15, -0.4):
        P = Par(g=g, v=p["v"], a=p["a"], k=p["k"], lam=p["lam"], mu0=p["mu0"], v0=p["v0"])
        worst = 0.0
        for j in (1, 2, 3):
            m_core = float(np.real(amoment((j,), P, 24)))
            m_num = amoment_numeric_phase((j,), P, 24, 32)
            worst = max(worst, rel(m_core, closed_mu(j, g, p)), rel(m_num, closed_mu(j, g, p)))
        for j in (1, 2):
            q_core = float(np.real(amoment((j, j + 1), P, 24)))
            q_num = amoment_numeric_phase((j, j + 1), P, 24, 32)
            worst = max(worst, rel(q_core, closed_Q(j, g, p)), rel(q_num, closed_Q(j, g, p)))
        r1 = float(np.real(amoment((2,), P, 24) / amoment((1,), P, 24)))
        r2 = float(np.real(amoment((2, 3), P, 24) / amoment((1, 2), P, 24)))
        # mutations: both must be visibly wrong
        sep = float(np.real(amoment((1, 2), P, 24, separate_phase=True)))
        m1 = float(np.real(amoment((1,), P, 24, mean_one_expZ=True)))
        sec4[f"{name}_g{g}"] = dict(
            worst_relerr_closed_forms=worst,
            ratio_mu=r1, exp_g=math.exp(g), ratio_Q=r2, exp_2g_plus_a=math.exp(2 * g + p["a"]),
            mutation_separate_phase_relerr=rel(sep, closed_Q(1, g, p)),
            mutation_mean_one_expZ_relerr=rel(m1, closed_mu(1, g, p)),
            ok=bool(worst < 1e-9 and rel(r1, math.exp(g)) < 1e-10 and rel(r2, math.exp(2 * g + p["a"])) < 1e-10),
            mutations_detected=bool(rel(sep, closed_Q(1, g, p)) > 1e-4 and rel(m1, closed_mu(1, g, p)) > 1e-2))
OUT["D_section4_closed_forms"] = sec4


# ---- F/G. Lemma 5: tilt identity, h', chi ---------------------------------------------------------
def h_proof(s, p, nrho=96):
    x, w = laggauss(nrho)
    r = x / p["lam"]
    return float(np.sum(w * ndtr(-np.sqrt(2 * p["v0"] + p["a"] * (s + r)) / 2)))


def minus_hprime_proof(s, p, nrho=96):
    x, w = laggauss(nrho)
    r = x / p["lam"]
    q = np.sqrt(2 * p["v0"] + p["a"] * (s + r))
    return float(np.sum(w * p["a"] / (4 * q) * norm.pdf(q / 2)))


def u0(x, p, nrho=96):
    """Rank fraction above x under the d=0, c=-a/2 renewal density, by direct quadrature."""
    xx, w = laggauss(nrho)
    r = xx / p["lam"]
    c = -p["a"] / 2
    return np.sum(w * (1 - ndtr((np.asarray(x)[..., None] - p["mu0"] - c * r) / np.sqrt(p["v0"] + p["a"] * r))), axis=-1)


lem5 = {}
for name, p in TP.items():
    a, v0, mu0, lam = p["a"], p["v0"], p["mu0"], p["lam"]
    c = -a / 2
    worst_tilt = 0.0
    for s, t in ((0.3, 0.3), (0.5, 1.7), (2.2, 3.9), (1.0, 1.0)):
        # E[e^{X_t} u0(X_s)] without any tilt: integrate over X_s, use E[e^{X_t}|X_s] = e^{X_s} (g=0)
        m, sd = mu0 + c * s, math.sqrt(v0 + a * s)
        f = lambda x: norm.pdf(x, m, sd) * math.exp(x) * float(u0(np.array([x]), p)[0])
        lhs = integrate.quad(f, m - 14 * sd, m + 14 * sd + 14 * sd * sd, limit=400, epsabs=1e-13, epsrel=1e-12)[0]
        rhs = math.exp(mu0 + v0 / 2) * h_proof(s, p)
        worst_tilt = max(worst_tilt, rel(lhs, rhs))
    # h' by central difference against (5.3)
    worst_hp = max(rel(-(h_proof(s + 1e-4, p) - h_proof(s - 1e-4, p)) / 2e-4, minus_hprime_proof(s, p)) for s in (0.2, 1.0, 2.5, 3.9))
    chi_star = (1 - math.exp(-lam)) * a / (4 * math.sqrt(2 * math.pi) * math.sqrt(2 * v0 + 5 * a)) * math.exp(-(2 * v0 + 5 * a) / 8)
    min_mhp = min(minus_hprime_proof(s, p) for s in np.linspace(0, 4, 41))
    # D_j from the proof's definition, and the second difference
    u, wu = phase_nodes(lam, 24)
    z, wz = gl(24)
    def H0(t):
        s, ws = gl(48, 0.0, t)
        return sum(wi * h_proof(si, p) for si, wi in zip(s, ws))
    D = [sum(wu[i] * wz[j] * H0(u[i] + jj - 1 + z[j]) for i in range(len(u)) for j in range(len(z))) for jj in (1, 2, 3)]
    chi = -(D[2] - 2 * D[1] + D[0])
    P = Par(g=0.0, v=p["v"], a=a, k=p["k"], lam=lam, mu0=mu0, v0=v0)
    K = math.exp(mu0 + (v0 + p["v"]) / 2)
    dmu = [dmoment_dd((jj,), P, 16, 16, 16, 96) for jj in (1, 2, 3)]
    worst_D = max(rel(dmu[i], K * D[i]) for i in range(3))
    lem5[name] = dict(tilt_identity_relerr=worst_tilt, hprime_formula_relerr=worst_hp,
                      chi=chi, chi_star=chi_star, min_minus_hprime_on_0_4=min_mhp,
                      D=D, dmu_over_K=[x / K for x in dmu], dmu_vs_KD_relerr=worst_D,
                      ok=bool(worst_tilt < 1e-8 and worst_hp < 1e-6 and chi >= chi_star > 0 and min_mhp >= chi_star and worst_D < 1e-7))
OUT["F_lemma5_tilt_and_curvature"] = lem5

# ---- H. Lemma 6: determinant identity and bound --------------------------------------------------
lem6 = {}
for name, p in TP.items():
    P = Par(g=0.0, v=p["v"], a=p["a"], k=p["k"], lam=p["lam"], mu0=p["mu0"], v0=p["v0"])
    A = jacobian_full(P, nz=24, nU=16, nr=16, nrho=96)
    A_pf = A[:, [0, 1, 2, 3, 4]]                       # already (g, v, a, k, d)
    det = float(np.linalg.det(A_pf))
    K = math.exp(p["mu0"] + (p["v0"] + p["v"]) / 2)
    C = math.exp(2 * p["mu0"] + 2 * p["v0"] + p["v"])
    Q = float(np.real(amoment((1, 2), P, 24)))
    hh = 1e-30
    Qk = float(np.imag(amoment((1, 2), Par(g=0.0, v=p["v"], a=p["a"], k=p["k"] + 1j * hh, lam=p["lam"], mu0=p["mu0"], v0=p["v0"]), 24)) / hh)
    chi = lem5[name]["chi"]
    det_formula = K ** 3 * chi * math.exp(p["a"]) * Q * (-Qk) / 2
    dfrak = K ** 3 * C ** 2 * math.exp(p["a"]) * p["v"] * math.exp(-2 * p["k"]) * lem5[name]["chi_star"] / 8
    tau = float(np.real(T_phase(1e-30j, p["lam"]).imag / 1e-30)) + 0.5
    rows_expected = np.array([[K * (tau + j), K / 2, 0.0, 0.0] for j in range(3)])
    rows_err = float(np.max(np.abs(A[:3, :4] - rows_expected)) / K)
    sv = np.linalg.svd(A, compute_uv=False)
    lem6[name] = dict(det_numeric=det, det_formula_abs=det_formula, relerr=rel(abs(det), det_formula),
                      dfrak_lower_bound=dfrak, Q=Q, C_star=C, minus_Qk=-Qk, minus_Qk_lower=C * p["v"] * math.exp(-2 * p["k"]) / 4,
                      first_three_rows_relerr=rows_err, singular_values=sv.tolist(), frobenius=float(np.linalg.norm(A)),
                      det_over_frob4=abs(det) / float(np.linalg.norm(A)) ** 4, matrix=A.tolist(),
                      ok=bool(rel(abs(det), det_formula) < 1e-6 and abs(det) >= dfrak and Q >= C and -Qk >= C * p["v"] * math.exp(-2 * p["k"]) / 4 and rows_err < 1e-9))
OUT["H_lemma6_determinant"] = lem6

# ---- summary ---------------------------------------------------------------------------------------
def collect(o):
    if isinstance(o, dict):
        mine = [o["ok"]] if "ok" in o else []
        if "all_ok" in o:
            mine.append(o["all_ok"])
        if "mutations_detected" in o:
            mine.append(o["mutations_detected"])
        for vv in o.values():
            mine += collect(vv)
        return mine
    return []

flags = collect(OUT)
OUT["summary"] = dict(n_checks=len(flags), n_pass=int(sum(flags)), all_pass=bool(all(flags)))
with open("stageA_checks.json", "w") as f:
    json.dump(OUT, f, indent=1, default=float)
print(json.dumps(OUT["summary"]))
for kname, vv in OUT.items():
    if kname in ("summary",):
        continue
    print(kname, json.dumps({kk: (vvv if not isinstance(vvv, dict) else {q: w for q, w in vvv.items() if q not in ("matrix", "table")}) for kk, vvv in vv.items()}, default=float)[:1500])
