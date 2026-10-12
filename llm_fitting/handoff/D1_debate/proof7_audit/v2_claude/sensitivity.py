"""Stage C3-C5: the exact low-order Jacobian at the base point, its conditioning, the sampling
covariance of the raw statistics, and the declared variants W1-W4 (DESIGN.md).

Everything is evaluated at the proof's base point x* = (g, v, a, k, d) = (0, v*, a*, k*, 0), where
the auxiliary moments are Gaussian integrals. All numbers are NUMERICAL ESTIMATES by Gauss
quadrature. Convergence across quadrature orders is reported as a diagnostic; it is not an error
certificate. The delta-method sample sizes are asymptotic and assume the theorem's iid retained
records. Deterministic; no data; no simulation.
Run:  python sensitivity.py  ->  sensitivity.json
"""
import json
import math

import numpy as np

from p7core import FEATURE_BINS, Par, amoment, dmoment_dd, jacobian_full, jacobian_gvak, raw_F, stat_cov

BENCH = {
    "B1": dict(a=1.0, k=1.0, v=1.0, lam=1.0, mu0=0.0, v0=1.0),
    "B2": dict(a=1.0, k=1.0, v=1.0, lam=0.05, mu0=0.0, v0=1.0),
    "B3": dict(a=0.01, k=1.0, v=0.25, lam=0.01, mu0=0.0, v0=1.0),
    "B4": dict(a=0.001, k=1.0, v=0.25, lam=0.01, mu0=0.0, v0=1.0),
    "B5": dict(a=0.01, k=1.0, v=0.25, lam=0.01, mu0=3.0, v0=1.0),
    "B6": dict(a=0.01, k=1.0, v=0.25, lam=0.01, mu0=-3.0, v0=1.0),
    "B7": dict(a=0.01, k=0.1, v=0.25, lam=0.01, mu0=0.0, v0=1.0),
    "B8": dict(a=0.01, k=10.0, v=0.25, lam=0.01, mu0=0.0, v0=1.0),
}
ALPHA = 0.05
Z95 = 1.959963984540054
REL = 0.10                      # target: 95% half-width equal to 10% of the declared scale
NAMES = ["g", "v", "a", "k", "d"]
MOV, NUI = [0, 2, 4], [1, 3]


def relchange(X, Y):
    return float(np.max(np.abs(X - Y) / (np.max(np.abs(Y), axis=1, keepdims=True) + 1e-300)))


def n_for(cov, scales):
    return [float((Z95 / REL) ** 2 * cov[i, i] / scales[i] ** 2) for i in range(len(scales))]


OUT = {}
for name, b in BENCH.items():
    P = Par(g=0.0, v=b["v"], a=b["a"], k=b["k"], lam=b["lam"], mu0=b["mu0"], v0=b["v0"])
    lam, a = b["lam"], b["a"]
    scales = np.array([a, b["v"], a, b["k"], 2 * math.sqrt(a * lam)])
    res = dict(inputs=b, scales=dict(zip(NAMES, scales.tolist())))

    # ---- C3: Jacobian of the five auxiliary moments ------------------------------------------
    A_lo = jacobian_full(P, nz=10, nU=10, nr=10, nrho=48)
    A = jacobian_full(P, nz=20, nU=16, nr=16, nrho=96)
    F = np.real(raw_F(P, 20))
    sv = np.linalg.svd(A, compute_uv=False)
    A_rel = (A / F[:, None]) * scales[None, :]              # d log(moment) / d (parameter / scale)
    sv_rel = np.linalg.svd(A_rel, compute_uv=False)
    Qn, _ = np.linalg.qr(A_rel[:, NUI])
    Am = A_rel[:, MOV]
    Am_perp = Am - Qn @ (Qn.T @ Am)
    res["C3"] = dict(
        F=F.tolist(), A_columns_g_v_a_k_d=A.tolist(), quadrature_relchange_J=relchange(A_lo, A),
        singular_values_raw=sv.tolist(), sigma_min_raw=float(sv[-1]), cond_raw=float(sv[0] / sv[-1]),
        det_over_frobenius4=float(abs(np.linalg.det(A)) / np.linalg.norm(A) ** 4),
        singular_values_relative=sv_rel.tolist(), cond_relative=float(sv_rel[0] / sv_rel[-1]),
        movement_sv_relative_nuisance_known=np.linalg.svd(Am, compute_uv=False).tolist(),
        movement_sv_relative_after_nuisance_projection=np.linalg.svd(Am_perp, compute_uv=False).tolist(),
        right_singular_vector_weakest_relative=np.linalg.svd(A_rel)[2][-1].tolist(),
    )

    # ---- C4: covariance of the raw statistics and delta-method sample sizes --------------------
    S_lo, _ = stat_cov(P, FEATURE_BINS, nz=8)
    S, m = stat_cov(P, FEATURE_BINS, nz=14)
    Ainv = np.linalg.inv(A)
    cov5 = Ainv @ S @ Ainv.T
    N5 = n_for(cov5, scales)
    rho14 = math.exp(-14 * lam) * (1 - math.exp(-lam)) / lam
    rho3 = math.exp(-3 * lam) * (1 - math.exp(-lam)) / lam
    second_moments = np.diag(S) + m ** 2                      # E[S_j^2], an epsilon-free bound on Var(f_j/eps^p)
    res["C4"] = dict(
        Sigma=S.tolist(), quadrature_relchange_Sigma=relchange(S_lo, S), cv_of_statistics=(np.sqrt(np.diag(S)) / m).tolist(),
        N_retained_for_10pct_of_scale=dict(zip(NAMES, N5)),
        retention_prob_m14=rho14, retention_prob_m3=rho3,
        births_for_10pct_m14=dict(zip(NAMES, [n / rho14 for n in N5])),
        births_for_10pct_m3=dict(zip(NAMES, [n / rho3 for n in N5])),
        sum_second_moments=float(second_moments.sum()),
    )

    # ---- the best the theorem's own architecture could do with sharp constants -----------------
    # E_est: first-order size of (DF_eps - DF)/eps; H_est: second derivatives at the base point.
    # Both leave out the d direction, so they understate the true constants.
    second = [((j, j),) for j in (1, 2, 3)] + [((1, 1, 2), (1, 2, 2)), ((2, 2, 3), (2, 3, 3))]
    E_est = 0.0
    for group in second:
        col = np.zeros(4)
        for bins in group:
            for jc, nm in enumerate(("g", "v", "a", "k")):
                Pp = Par(**{**P.__dict__, nm: getattr(P, nm) + 1e-30j})
                col[jc] += 0.5 * np.imag(amoment(bins, Pp, 14)) / 1e-30
        E_est = max(E_est, float(np.max(np.abs(col))))
    H_est = 0.0
    for jc, nm in enumerate(("g", "v", "a", "k")):
        h = 1e-4 * scales[[0, 1, 2, 3]][jc]
        Jp = jacobian_gvak(Par(**{**P.__dict__, nm: getattr(P, nm) + h}), 16)
        Jm = jacobian_gvak(Par(**{**P.__dict__, nm: getattr(P, nm) - h}), 16)
        H_est = max(H_est, float(np.max(np.abs(Jp - Jm)) / (2 * h)))
    smin = float(sv[-1])
    D_box = min(1.0, 0.5 * math.sqrt(a / 2 * lam), math.sqrt(a / 2 * lam) / (16 * math.sqrt(math.pi)))
    r_b = min(1.0, a / 2, b["k"] / 2, b["v"] / 2, D_box)
    s_opt = min(1.0, smin)
    eps_opt = min(0.5, s_opt / (20 * E_est))
    r_opt = min(r_b / 4, s_opt / (40 * math.sqrt(5) * H_est))
    lg = math.log(10 / ALPHA)
    log10_L = math.log10(4) - math.log10(s_opt) - 2 * math.log10(eps_opt)
    log10_Nbreak = math.log10(10 * lg / 16) + 2 * log10_L - 2 * math.log10(r_opt)
    N_cheb = 4 * second_moments.sum() / (s_opt ** 2 * r_opt ** 2 * ALPHA)
    res["best_case_for_this_architecture"] = dict(
        sigma_min=smin, E_est_without_d=E_est, H_est_without_d=H_est, r_b=r_b,
        eps=eps_opt, r=r_opt, prior_movement_diameter_4r=4 * r_opt, r_over_scale=dict(zip(NAMES, (r_opt / scales).tolist())),
        log10_L=log10_L, log10_N_break_even_hoeffding=log10_Nbreak,
        log10_N_break_even_chebyshev_variant=math.log10(N_cheb),
    )

    # ---- C5: declared variants ------------------------------------------------------------------
    Sinv = np.linalg.inv(S)
    Jm_raw = A[:, MOV]
    cov_w2 = np.linalg.inv(Jm_raw.T @ Sinv @ Jm_raw)
    N_w2 = n_for(cov_w2, scales[MOV])
    Jad = A[:, [2, 4]]
    cov_w3 = np.linalg.inv(Jad.T @ Sinv @ Jad)
    N_w3 = n_for(cov_w3, scales[[2, 4]])
    stats16 = [(j,) for j in range(1, 15)] + [(1, 2), (2, 3)]
    J16 = np.column_stack([jacobian_gvak(P, 12, stats16), [dmoment_dd(s, P, 10, 12, 16, 64) for s in stats16]])
    S16, m16 = stat_cov(P, stats16, nz=8)
    w, V = np.linalg.eigh(S16)
    S16inv = (V / w) @ V.T
    info16 = J16.T @ S16inv @ J16
    cov_w4 = np.linalg.inv(info16)
    N_w4 = n_for(cov_w4, scales)
    # W4 with nuisance known, for the d direction specifically
    cov_w4m = np.linalg.inv(J16[:, MOV].T @ S16inv @ J16[:, MOV])
    N_w4m = n_for(cov_w4m, scales[MOV])
    # post hoc (not in DESIGN.md): entry mean mu0 unknown as a sixth parameter
    mu_col5 = np.array([len(s) for s in FEATURE_BINS]) * m
    rank5 = int(np.linalg.matrix_rank(np.column_stack([A, mu_col5]), tol=1e-9 * np.linalg.norm(A)))
    mu_col16 = np.array([len(s) for s in stats16]) * m16
    J17 = np.column_stack([J16, mu_col16])
    info17 = J17.T @ S16inv @ J17
    ev17 = np.linalg.eigvalsh(info17)
    # (mu0, v, k) reach every one of these moments only through K = exp(mu0 + (v0 + v)/2) and the
    # single adjacent-lag integral in Q, so the information matrix is exactly singular: report that,
    # not a pseudo-inverse.
    identified17 = bool(ev17[0] / ev17[-1] > 1e-10)
    res["C5"] = dict(
        W1_births_ratio_m14_over_m3=math.exp(11 * lam),
        W2_nuisance_known_N=dict(zip(["g", "a", "d"], N_w2)),
        W3_nuisance_and_g_known_N=dict(zip(["a", "d"], N_w3)),
        W4_sixteen_moments_N=dict(zip(NAMES, N_w4)),
        W4_sixteen_moments_nuisance_known_N=dict(zip(["g", "a", "d"], N_w4m)),
        W4_cond_Sigma16=float(w[-1] / w[0]),
        posthoc_mu0_unknown=dict(five_moments_rank_of_5x6_jacobian=rank5, five_moments_identified=False,
                                 sixteen_moments_identified=identified17,
                                 sixteen_moments_info_eig_min_over_max=float(ev17[0] / ev17[-1])),
    )
    OUT[name] = res

with open("sensitivity.json", "w") as f:
    json.dump(OUT, f, indent=1, default=float)


def fmt(x):
    return f"{x:10.3g}"


print("C3  sigma_min(raw)  cond(raw)  cond(rel)  det/F^4   | movement sv (rel): nuisance known / projected | quad dJ")
for n, r in OUT.items():
    c = r["C3"]
    print(n, fmt(c["sigma_min_raw"]), fmt(c["cond_raw"]), fmt(c["cond_relative"]), fmt(c["det_over_frobenius4"]), "|",
          " ".join(fmt(x) for x in c["movement_sv_relative_nuisance_known"]), "/",
          " ".join(fmt(x) for x in c["movement_sv_relative_after_nuisance_projection"]), "|", fmt(c["quadrature_relchange_J"]))
print("\nC4  retained N for a 95% half-width of 10% of scale (g v a k d) | rho14 | quad dSigma")
for n, r in OUT.items():
    c = r["C4"]
    print(n, " ".join(fmt(c["N_retained_for_10pct_of_scale"][p]) for p in NAMES), "|", fmt(c["retention_prob_m14"]), "|", fmt(c["quadrature_relchange_Sigma"]))
print("\nbest case for the theorem's architecture: sigma_min E_est H_est eps r  log10L  log10N(Hoeffding) log10N(Chebyshev)")
for n, r in OUT.items():
    c = r["best_case_for_this_architecture"]
    print(n, fmt(c["sigma_min"]), fmt(c["E_est_without_d"]), fmt(c["H_est_without_d"]), fmt(c["eps"]), fmt(c["r"]),
          fmt(c["log10_L"]), fmt(c["log10_N_break_even_hoeffding"]), fmt(c["log10_N_break_even_chebyshev_variant"]))
print("\nC5  W2 (g a d) | W3 (a d) | W4 (g v a k d) | W4 nuisance known (g a d) | post hoc: mu0 unknown")
for n, r in OUT.items():
    c = r["C5"]
    print(n, " ".join(fmt(x) for x in c["W2_nuisance_known_N"].values()), "|", " ".join(fmt(x) for x in c["W3_nuisance_and_g_known_N"].values()), "|",
          " ".join(fmt(x) for x in c["W4_sixteen_moments_N"].values()), "|", " ".join(fmt(x) for x in c["W4_sixteen_moments_nuisance_known_N"].values()), "|",
          "identified with mu0 unknown (5 / 16 moments):", c["posthoc_mu0_unknown"]["five_moments_identified"], c["posthoc_mu0_unknown"]["sixteen_moments_identified"])
