"""Deterministic checks of PROOF_1_RESULT.md (Codex, 2026-10-12). Linear algebra
and quadrature only: no data, no model code, no stochastic simulation."""
import json
import numpy as np
from scipy.linalg import expm
from scipy import integrate

out = {}

# ---- Claim 3: the hidden-oscillation alias --------------------------------
def alias(delta, kappa):
    w = 2 * np.pi / delta
    d = kappa / (2 * w)
    s = np.sqrt(1 + d * d)
    K = np.array([[d, -s], [s, -d]])
    A0, Q0 = -kappa * np.eye(2), 2 * kappa * np.eye(2)
    A1, Q1 = -kappa * np.eye(2) + w * K, np.diag([kappa, 3 * kappa])
    r = np.exp(-kappa * delta)
    res = dict(
        K2_plus_I=float(np.abs(K @ K + np.eye(2)).max()),
        lyap0=float(np.abs(A0 + A0.T + Q0).max()),
        lyap1=float(np.abs(A1 + A1.T + Q1).max()),
        expm1_minus_rI=float(np.abs(expm(A1 * delta) - r * np.eye(2)).max()),
        expm0_minus_rI=float(np.abs(expm(A0 * delta) - r * np.eye(2)).max()),
        eig_real_max=float(np.linalg.eigvals(A1).real.max()),
    )
    # lag covariance of the first coordinate, formula (10) against expm
    hs = np.linspace(0.05, 3 * delta, 40)
    f10 = np.exp(-kappa * hs) * (np.cos(w * hs) + d * np.sin(w * hs))
    num = np.array([expm(A1 * h)[0, 0] for h in hs])
    res["eq10_maxerr"] = float(np.abs(f10 - num).max())
    res["corr_half_interval"] = float(expm(A1 * delta / 2)[0, 0])
    res["var_increment_half_interval_over_stationary_var"] = float(2 * (1 - expm(A1 * delta / 2)[0, 0]))
    res["var_increment_one_interval_over_stationary_var"] = float(2 * (1 - r))
    beta = A1[0]                                   # drift row of the first coordinate
    res["E_beta2"], res["E_beta2_formula"] = float(beta @ beta), float(w * w + kappa ** 2 / 2)
    res["bracket_model0_model1"] = [2 * kappa, kappa]
    res["b_star_slope_model0_model1"] = [float(A0[0, 0]), float(A1[0, 0])]   # b*(x) = slope * x
    return res

out["claim3"] = {f"delta={D},kappa={k}": alias(D, k) for D in (1.0, 7.0) for k in (0.005, 0.02, 0.3)}

# ---- Audit C and eq. (20): OU, by quadrature ------------------------------
def ou(a, kappa, h):
    v = a / (2 * kappa)
    D = (a / kappa) * (1 - np.exp(-kappa * h))
    varB = kappa ** 2 * v * integrate.dblquad(lambda r, s: np.exp(-kappa * abs(r - s)), 0, h, 0, h)[0]
    cov = -kappa * a * integrate.dblquad(lambda r, s: np.exp(-kappa * (s - r)), 0, h, 0, lambda s: s)[0]
    closed = a * (h - (1 - np.exp(-kappa * h)) / kappa)
    return dict(D=D, recomposed=a * h + varB + 2 * cov, varB=varB, varB_closed=closed, cov=cov, cov_closed=-closed)

out["auditC"] = ou(0.003, 0.01, 26.0)

# ---- what the two-lag statistic returns under reversion (eq. 19) ----------
def two_lag_factor(kappa, h1, h2):
    return (np.exp(-kappa * h1) - np.exp(-kappa * h2)) / (kappa * (h2 - h1))

out["two_lag_factor"] = {f"kappa={k}": dict(h13_26=two_lag_factor(k, 13, 26), h13_52=two_lag_factor(k, 13, 52),
                                             h13_65=two_lag_factor(k, 13, 65))
                         for k in (0.005, 0.01, 0.02, 0.04, 0.07)}

# ---- Claim 8: the three-date OU inversion and its conditioning -------------
def claim8(kappa, v=1.0, delta=1.0, eps=0.01):
    r = np.exp(-kappa * delta)
    g1, g2 = v * r, v * r * r
    k_hat = -np.log(g2 / g1) / delta
    k_pert = -np.log(g2 / g1 * (1 + eps)) / delta          # ratio of covariances off by +eps
    return dict(recovered=float(k_hat), rel_change_kappa_per_1pct_ratio_error=float(abs(k_pert - kappa) / kappa))

out["claim8"] = {f"kappa={k}": claim8(k) for k in (0.005, 0.01, 0.02, 0.07)}

# ---- section 7: the two-subtype example ------------------------------------
x = np.linspace(-4, 4, 81)
m, v, eps = 0.7, 0.5, 0.4
pp, pm = np.exp(-(x - m) ** 2 / (2 * v)), np.exp(-(x + m) ** 2 / (2 * v))
out["sec7_tanh_maxerr"] = float(np.abs((1 + eps) * pp / (pp + pm) + (1 - eps) * pm / (pp + pm) - (1 + eps * np.tanh(m * x / v))).max())

# ---- eq. (31): variance of the mean of overlapping increments --------------
def overlap(m_, h, d=1.0, a=1.0):
    k = np.arange(m_)
    Kmat = a * np.clip(h - np.abs(k[:, None] - k[None, :]) * d, 0, None)
    direct = Kmat.sum() / m_ ** 2
    formula = a / m_ ** 2 * (m_ * h + 2 * sum((m_ - dd) * max(h - dd * d, 0) for dd in range(1, m_)))
    return float(abs(direct - formula))

out["eq31_maxerr"] = max(overlap(60, 26), overlap(160, 52), overlap(71, 65))

# ---- Claim 5 at the project's lags, with the smallest defensible constants --
# (reviewer's illustration, not in the note): drift bound K = g, variance a = 2 g S
g, S = 0.003, 0.5
a = 2 * g * S
F = lambda h: g ** 2 * h ** 2 + 2 * g * np.sqrt(a) * h ** 1.5
out["claim5_illustration"] = dict(a=a, enclosure_halfwidth_13_26=(F(26) + F(13)) / 13,
                                  halfwidth_over_target=(F(26) + F(13)) / 13 / a)

# ---- reviewer's point: a finite-lag balance holds with matched moments -----
# stationary OU, N(0, v): lag-h mean change m_h(x) and mean squared change s_h(x) by start x.
# Second-order (diffusion-form) balance: s_h(x) p(x) = -2 * int_x^inf m_h p.  Exact ratio lhs/rhs:
def matched(kappa, h, z):                      # z = x / sd
    r = np.exp(-kappa * h)
    return ((1 - r) * z * z + (1 + r)) / 2
out["matched_horizon_balance_ratio"] = {f"kappa*h={kh}": {f"x={z}sd": matched(1.0, kh, z) for z in (0.0, 1.0, 2.0)}
                                        for kh in (0.02, 0.13, 0.26, 0.52)}
print(json.dumps(out, indent=1, default=float))
