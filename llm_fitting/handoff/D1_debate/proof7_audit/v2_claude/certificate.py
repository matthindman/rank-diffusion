"""Stage C1/C2: the explicit certificate of Proof 7, evaluated exactly as written, plus two
variants that the submitted argument already licenses. mpmath, 60 significant digits; base-10
logarithms are reported because most constants are far outside floating-point range.

Formulas typed from PROOF_7_RESULT.md: (3.1)-(3.8), (5.2), (5.4), (5.5), (6.1), (6.4), (7.3), (1.6).
Run:  python certificate.py  ->  certificate.json
"""
import json

from mpmath import mp, mpf, sqrt, exp, log, log10, pi, gamma, factorial, e as E

mp.dps = 60

BENCH = {
    "B1": dict(a=1, k=1, v=1, lam=1, mu0=0, v0=1),
    "B2": dict(a=1, k=1, v=1, lam="0.05", mu0=0, v0=1),
    "B3": dict(a="0.01", k=1, v="0.25", lam="0.01", mu0=0, v0=1),
    "B4": dict(a="0.001", k=1, v="0.25", lam="0.01", mu0=0, v0=1),
    "B5": dict(a="0.01", k=1, v="0.25", lam="0.01", mu0=3, v0=1),
    "B6": dict(a="0.01", k=1, v="0.25", lam="0.01", mu0=-3, v0=1),
    "B7": dict(a="0.01", k="0.1", v="0.25", lam="0.01", mu0=0, v0=1),
    "B8": dict(a="0.01", k=10, v="0.25", lam="0.01", mu0=0, v0=1),
}
ALPHA = mpf("0.05")


def certificate(b, H_env=15, j0_two=False, m_retain=14):
    a_s, k_s, v_s, lam, mu0, v0 = (mpf(b[x]) for x in ("a", "k", "v", "lam", "mu0", "v0"))
    a_m, a_p = a_s / 2, 3 * a_s / 2
    k_m, k_p = k_s / 2, 3 * k_s / 2
    v_m, v_p = v_s / 2, 3 * v_s / 2
    H = mpf(H_env)

    def R(i, j, l):                                                       # (3.1)
        n = 2 * i + j + l
        return sqrt(factorial(n)) * gamma(1 + mpf(j - l) / 2) / (2 ** i * a_m ** (mpf(n) / 2) * lam ** (1 + mpf(j - l) / 2))

    K = [max(R(i, r - i, 1) for i in range(r + 1)) for r in range(3)]
    Q = [lam * max(R(i, r - i, 0) for i in range(r + 1)) for r in range(3)]
    D = min(mpf(1), sqrt(a_m * lam) / 2, 1 / (16 * K[0]))                   # (3.2)
    n0 = 1 / sqrt(2 * pi * v0)
    n1 = exp(-mpf(1) / 2) / (sqrt(2 * pi) * v0)
    P0 = 2 * n0
    P1 = 2 * (Q[1] + 4 * K[0] + 4 * D * K[1])                               # (3.3)
    P2 = 2 * (Q[2] + 8 * K[1] + 4 * D * K[2] + 8 * (K[0] + D * K[1]) * P1 + 2 * D * K[0] * P1 ** 2)
    Px = (n1 + D * K[0] * P0 ** 2) / (1 - 2 * D * K[0])
    bx, b1, b2, bx1, bxx = D * P0, 1 + D * P1, 2 * P1 + D * P2, P0 + D * n0 * P1, D * Px
    X1 = exp(bx * H) * (H * b1 + 1 / (2 * sqrt(a_m)))                       # (3.5)
    X2 = exp(bx * H) * (1 / (4 * a_m ** mpf("1.5")) + H * (b2 + 2 * bx1 * X1 + bxx * X1 ** 2))
    j0 = mpf(2) if j0_two else 1 + k_p * H                                  # (3.6)
    j1 = H + k_p * H ** 2 / 2
    j2 = H ** 2 + k_p * H ** 3 / 3
    s0, s1, s2 = sqrt(2 * k_p), (2 * k_m) ** mpf("-0.5"), (2 * k_m) ** mpf("-1.5")
    t0 = 1 + s0 * j0
    t1 = H + s1 * j0 + s0 * j1
    t2 = H ** 2 + s2 * j0 + 2 * s1 * j1 + s0 * j2
    Z1 = t0 / (2 * sqrt(v_m)) + sqrt(v_p) * t1
    Z2 = t0 / (4 * v_m ** mpf("1.5")) + t1 / sqrt(v_m) + sqrt(v_p) * t2
    G1 = mpf(3) / 2 * X1 + Z1
    G2 = mpf(9) / 4 * X2 + Z2
    CX = (1 + a_p / 2 + D) * H                                              # (3.7)
    EX = sqrt(v0) + sqrt(a_p) + sqrt(v_p) * t0

    def B(r, n):
        return 64 * factorial(n) * exp(r * (mu0 + CX) + (r * EX + 1) + (H + 1) * (r * EX + 1) ** 2)

    BF = max(G1 * B(1, 1), 2 * G1 * B(2, 1))                                # (3.8)
    HF = max((G1 ** 2 + G2) * B(1, 2), (4 * G1 ** 2 + 2 * G2) * B(2, 2))
    EF = max(G1 * B(2, 1), 3 * G1 * B(3, 1))
    chi_s = (1 - exp(-lam)) * a_s / (4 * sqrt(2 * pi) * sqrt(2 * v0 + 5 * a_s)) * exp(-(2 * v0 + 5 * a_s) / 8)   # (5.2)
    Ks = exp(mu0 + (v0 + v_s) / 2)
    Cs = exp(2 * mu0 + 2 * v0 + v_s)
    dfrak = Ks ** 3 * Cs ** 2 * exp(a_s) * v_s * exp(-2 * k_s) * chi_s / 8   # (5.4)
    s = min(mpf(1), dfrak / (5 * BF) ** 4)                                  # (5.5)
    eps = min(mpf(1) / 2, s / (20 * EF))                                    # (6.1)
    rb = min(mpf(1), a_s / 2, k_s / 2, v_s / 2, D)
    r = min(rb / 4, s / (40 * sqrt(5) * HF))
    L = 4 / (s * eps ** 2)                                                  # (6.4)
    lg = log(10 / ALPHA)
    N_at_t_eq_r = 10 * L ** 2 * lg / r ** 2                                 # (7.3) with t = r
    N_break = 10 * L ** 2 * lg / (4 * r) ** 2                               # statistical term equals 4r
    rho = exp(-lam * m_retain) * (1 - exp(-lam)) / lam                      # (1.4)
    out = dict(
        D=D, K0=K[0], K1=K[1], K2=K[2], Q1=Q[1], Q2=Q[2], P1=P1, P2=P2, Px=Px,
        X1=X1, X2=X2, t0=t0, G1=G1, G2=G2, EX=EX, CX=CX,
        B21=B(2, 1), B31=B(3, 1), BF=BF, HF=HF, EF=EF, chi_star=chi_s, dfrak=dfrak,
        s=s, eps=eps, r_b=rb, r=r, L=L, prior_movement_diameter_4r=4 * r,
        N_retained_for_t_eq_r=N_at_t_eq_r, N_retained_break_even_with_4r=N_break,
        retention_prob=rho, births_per_retained=1 / rho,
        births_break_even=N_break / rho,
    )
    return {k: dict(log10=float(log10(val)), value=(float(val) if abs(log10(val)) < 300 else None)) for k, val in out.items()}


OUT = {}
for name, b in BENCH.items():
    OUT[name] = dict(
        inputs={k: str(v) for k, v in b.items()},
        as_written=certificate(b, 15, False),
        variant_H4=certificate(b, 4, False),
        variant_H4_J2=certificate(b, 4, True),
    )
    # arithmetic check: recompute at 120 digits and compare log10 L
    mp.dps = 120
    chk = certificate(b, 15, False)["L"]["log10"]
    mp.dps = 60
    OUT[name]["log10_L_recomputed_at_120_digits"] = chk
    OUT[name]["log10_L_precision_difference"] = abs(chk - OUT[name]["as_written"]["L"]["log10"])

with open("certificate.json", "w") as f:
    json.dump(OUT, f, indent=1)

cols = ["D", "EX", "BF", "HF", "EF", "dfrak", "s", "eps", "r", "L", "N_retained_break_even_with_4r", "births_break_even", "retention_prob"]
for variant in ("as_written", "variant_H4", "variant_H4_J2"):
    print("\n" + variant + "  (log10 values)")
    print("id  " + "  ".join(f"{c[:14]:>14}" for c in cols))
    for name in BENCH:
        print(name + "  " + "  ".join(f"{OUT[name][variant][c]['log10']:14.4g}" for c in cols))
print("\nmax |log10 L (60 digits) - log10 L (120 digits)|:", max(OUT[n]["log10_L_precision_difference"] for n in BENCH))
