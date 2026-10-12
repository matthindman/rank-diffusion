"""Diagnostic only: change in the Stage C Jacobian and covariance when every quadrature order is
raised well above the orders used in sensitivity.py. Not an error certificate.
Run:  python convergence_check.py  ->  convergence_check.json
"""
import json
import numpy as np
from p7core import FEATURE_BINS, Par, jacobian_full, stat_cov
from sensitivity import BENCH, relchange

OUT = {}
for name, b in BENCH.items():
    P = Par(g=0.0, v=b["v"], a=b["a"], k=b["k"], lam=b["lam"], mu0=b["mu0"], v0=b["v0"])
    A1 = jacobian_full(P, nz=20, nU=16, nr=16, nrho=96)
    A2 = jacobian_full(P, nz=28, nU=24, nr=24, nrho=160)
    S1, _ = stat_cov(P, FEATURE_BINS, nz=14)
    S2, _ = stat_cov(P, FEATURE_BINS, nz=20)
    s1, s2 = np.linalg.svd(A1, compute_uv=False)[-1], np.linalg.svd(A2, compute_uv=False)[-1]
    OUT[name] = dict(jacobian_relchange=relchange(A1, A2), sigma_min_relchange=float(abs(s1 - s2) / s2),
                     covariance_relchange=relchange(S1, S2))
    print(name, {k: f"{v:.2e}" for k, v in OUT[name].items()})
json.dump(OUT, open("convergence_check.json", "w"), indent=1)
