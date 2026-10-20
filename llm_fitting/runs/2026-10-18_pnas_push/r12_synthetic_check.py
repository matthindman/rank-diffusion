"""R12: known-answer check of the statistics (synthetic; no data).

    python llm_fitting/runs/2026-10-18_pnas_push/r12_synthetic_check.py
"""
"""Known-answer check of the R12 statistics on synthetic panels (no data): a random walk with passing noise, with and
without fixed heterogeneity b_i ~ N(0, sb^2).  Expected: gapped Pearson covariance ~ (52 sb)^2 at every gap with
heterogeneity, ~0 without; two-spacing lasting variance ~ q (+ 3 B h at h = 26 with heterogeneity)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import numpy as np
import r12_hetgrowth as r
rng = np.random.default_rng(1)
T, M, q = 212, 6000, 0.004
for sb in (0.0, 0.006):
    b = sb * rng.standard_normal(M)
    L = np.cumsum(np.sqrt(q) * rng.standard_normal((T, M)), axis=0) + np.arange(T)[:, None] * b[None, :]
    xi = np.zeros((T, M))
    for t in range(1, T):
        xi[t] = 0.87 * xi[t - 1] + 0.15 * rng.standard_normal(M)
    X = L + xi + 0.2 * rng.standard_normal((T, M))
    lab = np.repeat([0, 1, 2], M // 3)
    g = r.gapped_cov(X, lab, (0, 13, 26, 52))
    sf = r.sf_lasting(X, lab)
    print(f"sb {sb}: expected cov {(52*sb)**2:.4f}; got " + ", ".join(f"g{k}: {np.mean(v['cov']):.4f} (rho {v['rho']:+.3f})" for k, v in g.items()))
    print(f"   two-spacing lasting variance {np.mean(sf):.5f}; expected q {q} + 3 B h = {q + 3 * sb**2 * 26:.5f}")
    print("   asymmetry", r.asymmetry(X, lab))
