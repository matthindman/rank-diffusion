"""D8 (Round 8): false-positive persistent drift on DRIFT-FREE planted panels.
Hypothesis: the Atlas balancing drift g(current rank) varies within each coarse permanent-rank tier, so an
entity's own average g acts as a persistent drift in successive window changes (Cov ~ H^2 Var_tier(g)).
Compares, per tier, the raw gapped covariance after (a) tier demeaning (R4-6), (b) residualizing x and y on a
quadratic in permanent log-rank inside the tier, against (c) the planted model's H^2 Var_tier(g(z_perm))."""
import math, sys
from pathlib import Path
import numpy as np
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr
from test_common_rankdiff_opus_r8 import _make_planted_params

def covs(X, R, K, W, H, g, resid):
    T, N = X.shape
    fl = np.sum(np.isfinite(R), axis=1) + 1.0
    pen = np.where(np.isfinite(R), R, fl[:, None])
    pr = np.empty(N); pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
    Xc = X - cr._top_k_center(X, R, K)
    z = np.log((pr - 0.5) / N)
    lab = np.where(pr <= K / 16, 0, np.where(pr <= K / 4, 1, np.where(pr <= K, 2, -1)))
    out = {0: [], 1: [], 2: []}
    for o in range(0, T - (2 * H + g) + 1, W):
        a0 = Xc[o:o + W].mean(0); a1 = Xc[o + H - W:o + H].mean(0)
        b0 = Xc[o + H + g:o + H + g + W].mean(0); b1 = Xc[o + 2 * H + g - W:o + 2 * H + g].mean(0)
        x, y = a1 - a0, b1 - b0
        for j in (0, 1, 2):
            m = lab == j
            xj, yj = x[m], y[m]
            if resid:
                A = np.column_stack([np.ones(m.sum()), z[m], z[m] ** 2])
                xj = xj - A @ np.linalg.lstsq(A, xj, rcond=None)[0]
                yj = yj - A @ np.linalg.lstsq(A, yj, rcond=None)[0]
            else:
                xj, yj = xj - xj.mean(), yj - yj.mean()
            out[j].append(float(np.mean(xj * yj)))
    return {j: float(np.mean(v)) for j, v in out.items()}, z, lab

for label, kw, T in [("zero_temperament (T=120)", dict(N=2000, K=500, s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0), 120),
                     ("gaussian K=600 (T=200)", dict(N=2400, K=600, s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0), 200)]:
    p, L0 = _make_planted_params(**kw)
    X, R, _ = cr.simulate_common_panel(p, L0, T_sim=T, seed=11, apply_absence=False, burn=20)
    W, H, g = (13, 52, 13) if T >= 160 else (4, 26, 4)
    c_tier, z, lab = covs(X, R, p.K, W, H, g, resid=False)
    c_res, _, _ = covs(X, R, p.K, W, H, g, resid=True)
    gd = cr._G_drift(0.5 ** (1 / cr.DRIFT_HL_FIXED), W, H, g)
    gx = p.phi
    c_xi = {j: float(np.mean(cr.v_xi_of(p, z[lab == j]))) * cr._G_ar1(p.phi, W, H, g) for j in (0, 1, 2)}
    gz = np.interp(z, p.drift_z, p.drift_g)
    atlas = {j: float(np.var(gz[lab == j])) * (H - W) ** 2 for j in (0, 1, 2)}
    print(f"\n== {label}: design W={W} H={H} g={g}, G_d={gd:.1f}")
    for j, nm in enumerate(("K/16", "K/4", "K")):
        print(f"  tier {nm:5s} cov_tierdemean={c_tier[j]:+.5f} cov_quadresid={c_res[j]:+.5f} c_xi={c_xi[j]:+.5f} "
              f"atlas H^2Var(g)={atlas[j]:.5f} | sigma_d tier={math.sqrt(max((c_tier[j]-c_xi[j])/gd,0)):.4f} resid={math.sqrt(max((c_res[j]-c_xi[j])/gd,0)):.4f}")
