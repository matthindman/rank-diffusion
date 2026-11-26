"""D10 (Round 8): tier definition for the drift rank law on planted panels. Compares tier membership by
whole-panel permanent rank (selection: high-drift risers enter the head tier) with membership by the
entity's mean rank over the FIRST sub-window [o, o+W) of each gapped window pair (its rank when x starts)."""
import sys, math
import numpy as np
sys.path.insert(0, '.')
import common_rankdiff_opus_r8 as cr
from test_common_rankdiff_opus_r8 import _planted_drift_panel

def tier_sd(X, R, K, p, by_window_rank):
    T, N = X.shape
    fl = np.sum(np.isfinite(R), 1) + 1.0
    pen = np.where(np.isfinite(R), R, fl[:, None])
    pr = np.empty(N); pr[np.argsort(pen.mean(0), kind='stable')] = np.arange(1, N + 1)
    Xc = X - cr._top_k_center(X, R, K)
    W, H, g = (13, 52, 13) if T >= 160 else (4, 26, 4)
    gd = cr._G_drift(0.5 ** (1 / cr.DRIFT_HL_FIXED), W, H, g)
    gx = cr._G_ar1(p.phi, W, H, g)
    covs = {0: [], 1: [], 2: []}; zs = {0: [], 1: [], 2: []}; cx = {0: [], 1: [], 2: []}
    for o in range(0, T - (2 * H + g) + 1, W):
        a0 = Xc[o:o + W].mean(0); a1 = Xc[o + H - W:o + H].mean(0)
        b0 = Xc[o + H + g:o + H + g + W].mean(0); b1 = Xc[o + 2 * H + g - W:o + 2 * H + g].mean(0)
        x, y = a1 - a0, b1 - b0
        r = np.nanmean(R[o:o + W], 0) if by_window_rank else pr
        rk = np.empty(N); rk[np.argsort(r, kind='stable')] = np.arange(1, N + 1)
        lab = np.where(rk <= K / 16, 0, np.where(rk <= K / 4, 1, np.where(rk <= K, 2, -1)))
        z = np.log((rk - 0.5) / N)
        for j in (0, 1, 2):
            m = lab == j
            covs[j].append(float(np.mean((x[m] - x[m].mean()) * (y[m] - y[m].mean()))))
            zs[j].append(float(z[m].mean()))
            cx[j].append(float(np.mean(cr.v_xi_of(p, z[m]))) * gx)
    return {j: (float(np.mean(zs[j])) - p.z_K, math.sqrt(max((np.mean(covs[j]) - np.mean(cx[j])) / gd, 0))) for j in (0, 1, 2)}

for b in (0.0, 0.3, 0.6):
    for seed in (31, 32):
        p_true, X, R = _planted_drift_panel(sd_K=0.014, b_d=b, seed=seed)
        for mode in (False, True):
            t = tier_sd(X, R, 600, p_true, mode)
            xs = np.array([t[j][0] for j in t]); ys = np.log([max(t[j][1], 1e-6) for j in t])
            slope = np.polyfit(np.minimum(xs, 0), ys, 1)[0]
            print(f"b={b:.1f} seed={seed} tiers by {'window-start rank' if mode else 'permanent rank   '}: " +
                  " ".join(f"[dz={t[j][0]:+.2f} sd={t[j][1]:.4f} planted={0.014*math.exp(b*min(t[j][0],0)):.4f}]" for j in t) + f"  slope={slope:+.3f}")
