"""D9 (Round 8): is the Reddit gapped-covariance 'entity drift' partly a SYSTEMATIC rank-dependent trend
(e.g. the 2019->20 ladder compression) inside the coarse tiers? Compare tier-demeaned window-change
covariances with covariances after residualizing x and y on a quadratic in permanent log-rank inside each
tier (removes any smooth rank-dependent common trend, keeps entity-specific persistence), and by period."""
import math, sys
from pathlib import Path
import numpy as np
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr
CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
for pn in ("reddit_comments_ext", "reddit_submissions_long"):
    d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
    X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
    T, N = X.shape
    pen = np.where(np.isfinite(R), R, fl[:, None])
    pr = np.empty(N); pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
    pres = np.isfinite(R).mean(axis=0)
    Xc = X - cr._top_k_center(X, R, K)
    sel = (pr <= K) & (pres >= 0.5)
    z = np.log((pr - 0.5) / N)
    lab = np.where(pr <= K / 16, 0, np.where(pr <= K / 4, 1, 2))
    W, H, g = 13, 52, 13
    print(f"\n== {pn}")
    for o in range(0, T - (2 * H + g) + 1, W):
        with np.errstate(all="ignore"):
            a0 = np.nanmean(Xc[o:o + W], 0); a1 = np.nanmean(Xc[o + H - W:o + H], 0)
            b0 = np.nanmean(Xc[o + H + g:o + H + g + W], 0); b1 = np.nanmean(Xc[o + 2 * H + g - W:o + 2 * H + g], 0)
        x, y = a1 - a0, b1 - b0
        ok = sel & np.isfinite(x) & np.isfinite(y)
        row = []
        for j in (0, 1, 2):
            m = ok & (lab == j)
            xj, yj, zj = x[m], y[m], z[m]
            c_dm = float(np.mean((xj - xj.mean()) * (yj - yj.mean())))
            A = np.column_stack([np.ones(m.sum()), zj, zj ** 2])
            xr = xj - A @ np.linalg.lstsq(A, xj, rcond=None)[0]; yr = yj - A @ np.linalg.lstsq(A, yj, rcond=None)[0]
            c_q = float(np.mean(xr * yr))
            # correlation of window change with rank (systematic rank trend strength)
            rx = float(np.corrcoef(xj, zj)[0, 1])
            row.append(f"tier{j}: dm={c_dm:+.4f} quad={c_q:+.4f} corr(x,z)={rx:+.2f}")
        print(f"  origin {o:3d} (x: wk {o}-{o+H}, y: wk {o+H+g}-{o+2*H+g}) | " + " | ".join(row))
