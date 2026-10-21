"""Post hoc diagnostic (2026-10-21, after R18-C's outputs were read): why a few Facebook world runs give a head share
near zero. `fb_stats` sums activity in levels, expm1(X), over the last 26-week block, for the top 54 and the top K
ranked by block mean of log activity. One extreme week can then dominate the top K's sum without lifting its unit's
block mean into the top 54. This rebuilds one world run exactly as `r18_facebook.mode_worlds` does and reports the
units that dominate the denominator.

    R18_DIR=<dir holding r18_fit.pkl> python r18c_headshare_diag.py controlb0.019 0
"""
import pickle
import sys

import numpy as np

import r18_facebook as r


def main(variant, seed):
    with open(r._fit_path(), "rb") as f:
        fit = pickle.load(f)
    K, T = fit["K"], fit["T"]
    q, L0 = r._world_q(variant, fit["p"], fit["ladder"], fit["L0"], K)
    fr, _, _ = r.h.sim_frame(q, L0, T, seed, K)
    X, R, _, _, _ = r.m1.matrices(fr, K)
    nb = T // r.BLOCK
    start = T - nb * r.BLOCK
    M = r.ba.block_means(X, r.BLOCK, start=start, min_frac=r.PRES)
    top = r._order(M[-1])
    lo, hi = start + (nb - 1) * r.BLOCK, start + nb * r.BLOCK
    A = np.nan_to_num(np.expm1(X[lo:hi]), nan=0.0)
    col = A[:, top[:K]].sum(0)
    share = col / col.sum()
    print(f"{variant} seed {seed}: head share {col[:K // 64].sum() / col.sum():.3f}; median weekly log activity of the top K "
          f"{np.nanmedian(X[lo:hi][:, top[:K]]):.2f}")
    for j in np.argsort(-share)[:5]:
        u = top[j]
        x = X[lo:hi, u]
        print(f"  block rank {j + 1}: share of top-K sum {share[j]:.3f}; block mean {M[-1][u]:.2f}; max week {np.nanmax(x):.2f} "
              f"(week {lo + int(np.nanargmax(x))}); weeks present {int(np.isfinite(x).sum())}/{hi - lo}; "
              f"second-largest week {np.sort(x[np.isfinite(x)])[-2]:.2f}")


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
