#!/usr/bin/env python3
"""(e) 2026-07-05 (review B4): MD weighting robustness -- identity vs
diagonal-precision weighting of the covariance-structure fit.

The production estimator fits gamma_0..L (+ D(h) under --md-vr[-long]) by
UNWEIGHTED lstsq on a (kappa, phi) grid.  Moments have very different
precision (gamma_0 vs D(52)); under misspecification the weighting choice
can move the compromise.  This audit asks: do the fitted (kappa, phi,
sigma_eta, sigma_nu, sigma_e) move materially when each moment row is
weighted by its inverse bootstrap SE?

Design (declared, standalone -- production defaults untouched):
  - Panel-level moments on the restricted universe: pooled over all entities,
    and separately over the HEAD zbar-quintile (where the split matters most).
  - gamma_k = pooled autocovariance of factor-removed changes u at lags 0..6;
    D(h) = pooled variance of factor-removed h-step changes.
  - SEs: entity bootstrap (B=200, joint across the moment vector).
  - Fit: the same (a, phi) grid + clipped NNLS as _md_partition, run twice --
    identity weights vs w = 1/SE.

Usage: python llm_fitting/weighting_robustness.py <platform> <K> [--long]
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

LAGS = 6


def panel_matrices(df: pd.DataFrame):
    T = int(df["period"].max()) + 1
    X = df.pivot(index="period", columns="entity_id", values="X") \
          .reindex(range(T)).to_numpy(dtype=float)
    dX = np.diff(X, axis=0)
    F = np.nanmean(dX, axis=1, keepdims=True)
    U = dX - F                                   # factor-removed changes
    cumF = np.vstack([np.zeros((1, 1)), np.cumsum(F, axis=0)])
    Xf = X - cumF                                # factor-removed levels
    N = int(round(np.mean(np.sum(np.isfinite(X), axis=1))))
    with np.errstate(invalid="ignore"):
        rbar = np.nanmean(np.where(df.pivot(index="period", columns="entity_id",
                                            values="rank").reindex(range(T))
                                   .to_numpy(dtype=float) > 0,
                                   df.pivot(index="period", columns="entity_id",
                                            values="rank").reindex(range(T))
                                   .to_numpy(dtype=float), np.nan), axis=0)
    zbar = np.log(np.clip((rbar - 0.5) / N, mrd.Z_CLIP, 1.0))
    return U, Xf, zbar


def moments(U: np.ndarray, Xf: np.ndarray, cols: np.ndarray, d_h: tuple):
    """gamma_0..LAGS and D(h) pooled over the given entity columns."""
    u = U[:, cols]
    g = []
    for k in range(LAGS + 1):
        a = u[:-k] if k else u
        b = u[k:] if k else u
        prod = a * b
        g.append(np.nanmean(prod))
    d = []
    xf = Xf[:, cols]
    for h in d_h:
        dh = xf[h:] - xf[:-h]
        d.append(np.nanvar(dh, ddof=1))
    return np.array(g), np.array(d)


def md_fit(gk: np.ndarray, d_mom: np.ndarray, d_h: tuple,
           w: np.ndarray | None):
    """_md_partition's grid fit with optional row weights (w = 1/SE)."""
    L = len(gk) - 1
    y = np.concatenate([gk, d_mom]) if len(d_mom) else gk
    best = (np.inf, None)
    for a in mrd.A_GRID_VR:
        for phi in mrd.PHI_GRID:
            rows = [[2.0 / (1 - a), 2.0 / (1 - phi)], [-1.0, -1.0]]
            rows += [[-a ** (k - 1), -phi ** (k - 1)] for k in range(2, L + 1)]
            n_g = len(rows)
            rows += [[2.0 * (1 - a ** h) / (1 - a) ** 2,
                      2.0 * (1 - phi ** h) / (1 - phi) ** 2] for h in d_h]
            X = np.array(rows)
            noise = np.array([[2.0], [-1.0]] + [[0.0]] * (n_g - 2)
                             + [[2.0]] * (len(rows) - n_g))
            X = np.hstack([X, noise])
            Xw = X * w[:, None] if w is not None else X
            yw = y * w if w is not None else y
            coef, *_ = np.linalg.lstsq(Xw, yw, rcond=None)
            coef = np.clip(coef, 0.0, None)
            sse = float(np.sum((yw - Xw @ coef) ** 2))
            if sse < best[0]:
                best = (sse, (a, phi, coef))
    a, phi, coef = best[1]
    A, B, s_e = coef[0], coef[1], coef[2]
    return dict(kappa=1.0 - a, phi=phi,
                sigma_eta=np.sqrt(max(A * (1 + a) / (1 - a), 0.0)),
                sigma_nu=np.sqrt(max(B * (1 + phi) / (1 - phi), 0.0)),
                sigma_e=np.sqrt(max(s_e, 0.0)))


def run(U, Xf, cols, d_h, label, B=200, seed=0):
    g0, d0 = moments(U, Xf, cols, d_h)
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(B):
        bs = cols[rng.integers(0, len(cols), size=len(cols))]
        gb, db = moments(U, Xf, bs, d_h)
        draws.append(np.concatenate([gb, db]) if len(db) else gb)
    se = np.std(np.array(draws), axis=0, ddof=1)
    se = np.where(se > 0, se, np.nanmax(se))
    fi = md_fit(g0, d0, d_h, None)
    fw = md_fit(g0, d0, d_h, 1.0 / se)
    print(f"\n  [{label}]  n_ent={len(cols)}  moment |SE| range "
          f"{se.min():.2e}..{se.max():.2e} ({se.max() / se.min():.0f}x spread)")
    print(f"    {'':<10}{'kappa':>8}{'phi':>7}{'sig_eta':>9}{'sig_nu':>8}{'sig_e':>8}")
    for name, f in (("identity", fi), ("precision", fw)):
        print(f"    {name:<10}{f['kappa']:>8.3f}{f['phi']:>7.2f}"
              f"{f['sigma_eta']:>9.3f}{f['sigma_nu']:>8.3f}{f['sigma_e']:>8.3f}")
    return fi, fw


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    platform = args[0] if args else "reddit"
    K = int(args[1]) if len(args) > 1 else 5000
    d_h = mrd.VR_MOM_H_LONG if "--long" in sys.argv else ()
    cfg = mrd.PLATFORMS[platform]
    df = mrd.restrict_universe(mrd.load_panel(cfg), K, buffer_mult=4)
    T = int(df["period"].max()) + 1
    d_h = tuple(h for h in d_h if T >= 2.5 * h)
    U, Xf, zbar = panel_matrices(df)
    fin = np.isfinite(zbar)
    allc = np.where(fin)[0]
    head = np.where(fin & (zbar <= np.quantile(zbar[fin], 0.2)))[0]
    print(f"=== MD weighting robustness: {platform} K={K} T={T} "
          f"lags 0..{LAGS} D(h)={d_h or 'none'} ===")
    run(U, Xf, allc, d_h, "pooled")
    run(U, Xf, head, d_h, "head quintile")


if __name__ == "__main__":
    main()
