#!/usr/bin/env python3
"""E4 runner — κ_i train→extension transport (protocol A1, executable per A6.4).

Registered construction (all pins in CONFIRMATION_PROTOCOL §11 A6.4):
  - Populations: scored complete-column population of each window (train =
    periods < T0; extension = periods >= T0), universe per §2; shared set =
    the id intersection; n reported.
  - TRAIN statistic: kappa_hat_i = rho_hat * r_i^train, where r_i^train is
    the per-entity log VR13 curvature demeaned within 5x5 rank x volatility
    quantile cells (kappa_probe machinery), and rho_hat = the split-half
    signal share measured on train, cov12 / (sd1*sd2), clipped to [0, 1]
    (EB shrinkage toward 0).
  - Cell EDGES from train quantiles; cell ASSIGNMENT of every entity by its
    TRAIN (rank, volatility) — reused verbatim on the extension (no
    extension-dependent recategorization).
  - EXTENSION statistic: r_i^ext = extension-window log VR13 curvature,
    demeaned within the train-assigned cells (means over the shared set).
  - Test: Spearman(kappa_hat, r^ext) >= 0.20 AND concentration ratio
    mean|r^ext| over (Q1 u Q5 of kappa_hat) / mean|r^ext| over Q3 >= 1.3.
    Bootstrap CIs reported as secondary uncertainty, never gates.

Usage (E4, after the intake gate PASSes):
  python llm_fitting/e4_kappa_transport.py reddit_comments_ext 136 --top-k 12500
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

N_CELLS = 5
H = 13


def _curv(X):
    """Per-entity log VR13 curvature + log 1-week change variance."""
    D1, Dh = np.diff(X, axis=0), X[H:] - X[:-H]
    v1 = np.nanvar(D1, axis=0, ddof=1)
    vh = np.nanvar(Dh, axis=0, ddof=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.log(np.clip(vh / (H * v1), 1e-9, None)), np.log(np.clip(v1, 1e-12, None))


def _cells(zbar, vol, z_edges, v_edges):
    qz = np.clip(np.searchsorted(z_edges, zbar, side="right") - 1, 0, N_CELLS - 1)
    qv = np.clip(np.searchsorted(v_edges, vol, side="right") - 1, 0, N_CELLS - 1)
    return qz * N_CELLS + qv


def _demean_in(c, cell):
    r = c.copy()
    for k in np.unique(cell):
        m = cell == k
        if m.sum() >= 3:
            r[m] = c[m] - np.nanmean(c[m])
    return r


def e4_stats(X_tr: np.ndarray, X_ext: np.ndarray, boot: int = 500,
             seed: int = 0) -> dict:
    """Pure E4 computation on aligned panels (columns = the SHARED entity
    set, identical order).  Returns the registered statistics."""
    T_tr = X_tr.shape[0]
    R_tr = pd.DataFrame(X_tr).rank(axis=1, ascending=False).to_numpy()
    zbar = np.log(np.nanmean(R_tr, axis=0))
    c_tr, vol = _curv(X_tr)
    z_edges = np.quantile(zbar, np.linspace(0, 1, N_CELLS + 1))
    v_edges = np.quantile(vol, np.linspace(0, 1, N_CELLS + 1))
    cell = _cells(zbar, vol, z_edges, v_edges)     # TRAIN-assigned, frozen

    # split-half signal share on TRAIN (EB shrinkage factor)
    half = T_tr // 2
    c1, _ = _curv(X_tr[:half])
    c2, _ = _curv(X_tr[half:])
    r1, r2 = _demean_in(c1, cell), _demean_in(c2, cell)
    ok12 = np.isfinite(r1) & np.isfinite(r2)
    cov12 = float(np.cov(r1[ok12], r2[ok12])[0, 1])
    sd1 = float(np.std(r1[ok12], ddof=1))
    sd2 = float(np.std(r2[ok12], ddof=1))
    rho_hat = float(np.clip(cov12 / max(sd1 * sd2, 1e-12), 0.0, 1.0))

    khat = rho_hat * _demean_in(c_tr, cell)
    c_ext, _ = _curv(X_ext)
    r_ext = _demean_in(c_ext, cell)                # train-assigned cells

    ok = np.isfinite(khat) & np.isfinite(r_ext)
    k, r = khat[ok], r_ext[ok]
    spear = float(pd.Series(k).corr(pd.Series(r), method="spearman"))
    q = np.quantile(k, [0.2, 0.4, 0.6, 0.8])
    extreme = (k <= q[0]) | (k > q[3])
    middle = (k > q[1]) & (k <= q[2])
    conc = float(np.mean(np.abs(r[extreme])) / max(np.mean(np.abs(r[middle])), 1e-12))

    rng = np.random.default_rng(seed)
    bs_s, bs_c = [], []
    n = k.size
    for _ in range(boot):
        i = rng.integers(0, n, n)
        kb, rb = k[i], r[i]
        bs_s.append(pd.Series(kb).corr(pd.Series(rb), method="spearman"))
        qb = np.quantile(kb, [0.2, 0.4, 0.6, 0.8])
        eb = (kb <= qb[0]) | (kb > qb[3])
        mb = (kb > qb[1]) & (kb <= qb[2])
        if mb.sum() and eb.sum():
            bs_c.append(np.mean(np.abs(rb[eb])) / max(np.mean(np.abs(rb[mb])), 1e-12))
    return dict(n=int(n), rho_hat=rho_hat, spearman=spear, concentration=conc,
                spearman_ci=tuple(np.percentile(bs_s, [2.5, 97.5])),
                concentration_ci=tuple(np.percentile(bs_c, [2.5, 97.5])),
                passes=bool(spear >= 0.20 and conc >= 1.3))


def _window_panel(df, lo, hi, score_k):
    """Scored complete-column value panel for periods lo..hi-1."""
    sub = df[(df["period"] >= lo) & (df["period"] < hi)]
    w = sub.pivot_table(index="period", columns="entity_id", values="X")
    rk = sub.pivot_table(index="period", columns="entity_id", values="rank")
    mean_rank = rk.mean(axis=0)
    keep = mean_rank.index[(mean_rank <= score_k)]
    w = w[keep]
    w = w.loc[:, w.notna().all(axis=0)]
    return w


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("platform")
    ap.add_argument("t0", type=int)
    ap.add_argument("--top-k", type=int, required=True)
    ap.add_argument("--boot", type=int, default=500)
    ap.add_argument("--ext-window", type=int, nargs=2, default=None,
                    metavar=("LO", "HI"),
                    help="EXPLORATORY subsample stability readout ONLY "
                         "(never confirmatory): score extension residuals on "
                         "periods [LO, HI) instead of [t0, T). Train stat "
                         "unchanged (periods < t0). Default = registered E4.")
    a = ap.parse_args()
    df = mrd.load_panel(mrd.PLATFORMS[a.platform])
    df = mrd.restrict_universe(df, a.top_k, buffer_mult=4)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    ext_lo, ext_hi = (a.t0, T) if a.ext_window is None else a.ext_window
    if a.ext_window is not None:
        print(f"EXPLORATORY ext-window [{ext_lo}, {ext_hi}) -- subsample "
              f"stability readout, NOT the registered E4 (which scored "
              f"[{a.t0}, {T}))")
    wtr = _window_panel(df, 0, a.t0, score_k)
    wex = _window_panel(df, ext_lo, ext_hi, score_k)
    shared = wtr.columns.intersection(wex.columns)
    print(f"train n={wtr.shape[1]:,}  ext n={wex.shape[1]:,}  "
          f"SHARED n={len(shared):,} (shared-survivor-conditioned, A5)")
    out = e4_stats(wtr[shared].to_numpy(), wex[shared].to_numpy(), boot=a.boot)
    print(f"rho_hat (train split-half signal share): {out['rho_hat']:.3f}")
    print(f"Spearman(khat_train, resid_ext) = {out['spearman']:.3f} "
          f"[{out['spearman_ci'][0]:.3f}, {out['spearman_ci'][1]:.3f}]  (gate >= 0.20)")
    print(f"concentration (Q1uQ5 / Q3)      = {out['concentration']:.3f} "
          f"[{out['concentration_ci'][0]:.3f}, {out['concentration_ci'][1]:.3f}]  (gate >= 1.3)")
    print(f"E4 VERDICT: {'PREDICTIVE (both thresholds met)' if out['passes'] else 'NOT predictive'}")


if __name__ == "__main__":
    main()
