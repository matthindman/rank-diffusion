#!/usr/bin/env python3
"""P5 (2026-07-05, review A10/D1/D3): interpretation uniqueness of the 2p
"directional lifecycle arcs" reading.

(a) PHASE-RANDOMIZED SURROGATES.  Multivariate FFT surrogate of the per-entity
INCREMENT panel (one common random phase rotation across entities per draw):
preserves every entity's increment amplitude spectrum AND all cross-spectra
(so the common factor and any stationary long-memory structure survive),
destroys only phase alignment -- exactly the directional/arc structure 2p
inferred.  Gaussian marginals are a declared property of the surrogate class.
Statistics (identical functionals on data and surrogates, complete-column
scored population):
  - demeaning ratio rho(h) = sum_i Var_t(dX_h) / sum_i E_t[dX_h^2]  (share of
    h-horizon movement surviving per-entity windowed demeaning; 2p: data
    loses ~50% at h=52, fitted model only ~28%)
  - scored VR_sc(h) = median_i Var_t(dX_h)/(h Var_t(dX_1))
  - population-internal RACF13 (ranks recomputed within the population --
    declared: differs from the full-universe scorecard RACF13)
Decision rule (pre-registered in the handoff): if the empirical demeaning
ratio at h=52 AND RACF13 sit inside the surrogate band, a stationary Gaussian
process with the empirical spectrum reproduces the 2p evidence -> soften
"lifecycle arcs" to "excess low-frequency structure"; if outside, the
directional-arc interpretation strengthens.

(b) PER-ENTITY kappa_i HETEROGENEITY PROBE (fine-bands' missing axis --
reversion, not volatility).  Per-entity curvature c_i = log VR_i(13),
demeaned within (rank x volatility) quantile cells; split-half stability of
the residual (Spearman), and the noise-corrected true dispersion via
Cov(first-half, second-half) of the cell-demeaned curvature.

Usage: python llm_fitting/surrogate_test.py [platform] [K] [n_surr]
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402


def complete_panel(df: pd.DataFrame, score_k: int | None):
    """(T x n) level matrix of the complete-column scored population."""
    T = int(df["period"].max()) + 1
    pres = df.groupby("entity_id")["period"].nunique()
    elig = pres[pres == T].index
    sub = df[df["entity_id"].isin(elig)]
    if score_k is not None:
        mr = sub.groupby("entity_id")["rank"].mean()
        elig = mr[mr <= score_k].index
        sub = sub[sub["entity_id"].isin(elig)]
    X = sub.pivot(index="period", columns="entity_id", values="X") \
           .reindex(range(T)).to_numpy(dtype=float)
    return X


def demean_ratio(X: np.ndarray, h: int) -> float:
    D = X[h:] - X[:-h]
    num = np.nansum(np.nanvar(D, axis=0, ddof=1) * (D.shape[0] - 1))
    den = np.nansum(D ** 2)
    return float(num / den)


def scored_vr(X: np.ndarray, h: int) -> float:
    D1 = np.diff(X, axis=0)
    Dh = X[h:] - X[:-h]
    v1 = np.nanvar(D1, axis=0, ddof=1)
    vh = np.nanvar(Dh, axis=0, ddof=1)
    m = (v1 > 1e-12) & np.isfinite(vh)
    return float(np.nanmedian(vh[m] / (h * v1[m])))


def internal_racf(X: np.ndarray, lag: int) -> float:
    R = pd.DataFrame(X).rank(axis=1, ascending=False).to_numpy()
    vals = [mrd._safe_acf(R[:, i], lag) for i in range(R.shape[1])]
    vals = [v for v in vals if np.isfinite(v)]
    return float(np.median(vals))


def surrogate(X: np.ndarray, rng) -> np.ndarray:
    """Multivariate phase-randomized surrogate: common phase rotation on the
    increment panel, then re-integrate from the empirical starting levels."""
    dX = np.diff(X, axis=0)
    n = dX.shape[0]
    F = np.fft.rfft(dX, axis=0)
    ph = rng.uniform(0, 2 * np.pi, size=F.shape[0])
    ph[0] = 0.0                      # keep each entity's mean increment (drift)
    if n % 2 == 0:
        ph[-1] = 0.0                 # Nyquist must stay real
    Fs = F * np.exp(1j * ph)[:, None]
    dXs = np.fft.irfft(Fs, n=n, axis=0)
    return np.vstack([X[0], X[0] + np.cumsum(dXs, axis=0)])


def stats(X: np.ndarray, hs=(13, 26, 52)) -> dict:
    out = {}
    T = X.shape[0]
    for h in hs:
        if h < T - 2:
            out[f"rho{h}"] = demean_ratio(X, h)
            out[f"VRsc{h}"] = scored_vr(X, h)
    out["RACF13"] = internal_racf(X, 13)
    out["RACF1"] = internal_racf(X, 1)
    return out


def kappa_probe(X: np.ndarray, n_cells: int = 5) -> None:
    """(b): conditioned split-half stability of per-entity VR13 curvature."""
    T, n = X.shape
    half = T // 2
    R = pd.DataFrame(X).rank(axis=1, ascending=False).to_numpy()

    def curv(Xw):
        D1, Dh = np.diff(Xw, axis=0), Xw[13:] - Xw[:-13]
        v1 = np.nanvar(D1, axis=0, ddof=1)
        vh = np.nanvar(Dh, axis=0, ddof=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.log(np.clip(vh / (13 * v1), 1e-9, None)), np.log(v1)

    c1, vol1 = curv(X[:half])
    c2, vol2 = curv(X[half:])
    zbar = np.log(np.nanmean(R, axis=0))
    vol = 0.5 * (vol1 + vol2)

    # cell-demean within (rank x volatility) quantile cells
    def cell_resid(c):
        qz = np.clip(np.searchsorted(np.quantile(zbar, np.linspace(0, 1, n_cells + 1)),
                                     zbar, side="right") - 1, 0, n_cells - 1)
        qv = np.clip(np.searchsorted(np.quantile(vol, np.linspace(0, 1, n_cells + 1)),
                                     vol, side="right") - 1, 0, n_cells - 1)
        r = c.copy()
        for a in range(n_cells):
            for b in range(n_cells):
                m = (qz == a) & (qv == b)
                if m.sum() >= 3:
                    r[m] = c[m] - np.nanmean(c[m])
        return r

    r1, r2 = cell_resid(c1), cell_resid(c2)
    ok = np.isfinite(r1) & np.isfinite(r2)
    rho_s = pd.Series(r1[ok]).corr(pd.Series(r2[ok]), method="spearman")
    cov12 = float(np.cov(r1[ok], r2[ok])[0, 1])       # true-signal variance
    sd1, sd2 = np.std(r1[ok], ddof=1), np.std(r2[ok], ddof=1)
    true_sd = np.sqrt(max(cov12, 0.0))
    print(f"\n(b) kappa_i heterogeneity probe (n={ok.sum()} entities, "
          f"{n_cells}x{n_cells} rank-x-vol cells):")
    print(f"    split-half Spearman of conditioned log VR13 curvature: {rho_s:.3f}")
    print(f"    half SDs {sd1:.3f}/{sd2:.3f}; noise-corrected TRUE SD "
          f"= sqrt(cov12) = {true_sd:.3f} "
          f"({100 * true_sd ** 2 / max(sd1 * sd2, 1e-12):.0f}% of observed variance is signal)")


def model_complete_panel(p, T, score_k, seed):
    """Complete-column tracked panel from a FITTED-model simulation, built
    with the same population filter as complete_panel (mean rank <= score_k,
    complete columns)."""
    sim = mrd.simulate(p, T, seed=seed, top_record=score_k)
    tv, tr, _, _ = mrd._sim_struct(sim)
    with np.errstate(invalid="ignore"):
        rf = np.where(tr > 0, tr.astype(float), np.nan)
        mean_rank = np.nanmean(rf, axis=0)
    keep = np.isfinite(mean_rank)
    if score_k is not None:
        keep &= mean_rank <= score_k
    X = tv[:, keep]
    return X[:, np.all(np.isfinite(X), axis=0)]


def model_side(df, score_k, T, n_surr, n_seeds=3):
    """(c) 2026-07-05: phase-randomize FITTED-model paths.  The model's own
    functional gap Delta_model = VR_sc(surrogate of model) - VR_sc(model)
    measures how much of the data-side gap (+0.04, MODEL_STATUS 2v) the
    existing t-tails/non-Gaussian machinery already produces.  Long-stack
    estimate (the comments structure-primary spec, 2s)."""
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
                     md_vr_long=True, stat_factor=True, two_scale=True,
                     mix_hetero=True)
    rng = np.random.default_rng(1)
    rows_pt, rows_sur = [], []
    for seed in range(n_seeds):
        Xm = model_complete_panel(p, T, score_k, seed)
        rows_pt.append(stats(Xm))
        per = max(2, n_surr // n_seeds)
        rows_sur += [stats(surrogate(Xm, rng)) for _ in range(per)]
        print(f"    model seed {seed}: n={Xm.shape[1]} complete columns, "
              f"{per} surrogates")
    Pt, Su = pd.DataFrame(rows_pt), pd.DataFrame(rows_sur)
    print(f"\n(c) {'stat':<10}{'model':>10}{'model-surr mean':>16}"
          f"{'surr 2.5-97.5%':>22}{'Delta_model':>12}")
    for k in Pt.columns:
        lo, hi = np.percentile(Su[k], [2.5, 97.5])
        print(f"    {k:<10}{Pt[k].mean():>10.3f}{Su[k].mean():>16.3f}"
              f"    [{lo:7.3f},{hi:7.3f}]{Su[k].mean() - Pt[k].mean():>+12.3f}")
    return Pt, Su


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    platform = args[0] if len(args) > 0 else "reddit_comments"
    K = int(args[1]) if len(args) > 1 else 12500
    n_surr = int(args[2]) if len(args) > 2 else 50
    do_model = "--model" in sys.argv
    cfg = mrd.PLATFORMS[platform]
    df = mrd.restrict_universe(mrd.load_panel(cfg), K, buffer_mult=4)
    score_k = df.attrs.get("score_k")
    T = int(df["period"].max()) + 1
    X = complete_panel(df, score_k)
    print(f"=== surrogate test: {platform} K={K}  complete-column population "
          f"n={X.shape[1]}, T={X.shape[0]}, {n_surr} surrogates ===")

    emp = stats(X)
    rng = np.random.default_rng(0)
    S = pd.DataFrame([stats(surrogate(X, rng)) for _ in range(n_surr)])
    print(f"\n(a) {'stat':<10}{'empirical':>10}{'surrogate mean':>15}"
          f"{'surr 2.5-97.5%':>22}{'emp inside?':>12}")
    for k, v in emp.items():
        lo, hi = np.percentile(S[k], [2.5, 97.5])
        inside = lo <= v <= hi
        print(f"    {k:<10}{v:>10.3f}{S[k].mean():>15.3f}"
              f"    [{lo:7.3f},{hi:7.3f}]{str(inside):>12}")

    kappa_probe(X)

    if do_model:
        print("\n--- model-side surrogates (fitted long stack) ---")
        Pt, Su = model_side(df, score_k, T, n_surr)
        d_data = S["VRsc13"].mean() - emp["VRsc13"]
        d_model = Su["VRsc13"].mean() - Pt["VRsc13"].mean()
        print(f"\n    functional gap at VR_sc(13): data {d_data:+.3f} "
              f"vs model {d_model:+.3f} -> the model's non-Gaussian machinery "
              f"produces {100 * d_model / d_data if d_data else np.nan:.0f}% "
              f"of the data-side gap")


if __name__ == "__main__":
    main()
