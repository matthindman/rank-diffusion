#!/usr/bin/env python3
"""Discriminate two hypotheses for the within-band volatility dispersion:

  H-fine:   sigma is a (possibly steep) DETERMINISTIC function of permanent
            rank; coarse bands just failed to resolve it.  Conditional on
            exact rank there is no heterogeneity.
  H-temper: conditional on exact permanent rank, entities differ PERSISTENTLY
            in volatility (mixture with log-SD s ~ 0.9).

Four tests, all on bias-corrected per-entity log change-variances
e_i = log s_i^2 - E[log chi2_nu/nu] with sampling noise psi'(nu/2):

  A. Nested-band residual spread: s_hat as quantile bands shrink 1 -> ~2000.
     H-fine: s_hat -> 0.  H-temper: s_hat plateaus at s.
  B. Variogram: gamma(dr) = Var(e_(j) - e_(j+dr))/2 - noise, by permanent-rank
     separation dr.  Any smooth sigma(rank) gives gamma(1) ~ 0; a mixture gives
     gamma(dr) ~ s^2 flat from dr = 1.
  C. Does volatility travel with the ENTITY or the RANK?  Split-half residual
     correlation after conditioning each half's e on a fine k-NN rank curve of
     THAT half; movers (large between-half rank change) vs stayers.  H-fine:
     movers' correlation ~ 0 (they adopt the new rank's sigma).
  D. Predictive shootout for second-half variance: M1 = fine k-NN rank curve
     (fit on first half); M2 = entity's own EB-shrunken first-half variance.

Usage: python llm_fitting/temperament_vs_finebands.py [reddit] [facebook]
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import digamma, polygamma

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

UNIVERSE = {"reddit": 5000, "facebook": 3500}


def corrected_logvar(df, periods=None, min_changes=8):
    """Per-entity bias-corrected log change-variance e_i, its sampling noise
    trig_i = psi'(nu_i/2), and the entity's mean rank over the window."""
    if periods is not None:
        df = df[df["period"].isin(periods)]
    eid, u, same, _ = mrd._change_panel(df)
    ok = same & np.isfinite(u)
    sub = pd.DataFrame({"eid": eid[ok], "u": u[ok]})
    g = sub.groupby("eid")["u"]
    n_i, s2_i = g.count(), g.var(ddof=1)
    keep = n_i >= min_changes
    n_i, s2_i = n_i[keep], s2_i[keep]
    # Satterthwaite effective df from pooled lag-1/2 ACF
    rho = []
    var0 = np.nanvar(u[ok])
    for lag in (1, 2):
        idx = np.arange(len(u) - lag)
        v = np.ones(len(u) - lag, dtype=bool)
        for j in range(lag + 1):
            v &= same[idx + j]
        prod = u[idx] * u[idx + lag]
        v &= np.isfinite(prod)
        rho.append(float(np.mean(prod[v]) / var0) if v.any() else 0.0)
    kappa = 1.0 + 2.0 * sum(r * r for r in rho)
    nu = (n_i.to_numpy() - 1.0) / kappa
    e = np.log(np.clip(s2_i.to_numpy(), 1e-12, None)) - (digamma(nu / 2) - np.log(nu / 2))
    trig = polygamma(1, nu / 2)
    rbar = df[df["entity_id"].isin(s2_i.index)].groupby("entity_id")["rank"].mean()
    out = pd.DataFrame({"e": e, "trig": trig}, index=s2_i.index)
    out["rank"] = rbar.reindex(out.index)
    return out.dropna().sort_values("rank")


def knn_curve(rank_train, e_train, rank_eval, k=20):
    """k-NN (by permanent rank) local mean of e -- the 'fine sigma(rank) curve'."""
    order = np.argsort(rank_train)
    rt, et = rank_train[order], e_train[order]
    pos = np.searchsorted(rt, rank_eval)
    out = np.empty(len(rank_eval))
    h = k // 2
    for j, p in enumerate(pos):
        lo = max(0, min(p - h, len(rt) - k))
        out[j] = et[lo:lo + k].mean()
    return out


def run(platform):
    K = UNIVERSE[platform]
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[platform]), K, buffer_mult=4)
    T = int(df["period"].max()) + 1
    tab = corrected_logvar(df)
    n = len(tab)
    e, trig, rank = tab["e"].to_numpy(), tab["trig"].to_numpy(), tab["rank"].to_numpy()
    print(f"\n{'='*72}\n{platform.upper()} K={K}: fine-sigma(rank) vs temperament  "
          f"(n={n} entities, noise sd={np.sqrt(trig.mean()):.2f})\n{'='*72}")

    # ---- A. nested-band residual spread --------------------------------- #
    print("\n  A. residual spread s_hat vs band fineness (H-fine: -> 0)")
    for nb in [1, 5, 10, 50, 100, 500, 1000, 2000]:
        if nb > n // 3:
            continue
        qs = np.quantile(rank, np.linspace(0, 1, nb + 1))
        band = np.clip(np.searchsorted(qs, rank, side="right") - 1, 0, nb - 1)
        ehat = e - pd.Series(e).groupby(band).transform("mean").to_numpy()
        s2 = max(0.0, float(np.sum(ehat**2) / (n - nb) - trig.mean()))
        print(f"     {nb:>5} bands (~{n//nb:>5}/bin): s_hat = {np.sqrt(s2):.3f}")

    # ---- B. variogram by permanent-rank separation ---------------------- #
    print("\n  B. variogram s_local(dr) (H-fine: ~0 at dr=1; H-temper: flat ~ s)")
    for label, sel in [("all", np.ones(n, bool)), ("top-500", rank <= 500)]:
        es, ts = e[sel], trig[sel]
        line = []
        for dr in [1, 2, 5, 10, 50, 100]:
            if dr >= sel.sum():
                continue
            d = es[:-dr] - es[dr:]
            g = max(0.0, float(np.var(d) / 2 - ts.mean()))
            line.append(f"dr={dr}: {np.sqrt(g):.2f}")
        print(f"     [{label:>7}] " + "  ".join(line))

    # ---- C/D. split halves ---------------------------------------------- #
    h = T // 2
    t1 = corrected_logvar(df, periods=range(h))
    t2 = corrected_logvar(df, periods=range(h, T))
    both = t1.join(t2, lsuffix="_1", rsuffix="_2", how="inner").dropna()
    e1, e2 = both["e_1"].to_numpy(), both["e_2"].to_numpy()
    r1, r2 = both["rank_1"].to_numpy(), both["rank_2"].to_numpy()
    tr1, tr2 = both["trig_1"].to_numpy(), both["trig_2"].to_numpy()
    # residuals vs each half's OWN fine k-NN rank curve
    res1 = e1 - knn_curve(r1, e1, r1)
    res2 = e2 - knn_curve(r2, e2, r2)
    rel_move = np.abs(r2 - r1) / np.maximum(np.minimum(r1, r2), 1.0)
    movers = rel_move >= np.quantile(rel_move, 0.8)
    rho_all = float(np.corrcoef(res1, res2)[0, 1])
    rho_mov = float(np.corrcoef(res1[movers], res2[movers])[0, 1])
    rho_stay = float(np.corrcoef(res1[~movers], res2[~movers])[0, 1])
    s2_hat = max(0.0, float(np.var(res1) - tr1.mean()))
    rho_pred = s2_hat / np.sqrt((s2_hat + tr1.mean()) * (s2_hat + tr2.mean()))
    print(f"\n  C. split-half residual corr AFTER conditioning on each half's own"
          f"\n     fine rank curve (k-NN 20).  H-fine: ~0 (esp. movers)."
          f"\n     all: {rho_all:.3f}   stayers: {rho_stay:.3f}   movers(top 20% rank change): {rho_mov:.3f}"
          f"\n     (pure-temperament prediction: {rho_pred:.3f})")

    # D. predict e2: fine rank curve (fit on half 1) vs own shrunken e1
    m1 = knn_curve(r1, e1, r1)                       # sigma(rank) forecast
    shrink = s2_hat / (s2_hat + tr1)
    m2 = m1 + shrink * (e1 - m1)                     # EB temperament forecast
    mse1 = float(np.mean((e2 - m1) ** 2))
    mse2 = float(np.mean((e2 - m2) ** 2))
    floor = float(tr2.mean())                        # irreducible sampling noise
    print(f"\n  D. out-of-half prediction of entity variance (MSE, log scale; "
          f"noise floor {floor:.3f})"
          f"\n     M1 fine sigma(rank) curve : {mse1:.3f}"
          f"\n     M2 own shrunken variance  : {mse2:.3f}   "
          f"(explains {100*(mse1-mse2)/max(mse1-floor,1e-9):.0f}% of the explainable gap)")


if __name__ == "__main__":
    for plat in (sys.argv[1:] or ["reddit", "facebook"]):
        run(plat)
