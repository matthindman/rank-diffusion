#!/usr/bin/env python3
"""(f) 2026-07-05: paper figures 2-4 (review E3 figure plan), static PDF+PNG.

Fig 2  Identification, not just fit (comments K=12,500, head quintile):
       (A) change-autocovariance tail gamma_1..6, empirical vs fitted;
       (B) SSE(a) profile -- flat under gamma-only md6, V-shaped with D(h);
       (C) D(h) curves incl. h=26,52, empirical vs fitted.
Fig 3  Measurement noise from daily replication (all three platforms):
       Spec-A weekly-covariance curve vs the CENTERED Spec-B floor (pinned)
       vs the superseded legacy floor; shaded = the bounded region
       [centered floor, Spec-A]; FB head annotated as identified.
Fig 4  Entity-amplitude collapse:
       (A) standardized corrected log change-variance densities vs N(0,1)
           (the lognormal-temperament collapse across metrics);
       (B) s(h) flatness (the b~1 instrument);
       (C) b forest plot -- all 2u variants vs b=1.

Colors validated per the dataviz procedure (light surface #fcfcfb):
#2a78d6 empirical/data, #e34948 fitted/model+legacy, #1baf7a bounds,
#eda100 third series; direct labels everywhere (contrast relief).
Output: figures/fig{2,3,4}_*.{pdf,png}
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd            # noqa: E402
import spec_b_sigma_obs as sb             # noqa: E402
import weighting_robustness as wr         # noqa: E402

FIGDIR = Path(__file__).resolve().parent.parent / "figures"
FIGDIR.mkdir(exist_ok=True)
C_DATA, C_FIT, C_BOUND, C_ALT = "#2a78d6", "#e34948", "#1baf7a", "#eda100"
GRAY = "#52514e"

plt.rcParams.update({
    "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb",
    "axes.edgecolor": GRAY, "axes.linewidth": 0.8,
    "axes.grid": True, "grid.alpha": 0.25, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 9.5, "axes.titlesize": 10, "axes.labelsize": 9.5,
    "lines.linewidth": 1.8, "legend.frameon": False,
})


def _save(fig, name):
    for ext in ("pdf", "png"):
        fig.savefig(FIGDIR / f"{name}.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote figures/{name}.pdf/.png")


def _sse_profile(gk, d_mom, d_h):
    """min-over-phi SSE at each grid a (the 2i identification picture)."""
    L = len(gk) - 1
    y = np.concatenate([gk, d_mom]) if len(d_mom) else gk
    out = {}
    for a in mrd.A_GRID_VR:
        best = np.inf
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
            coef, *_ = np.linalg.lstsq(X, y, rcond=None)
            coef = np.clip(coef, 0.0, None)
            best = min(best, float(np.sum((y - X @ coef) ** 2)))
        out[1.0 - a] = best
    return out


def _model_curves(fit, d_h_all, L=6):
    a, phi = 1.0 - fit["kappa"], fit["phi"]
    W = fit["sigma_eta"] ** 2 / (1 - a ** 2) if a < 1 else 0.0
    V = fit["sigma_nu"] ** 2 / (1 - phi ** 2) if phi < 1 else 0.0
    s_e = fit["sigma_e"] ** 2
    A, B = W * (1 - a) ** 2, V * (1 - phi) ** 2
    g = [2 * W * (1 - a) + 2 * V * (1 - phi) + 2 * s_e, -A - B - s_e]
    g += [-A * a ** (k - 1) - B * phi ** (k - 1) for k in range(2, L + 1)]
    D = [2 * W * (1 - a ** h) + 2 * V * (1 - phi ** h) + 2 * s_e for h in d_h_all]
    return np.array(g), np.array(D)


def fig2_identification():
    print("Fig 2: identification (comments head quintile) ...")
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS["reddit_comments"]),
                               12500, buffer_mult=4)
    U, Xf, zbar = wr.panel_matrices(df)
    fin = np.isfinite(zbar)
    head = np.where(fin & (zbar <= np.quantile(zbar[fin], 0.2)))[0]
    d_h = (2, 4, 8, 13, 26, 52)
    gk, dm = wr.moments(U, Xf, head, d_h)
    fit = wr.md_fit(gk, dm, d_h, None)
    g_fit, D_fit = _model_curves(fit, (1,) + d_h)
    dm_all = np.concatenate([[np.nanvar(np.diff(Xf[:, head], axis=0), ddof=1)], dm])

    fig, ax = plt.subplots(1, 3, figsize=(10.5, 3.2), constrained_layout=True)
    ks = np.arange(1, 7)
    ax[0].axhline(0, color=GRAY, lw=0.8)
    ax[0].plot(ks, gk[1:], "o-", color=C_DATA, ms=5)
    ax[0].plot(ks, g_fit[1:], "s--", color=C_FIT, ms=4)
    ax[0].text(ks[-1], gk[-1] - 3e-4, "empirical", color=C_DATA, ha="right", va="top")
    ax[0].text(ks[-1], g_fit[-1] + 3e-4, "fitted (OU + AR + noise)",
               color=C_FIT, ha="right", va="bottom")
    ax[0].set(xlabel="lag k (weeks)", ylabel=r"$\gamma_k$ (change autocov.)",
              title="A  The negative tail identifies structure")

    prof_g = _sse_profile(gk, np.array([]), ())
    prof_d = _sse_profile(gk, dm, d_h)
    for prof, c, lbl in ((prof_g, C_ALT, r"$\gamma_{0..6}$ only (flat)"),
                         (prof_d, C_DATA, r"$+\ D(h),\ h\leq 52$ (V-shaped)")):
        kx = np.array(sorted(prof))
        vy = np.array([prof[k] for k in kx])
        ax[1].plot(kx, vy / vy.min(), "o-", color=c, ms=4)
        ax[1].text(kx[-1], (vy / vy.min())[-1], " " + lbl, color=c,
                   ha="left", va="center", fontsize=8.5)
    ax[1].set(xlabel=r"home reversion $\kappa = 1-a$", ylabel="SSE / min SSE",
              yscale="log", title=r"B  $\kappa$ is unidentified without $D(h)$",
              xlim=(-0.005, 0.42))

    hs = np.array((1,) + d_h)
    ax[2].plot(hs, dm_all, "o-", color=C_DATA, ms=5)
    ax[2].plot(hs, D_fit, "s--", color=C_FIT, ms=4)
    ax[2].text(hs[-1], dm_all[-1] * 0.80, "empirical", color=C_DATA,
               ha="right", va="top")
    ax[2].text(hs[-1], D_fit[-1] * 0.62, "fitted", color=C_FIT,
               ha="right", va="top")
    ax[2].set(xlabel="horizon h (weeks)", ylabel=r"$D(h)=\mathrm{Var}(X_{t+h}-X_t)$",
              xscale="log", title="C  Long-horizon variance curvature")
    ax[2].set_xticks(hs, [str(h) for h in hs])
    fig.suptitle("Identification: reddit comments (census, T=136), head quintile",
                 fontsize=10.5)
    _save(fig, "fig2_identification")


def fig3_sigma_obs():
    print("Fig 3: sigma_obs two-instrument curves ...")
    panels = [("reddit", 5000, "Reddit submissions"),
              ("facebook_a", 3500, "Facebook Era A (tracked)"),
              ("reddit_comments", 12500, "Reddit comments")]
    fig, axes = plt.subplots(1, 3, figsize=(10.5, 3.4), constrained_layout=True)
    for ax, (plat, K, title) in zip(axes, panels):
        cfg = mrd.PLATFORMS[plat]
        uni = mrd.restrict_universe(mrd.load_panel(cfg), K, buffer_mult=4)
        daily = sb.load_daily(set(uni["entity_id"].unique()),
                              path=cfg.get("daily_path", sb.DAILY_PATH),
                              day_guard=cfg.get("day_guard", False))
        cur = sb.spec_b_curve(uni, daily)                  # centered (pinned)
        t = cur["table"]
        pA = mrd.estimate(uni, temper=True, min_knot_n=8, md_lags=6)
        specA = np.interp(cur["z"], pA.z_knots, pA.sigma_obs)
        r = t["rank"].to_numpy()
        ax.fill_between(r, t["sigma_obsB_cent"], specA, color=C_BOUND, alpha=0.18,
                        lw=0)
        ax.plot(r, specA, "o-", color=C_DATA, ms=4)
        ax.plot(r, t["sigma_obsB_cent"], "s-", color=C_BOUND, ms=4)
        ax.plot(r, t["sigma_obsB"], "--", color=C_FIT, lw=1.2, alpha=0.8)
        ax.set(xscale="log", xlabel="rank (permanent)", title=title,
               ylim=(0, None))
        if ax is axes[0]:
            ax.set_ylabel(r"weekly $\sigma_{obs}$ (log units)")
            ax.text(r[2], specA[2] * 1.12, "Spec-A (weekly cov.)", color=C_DATA,
                    fontsize=8.5)
            ax.text(r[4], t["sigma_obsB_cent"].iloc[4] * 0.72,
                    "Spec-B centered floor (pinned)", color=C_BOUND, fontsize=8.5)
            ax.text(r[6], t["sigma_obsB"].iloc[6] * 1.18,
                    "legacy floor (superseded)", color=C_FIT, fontsize=8.5)
        if plat == "facebook_a":
            ax.annotate("head: identified\n(two instruments, ~12%)",
                        xy=(r[0], specA[0]), xytext=(r[0] * 3.2, specA[0] * 0.45),
                        fontsize=8, color=GRAY,
                        arrowprops=dict(arrowstyle="->", color=GRAY, lw=0.8))
    fig.suptitle("Measurement noise from daily replication — shaded = bounded "
                 "region [centered floor, Spec-A]", fontsize=10.5)
    _save(fig, "fig3_sigma_obs")


def fig4_amplitude():
    print("Fig 4: amplitude collapse ...")
    fig, ax = plt.subplots(1, 3, figsize=(10.5, 3.3), constrained_layout=True)
    plats = [("reddit_comments", 12500, "comments", C_DATA),
             ("facebook_a", 3500, "FB Era A", C_ALT),
             ("reddit", 5000, "submissions", C_BOUND)]
    dfs = {}
    # (A) standardized corrected log-variance collapse
    for plat, K, lbl, c in plats:
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[plat]), K,
                                   buffer_mult=4)
        dfs[plat] = df
        t = mrd.estimate_temperament(df)
        e = t["entities"]["e_hat"].to_numpy()
        sd = np.sqrt(t["s"] ** 2 + t["entities"]["trig"].mean())
        gx = np.linspace(-4, 4, 120)
        kde = np.histogram(e / sd, bins=60, range=(-4, 4), density=True)
        ctr = 0.5 * (kde[1][1:] + kde[1][:-1])
        ax[0].plot(ctr, kde[0], color=c, lw=1.6)
        ax[0].text(2.05, np.interp(1.35, ctr, kde[0]) + {"comments": 0.048,
                   "FB Era A": 0.024, "submissions": 0.0}[lbl],
                   f"{lbl} (s={t['s']:.2f})", color=c, fontsize=8.5)
    gx = np.linspace(-4, 4, 200)
    ax[0].plot(gx, np.exp(-gx ** 2 / 2) / np.sqrt(2 * np.pi), "--", color=GRAY,
               lw=1.4)
    ax[0].text(-3.7, 0.36, "N(0,1)", color=GRAY, fontsize=8.5)
    ax[0].set(xlabel="standardized corrected log change-variance",
              ylabel="density", title="A  One lognormal amplitude, all metrics")

    # (B) s(h) flatness
    for plat, K, lbl, c in plats:
        df, hs, ss = dfs[plat], [], []
        T = int(df["period"].max()) + 1
        for h in (1, 2, 4, 8, 13):
            if T // h < 9:
                continue
            sub = df if h == 1 else df[df["period"] % h == 0].copy()
            if h > 1:
                sub["period"] //= h
            s = mrd.estimate_temperament(sub, min_changes=8)["s"]
            if s > 0:
                hs.append(h); ss.append(s)
        ax[1].plot(hs, ss, "o-", color=c, ms=4)
        ax[1].text(hs[-1] * 1.1, ss[-1], lbl, color=c, fontsize=8.5, va="center")
    ax[1].set(xlabel="horizon h (weeks)", ylabel=r"temperament dispersion $s(h)$",
              xscale="log", ylim=(0, 1.15), title=r"B  $s(h)$ flat $\Rightarrow$ one amplitude, $b\approx 1$")
    ax[1].set_xticks([1, 2, 4, 8, 13], ["1", "2", "4", "8", "13"])

    # (C) b forest plot -- values from MODEL_STATUS 2n/2u (this session's runs)
    rows = [
        ("FB entity boot (2n)", 1.024, 1.008, 1.040, C_ALT),
        ("FB block boot (2u)", 0.983, 0.939, 1.064, C_ALT),
        ("FB detrended", 1.024, None, None, C_ALT),
        ("comments entity boot (2n)", 1.079, 1.065, 1.093, C_DATA),
        ("comments block boot (2u)", 1.005, 0.958, 1.108, C_DATA),
        ("comments detrended", 1.059, None, None, C_DATA),
        ("comments h=4", 1.002, None, None, C_DATA),
        ("comments 2nd half", 0.994, None, None, C_DATA),
    ]
    ax[2].axvline(1.0, color=GRAY, lw=1.2, ls="--")
    for i, (lbl, b, lo, hi, c) in enumerate(rows):
        y = len(rows) - i
        if lo is not None:
            ax[2].plot([lo, hi], [y, y], color=c, lw=1.8)
        ax[2].plot([b], [y], "o", color=c, ms=5)
        ax[2].text(0.855, y, lbl, ha="right", va="center", fontsize=8.2)
    ax[2].set(xlim=(0.62, 1.16), ylim=(0.3, len(rows) + 0.7),
              xlabel=r"mix exponent $b$",
              title=r"C  $b=1$ inside every time-aware CI")
    ax[2].set_yticks([])
    ax[2].text(1.005, 0.52, "b = 1", color=GRAY, fontsize=8.5)
    fig.suptitle("Entity-amplitude collapse and the b = 1 factorization",
                 fontsize=10.5)
    _save(fig, "fig4_amplitude")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "2"):
        fig2_identification()
    if which in ("all", "3"):
        fig3_sigma_obs()
    if which in ("all", "4"):
        fig4_amplitude()
