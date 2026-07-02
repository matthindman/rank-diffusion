"""
Tail estimation analysis for Facebook weekly activity data.
v5: Polynomial-in-log-alpha framework.

Four nested estimates of unobserved tail mass, each corresponding to
a polynomial of increasing degree in log α(x):

  Degree 0 — Power law:      α = α₀                (upper bound)
  Degree 1 — Constant-η:     log α = η₀·u           (tighter bound)
  Degree 2 — Linear-η:       log α = η₀·u + ½η₁·u²  (best guess)
  Degree 3 — Quadratic-η:    log α = η₀·u + ½η₁·u² + ⅙η₂·u³

Ordering within the fitted family: PL tail >= deg-1 tail >= deg-2 tail >= deg-3 tail.

η₀ >= 0 follows from concavity of the log-log curve (the slope steepens).
η₁, η₂ >= 0 are additional modeling choices — motivated by empirical
observation in fully-observed systems (Reddit, equities) but not implied
by concavity alone.  They preserve the nested ordering and encode the
assumption that the tail steepens at least as fast as the observed rate.
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.integrate import quad
from dataclasses import dataclass
from pathlib import Path

_LOG_EXP_CLIP_LOW = -745.0
_LOG_EXP_CLIP_HIGH = 700.0


# ── Kernel utilities ──────────────────────────────────────────────────────

def _tricube(u):
    u = np.asarray(u, dtype=float)
    w = np.clip(1 - np.abs(u) ** 3, 0.0, None) ** 3
    w[np.abs(u) >= 1.0] = 0.0
    return w


# ── Boundary fitting ─────────────────────────────────────────────────────

def anchor_y0(rank, views, r0, half_width=30):
    """Observed y0 at the boundary via local-linear smoother on log-views."""
    log_r = np.log(rank.astype(float))
    log_v = np.log(views.astype(float))
    target = np.log(float(r0))

    mask = np.abs(rank - r0) <= half_width
    if mask.sum() < 5:
        mask = np.abs(rank - r0) <= 100
    if mask.sum() < 1:
        raise ValueError(f"No data near rank {r0}")

    dx = log_r[mask] - target
    z = log_v[mask]
    dist = np.abs(dx)
    max_dist = max(dist.max(), 1e-10)
    w = _tricube(dist / max_dist)
    w = np.maximum(w, 0.01)

    X = np.column_stack([np.ones_like(dx), dx])
    sw = np.sqrt(w)
    beta, *_ = np.linalg.lstsq(X * sw[:, None], z * sw, rcond=None)
    return float(np.exp(beta[0])), float(beta[0])


def _fit_eta_at(log_rank, log_views, log_r0, n_fit):
    """Fit a single local quadratic at log_r0 and return (alpha, eta)."""
    keep = log_rank <= log_r0 + 1e-12
    x = log_rank[keep]
    z = log_views[keep]
    if len(x) < 10:
        raise ValueError("Fewer than 10 observations below boundary.")
    start = max(0, len(x) - n_fit)
    x, z = x[start:], z[start:]
    dx = x - log_r0
    window = abs(dx.min())
    if window < 1e-8:
        raise ValueError("Fitting window is degenerate.")
    w = _tricube(dx / window)
    X = np.column_stack([np.ones_like(dx), dx, 0.5 * dx ** 2])
    sw = np.sqrt(w)
    beta, *_ = np.linalg.lstsq(X * sw[:, None], z * sw, rcond=None)
    alpha = max(-beta[1], 0.1)
    eta = max(-beta[2], 0.0) / alpha
    return alpha, eta, window, len(x)


def estimate_log_alpha_poly(log_rank, log_views, log_r0, n_fit=1000, delta=0.25):
    """
    Estimate the polynomial coefficients in log α using staggered quadratics.

    Fits local quadratics at x0, x0-δ, and x0-2δ to get η at three points.
    Then uses standard backward finite-difference formulas:

      η₀ = η(x0)
      η₁ = (3η(x0) - 4η(x0-δ) + η(x0-2δ)) / (2δ)     [second-order accurate]
      η₂ = (η(x0) - 2η(x0-δ) + η(x0-2δ)) / δ²

    The three-point formula for η₁ is exact for quadratic η(u) and avoids
    the O(δ) bias of a simple backward difference.

    Returns alpha0, eta0, eta1, eta2, window, n_used.
    """
    alpha0, eta_0, window, n_used = _fit_eta_at(log_rank, log_views, log_r0, n_fit)

    # Shifted fits for η₁ and η₂
    eta_vals = [eta_0]
    for k in range(1, 3):
        shifted = log_r0 - k * delta
        try:
            _, eta_k, _, _ = _fit_eta_at(log_rank, log_views, shifted, n_fit)
            eta_vals.append(eta_k)
        except Exception:
            eta_vals.append(eta_0)  # fallback: assume flat

    # Three-point backward finite differences
    eta1 = (3 * eta_vals[0] - 4 * eta_vals[1] + eta_vals[2]) / (2 * delta)
    eta2 = (eta_vals[0] - 2 * eta_vals[1] + eta_vals[2]) / (delta ** 2)

    # Enforce non-negative (tail steepens, doesn't flatten)
    eta1 = max(eta1, 0.0)
    eta2 = max(eta2, 0.0)

    return alpha0, eta_0, eta1, eta2, window, n_used


# ── General polynomial-in-log-alpha model ────────────────────────────────

def _exp_from_log(log_value):
    """Exponentiate a scalar log-value without emitting overflow warnings."""
    return float(np.exp(np.clip(log_value, _LOG_EXP_CLIP_LOW, _LOG_EXP_CLIP_HIGH)))


def _log_alpha_poly(s, eta_coeffs):
    """
    Evaluate the polynomial in log α at u=s:
      P(s) = η₀·s + ½η₁·s² + ⅙η₂·s³ + ...

    eta_coeffs = [η₀, η₁, η₂, ...]
    """
    val = 0.0
    factorial = 1.0
    for k, c in enumerate(eta_coeffs):
        factorial *= (k + 1)
        val += c * s ** (k + 1) / factorial
    return val


def _integrated_alpha(u_val, alpha0, eta_coeffs):
    """Signed integral ∫₀ᵘ α₀·exp(P(s)) ds where u may be positive or negative."""
    if abs(u_val) <= 1e-15:
        return 0.0

    def integrand(s):
        return alpha0 * _exp_from_log(_log_alpha_poly(s, eta_coeffs))

    if u_val > 0:
        val, _ = quad(integrand, 0, u_val, limit=200)
        return val

    val, _ = quad(integrand, u_val, 0, limit=200)
    return -val


def _alpha_at(u_val, alpha0, eta_coeffs):
    """Local Pareto exponent α(u) = α₀ exp(P(u))."""
    return alpha0 * _exp_from_log(_log_alpha_poly(u_val, eta_coeffs))


def _tail_remainder_bound(u_val, alpha0, eta_coeffs):
    """
    Upper bound on the remaining dimensionless tail integral beyond u_val.

    Since α(u) is non-decreasing when η-coefficients are non-negative,
    the log-integrand slope is at most -(α(u_val) - 1) for all larger u.
    """
    alpha_u = _alpha_at(u_val, alpha0, eta_coeffs)
    if alpha_u <= 1.0 + 1e-12:
        return np.inf

    F = _integrated_alpha(u_val, alpha0, eta_coeffs)
    log_integrand = u_val - F
    if log_integrand <= _LOG_EXP_CLIP_LOW:
        return 0.0

    return _exp_from_log(log_integrand) / (alpha_u - 1.0)


def predict_poly(r, y0, r0, alpha0, eta_coeffs):
    """
    Predict y(r) under polynomial-in-log-alpha model.

    log y(u) = log y0 - ∫₀ᵘ α₀·exp(P(s)) ds
    where u = log(r/r0) and P(s) = η₀s + ½η₁s² + ⅙η₂s³ + ...
    """
    r_arr = np.asarray(r, dtype=float)
    u_arr = np.log(r_arr / float(r0))
    result = np.empty_like(u_arr)

    for i, u in enumerate(u_arr):
        if abs(u) < 1e-12:
            result[i] = y0
        else:
            F = _integrated_alpha(u, alpha0, eta_coeffs)
            result[i] = y0 * _exp_from_log(-F)

    return result


def tail_mass_poly(y0, r0, alpha0, eta_coeffs):
    """
    Tail mass under polynomial-in-log-alpha model.

    = y0·r0 · ∫₀^∞ exp(u - ∫₀ᵘ α₀·exp(P(s)) ds) du

    For degree 0 (power law), uses analytic formula.
    Otherwise numerical integration.
    """
    # Check if effectively power law (all coefficients near zero)
    if all(abs(c) < 1e-12 for c in eta_coeffs):
        if alpha0 <= 1.0:
            return np.inf
        return float(y0) * float(r0) / (float(alpha0) - 1.0)

    def integrand(u):
        F = _integrated_alpha(u, alpha0, eta_coeffs)
        log_integrand = u - F
        if log_integrand <= _LOG_EXP_CLIP_LOW:
            return 0.0
        return _exp_from_log(log_integrand)

    total = 0.0
    u_left = 0.0
    step = 30.0
    remainder_tol = 1e-10

    for _ in range(12):
        u_right = u_left + step
        piece, _ = quad(integrand, u_left, u_right, limit=300)
        total += piece

        if _tail_remainder_bound(u_right, alpha0, eta_coeffs) <= remainder_tol:
            return float(y0) * float(r0) * total

        u_left = u_right
        step *= 2.0

    return float(y0) * float(r0) * total


# ── Convenience wrappers for the four standard models ────────────────────

def predict_power_law(r, y0, r0, alpha0):
    u = np.log(np.asarray(r, dtype=float) / float(r0))
    return y0 * np.exp(-alpha0 * u)


def predict_deg1(r, y0, r0, alpha0, eta0):
    if abs(eta0) < 1e-12:
        return predict_power_law(r, y0, r0, alpha0)
    u = np.log(np.asarray(r, dtype=float) / float(r0))
    return y0 * np.exp(-(alpha0 / eta0) * np.expm1(eta0 * u))


def predict_deg2(r, y0, r0, alpha0, eta0, eta1):
    return predict_poly(r, y0, r0, alpha0, [eta0, eta1])


def predict_deg3(r, y0, r0, alpha0, eta0, eta1, eta2):
    return predict_poly(r, y0, r0, alpha0, [eta0, eta1, eta2])


def tail_mass_pl(y0, r0, alpha0):
    if alpha0 <= 1.0:
        return np.inf
    return float(y0) * float(r0) / (float(alpha0) - 1.0)


def tail_mass_deg1(y0, r0, alpha0, eta0):
    return tail_mass_poly(y0, r0, alpha0, [eta0])


def tail_mass_deg2(y0, r0, alpha0, eta0, eta1):
    return tail_mass_poly(y0, r0, alpha0, [eta0, eta1])


def tail_mass_deg3(y0, r0, alpha0, eta0, eta1, eta2):
    return tail_mass_poly(y0, r0, alpha0, [eta0, eta1, eta2])


# ── Combined fit ─────────────────────────────────────────────────────────

@dataclass
class TailEstimates:
    r0: float
    y0: float
    alpha0: float
    eta0: float           # degree-1 coefficient
    eta1: float           # degree-2 coefficient
    eta2: float           # degree-3 coefficient
    obs_total: float
    # Tail masses (ordered: pl >= d1 >= d2 >= d3)
    pl_tail: float
    d1_tail: float
    d2_tail: float
    d3_tail: float
    # Totals
    pl_total: float
    d1_total: float
    d2_total: float
    d3_total: float
    # Metadata
    n_fit: int
    window_logrank: float


def fit_tail_estimates(rank, views, r0, n_fit=None):
    """Fit all four tail models at censoring boundary r0.

    If n_fit is None, uses max(1000, floor(0.3 * r0)).
    """
    if n_fit is None:
        n_fit = max(1000, int(r0 * 0.3))

    rank = np.asarray(rank, dtype=float)
    views = np.asarray(views, dtype=float)
    order = np.argsort(rank)
    rank, views = rank[order], views[order]

    pos = views > 0
    rp, vp = rank[pos], views[pos]

    y0, _ = anchor_y0(rp, vp, r0)

    alpha0, eta0, eta1, eta2, window, n_used = estimate_log_alpha_poly(
        np.log(rp), np.log(vp), np.log(float(r0)), n_fit=n_fit
    )

    obs_total = float(views[rank <= r0].sum())

    pl_tail = tail_mass_pl(y0, r0, alpha0)
    d1_tail = tail_mass_deg1(y0, r0, alpha0, eta0)
    d2_tail = tail_mass_deg2(y0, r0, alpha0, eta0, eta1)
    d3_tail = tail_mass_deg3(y0, r0, alpha0, eta0, eta1, eta2)

    return TailEstimates(
        r0=r0, y0=y0, alpha0=alpha0, eta0=eta0, eta1=eta1, eta2=eta2,
        obs_total=obs_total,
        pl_tail=pl_tail, d1_tail=d1_tail, d2_tail=d2_tail, d3_tail=d3_tail,
        pl_total=obs_total + pl_tail,
        d1_total=obs_total + d1_tail,
        d2_total=obs_total + d2_tail,
        d3_total=obs_total + d3_tail,
        n_fit=n_used, window_logrank=window,
    )


# Keep backward-compatible alias
fit_three_estimates = fit_tail_estimates


# ── Plotting ─────────────────────────────────────────────────────────────

def make_plot(df_week, date_str, thresholds, outpath):
    fig, axes = plt.subplots(1, len(thresholds), figsize=(7 * len(thresholds), 6.5),
                             sharey=True)
    if len(thresholds) == 1:
        axes = [axes]
    fig.suptitle(f"Facebook – {date_str}    "
                 "Polynomial-in-log-α:  deg 0 (PL) > deg 1 > deg 2 > deg 3",
                 fontsize=11, fontweight="bold")

    rank = df_week["rank"].values.astype(float)
    views = df_week["metric_value"].values.astype(float)
    order = np.argsort(rank)
    rank, views = rank[order], views[order]

    total_obs = views.sum()
    share = views / total_obs

    for ax, r0 in zip(axes, thresholds):
        max_rank = int(rank.max())
        if r0 > max_rank:
            ax.set_title(f"$r_c$ = {r0:,}  (exceeds max rank)")
            ax.set_xlabel("Rank")
            continue

        pos = views > 0
        try:
            est = fit_tail_estimates(rank[pos], views[pos], r0=r0)
        except Exception as e:
            ax.set_title(f"$r_c$ = {r0:,}  Fit failed: {e}")
            continue

        # Observed data
        ax.loglog(rank[pos], share[pos], '-', color='0.60', linewidth=0.5,
                  alpha=0.7, label='Observed', zorder=1)

        # Rank grids for model curves
        r_obs_start = max(int(r0 * 0.8), 1)
        r_obs = np.arange(r_obs_start, int(r0) + 1, dtype=float)
        r_tail_max = min(int(r0 * 150), 500000)
        r_tail_lin = np.arange(int(r0) + 1, min(int(r0) + 2001, r_tail_max), dtype=float)
        r_tail_log = np.unique(np.logspace(
            np.log10(max(r0 + 1, r0 * 1.2)), np.log10(r_tail_max), 200
        ).astype(int)).astype(float)
        r_tail = np.unique(np.concatenate([r_tail_lin, r_tail_log]))
        r_full = np.concatenate([r_obs, r_tail])

        # Degree 0: Power law (red dashed)
        y_pl = predict_power_law(r_full, est.y0, r0, est.alpha0)
        ax.loglog(r_full, y_pl / total_obs, '--', color='tab:red', linewidth=1.5,
                  alpha=0.8, label=f'PL (α={est.alpha0:.2f})', zorder=3)

        # Degree 1: Constant η (blue)
        y_d1 = predict_deg1(r_full, est.y0, r0, est.alpha0, est.eta0)
        ax.loglog(r_full, y_d1 / total_obs, '-', color='tab:blue', linewidth=1.5,
                  alpha=0.8, label=f'Deg 1 (η₀={est.eta0:.2f})', zorder=3)

        # Degree 2: Linear η (green)
        y_d2 = predict_deg2(r_full, est.y0, r0, est.alpha0, est.eta0, est.eta1)
        ax.loglog(r_full, y_d2 / total_obs, '-', color='tab:green', linewidth=1.5,
                  alpha=0.8, label=f'Deg 2 (η₁={est.eta1:.2f})', zorder=3)

        # Degree 3: Quadratic η (purple)
        y_d3 = predict_deg3(r_full, est.y0, r0, est.alpha0, est.eta0, est.eta1, est.eta2)
        ax.loglog(r_full, y_d3 / total_obs, '-', color='tab:purple', linewidth=1.5,
                  alpha=0.8, label=f'Deg 3 (η₂={est.eta2:.2f})', zorder=3)

        # Anchor point and boundaries
        ax.plot(r0, est.y0 / total_obs, 'o', color='black', ms=5, zorder=5)
        ax.axvline(r0, color='black', ls='--', lw=0.7, alpha=0.4)
        fit_left = max(1, int(r0) - est.n_fit)
        ax.axvline(fit_left, color='black', ls=':', lw=0.5, alpha=0.25)

        def pct(tail, total):
            return f"{tail / total * 100:.1f}%" if np.isfinite(tail) else "inf"

        title_lines = [
            f"$r_c$={r0:,}  α={est.alpha0:.2f}  "
            f"η₀={est.eta0:.3f}  η₁={est.eta1:.3f}  η₂={est.eta2:.3f}",
            f"PL:{pct(est.pl_tail, est.pl_total)}  "
            f"D1:{pct(est.d1_tail, est.d1_total)}  "
            f"D2:{pct(est.d2_tail, est.d2_total)}  "
            f"D3:{pct(est.d3_tail, est.d3_total)}",
        ]
        ax.set_title("\n".join(title_lines), fontsize=9)
        ax.set_xlabel("Rank")
        if ax is axes[0]:
            ax.set_ylabel("Weekly share")
        ax.legend(fontsize=7, loc='lower left')
        ax.set_xlim(1, r_tail_max)
        ymin = share[pos].min() * 0.1
        ax.set_ylim(bottom=max(ymin, 1e-12))

    plt.tight_layout()
    fig.savefig(outpath, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {outpath}")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    repo = Path(__file__).resolve().parent.parent
    data_path = repo / "data" / "raw" / "fb_ranked_weekly_cutdown.parquet"

    print(f"Loading {data_path} ...")
    df = pd.read_parquet(data_path)
    print(f"  Shape: {df.shape}, Columns: {list(df.columns)}")

    dates = sorted(df["date"].unique())
    print(f"  {len(dates)} weeks: {dates[0]} to {dates[-1]}")

    rng = np.random.default_rng(42)
    chosen = sorted(rng.choice(dates, size=3, replace=False))
    print(f"\nSelected weeks: {chosen}")

    thresholds = [3000, 5000, 8000]
    outdir = Path(__file__).resolve().parent
    plot_files = []

    for date_val in chosen:
        week = df[df["date"] == date_val].copy()
        week = week[week["metric_value"] > 0].sort_values("rank").reset_index(drop=True)

        date_str = str(date_val)
        print(f"\n{'='*80}")
        print(f"Week: {date_str}  |  N={len(week):,}  |  Max rank={week['rank'].max():,}")
        total = week['metric_value'].sum()
        print(f"Total observed views: {total:,.0f}")

        for r0 in thresholds:
            try:
                est = fit_tail_estimates(
                    week["rank"].values.astype(float),
                    week["metric_value"].values.astype(float),
                    r0=r0,
                )
                print(f"\n  r_c = {r0:,}:  α={est.alpha0:.3f}  "
                      f"η₀={est.eta0:.4f}  η₁={est.eta1:.4f}  η₂={est.eta2:.4f}")
                print(f"    y0={est.y0:,.0f}  n_fit={est.n_fit}  "
                      f"Δlog_r={est.window_logrank:.3f}")

                def fmt(label, tail, total):
                    if np.isfinite(tail):
                        return (f"    {label:12s}: tail={tail:>14,.0f}  "
                                f"total={total:>14,.0f}  "
                                f"tail%={tail/total*100:>5.1f}%")
                    return f"    {label:12s}: tail={'inf':>14s}"

                print(fmt("PL (deg 0)", est.pl_tail, est.pl_total))
                print(fmt("Deg 1", est.d1_tail, est.d1_total))
                print(fmt("Deg 2", est.d2_tail, est.d2_total))
                print(fmt("Deg 3", est.d3_tail, est.d3_total))

            except Exception as e:
                import traceback
                print(f"  r_c={r0:,}: failed: {e}")
                traceback.print_exc()

        outfile = outdir / f"fb_tail_estimation_{date_str}.png"
        make_plot(week, date_str, thresholds, outfile)
        plot_files.append(outfile)

    # Summary table
    print(f"\n{'='*80}")
    print("SUMMARY: Unobserved tail as % of estimated total")
    print(f"{'Week':<14s}  {'r_c':>6s}  {'α':>5s}  {'η₀':>6s}  {'η₁':>6s}  {'η₂':>6s}  "
          f"{'PL':>6s}  {'D1':>6s}  {'D2':>6s}  {'D3':>6s}")
    print("-" * 82)
    for date_val in chosen:
        week = df[df["date"] == date_val].copy()
        week = week[week["metric_value"] > 0]
        for r0 in thresholds:
            try:
                est = fit_tail_estimates(
                    week["rank"].values.astype(float),
                    week["metric_value"].values.astype(float),
                    r0=r0,
                )
                def p(t, tot):
                    return f"{t/tot*100:.1f}%" if np.isfinite(t) else "inf"
                print(f"{str(date_val):<14s}  {r0:>6,d}  {est.alpha0:>5.2f}  "
                      f"{est.eta0:>6.3f}  {est.eta1:>6.3f}  {est.eta2:>6.3f}  "
                      f"{p(est.pl_tail, est.pl_total):>6s}  "
                      f"{p(est.d1_tail, est.d1_total):>6s}  "
                      f"{p(est.d2_tail, est.d2_total):>6s}  "
                      f"{p(est.d3_tail, est.d3_total):>6s}")
            except Exception:
                print(f"{str(date_val):<14s}  {r0:>6,d}  failed")

    print(f"\n{'='*80}")
    print("Output files:")
    for f in plot_files:
        print(f"  {f}")


if __name__ == "__main__":
    main()
