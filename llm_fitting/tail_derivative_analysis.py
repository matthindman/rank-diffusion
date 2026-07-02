"""
Derivative analysis of the rank–activity curve in log-log space.

For a single FB week, compute and plot:
  1. z(x)        = smoothed log-views vs log-rank  (the curve itself)
  2. z'(x)       = local slope  (= -alpha(x), the local Pareto exponent)
  3. z''(x)      = rate of change of slope (= -kappa(x))
  4. z'''(x)     = change in rate of change

All computed after local smoothing (Gaussian kernel in log-rank).
Excludes top 100 ranks to avoid superstar distortion.
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter1d
from pathlib import Path


def smooth_and_differentiate(x, z, sigma=0.03):
    """
    Smooth z(x) with a Gaussian kernel of width sigma (in x-units),
    then compute first three derivatives via finite differences.

    Parameters
    ----------
    x : array, sorted, uniformly-ish spaced in log-rank
    z : array, log-views at each log-rank
    sigma : float, smoothing bandwidth in log-rank units

    Returns
    -------
    z_smooth, dz, d2z, d3z : arrays (same length as x)
    """
    # Convert sigma from x-units to index-units
    dx_median = np.median(np.diff(x))
    sigma_idx = sigma / dx_median

    # Smooth
    zs = gaussian_filter1d(z, sigma=sigma_idx, mode='nearest')

    # Derivatives via central finite differences on the smoothed curve
    dz = np.gradient(zs, x)
    d2z = np.gradient(dz, x)
    d3z = np.gradient(d2z, x)

    return zs, dz, d2z, d3z


def main():
    repo = Path(__file__).resolve().parent.parent
    data_path = repo / "data" / "raw" / "fb_ranked_weekly_cutdown.parquet"

    print(f"Loading {data_path} ...")
    df = pd.read_parquet(data_path)

    dates = sorted(df["date"].unique())
    # Pick one week near the middle of the sample
    chosen_date = dates[len(dates) // 2]
    print(f"Selected week: {chosen_date}")

    week = df[df["date"] == chosen_date].copy()
    week = week[week["metric_value"] > 0].sort_values("rank").reset_index(drop=True)
    print(f"  N = {len(week):,}, max rank = {week['rank'].max():,}")

    # Aggregate to unique ranks (max views if any ties) and exclude top 100
    week = week[week["rank"] > 100]
    agg = week.groupby("rank")["metric_value"].max().reset_index()
    agg = agg.sort_values("rank")

    rank = agg["rank"].values.astype(float)
    views = agg["metric_value"].values.astype(float)

    print(f"  After dedup: {len(rank):,} unique ranks in [{int(rank.min())}, {int(rank.max())}]")

    x = np.log(rank)
    z = np.log(views)

    # Verify strictly increasing x (should be, after dedup+sort)
    assert np.all(np.diff(x) > 0), "log-rank must be strictly increasing"

    # Use two smoothing bandwidths: moderate and heavier
    sigma_light = 0.04   # ~4% of a log-rank unit
    sigma_heavy = 0.10   # ~10%

    zs_l, dz_l, d2z_l, d3z_l = smooth_and_differentiate(x, z, sigma=sigma_light)
    zs_h, dz_h, d2z_h, d3z_h = smooth_and_differentiate(x, z, sigma=sigma_heavy)

    # Convert to more interpretable quantities:
    #   alpha(x) = -z'(x)       local Pareto exponent
    #   kappa(x) = -z''(x)      rate of steepening  (>0 if tail steepens)
    #   kappa'(x) = -z'''(x)    change in steepening rate
    alpha_l, alpha_h = -dz_l, -dz_h
    kappa_l, kappa_h = -d2z_l, -d2z_h
    dkappa_l, dkappa_h = -d3z_l, -d3z_h

    # Also compute eta(x) = kappa(x)/alpha(x) = d log(alpha) / dx
    # This is the log-slope parameter from the tail estimator
    with np.errstate(divide='ignore', invalid='ignore'):
        eta_l = np.where(alpha_l > 0.05, kappa_l / alpha_l, np.nan)
        eta_h = np.where(alpha_h > 0.05, kappa_h / alpha_h, np.nan)

    # Candidate thresholds
    thresholds = [2000, 5000, 8000]
    th_logr = [np.log(t) for t in thresholds]

    # ── Clip display range ─────────────────────────────────────────────
    # The derivatives explode at the data cliff (last ~10% of ranks).
    # Restrict display to ranks 101 through ~90th percentile of max rank
    # to focus on the informative region.
    x_max_display = np.log(rank.max() * 0.85)
    display = x <= x_max_display

    xd = x[display]
    zd, zs_ld, zs_hd = z[display], zs_l[display], zs_h[display]
    alpha_ld, alpha_hd = alpha_l[display], alpha_h[display]
    kappa_ld, kappa_hd = kappa_l[display], kappa_h[display]
    eta_ld, eta_hd = eta_l[display], eta_h[display]
    dkappa_ld, dkappa_hd = dkappa_l[display], dkappa_h[display]

    # ── Plot ──────────────────────────────────────────────────────────
    fig, axes = plt.subplots(5, 1, figsize=(14, 20), sharex=True)
    fig.suptitle(f"Facebook – {chosen_date} – Derivative structure (ranks > 100)",
                 fontsize=14, fontweight="bold")

    # Panel 0: the curve itself
    ax = axes[0]
    ax.plot(xd, zd, '-', color='0.75', linewidth=0.4, alpha=0.5, label='Raw')
    ax.plot(xd, zs_ld, '-', color='tab:blue', linewidth=1.0, alpha=0.8,
            label=f'Smoothed (σ={sigma_light})')
    ax.plot(xd, zs_hd, '-', color='tab:red', linewidth=1.0, alpha=0.8,
            label=f'Smoothed (σ={sigma_heavy})')
    ax.set_ylabel('log(views) = z(x)')
    ax.legend(fontsize=8)
    ax.set_title('(a)  Log-views vs log-rank', fontsize=10, loc='left')
    for lr in th_logr:
        ax.axvline(lr, color='0.3', ls=':', lw=0.7, alpha=0.5)

    # Panel 1: alpha(x) = -z'(x) = local Pareto exponent
    ax = axes[1]
    ax.plot(xd, alpha_ld, '-', color='tab:blue', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_light}')
    ax.plot(xd, alpha_hd, '-', color='tab:red', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_heavy}')
    ax.axhline(1.0, color='black', ls='-', lw=0.5, alpha=0.3)
    ax.axhline(2.0, color='black', ls='-', lw=0.5, alpha=0.3)
    ax.set_ylabel('α(x) = −z\'(x)')
    ax.set_ylim(-0.2, 5.0)
    ax.legend(fontsize=8)
    ax.set_title('(b)  Local Pareto exponent  (α > 1 → finite tail mass)', fontsize=10, loc='left')
    for lr in th_logr:
        ax.axvline(lr, color='0.3', ls=':', lw=0.7, alpha=0.5)

    # Panel 2: kappa(x) = -z''(x) = rate of steepening
    ax = axes[2]
    ax.plot(xd, kappa_ld, '-', color='tab:blue', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_light}')
    ax.plot(xd, kappa_hd, '-', color='tab:red', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_heavy}')
    ax.axhline(0.0, color='black', ls='-', lw=0.5, alpha=0.3)
    ax.set_ylabel('κ(x) = −z\'\'(x)')
    # Clip y-axis to see the structure
    kappa_p99 = np.nanpercentile(np.abs(kappa_hd), 95)
    ax.set_ylim(-kappa_p99 * 0.5, kappa_p99 * 2.0)
    ax.legend(fontsize=8)
    ax.set_title('(c)  Rate of steepening  (>0 = tail getting steeper)', fontsize=10, loc='left')
    for lr in th_logr:
        ax.axvline(lr, color='0.3', ls=':', lw=0.7, alpha=0.5)

    # Panel 3: eta(x) = kappa/alpha = d log(alpha)/dx
    ax = axes[3]
    ax.plot(xd, eta_ld, '-', color='tab:blue', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_light}')
    ax.plot(xd, eta_hd, '-', color='tab:red', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_heavy}')
    ax.axhline(0.0, color='black', ls='-', lw=0.5, alpha=0.3)
    ax.set_ylabel('η(x) = κ/α')
    eta_p99 = np.nanpercentile(np.abs(eta_hd[np.isfinite(eta_hd)]), 95)
    ax.set_ylim(-eta_p99 * 0.5, eta_p99 * 2.0)
    ax.legend(fontsize=8)
    ax.set_title('(d)  Log-slope parameter η = κ/α  (constant η assumed in tail estimator)',
                 fontsize=10, loc='left')
    for lr in th_logr:
        ax.axvline(lr, color='0.3', ls=':', lw=0.7, alpha=0.5)

    # Panel 4: dkappa(x) = -z'''(x) = change in steepening rate
    ax = axes[4]
    ax.plot(xd, dkappa_ld, '-', color='tab:blue', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_light}')
    ax.plot(xd, dkappa_hd, '-', color='tab:red', linewidth=1.0, alpha=0.8,
            label=f'σ={sigma_heavy}')
    ax.axhline(0.0, color='black', ls='-', lw=0.5, alpha=0.3)
    ax.set_ylabel("κ'(x) = −z'''(x)")
    dk_p99 = np.nanpercentile(np.abs(dkappa_hd), 95)
    ax.set_ylim(-dk_p99 * 1.5, dk_p99 * 1.5)
    ax.legend(fontsize=8)
    ax.set_title("(e)  Change in steepening rate  (≈ 0 where constant-κ or constant-η models hold)",
                 fontsize=10, loc='left')
    for lr in th_logr:
        ax.axvline(lr, color='0.3', ls=':', lw=0.7, alpha=0.5)

    ax.set_xlabel('x = log(rank)')

    # Add rank tick labels on top axis
    ax_top = axes[0].twiny()
    rank_ticks = [200, 500, 1000, 2000, 5000, 8000]
    ax_top.set_xlim(axes[0].get_xlim())
    ax_top.set_xticks([np.log(r) for r in rank_ticks])
    ax_top.set_xticklabels([f'{r:,}' for r in rank_ticks], fontsize=8)
    ax_top.set_xlabel('Rank', fontsize=9)

    # Annotate threshold lines
    for lr, th in zip(th_logr, thresholds):
        axes[0].annotate(f'r={th:,}', xy=(lr, axes[0].get_ylim()[1]),
                         xytext=(3, -12), textcoords='offset points',
                         fontsize=7, color='0.3', rotation=0)

    plt.tight_layout()
    outpath = Path(__file__).resolve().parent / "fb_derivative_structure.png"
    fig.savefig(outpath, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {outpath}")

    # ── Print key values at threshold locations ──
    print(f"\n{'='*70}")
    print(f"Derivative values at candidate thresholds (heavy smoothing σ={sigma_heavy}):")
    dk_label = "κ'"
    print(f"{'Rank':>8}  {'log(r)':>7}  {'α':>6}  {'κ':>7}  {'η':>7}  {dk_label:>7}")
    print(f"{'-'*50}")
    for th in thresholds:
        lr = np.log(th)
        idx = np.argmin(np.abs(x - lr))
        print(f"{th:>8,}  {x[idx]:>7.3f}  {alpha_h[idx]:>6.3f}  "
              f"{kappa_h[idx]:>7.4f}  {eta_h[idx]:>7.4f}  {dkappa_h[idx]:>7.4f}")

    # Also find where alpha crosses key values
    print(f"\n{'='*70}")
    print("Structural landmarks:")
    for threshold_alpha in [1.0, 1.5, 2.0, 2.5, 3.0]:
        crossings = np.where(np.diff(np.sign(alpha_h - threshold_alpha)))[0]
        if len(crossings) > 0:
            idx = crossings[0]
            r_cross = np.exp(x[idx])
            print(f"  α crosses {threshold_alpha:.1f} at rank ≈ {r_cross:,.0f}  "
                  f"(κ={kappa_h[idx]:.4f}, η={eta_h[idx]:.4f})")

    # Find where kappa peaks
    peak_idx = np.argmax(kappa_h[10:-10]) + 10  # avoid edges
    print(f"  κ peaks at rank ≈ {np.exp(x[peak_idx]):,.0f}  "
          f"(κ={kappa_h[peak_idx]:.4f}, α={alpha_h[peak_idx]:.3f})")

    # Find where kappa' ≈ 0 (kappa is roughly constant)
    # Look for the region where |kappa'| is smallest
    abs_dkappa = np.abs(dkappa_h[50:-50])
    flat_idx = np.argmin(abs_dkappa) + 50
    print(f"  κ' ≈ 0 (κ most stable) at rank ≈ {np.exp(x[flat_idx]):,.0f}  "
          f"(κ={kappa_h[flat_idx]:.4f}, α={alpha_h[flat_idx]:.3f})")

    # Find where eta is most stable
    eta_valid = eta_h.copy()
    eta_valid[np.isnan(eta_valid)] = 999
    d_eta = np.abs(np.gradient(eta_valid, x))
    d_eta[:50] = 999
    d_eta[-50:] = 999
    stable_eta_idx = np.argmin(d_eta)
    print(f"  η most stable at rank ≈ {np.exp(x[stable_eta_idx]):,.0f}  "
          f"(η={eta_h[stable_eta_idx]:.4f}, α={alpha_h[stable_eta_idx]:.3f})")


if __name__ == "__main__":
    main()
