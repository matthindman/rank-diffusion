#!/usr/bin/env python3
"""
Generate structural visualization diagrams for model_v42.py.
Produces two figures:
  1. v42_architecture.png  - Data flow / pipeline architecture
  2. v42_call_graph.png    - Call graph with functional clustering
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import numpy as np

# ============================================================
# Color palette
# ============================================================
PAL = {
    'data':  ('#dbeafe', '#1e40af'),   # blue
    'est':   ('#dcfce7', '#166534'),   # green
    'sim':   ('#ffedd5', '#9a3412'),   # orange
    'cal':   ('#fce7f3', '#9d174d'),   # pink
    'val':   ('#ede9fe', '#5b21b6'),   # purple
    'ext':   ('#ccfbf1', '#115e59'),   # teal
    'lib':   ('#fef9c3', '#854d0e'),   # yellow (external libs)
    'gray':  ('#f1f5f9', '#475569'),   # neutral
}


def box(ax, cx, cy, w, h, lines, cat, fontsize=9, bold_line=0, pad=0.3):
    """Draw a rounded box centered at (cx, cy) with multi-line text.
    lines: list of strings (each rendered on its own line).
    cat: key into PAL for colors.
    bold_line: index of the line to render bold (-1 for none).
    Returns (cx, cy) for arrow connections.
    """
    fc, ec = PAL[cat]
    x0, y0 = cx - w/2, cy - h/2
    b = FancyBboxPatch((x0, y0), w, h,
                        boxstyle=f"round,pad={pad}",
                        facecolor=fc, edgecolor=ec,
                        linewidth=1.8, zorder=2)
    ax.add_patch(b)
    n = len(lines)
    line_h = fontsize * 1.6 / 72  # approx line height in inches (converted later)
    # We use axis coords, so just space lines evenly within the box
    top = cy + h/2 - 0.35 * h / max(n, 1)
    spacing = 0.7 * h / max(n, 1)
    for i, txt in enumerate(lines):
        yy = top - i * spacing
        fw = 'bold' if i == bold_line else 'normal'
        ax.text(cx, yy, txt, ha='center', va='center',
                fontsize=fontsize, fontweight=fw, color=ec, zorder=3)
    return cx, cy


def arrow(ax, x1, y1, x2, y2, color='#64748b', lw=1.4, style='-|>',
          rad=0.0):
    """Draw an arrow from (x1,y1) to (x2,y2)."""
    cs = f'arc3,rad={rad}' if rad else 'arc3,rad=0'
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color,
                                linewidth=lw, connectionstyle=cs,
                                shrinkA=4, shrinkB=4),
                zorder=1)


def section_bg(ax, x, y, w, h, cat, label='', fontsize=10):
    """Shaded background rectangle with optional label."""
    fc, ec = PAL[cat]
    r = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15",
                        facecolor=fc, edgecolor=ec,
                        linewidth=1.0, alpha=0.35, zorder=0)
    ax.add_patch(r)
    if label:
        ax.text(x + 0.3, y + h - 0.25, label, fontsize=fontsize,
                fontweight='bold', color=ec, va='top', zorder=1)


# ============================================================
# FIGURE 1: Pipeline Architecture & Data Flow
# ============================================================
def make_architecture_figure():
    """Clean vertical pipeline diagram.  Each major phase gets its own row.
    Uses a wide canvas with generous spacing. No fan-of-arrows — uses
    horizontal collector bars instead."""

    fig, ax = plt.subplots(figsize=(24, 34))
    ax.set_xlim(0, 24)
    ax.set_ylim(0, 34)
    ax.set_aspect('equal')
    ax.axis('off')
    fig.subplots_adjust(left=0.02, right=0.98, top=0.97, bottom=0.01)

    ax.text(12, 33.2, 'model_v42.py  —  Pipeline Architecture & Data Flow',
            ha='center', va='center', fontsize=22, fontweight='bold',
            color='#1e293b')

    # ----------------------------------------------------------
    # ROW 1: Data Loading  (y ~ 31)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 29.5, 23, 3.0, 'data', 'DATA LOADING  (lines 50–141)')

    box(ax, 3.5, 31.2, 4.2, 1.3,
        ['Parquet File', 'fb_ranked_weekly_cutdown'], 'data', fontsize=11, bold_line=0)
    box(ax, 9, 31.2, 4.5, 1.3,
        ['Balanced Panel', 'log_metric, log_changes', '(T × N_balanced)'], 'data', fontsize=11, bold_line=0)
    box(ax, 15, 31.2, 5, 1.3,
        ['Empirical Targets', 'vr, acf, racf, pers, xr2', 'emp_kurt, zipf_slope'], 'data', fontsize=11, bold_line=0)
    box(ax, 21, 31.2, 3.5, 1.3,
        ['Band Stats (5 bands)', 'var, vr4, vr13, acf1'], 'data', fontsize=10, bold_line=0)

    box(ax, 9, 30.0, 4.5, 0.7,
        ['Weekly Stats: mean_N, mean_exits'], 'data', fontsize=9)

    arrow(ax, 5.6, 31.2, 6.75, 31.2, PAL['data'][1], lw=2.5)
    arrow(ax, 11.25, 31.2, 12.5, 31.2, PAL['data'][1], lw=2.5)
    arrow(ax, 11.25, 31.0, 19.25, 31.0, PAL['data'][1], lw=1.5)

    # ----------------------------------------------------------
    # ROW 2: Estimation Stages 1–4.5  (y ~ 26.5)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 24.5, 23, 4.5, 'est', 'PARAMETER ESTIMATION  (lines 143–424)')

    box(ax, 3, 27.5, 3.5, 1.5,
        ['Stage 1', '(143–177)', 'sigma_obs', 'ACF structure'], 'est', fontsize=10, bold_line=0)
    box(ax, 7.5, 27.5, 3.5, 1.5,
        ['Stage 2', '(179–186)', 'sigma_het', 'mean/median var'], 'est', fontsize=10, bold_line=0)
    box(ax, 12, 27.5, 3.5, 1.5,
        ['Stage 3', '(188–250)', 'band t_df (5)', 'noise correction'], 'est', fontsize=10, bold_line=0)
    box(ax, 16.5, 27.5, 3.5, 1.5,
        ['Stage 4', '(252–263)', 'jump_prob', 'jump_scale'], 'est', fontsize=10, bold_line=0)
    box(ax, 21, 27.5, 3.5, 1.5,
        ['Stage 4.5', '(265–289)', 'alpha_arch', 'ACF of z²'], 'est', fontsize=10, bold_line=0)

    # Simple downward arrows from data to estimation
    arrow(ax, 15, 30.55, 3, 28.25, PAL['data'][1], lw=1.3)
    arrow(ax, 15, 30.55, 7.5, 28.25, PAL['data'][1], lw=1.3)
    arrow(ax, 15, 30.55, 12, 28.25, PAL['data'][1], lw=1.3)
    arrow(ax, 15, 30.55, 16.5, 28.25, PAL['data'][1], lw=1.3)
    arrow(ax, 21, 30.55, 21, 28.25, PAL['data'][1], lw=1.3)

    # Stage 5 & 6
    box(ax, 7, 25.5, 7, 1.2,
        ['Stage 5 (291–353)', 'se, phi, sn via fit_params() × 200 restarts → get_p(ranks)'],
        'est', fontsize=11, bold_line=0)
    box(ax, 17.5, 25.5, 7, 1.2,
        ['Stage 6 (355–424)', 'kappa_base, kappa_global, w0_sorted, N_FULL, exit params'],
        'est', fontsize=11, bold_line=0)

    arrow(ax, 3, 26.75, 4.5, 26.1, PAL['est'][1], lw=1.5)
    arrow(ax, 7.5, 26.75, 7, 26.1, PAL['est'][1], lw=1.5)
    arrow(ax, 10.5, 25.5, 14, 25.5, PAL['est'][1], lw=2.5)
    arrow(ax, 21, 26.75, 20, 26.1, PAL['est'][1], lw=1.5)

    # ----------------------------------------------------------
    # Collector bar: "All parameters" (y ~ 23.8)
    # ----------------------------------------------------------
    bar_y = 24.0
    ax.plot([3, 21], [bar_y, bar_y], color=PAL['est'][1], lw=3, solid_capstyle='round', zorder=2)
    ax.text(12, bar_y + 0.25, 'All estimated parameters', ha='center', va='bottom',
            fontsize=10, color=PAL['est'][1], style='italic', fontweight='bold')
    # Arrows down from stages to bar
    for cx in [3, 7.5, 12, 16.5, 21]:
        arrow(ax, cx, 26.75, cx, bar_y + 0.1, PAL['est'][1], lw=0.8)
    # Arrows from S5/S6 to bar
    arrow(ax, 7, 24.9, 7, bar_y + 0.1, PAL['est'][1], lw=1.2)
    arrow(ax, 17.5, 24.9, 17.5, bar_y + 0.1, PAL['est'][1], lw=1.2)

    # ----------------------------------------------------------
    # ROW 3: Simulation Engine  (y ~ 20.5)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 18.5, 23, 5.0, 'sim', 'SIMULATION ENGINE  (lines 426–759)')

    # Single arrow from bar to run_sim
    arrow(ax, 12, bar_y - 0.1, 12, 22.7, PAL['est'][1], lw=3)

    box(ax, 7, 21.8, 7, 1.6,
        ['run_sim(seed, *, config, ...)',
         'lines 440–759  |  320 lines',
         '8 feature flags · 6 param overrides',
         '3 return modes'],
        'sim', fontsize=12, bold_line=0)

    box(ax, 18, 21.8, 6.5, 1.6,
        ['Time Loop  t = 1..T_total',
         'A: get_p(ranks) → se, phi, sn',
         'B: Permanent innov (η) ± jumps',
         'C: ARCH  · D: Transitory (ν)',
         'E–H: state  · I: Exit/entry  · J: Rank'],
        'sim', fontsize=10)

    box(ax, 7, 19.5, 7, 1.0,
        ['get_tdf_local(ranks)  (nested, lines 493–499)',
         'Interpolates t_df by rank (calibrated or pre-cal)'],
        'sim', fontsize=10, bold_line=0)

    arrow(ax, 10.5, 21.8, 14.75, 21.8, PAL['sim'][1], lw=2.5)
    arrow(ax, 7, 21.0, 7, 20.0, PAL['sim'][1], lw=1.5)

    # ----------------------------------------------------------
    # ROW 4: Calibration & Monte Carlo  (y ~ 16)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 14.5, 11, 3.5, 'cal', 'KURTOSIS CALIBRATION  (lines 762–851)')
    section_bg(ax, 12.5, 14.5, 11, 3.5, 'sim', 'MONTE CARLO  (lines 853–880)')

    box(ax, 4, 16.2, 5, 1.3,
        ['Kurtosis Calibration', '5 cal reps → adjust t_df', 'overshoot = 1.5×'],
        'cal', fontsize=11, bold_line=0)
    box(ax, 9.5, 16.2, 2.5, 1.3,
        ['Protected:', 'bands 1–100', 'and 101–500'],
        'cal', fontsize=9)

    box(ax, 16, 16.2, 5, 1.3,
        ['Main MC', '25 reps, seeds 42..123', 'mc_stats {mean, std, CI}'],
        'sim', fontsize=11, bold_line=0)
    box(ax, 21.5, 16.2, 2.5, 1.3,
        ['rep_extra', 'seed = 42', 'full arrays'],
        'sim', fontsize=9)

    arrow(ax, 4, 19.0, 4, 16.85, PAL['cal'][1], lw=2.5)
    arrow(ax, 10, 19.0, 16, 16.85, PAL['sim'][1], lw=2.5)
    arrow(ax, 18.5, 16.2, 21.5, 16.2, PAL['sim'][1], lw=1.5)

    # Calibration feedback to Stage 3
    ax.annotate('', xy=(12, 28.25), xytext=(4, 15.55),
                arrowprops=dict(arrowstyle='-|>', color=PAL['cal'][1],
                                linewidth=2.0,
                                connectionstyle='arc3,rad=-0.5',
                                linestyle='dashed'),
                zorder=1)
    ax.text(1.2, 22.5, 'feedback → t_df', fontsize=9, color=PAL['cal'][1],
            ha='center', style='italic', rotation=90, fontweight='bold')

    # ----------------------------------------------------------
    # ROW 5: Validation & Diagnostics  (y ~ 11)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 9.0, 23, 5.0, 'val', 'VALIDATION & DIAGNOSTICS  (lines 882–1693)')

    box(ax, 3.5, 12.2, 5, 1.3,
        ['Validation (882–968)', '15 diagnostics', 'pass / fail → n_pass / 15'],
        'val', fontsize=11, bold_line=0)
    box(ax, 10, 12.2, 5.5, 1.3,
        ['MC Uncertainty (970–1013)', 'SE, d/SE, fragility', 'solid | marginal | FRAGILE'],
        'val', fontsize=11, bold_line=0)
    box(ax, 17.5, 12.2, 5.5, 1.3,
        ['Statistical Tests (1036–1257)', 'KS, JB, Ljung-Box', 'Hill, Shorrocks'],
        'val', fontsize=11, bold_line=0)

    box(ax, 4, 10.0, 5.5, 1.0,
        ['v42_diagnostics.png', '15 panels (lines 1259–1414)'],
        'val', fontsize=10, bold_line=0)
    box(ax, 11, 10.0, 6, 1.0,
        ['v42_pub_diagnostics.png', '15 panels (lines 1416–1684)'],
        'val', fontsize=10, bold_line=0)

    arrow(ax, 16, 15.55, 3.5, 12.85, PAL['val'][1], lw=1.8)
    arrow(ax, 16, 15.55, 10, 12.85, PAL['val'][1], lw=1.8)
    arrow(ax, 21.5, 15.55, 17.5, 12.85, PAL['val'][1], lw=1.8)
    arrow(ax, 3.5, 11.55, 4, 10.5, PAL['val'][1], lw=1.2)
    arrow(ax, 10, 11.55, 11, 10.5, PAL['val'][1], lw=1.2)

    # ----------------------------------------------------------
    # ROW 6: Ablation & Sensitivity  (y ~ 6)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 4.5, 11, 4.0, 'ext', 'ABLATION STUDY  (lines 1694–1887)')
    section_bg(ax, 12.5, 4.5, 11, 4.0, 'ext', 'SENSITIVITY ANALYSIS  (lines 1889–2021)')

    box(ax, 6, 6.5, 6, 1.5,
        ['Ablation Study', '8 cumulative levels × 25 reps', 'Base → Full v3.9',
         '→ v42_ablation.png'],
        'ext', fontsize=11, bold_line=0)
    box(ax, 18, 6.5, 6, 1.5,
        ['Sensitivity Analysis', '6 params × 5 perturbations × 25 reps',
         '±10%, ±20%', '→ v42_sensitivity.png'],
        'ext', fontsize=11, bold_line=0)

    # These call run_sim (upward arrows, curved to left/right to avoid crossing)
    arrow(ax, 3.5, 7.25, 4, 19.0, PAL['ext'][1], lw=1.8, rad=0.4)
    arrow(ax, 20.5, 7.25, 10, 19.0, PAL['ext'][1], lw=1.8, rad=-0.4)
    ax.text(2.2, 13, 'calls run_sim', fontsize=8, color=PAL['ext'][1],
            ha='center', style='italic', rotation=90)
    ax.text(21.8, 13, 'calls run_sim', fontsize=8, color=PAL['ext'][1],
            ha='center', style='italic', rotation=90)

    # ----------------------------------------------------------
    # Legend  (y ~ 3)
    # ----------------------------------------------------------
    legend_y = 3.2
    cats = [('data', 'Data Loading'), ('est', 'Estimation'),
            ('sim', 'Simulation'), ('cal', 'Calibration'),
            ('val', 'Validation'), ('ext', 'Ablation/Sensitivity')]
    for i, (cat, label) in enumerate(cats):
        fc, ec = PAL[cat]
        xx = 1.5 + i * 3.7
        ax.add_patch(mpatches.FancyBboxPatch(
            (xx, legend_y), 0.6, 0.4, boxstyle="round,pad=0.05",
            facecolor=fc, edgecolor=ec, linewidth=1.2))
        ax.text(xx + 0.9, legend_y + 0.2, label, fontsize=10, va='center',
                color=ec, fontweight='bold')

    fig.savefig('/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/v42_architecture.png',
                dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print("Saved: v42_architecture.png")


# ============================================================
# FIGURE 2: Call Graph with Functional Clustering
# ============================================================
def make_call_graph_figure():
    """Clean call graph.  Central hub (run_sim) in the middle,
    callers above, callees below, helpers to the sides.
    Uses wider canvas and avoids crossing arrows."""

    fig, ax = plt.subplots(figsize=(26, 22))
    ax.set_xlim(0, 26)
    ax.set_ylim(0, 22)
    ax.set_aspect('equal')
    ax.axis('off')
    fig.subplots_adjust(left=0.01, right=0.99, top=0.97, bottom=0.01)

    ax.text(13, 21.3, 'model_v42.py  —  Call Graph with Functional Clustering',
            ha='center', va='center', fontsize=22, fontweight='bold',
            color='#1e293b')

    # ----------------------------------------------------------
    # TOP: Script-level execution order  (y ~ 19)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 17.0, 25, 3.5, 'gray',
               'TOP-LEVEL SCRIPT  (sequential execution order →)')

    # Row 1: data → estimation → calibration → MC
    box(ax, 2.5, 19.3, 3.5, 1.2,
        ['Data Loading', 'lines 50–141'], 'data', fontsize=11, bold_line=0)
    box(ax, 7, 19.3, 3.5, 1.2,
        ['Stages 1–4.5', 'lines 143–289'], 'est', fontsize=11, bold_line=0)
    box(ax, 11.5, 19.3, 3.5, 1.2,
        ['Stage 5', 'lines 291–353'], 'est', fontsize=11, bold_line=0)
    box(ax, 16, 19.3, 3.5, 1.2,
        ['Stage 6', 'lines 355–424'], 'est', fontsize=11, bold_line=0)
    box(ax, 20.5, 19.3, 3.5, 1.2,
        ['Kurtosis Cal', 'lines 762–851'], 'cal', fontsize=11, bold_line=0)
    box(ax, 24.5, 19.3, 2, 1.2,
        ['MC', '853–880'], 'sim', fontsize=10, bold_line=0)

    for x_from, x_to in [(4.25, 5.25), (8.75, 9.75), (13.25, 14.25), (17.75, 18.75), (22.25, 23.5)]:
        arrow(ax, x_from, 19.3, x_to, 19.3, '#94a3b8', lw=2)

    # Row 2: validation → pub diags → ablation → sensitivity
    box(ax, 3.5, 17.7, 4, 0.9,
        ['Validation (882–968)'], 'val', fontsize=10)
    box(ax, 9.5, 17.7, 5, 0.9,
        ['Pub Diagnostics (1015–1693)'], 'val', fontsize=10)
    box(ax, 16, 17.7, 4, 0.9,
        ['Ablation (1694–1887)'], 'ext', fontsize=10)
    box(ax, 22, 17.7, 4, 0.9,
        ['Sensitivity (1889–2021)'], 'ext', fontsize=10)

    arrow(ax, 5.5, 17.7, 7, 17.7, '#94a3b8', lw=1.2)
    arrow(ax, 12, 17.7, 14, 17.7, '#94a3b8', lw=1.2)
    arrow(ax, 18, 17.7, 20, 17.7, '#94a3b8', lw=1.2)
    # MC -> validation (wraps to next row)
    arrow(ax, 24.5, 18.7, 5, 18.15, '#94a3b8', lw=1.2)

    # ----------------------------------------------------------
    # LEFT cluster: Structural Estimation  (y ~ 13)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 11.0, 8.5, 5.5, 'est', 'STRUCTURAL ESTIMATION')

    box(ax, 5, 15, 6, 1.3,
        ['fit_params()', 'lines 309–330', '200 Nelder-Mead restarts'],
        'est', fontsize=11, bold_line=0)
    box(ax, 3, 12.5, 3.5, 1.2,
        ['model_vr()', 'lines 295–300', 'VR(k) theory'],
        'est', fontsize=10, bold_line=0)
    box(ax, 7.5, 12.5, 3.5, 1.2,
        ['model_acf1_fn()', 'lines 302–306', 'ACF(1) theory'],
        'est', fontsize=10, bold_line=0)

    # fit_params calls model_vr and model_acf1
    arrow(ax, 3.5, 14.35, 3, 13.1, PAL['est'][1], lw=1.8)
    arrow(ax, 6.5, 14.35, 7.5, 13.1, PAL['est'][1], lw=1.8)

    # Stage 5 -> fit_params
    arrow(ax, 11.5, 18.7, 6, 15.65, PAL['est'][1], lw=2.5)

    # ----------------------------------------------------------
    # RIGHT cluster: Interpolation  (y ~ 13)
    # ----------------------------------------------------------
    section_bg(ax, 9.5, 11.0, 8, 5.5, 'est', 'INTERPOLATION FUNCTIONS')

    box(ax, 12, 15, 4, 1.2,
        ['get_p(ranks)', 'lines 349–353', 'Interp se, phi, sn'],
        'est', fontsize=11, bold_line=0)
    box(ax, 15.5, 13, 4, 1.2,
        ['get_tdf_local(ranks)', 'lines 493–499 (nested)', 'Interp t_df by rank'],
        'sim', fontsize=10, bold_line=0)
    box(ax, 15.5, 11.5, 4, 0.8,
        ['get_tdf_precal()', 'lines 843–846 (unused)'],
        'gray', fontsize=9)

    # ----------------------------------------------------------
    # CENTER: run_sim  (y ~ 8)
    # ----------------------------------------------------------
    section_bg(ax, 3.5, 6.5, 19, 4.0, 'sim', 'SIMULATION KERNEL')

    box(ax, 13, 8.5, 12, 1.8,
        ['run_sim(seed, *, config, return_extra, return_band_kurtosis)',
         'lines 440–759  |  320 lines  |  THE CENTRAL HUB',
         'State: tau, c_state, het_multiplier, ep_type, endpoint_id, last_z_sq',
         'Loop: t = 1..T_total  |  18 conditional branches'],
        'sim', fontsize=11, bold_line=0)

    # run_sim calls get_p and get_tdf_local (upward)
    arrow(ax, 11, 9.4, 12, 14.4, PAL['sim'][1], lw=2.5)
    ax.text(10.5, 12, 'calls', fontsize=9, color=PAL['sim'][1],
            ha='center', style='italic', rotation=70)
    arrow(ax, 15, 9.4, 15.5, 12.4, PAL['sim'][1], lw=2.5)
    ax.text(15.8, 11, 'calls', fontsize=9, color=PAL['sim'][1],
            ha='center', style='italic', rotation=70)

    # Callers of run_sim (downward from top).
    # Route each arrow to a different x-position on run_sim to avoid overlap.
    arrow(ax, 20.5, 18.7, 17, 9.4, PAL['cal'][1], lw=2.5)   # kurtosis cal
    ax.text(19.5, 14, 'Kurtosis Cal', fontsize=8, color=PAL['cal'][1],
            ha='center', rotation=65, style='italic')
    arrow(ax, 24.5, 18.7, 19, 9.4, PAL['sim'][1], lw=2.5)    # main MC
    ax.text(22.5, 14, 'Main MC', fontsize=8, color=PAL['sim'][1],
            ha='center', rotation=65, style='italic')
    arrow(ax, 16, 17.25, 13, 9.4, PAL['ext'][1], lw=2.0)     # ablation
    arrow(ax, 22, 17.25, 15, 9.4, PAL['ext'][1], lw=2.0)     # sensitivity

    # ----------------------------------------------------------
    # BOTTOM LEFT: External library calls  (y ~ 3.5)
    # ----------------------------------------------------------
    section_bg(ax, 0.5, 2.5, 12, 3.5, 'lib',
               'EXTERNAL LIBRARY CALLS  (called by run_sim)')

    box(ax, 3, 4.5, 3.5, 1.1,
        ['scipy.stats.t.rvs()', 'heavy-tailed draws'],
        'lib', fontsize=10)
    box(ax, 7.5, 4.5, 3.5, 1.1,
        ['scipy.stats.kurtosis()', 'band diagnostics'],
        'lib', fontsize=10)
    box(ax, 11, 4.5, 2.5, 1.1,
        ['np.polyfit()', 'Zipf slope'],
        'lib', fontsize=10)

    arrow(ax, 9, 7.6, 3, 5.05, PAL['sim'][1], lw=1.5)
    arrow(ax, 11, 7.6, 7.5, 5.05, PAL['sim'][1], lw=1.5)
    arrow(ax, 13, 7.6, 11, 5.05, PAL['sim'][1], lw=1.5)

    # ----------------------------------------------------------
    # BOTTOM RIGHT: Helper functions  (y ~ 3.5)
    # ----------------------------------------------------------
    section_bg(ax, 13, 2.5, 12.5, 3.5, 'val', 'HELPER FUNCTIONS')

    box(ax, 16, 4.8, 4, 1.0,
        ['hill_estimator(x, k)', 'lines 1095–1102', 'Tail index + SE'],
        'val', fontsize=10, bold_line=0)
    box(ax, 21.5, 4.8, 5, 1.0,
        ['compute_transition_matrix()', 'lines 1117–1129', 'Quintile transition probs'],
        'val', fontsize=10, bold_line=0)
    box(ax, 16, 3.3, 4, 0.8,
        ['abl_passes() (1762–1768)'],
        'ext', fontsize=9)
    box(ax, 21.5, 3.3, 5, 0.8,
        ['Called by ablation + sensitivity'],
        'ext', fontsize=9)

    # Pub diagnostics -> helpers (curved right to avoid crossing center)
    arrow(ax, 11, 17.25, 16, 5.3, PAL['val'][1], lw=1.5, rad=-0.3)
    arrow(ax, 11, 17.25, 21.5, 5.3, PAL['val'][1], lw=1.5, rad=-0.25)
    # Ablation -> abl_passes
    arrow(ax, 16, 17.25, 16, 3.7, PAL['ext'][1], lw=1.2, rad=0.4)

    # ----------------------------------------------------------
    # Legend
    # ----------------------------------------------------------
    legend_y = 1.2
    cats = [('data', 'Data'), ('est', 'Estimation'), ('sim', 'Simulation'),
            ('cal', 'Calibration'), ('val', 'Validation'), ('ext', 'Ablation/Sens'),
            ('lib', 'External Libs')]
    for i, (cat, label) in enumerate(cats):
        fc, ec = PAL[cat]
        xx = 1.5 + i * 3.3
        ax.add_patch(mpatches.FancyBboxPatch(
            (xx, legend_y), 0.5, 0.35, boxstyle="round,pad=0.04",
            facecolor=fc, edgecolor=ec, linewidth=1.2))
        ax.text(xx + 0.7, legend_y + 0.17, label, fontsize=10, va='center',
                color=ec, fontweight='bold')

    fig.savefig('/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/v42_call_graph.png',
                dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print("Saved: v42_call_graph.png")


# ============================================================
if __name__ == '__main__':
    make_architecture_figure()
    make_call_graph_figure()
    print("Done.")
