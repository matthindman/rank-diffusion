from __future__ import annotations

from pathlib import Path

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap


def plot_core_diagnostics(bundle, score: dict[str, object], out_dir: str | Path, prefix: str) -> Path:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    emp = bundle.empirical
    mc = score["mc_stats"]

    fig, axes = plt.subplots(2, 2, figsize=(12, 9))

    ax = axes[0, 0]
    vr_lags = sorted(emp["vr_emp"].keys())
    ax.plot(vr_lags, [emp["vr_emp"][k] for k in vr_lags], "ko-", label="Emp")
    ax.plot(vr_lags, [mc.get(f"vr{k}", {}).get("mean", np.nan) for k in vr_lags], "rs--", label="Sim")
    ax.set_title("Variance Ratio")
    ax.grid(True, alpha=0.3)
    ax.legend()

    ax = axes[0, 1]
    racf_lags = sorted(emp["racf_emp"].keys())
    x = np.arange(len(racf_lags))
    ax.bar(x - 0.15, [emp["racf_emp"][k] for k in racf_lags], width=0.3, color="black", alpha=0.7, label="Emp")
    ax.bar(x + 0.15, [mc.get(f"racf{k}", {}).get("mean", np.nan) for k in racf_lags], width=0.3, color="red", alpha=0.7, label="Sim")
    ax.set_xticks(x)
    ax.set_xticklabels(racf_lags)
    ax.set_title("Rank ACF")
    ax.grid(True, alpha=0.3)
    ax.legend()

    ax = axes[1, 0]
    r2_horizons = sorted(emp["xr2_emp"].keys())
    ax.plot(r2_horizons, [emp["xr2_emp"][k] for k in r2_horizons], "ko-", label="Emp")
    ax.plot(r2_horizons, [mc.get(f"xr2_{k}", {}).get("mean", np.nan) for k in r2_horizons], "rs--", label="Sim")
    ax.set_title("Cross-Sectional R2")
    ax.grid(True, alpha=0.3)
    ax.legend()

    ax = axes[1, 1]
    pers_horizons = sorted(emp["pers_emp"].keys())
    ax.plot(pers_horizons, [emp["pers_emp"][k] for k in pers_horizons], "ko-", label="Emp")
    ax.plot(pers_horizons, [mc.get(f"pers{k}", {}).get("mean", np.nan) for k in pers_horizons], "rs--", label="Sim")
    ax.set_title(f"Top-{emp['top_k']} Persistence")
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.suptitle(f"{bundle.platform.title()} RankDiff v43 | {score['n_pass']}/{score['n_total']}")
    fig.tight_layout()
    file_path = out_path / f"{prefix}_v43_diagnostics.png"
    fig.savefig(file_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return file_path


def plot_ablation(results: list[dict], out_dir: str | Path, prefix: str) -> Path:
    from .ablation import DIAG_KEYS, DIAG_NAMES

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    n_lvls = len(results)
    n_diags = len(DIAG_NAMES)

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), gridspec_kw={"width_ratios": [3, 1]})

    ax_h = axes[0]
    hm_data = np.zeros((n_lvls, n_diags))
    for i, res in enumerate(results):
        for j, key in enumerate(DIAG_KEYS):
            hm_data[i, j] = 1.0 if res["pass_fail"][key] else 0.0

    cmap = ListedColormap(["#d32f2f", "#4caf50"])
    ax_h.imshow(hm_data, aspect="auto", cmap=cmap, vmin=0, vmax=1, interpolation="nearest")
    ax_h.set_xticks(range(n_diags))
    ax_h.set_xticklabels(DIAG_NAMES, rotation=45, ha="right", fontsize=8)
    ax_h.set_yticks(range(n_lvls))
    ax_h.set_yticklabels([r["level"]["short"] for r in results], fontsize=9)
    for i in range(n_lvls):
        for j in range(n_diags):
            ax_h.text(j, i, "Y" if hm_data[i, j] > 0.5 else "N",
                      ha="center", va="center", fontsize=7, fontweight="bold", color="white")
        ax_h.text(n_diags + 0.3, i, f"{results[i]['n_pass']}/{results[i]['n_total']}",
                  ha="left", va="center", fontsize=9, fontweight="bold")
    ax_h.set_title("Ablation: Diagnostic Pass/Fail by Model Level", fontsize=12, pad=10)
    ax_h.set_xlabel("Diagnostic")
    ax_h.set_ylabel("Model Level (cumulative features)")

    ax_s = axes[1]
    scores = [r["n_pass"] for r in results]
    max_score = results[0]["n_total"] if results else 15
    colors = ["#4caf50" if s == max_score else "#ff9800" if s >= max_score - 3 else "#d32f2f" for s in scores]
    ax_s.barh(range(n_lvls), scores, color=colors, height=0.7, edgecolor="white", linewidth=0.5)
    ax_s.set_yticks(range(n_lvls))
    ax_s.set_yticklabels([""] * n_lvls)
    ax_s.set_xlim(0, max_score + 1)
    ax_s.set_xlabel(f"Diagnostics Passing (/{max_score})")
    ax_s.set_title("Score", fontsize=12, pad=10)
    ax_s.axvline(x=max_score, color="green", linestyle="--", alpha=0.5, linewidth=1)
    for i, s in enumerate(scores):
        ax_s.text(s + 0.2, i, str(s), ha="left", va="center", fontsize=9)
    ax_s.invert_yaxis()
    axes[0].invert_yaxis()

    plt.tight_layout()
    file_path = out_path / f"{prefix}_v43_ablation.png"
    fig.savefig(file_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return file_path


def plot_sensitivity(
    results: dict[str, dict[float, dict]],
    deltas: tuple[float, ...],
    out_dir: str | Path,
    prefix: str,
) -> Path:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    param_names = list(results.keys())
    n_params = len(param_names)
    ncols = min(3, n_params)
    nrows = (n_params + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(5.5 * ncols, 5 * nrows))
    if n_params == 1:
        axes = np.array([[axes]])
    elif nrows == 1:
        axes = axes[np.newaxis, :]
    axes_flat = axes.flatten()

    for pi, pname in enumerate(param_names):
        ax = axes_flat[pi]
        scores = [results[pname][d]["n_pass"] for d in deltas]
        max_score = results[pname][deltas[0]]["n_total"]
        pct_labels = [f"{d * 100:+.0f}%" for d in deltas]
        colors = ["#4caf50" if s == max_score else "#ff9800" if s >= max_score - 3 else "#d32f2f" for s in scores]
        ax.bar(range(len(deltas)), scores, color=colors, edgecolor="white", linewidth=0.5)
        ax.set_xticks(range(len(deltas)))
        ax.set_xticklabels(pct_labels, fontsize=9)
        ax.set_ylim(0, max_score + 1)
        ax.axhline(y=max_score, color="green", linestyle="--", alpha=0.5, linewidth=1)
        base_val = results[pname][0.0]["value"] if 0.0 in results[pname] else 0.0
        ax.set_title(f"{pname} (base={base_val:.4f})", fontsize=10)
        ax.set_ylabel(f"Score (/{max_score})")
        for i, s in enumerate(scores):
            ax.text(i, s + 0.3, str(s), ha="center", fontsize=8)

    for pi in range(n_params, len(axes_flat)):
        axes_flat[pi].set_visible(False)

    fig.suptitle("Parameter Sensitivity: Score vs Perturbation", fontsize=13, y=1.01)
    plt.tight_layout()
    file_path = out_path / f"{prefix}_v43_sensitivity.png"
    fig.savefig(file_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return file_path
