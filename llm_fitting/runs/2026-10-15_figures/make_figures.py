"""Core figures for the paper's movement-to-structure argument, from saved run outputs only
(no data file opened): MODEL_STATUS 2z-cg, 2z-cm, 2z-co.

    python llm_fitting/runs/2026-10-15_figures/make_figures.py
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

R = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
PAN = {"reddit_comments_ext": "Reddit comments (K = 12,500)", "reddit_submissions_long": "Reddit submissions (K = 5,000)"}
BLUE, ORANGE, GREY = "#1f5fa8", "#d9772b", "#777777"


def load(p):
    return json.loads(Path(p).read_text())


def fig_boundary():
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.6))
    for i, pn in enumerate(PAN):
        b = load(R / "2026-10-14_boundary_law" / "cells" / f"boundary__{pn}.json")["pooled"]
        v = load(R / "2026-10-15_variogram" / "cells" / f"vario__{pn}.json")["pooled"]["cells"]
        k = np.array([r["k"] for r in b])
        a = ax[i]
        R_ = np.array([r["R_lasting"] for r in b]) * 1000
        Rq = np.array([r["R_lasting_q"] for r in b]) * 1000
        c1 = np.array([r["c"] for r in b]) * 1000
        c1q = np.array([r["c_q"] for r in b]) * 1000
        c2 = np.array([r["c"] for r in v]) * 1000
        c2q = np.array([r["c_q"] for r in v]) * 1000
        a.errorbar(k, R_, yerr=[R_ - Rq[:, 0], Rq[:, 1] - R_], fmt="o-", color="k", capsize=3, label="measured crossings (lasting)")
        a.errorbar(k * 0.93, c1, yerr=[c1 - c1q[:, 0], c1q[:, 1] - c1], fmt="s--", color=BLUE, capsize=3, label="σ²/2S, variance route")
        a.errorbar(k * 1.07, c2, yerr=[c2 - np.clip(c2q[:, 0], 0, None), c2q[:, 1] - c2], fmt="^:", color=ORANGE, capsize=3, label="σ²/2S, variogram")
        a.set_xscale("log")
        a.set_xlabel("rank k (cut-off)")
        a.set_ylabel("gain per member per 1,000 weeks (log units)")
        a.set_title(PAN[pn])
        a.set_ylim(0, 12 if pn == "reddit_submissions_long" else 8)
        a.legend(fontsize=8, loc="upper right")
    a = ax[2]
    for pn, col in zip(PAN, (BLUE, ORANGE)):
        s = load(R / "2026-10-14_boundary_law" / f"by_start__{pn}.json")
        c = np.array([r["c"] for st in s["starts"] for r in st["rows"]]) * 1000
        x = np.array([r["R"] for st in s["starts"] for r in st["rows"]]) * 1000
        cc = np.corrcoef(c, x)[0, 1]
        a.scatter(c, x, color=col, s=28, label=f"{PAN[pn].split(' (')[0]}: r = {cc:.2f}")
    lim = [0, 10]
    a.plot(lim, lim, color=GREY, lw=1)
    a.set_xlim(lim)
    a.set_ylim(lim)
    a.set_xlabel("predicted σ²/2S (variance route, own start)")
    a.set_ylabel("measured crossings")
    a.set_title("By start, 2020–2022 (3 starts × 7 ranks)")
    a.legend(fontsize=8, loc="upper left")
    fig.suptitle("Crossings at a rank's boundary return lasting variance over twice the local slope", fontsize=12)
    fig.tight_layout()
    fig.savefig(HERE / "fig_boundary_theory.png", dpi=150)


def fig_law_and_head():
    fig, ax = plt.subplots(1, 2, figsize=(11, 4.6))
    a = ax[0]
    for pn, col in zip(PAN, (BLUE, ORANGE)):
        res = load(R / "2026-10-14_balance_law_halfyear" / "cells" / f"law__{pn}.json")
        K = res["K"]
        p = np.array([r["pred"] for q in res["pairs"] for r in q["rows"] if r["k"] < K]) * 1000
        o = np.array([r["obs_P"] for q in res["pairs"] for r in q["rows"] if r["k"] < K]) * 1000
        a.scatter(p, o, color=col, s=24, label=PAN[pn].split(" (")[0])
    lim = [-9, 4]
    a.plot(lim, lim, color=GREY, lw=1)
    a.axhline(0, color=GREY, lw=0.5)
    a.axvline(0, color=GREY, lw=0.5)
    a.set_xlim(lim)
    a.set_ylim(lim)
    a.set_xlabel("predicted: σ²/2S − members' slide (per 1,000 weeks)")
    a.set_ylabel("observed change of the ladder's shape")
    a.set_title("The rate law, same-year form (60 cells; 2z-cg)")
    a.legend(fontsize=8, loc="upper left")
    a = ax[1]
    s = load(R / "2026-10-14_boundary_law" / "by_start__reddit_submissions_long.json")
    # the difference of spacings measures crossings from the middle of block a+2 to the middle of block a+4
    labs = ["Oct 2020–Sep 2021\n(June 2021 in its last quarter)", "Apr 2021–Mar 2022\n(June 2021 in its first quarter)",
            "Oct 2021–Sep 2022\n(entirely after)"]
    for i, (kk, col) in enumerate(((78, ORANGE), (156, BLUE), (1250, GREY))):
        y = [next(r for r in st["rows"] if r["k"] == kk) for st in s["starts"]]
        v = np.array([r["R"] for r in y]) * 1000
        q = np.array([r["R_q"] for r in y]) * 1000
        x = np.arange(3) + (i - 1) * 0.12
        a.errorbar(x, v, yerr=[v - q[:, 0], q[:, 1] - v], fmt="o-", color=col, capsize=3, label=f"top {kk}")
    a.set_xticks(range(3))
    a.set_xticklabels(labs, fontsize=8)
    a.set_ylabel("crossings per member per 1,000 weeks")
    a.set_title("Crossings at the head of submissions (2z-cm)")
    a.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(HERE / "fig_rate_law_and_head_mobility.png", dpi=150)


if __name__ == "__main__":
    fig_boundary()
    fig_law_and_head()
    print("written:", *sorted(p.name for p in HERE.glob("*.png")))
