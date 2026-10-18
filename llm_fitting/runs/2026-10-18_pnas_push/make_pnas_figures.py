"""Main-text figures for the manuscript (frame d), from saved run outputs only; no data file is opened.

    python llm_fitting/runs/2026-10-18_pnas_push/make_pnas_figures.py [fig1 fig2 ...]

Fig 1  the puzzle: a slowly changing ladder, moving names, turnover at every scale (2z-cu; 2z-dg A7-5)
Fig 2  size gives little protection, so turnover is scale-free (2z-dc; 2z-dg A1, A7)
Fig 3  churn sustains the hierarchy: crossings predicted from churn, the balance with the slide, the rate law
Fig 4  the generative model's core: counterfactuals and held-out movement (2z-dg A3, A5)
Fig 5  where the balance tips: persistent growth and the falling head (2z-df; 2z-dg A4, A8)
"""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

R = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
plt.rcParams.update({"font.size": 7, "axes.titlesize": 7.5, "axes.labelsize": 7, "legend.fontsize": 6, "xtick.labelsize": 6.5,
                     "ytick.labelsize": 6.5, "axes.spines.top": False, "axes.spines.right": False, "font.family": "sans-serif",
                     "pdf.fonttype": 42})
COL = {"reddit_comments_ext": "#1f5fa8", "reddit_submissions_long": "#d9772b", "facebook_a": "#2a9d5c"}
LAB = {"reddit_comments_ext": "Reddit comments", "reddit_submissions_long": "Reddit submissions", "facebook_a": "Facebook pages"}
MK = {"reddit_comments_ext": "o", "reddit_submissions_long": "s", "facebook_a": "^"}
GREY = "#777777"
W2 = 7.0                                            # PNAS two-column width (inches)


def load(p):
    return json.loads(Path(p).read_text())


def tag(a, s):
    a.text(-0.22, 1.02, s, transform=a.transAxes, fontsize=9, fontweight="bold", va="bottom")


def save(fig, name):
    for ext in ("pdf", "png"):
        fig.savefig(HERE / f"{name}.{ext}", dpi=300, bbox_inches="tight")
    plt.close(fig)


def fig1():
    P = {pn: load(R / "2026-10-15_puzzle" / f"puzzle__{pn}.json") for pn in COL}
    rob = load(HERE / "exponent_robustness.json")
    fig, ax = plt.subplots(1, 3, figsize=(W2, 2.3))
    a = ax[0]
    off = {"reddit_comments_ext": 0.0, "reddit_submissions_long": 4.0, "facebook_a": 8.0}
    for pn, d in P.items():
        n = len(d["ladders"])
        for i, l in enumerate(d["ladders"]):
            r = np.array(l["ranks"]) / d["K"]
            a.plot(r, np.array(l["level"]) + off[pn], color=COL[pn], alpha=0.3 + 0.7 * i / max(n - 1, 1), lw=1,
                   label=f"{LAB[pn]} ({n} {'years' if d['block'] == 52 else 'half-years'})" if i == n - 1 else None)
    a.set_xscale("log")
    a.set_xlabel("rank / K")
    a.set_ylabel("log activity relative to rank K (offset)")
    a.set_title("The hierarchy changes slowly")
    a.legend(loc="lower left", frameon=False)
    tag(a, "A")
    a = ax[1]
    d = P["reddit_comments_ext"]
    for path in d["paths"]["log10_rank"][:100]:
        y = np.array([np.nan if v is None else v for v in path], float)
        a.plot(np.arange(y.size) * d["paths"]["weeks_step"] / 52 + 2019, y, color=COL["reddit_comments_ext"], alpha=0.16, lw=0.5)
    a.invert_yaxis()
    a.set_xticks([2019, 2020, 2021, 2022, 2023])
    a.set_ylabel("log$_{10}$ weekly rank")
    a.set_title("Its names move (100 communities)")
    tag(a, "B")
    a = ax[2]
    for pn, d in P.items():
        ks = sorted(int(k) for k in d["turnover"])
        rep = [1 - d["turnover"][str(k)][0] for k in ks]            # replaced within one block
        extra = [(10, 1 - np.mean(rob[pn]["retention"]["10"])), (25, 1 - np.mean(rob[pn]["retention"]["25"]))]
        xs = [e[0] for e in extra] + ks
        ys = [e[1] for e in extra] + rep
        a.plot(xs, ys, "-", marker=MK[pn], color=COL[pn], ms=3.5, lw=1, label=LAB[pn] + (" (half-year)" if pn == "facebook_a" else ""))
    a.set_xscale("log")
    a.set_ylim(0, 0.45)
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("share replaced in a year")
    a.set_title("The top turns over fastest")
    a.legend(loc="upper right", frameon=False)
    tag(a, "C")
    fig.tight_layout(w_pad=1.2)
    save(fig, "fig1_puzzle")


def fig2():
    rob = load(HERE / "exponent_robustness.json")
    kin = load(HERE / "turnover_kinematics.json")
    fig, ax = plt.subplots(1, 3, figsize=(W2, 2.3))
    a = ax[0]
    for pn in COL:
        key = "1->2" if pn != "facebook_a" else "1->5"
        d = rob[pn]["loo"][key]
        sz, sd = np.array(d["size"]), np.array(d["sd"])
        a.plot(sz, sd, "-", marker=MK[pn], color=COL[pn], ms=3.5, lw=1, label=f"{LAB[pn]}: β = {d['full']:.2f}")
    xs = np.linspace(6.5, 14.5, 10)
    a.plot(xs, 0.67 * np.exp(-0.2 * (xs - 6.5)), "--", color=GREY, lw=1, label="firms, β = 0.2")
    a.set_yscale("log")
    a.set_ylim(0.12, 1.3)
    a.set_xlabel("size (mean log activity of the stratum)")
    a.set_ylabel("sd of one-year change (log)")
    a.set_title("Size gives little protection")
    a.legend(loc="lower left", frameon=False)
    tag(a, "A")
    a = ax[1]
    for pn in COL:
        o = np.array([r["obs"] for r in kin[pn]])
        p = np.array([r["pred_exp"] for r in kin[pn]])
        a.scatter(p, o, s=10, marker=MK[pn], color=COL[pn], label=LAB[pn], alpha=0.85, lw=0)
    lim = [0.1, 0.4]
    a.plot(lim, lim, color=GREY, lw=0.8)
    a.set_xlim(lim)
    a.set_ylim(lim)
    a.set_xlabel("predicted: E[1 − exp(−D⁺/S)]")
    a.set_ylabel("observed share replaced")
    a.set_title("Turnover from churn and slope")
    a.legend(loc="upper left", frameon=False)
    tag(a, "B")
    a = ax[2]
    for pn in COL:
        rows = kin[pn]
        ks = sorted({r["k"] for r in rows})
        e = [np.mean([r["e"] for r in rows if r["k"] == k]) for k in ks]
        S = [np.mean([r["S"] for r in rows if r["k"] == k]) for k in ks]
        a.plot(ks, e, "-", marker=MK[pn], color=COL[pn], ms=3, lw=1)
        a.plot(ks, S, ":", marker=MK[pn], color=COL[pn], ms=3, lw=1, mfc="white")
    a.plot([], [], "-", color="k", label="mean downward move at the boundary, E[D⁺]")
    a.plot([], [], ":", color="k", mfc="white", marker="o", ms=3, label="local slope of the ladder, S")
    a.set_xscale("log")
    a.set_ylim(0, 2.8)
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("log units")
    a.set_title("Churn is flat; the slope steepens")
    a.legend(loc="upper left", frameon=False)
    tag(a, "C")
    fig.tight_layout(w_pad=1.2)
    save(fig, "fig2_scale_free")


def fig3():
    sets = [("reddit_comments_ext", "boundary__reddit_comments_ext.json"), ("reddit_submissions_long", "boundary__reddit_submissions_long.json"),
            ("facebook_a", "boundary__facebook_a__block13.json")]
    fig, ax = plt.subplots(1, 3, figsize=(W2, 2.4))
    a = ax[0]
    xs = np.linspace(0, 14, 10)
    a.plot(xs, xs, color=GREY, lw=0.8)
    for pn, f in sets:
        d = load(R / "2026-10-14_boundary_law" / "cells" / f)
        K = d["K"]
        for r in d["pooled"]:
            c, cq = 1000 * r["c"], [1000 * v for v in r["c_q"]]
            x, xq = 1000 * r["R_lasting"], [1000 * v for v in r["R_lasting_q"]]
            small = r["k"] < K / 16 - 1 and pn != "reddit_comments_ext"
            a.errorbar(c, x, xerr=[[c - cq[0]], [cq[1] - c]], yerr=[[x - xq[0]], [xq[1] - x]], fmt=MK[pn], color=COL[pn],
                       mfc="white" if small else COL[pn], ms=3.5, capsize=1.5, lw=0.6)
        a.plot([], [], MK[pn], color=COL[pn], ms=3.5, label=LAB[pn])
    a.set_xlim(0, 14)
    a.set_ylim(0, 14)
    a.set_xlabel("predicted from churn: σ²/2S (per 1,000 wk)")
    a.set_ylabel("measured crossing gain")
    a.set_title("Churn predicts crossings")
    a.legend(loc="upper left", frameon=False)
    tag(a, "A")
    a = ax[1]
    lim = [0, 12]
    a.plot(lim, lim, color=GREY, lw=0.8)
    for pn in ("reddit_comments_ext", "reddit_submissions_long"):
        d = load(R / "2026-10-14_balance_law" / "cells" / f"law__{pn}.json")
        K = d["K"]
        for q in d["pairs"]:
            if not q["primary"]:
                continue
            rows = [r for r in q["rows"] if r["k"] < K]
            c = np.array([r["c"] for r in rows]) * 1000
            s = np.array([r["sbar"] for r in rows]) * 1000
            k = np.array([r["k"] for r in rows])
            a.scatter(s, c, s=6 + 30 * (np.log(K / k) / np.log(64)), marker=MK[pn], color=COL[pn], alpha=0.8, lw=0)
        a.plot([], [], MK[pn], color=COL[pn], ms=3.5, label=LAB[pn])
    a.text(10.2, 1.0, "slide wins:\nflattens", fontsize=6, color=GREY, ha="center")
    a.text(1.8, 6.8, "churn wins:\nsteepens", fontsize=6, color=GREY, ha="center")
    a.set_xlim(lim)
    a.set_ylim(0, 8)
    a.set_xlabel("slide of the names above k (per 1,000 wk)")
    a.set_ylabel("crossing gain σ²/2S (per 1,000 wk)")
    a.set_title("Crossings against the slide")
    a.legend(loc="upper right", frameon=False)
    tag(a, "B")
    a = ax[2]
    for pn in ("reddit_comments_ext", "reddit_submissions_long"):
        res = load(R / "2026-10-14_balance_law_halfyear" / "cells" / f"law__{pn}.json")
        K = res["K"]
        p = np.array([r["pred"] for q in res["pairs"] for r in q["rows"] if r["k"] < K]) * 1000
        o = np.array([r["obs_P"] for q in res["pairs"] for r in q["rows"] if r["k"] < K]) * 1000
        a.scatter(p, o, s=8, marker=MK[pn], color=COL[pn], label=LAB[pn], lw=0, alpha=0.85)
    lim = [-9, 4]
    a.plot(lim, lim, color=GREY, lw=0.8)
    a.axhline(0, color=GREY, lw=0.4)
    a.axvline(0, color=GREY, lw=0.4)
    a.set_xlim(lim)
    a.set_ylim(lim)
    a.set_xlabel("crossing gain − slide (per 1,000 wk)")
    a.set_ylabel("observed change of the shape")
    a.set_title("The imbalance is the change")
    a.legend(loc="upper left", frameon=False)
    tag(a, "C")
    fig.tight_layout(w_pad=1.2)
    save(fig, "fig3_balance")


if __name__ == "__main__":
    which = sys.argv[1:] or ["fig1", "fig2", "fig3"]
    for f in which:
        globals()[f]()
        print("drew", f)
