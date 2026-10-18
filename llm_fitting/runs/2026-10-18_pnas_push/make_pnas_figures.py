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
    a.text(-0.25, 1.02, s, transform=a.transAxes, fontsize=9, fontweight="bold", va="bottom")


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


def fig4():
    CF = {pn: load(HERE / f"churn_counterfactual__{pn}.json") for pn in ("reddit_comments_ext", "reddit_submissions_long")}
    for pn in CF:                                   # the flat start at 20 seeds with bands (R3-C), where available
        f20 = HERE / f"churn_counterfactual__{pn}__flat20.json"
        if f20.exists():
            CF[pn]["flat_start"] = load(f20)["flat_start"]
    gates = {}
    for pn in ("reddit_submissions_long", "reddit_comments_ext"):
        for arm in ("home", "movinghome", "movinghome_vp", "movinghome_nodrift", "homedrift", "movinghome_vp_nodrift"):
            f = HERE / f"gate_{arm}__{pn}.json"
            if f.exists():
                gates[(pn, arm)] = load(f)
    fig, axs = plt.subplots(2, 2, figsize=(W2 * 0.82, 4.6))
    ax = axs.ravel()
    a = ax[0]
    for pn, d in CF.items():
        traj = np.array(d["flat_start"]["shape_by_year"])
        a.plot(np.arange(traj.shape[0]), traj[:, 0], "-", color=COL[pn], lw=1.2, label=LAB[pn])
        if "shape_q" in d["flat_start"]:
            lo, hi = np.array(d["flat_start"]["shape_q"][0]), np.array(d["flat_start"]["shape_q"][1])
            a.fill_between(np.arange(traj.shape[0]), lo[:, 0], hi[:, 0], color=COL[pn], alpha=0.2, lw=0)
    a.set_xlabel("years from complete equality")
    a.set_ylabel("shape at K/64 (log units)")
    a.set_title("Churn builds a hierarchy from equality")
    a.legend(loc="lower right", frameon=False)
    tag(a, "A")
    a = ax[1]
    for pn, d in CF.items():
        f = d["flat_start"]
        if "S_end_q" in f:
            se = np.array(f["S_end"])
            a.errorbar(f["S_star_sim"], se, yerr=[se - np.array(f["S_end_q"][0]), np.array(f["S_end_q"][1]) - se], fmt="none",
                       ecolor=COL[pn], lw=0.8, capsize=1.5)
        a.scatter(f["S_star_sim"], f["S_end"], s=14, marker=MK[pn], color=COL[pn], label=f"{LAB[pn]}: model")
        a.scatter(f["S_star_sim"], f["S_observed"], s=14, marker=MK[pn], facecolor="white", edgecolor=COL[pn], label="observed ladder")
    lim = [0, 2.4]
    a.plot(lim, lim, color=GREY, lw=0.8)
    a.set_xlim(lim)
    a.set_ylim(lim)
    a.set_xlabel("churn over slide, σ²/(2 s̄)")
    a.set_ylabel("local slope S")
    a.set_title("Its steepness is churn over slide")
    a.legend(loc="upper left", frameon=False, fontsize=5.5)
    tag(a, "B")
    a = ax[2]                                       # R6: 30 years from the observed ladder, 20 seeds, 90% bands
    LS = {"churn_slide": ("-", "churn + slide"), "slide_only": ("--", "slide only (no churn)"), "churn_only": (":", "churn only (no slide)")}
    for pn in CF:
        f = HERE / f"churn_counterfactual__{pn}__long30.json"
        if not f.exists():
            continue
        L30 = load(f)
        for wk, (ls, lab) in LS.items():
            y = np.array(L30[wk]["shape_by_year"])[:, 0]
            lo, hi = (np.array(q)[:, 0] for q in L30[wk]["shape_q"])
            yrs = np.arange(y.size)
            a.plot(yrs, y, ls, color=COL[pn], lw=1.1, label=lab if pn == "reddit_submissions_long" else None)
            a.fill_between(yrs, lo, hi, color=COL[pn], alpha=0.15, lw=0)
    a.set_xlabel("years from the observed 2019 ladder")
    a.set_ylabel("shape at K/64 (log units)")
    a.set_title("Thirty years without churn, or without slide")
    a.legend(loc="upper left", frameon=False, fontsize=5.5)
    tag(a, "C")
    a = ax[3]
    arms = (("home", "fixed\nhome"), ("homedrift", "fixed\n+drift"), ("movinghome_vp_nodrift", "wander"),
            ("movinghome_vp", "wander\n+drift"), ("movinghome", "first\nfrozen"))
    for j, pn in enumerate(("reddit_submissions_long", "reddit_comments_ext")):
        for i, (arm, lab) in enumerate(arms):
            g = gates.get((pn, arm))
            if g is None:
                continue
            a.errorbar(i + (j - 0.5) * 0.25, g["model_rel"], yerr=g["model_rel_sd"], fmt=MK[pn], color=COL[pn], ms=4, capsize=2,
                       label=LAB[pn] if i == 0 else None)
        b = next((gates[(pn, arm)] for arm, _ in arms if (pn, arm) in gates), None)
        if b is not None:
            a.axhline(b["base_rel"], color=COL[pn], lw=0.8, ls=":")
    a.set_xticks(range(len(arms)))
    a.set_xticklabels([lab for _, lab in arms], fontsize=5.5)
    a.set_ylabel("held-out movement error")
    a.set_title("Held-out movement, long panels")
    a.legend(loc="upper left", frameon=False)
    tag(a, "D")
    fig.tight_layout(w_pad=1.5, h_pad=1.6)
    save(fig, "fig4_model")


def fig5():
    gm = load(R / "2026-10-18_momentum" / "gapped_momentum.json")
    fig, ax = plt.subplots(1, 3, figsize=(W2, 2.3))
    a = ax[0]
    for pn in COL:
        d = gm[pn]["gapped"]
        gs = sorted(d, key=lambda x: int(x))
        a.plot([int(g) for g in gs], [d[g]["mean"] for g in gs], "-", marker=MK[pn], color=COL[pn], ms=3.5, lw=1,
               label={"reddit_comments_ext": "Reddit comments", "reddit_submissions_long": "Reddit submissions", "facebook_a": "Facebook (4-week windows)"}[pn])
    for n, (w, lab, col, ls) in enumerate(WORLDS):
        nw = load(HERE / f"null_world__{w}.json")["gapped"]
        a.plot([13, 26], [nw["13"], nw["26"]], "x", color="#666666", ms=4, mew=1, label="simulated worlds" if n == 0 else None)
    syn = gm["synthetic"]
    a.plot([0, 13, 26], [syn["i"]["gapped"][g] for g in ("0", "13", "26")], "--", color=GREY, lw=1, label="random walk + noise")
    a.axhline(0, color=GREY, lw=0.4)
    a.set_xlabel("gap between the two changes (weeks)")
    a.set_ylabel("rank correlation of changes")
    a.set_title("Changes persist on Reddit")
    a.legend(loc="upper center", frameon=False, fontsize=4.6, ncol=2, columnspacing=0.5, handlelength=1.3, handletextpad=0.3)
    a.set_ylim(-0.15, 0.39)
    a.set_yticks([-0.1, 0, 0.1, 0.2])
    tag(a, "A")
    a = ax[1]
    cats = ("activity", "items", "per\nitem", "later\norigins", "net of\ntrend")
    w = 0.38
    for j, pn in enumerate(("reddit_submissions_long", "reddit_comments_ext")):
        f = HERE / f"momentum_decomposition__{pn}.json"
        f2 = HERE / f"persistence_r3__{pn}.json"
        if not (f.exists() and f2.exists()):
            continue
        d, d2 = load(f), load(f2)["full_membership"]["factor"]
        g = "13"
        vals = [d["D6"]["x"][g], d["D6"]["items"][g], d["D6"]["per_item"][g], d2["raw_matched"][g], d2["platform_total"][g]]
        a.bar(np.arange(5) + (j - 0.5) * w, vals, w, color=COL[pn], label=LAB[pn])
    a.axvline(2.5, color=GREY, lw=0.5, ls=":")
    a.axhline(0, color=GREY, lw=0.6)
    a.set_xticks(range(5))
    a.set_xticklabels(cats, fontsize=5.5)
    a.set_ylabel("gapped momentum (13-wk gap)")
    a.set_title("Carried by participation")
    a.set_ylim(0, 0.235)
    a.legend(loc="upper center", frameon=False, ncol=2, fontsize=5.5)
    tag(a, "B")
    a = ax[2]
    gd = load(HERE / "gomez_decomposition.json")
    labs = []
    x = 0
    for pn, k in (("reddit_submissions_long", 78), ("reddit_comments_ext", 195)):
        rows = [r for r in gd[pn] if r["k"] == k]
        for r in rows:
            a.bar(x, r["within"], 0.7, color=COL[pn], alpha=0.55)
            a.bar(x, r["between"], 0.7, color=COL[pn])
            a.plot(x, r["share_change"], "k_", ms=9, mew=1.5)
            labs.append(f"{2019 + r['pair'][0]}–{(19 + r['pair'][1]) % 100:02d}")
            x += 1
        x += 0.6
    a.axhline(0, color=GREY, lw=0.6)
    a.set_xticks([0, 1, 2, 3.6, 4.6, 5.6])
    a.set_xticklabels(labs, fontsize=5.5, rotation=30)
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    hand = [Patch(color="#999999", alpha=0.55, label="within (members)"), Patch(color="#555555", label="displacement"),
            Line2D([], [], color="k", marker="_", ls="", ms=9, mew=1.5, label="share change")]
    a.text(1, 0.095, "submissions\ntop 78", ha="center", fontsize=5.5, color=COL["reddit_submissions_long"])
    a.text(4.6, 0.095, "comments\ntop 195", ha="center", fontsize=5.5, color=COL["reddit_comments_ext"])
    a.set_ylim(-0.30, 0.145)
    a.set_ylabel("relative change of the share a year")
    a.set_title("Displacement steady; members fall")
    a.legend(handles=hand, loc="lower right", frameon=False, fontsize=5.5)
    tag(a, "C")
    fig.tight_layout(w_pad=1.0)
    save(fig, "fig5_tipping")


WORLDS = (("gibrat", "size-blind random growth", "#444444", "--"), ("atlas", "firm-like (β = 0.2)", "#8e44ad", ":"),
          ("neutral", "neutral copying", "#b5651d", "-."), ("rdm", "the model", "#999999", "-"))
# R7 (20 runs each): worlds started from the data's ladder (calibrated) and from a constant-slope Zipf ladder
WORLDS7 = (("sb_data", "size-blind, data's ladder", "#444444", "--"), ("firm_data", "firm-like (β 0.2), data's ladder", "#8e44ad", "--"),
           ("gibrat", "size-blind, constant slope", "#444444", ":"), ("atlas", "firm-like, constant slope", "#8e44ad", ":"),
           ("neutral", "neutral copying", "#b5651d", "-."))


def r7():
    f = HERE / "null_worlds_r7_summary.json"
    return load(f) if f.exists() else {}


def fig2v2():
    """Size-blindness and its consequence (R3-A): data against null worlds."""
    rob = load(HERE / "exponent_robustness.json")
    kin = load(HERE / "turnover_kinematics.json")
    rc = load(HERE / "rank_change_data.json")
    NW = {w: load(HERE / f"null_world__{w}.json") for w, *_ in WORLDS}
    fig, ax = plt.subplots(1, 3, figsize=(W2, 2.35))
    a = ax[0]
    for pn in COL:
        key = "1->2" if pn != "facebook_a" else "1->5"
        d = rob[pn]["loo"][key]
        a.plot(d["size"], d["sd"], "-", marker=MK[pn], color=COL[pn], ms=3.5, lw=1, label=f"{LAB[pn]}: β = {d['full']:.2f}")
    xs = np.linspace(6.5, 14.5, 10)
    a.plot(xs, 0.67 * np.exp(-0.2 * (xs - 6.5)), "--", color=GREY, lw=1, label="firms, β = 0.2")
    a.set_yscale("log")
    a.set_ylim(0.12, 1.3)
    a.set_yticks([0.15, 0.2, 0.3, 0.5, 0.7, 1.0])
    a.set_yticklabels(["0.15", "0.2", "0.3", "0.5", "0.7", "1"])
    a.minorticks_off()
    a.set_xlabel("size (mean log activity of the stratum)")
    a.set_ylabel("sd of one-year change (log)")
    a.set_title("Size gives limited protection")
    a.legend(loc="lower left", frameon=False, fontsize=5.5)
    tag(a, "A")
    a = ax[1]
    for pn in ("reddit_submissions_long", "reddit_comments_ext"):
        rows = kin[pn]
        ks = sorted({r["k"] for r in rows})
        ys = [np.mean([r["obs"] for r in rows if r["k"] == k]) for k in ks]
        xk = [10, 25] + ks
        yk = [1 - np.mean(rob[pn]["retention"]["10"]), 1 - np.mean(rob[pn]["retention"]["25"])] + ys
        a.plot(xk, yk, "-", marker=MK[pn], color=COL[pn], ms=3.5, lw=1.2, label=LAB[pn] + " (data)")
    S7 = r7()
    for w, lab, col, ls in WORLDS7:
        if w not in S7:
            continue
        t = S7[w]["turnover"]
        kk = sorted(int(k) for k in t)
        a.plot(kk, [t[str(k)]["mean"] for k in kk], ls, color=col, lw=1, label=lab)
        if w in ("sb_data", "firm_data"):
            a.fill_between(kk, [t[str(k)]["q05"] for k in kk], [t[str(k)]["q95"] for k in kk], color=col, alpha=0.12, lw=0)
    a.set_xscale("log")
    a.set_ylim(0, 0.66)
    a.set_yticks([0, 0.1, 0.2, 0.3, 0.4])
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("share replaced in a year")
    a.set_title("Fastest at the top, on this ladder")
    a.legend(loc="upper center", frameon=False, fontsize=4.6, ncol=2, columnspacing=0.6, handlelength=1.6, bbox_to_anchor=(0.5, 1.02))
    tag(a, "B")
    a = ax[2]
    for pn in COL:
        d = rc[pn]
        rr = sorted(int(r) for r in d)
        a.plot(rr, [d[str(r)] for r in rr], "-", marker=MK[pn], color=COL[pn], ms=3, lw=1.2, label=LAB[pn])
    for w, lab, col, ls in WORLDS7:
        if w not in S7:
            continue
        d = S7[w]["C_R"]
        rr = sorted(int(r) for r in d)
        a.plot(rr, [d[str(r)]["mean"] for r in rr], ls, color=col, lw=1)
    a.set_xscale("log")
    a.set_ylim(-0.02, 1.05)
    a.set_xlabel("rank R")
    a.set_ylabel("weekly change of occupant")
    a.set_title("On Reddit, rank one rarely changes")
    a.legend(loc="lower right", frameon=False, fontsize=5.5)
    tag(a, "C")
    fig.tight_layout(w_pad=1.2)
    save(fig, "fig2_size_blind")


def fig3v2():
    """Turnover replenishes the hierarchy: the boundary test, crossings against the slide with null-world means, and
    turnover decomposed into churn and slope (Eq. 1 as a decomposition)."""
    NW = {w: load(HERE / f"null_world__{w}.json") for w, *_ in WORLDS}
    kin = load(HERE / "turnover_kinematics.json")
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
    S7 = r7()

    def _b(w):
        b = S7.get(w, {}).get("boundary_pooled")
        return f"{b['mean']:.2f}" if b else "n/a"
    a.text(0.4, 13.6, f"pooled ratio: data 0.94 (0.86-1.04)\nrandom growth: {_b('sb_data')}, {_b('firm_data')}, {_b('gibrat')}, {_b('atlas')}\n"
           f"jumps to random ranks 1.56\nneutral copying {_b('neutral')} (universe edge)", ha="left", va="top", fontsize=5.2, color="#333333")
    a.set_xlim(0, 14)
    a.set_ylim(0, 14)
    a.set_xlabel("predicted from churn: σ²/2S (per 1,000 wk)")
    a.set_ylabel("measured crossing gain")
    a.set_title("Crossings match churn")
    a.legend(loc="lower right", frameon=False)
    tag(a, "A")
    a = ax[1]
    lim = [0, 12]
    a.plot(lim, lim, color=GREY, lw=0.8)
    for pn in ("reddit_comments_ext", "reddit_submissions_long"):            # half-year starts: the cells the text quotes
        d = load(R / "2026-10-14_balance_law_halfyear" / "cells" / f"law__{pn}.json")
        K = d["K"]
        for q in d["pairs"]:
            rows = [r for r in q["rows"] if r["k"] < K]
            c = np.array([r["c"] for r in rows]) * 1000
            sb = np.array([r["sbar"] for r in rows]) * 1000
            k = np.array([r["k"] for r in rows])
            a.scatter(sb, c, s=6 + 30 * (np.log(K / k) / np.log(64)), marker=MK[pn], color=COL[pn], alpha=0.75, lw=0)
        a.plot([], [], MK[pn], color=COL[pn], ms=3.5, label=LAB[pn])
    for w, lab, col, ls in WORLDS7:
        o = S7.get(w)
        if not o or "law_c" not in o:
            continue
        x, y = 1000 * o["law_sbar"]["mean"], 1000 * o["law_c"]["mean"]
        a.errorbar([x], [y], xerr=[[x - 1000 * o["law_sbar"]["q05"]], [1000 * o["law_sbar"]["q95"] - x]],
                   yerr=[[y - 1000 * o["law_c"]["q05"]], [1000 * o["law_c"]["q95"] - y]], fmt="X", color=col, ms=5,
                   mec="white", mew=0.4, elinewidth=0.6, label=lab, alpha=0.9 if ls == "--" else 0.55)
    a.text(11.6, 0.3, "slide wins: flattens", fontsize=5.5, color=GREY, ha="right")
    a.text(0.4, 10.8, "crossings win: steepens", fontsize=5.5, color=GREY, ha="left")
    a.set_xlim(lim)
    a.set_ylim(0, 11.5)
    a.set_xlabel("slide of the names above k (per 1,000 wk)")
    a.set_ylabel("crossing gain σ²/2S (per 1,000 wk)")
    a.set_title("Crossings return most of the slide")
    a.legend(loc="upper right", frameon=False, fontsize=4.6, bbox_to_anchor=(1.03, 0.93), handletextpad=0.3)
    tag(a, "B")
    a = ax[2]
    for pn in COL:
        rows = kin[pn]
        ks = sorted({r["k"] for r in rows})
        e = [np.mean([r["e"] for r in rows if r["k"] == k]) for k in ks]
        S = [np.mean([r["S"] for r in rows if r["k"] == k]) for k in ks]
        a.plot(ks, e, "-", marker=MK[pn], color=COL[pn], ms=3, lw=1)
        a.plot(ks, S, ":", marker=MK[pn], color=COL[pn], ms=3, lw=1, mfc="white")
    a.plot([], [], "-", color="k", label="churn: mean downward move at the boundary")
    a.plot([], [], ":", color="k", mfc="white", marker="o", ms=3, label="local slope of the ladder")
    a.set_xscale("log")
    a.set_ylim(0, 2.6)
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("log units")
    a.set_title("The two ingredients of turnover (Eq. 1)")
    a.legend(loc="upper left", frameon=False, fontsize=5.5)
    tag(a, "C")
    fig.tight_layout(w_pad=1.2)
    save(fig, "fig3_replenish")


if __name__ == "__main__":
    which = sys.argv[1:] or ["fig1", "fig2v2", "fig3v2", "fig4", "fig5"]
    for f in which:
        globals()[f]()
        print("drew", f)
