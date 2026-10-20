"""Main-text figures for manuscript v5 (2026-10-20), from saved run outputs only; no data file is opened.

    python llm_fitting/runs/2026-10-18_pnas_push/make_v5_figures.py [fig1v5 fig2v5 fig3v5 fig4v5]

Fig 1  the puzzle: the ladder and its three flows, yearly curves, turnover by k, leaders that keep rank and lose share
Fig 2  random growth measured rank by rank: time scales, crossings, the balance, and geometry under identical dynamics
Fig 3  the model built from the measured ingredients: from equality, three scenarios from 2019, the 2021-22 backtest
Fig 4  what homogeneous growth misses: the head's share against the calibrated forecast, items vs per item,
       persistence across gaps, and the slide by prior change (R12 heterogeneous worlds drawn when their outputs exist)
"""
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from make_pnas_figures import COL, GREY, LAB, MK, R, W2, WORLDS7, load, plt, r7, save, tag  # noqa: E402

PANELS2 = ("reddit_comments_ext", "reddit_submissions_long")


def _law(pn):
    """Mean over the primary year pairs (2020-21, 2021-22) of the balance-law cells, by k."""
    d = load(R / "2026-10-14_balance_law" / "cells" / f"law__{pn}.json")
    prs = [q for q in d["pairs"] if q["primary"]]
    ks = sorted(r["k"] for r in prs[0]["rows"])
    out = {}
    for key in ("sigma2", "S", "S_star", "c", "sbar", "entry", "obs_P"):
        out[key] = np.array([np.mean([next(r for r in q["rows"] if r["k"] == k)[key] for q in prs]) for k in ks])
    out["k"] = np.array(ks)
    out["K"] = d["K"]
    return out


def fig1v5():
    P = {pn: load(R / "2026-10-15_puzzle" / f"puzzle__{pn}.json") for pn in COL}
    rob = load(HERE / "exponent_robustness.json")
    fig, ax = plt.subplots(1, 4, figsize=(W2, 2.2), gridspec_kw=dict(width_ratios=[1.15, 1, 1, 1]))
    # A: the 2019 comments ladder, the top-k boundary at K/64 and the three flows (per year, log units)
    a = ax[0]
    pn = "reddit_comments_ext"
    d = P[pn]
    l0 = d["ladders"][0]
    r = np.array(l0["ranks"], float)
    a.plot(r, np.array(l0["level"]), color=COL[pn], lw=1.4)
    L = _law(pn)
    k = int(L["k"][0])
    yk = float(np.interp(np.log(k), np.log(r), np.array(l0["level"])))
    a.axvline(k, color=GREY, lw=0.7, ls="--")
    a.text(k * 1.15, 12.4, f"top {k}", fontsize=5.0, color=GREY, va="top")
    s, c, e = 52 * L["sbar"][0], 52 * L["c"][0], 52 * L["entry"][0]
    a.annotate("", xy=(k * 0.55, yk + 0.15), xytext=(k * 0.22, yk + 1.55), arrowprops=dict(arrowstyle="->", color="#b03a2e", lw=1.1))
    a.annotate("", xy=(k * 0.8, yk + 0.9), xytext=(k * 2.4, yk - 0.6), arrowprops=dict(arrowstyle="->", color="#1e8449", lw=1.1))
    a.text(1.3, 1.6, f"at the boundary of the top {k}:\n", fontsize=4.9, color="#333333", va="top")
    a.text(1.3, 0.7, f"members slide {s:.2f} a year", fontsize=4.9, color="#b03a2e", va="top")
    a.text(1.3, -0.3, f"risers cross in {c:.2f} a year", fontsize=4.9, color="#1e8449", va="top")
    a.text(1.3, -1.3, f"entry and exit {e:+.2f} a year", fontsize=4.9, color="#555555", va="top")
    a.set_xscale("log")
    a.set_ylim(-2.6, 12.8)
    a.set_xlabel("rank")
    a.set_ylabel("log activity relative to rank K")
    a.set_title("One ladder, three flows", fontsize=6.3)
    tag(a, "A")
    # B: the ladder by year, three ladders (offset)
    a = ax[1]
    off = {"reddit_comments_ext": 0.0, "reddit_submissions_long": 4.0, "facebook_a": 8.0}
    for pn, d in P.items():
        n = len(d["ladders"])
        for i, l in enumerate(d["ladders"]):
            a.plot(np.array(l["ranks"]) / d["K"], np.array(l["level"]) + off[pn], color=COL[pn], alpha=0.3 + 0.7 * i / max(n - 1, 1), lw=1,
                   label=f"{LAB[pn]}" if i == n - 1 else None)
    a.set_xscale("log")
    a.set_xlabel("rank / K")
    a.set_ylabel("log activity (offset)")
    a.set_title("The shape changes far less", fontsize=6.3)
    a.legend(loc="lower left", frameon=False, fontsize=4.8)
    tag(a, "B")
    # C: share of each top set replaced in a year
    a = ax[2]
    for pn, d in P.items():
        ks = sorted(int(k) for k in d["turnover"])
        rep = [1 - d["turnover"][str(k)][0] for k in ks]
        xs = [10, 25] + ks
        ys = [1 - np.mean(rob[pn]["retention"]["10"]), 1 - np.mean(rob[pn]["retention"]["25"])] + rep
        a.plot(xs, ys, "-", marker=MK[pn], color=COL[pn], ms=3, lw=1, label=LAB[pn] + (" (half-year)" if pn == "facebook_a" else ""))
    a.set_xscale("log")
    a.set_ylim(0, 0.45)
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("share replaced in a year")
    a.set_title("Members turn over", fontsize=6.3)
    a.legend(loc="upper right", frameon=False, fontsize=4.8)
    tag(a, "C")
    # D: leaders keep their rank and lose share (2019 -> 2022), the five largest losers of each 2019 head
    a = ax[3]
    for j, pn in enumerate(("reddit_submissions_long", "reddit_comments_ext")):
        hn = load(HERE / f"head_named__{pn}.json")
        ranks22 = dict((n, rk) for n, rk in hn["head"]["rank_2022_of_2019_head"])
        rows = hn["head"]["top10_losers"][:3]
        x0, x1 = j * 4.6, j * 4.6 + 1
        ys = []
        for name, s19, s22 in rows:
            a.plot([x0, x1], [100 * s19, 100 * s22], "-", marker=MK[pn], color=COL[pn], ms=2.5, lw=0.9)
            ys.append([100 * s22, name])
        ys.sort(key=lambda t: t[0])
        lab_y = []
        for y, name in ys:
            y_ = max(y, lab_y[-1] * 1.45) if lab_y else y
            lab_y.append(y_)
            rk = ranks22.get(name, -1)
            a.text(x1 + 0.08, y_, f"r/{name} ({'gone' if rk < 0 else f'#{rk}'})", fontsize=4.0, va="center", color="#333333")
    a.set_xticks([0, 1, 4.6, 5.6])
    a.set_xticklabels(["'19", "'22", "'19", "'22"], fontsize=5)
    a.text(0.5, 17, "submissions", ha="center", fontsize=5, color=COL["reddit_submissions_long"])
    a.text(5.1, 17, "comments", ha="center", fontsize=5, color=COL["reddit_comments_ext"])
    a.set_xlim(-0.3, 8.6)
    a.set_yscale("log")
    a.set_ylim(0.03, 22)
    a.set_yticks([0.03, 0.1, 0.3, 1, 3, 10])
    a.set_yticklabels(["0.03", "0.1", "0.3", "1", "3", "10"])
    a.minorticks_off()
    a.set_ylabel("share of the top K's activity (%)")
    a.set_title("Leaders keep rank, lose share", fontsize=6.3)
    tag(a, "D")
    fig.tight_layout(w_pad=0.9)
    save(fig, "fig1_v5")


def fig2v5():
    kin = load(HERE / "turnover_kinematics.json")
    rob = load(HERE / "exponent_robustness.json")
    S7 = r7()
    fig, ax = plt.subplots(1, 4, figsize=(W2, 2.25))
    # A: time scales by rank, model-implied, against observed membership turnover
    a = ax[0]
    for pn in PANELS2:
        L = _law(pn)
        k, K = L["k"], L["K"]
        sel = k < K
        yr = 52.0
        move = L["S"] ** 2 / L["sigma2"] / yr                                   # lasting movement across one e-fold of rank
        relax_bal = 2 * L["S_star"] ** 2 * np.log(K / k) / L["sigma2"] / yr     # linearized relaxation toward the balance
        rows = kin[pn]
        kk = sorted({r["k"] for r in rows})
        f = np.array([np.mean([r["obs"] for r in rows if r["k"] == q]) for q in kk])
        memb = -1.0 / np.log(1.0 - f)                                             # e-folding time of membership, years
        a.plot(k[sel], relax_bal[sel], "-", color=COL[pn], lw=1.2)
        a.plot(k[sel], move[sel], ":", color=COL[pn], lw=1.2)
        a.plot(kk, memb, MK[pn], color=COL[pn], ms=3, mfc="white", lw=0)
    a.plot([], [], "-", color="k", label="shape relaxes (model)")
    a.plot([], [], ":", color="k", label="lasting move, one e-fold (model)")
    a.plot([], [], "o", color="k", mfc="white", ms=3, label="membership turns over (data)")
    a.set_xscale("log")
    a.set_yscale("log")
    a.set_ylim(0.8, 60)
    a.set_yticks([1, 2, 5, 10, 20, 50])
    a.set_yticklabels(["1", "2", "5", "10", "20", "50"])
    a.minorticks_off()
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("years")
    a.set_title("Movement and relaxation times", fontsize=6.3)
    a.legend(loc="upper left", frameon=False, fontsize=4.4)
    tag(a, "A")
    # B: measured crossings against sigma^2/2S
    a = ax[1]
    sets = [("reddit_comments_ext", "boundary__reddit_comments_ext.json"), ("reddit_submissions_long", "boundary__reddit_submissions_long.json"),
            ("facebook_a", "boundary__facebook_a__block13.json")]
    xs = np.linspace(0, 14, 10)
    a.plot(xs, xs, color=GREY, lw=0.8)
    for pn, fn in sets:
        d = load(R / "2026-10-14_boundary_law" / "cells" / fn)
        K = d["K"]
        for rr in d["pooled"]:
            c, cq = 1000 * rr["c"], [1000 * v for v in rr["c_q"]]
            x, xq = 1000 * rr["R_lasting"], [1000 * v for v in rr["R_lasting_q"]]
            small = rr["k"] < K / 16 - 1 and pn != "reddit_comments_ext"
            a.errorbar(c, x, xerr=[[c - cq[0]], [cq[1] - c]], yerr=[[x - xq[0]], [xq[1] - x]], fmt=MK[pn], color=COL[pn],
                       mfc="white" if small else COL[pn], ms=3, capsize=1.2, lw=0.5)
        a.plot([], [], MK[pn], color=COL[pn], ms=3, label=LAB[pn])
    a.text(0.4, 13.6, "pooled ratio 0.94 (0.86-1.04)\nrandom-growth worlds 1.04-1.12", ha="left", va="top", fontsize=4.8, color="#333333")
    a.set_xlim(0, 14)
    a.set_ylim(0, 14)
    a.set_xlabel("predicted, σ²/2S (per 1,000 wk)")
    a.set_ylabel("measured crossing gain")
    a.set_title("Crossings: σ²/2S", fontsize=6.3)
    a.legend(loc="lower right", frameon=False, fontsize=4.6)
    tag(a, "B")
    # C: the balance, crossings against the members' net decline s̄ - e (half-year cells); on the diagonal the shape holds
    a = ax[2]
    lim = [0, 12]
    a.plot(lim, lim, color=GREY, lw=0.8)
    for pn in PANELS2:
        d = load(R / "2026-10-14_balance_law_halfyear" / "cells" / f"law__{pn}.json")
        K = d["K"]
        for q in d["pairs"]:
            rows = [rr for rr in q["rows"] if rr["k"] < K]
            c = np.array([rr["c"] for rr in rows]) * 1000
            sb = np.array([rr["sbar"] - rr["entry"] for rr in rows]) * 1000
            kk = np.array([rr["k"] for rr in rows])
            a.scatter(sb, c, s=5 + 26 * (np.log(K / kk) / np.log(64)), marker=MK[pn], color=COL[pn], alpha=0.75, lw=0)
    a.text(11.6, 0.3, "decline wins: flattens", fontsize=5, color=GREY, ha="right")
    a.text(0.4, 10.8, "crossings win: steepens", fontsize=5, color=GREY, ha="left")
    a.set_xlim(lim)
    a.set_ylim(0, 11.5)
    a.set_xlabel("net decline, s̄ − e (per 1,000 wk)")
    a.set_ylabel("crossing gain (per 1,000 wk)")
    a.set_title("Crossings return two thirds", fontsize=6.3)
    tag(a, "C")
    # D: geometry under identical dynamics (R9)
    a = ax[3]
    for pn in PANELS2:
        rows = kin[pn]
        ks = sorted({rr["k"] for rr in rows})
        ys = [np.mean([rr["obs"] for rr in rows if rr["k"] == q]) for q in ks]
        a.plot([10, 25] + ks, [1 - np.mean(rob[pn]["retention"]["10"]), 1 - np.mean(rob[pn]["retention"]["25"])] + ys, "-",
               marker=MK[pn], color=COL[pn], ms=3, lw=1.1, label=LAB[pn])
    for w, lab, col, ls in WORLDS7[:4]:
        if w not in S7:
            continue
        t = S7[w]["turnover"]
        kk = sorted(int(q) for q in t)
        a.plot(kk, [t[str(q)]["mean"] for q in kk], ls, color=col, lw=1, label=lab)
        if w in ("sb_data", "sb_flat"):
            a.fill_between(kk, [t[str(q)]["q05"] for q in kk], [t[str(q)]["q95"] for q in kk], color=col, alpha=0.1, lw=0)
    a.set_xscale("log")
    a.set_ylim(0, 0.5)
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("share replaced in a year")
    a.set_title("Geometry sets the order", fontsize=6.3)
    a.legend(loc="upper right", frameon=False, fontsize=3.9, ncol=1, handlelength=1.5)
    tag(a, "D")
    fig.tight_layout(w_pad=0.8)
    save(fig, "fig2_v5")


def fig3v5():
    fig, ax = plt.subplots(1, 3, figsize=(W2, 2.3))
    CF = {pn: load(HERE / f"churn_counterfactual__{pn}__flat20__noentry.json")["flat_start"] for pn in PANELS2}
    L30 = {pn: load(HERE / f"churn_counterfactual__{pn}__long30__noentry.json") for pn in PANELS2}
    # A: from equality (0-10 years), shape at K/64, 20 runs, 90% band; observed 2019 shape dotted
    a = ax[0]
    for pn in PANELS2:
        tr = np.array(CF[pn]["shape_by_year"])[:11, 0]
        lo, hi = (np.array(q)[:11, 0] for q in CF[pn]["shape_q"])
        yrs = np.arange(tr.size)
        a.plot(yrs, tr, "-", color=COL[pn], lw=1.2, label=LAB[pn])
        a.fill_between(yrs, lo, hi, color=COL[pn], alpha=0.2, lw=0)
        a.axhline(L30[pn]["start"]["shape"][0], color=COL[pn], ls=":", lw=0.8)
    a.axvspan(0, 5, color="#f2f2f2", zorder=0)
    a.text(0.3, 5.55, "observed 2019 (dotted)", fontsize=4.8, color=GREY)
    a.set_xlim(0, 10)
    a.set_ylim(0, 6)
    a.set_xlabel("years from complete equality")
    a.set_ylabel("shape at K/64 (log units)")
    a.set_title("Volatility and decline build a hierarchy", fontsize=6.3)
    a.legend(loc="lower right", frameon=False, fontsize=4.8)
    tag(a, "A")
    # B: change of the head's shape since 2019: the model with stable parameters (three scenarios, 0-10 years)
    # against the observed path 2019-22 (R15 D1, exact block means)
    a = ax[1]
    D1 = load(HERE / "r15_d1_shape_path.json")
    LS = {"churn_slide": ("-", "model: volatility and decline"), "slide_only": ("--", "decline only"), "churn_only": (":", "volatility only")}
    for pn in PANELS2:
        for wk, (ls, lab) in LS.items():
            y = np.array(L30[pn][wk]["shape_by_year"])[:11, 0]
            lo, hi = (np.array(q)[:11, 0] for q in L30[pn][wk]["shape_q"])
            yrs = np.arange(y.size)
            a.plot(yrs, y - y[0], ls, color=COL[pn], lw=1.0, label=lab if pn == "reddit_submissions_long" else None)
            a.fill_between(yrs, lo - y[0], hi - y[0], color=COL[pn], alpha=0.12, lw=0)
        obs = np.array(D1[pn]["shape"])[:, 0]
        a.plot(np.arange(obs.size), obs - obs[0], "o", color=COL[pn], ms=3.2, mec="black", mew=0.4,
               label="observed, 2019-22" if pn == "reddit_submissions_long" else None)
    a.axvspan(5, 10, color="#efefef", zorder=0)
    a.text(5.3, 0.62, "model-implied", fontsize=4.8, color=GREY)
    a.axhline(0, color=GREY, lw=0.4)
    a.set_xlim(0, 10)
    a.set_ylim(-1.6, 0.8)
    a.set_xlabel("years from the 2019 ladder")
    a.set_ylabel("change in shape at K/64 (log units)")
    a.set_title("Stable parameters miss the falls", fontsize=6.3)
    a.legend(loc="lower left", frameon=False, fontsize=4.2)
    tag(a, "B")
    # C: backtest, fit on 2019-20, forecast 2021-22 one-year retention, by k (A6)
    a = ax[2]
    for pn in PANELS2:
        o = load(HERE / f"structure_oop__{pn}__home_movinghome_vp.json")
        K = o["K"]
        ks = sorted(int(q) for q in o["data"]["ret_23"])
        dat = [1 - o["data"]["ret_23"][str(q)] for q in ks]
        mod = [1 - o["arms"]["movinghome_vp"]["ret_23"][str(q)] for q in ks]
        car = [1 - o["data"]["ret_01"][str(q)] for q in ks]
        a.plot(ks, dat, "-", marker=MK[pn], color=COL[pn], ms=3, lw=1.1, label=f"{LAB[pn]}: observed")
        a.plot(ks, mod, "--", marker=MK[pn], color=COL[pn], ms=3, mfc="white", lw=0.9, label="model, from 2019-20")
        a.plot(ks, car, ":", color=COL[pn], lw=0.8, label="2019-20 carried forward" if pn == "reddit_submissions_long" else None)
    a.set_xscale("log")
    a.set_ylim(0, 0.32)
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("share replaced, 2021 to 2022")
    a.set_title("A year ahead, replacement is reproduced", fontsize=6.3)
    a.legend(loc="upper right", frameon=False, fontsize=4.2)
    tag(a, "C")
    fig.tight_layout(w_pad=1.0)
    save(fig, "fig3_v5")


def fig4v5():
    S7 = r7()
    fig, ax = plt.subplots(1, 4, figsize=(W2, 2.3), gridspec_kw=dict(width_ratios=[1, 0.95, 1.1, 0.95]))
    # A: the head's arithmetic share, 2019-2022, against the calibrated homogeneous forecast for 2022 (fit 2019-20)
    a = ax[0]
    for j, pn in enumerate(PANELS2):
        hc = load(HERE / f"head_components__{pn}.json")
        yrs = [rr["year"] for rr in hc["rows"]]
        a.plot(yrs, [100 * rr["reselected"]["share_karma"] for rr in hc["rows"]], "-", marker=MK[pn], color=COL[pn], ms=3, lw=1.1,
               label=f"{LAB[pn]} (top {hc['k']})")
        o = load(HERE / f"structure_oop__{pn}__home_movinghome_vp.json")
        m, (lo, hi) = o["arms"]["movinghome_vp"]["share_3"], o["arms"]["movinghome_vp"]["share_3_q"]
        x = 2022.15 + 0.12 * j
        a.errorbar([x], [100 * m], yerr=[[100 * (m - lo)], [100 * (hi - m)]], fmt="D", color=COL[pn], mfc="white", ms=3.2, capsize=1.5, lw=0.8)
        a.plot([2022, x], [100 * o["data"]["share_3"]] * 2, ":", color=COL[pn], lw=0.6)
        for i_, (tag_, mk_) in enumerate((("fixed", "v"), ("decay", "^"), ("factor", "P"))):   # R12 heterogeneous worlds, when run
            f = HERE / f"r12_oop__{pn}__{tag_}.json"
            if f.exists():
                g = load(f)
                a.errorbar([x + 0.07 * (i_ + 1)], [100 * g["share_3"]], yerr=[[100 * (g["share_3"] - g["share_3_q"][0])], [100 * (g["share_3_q"][1] - g["share_3"])]],
                           fmt=mk_, color=COL[pn], ms=2.6, lw=0.6)
    a.plot([], [], "D", color="k", mfc="white", ms=3, label="homogeneous, fit 2019-20")
    if (HERE / "r12_oop__reddit_submissions_long__factor.json").exists():
        for tag_, mk_ in (("fixed", "v"), ("decay", "^"), ("factor", "P")):
            a.plot([], [], mk_, color="k", ms=2.6, label={"fixed": "fixed growth diff.", "decay": "decaying diff.",
                                                           "factor": "platform-trend exposure"}[tag_])
    a.set_xticks([2019, 2020, 2021, 2022]); a.set_xticklabels(["2019", "'20", "'21", "'22"], fontsize=5.5)
    a.set_xlim(2018.8, 2022.6)
    a.set_ylim(20, 100)
    a.set_ylabel("head's share of the top K (%)")
    a.set_title("Faster than forecast", fontsize=6.3)
    a.legend(loc="upper left", frameon=False, fontsize=4.2)
    tag(a, "A")
    # B: the head's share split exactly into items and activity per item
    a = ax[1]
    for pn in ("reddit_submissions_long", "reddit_comments_ext"):
        hc = load(HERE / f"head_components__{pn}.json")
        yrs = [rr["year"] for rr in hc["rows"]]
        base = hc["rows"][0]["reselected"]
        for key, ls, lab in (("share_karma", "-", "share of activity"), ("share_items", "--", "share of items"),
                             ("rel_karma_per_item", ":", "activity per item")):
            a.plot(yrs, [rr["reselected"][key] / base[key] for rr in hc["rows"]], ls, marker=MK[pn], color=COL[pn], ms=2.5, lw=1,
                   label=lab if pn == "reddit_submissions_long" else None)
    a.axhline(1, color=GREY, lw=0.4)
    a.set_yscale("log")
    a.set_yticks([0.4, 0.6, 1, 1.5])
    a.set_yticklabels(["0.4", "0.6", "1", "1.5"])
    a.minorticks_off()
    a.set_xticks([2019, 2020, 2021, 2022])
    a.set_xticklabels(["2019", "2020", "2021", "2022"], fontsize=5)
    a.set_ylabel("relative to 2019")
    a.set_title("It fell in posting", fontsize=6.3)
    a.legend(loc="lower left", frameon=False, fontsize=4.6)
    tag(a, "B")
    # C: persistence across gaps: data, factor-adjusted, Facebook, and the simulated worlds
    a = ax[2]
    gm = load(R / "2026-10-18_momentum" / "gapped_momentum.json")
    for pn in COL:
        dd = gm[pn]["gapped"]
        gs = sorted(dd, key=lambda x: int(x))
        a.plot([int(g) for g in gs], [dd[g]["mean"] for g in gs], "-", marker=MK[pn], color=COL[pn], ms=3, lw=1,
               label={"reddit_comments_ext": "comments", "reddit_submissions_long": "submissions", "facebook_a": "Facebook"}[pn])
    for n, w in enumerate(("sb_data", "firm_data", "sb_flat", "firm_flat", "gibrat", "atlas", "neutral", "rdm")):
        if w not in S7:
            continue
        g7 = S7[w]["gapped"]
        for gap in ("13", "26"):
            a.errorbar([int(gap) + (n - 3.5) * 0.35], [g7[gap]["mean"]], yerr=[[g7[gap]["mean"] - g7[gap]["q05"]], [g7[gap]["q95"] - g7[gap]["mean"]]],
                       fmt="x", color="#666666", ms=2.5, mew=0.7, elinewidth=0.4, label="homogeneous worlds" if (n == 0 and gap == "13") else None)
    for i_, (tag_, mk) in enumerate((("fixed", "v"), ("decay", "^"), ("factor", "P"))):   # R12 heterogeneous worlds, when run
        shown = False
        for pn in PANELS2:
            f = HERE / f"r12_worlds__{pn}__{tag_}.json"
            if f.exists():
                g = load(f)["gapped_stats"]                                 # the same statistic as the homogeneous worlds
                for gap in ("13", "26"):
                    a.errorbar([int(gap) + 1.6 + 0.9 * i_], [g[gap]["mean"]], yerr=[[g[gap]["mean"] - g[gap]["q05"]], [g[gap]["q95"] - g[gap]["mean"]]],
                               fmt=mk, color=COL[pn], ms=2.6, lw=0.6,
                               label=({"fixed": "fixed growth diff.", "decay": "decaying diff.", "factor": "platform-trend exposure"}[tag_]
                                      if (not shown and gap == "13") else None))
                    shown = True
    a.axhline(0, color=GREY, lw=0.4)
    a.set_xlabel("gap between the two changes (weeks)")
    a.set_ylabel("rank correlation of changes")
    a.set_title("Changes persist on Reddit", fontsize=6.3)
    a.legend(loc="upper right", frameon=False, fontsize=4.0, ncol=1, handlelength=1.4, labelspacing=0.3)
    a.set_ylim(-0.16, 0.66)
    tag(a, "C")
    # D: the slide by prior change: the bottom fifth of prior change against the rest, data and worlds
    a = ax[3]
    sp = load(HERE / "slide_by_persistence__data.json")
    worlds = {("reddit_submissions_long", "driftless"): "slide_by_persistence__reddit_submissions_long__driftless.json",
              ("reddit_comments_ext", "driftless"): "slide_by_persistence__reddit_comments_ext__driftless.json",
              ("reddit_submissions_long", "model"): "slide_by_persistence__reddit_submissions_long__model.json"}
    x = 0
    ticks, labs = [], []
    for pn in PANELS2:
        sm = sp[pn]["summary"]
        a.bar(x, 1000 * sm["mean_Q1"], 0.8, color=COL[pn])
        a.bar(x + 0.85, 1000 * sm["mean_rest"], 0.8, color=COL[pn], alpha=0.45)
        ticks += [x + 0.42]
        labs += ["data"]
        a.text(x + 0.42 + (1.1 if pn == "reddit_comments_ext" else 2.2), 9.6, {"reddit_comments_ext": "comments", "reddit_submissions_long": "submissions"}[pn],
               ha="center", fontsize=5, color=COL[pn])
        x += 2.2
        for (p2, kind), fn in worlds.items():
            if p2 != pn:
                continue
            wm = load(HERE / fn)["summary"]
            a.bar(x, 1000 * wm["mean_Q1"], 0.8, color=COL[pn], hatch="////", edgecolor="white", lw=0)
            a.bar(x + 0.85, 1000 * wm["mean_rest"], 0.8, color=COL[pn], alpha=0.45, hatch="////", edgecolor="white", lw=0)
            ticks += [x + 0.42]
            labs += [kind]
            x += 2.2
        x += 0.6
    a.axhline(0, color=GREY, lw=0.5)
    a.set_xticks(ticks)
    a.set_xticklabels(labs, fontsize=4.6, rotation=30)
    a.set_ylabel("slide of members (per 1,000 wk)")
    a.set_title("Decliners keep declining", fontsize=6.3)
    from matplotlib.patches import Patch
    a.set_ylim(top=a.get_ylim()[1] * 1.35)
    a.legend(handles=[Patch(color="#555555", label="bottom fifth of prior change"), Patch(color="#555555", alpha=0.45, label="the rest")],
             loc="upper left", frameon=False, fontsize=4.4, ncol=2, columnspacing=0.8)
    tag(a, "D")
    fig.tight_layout(w_pad=0.8)
    save(fig, "fig4_v5")


if __name__ == "__main__":
    for f in sys.argv[1:] or ["fig1v5", "fig2v5", "fig3v5", "fig4v5"]:
        globals()[f]()
        print("drew", f)
