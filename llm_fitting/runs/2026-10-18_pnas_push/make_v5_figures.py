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
    a.set_ylabel("share replaced (a year; Facebook, half)")
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


def _fig_timescales(a, kin):
    """Time scales by top set (the v8 Fig. 2A, now SI Fig. S4)."""
    for pn in PANELS2:
        L = _law(pn)
        k, K = L["k"], L["K"]
        sel = k < K
        yr = 52.0
        move = L["S"] ** 2 / L["sigma2"] / yr                                   # lasting movement across one e-fold of rank
        rows = kin[pn]
        kk = sorted({r["k"] for r in rows})
        f = np.array([np.mean([r["obs"] for r in rows if r["k"] == q]) for q in kk])
        memb = -1.0 / np.log(1.0 - f)                                             # e-folding time of membership, years
        a.plot(k[sel], move[sel], ":", color=COL[pn], lw=1.2)
        a.plot(kk, memb, MK[pn], color=COL[pn], ms=3, mfc="white", lw=0)
        kh = K // 64
        s17 = HERE / "r17_durability_scores.json"                                  # R17-C: the lasting shape's response, homogeneous world
        s16 = HERE / "r16_durability_scores.json"                                  # R16-F: lag-aware fit to the observed shape
        if s17.exists() and f"{pn}__control_num" in load(s17):
            d17 = load(s17)[f"{pn}__control_num"]["head_step"]
            tau, (lo, hi) = d17["fit_lasting"]["tau"], d17["tau_lasting_band"]
            a.errorbar([kh * 1.08], [tau], yerr=[[tau - lo], [hi - tau]], fmt="*", color=COL[pn], ms=5.5, mec="k", mew=0.3, elinewidth=0.6, capsize=1.2)
        elif s16.exists():
            dsc = load(s16)[pn]
            taus = [dsc[m_]["tau_lag"] for m_ in ("0.7", "1.0", "1.4")]
            a.errorbar([kh * 1.08], [taus[1]], yerr=[[taus[1] - min(taus)], [max(taus) - taus[1]]], fmt="*", color=COL[pn], ms=5.5,
                       mec="k", mew=0.3, elinewidth=0.6, capsize=1.2)
    a.plot([], [], ":", color="k", label="lasting move, one e-fold (measured)")
    a.plot([], [], "*", color="k", ms=5, label="shape responds (calibrated model)")
    a.plot([], [], "o", color="k", mfc="white", ms=3, label="membership turns over (data)")
    a.set_xscale("log")
    a.set_yscale("log")
    a.set_ylim(0.8, 60)
    a.set_yticks([1, 2, 5, 10, 20, 50])
    a.set_yticklabels(["1", "2", "5", "10", "20", "50"])
    a.minorticks_off()
    a.set_xlabel("size of the top set, k")
    a.set_ylabel("years")
    a.set_title("Response and turnover times", fontsize=6.3)
    a.legend(loc="upper left", frameon=False, fontsize=4.4)


def figS4v5():
    fig, a = plt.subplots(1, 1, figsize=(W2 / 3.0, 2.4))
    _fig_timescales(a, load(HERE / "turnover_kinematics.json"))
    fig.tight_layout()
    save(fig, "figS4_v5")


def fig2v5():
    kin = load(HERE / "turnover_kinematics.json")
    rob = load(HERE / "exponent_robustness.json")
    S7 = r7()
    fig, ax = plt.subplots(1, 4, figsize=(W2, 2.25))
    # A (v9.1, round 9 iteration 4): the share of each top set replaced, observed against the turnover formula (SI Eq. S2),
    # every cell of the three ladders (Facebook: 26-week pairs; Reddit: 52-week pairs)
    a = ax[0]
    for pn in ("facebook_a", "reddit_comments_ext", "reddit_submissions_long"):
        rows = kin[pn]
        a.plot([r["obs"] for r in rows], [r["pred_exp"] for r in rows], MK[pn], color=COL[pn], ms=3, mfc="white" if pn != "facebook_a" else COL[pn],
               lw=0, label={"facebook_a": "Facebook (half-year)", "reddit_comments_ext": "Reddit comments", "reddit_submissions_long": "Reddit submissions"}[pn])
    lim = [0.05, 0.45]
    a.plot(lim, lim, color=GREY, lw=0.8)
    a.fill_between(lim, [0.82 * x for x in lim], [1.18 * x for x in lim], color=GREY, alpha=0.12, lw=0)
    a.set_xlim(*lim)
    a.set_ylim(*lim)
    a.set_xlabel("share replaced, observed")
    a.set_ylabel("share replaced, reconstructed")
    a.set_title("Turnover reconstructed, 48 cells", fontsize=6.3)
    a.legend(loc="upper left", frameon=False, fontsize=4.6)
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
    for pn in PANELS2:                  # the data by the worlds' code on the universe frame (R17-B; round 8, H9)
        tt = load(HERE / f"r17_table_data__{pn}.json")["turnover"]
        ks = sorted(int(q) for q in tt)
        a.plot(ks, [tt[str(q)] for q in ks], "-", marker=MK[pn], color=COL[pn], ms=3, lw=1.1, label=LAB[pn])
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
    a.set_title("A year ahead: better than carrying forward\non submissions, not on comments", fontsize=6.3)
    a.legend(loc="upper right", frameon=False, fontsize=4.2)
    tag(a, "C")
    fig.tight_layout(w_pad=1.0)
    save(fig, "fig3_v5")


R16_TAGS = {"reddit_submissions_long": {"joint": "joint_s0.8_head0.25_km0.5_m0.8", "decline": "decline_pi0.25_s0.7_hl208_m1.0"},
            "reddit_comments_ext": {"joint": "joint_s0.6_head0.25_km0.5_m0.7", "decline": "decline_pi0.25_s0.7_hl208_m0.7"}}
HET_LAB = {"joint": "fixed growth diff. (joint)", "decline": "decline state", "factor": "platform-trend exposure"}
HET_MK = {"joint": "v", "decline": "^", "factor": "P"}


def _r16_gapped(pn, kind):
    """R16 worlds (r16_tier3.py): mean and 5-95% of the paper's gapped statistic at gaps 13 and 26 across seeds."""
    import glob
    W = [load(Path(f)) for f in sorted(glob.glob(str(HERE / f"r16_world__{pn}__{R16_TAGS[pn][kind]}__s*.json")))]
    if not W:
        return None
    return {g: dict(mean=float(np.mean([w["gapped"][g] for w in W])), q05=float(np.quantile([w["gapped"][g] for w in W], 0.05)),
                    q95=float(np.quantile([w["gapped"][g] for w in W], 0.95))) for g in ("13", "26")}


def _het_oop(pn, kind):
    f = {"joint": HERE / f"r16_oop__{pn}__joint.json", "decline": HERE / f"r16_oop__{pn}__decline_hist.json",
         "factor": HERE / f"r12_oop__{pn}__factor.json"}[kind]
    if not f.exists():
        return None
    g = load(f)
    return _med_q(g["shares"])


def _med_q(v):
    """Median and quartiles over runs: the head share is a level sum that one extreme simulated week can swamp in a run."""
    v = np.asarray(v, float)
    return float(np.median(v)), (float(np.quantile(v, 0.25)), float(np.quantile(v, 0.75)))


def _fig_items(a):
    """The head's share split exactly into items and activity per item (R8; the v7 Fig. 3B, now SI Fig. S2)."""
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
    a.set_title("The head's fall was in posting", fontsize=6.3)
    a.legend(loc="lower left", frameon=False, fontsize=4.6)


def figS2v5():
    fig, a = plt.subplots(1, 1, figsize=(W2 / 3.2, 2.3))
    _fig_items(a)
    fig.tight_layout()
    save(fig, "figS2_v5")


R17_TAGS = {"reddit_submissions_long": {"joint17": "joint_s0.8_head0.25_km0.5_m0.6",
                                        "traded": "joint_decline_s0.6_head0.25_km0.5_m0.6_pi0.25_hl208_sd0.5"},
            "reddit_comments_ext": {"joint17": "joint_s0.6_head0.0_km0.35_m0.7",
                                    "traded": "joint_decline_s0.6_head0.0_km0.35_m0.7_pi0.25_hl208_sd0.3"}}


def _fig_forecast(a):
    """The head's arithmetic share against the forecasts (the v8.2 Fig. 3D, now SI Fig. S3)."""
    for j, pn in enumerate(PANELS2):
        hc = load(HERE / f"head_components__{pn}.json")
        yrs = [rr["year"] for rr in hc["rows"]]
        a.plot(yrs, [100 * rr["reselected"]["share_karma"] for rr in hc["rows"]], "-", marker=MK[pn], color=COL[pn], ms=3, lw=1.1,
               label=f"{LAB[pn]} (top {hc['k']})")
        o = load(HERE / f"structure_oop__{pn}__home_movinghome_vp.json")
        m, (lo, hi) = _med_q(load(HERE / f"r12_oop__{pn}__control.json")["shares"])   # the same runs as the movinghome_vp arm
        x = 2022.15 + 0.12 * j
        a.errorbar([x], [100 * m], yerr=[[100 * (m - lo)], [100 * (hi - m)]], fmt="D", color=COL[pn], mfc="white", ms=3.2, capsize=1.5, lw=0.8)
        a.plot([2022, x], [100 * o["data"]["share_3"]] * 2, ":", color=COL[pn], lw=0.6)
        for i_, kind in enumerate(("joint", "decline", "factor")):
            g = _het_oop(pn, kind)
            if g is not None:
                m_, (lo_, hi_) = g
                a.errorbar([x + 0.07 * (i_ + 1)], [100 * m_], yerr=[[100 * (m_ - lo_)], [100 * (hi_ - m_)]], fmt=HET_MK[kind], color=COL[pn], ms=2.6, lw=0.6)
    a.plot([], [], "D", color="k", mfc="white", ms=3, label="homogeneous, fit 2019-20")
    for kind in ("joint", "decline", "factor"):
        a.plot([], [], HET_MK[kind], color="k", ms=2.6, label=HET_LAB[kind])
    a.set_xticks([2019, 2020, 2021, 2022]); a.set_xticklabels(["2019", "'20", "'21", "'22"], fontsize=5.5)
    a.set_xlim(2018.8, 2022.6)
    a.set_ylim(20, 100)
    a.set_ylabel("head's share of the top K (%)")
    a.set_title("No forecast of the falls", fontsize=6.3)
    a.legend(loc="upper left", frameon=False, fontsize=4.2)


def figS3v5():
    fig, a = plt.subplots(1, 1, figsize=(W2 / 3.0, 2.4))
    _fig_forecast(a)
    fig.tight_layout()
    save(fig, "figS3_v5")


def fig4v5():
    """Main-text Fig. 3 (v8.2, round 9): the asymmetry leads. A persistence across gaps; B the decile curves; C the
    asymmetry D with its bands for the data, by stratum and on item counts, beside every calibrated world; D the head's
    share against the homogeneous forecast."""
    S7 = r7()
    fig, ax = plt.subplots(1, 3, figsize=(W2, 2.55), gridspec_kw=dict(width_ratios=[1, 1, 1.3]))
    # A: persistence across gaps: data, Facebook, and the simulated worlds
    a = ax[0]
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
    for i_, kind in enumerate(("joint", "decline", "factor")):           # heterogeneous worlds (R16 joint and decline; R12 factor)
        shown = False
        for pn in PANELS2:
            if kind == "factor":
                f = HERE / f"r12_worlds__{pn}__factor.json"
                g = load(f)["gapped_stats"] if f.exists() else None
            else:
                g = _r16_gapped(pn, kind)
            if g is None:
                continue
            for gap in ("13", "26"):
                a.errorbar([int(gap) + 1.6 + 0.9 * i_], [g[gap]["mean"]], yerr=[[g[gap]["mean"] - g[gap]["q05"]], [g[gap]["q95"] - g[gap]["mean"]]],
                           fmt=HET_MK[kind], color=COL[pn], ms=2.6, lw=0.6, label=(HET_LAB[kind] if (not shown and gap == "13") else None))
                shown = True
    f18 = HERE / "r18_summary.json"                                      # R18-C: Facebook's own model, matched control, beta 0.019 (Table 1)
    if f18.exists():
        g18 = load(f18)["worlds"]["controlb0.019"]["gapped"]
        for gap in ("0", "4", "8"):
            b = g18[gap]
            a.errorbar([int(gap) + 0.9], [b["mean"]], yerr=[[b["mean"] - b["q05"]], [b["q95"] - b["mean"]]], fmt="x", color=COL["facebook_a"],
                       ms=2.8, mew=0.8, elinewidth=0.5, label="Facebook's model worlds" if gap == "0" else None)
    a.axhline(0, color=GREY, lw=0.4)
    a.set_xlabel("gap between the changes (weeks)")
    a.set_ylabel("rank correlation of changes")
    a.set_title("Changes persist beyond random growth", fontsize=6.3)
    a.legend(loc="upper center", frameon=False, fontsize=4.4, ncol=2, handlelength=1.1, labelspacing=0.2, columnspacing=0.5)
    a.set_ylim(-0.17, 0.43)
    a.set_yticks([-0.1, 0, 0.1, 0.2])
    tag(a, "A")
    # B: the later change against the initial one, by decile of the initial change (stratum-demeaned)
    a = ax[1]
    sty = {"control": dict(ls="--", color="#777777"), "joint": dict(ls=":", color="#222222"), "decline": dict(ls="-.", color="#999999")}
    labw = {"control": "homogeneous", "joint": "fixed diff. (joint)", "decline": "decline state"}
    for pn in PANELS2:
        dd = load(HERE / f"r17_asym_data__{pn}.json")
        cur = np.array(dd["primary"]["curve"])
        a.plot(cur[:, 0], cur[:, 1], "-", marker=MK[pn], color=COL[pn], ms=2.6, lw=1.1,
               label={"reddit_comments_ext": "comments", "reddit_submissions_long": "submissions"}[pn])
        if pn != "reddit_submissions_long":
            continue
        for kind in ("control", "joint", "decline"):
            tg = "control_num_" if kind == "control" else R16_TAGS[pn][kind]
            cw = np.array(load(HERE / f"r17_asym_worlds__{pn}__{tg}.json")["curve"])
            a.plot(cw[:, 0], cw[:, 1], lw=0.9, label=labw[kind], **sty[kind])
    a.axhline(0, color=GREY, lw=0.4)
    a.axvline(0, color=GREY, lw=0.4)
    a.set_xlabel("initial year-long change, by decile")
    a.set_ylabel("later year-long change")
    a.set_title("Declines continue", fontsize=6.3)
    a.legend(loc="lower right", frameon=False, fontsize=5.0, handlelength=1.8, labelspacing=0.25)
    tag(a, "B")
    # C: D with its bands. Data: 5-95% over 200 bootstrap draws of communities within strata (strata: 100 draws).
    # Worlds: mean and 5-95% range over runs.
    a = ax[2]
    rows = []
    for pn in PANELS2:
        dd = load(HERE / f"r17_asym_data__{pn}.json")
        it = load(HERE / f"r17_asym_items__{pn}.json")
        b = dd["primary_band"]["D"]
        rows.append((pn, "data, karma", dd["primary"]["D"], b[0], b[2], "data"))
        for j, lab in enumerate(("top K/16", "K/16 to K/4", "K/4 to K")):
            bs = dd["by_stratum_band"][str(j)]["D"]
            rows.append((pn, f"  {lab}", dd["by_stratum"][str(j)]["D"], bs[0], bs[2], "data"))
        bi = it["items_r17a_band"]["D"]
        rows.append((pn, "data, item counts", it["items_r17a"]["D"], bi[0], bi[2], "data"))
        for lab, tg in (("homogeneous", "control_num_"), ("fixed diff. (R16)", R16_TAGS[pn]["joint"]), ("fixed diff. (R17)", R17_TAGS[pn]["joint17"]),
                        ("fixed + decline", R17_TAGS[pn]["traded"]), ("decline state", R16_TAGS[pn]["decline"])):
            w = load(HERE / f"r17_asym_worlds__{pn}__{tg}.json")["D"]
            rows.append((pn, lab, w["mean"], w["q05"], w["q95"], "world"))
    f18, b18 = HERE / "r18_summary.json", HERE / "r18_boot__facebook_a.json"
    fbrows = []
    if f18.exists() and b18.exists():
        s18, bb = load(f18), load(b18)
        fbrows = [("Facebook data", s18["data"]["D_FB"]["D"], bb["D_FB_band"][0], bb["D_FB_band"][2], "data"),
                  ("Facebook model", s18["worlds"]["controlb0.019"]["D_FB"]["mean"], s18["worlds"]["controlb0.019"]["D_FB"]["q05"],
                   s18["worlds"]["controlb0.019"]["D_FB"]["q95"], "world")]
    labels = [r[1] for r in rows if r[0] == "reddit_submissions_long"] + [r[0] for r in fbrows]
    n = len(labels)
    for k, pn in enumerate(PANELS2):
        rr = [r for r in rows if r[0] == pn]
        for i, (_, lab, m, lo, hi, kind) in enumerate(rr):
            y = n - 1 - i + (0.18 if k == 0 else -0.18)
            a.errorbar([m], [y], xerr=[[m - lo], [hi - m]], fmt=MK[pn] if kind == "data" else "o", color=COL[pn],
                       mfc=COL[pn] if kind == "data" else "white", ms=2.6, lw=0.8, capsize=0)
    for i, (lab, m, lo, hi, kind) in enumerate(fbrows):
        y = n - 1 - (len(labels) - len(fbrows) + i)
        a.errorbar([m], [y], xerr=[[m - lo], [hi - m]], fmt="^", color=COL["facebook_a"], mfc=COL["facebook_a"] if kind == "data" else "white",
                   ms=2.8, lw=0.8, capsize=0)
    a.axvline(0, color=GREY, lw=0.5)
    a.axhline(n - 5.5, color=GREY, lw=0.3, ls=":")
    if fbrows:
        a.axhline(len(fbrows) - 0.5, color=GREY, lw=0.3, ls=":")
    a.set_yticks(range(n))
    a.set_yticklabels(labels[::-1], fontsize=5.6)
    a.set_xlabel("asymmetry D = p_fall − p_rise")
    a.set_title("Asymmetry in data and worlds", fontsize=6.3)
    a.plot([], [], "s", color=COL["reddit_submissions_long"], ms=2.6, label="submissions")
    a.plot([], [], "o", color=COL["reddit_comments_ext"], ms=2.6, label="comments")
    a.legend(loc="center right", bbox_to_anchor=(1.0, 0.33), frameon=False, fontsize=5.2, handlelength=1.0)
    tag(a, "C")
    fig.tight_layout(w_pad=0.8)
    save(fig, "fig4_v5")


if __name__ == "__main__":
    for f in sys.argv[1:] or ["fig1v5", "fig2v5", "fig3v5", "fig4v5", "figS2v5", "figS3v5", "figS4v5"]:
        globals()[f]()
        print("drew", f)
