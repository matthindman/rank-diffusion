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


def fig_ladders(include_instagram=False):
    """Crossings against sigma^2/2S rank by rank on three ladders (2z-cm, 2z-ct)."""
    sets = [("reddit_comments_ext", "boundary__reddit_comments_ext.json", "Reddit comments", BLUE, "o"),
            ("reddit_submissions_long", "boundary__reddit_submissions_long.json", "Reddit submissions", ORANGE, "s"),
            ("facebook_a", "boundary__facebook_a__block13.json", "Facebook pages", "#2a9d5c", "^")]
    if include_instagram:                                   # 2z-cx: through the censoring apparatus
        sets.append(("instagram_hm", "IG", "Instagram (a-matching; displacement variance, 14→28 weeks)", "#8e44ad", "D"))
    fig, ax = plt.subplots(1, 2, figsize=(11.5, 4.8))
    a = ax[0]
    xs = np.linspace(0, 14, 10)
    a.fill_between(xs, 0.75 * xs, 1.33 * xs, color="#dddddd", alpha=0.6, lw=0, label="within the registered band [0.75, 1.33]")
    a.plot(xs, xs, color=GREY, lw=1)
    for pn, f, lab, col, mk in sets:
        d = load(R / "2026-10-15_instagram" / "ig_boundary__instagram_hm.json")["apparatus"] if f == "IG" else load(R / "2026-10-14_boundary_law" / "cells" / f)
        K = d["K"]
        for r in d["pooled"]:
            c, cq = 1000 * r["c"], [1000 * v for v in r["c_q"]]
            x, xq = 1000 * r["R_lasting"], [1000 * v for v in r["R_lasting_q"]]
            small = r["k"] < K / 16 - 1 and pn != "reddit_comments_ext"
            if pn == "instagram_hm" and r["k"] < K / 16 - 1:
                continue                                        # head strata of 47-102 accounts: noise (2z-cx)
            a.errorbar(c, x, xerr=[[c - cq[0]], [cq[1] - c]], yerr=[[x - xq[0]], [xq[1] - x]], fmt=mk, color=col,
                       mfc="white" if small else col, ms=6, capsize=2, lw=0.8, alpha=0.9)
        a.plot([], [], mk, color=col, label=lab)
    a.plot([], [], "o", color="k", mfc="white", label="open: boundary strata of < 150 names (Instagram above K/16 not drawn)")
    a.set_xlim(0, 14)
    a.set_ylim(0, 14)
    a.set_xlabel("predicted: lasting variance / (2 × local slope)")
    a.set_ylabel("measured: lasting crossing gain")
    a.set_title("Per member per 1,000 weeks, cut-offs K/64 … K (Instagram K/16 … K)")
    a.legend(fontsize=7.5, loc="lower right")
    a = ax[1]
    for pn, f, lab, col, mk in sets:
        d = load(R / "2026-10-15_instagram" / "ig_boundary__instagram_hm.json")["apparatus"] if f == "IG" else load(R / "2026-10-14_boundary_law" / "cells" / f)
        K = d["K"]
        rel = np.array([r["k"] / K for r in d["pooled"]])
        rat = np.array([r["ratio"] for r in d["pooled"]])
        q = np.array([r["ratio_q"] for r in d["pooled"]])
        xo = rel * {"reddit_comments_ext": 0.92, "reddit_submissions_long": 0.97, "facebook_a": 1.03, "instagram_hm": 1.08}[pn]
        a.plot(xo, rat, "-", color=col, lw=1, label=lab)
        small = (rel < 1 / 16 - 1e-9) & (pn != "reddit_comments_ext")             # boundary strata of < 150 names
        for s_, mfc in ((~small, col), (small, "white")):
            a.errorbar(xo[s_], rat[s_], yerr=[rat[s_] - q[s_, 0], q[s_, 1] - rat[s_]], fmt=mk, color=col, mfc=mfc, capsize=2, lw=1)
    a.axhspan(0.75, 1.33, color="#dddddd", alpha=0.6, lw=0)
    a.axhline(1, color=GREY, lw=1)
    a.set_xscale("log")
    a.set_ylim(0, 3.2)
    a.set_xlabel("cut-off as a share of the universe, k / K")
    a.set_ylabel("measured / predicted")
    a.set_title("The ratio by rank (5–95% intervals)")
    a.legend(fontsize=8, loc="upper right")
    n = "four" if include_instagram else "three"
    fig.suptitle(f"Crossings at a rank's boundary against lasting variance over twice the slope, {n} ladders", fontsize=11)
    fig.text(0.01, 0.005, "Reddit: variance from the 2019 cohort over 2019-21, crossings pooled over 2020-22 (registered design, not time-aligned; "
             "time-aligned ratios in 2z-cm by start). Open symbols: boundary strata of < 150 names.", fontsize=6.5, color=GREY)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(HERE / ("fig_four_ladders.png" if include_instagram else "fig_three_ladders.png"), dpi=150)



def fig_three_ladders():
    fig_ladders(False)


def fig_four_ladders():
    fig_ladders(True)

def fig_puzzle():
    """The opening facts: the ladder barely changes while the names on it move (2z-cu)."""
    P = {pn: load(R / "2026-10-15_puzzle" / f"puzzle__{pn}.json") for pn in ("reddit_comments_ext", "reddit_submissions_long", "facebook_a")}
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.6))
    a = ax[0]
    cols = {"reddit_comments_ext": BLUE, "reddit_submissions_long": ORANGE, "facebook_a": "#2a9d5c"}
    labs = {"reddit_comments_ext": "Reddit comments", "reddit_submissions_long": "Reddit submissions", "facebook_a": "Facebook pages"}
    off = {"reddit_comments_ext": 0.0, "reddit_submissions_long": 4.0, "facebook_a": 8.0}
    for pn, d in P.items():
        n = len(d["ladders"])
        for i, l in enumerate(d["ladders"]):
            r = np.array(l["ranks"]) / d["K"]
            a.plot(r, np.array(l["level"]) + off[pn], color=cols[pn], alpha=0.35 + 0.65 * i / max(n - 1, 1), lw=1.2,
                   label=f"{labs[pn]}: {n} {'years' if d['block'] == 52 else 'half-years'}" if i == n - 1 else None)
    a.set_xscale("log")
    a.set_xlabel("rank / K")
    a.set_ylabel("log activity relative to rank K (curves offset)")
    a.set_title("The ladder: one curve per block, darker = later")
    a.legend(fontsize=8, loc="upper right")
    a = ax[1]
    d = P["reddit_comments_ext"]
    for path in d["paths"]["log10_rank"][:120]:
        y = np.array([np.nan if v is None else v for v in path], float)
        a.plot(np.arange(y.size) * d["paths"]["weeks_step"] / 52 + 2019, y, color=BLUE, alpha=0.18, lw=0.7)
    a.invert_yaxis()
    a.set_xticks([2019, 2020, 2021, 2022, 2023])
    a.set_xticklabels(["2019", "2020", "2021", "2022", "2023"])
    a.set_ylabel("log10 rank (weekly)")
    a.set_xlabel("year")
    a.set_title("The names: 120 random communities in the 2019 top 12,500")
    a = ax[2]
    for pn, d in P.items():
        ks = sorted(d["turnover"], key=int)
        for k, ls in zip(ks, (":", "--", "-")):
            y = [1.0] + d["turnover"][k]
            x = np.arange(len(y)) * (d["block"] / 52)
            a.plot(x, y, ls, color=cols[pn], marker="o", ms=3, label=f"{labs[pn]}, top {int(k):,}")
    a.set_ylim(0.4, 1.02)
    a.set_xlabel("years later")
    a.set_ylabel("share of the top k still in the top k")
    a.set_title("Turnover of the top sets")
    a.legend(fontsize=6.5, loc="lower left", ncol=1)
    fig.suptitle("Moving names, a slowly changing ladder: a fifth of each top set is replaced every year while the rank-size curve changes far more slowly", fontsize=11)
    fig.tight_layout()
    fig.savefig(HERE / "fig_puzzle.png", dpi=150)


def fig_sustained_ladder():
    """The ladder that a year's measured movement sustains, S*_k = sigma^2 / (2 (sbar_k - entry)), integrated
    over log rank from K upward, against the observed ladder of the persisting set (2z-cd cells, 2021->2022)."""
    fig, ax = plt.subplots(1, 2, figsize=(11, 4.6))
    for a, pn, lab, col in zip(ax, ("reddit_comments_ext", "reddit_submissions_long"), ("Reddit comments", "Reddit submissions"), (BLUE, ORANGE)):
        d = load(R / "2026-10-14_balance_law" / "cells" / f"law__{pn}.json")
        K = d["K"]
        for q, ls, tag in zip([q for q in d["pairs"] if q["primary"]], (":", "-"), ("2020→2021", "2021→2022")):
            rows = sorted(q["rows"], key=lambda r: r["k"])
            ks = np.array([r["k"] for r in rows], float)
            S = np.array([r["S"] for r in rows])
            Ss = np.array([np.nan if r["S_star"] is None else r["S_star"] for r in rows])
            def ladder(slopes):
                lv = np.zeros(ks.size)                       # level at k minus level at K, by the trapezoid rule in log rank
                for i in range(ks.size - 2, -1, -1):
                    lv[i] = lv[i + 1] + 0.5 * (slopes[i] + slopes[i + 1]) * np.log(ks[i + 1] / ks[i])
                return lv
            a.plot(ks / K, ladder(S), ls, color="k", lw=1.5, label=f"observed slope, {tag}")
            a.plot(ks / K, ladder(Ss), ls, color=col, lw=2, label=f"sustained by that year's movement, {tag}")
        a.set_xscale("log")
        a.set_xlabel("rank / K")
        a.set_ylabel("log level above rank K")
        a.set_title(lab)
        a.legend(fontsize=7.5, loc="upper right")
    fig.suptitle("The ladder each year's movement sustains, against the ladder observed (cut-offs K/64 … K)", fontsize=11)
    fig.tight_layout()
    fig.savefig(HERE / "fig_sustained_ladder.png", dpi=150)


def fig_why_it_curves():
    """Why the ladder is steeper at the bottom: lasting variance and net slide by rank, and the slope they sustain
    (2z-cd cells, both primary year pairs)."""
    fig, ax = plt.subplots(1, 3, figsize=(14, 4.3))
    for pn, lab, col in (("reddit_comments_ext", "comments", BLUE), ("reddit_submissions_long", "submissions", ORANGE)):
        d = load(R / "2026-10-14_balance_law" / "cells" / f"law__{pn}.json")
        K = d["K"]
        for q, ls, tag in zip([q for q in d["pairs"] if q["primary"]], (":", "-"), ("2020→21", "2021→22")):
            rows = sorted(q["rows"], key=lambda r: r["k"])
            k = np.array([r["k"] for r in rows]) / K
            if tag == "2020→21":                                      # one measurement per panel (2z-cv item 3)
                ax[0].plot(k, [1000 * r["sigma2"] for r in rows], "-", color=col, marker="o", ms=3, label=f"{lab} (one measurement, 2019 cohort)")
            ax[1].plot(k, [1000 * (r["sbar"] - r["entry"]) for r in rows], ls, color=col, marker="o", ms=3, label=f"{lab} {tag}")
            ax[2].plot(k, [r["S"] for r in rows], ls, color="k", lw=1, alpha=0.6)
            ax[2].plot(k, [np.nan if r["S_star"] is None else r["S_star"] for r in rows], ls, color=col, marker="o", ms=3, label=f"{lab} sustained, {tag}")
    ax[2].plot([], [], color="k", alpha=0.6, label="observed (both panels, both years)")
    for a, t, y in zip(ax, ("Lasting variance by rank", "Net slide by rank (members' slide plus entry)", "Slope: observed and sustained S* = σ²/2(net slide)"),
                       ("per 1,000 weeks", "per 1,000 weeks", "S (− d log level / d log rank)")):
        a.set_xscale("log")
        a.set_xlabel("rank / K")
        a.set_title(t, fontsize=10)
        a.set_ylabel(y)
        a.legend(fontsize=7)
    fig.suptitle("Why the ladder steepens down the ranks: variance rises and the net slide falls toward the bottom", fontsize=11)
    fig.tight_layout()
    fig.savefig(HERE / "fig_why_it_curves.png", dpi=150)


def fig_concentration():
    """The head's share of the top K's activity: observed by year and sustained by each year's movement
    (runs/2026-10-15_concentration; an index of exp(mean log activity), within the top K)."""
    c = load(R / "2026-10-15_concentration" / "concentration.json")
    fig, ax = plt.subplots(1, 2, figsize=(10.5, 4.2), sharey=True)
    for a, pn, lab, col in zip(ax, ("reddit_comments_ext", "reddit_submissions_long"), ("Reddit comments", "Reddit submissions"), (BLUE, ORANGE)):
        d = c[pn]
        K = d["K"]
        k0, k1 = str(K // 64), str(K // 16)
        yrs = list(d["observed_by_year"])
        x = np.arange(len(yrs))
        a.plot(x, [100 * d["observed_by_year"][y][k0] for y in yrs], "o-", color=col, label=f"top {int(k0):,} (K/64), observed")
        a.plot(x, [100 * d["observed_by_year"][y][k1] for y in yrs], "s--", color=col, alpha=0.7, label=f"top {int(k1):,} (K/16), observed")
        vals = [100 * v[k0] for v in d["sustained"].values()]
        for i, v in enumerate(vals):
            a.axhline(v, color=col, lw=1, ls=(0, (1, 2 + i)), alpha=0.9)
        if abs(vals[0] - vals[1]) < 4:
            a.text(len(yrs) - 0.95, max(vals) + 1, f"top K/64 sustained by 2020→21 and 2021→22 movement ({min(vals):.0f}–{max(vals):.0f}%)", fontsize=7, color=col, ha="right")
        else:
            for i, v in enumerate(vals):
                a.text(len(yrs) - 0.95, v + 1, f"top K/64 sustained by {'2020→21' if i == 0 else '2021→22'} movement", fontsize=7, color=col, ha="right")
        a.set_xticks(x)
        a.set_xticklabels([str(int(y[:4]) + (1 if y[5:7] == "12" else 0)) for y in yrs])     # blocks start in the last days of December
        a.set_xlim(-0.3, len(yrs) + 0.6)
        a.set_title(lab)
        ar = R / "2026-10-18_arithmetic_shares" / "arithmetic_shares.json"
        if ar.exists() and pn in load(ar):
            A = load(ar)[pn]["years"]
            a.plot(x, [100 * A[str(i)]["arith"][k0] for i in range(len(yrs))], "x", color="k", ms=7, label="top K/64, arithmetic share of activity")
        a.set_ylabel("index: share of exp(mean log activity) in the top K (%)")
        a.legend(fontsize=7.5, loc="lower left" if pn == "reddit_comments_ext" else "upper right")
    fig.suptitle("Where the head is going: its concentration index now, and the index its own movement sustains", fontsize=11)
    fig.tight_layout()
    fig.savefig(HERE / "fig_concentration.png", dpi=150)


def fig_instagram_censoring():
    """Instagram's "a"-query censoring and the boundary test (2z-cx): planted validation, and how much of the
    movement the matching-post count could carry."""
    v = load(R / "2026-10-15_instagram" / "planted_validation.json")
    d = load(R / "2026-10-15_instagram" / "ig_boundary__instagram_hm.json")["decomposition"]
    fig, ax = plt.subplots(1, 2, figsize=(11.5, 4.4))
    a = ax[0]
    names = ["truth", "censored", "q drift 0.002/wk", "q log-sd 2"]
    labels = ["truth", "censored\n(persistent q)", "censored,\nq drifting", "censored,\nwide q spread"]
    for i, nm in enumerate(names):
        vals = np.array([np.mean(x) for x in v[nm] if x is not None])
        a.scatter(np.full(vals.size, i) + np.linspace(-0.12, 0.12, vals.size), vals, color="#8e44ad" if i else "k", s=22)
        a.plot([i - 0.25, i + 0.25], [vals.mean()] * 2, color="#8e44ad" if i else "k", lw=2)
    a.axhspan(0.75, 1.33, color="#dddddd", alpha=0.6, lw=0)
    a.axhline(1, color=GREY, lw=1)
    a.set_xticks(range(len(names)))
    a.set_xticklabels(labels, fontsize=8)
    a.set_ylim(0.4, 1.6)
    a.set_ylabel("measured / predicted crossings, pooled K/8 … K")
    a.set_title("Planted worlds: the censoring leaves the test unbiased\n(default rules cannot read the censored panel: 0 of 6)", fontsize=9.5)
    a = ax[1]
    rows = [j for j, r in enumerate(d["log Y"]) if r["n"] >= 400]
    lab = [f"({d['log Y'][j]['lo']:.0f}, {d['log Y'][j]['hi']:.0f}]" for j in rows]
    m = np.array([d["log M"][j]["sigma2"] for j in rows]) * 1000
    y = np.array([d["log Ybar"][j]["sigma2"] for j in rows]) * 1000
    tot = np.array([d["log Y"][j]["sigma2"] for j in rows]) * 1000
    x = np.arange(len(rows))
    a.bar(x - 0.2, y, 0.4, color="#2a9d5c", label="engagement per matching post (q-free)")
    a.bar(x + 0.2, m, 0.4, color="#8e44ad", label="matching-post count (posting volume and q)")
    a.plot(x, tot, "ko-", label="total")
    a.set_xticks(x)
    a.set_xticklabels(lab, fontsize=7.5, rotation=15)
    a.set_xlabel("stratum of permanent rank")
    a.set_ylabel("displacement variance per 1,000 weeks (14→28 weeks)")
    a.set_title("Instagram totals: what the movement is made of", fontsize=9.5)
    a.set_ylim(0, 1.45 * tot.max())
    a.legend(fontsize=7.5, loc="upper right", ncol=1)
    fig.suptitle("Instagram's \"a\"-query censoring: modelled, validated, and bounded", fontsize=11)
    fig.tight_layout()
    fig.savefig(HERE / "fig_instagram_censoring.png", dpi=150)


def fig_lockin():
    """Ex ante forecasts of lock-in (2z-cy): retention of the top K/64 and K/4 by horizon on four ladders, forecast
    from movement against observed and constant turnover; and calibration of the per-member forecasts."""
    sets = [("reddit_comments_ext", "Reddit comments", BLUE), ("reddit_submissions_long", "Reddit submissions", ORANGE),
            ("facebook_a", "Facebook pages", "#2a9d5c"), ("instagram_hm", "Instagram (a-matching)", "#8e44ad")]
    fig, ax = plt.subplots(1, 5, figsize=(19, 4.2))
    allp, allo = [], []
    for a, (pn, lab, col) in zip(ax[:4], sets):
        r = load(R / "2026-10-15_lockin" / f"lockin__{pn}.json")
        bw = r["block"]
        for k, ls in zip((r["ks"][0], r["ks"][2]), ("-", "--")):
            hs = sorted({int(h) for c in r["origins"] for h in c["ks"][str(k)]["horizons"]})
            agg = {h: [c["ks"][str(k)]["horizons"][str(h)] for c in r["origins"] if str(h) in c["ks"][str(k)]["horizons"]] for h in hs}
            x = [0] + [h * bw / 52 for h in hs]
            a.plot(x, [1] + [np.mean([v["observed"] for v in agg[h]]) for h in hs], ls, color="k", marker="o", ms=3,
                   label=f"observed, top {k:,}")
            a.plot(x, [1] + [np.mean([v["model"] for v in agg[h]]) for h in hs], ls, color=col, marker="s", ms=3,
                   label=f"forecast from movement, top {k:,}")
            a.plot(x, [1] + [np.mean([v["geometric"] for v in agg[h]]) for h in hs], ls, color=GREY, lw=0.8,
                   label=f"constant turnover, top {k:,}" if k == r["ks"][0] else None)
            a.plot(x, [1] + [np.mean([v["logistic"] for v in agg[h]]) for h in hs], ls, color="#c0392b", lw=0.8,
                   label="logistic on log rank, log horizon" if k == r["ks"][0] else None)
        for c in r["origins"]:
            for v in c["ks"].values():
                for x_ in v["horizons"].values():
                    for b in x_["calibration"]:
                        allp.append((b["p"], b["obs"], b["n"]))
        a.set_title(lab, fontsize=10)
        a.set_xlabel("years later")
        a.set_ylabel("share still in the top k")
        a.set_ylim(0.4, 1.02)
        a.legend(fontsize=6.3, loc="lower left")
    a = ax[4]
    P = np.array(allp)
    bins = np.linspace(0, 1, 11)
    idx = np.digitize(P[:, 0], bins) - 1
    xs, ys = [], []
    for b in range(10):
        m = idx == b
        if P[m, 2].sum() > 0:
            xs.append(np.average(P[m, 0], weights=P[m, 2]))
            ys.append(np.average(P[m, 1], weights=P[m, 2]))
    a.plot([0, 1], [0, 1], color=GREY, lw=1)
    a.plot(xs, ys, "ko-")
    a.set_xlabel("forecast probability of staying")
    a.set_ylabel("share that stayed")
    a.set_title("Calibration, all ladders, origins, k, h", fontsize=10)
    fig.suptitle("Retention forecast ex ante from measured movement, against constant turnover and a logistic on rank fitted to earlier outcomes", fontsize=11)
    fig.tight_layout()
    fig.savefig(HERE / "fig_lockin.png", dpi=150)


if __name__ == "__main__":
    fig_lockin()
    fig_instagram_censoring()
    fig_four_ladders()
    fig_concentration()
    fig_why_it_curves()
    fig_sustained_ladder()
    fig_puzzle()
    fig_boundary()
    fig_law_and_head()
    fig_three_ladders()
    print("written:", *sorted(p.name for p in HERE.glob("*.png")))
