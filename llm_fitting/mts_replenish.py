"""Replenishment test (movement-to-structure; design in runs/2026-10-14_replenishment/DESIGN.md).

How much of the incumbents' slide is displacement by entrants, and does the rest equal the
lasting reordering rate?  Exact decomposition, for a cohort fixed on an earlier block:

    slide = slide_P + I,     I = E - X

slide    the cohort's mean change against the centre of all endpoints (step 1's slide)
slide_P  the same against the centre of the endpoints with a level in both blocks
E        what entrants (a level at the end, none at the start) add to the end centre
X        what exits (a level at the start, none at the end) added to the start centre

Then two things that are not identities: whether slide_P agrees with lasting rates measured
from reordering and from variance, and whether named endpoints' mean change depends on
their rank (it does not if nothing but diffusion and entry moves them).

    python llm_fitting/mts_replenish.py run <panel>
    python llm_fitting/mts_replenish.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-14_replenishment")
PANELS = ("reddit_comments_ext", "reddit_submissions_long")
FLAG_KIND = {"reddit_comments_ext": "comments", "reddit_submissions_long": "submissions"}
REORDER_REF = {"reddit_comments_ext": (0.0034, 0.0036),          # MODEL_STATUS 2z-by, item 5
               "reddit_submissions_long": (0.0035, 0.0040)}
BOOT = 400
DEV = 0.0010          # a bin's departure from the cohort's mean change that counts, per week
R1_RANGE = {"reddit_comments_ext": (0.20, 0.40), "reddit_submissions_long": (0.05, 0.25)}


def _top_mean(L, mask, K):
    v = L[mask]
    if v.size < K:
        return np.nan
    return float(-np.partition(-v, K - 1)[:K].mean())


def _kth(L, mask, K):
    v = L[mask]
    return float(-np.partition(-v, K - 1)[K - 1]) if v.size >= K else np.nan


def _pool(means, ns):
    means, ns = np.asarray(means, float), np.asarray(ns, float)
    ok = np.isfinite(means)
    return float((means[ok] * ns[ok]).sum() / ns[ok].sum()) if ok.any() else np.nan


def analyse(df, K, block, boot=BOOT, drop_weeks=None, seed=0, dev=DEV):
    X, R, floors, outside, ids = m1.matrices(df, K)
    if drop_weeks:
        X = X.copy()
        X[list(drop_weeks)] = np.nan
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
    n_cert = []
    for b in range(nb):
        seg = outside[start + b * block:start + (b + 1) * block]
        bd = float(np.nanmax(seg)) if np.isfinite(seg).any() else -np.inf
        n_cert.append(int((M[b][np.isfinite(M[b])] > bd).sum()))
    seen = np.isfinite(X)
    first = np.where(seen.any(axis=0), seen.argmax(axis=0), T)
    pr, pres = ba.perm_rank(R, start, start + block, floors)
    labels, which = ba.cohort_bins(pr, 2 * K, pres, pres_floor=m1.PRES)
    bins = [j for j, lb in enumerate(labels) if lb != "outside" and lb[1] >= K / 64 - 1e-9]
    inner = [j for j in bins if labels[j][1] <= K + 1e-9]
    rng = np.random.default_rng(seed)
    out = dict(K=int(K), T=int(T), block=int(block), n_tracked=int(X.shape[1]), dropped_weeks=sorted(drop_weeks or []),
               blocks=dict(n=int(nb), start=int(start), certified_top=n_cert),
               bins=[list(map(float, labels[j])) for j in bins], pairs=[])

    for a in range(1, nb - 1):
        for b in range(a + 1, nb):
            D = (b - a) * block
            pa, pb = np.isfinite(M[a]), np.isfinite(M[b])
            P = pa & pb
            pair = dict(start=a, end=b, weeks=int(D))
            if min(n_cert[a], n_cert[b]) < K or P.sum() < K:
                pair["not_adjudicated"] = "the centre's ranks are not certified exact in both blocks"
                out["pairs"].append(pair)
                continue
            ca = (_top_mean(M[a], pa, K), _top_mean(M[b], pb, K))
            cp = (_top_mean(M[a], P, K), _top_mean(M[b], P, K))
            ent, ext = pb & ~pa, pa & ~pb
            new = ent & (first >= start + a * block)
            E = (ca[1] - cp[1]) / D
            Xo = (ca[0] - cp[0]) / D
            E_new = (ca[1] - _top_mean(M[b], pb & ~new, K)) / D
            ka, kb = _kth(M[a], pa, K), _kth(M[b], pb, K)
            d = M[b] - M[a]
            xs = {j: d[(which == j) & P] for j in bins}
            ns = {j: xs[j].size for j in bins}
            mean = {j: (float(xs[j].mean()) if ns[j] >= 5 else np.nan) for j in bins}
            raw_in = _pool([mean[j] for j in inner], [ns[j] for j in inner])
            raw_all = _pool([mean[j] for j in bins], [ns[j] for j in bins])
            bm = np.full((boot, len(bins)), np.nan)
            for i, j in enumerate(bins):
                if ns[j] >= 5:
                    bm[:, i] = xs[j][rng.integers(0, ns[j], (boot, ns[j]))].mean(axis=1)
            nvec = np.array([ns[j] for j in bins], float)
            in_ix = [bins.index(j) for j in inner]
            okc = np.isfinite(bm[0])
            b_in = np.array([_pool(bm[t, in_ix], nvec[in_ix]) for t in range(boot)])
            b_all = (np.where(okc, bm, 0) * nvec).sum(axis=1) / nvec[okc].sum()
            q = lambda v: [float(np.nanquantile(v, 0.05)), float(np.nanquantile(v, 0.95))]  # noqa: E731
            slide = -(raw_in - (ca[1] - ca[0])) / D
            slide_P = -(raw_in - (cp[1] - cp[0])) / D
            rows = []
            for i, j in enumerate(bins):
                devs = (bm[:, i] - b_all) / D
                rows.append(dict(bin=list(map(float, labels[j])), n=int(ns[j]),
                                 n_start=int(((which == j) & pa).sum()),
                                 slide_P=None if not np.isfinite(mean[j]) else float(-(mean[j] - (cp[1] - cp[0])) / D),
                                 dev=None if not np.isfinite(mean[j]) else float((mean[j] - raw_all) / D),
                                 dev_q=None if not np.isfinite(mean[j]) else q(devs),
                                 dev_sd=None if not np.isfinite(mean[j]) else float(np.nanstd(devs))))
            # tilt: does the mean change of named endpoints depend on their rank?  Weighted least squares of
            # the bin means on log2 of the bin's upper edge; reported as bottom bin minus top bin, per week.
            use = [i for i, j in enumerate(bins) if np.isfinite(mean[j])]
            tilt = tilt_q = None
            if len(use) >= 4:
                lx = np.log2([labels[bins[i]][1] for i in use])
                w = 1.0 / np.maximum(np.nanvar(bm[:, use], axis=0), 1e-18)
                lxc = lx - (w * lx).sum() / w.sum()
                span = lx.max() - lx.min()
                fit = lambda y: float((w * lxc * y).sum() / (w * lxc * lxc).sum() * span / D)  # noqa: E731
                tilt = fit(np.array([mean[bins[i]] for i in use]))
                tilt_q = q(np.array([fit(bm[t, use]) for t in range(boot)]))
            pair.update(
                slide=float(slide), slide_q=q(-(b_in - (ca[1] - ca[0])) / D),
                slide_P=float(slide_P), slide_P_q=q(-(b_in - (cp[1] - cp[0])) / D),
                I=float(E - Xo), E=float(E), X=float(Xo), E_new=float(E_new),
                identity_residual=float(slide - slide_P - (E - Xo)),
                centre_all=[float(ca[0]), float(ca[1])], centre_P=[float(cp[0]), float(cp[1])],
                n_both=int(P.sum()),
                n_entrants_topK=int((ent & (M[b] >= kb)).sum()), n_new_topK=int((new & (M[b] >= kb)).sum()),
                n_exits_topK=int((ext & (M[a] >= ka)).sum()), tilt=tilt, tilt_q=tilt_q, bins=rows)
            out["pairs"].append(pair)

    # ---- lasting variance from the growth of the variance with spacing, cohort bins, blocks 1, 2, 3 ----
    if nb >= 4 and min(n_cert[1:4]) >= K:
        tri = np.isfinite(M[1]) & np.isfinite(M[2]) & np.isfinite(M[3])
        lads = [ba.ladder(M[t]) for t in (1, 2, 3)]
        rows, draws, wts = [], [], []
        for j in bins:
            m = (which == j) & tri
            n = int(m.sum())
            lo, hi = labels[j]
            S = float(np.mean([ba.local_slope(L, lo, hi) for L in lads]))
            row = dict(bin=[float(lo), float(hi)], n=n, S=S)
            if n >= 20 and np.isfinite(S) and S > 0:
                d12, d23, d13 = M[2][m] - M[1][m], M[3][m] - M[2][m], M[3][m] - M[1][m]
                s2 = (d13.var() - 0.5 * (d12.var() + d23.var())) / block
                ix = rng.integers(0, n, (boot, n))
                bs = (d13[ix].var(axis=1) - 0.5 * (d12[ix].var(axis=1) + d23[ix].var(axis=1))) / block
                row.update(sigma2=float(s2), c_var=float(s2 / (2 * S)),
                           c_var_q=[float(np.quantile(bs, 0.05) / (2 * S)), float(np.quantile(bs, 0.95) / (2 * S))])
                if j in inner:
                    draws.append(bs / (2 * S))
                    wts.append(n)
            rows.append(row)
        if draws:
            w = np.array(wts, float)
            pooled = (np.array(draws) * w[:, None]).sum(axis=0) / w.sum()
            point = _pool([r["c_var"] for r in rows if "c_var" in r and r["bin"][1] <= K + 1e-9], wts)
            out["variance_route"] = dict(c_var=float(point), c_var_q=[float(np.quantile(pooled, 0.05)), float(np.quantile(pooled, 0.95))],
                                         n_triple=int(tri.sum()), bins=rows)
    return out


def _pair(res, a, b):
    for p in res["pairs"]:
        if p["start"] == a and p["end"] == b:
            return p
    return None


def score(res, panel, dev=DEV):
    """The predictions of DESIGN.md, by its fixed rules."""
    sc = {}
    K = res["K"]
    pairs = [p for p in (_pair(res, 1, 2), _pair(res, 2, 3)) if p and "slide" in p]
    if len(pairs) < 2:
        return dict(status=m1.NA)
    prim = pairs[1]
    lo, hi = R1_RANGE[panel]
    share = [p["I"] / p["slide"] for p in pairs]
    sc["R1"] = dict(status="PASS" if all(lo <= s <= hi for s in share) else "FAIL", share=share, range=[lo, hi])
    rlo, rhi = REORDER_REF[panel]
    sc["R2"] = dict(status="PASS" if 0.85 * rlo <= prim["slide_P"] <= 1.15 * rhi else "FAIL",
                    slide_P=prim["slide_P"], reference=[rlo, rhi])
    if "variance_route" in res:
        r = res["variance_route"]["c_var"] / prim["slide_P"]
        sc["R3"] = dict(status="PASS" if 0.75 <= r <= 1.25 else "FAIL", ratio=float(r))
    else:
        sc["R3"] = dict(status=m1.NA)
    newshare = [p["E_new"] / p["E"] if p["E"] > 0 else np.nan for p in pairs]
    sc["R4"] = dict(status="PASS" if all(np.isfinite(s) and s >= 0.70 for s in newshare) else "FAIL", share=newshare)

    def departs(row, sign=0):
        """A bin's mean change is more than `dev` a week from the cohort's and more than three
        bootstrap standard deviations from it."""
        if row["dev"] is None or not row["dev_sd"]:
            return False
        return (abs(row["dev"]) > dev and abs(row["dev"]) > 3 * row["dev_sd"]
                and (sign == 0 or np.sign(row["dev"]) == sign))

    def tilted(p, sign=0):
        """The fitted difference between the bottom and the top bin exceeds `dev` a week, with a
        5-95% interval that excludes zero."""
        if p["tilt"] is None:
            return False
        return (abs(p["tilt"]) > dev and (p["tilt_q"][0] > 0 or p["tilt_q"][1] < 0)
                and (sign == 0 or np.sign(p["tilt"]) == sign))
    tilts = [dict(blocks=[p["start"], p["end"]], tilt=p["tilt"], q=p["tilt_q"]) for p in pairs]
    if panel == "reddit_comments_ext":
        sc["R5a"] = dict(status="PASS" if not any(tilted(p) for p in pairs) else "FAIL", tilts=tilts)
    else:
        sc["R5b"] = dict(status="PASS" if tilted(prim, sign=1) else "FAIL", tilts=tilts)
    below = [(p["start"], p["end"], r["dev"], r["dev_sd"]) for p in pairs for r in p["bins"]
             if r["bin"][0] >= K - 1e-9 and departs(r, sign=1)]
    sc["R5c"] = dict(status="PASS" if not below else "FAIL", rising=below)
    sc["departures"] = [(p["start"], p["end"], r["bin"], r["dev"], r["dev_sd"]) for p in pairs for r in p["bins"] if departs(r)]
    return sc


def run(panel):
    import minimal_rankdiff as mrd
    import pandas as pd
    import instrument_eras as ie
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    res = dict(panel=panel, all_weeks=analyse(df, cfg["K"], cfg["block"]))
    res["all_weeks"]["scores"] = score(res["all_weeks"], panel)
    weeks = {pd.Timestamp(x) for x in ie.reddit_flagged_weeks(FLAG_KIND[panel])}
    ts = df.groupby("period")["ts"].first()
    flagged = [int(p) for p, d in ts.items() if pd.Timestamp(d) in weeks]
    if flagged:
        res["flagged_weeks_dropped"] = analyse(df, cfg["K"], cfg["block"], drop_weeks=flagged)
        res["flagged_weeks_dropped"]["scores"] = score(res["flagged_weeks_dropped"], panel)
    res["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"replenish__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    a = res["all_weeks"]
    print(f"[{panel}] T={a['T']} tracked={a['n_tracked']} blocks={a['blocks']['n']} certified_top={a['blocks']['certified_top']} "
          f"flagged weeks dropped in the second run: {flagged} written", flush=True)


def report():
    lines = []
    for panel in PANELS:
        f = OUT / "cells" / f"replenish__{panel}.json"
        if not f.exists():
            continue
        res = json.loads(f.read_text())
        for key in ("all_weeks", "flagged_weeks_dropped"):
            if key not in res:
                continue
            r = res[key]
            lines += [f"\n## {panel}, {key.replace('_', ' ')}  (K = {r['K']}, blocks of {r['block']} weeks, "
                      f"certified top {r['blocks']['certified_top']}, dropped weeks {r['dropped_weeks']})", "",
                      "Per endpoint per week; cohort fixed on block 0; seven octave bins from (K/128, K/64] to (K/2, K].", "",
                      "| blocks | weeks | slide (5-95%) | entry E | exit X | I = E - X | I / slide | slide_P (5-95%) | new share of E | entrants, new, exits in the top K | identity residual |",
                      "|---|---|---|---|---|---|---|---|---|---|---|"]
            for p in r["pairs"]:
                if "slide" not in p:
                    lines.append(f"| {p['start']}→{p['end']} | {p['weeks']} | not adjudicated | | | | | | | | |")
                    continue
                lines.append(f"| {p['start']}→{p['end']} | {p['weeks']} | {p['slide']:.5f} ({p['slide_q'][0]:.5f} to {p['slide_q'][1]:.5f}) | "
                             f"{p['E']:.5f} | {p['X']:.5f} | {p['I']:.5f} | {p['I'] / p['slide']:.0%} | "
                             f"{p['slide_P']:.5f} ({p['slide_P_q'][0]:.5f} to {p['slide_P_q'][1]:.5f}) | "
                             f"{(p['E_new'] / p['E'] if p['E'] > 0 else float('nan')):.0%} | "
                             f"{p['n_entrants_topK']}, {p['n_new_topK']}, {p['n_exits_topK']} | {p['identity_residual']:.1e} |")
            if "variance_route" in r:
                v = r["variance_route"]
                lines += ["", f"Lasting variance route, cohort bins, blocks 1 to 3: pooled c_var = {v['c_var']:.5f} "
                              f"({v['c_var_q'][0]:.5f} to {v['c_var_q'][1]:.5f}); reordering route (2z-by): "
                              f"{REORDER_REF[panel][0]} to {REORDER_REF[panel][1]}", "",
                          "| bin of permanent rank | n | slope S | lasting variance per week | c_var (5-95%) |", "|---|---|---|---|---|"]
                for b in v["bins"]:
                    if "c_var" in b:
                        lines.append(f"| ({b['bin'][0]:.0f}, {b['bin'][1]:.0f}] | {b['n']} | {b['S']:.3f} | {b['sigma2']:.5f} | "
                                     f"{b['c_var']:.5f} ({b['c_var_q'][0]:.5f} to {b['c_var_q'][1]:.5f}) |")
                    else:
                        lines.append(f"| ({b['bin'][0]:.0f}, {b['bin'][1]:.0f}] | {b['n']} | {b['S']:.3f} | too few | |")
            lines += ["", "Tilt of the named cohort (fitted bottom bin minus top bin, per week; 5-95%): " + "; ".join(
                f"{p['start']}→{p['end']} {p['tilt']:+.5f} ({p['tilt_q'][0]:+.5f} to {p['tilt_q'][1]:+.5f})"
                for p in r["pairs"] if p.get("tilt") is not None)]
            lines += ["", "Mean change of each bin against the mean of all eight (per week; positive = rises against the cohort):", "",
                      "| bin of permanent rank | " + " | ".join(f"{p['start']}→{p['end']} (5-95%)" for p in r["pairs"] if "slide" in p) + " |",
                      "|---|" + "---|" * sum("slide" in p for p in r["pairs"])]
            for i, lb in enumerate(r["bins"]):
                cells = []
                for p in r["pairs"]:
                    if "slide" not in p:
                        continue
                    b = p["bins"][i]
                    cells.append("too few" if b["dev"] is None else
                                 f"{b['dev']:+.5f} ({b['dev_q'][0]:+.5f} to {b['dev_q'][1]:+.5f}), z {b['dev'] / b['dev_sd']:+.1f}, n {b['n']}")
                lines.append(f"| ({lb[0]:.0f}, {lb[1]:.0f}] | " + " | ".join(cells) + " |")
            lines += ["", "Scores: " + "; ".join(f"{k} {v['status']}" for k, v in r["scores"].items() if isinstance(v, dict) and "status" in v)]
            lines += ["", "Score details: " + json.dumps(r["scores"], default=float)]
    (OUT / "report_tables.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) == 2 and args[1] in PANELS:
        run(args[1])
    elif args[:1] == ["report"]:
        report()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
