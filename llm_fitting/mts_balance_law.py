"""The balance by rank, out of equilibrium (design in runs/2026-10-14_balance_law/DESIGN.md).

For the endpoints with a level in both of two blocks (a closed set P), with U_k the mean
level of its top k positions and shape_k = U_k - U_K:

    d shape_k / dt  =  c_k - sbar_k            (k < K)
    c_k    = sigma_k^2 / (2 S_k)   lasting variance at rank k over twice the local slope there
    sbar_k = mean slide, against U_K, of the named endpoints ranked above k

The left side is read off the ladder.  The right side uses only the first two moments of
named endpoints' movement by rank, and the slope at the start.  Entry is handled exactly
and apart: shape change of all endpoints = shape change of P + an entry term computed from
the two ladders with and without entrants and exits.

    python llm_fitting/mts_balance_law.py run <panel>
    python llm_fitting/mts_balance_law.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-14_balance_law")
PANELS = ("reddit_comments_ext", "reddit_submissions_long")
BOOT = 300
WIDTH = np.sqrt(2.0)


def cutoffs(K):
    """Cut-offs K/64 ... K as (index of the stratum that ends there, position on the ladder).
    The strata's edges are fractions of K and need not be whole numbers; the cut-off is the
    edge, and the position is the edge rounded."""
    st = strata(K)
    return [(j, int(round(hi))) for j, (lo, hi) in enumerate(st) if K / 64 - 1e-9 <= hi <= K + 1e-9]


def strata(K):
    """Rank strata (lo, hi]: one pooled stratum at the top, then octaves down to (K, 2K]."""
    e = [0.0, K / 128.0]
    while e[-1] < 2 * K - 1e-9:
        e.append(e[-1] * 2)
    return [(e[i], e[i + 1]) for i in range(len(e) - 1)]


def _lad(L, mask):
    return np.sort(L[mask])[::-1]


def _U(lad, k):
    return float(lad[:k].mean()) if lad.size >= k else np.nan


def _slope(lad, k):
    return ba.local_slope(lad, k / WIDTH, min(k * WIDTH, lad.size))


def _assign(pr, pres, st):
    which = np.full(pr.size, -1)
    ok = pres >= m1.PRES
    for j, (lo, hi) in enumerate(st):
        which[ok & (pr > lo) & (pr <= hi)] = j
    return which


def lasting_variance(M, which, st, blocks, block, rng, boot):
    """Per stratum: [var(change a->c) - (var(a->b) + var(b->c)) / 2] / spacing, for the cohort
    members with a level in all three blocks, `block` being the spacing a->b in weeks.  The
    averaging offset and a constant left by passing swings cancel in the difference."""
    a, b, c = blocks
    tri = np.isfinite(M[a]) & np.isfinite(M[b]) & np.isfinite(M[c])
    out, draws = [], []
    for j in range(len(st)):
        m = (which == j) & tri
        n = int(m.sum())
        if n < 20:
            out.append(dict(n=n, sigma2=np.nan))
            draws.append(np.full(boot, np.nan))
            continue
        d1, d2, d3 = M[b][m] - M[a][m], M[c][m] - M[b][m], M[c][m] - M[a][m]
        s2 = (d3.var() - 0.5 * (d1.var() + d2.var())) / block
        ix = rng.integers(0, n, (boot, n))
        bs = (d3[ix].var(axis=1) - 0.5 * (d1[ix].var(axis=1) + d2[ix].var(axis=1))) / block
        out.append(dict(n=n, sigma2=float(s2)))
        draws.append(bs)
    return out, np.array(draws)


def _at_boundary(vals, wts, j_end):
    """Value at the boundary where stratum j_end ends: mean of it and the next, weighted by members."""
    js = [j for j in (j_end, j_end + 1) if j < len(vals)]
    v = np.array([vals[j] for j in js], float)
    w = np.array([wts[j] for j in js], float)
    ok = np.isfinite(v)
    return float((v[ok] * w[ok]).sum() / w[ok].sum()) if ok.any() else np.nan


def pair_terms(M, which, st, a, b, K, ks, block, rng, boot):
    """Observed shape change and the slide side of the law for one pair of blocks."""
    D = (b - a) * block
    pa, pb = np.isfinite(M[a]), np.isfinite(M[b])
    P = pa & pb
    lad = dict(all_a=_lad(M[a], pa), all_b=_lad(M[b], pb), P_a=_lad(M[a], P), P_b=_lad(M[b], P))
    dUK_P = _U(lad["P_b"], K) - _U(lad["P_a"], K)
    d = M[b] - M[a]
    width = np.array([hi - lo for lo, hi in st])
    xs = [d[(which == j) & P] for j in range(len(st))]
    ns = np.array([x.size for x in xs])
    slide = np.array([-(x.mean() - dUK_P) / D if x.size >= 5 else np.nan for x in xs])
    bs = np.full((boot, len(st)), np.nan)
    for j, x in enumerate(xs):
        if x.size >= 5:
            bs[:, j] = -(x[rng.integers(0, x.size, (boot, x.size))].mean(axis=1) - dUK_P) / D
    rows = []
    for j_end, k in ks:
        js = list(range(j_end + 1))
        ok = [j for j in js if np.isfinite(slide[j])]
        cover = width[ok].sum() / width[js].sum() if js else 0.0
        sbar = float((slide[ok] * width[ok]).sum() / width[ok].sum()) if ok else np.nan
        sbar_bs = (bs[:, ok] * width[ok]).sum(axis=1) / width[ok].sum() if ok else np.full(boot, np.nan)
        shp = {s: (_U(lad[s + "_b"], k) - _U(lad[s + "_b"], K)) - (_U(lad[s + "_a"], k) - _U(lad[s + "_a"], K)) for s in ("all", "P")}
        rows.append(dict(k=int(k), obs_P=float(shp["P"] / D), obs_all=float(shp["all"] / D),
                         entry=float((shp["all"] - shp["P"]) / D), sbar=sbar, sbar_bs=sbar_bs, cover=float(cover),
                         S_a=_slope(lad["P_a"], k), S_b=_slope(lad["P_b"], k), S_all_a=_slope(lad["all_a"], k)))
    return dict(start=a, end=b, weeks=int(D), n_both=int(P.sum()), rows=rows,
                strata=[dict(lo=float(lo), hi=float(hi), n=int(n), slide=None if not np.isfinite(s) else float(s))
                        for (lo, hi), n, s in zip(st, ns, slide)])


def analyse(df, K, block, boot=BOOT, seed=0, step=1):
    """step: pairs (a, a + step) in blocks.  With 52-week blocks (step 1) this is the design of
    2z-cd exactly.  With 26-week blocks and step 2 the pairs are a year apart, start every half
    year, and the annual cycle does not enter a change; the variance then comes from blocks 1,
    3, 5 of the cohort fixed on block 0."""
    X, R, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
    n_cert = []
    for b in range(nb):
        seg = outside[start + b * block:start + (b + 1) * block]
        bd = float(np.nanmax(seg)) if np.isfinite(seg).any() else -np.inf
        n_cert.append(int((M[b][np.isfinite(M[b])] > bd).sum()))
    rng = np.random.default_rng(seed)
    st, ks = strata(K), cutoffs(K)
    dates = (df.groupby("period")["ts"].first().reindex(range(T)).astype(str).tolist() if "ts" in df.columns else None)
    out = dict(K=int(K), T=int(T), block=int(block), step=int(step), n_tracked=int(X.shape[1]),
               blocks=dict(n=int(nb), start=int(start), certified_top=n_cert,
                           first_week=None if dates is None else [dates[start + b * block] for b in range(nb)]),
               strata=[[float(lo), float(hi)] for lo, hi in st], ks=[k for _, k in ks], pairs=[])
    vb = (1, 2, 3) if step == 1 else (1, 1 + step, 1 + 2 * step)
    if nb < vb[-1] + 1 or min(n_cert) < int(np.ceil(WIDTH * K)):
        out["not_adjudicated"] = "needs the variance blocks and ranks to 1.42 K certified exact"
        return out
    which = {}
    for sel in range(nb - step - 1):
        pr, pres = ba.perm_rank(R, start + sel * block, start + (sel + 1) * block, floors)
        which[sel] = _assign(pr, pres, st)
    var_rows, var_bs = lasting_variance(M, which[0], st, vb, step * block, rng, boot)
    out["variance"] = [dict(lo=float(lo), hi=float(hi), **v) for (lo, hi), v in zip(st, var_rows)]
    out["variance_blocks"] = list(vb)
    vn = [v["n"] for v in var_rows]
    s2 = {k: _at_boundary([v["sigma2"] for v in var_rows], vn, j) for j, k in ks}
    s2_bs = {k: np.array([_at_boundary(var_bs[:, t], vn, j) for t in range(boot)]) for j, k in ks}

    keys = [(a, a + step, a - 1) for a in range(1, nb - step)]
    if step == 1:
        keys = [(1, 2, 0), (2, 3, 1), (2, 3, 0)]
    terms = {key: pair_terms(M, which[key[2]], st, key[0], key[1], K, ks, block, rng, boot) for key in keys}
    q = lambda v: ([float(np.nanquantile(v, 0.05)), float(np.nanquantile(v, 0.95))]  # noqa: E731
                   if np.isfinite(v).any() else [np.nan, np.nan])
    for (a, b, sel), t in terms.items():
        lag = terms.get((a - step, a, a - step - 1))
        if step == 1:
            lag = terms[(1, 2, 0)] if (a, b) == (2, 3) else None
        rows = []
        for i, r in enumerate(t["rows"]):
            k = r["k"]
            S = 0.5 * (r["S_a"] + r["S_b"])
            c = s2[k] / (2 * S)
            c_bs = s2_bs[k] / (2 * S)
            pred = c - r["sbar"]
            row = dict(k=k, obs_P=r["obs_P"], obs_all=r["obs_all"], entry=r["entry"], S=float(S), S_start=float(r["S_a"]),
                       sigma2=float(s2[k]), c=float(c), c_q=q(c_bs), sbar=float(r["sbar"]), sbar_q=q(r["sbar_bs"]),
                       cover=r["cover"], pred=float(pred), pred_q=q(c_bs - r["sbar_bs"]),
                       S_star=float(s2[k] / (2 * (r["sbar"] - r["entry"]))) if r["sbar"] - r["entry"] > 0 else None,
                       S_all=float(r["S_all_a"]))
            if lag is not None:
                lr = lag["rows"][i]
                c0 = s2[k] / (2 * r["S_a"])                       # slope at the start only: nothing from the target year
                row.update(pred_lag=float(c0 - lr["sbar"]), pred_lag_q=q(s2_bs[k] / (2 * r["S_a"]) - lr["sbar_bs"]),
                           last_year=float(lr["obs_P"]))
            rows.append(row)
        out["pairs"].append(dict(start=a, end=b, weeks=t["weeks"], cohort_on=sel, n_both=t["n_both"],
                                 primary=bool(sel == a - 1), variance_out_of_sample=bool(vb[-1] <= a),
                                 strata=t["strata"], rows=rows))
    return out


def summary(rows, key="pred", obs="obs_P"):
    """Fit of predicted to observed over cut-offs below K: slope through the origin, rms errors."""
    r = [x for x in rows if x.get(key) is not None and np.isfinite(x[key]) and np.isfinite(x[obs])]
    if not r:
        return None
    p, o = np.array([x[key] for x in r]), np.array([x[obs] for x in r])
    return dict(n=len(r), slope=float((p * o).sum() / (p * p).sum()) if (p * p).sum() > 0 else np.nan,
                rmse=float(np.sqrt(((o - p) ** 2).mean())), rmse_zero=float(np.sqrt((o ** 2).mean())),
                sign_agree=int((np.sign(p) == np.sign(o)).sum()),
                corr=float(np.corrcoef(p, o)[0, 1]) if len(r) > 2 and p.std() > 0 and o.std() > 0 else np.nan)


def run(panel, halfyear=False):
    import minimal_rankdiff as mrd
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    res = analyse(df, cfg["K"], 26, step=2) if halfyear else analyse(df, cfg["K"], cfg["block"])
    res.update(panel=panel, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    out_dir = OUT if not halfyear else Path("llm_fitting/runs/2026-10-14_balance_law_halfyear")
    (out_dir / "cells").mkdir(parents=True, exist_ok=True)
    (out_dir / "cells" / f"law__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] T={res['T']} tracked={res['n_tracked']} blocks={res['blocks']['n']} "
          f"certified_top={res['blocks']['certified_top']} written", flush=True)


def report():
    lines, pooled, pooled_lag = [], [], []
    for panel in PANELS:
        f = OUT / "cells" / f"law__{panel}.json"
        if not f.exists():
            continue
        res = json.loads(f.read_text())
        K = res["K"]
        lines += [f"\n## {panel}  (K = {K}, blocks of {res['block']} weeks, certified top {res['blocks']['certified_top']})", "",
                  "Lasting variance per week by stratum of permanent rank (cohort of block 0, blocks 1 to 3): " +
                  "; ".join(f"({v['lo']:.0f}, {v['hi']:.0f}] {v['sigma2']:.4f} (n {v['n']})" for v in res["variance"])]
        for p in res["pairs"]:
            below = [r for r in p["rows"] if r["k"] < K]
            sm = summary(below)
            tag = "primary" if p["primary"] else "secondary cohort"
            lines += ["", f"### blocks {p['start']}→{p['end']}, cohort fixed on block {p['cohort_on']} ({tag}); per endpoint per week", "",
                      "| k | slope S | c = σ²/2S (5-95%) | slide above k (5-95%) | predicted c − slide (5-95%) | observed, persisting set | entry term | observed, all | S* / S |",
                      "|---|---|---|---|---|---|---|---|---|"]
            for r in p["rows"]:
                star = "" if r["S_star"] is None else f"{r['S_star'] / r['S_all']:.2f}"
                lines.append(f"| {r['k']} | {r['S']:.3f} | {r['c']:.5f} ({r['c_q'][0]:.5f} to {r['c_q'][1]:.5f}) | "
                             f"{r['sbar']:.5f} ({r['sbar_q'][0]:.5f} to {r['sbar_q'][1]:.5f}) | "
                             f"{r['pred']:+.5f} ({r['pred_q'][0]:+.5f} to {r['pred_q'][1]:+.5f}) | {r['obs_P']:+.5f} | "
                             f"{r['entry']:+.5f} | {r['obs_all']:+.5f} | {star} |")
            lines += ["", f"Below K: slope of observed on predicted {sm['slope']:.2f}, correlation {sm['corr']:.2f}, rms error {sm['rmse']:.5f} "
                          f"against {sm['rmse_zero']:.5f} for no change; sign agrees in {sm['sign_agree']} of {sm['n']}."]
            if p["primary"]:
                pooled += below
            if p["primary"] and "pred_lag" in p["rows"][0]:
                sl = summary(below, "pred_lag")
                ly = summary(below, "last_year")
                lines += ["", "Lagged: slides from blocks 1→2, slope at the start of the pair, variance from blocks 1 to 3.", "",
                          "| k | lagged prediction (5-95%) | last year's change | observed |", "|---|---|---|---|"]
                for r in below:
                    lines.append(f"| {r['k']} | {r['pred_lag']:+.5f} ({r['pred_lag_q'][0]:+.5f} to {r['pred_lag_q'][1]:+.5f}) | {r['last_year']:+.5f} | {r['obs_P']:+.5f} |")
                lines += ["", f"rms error: lagged prediction {sl['rmse']:.5f}; repeat last year's change {ly['rmse']:.5f}; no change {sl['rmse_zero']:.5f}. "
                              f"Slope of observed on the lagged prediction {sl['slope']:.2f}; sign agrees in {sl['sign_agree']} of {sl['n']}."]
                pooled_lag += below
    if pooled:
        sm = summary(pooled)
        big = [r for r in pooled if abs(r["pred"]) > 0.0010]
        agree = sum(np.sign(r["pred"]) == np.sign(r["obs_P"]) for r in big)
        lines += ["", "## Pooled over panels and primary pairs, cut-offs below K", "",
                  f"{sm['n']} cells: slope of observed on predicted {sm['slope']:.2f}, correlation {sm['corr']:.2f}, rms error {sm['rmse']:.5f} "
                  f"against {sm['rmse_zero']:.5f} for no change. Where the prediction exceeds 0.0010 a week in size: sign agrees in {agree} of {len(big)}."]
    if pooled_lag:
        sl, ly = summary(pooled_lag, "pred_lag"), summary(pooled_lag, "last_year")
        lines += [f"Lagged, {sl['n']} cells: rms error {sl['rmse']:.5f}; repeat last year {ly['rmse']:.5f}; no change {sl['rmse_zero']:.5f}; "
                  f"slope {sl['slope']:.2f}; sign agrees in {sl['sign_agree']} of {sl['n']}."]
    (OUT / "report_tables.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def report_halfyear():
    """Half-year starts, pairs a year apart (2z-cg)."""
    d = Path("llm_fitting/runs/2026-10-14_balance_law_halfyear")
    lines, pooled, pooled_lag = [], [], []
    for panel in PANELS:
        f = d / "cells" / f"law__{panel}.json"
        if not f.exists():
            continue
        res = json.loads(f.read_text())
        K = res["K"]
        fw_ = res["blocks"]["first_week"]
        lines += [f"\n## {panel}  (K = {K}, 26-week blocks from {fw_[0]}, pairs a year apart; variance from blocks {res['variance_blocks']})", "",
                  "| pair (start of first block → start of second) | predicted, K/64 … K/2 (per 1,000 weeks) | observed, persisting set | rms error, against no change | lagged: predicted | lagged rms, repeat last |",
                  "|---|---|---|---|---|---|"]
        for p in res["pairs"]:
            below = [r for r in p["rows"] if r["k"] < K]
            sm = summary(below)
            pooled += below
            lag = ""
            lagr = ""
            if "pred_lag" in below[0]:
                sl, ly = summary(below, "pred_lag"), summary(below, "last_year")
                lag = ", ".join(f"{1000 * r['pred_lag']:+.1f}" for r in below)
                lagr = f"{1000 * sl['rmse']:.2f}, {1000 * ly['rmse']:.2f}"
                pooled_lag += below
            lines.append(f"| {fw_[p['start']]} → {fw_[p['end']]} | " + ", ".join(f"{1000 * r['pred']:+.1f}" for r in below) + " | " +
                         ", ".join(f"{1000 * r['obs_P']:+.1f}" for r in below) + f" | {1000 * sm['rmse']:.2f}, {1000 * sm['rmse_zero']:.2f} | {lag} | {lagr} |")
    if pooled:
        sm = summary(pooled)
        big = [r for r in pooled if abs(r["pred"]) > 0.0010]
        agree = sum(np.sign(r["pred"]) == np.sign(r["obs_P"]) for r in big)
        lines += ["", f"Pooled, {sm['n']} cells: slope {sm['slope']:.2f}, correlation {sm['corr']:.2f}, rms error {1000 * sm['rmse']:.2f} against "
                      f"{1000 * sm['rmse_zero']:.2f} for no change (per 1,000 weeks); sign right in {agree} of {len(big)} cells above 1.0."]
    if pooled_lag:
        sl, ly = summary(pooled_lag, "pred_lag"), summary(pooled_lag, "last_year")
        lines += [f"Lagged, {sl['n']} cells: rms error {1000 * sl['rmse']:.2f}; repeat last {1000 * ly['rmse']:.2f}; no change {1000 * sl['rmse_zero']:.2f}; "
                  f"slope {sl['slope']:.2f}; sign right in {sl['sign_agree']} of {sl['n']}."]
    (d / "report_tables.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) == 2 and args[1] in PANELS:
        run(args[1])
    elif args[:1] == ["run-halfyear"] and len(args) == 2 and args[1] in PANELS:
        run(args[1], halfyear=True)
    elif args[:1] == ["report-halfyear"]:
        report_halfyear()
    elif args[:1] == ["report"]:
        report()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
