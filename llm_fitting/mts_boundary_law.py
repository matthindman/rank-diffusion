"""Where the rate law's physics holds: crossings and slides by rank, lasting parts only
(design in runs/2026-10-14_boundary_law/DESIGN.md).

Exact, for the endpoints P with a level in blocks a and b (D weeks apart), T_a the top k
of P by level in block a:

    U_k(b) - U_k(a) = G_k + R_k
    G_k = mean change a -> b of T_a's members              (the current top k's movement)
    R_k = U_k(b) - mean level of T_a's members at b >= 0     (reordering gain)

So the same-year rate law of 2z-cd, obs_k = c_k - sbar_k, has the error
    obs_k - pred_k = (L_k - E_k) / D  +  (R_k / D - c_k)
with E_k = -(G_k - dU_K) the slide of the current top k against U_K (Eulerian), L_k the
slide of the cohort fixed by permanent rank on the block before a (Lagrangian, the law's
sbar_k times D) and c_k = sigma_k^2 / (2 S_k).  The first piece is the slide the law uses
against the slide of whoever is on top; the second is the boundary theory.

Both G_k and R_k carry one-time constants from passing swings and block averaging
(selection on block a's level).  A difference over two spacings D1 < D2 from the same start
removes them:
    R~_k = (R_k(D2) - R_k(D1)) / (D2 - D1)      lasting crossing gain per member per week
    E~_k, L~_k likewise for the two slides.
The boundary theory says R~_k = c_k at every rank.  Once selection constants are removed,
E~_k should equal L~_k; at one spacing E_k / D should exceed L_k / D.

    python llm_fitting/mts_boundary_law.py run <panel>
    python llm_fitting/mts_boundary_law.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-14_boundary_law")
PANELS = bl.PANELS
BLOCK = 26
SPACINGS = (2, 4, 6)                 # in blocks: 52, 104, 156 weeks
DIFFS = ((2, 4), (4, 6), (2, 6))
BOOT = 200


def _terms(La, Lb, strat, width, ks, K):
    """For one pair of block levels over the endpoints present in both (arrays aligned):
    G_k, R_k, E_k, L_k (log units over the interval) and the slope at a, per cut-off."""
    oa = np.argsort(-La, kind="stable")
    lad_a, lad_b = La[oa], np.sort(Lb)[::-1]
    d = Lb - La
    dUK = lad_b[:K].mean() - lad_a[:K].mean()
    cum_b = np.cumsum(Lb[oa])
    slide_j = np.full(width.size, np.nan)
    for j in range(width.size):
        m = strat == j
        if m.sum() >= 5:
            slide_j[j] = -(d[m].mean() - dUK)
    out = []
    for j_end, k in ks:
        G = float(cum_b[k - 1] / k - lad_a[:k].mean())
        R = float(lad_b[:k].mean() - cum_b[k - 1] / k)
        js = [j for j in range(j_end + 1) if np.isfinite(slide_j[j])]
        L = float((slide_j[js] * width[js]).sum() / width[js].sum()) if js else np.nan
        out.append(dict(k=int(k), G=G, R=R, E=float(-(G - dUK)), L=L, dUK=float(dUK),
                        S_a=bl._slope(lad_a, k), S_b=bl._slope(lad_b, k)))
    return out


def analyse(df, K, block=BLOCK, boot=BOOT, seed=0):
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
    rng = np.random.default_rng(seed)
    st, ks = bl.strata(K), bl.cutoffs(K)
    width = np.array([hi - lo for lo, hi in st])
    n_cert = []
    for b in range(nb):
        seg = outside[start + b * block:start + (b + 1) * block]
        bd = float(np.nanmax(seg)) if np.isfinite(seg).any() else -np.inf
        n_cert.append(int((M[b][np.isfinite(M[b])] > bd).sum()))
    out = dict(K=int(K), T=int(T), block=int(block), n_blocks=int(nb), certified_top=n_cert,
               ks=[k for _, k in ks], starts=[])
    vb = (1, 3, 5)
    if nb < 8 or min(n_cert) < int(np.ceil(bl.WIDTH * K)):
        out["not_adjudicated"] = "needs eight 26-week blocks and ranks to 1.42 K certified exact"
        return out
    pr0, pres0 = ba.perm_rank(Rk, start, start + block, floors)
    var_rows, var_bs = bl.lasting_variance(M, bl._assign(pr0, pres0, st), st, vb, 2 * block, rng, boot)
    vn = [v["n"] for v in var_rows]
    s2 = {k: bl._at_boundary([v["sigma2"] for v in var_rows], vn, j) for j, k in ks}
    s2_bs = {k: np.array([bl._at_boundary(var_bs[:, t], vn, j) for t in range(boot)]) for j, k in ks}
    out["variance"] = [dict(lo=float(lo), hi=float(hi), **v) for (lo, hi), v in zip(st, var_rows)]

    for a in range(1, nb - SPACINGS[0]):
        prc, presc = ba.perm_rank(Rk, start + (a - 1) * block, start + a * block, floors)
        which = bl._assign(prc, presc, st)
        Da = [D for D in SPACINGS if a + D <= nb - 1]
        if len(Da) < 2:
            continue
        pa = np.isfinite(M[a])
        idx_a = np.flatnonzero(pa)
        point, draws = {}, {D: [] for D in Da}
        for D in Da:
            m = np.isfinite(M[a + D][idx_a])
            ix = idx_a[m]
            point[D] = _terms(M[a][ix], M[a + D][ix], which[ix], width, ks, K)
        for _ in range(boot):
            ixb = idx_a[rng.integers(0, idx_a.size, idx_a.size)]
            for D in Da:
                ix = ixb[np.isfinite(M[a + D][ixb])]
                draws[D].append(_terms(M[a][ix], M[a + D][ix], which[ix], width, ks, K))
        rec = dict(start=int(a), spacings=[int(D) for D in Da], single={}, diff={})
        for D in Da:
            w = D * block
            rec["single"][str(D)] = [dict(k=r["k"], R=r["R"] / w, E=r["E"] / w, L=r["L"] / w, S_a=r["S_a"], S_b=r["S_b"],
                                          obs=(r["R"] - r["E"]) / w, identity=r["G"] + r["R"]) for r in point[D]]
        for D1, D2 in DIFFS:
            if D1 not in Da or D2 not in Da:
                continue
            w = (D2 - D1) * block
            rows = []
            for i, (j_end, k) in enumerate(ks):
                f = lambda key, P1, P2: (P2[i][key] - P1[i][key]) / w  # noqa: E731
                bsR = np.array([f("R", b1, b2) for b1, b2 in zip(draws[D1], draws[D2])])
                bsE = np.array([f("E", b1, b2) for b1, b2 in zip(draws[D1], draws[D2])])
                bsL = np.array([f("L", b1, b2) for b1, b2 in zip(draws[D1], draws[D2])])
                rows.append(dict(k=int(k), R=f("R", point[D1], point[D2]), E=f("E", point[D1], point[D2]),
                                 L=f("L", point[D1], point[D2]), R_bs=bsR, E_bs=bsE, L_bs=bsL))
            rec["diff"][f"{D1}-{D2}"] = rows
        out["starts"].append(rec)

    # pooled over starts: spacing differences 2-4 (all starts), single spacing 2.  The slope for the
    # crossings of one spacing is the mean of its two blocks (as 2z-cd); for the difference 2-4 it is the
    # mean of the blocks at spacings 2 and 4, the interval whose crossings the difference measures.
    st2 = [s for s in out["starts"] if "2-4" in s["diff"]]
    S_one = {k: float(np.nanmean([0.5 * (s["single"]["2"][i]["S_a"] + s["single"]["2"][i]["S_b"]) for s in st2])) for i, (_, k) in enumerate(ks)}
    S_late = {k: float(np.nanmean([0.5 * (s["single"]["2"][i]["S_b"] + s["single"]["4"][i]["S_b"]) for s in st2])) for i, (_, k) in enumerate(ks)}
    pooled = []
    q = lambda v: [float(np.nanquantile(v, 0.05)), float(np.nanquantile(v, 0.95))]  # noqa: E731
    for i, (_, k) in enumerate(ks):
        dd = [s["diff"]["2-4"][i] for s in out["starts"] if "2-4" in s["diff"]]
        R = float(np.mean([r["R"] for r in dd]))
        E = float(np.mean([r["E"] for r in dd]))
        L = float(np.mean([r["L"] for r in dd]))
        Rb = np.mean([r["R_bs"] for r in dd], axis=0)
        Eb = np.mean([r["E_bs"] for r in dd], axis=0)
        Lb = np.mean([r["L_bs"] for r in dd], axis=0)
        c = s2[k] / (2 * S_late[k])
        cb = s2_bs[k] / (2 * S_late[k])
        c1 = s2[k] / (2 * S_one[k])
        one = [s["single"]["2"][i] for s in out["starts"] if "2-4" in s["diff"]]
        R1 = float(np.mean([r["R"] for r in one]))
        E1 = float(np.mean([r["E"] for r in one]))
        L1 = float(np.mean([r["L"] for r in one]))
        obs1 = float(np.mean([r["obs"] for r in one]))
        pooled.append(dict(k=int(k), S=S_late[k], S_one=S_one[k], sigma2=float(s2[k]), c=float(c), c_q=q(cb), c_one=float(c1),
                           R_lasting=R, R_lasting_q=q(Rb), ratio=R / c if c else np.nan, ratio_q=q(Rb / cb),
                           E_lasting=E, E_lasting_q=q(Eb), L_lasting=L, L_lasting_q=q(Lb), EL_diff_q=q(Eb - Lb),
                           R_one=R1, E_one=E1, L_one=L1, obs_one=obs1, pred_one=float(c1 - L1),
                           err_one=float(obs1 - (c1 - L1)), err_slide=float(L1 - E1), err_boundary=float(R1 - c1),
                           obs_lasting=float(R - E), pred_lasting=float(c - L),
                           n_starts=len(dd)))
    out["pooled"] = pooled
    for s in out["starts"]:
        for key in s["diff"]:
            for r in s["diff"][key]:
                for b in ("R_bs", "E_bs", "L_bs"):
                    r[b + "_q"] = q(r.pop(b))
    return out


def run(panel):
    import minimal_rankdiff as mrd
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    res = analyse(df, cfg["K"])
    res.update(panel=panel, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"boundary__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] T={res['T']} blocks={res['n_blocks']} certified_top={res['certified_top']} written", flush=True)


def report():
    lines = []
    for panel in PANELS:
        f = OUT / "cells" / f"boundary__{panel}.json"
        if not f.exists():
            continue
        res = json.loads(f.read_text())
        K = res["K"]
        lines += [f"\n## {panel} (K = {K}; 26-week blocks; lasting = difference of spacings 52 and 104 weeks, "
                  f"mean of {res['pooled'][0]['n_starts']} starts; per endpoint per 1,000 weeks)", "",
                  "| k | slope S | c = σ²/2S (5–95%) | lasting crossing gain R~ (5–95%) | R~ / c (5–95%) | lasting slide, current top k | "
                  "lasting slide, cohort | one spacing: current top k | one spacing: cohort |",
                  "|---|---|---|---|---|---|---|---|---|"]
        for r in res["pooled"]:
            t = lambda v: 1000 * v  # noqa: E731
            lines.append(f"| {r['k']} | {r['S']:.2f} | {t(r['c']):.2f} ({t(r['c_q'][0]):.2f}–{t(r['c_q'][1]):.2f}) | "
                         f"{t(r['R_lasting']):.2f} ({t(r['R_lasting_q'][0]):.2f}–{t(r['R_lasting_q'][1]):.2f}) | "
                         f"{r['ratio']:.2f} ({r['ratio_q'][0]:.2f}–{r['ratio_q'][1]:.2f}) | {t(r['E_lasting']):.2f} | "
                         f"{t(r['L_lasting']):.2f} | {t(r['E_one']):.2f} | {t(r['L_one']):.2f} |")
        lines += ["", "Error of the same-year law at one spacing (52 weeks), observed − predicted = slide piece + boundary piece "
                      "(per 1,000 weeks):", "", "| k | observed | predicted c − cohort slide | error | slide piece (cohort − current top) | boundary piece (R − c) |",
                  "|---|---|---|---|---|---|"]
        for r in res["pooled"]:
            if r["k"] < K:
                lines.append(f"| {r['k']} | {1000 * r['obs_one']:+.2f} | {1000 * r['pred_one']:+.2f} | {1000 * r['err_one']:+.2f} | "
                             f"{1000 * r['err_slide']:+.2f} | {1000 * r['err_boundary']:+.2f} |")
        below = [r for r in res["pooled"] if r["k"] < K]
        lines += ["", "The law in its lasting form, the change of shape in the second year from a start (difference of spacings) "
                      "against c − lasting cohort slide (per 1,000 weeks):", "", "| k | observed | predicted |", "|---|---|---|"]
        lines += [f"| {r['k']} | {1000 * r['obs_lasting']:+.2f} | {1000 * r['pred_lasting']:+.2f} |" for r in below]
        for key, obs, pred in (("one spacing", "obs_one", "pred_one"), ("lasting form", "obs_lasting", "pred_lasting")):
            o, pr = np.array([r[obs] for r in below]), np.array([r[pred] for r in below])
            lines.append(f"- {key}, cut-offs below K: slope of observed on predicted {(o * pr).sum() / (pr * pr).sum():.2f}, "
                         f"rms error {1000 * np.sqrt(((o - pr) ** 2).mean()):.2f} against {1000 * np.sqrt((o ** 2).mean()):.2f} for no change")
        lines += ["", "By start (difference 52 → 104 weeks, per 1,000 weeks): R~ at each cut-off", ""]
        for s in res["starts"]:
            if "2-4" in s["diff"]:
                lines.append(f"- start block {s['start']}: " + ", ".join(f"{r['k']}: {1000 * r['R']:.2f}" for r in s["diff"]["2-4"]))
        for s in res["starts"]:
            for key in ("4-6", "2-6"):
                if key in s["diff"]:
                    lines.append(f"- start block {s['start']}, spacings {key}: " + ", ".join(f"{r['k']}: {1000 * r['R']:.2f}" for r in s["diff"][key]))
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
