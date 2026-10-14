"""POST HOC (after the scored run of 2z-cm): the boundary theory start by start.

For each start a (cohort fixed on block a-1), the lasting variance by stratum is taken from
the same cohort over blocks a, a+2, a+4 (the variance route, 52-week steps), the slope from
the blocks a+2 and a+4 whose crossings the difference of spacings measures, and c_k(a) is
compared with R~_k(a).  This asks whether crossings follow sigma^2/(2S) as both change over
2020-2022, including across Reddit's June 2021 Home-feed change.

    python llm_fitting/runs/2026-10-14_boundary_law/by_start_post_hoc.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

BOOT = 200


def main(panel):
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    K = cfg["K"]
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    nb = T // bd.BLOCK
    start = T - nb * bd.BLOCK
    M = ba.block_means(X, bd.BLOCK, start=start, min_frac=m1.PRES)
    dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).tolist() if "ts" in df.columns else None
    st, ks = bl.strata(K), bl.cutoffs(K)
    width = np.array([hi - lo for lo, hi in st])
    rng = np.random.default_rng(1)
    out = dict(panel=panel, K=K, first_week=None if dates is None else [dates[start + b * bd.BLOCK] for b in range(nb)], starts=[])
    for a in (1, 2, 3):
        prc, presc = ba.perm_rank(Rk, start + (a - 1) * bd.BLOCK, start + a * bd.BLOCK, floors)
        which = bl._assign(prc, presc, st)
        var_rows, var_bs = bl.lasting_variance(M, which, st, (a, a + 2, a + 4), 2 * bd.BLOCK, rng, BOOT)
        vn = [v["n"] for v in var_rows]
        idx_a = np.flatnonzero(np.isfinite(M[a]))
        terms, draws = {}, {2: [], 4: []}
        for D in (2, 4):
            ix = idx_a[np.isfinite(M[a + D][idx_a])]
            terms[D] = bd._terms(M[a][ix], M[a + D][ix], which[ix], width, ks, K)
        for _ in range(BOOT):
            ixb = idx_a[rng.integers(0, idx_a.size, idx_a.size)]
            for D in (2, 4):
                ix = ixb[np.isfinite(M[a + D][ixb])]
                draws[D].append(bd._terms(M[a][ix], M[a + D][ix], which[ix], width, ks, K))
        rows = []
        for i, (j, k) in enumerate(ks):
            s2 = bl._at_boundary([v["sigma2"] for v in var_rows], vn, j)
            s2b = np.array([bl._at_boundary(var_bs[:, t], vn, j) for t in range(BOOT)])
            S = 0.5 * (terms[2][i]["S_b"] + terms[4][i]["S_b"])
            R = (terms[4][i]["R"] - terms[2][i]["R"]) / (2 * bd.BLOCK)
            Rb = np.array([(d4[i]["R"] - d2[i]["R"]) / (2 * bd.BLOCK) for d2, d4 in zip(draws[2], draws[4])])
            c, cb = s2 / (2 * S), s2b / (2 * S)
            q = lambda v: [float(np.nanquantile(v, 0.05)), float(np.nanquantile(v, 0.95))]  # noqa: E731
            rows.append(dict(k=int(k), S=float(S), sigma2=float(s2), c=float(c), c_q=q(cb), R=float(R), R_q=q(Rb),
                             ratio=float(R / c), ratio_q=q(Rb / cb)))
        out["starts"].append(dict(start=a, crossings_between=[a + 2, a + 4], variance_blocks=[a, a + 2, a + 4],
                                  variance=[dict(lo=float(lo), hi=float(hi), **v) for (lo, hi), v in zip(st, var_rows)], rows=rows))
        print(f"[{panel}] start {a} done", flush=True)
    path = Path(__file__).with_name(f"by_start__{panel}.json")
    path.write_text(json.dumps(out, indent=1, default=float))
    fw = out["first_week"]
    for s in out["starts"]:
        b0, b1 = s["crossings_between"]
        print(f"\n{panel}, start {s['start']}: crossings between blocks starting {fw[b0] if fw else b0} and {fw[b1] if fw else b1}")
        for r in s["rows"]:
            print(f"  k={r['k']:>5} S={r['S']:.2f} c={1000 * r['c']:.2f} ({1000 * r['c_q'][0]:.2f}-{1000 * r['c_q'][1]:.2f}) "
                  f"R~={1000 * r['R']:.2f} ({1000 * r['R_q'][0]:.2f}-{1000 * r['R_q'][1]:.2f}) ratio={r['ratio']:.2f} "
                  f"({r['ratio_q'][0]:.2f}-{r['ratio_q'][1]:.2f})")
    allr = [r for s in out["starts"] for r in s["rows"]]
    rr = np.array([r["ratio"] for r in allr])
    inside = int(((rr >= 0.75) & (rr <= 1.33)).sum())
    c_, R_ = np.array([r["c"] for r in allr]), np.array([r["R"] for r in allr])
    print(f"\n{panel}: {inside} of {len(allr)} start-by-cut-off cells with R~/c in [0.75, 1.33]; "
          f"correlation of R~ with c across cells {np.corrcoef(c_, R_)[0, 1]:.2f}; slope of R~ on c through the origin {(c_ * R_).sum() / (c_ * c_).sum():.2f}")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in bd.PANELS:
        sys.exit(__doc__)
    main(sys.argv[1])
