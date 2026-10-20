"""R15 D5 (PLAN.md addendum R15-D5): the composition term C of the crossing statistic in simulated worlds.

The data's crossing statistic R_ac − R_ab splits exactly into R_bc + C (mts_boundary_law.remainder; the data's
values in runs/2026-10-18_remainder/output.txt).  Here the same split, with the same blocks (26 weeks), starts
(a = 1 … nb − 5, b = a + 2, c = a + 4), cohorts and cut-offs as remainder_run.py, is computed on worlds simulated
from the R12 calibration: the matched homogeneous control (drift re-solved numerically, s 0) and fixed growth
differences (s 0.5, m 1.0), exits on, 3 seeds each, both Reddit ladders.  Reads the lean cache ($R12_CACHE).

    R12_CACHE=<dir> python llm_fitting/runs/2026-10-18_pnas_push/r15_d5_composition.py
"""
import gc
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
import r12_hetgrowth as h  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
from r15_d2_planted import sim_frame_lasting, world  # noqa: E402

BLOCK, SEEDS, S_PLANT = 26, (0, 1, 2), 0.5
DATA = HERE.parent / "2026-10-18_remainder"


def split(fr, K):
    """remainder_run.py's loop on one panel frame: per cut-off, R_bc and C per week, averaged over starts."""
    X, Rk, floors, _, _ = h.m1.matrices(fr, K)
    T = X.shape[0]
    nb = T // BLOCK
    start = T - nb * BLOCK
    M = ba.block_means(X, BLOCK, start=start, min_frac=h.m1.PRES)
    st, ks = bl.strata(K), bl.cutoffs(K)
    rows = {}
    for a in range(1, nb - 4):
        pr, pres = ba.perm_rank(Rk, start + (a - 1) * BLOCK, start + a * BLOCK, floors)
        which = bl._assign(pr, pres, st)
        for r in bd.remainder(M, which, st, ks, K, a, a + 2, a + 4):
            rows.setdefault(r["k"], []).append(r)
    w = 2 * BLOCK
    return {k: dict(R_bc=float(np.mean([r["R_bc"] / w for r in rs])), C=float(np.mean([r["C"] / w for r in rs])),
                    C_drift=float(np.mean([r["C_drift"] / w for r in rs])), starts=len(rs)) for k, rs in rows.items()}


def share(d):
    return d["C"] / (d["R_bc"] + d["C"])


def main():
    out = {}
    for pn in ("reddit_submissions_long", "reddit_comments_ext"):
        df, K = h.load(pn)
        T = int(df["period"].max()) + 1
        p, base, ladder = h.fit_model(df, K, pn)
        L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
        del df
        gc.collect()
        data = {r["k"]: r for r in json.loads((DATA / f"remainder__{pn}.json").read_text())}
        res = {"data": {str(k): dict(R_bc=v["R_bc"], C=v["C"], share=share(v)) for k, v in data.items()}}
        for tag, s in (("matched control", 0.0), ("fixed differences", S_PLANT)):
            q = world(p, ladder, s, True, True)
            per = []
            for seed in SEEDS:
                fr, _, _ = sim_frame_lasting(q, L0, T, seed, K, "movinghome_vp")
                per.append(split(fr, K))
                del fr
                gc.collect()
            ks = sorted(per[0])
            res[tag] = {str(k): dict(R_bc=float(np.mean([d[k]["R_bc"] for d in per])), C=float(np.mean([d[k]["C"] for d in per])),
                                     C_drift=float(np.mean([d[k]["C_drift"] for d in per])),
                                     share=float(np.mean([share(d[k]) for d in per])),
                                     share_by_seed=[float(share(d[k])) for d in per]) for k in ks}
            head = [k for k in ks if k <= K // 4]
            print(f"== {pn} {tag}: share C/(R_bc + C) by k " + "; ".join(f"{k}: {res[tag][str(k)]['share']:+.3f} "
                  f"(seeds {np.round(res[tag][str(k)]['share_by_seed'], 3).tolist()}; R_bc {1000 * res[tag][str(k)]['R_bc']:.2f}, "
                  f"C {1000 * res[tag][str(k)]['C']:+.2f}, C_drift {1000 * res[tag][str(k)]['C_drift']:+.2f} per 1,000 wk)" for k in ks), flush=True)
            res[tag]["pooled_K64_K4"] = float(np.mean([res[tag][str(k)]["share"] for k in head]))
            print(f"   pooled share K/64 … K/4: {res[tag]['pooled_K64_K4']:+.3f}; data {np.mean([share(data[k]) for k in head]):+.3f}", flush=True)
        res["data"]["pooled_K64_K4"] = float(np.mean([share(data[k]) for k in sorted(data) if k <= K // 4]))
        out[pn] = res
    (HERE / "r15_d5_composition.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
