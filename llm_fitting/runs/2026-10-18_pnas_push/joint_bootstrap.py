"""A9 of PLAN.md: a joint bootstrap of the pooled boundary ratio (round-1 and round-2 referees).

The registered intervals (mts_boundary_law.analyse) resample communities separately for the lasting variance and
for each start's crossings, and pool starts as if independent.  Here each replicate draws ONE Poisson(1) weight
per community, used for the variance cohort and for every start's cohort, and resamples the starts themselves
with replacement (the time dimension: starts share shocks).  Point estimates are those of the registered
analysis; only the interval changes.  Pooled ratio = geometric mean of measured/predicted over cut-offs K/16..K
(as runs/2026-10-18_pooled_ratio).

    python llm_fitting/runs/2026-10-18_pnas_push/joint_bootstrap.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_boundary_law as mb  # noqa: E402
import mts_step1 as m1  # noqa: E402

BOOT = 400


def variance_at(M, which, st, vb, block, idx_w):
    """Lasting variance per stratum (as bl.lasting_variance) on a weighted (repeated-index) sample."""
    a, b, c = vb
    out = []
    for j in range(len(st)):
        ix = idx_w[which[idx_w] == j]
        ix = ix[np.isfinite(M[a][ix]) & np.isfinite(M[b][ix]) & np.isfinite(M[c][ix])]
        if ix.size < 20:
            out.append(np.nan)
            continue
        d1, d2, d3 = M[b][ix] - M[a][ix], M[c][ix] - M[b][ix], M[c][ix] - M[a][ix]
        out.append((d3.var() - 0.5 * (d1.var() + d2.var())) / block)
    return out


def analyse_joint(panel, K, block, seed=0):
    df = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]])
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    del df
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
    st, ks = bl.strata(K), bl.cutoffs(K)
    width = np.array([hi - lo for lo, hi in st])
    vb = (1, 3, 5)
    pr0, pres0 = mb._perm(Rk, start, start + block, floors, "absence_penalized")
    which0 = mb._assign_at(pr0, pres0, st, m1.PRES)
    n_all = X.shape[1]
    allidx = np.arange(n_all)
    v_pt = variance_at(M, which0, st, vb, 2 * block, allidx)
    vn = [int(((which0 == j) & np.isfinite(M[1]) & np.isfinite(M[3]) & np.isfinite(M[5])).sum()) for j in range(len(st))]
    starts = []
    for a in range(1, nb - 2):
        if a + 4 > nb - 1:
            continue
        prc, presc = mb._perm(Rk, start + (a - 1) * block, start + a * block, floors, "absence_penalized")
        starts.append(dict(a=a, which=mb._assign_at(prc, presc, st, m1.PRES)))

    def crossings(s, idx_w):
        a = s["a"]
        ix = idx_w[np.isfinite(M[a][idx_w])]
        res = {}
        for D in (2, 4):
            j = ix[np.isfinite(M[a + D][ix])]
            res[D] = mb._terms(M[a][j], M[a + D][j], s["which"][j], width, ks, K)
        return [(res[4][i]["R"] - res[2][i]["R"]) / (2 * block) for i in range(len(ks))], \
               [0.5 * (res[2][i]["S_b"] + res[4][i]["S_b"]) for i in range(len(ks))]

    pt = [crossings(s, allidx) for s in starts]
    R_pt = np.mean([p[0] for p in pt], axis=0)
    S_late = np.nanmean([p[1] for p in pt], axis=0)
    s2_pt = np.array([bl._at_boundary(v_pt, vn, j) for j, _ in ks])
    c_pt = s2_pt / (2 * S_late)
    ratio_pt = R_pt / c_pt
    kk = np.array([k for _, k in ks])
    use = kk >= K // 16 - 1
    pooled_pt = float(np.exp(np.mean(np.log(ratio_pt[use]))))
    rng = np.random.default_rng(seed)
    draws, per_k = [], []
    for _ in range(BOOT):
        w = rng.poisson(1.0, n_all)
        idx_w = np.repeat(allidx, w)
        v = variance_at(M, which0, st, vb, 2 * block, idx_w)
        s2 = np.array([bl._at_boundary(v, vn, j) for j, _ in ks])
        pick = rng.integers(0, len(starts), len(starts))
        Rb = np.mean([crossings(starts[i], idx_w)[0] for i in pick], axis=0)
        rb = Rb / (s2 / (2 * S_late))
        per_k.append(rb)
        ok = rb[use] > 0
        draws.append(float(np.exp(np.mean(np.log(rb[use][ok])))) if ok.all() else np.nan)
    draws = np.array(draws)
    per_k = np.array(per_k)
    return dict(panel=panel, K=K, block=block, n_starts=len(starts), ks=kk.tolist(), ratio_point=ratio_pt.tolist(),
                pooled_point=pooled_pt, pooled_q=[float(np.nanquantile(draws, 0.05)), float(np.nanquantile(draws, 0.95))],
                per_k_q=[[float(np.nanquantile(per_k[:, i], 0.05)), float(np.nanquantile(per_k[:, i], 0.95))] for i in range(len(kk))],
                draws=[float(x) for x in draws])


out = {}
for panel, block in (("reddit_comments_ext", 26), ("reddit_submissions_long", 26), ("facebook_a", 13)):
    K = m1.PANELS[panel]["K"]
    r = analyse_joint(panel, K, block)
    out[panel] = r
    print(f"{panel}: {r['n_starts']} starts; pooled K/16..K {r['pooled_point']:.3f} (joint 5-95% {r['pooled_q'][0]:.3f}-{r['pooled_q'][1]:.3f}); "
          f"by cut-off {np.round(r['ratio_point'], 2).tolist()}", flush=True)
D = np.array([out[p]["draws"] for p in out])
ok = np.all(np.isfinite(D) & (D > 0), axis=0)
three = np.exp(np.log(D[:, ok]).mean(axis=0))
pt3 = float(np.exp(np.mean(np.log([out[p]["pooled_point"] for p in out]))))
out["three_ladders"] = dict(point=pt3, q=[float(np.quantile(three, 0.05)), float(np.quantile(three, 0.95))])
print(f"three ladders pooled {pt3:.3f} (joint 5-95% {out['three_ladders']['q'][0]:.3f}-{out['three_ladders']['q'][1]:.3f})", flush=True)
Path(__file__).with_name("joint_bootstrap.json").write_text(json.dumps(out, indent=1))
