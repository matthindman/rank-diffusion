"""POST HOC (after 2z-cy): does the excess retention come from calm members?  For each origin, k = K/16 and
horizons 1-2 blocks, the members of the top k are split, within their stratum of current rank, by the size of
their own change over the block before the origin (|change of level against U_K from a-1 to a|, a proxy for a
persistent volatility type); observed minus forecast retention is reported for the calm and the volatile half.

    python llm_fitting/runs/2026-10-15_lockin/calm_vs_volatile.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_lockin as lk  # noqa: E402
import mts_step1 as m1  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

panel = sys.argv[1]
s = lk.SETTINGS[panel]
cfg = dict(mrd.PLATFORMS[m1.PANELS[panel]["platform"] if panel in m1.PANELS else panel])
if s["date_min"]:
    cfg["date_min"] = s["date_min"]
df = mrd.load_panel(cfg)
K = s["K"]
X, Rk, floors, outside, ids = m1.matrices(df, K)
T = X.shape[0]
nb = T // s["block"]
M = ba.block_means(X, s["block"], start=T - nb * s["block"], min_frac=m1.PRES if s["pres"] is None else s["pres"])
del X
st = bl.strata(K)
UK = np.array([np.sort(M[b][np.isfinite(M[b])])[::-1][:K].mean() for b in range(nb)])
Y = M - UK[:, None]
R = np.array([lk._ranks(M[b]) for b in range(nb)])
k = K // 16
res = {"calm": {"obs": [], "p": []}, "volatile": {"obs": [], "p": []}}
for a in range(2, nb - 1):
    t0 = a - 2
    mom = []
    for lo, hi in st:
        m = (R[t0] > lo) & (R[t0] <= hi)
        d1, d2 = Y[t0 + 1][m] - Y[t0][m], Y[t0 + 2][m] - Y[t0][m]
        o1, o2 = np.isfinite(d1), np.isfinite(d2)
        mom.append((o1.mean() if m.any() else np.nan, o2.mean() if m.any() else np.nan,
                    d1[o1].mean() if o1.sum() > 5 else np.nan, d2[o2].mean() if o2.sum() > 5 else np.nan,
                    d1[o1].var() if o1.sum() > 5 else np.nan, d2[o2].var() if o2.sum() > 5 else np.nan))
    bk = lambda t: float(np.sort(Y[t][np.isfinite(Y[t])])[::-1][k - 1])  # noqa: E731
    beta = (bk(a) - bk(t0)) / 2
    mem = np.flatnonzero(R[a] <= k)
    prev = np.abs(Y[a][mem] - Y[a - 1][mem])
    sj = np.array([next(j for j, (lo, hi) in enumerate(st) if lo < r <= hi) for r in R[a][mem]])
    calm = np.zeros(mem.size, bool)
    for j in np.unique(sj):
        sel = (sj == j) & np.isfinite(prev)
        if sel.sum() >= 4:
            calm[sel] = prev[sel] <= np.median(prev[sel])
    ok = np.isfinite(prev)
    for h in (1, 2):
        if a + h >= nb:
            continue
        q1, q2, mu1, mu2, v1, v2 = (np.array([mom[j][i] for j in sj]) for i in range(6))
        mu, v = mu1 + (mu2 - mu1) * (h - 1), np.maximum(v1 + (v2 - v1) * (h - 1), 1e-6)
        q = q1 * (q2 / q1) ** (h - 1)
        p = np.clip(q * lk._phi((Y[a][mem] - bk(a) + mu - beta * h) / np.sqrt(v)), 0, 1)
        obs = (R[a + h][mem] <= k).astype(float)
        for name, m_ in (("calm", calm & ok), ("volatile", ~calm & ok)):
            res[name]["obs"] += obs[m_].tolist()
            res[name]["p"] += p[m_].tolist()
out = {n: dict(n=len(v["obs"]), observed=float(np.mean(v["obs"])), forecast=float(np.mean(v["p"])),
               excess=float(np.mean(v["obs"]) - np.mean(v["p"]))) for n, v in res.items()}
print(panel, f"top {k}, h = 1-2 blocks:", json.dumps(out))
Path(__file__).with_name(f"calm_vs_volatile__{panel}.json").write_text(json.dumps(out, indent=1))
