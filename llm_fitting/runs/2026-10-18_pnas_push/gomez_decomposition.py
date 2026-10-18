"""A8 of PLAN.md: Gomez's decomposition of the arithmetic top shares (the shares of 2z-da: total activity of the
year's top k by total over the total of the top K, 52-week blocks, endpoints ever in the weekly top 4K).

For consecutive blocks a -> b, with P_a the top k at a, W the top-k total and D the top-K total:
    s_b / s_a - 1 = [ (sum_{P_a} tot_b / W_a - D_b / D_a)  +  (W_b - sum_{P_a} tot_b) / W_a ] / (D_b / D_a)
                     within: the initial members against the total     between (displacement): entrants vs leavers
(exact; absent weeks count zero, so a member that vanishes stays in the within term at its remaining total).
Model-implied between per year (Gomez Prop. 1): 0.5 (zeta - 1) nu^2, zeta = 1 / S_arith with S_arith the local slope
of log total on log rank at k (ranks k/sqrt2..k*sqrt2), nu^2 the lasting variance per year at the boundary
(the variance route of 2z-cc, law cells, x 52).

    python llm_fitting/runs/2026-10-18_pnas_push/gomez_decomposition.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

R = ROOT / "llm_fitting" / "runs"
out = {}
for pn in ("reddit_submissions_long", "reddit_comments_ext"):
    cfg = m1.PANELS[pn]
    K, block = cfg["K"], cfg["block"]
    law = json.loads((R / "2026-10-14_balance_law/cells" / f"law__{pn}.json").read_text())
    sig2 = {}
    for q in law["pairs"]:
        if q["primary"]:
            for r in q["rows"]:
                sig2.setdefault(r["k"], []).append(r["sigma2"])
    sig2 = {k: float(np.mean(v)) for k, v in sig2.items()}
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    X = m1.matrices(df, K)[0]
    del df
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    W = np.nan_to_num(np.expm1(X), nan=0.0)
    del X
    tots = [W[start + b * block:start + (b + 1) * block].sum(0) for b in range(nb)]
    res = []
    for k in (K // 64, K // 16, K // 4):
        for a in range(nb - 1):
            ta, tb = tots[a], tots[a + 1]
            oa, ob = np.argsort(-ta, kind="stable"), np.argsort(-tb, kind="stable")
            Pa = oa[:k]
            Wa, Wb = ta[Pa].sum(), tb[ob[:k]].sum()
            Da, Db = ta[oa[:K]].sum(), tb[ob[:K]].sum()
            g = Db / Da
            within = (tb[Pa].sum() / Wa - g) / g
            between = ((Wb - tb[Pa].sum()) / Wa) / g
            share_change = (Wb / Db) / (Wa / Da) - 1
            lo, hi = int(np.floor(k / np.sqrt(2))), int(np.ceil(k * np.sqrt(2)))
            r = np.arange(lo, hi + 1)
            S = float(-np.polyfit(np.log(r), np.log(ta[oa[lo - 1:hi]]), 1)[0])
            kk = min(sig2, key=lambda x: abs(np.log(x / k)))
            nu2 = sig2[kk] * 52
            implied = 0.5 * (1 / S - 1) * nu2
            row = dict(k=int(k), pair=[a, a + 1], share_a=float(Wa / Da), share_b=float(Wb / Db), share_change=float(share_change),
                       within=float(within), between=float(between), S_arith=S, zeta=1 / S, nu2_year=nu2, implied_between=float(implied))
            res.append(row)
            print(f"{pn} k {k:>5} blocks {a}->{a + 1}: share {Wa / Da:.3f} -> {Wb / Db:.3f} ({share_change:+.3f}) = within {within:+.3f} "
                  f"+ between {between:+.3f}; zeta {1 / S:.2f}, nu^2/yr {nu2:.3f}, implied between {implied:+.3f}", flush=True)
    out[pn] = res
Path(__file__).with_name("gomez_decomposition.json").write_text(json.dumps(out, indent=1))
