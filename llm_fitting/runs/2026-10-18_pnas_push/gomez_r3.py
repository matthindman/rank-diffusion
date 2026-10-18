"""R3-E of PLAN.md: Gomez's decomposition with the general displacement term and permanent-rank cohorts.

General displacement (Gomez Prop. 1, no Pareto approximation), for the top k within the top-K total:
    between_implied = 0.5 * nu^2 * (a_k / abar_top_k) / S_k
a_k the activity total of the rank-k name, abar_top_k the mean of the top k, S_k the local slope of log total on log
rank (ranks k/sqrt2..k*sqrt2), nu^2 the lasting variance per year at the boundary.  On a Pareto tail a_k / abar =
(zeta - 1)/zeta and S = 1/zeta, which gives 0.5 (zeta - 1) nu^2; here it is valid for any slope.
Cohorts: (a) the start year's top k by total (as A8); (b) the top k by permanent rank over the start block
(absence-penalised mean weekly rank), which removes selection on the start year's own totals.

    python llm_fitting/runs/2026-10-18_pnas_push/gomez_r3.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
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
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    del df
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    Wk = np.nan_to_num(np.expm1(X), nan=0.0)
    tots = [Wk[start + b * block:start + (b + 1) * block].sum(0) for b in range(nb)]
    rows = []
    for k in (K // 64, K // 16, K // 4):
        for a in range(nb - 1):
            ta, tb = tots[a], tots[a + 1]
            oa, ob = np.argsort(-ta, kind="stable"), np.argsort(-tb, kind="stable")
            Da, Db = ta[oa[:K]].sum(), tb[ob[:K]].sum()
            Wb = tb[ob[:k]].sum()
            g = Db / Da
            pr, _ = ba.perm_rank(Rk, start + a * block, start + (a + 1) * block, floors)
            cohorts = dict(current=oa[:k], permanent=np.argsort(pr, kind="stable")[:k])
            dec = {}
            for name, Pa in cohorts.items():
                Wa = ta[Pa].sum()
                within = (tb[Pa].sum() / Wa - g) / g
                between = ((Wb - tb[Pa].sum()) / Wa) / g
                dec[name] = dict(share_a=float(Wa / Da), within=float(within), between=float(between))
            lo, hi = int(np.floor(k / np.sqrt(2))), int(np.ceil(k * np.sqrt(2)))
            S = float(-np.polyfit(np.log(np.arange(lo, hi + 1)), np.log(ta[oa[lo - 1:hi]]), 1)[0])
            a_k, abar = ta[oa[k - 1]], ta[oa[:k]].mean()
            kk = min(sig2, key=lambda x: abs(np.log(x / k)))
            nu2 = sig2[kk] * 52
            implied = 0.5 * nu2 * (a_k / abar) / S
            share_change = (Wb / Db) / (ta[oa[:k]].sum() / Da) - 1
            row = dict(k=int(k), pair=[a, a + 1], share_change=float(share_change), S_arith=S, ratio_ak_abar=float(a_k / abar),
                       nu2_year=nu2, implied_between_general=float(implied), zeta=1 / S, **{f"{c}_{t}": dec[c][t] for c in dec for t in dec[c]})
            rows.append(row)
            print(f"{pn} k {k:>5} {a}->{a + 1}: share change {share_change:+.3f}; current cohort within {dec['current']['within']:+.3f} "
                  f"between {dec['current']['between']:+.3f}; permanent cohort within {dec['permanent']['within']:+.3f} between "
                  f"{dec['permanent']['between']:+.3f}; implied (general) {implied:+.3f} [S {S:.2f}, a_k/abar {a_k / abar:.2f}, nu2 {nu2:.3f}]", flush=True)
    out[pn] = rows
Path(__file__).with_name("gomez_r3.json").write_text(json.dumps(out, indent=1))
