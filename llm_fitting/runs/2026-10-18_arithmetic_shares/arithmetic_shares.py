"""Arithmetic activity shares beside the geometric-mean index of 2z-cw (reviews of 2026-10-18; post hoc).

The index of 2z-cw is the share of exp(block mean of log activity) on the year's sorted ladder, a share of
geometric means. Here, on the same 52-week blocks:
  - the arithmetic share: total activity (karma) of the year's top k by total, over the total of the top K;
  - its spread over the year: the 10th-90th percentiles of the same share computed week by week;
  - the Jensen gap by rank band, log(arithmetic mean) - mean(log), for the members of each band;
  - an arithmetic translation of the sustained ladder (2z-cw): the sustained log ladder plus the last year's
    Jensen gap by rank, then shares.  A translation under the assumption that the within-year spread by rank
    stays as it was; not a measurement.
Endpoints ever in the weekly top 4K are kept (mts_step1.matrices), as in every B analysis: a top-K total
from outside that set would need 52 weeks below rank 4K.  No name is printed or written.

    python llm_fitting/runs/2026-10-18_arithmetic_shares/arithmetic_shares.py
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
CONC = json.loads((R / "2026-10-15_concentration" / "concentration.json").read_text())
out = {}
for pn in ("reddit_comments_ext", "reddit_submissions_long"):
    cfg = m1.PANELS[pn]
    K, block = cfg["K"], cfg["block"]
    ks = (K // 64, K // 16, K // 4)
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    X = m1.matrices(df, K)[0]                 # endpoints ever in the weekly top 4K (as every B analysis)
    del df
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    W = np.nan_to_num(np.expm1(X), nan=0.0)   # weekly activity; absent weeks count zero
    del X
    res = dict(K=K, years={})
    for b in range(nb):
        Wb = W[start + b * block:start + (b + 1) * block]
        tot = Wb.sum(0)
        order = np.argsort(-tot, kind="stable")
        topK = order[:K]
        arith = {int(k): float(tot[order[:k]].sum() / tot[topK].sum()) for k in ks}
        wk = {int(k): [] for k in ks}                      # weekly shares, weekly ranking
        for w in Wb:
            o = np.argsort(-w, kind="stable")
            den = w[o[:K]].sum()
            for k in ks:
                wk[k].append(w[o[:k]].sum() / den)
        spread = {int(k): [float(np.quantile(v, 0.1)), float(np.quantile(v, 0.9))] for k, v in wk.items()}
        # Jensen gap by rank band of the year's ladder (members present in >= 90% of weeks, as the index)
        pres = (Wb > 0).mean(0) >= m1.PRES
        oo = [i for i in order if pres[i]][:K]
        L = np.log1p(Wb[:, oo])
        jg = np.log1p(Wb[:, oo].mean(0)) - L.mean(0)
        bands = [(1, K // 64), (K // 64, K // 16), (K // 16, K // 4), (K // 4, K)]
        jgap = {f"{lo}-{hi}": float(np.mean(jg[lo - 1:hi])) for lo, hi in bands}
        res["years"][str(b)] = dict(arith=arith, weekly_10_90=spread, jensen_gap=jgap, jg_by_rank=jg.tolist())
    # arithmetic translation of the sustained ladders: S* ladder (2z-cw) + last year's Jensen gap by rank
    jg_last = np.array(res["years"][str(nb - 1)]["jg_by_rank"])
    jg_full = np.interp(np.arange(K), np.arange(jg_last.size), jg_last)
    import importlib.util
    spec = importlib.util.spec_from_file_location("conc", R / "2026-10-15_concentration" / "concentration.py")
    res["note_sustained"] = "translation: sustained log ladder + last year's Jensen gap by rank"
    print(pn, "K", K)
    for b, y in res["years"].items():
        idx = CONC[pn]["observed_by_year"]
        yk = list(idx.keys())[int(b)]
        print(f"  year from {yk}: " + "; ".join(f"top {k}: arithmetic {100 * y['arith'][k]:.1f}% (weekly {100 * y['weekly_10_90'][k][0]:.1f}-{100 * y['weekly_10_90'][k][1]:.1f}%), index {100 * idx[yk][str(k)]:.1f}%" for k in ks))
        print("     Jensen gap log(arith mean) - mean log by band: " + ", ".join(f"{bk} {v:.2f}" for bk, v in y["jensen_gap"].items()))
        y.pop("jg_by_rank")
    res["jg_last_by_rank"] = jg_full.tolist()
    out[pn] = res
Path(__file__).with_name("arithmetic_shares.json").write_text(json.dumps(out))
