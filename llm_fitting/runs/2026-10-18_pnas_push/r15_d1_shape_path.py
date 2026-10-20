"""R15 D1 (PLAN.md addendum R15; descriptive): the observed shape path against the rate law and the stable-parameter model.

  - exact U_k − U_K by year (52-week block means aligned to the panel's end, presence 90%) at K/64, K/32, K/16, K/8,
    from the lean universe cache ($R12_CACHE; r12_cache.py, checked exact);
  - the saved half-year rate-law cells: observed change of the shape against c − s̄ + e, per year, by start;
  - the stable-parameter model from the 2019 ladder (R11 noentry runs): the shape by year, years 0–5.

    R12_CACHE=<dir> python llm_fitting/runs/2026-10-18_pnas_push/r15_d1_shape_path.py
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402

CELLS = HERE.parent / "2026-10-14_balance_law_halfyear" / "cells"


def main():
    out = {}
    for pn in ("reddit_comments_ext", "reddit_submissions_long"):
        K = m1.PANELS[pn]["K"]
        df = pd.read_parquet(Path(os.environ["R12_CACHE"]) / f"univ__{pn}.parquet")
        X, Rk, floors, _, _ = m1.matrices(df, K)
        del df
        T = X.shape[0]
        nb = T // 52
        start = T - nb * 52
        M = ba.block_means(X, 52, start=start, min_frac=m1.PRES)
        ks = [K // 64, K // 32, K // 16, K // 8]
        shape = []
        for b in range(nb):
            lad = np.sort(M[b][np.isfinite(M[b])])[::-1]
            shape.append([float(lad[:k].mean() - lad[:K].mean()) for k in ks])
        shape = np.array(shape)
        # half-year rate-law cells: per-week values -> per year
        cells = json.loads((CELLS / f"law__{pn}.json").read_text())
        law = []
        for p in cells["pairs"]:
            if not p.get("primary", True):
                continue
            for r in p["rows"]:
                if r["k"] in ks:
                    law.append(dict(start=p["start"], k=int(r["k"]), obs=52 * r["obs_all"], pred=52 * (r["c"] - r["sbar"] + r["entry"]),
                                    c=52 * r["c"], sbar=52 * r["sbar"], e=52 * r["entry"]))
        cf = json.loads((HERE / f"churn_counterfactual__{pn}__long30__noentry.json").read_text())
        model = {}
        for arm in ("churn_slide", "slide_only", "churn_only"):
            sb = cf[arm]["shape_by_year"]                # years 0..30 x ks
            model[arm] = {str(k): [float(sb[y][j]) for y in range(6)] for j, k in enumerate(cf["ks"][:4])}
        out[pn] = dict(K=K, ks=ks, years=[f"block {b}" for b in range(nb)], shape=shape.tolist(), law=law, model=model,
                       model_start=cf["start"])
        print(f"== {pn} (K {K}); blocks of 52 weeks from week {start}")
        for j, k in enumerate(ks):
            print(f"  k {k:5d}: shape by year {np.round(shape[:, j], 2).tolist()}; year-on-year {np.round(np.diff(shape[:, j]), 2).tolist()}; "
                  f"total {shape[-1, j] - shape[0, j]:+.2f}")
        for k, v in model["churn_slide"].items():
            print(f"  model from the 2019 ladder (volatility + slide), k {k}: years 0-5 {np.round(v, 2).tolist()}")
        for k in ks:
            rows = sorted([r for r in law if r["k"] == k], key=lambda r: r["start"])
            print(f"  rate law k {k}: " + "; ".join(f"start {r['start']}: obs {r['obs']:+.3f}/yr, c−s̄+e {r['pred']:+.3f} (s̄ {r['sbar']:.3f})" for r in rows))
    (HERE / "r15_d1_shape_path.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
