"""R15 D4 (PLAN.md addendum R15): the durability factor from measured crossings, from the saved half-year rate-law cells
(no data).  F = 2 ln(K/k) (c/(s̄ − e))², the ratio of the linearized relaxation time to the movement time at balance
(SI, time scales), with the measured crossing gain c gross and with a composition term C removed (c × (1 − C)).

    python llm_fitting/runs/2026-10-18_pnas_push/r15_d4_durability.py
"""
import json
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
CELLS = HERE.parent / "2026-10-14_balance_law_halfyear" / "cells"


def main():
    out = {}
    for pn in ("reddit_comments_ext", "reddit_submissions_long"):
        d = json.loads((CELLS / f"law__{pn}.json").read_text())
        K = d["K"]
        rows = {}
        for p in d["pairs"]:
            if not p.get("primary", True):
                continue
            for r in p["rows"]:
                if r["k"] >= K:
                    continue
                net = r["sbar"] - r["entry"]
                if net <= 0:
                    continue
                rows.setdefault(int(r["k"]), []).append(dict(start=p["start"], ratio=r["c"] / net, ln=np.log(K / r["k"])))
        res = {}
        for k, cells in sorted(rows.items()):
            ratio = np.array([c["ratio"] for c in cells])
            ln = cells[0]["ln"]
            res[k] = dict(n=len(cells), ratio_median=float(np.median(ratio)), ratio_range=[float(ratio.min()), float(ratio.max())],
                          F_gross_median=float(np.median(2 * ln * ratio ** 2)),
                          F_C25_median=float(np.median(2 * ln * (0.75 * ratio) ** 2)),
                          F_C30_median=float(np.median(2 * ln * (0.70 * ratio) ** 2)),
                          F_C35_median=float(np.median(2 * ln * (0.65 * ratio) ** 2)),
                          F_by_start={int(c["start"]): float(2 * ln * c["ratio"] ** 2) for c in cells})
        out[pn] = dict(K=K, by_k=res)
        print(f"== {pn} (K {K})")
        for k, r in res.items():
            print(f"  k {k:5d} (K/{K // k}): c/(s̄−e) median {r['ratio_median']:.2f} [{r['ratio_range'][0]:.2f}, {r['ratio_range'][1]:.2f}] n {r['n']};"
                  f"  F gross {r['F_gross_median']:.2f}, C .25 {r['F_C25_median']:.2f}, C .30 {r['F_C30_median']:.2f}, C .35 {r['F_C35_median']:.2f}")
    (HERE / "r15_d4_durability.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
