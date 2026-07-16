#!/usr/bin/env python3
"""A6.1' registered wrapper runners: execute the FROZEN A1.4 (P6) and
A2.2/A4.6 (P10 iv) movement-gate parameter sets through oos_movement and
adjudicate with the pure verdicts. The frozen command texts in the prereg
are the parameter registration; this wrapper is the execution vehicle
(same parameters verbatim, plus the returned-summary -> verdict wiring).

  python3 llm_fitting/gate_verdicts.py p6 --top-k <K from P3>
  python3 llm_fitting/gate_verdicts.py p10iv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

MEMBER_SHA = ("130726eb194597fcbba67ca3eced29a5f8e5e20d34dcc8e3ae186e455"
              "ac75aac")


def run_p6(top_k):
    import rankdiff_kalman as rk
    from ig_daily_2023 import p6_verdict
    res = rk.oos_movement(
        "reddit_submissions_long", top_k=top_k, temper=True, min_knot_n=8,
        md_lags=6, t_tails=True, conditional="state", dist_scores=True,
        reps=20, boot=2000)
    ok = p6_verdict(res["model_rel"], res["base_rel"], res["coverage"])
    print(f"\nP6 VERDICT: model {res['model_rel']:.3f} vs base "
          f"{res['base_rel']:.3f} + 0.05, coverage {res['coverage']:.2f} "
          f"-> {'PASS' if ok else 'FAIL'}")
    return dict(**res, p6_pass=ok)


def run_p10iv():
    import rankdiff_kalman as rk
    from ig_daily_2023 import p10iv_verdict
    res = rk.oos_movement(
        "instagram_hm_ts", top_k=10_000, temper=True, min_knot_n=8,
        md_lags=6, t_tails=True, conditional="state", dist_scores=True,
        spec_b=True, reps=20, boot=2000,
        member_ids_file="llm_fitting/ig_trainsafe_members.parquet",
        expect_member_sha=MEMBER_SHA)
    ok = p10iv_verdict(res["model_rel"], res["base_rel"], res["coverage"])
    print(f"\nP10(iv) VERDICT: model {res['model_rel']:.3f} (|Δ0.320| = "
          f"{abs(res['model_rel'] - 0.320):.3f}), base {res['base_rel']:.3f}"
          f", coverage {res['coverage']:.2f} -> {'PASS' if ok else 'FAIL'}")
    return dict(**res, p10iv_pass=ok)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=("p6", "p10iv"))
    ap.add_argument("--top-k", type=int, default=None)
    a = ap.parse_args()
    if a.what == "p6":
        assert a.top_k, "--top-k required (from P3)"
        run_p6(a.top_k)
    else:
        run_p10iv()
