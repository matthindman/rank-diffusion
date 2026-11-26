#!/usr/bin/env python3
"""Exact side-by-side Goal 1 + Goal 2 comparison of Round 9 candidates vs Round 6 champion."""
import json
from pathlib import Path

base = Path("llm_fitting/runs/2026-11-25_common_model")
r6 = json.loads((base / "round6_meta/final_bench/candidate_gold_benchmark_results.json").read_text())["panels"]
r9_rel = json.loads((base / "round9_unified/rel_unified/candidate_gold_benchmark_results.json").read_text())["panels"]
r9_rsh = json.loads((base / "round9_unified/gold_armB/candidate_gold_benchmark_results.json").read_text())["panels"]

r9_2t = json.loads((base / "round9_unified/two_timescale/candidate_gold_benchmark_results.json").read_text())["panels"]

for tag, cand in [("R9_two_timescale (+1 sc w_fast, rel)", r9_2t)]:
    print("\n" + "#" * 88)
    print(f"# {tag} vs ROUND 6 CHAMPION (S2)")
    print("#" * 88)
    xw = xl = lw = ll = kw = kl = sw = sl = cw = cl = 0
    max_dx = (-1.0, "")
    for pn in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
        b, c = r6[pn], cand[pn]
        emp = b["emp"]
        b_bnd, c_bnd = b["g1_s2"]["boundary_ratio"]["median"], c["g1_s2"]["boundary_ratio"]["median"]
        b_hs, c_hs = b["g1_s2"]["head_share"]["median"], c["g1_s2"]["head_share"]["median"]
        b_t10, c_t10 = b["g1_s2"]["turnover"]["10"]["median"], c["g1_s2"]["turnover"]["10"]["median"]
        print(
            f"\n  [{pn}] G1 Bound: emp={emp['boundary_ratio']:.4f} R6={b_bnd:.4f}->{c_bnd:.4f} | "
            f"HeadSh: emp={emp['head_share']:.4f} R6={b_hs:.4f}->{c_hs:.4f} | "
            f"Turn10: emp={emp['turnover']['10']:.4f} R6={b_t10:.4f}->{c_t10:.4f}"
        )
        for hs, ch in c["goal2"]["by_horizon"].items():
            bh = b["goal2"]["by_horizon"][hs]
            dx = ch["crps_X_s2"] - bh["crps_X_s2"]
            dlr = ch["crps_lr_s2"] - bh["crps_lr_s2"]
            kal_key = next(k for k in ch if "kal" in k and k.endswith("_s2"))
            dk = ch[kal_key] - bh[kal_key]
            ds = ch["share_mae_hd_s2"] - bh["share_mae_hd_s2"]
            dc = abs(ch["cov90_s2"] - 0.9) - abs(bh["cov90_s2"] - 0.9)
            if dx > max_dx[0]:
                max_dx = (dx, f"{pn} h={hs}")
            xw += int(dx < 0); xl += int(dx > 0)
            lw += int(dlr < 0); ll += int(dlr > 0)
            kw += int(dk < 0); kl += int(dk > 0)
            sw += int(ds < 0); sl += int(ds > 0)
            cw += int(dc < 0); cl += int(dc > 0)
            print(
                f"    h={int(hs):2d}w | dX={dx:+.4f} dLR={dlr:+.4f} dKal={dk:+.4f} "
                f"cov:{bh['cov90_s2']:.3f}->{ch['cov90_s2']:.3f} dShHD={ds*1e6:+.1f}"
            )
    print(
        f"\n  TALLY S2 (17 cells): X_CRPS {xw}W/{xl}L (worst {max_dx[0]:+.4f} @ {max_dx[1]}) | "
        f"logR {lw}W/{ll}L | kalMAE {kw}W/{kl}L | shHD {sw}W/{sl}L | |cov-0.9| {cw}W/{cl}L"
    )
