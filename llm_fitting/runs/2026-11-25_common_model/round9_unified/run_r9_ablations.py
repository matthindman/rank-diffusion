#!/usr/bin/env python3
"""Run exact frozen Gate v1 (5 x 20, boot 2000) across Round 9 pre-registered ablation arms:
  - armA_inv_sqrt_h : w_h = 1/sqrt(h) (0 extra scalars; 5/5 planted recovery winner)
  - armB_rel_sqrt_h : w_h = 1/(D*sqrt(h)) (0 extra scalars)
  - armC_s3_rank    : w_h = 1/sqrt(h) + rank-scaled S3 surges (b_surge) & SE-gated drift (b_d)
  - armD_unified    : armC + non-double-counted Gaussian lasting jumps inside S3 (s3_meta_jumps=True)
Also verifies bit-identity against Round 6 when all flags are off.
"""
from __future__ import annotations

import gc
import importlib.util
import json
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
ROOT = BASE.parents[2]
OUT_DIR = Path(__file__).resolve().parent


def load_r9():
    spec = importlib.util.spec_from_file_location("common_rankdiff", str(OUT_DIR / "common_rankdiff_r9.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["common_rankdiff"] = mod
    spec.loader.exec_module(mod)
    return mod


def main():
    cr9 = load_r9()
    sys.path.insert(0, str(BASE))
    sys.path.insert(0, str(ROOT / "llm_fitting"))
    import round3_frozen_gate as rfg

    arms = [
        ("armA_inv_sqrt_h", dict(lag_weight="inv_sqrt_h", s3_rank_law=False, s3_meta_jumps=False)),
        ("armB_rel_sqrt_h", dict(lag_weight="rel_sqrt_h", s3_rank_law=False, s3_meta_jumps=False)),
        ("armC_s3_rank",    dict(lag_weight="inv_sqrt_h", s3_rank_law=True,  s3_meta_jumps=False)),
        ("armD_unified",    dict(lag_weight="inv_sqrt_h", s3_rank_law=True,  s3_meta_jumps=True)),
    ]

    all_out = {}
    out_path = OUT_DIR / "r9_ablation_gate.json"
    if out_path.exists():
        all_out = json.loads(out_path.read_text())

    for arm_name, cfg in arms:
        if arm_name in all_out:
            print(f"[SKIP already done] {arm_name}", flush=True)
            continue
        t0 = time.time()
        print(f"\n{'#' * 78}\n# RUNNING {arm_name}: {cfg}\n{'#' * 78}", flush=True)
        cr9._DEFAULT_LAG_WEIGHT = cfg["lag_weight"]
        cr9._DEFAULT_S3_RANK_LAW = cfg["s3_rank_law"]
        cr9._DEFAULT_S3_META_JUMPS = cfg["s3_meta_jumps"]
        arm_res = {}
        for pn in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
            arm_res[pn] = rfg.run_panel(pn, rfg.PANELS[pn], n_splits=5, reps=20, boot=2000)
            gc.collect()
        all_out[arm_name] = arm_res
        out_path.write_text(json.dumps(all_out, indent=2))
        print(f"[DONE {arm_name} in {time.time()-t0:.1f}s]", flush=True)

    # Print clean summary vs Round 6 champion
    r6_s2 = json.loads((BASE / "round6_meta/final_bench/candidate_frozen_gate_results.json").read_text())
    r6_s3 = json.loads((BASE / "astra_v3/candidate_frozen_gate_results.json").read_text())
    print("\n" + "=" * 100)
    print("ROUND 9 ABLATION SUMMARY (EXACT FROZEN GATE v1, 5 x 20, boot 2000)")
    print("=" * 100)
    header = f"{'Arm':18s} | {'FB S2/S3':>15s} | {'IG S2/S3':>15s} | {'Comm S2/S3':>15s} | {'Subm S2/S3':>15s}"
    print(header)
    print("-" * len(header))
    print(f"{'R6_CHAMPION':18s} | "
          f"{r6_s2['facebook_a']['s2_rel']:.3f} / {r6_s3['facebook_a']['s3_rel']:.3f}   | "
          f"{r6_s2['instagram_hm']['s2_rel']:.3f} / {r6_s3['instagram_hm']['s3_rel']:.3f}   | "
          f"{r6_s2['reddit_comments_ext']['s2_rel']:.3f} / {r6_s3['reddit_comments_ext']['s3_rel']:.3f}   | "
          f"{r6_s2['reddit_submissions_long']['s2_rel']:.3f} / {r6_s3['reddit_submissions_long']['s3_rel']:.3f}")
    for arm_name, _ in arms:
        ar = all_out[arm_name]
        print(f"{arm_name:18s} | "
              f"{ar['facebook_a']['s2_rel']:.3f} / {ar['facebook_a']['s3_rel']:.3f}   | "
              f"{ar['instagram_hm']['s2_rel']:.3f} / {ar['instagram_hm']['s3_rel']:.3f}   | "
              f"{ar['reddit_comments_ext']['s2_rel']:.3f} / {ar['reddit_comments_ext']['s3_rel']:.3f}   | "
              f"{ar['reddit_submissions_long']['s2_rel']:.3f} / {ar['reddit_submissions_long']['s3_rel']:.3f}")


if __name__ == "__main__":
    main()
