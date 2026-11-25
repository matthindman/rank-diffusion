#!/usr/bin/env python3
"""Apples-to-apples candidate benchmark runner for Astra 6.2 / Fable 5.2 model revisions.

Loads a candidate module (e.g. astra_v2/common_rankdiff_astra.py) under the name
`common_rankdiff` and runs the EXACT bit-identical frozen Gate v1 (`round3_frozen_gate.py`)
and/or repaired Round 3 Gold Benchmark (`round3_gold_benchmark.py`), saving outputs into
`--out-dir` and printing a side-by-side comparison against the Round 3 baseline JSONs.

Usage:
  /Library/Frameworks/Python.framework/Versions/3.11/bin/python3 -u benchmark_candidate.py \
      --module astra_v2/common_rankdiff_astra.py \
      --out-dir astra_v2 \
      --mode gate|gold|both \
      [--panels facebook_a,instagram_hm,reddit_comments_ext,reddit_submissions_long]
"""
from __future__ import annotations

import argparse
import gc
import importlib.util
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ROOT = BASE_DIR.parents[2]


def load_candidate_as_common_rankdiff(module_path: Path):
    spec = importlib.util.spec_from_file_location("common_rankdiff", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["common_rankdiff"] = mod
    spec.loader.exec_module(mod)
    return mod


def print_gate_diff(cand: dict, base_path: Path):
    if not base_path.exists():
        return
    base = json.loads(base_path.read_text())
    print("\n================ GATE v1 SIDE-BY-SIDE vs ROUND 3 BASELINE ================", flush=True)
    print(f"{'Panel':24s} | {'Base S2':>10s} {'Cand S2':>10s} {'dS2':>8s} | {'Base S3':>10s} {'Cand S3':>10s} {'dS3':>8s} | {'Persist':>8s}", flush=True)
    print("-" * 102, flush=True)
    for pn, cv in cand.items():
        bv = base.get(pn, {})
        bs2, cs2 = bv.get("s2_rel", float("nan")), cv["s2_rel"]
        bs3, cs3 = bv.get("s3_rel", float("nan")), cv["s3_rel"]
        print(f"{pn:24s} | {bs2:10.3f} {cs2:10.3f} {cs2-bs2:+8.3f} | {bs3:10.3f} {cs3:10.3f} {cs3-bs3:+8.3f} | {cv['base_rel']:8.3f}", flush=True)


def print_gold_diff(cand: dict, base_path: Path):
    if not base_path.exists():
        return
    base = json.loads(base_path.read_text())["panels"]
    print("\n================ GOLD BENCHMARK SIDE-BY-SIDE vs ROUND 3 BASELINE ================", flush=True)
    for pn, cv in cand["panels"].items():
        bv = base.get(pn)
        if not bv:
            continue
        print(f"\n--- {pn} (S2 sc: {bv['params_s2']['n_scalars']}->{cv['params_s2']['n_scalars']}, S3 sc: {bv['params_s3']['n_scalars']}->{cv['params_s3']['n_scalars']}) ---", flush=True)
        for lbl, path_fn in [
            ("Turn top10", lambda x: x["turnover"]["10"]["median"]),
            ("Head share", lambda x: x["head_share"]["median"]),
            ("Boundary", lambda x: x["boundary_ratio"]["median"]),
            ("Dep imm%", lambda x: x["dep_imm_cross_frac"]["median"]),
            ("Gapped rho", lambda x: x["gapped_rho"]["median"]),
            ("Asym D", lambda x: x["D"]["median"]),
        ]:
            b2, c2 = path_fn(bv["g1_s2"]), path_fn(cv["g1_s2"])
            b3, c3 = path_fn(bv["g1_s3"]), path_fn(cv["g1_s3"])
            print(f"  G1 {lbl:10s}: S2 {b2:+.4f} -> {c2:+.4f} | S3 {b3:+.4f} -> {c3:+.4f}", flush=True)
        for hs, cr_h in cv["goal2"]["by_horizon"].items():
            br_h = bv["goal2"]["by_horizon"][hs]
            print(
                f"  G2 h={int(hs):2d}w: X_CRPS S2 {br_h['crps_X_s2']:.4f}->{cr_h['crps_X_s2']:.4f} S3 {br_h['crps_X_s3']:.4f}->{cr_h['crps_X_s3']:.4f} | "
                f"logR S2 {br_h['crps_lr_s2']:.4f}->{cr_h['crps_lr_s2']:.4f} S3 {br_h['crps_lr_s3']:.4f}->{cr_h['crps_lr_s3']:.4f} | "
                f"cov90 S2 {br_h['cov90_s2']:.3f}->{cr_h['cov90_s2']:.3f} S3 {br_h['cov90_s3']:.3f}->{cr_h['cov90_s3']:.3f} | "
                f"shHD S2 {br_h['share_mae_hd_s2']:.6f}->{cr_h['share_mae_hd_s2']:.6f} S3 {br_h['share_mae_hd_s3']:.6f}->{cr_h['share_mae_hd_s3']:.6f}",
                flush=True,
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--module", required=True, help="Path to candidate common_rankdiff python file")
    ap.add_argument("--out-dir", required=True, help="Output directory for candidate JSON results")
    ap.add_argument("--mode", choices=["gate", "gold", "both"], default="both")
    ap.add_argument("--panels", default="facebook_a,instagram_hm,reddit_comments_ext,reddit_submissions_long")
    ap.add_argument("--n-seeds", type=int, default=20)
    ap.add_argument("--n-ens", type=int, default=39)
    args = ap.parse_args()

    mod_path = Path(args.module).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    panels = [p.strip() for p in args.panels.split(",") if p.strip()]

    load_candidate_as_common_rankdiff(mod_path)
    sys.path.insert(0, str(BASE_DIR))
    sys.path.insert(0, str(ROOT / "llm_fitting"))

    if args.mode in ("gate", "both"):
        import round3_frozen_gate as rfg
        gate_out = {}
        for pn in panels:
            gate_out[pn] = rfg.run_panel(pn, rfg.PANELS[pn], n_splits=5, reps=20, boot=2000)
            gc.collect()
        g_path = out_dir / "candidate_frozen_gate_results.json"
        g_path.write_text(json.dumps(gate_out, indent=2))
        print(f"\nSaved -> {g_path}", flush=True)
        print_gate_diff(gate_out, BASE_DIR / "round3_frozen_gate_results.json")

    if args.mode in ("gold", "both"):
        import round3_gold_benchmark as rgb
        oracle = rgb.run_gaussian_oracle_check()
        gold_out = {"oracle_check": oracle, "panels": {}}
        for pn in panels:
            gold_out["panels"][pn] = rgb.run_panel(pn, rgb.PANELS[pn], n_seeds=args.n_seeds, n_ens=args.n_ens)
            gc.collect()
        gold_path = out_dir / "candidate_gold_benchmark_results.json"
        gold_path.write_text(json.dumps(gold_out, indent=2))
        print(f"\nSaved -> {gold_path}", flush=True)
        print_gold_diff(gold_out, BASE_DIR / "round3_gold_benchmark_results.json")


if __name__ == "__main__":
    main()
