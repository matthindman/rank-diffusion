#!/usr/bin/env python3
"""Quick Goal 1 structure check (reduced seeds) for a candidate module, using the gold benchmark's own
`_fit_and_sim_g1` / `structure_stats` code path so numbers are comparable to round3_gold_benchmark_results.json.
Usage: diag_g1_quick.py <module.py> <out.json> [n_seeds] [panels,...]"""
from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
ROOT = BASE.parents[2]
mod_path = Path(sys.argv[1]).resolve()
out_path = Path(sys.argv[2]).resolve()
n_seeds = int(sys.argv[3]) if len(sys.argv) > 3 else 6
panels = sys.argv[4].split(",") if len(sys.argv) > 4 else ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]
spec = importlib.util.spec_from_file_location("common_rankdiff", str(mod_path))
cr = importlib.util.module_from_spec(spec)
sys.modules["common_rankdiff"] = cr
spec.loader.exec_module(cr)
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(ROOT / "llm_fitting"))
import round3_gold_benchmark as rgb  # noqa: E402

r3 = json.loads((BASE / "round3_gold_benchmark_results.json").read_text())["panels"]
out = {}
for pn in panels:
    t0 = time.time()
    cfg = rgb.PANELS[pn]
    data, sb = rgb.load_cached(pn)
    X = data["X"].astype(np.float64)
    R = data["R"].astype(np.float64)
    fl = data["floors"].astype(np.float64)
    K = int(data["K"])
    emp = r3[pn]["emp"]
    res = {}
    for lbl, asym in (("S2", False), ("S3", True)):
        p, g1 = rgb._fit_and_sim_g1(X, R, fl, K, cfg, sb, asymmetry=asym, n_seeds=n_seeds)
        res[lbl] = dict(params=rgb._param_dict(p), g1=g1)
    out[pn] = res
    print(f"\n===== {pn} (n_seeds={n_seeds}, {time.time()-t0:.0f}s) | S2 sL0={res['S2']['params']['sigma_L0']:.4f} beta={res['S2']['params']['beta']:+.3f} phi={res['S2']['params']['phi']:.2f} | "
          f"S3 sL0={res['S3']['params']['sigma_L0']:.4f} surge(lam={res['S3']['params']['surge_lam']:.5f},mu={res['S3']['params']['surge_mu']:.2f},Vs={res['S3']['params']['surge_Vs']:.3f}) sd={res['S3']['params']['sigma_d']:.5f}")
    print(f"  {'metric':12s} | {'emp':>8s} | {'R3 S2':>8s} {'CAND S2':>8s} | {'R3 S3':>8s} {'CAND S3':>8s}")
    rows = [
        ("Turn top10", lambda d: d["turnover"]["10"]),
        ("Turn K/64", lambda d: d["turnover"][str(K // 64)]),
        ("Turn K/4", lambda d: d["turnover"][str(K // 4)]),
        ("Rank-1 chg", lambda d: d["occ_change"]["1"]),
        ("Head share", lambda d: d["head_share"]),
        ("Max X", lambda d: d["max_X"]),
        ("Boundary", lambda d: d["boundary_ratio"]),
        ("Dep rate", lambda d: d["dep_rate"]),
        ("Dep imm%", lambda d: d["dep_imm_cross_frac"]),
        ("Dep ret13%", lambda d: d["dep_return13_frac"]),
        ("Gapped rho", lambda d: d["gapped_rho"]),
        ("Asym D", lambda d: d["D"]),
        ("p_fall", lambda d: d["p_fall"]),
        ("p_rise", lambda d: d["p_rise"]),
        ("m21", lambda d: d["m21"]),
        ("lev", lambda d: d["lev"]),
        ("Lad gap r1", lambda d: d["lad_last"]["1"]),
    ]
    for name, fn in rows:
        try:
            e = fn(emp)
            a2 = fn(r3[pn]["g1_s2"])["median"]
            c2 = fn(res["S2"]["g1"])["median"]
            a3 = fn(r3[pn]["g1_s3"])["median"]
            c3 = fn(res["S3"]["g1"])["median"]
            print(f"  {name:12s} | {e:8.3f} | {a2:8.3f} {c2:8.3f} | {a3:8.3f} {c3:8.3f}")
        except Exception as ex:  # noqa: BLE001
            print(f"  {name:12s} | (n/a: {ex})")
    hb_e = emp["head_share_blocks"]
    hb2 = [d["median"] for d in res["S2"]["g1"]["head_share_blocks"]]
    hb3 = [d["median"] for d in res["S3"]["g1"]["head_share_blocks"]]
    print("  head share blocks emp :", " ".join(f"{v:.3f}" for v in hb_e))
    print("  head share blocks S2  :", " ".join(f"{v:.3f}" for v in hb2))
    print("  head share blocks S3  :", " ".join(f"{v:.3f}" for v in hb3))
    print("  dec_y emp:", " ".join(f"{v:+.3f}" for v in emp["dec_y"]))
    print("  dec_y S3 :", " ".join(f"{d['median']:+.3f}" for d in res["S3"]["g1"]["dec_y"]))
    out_path.write_text(json.dumps(out, indent=1, default=float))
