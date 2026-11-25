#!/usr/bin/env python3
"""Compare a candidate gold-benchmark archive against Round 3 and Round 4.
Round 4 S2 = Astra candidate gold (S2 slots); Round 4 S3 = Astra diag_ablation.json goal2/oop 'S2' slots
(filter-only). Round 4 unconditional structure == Round 3 structure (the filter does not touch it).
Usage: diag_gold_compare.py <candidate_gold_benchmark_results.json>"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
cand = json.loads(Path(sys.argv[1]).resolve().read_text())["panels"]
r3 = json.loads((BASE / "round3_gold_benchmark_results.json").read_text())["panels"]
r4 = json.loads((BASE / "astra_v2" / "candidate_gold_benchmark_results.json").read_text())["panels"]
abl = json.loads((BASE / "astra_v2" / "diag_ablation.json").read_text())


def fmt(d):
    return f"{d['median']:+.3f} [{d['q05']:+.3f},{d['q95']:+.3f}]"


for pn, cv in cand.items():
    a = r3[pn]
    K = cv["K"]
    print(f"\n==================== {pn} (K={K}) ====================")
    p2a, p2c, p3a, p3c = a["params_s2"], cv["params_s2"], a["params_s3"], cv["params_s3"]
    print(f"  scalars S2 {p2a['n_scalars']}->{p2c['n_scalars']} S3 {p3a['n_scalars']}->{p3c['n_scalars']} | "
          f"S2 sL0 {p2a['sigma_L0']:.4f}->{p2c['sigma_L0']:.4f} beta {p2a['beta']:+.3f}->{p2c['beta']:+.3f} phi {p2a['phi']:.2f}->{p2c['phi']:.2f} "
          f"a_xi {p2a['a_xi']:+.2f}->{p2c['a_xi']:+.2f} a_obs {p2a['a_obs']:+.2f}->{p2c['a_obs']:+.2f} | "
          f"S3 sL0 {p3a['sigma_L0']:.4f}->{p3c['sigma_L0']:.4f} surge(lam {p3a['surge_lam']:.5f}->{p3c['surge_lam']:.5f}, mu {p3a['surge_mu']:.2f}->{p3c['surge_mu']:.2f}) sd {p3a['sigma_d']:.5f}->{p3c['sigma_d']:.5f}")
    print(f"  --- Goal 1 (20 seeds, median [q05,q95]): emp | R3/R4 S2 -> CAND S2 | R3/R4 S3 -> CAND S3")
    rows = [
        ("Turn top10", lambda d: d["turnover"]["10"]), ("Turn K/64", lambda d: d["turnover"][str(K // 64)]),
        ("Turn K/16", lambda d: d["turnover"][str(K // 16)]), ("Turn K/4", lambda d: d["turnover"][str(K // 4)]),
        ("Rank-1 chg", lambda d: d["occ_change"]["1"]), ("Head share", lambda d: d["head_share"]),
        ("Max X", lambda d: d["max_X"]), ("Boundary", lambda d: d["boundary_ratio"]),
        ("Dep rate", lambda d: d["dep_rate"]), ("Dep imm%", lambda d: d["dep_imm_cross_frac"]),
        ("Dep ret13%", lambda d: d["dep_return13_frac"]), ("Dep term%", lambda d: d["dep_terminal_frac"]),
        ("Gapped rho", lambda d: d["gapped_rho"]), ("Asym D", lambda d: d["D"]),
        ("p_fall", lambda d: d["p_fall"]), ("p_rise", lambda d: d["p_rise"]),
        ("m21", lambda d: d["m21"]), ("m12", lambda d: d["m12"]), ("lev", lambda d: d["lev"]),
        ("runs r2", lambda d: d["r2"]), ("Lad gap r1", lambda d: d["lad_last"]["1"]),
    ]
    emp = cv["emp"]
    for name, fn in rows:
        try:
            print(f"    {name:11s}: {fn(emp):+.3f} | {fmt(fn(a['g1_s2']))} -> {fmt(fn(cv['g1_s2']))} | {fmt(fn(a['g1_s3']))} -> {fmt(fn(cv['g1_s3']))}")
        except Exception as ex:  # noqa: BLE001
            print(f"    {name:11s}: n/a ({ex})")
    print("    head share blocks emp     :", " ".join(f"{v:.3f}" for v in emp["head_share_blocks"]))
    for lbl, src in (("R3 S2", a["g1_s2"]), ("CAND S2", cv["g1_s2"]), ("R3 S3", a["g1_s3"]), ("CAND S3", cv["g1_s3"])):
        print(f"    head share blocks {lbl:8s}:", " ".join(f"{d['median']:.3f}" for d in src["head_share_blocks"]))
    print("    dec_y emp     :", " ".join(f"{v:+.3f}" for v in emp["dec_y"]))
    for lbl, src in (("R3 S2", a["g1_s2"]), ("CAND S2", cv["g1_s2"]), ("R3 S3", a["g1_s3"]), ("CAND S3", cv["g1_s3"])):
        print(f"    dec_y {lbl:8s}:", " ".join(f"{d['median']:+.3f}" for d in src["dec_y"]))

    print(f"  --- Goal 2 (M=39, 5 origins): per horizon R3 -> R4 -> CAND (S2) | R3 -> R4 -> CAND (S3) | baselines")
    g2c, g2a, g2r4 = cv["goal2"]["by_horizon"], a["goal2"]["by_horizon"], r4[pn]["goal2"]["by_horizon"]
    g2ab = abl[pn]["goal2"]["by_horizon"]
    for hs in g2c:
        c, o, f4, ab = g2c[hs], g2a[hs], g2r4[hs], g2ab[hs]
        print(f"   h={int(hs):2d}w")
        for metric, lbl, base_keys in [
            ("crps_X", "X CRPS", []), ("mae_kal", "Kalman MAE", ["mae_no", "mae_ll"]), ("mae_ens", "Ens MAE", []),
            ("cov90", "cov90", []), ("pit_sd", "PIT SD", []), ("crps_lr", "logR CRPS", ["crps_lr_no", "crps_lr_rb"]),
            ("brier_hd", "fBrier HD", ["brier_hd_rb"]), ("share_mae_hd", "shHD MAE", ["share_mae_no_hd", "share_mae_ll_hd"]),
            ("share_corr_hd", "shHD corr", ["share_corr_ll_hd"]),
        ]:
            sc = 1e6 if metric.startswith("share_mae") else 1.0
            s2 = f"{o[metric+'_s2']*sc:.4f} -> {f4[metric+'_s2']*sc:.4f} -> {c[metric+'_s2']*sc:.4f}"
            s3 = f"{o[metric+'_s3']*sc:.4f} -> {ab[metric+'_s2']*sc:.4f} -> {c[metric+'_s3']*sc:.4f}"
            bl = " ".join(f"{k.replace('_hd','')}={c[k]*sc:.4f}" for k in base_keys)
            print(f"      {lbl:11s}: S2 {s2} | S3 {s3} | {bl}")

    if cv.get("oop_2019_20_to_2021_22"):
        oc, oa = cv["oop_2019_20_to_2021_22"], a["oop_2019_20_to_2021_22"]
        oab = abl[pn]["oop"]
        e21 = oc["emp_2021_22"]
        print(f"  --- OOP 2019-20 -> 2021-22 (20 seeds): emp21-22 | R3 S2 -> CAND S2 | R3 S3 -> R4 S3 -> CAND S3")
        for name, fn in [
            ("Head share", lambda d: d["head_share"]), ("Turn top10", lambda d: d["turnover"]["10"]),
            ("Turn K/4", lambda d: d["turnover"][str(K // 4)]), ("Lad gap r1", lambda d: d["lad_last"]["1"]),
            ("Boundary", lambda d: d["boundary_ratio"]), ("Dep rate", lambda d: d["dep_rate"]),
            ("Dep imm%", lambda d: d["dep_imm_cross_frac"]), ("Gapped rho", lambda d: d["gapped_rho"]), ("Asym D", lambda d: d["D"]),
        ]:
            print(f"    {name:11s}: {fn(e21):+.3f} | {fmt(fn(oa['S2']['sim']))} -> {fmt(fn(oc['S2']['sim']))} | "
                  f"{fmt(fn(oa['S3']['sim']))} -> {fmt(fn(oab['S2']['sim']))} -> {fmt(fn(oc['S3']['sim']))}")
        print("    OOP dec_y emp21 :", " ".join(f"{v:+.3f}" for v in e21["dec_y"]))
        print("    OOP dec_y CAND S3:", " ".join(f"{d['median']:+.3f}" for d in oc["S3"]["sim"]["dec_y"]))
