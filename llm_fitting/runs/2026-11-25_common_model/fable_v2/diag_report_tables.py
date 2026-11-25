#!/usr/bin/env python3
"""Markdown tables for FABLE_REPORT §5 from the candidate archives vs Round 3 and Round 4 (Astra) archives.
Round 4 S2 = Astra candidate; Round 4 S3 = Astra diag_ablation 'filter only' (s2 slots); Round 4 structure = Round 3.
Usage: diag_report_tables.py <gate.json> <gold.json> > final/report_tables.md"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
gate_c = json.loads(Path(sys.argv[1]).read_text())
gold_c = json.loads(Path(sys.argv[2]).read_text())["panels"]
gate_r3 = json.loads((BASE / "round3_frozen_gate_results.json").read_text())
gold_r3 = json.loads((BASE / "round3_gold_benchmark_results.json").read_text())["panels"]
gate_r4 = json.loads((BASE / "astra_v2" / "candidate_frozen_gate_results.json").read_text())
gold_r4 = json.loads((BASE / "astra_v2" / "candidate_gold_benchmark_results.json").read_text())["panels"]
abl = json.loads((BASE / "astra_v2" / "diag_ablation.json").read_text())
SHORT = {"facebook_a": "FB", "instagram_hm": "IG", "reddit_comments_ext": "Comments", "reddit_submissions_long": "Submissions"}


def g(d, k, sd, cov, wins):
    return f"{d[k]:.3f} ± {d[sd]:.3f} · {int(round(d[cov]*100))}% · {d[wins]}/5"


print("### 5.1 Exact frozen Gate v1 (5 splits × 20 reps, boot 2000): mean rel. error ± split SD · median-consistency · wins vs persistence\n")
print("| Panel | Persistence | R3 S2 | R4 S2 | **Cand S2** | paired ΔS2 vs R4 | R3 S3 | R4 S3 | **Cand S3** | paired ΔS3 vs R4 |")
print("|---|---|---|---|---|---|---|---|---|---|")
for pn, c in gate_c.items():
    a, b2, b3 = gate_r3[pn], gate_r4[pn], abl[pn]["gate"]
    d2 = [x["s2_rel"] - y["s2_rel"] for x, y in zip(c["splits"], b2["splits"])]
    d3 = [x["s3_rel"] - y["s2_rel"] for x, y in zip(c["splits"], b3["splits"])]
    print(f"| {SHORT[pn]} | {c['base_rel']:.3f} ± {c['base_sd']:.3f} | {g(a,'s2_rel','s2_sd','s2_cov','s2_wins_base')} | {g(b2,'s2_rel','s2_sd','s2_cov','s2_wins_base')} | **{g(c,'s2_rel','s2_sd','s2_cov','s2_wins_base')}** | {np.mean(d2):+.3f} ± {np.std(d2):.3f} ({sum(d<0 for d in d2)}/5 better) | {g(a,'s3_rel','s3_sd','s3_cov','s3_wins_base')} | {g(b3,'s2_rel','s2_sd','s2_cov','s2_wins_base')} | **{g(c,'s3_rel','s3_sd','s3_cov','s3_wins_base')}** | {np.mean(d3):+.3f} ± {np.std(d3):.3f} ({sum(d<0 for d in d3)}/5 better) |")

print("\nMean gate moments across the five origins (emp · historical · R4 S2 · Cand S2 · R4 S3 · Cand S3):\n")
print("| Panel | moment | emp | hist | R4 S2 | Cand S2 | R4 S3 | Cand S3 |")
print("|---|---|---|---|---|---|---|---|")
for pn, c in gate_c.items():
    b2, b3 = gate_r4[pn], abl[pn]["gate"]
    keys = [f"dRank{h}" for h in c["hor"]] + ["RACF1", "coll1", "coll5", "coll20"]
    for k in keys:
        def m(lst, slot):
            v = [s[slot].get(k, np.nan) for s in lst]
            return float(np.nanmean(v)) if np.any(np.isfinite(v)) else np.nan
        print(f"| {SHORT[pn]} | {k} | {m(c['splits'],'em'):.3f} | {m(c['splits'],'bm'):.3f} | {m(b2['splits'],'sm2'):.3f} | {m(c['splits'],'sm2'):.3f} | {m(b3['splits'],'sm2'):.3f} | {m(c['splits'],'sm3'):.3f} |")


def fm(d):
    return f"{d['median']:+.3f} [{d['q05']:+.3f}, {d['q95']:+.3f}]"


def meds(lst, fmt):
    return " ".join(format(d["median"], fmt) for d in lst)


def medsl(lst, fmt):
    return " / ".join(format(d["median"], fmt) for d in lst)


def vals(lst, fmt):
    return " ".join(format(v, fmt) for v in lst)


print("\n### 5.2 Goal 1 — unconditional structure (20 paired seeds; median [q05, q95]); Round 4 structure = Round 3\n")
for pn, cv in gold_c.items():
    a = gold_r3[pn]
    K = cv["K"]
    emp = cv["emp"]
    p2a, p2c, p3a, p3c = a["params_s2"], cv["params_s2"], a["params_s3"], cv["params_s3"]
    print(f"\n**{SHORT[pn]}** (K = {K}). Scalars S2 {p2a['n_scalars']} → {p2c['n_scalars']}, S3 {p3a['n_scalars']} → {p3c['n_scalars']}. "
          f"S2 σ_L0 {p2a['sigma_L0']:.4f} → {p2c['sigma_L0']:.4f}, β {p2a['beta']:+.3f} → {p2c['beta']:+.3f}, φ {p2a['phi']:.2f} → {p2c['phi']:.2f}, "
          f"a_ξ {p2a['a_xi']:+.2f} → {p2c['a_xi']:+.2f}, a_obs {p2a['a_obs']:+.2f} → {p2c['a_obs']:+.2f}; "
          ""
          f"S3 σ_L0 {p3a['sigma_L0']:.4f} → {p3c['sigma_L0']:.4f}, surge (λ {p3c['surge_lam']:.5f}, μ {p3c['surge_mu']:.2f}, hl {p3c['surge_hl']:.0f}), σ_d {p3c['sigma_d']:.5f}.\n")
    print("| Statistic | Empirical | R3/R4 S2 | **Cand S2** | R3/R4 S3 | **Cand S3** |")
    print("|---|---|---|---|---|---|")
    rows = [
        ("Top-10 turnover", lambda d: d["turnover"]["10"]), ("K/64 turnover", lambda d: d["turnover"][str(K // 64)]),
        ("K/16 turnover", lambda d: d["turnover"][str(K // 16)]), ("K/4 turnover", lambda d: d["turnover"][str(K // 4)]),
        ("Rank-1 weekly change", lambda d: d["occ_change"]["1"]), ("Head share (last block)", lambda d: d["head_share"]),
        ("Max log activity", lambda d: d["max_X"]), ("Boundary ratio", lambda d: d["boundary_ratio"]),
        ("Departure rate", lambda d: d["dep_rate"]), ("Immediate crossing / dep.", lambda d: d["dep_imm_cross_frac"]),
        ("Absent→returns ≤13w / dep.", lambda d: d["dep_return13_frac"]), ("Terminal / dep.", lambda d: d["dep_terminal_frac"]),
        ("Gapped ρ", lambda d: d["gapped_rho"]), ("Asymmetry D", lambda d: d["D"]), ("p_fall", lambda d: d["p_fall"]), ("p_rise", lambda d: d["p_rise"]),
        ("m21", lambda d: d["m21"]), ("m12", lambda d: d["m12"]), ("Leverage", lambda d: d["lev"]), ("Decline/gain run ratio r2", lambda d: d["r2"]),
    ]
    for name, fn in rows:
        try:
            print(f"| {name} | {fn(emp):+.3f} | {fm(fn(a['g1_s2']))} | **{fm(fn(cv['g1_s2']))}** | {fm(fn(a['g1_s3']))} | **{fm(fn(cv['g1_s3']))}** |")
        except Exception:  # noqa: BLE001
            pass
    hs_e = " / ".join(format(v, ".3f") for v in emp["head_share_blocks"])
    print(f"| Head share by block | {hs_e} | {medsl(a['g1_s2']['head_share_blocks'], '.3f')} | **{medsl(cv['g1_s2']['head_share_blocks'], '.3f')}** | {medsl(a['g1_s3']['head_share_blocks'], '.3f')} | **{medsl(cv['g1_s3']['head_share_blocks'], '.3f')}** |")
    print(f"| E[y given x decile] d1..d10 | {vals(emp['dec_y'], '+.2f')} | {meds(a['g1_s2']['dec_y'], '+.2f')} | **{meds(cv['g1_s2']['dec_y'], '+.2f')}** | {meds(a['g1_s3']['dec_y'], '+.2f')} | **{meds(cv['g1_s3']['dec_y'], '+.2f')}** |")

print("\n### 5.3 Goal 2 — individual endpoint forecasts (M = 39, 5 rolling origins, common truth cohort); each cell R3 → R4 → **Cand**\n")
for pn, cv in gold_c.items():
    a, f4, ab = gold_r3[pn], gold_r4[pn], abl[pn]
    print(f"\n**{SHORT[pn]}**\n")
    print("| h | arm | X CRPS | Kalman MAE (no / EWMA) | cov90 (0.900) | PIT SD (0.289) | logR CRPS (no / rank-bin) | fair Brier head (rank-bin) | head share-shift MAE ×1e6 (zero / EWMA) | head shift corr (EWMA) |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for hs, c in cv["goal2"]["by_horizon"].items():
        o, r4h, abh = a["goal2"]["by_horizon"][hs], f4["goal2"]["by_horizon"][hs], ab["goal2"]["by_horizon"][hs]
        for arm, slot_o, slot_4 in (("S2", "s2", "s2"), ("S3", "s3", "s2")):
            src4 = r4h if arm == "S2" else abh
            def tri(metric, sc=1.0, fmt=".4f"):
                return f"{o[metric+'_'+slot_o]*sc:{fmt}} → {src4[metric+'_'+slot_4]*sc:{fmt}} → **{c[metric+'_'+slot_o]*sc:{fmt}}**"
            print(f"| {int(hs)} | {arm} | {tri('crps_X')} | {tri('mae_kal')} ({c['mae_no']:.4f} / {c['mae_ll']:.4f}) | {tri('cov90', 1, '.3f')} | {tri('pit_sd', 1, '.3f')} | "
                  f"{tri('crps_lr')} ({c['crps_lr_no']:.4f} / {c['crps_lr_rb']:.4f}) | {tri('brier_hd')} ({c['brier_hd_rb']:.4f}) | "
                  f"{tri('share_mae_hd', 1e6, '.1f')} ({c['share_mae_no_hd']*1e6:.1f} / {c['share_mae_ll_hd']*1e6:.1f}) | {tri('share_corr_hd', 1, '.3f')} ({c['share_corr_ll_hd']:.3f}) |")

print("\n### 5.4 Out-of-period structure (fit 2019–20 weeks 0..103 → roll 2021–22, 20 seeds): emp 21–22 | R3 S2 → **Cand S2** | R3 S3 → R4 S3 → **Cand S3**\n")
for pn, cv in gold_c.items():
    oc = cv.get("oop_2019_20_to_2021_22")
    if not oc:
        continue
    oa, oab = gold_r3[pn]["oop_2019_20_to_2021_22"], abl[pn]["oop"]
    e21 = oc["emp_2021_22"]
    K = cv["K"]
    print(f"\n**{SHORT[pn]}**\n")
    print("| Statistic | emp 2021–22 | R3 S2 | **Cand S2** | R3 S3 | R4 S3 | **Cand S3** |")
    print("|---|---|---|---|---|---|---|")
    for name, fn in [
        ("Head share", lambda d: d["head_share"]), ("Top-10 turnover", lambda d: d["turnover"]["10"]),
        ("K/4 turnover", lambda d: d["turnover"][str(K // 4)]), ("Ladder gap rank 1", lambda d: d["lad_last"]["1"]),
        ("Boundary ratio", lambda d: d["boundary_ratio"]), ("Departure rate", lambda d: d["dep_rate"]),
        ("Immediate crossing / dep.", lambda d: d["dep_imm_cross_frac"]), ("Gapped ρ", lambda d: d["gapped_rho"]), ("Asymmetry D", lambda d: d["D"]),
    ]:
        print(f"| {name} | {fn(e21):+.3f} | {fm(fn(oa['S2']['sim']))} | **{fm(fn(oc['S2']['sim']))}** | {fm(fn(oa['S3']['sim']))} | {fm(fn(oab['S2']['sim']))} | **{fm(fn(oc['S3']['sim']))}** |")
    print(f"| E[y given x decile] d1..d10 | {vals(e21['dec_y'], '+.2f')} | | | {meds(oa['S3']['sim']['dec_y'], '+.2f')} | | **{meds(oc['S3']['sim']['dec_y'], '+.2f')}** |")
