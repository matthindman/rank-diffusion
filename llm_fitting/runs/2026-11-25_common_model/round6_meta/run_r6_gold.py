"""Run full Gold Benchmark (20 seeds Goal 1 + M=39 Goal 2 + 20 seeds OOP) on R6-meta_compact."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
RUNS = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent

sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "llm_fitting"))
sys.path.insert(0, str(RUNS))

spec = importlib.util.spec_from_file_location("common_rankdiff", str(OUT / "common_rankdiff_r6.py"))
cr6 = importlib.util.module_from_spec(spec)
sys.modules["common_rankdiff"] = cr6
spec.loader.exec_module(cr6)

import round3_gold_benchmark as rgb

PANELS = ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]
orig_est = cr6.estimate_common
KW = dict(scale_moment="meta", fit_jump_slope=True, fit_s3_rank_laws=False, filter_stat="median")

def _wrapped(*args, **kwargs):
    return orig_est(*args, **{**KW, **kwargs})

cr6.estimate_common = _wrapped

gold = {"oracle_check": rgb.run_gaussian_oracle_check(), "panels": {}}
for pn in PANELS:
    print(f"\n=== GOLD BENCHMARK (R6-meta_compact): {pn} ===", flush=True)
    gold["panels"][pn] = rgb.run_panel(pn, rgb.PANELS[pn])

with open(OUT / "r6_meta_compact_gold.json", "w") as f:
    json.dump(gold, f, indent=2)

# Print full comparison vs Round 4 (Astra) & Round 5 (Fable S2+J)
b4 = json.load(open(RUNS / "astra_v2/candidate_gold_benchmark_results.json"))
b5 = json.load(open(RUNS / "fable_v2/candidate_gold_benchmark_results.json"))
b3 = json.load(open(RUNS / "round3_gold_benchmark_results.json"))

print("\n================ GOAL 1 COMPARISON (20 seeds median) ================")
for pn in PANELS:
    p3, p4, p5, p6 = b3["panels"][pn], b4["panels"][pn], b5["panels"][pn], gold["panels"][pn]
    print(f"\n[{pn}] scalars: S2 R4={p4['params_s2']['n_scalars']} R5={p5['params_s2']['n_scalars']} R6={p6['params_s2']['n_scalars']} | S3 R3={p3['params_s3']['n_scalars']} R6={p6['params_s3']['n_scalars']}")
    for m in ["dep_rate", "dep_imm_cross_frac", "head_share", "boundary_ratio", "max_X", "gapped_rho", "D"]:
        e = p6["emp"][m]
        print(f"  {m:18s}: emp={e:+.4f} | R4_S2={p4['g1_s2'][m]['median']:+.4f} R5_S2={p5['g1_s2'][m]['median']:+.4f} R6_S2={p6['g1_s2'][m]['median']:+.4f} | R3_S3={p3['g1_s3'][m]['median']:+.4f} R6_S3={p6['g1_s3'][m]['median']:+.4f}")
    print(f"  {'top10_turn':18s}: emp={p6['emp']['turnover']['10']:.4f} | R4_S2={p4['g1_s2']['turnover']['10']['median']:.4f} R5_S2={p5['g1_s2']['turnover']['10']['median']:.4f} R6_S2={p6['g1_s2']['turnover']['10']['median']:.4f} | R3_S3={p3['g1_s3']['turnover']['10']['median']:.4f} R6_S3={p6['g1_s3']['turnover']['10']['median']:.4f}")
    print(f"  {'rank1_chg':18s}: emp={p6['emp']['occ_change']['1']:.4f} | R4_S2={p4['g1_s2']['occ_change']['1']['median']:.4f} R5_S2={p5['g1_s2']['occ_change']['1']['median']:.4f} R6_S2={p6['g1_s2']['occ_change']['1']['median']:.4f} | R3_S3={p3['g1_s3']['occ_change']['1']['median']:.4f} R6_S3={p6['g1_s3']['occ_change']['1']['median']:.4f}")

print("\n================ GOAL 2 COMPARISON (M=39 exact) ================")
for pn in PANELS:
    p4, p5, p6 = b4["panels"][pn], b5["panels"][pn], gold["panels"][pn]
    print(f"\n[{pn}]")
    for h_str in p6["goal2"]["by_horizon"]:
        h4, h5, h6 = p4["goal2"]["by_horizon"][h_str], p5["goal2"]["by_horizon"][h_str], p6["goal2"]["by_horizon"][h_str]
        s2_4, s2_5, s2_6 = h4["s2"], h5["s2"], h6["s2"]
        s3_6 = h6["s3"]
        print(f"  h={h_str:>2s} S2 | X_crps R4={s2_4['X_crps']:.4f} R5={s2_5['X_crps']:.4f} R6={s2_6['X_crps']:.4f} | cov90 R4={s2_4['cov90']:.3f} R5={s2_5['cov90']:.3f} R6={s2_6['cov90']:.3f} | pit R6={s2_6['pit_sd']:.3f} | hMAE R4={s2_4['head_share_mae']*1e6:.1f} R5={s2_5['head_share_mae']*1e6:.1f} R6={s2_6['head_share_mae']*1e6:.1f}")
        print(f"       S3 | X_crps R6={s3_6['X_crps']:.4f} | cov90 R6={s3_6['cov90']:.3f} | pit R6={s3_6['pit_sd']:.3f} | hMAE R6={s3_6['head_share_mae']*1e6:.1f} corr={s3_6['head_share_corr']:.3f}")
