"""Round 6 factorial Gate v1 ablation (exact frozen Gate v1, 5x20, boot 2000; DEVELOPMENT cache only).

Compares four clean arms against Round 4 (Astra) and Round 5 (Fable S2+J):
  Arm 1 (trim10_only):  scale_moment="variance", filter_stat="trim10" (0 new scalars)
  Arm 2 (rob_bJ):       scale_moment="robust",   filter_stat="trim10", fit_jump_slope=True (13 sc S2)
  Arm 3 (meta_split):   scale_moment="meta",     filter_stat="trim10", fit_jump_slope=True, fit_s3_rank_laws=True
                        (unified lasting split + rank-attenuated jumps/surges/drift; works for BOTH S2 & S3!)
  Arm 4 (var_s3rank):   scale_moment="variance", filter_stat="trim10", fit_s3_rank_laws=True
                        (tests S3 rank laws + trim10 WITHOUT lasting jump split)
"""
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

# Load common_rankdiff_r6 AS "common_rankdiff" before importing round3_frozen_gate!
spec = importlib.util.spec_from_file_location("common_rankdiff", str(OUT / "common_rankdiff_r6.py"))
cr6 = importlib.util.module_from_spec(spec)
sys.modules["common_rankdiff"] = cr6
spec.loader.exec_module(cr6)

import round3_frozen_gate as rfg

PANELS = ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]

ARMS = [
    ("arm1_trim10", dict(scale_moment="variance", fit_jump_slope=False, fit_s3_rank_laws=False, filter_stat="trim10")),
    ("arm2_rob_bJ", dict(scale_moment="robust", fit_jump_slope=True, fit_s3_rank_laws=False, filter_stat="trim10")),
    ("arm3_meta_split", dict(scale_moment="meta", fit_jump_slope=True, fit_s3_rank_laws=True, filter_stat="trim10")),
    ("arm4_var_s3rank", dict(scale_moment="variance", fit_jump_slope=False, fit_s3_rank_laws=True, filter_stat="trim10")),
]

orig_est = cr6.estimate_common
all_res = {}

for arm_name, kw in ARMS:
    print(f"\n==================== {arm_name} ({kw}) ====================", flush=True)
    def _wrapped(*args, _kw=kw, **kwargs):
        merged = {**_kw, **kwargs}
        return orig_est(*args, **merged)
    cr6.estimate_common = _wrapped
    arm_res = {}
    for panel in PANELS:
        r = rfg.run_panel(panel, rfg.PANELS[panel])
        arm_res[panel] = r
        print(
            f"  [{arm_name} | {panel}] S2={r['s2_rel']:.3f}±{r['s2_sd']:.3f} cov={r['s2_cov']:.2f} ({r['s2_wins_base']}/5)"
            f" | S3={r['s3_rel']:.3f}±{r['s3_sd']:.3f} cov={r['s3_cov']:.2f} ({r['s3_wins_base']}/5)",
            flush=True,
        )
    all_res[arm_name] = arm_res
    with open(OUT / "r6_ablation_gate.json", "w") as f:
        json.dump(all_res, f, indent=2)

print("\n=== SUMMARY COMPARISON vs R4 (Astra) & R5 (Fable S2+J) ===")
r4 = json.load(open(RUNS / "round4_baseline/candidate_frozen_gate_results.json"))
r5 = json.load(open(RUNS / "fable_v2/candidate_frozen_gate_results.json"))
for panel in PANELS:
    print(f"\n[{panel}] base={r4[panel]['base_rel']:.3f}")
    print(f"  R4(Astra)       S2={r4[panel]['s2_rel']:.3f} ({r4[panel]['s2_wins_base']}/5, {int(r4[panel]['s2_cov']*100)}%) | S3={r4[panel]['s3_rel']:.3f} ({r4[panel]['s3_wins_base']}/5, {int(r4[panel]['s3_cov']*100)}%)")
    print(f"  R5(Fable S2+J)  S2={r5[panel]['s2_rel']:.3f} ({r5[panel]['s2_wins_base']}/5, {int(r5[panel]['s2_cov']*100)}%) | S3={r5[panel]['s3_rel']:.3f} ({r5[panel]['s3_wins_base']}/5, {int(r5[panel]['s3_cov']*100)}%)")
    for arm_name, _ in ARMS:
        r = all_res[arm_name][panel]
        print(f"  {arm_name:15s} S2={r['s2_rel']:.3f} ({r['s2_wins_base']}/5, {int(r['s2_cov']*100)}%) | S3={r['s3_rel']:.3f} ({r['s3_wins_base']}/5, {int(r['s3_cov']*100)}%)")
