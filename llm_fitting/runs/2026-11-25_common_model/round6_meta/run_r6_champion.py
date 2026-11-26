"""Round 6 Champion Evaluation: Unified Meta-Split (scale_moment='meta' + fit_jump_slope=True + Astra median filter).

Runs:
  1. Exact Frozen Gate v1 (5x20, boot 2000) for:
       - Meta-Compact: scale_moment='meta', fit_jump_slope=True, fit_s3_rank_laws=False, filter_stat='median'
         (S2: 13 sc; S3: 16-17 sc)
       - Meta-Full:    scale_moment='meta', fit_jump_slope=True, fit_s3_rank_laws=True,  filter_stat='median'
         (S2: 13 sc; S3: 17-19 sc)
  2. Full Gold Benchmark (20 seeds Goal 1 + M=39 Goal 2 + 20 seeds 2019-20->2021-22 OOP)
     for BOTH S2 and S3 on Meta-Full (and S3 on Meta-Compact for Goal 1 comparison!).
Strictly DEVELOPMENT cache only; zero sealed contact.
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

spec = importlib.util.spec_from_file_location("common_rankdiff", str(OUT / "common_rankdiff_r6.py"))
cr6 = importlib.util.module_from_spec(spec)
sys.modules["common_rankdiff"] = cr6
spec.loader.exec_module(cr6)

import round3_frozen_gate as rfg
import round3_gold_benchmark as rgb

PANELS = ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]
orig_est = cr6.estimate_common


def set_mode(**kw):
    def _wrapped(*args, _kw=kw, **kwargs):
        merged = {**_kw, **kwargs}
        return orig_est(*args, **merged)
    cr6.estimate_common = _wrapped


# 1. Exact Frozen Gate v1 for Meta-Compact and Meta-Full
gate_res = {}
for tag, kw in [
    ("meta_compact", dict(scale_moment="meta", fit_jump_slope=True, fit_s3_rank_laws=False, filter_stat="median")),
    ("meta_full",    dict(scale_moment="meta", fit_jump_slope=True, fit_s3_rank_laws=True,  filter_stat="median")),
]:
    print(f"\n==================== GATE v1: {tag} ====================", flush=True)
    set_mode(**kw)
    gate_res[tag] = {}
    for pn in PANELS:
        r = rfg.run_panel(pn, rfg.PANELS[pn])
        gate_res[tag][pn] = r
        print(
            f"  [{tag} | {pn}] S2={r['s2_rel']:.3f}±{r['s2_sd']:.3f} cov={r['s2_cov']:.2f} ({r['s2_wins_base']}/5)"
            f" | S3={r['s3_rel']:.3f}±{r['s3_sd']:.3f} cov={r['s3_cov']:.2f} ({r['s3_wins_base']}/5)",
            flush=True,
        )

with open(OUT / "r6_champion_gate.json", "w") as f:
    json.dump(gate_res, f, indent=2)

# 2. Full Gold Benchmark on Meta-Compact (S2+S3) AND Meta-Full (S3 diff!)
for tag, kw in [
    ("meta_compact", dict(scale_moment="meta", fit_jump_slope=True, fit_s3_rank_laws=False, filter_stat="median")),
    ("meta_full",    dict(scale_moment="meta", fit_jump_slope=True, fit_s3_rank_laws=True,  filter_stat="median")),
]:
    print(f"\n==================== GOLD BENCHMARK: {tag} ====================", flush=True)
    set_mode(**kw)
    gold = {"oracle_check": rgb.verify_coverage_oracle(), "panels": {}}
    for pn in PANELS:
        gold["panels"][pn] = rgb.run_panel(pn, rgb.PANELS[pn])
    with open(OUT / f"r6_{tag}_gold.json", "w") as f:
        json.dump(gold, f, indent=2)

# Print clean summary table
r4g = json.load(open(RUNS / "round4_baseline/candidate_frozen_gate_results.json"))
r5g = json.load(open(RUNS / "fable_v2/candidate_frozen_gate_results.json"))
print("\n================ FINAL ROUND 6 GATE v1 COMPARISON ================")
for pn in PANELS:
    print(f"\n[{pn}] base={r4g[pn]['base_rel']:.3f}")
    print(f"  R4(Astra)     S2={r4g[pn]['s2_rel']:.3f} ({r4g[pn]['s2_wins_base']}/5, {int(r4g[pn]['s2_cov']*100)}%) | S3={r4g[pn]['s3_rel']:.3f} ({r4g[pn]['s3_wins_base']}/5, {int(r4g[pn]['s3_cov']*100)}%)")
    print(f"  R5(Fable)     S2={r5g[pn]['s2_rel']:.3f} ({r5g[pn]['s2_wins_base']}/5, {int(r5g[pn]['s2_cov']*100)}%) | S3={r5g[pn]['s3_rel']:.3f} ({r5g[pn]['s3_wins_base']}/5, {int(r5g[pn]['s3_cov']*100)}%)")
    for tag in ("meta_compact", "meta_full"):
        r = gate_res[tag][pn]
        print(f"  R6-{tag:12s} S2={r['s2_rel']:.3f} ({r['s2_wins_base']}/5, {int(r['s2_cov']*100)}%) | S3={r['s3_rel']:.3f} ({r['s3_wins_base']}/5, {int(r['s3_cov']*100)}%)")
