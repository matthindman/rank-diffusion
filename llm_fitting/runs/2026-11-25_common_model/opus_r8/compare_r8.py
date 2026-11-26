#!/usr/bin/env python3
"""Round 8 side-by-side comparison of a candidate benchmark (runs/<tag>/) against Round 6 and earlier rounds.

Comparators (as specified in OPUS_R8_BRIEF.md section 3):
  Gate  S2: baseline_r6/r6_s2_frozen_gate.json (s2 arm)       S3: baseline_r6/r6_s3_frozen_gate.json (s3 arm)
  Gold  S2: baseline_r6/r6_s2_gold.json (s2 fields)           S3: ../astra_v3/candidate_gold_benchmark_results.json (s3 fields)
  Gate history: R4 ../round4_baseline, R5 ../fable_v2, R7 ../astra_v3 (s2 arm).
Usage: python compare_r8.py <tag> [> runs/<tag>/comparison.md]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
PANELS = ["facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"]
SHORT = {"facebook_a": "FB", "instagram_hm": "IG", "reddit_comments_ext": "Comments", "reddit_submissions_long": "Submissions"}


def load(p):
    return json.loads((HERE / p).read_text())


def gate_section(tag):
    c = load(f"runs/{tag}/candidate_frozen_gate_results.json")
    r6s2 = load("baseline_r6/r6_s2_frozen_gate.json")
    r6s3 = load("baseline_r6/r6_s3_frozen_gate.json")
    hist = {"R4": load("../round4_baseline/candidate_frozen_gate_results.json"),
            "R5": load("../fable_v2/candidate_frozen_gate_results.json"),
            "R7": load("../astra_v3/candidate_frozen_gate_results.json")}
    out = ["## Gate v1 (5 x 20, boot 2000; mean relative error, lower is better)", "",
           "| Panel | Persist. | R4 S2 | R5 S2 | R7 S2 | **R6 S2** | **R8 S2** | dS2 (paired SD, wins) | **R6 S3** | **R8 S3** | dS3 (paired SD, wins) |",
           "|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---|"]
    rows = {}
    for pn in PANELS:
        cv, a2, a3 = c[pn], r6s2[pn], r6s3[pn]
        d2 = np.array([s["s2_rel"] for s in cv["splits"]]) - np.array([s["s2_rel"] for s in a2["splits"]])
        d3 = np.array([s["s3_rel"] for s in cv["splits"]]) - np.array([s["s3_rel"] for s in a3["splits"]])
        rows[pn] = dict(d2=d2.tolist(), d3=d3.tolist())
        out.append(
            f"| {SHORT[pn]} | {cv['base_rel']:.3f} | {hist['R4'][pn]['s2_rel']:.3f} | {hist['R5'][pn]['s2_rel']:.3f} | "
            f"{hist['R7'][pn]['s2_rel']:.3f} | {a2['s2_rel']:.3f} ± {a2['s2_sd']:.3f} | {cv['s2_rel']:.3f} ± {cv['s2_sd']:.3f} "
            f"({cv['s2_cov']*100:.0f}%, {cv['s2_wins_base']}/5) | {d2.mean():+.3f} ± {d2.std():.3f} ({int((d2 < 0).sum())}↓/{int((d2 > 0).sum())}↑) | "
            f"{a3['s3_rel']:.3f} ± {a3['s3_sd']:.3f} | {cv['s3_rel']:.3f} ± {cv['s3_sd']:.3f} ({cv['s3_cov']*100:.0f}%, {cv['s3_wins_base']}/5) | "
            f"{d3.mean():+.3f} ± {d3.std():.3f} ({int((d3 < 0).sum())}↓/{int((d3 > 0).sum())}↑) |")
    out += ["", "Per-origin moment errors (signed: + = model above data) for the Reddit panels:", ""]
    for pn in PANELS[2:]:
        for arm, ref, key in (("S2", r6s2, "s2"), ("S3", r6s3, "s3")):
            out.append(f"**{SHORT[pn]} {arm}**")
            out.append("")
            out.append("| T0 | R6 total | R8 total | R6 moments | R8 moments |")
            out.append("|---:|---:|---:|---|---|")
            for sc, sr in zip(c[pn]["splits"], ref[pn]["splits"]):
                def fmt(s):
                    em, sm, ev = s["em"], s["sm2" if key == "s2" else "sm3"], s[f"err_vec_{key}"]
                    return " ".join(f"{k}:{np.sign(sm[k]-em[k])*v:+.2f}" for k, v in ev.items())
                out.append(f"| {sc['T0']} | {sr[f'{key}_rel']:.3f} | {sc[f'{key}_rel']:.3f} | {fmt(sr)} | {fmt(sc)} |")
            out.append("")
    return out, rows


def gold_section(tag):
    c = load(f"runs/{tag}/candidate_gold_benchmark_results.json")["panels"]
    r2 = load("baseline_r6/r6_s2_gold.json")["panels"]
    r3 = load("../astra_v3/candidate_gold_benchmark_results.json")["panels"]
    out = ["## Gold Goal 1 (20 seeds; median [q05, q95]; empirical with entity-cluster 90% CI where available)", ""]

    def g(x, path):
        for p_ in path:
            x = x[p_]
        return x

    for pn in PANELS:
        K = c[pn]["K"]
        e, ci = c[pn]["emp"], c[pn]["emp_cluster_ci"]
        out += [f"### {SHORT[pn]} (S2 scalars R6 {r2[pn]['params_s2']['n_scalars']} → R8 {c[pn]['params_s2']['n_scalars']}; "
                f"S3 R6 {r3[pn]['params_s3']['n_scalars']} → R8 {c[pn]['params_s3']['n_scalars']})", "",
                "| Statistic | Data | R6 S2 | R8 S2 | R6 S3 | R8 S3 |", "|---|---:|---:|---:|---:|---:|"]
        for lbl, path in [("Top-10 turnover", ["turnover", "10"]), ("K/4 turnover", ["turnover", str(K // 4)]),
                          ("Rank-1 change", ["occ_change", "1"]), ("Head share (K/64)", ["head_share"]),
                          ("Boundary ratio", ["boundary_ratio"]), ("Departure rate", ["dep_rate"]),
                          ("Immediate-crossing share", ["dep_imm_cross_frac"]), ("Gapped rho", ["gapped_rho"]),
                          ("Asymmetry D", ["D"]), ("p_fall", ["p_fall"]), ("p_rise", ["p_rise"]),
                          ("Decline runs r2", ["r2"]), ("Decline runs r3", ["r3"])]:
            ev = g(e, path)
            cis = ""
            if path[-1] in ci and ci[path[-1]]:
                cis = f" [{ci[path[-1]]['q05']:+.3f}, {ci[path[-1]]['q95']:+.3f}]"
            cells = []
            for src, arm in ((r2, "g1_s2"), (c, "g1_s2"), (r3, "g1_s3"), (c, "g1_s3")):
                v = g(src[pn][arm], path)
                cells.append(f"{v['median']:+.3f} [{v['q05']:+.3f}, {v['q95']:+.3f}]")
            out.append(f"| {lbl} | {ev:+.3f}{cis} | " + " | ".join(cells) + " |")
        hook = lambda src, arm: " ".join(f"{x['median']:+.3f}" for x in src[pn][arm]["dec_y"])
        out += ["", f"10-decile hook E[y | x decile]: data `{' '.join(f'{v:+.3f}' for v in e['dec_y'])}`",
                f"- R6 S2 `{hook(r2, 'g1_s2')}`", f"- R8 S2 `{hook(c, 'g1_s2')}`",
                f"- R6 S3 `{hook(r3, 'g1_s3')}`", f"- R8 S3 `{hook(c, 'g1_s3')}`", ""]
        hb = lambda src, arm: " ".join(f"{x['median']:.3f}" for x in src[pn][arm]["head_share_blocks"])
        out += [f"Head share by block: data `{' '.join(f'{v:.3f}' for v in e['head_share_blocks'])}` · R6 S2 `{hb(r2, 'g1_s2')}` · "
                f"R8 S2 `{hb(c, 'g1_s2')}` · R6 S3 `{hb(r3, 'g1_s3')}` · R8 S3 `{hb(c, 'g1_s3')}`", ""]

    out += ["## Gold Goal 2 (5 origins x M = 39; exact [x_(2), x_(38)] cov90, reference 0.900, PIT SD 0.289)", "",
            "| Panel | h | X CRPS R6/R8 S2 | X CRPS R6/R8 S3 | Kal MAE S2 R6/R8 (EWMA) | Kal MAE S3 R6/R8 | cov90 S2 R6/R8 | cov90 S3 R6/R8 | PIT SD S2 R6/R8 | PIT SD S3 R6/R8 | logR CRPS S2 R6/R8 | logR CRPS S3 R6/R8 | head share MAE S2 R6/R8 (corr) | head share MAE S3 R6/R8 (corr) | fair head Brier S2 R6/R8 | S3 R6/R8 |",
            "|---|---:|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for pn in PANELS:
        for h, cr_ in c[pn]["goal2"]["by_horizon"].items():
            a2 = r2[pn]["goal2"]["by_horizon"][h]
            a3 = r3[pn]["goal2"]["by_horizon"][h]
            out.append(
                f"| {SHORT[pn]} | {h} | {a2['crps_X_s2']:.4f} / {cr_['crps_X_s2']:.4f} | {a3['crps_X_s3']:.4f} / {cr_['crps_X_s3']:.4f} | "
                f"{a2['mae_kal_s2']:.4f} / {cr_['mae_kal_s2']:.4f} ({cr_['mae_ll']:.4f}) | {a3['mae_kal_s3']:.4f} / {cr_['mae_kal_s3']:.4f} | "
                f"{a2['cov90_s2']:.3f} / {cr_['cov90_s2']:.3f} | {a3['cov90_s3']:.3f} / {cr_['cov90_s3']:.3f} | "
                f"{a2['pit_sd_s2']:.3f} / {cr_['pit_sd_s2']:.3f} | {a3['pit_sd_s3']:.3f} / {cr_['pit_sd_s3']:.3f} | "
                f"{a2['crps_lr_s2']:.4f} / {cr_['crps_lr_s2']:.4f} | {a3['crps_lr_s3']:.4f} / {cr_['crps_lr_s3']:.4f} | "
                f"{a2['share_mae_hd_s2']*1e4:.3f} / {cr_['share_mae_hd_s2']*1e4:.3f} ({a2['share_corr_hd_s2']:+.2f}/{cr_['share_corr_hd_s2']:+.2f}) | "
                f"{a3['share_mae_hd_s3']*1e4:.3f} / {cr_['share_mae_hd_s3']*1e4:.3f} ({a3['share_corr_hd_s3']:+.2f}/{cr_['share_corr_hd_s3']:+.2f}) | "
                f"{a2['brier_hd_s2']:.4f} / {cr_['brier_hd_s2']:.4f} | {a3['brier_hd_s3']:.4f} / {cr_['brier_hd_s3']:.4f} |")
    out += ["", "Head share MAE is ×10⁻⁴.", "", "## Out-of-period structure (fit 2019–20, simulate 2021–22, 20 seeds)", ""]
    for pn in PANELS[2:]:
        oc, o2, o3 = c[pn]["oop_2019_20_to_2021_22"], r2[pn]["oop_2019_20_to_2021_22"], r3[pn]["oop_2019_20_to_2021_22"]
        K = c[pn]["K"]
        out += [f"**{SHORT[pn]}**", "", "| Statistic | 2019–20 | 2021–22 | R6 S2 | R8 S2 | R6 S3 | R8 S3 |", "|---|---:|---:|---:|---:|---:|---:|"]
        for lbl, path in [("Head share", ["head_share"]), ("Top-10 turnover", ["turnover", "10"]), ("K/4 turnover", ["turnover", str(K // 4)]),
                          ("Ladder span rank 1", ["lad_last", "1"]), ("Ladder span K/64", ["lad_last", str(K // 64)]),
                          ("Boundary ratio", ["boundary_ratio"]), ("Departure rate", ["dep_rate"]), ("Immediate-crossing share", ["dep_imm_cross_frac"]),
                          ("Gapped rho", ["gapped_rho"]), ("Asymmetry D", ["D"])]:
            try:
                e19, e21 = g(oc["emp_2019_20"], path), g(oc["emp_2021_22"], path)
                vals = [g(o2["S2"]["sim"], path)["median"], g(oc["S2"]["sim"], path)["median"],
                        g(o3["S3"]["sim"], path)["median"], g(oc["S3"]["sim"], path)["median"]]
                out.append(f"| {lbl} | {e19:+.3f} | {e21:+.3f} | " + " | ".join(f"{v:+.3f}" for v in vals) + " |")
            except (KeyError, TypeError):
                continue
        out.append("")
    return out


def params_section(tag):
    c = load(f"runs/{tag}/candidate_gold_benchmark_results.json")["panels"]
    out = ["## Full-panel fitted parameters (R8)", "", "| Panel | Arm | scalars | sigma_L0 | beta | phi | b_xi | s_vol | surge (lam, mu, hl) | sigma_d(K) |",
           "|---|---|---:|---:|---:|---:|---:|---:|---|---:|"]
    for pn in PANELS:
        for arm in ("s2", "s3"):
            p = c[pn][f"params_{arm}"]
            out.append(f"| {SHORT[pn]} | {arm.upper()} | {p['n_scalars']} | {p['sigma_L0']:.4f} | {p['beta']:+.3f} | {p['phi']:.2f} | {p['b_xi']:+.3f} | "
                       f"{p['s_vol']:.3f} | ({p['surge_lam']:.4f}, {p['surge_mu']:.2f}, {p['surge_hl']:.0f}) | {p['sigma_d']:.4f} |")
    return out + [""]


def main():
    tag = sys.argv[1]
    lines = [f"# Round 8 comparison — `{tag}`", ""]
    gp = HERE / f"runs/{tag}/candidate_frozen_gate_results.json"
    if gp.exists():
        g_lines, _ = gate_section(tag)
        lines += g_lines + [""]
    if (HERE / f"runs/{tag}/candidate_gold_benchmark_results.json").exists():
        lines += params_section(tag) + gold_section(tag)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
