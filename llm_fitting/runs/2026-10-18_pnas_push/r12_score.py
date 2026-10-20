"""Score R12 (PLAN.md addendum R12, amendments A1 and A2) from the saved outputs of r12_hetgrowth.py.

Reads r12_moments__*, r12_grid__*, r12_worlds__*, r12_oop__* and r12_recovery__*; writes r12_score.json and prints
the table.  Rules are the registered ones; where a rule needs an operational choice it is stated in `notes`.

    python llm_fitting/runs/2026-10-18_pnas_push/r12_score.py
"""
import json
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
PANELS = (("reddit_comments_ext", "comments"), ("reddit_submissions_long", "submissions"))
HOMOG_SHARE, OBS_SHARE = 0.537, 0.356          # A6: homogeneous forecast and observed 2022 share, submissions


def jl(name):
    f = HERE / name
    return json.loads(f.read_text()) if f.exists() else None


def profile(grid):
    """Objective profiled over the nuisance: min over m for each s (fixed) or over s for each half-life (decay)."""
    out = {}
    for r in grid["rows"]:
        key = r["s"] if grid["variant"] == "fixed" else r["half_life"]
        out[key] = min(out.get(key, np.inf), r["obj"])
    return {str(k): float(v) for k, v in sorted(out.items())}


def main():
    res, notes, table = {}, [], []
    def score(tag, label, ok, value, prior):
        verdict = "NOT RUN" if ok is None else ("PASS" if ok else "FAIL")
        table.append((tag, label, verdict, value, prior))
        res[f"{tag} {label}"] = dict(verdict=verdict, value=value, prior=prior)

    for pn, lab in PANELS:
        dm = jl(f"r12_moments__{pn}.json")
        gf, gd = jl(f"r12_grid__{pn}__fixed.json"), jl(f"r12_grid__{pn}__decay.json")
        W = {v: jl(f"r12_worlds__{pn}__{v}.json") for v in ("control", "control_num", "fixed", "decay", "factor")}
        O = {v: jl(f"r12_oop__{pn}__{v}.json") for v in ("control", "control_num", "fixed", "decay", "factor")}
        C = W["control_num"] or W["control"]             # A4: the matched control when it exists
        cname = "control_num" if W["control_num"] else "control (closed-form drift; matched control not yet run)"
        res[f"{lab} calibration"] = dict(fixed_best=gf and gf["best"], fixed_profile=gf and profile(gf),
                                         decay_best=gd and gd["best"], decay_profile=gd and profile(gd))
        # R12-1 / 1b: sigma_b > 0 at the optimum of (a), annual spread 0.1-0.5
        tag = "R12-1" if lab == "comments" else "R12-1b"
        if gf:
            s = gf["best"]["s"]
            score(tag, lab, s > 0 and 0.1 <= s <= 0.5, f"s = {s}, m = {gf['best']['m']}", "60%" if lab == "comments" else "50%")
        else:
            score(tag, lab, None, "-", "")
        # R12-2 as amended (A2): (a) reproduces the data's 52-week lag-profile value within 0.05 on comments, not on submissions
        d52 = dm["aux"]["gapped"]["52"]["rho"] if dm else np.nan
        if W["fixed"]:
            w52 = W["fixed"]["gapped"]["52"]["mean"]
            within = abs(w52 - d52) <= 0.05
            score("R12-2(a)", lab, within if lab == "comments" else not within,
                  f"world {w52:+.3f} [{W['fixed']['gapped']['52']['q05']:+.3f}, {W['fixed']['gapped']['52']['q95']:+.3f}] vs data {d52:+.3f}",
                  "50%" if lab == "comments" else "65% (expects a miss)")
        else:
            score("R12-2(a)", lab, None, "-", "")
        # R12-2 for (c), as written: gapped persistence >= +0.08 at a 13-week gap (null_worlds.stats definition)
        if W["factor"]:
            g13 = W["factor"]["gapped_stats"]["13"]
            score("R12-2(c)", lab, g13["mean"] >= 0.08, f"{g13['mean']:+.3f} [{g13['q05']:+.3f}, {g13['q95']:+.3f}]",
                  "25%" if lab == "comments" else "45%")
        else:
            score("R12-2(c)", lab, None, "-", "")
        # R12-3 / 3b: (c) on submissions, out of period
        if lab == "submissions":
            of = O["factor"]
            if of:
                score("R12-3", lab, of["share_3"] <= HOMOG_SHARE - 0.05, f"{of['share_3']:.3f} (homogeneous {HOMOG_SHARE})", "45%")
                lo, hi = of["share_3_q"]
                score("R12-3b", lab, lo <= OBS_SHARE <= hi, f"[{lo:.3f}, {hi:.3f}] vs observed {OBS_SHARE}", "20%")
                for cv in ("control", "control_num"):
                    if O[cv]:
                        d = np.array(of["shares"]) - np.array(O[cv]["shares"])
                        res[f"R12-3 matched {cv}"] = dict(control_share=O[cv]["share_3"], paired_diff=float(d.mean()),
                                                          paired_q=[float(np.quantile(d, 0.05)), float(np.quantile(d, 0.95))])
                        table.append(("R12-3 vs", cv, "info", f"factor - {cv} = {d.mean():+.3f} [{np.quantile(d, 0.05):+.3f}, {np.quantile(d, 0.95):+.3f}] (control {O[cv]['share_3']:.3f})", "A4"))
            else:
                score("R12-3", lab, None, "-", "")
                score("R12-3b", lab, None, "-", "")
        # R12-4: in calibrated (a) and (b) on comments, prior decliners slide faster (R10 contrast > 0)
        if lab == "comments":
            if W["fixed"] and W["decay"]:
                cf, cd = W["fixed"]["r10_contrast"]["mean"], W["decay"]["r10_contrast"]["mean"]
                cc = W["control"]["r10_contrast"]["mean"] if W["control"] else np.nan
                cn = W["control_num"]["r10_contrast"]["mean"] if W["control_num"] else np.nan
                score("R12-4", lab, cf > 0 and cd > 0, f"fixed {cf:+.4f}, decay {cd:+.4f} (control_num {cn:+.4f}, control {cc:+.4f}; data +0.0023)", "60%")
            else:
                score("R12-4", lab, None, "-", "")
        # R12-6: must not worsen against the control
        if C:
            tc = C["turnover"]
            ks = sorted(int(k) for k in tc)
            K = max(ks) * 2
            k64, k2 = str(K // 64), str(K // 2)
            for v in ("fixed", "decay", "factor"):
                if not W[v]:
                    score("R12-6", f"{lab} {v}", None, "-", "")
                    continue
                tv = W[v]["turnover"]
                d64, d2 = tv[k64]["mean"] - tc[k64]["mean"], tv[k2]["mean"] - tc[k2]["mean"]
                b = W[v]["boundary"]["mean"]
                ok = abs(d64) <= 0.03 and abs(d2) <= 0.03 and 0.90 <= b <= 1.15
                score("R12-6", f"{lab} {v}", ok, f"vs {cname}: turnover K/64 {d64:+.3f}, K/2 {d2:+.3f}; boundary {b:.3f}", "70%")
        # R12-7 (A1): calibrated (a) and (b) give a faller-riser asymmetry below half the data's
        if dm:
            da = dm["aux"]["asym"]["diff"]
            for v in ("fixed", "decay"):
                if W[v]:
                    wa = W[v]["asym"]["mean"]
                    score("R12-7", f"{lab} {v}", wa < 0.5 * da, f"world {wa:+.3f} vs data {da:+.3f}", "70%")
                else:
                    score("R12-7", f"{lab} {v}", None, "-", "")
        # R12-5: recovery under (a) at the planted spread nearest the calibrated one
        rec = {s: jl(f"r12_recovery__{pn}__s{s}.json") for s in ("0.0", "0.15", "0.3", "0.45")}
        res[f"{lab} recovery"] = {}
        base0 = rec["0.0"]
        for s, r in rec.items():
            if not r:
                continue
            sb = np.array([row["s_band"] for row in r["rows"]])
            be = np.array([row["beta"] for row in r["rows"]])
            s0 = np.array([row["sigma0"] for row in r["rows"]])
            ent = dict(n=len(r["rows"]), true_beta=r["true_beta"], beta_mean=float(be.mean()), beta_se=float(be.std(ddof=1) / np.sqrt(be.size)),
                       sigma0_mean=float(s0.mean()), true_sigma0=r["true_sigma0"], head_lasting_mean=float(sb[:, 0].mean()))
            if base0:
                sb0 = np.array([row["s_band"] for row in base0["rows"]])
                be0 = np.array([row["beta"] for row in base0["rows"]])
                ent["head_inflation_vs_s0"] = float(sb[:, 0].mean() / sb0[:, 0].mean() - 1)
                ent["beta_shift_vs_s0"] = float(be.mean() - be0.mean())
            ent["beta_bias_vs_truth"] = float(be.mean() - r["true_beta"])
            res[f"{lab} recovery"][s] = ent
        if gf and base0:
            sc = gf["best"]["s"]
            near = min((s for s in rec if rec[s]), key=lambda s: abs(float(s) - sc))
            e = res[f"{lab} recovery"][near]
            if float(near) == 0.0:
                score("R12-5", lab, False, f"calibrated s = {sc}: nothing planted at the nearest grid value", "50%")
            else:
                ok = e["head_inflation_vs_s0"] > 0.10 and abs(e["beta_shift_vs_s0"]) > 0.02
                score("R12-5", lab, ok, f"planted s {near} (calibrated {sc}): head lasting variance {e['head_inflation_vs_s0']:+.1%}, "
                      f"beta {e['beta_shift_vs_s0']:+.3f} vs s = 0 (vs truth {e['beta_bias_vs_truth']:+.3f})", "50%")
        else:
            score("R12-5", lab, None, "-", "")
        res[f"{lab} oop"] = {v: (o and dict(share_3=o["share_3"], q=o["share_3_q"])) for v, o in O.items()}
        res[f"{lab} data"] = dm and dict(gapped_rho={g: dm["aux"]["gapped"][g]["rho"] for g in dm["aux"]["gapped"]}, asym=dm["aux"]["asym"])

    notes += [
        "R12-5 compares each planted world with the planted-0 world on the same seeds (oracle passing parts), so the "
        "estimator's own bias without heterogeneity is differenced out; the bias against the truth is shown beside it.",
        "R12-5 uses the planted spread nearest the calibrated one (grid 0, 0.15, 0.30, 0.45).",
        "R12-2(a) uses the pooled Spearman lag profile of r12_hetgrowth (gaps 0-52); R12-2(c) the null_worlds.stats gapped "
        "persistence at 13 weeks, as registered.",
        "R12-6 compares each variant with the matched control (control_num, A4) on the same seeds; the boundary ratio range is absolute.",
        "R12-3 is scored as registered against 0.537 (A6, closed-form drift); the paired difference from each control on the same seeds is shown beside it (A4).",
    ]
    res["notes"] = notes
    (HERE / "r12_score.json").write_text(json.dumps(res, indent=1, default=float))
    w = max(len(t[1]) for t in table)
    for tag, label, verdict, value, prior in table:
        print(f"{tag:9s} {label:{w}s} {verdict:8s} {value}   (prior {prior})")


if __name__ == "__main__":
    main()
