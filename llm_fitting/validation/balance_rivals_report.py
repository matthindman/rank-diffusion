"""V4 recovery map, confusion, slope error and explicitly scoped coverage."""
from __future__ import annotations

import argparse
from collections import defaultdict
import gzip
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/"llm_fitting"))
from validation import balance_rivals_sim as sim
from validation import balance_rivals_study as st


def wilson(k, n):
    if n == 0:
        return dict(k=k, n=n, rate=None, mcse=None, lo=None, hi=None)
    p, z = k/n, 1.959963984540054
    den = 1+z*z/n
    centre = (p+z*z/(2*n))/den
    half = z*np.sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return dict(k=k, n=n, rate=p, mcse=np.sqrt(p*(1-p)/n), lo=centre-half, hi=centre+half)


def leaves(x, prefix=""):
    """Retain full paths, so boundaries, horizon, k and cohort never pool."""
    if isinstance(x, dict):
        for k, v in x.items():
            yield from leaves(v, prefix+"/"+str(k))
    elif isinstance(x, list):
        for j, v in enumerate(x):
            yield from leaves(v, prefix+"/"+str(j))
    elif isinstance(x, (int, float)) and not isinstance(x, bool) and np.isfinite(x):
        yield prefix, float(x)


def classify(rows):
    """Hold out paired seeds jointly across every rival; no target leakage."""
    features = sorted({k for r in rows if r["status"] == "RAN-OK" for k in r["features"]})
    X = np.array([[r.get("features", {}).get(k, np.nan) for k in features] for r in rows], float)
    out = []
    for i, row in enumerate(rows):
        pred = "FAILED"
        if row["status"] == "RAN-OK":
            seed = row["provenance"]["seed"]
            train = np.array([r["status"] == "RAN-OK" and r["provenance"]["seed"] != seed for r in rows])
            A = X[train]
            if len(A):
                finite = np.isfinite(A)
                count = finite.sum(0)
                mu = np.nansum(A, axis=0)/np.maximum(count, 1)
                sd = np.sqrt(np.nansum((A-mu)**2, axis=0)/np.maximum(count-1, 1))
                keep = (count >= 2) & (sd > 1e-12)
                if keep.any():
                    def transform(B):
                        return np.column_stack([np.where(np.isfinite(B[:, keep]),
                                    (B[:, keep]-mu[keep])/sd[keep], 0.), (~np.isfinite(B[:, keep])).astype(float)])
                    Z, query = transform(A), transform(X[i:i+1])[0]
                    labels = np.array([r["provenance"]["rival"] for j, r in enumerate(rows) if train[j]])
                    distances = [float(np.sum((Z[labels == rv].mean(0)-query)**2)) if (labels == rv).any() else np.inf
                                 for rv in sim.RIVALS]
                    pred = sim.RIVALS[int(np.argmin(distances))]
                else:
                    pred = "UNRESOLVED"
        truth = row["provenance"]["rival"]
        out.append(dict(panel=row["provenance"]["panel"], seed=row["provenance"]["seed"], rival=truth,
                        truth_class=sim.CLASSES[truth], prediction=pred, predicted_class=sim.CLASSES.get(pred, pred),
                        correct=sim.CLASSES.get(pred) == sim.CLASSES[truth],
                        missing_fraction=float(np.mean(~np.isfinite(X[i]))) if X.shape[1] else 1.))
    return out


def coverage_target(calibration, pred, dc, R):
    """Independent bin-population target, joint seed-jackknife MC error."""
    weights = np.array(pred["bin_counts"], float)
    use = np.flatnonzero(weights > 0)
    if not len(use) or R <= 0 or len(calibration) != 20:
        return None
    total = np.array([[c.get("absolute_bin_moments", {}).get(str(j), {}).get("total", 0.)
                       for j in use] for c in calibration])
    count = np.array([[c.get("absolute_bin_moments", {}).get(str(j), {}).get("n", 0)
                       for j in use] for c in calibration], float)
    sums, ns = total.sum(0), count.sum(0)
    if np.any(ns <= 0) or np.any(ns-count <= 0):
        return None
    point = -float(weights[use] @ (sums/ns-dc))/R
    jack = -(((sums-total)/(ns-count)-dc) @ weights[use])/R
    se = float(np.sqrt((len(jack)-1)/len(jack)*np.sum((jack-jack.mean())**2)))
    lo, hi = pred["G_pred_q05"]/R, pred["G_pred_q95"]/R
    tlo, thi = point-1.96*se, point+1.96*se
    return dict(target=point, target_mcse=se, target_lo=tlo, target_hi=thi,
                lo=lo, hi=hi, covered=bool(lo <= point <= hi),
                definitely_covered=bool(lo <= tlo and thi <= hi),
                possibly_covered=bool(thi >= lo and tlo <= hi),
                precise=bool(thi-tlo <= .1*(hi-lo)))


def collect(out, allow_partial=False):
    rows, absent = [], []
    hashes = st.source_hashes()
    for panel in sim.PANELS:
        inp_hash = st.digest(out/"inputs"/f"{panel}.json")
        absence_hash = st.digest(out/"inputs"/f"{panel}_absence.json")
        for rival in sim.RIVALS:
            for stage, seeds in (("evaluation", range(1000, 1020)), ("calibration", range(2000, 2020))):
                for seed in seeds:
                    f = out/"cells"/panel/rival/f"{stage}_{seed}.json"
                    if not f.exists():
                        absent.append(str(f.relative_to(out)))
                        continue
                    row = st.read(f)
                    p = row["provenance"]
                    if p["sources"] != hashes or p["input_sha256"] != inp_hash:
                        raise RuntimeError(f"Source/input mismatch: {f}")
                    if p.get("absence_sha256") != absence_hash:
                        raise RuntimeError(f"Absence input mismatch: {f}")
                    if (p["panel"], p["rival"], p["seed"], p["stage"]) != (panel, rival, seed, stage):
                        raise RuntimeError(f"Cell identity mismatch: {f}")
                    if p["config"] != sim.PANELS[panel]:
                        raise RuntimeError(f"Wrong panel configuration: {f}")
                    if row["status"] not in ("RAN-OK", "FAILED"):
                        raise RuntimeError(f"Nonterminal cell: {f}")
                    if row["status"] == "RAN-OK":
                        expected_n = sim.PANELS[panel]["K"]*(16 if rival == "replacement" else 8)
                        if row.get("simulation_population") != expected_n:
                            raise RuntimeError(f"Wrong population size: {f}")
                        if stage == "evaluation" and any(row["analysis"][k] != sim.PANELS[panel][k]
                                                          for k in ("K", "T", "block")):
                            raise RuntimeError(f"Wrong analysis dimensions: {f}")
                    rows.append(row)
    if absent and not allow_partial:
        raise RuntimeError(f"Grid incomplete: {len(absent)}/1200 cells missing; no completed report written")
    return rows, absent


def summarize(out, allow_partial=False):
    missing_inputs = [str(out/"inputs"/f"{p}{suffix}.json") for p in sim.PANELS
                      for suffix in ("", "_absence") if not (out/"inputs"/f"{p}{suffix}.json").exists()]
    if missing_inputs and allow_partial:
        st.write(out/"progress.json", dict(status="IN PROGRESS", expected=1200,
                                          pending_inputs=missing_inputs, note="Native input preparation not complete."))
        return
    rows, absent = collect(out, allow_partial)
    evaluations = [r for r in rows if r["provenance"]["stage"] == "evaluation"]
    calibration = [r for r in rows if r["provenance"]["stage"] == "calibration"]
    if absent:
        st.write(out/"progress.json", dict(status="IN PROGRESS", completed=len(rows), expected=1200, missing=absent,
                                          failed=[r["provenance"] for r in rows if r["status"] == "FAILED"],
                                          note="No partial scientific aggregate or completed recovery map."))
        return
    classifications = []
    for panel in sim.PANELS:
        classifications += classify([r for r in evaluations if r["provenance"]["panel"] == panel])
    rates, confusion, scores, slopes, coverage, numeric = [], [], [], [], [], defaultdict(list)
    for panel in sim.PANELS:
        for rival in sim.RIVALS:
            cls = [r for r in classifications if r["panel"] == panel and r["rival"] == rival]
            rates.append(dict(panel=panel, rival=rival, metric="class recovery", **wilson(sum(r["correct"] for r in cls), 20)))
            rates.append(dict(panel=panel, rival=rival, metric="named-level calls", **wilson(sum(r["predicted_class"] == "named stationary level" for r in cls), 20)))
            for predicted in (*sim.RIVALS, "FAILED", "UNRESOLVED"):
                confusion.append(dict(panel=panel, rival=rival, prediction=predicted,
                                      **wilson(sum(r["prediction"] == predicted for r in cls), 20)))
            bank = [r["oracle"] for r in calibration if r["status"] == "RAN-OK" and
                    r["provenance"]["panel"] == panel and r["provenance"]["rival"] == rival]
            group = [r for r in evaluations if r["provenance"]["panel"] == panel and r["provenance"]["rival"] == rival]
            recovered = 0
            screen_success = []
            for row in group:
                tag = dict(panel=panel, rival=rival, seed=row["provenance"]["seed"])
                if row["status"] != "RAN-OK":
                    for name in ("A1", "A2", "A2b", "A3", "A4", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B9", "B10", "screen"):
                        scores.append(dict(**tag, prediction=name, status="NOT ADJUDICATED", reason="RUN FAILED"))
                    continue
                for family in ("analysis", "features", "oracle"):
                    for key, value in leaves(row[family], family):
                        numeric[(panel, rival, key)].append(value)
                for name, score in row["scores"].items():
                    if name != "_slide_no_gap":
                        scores.append(dict(**tag, prediction=name, status=score["status"]))
                success = len(row["heldout"]) == 5 and all(r["success"] for r in row["heldout"].values())
                recovered += success
                if row["scores"]["screen"]["status"] == "PASS":
                    screen_success.append(success)
                for k, r in row["heldout"].items():
                    slopes.append(dict(**tag, k=int(k), screen=row["scores"]["screen"]["status"], **r))
                cell = row["analysis"]["boundaries"]["1.0"]
                p = st.m1._pair(cell, "primary")
                if p and "not_adjudicated" not in p:
                    car = p.get("carriers", {})
                    for name, flag in (("negative position association", car.get("pos_q95", np.inf) < 0),
                                       ("joint restoring association", car.get("pos_q95", np.inf) < 0 and car.get("home_q05", -np.inf) > 0),
                                       ("bin differences B3 FAIL", row["scores"]["B3"]["status"] == "FAIL")):
                        numeric[(panel, rival, "flags/"+name)].append(float(flag))
                    for a, pred in zip(p["accounting"], p["predictable"]):
                        if pred.get("G_pred") is None or a.get("R", 0) <= 0:
                            continue
                        target = coverage_target(bank, pred, row["oracle"]["centre_change"], a["R"])
                        if target:
                            coverage.append(dict(**tag, k=a["k"], **target))
            rates.append(dict(panel=panel, rival=rival, metric="all five held-out slopes within 25%", **wilson(recovered, 20)))
            rates.append(dict(panel=panel, rival=rival, metric="held-out slope success conditional on screen PASS",
                              **wilson(sum(screen_success), len(screen_success))))
    # B8 is cross-panel and follows the existing M1 report rule, for each paired seed.
    lookup = {(r["provenance"]["panel"], r["provenance"]["rival"], r["provenance"]["seed"]): r for r in evaluations}
    for rival in sim.RIVALS:
        for seed in range(1000, 1020):
            vals = [lookup[(p, rival, seed)].get("scores", {}).get("_slide_no_gap") for p in sim.PANELS]
            ratio = None
            if all(v is not None for v in vals) and vals[0] != 0 and min(vals[1:]) > 0:
                ratio = vals[0]/float(np.mean(vals[1:]))
            scores.append(dict(panel="cross-panel", rival=rival, seed=seed, prediction="B8",
                               status="NOT ADJUDICATED" if ratio is None else "PASS" if 1.3 <= ratio <= 3 else "FAIL"))
    summaries = []
    for (panel, rival, statistic), values in numeric.items():
        x = np.array(values, float)
        summaries.append(dict(panel=panel, rival=rival, statistic=statistic, n=len(x), mean=x.mean(),
                              sd=x.std(ddof=1) if len(x) > 1 else None,
                              q05=np.quantile(x, .05), q95=np.quantile(x, .95),
                              mcse=x.std(ddof=1)/np.sqrt(len(x)) if len(x) > 1 else None))
    tables = dict(classifications=classifications, rates=rates, confusion=confusion, scores=scores,
                  heldout_slopes=slopes, coverage=coverage, numeric_map=summaries)
    matching, coverage_rates, predictions = adjudicate(evaluations, rates, coverage)
    tables.update(marginal_matching=matching, coverage_rates=coverage_rates)
    st.write(out/"predictions.json", predictions)
    for name, vals in tables.items():
        pd.DataFrame(vals).to_csv(out/(name+".csv.gz"), index=False, compression={"method": "gzip", "mtime": 0})
    score_df = pd.DataFrame(scores)
    score_counts = score_df.groupby(["panel", "rival", "prediction", "status"]).size().reset_index(name="count")
    score_counts.to_csv(out/"score_counts.csv", index=False)
    # Exact completed seed evidence can be restored without regenerating panels.
    with gzip.GzipFile(filename=str(out/"seed_evidence.jsonl.gz"), mode="wb", mtime=0) as f:
        for row in rows:
            f.write((json.dumps(st.clean(row), sort_keys=True, separators=(",", ":"))+"\n").encode())
    failed = [r["provenance"] for r in rows if r["status"] == "FAILED"]
    st.write(out/"audit.json", dict(status="COMPLETE WITH FAILURES" if failed else "RAN-OK", expected=1200,
                                   terminal=len(rows), failed=failed, sources=st.source_hashes(),
                                   tables={name: st.digest(out/(name+".csv.gz")) for name in tables},
                                   scope="Exploratory; synthetic; no empirical M1 results read."))
    lines = ["# V4 recovery map", "", "All 1,200 planned cells have terminal status. "
             f"{len(failed)} failed cells remain in the denominators.", "",
             "Rates have Wilson 95% intervals. These do not account for overlapping classifier training sets.", "",
             "| profile | rival | class recovery | held-out slope success |", "|---|---|---|---|"]
    def fmt(r):
        return f"{r['k']}/{r['n']} ({r['lo']:.2f}–{r['hi']:.2f})"
    for panel in sim.PANELS:
        for rv in sim.RIVALS:
            rr = [r for r in rates if r["panel"] == panel and r["rival"] == rv]
            lines.append(f"| {panel} | {rv} | {fmt(rr[0])} | {fmt(rr[2])} |")
    lines += ["", "This machine-written table is not the final interpretation. The handoff REPORT must score P1–P8, "
              "check marginal matching and coverage precision, and state limitations before claiming completion.",
              "The complete per-cell statistics, scores, conditional coverage targets and signed slope errors are in the companion tables."]
    (out/"RECOVERY_MAP.md").write_text("\n".join(lines)+"\n")


def adjudicate(evaluations, rates, coverage):
    """The fixed predictions, with failures/eligibility retained explicitly."""
    matching, coverage_rates = [], []
    groups = {(p, rv): [r for r in evaluations if r["provenance"]["panel"] == p and
                        r["provenance"]["rival"] == rv] for p in sim.PANELS for rv in sim.RIVALS}
    def summary(group):
        valid = [r["oracle"] for r in group if r["status"] == "RAN-OK"]
        if not valid:
            return {}, np.nan
        keys = sorted({k for r in valid for k in r["block_slopes"]}, key=int)
        S = {k: np.median([r["block_slopes"][k] for r in valid if r["block_slopes"].get(k) is not None]) for k in keys}
        v = [r["latent_variance"] for r in valid if r["latent_variance"] is not None]
        return S, np.median(v) if v else np.nan
    for panel in sim.PANELS:
        sums = {rv: summary(groups[(panel, rv)]) for rv in sim.RIVALS}
        for i, a in enumerate(sim.RIVALS):
            for b in sim.RIVALS[i+1:]:
                sa, va = sums[a]
                sb, vb = sums[b]
                ratios = [sb[k]/v for k, v in sa.items() if v > 0 and k in sb and np.isfinite(sb[k])]
                vr = vb/va if va > 0 else np.nan
                flag = ("NEGATIVE CONTROL" if "fixed_static" in (a, b) else
                        "NOT ADJUDICATED" if len(ratios) != 5 or not np.isfinite(vr) else
                        "MATCHED" if all(2/3 <= q <= 1.5 for q in ratios+[vr]) else "MARGINAL MISMATCH")
                matching.append(dict(panel=panel, rival_a=a, rival_b=b, status=flag,
                                      min_slope_ratio=min(ratios) if ratios else None,
                                      max_slope_ratio=max(ratios) if ratios else None, variance_ratio=vr))
    for panel, rv in groups:
        cells = [r for r in coverage if r["panel"] == panel and r["rival"] == rv]
        for k in sorted(set(r["k"] for r in cells)):
            block = [r for r in cells if r["k"] == k]
            for key in ("covered", "definitely_covered", "possibly_covered", "precise"):
                coverage_rates.append(dict(panel=panel, rival=rv, k_boundary=k, metric=key,
                                            eligible=len(block), missing=20-len(block),
                                            **wilson(sum(r[key] for r in block), len(block))))
    valid = [r for r in evaluations if r["status"] == "RAN-OK"]
    failed = len(evaluations)-len(valid)
    predictions = {}
    def put(name, outcome, **detail):
        predictions[name] = dict(status="PASS" if outcome else "FAIL", detail=detail)
    inv = [r.get("invariance", {}) for r in valid if r["provenance"]["rival"] == "equal_components"]
    put("P1", not failed and len(inv) == 60 and all(r["scores"]["A1"]["status"] == "PASS" for r in valid)
        and all(r.get("same_input") and r.get("same_output") for r in inv), failed=failed, invariance_seeds=len(inv))
    false_sign = []
    static_loss = []
    bridges = []
    for (panel, rv), group in groups.items():
        flags, noise = [], []
        by_k = defaultdict(list)
        for row in group:
            if row["status"] != "RAN-OK":
                continue
            p = st.m1._pair(row["analysis"]["boundaries"]["1.0"], "primary")
            car = (p or {}).get("carriers", {})
            if "pos_q95" in car and "home_q05" in car:
                flags.append(car["pos_q95"] < 0 and car["home_q05"] > 0)
            target = sim.PANELS[panel]["K"]/4
            cc = row["oracle"].get("contributions", [])
            if cc:
                noise.append(min(cc, key=lambda v: abs(v["k"]-target))["nuisance_g"] > 0)
            for k, val in row["scores"]["B9"]["detail"].get("r_over_small_step", {}).items():
                if val is not None and np.isfinite(val):
                    by_k[k].append(val)
        false_sign.append(dict(panel=panel, rival=rv, eligible=len(flags), **wilson(sum(flags), 20)))
        if rv == "fixed_static":
            static_loss.append(dict(panel=panel, eligible=len(noise), **wilson(sum(noise), 20)))
        if sim.PANELS[panel]["block"] == 52:
            bridges += [dict(panel=panel, rival=rv, k=k, n=len(v), median=float(np.median(v))) for k, v in by_k.items()]
    put("P2", any(r["k"] >= 4 and sim.PANELS[r["panel"]]["system"] == "reddit" and
                  sim.CLASSES[r["rival"]] != "named stationary level" for r in false_sign), rates=false_sign)
    recovery = [r for r in rates if r["metric"] == "class recovery"]
    home_calls = [r for r in rates if r["metric"] == "named-level calls"]
    bad = [r for r in recovery if sim.CLASSES[r["rival"]] == "named stationary level" and r["lo"] < .8]
    bad += [r for r in home_calls if sim.CLASSES[r["rival"]] != "named stationary level" and r["hi"] > .2]
    put("P3", bool(bad), failed_requirements=bad)
    good_panels = [p for p in sim.PANELS if all(r["rate"] >= .8 for r in recovery if r["panel"] == p and
                                                r["rival"] in ("replacement", "rank_drift", "position_ou"))]
    put("P4", len(good_panels) >= 2, profiles=good_panels, qualification="Check marginal_matching.csv.gz; mismatch precludes matched-marginal identification.")
    put("P5", len(static_loss) == 3 and all(r["k"] >= 16 for r in static_loss), rates=static_loss)
    put("P6", any(not .8 <= r["median"] <= 1.2 for r in bridges), cells=bridges)
    failures = [r for r in rates if r["metric"] == "all five held-out slopes within 25%" and
                sim.CLASSES[r["rival"]] != "named stationary level" and r["rival"] != "fixed_static" and r["lo"] < .8]
    put("P7", bool(failures), failed_requirements=failures)
    low = [r for r in coverage_rates if r["metric"] == "covered" and r["rate"] < .8]
    imprecise = [r for r in coverage_rates if r["metric"] == "precise" and r["rate"] < 1]
    no_coverage = [list(g) for g in groups if not any((r["panel"], r["rival"]) == g for r in coverage)]
    put("P8", bool(low or imprecise or no_coverage), below_80_percent=low, imprecise=imprecise,
        no_adjudicable_target=no_coverage, qualification="Independent population-bin target, not exact conditional coverage.")
    return matching, coverage_rates, predictions


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=st.OUT)
    ap.add_argument("--progress-only", action="store_true")
    args = ap.parse_args()
    summarize(args.out, args.progress_only)


if __name__ == "__main__":
    main()
