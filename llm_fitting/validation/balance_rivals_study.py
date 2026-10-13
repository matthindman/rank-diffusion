"""V4 atomic runner. Inputs are saved V2 parameters; no M1 results are read."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
import traceback
import warnings

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import balance_accounting as ba
import mts_step1 as m1
from validation import balance_rivals_sim as sim
from validation import balance_rivals_inputs as inputs

OUT = ROOT / "llm_fitting/runs/2026-10-13_V4_planted_rivals"
HANDOFF = ROOT / "llm_fitting/handoff/V4_planted_rivals"
SOURCES = ("llm_fitting/balance_accounting.py", "llm_fitting/mts_step1.py",
           "llm_fitting/MTS_PROTOCOL_STEPS_1_2_2026-10-13.md",
           "llm_fitting/validation/planted_sim.py", "llm_fitting/validation/balance_rivals_sim.py",
           "llm_fitting/validation/balance_rivals_frozen.py",
           "llm_fitting/validation/balance_rivals_inputs.py",
           "llm_fitting/validation/balance_rivals_report.py",
           "llm_fitting/validation/balance_rivals_study.py", "llm_fitting/handoff/V4_planted_rivals/DESIGN.md")


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for buf in iter(lambda: f.read(2**20), b""):
            h.update(buf)
    return h.hexdigest()


def clean(x):
    if isinstance(x, dict):
        return {str(k): clean(v) for k, v in x.items()}
    if isinstance(x, (tuple, list, np.ndarray)):
        return [clean(v) for v in x]
    if isinstance(x, np.generic):
        return clean(x.item())
    if isinstance(x, float) and not np.isfinite(x):
        return None
    return x


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name+f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(clean(value), sort_keys=True, separators=(",", ":"))+"\n")
    os.replace(temp, path)


def read(path):
    return json.loads(Path(path).read_text())


def source_hashes():
    return {p: digest(ROOT/p) for p in SOURCES}


def prepare(source, out, with_absence=False):
    """Copy only the three named saved parameter inputs, never traverse data."""
    manifest = {}
    for panel, config in sim.PANELS.items():
        src = source/config["source"]/"parameters.json"
        d = read(src)
        if d["panel"] != config["source"] or d["K"] != config["K"]:
            raise ValueError(f"Unexpected V2 input: {src}")
        dest = out/"inputs"/f"{panel}.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and digest(dest) != digest(src):
            raise RuntimeError("Refusing to replace a different input")
        if not dest.exists():
            shutil.copyfile(src, dest)
        cal = sim.calibrate(d, config)
        manifest[panel] = dict(path=str(src), sha256=digest(src), source_panel=d["panel"],
                               source_weeks=d["T"], config=config,
                               parameters={k: v for k, v in cal.items() if k not in ("p", "config", "absence")},
                               V2_input_sha256=d["input_sha256"], V2_git=d["git"])
    dest = out/"input_manifest.json"
    if dest.exists() and read(dest) != clean(manifest):
        raise RuntimeError("Input manifest changed")
    write(dest, manifest)
    if with_absence:
        for panel, config in sim.PANELS.items():
            dest = out/"inputs"/f"{panel}_absence.json"
            producer = digest(ROOT/"llm_fitting/validation/balance_rivals_inputs.py")
            loader = digest(ROOT/"llm_fitting/minimal_rankdiff.py")
            if dest.exists():
                old = read(dest)
                if old.get("producer_sha256") != producer or old.get("loader_sha256") != loader:
                    raise RuntimeError("Absence input producer changed")
            else:
                profile = inputs.extract(panel, config)
                profile.update(producer_sha256=producer, loader_sha256=loader)
                write(dest, profile)
            print(panel, "absence profile verified", flush=True)
        write(out/"absence_manifest.json", {p: read(out/"inputs"/f"{p}_absence.json") for p in sim.PANELS})
    return manifest


def feature_vector(res):
    """Only predeclared M1 summaries, never a slope or population-size feature."""
    c = res["boundaries"]["1.0"]
    K, ks = res["K"], c["ks"]
    KR = [k for k in ks if K/16-1 <= k <= K]
    ix = [ks.index(k) for k in KR]
    f = {}
    def mean(vals):
        v = np.asarray(vals, float)
        return float(np.mean(v[np.isfinite(v)])) if np.isfinite(v).any() else np.nan
    for h, w in c["weekly"].items():
        for key in ("g", "r", "f", "shape"):
            f[f"weekly/{h}/{key}"] = mean([w[key]["mean"][j] for j in ix])
        f[f"weekly/{h}/bridge"] = mean([w["r"]["mean"][j]/w["r_diff"]["mean"][j]
                                          if w["r_diff"]["mean"][j] else np.nan for j in ix])
    names = ["slide", "bin_spread", "pos", "home", "share", "block/g", "block/r",
             "block/f", "block/shape", "block/bridge", "gap_ratio"]
    f.update({n: np.nan for n in names})
    p = m1._pair(c, "primary")
    if p is None or "not_adjudicated" in p:
        return f
    f["slide"] = m1._slide(p, K)[0]
    f["bin_spread"] = float(np.std([r["mean"] for r in m1._octaves(p, K) if "mean" in r]))
    for name in ("pos", "home"):
        f[name] = p.get("carriers", {}).get(name, np.nan)
    for name, source in (("g", "G"), ("r", "R"), ("f", "F"), ("shape", "shape")):
        f[f"block/{name}"] = mean([a[source]/(a["k"]*p["weeks"]) for a in p["accounting"]
                                       if a["k"] in KR and source in a])
    bridge, shares = [], []
    share_ks = [m1._near(ks, K/d) for d in (4, 2, 1)]
    for a, d, pr in zip(p["accounting"], p["diffusion"], p["predictable"]):
        if a["k"] in KR and a.get("R") is not None and d.get("R_diff", 0) > 0:
            bridge.append(a["R"]/d["R_diff"])
        if a["k"] in share_ks and pr.get("G_pred") is not None and a.get("R", 0) > 0:
            shares.append(pr["G_pred"]/a["R"])
    f["share"], f["block/bridge"] = mean(shares), mean(bridge)
    ng = m1._pair(c, "no gap")
    if ng and "drift" in ng and f["slide"] and f["slide"] > 0:
        f["gap_ratio"] = m1._slide(ng, K)[0]/f["slide"]
    return f


def slope_predictions(res):
    """Movement-only API: explicitly strip S and R_diff before prediction."""
    cell, K = res["boundaries"]["1.0"], res["K"]
    p = m1._pair(cell, "primary")
    kk = sorted(set(m1._near(cell["ks"], K/d) for d in (16, 8, 4, 2, 1)))
    if p is None or "not_adjudicated" in p:
        return {str(k): dict(status="FAILED", reason="uncertified primary pair") for k in kk}
    s = m1._slide(p, K)[0]
    movement = {d["k"]: d["v"]/p["weeks"] for d in p["diffusion"] if "v" in d}
    return {str(k): (dict(status="VALID", slide=s, variance_per_week=movement[k],
                          predicted=movement[k]/(2*s)) if s is not None and s > 0 and
                    k in movement and np.isfinite(movement[k]) else
                    dict(status="FAILED", reason="nonpositive slide or unavailable variance")) for k in kk}


def audit_layers(path, df, config, res=None):
    """Oracle accounting contributions on the SAME observed occupant sets."""
    K, block, T = config["K"], config["block"], config["T"]
    X, R, floors, _, ids = m1.matrices(df, K)
    _, L = sim.aligned_layers(path, ids)
    B = ba.block_means(X, block, start=T % block, min_frac=m1.PRES)
    BL = ba.block_means(L, block, start=T % block, min_frac=m1.PRES)
    BN = B - BL
    lo = T % block
    pr, pres = ba.perm_rank(R, lo, lo+block, floors)
    labels, which = ba.cohort_bins(pr, K, pres, pres_floor=m1.PRES)
    b0, b1 = ((2, 3) if len(B) >= 4 else (1, 2))
    valid = np.isfinite(B[b0]) & np.isfinite(B[b1])
    means = ba.cohort_drift(B, np.zeros(len(B)), which, len(labels), b0, b1, n_boot=1)
    oracle = {str(j): dict(n=r["n"], total=r["mean"]*r["n"]) for j, r in enumerate(means[:-1]) if "mean" in r}
    out = dict(absolute_bin_moments=oracle, labels=labels,
               latent_variance=float(np.nanvar((BL[b1]-BL[b0])[(pr > K/16) & (pr <= K) & valid])),
               block_slopes={str(k): sim.slope(ba.ladder(B[b0]), k)
                             for k in m1._ks(K, K) if K/16-1 <= k <= K})
    if res is None:
        return out
    p = m1._pair(res["boundaries"]["1.0"], "primary")
    if p is None or "not_adjudicated" in p:
        return out
    o0, o1 = ba._order(B[b0]), ba._order(B[b1])
    dcN = BN[b1, o1[:K]].mean()-BN[b0, o0[:K]].mean()
    dcL = BL[b1, o1[:K]].mean()-BL[b0, o0[:K]].mean()
    dn, dl = BN[b1]-BN[b0]-dcN, BL[b1]-BL[b0]-dcL
    bin_noise = ba.cohort_drift(BN, np.zeros(len(B)), which, len(labels), b0, b1, n_boot=1)
    out["centre_change"] = float(B[b1, o1[:K]].mean()-B[b0, o0[:K]].mean())
    out["contributions"] = []
    for a, pred in zip(p["accounting"], p["predictable"]):
        if "G" not in a:
            continue
        k = a["k"]
        occ = o0[:k]
        surv = occ[np.isfinite(B[b1, occ])]
        gn, gl = -dn[surv].sum(), -dl[surv].sum()
        w = np.asarray(pred["bin_counts"])
        use = w > 0
        mn = np.array([r.get("mean", np.nan) for r in bin_noise]) - dcN
        pn = -float(w[use] @ mn[use]) if np.isfinite(mn[use]).all() else np.nan
        out["contributions"].append(dict(k=k, nuisance_G=gn, latent_G=gl,
                                         resid=gn+gl-a["G"], nuisance_g=gn/(k*p["weeks"]),
                                         nuisance_fraction=gn/a["G"] if a["G"] > 0 else None,
                                         nuisance_predictable_share=pn/a["R"] if a["R"] > 0 else None))
    return out


def heldout_truth(path, predictions, config):
    ids = np.unique(path["ids"])
    X, L = sim.aligned_layers(path, ids)
    bx = ba.block_means(X, config["block"], min_frac=m1.PRES)[0]
    bl = ba.block_means(L, config["block"], min_frac=m1.PRES)[0]
    rows = {}
    for key, pred in predictions.items():
        k = int(key)
        truth, latent = sim.slope(ba.ladder(bx), k), sim.slope(ba.ladder(bl), k)
        row = dict(**pred, truth=truth, latent_truth=latent)
        if pred["status"] == "VALID" and truth > 0 and np.isfinite(truth):
            row["relative_error"] = pred["predicted"]/truth-1
            row["latent_relative_error"] = pred["predicted"]/latent-1 if latent > 0 else None
            row["success"] = bool(abs(row["relative_error"]) <= .25)
        else:
            row["success"] = False
        rows[key] = row
    return rows


def run_cell(panel, rival, seed, stage, out, config_override=None):
    config = config_override or sim.PANELS[panel]
    inp = out/"inputs"/f"{panel}.json"
    absence_path = out/"inputs"/f"{panel}_absence.json"
    absence = None if config_override is not None else read(absence_path)
    provenance = dict(sources=source_hashes(), input_sha256=digest(inp), config=config,
                      absence_sha256=digest(absence_path) if absence is not None else "synthetic-fixture",
                      stage=stage, panel=panel, rival=rival, seed=seed)
    dest = out/"cells"/panel/rival/f"{stage}_{seed}.json"
    if dest.exists():
        old = read(dest)
        if old["provenance"] != provenance:
            raise RuntimeError(f"Checkpoint identity changed: {dest}")
        return old
    started = time.time()
    attempt = out/"attempts"/f"{panel}_{rival}_{stage}_{seed}_{time.time_ns()}.json"
    write(attempt, dict(status="STARTED", provenance=provenance))
    row = dict(provenance=provenance, status="FAILED", python=platform.python_version(), numpy=np.__version__,
               git=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip())
    try:
        cal = sim.calibrate(read(inp), config, absence)
        process = sim.Process(cal, rival, seed)
        row["simulation_population"] = process.n
        path = process.generate(config["T"])
        df = sim.long_frame(path)
        with warnings.catch_warnings():
            # Empty cohorts remain explicit NA values; avoid unbounded warning capture.
            warnings.simplefilter("ignore", RuntimeWarning)
            if stage == "calibration":
                row["oracle"] = audit_layers(path, df, config)
            else:
                res = m1.analyse(df, config["K"], config["block"])
                row.update(analysis=res, scores=m1.score(res, config["system"]), features=feature_vector(res),
                           oracle=audit_layers(path, df, config, res))
                preds = slope_predictions(res)
                # The forecast is durably fixed before ANY target week is generated.
                prediction_path = out/"predictions"/panel/rival/f"{seed}.json"
                prediction_record = clean(dict(provenance=provenance, predictions=preds))
                if prediction_path.exists() and read(prediction_path) != prediction_record:
                    raise RuntimeError("An interrupted attempt has a different frozen prediction")
                write(prediction_path, prediction_record)
                if rival == "equal_components":
                    # Relabel only the private metadata; the measured input is the same array.
                    before = hashlib.sha256(path["values"].tobytes()).hexdigest()
                    labels = dict(signal="slow", noise="fast")
                    labels["signal"], labels["noise"] = labels["noise"], labels["signal"]
                    again = m1.analyse(df, config["K"], config["block"])
                    row["invariance"] = dict(input_sha256=before,
                        same_input=before == hashlib.sha256(path["values"].tobytes()).hexdigest(),
                        same_output=clean(res) == clean(again), labels=labels)
                del df, path
                gc.collect()
                row["heldout"] = heldout_truth(process.generate(config["block"]), preds, config)
        row["status"] = "RAN-OK"
    except Exception:
        row["traceback"] = traceback.format_exc()
    row["seconds"] = time.time()-started
    write(dest, row)
    write(attempt.with_name(attempt.stem+"_finished.json"), dict(status=row["status"], seconds=row["seconds"], cell=str(dest)))
    print(panel, rival, stage, seed, row["status"], f"{row['seconds']:.1f}s", flush=True)
    gc.collect()
    return clean(row)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("action", choices=("prepare", "run"))
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--source", type=Path,
                    default=Path("/Users/hindman/Documents/GitHub/rank-diffusion-codex/llm_fitting/runs/2026-10-12_V2_planted_mechanisms"))
    ap.add_argument("--panel", choices=sim.PANELS)
    ap.add_argument("--rival", choices=sim.RIVALS)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--stage", choices=("evaluation", "calibration"))
    ap.add_argument("--with-absence", action="store_true", help="Prepare the declared DEVELOPMENT absence profiles")
    args = ap.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if args.action == "prepare":
        prepare(args.source, out, args.with_absence)
        print("Three V2 parameter files and their marginal calibrations verified.")
        return
    # Prevent overlapping launches even if both acquire separate machine slots.
    with open(out/"study.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for stage in ([args.stage] if args.stage else ("calibration", "evaluation")):
            seeds = range(1000, 1020) if stage == "evaluation" else range(2000, 2020)
            if args.seed is not None:
                if args.seed not in seeds:
                    raise ValueError("Seed outside the fixed study grid")
                seeds = [args.seed]
            for panel in ([args.panel] if args.panel else sim.PANELS):
                for rival in ([args.rival] if args.rival else sim.RIVALS):
                    for seed in seeds:
                        run_cell(panel, rival, seed, stage, out)


if __name__ == "__main__":
    main()
