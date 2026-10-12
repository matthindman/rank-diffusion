"""Audit complete V2 seed sets, sources, wrappers and successful cross-check."""
import argparse,ast,hashlib,json,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import planted_crosscheck as pc
from validation import planted_study as study
from validation import planted_extension as extension

def historical_hashes(path):
    commits=subprocess.check_output(["git","log","--format=%H","--",path],cwd=ROOT,text=True).split()
    versions={}
    for commit in commits:
        data=subprocess.check_output(["git","show",commit+":"+path],cwd=ROOT)
        versions[hashlib.sha256(data).hexdigest()]=commit
    versions.setdefault(pc.digest(ROOT/path),"working copy")
    return versions
def audit(out):
    cross=pc.read(out/"crosscheck_summary.json");pc.check_sources(cross)
    if cross["status"]!="PASS":raise RuntimeError("Baseline STOP rule")
    wrappers={p:historical_hashes(p) for p in (
        "llm_fitting/validation/planted_warning_guard.py",
        "llm_fitting/validation/planted_efficient_run.py")}
    counts={};failed=[];used_wrappers={};risk_counts={};zero_risk=[]
    for panel in pc.PANELS:
        phash=pc.digest(out/panel/"parameters.json")
        plans=[("crosscheck",[(c,s) for c,ss in (("production",range(20)),("independent",range(1000,1020))) for s in ss]),
               ("recovery",[(c,s) for c in ("baseline","zero_temperament") for s in range(1000,1020)]),
               ("diagnostics",[(c,s) for c in study.CONDITIONS+extension.ADDITIONS for s in range(1000,1020)]+[("calibration",s) for s in range(2000,2039)]),
               ("extension_fits",[(c,s) for c in extension.FIT_CONDITIONS for s in range(1000,1020)])]
        counts[panel]={};risk_counts[panel]=[]
        for stage,jobs in plans:
            n=0
            for c,seed in jobs:
                path=out/panel/stage/f"{c}_{seed:04d}.json"
                d=pc.read(path)
                expected=(pc.identity() if stage=="crosscheck" else extension.sources() if stage=="extension_fits" or c in extension.ADDITIONS else study.sources())
                if d["sources"]!=expected:raise RuntimeError(f"Source mismatch: {path}")
                if d["parameter_sha256"]!=phash or d["seed"]!=seed:raise RuntimeError(f"Identity mismatch: {path}")
                if stage!="crosscheck" and (d["condition"]!=c or d["panel"]!=panel or d["stage"]!=stage):raise RuntimeError(f"Label mismatch: {path}")
                if d.get("status","RAN-OK")!="RAN-OK":failed.append(str(path))
                for key in ("execution_wrapper","eligibility_wrapper"):
                    if key not in d:continue
                    w=d[key]
                    if w["sha256"] not in wrappers.get(w["path"],{}):raise RuntimeError(f"Unknown wrapper: {path}")
                    used_wrappers[w["sha256"]]=dict(path=w["path"],commit=wrappers[w["path"]][w["sha256"]])
                if stage=="diagnostics" and d.get("status")=="RAN-OK":
                    raw=d["result"]["raw"]
                    risk=(int(raw["departure"][0]["exposure"]),int(raw["fate"][0]["n"]))
                    risk_counts[panel].append(risk)
                    if min(risk)==0:zero_risk.append(str(path))
                n+=1
            actual={p.name for p in (out/panel/stage).glob("*.json")}
            expected_names={f"{c}_{seed:04d}.json" for c,seed in jobs}
            if actual!=expected_names:raise RuntimeError(f"Unexpected/missing cells in {panel}/{stage}")
            counts[panel][stage]=n
    eq=pc.read(out/"eligibility_equivalence.json")
    if eq["status"]!="PASS" or not all(r["every_numeric_value_and_count_equal"] for r in eq["checks"]):raise RuntimeError("Eligibility comparison failed")
    # The measured optimization version predates scheduling/locking additions.
    # Confirm the numerical selection/wrapper functions have not changed.
    helper="llm_fitting/validation/planted_efficient_run.py"
    measured=subprocess.check_output(["git","show","7abf973:"+helper],cwd=ROOT,text=True)
    def functions(source):
        return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(source).body if isinstance(n,ast.FunctionDef)}
    before,now=functions(measured),functions((ROOT/helper).read_text())
    for name in ("eligible_identities","eligible_statistics"):
        if before[name]!=now[name]:raise RuntimeError("Eligibility math differs from native-tested version")
    attempts=[]
    for path in sorted((out/"attempts").glob("*.json")):
        d=pc.read(path);attempts.append(dict(file=path.name,stage=d.get("stage"),panel=d.get("panel"),status=d.get("status")))
    # STARTED entries from terminated processes remain immutable. Their resolution
    # is the explicit interruption event, never an invented successful completion.
    return dict(status="COMPLETE_WITH_FAILED_SEEDS" if failed else "PASS",
                counts=counts,failed_seed_records=failed,wrappers=used_wrappers,attempts=attempts,
                minimum_risk_counts={p:dict(departure_exposure=min(a[0] for a in v),fate_cohort=min(a[1] for a in v)) for p,v in risk_counts.items()},
                zero_risk_cells=zero_risk,
                eligibility_core_matches_native_tested_version=True,
                eligibility_equivalence_sha256=pc.digest(out/"eligibility_equivalence.json"),
                audit_sha256=pc.digest(__file__),
                note="Complete atomic seed cells only. Interrupted parent attempts are documented separately; no incomplete seed enters a table.")
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--out",type=Path,required=True);args=ap.parse_args()
    result=audit(args.out);pc.write(args.out/"audit.json",result)
    print(json.dumps({k:result[k] for k in ("status","counts","failed_seed_records")},indent=2))
if __name__=="__main__":main()
