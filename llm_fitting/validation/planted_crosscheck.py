"""V2 frozen-parameter extraction and independent simulator discrepancy check.

No real-panel diagnostics, residuals or movement gate are evaluated here.
Run via heavy_lock.py; checkpoints refuse source/parameter identity changes.
"""
from __future__ import annotations
import argparse, dataclasses, hashlib, json, os, platform, subprocess, sys, time, traceback
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
sys.path.insert(0,str(ROOT/"scripts"))
import minimal_rankdiff as mrd
import ladder_explainer as le
from sealed_data_guard import require_released
from validation import planted_sim as independent

PANELS={"facebook_a":86,"reddit_comments":136,"reddit_submissions_long":212}
KEYS=[k for block in le.MACRO_BLOCKS.values() for k,_ in block]
SOURCES=["llm_fitting/validation/planted_sim.py",
         "llm_fitting/validation/planted_crosscheck.py",
         "llm_fitting/minimal_rankdiff.py","llm_fitting/rankdiff_kalman.py",
         "llm_fitting/ladder_explainer.py","llm_fitting/community_metrics.py"]
def digest(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for b in iter(lambda:f.read(2**20),b""): h.update(b)
    return h.hexdigest()
def identity():
    return {p:digest(ROOT/p) for p in SOURCES}
def clean(x):
    if isinstance(x,np.ndarray): return clean(x.tolist())
    if isinstance(x,dict): return {str(k):clean(v) for k,v in x.items()}
    if isinstance(x,(list,tuple)): return [clean(v) for v in x]
    if isinstance(x,np.generic): return clean(x.item())
    if isinstance(x,float) and not np.isfinite(x):
        return "Infinity" if x==np.inf else "-Infinity" if x==-np.inf else None
    return x
def write(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+".tmp")
    temp.write_text(json.dumps(clean(data),indent=2,sort_keys=True)+"\n")
    os.replace(temp,path)
def read(path): return json.loads(Path(path).read_text())
def unpack(d):
    out={}
    for f in dataclasses.fields(mrd.RankParams):
        x=d[f.name]
        out[f.name]=np.asarray(x,float) if isinstance(x,list) else np.inf if x=="Infinity" else x
    return mrd.RankParams(**out)
def check_sources(d):
    if d["sources"]!=identity(): raise RuntimeError("Checkpoint source identity mismatch")
def extract(panel,out):
    dest=out/panel/"parameters.json"
    if dest.exists():
        d=read(dest);check_sources(d)
        require_released(d["input_path"])
        if digest(d["input_path"])!=d["input_sha256"]: raise RuntimeError("Input changed")
        print(panel,"parameters verified",flush=True);return d
    cfg=mrd.PLATFORMS[le.PANELS[panel]["struct"]]
    require_released(cfg["path"])
    inp=digest(cfg["path"])
    df=mrd.restrict_universe(mrd.load_panel(cfg),le.PANELS[panel]["K"],buffer_mult=4)
    T=int(df.period.max())+1
    if T!=PANELS[panel]: raise RuntimeError(f"Unexpected native T: {T}")
    spec=le.spec_for(panel,"struct","S4")
    start=time.time();p=le._fit(df,spec)
    d=dict(panel=panel,K=le.PANELS[panel]["K"],T=T,spec=spec,
           parameters=dataclasses.asdict(p),sources=identity(),
           input_path=cfg["path"],input_sha256=inp,seconds=time.time()-start,
           git=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
           python=platform.python_version(),numpy=np.__version__,pandas=pd.__version__,
           universe=dict(rows=len(df),identities=df.entity_id.nunique(),
                         weekly_n=df.groupby("period").size().tolist()))
    write(dest,d);print(panel,"parameters saved",flush=True)
    return read(dest)

def describe(a):
    a=np.asarray(a,float)
    return dict(n=len(a),mean=a.mean(0),sd=a.std(0,ddof=1),
                q05=np.quantile(a,.05,axis=0),q95=np.quantile(a,.95,axis=0))

def compare(a,b,permutations=20000,rng_seed=2026101202,family=57):
    """Whole-vector randomization; plus-one p values, conservative with ties."""
    a=np.asarray(a,float);b=np.asarray(b,float)
    if a.shape!=b.shape or a.ndim!=2: raise ValueError("Matched matrix shapes required")
    if not np.all(np.isfinite(a)) or not np.all(np.isfinite(b)):
        raise ValueError("Required statistic unavailable")
    n=len(a);pool=np.concatenate([a,b]);sums=pool.sum(0)
    rng=np.random.default_rng(rng_seed)
    null=np.empty((permutations,a.shape[1]))
    for j in range(permutations):
        selected=rng.permutation(2*n)[:n]
        null[j]=(2*pool[selected].sum(0)-sums)/n
    delta=b.mean(0)-a.mean(0)
    # Allow floating summation noise at exact ties only.
    abs_delta=np.abs(delta)
    tol=32*np.finfo(float).eps*np.maximum(1,np.max(np.abs(pool),axis=0))
    p=(1+(np.abs(null)>=abs_delta-tol).sum(0))/(permutations+1)
    return dict(production=describe(a),independent=describe(b),
                delta=delta,mcse=np.sqrt(a.var(0,ddof=1)/n+b.var(0,ddof=1)/n),
                p=p,p_bonferroni=np.minimum(1,p*family),
                null_lower=np.quantile(null,.05/(2*family),axis=0),
                null_upper=np.quantile(null,1-.05/(2*family),axis=0),
                reject=p<=.05/family,permutations=permutations,family=family)

def run(panel,out):
    pars=extract(panel,out);p=unpack(pars["parameters"])
    phash=digest(out/panel/"parameters.json")
    for kind,seeds in (("production",range(20)),("independent",range(1000,1020))):
        for seed in seeds:
            dest=out/panel/"crosscheck"/f"{kind}_{seed:04d}.json"
            if dest.exists():
                d=read(dest);check_sources(d)
                if d["parameter_sha256"]!=phash: raise RuntimeError("Parameter changed")
                continue
            start=time.time()
            if kind=="production":
                sim=mrd.simulate(p,pars["T"],seed=seed,track=4000,top_record=pars["K"])
            else:
                sim=independent.simulate(p,pars["T"],seed=seed,track=4000,top_record=pars["K"])
            stats=le._stats(sim["tvals"],sim["tranks"],sim["top_ids"],sim["ranksize"],
                            max(10,round(.01*pars["K"])),pars["K"],min(p.N,2000))
            write(dest,dict(kind=kind,seed=seed,statistics={k:stats[k] for k in KEYS},
                            seconds=time.time()-start,sources=identity(),parameter_sha256=phash))
            print(panel,kind,seed,"saved",flush=True)
    print(panel,"crosscheck seed bank complete",flush=True)

def summarize(out):
    result={}
    for panel in PANELS:
        rows=[]
        for kind,seeds in (("production",range(20)),("independent",range(1000,1020))):
            r=[]
            for seed in seeds:
                d=read(out/panel/"crosscheck"/f"{kind}_{seed:04d}.json");check_sources(d)
                r.append([d["statistics"][k] for k in KEYS])
            rows.append(np.asarray(r,float))
        try:
            c=compare(*rows);c["status"]="STOP" if np.any(c["reject"]) else "PASS"
        except ValueError as e:
            c=dict(status="STOP",reason=str(e),production=rows[0],independent=rows[1])
        result[panel]=c
    write(out/"crosscheck_summary.json",dict(keys=KEYS,panels=result,
        status="PASS" if all(r["status"]=="PASS" for r in result.values()) else "STOP",
        sources=identity()))
    print({k:r["status"] for k,r in result.items()},flush=True)

def main():
    ap=argparse.ArgumentParser();ap.add_argument("stage",choices=["run","summarize"])
    ap.add_argument("--panel",choices=list(PANELS));ap.add_argument("--out",type=Path,required=True)
    args=ap.parse_args()
    os.chdir(ROOT)
    attempt=args.out/"attempts"/f"{time.time_ns()}.json"
    write(attempt,dict(stage=args.stage,panel=args.panel,status="STARTED",sources=identity()))
    try:
        if args.stage=="run": run(args.panel,args.out)
        else: summarize(args.out)
    except BaseException:
        write(attempt,dict(stage=args.stage,panel=args.panel,status="FAILED",
                          traceback=traceback.format_exc(),sources=identity()))
        raise
    write(attempt,dict(stage=args.stage,panel=args.panel,status="RAN-OK",sources=identity()))
if __name__=="__main__": main()
