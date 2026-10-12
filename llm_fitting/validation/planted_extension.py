"""Dated V2 addendum; original simulator/checkpoints remain immutable."""
from __future__ import annotations
import argparse,gc,os,sys,time,traceback,warnings
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import planted_crosscheck as pc
from validation import planted_study as study
from validation import planted_sim as original
from validation import planted_extended as extended
ADDITIONS=[f"{m}_{j}" for m in ("ladder_attraction","annual_cycle") for j in (0,1)]
FIT_CONDITIONS=ADDITIONS+["random_walk_0","random_walk_1"]
def sources():
    return {**study.sources(),
            "llm_fitting/validation/planted_extended.py":pc.digest(ROOT/"llm_fitting/validation/planted_extended.py"),
            "llm_fitting/validation/planted_extension.py":pc.digest(__file__)}
def valid_existing(path,phash):
    if not path.exists():return False
    r=pc.read(path)
    if r["sources"]!=sources() or r["parameter_sha256"]!=phash:raise RuntimeError("Extension checkpoint mismatch")
    return True
def run(panel,out):
    pars,p=study.get_params(panel,out);phash=pc.digest(out/panel/"parameters.json")
    for name in FIT_CONDITIONS:
        for seed in range(1000,1020):
            fitpath=out/panel/"extension_fits"/f"{name}_{seed:04d}.json"
            diapath=out/panel/"diagnostics"/f"{name}_{seed:04d}.json"
            needfit=not valid_existing(fitpath,phash)
            needdiag=name in ADDITIONS and not valid_existing(diapath,phash)
            if not (needfit or needdiag):continue
            m,j=study.condition(name);start=time.time()
            simulator=extended if name in ADDITIONS else original
            s=simulator.simulate(p,pars["T"],seed,mechanism=m,strength=j,track=p.N,top_record=pars["K"])
            sim_seconds=time.time()-start
            for stage,path,needed,fn in (
                ("extension_fits",fitpath,needfit,lambda:study.recover(s,pars)),
                ("diagnostics",diapath,needdiag,lambda:study.diagnostics(s,pars["K"]))):
                if not needed:continue
                row=dict(panel=panel,condition=name,seed=seed,stage=stage,sources=sources(),
                         parameter_sha256=phash,simulation_seconds=sim_seconds)
                start=time.time()
                try:
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter("always")
                        row.update(status="RAN-OK",result=fn(),
                                   warnings={str(w.message):sum(str(z.message)==str(w.message) for z in caught) for w in caught})
                except Exception:
                    row.update(status="FAILED",traceback=traceback.format_exc())
                row["seconds"]=time.time()-start;pc.write(path,row)
                print(panel,stage,name,seed,row["status"],round(row["seconds"],1),flush=True)
            del s;gc.collect()
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--panel",choices=list(pc.PANELS),required=True)
    ap.add_argument("--out",type=Path,required=True);args=ap.parse_args();os.chdir(ROOT)
    attempt=args.out/"attempts"/f"{time.time_ns()}.json"
    pc.write(attempt,dict(stage="extension",panel=args.panel,status="STARTED",sources=sources()))
    try:run(args.panel,args.out)
    except BaseException:
        pc.write(attempt,dict(stage="extension",panel=args.panel,status="FAILED",sources=sources(),traceback=traceback.format_exc()))
        raise
    pc.write(attempt,dict(stage="extension",panel=args.panel,status="RAN-OK",sources=sources()))
if __name__=="__main__":main()
