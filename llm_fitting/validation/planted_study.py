"""Fixed V2 recovery and diagnostic experiments. Refuses a failed cross-check."""
from __future__ import annotations
import argparse, copy, dataclasses, gc, os, sys, time, traceback, warnings
from pathlib import Path
import numpy as np
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import planted_crosscheck as pc
from validation import planted_sim as sim
import residual_anatomy as ra
import minimal_rankdiff as mrd
import ladder_explainer as le

CONDITIONS=["baseline","gaussian_control"]+[f"{m}_{j}" for m,v in sim.MECHANISMS.items() if v is not None for j in (0,1)]
COUNT_KEYS={"n","n_present","entities","exposure"}
LABEL_KEYS={"group","h","k","mode","cls","block","pair"}
def sources():
    return {**pc.identity(),
            "llm_fitting/residual_anatomy.py":pc.digest(ROOT/"llm_fitting/residual_anatomy.py"),
            "llm_fitting/validation/planted_study.py":pc.digest(__file__)}
def get_params(panel,out):
    cross=pc.read(out/"crosscheck_summary.json")
    pc.check_sources(cross)
    if cross["status"]!="PASS": raise RuntimeError("V2 STOP: baseline discrepancy")
    d=pc.read(out/panel/"parameters.json");pc.check_sources(d)
    return d,pc.unpack(d["parameters"])
def condition(name):
    if name in ("baseline","gaussian_control"):return name,0
    m,j=name.rsplit("_",1);return m,int(j)
def diagnostics(s,K):
    R,X=ra.from_sim(s);T=R.shape[0];fl=s["tranks"].shape[1]+1
    kw=dict(floor=fl)
    events,exposure=ra.departures(R,K,T//2,t1=T,**kw)
    raw={
       "tails":ra.tails_by_horizon(R,X,K,**kw),
       "retention":ra.retention(R,X,K,**kw),
       "departure":[ra.departure_summary(events,exposure)],
       "dispersion":ra.long_dispersion(R,X,K,**kw),
       "fate":[ra.cohort_fate(R,K,T//2,t1=T,**kw)],
       "volatility":ra.volatility_persistence(R,X,K,**kw),
       "far_tail":ra.far_tail(R,X,K,**kw),
       "momentum":[],"coherence":[]}
    for b in (13,26):
        if T>=5*b: raw["momentum"]+=ra.momentum(R,X,K,block=b,**kw)
    raw["momentum"]+=ra.momentum(R,X,K,block=13,gaps=(1,2,3,4,6,8),**kw)
    if T>=208:raw["momentum"]+=ra.momentum(R,X,K,block=52,gaps=(1,2),**kw)
    for h in (13,26):
        if T>=5*h:raw["coherence"]+=ra.amplitude_coherence(R,X,K,h_long=h,**kw)
    features={}
    for family,rows in raw.items():
        for row in rows:
            tags="/".join(f"{k}={row[k]}" for k in sorted(LABEL_KEYS) if k in row)
            for k,v in row.items():
                if k not in COUNT_KEYS|LABEL_KEYS and isinstance(v,(int,float,np.number)):
                    key="/".join(x for x in (family,tags,k) if x)
                    if key in features:raise RuntimeError(f"Duplicate feature {key}")
                    features[key]=float(v)
    return dict(features=features,raw=raw,identities=R.shape[1],weeks=T)

def recover(s,pars):
    p=pc.unpack(pars["parameters"])
    df=sim.registered_panel(dict(full_values=s["tvals"],full_ids=s["tids"]))
    floor_fraction=float((df.metric==0).mean())
    u=mrd.restrict_universe(df,pars["K"],buffer_mult=4)
    del df
    estimate=le._fit(u,pars["spec"])
    zk=p.z_knots
    values={}
    for name in ("kappa_z","sigma_perm","sigma_trans","sigma_obs","phi","phi2","sigma_trans2"):
        arr=getattr(estimate,name)
        values[name]=np.interp(zk,estimate.z_knots,arr) if arr is not None else np.full(len(zk),np.nan)
    values["V"]=values["sigma_perm"]**2/(2*values["kappa_z"])
    values["V_discrete"]=values["sigma_perm"]**2/(values["kappa_z"]*(2-values["kappa_z"]))
    values["temper_s"]=estimate.temper_s;values["t_df"]=estimate.t_df
    values["tail_weight"]=0. if np.isinf(estimate.t_df) else 1/(estimate.t_df-4)
    edge=np.isclose(estimate.kappa_z,1-mrd.A2_GRID.min())|np.isclose(estimate.kappa_z,1-mrd.A2_GRID.max())
    return dict(estimates=values,kappa_edge_fraction=float(edge.mean()),
                nnls_zero_fraction={n:float(np.mean(getattr(estimate,n)==0)) for n in
                    ("sigma_perm","sigma_trans","sigma_obs","sigma_trans2")},
                zero_floor_fraction=floor_fraction,N=estimate.N,rows=len(u),
                selected_identities=u.entity_id.nunique(),
                weekly_n=u.groupby("period").size().tolist(),
                fitted_parameters=dataclasses.asdict(estimate))

def run(stage,panel,out):
    pars,p=get_params(panel,out);phash=pc.digest(out/panel/"parameters.json")
    if stage=="recovery":jobs=[(c,s) for c in ("baseline","zero_temperament") for s in range(1000,1020)]
    else:jobs=[(c,s) for c in CONDITIONS for s in range(1000,1020)]+[("calibration",s) for s in range(2000,2039)]
    for name,seed in jobs:
        dest=out/panel/stage/f"{name}_{seed:04d}.json"
        if dest.exists():
            d=pc.read(dest)
            if d["sources"]!=sources() or d["parameter_sha256"]!=phash:raise RuntimeError("Changed checkpoint inputs")
            # FAILED seeds remain in the denominator; no adaptive replacement.
            continue
        start=time.time();pp=copy.deepcopy(p)
        if name=="zero_temperament":pp.temper_s=0.
        mechanism,strength=condition("baseline" if name in ("calibration","zero_temperament") else name)
        row=dict(panel=panel,condition=name,seed=seed,stage=stage,sources=sources(),parameter_sha256=phash)
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                s=sim.simulate(pp,pars["T"],seed,mechanism=mechanism,strength=strength,track=pp.N,top_record=pars["K"])
                result=recover(s,pars) if stage=="recovery" else diagnostics(s,pars["K"])
                row.update(status="RAN-OK",result=result,
                           warnings={str(w.message):sum(str(z.message)==str(w.message) for z in caught) for w in caught})
            del s
        except Exception:
            row.update(status="FAILED",traceback=traceback.format_exc())
        row["seconds"]=time.time()-start
        pc.write(dest,row);gc.collect()
        print(panel,stage,name,seed,row["status"],round(row["seconds"],1),flush=True)
def main():
    ap=argparse.ArgumentParser();ap.add_argument("stage",choices=["recovery","diagnostics"])
    ap.add_argument("--panel",choices=list(pc.PANELS),required=True);ap.add_argument("--out",type=Path,required=True)
    args=ap.parse_args();os.chdir(ROOT)
    attempt=args.out/"attempts"/f"{time.time_ns()}.json"
    pc.write(attempt,dict(stage=args.stage,panel=args.panel,status="STARTED",sources=sources()))
    try:
        run(args.stage,args.panel,args.out)
    except BaseException:
        pc.write(attempt,dict(stage=args.stage,panel=args.panel,status="FAILED",sources=sources(),traceback=traceback.format_exc()))
        raise
    pc.write(attempt,dict(stage=args.stage,panel=args.panel,status="RAN-OK",sources=sources()))
if __name__=="__main__":main()
