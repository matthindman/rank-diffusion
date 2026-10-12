"""Complete V2 recovery and conditional diagnostic tables, no selected seeds."""
from __future__ import annotations
import argparse,itertools,sys
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import planted_crosscheck as pc
from validation import planted_study as study

def rate_interval(k,n):
    if n==0:return dict(n=0,k=k,rate=None,mcse=None,lower=None,upper=None)
    p=k/n;z=1.959963984540054
    den=1+z*z/n;center=(p+z*z/(2*n))/den
    half=z*np.sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return dict(n=n,k=k,rate=p,mcse=np.sqrt(p*(1-p)/n),lower=center-half,upper=center+half)
def summary(a,truth=None):
    a=np.asarray(a,float)
    finite=a[np.isfinite(a)]
    n=len(finite)
    out=dict(requested=len(a),valid=n,missing=int(np.isnan(a).sum()),
             positive_infinity=int(np.isposinf(a).sum()),negative_infinity=int(np.isneginf(a).sum()))
    if not n:return out
    out.update(mean=finite.mean(),sd=finite.std(ddof=1) if n>1 else np.nan,
               q05=np.quantile(finite,.05),q50=np.median(finite),q95=np.quantile(finite,.95),
               mcse_mean=finite.std(ddof=1)/np.sqrt(n) if n>1 else np.nan)
    if truth is not None:
        out["truth"]=truth;out["bias"]=finite.mean()-truth
        out["rmse"]=np.sqrt(np.mean((finite-truth)**2))
    if n>=3:
        for name,fn in (("sd",lambda x:np.std(x,ddof=1)),
                        ("rmse",lambda x:np.sqrt(np.mean((x-truth)**2)))):
            if name=="rmse" and truth is None:continue
            jack=np.array([fn(np.delete(finite,j)) for j in range(n)])
            out["mcse_"+name]=np.sqrt((n-1)/n*np.sum((jack-jack.mean())**2))
    return out

def load_rows(out,panel,stage,name,seeds):
    rows=[]
    for seed in seeds:
        r=pc.read(out/panel/stage/f"{name}_{seed:04d}.json")
        if r["sources"]!=study.sources():raise RuntimeError("Source mismatch")
        if r["parameter_sha256"]!=pc.digest(out/panel/"parameters.json"):raise RuntimeError("Parameter mismatch")
        rows.append(r)
    return rows
def group_for(z,N,K):
    rank=N*np.exp(z)+.5
    return np.select([rank<=.1*K,rank<=.5*K,rank<=K],["head","middle","shell"],default="tail")

def recovery(out):
    records=[];extras=[];failures=[]
    for panel in pc.PANELS:
        pars,p=study.get_params(panel,out)
        truth={n:getattr(p,n) for n in ("kappa_z","sigma_perm","sigma_trans","sigma_obs","phi","phi2","sigma_trans2")}
        truth.update(V=p.sigma_perm**2/(2*p.kappa_z),V_discrete=p.sigma_perm**2/(p.kappa_z*(2-p.kappa_z)),
                     temper_s=p.temper_s,t_df=p.t_df,tail_weight=0 if np.isinf(p.t_df) else 1/(p.t_df-4))
        groups=group_for(p.z_knots,p.N,pars["K"])
        for condition in ("baseline","zero_temperament"):
            rows=load_rows(out,panel,"recovery",condition,range(1000,1020))
            failures += [dict(panel=panel,stage="recovery",condition=condition,seed=r["seed"],traceback=r["traceback"]) for r in rows if r["status"]!="RAN-OK"]
            get=lambda r,n:r["result"]["estimates"].get(n) if r["status"]=="RAN-OK" else None
            for name,tr in truth.items():
                if name=="temper_s" and condition=="zero_temperament":tr=0.
                vals=[]
                for r in rows:
                    a=get(r,name)
                    vals.append(np.full_like(tr,np.nan,dtype=float) if a is None else
                                np.inf if a=="Infinity" else a if not isinstance(a,list) else [np.nan if v is None else v for v in a])
                a=np.asarray(vals,float)
                if a.ndim==1:
                    row=dict(panel=panel,condition=condition,parameter=name,knot=None,group="scalar",**summary(a,tr))
                    if np.isinf(a).any() or not np.isfinite(tr):
                        row["finite_conditional"]=True
                        row["unconditional_mean"]=None;row["unconditional_bias"]=None
                    records.append(row)
                else:
                    for j in range(a.shape[1]):
                        records.append(dict(panel=panel,condition=condition,parameter=name,knot=j,
                                            z=p.z_knots[j],group=groups[j],**summary(a[:,j],tr[j])))
            for field in ("kappa_edge_fraction","zero_floor_fraction","N","selected_identities"):
                a=[r["result"][field] if r["status"]=="RAN-OK" else np.nan for r in rows]
                extras.append(dict(panel=panel,condition=condition,parameter=field,**summary(a)))
            for name in ("sigma_perm","sigma_trans","sigma_obs","sigma_trans2"):
                a=[r["result"]["nnls_zero_fraction"][name] if r["status"]=="RAN-OK" else np.nan for r in rows]
                extras.append(dict(panel=panel,condition=condition,parameter="zero_fraction_"+name,**summary(a)))
            if condition=="zero_temperament":
                a=np.array([get(r,"temper_s") if r["status"]=="RAN-OK" else np.nan for r in rows],float)
                extras.append(dict(panel=panel,condition=condition,parameter="s_positive",failed=int(np.isnan(a).sum()),
                                   **rate_interval(int(np.sum(a>1e-12)),int(np.isfinite(a).sum()))))
    pc.write(out/"recovery_summary.json",dict(records=records,extras=extras,failures=failures))
    pd.DataFrame(records).to_csv(out/"recovery_knots.csv",index=False)
    pd.DataFrame(extras).to_csv(out/"recovery_extras.csv",index=False)
    # Unweighted knot summaries, not independent replication counts.
    frame=pd.DataFrame(records);profile=frame[frame.knot.notna()]
    group=profile.groupby(["panel","condition","parameter","group"],sort=True).agg(
        knots=("knot","count"),mean_truth=("truth","mean"),mean_bias=("bias","mean"),
        mean_rmse=("rmse","mean"),mean_seed_sd=("sd","mean"),minimum_valid_seeds=("valid","min")).reset_index()
    group.to_csv(out/"recovery_groups.csv",index=False)
    return records,extras,failures

def separation(a,b):
    a=np.asarray(a,float);b=np.asarray(b,float)
    aa=a[np.isfinite(a)];bb=b[np.isfinite(b)]
    if min(len(aa),len(bb))<2:return dict(label="unavailable",n_a=len(aa),n_b=len(bb))
    # Conventional pooled sample SD, with actual denominators if missing.
    sd=np.sqrt(((len(aa)-1)*aa.var(ddof=1)+(len(bb)-1)*bb.var(ddof=1))/(len(aa)+len(bb)-2))
    qa=np.quantile(aa,[.05,.95]);qb=np.quantile(bb,[.05,.95]);delta=bb.mean()-aa.mean()
    snr=abs(delta)/sd if sd>0 else np.nan
    label="degenerate" if not sd>0 else "unresolved" if snr<1 else "separated" if snr>=2 and (qa[1]<qb[0] or qb[1]<qa[0]) else "overlapping"
    return dict(label=label,separation=snr,delta=delta,pooled_sd=sd,n_a=len(aa),n_b=len(bb),
                a_q05=qa[0],a_q95=qa[1],b_q05=qb[0],b_q95=qb[1])
def diagnostic_tables(out):
    tables=[];pairs=[];effects=[];family=[];failures=[]
    for panel in pc.PANELS:
        rows={c:load_rows(out,panel,"diagnostics",c,range(2000,2039) if c=="calibration" else range(1000,1020))
              for c in study.CONDITIONS+["calibration"]}
        failures += [dict(panel=panel,stage="diagnostics",condition=c,seed=r["seed"],traceback=r["traceback"]) for c,rr in rows.items() for r in rr if r["status"]!="RAN-OK"]
        keys=sorted({k for rr in rows.values() for r in rr if r["status"]=="RAN-OK" for k in r["result"]["features"]})
        data={c:{k:np.array([r["result"]["features"].get(k) if r["status"]=="RAN-OK" else np.nan for r in rr],float) for k in keys} for c,rr in rows.items()}
        anyflag={c:np.zeros(20,bool) for c in study.CONDITIONS}
        anyavailable={c:np.zeros(20,bool) for c in study.CONDITIONS}
        for key in keys:
            cal=data["calibration"][key];valid_cal=np.isfinite(cal)
            complete=valid_cal.sum()==39
            lo,hi=(cal.min(),cal.max()) if complete else (np.nan,np.nan)
            for c in study.CONDITIONS:
                a=data[c][key];valid=np.isfinite(a)
                flags=valid&((a<lo)|(a>hi)) if complete else np.zeros(20,bool)
                if complete:
                    anyflag[c]|=flags;anyavailable[c]|=valid
                tables.append(dict(panel=panel,condition=c,statistic=key,calibration_n=int(valid_cal.sum()),
                    envelope_low=lo,envelope_high=hi,**summary(a),
                    **{"detection_"+k:v for k,v in rate_interval(int(flags.sum()),int(valid.sum()) if complete else 0).items()}))
                for control in (["baseline","gaussian_control"] if c.startswith("log_volatility") else ["baseline"]):
                    base=data[control][key];paired=np.isfinite(base)&valid
                    basegood=base[np.isfinite(base)];sd=basegood.std(ddof=1) if len(basegood)>1 else np.nan
                    delta=a[paired]-base[paired]
                    effects.append(dict(panel=panel,condition=c,control=control,statistic=key,paired_n=int(paired.sum()),
                        control_sd=sd,difference=delta.mean() if len(delta) else np.nan,
                        standardized_difference=delta.mean()/sd if len(delta) and sd>0 else np.nan,
                        mcse_difference=delta.std(ddof=1)/np.sqrt(len(delta)) if len(delta)>1 else np.nan))
            for ca,cb in itertools.combinations(study.CONDITIONS,2):
                pairs.append(dict(panel=panel,condition_a=ca,condition_b=cb,statistic=key,
                                  **separation(data[ca][key],data[cb][key])))
        for c in study.CONDITIONS:
            family.append(dict(panel=panel,condition=c,**rate_interval(int(anyflag[c].sum()),int(anyavailable[c].sum()))))
    for name,table in (("diagnostic_rates",tables),("diagnostic_effects",effects),("diagnostic_pairs",pairs),("diagnostic_any_flag",family)):
        pd.DataFrame(table).to_csv(out/(name+".csv"),index=False)
    pc.write(out/"diagnostic_failures.json",failures)
    return tables,effects,pairs,family,failures
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--out",type=Path,required=True);args=ap.parse_args()
    recovery(args.out);diagnostic_tables(args.out)
    pc.write(args.out/"analysis_provenance.json",dict(sources=study.sources(),summary_sha256=pc.digest(__file__)))
if __name__=="__main__":main()
