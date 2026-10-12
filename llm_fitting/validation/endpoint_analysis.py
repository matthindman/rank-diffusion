"""Aggregate supplementary endpoint scores without treating endpoints as replicates."""
import argparse,json,sys
from functools import lru_cache
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import endpoint_scores as es
HORIZONS=(1,4,13)
METHODS=("frozen_model","no_change","historical_mobility","rank_bin")
def scalar_rows(x,y,masks,metric,common):
    if metric=="crps":
        score,jack=es.crps_jackknife(x,y)
    elif metric=="brier":
        score,jack=es.brier_jackknife(x,y)
    else:raise ValueError(metric)
    rows=[]
    for group,mask in masks.items():
        n=int(mask.sum())
        if not n:continue
        j=jack[mask].mean(axis=0)
        row={**common,"group":group,"metric":metric,"n":n,"value":float(score[mask].mean()),
             "simulation_mcse":float(es.mcse_from_jackknife(j))}
        row.update({f"jack_{k}":float(v) for k,v in enumerate(j)})
        rows.append(row)
    return rows
def calibration_rows(x,y,masks,common):
    rows=[]
    for level,values in es.percentile_calibration(x,y).items():
        for group,mask in masks.items():
            n=int(mask.sum())
            if not n:continue
            for metric in ("rank_cdf","below_quantile","at_or_below_quantile"):
                rows.append({**common,"group":group,"metric":metric,"level":level,"n":n,
                    "value":float(values[metric][mask].mean()),
                    "continuous_order_statistic_reference":values["continuous_order_statistic_reference"]})
    return rows
def score_split(export_path,baseline_path):
    with np.load(export_path,allow_pickle=False) as z: a=dict(z)
    with np.load(baseline_path,allow_pickle=False) as z: b=dict(z)
    if not np.array_equal(a["entity_id"],b["entity_id"]):raise RuntimeError("Entity alignment")
    K=int(a["top_k"]);T0=int(a["T0"]);panel=str(a["platform"])
    if len(a["seeds"])!=20 or not np.array_equal(a["seeds"],np.arange(20)):raise RuntimeError("Expected frozen seeds 0..19")
    masks=es.cohort_masks(a["train_perm_rank"],a["train_presence"],a["origin_rank"],K)
    rows=[];cal=[]
    for h in HORIZONS:
        if h>len(a["obs_rank"]):continue
        y=es.coarsened_log_rank(a["obs_rank"][h-1],K)
        event_y=es.outside_within(a["obs_rank"],K,h).astype(float)
        for method in METHODS:
            if method=="frozen_model":
                ranks=a["sim_rank"][:,h-1,:].T
                event=es.outside_within(a["sim_rank"],K,h).T.astype(float)
            else:
                rankkey=f"{method}__h{h}__rank";eventkey=f"{method}__h{h}__event"
                if rankkey not in b:continue
                ranks=b[rankkey];event=b[eventkey].astype(float) if eventkey in b else None
            x=es.coarsened_log_rank(ranks,K)
            common={"panel":panel,"T0":T0,"horizon":h,"method":method}
            rows+=scalar_rows(x,y,masks,"crps",common)
            cal+=calibration_rows(x,y,masks,common)
            if event is not None:
                rows+=scalar_rows(event,event_y,masks,"brier",common)
                for group,mask in masks.items():
                    if not mask.any():continue
                    for metric,val in (("event_probability",event.mean(axis=1)),("event_frequency",event_y)):
                        cal.append({**common,"group":group,"metric":metric,"level":-1.,
                            "n":int(mask.sum()),"value":float(val[mask].mean()),
                            "continuous_order_statistic_reference":np.nan})
    return rows,cal
@lru_cache(maxsize=None)
def block_scheme(origins,block):
    return es.calendar_block_weights(origins,block=block,reps=2000)
def band(values,origins,block):
    res=block_scheme(tuple(int(x) for x in origins),block)
    if res is None:
        return {"status":"WITHHELD: insufficient calendar support","lower":None,"upper":None}
    z=res["weights"]@np.asarray(values,float)
    return {"status":"descriptive conditional calendar blocks","lower":float(np.quantile(z,.025)),
            "upper":float(np.quantile(z,.975)),"empty_draws":res["empty_draws"],
            "span_weeks":res["span_weeks"]}
def aggregate(scores,calibration):
    summaries=[];comparisons=[];cal=[]
    for key,g in scores.groupby(["panel","horizon","group","metric","method"],sort=True):
        g=g.sort_values("T0");row=dict(zip(("panel","horizon","group","metric","method"),key))
        vals=g.value.to_numpy();jack=g[[f"jack_{j}" for j in range(20)]].to_numpy().mean(axis=0)
        row.update(value=float(vals.mean()),origin_sd=float(vals.std(ddof=1)),origins=len(g),
                   endpoints_min=int(g.n.min()),endpoints_max=int(g.n.max()),
                   simulation_mcse=float(es.mcse_from_jackknife(jack)))
        for block in (13,26):
            row.update({f"block{block}_{k}":v for k,v in band(vals,g.T0,block).items()})
        summaries.append(row)
    model=scores[scores.method=="frozen_model"]
    for baseline in METHODS[1:]:
        base=scores[scores.method==baseline]
        paired=model.merge(base,on=["panel","T0","horizon","group","metric"],suffixes=("_model","_base"))
        if (paired.n_model!=paired.n_base).any():raise RuntimeError("Comparison cases differ")
        paired["difference"]=paired.value_base-paired.value_model
        for key,g in paired.groupby(["panel","horizon","group","metric"],sort=True):
            g=g.sort_values("T0");row=dict(zip(("panel","horizon","group","metric"),key))
            d=g.difference.to_numpy()
            j=np.array([(g[f"jack_{i}_base"]-g[f"jack_{i}_model"]).mean() for i in range(20)])
            bv=float(g.value_base.mean());mv=float(g.value_model.mean())
            row.update(baseline=baseline,model=mv,baseline_score=bv,improvement=float(d.mean()),
                relative_skill=1-mv/bv if bv>0 else None,origin_sd=float(d.std(ddof=1)),
                origins=len(g),origin_list=",".join(map(str,g.T0.tolist())),
                endpoints_min=int(g.n_model.min()),endpoints_max=int(g.n_model.max()),
                simulation_mcse=float(es.mcse_from_jackknife(j)))
            for block in (13,26):
                row.update({f"block{block}_{k}":v for k,v in band(d,g.T0,block).items()})
            comparisons.append(row)
    for key,g in calibration.groupby(["panel","horizon","group","method","metric","level"],sort=True):
        g=g.sort_values("T0");row=dict(zip(("panel","horizon","group","method","metric","level"),key))
        row.update(value=float(g.value.mean()),origin_sd=float(g.value.std(ddof=1)),origins=len(g),
                   continuous_order_statistic_reference=float(g.continuous_order_statistic_reference.iloc[0]))
        for block in (13,26):
            row.update({f"block{block}_{k}":v for k,v in band(g.value,g.T0,block).items()})
        cal.append(row)
    return pd.DataFrame(summaries),pd.DataFrame(comparisons),pd.DataFrame(cal)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--exports",type=Path,required=True)
    ap.add_argument("--out",type=Path,required=True)
    args=ap.parse_args();args.out.mkdir(parents=True,exist_ok=True)
    rows=[];cal=[];manifest={}
    from validation.gate_instrument import sha_file
    files=sorted(p for p in args.exports.glob("*/*__T0_*.npz") if "__baselines" not in p.name)
    if len(files)!=20:raise RuntimeError(f"Expected20 split files, found{len(files)}")
    for p in files:
        b=p.with_name(p.stem+"__baselines.npz")
        r,c=score_split(p,b);rows+=r;cal+=c
        manifest[str(p)]=sha_file(p);manifest[str(b)]=sha_file(b)
    scores=pd.DataFrame(rows);calibration=pd.DataFrame(cal)
    summary,comparisons,cal=aggregate(scores,calibration)
    for name,table in (("origin_scores",scores),("origin_calibration",calibration),
                       ("scores",summary),("comparisons",comparisons),("calibration",cal)):
        if "simulation_mcse" in table:
            mean_col="improvement" if name=="comparisons" else "value"
            table["mc95_lower"]=table[mean_col]-1.96*table.simulation_mcse
            table["mc95_upper"]=table[mean_col]+1.96*table.simulation_mcse
        table.insert(0,"evidence","supplementary, retrospective, development data")
        table.to_csv(args.out/(name+".csv"),index=False)
    manifest["sources"]={str(p):sha_file(p) for p in (Path(__file__),ROOT/"llm_fitting/validation/endpoint_scores.py")}
    (args.out/"analysis_manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
    print({"splits":len(files),"score_rows":len(scores),"comparison_rows":len(comparisons),"calibration_rows":len(cal)})
if __name__=="__main__":main()
