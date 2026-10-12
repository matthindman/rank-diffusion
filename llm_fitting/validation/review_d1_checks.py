"""Deterministic artifact checks for R1; no panel/model evaluation."""
import argparse,json,sys,hashlib,subprocess,io
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
import minimal_rankdiff as mrd
import community_metrics as cm
def coordinate_check():
    n,T=200,12
    df=pd.DataFrame([dict(entity_id=f"e{i:03d}",period=t,rank=i+1,
        X=10.-.01*i+.001*t,N=n,z=np.log((i+.5)/n)) for t in range(T) for i in range(n)])
    order=df.groupby("entity_id")["rank"].mean().sort_values(kind="mergesort").index
    full=mrd._change_panel(df)[-1]
    rows=[]
    for h in (set(order[0::2]),set(order[1::2])):
        sub=df[df.entity_id.isin(h)].copy();half=mrd._change_panel(sub)[-1]
        truth=full.reindex(half.index)
        unclip=half<0
        rows.append({"N_half":int(sub.groupby("period").size().mean()),
            "N_full":n,"half_clipped_fraction":float((half==0).mean()),
            "full_clipped_fraction":float((truth==0).mean()),
            "unclipped_coordinate_shift_min":float((half[unclip]-truth[unclip]).min()),
            "unclipped_coordinate_shift_max":float((half[unclip]-truth[unclip]).max()),
            "example_entity":half.index[24],"half_z":float(half.iloc[24]),"full_z":float(truth.iloc[24])})
    return rows
def drift_check():
    n,T=100,12
    activity=np.exp(np.linspace(16.,10.,n))
    factors=np.r_[np.ones(T//3),np.exp(.5)*np.ones(T//3),np.exp(1)*np.ones(T//3)]
    values=factors[:,None]*activity
    shares=values/values.sum(axis=1,keepdims=True)
    X=np.log1p(values)
    return {"ladder_drift":cm.ladder_drift(X),"max_share_change":float(np.max(np.abs(shares-shares[0]))),
            "identity_ranks_change":False,"common_activity_factor_last_vs_first":float(np.e)}
def pair_check(outdir):
    p=ROOT/"llm_fitting/runs/2026-10-12_V2_planted_mechanisms/diagnostic_pairs.csv.gz"
    # Read the committed V2 artifact, not a reconstructed or tuned contrast.
    raw=subprocess.check_output(["git","show","6772c1d8c8d7b94159052e9b051649bf6797b318:"+str(p.relative_to(ROOT))])
    df=pd.read_csv(io.BytesIO(raw),compression="gzip")
    q=df[(df.condition_a=="random_walk_1")&(df.condition_b=="ladder_attraction_1")].copy()
    if len(q)==0:raise RuntimeError("Missing declared V2 pair")
    q.to_csv(outdir/"V2_STRONG_PAIR_ALL.csv",index=False)
    counts=q.groupby(["panel","label"]).size().rename("count").reset_index()
    counts.to_csv(outdir/"V2_STRONG_PAIR_COUNTS.csv",index=False)
    # Full list retained, not just the strongest examples.
    sep=q[q.label=="separated"].sort_values(["panel","statistic"])
    sep.to_csv(outdir/"V2_STRONG_PAIR_SEPARATED.csv",index=False)
    return {"source":str(p.relative_to(ROOT)),"commit":"6772c1d8c8d7b94159052e9b051649bf6797b318","sha256":hashlib.sha256(raw).hexdigest(),
            "rows":len(q),"counts":counts.to_dict(orient="records"),
            "separated_rows":len(sep),"rule":"V2 fixed descriptive labels; no new test or composite-nuisance refit"}
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--out",type=Path,required=True);a=ap.parse_args()
    out={"rank_coordinate":coordinate_check(),"drift":drift_check(),"v2_pair":pair_check(a.out.parent),
        "commit":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
        "source_hashes":{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            (ROOT/"llm_fitting/phase1_a4.py",ROOT/"llm_fitting/minimal_rankdiff.py",
             ROOT/"llm_fitting/community_metrics.py",Path(__file__))}}
    a.out.write_text(json.dumps(out,indent=2)+"\n");print(json.dumps(out,indent=2))
if __name__=="__main__":main()
