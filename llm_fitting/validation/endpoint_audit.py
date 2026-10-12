"""Validate the full V3 export before scoring; no model refits."""
import argparse,json,sys
from pathlib import Path
import numpy as np
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
import rankdiff_kalman as rk
from validation import gate_instrument as gi
PANELS={"facebook_a":(86,3500),"reddit_comments":(136,12500),
        "reddit":(30,5000),"reddit_submissions_long":(212,5000)}
def audit(root):
    entries=[]
    for panel,(T,K) in PANELS.items():
        complete=json.loads((root/"runs"/panel/"COMPLETE.json").read_text())
        assert complete["status"]=="RAN-OK" and complete["fixed"]["reps"]==20
        assert complete["result"]["n_splits"]==5
        for f,digest in complete["export_files"].items():assert gi.sha_file(f)==digest
        origins,L=rk._gate_windows(T,5)
        for t0 in origins:
            f=root/"exports"/panel/f"{panel}__T0_{t0:04d}.npz"
            b=f.with_name(f.stem+"__baselines.npz")
            with np.load(f,allow_pickle=False) as z:a=dict(z)
            with np.load(b,allow_pickle=False) as z:base=dict(z)
            n=len(a["entity_id"])
            assert len(set(a["entity_id"]))==n and np.array_equal(a["entity_id"],base["entity_id"])
            assert int(a["T0"])==t0 and int(a["test_len"])==L and int(a["top_k"])==K
            assert a["sim_rank"].shape==(20,L,n) and a["obs_rank"].shape==(L,n)
            assert np.array_equal(a["seeds"],np.arange(20))
            assert np.all(np.isfinite(a["train_perm_rank"])) and np.all(a["train_perm_rank"]>=1)
            assert np.all((a["train_presence"]>=0)&(a["train_presence"]<=1))
            assert np.all((a["sim_rank"]>=0)&(a["sim_rank"]<=n))
            assert np.all((a["obs_rank"]>=0)&(a["obs_rank"]<=n))
            assert not np.any((a["sim_rank"][:,:-1]==0)&(a["sim_rank"][:,1:]!=0))
            for row in np.reshape(a["sim_rank"],(-1,n)):
                positive=row[row>0];assert len(np.unique(positive))==len(positive)
            for key,r in base.items():
                if key=="entity_id":continue
                assert r.shape==(n,20)
                if key.endswith("__rank"):
                    assert np.all((r>=1)&(r<=K+1))
                    evkey=key[:-4]+"event"
                    if evkey in base:assert np.all(base[evkey][r>K])
                else:assert np.all((r==0)|(r==1))
            entries.append({"panel":panel,"T0":t0,"test_len":L,"members":n,
                "export_sha256":gi.sha_file(f),"baseline_sha256":gi.sha_file(b),
                "historical_horizons":[h for h in (1,4,13) if f"historical_mobility__h{h}__rank" in base],
                "kernel_horizons":[h for h in (1,4,13) if f"rank_bin__h{h}__rank" in base]})
    return {"status":"PASS","splits":len(entries),"members":20,"entries":entries}
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--root",type=Path,required=True)
    ap.add_argument("--out",type=Path,required=True);a=ap.parse_args()
    result=audit(a.root);gi.write_json(a.out,result);print({"status":result["status"],"splits":result["splits"]})
if __name__=="__main__":main()
