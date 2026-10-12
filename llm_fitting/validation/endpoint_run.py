"""Frozen-gate endpoint export with restartable draws and training-only baselines."""
from __future__ import annotations
import argparse,contextlib,dataclasses,hashlib,json,os,subprocess,sys,time,uuid
from pathlib import Path
from unittest.mock import patch
import numpy as np
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
import minimal_rankdiff as mrd
import rankdiff_kalman as rk
import ladder_explainer as le
from validation import gate_instrument as gi
from validation import endpoint_baselines as eb
sys.path.insert(0,str(ROOT/"scripts"))
from sealed_data_guard import require_released

def sources():
    paths=["minimal_rankdiff.py","rankdiff_kalman.py","ladder_explainer.py",
           "validation/gate_instrument.py","validation/endpoint_run.py","validation/endpoint_baselines.py",
           "validation/endpoint_scores.py"]
    return {"llm_fitting/"+p:gi.sha_file(ROOT/"llm_fitting"/p) for p in paths}
def parameter_hash(p):
    h=hashlib.sha256()
    for f in dataclasses.fields(p):
        v=getattr(p,f.name);h.update(f.name.encode())
        if isinstance(v,np.ndarray):
            h.update(str(v.dtype).encode());h.update(str(v.shape).encode());h.update(v.tobytes())
        else:h.update(repr(v).encode())
    return h.hexdigest()
def atomic_npz(path,**arrays):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(".tmp")
    with tmp.open("wb") as f:np.savez_compressed(f,**arrays)
    os.replace(tmp,path)

class EndpointWrapper(gi.BankWrapper):
    def __init__(self,out,exports,input_hashes=None):
        super().__init__(out,0,False,input_hashes)
        self.exports=Path(exports)
        self.original_export=rk._write_endpoint_export
        self.baseline_metadata=[]
    def bank(self,p,*args,**kwargs):
        original=self.input_hashes
        self.input_hashes={**original,"full_parameters":parameter_hash(p),
                          "endpoint_wrapper_sources":sources()}
        try:return super().bank(p,*args,**kwargs)
        finally:self.input_hashes=original
    def sim_test(self,p,df_tr,T,horizons,reps=3,**kwargs):
        dest=self.out/str(self.state["origin"])/"endpoint_test"
        dest.mkdir(parents=True,exist_ok=True)
        cfg={"origin":self.state["origin"],"T":T,"reps":reps,
             "parameters":parameter_hash(p),"sources":sources(),"inputs":self.input_hashes,
             "cond_home":kwargs.get("cond_home","state"),"vhat":kwargs.get("vhat") is not None}
        config=dest/"config.json"
        if config.exists() and gi.sha_file(config)!=hashlib.sha256((json.dumps(cfg,indent=2,allow_nan=True)+"\n").encode()).hexdigest():
            raise RuntimeError("Conditional draw cache changed")
        if not config.exists():gi.write_json(config,cfg)
        records=[]
        for seed in range(reps):
            path=dest/f"seed_{seed:04d}.npz"
            if path.exists():
                with np.load(path,allow_pickle=False) as z:a=dict(z)
            else:
                exp={}
                cr,ti=rk.sim_cohort_conditional(p,df_tr,T,kwargs.get("kappa",0.),seed=seed,
                    vhat=kwargs.get("vhat"),cond_home=kwargs.get("cond_home","state"),export=exp)
                a=dict(cr=cr,ti=ti,rank=exp["rank"],entity_id=np.asarray(exp["entity_id"]).astype(str))
                atomic_npz(path,**a)
            if kwargs.get("export") is not None:
                kwargs["export"].append(dict(rank=a["rank"],entity_id=a["entity_id"]))
            d,rf=rk._rank_dist(a["cr"],horizons)
            cc=rk._collisions_from_topids(a["ti"],(1,5,20))
            records.append(gi.pack_rep(d,rf,cc))
        return gi.pool_records(records,horizons)
    def export(self,out_dir,platform,T0,test_len,top_k,df_tr,df_te,reps,stack):
        path=self.original_export(out_dir,platform,T0,test_len,top_k,df_tr,df_te,reps,stack)
        ids=reps[0]["entity_id"]
        terminal,events,meta=eb.baseline_samples(df_tr,ids,top_k,m=len(reps))
        arrays={"entity_id":np.asarray(ids).astype(str)}
        arrays.update({f"{m}__h{h}__rank":a for (m,h),a in terminal.items()})
        arrays.update({f"{m}__h{h}__event":a for (m,h),a in events.items()})
        atomic_npz(self.exports/f"{platform}__T0_{T0:04d}__baselines.npz",**arrays)
        self.baseline_metadata.append({"panel":platform,"T0":T0,"horizons":meta})
        gi.write_json(self.out/str(T0)/"baseline_metadata.json",self.baseline_metadata[-1])
        return path
    @contextlib.contextmanager
    def install(self):
        with super().install(),patch.object(rk,"_write_endpoint_export",self.export):yield self

def run(panel,out,exports):
    cfg=le.PANELS[panel];dest=out/panel;dest.mkdir(parents=True,exist_ok=True)
    paths=[mrd.PLATFORMS[panel]["path"]]
    if panel=="facebook_a":paths.append(mrd.PLATFORMS[panel]["daily_path"])
    for p in paths:
        require_released(p)
        if not Path(p).exists():raise FileNotFoundError(p)
    fixed={"panel":panel,"sources":sources(),"inputs":{p:gi.sha_file(p) for p in paths},
           "reps":20,"boot":2000,"seeds":list(range(20))}
    complete=dest/"COMPLETE.json"
    if complete.exists():
        old=json.loads(complete.read_text())
        if old["fixed"]!=fixed:raise RuntimeError("Completed panel inputs changed")
        for path,digest in old["export_files"].items():
            if not Path(path).exists() or gi.sha_file(path)!=digest:
                raise RuntimeError("Completed export missing or changed: "+path)
        print("Already complete",panel);return
    attempt={"id":str(uuid.uuid4()),"status":"STARTED","fixed":fixed,
             "commit":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
             "command":sys.argv,"started":time.time()}
    journal=dest/"attempts";journal.mkdir(exist_ok=True)
    path=journal/(attempt["id"]+".json");gi.write_json(path,attempt)
    try:
        cache=le.PanelCache()
        full=mrd.load_panel(mrd.PLATFORMS[panel])
        T=int(full.period.max())+1;origins,L=rk._gate_windows(T,5)
        if cfg["slim"]:
            for t0 in origins:mrd.restrict_universe(full,cfg["K"],buffer_mult=4,member_window=t0)
            cache.slim(mrd.PLATFORMS[panel]);del full
        wrapper=EndpointWrapper(dest/"cache",exports/panel,fixed["inputs"])
        with (dest/"gate.log").open("w") as log,contextlib.redirect_stdout(le._Tee(sys.stdout,log)),wrapper.install():
            result=rk.oos_movement(panel,n_splits=5,reps=20,boot=2000,top_k=cfg["K"],
                buffer_mult=4,temper=True,min_knot_n=8,md_lags=6,t_tails=True,
                spec_b=panel=="facebook_a",conditional="state",
                mix_hetero=panel=="reddit_comments",dist_scores=False,
                endpoint_export=str(exports/panel))
        cache.check_unmutated()
        final={**attempt,"status":"RAN-OK","result":result,"splits":wrapper.splits,
               "baseline_metadata":wrapper.baseline_metadata,
               "export_files":{str(p):gi.sha_file(p) for p in sorted((exports/panel).glob("*.npz"))},
               "seconds":time.time()-attempt["started"]}
        gi.write_json(complete,final);gi.write_json(path,final)
    except BaseException as exc:
        gi.write_json(path,{**attempt,"status":"RAN-FAILED","error":repr(exc),"seconds":time.time()-attempt["started"]})
        raise
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--panel",choices=gi.PANELS,required=True)
    ap.add_argument("--out",type=Path,required=True);ap.add_argument("--exports",type=Path,required=True)
    a=ap.parse_args();os.chdir(ROOT);run(a.panel,a.out,a.exports)
if __name__=="__main__":main()
