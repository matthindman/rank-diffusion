"""Compare A10's saved temporal reconstruction against a matched-grid baseline."""
import sys,json,subprocess,time
from pathlib import Path
import numpy as np
sys.path.insert(0,str(Path.cwd()/'llm_fitting'))
import minimal_rankdiff as mrd
import residual_anatomy as ra
P=Path(__file__).resolve().parent
SOURCE='001fa74669cc7a135075b5b8d667b9a1d958ce14'
def ladder(X):
 a=np.sort(np.where(np.isfinite(X),X,-np.inf),axis=1)[:,::-1];a[~np.isfinite(a)]=np.nan
 return np.nanmedian(a[:,:int(np.isfinite(a).sum(axis=1).min())],axis=0)
name=sys.argv[1];K=5000 if name=='reddit_submissions_long' else 12500
start=time.time();print('Loading for matched-grid baseline:',name,flush=True)
df=mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[name]),K,buffer_mult=4)
_,X,_=ra.from_long(df);del df
h=len(X)//2;l1=ladder(X[:h]);l2=ladder(X[h:]);r=np.unique(np.round(np.exp(np.linspace(0,np.log(K),200))).astype(int))
e=l1[:K]-l2[:K]-(l1[K-1]-l2[K-1]);raw=json.loads(subprocess.check_output(['git','show',f'{SOURCE}:llm_fitting/runs/2026-10-12_A10_two_way/cells/a10__{name}.json']))
saved=raw['time_holdout'];b=float(np.sqrt(np.mean(e[r-1]**2)));uni=float(np.sqrt(np.mean(e**2)))
assert np.isclose(uni,saved['ladder_moved_rmse'],rtol=1e-10,atol=1e-10)
out=dict(panel=name,source=SOURCE,T=len(X),K=K,grid_size=len(r),carry_forward_rmse_log_grid=b,carry_forward_rmse_uniform_rank=uni,saved_variance_reconstruction_rmse=saved['second_from_first']['rmse'],matched_rmse_ratio=saved['second_from_first']['rmse']/b,seconds=time.time()-start)
(P/f'audit_a10_{name}.json').write_text(json.dumps(out,indent=2));print(json.dumps(out),flush=True)
