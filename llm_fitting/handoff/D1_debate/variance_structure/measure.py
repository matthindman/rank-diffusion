"""Standalone descriptive diagnostics. No estimator/simulator calls."""
import os
os.environ.setdefault('PYTHONDONTWRITEBYTECODE','1')
import sys, json, subprocess, hashlib, time
from pathlib import Path
import numpy as np
from scipy.stats import spearmanr
ROOT=Path.cwd()
OUT=ROOT/'llm_fitting/handoff/D1_debate/variance_structure'
sys.path.insert(0,str(ROOT/'llm_fitting'))
import minimal_rankdiff as mrd
import residual_anatomy as ra
PANELS={'facebook_a':3500,'reddit_comments':12500,'reddit_submissions_long':5000,'reddit_comments_ext':12500}
SOURCE='c7c1767fc6aa5ddf48278b43567d9b000190c88a'
def fetch(panel):
    path=f'llm_fitting/runs/2026-10-12_A9_variance_ladder/cells/a9__{panel}.json'
    return json.loads(subprocess.check_output(['git','show',f'{SOURCE}:{path}']))
def summary(rows,vkey='var_rate',skey='slope'):
    v=np.array([r[vkey] for r in rows]); s=np.array([r[skey] for r in rows])
    if np.any(v<=0) or np.any(s<=0): return {'unavailable':'nonpositive rate or slope','n':len(rows)}
    rho=v/(2*s); g=np.median(rho); slo=v/(2*g); vlo=2*g*s
    lobo=np.array([np.median(np.delete(rho,i)) for i in range(len(rho))])
    return dict(n=len(rows),g=float(g),rho_cv=float(rho.std(ddof=1)/rho.mean()),spearman=float(spearmanr(v,s).statistic),
      slope_mare=float(np.mean(abs(slo/s-1))),variance_mare=float(np.mean(abs(vlo/v-1))),
      slope_mdare=float(np.median(abs(slo/s-1))),variance_mdare=float(np.median(abs(vlo/v-1))),
      lobo_slope_mare=float(np.mean(abs(v/(2*lobo*s)-1))),lobo_variance_mare=float(np.mean(abs(2*lobo*s/v-1))))
def a9():
    result={}
    for p in PANELS:
        c=fetch(p); rows=c['bins']; b=rows[1:]
        common=[r for r in rows if r['lo']>=c['K']/128-1e-8]
        result[p]={'all_but_top':summary(b),'seven_octaves':summary(common),'source':SOURCE,'bins':rows}
    (OUT/'a9_bidirectional.json').write_text(json.dumps(result,indent=2))
    print(json.dumps({k:{x:y for x,y in v.items() if x!='bins'} for k,v in result.items()},indent=2))
def slopes(ladder,lo,hi):
    r=np.arange(int(np.floor(lo))+1,int(np.floor(hi))+1); r=r[(r>=1)&(r<=len(ladder))]
    return float(-np.polyfit(np.log(r),ladder[r-1],1)[0]) if len(r)>=3 else float('nan')
def calc(d1,d2,delta,rng):
    mask=np.isfinite(d1)&np.isfinite(d2)
    A=np.where(mask,d1,0); B=np.where(mask,d2,0)
    n=mask.sum(axis=1); s1=A.sum(axis=1); s2=B.sum(axis=1); q1=(A*A).sum(axis=1); q2=(B*B).sum(axis=1)
    def rate(idx):
        N=n[idx].sum(); m1=s1[idx].sum()/N; m2=s2[idx].sum()/N
        return ((q2[idx].sum()/N-m2*m2)-(q1[idx].sum()/N-m1*m1))/delta
    full=np.arange(len(n)); point=rate(full)
    # Drift component of total variance, weighted by actual paired origins.
    ni=mask.sum(axis=0); good=ni>0; wi=ni[good]/ni.sum()
    m1i=A[:,good].sum(axis=0)/ni[good];m2i=B[:,good].sum(axis=0)/ni[good]
    between=(np.sum(wi*(m2i-np.sum(wi*m2i))**2)-np.sum(wi*(m1i-np.sum(wi*m1i))**2))/delta
    block=delta+13; boots=[]
    for _ in range(400):
        starts=rng.integers(0,len(n),size=int(np.ceil(len(n)/block)))
        idx=((starts[:,None]+np.arange(block))%len(n)).ravel()[:len(n)]
        boots.append(rate(idx))
    return dict(var_rate=float(point),block_q05=float(np.quantile(boots,.05)),block_q95=float(np.quantile(boots,.95)),
        between_endpoint_rate=float(between),within_endpoint_rate=float(point-between),
        paired_observations=int(mask.sum()),paired_fraction=float(mask.mean()),starts=len(n),block=block)
def run(panel):
    start=time.time(); K=PANELS[panel]; cfg=mrd.PLATFORMS[panel]
    print('Loading',panel,flush=True)
    df=mrd.restrict_universe(mrd.load_panel(cfg),K,buffer_mult=4)
    R,X,_=ra.from_long(df); del df
    T=len(X);pr,pres=ra.perm_rank(R,0,T,None);base=(pr<=K)&(pres>=.7);Y=ra.defactor(X,base)
    order=np.sort(np.where(np.isfinite(X),X,-np.inf),axis=1)[:,::-1];order[~np.isfinite(order)]=np.nan
    L=int(np.isfinite(order).sum(axis=1).min()); order=order[:,:L]
    median=np.nanmedian(order,axis=0);mean=np.nanmean(order,axis=0)
    bins=fetch(panel)['bins']; out={'panel':panel,'K':K,'T':T,'N':X.shape[1],'base':int(base.sum()),'input':cfg['path'],'source_commit':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'variants':{}}
    for h2 in ([26] if panel=='facebook_a' else [52,65]):
        H=T-h2; d1=Y[13:13+H]-Y[:H];d2=Y[h2:h2+H]-Y[:H];rows=[]
        for old in bins:
            lo,hi=old['lo'],old['hi'];keep=base&(pr>lo)&(pr<=hi);n=int(keep.sum())
            if n<5: continue
            row=dict(lo=lo,hi=hi,mid=old['mid'],n=n,slope=slopes(median,lo,hi),mean_slope=slopes(mean,lo,hi))
            row.update(calc(d1[:,keep],d2[:,keep],h2-13,np.random.default_rng(20261012)))
            row['rho']=row['var_rate']/(2*row['slope']);rows.append(row)
        common=[r for r in rows if r['lo']>=K/128-1e-8]
        out['variants'][str(h2)]={'bins':rows,'all_but_top':summary(rows[1:]),'seven_octaves':summary(common),'mean_ladder_seven':summary(common,skey='mean_slope')}
    out['seconds']=time.time()-start
    (OUT/f'measure_{panel}.json').write_text(json.dumps(out,indent=2))
    print(json.dumps({k:v for k,v in out.items() if k!='variants'}),flush=True)
    for h,v in out['variants'].items():print(h,json.dumps(v['seven_octaves']),flush=True)
if __name__=='__main__':
    if sys.argv[1]=='a9': a9()
    else: run(sys.argv[1])
