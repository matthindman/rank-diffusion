"""Cross-fit slow and fast variance by early/late volatility, inside permanent-rank strata."""
import json
from pathlib import Path
from scipy.optimize import nnls
import numpy as np
from diag_moments import cr,gold

def estimate(x,phi):
    T=len(x)
    hs=np.arange(1,min(T//2,26)+1)
    y=[]
    for h in hs:
        d=x[h:]-x[:-h]
        d-=np.nanmean(d,axis=1,keepdims=True)
        y.append(np.nansum(d*d)/max(1,np.isfinite(d).sum()-len(d)))
    A=np.column_stack([np.ones(len(hs)),1-phi**hs,hs])
    c=nnls(A,y)[0]
    return c[2],y[0]

def run(name):
    d,sb=gold.load_cached(name)
    X,R,K,fl=d['X'].astype(float),d['R'].astype(float),int(d['K']),d['floors']
    idx=np.argsort(np.where(np.isfinite(R),R,fl[:,None]).mean(axis=0),kind='stable')[:K]
    x=(X-cr._top_k_center(X,R,K))[:,idx]
    half=len(x)//2
    p=cr.estimate_common(X,R,K,fl,sb,refine_drift=False)
    out=[]
    for tr,te in [(x[:half],x[half:]),(x[half:],x[:half])]:
        diff=np.diff(tr,axis=0)
        diff-=np.nanmean(diff,axis=1,keepdims=True)
        score=np.nanmedian(np.abs(diff),axis=0)
        for lo,hi in [(0,K//16),(K//16,K//4),(K//4,K)]:
            ok=(np.arange(K)>=lo)&(np.arange(K)<hi)&(np.isfinite(tr).mean(axis=0)>=.5)&(np.isfinite(te).mean(axis=0)>=.5)&np.isfinite(score)
            ids=np.flatnonzero(ok)
            qs=np.array_split(ids[np.argsort(score[ids])],3)
            out.append(dict(lo=lo,hi=hi,moments=[estimate(te[:,i],p.phi) for i in qs]))
    return out
if __name__=='__main__':
    out={}
    for n in gold.PANELS:
        out[n]=run(n)
        m=np.array([a['moments'] for a in out[n]])
        print(n,'high/low (q,D1)',m[:,2].mean(axis=0)/m[:,0].mean(axis=0),'band/half q',m[:,:,0],flush=True)
        Path('diag_lasting.json').write_text(json.dumps(out,indent=2))
