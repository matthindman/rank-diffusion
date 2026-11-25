"""Decompose the S2 denominator and compute the point no-change head Brier omitted by gold."""
from pathlib import Path
import json
import numpy as np
from diag_moments import gold, cr

def head_median(values, mask, K):
    vals=values[mask & np.isfinite(values)]
    count=min(K, len(vals))
    return float(np.median(np.partition(vals, len(vals)-count)[-count:]))

def run(name):
    d,sb=gold.load_cached(name)
    X,R,fl,K=d['X'].astype(float),d['R'].astype(float),d['floors'],int(d['K'])
    hs=gold.PANELS[name]['horizons']
    T,N=X.shape
    start=78 if T>=180 else (36 if T>=80 else 22)
    origins=np.unique(np.round(np.linspace(start,T-max(hs)-1,5)).astype(int))
    result=[]
    for split,T0 in enumerate(origins):
        r=R[:T0]; x=X[:T0]
        f=fl[:T0]
        p=cr.estimate_common(x,r,K,f,sb,refine_drift=True)
        filt=cr.filter_endpoints(x,r,p,f)
        last=filt['last_pmean']
        present=np.isfinite(r[-1])
        center=filt['L_hat']+p.phi*filt['xi_hat']
        rank=np.argsort(np.where(np.isfinite(r),r,f[:,None]).mean(axis=0),kind='stable')[:K]
        if name=='instagram_hm':
            rank=rank[np.isfinite(r[:,rank]).mean(axis=0)>=.5]
        near=rank[np.isfinite(r[-1,rank]) & (r[-1,rank]<=K//4)]
        no={}
        for h in hs:
            true=np.isfinite(R[T0-1+h,near]) & (R[T0-1+h,near]<=K//16)
            no[str(h)]=float(np.mean(true!=(r[-1,near]<=K//16)))
        c11=np.sqrt(np.maximum(filt['P11'],1e-10))
        c21=filt['P12']/c11
        c22=np.sqrt(np.maximum(filt['P22']-c21**2,1e-10))
        shifts=[]
        for m in range(39):
            rng=np.random.default_rng(2000*split+m)
            a,b=rng.standard_normal(N),rng.standard_normal(N)
            lv=np.log(filt['v_hat'])-.5*filt['post_s_v']**2+filt['post_s_v']*np.clip(rng.standard_normal(N),-2,2)
            st=cr.CommonState(p,filt['L_hat']+c11*a,rng,
                       xi0=filt['xi_hat']+c21*a+c22*b,v0=np.exp(np.clip(lv,-2.5*p.s_vol,2.5*p.s_vol)))
            init=st.L+p.phi*st.xi
            future=st.step(apply_absence=False)
            mask=(rng.random(N)>=cr.p_abs_of(p,st._ranks_and_z()[1])) if p.a_abs>-7.5 else np.ones(N,bool)
            shifts.append([head_median(center,present,K),head_median(center,mask,K),
                           head_median(init,mask,K),head_median(future,mask,K)])
        result.append(dict(T0=int(T0),medians_minus_training=(np.mean(shifts,axis=0)-last).tolist(),
                           brier_no=no))
    return result

if __name__=='__main__':
    out={}
    for name in gold.PANELS:
        out[name]=run(name)
        Path('diag_reference.json').write_text(json.dumps(out,indent=2))
        print(name,np.mean([v['medians_minus_training'] for v in out[name]],axis=0),flush=True)
