"""Isolate finite-member error, random denominator and rank drift on the original S2."""
import json
from pathlib import Path
import warnings
import numpy as np
from diag_moments import gold, cr

warnings.filterwarnings('ignore', category=RuntimeWarning)

def one_origin(X,R,fl,K,sb,T0,seed):
    x,r,floors=X[:T0],R[:T0],fl[:T0]
    p=cr.estimate_common(x,r,K,floors,sb,refine_drift=True)
    f=cr.filter_endpoints(x,r,p,floors)
    pr=np.argsort(np.where(np.isfinite(r),r,floors[:,None]).mean(axis=0),kind="stable")[:K]
    truth=X[T0]-cr._top_k_center(X[T0:T0+1],R[T0:T0+1],K)[0,0]
    sel=pr[np.isfinite(truth[pr]) & np.any(np.isfinite(x[:,pr]),axis=0)]
    last=f['last_pmean']
    pred=gold._analytical_kalman_point(f,p,1,last)[sel]
    c11=np.sqrt(np.maximum(f['P11'],1e-10))
    c21=f['P12']/c11
    c22=np.sqrt(np.maximum(f['P22']-c21*c21,1e-10))
    N=len(c11)
    raw=[]; med=[]; delta_g=[]
    for m in range(195):
        rng=np.random.default_rng(seed+m)
        a,b=rng.standard_normal(N),rng.standard_normal(N)
        lv=np.log(f['v_hat'])-.5*f['post_s_v']**2+f['post_s_v']*np.clip(rng.standard_normal(N),-2,2)
        vi=np.exp(np.clip(lv,-2.5*p.s_vol,2.5*p.s_vol))
        state=cr.CommonState(p,f['L_hat']+c11*a,rng,xi0=f['xi_hat']+c21*a+c22*b,v0=vi)
        z=state._ranks_and_z()[1]
        delta_g.append(np.mean(np.interp(z[sel],p.drift_z,p.drift_g)))
        val=state.step(apply_absence=False)
        observed=val
        if p.a_abs>-7.5:
            z=state._ranks_and_z()[1]
            observed=val[rng.random(N)>=cr.p_abs_of(p,z)]
        obs=np.sort(observed)[::-1][:K]
        med.append(float(np.median(obs))-last)
        raw.append(np.where(state.ids[sel]==sel,val[sel]-last,np.nan))
    raw=np.array(raw)
    med=np.asarray(med)
    y=truth[sel]
    out=dict(T0=T0,denom=np.mean(med),sd_denom=np.std(med),mean_g=np.mean(delta_g),
             mae_kal=np.mean(abs(pred-y)),exited=np.mean(~np.isfinite(raw)),
             mean_raw_minus_kal=np.nanmean(raw-pred))
    for M in [39,195]:
        fixed=np.nanmedian(raw[:M],axis=0)
        future=np.nanmedian(raw[:M]-med[:M,None],axis=0)
        out.update({f'mae_fixed_{M}':np.mean(abs(fixed-y)),f'mae_future_{M}':np.mean(abs(future-y)),
                    f'raw_med_minus_kal_{M}':np.mean(fixed-pred)})
    return out

if __name__=='__main__':
    out={}
    for name,cfg in gold.PANELS.items():
        d,sb=gold.load_cached(name)
        X,R,fl,K=d['X'].astype(float),d['R'].astype(float),d['floors'],int(d['K'])
        T=len(X)
        lo=78 if T>=180 else (36 if T>=80 else 22)
        origins=np.unique(np.round(np.linspace(lo,T-max(cfg['horizons'])-1,5)).astype(int))
        out[name]=[one_origin(X,R,fl,K,sb,int(T0),2000*i) for i,T0 in enumerate(origins)]
        Path('diag_forecast.json').write_text(json.dumps(out,indent=2))
        print(name,{k:round(float(np.mean([r[k] for r in out[name]])),5) for k in out[name][0]},flush=True)
