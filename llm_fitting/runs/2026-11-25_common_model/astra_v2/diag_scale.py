"""What variance of a typical entity-week is predictable from the previous half?"""
from diag_moments import gold, cr
import numpy as np
import json
from pathlib import Path

def scores(x,z):
    # log(rank) removes smooth between-stratum differences when testing serial repeatability
    A=np.column_stack([np.ones(len(z)),z,z*z])
    m=[]
    for v in x:
        ok=np.isfinite(v)&(v>1e-9)
        y=np.full_like(v,np.nan)
        y[ok]=np.log(v[ok])-A[ok]@np.linalg.lstsq(A[ok],np.log(v[ok]),rcond=None)[0]
        m.append(y)
    ok=np.all(np.isfinite(m),axis=0)
    return np.round(np.corrcoef(np.array(m)[:,ok]),4).tolist()

def run(name):
    d,sb=gold.load_cached(name)
    X,R,K,fl=d['X'].astype(float),d['R'].astype(float),int(d['K']),d['floors']
    order=np.argsort(np.where(np.isfinite(R),R,fl[:,None]).mean(axis=0),kind='stable')[:K]
    p=cr.estimate_common(X,R,K,fl,sb,refine_drift=False)
    z=np.log((np.arange(K)+.5)/X.shape[1])
    x=(X-cr._top_k_center(X,R,K))[:,order]
    q=cr.sigma_L_of(p,np.nanmean(X[:,order],axis=0))**2
    r=2*cr.sigma_obs_of(p,z)**2+2*(1-p.phi)*cr.v_xi_of(p,z)
    vs=[]
    masks=[np.arange(K)<100,np.arange(K)<K]
    result={}
    for side,y in enumerate([x[:len(x)//2],x[len(x)//2:]]):
        dif=np.diff(y,axis=0)
        dif-=np.nanmean(dif,axis=1,keepdims=True)
        ms=np.nanmean(dif**2,axis=0)
        ma=np.nanmean(abs(dif),axis=0)
        # robust bounded influence on typical variance via median absolute deviation
        mad=np.nanmedian(abs(dif),axis=0)
        longs=y[13:]-y[:-13]
        longs-=np.nanmean(longs,axis=1,keepdims=True)
        lms=np.nanmean(longs**2,axis=0)
        lma=np.nanmean(abs(longs),axis=0)
        vs.extend([ms,ma**2,mad**2,lms,lma**2])
        for label,mask in zip(['100','K'],masks):
            v=(ms/(q+r))[mask]
            result[f'half{side}_{label}']=dict(mean_ratio=np.nanmean(v),med_ratio=np.nanmedian(v),
                 abs_over_rms=np.nanmedian(ma[mask]/np.sqrt(ms[mask])),normal=.797885,
                 med_over_rms=np.nanmedian(mad[mask]/np.sqrt(ms[mask])),median_qfraction=np.nanmedian((q/(q+r))[mask]))
    result['correlations']=scores(vs,z)
    return result

if __name__=='__main__':
    res={n:run(n) for n in gold.PANELS}
    Path('diag_scale.json').write_text(json.dumps(res,indent=2))
    for n,d in res.items(): print(n,d['half0_100'],d['half0_K'],np.asarray(d['correlations'])[:3,5:])
