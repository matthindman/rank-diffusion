"""Overidentification checks for rare positive shot noise. Raw positive increments never set a gate threshold."""
from diag_moments import cr,gold,cumulants
import json
from pathlib import Path
import numpy as np

def run(n):
    d,sb=gold.load_cached(n)
    X,R,K,fl=d['X'].astype(float),d['R'].astype(float),int(d['K']),d['floors']
    p=cr.estimate_common(X,R,K,fl,sb,refine_drift=False,asymmetry=True)
    ids=np.argsort(np.where(np.isfinite(R),R,fl[:,None]).mean(axis=0),kind='stable')[:K]
    keep=np.isfinite(R[:,ids]).mean(axis=0)>=.5
    x=(X-cr._top_k_center(X,R,K))[:,ids][:,keep]
    ranks=np.arange(1,K+1)[keep]
    z=np.log((ranks-.5)/X.shape[1])
    old=cr._surge_scale_of(p.b_xi,z,p.z_K)
    obs=cr.sigma_obs_of(p,z)
    mix=np.sqrt((1-p.phi**2)*cr.v_xi_of(p,z)+obs**2)
    head=ranks<=K/16; body=ranks>=K/2
    pred={k:np.mean(s[head]**3)/np.mean(s[body]**3) for k,s in [('passing',old),('weekly_sd',mix),('obs',obs)]}
    for h in [4,8,13,20,26]:
        ds=x[h:]-x[:-h]
        if len(ds)<5:continue
        pred[f'k3_h{h}']=cumulants(ds[:,head])[0]/cumulants(ds[:,body])[0]
    cross={}
    for h in (4,8,13):
        gap=1
        if len(x)<=2*h+gap: continue
        dx=x[h:]-x[:-h]
        dx-=np.nanmean(dx,axis=1,keepdims=True)
        a,b=dx[:-h-gap],dx[h+gap:]
        a=np.clip(a,-7*np.nanstd(a),7*np.nanstd(a))
        b=np.clip(b,-7*np.nanstd(b),7*np.nanstd(b))
        ok=np.isfinite(a)&np.isfinite(b)
        a=a[ok];b=b[ok];a-=a.mean();b-=b.mean()
        r=p.surge_rho**h; t=p.surge_rho**gap
        C=p.diag['stage3']['surge'].get('C3',0)
        cross[h]=dict(k21=np.mean(a*a*b),model21=-C*t*(1-r)**2*(1+2*r),
                      k12=np.mean(a*b*b),model12=C*t*t*(1-r)**3*(1+r))
    return dict(scale=pred,cross=cross)
if __name__=='__main__':
    out={n:run(n) for n in gold.PANELS}
    Path('diag_surge_rank.json').write_text(json.dumps(out,indent=2))
    for n,v in out.items():print(n,v)
