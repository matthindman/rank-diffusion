"""Training moments only. Run against the immutable Round 3 reference."""
import json
import sys
import warnings
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import round3_gold_benchmark as gold
import common_rankdiff as cr

warnings.filterwarnings("ignore", category=RuntimeWarning)

def cumulants(dx):
    dx = dx - np.nanmean(dx, axis=1, keepdims=True)
    x = dx[np.isfinite(dx)]
    x = np.clip(x, -7 * x.std(), 7 * x.std())
    x -= x.mean()
    return [float(np.mean(x**3)), float(np.mean(x**5) - 10*np.mean(x**2)*np.mean(x**3))]

def profile(hs, k3):
    # Coefficient of h is signed: a skewed permanent increment can have either sign.
    out = []
    for hl in cr.SURGE_HL_GRID:
        a = .5 ** (hs / hl)
        A = np.column_stack([3*a*(1-a), hs])
        c = np.linalg.lstsq(A, k3, rcond=None)[0]
        out.append([hl, *c, float(np.linalg.norm(k3 - A @ c)), float(np.corrcoef(A.T)[0,1])])
    return out

def vol_relation(x, ranks):
    # Compare early amplitude to independent late amplitude at two lags, conditional on rank.
    half = len(x)//2
    z = np.log(ranks)
    pieces = []
    for block in (x[:half], x[half:]):
        for h in (1, 13):
            d = block[h:] - block[:-h]
            d -= np.nanmean(d, axis=1, keepdims=True)
            v = np.nanmean(d*d, axis=0)
            v[(np.sum(np.isfinite(d),axis=0)<5) | (v<=0)] = np.nan
            pieces.append(np.log(v))
    ok = np.isfinite(pieces).all(axis=0)
    A = np.column_stack([np.ones(ok.sum()), z[ok], z[ok]**2])
    Y = np.array(pieces)[:,ok].T
    u = Y - A@np.linalg.lstsq(A,Y,rcond=None)[0]
    C = u.T@u / len(u)
    return dict(n=int(ok.sum()), short_cov=C[0,2], long_cov=C[1,3],
                cross_cov=(C[0,3]+C[1,2])/2,
                spread_ratio=np.sqrt(max(0,C[1,3])/C[0,2]),
                cross_slope=(C[0,3]+C[1,2])/(2*C[0,2]))

def run(name):
    d,sb = gold.load_cached(name)
    X,R,K,fl = d['X'].astype(float),d['R'].astype(float),int(d['K']),d['floors']
    p = cr.estimate_common(X,R,K,fl,sb,refine_drift=False,fit_gamma_L=True)
    N = X.shape[1]
    idx = np.argsort(np.where(np.isfinite(R),R,fl[:,None]).mean(axis=0),kind='stable')[:K]
    x = (X-cr._top_k_center(X,R,K))[:,idx]
    ok = np.isfinite(x).mean(axis=0)>=.5
    hs = np.array([h for h in (1,4,8,13,20,26,39,52) if h<=len(X)//2])
    res = {}
    for label, select in [('whole',ok),('head',ok & (np.arange(K)<K/16)),('body',ok & (np.arange(K)>=K/2))]:
        a = x[:,select]
        m = np.array([cumulants(a[h:]-a[:-h]) for h in hs])
        blocks = np.nanmean(a[:len(a)//4*4].reshape(-1,4,a.shape[1]),axis=1)
        res[label] = dict(h=hs.tolist(), k3=m[:,0].tolist(), k5=m[:,1].tolist(),
                          block4_lag2_k3=cumulants(blocks[2:]-blocks[:-2])[0],
                          linear_plus_wave=profile(hs[1:],m[1:,0]))
    D=np.asarray(p.diag['D'])
    sL=cr.sigma_L_of(p,np.array(p.diag['band_level']))
    z=np.array(p.diag['band_z'])
    f=cr.filter_endpoints(X,R,p,fl)
    med_v_by_band=[]
    power_by_band=[]
    for left,right in zip(cr._band_edges(K,N)[:-2],cr._band_edges(K,N)[1:-1]):
        vals=f['v_hat'][idx[max(0,int(left)-1):max(1,int(right)-1)]]
        med_v_by_band.append(float(np.median(vals)))
        power_by_band.append(float(np.mean(vals**p.gamma_L)))
    res.update(gamma_naive=p.gamma_L, s=p.s_vol,vol=vol_relation(x[:,ok],np.arange(1,K+1)[ok]),
               med_v=med_v_by_band, mean_v_gamma=power_by_band,
               pool_exkurt=float(np.mean(p.innov_pool**4)-3),
               nnls_obs=p.diag['so2_band'], nnls_passing=p.diag['vxi_band'])
    for h in (1,4,13):
        fit=2*cr.sigma_obs_of(p,z)**2 + 2*cr.v_xi_of(p,z)*(1-p.phi**h)+h*sL**2
        res[f'smooth_to_D{h}']=(fit[:-1]/D[:-1,h-1]).tolist()
    return res

if __name__=='__main__':
    res={}
    for name in gold.PANELS:
        res[name]=run(name)
        Path('diag_moments.json').write_text(json.dumps(res,indent=2))
        print(name, res[name]['smooth_to_D1'],res[name]['vol'],flush=True)
