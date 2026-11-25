"""Bounded-frequency identification of the asymmetry of stationary shot-noise increments."""
import json
import math
from pathlib import Path
import numpy as np
from scipy.optimize import least_squares
from diag_moments import cr, gold

def phase_inputs(Xc, pr_ord, pres, K):
    sel=(pr_ord<=K)&(pres>=.5)
    x=Xc[:,sel]
    hs=[h for h in (4,8,13,20,26,39,52) if h<=min(52,len(x)//2)]
    if sel.sum()<50 or len(hs)<2: return None
    hs_out=[]; freq=[]; target=[]; modulus=[]
    for h in hs:
        d=x[h:]-x[:-h]
        d-=np.nanmean(d,axis=1,keepdims=True)
        v=d[np.isfinite(d)]
        if len(v)<100 or np.std(v)<1e-6: continue
        for u in (.5,1.,1.5):
            t=u/np.std(v)
            cf=np.mean(np.cos(t*v))+1j*np.mean(np.sin(t*v))
            hs_out.append(h); freq.append(t); target.append(np.angle(cf)); modulus.append(abs(cf))
    if len(freq)<6: return None
    return tuple(np.asarray(a) for a in (hs_out,freq,target,modulus))

def jump_phase(t,lam,mu):
    u=t*mu
    # CF of min(Exp(1),C), including its point mass at C.
    jump=(1-1j*u*np.exp((-1+1j*u)*cr.SURGE_EXP_CAP))/(1-1j*u)
    return np.angle(1+lam*(jump-1))

def stationary_phase(t,lam,mu,rho):
    powers=rho**np.arange(math.ceil(math.log(1e-4)/math.log(rho)))
    # Removing the linear term makes the omitted phase tail O(t^3*rho^(3J)).
    u=t[:,None]*powers
    return (jump_phase(u,lam,mu)-lam*mu*cr.CAP_M1*u).sum(axis=1)

def phase_fit(inputs):
    h,t,target,weight=inputs
    best=None
    for hl in cr.SURGE_HL_GRID:
        rho=.5**(1/hl)
        a=rho**h
        args=np.concatenate([t,(a-1)*t,a*t])
        def resid(theta):
            lam,mu=np.exp(theta)
            a1,a2,a3=np.split(stationary_phase(args,lam,mu,rho),3)
            return weight*(a1+a2-a3-target)
        fit=least_squares(resid,np.log([.007,.65]),bounds=(np.log([1.e-6,.15]),np.log([.15,2.5])),
                          ftol=1e-8,gtol=1e-9)
        norm=np.linalg.norm(fit.fun)
        if best is None or norm<best[0]: best=(norm,*np.exp(fit.x),rho)
    return best

if __name__=='__main__':
    out={}
    for name in gold.PANELS:
        d,sb=gold.load_cached(name)
        X,R,K,fl=d['X'].astype(float),d['R'].astype(float),int(d['K']),d['floors']
        rank=np.argsort(np.argsort(np.where(np.isfinite(R),R,fl[:,None]).mean(axis=0),kind='stable'))
        inputs=phase_inputs(X-cr._top_k_center(X,R,K),rank+1,np.isfinite(R).mean(axis=0),K)
        err,lam,mu,rho=phase_fit(inputs)
        out[name]=dict(err=err,lam=lam,mu=mu,hl=-math.log(2)/math.log(rho),
                       Vs=cr._surge_variance(lam,mu,rho),phase=inputs[2].tolist())
        print(name,out[name],flush=True)
        Path('diag_surge.json').write_text(json.dumps(out,indent=2))
