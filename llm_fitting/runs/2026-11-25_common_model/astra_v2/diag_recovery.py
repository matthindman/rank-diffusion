"""Operating characteristics of the surge check and conditional scale instrument, with fixed seeds."""
import json
import math
from pathlib import Path
import numpy as np
from diag_moments import cr as ref, gold
import common_rankdiff_astra as cr
from test_common_rankdiff_astra import _make_planted_params

def direction_recovery():
    out={}
    for T in (52,86,212):
        per_case={k:[] for k in ('symmetric','slow_positive','fast_positive','opposite_plus_skewed_lasting')}
        for seed in range(24):
            rng=np.random.default_rng(4000+seed)
            N=600
            fast,slow=np.zeros(N),np.zeros(N)
            A=[];B=[]
            for t in range(T+350):
                jump=(rng.random(N)<.0013)*1.3*np.minimum(rng.exponential(size=N),5)
                slow=.5**(1/52)*slow+jump
                fast=.5**(1/8)*fast+(rng.random(N)<.015)*.7*np.minimum(rng.exponential(size=N),5)
                if t>=350:
                    A.append(slow.copy())
                    B.append(fast.copy())
            lasting=np.cumsum(.08*rng.normal(size=(T,N)),axis=0)
            baseline=lasting+.24*rng.normal(size=(T,N))
            skew_jumps=(rng.random((T,N))<.003)*rng.exponential(size=(T,N))
            cases=[baseline,baseline+np.array(A),baseline+np.array(B),
                   baseline-np.array(B)+np.cumsum(skew_jumps,axis=0)]
            for name,X in zip(per_case,cases):
                j=cr._surge_direction_check(X)
                per_case[name].append([int(j['contradicted']),j['mean'],j['cluster_se']])
        out[T]={k:dict(n=24,rejections=int(np.sum(np.array(v)[:,0])),
                        mean=np.mean(np.array(v)[:,1]),rows=v) for k,v in per_case.items()}
    return out

def empirical_sign():
    out={}
    for name in gold.PANELS:
        d,sb=gold.load_cached(name)
        X,R,K,fl=d['X'].astype(float),d['R'].astype(float),int(d['K']),d['floors']
        rows=[]
        for T in sorted(set([len(X)]+list(map(int,gold_origins(len(X),gold.PANELS[name]['horizons']))))):
            r=R[:T]
            idx=np.argsort(np.where(np.isfinite(r),r,fl[:T,None]).mean(axis=0),kind='stable')[:K]
            idx=idx[np.isfinite(r[:,idx]).mean(axis=0)>=.5]
            j=cr._surge_direction_check((X[:T]-ref._top_k_center(X[:T],r,K))[:,idx])
            rows.append(dict(T=T,**j))
        out[name]=rows
    return out

def gold_origins(T,hs):
    return np.unique(np.round(np.linspace(78 if T>=180 else (36 if T>=80 else 22),T-max(hs)-1,5)).astype(int))

def scales():
    out={}
    for T in (30,100):
        rows=[]
        for seed in range(12):
            N=300
            p,levels=_make_planted_params(N=N,K=120,b_xi=0,b_obs=0,beta=0,gamma_L=0,
                                           s_vol=.70,lam_exit=0,a_abs=-10,innov_mode='empirical')
            rng=np.random.default_rng(seed+12345)
            # Heavy-tailed but bounded, with the model's own pool normalization.
            u=rng.standard_t(4,4096).clip(-8,8)
            p.innov_pool=(u-u.mean())/u.std()
            p.drift_g[:]=0
            v=np.exp(np.clip(.7*rng.normal(size=N)-.245,-1.75,1.75))
            st=cr.CommonState(p,levels,rng,v0=v)
            vals=np.array([st.step(apply_absence=False) for _ in range(T)])
            ranks=np.argsort(np.argsort(-vals,axis=1),axis=1)+1
            for contam in (False,True):
                x=vals.copy()
                if contam:
                    x[T//2]+=rng.choice([-1,1],size=N)*3
                old=ref.filter_endpoints(x,ranks,p)['v_hat']
                new=cr.filter_endpoints(x,ranks,p)['v_hat']
                rows.append(dict(seed=seed,contaminated=contam,
                    old=np.mean(abs(np.log(old/v))),new=np.mean(abs(np.log(new/v)))))
        out[T]=rows
    return out

if __name__=='__main__':
    out=dict(direction=direction_recovery(),empirical=empirical_sign(),scale=scales())
    Path('diag_recovery.json').write_text(json.dumps(out,indent=2))
    for T,v in out['direction'].items():print('T',T,{k:c['rejections'] for k,c in v.items()})
    for n,v in out['empirical'].items():print(n,[(x['T'],round(x['mean'],4),round(x['cluster_se'],4),x['contradicted']) for x in v])
    for T,v in out['scale'].items():
        for c in (False,True):
            print(T,c,[np.mean([r[k] for r in v if r['contaminated']==c]) for k in ['old','new']])
