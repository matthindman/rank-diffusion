"""Isolate the old gamma switch with the immutable R3 simulator and gold activity scorer."""
import json
from pathlib import Path
import numpy as np
from diag_moments import cr,gold

def main():
    base=json.loads(Path('../round3_gold_benchmark_results.json').read_text())['panels']
    out={}
    for name,cfg in gold.PANELS.items():
        d,sb=gold.load_cached(name)
        X,R,K,fl=d['X'].astype(float),d['R'].astype(float),int(d['K']),d['floors']
        med=cr._top_k_center(X,R,K)[:,0]
        by=base[name]['goal2']['by_horizon']
        rows=[]
        for i,t in enumerate(base[name]['goal2']['origins']):
            p=cr.estimate_common(X[:t],R[:t],K,fl[:t],sb,refine_drift=False,fit_gamma_L=True)
            row=dict(T0=t,gamma=p.gamma_L,horizons={})
            if p.gamma_L>0:
                z,g,_=cr._refine_drift_smooth(p,p.ladder_obs)
                p=cr.replace(p,drift_z=z,drift_g=g)
                f=cr.filter_endpoints(X[:t],R[:t],p,fl[:t])
                fc=gold._forecast_exact_single_pass(f,p,cfg['horizons'],39,2000*i)
                ids=np.argsort(np.where(np.isfinite(R[:t]),R[:t],fl[:t,None]).mean(axis=0),kind='stable')[:K]
                if name=='instagram_hm':ids=ids[np.isfinite(R[:t,ids]).mean(axis=0)>=.5]
                past=np.isfinite(X[:t,ids]).any(axis=0)
            for h in cfg['horizons']:
                ref=by[str(h)]['by_origin'][i]
                if p.gamma_L>0:
                    fut=t-1+h
                    y=X[fut,ids]-med[fut]
                    ok=np.isfinite(y)&past
                    assert int(ok.sum())==ref['n_ok_x']
                    ens=fc[h]['X_cond'][ids[ok]]
                    score=np.mean(gold.fair_ensemble_crps(y[ok],ens))
                    es=np.sort(ens,axis=1)
                    cov=np.mean((y[ok]>=es[:,1])&(y[ok]<=es[:,37]))
                else:
                    score=ref['crps_X_s2'];cov=ref['cov90_s2']
                row['horizons'][h]=dict(crps=score,original=ref['crps_X_s2'],
                                       cov90=cov,original_cov90=ref['cov90_s2'])
            rows.append(row)
        out[name]=rows
        Path('diag_gamma_control.json').write_text(json.dumps(out,indent=2))
        delta=[np.mean([r['horizons'][h]['crps']-r['horizons'][h]['original'] for r in rows]) for h in cfg['horizons']]
        print(name,'gamma',np.round([r['gamma'] for r in rows],3),'paired CRPS deltas',np.round(delta,6),flush=True)
if __name__=='__main__':
    main()
