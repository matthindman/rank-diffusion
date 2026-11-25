"""Post-design measurement: fingerprint of gapped D in continuously observed endpoints."""
import json
from pathlib import Path
import numpy as np
from diag_moments import gold, cumulants

def summarize(windows):
    stats=gold._asym_from_windows(windows)
    idx=np.unique(np.concatenate([w[0] for w in windows]))
    gen=np.random.default_rng(77)
    D=[]
    for _ in range(200):
        weights=np.bincount(gen.choice(idx,size=idx.size),minlength=int(idx.max())+1)
        D.append(gold._asym_from_windows(windows,weights_by_ent=weights)['D'])
    return dict(n=idx.size,D=stats['D'],band=np.quantile(D,[.05,.95]).tolist(),
                p_fall=stats['p_fall'],p_rise=stats['p_rise'],rho=stats['gapped_rho'])

if __name__=='__main__':
    res={}
    for name,cfg in gold.PANELS.items():
        d,_=gold.load_cached(name)
        X,R,K=d['X'].astype(float),d['R'].astype(float),int(d['K'])
        wins,lab=gold._gapped_windows_arrays(X,R,K,cfg)
        present=np.isfinite(X).all(axis=0)
        cohorts={}
        for label,mask in [('all_weeks',present),('at_least_one_absence',~present)]:
            selected=[]
            for ids,x,y in wins:
                use=mask[ids]
                if use.sum()>=100:
                    selected.append((ids[use],x[use],y[use]))
            if selected:
                m=summarize(selected)
                a=X[:,(lab>=0)&mask]
                m['k3_h8']=cumulants(a[8:]-a[:-8])[0]
                cohorts[label]=m
        res[name]=cohorts
        print(name,cohorts,flush=True)
        Path('diag_presence.json').write_text(json.dumps(res,indent=2))
