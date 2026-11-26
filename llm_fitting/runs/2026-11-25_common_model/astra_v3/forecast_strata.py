"""Localize coverage and CRPS using the exact frozen Gold conditional ensembles."""
import json
import sys
from pathlib import Path

import numpy as np
import common_rankdiff_astra_r7 as candidate

WORK = Path(__file__).resolve().parent
sys.path.insert(0, str(WORK.parent))
import round3_gold_benchmark as gold
gold.cr = candidate
BASE = json.loads((WORK/'baseline_r6/candidate_gold_benchmark_results.json').read_text())['panels']


def main():
    path = WORK/'forecast_strata.json'
    out = json.loads(path.read_text()) if path.exists() else {}
    for name in ('instagram_hm','reddit_submissions_long'):
        data,_ = gold.load_cached(name)
        x,r,floors = (data[key].astype(float) for key in ('X','R','floors'))
        k = int(data['K'])
        med = np.array([np.nanmedian(v[np.isfinite(rk) & (rk <= k)]) for v,rk in zip(x,r)])
        rel = x-med[:,None]
        horizons = gold.PANELS[name]['horizons']
        row=out.setdefault(name,{})
        for label,mode in [('r6_source','meta'),('r7','bounded')]:
            if label in row:
                continue
            results = []
            for i,end in enumerate(BASE[name]['goal2']['origins']):
                xtr,rtr,fl = x[:end],r[:end],floors[:end]
                p = candidate.estimate_common(xtr,rtr,k,fl,scale_moment=mode,refine_drift=True)
                filt = candidate.filter_endpoints(xtr,rtr,p,fl)
                fc = gold._forecast_exact_single_pass(filt,p,horizons,39,2000*i)
                perm = np.mean(np.where(np.isfinite(rtr),rtr,fl[:,None]),axis=0)
                members = np.argsort(perm,kind='stable')[:k]
                rank = np.arange(1,k+1)
                if name == 'instagram_hm':
                    keep = np.isfinite(rtr[:,members]).mean(axis=0)>=.5
                    members,rank = members[keep],rank[keep]
                _,ewma = gold._tune_local_level_ewma(rel[:end],members)
                observed = np.any(np.isfinite(rel[:end,members]),axis=0)
                for h in horizons:
                    y = rel[end-1+h,members]
                    valid = np.isfinite(y) & np.isfinite(ewma[members]) & observed
                    ens = fc[h]['X_cond'][members]
                    es = np.sort(ens,axis=1)
                    for band, mask in [('all',rank>0),('head',rank<=k//16),
                                       ('middle',(rank>k//16)&(rank<=k//2)),('outer',rank>k//2)]:
                        mask = mask & valid
                        obs,lo,hi,pred = y[mask],es[mask,1],es[mask,37],es[mask,19]
                        results.append(dict(T0=end,h=h,band=band,n=int(mask.sum()),
                                            below=float(np.mean(obs<lo)),above=float(np.mean(obs>hi)),
                                            width=float(np.mean(hi-lo)),error=float(np.mean(obs-pred)),
                                            mae=float(np.mean(abs(obs-pred))),
                                            crps=float(np.mean(gold.fair_ensemble_crps(obs,ens[mask])))))
            row[label] = results
            path.write_text(json.dumps(out,indent=2))
            print(name,label,'complete',flush=True)


if __name__ == '__main__':
    main()
