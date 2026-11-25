"""Summarize paired outcomes for the narrative without optimizing any model parameter."""
from diag_result_tables import load
import numpy as np

def main():
    base=load('../round3_gold_benchmark_results.json')['panels']
    cand=load('candidate_gold_benchmark_results.json')['panels']
    bg=load('../round3_frozen_gate_results.json')
    cg=load('candidate_frozen_gate_results.json')
    ab=load('diag_ablation.json')
    refs=load('diag_reference.json')
    for n,p in cand.items():
        hrows=p['goal2']['by_horizon'];brows=base[n]['goal2']['by_horizon']
        print(n)
        for v in ('s2','s3'):
            gm='sm2' if v=='s2' else 'sm3'
            for k in ('dRank1','dRank4','dRank13','coll1'):
                av=[]
                for g,w in [(bg,'em'),(bg,gm),(cg,gm)]:
                    y=[s[w].get(k,np.nan) for s in g[n]['splits']]
                    av.append(np.round(np.nanmean(y),3) if np.isfinite(y).any() else None)
                print(v,k,av)
            for k in ('mae_kal','crps_X','crps_lr','brier_hd','share_mae_hd','share_corr_hd','cov90','pit_sd'):
                b=np.array([brows[h][k+'_'+v] for h in hrows]);c=np.array([hrows[h][k+'_'+v] for h in hrows])
                print(v,k,'old',np.round(b,6),'new',np.round(c,6))
        for ctx in ['oop_2019_20_to_2021_22']:
            if p[ctx]:
                emp=p[ctx]['emp_2021_22']
                for k in ('head_share','boundary_ratio','D','gapped_rho'):
                    print('frozen OOP',k,np.round([emp[k]]+[d[ctx][v]['sim'][k]['median'] for v in ['S2','S3'] for d in [base[n],p]],4))
    for v in ('s2','s3'):
        print('TOTALS',v)
        for key,lo,hi in [('mae_kal','mae_no','mae_ll'),('mae_ens','mae_no','mae_ll'),('crps_X','mae_no','mae_ll'),
                          ('crps_lr','crps_lr_no','crps_lr_rb'),('share_mae_hd','share_mae_no_hd','share_mae_ll_hd')]:
            metrics=[]
            for n,p in cand.items():
                for h,c in p['goal2']['by_horizon'].items():
                    old=base[n]['goal2']['by_horizon'][h]
                    k=key+'_'+v
                    paired=[x[k]-y[k] for x,y in zip(c['by_origin'],old['by_origin'])]
                    metrics.append([old[k]<min(c[lo],c[hi]),c[k]<min(c[lo],c[hi]),c[k]<old[k],np.mean(np.array(paired)<0)])
            print(key,np.round(np.sum(metrics,axis=0),3))
        counts=[]
        for n,p in cand.items():
            for h,c in p['goal2']['by_horizon'].items():
                b=base[n]['goal2']['by_horizon'][h]
                fbno=np.mean([x['brier_no'][h] for x in refs[n]])
                counts.append([b['brier_hd_'+v]<min(fbno,c['brier_hd_rb']),c['brier_hd_'+v]<min(fbno,c['brier_hd_rb']),
                               abs(c['cov90_'+v]-.9)<abs(b['cov90_'+v]-.9),abs(c['pit_sd_'+v]-.289)<abs(b['pit_sd_'+v]-.289)])
        print('Brier baseline wins old/new, calibration improvement coverage/PIT',np.sum(counts,axis=0))
    print('S3 ablation gates filter/sign')
    for n,a in ab.items():print(n, a['gate']['s2_rel'],a['gate']['s3_rel'])
if __name__=='__main__':
    main()
