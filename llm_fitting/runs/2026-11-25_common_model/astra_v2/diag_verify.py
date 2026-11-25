"""Verify completion, pairing and numerical invariants against both archives before writing the verdict."""
import json
import math
from pathlib import Path
import numpy as np

def load(path):
    return json.loads(Path(path).read_text())

def same(a,b):
    if isinstance(a,dict):
        return a.keys()==b.keys() and all(same(a[k],b[k]) for k in a)
    if isinstance(a,list):
        return len(a)==len(b) and all(same(x,y) for x,y in zip(a,b))
    if isinstance(a,float) and isinstance(b,float) and math.isnan(a):
        return math.isnan(b)
    return a==b

def main():
    bg=load('../round3_frozen_gate_results.json');cg=load('candidate_frozen_gate_results.json')
    base=load('../round3_gold_benchmark_results.json');cand=load('candidate_gold_benchmark_results.json')
    ab=load('diag_ablation.json'); calendar=load('diag_calendar_oop.json'); blocks=load('diag_calendar_blocks.json')
    assert same(base['oracle_check'],cand['oracle_check'])
    print('Exchangeable Gaussian oracle',cand['oracle_check'])
    assert bg.keys()==cg.keys()==base['panels'].keys()==cand['panels'].keys()==ab.keys()
    for n,p in cand['panels'].items():
        b=base['panels'][n]; ag=ab[n]['gate'];a=ab[n]['goal2']
        assert len(cg[n]['splits'])==len(ag['splits'])==5
        for s0,s1,s2 in zip(bg[n]['splits'],cg[n]['splits'],ag['splits']):
            for key in ('T0','em','bm','base_rel'):
                assert same(s0[key],s1[key]) and same(s0[key],s2[key])
        assert p['goal2']['n_ens']==a['n_ens']==39
        assert p['params_s2']['n_scalars']<=12 and p['params_s3']['n_scalars']<=15
        assert same(b['emp'],p['emp']) and same(b['emp_cluster_ci'],p['emp_cluster_ci'])
        assert same(b['g1_s2'],p['g1_s2'])
        for h,c in p['goal2']['by_horizon'].items():
            assert c['n_origins']==5 and len(c['by_origin'])==5
            for key in ('mae_no','mae_ll','crps_lr_rb','share_mae_no_hd','brier_hd_rb','n_ok_x','n_head'):
                assert c[key]==b['goal2']['by_horizon'][h][key]==a['by_horizon'][h][key]
            for split in c['by_origin']:
                for v in ('s2','s3'):
                    assert np.isfinite([split[k+'_'+v] for k in ('mae_kal','mae_ens','crps_X','crps_lr','share_mae_hd','brier_hd','pit_sd')]).all()
                    assert 0<=split['cov90_'+v]<=1
        if n in calendar:
            assert same(b['oop_2019_20_to_2021_22']['emp_2021_22'],p['oop_2019_20_to_2021_22']['emp_2021_22'])
            for v in (calendar[n],blocks[n]):
                assert v['candidate']['T_te']==103 and v['candidate']['T_tr']==104
                assert same(v['round3']['emp_2021_22'],v['candidate']['emp_2021_22'])
        print(n,'five paired splits, fixed baselines, identical S2 structure, all forecasts finite; scalars',p['params_s2']['n_scalars'],p['params_s3']['n_scalars'])
    print('All archive and cohort checks passed.')
if __name__=='__main__':
    main()
