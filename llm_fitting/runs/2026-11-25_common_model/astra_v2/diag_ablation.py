"""Factorial attribution: reuse both frozen score functions, without changing parent files."""
import gc
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
from diag_moments import cr as old, gold
import round3_frozen_gate as gate
import common_rankdiff_astra as new

def fit(*args, asymmetry=False, **kwargs):
    # The evaluator exposes two slots; both are S3 in this attribution experiment.
    estimator = new if asymmetry else old
    p = estimator.estimate_common(*args, asymmetry=True, **kwargs)
    p.diag['ablation'] = 'sign_only' if asymmetry else 'filter_only'
    return p

def filtered(x,r,p,**kwargs):
    module = old if p.diag['ablation']=='sign_only' else new
    return module.filter_endpoints(x,r,p,**kwargs)

def main():
    api=SimpleNamespace(**{k:getattr(old,k) for k in dir(old) if not k.startswith('__')})
    api.estimate_common=fit
    api.filter_endpoints=filtered
    gold.cr=api
    gate.cr=api
    result={}
    for name,cfg in gold.PANELS.items():
        row={}
        row['gate']=gate.run_panel(name, gate.PANELS[name], n_splits=5,reps=20,boot=2000)
        d,sb=gold.load_cached(name)
        x,r,f,k=d['X'].astype(float),d['R'].astype(float),d['floors'].astype(float),int(d['K'])
        row['goal2']=gold.oos_goal2_gold(name,x,r,f,k,cfg,sb,n_ens=39)
        if name.startswith('reddit_'):
            row['oop']=gold.out_of_period_structure_test(name,x,r,f,k,cfg,n_seeds=20)
        result[name]=row
        Path('diag_ablation.json').write_text(json.dumps(result,indent=2))
        print('completed factorial comparison',name,flush=True)
        del d,x,r,row
        gc.collect()

if __name__=='__main__':
    main()
