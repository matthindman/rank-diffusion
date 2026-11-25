"""Evaluate the exact calendar split with the unchanged OOP estimator, seed bank and scoring."""
import gc
import json
from pathlib import Path
import numpy as np
from diag_moments import gold, cr as baseline
import common_rankdiff_astra as candidate

def main():
    out={}
    for name in ('reddit_comments_ext','reddit_submissions_long'):
        d,_=gold.load_cached(name)
        dates=np.asarray(d['dates'],dtype='datetime64[D]')
        in_period=(dates>=np.datetime64('2019-01-01')) & (dates<np.datetime64('2023-01-01'))
        dates=dates[in_period]
        train=dates<np.datetime64('2021-01-01')
        assert train.sum()==104 and train[:104].all() and (~train[104:]).all()
        assert np.all(np.diff(dates)==np.timedelta64(7,'D'))
        X=d['X'][in_period].astype(float)
        R=d['R'][in_period].astype(float)
        floors=d['floors'][in_period].astype(float)
        row=dict(start=str(dates[0]),train_end=str(dates[103]),test_start=str(dates[104]),test_end=str(dates[-1]))
        for label,model in [('round3',baseline),('candidate',candidate)]:
            gold.cr=model
            row[label]=gold.out_of_period_structure_test(name,X,R,floors,int(d['K']),gold.PANELS[name],n_seeds=20)
            gc.collect()
            print(name,label,row['start'],row['train_end'],row['test_start'],row['test_end'],flush=True)
        out[name]=row
        Path('diag_calendar_oop.json').write_text(json.dumps(out,indent=2))
        del row,X,R,d
        gc.collect()

if __name__=='__main__':
    main()
