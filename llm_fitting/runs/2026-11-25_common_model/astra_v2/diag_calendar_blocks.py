"""Equal-exposure annual turnover for the exact-calendar OOP run's 103-week test."""
import json
from pathlib import Path
import numpy as np
from diag_moments import gold,cr as baseline
import common_rankdiff_astra as candidate

def block_stats(X,R,K,cfg,ids=None):
    scores={}
    # Input train and test start in the first calendar week at index 0;
    # both cover two years with starts exactly 52 weeks apart.
    top=[]
    for y,start in enumerate((0,52)):
        A=X[start:start+51]
        means=gold.wmean(X,start,start+51,cfg['pres'])
        order=gold._order_desc(means)
        top.append(order)
        for k in (1,K//64,K//16,K//4):
            scores[f'year{y+1}_span_{k}']=float(means[order[k-1]]-means[order[K-1]])
        totals=np.nansum(np.expm1(np.clip(A[:,order[:K]],0,45)),axis=0)
        scores[f'year{y+1}_head']=float(totals[:K//64].sum()/totals.sum())
    for k in (10,K//64,K//16,K//4,K//2):
        scores[f'turn_{k}']=1-np.intersect1d(top[0][:k],top[1][:k]).size/k
    return scores

def main():
    gold.structure_stats=block_stats
    gold._agg_structure_sims=lambda rows:{k:gold._band_agg(rows,k) for k in rows[0]}
    out={}
    for name in ('reddit_comments_ext','reddit_submissions_long'):
        d,_=gold.load_cached(name)
        dates=np.asarray(d['dates'],dtype='datetime64[D]')
        sel=(dates>=np.datetime64('2019-01-01'))&(dates<np.datetime64('2023-01-01'))
        year=dates[sel].astype('datetime64[Y]').astype(int)+1970
        assert [year[i] for i in (0,52,104,156)]==[2019,2020,2021,2022]
        x=d['X'][sel].astype(float); r=d['R'][sel].astype(float)
        out[name]={}
        for label,model in [('round3',baseline),('candidate',candidate)]:
            gold.cr=model
            out[name][label]=gold.out_of_period_structure_test(name,x,r,d['floors'][sel],int(d['K']),gold.PANELS[name],n_seeds=20)
            print(name,label,flush=True)
        Path('diag_calendar_blocks.json').write_text(json.dumps(out,indent=2))

if __name__=='__main__':
    main()
