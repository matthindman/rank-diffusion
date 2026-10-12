#!/usr/bin/env python3
"""Resummarize complete V1 seed banks; never fits or simulates a model."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parent))
import gate_instrument as gi


def pooled_hist_quantile(values, counts, weights, q=.5):
    """Exact pooled quantile without a draws-by-all-ranks dense matrix.

    Binary search each draw's weighted cumulative histogram. This uses
    O(draws * seeds) temporary memory, even for the large Reddit universe.
    """
    values=np.asarray(values)
    counts=np.asarray(counts,dtype=np.int64)
    weights=np.asarray(weights,dtype=np.int64)
    total=weights@counts.sum(axis=1)
    if not len(values):
        return np.full(len(weights),np.nan)
    cdf=counts.cumsum(axis=1)
    at=(total-1)*q
    def select(target):
        lo=np.zeros(len(weights),dtype=int)
        hi=np.full(len(weights),len(values)-1,dtype=int)
        while np.any(lo<hi):
            mid=(lo+hi)//2
            below=(weights*cdf[:,mid].T).sum(axis=1)<=target
            lo=np.where(below,mid+1,lo)
            hi=np.where(below,hi,mid)
        return values[np.minimum(lo,len(values)-1)]
    low=np.floor(at)
    result=(1-(at-low))*select(low)+(at-low)*select(np.ceil(at))
    return np.where(total>0,result,np.nan)


def moments_from_bank(path, horizons, draws):
    """Exact pooled medians / seed-RACF medians / mean collisions per draw."""
    records=[]
    for seed in range(1000,1080):
        with np.load(Path(path)/f'seed_{seed:04d}.npz',allow_pickle=False) as z:
            records.append(dict(z))
    weights=np.zeros((len(draws),80),float)
    np.add.at(weights,(np.repeat(np.arange(len(draws)),draws.shape[1]),draws.ravel()),1)
    result={}
    for h in horizons:
        values=np.unique(np.concatenate([r[f'v{h}'] for r in records]))
        counts=np.zeros((80,len(values)),float)
        for i,r in enumerate(records):
            counts[i,np.searchsorted(values,r[f'v{h}'])]=r[f'n{h}']
        result[f'dRank{h}']=pooled_hist_quantile(values,counts,weights,.5)
    rfs=np.array([r['racf'] for r in records])
    result['RACF1']=np.nanmedian(rfs[draws],axis=1)
    cc=np.array([r['collisions'] for r in records])
    for i,c in enumerate((1,5,20)):
        valid=np.isfinite(cc[:,i])
        result[f'coll{c}']=np.divide(weights@np.nan_to_num(cc[:,i]),weights@valid,
            out=np.full(len(draws),np.nan),where=(weights@valid)>0)
    return result


def contribution_arrays(emp,sim,keys):
    valid=np.array([np.isfinite(emp.get(k,np.nan)) and abs(emp.get(k,0))>=gi.rk.MOM_FLOOR for k in keys])
    a=np.column_stack([sim.get(k,np.full(len(next(iter(sim.values()))),np.nan)) for k in keys])
    valid_draw=valid[None,:]&np.isfinite(a)
    denom=np.array([max(abs(emp.get(k,0)),1e-6) for k in keys])
    e=np.abs(a-np.array([emp.get(k,np.nan) for k in keys]))/denom
    c=np.where(valid_draw,e,0)/valid_draw.sum(axis=1)[:,None]
    return c,valid_draw


def summary(x):
    x=np.asarray(x)
    return dict(mean=float(np.mean(x)),sd=float(np.std(x,ddof=1)),
                q025=float(np.quantile(x,.025)),q975=float(np.quantile(x,.975)),
                mcse_mean=float(np.std(x,ddof=1)/np.sqrt(len(x))))


def analyse_panel(root,panel,B=2000):
    dest=Path(root)/panel/'bank'
    complete=json.loads((dest/'COMPLETE.json').read_text())
    result=json.loads((dest/'RESULT.json').read_text())
    splits=result['splits'];keys=[f'dRank{h}' for h in splits[0]['horizons']]+['RACF1','coll1','coll5','coll20']
    out=dict(panel=panel,conditional_on='empirical panels, fitted parameters, and finite seed bank',
             B=B,seed_bank=list(range(1000,1080)),reps={},floor={})
    for k in keys:
        active=[s['origin'] for s in splits if np.isfinite(s['emp'].get(k,np.nan)) and abs(s['emp'][k])>=gi.rk.MOM_FLOOR]
        out['floor'][k]=dict(included_origins=active,excluded_origins=[s['origin'] for s in splits if s['origin'] not in active])
    for n in gi.REPS_GRID:
        rng=np.random.default_rng(20261012+n)
        draws=rng.integers(0,80,size=(2*B,n))
        full_c=[]; fixed_c=[]; fixed_jensen=[]; fixed_centers=[]; split_out=[]
        for split in splits:
            origin=split['origin'];horizons=split['horizons']
            train=[];test=[]
            for scale in gi.SCALES:
                path=dest/str(origin)
                train.append(moments_from_bank(path/'train'/f'scale_{scale:g}',horizons,draws))
                test.append(moments_from_bank(path/'test'/f'scale_{scale:g}',horizons,draws))
            em=split['train_calibration']
            cal=[]
            for moments in train:
                vals=[]
                for k,w in gi.rk.CAL_WEIGHTS.items():
                    if np.isfinite(em.get(k,np.nan)):
                        vals.append(w*np.abs(moments[k]-em[k])/max(abs(em[k]),1e-6))
                cal.append(np.nanmean(vals,axis=0))
            selected=np.argmin(np.array(cal),axis=0)
            chosen={k:np.array([t[k] for t in test])[selected,np.arange(2*B)] for k in keys}
            c,valid=contribution_arrays(split['emp'],chosen,keys)
            fixed_index=gi.SCALES.index(split['scale'])
            fc,fv=contribution_arrays(split['emp'],test[fixed_index],keys)
            full_c.append(c);fixed_c.append(fc)
            mean_sim={k:np.array([np.mean(test[fixed_index][k])]) for k in keys}
            center_c,_=contribution_arrays(split['emp'],mean_sim,keys)
            fixed_jensen.append(fc.mean(axis=0)-center_c[0])
            fixed_centers.append(center_c[0])
            e=c.sum(axis=1);d=e[:B]-e[B:]
            split_out.append(dict(origin=origin,score=summary(e),null_difference=summary(d),
                resolution95=float(np.quantile(np.abs(d),.95)),
                fixed_score=summary(fc.sum(axis=1)),
                fixed_mean_moment={k:float(mean_sim[k][0]) for k in keys},
                fixed_bank80_moment={k:split['sim'][k] for k in keys},
                fixed_center_error=float(center_c.sum()),
                fixed_jensen=dict(zip(keys,fc.mean(axis=0)-center_c[0])),
                scale_frequencies={str(sc):float(np.mean(selected==i)) for i,sc in enumerate(gi.SCALES)},
                moment_mean=dict(zip(keys,c.mean(axis=0))),
                moment_variance_share=dict(zip(keys,gi.variance_shares(c)))))
        total=np.mean(full_c,axis=0);fixed=np.mean(fixed_c,axis=0)
        score=total.sum(axis=1);fixed_score=fixed.sum(axis=1);diff=score[:B]-score[B:]
        shares=gi.variance_shares(total)
        rep=dict(score=summary(score),fixed_score=summary(fixed_score),
            null_difference=summary(diff),resolution95=float(np.quantile(np.abs(diff),.95)),
            splits=split_out,
            moment_mean=dict(zip(keys,total.mean(axis=0))),
            moment_error_share=dict(zip(keys,total.mean(axis=0)/score.mean())),
            moment_variance_share=dict(zip(keys,shares)),
            fixed_jensen=dict(zip(keys,np.mean(fixed_jensen,axis=0))),
            fixed_center_error=float(np.mean(fixed_centers,axis=0).sum()),
            calibration_reselection_effect=float(score.mean()-fixed_score.mean()),
            bank80_score=float(result['result']['model_rel']))
        out['reps'][str(n)]=rep
        gi.write_json(Path(root)/'analysis'/f'{panel}.json',out)
        print(panel,n,rep['score'],flush=True)
    # Genuine disjoint draws, not bootstrap duplicates: four groups at 20,
    # two at 40. Reuse the same grouping on all origins/scales.
    out['disjoint_blocks']={}
    for n in (20,40):
        draws=np.arange(80).reshape(-1,n)
        scores=[]
        for split in splits:
            cal=[];tests=[]
            for sc in gi.SCALES:
                path=dest/str(split['origin'])
                tr=moments_from_bank(path/'train'/f'scale_{sc:g}',split['horizons'],draws)
                tests.append(moments_from_bank(path/'test'/f'scale_{sc:g}',split['horizons'],draws))
                em=split['train_calibration']
                cal.append(np.nanmean([w*np.abs(tr[k]-em[k])/max(abs(em[k]),1e-6)
                    for k,w in gi.rk.CAL_WEIGHTS.items() if np.isfinite(em.get(k,np.nan))],axis=0))
            select=np.argmin(np.array(cal),axis=0)
            sm={k:np.array([t[k] for t in tests])[select,np.arange(len(draws))] for k in keys}
            c,_=contribution_arrays(split['emp'],sm,keys);scores.append(c.sum(axis=1))
        out['disjoint_blocks'][str(n)]=dict(scores=np.mean(scores,axis=0).tolist(),
            per_split=np.array(scores).tolist(),seed_ranges=[[1000+int(r[0]),1000+int(r[-1])] for r in draws])
    gi.write_json(Path(root)/'analysis'/f'{panel}.json',out)
    return out


def ladder_analysis(ladder_root,analyses):
    rows=[];wrong=[]
    for panel in gi.PANELS:
        cfg=gi.le.PANELS[panel];cells={}
        for rung in cfg['gate_path']:
            cells[rung]=json.loads((Path(ladder_root)/'cells'/f'{panel}__gate__{rung}.json').read_text())
        for rung in ('R0','R1'):
            c=cells[rung]
            wrong.append(dict(panel=panel,rung=rung,model=c['model_rel'],baseline=c['base_rel'],
                excess=c['model_rel']-c['base_rel'],rejected_error_rule=c['model_rel']>c['base_rel']+.05,
                coverage=c['coverage']))
        for prev,cur in zip(cfg['gate_path'][:-1],cfg['gate_path'][1:]):
            a=np.array([s['model'] for s in cells[prev]['splits']]);b=np.array([s['model'] for s in cells[cur]['splits']])
            d=b-a;unpaired=float(np.var(a,ddof=1)+np.var(b,ddof=1))
            r=dict(panel=panel,previous=prev,current=cur,delta_mean=float(d.mean()),
                split_delta_sd=float(d.std(ddof=1)),unpaired_split_sd=float(np.sqrt(unpaired)),
                split_pair_variance_reduction=float(1-np.var(d,ddof=1)/unpaired) if unpaired else None,
                coverage=cells[cur]['coverage'],rule_fixed_005=bool(d.mean()<=.05),
                seed_CRN_reduction='NOT IDENTIFIABLE: stored cells contain pooled per-split moments only')
            if panel in analyses:
                floor=analyses[panel]['reps']['20']['resolution95']
                r.update(null_noise_floor=floor,rule_not_resolved_worsening=bool(d.mean()<=floor),
                         rule_005_plus_noise=bool(d.mean()+floor<=.05))
            rows.append(r)
    return dict(pairs=rows,wrong_model=wrong,
        caution='Rules illustrate candidate-minus-incumbent worsening only; no adoption. Split pairing is not seed-CRN evidence. Frozen-stack MC resolution need not apply to a different rung.')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out',default='llm_fitting/runs/2026-10-12_V1_gate_instrument')
    p.add_argument('--panel',choices=gi.PANELS)
    p.add_argument('--ladder-only',action='store_true')
    a=p.parse_args();analyses={}
    if not a.ladder_only:
        for panel in ([a.panel] if a.panel else gi.PANELS):
            analyses[panel]=analyse_panel(a.out,panel)
    for panel in gi.PANELS:
        f=Path(a.out)/'analysis'/f'{panel}.json'
        if f.exists():analyses[panel]=json.loads(f.read_text())
    gi.write_json(Path(a.out)/'analysis'/'ladder.json',ladder_analysis(
        gi.ROOT/'llm_fitting/runs/2026-10-12_ladder',analyses))

if __name__=='__main__':main()
