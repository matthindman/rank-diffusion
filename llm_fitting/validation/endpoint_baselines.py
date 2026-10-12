"""Training-only comparator laws for supplementary endpoint scoring."""
import numpy as np
from validation import endpoint_scores as es
HORIZONS=(1,4,13)
def training_ranks(df,ids):
    T=int(df.period.max())+1
    return df.pivot(index="period",columns="entity_id",values="rank").reindex(index=range(T),columns=ids).fillna(0).to_numpy(dtype=np.int32)
def canonical_uniforms(origin_rank,m,seed):
    """Relabel-invariant iid members indexed by unique numeric origin ranks."""
    unique,inverse=np.unique(origin_rank,return_inverse=True)
    return np.random.default_rng(seed).random((len(unique),m))[inverse]
def baseline_samples(df,ids,K,m=20,seed=2026101203):
    """Terminal coarsened ranks and matched any-outside event at direct horizons.

    The historical absolute-displacement law gets a symmetric sign lift.
    Its survivor marginal has no joint path and thus no event Brier forecast.
    For present origins, kernel draws transfer complete signed-rank-change
    snippets within the origin bin, anchoring them at the endpoint's exact rank.
    Absence in a snippet stays absence; crossings are recomputed after the shift.
    For absent origins, use the empirical joint terminal-state/event law.
    """
    import rankdiff_kalman as rk
    R=training_ranks(df,ids);T,n=R.shape;origin=R[-1]
    valid_h=[h for h in HORIZONS if h<T]
    history=rk.emp_dist(df,valid_h)[0] if valid_h else {}
    terminal={};events={};meta={}
    codes=es.rank_bins(origin,K)
    for h in HORIZONS:
        no=np.where((origin==0)|(origin>K),K+1,origin)
        terminal[("no_change",h)]=np.repeat(no[:,None],m,axis=1)
        events[("no_change",h)]=np.repeat(((origin==0)|(origin>K))[:,None],m,axis=1)
        if h>=T:
            meta[h]={"available":False,"reason":"fewer than h+1 training weeks"}
            continue
        u=canonical_uniforms(origin,m,seed+101*h)
        d=np.sort(np.asarray(history[h],float))
        if d.size:
            # Two equal-probability signs, including doubled zero mass.
            signed=np.sort(np.r_[-d,d])
            sampled=signed[np.minimum((u*len(signed)).astype(int),len(signed)-1)]
            raw=np.maximum(1,origin[:,None]+sampled)
            raw[origin==0]=K+1
            terminal[("historical_mobility",h)]=np.where(raw>K,K+1,raw).astype(np.int32)
        rb=es.rank_bins(R[:-h],K)
        target=R[h:];coarse=np.where((target==0)|(target>K),K+1,target)
        outside=(R==0)|(R>K)
        running=np.vstack([np.zeros((1,n),int),np.cumsum(outside,axis=0)])
        within=(running[h+1:]-running[1:T-h+1])>0
        draw=np.empty((n,m),np.int32);event=np.empty((n,m),bool);count={};fallback=[]
        for b in range(7):
            take=codes==b
            tt,cc=np.nonzero(rb==b)
            count[b]=len(tt)
            if not len(tt):
                draw[take]=no[take,None];event[take]=((origin[take]==0)|(origin[take]>K))[:,None]
                if take.any():fallback.append(b)
                continue
            if not take.any():continue
            if b==0:
                pairs=np.column_stack([coarse[rb==b],within[rb==b].astype(int)])
                states,counts=np.unique(pairs,axis=0,return_counts=True)
                ix=np.searchsorted(np.cumsum(counts)/counts.sum(),u[take],side="right")
                draw[take]=states[ix,0];event[take]=states[ix,1]
            else:
                # Positive ranks are unique within a week. This ordering is
                # unchanged by endpoint names or dataframe/column order.
                order=np.lexsort((R[tt,cc],tt))
                ix=np.minimum((u[take]*len(order)).astype(int),len(order)-1)
                t,c=tt[order[ix]],cc[order[ix]]
                start=R[t,c];current=origin[take,None]
                hit=np.zeros(t.shape,bool)
                for step in range(1,h+1):
                    future=R[t+step,c]
                    shifted=np.where(future==0,0,np.maximum(1,current+future-start))
                    hit|=(shifted==0)|(shifted>K)
                draw[take]=np.where((shifted==0)|(shifted>K),K+1,shifted)
                event[take]=hit
        terminal[("rank_bin",h)]=draw
        events[("rank_bin",h)]=event
        meta[h]={"available":True,"historical_pairs":len(d),"kernel_pairs_by_bin":count,
                 "fallback_no_change_bins":fallback}
    return terminal,events,meta
