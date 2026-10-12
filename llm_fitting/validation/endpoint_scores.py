"""Proper scores for coarsened log-rank forecasts, conditional on fitted inputs."""
import numpy as np

def coarsened_log_rank(rank, K):
    a=np.asarray(rank)
    if K<1 or np.any(a<0) or not np.all(np.isfinite(a)) or np.any(a!=np.floor(a)):
        raise ValueError("Ranks must be finite, nonnegative, with zero for absence.")
    return np.log(np.where((a==0)|(a>K),K+1,a).astype(float))

def _sample_arrays(samples, observed):
    x,y=np.asarray(samples,float),np.asarray(observed,float)
    if x.ndim!=2 or y.shape!=(x.shape[0],) or x.shape[1]<1:
        raise ValueError("Expected samples (cases, members) and observations (cases,).")
    if not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
        raise ValueError("Missing values must be encoded as the declared outside state.")
    return x,y

def crps(samples,observed,*,iid=True):
    """Fair ensemble CRPS for iid draws; exact empirical-distribution CRPS otherwise."""
    x,y=_sample_arrays(samples,observed);m=x.shape[1]
    if iid and m<2: raise ValueError("Fair ensemble CRPS requires at least two iid members.")
    ordered=np.sort(x,axis=1)
    pair_sum=ordered@(2*np.arange(m)-m+1)
    denominator=m*(m-1) if iid else m*m
    return np.mean(np.abs(x-y[:,None]),axis=1)-pair_sum/denominator

def brier(events,observed,*,iid=True):
    x,y=_sample_arrays(events,observed);m=x.shape[1]
    if not np.all((x==0)|(x==1)) or not np.all((y==0)|(y==1)):
        raise ValueError("Brier score requires binary samples and labels.")
    if iid and m<2: raise ValueError("Fair ensemble Brier requires at least two iid members.")
    p=x.mean(axis=1)
    return (p-y)**2-(p*(1-p)/(m-1) if iid else 0.)

def percentile_calibration(samples,observed,levels=(.1,.5,.9)):
    """Expected randomized ensemble PIT indicators, with ties integrated exactly.

    With M iid predictive members and an exchangeable verifying outcome these
    indicators average to their nominal levels, including discrete atoms.
    Raw empirical quantile brackets are also returned as descriptive values.
    """
    x,y=_sample_arrays(samples,observed);m=x.shape[1]
    less=(x<y[:,None]).sum(axis=1);equal=(x==y[:,None]).sum(axis=1)
    rows={}
    for a in levels:
        if not 0<a<1: raise ValueError("Levels must be inside (0,1).")
        j=int(np.ceil(m*a))
        q=np.sort(x,axis=1)[:,j-1]
        rows[a]={"rank_cdf":np.clip((a*(m+1)-less)/(equal+1),0,1),
                 "below_quantile":(y<q).astype(float),
                 "at_or_below_quantile":(y<=q).astype(float),
                 "continuous_order_statistic_reference":j/(m+1)}
    return rows

def outside_within(paths,K,h):
    """paths (..., time, entity); labels include absence and observed ranks beyond K."""
    p=np.asarray(paths)
    if h<1 or h>p.shape[-2]: raise ValueError("Unavailable horizon.")
    return np.any((p[...,:h,:]==0)|(p[...,:h,:]>K),axis=-2)

def crps_jackknife(samples,observed,*,iid=True):
    """Scores and leave-one-member scores. Conditional simulation MC only."""
    x,y=_sample_arrays(samples,observed);m=x.shape[1]
    score=crps(x,y,iid=iid)
    if m==1 and not iid:return score,score[:,None]
    if iid and m<3:raise ValueError("Jackknife requires at least three iid members.")
    order=np.argsort(x,axis=1);ordered=np.take_along_axis(x,order,axis=1)
    pair_sum=ordered@(2*np.arange(m)-m+1)
    left=np.cumsum(ordered,axis=1)-ordered
    dist_sorted=(2*np.arange(m)-m)*ordered+ordered.sum(axis=1)[:,None]-2*left
    dist=np.empty_like(x);np.put_along_axis(dist,order,dist_sorted,axis=1)
    abs_error=np.abs(x-y[:,None])
    d=(m-1)*(m-2) if iid else (m-1)**2
    jack=(abs_error.sum(axis=1)[:,None]-abs_error)/(m-1)-(pair_sum[:,None]-dist)/d
    return score,jack

def brier_jackknife(samples,observed,*,iid=True):
    x,y=_sample_arrays(samples,observed);m=x.shape[1]
    score=brier(x,y,iid=iid)
    if m==1 and not iid:return score,score[:,None]
    if iid and m<3:raise ValueError("Jackknife requires at least three iid members.")
    p=(x.sum(axis=1)[:,None]-x)/(m-1)
    jack=(p-y[:,None])**2-(p*(1-p)/(m-2) if iid else 0.)
    return score,jack

def mcse_from_jackknife(jack):
    a=np.asarray(jack,float)
    m=a.shape[-1]
    return np.sqrt((m-1)/m*np.sum((a-a.mean(axis=-1,keepdims=True))**2,axis=-1))

def rank_bins(rank,K):
    r=np.asarray(rank)
    if np.any(r<0) or not np.all(np.isfinite(r)):raise ValueError("Invalid ranks")
    return np.where(r==0,0,1+np.searchsorted(np.array([.02,.1,.5,1.,2.])*K,r,side="left"))

def cohort_masks(permanent_rank,presence,origin_rank,K):
    pr=np.asarray(permanent_rank,float);pres=np.asarray(presence,float);r=np.asarray(origin_rank)
    if not (pr.shape==pres.shape==r.shape) or not np.all(np.isfinite(pr)):raise ValueError("Invalid cohort metadata")
    established=(pr<=K/2)&(pres>=.7)
    return {"all":np.ones(pr.shape,bool),"head":pr<=.1*K,
            "middle":(pr>.1*K)&(pr<=.5*K),"shell":(pr>.5*K)&(pr<=K),
            "tail":pr>K,"established":established,
            "established_origin_inside":established&(r>0)&(r<=K)}

def calendar_block_weights(origins,block=13,reps=2000,seed=20261012):
    """Circular calendar blocks of whole origin bundles; descriptive only.

    Returns None where calendar support cannot contain two blocks or there are
    fewer than three distinct origins. No endpoint or split-iid resampling.
    """
    origins=np.asarray(origins,int)
    if len(np.unique(origins))!=len(origins):raise ValueError("Origins must be unique")
    if len(origins)<3:return None
    width=int(origins.max()-origins.min()+1)
    if width<2*block:return None
    rng=np.random.default_rng(seed);offset=origins-origins.min()
    weights=[];empty=0
    while len(weights)<reps:
        starts=rng.integers(0,width,size=int(np.ceil(width/block)))
        weeks=((starts[:,None]+np.arange(block))%width).ravel()[:width]
        counts=np.bincount(weeks,minlength=width)[offset]
        if not counts.sum():
            empty+=1
            if empty>100*reps:raise RuntimeError("No origins in bootstrap support")
            continue
        weights.append(counts/counts.sum())
    return {"weights":np.asarray(weights),"empty_draws":empty,
            "span_weeks":width,"block_weeks":block,"replications":reps}
