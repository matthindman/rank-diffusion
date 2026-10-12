"""Exact eligibility reduction for V2 diagnostics, leaving every estimand intact.

All full-window residual statistics use permanent rank <=K and presence>=.7;
departures/fate use train permanent rank <=K/2 and train presence>=.7.
Only identities outside BOTH sets can be omitted. The absent-rank floor is
the original N+1, never recomputed from the reduced matrices. Identity order
matches residual_anatomy.from_sim exactly. Runtime-only opt-in wrapper.
"""
from contextlib import contextmanager
from pathlib import Path
import fcntl,sys
import numpy as np
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import planted_crosscheck as pc
from validation import planted_study as study
from validation.planted_warning_guard import bounded_warnings
import residual_anatomy as ra

def eligible_identities(s,K):
    ranks,values,ids=(np.asarray(s[k]) for k in ("tranks","tvals","tids"))
    T,n=ranks.shape;t0=T//2;floor=n+1
    rr=[];xx=[];total=0
    for j in range(n):
        cuts=np.flatnonzero(ids[1:,j]!=ids[:-1,j])+1
        for a,b in zip(np.r_[0,cuts],np.r_[cuts,T]):
            total+=1;r=ranks[a:b,j];positive=r>0;present=int(positive.sum())
            full=present>=.7*T and (r[positive].sum()+floor*(T-present))/T<=K
            rtr=ranks[a:min(b,t0),j] if a<t0 else np.array([],dtype=np.int32)
            tp=rtr>0;seen=int(tp.sum())
            train=seen>=.7*t0 and (rtr[tp].sum()+floor*(t0-seen))/t0<=K/2
            if not (full or train):continue
            rc=np.zeros(T,np.int32);xc=np.full(T,np.nan)
            rc[a:b]=r;xc[a:b]=values[a:b,j];xc[rc<=0]=np.nan
            rr.append(rc);xx.append(xc)
    if not rr:return np.zeros((T,0),np.int32),np.empty((T,0)),total
    return np.stack(rr,axis=1),np.stack(xx,axis=1),total

@contextmanager
def eligible_statistics():
    original=study.diagnostics;original_write=pc.write
    meta=dict(path="llm_fitting/validation/planted_efficient_run.py",sha256=pc.digest(__file__),
              criterion="union of declared full-window and train-only cohorts; original N+1 floor")
    def diagnostic(s,K):
        R,X,total=eligible_identities(s,K)
        builder=ra.from_sim
        def reduced(arg):
            if arg is not s:raise RuntimeError("Unexpected nested simulator input")
            return R,X
        ra.from_sim=reduced
        try:out=original(s,K)
        finally:ra.from_sim=builder
        out["identities"]=total
        return out
    def write(path,data):
        if isinstance(data,dict):data={**data,"eligibility_wrapper":meta}
        return original_write(path,data)
    study.diagnostics=diagnostic;pc.write=write
    try:yield
    finally:study.diagnostics=original;pc.write=original_write

@contextmanager
def diagnostic_lock(argv,mode):
    """Serialize duplicate diagnostic workers while allowing separate fits."""
    if mode!="study" or len(argv)<2 or argv[1]!="diagnostics":
        yield
        return
    out=Path(argv[argv.index("--out")+1]);panel=argv[argv.index("--panel")+1]
    directory=out/"locks";directory.mkdir(parents=True,exist_ok=True)
    with (directory/f"diagnostics_{panel}.lock").open("a+") as handle:
        fcntl.flock(handle,fcntl.LOCK_EX)
        try:yield
        finally:fcntl.flock(handle,fcntl.LOCK_UN)

def main():
    if len(sys.argv)<2 or sys.argv[1] not in ("study","extension"):
        raise SystemExit("Usage: planted_efficient_run.py {study|extension} [runner args]")
    mode=sys.argv.pop(1)
    if mode=="study":runner=study
    else:
        from validation import planted_extension as runner
    with diagnostic_lock(sys.argv,mode),bounded_warnings(),eligible_statistics():runner.main()
if __name__=="__main__":main()
