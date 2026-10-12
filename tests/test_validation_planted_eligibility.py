import sys
from pathlib import Path
import warnings
import numpy as np
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"llm_fitting"))
from validation import planted_sim as sim
from validation import planted_study as study
from validation import planted_crosscheck as pc
from validation.planted_efficient_run import eligible_identities,eligible_statistics
import residual_anatomy as ra
from test_validation_planted_sim import params

@pytest.mark.parametrize("mechanism",["baseline","off","random_walk"])
def test_eligibility_reduction_matches_every_statistic_and_count(mechanism):
    p=params(n=800,sp=.15,st=.3,so=.1,df=5.)
    p["w0"]=np.linspace(12,2,p["N"]);p["bottom_mu"]=np.array([1.,2.,3.])
    p["temper_s"]=.5;p["exit_rate"][:]=.01
    s=sim.simulate(p,136,655,mechanism=mechanism,strength=1,track=p["N"])
    R,X,total=eligible_identities(s,300)
    fullR,fullX=ra.from_sim(s)
    pr,pres=ra.perm_rank(fullR,floor=801)
    keep=((pr<=300)&(pres>=.7))|ra.cohort(fullR,0,68,150,floor=801)
    assert R.shape[1]<fullR.shape[1] and total==fullR.shape[1]
    np.testing.assert_array_equal(R,fullR[:,keep])
    np.testing.assert_array_equal(X,fullX[:,keep])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        a=study.diagnostics(s,300)
        with eligible_statistics():b=study.diagnostics(s,300)
    assert pc.clean(a)==pc.clean(b)

def test_diagnostic_lock_prevents_duplicate_stage_writers(tmp_path):
    import fcntl
    from validation.planted_efficient_run import diagnostic_lock
    argv=["runner","diagnostics","--out",str(tmp_path),"--panel","a"]
    with diagnostic_lock(argv,"study"):
        with (tmp_path/"locks"/"diagnostics_a.lock").open("a+") as f:
            with pytest.raises(BlockingIOError):
                fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
    with (tmp_path/"locks"/"diagnostics_a.lock").open("a+") as f:
        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
