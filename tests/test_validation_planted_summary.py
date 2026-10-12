from pathlib import Path
import sys
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"llm_fitting"))
from validation.planted_summary import rate_interval, summary, separation, group_for

def test_wilson_zero_retains_upper_uncertainty():
    r=rate_interval(0,20)
    assert r["rate"]==0 and .15<r["upper"]<.17
    assert rate_interval(0,0)["rate"] is None

def test_recovery_retains_denominators():
    s=summary([1.,2.,3.,np.nan,np.inf],2.)
    assert s["requested"]==5 and s["valid"]==3 and s["positive_infinity"]==1
    assert s["bias"]==0
    assert np.isclose(s["rmse"],np.sqrt(2/3))
    assert s["mcse_rmse"]>0 and s["mcse_sd"]>0

def test_pair_separation_does_not_invent_precision():
    a=np.linspace(-1,1,20)
    assert separation(a,a)["label"]=="unresolved"
    assert separation(a,a+10)["label"]=="separated"
    assert separation(np.ones(20),np.ones(20)*2)["label"]=="degenerate"

def test_rank_groups_include_tail():
    ranks=np.array([1,10,30,70,200.])
    z=np.log((ranks-.5)/400)
    assert list(group_for(z,400,100))==["head","head","middle","shell","tail"]


def test_truth_is_retained_when_all_estimates_are_infinite():
    s=summary([np.inf]*20,5.)
    assert s["truth"]==5. and s["valid"]==0 and s["positive_infinity"]==20
