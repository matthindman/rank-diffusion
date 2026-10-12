"""Pre-native checks for the dated V2 additions."""
import sys
from pathlib import Path
import numpy as np
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"llm_fitting"))
from validation import planted_sim as old
from validation import planted_extended as new
from test_validation_planted_sim import params

@pytest.mark.parametrize("mechanism",list(old.MECHANISMS))
def test_original_mechanisms_remain_bit_identical(mechanism):
    p=params(n=80,sp=.2,st=.4,so=.1,df=5.)
    p["exit_rate"][:]=.01;p["temper_s"]=.6;p["mix_b"]=.7
    p["lam"][:]=.2;p["sigma_F"]=.3;p["factor_rho"]=.4
    p["phi2"]=np.full(2,.8);p["sigma_trans2"]=np.full(2,.1)
    a=old.simulate(p,30,873,mechanism=mechanism,strength=1,track=80)
    b=new.simulate(p,30,873,mechanism=mechanism,strength=1,track=80)
    for k in a:np.testing.assert_array_equal(a[k],b[k])

def test_current_rank_pull_constrains_ladder_relative_to_random_walk():
    p=params(n=2000,sp=1.,k=0.);p["w0"]=np.linspace(10,0,p["N"])
    rw=old.simulate(p,150,673,mechanism="random_walk",strength=1,track=p["N"])["tvals"]
    cur=new.simulate(p,150,673,mechanism="ladder_attraction",strength=1,track=p["N"])["tvals"]
    def error(x):return np.sqrt(np.mean((np.sort(x[-20:],axis=1)[:,::-1]-p["w0"])**2))
    assert error(cur)<.4*error(rw)

def test_annual_cycle_has_declared_period_and_nonzero_amplitude():
    p=params(n=2500)
    x=new.simulate(p,156,783,mechanism="annual_cycle",strength=1,track=p["N"])["tvals"]
    np.testing.assert_allclose(x[52:],x[:-52],atol=1e-12)
    amp=(x.max(axis=0)-x.min(axis=0))/2
    assert 1.35<amp.mean()<1.65
    assert np.mean(x*x)>.5
