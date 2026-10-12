"""Controlled mathematical and observation tests, independent of native results."""
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"llm_fitting"))
from validation import planted_sim as s
from validation.planted_crosscheck import compare
import minimal_rankdiff as mrd

def params(n=2500,sp=0.,st=0.,so=0.,k=.2,phi=0.,df=np.inf):
    z=np.array([-15.,0.])
    return dict(N=n,z_knots=z,w0=np.zeros(n),bottom_mu=np.zeros(10),
                phi=np.full(2,phi),sigma_trans=np.full(2,st),
                sigma_perm=np.full(2,sp),sigma_obs=np.full(2,so),
                lam=np.zeros(2),exit_rate=np.zeros(2),kappa=k,
                kappa_z=np.full(2,k),temper_s=0.,mix_b=0.,t_df=df,sigma_F=0.)

def test_fixed_covariance():
    p=params(sp=.3,st=.5,so=.2,k=.2,phi=.4)
    x=s.simulate(p,180,seed=91234,track=p["N"],burn=350)["tvals"]
    for h in (0,1,4,13):
        measured=np.mean(x[h:]*x[:len(x)-h]) if h else np.mean(x*x)
        expected=s.fixed_covariance(h,.2,.3,.4,.5,.2)
        assert abs(measured-expected)<.012

def test_fat_lasting():
    p=params(sp=1.,k=1.)
    a=s.simulate(p,100,911,track=p["N"])["tvals"]
    b=s.simulate(p,100,911,mechanism="fat_lasting",strength=1,track=p["N"])["tvals"]
    assert np.mean(abs(b)>8)>20*np.mean(abs(a)>8)+1e-4

def test_off_returns_same_identity():
    p=params()
    a=s.simulate(p,150,911,mechanism="off",strength=1,track=p["N"])
    absent=a["tranks"]==0
    assert absent.mean()>.08
    returns=absent[:-1]&~absent[1:]
    assert returns.sum()>100
    assert np.all(a["tids"][1:][returns]==a["tids"][:-1][returns])

def test_slow_memory():
    p=params(sp=1.,k=.2)
    a=s.simulate(p,160,911,track=p["N"],burn=500)["tvals"]
    b=s.simulate(p,160,911,mechanism="slow_memory",strength=1,track=p["N"],burn=500)["tvals"]
    assert np.mean(b[30:]*b[:-30])-np.mean(a[30:]*a[:-30])>.4

def test_bounded_fast():
    p=params(st=1.,df=4.3)
    b=s.simulate(p,100,911,mechanism="bounded_fast",strength=1,track=p["N"])["tvals"]
    assert abs(b).max()<=4/np.sqrt(s.clip_variance(4.3,4))+1e-12
    assert abs(np.var(b)-1)<.025

def test_changing_volatility():
    p=params(st=1.)
    a=s.simulate(p,180,911,mechanism="gaussian_control",track=p["N"])["tvals"]
    b=s.simulate(p,180,911,mechanism="log_volatility",strength=1,track=p["N"])["tvals"]
    def corr(x): return np.corrcoef((x[10:]**2).ravel(),(x[:-10]**2).ravel())[0,1]
    assert corr(b)-corr(a)>.04

def test_random_walk():
    p=params(sp=1.,k=1.)
    b=s.simulate(p,150,911,mechanism="random_walk",strength=1,track=p["N"])["tvals"]
    ratio=np.mean((b[10:]-b[:-10])**2)/np.mean(np.diff(b,axis=0)**2)
    assert 9<ratio<11

def test_replacement_ids():
    p=params(n=20);p["exit_rate"][:]=1
    a=s.simulate(p,4,811,track=20,burn=0)
    assert len(np.unique(a["tids"]))==80
    assert not np.intersect1d(a["top_ids"][0],a["top_ids"][1]).size

def test_loader_mapping(tmp_path):
    sim=dict(full_values=np.array([[-1.,0.,2.],[1.,np.nan,1.]]),
             full_ids=np.array([[10,2,3],[10,2,3]]))
    d=s.registered_panel(sim)
    f=tmp_path/"fixture.parquet";d[["entity_id","ts","metric"]].to_parquet(f,index=False)
    loaded=mrd.load_panel(dict(path=str(f),id_col="entity_id",ts_col="ts",metric_col="metric",max_rank=None))
    pd.testing.assert_frame_equal(d[loaded.columns],loaded,check_dtype=False)
    assert (d.metric==0).sum()==2
    assert len(d)==5

def test_permutation_zero_and_large_discrepancy():
    a=np.tile(np.arange(20)[:,None],(1,3))
    c=compare(a,a,permutations=2000)
    assert np.all(c["p"]==1)
    d=compare(a,a+100,permutations=2000)
    assert np.all(d["reject"])
    assert np.all(d["p"]>0)
    with pytest.raises(ValueError): compare(a,np.full_like(a,np.nan,dtype=float))
