"""Known-answer and parity checks for the V1 measuring instrument."""
import sys
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'llm_fitting/validation'))
import gate_instrument as gi


def params(n=100):
    a=lambda x:np.full(2,x)
    return gi.mrd.RankParams(z_knots=np.array([-10.,0.]),phi=a(.2),
        sigma_trans=a(.2),sigma_perm=a(.04),sigma_obs=a(.15),lam=a(0.),
        exit_rate=a(.001),T_curve=a(0.),kappa=.05,sigma_F=0.,N=n,
        w0=np.linspace(8,2,n),bottom_mu=np.array([1.,2.]),temper_s=.4,
        kappa_z=a(.05),t_df=5.,mix_b=1.)


def panel(n=100,T=15):
    rng=np.random.default_rng(2)
    x=np.linspace(8,2,n)+rng.normal(0,.25,(T,n))
    ranks=np.empty((T,n),int)
    ranks[np.arange(T)[:,None],np.argsort(-x,axis=1)]=np.arange(1,n+1)
    return pd.DataFrame(dict(period=np.repeat(np.arange(T),n),
        entity_id=np.tile(np.arange(n).astype(str),T),X=x.ravel(),rank=ranks.ravel()))


def test_sparse_quantile_preserves_unequal_counts_and_even_median():
    records=[gi.pack_rep({1:np.array([0.,0.,0.,0.,0.])},.2,{'coll1':.2}),
             gi.pack_rep({1:np.array([10.])},.6,{'coll1':.4})]
    with np.errstate(invalid='ignore'):
        dist,rf,cc=gi.pool_records(records,[1])
    assert np.median(dist[1])==0.  # mean of replicate medians would be 5
    assert rf==.4
    assert cc[1]==pytest.approx(.3)
    assert gi.histogram_quantile([1,3],[1,1])==2
    assert np.isnan(gi.histogram_quantile([1],[0]))
    for q in [.1,.5,.9]:
        assert gi.histogram_quantile([0,10],[5,1],q)==np.quantile(dist[1],q)


def test_floor_and_unavailable_horizon_are_not_zero_error_inclusions():
    em={'dRank1':2.,'dRank4':4.,'RACF1':.4,'coll1':0.,'coll5':.019,'coll20':.02}
    sm=dict(em,dRank1=4.,coll1=1.,coll5=1.,coll20=.04)
    c,keys=gi.gate_components(em,sm,[1,4])
    assert keys==['dRank1','dRank4','RACF1','coll20']
    assert sum(c.values())==.5
    assert 'dRank13' not in c


def test_absolute_noise_bias_and_null_difference_have_known_size():
    rng=np.random.default_rng(81)
    # Normal sample-mean noise about a perfectly correct scalar moment.
    for reps in (3,20,40):
        x=rng.normal(0,1/np.sqrt(reps),size=(100000,2))
        e=np.abs(x)
        assert e.mean()==pytest.approx(np.sqrt(2/np.pi/reps),rel=.015)
        assert np.std(e[:,0]-e[:,1])==pytest.approx(
            np.sqrt(2*(1-2/np.pi)/reps),rel=.015)


def test_covariance_variance_allocation_includes_cross_terms():
    x=np.arange(20.)
    shares=gi.variance_shares(np.column_stack([2*x,-x,.5*x]))
    np.testing.assert_allclose(shares,[4/3,-2/3,1/3])
    assert shares.sum()==pytest.approx(1)


def test_bank_matches_original_unconditional_distribution(tmp_path):
    p=params(); hor=[1,4,13]
    expected=gi.rk.sim_dist(p,18,hor,reps=3)
    w=gi.BankWrapper(tmp_path,0);w.state={'origin':15}
    actual=w.bank(p,18,hor,3,'train',1.)
    for h in hor:
        np.testing.assert_array_equal(np.sort(expected[0][h]),np.sort(actual[0][h]))
    assert expected[1]==actual[1]
    assert expected[2]==actual[2]
    # Completed seed files are exact restart checkpoints.
    with patch.object(gi.rk,'sim_cohort',side_effect=AssertionError('reran')):
        again=w.bank(p,18,hor,3,'train',1.)
    assert again[1]==actual[1]


def test_conditional_filter_cache_is_exact_and_originals_restore(tmp_path):
    p=params(); df=panel();hor=[1,4,13]
    expected=gi.rk.sim_dist_cond(p,df,18,hor,reps=3)
    # The original needs the training frame as its second argument.

    w=gi.BankWrapper(tmp_path,0)
    w.state={'origin':15,'p':p,'df_tr':df,'scale':1.}
    original=gi.rk.sim_dist_cond
    with w.install():
        actual=gi.rk.sim_dist_cond(p,df,18,hor,reps=3)
    assert gi.rk.sim_dist_cond is original
    for h in hor:
        np.testing.assert_array_equal(np.sort(expected[0][h]),np.sort(actual[0][h]))
    assert expected[1]==actual[1]
    assert expected[2]==actual[2]


def test_calibration_preserves_first_tie_and_train_moment(tmp_path):
    w=gi.BankWrapper(tmp_path,0);w.state={'origin':15,'p':params(),'calls':[]}
    d={1:np.arange(1.,10.),4:np.arange(1.,10.)}
    with patch.object(gi.rk,'emp_dist',return_value=(d,.3,{1:.5,5:.5,20:.5})), \
         patch.object(w,'bank',return_value=(d,.3,{1:.5,5:.5,20:.5})),w.install():
        assert gi.rk._calibrate_scale(params(),panel(),[1,4],15,reps=3)==1.
    assert w.state['cal_index']==9
    assert w.state['train_moments']['dRank1']==5.


def test_weighted_histogram_search_matches_expanded_pool():
    import gate_instrument_analysis as ga
    rng=np.random.default_rng(901)
    values=np.arange(73.)
    counts=rng.integers(0,8,size=(7,len(values)))
    weights=rng.integers(0,4,size=(33,7))
    weights[0]=0
    for q in (.1,.5,.9):
        actual=ga.pooled_hist_quantile(values,counts,weights,q)
        expected=gi.histogram_quantile(values,weights@counts,q)
        np.testing.assert_array_equal(actual,expected)


def test_vectorized_error_matches_frozen_gate_with_missing_moments():
    import gate_instrument_analysis as ga
    em={'dRank1':2.,'dRank4':4.,'RACF1':.4,'coll1':0.,'coll5':.019,'coll20':.02}
    keys=list(em)
    sims=[dict(em,dRank1=3.,coll20=.01),dict(em,RACF1=np.nan,dRank4=8.)]
    arrays={k:np.array([s[k] for s in sims]) for k in keys}
    actual,valid=ga.contribution_arrays(em,arrays,keys)
    for row,sim in zip(actual,sims):
        expected,active=gi.gate_components(em,sim,[1,4])
        np.testing.assert_allclose(row,[expected[k] for k in keys])


def test_bank_resampling_matches_literal_seed_repetition(tmp_path):
    import gate_instrument_analysis as ga
    records=[]
    for i in range(80):
        d={1:np.array([i%4]*((i%3)+1),float),4:np.array([i%7,i%9],float)}
        record=gi.pack_rep(d,i/80,{'coll1':i/90,'coll5':i/95,'coll20':i/100})
        records.append(record)
        np.savez(tmp_path/f'seed_{1000+i:04d}.npz',**record)
    draws=np.array([[0,0,2,79],[4,8,16,32],[79,1,1,1]])
    actual=ga.moments_from_bank(tmp_path,[1,4],draws)
    for i,draw in enumerate(draws):
        dist,rf,cc=gi.pool_records([records[j] for j in draw],[1,4])
        expected=gi.rk._moments(dist,rf,cc,[1,4])
        for k in actual:
            assert actual[k][i]==pytest.approx(expected[k],abs=1e-14)


def test_full_analysis_decomposition_is_exact_on_known_fixture(tmp_path):
    import gate_instrument_analysis as ga
    em={'dRank1':2.,'dRank4':4.,'RACF1':.4,'coll1':0.,'coll5':.2,'coll20':.2}
    root=tmp_path/'fixture/bank';root.mkdir(parents=True)
    gi.write_json(root/'COMPLETE.json',{})
    gi.write_json(root/'RESULT.json',{'result':{'model_rel':0.},'splits':[
        {'origin':12,'horizons':[1,4],'emp':em,'sim':em,'scale':0.,'train_calibration':em}]})
    def fake_bank(path,horizons,draws):
        scale=float(Path(path).name.removeprefix('scale_'))
        noise=(draws.mean(axis=1)-39.5)/40
        return {k:v*(1+.05*scale+.1*noise) for k,v in em.items()}
    with patch.object(ga,'moments_from_bank',side_effect=fake_bank):
        result=ga.analyse_panel(tmp_path,'fixture',B=17)
    assert result['floor']['coll1']['excluded_origins']==[12]
    assert len(result['disjoint_blocks']['20']['scores'])==4
    for r in result['reps'].values():
        assert r['score']['mean']==pytest.approx(
            r['calibration_reselection_effect']+sum(r['fixed_jensen'].values())+r['fixed_center_error'])
        assert sum(r['moment_error_share'].values())==pytest.approx(1)
        assert sum(r['moment_variance_share'].values())==pytest.approx(1)
