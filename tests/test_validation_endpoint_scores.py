"""Known-answer checks for the supplementary per-endpoint scorer."""
import itertools,sys
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"llm_fitting"))
from validation import endpoint_scores as es
from validation import endpoint_baselines as eb

def test_conditional_truth_beats_misspecification_while_pooled_laws_tie():
    truth=np.array([0.,10.]);conditional=np.array([[0.,0.],[10.,10.]])
    blind=np.array([[0.,10.],[0.,10.]])
    assert es.crps(conditional,truth,iid=False).mean()==0
    assert es.crps(blind,truth,iid=False).mean()==2.5
    assert es.crps(conditional[::-1],truth,iid=False).mean()==10
    pooled_a=np.tile(conditional.ravel(),(2,1));pooled_b=np.tile(blind.ravel(),(2,1))
    np.testing.assert_array_equal(es.crps(pooled_a,truth,iid=False),es.crps(pooled_b,truth,iid=False))

def test_fair_scores_average_to_distribution_score_over_all_small_ensembles():
    m=4;p=.3;y=np.array([0.])
    fair_c=fair_b=emp_c=0.
    for sample in itertools.product((0.,1.),repeat=m):
        sample=np.array(sample);w=p**sample.sum()*(1-p)**(m-sample.sum())
        fair_c+=w*es.crps(sample[None],y)[0]
        fair_b+=w*es.brier(sample[None],y)[0]
        emp_c+=w*es.crps(sample[None],y,iid=False)[0]
    assert fair_c==pytest.approx(p*p)
    assert fair_b==pytest.approx(p*p)
    assert emp_c==pytest.approx(p*p+p*(1-p)/m)

def test_randomized_ensemble_pit_is_uniform_even_with_atoms():
    m=3;p=.3;mean={a:0. for a in (.1,.5,.9)}
    for seq in itertools.product((0.,1.),repeat=m+1):
        a=np.array(seq);weight=p**a.sum()*(1-p)**(m+1-a.sum())
        out=es.percentile_calibration(a[:-1][None],a[-1:])
        for level in mean:mean[level]+=weight*out[level]["rank_cdf"][0]
    for level,value in mean.items():assert value==pytest.approx(level)

@pytest.mark.parametrize("kind",["crps","brier"])
def test_member_jackknife_matches_deleting_whole_members(kind):
    rng=np.random.default_rng(431)
    x=rng.normal(size=(11,7));y=rng.normal(size=11)
    if kind=="brier":x=(x>0).astype(float);y=(y>0).astype(float)
    fn=getattr(es,kind);score,jack=getattr(es,kind+"_jackknife")(x,y)
    np.testing.assert_allclose(score,fn(x,y),atol=1e-14)
    want=np.column_stack([fn(np.delete(x,j,axis=1),y) for j in range(x.shape[1])])
    np.testing.assert_allclose(jack,want,atol=1e-14)

def test_outside_then_return_is_an_event_and_coarsening_is_explicit():
    R=np.array([[2,4,0],[0,5,1],[2,4,1]])
    np.testing.assert_array_equal(es.outside_within(R,4,3),[True,True,True])
    np.testing.assert_allclose(es.coarsened_log_rank(np.array([0,1,4,8]),4),np.log([5,1,4,5]))
    assert es.brier(np.array([[1.,1.]]),np.array([1.]))[0]==0
    with pytest.raises(ValueError):es.coarsened_log_rank(np.array([np.nan]),4)

def fixture_panel():
    rng=np.random.default_rng(148)
    r=[]
    for t in range(32):
        order=rng.permutation(12)
        for j in range(12):
            if (j+t)%11==0:continue
            r.append({"period":t,"entity_id":f"id{j}","rank":int(order[j]+1)})
    return pd.DataFrame(r)

def test_baselines_are_invariant_to_id_relabelling_and_column_order():
    df=fixture_panel();ids=np.array(sorted(df.entity_id.unique()));K=6
    a,e,meta=eb.baseline_samples(df,ids,K)
    renamed={x:f"renamed_{len(ids)-i}" for i,x in enumerate(ids)}
    rev=np.arange(len(ids))[::-1];changed=df.copy();changed.entity_id=changed.entity_id.map(renamed)
    b,f,other=eb.baseline_samples(changed,np.array([renamed[x] for x in ids[rev]]),K)
    for key in a:np.testing.assert_array_equal(a[key][rev],b[key])
    for key in e:np.testing.assert_array_equal(e[key][rev],f[key])
    assert meta==other

def test_kernel_terminal_event_pairs_are_training_only_and_consistent():
    df=fixture_panel();ids=np.array(sorted(df.entity_id.unique()));K=6
    ranks,events,meta=eb.baseline_samples(df,ids,K,m=20)
    for h in (1,4,13):
        r=ranks[("rank_bin",h)];event=events[("rank_bin",h)]
        assert np.all(event[r>K])
        assert sum(meta[h]["kernel_pairs_by_bin"].values())==(32-h)*len(ids)
        assert ("historical_mobility",h) not in events
    short=df[df.period<10]
    r,e,meta=eb.baseline_samples(short,ids,K)
    assert ("rank_bin",13) not in r and ("historical_mobility",13) not in r
    assert not meta[13]["available"]

def test_calendar_blocks_keep_whole_origin_bundles_and_withhold_short_span():
    assert es.calendar_block_weights([12,14,15,16,18],block=13) is None
    a=es.calendar_block_weights([21,32,43,54,65],block=13,reps=100)
    np.testing.assert_allclose(a["weights"].sum(axis=1),1.)
    assert a["weights"].shape==(100,5)
    assert es.calendar_block_weights([21,32,43,54,65],block=26) is None

def test_cohorts_use_absence_penalized_permanent_rank():
    m=es.cohort_masks([1,4,9,18],[1,.6,.9,1],[0,1,4,2],10)
    np.testing.assert_array_equal(m["established"],[True,False,False,False])
    np.testing.assert_array_equal(m["established_origin_inside"],[False,False,False,False])

def test_conditional_wrapper_primitives_and_cache_replay_are_exact(tmp_path,monkeypatch):
    from dataclasses import make_dataclass
    import rankdiff_kalman as rk
    from validation.endpoint_run import EndpointWrapper
    values=dict(z_knots=np.array([-10.,0.]),phi=np.array([.3,.3]),
        sigma_trans=np.array([.12,.1]),sigma_perm=np.array([.04,.04]),
        sigma_obs=np.array([.05,.05]),kappa_z=np.array([.02,.02]),
        exit_rate=np.array([.002,.002]),t_df=5.,bottom_mu=np.array([.1]))
    P=make_dataclass("FixtureParameters",[(k,object) for k in values])
    p=P(**values);df=pd.DataFrame()
    levels=pd.Series(np.linspace(6.,0.,250),index=[f"e{i}" for i in range(250)])
    monkeypatch.setattr(rk,"_filtered_levels",lambda df,p:levels.copy())
    original_export=[];want=rk.sim_dist_cond(p,df,10,[1,4],reps=3,export=original_export)
    w=EndpointWrapper(tmp_path/"bank",tmp_path/"exports");w.state={"origin":40}
    observed_export=[]
    got=w.sim_test(p,df,10,[1,4],reps=3,export=observed_export)
    for h in (1,4):np.testing.assert_array_equal(np.sort(want[0][h]),np.sort(got[0][h]))
    assert want[1:]==got[1:]
    for a,b in zip(original_export,observed_export):
        for key in ("rank","entity_id"):np.testing.assert_array_equal(a[key],b[key])
    monkeypatch.setattr(rk,"sim_cohort_conditional",lambda *a,**k:pytest.fail("cache not used"))
    replay=w.sim_test(p,df,10,[1,4],reps=3)
    for h in (1,4):np.testing.assert_array_equal(got[0][h],replay[0][h])
    assert got[1:]==replay[1:]
