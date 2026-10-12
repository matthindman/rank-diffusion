import json
from pathlib import Path
import sys
import warnings
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"llm_fitting"))
from validation import planted_warning_guard as guard
from validation import planted_crosscheck as pc
from validation import planted_sim as sim
from test_validation_planted_sim import params

def test_warning_guard_bounds_capture_and_stamps_provenance(tmp_path):
    old=warnings.simplefilter
    with guard.bounded_warnings():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            for _ in range(1000):warnings.warn("repeated empty identity",RuntimeWarning)
        assert len(caught)==1
        f=tmp_path/"result.json";pc.write(f,dict(value=3))
        d=json.loads(f.read_text())
        assert d["value"]==3 and d["execution_wrapper"]["sha256"]==pc.digest(guard.__file__)
    assert warnings.simplefilter is old

def test_warning_guard_does_not_change_paths_or_numeric_operations():
    p=params(n=100,sp=.1,st=.3,so=.1)
    a=sim.simulate(p,20,44)
    with guard.bounded_warnings():b=sim.simulate(p,20,44)
    for key in a:np.testing.assert_array_equal(a[key],b[key])
    x=np.array([[1.,np.nan,3.],[2.,np.nan,4.]])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore");a=np.nanmedian(x,axis=0)
    with guard.bounded_warnings():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore");b=np.nanmedian(x,axis=0)
    np.testing.assert_array_equal(a,b)

def test_attempt_names_distinguish_concurrent_workers_and_stages(tmp_path,monkeypatch):
    f=tmp_path/"attempts"/"123456.json"
    with guard.bounded_warnings():
        monkeypatch.setattr(guard.os,"getpid",lambda:100)
        pc.write(f,dict(stage="recovery",panel="a",status="STARTED"))
        pc.write(f,dict(stage="recovery",panel="a",status="RAN-OK"))
        monkeypatch.setattr(guard.os,"getpid",lambda:200)
        pc.write(f,dict(stage="recovery",panel="b",status="STARTED"))
    files=list(f.parent.glob("*.json"))
    assert len(files)==2
    assert {json.loads(p.read_text())["status"] for p in files}=={"STARTED","RAN-OK"}
