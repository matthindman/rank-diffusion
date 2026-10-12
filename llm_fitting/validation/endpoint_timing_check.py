"""Executable timing audit of the frozen conditional export; no model edit."""
import json,sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
import rankdiff_kalman as rk
class KnownInnovations:
    def __init__(self):self.calls=0
    def standard_normal(self,n):
        v=np.array([-.75,0.,0.]) if self.calls%3==0 else np.zeros(n)
        self.calls+=1
        return v
    def random(self,n):return np.ones(n)
def check():
    p=SimpleNamespace(z_knots=np.array([-10.,0.]),phi=np.zeros(2),
        sigma_trans=np.zeros(2),sigma_perm=np.ones(2),sigma_obs=np.zeros(2),
        kappa_z=np.zeros(2),exit_rate=np.zeros(2),t_df=np.inf,bottom_mu=np.zeros(1))
    rng=KnownInnovations();export={}
    with patch.object(rk,"_filtered_levels",return_value=pd.Series([3.,2.,1.],index=["a","b","c"])),\
         patch.object(rk.np.random,"default_rng",return_value=rng):
        rk.sim_cohort_conditional(p,pd.DataFrame(),1,0.,export=export)
    return {"expected_first_week_ranks":[1,2,3],"exported_nominal_h1":export["rank"][0].tolist(),
            "permanent_updates":rng.calls//3,
            "first_week_matches_one_transition":export["rank"][0].tolist()==[1,2,3],
            "construction":"a starts 3, b 2, c 1. Each lasting innovation is [-.75,0,0]; all other layers and exits zero."}
if __name__=="__main__":print(json.dumps(check(),indent=2))
