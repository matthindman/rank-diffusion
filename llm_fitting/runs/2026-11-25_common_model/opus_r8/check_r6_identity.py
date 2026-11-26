"""Round 8 compatibility check: with drift_core=False and drift_rank_law=False the candidate must reproduce
the Round 6 source (baseline_r6/common_rankdiff_r6_source_copy.py) bit for bit: fitted scalars, filtered states,
and a short simulated stretch, for both arms on one DEVELOPMENT panel (training window of the first Reddit gate
origin with an active drift, T0 = 106)."""
import importlib.util, sys
from pathlib import Path
import numpy as np
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cand
spec = importlib.util.spec_from_file_location("r6src", HERE / "baseline_r6" / "common_rankdiff_r6_source_copy.py")
r6 = importlib.util.module_from_spec(spec); sys.modules["r6src"] = r6; spec.loader.exec_module(r6)
CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
d = np.load(CACHE / "reddit_submissions_long.npz", allow_pickle=True)
X, R, fl, K = d["X"][:106].astype(float), d["R"][:106].astype(float), d["floors"][:106].astype(float), int(d["K"])
ok = True
for asym in (False, True):
    a = r6.estimate_common(X, R, K=K, floors=fl, refine_drift=True, asymmetry=asym)
    b = cand.estimate_common(X, R, K=K, floors=fl, refine_drift=True, asymmetry=asym, drift_core=False, drift_rank_law=False)
    for f in ("sigma_L0", "beta", "phi", "a_xi", "b_xi", "a_obs", "b_obs", "s_vol", "jump_c", "surge_lam", "surge_mu", "decline_var"):
        if getattr(a, f) != getattr(b, f):
            ok = False; print("PARAM MISMATCH", asym, f, getattr(a, f), getattr(b, f))
    if not np.array_equal(a.drift_g, b.drift_g):
        ok = False; print("drift_g mismatch", asym)
    fa = r6.filter_endpoints(X, R, a, floors_tr=fl); fb = cand.filter_endpoints(X, R, b, floors_tr=fl)
    for k in ("L_hat", "xi_hat", "P11", "v_hat", "post_s_v"):
        if not np.array_equal(fa[k], fb[k]):
            ok = False; print("FILTER MISMATCH", asym, k, float(np.nanmax(np.abs(fa[k] - fb[k]))))
    xa, ra, _ = r6.simulate_common_panel(a, a.ladder_obs, T_sim=6, seed=3)
    xb, rb, _ = cand.simulate_common_panel(b, b.ladder_obs, T_sim=6, seed=3)
    if not (np.array_equal(xa, xb, equal_nan=True) and np.array_equal(ra, rb, equal_nan=True)):
        ok = False; print("SIM MISMATCH", asym)
    print(f"asymmetry={asym}: n_sc R6={a.n_free_scalars()} cand={b.n_free_scalars()} decline_var={a.decline_var:.3e}")
print("BIT-IDENTICAL TO R6" if ok else "NOT IDENTICAL")
