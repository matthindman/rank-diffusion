"""Round 8 parameter check (no evaluation scores): candidate S2'/S3' drift parameters on the four full
DEVELOPMENT panels and on the Reddit gate/Gold training windows, with the tiered moments behind them."""
import json, math, sys, time
from pathlib import Path
import numpy as np
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr
from diag_asym_model import CACHE, PANELS
out = {}
for pn, has_sb in PANELS.items():
    d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
    X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
    sb = None
    if has_sb:
        js = json.loads((CACHE / f"{pn}_spec_b.json").read_text()); sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
    T = X.shape[0]
    windows = [T] + ([80, 106, 133, 160] if T > 200 else [])
    for T0 in windows:
        for asym in (False, True):
            t0 = time.time()
            p = cr.estimate_common(X[:T0], R[:T0], K=K, floors=fl[:T0], spec_b=sb, refine_drift=False, asymmetry=asym)
            dr = (p.diag.get("stage3") or {}).get("drift", {}) if p.diag.get("stage3") else {}
            key = f"{pn}@{T0}:{'S3' if asym else 'S2'}"
            tiers = dr.get("tiers", [])
            out[key] = dict(n_sc=p.n_free_scalars(), sigma_d_K=math.sqrt(p.decline_var), b_d=p.decline_b, jump_c=p.jump_c,
                            surge=[p.surge_lam, p.surge_mu, p.surge_rho], sL0=p.sigma_L0, beta=p.beta, phi=p.phi,
                            pooled_z=(dr.get("net_cov", 0) / dr["se_cov"]) if dr.get("se_cov") and np.isfinite(dr.get("se_cov", np.nan)) and dr["se_cov"] > 0 else None,
                            tiers=[(t["tier"], t["n"], round(math.sqrt(t["sd2"]), 5), round(t["net"] / t["se"], 1) if t["se"] and np.isfinite(t["se"]) else None) for t in tiers],
                            secs=round(time.time() - t0, 1))
            print(key, json.dumps(out[key]), flush=True)
(HERE / "diag_params_r8.json").write_text(json.dumps(out, indent=1, default=float))
