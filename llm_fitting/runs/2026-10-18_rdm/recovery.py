"""R1 of DOSSIER.md (ADEMP).  Aim: does the estimator recover the moving-home RDM's lasting parameters?
Data-generating mechanism: the frozen moving-home RDM with parameters taken from the Facebook-pages fit
(truth), N 14,000 endpoints, panels of T = 86 and T = 160 weeks, started from the fitted ladder.  Estimands:
sigma0 and beta (size law of the lasting variance) and the band lasting variances s_b.  Method: the full
pipeline (model A's estimator on the synthetic panel, then estimate_rdm).  Performance: bias and spread over 6
replications per length; R1 rule: beta within +-0.03, sigma0 within +-15% (median over replications).
Also the null: a panel with beta = 0 must give |beta_hat| < 0.03 (median).

    python llm_fitting/runs/2026-10-18_rdm/recovery.py
"""
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
import rankdiff_rdm as rdm  # noqa: E402

K = 3500
EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True)
df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS["facebook_a"]), K, buffer_mult=4)
base = mrd.estimate(df, **EST)
truth = rdm.estimate_rdm(df, K, base=base, jumps=False)
L0 = rk._filtered_levels(df, base).to_numpy()
out = dict(truth=dict(sigma0=truth.sigma0, beta=truth.beta, s_band=truth.s_band.tolist()), runs={})
for label, beta in (("fitted", truth.beta), ("null_beta0", 0.0)):
    tr = replace(truth, beta=beta)
    tr = replace(tr, drift_z=rdm.drift_hold_numeric(tr, L0)[0], drift_g=rdm.drift_hold_numeric(tr, L0)[1])
    for T in (86, 160):
        rows = []
        for rep in range(6):
            Xs, ids = rdm.simulate_panel(tr, L0, T, seed=1000 + rep, lasting="movinghome")
            syn = rdm.panel_long_frame(Xs, ids)
            syn = syn[syn.groupby("entity_id")["period"].transform("size") >= 10]
            b_hat = mrd.estimate(syn, **EST)
            p_hat = rdm.estimate_rdm(syn, K, base=b_hat, jumps=False, variant="move", slides=(np.array([1.0, 2.0]), np.zeros(2)))
            rows.append(dict(sigma0=p_hat.sigma0, beta=p_hat.beta, s_band=p_hat.s_band.tolist()))
            print(f"{label} T={T} rep {rep}: sigma0 {p_hat.sigma0:.4f} (truth {tr.sigma0:.4f}), beta {p_hat.beta:+.3f} (truth {beta:+.3f})", flush=True)
        s0 = np.array([r["sigma0"] for r in rows])
        bt = np.array([r["beta"] for r in rows])
        out["runs"][f"{label}_T{T}"] = dict(rows=rows, sigma0_median_rel_err=float(np.median(s0 / tr.sigma0 - 1)),
                                            beta_median_err=float(np.median(bt - beta)), beta_sd=float(np.std(bt)))
        print(f"  {label} T={T}: sigma0 median rel err {np.median(s0 / tr.sigma0 - 1):+.3f}; beta median err {np.median(bt - beta):+.3f} (sd {np.std(bt):.3f})", flush=True)
Path(__file__).with_name("recovery.json").write_text(json.dumps(out, indent=1))
