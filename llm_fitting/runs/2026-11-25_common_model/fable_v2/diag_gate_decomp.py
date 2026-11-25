#!/usr/bin/env python3
"""D2: which component drives the Submissions gate median-displacement overshoot?

Uses the EXACT frozen gate machinery (round3_frozen_gate.sim_dist_common / rk._moments) at the five
frozen origins, S2 only, 20 reps, and perturbs the FITTED S2 in four ways (diagnostic interventions,
never candidates): (a) baseline; (b) sigma_L0 x 0.7; (c) innov_mode='gaussian'; (d) v_hat := 1.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
ROOT = BASE.parents[2]
spec = importlib.util.spec_from_file_location("common_rankdiff", str(HERE / "common_rankdiff_fable.py"))
cr = importlib.util.module_from_spec(spec)
sys.modules["common_rankdiff"] = cr
spec.loader.exec_module(cr)
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(ROOT / "llm_fitting"))
import round3_frozen_gate as rfg  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402


def main(pn: str):
    data, sb = rfg.load_cached(pn)
    X = data["X"].astype(np.float64)
    R = data["R"].astype(np.float64)
    floors = data["floors"].astype(np.float64)
    K = int(data["K"])
    T, N = X.shape
    origins, test_len = rk._gate_windows(T, 5)
    hor = [h for h in (1, 4, 13) if h < test_len]
    out = {}
    for si, T0 in enumerate(origins):
        X_tr, R_tr, fl_tr = X[:T0], R[:T0], floors[:T0]
        R_te = R[T0:T0 + test_len]
        ed, erf, ec = rfg.emp_dist_from_R(R_te, hor)
        em = rk._moments(ed, erf, ec, hor)
        p2 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=sb if rfg.PANELS[pn]["spec_b"] else None, refine_drift=True, asymmetry=False)
        f2 = cr.filter_endpoints(X_tr, R_tr, p2, floors_tr=fl_tr)
        variants = {
            "base": (p2, f2),
            "sL0x0.7": (cr.replace(p2, sigma_L0=0.7 * p2.sigma_L0), f2),
            "gauss": (cr.replace(p2, innov_mode="gaussian"), f2),
            "vhat1": (p2, {**f2, "v_hat": np.ones(N), "post_s_v": np.zeros(N)}),
        }
        row = {"T0": int(T0), "emp": {k: em[k] for k in em}}
        line = f"  T0={T0:3d} emp: " + " ".join(f"{k}={em[k]:.2f}" for k in [f"dRank{h}" for h in hor] + ["RACF1"])
        print(line, flush=True)
        for name, (pv, fv) in variants.items():
            sd, srf, sc = rfg.sim_dist_common(fv, pv, test_len, hor, reps=20, seed_offset=1000 * si)
            sm = rk._moments(sd, srf, sc, hor)
            row[name] = {k: sm[k] for k in sm}
            print(f"      {name:8s}: " + " ".join(f"{k}={sm[k]:.2f}" for k in [f"dRank{h}" for h in hor] + ["RACF1"]) +
                  f"  coll1={sm.get('coll1', np.nan):.3f} coll5={sm.get('coll5', np.nan):.3f}", flush=True)
        out[str(T0)] = row
    (HERE / f"diag_gate_decomp_{pn}.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "reddit_submissions_long")
