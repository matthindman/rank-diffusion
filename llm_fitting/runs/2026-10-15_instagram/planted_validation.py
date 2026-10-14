"""Planted validation of the boundary test under the Instagram censoring (synthetic only): six seeds of
truth, censored (persistent q), censored with q drifting, censored with a wide q spread.  Saved for the
figure; the same numbers are quoted in DESIGN.md and MODEL_STATUS 2z-cx.

    python llm_fitting/runs/2026-10-15_instagram/planted_validation.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(ROOT / "tests"))
import ig_censoring_sim as ig  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

K, KS = 1600, (200, 400, 800, 1600)


def ratios(X, **kw):
    r = bd.analyse(long_frame(X), K, 7, boot=5, **kw)
    if "not_adjudicated" in r:
        return None
    p = {x["k"]: x for x in r["pooled"]}
    return [p[k]["ratio"] for k in KS]


IG = dict(pres=0.5, rank_rule="present_weeks")
out = {"ks": list(KS), "truth": [], "censored": [], "censored, default rules": [], "q drift 0.002/wk": [], "q log-sd 2": []}
for seed in range(2, 8):
    X, P = ig.true_world(seed=seed)
    out["truth"].append(ratios(X))
    Xc = ig.censor(X, P, seed=100 + seed)
    out["censored"].append(ratios(Xc, **IG))
    out["censored, default rules"].append(ratios(Xc))
    out["q drift 0.002/wk"].append(ratios(ig.censor(X, P, q_drift=0.002, seed=200 + seed), **IG))
    out["q log-sd 2"].append(ratios(ig.censor(X, P, q_logsd=2.0, seed=300 + seed), **IG))
Path(__file__).with_name("planted_validation.json").write_text(json.dumps(out, indent=1))
for k, v in out.items():
    if k != "ks":
        ok = [x for x in v if x is not None]
        print(k.ljust(24), "adjudicated", len(ok), "of", len(v), "" if not ok else f"pooled mean {np.mean(ok):.3f}")
