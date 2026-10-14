"""Known-answer tests: the boundary test of 2z-cm under the Instagram "a"-query censoring of 2z-c
(ig_censoring_sim).  Six seeds on 2026-10-15 gave a pooled ratio over K/8 .. K of 1.014 on the truth and
1.015 on the censored data (difference 0.001, per-seed spread 0.17); these tests use two seeds."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import ig_censoring_sim as ig  # noqa: E402
import mts_boundary_law as bd  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

K, BLOCK, KS = 1600, 7, (200, 400, 800, 1600)
IG = dict(pres=0.5, rank_rule="present_weeks")


def ratios(X, **kw):
    r = bd.analyse(long_frame(X), K, BLOCK, boot=5, **kw)
    if "not_adjudicated" in r:
        return None
    p = {x["k"]: x for x in r["pooled"]}
    return np.array([p[k]["ratio"] for k in KS])


def test_the_options_change_nothing_when_nothing_is_absent():
    X, P = ig.true_world(seed=2)
    assert np.allclose(ratios(X), ratios(X, **IG))


def test_the_default_rules_cannot_read_the_censored_panel_and_the_options_recover_the_truth():
    diffs = []
    for seed in (2, 3):
        X, P = ig.true_world(seed=seed)
        Xc = ig.censor(X, P, seed=100 + seed)
        assert ratios(Xc) is None                      # dropout and absence break the 90% presence rule
        t, c = ratios(X), ratios(Xc, **IG)
        assert 0.75 < c.mean() < 1.33, c
        diffs.append(c.mean() - t.mean())
    assert abs(np.mean(diffs)) < 0.2, diffs
