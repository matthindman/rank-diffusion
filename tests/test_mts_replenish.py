"""Known-answer tests for the replenishment test (mts_replenish).  Synthetic panels only."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import balance_accounting as ba  # noqa: E402
import mts_replenish as mr  # noqa: E402

N, K, S, SIG2, BLOCK, NB = 6400, 400, 1.5, 0.02, 26, 4
C = SIG2 / (2 * S)


def long_frame(X):
    R = np.zeros(X.shape, dtype=np.int32)
    for t in range(X.shape[0]):
        o = ba._order(X[t])
        R[t, o] = np.arange(1, o.size + 1)
    per, ent = np.nonzero(np.isfinite(X))
    df = pd.DataFrame(dict(entity_id=[f"e{j:05d}" for j in ent], period=per, X=X[per, ent], rank=R[per, ent]))
    df["N"] = df.groupby("period")["entity_id"].transform("size")
    return df


def walks(seed, drift=None):
    rng = np.random.default_rng(seed)
    step = rng.normal(0, np.sqrt(SIG2), (BLOCK * NB, N))
    if drift is not None:
        step = step + drift
    return S * np.log(N / np.arange(1, N + 1)) + np.cumsum(step, axis=0)


def test_closed_walks_have_no_entry_term_and_the_slide_is_the_diffusion_rate():
    res = mr.analyse(long_frame(walks(0)), K, BLOCK, boot=100)
    p = mr._pair(res, 2, 3)
    assert p["I"] == pytest.approx(0, abs=1e-12) and p["identity_residual"] == pytest.approx(0, abs=1e-12)
    assert p["slide_P"] / C == pytest.approx(1.0, abs=0.3)
    assert res["variance_route"]["c_var"] / C == pytest.approx(1.0, abs=0.35)
    sc = mr.score(res, "reddit_comments_ext", dev=0.3 * C)
    assert sc["R5a"]["status"] == "PASS" and sc["R5c"]["status"] == "PASS" and not sc["departures"]


def test_births_show_up_as_the_entry_term_and_the_identity_is_exact():
    X = walks(1)
    rng = np.random.default_rng(11)
    T = X.shape[0]
    newcomers = []
    for t in range(BLOCK, T - BLOCK, 2):                       # two a fortnight, landing inside the top K
        kth = np.sort(X[t])[::-1][K - 1]
        path = np.full(T, np.nan)
        path[t:] = kth + rng.uniform(0.2, 1.5) + np.cumsum(rng.normal(0, np.sqrt(SIG2), T - t))
        newcomers.append(path)
    X = np.column_stack([X] + newcomers)
    res = mr.analyse(long_frame(X), K, BLOCK, boot=100)
    for a, b in ((1, 2), (2, 3)):
        p = mr._pair(res, a, b)
        assert p["identity_residual"] == pytest.approx(0, abs=1e-12)
        assert p["E"] > 0.05 * C and p["X"] == pytest.approx(0, abs=1e-12)
        assert p["slide"] > p["slide_P"]
        assert p["n_new_topK"] == p["n_entrants_topK"] > 0
        assert p["E_new"] / p["E"] == pytest.approx(1.0, abs=1e-9)
    assert mr._pair(res, 2, 3)["slide_P"] / C == pytest.approx(1.0, abs=0.35)


def test_drift_by_rank_among_named_endpoints_is_detected_by_the_bin_comparison():
    drift = np.zeros(N)
    drift[K:2 * K] = 2 * C                                     # the endpoints first ranked K to 2K gain on the rest
    res = mr.analyse(long_frame(walks(2, drift=drift)), K, BLOCK, boot=100, dev=0.3 * C)
    p = mr._pair(res, 2, 3)
    low = [r for r in p["bins"] if r["bin"][0] >= K - 1e-9][0]
    assert low["dev"] > 0.5 * C and low["dev"] > 3 * low["dev_sd"]
    assert p["tilt"] > 0.5 * C and p["tilt_q"][0] > 0
    sc = mr.score(res, "reddit_comments_ext", dev=0.3 * C)
    assert sc["R5c"]["status"] == "FAIL" and sc["R5a"]["status"] == "FAIL"
