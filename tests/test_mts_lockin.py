"""Known-answer tests for the ex ante lock-in forecasts (mts_lockin).  In planted diffusion worlds with
passing swings the movement-based forecast of who stays in the top k is calibrated and beats past
turnover extrapolated at a constant rate."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_lockin as lk  # noqa: E402
from test_mts_boundary_law import world  # noqa: E402  (N 25,600, K 1,600, 8 blocks of 26 weeks)

K = 1600


def results(kind):
    M = ba.block_means(world(kind), 26, start=0, min_frac=0.9)
    return lk.analyse(M, K, (K // 64, K // 16, K // 4), horizons=range(1, 8), origins=range(2, 7))


def test_forecasts_are_calibrated_and_beat_constant_turnover_in_a_diffusion_world():
    for kind in ("stationary", "swings"):
        cs = [x for c in results(kind) for k, v in c["ks"].items() if int(k) >= K // 16 for x in v["horizons"].values()]
        err = np.array([abs(x["model"] - x["observed"]) for x in cs])
        assert np.median(err) < 0.07, (kind, np.median(err))          # planted 2026-10-15: means 0.01-0.07 at these k
        n = sum(x["n"] for x in cs)
        bm = sum(x["brier_model"] * x["n"] for x in cs) / n
        bg = sum(x["brier_geometric"] * x["n"] for x in cs) / n
        assert bm < 0.9 * bg, (kind, bm, bg)
