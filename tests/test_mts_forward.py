"""Known-answer tests for the forward simulation (mts_forward).  Planted random walks."""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mts_forward as fw  # noqa: E402
from test_mts_balance_law import BLOCK, K, long_frame, world  # noqa: E402


def test_entrants_are_exactly_the_newcomers_to_the_top_w():
    M, n_cert = fw.block_ladders(long_frame(world("stationary")), K, BLOCK)
    tr = fw.transition(M, 1, K)
    top_a, top_b = set(fw._window(M[1], 2 * K)), set(fw._window(M[2], 2 * K))
    assert tr["n_in"] == len(top_b - top_a) and tr["offsets"].size == tr["n_in"]
    assert sum(r["n"] for r in tr["rows"]) == 2 * K
    assert all(np.isfinite(v) for v in tr["smooth"]["mean"] + tr["smooth"]["logvar"] + tr["smooth"]["logit_q"])


def test_a_pressed_head_is_forecast_from_last_years_movement():
    res = fw.analyse(long_frame(world("pressed_head")), K, BLOCK, n_sim=20)
    sc = {(s["kind"], s["origin"], tuple(s["transitions"])): s for s in fw.scores(res)}
    one = sc[("one step, out of sample", 2, (1,))]
    two = sc[("two steps, out of sample", 1, (0, 0))]
    assert one["empirical"] < 0.4 * one["no_change"] and one["moments"] < 0.4 * one["no_change"]
    assert two["empirical"] < 0.6 * two["no_change"]
    c = [x for x in res["cases"] if x["kind"] == "one step, out of sample" and x["origin"] == 2][0]
    ks = res["ks"]
    assert all(c["predicted"]["empirical"]["change"][k] < 0 for k in ks[:4])     # the head is forecast to sink
