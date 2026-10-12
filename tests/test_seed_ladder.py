"""The opt-in seeding of the structure simulation from a time-median ladder
(owner-approved 2026-10-12, MODEL_STATUS 2z-ba): off by default and
bit-identical when off; confined to the structure simulation."""
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402


@pytest.fixture(scope="module")
def fitted(tmp_path_factory):
    rng = np.random.default_rng(0)
    n, T = 300, 60
    base = np.sort(rng.lognormal(3, 1.2, n))[::-1]
    rows = []
    for d in pd.date_range("2020-01-06", periods=T, freq="7D"):
        vals = base * np.exp(rng.normal(0, 0.25, n))
        rows.extend((f"e{i:03d}", d, float(vals[i])) for i in range(n))
    path = tmp_path_factory.mktemp("seed") / "panel.parquet"
    pd.DataFrame(rows, columns=["endpoint_id", "date", "metric_value"]).to_parquet(path, index=False)
    cfg = dict(path=str(path), id_col="endpoint_id", ts_col="date",
               metric_col="metric_value", max_rank=None)
    df = mrd.restrict_universe(mrd.load_panel(cfg), 100, buffer_mult=4)
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True)
    return df, p, np.log(base)


def same(a, b):
    return all(np.array_equal(a[k], b[k], equal_nan=True) for k in a)


def test_default_is_unchanged_and_the_option_only_replaces_the_seed(fitted):
    df, p, _ = fitted
    base = mrd.simulate(p, 30, seed=1, top_record=100)
    assert same(base, mrd.simulate(p, 30, seed=1, top_record=100, seed_ladder=None))
    assert same(base, mrd.simulate(p, 30, seed=1, top_record=100, seed_ladder=p.w0))
    ladder = mrd.time_median_ladder(df, int(np.asarray(p.w0).size))
    opt = mrd.simulate(p, 30, seed=1, top_record=100, seed_ladder=ladder)
    assert not same(base, opt)
    # the option is exactly "the fitted parameters with another seed ladder"
    assert same(opt, mrd.simulate(replace(p, w0=ladder), 30, seed=1, top_record=100))


def test_time_median_ladder_known_answers(fitted):
    df, p, log_base = fitted
    ladder = mrd.time_median_ladder(df)
    assert ladder.size == 300 and np.all(np.diff(ladder) <= 0)
    first = np.sort(df.loc[df["period"] == 0, "X"].to_numpy())[::-1]
    # noise widens every observed ladder: the top sits above the true top level,
    # and the median over weeks is steadier than any one week
    weekly_tops = df.groupby("period")["X"].max().to_numpy()
    assert abs(ladder[0] - np.median(weekly_tops)) < 1e-12
    assert abs(ladder[0] - first[0]) <= np.ptp(weekly_tops)
    assert mrd.time_median_ladder(df, 50).size == 50
    # without noise the ladder is the sorted levels exactly
    quiet = df.copy()
    level = quiet.groupby("entity_id")["X"].transform("mean")
    quiet["X"] = level
    np.testing.assert_allclose(mrd.time_median_ladder(quiet),
                               np.sort(level.groupby(quiet["entity_id"]).first().to_numpy())[::-1])


def test_the_gate_simulators_do_not_take_the_option(fitted):
    """Confined to the structure simulation: the cohort simulators behind the
    gate and its calibration step read p.w0 and have no seed_ladder argument."""
    import inspect
    for fn in (rk.sim_cohort, rk.sim_cohort_conditional, rk.sim_dist, rk.sim_dist_cond,
               rk.oos_movement, rk._calibrate_scale):
        assert "seed_ladder" not in inspect.signature(fn).parameters


def test_command_line_flag_is_listed():
    import subprocess
    r = subprocess.run([sys.executable, "llm_fitting/minimal_rankdiff.py", "--help"],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0 and "--seed-ladder" in r.stdout and "time-median" in r.stdout
