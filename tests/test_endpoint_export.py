"""The opt-in per-endpoint export of the gate (work plan A6): it changes no
gate number, and what it writes is what the gate scored."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402

STACK = dict(top_k=100, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
             conditional="state", reps=2, boot=100)


@pytest.fixture()
def platform(tmp_path):
    rng = np.random.default_rng(0)
    n, T = 250, 60
    base = np.sort(rng.lognormal(3, 1.2, n))[::-1]
    rows = []
    for d in pd.date_range("2020-01-06", periods=T, freq="7D"):
        vals = base * np.exp(rng.normal(0, 0.25, n))
        rows.extend((f"e{i:03d}", d, float(vals[i])) for i in range(n))
    path = tmp_path / "panel.parquet"
    pd.DataFrame(rows, columns=["endpoint_id", "date", "metric_value"]).to_parquet(path, index=False)
    mrd.PLATFORMS["_a6_export"] = dict(path=str(path), id_col="endpoint_id", ts_col="date",
                                       metric_col="metric_value", max_rank=None)
    yield "_a6_export"
    del mrd.PLATFORMS["_a6_export"]


def test_simulator_export_is_read_only_and_matches_the_tracked_cohort(platform):
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[platform]), 100, buffer_mult=4,
                               member_window=40)
    df_tr, _ = rk._split_panel(df, 40, 15)
    p = rk._estimate_fast(df_tr, temper=True, min_knot_n=8, md_lags=6, t_tails=True)
    cr0, ti0 = rk.sim_cohort_conditional(p, df_tr, 15, 0.0, seed=3)
    ex = {}
    cr1, ti1 = rk.sim_cohort_conditional(p, df_tr, 15, 0.0, seed=3, export=ex)
    np.testing.assert_array_equal(cr0, cr1)
    np.testing.assert_array_equal(ti0, ti1)
    R = ex["rank"].astype(float)
    assert R.shape == (15, len(ex["entity_id"]))
    first = R[0]
    cohort = np.flatnonzero((first > 0) & (first <= 200))
    cohort = cohort[np.argsort(first[cohort])]            # the simulator's cohort order
    mine = np.where(R[:, cohort] > 0, R[:, cohort], np.nan)
    np.testing.assert_array_equal(mine, cr1)
    for t in range(R.shape[0]):                           # live ranks are distinct
        live = R[t][R[t] > 0]
        assert live.size == np.unique(live).size and live.max() <= R.shape[1]


def test_gate_numbers_are_unchanged_and_files_follow_the_schema(platform, tmp_path, capsys):
    plain = rk.oos_movement(platform, **STACK)
    out = tmp_path / "export"
    with_export = rk.oos_movement(platform, endpoint_export=str(out), **STACK)
    assert plain == with_export
    files = sorted(out.glob("*.npz"))
    assert len(files) == plain["n_splits"]
    z = np.load(files[0], allow_pickle=False)
    T0, test_len, n = int(z["T0"]), int(z["test_len"]), z["entity_id"].size
    assert z["sim_rank"].shape == (STACK["reps"], test_len, n)
    assert z["obs_rank"].shape == (test_len, n)
    assert z["origin_rank"].shape == z["train_perm_rank"].shape == z["train_presence"].shape == (n,)
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[platform]), 100, buffer_mult=4,
                               member_window=T0)
    want = df[df["period"] == T0].set_index("entity_id")["rank"].reindex(z["entity_id"])
    np.testing.assert_array_equal(z["obs_rank"][0], want.fillna(0).to_numpy().astype(np.uint32))
    want = df[df["period"] == T0 - 1].set_index("entity_id")["rank"].reindex(z["entity_id"])
    np.testing.assert_array_equal(z["origin_rank"], want.fillna(0).to_numpy().astype(np.uint32))
    assert np.all(z["train_presence"] == 1.0)             # every endpoint observed every week here
    mean_rank = (df[df["period"] < T0].groupby("entity_id")["rank"].mean()
                 .reindex(z["entity_id"]).to_numpy())
    np.testing.assert_allclose(z["train_perm_rank"], mean_rank)


def test_export_refuses_an_unconditional_stack(platform, tmp_path):
    with pytest.raises(SystemExit):
        rk.oos_movement(platform, top_k=100, reps=2, boot=50,
                        endpoint_export=str(tmp_path / "x"))
