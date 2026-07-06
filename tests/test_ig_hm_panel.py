"""IG censoring-rescue panel builder: the absence-penalized permanent rank
must exclude ghost spikers (§2z-c L3 — the low-q flicker heads that made the
naive IG panels pathological).  Same property the universe rule locks in
tests/test_universe_restriction.py, applied at the pre-cut stage."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))

import ig_build_hm_panels as bp  # noqa: E402


def _panel():
    """10 weeks; 3 steady accounts (values 100/50/20 every week) and one
    ghost spiker present a single week with a huge value."""
    rows = []
    dates = pd.date_range("2023-01-02", periods=10, freq="7D")
    for t, dt in enumerate(dates):
        for name, val in [("steady_hi", 100.0), ("steady_mid", 50.0),
                          ("steady_lo", 20.0)]:
            rows.append(dict(date=dt, user_name=name, metric_value=val,
                             n_posts=5, t=t))
    rows.append(dict(date=dates[3], user_name="ghost_spiker",
                     metric_value=10_000.0, n_posts=1, t=3))
    return pd.DataFrame(rows)


def test_ghost_spiker_excluded(tmp_path):
    df = _panel()
    out = tmp_path / "panel.parquet"
    bp.build(df, df["metric_value"].to_numpy(float), str(out), keep=3)
    kept = set(pd.read_parquet(out)["user_name"].unique())
    assert kept == {"steady_hi", "steady_mid", "steady_lo"}, (
        "absence-penalized permanent rank must sink the 1-week ghost spiker "
        f"below the steady accounts; kept={kept}"
    )


def test_present_week_values_preserved(tmp_path):
    df = _panel()
    out = tmp_path / "panel.parquet"
    bp.build(df, df["metric_value"].to_numpy(float), str(out), keep=3)
    got = pd.read_parquet(out)
    hi = got[got["user_name"] == "steady_hi"]
    assert len(hi) == 10
    assert np.allclose(hi["metric_value"], 100.0)
