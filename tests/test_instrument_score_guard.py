"""The Reddit score-capture guard of instrument_eras (declared 2026-10-14).
Synthetic series only; the recorded list is checked for form, not re-derived."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import instrument_eras as ie  # noqa: E402


def series(n=200, seed=0):
    rng = np.random.default_rng(seed)
    days = pd.date_range("2020-01-06", periods=n)
    weekday = np.where(days.weekday >= 5, 1.25, 1.0)          # a strong weekend pattern
    return pd.Series(50.0 * weekday * np.exp(rng.normal(0, 0.03, n)), index=days)


def test_planted_pockets_are_flagged_and_the_weekday_pattern_is_not():
    kpi = series()
    assert len(ie.flag_score_days(kpi)) == 0
    kpi.iloc[[100, 101]] *= 0.02
    kpi.iloc[150] *= 0.6
    flagged = ie.flag_score_days(kpi)
    assert list(flagged) == [kpi.index[100], kpi.index[101], kpi.index[150]]
    r = ie.score_ratio(kpi)
    assert r.iloc[100] < 0.05 and 0.5 < r.iloc[150] < 0.7


def test_a_slow_trend_is_not_flagged():
    kpi = series() * np.exp(-0.004 * np.arange(200))          # falls by half over the series
    assert len(ie.flag_score_days(kpi)) == 0


def test_days_without_enough_neighbours_are_never_flagged():
    kpi = series(n=20)
    kpi.iloc[3] *= 0.01
    r = ie.score_ratio(kpi)
    assert np.isnan(r.iloc[3]) and len(ie.flag_score_days(kpi)) == 0


def test_recorded_list_maps_to_monday_weeks():
    weeks = ie.reddit_flagged_weeks("submissions")
    assert weeks == {pd.Timestamp("2020-04-27"), pd.Timestamp("2021-06-07")}
    assert all(pd.Timestamp(w).weekday() == 0 for w in weeks)
    assert ie.reddit_flagged_weeks("comments") == set()
