"""Regression tests for the closed Lagrangian top-coverage universe
(minimal_rankdiff.restrict_universe).

Guards, in the spirit of the band-alignment pitfall, are IDENTITY-based:
membership must be exactly the right entity_id set, ranks must be a clean
per-period permutation, membership must never leak from test periods, and
below-K member observations must be retained (no censoring).
"""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def make_panel(values_by_entity: dict[str, list[float]]) -> pd.DataFrame:
    """Build a canonical panel (entity_id, period, metric, X, rank, N, z) from
    per-entity metric paths; NaN = absent that week."""
    rows = []
    for eid, vals in values_by_entity.items():
        for t, v in enumerate(vals):
            if np.isfinite(v):
                rows.append(dict(entity_id=eid, period=t, metric=float(v)))
    df = pd.DataFrame(rows)
    df["X"] = np.log1p(df["metric"].to_numpy(dtype=float))
    return mrd._rank_within(df)


class UniverseRestrictionTests(unittest.TestCase):
    def test_membership_is_top_B_by_permanent_rank_identity(self):
        # 8 entities with strictly ordered constant levels -> permanent rank
        # order is a..h; B = 2*2 = 4 must select exactly {a, b, c, d}.
        levels = {e: [100.0 - 10 * i] * 5 for i, e in enumerate("abcdefgh")}
        df = make_panel(levels)
        out = mrd.restrict_universe(df, top_k=2, buffer_mult=2)
        self.assertEqual(set(out["entity_id"].unique()), {"a", "b", "c", "d"})
        self.assertEqual(out.attrs["score_k"], 2)
        self.assertEqual(out.attrs["universe_B"], 4)

    def test_weekly_ranks_are_permutation_within_universe(self):
        rng = np.random.default_rng(0)
        levels = {f"e{i}": list(50.0 + rng.normal(0, 5, 6)) for i in range(20)}
        df = make_panel(levels)
        out = mrd.restrict_universe(df, top_k=3, buffer_mult=3)
        for _, g in out.groupby("period"):
            self.assertEqual(sorted(g["rank"].tolist()), list(range(1, len(g) + 1)))
            # ranks respect the metric ordering within the universe
            gs = g.sort_values("rank")
            self.assertTrue((np.diff(gs["metric"].to_numpy()) <= 0).all())

    def test_membership_uses_train_window_only(self):
        # 'spike' is tiny in train (periods 0-2) and huge in test (3-5).
        # With member_window=3 it must be EXCLUDED even though its full-panel
        # permanent rank would admit it.
        levels = {f"e{i}": [50.0 - i] * 6 for i in range(6)}
        levels["spike"] = [0.1, 0.1, 0.1, 1000.0, 1000.0, 1000.0]
        df = make_panel(levels)
        out_train = mrd.restrict_universe(df, top_k=2, buffer_mult=2, member_window=3)
        self.assertNotIn("spike", set(out_train["entity_id"].unique()))
        out_full = mrd.restrict_universe(df, top_k=2, buffer_mult=2)
        self.assertIn("spike", set(out_full["entity_id"].unique()))

    def test_below_k_member_observations_retained_uncensored(self):
        # 'dipper' is a solid member whose metric collapses in period 2,
        # pushing it below top_k but NOT out of the universe: the observation
        # must be retained with a within-universe rank > top_k.
        levels = {f"e{i}": [50.0 - i] * 5 for i in range(5)}
        levels["dipper"] = [49.5, 49.5, 0.5, 49.5, 49.5]
        df = make_panel(levels)
        out = mrd.restrict_universe(df, top_k=2, buffer_mult=3)
        self.assertIn("dipper", set(out["entity_id"].unique()))
        dip = out[(out["entity_id"] == "dipper") & (out["period"] == 2)]
        self.assertEqual(len(dip), 1)  # observed, not censored
        self.assertGreater(int(dip["rank"].iloc[0]), 2)  # below the score boundary
        # all 5 weeks retained
        self.assertEqual(out[out["entity_id"] == "dipper"]["period"].nunique(), 5)

    def test_load_panel_equivalence_after_refactor(self):
        # _rank_within must reproduce the original sort/cumcount idiom:
        # unique 1..N ranks, metric-descending with entity_id tiebreak.
        df = make_panel({"b": [5.0, 5.0], "a": [5.0, 5.0], "c": [7.0, 1.0]})
        r0 = df[df["period"] == 0].sort_values("rank")["entity_id"].tolist()
        self.assertEqual(r0, ["c", "a", "b"])  # tie a/b broken by id


if __name__ == "__main__":
    unittest.main()
