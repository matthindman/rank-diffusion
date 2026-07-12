"""restrict_universe(member_ids=...) — fixed exact membership (review round 3):
the ONLY leak-safe universe path on pre-cut source panels.  Locks: fixed ids
are used verbatim (no re-selection), ranks are recomputed within the fixed
set, attrs are set, and the default selection path is untouched."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402


def _panel(n=30, T=8, seed=0):
    rng = np.random.default_rng(seed)
    base = np.sort(rng.normal(8, 2, n))[::-1]
    rows = []
    for t in range(T):
        x = base + rng.normal(0, 0.2, n)
        for i in range(n):
            rows.append(dict(entity_id=f"e{i:02d}", period=t,
                             metric=float(np.expm1(x[i])), X=x[i]))
    df = pd.DataFrame(rows)
    return mrd._rank_within(df)


class TestMemberIds(unittest.TestCase):
    def test_fixed_ids_used_verbatim(self):
        df = _panel()
        ids = {"e00", "e05", "e07", "e11", "e29"}
        out = mrd.restrict_universe(df, top_k=3, member_ids=ids)
        self.assertEqual(set(out["entity_id"].unique()), ids)
        self.assertEqual(out.attrs["score_k"], 3)
        self.assertEqual(out.attrs["universe_B"], 5)
        # ranks recomputed WITHIN the fixed set: exactly 1..5 each week
        for _, g in out.groupby("period"):
            self.assertEqual(sorted(g["rank"]), [1, 2, 3, 4, 5])

    def test_ids_absent_from_panel_are_dropped(self):
        df = _panel()
        out = mrd.restrict_universe(df, top_k=2,
                                    member_ids={"e01", "ghost-entity"})
        self.assertEqual(set(out["entity_id"].unique()), {"e01"})
        self.assertEqual(out.attrs["universe_B"], 1)

    def test_exclusive_with_member_window(self):
        df = _panel()
        with self.assertRaises(AssertionError):
            mrd.restrict_universe(df, top_k=2, member_window=4,
                                  member_ids={"e01"})

    def test_default_path_unchanged(self):
        df = _panel()
        a = mrd.restrict_universe(df, top_k=3, buffer_mult=2)
        b = mrd.restrict_universe(df, top_k=3, buffer_mult=2)
        pd.testing.assert_frame_equal(a, b)
        self.assertEqual(a.attrs["universe_B"], 6)


if __name__ == "__main__":
    unittest.main()
