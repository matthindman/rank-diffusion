"""A5/E2 execution path (2026-07-11, MODEL_STATUS 2z-i): explicit gate
origins/test_len.  Locks: (a) the default auto-derivation is byte-identical
to the committed behavior for the recorded panel lengths; (b) explicit
values pass through verbatim with validation; (c) the registered E2 design
(origins=[136], test_len=34) trains on periods < 136 and scores EXACTLY
periods 136..169; (d) the CLI defaults wire nnls = not legacy_clip
(tripwire for the round-4 reviewer's wiring concern)."""
import re
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import rankdiff_kalman as rk  # noqa: E402


class TestGateWindows(unittest.TestCase):
    def test_defaults_reproduce_committed_derivation(self):
        # the recorded panels: IG T=52, FB Era A T=86, comments T=136
        self.assertEqual(rk._gate_windows(52, 5),
                         ([13, 20, 26, 32, 39], 13))
        self.assertEqual(rk._gate_windows(86, 5),
                         ([21, 32, 43, 54, 65], 21))
        self.assertEqual(rk._gate_windows(136, 5),
                         ([34, 51, 68, 85, 102], 34))

    def test_explicit_e2_design_passthrough(self):
        # registered E2: extended panel T=170, one block at 136, 34 weeks
        self.assertEqual(rk._gate_windows(170, 5, test_len=34, origins=[136]),
                         ([136], 34))

    def test_explicit_validation(self):
        with self.assertRaises(SystemExit):
            rk._gate_windows(170, 5, test_len=35, origins=[136])   # 136+35 > 170
        with self.assertRaises(SystemExit):
            rk._gate_windows(170, 5, test_len=34, origins=[1])     # T0 < 2


class TestSplitPanel(unittest.TestCase):
    def test_e2_block_trains_below_136_scores_136_to_169(self):
        df = pd.DataFrame({"period": np.arange(170).repeat(3),
                           "entity_id": ["a", "b", "c"] * 170,
                           "X": np.zeros(170 * 3)})
        tr, te = rk._split_panel(df, T0=136, test_len=34)
        self.assertEqual(set(tr["period"]), set(range(136)))
        # test window re-indexed to 0..33, and its SOURCE periods are 136..169
        self.assertEqual(set(te["period"]), set(range(34)))
        self.assertEqual(len(te), 34 * 3)
        src = df[(df["period"] >= 136) & (df["period"] < 170)]
        self.assertEqual(len(te), len(src))
        # nothing beyond 169 leaks in even if the panel were longer
        df2 = pd.DataFrame({"period": np.arange(200).repeat(2),
                            "entity_id": ["a", "b"] * 200,
                            "X": np.zeros(400)})
        _, te2 = rk._split_panel(df2, T0=136, test_len=34)
        self.assertEqual(te2["period"].max(), 33)
        self.assertEqual(len(te2), 34 * 2)


class TestCLIWiring(unittest.TestCase):
    def test_cli_default_is_nnls_not_legacy(self):
        # wiring tripwire: the CLI must derive nnls from --legacy-clip
        # (nnls=not args.legacy_clip) in BOTH entry points
        for mod in ("minimal_rankdiff.py", "rankdiff_kalman.py"):
            src = (Path(__file__).resolve().parents[1] / "llm_fitting" / mod
                   ).read_text()
            self.assertTrue(re.search(r"nnls\s*=\s*not\s+args\.legacy_clip", src),
                            f"{mod}: CLI no longer wires nnls = not legacy_clip")


if __name__ == "__main__":
    unittest.main()
