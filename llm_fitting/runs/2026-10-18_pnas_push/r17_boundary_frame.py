"""Post hoc (R17-B, after `table_data` gave a pooled crossing ratio of 1.04 on submissions against the registered 0.80):
the registered boundary estimator (mts_boundary_law.analyse, defaults) on the universe frame that the simulated worlds
produce (restrict_universe, buffer 4K; the lean cache), cut-off by cut-off, beside the registered cells, which were
computed on the full panel with every community ever in a week's top 4K tracked (2026-10-14_boundary_law/cells).
Pooled = geometric mean over K/16 ... K, as pooled_ratio.py.

    R12_CACHE=<dir> python r17_boundary_frame.py <panel>
"""
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import mts_boundary_law as mb  # noqa: E402
import r12_hetgrowth as h  # noqa: E402


def main():
    pn = sys.argv[1]
    df, K = h.load(pn)
    res = mb.analyse(df, K)
    reg = json.loads((HERE.parent / "2026-10-14_boundary_law" / "cells" / f"boundary__{pn}.json").read_text())
    pool = lambda rows: float(np.exp(np.mean(np.log([r["ratio"] for r in rows if r["k"] >= K // 16 - 1]))))
    out = dict(panel=pn, K=K, universe_frame={int(r["k"]): float(r["ratio"]) for r in res["pooled"]},
               full_panel={int(r["k"]): float(r["ratio"]) for r in reg["pooled"]},
               pooled_universe_frame=pool(res["pooled"]), pooled_full_panel=pool(reg["pooled"]),
               certified_top=dict(universe_frame=res["certified_top"], full_panel=reg["certified_top"]))
    (HERE / f"r17_boundary_frame__{pn}.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1), flush=True)


if __name__ == "__main__":
    main()
