"""R7 of PLAN.md (item 4): Gomez's exact decomposition with a genuinely prior cohort.

The cohort for the pair (a, a+1) is the top k by total activity in block a-1, so that selection does not use the
year whose activity enters the change.  Same blocks, universe and arithmetic as gomez_r3.py.

    python llm_fitting/runs/2026-10-18_pnas_push/gomez_r7_prior.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

out = {}
for pn in ("reddit_submissions_long", "reddit_comments_ext"):
    cfg = m1.PANELS[pn]
    K, block = cfg["K"], cfg["block"]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    X = m1.matrices(df, K)[0]
    del df
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    Wk = np.nan_to_num(np.expm1(X), nan=0.0)
    tots = [Wk[start + b * block:start + (b + 1) * block].sum(0) for b in range(nb)]
    rows = []
    for k in (K // 64, K // 16, K // 4):
        for a in range(1, nb - 1):
            ta, tb = tots[a], tots[a + 1]
            oa, ob = np.argsort(-ta, kind="stable"), np.argsort(-tb, kind="stable")
            Da, Db = ta[oa[:K]].sum(), tb[ob[:K]].sum()
            Wb = tb[ob[:k]].sum()
            g = Db / Da
            row = dict(k=int(k), pair=[a, a + 1], share_change=float((Wb / Db) / (ta[oa[:k]].sum() / Da) - 1))
            for name, P in (("current", oa[:k]), ("prior", np.argsort(-tots[a - 1], kind="stable")[:k])):
                Wa = ta[P].sum()
                row[f"{name}_within"] = float((tb[P].sum() / Wa - g) / g)
                row[f"{name}_between"] = float(((Wb - tb[P].sum()) / Wa) / g)
                row[f"{name}_share_a"] = float(Wa / Da)
            rows.append(row)
            print(f"{pn} k {k:>5} {a}->{a + 1}: share change {row['share_change']:+.3f}; current within {row['current_within']:+.3f} "
                  f"between {row['current_between']:+.3f}; prior-year cohort within {row['prior_within']:+.3f} between {row['prior_between']:+.3f}", flush=True)
    out[pn] = rows
Path(__file__).with_name("gomez_r7_prior.json").write_text(json.dumps(out, indent=1))
