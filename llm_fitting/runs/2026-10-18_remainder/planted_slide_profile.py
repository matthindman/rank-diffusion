"""Does submissions' own slide profile, planted as drift by current rank, pull the boundary statistic down at
ranks K/16 ... K/8 as far as the data's 0.61-0.72?  (Post hoc, after the remainder split of 2z-da; planted
worlds only; the profile is read from the saved 2z-cd cells.)

World (the test-suite world of tests/test_mts_boundary_law.py): N 25,600, K 1,600, S 1.5, lasting variance
0.02 a week, 26-week blocks, 8 blocks.  Drift by current relative rank r/K: minus the members' slide of the
submissions panel at that relative rank, less its value at K, in units of the crossing gain (the data's
c = 4.0 per 1,000 weeks), times the planted crossing gain.  Below K the extra drift is zero.  Comments'
profile is the contrast.  Three seeds.

    python llm_fitting/runs/2026-10-18_remainder/planted_slide_profile.py
"""
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(ROOT / "tests"))
import mts_boundary_law as bd  # noqa: E402
from test_mts_balance_law import long_frame  # noqa: E402

N, K, S, SIG2, BLOCK, NB = 25600, 1600, 1.5, 0.02, 26, 8
C = SIG2 / (2 * S)
C_DATA = 0.0040
R = ROOT / "llm_fitting" / "runs"


def profile(pn):
    d = json.loads((R / "2026-10-14_balance_law" / "cells" / f"law__{pn}.json").read_text())
    Kp = d["K"]
    rows = [sorted(q["rows"], key=lambda r: r["k"]) for q in d["pairs"] if q["primary"]]
    rel = np.array([r["k"] / Kp for r in rows[0]])
    sbar = np.mean([[r["sbar"] for r in rr] for rr in rows], axis=0)
    return rel, (sbar - sbar[-1]) / C_DATA          # extra slide in units of the crossing gain, 0 at K


def world(rel, extra, seed):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    out = np.empty((BLOCK * NB, N))
    lr = np.log(rel)
    for t in range(BLOCK * NB):
        order = np.argsort(-x, kind="stable")
        rank = np.empty(N)
        rank[order] = np.arange(1, N + 1)
        q = np.clip(np.log(rank / K), lr[0], 0.0)
        drift = -C * np.interp(q, lr, extra) * (rank <= K)
        x = x + drift + rng.normal(0, np.sqrt(SIG2), N)
        out[t] = x
    return out


out = {}
for pn in ("reddit_submissions_long", "reddit_comments_ext"):
    rel, extra = profile(pn)
    print(pn, "extra slide in units of c at k/K", dict(zip(np.round(rel, 4).tolist(), np.round(extra, 2).tolist())), flush=True)
    ratios = {}
    for seed in (11, 12, 13):
        res = bd.analyse(long_frame(world(rel, extra, seed)), K, BLOCK, boot=10)
        for r in res["pooled"]:
            ratios.setdefault(r["k"], []).append(r["R_lasting"] / (SIG2 / (2 * r["S"])))
    out[pn] = {int(k): dict(mean=float(np.mean(v)), seeds=[float(x) for x in v]) for k, v in ratios.items()}
    print("   ratio by k/K: " + "; ".join(f"{k / K:.4f}: {np.mean(v):.2f} ({min(v):.2f}-{max(v):.2f})" for k, v in sorted(ratios.items())), flush=True)
Path(__file__).with_name("planted_slide_profile.json").write_text(json.dumps(out, indent=1))
