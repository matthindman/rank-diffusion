"""Slopes into shares (saved outputs only; no data file opened): the share of the top K's activity held by
the top k, observed by year (2z-cu ladders), and under the ladder that a year's movement sustains (2z-cd:
S*_k integrated over log rank from K up to K/64; above K/64 the observed shape of the very top is kept, which
is conservative).  Levels are block means of log activity, so shares are of exp(level), within the top K.

    python llm_fitting/runs/2026-10-15_concentration/concentration.py
"""
import json
from pathlib import Path

import numpy as np

R = Path(__file__).resolve().parents[1]


def full_ladder(ranks, level, K):
    r = np.arange(1, K + 1)
    return np.interp(np.log(r), np.log(np.asarray(ranks, float)), np.asarray(level, float))


def shares(lv, ks):
    w = np.exp(lv - lv.max())
    tot = w.sum()
    return {int(k): float(w[:k].sum() / tot) for k in ks}


out = {}
for pn in ("reddit_comments_ext", "reddit_submissions_long"):
    P = json.loads((R / "2026-10-15_puzzle" / f"puzzle__{pn}.json").read_text())
    K = P["K"]
    ks = (K // 64, K // 16, K // 4)
    obs = {P["first_week"][b]: shares(full_ladder(l["ranks"], l["level"], K), ks) for b, l in enumerate(P["ladders"])}
    law = json.loads((R / "2026-10-14_balance_law" / "cells" / f"law__{pn}.json").read_text())
    last = full_ladder(P["ladders"][-1]["ranks"], P["ladders"][-1]["level"], K)
    sus = {}
    for q in [q for q in law["pairs"] if q["primary"]]:
        rows = sorted(q["rows"], key=lambda r: r["k"])
        kc = np.array([r["k"] for r in rows], float)
        Ss = np.array([r["S_star"] for r in rows], float)
        r = np.arange(1, K + 1)
        S_at = np.interp(np.log(np.clip(r, kc[0], K)), np.log(kc), Ss)       # S* by rank, flat above K/64
        lv = np.zeros(K)                                                       # level relative to rank K
        lr = np.log(r)
        for i in range(K - 2, -1, -1):
            lv[i] = lv[i + 1] + 0.5 * (S_at[i] + S_at[i + 1]) * (lr[i + 1] - lr[i])
        top = int(kc[0])                                                       # keep the observed shape above K/64
        lv[:top] = lv[top - 1] + (last[:top] - last[top - 1])
        sus[f"blocks {q['start']}->{q['end']}"] = shares(lv, ks)
    out[pn] = dict(K=K, observed_by_year=obs, sustained=sus)
    print(pn, "K", K)
    for y, s in obs.items():
        print("  observed", y, {k: f"{100 * v:.1f}%" for k, v in s.items()})
    for y, s in sus.items():
        print("  sustained", y, {k: f"{100 * v:.1f}%" for k, v in s.items()})
Path(__file__).with_name("concentration.json").write_text(json.dumps(out, indent=1))
