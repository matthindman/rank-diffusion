#!/usr/bin/env python3
"""EXPLORATORY (post-confirmation-report; protocol §5 §2z-q line governs):
decompose the §2z-q E3 boundary-flux residual (sim outfluxK 0.149 vs emp
0.086; return4K 0.294 vs 0.398) on the extended comments panel.

Mirrors the card's _boundary_flux definitions exactly (weekly out-flux =
share of top-K at t absent from top-K at t+1; return = share of droppers
back at t+h), then extends them:
  - return curve at h = 1, 2, 4, 8, 13
  - permanent-exit share: droppers NOT back within 13 weeks
  - out-flux by the dropper's rank band at t: core (<=0.5K), mid
    (0.5K..0.9K], shell (0.9K..K] — localizes whether the excess is a
    boundary-shell phenomenon or a universe-wide hazard
Sim side: E3 LONG stack (temper pool8 md6 t mix + md-vr-long + stat-factor
+ two-scale), 3 seeds (declared MC caveat: head/boundary rows carry seed
noise; this is a localization readout, not a card).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

K, BUF = 12_500, 4
RET_HS = [1, 2, 4, 8, 13]


def flux_decomp(top_ids: np.ndarray, ranks_of=None, K: int = K) -> dict:
    T = top_ids.shape[0]
    sets = [set(top_ids[t, :K]) - {-1} for t in range(T)]
    pos = [{v: r for r, v in enumerate(top_ids[t, :K]) if v != -1}
           for t in range(T)]
    out = dict(outflux=[], ret={h: [] for h in RET_HS}, perm=[],
               out_core=[], out_mid=[], out_shell=[])
    for t in range(T - 1):
        cur = sets[t]
        if not cur:
            continue
        dropped = cur - sets[t + 1]
        out["outflux"].append(len(dropped) / len(cur))
        for h in RET_HS:
            if t + 1 + h < T and dropped:
                out["ret"][h].append(
                    len(dropped & sets[t + 1 + h]) / len(dropped))
        if t + 1 + 13 < T and dropped:
            back_any = set()
            for h in range(1, 14):
                back_any |= dropped & sets[t + 1 + h]
            out["perm"].append(len(dropped - back_any) / len(dropped))
        if dropped:
            rr = np.array([pos[t][v] for v in dropped])
            n = len(cur)
            out["out_core"].append((rr <= 0.5 * K).sum() / n)
            out["out_mid"].append(((rr > 0.5 * K) & (rr <= 0.9 * K)).sum() / n)
            out["out_shell"].append((rr > 0.9 * K).sum() / n)
    return {k: (float(np.mean(v)) if not isinstance(v, dict)
                else {h: float(np.mean(x)) if x else np.nan
                      for h, x in v.items()})
            for k, v in out.items()}


def show(tag, d):
    r = d["ret"]
    print(f"  {tag:<14} outflux {d['outflux']:.4f}  "
          f"ret h=1/2/4/8/13: {r[1]:.3f}/{r[2]:.3f}/{r[4]:.3f}/"
          f"{r[8]:.3f}/{r[13]:.3f}  perm-exit {d['perm']:.3f}  "
          f"by-band core/mid/shell {d['out_core']:.4f}/"
          f"{d['out_mid']:.4f}/{d['out_shell']:.4f}", flush=True)


def main():
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    df = mrd.restrict_universe(df, K, buffer_mult=BUF)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    print(f"extended panel T={T}, score_k={score_k}")
    _, _, et, _ = mrd.empirical_structures(df, 10, topid_k=score_k)
    print("empirical:")
    emp = flux_decomp(et, K=score_k)
    show("emp", emp)
    print("estimating (E3 LONG stack, NNLS default)...", flush=True)
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
                     md_vr_long=True, stat_factor=True, two_scale=True,
                     mix_hetero=True)
    print("simulated (3 seeds; MC caveat declared):")
    sims = []
    for s in range(3):
        sim = mrd.simulate(p, T, seed=s, top_record=score_k)
        d = flux_decomp(sim["top_ids"], K=score_k)
        show(f"sim seed {s}", d)
        sims.append(d)
    print("\nsim mean vs emp:")
    for k in ("outflux", "perm", "out_core", "out_mid", "out_shell"):
        sm = float(np.mean([d[k] for d in sims]))
        print(f"  {k:<10} emp {emp[k]:.4f}   sim {sm:.4f}   ratio {sm / emp[k] if emp[k] else np.nan:.2f}")
    for h in RET_HS:
        sm = float(np.mean([d["ret"][h] for d in sims]))
        print(f"  ret h={h:<3}   emp {emp['ret'][h]:.4f}   sim {sm:.4f}")


if __name__ == "__main__":
    main()
