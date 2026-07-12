#!/usr/bin/env python3
"""E5 runner — stationary head-law diagnostic (protocol A2/A3/A6.5/A7).

Frozen invocation (the E5 evaluation; run AFTER the intake gate PASSes):
  python llm_fitting/e5_headlaw.py reddit_comments_ext --top-k 12500

Computes, at exactly 20 seeds (0..19), with the registered E3 LONG stack:
  - S(1), S(10): time-mean top-share WITHIN THE RECORDED TOP-2,000 (A3),
    empirical and simulated
  - head offset over ranks 1-600: RAW (A2 registration fidelity) and
    per-week LEVEL-ADJUSTED (A6.5 declared clarification -- raw is
    level-contaminated on a growing census)
  - the REGISTERED TRIGGER, algebraically (A6.5):
        mean_seed(S1_sim) - S1_emp  >  2 * SD_seed(S1_sim)
    (same direction as the recorded overshoot). S(10) and the offsets are
    diagnostic readouts only; S(1) alone triggers the registered reading.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import community_metrics as cm  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

SEEDS = range(20)     # frozen (A6.5): exactly 20 seeds, 0..19


def e5_trigger(s1_emp: float, s1_sims) -> dict:
    """Pure registered-trigger algebra (A6.5)."""
    s1_sims = np.asarray(list(s1_sims), dtype=float)
    mu, sd = float(np.mean(s1_sims)), float(np.std(s1_sims, ddof=1))
    fired = bool(mu - s1_emp > 2.0 * sd)
    return dict(mean_sim=mu, sd_sim=sd, emp=float(s1_emp), excess=mu - s1_emp,
                threshold=2.0 * sd, fired=fired)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("platform")
    ap.add_argument("--top-k", type=int, required=True)
    a = ap.parse_args()
    df = mrd.load_panel(mrd.PLATFORMS[a.platform])
    df = mrd.restrict_universe(df, a.top_k, buffer_mult=4)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    top_k = max(10, int(round(0.01 * score_k)))
    ev, er, et, ers = mrd.empirical_structures(df, top_k, topid_k=score_k)
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
                     md_vr_long=True, stat_factor=True, two_scale=True,
                     mix_hetero=True)                # registered E3 LONG stack
    s1s, s10s, offr, offa = [], [], [], []
    for s in SEEDS:
        rs = mrd._sim_struct(mrd.simulate(p, T, seed=s, top_record=score_k))[3]
        s1s.append(cm.top_share(rs, 1))
        s10s.append(cm.top_share(rs, 10))
        offr.append(cm.head_offset(ers, rs, level_adjust=False))
        offa.append(cm.head_offset(ers, rs, level_adjust=True))
        print(f"  seed {s + 1}/20 done")
    M = min(ers.shape[1], 2000)
    e1v, e10 = cm.top_share(ers, 1), cm.top_share(ers, 10)
    print(f"\nE5 readouts (shares within recorded top-{M}, A3; 20 seeds frozen):")
    print(f"  S(1)  emp {e1v:.4f}   sim {np.mean(s1s):.4f} ± {np.std(s1s, ddof=1):.4f}")
    print(f"  S(10) emp {e10:.4f}   sim {np.mean(s10s):.4f} ± {np.std(s10s, ddof=1):.4f}   [diagnostic]")
    print(f"  head offset 1-600 RAW           {np.mean(offr):+.4f} ± {np.std(offr):.4f}   "
          f"[diagnostic; level-contaminated on growth panels]")
    print(f"  head offset 1-600 LEVEL-ADJ     {np.mean(offa):+.4f} ± {np.std(offa):.4f}   [diagnostic]")
    t = e5_trigger(e1v, s1s)
    print(f"\nE5 TRIGGER (registered, S(1) only): excess {t['excess']:+.4f} vs "
          f"2*SD {t['threshold']:.4f}  ->  "
          f"{'FIRED (cross-platform structural; the pre-registered Eulerian-constraint step activates)' if t['fired'] else 'NOT fired (head-law excess recorded as FB-measurement-regime limitation; no model change)'}")


if __name__ == "__main__":
    main()
