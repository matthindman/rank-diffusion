#!/usr/bin/env python3
"""PREREG_2026-07-16 submissions backtest runners (A1.2/A1.3/A1.7 + A1.10).
Run ONLY after check_long_panels.py prints PASS (Phase 0). One pass each.

  p3           K = smallest grid value with mean weekly metric share >= 0.90
               (A1.10; NO override; grid max < 0.90 => STOP)
  p4p5 --top-k K   P4 trend (windows [0,71)/[71,142)/[142,212), strict
               monotone + four-cell era-vs-composition guard) and P5
               amplitude collapse (b(4), b(8) in [0.95, 1.15])
  p9 --top-k K     VR functional decomposition: F in [0.5, 0.75] (A2.4)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

PLATFORM = "reddit_submissions_long"
WINDOWS = ((0, 71), (71, 142), (142, 212))
K_GRID = (2_500, 5_000, 7_500, 10_000, 12_500, 15_000, 20_000)
B_BAND = (0.95, 1.15)
LONG = dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True,
            md_vr_long=True, stat_factor=True, two_scale=True,
            mix_hetero=True)


def p3_k(df) -> int:
    """A1.10: smallest grid K with mean weekly top-K share >= 0.90."""
    shares = {}
    for k in K_GRID:
        s = (df[df["rank"] <= k].groupby("period")["metric"].sum()
             / df.groupby("period")["metric"].sum())
        shares[k] = float(s.mean())
        print(f"  K={k:>6,}: mean weekly share {shares[k]:.4f}", flush=True)
        if shares[k] >= 0.90:
            print(f"P3 K = {k:,} (smallest grid value >= 0.90; frozen, "
                  f"no override)")
            return k
    raise SystemExit("P3 STOP: grid max < 0.90 mean share -- owner "
                     "decision required (registered stop condition)")


def s_of(df, lo, hi, member_ids=None, member_span=None, top_k=None):
    u = mrd.restrict_universe(df, top_k, buffer_mult=4,
                              member_ids=member_ids, member_span=member_span)
    seg = u[(u["period"] >= lo) & (u["period"] < hi)].copy()
    seg["period"] -= lo
    return float(mrd.estimate_temperament(seg, min_changes=12)["s"])


def ids_of(df, span, top_k):
    u = mrd.restrict_universe(df, top_k, buffer_mult=4, member_span=span)
    return set(u["entity_id"].unique())


def p4_verdict(s1, s2, s3):
    return bool(s1 < s2 < s3)


def p5_verdict(b4, b8):
    return bool(B_BAND[0] <= b4 <= B_BAND[1]
                and B_BAND[0] <= b8 <= B_BAND[1])


def p9_verdict(F, excess):
    return dict(p9a=bool(excess > 0), p9b=bool(0.5 <= F <= 0.75))


def mbb_ci(seg, boot=100, L=8, seed=0, min_changes=12):
    """A4.9 non-gating MBB CI for a window's s (gapped relabel; e1
    convention)."""
    import minimal_rankdiff as mrd
    rng = np.random.default_rng(seed)
    T = int(seg["period"].max()) + 1
    n_blocks = int(np.ceil(T / L))
    vals = []
    for _ in range(boot):
        parts = []
        for j, s0 in enumerate(rng.integers(0, max(T - L + 1, 1), n_blocks)):
            blk = seg[(seg["period"] >= s0) & (seg["period"] < s0 + L)].copy()
            blk["period"] = blk["period"] - s0 + j * (L + 1)
            parts.append(blk)
        try:
            import pandas as pd
            vals.append(mrd.estimate_temperament(
                pd.concat(parts, ignore_index=True),
                min_changes=min_changes)["s"])
        except Exception:
            continue
    return (np.percentile(vals, [2.5, 97.5]) if vals else (np.nan, np.nan))


def four_cell(sA, sB, sC, sD):
    """era = window effect at fixed membership; comp = membership effect at
    fixed window (means of the two one-factor contrasts; §2z-s design).
    A = (W1, mem1)  B = (W3, mem3)  C = (W1, mem3)  D = (W3, mem1)."""
    era = ((sB - sC) + (sD - sA)) / 2.0
    comp = ((sC - sA) + (sB - sD)) / 2.0
    return era, comp


def p4p5(df, top_k):
    from e1_transport import b_at_h
    print("P4 windows (own-window universe):", flush=True)
    ss = []
    for lo, hi in WINDOWS:
        u = mrd.restrict_universe(df, top_k, buffer_mult=4,
                                  member_span=(lo, hi))
        seg = u[(u["period"] >= lo) & (u["period"] < hi)].copy()
        seg["period"] -= lo
        s = float(mrd.estimate_temperament(seg, min_changes=12)["s"])
        lo_ci, hi_ci = mbb_ci(seg)                    # A4.9, non-gating
        ss.append(s)
        print(f"  W[{lo},{hi}): s = {s:.4f}  [MBB CI {lo_ci:.3f}, "
              f"{hi_ci:.3f}; non-gating]", flush=True)
    p4a = p4_verdict(*ss)
    print(f"P4a strict monotone: {'PASS' if p4a else 'FAIL'}")
    m1 = ids_of(df, WINDOWS[0], top_k)
    m3 = ids_of(df, WINDOWS[2], top_k)
    sA = s_of(df, *WINDOWS[0], member_ids=m1, top_k=top_k)
    sB = s_of(df, *WINDOWS[2], member_ids=m3, top_k=top_k)
    sC = s_of(df, *WINDOWS[0], member_ids=m3, top_k=top_k)
    sD = s_of(df, *WINDOWS[2], member_ids=m1, top_k=top_k)
    era, comp = four_cell(sA, sB, sC, sD)
    p4b = abs(era) > abs(comp)
    print(f"P4b four-cell: A={sA:.4f} B={sB:.4f} C={sC:.4f} D={sD:.4f}  "
          f"era {era:+.4f} comp {comp:+.4f}  era-dominant: "
          f"{'PASS' if p4b else 'FAIL'}")
    u = mrd.restrict_universe(df, top_k, buffer_mult=4)
    s1 = float(mrd.estimate_temperament(u, min_changes=12)["s"])
    b4 = b_at_h(u, s1, h=4)
    b8 = b_at_h(u, s1, h=8)
    ok5 = p5_verdict(b4, b8)
    print(f"P5: s(1)={s1:.4f}  b(4)={b4:.4f}  b(8)={b8:.4f}  -> P5 "
          f"{'PASS' if ok5 else 'FAIL'}")
    return dict(p4a=p4a, p4b=p4b, p5=ok5, ss=ss, era=era, comp=comp,
                b4=b4, b8=b8)


def f_ratio(vrsc13_surr_mean, vrsc13_emp, vr13_sim, vr13_emp):
    """A2.4 functional share F (the §2z-q convention, mismatch declared)."""
    denom = vr13_sim - vr13_emp
    return (vrsc13_surr_mean - vrsc13_emp) / denom if denom else np.nan


def p9(df, top_k):
    import surrogate_test as st
    u = mrd.restrict_universe(df, top_k, buffer_mult=4)
    score_k = u.attrs["score_k"]
    T = int(u["period"].max()) + 1
    ev, er, et, ers = mrd.empirical_structures(u, 10, topid_k=score_k)
    emp_d = mrd.diagnostics(ev, er, et, ers, 10, score_k=score_k)
    p = mrd.estimate(u, **LONG)
    sims = [mrd.diagnostics(*mrd._sim_struct(
        mrd.simulate(p, T, seed=s, top_record=score_k)), 10, score_k=score_k)
        for s in range(20)]
    vr13_sim = float(np.nanmean([s["VR13"] for s in sims]))
    vr13_emp = float(emp_d["VR13"])
    X = st.complete_panel(u, score_k)
    emp_sc = st.stats(X)["VRsc13"]
    rng = np.random.default_rng(0)
    surr = float(np.mean([st.stats(st.surrogate(X, rng))["VRsc13"]
                          for _ in range(50)]))
    F = f_ratio(surr, emp_sc, vr13_sim, vr13_emp)
    v = p9_verdict(F, vr13_sim - vr13_emp)
    print(f"P9: VR13 emp {vr13_emp:.4f} sim {vr13_sim:.4f} "
          f"(excess {vr13_sim - vr13_emp:+.4f}) -> P9a "
          f"{'PASS' if v['p9a'] else 'FAIL'}")
    print(f"    VRsc13 emp {emp_sc:.4f} surrogate mean {surr:.4f}  "
          f"F = {F:.3f}  in [0.5, 0.75] -> P9b "
          f"{'PASS' if v['p9b'] else 'FAIL'}")
    return dict(**v, F=F, vr13_emp=vr13_emp, vr13_sim=vr13_sim)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=("p3", "p4p5", "p9"))
    ap.add_argument("--top-k", type=int, default=None)
    a = ap.parse_args()
    panel = mrd.load_panel(mrd.PLATFORMS[PLATFORM])
    if a.what == "p3":
        p3_k(panel)
    elif a.what == "p4p5":
        assert a.top_k, "--top-k required (from p3)"
        p4p5(panel, a.top_k)
    else:
        assert a.top_k, "--top-k required (from p3)"
        p9(panel, a.top_k)
