#!/usr/bin/env python3
"""Root-cause probe for the two candidate error patterns surfaced by the
community-metrics layer (2026-07-06 audit; MODEL_STATUS 2z).

(1) HEAD IDENTITY DIVERSITY -- d(1): distinct rank-1 occupants per week.
    Default-estimator smoke showed FB emp 0.216 vs sim 0.074.  2o root-caused
    the unconditional head to ELECTION-WEEK INITIALIZATION (w0 = period-0
    ladder; top-2 log-gap 0.511 vs era norm 0.217) and flagged -- but never
    ran -- seeding from a typical-week ladder.  This probe runs that
    experiment with ZERO model-code change: dataclasses.replace(p, w0 =
    era-median ladder).  Pre-registered predictions:
      P1: FB median-ladder top-2 gap ~ era norm (~0.2), vs period-0 ~0.5.
      P2: under median-w0 the FB sim d(1..5) and coll1/2/5 move materially
          toward emp (>= half the gap at d(1)).
      P3: Reddit subs (week 0 unremarkable) moves little under median-w0.
(2) LONG JUMPS -- the h=1 rank-band transition kernel showed emp mass at
    band-jumps >= 2 of 0.5-4% vs sim ~0 (default-estimator smoke, t-tails
    OFF).  Quantify at the paper-grade stack (t-tails ON): occupancy-weighted
    P(band jump >= 2), per-band values, and the pooled displacement tail
    P(|dR| >= x) among rank <= 200 at h=1.  Connects to the measured
    non-Gaussianity residual (2v/2y: data increments burstier than
    t-innovations; honest dynamics target ~ +0.03).

Usage:
  python llm_fitting/head_diversity_probe.py facebook --top-k 3500 \
      --temperament --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long \
      --stat-factor --two-scale --mix-hetero --reps 10
  python llm_fitting/head_diversity_probe.py reddit --top-k 5000 \
      --temperament --min-knot-entities 8 --md-lags 6 --t-tails --stat-factor --reps 10
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402
import community_metrics as cm  # noqa: E402

JUMP_THRESHOLDS = (20, 50, 100, 200)


def median_ladder(df) -> np.ndarray:
    """Era-median rank-size curve: element-wise median over weeks of the
    sorted (descending) weekly X ladders, truncated to the shortest week."""
    per = [np.sort(g["X"].to_numpy(dtype=float))[::-1]
           for _, g in df.groupby("period")]
    L = min(len(x) for x in per)
    return np.median(np.stack([x[:L] for x in per]), axis=0)


def top2_gap(ranksize: np.ndarray) -> float:
    """Stationary mean top-2 log-gap X_(1) - X_(2) over weeks."""
    g = ranksize[:, 0] - ranksize[:, 1]
    return float(np.nanmean(g))


def disp_tail(ranks: np.ndarray, cap: int = 200) -> dict:
    """Pooled h=1 displacement tail among entities with rank <= cap at t."""
    R = np.where(ranks > 0, ranks.astype(float), np.nan)
    r0, r1 = R[:-1], R[1:]
    m = np.isfinite(r0) & np.isfinite(r1) & (r0 <= cap)
    d = np.abs(r1[m] - r0[m])
    out = {f"P(d>={t})": float(np.mean(d >= t)) for t in JUMP_THRESHOLDS}
    out["n"] = int(d.size)
    return out


def jump_mass(top_ids: np.ndarray, K: int) -> tuple[float, np.ndarray]:
    """Occupancy-weighted P(band jump >= 2) from the h=1 transition matrix,
    plus the per-start-band values.  'out' transitions are excluded (that is
    boundary flux, already a scored metric)."""
    M, mass, labels = cm.transition_matrix(top_ids, h=1, K=K)
    nb = len(labels) - 1
    per = np.full(nb, np.nan)
    for a in range(nb):
        far = [b for b in range(nb) if abs(b - a) >= 2]
        stay = M[a, :nb].sum()
        per[a] = M[a, far].sum() / stay if stay > 0 else np.nan
    w = mass / max(mass.sum(), 1.0)
    ok = np.isfinite(per)
    return float(np.sum(w[ok] * per[ok]) / max(np.sum(w[ok]), 1e-12)), per


def head_stats(top_ids: np.ndarray, ranksize: np.ndarray, K: int) -> dict:
    d = cm.rank_diversity(top_ids, kmax=10)
    out = {f"d({k})": d[k - 1] for k in (1, 2, 5, 10)}
    for cr in (1, 2, 5):
        prev, cur = top_ids[:-1, cr - 1], top_ids[1:, cr - 1]
        m = (prev >= 0) & (cur >= 0)
        out[f"coll{cr}"] = float(np.mean(prev[m] != cur[m])) if m.any() else np.nan
    out["top2gap"] = top2_gap(ranksize)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("platform")
    ap.add_argument("--top-k", type=int, default=None)
    ap.add_argument("--reps", type=int, default=10)
    ap.add_argument("--temperament", action="store_true")
    ap.add_argument("--min-knot-entities", type=int, default=None)
    ap.add_argument("--md-lags", type=int, default=None)
    ap.add_argument("--t-tails", action="store_true")
    ap.add_argument("--md-vr", action="store_true")
    ap.add_argument("--md-vr-long", action="store_true")
    ap.add_argument("--stat-factor", action="store_true")
    ap.add_argument("--two-scale", action="store_true")
    ap.add_argument("--mix-hetero", action="store_true")
    a = ap.parse_args()

    df = mrd.load_panel(mrd.PLATFORMS[a.platform])
    if a.top_k:
        df = mrd.restrict_universe(df, a.top_k, buffer_mult=4)
    score_k = df.attrs.get("score_k")
    T = int(df["period"].max()) + 1
    mean_n = df.groupby("period").size().mean()
    top_k = max(10, int(round(0.01 * (score_k if score_k else mean_n))))
    K = score_k or 100
    print(f"=== head-diversity / long-jump probe: {a.platform} T={T} "
          f"score_k={score_k} reps={a.reps} ===")

    ev, er, et, ers = mrd.empirical_structures(df, top_k, topid_k=score_k)
    emp = head_stats(et, ers, K)
    emp_jm, emp_per = jump_mass(et, K)
    emp_tail = disp_tail(er)

    p = mrd.estimate(df, temper=a.temperament, min_knot_n=a.min_knot_entities,
                     md_lags=a.md_lags, t_tails=a.t_tails, md_vr=a.md_vr,
                     md_vr_long=a.md_vr_long, stat_factor=a.stat_factor,
                     two_scale=a.two_scale, mix_hetero=a.mix_hetero)
    w0_med = median_ladder(df)
    print(f"\n  INITIALIZATION LADDERS (P1): period-0 top-2 log-gap = "
          f"{p.w0[0] - p.w0[1]:.3f}   era-median = {w0_med[0] - w0_med[1]:.3f}   "
          f"empirical stationary mean = {emp['top2gap']:.3f}")

    variants = {"period-0 w0 (committed)": p,
                "era-median w0 (2o probe)": replace(p, w0=w0_med)}
    res = {}
    for name, pv in variants.items():
        rows, jms, tails = [], [], []
        for s in range(a.reps):
            sim = mrd.simulate(pv, T, seed=s, kappa=None if a.md_lags else 0.15,
                               top_record=score_k)
            tv, tr, ti, rs = mrd._sim_struct(sim)
            rows.append(head_stats(ti, rs, K))
            jms.append(jump_mass(ti, K)[0])
            tails.append(disp_tail(tr))
            print(f"  [{name}] rep {s + 1}/{a.reps} done")
        res[name] = (rows, jms, tails)

    keys = [f"d({k})" for k in (1, 2, 5, 10)] + ["coll1", "coll2", "coll5", "top2gap"]
    hdr = f"    {'stat':<10}{'emp':>9}"
    for name in variants:
        hdr += f"{name.split()[0]:>16}{'SD':>8}"
    print("\n  HEAD IDENTITY (P2/P3): emp vs sim under the two initializations")
    print(hdr)
    for k in keys:
        line = f"    {k:<10}{emp[k]:>9.3f}"
        for name in variants:
            v = np.array([r[k] for r in res[name][0]], dtype=float)
            line += f"{np.nanmean(v):>16.3f}{np.nanstd(v):>8.3f}"
        print(line)

    print("\n  LONG JUMPS (2): h=1 kernel mass at band-jump >= 2 "
          "(occupancy-weighted, within-K)")
    line = f"    {'P(jump>=2)':<12}emp {emp_jm:>8.4f}"
    for name in variants:
        v = np.array(res[name][1], dtype=float)
        line += f"   {name.split()[0]} {np.nanmean(v):>8.4f} ± {np.nanstd(v):.4f}"
    print(line)
    print(f"    emp per-band P(jump>=2 | band): "
          + " ".join(f"{x:.4f}" for x in emp_per))

    print(f"\n  DISPLACEMENT TAIL @ h=1, start rank <= 200 "
          f"(emp n={emp_tail['n']}):")
    print(f"    {'threshold':<12}{'emp':>10}" +
          "".join(f"{name.split()[0]:>16}" for name in variants))
    for t in JUMP_THRESHOLDS:
        k = f"P(d>={t})"
        line = f"    {k:<12}{emp_tail[k]:>10.4f}"
        for name in variants:
            v = np.array([x[k] for x in res[name][2]], dtype=float)
            line += f"{np.nanmean(v):>16.4f}"
        print(line)


if __name__ == "__main__":
    main()
