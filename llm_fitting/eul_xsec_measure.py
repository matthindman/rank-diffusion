#!/usr/bin/env python3
"""Head-law MEASURE-FIRST session (2z-u step 3; dev panels for the design
work, extension ONLY for the labeled-exploratory 2c discriminator).
No estimator changes here — measurement only, per the 8-point design spec.

Declared discriminations (written before running):
  D1 (spacing): emp head-ladder spacings X(1)-X(2), X(1)-X(10), X(10)-X(100)
      vs sim (10 seeds). The S(1) overshoot must appear as excess sim
      spacing at the very top if it is a stationary-law width problem.
  D2 (dispersion split, emp): between-entity home dispersion vs
      within-entity dispersion at the head (permanent rank <= 40), net of
      the common level path — vs the model-implied around-home stationary
      level. Variant 1 (2z-t) showed within-entity variance >> implied;
      if sim STILL overshoots spacing with a SMALLER around-home level,
      the excess is not plain dispersion magnitude.
  D3 (attribution grid, facebook_a): S(1) under interventions
      temper_s x {0.5, 0.75, 1.0} and head-third kappa x {1, 2, 4},
      3 seeds each, CRN. Which lever moves S(1) toward emp 0.017?
      Direction deliberately NOT predicted — this IS the discrimination.
  D4 (2c discriminator, extension, EXPLORATORY): tracked-population core
      exits (rank <= K/2 at t, gone from top-K at t+1) decomposed into
      ABSENCE (rank 0) vs CROSSING (present below K), and exit rates by
      trailing-8-week realized-volatility quintile (measurable identically
      emp/sim — adaptation of the v-quintile idea, declared; v_i is not
      observable). Absence-dominant => exit-machinery story; crossing-
      dominant + top-quintile-concentrated => merges with the head-law /
      amplitude-tail family. Tracked population = >=70% presence both
      sides (declared conditioning).
"""
from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

STACKS = {
    "facebook_a": (3500, dict(temper=True, min_knot_n=8, md_lags=6,
                              t_tails=True, md_vr_long=True, stat_factor=True,
                              two_scale=True, mix_hetero=True)),
    "reddit_comments": (12500, dict(temper=True, min_knot_n=8, md_lags=6,
                                    t_tails=True, md_vr_long=True,
                                    stat_factor=True, two_scale=True,
                                    mix_hetero=True)),
}


def spacings(rs):
    d = {}
    for a, b, tag in ((0, 1, "X(1)-X(2)"), (0, 9, "X(1)-X(10)"),
                      (9, 99, "X(10)-X(100)")):
        d[tag] = float(np.nanmean(rs[:, a] - rs[:, b]))
    return d


def head_split(df, head_n=40):
    """Emp between-home vs within-entity dispersion at the head, net of the
    common level path (cumsum of the cross-sec mean change)."""
    df = df.sort_values(["entity_id", "period"])
    eid = df["entity_id"].to_numpy()
    per = df["period"].to_numpy()
    X = df["X"].to_numpy()
    last = int(per.max())
    same = np.zeros(len(df), dtype=bool)
    same[:-1] = (eid[1:] == eid[:-1]) & (per[1:] == per[:-1] + 1)
    dX = np.full(len(df), np.nan); dX[:-1] = X[1:] - X[:-1]
    pc = np.bincount(per[same], minlength=last + 1).astype(float)
    F = np.divide(np.bincount(per[same], weights=dX[same], minlength=last + 1),
                  pc, out=np.zeros(last + 1), where=pc > 0)
    lev = np.cumsum(F)
    import pandas as pd
    d = pd.DataFrame({"e": eid, "y": X - lev[np.clip(per, 0, last)],
                      "r": df["rank"].to_numpy()})
    pr = d.groupby("e")["r"].mean().sort_values()
    head = set(pr.index[:head_n])
    dh = d[d["e"].isin(head)]
    g = dh.groupby("e")["y"]
    between = float(g.mean().var(ddof=0))
    within = float(g.var(ddof=0).mean())
    return between, within


def s1_of(p, T, score_k, seeds=range(3)):
    import community_metrics as cm
    return [cm.top_share(mrd._sim_struct(
        mrd.simulate(p, T, seed=s, top_record=score_k))[3], 1) for s in seeds]


def part_a(plat):
    top_k, kw = STACKS[plat]
    df = mrd.load_panel(mrd.PLATFORMS[plat])
    df = mrd.restrict_universe(df, top_k, buffer_mult=4)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    _, _, _, ers = mrd.empirical_structures(df, 10, topid_k=score_k)
    print(f"== {plat} T={T} ==")
    print(f"  D1 emp spacings: {spacings(ers)}")
    p = mrd.estimate(df, **kw)
    sim_sp = []
    for s in range(10):
        rs = mrd._sim_struct(mrd.simulate(p, T, seed=s, top_record=score_k))[3]
        sim_sp.append(spacings(rs))
    for k in sim_sp[0]:
        v = [d[k] for d in sim_sp]
        print(f"  D1 sim {k}: {np.mean(v):.4f} ± {np.std(v):.4f}")
    bw, wi = head_split(df)
    a = 1.0 - np.asarray(p.kappa_z[:4])
    W = float(np.mean(np.asarray(p.sigma_perm[:4]) ** 2
                      / np.clip(1 - a ** 2, 1e-9, None)))
    print(f"  D2 emp head(perm<=40): between-home var {bw:.4f}  "
          f"within-entity var {wi:.4f}  | model head W {W:.4f}, "
          f"implied around-home level see 2z-t (FB 0.19)")
    if plat == "facebook_a":
        print("  D3 attribution grid: S(1) mean over 3 CRN seeds "
              f"(emp 0.0170; flag-off sim ref 0.0463)")
        for fs in (1.0, 0.75, 0.5):
            for fk in (1, 2, 4):
                kz = np.asarray(p.kappa_z, dtype=float).copy()
                n3 = max(1, len(kz) // 3)
                kz[:n3] = np.clip(kz[:n3] * fk, None, 0.9)
                p2 = replace(p, temper_s=p.temper_s * fs, kappa_z=kz)
                s1 = s1_of(p2, T, score_k)
                print(f"    s x{fs:<5} kappa_head x{fk}:  "
                      f"S(1) {np.mean(s1):.4f} ± {np.std(s1):.4f}")


def part_b():
    print("== D4 (EXPLORATORY, extension): core exits — absence vs crossing, "
          "by realized-vol quintile ==")
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    df = mrd.restrict_universe(df, 12500, buffer_mult=4)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    ev, er, _, _ = mrd.empirical_structures(df, 10, topid_k=score_k)
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
                     md_vr_long=True, stat_factor=True, two_scale=True,
                     mix_hetero=True)

    def decomp(vals, ranks, tag):
        K, half = score_k, score_k // 2
        inK = (ranks > 0) & (ranks <= half)              # core at t
        ev_t = inK[:-1] & ((ranks[1:] == 0) | (ranks[1:] > K))
        absent = inK[:-1] & (ranks[1:] == 0)
        n_ev, n_abs = int(ev_t.sum()), int(absent.sum())
        print(f"  {tag}: core-exit events {n_ev:,}  absence share "
              f"{n_abs / max(n_ev, 1):.3f}  crossing share "
              f"{(n_ev - n_abs) / max(n_ev, 1):.3f}")
        # trailing-8wk vol quintiles among core entity-weeks
        T_, n = vals.shape
        vol = np.full((T_, n), np.nan)
        for t in range(8, T_):
            vol[t] = np.nanstd(np.diff(vals[t - 8:t], axis=0), axis=0)
        vq = vol[:-1][ev_t]
        vall = vol[:-1][inK[:-1] & np.isfinite(vol[:-1])]
        if np.isfinite(vq).sum() > 20:
            qs = np.nanpercentile(vall, [20, 40, 60, 80])
            counts = np.histogram(vq[np.isfinite(vq)],
                                  bins=[-np.inf, *qs, np.inf])[0]
            base = np.histogram(vall, bins=[-np.inf, *qs, np.inf])[0]
            rr = counts / np.maximum(base, 1)
            rr = rr / rr.mean() if rr.mean() else rr
            print(f"  {tag}: exit rate by vol quintile (rel to mean) "
                  f"{np.array2string(rr, precision=2)}")

    decomp(ev, er, "emp")
    for s in range(3):
        sim = mrd.simulate(p, T, seed=s, top_record=score_k)
        decomp(sim["tvals"], sim["tranks"], f"sim seed {s}")


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what in ("facebook_a", "reddit_comments"):
        part_a(what)
    elif what == "ext":
        part_b()
    else:
        part_a("facebook_a")
        part_a("reddit_comments")
        part_b()
