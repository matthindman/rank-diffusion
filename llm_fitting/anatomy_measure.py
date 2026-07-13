#!/usr/bin/env python3
"""THE ANATOMY SESSION (2z-w agenda; measurement only, no estimator changes).
Dev panels for spike/tail work; extension ONLY for the labeled deep-drop
component attribution. Declared predictions, written before running:

  A1 (spike anatomy / "from below" test): if the spike story holds, sim
     rank-1 incursions originate from DEEPER ranks than empirical ones
     (higher median origin, larger origin>10 share) and hold #1 for
     SHORTER spells. Identical event definition both sides (selection
     effects cancel).
  A2 (observable tails, identical construction): sim standardized
     positive-tail q99/q90 in the CONTENDER stratum (perm rank 2-20)
     EXCEEDS empirical — the t power tail vs an empirical "shoulder".
     One-sided tails separately; entity-scale-standardized,
     common-change-removed; q99.9/q99 only where n >= 20,000.
  A3 (v_i x t product-tail hypothesis, 2x2 factorial, 20 paired seeds):
     POSITIVE interaction — the t-removal effect on S(1) is larger at
     full s than at half s.
  A4 (deep-drop component attribution, extension, 5 paired seeds;
     statistically paired interventions, NOT path-replay counterfactuals
     — replay deferred, deviation declared): the arm that moves outflux
     toward emp 0.0861 names the component. Named hypothesis on record:
     persistent/medium-timescale. NOTE these arms are attribution probes,
     not candidate models (zeroing a component breaks other moments).
"""
from __future__ import annotations

import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import community_metrics as cm  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

LONG = dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True,
            md_vr_long=True, stat_factor=True, two_scale=True,
            mix_hetero=True)


def spike_anatomy(top_ids):
    T = top_ids.shape[0]
    r1 = top_ids[:, 0]
    origins, spells = [], []
    for t in range(1, T):
        if r1[t] != r1[t - 1] and r1[t] != -1:
            pos = np.where(top_ids[t - 1] == r1[t])[0]
            origins.append(int(pos[0]) + 1 if len(pos) else -1)
    cur, L = r1[0], 1
    for t in range(1, T):
        if r1[t] == cur:
            L += 1
        else:
            spells.append(L)
            cur, L = r1[t], 1
    spells.append(L)
    cnt = Counter(x for x in r1.tolist() if x != -1)
    o = np.array([x for x in origins if x > 0])
    return dict(
        events_per_wk=len(origins) / (T - 1),
        med_origin=float(np.median(o)) if len(o) else np.nan,
        origin_gt10=float(np.mean(o > 10)) if len(o) else np.nan,
        origin_outside=float(np.mean(np.array(origins) < 0)) if origins else np.nan,
        med_spell=float(np.median(spells)),
        modal_share=max(cnt.values()) / (T if T else 1),
        n_distinct=len(cnt))


def tail_ratios(vals, ranks):
    pr = np.where(ranks > 0, ranks.astype(float), np.nan)
    perm = np.nanmean(pr, axis=0)
    dv = np.diff(vals, axis=0)
    u = dv - np.nanmean(dv, axis=1, keepdims=True)
    sd = np.nanstd(u, axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        u = u / np.where(sd > 0, sd, np.nan)
    out = {}
    for tag, lo, hi in (("contender2-20", 2, 20), ("21-100", 21, 100),
                        ("101-1000", 101, 1000)):
        cols = (perm >= lo) & (perm <= hi)
        x = u[:, cols].ravel()
        x = x[np.isfinite(x)]
        d = dict(n_cols=int(cols.sum()), n_obs=len(x))
        for side, arr in (("pos", x[x > 0]), ("neg", -x[x < 0])):
            if len(arr) >= 500:
                q90, q99 = np.percentile(arr, [90, 99])
                d[f"{side}_q99/q90"] = round(float(q99 / q90), 3)
                if len(arr) >= 20000:
                    d[f"{side}_q999/q99"] = round(
                        float(np.percentile(arr, 99.9) / q99), 3)
        out[tag] = d
    return out


def dev_panel(plat, top_k, n_seeds=10):
    df = mrd.load_panel(mrd.PLATFORMS[plat])
    df = mrd.restrict_universe(df, top_k, buffer_mult=4)
    sk = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    ev, er, et, _ = mrd.empirical_structures(df, 10, topid_k=sk)
    print(f"== {plat} T={T} ==")
    print(f"  A1 emp spike anatomy: {spike_anatomy(et)}")
    print(f"  A2 emp tails: {tail_ratios(ev, er)}", flush=True)
    p = mrd.estimate(df, **LONG)
    sims = [mrd.simulate(p, T, seed=s, top_record=sk) for s in range(n_seeds)]
    an = [spike_anatomy(s["top_ids"]) for s in sims]
    print("  A1 sim spike anatomy (mean ± sd over "
          f"{n_seeds} seeds):")
    for k in an[0]:
        v = [a[k] for a in an]
        print(f"    {k}: {np.nanmean(v):.3f} ± {np.nanstd(v):.3f}")
    uv = np.concatenate([np.asarray(s["tvals"], dtype=float) for s in sims], axis=1)
    ur = np.concatenate([np.asarray(s["tranks"], dtype=float) for s in sims], axis=1)
    print(f"  A2 sim tails (pooled {n_seeds} seeds): {tail_ratios(uv, ur)}",
          flush=True)
    return df, p, T, sk


def factorial(p, T, sk):
    print("  A3 2x2 factorial (t on/off x s full/half), S(1), 20 paired seeds:")
    arms = {"t,s": p,
            "t,s/2": replace(p, temper_s=p.temper_s * 0.5),
            "inf,s": replace(p, t_df=float("inf")),
            "inf,s/2": replace(p, t_df=float("inf"), temper_s=p.temper_s * 0.5)}
    m = {}
    for tag, p2 in arms.items():
        s1 = np.array([cm.top_share(mrd._sim_struct(
            mrd.simulate(p2, T, seed=s, top_record=sk))[3], 1)
            for s in range(20)])
        m[tag] = s1
        print(f"    [{tag:<7}] S(1) {s1.mean():.4f} ± {s1.std(ddof=0):.4f}")
    inter = m["t,s"] - m["t,s/2"] - m["inf,s"] + m["inf,s/2"]
    print(f"    interaction (per-seed paired): {inter.mean():+.4f} ± "
          f"{inter.std(ddof=0):.4f}  (positive => product-tail hypothesis "
          f"supported)")


def ext_components():
    print("== A4 (EXPLORATORY, extension): deep-drop component attribution, "
          "5 paired seeds ==")
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    df = mrd.restrict_universe(df, 12500, buffer_mult=4)
    sk = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    p = mrd.estimate(df, **LONG)
    z = np.zeros_like(np.asarray(p.sigma_trans))
    arms = {"baseline": (p, {}),
            "fast=0": (replace(p, sigma_trans=z), {}),
            "medium=0": (replace(p, sigma_trans2=z) if p.sigma_trans2
                         is not None else None, {}),
            "factor off": (p, dict(use_factor=False)),
            "perm x0.5": (replace(p, sigma_perm=np.asarray(p.sigma_perm) * 0.5), {})}

    def outflux(top_ids, K):
        sets = [set(top_ids[t, :K]) - {-1} for t in range(top_ids.shape[0])]
        of = [len(sets[t] - sets[t + 1]) / len(sets[t])
              for t in range(len(sets) - 1) if sets[t]]
        return float(np.mean(of))

    for tag, (p2, kw) in arms.items():
        if p2 is None:
            continue
        of = [outflux(mrd.simulate(p2, T, seed=s, top_record=sk, **kw)["top_ids"], sk)
              for s in range(5)]
        print(f"  {tag:<11} outfluxK {np.mean(of):.4f} ± {np.std(of):.4f}   "
              f"(emp 0.0861; baseline sim ~0.148)", flush=True)


if __name__ == "__main__":
    what = sys.argv[1]
    if what == "fb":
        df, p, T, sk = dev_panel("facebook_a", 3500)
        factorial(p, T, sk)
    elif what == "comments":
        dev_panel("reddit_comments", 12500)
    elif what == "ext":
        ext_components()
