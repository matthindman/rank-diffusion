#!/usr/bin/env python3
"""A1 and A2 of the 2026-10-12 work plan: runner for residual_anatomy.py.

Design and predictions: llm_fitting/runs/2026-10-12_A1A2_anatomy/DESIGN.md.

    python llm_fitting/phase1_anatomy.py a1 facebook_a
    python llm_fitting/phase1_anatomy.py a2 reddit_comments_ext
    python llm_fitting/phase1_anatomy.py report
    python llm_fitting/phase1_anatomy.py benchmark
    python llm_fitting/phase1_anatomy.py a7 facebook_a          # follow-ups a, c, d
    python llm_fitting/phase1_anatomy.py a7b reddit_comments_ext  # follow-up b
    python llm_fitting/phase1_anatomy.py report7
"""
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exit_audit as ea  # noqa: E402
import ladder_explainer as le  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import residual_anatomy as ra  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-12_A1A2_anatomy")
A1_PANELS = ("facebook_a", "reddit_submissions_long", "reddit_comments")
A2_PANELS = {"reddit_comments_ext": dict(K=12_500, t0=136, anchor=None),
             "reddit_submissions_long": dict(K=5_000, t0=None, anchor=ea.P7_ANCHOR)}
A1_SEEDS, A2_SEEDS = 20, 30


def _write(name, obj):
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / name).write_text(json.dumps(obj, indent=1, default=float))


def a1(panel):
    cfg = le.PANELS[panel]
    spec = le.spec_for(panel, "struct", cfg["struct_path"][-1])
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                               cfg["K"], buffer_mult=4)
    K, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    p = le._fit(df, spec)
    R, X, _ = ra.from_long(df)

    def stats(R, X):
        return dict(tails=ra.tails_by_horizon(R, X, K),
                    retention=ra.retention(R, X, K),
                    dispersion=ra.long_dispersion(R, X, K))

    emp, sims, t0 = stats(R, X), [], time.time()
    print(f"[{panel}] data: {R.shape[1]:,} identities, {T} weeks", flush=True)
    for s in range(A1_SEEDS):
        sim = mrd.simulate(p, T, seed=s, kappa=None, top_record=K, track=p.N,
                           track_ids=True)
        sims.append(stats(*ra.from_sim(sim)))
        print(f"    seed {s + 1}/{A1_SEEDS} ({time.time() - t0:.0f}s)", flush=True)
    _write(f"a1__{panel}.json", dict(
        panel=panel, K=K, T=T, spec=spec, seeds=A1_SEEDS, emp=emp, sims=sims,
        commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")))


def a2(panel):
    cfg = A2_PANELS[panel]
    K = cfg["K"]
    df = mrd.load_panel(mrd.PLATFORMS[panel])
    if cfg["anchor"] is not None:
        anchor = df[["period", "ts"]].drop_duplicates().set_index("ts")["period"]
        t0 = int(anchor.loc[pd.Timestamp(cfg["anchor"])])
    else:
        t0 = cfg["t0"]
    full = mrd.restrict_universe(df, K, buffer_mult=4)            # for long_dispersion
    df = mrd.restrict_universe(df, K, buffer_mult=4, member_window=t0)
    sk, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    floor = ea.N_FLOOR
    R, _, _ = ra.from_long(df)
    Rf, Xf, _ = ra.from_long(full)
    half = t0 // 2

    def stats(R):
        ev, ex = ra.departures(R, sk, t0, floor=floor)
        ev2, ex2 = ra.departures(R, sk, half, t1=t0, floor=floor)
        return dict(main=ra.departure_summary(ev, ex),
                    within_train=ra.departure_summary(ev2, ex2),
                    margins=[e["margin"] for e in ev][:20000],
                    pre_med=[e["pre_med"] for e in ev][:20000])

    ref = ea.score_matrix(R, t0, sk, K // 2, 0.7, is_sim=False)
    emp = stats(R)
    emp["dispersion"] = ra.long_dispersion(Rf, Xf, sk)
    print(f"[{panel}] t0 = {t0}, T = {T}; data rate {emp['main']['rate']:.5f} "
          f"(exit_audit: {ref['rate']:.5f}), {emp['main']['n']} events", flush=True)
    p = mrd.estimate(df, **ea.LONG)
    pf = mrd.estimate(full, **ea.LONG)
    sims, t_start = [], time.time()
    for s in range(A2_SEEDS):
        sim = mrd.simulate(p, T, seed=s, top_record=sk, track_ids=True)
        Rs, _ = ra.from_sim(sim)
        one = stats(Rs)
        one["ref_rate"] = ea.score_matrix(Rs, t0, sk, K // 2, 0.7, is_sim=True)["rate"]
        simf = mrd.simulate(pf, T, seed=s, top_record=sk, track=pf.N, track_ids=True)
        one["dispersion"] = ra.long_dispersion(*ra.from_sim(simf), sk)
        sims.append(one)
        print(f"    seed {s + 1}/{A2_SEEDS}: rate {one['main']['rate']:.5f} "
              f"({time.time() - t_start:.0f}s)", flush=True)
    _write(f"a2__{panel}.json", dict(
        panel=panel, K=K, t0=t0, T=T, seeds=A2_SEEDS, exit_audit_rate=ref["rate"],
        emp=emp, sims=sims, commit=le._commit(),
        finished=time.strftime("%Y-%m-%d %H:%M:%S")))


# --------------------------------------------------------------------------- #
def _join(emp_rows, sim_rows_list, keys, value):
    """Data value, simulation mean and SD over seeds, per key tuple."""
    out = {}
    for r in emp_rows:
        k = tuple(r[x] for x in keys)
        vals = [next((q[value] for q in s if tuple(q[x] for x in keys) == k), np.nan)
                for s in sim_rows_list]
        vals = np.array(vals, dtype=float)
        vals = vals[np.isfinite(vals)]
        if vals.size >= 5:
            m, sd = float(vals.mean()), float(vals.std(ddof=1))
            out[k] = dict(emp=r[value], sim=m, sd=sd,
                          z=(r[value] - m) / sd if sd > 0 else np.nan)
    return out


def report():
    lines = []
    for panel in A1_PANELS:
        f = OUT / "cells" / f"a1__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        lines.append(f"\n## A1 {panel} (K={c['K']}, T={c['T']}, {c['seeds']} seeds)")
        for mode in ("entity", "group"):
            j = _join([r for r in c["emp"]["tails"] if r["mode"] == mode],
                      [[r for r in s["tails"] if r["mode"] == mode] for s in c["sims"]],
                      ("group", "h"), "r9990")
            lines.append(f"q99/q90 of |standardized change|, {mode} scale: "
                         f"data / sim mean (sim SD) [z]")
            for g in ("head", "middle", "shell"):
                cells = [f"h{h}: {v['emp']:.2f}/{v['sim']:.2f} ({v['sd']:.2f}) [{v['z']:+.1f}]"
                         for (gg, h), v in j.items() if gg == g]
                lines.append(f"  {g:7s} " + "  ".join(cells))
        j = _join(c["emp"]["retention"], [s["retention"] for s in c["sims"]],
                  ("group", "cls", "k"), "retention")
        lines.append("retention of a one-week move, middle group: data / sim mean (sim SD) [z]")
        for cls in ("big_up", "big_down", "small_up", "small_down"):
            cells = [f"k{k}: {v['emp']:.2f}/{v['sim']:.2f} ({v['sd']:.2f}) [{v['z']:+.1f}]"
                     for (g, cc, k), v in j.items() if g == "middle" and cc == cls]
            lines.append(f"  {cls:10s} " + "  ".join(cells))
    for panel in A2_PANELS:
        f = OUT / "cells" / f"a2__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        lines.append(f"\n## A2 {panel} (K={c['K']}, t0={c['t0']}, T={c['T']}, {c['seeds']} seeds)")
        for split in ("main", "within_train"):
            e = c["emp"][split]
            lines.append(f"{split}: data events {e['n']}, exposure {e['exposure']}")
            for key in ("rate", "rate_slide", "rate_excursion", "rate_lasting",
                        "rate_censored", "rate_crossing", "rate_absence",
                        "rate_first", "rate_repeat", "back_within_4",
                        "back_within_13", "back_within_26", "median_margin"):
                sv = np.array([s[split].get(key, np.nan) for s in c["sims"]], dtype=float)
                sv = sv[np.isfinite(sv)]
                ev = e.get(key, np.nan)
                ratio = ev / sv.mean() if sv.size and sv.mean() > 0 else np.nan
                lines.append(f"  {key:16s} data {ev:.5f}  sim {sv.mean():.5f} "
                             f"[q10 {np.quantile(sv, .1):.5f}, q90 {np.quantile(sv, .9):.5f}]"
                             f"  data/sim {ratio:.2f}")
        e, sm = c["emp"]["main"], [s["main"] for s in c["sims"]]
        gap = e["rate"] - np.mean([x["rate"] for x in sm])
        lines.append("share of the shortfall (data rate minus sim rate) by class: " + ", ".join(
            f"{k} {100 * (e['rate_' + k] - np.mean([x['rate_' + k] for x in sm])) / gap:.0f}%"
            for k in ("slide", "excursion", "lasting", "censored")))
        lines.append("share of data events by class: " + ", ".join(
            f"{k} {100 * e['rate_' + k] / e['rate']:.1f}%"
            for k in ("slide", "excursion", "lasting", "censored"))
            + f"; repeat exits {100 * e['rate_repeat'] / e['rate']:.0f}%")
        for key, label in (("robust_var", "robust spread (MAD^2)"), ("var", "variance")):
            j = _join(c["emp"]["dispersion"], [s["dispersion"] for s in c["sims"]],
                      ("group", "h"), key)
            lines.append(f"{label} of h-week changes, full-window universe: "
                         f"data / sim mean (sim SD)")
            for g in ("head", "middle", "shell"):
                cells = [f"h{h}: {v['emp']:.3f}/{v['sim']:.3f} ({v['sd']:.3f})"
                         for (gg, h), v in j.items() if gg == g]
                lines.append(f"  {g:7s} " + "  ".join(cells))
            mid = {h: v for (gg, h), v in j.items() if gg == "middle"}
            if 26 in mid and 78 in mid:
                rs = [next(q[key] for q in s["dispersion"] if q["group"] == "middle" and q["h"] == 78)
                      / next(q[key] for q in s["dispersion"] if q["group"] == "middle" and q["h"] == 26)
                      for s in c["sims"]]
                lines.append(f"  middle, 78 weeks over 26: data {mid[78]['emp'] / mid[26]['emp']:.2f}, "
                             f"sim {np.mean(rs):.2f} (SD {np.std(rs, ddof=1):.2f}; a random walk gives 3)")
    text = "\n".join(lines)
    (OUT / "report_tables.txt").write_text(text + "\n")
    print(text)


# --------------------------------------------------------------------------- #
# A7: follow-ups (design in runs/2026-10-12_A7_followups/DESIGN.md)
# --------------------------------------------------------------------------- #
OUT7 = Path("llm_fitting/runs/2026-10-12_A7_followups")


def _write7(name, obj):
    (OUT7 / "cells").mkdir(parents=True, exist_ok=True)
    (OUT7 / "cells" / name).write_text(json.dumps(obj, indent=1, default=float))


def a7(panel):
    cfg = le.PANELS[panel]
    spec = le.spec_for(panel, "struct", cfg["struct_path"][-1])
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                               cfg["K"], buffer_mult=4)
    K, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    p = le._fit(df, spec)
    R, X, _ = ra.from_long(df)
    blocks = [b for b in (13, 26) if T >= 5 * b]

    def stats(R, X):
        return dict(tails=[r for r in ra.tails_by_horizon(R, X, K)
                           if r["mode"] in ("anchor", "entity")],
                    momentum=[r for b in blocks for r in ra.momentum(R, X, K, block=b)],
                    vol=ra.volatility_persistence(R, X, K))

    emp, sims, t0 = stats(R, X), [], time.time()
    print(f"[{panel}] data: {R.shape[1]:,} identities, {T} weeks", flush=True)
    for s in range(A1_SEEDS):
        sim = mrd.simulate(p, T, seed=s, kappa=None, top_record=K, track=p.N,
                           track_ids=True)
        sims.append(stats(*ra.from_sim(sim)))
        print(f"    seed {s + 1}/{A1_SEEDS} ({time.time() - t0:.0f}s)", flush=True)
    _write7(f"a7__{panel}.json", dict(
        panel=panel, K=K, T=T, spec=spec, seeds=A1_SEEDS, emp=emp, sims=sims,
        commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")))


def a7b(panel):
    cfg = A2_PANELS[panel]
    K = cfg["K"]
    df = mrd.load_panel(mrd.PLATFORMS[panel])
    if cfg["anchor"] is not None:
        anchor = df[["period", "ts"]].drop_duplicates().set_index("ts")["period"]
        t0 = int(anchor.loc[pd.Timestamp(cfg["anchor"])])
    else:
        t0 = cfg["t0"]
    df = mrd.restrict_universe(df, K, buffer_mult=4, member_window=t0)
    sk, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    floor, half = ea.N_FLOOR, t0 // 2
    R, _, _ = ra.from_long(df)

    def stats(R):
        return dict(main=ra.cohort_fate(R, sk, t0, floor=floor),
                    within_train=ra.cohort_fate(R, sk, half, t1=t0, floor=floor))

    emp = stats(R)
    print(f"[{panel}] t0 = {t0}, T = {T}; cohort {emp['main']['n']}", flush=True)
    p = mrd.estimate(df, **ea.LONG)
    sims, t_start = [], time.time()
    for s in range(A2_SEEDS):
        sim = mrd.simulate(p, T, seed=s, top_record=sk, track_ids=True)
        sims.append(stats(ra.from_sim(sim)[0]))
        print(f"    seed {s + 1}/{A2_SEEDS} ({time.time() - t_start:.0f}s)", flush=True)
    _write7(f"a7b__{panel}.json", dict(
        panel=panel, K=K, t0=t0, T=T, seeds=A2_SEEDS, emp=emp, sims=sims,
        commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")))


def a7c(panel):
    """Post hoc additions to A7 (DESIGN.md, addendum): direction at several
    spacings and with whole-year blocks, and whether long-run and short-run
    volatility share one amplitude."""
    if panel in A1_PANELS:
        cfg = le.PANELS[panel]
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                                   cfg["K"], buffer_mult=4)
        p = le._fit(df, le.spec_for(panel, "struct", cfg["struct_path"][-1]))
    else:                                   # the comments extension, full window
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[panel]),
                                   A2_PANELS[panel]["K"], buffer_mult=4)
        p = mrd.estimate(df, **ea.LONG)
    K, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    R, X, _ = ra.from_long(df)

    def stats(R, X):
        mom = ra.momentum(R, X, K, block=13, gaps=(1, 2, 3, 4, 6, 8))
        if T >= 4 * 52:
            mom += ra.momentum(R, X, K, block=52, gaps=(1, 2))
        return dict(momentum=mom,
                    coherence=[r for h in (13, 26) if T >= 5 * h
                               for r in ra.amplitude_coherence(R, X, K, h_long=h)])

    emp, sims, t0 = stats(R, X), [], time.time()
    print(f"[{panel}] data: {R.shape[1]:,} identities, {T} weeks", flush=True)
    for s in range(A1_SEEDS):
        sim = mrd.simulate(p, T, seed=s, kappa=None, top_record=K, track=p.N,
                           track_ids=True)
        sims.append(stats(*ra.from_sim(sim)))
        print(f"    seed {s + 1}/{A1_SEEDS} ({time.time() - t0:.0f}s)", flush=True)
    _write7(f"a7c__{panel}.json", dict(
        panel=panel, K=K, T=T, seeds=A1_SEEDS, emp=emp, sims=sims,
        commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")))


def report7():
    lines = []
    for panel in A1_PANELS:
        f = OUT7 / "cells" / f"a7__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        lines.append(f"\n## A7 {panel} (K={c['K']}, T={c['T']}, {c['seeds']} seeds)")
        for mode in ("anchor", "entity"):
            j = _join([r for r in c["emp"]["tails"] if r["mode"] == mode],
                      [[r for r in s["tails"] if r["mode"] == mode] for s in c["sims"]],
                      ("group", "h"), "r9990")
            lines.append(f"q99/q90, {mode} scale: data / sim mean (sim SD) [data - sim]")
            for g in ("head", "middle", "shell"):
                cells = [f"h{h}: {v['emp']:.2f}/{v['sim']:.2f} ({v['sd']:.2f}) [{v['emp'] - v['sim']:+.2f}]"
                         for (gg, h), v in j.items() if gg == g]
                lines.append(f"  {g:7s} " + "  ".join(cells))
        j = _join(c["emp"]["momentum"], [s["momentum"] for s in c["sims"]],
                  ("group", "block", "pair"), "slope")
        lines.append("momentum slope: data / sim mean (sim SD)")
        for (g, b, pair), v in j.items():
            lines.append(f"  {g:7s} block {b:2d} {pair:8s} {v['emp']:+.3f} / {v['sim']:+.3f} ({v['sd']:.3f})")
        for key in ("corr_interleaved", "corr_halves", "ratio"):
            j = _join(c["emp"]["vol"], [s["vol"] for s in c["sims"]], ("group",), key)
            lines.append(f"volatility {key}: " + "  ".join(
                f"{g} {v['emp']:.3f}/{v['sim']:.3f} ({v['sd']:.3f})" for (g,), v in j.items()))
    for panel in A2_PANELS:
        f = OUT7 / "cells" / f"a7b__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        lines.append(f"\n## A7b {panel} (K={c['K']}, t0={c['t0']}, T={c['T']}, {c['seeds']} seeds)")
        for split in ("main", "within_train"):
            e = c["emp"][split]
            lines.append(f"{split}: cohort {e['n']}, present in the scoring window {e['n_present']}")
            for key in [k for k in e if k not in ("n", "n_present")]:
                sv = np.array([s[split].get(key, np.nan) for s in c["sims"]], dtype=float)
                sv = sv[np.isfinite(sv)]
                lines.append(f"  {key:20s} data {e[key]:.4f}  sim {sv.mean():.4f} "
                             f"[q10 {np.quantile(sv, .1):.4f}, q90 {np.quantile(sv, .9):.4f}]")
    for panel in A1_PANELS + ("reddit_comments_ext",):
        f = OUT7 / "cells" / f"a7c__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        lines.append(f"\n## A7c {panel} (K={c['K']}, T={c['T']}, {c['seeds']} seeds)")
        j = _join(c["emp"]["momentum"], [s["momentum"] for s in c["sims"]],
                  ("group", "block", "pair"), "slope")
        lines.append("slope of a later change on an earlier one, middle group: data / sim mean (sim SD)")
        for b in (13, 52):
            cells = [f"{pair}: {v['emp']:+.3f}/{v['sim']:+.3f} ({v['sd']:.3f})"
                     for (g, bb, pair), v in j.items() if g == "middle" and bb == b]
            if cells:
                lines.append(f"  {b}-week blocks  " + "  ".join(cells))
        for key in ("coherence", "rel_short", "rel_long"):
            j = _join(c["emp"]["coherence"], [s["coherence"] for s in c["sims"]],
                      ("group", "h"), key)
            lines.append(f"amplitude {key}: " + "  ".join(
                f"{g} h{h} {v['emp']:.3f}/{v['sim']:.3f} ({v['sd']:.3f})"
                for (g, h), v in j.items()))
    text = "\n".join(lines)
    (OUT7 / "report_tables.txt").write_text(text + "\n")
    print(text)


def benchmark(K=3_500, seeds=5):
    """q99/q90 (entity scale, middle group) for a Gaussian random walk at each
    A1 panel length.  The large-sample value is 1.57; with each endpoint's
    scale estimated from overlapping changes it is higher and rises with h."""
    out = {}
    for T in (86, 136, 212):
        vals = {}
        for seed in range(seeds):
            rng = np.random.default_rng(seed)
            home = np.sort(rng.normal(0, 1.5, K))[::-1]
            X = home[None, :] + np.cumsum(rng.normal(0, 0.05, (T, K)), axis=0)
            R = (-X).argsort(axis=1).argsort(axis=1) + 1.0
            for r in ra.tails_by_horizon(R, X, K):
                if r["group"] == "middle" and r["mode"] == "entity":
                    vals.setdefault(r["h"], []).append(r["r9990"])
        out[T] = {h: round(float(np.mean(v)), 3) for h, v in vals.items()}
        print(T, out[T])
    (OUT / "gaussian_walk_benchmark.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "a1":
        a1(sys.argv[2])
    elif cmd == "a2":
        a2(sys.argv[2])
    elif cmd == "benchmark":
        benchmark()
    elif cmd == "a7":
        a7(sys.argv[2])
    elif cmd == "a7b":
        a7b(sys.argv[2])
    elif cmd == "a7c":
        a7c(sys.argv[2])
    elif cmd == "report7":
        report7()
    else:
        report()
