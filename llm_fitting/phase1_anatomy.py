#!/usr/bin/env python3
"""A1 and A2 of the 2026-10-12 work plan: runner for residual_anatomy.py.

Design and predictions: llm_fitting/runs/2026-10-12_A1A2_anatomy/DESIGN.md.

    python llm_fitting/phase1_anatomy.py a1 facebook_a
    python llm_fitting/phase1_anatomy.py a2 reddit_comments_ext
    python llm_fitting/phase1_anatomy.py report
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
        j = _join(c["emp"]["dispersion"], [s["dispersion"] for s in c["sims"]],
                  ("group", "h"), "robust_var")
        lines.append("robust spread of h-week changes: data / sim mean (sim SD)")
        for g in ("head", "middle", "shell"):
            cells = [f"h{h}: {v['emp']:.3f}/{v['sim']:.3f} ({v['sd']:.3f})"
                     for (gg, h), v in j.items() if gg == g]
            lines.append(f"  {g:7s} " + "  ".join(cells))
    text = "\n".join(lines)
    (OUT / "report_tables.txt").write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "a1":
        a1(sys.argv[2])
    elif cmd == "a2":
        a2(sys.argv[2])
    else:
        report()
