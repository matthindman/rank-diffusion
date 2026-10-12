#!/usr/bin/env python3
"""A8: the fitted model simulated with reversion to the home switched off.

An intervention in a fitted model, not a refit and not a candidate: every
parameter stays as fitted and `simulate(..., kappa=0.0)`, an existing
argument, makes the lasting level a random walk.  Design and predictions:
llm_fitting/runs/2026-10-12_A8_no_reversion/DESIGN.md.

    python llm_fitting/phase1_a8.py struct facebook_a
    python llm_fitting/phase1_a8.py dep reddit_comments_ext
    python llm_fitting/phase1_a8.py report
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
import phase1_a3 as a3  # noqa: E402
import residual_anatomy as ra  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-12_A8_no_reversion")
STRUCT_PANELS = ("facebook_a", "reddit_submissions_long", "reddit_comments")
DEP_PANELS = {"reddit_comments_ext": dict(K=12_500, t0=136, anchor=None),
              "reddit_submissions_long": dict(K=5_000, t0=None, anchor=ea.P7_ANCHOR)}
ARMS = {"base": None, "no_reversion": 0.0}       # value passed as simulate(kappa=...)
STRUCT_SEEDS, DEP_SEEDS = 20, 30


def _write(name, obj):
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / name).write_text(json.dumps(obj, indent=1, default=float))


def struct(panel):
    """Full-window universe, the panel's frozen structure stack: the card and
    the ladder statistics, the head, the spread by horizon and direction."""
    cfg = le.PANELS[panel]
    spec = le.spec_for(panel, "struct", cfg["struct_path"][-1])
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                               cfg["K"], buffer_mult=4)
    K, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    top_k = max(10, int(round(0.01 * K)))
    p = le._fit(df, spec)
    ev, er, et, ers = mrd.empirical_structures(df, top_k, topid_k=K)
    M = int(min(ers.shape[1], min(p.N, 2000)))
    R, X, _ = ra.from_long(df)

    def moves(R, X):
        mom = ra.momentum(R, X, K, block=13, gaps=(1, 2, 4))
        if T >= 4 * 52:
            mom += ra.momentum(R, X, K, block=52, gaps=(1, 2))
        return dict(dispersion=ra.long_dispersion(R, X, K), momentum=mom)

    emp = dict(card=le._stats(ev, er, et, ers, top_k, K, M), head=a3.head_stats(ers, M),
               **moves(R, X))
    arms, t0 = {}, time.time()
    for arm, kap in ARMS.items():
        rows = []
        for s in range(STRUCT_SEEDS):
            # default tracking, the stream of the recorded cards
            sim = mrd.simulate(p, T, seed=s, kappa=kap, top_record=K)
            one = dict(card=le._stats(*mrd._sim_struct(sim), top_k, K, M),
                       head=a3.head_stats(sim["ranksize"], M))
            # every slot tracked, for the endpoint-level statistics
            sim = mrd.simulate(p, T, seed=s, kappa=kap, top_record=K, track=p.N,
                               track_ids=True)
            one.update(moves(*ra.from_sim(sim)))
            rows.append(one)
        arms[arm] = rows
        mean = le._seed_mean([r["card"] for r in rows])
        npass, ntot, churn = mrd._score(emp["card"], mean, top_k)
        idx, _ = le.macro_index(emp["card"], mean, top_k)
        print(f"[{panel}] {arm:13s} card {npass}/{ntot}, churn {churn:.3f}, index {idx:.2f} "
              f"({time.time() - t0:.0f}s)", flush=True)
    _write(f"struct__{panel}.json", dict(
        panel=panel, K=K, T=T, top_k=top_k, M=M, spec=spec, seeds=STRUCT_SEEDS, emp=emp,
        arms=arms, commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")))


def dep(panel):
    """The A2 settings: train-only universe, LONG stack, established cohort."""
    cfg = DEP_PANELS[panel]
    K = cfg["K"]
    df = mrd.load_panel(mrd.PLATFORMS[panel])
    if cfg["anchor"] is not None:
        anchor = df[["period", "ts"]].drop_duplicates().set_index("ts")["period"]
        t0 = int(anchor.loc[pd.Timestamp(cfg["anchor"])])
    else:
        t0 = cfg["t0"]
    df = mrd.restrict_universe(df, K, buffer_mult=4, member_window=t0)
    sk, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    floor = ea.N_FLOOR

    def stats(R):
        ev, ex = ra.departures(R, sk, t0, floor=floor)
        return dict(departures=ra.departure_summary(ev, ex),
                    fate=ra.cohort_fate(R, sk, t0, floor=floor))

    emp = stats(ra.from_long(df)[0])
    p = mrd.estimate(df, **ea.LONG)
    arms, t_start = {}, time.time()
    for arm, kap in ARMS.items():
        arms[arm] = [stats(ra.from_sim(mrd.simulate(p, T, seed=s, kappa=kap, top_record=sk,
                                                    track_ids=True))[0])
                     for s in range(DEP_SEEDS)]
        print(f"[{panel}] {arm:13s} rate "
              f"{np.mean([r['departures']['rate'] for r in arms[arm]]):.5f} "
              f"(data {emp['departures']['rate']:.5f}) ({time.time() - t_start:.0f}s)", flush=True)
    _write(f"dep__{panel}.json", dict(
        panel=panel, K=K, t0=t0, T=T, seeds=DEP_SEEDS, emp=emp, arms=arms,
        commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")))


def report():
    lines = []
    for panel in STRUCT_PANELS:
        f = OUT / "cells" / f"struct__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        e = c["emp"]
        lines.append(f"\n## structure, {panel} (K={c['K']}, T={c['T']}, {c['seeds']} seeds)")
        for arm, rows in c["arms"].items():
            mean = le._seed_mean([r["card"] for r in rows])
            npass, ntot, churn = mrd._score(e["card"], mean, c["top_k"])
            idx, blocks = le.macro_index(e["card"], mean, c["top_k"])
            s1 = np.array([r["head"]["S1"] for r in rows])
            lines.append(
                f"{arm:13s} card {npass}/{ntot}  churn {churn:.3f}  index {idx:.2f}  blocks "
                + " ".join(f"{b} {v:.2f}" for b, v in blocks.items())
                + f"  S1/data {s1.mean() / e['head']['S1']:.2f} ({s1.std(ddof=1) / e['head']['S1']:.2f})"
                + f"  ladder drift {mean['ladder_drift']:.3f} (data {e['card']['ladder_drift']:.3f})")
        for g in ("head", "middle", "shell"):
            cells = []
            for h in (13, 26, 52, 78):
                ev = next((r["var"] for r in e["dispersion"] if r["group"] == g and r["h"] == h), None)
                if ev is None:
                    continue
                vals = [np.mean([next(q["var"] for q in r["dispersion"] if q["group"] == g and q["h"] == h)
                                 for r in rows]) for rows in c["arms"].values()]
                cells.append(f"h{h} {ev:.3f} / " + " / ".join(f"{v:.3f}" for v in vals))
            lines.append(f"  variance of h-week changes, {g:7s} data / " + " / ".join(c["arms"]) + ":  "
                         + "   ".join(cells))
        for er in e["momentum"]:
            if er["group"] != "middle":
                continue
            vals = [np.mean([next(q["slope"] for q in r["momentum"] if q["group"] == "middle"
                                  and q["block"] == er["block"] and q["pair"] == er["pair"])
                             for r in rows]) for rows in c["arms"].values()]
            lines.append(f"  direction, middle, {er['block']}-week blocks {er['pair']}: data "
                         f"{er['slope']:+.3f} / " + " / ".join(f"{v:+.3f}" for v in vals))
    for panel in DEP_PANELS:
        f = OUT / "cells" / f"dep__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        lines.append(f"\n## departures, {panel} (K={c['K']}, t0={c['t0']}, {c['seeds']} seeds): "
                     f"data / " + " / ".join(c["arms"]))
        for block, keys in (("departures", ("rate", "rate_slide", "rate_excursion", "rate_lasting",
                                            "rate_first", "rate_repeat", "back_within_13")),
                            ("fate", ("share_relocated", "share_gone", "share_time_outside",
                                      "exits_per_exiting", "shift_q50", "shift_q75", "shift_q95",
                                      "first_slide", "first_lasting"))):
            for k in keys:
                cells = []
                for rows in c["arms"].values():
                    v = np.array([r[block].get(k, np.nan) for r in rows], dtype=float)
                    v = v[np.isfinite(v)]
                    cells.append(f"{v.mean():.5f} [{np.quantile(v, .1):.5f}, {np.quantile(v, .9):.5f}]")
                lines.append(f"  {k:20s} {c['emp'][block][k]:.5f} / " + " / ".join(cells))
    text = "\n".join(lines)
    (OUT / "report_tables.txt").write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "struct":
        struct(sys.argv[2])
    elif cmd == "dep":
        dep(sys.argv[2])
    else:
        report()
