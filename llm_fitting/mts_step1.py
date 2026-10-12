#!/usr/bin/env python3
"""M1: the balance accounting on a panel (movement-to-structure agenda, step 1).

Protocol, estimands, predictions and stopping rules:
llm_fitting/MTS_PROTOCOL_STEPS_1_2_2026-10-13.md.  Statistics: balance_accounting.py.
Data only: no fit, no simulation, no gate evaluation.

    python llm_fitting/mts_step1.py run reddit_submissions_long --reviewed
    python llm_fitting/mts_step1.py report

`run` on a registered panel refuses to start without --reviewed: the protocol is
reviewed before anything runs on the panels (agenda, "First deliverable").
analyse() is the one code path for real and planted panels.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-13_M1_balance")
PANELS = {                      # platform key in minimal_rankdiff.PLATFORMS, K, block length in weeks
    "facebook_a": dict(platform="facebook_a", K=3500, block=26),
    "reddit_comments_ext": dict(platform="reddit_comments_ext", K=12500, block=52),
    "reddit_submissions_long": dict(platform="reddit_submissions_long", K=5000, block=52),
    "reddit_comments": dict(platform="reddit_comments", K=12500, block=26),   # declared secondary
}
HORIZONS = (1, 4, 13, 26, 52)
BOUNDARIES = (0.5, 1.0, 2.0)    # centre and cohort cutoff, as multiples of K
TRACK_MULT = 4                  # endpoints ever inside the weekly top 4K are tracked
BOOT = 400


def matrices(df, K):
    """Level and weekly-rank matrices for every endpoint that is ever inside the
    weekly top TRACK_MULT * K, from a long panel with columns entity_id, period,
    rank (among everything observed that week), X and N.  Also the weekly
    floors N_t + 1 and, per week, the level at rank TRACK_MULT * K + 1 (the most
    an untracked endpoint can have)."""
    T = int(df["period"].max()) + 1
    cut = TRACK_MULT * K
    keep = df.loc[df["rank"] <= cut, "entity_id"].unique()
    sub = df[df["entity_id"].isin(keep)]
    x = sub.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    r = sub.pivot(index="period", columns="entity_id", values="rank").reindex(range(T))
    X = x.to_numpy(dtype=float)
    R = np.nan_to_num(r.to_numpy(dtype=float), nan=0.0).astype(np.int64)
    floors = df.groupby("period")["N"].first().reindex(range(T)).to_numpy(dtype=float) + 1.0
    outside = (df[df["rank"] == cut + 1].set_index("period")["X"].reindex(range(T)).to_numpy(dtype=float))
    return X, R, floors, outside, np.asarray(x.columns)


def _ks(K, top):
    ks, k = [], float(top)
    while k >= 12:
        ks.append(int(round(k)))
        k /= 2
    return sorted(set(ks))


def _summ(res, ks, n_seg=4):
    """Mean over origins and the range over n_seg consecutive calendar segments."""
    out = {}
    seg = np.array_split(np.arange(res["origins"].size), n_seg)
    for q in ("g", "r", "f", "shape", "r_diff", "step", "swap"):
        a = res[q]
        out[q] = dict(mean=np.nanmean(a, axis=0).tolist(),
                      seg_min=np.nanmin([np.nanmean(a[s], axis=0) for s in seg if s.size], axis=0).tolist(),
                      seg_max=np.nanmax([np.nanmean(a[s], axis=0) for s in seg if s.size], axis=0).tolist())
    out["max_abs_resid"] = float(np.nanmax(np.abs(res["resid"])))
    out["n_origins"] = int(res["origins"].size)
    out["ks"] = list(ks)
    return out


def analyse(df, K, block, horizons=HORIZONS, boundaries=BOUNDARIES, boot=BOOT):
    """Parts A and B of the protocol for one panel.  Returns a JSON-ready dict."""
    X, R, floors, outside, ids = matrices(df, K)
    T = X.shape[0]
    out = dict(K=int(K), T=int(T), n_tracked=int(X.shape[1]), block=int(block), boundaries={})
    nb = T // block
    start = T - nb * block                             # drop the earliest weeks, keep whole blocks
    M = ba.block_means(X, block, start=start)
    # the top 2K of the block ladder is exact if no untracked endpoint could reach it
    bound = np.array([np.nanmax(outside[start + b * block:start + (b + 1) * block]) for b in range(nb)])
    out["blocks"] = dict(n=int(nb), start=int(start),
                         level_at_2K=[float(ba.ladder(M[b], 2 * K)[-1]) if np.isfinite(M[b]).sum() >= 2 * K else None
                                      for b in range(nb)],
                         untracked_bound=bound.tolist())
    for mult in boundaries:
        kc = int(round(mult * K))
        ks = _ks(K, kc)
        cell = dict(k_centre=kc, ks=ks)
        # ---- Part A: exact accounting on observed weekly positions --------------------------
        cell["weekly"] = {}
        for h in horizons:
            if h >= T // 2:
                continue
            origins = np.arange(0, T - h, max(1, h // 2))
            cell["weekly"][str(h)] = _summ(ba.accounting_series(X, h, ks, kc, origins), ks)
        # ---- Part B: block means, cohorts fixed on an earlier block -------------------------
        c = ba.centre_series(M, kc)
        blocks = dict(centre=c.tolist(), pairs=[])
        for sel in range(nb - 2):
            lo, hi = start + sel * block, start + (sel + 1) * block
            pr, pres = ba.perm_rank(R, lo, hi, floors)
            labels, which = ba.cohort_bins(pr, kc, pres)
            for b0 in range(sel + 1, nb - 1):
                for b1 in range(b0 + 1, nb):
                    drift = ba.cohort_drift(M, c, which, len(labels), b0, b1, n_boot=boot)
                    acc = ba.topk_accounting(M[b0], M[b1], ks, kc)
                    pair = dict(select=sel, start=b0, end=b1, weeks=int((b1 - b0) * block),
                                bins=[list(map(float, lb)) if lb != "outside" else lb for lb in labels],
                                drift=drift, accounting=acc,
                                diffusion=[ba.diffusion_gain(M[b0], M[b1], k, kc) for k in ks],
                                predictable=ba.predictable_loss(M, c, which, drift, b0, ks),
                                occupancy_at_start=ba.occupancy(which, len(labels), M[b0], ks))
                    if b1 == b0 + 1:
                        inside = which < len(labels) - 1
                        pair["carriers"] = ba.carrier_regression(
                            M, c, b_pos=sel if b0 == sel + 1 else b0 - 1, b_start=b0, b_end=b1, mask=inside,
                            b_home=sel if b0 - 1 > sel else None, n_boot=boot)
                    blocks["pairs"].append(pair)
        cell["blocks"] = blocks
        out["boundaries"][str(mult)] = cell
    return out


def run(panel, reviewed, block=None):
    if not reviewed:
        sys.exit("refusing to run on a registered panel without --reviewed: the protocol "
                 "(MTS_PROTOCOL_STEPS_1_2_2026-10-13.md) is reviewed before anything runs")
    import minimal_rankdiff as mrd
    cfg = PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    res = analyse(df, cfg["K"], block or cfg["block"])
    res.update(panel=panel, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    name = f"m1__{panel}.json" if block is None else f"m1__{panel}__block{block}.json"
    (OUT / "cells" / name).write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] T={res['T']} tracked={res['n_tracked']} blocks={res['blocks']['n']} written", flush=True)


def report():
    for f in sorted((OUT / "cells").glob("m1__*.json")):
        d = json.loads(f.read_text())
        cell = d["boundaries"]["1.0"]
        print(f"\n== {d['panel']}  K={d['K']}  T={d['T']}  tracked={d['n_tracked']}")
        print("weekly positions, rates per endpoint per week: h | k | g (occupant loss) r (reordering) "
              "r_diff (k v / 2S) f shape")
        for h, w in cell["weekly"].items():
            for j, k in enumerate(w["ks"]):
                print(f"  {h:>3} {k:>6}  {w['g']['mean'][j]:.5f} {w['r']['mean'][j]:.5f} "
                      f"{w['r_diff']['mean'][j]:.5f} {w['f']['mean'][j]:+.5f} {w['shape']['mean'][j]:+.5f}")
        for p in cell["blocks"]["pairs"]:
            print(f"blocks select={p['select']} {p['start']}->{p['end']} ({p['weeks']} weeks)")
            for a, pl, dg in zip(p["accounting"], p["predictable"], p["diffusion"]):
                kw = a["k"] * p["weeks"]
                cov = pl["G_pred"] / a["R"] if a["R"] > 0 else float("nan")
                print(f"  k={a['k']:>6}  g={a['G'] / kw:.5f} r={a['R'] / kw:.5f} r_diff={dg['R_diff'] / kw:.5f} "
                      f"f={a['F'] / kw:+.5f} shape={a['shape'] / kw:+.5f}  predictable/R={cov:.2f}")
            if "carriers" in p:
                print("  carriers:", {k: round(v, 4) for k, v in p["carriers"].items()})


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("panel", choices=sorted(PANELS))
    r.add_argument("--reviewed", action="store_true")
    r.add_argument("--block", type=int, default=None,
                   help="declared secondary runs only (protocol section 7)")
    sub.add_parser("report")
    a = ap.parse_args()
    run(a.panel, a.reviewed, a.block) if a.cmd == "run" else report()


if __name__ == "__main__":
    main()
