#!/usr/bin/env python3
"""A5 of the 2026-10-12 work plan: the ladder cell that was not run.

R4m = the R3 fit + --temperament --mix-hetero, structure only, through the
ladder's own run_struct so that statistics, universe and seeds are those of
MODEL_STATUS 2z-ap.  Design and predictions:
llm_fitting/runs/2026-10-12_A5_full_amplitude/DESIGN.md.

    python llm_fitting/phase1_a5.py run        # cells not yet on disk
    python llm_fitting/phase1_a5.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ladder_explainer as le  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-12_A5_full_amplitude")
LADDER = Path("llm_fitting/runs/2026-10-12_ladder/cells")
PANELS = ["reddit", "facebook_a", "instagram", "reddit_submissions_long",
          "reddit_comments"]
REPS = 20


def run() -> None:
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    base = le.spec_for
    for panel in PANELS:
        dest = OUT / "cells" / f"{panel}__struct__R4m.json"
        if dest.exists():
            continue
        cache = le.PanelCache()
        spec = dict(base(panel, "struct", "R4"), mix_hetero=True)
        le.spec_for = lambda p, k, r, _s=spec: dict(_s)
        try:
            t0 = time.time()
            print(f"##### {panel} R4m {spec}", flush=True)
            cell = le.run_struct(panel, "R4", REPS)
        finally:
            le.spec_for = base
        cache.check_unmutated()
        le.mrd.load_panel, le.mrd.restrict_universe = cache._load, cache._ru
        cell.update(rung="R4m", spec=spec, params=le.param_count(spec),
                    commit=le._commit(), seconds=round(time.time() - t0, 1),
                    finished=time.strftime("%Y-%m-%d %H:%M:%S"))
        le._write_json(dest, cell)
        print(f"##### done {panel} in {cell['seconds']:.0f}s", flush=True)


def _index(cell, idx=None):
    return le.macro_index(cell["emp"], le._seed_mean(cell["seeds"], idx),
                          cell["top_k"])


def report() -> None:
    rng = np.random.default_rng(20261012)
    rows = {}
    for panel in PANELS:
        c = {r: json.loads((LADDER / f"{panel}__struct__{r}.json").read_text())
             for r in ("R3", "R4")}
        c["R4m"] = json.loads((OUT / "cells" / f"{panel}__struct__R4m.json").read_text())
        n = len(c["R4m"]["seeds"])
        assert all(len(c[r]["seeds"]) == n for r in c)
        draws = rng.integers(0, n, size=(2000, n))
        idx = {r: _index(c[r]) for r in c}
        d_m = np.array([_index(c["R4m"], i)[0] - _index(c["R4"], i)[0] for i in draws])
        d_4 = np.array([_index(c["R4"], i)[0] - _index(c["R3"], i)[0] for i in draws])
        loss = idx["R4"][0] - idx["R3"][0]
        s1 = {r: le._seed_mean(c[r]["seeds"])["S1"] / c[r]["emp"]["S1"] for r in c}
        rows[panel] = dict(
            index={r: idx[r][0] for r in c}, blocks={r: idx[r][1] for r in c},
            r4m_minus_r4=dict(mean=idx["R4m"][0] - idx["R4"][0],
                              lo=float(np.percentile(d_m, 5)),
                              hi=float(np.percentile(d_m, 95))),
            r4_minus_r3=dict(mean=loss, lo=float(np.percentile(d_4, 5)),
                             hi=float(np.percentile(d_4, 95))),
            recovered_share=(idx["R4"][0] - idx["R4m"][0]) / loss if loss else None,
            top1_ratio=s1, mix_b=c["R4m"]["fit"]["mix_b"],
            temper_s=c["R4m"]["fit"]["temper_s"])
    (OUT / "summary.json").write_text(json.dumps(rows, indent=1))
    print(f"{'panel':26s} {'R3':>6s} {'R4':>6s} {'R4m':>6s}  R4m-R4 [5%,95%]        "
          f"recovered  b     top-1 R4 -> R4m")
    for p, r in rows.items():
        d = r["r4m_minus_r4"]
        rec = "n/a" if r["recovered_share"] is None else f"{100 * r['recovered_share']:.0f}%"
        print(f"{p:26s} {r['index']['R3']:6.2f} {r['index']['R4']:6.2f} "
              f"{r['index']['R4m']:6.2f}  {d['mean']:+.3f} [{d['lo']:+.3f}, {d['hi']:+.3f}]  "
              f"{rec:>8s}  {r['mix_b']:.2f}  {r['top1_ratio']['R4']:.2f} -> "
              f"{r['top1_ratio']['R4m']:.2f}")
        print(f"{'':26s} variance block: {r['blocks']['R3']['variance']:.2f} "
              f"{r['blocks']['R4']['variance']:.2f} {r['blocks']['R4m']['variance']:.2f}"
              f"   ladder block: {r['blocks']['R3']['ladder']:.2f} "
              f"{r['blocks']['R4']['ladder']:.2f} {r['blocks']['R4m']['ladder']:.2f}")


if __name__ == "__main__":
    {"run": run, "report": report}[sys.argv[1]]()
