"""Post hoc (after the scored run): (1) the June 2021 event stratum by stratum, both signs; (2) a scan of the
whole panel: for every 4th week, the head's and the tilt's drift over the trailing 4 and 8 weeks against the
26 weeks before them, z by the same bootstrap.  How often, and when, does drift by rank change?

    PYTHONPATH=. python llm_fitting/runs/2026-10-14_drift_detection/scan_post_hoc.py
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np

warnings.filterwarnings("ignore")
sys.path.insert(0, "llm_fitting")
import minimal_rankdiff as mrd  # noqa: E402
import mts_drift_detect as dd  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path(__file__).resolve().parent
res = {}
for panel in ("reddit_submissions_long", "reddit_comments_ext"):
    cfg = m1.PANELS[panel]
    K = cfg["K"]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    X, R, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).tolist()
    Mw, st, rng = dd.short_means(X), dd.strata(K), np.random.default_rng(7)
    # (1) the event, stratum by stratum
    te = min(range(T), key=lambda i: abs(np.datetime64(dates[i][:10]) - np.datetime64("2021-06-21")))
    tb = te - 26
    which = dd.cohort(R, floors, tb - dd.SMOOTH + 1, K, st)
    base = dd.window(Mw, which, st, tb, 26, K, rng)
    ev = []
    for t_end in range(te - 4, te + 13):
        w = dd.window(Mw, which, st, t_end - 4, 4, K, rng)
        z = (w["rel"] - base["rel"]) / np.sqrt(w["rel_se"] ** 2 + base["rel_se"] ** 2)
        ev.append(dict(weeks_after=int(t_end - te), end=dates[t_end][:10], rel=(1000 * w["rel"]).round(1).tolist(), z=np.round(z, 1).tolist()))
    # (2) the scan
    scan = []
    for t_end in range(26 + 26 + 8 + dd.SMOOTH, T, 4):
        for tau in (4, 8):
            tb = t_end - tau - 26
            which = dd.cohort(R, floors, tb - dd.SMOOTH + 1, K, st)
            b = dd.window(Mw, which, st, tb, 26, K, rng)
            w = dd.window(Mw, which, st, t_end - tau, tau, K, rng)
            if b is None or w is None:
                continue
            scan.append(dict(tau=tau, end=dates[t_end][:10],
                             z_head=float((w["head"] - b["head"]) / np.hypot(w["head_se"], b["head_se"])),
                             z_tilt=float((w["tilt"] - b["tilt"]) / np.hypot(w["tilt_se"], b["tilt_se"])),
                             head=float(1000 * w["head"]), head_base=float(1000 * b["head"])))
    res[panel] = dict(strata=[[float(lo), float(hi)] for lo, hi in st], event=ev, scan=scan)
(OUT / "scan_post_hoc.json").write_text(json.dumps(res, indent=1))
for panel, r in res.items():
    print(f"\n{panel}: strata {[(int(lo), int(hi)) for lo, hi in r['strata']]}")
    print("  June 2021, 4-week windows, z by stratum (top first):")
    for e in r["event"]:
        print(f"    {e['end']} ({e['weeks_after']:+d}): " + " ".join(f"{z:+5.1f}" for z in e["z"]))
    for tau in (4, 8):
        s = [x for x in r["scan"] if x["tau"] == tau]
        fh = np.mean([abs(x["z_head"]) >= 3 for x in s]); ft = np.mean([abs(x["z_tilt"]) >= 3 for x in s])
        print(f"  scan, {tau}-week windows every 4 weeks: {len(s)} windows; |z| >= 3 for the head in {fh:.0%}, for the tilt in {ft:.0%}")
        print("    head z by window end: " + " ".join(f"{x['end'][2:7]}:{x['z_head']:+.0f}" for x in s))
