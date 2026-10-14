"""How fast can a change in drift by rank be detected?  Design in runs/2026-10-14_drift_detection/DESIGN.md.

Drift of a rank stratum over a window [t, t + tau]: for a cohort fixed by permanent rank on the 26
weeks before the window, the mean change of the members' 4-week mean level from the window's start
to its end, per week, minus the same for the whole cohort (so a platform-wide move drops out).
Its sampling error comes from an endpoint bootstrap within the stratum.  The smallest change in
drift detectable at z = 3 after tau weeks, against a 26-week baseline, is
    MDE(tau) = 3 sqrt(SE(tau)^2 + SE(26)^2).
The tilt (fitted bottom minus top stratum, as in 2z-cc) is treated the same way.

    python llm_fitting/mts_drift_detect.py run <panel>
    python llm_fitting/mts_drift_detect.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402
from mts_balance_law import _assign, strata  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-14_drift_detection")
PANELS = ("reddit_comments_ext", "reddit_submissions_long")
TAUS = (4, 8, 13, 26)
SEL = 26          # weeks over which the cohort is fixed, just before the window
SMOOTH = 4        # levels are 4-week means
BOOT = 200
EVENT = {"reddit_submissions_long": "2021-06-21", "reddit_comments_ext": "2021-06-21"}


def short_means(X, w=SMOOTH):
    """Mean of the last w weeks, where at least w - 1 of them are present; NaN otherwise."""
    T, N = X.shape
    out = np.full((T, N), np.nan)
    fin = np.isfinite(X)
    Z = np.where(fin, X, 0.0)
    cs = np.vstack([np.zeros((1, N)), np.cumsum(Z, axis=0)])
    cn = np.vstack([np.zeros((1, N)), np.cumsum(fin, axis=0)])
    for t in range(w - 1, T):
        n = cn[t + 1] - cn[t + 1 - w]
        s = cs[t + 1] - cs[t + 1 - w]
        out[t] = np.where(n >= w - 1, s / np.maximum(n, 1), np.nan)
    return out


def cohort(R, floors, t0, K, st):
    pr, pres = ba.perm_rank(R, t0 - SEL, t0, floors)
    return _assign(pr, pres, st)


def window(Mw, which, st, t0, tau, K, rng, boot=BOOT):
    """Relative drift by stratum and the tilt over [t0, t0 + tau], with bootstrap draws."""
    d = (Mw[t0 + tau] - Mw[t0]) / tau
    all_js = list(range(len(st)))                                        # (0, K/128], (K/128, K/64] ... (K, 2K]
    xs = [d[(which == j) & np.isfinite(d)] for j in all_js]
    keep = [i for i, x in enumerate(xs) if x.size >= 10]                 # a stratum with under ten members is left out
    if len(keep) < 4:
        return None
    js, xs = [all_js[i] for i in keep], [xs[i] for i in keep]
    ns = np.array([x.size for x in xs], float)
    means = np.array([x.mean() for x in xs])
    bm = np.stack([x[rng.integers(0, x.size, (boot, x.size))].mean(axis=1) for x in xs], axis=1)
    pooled = (means * ns).sum() / ns.sum()
    bpool = (bm * ns).sum(axis=1) / ns.sum()
    rel, brel = means - pooled, bm - bpool[:, None]
    lx = np.log2([st[j][1] for j in js])                                 # the top stratum sits at its upper edge, K/128
    w = 1.0 / np.maximum(brel.var(axis=0), 1e-18)
    lxc = lx - (w * lx).sum() / w.sum()
    span = lx.max() - lx.min()
    fit = lambda y: (w * lxc * y).sum() / (w * lxc * lxc).sum() * span   # noqa: E731
    head = [i for i, j in enumerate(js) if st[j][1] <= K / 16 + 1e-9]   # ranks 1 to K/16
    hw = ns[head]
    full = lambda v: np.array([v[keep.index(i)] if i in keep else np.nan for i in range(len(all_js))])  # noqa: E731
    return dict(rel=full(rel), rel_se=full(brel.std(axis=0)), n=full(ns),
                tilt=float(fit(rel)), tilt_se=float(np.std([fit(brel[b]) for b in range(boot)])),
                head=float((rel[head] * hw).sum() / hw.sum()),
                head_se=float((brel[:, head] * hw).sum(axis=1).std() / hw.sum()))


def analyse(df, K, seed=0):
    X, R, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).tolist()
    Mw = short_means(X)
    st = strata(K)
    rng = np.random.default_rng(seed)
    out = dict(K=int(K), T=int(T), strata=[[float(lo), float(hi)] for lo, hi in st], taus=list(TAUS),
               first_week=dates[0], last_week=dates[-1])
    # 1. sampling error as a function of window length, over many start weeks
    se = {tau: [] for tau in TAUS}
    for t0 in range(SEL + SMOOTH, T - max(TAUS), 4):
        which = cohort(R, floors, t0 - SMOOTH + 1, K, st)                   # fixed before the first 4-week mean
        for tau in TAUS:
            w_ = window(Mw, which, st, t0, tau, K, rng)
            if w_ is not None:
                se[tau].append(dict(t0=t0, rel_se=w_["rel_se"].tolist(), tilt_se=w_["tilt_se"], head_se=w_["head_se"],
                                    n=w_["n"].tolist(), rel=w_["rel"].tolist(), tilt=w_["tilt"], head=w_["head"]))
    med = {tau: dict(rel_se=np.nanmedian(np.array([x["rel_se"] for x in se[tau]], float), axis=0).tolist(),
                     tilt_se=float(np.median([x["tilt_se"] for x in se[tau]])),
                     head_se=float(np.median([x["head_se"] for x in se[tau]])),
                     n=np.nanmedian(np.array([x["n"] for x in se[tau]], float), axis=0).tolist()) for tau in TAUS}
    out["median_se"] = {str(t): v for t, v in med.items()}
    base = med[26]
    out["mde"] = {str(tau): dict(rel=[float(3 * np.sqrt(a ** 2 + b ** 2)) for a, b in zip(med[tau]["rel_se"], base["rel_se"])],
                                 tilt=float(3 * np.sqrt(med[tau]["tilt_se"] ** 2 + base["tilt_se"] ** 2)),
                                 head=float(3 * np.sqrt(med[tau]["head_se"] ** 2 + base["head_se"] ** 2))) for tau in TAUS}
    # how much real drift by rank moves: the head's relative drift and the tilt, in successive
    # non-overlapping 26-week windows
    seq = [x for x in se[26] if (x["t0"] - (SEL + SMOOTH)) % 26 == 0]
    out["realised_26wk"] = [dict(start=dates[x["t0"]], head=x["head"], head_se=x["head_se"], tilt=x["tilt"], tilt_se=x["tilt_se"])
                            for x in seq]
    # 2. the June 2021 event: cohort fixed on the 26 weeks before the baseline; baseline = 26 weeks
    # before the event; trailing post windows of 4 and 8 weeks
    out["event"] = None
    return out, dict(X=X, R=R, floors=floors, Mw=Mw, st=st, dates=dates, rng=rng)


def event_study(state, K, event_date, taus=(4, 8)):
    Mw, R, floors, st, dates, rng = state["Mw"], state["R"], state["floors"], state["st"], state["dates"], state["rng"]
    te = min(range(len(dates)), key=lambda i: abs(np.datetime64(dates[i][:10]) - np.datetime64(event_date)))
    t_base = te - 26
    which = cohort(R, floors, t_base - SMOOTH + 1, K, st)
    b = window(Mw, which, st, t_base, 26, K, rng)
    if b is None:
        return dict(event_date=event_date, not_adjudicated="the baseline window has too few strata")
    rows = []
    for tau in taus:
        for t_end in range(te - 8, min(te + 27, Mw.shape[0])):
            t0 = t_end - tau
            if t0 < t_base + 26 - tau - 8:
                continue
            w_ = window(Mw, which, st, t0, tau, K, rng)
            if w_ is None:
                continue
            z_head = (w_["head"] - b["head"]) / np.sqrt(w_["head_se"] ** 2 + b["head_se"] ** 2)
            z_tilt = (w_["tilt"] - b["tilt"]) / np.sqrt(w_["tilt_se"] ** 2 + b["tilt_se"] ** 2)
            rows.append(dict(tau=tau, window_end=dates[t_end], weeks_after=int(t_end - te), head=w_["head"], head_base=b["head"],
                             z_head=float(z_head), tilt=w_["tilt"], z_tilt=float(z_tilt)))
    det = {}
    for tau in taus:
        r = [x for x in rows if x["tau"] == tau and x["weeks_after"] >= 0]
        hit = next((x["weeks_after"] for x, y in zip(r, r[1:]) if x["z_head"] <= -3 and y["z_head"] <= -3), None)
        pre = [x for x in rows if x["tau"] == tau and x["weeks_after"] < 0]
        det[str(tau)] = dict(head_detected_weeks_after=hit, pre_event_max_abs_z=float(max((abs(x["z_head"]) for x in pre), default=np.nan)))
    return dict(event_date=event_date, event_week=dates[te], baseline=dict(head=b["head"], head_se=b["head_se"], tilt=b["tilt"]),
                rows=rows, detection=det)


def run(panel):
    import minimal_rankdiff as mrd
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    out, state = analyse(df, cfg["K"])
    out["panel"] = panel
    out["event"] = event_study(state, cfg["K"], EVENT[panel])
    out["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"detect__{panel}.json").write_text(json.dumps(out, indent=1, default=float))
    print(f"[{panel}] T={out['T']} written", flush=True)


def report():
    lines = []
    for panel in PANELS:
        f = OUT / "cells" / f"detect__{panel}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        K = r["K"]
        lines += [f"\n## {panel} (K = {K}), weekly data {r['first_week'][:10]} to {r['last_week'][:10]}", "",
                  "Smallest change in a stratum's relative drift detectable at z = 3 against a 26-week baseline, per 1,000 weeks:", "",
                  "| stratum of permanent rank | members | " + " | ".join(f"after {t} weeks" for t in r["taus"]) + " |",
                  "|---|---|" + "---|" * len(r["taus"])]
        for i, (lo, hi) in enumerate(r["strata"]):
            n = r["median_se"]["26"]["n"][i]
            lines.append(f"| ({lo:.0f}, {hi:.0f}] | {n:.0f} | " + " | ".join(f"{1000 * r['mde'][str(t)]['rel'][i]:.1f}" for t in r["taus"]) + " |")
        lines.append("| head, ranks 1 to K/16 pooled | | " + " | ".join(f"{1000 * r['mde'][str(t)]['head']:.1f}" for t in r["taus"]) + " |")
        lines.append("| tilt, bottom minus top | | " + " | ".join(f"{1000 * r['mde'][str(t)]['tilt']:.1f}" for t in r["taus"]) + " |")
        lines += ["", "Realised drift in successive 26-week windows (head's relative drift and tilt, per 1,000 weeks, ± 1 se):", ""]
        for x in r["realised_26wk"]:
            lines.append(f"- from {x['start'][:10]}: head {1000 * x['head']:+.1f} ± {1000 * x['head_se']:.1f}; tilt {1000 * x['tilt']:+.1f} ± {1000 * x['tilt_se']:.1f}")
        e = r["event"]
        lines += ["", f"Event {e['event_date']} (week of {e['event_week'][:10]}): baseline head drift {1000 * e['baseline']['head']:+.1f} per 1,000 weeks.",
                  "Detection (two consecutive trailing windows with z ≤ −3 for the head): " +
                  "; ".join(f"{t}-week windows: {d['head_detected_weeks_after']} weeks after (largest |z| before: {d['pre_event_max_abs_z']:.1f})"
                            for t, d in e["detection"].items()), "",
                  "| window | ends | weeks after | head drift (per 1,000 wk) | z, head | z, tilt |", "|---|---|---|---|---|---|"]
        for x in e["rows"]:
            if x["tau"] == 4 and -4 <= x["weeks_after"] <= 16 or x["tau"] == 8 and 0 <= x["weeks_after"] <= 16 and x["weeks_after"] % 2 == 0:
                lines.append(f"| {x['tau']} wk | {x['window_end'][:10]} | {x['weeks_after']} | {1000 * x['head']:+.1f} | {x['z_head']:+.1f} | {x['z_tilt']:+.1f} |")
    (OUT / "report_tables.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) == 2 and args[1] in PANELS:
        run(args[1])
    elif args[:1] == ["report"]:
        report()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
