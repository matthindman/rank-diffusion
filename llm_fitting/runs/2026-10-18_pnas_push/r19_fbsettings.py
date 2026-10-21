"""R19 of PLAN.md (owner, 2026-10-21: "Yes, worth doing Reddit sims on FB settings"): Reddit at Facebook's settings and
dates, against Reddit's own homogeneous worlds.

The Reddit universe frame, and each world's frame, is cut to the 86 weeks starting at the week nearest 2020-12-28
(Facebook's first week) and renumbered from 0; `r18_facebook.fb_stats` is then applied unchanged, so every number is
Facebook's statistic computed by Facebook's code.  Worlds: the Reddit model fitted on the whole panel as R12 and R17
(`r12_hetgrowth.fit_model`, L0 from weeks < 30), the matched control (`control_num`) and the calibrated model
(`control`), simulated over the whole panel and cut to the same window.

    python r19_fbsettings.py data <panel>                      fb_stats on the cut data frame; writes the window
    python r19_fbsettings.py boot <panel>                      200 bootstrap draws (as R18-A1)
    python r19_fbsettings.py worlds <panel> <control|control_num> <s0> <s1>
    python r19_fbsettings.py summ <panel>

Set $R12_CACHE (the lean universe cache).
"""
import gc
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import mts_step1 as m1  # noqa: E402
import r12_hetgrowth as h  # noqa: E402
import r17_round8 as r17  # noqa: E402
import r18_facebook as fb  # noqa: E402

FB_START, T_W = pd.Timestamp("2020-12-28"), 86


def cut(fr, t0):
    """The frame's weeks t0 .. t0 + 85, renumbered from 0."""
    c = fr[(fr["period"] >= t0) & (fr["period"] < t0 + T_W)].copy()
    c["period"] = c["period"] - t0
    return c


def _window(pn):
    return json.loads((HERE / f"r19_data__{pn}.json").read_text())["t0"]


def mode_data(pn):
    df, K = h.load(pn)
    wk = df.groupby("period")["ts"].first()
    t0 = int(wk.index[np.argmin(np.abs((wk - FB_START).dt.days.to_numpy()))])
    c = cut(df, t0)
    del df
    gc.collect()
    out = dict(panel=pn, K=K, t0=t0, start=str(wk.loc[t0].date()), T=T_W, **fb.fb_stats(c, K, T_W))
    (HERE / f"r19_data__{pn}.json").write_text(json.dumps(out, indent=1, default=float))
    print(pn, "window from", out["start"], "(period", t0, ")", json.dumps({k: out[k] for k in ("turnover", "gapped", "boundary_pooled", "head_share")}, default=float),
          "D_FB", round(out["D_FB"]["D"], 3), flush=True)


def mode_boot(pn, n=200):
    """As r18_facebook.mode_boot: the cohort resampled for persistence, units within strata for D_FB."""
    df, K = h.load(pn)
    c = cut(df, _window(pn))
    del df
    gc.collect()
    X, R, floors, _, _ = m1.matrices(c, K)
    T = T_W
    rng = np.random.default_rng(1919)
    coh = fb._order(fb.wmean(X, 0, fb.SEL))[:K]
    pairs_g = {}
    for g in fb.GAPS:
        lst, a = [], fb.SEL
        while a + 2 * fb.H + 2 * fb.W + g <= T:
            x = fb.wmean(X, a + fb.H, a + fb.H + fb.W) - fb.wmean(X, a, a + fb.W)
            b0 = a + fb.H + fb.W + g
            lst.append((x, fb.wmean(X, b0 + fb.H, b0 + fb.H + fb.W) - fb.wmean(X, b0, b0 + fb.W)))
            a += fb.STEP
        pairs_g[g] = lst
    lab = h.strata(R, floors, K)
    pairs_d, a = [], fb.SEL
    while a + 2 * fb.H + 2 * fb.W + fb.G_D <= T:
        x = fb.wmean(X, a + fb.H, a + fb.H + fb.W) - fb.wmean(X, a, a + fb.W)
        b0 = a + fb.H + fb.W + fb.G_D
        pairs_d.append((x, fb.wmean(X, b0 + fb.H, b0 + fb.H + fb.W) - fb.wmean(X, b0, b0 + fb.W)))
        a += fb.STEP
    members = [np.flatnonzero(lab == j) for j in range(3)]

    def gapped_of(idx):
        out = {}
        for g, lst in pairs_g.items():
            vals = []
            for x, y in lst:
                xs, ys = x[idx], y[idx]
                ok = np.isfinite(xs) & np.isfinite(ys)
                vals.append(float(spearmanr(xs[ok], ys[ok]).correlation))
            out[str(g)] = float(np.mean(vals))
        return out

    def d_of(groups):
        Ds = []
        for x, y in pairs_d:
            xs, ys = [], []
            for gi in groups:
                xg, yg = x[gi], y[gi]
                ok = np.isfinite(xg) & np.isfinite(yg)
                xs.append(xg[ok] - xg[ok].mean())
                ys.append(yg[ok] - yg[ok].mean())
            xd, yd = np.concatenate(xs), np.concatenate(ys)
            f, r = r17.tail_ratios(xd, yd, np.ones(xd.size, bool))
            Ds.append(f - r)
        return float(np.mean(Ds))

    point = dict(gapped=gapped_of(coh), D_FB=d_of(members))
    draws_g, draws_d = [], []
    for _ in range(n):
        draws_g.append(gapped_of(rng.choice(coh, coh.size, replace=True)))
        draws_d.append(d_of([rng.choice(mm, mm.size, replace=True) for mm in members]))
    q = lambda v: [float(np.quantile(v, 0.05)), float(np.quantile(v, 0.5)), float(np.quantile(v, 0.95))]
    out = dict(panel=pn, n=n, point=point, gapped_band={g: q([d[g] for d in draws_g]) for g in point["gapped"]}, D_FB_band=q(draws_d))
    (HERE / f"r19_boot__{pn}.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out), flush=True)


def mode_worlds(pn, variant, s0, s1):
    import rankdiff_kalman as rk
    t0 = _window(pn)
    df, K = h.load(pn)
    T = int(df["period"].max()) + 1
    p, base, ladder = h.fit_model(df, K, pn)
    L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
    q = h.het_params(p, ladder, variant)
    del df
    gc.collect()
    for seed in range(s0, s1 + 1):
        fr, _, _ = h.sim_frame(q, L0, T, seed, K)
        c = cut(fr, t0)
        del fr
        out = dict(panel=pn, world=variant, seed=seed, t0=t0, **fb.fb_stats(c, K, T_W))
        (HERE / f"r19_world__{pn}__{variant}__s{seed}.json").write_text(json.dumps(out, indent=1, default=float))
        print(f"{pn} {variant} seed {seed}: gapped {[round(v, 3) for v in out['gapped'].values()]}; D_FB {out['D_FB']['D']:+.3f}; "
              f"turnover {[round(v, 3) for v in out['turnover'].values()]}; boundary {out.get('boundary_pooled')}", flush=True)
        del c
        gc.collect()


def mode_summ(pn):
    d = json.loads((HERE / f"r19_data__{pn}.json").read_text())
    out = dict(data=d, worlds={})
    for variant in ("control_num", "control"):
        W = [json.loads(f.read_text()) for f in sorted(HERE.glob(f"r19_world__{pn}__{variant}__s*.json"))]
        if not W:
            continue
        s = dict(n=len(W))
        s["turnover"] = {k: fb._band([w["turnover"][k] for w in W]) for k in W[0]["turnover"]}
        s["occupant_change"] = {k: fb._band([w["occupant_change"][k] for w in W]) for k in W[0]["occupant_change"]}
        s["boundary_pooled"] = fb._band([w.get("boundary_pooled") for w in W])
        s["gapped"] = {k: fb._band([w["gapped"][k] for w in W]) for k in W[0]["gapped"]}
        s["D_FB"] = fb._band([w["D_FB"]["D"] for w in W])
        s["p_fall"] = fb._band([w["D_FB"]["p_fall"] for w in W])
        s["p_rise"] = fb._band([w["D_FB"]["p_rise"] for w in W])
        s["head_share"] = fb._band([w["head_share"] for w in W])        # a level sum: cite the median
        s["excess"] = {k: d["gapped"][k] - s["gapped"][k]["mean"] for k in d["gapped"]}
        out["worlds"][variant] = s
    (HERE / f"r19_summary__{pn}.json").write_text(json.dumps(out, indent=1, default=float))
    for v, s in out["worlds"].items():
        print(pn, v, "gapped", {k: round(b["mean"], 3) for k, b in s["gapped"].items()}, "excess", {k: round(x, 3) for k, x in s["excess"].items()},
              "D_FB", round(s["D_FB"]["mean"], 3), [round(s["D_FB"]["q05"], 3), round(s["D_FB"]["q95"], 3)], flush=True)
    print(pn, "data gapped", {k: round(x, 3) for k, x in d["gapped"].items()}, "D_FB", round(d["D_FB"]["D"], 3), flush=True)


if __name__ == "__main__":
    mode, pn = sys.argv[1], sys.argv[2]
    if mode == "data":
        mode_data(pn)
    elif mode == "boot":
        mode_boot(pn)
    elif mode == "worlds":
        mode_worlds(pn, sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))
    elif mode == "summ":
        mode_summ(pn)
    else:
        sys.exit(__doc__)
