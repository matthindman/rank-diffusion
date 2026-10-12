#!/usr/bin/env python3
"""A3 of the 2026-10-12 work plan: what makes the simulated top too heavy.

Interventions in a fitted model, not refits: one thing is switched at a
time in the frozen structure stack of each panel and the head statistics
are recomputed over 20 seeds.  Design and predictions:
llm_fitting/runs/2026-10-12_A3_head_share/DESIGN.md.

    python llm_fitting/phase1_a3.py run facebook_a
    python llm_fitting/phase1_a3.py run_b facebook_a     # addendum: far tails, bounded shocks
    python llm_fitting/phase1_a3.py report
"""
import contextlib
import json
import sys
import time
from dataclasses import replace
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ladder_explainer as le  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import residual_anatomy as ra  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-12_A3_head_share")
PANELS = ("facebook_a", "instagram", "reddit_comments", "reddit_submissions_long")
SEEDS = 20
ARMS = ("base", "gauss", "no_amp", "no_passing", "frozen_homes",
        "seed_ladder", "seed_means", "seed_means_gauss")


def head_stats(ranksize, M):
    """Head statistics from a (weeks x ranks) array of sorted log values."""
    rs = ranksize[:, :M]
    act = np.expm1(rs)
    tot = np.nansum(act, axis=1)
    ok = tot > 0
    s1 = act[ok, 0] / tot[ok]
    top = np.sort(s1)[::-1][: max(1, int(round(0.05 * s1.size)))]
    return dict(S1=float(s1.mean()),
                S10=float(np.mean(np.nansum(act[ok, :10], axis=1) / tot[ok])),
                S100=float(np.mean(np.nansum(act[ok, :100], axis=1) / tot[ok])),
                gap12=float(np.nanmean(rs[:, 0] - rs[:, 1])),
                gap1_10=float(np.nanmean(rs[:, 0] - rs[:, 9])),
                gap10_100=float(np.nanmean(rs[:, 9] - rs[:, 99])),
                S1_first_week=float(s1[0]),
                S1_top5pct_weeks=float(top.sum() / s1.sum()))


def seed_ladders(df, n):
    """Two alternatives to seeding the homes from the first week's sorted
    observed values.

    ladder  for each rank, the median over weeks of the value observed at
            that rank: removes the first week's idiosyncrasy, keeps the
            noise that is in every observed ladder.
    means   each endpoint's mean over the weeks it is observed (endpoints
            present at least half the time), sorted: the ladder of lasting
            levels, without the weekly noise.  Ranks beyond the eligible
            endpoints are filled from `ladder`.
    """
    wide = df.pivot(index="period", columns="entity_id", values="X")
    V = -np.sort(-np.nan_to_num(wide.to_numpy(), nan=-np.inf), axis=1)
    V[~np.isfinite(V)] = np.nan
    L = min(n, int(np.isfinite(V).sum(axis=1).min()))
    ladder = np.nanmedian(V[:, :L], axis=0)
    pres = wide.notna().mean(axis=0)
    means = np.sort(wide.loc[:, pres >= 0.5].mean(axis=0).to_numpy())[::-1]
    k = min(means.size, L)
    filled = ladder.copy()
    filled[:k] = means[:k]
    # keep the ladder decreasing where the two pieces meet
    filled = np.minimum.accumulate(filled)
    return ladder, filled


def arm_params(p, arm, ladders):
    zeros = np.zeros_like(np.asarray(p.sigma_trans, dtype=float))
    if arm == "base":
        return p
    if arm == "gauss":
        return replace(p, t_df=float("inf"))
    if arm == "no_amp":
        return replace(p, temper_s=0.0)
    if arm == "no_passing":
        kw = dict(sigma_trans=zeros)
        if getattr(p, "sigma_trans2", None) is not None:
            kw["sigma_trans2"] = zeros.copy()
        return replace(p, **kw)
    if arm == "frozen_homes":
        return replace(p, sigma_perm=zeros)
    if arm == "seed_ladder":
        return replace(p, w0=ladders[0])
    if arm == "seed_means":
        return replace(p, w0=ladders[1])
    if arm == "seed_means_gauss":
        return replace(p, w0=ladders[1], t_df=float("inf"))
    raise ValueError(arm)


def run(panel):
    cfg = le.PANELS[panel]
    spec = le.spec_for(panel, "struct", cfg["struct_path"][-1])
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                               cfg["K"], buffer_mult=4)
    K, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    p = le._fit(df, spec)
    ers = mrd.empirical_structures(df, max(10, int(round(0.01 * K))), topid_k=K)[3]
    M = int(min(ers.shape[1], min(p.N, 2000)))
    emp = head_stats(ers, M)
    ladders = seed_ladders(df, int(np.asarray(p.w0).size))
    first = np.asarray(p.w0, dtype=float)
    info = dict(w0_top=[float(x) for x in first[:5]],
                ladder_top=[float(x) for x in ladders[0][:5]],
                means_top=[float(x) for x in ladders[1][:5]],
                sd_top100=dict(first=float(np.std(first[:100])),
                               ladder=float(np.std(ladders[0][:100])),
                               means=float(np.std(ladders[1][:100]))))
    print(f"[{panel}] K={K} T={T} M={M} data S1={emp['S1']:.4f}", flush=True)
    arms, t0 = {}, time.time()
    for arm in ARMS:
        pa = arm_params(p, arm, ladders)
        rows = []
        for s in range(SEEDS):
            sim = mrd.simulate(pa, T, seed=s, kappa=None, top_record=K)
            rows.append(head_stats(sim["ranksize"], M))
        arms[arm] = rows
        print(f"    {arm:18s} S1 {np.mean([r['S1'] for r in rows]):.4f} "
              f"({time.time() - t0:.0f}s)", flush=True)
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"a3__{panel}.json").write_text(json.dumps(dict(
        panel=panel, K=K, T=T, M=M, spec=spec, seeds=SEEDS, emp=emp, arms=arms,
        seeding=info, t_df=float(p.t_df), temper_s=float(p.temper_s),
        mix_b=float(getattr(p, "mix_b", 0.0)), commit=le._commit(),
        finished=time.strftime("%Y-%m-%d %H:%M:%S")), indent=1, default=float))


# --------------------------------------------------------------------------- #
# Addendum (DESIGN.md): the far tail of the passing shocks
# --------------------------------------------------------------------------- #
BOUNDS = (12.0, 8.0, 6.0)


@contextlib.contextmanager
def bounded_t(c):
    """Passing shocks drawn as before and clipped at +-c innovation SDs, then
    rescaled to unit variance.  Same random stream as the unbounded run, so a
    bounded arm differs from the base arm only where a draw exceeded c."""
    from scipy import integrate, stats
    orig, cache = mrd._tdraw, {}

    def unit_sd(df):
        if df not in cache:
            b = c * np.sqrt(df / (df - 2.0))
            var, _ = integrate.quad(lambda x: x * x * stats.t.pdf(x, df), -b, b)
            cache[df] = float(np.sqrt(var + 2.0 * b * b * stats.t.sf(b, df)))
        return cache[df]

    def draw(rng, df, n):
        if not np.isfinite(df):
            return orig(rng, df, n)
        b = c * np.sqrt(df / (df - 2.0))
        return np.clip(rng.standard_t(df, n), -b, b) / unit_sd(df)

    mrd._tdraw = draw
    try:
        yield
    finally:
        mrd._tdraw = orig


def run_b(panel):
    cfg = le.PANELS[panel]
    spec = le.spec_for(panel, "struct", cfg["struct_path"][-1])
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                               cfg["K"], buffer_mult=4)
    K, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    p = le._fit(df, spec)
    ers = mrd.empirical_structures(df, max(10, int(round(0.01 * K))), topid_k=K)[3]
    M = int(min(ers.shape[1], min(p.N, 2000)))
    emp = head_stats(ers, M)
    emp_far = ra.far_tail(*ra.from_long(df)[:2], K)
    arms, far, t0 = {"base": []}, [], time.time()
    for s in range(SEEDS):
        sim = mrd.simulate(p, T, seed=s, kappa=None, top_record=K, track=p.N,
                           track_ids=True)
        arms["base"].append(head_stats(sim["ranksize"], M))
        far.append(ra.far_tail(*ra.from_sim(sim), K))
    print(f"[{panel}] base S1 {np.mean([r['S1'] for r in arms['base']]):.4f} "
          f"(data {emp['S1']:.4f}) ({time.time() - t0:.0f}s)", flush=True)
    for c in BOUNDS:
        name = f"bound{c:g}"
        with bounded_t(c):
            arms[name] = [head_stats(mrd.simulate(p, T, seed=s, kappa=None,
                                                  top_record=K)["ranksize"], M)
                          for s in range(SEEDS)]
        print(f"    {name:10s} S1 {np.mean([r['S1'] for r in arms[name]]):.4f} "
              f"({time.time() - t0:.0f}s)", flush=True)
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"a3b__{panel}.json").write_text(json.dumps(dict(
        panel=panel, K=K, T=T, M=M, seeds=SEEDS, emp=emp, arms=arms,
        emp_far=emp_far, sim_far=far, t_df=float(p.t_df), commit=le._commit(),
        finished=time.strftime("%Y-%m-%d %H:%M:%S")), indent=1, default=float))


def report_b(lines):
    for panel in PANELS:
        f, f0 = OUT / "cells" / f"a3b__{panel}.json", OUT / "cells" / f"a3__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        e = c["emp"]
        base = np.array([r["S1"] for r in c["arms"]["base"]])
        gauss = (np.mean([r["S1"] for r in json.loads(f0.read_text())["arms"]["gauss"]])
                 if f0.exists() else np.nan)
        lines.append(f"\n## bounded passing shocks, {panel} (t df {c['t_df']:.1f}; "
                     f"data S1 {e['S1']:.4f}, Gaussian arm {gauss:.4f})")
        for arm, rows in c["arms"].items():
            v = np.array([r["S1"] for r in rows])
            d = v - base
            lines.append(
                f"{arm:10s} S1 {v.mean():.4f} ({v.std(ddof=1):.4f})  /data {v.mean() / e['S1']:5.2f}  "
                f"paired diff {d.mean():+.4f} ({d.std(ddof=1):.4f})  "
                f"share of the Gaussian arm's change {100 * d.mean() / (gauss - base.mean()):5.0f}%  "
                f"top-5%-weeks {np.mean([r['S1_top5pct_weeks'] for r in rows]):.3f} "
                f"(data {e['S1_top5pct_weeks']:.3f})")
        lines.append("far tail of one-week changes: data / sim median over seeds [sim min, max]")
        for er in c["emp_far"]:
            sims = [next((r for r in s if r["group"] == er["group"]), None) for s in c["sim_far"]]
            sims = [r for r in sims if r]
            if not sims:
                continue
            cells = []
            for key in ("z_q999_up", "z_q9999_up", "z_max_up", "z_max_dn",
                        "raw_q999_up", "raw_max_up", "beyond6", "beyond10"):
                sv = np.array([r[key] for r in sims])
                fmt = "{:.2e}" if key.startswith("beyond") else "{:.2f}"
                cells.append(f"{key} " + fmt.format(er[key]) + "/" + fmt.format(np.median(sv))
                             + " [" + fmt.format(sv.min()) + ", " + fmt.format(sv.max()) + "]")
            lines.append(f"  {er['group']:8s} (n {er['n']:,})  " + "  ".join(cells))


def report():
    lines = []
    for panel in PANELS:
        f = OUT / "cells" / f"a3__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        e = c["emp"]
        base = np.array([r["S1"] for r in c["arms"]["base"]])
        lines.append(f"\n## {panel} (K={c['K']}, T={c['T']}, top-{c['M']}, {c['seeds']} seeds; "
                     f"t df {c['t_df']:.1f}, s {c['temper_s']:.2f}, b {c['mix_b']:.2f})")
        lines.append(f"data: S1 {e['S1']:.4f} (first week {e['S1_first_week']:.4f}), "
                     f"S10 {e['S10']:.3f}, S100 {e['S100']:.3f}, gap 1-2 {e['gap12']:.3f}, "
                     f"gap 1-10 {e['gap1_10']:.3f}, gap 10-100 {e['gap10_100']:.3f}, "
                     f"top-5%-weeks share of S1 {e['S1_top5pct_weeks']:.3f}")
        s = c["seeding"]
        lines.append(f"seed ladders, SD of the top 100 log values: first week "
                     f"{s['sd_top100']['first']:.3f}, time-median ladder "
                     f"{s['sd_top100']['ladder']:.3f}, endpoint means {s['sd_top100']['means']:.3f}")
        lines.append("arm                 S1 mean (SD)     /data   paired diff vs base (SD)  "
                     "excess removed   S10/data  S100/data  gap 1-2   top-5%-weeks")
        for arm, rows in c["arms"].items():
            v = np.array([r["S1"] for r in rows])
            d = v - base
            excess = base.mean() - e["S1"]
            rem = (-d.mean() / excess) if abs(excess) > 1e-12 else np.nan
            lines.append(
                f"{arm:18s}  {v.mean():.4f} ({v.std(ddof=1):.4f})  {v.mean() / e['S1']:5.2f}   "
                f"{d.mean():+.4f} ({d.std(ddof=1):.4f})        {100 * rem:6.0f}%        "
                f"{np.mean([r['S10'] for r in rows]) / e['S10']:5.2f}    "
                f"{np.mean([r['S100'] for r in rows]) / e['S100']:5.2f}    "
                f"{np.mean([r['gap12'] for r in rows]):.3f}    "
                f"{np.mean([r['S1_top5pct_weeks'] for r in rows]):.3f}")
    report_b(lines)
    text = "\n".join(lines)
    (OUT / "report_tables.txt").write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    if sys.argv[1] == "run":
        run(sys.argv[2])
    elif sys.argv[1] == "run_b":
        run_b(sys.argv[2])
    else:
        report()
