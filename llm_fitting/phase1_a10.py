#!/usr/bin/env python3
"""A10: the variance-structure relation estimated both ways.

Design and predictions: llm_fitting/runs/2026-10-12_A10_two_way/DESIGN.md.
Data only, plus the frozen structure fit for one variance column.

    python llm_fitting/phase1_a10.py run facebook_a
    python llm_fitting/phase1_a10.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exit_audit as ea  # noqa: E402
import ladder_explainer as le  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import phase1_a9 as a9  # noqa: E402
import residual_anatomy as ra  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-12_A10_two_way")
PANELS = a9.PANELS
H1, H2 = 13, 26


def _ladders(X, pres):
    order = np.sort(np.where(np.isfinite(X), X, -np.inf), axis=1)[:, ::-1]
    order[~np.isfinite(order)] = np.nan
    L = int(np.isfinite(order).sum(axis=1).min())
    return np.nanmedian(order[:, :L], axis=0), np.sort(np.nanmean(X[:, pres >= 0.5], axis=0))[::-1]


def _cycle_variance(Y, cols):
    """Variance of an annual cycle from the covariance of 13-week block-mean
    changes one year apart (MODEL_STATUS 2z-aw addendum); nan if too short."""
    M = ra._block_means(Y[:, cols], 13)
    c = M[1:] - M[:-1]
    if c.shape[0] <= 4:
        return np.nan
    cnt = np.isfinite(c).sum(axis=1, keepdims=True)
    c = c - np.nansum(c, axis=1, keepdims=True) / np.maximum(cnt, 1)
    a, b = c[:-4].ravel(), c[4:].ravel()
    ok = np.isfinite(a) & np.isfinite(b)
    return float(np.mean(a[ok] * b[ok]) / 1.62)


def profile(R, X, K, bins, rng, fitted=None, N=None):
    """Per rank bin: the four variance estimates and the two ladder slopes."""
    T = R.shape[0]
    pr, pres = ra.perm_rank(R, 0, T, None)
    base = (pr <= K) & (pres >= 0.7)
    Y = ra.defactor(X, base)
    d0 = Y[1:] - Y[:-1]
    D1, D2 = Y[H1:] - Y[:-H1], Y[H2:] - Y[:-H2]
    med, means = _ladders(X, pres)
    S = {g: _cycle_variance(Y, m & base) for g, m in ra._groups(pr, K).items()}
    rows = []
    for lo, hi in bins:
        cols = base & (pr > lo) & (pr <= hi)
        if cols.sum() < 5:
            continue
        v2, q05, q95 = a9._var_rate(D1[:, cols], D2[:, cols], H1, H2, rng)
        g = "head" if hi <= 0.1 * K else ("middle" if hi <= 0.5 * K else "shell")
        s_cyc = S.get(g, np.nan)
        # the cycle adds 2S(1 - cos(2 pi h / 52)): 2S at 13 weeks, 4S at 26
        v3 = v2 - 2.0 * s_cyc / (H2 - H1) if np.isfinite(s_cyc) else np.nan
        f = d0[:, cols]
        row = dict(lo=lo, hi=hi, mid=float(np.sqrt(max(lo, 1.0) * hi)), n=int(cols.sum()),
                   V0=float(np.nanvar(f)), V2=v2, V2_q05=q05, V2_q95=q95, V3=v3,
                   slope=a9._slope(med, lo, hi), slope_means=a9._slope(means, lo, hi))
        if fitted is not None:
            zr = np.log(np.clip((np.arange(1, K + 1) - 0.5) / N, mrd.Z_CLIP, 1.0))
            sp2 = np.interp(zr, fitted.z_knots, fitted.sigma_perm) ** 2
            row["V1"] = float(np.mean(sp2[int(np.floor(lo)):int(np.floor(hi))]))
        rows.append(row)
    return rows, med


def _g(rows, key, slope="slope"):
    r = np.array([x[key] / (2 * x[slope]) for x in rows[1:]
                  if np.isfinite(x.get(key, np.nan)) and x[key] > 0 and x[slope] > 0])
    return float(np.median(r)), float(r.std(ddof=1) / r.mean()), r


def _grid(ladder, K):
    K = int(min(K, ladder.size))
    r = np.unique(np.round(np.exp(np.linspace(0, np.log(K), 200))).astype(int))
    return r, ladder[r - 1]


def predict_on_grid(rows, key, g, r, obs):
    """Integrate s_pred = V/(2g) in log rank upward from the observed value at
    the last grid rank (rank K); compare with the observed ladder on the grid."""
    use = [x for x in rows if np.isfinite(x.get(key, np.nan)) and x[key] > 0]
    mids = np.log([x["mid"] for x in use])
    spred = np.array([x[key] for x in use]) / (2 * g)
    r, obs = np.asarray(r), np.asarray(obs, dtype=float)
    lr = np.log(r)
    s_at = np.interp(lr, mids, spred)                       # flat beyond the end bins
    seg = 0.5 * (s_at[1:] + s_at[:-1]) * np.diff(lr)
    pred = obs[-1] + np.r_[np.cumsum(seg[::-1])[::-1], 0.0]
    err = pred - obs
    out = dict(rmse=float(np.sqrt(np.mean(err ** 2))), max_abs=float(np.max(np.abs(err))),
               err_rank1=float(err[0]), err_rank10=float(err[np.argmin(np.abs(r - 10))]))
    for name, deg in (("power_law", 1), ("quadratic", 2)):
        fit = np.polyval(np.polyfit(lr, obs, deg), lr)
        out[f"rmse_{name}"] = float(np.sqrt(np.mean((fit - obs) ** 2)))
    return out


def predict_ladder(rows, key, g, ladder, K):
    return predict_on_grid(rows, key, g, *_grid(ladder, K))


def run(panel):
    rng = np.random.default_rng(0)
    if panel in le.PANELS:
        cfg = le.PANELS[panel]
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]), cfg["K"], buffer_mult=4)
        p = le._fit(df, le.spec_for(panel, "struct", cfg["struct_path"][-1]))
    else:
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[panel]), 12_500, buffer_mult=4)
        p = mrd.estimate(df, **ea.LONG)
    K, T = int(df.attrs["score_k"]), int(df["period"].max()) + 1
    R, X, _ = ra.from_long(df)
    bins = a9._bins(K)
    rows, med = profile(R, X, K, bins, rng, fitted=p, N=p.N)
    gr, gobs = _grid(med, K)
    out = dict(panel=panel, K=K, T=T, bins=rows, g={}, ladder={},
               grid=dict(r=[int(x) for x in gr], obs=[float(x) for x in gobs]))
    for key in ("V0", "V1", "V2", "V3"):
        if not any(np.isfinite(x.get(key, np.nan)) for x in rows):
            continue
        g, cv, r = _g(rows, key)
        mid = np.array([x["mid"] for x in rows[1:] if np.isfinite(x.get(key, np.nan)) and x[key] > 0])
        out["g"][key] = dict(g=g, cv=cv, trend=float(np.polyfit(np.log(mid), np.log(r), 1)[0]))
        out["ladder"][key] = predict_ladder(rows, key, g, med, K)
    # structure -> variance, against V2
    g2 = out["g"]["V2"]["g"]
    body = rows[1:]
    imp = np.array([2 * g2 * x["slope"] for x in body])
    est = np.array([x["V2"] for x in body])
    ok = est > 0
    out["structure_to_variance"] = dict(
        rms_log_ratio=float(np.sqrt(np.mean(np.log(imp[ok] / est[ok]) ** 2))),
        inside_bracket=float(np.mean([(x["V2_q05"] <= i <= x["V2_q95"]) for x, i in zip(body, imp)])),
        by_bin=[dict(lo=x["lo"], hi=x["hi"], implied=float(i), estimated=x["V2"],
                     q05=x["V2_q05"], q95=x["V2_q95"]) for x, i in zip(body, imp)])
    # holdout in time: variance and g from the first half, ladder of the second half
    if T >= 200:
        h = T // 2
        r1, med1 = profile(R[:h], X[:h], K, bins, rng)
        r2, med2 = profile(R[h:], X[h:], K, bins, rng)
        g1, cv1, _ = _g(r1, "V2")
        g2h, cv2, _ = _g(r2, "V2")
        cross = [dict(x, slope=y["slope"]) for x, y in zip(r1, r2)]   # first-half variance, second-half slope
        out["time_holdout"] = dict(
            g_first=g1, g_second=g2h, cv_first=cv1, cv_second=cv2, cv_cross=_g(cross, "V2")[1],
            second_from_first=predict_ladder(r1, "V2", g1, med2, K),
            second_in_sample=predict_ladder(r2, "V2", g2h, med2, K),
            ladder_moved_rmse=float(np.sqrt(np.mean((med1[:K] - med2[:K] - (med1[K - 1] - med2[K - 1])) ** 2))))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"a10__{panel}.json").write_text(json.dumps(
        dict(out, commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")), indent=1, default=float))
    print(f"[{panel}] done", flush=True)


def report():
    cells = {p: json.loads((OUT / "cells" / f"a10__{p}.json").read_text())
             for p in PANELS if (OUT / "cells" / f"a10__{p}.json").exists()}
    lines = []
    for panel, c in cells.items():
        lines.append(f"\n## {panel} (K={c['K']}, T={c['T']}; variance growth from {H1} to {H2} weeks)")
        lines.append("variance estimate: g = median V/(2 slope); CV across bins; log-log trend of the ratio on rank | ladder: RMSE, largest error, error at rank 1, at rank 10 | power law RMSE, quadratic RMSE")
        for key, name in (("V0", "V0 one-week, no noise handling"), ("V1", "V1 fitted s_perm^2"),
                          ("V2", "V2 variance growth 13-26"), ("V3", "V3 V2 less the annual cycle")):
            if key not in c["g"]:
                continue
            g, L = c["g"][key], c["ladder"][key]
            lines.append(f"  {name:32s} g {g['g']:.5f}  CV {g['cv']:.2f}  trend {g['trend']:+.2f} | "
                         f"{L['rmse']:.3f}  {L['max_abs']:.3f}  {L['err_rank1']:+.3f}  {L['err_rank10']:+.3f} | "
                         f"{L['rmse_power_law']:.3f}  {L['rmse_quadratic']:.3f}")
        s = c["structure_to_variance"]
        lines.append(f"structure -> variance (against V2): RMS log ratio {s['rms_log_ratio']:.3f} "
                     f"(typical factor {np.exp(s['rms_log_ratio']):.2f}); implied inside the 5-95% bracket in "
                     f"{100 * s['inside_bracket']:.0f}% of bins")
        lines.append("  ranks: implied / estimated [5%, 95%]   " + "   ".join(
            f"{b['lo']:.0f}-{b['hi']:.0f}: {b['implied']:.4f}/{b['estimated']:.4f} [{b['q05']:.4f}, {b['q95']:.4f}]"
            for b in s["by_bin"]))
        if "time_holdout" in c:
            h = c["time_holdout"]
            lines.append(f"time holdout: g first half {h['g_first']:.5f}, second half {h['g_second']:.5f}; CV first "
                         f"{h['cv_first']:.2f}, second {h['cv_second']:.2f}, first-half variance over second-half slope "
                         f"{h['cv_cross']:.2f}; second-half ladder RMSE from first-half variance and g "
                         f"{h['second_from_first']['rmse']:.3f} (in sample {h['second_in_sample']['rmse']:.3f}; power law "
                         f"{h['second_in_sample']['rmse_power_law']:.3f}); the ladder itself moved by {h['ladder_moved_rmse']:.3f}")
    reddit = [p for p in cells if p.startswith("reddit")]
    if len(reddit) >= 2:
        lines.append("\n## across Reddit panels: each ladder predicted with another panel's g (V2)")
        from itertools import permutations
        for a, b in permutations(reddit, 2):
            ca, cb = cells[a], cells[b]
            other = predict_on_grid(cb["bins"], "V2", ca["g"]["V2"]["g"], cb["grid"]["r"], cb["grid"]["obs"])
            lines.append(f"  g from {a} ({ca['g']['V2']['g']:.5f}) on {b}: RMSE {other['rmse']:.3f} "
                         f"(own g {cb['g']['V2']['g']:.5f}: {cb['ladder']['V2']['rmse']:.3f}; power law "
                         f"{cb['ladder']['V2']['rmse_power_law']:.3f})")
    text = "\n".join(lines)
    (OUT / "report_tables.txt").write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    if sys.argv[1] == "run":
        run(sys.argv[2])
    else:
        report()
