#!/usr/bin/env python3
"""A9: does the lasting-variance profile predict the ladder's local slope?

Design and predictions: llm_fitting/runs/2026-10-12_A9_variance_ladder/DESIGN.md.
Data only, plus the frozen structure fit for one secondary column.

    python llm_fitting/phase1_a9.py run facebook_a
    python llm_fitting/phase1_a9.py report
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
import residual_anatomy as ra  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-12_A9_variance_ladder")
PANELS = ("facebook_a", "reddit_comments", "reddit_submissions_long", "reddit_comments_ext")
BOOT = 400


def _bins(K):
    edges = [float(K)]
    while edges[-1] / 2 >= 12:
        edges.append(edges[-1] / 2)
    out = [(edges[j + 1], edges[j]) for j in range(len(edges) - 1)] + [(0.0, edges[-1])]
    return out[::-1]                                  # head first


def _var_rate(d1, d2, h1, h2, rng):
    """Growth rate of the variance of h-week changes between two horizons,
    from per-endpoint sums, with an endpoint bootstrap."""
    def moments(d):
        ok = np.isfinite(d)
        return ok.sum(axis=0), np.where(ok, d, 0.0).sum(axis=0), np.where(ok, d * d, 0.0).sum(axis=0)

    def var(n, s, ss, idx):
        N = n[idx].sum()
        return ss[idx].sum() / N - (s[idx].sum() / N) ** 2

    m1, m2 = moments(d1), moments(d2)
    n_ent = d1.shape[1]
    full = np.arange(n_ent)
    rate = (var(*m2, full) - var(*m1, full)) / (h2 - h1)
    boots = []
    for _ in range(BOOT):
        idx = rng.integers(0, n_ent, n_ent)
        boots.append((var(*m2, idx) - var(*m1, idx)) / (h2 - h1))
    return float(rate), float(np.quantile(boots, 0.05)), float(np.quantile(boots, 0.95))


def _robust_rate(d1, d2, h1, h2):
    def mad2(d):
        f = d[np.isfinite(d)]
        return (ra.MAD_TO_SD * np.median(np.abs(f - np.median(f)))) ** 2
    return float((mad2(d2) - mad2(d1)) / (h2 - h1))


def _slope(ladder, lo, hi):
    r = np.arange(max(1, int(np.floor(lo)) + 1), int(np.floor(hi)) + 1)
    r = r[r <= ladder.size]
    if r.size < 3:
        return np.nan
    return float(-np.polyfit(np.log(r), ladder[r - 1], 1)[0])


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
    pr, pres = ra.perm_rank(R, 0, T, None)
    base = (pr <= K) & (pres >= 0.7)
    Y = ra.defactor(X, base)
    h1, h2 = (13, 52) if T >= 2.5 * 52 else (13, 26)
    D1, D2 = Y[h1:] - Y[:-h1], Y[h2:] - Y[:-h2]

    # ladders: the median over weeks at each rank; weekly ladders for spread; sorted endpoint means
    order = np.sort(np.where(np.isfinite(X), X, -np.inf), axis=1)[:, ::-1]
    order[~np.isfinite(order)] = np.nan
    L = int(np.isfinite(order).sum(axis=1).min())
    weekly = order[:, :L]
    median_ladder = np.nanmedian(weekly, axis=0)
    assert np.allclose(median_ladder, mrd.time_median_ladder(df, L))
    means = np.sort(np.nanmean(X[:, pres >= 0.5], axis=0))[::-1]

    # fitted lasting variance by permanent rank (secondary)
    zr = np.log(np.clip((np.arange(1, K + 1) - 0.5) / p.N, mrd.Z_CLIP, 1.0))
    sp2 = np.interp(zr, p.z_knots, p.sigma_perm) ** 2

    rows = []
    for lo, hi in _bins(K):
        cols = base & (pr > lo) & (pr <= hi)
        n = int(cols.sum())
        if n < 5:
            continue
        rate, q05, q95 = _var_rate(D1[:, cols], D2[:, cols], h1, h2, rng)
        wk = np.array([_slope(w, lo, hi) for w in weekly])
        s_med = _slope(median_ladder, lo, hi)
        rows.append(dict(
            lo=lo, hi=hi, mid=float(np.sqrt(max(lo, 1.0) * hi)), n=n,
            var_rate=rate, var_rate_q05=q05, var_rate_q95=q95,
            var_rate_robust=_robust_rate(D1[:, cols], D2[:, cols], h1, h2),
            sigma_perm2=float(np.mean(sp2[int(np.floor(lo)):int(np.floor(hi))])),
            slope=s_med, slope_q25=float(np.nanquantile(wk, 0.25)),
            slope_q75=float(np.nanquantile(wk, 0.75)), slope_means=_slope(means, lo, hi),
            rho=rate / (2 * s_med) if s_med > 0 else np.nan,
            rho_q05=q05 / (2 * s_med) if s_med > 0 else np.nan,
            rho_q95=q95 / (2 * s_med) if s_med > 0 else np.nan))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"a9__{panel}.json").write_text(json.dumps(dict(
        panel=panel, K=K, T=T, horizons=[h1, h2], ladder_len=L, n_base=int(base.sum()),
        bins=rows, commit=le._commit(), finished=time.strftime("%Y-%m-%d %H:%M:%S")),
        indent=1, default=float))
    print(f"[{panel}] {len(rows)} bins written", flush=True)


def report():
    from scipy.stats import spearmanr
    lines = []
    for panel in PANELS:
        f = OUT / "cells" / f"a9__{panel}.json"
        if not f.exists():
            continue
        c = json.loads(f.read_text())
        b = c["bins"]
        lines.append(f"\n## {panel} (K={c['K']}, T={c['T']}; variance rate from h={c['horizons'][0]} to {c['horizons'][1]})")
        lines.append("ranks            n   var/wk [5%, 95%]              robust   fitted s_perm^2   slope [IQR by week]     slope(means)   rho = var/(2 slope) [5%, 95%]   rho x mid-rank")
        for r in b:
            lines.append(
                f"{r['lo']:7.0f}-{r['hi']:<7.0f} {r['n']:5d}  {r['var_rate']:.5f} [{r['var_rate_q05']:.5f}, {r['var_rate_q95']:.5f}]  "
                f"{r['var_rate_robust']:.5f}   {r['sigma_perm2']:.5f}          {r['slope']:.3f} [{r['slope_q25']:.3f}, {r['slope_q75']:.3f}]    "
                f"{r['slope_means']:.3f}          {r['rho']:.5f} [{r['rho_q05']:.5f}, {r['rho_q95']:.5f}]      {r['rho'] * r['mid']:.3f}")
        body = [r for r in b[1:] if np.isfinite(r["rho"]) and r["rho"] > 0 and r["var_rate"] > 0]
        if len(body) >= 4:
            v = np.array([r["var_rate"] for r in body]); s = np.array([r["slope"] for r in body])
            rho = np.array([r["rho"] for r in body]); mid = np.array([r["mid"] for r in body])
            sm = np.array([r["slope_means"] for r in body]); fit = np.array([r["sigma_perm2"] for r in body])
            lines.append(
                f"top bin excluded ({len(body)} bins): Spearman(slope, var) {spearmanr(s, v)[0]:+.2f}; "
                f"log-log slope of slope on var {np.polyfit(np.log(v), np.log(s), 1)[0]:+.2f} (1 = one constant); "
                f"rho CV {rho.std(ddof=1) / rho.mean():.2f}, median {np.median(rho):.5f}; "
                f"log-log slope of rho on rank {np.polyfit(np.log(mid), np.log(rho), 1)[0]:+.2f} (0 = one constant)")
            lines.append(
                f"   with the endpoint-mean ladder: Spearman {spearmanr(sm, v)[0]:+.2f}, rho CV "
                f"{(v / (2 * sm)).std(ddof=1) / (v / (2 * sm)).mean():.2f}; with fitted s_perm^2: Spearman(slope, s_perm^2) "
                f"{spearmanr(s, fit)[0]:+.2f}, rho CV {(fit / (2 * s)).std(ddof=1) / (fit / (2 * s)).mean():.2f}; "
                f"var bottom octave over octave near K/16: {body[-1]['var_rate'] / body[-5]['var_rate'] if len(body) >= 5 else float('nan'):.2f}; "
                f"slope bottom over K/16: {body[-1]['slope'] / body[-5]['slope'] if len(body) >= 5 else float('nan'):.2f}")
    text = "\n".join(lines)
    (OUT / "report_tables.txt").write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    if sys.argv[1] == "run":
        run(sys.argv[2])
    else:
        report()
