#!/usr/bin/env python3
"""IG "a"-query censoring forensics (pre-registered: ig_censoring_prereg.md).

Scores predictions P1 (thinning noise law) and P2 (absence-is-thinning) on
the weekly top-50k IG panel.  Read-only; no model code touched.

Run:
  python -u llm_fitting/ig_censoring_forensics.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PATH = "llm_fitting/ig_weekly_ranked_top50k.parquet"


def main() -> None:
    df = pd.read_parquet(PATH, columns=["date", "user_name", "metric_value", "n_posts"])
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    week_idx = {w: k for k, w in enumerate(weeks)}
    df["t"] = df["date"].map(week_idx)
    T = len(weeks)
    print(f"panel: {len(df):,} rows, {df['user_name'].nunique():,} accounts, "
          f"T={T} weeks  [{pd.Timestamp(weeks[0]).date()} .. {pd.Timestamp(weeks[-1]).date()}]")

    # basic n_posts / metric facts
    m = df["n_posts"].to_numpy(float)
    y = df["metric_value"].to_numpy(float)
    print("\nn_posts distribution over account-weeks (present rows only):")
    qs = [5, 25, 50, 75, 90, 99]
    print("  quantiles", dict(zip(qs, np.percentile(m, qs))))
    print(f"  share M<=1: {(m <= 1).mean():.3f}   M<=3: {(m <= 3).mean():.3f}   "
          f"M>=20: {(m >= 20).mean():.3f}   M>=30: {(m >= 30).mean():.3f}")
    print(f"  metric_value==0 rows: {(y <= 0).mean():.4f}")

    # keep positive metric rows for log work
    ok = (y > 0) & (m > 0)
    d = df.loc[ok, ["user_name", "t", "metric_value", "n_posts"]].copy()
    d["logy"] = np.log(d["metric_value"].astype(float))

    # permanent-rank proxy for banding: mean log metric per account (level, not rank —
    # banding only, no estimation) + presence
    g = d.groupby("user_name", sort=False)
    acc = pd.DataFrame({
        "mlogy": g["logy"].mean(),
        "mposts": g["n_posts"].mean(),
        "npres": g["t"].size(),
    })
    acc["presfrac"] = acc["npres"] / T
    # rank bands by mean level (top-coverage flavor)
    acc["lvl_rank"] = acc["mlogy"].rank(ascending=False, method="first")

    # ---------------- P1: (dlog Y)^2 vs (1/M_t + 1/M_{t+1}) ----------------
    d = d.sort_values(["user_name", "t"])
    same = d["user_name"].to_numpy()
    tarr = d["t"].to_numpy()
    ly = d["logy"].to_numpy()
    mm = d["n_posts"].to_numpy(float)
    nxt = (same[1:] == same[:-1]) & (tarr[1:] == tarr[:-1] + 1)
    dly2 = (ly[1:][nxt] - ly[:-1][nxt]) ** 2
    hreg = 1.0 / mm[1:][nxt] + 1.0 / mm[:-1][nxt]
    uacc = same[:-1][nxt]
    print(f"\nP1: {len(dly2):,} consecutive present-week pairs")

    # pooled OLS with account demeaning is overkill; report pooled + banded binned means
    def ols(x, yv):
        x1 = np.column_stack([np.ones_like(x), x])
        beta, *_ = np.linalg.lstsq(x1, yv, rcond=None)
        return beta  # [intercept, slope]

    b0, b1 = ols(hreg, dly2)
    print(f"  pooled OLS: intercept={b0:.3f}  slope a={b1:.3f}")

    # robust check: binned means of dly2 by hreg decile
    bins = np.quantile(hreg, np.linspace(0, 1, 11))
    ib = np.clip(np.searchsorted(bins, hreg, side="right") - 1, 0, 9)
    print("  binned (hreg-decile): mean_h, mean_dlogy2")
    for k in range(10):
        s = ib == k
        print(f"    {hreg[s].mean():8.3f}  {dly2[s].mean():8.3f}   (n={s.sum():,})")

    # by level band (accounts bucketed by mean level)
    lvl = acc["lvl_rank"].reindex(uacc).to_numpy()
    print("  by account level band: [band] slope a, intercept, n")
    for name, lo, hi in [("top 1k", 0, 1000), ("1k-5k", 1000, 5000),
                          ("5k-20k", 5000, 20000), ("20k+", 20000, 10**9)]:
        s = (lvl > lo) & (lvl <= hi)
        if s.sum() > 500:
            c0, c1 = ols(hreg[s], dly2[s])
            print(f"    {name:8s} a={c1:6.3f}  intercept={c0:6.3f}  (n={s.sum():,})")

    # thinning share at low/high M
    for label, s in [("M<=3 both weeks", (mm[1:][nxt] <= 3) & (mm[:-1][nxt] <= 3)),
                     ("M>=30 both weeks", (mm[1:][nxt] >= 30) & (mm[:-1][nxt] >= 30))]:
        if s.sum() > 100:
            share = b1 * hreg[s].mean() / dly2[s].mean()
            print(f"  {label}: mean dlogy2={dly2[s].mean():.3f}, "
                  f"implied thinning share={share:.2f} (n={s.sum():,})")

    # ---------------- P2: absence hazard vs mean matching count ----------------
    # presence matrix approach: for each account, count present->absent transitions
    print("\nP2: absence hazard vs mean n_posts")
    d2 = df.loc[ok, ["user_name", "t"]].copy()
    d2 = d2.sort_values(["user_name", "t"])
    s2 = d2["user_name"].to_numpy()
    t2 = d2["t"].to_numpy()
    # a present row followed (same account) by gap>1 OR being last row before T-1
    # hazard = P(absent at t+1 | present at t), excluding t = T-1
    valid = t2 < T - 1
    nxt_same = np.zeros(len(t2), bool)
    nxt_same[:-1] = (s2[1:] == s2[:-1]) & (t2[1:] == t2[:-1] + 1)
    absent_next = valid & ~nxt_same
    present_next = valid & nxt_same
    hz = pd.DataFrame({"user_name": s2, "absent": absent_next, "valid": valid})
    hzacc = hz.groupby("user_name").agg(n_valid=("valid", "sum"), n_abs=("absent", "sum"))
    hzacc = hzacc.join(acc[["mposts", "presfrac"]])
    hzacc = hzacc[hzacc["n_valid"] > 0]
    hzacc["hazard"] = hzacc["n_abs"] / hzacc["n_valid"]
    mb = [0, 1, 2, 3, 5, 10, 20, 50, 100, 10**9]
    hzacc["mb"] = pd.cut(hzacc["mposts"], mb, right=False)
    tab = hzacc.groupby("mb", observed=True).agg(
        n=("hazard", "size"), mean_mposts=("mposts", "mean"),
        hazard=("hazard", "mean"), pres=("presfrac", "mean"))
    # Poisson-thinning prediction for the weekly absence prob given weekly mean m̄:
    tab["poisson_pred"] = np.exp(-tab["mean_mposts"])
    print(tab.to_string(float_format=lambda v: f"{v:.4f}"))
    hi = hzacc[hzacc["mposts"] >= 20]
    print(f"  accounts with m̄>=20: n={len(hi):,}, mean weekly absence hazard="
          f"{hi['hazard'].mean():.4f}  (P2 threshold: <0.05)")

    # where does the 'exit pathology' live?
    lowm = hzacc[hzacc["mposts"] < 3]
    print(f"  accounts m̄<3: n={len(lowm):,}, mean hazard={lowm['hazard'].mean():.3f}")


if __name__ == "__main__":
    main()
